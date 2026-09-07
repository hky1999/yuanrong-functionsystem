/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "usage_aware_filter.h"

#include <algorithm>
#include <cmath>
#include <chrono>

#include "common/logs/logging.h"
#include "common/resource_view/commitment_ledger.h"
#include "common/resource_view/resource_tool.h"
#include "common/resource_view/resource_sample_freshness.h"
#include "common/resource_view/restore_admission.h"
#include "common/schedule_plugin/common/constants.h"
#include "common/schedule_plugin/common/plugin_register.h"

namespace functionsystem::schedule_plugin::filter {

void UsageAwareFilter::SetConfig(double safety, double floorMb, int32_t maxInstances)
{
    resource_view::SetUsageAdmissionSettings(safety, floorMb, maxInstances);
    YRLOG_INFO("usage aware filter config: safety={}, floorMb={}, maxInstances={}", safety, floorMb, maxInstances);
}
double UsageAwareFilter::GetSafety() { return resource_view::GetUsageAdmissionSettings().safety; }
double UsageAwareFilter::GetFloorMb() { return resource_view::GetUsageAdmissionSettings().floorMb; }
int32_t UsageAwareFilter::GetMaxInstances() { return resource_view::GetUsageAdmissionSettings().maxInstances; }

std::string UsageAwareFilter::GetPluginName()
{
    return USAGE_AWARE_FILTER_NAME;
}

schedule_framework::Filtered UsageAwareFilter::Filter(const std::shared_ptr<schedule_framework::ScheduleContext> &ctx,
                                                      const resource_view::InstanceInfo &instance,
                                                      const resource_view::ResourceUnit &resourceUnit)
{
    // Cold-start protection cannot depend on telemetry: the first Stats
    // sample may not exist yet. Include selected, not-yet-visible instances
    // from this scheduling window without counting a propagated instance twice.
    const auto preContext = std::dynamic_pointer_cast<schedule_framework::PreAllocatedContext>(ctx);
    bool hasPendingRestores = false;
    int64_t pendingCount = 0;
    int64_t pendingNotVisible = 0;
    if (preContext != nullptr) {
        for (const auto &entry : preContext->preAllocatedSelectedFunctionAgentMap) {
            if (entry.second == resourceUnit.id()) {
                ++pendingCount;
                hasPendingRestores = hasPendingRestores || preContext->preAllocatedRestoreMemory.count(entry.first) != 0;
                if (resourceUnit.instances().find(entry.first) == resourceUnit.instances().end()) {
                    ++pendingNotVisible;
                }
            }
        }
    }
    const auto maxInstances = GetMaxInstances();
    const auto occupied = static_cast<int64_t>(resourceUnit.instances().size()) + pendingNotVisible;
    if (maxInstances > 0 && occupied >= maxInstances) {
        YRLOG_INFO("{}|usage aware filter rejects unit {}: visible {} + pending {} >= max {}",
                   instance.requestid(), resourceUnit.id(), resourceUnit.instances().size(),
                   pendingNotVisible, maxInstances);
        return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Instances: Node Instance Cap Reached"),
                                             false, -1 };
    }

    double visibleRestoreReserve = 0.0;
    for (const auto &entry : resourceUnit.instances()) {
        if (entry.second.snapshotinfo().checkpointid().empty()) { continue; }
        hasPendingRestores = true;
        const auto memory = entry.second.resources().resources().find(resource_view::MEMORY_RESOURCE_NAME);
        if (memory == entry.second.resources().resources().end() || !memory->second.has_scalar()
            || !std::isfinite(memory->second.scalar().value()) || memory->second.scalar().value() <= 0
            || !std::isfinite(memory->second.scalar().limit()) || memory->second.scalar().limit() < 0) {
            return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Memory: Tracked Restore Limit Unknown"),
                                                 false, -1 };
        }
        visibleRestoreReserve += std::max(memory->second.scalar().value(), memory->second.scalar().limit());
    }
    const bool restoring = !instance.snapshotinfo().checkpointid().empty();
    if (restoring || hasPendingRestores) {
        const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        if (resource_view::UsableNodeMemorySampleTime(resourceUnit, nowMs,
                resource_view::DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS) == 0) {
            return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Memory: Restore Usage Stale Or Unknown"),
                                                 false, -1 };
        }
    }

    const auto &actualResources = resourceUnit.actualuse().resources();
    auto actualMem = actualResources.find(resource_view::MEMORY_RESOURCE_NAME);
    auto capMem = resourceUnit.capacity().resources().find(resource_view::MEMORY_RESOURCE_NAME);
    if ((restoring || hasPendingRestores) && (actualMem == actualResources.end()
        || !actualMem->second.has_scalar()
        || !std::isfinite(actualMem->second.scalar().value()) || actualMem->second.scalar().value() < 0
        || capMem == resourceUnit.capacity().resources().end()
        || !capMem->second.has_scalar()
        || !std::isfinite(capMem->second.scalar().value()) || capMem->second.scalar().value() <= 0)) {
        return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Memory: Restore Usage Unknown"),
                                             false, -1 };
    }
    // no real-usage signal (e.g. metrics not propagated yet): stay neutral and
    // let DefaultFilter decide on the booked allocatable (guaranteed fallback)
    if (!restoring && !hasPendingRestores && (actualMem == actualResources.end() || actualMem->second.scalar().value() <= 0
        || capMem == resourceUnit.capacity().resources().end() || capMem->second.scalar().value() <= 0)) {
        YRLOG_DEBUG("{}|usage aware filter has no actual use signal on unit {}, fallback to booked check",
                    instance.requestid(), resourceUnit.id());
        return schedule_framework::Filtered{ Status::OK(), false, -1 };
    }

    double used = actualMem->second.scalar().value();
    double total = capMem->second.scalar().value();
    double safety = GetSafety();

    // reserve a slice for the incoming instance, capped by its own request:
    // a small instance must not be blocked by a fixed large floor
    double reserve = GetFloorMb();
    auto reqMem = instance.resources().resources().find(resource_view::MEMORY_RESOURCE_NAME);
    if (reqMem != instance.resources().resources().end() && reqMem->second.scalar().value() > 0) {
        reserve = std::min(reserve, reqMem->second.scalar().value());
    }

    if (restoring) {
        // Restore materializes an existing working set. The create-time floor
        // is insufficient: reserve its known ceiling until an effective
        // checkpoint footprint is carried by the snapshot contract.
        if (reqMem == instance.resources().resources().end() || !reqMem->second.has_scalar()
            || !std::isfinite(reqMem->second.scalar().value())
            || !std::isfinite(reqMem->second.scalar().limit())) {
            return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Memory: Restore Limit Unknown"),
                                                 false, -1 };
        }
        reserve = std::max(reqMem->second.scalar().value(), reqMem->second.scalar().limit());
        if (!std::isfinite(reserve) || reserve <= 0) {
            return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Memory: Restore Limit Invalid"),
                                                 false, -1 };
        }
    }

    // Ph0.1 TOCTOU: instances admitted earlier in this scheduling window are
    // live but cold — the node actualuse counters have not seen them yet
    // (usage propagates on the next resource-update cycle). Count a
    // floor-style reserve per in-flight instance, capped in total by their
    // summed booked requests, so a burst of concurrent creates cannot all
    // pass the line against the same stale usage snapshot. Cross-window
    // cold instances (context already reset, usage still absent) are the
    // instance cap's job.
    double pendingReserve = visibleRestoreReserve;
    if (preContext != nullptr) {
        double pendingBooked = 0.0;
        const auto allocIt = preContext->allocated.find(resourceUnit.id());
        if (allocIt != preContext->allocated.end()) {
            const auto memory = allocIt->second.resource.resources().find(resource_view::MEMORY_RESOURCE_NAME);
            if (memory != allocIt->second.resource.resources().end()) {
                pendingBooked = memory->second.scalar().value();
            }
        }
        double restoreBooked = 0.0;
        double restoreReserved = 0.0;
        int64_t restoreCount = 0;
        for (const auto &selected : preContext->preAllocatedSelectedFunctionAgentMap) {
            if (selected.second != resourceUnit.id()) { continue; }
            const auto restored = preContext->preAllocatedRestoreMemory.find(selected.first);
            if (restored == preContext->preAllocatedRestoreMemory.end()) { continue; }
            const auto &memory = restored->second;
            const double booked = memory.scalar().value();
            const double ceiling = memory.scalar().limit();
            if (!memory.has_scalar() || !std::isfinite(booked) || booked <= 0
                || !std::isfinite(ceiling) || ceiling < 0) {
                return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Memory: Pending Restore Limit Unknown"),
                                                     false, -1 };
            }
            ++restoreCount;
            restoreBooked += booked;
            if (resourceUnit.instances().find(selected.first) == resourceUnit.instances().end()) {
                restoreReserved += std::max(booked, ceiling);
            }
        }
        if (!std::isfinite(pendingBooked) || pendingBooked < 0 || !std::isfinite(restoreReserved)) {
            return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Memory: Pending Reservation Invalid"),
                                                 false, -1 };
        }
        // Replace each restore's small booking with its full ceiling. Only
        // ordinary creates retain floor-style reserve; never floor a wake.
        const double createBooked = std::max(0.0, pendingBooked - restoreBooked);
        pendingReserve += restoreReserved + std::min((pendingCount - restoreCount) * GetFloorMb(), createBooked);
    }

    // burst prediction (D-form): a ramping node is accounted at where it is
    // heading over the horizon, not where the last sample landed; the
    // pressure monitor (watermark + sustained samples) remains the second
    // line of defense for bursts that outrun admission anyway
    double predicted = resource_view::UsageTrendTracker::Predict(resourceUnit.id());
    if (predicted > used) {
        YRLOG_DEBUG("{}|usage aware filter uses trend prediction on unit {}: used {} -> predicted {}",
                    instance.requestid(), resourceUnit.id(), used, predicted);
        used = predicted;
        // D-6 F2: the extrapolated estimate is synthetic -- clamp it to the
        // node capacity. Beyond that the number is physically meaningless (a
        // tmpfs dd burst extrapolated over the horizon showed used=102G on a
        // 41G node) and only wedges admission and misleads operators.
        used = std::min(used, total);
    }

    double allowed = total * safety;
    // P2.0 commitment-aware admission: headroom promised by upgrade-ladder
    // raises (max lifted, usage not grown in yet) is not free memory. The
    // ledger decays it as node usage grows, so the promised part is never
    // double-counted with the realized actualuse. Bytes -> MB to match the
    // view units this filter speaks.
    const double commitmentMb = static_cast<double>(resource_view::CommitmentLedger::Outstanding(resourceUnit.id())) / (1024.0 * 1024.0);
    if (used + commitmentMb + reserve + pendingReserve > allowed) {
        YRLOG_INFO("{}|usage aware filter rejects unit {}: used {} + commitment {} + reserve {} + pending {}"
                   " > capacity {} * safety {}",
                   instance.requestid(), resourceUnit.id(), used, commitmentMb, reserve, pendingReserve, total, safety);
        return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Memory: Actual Use Exceeds Safety Line"),
                                             false, -1 };
    }
    if (commitmentMb > 0.0) {
        // INFO on the pass path only when a promise is on the books: the
        // acceptance scenario (and operators) need to see the filter riding
        // the ledger without DEBUG-level noise on every ordinary pass
        YRLOG_INFO("{}|usage aware filter passes unit {}: used {} + commitment {} + reserve {} + pending {}"
                   " <= allowed {}",
                   instance.requestid(), resourceUnit.id(), used, commitmentMb, reserve, pendingReserve, allowed);
    } else {
        YRLOG_DEBUG("{}|usage aware filter passes unit {}: used {} + commitment {} + reserve {} + pending {}"
                    " <= allowed {}",
                    instance.requestid(), resourceUnit.id(), used, commitmentMb, reserve, pendingReserve, allowed);
    }
    return schedule_framework::Filtered{ Status::OK(), false, -1 };
}

std::shared_ptr<schedule_framework::SchedulePolicyPlugin> UsageAwareFilterCreator()
{
    return std::make_shared<UsageAwareFilter>();
}

REGISTER_SCHEDULER_PLUGIN(USAGE_AWARE_FILTER_NAME, UsageAwareFilterCreator);

}  // namespace functionsystem::schedule_plugin::filter
