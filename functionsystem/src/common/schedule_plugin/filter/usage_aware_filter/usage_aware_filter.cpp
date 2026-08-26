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

#include "common/logs/logging.h"
#include "common/resource_view/commitment_ledger.h"
#include "common/resource_view/resource_tool.h"
#include "common/schedule_plugin/common/constants.h"
#include "common/schedule_plugin/common/plugin_register.h"

namespace functionsystem::schedule_plugin::filter {

std::mutex UsageAwareFilter::configMutex_;
double UsageAwareFilter::safety_ = 0.95;
double UsageAwareFilter::floorMb_ = 2048.0;
int32_t UsageAwareFilter::maxInstances_ = 0;

void UsageAwareFilter::SetConfig(double safety, double floorMb, int32_t maxInstances)
{
    std::lock_guard<std::mutex> lock(configMutex_);
    safety_ = safety;
    floorMb_ = floorMb;
    maxInstances_ = maxInstances;
    YRLOG_INFO("usage aware filter config: safety={}, floorMb={}, maxInstances={}", safety, floorMb, maxInstances);
}

double UsageAwareFilter::GetSafety()
{
    std::lock_guard<std::mutex> lock(configMutex_);
    return safety_;
}

double UsageAwareFilter::GetFloorMb()
{
    std::lock_guard<std::mutex> lock(configMutex_);
    return floorMb_;
}

int32_t UsageAwareFilter::GetMaxInstances()
{
    std::lock_guard<std::mutex> lock(configMutex_);
    return maxInstances_;
}

std::string UsageAwareFilter::GetPluginName()
{
    return USAGE_AWARE_FILTER_NAME;
}

schedule_framework::Filtered UsageAwareFilter::Filter(const std::shared_ptr<schedule_framework::ScheduleContext> &ctx,
                                                      const resource_view::InstanceInfo &instance,
                                                      const resource_view::ResourceUnit &resourceUnit)
{
    const auto &actualResources = resourceUnit.actualuse().resources();
    auto actualMem = actualResources.find(resource_view::MEMORY_RESOURCE_NAME);
    auto capMem = resourceUnit.capacity().resources().find(resource_view::MEMORY_RESOURCE_NAME);
    // no real-usage signal (e.g. metrics not propagated yet): stay neutral and
    // let DefaultFilter decide on the booked allocatable (guaranteed fallback)
    if (actualMem == actualResources.end() || actualMem->second.scalar().value() <= 0
        || capMem == resourceUnit.capacity().resources().end() || capMem->second.scalar().value() <= 0) {
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

    // Ph0.1 TOCTOU: instances admitted earlier in this scheduling window are
    // live but cold — the node actualuse counters have not seen them yet
    // (usage propagates on the next resource-update cycle). Count a
    // floor-style reserve per in-flight instance, capped in total by their
    // summed booked requests, so a burst of concurrent creates cannot all
    // pass the line against the same stale usage snapshot. Cross-window
    // cold instances (context already reset, usage still absent) are the
    // instance cap's job.
    double pendingReserve = 0.0;
    const auto preContext = std::dynamic_pointer_cast<schedule_framework::PreAllocatedContext>(ctx);
    if (preContext != nullptr) {
        auto allocIt = preContext->allocated.find(resourceUnit.id());
        if (allocIt != preContext->allocated.end()) {
            double pendingBooked = 0.0;
            auto pendMem = allocIt->second.resource.resources().find(resource_view::MEMORY_RESOURCE_NAME);
            if (pendMem != allocIt->second.resource.resources().end() && pendMem->second.scalar().value() > 0) {
                pendingBooked = pendMem->second.scalar().value();
            }
            int64_t pendingCount = 0;
            for (const auto &entry : preContext->preAllocatedSelectedFunctionAgentMap) {
                if (entry.second == resourceUnit.id()) {
                    pendingCount++;
                }
            }
            if (pendingCount > 0 && pendingBooked > 0) {
                pendingReserve = std::min(pendingCount * GetFloorMb(), pendingBooked);
            }
        }
    }

    // hard cap on concurrent instances (IO / runsc constant-resource backstop
    // for cold start: a fleet of zero-usage instances would otherwise all
    // pass the memory check until they warm up together)
    auto maxInstances = GetMaxInstances();
    if (maxInstances > 0 && static_cast<int64_t>(resourceUnit.instances().size()) >= maxInstances) {
        YRLOG_INFO("{}|usage aware filter rejects unit {}: instances {} >= max {}",
                   instance.requestid(), resourceUnit.id(), resourceUnit.instances().size(), maxInstances);
        return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Instances: Node Instance Cap Reached"),
                                             false, -1 };
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
