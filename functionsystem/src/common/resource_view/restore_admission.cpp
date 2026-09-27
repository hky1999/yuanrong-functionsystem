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

#include "restore_admission.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>
#include "commitment_ledger.h"
#include "common/constants/constants.h"
#include "resource_sample_freshness.h"
#include "usage_trend_tracker.h"

namespace functionsystem::resource_view {
namespace {
std::mutex settingsMutex;
UsageAdmissionSettings settings{0.95, 2048.0, 0};
double Reserve(const InstanceInfo &instance, double floor)
{
    const auto memory = instance.resources().resources().find(MEMORY_RESOURCE_NAME);
    if (memory == instance.resources().resources().end() || !memory->second.has_scalar()) { return -1; }
    const auto &scalar = memory->second.scalar();
    if (!std::isfinite(scalar.value()) || scalar.value() <= 0 || !std::isfinite(scalar.limit()) || scalar.limit() < 0) { return -1; }
    if (!instance.snapshotinfo().checkpointid().empty()) { return std::max(scalar.value(), scalar.limit()); }
    return UsesElasticAdmission(instance) ? scalar.value() : std::min(floor, scalar.value());
}
double ReserveCPU(const InstanceInfo &instance)
{
    const auto cpu = instance.resources().resources().find(CPU_RESOURCE_NAME);
    if (cpu == instance.resources().resources().end() || !cpu->second.has_scalar()) { return -1; }
    const auto &scalar = cpu->second.scalar();
    if (!std::isfinite(scalar.value()) || scalar.value() <= 0
        || !std::isfinite(scalar.limit()) || scalar.limit() < 0) { return -1; }
    return instance.snapshotinfo().checkpointid().empty() ? scalar.value()
                                                         : std::max(scalar.value(), scalar.limit());
}
static AdmissionCheckResult CheckRestoreCPU(const InstanceInfo *instance, const ResourceUnit &unit, double safety,
                                              int64_t nowMs, const AdmissionReserve &extra)
{
    const auto reject = [] { return Status(StatusCode::RESOURCE_NOT_ENOUGH, "CPU: Atomic Admission Rejected"); };
    const auto fail = [nowMs](AdmissionRejectReason r) -> AdmissionCheckResult {
        return {Status(StatusCode::RESOURCE_NOT_ENOUGH, "CPU: Atomic Admission Rejected"), r, nowMs};
    };
    if (UsableNodeCPUSampleTime(unit, nowMs, DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS) == 0) { return fail(AdmissionRejectReason::CPUSampleStale); }
    const auto used = unit.actualuse().resources().find(CPU_RESOURCE_NAME);
    const auto capacity = unit.capacity().resources().find(CPU_RESOURCE_NAME);
    if (used == unit.actualuse().resources().end() || capacity == unit.capacity().resources().end()
        || !used->second.has_scalar() || !capacity->second.has_scalar()
        || !std::isfinite(used->second.scalar().value()) || used->second.scalar().value() < 0
        || !std::isfinite(capacity->second.scalar().value()) || capacity->second.scalar().value() <= 0) { return fail(AdmissionRejectReason::Other); }
    double reserve = instance ? ReserveCPU(*instance) : 0;
    if (reserve < 0) { return fail(AdmissionRejectReason::Other); }
    reserve += extra.cpuMillicores;
    for (const auto &entry : unit.instances()) {
        if (entry.second.snapshotinfo().checkpointid().empty() && !entry.second.resource_admission_pending()) { continue; }
        const double pending = ReserveCPU(entry.second);
        if (pending < 0) { return fail(AdmissionRejectReason::Other); }
        reserve += pending;
    }
    if (!std::isfinite(reserve) || used->second.scalar().value() + reserve > capacity->second.scalar().value() * safety) {
        return fail(AdmissionRejectReason::CapacityExceeded);
    }
    return {Status::OK(), AdmissionRejectReason::None, nowMs};
}

}
bool UsesElasticAdmission(const InstanceInfo &instance)
{
    const auto enabled = instance.createoptions().find(SCHEDULER_ELASTIC_RESOURCES);
    return enabled != instance.createoptions().end() && enabled->second == "true";
}
void SetUsageAdmissionSettings(double safety, double floorMb, int32_t maxInstances)
{
    std::lock_guard<std::mutex> lock(settingsMutex);
    settings = {safety, floorMb, maxInstances};
}
UsageAdmissionSettings GetUsageAdmissionSettings()
{
    std::lock_guard<std::mutex> lock(settingsMutex);
    return settings;
}
static AdmissionCheckResult CheckAdmissionWithReason(const InstanceInfo *instance, const ResourceUnit &unit,
                                                     const AdmissionReserve &extra)
{
    const auto config = GetUsageAdmissionSettings();
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const auto reject = [] { return Status(StatusCode::RESOURCE_NOT_ENOUGH, "Memory: Atomic Admission Rejected"); };
    const auto fail = [nowMs](AdmissionRejectReason r) -> AdmissionCheckResult {
        return {Status(StatusCode::RESOURCE_NOT_ENOUGH, "Memory: Atomic Admission Rejected"), r, nowMs};
    };
    if (!std::isfinite(config.safety) || config.safety <= 0 || config.safety > 1
        || !std::isfinite(config.floorMb) || config.floorMb <= 0) { return fail(AdmissionRejectReason::Other); }
    if (!std::isfinite(extra.cpuMillicores) || !std::isfinite(extra.memoryMb)
        || extra.cpuMillicores < 0 || extra.memoryMb < 0) { return fail(AdmissionRejectReason::Other); }
    if (instance != nullptr && config.maxInstances > 0 && unit.instances().size() >= static_cast<size_t>(config.maxInstances)) { return fail(AdmissionRejectReason::Other); }
    if (UsableNodeMemorySampleTime(unit, nowMs, DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS) == 0) { return fail(AdmissionRejectReason::MemorySampleStale); }
    const auto used = unit.actualuse().resources().find(MEMORY_RESOURCE_NAME);
    const auto capacity = unit.capacity().resources().find(MEMORY_RESOURCE_NAME);
    if (used == unit.actualuse().resources().end() || capacity == unit.capacity().resources().end()
        || !used->second.has_scalar() || !capacity->second.has_scalar()
        || !std::isfinite(used->second.scalar().value()) || used->second.scalar().value() < 0
        || !std::isfinite(capacity->second.scalar().value()) || capacity->second.scalar().value() <= 0) { return fail(AdmissionRejectReason::Other); }
    double reserve = instance ? Reserve(*instance, config.floorMb) : 0;
    if (reserve < 0) { return fail(AdmissionRejectReason::Other); }
    reserve += extra.memoryMb;
    for (const auto &entry : unit.instances()) {
        if (entry.second.snapshotinfo().checkpointid().empty() && !entry.second.resource_admission_pending()) { continue; }
        const double pending = Reserve(entry.second, config.floorMb);
        if (pending < 0) { return fail(AdmissionRejectReason::Other); }
        reserve += pending;
    }
    const double predicted = UsageTrendTracker::Predict(unit.id());
    const double actual = std::min(capacity->second.scalar().value(),
        std::max(used->second.scalar().value(), predicted));
    const double commitment = static_cast<double>(CommitmentLedger::Outstanding(unit.id())) / (1024.0 * 1024.0);
    if (!std::isfinite(reserve) || actual + commitment + reserve > capacity->second.scalar().value() * config.safety) {
        return fail(AdmissionRejectReason::CapacityExceeded);
    }
    const auto cpuResult = CheckRestoreCPU(instance, unit, config.safety, nowMs, extra);
    if (!cpuResult.status.IsOk()) { return cpuResult; }
    return {Status::OK(), AdmissionRejectReason::None, nowMs};
}
AdmissionCheckResult CheckResourceHeadroomWithReason(const ResourceUnit &unit, const AdmissionReserve &extra)
{
    return CheckAdmissionWithReason(nullptr, unit, extra);
}
Status CheckResourceHeadroom(const ResourceUnit &unit, const AdmissionReserve &extra)
{
    return CheckAdmissionWithReason(nullptr, unit, extra).status;
}
Status CheckRestoreAllocation(const InstanceInfo &instance, const ResourceUnit &unit, const AdmissionReserve &extra)
{
    return CheckAdmissionWithReason(&instance, unit, extra).status;
}

}
