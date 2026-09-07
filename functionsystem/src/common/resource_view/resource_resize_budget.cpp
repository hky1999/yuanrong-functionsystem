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

#include "resource_view_actor.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include "resource_sample_freshness.h"

namespace functionsystem::resource_view {
namespace {
int64_t ResizeNowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
double CpuMilli(const runtime::v1::ResourceLimits &limits)
{
    return 1000.0 * static_cast<double>(limits.cpu_quota_usec()) / limits.cpu_period_usec();
}
bool FiniteLimits(const runtime::v1::ResourceLimits &limits)
{
    return limits.cpu_quota_usec() > 0 && limits.cpu_period_usec() > 0
        && limits.memory_limit_bytes() > 0
        && limits.memory_limit_bytes() <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
}
AdmissionReserve Credit(const runtime::v1::ResourceLimits &before, const runtime::v1::ResourceLimits &after,
                        double peakCpu, uint64_t peakMemory)
{
    return {std::max(0.0, CpuMilli(after) - std::max(CpuMilli(before), peakCpu)),
        static_cast<double>(after.memory_limit_bytes() > std::max(before.memory_limit_bytes(), peakMemory)
            ? after.memory_limit_bytes() - std::max(before.memory_limit_bytes(), peakMemory) : 0) / (1024.0 * 1024.0)};
}
bool FitsCeiling(const InstanceInfo &instance, const runtime::v1::ResourceLimits &target)
{
    for (const auto &name : {CPU_RESOURCE_NAME, MEMORY_RESOURCE_NAME}) {
        auto it = instance.resources().resources().find(name);
        if (it == instance.resources().resources().end() || !it->second.has_scalar()) { return false; }
        const auto &scalar = it->second.scalar();
        const double limit = scalar.limit() == 0 ? scalar.value() : scalar.limit();
        const double wanted = name == CPU_RESOURCE_NAME ? CpuMilli(target)
            : static_cast<double>(target.memory_limit_bytes()) / (1024.0 * 1024.0);
        if (!std::isfinite(limit) || limit <= 0 || wanted > limit) { return false; }
    }
    return true;
}
}

AdmissionReserve ResourceViewActor::OutstandingResize(const std::string &unitID) const
{
    AdmissionReserve result;
    for (const auto &item : resizeBudget_) {
        const auto &entry = item.second;
        if (entry.unitID != unitID) { continue; }
        const auto credit = Credit(entry.request.resize().expected(),
            entry.observed ? entry.landed : entry.request.resize().target(),
            entry.observed ? entry.peakCpuMillicores : 0, entry.observed ? entry.peakMemoryBytes : 0);
        result.cpuMillicores += credit.cpuMillicores;
        result.memoryMb += credit.memoryMb;
    }
    return result;
}

Status ResourceViewActor::ReserveResize(const messages::ResizeSandboxRequest &request)
{
    const auto reject = [](const std::string &why) { return Status(ERR_PARAM_INVALID, why); };
    const auto nowMs = ResizeNowMs();
    const auto &command = request.resize();
    if (!view_ || request.requestid().empty() || request.ByteSizeLong() > 64 * 1024 || !request.has_resize()
        || !command.has_expected() || !command.has_target() || !FiniteLimits(command.expected())
        || !FiniteLimits(command.target()) || command.deadline_unix_ms() <= nowMs
        || command.deadline_unix_ms() > nowMs + 60000 || resizeBudget_.size() >= 4096
        || resizeBudget_.count(request.requestid()) != 0) { return reject("invalid or duplicate resize reservation"); }
    const auto instance = view_->instances().find(request.instanceid());
    if (instance == view_->instances().end() || !UsesElasticAdmission(instance->second)
        || !instance->second.snapshotinfo().checkpointid().empty()) {
        return reject("resize requires an elastic instance outside restoration");
    }
    const auto &info = instance->second;
    const auto &sample = info.sandbox_resource_sample();
    const auto unit = view_->fragment().find(info.unitid());
    if (unit == view_->fragment().end() || unit->second.status() != static_cast<uint32_t>(UnitStatus::NORMAL)
        || request.runtimeid().empty() || request.runtimeid() != info.runtimeid()
        || sample.runtime_id() != request.runtimeid() || sample.sandbox_id().empty()
        || sample.sandbox_id() != command.sandbox_id() || sample.resource_generation().empty()
        || sample.resource_generation() != command.resource_generation() || !sample.cpu_limits_known()
        || sample.sampled_at_unix_ms() <= 0 || sample.sampled_at_unix_ms() > nowMs
        || nowMs - sample.sampled_at_unix_ms() > DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS
        || sample.cpu_quota_usec() != command.expected().cpu_quota_usec()
        || sample.cpu_period_usec() != command.expected().cpu_period_usec()
        || sample.memory_limit_bytes() != command.expected().memory_limit_bytes()
        || command.target().cpu_period_usec() != command.expected().cpu_period_usec()
        || command.target().cpu_quota_usec() < command.expected().cpu_quota_usec()
        || command.target().memory_limit_bytes() < command.expected().memory_limit_bytes()
        || !FitsCeiling(info, command.target())) { return reject("resize identity, expected limits or ceiling mismatch"); }
    for (const auto &item : resizeBudget_) {
        if (item.second.request.instanceid() != request.instanceid()) { continue; }
        if (!item.second.observed) { return Status(RESOURCE_NOT_ENOUGH, "prior resize outcome is unresolved"); }
        if (item.second.request.resize().resource_generation() == command.resource_generation()
            && (command.expected().cpu_quota_usec() < item.second.landed.cpu_quota_usec()
                || command.expected().memory_limit_bytes() < item.second.landed.memory_limit_bytes())) {
            return reject("physical sample predates a confirmed resize");
        }
    }
    auto total = OutstandingResize(unit->first);
    const auto added = Credit(command.expected(), command.target(), 0, 0);
    if (added.cpuMillicores == 0 && added.memoryMb == 0) { return reject("resize has no increase"); }
    total.cpuMillicores += added.cpuMillicores;
    total.memoryMb += added.memoryMb;
    const auto verdict = CheckResourceHeadroom(unit->second, total);
    if (!verdict.IsOk()) { return verdict; }
    ResizeBudgetEntry entry;
    entry.request = request;
    entry.unitID = unit->first;
    entry.reservedAtMs = nowMs;
    resizeBudget_.emplace(request.requestid(), std::move(entry));
    return Status::OK();
}

Status ResourceViewActor::CancelResizeBeforeDispatch(const std::string &reservationID)
{
    // Only the owning manager may call this after establishing that Send never
    // occurred (e.g. its budget future completed after the local waiter expired).
    auto it = resizeBudget_.find(reservationID);
    if (it != resizeBudget_.end() && !it->second.observed) { resizeBudget_.erase(it); }
    return Status::OK();
}

Status ResourceViewActor::ObserveResize(const messages::ResizeSandboxResponse &response)
{
    auto it = resizeBudget_.find(response.requestid());
    if (it == resizeBudget_.end()) { return Status::OK(); }
    auto &entry = it->second;
    const auto &command = entry.request.resize();
    if (response.instanceid() != entry.request.instanceid() || response.runtimeid() != entry.request.runtimeid()
        || response.sandboxid() != command.sandbox_id() || response.resourcegeneration() != command.resource_generation()) {
        return Status(ERR_PARAM_INVALID, "resize observation identity mismatch");
    }
    if (entry.observed) { return Status::OK(); } // late completions cannot undo reconciliation
    if (response.outcome() == messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH) {
        resizeBudget_.erase(it);
        return Status::OK();
    }
    if (response.outcome() != messages::ResizeSandboxResponse::OBSERVED || !response.has_result()
        || !response.result().has_actual()) { return Status::OK(); } // UNKNOWN retains the full possible write
    const auto &actual = response.result().actual();
    if (actual.resource_generation() != command.resource_generation() || !actual.cpu_limits_known()
        || actual.cpu_period_usec() != command.expected().cpu_period_usec()
        || actual.cpu_quota_usec() < command.expected().cpu_quota_usec()
        || actual.cpu_quota_usec() > command.target().cpu_quota_usec()
        || actual.memory_limit_bytes() < command.expected().memory_limit_bytes()
        || actual.memory_limit_bytes() > command.target().memory_limit_bytes()) {
        return Status(ERR_PARAM_INVALID, "resize readback lies outside reserved bounds");
    }
    entry.landed.set_cpu_quota_usec(actual.cpu_quota_usec());
    entry.landed.set_cpu_period_usec(actual.cpu_period_usec());
    entry.landed.set_memory_limit_bytes(actual.memory_limit_bytes());
    entry.observed = true;
    entry.observedAtMs = ResizeNowMs();
    const auto remaining = Credit(command.expected(), entry.landed, 0, 0);
    if (remaining.cpuMillicores == 0 && remaining.memoryMb == 0) { resizeBudget_.erase(it); }
    return Status::OK();
}

void ResourceViewActor::ReconcileResize(const ResourceUnit &unit)
{
    const auto nowMs = ResizeNowMs();
    const auto cpuAt = UsableNodeCPUSampleTime(unit, nowMs, DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS);
    const auto memoryAt = UsableNodeMemorySampleTime(unit, nowMs, DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS);
    for (auto it = resizeBudget_.begin(); it != resizeBudget_.end();) {
        auto &entry = it->second;
        const auto &command = entry.request.resize();
        auto instance = unit.instances().find(entry.request.instanceid());
        if (entry.unitID != unit.id() || instance == unit.instances().end()) { ++it; continue; }
        const auto &sample = instance->second.sandbox_resource_sample();
        if (sample.runtime_id() != entry.request.runtimeid() || sample.sandbox_id() != command.sandbox_id()
            || sample.resource_generation() != command.resource_generation() || !sample.cpu_limits_known()
            || sample.sampled_at_unix_ms() <= entry.reservedAtMs || sample.sampled_at_unix_ms() > nowMs
            || nowMs - sample.sampled_at_unix_ms() > DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS) {
            ++it; continue;
        }
        if (!entry.observed && sample.cpu_quota_usec() == command.target().cpu_quota_usec()
            && sample.cpu_period_usec() == command.target().cpu_period_usec()
            && sample.memory_limit_bytes() == command.target().memory_limit_bytes()) {
            entry.observed = true;
            entry.landed = command.target();
            entry.observedAtMs = sample.sampled_at_unix_ms();
        }
        if (entry.observed && sample.sampled_at_unix_ms() > entry.observedAtMs
            && cpuAt > entry.observedAtMs && memoryAt >= sample.sampled_at_unix_ms()
            && sample.cpu_usage_known() && std::isfinite(1000 * sample.cpu_usage_cores()) && sample.cpu_usage_cores() >= 0
            && sample.cpu_period_usec() == entry.landed.cpu_period_usec()
            && sample.cpu_quota_usec() >= entry.landed.cpu_quota_usec()
            && sample.memory_limit_bytes() >= entry.landed.memory_limit_bytes()) {
            entry.peakCpuMillicores = std::max(entry.peakCpuMillicores, 1000 * sample.cpu_usage_cores());
            entry.peakMemoryBytes = std::max(entry.peakMemoryBytes, sample.memory_current_bytes());
        }
        const auto remaining = Credit(command.expected(), entry.observed ? entry.landed : command.target(),
            entry.peakCpuMillicores, entry.peakMemoryBytes);
        if (entry.observed && remaining.cpuMillicores == 0 && remaining.memoryMb == 0) { it = resizeBudget_.erase(it); }
        else { ++it; }
    }
}
}
