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
#include "common/logs/logging.h"
#include "memory_elasticity_config.h"
#include "memory_elasticity_fence.h"
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
// YR-side birth floor: the instance's initial memory request. The daemon keeps
// its own persisted-policy floor; the caller may not push below either.
uint64_t InstanceMemoryFloorBytes(const InstanceInfo &instance)
{
    const auto memory = instance.resources().resources().find(MEMORY_RESOURCE_NAME);
    if (memory == instance.resources().resources().end() || !memory->second.has_scalar()) { return 0; }
    const double value = memory->second.scalar().value();
    if (!std::isfinite(value) || value <= 0) { return 0; }
    return static_cast<uint64_t>(value) * 1024ULL * 1024ULL;
}
// ExpectedHighBytes now lives in memory_elasticity_config.h so the transport
// checks and the pressure monitor share one ratio contract with the daemon.
// The soft limit is only proven when it is known, finite, and exactly the
// fixed-ratio page-aligned expectation for the readback hard limit.
bool StatsHighMatches(const runtime::v1::StatsResponse &actual)
{
    return actual.memory_high_known() && !actual.memory_high_unlimited()
        && actual.memory_high_bytes() > 0
        && actual.memory_high_bytes() == ExpectedHighBytes(actual.memory_limit_bytes());
}
bool SampleHighMatches(const resources::SandboxResourceSample &sample)
{
    return sample.memory_high_known() && !sample.memory_high_unlimited()
        && sample.memory_high_bytes() > 0
        && sample.memory_high_bytes() == ExpectedHighBytes(sample.memory_limit_bytes());
}
// Revisions are uint64 on the wire; the maximal revision has no successor and
// can never be advanced by an operation.
bool IsNextRevision(uint64_t revision, uint64_t next)
{
    return revision != std::numeric_limits<uint64_t>::max() && revision + 1 == next;
}
}

AdmissionReserve ResourceViewActor::EntryOutstanding(const ResizeBudgetEntry &entry)
{
    if (entry.shrink) { return {}; } // a pending shrink never reserves negative credit
    const auto &command = entry.request.resize();
    const auto &after = entry.observed ? entry.landed : command.target();
    AdmissionReserve result;
    result.cpuMillicores = std::max(0.0, CpuMilli(after)
        - std::max(CpuMilli(command.expected()), entry.observed ? entry.peakCpuMillicores : 0));
    const uint64_t floorBytes = std::max(command.expected().memory_limit_bytes(),
        entry.observed ? entry.peakMemoryBytes : 0);
    // credit_j = max(0, cap_j - max(expected_j, own_peak_j)); a confirmed shrink
    // only ever lowers cap_j, and the trim is permanent.
    const uint64_t capBytes = entry.observed ? std::min(entry.capMemoryBytes, after.memory_limit_bytes())
                                             : after.memory_limit_bytes();
    result.memoryMb = capBytes > floorBytes
        ? static_cast<double>(capBytes - floorBytes) / (1024.0 * 1024.0) : 0.0;
    return result;
}

AdmissionReserve ResourceViewActor::OutstandingResize(const std::string &unitID) const
{
    AdmissionReserve result;
    for (const auto &item : resizeBudget_) {
        if (item.second.unitID != unitID) { continue; }
        const auto credit = EntryOutstanding(item.second);
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
    const bool fenced = MemoryElasticityFenceEnabled();
    if (!view_ || request.requestid().empty() || request.ByteSizeLong() > 64 * 1024 || !request.has_resize()
        || !command.has_expected() || !command.has_target() || !FiniteLimits(command.expected())
        || !FiniteLimits(command.target()) || command.deadline_unix_ms() <= nowMs
        || command.deadline_unix_ms() > nowMs + 60000 || resizeBudget_.size() >= 4096
        || resizeBudget_.count(request.requestid()) != 0) { return reject("invalid or duplicate resize reservation"); }
    if (request.shrink() && !fenced) { return reject("shrink requires the memory elasticity contract"); }
    const auto instance = view_->instances().find(request.instanceid());
    // Named restores pending their admission-to-baseline handoff must not be
    // resized (shrunk): the ceiling reservation is still holding their credit.
    // The trusted extension alone is insufficient (it persists after release);
    // gate on resource_admission_pending so only the pre-handoff window blocks.
    const bool namedRestorePending = instance != view_->instances().end()
        && instance->second.resource_admission_pending()
        && instance->second.scheduleoption().extension().find(
               "yr.internal.reusable_snapshot.trusted_restore")
               != instance->second.scheduleoption().extension().end();
    if (instance == view_->instances().end() || !UsesElasticAdmission(instance->second)
        || !instance->second.snapshotinfo().checkpointid().empty()
        || namedRestorePending) {
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
        || command.target().cpu_period_usec() != command.expected().cpu_period_usec()) {
        return reject("resize identity, expected limits or ceiling mismatch");
    }
    if (fenced) {
        // The incarnation must expose the revision contract and be idle and
        // unfrozen with a known finite soft limit before any new operation.
        // Knownness comes from the mirrored flags, never inferred from values.
        if (!sample.resource_revision_known() || !sample.frozen_known() || sample.frozen()
            || !sample.memory_high_known() || sample.memory_high_unlimited()
            || !sample.memory_limit_known() || sample.resource_operation_pending()
            || command.operation_id().empty()
            || command.expected_revision() != sample.resource_revision()) {
            return reject("resource revision contract is not established for this incarnation");
        }
        // Reconciling inventory gate: every managed instance on this unit must
        // hold a fresh revision-known sample with no daemon-side pending work
        // before the unit accepts any new resource mutation.
        const auto blocker = UnitMutationBlocker(unit->second, nowMs);
        if (!blocker.empty()) {
            YRLOG_WARN("reserve resize REJECTED instance={} runtime={} request={} sandbox={} generation={} operationId={} reason=\"unit is reconciling instance {}\" unit_mutation_blocker=\"{}\"",
                       request.instanceid(), request.runtimeid(), request.requestid(),
                       command.sandbox_id(), command.resource_generation(), command.operation_id(),
                       blocker, blocker);
            return Status(RESOURCE_NOT_ENOUGH, "unit is reconciling instance " + blocker);
        }
        for (const auto &item : resizeBudget_) {
            if (!item.second.operationId.empty() && item.second.operationId == command.operation_id()) {
                return reject("operation id already owns an unresolved reservation");
            }
        }
    }
    if (request.shrink()) {
        // CPU must stay identical and only memory may decrease, never below the
        // birth floor. Shrinking consumes nothing, so there is no headroom math
        // and no CPU admission to bypass.
        const uint64_t floorBytes = InstanceMemoryFloorBytes(info);
        if (command.target().cpu_quota_usec() != command.expected().cpu_quota_usec()
            || command.target().memory_limit_bytes() >= command.expected().memory_limit_bytes()
            || floorBytes == 0 || command.target().memory_limit_bytes() < floorBytes
            || !FitsCeiling(info, command.target())) {
            return reject("shrink target must keep CPU unchanged and stay above floor and below ceiling");
        }
    } else {
        if (command.target().cpu_quota_usec() < command.expected().cpu_quota_usec()
            || command.target().memory_limit_bytes() < command.expected().memory_limit_bytes()
            || !FitsCeiling(info, command.target())) { return reject("resize identity, expected limits or ceiling mismatch"); }
    }
    for (const auto &item : resizeBudget_) {
        if (item.second.request.instanceid() != request.instanceid()) { continue; }
        if (!item.second.observed) {
            YRLOG_WARN("reserve resize REJECTED instance={} runtime={} request={} sandbox={} generation={} operationId={} reason=\"prior resize outcome unresolved (op {})\"",
                       request.instanceid(), request.runtimeid(), request.requestid(),
                       command.sandbox_id(), command.resource_generation(), command.operation_id(),
                       item.second.operationId);
            return Status(RESOURCE_NOT_ENOUGH, "prior resize outcome is unresolved");
        }
        if (item.second.request.resize().resource_generation() != command.resource_generation()) { continue; }
        if (fenced) {
            // Under the contract the sample revision, not historical landed
            // values, orders operations: a sample older than a confirmed
            // revision is stale. A permanently trimmed grow entry must never
            // block the follow-up shrink or grow at the current revision.
            if (item.second.confirmedRevision > sample.resource_revision()) {
                return reject("physical sample predates a confirmed resize");
            }
            continue;
        }
        if (command.expected().cpu_quota_usec() < item.second.landed.cpu_quota_usec()
            || command.expected().memory_limit_bytes() < item.second.landed.memory_limit_bytes()) {
            return reject("physical sample predates a confirmed resize");
        }
    }
    const AdmissionReserve added = request.shrink() ? AdmissionReserve{}
        : Credit(command.expected(), command.target(), 0, 0);
    if (!request.shrink()) {
        if (added.cpuMillicores == 0 && added.memoryMb == 0) { return reject("resize has no increase"); }
        auto total = OutstandingResize(unit->first);
        total.cpuMillicores += added.cpuMillicores;
        total.memoryMb += added.memoryMb;
        const auto checkResult = CheckResourceHeadroomWithReason(unit->second, total);
        if (!checkResult.status.IsOk()) {
            // Structured read-only rejection log: the reason comes from the
            // ACTUAL rejecting branch inside CheckAdmission — it is NEVER
            // guessed by re-reading timestamps after the failure. The
            // observation-fault test facility matches THIS line by
            // request/operation/instance ID + outcome=rejected + the
            // branch-specific reason. Sample timestamps from the unit are
            // included for independent verification.
            const char *reasonStr = "other";
            switch (checkResult.reason) {
            case AdmissionRejectReason::MemorySampleStale:
            case AdmissionRejectReason::CPUSampleStale:
                reasonStr = "node_observation_unknown_or_stale";
                break;
            case AdmissionRejectReason::CapacityExceeded:
                reasonStr = "capacity_exceeded";
                break;
            case AdmissionRejectReason::Other:
            default:
                reasonStr = "other";
                break;
            }
            YRLOG_WARN("resize admission REJECTED instance={} runtime={} request={} sandbox={} generation={} "
                       "operationId={} unit={} outcome=rejected reason={} "
                       "node_memory_sample_unix_ms={} node_cpu_sample_unix_ms={} "
                       "checked_at_unix_ms={}",
                       request.instanceid(), request.runtimeid(), request.requestid(),
                       command.sandbox_id(), command.resource_generation(),
                       command.operation_id(), unit->first,
                       reasonStr,
                       unit->second.memory_sample_unix_ms(), unit->second.cpu_sample_unix_ms(),
                       checkResult.checkedAtUnixMs);
            return checkResult.status;
        }
    }
    ResizeBudgetEntry entry;
    entry.request = request;
    entry.unitID = unit->first;
    entry.reservedAtMs = nowMs;
    entry.shrink = request.shrink();
    entry.capMemoryBytes = command.target().memory_limit_bytes();
    if (fenced) {
        entry.operationId = command.operation_id();
        entry.expectedRevision = command.expected_revision();
    }
    const auto requestID = request.requestid();
    resizeBudget_.emplace(requestID, std::move(entry));
    YRLOG_INFO("resize budget reserved instance={} runtime={} request={} sandbox={} generation={} shrink={} "
               "operationId={} expectedRevision={} addedCpuMillicores={} addedMemoryMb={}",
               request.instanceid(), request.runtimeid(), requestID, command.sandbox_id(),
               command.resource_generation(), request.shrink(), command.operation_id(), command.expected_revision(),
               added.cpuMillicores, added.memoryMb);
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
    if (it == resizeBudget_.end()) {
        YRLOG_WARN("resize observation dropped: no budget entry requestid={} entries={}", response.requestid(),
                   resizeBudget_.size());
        return Status::OK();
    }
    auto &entry = it->second;
    const auto &command = entry.request.resize();
    if (response.instanceid() != entry.request.instanceid() || response.runtimeid() != entry.request.runtimeid()
        || response.sandboxid() != command.sandbox_id() || response.resourcegeneration() != command.resource_generation()) {
        YRLOG_WARN("resize observation identity mismatch requestid={} inst({} vs {}) rt({} vs {}) sbx({} vs {}) gen({} vs {})",
                   response.requestid(), response.instanceid(), entry.request.instanceid(), response.runtimeid(),
                   entry.request.runtimeid(), response.sandboxid(), command.sandbox_id(), response.resourcegeneration(),
                   command.resource_generation());
        return Status(ERR_PARAM_INVALID, "resize observation identity mismatch");
    }
    if (entry.observed) { return Status::OK(); } // late completions cannot undo reconciliation
    if (response.outcome() == messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH) {
        resizeBudget_.erase(it);
        return Status::OK();
    }
    if (response.outcome() != messages::ResizeSandboxResponse::OBSERVED || !response.has_result()
        || !response.result().has_actual()) {
        YRLOG_WARN("resize observation retained (non-observed) requestid={} outcome={}", response.requestid(),
                   static_cast<int>(response.outcome()));
        return Status::OK(); // UNKNOWN retains the full possible write
    }
    const auto &actual = response.result().actual();
    // Entries reserved under the revision contract settle only on that
    // contract; legacy entries keep the numeric-only behaviour.
    const bool fenced = !entry.operationId.empty();
    if (fenced) {
        if (actual.resource_generation() != command.resource_generation() || !actual.cpu_limits_known()
            || !actual.memory_limit_known() || !actual.resource_revision_known()) {
            YRLOG_WARN("resize readback identity incomplete requestid={} genKnown={} cpuKnown={} memKnown={} revKnown={}",
                       response.requestid(), actual.resource_generation() == command.resource_generation(),
                       actual.cpu_limits_known(), actual.memory_limit_known(), actual.resource_revision_known());
            return Status(ERR_PARAM_INVALID, "resize readback identity is incomplete");
        }
        if (!response.result().complete()) {
            // A partial write stays UNKNOWN-equivalent unless it is a shrink
            // that provably terminated with the original limits restored:
            // same operation at the next revision, idle, unfrozen, CPU
            // unchanged and the exact ratio-aligned high for those limits.
            // That closes the failed operation's slot and adopts the new
            // revision without returning any grow credit.
            if (entry.shrink
                && IsNextRevision(entry.expectedRevision, actual.resource_revision())
                && actual.resource_operation_id() == entry.operationId && !actual.resource_operation_pending()
                && actual.frozen_known() && !actual.frozen()
                && actual.cpu_quota_usec() == command.expected().cpu_quota_usec()
                && actual.cpu_period_usec() == command.expected().cpu_period_usec()
                && actual.memory_limit_bytes() == command.expected().memory_limit_bytes()
                && StatsHighMatches(actual)) {
                entry.confirmedRevision = actual.resource_revision();
                entry.observedAtMs = ResizeNowMs();
                ApplyConfirmedResizeState(entry.request.instanceid(), entry, actual, entry.observedAtMs);
                YRLOG_INFO("shrink closed as failed-terminal with restored limits instance={} request={} "
                           "revision={} operationId={} restoredBytes={} growCreditRetained=1",
                           entry.request.instanceid(), response.requestid(), actual.resource_revision(),
                           entry.operationId, actual.memory_limit_bytes());
                resizeBudget_.erase(it);
                return Status::OK();
            }
            // Anything else keeps the conservative hold: never release credit
            // for an outcome that may still have written.
            YRLOG_INFO("resize readback incomplete, retaining reservation instance={} request={} shrink={}",
                       entry.request.instanceid(), response.requestid(), entry.shrink);
            return Status::OK();
        }
        if (!IsNextRevision(entry.expectedRevision, actual.resource_revision())
            || actual.resource_operation_id() != entry.operationId || actual.resource_operation_pending()
            || !actual.frozen_known() || actual.frozen() || !StatsHighMatches(actual)) {
            return Status(ERR_PARAM_INVALID, "resize readback does not close the operation contract");
        }
        if (entry.shrink) {
            // CPU must be bit-identical and a complete shrink must have landed
            // exactly on its target with the matching ratio-aligned high.
            if (actual.cpu_quota_usec() != command.expected().cpu_quota_usec()
                || actual.cpu_period_usec() != command.expected().cpu_period_usec()
                || actual.memory_limit_bytes() != command.target().memory_limit_bytes()) {
                return Status(ERR_PARAM_INVALID, "shrink readback does not confirm the target");
            }
        } else if (actual.cpu_period_usec() != command.expected().cpu_period_usec()
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
        entry.capMemoryBytes = actual.memory_limit_bytes();
        entry.confirmedRevision = actual.resource_revision();
        // A direct RPC readback establishes the confirmed revision; older
        // queued Stats must not roll it back afterwards.
        ApplyConfirmedResizeState(entry.request.instanceid(), entry, actual, entry.observedAtMs);
        YRLOG_INFO("resize budget observed instance={} runtime={} request={} sandbox={} generation={} shrink={} "
                   "revision={} operationId={} cpuQuotaUsec={} cpuPeriodUsec={} memoryLimitBytes={}",
                   response.instanceid(), response.runtimeid(), response.requestid(), response.sandboxid(),
                   response.resourcegeneration(), entry.shrink, actual.resource_revision(), entry.operationId,
                   actual.cpu_quota_usec(), actual.cpu_period_usec(), actual.memory_limit_bytes());
        if (entry.shrink) {
            TrimInstanceGrowCredits(entry.request.instanceid(), command.resource_generation(), entry.unitID,
                actual.memory_limit_bytes(), entry.operationId, entry.confirmedRevision);
            resizeBudget_.erase(it);
            return Status::OK();
        }
        const auto remaining = EntryOutstanding(entry);
        if (remaining.cpuMillicores == 0 && remaining.memoryMb == 0) { resizeBudget_.erase(it); }
        return Status::OK();
    }
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
    entry.capMemoryBytes = actual.memory_limit_bytes();
    YRLOG_INFO("resize budget observed instance={} runtime={} request={} sandbox={} generation={} "
               "cpuQuotaUsec={} cpuPeriodUsec={} memoryLimitBytes={}",
               response.instanceid(), response.runtimeid(), response.requestid(), response.sandboxid(),
               response.resourcegeneration(), actual.cpu_quota_usec(), actual.cpu_period_usec(),
               actual.memory_limit_bytes());
    const auto remaining = EntryOutstanding(entry);
    if (remaining.cpuMillicores == 0 && remaining.memoryMb == 0) { resizeBudget_.erase(it); }
    return Status::OK();
}

void ResourceViewActor::ApplyConfirmedResizeState(const std::string &instanceID, const ResizeBudgetEntry &entry,
                                                  const runtime::v1::StatsResponse &actual, int64_t sampledAtMs)
{
    ASSERT_IF_NULL(view_);
    auto unit = view_->mutable_fragment()->find(entry.unitID);
    if (unit == view_->mutable_fragment()->end()) { return; }
    const auto &command = entry.request.resize();
    const auto apply = [&](InstanceInfo *allocated) {
        if (!allocated->has_sandbox_resource_sample()) {
            auto *fresh = allocated->mutable_sandbox_resource_sample();
            fresh->set_runtime_id(entry.request.runtimeid());
            fresh->set_sandbox_id(command.sandbox_id());
            fresh->set_resource_generation(command.resource_generation());
        }
        auto *stored = allocated->mutable_sandbox_resource_sample();
        // Only advance the confirmed state; a readback for an incarnation or
        // revision older than what is already confirmed changes nothing.
        if (stored->resource_revision_known() && stored->resource_revision() > actual.resource_revision()) { return; }
        if (!stored->sandbox_id().empty() && stored->sandbox_id() != command.sandbox_id()) { return; }
        if (!stored->resource_generation().empty()
            && stored->resource_generation() != command.resource_generation()) { return; }
        if (!stored->runtime_id().empty() && stored->runtime_id() != entry.request.runtimeid()) { return; }
        stored->set_resource_revision(actual.resource_revision());
        stored->set_resource_revision_known(true);
        stored->set_cpu_limits_known(true);
        stored->set_cpu_quota_usec(actual.cpu_quota_usec());
        stored->set_cpu_period_usec(actual.cpu_period_usec());
        stored->set_memory_limit_bytes(actual.memory_limit_bytes());
        stored->set_memory_limit_known(actual.memory_limit_known());
        stored->set_memory_usage_known(actual.memory_usage_known());
        stored->set_memory_current_bytes(actual.memory_usage_bytes());
        stored->set_memory_peak_bytes(actual.memory_max_usage_bytes());
        stored->set_cpu_usage_ns(actual.cpu_usage_ns());
        // A cumulative RPC counter is not a fresh CPU rate. Wait for the next
        // executor delta sample instead of relabelling the old rate as current.
        stored->set_cpu_usage_known(false);
        stored->set_cpu_usage_cores(0);
        stored->set_memory_pressure_known(actual.memory_pressure_known());
        stored->set_memory_high_events(actual.memory_high_events());
        stored->set_memory_oom_events(actual.memory_oom_events());
        stored->set_memory_oom_kill_events(actual.memory_oom_kill_events());
        stored->set_memory_peak_known(actual.memory_peak_known());
        stored->set_memory_high_bytes(actual.memory_high_bytes());
        stored->set_memory_high_known(true);
        stored->set_memory_high_unlimited(false);
        stored->set_frozen(false);
        stored->set_frozen_known(true);
        stored->set_resource_operation_id(actual.resource_operation_id());
        stored->set_resource_operation_pending(false);
        stored->set_memory_cgroup_path(actual.memory_cgroup_path());
        // No source timestamp travels in this RPC response: dispatch/reserve
        // time is a conservative bound, never stamp cached usage with now.
        stored->set_sampled_at_unix_ms(std::min(sampledAtMs, entry.reservedAtMs));
    };
    auto leaf = unit->second.mutable_instances()->find(instanceID);
    if (leaf != unit->second.mutable_instances()->end()
        && (leaf->second.runtimeid().empty() || leaf->second.runtimeid() == entry.request.runtimeid())) {
        apply(&leaf->second);
    }
    auto top = view_->mutable_instances()->find(instanceID);
    if (top != view_->mutable_instances()->end() && top->second.unitid() == entry.unitID
        && (top->second.runtimeid().empty() || top->second.runtimeid() == entry.request.runtimeid())) {
        apply(&top->second);
    }
}

void ResourceViewActor::TrimInstanceGrowCredits(const std::string &instanceID, const std::string &generation,
                                                const std::string &unitID, uint64_t landedMaxBytes,
                                                const std::string &operationID, uint64_t revision)
{
    const AdmissionReserve before = OutstandingResize(unitID);
    double instanceBeforeMb = 0;
    double instanceAfterMb = 0;
    for (auto it = resizeBudget_.begin(); it != resizeBudget_.end();) {
        auto &entry = it->second;
        // Only this instance's observed grow/baseline entries in the same
        // physical incarnation are trimmed; other instances and generations
        // keep their caps.
        if (entry.shrink || !entry.observed || entry.request.instanceid() != instanceID
            || entry.request.resize().resource_generation() != generation) {
            ++it;
            continue;
        }
        instanceBeforeMb += EntryOutstanding(entry).memoryMb;
        entry.capMemoryBytes = std::min(entry.capMemoryBytes, landedMaxBytes);
        const auto remaining = EntryOutstanding(entry);
        instanceAfterMb += remaining.memoryMb;
        if (remaining.cpuMillicores == 0 && remaining.memoryMb == 0) { it = resizeBudget_.erase(it); }
        else { ++it; }
    }
    const AdmissionReserve after = OutstandingResize(unitID);
    YRLOG_INFO("shrink ledger trim instance={} generation={} landedBytes={} instanceBeforeMb={:.3f} "
               "instanceAfterMb={:.3f} unitOutstandingBeforeMb={:.3f} unitOutstandingAfterMb={:.3f} "
               "releasedMb={:.3f} operationId={} revision={}",
               instanceID, generation, landedMaxBytes, instanceBeforeMb, instanceAfterMb, before.memoryMb,
               after.memoryMb, before.memoryMb - after.memoryMb, operationID, revision);
}

ResourceViewActor::RecoveryBaselineResult ResourceViewActor::EstablishRecoveryBaseline(
    const std::string &unitID, const InstanceInfo &instance,
    const resources::SandboxResourceSample &sample)
{
    // A restart loses the grow ledger, but a missing record must not become a
    // zero commitment: hold the full observed max above current usage until
    // peaks or a confirmed shrink release it.
    // Unknownness is Deferred (keep the admission reservation); an instance
    // already at/below its initial floor is NotNeeded (release is safe); a
    // successfully created entry is Established.
    if (sample.resource_generation().empty() || !sample.resource_revision_known() || !sample.cpu_limits_known()
        || !sample.memory_limit_known() || sample.memory_limit_bytes() == 0) {
        return RecoveryBaselineResult::Deferred;
    }
    const uint64_t floorBytes = InstanceMemoryFloorBytes(instance);
    if (floorBytes == 0) { return RecoveryBaselineResult::Deferred; }
    if (sample.memory_limit_bytes() <= floorBytes) { return RecoveryBaselineResult::NotNeeded; }
    for (const auto &item : resizeBudget_) {
        if (item.second.request.instanceid() == instance.instanceid()
            && item.second.request.resize().resource_generation() == sample.resource_generation()) {
            // Idempotent only when the existing entry is the same
            // incarnation (unit, runtime, sandbox) and is confirmed. The
            // coverage check uses the SAME formula as a newly-created
            // baseline for this sample: credit = cap - max(usage, 0).
            // A cap-512/expected-512 entry has 0 credit; a 384MiB credit
            // against a 412MiB requirement (cap 512, usage 100) is
            // insufficient. Both must stay Deferred.
            if (item.second.unitID != unitID
                || item.second.request.runtimeid() != sample.runtime_id()
                || item.second.request.resize().sandbox_id() != sample.sandbox_id()) {
                continue;  // different incarnation — keep searching
            }
            if (item.second.shrink || !item.second.observed) {
                return RecoveryBaselineResult::Deferred;
            }
            // The covering entry must be confirmed at the sample's own
            // revision: an entry confirmed at a different revision describes
            // a different physical fact (a stale sample, or a revision this
            // view never dispatched). Releasing the admission against it
            // would drop a reservation measured for the wrong reading;
            // keep the reservation and retry on the next Stats round.
            if (item.second.confirmedRevision != sample.resource_revision()) {
                return RecoveryBaselineResult::Deferred;
            }
            // What a NEW baseline from THIS sample would hold:
            //   cap = sample.memory_limit_bytes()
            //   floor = usage (unknown → 0, per the establishment code)
            //   requiredCreditMb = (cap - floor) / (1024*1024)
            const uint64_t newFloorBytes = sample.memory_usage_known()
                ? sample.memory_current_bytes() : 0;
            const double requiredCreditMb =
                sample.memory_limit_bytes() > newFloorBytes
                    ? static_cast<double>(sample.memory_limit_bytes() - newFloorBytes)
                        / (1024.0 * 1024.0)
                    : 0.0;
            if (EntryOutstanding(item.second).memoryMb >= requiredCreditMb) {
                return RecoveryBaselineResult::Established;
            }
            return RecoveryBaselineResult::Deferred;
        }
    }
    ResizeBudgetEntry entry;
    entry.baseline = true;
    entry.unitID = unitID;
    entry.request.set_requestid("recovery-baseline:" + instance.instanceid() + ":" + sample.resource_generation());
    // The synthetic reservation IS an operation from the settlement's point
    // of view: give it the same deterministic id so ApplyPhysicalRetirement's
    // per-entry line carries a non-empty operationId and the reviewed
    // settlement matcher (which requires per-entry operationIds for any
    // non-zero credit) can correlate the release at retirement.
    entry.operationId = entry.request.requestid();
    entry.request.set_instanceid(instance.instanceid());
    entry.request.set_runtimeid(sample.runtime_id());
    auto *command = entry.request.mutable_resize();
    command->set_sandbox_id(sample.sandbox_id());
    command->set_resource_generation(sample.resource_generation());
    entry.landed.set_cpu_quota_usec(sample.cpu_quota_usec());
    entry.landed.set_cpu_period_usec(sample.cpu_period_usec());
    entry.landed.set_memory_limit_bytes(sample.memory_limit_bytes());
    command->mutable_expected()->CopyFrom(entry.landed);
    command->mutable_target()->CopyFrom(entry.landed);
    // The synthetic entry's floor is the usage observed at establishment, not
    // the limit: credit must hold exactly the un-consumed headroom
    // (cap - max(usage, later peaks)) without double-counting the usage that
    // node actual already reports. An unknown reading floors at zero, which
    // conservatively holds the full cap.
    const uint64_t usageBytes = sample.memory_usage_known() ? sample.memory_current_bytes() : 0;
    command->mutable_expected()->set_memory_limit_bytes(usageBytes);
    entry.reservedAtMs = sample.sampled_at_unix_ms();
    entry.observed = true;
    entry.observedAtMs = sample.sampled_at_unix_ms();
    entry.expectedRevision = sample.resource_revision();
    entry.confirmedRevision = sample.resource_revision();
    entry.capMemoryBytes = sample.memory_limit_bytes();
    entry.peakMemoryBytes = usageBytes; // the establishing reading is the first peak
    const double heldMb = EntryOutstanding(entry).memoryMb;
    const auto [it, inserted] = resizeBudget_.emplace(entry.request.requestid(), std::move(entry));
    if (!inserted) {
        // A different entry already holds the deterministic recovery-baseline
        // key for this instance+generation. Logging success without a real
        // entry would be a phantom handoff; report Deferred.
        YRLOG_WARN("recovery baseline key conflict instance={} generation={} requestID={}",
                   instance.instanceid(), sample.resource_generation(), "recovery-baseline-key");
        return RecoveryBaselineResult::Deferred;
    }
    YRLOG_INFO("recovery baseline reserved instance={} unit={} generation={} revision={} capBytes={} "
               "expectedBytes={} outstandingMemoryMb={:.3f}",
               instance.instanceid(), unitID, sample.resource_generation(), sample.resource_revision(),
               sample.memory_limit_bytes(), sample.memory_current_bytes(), heldMb);
    return RecoveryBaselineResult::Established;
}

bool ResourceViewActor::HasResizeBudgetEntries(const std::string &instanceID) const
{
    for (const auto &item : resizeBudget_) {
        if (item.second.request.instanceid() == instanceID) { return true; }
    }
    return false;
}

bool ResourceViewActor::RequiresPhysicalRetirement(const InstanceInfo &instance) const
{
    // Non-elastic instances and elastic allocations that never reached the
    // physical layer keep the generic rollback path. Everything else must
    // settle through a sandboxd retirement receipt.
    return UsesElasticAdmission(instance)
        && (instance.has_sandbox_resource_sample() || HasResizeBudgetEntries(instance.instanceid()));
}

Status ResourceViewActor::ApplyPhysicalRetirement(const PhysicalRetirementReceipt &receipt)
{
    if (!view_ || receipt.unitID.empty() || receipt.instanceID.empty() || receipt.runtimeID.empty()
        || receipt.sandboxID.empty() || receipt.resourceGeneration.empty() || receipt.requestID.size() > 256) {
        return Status(ERR_PARAM_INVALID, "physical retirement receipt requires a complete bounded identity");
    }
    const AdmissionReserve before = OutstandingResize(receipt.unitID);
    // Single-generation settlement: only entries whose full physical identity
    // (unit, instance, runtime, sandbox, generation) equals the receipt may be
    // erased — observed, pending, UNKNOWN, baseline and shrink entries alike.
    // Other instances and other incarnations of this instance keep everything.
    size_t settledEntries = 0;
    double releasedMemoryMb = 0;
    double releasedCpuMillicores = 0;
    for (auto it = resizeBudget_.begin(); it != resizeBudget_.end();) {
        const auto &entry = it->second;
        if (entry.unitID == receipt.unitID && entry.request.instanceid() == receipt.instanceID
            && entry.request.runtimeid() == receipt.runtimeID
            && entry.request.resize().sandbox_id() == receipt.sandboxID
            && entry.request.resize().resource_generation() == receipt.resourceGeneration) {
            const auto outstanding = EntryOutstanding(entry);
            releasedMemoryMb += outstanding.memoryMb;
            releasedCpuMillicores += outstanding.cpuMillicores;
            YRLOG_INFO("physical retirement settles entry instance={} runtime={} sandbox={} generation={} "
                       "request={} operationId={} observed={} shrink={} baseline={} confirmedRevision={} "
                       "entryCpuMillicores={:.3f} entryMemoryMb={:.3f}",
                       receipt.instanceID, receipt.runtimeID, receipt.sandboxID, receipt.resourceGeneration,
                       it->first, entry.operationId, entry.observed, entry.shrink, entry.baseline,
                       entry.confirmedRevision, outstanding.cpuMillicores, outstanding.memoryMb);
            it = resizeBudget_.erase(it);
            ++settledEntries;
        } else {
            ++it;
        }
    }
    const AdmissionReserve after = OutstandingResize(receipt.unitID);
    // Logical allocation: remove only when still bound to this exact
    // incarnation. A replacement generation or a different runtime keeps its
    // allocation; the erased entries above were already retired regardless.
    bool allocationDeleted = false;
    auto top = view_->mutable_instances()->find(receipt.instanceID);
    if (top != view_->mutable_instances()->end()) {
        const auto &allocated = top->second;
        const auto &sample = allocated.sandbox_resource_sample();
        if (allocated.unitid() == receipt.unitID && allocated.runtimeid() == receipt.runtimeID
            && sample.runtime_id() == receipt.runtimeID && sample.sandbox_id() == receipt.sandboxID
            && sample.resource_generation() == receipt.resourceGeneration) {
            // Same actor turn as the settlement above: no resize registration
            // can interleave between ledger erase and allocation delete.
            view_->set_revision(view_->revision() + 1);
            DeleteInstance(receipt.instanceID, false);
            allocationDeleted = true;
        } else {
            YRLOG_WARN("physical retirement keeps rebound allocation instance={} unit={} currentRuntime={} "
                       "currentSandbox={} currentGeneration={} receiptRuntime={} receiptSandbox={} "
                       "receiptGeneration={}",
                       receipt.instanceID, allocated.unitid(), allocated.runtimeid(), sample.sandbox_id(),
                       sample.resource_generation(), receipt.runtimeID, receipt.sandboxID,
                       receipt.resourceGeneration);
        }
    }
    YRLOG_INFO("physical retirement applied instance={} unit={} runtime={} sandbox={} generation={} "
               "auditRequest={} settledEntries={} releasedCpuMillicores={:.3f} releasedMemoryMb={:.3f} "
               "unitOutstandingBeforeMb={:.3f} unitOutstandingAfterMb={:.3f} allocationDeleted={} idempotent={}",
               receipt.instanceID, receipt.unitID, receipt.runtimeID, receipt.sandboxID,
               receipt.resourceGeneration, receipt.requestID, settledEntries, releasedCpuMillicores,
               releasedMemoryMb, before.memoryMb, after.memoryMb, allocationDeleted,
               settledEntries == 0 && !allocationDeleted);
    return Status::OK();
}

bool ResourceViewActor::MapsToDispatchedResize(const std::string &instanceID, const std::string &generation,
                                               uint64_t revision) const
{
    for (const auto &item : resizeBudget_) {
        const auto &entry = item.second;
        if (entry.baseline || entry.operationId.empty()
            || entry.request.instanceid() != instanceID
            || entry.request.resize().resource_generation() != generation
            || !IsNextRevision(entry.expectedRevision, revision)) { continue; }
        return true;
    }
    return false;
}

std::string ResourceViewActor::UnitMutationBlocker(const ResourceUnit &unit, int64_t nowMs) const
{
    for (const auto &entry : unit.instances()) {
        const auto &instance = entry.second;
        if (!UsesElasticAdmission(instance)) { continue; }
        // A freshly allocated instance is waiting for its birth sample; every
        // other state must already carry a fresh, revision-known reading for
        // the bound incarnation with no daemon-side operation in flight.
        if (instance.resource_admission_pending()) { return entry.first; }
        if (!instance.has_sandbox_resource_sample()) { return entry.first; }
        const auto &sample = instance.sandbox_resource_sample();
        if ((!instance.runtimeid().empty() && sample.runtime_id() != instance.runtimeid())
            || sample.runtime_id().empty() || sample.sandbox_id().empty()
            || sample.resource_generation().empty() || !sample.resource_revision_known()
            || !sample.cpu_limits_known() || !sample.memory_limit_known()
            || sample.resource_operation_pending()
            || sample.sampled_at_unix_ms() <= 0 || sample.sampled_at_unix_ms() > nowMs
            || nowMs - sample.sampled_at_unix_ms() > DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS) {
            return entry.first;
        }
    }
    return {};
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
        const bool fenced = !entry.operationId.empty();
        if (fenced && !entry.observed && command.deadline_unix_ms() < nowMs
            && sample.sampled_at_unix_ms() > command.deadline_unix_ms()
            && sample.resource_revision_known() && sample.resource_revision() == entry.expectedRevision
            && !sample.resource_operation_pending()
            && sample.resource_operation_id() != entry.operationId) {
            // A reading taken after the deadline shows the daemon idle at the
            // expected revision with a different last operation. The daemon
            // re-checks the deadline under the resource lock before persisting
            // an intent, and an intent precedes any side effect, so this
            // possible write provably never began.
            YRLOG_INFO("expired resize reservation released without daemon intent instance={} request={} "
                       "shrink={} revision={} operationId={}",
                       entry.request.instanceid(), entry.request.requestid(), entry.shrink,
                       sample.resource_revision(), entry.operationId);
            it = resizeBudget_.erase(it);
            continue;
        }
        // Terminal proof shared by both closures: our operation at the next
        // revision, idle, unfrozen, with the exact ratio-aligned high for the
        // reading's own hard limit.
        const bool terminalOurs = fenced
            && sample.resource_revision_known() && IsNextRevision(entry.expectedRevision, sample.resource_revision())
            && sample.resource_operation_id() == entry.operationId && !sample.resource_operation_pending()
            && sample.frozen_known() && !sample.frozen() && SampleHighMatches(sample);
        if (fenced && !entry.observed && entry.shrink && terminalOurs
            && sample.memory_limit_known() && sample.cpu_limits_known()
            && sample.cpu_quota_usec() == command.expected().cpu_quota_usec()
            && sample.cpu_period_usec() == command.expected().cpu_period_usec()
            && sample.memory_limit_bytes() == command.expected().memory_limit_bytes()) {
            // Failed-terminal shrink observed through Stats: the operation is
            // provably over with the original limits restored. Close its slot
            // and adopt the new revision, but keep every grow credit: nothing
            // landed, so nothing is returned and a retry may re-propose.
            entry.confirmedRevision = sample.resource_revision();
            YRLOG_INFO("shrink reconciled as failed-terminal with restored limits instance={} request={} "
                       "revision={} operationId={} restoredBytes={}",
                       entry.request.instanceid(), entry.request.requestid(), sample.resource_revision(),
                       entry.operationId, sample.memory_limit_bytes());
            it = resizeBudget_.erase(it);
            continue;
        }
        const bool numericTarget = sample.cpu_quota_usec() == command.target().cpu_quota_usec()
            && sample.cpu_period_usec() == command.target().cpu_period_usec()
            && sample.memory_limit_bytes() == command.target().memory_limit_bytes();
        // Under the revision contract a numeric match alone never closes an
        // operation: the terminal state must carry our operation at the next
        // revision, unfrozen with the exact expected soft limit.
        const bool contractClosed = !fenced || (terminalOurs && sample.memory_limit_known());
        if (!entry.observed && numericTarget && contractClosed) {
            entry.observed = true;
            entry.landed = command.target();
            entry.observedAtMs = sample.sampled_at_unix_ms();
            entry.capMemoryBytes = std::min(entry.capMemoryBytes, command.target().memory_limit_bytes());
            if (fenced) { entry.confirmedRevision = sample.resource_revision(); }
            YRLOG_INFO("resize budget reconciled instance={} request={} shrink={} revision={}",
                       entry.request.instanceid(), entry.request.requestid(), entry.shrink,
                       fenced ? sample.resource_revision() : 0);
            if (entry.shrink) {
                TrimInstanceGrowCredits(entry.request.instanceid(), command.resource_generation(), unit.id(),
                    entry.landed.memory_limit_bytes(), entry.operationId, entry.confirmedRevision);
                it = resizeBudget_.erase(it);
                continue;
            }
        }
        // own_peak_j only counts this instance's readings after the settled
        // operation, in the same incarnation, with fresh paired node windows.
        // Revision-tracked entries additionally require a sample at or beyond
        // the revision the operation established and an explicit usage-known
        // flag instead of inferring it from the numeric value.
        if (entry.observed && sample.sampled_at_unix_ms() > entry.observedAtMs
            && cpuAt > entry.observedAtMs && memoryAt >= sample.sampled_at_unix_ms()
            && sample.cpu_usage_known() && std::isfinite(1000 * sample.cpu_usage_cores()) && sample.cpu_usage_cores() >= 0
            && sample.cpu_period_usec() == entry.landed.cpu_period_usec()
            && sample.cpu_quota_usec() >= entry.landed.cpu_quota_usec()
            && sample.memory_limit_bytes() >= entry.landed.memory_limit_bytes()
            && (entry.confirmedRevision == 0
                || (sample.resource_revision_known() && sample.memory_usage_known()
                    && sample.resource_revision() >= entry.confirmedRevision))) {
            entry.peakCpuMillicores = std::max(entry.peakCpuMillicores, 1000 * sample.cpu_usage_cores());
            entry.peakMemoryBytes = std::max(entry.peakMemoryBytes, sample.memory_current_bytes());
        }
        const auto remaining = EntryOutstanding(entry);
        if (entry.observed && remaining.cpuMillicores == 0 && remaining.memoryMb == 0) { it = resizeBudget_.erase(it); }
        else { ++it; }
    }
}
}
