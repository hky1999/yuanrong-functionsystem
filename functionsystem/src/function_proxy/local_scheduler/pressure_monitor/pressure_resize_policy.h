#ifndef LOCAL_SCHEDULER_PRESSURE_RESIZE_POLICY_H
#define LOCAL_SCHEDULER_PRESSURE_RESIZE_POLICY_H

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include "common/resource_view/memory_elasticity_config.h"
#include "common/proto/pb/message_pb.h"
#include "common/resource_view/resource_type.h"

namespace functionsystem::local_scheduler {
// A closed, fully verified failed shrink can give up its operation slot without
// returning any growth credit. Exact original limits are required; partial
// lowered limits remain unresolved until the daemon/controller reconciles them.
template <typename Observation>
inline bool MemoryShrinkFailedTerminal(const runtime::v1::ResizeResourcesRequest &command,
                                       const Observation &sample)
{
    if (!command.has_expected() || command.operation_id().empty()
        || command.expected_revision() == std::numeric_limits<uint64_t>::max()) { return false; }
    const auto &expected = command.expected();
    const auto high = resource_view::ExpectedHighBytes(expected.memory_limit_bytes());
    return sample.resource_generation() == command.resource_generation()
        && sample.resource_revision_known() && sample.resource_revision() == command.expected_revision() + 1
        && sample.resource_operation_id() == command.operation_id() && !sample.resource_operation_pending()
        && sample.cpu_limits_known() && sample.cpu_quota_usec() == expected.cpu_quota_usec()
        && sample.cpu_period_usec() == expected.cpu_period_usec()
        && sample.memory_limit_known() && sample.memory_limit_bytes() == expected.memory_limit_bytes()
        && sample.memory_high_known() && !sample.memory_high_unlimited() && sample.memory_high_bytes() == high
        && sample.frozen_known() && !sample.frozen();
}

inline bool MemoryResizeNeverStarted(const runtime::v1::ResizeResourcesRequest &command,
    const std::string &runtimeID, const resources::SandboxResourceSample &sample,
    int64_t nowMs, int64_t maxAgeMs)
{
    return command.has_expected() && !command.operation_id().empty()
        && !runtimeID.empty() && !command.sandbox_id().empty() && !command.resource_generation().empty()
        && command.expected_revision() > 0 && maxAgeMs > 0
        && command.deadline_unix_ms() > 0 && sample.sampled_at_unix_ms() > command.deadline_unix_ms()
        && sample.sampled_at_unix_ms() <= nowMs && nowMs - sample.sampled_at_unix_ms() <= maxAgeMs
        && sample.runtime_id() == runtimeID && sample.sandbox_id() == command.sandbox_id()
        && sample.resource_generation() == command.resource_generation()
        && sample.resource_revision_known() && sample.resource_revision() == command.expected_revision()
        && !sample.resource_operation_pending() && sample.resource_operation_id() != command.operation_id()
        && sample.memory_limit_known() && sample.memory_limit_bytes() == command.expected().memory_limit_bytes()
        && sample.cpu_limits_known() && sample.cpu_quota_usec() == command.expected().cpu_quota_usec()
        && sample.cpu_period_usec() == command.expected().cpu_period_usec();
}

// A post-deadline locked Stats read of the exact target can resolve the
// monitor's unknown operation. It does not release the ResourceView budget.
inline bool PressureResizeTargetObserved(const runtime::v1::ResizeResourcesRequest &command,
    const std::string &runtimeID,
    const resources::SandboxResourceSample &sample, int64_t nowMs, int64_t maxAgeMs)
{
    if (!command.has_target() || runtimeID.empty()
        || command.sandbox_id().empty() || command.resource_generation().empty()
        || command.deadline_unix_ms() <= 0 || nowMs <= command.deadline_unix_ms() || maxAgeMs <= 0
        || sample.sampled_at_unix_ms() <= command.deadline_unix_ms()
        || sample.sampled_at_unix_ms() > nowMs || nowMs - sample.sampled_at_unix_ms() > maxAgeMs) {
        return false;
    }
    if (resource_view::MemoryElasticityEnabled()) {
        const auto targetHigh = resource_view::ExpectedHighBytes(command.target().memory_limit_bytes());
        if (command.operation_id().empty() || command.expected_revision() == 0
            || !sample.memory_limit_known()
            || command.expected_revision() == std::numeric_limits<uint64_t>::max()
            || !sample.resource_revision_known() || sample.resource_revision() != command.expected_revision() + 1
            || sample.resource_operation_id() != command.operation_id() || sample.resource_operation_pending()
            || !sample.frozen_known() || sample.frozen() || !sample.memory_high_known()
            || sample.memory_high_unlimited() || sample.memory_high_bytes() != targetHigh) { return false; }
    }
    return sample.runtime_id() == runtimeID && sample.sandbox_id() == command.sandbox_id()
        && sample.resource_generation() == command.resource_generation() && sample.cpu_limits_known()
        && command.target().cpu_quota_usec() > 0 && command.target().cpu_period_usec() > 0
        && command.target().memory_limit_bytes() > 0
        && sample.cpu_quota_usec() == command.target().cpu_quota_usec()
        && sample.cpu_period_usec() == command.target().cpu_period_usec()
        && sample.memory_limit_bytes() == command.target().memory_limit_bytes();
}

// Produces a proposal only. The ResourceView actor and daemon independently
// enforce budgets, current identity, expected limits and creation-time ceilings.
inline std::optional<runtime::v1::ResourceLimits> PressureResizeTarget(
    const resources::InstanceInfo &info, const resources::SandboxResourceSample *previous,
    int64_t nowMs, int64_t maxAgeMs, bool memoryEnabled, bool cpuEnabled,
    uint64_t memoryStep, double cpuStep,
    uint64_t memoryCap = std::numeric_limits<int64_t>::max(), uint64_t cpuCapQuotaAt100Ms = std::numeric_limits<int64_t>::max())
{
    const auto &s = info.sandbox_resource_sample();
    if (s.runtime_id().empty() || s.runtime_id() != info.runtimeid() || s.sandbox_id().empty()
        || s.resource_generation().empty() || s.elastic_policy().empty() || !s.cpu_limits_known()
        || s.cpu_quota_usec() <= 0 || s.cpu_period_usec() == 0 || s.memory_limit_bytes() == 0
        || s.memory_limit_bytes() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())
        || nowMs <= 0 || maxAgeMs <= 0 || s.sampled_at_unix_ms() <= 0 || s.sampled_at_unix_ms() > nowMs
        || nowMs - s.sampled_at_unix_ms() > maxAgeMs) { return std::nullopt; }
    const auto ceiling = [&info](const std::string &name) -> double {
        const auto it = info.resources().resources().find(name);
        if (it == info.resources().resources().end() || !it->second.has_scalar()) { return 0; }
        const auto &v = it->second.scalar();
        const double value = v.limit() == 0 ? v.value() : v.limit();
        return std::isfinite(value) && value > 0 ? value : 0;
    };
    const long double maxMemory = ceiling(resource_view::MEMORY_RESOURCE_NAME) * 1048576.0L;
    const long double maxQuota = ceiling(resource_view::CPU_RESOURCE_NAME) * s.cpu_period_usec() / 1000.0L;
    if (maxMemory < s.memory_limit_bytes() || maxQuota < s.cpu_quota_usec()
        || maxMemory > std::numeric_limits<int64_t>::max() || maxQuota > std::numeric_limits<int64_t>::max()) {
        return std::nullopt;
    }
    const bool comparable = previous && previous->runtime_id() == s.runtime_id()
        && previous->sandbox_id() == s.sandbox_id() && previous->resource_generation() == s.resource_generation()
        && previous->sampled_at_unix_ms() > 0 && previous->sampled_at_unix_ms() < s.sampled_at_unix_ms()
        && s.sampled_at_unix_ms() - previous->sampled_at_unix_ms() <= maxAgeMs;
    // A new OOM kill is a failure to reconcile, not a process rescued by growth.
    if (comparable && s.memory_pressure_known() && previous->memory_pressure_known()
        && s.memory_oom_kill_events() > previous->memory_oom_kill_events()) { return std::nullopt; }
    const bool memoryPressure = s.memory_current_bytes() >= s.memory_limit_bytes() * 0.8L
        || (comparable && s.memory_pressure_known() && previous->memory_pressure_known()
            && s.memory_high_events() > previous->memory_high_events());
    const bool cpuPressure = s.cpu_usage_known() && std::isfinite(s.cpu_usage_cores())
        && s.cpu_usage_cores() >= 0.8L * s.cpu_quota_usec() / s.cpu_period_usec();
    const bool throttled = comparable && s.cpu_pressure_known() && previous->cpu_pressure_known()
        && s.cpu_throttled_periods() > previous->cpu_throttled_periods();
    runtime::v1::ResourceLimits target;
    target.set_cpu_quota_usec(s.cpu_quota_usec());
    target.set_cpu_period_usec(s.cpu_period_usec());
    target.set_memory_limit_bytes(s.memory_limit_bytes());
    if (memoryEnabled && memoryPressure && memoryStep > 0 && memoryCap >= s.memory_limit_bytes()) {
        // At most double a small initial sandbox, even with a legacy 8GiB step.
        const auto step = std::min(memoryStep, s.memory_limit_bytes());
        target.set_memory_limit_bytes(static_cast<uint64_t>(std::min(maxMemory,
            std::min(static_cast<long double>(memoryCap), static_cast<long double>(s.memory_limit_bytes()) + step))));
    }
    const long double configuredQuota = static_cast<long double>(cpuCapQuotaAt100Ms) * s.cpu_period_usec() / 100000;
    if (cpuEnabled && (cpuPressure || throttled) && std::isfinite(cpuStep) && cpuStep > 1.0
        && configuredQuota >= s.cpu_quota_usec()) {
        target.set_cpu_quota_usec(static_cast<int64_t>(std::min(maxQuota,
            std::min(configuredQuota,
                     std::ceil(static_cast<long double>(s.cpu_quota_usec()) * cpuStep)))));
    }
    if (target.cpu_quota_usec() == s.cpu_quota_usec()
        && target.memory_limit_bytes() == s.memory_limit_bytes()) { return std::nullopt; }
    return target;
}


// The high watermark a COMPLETED elastic resize receipt must carry: the
// daemon computes high as ceil(ratio*max) page-aligned-down capped below
// max (memory_elasticity_config.h, default ratio 0.9). A local 0.8 copy in
// OnPressureResize misrecorded every CORRECT 0.9 receipt as unknown
// (pc-elastic-4 live evidence, 1631Z) — this helper keeps one contract.
inline uint64_t ElasticResizeReceiptHighBytes(const runtime::v1::ResizeResourcesRequest &command)
{
    return resource_view::ExpectedHighBytes(command.target().memory_limit_bytes());
}

} // namespace functionsystem::local_scheduler
#endif