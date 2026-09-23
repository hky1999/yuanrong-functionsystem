#ifndef FUNCTIONSYSTEM_MEMORY_ELASTICITY_CONFIG_H
#define FUNCTIONSYSTEM_MEMORY_ELASTICITY_CONFIG_H

#include <cstdlib>
#include <cmath>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <string_view>

namespace functionsystem::resource_view {
// Explicit process-start opt-in. This is independent of the legacy ladder name.
inline bool MemoryElasticityEnabled()
{
    static const bool enabled = [] {
        const char *value = std::getenv("AKERNEL_MEMORY_ELASTICITY");
        return value != nullptr && std::string_view(value) == "1";
    }();
    return enabled;
}
constexpr int MEMORY_ELASTIC_STATS_INTERVAL_MS = 200;

// Soft-limit ratio contract: the scheduler-side completion checks require the
// daemon's enforced memory.high to equal floor(ratio * max) page-aligned down,
// with the same ratio the daemon's memory_high_ratio config uses. The ratio is
// a process-start constant; deployments must keep it identical on both sides
// (default 0.9 matches the canonical chart's sandboxd memory_high_ratio).
inline double MemoryHighRatio()
{
    static const double ratio = [] {
        const char *value = std::getenv("AKERNEL_MEMORY_HIGH_RATIO");
        if (value == nullptr) {
            return 0.9;
        }
        try {
            const double parsed = std::stod(value);
            return parsed >= 0.5 && parsed < 1.0 ? parsed : 0.9;
        } catch (const std::exception &) {
            return 0.9;
        }
    }();
    return ratio;
}

// floor(ratio * maxBytes / pageSize) * pageSize with the same double math the
// daemon applies; the sub-page double error cannot cross a page boundary for
// realistic limits (verified against 0.9*128MiB -> 120795136 and
// 0.9*256MiB -> 241590272 physical readbacks).
inline uint64_t ExpectedHighBytes(uint64_t maxBytes)
{
    constexpr uint64_t kSafeMax = (1ULL << 62) / 4;
    if (maxBytes == 0 || maxBytes >= kSafeMax) { return 0; }
    const double pages = std::floor(MemoryHighRatio() * static_cast<double>(maxBytes) / 4096.0);
    return pages > 0.0 ? static_cast<uint64_t>(pages) * 4096 : 0;
}
}
#endif
