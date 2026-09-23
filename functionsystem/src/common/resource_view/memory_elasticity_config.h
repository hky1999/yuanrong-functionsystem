#ifndef FUNCTIONSYSTEM_MEMORY_ELASTICITY_CONFIG_H
#define FUNCTIONSYSTEM_MEMORY_ELASTICITY_CONFIG_H

#include <cstdlib>
#include <cmath>
#include <cstdio>
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
        const double fallback = 0.9;
        const char *value = std::getenv("AKERNEL_MEMORY_HIGH_RATIO");
        if (value == nullptr || *value == '\0') {
            return fallback;  // unset is the documented default, not an error
        }
        try {
            size_t consumed = 0;
            const double parsed = std::stod(std::string(value), &consumed);
            const bool whole = consumed == std::string(value).size();
            if (!whole || !(parsed >= 0.5 && parsed < 1.0)) {
                std::fprintf(stderr,
                             "AKERNEL_MEMORY_HIGH_RATIO=%s is malformed or out of [0.5,1); using the 0.9 default\n",
                             value);
                return fallback;
            }
            return parsed;
        } catch (const std::exception &) {
            std::fprintf(stderr,
                         "AKERNEL_MEMORY_HIGH_RATIO=%s is malformed or out of [0.5,1); using the 0.9 default\n",
                         value);
            return fallback;
        }
    }();
    return ratio;
}

// Mirrors the daemon's cgroupmanager.MemoryHighBytes exactly:
// ceil(ratio * limit) aligned DOWN to the kernel page, then kept strictly
// below the limit (limit-1) and zeroed under one page. The page size is
// fixed at 4096 here because the daemon container and the scheduler deploy
// on the same host page size; a mismatch is a deployment error.
inline uint64_t ExpectedHighBytes(uint64_t maxBytes)
{
    constexpr uint64_t kSafeMax = (1ULL << 62) / 4;
    constexpr uint64_t kPage = 4096;
    if (maxBytes == 0 || maxBytes >= kSafeMax) { return 0; }
    const double scaled = MemoryHighRatio() * static_cast<double>(maxBytes);
    const double ceiled = std::ceil(scaled);
    if (!(ceiled >= 1.0) || ceiled >= static_cast<double>(kSafeMax)) { return 0; }
    uint64_t high = static_cast<uint64_t>(ceiled);
    if (high >= kPage) { high -= high % kPage; }
    if (high >= maxBytes) { high = maxBytes - 1; }
    return high;
}
}
#endif
