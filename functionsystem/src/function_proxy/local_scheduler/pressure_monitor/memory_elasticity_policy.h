#ifndef FUNCTIONSYSTEM_MEMORY_ELASTICITY_POLICY_H
#define FUNCTIONSYSTEM_MEMORY_ELASTICITY_POLICY_H

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>

namespace functionsystem::local_scheduler {
// State belongs to one logical instance; physical generation/revision changes
// reset all event and idle baselines. All sizes are bytes, time is source time.
class MemoryElasticityPolicy {
public:
    struct Sample {
        std::string generation;
        uint64_t revision = 0;
        int64_t sampledMs = 0;
        uint64_t current = 0, high = 0, maximum = 0, floor = 0, ceiling = 0;
        uint64_t highEvents = 0, oomEvents = 0, oomKills = 0;
        double cpuCores = 0;
        bool memoryKnown = false, eventsKnown = false, cpuKnown = false;
        bool revisionKnown = false, frozenKnown = false, frozen = false, pending = false;
    };
    struct Decision { uint64_t target; bool shrink; };
    std::optional<Decision> Observe(const Sample &s, int64_t nowMs)
    {
        if (!s.memoryKnown || !s.revisionKnown || s.generation.empty() || s.revision == 0
            || !s.frozenKnown || s.frozen || s.pending || s.sampledMs <= 0
            || s.sampledMs > nowMs || nowMs - s.sampledMs > 1000
            || s.floor == 0 || s.floor > s.maximum || s.maximum > s.ceiling
            || s.high == 0 || s.high > s.maximum || s.ceiling > (1ULL << 50)
            || s.current > (1ULL << 50)) {
            idleSinceMs_ = 0;
            previous_.reset();
            return {};
        }
        const bool same = previous_ && previous_->generation == s.generation
            && previous_->revision == s.revision;
        if (same && s.sampledMs <= previous_->sampledMs) { return {}; }
        const bool countersContinuous = same && previous_->eventsKnown && s.eventsKnown
            && s.highEvents >= previous_->highEvents && s.oomEvents >= previous_->oomEvents
            && s.oomKills >= previous_->oomKills;
        const bool highIncrease = countersContinuous && s.highEvents > previous_->highEvents;
        const bool oomIncrease = countersContinuous
            && (s.oomEvents > previous_->oomEvents || s.oomKills > previous_->oomKills);
        const bool killIncrease = countersContinuous && s.oomKills > previous_->oomKills;
        const bool continuous = same && s.sampledMs - previous_->sampledMs <= 600;
        previous_ = s;
        if (!same || !continuous || !countersContinuous) { idleSinceMs_ = 0; }
        if (killIncrease) { idleSinceMs_ = 0; return {}; }
        // Absolute water level can grow immediately on the first new revision.
        const bool pressure = static_cast<double>(s.current) >= 0.95 * s.high
            || (highIncrease && static_cast<double>(s.current) >= 0.90 * s.high);
        if (pressure && s.maximum < s.ceiling) {
            idleSinceMs_ = 0;
            const auto headroomTarget = static_cast<uint64_t>(
                std::ceil((static_cast<double>(s.current) + 32 * MiB) / (0.8 * Page))) * Page;
            return Decision{std::min(s.ceiling, std::max(2 * s.maximum, headroomTarget)), false};
        }
        const bool low = s.eventsKnown && s.cpuKnown && std::isfinite(s.cpuCores)
            && s.cpuCores >= 0 && s.cpuCores < 0.1 && !highIncrease && !oomIncrease
            && static_cast<double>(s.current) < 0.4 * s.maximum;
        if (!low || (same && !countersContinuous)) { idleSinceMs_ = 0; return {}; }
        if (idleSinceMs_ == 0) { idleSinceMs_ = s.sampledMs; idlePeak_ = s.current; }
        idlePeak_ = std::max(idlePeak_, s.current);
        if (s.sampledMs - idleSinceMs_ < 5000 || s.maximum == s.floor) { return {}; }
        const uint64_t target = std::max(s.floor, s.maximum / 2);
        const uint64_t targetHigh = static_cast<uint64_t>(std::floor(0.8 * target / Page)) * Page;
        if (idlePeak_ + 32 * MiB > targetHigh) { return {}; }
        idleSinceMs_ = 0;
        return Decision{target, true};
    }
private:
    static constexpr uint64_t MiB = 1024 * 1024;
    static constexpr uint64_t Page = 4096;
    std::optional<Sample> previous_;
    int64_t idleSinceMs_ = 0;
    uint64_t idlePeak_ = 0;
};
}
#endif
