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

#ifndef LOCAL_SCHEDULER_UPGRADE_LADDER_H
#define LOCAL_SCHEDULER_UPGRADE_LADDER_H

#include <cstdint>
#include <string>
#include <unordered_set>

namespace functionsystem::local_scheduler {

/** Runtime knobs for UpgradeLadder (mirrors the driver flags). */
struct UpgradeLadderConfig {
    bool enabled = false;
    uint64_t stepBytes = 8ULL * 1024 * 1024 * 1024; // one ladder rung (I5: 8 GiB)
    uint64_t capBytes = 32ULL * 1024 * 1024 * 1024; // per-sandbox ceiling
    double safetyRatio = 0.9; // admit raise only if (use + step) <= safety * capacity
    double highRatio = 0.9;   // memory.high = highRatio * new max after a raise
};

/**
 * D-3 upgrade-as-signal: a sandbox crossing its memory.high soft limit is the
 * most accurate "about to grow" signal available (I5 asset). Instead of
 * raising the limit unconditionally, the raise goes through node admission
 * first (resizer.py semantics: (actualuse + step) vs safety * capacity):
 *
 * - admitted  -> write <pool>/memory.max += step, then
 *                <pool>/memory.high = highRatio * new max (throttle releases);
 * - denied    -> the sandbox is parked in the deferred set and keeps being
 *                throttled at its current high; every monitor sample re-checks
 *                the deferred set and releases at most one raise per sample
 *                once usage drops (e.g. after a pressure park reclaimed).
 *
 * Node-level by design: admission only looks at node actualuse vs capacity,
 * and the raise target is the watcher-reported pool dir — no pool-dir to
 * instance mapping is needed (pool ids are sandboxd-internal). All methods
 * do synchronous file IO and must be called from the pressure monitor actor
 * thread only.
 */
class UpgradeLadder {
public:
    UpgradeLadder(const std::string &nodeID, UpgradeLadderConfig config, std::string cgroupRoot);

    /**
     * A sandbox (pool dir) crossed memory.high. Try one ladder rung now;
     * park it in the deferred set when node admission denies the raise.
     */
    void OnThrottleEvent(const std::string &poolDir, uint64_t increment);

    /**
     * Node usage refresh (every monitor sample). Re-checks the deferred set;
     * releases at most one raise per call so simultaneous warm-ups cannot
     * burst through in a single window. Returns true when this call raised a
     * rung (D-3②: callers use it to cap ladder work at one rung per sample).
     */
    bool OnNodeUsage(uint64_t actualUseBytes, uint64_t capacityBytes);

    /** Test/inspection hook. */
    size_t DeferredCount() const
    {
        return deferredDirs_.size();
    }

private:
    /**
     * Admission + write for one dir. Returns true when a raise was written.
     * Caller supplies the cached node numbers; 0 capacity means "unknown" and
     * denies (fail-closed: no unbounded raise without a usage sample).
     * The cached actualuse can be a view sample stale (D-3①): the kernel's
     * <dir>/memory.current is re-read here and admission uses the max of both
     * (read failure falls back to the sample — the unit-test fake tree has no
     * such file). The rung is capped at the sandbox's own current max (D-3③),
     * so small cgroups climb proportionally instead of jumping the global
     * step in one go.
     */
    bool TryRaise(const std::string &poolDir, uint64_t actualUseBytes, uint64_t capacityBytes);

    std::string nodeID_;
    UpgradeLadderConfig config_;
    std::string cgroupRoot_;
    uint64_t lastActualUseBytes_ = 0;
    uint64_t lastCapacityBytes_ = 0;
    std::unordered_set<std::string> deferredDirs_;
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_UPGRADE_LADDER_H
