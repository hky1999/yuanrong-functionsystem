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

#ifndef LOCAL_SCHEDULER_CPU_UPGRADE_LADDER_H
#define LOCAL_SCHEDULER_CPU_UPGRADE_LADDER_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace functionsystem::local_scheduler {

/** Runtime knobs for CpuUpgradeLadder (mirrors the driver flags). */
struct CpuUpgradeLadderConfig {
    bool enabled = false;
    double stepRatio = 1.5;               // quota *= stepRatio per rung
    uint64_t capQuotaUsec = 800000;       // per-sandbox ceiling (8 cpus @ 100ms period)
    double safetyRatio = 0.9;             // (usage + step) <= safety * node capacity
    uint64_t nodeCapacityMilli = 0;       // node CPU domain, millicores; 0 = fail closed
    uint64_t minRateWindowUsec = 1000000; // scope rate trusted only over >= this window
};

/**
 * W-CPUL CPU upgrade-as-signal ladder: a sandbox whose cpu.stat nr_throttled
 * grows (it burned through its cpu.max quota within a period) asks for one
 * quota rung — cpu.max quota *= stepRatio, period untouched — admitted only
 * when the node CPU domain has headroom.
 *
 * Deliberately cgroupfs-self-contained (unlike the memory ladder, which rides
 * the resource view's actualuse sample): the standalone's per-instance stats
 * bridge only reports memory, and the view's CPU gauge units are ambiguous
 * across consumers ("m" vs "vmillicore"). Admission usage is therefore the
 * scope-aggregate usage RATE the ladder measures itself — delta of
 * Σ<pool>/cpu.stat usage_usec between two of its own reads over a wall-clock
 * window (>= minRateWindowUsec; shorter windows keep the last measured rate).
 *
 * Other semantics mirror the D-3 memory ladder: node-level by design (no
 * pool-dir to instance mapping needed), denied rungs park in the deferred set
 * and stay throttled, at most one release per sample, fresh admission recheck
 * before every write. CPU promises are NOT booked in the CommitmentLedger:
 * CPU oversell only degrades (throttling) where memory oversell kills, so the
 * ledger stays a memory-domain instrument (recorded as a follow-up item).
 *
 * All methods do synchronous file IO and must be called from the pressure
 * monitor actor thread only.
 */
class CpuUpgradeLadder {
public:
    /**
     * @param nowUsec monotonic clock in microseconds (injectable for tests;
     *                default = steady_clock)
     */
    CpuUpgradeLadder(const std::string &nodeID, CpuUpgradeLadderConfig config, std::string cgroupRoot,
                     std::function<uint64_t()> nowUsec = DefaultClock);

    /**
     * A sandbox (pool dir) saw nr_throttled grow. Try one quota rung now;
     * park it in the deferred set when node admission denies the raise.
     * (Async entry for a future event source; on cgroupfs the ladder relies
     * on the OnSample scan below — cpu.stat emits no inotify.)
     */
    void OnThrottleEvent(const std::string &poolDir, uint64_t increment);

    /**
     * Sampling tick (every monitor sample). Refreshes the measured scope rate,
     * scans the pool dirs for nr_throttled growth (cgroupfs cpu.stat cannot
     * be watched with inotify — cgroup_file_notify covers memory.events and
     * friends only, so the sampling cadence is the event source) and raises
     * one rung per throttled sandbox; also re-checks the deferred set.
     * Returns true when this call raised at least one rung.
     */
    bool OnSample();

    /** Test/inspection hook. */
    size_t DeferredCount() const
    {
        return deferredDirs_.size();
    }

    static uint64_t DefaultClock()
    {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
    }

private:
    /**
     * Admission + write for one dir. Returns true when a raise was written.
     * Fail-closed: no trusted rate sample yet (or nodeCapacityMilli == 0)
     * denies. A "max" (unlimited) quota is treated as already at cap.
     * The rung never crosses capQuotaUsec.
     */
    bool TryRaise(const std::string &poolDir);

    /**
     * Σ usage_usec over the pool dirs, plus the rate bookkeeping: arms the
     * baseline on the first read, then refreshes lastRateMilli_ (and trusts
     * it for admission) once a window >= minRateWindowUsec has elapsed.
     * Returns false when no pool dir exposed usage_usec.
     */
    bool ReadScopeUsage(uint64_t &usageUsec);

    std::string nodeID_;
    CpuUpgradeLadderConfig config_;
    std::string cgroupRoot_;
    std::function<uint64_t()> nowUsec_;

    // measured node usage (rate in millicores over the ladder's own reads)
    uint64_t lastScopeUsageUsec_ = 0;
    uint64_t lastScopeReadUsec_ = 0;
    bool haveBaseline_ = false;  // a previous scope read exists
    bool rateTrusted_ = false;   // one window >= minRateWindowUsec observed
    uint64_t lastRateMilli_ = 0;

    // last nr_throttled seen per pool dir (the OnSample scan's baseline); a
    // dir's first sighting only seeds, growth is what asks for a rung
    std::unordered_map<std::string, uint64_t> lastThrottled_;

    std::unordered_set<std::string> deferredDirs_;
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_CPU_UPGRADE_LADDER_H
