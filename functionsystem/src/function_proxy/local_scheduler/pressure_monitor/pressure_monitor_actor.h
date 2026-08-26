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

#ifndef LOCAL_SCHEDULER_PRESSURE_MONITOR_ACTOR_H
#define LOCAL_SCHEDULER_PRESSURE_MONITOR_ACTOR_H

#include <actor/actor.hpp>
#include <async/future.hpp>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/resource_view/resource_view.h"
#include "common/state_machine/instance_control_view.h"
#include "common/status/status.h"
#include "local_scheduler/instance_control/idle/idle_mgr.h"
#include "local_scheduler/pressure_monitor/cpu_upgrade_ladder.h"
#include "local_scheduler/pressure_monitor/upgrade_ladder.h"
#include "local_scheduler/snap_ctrl/snap_ctrl.h"

namespace functionsystem::local_scheduler {

/** Runtime knobs for PressureMonitorActor (mirrors the driver flags). */
struct PressureMonitorConfig {
    uint32_t checkIntervalMs = 5000;
    double highWatermark = 0.85;
    double lowWatermark = 0.70;
    uint32_t sustainSamples = 2;
    uint32_t parkTtlSec = 86400; // override ckpt manager's 1800s default
    uint32_t maxParked = 8;
    // W-CPUL: the monitor hosts the ladders too; park/unpark decisions stay
    // gated on the park feature alone (ladder-only nodes never park)
    bool enablePark = true;
    UpgradeLadderConfig upgradeLadder;
    std::string upgradeLadderCgroupRoot; // pool root for the ladder's writes
    CpuUpgradeLadderConfig cpuUpgradeLadder;
    std::string cpuUpgradeLadderCgroupRoot; // same pool root, cpu.stat/cpu.max files
};

/**
 * PressureMonitorActor drives watermark-based park/unpark on top of the
 * signal-18/19 snapshot primitives (W2 step 5):
 *
 * - every checkIntervalMs it samples the node resource view's actualuse vs
 *   capacity (Memory, MB);
 * - actualuse/capacity >= highWatermark for `sustainSamples` consecutive
 *   samples -> park ONE victim: a fully-idle local RUNNING instance
 *   (traffic idle AND no exec sessions, per IdleActor; in-flight tunnel
 *   traffic -- e.g. a pending LLM request -- disqualifies), largest
 *   per-instance actualuse first;
 * - actualuse/capacity <= lowWatermark -> unpark the OLDEST parked instance
 *   (FIFO fallback; response-ready wake via INSTANCE_WAKE_SNAPSHOT_SIGNAL
 *   takes priority externally and pops its own entry);
 * - parked population is capped at maxParked.
 *
 * Parks go through SnapCtrl::HandleSnapshot with leaveRunning=false and an
 * explicit ttl (parkTtlSec), so each parked instance is registered in
 * SnapCtrl's parked registry for wake/FIFO unpark.
 *
 * Cycle chaining: cross-actor reads are chained with Async back onto this
 * actor (idle set -> resource view -> parked registry -> decision), and the
 * next cycle is rescheduled first so one slow hop cannot stall the cadence.
 */
class PressureMonitorActor : public BasisActor {
public:
    PressureMonitorActor(const std::string &name, const std::string &nodeID, const PressureMonitorConfig &config);
    ~PressureMonitorActor() override = default;

    void Init() override;

    void BindInstanceControlView(const std::shared_ptr<InstanceControlView> &view)
    {
        instanceControlView_ = view;
    }

    void BindResourceView(const std::shared_ptr<resource_view::ResourceView> &view)
    {
        resourceView_ = view;
    }

    void BindSnapCtrl(const std::shared_ptr<SnapCtrl> &snapCtrl)
    {
        snapCtrl_ = snapCtrl;
    }

    void BindIdleMgr(const std::shared_ptr<IdleMgr> &idleMgr)
    {
        idleMgr_ = idleMgr;
    }

    /**
     * D-2/D-3 event entry: a sandbox crossed its memory.high soft limit
     * (PressureEventWatcher, off its own thread — reach this only via
     * litebus::Async). Runs one sampling cycle immediately instead of waiting
     * for the next polling tick; the throttle event also feeds the D-3
     * upgrade ladder inside the sampling chain (see OnResourceView). The
     * sustain-sample debounce still applies inside Decide, and the polling
     * RunCycle keeps running as the fallback.
     */
    void NotifyPressureEvent(std::string poolDir, uint64_t increment)
    {
        pendingEventDir_ = std::move(poolDir);
        pendingEventIncrement_ = increment;
        SampleNow();
    }

    /**
     * W-CPUL event entry: a sandbox's cpu.stat nr_throttled grew (second
     * PressureEventWatcher instance, off its own thread — Async only). Same
     * immediate-sampling contract as NotifyPressureEvent; feeds the CPU
     * upgrade ladder. The CPU ladder is cgroupfs-self-contained, so its tick
     * does not depend on the resource view's memory sample being present.
     */
    void NotifyCpuPressureEvent(std::string poolDir, uint64_t increment)
    {
        pendingCpuEventDir_ = std::move(poolDir);
        pendingCpuEventIncrement_ = increment;
        SampleNow();
    }

private:
    /** Kick off one sampling cycle; reschedules itself via AsyncAfter. */
    void RunCycle();

    /** One sampling cycle without the reschedule (shared by poll and event). */
    void SampleNow();

    /** Stage 1: store the idle set, fetch the node resource view. */
    void OnIdleSet(const std::vector<std::string> &idleInstances);

    /** Stage 2: compute the ratio, fetch the parked registry. */
    void OnResourceView(const std::shared_ptr<resource_view::ResourceUnit> &view);

    /** Stage 3: park/unpark decision with all three inputs at hand. */
    void Decide(const std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> &parked);

    /** Park one victim via SnapCtrl (leaveRunning=false + explicit ttl). */
    void ParkVictim(const std::string &instanceID, double instanceUseMb, int32_t priority);

    /** FIFO fallback: wake the oldest parked instance. */
    void UnparkOldest(const std::pair<std::string, SnapCtrlActor::ParkedEntry> &oldest);

    /** W10-1: a failed wake must retry soon, not ride the full storm cooldown. */
    void OnWakeFailed(const std::string &instanceID);

    std::string nodeID_;
    PressureMonitorConfig config_;

    std::shared_ptr<InstanceControlView> instanceControlView_;
    std::shared_ptr<resource_view::ResourceView> resourceView_;
    std::shared_ptr<SnapCtrl> snapCtrl_;
    std::shared_ptr<IdleMgr> idleMgr_;
    std::unique_ptr<UpgradeLadder> upgradeLadder_; // D-3, null unless enabled
    std::unique_ptr<CpuUpgradeLadder> cpuUpgradeLadder_; // W-CPUL, null unless enabled

    // cycle state (updated in order: OnIdleSet -> OnResourceView -> Decide)
    std::unordered_set<std::string> idleInstances_;
    double lastRatio_ = -1.0; // <0 = no usable sample this cycle
    std::unordered_map<std::string, double> lastInstanceUseMb_;

    // throttle event carried across the sampling chain (cleared once consumed)
    std::string pendingEventDir_;
    uint64_t pendingEventIncrement_ = 0;

    // cpu throttle event carried across the sampling chain (W-CPUL)
    std::string pendingCpuEventDir_;
    uint64_t pendingCpuEventIncrement_ = 0;

    uint32_t highSamples_ = 0; // consecutive samples at/above the high mark
    uint64_t requestSeq_ = 0;  // requestID sequence for pressure-* tracing

    // Wake cooldown (D-4): restores are async and slow; re-issuing HandleWake
    // for the same still-parked instance every cycle piles up restore-staging
    // entries until the underlayer scheduler stops answering (seen live: a
    // 24G checkpoint that can never restore in time spawned ~90 concurrent
    // SnapStarts and froze every other restore on the node). An instance that
    // just got a wake request is skipped for this many cycles (5s each).
    static constexpr uint32_t kWakeCooldownCycles = 60;
    // W10-1: a FAILED wake (e.g. stale state machine, admission transient) is
    // retried after this short backoff, not the full storm cooldown -- with 60
    // cycles (300s) a failed unpark looked permanent at run scale (W9-2 v4:
    // 2/5 wakes failed "retry later" and never ran again before phase end).
    static constexpr uint32_t kWakeRetryCycles = 2;
    std::unordered_map<std::string, uint32_t> wakeCooldown_;
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_PRESSURE_MONITOR_ACTOR_H
