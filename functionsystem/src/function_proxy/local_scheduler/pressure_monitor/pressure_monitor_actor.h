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
#include "memory_elasticity_policy.h"
#include "common/resource_view/resource_sample_freshness.h"
#include "common/state_machine/instance_control_view.h"
#include "common/status/status.h"
#include "local_scheduler/instance_control/idle/idle_mgr.h"
#include "local_scheduler/function_agent_manager/function_agent_mgr.h"
#include "local_scheduler/pressure_monitor/cpu_upgrade_ladder.h"
#include "local_scheduler/pressure_monitor/upgrade_ladder.h"
#include "local_scheduler/pressure_monitor/pressure_event_queue.h"
#include "local_scheduler/pressure_monitor/pressure_sample_gate.h"
#include "local_scheduler/pressure_monitor/physical_llm_client.h"
#include "local_scheduler/snap_ctrl/snap_ctrl.h"

namespace functionsystem::local_scheduler {

/** Runtime knobs for PressureMonitorActor (mirrors the driver flags). */
struct PressureMonitorConfig {
    // Park/ratio cadence: UNCHANGED (5000ms x sustainSamples semantics).
    uint32_t checkIntervalMs = 5000;
    // Elastic-policy consumption cadence: MemoryElasticityPolicy::Observe
    // requires ADJACENT CONSUMED samples <=600ms apart to accumulate the 5s
    // low-usage window; a 5000ms consumer never satisfied it, so elastic
    // sandboxes never shrank (pc-elastic-4 root cause #1; 1631Z experiment:
    // 200/500ms cadences shrink after 5s, 1000/5000ms never do). 200ms
    // matches the Stats producer contract. Only scheduled when memory
    // elasticity is enabled — the legacy path keeps its single 5000ms loop.
    uint32_t elasticIntervalMs = 200;
    double highWatermark = 0.85;
    double lowWatermark = 0.70;
    uint32_t sustainSamples = 2;
    uint32_t parkTtlSec = 86400; // override ckpt manager's 1800s default
    uint32_t maxParked = 8;
    // Stats polls each second; reject stale observations using the shared policy.
    int64_t resourceSampleMaxAgeMs = resource_view::DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS;
    double maxIdleCpuCores = 0.02;
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
 * SnapCtrl's parked registry for wake/FIFO unpark. Parking is additionally
 * gated on a physical LLM hold acquired from the bound PhysicalLLMClient:
 * the proxy must own the victim's LLM delivery before the checkpoint is
 * dispatched, and the hold token rides in the SnapOptions into the parked
 * registry entry. Without a bound client the park path fails closed (the
 * historical direct-snapshot park is never used as a fallback).
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

    void BindFunctionAgentMgr(const std::shared_ptr<FunctionAgentMgr> &manager) { functionAgentMgr_ = manager; }

    void BindIdleMgr(const std::shared_ptr<IdleMgr> &idleMgr)
    {
        idleMgr_ = idleMgr;
    }

    /**
     * Bind the physical LLM hold client (proxy /hold/physical endpoints).
     * Without a bound client the pressure-park path fails closed: the proxy
     * must own the victim's LLM delivery before any checkpoint is issued, so
     * the historical direct-snapshot park is never used as a fallback.
     */
    void BindPhysicalLLMClient(const std::shared_ptr<PhysicalLLMClient> &client)
    {
        physicalLLMClient_ = client;
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
        pendingMemoryEvents_.Push(poolDir, increment);
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
        pendingCpuEvents_.Push(poolDir, increment);
        SampleNow();
    }

private:
    /** Kick off one sampling cycle; reschedules itself via AsyncAfter. */
    void RunCycle();
    // fast tick (elasticIntervalMs): feeds ONLY the elasticity policy via
    // its OWN single-flight gate and timeout; park/ratio accounting and the
    // IdleMgr/parked-table hops stay exclusively on the 5000ms RunCycle
    void RunElasticCycle();
    void OnElasticSampleTimeout(uint64_t generation);
    void OnElasticResourceView(uint64_t generation,
                               const std::shared_ptr<resource_view::ResourceUnit> &view);

    /** One sampling cycle without the reschedule (shared by poll and event). */
    void SampleNow();

    /** Stage 1: store the idle set, fetch the node resource view. */
    void OnIdleSet(uint64_t generation, const std::vector<std::string> &idleInstances);

    /** Stage 2: compute the ratio, fetch the parked registry. */
    void OnResourceView(uint64_t generation, const std::shared_ptr<resource_view::ResourceUnit> &view);

    /** Stage 3: park/unpark decision with all three inputs at hand. */
    void Decide(uint64_t generation, const std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> &parked);

    void OnSampleTimeout(uint64_t generation);
    void ResizeUnderPressure(const resource_view::ResourceUnit &view, int64_t nowMs);
    void OnPressureResize(const std::string &instanceID, const std::string &requestID,
                          const litebus::Future<messages::ResizeSandboxResponse> &result);
    std::shared_ptr<FunctionAgentMgr> functionAgentMgr_;
    struct PressureResizeAttempt {
        std::string requestID;
        int64_t dispatchedAtMs;
        messages::ResizeSandboxRequest request;
        bool unknown = false;
    };
    std::unordered_map<std::string, PressureResizeAttempt> pressureResizes_;
    std::unordered_map<std::string, resources::SandboxResourceSample> resizeSamples_;
    std::unordered_map<std::string, int64_t> resizeAfterMs_;


    /**
     * Park one victim: acquire a physical LLM hold on the proxy, re-verify
     * eligibility against fresh observations, then hand the park to SnapCtrl
     * (leaveRunning=false, explicit ttl, hold token). Fails closed when no
     * PhysicalLLMClient is bound or the preconditions cannot be re-proven.
     */
    void ParkVictim(const std::string &instanceID, double instanceUseMb, int32_t priority);

    /** Prepare outcome of one park attempt; re-checks the attempt UUID first. */
    void OnParkPrepared(const std::string &instanceID, const std::string &attemptID,
                        const litebus::Future<PhysicalHoldResult> &result);

    /** Fresh idle-set + eligibility recheck after READY; dispatches or rolls the hold back. */
    void OnParkIdleChecked(const std::string &instanceID, const std::string &attemptID,
                           const litebus::Future<std::vector<std::string>> &idleInstances);

    /** KillResponse of the dispatched park; erases the record only on success. */
    void OnParkSnapshotDone(const std::string &instanceID, const std::string &attemptID,
                            const litebus::Future<KillResponse> &rsp);

    /** Roll the captured hold back (pre-dispatch only, exact identity + own token). */
    void BeginParkRelease(const std::string &instanceID, const std::string &attemptID);

    /** Release confirmation; erases the record only on RELEASED. */
    void OnParkReleased(const std::string &instanceID, const std::string &attemptID,
                        const litebus::Future<PhysicalHoldResult> &result);

    /**
     * One in-flight physical-hold park preparation. Lives from the Prepare
     * dispatch until the hold is confirmed released or the park is accepted;
     * UNKNOWN outcomes deliberately retain the entry — no timer may assume a
     * missing write never happened.
     */
    struct ParkPreparation {
        enum class Stage { PREPARING, READY, SNAPSHOT, RELEASING };
        std::string attemptID;                       // private UUID per attempt
        PhysicalLLMIdentity identity;                // instance/sandbox/runtime/generation at Prepare time
        std::shared_ptr<InstanceStateMachine> source; // exact state machine rechecks must see again
        std::string token;                           // owned hold token (set on HELD/READY)
        int64_t observedAtMs = 0;                    // LLM quiescence observation time (READY only)
        double useMb = 0.0;                          // logging only
        int32_t priority = 0;                        // logging only
        Stage stage = Stage::PREPARING;
    };
    static constexpr size_t kMaxParkPreparations = 1024;

    std::shared_ptr<PhysicalLLMClient> physicalLLMClient_;
    std::unordered_map<std::string, ParkPreparation> parkPreparations_;

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

    PressureSampleGate sampleGate_;

    // cycle state (updated in order: OnIdleSet -> OnResourceView -> Decide)
    std::unordered_set<std::string> idleInstances_;
    int64_t lastNodeMemorySampleMs_ = 0;
    double lastRatio_ = -1.0; // <0 = no usable sample this cycle
    std::unordered_map<std::string, resources::SandboxResourceSample> lastInstanceSamples_;

    // throttle event carried across the sampling chain (cleared once consumed)
    std::unordered_map<std::string, MemoryElasticityPolicy> memoryPolicies_;
    PressureEventQueue pendingMemoryEvents_;

    // cpu throttle event carried across the sampling chain (W-CPUL)
    PressureEventQueue pendingCpuEvents_;

    uint32_t highSamples_ = 0; // consecutive samples at/above the high mark
    // The fast path's OWN single-flight gate: interleaving with the slow
    // loop's sampleGate_ never blocks either side (a hanging IdleMgr on the
    // slow path cannot stall the 200ms policy consumption and vice versa).
    PressureSampleGate elasticGate_;
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
