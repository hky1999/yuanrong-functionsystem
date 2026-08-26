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

#ifndef LOCAL_SCHEDULER_IDLE_ACTOR_H
#define LOCAL_SCHEDULER_IDLE_ACTOR_H

#include <atomic>
#include <cstdint>
#include <actor/actor.hpp>
#include <async/future.hpp>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/state_machine/instance_control_view.h"
#include "common/utils/actor_driver.h"

namespace functionsystem::local_scheduler {

/**
 * IdleActor manages per-instance idle timeout logic.
 *
 * It tracks traffic idle state and exec session counts, starting/cancelling
 * timers accordingly. When an instance has been idle (no traffic, no active
 * sessions) for longer than its configured timeout, IdleActor sends
 * EvictByIdleTimeout to InstanceCtrlActor for the authoritative eviction
 * decision.
 *
 * Anti-race mechanism: each timer invocation is stamped with a generation
 * counter. CancelIdleTimer increments the generation before cancelling, so
 * any already-queued timeout callback will detect the stale generation and
 * return without requesting eviction.
 */
class IdleActor : public BasisActor {
public:
    /**
     * @param name                Actor name (nodeID + postfix)
     * @param nodeID              Node identifier for ownership checks
     * @param instanceControlView Shared view for reading instance info (read-only)
     * @param facadeAID           AID of InstanceCtrlActor for eviction callbacks
     */
    IdleActor(const std::string &name,
              const std::string &nodeID,
              const std::shared_ptr<InstanceControlView> &instanceControlView,
              const litebus::AID &facadeAID);

    ~IdleActor() override = default;

    void Init() override;
    void Finalize() override;

    /**
     * Report traffic state for an instance.
     * Called when the control plane observer detects a change in in-flight request count.
     *
     * @param instanceID     Instance identifier
     * @param processingNum  Current number of in-flight requests (0 = idle)
     */
    void TrafficReport(const std::string &instanceID, const size_t &processingNum);

    /**
     * Update exec session count for an instance.
     * Called when exec sessions start or end via ExecStream.
     *
     * @param instanceID  Instance identifier
     * @param delta       Session count change (+N or -N)
     */
    void SessionCountDelta(const std::string &instanceID, int delta);

    /**
     * Reconcile idle timer state after an instance reaches RUNNING.
     * This compensates for an initial idle traffic report that arrived before
     * the local state machine was visible as RUNNING.
     *
     * @param instanceID  Instance identifier
     */
    void OnInstanceRunning(const std::string &instanceID);

    /**
     * Mark an instance as ever-used because a client-originated data-plane
     * action (exec / filesystem / pty, as opposed to the create-flow's
     * lifecycle probes ping/get_info) was dispatched to it. SDK exec rides
     * the Invoke channel and never reaches ExecStreamService, so
     * SessionCountDelta alone cannot claim ownership for it. If an orphan
     * grace timer is currently armed, it is re-armed at the full idle
     * timeout: a used instance must not be reclaimed on the short window.
     *
     * @param instanceID  Instance identifier
     */
    void MarkInstanceUsed(const std::string &instanceID);

    /**
     * Lifecycle hook from SnapCtrlActor when a leaveRunning=false snapshot
     * (pressure park) completes. The instance is deleted but its ID will be
     * REUSED by the restore (SnapshotScheduler::BuildScheduleRequest), so all
     * per-ID bookkeeping must be dropped now: an idle timer armed before the
     * park would otherwise fire against the restored instance on the old
     * deadline (the parked/restore-in-flight kill, D-4 finding F1) or block
     * the fresh timer from being armed at all (StartIdleTimer no-ops while a
     * stale entry lingers in idleTimers_).
     *
     * @param instanceID  Instance identifier of the parked instance
     */
    void OnInstanceParked(const std::string &instanceID);

    /**
     * Copy of the currently fully-idle instance set: traffic idle AND no
     * active exec sessions. Actor-executed so the two maps are read
     * consistently; used by the pressure monitor as the park
     * victim-eligibility filter (an instance with in-flight tunnel/exec
     * traffic is never a victim).
     */
    std::vector<std::string> GetIdleInstances();

    // set once by the local scheduler driver before the actor is spawned;
    // 0 keeps the legacy behavior (no special orphan window).
    static void SetOrphanGraceSec(int64_t graceSec);
    static int64_t GetOrphanGraceSec();

private:
    void SessionAlive(const std::string &instanceID, bool hasActiveSessions);
    void StartIdleTimer(const std::string &instanceID);
    void CancelIdleTimer(const std::string &instanceID);

    /**
     * Timer callback. generation is compared against instanceTimerGeneration_[instanceID]
     * to detect stale callbacks that were queued after CancelIdleTimer incremented the counter.
     */
    void HandleIdleTimeout(const std::string &instanceID, uint64_t generation);

    std::string nodeID_;
    std::shared_ptr<InstanceControlView> instanceControlView_;
    litebus::AID facadeAID_;

    std::unordered_map<std::string, litebus::Timer> idleTimers_;

    // Generation counter per instance. Incremented by CancelIdleTimer to invalidate
    // any in-flight timeout callbacks already queued in this actor's mailbox.
    std::unordered_map<std::string, uint64_t> instanceTimerGeneration_;

    // Traffic idle state: true if processingNum == 0
    std::unordered_map<std::string, bool> instanceTrafficIdle_;

    // Session active flag: true if at least one exec session is active
    std::unordered_map<std::string, bool> instanceActiveSessions_;

    // Per-instance session counts for timer management decisions.
    std::unordered_map<std::string, size_t> instanceSessionCounts_;

    // Instances that have ever shown real use (an exec session or busy
    // traffic). A never-used instance is a create-race orphan candidate:
    // the client gave up (or crashed) before the first exec landed, so the
    // orphan grace window applies instead of the full idle timeout.
    std::unordered_map<std::string, bool> instanceEverUsed_;

    // Orphan grace window in seconds (0 = disabled); see SetOrphanGraceSec.
    static std::atomic<int64_t> orphanGraceSec_;
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_IDLE_ACTOR_H
