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

#ifndef LOCAL_SCHEDULER_SNAP_CTRL_ACTOR_H
#define LOCAL_SCHEDULER_SNAP_CTRL_ACTOR_H

#include <actor/actor.hpp>
#include <async/future.hpp>
#include <chrono>
#include <functional>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "snapshot_attempt_gate.h"
#include "common/logs/logging.h"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix_pb.h"
#include "common/schedule_decision/scheduler_common.h"
#include "common/state_machine/instance_control_view.h"
#include "common/state_machine/instance_state_machine.h"
#include "common/status/status.h"
#include "function_proxy/common/posix_client/control_plane_client/control_interface_client_manager_proxy.h"
#include "local_scheduler/function_agent_manager/function_agent_mgr_actor.h"
#include "local_scheduler/instance_control/instance_ctrl.h"
#include "local_scheduler/local_scheduler_service/local_sched_srv.h"

namespace functionsystem::local_scheduler {

class FunctionAgentMgr;
class LocalSchedSrv;
class InstanceCtrlActor;
class IdleMgr;

class SnapCtrlActor : public BasisActor {
public:
    SnapCtrlActor(const std::string &name, const std::string &nodeID);
    ~SnapCtrlActor() override = default;

    void Init() override;

    // Direct-restore logical-context bounds (process-local, payload budget
    // not RSS; entries are retained for the process lifetime — capacity
    // retirement and restart recovery remain open). The reservation counts
    // every RETAINED copy: context strings, restoreContexts_ key,
    // restoringCheckpoints_ key/value, the settlement callback's captured
    // requestID/nonce, the captured registration list and the bounded
    // response. Transient relay-request serialization is not retained.
    static constexpr size_t kMaxRestoreContexts = 4096;
    static constexpr size_t kMaxRestoreContextBytes = 64ULL * 1024 * 1024;
    static constexpr size_t kMaxRestoreRequestIDBytes = 1024;
    static constexpr size_t kMaxRestoreCheckpointIDBytes = 4096;
    static constexpr size_t kMaxRestorePayloadBytes = 64 * 1024;
    static constexpr size_t kMaxRestoreResponseBytes = 64 * 1024;

    /**
     * Handle INSTANCE_SNAPSHOT_SIGNAL
     * Create a snapshot of the running instance
     * @param requestID: Request ID for tracing
     * @param instanceID: ID of the instance to snapshot
     * @param payload: core_service::SnapOptions payload
     * @return KillResponse containing snapshot info payload
     */
    litebus::Future<KillResponse> HandleSnapshot(const std::string &requestID,
                                                  const std::string &instanceID,
                                                  const std::string &payload);

    /**
     * Handle INSTANCE_SNAPSHOT_SIGNAL with the caller's captured source state
     * machine pinned. Every check (non-null source, exact current machine,
     * RUNNING, payload parses with a complete expected tuple whose
     * expected_runtime_id matches the source) precedes any park/drain/gate
     * mutation; on success HandleSnapshot runs in the same actor turn.
     * @param requestID: Request ID for tracing
     * @param instanceID: ID of the instance to snapshot
     * @param payload: core_service::SnapOptions payload with expected tuple
     * @param source: the caller's originally captured InstanceStateMachine
     * @return Future of KillResponse
     */
    litebus::Future<KillResponse> HandleSnapshotIfSource(const std::string &requestID,
                                                         const std::string &instanceID,
                                                         const std::string &payload,
                                                         const std::shared_ptr<InstanceStateMachine> &source);

    /**
     * Callback to convert SnapshotResult to KillResponse. Also maintains the
     * parked registry: a successful leaveRunning=false snapshot parks the
     * instance (checkpoint + delete), so it is recorded for wake/FIFO unpark.
     * @param instanceID: ID of the snapshotted instance (registry key)
     * @param leaveRunning: false = the instance was parked by this snapshot
     * @param result: The snapshot result
     * @return KillResponse with appropriate code and payload
     */
    KillResponse OnHandleSnapshot(const std::string &instanceID, bool leaveRunning, const SnapshotResult &result, const std::string &holdToken = "", uint64_t snapshotAttempt = 0);

    /**
     * Handle INSTANCE_SNAPSTART_SIGNAL
     * Restore an instance from a snapshot
     * @param requestID: Request ID for tracing
     * @param checkpointID: The checkpoint ID to restore from
     * @param payload: core_service::SnapStartOptions payload
     * @return KillResponse with restore result
     */
    litebus::Future<KillResponse> HandleSnapStart(const std::string &requestID,
                                                   const std::string &checkpointID,
                                                   const std::string &payload);

    /**
     * Handle INSTANCE_WAKE_SNAPSHOT_SIGNAL: wake one parked instance by its
     * ORIGINAL instanceID. Looks the instance up in the parked registry and
     * SnapStarts it from the recorded checkpoint; the registry entry is
     * dropped once the restore succeeds (the restored instance re-uses the
     * original instanceID, see SnapshotScheduler::BuildScheduleRequest).
     * @param requestID: Request ID for tracing
     * @param instanceID: ORIGINAL instanceID of the parked instance
     * @return KillResponse with restore result
     */
    litebus::Future<KillResponse> HandleWake(const std::string &requestID, const std::string &instanceID);

    litebus::Future<KillResponse> HandleDirectedWake(const std::string &requestID,
        const std::string &instanceID, const std::string &payload);

    /** Registry entry for one parked instance. */
    struct ParkedEntry {
        std::string checkpointID;
        // Completion fence: a fresh UUID minted per park registration. An
        // async restore completion may clear this entry only by matching this
        // exact identity, never by scanning the checkpoint. Internal
        // bookkeeping only: never exposed in payloads or logs.
        std::string registrationID;
        std::chrono::steady_clock::time_point parkedAt;
        // error-outcome wake completions observed so far (saturating).
        // Diagnostic only: no threshold drops the entry — a count of errors
        // is not physical retirement evidence.
        uint32_t wakeFails = 0;
        // W9: a wake for this entry is IN FLIGHT. The restore chain can take
        // minutes and its response may time out and retry at the localSchedSrv
        // layer (W8 v7 log: the monitor re-picked the same entry after its
        // 300s cooldown while the first wake's deploy chain was still retrying,
        // producing a duplicate restore and a replay-conflict failure). Entries
        // with a live wake are skipped by the picker; reset only when it settles.
        bool waking = false;
        // F8 phase 1: the owned wake dispatch settled with a non-success code,
        // so the physical restore outcome is UNKNOWN. The entry is retained
        // with its full identity (checkpoint/registration/source/hold token),
        // excluded from new wakes and the FIFO picker, and never auto-retried
        // or dropped. Authoritative physical-result reconciliation and restart
        // recovery are pending and will be implemented separately.
        bool wakeUnknown = false;
        uint64_t wakeAttempt = 0;
        std::string sourceSandboxID;
        std::string sourceRuntimeID;
        std::string sourceGeneration;
        std::string holdToken;
    };

    /**
     * Copy of the parked registry (actor-executed) for the pressure
     * monitor's FIFO fallback unpark.
     */
    std::vector<std::pair<std::string, ParkedEntry>> GetParkedInstances();

    /**
     * Handle snapstart instance initialization after state transition to CREATING
     * Complete flow: DeployInstance -> CreateInstanceClient -> StartHeartbeat
     *                -> SnapStarted -> TransInstanceState(RUNNING) -> SetValue
     * @param scheduleResp: Promise to return schedule response
     * @param scheduleReq: The schedule request for the restored instance
     * @param result: Schedule result from scheduler
     * @param transResult: Transition result from state transition to CREATING
     * @return Option of TransitionResult
     */
    void SnapStart(
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
        const schedule_decision::ScheduleResult &result,
        const TransitionResult &transResult);

    /**
     * Bind the FunctionAgentMgr for sending snapshot requests to agents
     * @param functionAgentMgr: The function agent manager interface
     */
    void BindFunctionAgentMgr(const std::shared_ptr<FunctionAgentMgr> &functionAgentMgr)
    {
        functionAgentMgr_ = functionAgentMgr;
    }

    /**
     * Bind the LocalSchedSrv for recording snapshot metadata
     * @param localSchedSrv: The local scheduler service interface
     */
    void BindLocalSchedSrv(const std::shared_ptr<LocalSchedSrv> &localSchedSrv)
    {
        localSchedSrv_ = localSchedSrv;
    }

    /**
     * Bind the InstanceControlView for accessing instance state machines
     * @param instanceControlView: The instance control view
     */
    void BindInstanceControlView(const std::shared_ptr<InstanceControlView> &instanceControlView)
    {
        instanceControlView_ = instanceControlView;
    }

    /**
     * Bind the ControlInterfaceClientManagerProxy for accessing instance clients
     * @param clientManager: The client manager
     */
    void BindClientManager(const std::shared_ptr<ControlInterfaceClientManagerProxy> &clientManager)
    {
        clientManager_ = clientManager;
    }

    /**
     * Bind the InstanceCtrlActor for deleting instances
     * @param instanceCtrl: The instance control actor
     */
    void BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl)
    {
        instanceCtrl_ = instanceCtrl;
    }

    /**
     * Bind the IdleMgr so a successful park can clear the instance's idle
     * bookkeeping before its ID is reused by the restore (D-5 F1).
     * @param idleMgr: The idle subsystem interface
     */
    void BindIdleMgr(const std::shared_ptr<IdleMgr> &idleMgr)
    {
        idleMgr_ = idleMgr;
    }

    /**
     * Bind the per-instance reverse-tunnel admission gate for the PUBLIC
     * reusable-snapshot chain (type=SNAPSHOT). The acquire callback returns
     * false when a tunnel is active; a successful acquire is paired with
     * exactly one release after the reusable Snapshot operation reaches a
     * terminal response. Restored from the pre-G3b public chain: the driver
     * has wired this hook all along, but the sink was a no-op after the
     * actor-side chain was dropped.
     */
    void BindReusableSnapshotTunnelGate(
        std::function<bool(const std::string &)> acquire,
        std::function<void(const std::string &)> release)
    {
        reusableSnapshotTunnelGateAcquire_ = std::move(acquire);
        reusableSnapshotTunnelGateRelease_ = std::move(release);
    }

private:
    // ---- PUBLIC reusable named-snapshot chain (type=SNAPSHOT), restored
    // verbatim from the pre-G3b canonical actor (d01e1328^): Begin ->
    // READY-resolve | PUBLISHING -> PrepareSnap -> typed SnapshotRuntime ->
    // SnapStarted -> PublishSnapshotArtifact -> Commit -> exact-cleanup
    // Finalize, with Fail coordination and an idempotent-replay-preserving
    // error contract. This is the public wire contract the frontend/SDK
    // encode (name=5/checkpointTimeoutMs=6); it never falls back to the
    // anonymous RecordSnapshotMetadata path.
    struct ReusableSnapshotContext {
        std::string requestID;
        std::string instanceID;
        std::string snapshotID;
        std::string name;
        resources::InstanceInfo sourceInstanceInfo;
        ::messages::SnapshotArtifact artifact;
        ::core_service::SnapshotInfo publicInfo;
        std::shared_ptr<litebus::Promise<KillResponse>> completion;
        uint64_t snapshotAttempt{ 0 };  // shared per-instance single-flight slot
        bool tunnelGateHeld{ false };
        bool completed{ false };
        // terminal response delivered, but the PHYSICAL outcome is only
        // authoritative on the known-success/definite-failure paths; lost
        // replies and resultunknown/incomplete coordination leave it false
        // so the shared slot stays busy with a same-requestID replay entrance
        bool outcomeKnown{ false };
        // this request re-drives a previously UNKNOWN physical outcome:
        // pre-physical/comm failures must NOT clear the unknown then
        bool replayOfUnknown{ false };
    };

    /** Bounded re-query entrance for one UNKNOWN public-snapshot outcome. */
    struct UnknownReusableEntry {
        std::string requestID;
        uint64_t attempt;
        std::string runtimeID;  // original source identity fence for replay
        bool replaying{ false };  // single-flight for the re-query itself
    };

    litebus::Future<KillResponse> HandleReusableSnapshot(
        const std::string &requestID, const std::string &instanceID,
        const resources::InstanceInfo &instanceInfo, const SnapOptions &options,
        uint64_t snapshotAttempt = 0);
    void OnReusableSnapshotBegun(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        const litebus::Future<::messages::BeginReusableSnapshotResponse> &future);
    void OnReusableSnapshotResolvedForCleanup(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        const litebus::Future<::messages::ResolveReusableSnapshotForCreateResponse> &future);
    void OnReusableSnapshotPrepared(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        const litebus::Future<Status> &future);
    void OnReusableSnapshotCheckpointed(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        const litebus::Future<messages::SnapshotRuntimeResponse> &future);
    void OnReusableSnapshotPublished(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        const litebus::Future<messages::SnapshotRuntimeResponse> &future);
    void OnReusableSnapshotClient(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &future);
    void OnReusableSnapshotStarted(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        const litebus::Future<runtime::SnapStartedResponse> &future);
    void OnReusableSnapshotCommitted(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        const litebus::Future<::messages::CommitReusableSnapshotResponse> &future);
    void OnReusableSnapshotFinalized(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        ::messages::SnapshotAttemptFinalizeOperation operation,
        common::ErrorCode terminalCode, const std::string &terminalMessage,
        const litebus::Future<::messages::SnapshotAttemptFinalizeResponse> &future);
    void OnReusableSnapshotFailedRecord(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        common::ErrorCode terminalCode, const std::string &terminalMessage,
        const litebus::Future<::messages::FailReusableSnapshotResponse> &future);
    void FailReusableSnapshot(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        common::ErrorCode code, const std::string &message);
    void FinalizeReusableSnapshot(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        ::messages::SnapshotAttemptFinalizeOperation operation,
        common::ErrorCode terminalCode, const std::string &terminalMessage);
    void CompleteReusableSnapshotRequest(
        const std::shared_ptr<ReusableSnapshotContext> &context,
        const KillResponse &response);
    KillResponse BuildReusableSnapshotResponse(
        common::ErrorCode code, const std::string &message,
        const ::core_service::SnapshotInfo *snapshotInfo = nullptr) const;
    void ReleaseReusableSnapshotTunnelGate(
        const std::shared_ptr<ReusableSnapshotContext> &context);

    /**
     * Exact-pointer source fence for the conditional snapshot path: the
     * originally captured state machine must still be the instance's current
     * machine, RUNNING, on the same runtime. A replacement machine's newer
     * identity is never adopted here.
     * @param instanceID: ID of the instance
     * @param source: the originally captured state machine
     * @param runtimeID: the runtime identity captured with the source
     * @return true only when the exact captured source still owns the instance
     */
    bool SnapshotSourceMatches(const std::string &instanceID,
                               const std::shared_ptr<InstanceStateMachine> &source,
                               const std::string &runtimeID);

    /**
     * Prepare snapshot by calling runtime PrepareSnap interface
     * @param requestID: Request ID for tracing
     * @param instanceID: ID of the instance
     * @param capturedSource: conditional path's captured state machine; the
     *        source is re-fenced on the actor after the client lookup
     * @param capturedRuntimeID: runtime identity captured with the source
     * @return Status of preparation
     */
    litebus::Future<Status> PrepareSnap(const std::string &requestID, const std::string &instanceID,
        const std::shared_ptr<InstanceStateMachine> &capturedSource = nullptr,
        const std::string &capturedRuntimeID = "",
        const std::shared_ptr<std::atomic<bool>> &sourceMismatched = nullptr);

    /**
     * Handle DeploySnapStartInstance completion
     * @param scheduleResp: Promise to return schedule response
     * @param scheduleReq: The schedule request
     * @param deployFuture: Deploy response future
     */
    void OnDeploySnapStartInstanceComplete(
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
        const litebus::Future<messages::DeployInstanceResponse> &deployFuture);

    /**
     * Handle CreateInstanceClient completion
     * @param scheduleResp: Promise to return schedule response
     * @param scheduleReq: The schedule request with updated runtime info
     * @param clientResult: Client future result
     */
    void OnCreateInstanceClientComplete(
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
        const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &clientResult);

    /**
     * Handle SnapStarted RPC completion
     * @param scheduleResp: Promise to return schedule response
     * @param scheduleReq: The schedule request
     * @param snapStartedResult: SnapStarted RPC result
     */
    void OnSnapStartedRpcComplete(
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
        const litebus::Future<runtime::SnapStartedResponse> &snapStartedResult);

    /**
     * Handle TransInstanceState completion
     * @param scheduleResp: Promise to return schedule response
     * @param scheduleReq: The schedule request
     * @param transResult: Transition result
     */
    void OnTransInstanceStateComplete(
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
        const litebus::Future<TransitionResult> &transResult);

    std::string nodeID_;

    std::shared_ptr<FunctionAgentMgr> functionAgentMgr_;
    std::shared_ptr<LocalSchedSrv> localSchedSrv_;
    std::shared_ptr<InstanceControlView> instanceControlView_;
    std::shared_ptr<ControlInterfaceClientManagerProxy> clientManager_;
    std::shared_ptr<InstanceCtrl> instanceCtrl_;
    std::shared_ptr<IdleMgr> idleMgr_;

    // reverse-tunnel admission gate for the PUBLIC reusable-snapshot chain
    std::function<bool(const std::string &)> reusableSnapshotTunnelGateAcquire_;
    std::function<void(const std::string &)> reusableSnapshotTunnelGateRelease_;

    // Instances parked on this node via leaveRunning=false snapshots:
    // original instanceID -> checkpoint. Enables wake-by-instanceID and
    // FIFO unpark without external bookkeeping. Entries are dropped when
    // the checkpoint is restored (HandleWake / direct SnapStart) or when
    // the instance re-appears in the control view.
    std::unordered_map<std::string, ParkedEntry> parkedInstances_;
    SnapshotAttemptGate snapshotAttempts_;
    std::unordered_map<std::string, UnknownReusableEntry> unknownReusable_;
    void FinishSnapshotAttempt(const std::string &instanceID, uint64_t attempt, bool markParked,
        const std::shared_ptr<std::atomic<bool>> &dispatched, const litebus::Future<KillResponse> &result,
        const std::shared_ptr<std::atomic<bool>> &sourceMismatched = nullptr);

    /** Drop a registry entry after a successful wake/restore. */
    void ForgetParked(const std::string &instanceID);

    /** (instanceID, registrationID) pinned before an async restore relay. */
    using ParkedRegistration = std::pair<std::string, std::string>;

    /** Exact registrations parked on checkpointID right now; bounded by the map. */
    std::vector<ParkedRegistration> CaptureParkedRegistrations(const std::string &checkpointID);

    /** Drop only entries whose current nonempty registration matches the capture. */
    void ForgetParkedRegistrations(const std::vector<ParkedRegistration> &captured);

    /** OnWakeComplete: registry bookkeeping, returns rsp unchanged. */
    KillResponse OnWakeComplete(const std::string &instanceID, const std::string &checkpointID,
                                uint64_t attempt, const KillResponse &rsp);

    /**
     * Registration fence around HandleWake's completion: delegates to
     * OnWakeComplete only when the captured registration is nonempty and is
     * still the entry's current one; a replacement park stays untouched.
     */
    KillResponse OnWakeCompleteIfCurrent(const std::string &instanceID, const std::string &checkpointID,
                                         uint64_t attempt, const std::string &registrationID,
                                         const KillResponse &rsp);

    // W10-2: checkpoints whose restore (SnapStart chain) is currently in
    // flight, bound to the owning context's private nonce; a concurrent
    // SnapStart for the same checkpoint is rejected so duplicate restores
    // cannot mint two sandboxes from one checkpoint.
    std::unordered_map<std::string, std::string> restoringCheckpoints_;

    /**
     * One logical direct-restore context per caller requestID: the immutable
     * checkpoint + exact payload, the first captured registrations, a private
     * nonce and the retained response promise. Identical retries share the
     * retained future; changed requests conflict; entries are never evicted
     * (process-local — retirement/restart recovery remain open).
     */
    struct RestoreContext {
        std::string checkpointID;
        std::string payload;
        std::vector<ParkedRegistration> captured;
        std::string nonce;
        litebus::Promise<KillResponse> promise;
        bool completed = false;
    };
    std::unordered_map<std::string, std::shared_ptr<RestoreContext>> restoreContexts_;
    size_t restoreContextBytes_ = 0;

    /**
     * Actor-thread settlement of one dispatched restore relay, posted once
     * for any outcome. Verifies the context nonce, the not-yet-completed
     * flag and its own active marker before any mutation; stale or duplicate
     * callbacks do nothing (including no marker removal).
     */
    void OnRestoreRelayComplete(const std::string &requestID, const std::string &nonce,
        const litebus::Future<messages::RestoreSnapshotResponse> &future);

    /** Mark exactly the captured registrations unresolved (identity kept,
     *  waking untouched so the owning wake completion settles it once). */
    void MarkCapturedRegistrationsUnknown(const std::vector<ParkedRegistration> &captured);

    /** Void wrapper so AsyncAfter can schedule a wake (HandleWake returns a
     *  Future); HandleWake owns the unresolved-outcome rejection. */
    void RetryWake(const std::string &instanceID);
    /** Queued-callback fence: re-wakes only the still-matching entry; an
     *  unresolved-outcome entry is never restarted by an old callback. */
    void RetryWakeIfCurrent(const std::string &instanceID, const std::string &checkpointID, uint64_t attempt);
    uint64_t wakeAttemptSeq_ = 0;

    uint64_t retrySeq_ = 0;  // requestID sequence for stale retries
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_SNAP_CTRL_ACTOR_H
