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

#include "snap_ctrl_actor.h"
#include "hold_attempt_policy.h"

#include <chrono>
#include <nlohmann/json.hpp>

#include "async/async.hpp"
#include "async/defer.hpp"
#include "common/constants/actor_name.h"
#include "common/logs/logging.h"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix_pb.h"
#include "common/resource_view/resource_type.h"
#include "common/types/instance_state.h"
#include "common/utils/struct_transfer.h"
#include "local_scheduler/function_agent_manager/function_agent_mgr.h"
#include "local_scheduler/instance_control/idle/idle_mgr.h"
#include "function_proxy/common/parked_instance_registry/parked_instance_registry.h"
#include "local_scheduler/instance_control/instance_ctrl_message.h"
#include "local_scheduler/local_scheduler_service/local_sched_srv.h"

namespace functionsystem::local_scheduler {

namespace {
// Expected identity fields must be present, non-blank and bounded; a partial
// tuple never authorizes a conditional snapshot.
bool InvalidSnapshotExpectedField(const std::string &value)
{
    return value.empty() || value.size() > 256 || value.find_first_not_of(" \t\r\n\f\v") == std::string::npos;
}
}  // namespace

SnapCtrlActor::SnapCtrlActor(const std::string &name, const std::string &nodeID)
    : BasisActor(name), nodeID_(nodeID)
{
}

void SnapCtrlActor::Init()
{
    BasisActor::Init();
    // W19-1: hold-TTL expiry now carries a demand signal — wire the wake
    // provider so expired-held retries trigger an immediate restore instead
    // of starving behind the watermark FIFO.
    function_proxy::ParkedInstanceRegistry::Instance().SetWakeProvider(
        [aid(GetAID())](const std::string &instanceID) {
            YRLOG_INFO("hold-expiry wake requested for parked instance({})", instanceID);
            litebus::AsyncAfter(0, aid, &SnapCtrlActor::RetryWake, instanceID);
        });
    YRLOG_INFO("SnapCtrlActor initialized on node: {}", nodeID_);
}

static litebus::Future<SnapshotResult> RecordSnapshotMetadata(const std::shared_ptr<LocalSchedSrv> &localSchedSrv,
                                                              const messages::SnapshotRuntimeResponse &runtimeRsp,
                                                              const resource_view::InstanceInfo &instanceInfo,
                                                              const std::string &functionType)
{
    auto requestID = runtimeRsp.requestid();
    if (runtimeRsp.code() != common::ERR_NONE) {
        YRLOG_ERROR("{}|SnapshotRuntime failed: {}", requestID, runtimeRsp.message());
        return SnapshotResult{ .code = runtimeRsp.code(),
                               .message = runtimeRsp.message(),
                               .snapshotInfo = runtimeRsp.snapshotinfo() };
    }
    if (runtimeRsp.snapshotinfo().checkpointid().empty()) {
        return SnapshotResult{ .code = common::ERR_INNER_SYSTEM_ERROR,
            .message = "successful snapshot reply has no checkpoint identity", .snapshotInfo = runtimeRsp.snapshotinfo() };
    }
    auto req = std::make_shared<messages::RecordSnapshotRequest>();
    *req->mutable_snapshotinfo() = runtimeRsp.snapshotinfo();
    *req->mutable_instanceinfo() = instanceInfo;
    req->mutable_instanceinfo()->clear_args();
    req->set_requestid(requestID);
    auto *fk = req->mutable_functionkey();
    fk->set_tenantid(instanceInfo.tenantid());
    fk->set_functiontype(functionType.empty() ? instanceInfo.function() : functionType);
    const auto &ckptID = runtimeRsp.snapshotinfo().checkpointid();
    const auto &storagePath = runtimeRsp.snapshotinfo().storage();
    const auto size = runtimeRsp.snapshotinfo().size();

    YRLOG_INFO("{}|recording snapshot metadata, checkpointID: {}, storagePath:{}, size: {}", requestID, ckptID,
               storagePath, size);

    return localSchedSrv->RecordSnapshotMetadata(req).Then(
        [requestID, runtimeRsp](const messages::RecordSnapshotResponse &rsp) -> litebus::Future<SnapshotResult> {
            SnapshotResult result;
            result.code = rsp.code();
            result.message = rsp.message();
            result.snapshotInfo = runtimeRsp.snapshotinfo();
            if (result.code == common::ERR_NONE) {
                YRLOG_INFO("{}|snapshot metadata recorded successfully, checkpointID: {}", requestID,
                           runtimeRsp.snapshotinfo().checkpointid());
            } else {
                YRLOG_ERROR("{}|failed to record snapshot metadata, checkpointID: {}, code: {}, message: {}", requestID,
                            runtimeRsp.snapshotinfo().checkpointid(), result.code, result.message);
            }
            return result;
        });
}

litebus::Future<KillResponse> SnapCtrlActor::HandleSnapshotIfSource(const std::string &requestID,
                                                                    const std::string &instanceID,
                                                                    const std::string &payload,
                                                                    const std::shared_ptr<InstanceStateMachine> &source)
{
    // Every validation precedes any park/drain/gate mutation: a rejected
    // conditional request must leave the instance's state exactly as it was.
    // A null view is a safe rejection: source-aware invalid input must never
    // abort the actor on a missing binding.
    auto rejected = [&requestID, &instanceID](common::ErrorCode code, const std::string &reason) {
        YRLOG_ERROR("{}|{}|conditional snapshot rejected: {}", requestID, instanceID, reason);
        KillResponse rsp;
        rsp.set_code(code);
        rsp.set_message(reason);
        return rsp;
    };
    if (instanceControlView_ == nullptr) {
        return rejected(common::ERR_INNER_SYSTEM_ERROR, "instance control view is not bound");
    }
    if (source == nullptr) {
        return rejected(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)),
                        "conditional snapshot requires a captured source");
    }
    if (instanceControlView_->GetInstance(instanceID) != source) {
        return rejected(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)),
                        "captured source is no longer the instance state machine");
    }
    if (source->GetInstanceState() != InstanceState::RUNNING) {
        return rejected(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)),
                        "captured source is not RUNNING");
    }
    SnapOptions options;
    if (payload.empty() || !options.ParseFromString(payload)) {
        return rejected(static_cast<common::ErrorCode>(StatusCode::ERR_PARAM_INVALID),
                        "invalid conditional snapshot payload");
    }
    if (InvalidSnapshotExpectedField(options.expected_runtime_id())
        || InvalidSnapshotExpectedField(options.expected_sandbox_id())
        || InvalidSnapshotExpectedField(options.expected_resource_generation())) {
        return rejected(static_cast<common::ErrorCode>(StatusCode::ERR_PARAM_INVALID),
                        "conditional snapshot requires a complete expected identity tuple");
    }
    if (options.expected_runtime_id() != source->GetInstanceInfo().runtimeid()) {
        return rejected(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)),
                        "expected runtime does not match the captured snapshot source");
    }
    // Same actor turn: HandleSnapshot re-parses and re-fences, then runs.
    return HandleSnapshot(requestID, instanceID, payload);
}

bool SnapCtrlActor::SnapshotSourceMatches(const std::string &instanceID,
                                          const std::shared_ptr<InstanceStateMachine> &source,
                                          const std::string &runtimeID)
{
    // Exact-pointer fence: only the originally captured machine, still the
    // current one, RUNNING and on the same runtime, authorizes dispatch. A
    // replacement machine's newer identity is never adopted here. A null view
    // is a plain "cannot fence" (false), never an abort.
    if (instanceControlView_ == nullptr || source == nullptr || runtimeID.empty()) {
        return false;
    }
    return instanceControlView_->GetInstance(instanceID) == source
        && source->GetInstanceState() == InstanceState::RUNNING
        && source->GetInstanceInfo().runtimeid() == runtimeID;
}

litebus::Future<KillResponse> SnapCtrlActor::HandleSnapshot(const std::string &requestID, const std::string &instanceID,
                                                            const std::string &payload)
{
    // 1. 解析 payload 获取参数（core_service::SnapOptions）
    bool leaveRunning = false;
    int32_t ttl = 0;  // Default TTL is 0 (no expiration)
    std::string functionType;
    std::string holdToken;
    std::string expectedRuntimeID;
    std::string expectedSandboxID;
    std::string expectedGeneration;
    bool hasExpected = false;
    if (!payload.empty()) {
        SnapOptions options;
        if (!options.ParseFromString(payload)) {
            YRLOG_ERROR("{}|{}|failed to parse snapshot payload", requestID, instanceID);
            KillResponse errorRsp;
            errorRsp.set_code(static_cast<common::ErrorCode>(StatusCode::ERR_PARAM_INVALID));
            errorRsp.set_message("invalid payload format");
            return errorRsp;
        }
        leaveRunning = options.leaverunning();
        ttl = options.ttl();  // Extract TTL from SnapOptions
        functionType = options.functiontype();
        holdToken = options.hold_token();
        expectedRuntimeID = options.expected_runtime_id();
        expectedSandboxID = options.expected_sandbox_id();
        expectedGeneration = options.expected_resource_generation();
        hasExpected = !expectedRuntimeID.empty() || !expectedSandboxID.empty() || !expectedGeneration.empty();
        if (hasExpected && (InvalidSnapshotExpectedField(expectedRuntimeID)
            || InvalidSnapshotExpectedField(expectedSandboxID) || InvalidSnapshotExpectedField(expectedGeneration))) {
            // any expected field present => the complete tuple, non-blank and bounded
            KillResponse errorRsp;
            errorRsp.set_code(static_cast<common::ErrorCode>(StatusCode::ERR_PARAM_INVALID));
            errorRsp.set_message("incomplete or oversized expected identity tuple");
            return errorRsp;
        }
        if (!holdToken.empty() && !hasExpected) {
            // an old token-only payload no longer authorizes a park: the token
            // must pin the exact physical source it was taken against
            KillResponse errorRsp;
            errorRsp.set_code(common::ERR_PARAM_INVALID);
            errorRsp.set_message("hold token requires the expected identity tuple");
            return errorRsp;
        }
        if (!ValidParkHoldToken(holdToken, leaveRunning)) {
            KillResponse errorRsp;
            errorRsp.set_code(common::ERR_PARAM_INVALID);
            errorRsp.set_message("invalid park hold token");
            return errorRsp;
        }
    }
    if (snapshotAttempts_.Busy(instanceID) || parkedInstances_.count(instanceID) != 0) {
        KillResponse busy;
        busy.set_code(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)));
        busy.set_message("snapshot already pending or instance parked");
        return busy;
    }
    // 1. 获取实例状态机
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_ERROR("{}|{}|failed to get instance state machine for snapshot", requestID, instanceID);
        KillResponse errorRsp;
        errorRsp.set_code(static_cast<common::ErrorCode>(StatusCode::ERR_INSTANCE_NOT_FOUND));
        errorRsp.set_message("instance not found");
        return errorRsp;
    }
    YRLOG_INFO("{}|{}|start snapshot, leave_running: {}", requestID, instanceID, leaveRunning);
    auto instanceInfo = stateMachine->GetInstanceInfo();
    // Conditional-path fence, still before any gate/mark/drain mutation: the
    // expected runtime must be this source's and the source must be RUNNING.
    // The optional Stats cached in InstanceInfo are deliberately NOT trusted
    // to validate the physical generation — the captured expected pair travels
    // to the daemon, which re-checks it under its resource lock.
    if (hasExpected && (stateMachine->GetInstanceState() != InstanceState::RUNNING
        || expectedRuntimeID != instanceInfo.runtimeid())) {
        KillResponse errorRsp;
        errorRsp.set_code(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)));
        errorRsp.set_message("conditional snapshot source is not the RUNNING expected runtime");
        return errorRsp;
    }
    ASSERT_IF_NULL(functionAgentMgr_);
    const auto snapshotAttempt = snapshotAttempts_.Begin(instanceID);
    if (!snapshotAttempt.has_value()) {
        KillResponse busy;
        busy.set_code(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)));
        busy.set_message("snapshot attempt capacity exhausted");
        return busy;
    }
    const auto dispatched = std::make_shared<std::atomic<bool>>(false);
    // Set when a deferred source recheck fails: the park mark and drain may
    // then belong to a replacement source's lifecycle, so no cleanup in this
    // chain may clear them (reconciliation owns them instead).
    const auto sourceMismatched = std::make_shared<std::atomic<bool>>(false);
    // Mark the instance as parked BEFORE the checkpoint starts: the deliberate sandbox
    // kill of a park is reported to the control plane as an instance exit and reaches
    // the data plane (InstanceView FATAL / delete chains) before the snapshot completes,
    // so a mark taken only after completion races (and loses against) the teardown.
    auto &parkedRegistry = function_proxy::ParkedInstanceRegistry::Instance();
    const bool markParked = !leaveRunning && parkedRegistry.Enabled();
    if (markParked) {
        parkedRegistry.MarkParked(instanceID);
        YRLOG_INFO("{}|{}|instance marked parked (snapshot starting), data-plane invokes will be held until restore",
                   requestID, instanceID);
    }
    // 2. 排空在途 invoke（park drain，W6 方向 A）：杀沙箱前先让数据面静默——
    //    dispatcher 翻 not-ready 停新，等已投递的在途 invoke 在活沙箱里跑完。
    //    超时默认放弃本次 park（实例继续运行，上层可重试）；force 策略仅实验用。
    const bool forceOnDrainTimeout = markParked && parkedRegistry.DrainForceOnTimeout();
    return parkedRegistry.DrainInstance(instanceID)
        .Then(litebus::Defer(GetAID(),
            [this, requestID, instanceID, instanceInfo, ttl, leaveRunning, markParked, forceOnDrainTimeout,
             dispatched, sourceMismatched, functionAgentMgr(functionAgentMgr_), hasExpected, expectedSandboxID,
             expectedGeneration, source(stateMachine), runtimeID(instanceInfo.runtimeid())](const Status &drainStatus)
                -> litebus::Future<messages::SnapshotRuntimeResponse> {
            // Source fence FIRST: an already-flagged or freshly detected
            // mismatch skips every cleanup below — the mark/drain may belong
            // to a replacement source's lifecycle now.
            if (hasExpected && (sourceMismatched->load() || !SnapshotSourceMatches(instanceID, source, runtimeID))) {
                sourceMismatched->store(true);
                YRLOG_ERROR("{}|{}|snapshot source changed after drain; refusing cleanup and dispatch",
                            requestID, instanceID);
                messages::SnapshotRuntimeResponse mismatch;
                mismatch.set_code(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY));
                mismatch.set_message("snapshot source changed after drain");
                return mismatch;
            }
            if (drainStatus.IsError()) {
                YRLOG_ERROR("{}|{}|park drain failed: {}", requestID, instanceID, drainStatus.GetMessage());
                if (!forceOnDrainTimeout) {
                    // abandon this park: drop the mark, the drain phase already rolled
                    // the dispatcher back to ready, the instance keeps running
                    if (markParked) {
                        function_proxy::ParkedInstanceRegistry::Instance().Clear(instanceID);
                    }
                    messages::SnapshotRuntimeResponse errorRsp;
                    errorRsp.set_code(Status::GetPosixErrorCode(drainStatus.StatusCode()));
                    errorRsp.set_message(drainStatus.RawMessage());
                    return errorRsp;
                }
                YRLOG_WARN("{}|{}|park drain timed out but force is configured, proceeding with the park "
                           "(in-flight invokes may break)",
                           requestID, instanceID);
            }
            // 3. 调用 PrepareSnap 验证实例状态并准备快照
            return PrepareSnap(requestID, instanceID, hasExpected ? source : nullptr,
                hasExpected ? runtimeID : "", hasExpected ? sourceMismatched : nullptr)
                .Then(litebus::Defer(GetAID(),
                    [this, requestID, instanceID, instanceInfo, ttl, leaveRunning, markParked, functionAgentMgr,
                     dispatched, sourceMismatched, hasExpected, expectedSandboxID, expectedGeneration, source,
                     runtimeID](const Status &status) -> litebus::Future<messages::SnapshotRuntimeResponse> {
                        // Source fence FIRST (also pre-dispatch): an already-flagged
                        // or freshly detected mismatch skips the failure cleanup
                        // below, which must not clear another source's mark/drain.
                        if (hasExpected && (sourceMismatched->load()
                            || !SnapshotSourceMatches(instanceID, source, runtimeID))) {
                            sourceMismatched->store(true);
                            YRLOG_ERROR("{}|{}|snapshot source changed before dispatch", requestID, instanceID);
                            messages::SnapshotRuntimeResponse mismatch;
                            mismatch.set_code(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY));
                            mismatch.set_message("snapshot source changed before dispatch");
                            return mismatch;
                        }
                        if (status.IsError()) {
                            YRLOG_ERROR("{}|{}|PrepareSnap failed: {}", requestID, instanceID, status.GetMessage());
                            if (markParked) {
                                // park abandoned before the kill: roll the drain back too
                                function_proxy::ParkedInstanceRegistry::Instance().ReleaseDrain(instanceID);
                                function_proxy::ParkedInstanceRegistry::Instance().Clear(instanceID);
                            }
                            messages::SnapshotRuntimeResponse errorRsp;
                            errorRsp.set_code(Status::GetPosixErrorCode(status.StatusCode()));
                            errorRsp.set_message(status.RawMessage());
                            return errorRsp;
                        }
                        // 4. 通过 functionAgentMgr_ 发送 SnapshotRuntime 请求到 function_agent
                        dispatched->store(true);
                        if (!hasExpected) {
                            return functionAgentMgr->SnapshotRuntime(requestID, instanceInfo, ttl, leaveRunning);
                        }
                        // Conditional dispatch: the generation-fenced channel
                        // carries the caller's ORIGINAL identity pair (runtime
                        // already verified against the captured source); the
                        // legacy unfenced message is never sent on this path.
                        auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
                        request->set_requestid(requestID);
                        request->set_instanceid(instanceID);
                        request->set_runtimeid(instanceInfo.runtimeid());
                        request->set_containerid(instanceInfo.containerid());  // containerID is same as runtimeID in container mode
                        request->set_ttl(ttl);
                        request->set_leaverunning(leaveRunning);  // sandboxd PR#16 physical mode
                        request->set_expected_sandbox_id(expectedSandboxID);
                        request->set_expected_resource_generation(expectedGeneration);
                        return functionAgentMgr->SnapshotRuntimeIfGeneration(instanceInfo, request);
                    }));
            }))
        .Then([aid(GetAID()), localSchedSrv(localSchedSrv_), requestID, instanceInfo,
               functionType](const messages::SnapshotRuntimeResponse &runtimeRsp) -> litebus::Future<SnapshotResult> {
            return RecordSnapshotMetadata(localSchedSrv, runtimeRsp, instanceInfo, functionType);
        })
        .Then([requestID, instanceID, leaveRunning, markParked, dispatched, sourceMismatched, aid(GetAID()),
               instanceCtrl(instanceCtrl_), source(stateMachine), runtimeID(instanceInfo.runtimeid())](const SnapshotResult &result) -> litebus::Future<SnapshotResult> {
            if (result.code != common::ERR_NONE) {
                // snapshot failed: the instance keeps running (or died for real), drop
                // the park mark so later exit events take the legacy teardown path.
                // A source mismatch is the exception: the mark/drain may belong to
                // a replacement source now, so this chain must not clear them.
                if (markParked && !dispatched->load() && !sourceMismatched->load()) {
                    // park abandoned before dispatch: roll the drain back (no-op if
                    // the instance actually died — the FATAL path then governs)
                    function_proxy::ParkedInstanceRegistry::Instance().ReleaseDrain(instanceID);
                    function_proxy::ParkedInstanceRegistry::Instance().Clear(instanceID);
                }
                return result;
            }

            // 5. 日志记录最终状态
            if (!leaveRunning) {
                YRLOG_INFO("{}|{}|snapshot completed, deleting instance", requestID, instanceID);
                if (instanceCtrl == nullptr) {
                    auto failed = result;
                    failed.code = common::ERR_INNER_SYSTEM_ERROR;
                    failed.message = "snapshot source retirement unavailable";
                    return failed;
                }
                if (runtimeID.empty() || result.snapshotInfo.source_runtime_id() != runtimeID) {
                    auto failed = result;
                    failed.code = common::ERR_INSTANCE_BUSY;
                    failed.message = "checkpoint runtime does not match snapshot source";
                    return failed;
                }
                return instanceCtrl->RetireSnapshotSource(instanceID, source, runtimeID)
                    .Then([result](const Status &retired) {
                        auto completed = result;
                        if (retired.IsError()) {
                            completed.code = Status::GetPosixErrorCode(retired.StatusCode());
                            completed.message = retired.RawMessage();
                        }
                        return completed;
                    });
            } else {
                YRLOG_INFO("{}|{}|snapshot completed, instance continues running", requestID, instanceID);
                if (markParked) {  // defensive: leaveRunning cannot change mid-flight
                    function_proxy::ParkedInstanceRegistry::Instance().Clear(instanceID);
                }
            }

            return result;
        })
        .Then(litebus::Defer(GetAID(), &SnapCtrlActor::OnHandleSnapshot, instanceID, leaveRunning,
                             std::placeholders::_1, holdToken, snapshotAttempt.value()))
        .OnComplete(litebus::Defer(GetAID(), &SnapCtrlActor::FinishSnapshotAttempt, instanceID,
            snapshotAttempt.value(), markParked, dispatched, std::placeholders::_1, sourceMismatched));
}

KillResponse SnapCtrlActor::OnHandleSnapshot(const std::string &instanceID, bool leaveRunning,
                                             const SnapshotResult &result, const std::string &holdToken, uint64_t snapshotAttempt)
{
    KillResponse rsp;
    if (snapshotAttempt != 0 && !snapshotAttempts_.Matches(instanceID, snapshotAttempt)) {
        rsp.set_code(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)));
        rsp.set_message("stale snapshot completion");
        return rsp;
    }
    if (result.code == common::ERR_NONE && result.snapshotInfo.checkpointid().empty()) {
        rsp.set_code(common::ERR_INNER_SYSTEM_ERROR);
        rsp.set_message("snapshot completion has no checkpoint identity");
        return rsp;
    }
    rsp.set_code(static_cast<common::ErrorCode>(result.code));
    rsp.set_message(result.message);

    // 在 payload 中返回 core_service::SnapInfo 序列化结果
    if (result.code == common::ERR_NONE && !result.snapshotInfo.checkpointid().empty()) {
        SnapInfo info;
        info.set_snapshotid(result.snapshotInfo.checkpointid());
        info.set_size(result.snapshotInfo.size());
        rsp.set_payload(info.SerializeAsString());
        YRLOG_INFO("snapshot completed, checkpointID: {}, size: {}", result.snapshotInfo.checkpointid(),
                   result.snapshotInfo.size());
        if (!leaveRunning) {
            // park: the instance is deleted right after this response, so the
            // checkpoint is the only handle left. Record it for wake/FIFO unpark.
            ParkedEntry entry;
            entry.checkpointID = result.snapshotInfo.checkpointid();
            entry.sourceSandboxID = result.snapshotInfo.sandbox_id();
            entry.sourceRuntimeID = result.snapshotInfo.source_runtime_id();
            entry.sourceGeneration = result.snapshotInfo.source_generation();
            entry.holdToken = holdToken;
            entry.parkedAt = std::chrono::steady_clock::now();
            parkedInstances_[instanceID] = entry;
            YRLOG_INFO("instance({}) parked, registered checkpoint({}), parked total: {}", instanceID,
                       entry.checkpointID, parkedInstances_.size());
            // D-5 F1: clear the idle bookkeeping for the ID before the
            // restore reuses it, or the pre-park idle timer evicts the
            // restored instance on its old deadline.
            if (idleMgr_ != nullptr) {
                idleMgr_->OnInstanceParked(instanceID);
            }
        }
    }
    return rsp;
}

void SnapCtrlActor::FinishSnapshotAttempt(const std::string &instanceID, uint64_t attempt, bool markParked,
    const std::shared_ptr<std::atomic<bool>> &dispatched, const litebus::Future<KillResponse> &result,
    const std::shared_ptr<std::atomic<bool>> &sourceMismatched)
{
    if (!snapshotAttempts_.Matches(instanceID, attempt)) { return; }
    const bool mismatch = sourceMismatched != nullptr && sourceMismatched->load();
    const bool success = !result.IsError() && result.Get().code() == common::ERR_NONE;
    const bool beforeDispatch = !dispatched->load();
    if (mismatch) {
        // The mark/drain stay held by an unresolved owner, so the attempt must
        // not be cleared even on a synthetic success: retaining it UNKNOWN
        // keeps the gate fenced until authoritative reconciliation.
        snapshotAttempts_.Finish(instanceID, attempt, false);
        YRLOG_WARN("snapshot source changed for instance({}), retaining attempt {} unknown pending reconciliation",
                   instanceID, attempt);
        return;
    }
    snapshotAttempts_.Finish(instanceID, attempt, success || beforeDispatch);
    if (!success && beforeDispatch && markParked) {
        function_proxy::ParkedInstanceRegistry::Instance().ReleaseDrain(instanceID);
        function_proxy::ParkedInstanceRegistry::Instance().Clear(instanceID);
    }
    if (!success && !beforeDispatch) {
        YRLOG_WARN("snapshot outcome uncertain for instance({}), retaining attempt {} pending reconciliation",
                   instanceID, attempt);
    }
}

litebus::Future<KillResponse> SnapCtrlActor::HandleDirectedWake(const std::string &requestID,
    const std::string &instanceID, const std::string &payload)
{
    auto rejected = [](const std::string &reason) {
        KillResponse response;
        response.set_code(common::ERR_PARAM_INVALID);
        response.set_message(reason);
        return response;
    };
    if (payload.empty() || payload.size() > 4096) { return rejected("wake identity payload required"); }
    const auto command = nlohmann::json::parse(payload, nullptr, false);
    if (!command.is_object() || !command.contains("schema") || !command["schema"].is_number_integer()
        || (command["schema"] != 1 && command["schema"] != 2) || !command.contains("deadline_unix_ms")
        || !command["deadline_unix_ms"].is_number_integer()) { return rejected("invalid wake identity schema"); }
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    // Compare as JSON numbers before conversion so oversized unsigned input
    // cannot wrap into an apparently valid signed deadline.
    if (command["deadline_unix_ms"] <= nowMs || command["deadline_unix_ms"] > nowMs + 60000) {
        return rejected("wake identity command expired or too far in future");
    }
    for (const auto *field : {"sandbox_id", "runtime_id", "resource_generation"}) {
        if (!command.contains(field) || !command[field].is_string()
            || command[field].get_ref<const std::string &>().empty()) { return rejected("incomplete wake identity"); }
    }
    const auto it = parkedInstances_.find(instanceID);
    if (it == parkedInstances_.end() || it->second.sourceSandboxID.empty()
        || it->second.sourceRuntimeID.empty() || it->second.sourceGeneration.empty()
        || command["sandbox_id"] != it->second.sourceSandboxID
        || command["runtime_id"] != it->second.sourceRuntimeID
        || command["resource_generation"] != it->second.sourceGeneration) {
        return rejected("wake does not match parked physical source");
    }
    if (!WakeSchemaMatchesHoldAttempt(command, it->second.holdToken)) {
        return rejected("wake does not match parked hold attempt");
    }
    // Identity, attempt comparison and wake mutation execute on this actor.
    return HandleWake(requestID, instanceID);
}

litebus::Future<KillResponse> SnapCtrlActor::HandleWake(const std::string &requestID, const std::string &instanceID)
{
    auto it = parkedInstances_.find(instanceID);
    if (it == parkedInstances_.end()) {
        YRLOG_WARN("{}|wake rejected: instance({}) is not parked on this node", requestID, instanceID);
        KillResponse rsp;
        rsp.set_code(static_cast<common::ErrorCode>(StatusCode::ERR_INSTANCE_NOT_FOUND));
        rsp.set_message("instance is not parked on this node: " + instanceID);
        return rsp;
    }
    if (it->second.waking) {
        // W9: a previous wake for this entry is still in flight (its restore
        // chain can outlive the monitor's wake cooldown); a second concurrent
        // wake restores a DUPLICATE instance and the first then fails with a
        // replay conflict (W8 v7, second green line in the envelope plot).
        YRLOG_WARN("{}|wake rejected: instance({}) already has a wake in flight", requestID, instanceID);
        KillResponse rsp;
        rsp.set_code(static_cast<common::ErrorCode>(StatusCode::ERR_INSTANCE_EXITED));
        rsp.set_message("wake already in flight for parked instance: " + instanceID);
        return rsp;
    }
    it->second.waking = true;
    const auto attempt = ++wakeAttemptSeq_;
    it->second.wakeAttempt = attempt;
    const auto checkpointID = it->second.checkpointID;
    YRLOG_INFO("{}|wake parked instance({}) from checkpoint({})", requestID, instanceID, checkpointID);
    return HandleSnapStart(requestID, checkpointID, "")
        .Then(litebus::Defer(GetAID(), &SnapCtrlActor::OnWakeComplete, instanceID, checkpointID, attempt, std::placeholders::_1));
}

KillResponse SnapCtrlActor::OnWakeComplete(const std::string &instanceID, const std::string &checkpointID,
                                         uint64_t attempt, const KillResponse &rsp)
{
    const auto current = parkedInstances_.find(instanceID);
    if (current == parkedInstances_.end() || current->second.checkpointID != checkpointID
        || current->second.wakeAttempt != attempt || !current->second.waking) {
        return rsp; // duplicate/late completion cannot settle a newer park or wake
    }
    current->second.waking = false;

    if (rsp.code() == common::ERR_NONE) {
        ForgetParked(instanceID);
        return rsp;
    }
    // 步6 正规化：终态失败（checkpoint 工件不存在/过期、同 ID 实例已重建）
    // 时丢弃注册表项，否则水位 FIFO unpark 会每周期无限重试。工件若仍在，
    // 外部 signal 19（按 checkpointID）恢复不受影响；瞬时错误保留条目重试。
    const auto &msg = rsp.message();
    const bool terminal = msg.find("not found") != std::string::npos
        || msg.find("has expired") != std::string::npos
        || msg.find("same instance id") != std::string::npos;
    if (terminal) {
        YRLOG_WARN("wake of instance({}) failed terminally ({}), dropping parked entry; "
                   "signal-19 restore by checkpointID remains available",
                   instanceID, msg);
        ForgetParked(instanceID);
        return rsp;
    }
    // D-5 F4: non-terminal failures (e.g. a 24G checkpoint restore exceeding
    // the RPC deadline, code 1017) must not retry forever -- the monitor's
    // wake cooldown re-picks the same oldest entry every 300s. Give up after
    // kWakeGiveUpAfter consecutive failures; the checkpoint itself stays on
    // disk (park TTL) and signal-19 by checkpointID still works.
    auto it = parkedInstances_.find(instanceID);
    if (it != parkedInstances_.end()) {
        it->second.waking = false;  // settled: pickable again on the next retry
        // W16: the stale-state-machine failure is self-healing — the branch
        // that emitted it already superseded (deleted) the stale machine, so
        // the very next attempt schedules fresh. W15b measured two such
        // transients spaced by the 2-cycle monitor cooldown: 21s to restore,
        // one failure away from the F4 give-up dropping the entry entirely.
        // Re-wake immediately (short settle delay for the domain layer's own
        // in-flight re-dispatch) and keep this class out of the F4 budget.
        if (msg.find("without a pending schedule future") != std::string::npos) {
            if (++it->second.staleRetries <= kWakeStaleRetryMax) {
                YRLOG_WARN("wake of instance({}) hit a stale state machine (superseded); "
                           "immediate retry {}/{} in {}ms",
                           instanceID, it->second.staleRetries, kWakeStaleRetryMax, kWakeStaleRetryDelayMs);
                (void)litebus::AsyncAfter(kWakeStaleRetryDelayMs, GetAID(),
                                          &SnapCtrlActor::RetryWakeIfCurrent, instanceID, checkpointID, attempt);
                return rsp;
            }
            YRLOG_WARN("wake of instance({}) exhausted {} stale retries, falling back to cooldown pacing",
                       instanceID, kWakeStaleRetryMax);
        }
        if (++it->second.wakeFails >= kWakeGiveUpAfter) {
            YRLOG_WARN("wake of instance({}) failed {} times (last: {}), dropping parked entry; "
                       "signal-19 restore by checkpointID({}) remains available",
                       instanceID, it->second.wakeFails, msg, it->second.checkpointID);
            ForgetParked(instanceID);
        }
    }
    return rsp;
}

void SnapCtrlActor::RetryWake(const std::string &instanceID)
{
    // W16: void-returning wrapper so AsyncAfter can schedule the immediate
    // stale-transient re-wake (HandleWake returns a Future).
    const auto requestID = "stale-retry-" + std::to_string(++retrySeq_);
    // HandleWake already owns the single completion callback. Attaching a
    // second one here counted failures twice and could clear a newer wake.
    (void)HandleWake(requestID, instanceID);
}

void SnapCtrlActor::RetryWakeIfCurrent(const std::string &instanceID, const std::string &checkpointID,
                                     uint64_t attempt)
{
    const auto current = parkedInstances_.find(instanceID);
    if (current == parkedInstances_.end() || current->second.checkpointID != checkpointID
        || current->second.wakeAttempt != attempt || current->second.waking) {
        return;
    }
    RetryWake(instanceID);
}

void SnapCtrlActor::ForgetParked(const std::string &instanceID)
{
    if (parkedInstances_.erase(instanceID) > 0) {
        YRLOG_INFO("parked instance({}) restored, registry size: {}", instanceID, parkedInstances_.size());
    }
}

void SnapCtrlActor::ForgetParkedByCheckpoint(const std::string &checkpointID)
{
    for (auto it = parkedInstances_.begin(); it != parkedInstances_.end();) {
        if (it->second.checkpointID == checkpointID) {
            YRLOG_INFO("parked instance({}) restored by direct snapstart, registry size before: {}", it->first,
                       parkedInstances_.size());
            it = parkedInstances_.erase(it);
        } else {
            ++it;
        }
    }
}

void SnapCtrlActor::FinishSnapStart(const std::string &checkpointID)
{
    // W10-2: the restore chain settled (success or failure); allow a later
    // SnapStart for this checkpoint again (e.g. a retry after a failure).
    if (restoringCheckpoints_.erase(checkpointID) > 0) {
        YRLOG_INFO("snapstart of checkpoint({}) settled, in-flight restores: {}", checkpointID,
                   restoringCheckpoints_.size());
    }
}

std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> SnapCtrlActor::GetParkedInstances()
{
    std::vector<std::pair<std::string, ParkedEntry>> out;
    out.reserve(parkedInstances_.size());
    for (const auto &entry : parkedInstances_) {
        if (entry.second.waking) {
            // a wake is in flight for this entry: invisible to the FIFO picker
            continue;
        }
        out.emplace_back(entry.first, entry.second);
    }
    return out;
}

litebus::Future<KillResponse> SnapCtrlActor::HandleSnapStart(const std::string &requestID,
                                                             const std::string &checkpointID,
                                                             const std::string &payload)
{
    // 1. 验证 checkpointID
    if (checkpointID.empty()) {
        YRLOG_ERROR("{}|HandleSnapStart: empty checkpointID", requestID);
        KillResponse errorRsp;
        errorRsp.set_code(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_PARAM_INVALID)));
        errorRsp.set_message("empty checkpointID");
        return errorRsp;
    }

    // 2. 解析 SnapStartOptions payload
    SnapStartOptions options;
    if (!payload.empty() && !options.ParseFromString(payload)) {
        YRLOG_ERROR("{}|failed to parse SnapStartOptions payload", requestID);
        KillResponse errorRsp;
        errorRsp.set_code(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_PARAM_INVALID)));
        errorRsp.set_message("invalid SnapStartOptions payload");
        return errorRsp;
    }

    // W10-2: one restore per checkpoint at a time. Duplicate SnapStarts for
    // the same checkpoint (retried RPCs, a second wake path racing the first)
    // each mint a fresh synthetic "ckpt-" instance and restore a DUPLICATE
    // sandbox; the loser then fails with an identity/replay conflict (W9-2
    // v4: every successful restore was followed by one such noisy failure).
    // Reject the duplicate outright -- the parked-registry waking flag covers
    // the wake path only, this covers every caller of HandleSnapStart.
    if (restoringCheckpoints_.count(checkpointID) > 0) {
        YRLOG_WARN("{}|snapstart of checkpoint {} rejected: a restore is already in flight for it",
                   requestID, checkpointID);
        KillResponse errorRsp;
        errorRsp.set_code(static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)));
        errorRsp.set_message("restore already in flight for checkpoint: " + checkpointID);
        return errorRsp;
    }
    restoringCheckpoints_.insert(checkpointID);

    YRLOG_INFO("{}|start snapstart from checkpoint: {}", requestID, checkpointID);

    // 3. 构造 RestoreSnapshotRequest
    auto req = std::make_shared<messages::RestoreSnapshotRequest>();
    req->set_requestid(requestID);
    req->set_checkpointid(checkpointID);
    *req->mutable_snapstartoptions() = options;

    // 4. 通过 localSchedSrv_ 转发到 function_master 的 ckpt_manager
    ASSERT_IF_NULL(localSchedSrv_);
    return localSchedSrv_->SnapStartCheckpoint(req).Then(
        [aid(GetAID()), requestID, checkpointID](const messages::RestoreSnapshotResponse &rsp) -> KillResponse {
            // W10-2: settle the in-flight marker on the actor thread (this
            // continuation may run on the localSchedSrv actor)
            litebus::Async(aid, &SnapCtrlActor::FinishSnapStart, checkpointID);
            KillResponse killRsp;
            killRsp.set_code(static_cast<common::ErrorCode>(rsp.code()));
            killRsp.set_message(rsp.message());

            if (rsp.code() == common::ERR_NONE) {
                YRLOG_INFO("{}|snapstart checkpoint {} succeeded, new instanceID: {}", requestID, checkpointID,
                           rsp.instanceid());
                // direct restore (signal 19): drop any parked-registry entry
                // pointing at this checkpoint so the instance cannot be
                // double-woken afterwards.
                litebus::Async(aid, &SnapCtrlActor::ForgetParkedByCheckpoint, checkpointID);
                SnapStartedInfo info;
                info.set_instanceid(rsp.instanceid());
                if (rsp.has_snapstartinfo()) {
                    info.set_routeaddress(rsp.snapstartinfo().routeaddress());
                    info.set_portmappings(rsp.snapstartinfo().portmappings());
                    info.set_functionproxyid(rsp.snapstartinfo().functionproxyid());
                    info.set_nodeid(rsp.snapstartinfo().nodeid());
                    info.set_namespace_(rsp.snapstartinfo().namespace_());
                }
                killRsp.set_payload(info.SerializeAsString());
            } else {
                YRLOG_ERROR("{}|snapstart checkpoint {} failed: {}", requestID, checkpointID, rsp.message());
            }

            return killRsp;
        });
}

void SnapCtrlActor::SnapStart(
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq, const schedule_decision::ScheduleResult &result,
    const TransitionResult &transResult)
{
    const auto &instanceID = scheduleReq->instance().instanceid();
    const auto &requestID = scheduleReq->requestid();

    YRLOG_INFO("{}|{}|SnapStarted: start snapstart instance initialization flow", requestID, instanceID);

    // todo(lwy) :Check transition result

    // 1. DeployInstance - call InstanceCtrl to deploy the snapstart instance
    ASSERT_IF_NULL(instanceCtrl_);
    YRLOG_INFO("{}|{}|calling DeploySnapStartInstance", requestID, instanceID);
    instanceCtrl_->DeploySnapStartInstance(scheduleReq)
        .OnComplete(litebus::Defer(GetAID(), &SnapCtrlActor::OnDeploySnapStartInstanceComplete, scheduleResp,
                                   scheduleReq, std::placeholders::_1));
}

void SnapCtrlActor::OnDeploySnapStartInstanceComplete(
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const litebus::Future<messages::DeployInstanceResponse> &deployFuture)
{
    const auto &instanceID = scheduleReq->instance().instanceid();
    const auto &requestID = scheduleReq->requestid();

    if (deployFuture.IsError()) {
        YRLOG_ERROR("{}|{}|DeploySnapStartInstance future failed, error code: {}", requestID, instanceID,
                    deployFuture.GetErrorCode());
        instanceCtrl_->CleanupFailedSnapStart(scheduleReq, "DeploySnapStartInstance future failed");
        scheduleResp->SetValue(GenScheduleResponse(StatusCode::FAILED, "DeploySnapStartInstance failed", *scheduleReq));
        return;
    }

    const auto &deployResponse = deployFuture.Get();
    if (deployResponse.code() != 0) {
        YRLOG_ERROR("{}|{}|deploy snapstart instance failed, code: {}, message: {}", requestID, instanceID,
                    deployResponse.code(), deployResponse.message());
        instanceCtrl_->CleanupFailedSnapStart(scheduleReq,
                                              "deploy failed: " + std::to_string(deployResponse.code()));
        scheduleResp->SetValue(GenScheduleResponse(static_cast<StatusCode>(deployResponse.code()),
                                                   deployResponse.message(), *scheduleReq));
        return;
    }

    const auto &runtimeID = deployResponse.runtimeid();
    const auto &address = deployResponse.address();
    YRLOG_INFO("{}|{}|deploy snapstart instance succeeded, runtimeID: {}, address: {}", requestID, instanceID,
               runtimeID, address);

    // Update scheduleReq with runtime details
    scheduleReq->mutable_instance()->set_runtimeid(runtimeID);
    scheduleReq->mutable_instance()->set_runtimeaddress(address);
    scheduleReq->mutable_instance()->set_starttime(deployResponse.timeinfo());
    (*scheduleReq->mutable_instance()->mutable_extensions())["PID"] = std::to_string(deployResponse.pid());
    // 步6 正规化：restore 产生新 sandbox ID，必须同步进 control view（经
    // RUNNING 转换的 instanceInfo = scheduleReq->instance()）。否则实例信息
    // 仍带 park 前的旧 containerID，RuntimeReconcileActor 会把它判成 ghost
    // （expected 容器缺失）强制删除，同时新 sandbox 被当 orphan 候选——
    // 实测 wake 后 ≤60s 被 reconcile 清扫（fp18 run1）。
    scheduleReq->mutable_instance()->set_containerid(deployResponse.containerid());
    scheduleReq->mutable_instance()->set_containerip(deployResponse.containerip());
    scheduleReq->mutable_instance()->set_executortype(deployResponse.executortype());
    if (!deployResponse.portmappings().empty()) {
        (*scheduleReq->mutable_instance()->mutable_extensions())[PORT_FORWARD_KEY] = deployResponse.portmappings();
    }

    // 2. CreateInstanceClient
    ASSERT_IF_NULL(instanceCtrl_);
    YRLOG_INFO("{}|{}|creating instance client", requestID, instanceID);
    instanceCtrl_->CreateInstanceClient(instanceID, runtimeID, address)
        .OnComplete(litebus::Defer(GetAID(), &SnapCtrlActor::OnCreateInstanceClientComplete, scheduleResp, scheduleReq,
                                   std::placeholders::_1));
}

void SnapCtrlActor::OnCreateInstanceClientComplete(
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &clientResult)
{
    const auto &instanceID = scheduleReq->instance().instanceid();
    const auto &requestID = scheduleReq->requestid();
    const auto &runtimeID = scheduleReq->instance().runtimeid();

    if (clientResult.IsError() || clientResult.Get() == nullptr) {
        YRLOG_ERROR("{}|{}|failed to create instance client, error code: {}", requestID, instanceID,
                    clientResult.GetErrorCode());
        instanceCtrl_->CleanupFailedSnapStart(scheduleReq, "failed to create instance client");
        scheduleResp->SetValue(
            GenScheduleResponse(StatusCode::FAILED, "failed to create instance client", *scheduleReq));
        return;
    }

    auto client = clientResult.Get();
    YRLOG_INFO("{}|{}|instance client created successfully", requestID, instanceID);

    // 3. StartHeartbeat
    ASSERT_IF_NULL(instanceCtrl_);
    YRLOG_INFO("{}|{}|starting heartbeat for snapstart instance", requestID, instanceID);
    instanceCtrl_->StartHeartbeat(instanceID, 0, runtimeID, StatusCode::SUCCESS);

    // 4. Call SnapStarted RPC
    YRLOG_INFO("{}|{}|calling SnapStarted RPC on runtime", requestID, instanceID);
    runtime::SnapStartedRequest snapStartedReq{};
    client->SnapStarted(std::move(snapStartedReq))
        .OnComplete(litebus::Defer(GetAID(), &SnapCtrlActor::OnSnapStartedRpcComplete, scheduleResp, scheduleReq,
                                   std::placeholders::_1));
}

void SnapCtrlActor::OnSnapStartedRpcComplete(
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const litebus::Future<runtime::SnapStartedResponse> &snapStartedResult)
{
    const auto &instanceID = scheduleReq->instance().instanceid();
    const auto &requestID = scheduleReq->requestid();

    if (snapStartedResult.IsError()) {
        YRLOG_ERROR("{}|{}|SnapStarted RPC failed, error code: {}", requestID, instanceID,
                    snapStartedResult.GetErrorCode());
        instanceCtrl_->CleanupFailedSnapStart(scheduleReq, "SnapStarted RPC failed");
        scheduleResp->SetValue(GenScheduleResponse(StatusCode::FAILED, "SnapStarted RPC failed", *scheduleReq));
        return;
    }

    auto response = snapStartedResult.Get();
    if (response.code() != common::ERR_NONE) {
        YRLOG_ERROR("{}|{}|SnapStarted RPC returned error: code={}, message={}", requestID, instanceID, response.code(),
                    response.message());
        instanceCtrl_->CleanupFailedSnapStart(scheduleReq,
                                              "SnapStarted RPC error: " + std::to_string(response.code()));
        scheduleResp->SetValue(
            GenScheduleResponse(static_cast<StatusCode>(response.code()), response.message(), *scheduleReq));
        return;
    }

    YRLOG_INFO("{}|{}|SnapStarted RPC succeeded", requestID, instanceID);

    // 5. TransInstanceState to RUNNING
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        YRLOG_ERROR("{}|{}|failed to get instance state machine", requestID, instanceID);
        scheduleResp->SetValue(
            GenScheduleResponse(StatusCode::ERR_INSTANCE_NOT_FOUND, "instance state machine not found", *scheduleReq));
        return;
    }

    YRLOG_INFO("{}|{}|transitioning instance state to RUNNING", requestID, instanceID);
    TransContext transContext{ InstanceState::RUNNING, stateMachine->GetVersion(), "running" };
    transContext.scheduleReq = scheduleReq;

    ASSERT_IF_NULL(instanceCtrl_);
    instanceCtrl_->TransInstanceState(stateMachine, transContext)
        .OnComplete(litebus::Defer(GetAID(), &SnapCtrlActor::OnTransInstanceStateComplete, scheduleResp, scheduleReq,
                                   std::placeholders::_1));
}

void SnapCtrlActor::OnTransInstanceStateComplete(
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq, const litebus::Future<TransitionResult> &transResult)
{
    const auto &instanceID = scheduleReq->instance().instanceid();
    const auto &requestID = scheduleReq->requestid();

    if (transResult.IsError()) {
        YRLOG_ERROR("{}|{}|failed to transition instance to RUNNING state, error code: {}", requestID, instanceID,
                    transResult.GetErrorCode());
        instanceCtrl_->CleanupFailedSnapStart(scheduleReq, "RUNNING transition future failed");
        scheduleResp->SetValue(
            GenScheduleResponse(StatusCode::ERR_ETCD_OPERATION_ERROR, "failed to update instance state", *scheduleReq));
        return;
    }

    const auto &result = transResult.Get();
    if (result.status.IsError()) {
        YRLOG_ERROR("{}|{}|failed to transition instance to RUNNING state: {}", requestID, instanceID,
                    result.status.GetMessage());
        instanceCtrl_->CleanupFailedSnapStart(scheduleReq, "RUNNING transition failed");
        scheduleResp->SetValue(
            GenScheduleResponse(result.status.StatusCode(), result.status.GetMessage(), *scheduleReq));
        return;
    }

    // 6. SetValue to complete schedule
    YRLOG_INFO("{}|{}|snapstart instance initialized successfully, state: RUNNING", requestID, instanceID);
    scheduleResp->SetValue(GenScheduleResponse(StatusCode::SUCCESS, "success", *scheduleReq));
}

litebus::Future<Status> SnapCtrlActor::PrepareSnap(const std::string &requestID, const std::string &instanceID,
    const std::shared_ptr<InstanceStateMachine> &capturedSource, const std::string &capturedRuntimeID,
    const std::shared_ptr<std::atomic<bool>> &sourceMismatched)
{
    YRLOG_INFO("{}|{}|PrepareSnap: instance is running, getting client", requestID, instanceID);
    // 3. 获取 client 并调用 PrepareSnap
    ASSERT_IF_NULL(clientManager_);
    const bool conditional = capturedSource != nullptr && !capturedRuntimeID.empty();
    return clientManager_->GetControlInterfacePosixClient(instanceID)
        .Then(litebus::Defer(GetAID(),
            [this, requestID, instanceID, conditional, capturedSource, capturedRuntimeID, sourceMismatched]
            (const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &clientFuture)
                -> litebus::Future<Status> {
            // Conditional source check before any other outcome: latching the
            // flag here preserves the mismatch evidence even if the view
            // changes back before a later recheck. The client manager is keyed
            // by instanceID, so the lookup alone cannot pin the transport —
            // that binding is enforced at send time below via
            // PrepareSnapForRuntime, which validates the transport's runtime
            // label and dispatches under one lock (UpdateClient retagging the
            // underlying posix rejects instead of redirecting the request).
            if (conditional && ((sourceMismatched != nullptr && sourceMismatched->load())
                || !SnapshotSourceMatches(instanceID, capturedSource, capturedRuntimeID))) {
                if (sourceMismatched != nullptr) {
                    sourceMismatched->store(true);
                }
                YRLOG_ERROR("{}|{}|PrepareSnap skipped: snapshot source changed before the client lookup completed",
                            requestID, instanceID);
                return Status(StatusCode::ERR_INSTANCE_BUSY, "snapshot source changed before prepare");
            }
            // Reject the failed future before any Get; a null client is checked
            // separately so neither path dereferences a broken future.
            if (clientFuture.IsError()) {
                YRLOG_ERROR("{}|{}|failed to get control interface client, error code: {}", requestID, instanceID,
                            clientFuture.GetErrorCode());
                return Status(StatusCode::FAILED, "failed to get control interface client");
            }
            if (clientFuture.Get() == nullptr) {
                YRLOG_ERROR("{}|{}|failed to get control interface client, client is null", requestID, instanceID);
                return Status(StatusCode::FAILED, "failed to get control interface client");
            }
            auto client = clientFuture.Get();
            // 4. 调用 PrepareSnap 接口。Conditional snapshots dispatch ONLY via
            // the runtime-pinned entry point with the caller's captured
            // runtime; a pinned rejection is final — never a legacy retry,
            // which would send on whatever transport the instance slot now
            // holds. The unconditional path keeps the legacy entry point.
            runtime::PrepareSnapRequest prepareReq{};
            auto prepareFuture = conditional
                ? client->PrepareSnapForRuntime(std::move(prepareReq), capturedRuntimeID)
                : client->PrepareSnap(std::move(prepareReq));
            return prepareFuture
                .Then([requestID,
                       instanceID](const litebus::Future<runtime::PrepareSnapResponse> &prepareResult) -> Status {
                    if (prepareResult.IsError()) {
                        YRLOG_ERROR("{}|{}|PrepareSnap RPC failed, error code: {}", requestID, instanceID,
                                    prepareResult.GetErrorCode());
                        return Status(StatusCode::FAILED, "PrepareSnap RPC failed");
                    }

                    auto response = prepareResult.Get();
                    if (response.code() != common::ERR_NONE) {
                        YRLOG_ERROR("{}|{}|PrepareSnap failed: code={}, message={}", requestID, instanceID,
                                    response.code(), response.message());
                        return Status(StatusCode::FAILED, response.message());
                    }

                    YRLOG_INFO("{}|{}|PrepareSnap succeeded", requestID, instanceID);
                    return Status::OK();
                });
        }));
}
}  // namespace functionsystem::local_scheduler
