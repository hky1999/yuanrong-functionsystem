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

#include <chrono>
#include <exception>
#include <nlohmann/json.hpp>

#include "async/async.hpp"
#include "async/defer.hpp"
#include "common/constants/actor_name.h"
#include "common/logs/logging.h"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix_pb.h"
#include "common/resource_view/resource_type.h"
#include "local_scheduler/function_agent_manager/function_agent_mgr.h"
#include "local_scheduler/instance_control/instance_ctrl_message.h"
#include "local_scheduler/local_scheduler_service/local_sched_srv.h"

#include "function_proxy/common/parked_instance_registry/parked_instance_registry.h"

namespace functionsystem::local_scheduler {

namespace {
constexpr int32_t DEFAULT_PAUSE_TTL_SECONDS = 90'000;
constexpr uint32_t SNAPSHOT_ATTEMPT_PROTOCOL_VERSION = 1;
constexpr size_t ANONYMOUS_SNAPSHOT_ID_DIGEST_LENGTH = 40;

}

SnapCtrlActor::SnapCtrlActor(const std::string &name, const std::string &nodeID,
                             PauseRetryPolicy pauseRetryPolicy)
    : BasisActor(name), nodeID_(nodeID), pauseRetryPolicy_(pauseRetryPolicy)
{
}

void SnapCtrlActor::Init()
{
    BasisActor::Init();
    snapshotWorker_ = std::make_shared<ActorWorker>();
    YRLOG_INFO("SnapCtrlActor initialized on node: {}", nodeID_);
}

void SnapCtrlActor::Finalize()
{
    KillResponse response;
    response.set_code(common::ERR_INNER_COMMUNICATION);
    response.set_message("snapshot control actor shut down while operation was pending");
    for (auto &[instanceID, lifecycle] : instanceLifecycles_) {
        (void)instanceID;
        if (lifecycle.pauseContext != nullptr) {
            lifecycle.pauseContext->completion->SetValue(response);
        }
        if (lifecycle.deletePreparation != nullptr) {
            lifecycle.deletePreparation->SetFailed(
                static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION));
        }
    }
    instanceLifecycles_.clear();
    snapshotWorker_.reset();
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

litebus::Future<KillResponse> SnapCtrlActor::HandleSnapshot(const std::string &requestID, const std::string &instanceID,
                                                            const std::string &payload)
{
    // 1. 解析 payload 获取参数（core_service::SnapOptions）
    SnapOptions options;
    bool leaveRunning = false;
    int32_t ttl = 0;  // Default TTL is 0 (no expiration)
    uint64_t checkpointTimeoutMs = 0;
    std::string functionType;
    if (!payload.empty()) {
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
        checkpointTimeoutMs = options.checkpointtimeoutms();
    }
    if (options.type() == common::SnapType::PAUSE_RESUME
        && (instanceControlView_ == nullptr || instanceCtrl_ == nullptr || clientManager_ == nullptr
            || functionAgentMgr_ == nullptr)) {
        KillResponse errorRsp;
        errorRsp.set_code(common::ERR_INNER_SYSTEM_ERROR);
        errorRsp.set_message("pause dependencies are unavailable");
        return errorRsp;
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
    if (options.type() == common::SnapType::PAUSE_RESUME) {
        const auto effectiveTTL = ttl > 0 ? ttl : DEFAULT_PAUSE_TTL_SECONDS;
        return HandlePauseResumeSnapshot(
            requestID, instanceID, stateMachine, effectiveTTL, checkpointTimeoutMs);
    }
    auto instanceInfo = stateMachine->GetInstanceInfo();
    if (options.type() == common::SnapType::SNAPSHOT) {
        return HandleReusableSnapshot(requestID, instanceID, instanceInfo, options);
    }
    if (checkpointTimeoutMs > 0) {
        (*instanceInfo.mutable_createoptions())["YR_CHECKPOINT_TIMEOUT_MS"] =
            std::to_string(checkpointTimeoutMs);
    }
    YRLOG_INFO("{}|{}|start snapshot, leave_running: {}", requestID, instanceID, leaveRunning);
    ASSERT_IF_NULL(functionAgentMgr_);
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
        .Then([this, aid(GetAID()), requestID, instanceID, instanceInfo, ttl, markParked, forceOnDrainTimeout,
               functionAgentMgr(functionAgentMgr_)](const Status &drainStatus)
                   -> litebus::Future<messages::SnapshotRuntimeResponse> {
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
            return PrepareSnap(requestID, instanceID)
                .Then([aid, requestID, instanceID, instanceInfo, ttl, markParked,
                       functionAgentMgr](const Status &status) -> litebus::Future<messages::SnapshotRuntimeResponse> {
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
                        return functionAgentMgr->SnapshotRuntime(requestID, instanceInfo, ttl);
                    });
        })
        .Then([aid(GetAID()), localSchedSrv(localSchedSrv_), requestID, instanceInfo,
               functionType](const messages::SnapshotRuntimeResponse &runtimeRsp) -> litebus::Future<SnapshotResult> {
            return RecordSnapshotMetadata(localSchedSrv, runtimeRsp, instanceInfo, functionType);
        })
        .Then([requestID, instanceID, leaveRunning, markParked, aid(GetAID()),
               instanceCtrl(instanceCtrl_)](const SnapshotResult &result) -> litebus::Future<SnapshotResult> {
            if (result.code != common::ERR_NONE) {
                // snapshot failed: the instance keeps running (or died for real), drop
                // the park mark so later exit events take the legacy teardown path.
                if (markParked) {
                    // park abandoned before the delete: roll the drain back (no-op if
                    // the instance actually died — the FATAL path then governs)
                    function_proxy::ParkedInstanceRegistry::Instance().ReleaseDrain(instanceID);
                    function_proxy::ParkedInstanceRegistry::Instance().Clear(instanceID);
                }
                return result;
            }

            // 5. 日志记录最终状态
            if (!leaveRunning) {
                YRLOG_INFO("{}|{}|snapshot completed, deleting instance", requestID, instanceID);
                // 调用 ForceDeleteInstance 删除实例
                if (instanceCtrl != nullptr) {
                    instanceCtrl->ForceDeleteInstance(instanceID);
                } else {
                    YRLOG_WARN("{}|{}|instanceCtrl not bound, cannot delete instance", requestID, instanceID);
                }
            } else {
                YRLOG_INFO("{}|{}|snapshot completed, instance continues running", requestID, instanceID);
                if (markParked) {  // defensive: leaveRunning cannot change mid-flight
                    function_proxy::ParkedInstanceRegistry::Instance().Clear(instanceID);
                }
            }

            return result;
        })
        .Then(litebus::Defer(GetAID(), &SnapCtrlActor::OnHandleSnapshot, std::placeholders::_1));
}

litebus::Future<KillResponse> SnapCtrlActor::HandleAnonymousCheckpoint(
    const std::string &requestID, const std::string &instanceID,
    uint64_t checkpointTimeoutMs)
{
    auto completion = std::make_shared<litebus::Promise<KillResponse>>();
    auto context = std::make_shared<AnonymousCheckpointContext>();
    context->requestID = requestID;
    context->instanceID = instanceID;
    context->snapshotID = "anon-" + resume_identity::Sha256Hex(
        requestID + std::string(1, '\0') + instanceID).substr(
            0, ANONYMOUS_SNAPSHOT_ID_DIGEST_LENGTH);
    context->completion = completion;
    if (instanceControlView_ == nullptr || clientManager_ == nullptr || functionAgentMgr_ == nullptr) {
        CompleteAnonymousCheckpoint(
            context, common::ERR_INNER_SYSTEM_ERROR,
            "anonymous checkpoint dependencies are unavailable");
        return completion->GetFuture();
    }
    const auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr
        || stateMachine->GetInstanceState() != InstanceState::RUNNING) {
        CompleteAnonymousCheckpoint(
            context, common::ERR_STATE_MACHINE_ERROR,
            "anonymous checkpoint requires a running instance");
        return completion->GetFuture();
    }
    context->instanceInfo = stateMachine->GetInstanceInfo();
    if (checkpointTimeoutMs > 0) {
        (*context->instanceInfo.mutable_createoptions())["YR_CHECKPOINT_TIMEOUT_MS"] =
            std::to_string(checkpointTimeoutMs);
    }
    PrepareSnap(requestID, instanceID).OnComplete(litebus::Defer(
        GetAID(), &SnapCtrlActor::OnAnonymousCheckpointPrepared,
        context, std::placeholders::_1));
    return completion->GetFuture();
}

void SnapCtrlActor::OnAnonymousCheckpointPrepared(
    const std::shared_ptr<AnonymousCheckpointContext> &context,
    const litebus::Future<Status> &future)
{
    if (future.IsError() || future.Get().IsError()) {
        CompleteAnonymousCheckpoint(
            context, common::ERR_INNER_SYSTEM_ERROR,
            future.IsError() ? "anonymous checkpoint PrepareSnap future failed"
                             : future.Get().RawMessage());
        return;
    }
    functionAgentMgr_->SnapshotRuntimeAnonymous(
        context->requestID, context->instanceInfo, context->snapshotID)
        .OnComplete(litebus::Defer(
            GetAID(), &SnapCtrlActor::OnAnonymousCheckpointCreated,
            context, std::placeholders::_1));
}

void SnapCtrlActor::OnAnonymousCheckpointCreated(
    const std::shared_ptr<AnonymousCheckpointContext> &context,
    const litebus::Future<messages::SnapshotRuntimeResponse> &future)
{
    if (future.IsError()) {
        context->snapshotResponse.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION));
        context->snapshotResponse.set_message("anonymous SnapshotRuntime future failed");
    } else {
        context->snapshotResponse = future.Get();
    }
    if (context->snapshotResponse.code() != static_cast<int32_t>(StatusCode::SUCCESS)
        && context->snapshotResponse.checkpointnotstarted()
        && !context->snapshotResponse.resultunknown()) {
        CompleteAnonymousCheckpoint(
            context, Status::GetPosixErrorCode(context->snapshotResponse.code()),
            context->snapshotResponse.message());
        return;
    }
    context->snapStartedDeadline = std::chrono::steady_clock::now()
        + std::chrono::milliseconds(pauseRetryPolicy_.operationTimeoutMs);
    RetryAnonymousCheckpointClient(context);
}

void SnapCtrlActor::RetryAnonymousCheckpointClient(
    const std::shared_ptr<AnonymousCheckpointContext> &context)
{
    if (std::chrono::steady_clock::now() >= context->snapStartedDeadline) {
        CompleteAnonymousCheckpoint(
            context, common::ERR_INNER_COMMUNICATION,
            "timed out waiting for runtime reconnect after anonymous checkpoint");
        return;
    }
    clientManager_->GetControlInterfacePosixClient(context->instanceID)
        .OnComplete(litebus::Defer(
            GetAID(), &SnapCtrlActor::OnAnonymousCheckpointClient,
            context, std::placeholders::_1));
}

void SnapCtrlActor::OnAnonymousCheckpointClient(
    const std::shared_ptr<AnonymousCheckpointContext> &context,
    const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &future)
{
    if (future.IsError() || future.Get() == nullptr) {
        (void)litebus::AsyncAfter(
            pauseRetryPolicy_.initialDelayMs, GetAID(),
            &SnapCtrlActor::RetryAnonymousCheckpointClient, context);
        return;
    }
    runtime::SnapStartedRequest request;
    future.Get()->SnapStarted(std::move(request)).OnComplete(litebus::Defer(
        GetAID(), &SnapCtrlActor::OnAnonymousCheckpointStarted,
        context, std::placeholders::_1));
}

void SnapCtrlActor::OnAnonymousCheckpointStarted(
    const std::shared_ptr<AnonymousCheckpointContext> &context,
    const litebus::Future<runtime::SnapStartedResponse> &future)
{
    if (future.IsError()
        || future.Get().code() == common::ERR_REQUEST_BETWEEN_RUNTIME_BUS) {
        (void)litebus::AsyncAfter(
            pauseRetryPolicy_.initialDelayMs, GetAID(),
            &SnapCtrlActor::RetryAnonymousCheckpointClient, context);
        return;
    }
    if (future.Get().code() != common::ERR_NONE) {
        CompleteAnonymousCheckpoint(
            context, common::ERR_INNER_SYSTEM_ERROR,
            future.Get().message());
        return;
    }
    if (context->snapshotResponse.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
        CompleteAnonymousCheckpoint(
            context, Status::GetPosixErrorCode(context->snapshotResponse.code()),
            context->snapshotResponse.message());
        return;
    }
    KillResponse response;
    response.set_code(common::ERR_NONE);
    if (context->snapshotResponse.has_localsnapshot()) {
        response.set_payload(context->snapshotResponse.localsnapshot().SerializeAsString());
    }
    context->completion->SetValue(response);
}

void SnapCtrlActor::CompleteAnonymousCheckpoint(
    const std::shared_ptr<AnonymousCheckpointContext> &context,
    common::ErrorCode code, const std::string &message)
{
    KillResponse response;
    response.set_code(code);
    response.set_message(message);
    response.set_checkpointnotstarted(context->snapshotResponse.checkpointnotstarted()
                                     && !context->snapshotResponse.resultunknown());
    context->completion->SetValue(response);
}

litebus::Future<KillResponse> SnapCtrlActor::HandleReusableSnapshot(
    const std::string &requestID, const std::string &instanceID,
    const resources::InstanceInfo &instanceInfo, const SnapOptions &options)
{
    auto context = std::make_shared<ReusableSnapshotContext>();
    context->requestID = requestID;
    context->instanceID = instanceID;
    context->name = options.name();
    context->sourceInstanceInfo = instanceInfo;
    if (options.checkpointtimeoutms() > 0) {
        (*context->sourceInstanceInfo.mutable_createoptions())["YR_CHECKPOINT_TIMEOUT_MS"] =
            std::to_string(options.checkpointtimeoutms());
    }
    context->completion = std::make_shared<litebus::Promise<KillResponse>>();

    if (requestID.empty() || instanceID.empty() || instanceInfo.tenantid().empty()
        || instanceInfo.runtimeid().empty() || instanceInfo.functionagentid().empty()
        || instanceInfo.version() == 0 || localSchedSrv_ == nullptr
        || functionAgentMgr_ == nullptr || clientManager_ == nullptr) {
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                         common::ERR_INNER_SYSTEM_ERROR,
                         "reusable snapshot source identity or dependency is unavailable"));
        return context->completion->GetFuture();
    }

    if (reusableSnapshotTunnelGateAcquire_ != nullptr) {
        if (!reusableSnapshotTunnelGateAcquire_(instanceID)) {
            CompleteReusableSnapshotRequest(
                context, BuildReusableSnapshotResponse(
                             common::ERR_INNER_SYSTEM_ERROR,
                             "reusable snapshot is not allowed while a reverse tunnel is active"));
            return context->completion->GetFuture();
        }
        context->tunnelGateHeld = true;
    }

    auto request = std::make_shared<::messages::BeginReusableSnapshotRequest>();
    request->set_requestid(requestID);
    request->set_tenantid(instanceInfo.tenantid());
    request->set_sourceinstanceid(instanceID);
    if (!options.name().empty()) {
        request->add_names(options.name());
    }
    localSchedSrv_->BeginReusableSnapshot(request).OnComplete(
        litebus::Defer(GetAID(), &SnapCtrlActor::OnReusableSnapshotBegun,
                       context, std::placeholders::_1));
    return context->completion->GetFuture();
}

void SnapCtrlActor::OnReusableSnapshotBegun(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    const litebus::Future<::messages::BeginReusableSnapshotResponse> &future)
{
    if (future.IsError()) {
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                         common::ERR_INNER_COMMUNICATION,
                         "reusable snapshot Begin request failed"));
        return;
    }
    const auto &response = future.Get();
    if (response.code() != common::ERR_NONE) {
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                         static_cast<common::ErrorCode>(response.code()), response.message()));
        return;
    }
    if (response.snapshotid().empty()) {
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                         common::ERR_INNER_SYSTEM_ERROR,
                         "reusable snapshot Begin response omitted snapshot ID"));
        return;
    }
    context->snapshotID = response.snapshotid();
    if (response.has_snapshotinfo()) {
        context->publicInfo = response.snapshotinfo();
    }
    if (context->publicInfo.snapshotid().empty()) {
        context->publicInfo.set_snapshotid(context->snapshotID);
        if (!context->name.empty()) {
            context->publicInfo.add_names(context->name);
        }
    }

    if (response.phase() == ::messages::REUSABLE_SNAPSHOT_READY) {
        auto request = std::make_shared<::messages::ResolveReusableSnapshotForCreateRequest>();
        request->set_requestid(context->requestID);
        request->set_tenantid(context->sourceInstanceInfo.tenantid());
        request->set_snapshotid(context->snapshotID);
        localSchedSrv_->ResolveReusableSnapshotForCreate(request).OnComplete(
            litebus::Defer(GetAID(), &SnapCtrlActor::OnReusableSnapshotResolvedForCleanup,
                           context, std::placeholders::_1));
        return;
    }
    if (response.phase() != ::messages::REUSABLE_SNAPSHOT_PUBLISHING) {
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                         common::ERR_INNER_SYSTEM_ERROR,
                         "reusable snapshot Begin returned an invalid phase"));
        return;
    }

    PrepareSnap(context->requestID, context->instanceID).OnComplete(
        litebus::Defer(GetAID(), &SnapCtrlActor::OnReusableSnapshotPrepared,
                       context, std::placeholders::_1));
}

void SnapCtrlActor::OnReusableSnapshotResolvedForCleanup(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    const litebus::Future<::messages::ResolveReusableSnapshotForCreateResponse> &future)
{
    if (future.IsError()) {
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                         common::ERR_INNER_COMMUNICATION,
                         "reusable snapshot READY cleanup resolution failed"));
        return;
    }
    const auto &response = future.Get();
    if (response.code() != common::ERR_NONE
        || !response.has_reusablesnapshotrestore()
        || !response.reusablesnapshotrestore().has_artifact()) {
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                         response.code() == common::ERR_NONE ? common::ERR_INNER_SYSTEM_ERROR
                                                             : static_cast<common::ErrorCode>(response.code()),
                         response.message().empty()
                             ? "reusable snapshot READY record omitted artifact"
                             : response.message()));
        return;
    }
    context->artifact = response.reusablesnapshotrestore().artifact();
    FinalizeReusableSnapshot(context, ::messages::REUSABLE_SNAPSHOT_COMMITTED,
                             common::ERR_NONE, "");
}

void SnapCtrlActor::OnReusableSnapshotPrepared(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    const litebus::Future<Status> &future)
{
    if (future.IsError() || future.Get().IsError()) {
        const auto message = future.IsError() ? "reusable snapshot PrepareSnap future failed"
                                              : future.Get().RawMessage();
        FailReusableSnapshot(context, common::ERR_INNER_COMMUNICATION, message);
        return;
    }
    functionAgentMgr_->SnapshotRuntime(
        context->requestID, context->sourceInstanceInfo, 0, common::SNAPSHOT,
        context->snapshotID, {}).OnComplete(
            litebus::Defer(GetAID(), &SnapCtrlActor::OnReusableSnapshotCheckpointed,
                           context, std::placeholders::_1));
}

void SnapCtrlActor::OnReusableSnapshotCheckpointed(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    const litebus::Future<::messages::SnapshotRuntimeResponse> &future)
{
    if (future.IsError() || future.Get().code() != common::ERR_NONE
        || !future.Get().has_localsnapshot()) {
        FailReusableSnapshot(
            context, common::ERR_INNER_COMMUNICATION,
            future.IsError() ? "reusable snapshot LOCAL_READY future failed"
                             : future.Get().message());
        return;
    }
    clientManager_->GetControlInterfacePosixClient(context->instanceID)
        .OnComplete(litebus::Defer(
            GetAID(), &SnapCtrlActor::OnReusableSnapshotClient,
            context, std::placeholders::_1));
}

void SnapCtrlActor::OnReusableSnapshotPublished(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    const litebus::Future<::messages::SnapshotRuntimeResponse> &future)
{
    if (future.IsError()) {
        // The Agent may have checkpointed or published successfully before
        // its reply was lost. Without exact artifact facts, neither local nor
        // remote deletion is safe. Keep PUBLISHING for deterministic replay.
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                common::ERR_INNER_COMMUNICATION,
                "reusable snapshot Agent result is unknown"));
        return;
    }
    const auto &response = future.Get();
    if (response.code() != common::ERR_NONE) {
        if (response.has_reusablesnapshotartifact()
            && !response.reusablesnapshotartifact().storagebackend().empty()
            && !response.reusablesnapshotartifact().objectkey().empty()
            && response.reusablesnapshotartifact().size() > 0
            && !response.reusablesnapshotartifact().sha256().empty()) {
            context->artifact = response.reusablesnapshotartifact();
            if (context->artifact.storagebackend() == "local") {
                context->artifact.set_sourcenodeid(nodeID_);
            }
            FinalizeReusableSnapshot(context, ::messages::REUSABLE_SNAPSHOT_ABORTED,
                                     static_cast<common::ErrorCode>(response.code()),
                                     response.message());
            return;
        }
        // The publisher did not return an exact physical identity. Keep the
        // Master record in PUBLISHING so an idempotent replay can re-inspect
        // the deterministic checkpoint. Removing the record here would
        // orphan local/remote data that cannot be deleted safely.
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                static_cast<common::ErrorCode>(response.code()),
                response.message().empty()
                    ? "reusable snapshot publish failed without exact cleanup identity"
                    : response.message()));
        return;
    }
    if (!response.has_reusablesnapshotartifact()) {
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                common::ERR_INNER_SYSTEM_ERROR,
                "reusable snapshot Agent response omitted frozen artifact; publishing preserved"));
        return;
    }
    context->artifact = response.reusablesnapshotartifact();
    if (context->artifact.storagebackend() == "local") {
        context->artifact.set_sourcenodeid(nodeID_);
    }
    auto request = std::make_shared<::messages::CommitReusableSnapshotRequest>();
    request->set_requestid(context->requestID);
    request->set_tenantid(context->sourceInstanceInfo.tenantid());
    request->set_snapshotid(context->snapshotID);
    *request->mutable_sourceinstanceinfo() = context->sourceInstanceInfo;
    *request->mutable_artifact() = context->artifact;
    localSchedSrv_->CommitReusableSnapshot(request).OnComplete(
        litebus::Defer(GetAID(), &SnapCtrlActor::OnReusableSnapshotCommitted,
                       context, std::placeholders::_1));
}

void SnapCtrlActor::OnReusableSnapshotClient(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &future)
{
    if (future.IsError() || future.Get() == nullptr) {
        FinalizeReusableSnapshot(
            context, ::messages::REUSABLE_SNAPSHOT_ABORTED,
            common::ERR_INNER_COMMUNICATION,
            "failed to get runtime client for reusable SnapStarted");
        return;
    }
    runtime::SnapStartedRequest request;
    future.Get()->SnapStarted(std::move(request)).OnComplete(litebus::Defer(
        GetAID(), &SnapCtrlActor::OnReusableSnapshotStarted,
        context, std::placeholders::_1));
}

void SnapCtrlActor::OnReusableSnapshotStarted(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    const litebus::Future<runtime::SnapStartedResponse> &future)
{
    if (future.IsError() || future.Get().code() != common::ERR_NONE) {
        FinalizeReusableSnapshot(
            context, ::messages::REUSABLE_SNAPSHOT_ABORTED,
            common::ERR_INNER_COMMUNICATION,
            future.IsError() ? "reusable SnapStarted future failed"
                             : future.Get().message());
        return;
    }
    functionAgentMgr_->PublishSnapshotArtifact(
        context->requestID, context->sourceInstanceInfo, context->snapshotID)
        .OnComplete(litebus::Defer(
            GetAID(), &SnapCtrlActor::OnReusableSnapshotPublished,
            context, std::placeholders::_1));
}

void SnapCtrlActor::OnReusableSnapshotCommitted(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    const litebus::Future<::messages::CommitReusableSnapshotResponse> &future)
{
    if (future.IsError()) {
        // The Master may have committed READY even though its reply was lost.
        // Preserve both PUBLISHING/READY coordination and the exact artifact so
        // an idempotent replay can Resolve and finish cleanup safely.
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                         common::ERR_INNER_COMMUNICATION,
                         "reusable snapshot Commit result is unknown"));
        return;
    }
    const auto &response = future.Get();
    if (response.code() != common::ERR_NONE) {
        FinalizeReusableSnapshot(context, ::messages::REUSABLE_SNAPSHOT_ABORTED,
                                 static_cast<common::ErrorCode>(response.code()),
                                 response.message());
        return;
    }
    if (!response.has_snapshotinfo()
        || response.snapshotinfo().snapshotid() != context->snapshotID) {
        FinalizeReusableSnapshot(
            context, ::messages::REUSABLE_SNAPSHOT_ABORTED,
            common::ERR_INNER_SYSTEM_ERROR,
            "reusable snapshot Commit response omitted matching public metadata");
        return;
    }
    context->publicInfo = response.snapshotinfo();
    FinalizeReusableSnapshot(context, ::messages::REUSABLE_SNAPSHOT_COMMITTED,
                             common::ERR_NONE, "");
}

void SnapCtrlActor::FinalizeReusableSnapshot(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    ::messages::SnapshotAttemptFinalizeOperation operation,
    common::ErrorCode terminalCode, const std::string &terminalMessage)
{
    if (context->artifact.storagebackend().empty() || context->artifact.objectkey().empty()
        || (context->artifact.storagebackend() == "local"
            && context->artifact.sourcenodeid().empty())) {
        const auto message = "reusable snapshot exact cleanup artifact identity is incomplete";
        // Keep the PUBLISHING/READY coordination record when exact cleanup
        // cannot be proven.  Deleting it here would orphan an immutable
        // object that this node can no longer identify safely.
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(common::ERR_INNER_SYSTEM_ERROR, message));
        return;
    }
    ::messages::SnapshotAttemptFinalizeRequest request;
    request.set_protocolversion(SNAPSHOT_ATTEMPT_PROTOCOL_VERSION);
    request.set_operation(operation);
    request.set_tenantid(context->sourceInstanceInfo.tenantid());
    request.set_instanceid(context->instanceID);
    request.set_snapshotid(context->snapshotID);
    request.set_attemptid(context->requestID);
    request.set_runtimeid(context->sourceInstanceInfo.runtimeid());
    request.set_expectedsize(static_cast<uint64_t>(context->artifact.size()));
    request.set_expectedsha256(context->artifact.sha256());
    request.set_expectedstorage(context->artifact.storagebackend());
    functionAgentMgr_->FinalizeSnapshotAttempt(context->sourceInstanceInfo, request).OnComplete(
        litebus::Defer(GetAID(), &SnapCtrlActor::OnReusableSnapshotFinalized,
                       context, operation, terminalCode, terminalMessage,
                       std::placeholders::_1));
}

void SnapCtrlActor::OnReusableSnapshotFinalized(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    ::messages::SnapshotAttemptFinalizeOperation operation,
    common::ErrorCode terminalCode, const std::string &terminalMessage,
    const litebus::Future<::messages::SnapshotAttemptFinalizeResponse> &future)
{
    if (future.IsError() || future.Get().code() != static_cast<int32_t>(StatusCode::SUCCESS)
        || future.Get().resultunknown() || !future.Get().localcleanupcomplete()
        || !future.Get().remotecleanupcomplete()) {
        const auto message = future.IsError()
            ? "reusable snapshot exact cleanup request failed"
            : (future.Get().message().empty()
                   ? "reusable snapshot exact cleanup is incomplete"
                   : future.Get().message());
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(common::ERR_INNER_COMMUNICATION, message));
        return;
    }
    if (operation == ::messages::REUSABLE_SNAPSHOT_ABORTED) {
        FailReusableSnapshot(context, terminalCode, terminalMessage);
        return;
    }
    CompleteReusableSnapshotRequest(
        context, BuildReusableSnapshotResponse(common::ERR_NONE, "", &context->publicInfo));
}

void SnapCtrlActor::FailReusableSnapshot(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    common::ErrorCode code, const std::string &message)
{
    auto request = std::make_shared<::messages::FailReusableSnapshotRequest>();
    request->set_requestid(context->requestID);
    request->set_tenantid(context->sourceInstanceInfo.tenantid());
    request->set_snapshotid(context->snapshotID);
    request->set_reason(message);
    localSchedSrv_->FailReusableSnapshot(request).OnComplete(
        litebus::Defer(GetAID(), &SnapCtrlActor::OnReusableSnapshotFailedRecord,
                       context, code, message, std::placeholders::_1));
}

void SnapCtrlActor::OnReusableSnapshotFailedRecord(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    common::ErrorCode terminalCode, const std::string &terminalMessage,
    const litebus::Future<::messages::FailReusableSnapshotResponse> &future)
{
    if (future.IsError() || future.Get().code() != common::ERR_NONE) {
        const auto cleanupMessage = future.IsError()
            ? "reusable snapshot Fail coordination request failed"
            : future.Get().message();
        CompleteReusableSnapshotRequest(
            context, BuildReusableSnapshotResponse(
                         common::ERR_INNER_COMMUNICATION,
                         terminalMessage + "; " + cleanupMessage));
        return;
    }
    CompleteReusableSnapshotRequest(
        context, BuildReusableSnapshotResponse(terminalCode, terminalMessage));
}

KillResponse SnapCtrlActor::BuildReusableSnapshotResponse(
    common::ErrorCode code, const std::string &message,
    const ::core_service::SnapshotInfo *snapshotInfo) const
{
    KillResponse response;
    response.set_code(code);
    response.set_message(message);
    if (code == common::ERR_NONE && snapshotInfo != nullptr) {
        response.set_payload(snapshotInfo->SerializeAsString());
    }
    return response;
}

void SnapCtrlActor::ReleaseReusableSnapshotTunnelGate(
    const std::shared_ptr<ReusableSnapshotContext> &context)
{
    if (!context->tunnelGateHeld) {
        return;
    }
    context->tunnelGateHeld = false;
    if (reusableSnapshotTunnelGateRelease_ != nullptr) {
        reusableSnapshotTunnelGateRelease_(context->instanceID);
    }
}

void SnapCtrlActor::CompleteReusableSnapshotRequest(
    const std::shared_ptr<ReusableSnapshotContext> &context,
    const KillResponse &response)
{
    if (context->completed) {
        return;
    }
    context->completed = true;
    ReleaseReusableSnapshotTunnelGate(context);
    context->completion->SetValue(response);
}

KillResponse SnapCtrlActor::OnHandleSnapshot(const SnapshotResult &result)
{
    KillResponse rsp;
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
    }
    return rsp;
}

litebus::Future<Status> SnapCtrlActor::PrepareSnap(const std::string &requestID, const std::string &instanceID)
{
    YRLOG_INFO("{}|{}|PrepareSnap: instance is running, getting client", requestID, instanceID);
    // 3. 获取 client 并调用 PrepareSnap
    ASSERT_IF_NULL(clientManager_);
    return clientManager_->GetControlInterfacePosixClient(instanceID)
        .Then([requestID, instanceID](const litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> &clientFuture)
                  -> litebus::Future<Status> {
            if (clientFuture.IsError() || clientFuture.Get() == nullptr) {
                YRLOG_ERROR("{}|{}|failed to get control interface client, error code: {}", requestID, instanceID,
                            clientFuture.GetErrorCode());
                return Status(StatusCode::FAILED, "failed to get control interface client");
            }
            auto client = clientFuture.Get();
            // 4. 调用 PrepareSnap 接口
            runtime::PrepareSnapRequest prepareReq{};
            return client->PrepareSnap(std::move(prepareReq))
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
        });
}
}  // namespace functionsystem::local_scheduler
