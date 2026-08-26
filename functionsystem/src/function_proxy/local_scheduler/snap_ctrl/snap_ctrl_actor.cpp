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
#include <nlohmann/json.hpp>

#include "async/async.hpp"
#include "async/defer.hpp"
#include "common/constants/actor_name.h"
#include "common/logs/logging.h"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix_pb.h"
#include "common/resource_view/resource_type.h"
#include "common/utils/struct_transfer.h"
#include "local_scheduler/function_agent_manager/function_agent_mgr.h"
#include "local_scheduler/instance_control/idle/idle_mgr.h"
#include "function_proxy/common/parked_instance_registry/parked_instance_registry.h"
#include "local_scheduler/instance_control/instance_ctrl_message.h"
#include "local_scheduler/local_scheduler_service/local_sched_srv.h"

namespace functionsystem::local_scheduler {

SnapCtrlActor::SnapCtrlActor(const std::string &name, const std::string &nodeID)
    : BasisActor(name), nodeID_(nodeID)
{
}

void SnapCtrlActor::Init()
{
    BasisActor::Init();
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
    bool leaveRunning = false;
    int32_t ttl = 0;  // Default TTL is 0 (no expiration)
    std::string functionType;
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
        .Then([this, aid(GetAID()), requestID, instanceID, instanceInfo, ttl, leaveRunning, markParked,
               forceOnDrainTimeout, functionAgentMgr(functionAgentMgr_)](const Status &drainStatus)
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
                .Then([aid, requestID, instanceID, instanceInfo, ttl, leaveRunning, markParked,
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
                        return functionAgentMgr->SnapshotRuntime(requestID, instanceInfo, ttl, leaveRunning);
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
        .Then(litebus::Defer(GetAID(), &SnapCtrlActor::OnHandleSnapshot, instanceID, leaveRunning,
                             std::placeholders::_1));
}

KillResponse SnapCtrlActor::OnHandleSnapshot(const std::string &instanceID, bool leaveRunning,
                                             const SnapshotResult &result)
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
        if (!leaveRunning) {
            // park: the instance is deleted right after this response, so the
            // checkpoint is the only handle left. Record it for wake/FIFO unpark.
            ParkedEntry entry;
            entry.checkpointID = result.snapshotInfo.checkpointid();
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
    const auto checkpointID = it->second.checkpointID;
    YRLOG_INFO("{}|wake parked instance({}) from checkpoint({})", requestID, instanceID, checkpointID);
    return HandleSnapStart(requestID, checkpointID, "")
        .Then(litebus::Defer(GetAID(), &SnapCtrlActor::OnWakeComplete, instanceID, std::placeholders::_1));
}

KillResponse SnapCtrlActor::OnWakeComplete(const std::string &instanceID, const KillResponse &rsp)
{
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
                                          &SnapCtrlActor::RetryWake, instanceID);
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
    HandleWake(requestID, instanceID)
        .Then(litebus::Defer(GetAID(), &SnapCtrlActor::OnWakeComplete, instanceID, std::placeholders::_1));
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
