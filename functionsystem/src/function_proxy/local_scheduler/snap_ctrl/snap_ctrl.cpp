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

#include "snap_ctrl.h"

#include <async/async.hpp>

#include "common/constants/actor_name.h"
#include "common/logs/logging.h"
#include "local_scheduler/function_agent_manager/function_agent_mgr.h"
#include "local_scheduler/local_scheduler_service/local_sched_srv.h"

namespace functionsystem::local_scheduler {

SnapCtrl::SnapCtrl(const std::shared_ptr<SnapCtrlActor> &snapCtrlActor)
    : ActorDriver(snapCtrlActor), snapCtrlActor_(snapCtrlActor), aid_(snapCtrlActor->GetAID())
{
}

SnapCtrl::~SnapCtrl()
{
    Stop();
    Await();
}

std::unique_ptr<SnapCtrl> SnapCtrl::Create(const std::string &nodeID)
{
    auto actor = std::make_shared<SnapCtrlActor>(SNAP_CTRL_ACTOR_NAME, nodeID);
    litebus::Spawn(actor);
    return std::make_unique<SnapCtrl>(actor);
}

void SnapCtrl::Stop()
{
    if (snapCtrlActor_ != nullptr) {
        litebus::Terminate(snapCtrlActor_->GetAID());
    }
}

void SnapCtrl::Await()
{
    if (snapCtrlActor_ != nullptr) {
        litebus::Await(snapCtrlActor_->GetAID());
        snapCtrlActor_ = nullptr;
    }
}

void SnapCtrl::BindFunctionAgentMgr(const std::shared_ptr<FunctionAgentMgr> &functionAgentMgr)
{
    ASSERT_IF_NULL(snapCtrlActor_);
    litebus::Async(aid_, &SnapCtrlActor::BindFunctionAgentMgr, functionAgentMgr);
}

void SnapCtrl::BindLocalSchedSrv(const std::shared_ptr<LocalSchedSrv> &localSchedSrv)
{
    ASSERT_IF_NULL(snapCtrlActor_);
    litebus::Async(aid_, &SnapCtrlActor::BindLocalSchedSrv, localSchedSrv);
}

void SnapCtrl::BindInstanceControlView(const std::shared_ptr<InstanceControlView> &instanceControlView)
{
    ASSERT_IF_NULL(snapCtrlActor_);
    litebus::Async(aid_, &SnapCtrlActor::BindInstanceControlView, instanceControlView);
}

void SnapCtrl::BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl)
{
    ASSERT_IF_NULL(snapCtrlActor_);
    litebus::Async(aid_, &SnapCtrlActor::BindInstanceCtrl, instanceCtrl);
}

void SnapCtrl::BindIdleMgr(const std::shared_ptr<IdleMgr> &idleMgr)
{
    ASSERT_IF_NULL(snapCtrlActor_);
    litebus::Async(aid_, &SnapCtrlActor::BindIdleMgr, idleMgr);
}

void SnapCtrl::BindClientManager(const std::shared_ptr<ControlInterfaceClientManagerProxy> &clientManager)
{
    ASSERT_IF_NULL(snapCtrlActor_);
    litebus::Async(aid_, &SnapCtrlActor::BindClientManager, clientManager);
}

litebus::Future<KillResponse> SnapCtrl::HandleSnapshot(const std::string &requestID,
                                                       const std::string &instanceID,
                                                       const std::string &payload)
{
    ASSERT_IF_NULL(snapCtrlActor_);
    return litebus::Async(aid_, &SnapCtrlActor::HandleSnapshot, requestID, instanceID, payload);
}

litebus::Future<KillResponse> SnapCtrl::HandleSnapStart(
    const std::string &requestID, const std::string &checkpointID, const std::string &payload)
{
    ASSERT_IF_NULL(snapCtrlActor_);
    return litebus::Async(aid_, &SnapCtrlActor::HandleSnapStart, requestID, checkpointID, payload);
}

litebus::Future<KillResponse> SnapCtrl::HandleWake(const std::string &requestID, const std::string &instanceID)
{
    ASSERT_IF_NULL(snapCtrlActor_);
    return litebus::Async(aid_, &SnapCtrlActor::HandleWake, requestID, instanceID);
}

litebus::Future<std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>>> SnapCtrl::GetParkedInstances()
{
    ASSERT_IF_NULL(snapCtrlActor_);
    return litebus::Async(aid_, &SnapCtrlActor::GetParkedInstances);
}

void SnapCtrl::SnapStart(
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq, const schedule_decision::ScheduleResult &result,
    const TransitionResult &transResult)
{
    ASSERT_IF_NULL(snapCtrlActor_);
    litebus::Async(aid_, &SnapCtrlActor::SnapStart, scheduleResp, scheduleReq, result, transResult);
}

}  // namespace functionsystem::local_scheduler
