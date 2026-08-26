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

#include "function_agent_mgr.h"

#include "common/constants/actor_name.h"
#include "function_agent_mgr_actor.h"
#include "local_scheduler/instance_control/instance_ctrl.h"

namespace functionsystem::local_scheduler {
FunctionAgentMgr::FunctionAgentMgr(std::shared_ptr<FunctionAgentMgrActor> &&actor)
    : ActorDriver(actor), actor_(std::move(actor))
{
    YRLOG_INFO("create FunctionAgentMgrActor({}) successfully.", actor_->GetAID().HashString());
}

std::unique_ptr<FunctionAgentMgr> FunctionAgentMgr::Create(const std::string &nodeID,
                                                           const FunctionAgentMgrActor::Param &param,
                                                           const std::shared_ptr<MetaStoreClient> &metaStoreClient)
{
    std::string aid = nodeID + LOCAL_SCHED_FUNC_AGENT_MGR_ACTOR_NAME_POSTFIX;
    auto actor = std::make_shared<FunctionAgentMgrActor>(aid, param, nodeID, metaStoreClient);
    return std::make_unique<FunctionAgentMgr>(std::move(actor));
}

void FunctionAgentMgr::Start(const std::shared_ptr<InstanceCtrl> &instanceCtrl,
                             const std::shared_ptr<resource_view::ResourceView> &resourceView)
{
    BindResourceView(resourceView);
    BindInstanceCtrl(instanceCtrl);

    (void)litebus::Spawn(actor_);
}

litebus::Future<messages::DeployInstanceResponse> FunctionAgentMgr::DeployInstance(
    const std::shared_ptr<messages::DeployInstanceRequest> &request, const std::string &funcAgentID)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::DeployInstance, request, funcAgentID);
}

litebus::Future<messages::KillInstanceResponse> FunctionAgentMgr::KillInstance(
    const std::shared_ptr<messages::KillInstanceRequest> &request, const std::string &funcAgentID, bool isRecovering)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::KillInstance, request, funcAgentID, isRecovering);
}

litebus::Future<messages::InstanceStatusInfo> FunctionAgentMgr::QueryInstanceStatusInfo(const std::string &funcAgentID,
                                                                                        const std::string &instanceID,
                                                                                        const std::string &runtimeID)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::QueryInstanceStatusInfo, funcAgentID, instanceID,
                          runtimeID);
}


litebus::Future<Status> FunctionAgentMgr::QueryDebugInstanceInfos()
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::QueryDebugInstanceInfos);
}

void FunctionAgentMgr::BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl)
{
    ASSERT_IF_NULL(instanceCtrl);
    ASSERT_IF_NULL(actor_);
    actor_->BindInstanceCtrl(instanceCtrl);
}

void FunctionAgentMgr::BindResourceView(const std::shared_ptr<resource_view::ResourceView> &resourceView)
{
    ASSERT_IF_NULL(resourceView);
    ASSERT_IF_NULL(actor_);
    actor_->BindResourceView(resourceView);
}

litebus::Future<messages::UpdateCredResponse> FunctionAgentMgr::UpdateCred(
    const std::string &funcAgentID, const std::shared_ptr<messages::UpdateCredRequest> &request)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::UpdateCred, funcAgentID, request);
}

litebus::Future<messages::SnapshotRuntimeResponse> FunctionAgentMgr::SnapshotRuntime(
    const std::string &requestID,
    const resource_view::InstanceInfo &instanceInfo,
    int32_t ttl,
    bool leaveRunning)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::SnapshotRuntime, requestID, instanceInfo, ttl,
                          leaveRunning);
}

litebus::Future<messages::ReconcileRuntimesResponse> FunctionAgentMgr::ReconcileRuntimes(
    const std::string &funcAgentID, const std::shared_ptr<messages::ReconcileRuntimesRequest> &request)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::ReconcileRuntimes, funcAgentID, request);
}


litebus::Future<Status> FunctionAgentMgr::EvictAgent(const std::shared_ptr<messages::EvictAgentRequest> &req)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::EvictAgent, req);
}

void FunctionAgentMgr::BindLocalSchedSrv(const std::shared_ptr<LocalSchedSrv> &localSchedSrv)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::BindLocalSchedSrv, localSchedSrv);
}

void FunctionAgentMgr::BindBundleMgr(const std::shared_ptr<BundleMgr> &bundleMgr)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::BindBundleMgr, bundleMgr);
}

void FunctionAgentMgr::OnTenantUpdateInstance(const TenantEvent &event)
{
    ASSERT_IF_NULL(actor_);
    litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::OnTenantUpdateInstance, event);
}

void FunctionAgentMgr::OnTenantDeleteInstance(const TenantEvent &event)
{
    ASSERT_IF_NULL(actor_);
    litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::OnTenantDeleteInstance, event);
}

void FunctionAgentMgr::OnHealthyStatus(const Status &status)
{
    ASSERT_IF_NULL(actor_);
    litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::OnHealthyStatus, status);
}

litebus::Future<bool> FunctionAgentMgr::IsFuncAgentRecovering(const std::string &funcAgentID)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::IsFuncAgentRecovering, funcAgentID);
}

litebus::Future<Status> FunctionAgentMgr::GracefulShutdown()
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::GracefulShutdown);
}

void FunctionAgentMgr::SetAbnormal()
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::SetAbnormal);
}

litebus::Future<messages::StaticFunctionChangeResponse> FunctionAgentMgr::NotifyFunctionStatusChange(
    const std::shared_ptr<messages::StaticFunctionChangeRequest> &request, const std::string &funcAgentID)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::NotifyFunctionStatusChange, request, funcAgentID);
}

litebus::Future<Status> FunctionAgentMgr::RegisterToWarmUp(
    const std::shared_ptr<messages::DeployInstanceRequest> &request,
    const litebus::Option<std::string> &agentID)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::RegisterToWarmUp, request, agentID);
}

litebus::Future<Status> FunctionAgentMgr::UnRegisterWarmUp(
    const std::shared_ptr<messages::KillInstanceRequest> &request)
{
    ASSERT_IF_NULL(actor_);
    return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::UnRegisterWarmUp, request);
}
}  // namespace functionsystem::local_scheduler