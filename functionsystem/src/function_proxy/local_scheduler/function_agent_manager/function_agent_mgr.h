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

#ifndef LOCAL_SCHEDULER_FUNCTION_AGENT_MGR_H
#define LOCAL_SCHEDULER_FUNCTION_AGENT_MGR_H
#include <async/future.hpp>
#include <memory>

#include "common/utils/actor_driver.h"
#include "common/logs/logging.h"
#include "common/proto/pb/message_pb.h"
#include "common/observer/tenant_listener.h"
#include "meta_store_monitor/meta_store_healthy_observer.h"
#include "function_agent_mgr_actor.h"

namespace functionsystem::local_scheduler {
using FuncAgentRegisInfoMap = std::unordered_map<std::string, messages::FuncAgentRegisInfo>;

class InstanceCtrl;
class FunctionAgentMgr : public TenantListener, public ActorDriver, public MetaStoreHealthyObserver {
public:
    /**
     * function agent manager constructor
     * @param: actor: actor in function agent manager
     */
    explicit FunctionAgentMgr(std::shared_ptr<FunctionAgentMgrActor> &&actor);
    ~FunctionAgentMgr() override = default;

    static std::unique_ptr<FunctionAgentMgr> Create(const std::string &nodeID,
                                                    const FunctionAgentMgrActor::Param &param,
                                                    const std::shared_ptr<MetaStoreClient> &metaStoreClient);

    void Start(const std::shared_ptr<InstanceCtrl> &instanceCtrl,
               const std::shared_ptr<resource_view::ResourceView> &resourceView);

    void BindLocalSchedSrv(const std::shared_ptr<LocalSchedSrv> &localSchedSrv);

    void BindBundleMgr(const std::shared_ptr<BundleMgr> &bundleMgr);

    virtual litebus::Future<Status> GracefulShutdown();

    /**
     * wrap Async call of deployInstance interface
     * @param request: deploy instance request from instance control
     * @param funcAgentID: funcAgent where the instance needs to be deploy
     * @param isRecovering: is deploy during recovering agent
     * @return deploy instance response
     */
    virtual litebus::Future<messages::DeployInstanceResponse> DeployInstance(
        const std::shared_ptr<messages::DeployInstanceRequest> &request, const std::string &funcAgentID);

    /**
     * wrap Async call of KillInstance interface
     * @param request: kill instance request from instance control
     * @param funcAgentID: funcAgent where the instance resides
     * @param isRecovering: whether is kill during recovering instance
     * @return kill instance response
     */
    virtual litebus::Future<messages::KillInstanceResponse> KillInstance(
        const std::shared_ptr<messages::KillInstanceRequest> &request, const std::string &funcAgentID,
        bool isRecovering = false);

    virtual litebus::Future<messages::InstanceStatusInfo> QueryInstanceStatusInfo(const std::string &funcAgentID,
                                                                                  const std::string &instanceID,
                                                                                  const std::string &runtimeID);
    virtual litebus::Future<Status> QueryDebugInstanceInfos();

    virtual litebus::Future<messages::UpdateCredResponse> UpdateCred(
        const std::string &funcAgentID, const std::shared_ptr<messages::UpdateCredRequest> &request);

    virtual litebus::Future<Status> EvictAgent(const std::shared_ptr<messages::EvictAgentRequest> &req);

    virtual litebus::Future<bool> IsFuncAgentRecovering(const std::string &funcAgentID);

    virtual void SetAbnormal();

    virtual litebus::Future<messages::StaticFunctionChangeResponse> NotifyFunctionStatusChange(
        const std::shared_ptr<messages::StaticFunctionChangeRequest> &request, const std::string &funcAgentID);

    void OnTenantUpdateInstance(const TenantEvent &event) override;

    void OnTenantDeleteInstance(const TenantEvent &event) override;

    void OnHealthyStatus(const Status &status) override;

    virtual litebus::Future<Status> RegisterToWarmUp(
        const std::shared_ptr<messages::DeployInstanceRequest> &request,
        const litebus::Option<std::string> &agentID);

    virtual litebus::Future<Status> UnRegisterWarmUp(
        const std::shared_ptr<messages::KillInstanceRequest> &request);

    virtual litebus::Future<messages::SnapshotRuntimeResponse> SnapshotRuntime(
        const std::string &requestID,
        const resource_view::InstanceInfo &instanceInfo,
        int32_t ttl = 0,
        bool leaveRunning = false);

    virtual litebus::Future<messages::ReconcileRuntimesResponse> ReconcileRuntimes(
        const std::string &funcAgentID,
        const std::shared_ptr<messages::ReconcileRuntimesRequest> &request);

    void SetCoProcessReconcileCallback(std::function<void(const std::string &)> callback)
    {
        ASSERT_IF_NULL(actor_);
        actor_->SetCoProcessReconcileCallback(std::move(callback));
    }

    // for test
    [[maybe_unused]] litebus::Future<bool> IsRegistered(const std::string &funcAgentID) const
    {
        ASSERT_IF_NULL(actor_);
        return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::IsRegistered, funcAgentID);
    }

    // for test
    [[maybe_unused]] void SetRetrySendCleanStatusInterval(uint32_t interval) const
    {
        ASSERT_IF_NULL(actor_);
        return actor_->SetRetrySendCleanStatusInterval(interval);
    }

    // for test
    [[maybe_unused]] std::string Dump() const
    {
        ASSERT_IF_NULL(actor_);
        return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::Dump).Get();
    }

    // for test
    [[maybe_unused]] litebus::AID GetActorAID() const
    {
        ASSERT_IF_NULL(actor_);
        return actor_->GetAID();
    }

    // for test
    [[maybe_unused]] litebus::Future<Status> PutAgentRegisInfoWithProxyNodeID()
    {
        ASSERT_IF_NULL(actor_);
        return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::PutAgentRegisInfoWithProxyNodeID);
    }

    // for test
    [[maybe_unused]] void SetFuncAgentsRegis(FuncAgentRegisInfoMap &funcAgentsRegis)
    {
        ASSERT_IF_NULL(actor_);
        return actor_->SetFuncAgentsRegis(funcAgentsRegis);
    }

    // for test
    [[maybe_unused]] std::unordered_map<std::string, messages::FuncAgentRegisInfo> GetFuncAgentsRegis()
    {
        ASSERT_IF_NULL(actor_);
        return litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::GetFuncAgentsRegis).Get();
    }

    // for test
    [[maybe_unused]] void ClearFuncAgentsRegis()
    {
        ASSERT_IF_NULL(actor_);
        litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::ClearFuncAgentsRegis).Wait();
    }

    // for test
    [[maybe_unused]] std::string GetNodeID()
    {
        ASSERT_IF_NULL(actor_);
        return actor_->GetNodeID();
    }

    // for test
    [[maybe_unused]] virtual void SetFuncAgentUpdateMapPromise(
        const std::string &funcAgentID, std::shared_ptr<resource_view::ResourceUnit> &resourceUnit)
    {
        ASSERT_IF_NULL(actor_);
        actor_->SetFuncAgentUpdateMapPromise(funcAgentID, resourceUnit);
    }

    // for test
    [[maybe_unused]] void UpdateResources(const litebus::AID &from, std::string &&name, std::string &&msg)
    {
        ASSERT_IF_NULL(actor_);
        actor_->UpdateResources(from, std::move(name), std::move(msg));
    }

    // for test
    [[maybe_unused]] void SetNodeID(const std::string &nodeID)
    {
        ASSERT_IF_NULL(actor_);
        actor_->SetNodeID(nodeID);
    }

    // for test
    [[maybe_unused]] void EnableAgents()
    {
        ASSERT_IF_NULL(actor_);
        litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::EnableAgents);
    }

    // for test
    [[maybe_unused]] void InsertAgent(const std::string agentID)
    {
        ASSERT_IF_NULL(actor_);
        litebus::Async(actor_->GetAID(), &FunctionAgentMgrActor::InsertAgent, agentID);
    }

    // for test
    [[maybe_unused]] std::unordered_map<std::string, std::shared_ptr<TenantCache>> GetTenantCacheMap()
    {
        return actor_->GetTenantCacheMap();
    }

    // for test
    [[maybe_unused]] void UpdateLocalStatus(const litebus::AID &from, std::string &&name, std::string &&msg)
    {
        ASSERT_IF_NULL(actor_);
        actor_->UpdateLocalStatus(from, std::move(name), std::move(msg));
    }

    // for test
    [[maybe_unused]] int32_t GetLocalStatus()
    {
        ASSERT_IF_NULL(actor_);
        return actor_->GetLocalStatus();
    }

    // for test
    [[maybe_unused]] void StaticFunctionScheduleRequest(const litebus::AID &from, std::string &&name, std::string &&msg)
    {
        ASSERT_IF_NULL(actor_);
        actor_->StaticFunctionScheduleRequest(from, std::move(name), std::move(msg));
    }

private:
    virtual void BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl);

    virtual void BindResourceView(const std::shared_ptr<resource_view::ResourceView> &resourceView);

    std::shared_ptr<FunctionAgentMgrActor> actor_;
};
}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_FUNCTION_AGENT_MGR_H
