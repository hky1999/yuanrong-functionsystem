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

#ifndef LOCAL_SCHEDULER_INSTANCE_CTRL_ACTOR_CLIENT_H
#define LOCAL_SCHEDULER_INSTANCE_CTRL_ACTOR_CLIENT_H

#include <actor/actor.hpp>
#include <async/future.hpp>

#include "common/resource_view/resource_type.h"
#include "common/schedule_decision/schedule_queue_actor.h"
#include "common/schedule_decision/scheduler.h"
#include "common/state_machine/instance_state_machine.h"
#include "common/types/instance_state.h"
#include "common/utils/actor_driver.h"
#include "function_agent_manager/function_agent_mgr.h"
#include "function_proxy/common/posix_client/control_plane_client/control_interface_client_manager_proxy.h"
#include "idle/idle_mgr.h"
#include "instance_ctrl_actor.h"
#include "local_scheduler/subscription_manager/subscription_mgr.h"

namespace functionsystem::local_scheduler {

class LocalSchedSrv;
class SnapCtrl;
class TraefikRegistry;
class InstanceCtrl : public ActorDriver {
public:
    explicit InstanceCtrl(const std::shared_ptr<InstanceCtrlActor> &instanceCtrlActor);

    ~InstanceCtrl() override;

    static std::unique_ptr<InstanceCtrl> Create(const std::string &nodeID, const InstanceCtrlConfig &config);
    void Start(const std::shared_ptr<FunctionAgentMgr> &functionAgentMgr,
               const std::shared_ptr<ResourceViewMgr> &resourceViewMgr,
               const std::shared_ptr<function_proxy::ControlPlaneObserver> &observer,
               const std::string &aggregatedStrategy = "no_aggregate", uint16_t maxPriority = 0);

    void Stop() override;

    void Await() override;

    /**
     * wrap Async call of Schedule interface
     * request from other actor or grpc stream receiver
     * @param scheduleReq
     * @return
     */
    virtual litebus::Future<messages::ScheduleResponse> Schedule(
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise);

    /**
     * wrap Async call of Kill interface
     * request from other actor or grpc stream receiver
     * @param killReq: kill instance request from other actor or grpc stream
     * @return kill instance response
     */
    virtual litebus::Future<KillResponse> Kill(const std::string &srcInstanceID,
                                               const std::shared_ptr<KillRequest> &killReq);

    virtual litebus::Future<ExitResponse> Exit(const std::string &srcInstanceID,
                                               const std::shared_ptr<ExitRequest> &exitReq);

    /**
     * wrap Async call of UpdateInstanceStatus interface
     * request from other actor
     * @param info: instance status info need to update
     * @return update result
     */
    virtual litebus::Future<Status> UpdateInstanceStatus(const std::shared_ptr<InstanceExitStatus> &info);

    /**
     * wrap Async call of Sync Instance interface
     * request from function agent manager
     * @param view: instance info of function agent when register
     * @return Consistency check result
     */
    virtual litebus::Future<Status> SyncInstances(const std::shared_ptr<resource_view::ResourceUnit> &view);

    /**
     * Kill instance whose agent is not managed by proxy currently, or agent is failed
     *
     * @param agentMap all agent managed by proxy
     * @return agent sync result
     */
    virtual litebus::Future<Status> SyncAgent(
        const std::unordered_map<std::string, messages::FuncAgentRegisInfo> &agentMap);

    virtual litebus::Future<CallResultAck> CallResult(
        const std::string &from, const std::shared_ptr<functionsystem::CallResult> &callResult) const;

    virtual litebus::Future<litebus::Option<FunctionMeta>> GetFuncMeta(const std::string &funcKey)
    {
        return litebus::Async(aid_, &InstanceCtrlActor::GetFuncMeta, funcKey);
    }

    virtual void TriggerToWarmUpFunction(const std::string &agentID);

    litebus::Future<Status> UpdateInstanceStatusPromise(const std::string &instanceID, const std::string &errMsg) const;

    virtual void SetAbnormal();

    virtual void PutFailedInstanceStatusByAgentId(const std::string &funcAgentID);

    virtual litebus::Future<bool> IsSystemTenant(const std::string &tenantID);

    void BindScheduler(const std::shared_ptr<schedule_decision::Scheduler> &scheduler) const;
    inline std::shared_ptr<schedule_decision::Scheduler> GetScheduler()
    {
        return scheduler_;
    }

    void BindFunctionAgentMgr(const std::shared_ptr<FunctionAgentMgr> &functionAgentMgr) const;

    void BindObserver(const std::shared_ptr<function_proxy::ControlPlaneObserver> &observer) const;

    void BindResourceView(const std::shared_ptr<resource_view::ResourceViewMgr> &resourceViewMgr) const
    {
        litebus::Async(aid_, &InstanceCtrlActor::BindResourceView, resourceViewMgr);
    }

    void BindControlInterfaceClientManager(const std::shared_ptr<ControlInterfaceClientManagerProxy> &mgr) const
    {
        litebus::Async(aid_, &InstanceCtrlActor::BindControlInterfaceClientManager, mgr);
    }

    void BindMetaStoreClient(const std::shared_ptr<MetaStoreClient> &metaStoreClient) const
    {
        litebus::Async(aid_, &InstanceCtrlActor::BindMetaStoreClient, metaStoreClient);
    }

    void BindLocalSchedSrv(const std::shared_ptr<local_scheduler::LocalSchedSrv> &localSchedSrv) const
    {
        litebus::Async(aid_, &InstanceCtrlActor::BindLocalSchedSrv, localSchedSrv);
    }

    void BindInternalIAM(const std::shared_ptr<function_proxy::InternalIAM> &internalIAM) const
    {
        litebus::Async(aid_, &InstanceCtrlActor::BindInternalIAM, internalIAM);
    }

    void BindDataObjClient(const std::shared_ptr<DataObjClient> &dataObjClient) const
    {
        litebus::Async(aid_, &InstanceCtrlActor::BindDataObjClient, dataObjClient);
    }

    void BindResourceGroupCtrl(const std::shared_ptr<ResourceGroupCtrl> &rGroupCtrl) const
    {
        litebus::Async(aid_, &InstanceCtrlActor::BindResourceGroupCtrl, rGroupCtrl);
    }

    void BindSubscriptionMgr(const std::shared_ptr<SubscriptionMgr> &subscriptionMgr) const
    {
        litebus::Async(aid_, &InstanceCtrlActor::BindSubscriptionMgr, subscriptionMgr);
    }

    void BindSnapCtrl(const std::shared_ptr<SnapCtrl> &snapCtrl)
    {
        litebus::Async(aid_, &InstanceCtrlActor::BindSnapCtrl, snapCtrl);
    }

    void OnHealthyStatus(const Status &status) const
    {
        ASSERT_IF_NULL(instanceCtrlActor_);
        litebus::Async(instanceCtrlActor_->GetAID(), &InstanceCtrlActor::OnHealthyStatus, status);
    }

    void SetEnablePrintResourceView(bool enablePrintResourceView)
    {
        enablePrintResourceView_ = enablePrintResourceView;
    }

    virtual litebus::AID GetActorAID()
    {
        return aid_;
    }

    virtual std::shared_ptr<IdleMgr> GetIdleMgr() const
    {
        return idleMgr_;
    }

    void BindIdleMgr(const std::shared_ptr<IdleMgr> &idleMgr)
    {
        idleMgr_ = idleMgr;
        instanceCtrlActor_->SetIdleMgr(idleMgr);
    }

    void SetTraefikRegistry(const std::shared_ptr<TraefikRegistry> &registry);

    virtual litebus::Future<KillResponse> KillInstancesOfJob(const std::shared_ptr<KillRequest> &killReq) const;

    void BindInstanceControlView(const std::shared_ptr<InstanceControlView> &view)
    {
        ASSERT_IF_NULL(instanceCtrlActor_);
        instanceCtrlActor_->BindInstanceControlView(view);
    }

    std::shared_ptr<InstanceControlView> GetInstanceControlView() const
    {
        ASSERT_IF_NULL(instanceCtrlActor_);
        return instanceCtrlActor_->GetInstanceControlView();
    }

    virtual litebus::Future<Status> RescheduleWithID(const std::string &instanceID);

    virtual litebus::Future<Status> RescheduleAfterJudgeRecoverable(const std::string &instanceID,
                                                                    const std::string &funcAgentID);

    virtual litebus::Future<Status> Reschedule(const Status &status,
                                               const std::shared_ptr<messages::ScheduleRequest> &request);

    virtual litebus::Future<Status> EvictInstanceOnAgent(const std::shared_ptr<messages::EvictAgentRequest> &req);

    virtual litebus::Future<Status> EvictInstances(const std::unordered_set<std::string> &instanceSet,
                                                   const std::shared_ptr<messages::EvictAgentRequest> &req,
                                                   bool isEvictForReuse);

    void NotifyDsHealthy(bool healthy) const;

    virtual litebus::Future<bool> IsInstanceRunning(const std::string &instanceID);

    void SetNodeLabelsToMetricsContext(const std::string &functionAgentID,
                                       std::map<std::string, resources::Value::Counter> nodeLabels);

    Status RegisterPolicy(std::shared_ptr<schedule_decision::Scheduler> scheduler);
    virtual litebus::Future<Status> ToScheduling(const std::shared_ptr<messages::ScheduleRequest> &req);
    virtual litebus::Future<Status> ToCreating(const std::shared_ptr<messages::ScheduleRequest> &req,
                                               const schedule_decision::ScheduleResult &result);
    virtual litebus::Future<Status> DeleteSchedulingInstance(const std::string &instanceID,
                                                             const std::string &requestID);
    virtual void RegisterReadyCallback(const std::string &instanceID,
                                       const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                       InstanceReadyCallBack callback);
    virtual void RegisterReadyCallResultCallback(const std::string &instanceID,
                                                 const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                                 InstanceReadyCallResultCallBack callback);
    virtual litebus::Future<messages::ScheduleResponse> ScheduleFrontendAndWaitReady(
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise,
        InstanceReadyCallResultCallBack callback);
    virtual void UnregisterFrontendReadyWait(const std::string &requestID, const std::string &reason);
    virtual litebus::Future<KillResponse> KillFrontend(const std::string &tenantID,
                                                       const std::shared_ptr<KillRequest> &killReq);
    virtual litebus::Future<FrontendKillCleanupSnapshot> ProbeFrontendKillCleanup(
        const std::string &requestID, const std::string &instanceID);
    virtual litebus::Future<Status> ForceDeleteInstance(const std::string &instanceID);

    virtual litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> CreateInstanceClient(
        const std::string &instanceID, const std::string &runtimeID, const std::string &address)
    {
        return litebus::Async(aid_, &InstanceCtrlActor::CreateInstanceClient, instanceID, runtimeID, address,
                              nullptr, false);
    }

    virtual void StartHeartbeat(const std::string &instanceID, uint32_t timeoutTimes,
                               const std::string &runtimeID, const StatusCode &prevStatus)
    {
        litebus::Async(aid_, &InstanceCtrlActor::StartHeartbeat, instanceID, timeoutTimes, runtimeID, prevStatus);
    }

    virtual litebus::Future<TransitionResult> TransInstanceState(
        const std::shared_ptr<InstanceStateMachine> machine, const TransContext &context)
    {
        return litebus::Async(aid_, &InstanceCtrlActor::TransInstanceState, machine, context);
    }

    virtual litebus::Future<messages::DeployInstanceResponse> DeploySnapStartInstance(
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
    {
        return litebus::Async(aid_, &InstanceCtrlActor::DeploySnapStartInstance, scheduleReq);
    }

    /**
     * D-6 F3: forward to the actor to remove the state machine left behind
     * by a failed snapstart(restore) deploy (phantom-restore fix).
     */
    virtual void CleanupFailedSnapStart(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                        const std::string &reason)
    {
        litebus::Async(aid_, &InstanceCtrlActor::CleanupFailedSnapStart, scheduleReq, reason);
    }

    virtual void RegisterClearGroupInstanceCallBack(ClearGroupInstanceCallBack callback);
    // only for test
    void SetMaxForwardKillRetryTimes(uint32_t times);

    // only for test
    void SetMaxForwardKillRetryCycleMs(uint32_t cycleMs);

    virtual litebus::Future<Status> AddTokenReference(const std::pair<std::string, std::string> &tokenReferPair);

    std::shared_ptr<schedule_decision::ScheduleQueueActor> CreateScheduler(
        const std::string &tag,
        const uint16_t &maxPriority,
        const std::string &aggregatedStrategy,
        const std::shared_ptr<resource_view::ResourceView> resourceView);

    virtual litebus::Future<Status> GracefulShutdown();

    // Forward TenantQuotaExceeded event to the actor
    void OnTenantQuotaExceeded(const std::string &msg)
    {
        litebus::Async(aid_, &InstanceCtrlActor::OnTenantQuotaExceeded, msg);
    }

    // Forwards subscription-related events (including subscribe/unsubscribe requests)
    virtual litebus::Future<KillResponse> ForwardSubscriptionEvent(const std::shared_ptr<KillContext> &ctx);

private:
    std::shared_ptr<InstanceCtrlActor> instanceCtrlActor_;
    litebus::AID aid_;
    // should be moved out instance ctrl in the future
    std::shared_ptr<schedule_decision::ScheduleQueueActor> primaryScheduleQueueActor_;
    std::shared_ptr<schedule_decision::ScheduleQueueActor> virtualScheduleQueueActor_;
    std::shared_ptr<schedule_decision::Scheduler> scheduler_;
    std::shared_ptr<IdleMgr> idleMgr_;
    inline static std::string nodeID_;
    bool enablePrintResourceView_ = false;
    inline static std::string schedulePlugins_;
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_INSTANCE_CTRL_ACTOR_CLIENT_H
