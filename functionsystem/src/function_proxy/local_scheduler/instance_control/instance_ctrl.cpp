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

#include "instance_ctrl.h"

#include <async/async.hpp>

#include "common/constants/actor_name.h"
#include "idle/idle_actor.h"
#include "local_scheduler/traefik_registry/traefik_registry.h"
#include "common/schedule_plugin/common/constants.h"
#include "common/scheduler_framework/framework/framework_impl.h"

namespace functionsystem::local_scheduler {
using namespace schedule_plugin;

static const std::string IDLE_ACTOR_NAME_POSTFIX = "-LocalSchedIdleActor";

std::unordered_map<std::string, std::unordered_set<std::string>> PLUGINS_MAP = {
    { "Default", { DEFAULT_PREFILTER_NAME, DEFAULT_FILTER_NAME, DEFAULT_SCORER_NAME } },
    { "Label", { STRICT_NON_ROOT_LABEL_AFFINITY_FILTER_NAME, STRICT_LABEL_AFFINITY_SCORER_NAME } },
    { "Heterogeneous", { DEFAULT_HETEROGENEOUS_FILTER_NAME, DEFAULT_HETEROGENEOUS_SCORER_NAME } },
    { "ResourceSelector", { RESOURCE_SELECTOR_FILTER_NAME } },
    { "Disk", { DISK_FILTER_NAME, DISK_SCORER_NAME }},
    { "NUMA", { NUMA_AFFINITY_FILTER_NAME, NUMA_AFFINITY_SCORER_NAME }},
    { "UsageAware", { USAGE_AWARE_FILTER_NAME } },
};

InstanceCtrl::InstanceCtrl(const std::shared_ptr<InstanceCtrlActor> &instanceCtrlActor)
    : ActorDriver(instanceCtrlActor), instanceCtrlActor_(instanceCtrlActor), aid_(instanceCtrlActor->GetAID())
{
}

InstanceCtrl::~InstanceCtrl()
{
    Stop();
    Await();
}

void InstanceCtrl::Stop()
{
    if (instanceCtrlActor_ != nullptr) {
        litebus::Terminate(instanceCtrlActor_->GetAID());
    }
    if (primaryScheduleQueueActor_ != nullptr) {
        litebus::Terminate(primaryScheduleQueueActor_->GetAID());
    }
    if (virtualScheduleQueueActor_ != nullptr) {
        litebus::Terminate(virtualScheduleQueueActor_->GetAID());
    }
    if (idleMgr_ != nullptr) {
        idleMgr_->Stop();
    }
}

void InstanceCtrl::Await()
{
    if (instanceCtrlActor_ != nullptr) {
        litebus::Await(instanceCtrlActor_->GetAID());
        instanceCtrlActor_ = nullptr;
    }
    if (primaryScheduleQueueActor_ != nullptr) {
        litebus::Await(primaryScheduleQueueActor_->GetAID());
        primaryScheduleQueueActor_ = nullptr;
    }
    if (virtualScheduleQueueActor_ != nullptr) {
        litebus::Await(virtualScheduleQueueActor_->GetAID());
        virtualScheduleQueueActor_ = nullptr;
    }
    if (idleMgr_ != nullptr) {
        idleMgr_->Await();
        idleMgr_ = nullptr;
    }
}

litebus::Future<CallResultAck> InstanceCtrl::CallResult(
    const std::string &from, const std::shared_ptr<functionsystem::CallResult> &callResult) const
{
    return litebus::Async(aid_, &InstanceCtrlActor::CallResult, from, callResult);
}
litebus::Future<Status> InstanceCtrl::UpdateInstanceStatusPromise(const std::string &instanceID,
                                                                  const std::string &errMsg) const
{
    return litebus::Async(aid_, &InstanceCtrlActor::UpdateInstanceStatusPromise, instanceID,
                          errMsg);
}

void InstanceCtrl::PutFailedInstanceStatusByAgentId(const std::string &funcAgentID)
{
    litebus::Async(aid_, &InstanceCtrlActor::PutFailedInstanceStatusByAgentId, funcAgentID);
}

void InstanceCtrl::SetTraefikRegistry(const std::shared_ptr<TraefikRegistry> &registry)
{
    if (instanceCtrlActor_ != nullptr) {
        instanceCtrlActor_->SetTraefikRegistry(registry);
    }
}

litebus::Future<bool> InstanceCtrl::IsSystemTenant(const std::string &tenantID)
{
    return litebus::Async(aid_, &InstanceCtrlActor::IsSystemTenant, tenantID);
}

void InstanceCtrl::BindScheduler(const std::shared_ptr<schedule_decision::Scheduler> &scheduler) const
{
    litebus::Async(aid_, &InstanceCtrlActor::BindScheduler, scheduler);
}

void InstanceCtrl::BindFunctionAgentMgr(const std::shared_ptr<FunctionAgentMgr> &functionAgentMgr) const
{
    litebus::Async(aid_, &InstanceCtrlActor::BindFunctionAgentMgr, functionAgentMgr);
}

std::unique_ptr<InstanceCtrl> InstanceCtrl::Create(const std::string &nodeID, const InstanceCtrlConfig &config)
{
    nodeID_ = nodeID;
    std::string aid = nodeID + LOCAL_SCHED_INSTANCE_CTRL_ACTOR_NAME_POSTFIX;
    schedulePlugins_ = config.schedulePlugins;
    auto actor = std::make_shared<InstanceCtrlActor>(aid, nodeID, config);
    actor->ClearRateLimiterRegularly();

    auto ctrl = std::make_unique<InstanceCtrl>(std::move(actor));

    std::string idleAID = nodeID + IDLE_ACTOR_NAME_POSTFIX;
    auto idleActor = std::make_shared<IdleActor>(
        idleAID, nodeID, ctrl->instanceCtrlActor_->GetInstanceControlView(),
        litebus::AID(aid));
    auto idleMgr = std::make_shared<IdleMgr>(std::move(idleActor));
    ctrl->BindIdleMgr(idleMgr);
    return ctrl;
}

std::shared_ptr<schedule_decision::ScheduleQueueActor> InstanceCtrl::CreateScheduler(
    const std::string &tag,
    const uint16_t &maxPriority,
    const std::string &aggregatedStrategy,
    const std::shared_ptr<resource_view::ResourceView> resourceView)
{
    auto scheduleQueueActor =
        std::make_shared<schedule_decision::ScheduleQueueActor>(instanceCtrlActor_->GetAID().Name() + "-" + tag);
    auto framework = std::make_shared<schedule_framework::FrameworkImpl>();
    auto priorityScheduler = std::make_shared<schedule_decision::PriorityScheduler>(nullptr, maxPriority,
        schedule_decision::PriorityPolicyType::FIFO, aggregatedStrategy);
    priorityScheduler->RegisterSchedulePerformer(resourceView, framework, nullptr,
                                                 schedule_decision::AllocateType::ALLOCATION);
    scheduleQueueActor->RegisterScheduler(priorityScheduler);
    scheduleQueueActor->RegisterResourceView(resourceView);
    scheduleQueueActor->SetAllocateType(schedule_decision::AllocateType::ALLOCATION);
    // need to spawn and keep lifetime
    litebus::Spawn(scheduleQueueActor, false);
    return scheduleQueueActor;
}

void InstanceCtrl::Start(const std::shared_ptr<FunctionAgentMgr> &functionAgentMgr,
                         const std::shared_ptr<ResourceViewMgr> &resourceViewMgr,
                         const std::shared_ptr<function_proxy::ControlPlaneObserver> &observer,
                         const std::string &aggregatedStrategy, uint16_t maxPriority)
{
    if (instanceCtrlActor_ == nullptr) {
        YRLOG_ERROR("failed to start instance ctrl because actor pointer is null");
        return;
    }
    InstanceStateMachine::BindControlPlaneObserver(observer);
    instanceCtrlActor_->BindResourceView(resourceViewMgr);
    instanceCtrlActor_->BindObserver(observer);
    if (idleMgr_ != nullptr) {
        idleMgr_->Spawn();
    }
    (void)litebus::Spawn(instanceCtrlActor_, false);

    primaryScheduleQueueActor_ =
        CreateScheduler(PRIMARY_TAG, maxPriority, aggregatedStrategy,
                        resourceViewMgr->GetInf(resource_view::ResourceType::PRIMARY));
    virtualScheduleQueueActor_ =
        CreateScheduler(VIRTUAL_TAG, maxPriority, aggregatedStrategy,
                        resourceViewMgr->GetInf(resource_view::ResourceType::VIRTUAL));
    scheduler_ = std::make_shared<schedule_decision::Scheduler>(primaryScheduleQueueActor_->GetAID(),
                                                                virtualScheduleQueueActor_->GetAID());
    (void)RegisterPolicy(scheduler_);
    BindScheduler(scheduler_);
    BindFunctionAgentMgr(functionAgentMgr);
}

Status InstanceCtrl::RegisterPolicy(std::shared_ptr<schedule_decision::Scheduler> scheduler)
{
    nlohmann::json plugins;
    try {
        plugins = nlohmann::json::parse(schedulePlugins_);
    } catch (nlohmann::json::parse_error &e) {
        return Status(StatusCode::FAILED, "failed to register policy in local, not a valid json, reason: " +
                                              std::string(e.what()) + ", id: " + std::to_string(e.id));
    }

    if (!plugins.is_array()) {
        YRLOG_ERROR("failed to register policy in local, invalid format");
        return Status(StatusCode::FAILED, "failed to register policy, invalid format");
    }
    auto registerFunc = [scheduler](const std::string &pluginName) {
        (void)scheduler->RegisterPolicy(pluginName).OnComplete([pluginName](const litebus::Future<Status> &status) {
            if (status.IsError() || !status.Get().IsOk()) {
                YRLOG_WARN("failed to register {} policy in local, error: {}", pluginName, status.Get().ToString());
            }
        });
        return;
    };
    for (const auto &pluginName : plugins) {
        auto iter = PLUGINS_MAP.find(pluginName);
        if (iter == PLUGINS_MAP.end()) {
            registerFunc(pluginName);
            continue;
        }
        for (const auto &plugin : iter->second) {
            registerFunc(plugin);
        }
    }
    return Status::OK();
}

litebus::Future<messages::ScheduleResponse> InstanceCtrl::Schedule(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise)
{
    return litebus::Async(aid_, &InstanceCtrlActor::Schedule, scheduleReq, runtimePromise);
}

litebus::Future<KillResponse> InstanceCtrl::Kill(const std::string &srcInstanceID,
                                                 const std::shared_ptr<KillRequest> &killReq)
{
    return litebus::Async(aid_, &InstanceCtrlActor::Kill, srcInstanceID, killReq, false);
}

litebus::Future<ExitResponse> InstanceCtrl::Exit(const std::string &srcInstanceID,
                                                 const std::shared_ptr<ExitRequest> &exitReq)
{
    return litebus::Async(aid_, &InstanceCtrlActor::HandleExit, srcInstanceID, exitReq);
}

litebus::Future<KillResponse> InstanceCtrl::KillInstancesOfJob(const std::shared_ptr<KillRequest> &killReq) const
{
    return litebus::Async(aid_, &InstanceCtrlActor::KillInstancesOfJob, killReq);
}

litebus::Future<Status> InstanceCtrl::SyncInstances(const std::shared_ptr<resource_view::ResourceUnit> &resourceUnit)
{
    return litebus::Async(aid_, &InstanceCtrlActor::SyncInstance, resourceUnit);
}

litebus::Future<Status> InstanceCtrl::SyncAgent(
    const std::unordered_map<std::string, messages::FuncAgentRegisInfo> &agentMap)
{
    return litebus::Async(aid_, &InstanceCtrlActor::SyncAgent, agentMap);
}

litebus::Future<Status> InstanceCtrl::UpdateInstanceStatus(const std::shared_ptr<InstanceExitStatus> &info)
{
    return litebus::Async(aid_, &InstanceCtrlActor::UpdateInstanceStatus, info);
}

litebus::Future<Status> InstanceCtrl::RescheduleWithID(const std::string &instanceID)
{
    return litebus::Async(aid_, &InstanceCtrlActor::RescheduleWithID, instanceID);
}

litebus::Future<Status> InstanceCtrl::Reschedule(const Status &status,
                                                 const std::shared_ptr<messages::ScheduleRequest> &request)
{
    return litebus::Async(aid_, &InstanceCtrlActor::Reschedule, status, request);
}

void InstanceCtrl::BindObserver(const std::shared_ptr<function_proxy::ControlPlaneObserver> &observer) const
{
    ASSERT_IF_NULL(observer);
    litebus::Async(aid_, &InstanceCtrlActor::BindObserver, observer);
    InstanceStateMachine::BindControlPlaneObserver(observer);
}

void InstanceCtrl::SetAbnormal()
{
    litebus::Async(aid_, &InstanceCtrlActor::SetAbnormal);
}

litebus::Future<Status> InstanceCtrl::RescheduleAfterJudgeRecoverable(const std::string &instanceID,
                                                                      const std::string &funcAgentID)
{
    return litebus::Async(aid_, &InstanceCtrlActor::RescheduleAfterJudgeRecoverable, instanceID,
                          funcAgentID);
}

void InstanceCtrl::NotifyDsHealthy(bool healthy) const
{
    return litebus::Async(aid_, &InstanceCtrlActor::NotifyDsHealthy, healthy);
}

litebus::Future<Status> InstanceCtrl::EvictInstanceOnAgent(const std::shared_ptr<messages::EvictAgentRequest> &req)
{
    return litebus::Async(aid_, &InstanceCtrlActor::EvictInstanceOnAgent, req);
}

litebus::Future<Status> InstanceCtrl::EvictInstances(const std::unordered_set<std::string> &instanceSet,
                                                     const std::shared_ptr<messages::EvictAgentRequest> &req,
                                                     bool isEvictForReuse)
{
    return litebus::Async(aid_, &InstanceCtrlActor::EvictInstances, instanceSet, req,
                          isEvictForReuse);
}

void InstanceCtrl::SetNodeLabelsToMetricsContext(const std::string &functionAgentID,
                                                 std::map<std::string, resources::Value::Counter> nodeLabels)
{
    return litebus::Async(aid_, &InstanceCtrlActor::SetNodeLabelsToMetricsContext,
                          functionAgentID, nodeLabels);
}

void InstanceCtrl::SetMaxForwardKillRetryTimes(uint32_t times)
{
    return litebus::Async(aid_, &InstanceCtrlActor::SetMaxForwardKillRetryTimes, times);
}

void InstanceCtrl::SetMaxForwardKillRetryCycleMs(uint32_t cycleMs)
{
    return litebus::Async(aid_, &InstanceCtrlActor::SetMaxForwardKillRetryCycleMs, cycleMs);
}

litebus::Future<Status> InstanceCtrl::AddTokenReference(const std::pair<std::string, std::string> &tokenReferPair)
{
    return litebus::Async(aid_, &InstanceCtrlActor::AddTokenReference, tokenReferPair);
}

litebus::Future<Status> InstanceCtrl::ToScheduling(const std::shared_ptr<messages::ScheduleRequest> &req)
{
    return litebus::Async(aid_, &InstanceCtrlActor::ToScheduling, req);
}

litebus::Future<Status> InstanceCtrl::ToCreating(const std::shared_ptr<messages::ScheduleRequest> &req,
                                                 const schedule_decision::ScheduleResult &result)
{
    return litebus::Async(aid_, &InstanceCtrlActor::ToCreating, req, result);
}

litebus::Future<Status> InstanceCtrl::DeleteSchedulingInstance(const std::string &instanceID,
                                                               const std::string &requestID)
{
    return litebus::Async(aid_, &InstanceCtrlActor::DeleteSchedulingInstance, instanceID,
                          requestID);
}

void InstanceCtrl::RegisterReadyCallback(const std::string &instanceID,
                                         const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                         InstanceReadyCallBack callback)
{
    return litebus::Async(aid_, &InstanceCtrlActor::RegisterReadyCallback, instanceID,
                          scheduleReq, callback);
}

void InstanceCtrl::RegisterReadyCallResultCallback(const std::string &instanceID,
                                                   const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
                                                   InstanceReadyCallResultCallBack callback)
{
    return litebus::Async(aid_, &InstanceCtrlActor::RegisterReadyCallResultCallback, instanceID,
                          scheduleReq, callback);
}

litebus::Future<messages::ScheduleResponse> InstanceCtrl::ScheduleFrontendAndWaitReady(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise,
    InstanceReadyCallResultCallBack callback)
{
    return litebus::Async(aid_, &InstanceCtrlActor::ScheduleFrontendAndWaitReady, scheduleReq, runtimePromise,
                          std::move(callback));
}

void InstanceCtrl::UnregisterFrontendReadyWait(const std::string &requestID, const std::string &reason)
{
    (void)litebus::Async(aid_, &InstanceCtrlActor::UnregisterFrontendReadyWait, requestID, reason);
}

litebus::Future<KillResponse> InstanceCtrl::KillFrontend(const std::string &tenantID,
                                                         const std::shared_ptr<KillRequest> &killReq)
{
    return litebus::Async(aid_, &InstanceCtrlActor::KillFrontend, tenantID, killReq);
}

litebus::Future<FrontendKillCleanupSnapshot> InstanceCtrl::ProbeFrontendKillCleanup(
    const std::string &requestID, const std::string &instanceID)
{
    return litebus::Async(aid_, &InstanceCtrlActor::ProbeFrontendKillCleanup, requestID, instanceID);
}

litebus::Future<Status> InstanceCtrl::ForceDeleteInstance(const std::string &instanceID)
{
    return litebus::Async(aid_, &InstanceCtrlActor::ForceDeleteInstance, instanceID);
}

void InstanceCtrl::RegisterClearGroupInstanceCallBack(ClearGroupInstanceCallBack callback)
{
    return litebus::Async(aid_, &InstanceCtrlActor::RegisterClearGroupInstanceCallBack,
                          callback);
}
litebus::Future<Status> InstanceCtrl::GracefulShutdown()
{
    return litebus::Async(aid_, &InstanceCtrlActor::GracefulShutdown);
}

litebus::Future<KillResponse> InstanceCtrl::ForwardSubscriptionEvent(const std::shared_ptr<KillContext> &ctx)
{
    return litebus::Async(aid_, &InstanceCtrlActor::ForwardSubscriptionEvent, ctx);
}

litebus::Future<bool> InstanceCtrl::IsInstanceRunning(const std::string &instanceID)
{
    return litebus::Async(aid_, &InstanceCtrlActor::IsInstanceRunning, instanceID);
}

void InstanceCtrl::TriggerToWarmUpFunction(const std::string &agentID)
{
    return litebus::Async(aid_, &InstanceCtrlActor::TriggerToWarmUpFunction, agentID);
}
}  // namespace functionsystem::local_scheduler
