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

#include "domain_scheduler_driver.h"

#include <nlohmann/json.hpp>

#include "common/constants/actor_name.h"
#include "common/logs/logging.h"
#include "common/resource_view/resource_poller.h"
#include "common/schedule_decision/schedule_recorder/schedule_recorder.h"
#include "common/schedule_plugin/common/constants.h"
#include "common/scheduler_framework/framework/framework_impl.h"
#include "domain_group_control/domain_group_ctrl.h"

namespace functionsystem::domain_scheduler {
const uint32_t DEFAULT_HEARTBEAT_TIMES = 12;
const uint32_t DEFAULT_HEARTBEAT_INTERVAL = 1000;
using namespace schedule_plugin;
std::unordered_map<std::string, std::unordered_set<std::string>> PLUGINS_MAP = {
    { "Default", { DEFAULT_PREFILTER_NAME, DEFAULT_FILTER_NAME, DEFAULT_SCORER_NAME } },
    { "Heterogeneous", { DEFAULT_HETEROGENEOUS_FILTER_NAME, DEFAULT_HETEROGENEOUS_SCORER_NAME } },
    { "ResourceSelector", { RESOURCE_SELECTOR_FILTER_NAME } },
    { "Disk", { DISK_FILTER_NAME, DISK_SCORER_NAME }},
    { "NUMA", { NUMA_AFFINITY_FILTER_NAME, NUMA_AFFINITY_SCORER_NAME }},
    { "UsageAware", { USAGE_AWARE_FILTER_NAME } },
};

std::unordered_map<std::string, std::unordered_set<std::string>> ROOT_STRICT_LABEL_PLUGINS_MAP = {
    { "Label", { STRICT_ROOT_LABEL_AFFINITY_FILTER_NAME, STRICT_LABEL_AFFINITY_SCORER_NAME } },
};

std::unordered_map<std::string, std::unordered_set<std::string>> ROOT_RELAXED_LABEL_PLUGINS_MAP = {
    { "Label", { RELAXED_ROOT_LABEL_AFFINITY_FILTER_NAME, RELAXED_LABEL_AFFINITY_SCORER_NAME } },
};

std::unordered_map<std::string, std::unordered_set<std::string>> NON_ROOT_RELAXED_LABEL_PLUGINS_MAP = {
    { "Label", { RELAXED_NON_ROOT_LABEL_AFFINITY_FILTER_NAME, RELAXED_LABEL_AFFINITY_SCORER_NAME } },
};

std::unordered_map<std::string, std::unordered_set<std::string>> NON_ROOT_STRICT_LABEL_PLUGINS_MAP = {
    { "Label", { STRICT_NON_ROOT_LABEL_AFFINITY_FILTER_NAME, STRICT_LABEL_AFFINITY_SCORER_NAME } },
};

DomainSchedulerDriver::DomainSchedulerDriver(const DomainSchedulerParam &param) : ModuleDriver(), param_(param)
{
}

std::shared_ptr<schedule_decision::ScheduleQueueActor> DomainSchedulerDriver::CreateScheduler(
    const std::string &tag,
    const std::shared_ptr<schedule_decision::ScheduleRecorder> &scheduleRecorder,
    const std::shared_ptr<resource_view::ResourceView> resourceView)
{
    auto scheduleQueueActor = std::make_shared<schedule_decision::ScheduleQueueActor>(param_.identity + tag);
    auto framework = std::make_shared<schedule_framework::FrameworkImpl>(param_.relaxed);
    auto policyType = schedule_decision::PriorityPolicyType::FIFO;
    schedule_decision::PreemptInstancesFunc preemptCallbackFunc;
    if (param_.maxPriority > 0 && param_.enablePreemption) {
        preemptCallbackFunc =
            [aid(underlayerMgrActor_->GetAID())](
                const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
            litebus::Async(aid, &UnderlayerSchedMgrActor::PreemptInstance, preemptResults);
            return Status::OK();
        };
    }
    if (param_.maxPriority > 0) {
        policyType = schedule_decision::PriorityPolicyType::FAIRNESS;
    }
    YRLOG_INFO("start scheduler actor, enablePreemption:{} policyType:{}",
               (param_.maxPriority > 0 && param_.enablePreemption), static_cast<int>(policyType));
    auto priorityScheduler =
        std::make_shared<schedule_decision::PriorityScheduler>(scheduleRecorder, param_.maxPriority,
                                                               policyType, param_.aggregatedStrategy);
    priorityScheduler->RegisterSchedulePerformer(resourceView, framework, preemptCallbackFunc);
    scheduleQueueActor->RegisterScheduler(priorityScheduler);
    scheduleQueueActor->RegisterResourceView(resourceView);
    litebus::Spawn(scheduleQueueActor);
    return scheduleQueueActor;
}

Status DomainSchedulerDriver::Start()
{
    if (alreadyStarted && domainSrvActor_ != nullptr) {
        YRLOG_INFO("already start domain scheduler, just to trigger register to global");
        (void)litebus::Async(domainSrvActor_->GetAID(), &DomainSchedSrvActor::RegisterToGlobal);
        return Status::OK();
    }
    YRLOG_INFO(
        "start domain scheduler, identity:{} isScheduleTolerateAbnormal:{} heartbeatTimeoutMs:{} "
        "pullResourceInterval:{} enableMetrics:{} enablePrintResourceView:{} maxPriority:{} aggregatedStrategy:{}",
        param_.identity, param_.isScheduleTolerateAbnormal, param_.heartbeatTimeoutMs, param_.pullResourceInterval,
        param_.enableMetrics, param_.enablePrintResourceView, param_.maxPriority, param_.aggregatedStrategy);
    auto pingTimeout = param_.heartbeatTimeoutMs / 2;
    domainSrvActor_ = std::make_shared<DomainSchedSrvActor>(param_.identity, param_.metaStoreClient, pingTimeout);
    domainSrvActor_->SetComponentName(param_.componentName);
    auto domainSrv = std::make_shared<DomainSchedSrv>(domainSrvActor_->GetAID());
    auto heartbeatInterval = param_.heartbeatTimeoutMs / DEFAULT_HEARTBEAT_TIMES;
    heartbeatInterval = heartbeatInterval == 0 ? DEFAULT_HEARTBEAT_INTERVAL : heartbeatInterval;
    underlayerMgrActor_ =
        std::make_shared<UnderlayerSchedMgrActor>(param_.identity, DEFAULT_HEARTBEAT_TIMES, heartbeatInterval);
    auto underlayerMgr = std::make_shared<UnderlayerSchedMgr>(underlayerMgrActor_->GetAID());
    auto scheduleRecorder = schedule_decision::ScheduleRecorder::CreateScheduleRecorder();
    instanceCtrlActor_ = std::make_shared<InstanceCtrlActor>(param_.identity, param_.isScheduleTolerateAbnormal);
    auto instanceCtrl = std::make_shared<InstanceCtrl>(instanceCtrlActor_->GetAID());

    resourceViewMgr_ = std::make_shared<resource_view::ResourceViewMgr>();
    resourceViewMgr_->Init(param_.identity);
    resource_view::ResourcePoller::SetInterval(param_.pullResourceInterval);
    resourceViewMgr_->TriggerTryPull();

    primaryScheduleQueueActor_ =
        CreateScheduler(PRIMARY_TAG, scheduleRecorder, resourceViewMgr_->GetInf(resource_view::ResourceType::PRIMARY));
    virtualScheduleQueueActor_ =
        CreateScheduler(VIRTUAL_TAG, scheduleRecorder, resourceViewMgr_->GetInf(resource_view::ResourceType::VIRTUAL));
    auto scheduler = std::make_shared<schedule_decision::Scheduler>(primaryScheduleQueueActor_->GetAID(),
                                                                    virtualScheduleQueueActor_->GetAID());

    domainSrvActor_->BindInstanceCtrl(instanceCtrl);
    domainSrvActor_->BindResourceView(resourceViewMgr_);
    domainSrvActor_->BindUnderlayerMgr(underlayerMgr);
    domainSrvActor_->BindScheduleRecorder(scheduleRecorder);

    underlayerMgrActor_->BindDomainService(domainSrv);
    underlayerMgrActor_->BindResourceView(resourceViewMgr_);
    underlayerMgrActor_->BindInstanceCtrl(instanceCtrl);

    instanceCtrlActor_->BindUnderlayerMgr(underlayerMgr);
    instanceCtrlActor_->BindResourceView(resourceViewMgr_);
    instanceCtrlActor_->BindScheduler(scheduler);
    instanceCtrlActor_->BindScheduleRecorder(scheduleRecorder);
    instanceCtrlActor_->SetEnableHorizontalScale(param_.enableHorizontalScale);

    domainGroupCtrlActor_ = std::make_shared<DomainGroupCtrlActor>(DOMAIN_GROUP_CTRL_ACTOR_NAME);
    domainGroupCtrlActor_->BindScheduler(scheduler);
    domainGroupCtrlActor_->BindUnderlayerMgr(underlayerMgr);
    domainGroupCtrlActor_->BindScheduleRecorder(scheduleRecorder);
    auto groupCtrl = std::make_shared<DomainGroupCtrl>(domainGroupCtrlActor_);
    domainSrvActor_->BindDomainGroupCtrl(groupCtrl);
    litebus::Spawn(domainGroupCtrlActor_);

    litebus::Spawn(instanceCtrlActor_);
    litebus::Spawn(underlayerMgrActor_);
    litebus::Spawn(domainSrvActor_);
    RegisterPolicy(scheduler);
    domainSrv->EnableMetrics(param_.enableMetrics);
    alreadyStarted = true;
    return Status::OK();
}

Status DomainSchedulerDriver::RegisterPolicy(std::shared_ptr<schedule_decision::Scheduler> scheduler)
{
    YRLOG_DEBUG("start to RegisterPolicy, plugins: {}", param_.schedulePlugins);
    nlohmann::json plugins;
    try {
        plugins = nlohmann::json::parse(param_.schedulePlugins);
    } catch (nlohmann::json::parse_error &e) {
        YRLOG_ERROR("failed to register policy, not a valid json");
        return Status(StatusCode::FAILED, "failed to register policy, not a valid json, reason: "
                                              + std::string(e.what()) + ", id: " + std::to_string(e.id));
    }

    if (!plugins.is_array()) {
        YRLOG_ERROR("failed to register policy, invalid format");
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
    auto labelPlugins = param_.relaxed > 0 ? ROOT_RELAXED_LABEL_PLUGINS_MAP : ROOT_STRICT_LABEL_PLUGINS_MAP;
    for (const auto &pluginName : plugins) {
        if (auto iter = PLUGINS_MAP.find(pluginName); iter != PLUGINS_MAP.end()) {
            for (const auto &plugin : iter->second) {
                registerFunc(plugin);
            }
            continue;
        }
        if (auto iter = labelPlugins.find(pluginName); iter != labelPlugins.end()) {
            for (const auto &plugin : iter->second) {
                registerFunc(plugin);
            }
            continue;
        }
        registerFunc(pluginName);
    }
    return Status::OK();
}

Status DomainSchedulerDriver::Stop()
{
    if (domainSrvActor_) {
        litebus::Terminate(domainSrvActor_->GetAID());
    }
    if (underlayerMgrActor_) {
        litebus::Terminate(underlayerMgrActor_->GetAID());
    }
    if (instanceCtrlActor_) {
        litebus::Terminate(instanceCtrlActor_->GetAID());
    }
    if (primaryScheduleQueueActor_) {
        litebus::Terminate(primaryScheduleQueueActor_->GetAID());
    }
    if (virtualScheduleQueueActor_) {
        litebus::Terminate(virtualScheduleQueueActor_->GetAID());
    }
    if (domainGroupCtrlActor_) {
        litebus::Terminate(domainGroupCtrlActor_->GetAID());
    }
    alreadyStarted = false;
    return Status::OK();
}

void DomainSchedulerDriver::Await()
{
    if (domainSrvActor_) {
        litebus::Await(domainSrvActor_->GetAID());
    }
    if (underlayerMgrActor_) {
        litebus::Await(underlayerMgrActor_->GetAID());
    }
    if (instanceCtrlActor_) {
        litebus::Await(instanceCtrlActor_->GetAID());
    }
    if (primaryScheduleQueueActor_) {
        litebus::Await(primaryScheduleQueueActor_->GetAID());
    }
    if (virtualScheduleQueueActor_) {
        litebus::Await(virtualScheduleQueueActor_->GetAID());
    }
    if (domainGroupCtrlActor_) {
        litebus::Await(domainGroupCtrlActor_->GetAID());
    }
}
}  // namespace functionsystem::domain_scheduler