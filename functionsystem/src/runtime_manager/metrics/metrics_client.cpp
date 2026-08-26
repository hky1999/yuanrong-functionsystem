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
#include <spdlog/fmt/ranges.h>
#include "metrics_client.h"

#include "async/async.hpp"
#include "common/logs/logging.h"
#include "common/resource_view/resource_type.h"

namespace functionsystem::runtime_manager {

MetricsClient::MetricsClient()
{
    const std::string name = "MetricsActor_" + litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    actor_ = std::make_shared<MetricsActor>(name);
    litebus::Spawn(actor_);
}

MetricsClient::~MetricsClient()
{
    litebus::Terminate(actor_->GetAID());
    litebus::Await(actor_->GetAID());
}

std::vector<int> MetricsClient::GetCardIDs()
{
    if (firstGetCardID_) {
        cardIDs_ = litebus::Async(actor_->GetAID(), &MetricsActor::GetCardIDs).Get();
        YRLOG_DEBUG("get cardIDs_ from MetricsActor: [{}]", fmt::join(cardIDs_, ", "));
        firstGetCardID_ = false;
    }
    return cardIDs_;
}

void MetricsClient::DeleteInstanceMetrics(const std::string &deployDir, const std::string &instanceID) const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::DeleteInstance, deployDir, instanceID);
}

void MetricsClient::UpdateInstanceMemoryUsage(const std::string &instanceID, double memoryMb) const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::UpdateInstanceMemoryUsage, instanceID, memoryMb);
}

void MetricsClient::CreateInstanceMetrics(const litebus::Future<::messages::StartInstanceResponse> &response,
                                          const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    auto instanceInfo = request->runtimeinstanceinfo();
    auto resources = instanceInfo.runtimeconfig().resources().resources();
    double cpuLimit = 0;
    double memLimit = 0;
    for (auto resource : resources) {
        if (resource.first == resource_view::CPU_RESOURCE_NAME) {
            cpuLimit = resource.second.mutable_scalar()->value();
            YRLOG_INFO("{}|Read cpu limit: {}", request->runtimeinstanceinfo().requestid(), std::to_string(cpuLimit));
        } else if (resource.first == resource_view::MEMORY_RESOURCE_NAME) {
            memLimit = resource.second.mutable_scalar()->value();
            YRLOG_INFO("{}|Read memory limit: {}", request->runtimeinstanceinfo().requestid(),
                       std::to_string(memLimit));
        }
    }
    instanceInfo.set_address(response.Get().startruntimeinstanceresponse().address());
    pid_t pid = static_cast<pid_t>(response.Get().startruntimeinstanceresponse().pid());
    YRLOG_INFO("{}|create instance metrics, pid: {}", request->runtimeinstanceinfo().requestid(), std::to_string(pid));
    (void)litebus::Async(actor_->GetAID(), &MetricsActor::AddInstance, instanceInfo, pid, cpuLimit, memLimit);
}

resources::ResourceUnit MetricsClient::GetResourceUnit() const
{
    return litebus::Async(actor_->GetAID(), &MetricsActor::GetResourceUnit).Get();
}

litebus::Future<Status> MetricsClient::InitializeSandboxRuntimeCapabilities(
    const std::set<std::string> &runtimes) const
{
    return litebus::Async(actor_->GetAID(), &MetricsActor::InitializeSandboxRuntimeCapabilities, runtimes);
}

void MetricsClient::StartUpdateResource() const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::StartUpdateMetrics);
}

void MetricsClient::BeginUpdateMetrics() const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::BeginUpdateMetrics);
}

void MetricsClient::StopUpdateResource() const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::StopUpdateMetrics);
}

void MetricsClient::SetConfig(const Flags &flag) const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::SetConfig, flag);
}

void MetricsClient::SetRuntimeMemoryExceedLimitCallback(
    const RuntimeMemoryExceedLimitCallbackFunc &runtimeMemoryExceedLimitCallback)
{
    litebus::Async(actor_->GetAID(), &MetricsActor::SetRuntimeMemoryExceedLimitCallback,
                   runtimeMemoryExceedLimitCallback);
}

void MetricsClient::UpdateAgentInfo(const litebus::AID &agent) const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::UpdateAgentInfo, agent);
}

void MetricsClient::UpdateRuntimeManagerInfo(const litebus::AID &agent) const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::UpdateRuntimeManagerInfo, agent);
}

void MetricsClient::StartDiskUsageMonitor() const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::StartDiskUsageMonitor);
}

void MetricsClient::StopDiskUsageMonitor() const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::StopDiskUsageMonitor);
}

void MetricsClient::StartRuntimeMemoryLimitMonitor() const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::StartRuntimeMemoryLimitMonitor);
}

void MetricsClient::StopRuntimeMemoryLimitMonitor() const
{
    litebus::Async(actor_->GetAID(), &MetricsActor::StopRuntimeMemoryLimitMonitor);
}
}  // namespace functionsystem::runtime_manager
