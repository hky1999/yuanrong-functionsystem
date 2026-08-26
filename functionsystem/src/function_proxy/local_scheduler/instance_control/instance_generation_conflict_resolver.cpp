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

#include "instance_generation_conflict_resolver.h"

#include "common/metadata/metadata.h"
#include "common/utils/struct_transfer.h"
#include "function_proxy/config/direct_routing_config.h"

namespace functionsystem::local_scheduler {

InstanceGenerationConflictResolver::InstanceGenerationConflictResolver(
    const InstanceInfo &contenderSnapshot, bool arbitrationEnabled)
    : contenderSnapshot_(contenderSnapshot),
      arbitrationEnabled_(arbitrationEnabled),
      cleanupState_(std::make_shared<CleanupState>())
{
}

bool InstanceGenerationConflictResolver::IsRouteConflictResolutionCandidate(
    const InstanceInfo &contenderSnapshot)
{
    // DR has a broader first-persistence policy and remains outside this
    // route-conflict contract. The remaining exclusions keep recovery,
    // group, snapshot, and app-driver lifecycles on their existing paths.
    return !function_proxy::DirectRoutingConfig::IsEnabled() && IsLowReliabilityInstance(contenderSnapshot) &&
           contenderSnapshot.instancestatus().code() == static_cast<int32_t>(InstanceState::CREATING) &&
           contenderSnapshot.version() == 0 && contenderSnapshot.groupid().empty() &&
           !IsAppDriver(contenderSnapshot.createoptions()) && !IsStaticFunctionInstance(contenderSnapshot) &&
           !contenderSnapshot.has_snapshotinfo() &&
           !contenderSnapshot.ischeckpointed() && !IsRuntimeRecoverEnable(contenderSnapshot);
}

const InstanceInfo &InstanceGenerationConflictResolver::Contender() const
{
    return contenderSnapshot_;
}

bool InstanceGenerationConflictResolver::IsArbitrationEnabled() const
{
    return arbitrationEnabled_;
}

bool InstanceGenerationConflictResolver::HasResolvedWinner() const
{
    return !resolvedWinner_.instanceid().empty();
}

const InstanceInfo &InstanceGenerationConflictResolver::ResolvedWinner() const
{
    return resolvedWinner_;
}

void InstanceGenerationConflictResolver::RecordResolvedWinner(const InstanceInfo &winnerInfo)
{
    // Winner selection is write-once. Cleanup may be retried after another
    // watcher update, but that must not silently switch the response to a
    // different generation.
    if (!HasResolvedWinner()) {
        resolvedWinner_.CopyFrom(winnerInfo);
    }
}

InstanceGenerationConflictResolver::CleanupStatePtr InstanceGenerationConflictResolver::GetCleanupState() const
{
    return cleanupState_;
}

bool InstanceGenerationConflictResolver::IsExactPersistenceFailure(const TransitionResult &result) const
{
    const auto &previousInfo = result.previousInfo;
    return arbitrationEnabled_ && result.preState.IsNone() && result.status.IsError() &&
           previousInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::CREATING) &&
           previousInfo.version() == 0 && previousInfo.instanceid() == contenderSnapshot_.instanceid() &&
           previousInfo.requestid() == contenderSnapshot_.requestid() &&
           previousInfo.runtimeid() == contenderSnapshot_.runtimeid() &&
           previousInfo.functionagentid() == contenderSnapshot_.functionagentid() &&
           previousInfo.functionproxyid() == contenderSnapshot_.functionproxyid() &&
           previousInfo.function() == contenderSnapshot_.function() &&
           previousInfo.tenantid() == contenderSnapshot_.tenantid();
}

bool InstanceGenerationConflictResolver::IsReusableWinner(
    const InstanceInfo &winnerInfo, const std::string &localProxyID) const
{
    return arbitrationEnabled_ &&
           winnerInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING) &&
           !winnerInfo.functionproxyid().empty() && winnerInfo.functionproxyid() != localProxyID &&
           winnerInfo.instanceid() == contenderSnapshot_.instanceid() &&
           winnerInfo.function() == contenderSnapshot_.function() &&
           winnerInfo.tenantid() == contenderSnapshot_.tenantid() &&
           winnerInfo.issystemfunc() == contenderSnapshot_.issystemfunc();
}

bool InstanceGenerationConflictResolver::MatchesContenderGenerationView(const InstanceInfo &currentInfo) const
{
    // The state machine owns a copy of the schedule request created before
    // deployment.  The live request receives runtimeID after DeployInstance,
    // so its captured contender can legitimately be more complete than the
    // state-machine view when the init CallResult first arrives.  A different
    // non-empty runtimeID is still a real generation change and must not be
    // treated as this contender.
    return currentInfo.instanceid() == contenderSnapshot_.instanceid() &&
           currentInfo.requestid() == contenderSnapshot_.requestid() &&
           currentInfo.functionproxyid() == contenderSnapshot_.functionproxyid() &&
           currentInfo.functionagentid() == contenderSnapshot_.functionagentid() &&
           currentInfo.function() == contenderSnapshot_.function() &&
           currentInfo.tenantid() == contenderSnapshot_.tenantid() &&
           (currentInfo.runtimeid().empty() || currentInfo.runtimeid() == contenderSnapshot_.runtimeid());
}

bool InstanceGenerationConflictResolver::IsSameGeneration(
    const InstanceInfo &expectedInfo, const InstanceInfo &currentInfo)
{
    return currentInfo.instanceid() == expectedInfo.instanceid() &&
           currentInfo.requestid() == expectedInfo.requestid() &&
           currentInfo.functionproxyid() == expectedInfo.functionproxyid() &&
           currentInfo.runtimeid() == expectedInfo.runtimeid() &&
           currentInfo.functionagentid() == expectedInfo.functionagentid();
}

}  // namespace functionsystem::local_scheduler
