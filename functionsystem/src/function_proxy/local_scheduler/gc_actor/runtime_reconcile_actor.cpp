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

#include "runtime_reconcile_actor.h"

#include <unordered_map>
#include <unordered_set>

#include "async/async.hpp"
#include "async/asyncafter.hpp"
#include "async/defer.hpp"
#include "common/logs/logging.h"
#include "common/types/instance_state.h"
#include "common/utils/struct_transfer.h"

namespace functionsystem::local_scheduler {

namespace {
using ReconcileRequestMap = std::unordered_map<std::string, std::shared_ptr<messages::ReconcileRuntimesRequest>>;

std::shared_ptr<messages::ReconcileRuntimesRequest> MakeReconcileRequest()
{
    auto request = std::make_shared<messages::ReconcileRuntimesRequest>();
    request->set_requestid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    return request;
}

void AddLocalInstanceRequest(ReconcileRequestMap &agentRequests,
                             const std::string &nodeID,
                             const std::string &instanceID,
                             const resources::InstanceInfo &info)
{
    const auto &agentID = info.functionagentid();
    if (agentID.empty() || info.runtimeid().empty()) {
        return;
    }
    if (info.functionproxyid() != nodeID) {
        // This reconciler runs on the local proxy node and must only compare
        // the local executor inventory against instances owned by this proxy.
        // functionAgentID is the target agent to send the request to; proxy
        // ownership is recorded in functionProxyID.
        YRLOG_DEBUG("RuntimeReconcileActor: skip non-local instance {}, proxy {}, local node {}",
                    instanceID, info.functionproxyid(), nodeID);
        return;
    }

    auto it = agentRequests.find(agentID);
    if (it == agentRequests.end()) {
        it = agentRequests.emplace(agentID, MakeReconcileRequest()).first;
    }

    auto *entry = it->second->add_entries();
    entry->set_runtimeid(info.runtimeid());
    entry->set_containerid(info.containerid());
    entry->set_instanceid(instanceID);
    const auto portMappings = info.extensions().find(PORT_FORWARD_KEY);
    if (portMappings != info.extensions().end()) {
        entry->set_portmappings(portMappings->second);
    }
}
}  // namespace

RuntimeReconcileActor::RuntimeReconcileActor(const std::string &name,
                                             const std::string &nodeID)
    : BasisActor(name),
      nodeID_(nodeID)
{
}

void RuntimeReconcileActor::Init()
{
    BasisActor::Init();
    YRLOG_INFO("RuntimeReconcileActor initialized on node: {} (co-process mode, periodic={}ms)",
               nodeID_, kPeriodicReconcileIntervalMs);
    SchedulePeriodicCycle();
}

void RuntimeReconcileActor::Finalize()
{
    YRLOG_INFO("RuntimeReconcileActor finalizing on node: {}", nodeID_);
    BasisActor::Finalize();
}

void RuntimeReconcileActor::SchedulePeriodicCycle()
{
    (void)litebus::AsyncAfter(kPeriodicReconcileIntervalMs, GetAID(),
                              &RuntimeReconcileActor::RunPeriodicCycle);
}

void RuntimeReconcileActor::RunPeriodicCycle()
{
    // No agents in firstPassAgents => periodic mode: reconcile every agent that
    // owns at least one instance locally; never mutate UnitStatus.
    RunReconcileCycle({});
    RetryPendingGhosts();
    SchedulePeriodicCycle();
}

void RuntimeReconcileActor::TriggerOnce(const std::string &funcAgentID)
{
    (void)litebus::Async(GetAID(), &RuntimeReconcileActor::OnTrigger, funcAgentID);
}

void RuntimeReconcileActor::OnTrigger(const std::string &funcAgentID)
{
    pendingAgents_.push_back(funcAgentID);
    std::vector<std::string> firstPass;
    firstPass.swap(pendingAgents_);
    RunReconcileCycle(firstPass);
}

void RuntimeReconcileActor::RunReconcileCycle(const std::vector<std::string> &firstPassAgents)
{
    if (instanceControlView_ == nullptr || functionAgentMgr_ == nullptr) {
        YRLOG_WARN("RuntimeReconcileActor: dependencies not bound, skipping cycle");
        return;
    }

    auto instances = instanceControlView_->GetInstances();

    // Group instances by funcAgentID, collecting {runtimeID, containerID} entries
    ReconcileRequestMap agentRequests;

    for (const auto &[instanceID, sm] : instances) {
        if (sm == nullptr) {
            continue;
        }
        AddLocalInstanceRequest(agentRequests, nodeID_, instanceID, sm->GetInstanceInfo());
    }

    std::unordered_set<std::string> firstPassSet(firstPassAgents.begin(), firstPassAgents.end());

    // Always include the local node in every periodic cycle, even when it has
    // no live instances in etcd.  Orphan containers survive instance deletion,
    // so without this the local SandboxdExecutor would never receive a reconcile
    // request and the orphan grace-period timer would never fire.
    if (agentRequests.find(nodeID_) == agentRequests.end()) {
        agentRequests.emplace(nodeID_, MakeReconcileRequest());
    }

    YRLOG_INFO("RuntimeReconcileActor: starting reconcile cycle, {} agents from instance view, {} first-pass agents",
               agentRequests.size(), firstPassSet.size());

    // First-pass agents with NO local instances: nothing to reconcile against, but
    // we still owe them a RECOVERING → NORMAL transition. The executor side may
    // still have stale containers (proxy lost view, e.g. restart with empty etcd
    // sync). Send an empty-entries reconcile request so the executor can detect
    // and clean orphans from its own DoList() perspective.
    for (const auto &agentID : firstPassAgents) {
        if (agentRequests.find(agentID) != agentRequests.end()) {
            continue;
        }
        agentRequests.emplace(agentID, MakeReconcileRequest());
    }

    for (const auto &[agentID, request] : agentRequests) {
        if (!inProgressAgents_.insert(agentID).second) {
            YRLOG_INFO("RuntimeReconcileActor: agent {} reconcile already in-flight, skip", agentID);
            continue;
        }
        bool isFirstPass = firstPassSet.count(agentID) > 0;
        ReconcileAgent(agentID, request, isFirstPass);
    }
}

void RuntimeReconcileActor::ReconcileAgent(
    const std::string &funcAgentID,
    const std::shared_ptr<messages::ReconcileRuntimesRequest> &request,
    bool isFirstPass)
{
    YRLOG_INFO("{}|RuntimeReconcileActor: reconciling agent {}, entries: {}, firstPass: {}",
               request->requestid(), funcAgentID, request->entries_size(), isFirstPass);

    functionAgentMgr_->ReconcileRuntimes(funcAgentID, request)
        .OnComplete([aid(GetAID()), funcAgentID, isFirstPass](
            const litebus::Future<messages::ReconcileRuntimesResponse> &future) {
            if (future.IsError()) {
                (void)litebus::Async(aid, &RuntimeReconcileActor::OnReconcileError,
                                     funcAgentID, isFirstPass,
                                     std::string("ReconcileRuntimes RPC failed"));
                return;
            }
            (void)litebus::Async(aid, &RuntimeReconcileActor::OnReconcileComplete,
                                 future.Get(), funcAgentID, isFirstPass);
        });
}

void RuntimeReconcileActor::OnReconcileError(const std::string &funcAgentID, bool isFirstPass,
                                             const std::string &reason)
{
    (void)inProgressAgents_.erase(funcAgentID);

    // Do NOT fail-open to NORMAL on transient errors. If the agent is genuinely
    // offline, the registration/health-check pipeline owns the state transition.
    // We will retry on the next periodic cycle.
    YRLOG_WARN("RuntimeReconcileActor: agent {} reconcile error ({}), firstPass={}, "
               "keeping UnitStatus unchanged; will retry next cycle",
               funcAgentID, reason, isFirstPass);
}

void RuntimeReconcileActor::OnReconcileComplete(const messages::ReconcileRuntimesResponse &resp,
                                                const std::string &funcAgentID,
                                                bool isFirstPass)
{
    (void)inProgressAgents_.erase(funcAgentID);

    if (resp.code() != 0) {
        // Executor-side hard error (e.g. containerd not connected after retries).
        // Treat same as RPC error: do not flip state, retry next cycle.
        YRLOG_WARN("{}|RuntimeReconcileActor: agent {} returned error code={}, msg={}, "
                   "keeping UnitStatus unchanged; will retry next cycle",
                   resp.requestid(), funcAgentID, resp.code(), resp.message());
        return;
    }

    YRLOG_INFO("{}|RuntimeReconcileActor: agent {} reconcile result: orphansCleaned={}, missingIDs={}, "
               "confirmed={}, firstPass={}",
               resp.requestid(), funcAgentID, resp.orphanscleaned(), resp.missingids_size(),
               resp.confirmedentries_size(), isFirstPass);

    // Handle ghost instances: containers expected by proxy but missing in executor
    auto instances = instanceControlView_->GetInstances();
    std::unordered_map<std::string, std::string> containerToInstance;
    for (const auto &[instanceID, sm] : instances) {
        if (sm == nullptr) {
            continue;
        }
        auto info = sm->GetInstanceInfo();
        if (info.functionagentid() == funcAgentID && !info.containerid().empty()) {
            containerToInstance[info.containerid()] = instanceID;
        }
    }

    for (const auto &missingContainerID : resp.missingids()) {
        auto it = containerToInstance.find(missingContainerID);
        if (it != containerToInstance.end()) {
            YRLOG_WARN("{}|RuntimeReconcileActor: ghost instance {} (container {}) on agent {} — "
                       "container missing from executor",
                       resp.requestid(), it->second, missingContainerID, funcAgentID);
            CleanGhostInstance(it->second);
        }
    }

    // Transition agent RECOVERING → NORMAL only on first pass after registration.
    // Periodic passes never flip the unit status; that is owned by registration and
    // health-check pipelines.
    if (isFirstPass && resourceView_) {
        YRLOG_INFO("{}|RuntimeReconcileActor: agent {} first-pass reconcile complete, "
                   "transitioning to NORMAL",
                   resp.requestid(), funcAgentID);
        resourceView_->UpdateUnitStatus(funcAgentID, resource_view::UnitStatus::NORMAL);
    }
}

void RuntimeReconcileActor::CleanGhostInstance(const std::string &instanceID)
{
    if (instanceCtrl_ == nullptr) {
        YRLOG_WARN("RuntimeReconcileActor: instanceCtrl not bound, cannot clean ghost instance {}", instanceID);
        (void)pendingGhostInstances_.insert(instanceID);
        return;
    }

    (void)pendingGhostInstances_.insert(instanceID);
    (void)instanceCtrl_->ForceDeleteInstance(instanceID)
        .Then(litebus::Defer(GetAID(), &RuntimeReconcileActor::OnForceDeleteComplete, instanceID,
                             std::placeholders::_1));
}

Status RuntimeReconcileActor::OnForceDeleteComplete(const std::string &instanceID, const Status &status)
{
    if (status.IsError()) {
        YRLOG_WARN("RuntimeReconcileActor: ForceDeleteInstance failed for ghost instance {}: {} "
                   "(will retry next cycle)",
                   instanceID, status.GetMessage());
        // Keep in pendingGhostInstances_ for retry.
        return status;
    }
    YRLOG_INFO("RuntimeReconcileActor: ghost instance {} cleaned successfully", instanceID);
    (void)pendingGhostInstances_.erase(instanceID);
    return status;
}

void RuntimeReconcileActor::RetryPendingGhosts()
{
    if (pendingGhostInstances_.empty() || instanceCtrl_ == nullptr) {
        return;
    }
    // Snapshot to avoid iterator invalidation across async callbacks.
    std::vector<std::string> snapshot(pendingGhostInstances_.begin(), pendingGhostInstances_.end());
    YRLOG_INFO("RuntimeReconcileActor: retrying {} pending ghost instances", snapshot.size());
    for (const auto &instanceID : snapshot) {
        (void)instanceCtrl_->ForceDeleteInstance(instanceID)
            .Then(litebus::Defer(GetAID(), &RuntimeReconcileActor::OnForceDeleteComplete, instanceID,
                                 std::placeholders::_1));
    }
}

}  // namespace functionsystem::local_scheduler
