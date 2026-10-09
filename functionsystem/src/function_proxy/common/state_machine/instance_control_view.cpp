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

#include "instance_control_view.h"

#include "async/try.hpp"
#include "common/logs/logging.h"
#include "common/utils/struct_transfer.h"

namespace functionsystem {

InstanceControlView::InstanceControlView(const std::string &nodeID, bool isMetaStoreEnable)
    : self_(nodeID), isMetaStoreEnable_(isMetaStoreEnable)
{
}

void InstanceControlView::UpdateInstanceContext(const std::string &instanceID,
                                                const std::shared_ptr<InstanceContext> &newContext)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (machines_.find(instanceID) == machines_.end()) {
        auto stateMachine = std::make_unique<InstanceStateMachine>(self_, newContext, isMetaStoreEnable_);
        stateMachine->BindMetaStoreClient(metaStoreClient_);
        machines_[instanceID] = std::move(stateMachine);
        requestInstances_[newContext->GetRequestID()] = instanceID;
    } else {
        machines_[instanceID]->UpdateInstanceContext(newContext);
    }
    if (isLocalAbnormal_) {
        machines_[instanceID]->SetLocalAbnormal();
    }
}

void InstanceControlView::Update(const std::string &instanceID, const resources::InstanceInfo &instanceInfo,
                                 bool isForceUpdate)
{
    auto newOwner = instanceInfo.functionproxyid();
    std::lock_guard<std::mutex> guard(lock_);
    // update instance mod revision, when fast publish event send to other nodes, it can be filtered by mod revision
    if (auto modRevision = GetModRevisionFromInstanceInfo(instanceInfo);
        modRevision > 0 && machines_.find(instanceID) != machines_.end()) {
        const auto currentRevision = machines_.at(instanceID)->GetModRevision();
        if (modRevision < currentRevision || (modRevision == currentRevision && !isForceUpdate)) {
            YRLOG_WARN("receive old instance update events, ignore, current({}) vs received({})",
                       currentRevision, modRevision);
            return;
        }
        if (modRevision > currentRevision) {
            machines_.at(instanceID)->SetModRevision(modRevision);
        }
    }
    if (!isForceUpdate && newOwner == self_ && !IsDriver(instanceInfo)) {
        YRLOG_WARN("{} instance is owned by self({}), ignore it", instanceID, newOwner);
        return;
    }
    auto state = instanceInfo.instancestatus().code();
    if (machines_.find(instanceID) != machines_.end()) {
        auto currentOwner = machines_.at(instanceID)->GetOwner();
        // Events from current node are not concerned
        if (!isForceUpdate && newOwner == self_) {
            return;
        }
        // The owner is current node does not care about etcd events.
        // ===Special case===: When instance is in SUSPEND state and receives update event
        // where the owner in instanceInfo is not the current node (newOwner != self_),
        // this indicates ownership has been transferred, thus instanceInfo needs to
        // be updated to reflect the new owner.
        if (!isForceUpdate && currentOwner == self_
                && machines_.at(instanceID)->GetInstanceState() != InstanceState::SUSPEND
                && state != static_cast<int32_t>(InstanceState::PAUSED)) {
            return;
        }
        if (newOwner != currentOwner) {
            YRLOG_DEBUG("change instance({}) state machine's owner to {} from {}.", instanceID, newOwner, currentOwner);
        }
        machines_.at(instanceID)->UpdateInstanceInfo(instanceInfo);
        // 实例休眠状态下，使所有节点的version一致，避免休眠实例唤醒到其他节点后，因version=0无法更新实例状态
        if (currentOwner != self_ && state != static_cast<int32_t>(InstanceState::SUSPEND)) {
            machines_.at(instanceID)->SetVersion(0);
        }
        // Rescheduling can be triggered in the following states:
        if (state == static_cast<int32_t>(InstanceState::SCHEDULE_FAILED) ||
            state == static_cast<int32_t>(InstanceState::FAILED)) {
            if (createRequestFuture_.find(instanceInfo.requestid()) != createRequestFuture_.end()) {
                (void)createRequestFuture_.erase(instanceInfo.requestid());
            }
        }
    } else {
        YRLOG_DEBUG("create instance({}) state machine. owner:{}, state:{}", instanceID, newOwner, state);
        machines_[instanceID] = NewStateMachine(instanceID, instanceInfo);
        requestInstances_[instanceInfo.requestid()] = instanceID;
    }
}

std::shared_ptr<InstanceStateMachine> InstanceControlView::NewStateMachine(const std::string &instanceID,
                                                                           const resources::InstanceInfo &instanceInfo)
{
    auto request = std::make_shared<messages::ScheduleRequest>();
    request->set_requestid(instanceInfo.requestid());
    request->set_traceid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    request->mutable_instance()->CopyFrom(instanceInfo);
    auto context = std::make_shared<InstanceContext>(request);
    auto stateMachine = std::make_unique<InstanceStateMachine>(self_, std::move(context), isMetaStoreEnable_);
    stateMachine->BindMetaStoreClient(metaStoreClient_);
    stateMachine->SetVersion(instanceInfo.version());
    stateMachine->SetUpdateByRouteInfo();  // set update flag for machine created by routeInfo
    return stateMachine;
}

void InstanceControlView::GenerateStateMachine(const std::string &instanceID, const InstanceInfo &instanceInfo)
{
    std::lock_guard<std::mutex> guard(lock_);
    machines_[instanceID] = NewStateMachine(instanceID, instanceInfo);
    requestInstances_[instanceInfo.requestid()] = instanceID;
}

bool InstanceControlView::SetOwner(const std::string &instanceID)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (machines_.find(instanceID) == machines_.end()) {
        YRLOG_WARN("could not get instance({}) context, unable to update owner", instanceID);
        return false;
    }
    machines_[instanceID]->UpdateOwner(self_);
    return true;
}

bool InstanceControlView::ReleaseOwner(const std::string &instanceID)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (machines_.find(instanceID) == machines_.end()) {
        YRLOG_WARN("could not get instance({}) context, unable to release owner", instanceID);
        return false;
    }
    machines_[instanceID]->ReleaseOwner();
    return true;
}

void InstanceControlView::Delete(const std::string &instanceID, int64_t modRevision)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (machines_.find(instanceID) != machines_.end()) {
        auto requestID = machines_[instanceID]->GetRequestID();
        auto machine = machines_[instanceID];

        if (machines_[instanceID]->GetModRevision() != 0 && modRevision != -1
            && modRevision < machines_[instanceID]->GetModRevision()) {
            YRLOG_WARN("try delete new instance({}), revision({}), with old revision({}), ignore", instanceID,
                       machines_[instanceID]->GetModRevision(), modRevision);
            return;
        }

        machine->ExecuteStateChangeCallback(machine->GetRequestID(), InstanceState::EXITED);
        // only owner would try to exit instance
        if (machine->GetOwner() == self_) {
            machine->ExitInstance();
        }
        (void)machines_.erase(instanceID);
        (void)requestInstances_.erase(requestID);
        (void)createRequestFuture_.erase(requestID);
    }
}

litebus::Future<Status> InstanceControlView::TryExitInstance(const std::string &instanceID, bool isSynchronized)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (machines_.find(instanceID) == machines_.end()) {
        YRLOG_ERROR("failed to try exit instance({})", instanceID);
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND, "failed to find instance");
    }
    auto promise = std::make_shared<litebus::Promise<Status>>();
    auto context = std::make_shared<KillContext>();
    context->instanceContext = machines_[instanceID]->GetInstanceContextCopy();
    (void)machines_[instanceID]->TryExitInstance(promise, context, isSynchronized)
        .Then([machine(machines_[instanceID])](litebus::Future<Status> statusFuture) {
            if (statusFuture.IsOK()) {
                machine->ExecuteStateChangeCallback(machine->GetRequestID(), InstanceState::EXITING);
            }
            return statusFuture;
        });
    return promise->GetFuture();
}

void InstanceControlView::BindMetaStoreClient(const std::shared_ptr<MetaStoreClient> &client)
{
    metaStoreClient_ = client;
}

litebus::Future<GeneratedInstanceStates> InstanceControlView::NewInstance(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    return TryGenerateNewInstance(scheduleReq);
}

bool InstanceControlView::IsDuplicateScheduling(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    auto instanceInfo = scheduleReq->mutable_instance();
    auto mayGeneratedInstanceID = TryGetInstanceIDByReq(scheduleReq->requestid());
    if (!mayGeneratedInstanceID.empty()) {
        instanceInfo->set_instanceid(mayGeneratedInstanceID);
        auto stateMachine = GetInstance(mayGeneratedInstanceID);
        if (stateMachine == nullptr || stateMachine->GetOwner() != self_) {
            return false;
        }
        auto preState = stateMachine->GetInstanceState();
        if (preState == InstanceState::FATAL || preState == InstanceState::RUNNING
            || preState == InstanceState::CREATING) {
            YRLOG_WARN("{}|{}|instance({}) duplicate SCHEDULING request", scheduleReq->traceid(),
                       scheduleReq->requestid(), scheduleReq->instance().instanceid());
            return true;
        }
    }
    return false;
}

GeneratedInstanceStates InstanceControlView::TryGenerateNewInstance(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    ASSERT_IF_NULL(scheduleReq);
    auto instanceInfo = scheduleReq->mutable_instance();
    if (instanceInfo->instancestatus().code() == static_cast<int32_t>(InstanceState::SCHEDULING)) {
        if (IsDuplicateScheduling(scheduleReq)) {
            return GeneratedInstanceStates{ instanceInfo->instanceid(), InstanceState::SCHEDULING, true };
        }
        YRLOG_INFO("{}|{}|instance({}) state is scheduling, change owner to {}", scheduleReq->traceid(),
                   scheduleReq->requestid(), scheduleReq->instance().instanceid(), self_);
        instanceInfo->set_functionproxyid(self_);
        auto context = std::make_shared<InstanceContext>(std::make_shared<messages::ScheduleRequest>(*scheduleReq));
        UpdateInstanceContext(instanceInfo->instanceid(), context);
        (void)SetOwner(instanceInfo->instanceid());
        return GeneratedInstanceStates{ instanceInfo->instanceid(), InstanceState::SCHEDULING, false };
    }
    if (instanceInfo->instancestatus().code() != static_cast<int32_t>(InstanceState::NEW)) {
        YRLOG_ERROR("{}|{}|failed to add new instance({}), state {} which is not NEW", scheduleReq->traceid(),
                    scheduleReq->requestid(), scheduleReq->instance().instanceid(),
                    instanceInfo->instancestatus().code());
        return GeneratedInstanceStates{ "" };
    }
    auto mayGeneratedInstanceID = TryGetInstanceIDByReq(scheduleReq->requestid());
    if (!mayGeneratedInstanceID.empty()) {
        YRLOG_INFO("{}|{}|use the exist instance id({}).", scheduleReq->traceid(), scheduleReq->requestid(),
                   mayGeneratedInstanceID);
        instanceInfo->set_instanceid(mayGeneratedInstanceID);
        auto stateMachine = GetInstance(mayGeneratedInstanceID);
        ASSERT_IF_NULL(stateMachine);
        auto owner = stateMachine->GetOwner();
        instanceInfo->set_functionproxyid(owner);
        return GeneratedInstanceStates{ instanceInfo->instanceid(), stateMachine->GetInstanceState(), true };
    }
    auto newInstanceID = instanceInfo->instanceid().empty() ? litebus::uuid_generator::UUID::GetRandomUUID().ToString()
                                                            : instanceInfo->instanceid();
    YRLOG_INFO("{}|{}|generate a new instance id({}).", scheduleReq->traceid(), scheduleReq->requestid(),
               newInstanceID);
    instanceInfo->set_instanceid(newInstanceID);
    instanceInfo->set_functionproxyid(self_);
    auto instanceContext = std::make_shared<InstanceContext>(std::make_shared<messages::ScheduleRequest>(*scheduleReq));
    UpdateInstanceContext(instanceInfo->instanceid(), instanceContext);
    (void)SetOwner(instanceInfo->instanceid());
    return GeneratedInstanceStates{ instanceInfo->instanceid(), InstanceState::NEW, false };
}

litebus::Future<Status> InstanceControlView::DelInstance(const std::string &instanceID)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (machines_.find(instanceID) == machines_.end()) {
        YRLOG_WARN("instance control view failed to find instance({})", instanceID);
        return Status::OK();
    }
    auto machine = machines_[instanceID];
    return machine->DelInstance(instanceID).Then([machine, instanceID](const Status &status) -> Status {
        if (status.IsOk()) {
            machine->PublishDeleteToLocalObserver(instanceID);
        }
        return status;
    });
}

litebus::Future<Status> InstanceControlView::DelInstance(const std::string &instanceID,
    const std::string &expectedRuntimeID, const std::string &expectedRequestID)
{
    if (expectedRuntimeID.empty() && expectedRequestID.empty()) {
        return DelInstance(instanceID);  // legacy unchecked path
    }
    std::shared_ptr<InstanceStateMachine> machine;
    {
        // Machine selection + PREFILTER under the view lock: this rejects a
        // replacement that is already visible NOW, with view-level context
        // in the log. It is NOT the authoritative fence — the window between
        // releasing this lock and the machine reading its own metadata is
        // closed by InstanceStateMachine::DelInstance's identity check under
        // the MACHINE lock (UpdateInstanceInfo updates the same object in
        // place, so only the machine's own lock serializes them).
        std::lock_guard<std::mutex> guard(lock_);
        auto it = machines_.find(instanceID);
        if (it == machines_.end()) {
            YRLOG_WARN("instance control view failed to find instance({})", instanceID);
            return litebus::Future<Status>(Status::OK());
        }
        machine = it->second;
        const auto &current = machine->GetInstanceInfo();
        if (!expectedRuntimeID.empty() && current.runtimeid() != expectedRuntimeID) {
            YRLOG_WARN("ownership-fenced delete refused: instance({}) now runtime({}), "
                       "retiring chain runtime({})", instanceID, current.runtimeid(), expectedRuntimeID);
            return litebus::Future<Status>(Status(StatusCode::ERR_INSTANCE_BUSY,
                "control view bound to another incarnation"));
        }
        // STRICT request check: when the retiring chain carries a request
        // (it always does), an UNKNOWN (empty) current request refuses —
        // unknown is not consent.
        if (!expectedRequestID.empty() && machine->GetRequestID() != expectedRequestID) {
            YRLOG_WARN("ownership-fenced delete refused: instance({}) request({}), "
                       "retiring chain request({})", instanceID, machine->GetRequestID(),
                       expectedRequestID);
            return litebus::Future<Status>(Status(StatusCode::ERR_INSTANCE_BUSY,
                "control view request generation mismatch or unknown"));
        }
    }
    // The expected identity travels to the point that actually reads the
    // metadata and builds the version-fenced delete transaction: the
    // machine re-verifies it under its own lock_.
    return machine->DelInstance(instanceID, expectedRuntimeID, expectedRequestID)
        .Then([machine, instanceID](const Status &status) -> Status {
            if (status.IsOk()) {
                machine->PublishDeleteToLocalObserver(instanceID);
            }
            return status;
        });
}

Status InstanceControlView::SaveStateGuarded(const std::string &instanceID,
                                             const std::string &expectedRuntimeID,
                                             const std::string &expectedRequestID,
                                             const std::function<Status()> &op)
{
    // VIEW lock serializes against map swaps/inserts (Update / Generate-
    // StateMachine / OnDelInstance); the machine's OWN lock — taken inside
    // ExecuteStateOpUnderLock and held across the re-check and the
    // synchronous op — serializes against the DIRECT in-place update paths
    // that never take the view lock (InstanceCtrlActor::UpdateInstanceInfo,
    // transition callbacks, recovery handling). Both locks, one order
    // (view → machine, the same order Update itself uses).
    std::lock_guard<std::mutex> guard(lock_);
    auto it = machines_.find(instanceID);
    if (it == machines_.end()) {
        YRLOG_WARN("state save refused for instance({}): no machine (unknown)", instanceID);
        return Status(StatusCode::ERR_INSTANCE_BUSY,
            "state save refused: stream runtime superseded or unknown");
    }
    return it->second->ExecuteStateOpUnderLock(expectedRuntimeID, expectedRequestID, op);
}

Status InstanceControlView::DeleteStateGuarded(const std::string &instanceID,
                                               const std::string &expectedRuntimeID,
                                               const std::string &expectedRequestID,
                                               const std::function<Status()> &op)
{
    std::lock_guard<std::mutex> guard(lock_);
    auto it = machines_.find(instanceID);
    if (it == machines_.end()) {
        // Retire completion: no machine — serialized against same-name
        // inserts by the view lock.
        return op();
    }
    return it->second->ExecuteStateOpUnderLock(expectedRuntimeID, expectedRequestID, op);
}

std::pair<std::string, std::string> InstanceControlView::GetIncarnationSnapshot(
    const std::string &instanceID)
{
    std::lock_guard<std::mutex> guard(lock_);
    auto it = machines_.find(instanceID);
    if (it == machines_.end()) {
        return {"", ""};
    }
    // ONE locked complete copy for both fields (root-verified:
    // InstanceContext::GetRequestID() == instanceInfo.requestid()) — no
    // torn identity reads.
    const auto info = it->second->GetInstanceInfo();
    return {info.runtimeid(), info.requestid()};
}

InstanceControlView::InstanceIdentitySnapshot InstanceControlView::GetInstanceIdentitySnapshot(
    const std::string &instanceID)
{
    std::lock_guard<std::mutex> guard(lock_);
    InstanceIdentitySnapshot out;
    auto it = machines_.find(instanceID);
    if (it == machines_.end()) {
        return out;  // truly absent
    }
    out.exists = true;
    // ONE locked complete copy (the machine's own recursive lock) — both
    // identity fields from the same observation, never two torn reads.
    const auto info = it->second->GetInstanceInfo();
    out.runtimeID = info.runtimeid();
    out.requestID = info.requestid();
    return out;
}

void InstanceControlView::OnDelInstance(const std::string &instanceID, const std::string &requestID, bool needErase)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (machines_.find(instanceID) == machines_.end()) {
        YRLOG_WARN("instance control view failed to find instance({})", instanceID);
        return;
    }

    auto currentRequestID = machines_[instanceID]->GetRequestID();
    if (requestID != currentRequestID) {
        YRLOG_WARN("receive old instance event old({}) current({}), failed to delete instance({}), ", requestID,
                   currentRequestID, instanceID);
        return;
    }

    (void)requestInstances_.erase(currentRequestID);
    (void)createRequestFuture_.erase(currentRequestID);
    if (needErase) {
        YRLOG_INFO("erase instance({}) state Machine", instanceID);
        (void)machines_.erase(instanceID);
    }
}

std::shared_ptr<InstanceStateMachine> InstanceControlView::GetInstance(const std::string &instanceID)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (machines_.find(instanceID) == machines_.end()) {
        return nullptr;
    }
    return machines_[instanceID];
}

std::string InstanceControlView::TryGetInstanceIDByReq(const std::string &requestID)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (requestInstances_.find(requestID) == requestInstances_.end()) {
        return "";
    }
    return requestInstances_[requestID];
}

bool InstanceControlView::IsRescheduledRequest(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    ASSERT_IF_NULL(scheduleReq);
    std::lock_guard<std::mutex> guard(lock_);
    auto &instanceID = scheduleReq->instance().instanceid();
    if (!instanceID.empty() && machines_.find(instanceID) != machines_.end()) {
        YRLOG_DEBUG("{}|{}|instanceID({}) is rescheduled request",
            scheduleReq->traceid(), scheduleReq->requestid(), instanceID);
        return true;
    }
    return false;
}

litebus::Option<litebus::Future<messages::ScheduleResponse>> InstanceControlView::IsDuplicateRequest(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise)
{
    ASSERT_IF_NULL(scheduleReq);
    std::lock_guard<std::mutex> guard(lock_);
    auto &instanceID = scheduleReq->instance().instanceid();
    if (!instanceID.empty() && machines_.find(instanceID) != machines_.end()) {
        if (scheduleReq->scheduleround() > machines_[instanceID]->GetScheduleRound()) {
            YRLOG_INFO("{}|{}|schedule request is rescheduled, don't check duplicate", scheduleReq->traceid(),
                       scheduleReq->requestid());
            return litebus::None();
        }
    }
    const auto &requestID = scheduleReq->requestid();
    if (createRequestFuture_.find(requestID) == createRequestFuture_.end()) {
        return litebus::None();
    }
    scheduleReq->mutable_instance()->set_instanceid(requestInstances_[requestID]);
    if (auto iter = createRequestRuntimeFuture_.find(requestID); iter != createRequestRuntimeFuture_.end()) {
        runtimePromise->Associate(iter->second->GetFuture());
    }
    return createRequestFuture_[requestID];
}

litebus::Future<messages::ScheduleResponse> InstanceControlView::GetRequestFuture(const std::string &requestID)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (auto iter = createRequestRuntimeFuture_.find(requestID); iter != createRequestRuntimeFuture_.end()) {
        return iter->second->GetFuture();
    }

    return litebus::Status(litebus::Status::KERROR);
}

void InstanceControlView::InsertRequestFuture(
    const std::string &requestID, const litebus::Future<messages::ScheduleResponse> &future,
    const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise)
{
    std::lock_guard<std::mutex> guard(lock_);
    createRequestFuture_[requestID] = future;
    createRequestRuntimeFuture_[requestID] = runtimePromise;
}

void InstanceControlView::DeleteRequestFuture(const std::string &requestID)
{
    std::lock_guard<std::mutex> guard(lock_);
    if (createRequestFuture_.find(requestID) != createRequestFuture_.end()) {
        (void)createRequestFuture_.erase(requestID);
    }

    if (createRequestRuntimeFuture_.find(requestID) != createRequestRuntimeFuture_.end()) {
        (void)createRequestRuntimeFuture_.erase(requestID);
    }
}
void InstanceControlView::SetLocalAbnormal()
{
    std::lock_guard<std::mutex> guard(lock_);
    isLocalAbnormal_ = true;
    for (auto iter : machines_) {
        iter.second->SetLocalAbnormal();
    }
}

function_proxy::InstanceInfoMap InstanceControlView::GetInstancesWithStatus(const InstanceState &state)
{
    std::lock_guard<std::mutex> guard(lock_);
    function_proxy::InstanceInfoMap m;
    for (auto it : machines_) {
        if (it.second->GetOwner() == self_ && it.second->GetInstanceState() == state) {
            m[it.first] = it.second->GetInstanceInfo();
        }
    }
    return m;
}

std::unordered_map<std::string, std::shared_ptr<InstanceStateMachine>> InstanceControlView::GetInstances()
{
    std::lock_guard<std::mutex> guard(lock_);
    return machines_;
}

}  // namespace functionsystem
