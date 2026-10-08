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

#ifndef FUNCTION_PROXY_COMMON_STATE_MACHINE_INSTANCE_CONTROL_VIEW_H
#define FUNCTION_PROXY_COMMON_STATE_MACHINE_INSTANCE_CONTROL_VIEW_H

#include <mutex>
#include <unordered_map>

#include "async/future.hpp"
#include "meta_store_client/meta_store_client.h"
#include "common/status/status.h"
#include "instance_listener.h"
#include "instance_state_machine.h"

namespace functionsystem {

struct GeneratedInstanceStates {
    std::string instanceID = "";
    InstanceState preState = InstanceState::NEW;
    bool isDuplicate = false;
};

class InstanceControlView : public InstanceListener {
public:
    explicit InstanceControlView(const std::string &nodeID, bool isMetaStoreEnable);
    ~InstanceControlView() override = default;

    virtual litebus::Future<GeneratedInstanceStates> NewInstance(
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq);
    virtual litebus::Future<Status> DelInstance(const std::string &instanceID);
    // Ownership-fenced delete: the machine is selected and its incarnation
    // identity PREFILTERED inside the view lock, then the expected identity
    // is carried into InstanceStateMachine::DelInstance, which re-verifies
    // it under the MACHINE lock — the exact point the metadata (info,
    // version, store keys) is read for the delete transaction. That closes
    // the view-check-to-metadata-read window: ObserverActor updates the
    // same machine object in place, so only the machine's own lock
    // serializes the two. A same-name replacement (old chain arriving
    // late) is refused with ERR_INSTANCE_BUSY and untouched. Empty
    // expected fields = unchecked legacy behavior (the two-arg overload).
    virtual litebus::Future<Status> DelInstance(const std::string &instanceID,
                                                const std::string &expectedRuntimeID,
                                                const std::string &expectedRequestID);
    // Lock-held identity snapshot for post-delete re-verification (#2).
    virtual std::pair<std::string, std::string> GetIncarnationSnapshot(const std::string &instanceID);
    void OnDelInstance(const std::string &instanceID, const std::string &requestID, bool needErase);

    void Update(const std::string &instanceID, const resources::InstanceInfo &instanceInfo,
                bool isForceUpdate) override;
    void Delete(const std::string &instanceID, int64_t modRevision = -1) override;

    virtual litebus::Future<Status> TryExitInstance(const std::string &instanceID, bool isSynchronized = false);
    void BindMetaStoreClient(const std::shared_ptr<MetaStoreClient> &client);

    virtual std::shared_ptr<InstanceStateMachine> GetInstance(const std::string &instanceID);
    bool SetOwner(const std::string &instanceID);
    bool ReleaseOwner(const std::string &instanceID);
    std::string TryGetInstanceIDByReq(const std::string &requestID);
    bool IsRescheduledRequest(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq);
    virtual litebus::Option<litebus::Future<messages::ScheduleResponse>> IsDuplicateRequest(
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise);
    virtual void InsertRequestFuture(
        const std::string &requestID, const litebus::Future<messages::ScheduleResponse> &future,
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise);
    virtual void DeleteRequestFuture(const std::string &requestID);

    virtual GeneratedInstanceStates TryGenerateNewInstance(
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq);

    litebus::Future<messages::ScheduleResponse> GetRequestFuture(const std::string &requestID);

    std::shared_ptr<InstanceStateMachine> NewStateMachine(const std::string &instanceID,
                                                          const resources::InstanceInfo &instanceInfo);
    void GenerateStateMachine(const std::string &instanceID, const resources::InstanceInfo &instanceInfo);

    void SetLocalAbnormal();

    virtual function_proxy::InstanceInfoMap GetInstancesWithStatus(const InstanceState &state);
    virtual std::unordered_map<std::string, std::shared_ptr<InstanceStateMachine>> GetInstances();

private:
    void UpdateInstanceContext(const std::string &instanceID, const std::shared_ptr<InstanceContext> &context);
    bool IsDuplicateScheduling(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq);

    std::string self_;
    std::mutex lock_;
    std::shared_ptr<MetaStoreClient> metaStoreClient_;
    std::unordered_map<std::string, std::shared_ptr<InstanceStateMachine>> machines_;
    std::unordered_map<std::string, std::string> requestInstances_;
    std::unordered_map<std::string, litebus::Future<messages::ScheduleResponse>> createRequestFuture_;
    std::unordered_map<std::string, std::shared_ptr<litebus::Promise<messages::ScheduleResponse>>>
        createRequestRuntimeFuture_;
    bool isLocalAbnormal_ = false;
    bool isMetaStoreEnable_ = false;
};

}  // namespace functionsystem

#endif  // FUNCTION_PROXY_COMMON_STATE_MACHINE_INSTANCE_CONTROL_VIEW_H
