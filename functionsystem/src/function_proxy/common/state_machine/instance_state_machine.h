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

#ifndef FUNCTION_PROXY_COMMON_STATE_MACHINE_INSTANCE_STATE_MACHINE_H
#define FUNCTION_PROXY_COMMON_STATE_MACHINE_INSTANCE_STATE_MACHINE_H

#include <atomic>
#include <functional>

#include "async/option.hpp"
#include "common/constants/constants.h"
#include "common/meta_store_adapter/instance_operator.h"
#include "meta_store_client/meta_store_client.h"
#include "common/observer/control_plane_observer/control_plane_observer.h"
#include "common/proto/pb/message_pb.h"
#include "common/status/status.h"
#include "common/utils/struct_transfer.h"
#include "instance_context.h"

namespace functionsystem {

const int32_t INVALID_LAST_SAVE_FAILED_STATE = -1;

struct TransContext {
    InstanceState newState;
    int64_t version = 0;
    std::string msg = {};
    bool persistence = true;
    int32_t errCode = 0;
    int32_t exitCode = 0;
    int32_t type = static_cast<int32_t>(EXIT_TYPE::NONE_EXIT);
    // if not nullptr, need update stateMachine by scheduleReq
    std::shared_ptr<messages::ScheduleRequest> scheduleReq = nullptr;
};

struct TransitionResult {
    litebus::Option<InstanceState> preState;
    InstanceInfo savedInfo;
    InstanceInfo previousInfo;
    int64_t version = 0;
    Status status;
    int64_t currentModRevision = 0;
};

struct StateChangeCallback {
    std::unordered_set<InstanceState> statesConcerned;
    std::function<void(const resources::InstanceInfo)> callback;
};

using ExitHandler = std::function<litebus::Future<Status>(const resources::InstanceInfo &instanceInfo)>;
using ExitFailedHandler = std::function<void(const TransitionResult &result)>;

class InstanceStateMachine : public std::enable_shared_from_this<InstanceStateMachine> {
public:
    InstanceStateMachine(const std::string &nodeID, const std::shared_ptr<InstanceContext> &context,
                         bool isMetaStoreEnable);
    virtual ~InstanceStateMachine();
    virtual litebus::Future<Status> DelInstance(const std::string &instanceID);
    virtual litebus::Future<TransitionResult> TransitionTo(const TransContext &context);
    virtual litebus::Future<Status> ForceDelInstance();
    void BindMetaStoreClient(const std::shared_ptr<MetaStoreClient> &client);
    virtual void UpdateScheduleReq(const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
    {
        std::lock_guard<std::recursive_mutex> guard(lock_);
        instanceContext_->UpdateScheduleReq(scheduleReq);
    }
    static void BindControlPlaneObserver(const std::shared_ptr<function_proxy::ControlPlaneObserver> &observer)
    {
        controlPlaneObserver_ = observer;
    }

    static void UnBindControlPlaneObserver()
    {
        controlPlaneObserver_ = nullptr;
    }

    inline static std::shared_ptr<function_proxy::ControlPlaneObserver> GetObserver()
    {
        return controlPlaneObserver_;
    }

    static void SetExitHandler(const ExitHandler exitHandler)
    {
        exitHandler_ = exitHandler;
    }

    static void SetExitFailedHandler(const ExitFailedHandler handler)
    {
        exitFailedHandler_ = handler;
    }

    virtual InstanceState GetInstanceState();

    virtual litebus::Future<Status> TryExitInstance(const std::shared_ptr<litebus::Promise<Status>> &promise,
                                 const std::shared_ptr<KillContext> &killCtx, bool isSynchronized = false);

    void UpdateInstanceContext(const std::shared_ptr<InstanceContext> &context);

    void UpdateOwner(const std::string &owner);

    virtual void UpdateInstanceInfo(const resources::InstanceInfo &instanceInfo);

    virtual std::string GetOwner();

    virtual void ReleaseOwner();

    virtual resources::InstanceInfo GetInstanceInfo();

    virtual std::string GetRuntimeID();

    virtual void AddStateChangeCallback(const std::unordered_set<InstanceState> &statesConcerned,
                                        const std::function<void(const resources::InstanceInfo &)> &callback,
                                        const std::string &eventKey = "");

    virtual void DeleteStateChangeCallback(const std::string &eventKey);

    virtual bool HasStateChangeCallback(const std::string &eventKey);

    virtual std::shared_ptr<messages::ScheduleRequest> GetScheduleRequest();

    virtual std::shared_ptr<InstanceContext> GetInstanceContextCopy();

    virtual void SetScheduleTimes(const int32_t &scheduleTimes);

    virtual void SetDeployTimes(const int32_t &deployTimes);

    virtual int32_t GetScheduleTimes();

    virtual int32_t GetDeployTimes();

    virtual std::string GetRequestID();

    void SetLocalAbnormal();

    virtual void SetFunctionAgentIDAndHeteroConfig(const schedule_decision::ScheduleResult &result);

    virtual void SetDataSystemHost(const std::string &ip);

    virtual void SetRuntimeID(const std::string &runtimeID);

    virtual void SetStartTime(const std::string &timeInfo);

    virtual void SetRuntimeAddress(const std::string &address);

    virtual void IncreaseScheduleRound();

    virtual uint32_t GetScheduleRound();

    virtual void SetCheckpointed(const bool flag);

    virtual void SetVersion(const int64_t version);

    virtual int64_t GetVersion();

    virtual litebus::Future<bool> GetSavingFuture();

    virtual bool IsSaving();

    std::string Information();

    virtual int64_t GetGracefulShutdownTime();

    virtual void SetGracefulShutdownTime(const int64_t time);

    void SetTraceID(const std::string &traceID);

    virtual int32_t GetLastSaveFailedState();

    virtual int32_t GetLastSaveFailedErrCode();

    virtual void ResetLastSaveFailedState();

    virtual litebus::Future<resources::InstanceInfo> SyncInstanceFromMetaStore();

    virtual void SetUpdateByRouteInfo();

    virtual bool GetUpdateByRouteInfo();

    virtual void ExecuteStateChangeCallback(const std::string &requestID, const InstanceState newState);

    virtual void PublishToLocalObserver(const resources::InstanceInfo &newInstanceInfo, int64_t modRevision);

    virtual void PublishDeleteToLocalObserver(const std::string &instanceID);

    virtual void TagStop();
    virtual bool IsStopped();

    virtual void SetModRevision(const int64_t modRevision);
    virtual int64_t GetModRevision();

    void ExitInstance();

    virtual litebus::Future<std::string> GetCancelFuture();
    void SetCancel(const std::string &reason);

private:
    void NewSavingPomise();

    void TagSaved();

    void SetLastSaveFailedState(int32_t state, int32_t errorCode);

    litebus::Future<TransitionResult> SaveInstanceInfoToMetaStore(const resources::InstanceInfo &newInstanceInfo,
                                                                  const resources::InstanceInfo &prevInstanceInfo,
                                                                  const InstanceState oldState,
                                                                  const TransContext &context);
    litebus::Future<TransitionResult> PersistenceInstanceInfo(const resources::InstanceInfo &newInstanceInfo,
                                                              const resources::InstanceInfo &prevInstanceInfo,
                                                              const InstanceState oldState,
                                                              const TransContext &context);
    litebus::Future<TransitionResult> ResolveInitialLowReliabilityCreateConflict(
        const resources::InstanceInfo &newInstanceInfo, const resources::InstanceInfo &prevInstanceInfo,
        const Status &createStatus);

    TransitionResult VerifyTransitionState(const TransContext &context, std::string &requestID, InstanceState oldState);

    void PrepareTransitionInfo(const TransContext &context, resources::InstanceInfo &instanceInfo,
                              resources::InstanceInfo &previousInfo);
    void UpdateInstanceVersion(const TransContext &context, resources::InstanceInfo &instanceInfo);
    void SetInstanceBillingTerminated(const std::string &instanceID, const InstanceState &newState);

    static Status TransToStoredKeys(const resources::InstanceInfo &instanceInfo,
                                    std::shared_ptr<StoreInfo> &instancePutInfo,
                                    std::shared_ptr<StoreInfo> &routePutInfo,
                                    const PersistenceType persistence,
                                    std::string &key);
    static Status TransToStoredData(const resources::InstanceInfo &instanceInfo,
                                    std::shared_ptr<StoreInfo> &instancePutInfo,
                                    std::shared_ptr<StoreInfo> &routePutInfo, const PersistenceType persistence,
                                    std::string &key);
    static Status TransInstanceInfo(const resources::InstanceInfo &instanceInfo, std::string &output,
                                    litebus::Option<std::string> &path);
    static Status TransRouteInfo(const resources::InstanceInfo &instanceInfo, std::string &output, std::string &path);
    std::string owner_;
    std::string instanceID_;
    std::recursive_mutex lock_;
    bool isLocalAbnormal_ = false;

    // <InstanceState, ErrorCode>
    std::pair<int32_t, int32_t> lastSaveFailedState_{ INVALID_LAST_SAVE_FAILED_STATE, INVALID_LAST_SAVE_FAILED_STATE };
    std::shared_ptr<InstanceOperator> instanceOpt_;
    std::shared_ptr<InstanceContext> instanceContext_;
    std::unordered_map<std::string, StateChangeCallback> stateChangeCallbacks_;
    std::shared_ptr<litebus::Promise<bool>> savePromise_;
    int64_t exitTimes_ {0};
    bool isUpdateByRouteInfo_ = false;
    bool isMetaStoreEnable_ = false;
    std::atomic<bool> isWatching_{false};

    inline static ExitHandler exitHandler_;
    inline static ExitFailedHandler exitFailedHandler_;
    inline static std::shared_ptr<function_proxy::ControlPlaneObserver> controlPlaneObserver_;

    friend class InstanceStateMachineTest;
    bool IsInitialLowReliabilityRunningPersistence(const InstanceInfo &newInstanceInfo,
                                                   const InstanceState &oldState,
                                                   const int64_t version) const;
    bool IsFirstPersistence(const InstanceInfo &newInstanceInfo, const InstanceState &oldState,
                            const int64_t version) const;
};

}  // namespace functionsystem

#endif  // FUNCTION_PROXY_COMMON_STATE_MACHINE_INSTANCE_STATE_MACHINE_H
