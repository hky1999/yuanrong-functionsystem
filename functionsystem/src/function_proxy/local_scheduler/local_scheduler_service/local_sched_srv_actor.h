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

#ifndef LOCAL_SCHEDULER_LOCAL_SCHED_SRV_ACTOR_H
#define LOCAL_SCHEDULER_LOCAL_SCHED_SRV_ACTOR_H

#include <actor/actor.hpp>
#include <async/future.hpp>
#include <map>
#include <string>

#include "common/explorer/explorer.h"
#include "common/heartbeat/heartbeat_client.h"
#include "common/heartbeat/heartbeat_observer.h"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix_pb.h"
#include "common/resource_view/resource_type.h"
#include "common/resource_view/resource_view_mgr.h"
#include "common/status/status.h"
#include "common/utils/actor_driver.h"
#include "common/utils/request_sync_helper.h"
#include "local_scheduler/function_agent_manager/function_agent_mgr.h"
#include "local_scheduler/instance_control/instance_ctrl.h"
#include "local_scheduler/subscription_manager/subscription_mgr.h"

namespace functionsystem::local_scheduler {
const int32_t PING_TIME_OUT_MS = 6000;
const uint32_t DEFAULT_REGISTER_CYCLE_MS = 1000;
const int32_t UPDATE_RESOURCE_CYCLE_MS = 1000;     // ms
const uint32_t FORWARD_SCHEDULE_MAX_RETRY = 3;
const uint32_t FORWARD_SCHEDULE_TIMEOUT = 200000;  // ms
const uint32_t GROUP_FORWARD_SCHEDULE_TIMEOUT = 20000; // ms
const uint32_t FORWARD_KILL_MAX_RETRY = 3;
const uint32_t FORWARD_KILL_TIMEOUT = 5000;        // ms
const uint32_t TRY_CANCEL_TIMEOUT = 5000;        // ms

class LocalSchedSrvActor : public BasisActor {
public:
    struct Param {
        std::string nodeID;
        std::string globalSchedAddress;
        bool isK8sEnabled{ false };
        uint32_t registerCycleMs{ DEFAULT_REGISTER_CYCLE_MS };
        uint32_t pingTimeOutMs{ PING_TIME_OUT_MS };
        uint32_t updateResourceCycleMs{ UPDATE_RESOURCE_CYCLE_MS };
        std::string componentName{ "" };
        uint32_t forwardRequestTimeOutMs{ FORWARD_SCHEDULE_TIMEOUT };
        uint32_t groupScheduleTimeout{ GROUP_FORWARD_SCHEDULE_TIMEOUT };
        uint32_t groupKillTimeout{ FORWARD_KILL_TIMEOUT };
    };
    explicit LocalSchedSrvActor(const Param &param);

    // use it before start actor
    void BindPingPongDriver(const std::shared_ptr<HeartbeatClientDriver> &pingPongDriver);

    ~LocalSchedSrvActor() override;

    void ToReady() override;
    /**
     * receive request to schedule instance from domain scheduler or runtime
     * @param from: caller AID
     * @param name: interface name
     * @param msg: schedule request
     */
    virtual void Schedule(const litebus::AID &from, std::string &&, std::string &&msg);

    /**
     * receive request update domain scheduler info from global scheduler when the domain scheduler to which the local
     * scheduler belongs changed(eg. domain scheduler failure exit)
     * @param from: caller AID
     * @param name: interface name
     * @param msg: domain scheduler info
     */
    void UpdateSchedTopoView(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * receive the returned registry result request from domain scheduler or global scheduler
     * @param from: caller AID
     * @param name: interface name
     * @param msg: null when register to domain scheduler, and domain scheduler info when register to global scheduler
     */
    void Registered(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * send registry request to global and domain scheduler
     * @return register result
     */
    litebus::Future<Status> Register();

    /**
     * evicted all agent & instance, adn send unregister request to global and domain scheduler
     * @return register result
     */
    litebus::Future<Status> GracefulShutdown();

    /**
     * receive the returned unregister result request from global scheduler
     * @param from: caller AID
     * @param name: interface name
     */
    void UnRegistered(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * local scheduler forwards schedule request to domain scheduler when local scheduler schedules failed
     * @param req: schedule request
     * @return forward schedule response
     */
    litebus::Future<messages::ScheduleResponse> ForwardSchedule(const std::shared_ptr<messages::ScheduleRequest> &req);

    /**
     * notify worker status to domain scheduler -> global scheduler
     *
     * @param healthy worker status
     * @return status
     */
    litebus::Future<Status> NotifyWorkerStatus(const bool healthy);
    litebus::Future<Status> UpdateSchedulingStatus(bool evicting);
    void OnUpdateSchedulingStatus(const litebus::AID &from, std::string &&name, std::string &&msg);
    void SendUpdateSchedulingStatusResponse(const litebus::Future<Status> &status,
                                            const litebus::AID &to,
                                            const std::string &requestID);

    /**
     * receive forwarding schedule response from domain scheduler
     * @param from: caller AID(domain scheduler AID)
     * @param name: interface name
     * @param msg: forward schedule response
     */
    virtual void ResponseForwardSchedule(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * receive notify worker status response
     *
     * @param from: caller AID(domain scheduler AID)
     * @param name: interface name
     * @param msg:  NotifyWorkerStatusResponse
     */
    virtual void ResponseNotifyWorkerStatus(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * evict agent by specific agentID recevied from master
     *
     * @param from: caller AID(function master AID)
     * @param name: interface name
     * @param msg:  EvictAgentRequest
     */
    void EvictAgent(const litebus::AID &from, std::string &&name, std::string &&msg);

    void PreemptInstances(const litebus::AID &from, std::string &&name, std::string &&msg);
    /**
     * ACK of the evicting result received by the master
     *
     * @param from: caller AID(function master AID)
     * @param name: interface name
     * @param msg:  EvictAgentResultAck
     */
    void NotifyEvictResultAck(const litebus::AID &, std::string &&, std::string &&msg);

    /**
     * delete pod response received from master
     *
     * @param from: caller AID(function master AID)
     * @param name: interface name
     * @param msg:  DeletePodResponse
     */
    void DeletePodResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * Notify the master of the evict result.
     * @param req: EvictAgentResult request
     * @return Status
     */
    virtual void NotifyEvictResult(const std::shared_ptr<messages::EvictAgentResult> &req);

    litebus::Future<messages::ForwardKillResponse> ForwardKillToInstanceManager(
        const std::shared_ptr<messages::ForwardKillRequest> &req);
    void ResponseForwardKill(const litebus::AID &from, std::string &&name, std::string &&msg);

    void BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl);

    void DeletePod(const std::string &agentID, const std::string &reqID, const std::string &msg);

    void BindResourceView(const std::shared_ptr<resource_view::ResourceViewMgr> &resourceViewMgr);

    void BindFunctionAgentMgr(const std::shared_ptr<FunctionAgentMgr> &functionAgentMgr);

    void BindSubscriptionMgr(const std::shared_ptr<SubscriptionMgr> &subscriptionMgr);

    void UpdateMasterInfo(const explorer::LeaderInfo &leaderInfo);

    litebus::Future<std::string> QueryMasterIP();

    litebus::Future<messages::GroupResponse> ForwardGroupSchedule(
        const std::shared_ptr<messages::GroupInfo> &groupInfo);
    void DoForwardGroupSchedule(const std::shared_ptr<litebus::Promise<messages::GroupResponse>> &promise,
                                std::chrono::time_point<std::chrono::high_resolution_clock> beginTime,
                                const std::shared_ptr<messages::GroupInfo> &groupInfo);
    void OnForwardGroupSchedule(const litebus::AID &from, std::string &&name, std::string &&msg);
    litebus::Future<Status> KillGroup(const std::shared_ptr<messages::KillGroup> &killReq);
    void DoKillGroup(const std::shared_ptr<litebus::Promise<Status>> &promise,
                     const std::shared_ptr<messages::KillGroup> &killReq);
    void OnKillGroup(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<Status> TryCancelSchedule(const std::shared_ptr<messages::CancelSchedule> &cancelRequest);

    litebus::Future<::messages::RecordSnapshotResponse> RecordSnapshotMetadata(
        const std::shared_ptr<::messages::RecordSnapshotRequest> &req);
    void OnRecordSnapshotMetadataResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<::messages::RestoreSnapshotResponse> SnapStartCheckpoint(
        const std::shared_ptr<::messages::RestoreSnapshotRequest> &req);
    void DoSnapStartCheckpoint(const std::shared_ptr<litebus::Promise<::messages::RestoreSnapshotResponse>> &promise,
                               const std::shared_ptr<::messages::RestoreSnapshotRequest> &req);
    void OnSnapStartCheckpointResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<::messages::ListSnapshotsByFunctionKeyResponse> ListSnapshotsByFunctionKey(
        const std::shared_ptr<::messages::ListSnapshotsByFunctionKeyRequest> &req);
    void OnListSnapshotsByFunctionKeyResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<::messages::ListSnapshotsByTenantResponse> ListSnapshotsByTenant(
        const std::shared_ptr<::messages::ListSnapshotsByTenantRequest> &req);
    void OnListSnapshotsByTenantResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<::messages::DeleteSnapshotResponse> DeleteSnapshot(
        const std::shared_ptr<::messages::DeleteSnapshotRequest> &req);
    void OnDeleteSnapshotResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * Receive tenant quota exceeded notification from domain scheduler.
     * Blocks scheduling for the tenant during cooldown period.
     * @param from: caller AID
     * @param name: interface name
     * @param msg: serialized TenantQuotaExceeded proto
     */
    void OnTenantQuotaExceeded(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<Status> IsRegisteredToGlobal();

    // for test
    [[maybe_unused]] litebus::Future<litebus::AID> GetDomainSchedAID()
    {
        return domainSchedRegisterInfo_.aid;
    }

    // for test
    [[maybe_unused]] bool HeartBeatInvalid()
    {
        return pingPongDriver_ == nullptr;
    }

    [[maybe_unused]] void UpdateGlobalSchedulerAddress(const litebus::AID aid)
    {
        globalSchedRegisterInfo_.aid = aid;
    }

    // for test
    [[maybe_unused]] void UpdateDomainSchedulerAddress(const litebus::AID aid)
    {
        domainSchedRegisterInfo_.aid = aid;
    }

    [[maybe_unused]] litebus::Future<bool> GetEnableFlag()
    {
        return enableService_;
    }

    [[maybe_unused]] void Disable()
    {
        enableService_ = false;
    }

protected:
    void Init() override;
    void Finalize() override;

    void TimeOutEvent();
    litebus::Future<Status> EnableLocalSrv(const litebus::Future<Status> &future);

    Status ScheduleResp(const std::shared_ptr<messages::ScheduleResponse> &scheduleRsp,
                      const messages::ScheduleRequest &req, const litebus::AID &from);
    litebus::Future<std::shared_ptr<messages::ScheduleResponse>> CollectCurrentResource(
        const messages::ScheduleResponse &resp);
    litebus::Future<Status> AsyncNotifyWorkerStatus(const std::shared_ptr<messages::NotifyWorkerStatusRequest> &req,
                                                    const bool isRetry = false);

private:
    struct RegisterInfo {
        litebus::AID aid;
        litebus::Promise<messages::Registered> registeredPromise;
        litebus::Timer reRegisterTimer;
        std::string name;
    };

    litebus::Future<Status> UnRegister();

    // send registry request to domain scheduler
    litebus::Future<messages::Registered> RegisterToDomainScheduler(const messages::Registered &registeredFuture);

    // send registry request to global scheduler
    litebus::Future<messages::Registered> RegisterToGlobalScheduler();

    litebus::Future<messages::Registered> DoRegistry(RegisterInfo &info, bool isRetry = false);

    Status SendRegisterWithRes(const litebus::AID aid, const std::shared_ptr<messages::Register> &req,
                               const std::unordered_map<ResourceType, std::shared_ptr<ResourceUnit>> &resources);

    void RetryRegistry(const litebus::AID &aid);

    void ForwardScheduleWithRetry(const std::shared_ptr<messages::ScheduleRequest> &req,
                                  const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &promise,
                                  const uint32_t retryTimes);

    void ForwardKillWithRetry(const std::shared_ptr<messages::ForwardKillRequest> &req, const uint32_t retryTimes);

    void GenErrorForwardResponseClearPromise(
        const std::shared_ptr<messages::ScheduleRequest> &req,
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &promise, const std::string &errorMsg,
        const int32_t code);

    void GenErrorForwardKillClearPromise(const std::shared_ptr<messages::ForwardKillRequest> &req,
                                         const std::string &errorMsg, const int32_t code);

    void SendForwardToDomain(const std::shared_ptr<messages::ScheduleRequest> &req);

    void SendEvictAck(const litebus::Future<Status> &status, const std::shared_ptr<messages::EvictAgentRequest> &req,
                      const litebus::AID &to);

    void DoTryCancel(const std::shared_ptr<messages::CancelSchedule> &cancelRequest,
                     const std::shared_ptr<litebus::Promise<Status>> &promise);
    void TryCancelResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::AID masterAid_;

    std::string nodeID_;
    bool isK8sEnabled_ = false;

    uint32_t registerCycleMs_;  // millisecond
    uint32_t pingTimeOutMs_;
    uint32_t updateResourceCycleMs_;
    uint32_t forwardRequestTimeOutMs_;

    bool enableService_ = false;
    bool dsWorkerHealthy_ = true;
    bool exiting_ = false;

    RegisterInfo globalSchedRegisterInfo_;
    RegisterInfo domainSchedRegisterInfo_;
    std::shared_ptr<resource_view::ResourceViewMgr> resourceViewMgr_;
    std::weak_ptr<InstanceCtrl> instanceCtrl_;
    std::shared_ptr<FunctionAgentMgr> functionAgentMgr_;
    std::shared_ptr<HeartbeatClientDriver> pingPongDriver_;

    std::map<std::string, std::shared_ptr<litebus::Promise<messages::ScheduleResponse>>> forwardSchedulePromise_;
    std::map<std::string, std::shared_ptr<litebus::Promise<messages::ForwardKillResponse>>> forwardKillPromise_;
    std::unordered_map<std::string, std::shared_ptr<litebus::Promise<Status>>> cancelPromise_;
    const uint32_t notifyWorkerStatusTimeout_ = 5000;
    REQUEST_SYNC_HELPER(LocalSchedSrvActor, Status, notifyWorkerStatusTimeout_, notifyWorkerStatusSync_);
    const uint32_t notifyEvictResultTimeout_ = 5000;
    REQUEST_SYNC_HELPER(LocalSchedSrvActor, Status, notifyEvictResultTimeout_, notifyEvictResultSync_);

    const uint32_t groupTimeout_ = 10000;
    REQUEST_SYNC_HELPER(LocalSchedSrvActor, messages::GroupResponse, groupTimeout_, requestGroupScheduleMatch_);
    const uint32_t groupKillTimeout_ = 3000;
    REQUEST_SYNC_HELPER(LocalSchedSrvActor, Status, groupKillTimeout_, requestGroupKillMatch_);
    const uint32_t deletePodTimeout_ = 5000;
    REQUEST_SYNC_HELPER(LocalSchedSrvActor, Status, deletePodTimeout_, deletePodMatch_);

    const uint32_t recordSnapshotTimeout_ = 10000;
    REQUEST_SYNC_HELPER(LocalSchedSrvActor, messages::RecordSnapshotResponse,
                        recordSnapshotTimeout_, recordSnapshotSync_);

    // W10-2: the SnapStart round trip includes the domain-scheduler hop and
    // measures 10-20s live (response arriving after a 10s timeout is dropped
    // as "no matching request", the promise then settles as a spurious error,
    // the parked entry survives a SUCCESSFUL physical restore, and the wake
    // path re-fires duplicates every monitor tick). 30s covers the observed
    // tail; the parked-registry waking flag still bounds concurrency.
    const uint32_t snapStartCheckpointTimeout_ = 30000;
    REQUEST_SYNC_HELPER(LocalSchedSrvActor, messages::RestoreSnapshotResponse,
                        snapStartCheckpointTimeout_, snapStartCheckpointSync_);
    const uint32_t listSnapshotTimeout_ = 10000;
    REQUEST_SYNC_HELPER(LocalSchedSrvActor, ::messages::ListSnapshotsByFunctionKeyResponse, listSnapshotTimeout_,
                        listSnapshotsByFunctionKeySync_);
    REQUEST_SYNC_HELPER(LocalSchedSrvActor, ::messages::ListSnapshotsByTenantResponse, listSnapshotTimeout_,
                        listSnapshotsByTenantSync_);
    REQUEST_SYNC_HELPER(LocalSchedSrvActor, ::messages::DeleteSnapshotResponse, listSnapshotTimeout_,
                        deleteSnapshotSync_);

    litebus::Promise<Status> unRegistered_;

    std::shared_ptr<SubscriptionMgr> subscriptionMgr_;

    std::string  componentName_;
};

}  // namespace functionsystem::local_scheduler
#endif  // LOCAL_SCHEDULER_LOCAL_SCHED_SRV_ACTOR_H
