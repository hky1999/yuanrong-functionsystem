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

#ifndef LOCAL_SCHEDULER_FUNCTION_AGENT_MGR_FUNCTION_AGENT_MGR_ACTOR_H
#define LOCAL_SCHEDULER_FUNCTION_AGENT_MGR_FUNCTION_AGENT_MGR_ACTOR_H

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/utils/actor_driver.h"
#include "common/heartbeat/heartbeat_observer.h"
#include "meta_store_client/meta_store_client.h"
#include "common/observer/tenant_listener.h"
#include "common/proto/pb/message_pb.h"
#include "common/resource_view/resource_view.h"
#include "common/utils/actor_driver.h"
#include "common/utils/request_sync_helper.h"

namespace functionsystem::local_scheduler {
class InstanceCtrl;
class LocalSchedSrv;
class BundleMgr;

namespace function_agent_mgr {
const uint32_t DEFAULT_RETRY_TIMES = 6;
const uint32_t DEFAULT_RETRY_CYCLE = 10000;  // ms

const uint32_t MIN_PING_TIMES = 10;
const uint32_t MIN_PING_CYCLE = 1000;  // ms
const uint64_t GET_FUNC_AGENT_REGIS_INFO_CYCLE_MS = 3000;
const uint32_t GET_FUNC_AGENT_REGIS_INFO_RETRY_TIME = 3;
const uint64_t AGENT_FAILED_GC_TIME = 15 * 60 * 1000;

}  // namespace function_agent_mgr

using messages::RuleType;
using FunctionAgentCache = struct FunctionAgentCache {
    bool isAgentOnThisNode{false};
    std::string agentPodIp;
    std::unordered_set<std::string> instanceIDs;
};
using TenantCache = struct TenantCache {
    bool isTenantOnThisNode{false};
    // key: agent id
    std::unordered_map<std::string, FunctionAgentCache> functionAgentCacheMap;
    std::unordered_set<std::string> podIps;
};

struct SnapshotResult {
    int32_t code{0};
    std::string message;
    messages::SnapshotInfo snapshotInfo;
};

class FunctionAgentMgrActor : public BasisActor {
public:
    struct FuncAgentInfo {
        bool isEnable = false;
        bool isInit = false;
        std::shared_ptr<litebus::Promise<bool>> recoverPromise = nullptr;
        litebus::AID aid;
        std::unordered_set<std::string> instanceIDs;
    };

    enum class RegisStatus {
        SUCCESS = 1,
        FAILED = 2,  // json serialize deserialize will ignore value 0, so make FAILED equals to 2
        EVICTING = 3,
        EVICTED = 4
    };

    struct Param {
        uint32_t retryTimes{ function_agent_mgr::DEFAULT_RETRY_TIMES };
        uint32_t retryCycleMs{ function_agent_mgr::DEFAULT_RETRY_CYCLE };
        uint32_t pingTimes{ function_agent_mgr::MIN_PING_TIMES };
        uint32_t pingCycleMs{ function_agent_mgr::MIN_PING_CYCLE };
        bool enableTenantAffinity { true };
        int32_t tenantPodReuseTimeWindow { 10 };
        bool enableIpv4TenantIsolation { true };
        bool enableForceDeletePod { true };
        uint32_t getAgentInfoRetryMs {function_agent_mgr::GET_FUNC_AGENT_REGIS_INFO_CYCLE_MS};
        uint64_t invalidAgentGCInterval {function_agent_mgr::AGENT_FAILED_GC_TIME};
        bool enableCoProcessMode { false };
    };
    FunctionAgentMgrActor(const std::string &name, const Param &param, const std::string &nodeID,
                          std::shared_ptr<MetaStoreClient> metaStoreClient);

    ~FunctionAgentMgrActor() override;

    /**
     * get function agent registration information by function proxy id as key from etcd
     */
    litebus::Future<Status> Sync() override;

    litebus::Future<Status> Recover() override;

    // for metastore recovery
    void OnHealthyStatus(const Status &status);

    /**
     * register function agent to scheduler
     * @param from: function agent's AID
     * @param name: function name
     * @param msg: register message, type is message::registerRequest
     */
    virtual void Register(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * update resource of function agent to scheduler
     * @param from: function agent's AID
     * @param name: function name
     * @param msg: resource message, type is message::UpdateResourceRequest
     */
    virtual void UpdateResources(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * update status of instance to instance control
     * @param from: function agent's AID
     * @param name: function name
     * @param msg: status message, type is message::UpdateInstanceStatusRequest
     */
    virtual void UpdateInstanceStatus(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * response to request of deployment instance
     * @param from: function agent's AID
     * @param name: function name
     * @param msg: response data, type is message::DeployInstanceResponse
     */
    virtual void DeployInstanceResp(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * response to request of kill instance
     * @param from: function agent's AID
     * @param name: function name
     * @param msg: response data, type is message::KillInstanceResponse
     */
    virtual void KillInstanceResp(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * update status of function agent
     * @param from: function agent's AID
     * @param name: function name
     * @param msg: status message, type is message::UpdateAgentStatusRequest
     */
    virtual void UpdateAgentStatus(const litebus::AID &from, std::string &&name, std::string &&msg);

    virtual void BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl);

    virtual void BindResourceView(const std::shared_ptr<resource_view::ResourceView> &resourceView);

    virtual void BindLocalSchedSrv(const std::shared_ptr<LocalSchedSrv> &localSchedSrv);

    virtual void BindBundleMgr(const std::shared_ptr<BundleMgr> &bundleMgr);

    virtual litebus::Future<messages::DeployInstanceResponse> DeployInstance(
        const std::shared_ptr<messages::DeployInstanceRequest> &request, const std::string &funcAgentID);

    virtual litebus::Future<messages::StaticFunctionChangeResponse> NotifyFunctionStatusChange(
        const std::shared_ptr<messages::StaticFunctionChangeRequest> &request, const std::string &funcAgentID);

    virtual void RetryNotifyFunctionStatus(const std::string &requestID, const std::string &funcAgentID,
                                            const std::shared_ptr<messages::StaticFunctionChangeRequest> &request);

    void NotifyFunctionStatusChangeResp(const litebus::AID &from, std::string &&, std::string &&msg);

    virtual litebus::Future<messages::KillInstanceResponse> KillInstance(
        const std::shared_ptr<messages::KillInstanceRequest> &request, const std::string &funcAgentID,
        bool isRecovering);

    /**
     * show view of function agent manager
     * @return view information of JSON format
     */
    litebus::Future<std::string> Dump();

    litebus::Future<bool> IsRegistered(const std::string &funcAgentID);

    litebus::Future<Status> GracefulShutdown();

    /**
     * put function agent registration information as value with function proxy id as key into etcd
     * AIDs of corresponding function agents and runtime managers, runtime manager random ids
     * @return future of put status
     */
    litebus::Future<Status> PutAgentRegisInfoWithProxyNodeID();

    /**
     * retry retrieve function agent registration information from etcd
     */
    litebus::Future<Status> EvictAgent(const std::shared_ptr<messages::EvictAgentRequest> &req);

    virtual litebus::Future<Status> DoAddFuncAgent(const Status &status, const std::string &funcAgentID);

    litebus::Future<messages::InstanceStatusInfo> QueryInstanceStatusInfo(const std::string &funcAgentID,
                                                                          const std::string &instanceID,
                                                                          const std::string &runtimeID);

    void QueryInstanceStatusInfoResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    // proxy向agent发起instance信息查询
    virtual litebus::Future<Status> QueryDebugInstanceInfos();

    // agent向proxy返回查询的instance信息
    void QueryDebugInstanceInfosResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    virtual litebus::Future<Status> PutDebugInstanceInfos(const std::list<messages::DebugInstanceInfo> &debugInstInfos);

    void CleanStatusResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    void SetNetworkIsolationResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<messages::UpdateCredResponse> UpdateCred(
        const std::string &funcAgentID, const std::shared_ptr<messages::UpdateCredRequest> &request);

    void UpdateCredResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    void SendCleanStatusToFunctionAgent(const litebus::AID &funcAgentAid, uint32_t curRetryTimes);

    void OnTenantUpdateInstance(const TenantEvent &event);

    void OnTenantDeleteInstance(const TenantEvent &event);

    void UpdateLocalStatus(const litebus::AID &from, std::string &&name, std::string &&msg);

    void NotifyUpdateLocalResult(litebus::AID from, const uint32_t &localStatus, const bool &healthy);

    void StaticFunctionScheduleRequest(const litebus::AID &from, std::string &&, std::string &&msg);

    litebus::Future<Status> SendStaticFunctionScheduleResponse(const messages::ScheduleResponse &scheduleResponse,
                                                               const litebus::AID &from);

    litebus::Future<Status> RegisterToWarmUp(
        const std::shared_ptr<messages::DeployInstanceRequest> &request,
        const litebus::Option<std::string> &agentID);

    litebus::Future<Status> UnRegisterWarmUp(
        const std::shared_ptr<messages::KillInstanceRequest> &request);

    litebus::Future<messages::SnapshotRuntimeResponse> SnapshotRuntime(const std::string &requestID,
                                                     const resource_view::InstanceInfo &instanceInfo,
                                                     int32_t ttl,
                                                     bool leaveRunning);
    void SnapshotRuntimeResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<messages::ReconcileRuntimesResponse> ReconcileRuntimes(
        const std::string &funcAgentID,
        const std::shared_ptr<messages::ReconcileRuntimesRequest> &request);
    void ReconcileRuntimesResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    // for test
    [[maybe_unused]] void SetFuncAgentsRegis(
        std::unordered_map<std::string, messages::FuncAgentRegisInfo> &funcAgentsRegis)
    {
        for (auto &funcAgentInfo : funcAgentsRegis) {
            funcAgentsRegisMap_[funcAgentInfo.first] = funcAgentInfo.second;
        }
    }

    // for test
    [[maybe_unused]] std::unordered_map<std::string, messages::FuncAgentRegisInfo> GetFuncAgentsRegis()
    {
        return funcAgentsRegisMap_;
    }

    /**
     * trans to protobuf struct of function agent registration info from json string
     * @param messageInfo: protobuf struct of FuncAgentRegisInfoCollection
     * @param jsonStr: json string
     * @return true if trans success, false if trans failed
     */
    [[maybe_unused]] static bool TransToRegisInfoCollectionFromJson(messages::FuncAgentRegisInfoCollection &messageInfo,
                                                                    const std::string &jsonStr)
    {
        auto jsonOpt = google::protobuf::util::JsonParseOptions();
        jsonOpt.ignore_unknown_fields = true;
        return google::protobuf::util::JsonStringToMessage(jsonStr, &messageInfo, jsonOpt).ok();
    }

    /**
     * trans to json string from protobuf struct of function agent registration info collections
     * @param jsonStr: json string
     * @param messageInfo: protobuf struct of FuncAgentRegisInfoCollection
     * @return true if trans success, false if trans failed
     */
    [[maybe_unused]] static bool TransToJsonFromRegisInfoCollection(
        std::string &jsonStr, const messages::FuncAgentRegisInfoCollection &messageInfo)
    {
        return google::protobuf::util::MessageToJsonString(messageInfo, &jsonStr).ok();
    }

    // for test
    [[maybe_unused]] bool ClearFuncAgentsRegis()
    {
        funcAgentsRegisMap_.clear();
        return true;
    }

    // for test
    [[maybe_unused]] std::string GetNodeID()
    {
        return nodeID_;
    }

    // for test
    [[maybe_unused]] void SetNodeID(const std::string &nodeID)
    {
        nodeID_ = nodeID;
    }

    // for test
    [[maybe_unused]] void SetFuncAgentUpdateMapPromise(const std::string &funcAgentID,
                                                       const std::shared_ptr<resource_view::ResourceUnit> &resourceUnit)
    {
        if (funcAgentResUpdatedMap_.find(funcAgentID) == funcAgentResUpdatedMap_.end()) {
            return;
        }
        funcAgentResUpdatedMap_[funcAgentID].SetValue(resourceUnit);
    }

    // for test
    [[maybe_unused]] void SetRetrySendCleanStatusInterval(uint32_t interval)
    {
        retrySendCleanStatusInterval_ = interval;
    }

    // for test
    [[maybe_unused]] void EnableAgents()
    {
        for (auto &agent : funcAgentTable_) {
            agent.second.isEnable = true;
            if (agent.second.recoverPromise != nullptr) {
                YRLOG_INFO("Enable test agent({})", agent.first);
                agent.second.recoverPromise->SetValue(true);
            }
        }
    }

    [[maybe_unused]] void InsertAgent(const std::string agentID)
    {
        funcAgentTable_[agentID] = {
            .isEnable =  false,
            .isInit =  true,
            .recoverPromise = std::make_shared<litebus::Promise<bool>>(),
            .aid =  litebus::AID(),
            .instanceIDs =  {}
        };
    }

    // for test
    [[maybe_unused]] void InsertAgent(const std::string &agentID, const FuncAgentInfo &funcAgentInfo)
    {
        funcAgentTable_[agentID] = funcAgentInfo;
    }

    void TimeoutEvent(const std::string &funcAgentID);

    void StopHeartbeat(const std::string &funcAgentID);

    // for test
    [[maybe_unused]] std::unordered_map<std::string, std::shared_ptr<TenantCache>> GetTenantCacheMap()
    {
        return tenantCacheMap_;
    }

    // for test
    [[maybe_unused]] int32_t GetLocalStatus() const
    {
        return localStatus_;
    }

    litebus::Future<bool> IsFuncAgentRecovering(const std::string &funcAgentID);

    void SetAbnormal();

    void SetCoProcessReconcileCallback(std::function<void(const std::string &)> callback)
    {
        reconcileCallback_ = std::move(callback);
    }

protected:
    void Init() override;

    litebus::Future<Status> StartHeartbeat(const std::string &funcAgentID, const std::string &address);

    void ResetHeartbeat(const std::string &funcAgentID, const std::string &address);

    litebus::Future<std::shared_ptr<resource_view::ResourceUnit>> SetFuncAgentInfo(
        const Status &status, const std::string &funcAgentID,
        const std::shared_ptr<resource_view::ResourceUnit> &resourceUnit);

    litebus::Future<Status> SyncInstances(const std::shared_ptr<resource_view::ResourceUnit> &resourceUnit);

    litebus::Future<Status> AddFuncAgent(const Status &status, const std::string &funcAgentID,
                                         const std::shared_ptr<resource_view::ResourceUnit> &view);

    litebus::Future<Status> EnableFuncAgent(const litebus::Future<Status> &status, const std::string &funcAgentID);

    void RetryDeploy(const std::string &requestID, const std::string &funcAgentID,
                     const std::shared_ptr<messages::DeployInstanceRequest> &request);

    void RetryKill(const std::string &requestID, const std::string &funcAgentID,
                   const std::shared_ptr<messages::KillInstanceRequest> &request);

    litebus::Future<Status> UpdateInstanceStatusResp(const Status &status, const litebus::AID &aid,
                                                     const std::string &requestID);

    void SyncFailedAgentInstances();

    void SyncFailedAgentBundles();

private:
    /**
     * generate string from vector of function agents registration information
     * @param funcAgentsRegisMap function agents registration information map:
     * {key:runtime manager random ID, value:FuncAgentRegisInfo message}
     * @return generated string
     */
    std::string FuncAgentRegisToCollectionStr(
            std::unordered_map<std::string, messages::FuncAgentRegisInfo> &funcAgentsRegisMap);

    /**
     * put function agent registration information into etcd
     * @param regisInfoStrs function agent registration information
     */
    void PutAgentRegisInfo(const std::string &regisInfoStrs);

    void DoPutAgentRegisInfoWithProxyNodeID();
    void OnAgentInfoPut(const litebus::Future<std::shared_ptr<PutResponse>> &putResponse);

    /**
     * parse result after get function agent registration information form etcd
     * @param getResp response from meta store client
     */
    litebus::Future<Status> OnSyncAgentRegisInfoParser(const std::shared_ptr<GetResponse> &getResp);

    /**
     * help recover heartbeat between function proxy and corresponding function agents
     */
    void RecoverHeartBeatHelper();

    /**
     * wait for function agent corresponding resource unit
     * @param resourceUnit resource unit
     * @param funcAgentID function agent id
     * @return future of resource unit pointer
     */
    litebus::Future<std::shared_ptr<resource_view::ResourceUnit>> SetResourceUnitPromise(
            const std::shared_ptr<resource_view::ResourceUnit> &resourceUnit, const std::string &funcAgentID);

    /**
     * update function agent registration information status
     * @param funcAgentID function agent aid to be set to
     * @param status status of function agent to set
     */
    void UpdateFuncAgentRegisInfoStatus(const std::string &funcAgentID,
                                        const FunctionAgentMgrActor::RegisStatus &status);

    void LogPutAgentInfo(const litebus::Future<Status> &status, const std::string &funcAgentID);

    bool CheckFunctionAgentRegisterParam(const litebus::AID &from, const messages::Register &req);

    void EvictInstanceOnAgent(const std::shared_ptr<messages::EvictAgentRequest> &req);

    void RollbackEvictingAgent(const std::string &agentID, const int32_t &preStatus);

    void OnInstanceEvicted(const litebus::Future<Status> &future,
                           const std::shared_ptr<messages::EvictAgentRequest> &req);

    void NotifyEvcitResult(const std::string &agentID, StatusCode code, const std::string &msg);

    bool IsEvictingAgent(const std::string &agentID);
    bool IsEvictedAgent(const std::string &agentID);
    void DeferGCInvalidAgent(const std::string &agentID);

    void NotifyLocalAgentsInTenant(const std::shared_ptr<TenantCache> &tenantCache,
        const RuleType &type, std::vector<std::string> &rules, bool isClusterWide);

    void SetNetworkIsolation(const std::string &agentID, const RuleType &type, std::vector<std::string> &rules);

    void OnTenantFirstInstanceSchedInLocalPod(
        const std::shared_ptr<TenantCache> tenantCache, const TenantEvent& event);

    bool OnTenantInstanceSchedInRemotePodOnAnotherNode(
        const std::shared_ptr<TenantCache> tenantCache, const TenantEvent& event);

    bool OnTenantInstanceSchedInNewPodOnCurrentNode(
        const std::shared_ptr<TenantCache> tenantCache, const TenantEvent& event);

    bool OnTenantInstanceInPodDeleted(
        const std::shared_ptr<TenantCache> tenantCache, const TenantEvent& event);

    bool OnTenantInstanceInPodAllDeleted(
        const std::shared_ptr<TenantCache> tenantCache, const TenantEvent& event);

    bool ValidateUpdateResourcesRequest(messages::UpdateResourcesRequest &req, const litebus::AID &from);

    void CleanupAgentResources(const std::string &agentID, bool shouldDeletePod, const std::string &logMessage,
                               const std::shared_ptr<LocalSchedSrv> &localScheSrv,
                               const std::shared_ptr<messages::UpdateAgentStatusRequest> &req);

    litebus::Future<Status> DeleteRegisteredAgentInfos();

    uint32_t retryTimes_;
    uint32_t retryCycleMs_;
    uint32_t pingTimes_;
    uint32_t pingCycleMs_;
    bool enableTenantAffinity_;
    int32_t tenantPodReuseTimeWindow_;
    bool enableIpv4TenantIsolation_;
    uint64_t invalidAgentGCInterval_;

    using DeployNotifyPromise = litebus::Promise<messages::DeployInstanceResponse>;
    using KillNotifyPromise = litebus::Promise<messages::KillInstanceResponse>;
    using instanceStatusNotifyPromise = litebus::Promise<messages::StaticFunctionChangeResponse>;
    std::unordered_map<std::string, FuncAgentInfo> funcAgentTable_;  // key: function agent ID
    std::unordered_map<litebus::AID, std::string> aidTable_;         // key: AID, value: function agent ID

    // { agentID, { requestID, { promise, retryTimes }}}
    std::unordered_map<std::string,
                       std::unordered_map<std::string, std::pair<std::shared_ptr<DeployNotifyPromise>, uint32_t>>>
        deployNotifyPromise_;
    std::unordered_map<
        std::string, std::unordered_map<std::string, std::pair<std::shared_ptr<instanceStatusNotifyPromise>, uint32_t>>>
        instanceHealthyNotifyPromise_;
    std::unordered_map<std::string,
                       std::unordered_map<std::string, std::pair<std::shared_ptr<KillNotifyPromise>, uint32_t>>>
        killNotifyPromise_;

    std::weak_ptr<InstanceCtrl> instanceCtrl_;
    std::weak_ptr<resource_view::ResourceView> resourceView_;
    std::weak_ptr<LocalSchedSrv> localSchedSrv_;
    std::weak_ptr<BundleMgr> bundleMgr_;

    // use function-agent ID as key, function agent registration information as value
    std::unordered_map<std::string, messages::FuncAgentRegisInfo> funcAgentsRegisMap_;
    int32_t localStatus_{0};
    std::string nodeID_;
    std::shared_ptr<MetaStoreClient> metaStoreClient_;
    bool enableForceDeletePod_{ true };
    bool coProcessMode_{ false };
    std::function<void(const std::string &)> reconcileCallback_;

    // use function-agent ID as key, corresponding function agent ResourceUnit as value
    std::unordered_map<std::string, litebus::Promise<std::shared_ptr<resource_view::ResourceUnit>>>
        funcAgentResUpdatedMap_;

    uint32_t retrySendCleanStatusInterval_ { DEFAULT_RETRY_SEND_CLEAN_STATUS_INTERVAL };
    std::unordered_map<std::string, litebus::Promise<StatusCode>> sendCleanStatusPromiseMap_;
    std::set<std::string> monopolyAgents_;

    const uint32_t queryTimeout_ = 60000;
    const uint32_t updateTokenTimeout_ = 60000;
    const uint32_t checkpointRuntimeTimeout_ = 120000;  // checkpoint may take longer
    const uint32_t snapshotRuntimeTimeout_ = 120000;  // snapshot may take longer
    REQUEST_SYNC_HELPER(FunctionAgentMgrActor, messages::InstanceStatusInfo, queryTimeout_, queryStatusSync_);
    REQUEST_SYNC_HELPER(FunctionAgentMgrActor, messages::UpdateCredResponse, updateTokenTimeout_, updateTokenSync_);
    REQUEST_SYNC_HELPER(FunctionAgentMgrActor, messages::SnapshotRuntimeResponse,
                        snapshotRuntimeTimeout_, snapshotRuntimeSync_);
    REQUEST_SYNC_HELPER(FunctionAgentMgrActor, messages::QueryDebugInstanceInfosResponse,
                        QUERY_DEBUG_INSTANCE_INFO_INTERVAL_MS, queryDebugInstInfoSync_);
    const uint32_t reconcileTimeout_ = 120000;  // reconcile may take longer (waits for containerd)
    REQUEST_SYNC_HELPER(FunctionAgentMgrActor, messages::ReconcileRuntimesResponse, reconcileTimeout_, reconcileSync_);
    // key: request id, value: function agent id
    std::unordered_map<std::string, std::string> queryReqMap_;

    // key: tenant id
    std::unordered_map<std::string, std::shared_ptr<TenantCache>> tenantCacheMap_;

    std::shared_ptr<litebus::Promise<Status>> waitToPutAgentInfo_ {nullptr};
    std::shared_ptr<litebus::Promise<Status>> persistingAgentInfo_ {nullptr};
    bool abnormal_{ false };

    std::unordered_set<std::string> staticFunctionScheduleRequestIDs_;

    std::unordered_map<std::string, litebus::AID> pingPongAIDMap_;
    std::shared_ptr<HeartbeatObserveDriver> heartbeatObserveDriver_ = nullptr;
};
}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_FUNCTION_AGENT_MGR_FUNCTION_AGENT_MGR_ACTOR_H
