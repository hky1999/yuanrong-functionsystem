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

#ifndef FUNCTION_AGENT_AGENT_SERVICE_ACTOR_H
#define FUNCTION_AGENT_AGENT_SERVICE_ACTOR_H

#include <actor/actor.hpp>
#include <cstddef>
#include <memory>
#include <queue>

#include "async/future.hpp"
#include "common/constants/constants.h"
#include "common/heartbeat/heartbeat_client.h"
#include "common/network/network_isolation.h"
#include "common/register/register_helper.h"
#include "common/snapshot_storage/snapshot_storage.h"
#include "common/static_function_util.h"
#include "common/types/instance_state.h"
#include "common/utils/struct_transfer.h"
#include "function_agent/code_deployer/deployer.h"
#include "function_agent/code_deployer/s3_deployer.h"
#include "function_agent/common/constants.h"
#include "function_agent/common/types.h"
#include "function_agent/network/network_tool.h"
#include "function_agent/plugin/multi_plugin_client.h"
#include "function_agent/snapshot/local_snapshot_store.h"
#include "function_agent/snapshot/snapshot_storage_config.h"

namespace functionsystem::snapshot_storage {
struct ArtifactPublishResult;
}

namespace functionsystem::function_agent {

const uint32_t DEFAULT_INTERVAL = 5000;
const uint32_t DOWNLOAD_CODE_RETRY_INTERVAL = 3000;                    // 3s
const uint32_t STATIC_FUNCTION_SCHEDULE_RETRY_INTERVAL = 3000;         // 3s
const std::string TENANT_PODIP_IPSET_NAME = "tenant-podip-whitelist";  // length cannot exceed 31

struct DeployerParameters {
    std::shared_ptr<Deployer> deployer;
    std::string destination;
    std::shared_ptr<messages::DeployRequest> request;
};

using DeployInstanceRequest = std::shared_ptr<messages::DeployInstanceRequest>;
struct DeployInstanceRequestWrapper {
    litebus::AID from;
    DeployInstanceRequest request;
    bool trustedResume{ false };
    messages::DeployInstanceRequest originalRequest;
};

using KillInstanceRequest = std::shared_ptr<messages::KillInstanceRequest>;
struct KillInstanceRequestWrapper {
    litebus::AID from;
    KillInstanceRequest request;
    litebus::AID runtimeManager;
    std::string runtimeManagerID;
};

struct SnapshotRuntimeFinalizeContext {
    litebus::AID caller;
    ::messages::SnapshotAttemptFinalizeRequest request;
    std::string runtimeID;
};

class AgentServiceActor : public litebus::ActorBase {
public:
    struct RuntimeManagerContext {
        std::string name;
        std::string address;
        std::string id;
        bool registered;
    };

    struct Config {
        litebus::AID localSchedFuncAgentMgrAID;
        S3Config s3Config;
        messages::CodePackageThresholds codePackageThresholds;
        std::string codePkgThresholdsCfgPath;
        uint32_t pingTimeoutMs = 0;
        std::string ipsetName = TENANT_PODIP_IPSET_NAME;
        std::string nodeID;
    };

    AgentServiceActor(const std::string &name, const std::string &agentID, const Config &config,
                      const std::string &alias = "", const std::string &componentName = "")
        : ActorBase(name),
          agentID_(agentID),
          alias_(alias),
          codeReferInfos_(std::make_shared<std::unordered_map<std::string, CodeReferInfo>>()),
          localSchedFuncAgentMgrAID_(config.localSchedFuncAgentMgrAID),
          runtimesDeploymentCache_(std::make_shared<RuntimesDeploymentCache>()),
          registeredResourceUnit_(std::make_shared<resources::ResourceUnit>()),
          s3Config_(config.s3Config),
          codePackageThresholds_(config.codePackageThresholds),
          codePkgThresholdsCfgPath_(config.codePkgThresholdsCfgPath),
          agentServiceName_(name),
          isRegisterCompleted_(false),
          pingTimeoutMs_(config.pingTimeoutMs),
          ipsetIsolation_(std::make_shared<IpsetIpv4NetworkIsolation>(config.ipsetName)),
          componentName_(componentName),
          nodeID_(config.nodeID)

    {
        randomUuid_ = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    }
    ~AgentServiceActor() override = default;

    virtual void Registered(const litebus::AID &from, std::string &&name, std::string &&msg);
    void TimeOutEvent();
    litebus::Future<messages::Registered> StartPingPong(const messages::Registered &registered);

    /**
     * request to deploy an instance from local scheduler to function agent
     * @param from: local scheduler's AID
     * @param name: function name
     * @param msg: request data, type is messages::DeployInstanceRequest
     */
    virtual void DeployInstance(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * request to kill an instance from local scheduler to function agent
     * @param from: local scheduler's AID
     * @param name: function name
     * @param msg: request data, type is messages::KillInstanceRequest
     */
    virtual void KillInstance(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * response of starting an instance from runtime manager to function agent
     * @param from: runtime manager's AID
     * @param name: function name
     * @param msg: response data, type is messages::StartInstanceResponse
     */
    virtual void StartInstanceResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * response of stopping an instance from runtime manager to function agent
     * @param from: runtime manager's AID
     * @param name: function name
     * @param msg: response data, type is messages::StartInstanceResponse
     */
    virtual void StopInstanceResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * request to update resources from runtime manager to function agent
     * @param from: runtime manager's AID
     * @param name: function name
     * @param msg: request data, type is messages::UpdateResourcesRequest
     */
    virtual void UpdateResources(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * request to update resources from runtime manager to function agent
     * @param from: runtime manager's AID
     * @param name: function name
     * @param msg: request data, type is messages::UpdateResourcesRequest
     */
    virtual void UpdateMetrics(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * request to update an instance's status from runtime manager to function agent
     * @param from: runtime manager's AID
     * @param name: function name
     * @param msg: request data, type is messages::UpdateInstanceStatusRequest
     */
    virtual void UpdateInstanceStatus(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * response of updating an instance's status from local scheduler to function agent
     * @param from: local scheduler's AID
     * @param name: function name
     * @param msg: response data, type is messages::UpdateInstanceStatusResponse
     */
    virtual void UpdateInstanceStatusResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * response of updating an agent's status from local scheduler to function agent
     * @param from: local scheduler's AID
     * @param name: function name
     * @param msg: response data, type is messages::UpdateAgentStatusResponse
     */
    virtual void UpdateAgentStatusResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     *  request to update a runtime manager's status from runtime manager to function agent
     * @param from: local scheduler's AID
     * @param name: function name
     * @param msg: response data, type is messages::UpdateDiskUsageRequest
     */
    virtual void UpdateRuntimeStatus(const litebus::AID &from, std::string &&name, std::string &&msg);

    virtual void MarkRuntimeManagerUnavailable(const std::string &id);
    void DrainPendingPauseRequestsForRuntimeManager(const std::string &runtimeManagerID,
                                                    const std::string &message);

    virtual void QueryInstanceStatusInfo(const litebus::AID &from, std::string &&name, std::string &&msg);

    virtual void QueryInstanceStatusInfoResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    virtual void CleanStatus(const litebus::AID &from, std::string &&name, std::string &&msg);

    virtual void CleanStatusResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    virtual void UpdateCred(const litebus::AID &, std::string &&, std::string &&msg);

    virtual void UpdateCredResponse(const litebus::AID &, std::string &&, std::string &&msg);
    virtual void UpdateNetworkPolicy(const litebus::AID &, std::string &&, std::string &&msg);

    virtual void UpdateNetworkPolicyResponse(const litebus::AID &, std::string &&, std::string &&msg);


    void GracefulShutdownFinish(const litebus::AID &, std::string &&, std::string &&msg);

    virtual void SetNetworkIsolationRequest(const litebus::AID &, std::string &&, std::string &&msg);

    /**
     * request to snapshot a runtime instance from function agent manager to function agent
     * @param from: function agent manager's AID
     * @param name: function name
     * @param msg: request data, type is messages::SnapshotRuntimeRequest
     */
    virtual void SnapshotRuntime(const litebus::AID &from, std::string &&name, std::string &&msg);

    void ResizeSandbox(const litebus::AID &from, std::string &&name, std::string &&msg);
    void ResizeSandboxResponse(const litebus::AID &from, std::string &&name, std::string &&msg);
    void ResizeSandboxTimeout(const std::string &forwardID);

    /**
     * response to snapshot runtime request from runtime manager to function agent
     * @param from: runtime manager's AID
     * @param name: function name
     * @param msg: response data, type is messages::SnapshotRuntimeResponse
     */
    virtual void SnapshotRuntimeResponse(const litebus::AID &from, std::string &&name, std::string &&msg);
    virtual void PublishSnapshotArtifact(const litebus::AID &from, std::string &&name, std::string &&msg);

    /** Handle immediate exact cleanup for a completed Pause/Resume attempt. */
    virtual void SnapshotAttemptFinalize(const litebus::AID &from, std::string &&name, std::string &&msg);
    virtual void DeleteReusableSnapshotArtifact(
        const litebus::AID &from, std::string &&name, std::string &&msg);
    virtual void ListLocalSnapshots(const litebus::AID &from, std::string &&name, std::string &&msg);
    virtual void DeleteLocalSnapshot(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * generation-fenced request to snapshot a runtime instance from local
     * scheduler to function agent; relayed to the runtime manager over a
     * dedicated channel that never emits the legacy SnapshotRuntime message
     * @param from: local scheduler's AID
     * @param name: function name
     * @param msg: request data, type is messages::SnapshotRuntimeRequest
     */
    void SnapshotRuntimeIfGeneration(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * response to the generation-fenced snapshot request from runtime manager
     * to function agent; only the fenced channel's own pending entry completes
     * @param from: runtime manager's AID
     * @param name: function name
     * @param msg: response data, type is messages::SnapshotRuntimeResponse
     */
    void SnapshotRuntimeIfGenerationResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    void SnapshotRuntimeIfGenerationTimeout(const std::string &forwardID);

    litebus::Future<bool> GracefulShutdown();

    litebus::Future<Status> SetDeployers(const std::string &storageType, const std::shared_ptr<Deployer> &deployer);

    litebus::Future<Status> Readiness();
    litebus::Future<Status> IsAgentReadiness();

    void SetRegisterHelper(const std::shared_ptr<RegisterHelper> &helper);

    void BindSnapshotDataPlane(
        const std::shared_ptr<snapshot_storage::SnapshotStorage> &snapshotStorage,
        const std::string &checkpointRoot, const std::string &storageBackend = "",
        SnapshotStorageMode storageMode = SnapshotStorageMode::LOCAL_ONLY,
        uint64_t localCacheMaxBytes = 0)
    {
        snapshotStorage_ = snapshotStorage;
        checkpointRoot_ = checkpointRoot;
        snapshotStorageBackend_ = storageBackend;
        snapshotStorageMode_ = storageMode;
        snapshotLocalCacheMaxBytes_ = localCacheMaxBytes;
        const bool validCheckpointRoot = !checkpointRoot_.empty()
            && std::filesystem::path(checkpointRoot_).is_absolute();
        const auto cacheBudget = storageMode == SnapshotStorageMode::DISTRIBUTED_CACHE
            ? localCacheMaxBytes : 0;
        // managed artifacts live under the dedicated 'managed' sub-root:
        // their lifecycle belongs to this store alone; the runtime
        // manager's anonymous checkpoint cache uses the sibling
        // 'anonymous' sub-root, so its TTL scan can never reach them
        const auto managedRoot = validCheckpointRoot
            ? (std::filesystem::path(checkpointRoot_) / "managed").lexically_normal()
            : std::filesystem::path{};
        localSnapshotStore_ = validCheckpointRoot
                                  ? std::make_shared<LocalSnapshotStore>(managedRoot, cacheBudget)
                                  : nullptr;
        if (snapshotStorage_ != nullptr && !checkpointRoot_.empty() && snapshotWorker_ == nullptr) {
            snapshotWorker_ = std::make_shared<ActorWorker>();
        }
    }

    virtual void QueryDebugInstanceInfos(const litebus::AID &, std::string &&, std::string &&msg);

    virtual void QueryDebugInstanceInfosResponse(const litebus::AID &, std::string &&, std::string &&msg);

    virtual void ReconcileRuntimes(const litebus::AID &from, std::string &&name, std::string &&msg);
    virtual void ReconcileRuntimesResponse(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<Status> CreateStaticFunctionInstance();

    litebus::Future<Status> LoadPlugins(const std::string &configs);

    void CodePkgThresholdsCfgCallback(const std::string &path, const std::string &name, uint32_t mask);

    void LoadCodePkgThresholdsCfg();

    // for test
    [[maybe_unused]] void SetIpsetName(std::string ipsetName)
    {
        ipsetName_ = ipsetName;
    }

    std::string GetIpsetName()
    {
        return ipsetName_;
    }

    // for test
    [[maybe_unused]] std::string GetRegisteredResourceUnitID() const
    {
        return registeredResourceUnit_->id();
    }

    // for test
    [[maybe_unused]] void SetRegisteredResourceUnit(const std::shared_ptr<resources::ResourceUnit> &unit)
    {
        registeredResourceUnit_ = unit;
    }

    // for test
    [[maybe_unused]] std::shared_ptr<HeartbeatClientDriver> GetPingPongDriver() const
    {
        return pingPongDriver_;
    }

    // for test
    [[maybe_unused]] std::shared_ptr<RuntimesDeploymentCache> GetRuntimesDeploymentCache() const
    {
        return runtimesDeploymentCache_;
    }

    // for test
    [[maybe_unused]] void UpdateRuntimesDeploymentCache(
        std::shared_ptr<RuntimesDeploymentCache> runtimesDeploymentCache)
    {
        runtimesDeploymentCache_ = std::move(runtimesDeploymentCache);
    }

    // for test
    [[maybe_unused]] void SetRuntimeManagerAID(const litebus::AID &aid, bool registered = true,
                                               const std::string &id = "")
    {
        registerRuntimeMgr_.name = aid.Name();
        registerRuntimeMgr_.address = aid.Url();
        registerRuntimeMgr_.registered = registered;
        registerRuntimeMgr_.id = id;
    }

    [[maybe_unused]] RuntimeManagerContext GetRuntimeManagerContext() const
    {
        return registerRuntimeMgr_;
    }

    // for test
    [[maybe_unused]] void SetLocalSchedFuncAgentMgrAID(const litebus::AID &aid)
    {
        localSchedFuncAgentMgrAID_ = aid;
    }

    // for test
    [[maybe_unused]] void SetCodeReferManager(
        const std::shared_ptr<std::unordered_map<std::string, function_agent::CodeReferInfo>> &codeReferManager)
    {
        codeReferInfos_ = codeReferManager;
    }

    // for test
    [[maybe_unused]] std::shared_ptr<std::unordered_map<std::string, CodeReferInfo>> GetCodeReferManager() const
    {
        return codeReferInfos_;
    }

    // for test
    [[maybe_unused]] void SetRegisterComplete(bool status)
    {
        isRegisterCompleted_ = status;
    }

    // for test
    [[maybe_unused]] bool GetRegisterComplete() const
    {
        return isRegisterCompleted_;
    }

    // for test
    [[maybe_unused]] void SetRegisterInfo(const RegisterInfo &registerInfo)
    {
        registerInfo_ = registerInfo;
    }

    // for test
    [[maybe_unused]] bool ProtectedSetNetwork(const std::vector<NetworkConfig> &configs)
    {
        return SetNetwork(configs);
    }

    // for test
    [[maybe_unused]] void ProtectedStartProbers(const std::vector<ProberConfig> &config)
    {
        StartProbers(config);
    }

    // for test
    [[maybe_unused]] void SetUpdateAgentStatusInfos(const std::unordered_map<std::string, litebus::Timer> &infoMap)
    {
        updateAgentStatusInfos_ = infoMap;
    }

    // for test
    [[maybe_unused]] void ProtectedReceiveRegister(const std::string &message)
    {
        ReceiveRegister(message);
    }

    // for test
    [[maybe_unused]] litebus::Future<messages::Registered> ProtectedRegisterAgent()
    {
        return RegisterAgent();
    }

    // for test
    [[maybe_unused]] void ProtectedRetryRegisterAgent(const std::string &msg)
    {
        RetryRegisterAgent(msg);
    }

    // for test
    [[maybe_unused]] void ProtectedAddCodeReferInfo(const messages::RuntimeInstanceInfo &info)
    {
        AddCodeReferByRuntimeInstanceInfo(info);
    }

    // for test
    [[maybe_unused]] void SetClearCodePackageInterval(uint32_t interval)
    {
        clearCodePackageInterval_ = interval;
    }

    // for test
    [[maybe_unused]] void SetRetrySendCleanStatusInterval(uint32_t interval)
    {
        retrySendCleanStatusInterval_ = interval;
    }

    // for test
    [[maybe_unused]] void SetRetryRegisterInterval(uint32_t interval)
    {
        retryRegisterInterval_ = interval;
    }

    // for test
    [[maybe_unused]] void SetUnitTestSituation(bool state)
    {
        isUnitTestSituation_ = state;
    }

    // for test
    [[maybe_unused]] std::shared_ptr<IpsetIpv4NetworkIsolation> GetIpsetIpv4NetworkIsolation()
    {
        return ipsetIsolation_;
    }

    // for test
    [[maybe_unused]] void SetIpsetIpv4NetworkIsolation(std::shared_ptr<IpsetIpv4NetworkIsolation> ipsetIsolation)
    {
        ipsetIsolation_ = ipsetIsolation;
    }

    // for test
    [[maybe_unused]] void SetFailedDownloadRequests(const std::string &requestID)
    {
        DeployResult result;
        result.status = Status(StatusCode::ERR_USER_CODE_LOAD, "code package download failed");
        failedDownloadRequests_[requestID] = result;
    }

    [[maybe_unused]] void SetFailedDeployingObjects(const std::string &destination)
    {
        litebus::Promise<DeployResult> promise;
        DeployResult result;
        result.status = Status(StatusCode::ERR_USER_CODE_LOAD, "code package download failed");
        promise.SetValue(result);
        deployingObjects_[destination] = promise;
    }

    // for test
    [[maybe_unused]] void SetS3Config(const S3Config &s3Config)
    {
        s3Config_ = s3Config;
    }

    // for test
    [[maybe_unused]] S3Config GetS3Config()
    {
        return s3Config_;
    }

    // for test
    [[maybe_unused]] void SetPingTimeoutMs(uint32_t pingTimeoutMs)
    {
        pingTimeoutMs_ = pingTimeoutMs;
    }

protected:
    // litebus virtual functions
    void Init() override;
    void Finalize() override;
    void DownloadCodeAndStartRuntime(const std::shared_ptr<std::queue<DeployerParameters>> &deployObjects,
                                     const std::shared_ptr<messages::DeployInstanceRequest> &req);

private:
    static bool RejectReservedResumeIdentityFromUntrustedSender(
        messages::DeployInstanceRequest &request, const litebus::AID &from,
        const litebus::AID &trustedSender);

    bool SetNetwork(const std::vector<NetworkConfig> &configs);
    void StartProbers(const std::vector<ProberConfig> &config);

    messages::DeployInstanceResponse InitDeployInstanceResponse(const int32_t code, const std::string &message,
                                                                const messages::DeployInstanceRequest &source);

    void InitKillInstanceResponse(messages::KillInstanceResponse *target, const messages::KillInstanceRequest &source);

    Status StartRuntime(const DeployInstanceRequest &request, const litebus::Future<Status> &prepareEnvRes);
    Status ForwardStartRuntime(
        const DeployInstanceRequest &request,
        const std::shared_ptr<messages::StartInstanceRequest> &startInstanceRequest,
        bool trustedResume);
    void OnTrustedResumeMaterialized(
        const DeployInstanceRequest &request,
        const std::shared_ptr<messages::StartInstanceRequest> &startInstanceRequest,
        const litebus::Future<Status> &materialized);
    litebus::Future<Status> MaterializeRemoteSnapshot(
        const DeployInstanceRequest &request,
        const std::shared_ptr<messages::StartInstanceRequest> &startInstanceRequest,
        bool reusable);
    Status CommitMaterializedSnapshot(
        const DeployInstanceRequest &request,
        const std::shared_ptr<messages::StartInstanceRequest> &startInstanceRequest,
        const Status &materialized);
    void ForgetSnapshotMaterialization(
        const std::string &snapshotID, const litebus::Future<Status> &materialized);
    void CompleteStartInstanceResponse(
        const litebus::AID &from, messages::StartInstanceResponse startInstanceResponse);
    litebus::Future<messages::Registered> RegisterAgent();
    void RetryRegisterAgent(const std::string &msg);
    void ReceiveRegister(const std::string &message);

    bool UpdateDeployedObjectByDestination(const std::shared_ptr<messages::DeployInstanceRequest> &req,
                                           const std::string &destination, const DeployResult &result);
    std::shared_ptr<std::queue<DeployerParameters>> BuildDeployerParameters(
        const std::shared_ptr<messages::DeployInstanceRequest> &req);
    void AddCodeReferByRuntimeInstanceInfo(const messages::RuntimeInstanceInfo &info);
    void AddCodeRefer(const std::string &dstDir, const std::string &instanceID,
                      const std::shared_ptr<Deployer> &deployer);
    void DeleteCodeReferByDeployInstanceRequest(const std::shared_ptr<messages::DeployInstanceRequest> &req);
    void DeleteCodeReferByRuntimeInstanceInfo(const messages::RuntimeInstanceInfo &info);
    void DeleteFunction(const std::string &functionDestination, const std::string &instanceID);
    litebus::Future<bool> UpdateAgentStatusToLocal(int32_t status, const std::string &msg = "");
    void RetryUpdateAgentStatusToLocal(const std::string &requestID, const std::string &msg);

    void RemoveCodePackageAsync();
    void CommitSuicide();
    void CleanRuntimeManagerStatus(uint32_t retryTimes);
    void GetDownloadCodeResult(const std::shared_ptr<std::queue<DeployerParameters>> &deployObjects,
                               const std::shared_ptr<messages::DeployInstanceRequest> &req,
                               const std::string &destination, const litebus::Future<DeployResult> &result);
    bool IsDownloadFailed(const std::shared_ptr<messages::DeployInstanceRequest> &req);
    static void StartCodeDownloadSpan(const DeployInstanceRequest &request);
    static void StopCodeDownloadSpan(const DeployInstanceRequest &request, const DeployResult &result);
    void DownloadCode(const std::shared_ptr<messages::DeployRequest> &request,
                      const std::shared_ptr<Deployer> &deployer,
                      const std::shared_ptr<litebus::Promise<DeployResult>> &promise, const uint32_t retryTimes);

    litebus::Future<DeployResult> AsyncDownloadCode(const std::shared_ptr<messages::DeployRequest> &request,
                                                    const std::shared_ptr<Deployer> &deployer);

    litebus::Future<Status> PrepareEnv(const DeployInstanceRequest &request);

    litebus::Future<bool> RecoverPluginCache(const std::string &message);

    void AttachTemporaryAccesskey(const std::string &storageType,
                                  std::shared_ptr<messages::DeployRequest> &deployRequest,
                                  const std::shared_ptr<messages::DeployInstanceRequest> &req);

    std::shared_ptr<messages::ScheduleRequest> CreateScheduleRequest(const StaticFunctionConfig &config,
                                                                     const FunctionMeta &meta);
    void RetryInstanceSchedule(const std::string &msg, const std::string &requestId, uint32_t retryTime);

    virtual void StaticFunctionScheduleResponse(const litebus::AID &from, std::string &&, std::string &&msg);
    virtual void NotifyFunctionStatusChange(const litebus::AID &from, std::string &&, std::string &&msg);
    DeployResult PrepareSharedDir(std::shared_ptr<messages::DeployInstanceRequest> &req);

    bool IsDelegateWorkingDirPath(const DeployerParameters &deployObject);

private:
    std::unordered_map<std::string, std::shared_ptr<Deployer>> deployers_;
    std::shared_ptr<MultiPluginClient> pluginClient_;

    /** <requestID : DeployInstanceRequestWrapper> for response */
    std::unordered_map<std::string, DeployInstanceRequestWrapper> deployingRequest_;
    /** <requestID : KillInstanceRequestWrapper> for response */
    std::unordered_map<std::string, KillInstanceRequestWrapper> killingRequest_;
    struct PendingSnapshotRequest {
        litebus::AID caller;
        litebus::AID publishCaller;
        ::messages::SnapshotRuntimeRequest request;
        litebus::AID runtimeManagerAID;
        std::string runtimeManagerID;
        std::string artifactPath;
        std::string artifactObjectKey;
        std::string storageBackend;
        int64_t createdAtUnixSeconds{ 0 };
        LocalSnapshotCommitRequest localCommitRequest;
        ::messages::LocalSnapshotMetadata localSnapshot;
        bool completed{ false };
        bool localReady{ false };
        bool publicationStarted{ false };
        ::messages::SnapshotRuntimeResponse localReadyResponse;
        ::messages::SnapshotRuntimeResponse completedResponse;
    };
    /** <requestID : pending SnapshotRuntime request> for response forwarding */
    std::unordered_map<std::string, PendingSnapshotRequest> snapshotRequests_;
    /** <requestID : caller's AID> scheduler-line fenced snapshot forwarding */
    std::unordered_map<std::string, litebus::AID> snapshotCallers_;
    struct ResizePending {
        litebus::AID requester;
        litebus::AID runtimeManager;
        std::shared_ptr<messages::ResizeSandboxRequest> request;
        litebus::Timer timeout;
    };
    std::unordered_map<std::string, ResizePending> resizeRequests_;
    // Pending relay state for the generation-fenced snapshot channel. Separate
    // from snapshotRequests_ so legacy unfenced attempts can never be completed
    // by a fenced response or vice versa.
    struct SnapshotIfGenerationPending {
        litebus::AID requester;
        litebus::AID runtimeManager;
        std::string runtimeManagerID;
        std::shared_ptr<messages::SnapshotRuntimeRequest> request;
        litebus::Timer timeout;
    };
    std::unordered_map<std::string, SnapshotIfGenerationPending> snapshotIfGenerationRequests_;
    std::string agentID_;
    std::string alias_;
    std::unordered_map<std::string, litebus::Promise<DeployResult>> deployingObjects_;
    // key: requestID, value: DeployResult
    std::unordered_map<std::string, DeployResult> failedDownloadRequests_;
    // key: requestID
    std::unordered_map<std::string, std::shared_ptr<litebus::Promise<Status>>> prepareEnvRequest_;

    // manager function code and layer's reference numbers
    std::shared_ptr<std::unordered_map<std::string, CodeReferInfo>> codeReferInfos_{ nullptr };

    litebus::AID localSchedFuncAgentMgrAID_;

    // keep runtimes' deployment configs, to update code references in handling KillInstance requests
    std::shared_ptr<RuntimesDeploymentCache> runtimesDeploymentCache_{ nullptr };

    // keep runtime_manager registered resource unit and register this unit to local scheduler
    std::shared_ptr<resources::ResourceUnit> registeredResourceUnit_{ nullptr };

    std::unordered_map<std::string, std::shared_ptr<litebus::Promise<bool>>> updateAgentStatusInfoPromises_;
    std::unordered_map<std::string, litebus::Timer> updateAgentStatusInfos_;

    // some configs passed by agent's startup parameters
    S3Config s3Config_;
    messages::CodePackageThresholds codePackageThresholds_;
    std::string codePkgThresholdsCfgPath_;

    std::string agentServiceName_;
    std::shared_ptr<HeartbeatClientDriver> pingPongDriver_{ nullptr };

    // Registration
    std::shared_ptr<RegisterHelper> registerHelper_{ nullptr };
    RegisterInfo registerInfo_;
    RuntimeManagerContext registerRuntimeMgr_{ "", "", "", false };
    bool isRegisterCompleted_;
    uint32_t pingTimeoutMs_;

    uint32_t retryRegisterInterval_{ REGISTER_AGENT_TIMEOUT };
    uint32_t retryDownloadInterval_{ DOWNLOAD_CODE_RETRY_INTERVAL };

    // Clean status
    litebus::Timer clearCodePackageTimer_;
    uint32_t clearCodePackageInterval_{ DEFAULT_INTERVAL };
    uint32_t retrySendCleanStatusInterval_{ DEFAULT_RETRY_SEND_CLEAN_STATUS_INTERVAL };
    int remainedClearCodePackageRetryTimes_{ -1 };
    bool isCleaningStatus_{ false };
    litebus::Promise<StatusCode> clearCodePackagePromise_;
    litebus::Promise<StatusCode> sendCleanStatusPromise_;
    bool monopolyUsed_{ false };
    bool isUnitTestSituation_{ false };
    bool exiting_ = false;
    litebus::Promise<bool> runtimeManagerGracefulShutdown_;
    int64_t gracefulShutdownTime_{ 0 };
    std::string ipsetName_{ TENANT_PODIP_IPSET_NAME };
    std::shared_ptr<IpsetIpv4NetworkIsolation> ipsetIsolation_{ nullptr };
    // when it is true and monopolyUsed is true, process will be restarted after runtime is killed
    bool enableRestartForReuse_{ false };

    std::string randomUuid_;
    uint32_t retryScheduleInterval_{ STATIC_FUNCTION_SCHEDULE_RETRY_INTERVAL };
    std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResponsePromise_;
    std::string componentName_;
    std::string nodeID_;
    std::shared_ptr<snapshot_storage::SnapshotStorage> snapshotStorage_;
    std::string snapshotStorageBackend_;
    SnapshotStorageMode snapshotStorageMode_{ SnapshotStorageMode::LOCAL_ONLY };
    uint64_t snapshotLocalCacheMaxBytes_{ 0 };
    std::string checkpointRoot_;
    std::shared_ptr<ActorWorker> snapshotWorker_;
    std::shared_ptr<LocalSnapshotStore> localSnapshotStore_;
    std::unordered_map<std::string, litebus::Future<Status>> snapshotMaterializations_;
    std::unordered_map<std::string, std::string> restoreSnapshotPins_;
    std::unordered_map<std::string, std::string> runtimeRestoreSnapshotPins_;
    std::unordered_map<std::string, SnapshotRuntimeFinalizeContext> snapshotRuntimeFinalizations_;

    bool HandleSnapshotFinalizeStopInstanceResponse(
        const litebus::AID &from, const ::messages::StopInstanceResponse &response);
    void ContinuePhysicalSnapshotFinalize(
        const litebus::AID &caller, const ::messages::SnapshotAttemptFinalizeRequest &request,
        size_t remoteKeyIndex);
    void OnPhysicalSnapshotObjectDeleted(
        const litebus::AID &caller, const ::messages::SnapshotAttemptFinalizeRequest &request,
        size_t remoteKeyIndex, const litebus::Future<Status> &future);
    void ForwardSnapshotRuntimeRequest(const std::string &requestID);
    void ReleaseRestoreSnapshotPin(const std::string &requestID);
    void PromoteRestoreSnapshotPin(const std::string &requestID, const std::string &runtimeID);
    void ReleaseRuntimeRestoreSnapshotPin(const std::string &runtimeID);
    void ContinueSnapshotAfterLocalCommit(
        const std::string &requestID, ::messages::SnapshotRuntimeResponse response,
        bool publishRequested = false);
    void SendSnapshotPhaseResponse(PendingSnapshotRequest &pending,
                                   const ::messages::SnapshotRuntimeResponse &response);
    void OnPauseArtifactPublished(
        const std::string &requestID,
        const litebus::Future<snapshot_storage::ArtifactPublishResult> &publishFuture);
    void OnReusableArtifactPublished(
        const std::string &requestID,
        const litebus::Future<snapshot_storage::ArtifactPublishResult> &publishFuture);
    void CompletePauseSnapshot(const std::string &requestID,
                               const snapshot_storage::SnapshotObjectMetadata &metadata);
    void CompleteReusableSnapshot(const std::string &requestID,
                                  const snapshot_storage::SnapshotObjectMetadata &metadata);
    void CompletePauseSnapshotError(const std::string &requestID, int32_t code,
                                    const std::string &message, bool resultUnknown);
    void OnReusableSnapshotArtifactDeleted(
        const litebus::AID &caller,
        const ::messages::DeleteReusableSnapshotArtifactRequest &request,
        const litebus::Future<Status> &future);
    void SendDeleteReusableSnapshotArtifactResponse(
        const litebus::AID &caller, const std::string &requestID,
        int32_t code, const std::string &message);
    void SendSnapshotAttemptFinalizeResponse(
        const litebus::AID &caller, const std::string &attemptID, int32_t code, const std::string &message,
        bool resultUnknown, bool localComplete, bool remoteComplete);

    bool HandleNetworkIsolation(const std::shared_ptr<messages::SetNetworkIsolationRequest> &req);

    // {instanceID, int32_t}
    std::unordered_map<std::string, int32_t> instanceHealthyMap_;

    bool isReady_ = false;
};

}  // namespace functionsystem::function_agent

#endif  // FUNCTION_AGENT_AGENT_SERVICE_ACTOR_H
