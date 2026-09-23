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
#ifndef RUNTIME_MANAGER_MANAGER_RUNTIME_MANAGER_H
#define RUNTIME_MANAGER_MANAGER_RUNTIME_MANAGER_H

#include "common/constants/constants.h"
#include "common/heartbeat/heartbeat_client.h"
#include "common/register/register_helper.h"
#include "runtime_manager/config/flags.h"
#include "runtime_manager/debug/debug_server_mgr.h"
#include "runtime_manager/executor/executor.h"
#include "runtime_manager/healthcheck/health_check.h"
#include "runtime_manager/log/log_manager.h"
#include "runtime_manager/metrics/metrics_client.h"

namespace functionsystem::runtime_manager {

class RuntimeManager : public litebus::ActorBase {
public:
    explicit RuntimeManager(const std::string &name, bool logReuse = false);

    ~RuntimeManager() override = default;

    /**
     * Register execute actor to function agent.
     *
     * @param from Execute actor AID.
     */
    void RegisterToFunctionAgent();

    /**
     * Create Instance when receive create message from function agent.
     *
     * @param from Function agent aid.
     * @param msg Runtime infos.
     */
    void StartInstance(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * Kill Instance when receive kill message from function agent.
     *
     * @param from Function agent aid.
     * @param msg Runtime infos.
     */
    void StopInstance(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * Snapshot Runtime when receive snapshot message from function agent.
     *
     * @param from Function agent aid.
     * @param msg Snapshot request with runtime infos.
     */
    void SnapshotRuntime(const litebus::AID &from, std::string &&name, std::string &&msg);

    /** Delete one exact reusable checkpoint after the logical Snapshot READY commit. */
    void ResizeSandbox(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * Generation-fenced snapshot when receiving the fenced snapshot message
     * from function agent. Only the sandboxd executor honors the expected
     * physical identity pair, so no other executor type is consulted.
     *
     * @param from Function agent aid.
     * @param msg Snapshot request with complete expected identity.
     */
    void SnapshotRuntimeIfGeneration(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * OOM Kill instance when receive event from metrics actor.
     */
    void OomKillInstance(const std::string &instanceID, const std::string &runtimeID, const std::string &requestID);

    /**
     * Inner OOM(RuntimeMemoryExceedLimit) Kill (from runtime_manager) instance: clean and stop instance.
     */
    void InnerOomKillInstance(const litebus::Future<Status> &status, const std::string &instanceID,
                              const std::string &runtimeID, const std::string &requestID);

    void DeleteOomNotifyData(const litebus::Future<Status> &status,
                             const std::shared_ptr<messages::StopInstanceRequest> &request);

    /**
     * QueryRuntimeStatus
     *
     * @param from Function agent aid.
     * @param msg instanceID.
     */
    void QueryInstanceStatusInfo(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * QueryDebugInstanceInfos
     *
     * @param from Function agent aid.
     */
    void QueryDebugInstanceInfos(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * clean status request from funtion agent
     */
    void CleanStatus(const litebus::AID &from, std::string &&, std::string &&msg);

    void UpdateCred(const litebus::AID &from, std::string &&, std::string &&msg);
    void UpdateNetworkPolicy(const litebus::AID &from, std::string &&, std::string &&msg);

    /**
     *  Handle Prestart Runtime Process Exit from HealthCheckActor
     *
     * @param msg exit process pid
     */
    void HandlePrestartRuntimeExit(const pid_t pid);

    void SetConfig(const Flags &flags);

    void CollectCpuType();

    std::string GetCpuType() const;

    void SetRegisterHelper(const std::shared_ptr<RegisterHelper> helper);

    void Start();

    void HeartbeatTimeoutHandler(const litebus::AID &from);

    litebus::Future<bool> GracefulShutdown();
    void OnExecuterShutdown();

    std::string GetCpuTypeByProc();

    std::string GetCpuTypeByCommand();

    litebus::Future<bool> IsRuntimeActive(const std::string &runtimeID);

    litebus::Future<bool> IsRuntimeActiveByPid(const pid_t &pid);

    void QueryRuntimeActiveResponse(const litebus::AID &to, const litebus::Future<bool> &isActiveFutrue,
        const std::string &runtimeID, const std::string &requestID);

    litebus::Future<Status> NotifyInstancesDiskUsageExceedLimit(const std::string &description, const int limit);

    EXECUTOR_TYPE GetRuntimeType(const std::string &runtimeID);

    static std::string RuntimeIDForStart(const messages::RuntimeInstanceInfo &instance,
                                         bool deterministicAttempt);

protected:
    void Init() override;

    void Finalize() override;

private:
    void OnResizeSandboxComplete(const litebus::AID &from,
        const std::shared_ptr<messages::ResizeSandboxRequest> &request,
        const litebus::Future<messages::ResizeSandboxResponse> &result);

    void OnSnapshotRuntimeIfGenerationComplete(const litebus::AID &from,
        const std::shared_ptr<messages::SnapshotRuntimeRequest> &request,
        const litebus::Future<messages::SnapshotRuntimeResponse> &result);

    enum class SandboxRuntimeDiscoveryState { NOT_REQUIRED, PENDING, READY };

    EXECUTOR_TYPE ResolveStopExecutorType(const std::shared_ptr<messages::StopInstanceRequest> &request);

    Status ResolveSnapshotExecutorType(const messages::SnapshotRuntimeRequest &request,
                                       EXECUTOR_TYPE &executorType);

    std::unordered_map<EXECUTOR_TYPE, std::shared_ptr<ExecutorProxy>> executorMap_;

    std::shared_ptr<MetricsClient> metricsClient_;

    std::shared_ptr<HealthCheck> healthCheckClient_;

    std::shared_ptr<LogManager> logManagerClient_;

    bool runtimeInstanceDebugEnable_{ false };
    std::shared_ptr<DebugServerMgr> debugServerMgr_{ nullptr };

    std::shared_ptr<RegisterHelper> registerHelper_{ nullptr };

    litebus::AID functionAgentAID_;
    std::string runtimeManagerID_;
    std::string checkpointDir_;
    bool isUnitTestSituation_{ false };
    uint32_t pingTimeoutMs_{ DEFAULT_PING_PONG_TIMEOUT };

    std::string nodeID_ = "nodeID";
    std::string cpuType_;

    std::map<std::string, messages::RuntimeInstanceInfo> instanceInfoMap_;
    std::map<std::string, messages::StartInstanceResponse> instanceResponseMap_;
    std::map<std::string, messages::StartInstanceResponse> runtimeResponseMap_;
    std::map<std::string, std::string> latestInFlightStartRequestByInstance_;
    std::unordered_set<std::string> receivedStartingReq_;

    bool connected_ = false;
    SandboxRuntimeDiscoveryState sandboxRuntimeDiscoveryState_{SandboxRuntimeDiscoveryState::NOT_REQUIRED};
    bool startRequested_{false};
    bool initialRegistrationStarted_{false};

    bool logReuse_ = false;

    std::shared_ptr<ExecutorProxy> FindExecutor(EXECUTOR_TYPE);
    std::shared_ptr<ExecutorProxy> CreateRuntimeExecutor();
    std::shared_ptr<ExecutorProxy> CreateSupervisorExecutor();
    std::shared_ptr<ExecutorProxy> CreateConchExecutor();
    std::shared_ptr<ExecutorProxy> CreateDockerExecutor();
    std::shared_ptr<ExecutorProxy> CreateSandboxdExecutor();

    void TryInitialRegisterToFunctionAgent();
    void OnInitialAvailableRuntimes(bool enabled, const std::set<std::string> &runtimes);
    Status OnInitialAvailableRuntimesApplied(const Status &status);

    // Human-readable message for an executor type whose daemon/service is not ready.
    static std::string GetExecutorUnavailableMessage(EXECUTOR_TYPE type);

    void StartInstanceResponse(const litebus::AID &from, const std::string &instanceID,
                               const std::string &requestID,
                               const litebus::Future<messages::StartInstanceResponse> &response);
    void StartInstanceExecutorUnavailable(const litebus::AID &from,
                                          const std::shared_ptr<messages::StartInstanceRequest> &request,
                                          EXECUTOR_TYPE type);
    void StartInstanceCapabilityInvalid(const litebus::AID &from,
                                        const std::shared_ptr<messages::StartInstanceRequest> &request);

    void StopInstanceResponse(const litebus::AID &from, const litebus::Future<Status> &status,
                              const std::shared_ptr<messages::StopInstanceRequest> &request);

    // Receipt-bearing completion: keeps the physical-retirement proof until the
    // runtime exit observation succeeds, then copies it into the response.
    void StopInstanceOutcomeComplete(const litebus::AID &from, const litebus::Future<StopInstanceOutcome> &result,
                                     const std::shared_ptr<messages::StopInstanceRequest> &request);

    void SendStopInstanceResponse(const litebus::AID &from, const std::string &runtimeID,
                                  const std::shared_ptr<messages::StopInstanceResponse> &response);

    // Fault-test helper (RetireFaultGate): end of the bounded stop-point on
    // the ORIGINAL retirement receipt — Drop only for a timely exact
    // confirmation, otherwise send the original message exactly once.
    void ResolveHeldRetireReceipt(const std::shared_ptr<litebus::AID> &from,
                                  const std::shared_ptr<std::string> &payload,
                                  const std::string &sandboxID, const std::string &generation,
                                  const std::string &requestID, const std::string &runtimeID);

    void OnGetRuntimeStatus(const litebus::AID &from, const std::shared_ptr<messages::StopInstanceRequest> &request,
                            const std::shared_ptr<messages::StopInstanceResponse> &response,
                            const litebus::Future<Status> &instanceStatus);

    Status QueryInstanceStatusInfoResponse(const litebus::AID &from, const std::string &requestID,
                                           const messages::InstanceStatusInfo &info);

    Status QueryDebugInstanceInfosResponse(const litebus::AID &from,
                                           const messages::QueryDebugInstanceInfosResponse &response);

    Status SnapshotRuntimeResponse(const litebus::AID &from,
                                   const std::shared_ptr<messages::SnapshotRuntimeRequest> &request,
                                   const litebus::Future<messages::SnapshotRuntimeResponse> &responseFuture);

    void CreateInstanceMetrics(const litebus::Future<messages::StartInstanceResponse> &response,
                               const std::shared_ptr<messages::StartInstanceRequest> &request);

    void CheckHealthForRuntime(const litebus::Future<messages::StartInstanceResponse> &response,
                               const std::shared_ptr<messages::StartInstanceRequest> &request);

    litebus::Future<messages::StartInstanceResponse> CreateDebugServer(
        const litebus::Future<messages::StartInstanceResponse> &response,
        const std::shared_ptr<messages::StartInstanceRequest> &request);

    litebus::Future<messages::StartInstanceResponse> DebugServerAddRecord(
        const litebus::Future<messages::StartInstanceResponse> &response,
        const std::shared_ptr<messages::StartInstanceRequest> &request);

    litebus::Future<messages::StartInstanceResponse> ExecutorStartInstance(
        const litebus::Future<messages::StartInstanceResponse> &response,
        const std::shared_ptr<ExecutorProxy> &executor, const std::shared_ptr<messages::StartInstanceRequest> &request,
        const std::vector<int> &cardIDs);

    litebus::Future<messages::StartInstanceResponse> TryAcquireLogPrefix(
        const litebus::Future<messages::StartInstanceResponse> &response,
        const std::shared_ptr<messages::StartInstanceRequest> &request);

    void DestroyDebugServer(const litebus::Future<Status> &status,
                            const std::shared_ptr<messages::StopInstanceRequest> &request);

    void DeleteInstanceMetrics(const litebus::Future<Status> &status,
                               const std::shared_ptr<messages::StopInstanceRequest> &request);

    void ReleasePort(const litebus::Future<Status> &status,
                     const std::shared_ptr<messages::StopInstanceRequest> &request);

    void ReceiveRegistered(const std::string &message);

    void RegisterTimeout();

    Status StartRegister(const std::map<std::string, messages::RuntimeInstanceInfo> &runtimeInfos,
                         const std::shared_ptr<messages::RegisterRuntimeManagerRequest> &request);

    void CommitSuicide() const;

    void ClearRuntimeManagerCapability(const std::shared_ptr<ExecutorProxy> &executorProxy,
                                       const litebus::Future<messages::StartInstanceResponse> &response,
                                       const std::shared_ptr<messages::StartInstanceRequest> &request) const
    {
        if (response.IsError() || response.Get().code() != static_cast<int32_t>(SUCCESS)) {
            YRLOG_ERROR("{}|{}|failed to start instance, no need to clear capability.",
                        request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid());
            return;
        }

        if (request->scheduleoption().schedpolicyname() == MONOPOLY_SCHEDULE) {
            executorProxy->ClearCapability();
        }
    }

    void UpdateCredResponse(const litebus::AID &to, const litebus::Future<messages::UpdateCredResponse> &rsp);
    void UpdateNetworkPolicyResponse(
        const litebus::AID &to,
        const std::string &requestID,
        const litebus::Future<messages::UpdateNetworkPolicyResponse> &rsp);

    void ReconcileRuntimes(const litebus::AID &from, std::string &&name, std::string &&msg);

    bool CheckStartInstanceRequest(const messages::RuntimeInstanceInfo &instance);

    bool CheckInstanceIsDeployed(const litebus::AID &to, const messages::RuntimeInstanceInfo &instance);

    void SetRegisterInterval(uint64_t interval)
    {
        registerHelper_->SetRegisterInterval(interval);
    }

    bool ShouldSkipDebugServerCreation(const litebus::Future<messages::StartInstanceResponse> &response,
                                       const std::shared_ptr<messages::StartInstanceRequest> &request) const;
};
}  // namespace functionsystem::runtime_manager

#endif  // RUNTIME_MANAGER_MANAGER_RUNTIME_MANAGE_H
