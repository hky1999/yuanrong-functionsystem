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

#include "runtime_manager.h"

#include "async/async.hpp"
#include "async/defer.hpp"
#include "async/future.hpp"
#include "common/constants/actor_name.h"
#include "common/constants/constants.h"
#include "common/datasystem_capability.h"
#include "common/logs/logging.h"
#include "common/proto/pb/message_pb.h"
#include "common/status/status.h"
#include "common/utils/resume_identity.h"
#include "common/utils/exec_utils.h"
#include "common/utils/struct_transfer.h"
#include "executor/conch_executor.h"
#include "executor/docker_executor.h"
#include "executor/runtime_executor.h"
#include "executor/sandboxd/sandboxd_executor.h"
#include "executor/sandboxd/retire_fault_gate.h"
#include "executor/supervisor_executor.h"
#include "port/port_manager.h"
#include "runtime_manager/executor/executor.h"

namespace functionsystem::runtime_manager {
const uint32_t HALF = 2;
const uint32_t MAX_REGISTER_RETRY_TIMES = 30;

namespace {
struct RuntimeManagerResumeIdentity {
    enum class Trust {
        ORDINARY,
        TRUSTED,
        REUSABLE,
        INVALID,
    };

    Trust trust{Trust::ORDINARY};
    bool trusted{false};
    bool reusable{false};
    std::string tenantID;
    std::string logicalRequestID;
    std::string targetAgentID;
    int64_t expectedVersion{0};
    int64_t protocolVersion{0};
};

RuntimeManagerResumeIdentity ConsumeRuntimeManagerResumeIdentity(messages::StartInstanceRequest &request,
                                                                 bool trustedSender)
{
    RuntimeManagerResumeIdentity identity;
    auto *extensions = request.mutable_scheduleoption()->mutable_extension();
    std::string marker;
    bool hasReservedResumeIdentity = false;
    bool onlyRuntimeManagerBoundaryKeys = true;
    bool parsedExpectedVersion = false;
    bool parsedProtocolVersion = false;
    for (const auto &[key, value] : *extensions) {
        if (!resume_identity::IsResumeProtocolExtension(key)) {
            continue;
        }
        hasReservedResumeIdentity = true;
        if (key == resume_identity::LOGICAL_REQUEST_EXTENSION) {
            identity.logicalRequestID = value;
        } else if (key == resume_identity::EXPECTED_VERSION_EXTENSION) {
            parsedExpectedVersion =
                resume_identity::ParsePositiveInt64(value, &identity.expectedVersion);
        } else if (key == resume_identity::TARGET_AGENT_EXTENSION) {
            identity.targetAgentID = value;
        } else if (key == resume_identity::PROTOCOL_VERSION_EXTENSION) {
            parsedProtocolVersion =
                resume_identity::ParsePositiveInt64(value, &identity.protocolVersion);
        } else if (key == resume_identity::RUNTIME_MANAGER_MARKER_EXTENSION) {
            marker = value;
        } else {
            onlyRuntimeManagerBoundaryKeys = false;
        }
    }
    const auto &instance = request.runtimeinstanceinfo();
    const auto tenant = instance.runtimeconfig().posixenvs().find(YR_TENANT_ID);
    if (tenant != instance.runtimeconfig().posixenvs().end()) {
        identity.tenantID = tenant->second;
    }
    identity.trusted = trustedSender && onlyRuntimeManagerBoundaryKeys
        && parsedExpectedVersion && parsedProtocolVersion && instance.has_snapshotinfo()
        && resume_identity::ValidateBoundaryIdentity(instance.instanceid(), identity.logicalRequestID,
                                                     identity.tenantID, instance.requestid(),
                                                     instance.snapshotinfo(), identity.expectedVersion,
                                                     identity.targetAgentID, identity.protocolVersion, marker);
    identity.reusable = trustedSender && !hasReservedResumeIdentity
        && !instance.has_snapshotinfo() && instance.has_reusablesnapshotrestore()
        && resume_identity::ValidateReusableSnapshotRestore(instance.reusablesnapshotrestore());
    identity.trust = identity.trusted ? RuntimeManagerResumeIdentity::Trust::TRUSTED
        : (identity.reusable ? RuntimeManagerResumeIdentity::Trust::REUSABLE
            : ((hasReservedResumeIdentity || instance.has_reusablesnapshotrestore())
                ? RuntimeManagerResumeIdentity::Trust::INVALID
                : RuntimeManagerResumeIdentity::Trust::ORDINARY));
    resume_identity::StripReservedExtensions(extensions);
    return identity;
}
}

std::string RuntimeManager::RuntimeIDForStart(const messages::RuntimeInstanceInfo &instance,
                                              bool deterministicAttempt)
{
    return deterministicAttempt
        ? resume_identity::RuntimeID(instance.instanceid(), instance.requestid())
        : GenerateRuntimeID(instance);
}

// Container workloads are served exclusively by sandboxd. CONTAINER remains a
// wire-level compatibility value in existing requests and persisted responses,
// but it is normalized to SANDBOXD before executor lookup.
EXECUTOR_TYPE ContainerExecutorType()
{
    return EXECUTOR_TYPE::SANDBOXD;
}

EXECUTOR_TYPE NormalizeExecutorType(EXECUTOR_TYPE type)
{
    return type == EXECUTOR_TYPE::CONTAINER ? ContainerExecutorType() : type;
}
RuntimeManager::RuntimeManager(const std::string &name, bool logReuse) : ActorBase(name), logReuse_(logReuse)
{
}

void RuntimeManager::Init()
{
    YRLOG_INFO("init RuntimeManagerActor {}", ActorBase::GetAID().Name());
    ActorBase::Receive("StartInstance", &RuntimeManager::StartInstance);
    ActorBase::Receive("StopInstance", &RuntimeManager::StopInstance);
    ActorBase::Receive("SnapshotRuntime", &RuntimeManager::SnapshotRuntime);
    ActorBase::Receive("SnapshotRuntimeIfGeneration", &RuntimeManager::SnapshotRuntimeIfGeneration);
    ActorBase::Receive("ResizeSandbox", &RuntimeManager::ResizeSandbox);
    ActorBase::Receive("QueryInstanceStatusInfo", &RuntimeManager::QueryInstanceStatusInfo);
    ActorBase::Receive("CleanStatus", &RuntimeManager::CleanStatus);
    ActorBase::Receive("UpdateCred", &RuntimeManager::UpdateCred);
    ActorBase::Receive("UpdateNetworkPolicy", &RuntimeManager::UpdateNetworkPolicy);
    ActorBase::Receive("QueryDebugInstanceInfos", &RuntimeManager::QueryDebugInstanceInfos);
    ActorBase::Receive("ReconcileRuntimes", &RuntimeManager::ReconcileRuntimes);
    metricsClient_ = std::make_shared<MetricsClient>();
    healthCheckClient_ = std::make_shared<HealthCheck>();
    auto logManagerActor =
        std::make_shared<LogManagerActor>(RUNTIME_MANAGER_LOG_MANAGER_ACTOR_NAME, GetAID(), logReuse_);
    logManagerClient_ = std::make_shared<LogManager>(logManagerActor);
    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    runtimeManagerID_ = uuid.ToString();
}

void RuntimeManager::Finalize()
{
    ASSERT_IF_NULL(logManagerClient_);
    logManagerClient_->StopScanLogs();
    for (const auto &iter : executorMap_) {
        iter.second->Stop();
    }
    healthCheckClient_.reset();
    ActorBase::Finalize();
    PortManager::GetInstance().Clear();
}

litebus::Future<bool> RuntimeManager::GracefulShutdown()
{
    litebus::Promise<bool> p;
    for (const auto &iter : executorMap_) {
        YRLOG_INFO("runtimeManager graceful shutdown, terminate executor name: {}", iter.second->GetName());
        p.Associate(iter.second->GracefulShutdown());
    }
    if (runtimeInstanceDebugEnable_) {
        ASSERT_IF_NULL(debugServerMgr_);
        debugServerMgr_->DestroyAllServers();
    }

    if (executorMap_.empty()) {
        p.SetValue(true);
    }
    return p.GetFuture().OnComplete(litebus::Defer(GetAID(), &RuntimeManager::OnExecuterShutdown));
}

void RuntimeManager::OnExecuterShutdown()
{
    YRLOG_INFO("send GracefulShutdownFinish to agent: {}", functionAgentAID_.HashString());
    Send(functionAgentAID_, "GracefulShutdownFinish", "");
}

void RuntimeManager::StartInstance(const litebus::AID &from, std::string && /* name */, std::string &&msg)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    if (msg.empty() || !request->ParseFromString(msg)) {
        YRLOG_ERROR("failed to start instance, message({}) from({}) is invalid.", msg, from.HashString());
        return;
    }
    const bool trustedSender = functionAgentAID_.OK()
        && from.HashString() == functionAgentAID_.HashString();
    const auto resumeIdentity = ConsumeRuntimeManagerResumeIdentity(*request, trustedSender);
    const auto &instance = request->runtimeinstanceinfo();
    if (resumeIdentity.trust == RuntimeManagerResumeIdentity::Trust::INVALID) {
        messages::StartInstanceResponse response;
        response.set_requestid(instance.requestid());
        response.set_code(static_cast<int32_t>(RUNTIME_MANAGER_PARAMS_INVALID));
        response.set_message("trusted resume identity is invalid");
        Send(from, "StartInstanceResponse", response.SerializeAsString());
        return;
    }
    if (!CheckStartInstanceRequest(instance)) {
        return;
    }
    const auto capability = datasystem_capability::ResolveCapability(instance.runtimeconfig().posixenvs());
    if (!capability.IsValid()) {
        StartInstanceCapabilityInvalid(from, request);
        return;
    }
    (void)receivedStartingReq_.insert(instance.requestid());
    if (!resumeIdentity.trusted && CheckInstanceIsDeployed(from, instance)) {
        return;
    }
    latestInFlightStartRequestByInstance_[instance.instanceid()] = instance.requestid();
    // CONTAINER is retained on the wire for compatibility and normalized to the
    // only supported container backend, SANDBOXD.
    auto type = static_cast<EXECUTOR_TYPE>(request->type());
    type = NormalizeExecutorType(type);
    auto executor = FindExecutor(type);
    if (executor == nullptr) {
        StartInstanceExecutorUnavailable(from, request, type);
        return;
    }
    if (resumeIdentity.trusted) {
        (void)instanceResponseMap_.erase(instance.instanceid());
    }
    auto *extensions = request->mutable_scheduleoption()->mutable_extension();
    resume_identity::StripReservedExtensions(extensions);
    std::string runtimeID = RuntimeIDForStart(
        instance, resumeIdentity.trusted || resumeIdentity.reusable);
    request->mutable_runtimeinstanceinfo()->set_runtimeid(runtimeID);
    if (resumeIdentity.trusted) {
        (*extensions)[resume_identity::LOGICAL_REQUEST_EXTENSION] = resumeIdentity.logicalRequestID;
        (*extensions)[resume_identity::EXPECTED_VERSION_EXTENSION] =
            std::to_string(resumeIdentity.expectedVersion);
        (*extensions)[resume_identity::TARGET_AGENT_EXTENSION] = resumeIdentity.targetAgentID;
        (*extensions)[resume_identity::PROTOCOL_VERSION_EXTENSION] =
            std::to_string(resumeIdentity.protocolVersion);
        (*extensions)[resume_identity::EXECUTOR_MARKER_EXTENSION] = resume_identity::ExecutorMarker(
            instance.instanceid(), resumeIdentity.logicalRequestID, resumeIdentity.tenantID,
            instance.requestid(), instance.snapshotinfo(), resumeIdentity.expectedVersion,
            resumeIdentity.targetAgentID, resumeIdentity.protocolVersion);
    }
    YRLOG_INFO("{}|{}|begin to start runtime({}) for instance({}).", instance.traceid(), instance.requestid(),
               instance.runtimeid(), instance.instanceid());
    auto vecs = metricsClient_->GetCardIDs();
    messages::StartInstanceResponse response;
    response.mutable_startruntimeinstanceresponse()->set_runtimeid(runtimeID);
    response.mutable_startruntimeinstanceresponse()->set_executortype(static_cast<int32_t>(type));

    // start debug server ahead of runtime
    CreateDebugServer(response, request)
        .Then(litebus::Defer(GetAID(), &RuntimeManager::TryAcquireLogPrefix, std::placeholders::_1, request))
        .Then(litebus::Defer(GetAID(), &RuntimeManager::ExecutorStartInstance, std::placeholders::_1, executor, request,
                             vecs))
        .OnComplete(
            litebus::Defer(this->GetAID(), &RuntimeManager::DebugServerAddRecord, std::placeholders::_1, request))
        .OnComplete(
            litebus::Defer(this->GetAID(), &RuntimeManager::CreateInstanceMetrics, std::placeholders::_1, request))
        .OnComplete(
            litebus::Defer(this->GetAID(), &RuntimeManager::CheckHealthForRuntime, std::placeholders::_1, request))
        .OnComplete(litebus::Defer(this->GetAID(), &RuntimeManager::StartInstanceResponse, from,
                                   request->runtimeinstanceinfo().instanceid(),
                                   request->runtimeinstanceinfo().requestid(), std::placeholders::_1));
}

void RuntimeManager::StartInstanceCapabilityInvalid(
    const litebus::AID &from, const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    const auto &instance = request->runtimeinstanceinfo();
    constexpr char message[] = "YR_DATASYSTEM_DEPLOYED=false requires YR_BYPASS_DATASYSTEM=true";
    YRLOG_ERROR("{}|{}|failed to start instance({}): {}", instance.traceid(), instance.requestid(),
                instance.instanceid(), message);
    messages::StartInstanceResponse response;
    response.set_requestid(instance.requestid());
    response.set_code(static_cast<int32_t>(RUNTIME_MANAGER_PARAMS_INVALID));
    response.set_message(message);
    litebus::Future<messages::StartInstanceResponse> promise;
    promise.SetValue(response);
    litebus::Async(GetAID(), &RuntimeManager::StartInstanceResponse, from, instance.instanceid(),
                   instance.requestid(), promise);
}

void RuntimeManager::StartInstanceExecutorUnavailable(const litebus::AID &from,
    const std::shared_ptr<messages::StartInstanceRequest> &request,
    EXECUTOR_TYPE type)
{
    const auto &instance = request->runtimeinstanceinfo();
    YRLOG_ERROR("{}|{}|the type({}) is not supported to start runtime for instance({}).", instance.traceid(),
                instance.requestid(), request->type(), instance.instanceid());
    messages::StartInstanceResponse response;
    response.set_requestid(instance.requestid());
    response.set_code(static_cast<int32_t>(RUNTIME_MANAGER_PARAMS_INVALID));
    response.set_message(GetExecutorUnavailableMessage(type));
    litebus::Future<messages::StartInstanceResponse> promise;
    promise.SetValue(response);
    litebus::Async(GetAID(), &RuntimeManager::StartInstanceResponse, from, instance.instanceid(),
                   instance.requestid(), promise);
}

litebus::Future<messages::StartInstanceResponse> RuntimeManager::ExecutorStartInstance(
    const litebus::Future<messages::StartInstanceResponse> &response, const std::shared_ptr<ExecutorProxy> &executor,
    const std::shared_ptr<messages::StartInstanceRequest> &request, const std::vector<int> &cardIDs)
{
    if (response.IsError()) {
        YRLOG_ERROR("{}|{}|failed to start instance, do not execute, instanceID: {}, runtimeID: {}",
                    request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(),
                    request->runtimeinstanceinfo().instanceid(), request->runtimeinstanceinfo().runtimeid());
        return response;
    }
    if (response.Get().code() != static_cast<int32_t>(SUCCESS)) {
        YRLOG_ERROR("{}|{}|failed to start instance, do not execute, instanceID: {}, runtimeID: {}",
                    request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(),
                    request->runtimeinstanceinfo().instanceid(), request->runtimeinstanceinfo().runtimeid());
        return response;
    }

    return executor->StartInstance(request, cardIDs);
}

litebus::Future<messages::StartInstanceResponse> RuntimeManager::TryAcquireLogPrefix(
    const litebus::Future<messages::StartInstanceResponse> &response,
    const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    if (logReuse_) {
        return logManagerClient_->AcquireLogPrefix(request->runtimeinstanceinfo().runtimeid())
            .Then([response, request](const std::string &logPrefix) {
                request->set_logprefix(logPrefix);
                return response;
            });
    }

    return response;
}

bool RuntimeManager::CheckInstanceIsDeployed(const litebus::AID &to, const messages::RuntimeInstanceInfo &instance)
{
    if (auto iter(instanceResponseMap_.find(instance.instanceid())); iter != instanceResponseMap_.end()) {
        auto reqID = iter->second.requestid();
        if (reqID == instance.requestid()) {
            YRLOG_INFO("{}|{}|instance({}) has been deployed", instance.traceid(), instance.requestid(),
                       instance.instanceid());
            auto output(iter->second);
            output.set_code(static_cast<int32_t>(RUNTIME_MANAGER_INSTANCE_HAS_BEEN_DEPLOYED));
            (void)Send(to, "StartInstanceResponse", output.SerializeAsString());
        } else {
            YRLOG_WARN("{}|{}|instance({}) exist, but not the same one({})", instance.traceid(), instance.requestid(),
                       instance.instanceid(), reqID);
            messages::StartInstanceResponse response;
            response.set_requestid(instance.requestid());
            response.set_code(static_cast<int32_t>(RUNTIME_MANAGER_INSTANCE_EXIST));
            response.set_message("instance exist but not the same one");
            (void)Send(to, "StartInstanceResponse", response.SerializeAsString());
        }
        (void)receivedStartingReq_.erase(instance.requestid());
        return true;
    }
    return false;
}

bool RuntimeManager::CheckStartInstanceRequest(const messages::RuntimeInstanceInfo &instance)
{
    if (!connected_) {
        YRLOG_ERROR(
            "{}|{}|runtimeManager registration to functionAgent is not complete, "
            "ignore start instance request, instanceID {}.",
            instance.traceid(), instance.requestid(), instance.instanceid());
        return false;
    }
    if (receivedStartingReq_.find(instance.requestid()) != receivedStartingReq_.end()) {
        YRLOG_WARN("{}|{}|received repeated request ignore it", instance.traceid(), instance.requestid());
        return false;
    }
    return true;
}

void RuntimeManager::InnerOomKillInstance(const litebus::Future<Status> &status, const std::string &instanceID,
                                          const std::string &runtimeID, const std::string &requestID)
{
    if (status.IsError() || status.Get().IsError()) {
        YRLOG_ERROR("{}|{}|status get error, can not inner OOM kill instance, runtimeID: {}", requestID, instanceID,
                    runtimeID);
        return;
    }

    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid(runtimeID);
    request->set_requestid(requestID);
    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    request->set_traceid("trace-OOM-Kill_" + runtimeID + "_" + uuid.ToString());
    request->set_type(static_cast<int32_t>(GetRuntimeType(runtimeID)));

    auto executor = FindExecutor(EXECUTOR_TYPE(request->type()));
    YRLOG_INFO("{}|{}|begin to oom kill runtime({}).", request->traceid(), requestID, runtimeID);
    executor->GetExecByRuntimeID(runtimeID)
        .Then([healthCheckClient(healthCheckClient_)](const std::shared_ptr<litebus::Exec> &exePtr) {
            return healthCheckClient->StopHeathCheckByPID(exePtr);
        })
        .Then([executor, request]() { return executor->StopInstance(request, true); })
        .OnComplete(litebus::Defer(GetAID(), &RuntimeManager::DeleteOomNotifyData, std::placeholders::_1, request))
        .OnComplete(litebus::Defer(GetAID(), &RuntimeManager::DeleteInstanceMetrics, std::placeholders::_1, request))
        .OnComplete(litebus::Defer(GetAID(), &RuntimeManager::DestroyDebugServer, std::placeholders::_1, request))
        .OnComplete(litebus::Defer(GetAID(), &RuntimeManager::ReleasePort, std::placeholders::_1, request));
}

void RuntimeManager::DeleteOomNotifyData(const litebus::Future<Status> &status,
                                         const std::shared_ptr<messages::StopInstanceRequest> &request)
{
    if (status.IsError() || status.Get().IsError()) {
        YRLOG_ERROR("{}|{}|can not delete inner OOM notify data, stop instance failed, runtimeID({})",
                    request->traceid(), request->requestid(), request->runtimeid());
        return;
    }
    healthCheckClient_->DeleteOomNotifyData(request->requestid());
}

void RuntimeManager::OomKillInstance(const std::string &instanceID, const std::string &runtimeID,
                                     const std::string &requestID)
{
    YRLOG_DEBUG("{}|received event OOM Kill instanceID({}) runtimeID({})", requestID, instanceID, runtimeID);
    healthCheckClient_->NotifyOomKillInstanceInAdvance(requestID, instanceID, runtimeID)
        .OnComplete(litebus::Defer(GetAID(), &RuntimeManager::InnerOomKillInstance, std::placeholders::_1, instanceID,
                                   runtimeID, requestID));
}

void RuntimeManager::StopInstance(const litebus::AID &from, std::string && /* name */, std::string &&msg)
{
    auto request = std::make_shared<messages::StopInstanceRequest>();
    if (msg.empty() || !request->ParseFromString(msg)) {
        YRLOG_ERROR("failed to stop instance, message({}) from({}) is invalid.", msg, from.HashString());
        return;
    }
    if (!connected_) {
        YRLOG_ERROR(
            "{}|{}|runtimeManager registration to functionAgent is not complete, "
            "ignore stop instance request for runtime({}).",
            request->traceid(), request->requestid(), request->runtimeid());
        return;
    }
    const auto executorType = ResolveStopExecutorType(request);
    auto executor = FindExecutor(executorType);
    if (executor == nullptr) {
        YRLOG_ERROR("{}|{}|the type({}) is not supported to stop runtime({})", request->traceid(), request->requestid(),
                    static_cast<int32_t>(executorType), request->runtimeid());
        messages::StopInstanceResponse response;
        response.set_requestid(request->requestid());
        response.set_runtimeid(request->runtimeid());
        response.set_code(static_cast<int32_t>(RUNTIME_MANAGER_PARAMS_INVALID));
        response.set_message("unknown instance type, cannot stop instance");
        Send(from, "StopInstanceResponse", response.SerializeAsString());
        return;
    }

    YRLOG_INFO("{}|{}|begin to stop runtime({}).", request->traceid(), request->requestid(), request->runtimeid());
    executor->GetExecByRuntimeID(request->runtimeid())
        .Then([healthCheckClient(healthCheckClient_)](const std::shared_ptr<litebus::Exec> &exePtr) {
            // A replayed retirement carries no exec (state already unregistered);
            // the null stop must not block the durable-receipt lookup below.
            return healthCheckClient->StopHeathCheckByPID(exePtr);
        })
        .Then([executor, request]() { return executor->StopInstanceWithOutcome(request, false); })
        .OnComplete(litebus::Defer(GetAID(), &RuntimeManager::StopInstanceOutcomeComplete, from,
                                   std::placeholders::_1, request));
}

void RuntimeManager::StopInstanceOutcomeComplete(const litebus::AID &from,
                                                 const litebus::Future<StopInstanceOutcome> &result,
                                                 const std::shared_ptr<messages::StopInstanceRequest> &request)
{
    auto response = std::make_shared<messages::StopInstanceResponse>();
    response->set_runtimeid(request->runtimeid());
    response->set_requestid(request->requestid());
    response->set_traceid(request->traceid());
    response->set_instanceid(request->instanceid());
    if (result.IsError() || result.Get().status.IsError()) {
        const auto code = result.IsError()
            ? static_cast<int32_t>(RUNTIME_MANAGER_STOP_INSTANCE_FAILED)
            : static_cast<int32_t>(result.Get().status.StatusCode());
        YRLOG_ERROR("{}|{}|failed to stop runtime({}).", request->traceid(), request->requestid(),
                    request->runtimeid());
        response->set_code(code);
        response->set_message("stop instance failed");
        SendStopInstanceResponse(from, request->runtimeid(), response);
        return;
    }
    const auto &proof = result.Get().retirement;
    if (proof.confirmed) {
        // The receipt must name exactly the incarnation the scheduler asked to
        // retire; anything else is fail-closed, never a proof.
        if (proof.sandboxID != request->expected_sandbox_id()
            || proof.resourceGeneration != request->expected_resource_generation()) {
            YRLOG_ERROR("{}|{}|retirement receipt identity mismatch for runtime({}).", request->traceid(),
                        request->requestid(), request->runtimeid());
            response->set_code(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY));
            response->set_message("retirement receipt identity mismatch");
            SendStopInstanceResponse(from, request->runtimeid(), response);
            return;
        }
        response->set_physical_retirement_confirmed(true);
        response->set_retired_sandbox_id(proof.sandboxID);
        response->set_retired_resource_generation(proof.resourceGeneration);
    }
    healthCheckClient_->GetRuntimeStatus(request->runtimeid())
        .OnComplete(litebus::Defer(GetAID(), &RuntimeManager::OnGetRuntimeStatus, from, request, response,
                                   std::placeholders::_1));
}

void RuntimeManager::ResizeSandbox(const litebus::AID &from, std::string &&, std::string &&msg)
{
    // This is a control-plane command, never a sandbox-selected network route.
    if (functionAgentAID_.Name().empty() || from != functionAgentAID_) {
        YRLOG_WARN("reject ResizeSandbox from unregistered function-agent {}", from.HashString());
        return;
    }
    auto request = std::make_shared<messages::ResizeSandboxRequest>();
    if (msg.size() > 64 * 1024 || !request->ParseFromString(msg)) {
        YRLOG_WARN("invalid ResizeSandbox request from registered function-agent");
        return;
    }
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    auto executor = std::dynamic_pointer_cast<SandboxdExecutorProxy>(FindExecutor(EXECUTOR_TYPE::SANDBOXD));
    if (executor == nullptr || request->resize().deadline_unix_ms() <= nowMs
        || request->resize().deadline_unix_ms() > nowMs + 60000) {
        messages::ResizeSandboxResponse response;
        response.set_requestid(request->requestid());
        response.set_instanceid(request->instanceid());
        response.set_runtimeid(request->runtimeid());
        response.set_sandboxid(request->resize().sandbox_id());
        response.set_resourcegeneration(request->resize().resource_generation());
        response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
        response.set_message("sandboxd executor or command deadline unavailable before dispatch");
        response.set_outcome(messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH);
        Send(from, "ResizeSandboxResponse", response.SerializeAsString());
        return;
    }
    executor->ResizeSandbox(request).OnComplete(
        [aid(GetAID()), from, request](const litebus::Future<messages::ResizeSandboxResponse> &result) {
            litebus::Async(aid, &RuntimeManager::OnResizeSandboxComplete, from, request, result);
        });
}

void RuntimeManager::OnResizeSandboxComplete(const litebus::AID &from,
    const std::shared_ptr<messages::ResizeSandboxRequest> &request,
    const litebus::Future<messages::ResizeSandboxResponse> &result)
{
    YRLOG_INFO("{}|resize relay(rm) complete ok={} outcome={}", request->requestid(), result.IsOK(),
               result.IsOK() ? static_cast<int>(result.Get().outcome()) : -1);
    messages::ResizeSandboxResponse response;
    if (result.IsOK()) {
        response = result.Get();
    } else {
        response.set_requestid(request->requestid());
        response.set_instanceid(request->instanceid());
        response.set_runtimeid(request->runtimeid());
        response.set_sandboxid(request->resize().sandbox_id());
        response.set_resourcegeneration(request->resize().resource_generation());
        response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
        response.set_message("resize executor completion failed; reconcile before releasing budget");
        response.set_outcome(messages::ResizeSandboxResponse::UNKNOWN);
    }
    Send(from, "ResizeSandboxResponse", response.SerializeAsString());
}

void RuntimeManager::SnapshotRuntime(const litebus::AID &from, std::string &&, std::string &&msg)
{
    auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
    if (!request->ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse SnapshotRuntimeRequest");
        return;
    }

    const std::string &instanceID = request->instanceid();
    const std::string &runtimeID = request->runtimeid();
    YRLOG_INFO("{}|received SnapshotRuntime request for instance({}), runtime({})", request->requestid(), instanceID,
               runtimeID);

    auto executorType = EXECUTOR_TYPE::UNKNOWN;
    const auto resolveStatus = ResolveSnapshotExecutorType(*request, executorType);
    if (resolveStatus.IsError()) {
        YRLOG_ERROR("{}|failed to resolve snapshot executor for runtime({}): {}", request->requestid(),
                    runtimeID, resolveStatus.ToString());
        messages::SnapshotRuntimeResponse response;
        response.set_requestid(request->requestid());
        response.set_checkpointnotstarted(true);
        response.set_code(static_cast<int32_t>(resolveStatus.StatusCode()));
        response.set_message(resolveStatus.RawMessage());
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }
    auto executor = FindExecutor(executorType);
    if (executor == nullptr) {
        YRLOG_ERROR("{}|container executor not found", request->requestid());
        messages::SnapshotRuntimeResponse response;
        response.set_requestid(request->requestid());
        response.set_checkpointnotstarted(true);
        response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
        response.set_message("container executor not found");
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }

    executor->SnapshotRuntime(request).OnComplete(litebus::Defer(
        GetAID(), &RuntimeManager::SnapshotRuntimeResponse, from, request, std::placeholders::_1));
}

Status RuntimeManager::ResolveSnapshotExecutorType(const messages::SnapshotRuntimeRequest &request,
                                                   EXECUTOR_TYPE &executorType)
{
    if (instanceInfoMap_.find(request.runtimeid()) == instanceInfoMap_.end()) {
        return Status(StatusCode::RUNTIME_MANAGER_RUNTIME_PROCESS_NOT_FOUND,
                      "snapshot runtime is not registered in RuntimeManager");
    }

    const auto authoritativeType = GetRuntimeType(request.runtimeid());
    if (authoritativeType == EXECUTOR_TYPE::UNKNOWN) {
        return Status(StatusCode::RUNTIME_MANAGER_PARAMS_INVALID,
                      "snapshot runtime has an unknown executor type");
    }
    if (request.type() == common::PAUSE_RESUME
        && authoritativeType != EXECUTOR_TYPE::SANDBOXD) {
        return Status(StatusCode::RUNTIME_MANAGER_PARAMS_INVALID,
                      "pause/resume snapshot requires a sandboxd runtime");
    }

    executorType = authoritativeType;
    return Status::OK();
}

void RuntimeManager::SnapshotRuntimeIfGeneration(const litebus::AID &from, std::string &&, std::string &&msg)
{
    // Generation-fenced snapshot execution on a channel separate from the
    // legacy SnapshotRuntime handler; replies use only the new response name.
    if (functionAgentAID_.Name().empty() || from != functionAgentAID_) {
        YRLOG_WARN("reject SnapshotRuntimeIfGeneration from unregistered function-agent {}", from.HashString());
        return;
    }
    auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
    if (msg.size() > 64 * 1024 || !request->ParseFromString(msg)) {
        YRLOG_WARN("invalid SnapshotRuntimeIfGeneration request from registered function-agent");
        return;
    }
    // Only SandboxdExecutorProxy interprets the expected identity pair; another
    // executor would snapshot without the fence and report hollow success.
    auto executor = std::dynamic_pointer_cast<SandboxdExecutorProxy>(FindExecutor(EXECUTOR_TYPE::SANDBOXD));
    const auto validID = [](const std::string &id) {
        return !id.empty() && id.size() <= 256 && id.find_first_not_of(" \t\n\r\v\f") != std::string::npos;
    };
    if (executor == nullptr || !validID(request->requestid()) || !validID(request->instanceid())
        || !validID(request->runtimeid()) || !validID(request->expected_sandbox_id())
        || !validID(request->expected_resource_generation())) {
        messages::SnapshotRuntimeResponse response;
        response.set_requestid(request->requestid());
        response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
        response.set_message("sandboxd executor or complete expected identity unavailable before dispatch");
        Send(from, "SnapshotRuntimeIfGenerationResponse", response.SerializeAsString());
        return;
    }
    YRLOG_INFO("{}|begin generation-fenced snapshot for instance({}), runtime({}), expected sandbox({})",
               request->requestid(), request->instanceid(), request->runtimeid(), request->expected_sandbox_id());
    executor->SnapshotRuntime(request).OnComplete(
        [aid(GetAID()), from, request](const litebus::Future<messages::SnapshotRuntimeResponse> &result) {
            litebus::Async(aid, &RuntimeManager::OnSnapshotRuntimeIfGenerationComplete, from, request, result);
        });
}

void RuntimeManager::OnSnapshotRuntimeIfGenerationComplete(const litebus::AID &from,
    const std::shared_ptr<messages::SnapshotRuntimeRequest> &request,
    const litebus::Future<messages::SnapshotRuntimeResponse> &result)
{
    messages::SnapshotRuntimeResponse response;
    if (result.IsOK()) {
        response = result.Get();
        if (response.requestid() != request->requestid()) {
            response.clear_snapshotinfo();
            response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
            response.set_message("snapshot reply does not match the fenced request; outcome unknown");
        } else if (response.code() == static_cast<int32_t>(StatusCode::SUCCESS)
                   && (!response.has_snapshotinfo()
                       || response.snapshotinfo().source_runtime_id() != request->runtimeid()
                       || response.snapshotinfo().sandbox_id() != request->expected_sandbox_id()
                       || response.snapshotinfo().source_generation() != request->expected_resource_generation()
                       || response.snapshotinfo().checkpointid().empty())) {
            // A checkpoint exists but is not proven to belong to the expected
            // sandbox/generation; report unknown instead of success.
            response.clear_snapshotinfo();
            response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
            response.set_message("snapshot source identity unverified; outcome unknown, reconcile checkpoint state");
        }
    } else {
        // A failed future carries no response payload; never call Get on it.
        response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
        response.set_message("snapshot executor completion failed; outcome unknown, reconcile checkpoint state");
    }
    response.set_requestid(request->requestid());
    Send(from, "SnapshotRuntimeIfGenerationResponse", response.SerializeAsString());
}

void RuntimeManager::HandlePrestartRuntimeExit(const pid_t pid)
{
    auto executor = FindExecutor(EXECUTOR_TYPE(EXECUTOR_TYPE::RUNTIME));
    if (executor == nullptr) {
        return;
    }
    executor->UpdatePrestartRuntimePromise(pid);
}

void RuntimeManager::SetConfig(const Flags &flags)
{
    functionAgentAID_ = litebus::AID(FUNCTION_AGENT_AGENT_SERVICE_ACTOR_NAME, flags.GetAgentAddress());
    checkpointDir_ = flags.GetCheckpointDir();
    // Configure the process-local runtime executor plus the active container
    // executor (SANDBOXD when enabled, else CONTAINER), while preserving
    // explicit supervisor/docker executors used by the master-side branch.
    auto containerType = ContainerExecutorType();
    if (containerType == EXECUTOR_TYPE::SANDBOXD && !initialRegistrationStarted_) {
        sandboxRuntimeDiscoveryState_ = SandboxRuntimeDiscoveryState::PENDING;
    }
    for (auto type : { EXECUTOR_TYPE::RUNTIME, containerType, EXECUTOR_TYPE::SUPERVISOR, EXECUTOR_TYPE::CONCH,
                        EXECUTOR_TYPE::DOCKER }) {
        auto executor = FindExecutor(type);
        YRLOG_INFO("SetRuntimeConfig for type({})", fmt::underlying(type));
        if (executor != nullptr) {
            executor->SetRuntimeConfig(flags);
        }
    }

    RETURN_IF_NULL(metricsClient_);
    if (flags.GetOomKillEnable()) {
        auto runtimeMemoryExceedLimitCallback = std::bind(&RuntimeManager::OomKillInstance, this, std::placeholders::_1,
                                                          std::placeholders::_2, std::placeholders::_3);
        metricsClient_->SetRuntimeMemoryExceedLimitCallback(runtimeMemoryExceedLimitCallback);
        YRLOG_INFO("set OOM Kill callback to metricsClient");
    }
    metricsClient_->SetConfig(flags);

    RETURN_IF_NULL(healthCheckClient_);
    healthCheckClient_->SetConfig(flags);

    RETURN_IF_NULL(logManagerClient_);
    logManagerClient_->SetConfig(flags);

    if (flags.GetRuntimeInstanceDebugEnable()) {
        runtimeInstanceDebugEnable_ = true;
        auto debugServerMgrActor = std::make_shared<DebugServerMgrActor>(RUNTIME_MANAGER_DEBUG_SERVER_MGR_ACTOR_NAME);
        debugServerMgr_ = std::make_shared<DebugServerMgr>(debugServerMgrActor);
        debugServerMgr_->SetConfig(flags);
        debugServerMgr_->SetEnableDebug();
    }

    auto handlePrestartuntimeExit = [aid(GetAID())](const pid_t pid) {
        litebus::Async(aid, &RuntimeManager::HandlePrestartRuntimeExit, pid);
    };
    healthCheckClient_->RegisterProcessExitCallback(handlePrestartuntimeExit);
    if (logReuse_) {
        auto handleLogPrefixExit = [logManagerClient(logManagerClient_)](const std::string runtimeID) {
            YRLOG_INFO("runtime {} exiting abnormally, and release log prefix.", runtimeID);
            logManagerClient->ReleaseLogPrefix(runtimeID);
        };
        healthCheckClient_->RegisterHandleLogPrefixExit(handleLogPrefixExit);
    }
    nodeID_ = flags.GetNodeID();
    pingTimeoutMs_ = flags.GetSystemTimeout() / HALF;
}

void RuntimeManager::CollectCpuType()
{
    auto cpuType = GetCpuTypeByProc();
    if (cpuType.empty()) {
        cpuType = GetCpuTypeByCommand();
    }
    cpuType_ = cpuType;
}

std::string RuntimeManager::GetCpuTypeByProc()
{
    std::ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo.is_open()) {
        std::cerr << "Unable to open /proc/cpuinfo" << std::endl;
        YRLOG_WARN("unable to open /proc/cpuinfo");
        return "";
    }
    std::string line;
    while (std::getline(cpuinfo, line)) {
        if (line.find("model name") != std::string::npos) {
            std::size_t pos = line.find(':');
            if (pos != std::string::npos && pos + 2 < line.size()) { // +2 to skip the colon and the space
                std::string modelName = line.substr(pos + 2);  // +2 to skip the colon and the space
                YRLOG_INFO("CPU Model Name: {}", modelName);
                return modelName;
            }
        }
    }
    cpuinfo.close();
    return "";
}

// exec lscpu command then parse result
std::string RuntimeManager::GetCpuTypeByCommand()
{
    std::string outputStr = ExecuteCommand("lscpu").output;
    for (auto &it : litebus::strings::Split(outputStr, "\n")) {
        if (it.find("Model name") != std::string::npos) {
            auto target = litebus::strings::Split(it, ":");
            if (target.size() > 1) {
                std::string modelName = litebus::strings::Trim(target[1]);
                YRLOG_INFO("CPU Model Name: {}", modelName);
                return modelName;
            }
        }
    }
    return "";
}

std::string RuntimeManager::GetCpuType() const
{
    return cpuType_;
}

std::string RuntimeManager::GetExecutorUnavailableMessage(EXECUTOR_TYPE type)
{
    if (type == EXECUTOR_TYPE::RUNTIME) {
        return "runtime service is not ready, please check whether the service is abnormal";
    }
    if (type == EXECUTOR_TYPE::CONTAINER) {
        return "container service is not ready, please check whether the service is abnormal";
    }
    if (type == EXECUTOR_TYPE::SUPERVISOR) {
        return "supervisor service is not ready, please check whether the service is abnormal";
    }
    if (type == EXECUTOR_TYPE::CONCH) {
        return "conch service is not ready, please check whether jiuwenbox/conchd is abnormal";
    }
    if (type == EXECUTOR_TYPE::DOCKER) {
        return "docker service is not ready, please check whether the Docker daemon is abnormal";
    }
    if (type == EXECUTOR_TYPE::SANDBOXD) {
        return "sandboxd service is not ready, please check whether the service is abnormal";
    }
    return "unknown instance type, cannot start instance";
}

std::shared_ptr<ExecutorProxy> RuntimeManager::FindExecutor(EXECUTOR_TYPE type)
{
    if (type == EXECUTOR_TYPE::CONTAINER) {
        type = ContainerExecutorType();
    }
    if (auto iter(executorMap_.find(type)); iter != executorMap_.end()) {
        return iter->second;
    }
    switch (type) {
        case EXECUTOR_TYPE::RUNTIME:
            return CreateRuntimeExecutor();
        case EXECUTOR_TYPE::SUPERVISOR:
            return CreateSupervisorExecutor();
        case EXECUTOR_TYPE::CONCH:
            return CreateConchExecutor();
        case EXECUTOR_TYPE::DOCKER:
            return CreateDockerExecutor();
        case EXECUTOR_TYPE::SANDBOXD:
            return CreateSandboxdExecutor();
        default:
            return nullptr;
    }
}

std::shared_ptr<ExecutorProxy> RuntimeManager::CreateRuntimeExecutor()
{
    YRLOG_DEBUG("not found a executor, create a runtime executor.");
    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    const std::string name = "RuntimeExecutor_" + uuid.ToString();
    auto executor = std::make_shared<RuntimeExecutor>(name, functionAgentAID_);
    litebus::Spawn(executor, false);
    auto executorProxy = std::make_shared<RuntimeExecutorProxy>(executor);
    (void)executorMap_.insert(std::make_pair(EXECUTOR_TYPE::RUNTIME, executorProxy));
    return executorProxy;
}

std::shared_ptr<ExecutorProxy> RuntimeManager::CreateSupervisorExecutor()
{
    YRLOG_INFO("not found a executor, create a supervisor executor.");
    auto executor = std::make_shared<SupervisorExecutor>("SupervisorExecutor", functionAgentAID_);
    litebus::Spawn(executor, false);
    auto executorProxy = std::make_shared<SupervisorExecutorProxy>(executor);
    (void)executorMap_.insert(std::make_pair(EXECUTOR_TYPE::SUPERVISOR, executorProxy));
    return executorProxy;
}

std::shared_ptr<ExecutorProxy> RuntimeManager::CreateConchExecutor()
{
    YRLOG_INFO("not found a executor, create a conch executor.");
    auto executor = std::make_shared<ConchExecutor>("ConchExecutor", functionAgentAID_);
    litebus::Spawn(executor, false);
    auto executorProxy = std::make_shared<ConchExecutorProxy>(executor);
    (void)executorMap_.insert(std::make_pair(EXECUTOR_TYPE::CONCH, executorProxy));
    return executorProxy;
}

std::shared_ptr<ExecutorProxy> RuntimeManager::CreateDockerExecutor()
{
    auto dockerHost = litebus::os::GetEnv("DOCKER_HOST");
    bool dockerHostValid = dockerHost.IsSome() && !dockerHost.Get().empty();
    if (!dockerHostValid && !litebus::os::ExistPath(DEFAULT_DOCKER_SOCKET)) {
        YRLOG_INFO("docker executor disabled, no Docker daemon endpoint found");
        return nullptr;
    }
    YRLOG_INFO("not found a executor, create a docker executor.");
    auto executor = std::make_shared<DockerExecutor>("DockerExecutor", functionAgentAID_);
    litebus::Spawn(executor, false);
    auto executorProxy = std::make_shared<DockerExecutorProxy>(executor);
    (void)executorMap_.insert(std::make_pair(EXECUTOR_TYPE::DOCKER, executorProxy));
    return executorProxy;
}

std::shared_ptr<ExecutorProxy> RuntimeManager::CreateSandboxdExecutor()
{
    YRLOG_INFO("create a sandboxd executor.");
    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    const std::string name = "RuntimeExecutor_" + uuid.ToString();
    auto availableRuntimesCallback = [aid(GetAID())](bool enabled,
                                         const SandboxdExecutor::AvailableRuntimes &runtimes) {
        litebus::Async(aid, &RuntimeManager::OnInitialAvailableRuntimes, enabled, runtimes);
    };
    auto executor = std::make_shared<SandboxdExecutor>(name, functionAgentAID_, checkpointDir_,
                                                       std::move(availableRuntimesCallback));
    executor->SetHealthCheckClient(healthCheckClient_);
    if (metricsClient_ != nullptr) {
        // D-7: bridge the sandboxd Stats poll into the resource view so
        // per-instance actualuse is populated even though runsc reports pid=0
        auto metricsClient = metricsClient_;
        executor->SetInstanceResourceSampleReporter(
            [metricsClient](const std::string &instanceID, const resources::SandboxResourceSample &sample) {
                metricsClient->UpdateInstanceResourceSample(instanceID, sample);
            });
    }
    litebus::Spawn(executor, false);
    auto executorProxy = std::make_shared<SandboxdExecutorProxy>(executor);
    (void)executorMap_.insert(std::make_pair(EXECUTOR_TYPE::SANDBOXD, executorProxy));
    return executorProxy;
}

void RuntimeManager::StartInstanceResponse(const litebus::AID &from, const std::string &instanceID,
                                           const std::string &requestID,
                                           const litebus::Future<messages::StartInstanceResponse> &response)
{
    if (response.IsError()) {
        messages::StartInstanceResponse failResponse;
        failResponse.set_code(static_cast<int32_t>(RUNTIME_MANAGER_START_INSTANCE_FAILED));
        failResponse.set_message("start instance failed");
        Send(from, "StartInstanceResponse", failResponse.SerializeAsString());
        return;  // end
    }

    auto output = response.Get();
    if (output.code() != static_cast<int32_t>(SUCCESS)) {
        YRLOG_ERROR("{}|failed to start runtime, code {}", output.requestid(), output.code());
    } else {
        YRLOG_DEBUG("{}|success to start runtime.", output.requestid());
        const auto &runtimeID = output.startruntimeinstanceresponse().runtimeid();
        if (!runtimeID.empty()) {
            runtimeResponseMap_[runtimeID] = output;
        }
        const auto latest = latestInFlightStartRequestByInstance_.find(instanceID);
        if (latest != latestInFlightStartRequestByInstance_.end() && latest->second == requestID) {
            instanceResponseMap_[instanceID] = output;
        }
    }
    const auto latest = latestInFlightStartRequestByInstance_.find(instanceID);
    if (latest != latestInFlightStartRequestByInstance_.end() && latest->second == requestID) {
        latestInFlightStartRequestByInstance_.erase(latest);
    }
    output.mutable_startruntimeinstanceresponse()->set_cputype(cpuType_);
    (void)receivedStartingReq_.erase(output.requestid());
    (void)Send(from, "StartInstanceResponse", output.SerializeAsString());
}

void RuntimeManager::RegisterToFunctionAgent()
{
    RETURN_IF_NULL(metricsClient_);
    resources::ResourceUnit unit = metricsClient_->GetResourceUnit();

    auto request = std::make_shared<messages::RegisterRuntimeManagerRequest>();
    request->set_name(GetAID().Name());
    request->set_address(GetAID().Url());
    request->set_id(runtimeManagerID_);
    auto resourceUnit = request->mutable_resourceunit();
    resourceUnit->CopyFrom(unit);
    for (auto &ins : resourceUnit->instances()) {
        auto instanceID = ins.first;
        std::string runtimeID;
        if (instanceResponseMap_.find(instanceID) == instanceResponseMap_.end()) {
            YRLOG_WARN("failed to find instance({}) in instance info map", instanceID);
        } else {
            runtimeID = instanceResponseMap_[instanceID].startruntimeinstanceresponse().runtimeid();
            resourceUnit->mutable_instances()->at(instanceID).set_runtimeid(runtimeID);
        }
        if (runtimeID.empty() || instanceInfoMap_.find(runtimeID) == instanceInfoMap_.end()) {
            YRLOG_WARN("failed to find instance({}), runtime({}) in runtime info map", instanceID, runtimeID);
        } else {
            auto storageType = instanceInfoMap_[runtimeID].deploymentconfig().storagetype();
            auto requestID = instanceInfoMap_[runtimeID].requestid();
            resourceUnit->mutable_instances()->at(instanceID).set_storagetype(storageType);
            resourceUnit->mutable_instances()->at(instanceID).set_requestid(requestID);
        }
        YRLOG_DEBUG("add instance({}) to regitster info", instanceID);
    }
    // todo merge both executor of info
    auto executor = FindExecutor(EXECUTOR_TYPE::RUNTIME);
    if (executor == nullptr) {
        YRLOG_ERROR("failed to get runtime executor.");
        return;
    }
    (void)executor->GetRuntimeInstanceInfos().Then(
        litebus::Defer(GetAID(), &RuntimeManager::StartRegister, std::placeholders::_1, request));
}

void RuntimeManager::TryInitialRegisterToFunctionAgent()
{
    if (!startRequested_ || initialRegistrationStarted_ ||
        sandboxRuntimeDiscoveryState_ == SandboxRuntimeDiscoveryState::PENDING) {
        return;
    }
    initialRegistrationStarted_ = true;
    RegisterToFunctionAgent();
}

void RuntimeManager::OnInitialAvailableRuntimes(bool enabled, const std::set<std::string> &runtimes)
{
    if (!enabled) {
        sandboxRuntimeDiscoveryState_ = SandboxRuntimeDiscoveryState::NOT_REQUIRED;
        TryInitialRegisterToFunctionAgent();
        return;
    }

    RETURN_IF_NULL(metricsClient_);
    metricsClient_->InitializeSandboxRuntimeCapabilities(runtimes).Then(
        litebus::Defer(GetAID(), &RuntimeManager::OnInitialAvailableRuntimesApplied, std::placeholders::_1));
}

Status RuntimeManager::OnInitialAvailableRuntimesApplied(const Status &status)
{
    if (!status.IsOk()) {
        YRLOG_ERROR("failed to initialize sandbox runtime capability snapshot: {}", status.RawMessage());
        return status;
    }
    sandboxRuntimeDiscoveryState_ = SandboxRuntimeDiscoveryState::READY;
    TryInitialRegisterToFunctionAgent();
    return Status::OK();
}

Status RuntimeManager::StartRegister(const std::map<std::string, messages::RuntimeInstanceInfo> &runtimeInfos,
                                     const std::shared_ptr<messages::RegisterRuntimeManagerRequest> &request)
{
    auto infos = request->mutable_runtimeinstanceinfos();
    for (const auto &info : runtimeInfos) {
        (void)infos->insert({ info.first, info.second });
    }
    auto receivedCallBack = [aid(GetAID())](const std::string &msg) {
        litebus::Async(aid, &RuntimeManager::ReceiveRegistered, msg);
    };
    auto timoutCallBack = [aid(GetAID())]() { litebus::Async(aid, &RuntimeManager::RegisterTimeout); };

    RETURN_STATUS_IF_NULL(registerHelper_, StatusCode::POINTER_IS_NULL, "registerHelper pointer is nullptr");
    registerHelper_->SetRegisteredCallback(receivedCallBack);
    registerHelper_->SetRegisterTimeoutCallback(timoutCallBack);
    registerHelper_->StartRegister(functionAgentAID_.Name(), functionAgentAID_.Url(), request->SerializeAsString(),
                                   MAX_REGISTER_RETRY_TIMES);
    return Status::OK();
}

void RuntimeManager::SendStopInstanceResponse(const litebus::AID &from, const std::string &runtimeID,
                                              const std::shared_ptr<messages::StopInstanceResponse> &response)
{
    if (auto iter(instanceInfoMap_.find(runtimeID)); iter != instanceInfoMap_.end()) {
        const auto &instanceID = iter->second.instanceid();
        const bool exactResumeTarget = resume_identity::IsExactResumeTargetCleanupRequest(
            response->requestid(), instanceID, runtimeID);
        const auto owner = instanceResponseMap_.find(instanceID);
        const bool stoppingLogicalOwner = owner == instanceResponseMap_.end()
            || owner->second.startruntimeinstanceresponse().runtimeid() == runtimeID;
        if (!exactResumeTarget) {
            (void)instanceResponseMap_.erase(instanceID);
        } else if (stoppingLogicalOwner) {
            bool rebound = false;
            for (const auto &[survivingRuntimeID, survivingInfo] : instanceInfoMap_) {
                if (survivingRuntimeID == runtimeID || survivingInfo.instanceid() != instanceID) {
                    continue;
                }
                const auto survivingResponse = runtimeResponseMap_.find(survivingRuntimeID);
                if (survivingResponse == runtimeResponseMap_.end()) {
                    continue;
                }
                instanceResponseMap_[instanceID] = survivingResponse->second;
                rebound = true;
                break;
            }
            if (!rebound) {
                (void)instanceResponseMap_.erase(instanceID);
            }
        }
        (void)runtimeResponseMap_.erase(runtimeID);
        (void)instanceInfoMap_.erase(runtimeID);
    if (response->code() == static_cast<int32_t>(StatusCode::SUCCESS)) {
        if (auto iter(instanceInfoMap_.find(runtimeID)); iter != instanceInfoMap_.end()) {
            (void)instanceResponseMap_.erase(iter->second.instanceid());
            (void)instanceInfoMap_.erase(iter);
        }
    }
    // Physical fault-test hook (default off): suppress exactly one confirmed
    // retirement receipt travelling upstream after sandboxd durably completed
    // the delete. The durable receipt stays in sandboxd; the caller's retry
    // with the same identity must replay it and settle exactly once.
    if (response->physical_retirement_confirmed()
        && RetireFaultGate::Instance().DropReceiptIfMatched(response->retired_sandbox_id(),
                                                           response->retired_resource_generation())) {
        YRLOG_WARN("{}|retire-fault: dropping confirmed retirement receipt for runtime({}) sandbox({}) "
                   "generation({}); caller retry must replay the durable receipt",
                   response->requestid(), runtimeID, response->retired_sandbox_id(),
                   response->retired_resource_generation());
        return;
    }
    Send(from, "StopInstanceResponse", response->SerializeAsString());
}
}

void RuntimeManager::OnGetRuntimeStatus(const litebus::AID &from,
                                        const std::shared_ptr<messages::StopInstanceRequest> &request,
                                        const std::shared_ptr<messages::StopInstanceResponse> &response,
                                        const litebus::Future<Status> &instanceStatus)
{
    if (instanceStatus.IsError() || instanceStatus.Get().StatusCode() != StatusCode::SUCCESS) {
        const auto reason = instanceStatus.IsError()
            ? "runtime exit observation failed: " + std::to_string(instanceStatus.GetErrorCode())
            : instanceStatus.Get().RawMessage();
        YRLOG_ERROR("{}|{}|failed to stop runtime({}), {}.", request->traceid(), request->requestid(),
                    request->runtimeid(), reason);
        response->set_code(static_cast<int32_t>(RUNTIME_MANAGER_STOP_INSTANCE_FAILED));
        response->set_message(reason);
        // A failed exit observation is not a complete logical stop: the proof
        // stays internal (retry re-obtains the idempotent receipt) and must
        // not travel upward on a failure response.
        response->set_physical_retirement_confirmed(false);
        response->clear_retired_sandbox_id();
        response->clear_retired_resource_generation();
    } else {
        YRLOG_INFO("{}|{}|success to stop runtime({}).", request->traceid(), request->requestid(),
                   request->runtimeid());
        response->set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        response->set_message("stop instance success");
        // Retain identity, telemetry and port ownership until exit is confirmed.
        const litebus::Future<Status> retired(Status::OK());
        DeleteInstanceMetrics(retired, request);
        DestroyDebugServer(retired, request);
        ReleasePort(retired, request);
        healthCheckClient_->RemoveRuntimeStatusCache(request->runtimeid());
    }
    SendStopInstanceResponse(from, request->runtimeid(), response);
}

void RuntimeManager::StopInstanceResponse(const litebus::AID &from, const litebus::Future<Status> &status,
                                          const std::shared_ptr<messages::StopInstanceRequest> &request)
{
    auto response = std::make_shared<messages::StopInstanceResponse>();
    response->set_runtimeid(request->runtimeid());
    response->set_requestid(request->requestid());
    response->set_traceid(request->traceid());
    if (status.IsError()) {
        YRLOG_ERROR("{}|{}|failed to stop runtime({}).", request->traceid(), request->requestid(),
                    request->runtimeid());
        response->set_code(static_cast<int32_t>(RUNTIME_MANAGER_STOP_INSTANCE_FAILED));
        response->set_message("stop instance failed");
        SendStopInstanceResponse(from, request->runtimeid(), response);
        return;
    } else if (status.Get().IsError()) {
        YRLOG_ERROR("{}|{}|failed({}) to stop runtime({}).", request->traceid(), request->requestid(),
                    fmt::underlying(status.Get().StatusCode()), request->runtimeid());
        response->set_code(static_cast<int32_t>(status.Get().StatusCode()));
        response->set_message("stop instance failed");
        SendStopInstanceResponse(from, request->runtimeid(), response);
        return;
    }

    // Note: response after runtime child process exited: indicates that resources ownered by runtime are reclaimed.
    healthCheckClient_->GetRuntimeStatus(request->runtimeid())
        .OnComplete(litebus::Defer(GetAID(), &RuntimeManager::OnGetRuntimeStatus, from, request, response,
                                   std::placeholders::_1));
}

void RuntimeManager::DeleteInstanceMetrics(const litebus::Future<Status> &status,
                                           const std::shared_ptr<messages::StopInstanceRequest> &request)
{
    if (status.IsError()) {
        YRLOG_ERROR("{}|{}|can not delete metrics, stop instance failed, runtimeID({})", request->traceid(),
                    request->requestid(), request->runtimeid());
        return;
    }
    if (status.Get().IsError()) {
        YRLOG_ERROR("{}|{}|can not delete metrics, stop instance failed, runtimeID({})", request->traceid(),
                    request->requestid(), request->runtimeid());
        return;
    }
    std::string runtimeID = request->runtimeid();
    if (instanceInfoMap_.find(runtimeID) != instanceInfoMap_.end()) {
        auto instanceInfo = instanceInfoMap_[runtimeID];
        if (resume_identity::IsExactResumeTargetCleanupRequest(
                request->requestid(), instanceInfo.instanceid(), runtimeID)) {
            for (const auto &[ownedRuntimeID, ownedInfo] : instanceInfoMap_) {
                if (ownedRuntimeID != runtimeID
                    && ownedInfo.instanceid() == instanceInfo.instanceid()) {
                    YRLOG_INFO("{}|{}|keep winner metrics while stopping exact resume loser runtime({})",
                               request->traceid(), request->requestid(), runtimeID);
                    return;
                }
            }
        }
        RETURN_IF_NULL(metricsClient_);
        (void)metricsClient_->DeleteInstanceMetrics(instanceInfo.deploymentconfig().deploydir(),
                                                    instanceInfo.instanceid());
        YRLOG_INFO("{}|{}|runtime manager erase collector, runtimeID({}), instanceID({})", request->traceid(),
                   request->requestid(), request->runtimeid(), instanceInfo.instanceid());
    }
}

void RuntimeManager::CreateInstanceMetrics(const litebus::Future<messages::StartInstanceResponse> &response,
                                           const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    if (response.IsError()) {
        YRLOG_ERROR("{}|{}|failed to start instance, do not add metrics collector, instanceID: {}, runtimeID: {}",
                    request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(),
                    request->runtimeinstanceinfo().instanceid(), request->runtimeinstanceinfo().runtimeid());
        return;
    }
    if (response.Get().code() != static_cast<int32_t>(SUCCESS)) {
        YRLOG_ERROR("{}|{}|failed to start instance, do not add metrics collector, instanceID: {}, runtimeID: {}",
                    request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(),
                    request->runtimeinstanceinfo().instanceid(), request->runtimeinstanceinfo().runtimeid());
        return;
    }

    auto instanceInfo = request->runtimeinstanceinfo();
    instanceInfoMap_[response.Get().startruntimeinstanceresponse().runtimeid()] = instanceInfo;
    RETURN_IF_NULL(metricsClient_);
    metricsClient_->CreateInstanceMetrics(response, request);
}

void RuntimeManager::ReleasePort(const litebus::Future<Status> &status,
                                 const std::shared_ptr<messages::StopInstanceRequest> &request)
{
    if (status.IsError()) {
        YRLOG_ERROR("{}|{}|status get error, can not release port, runtimeID: {}", request->traceid(),
                    request->requestid(), request->runtimeid());
        return;
    }
    if (status.Get().IsError()) {
        YRLOG_ERROR("{}|{}|status get error, can not release port, runtimeID: {}", request->traceid(),
                    request->requestid(), request->runtimeid());
        return;
    }

    YRLOG_INFO("{}|{}|release port, runtimeID: {}", request->traceid(), request->requestid(), request->runtimeid());
    (void)PortManager::GetInstance().ReleasePort(request->runtimeid());

    if (logReuse_) {
        logManagerClient_->ReleaseLogPrefix(request->runtimeid());
    }
}

void RuntimeManager::HeartbeatTimeoutHandler(const litebus::AID &from)
{
    YRLOG_ERROR("heartbeat with FunctionAgent({}) timeout", std::string(from));
    connected_ = false;

    RETURN_IF_NULL(registerHelper_);
    registerHelper_->StopPingPongDriver();

    RETURN_IF_NULL(metricsClient_);
    metricsClient_->StopUpdateResource();
    metricsClient_->StopDiskUsageMonitor();
    RegisterToFunctionAgent();
}

void RuntimeManager::CheckHealthForRuntime(const litebus::Future<messages::StartInstanceResponse> &response,
                                           const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    if (response.IsError()) {
        YRLOG_ERROR("{}|{}|failed to start instance, do not check health for instance({}) runtime({})",
                    request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(),
                    request->runtimeinstanceinfo().instanceid(), request->runtimeinstanceinfo().runtimeid());
        return;
    }
    if (response.Get().code() != static_cast<int32_t>(SUCCESS)) {
        YRLOG_ERROR("{}|{}|failed to start instance, do not check health for instance({}) runtime({})",
                    request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(),
                    request->runtimeinstanceinfo().instanceid(), request->runtimeinstanceinfo().runtimeid());
        return;
    }

    auto runtimeID = response.Get().startruntimeinstanceresponse().runtimeid();
    auto pid = static_cast<pid_t>(response.Get().startruntimeinstanceresponse().pid());
    // todo: health check should be mv to executor
    if (pid == 0) {
        return;
    }
    YRLOG_INFO("{}|{}|check health for instance({}) runtime({}) pid({})", request->runtimeinstanceinfo().traceid(),
               request->runtimeinstanceinfo().requestid(), request->runtimeinstanceinfo().instanceid(), runtimeID, pid);
    auto instanceID = request->runtimeinstanceinfo().instanceid();
    RETURN_IF_NULL(healthCheckClient_);
    healthCheckClient_->AddRuntimeRecord(functionAgentAID_, pid, instanceID, runtimeID, nodeID_);
}

bool RuntimeManager::ShouldSkipDebugServerCreation(const litebus::Future<messages::StartInstanceResponse> &response,
                                                   const std::shared_ptr<messages::StartInstanceRequest> &request) const
{
    if (!runtimeInstanceDebugEnable_) {
        return true;
    }

    if (response.IsError() || response.Get().code() != static_cast<int32_t>(SUCCESS)) {
        YRLOG_ERROR("{}|{}|failed to start instance, do not create debug server, instanceID: {}, runtimeID: {}",
                    request->runtimeinstanceinfo().traceid(), request->runtimeinstanceinfo().requestid(),
                    request->runtimeinstanceinfo().instanceid(), request->runtimeinstanceinfo().runtimeid());
        return true;
    }

    const auto &envs = request->runtimeinstanceinfo().runtimeconfig().posixenvs();
    if (envs.find(YR_DEBUG_CONFIG) == envs.end()) {
        return true;
    }

    // parse debug config
    nlohmann::json parser;
    auto iter = request->runtimeinstanceinfo().runtimeconfig().posixenvs().find(YR_DEBUG_CONFIG);
    try {
        parser = nlohmann::json::parse(iter->second);
    } catch (nlohmann::json::parse_error &e) {
        YRLOG_WARN("failed to parse ({}) to json, error: {}", iter->second, e.what());
        return true;
    }
    const auto &instanceID = request->runtimeinstanceinfo().instanceid();
    const auto &runtimeID = request->runtimeinstanceinfo().runtimeid();
    static const std::string ENABLE_DEBUG_KEY = "enable";
    if (parser.find(ENABLE_DEBUG_KEY) == parser.end() || parser[ENABLE_DEBUG_KEY] == "false") {
        YRLOG_DEBUG("{}|{}|disable debug instanceID: {}, runtimeID: {}", request->runtimeinstanceinfo().traceid(),
                    request->runtimeinstanceinfo().requestid(), instanceID, runtimeID);
        return true;
    }
    return false;
}

litebus::Future<messages::StartInstanceResponse> RuntimeManager::DebugServerAddRecord(
    const litebus::Future<messages::StartInstanceResponse> &response,
    const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    if (ShouldSkipDebugServerCreation(response, request)) {
        return response;
    }
    ASSERT_IF_NULL(debugServerMgr_);
    auto pid = static_cast<pid_t>(response.Get().startruntimeinstanceresponse().pid());
    const auto &runtimeID = request->runtimeinstanceinfo().runtimeid();
    debugServerMgr_->AddRecord(runtimeID, pid);
    return response;
}

litebus::Future<messages::StartInstanceResponse> RuntimeManager::CreateDebugServer(
    const litebus::Future<messages::StartInstanceResponse> &response,
    const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    if (ShouldSkipDebugServerCreation(response, request)) {
        return response;
    }
    const auto &instanceID = request->runtimeinstanceinfo().instanceid();
    const auto &runtimeID = request->runtimeinstanceinfo().runtimeid();
    YRLOG_DEBUG("{}|{}|enable debug instanceID: {}, runtimeID: {}", request->runtimeinstanceinfo().traceid(),
                request->runtimeinstanceinfo().requestid(), instanceID, runtimeID);

    // allocate a port for debug server
    auto debugServerPort = PortManager::GetInstance().RequestPort(runtimeID);
    request->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_debugserverport(debugServerPort);
    std::string language = request->runtimeinstanceinfo().runtimeconfig().language();
    (void)transform(language.begin(), language.end(), language.begin(), ::tolower);

    // create debug server
    ASSERT_IF_NULL(debugServerMgr_);
    litebus::Future<Status> status =
        debugServerMgr_->CreateServer(runtimeID, debugServerPort, instanceID, language)
            .Then([traceID(request->runtimeinstanceinfo().traceid()),
                   requestID(request->runtimeinstanceinfo().requestid()), debugServerPort, instanceID,
                   runtimeID](litebus::Future<Status> status) {
                if (status.IsError() || status.Get().IsError()) {
                    return status;
                }
                YRLOG_INFO("{}|{}|create debug server port({}) for instance({}) runtime({})", traceID, requestID,
                           debugServerPort, instanceID, runtimeID);
                return status;
            });

    // return fail when not found gdbserver
    if (status.IsError() || status.Get().IsError()) {
        messages::StartInstanceResponse failResponse;
        failResponse.set_requestid(request->runtimeinstanceinfo().requestid());
        failResponse.set_code(status.Get().StatusCode());
        failResponse.set_message(status.Get().RawMessage());
        return failResponse;
    }
    return response;
}

void RuntimeManager::DestroyDebugServer(const litebus::Future<Status> &status,
                                        const std::shared_ptr<messages::StopInstanceRequest> &request)
{
    if (!runtimeInstanceDebugEnable_) {
        return;
    }

    const auto &runtimeID = request->runtimeid();
    if (status.IsError()) {
        YRLOG_ERROR("{}|{}|status get error, can not destroy debug server, runtimeID: {}", request->traceid(),
                    request->requestid(), runtimeID);
        return;
    }
    if (status.Get().IsError()) {
        YRLOG_ERROR("{}|{}|status get error, can not destroy debug server, runtimeID: {}", request->traceid(),
                    request->requestid(), runtimeID);
        return;
    }

    RETURN_IF_NULL(debugServerMgr_);
    debugServerMgr_->DestroyServer(runtimeID).Then(
        [traceID(request->traceid()), requestID(request->requestid()), runtimeID](litebus::Future<Status> status) {
            if (status.IsError() || status.Get().IsError()) {
                return status;
            }
            YRLOG_INFO("{}|{}|destroy debug server, runtimeID: {}", traceID, requestID, runtimeID);
            return status;
        });
}

void RuntimeManager::SetRegisterHelper(const std::shared_ptr<RegisterHelper> helper)
{
    registerHelper_ = helper;
}

void RuntimeManager::Start()
{
    startRequested_ = true;
    TryInitialRegisterToFunctionAgent();
    RETURN_IF_NULL(logManagerClient_);
    logManagerClient_->StartScanLogs();
}

void RuntimeManager::ReceiveRegistered(const std::string &message)
{
    YRLOG_INFO("receive registered message");
    messages::RegisterRuntimeManagerResponse response;
    if (!response.ParseFromString(message)) {
        YRLOG_ERROR("failed to parse Registered message");
        return;
    }

    int code = response.code();
    if (code == static_cast<int32_t>(StatusCode::SUCCESS)) {
        YRLOG_INFO("succeed to register to FunctionAgent");
        RETURN_IF_NULL(registerHelper_);
        registerHelper_->SetPingPongDriver(
            GetAID().Name(), functionAgentAID_.Url(), pingTimeoutMs_,
            [aid(GetAID()), name = GetAID().Name()](const litebus::AID &dst) {
                YRLOG_WARN("pingpong {} timeout, from runtime to agent. dst: {}", name, std::string(dst));
                litebus::Async(aid, &RuntimeManager::HeartbeatTimeoutHandler, dst);
            },
            RUNTIME_MANAGER_ACTOR_NAME);

        RETURN_IF_NULL(metricsClient_);
        metricsClient_->UpdateAgentInfo(functionAgentAID_);
        metricsClient_->UpdateRuntimeManagerInfo(GetAID());
        metricsClient_->StartUpdateResource();
        metricsClient_->BeginUpdateMetrics();
        metricsClient_->StartDiskUsageMonitor();
        metricsClient_->StartRuntimeMemoryLimitMonitor();
        RETURN_IF_NULL(healthCheckClient_);
        healthCheckClient_->UpdateAgentInfo(functionAgentAID_);
        connected_ = true;
        return;
    }
    YRLOG_WARN("{}|runtime manager failed to register to FunctionAgent", runtimeManagerID_);
    // clean status after register failed
    CommitSuicide();
}

void RuntimeManager::RegisterTimeout()
{
    YRLOG_WARN("{}|runtime manager register to FunctionAgent timeout", runtimeManagerID_);
    // clean status after register failed
    CommitSuicide();
}

void RuntimeManager::QueryDebugInstanceInfos(const litebus::AID &from, std::string &&, std::string &&msg)
{
    if (!runtimeInstanceDebugEnable_) {
        YRLOG_WARN("runtime instance not enable debug mode");
        return;
    }

    messages::QueryDebugInstanceInfosRequest request;
    if (!request.ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse QueryDebugInstanceInfosRequest");
        return;
    }
    if (!connected_) {
        YRLOG_ERROR(
            "{}|runtimeManager registration to functionAgent is not complete, ignore query debug instance infos.",
            request.requestid());
        return;
    }
    YRLOG_DEBUG("{}|received query debug instances infos.", request.requestid());

    RETURN_IF_NULL(debugServerMgr_);
    debugServerMgr_->QueryDebugInstanceInfos(request.requestid())
        .Then(litebus::Defer(GetAID(), &RuntimeManager::QueryDebugInstanceInfosResponse, from, std::placeholders::_1));
}

Status RuntimeManager::QueryDebugInstanceInfosResponse(const litebus::AID &from,
                                                       const messages::QueryDebugInstanceInfosResponse &response)
{
    YRLOG_DEBUG("{}|response query debug instances infos.", response.requestid());
    (void)Send(from, "QueryDebugInstanceInfosResponse", response.SerializeAsString());
    return Status::OK();
}

Status RuntimeManager::SnapshotRuntimeResponse(
    const litebus::AID &from,
    const std::shared_ptr<messages::SnapshotRuntimeRequest> &request,
    const litebus::Future<messages::SnapshotRuntimeResponse> &responseFuture)
{
    const auto &requestID = request->requestid();
    if (responseFuture.IsError()) {
        YRLOG_ERROR("{}|snapshot runtime future error: {}", requestID, responseFuture.GetErrorCode());
        messages::SnapshotRuntimeResponse response;
        response.set_requestid(requestID);
        response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
        response.set_message("snapshot runtime failed");
        (void)Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return Status::OK();
    }

    auto response = responseFuture.Get();
    YRLOG_INFO("{}|snapshot runtime completed for instance({}), code: {}", requestID, request->instanceid(),
               response.code());
    (void)Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
    return Status::OK();
}

void RuntimeManager::QueryInstanceStatusInfo(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::QueryInstanceStatusRequest request;
    if (!request.ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse QueryInstanceStatusRequest");
        return;
    }
    if (!connected_) {
        YRLOG_ERROR(
            "{}|runtimeManager registration to functionAgent is not complete, "
            "ignore query instance status info, instanceID {}, runtimeID {}.",
            request.requestid(), request.instanceid(), request.runtimeid());
        return;
    }
    if (instanceInfoMap_.find(request.runtimeid()) == instanceInfoMap_.end()) {
        YRLOG_WARN("{}|received query instanceID({}) runtimeID({}). which is not existed", request.requestid(),
                   request.instanceid(), request.runtimeid());
        messages::InstanceStatusInfo info;
        info.set_instanceid(request.instanceid());
        info.set_runtimeid(request.runtimeid());
        info.set_status(-1);
        info.set_instancemsg("an unknown error caused the instance exited. instance:" + request.instanceid()
                             + " runtime:" + request.runtimeid() + " which is not found. ");
        info.set_type(static_cast<int32_t>(EXIT_TYPE::NONE_EXIT));
        QueryInstanceStatusInfoResponse(from, request.requestid(), info);
        return;
    }
    YRLOG_INFO("{}|received query instanceID({}) runtimeID({}) status.", request.requestid(), request.instanceid(),
               request.runtimeid());

    RETURN_IF_NULL(healthCheckClient_);
    healthCheckClient_->QueryInstanceStatusInfo(request.instanceid(), request.runtimeid())
        .Then(litebus::Defer(GetAID(), &RuntimeManager::QueryInstanceStatusInfoResponse, from, request.requestid(),
                             std::placeholders::_1));
}

Status RuntimeManager::QueryInstanceStatusInfoResponse(const litebus::AID &from, const std::string &requestID,
                                                       const messages::InstanceStatusInfo &info)
{
    messages::QueryInstanceStatusResponse response;
    *response.mutable_instancestatusinfo() = info;
    response.set_requestid(requestID);
    YRLOG_INFO("{}|response query instanceID({}) status.", requestID, info.instanceid());
    (void)Send(from, "QueryInstanceStatusInfoResponse", response.SerializeAsString());
    return Status::OK();
}

void RuntimeManager::CleanStatus(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::CleanStatusRequest cleanStatusRequest;
    if (!cleanStatusRequest.ParseFromString(msg)) {
        YRLOG_ERROR("{}|failed to parse function-agent({}) CleanStatus message", runtimeManagerID_, from.HashString());
        return;
    }

    messages::CleanStatusResponse cleanStatusResponse;
    (void)Send(from, "CleanStatusResponse", cleanStatusResponse.SerializeAsString());

    if (cleanStatusRequest.name() == runtimeManagerID_) {
        YRLOG_WARN("{}|receive CleanStatus from function-agent, runtime-manager gonna to suicide", runtimeManagerID_);
        CommitSuicide();
        return;
    }
    YRLOG_INFO("{}|receive CleanStatus from function-agent, RuntimeManagerID error, err id = {}", runtimeManagerID_,
               cleanStatusRequest.name());
}
void RuntimeManager::CommitSuicide() const
{
    if (!isUnitTestSituation_) {
        (void)raise(SIGINT);
    }
}

void RuntimeManager::UpdateCred(const litebus::AID &from, std::string &&, std::string &&msg)
{
    auto request = std::make_shared<messages::UpdateCredRequest>();
    if (!request->ParseFromString(msg)) {
        YRLOG_ERROR("{}|failed to parse function-agent({}) UpdateCred message", runtimeManagerID_, from.HashString());
        return;
    }
    auto requestID = request->requestid();
    auto runtimeID = request->runtimeid();

    YRLOG_DEBUG("{}|{}|runtime-manager({}) receive UpdateCred from function-agent", requestID, runtimeID,
                runtimeManagerID_);

    auto executor = FindExecutor(GetRuntimeType(runtimeID));
    if (executor == nullptr) {
        YRLOG_ERROR("{}|{}|failed to get runtime executor", requestID, runtimeID);
        messages::UpdateCredResponse updateTokenResponse;
        updateTokenResponse.set_code(static_cast<int32_t>(RUNTIME_MANAGER_PARAMS_INVALID));
        updateTokenResponse.set_message("failed to get runtime executor");
        Send(from, "UpdateCredResponse", updateTokenResponse.SerializeAsString());
        return;
    }
    executor->UpdateCredForRuntime(request).OnComplete(
        litebus::Defer(this->GetAID(), &RuntimeManager::UpdateCredResponse, from, std::placeholders::_1));
}

void RuntimeManager::UpdateCredResponse(const litebus::AID &to,
                                        const litebus::Future<messages::UpdateCredResponse> &response)
{
    if (response.IsError()) {
        messages::UpdateCredResponse failResponse;
        failResponse.set_code(static_cast<int32_t>(RUNTIME_MANAGER_UPDATE_TOKEN_FAILED));
        failResponse.set_message("update token failed");
        Send(to, "UpdateCredResponse", failResponse.SerializeAsString());
        return;
    }
    const auto &output = response.Get();
    (void)Send(to, "UpdateCredResponse", output.SerializeAsString());
}

void RuntimeManager::UpdateNetworkPolicy(
    const litebus::AID &from, std::string &&, std::string &&msg)
{
    auto request = std::make_shared<messages::UpdateNetworkPolicyRequest>();
    if (!request->ParseFromString(msg)) {
        YRLOG_ERROR("{}|failed to parse UpdateNetworkPolicy from function-agent({})",
                    runtimeManagerID_, from.HashString());
        return;
    }
    const auto &requestID = request->requestid();
    const auto &runtimeID = request->runtimeid();
    auto executor = FindExecutor(GetRuntimeType(runtimeID));
    if (executor == nullptr) {
        messages::UpdateNetworkPolicyResponse response;
        response.set_requestid(requestID);
        response.set_code(static_cast<int32_t>(RUNTIME_MANAGER_PARAMS_INVALID));
        response.set_message("failed to get runtime executor");
        Send(from, "UpdateNetworkPolicyResponse", response.SerializeAsString());
        return;
    }
    executor->UpdateNetworkPolicyForRuntime(request).OnComplete(
        litebus::Defer(GetAID(), &RuntimeManager::UpdateNetworkPolicyResponse,
                       from, requestID, std::placeholders::_1));
}

void RuntimeManager::UpdateNetworkPolicyResponse(
    const litebus::AID &to,
    const std::string &requestID,
    const litebus::Future<messages::UpdateNetworkPolicyResponse> &response)
{
    if (response.IsError()) {
        messages::UpdateNetworkPolicyResponse failed;
        failed.set_requestid(requestID);
        failed.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION));
        failed.set_message("network policy update failed");
        Send(to, "UpdateNetworkPolicyResponse", failed.SerializeAsString());
        return;
    }
    (void)Send(to, "UpdateNetworkPolicyResponse",
               response.Get().SerializeAsString());
}

litebus::Future<Status> RuntimeManager::NotifyInstancesDiskUsageExceedLimit(const std::string &description,
                                                                            const int limit)
{
    auto executor = FindExecutor(EXECUTOR_TYPE::RUNTIME);
    if (executor == nullptr) {
        YRLOG_ERROR("failed to get runtime executor");
        return Status(StatusCode::FAILED);
    }
    return executor->NotifyInstancesDiskUsageExceedLimit(description, limit);
}

litebus::Future<bool> RuntimeManager::IsRuntimeActive(const std::string &runtimeID)
{
    auto executor = FindExecutor(GetRuntimeType(runtimeID));
    if (executor == nullptr) {
        YRLOG_ERROR("failed to get runtime({}) executor", runtimeID);
        return false;
    }
    return executor->IsRuntimeActive(runtimeID);
}

EXECUTOR_TYPE RuntimeManager::ResolveStopExecutorType(const std::shared_ptr<messages::StopInstanceRequest> &request)
{
    auto executorType = static_cast<EXECUTOR_TYPE>(request->executortype());
    if (executorType != EXECUTOR_TYPE::RUNTIME) {
        executorType = NormalizeExecutorType(executorType);
        request->set_executortype(static_cast<int32_t>(executorType));
        return executorType;
    }

    const auto runtimeType = GetRuntimeType(request->runtimeid());
    if (runtimeType != EXECUTOR_TYPE::RUNTIME) {
        request->set_executortype(static_cast<int32_t>(runtimeType));
        return runtimeType;
    }
    return executorType;
}

litebus::Future<bool> RuntimeManager::IsRuntimeActiveByPid(const pid_t &pid)
{
    auto executor = FindExecutor(EXECUTOR_TYPE::RUNTIME);
    if (executor == nullptr) {
        YRLOG_ERROR("failed to get runtime({}) executor", pid);
        return false;
    }
    return executor->IsRuntimeActiveByPid(pid);
}

EXECUTOR_TYPE RuntimeManager::GetRuntimeType(const std::string &runtimeID)
{
    const auto instance = instanceInfoMap_.find(runtimeID);
    if (instance == instanceInfoMap_.end()) {
        return EXECUTOR_TYPE::RUNTIME;
    }

    if (instance->second.has_container()) {
        const auto response = instanceResponseMap_.find(instance->second.instanceid());
        if (response != instanceResponseMap_.end()) {
            const auto executorType =
                static_cast<EXECUTOR_TYPE>(response->second.startruntimeinstanceresponse().executortype());
            if (executorType != EXECUTOR_TYPE::RUNTIME) {
                return NormalizeExecutorType(executorType);
            }
        }
        return ContainerExecutorType();
    }

    const auto response = instanceResponseMap_.find(instance->second.instanceid());
    if (response != instanceResponseMap_.end()) {
        return NormalizeExecutorType(
            static_cast<EXECUTOR_TYPE>(response->second.startruntimeinstanceresponse().executortype()));
    }
    return EXECUTOR_TYPE::RUNTIME;
}
void RuntimeManager::ReconcileRuntimes(const litebus::AID &from, std::string &&, std::string &&msg)
{
    auto request = std::make_shared<messages::ReconcileRuntimesRequest>();
    if (!request->ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse ReconcileRuntimesRequest");
        return;
    }
    auto executor = FindExecutor(ContainerExecutorType());
    if (executor == nullptr) {
        YRLOG_WARN("{}|no container executor available for ReconcileRuntimes", request->requestid());
        messages::ReconcileRuntimesResponse resp;
        resp.set_requestid(request->requestid());
        resp.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
        resp.set_message("container executor not available");
        Send(from, "ReconcileRuntimesResponse", resp.SerializeAsString());
        return;
    }
    executor->ReconcileRuntimes(request).OnComplete(litebus::Defer(
        GetAID(), [this, from,
                   requestID = request->requestid()](const litebus::Future<messages::ReconcileRuntimesResponse> &resp) {
                if (resp.IsError()) {
                    YRLOG_ERROR("{}|ReconcileRuntimes failed: {}", requestID, resp.GetErrorCode());
                    messages::ReconcileRuntimesResponse errResp;
                    errResp.set_requestid(requestID);
                    errResp.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
                    errResp.set_message("ReconcileRuntimes execution failed");
                    Send(from, "ReconcileRuntimesResponse", errResp.SerializeAsString());
                    return;
                }
                // Register confirmed entries in instanceInfoMap_ so GetRuntimeType
                // routes subsequent Stop requests to the CONTAINER executor.
                const auto &result = resp.Get();
                for (const auto &entry : result.confirmedentries()) {
                    if (instanceInfoMap_.find(entry.runtimeid()) == instanceInfoMap_.end()) {
                        messages::RuntimeInstanceInfo info;
                        info.set_runtimeid(entry.runtimeid());
                        if (!entry.instanceid().empty()) {
                            info.set_instanceid(entry.instanceid());
                        }
                        info.mutable_container()->set_id(entry.containerid());
                        instanceInfoMap_[entry.runtimeid()] = info;
                    }
                }
                Send(from, "ReconcileRuntimesResponse", result.SerializeAsString());
            }));
}
}  // namespace functionsystem::runtime_manager
