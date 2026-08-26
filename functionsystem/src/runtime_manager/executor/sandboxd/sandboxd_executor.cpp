/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "sandboxd_executor.h"

#include <algorithm>
#include <chrono>
#include <nlohmann/json.hpp>
#include <unordered_set>

#include "async/asyncafter.hpp"
#include "async/collect.hpp"
#include "common/constants/actor_name.h"
#include "common/constants/constants.h"
#include "common/logs/logging.h"
#include "common/metrics/metrics_adapter.h"
#include "common/resource_view/resource_type.h"
#include "common/trace/create_trace_helper.h"
#include "common/utils/actor_worker.h"
#include "common/utils/collect_status.h"
#include "common/utils/generate_message.h"
#include "common/utils/port_forward_mapping.h"
#include "common/utils/struct_transfer.h"
#include "port/port_manager.h"
#include "runtime_manager/config/build.h"
#include "runtime_manager/executor/sandboxd/sandbox_command_utils.h"
#include "sandboxd_request_builder.h"
#include "utils/utils.h"

namespace functionsystem::runtime_manager {

using json = nlohmann::json;

namespace {
constexpr int64_t DEFAULT_GRACEFUL_SHUTDOWN         = 5;
constexpr int64_t RECONNECT_INTERVAL_MS             = 5000;
constexpr int64_t RECONCILE_RETRY_INITIAL_MS        = 1000;
constexpr int64_t RECONCILE_RETRY_MAX_INTERVAL_MS   = 10000;
constexpr int32_t RECONCILE_MAX_RETRIES             = 120;
constexpr int32_t CONTAINER_DELETE_TIMEOUT_SEC      = 10;

constexpr int32_t WAIT_MAX_RETRIES                  = 30;
constexpr int64_t WAIT_RETRY_INTERVAL_MS            = 10000;
constexpr int64_t SANDBOX_STATS_COLLECT_INTERVAL_MS    = 10000;
constexpr int64_t SANDBOX_RUNNING_HEARTBEAT_INTERVAL_MS = 30000;
constexpr double CPU_MILLICORES_PER_CORE            = 1000.0;
constexpr double BYTES_PER_MB                       = 1024.0 * 1024.0;
constexpr double DEFAULT_SANDBOX_CPU_MILLICORES     = 500.0;
constexpr double DEFAULT_SANDBOX_MEMORY_MB          = 500.0;
constexpr int32_t MAX_PORT_NUMBER                   = 65535;

struct SandboxRequestedResources {
    double cpuCores = 0.0;
    double memoryBytes = 0.0;
};

Status ParseReconcileHostPorts(const messages::RuntimeReconcileEntry &entry, std::vector<int> *hostPorts)
{
    hostPorts->clear();
    if (entry.portmappings().empty()) {
        return Status::OK();
    }

    try {
        const auto mappings = json::parse(entry.portmappings());
        if (!mappings.is_array()) {
            return Status(StatusCode::RUNTIME_MANAGER_PORT_UNAVAILABLE,
                          "persisted port mappings for runtime " + entry.runtimeid() + " are not a JSON array");
        }
        for (const auto &value : mappings) {
            if (!value.is_string()) {
                return Status(StatusCode::RUNTIME_MANAGER_PORT_UNAVAILABLE,
                              "persisted port mapping for runtime " + entry.runtimeid() + " is not a string");
            }
            const auto mapping = ParsePortForwardMapping(value.get<std::string>());
            if (!mapping.has_value()) {
                return Status(StatusCode::RUNTIME_MANAGER_PORT_UNAVAILABLE,
                              "invalid persisted port mapping for runtime " + entry.runtimeid() + ": " +
                                  value.dump());
            }
            hostPorts->push_back(static_cast<int>(mapping->hostPort));
        }
    } catch (const json::exception &e) {
        return Status(StatusCode::RUNTIME_MANAGER_PORT_UNAVAILABLE,
                      "failed to parse persisted port mappings for runtime " + entry.runtimeid() + ": " + e.what());
    }
    return Status::OK();
}

// Downstream sandboxd port forwarding only accepts L4 protocols (tcp/udp). L7
// portForward schemes (http/https/ws/wss) are normalized to tcp before sending
// to sandboxd because the underlying mapping is TCP NAT; other schemes are
// preserved.
// The portForward written back to instanceinfo canonicalizes L7 routing metadata to http/https.
std::string ToDownstreamL4Protocol(const std::string &proto)
{
    if (proto == "http" || proto == "https" || proto == "ws" || proto == "wss") {
        return "tcp";
    }
    return proto;
}

std::string ResolveRuntimeLanguage(const messages::RuntimeInstanceInfo &info)
{
    std::string language = info.runtimeconfig().language();
    std::transform(language.begin(), language.end(), language.begin(), ::tolower);
    return language;
}

std::string RootfsTypeToLabel(runtime::v1::RootfsSrcType rootfsType)
{
    switch (rootfsType) {
        case runtime::v1::IMAGE:
            return "image";
        case runtime::v1::S3:
            return "s3";
        case runtime::v1::LOCAL:
            return "local";
        default:
            return "unknown";
    }
}

std::string BuildS3RootfsRef(const runtime::v1::S3Config &s3Config)
{
    if (!s3Config.object().empty()) {
        return s3Config.bucket().empty() ? s3Config.object() : s3Config.bucket() + "/" + s3Config.object();
    }
    return s3Config.bucket();
}

std::string GetJsonString(const json &value, const std::string &key)
{
    if (value.contains(key) && value.at(key).is_string()) {
        return value.at(key).get<std::string>();
    }
    return "";
}

std::string ResolveS3RootfsOption(const json &rootfsOption)
{
    if (!rootfsOption.contains("storageInfo")) {
        return "";
    }
    const auto &storageInfo = rootfsOption.at("storageInfo");
    const auto bucket = GetJsonString(storageInfo, "bucket");
    const auto object = GetJsonString(storageInfo, "object");
    return object.empty() ? bucket : bucket + "/" + object;
}

std::string ResolveRootfsOption(const json &rootfsOption)
{
    if (!rootfsOption.contains("type")) {
        return "";
    }
    const std::string typeStr = rootfsOption.at("type").get<std::string>();
    if (typeStr == "s3") {
        return ResolveS3RootfsOption(rootfsOption);
    }
    if (typeStr == "image") {
        return GetJsonString(rootfsOption, "imageurl");
    }
    if (typeStr == "local") {
        return GetJsonString(rootfsOption, "path");
    }
    return "";
}

std::string ResolveSandboxImage(const messages::RuntimeInstanceInfo &info)
{
    auto it = info.deploymentconfig().deployoptions().find("rootfs");
    if (it != info.deploymentconfig().deployoptions().end() && !it->second.empty()) {
        try {
            return ResolveRootfsOption(json::parse(it->second));
        } catch (const std::exception &e) {
            YRLOG_WARN("ResolveSandboxImage: failed to parse rootfs deploy option: {}", e.what());
        }
    }

    const auto &rootfs = info.container().rootfsconfig();
    switch (rootfs.type()) {
        case runtime::v1::IMAGE:
            if (!rootfs.image_url().empty()) {
                return rootfs.image_url();
            }
            break;
        case runtime::v1::LOCAL:
            if (!rootfs.path().empty()) {
                return rootfs.path();
            }
            break;
        case runtime::v1::S3:
            if (rootfs.has_s3_config()) {
                return BuildS3RootfsRef(rootfs.s3_config());
            }
            break;
        default:
            break;
    }

    return "";
}

SandboxRequestedResources GetSandboxRequestedResources(const messages::RuntimeInstanceInfo &info)
{
    SandboxRequestedResources requested;
    const auto &resources = info.runtimeconfig().resources().resources();

    auto cpuIt = resources.find(resource_view::CPU_RESOURCE_NAME);
    double cpuMillicores = cpuIt != resources.end()
        ? Executor::GetEffectiveScalarLimit(cpuIt->second, DEFAULT_SANDBOX_CPU_MILLICORES)
        : DEFAULT_SANDBOX_CPU_MILLICORES;
    requested.cpuCores = cpuMillicores / CPU_MILLICORES_PER_CORE;

    auto memoryIt = resources.find(resource_view::MEMORY_RESOURCE_NAME);
    double memoryMb = memoryIt != resources.end()
        ? Executor::GetEffectiveScalarLimit(memoryIt->second, DEFAULT_SANDBOX_MEMORY_MB)
        : DEFAULT_SANDBOX_MEMORY_MB;
    requested.memoryBytes = memoryMb * BYTES_PER_MB;

    return requested;
}

functionsystem::metrics::LabelType BuildSandboxMetricLabels(const messages::RuntimeInstanceInfo &info,
                                                            const std::string &runtimeID, const std::string &sandboxID)
{
    return {
        { "instance_id", info.instanceid() },
        { "runtime_id", runtimeID },
        { "sandbox_id", sandboxID },
        { "sandbox_runtime", info.container().runtime() },
        { "rootfs_type", RootfsTypeToLabel(info.container().rootfsconfig().type()) },
        { "image", ResolveSandboxImage(info) },
    };
}

bool IsSandboxMetricsEnabled()
{
    static const bool enabled = []() {
        auto envOpt = litebus::os::GetEnv("YR_SANDBOX_METRICS_ENABLED");
        bool e = false;
        if (envOpt.IsSome()) {
            const auto &v = envOpt.Get();
            e = (v == "1" || v == "true" || v == "TRUE" || v == "True");
        }
        YRLOG_INFO("[sandbox-metrics] YR_SANDBOX_METRICS_ENABLED={} (default OFF; set to 1 to enable)",
                   e ? "true" : "false");
        return e;
    }();
    return enabled;
}

void ReportSandboxGauge(const functionsystem::metrics::MeterTitle &title,
                        const functionsystem::metrics::LabelType &labels, double value)
{
    if (!IsSandboxMetricsEnabled()) {
        return;
    }
    functionsystem::metrics::MeterData data{ value, labels };
    functionsystem::metrics::MetricsAdapter::GetInstance().ReportDoubleGauge(title, data, { "node_id", "ip" });
}

bool IsNormalSandboxExit(const runtime::v1::WaitResponse &response)
{
    return response.exit_code() == 0 && response.status() == 0;
}

}  // namespace

bool SandboxdExecutor::IsRetryableWaitError(const Status &status)
{
    const auto code = status.StatusCode();
    return code == GRPC_UNAVAILABLE || code == GRPC_CANCELLED || code == GRPC_DEADLINE_EXCEEDED
        || code == GRPC_INTERNAL;
}

uint32_t SandboxdExecutor::SandboxReclaimBackoffMs(uint32_t failedAttempts)
{
    uint64_t delayMs = kSandboxReclaimInitialBackoffMs;
    for (uint32_t attempt = 1; attempt < failedAttempts && delayMs < kSandboxReclaimMaxBackoffMs; ++attempt) {
        delayMs = std::min<uint64_t>(delayMs * 2, kSandboxReclaimMaxBackoffMs);
    }
    return static_cast<uint32_t>(delayMs);
}

void SandboxdExecutor::StartSandboxCreateSpan(const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    trace::StartSandboxCreateSpan(request);
}

void SandboxdExecutor::StopSandboxCreateSpan(const std::shared_ptr<messages::StartInstanceRequest> &request,
    const runtime::v1::StartResponse &response)
{
    trace::StopSandboxCreateSpan(request, response);
}

// ── Construction ──────────────────────────────────────────────────────────────

SandboxdExecutor::SandboxdExecutor(const std::string &name, const litebus::AID &functionAgentAID,
                                   const std::string &checkpointDir,
                                   AvailableRuntimesCallback availableRuntimesCallback)
    : Executor(name), functionAgentAID_(functionAgentAID),
      checkpointDir_(checkpointDir.empty() ? DEFAULT_CHECKPOINT_DIR : checkpointDir),
      availableRuntimesCallback_(std::move(availableRuntimesCallback))
{
    const std::string &dir = checkpointDir_;
    auto ckptActor = std::make_shared<CkptFileManagerActor>(name + "_CkptFileManager", dir);
    litebus::Spawn(ckptActor);
    ckptFileManager_ = std::make_shared<CkptFileManager>(ckptActor);
}

// ── Executor lifecycle ────────────────────────────────────────────────────────

void SandboxdExecutor::InitConfig()
{
    cmdBuilder_.SetRuntimeConfig(config_);
    auto ep = litebus::os::GetEnv("CONTAINER_EP");
    if (!ep.IsNone()) {
        const std::string endpoint = ep.Get();
        YRLOG_INFO("SandboxdExecutor: connecting to sandboxd at {}", endpoint);
        sandboxd_ = GrpcClient<runtime::v1::SandboxService>::CreateUdsGrpcClient(endpoint);
        synced_ = true;
        CheckConnectivity();
        FetchInitialAvailableRuntimes();
    } else {
        YRLOG_INFO("SandboxdExecutor: no sandboxd endpoint, executor disabled");
        if (availableRuntimesCallback_) {
            availableRuntimesCallback_(false, {});
        }
    }
    // ckptOrch_ MUST be created after sandboxd_ is set. Sync() runs after.
    ckptOrch_ = std::make_shared<SandboxdCheckpointOrchestrator>(GetAID(), sandboxd_, ckptFileManager_, stateManager_,
                                                                checkpointDir_);
    Sync();
}

void SandboxdExecutor::Init()
{
    // intentionally empty: real init happens in InitConfig() once sandboxd_ is ready
}

void SandboxdExecutor::Finalize()
{
    Executor::Finalize();
}

// ── StartInstance ─────────────────────────────────────────────────────────────

litebus::Future<messages::StartInstanceResponse> SandboxdExecutor::StartInstance(
    const std::shared_ptr<messages::StartInstanceRequest> &request, const std::vector<int> &cardIDs)
{
    const auto &info     = request->runtimeinstanceinfo();
    const auto &runtimeID = info.runtimeid();

    if (auto existing = stateManager_.GetInProgressFuture(runtimeID)) {
        YRLOG_INFO("{}|{}|StartInstance: dedup runtimeID({})", info.traceid(), info.requestid(), runtimeID);
        return *existing;
    }

    stateManager_.Register(SandboxInfo{runtimeID, {}, {}, {}, info});
    stateManager_.ClearPendingDelete(runtimeID);
    sandboxLifecycleStates_.erase(runtimeID);

    // Route by request shape: warm-up -> Register, restore -> Restore, else Start.
    const bool isWarmUp = info.warmuptype() != static_cast<int32_t>(WarmupType::NONE);
    if (!isWarmUp) {
        ReportSandboxLifecycleStatus(info, runtimeID, SandboxLifecycleStatus::CREATING);
    }

    std::string port;
    if (const auto &tls = info.runtimeconfig().tlsconfig(); tls.enableservermode()) {
        port = tls.posixport();
    }

    CommandArgs cmdArgs;
    auto buildStatus = BuildStartCommandArgs(request, port, &cmdArgs);
    if (buildStatus.IsError()) {
        return GenFailStartInstanceResponse(request, buildStatus.StatusCode(), buildStatus.GetMessage());
    }

    RuntimeFeatures features;
    features.cleanStreamProducerEnable = config_.cleanStreamProducerEnable;
    Envs envs = GenerateEnvs(config_, request, port, cardIDs, features);

    // Create a placeholder future for dedup; will be replaced by the real future
    litebus::Promise<messages::StartInstanceResponse> promise;
    auto guard = std::make_shared<SandboxdStartGuard>(stateManager_, runtimeID, promise.GetFuture());
    SandboxdStartContext context{request, cmdArgs, port, envs, cardIDs, guard};

    YRLOG_INFO("{}|{}|StartInstance: route to {}",
               info.traceid(), info.requestid(),
               isWarmUp ? "WarmUp" : (info.snapshotinfo().checkpointid().empty() ? "Normal" : "Restore"));

    litebus::Future<messages::StartInstanceResponse> future;
    if (isWarmUp) {
        future = StartWarmUp(request, cmdArgs, port, envs, guard);
    } else if (!info.snapshotinfo().checkpointid().empty()) {
        future = StartBySnapshot(context);
    } else {
        future = StartNormal(context);
    }
    future =
        future.Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnStartCompleted, runtimeID, std::placeholders::_1));
    stateManager_.MarkStartInProgress(runtimeID, future);
    return future;
}

Status SandboxdExecutor::BuildStartCommandArgs(const std::shared_ptr<messages::StartInstanceRequest> &request,
                                               const std::string &port, CommandArgs *cmdArgs)
{
    const auto &info = request->runtimeinstanceinfo();
    if (HasSelfContainedSandboxBootstrap(request)) {
        YRLOG_DEBUG("{}|{}|StartInstance: using self-contained bootstrap command without runtime args", info.traceid(),
                    info.requestid());
        return Status::OK();
    }

    auto [buildStatus, builtCmdArgs] = cmdBuilder_.BuildArgs(ResolveRuntimeLanguage(info), port, *request);
    if (buildStatus.IsOk()) {
        *cmdArgs = std::move(builtCmdArgs);
        return Status::OK();
    }

    YRLOG_ERROR("{}|{}|BuildArgs failed for instanceID({}): {}", info.traceid(), info.requestid(), info.instanceid(),
                buildStatus.RawMessage());
    ReportSandboxLifecycleStatus(info, info.runtimeid(), SandboxLifecycleStatus::ABNORMAL);
    stateManager_.Unregister(info.runtimeid());
    return buildStatus;
}

litebus::Future<messages::StartInstanceResponse> SandboxdExecutor::OnStartCompleted(
    const std::string &runtimeID, const messages::StartInstanceResponse &response)
{
    stateManager_.MarkStartDone(runtimeID);
    if (!stateManager_.IsPendingDelete(runtimeID)) {
        return response;
    }
    YRLOG_INFO("runtime({}) started but has pending delete; cleaning up", runtimeID);
    stateManager_.ClearPendingDelete(runtimeID);
    if (response.code() == static_cast<int32_t>(StatusCode::SUCCESS)) {
        auto stopReq = std::make_shared<messages::StopInstanceRequest>();
        stopReq->set_runtimeid(runtimeID);
        StopInstance(stopReq, false);
    }
    messages::StartInstanceResponse newRsp = response;
    newRsp.set_requestid(response.requestid());
    return newRsp;
}

// ── Warm-up path (Register a reusable SandboxTemplate) ───────────────────────

litebus::Future<messages::StartInstanceResponse> SandboxdExecutor::StartWarmUp(
    const std::shared_ptr<messages::StartInstanceRequest> &request, const CommandArgs &cmdArgs, const std::string &port,
    const Envs &envs, std::shared_ptr<SandboxdStartGuard> guard)
{
    const auto &info      = request->runtimeinstanceinfo();
    const auto &runtimeID = info.runtimeid();
    const auto combineEnvs = cmdBuilder_.CombineEnvs(envs);

    auto registerReq = std::make_shared<runtime::v1::RegisterRequest>();
    auto *tmpl = registerReq->add_templates();
    tmpl->set_id(runtimeID);
    tmpl->set_runtime(info.container().runtime());
    *tmpl->mutable_rootfs() = info.container().rootfsconfig();
    tmpl->set_make_seed(info.warmuptype() == static_cast<int32_t>(WarmupType::SEED));

    // Working root mount
    SandboxdRequestBuilder builder{cmdBuilder_};
    std::string workingRoot;
    builder.ApplyBootstrapMount(request, tmpl->mutable_mounts(), workingRoot);
    (*tmpl->mutable_envs())["YR_RT_WORKING_DIR"] = workingRoot;

    for (const auto &cmd : BuildBootstrapCommands(request)) {
        *tmpl->add_command() = cmd;
    }
    for (const auto &arg : cmdArgs.args) {
        *tmpl->add_command() = arg;
    }
    tmpl->mutable_envs()->insert(combineEnvs.begin(), combineEnvs.end());
    if (auto env = litebus::os::GetEnv("YR_ENV_FILE"); env.IsSome()) {
        (*tmpl->mutable_envs())["YR_ENV_FILE"] = env.Get();
    }
    if (auto ready = litebus::os::GetEnv("YR_SEED_FILE"); ready.IsSome()) {
        (*tmpl->mutable_envs())["YR_SEED_FILE"] = ready.Get();
    }
    (*tmpl->mutable_envs())[YR_ONLY_STDOUT] = "true";

    // YR_LANGUAGE follows the service runtime field. The container runtime is
    // the sandbox backend (for example runc/runsc), not the user runtime.
    (*tmpl->mutable_envs())["YR_LANGUAGE"] = ResolveRuntimeLanguage(info);

    return DoRegister(registerReq)
        .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnWarmUpRegistered, std::placeholders::_1, request, guard));
}

litebus::Future<messages::StartInstanceResponse> SandboxdExecutor::OnWarmUpRegistered(
    const runtime::v1::NormalResponse &response, const std::shared_ptr<messages::StartInstanceRequest> &request,
    std::shared_ptr<SandboxdStartGuard> guard)
{
    const auto &info      = request->runtimeinstanceinfo();
    const auto &runtimeID = info.runtimeid();
    if (!response.success()) {
        return GenFailStartInstanceResponse(
            request, RUNTIME_MANAGER_WARMUP_FAILURE,
            fmt::format("warmup register failed for instance({}): {}", info.instanceid(), response.message()));
    }
    warmupRuntimes_.insert(runtimeID);
    registeredTemplateIDs_.insert(runtimeID);
    guard->Commit();
    messages::StartInstanceResponse rsp;
    rsp.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    rsp.set_requestid(info.requestid());
    rsp.mutable_startruntimeinstanceresponse()->set_runtimeid(runtimeID);
    return rsp;
}

// ── Restore path (download checkpoint -> add ref -> Restore RPC) ─────────────

litebus::Future<messages::StartInstanceResponse> SandboxdExecutor::StartBySnapshot(const SandboxdStartContext &context)
{
    const auto &request = context.request;
    const auto &info         = request->runtimeinstanceinfo();
    const auto &snapshotInfo = info.snapshotinfo();

    YRLOG_INFO("{}|{}|StartBySnapshot: instance({}) runtime({}) checkpoint({})", info.traceid(), info.requestid(),
               info.instanceid(), info.runtimeid(), snapshotInfo.checkpointid());

    ASSERT_IF_NULL(ckptOrch_);
    return ckptOrch_
        ->DownloadForRestore(snapshotInfo.checkpointid(), snapshotInfo.storage(), info.requestid(),
                             snapshotInfo.sha256(), snapshotInfo.size())
        .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnCheckpointDownloaded, std::placeholders::_1, context));
}

litebus::Future<messages::StartInstanceResponse> SandboxdExecutor::OnCheckpointDownloaded(
    const std::string &checkpointPath, const SandboxdStartContext &context)
{
    const auto &request = context.request;
    const auto &info         = request->runtimeinstanceinfo();
    const auto &runtimeID    = info.runtimeid();
    const auto &checkpointID = info.snapshotinfo().checkpointid();
    SandboxdRestoreContext restoreContext{checkpointPath, context};

    return ckptOrch_->AddRef(checkpointID, runtimeID, info.requestid())
        .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnCheckpointRefAdded, std::placeholders::_1,
                             restoreContext));
}

litebus::Future<messages::StartInstanceResponse> SandboxdExecutor::OnCheckpointRefAdded(
    const Status &refStatus, const SandboxdRestoreContext &context)
{
    const auto &request = context.start.request;
    const auto &info = request->runtimeinstanceinfo();
    if (refStatus.IsError()) {
        ReportSandboxLifecycleStatus(info, info.runtimeid(), SandboxLifecycleStatus::ABNORMAL);
        return GenFailStartInstanceResponse(request, StatusCode::RUNTIME_MANAGER_CHECKPOINT_FAILED,
                                            "add checkpoint reference failed: " + refStatus.RawMessage());
    }

    SandboxdRequestBuilder builder{cmdBuilder_};
    SandboxdStartParams params;
    params.request   = request;
    params.cmdArgs   = context.start.cmdArgs;
    params.envs      = context.start.envs;
    params.runtimeID = info.runtimeid();
    params.registeredTemplateIDs = registeredTemplateIDs_;

    auto [status, startReq] = builder.Build(params);
    if (!status.IsOk()) {
        ReportSandboxLifecycleStatus(info, info.runtimeid(), SandboxLifecycleStatus::ABNORMAL);
        // Compensate: remove the reference we just added
        ckptOrch_->ReleaseRef(info.runtimeid(), info.requestid());
        return GenFailStartInstanceResponse(request, status.StatusCode(), status.RawMessage());
    }

    auto restoreReq = std::make_shared<runtime::v1::RestoreRequest>();
    *restoreReq->mutable_sandbox() = *startReq;
    restoreReq->set_checkpoint_dir(context.checkpointPath);
    // 回填身份/完整性，激活 sandboxd PR#16 的 identity-checked restore。
    // 分派契约是 all-or-nothing：checkpoint_id/sha/size 三字段全空走 legacy
    // dir 路径，任一非空则严格路径要求全部非空且 config.sandbox_id 非空
    // （sandboxd ReserveID 尊重调用方 id）——所以四个条件整体满足才填，
    // 半填会被直接拒绝（实测 80004 invalid argument）。sandbox_id 用
    // SnapshotInfo 持久化的源沙箱 id（park 会注销 stateManager 映射，运行
    // 时查不到），与 PR#16 的 replay 去重设计（同 id + 同 restore identity
    // 幂等返回）对齐；任一条件缺失时整体留空退回 legacy 语义。
    const auto &snapshotInfo = info.snapshotinfo();
    if (!snapshotInfo.sandbox_id().empty() && !snapshotInfo.sha256().empty() && snapshotInfo.size() > 0) {
        restoreReq->mutable_sandbox()->set_sandbox_id(snapshotInfo.sandbox_id());
        restoreReq->set_checkpoint_id(snapshotInfo.checkpointid());
        restoreReq->set_expected_sha256(snapshotInfo.sha256());
        restoreReq->set_expected_size(snapshotInfo.size());
    }

    StartSandboxCreateSpan(request);
    return DoRestore(request, restoreReq)
        .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnRestoreDone, std::placeholders::_1, request,
                             context.start.guard));
}

litebus::Future<messages::StartInstanceResponse> SandboxdExecutor::OnRestoreDone(
    const runtime::v1::RestoreResponse &response, const std::shared_ptr<messages::StartInstanceRequest> &request,
    std::shared_ptr<SandboxdStartGuard> guard)
{
    const auto &info = request->runtimeinstanceinfo();
    if (response.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
        YRLOG_ERROR("{}|{}|restore failed for runtime({}): {}", info.traceid(), info.requestid(), info.runtimeid(),
                    response.message());
        ReportSandboxLifecycleStatus(info, info.runtimeid(), SandboxLifecycleStatus::ABNORMAL);
        // Compensate: release the checkpoint ref we added before restore
        ckptOrch_->ReleaseRef(info.runtimeid(), info.requestid());
        // guard destructor rolls back state
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_CREATE_EXEC_FAILED, response.message());
    }
    const std::string sandboxID = response.id();
    stateManager_.UpdateSandboxID(info.runtimeid(), sandboxID);
    guard->Commit();
    sandboxStatsPollingRuntimes_.insert(info.runtimeid());
    CollectSandboxStats(info.runtimeid(), sandboxID);
    DoWaitWithRetry(sandboxID, info.runtimeid(), 0);
    ReportMetrics(info.instanceid(), info.runtimeid(), sandboxID,
                  {"yr_app_instance_start_time", " start timestamp", "ms"});
    YRLOG_INFO("{}|{}|restore success: instance({}) runtime({}) sandbox({})", info.traceid(), info.requestid(),
               info.instanceid(), info.runtimeid(), sandboxID);
    return MakeSuccessStartResponse(request, sandboxID);
}

// ── Start path ───────────────────────────────────────────────────────────────

litebus::Future<messages::StartInstanceResponse> SandboxdExecutor::StartNormal(
    const SandboxdStartContext &context)
{
    const auto &request = context.request;
    SandboxdRequestBuilder builder{cmdBuilder_};
    SandboxdStartParams params;
    params.request   = request;
    params.cmdArgs   = context.cmdArgs;
    params.envs      = context.envs;
    params.runtimeID = request->runtimeinstanceinfo().runtimeid();
    params.registeredTemplateIDs = registeredTemplateIDs_;
    const auto portStatus = ApplyPortForwardMappings(&params, request);
    if (portStatus.IsError()) {
        ReportSandboxLifecycleStatus(request->runtimeinstanceinfo(), params.runtimeID,
                                     SandboxLifecycleStatus::ABNORMAL);
        stateManager_.UpdatePortMappings(params.runtimeID, "");
        PortManager::GetInstance().ReleasePorts(params.runtimeID);
        return GenFailStartInstanceResponse(request, portStatus.StatusCode(), portStatus.RawMessage());
    }

    auto [status, startReq] = builder.Build(params);
    if (!status.IsOk()) {
        ReportSandboxLifecycleStatus(request->runtimeinstanceinfo(), params.runtimeID,
                                     SandboxLifecycleStatus::ABNORMAL);
        stateManager_.UpdatePortMappings(params.runtimeID, "");
        PortManager::GetInstance().ReleasePorts(params.runtimeID);
        return GenFailStartInstanceResponse(request, status.StatusCode(), status.RawMessage());
    }
    StartSandboxCreateSpan(request);
    return DoStart(request, startReq)
        .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnStartDone, std::placeholders::_1, request, context.guard));
}

Status SandboxdExecutor::ApplyPortForwardMappings(SandboxdStartParams *params,
    const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    const auto &deployOpts = request->runtimeinstanceinfo().deploymentconfig().deployoptions();
    auto networkIt = deployOpts.find(CONTAINER_NETWORK);
    if (networkIt == deployOpts.end()) {
        return Status::OK();
    }
    const auto forwardConfigs = ParseForwardPorts(networkIt->second);
    if (forwardConfigs.empty()) {
        return Status::OK();
    }
    auto hostPorts =
        PortManager::GetInstance().RequestPorts(params->runtimeID, static_cast<int>(forwardConfigs.size()));
    if (hostPorts.size() != forwardConfigs.size()) {
        return Status(StatusCode::RUNTIME_MANAGER_PORT_UNAVAILABLE,
                      "failed to allocate all requested port mappings for runtime " + params->runtimeID);
    }
    json portJson = json::array();
    for (size_t i = 0; i < forwardConfigs.size(); ++i) {
        const std::string hostPort = std::to_string(hostPorts[i]);
        const std::string containerPort = std::to_string(forwardConfigs[i].containerPort);
        const std::string &scheme = forwardConfigs[i].protocol;
        params->portMappings.push_back(ToDownstreamL4Protocol(scheme) + ":" + hostPort + ":" + containerPort);
        portJson.push_back(FormatPortForwardMapping({
            forwardConfigs[i].routeKind, scheme, static_cast<uint16_t>(hostPorts[i]),
            static_cast<uint16_t>(forwardConfigs[i].containerPort), false}));
    }
    stateManager_.UpdatePortMappings(params->runtimeID, portJson.dump());
    return Status::OK();
}

litebus::Future<messages::StartInstanceResponse> SandboxdExecutor::OnStartDone(
    const runtime::v1::StartResponse &response, const std::shared_ptr<messages::StartInstanceRequest> &request,
    std::shared_ptr<SandboxdStartGuard> guard)
{
    const auto &info      = request->runtimeinstanceinfo();
    const auto &runtimeID = info.runtimeid();
    StopSandboxCreateSpan(request, response);

    if (response.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
        YRLOG_ERROR("{}|{}|StartNormal failed for instance({}) runtime({}): {}", info.traceid(), info.requestid(),
                    info.instanceid(), runtimeID, response.message());
        ReportSandboxLifecycleStatus(info, runtimeID, SandboxLifecycleStatus::ABNORMAL);
        return GenFailStartInstanceResponse(request, RUNTIME_MANAGER_CREATE_EXEC_FAILED, response.message());
    }

    const std::string sandboxID = response.id();
    stateManager_.UpdateSandboxID(runtimeID, sandboxID);
    guard->Commit();

    sandboxStatsPollingRuntimes_.insert(runtimeID);
    CollectSandboxStats(runtimeID, sandboxID);
    DoWaitWithRetry(sandboxID, runtimeID, 0);
    ReportMetrics(info.instanceid(), runtimeID, sandboxID, { "yr_app_instance_start_time", " start timestamp", "ms" });
    YRLOG_INFO("{}|{}|StartNormal success: instance({}) runtime({}) sandbox({})", info.traceid(), info.requestid(),
               info.instanceid(), runtimeID, sandboxID);
    return MakeSuccessStartResponse(request, sandboxID);
}

// ── StopInstance ──────────────────────────────────────────────────────────────

litebus::Future<Status> SandboxdExecutor::StopInstance(const std::shared_ptr<messages::StopInstanceRequest> &request,
                                                       bool oomKilled)
{
    const std::string &runtimeID = request->runtimeid();
    const std::string &requestID = request->requestid();

    // Warm-up runtimes are teardown via the Unregister RPC, not Delete.
    if (warmupRuntimes_.count(runtimeID) > 0) {
        return UnregisterWarmUp(runtimeID, requestID);
    }
    return StopSandbox(runtimeID, requestID, oomKilled);
}

litebus::Future<Status> SandboxdExecutor::StopSandbox(const std::string &runtimeID, const std::string &requestID,
                                                      bool oomKilled)
{
    if (stateManager_.IsStartInProgress(runtimeID)) {
        YRLOG_INFO("{}|runtime({}) start in progress, marking pending delete", requestID, runtimeID);
        stateManager_.MarkPendingDelete(runtimeID);
        return Status::OK();
    }

    const auto sandboxID = stateManager_.GetSandboxID(runtimeID);
    if (sandboxID.empty()) {
        YRLOG_WARN("{}|runtime({}) not found in state manager", requestID, runtimeID);
        return Status::OK();
    }

    // Release checkpoint reference (no-op if this sandbox was not restored)
    if (ckptOrch_ != nullptr) {
        ckptOrch_->ReleaseRef(runtimeID, requestID);
    }

    return TerminateSandbox(runtimeID, requestID, sandboxID, oomKilled);
}

litebus::Future<Status> SandboxdExecutor::TerminateSandbox(const std::string &runtimeID, const std::string &requestID,
                                                           const std::string &sandboxID, bool force)
{
    auto reclaimIt = sandboxReclaims_.find(runtimeID);
    if (reclaimIt != sandboxReclaims_.end() && reclaimIt->second.sandboxID == sandboxID) {
        YRLOG_INFO("{}|sandbox({}) runtime({}) is already being reclaimed locally; joining the retrying delete",
                   requestID, sandboxID, runtimeID);
        return reclaimIt->second.completion->GetFuture();
    }

    int64_t timeout = DEFAULT_GRACEFUL_SHUTDOWN;
    if (auto info = stateManager_.Find(runtimeID)) {
        timeout = info->instanceInfo.gracefulshutdowntime();
    }
    auto del = std::make_shared<runtime::v1::DeleteRequest>();
    del->set_id(sandboxID);
    del->set_timeout(force ? 0 : timeout);
    YRLOG_INFO("{}|terminating sandbox({}) runtime({})", requestID, sandboxID, runtimeID);
    userInitiatedTerminateRuntimes_.insert(runtimeID);
    return DoDelete("", runtimeID, requestID, del)
        .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnDeleteDone, runtimeID, requestID, sandboxID,
                             std::placeholders::_1));
}

litebus::Future<Status> SandboxdExecutor::OnDeleteDone(const std::string &runtimeID, const std::string &requestID,
    const std::string &sandboxID,
    const runtime::v1::DeleteResponse & /* response */)
{
    YRLOG_INFO("{}|sandbox({}) deleted for runtime({})", requestID, sandboxID, runtimeID);

    PortManager::GetInstance().ReleasePorts(runtimeID);

    if (auto info = stateManager_.Find(runtimeID)) {
        auto lifecycleIt = sandboxLifecycleStates_.find(runtimeID);
        if (lifecycleIt == sandboxLifecycleStates_.end() || lifecycleIt->second != SandboxLifecycleStatus::ABNORMAL) {
            ReportSandboxLifecycleStatus(info->instanceInfo, runtimeID, SandboxLifecycleStatus::COMPLETED);
        }
        ReportMetrics(info->instanceInfo.instanceid(), runtimeID, sandboxID,
                      {"yr_instance_stop_time", "stop timestamp", "num"});
    }

    ClearSandboxMetricsState(runtimeID);
    sandboxLifecycleStates_.erase(runtimeID);
    stateManager_.Unregister(runtimeID);

    auto reclaimIt = sandboxReclaims_.find(runtimeID);
    if (reclaimIt != sandboxReclaims_.end() && reclaimIt->second.sandboxID == sandboxID) {
        reclaimIt->second.completion->SetValue(Status::OK());
        sandboxReclaims_.erase(reclaimIt);
    }
    return Status::OK();
}

// ── Warm-up teardown (Unregister RPC) ────────────────────────────────────────

litebus::Future<Status> SandboxdExecutor::UnregisterWarmUp(const std::string &runtimeID, const std::string &requestID)
{
    auto unReg = std::make_shared<runtime::v1::UnregisterRequest>();
    *unReg->add_ids() = runtimeID;
    YRLOG_INFO("{}|unregistering warm-up runtime({})", requestID, runtimeID);
    return DoUnregister(unReg).Then(
        litebus::Defer(GetAID(), &SandboxdExecutor::OnWarmUpUnregistered, std::placeholders::_1, runtimeID, requestID));
}

litebus::Future<Status> SandboxdExecutor::OnWarmUpUnregistered(const runtime::v1::NormalResponse &response,
                                                               const std::string &runtimeID,
                                                               const std::string &requestID)
{
    if (!response.success()) {
        YRLOG_ERROR("{}|unregister warm-up failed for ({})", requestID, runtimeID);
        return Status(StatusCode::RUNTIME_MANAGER_WARMUP_FAILURE);
    }
    warmupRuntimes_.erase(runtimeID);
    registeredTemplateIDs_.erase(runtimeID);
    YRLOG_INFO("{}|warm-up unregistered for ({})", requestID, runtimeID);
    return Status::OK();
}

// ── SnapshotRuntime (sandboxd Checkpoint RPC via the orchestrator) ────────────

litebus::Future<messages::SnapshotRuntimeResponse> SandboxdExecutor::SnapshotRuntime(
    const std::shared_ptr<messages::SnapshotRuntimeRequest> &request)
{
    ASSERT_IF_NULL(ckptOrch_);
    return ckptOrch_->TakeSnapshot(request);
}

// ── Other Executor interface methods ─────────────────────────────────────────

litebus::Future<bool> SandboxdExecutor::StopAllSandboxes()
{
    std::list<litebus::Future<Status>> futures;
    for (const auto &[runtimeID, info] : stateManager_.GetAllSandboxes()) {
        if (!info.sandboxID.empty()) {
            futures.emplace_back(TerminateSandbox(runtimeID, "", info.sandboxID, false));
        }
    }
    return CollectStatus(futures, "").Then([]() -> litebus::Future<bool> { return true; });
}

std::map<std::string, messages::RuntimeInstanceInfo> SandboxdExecutor::GetRuntimeInstanceInfos()
{
    return stateManager_.GetAllInstanceInfos();
}

bool SandboxdExecutor::IsRuntimeActive(const std::string &runtimeID)
{
    return stateManager_.IsActive(runtimeID);
}

std::shared_ptr<litebus::Exec> SandboxdExecutor::GetExecByRuntimeID(const std::string &runtimeID)
{
    if (auto it = runtime2Exec_.find(runtimeID); it != runtime2Exec_.end()) {
        return it->second;
    }
    if (stateManager_.IsActive(runtimeID)) {
        YRLOG_INFO("GetExecByRuntimeID: runtime({}) found in stateManager (reconciled), no local exec", runtimeID);
        return nullptr;
    }
    YRLOG_ERROR("can not find exec by runtimeID: {}", runtimeID);
    return nullptr;
}

litebus::Future<messages::UpdateCredResponse> SandboxdExecutor::UpdateCredForRuntime(
    const std::shared_ptr<messages::UpdateCredRequest> &request)
{
    messages::UpdateCredResponse response;
    response.set_requestid(request->requestid());
    if (!stateManager_.IsActive(request->runtimeid())) {
        YRLOG_WARN("{}|runtime({}) not found for UpdateCred", request->requestid(), request->runtimeid());
    }
    response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    return response;
}

litebus::Future<Status> SandboxdExecutor::NotifyInstancesDiskUsageExceedLimit(const std::string & /* description */,
                                                                              const int /* limit */)
{
    return Status::OK();
}

// ── Runtime reconciliation ────────────────────────────────────────────────────

litebus::Future<runtime::v1::ListSandboxesResponse> SandboxdExecutor::DoList()
{
    ASSERT_IF_NULL(sandboxd_);
    auto req = std::make_shared<runtime::v1::ListSandboxesRequest>();
    auto resp = std::make_shared<runtime::v1::ListSandboxesResponse>();
    return sandboxd_->CallAsyncX("List", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncList)
        .Then([resp](const Status &status) -> litebus::Future<runtime::v1::ListSandboxesResponse> {
            if (status.IsOk()) {
                return *resp;
            }
            YRLOG_ERROR("List gRPC failed: {}", status.RawMessage());
            return runtime::v1::ListSandboxesResponse{};
        });
}

litebus::Future<messages::ReconcileRuntimesResponse> SandboxdExecutor::ReconcileRuntimes(
    const std::shared_ptr<messages::ReconcileRuntimesRequest> &request)
{
    if (!request) {
        messages::ReconcileRuntimesResponse resp;
        resp.set_code(static_cast<int32_t>(StatusCode::PARAMETER_ERROR));
        resp.set_message("request is null");
        return resp;
    }
    if (!sandboxd_) {
        messages::ReconcileRuntimesResponse resp;
        resp.set_requestid(request->requestid());
        resp.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        return resp;
    }

    auto promise = std::make_shared<litebus::Promise<messages::ReconcileRuntimesResponse>>();
    WaitAndReconcile(request, 0, promise);
    return promise->GetFuture();
}

void SandboxdExecutor::WaitAndReconcile(
    const std::shared_ptr<messages::ReconcileRuntimesRequest> &request, int32_t retryCount,
    const std::shared_ptr<litebus::Promise<messages::ReconcileRuntimesResponse>> &promise)
{
    if (!sandboxd_->IsConnected()) {
        if (retryCount >= RECONCILE_MAX_RETRIES) {
            YRLOG_ERROR("{}|ReconcileRuntimes: sandboxd still not connected after {} retries, returning error",
                        request->requestid(), retryCount);
            messages::ReconcileRuntimesResponse errResp;
            errResp.set_requestid(request->requestid());
            errResp.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
            errResp.set_message("sandboxd not connected after max retries");
            promise->SetValue(errResp);
            return;
        }

        YRLOG_INFO("{}|ReconcileRuntimes: sandboxd not connected yet, retry {}/{}", request->requestid(),
                   retryCount + 1, RECONCILE_MAX_RETRIES);
        int64_t delayMs = RECONCILE_RETRY_INITIAL_MS << std::min(retryCount, 6);
        if (delayMs > RECONCILE_RETRY_MAX_INTERVAL_MS || delayMs <= 0) {
            delayMs = RECONCILE_RETRY_MAX_INTERVAL_MS;
        }
        litebus::AsyncAfter(delayMs, GetAID(), &SandboxdExecutor::WaitAndReconcile, request, retryCount + 1, promise);
        return;
    }

    auto req = std::make_shared<runtime::v1::ListSandboxesRequest>();
    auto resp = std::make_shared<runtime::v1::ListSandboxesResponse>();
    auto resultFuture = sandboxd_->CallAsyncX("List", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncList)
                            .Then([aid = GetAID(), request,
                                   resp](const Status &status) -> litebus::Future<messages::ReconcileRuntimesResponse> {
            if (!status.IsOk()) {
                                    YRLOG_ERROR("{}|ReconcileRuntimes: DoList gRPC failed: {}", request->requestid(),
                                                status.RawMessage());
                messages::ReconcileRuntimesResponse errResp;
                errResp.set_requestid(request->requestid());
                errResp.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
                errResp.set_message("DoList gRPC failed: " + status.RawMessage());
                return errResp;
            }
            return litebus::Async(aid, &SandboxdExecutor::OnReconcileRuntimes, request, resp);
        });
    promise->Associate(resultFuture);
}

messages::ReconcileRuntimesResponse SandboxdExecutor::OnReconcileRuntimes(
    const std::shared_ptr<messages::ReconcileRuntimesRequest> &request,
    const std::shared_ptr<runtime::v1::ListSandboxesResponse> &listResp)
{
    messages::ReconcileRuntimesResponse response;
    response.set_requestid(request->requestid());
    response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));

    std::unordered_set<std::string> expectedIDs;
    for (const auto &entry : request->entries()) {
        if (!entry.containerid().empty()) {
            expectedIDs.insert(entry.containerid());
        }
    }

    std::unordered_set<std::string> actualRunningIDs;
    for (const auto &sandbox : listResp->sandboxes()) {
        if (sandbox.state() == runtime::v1::SANDBOX_STATE_RUNNING) {
            actualRunningIDs.insert(sandbox.id());
        }
    }

    auto now = std::chrono::steady_clock::now();
    int32_t orphansCleaned = 0;

    CleanupExitedSandboxes(request->requestid(), listResp, &response, &orphansCleaned);
    CleanupOrphanSandboxes(ReconcileCleanupContext{ request->requestid(), listResp, expectedIDs, now, &response,
                                                    &orphansCleaned, &actualRunningIDs });
    AddMissingAndConfirmedEntries(request, actualRunningIDs, &response);
    PurgeOrphanTracking(actualRunningIDs);

    response.set_orphanscleaned(orphansCleaned);
    YRLOG_INFO("{}|ReconcileRuntimes: {} orphans cleaned, {} missing, {} confirmed", request->requestid(),
               orphansCleaned, response.missingids_size(), response.confirmedentries_size());
    return response;
}

void SandboxdExecutor::CleanupExitedSandboxes(const std::string &requestID,
                                              const std::shared_ptr<runtime::v1::ListSandboxesResponse> &listResp,
                                              messages::ReconcileRuntimesResponse *response, int32_t *orphansCleaned)
{
    for (const auto &sandbox : listResp->sandboxes()) {
        if (sandbox.state() != runtime::v1::SANDBOX_STATE_EXITED) {
            continue;
        }
        const auto &sandboxID = sandbox.id();
        YRLOG_INFO("{}|ReconcileRuntimes: deleting exited sandbox {}", requestID, sandboxID);
        DeleteSandboxAsync(sandboxID);
        orphanFirstSeen_.erase(sandboxID);
        response->add_orphanids(sandboxID);
        ++(*orphansCleaned);
    }
}

void SandboxdExecutor::CleanupOrphanSandboxes(const ReconcileCleanupContext &context)
{
    for (const auto &sandbox : context.listResp->sandboxes()) {
        if (sandbox.state() != runtime::v1::SANDBOX_STATE_RUNNING) {
            continue;
        }
        const auto &sandboxID = sandbox.id();

        if (context.expectedIDs.count(sandboxID) > 0) {
            if (orphanFirstSeen_.erase(sandboxID)) {
                YRLOG_INFO("{}|ReconcileRuntimes: orphan timer cleared for {} (re-appeared in expected)",
                           context.requestID, sandboxID);
            }
            continue;
        }

        auto it = orphanFirstSeen_.find(sandboxID);
        if (it == orphanFirstSeen_.end()) {
            orphanFirstSeen_.emplace(sandboxID, context.now);
            YRLOG_INFO("{}|ReconcileRuntimes: orphan candidate sandbox {} (first seen)", context.requestID, sandboxID);
            continue;
        }

        auto elapsedSec = std::chrono::duration_cast<std::chrono::seconds>(context.now - it->second).count();
        if (elapsedSec < static_cast<int64_t>(orphanGracePeriodSec_)) {
            continue;
        }

        YRLOG_INFO("{}|ReconcileRuntimes: deleting orphan sandbox {} (orphan for {}s)", context.requestID, sandboxID,
                   elapsedSec);
        CleanupLocalRuntimeStateForOrphan(context.requestID, sandboxID);
        DeleteSandboxAsync(sandboxID);
        orphanFirstSeen_.erase(it);
        context.actualRunningIDs->erase(sandboxID);
        context.response->add_orphanids(sandboxID);
        ++(*context.orphansCleaned);
    }
}

void SandboxdExecutor::AddMissingAndConfirmedEntries(const std::shared_ptr<messages::ReconcileRuntimesRequest> &request,
    const std::unordered_set<std::string> &actualRunningIDs,
    messages::ReconcileRuntimesResponse *response)
{
    for (const auto &entry : request->entries()) {
        if (!entry.containerid().empty() && actualRunningIDs.count(entry.containerid()) == 0) {
            response->add_missingids(entry.containerid());
        }
    }

    std::unordered_set<std::string> missingSet;
    for (const auto &id : response->missingids()) {
        missingSet.insert(id);
    }
    for (const auto &entry : request->entries()) {
        if (entry.containerid().empty() || actualRunningIDs.count(entry.containerid()) == 0
            || missingSet.count(entry.containerid()) > 0) {
            continue;
        }

        std::vector<int> hostPorts;
        auto restoreStatus = ParseReconcileHostPorts(entry, &hostPorts);
        if (restoreStatus.IsOk()) {
            restoreStatus = PortManager::GetInstance().ReservePorts(entry.runtimeid(), hostPorts);
        }
        if (restoreStatus.IsError()) {
            YRLOG_ERROR("{}|ReconcileRuntimes: failed to restore port reservations for runtime({}): {}",
                        request->requestid(), entry.runtimeid(), restoreStatus.RawMessage());
            response->set_code(static_cast<int32_t>(restoreStatus.StatusCode()));
            response->set_message(restoreStatus.RawMessage());
            response->clear_confirmedentries();
            return;
        }

        auto *confirmed = response->add_confirmedentries();
        *confirmed = entry;
        if (!stateManager_.IsActive(entry.runtimeid())) {
            messages::RuntimeInstanceInfo instanceInfo;
            instanceInfo.set_instanceid(entry.instanceid());
            instanceInfo.set_runtimeid(entry.runtimeid());
            stateManager_.Register(
                {entry.runtimeid(), entry.containerid(), {}, entry.portmappings(), instanceInfo});
            stateManager_.MarkStartDone(entry.runtimeid());
            DoWaitWithRetry(entry.containerid(), entry.runtimeid(), 0);
        } else {
            stateManager_.UpdatePortMappings(entry.runtimeid(), entry.portmappings());
        }
    }
}

void SandboxdExecutor::CleanupLocalRuntimeStateForOrphan(const std::string &requestID, const std::string &sandboxID)
{
    const auto runtimeID = stateManager_.FindRuntimeIDBySandboxID(sandboxID);
    if (runtimeID.empty()) {
        return;
    }

    YRLOG_WARN(
        "{}|ReconcileRuntimes: orphan sandbox {} is still registered as runtime({}); "
        "releasing local runtime resources before orphan delete",
        requestID, sandboxID, runtimeID);

    if (ckptOrch_ != nullptr) {
        ckptOrch_->ReleaseRef(runtimeID, requestID);
    }
    PortManager::GetInstance().ReleasePorts(runtimeID);
    ClearSandboxMetricsState(runtimeID);
    sandboxLifecycleStates_.erase(runtimeID);
    stateManager_.Unregister(runtimeID);
}

void SandboxdExecutor::PurgeOrphanTracking(const std::unordered_set<std::string> &actualRunningIDs)
{
    for (auto it = orphanFirstSeen_.begin(); it != orphanFirstSeen_.end();) {
        if (actualRunningIDs.count(it->first) == 0) {
            it = orphanFirstSeen_.erase(it);
        } else {
            ++it;
        }
    }
}

void SandboxdExecutor::DeleteSandboxAsync(const std::string &sandboxID)
{
    auto deleteReq = std::make_shared<runtime::v1::DeleteRequest>();
    deleteReq->set_id(sandboxID);
    deleteReq->set_timeout(CONTAINER_DELETE_TIMEOUT_SEC);
    sandboxd_
        ->CallAsync("Delete", *deleteReq, static_cast<runtime::v1::DeleteResponse *>(nullptr),
                    &runtime::v1::SandboxService::Stub::AsyncDelete)
        .Then([aid(GetAID()), sandboxID](litebus::Try<runtime::v1::DeleteResponse> rsp) -> litebus::Future<Status> {
            return litebus::Async(aid, &SandboxdExecutor::OnDeleteSandboxComplete, sandboxID, rsp);
        });
}

Status SandboxdExecutor::OnDeleteSandboxComplete(const std::string &sandboxID,
    litebus::Try<runtime::v1::DeleteResponse> rsp)
{
    if (rsp.IsOK()) {
        YRLOG_INFO("DeleteSandboxAsync: sandbox({}) deleted", sandboxID);
        return Status::OK();
    }
    YRLOG_ERROR("DeleteSandboxAsync: sandbox({}) delete failed: {}, scheduling retry in ~{}s", sandboxID,
                rsp.GetErrorCode(), kOrphanDeleteRetryIntervalSec);
    auto retryFirstSeen = std::chrono::steady_clock::now() -
                          std::chrono::seconds(static_cast<int64_t>(orphanGracePeriodSec_)) +
                          std::chrono::seconds(static_cast<int64_t>(kOrphanDeleteRetryIntervalSec));
    orphanFirstSeen_.emplace(sandboxID, retryFirstSeen);
    return Status(StatusCode::ERR_INNER_SYSTEM_ERROR);
}

// ── Connectivity ──────────────────────────────────────────────────────────────

void SandboxdExecutor::CheckConnectivity()
{
    litebus::AsyncAfter(RECONNECT_INTERVAL_MS, GetAID(), &SandboxdExecutor::CheckConnectivity);
    if (!sandboxd_ || sandboxd_->IsConnected()) {
        return;
    }
    if (reconnecting_) {
        return;
    }
    ReconnectContainerd();
}

void SandboxdExecutor::ReconnectContainerd()
{
    if (sandboxd_->IsConnected()) {
        reconnecting_ = false;
        return;
    }
    reconnecting_ = true;
    auto actor = std::make_shared<ActorWorker>();
    actor->AsyncWork([sandboxd(sandboxd_)]() { sandboxd->CheckChannelAndWaitForReconnect(true); })
        .OnComplete([actor, aid(GetAID())](const litebus::Future<Status> &) {
            actor->Terminate();
            litebus::Async(aid, &SandboxdExecutor::OnReconnectContainerd);
        });
}

void SandboxdExecutor::OnReconnectContainerd()
{
    if (!sandboxd_->IsConnected()) {
        reconnecting_ = false;
        litebus::AsyncAfter(RECONNECT_INTERVAL_MS, GetAID(), &SandboxdExecutor::ReconnectContainerd);
        return;
    }
    reconnecting_ = false;
    YRLOG_INFO("SandboxdExecutor: reconnect sandboxd success");
    // Reconnect only helps finish the initial snapshot. Once initialized, the
    // advertised capability remains static for this process lifetime.
    FetchInitialAvailableRuntimes();
}

void SandboxdExecutor::FetchInitialAvailableRuntimes()
{
    if (!sandboxd_ || availableRuntimesInitialized_ || fetchingAvailableRuntimes_) {
        return;
    }
    fetchingAvailableRuntimes_ = true;
    auto request = std::make_shared<runtime::v1::ListAvailableRuntimesRequest>();
    auto response = std::make_shared<runtime::v1::ListAvailableRuntimesResponse>();
    sandboxd_
        ->CallAsyncX("ListAvailableRuntimes", *request, response.get(),
                     &runtime::v1::SandboxService::Stub::AsyncListAvailableRuntimes)
        .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnListAvailableRuntimes, response,
                             std::placeholders::_1));
}

Status SandboxdExecutor::OnListAvailableRuntimes(
    const std::shared_ptr<runtime::v1::ListAvailableRuntimesResponse> &response, const Status &status)
{
    fetchingAvailableRuntimes_ = false;
    if (!status.IsOk()) {
        YRLOG_WARN("ListAvailableRuntimes gRPC failed: {}; initial registration remains pending",
                   status.RawMessage());
        ScheduleAvailableRuntimesRetry();
        return status;
    }

    AvailableRuntimes runtimes;
    for (const auto &runtime : response->runtime_classes()) {
        if (!runtime.empty()) {
            runtimes.insert(runtime);
        }
    }
    availableRuntimesInitialized_ = true;
    YRLOG_INFO("initialized sandboxd runtime capability snapshot with {} runtimes", runtimes.size());
    if (availableRuntimesCallback_) {
        availableRuntimesCallback_(true, runtimes);
    }
    return Status::OK();
}

void SandboxdExecutor::ScheduleAvailableRuntimesRetry()
{
    if (availableRuntimesInitialized_ || availableRuntimesRetryScheduled_) {
        return;
    }
    availableRuntimesRetryScheduled_ = true;
    litebus::AsyncAfter(RECONNECT_INTERVAL_MS, GetAID(), &SandboxdExecutor::RetryFetchInitialAvailableRuntimes);
}

void SandboxdExecutor::RetryFetchInitialAvailableRuntimes()
{
    availableRuntimesRetryScheduled_ = false;
    FetchInitialAvailableRuntimes();
}

void SandboxdExecutor::Sync()
{
    if (!sandboxd_) {
        return;
    }
    // Sync registered templates first. Start requests only carry template_id
    // for IDs that sandboxd confirms as registered; otherwise production
    // sandboxd rejects the start.
    DoGetRegistered();

    // Discover running sandboxes and resume Wait for them.
    DoList().Then([aid(GetAID())](const runtime::v1::ListSandboxesResponse &listResp) -> litebus::Future<Status> {
        int resumed = 0;
        for (const auto &sandbox : listResp.sandboxes()) {
            if (sandbox.state() == runtime::v1::SANDBOX_STATE_RUNNING) {
                YRLOG_INFO("Sync: resume Wait for running sandbox({})", sandbox.id());
                litebus::Async(aid, &SandboxdExecutor::RestoreWait, sandbox.id());
                ++resumed;
            }
        }
        YRLOG_INFO("Sync: resumed Wait for {} running sandboxes", resumed);
        return Status::OK();
    });
}

// ── gRPC wrappers ─────────────────────────────────────────────────────────────

litebus::Future<runtime::v1::StartResponse> SandboxdExecutor::DoStart(
    const std::shared_ptr<messages::StartInstanceRequest> &request,
    const std::shared_ptr<runtime::v1::StartRequest> &startReq)
{
    YRLOG_INFO("{}|{}|DoStart: {}", request->runtimeinstanceinfo().traceid(),
               request->runtimeinstanceinfo().requestid(), startReq->ShortDebugString());
    ASSERT_IF_NULL(sandboxd_);
    auto resp = std::make_shared<runtime::v1::StartResponse>();
    return sandboxd_->CallAsyncX("Start", *startReq, resp.get(), &runtime::v1::SandboxService::Stub::AsyncStart)
        .Then([request, resp, startReq](const Status &status) -> litebus::Future<runtime::v1::StartResponse> {
            if (status.IsOk()) {
                return *resp;
            }
            runtime::v1::StartResponse err;
            err.set_code(static_cast<int32_t>(status.StatusCode()));
            err.set_message(fmt::format("Start gRPC failed for runtime({}): {}",
                                        request->runtimeinstanceinfo().runtimeid(), status.RawMessage()));
            YRLOG_ERROR("{}|{}", request->runtimeinstanceinfo().traceid(), err.message());
            return err;
        });
}

litebus::Future<runtime::v1::DeleteResponse> SandboxdExecutor::DoDelete(
    const std::string &instanceID, const std::string &runtimeID, const std::string &requestID,
    const std::shared_ptr<runtime::v1::DeleteRequest> &req)
{
    YRLOG_INFO("{}|DoDelete: sandbox({}) runtime({})", requestID, req->id(), runtimeID);
    ASSERT_IF_NULL(sandboxd_);
    return sandboxd_
        ->CallAsync("Delete", *req, static_cast<runtime::v1::DeleteResponse *>(nullptr),
                    &runtime::v1::SandboxService::Stub::AsyncDelete)
        .Then([req, runtimeID, requestID](
                  litebus::Try<runtime::v1::DeleteResponse> rsp) -> litebus::Future<runtime::v1::DeleteResponse> {
            if (rsp.IsOK()) {
                return rsp.Get();
            }
            YRLOG_ERROR("{}|Delete gRPC failed for sandbox({}) runtime({}): {}", requestID, req->id(), runtimeID,
                        rsp.GetErrorCode());
            return litebus::Status(rsp.GetErrorCode());
        });
}

litebus::Future<runtime::v1::NormalResponse> SandboxdExecutor::DoRegister(
    const std::shared_ptr<runtime::v1::RegisterRequest> &req)
{
    ASSERT_IF_NULL(sandboxd_);
    auto resp = std::make_shared<runtime::v1::NormalResponse>();
    return sandboxd_->CallAsyncX("Register", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncRegister)
        .Then([resp](const Status &status) -> litebus::Future<runtime::v1::NormalResponse> {
            if (status.IsOk()) {
                return *resp;
            }
            runtime::v1::NormalResponse err;
            err.set_success(false);
            err.set_message("Register gRPC failed: " + status.RawMessage());
            return err;
        });
}

litebus::Future<runtime::v1::NormalResponse> SandboxdExecutor::DoUnregister(
    const std::shared_ptr<runtime::v1::UnregisterRequest> &req)
{
    ASSERT_IF_NULL(sandboxd_);
    auto resp = std::make_shared<runtime::v1::NormalResponse>();
    return sandboxd_->CallAsyncX("Unregister", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncUnregister)
        .Then([resp](const Status &status) -> litebus::Future<runtime::v1::NormalResponse> {
            if (status.IsOk()) {
                return *resp;
            }
            runtime::v1::NormalResponse err;
            err.set_success(false);
            err.set_message("Unregister gRPC failed: " + status.RawMessage());
            return err;
        });
}

litebus::Future<runtime::v1::GetRegisteredResponse> SandboxdExecutor::DoGetRegistered()
{
    ASSERT_IF_NULL(sandboxd_);
    auto req = std::make_shared<runtime::v1::GetRegisteredRequest>();
    auto resp = std::make_shared<runtime::v1::GetRegisteredResponse>();
    return sandboxd_
        ->CallAsyncX("GetRegistered", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncGetRegistered)
        .Then([this, resp](const Status &status) -> litebus::Future<runtime::v1::GetRegisteredResponse> {
            if (!status.IsOk()) {
                YRLOG_WARN("GetRegistered gRPC failed: {}", status.RawMessage());
                return runtime::v1::GetRegisteredResponse{};
            }
            registeredTemplateIDs_.clear();
            for (const auto &tmpl : resp->templates()) {
                if (!tmpl.id().empty()) {
                    registeredTemplateIDs_.insert(tmpl.id());
                }
            }
            YRLOG_INFO("GetRegistered synced {} sandbox templates", registeredTemplateIDs_.size());
            return *resp;
        });
}

litebus::Future<runtime::v1::RestoreResponse> SandboxdExecutor::DoRestore(
    const std::shared_ptr<messages::StartInstanceRequest> &request,
    const std::shared_ptr<runtime::v1::RestoreRequest> &req)
{
    YRLOG_INFO("{}|{}|DoRestore: {}", request->runtimeinstanceinfo().traceid(),
               request->runtimeinstanceinfo().requestid(), req->ShortDebugString());
    ASSERT_IF_NULL(sandboxd_);
    auto resp = std::make_shared<runtime::v1::RestoreResponse>();
    return sandboxd_->CallAsyncX("Restore", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncRestore)
        .Then([request, resp](const Status &status) -> litebus::Future<runtime::v1::RestoreResponse> {
            if (status.IsOk()) {
                return *resp;
            }
            runtime::v1::RestoreResponse err;
            err.set_code(static_cast<int32_t>(status.StatusCode()));
            err.set_message(fmt::format("Restore gRPC failed for runtime({}): {}",
                                        request->runtimeinstanceinfo().runtimeid(), status.RawMessage()));
            YRLOG_ERROR("{}|{}", request->runtimeinstanceinfo().traceid(), err.message());
            return err;
        });
}

void SandboxdExecutor::DoWait(const std::string &sandboxID, const std::string &runtimeID)
{
    ASSERT_IF_NULL(sandboxd_);
    auto req = std::make_shared<runtime::v1::WaitRequest>();
    req->set_id(sandboxID);
    auto resp = std::make_shared<runtime::v1::WaitResponse>();
    YRLOG_INFO("DoWait: sandbox({}) runtime({})", sandboxID, runtimeID);
    sandboxd_->CallAsyncX("Wait", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncWait)
        .Then([req, resp, runtimeID, aid(GetAID())](const Status &status) -> litebus::Future<Status> {
            runtime::v1::WaitResponse response;
            if (status.IsOk()) {
                response = *resp;
            } else {
                auto msg = fmt::format("failed to wait sandbox {}, grpc err: {}", req->id(), status.RawMessage());
                YRLOG_ERROR("{}", msg);
                response.set_status(static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION));
                response.set_message(msg);
            }
            return litebus::Async(aid, &SandboxdExecutor::OnWaitDone, runtimeID, response);
        });
}

void SandboxdExecutor::RestoreWait(const std::string &sandboxID)
{
    DoWaitWithRetry(sandboxID, sandboxID, 0);
}

// ── Wait retry on sandboxd disconnection ──────────────────────────────────────

void SandboxdExecutor::DoWaitWithRetry(const std::string &sandboxID, const std::string &runtimeID, int retryCount)
{
    ASSERT_IF_NULL(sandboxd_);
    auto req = std::make_shared<runtime::v1::WaitRequest>();
    req->set_id(sandboxID);
    auto resp = std::make_shared<runtime::v1::WaitResponse>();
    YRLOG_INFO("DoWaitWithRetry: sandbox({}) runtime({}) retry({})", sandboxID, runtimeID, retryCount);
    sandboxd_->CallAsyncX("Wait", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncWait)
        .Then([req, resp, sandboxID, runtimeID, retryCount,
               aid(GetAID())](const Status &status) -> litebus::Future<Status> {
            YRLOG_INFO("DoWaitWithRetry returned: sandbox({}) runtime({}) retry({}) statusCode({}) msg({})", sandboxID,
                       runtimeID, retryCount, fmt::underlying(status.StatusCode()), status.RawMessage());
            if (status.IsOk()) {
                return litebus::Async(aid, &SandboxdExecutor::OnWaitDone, runtimeID, *resp);
            }
            if (!SandboxdExecutor::IsRetryableWaitError(status)) {
                runtime::v1::WaitResponse response;
                response.set_status(static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION));
                response.set_message(fmt::format("failed to wait sandbox {}, non-retryable grpc err: {}", req->id(),
                                                 status.RawMessage()));
                return litebus::Async(aid, &SandboxdExecutor::OnWaitDone, runtimeID, response);
            }
            if (retryCount >= WAIT_MAX_RETRIES) {
                return litebus::Async(aid, &SandboxdExecutor::CleanupSandboxAfterMaxRetries, runtimeID, sandboxID);
            }
            YRLOG_WARN("DoWait failed for sandbox {}, retryable error ({}), will retry in {}ms (attempt {}/{})",
                       req->id(), status.RawMessage(), WAIT_RETRY_INTERVAL_MS, retryCount + 1, WAIT_MAX_RETRIES);
            (void)litebus::AsyncAfter(WAIT_RETRY_INTERVAL_MS, aid, &SandboxdExecutor::DoWaitWithRetry, sandboxID,
                                      runtimeID, retryCount + 1);
            return Status::OK();
        });
}

void SandboxdExecutor::ScheduleSandboxStatsCollection(const std::string &runtimeID, const std::string &sandboxID)
{
    // D-7: the Stats poll is no longer Prometheus-only -- with the resource
    // view reporter wired it also feeds per-instance actualuse, so it must
    // run even when the metrics env knob is OFF (deploy strips container env)
    if (!IsSandboxMetricsEnabled() && instanceMemoryUsageReporter_ == nullptr) {
        return;
    }
    if (sandboxStatsPollingRuntimes_.count(runtimeID) == 0) {
        return;
    }
    if (stateManager_.GetSandboxID(runtimeID) != sandboxID) {
        return;
    }
    (void)litebus::AsyncAfter(SANDBOX_STATS_COLLECT_INTERVAL_MS, GetAID(), &SandboxdExecutor::CollectSandboxStats,
                              runtimeID, sandboxID);
}

void SandboxdExecutor::ScheduleRunningStatusHeartbeat(const std::string &runtimeID)
{
    if (!IsSandboxMetricsEnabled()) {
        return;
    }
    auto it = sandboxLifecycleStates_.find(runtimeID);
    if (it == sandboxLifecycleStates_.end() || it->second != SandboxLifecycleStatus::RUNNING) {
        return;
    }
    (void)litebus::AsyncAfter(SANDBOX_RUNNING_HEARTBEAT_INTERVAL_MS, GetAID(),
                              &SandboxdExecutor::ReportRunningStatusHeartbeat, runtimeID);
}

void SandboxdExecutor::ReportRunningStatusHeartbeat(const std::string &runtimeID)
{
    auto infoOpt = stateManager_.Find(runtimeID);
    if (!infoOpt.has_value()) {
        return;
    }
    auto stateIt = sandboxLifecycleStates_.find(runtimeID);
    if (stateIt == sandboxLifecycleStates_.end() || stateIt->second != SandboxLifecycleStatus::RUNNING) {
        return;  // sandbox already in terminal state, stop heartbeat
    }
    ReportSandboxLifecycleStatus(infoOpt->instanceInfo, runtimeID, SandboxLifecycleStatus::RUNNING);
    ScheduleRunningStatusHeartbeat(runtimeID);
}

void SandboxdExecutor::CollectSandboxStats(const std::string &runtimeID, const std::string &sandboxID)
{
    // D-7: see ScheduleSandboxStatsCollection -- reporter implies collect
    if (!IsSandboxMetricsEnabled() && instanceMemoryUsageReporter_ == nullptr) {
        return;
    }
    if (sandboxStatsPollingRuntimes_.count(runtimeID) == 0) {
        return;
    }
    if (sandboxID.empty() || stateManager_.GetSandboxID(runtimeID) != sandboxID) {
        return;
    }

    auto req = std::make_shared<runtime::v1::StatsRequest>();
    req->set_id(sandboxID);
    auto resp = std::make_shared<runtime::v1::StatsResponse>();
    auto collectedAt = std::chrono::steady_clock::now();

    ASSERT_IF_NULL(sandboxd_);
    sandboxd_->CallAsyncX("Stats", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncStats)
        .Then([runtimeID, sandboxID, resp, collectedAt,
               aid(GetAID())](const Status &status) -> litebus::Future<Status> {
            runtime::v1::StatsResponse statsResponse;
            if (status.IsOk()) {
                statsResponse = *resp;
            }
            return litebus::Async(aid, &SandboxdExecutor::OnSandboxStatsCollected, runtimeID, sandboxID, status,
                                  statsResponse, collectedAt);
        });
}

litebus::Future<Status> SandboxdExecutor::OnSandboxStatsCollected(const std::string &runtimeID,
                                                                  const std::string &sandboxID, const Status &status,
                                                                  const runtime::v1::StatsResponse &response,
                                                                  std::chrono::steady_clock::time_point collectedAt)
{
    if (sandboxStatsPollingRuntimes_.count(runtimeID) == 0) {
        return Status::OK();
    }
    if (sandboxID.empty() || stateManager_.GetSandboxID(runtimeID) != sandboxID) {
        return Status::OK();
    }

    if (!status.IsOk()) {
        YRLOG_WARN("OnSandboxStatsCollected: runtime({}) sandbox({}) stats failed: {}", runtimeID, sandboxID,
                   status.RawMessage());
        ScheduleSandboxStatsCollection(runtimeID, sandboxID);
        return Status::OK();
    }

    if (auto info = stateManager_.Find(runtimeID)) {
        ReportSandboxUsageMetrics(info->instanceInfo, runtimeID, response, collectedAt);
    }
    ScheduleSandboxStatsCollection(runtimeID, sandboxID);
    return Status::OK();
}

litebus::Future<Status> SandboxdExecutor::CleanupSandboxAfterMaxRetries(const std::string &runtimeID,
    const std::string &sandboxID)
{
    auto info = stateManager_.Find(runtimeID);
    if (!info.has_value()) {
        YRLOG_WARN("CleanupSandboxAfterMaxRetries: runtime({}) already unregistered", runtimeID);
        return Status::OK();
    }

    const auto &instanceID = info->instanceInfo.instanceid();
    auto requestID = litebus::os::Join("update-instance-status-request", runtimeID, '-');

    auto msg =
        fmt::format("Sandbox {} wait failed after {} retries, marking instance fatal", sandboxID, WAIT_MAX_RETRIES);
    YRLOG_ERROR("{}|{}", requestID, msg);

    ReportSandboxLifecycleStatus(info->instanceInfo, runtimeID, SandboxLifecycleStatus::ABNORMAL);
    ClearSandboxMetricsState(runtimeID);

    // Notify the control plane first so it can transition the instance and run
    // its coordinated kill bookkeeping. Local deletion is an independent,
    // idempotent fallback: it retains runtime/port ownership across failures,
    // retries with bounded exponential backoff, and only releases resources
    // after sandboxd confirms deletion (or confirms the sandbox is absent).
    auto notify = healthCheckClient_->NotifySandboxExit(instanceID, runtimeID, -1, msg, requestID);
    (void)notify.OnComplete([requestID, runtimeID](const litebus::Future<Status> &result) {
        if (result.IsError() || result.Get().IsError()) {
            YRLOG_ERROR("{}|failed to notify control plane about runtime({}) wait failure; local sandbox reclaim "
                        "continues in the background",
                        requestID, runtimeID);
        }
    });
    StartSandboxReclaim(runtimeID, sandboxID, requestID);
    return notify;
}

void SandboxdExecutor::StartSandboxReclaim(const std::string &runtimeID, const std::string &sandboxID,
                                           const std::string &requestID)
{
    auto [it, inserted] = sandboxReclaims_.try_emplace(
        runtimeID, SandboxReclaimState{ sandboxID, 0, std::make_shared<litebus::Promise<Status>>() });
    if (!inserted) {
        if (it->second.sandboxID == sandboxID) {
            YRLOG_WARN("{}|sandbox({}) runtime({}) reclaim is already scheduled", requestID, sandboxID, runtimeID);
            return;
        }

        const auto staleSandboxID = it->second.sandboxID;
        YRLOG_ERROR("{}|replacing stale sandbox({}) reclaim for runtime({}) with sandbox({})", requestID,
                    staleSandboxID, runtimeID, sandboxID);
        it->second.completion->SetValue(
            Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "sandbox changed during local reclaim"));
        sandboxReclaims_.erase(it);
        userInitiatedTerminateRuntimes_.erase(runtimeID);
        StartSandboxReclaim(runtimeID, sandboxID, requestID);
        return;
    }

    userInitiatedTerminateRuntimes_.insert(runtimeID);
    YRLOG_WARN("{}|starting local reclaim for sandbox({}) runtime({}); delete failures will be retried with "
               "exponential backoff capped at {}ms",
               requestID, sandboxID, runtimeID, kSandboxReclaimMaxBackoffMs);
    (void)litebus::Async(GetAID(), &SandboxdExecutor::ReclaimSandbox, runtimeID, sandboxID, requestID);
}

void SandboxdExecutor::ReclaimSandbox(const std::string &runtimeID, const std::string &sandboxID,
                                      const std::string &requestID)
{
    auto reclaimIt = sandboxReclaims_.find(runtimeID);
    if (reclaimIt == sandboxReclaims_.end() || reclaimIt->second.sandboxID != sandboxID) {
        return;
    }
    if (stateManager_.GetSandboxID(runtimeID) != sandboxID) {
        YRLOG_ERROR("{}|stop retrying local reclaim for sandbox({}) runtime({}): runtime ownership changed",
                    requestID, sandboxID, runtimeID);
        reclaimIt->second.completion->SetValue(
            Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "runtime ownership changed during sandbox reclaim"));
        sandboxReclaims_.erase(reclaimIt);
        userInitiatedTerminateRuntimes_.erase(runtimeID);
        return;
    }

    auto deleteReq = std::make_shared<runtime::v1::DeleteRequest>();
    deleteReq->set_id(sandboxID);
    deleteReq->set_timeout(0);
    ASSERT_IF_NULL(sandboxd_);
    sandboxd_
        ->CallAsync("Delete", *deleteReq, static_cast<runtime::v1::DeleteResponse *>(nullptr),
                    &runtime::v1::SandboxService::Stub::AsyncDelete)
        .Then([aid(GetAID()), runtimeID, sandboxID,
               requestID](litebus::Try<runtime::v1::DeleteResponse> response) -> litebus::Future<Status> {
            return litebus::Async(aid, &SandboxdExecutor::OnReclaimSandboxDone, runtimeID, sandboxID, requestID,
                                  response);
        });
}

litebus::Future<Status> SandboxdExecutor::OnReclaimSandboxDone(
    const std::string &runtimeID, const std::string &sandboxID, const std::string &requestID,
    litebus::Try<runtime::v1::DeleteResponse> response)
{
    auto reclaimIt = sandboxReclaims_.find(runtimeID);
    if (reclaimIt == sandboxReclaims_.end() || reclaimIt->second.sandboxID != sandboxID) {
        YRLOG_INFO("{}|ignore completed reclaim for stale sandbox({}) runtime({})", requestID, sandboxID, runtimeID);
        return Status::OK();
    }

    if (response.IsOK() || response.GetErrorCode() == static_cast<int32_t>(GRPC_NOT_FOUND)) {
        if (response.IsOK()) {
            YRLOG_INFO("{}|local reclaim deleted sandbox({}) runtime({}) after {} failed attempt(s)", requestID,
                       sandboxID, runtimeID, reclaimIt->second.failedAttempts);
            return OnDeleteDone(runtimeID, requestID, sandboxID, response.Get());
        }
        YRLOG_WARN("{}|sandbox({}) runtime({}) was already absent during local reclaim; releasing retained resources",
                   requestID, sandboxID, runtimeID);
        return OnDeleteDone(runtimeID, requestID, sandboxID, runtime::v1::DeleteResponse{});
    }

    ++reclaimIt->second.failedAttempts;
    const auto delayMs = SandboxReclaimBackoffMs(reclaimIt->second.failedAttempts);
    YRLOG_ERROR("{}|local reclaim failed for sandbox({}) runtime({}), grpcCode({}), failedAttempts({}); retrying in "
                "{}ms",
                requestID, sandboxID, runtimeID, response.GetErrorCode(), reclaimIt->second.failedAttempts, delayMs);
    (void)litebus::AsyncAfter(delayMs, GetAID(), &SandboxdExecutor::ReclaimSandbox, runtimeID, sandboxID, requestID);
    return Status(StatusCode::ERR_INNER_COMMUNICATION, "sandbox reclaim delete failed; retry scheduled");
}

litebus::Future<Status> SandboxdExecutor::OnWaitDone(const std::string &runtimeID,
                                                     const runtime::v1::WaitResponse &response)
{
    auto info = stateManager_.Find(runtimeID);
    if (!info.has_value()) {
        YRLOG_INFO("OnWaitDone: runtime({}) already unregistered, skip", runtimeID);
        return Status::OK();
    }

    const auto &instanceID = info->instanceInfo.instanceid();
    auto requestID = litebus::os::Join("update-instance-status-request", runtimeID, '-');

    YRLOG_INFO("{}|OnWaitDone: sandbox exited for runtime({}), exit_code({}), status({})", requestID, runtimeID,
               response.exit_code(), response.status());

    if (userInitiatedTerminateRuntimes_.count(runtimeID) > 0) {
        YRLOG_INFO("{}|OnWaitDone: user-initiated terminate for runtime({}), defer to OnDeleteDone", requestID,
                   runtimeID);
        return Status::OK();
    }

    ReportSandboxLifecycleStatus(
        info->instanceInfo, runtimeID,
        IsNormalSandboxExit(response) ? SandboxLifecycleStatus::COMPLETED : SandboxLifecycleStatus::ABNORMAL);
    ClearSandboxMetricsState(runtimeID);

    return healthCheckClient_->NotifySandboxExit(instanceID, runtimeID, response.exit_code(), response.message(),
                                                 requestID);
}

// ── Utilities ─────────────────────────────────────────────────────────────────

messages::StartInstanceResponse SandboxdExecutor::MakeSuccessStartResponse(
    const std::shared_ptr<messages::StartInstanceRequest> &request, const std::string &sandboxID)
{
    const auto &info = request->runtimeinstanceinfo();
    messages::StartInstanceResponse rsp;
    rsp.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    rsp.set_message("start instance success");
    rsp.set_requestid(info.requestid());

    auto *ir = rsp.mutable_startruntimeinstanceresponse();
    ir->set_runtimeid(info.runtimeid());
    ir->set_containerid(sandboxID);
    ir->set_pid(0);

    if (auto portJson = stateManager_.GetPortMappingsJson(info.runtimeid()); !portJson.empty()) {
        ir->set_port(portJson);
    }
    return rsp;
}

void SandboxdExecutor::ReportMetrics(const std::string &instanceID, const std::string &runtimeID,
                                     const std::string &sandboxID, const functionsystem::metrics::MeterTitle &title)
{
    DoReportMetrics(instanceID, runtimeID, sandboxID, title);
}

void SandboxdExecutor::ReportSandboxLifecycleStatus(const messages::RuntimeInstanceInfo &info,
    const std::string &runtimeID,
    SandboxLifecycleStatus lifecycleStatus)
{
    sandboxLifecycleStates_[runtimeID] = lifecycleStatus;
    const auto sandboxID = stateManager_.GetSandboxID(runtimeID);

    if (lifecycleStatus == SandboxLifecycleStatus::RUNNING) {
        sandboxRunningStartTimes_.emplace(runtimeID, std::chrono::steady_clock::now());
        ScheduleRunningStatusHeartbeat(runtimeID);
    } else if (lifecycleStatus == SandboxLifecycleStatus::COMPLETED ||
               lifecycleStatus == SandboxLifecycleStatus::ABNORMAL) {
        auto startIt = sandboxRunningStartTimes_.find(runtimeID);
        if (startIt != sandboxRunningStartTimes_.end()) {
            const double durationSec =
                std::chrono::duration<double>(std::chrono::steady_clock::now() - startIt->second).count();
            ReportSandboxGauge(
                { functionsystem::metrics::YR_SANDBOX_LIFECYCLE_SECONDS, "sandbox lifecycle duration in seconds", "s" },
                BuildSandboxMetricLabels(info, runtimeID, sandboxID), durationSec);
        }
    }

    ReportSandboxGauge({ functionsystem::metrics::YR_SANDBOX_LIFECYCLE_STATUS,
                         "sandbox lifecycle status: 1-Creating, 2-Running, 3-Completed, 4-Abnormal", "enum" },
                       BuildSandboxMetricLabels(info, runtimeID, sandboxID), static_cast<double>(lifecycleStatus));
}

void SandboxdExecutor::ReportSandboxRequestedResources(const messages::RuntimeInstanceInfo &info,
    const std::string &runtimeID)
{
    const auto sandboxID = stateManager_.GetSandboxID(runtimeID);
    const auto labels = BuildSandboxMetricLabels(info, runtimeID, sandboxID);
    const auto requested = GetSandboxRequestedResources(info);

    ReportSandboxGauge(
        { functionsystem::metrics::YR_SANDBOX_REQUESTED_CPU_CORES, "requested cpu limit for sandbox", "cores" }, labels,
        requested.cpuCores);
    ReportSandboxGauge(
        { functionsystem::metrics::YR_SANDBOX_REQUESTED_MEMORY_BYTES, "requested memory limit for sandbox", "By" },
        labels, requested.memoryBytes);
}

void SandboxdExecutor::ReportSandboxUsageMetrics(const messages::RuntimeInstanceInfo &info,
    const std::string &runtimeID,
    const runtime::v1::StatsResponse &response,
    std::chrono::steady_clock::time_point collectedAt)
{
    const auto sandboxID = stateManager_.GetSandboxID(runtimeID);
    auto labels = BuildSandboxMetricLabels(info, runtimeID, sandboxID);

    ReportSandboxGauge(
        { functionsystem::metrics::YR_SANDBOX_MEMORY_USAGE_BYTES, "sandbox memory usage in bytes", "By" }, labels,
        static_cast<double>(response.memory_usage_bytes()));
    ReportSandboxGauge(
        { functionsystem::metrics::YR_SANDBOX_MEMORY_LIMIT_BYTES, "sandbox memory limit in bytes", "By" }, labels,
        static_cast<double>(response.memory_limit_bytes()));
    ReportSandboxGauge(
        { functionsystem::metrics::YR_SANDBOX_MEMORY_USAGE_RATIO, "sandbox memory usage ratio", "ratio" }, labels,
        response.memory_limit_bytes() == 0
            ? 0.0
            : static_cast<double>(response.memory_usage_bytes()) / static_cast<double>(response.memory_limit_bytes()));

    // D-7: also feed the resource view -- the pid-based instance collectors
    // are no-ops under runsc (pid=0), so this Stats poll is the only live
    // per-instance memory source for admission/park decisions.
    if (instanceMemoryUsageReporter_ != nullptr) {
        const double memoryMb = static_cast<double>(response.memory_usage_bytes()) / (1024.0 * 1024.0);
        YRLOG_INFO("[D-7] sandbox stats bridge: instance({}) runtime({}) memory {} MB", info.instanceid(), runtimeID,
                   memoryMb);
        instanceMemoryUsageReporter_(info.instanceid(), memoryMb);
    }

    auto previousIt = sandboxStatsSnapshots_.find(runtimeID);
    if (previousIt != sandboxStatsSnapshots_.end()) {
        const auto elapsedNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(collectedAt - previousIt->second.collectedAt).count();
        if (elapsedNs > 0 && response.cpu_usage_ns() >= previousIt->second.cpuUsageNs) {
            const double cpuUsageCores = static_cast<double>(response.cpu_usage_ns() - previousIt->second.cpuUsageNs)
                                         / static_cast<double>(elapsedNs);
            ReportSandboxGauge({ functionsystem::metrics::YR_SANDBOX_CPU_USAGE_CORES,
                  "sandbox cpu usage expressed as used cores", "cores" },
                labels, cpuUsageCores);
        }
    }

    sandboxStatsSnapshots_[runtimeID] = SandboxStatsSnapshot{ response.cpu_usage_ns(), collectedAt };

    ReportSandboxRequestedResources(info, runtimeID);
}

void SandboxdExecutor::ClearSandboxMetricsState(const std::string &runtimeID)
{
    sandboxStatsSnapshots_.erase(runtimeID);
    sandboxStatsPollingRuntimes_.erase(runtimeID);
    userInitiatedTerminateRuntimes_.erase(runtimeID);
    sandboxRunningStartTimes_.erase(runtimeID);
}

void SandboxdExecutor::DoReportMetrics(const std::string &instanceID, const std::string &runtimeID,
                                       const std::string &sandboxID, const functionsystem::metrics::MeterTitle &title)
{
    (void)instanceID;

    auto info = stateManager_.Find(runtimeID);
    if (!info.has_value()) {
        return;
    }

    auto labels = BuildSandboxMetricLabels(info->instanceInfo, runtimeID, sandboxID);
    const auto nowMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    ReportSandboxGauge(title, labels, static_cast<double>(nowMs));
    ReportSandboxRequestedResources(info->instanceInfo, runtimeID);

    if (title.name == "yr_app_instance_start_time") {
        ReportSandboxLifecycleStatus(info->instanceInfo, runtimeID, SandboxLifecycleStatus::RUNNING);
    } else if (title.name == "yr_instance_stop_time") {
        auto lifecycleIt = sandboxLifecycleStates_.find(runtimeID);
        if (lifecycleIt == sandboxLifecycleStates_.end() || lifecycleIt->second != SandboxLifecycleStatus::ABNORMAL) {
            ReportSandboxLifecycleStatus(info->instanceInfo, runtimeID, SandboxLifecycleStatus::COMPLETED);
        }
    }
}

// ── Port forward helpers ──────────────────────────────────────────────────────

std::vector<SandboxdExecutor::PortForwardConfig> SandboxdExecutor::ParseForwardPorts(const std::string &networkJson)
{
    std::vector<PortForwardConfig> configs;
    if (networkJson.empty()) {
        return configs;
    }
    try {
        auto j = json::parse(networkJson);
        if (!j.contains("portForwardings") || !j["portForwardings"].is_array()) {
            return configs;
        }
        for (const auto &item : j["portForwardings"]) {
            if (!item.is_object() || !item.contains("port") || !item["port"].is_number_integer()) {
                continue;
            }
            int p = item["port"].get<int>();
            if (p <= 0 || p > MAX_PORT_NUMBER) {
                continue;
            }
            PortForwardConfig cfg;
            cfg.containerPort = static_cast<uint32_t>(p);
            cfg.protocol = "tcp";
            if (item.contains("protocol") && item["protocol"].is_string()) {
                cfg.protocol = item["protocol"].get<std::string>();
                std::transform(cfg.protocol.begin(), cfg.protocol.end(), cfg.protocol.begin(), ::tolower);
            }
            if (item.contains("routeKind")) {
                if (!item["routeKind"].is_string()) {
                    YRLOG_WARN("ParseForwardPorts: routeKind must be a string, got {}",
                               item["routeKind"].type_name());
                    continue;
                }
                const auto routeKind = ParsePortRouteKind(item["routeKind"].get<std::string>());
                if (!routeKind.has_value()) {
                    YRLOG_WARN("ParseForwardPorts: unsupported routeKind '{}'", item["routeKind"].get<std::string>());
                    continue;
                }
                cfg.routeKind = *routeKind;
            }
            configs.push_back(cfg);
        }
    } catch (const std::exception &e) {
        YRLOG_WARN("ParseForwardPorts: {}", e.what());
    }
    return configs;
}

}  // namespace functionsystem::runtime_manager
