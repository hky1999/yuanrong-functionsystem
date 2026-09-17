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
#include "common/resource_view/memory_elasticity_config.h"
#include "sandbox_cpu_sample.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <nlohmann/json.hpp>
#include <unordered_set>

#include "async/asyncafter.hpp"
#include "async/uuid_generator.hpp"
#include "async/collect.hpp"
#include "common/constants/actor_name.h"
#include "common/constants/constants.h"
#include "common/constants/warmup_type.h"
#include "common/logs/logging.h"
#include "common/metrics/metrics_adapter.h"
#include "common/resource_view/resource_type.h"
#include "common/trace/create_trace_helper.h"
#include "common/utils/actor_worker.h"
#include "common/utils/collect_status.h"
#include "common/utils/generate_message.h"
#include "common/utils/port_forward_mapping.h"
#include "port/port_manager.h"
#include "runtime_manager/config/build.h"
#include "runtime_manager/executor/sandboxd/sandbox_command_utils.h"
#include "runtime_manager/executor/sandboxd/retire_fault_gate.h"
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
constexpr int64_t SANDBOX_STATS_COLLECT_INTERVAL_MS    = 1000;
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
    if (resource_view::MemoryElasticityEnabled() && !memoryEventWatcher_) {
        memoryEventWatcher_ = std::make_unique<SandboxMemoryEventWatcher>(
            [aid(GetAID())](const SandboxMemoryEventWatcher::Identity &identity) {
                litebus::Async(aid, &SandboxdExecutor::RefreshMemoryPressureStats, identity);
            });
        if (!memoryEventWatcher_->Start()) {
            YRLOG_WARN("memory event watcher unavailable; periodic Stats remains enabled");
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
    memoryEventWatcher_.reset(); // wake/join before the actor can be destroyed
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

    if (auto stopping = deferredStops_.find(runtimeID); stopping != deferredStops_.end()) {
        const auto result = stopping->second.completion->GetFuture();
        if (!result.IsOK() || result.Get().IsError() || stateManager_.IsActive(runtimeID)) {
            return GenFailStartInstanceResponse(request, StatusCode::ERR_INSTANCE_BUSY,
                "previous create/stop retirement is unresolved");
        }
        deferredStops_.erase(stopping);
    }
    stateManager_.Register(SandboxInfo{runtimeID, {}, {}, {}, info, {}, {}, {}});
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
        if (ResolveRuntimeLanguage(info).find(PYTHON_LANGUAGE) != std::string::npos) {
            const std::string yrServerPath =
                "import os,sys;"
                "p=os.path.join(os.path.dirname(__import__('yr').__file__),'main','yr_runtime_main.py');"
                "os.execv(sys.executable,[sys.executable,'-u',p]+sys.argv[1:])";
            builtCmdArgs.args.insert(builtCmdArgs.args.begin(), {"-u", "-c", yrServerPath});
        }
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
    return response;
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
    stateManager_.UpdateSandboxID(info.runtimeid(), sandboxID, response.resource_generation());
    guard->Commit();
    StartSandboxStatsCollection(info.runtimeid(), sandboxID);
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
    stateManager_.UpdateSandboxID(runtimeID, sandboxID, response.resource_generation());
    guard->Commit();

    StartSandboxStatsCollection(runtimeID, sandboxID);
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
    return StopInstanceWithOutcome(request, oomKilled)
        .Then([](const StopInstanceOutcome &outcome) { return outcome.status; });
}

litebus::Future<StopInstanceOutcome> SandboxdExecutor::StopInstanceWithOutcome(
    const std::shared_ptr<messages::StopInstanceRequest> &request, bool oomKilled)
{
    const std::string &runtimeID = request->runtimeid();
    const std::string &requestID = request->requestid();
    const std::string &expectedSandboxID = request->expected_sandbox_id();
    const std::string &expectedGeneration = request->expected_resource_generation();
    const auto noProof = [](const Status &status) {
        return StopInstanceOutcome{status, PhysicalRetirementProof{}};
    };
    // Identity must arrive complete or absent; a single field can never fence
    // a physical generation, so reject before any dispatch.
    if (expectedSandboxID.empty() != expectedGeneration.empty()) {
        return StopInstanceOutcome{Status(StatusCode::ERR_INSTANCE_BUSY,
            "stop requires both expected sandbox and generation or neither"), PhysicalRetirementProof{}};
    }
    const bool requireProof = !expectedSandboxID.empty();

    if (auto pending = deferredStops_.find(runtimeID); pending != deferredStops_.end()) {
        // Joining a deferred attempt cannot hand over a first-hand receipt;
        // the caller retries with the same identity and replays the durable
        // completion once the deferred delete has unregistered the state.
        return pending->second.completion->GetFuture().Then(noProof);
    }
    // Warm-up runtimes are teardown via the Unregister RPC, not Delete.
    if (warmupRuntimes_.count(runtimeID) > 0) {
        return UnregisterWarmUp(runtimeID, requestID).Then(noProof);
    }
    if (stateManager_.GetInProgressFuture(runtimeID)) {
        // Stop races an in-progress start: StopSandbox defers the delete and
        // the deferred completion carries no receipt. UNKNOWN for proof
        // purposes; a retry replays the durable receipt.
        return StopSandbox(runtimeID, requestID, oomKilled).Then(noProof);
    }
    const auto source = stateManager_.Find(runtimeID);
    if (!source) {
        if (!requireProof) {
            return StopInstanceOutcome{Status(StatusCode::ERR_INSTANCE_BUSY,
                "physical retirement unknown for missing runtime"), PhysicalRetirementProof{}};
        }
        return ReplayPhysicalRetirement(runtimeID, requestID, expectedSandboxID, expectedGeneration);
    }
    if (requireProof) {
        // First-hand delete: the caller's expected identity must equal the
        // locally registered incarnation before anything reaches sandboxd.
        if (source->sandboxID != expectedSandboxID || source->resourceGeneration != expectedGeneration
            || source->registrationID.empty()) {
            return StopInstanceOutcome{Status(StatusCode::ERR_INSTANCE_BUSY,
                "expected physical identity does not match the local runtime"), PhysicalRetirementProof{}};
        }
        return TerminateSandbox(runtimeID, requestID, source->sandboxID, oomKilled)
            .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnOutcomeStopDone, runtimeID, requestID,
                                 expectedSandboxID, expectedGeneration, std::placeholders::_1));
    }
    return StopSandbox(runtimeID, requestID, oomKilled).Then(noProof);
}

StopInstanceOutcome SandboxdExecutor::OnOutcomeStopDone(const std::string &runtimeID,
    const std::string &requestID, const std::string &sandboxID, const std::string &resourceGeneration,
    const Status &status)
{
    if (status.IsError()) { return StopInstanceOutcome{status, PhysicalRetirementProof{}}; }
    // TerminateSandbox fenced dispatch on exactly this identity, and DoDelete
    // only resolves OK when retired_generation equals the fenced generation.
    // The daemon receipt is durable, so this proof survives replays.
    YRLOG_INFO("{}|physical retirement proven sandbox({}) generation({}) runtime({})", requestID, sandboxID,
               resourceGeneration, runtimeID);
    return StopInstanceOutcome{Status::OK(), PhysicalRetirementProof{true, sandboxID, resourceGeneration}};
}

litebus::Future<StopInstanceOutcome> SandboxdExecutor::ReplayPhysicalRetirement(const std::string &runtimeID,
    const std::string &requestID, const std::string &sandboxID, const std::string &resourceGeneration)
{
    // Local state is already unregistered: a previous attempt finished the
    // delete, or the runtime manager restarted. Only sandboxd's durable
    // complete receipt may authorize settlement; keep everything else UNKNOWN.
    auto command = std::make_shared<runtime::v1::DeleteIfGenerationRequest>();
    command->set_id(sandboxID);
    command->set_expected_generation(resourceGeneration);
    YRLOG_INFO("{}|replaying physical retirement for sandbox({}) generation({}) runtime({})", requestID, sandboxID,
               resourceGeneration, runtimeID);
    ASSERT_IF_NULL(sandboxd_);
    return sandboxd_->CallAsync("DeleteIfGeneration", *command,
        static_cast<runtime::v1::DeleteIfGenerationResponse *>(nullptr),
        &runtime::v1::SandboxService::Stub::AsyncDeleteIfGeneration)
        .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnReplayRetirementDone, sandboxID, resourceGeneration,
                             runtimeID, requestID, std::placeholders::_1));
}

StopInstanceOutcome SandboxdExecutor::OnReplayRetirementDone(const std::string &sandboxID,
    const std::string &resourceGeneration, const std::string &runtimeID, const std::string &requestID,
    litebus::Try<runtime::v1::DeleteIfGenerationResponse> response)
{
    if (response.IsOK() && response.Get().retired_generation() == resourceGeneration) {
        // Idempotent: the receipt names the exact generation; nothing local
        // remains registered for this incarnation.
        YRLOG_INFO("{}|replayed physical retirement confirmed sandbox({}) generation({}) runtime({})", requestID,
                   sandboxID, resourceGeneration, runtimeID);
        return StopInstanceOutcome{Status::OK(), PhysicalRetirementProof{true, sandboxID, resourceGeneration}};
    }
    // A missing sandbox without a complete receipt, a transport failure or a
    // different retired generation all stay UNKNOWN; credit is retained.
    const auto grpcCode = response.IsOK() ? -1 : response.GetErrorCode();
    YRLOG_WARN("{}|replayed physical retirement unconfirmed sandbox({}) generation({}) runtime({}) grpcCode({})",
               requestID, sandboxID, resourceGeneration, runtimeID, grpcCode);
    return StopInstanceOutcome{Status(StatusCode::ERR_INSTANCE_BUSY,
        "physical retirement replay unconfirmed"), PhysicalRetirementProof{}};
}

litebus::Future<Status> SandboxdExecutor::StopSandbox(const std::string &runtimeID, const std::string &requestID,
                                                      bool oomKilled)
{
    if (auto pending = deferredStops_.find(runtimeID); pending != deferredStops_.end()) {
        return pending->second.completion->GetFuture();
    }
    if (auto start = stateManager_.GetInProgressFuture(runtimeID)) {
        const auto source = stateManager_.Find(runtimeID);
        if (!source || source->instanceInfo.requestid().empty() || deferredStops_.size() >= 4096) {
            return Status(StatusCode::ERR_INSTANCE_BUSY, "create/stop identity unavailable or capacity exhausted");
        }
        auto completion = std::make_shared<litebus::Promise<Status>>();
        const auto sourceRequest = source->instanceInfo.requestid();
        deferredStops_[runtimeID] = {sourceRequest, completion};
        completion->Associate(start->Then(litebus::Defer(GetAID(), &SandboxdExecutor::ResumeDeferredStop,
            runtimeID, sourceRequest, requestID, oomKilled, std::placeholders::_1)));
        completion->GetFuture().OnComplete(litebus::Defer(GetAID(), &SandboxdExecutor::FinishDeferredStop,
            runtimeID, completion, std::placeholders::_1));
        return completion->GetFuture();
    }

    const auto sandboxID = stateManager_.GetSandboxID(runtimeID);
    if (sandboxID.empty()) {
        YRLOG_WARN("{}|runtime({}) not found in state manager", requestID, runtimeID);
        return Status(StatusCode::ERR_INSTANCE_BUSY, "physical retirement unknown for missing runtime");
    }

    return TerminateSandbox(runtimeID, requestID, sandboxID, oomKilled);
}

void SandboxdExecutor::FinishDeferredStop(const std::string &runtimeID,
    const std::shared_ptr<litebus::Promise<Status>> &completion, const litebus::Future<Status> &result)
{
    const auto pending = deferredStops_.find(runtimeID);
    if (pending == deferredStops_.end() || pending->second.completion != completion) { return; }
    // Retire only this completed successful attempt, after its local physical
    // registration is gone. A timeout, missing observation or error never frees it.
    if (!result.IsOK() || result.Get().IsError() || stateManager_.IsActive(runtimeID)
        || stateManager_.IsStartInProgress(runtimeID)) { return; }
    deferredStops_.erase(pending);
}

litebus::Future<Status> SandboxdExecutor::ResumeDeferredStop(const std::string &runtimeID,
    const std::string &startRequestID, const std::string &stopRequestID, bool force,
    const messages::StartInstanceResponse &started)
{
    const auto pending = deferredStops_.find(runtimeID);
    const auto source = stateManager_.Find(runtimeID);
    if (pending == deferredStops_.end() || pending->second.startRequestID != startRequestID
        || !source || source->instanceInfo.requestid() != startRequestID
        || started.requestid() != startRequestID || started.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
        return Status(StatusCode::ERR_INSTANCE_BUSY, "create/stop outcome requires physical reconciliation");
    }
    if (warmupRuntimes_.count(runtimeID) > 0) { return UnregisterWarmUp(runtimeID, stopRequestID); }
    if (source->sandboxID.empty() || source->resourceGeneration.empty()) {
        return Status(StatusCode::ERR_INSTANCE_BUSY, "created sandbox has no physical retirement identity");
    }
    return TerminateSandbox(runtimeID, stopRequestID, source->sandboxID, force);
}

litebus::Future<Status> SandboxdExecutor::TerminateSandbox(const std::string &runtimeID, const std::string &requestID,
                                                           const std::string &sandboxID, bool force)
{
    const auto source = stateManager_.Find(runtimeID);
    if (!source || source->sandboxID != sandboxID || source->registrationID.empty()) {
        return Status(StatusCode::ERR_INSTANCE_BUSY, "sandbox source changed before delete");
    }
    auto reclaimIt = sandboxReclaims_.find(runtimeID);
    if (reclaimIt != sandboxReclaims_.end() && reclaimIt->second.sandboxID == sandboxID
        && reclaimIt->second.registrationID == source->registrationID) {
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
    // Physical fault-test hook (default off): hold the delete dispatch for a
    // fixed delay BEFORE it reaches sandboxd, keeping the original operation
    // identity so the settlement converges once the hold elapses.
    int64_t faultDelayMs = 0;
    if (RetireFaultGate::Instance().BlockDeleteIfMatched(sandboxID, source->resourceGeneration, faultDelayMs)) {
        auto completion = std::make_shared<litebus::Promise<runtime::v1::DeleteResponse>>();
        litebus::AsyncAfter(std::chrono::milliseconds(faultDelayMs), GetAID(),
                            &SandboxdExecutor::RunFaultDeferredDelete, completion, runtimeID, requestID, del);
        return completion->GetFuture()
            .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnDeleteDone, runtimeID, requestID, sandboxID,
                                 std::placeholders::_1, source->registrationID));
    }
    return DoDelete("", runtimeID, requestID, del)
        .Then(litebus::Defer(GetAID(), &SandboxdExecutor::OnDeleteDone, runtimeID, requestID, sandboxID,
                             std::placeholders::_1, source->registrationID));
}

void SandboxdExecutor::RunFaultDeferredDelete(
    const std::shared_ptr<litebus::Promise<runtime::v1::DeleteResponse>> &completion,
    const std::string &runtimeID, const std::string &requestID,
    const std::shared_ptr<runtime::v1::DeleteRequest> &del)
{
    YRLOG_INFO("{}|fault-held delete dispatch resumes now for runtime({})", requestID, runtimeID);
    completion->Associate(DoDelete("", runtimeID, requestID, del));
}

litebus::Future<Status> SandboxdExecutor::OnDeleteDone(const std::string &runtimeID, const std::string &requestID,
    const std::string &sandboxID,
    const runtime::v1::DeleteResponse & /* response */, const std::string &registrationID)
{
    const auto current = stateManager_.Find(runtimeID);
    if (!current || current->sandboxID != sandboxID || registrationID.empty()
        || current->registrationID != registrationID) {
        return Status(StatusCode::ERR_INSTANCE_BUSY, "stale sandbox deletion completion");
    }
    YRLOG_INFO("{}|sandbox({}) deleted for runtime({})", requestID, sandboxID, runtimeID);
    if (ckptOrch_ != nullptr) { ckptOrch_->ReleaseRef(runtimeID, requestID); }

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
    int32_t orphansCleaned = static_cast<int32_t>(completedOrphanDeletes_.size());
    for (const auto &id : completedOrphanDeletes_) { response.add_orphanids(id); }
    completedOrphanDeletes_.clear();

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
        const auto generation = sandbox.labels().find("akernel.scheduler/resource-generation");
        if (generation == sandbox.labels().end() || generation->second.empty()) { continue; }
        DeleteSandboxAsync(sandboxID, generation->second, requestID);
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
            orphanGenerations_.erase(sandboxID);
            if (orphanFirstSeen_.erase(sandboxID)) {
                YRLOG_INFO("{}|ReconcileRuntimes: orphan timer cleared for {} (re-appeared in expected)",
                           context.requestID, sandboxID);
            }
            continue;
        }

        const auto generation = sandbox.labels().find("akernel.scheduler/resource-generation");
        if (generation == sandbox.labels().end() || generation->second.empty()) { continue; }
        if (orphanGenerations_[sandboxID] != generation->second) {
            orphanGenerations_[sandboxID] = generation->second;
            orphanFirstSeen_[sandboxID] = context.now;
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
        DeleteSandboxAsync(sandboxID, generation->second, context.requestID);
        // Keep the observed running set and local resources until a matching
        // physical receipt arrives; dispatch is not a completed cleanup.
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
                {entry.runtimeid(), entry.containerid(), {}, entry.portmappings(), instanceInfo, {}, {}, {}});
            stateManager_.MarkStartDone(entry.runtimeid());
            StartSandboxStatsCollection(entry.runtimeid(), entry.containerid());
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
        "releasing local runtime resources after confirmed orphan delete",
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
            orphanGenerations_.erase(it->first);
            it = orphanFirstSeen_.erase(it);
        } else {
            ++it;
        }
    }
}

void SandboxdExecutor::DeleteSandboxAsync(const std::string &sandboxID, const std::string &generation,
    const std::string &requestID)
{
    if (generation.empty() || orphanDeletes_.count(sandboxID) != 0 || orphanDeletes_.size() >= 4096
        || completedOrphanDeletes_.size() >= 4096) { return; }
    OrphanDeleteAttempt pending;
    pending.generation = generation;
    pending.requestID = requestID;
    pending.ticket = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    pending.runtimeID = stateManager_.FindRuntimeIDBySandboxID(sandboxID);
    if (!pending.runtimeID.empty()) {
        const auto source = stateManager_.Find(pending.runtimeID);
        if (!source || source->resourceGeneration != generation) { return; }
        pending.registrationID = source->registrationID;
    }
    auto command = std::make_shared<runtime::v1::DeleteIfGenerationRequest>();
    command->set_id(sandboxID);
    command->set_expected_generation(generation);
    orphanDeletes_[sandboxID] = pending;
    ASSERT_IF_NULL(sandboxd_);
    sandboxd_->CallAsync("DeleteIfGeneration", *command,
        static_cast<runtime::v1::DeleteIfGenerationResponse *>(nullptr),
        &runtime::v1::SandboxService::Stub::AsyncDeleteIfGeneration)
        .Then([aid(GetAID()), sandboxID, ticket(pending.ticket), command](
            litebus::Try<runtime::v1::DeleteIfGenerationResponse> response) -> litebus::Future<Status> {
            return litebus::Async(aid, &SandboxdExecutor::OnDeleteSandboxComplete, sandboxID, ticket, response);
        });
}

Status SandboxdExecutor::OnDeleteSandboxComplete(const std::string &sandboxID, const std::string &ticket,
    litebus::Try<runtime::v1::DeleteIfGenerationResponse> rsp)
{
    const auto it = orphanDeletes_.find(sandboxID);
    if (it == orphanDeletes_.end() || it->second.ticket != ticket) {
        return Status(StatusCode::ERR_INSTANCE_BUSY, "stale orphan deletion completion");
    }
    const auto pending = it->second;
    orphanDeletes_.erase(it);
    if (rsp.IsOK() && rsp.Get().retired_generation() == pending.generation) {
        const auto source = stateManager_.Find(pending.runtimeID);
        if (source && source->sandboxID == sandboxID && source->registrationID == pending.registrationID
            && source->resourceGeneration == pending.generation
            && stateManager_.FindRuntimeIDBySandboxID(sandboxID) == pending.runtimeID) {
            CleanupLocalRuntimeStateForOrphan(pending.requestID, sandboxID);
        }
        completedOrphanDeletes_.push_back(sandboxID);
        YRLOG_INFO("{}|confirmed orphan delete sandbox({}) generation({})", pending.requestID, sandboxID,
            pending.generation);
        return Status::OK();
    }
    // Only this generation's timer may be moved by the failed attempt.
    const auto observed = orphanGenerations_.find(sandboxID);
    if (observed != orphanGenerations_.end() && observed->second == pending.generation) {
        orphanFirstSeen_[sandboxID] = std::chrono::steady_clock::now()
            - std::chrono::seconds(static_cast<int64_t>(orphanGracePeriodSec_))
            + std::chrono::seconds(static_cast<int64_t>(kOrphanDeleteRetryIntervalSec));
    }
    return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "orphan delete unconfirmed; ownership retained");
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
    const auto source = stateManager_.Find(runtimeID);
    if (!source || source->sandboxID != req->id() || source->resourceGeneration.empty()) {
        return litebus::Status(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY));
    }
    auto command = std::make_shared<runtime::v1::DeleteIfGenerationRequest>();
    command->set_id(req->id());
    command->set_expected_generation(source->resourceGeneration);
    ASSERT_IF_NULL(sandboxd_);
    return sandboxd_->CallAsync("DeleteIfGeneration", *command,
        static_cast<runtime::v1::DeleteIfGenerationResponse *>(nullptr),
        &runtime::v1::SandboxService::Stub::AsyncDeleteIfGeneration)
        .Then([command](litebus::Try<runtime::v1::DeleteIfGenerationResponse> response)
            -> litebus::Future<runtime::v1::DeleteResponse> {
            if (!response.IsOK()) { return litebus::Status(response.GetErrorCode()); }
            if (response.Get().retired_generation() != command->expected_generation()) {
                return litebus::Status(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
            }
            return runtime::v1::DeleteResponse{};
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

litebus::Future<messages::ResizeSandboxResponse> SandboxdExecutor::ResizeSandbox(
    const std::shared_ptr<messages::ResizeSandboxRequest> &request)
{
    messages::ResizeSandboxResponse response;
    if (request == nullptr) {
        response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
        response.set_outcome(messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH);
        response.set_message("resize request is missing");
        return response;
    }
    response.set_requestid(request->requestid());
    response.set_instanceid(request->instanceid());
    response.set_runtimeid(request->runtimeid());
    response.set_sandboxid(request->resize().sandbox_id());
    response.set_resourcegeneration(request->resize().resource_generation());
    response.set_outcome(messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH);
    response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));

    const auto info = stateManager_.Find(request->runtimeid());
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    if (request->requestid().empty() || request->instanceid().empty() || request->runtimeid().empty()
        || !request->has_resize() || request->resize().sandbox_id().empty()
        || request->resize().resource_generation().empty()
        || !request->resize().has_expected() || !request->resize().has_target()
        || request->resize().deadline_unix_ms() <= nowMs || request->resize().deadline_unix_ms() > nowMs + 60000) {
        response.set_message("resize requires valid request, identity, limits and unexpired deadline");
        return response;
    }
    if (!info.has_value() || !stateManager_.IsActive(request->runtimeid())
        || info->instanceInfo.instanceid() != request->instanceid()
        || stateManager_.GetSandboxID(request->runtimeid()) != request->resize().sandbox_id()) {
        response.set_message("resize identity does not match an active local runtime");
        return response;
    }
    if (sandboxd_ == nullptr) {
        response.set_message("sandboxd client unavailable before dispatch");
        return response;
    }

    if ((request->shrink() && !resource_view::MemoryElasticityEnabled())
        || (resource_view::MemoryElasticityEnabled()
            && (request->resize().expected_revision() == 0 || request->resize().operation_id().empty()))) {
        response.set_message("memory elasticity requires enabled feature and revision/operation identity");
        return response;
    }
    auto actual = std::make_shared<runtime::v1::ResizeResourcesResponse>();
    auto shrink = std::make_shared<runtime::v1::ShrinkResourcesRequest>();
    litebus::Future<Status> dispatched;
    if (request->shrink()) {
        const auto &command = request->resize();
        shrink->set_sandbox_id(command.sandbox_id());
        shrink->set_resource_generation(command.resource_generation());
        *shrink->mutable_expected() = command.expected();
        *shrink->mutable_target() = command.target();
        shrink->set_deadline_unix_ms(command.deadline_unix_ms());
        shrink->set_operation_id(command.operation_id());
        shrink->set_expected_revision(command.expected_revision());
        dispatched = sandboxd_->CallAsyncX("ShrinkResources", *shrink, actual.get(),
            &runtime::v1::SandboxService::Stub::AsyncShrinkResources, 5);
    } else {
        dispatched = sandboxd_->CallAsyncX("ResizeResources", request->resize(), actual.get(),
            &runtime::v1::SandboxService::Stub::AsyncResizeResources, 5);
    }
    // Both operation payloads and the response remain alive through gRPC completion.
    return dispatched.Then([request, actual, shrink, response](const Status &status) mutable {
            response.set_outcome(messages::ResizeSandboxResponse::UNKNOWN);
            response.set_code(static_cast<int32_t>(status.StatusCode()));
            response.set_message(status.RawMessage());
            if (!status.IsOk()) {
                // Even a transport failure can follow a successful write. The
                // scheduler retains its reservation until same-generation Stats.
                return response;
            }
            if (!actual->has_actual()
                || actual->actual().resource_generation() != request->resize().resource_generation()
                || !actual->actual().cpu_limits_known() || actual->actual().cpu_quota_usec() <= 0
                || actual->actual().cpu_period_usec() == 0 || actual->actual().memory_limit_bytes() == 0
                || actual->actual().memory_limit_bytes() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                response.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
                response.set_message("resize response lacks matching finite resource readback");
                return response;
            }
            // OBSERVED includes partial writes (complete=false). Passing this
            // result through must not release the whole reservation on failure.
            response.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
            *response.mutable_result() = *actual;
            return response;
        });
}

void SandboxdExecutor::RefreshMemoryPressureStats(const SandboxMemoryEventWatcher::Identity &identity)
{
    const auto info = stateManager_.Find(identity.runtimeID);
    if (!resource_view::MemoryElasticityEnabled() || !info || !stateManager_.IsActive(identity.runtimeID)
        || info->sandboxID != identity.sandboxID || info->resourceGeneration != identity.resourceGeneration) { return; }
    const auto ticket = sandboxStatsPolls_.RefreshTicket(identity.runtimeID);
    if (!ticket) { return; } // coalesce with the already in-flight Stats RPC
    YRLOG_INFO("memory event refresh instance={} sandbox={} generation={}",
                info->instanceInfo.instanceid(), identity.sandboxID, identity.resourceGeneration);
    CollectSandboxStats(identity.runtimeID, identity.sandboxID, *ticket);
}

void SandboxdExecutor::StartSandboxStatsCollection(const std::string &runtimeID, const std::string &sandboxID)
{
    sandboxStatsSnapshots_.erase(runtimeID);
    const auto ticket = sandboxStatsPolls_.Start(runtimeID);
    CollectSandboxStats(runtimeID, sandboxID, ticket);
}

void SandboxdExecutor::ScheduleSandboxStatsCollection(const std::string &runtimeID, const std::string &sandboxID,
    uint64_t ticket, std::chrono::steady_clock::time_point collectedAt)
{
    // Maintain start-to-start cadence without overlapping requests or catching
    // up missed ticks in a burst. Slow calls remain visible as wider windows.
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - collectedAt).count();
    const auto info = stateManager_.Find(runtimeID);
    bool elastic = false;
    if (info && resource_view::MemoryElasticityEnabled()) {
        const auto &options = info->instanceInfo.deploymentconfig().deployoptions();
        const auto enabled = options.find(SCHEDULER_ELASTIC_RESOURCES);
        elastic = enabled != options.end() && enabled->second == "true";
    }
    const auto interval = elastic ? resource_view::MEMORY_ELASTIC_STATS_INTERVAL_MS
                                  : SANDBOX_STATS_COLLECT_INTERVAL_MS;
    const auto delayMs = SandboxStatsPolls::NextDelayMs(elapsedMs, interval);
    (void)litebus::AsyncAfter(delayMs, GetAID(), &SandboxdExecutor::CollectSandboxStats,
                              runtimeID, sandboxID, ticket);
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

void SandboxdExecutor::CollectSandboxStats(const std::string &runtimeID, const std::string &sandboxID,
    uint64_t ticket)
{
    // D-7: see ScheduleSandboxStatsCollection -- reporter implies collect
    if (!IsSandboxMetricsEnabled() && instanceResourceSampleReporter_ == nullptr) {
        return;
    }
    if (sandboxID.empty() || stateManager_.GetSandboxID(runtimeID) != sandboxID) {
        return;
    }

    if (!sandboxStatsPolls_.Begin(runtimeID, ticket)) {
        return;
    }

    auto req = std::make_shared<runtime::v1::StatsRequest>();
    req->set_id(sandboxID);
    auto resp = std::make_shared<runtime::v1::StatsResponse>();
    auto collectedAt = std::chrono::steady_clock::now();

    ASSERT_IF_NULL(sandboxd_);
    sandboxd_->CallAsyncX("Stats", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncStats, 1)
        .Then([runtimeID, sandboxID, req, resp, collectedAt, ticket,
               aid(GetAID())](const Status &status) -> litebus::Future<Status> {
            runtime::v1::StatsResponse statsResponse;
            if (status.IsOk()) {
                statsResponse = *resp;
            }
            return litebus::Async(aid, &SandboxdExecutor::OnSandboxStatsCollected, runtimeID, sandboxID, status,
                                  statsResponse, collectedAt, ticket);
        });
}

litebus::Future<Status> SandboxdExecutor::OnSandboxStatsCollected(const std::string &runtimeID,
                                                                  const std::string &sandboxID, const Status &status,
                                                                  const runtime::v1::StatsResponse &response,
                                                                  std::chrono::steady_clock::time_point collectedAt,
                                                                  uint64_t ticket)
{
    const auto nextTicket = sandboxStatsPolls_.Complete(runtimeID, ticket);
    if (!nextTicket.has_value()) {
        return Status::OK();
    }
    if (sandboxID.empty() || stateManager_.GetSandboxID(runtimeID) != sandboxID) {
        return Status::OK();
    }

    if (!status.IsOk()) {
        // The next success is a new baseline, not an average spanning an
        // unobserved interval. Stale tickets return above without touching it.
        sandboxStatsSnapshots_.erase(runtimeID);
        YRLOG_WARN("OnSandboxStatsCollected: runtime({}) sandbox({}) stats failed: {}", runtimeID, sandboxID,
                   status.RawMessage());
        ScheduleSandboxStatsCollection(runtimeID, sandboxID, *nextTicket, collectedAt);
        return Status::OK();
    }

    if (auto info = stateManager_.Find(runtimeID)) {
        ReportSandboxUsageMetrics(info->instanceInfo, runtimeID, response, collectedAt);
    }
    ScheduleSandboxStatsCollection(runtimeID, sandboxID, *nextTicket, collectedAt);
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
    const auto source = stateManager_.Find(runtimeID);
    if (!source || source->sandboxID != sandboxID || source->registrationID.empty()) { return; }
    auto [it, inserted] = sandboxReclaims_.try_emplace(
        runtimeID, SandboxReclaimState{ sandboxID, 0, std::make_shared<litebus::Promise<Status>>(), source->registrationID });
    if (!inserted) {
        if (it->second.sandboxID == sandboxID && it->second.registrationID == source->registrationID) {
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
    (void)litebus::Async(GetAID(), &SandboxdExecutor::ReclaimSandbox, runtimeID, sandboxID, requestID, source->registrationID);
}

void SandboxdExecutor::ReclaimSandbox(const std::string &runtimeID, const std::string &sandboxID,
                                      const std::string &requestID, const std::string &registrationID)
{
    auto reclaimIt = sandboxReclaims_.find(runtimeID);
    if (reclaimIt == sandboxReclaims_.end() || reclaimIt->second.sandboxID != sandboxID
        || reclaimIt->second.registrationID != registrationID) {
        return;
    }
    const auto source = stateManager_.Find(runtimeID);
    if (!source || source->sandboxID != sandboxID || source->registrationID != registrationID) {
        YRLOG_ERROR("{}|stop retrying local reclaim for sandbox({}) runtime({}): runtime ownership changed",
                    requestID, sandboxID, runtimeID);
        reclaimIt->second.completion->SetValue(
            Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "runtime ownership changed during sandbox reclaim"));
        sandboxReclaims_.erase(reclaimIt);
        userInitiatedTerminateRuntimes_.erase(runtimeID);
        return;
    }

    if (source->resourceGeneration.empty()) {
        reclaimIt->second.completion->SetValue(
            Status(StatusCode::ERR_INSTANCE_BUSY, "physical generation unknown; reconcile before reclaim"));
        return;
    }
    auto deleteReq = std::make_shared<runtime::v1::DeleteIfGenerationRequest>();
    deleteReq->set_id(sandboxID);
    deleteReq->set_expected_generation(source->resourceGeneration);
    ASSERT_IF_NULL(sandboxd_);
    sandboxd_->CallAsync("DeleteIfGeneration", *deleteReq,
        static_cast<runtime::v1::DeleteIfGenerationResponse *>(nullptr),
        &runtime::v1::SandboxService::Stub::AsyncDeleteIfGeneration)
        .Then([aid(GetAID()), runtimeID, sandboxID, requestID, registrationID, deleteReq](
            litebus::Try<runtime::v1::DeleteIfGenerationResponse> response) -> litebus::Future<Status> {
            litebus::Try<runtime::v1::DeleteResponse> verified =
                response.IsOK() && response.Get().retired_generation() == deleteReq->expected_generation()
                ? litebus::Try<runtime::v1::DeleteResponse>(runtime::v1::DeleteResponse{})
                : litebus::Try<runtime::v1::DeleteResponse>(response.IsOK()
                    ? static_cast<int32_t>(GRPC_INTERNAL) : response.GetErrorCode());
            return litebus::Async(aid, &SandboxdExecutor::OnReclaimSandboxDone, runtimeID, sandboxID,
                requestID, verified, registrationID);
        });
}

litebus::Future<Status> SandboxdExecutor::OnReclaimSandboxDone(
    const std::string &runtimeID, const std::string &sandboxID, const std::string &requestID,
    litebus::Try<runtime::v1::DeleteResponse> response, const std::string &registrationID)
{
    auto reclaimIt = sandboxReclaims_.find(runtimeID);
    if (reclaimIt == sandboxReclaims_.end() || reclaimIt->second.sandboxID != sandboxID
        || reclaimIt->second.registrationID != registrationID) {
        YRLOG_INFO("{}|ignore completed reclaim for stale sandbox({}) runtime({})", requestID, sandboxID, runtimeID);
        return Status::OK();
    }

    if (response.IsOK()) {
        return OnDeleteDone(runtimeID, requestID, sandboxID, response.Get(), registrationID);
    }
    // NotFound is not a generation receipt: a lost ack or daemon restart needs
    // authoritative reconciliation, never an unconditional deletion fallback.

    ++reclaimIt->second.failedAttempts;
    const auto delayMs = SandboxReclaimBackoffMs(reclaimIt->second.failedAttempts);
    YRLOG_ERROR("{}|local reclaim failed for sandbox({}) runtime({}), grpcCode({}), failedAttempts({}); retrying in "
                "{}ms",
                requestID, sandboxID, runtimeID, response.GetErrorCode(), reclaimIt->second.failedAttempts, delayMs);
    (void)litebus::AsyncAfter(delayMs, GetAID(), &SandboxdExecutor::ReclaimSandbox, runtimeID, sandboxID, requestID, registrationID);
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
    ir->set_sandboxid(sandboxID);
    ir->set_sandboxip(stateManager_.GetSandboxIP(info.runtimeid()));
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
    const auto physical = stateManager_.Find(runtimeID);
    if (resource_view::MemoryElasticityEnabled()
        && (!physical || physical->resourceGeneration.empty()
            || physical->resourceGeneration != response.resource_generation())) {
        sandboxStatsSnapshots_.erase(runtimeID);
        return;
    }
    if (memoryEventWatcher_ && response.resource_revision_known() && !response.memory_cgroup_path().empty()) {
        const auto &options = info.deploymentconfig().deployoptions();
        const auto enabled = options.find(SCHEDULER_ELASTIC_RESOURCES);
        if (enabled != options.end() && enabled->second == "true") {
            const SandboxMemoryEventWatcher::Identity identity{
                runtimeID, sandboxID, response.resource_generation(), response.memory_cgroup_path()};
            if (!memoryEventWatcher_->Watch(identity)) {
                YRLOG_WARN("memory event registration failed instance={}; periodic Stats remains enabled", info.instanceid());
            }
        }
    }

    // A server that cannot read a usage field returns the protobuf zero
    // default, which is indistinguishable from a genuine idle/empty reading.
    // Never feed such a sample to gauges, the resource view, or the CPU
    // baseline: drop it, break differencing so the next known sample restarts
    // from a fresh baseline, and keep reporting requested (booked, not
    // observed) resources only. A known zero passes this gate on purpose.
    if (!response.cpu_usage_known() || !response.memory_usage_known() || !response.memory_limit_known()) {
        sandboxStatsSnapshots_.erase(runtimeID);
        const json unknownSample = {
            {"schema", 1}, {"event", "stats_usage_unknown"}, {"instance_id", info.instanceid()},
            {"runtime_id", runtimeID}, {"sandbox_id", sandboxID},
            {"resource_generation", response.resource_generation()},
            {"cpu_usage_known", response.cpu_usage_known()},
            {"memory_usage_known", response.memory_usage_known()},
            {"memory_limit_known", response.memory_limit_known()},
            {"memory_peak_known", response.memory_peak_known()},
        };
        YRLOG_WARN("[scheduler-resource] {}", unknownSample.dump());
        ReportSandboxRequestedResources(info, runtimeID);
        return;
    }

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
    // W17: the bridge log now also carries the delta-derived CPU cores so
    // per-trial CPU curves are reconstructable from logs (memory-only before).
    auto previousIt = sandboxStatsSnapshots_.find(runtimeID);
    const bool samePhysical = previousIt != sandboxStatsSnapshots_.end()
        && !response.resource_generation().empty()
        && previousIt->second.resourceGeneration == response.resource_generation();
    double cpuUsageCores = -1.0;
    int64_t cpuWindowNs = 0;
    if (samePhysical) {
        const auto elapsedNs =
            std::chrono::duration_cast<std::chrono::nanoseconds>(collectedAt - previousIt->second.collectedAt).count();
        const auto cores = SandboxCpuCores(previousIt->second.cpuUsageNs, response.cpu_usage_ns(), elapsedNs);
        if (cores.has_value()) {
            cpuWindowNs = elapsedNs;
            cpuUsageCores = *cores;
            ReportSandboxGauge({ functionsystem::metrics::YR_SANDBOX_CPU_USAGE_CORES,
                  "sandbox cpu usage expressed as used cores", "cores" },
                labels, cpuUsageCores);
        }
    }
    // Effective limits come from cgroup accounting, not the original booking.
    // A structured per-runtime event lets collectors retain each observation
    // without merging unrelated instances or treating missing CPU as zero.
    const auto timestampMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const auto requestAgeMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - collectedAt).count();
    const json sample = {
        {"schema", 1}, {"instance_id", info.instanceid()}, {"runtime_id", runtimeID},
        {"sandbox_id", sandboxID}, {"timestamp_ms", timestampMs},
        {"sampled_at_unix_ms", timestampMs - requestAgeMs},
        {"stats_request_duration_ms", requestAgeMs}, {"cpu_window_ns", cpuWindowNs},
        {"resource_generation", response.resource_generation()},
        {"cpu_pressure_known", response.cpu_pressure_known()},
        {"cpu_periods", response.cpu_periods()},
        {"cpu_throttled_periods", response.cpu_throttled_periods()},
        {"cpu_throttled_ns", response.cpu_throttled_ns()},
        {"memory_pressure_known", response.memory_pressure_known()},
        {"memory_high_events", response.memory_high_events()},
        {"memory_oom_events", response.memory_oom_events()},
        {"memory_oom_kill_events", response.memory_oom_kill_events()},

        {"elastic_policy", response.elastic_policy()},
        {"first_observation", !samePhysical},
        {"cpu_usage_ns", response.cpu_usage_ns()}, {"cpu_usage_cores", cpuUsageCores},
        {"cpu_usage_known", response.cpu_usage_known()},
        {"cpu_limits_known", response.cpu_limits_known()},
        {"cpu_quota_usec", response.cpu_quota_usec()}, {"cpu_period_usec", response.cpu_period_usec()},
        {"memory_current_bytes", response.memory_usage_bytes()},
        {"memory_usage_known", response.memory_usage_known()},
        {"memory_max_bytes", response.memory_limit_bytes()},
        {"memory_limit_known", response.memory_limit_known()},
        // Legacy servers may still fill the numeric peak without its flag;
        // only the flag says whether it is an observation.
        {"memory_peak_bytes", response.memory_max_usage_bytes()},
        {"memory_peak_known", response.memory_peak_known()}
    };
    YRLOG_INFO("[scheduler-resource] {}", sample.dump());
    if (instanceResourceSampleReporter_ != nullptr) {
        const double memoryMb = static_cast<double>(response.memory_usage_bytes()) / (1024.0 * 1024.0);
        YRLOG_INFO("[D-7] sandbox stats bridge: instance({}) runtime({}) memory {} MB cpu {} cores", info.instanceid(),
                   runtimeID, memoryMb, cpuUsageCores);
        resources::SandboxResourceSample observation;
        observation.set_runtime_id(runtimeID);
        observation.set_sandbox_id(sandboxID);
        observation.set_resource_generation(response.resource_generation());
        observation.set_sampled_at_unix_ms(timestampMs - requestAgeMs);
        observation.set_cpu_usage_ns(response.cpu_usage_ns());
        observation.set_cpu_usage_known(cpuUsageCores >= 0.0);
        observation.set_cpu_usage_cores(cpuUsageCores >= 0.0 ? cpuUsageCores : 0.0);
        observation.set_cpu_limits_known(response.cpu_limits_known());
        observation.set_cpu_quota_usec(response.cpu_quota_usec());
        observation.set_cpu_period_usec(response.cpu_period_usec());
        observation.set_memory_current_bytes(response.memory_usage_bytes());
        observation.set_memory_limit_bytes(response.memory_limit_bytes());
        observation.set_memory_peak_bytes(response.memory_max_usage_bytes());
        observation.set_elastic_policy(response.elastic_policy());
        observation.set_resource_revision(response.resource_revision());
        observation.set_resource_revision_known(response.resource_revision_known());
        observation.set_memory_high_bytes(response.memory_high_bytes());
        observation.set_memory_high_known(response.memory_high_known());
        observation.set_memory_high_unlimited(response.memory_high_unlimited());
        observation.set_frozen(response.frozen());
        observation.set_frozen_known(response.frozen_known());
        observation.set_resource_operation_id(response.resource_operation_id());
        observation.set_resource_operation_pending(response.resource_operation_pending());
        observation.set_memory_cgroup_path(response.memory_cgroup_path());
        observation.set_memory_usage_known(response.memory_usage_known());
        observation.set_memory_limit_known(response.memory_limit_known());
        observation.set_memory_peak_known(response.memory_peak_known());

        observation.set_cpu_pressure_known(response.cpu_pressure_known());
        observation.set_cpu_periods(response.cpu_periods());
        observation.set_cpu_throttled_periods(response.cpu_throttled_periods());
        observation.set_cpu_throttled_ns(response.cpu_throttled_ns());
        observation.set_memory_pressure_known(response.memory_pressure_known());
        observation.set_memory_high_events(response.memory_high_events());
        observation.set_memory_oom_events(response.memory_oom_events());
        observation.set_memory_oom_kill_events(response.memory_oom_kill_events());

        instanceResourceSampleReporter_(info.instanceid(), observation);
    }

    sandboxStatsSnapshots_[runtimeID] = SandboxStatsSnapshot{
        response.cpu_usage_ns(), collectedAt, response.resource_generation() };

    ReportSandboxRequestedResources(info, runtimeID);
}

void SandboxdExecutor::ClearSandboxMetricsState(const std::string &runtimeID)
{
    sandboxStatsSnapshots_.erase(runtimeID);
    sandboxStatsPolls_.Stop(runtimeID);
    if (memoryEventWatcher_) { memoryEventWatcher_->Unwatch(runtimeID); }
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
