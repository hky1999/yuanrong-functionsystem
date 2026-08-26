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

#include "sandboxd_request_builder.h"

#include <algorithm>
#include <arpa/inet.h>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <limits>

#include "common/constants/constants.h"
#include "common/logs/logging.h"
#include "common/resource_view/resource_type.h"
#include "common/utils/files.h"
#include "common/utils/path.h"
#include "runtime_manager/executor/executor.h"
#include "runtime_manager/executor/sandboxd/sandbox_command_utils.h"

namespace functionsystem::runtime_manager {

namespace {
// Log-redirect
const std::string YR_RT_WORKING_DIR       = "YR_RT_WORKING_DIR";
// Deploy-option keys
const std::string CONTAINER_ROOTFS             = "rootfs";
const std::string CONTAINER_EXTRA_CONFIG       = "extra_config";
const std::string CONTAINER_MOUNTS             = "mounts";
const std::string CONTAINER_NETWORK            = "network";
const std::string STORAGE_RESOURCE_NAME        = "storage";
// Resource defaults
constexpr double DEFAULT_CPU_MILLICORES   = 500.0;
constexpr double DEFAULT_MEMORY_MB        = 500.0;
constexpr uint32_t MAX_NETWORK_PORT        = std::numeric_limits<uint16_t>::max();
// Mount
const std::string YR_FUNCTION_LIB_PATH   = "YR_FUNCTION_LIB_PATH";
const std::string FUNCTION_LIB_PATH      = "FUNCTION_LIB_PATH";
const std::string GPU_DEVICE_IDS         = "GPU-DEVICE-IDS";
// Namespace alias for brevity
using namespace resource_view;  // NOLINT(google-build-using-namespace)

// Returns the directory portion of a path (like dirname(3))
std::string DirName(const std::string &path)
{
    if (path.empty()) {
        return "";
    }
    size_t pos = path.find_last_of('/');
    if (pos == std::string::npos) {
        return "";
    }
    return pos == 0 ? "/" : path.substr(0, pos);
}

Status InvalidRootfsConfig(const std::string &message)
{
    YRLOG_ERROR("Invalid rootfs overlay: {}", message);
    return Status(StatusCode::ERR_PARAM_INVALID, message);
}

bool HasNonEmptyString(const nlohmann::json &object, const char *key)
{
    return object.contains(key) && object.at(key).is_string() && !object.at(key).get<std::string>().empty();
}

Status ValidateS3RootfsConfig(const nlohmann::json &rootfsConfig)
{
    if (!rootfsConfig.contains("storageInfo") || !rootfsConfig.at("storageInfo").is_object()) {
        return InvalidRootfsConfig("s3 rootfs requires an object storageInfo");
    }
    const auto &storageInfo = rootfsConfig.at("storageInfo");
    for (const auto *key : {"endpoint", "bucket", "object"}) {
        if (!HasNonEmptyString(storageInfo, key)) {
            return InvalidRootfsConfig(fmt::format("s3 rootfs requires non-empty storageInfo.{}", key));
        }
    }
    for (const auto *key : {"accessKey", "secretKey"}) {
        if (storageInfo.contains(key) && !storageInfo.at(key).is_string()) {
            return InvalidRootfsConfig(fmt::format("storageInfo.{} must be a string", key));
        }
    }
    return Status::OK();
}

void ApplyS3RootfsConfig(runtime::v1::StartRequest &start, const nlohmann::json &rootfsConfig)
{
    auto s3 = start.mutable_rootfs()->mutable_s3_config();
    const auto &storageInfo = rootfsConfig.at("storageInfo");
    if (storageInfo.contains("endpoint"))
        s3->set_endpoint(storageInfo.at("endpoint").get<std::string>());
    if (storageInfo.contains("bucket"))
        s3->set_bucket(storageInfo.at("bucket").get<std::string>());
    if (storageInfo.contains("object"))
        s3->set_object(storageInfo.at("object").get<std::string>());
    if (storageInfo.contains("accessKey"))
        s3->set_access_key_id(storageInfo.at("accessKey").get<std::string>());
    if (storageInfo.contains("secretKey"))
        s3->set_access_key_secret(storageInfo.at("secretKey").get<std::string>());
}

Status ValidateResolvedRootfs(const runtime::v1::StartRequest &start)
{
    if (start.runtime().empty()) {
        return InvalidRootfsConfig("resolved sandbox runtime must not be empty");
    }
    switch (start.rootfs().type()) {
        case runtime::v1::RootfsSrcType::S3:
            if (!start.rootfs().has_s3_config() || start.rootfs().s3_config().endpoint().empty()
                || start.rootfs().s3_config().bucket().empty() || start.rootfs().s3_config().object().empty()) {
                return InvalidRootfsConfig("resolved s3 rootfs is incomplete");
            }
            break;
        case runtime::v1::RootfsSrcType::IMAGE:
            if (start.rootfs().image_url().empty()) {
                return InvalidRootfsConfig("resolved image rootfs requires imageurl");
            }
            break;
        case runtime::v1::RootfsSrcType::LOCAL:
            if (start.rootfs().path().empty()) {
                return InvalidRootfsConfig("resolved local rootfs requires path");
            }
            break;
        default:
            return InvalidRootfsConfig("resolved rootfs has an unsupported type");
    }
    return Status::OK();
}

Status ApplyRootfsCommonFields(runtime::v1::StartRequest &start, const nlohmann::json &rootfsConfig)
{
    if (rootfsConfig.contains("runtime")) {
        if (!HasNonEmptyString(rootfsConfig, "runtime")) {
            return InvalidRootfsConfig("runtime must be a non-empty string");
        }
        start.set_runtime(rootfsConfig.at("runtime").get<std::string>());
    }
    if (rootfsConfig.contains("readonly")) {
        if (!rootfsConfig.at("readonly").is_boolean()) {
            return InvalidRootfsConfig("readonly must be a boolean");
        }
        start.mutable_rootfs()->set_readonly(rootfsConfig.at("readonly").get<bool>());
    }
    return Status::OK();
}

Status ApplyS3RootfsOverlay(runtime::v1::StartRequest &start, const nlohmann::json &rootfsConfig)
{
    if (rootfsConfig.contains("path") || rootfsConfig.contains("imageurl")) {
        return InvalidRootfsConfig("s3 rootfs cannot contain path or imageurl");
    }
    if (auto status = ValidateS3RootfsConfig(rootfsConfig); !status.IsOk()) {
        return status;
    }
    start.mutable_rootfs()->set_type(runtime::v1::RootfsSrcType::S3);
    ApplyS3RootfsConfig(start, rootfsConfig);
    return Status::OK();
}

Status ApplyImageRootfsOverlay(runtime::v1::StartRequest &start, const nlohmann::json &rootfsConfig)
{
    if (rootfsConfig.contains("path") || rootfsConfig.contains("storageInfo")) {
        return InvalidRootfsConfig("image rootfs cannot contain path or storageInfo");
    }
    if (!HasNonEmptyString(rootfsConfig, "imageurl")) {
        return InvalidRootfsConfig("image rootfs requires non-empty imageurl");
    }
    start.mutable_rootfs()->set_type(runtime::v1::RootfsSrcType::IMAGE);
    start.mutable_rootfs()->set_image_url(rootfsConfig.at("imageurl").get<std::string>());
    return Status::OK();
}

Status ApplyLocalRootfsOverlay(runtime::v1::StartRequest &start, const nlohmann::json &rootfsConfig)
{
    if (rootfsConfig.contains("imageurl") || rootfsConfig.contains("storageInfo")) {
        return InvalidRootfsConfig("local rootfs cannot contain imageurl or storageInfo");
    }
    if (!HasNonEmptyString(rootfsConfig, "path")) {
        return InvalidRootfsConfig("local rootfs requires non-empty path");
    }
    start.mutable_rootfs()->set_type(runtime::v1::RootfsSrcType::LOCAL);
    start.mutable_rootfs()->set_path(rootfsConfig.at("path").get<std::string>());
    return Status::OK();
}

Status ApplyRootfsSourceOverlay(runtime::v1::StartRequest &start, const nlohmann::json &rootfsConfig)
{
    if (!rootfsConfig.contains("type")) {
        if (rootfsConfig.contains("path") || rootfsConfig.contains("imageurl")
            || rootfsConfig.contains("storageInfo")) {
            return InvalidRootfsConfig("rootfs source fields require type");
        }
        return Status::OK();
    }
    if (!rootfsConfig.at("type").is_string()) {
        return InvalidRootfsConfig("rootfs type must be a string");
    }
    const std::string type = rootfsConfig.at("type").get<std::string>();
    if (type == "s3") {
        return ApplyS3RootfsOverlay(start, rootfsConfig);
    }
    if (type == "image") {
        return ApplyImageRootfsOverlay(start, rootfsConfig);
    }
    if (type == "local") {
        return ApplyLocalRootfsOverlay(start, rootfsConfig);
    }
    return InvalidRootfsConfig(fmt::format("unsupported rootfs type: {}", type));
}

// Apply the rootfs deploy-option as a field-level overlay. The caller seeds
// start from the service container config before invoking this function.
Status ApplyRootfsJsonOverlay(runtime::v1::StartRequest &start, const std::string &rootfsJson)
{
    try {
        const nlohmann::json rootfsConfig = nlohmann::json::parse(rootfsJson);
        if (!rootfsConfig.is_object()) {
            return InvalidRootfsConfig("rootfs overlay must be a JSON object");
        }
        if (auto status = ApplyRootfsCommonFields(start, rootfsConfig); !status.IsOk()) {
            return status;
        }
        return ApplyRootfsSourceOverlay(start, rootfsConfig);
    } catch (const std::exception &e) {
        auto msg = fmt::format("Failed to parse rootfs JSON: {}", e.what());
        YRLOG_ERROR("{}", msg);
        return Status(StatusCode::ERR_PARAM_INVALID, msg);
    }
}

bool HasRootfsSourceOverride(const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    const auto &opts = request->runtimeinstanceinfo().deploymentconfig().deployoptions();
    auto it = opts.find(CONTAINER_ROOTFS);
    if (it == opts.end()) {
        return false;
    }
    try {
        const auto overlay = nlohmann::json::parse(it->second);
        return overlay.is_object() && overlay.contains("type");
    } catch (const std::exception &) {
        // BuildRootfs reports the parse error on normal starts. Conservatively
        // keep the bootstrap available if this helper is used by warm-up.
        return true;
    }
}

bool IsTemplateCompatible(const messages::RuntimeInstanceInfo &info, const runtime::v1::StartRequest &start)
{
    return start.runtime() == info.container().runtime()
           && start.rootfs().SerializeAsString() == info.container().rootfsconfig().SerializeAsString();
}

std::string ResolveRuntimeLanguage(const std::shared_ptr<messages::StartInstanceRequest> &request)
{
    std::string language = request->runtimeinstanceinfo().runtimeconfig().language();
    std::transform(language.begin(), language.end(), language.begin(), ::tolower);
    return language;
}

void SetS3MountSource(runtime::v1::Mount *mount, const nlohmann::json &s3)
{
    auto *s3Config = mount->mutable_s3_config();
    if (s3.find("endpoint") != s3.end()) {
        s3Config->set_endpoint(s3.at("endpoint").get<std::string>());
    }
    if (s3.find("bucket") != s3.end()) {
        s3Config->set_bucket(s3.at("bucket").get<std::string>());
    }
    if (s3.find("object") != s3.end()) {
        s3Config->set_object(s3.at("object").get<std::string>());
    }
    if (s3.find("accessKey") != s3.end()) {
        s3Config->set_access_key_id(s3.at("accessKey").get<std::string>());
    }
    if (s3.find("secretKey") != s3.end()) {
        s3Config->set_access_key_secret(s3.at("secretKey").get<std::string>());
    }
}

void SetMountSource(runtime::v1::Mount *mount, const nlohmann::json &item)
{
    if (item.find("host_path") != item.end()) {
        mount->set_host_path(item.at("host_path").get<std::string>());
    } else if (item.find("s3_config") != item.end() && item.at("s3_config").is_object()) {
        SetS3MountSource(mount, item.at("s3_config"));
    } else if (item.find("image_url") != item.end()) {
        mount->set_image_url(item.at("image_url").get<std::string>());
    }
}

Status MountsJsonParse(runtime::v1::StartRequest &start, const std::string &mountsJson)
{
    try {
        nlohmann::json arr = nlohmann::json::parse(mountsJson);
        if (!arr.is_array()) {
            return Status(StatusCode::ERR_PARAM_INVALID, "mounts config must be a JSON array");
        }
        for (const auto &item : arr) {
            if (!item.is_object()) {
                continue;
            }
            auto *mount = start.add_mounts();

            if (item.find("type") != item.end()) {
                mount->set_type(item.at("type").get<std::string>());
            }
            if (item.find("target") != item.end()) {
                mount->set_target(item.at("target").get<std::string>());
            }
            if (item.find("options") != item.end() && item.at("options").is_array()) {
                for (const auto &opt : item.at("options")) {
                    if (opt.is_string()) {
                        mount->add_options(opt.get<std::string>());
                    }
                }
            }

            SetMountSource(mount, item);
        }
    } catch (std::exception &e) {
        auto err = fmt::format("Failed to parse mounts JSON: {}", std::string(e.what()));
        YRLOG_ERROR("{}", err);
        return Status(StatusCode::ERR_PARAM_INVALID, err);
    }
    return Status::OK();
}

// Build log paths and ensure the files exist. Fills stdOut and stdErr.
void ResolveLogPaths(const std::string &logDir, const std::string &runtimeID, std::string &stdOut, std::string &stdErr)
{
    if (!litebus::os::ExistPath(logDir)) {
        YRLOG_WARN("std log dir {} not found, attempting mkdir", logDir);
        if (!litebus::os::Mkdir(logDir).IsNone()) {
            YRLOG_WARN("failed to create {}: {}", logDir, litebus::os::Strerror(errno));
            return;
        }
    }
    stdOut = litebus::os::Join(logDir, fmt::format("{}.out", runtimeID));
    stdErr = litebus::os::Join(logDir, fmt::format("{}.err", runtimeID));
    if (!litebus::os::ExistPath(stdOut) && TouchFile(stdOut) != 0) {
        YRLOG_WARN("create stdout log {} failed: {}", stdOut, litebus::os::Strerror(errno));
    }
    if (!litebus::os::ExistPath(stdErr) && TouchFile(stdErr) != 0) {
        YRLOG_WARN("create stderr log {} failed: {}", stdErr, litebus::os::Strerror(errno));
    }
}

}  // namespace

// ── Construction ──────────────────────────────────────────────────────────────

SandboxdRequestBuilder::SandboxdRequestBuilder(const CommandBuilder &cmdBuilder) : cmdBuilder_(cmdBuilder)
{
}

// ── Public Build ──────────────────────────────────────────────────────────────

std::pair<Status, std::shared_ptr<runtime::v1::StartRequest>> SandboxdRequestBuilder::Build(
    const SandboxdStartParams &params) const
{
    return BuildStart(params);
}

// ── Start path ────────────────────────────────────────────────────────────────

std::pair<Status, std::shared_ptr<runtime::v1::StartRequest>> SandboxdRequestBuilder::BuildStart(
    const SandboxdStartParams &params) const
{
    auto start = std::make_shared<runtime::v1::StartRequest>();

    // sandbox_id is intentionally left empty: per the sandboxd SandboxService
    // contract, sandboxd generates the sandbox ID and returns it in
    // StartResponse.id; the executor stores that via UpdateSandboxID(runtimeID).
    // Passing the client runtimeID here would conflate the two identities.

    // Attach tenant ID as a metric label for sandboxd observability.
    // tenant_id is passed via runtimeconfig.posixenvs as YR_TENANT_ID.
    {
        const auto &posixEnvs = params.request->runtimeinstanceinfo().runtimeconfig().posixenvs();
        if (auto it = posixEnvs.find("YR_TENANT_ID"); it != posixEnvs.end() && !it->second.empty()) {
            (*start->mutable_metric_labels())["tenantid"] = it->second;
        }
    }

    // Resolve rootfs (and runtime handler) from deploy options or container config.
    if (auto s = BuildRootfs(params.request, *start); !s.IsOk()) {
        return {s, nullptr};
    }

    ApplyExtraConfig(params.request, start.get());
    if (auto status = ApplyNetworkPolicy(params.request, params.portMappings, start.get()); !status.IsOk()) {
        return {status, nullptr};
    }
    ApplyPortMappings(params.portMappings, start->mutable_ports());

    std::string workingRoot;
    ApplyBootstrapMount(params.request, start->mutable_mounts(), workingRoot);
    ApplyCommands(params.request, params.cmdArgs, start.get());
    // Flat request has a single envs map; seed the working-root env there.
    (*start->mutable_envs())[YR_RT_WORKING_DIR] = workingRoot;

    Envs updatedEnvs = ApplyCodeMounts(params.request, start->mutable_mounts(), params.envs);

    // Build custom mounts from deployOptions
    {
        const auto &opts = params.request->runtimeinstanceinfo().deploymentconfig().deployoptions();
        if (auto it = opts.find(CONTAINER_MOUNTS); it != opts.end()) {
            if (auto status = MountsJsonParse(*start, it->second); !status.IsOk()) {
                return {status, nullptr};
            }
        }
        // Network mode (optional); empty => sandbox network on the sandboxd side.
        if (auto netIt = opts.find(CONTAINER_NETWORK); netIt != opts.end()) {
            start->set_network(netIt->second);
        }
    }

    ApplyResources(params.request, start->mutable_resources());
    if (auto status = ApplyWritableLayerSize(params.request, start.get()); !status.IsOk()) {
        return {status, nullptr};
    }

    // BuildRootfs above has already applied the request overlay to the service
    // baseline, and ApplyWritableLayerSize has completed the resolved rootfs.
    // Reuse the registered baseline template only when that final result still
    // matches it; never mutate a registered template with a request overlay.
    const auto &info = params.request->runtimeinstanceinfo();
    const auto &templateID = info.container().id();
    if (params.registeredTemplateIDs.count(templateID) > 0 && IsTemplateCompatible(info, *start)) {
        start->set_template_id(templateID);
    }
    ApplyXpuAllocations(params.envs, start.get());
    ApplyEnvsAndLogs(updatedEnvs, params.runtimeID, start.get());

    // YR_LANGUAGE follows the service runtime field. The container runtime is
    // the sandbox backend (for example runc/runsc), not the user runtime.
    (*start->mutable_envs())["YR_LANGUAGE"] = ResolveRuntimeLanguage(params.request);

    // trace_id is the distributed trace ID propagated from the upstream request
    // (runtimeinstanceinfo().traceid()), not the local runtimeID.
    start->set_trace_id(params.request->runtimeinstanceinfo().traceid());

    return {Status::OK(), std::move(start)};
}

Status SandboxdRequestBuilder::BuildRootfs(const std::shared_ptr<messages::StartInstanceRequest> &request,
                                           runtime::v1::StartRequest &start) const
{
    const auto &container = request->runtimeinstanceinfo().container();
    start.set_runtime(container.runtime());
    *start.mutable_rootfs() = container.rootfsconfig();

    const auto &opts = request->runtimeinstanceinfo().deploymentconfig().deployoptions();
    if (auto it = opts.find(CONTAINER_ROOTFS); it != opts.end()) {
        if (auto status = ApplyRootfsJsonOverlay(start, it->second); !status.IsOk()) {
            return status;
        }
    }
    return ValidateResolvedRootfs(start);
}

// ── Shared helpers ────────────────────────────────────────────────────────────

Envs SandboxdRequestBuilder::ApplyCodeMounts(const std::shared_ptr<messages::StartInstanceRequest> &request,
                                             google::protobuf::RepeatedPtrField<runtime::v1::Mount> *mounts,
                                             const Envs &envs) const
{
    Envs updated = envs;
    auto workingDirIt = envs.posixEnvs.find(UNZIPPED_WORKING_DIR);
    if (workingDirIt == envs.posixEnvs.end() || workingDirIt->second.empty()) {
        return updated;
    }

    const auto &deploySpec = request->runtimeinstanceinfo().deploymentconfig();
    auto funcPath =
        litebus::os::Join(litebus::os::Join(deploySpec.deploydir(), RUNTIME_LAYER_DIR_NAME), RUNTIME_FUNC_DIR_NAME);

    if (auto libIt = envs.posixEnvs.find(YR_FUNCTION_LIB_PATH);
        libIt != envs.posixEnvs.end() && !libIt->second.empty()) {
        funcPath = libIt->second;
    }

    auto *code = mounts->Add();
    code->set_type("bind");
    if (workingDirIt->second.find(".img") != std::string::npos) {
        code->set_type("erofs");
        funcPath = DirName(workingDirIt->second);
    }
    code->set_host_path(workingDirIt->second);
    code->set_target(request->runtimeinstanceinfo().container().mountpoint());

    updated.posixEnvs[UNZIPPED_WORKING_DIR]  = code->target();
    updated.posixEnvs[YR_FUNCTION_LIB_PATH]  = code->target();
    updated.posixEnvs[FUNCTION_LIB_PATH]     = code->target();

    for (const auto &layer : GenerateLayerPath(request->runtimeinstanceinfo())) {
        auto *layerMount = mounts->Add();
        layerMount->set_type("bind");
        layerMount->set_host_path(layer);
        std::string target = layer;
        std::replace(target.begin(), target.end(), '/', '-');
        layerMount->set_target(litebus::os::Join("/opt", target));
    }
    return updated;
}

void SandboxdRequestBuilder::ApplyBootstrapMount(const std::shared_ptr<messages::StartInstanceRequest> &request,
    google::protobuf::RepeatedPtrField<runtime::v1::Mount> *mounts,
    std::string &workingRoot) const
{
    workingRoot = "/";
    const auto &bc = request->runtimeinstanceinfo().bootstrapconfig();
    if (bc.type().empty() || bc.root().empty()) {
        return;
    }
    if (!HasRootfsSourceOverride(request)) {
        return;
    }
    auto *mount = mounts->Add();
    const std::string mountDst = "/__yuanrong";
    mount->set_host_path(bc.root());
    mount->set_target(mountDst);
    mount->set_type((bc.type() == "erofs") ? "erofs" : "bind");
    workingRoot = mountDst;
}

void SandboxdRequestBuilder::ApplyCommands(const std::shared_ptr<messages::StartInstanceRequest> &request,
                                           const CommandArgs &cmdArgs, runtime::v1::StartRequest *start) const
{
    for (const auto &cmd : BuildBootstrapCommands(request)) {
        *start->add_command() = cmd;
    }
    bool skipNext = false;
    for (const auto &arg : cmdArgs.args) {
        if (skipNext) {
            skipNext = false;
            continue;
        }
        if (arg == "--job_id" || arg.rfind("--job_id=", 0) == 0 || arg == "--runtime_id"
            || arg.rfind("--runtime_id=", 0) == 0) {
            if (arg == "--job_id" || arg == "--runtime_id") {
                skipNext = true;
            }
            continue;
        }
        *start->add_command() = arg;
    }
}

void SandboxdRequestBuilder::ApplyResources(const std::shared_ptr<messages::StartInstanceRequest> &request,
                                            google::protobuf::Map<std::string, double> *resources) const
{
    const auto &res = request->runtimeinstanceinfo().runtimeconfig().resources().resources();
    auto getEffectiveValue = [](const resource_view::Resource &res, double defaultVal) -> double {
        if (res.type() != ValueType::Value_Type_SCALAR) {
            return defaultVal;
        }
        double limit = res.scalar().limit();
        if (limit > 0) {
            return limit;
        }
        return res.scalar().value();
    };

    auto cpuIt = res.find(CPU_RESOURCE_NAME);
    (*resources)[CPU_RESOURCE_NAME] =
        (cpuIt != res.end()) ? getEffectiveValue(cpuIt->second, DEFAULT_CPU_MILLICORES) : DEFAULT_CPU_MILLICORES;

    auto memIt = res.find(MEMORY_RESOURCE_NAME);
    (*resources)[MEMORY_RESOURCE_NAME] =
        (memIt != res.end()) ? getEffectiveValue(memIt->second, DEFAULT_MEMORY_MB) : DEFAULT_MEMORY_MB;
}

Status SandboxdRequestBuilder::ApplyWritableLayerSize(
    const std::shared_ptr<messages::StartInstanceRequest> &request, runtime::v1::StartRequest *start) const
{
    const auto &resources = request->runtimeinstanceinfo().runtimeconfig().resources().resources();
    const auto iter = resources.find(STORAGE_RESOURCE_NAME);
    if (iter == resources.end()) {
        return Status::OK();
    }
    if (iter->second.type() != ValueType::Value_Type_SCALAR) {
        return Status(StatusCode::ERR_PARAM_INVALID, "storage resource must be scalar");
    }

    const auto &scalar = iter->second.scalar();
    const double storage = scalar.limit() > 0 ? scalar.limit() : scalar.value();
    if (!std::isfinite(storage) || storage <= 0 || std::floor(storage) != storage ||
        storage > static_cast<double>(std::numeric_limits<uint64_t>::max())) {
        return Status(StatusCode::ERR_PARAM_INVALID, "storage resource must be a positive integer number of bytes");
    }
    const auto storageBytes = static_cast<uint64_t>(storage);
    // Field 19 is sandboxd's canonical hard quota. Keep the rootfs field 6
    // compatibility alias populated for older sandboxd deployments.
    start->set_writable_layer_limit_bytes(storageBytes);
    start->mutable_rootfs()->set_writable_layer_size_bytes(storageBytes);
    return Status::OK();
}

void SandboxdRequestBuilder::ApplyXpuAllocations(const Envs &envs, runtime::v1::StartRequest *start) const
{
    auto iter = envs.userEnvs.find(GPU_DEVICE_IDS);
    if (iter == envs.userEnvs.end() || iter->second.empty()) {
        return;
    }

    auto *allocation = start->add_xpu_allocations();
    allocation->set_type("gpu");

    const auto &value = iter->second;
    size_t begin = 0;
    while (begin <= value.size()) {
        const auto end = value.find(',', begin);
        const auto token = value.substr(begin, end == std::string::npos ? value.size() - begin : end - begin);
        uint32_t deviceID = 0;
        const auto result = std::from_chars(token.data(), token.data() + token.size(), deviceID);
        if (result.ec == std::errc() && result.ptr == token.data() + token.size()) {
            allocation->add_device_ids(deviceID);
        } else {
            YRLOG_WARN("ignore invalid physical GPU device ID: {}", token);
        }
        if (end == std::string::npos) {
            break;
        }
        begin = end + 1;
    }

    if (allocation->device_ids().empty()) {
        start->mutable_xpu_allocations()->RemoveLast();
    }
}

void SandboxdRequestBuilder::ApplyEnvsAndLogs(const Envs &envs, const std::string &runtimeID,
                                              runtime::v1::StartRequest *start) const
{
    const auto &config = cmdBuilder_.GetConfig();
    const std::string logDir = litebus::os::Join(config.runtimeLogPath, config.runtimeStdLogDir);

    // Flat request carries a single envs map (no separate runtime/user envs).
    const auto combined = cmdBuilder_.CombineEnvs(envs);
    start->mutable_envs()->insert(combined.begin(), combined.end());
    (*start->mutable_envs())[YR_ONLY_STDOUT] = "true";

    std::string stdOut;
    std::string stdErr;
    ResolveLogPaths(logDir, runtimeID, stdOut, stdErr);
    start->set_stdout(stdOut);
    start->set_stderr(stdErr);
}

void SandboxdRequestBuilder::ApplyExtraConfig(const std::shared_ptr<messages::StartInstanceRequest> &request,
                                              runtime::v1::StartRequest *start) const
{
    const auto &opts = request->runtimeinstanceinfo().deploymentconfig().deployoptions();
    if (auto it = opts.find(CONTAINER_EXTRA_CONFIG); it != opts.end()) {
        start->set_extra_config(it->second);
    }
}

Status SandboxdRequestBuilder::ApplyNetworkPolicy(
    const std::shared_ptr<messages::StartInstanceRequest> &request,
    const std::vector<std::string> &portMappings, runtime::v1::StartRequest *start) const
{
    const auto &opts = request->runtimeinstanceinfo().deploymentconfig().deployoptions();

    try {
        auto iter = opts.find(CONTAINER_NETWORK_POLICY);
        if (iter == opts.end() || iter->second.empty()) {
            return Status::OK();
        }
        auto policy = nlohmann::json::parse(iter->second);
        if (!policy.is_object()) {
            return Status(StatusCode::ERR_PARAM_INVALID, "network_policy must be a JSON object");
        }

        bool blockNetwork = false;
        if (policy.contains("blockNetwork")) {
            if (!policy.at("blockNetwork").is_boolean()) {
                return Status(StatusCode::ERR_PARAM_INVALID, "network_policy.blockNetwork must be boolean");
            }
            blockNetwork = policy.at("blockNetwork").get<bool>();
        }

        std::vector<std::string> dnsBlacklist;
        if (policy.contains("dnsBlacklist")) {
            if (!policy.at("dnsBlacklist").is_array()) {
                return Status(StatusCode::ERR_PARAM_INVALID, "network_policy.dnsBlacklist must be an array");
            }
            for (const auto &pattern : policy.at("dnsBlacklist")) {
                if (!pattern.is_string() || pattern.get<std::string>().empty()) {
                    return Status(StatusCode::ERR_PARAM_INVALID,
                                  "network_policy.dnsBlacklist entries must be non-empty strings");
                }
                dnsBlacklist.push_back(pattern.get<std::string>());
            }
        }
        if (blockNetwork && !dnsBlacklist.empty()) {
            return Status(StatusCode::ERR_PARAM_INVALID,
                          "network_policy blockNetwork and dnsBlacklist cannot be combined");
        }
        if (!blockNetwork && dnsBlacklist.empty()) {
            return Status::OK();
        }

        auto *networkPolicy = start->mutable_network_policy();
        if (blockNetwork) {
            const auto &config = cmdBuilder_.GetConfig();
            in_addr address{};
            if (inet_pton(AF_INET, config.proxyIP.c_str(), &address) != 1) {
                return Status(StatusCode::ERR_PARAM_INVALID,
                              fmt::format("proxy IP '{}' is not a valid IPv4 address", config.proxyIP));
            }
            uint32_t port = 0;
            const auto &portText = config.proxyGrpcServerPort;
            const auto parsed = std::from_chars(portText.data(), portText.data() + portText.size(), port);
            if (parsed.ec != std::errc() || parsed.ptr != portText.data() + portText.size() || port == 0 ||
                port > MAX_NETWORK_PORT) {
                return Status(StatusCode::ERR_PARAM_INVALID,
                              fmt::format("proxy gRPC port '{}' is invalid", portText));
            }

            auto *traffic = networkPolicy->mutable_traffic();
            traffic->set_default_action(runtime::v1::NETWORK_POLICY_ACTION_DENY);
            traffic->set_mode(runtime::v1::TRAFFIC_POLICY_MODE_STATEFUL);
            auto *rule = traffic->add_rules();
            rule->set_action(runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
            rule->set_direction(runtime::v1::NETWORK_DIRECTION_BOTH);
            rule->set_protocol(runtime::v1::NETWORK_PROTOCOL_TCP);
            rule->mutable_peer()->set_address(config.proxyIP);
            rule->mutable_peer()->set_port(port);

            std::unordered_set<std::string> publishedTargets;
            for (const auto &mapping : portMappings) {
                const auto firstSeparator = mapping.find(':');
                const auto secondSeparator =
                    firstSeparator == std::string::npos ? std::string::npos : mapping.find(':', firstSeparator + 1);
                if (firstSeparator == std::string::npos || secondSeparator == std::string::npos ||
                    mapping.find(':', secondSeparator + 1) != std::string::npos) {
                    return Status(StatusCode::ERR_PARAM_INVALID,
                                  fmt::format("invalid sandboxd port mapping '{}'", mapping));
                }
                const auto protocolText = mapping.substr(0, firstSeparator);
                runtime::v1::NetworkProtocol protocol;
                if (protocolText == "tcp") {
                    protocol = runtime::v1::NETWORK_PROTOCOL_TCP;
                } else if (protocolText == "udp") {
                    protocol = runtime::v1::NETWORK_PROTOCOL_UDP;
                } else {
                    return Status(StatusCode::ERR_PARAM_INVALID,
                                  fmt::format("unsupported sandboxd port mapping protocol '{}'", protocolText));
                }
                const auto targetText = mapping.substr(secondSeparator + 1);
                uint32_t targetPort = 0;
                const auto targetParsed =
                    std::from_chars(targetText.data(), targetText.data() + targetText.size(), targetPort);
                if (targetParsed.ec != std::errc() ||
                    targetParsed.ptr != targetText.data() + targetText.size() ||
                    targetPort == 0 || targetPort > MAX_NETWORK_PORT) {
                    return Status(StatusCode::ERR_PARAM_INVALID,
                                  fmt::format("invalid sandboxd target port in mapping '{}'", mapping));
                }
                const auto targetKey = protocolText + ":" + targetText;
                if (!publishedTargets.insert(targetKey).second) {
                    continue;
                }
                auto *published = traffic->add_rules();
                published->set_action(runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
                published->set_direction(runtime::v1::NETWORK_DIRECTION_INGRESS);
                published->set_protocol(protocol);
                published->set_sandbox_port(targetPort);
            }
        } else {
            auto *dns = networkPolicy->mutable_dns();
            dns->set_default_action(runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
            for (const auto &pattern : dnsBlacklist) {
                auto *rule = dns->add_rules();
                rule->set_action(runtime::v1::NETWORK_POLICY_ACTION_DENY);
                rule->set_pattern(pattern);
            }
        }
    } catch (const std::exception &e) {
        return Status(StatusCode::ERR_PARAM_INVALID,
                      fmt::format("failed to parse network_policy JSON: {}", e.what()));
    }
    return Status::OK();
}

void SandboxdRequestBuilder::ApplyPortMappings(const std::vector<std::string> &portMappings,
                                               google::protobuf::RepeatedPtrField<std::string> *ports) const
{
    for (const auto &mapping : portMappings) {
        *ports->Add() = mapping;
    }
}

}  // namespace functionsystem::runtime_manager
