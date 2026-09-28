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

#include <arpa/inet.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <limits>
#include <string_view>
#include <unordered_set>
#include <set>

#include <utils/os_utils.hpp>

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
const std::string YR_RT_WORKING_DIR = "YR_RT_WORKING_DIR";
// Deploy-option keys
const std::string CONTAINER_ROOTFS = "rootfs";
const std::string CONTAINER_EXTRA_CONFIG = "extra_config";
const std::string CONTAINER_MOUNTS = "mounts";
const std::string CONTAINER_NETWORK = "network";
const std::string YR_IMAGE_PROCESS_CONFIG = "YR_IMAGE_PROCESS_CONFIG";
const std::string DEFAULT_IMAGE_PROCESS_CONFIG = "/tmp/yr-image-process.json";
const std::string STORAGE_RESOURCE_NAME = "storage";
// Resource defaults
constexpr double DEFAULT_CPU_MILLICORES = 500.0;
constexpr double DEFAULT_MEMORY_MB = 500.0;
constexpr uint32_t MAX_NETWORK_PORT = std::numeric_limits<uint16_t>::max();
constexpr uint32_t DEFAULT_NETWORK_RULE_PRIORITY = 100;
constexpr uint32_t PLATFORM_NETWORK_RULE_PRIORITY = std::numeric_limits<uint32_t>::max();
constexpr uint32_t MAX_USER_NETWORK_RULE_PRIORITY = PLATFORM_NETWORK_RULE_PRIORITY - 1;
constexpr size_t MAX_NETWORK_RULES = 256;
// Mount
const std::string YR_FUNCTION_LIB_PATH = "YR_FUNCTION_LIB_PATH";
const std::string FUNCTION_LIB_PATH = "FUNCTION_LIB_PATH";
const std::string GPU_DEVICE_IDS = "GPU-DEVICE-IDS";
const std::string NPU_DEVICE_IDS = "NPU-DEVICE-IDS";
const std::string CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES";
const std::string ASCEND_VISIBLE_DEVICES = "ASCEND_VISIBLE_DEVICES";
const std::string ASCEND_RT_VISIBLE_DEVICES = "ASCEND_RT_VISIBLE_DEVICES";
// Namespace alias for brevity
using namespace resource_view;  // NOLINT(google-build-using-namespace)

Status ParseDeviceIDs(const std::string &value, runtime::v1::XpuAllocation *allocation)
{
    if (value.empty()) {
        return Status(StatusCode::ERR_PARAM_INVALID, "XPU device IDs must not be empty");
    }
    std::set<uint32_t> seen;
    size_t begin = 0;
    while (begin <= value.size()) {
        const auto end = value.find(',', begin);
        const auto token = value.substr(begin, end == std::string::npos ? value.size() - begin : end - begin);
        if (token.empty()) {
            return Status(StatusCode::ERR_PARAM_INVALID, "XPU device IDs contain an empty token");
        }
        uint32_t deviceID = 0;
        const auto result = std::from_chars(token.data(), token.data() + token.size(), deviceID);
        if (result.ec != std::errc() || result.ptr != token.data() + token.size()) {
            return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("invalid XPU device ID: {}", token));
        }
        if (!seen.insert(deviceID).second) {
            return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("duplicate XPU device ID: {}", deviceID));
        }
        allocation->add_device_ids(deviceID);
        if (end == std::string::npos) {
            break;
        }
        begin = end + 1;
    }
    return Status::OK();
}

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

Status ConfigureImageEntrypoint(const std::shared_ptr<messages::StartInstanceRequest> &request,
                                runtime::v1::StartRequest &start)
{
    const auto &opts = request->runtimeinstanceinfo().deploymentconfig().deployoptions();
    const auto option = opts.find(CONTAINER_INHERIT_ENTRYPOINT);
    if (option == opts.end() || option->second == "false") {
        return Status::OK();
    }
    if (option->second != "true") {
        return Status(StatusCode::ERR_PARAM_INVALID,
                      "inherit_entrypoint must be exactly true or false");
    }
    if (start.rootfs().type() != runtime::v1::RootfsSrcType::IMAGE) {
        return Status(StatusCode::ERR_PARAM_INVALID,
                      "inherit_entrypoint requires a resolved image rootfs");
    }

    std::string target = DEFAULT_IMAGE_PROCESS_CONFIG;
    if (auto configured = litebus::os::GetEnv(YR_IMAGE_PROCESS_CONFIG);
        configured.IsSome() && !configured.Get().empty()) {
        target = configured.Get();
    }
    const auto normalized = std::filesystem::path(target).lexically_normal();
    if (!normalized.is_absolute() || normalized == "/" || normalized.string() != target) {
        return Status(StatusCode::ERR_PARAM_INVALID,
                      fmt::format("{} must be an absolute canonical file path", YR_IMAGE_PROCESS_CONFIG));
    }
    start.set_inject_entrypoint(target);
    return Status::OK();
}

Status ValidateJSONKeys(const nlohmann::json &object, const std::string &path,
                        std::initializer_list<std::string_view> allowed)
{
    if (!object.is_object()) {
        return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} must be an object", path));
    }
    for (const auto &[key, value] : object.items()) {
        (void)value;
        const auto found = std::find(allowed.begin(), allowed.end(), key);
        if (found == allowed.end()) {
            return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} contains unknown field '{}'", path, key));
        }
    }
    return Status::OK();
}

Status ParseNetworkAction(const nlohmann::json &value, const std::string &path,
                          runtime::v1::NetworkPolicyAction &action)
{
    if (!value.is_string()) {
        return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} must be 'allow' or 'deny'", path));
    }
    const auto text = value.get<std::string>();
    if (text == "allow") {
        action = runtime::v1::NETWORK_POLICY_ACTION_ALLOW;
        return Status::OK();
    }
    if (text == "deny") {
        action = runtime::v1::NETWORK_POLICY_ACTION_DENY;
        return Status::OK();
    }
    return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} must be 'allow' or 'deny'", path));
}

Status ParseNetworkDirection(const nlohmann::json &value, const std::string &path,
                             runtime::v1::NetworkDirection &direction)
{
    if (!value.is_string()) {
        return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} must be 'ingress', 'egress', or 'both'", path));
    }
    const auto text = value.get<std::string>();
    if (text == "ingress") {
        direction = runtime::v1::NETWORK_DIRECTION_INGRESS;
        return Status::OK();
    }
    if (text == "egress") {
        direction = runtime::v1::NETWORK_DIRECTION_EGRESS;
        return Status::OK();
    }
    if (text == "both") {
        direction = runtime::v1::NETWORK_DIRECTION_BOTH;
        return Status::OK();
    }
    return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} must be 'ingress', 'egress', or 'both'", path));
}

Status ParseNetworkProtocol(const nlohmann::json &value, const std::string &path,
                            runtime::v1::NetworkProtocol &protocol)
{
    if (!value.is_string()) {
        return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} must be 'any', 'tcp', 'udp', or 'icmp'", path));
    }
    const auto text = value.get<std::string>();
    if (text == "any") {
        protocol = runtime::v1::NETWORK_PROTOCOL_ANY;
        return Status::OK();
    }
    if (text == "tcp") {
        protocol = runtime::v1::NETWORK_PROTOCOL_TCP;
        return Status::OK();
    }
    if (text == "udp") {
        protocol = runtime::v1::NETWORK_PROTOCOL_UDP;
        return Status::OK();
    }
    if (text == "icmp") {
        protocol = runtime::v1::NETWORK_PROTOCOL_ICMP;
        return Status::OK();
    }
    return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} must be 'any', 'tcp', 'udp', or 'icmp'", path));
}

Status ParsePortRange(const nlohmann::json &value, const std::string &path, runtime::v1::PortRange *portRange)
{
    if (auto status = ValidateJSONKeys(value, path, { "first", "last" }); !status.IsOk()) {
        return status;
    }
    if (!value.contains("first") || !value.contains("last") || !value.at("first").is_number_integer()
        || !value.at("last").is_number_integer()) {
        return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} requires integer first and last", path));
    }
    const auto first = value.at("first").get<int64_t>();
    const auto last = value.at("last").get<int64_t>();
    if (first <= 0 || last <= 0 || first > last || last > MAX_NETWORK_PORT) {
        return Status(StatusCode::ERR_PARAM_INVALID,
                      fmt::format("{} must satisfy 1 <= first <= last <= {}", path, MAX_NETWORK_PORT));
    }
    portRange->set_first(static_cast<uint32_t>(first));
    portRange->set_last(static_cast<uint32_t>(last));
    return Status::OK();
}

bool IsValidIPv4CIDR(const std::string &value)
{
    const auto slash = value.find('/');
    const auto addressText = value.substr(0, slash);
    in_addr address{};
    if (inet_pton(AF_INET, addressText.c_str(), &address) != 1) {
        return false;
    }
    if (slash == std::string::npos) {
        return true;
    }
    if (value.find('/', slash + 1) != std::string::npos) {
        return false;
    }
    const auto prefixText = value.substr(slash + 1);
    uint32_t prefix = 0;
    const auto parsed = std::from_chars(prefixText.data(), prefixText.data() + prefixText.size(), prefix);
    return !prefixText.empty() && parsed.ec == std::errc() && parsed.ptr == prefixText.data() + prefixText.size()
           && prefix <= 32;
}

bool IsValidDomainPattern(const std::string &value)
{
    if (value.empty() || value.size() > 255 || value.find('?') != std::string::npos) {
        return false;
    }
    const auto wildcard = value.rfind("*.", 0) == 0;
    const auto remainder = wildcard ? value.substr(2) : value;
    if (remainder.empty() || remainder.size() > 253 || remainder.find('*') != std::string::npos
        || remainder.find_first_of(" \t\r\n/") != std::string::npos) {
        return false;
    }
    size_t start = 0;
    while (start <= remainder.size()) {
        const auto end = remainder.find('.', start);
        const auto length = (end == std::string::npos ? remainder.size() : end) - start;
        if (length == 0 || length > 63 || remainder[start] == '-' || remainder[start + length - 1] == '-') {
            return false;
        }
        for (size_t index = start; index < start + length; ++index) {
            const auto character = remainder[index];
            const bool asciiAlphaNumeric = (character >= 'a' && character <= 'z')
                                           || (character >= 'A' && character <= 'Z')
                                           || (character >= '0' && character <= '9');
            if (!asciiAlphaNumeric && character != '-' && character != '_') {
                return false;
            }
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return true;
}

Status ParseTrafficRule(const nlohmann::json &value, size_t index, runtime::v1::TrafficRule *rule)
{
    const auto path = fmt::format("network_policy.traffic.rules[{}]", index);
    if (auto status = ValidateJSONKeys(value, path,
                                       { "action", "direction", "protocol", "peer", "sandboxPortRange", "priority" });
        !status.IsOk()) {
        return status;
    }
    for (const auto *required : { "action", "direction", "protocol" }) {
        if (!value.contains(required)) {
            return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} requires {}", path, required));
        }
    }
    runtime::v1::NetworkPolicyAction action = runtime::v1::NETWORK_POLICY_ACTION_UNSPECIFIED;
    if (auto status = ParseNetworkAction(value.at("action"), path + ".action", action); !status.IsOk()) {
        return status;
    }
    runtime::v1::NetworkDirection direction = runtime::v1::NETWORK_DIRECTION_UNSPECIFIED;
    if (auto status = ParseNetworkDirection(value.at("direction"), path + ".direction", direction); !status.IsOk()) {
        return status;
    }
    runtime::v1::NetworkProtocol protocol = runtime::v1::NETWORK_PROTOCOL_UNSPECIFIED;
    if (auto status = ParseNetworkProtocol(value.at("protocol"), path + ".protocol", protocol); !status.IsOk()) {
        return status;
    }
    rule->set_action(action);
    rule->set_direction(direction);
    rule->set_protocol(protocol);

    uint32_t priority = DEFAULT_NETWORK_RULE_PRIORITY;
    if (value.contains("priority")) {
        if (!value.at("priority").is_number_integer()) {
            return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{}.priority must be an integer", path));
        }
        const auto parsed = value.at("priority").get<int64_t>();
        if (parsed <= 0 || parsed > MAX_USER_NETWORK_RULE_PRIORITY) {
            return Status(StatusCode::ERR_PARAM_INVALID,
                          fmt::format("{}.priority must be in 1..{}", path, MAX_USER_NETWORK_RULE_PRIORITY));
        }
        priority = static_cast<uint32_t>(parsed);
    }
    rule->set_priority(priority);

    bool hasPorts = false;
    if (value.contains("peer")) {
        const auto &peer = value.at("peer");
        if (auto status = ValidateJSONKeys(peer, path + ".peer", { "cidr", "domain", "portRange" }); !status.IsOk()) {
            return status;
        }
        const bool hasCIDR =
            peer.contains("cidr") && peer.at("cidr").is_string() && !peer.at("cidr").get<std::string>().empty();
        const bool hasDomain =
            peer.contains("domain") && peer.at("domain").is_string() && !peer.at("domain").get<std::string>().empty();
        if ((peer.contains("cidr") && !hasCIDR) || (peer.contains("domain") && !hasDomain)) {
            return Status(StatusCode::ERR_PARAM_INVALID,
                          fmt::format("{}.peer cidr and domain must be non-empty strings", path));
        }
        if (hasCIDR && hasDomain) {
            return Status(StatusCode::ERR_PARAM_INVALID,
                          fmt::format("{}.peer cannot contain both cidr and domain", path));
        }
        if (hasCIDR) {
            const auto cidr = peer.at("cidr").get<std::string>();
            if (!IsValidIPv4CIDR(cidr)) {
                return Status(StatusCode::ERR_PARAM_INVALID,
                              fmt::format("{}.peer.cidr is not an IPv4 address or CIDR", path));
            }
            rule->mutable_peer()->set_cidr(cidr);
        }
        if (hasDomain) {
            const auto domain = peer.at("domain").get<std::string>();
            if (direction != runtime::v1::NETWORK_DIRECTION_EGRESS || !IsValidDomainPattern(domain)) {
                return Status(StatusCode::ERR_PARAM_INVALID,
                              fmt::format("{}.peer.domain must be a valid egress-only domain pattern", path));
            }
            rule->mutable_peer()->set_domain(domain);
        }
        if (peer.contains("portRange")) {
            if (auto status = ParsePortRange(peer.at("portRange"), path + ".peer.portRange",
                                             rule->mutable_peer()->mutable_port_range());
                !status.IsOk()) {
                return status;
            }
            hasPorts = true;
        }
    }
    if (value.contains("sandboxPortRange")) {
        if (auto status = ParsePortRange(value.at("sandboxPortRange"), path + ".sandboxPortRange",
                                         rule->mutable_sandbox_port_range());
            !status.IsOk()) {
            return status;
        }
        hasPorts = true;
    }
    if (hasPorts && protocol != runtime::v1::NETWORK_PROTOCOL_TCP && protocol != runtime::v1::NETWORK_PROTOCOL_UDP) {
        return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} port ranges require tcp or udp", path));
    }
    return Status::OK();
}

Status ParseTrafficPolicy(const nlohmann::json &value, runtime::v1::TrafficPolicy *traffic)
{
    constexpr auto path = "network_policy.traffic";
    if (auto status = ValidateJSONKeys(value, path, { "ingressDefaultAction", "egressDefaultAction", "mode", "rules" });
        !status.IsOk()) {
        return status;
    }
    for (const auto *required : { "ingressDefaultAction", "egressDefaultAction" }) {
        if (!value.contains(required)) {
            return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} requires {}", path, required));
        }
    }
    runtime::v1::NetworkPolicyAction ingress = runtime::v1::NETWORK_POLICY_ACTION_UNSPECIFIED;
    if (auto status =
            ParseNetworkAction(value.at("ingressDefaultAction"), std::string(path) + ".ingressDefaultAction", ingress);
        !status.IsOk()) {
        return status;
    }
    runtime::v1::NetworkPolicyAction egress = runtime::v1::NETWORK_POLICY_ACTION_UNSPECIFIED;
    if (auto status =
            ParseNetworkAction(value.at("egressDefaultAction"), std::string(path) + ".egressDefaultAction", egress);
        !status.IsOk()) {
        return status;
    }
    traffic->set_ingress_default_action(ingress);
    traffic->set_egress_default_action(egress);
    traffic->set_mode(runtime::v1::TRAFFIC_POLICY_MODE_STATEFUL);
    if (value.contains("mode")) {
        if (!value.at("mode").is_string()) {
            return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{}.mode must be stateful or stateless", path));
        }
        const auto mode = value.at("mode").get<std::string>();
        if (mode == "stateful") {
            traffic->set_mode(runtime::v1::TRAFFIC_POLICY_MODE_STATEFUL);
        } else if (mode == "stateless") {
            traffic->set_mode(runtime::v1::TRAFFIC_POLICY_MODE_STATELESS);
        } else {
            return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{}.mode must be stateful or stateless", path));
        }
    }
    if (!value.contains("rules")) {
        return Status::OK();
    }
    if (!value.at("rules").is_array() || value.at("rules").size() > MAX_NETWORK_RULES) {
        return Status(StatusCode::ERR_PARAM_INVALID,
                      fmt::format("{}.rules must be an array with at most {} entries", path, MAX_NETWORK_RULES));
    }
    size_t index = 0;
    for (const auto &rule : value.at("rules")) {
        if (auto status = ParseTrafficRule(rule, index, traffic->add_rules()); !status.IsOk()) {
            return status;
        }
        ++index;
    }
    return Status::OK();
}

Status ParseDNSPolicy(const nlohmann::json &value, runtime::v1::DNSPolicy *dns)
{
    constexpr auto path = "network_policy.dns";
    if (auto status = ValidateJSONKeys(value, path, { "defaultAction", "rules" }); !status.IsOk()) {
        return status;
    }
    if (!value.contains("defaultAction")) {
        return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{} requires defaultAction", path));
    }
    runtime::v1::NetworkPolicyAction defaultAction = runtime::v1::NETWORK_POLICY_ACTION_UNSPECIFIED;
    if (auto status =
            ParseNetworkAction(value.at("defaultAction"), std::string(path) + ".defaultAction", defaultAction);
        !status.IsOk()) {
        return status;
    }
    dns->set_default_action(defaultAction);
    if (!value.contains("rules")) {
        return Status::OK();
    }
    if (!value.at("rules").is_array()) {
        return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{}.rules must be an array", path));
    }
    size_t index = 0;
    for (const auto &valueRule : value.at("rules")) {
        const auto rulePath = fmt::format("{}.rules[{}]", path, index);
        if (auto status = ValidateJSONKeys(valueRule, rulePath, { "action", "pattern" }); !status.IsOk()) {
            return status;
        }
        if (!valueRule.contains("action") || !valueRule.contains("pattern") || !valueRule.at("pattern").is_string()) {
            return Status(StatusCode::ERR_PARAM_INVALID,
                          fmt::format("{} requires action and a string pattern", rulePath));
        }
        runtime::v1::NetworkPolicyAction action = runtime::v1::NETWORK_POLICY_ACTION_UNSPECIFIED;
        if (auto status = ParseNetworkAction(valueRule.at("action"), rulePath + ".action", action); !status.IsOk()) {
            return status;
        }
        const auto pattern = valueRule.at("pattern").get<std::string>();
        if (!IsValidDomainPattern(pattern)) {
            return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("{}.pattern is invalid", rulePath));
        }
        auto *rule = dns->add_rules();
        rule->set_action(action);
        rule->set_pattern(pattern);
        ++index;
    }
    return Status::OK();
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
    for (const auto *key : { "endpoint", "bucket", "object" }) {
        if (!HasNonEmptyString(storageInfo, key)) {
            return InvalidRootfsConfig(fmt::format("s3 rootfs requires non-empty storageInfo.{}", key));
        }
    }
    for (const auto *key : { "accessKey", "secretKey" }) {
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

Status SandboxdRequestBuilder::AttachCheckpointInfo(runtime::v1::StartRequest &request,
                                                    const std::string &directory)
{
    const auto checkpointDirectory = std::filesystem::path(directory).lexically_normal();
    if (!checkpointDirectory.is_absolute()) {
        return Status(StatusCode::ERR_PARAM_INVALID, "checkpoint directory must be absolute");
    }
    request.mutable_checkpoint_info()->set_checkpoint_dir(checkpointDirectory.string());
    return Status::OK();
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

    // Ordinary starts let sandboxd generate the physical identity. Trusted
    // resume supplies a deterministic sbox-* identity so replay and uncertain
    // results can query the exact physical fact.
    if (!params.sandboxID.empty()) {
        start->set_sandbox_id(params.sandboxID);
    }

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
        return { s, nullptr };
    }
    if (auto s = ConfigureImageEntrypoint(params.request, *start); !s.IsOk()) {
        return { s, nullptr };
    }

    ApplyExtraConfig(params.request, start.get());
    // Bind identities from the executor's control request, never user envs or
    // deploy options. sandboxd adds the physical generation independently.
    const auto &instanceID = params.request->runtimeinstanceinfo().instanceid();
    if (!instanceID.empty() && !params.runtimeID.empty()) {
        (*start->mutable_labels())["akernel.scheduler/instance-id"] = instanceID;
        (*start->mutable_labels())["akernel.scheduler/runtime-id"] = params.runtimeID;
    }
    if (auto status = ApplyNetworkPolicy(params.request, params.portMappings, start.get()); !status.IsOk()) {
        return { status, nullptr };
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
                return { status, nullptr };
            }
        }
        // Network mode (optional); empty => sandbox network on the sandboxd side.
        if (auto netIt = opts.find(CONTAINER_NETWORK); netIt != opts.end()) {
            start->set_network(netIt->second);
        }
    }

    ApplyResources(params.request, start->mutable_resources());
    if (auto status = ApplyElasticResources(params.request, start.get()); !status.IsOk()) {
        return {status, nullptr};
    }
    if (auto status = ApplyWritableLayerSize(params.request, start.get()); !status.IsOk()) {
        return { status, nullptr };
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
    if (auto status = ApplyXpuAllocations(params.envs, start.get()); !status.IsOk()) {
        return {status, nullptr};
    }
    ApplyEnvsAndLogs(updatedEnvs, params.runtimeID, start.get());

    // YR_LANGUAGE follows the service runtime field. The container runtime is
    // the sandbox backend (for example runc/runsc), not the user runtime.
    (*start->mutable_envs())["YR_LANGUAGE"] = ResolveRuntimeLanguage(params.request);
    if (start->inject_entrypoint().empty()) {
        start->mutable_envs()->erase(YR_IMAGE_PROCESS_CONFIG);
    } else {
        // FunctionSystem owns runtime discovery. sandboxd consumes the field
        // only to inject the read-only process spec at the same path.
        (*start->mutable_envs())[YR_IMAGE_PROCESS_CONFIG] = start->inject_entrypoint();
    }

    // trace_id is the distributed trace ID propagated from the upstream request
    // (runtimeinstanceinfo().traceid()), not the local runtimeID.
    start->set_trace_id(params.request->runtimeinstanceinfo().traceid());

    return { Status::OK(), std::move(start) };
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

    updated.posixEnvs[UNZIPPED_WORKING_DIR] = code->target();
    updated.posixEnvs[YR_FUNCTION_LIB_PATH] = code->target();
    updated.posixEnvs[FUNCTION_LIB_PATH] = code->target();

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
    const auto bootstrapCommands = BuildBootstrapCommands(request);
    if (bootstrapCommands.empty()) {
        if (!cmdArgs.execPath.empty()) {
            *start->add_command() = cmdArgs.execPath;
        }
    } else {
        for (const auto &cmd : bootstrapCommands) {
            *start->add_command() = cmd;
        }
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

Status SandboxdRequestBuilder::ApplyElasticResources(
    const std::shared_ptr<messages::StartInstanceRequest> &request, runtime::v1::StartRequest *start) const
{
    auto options = request->runtimeinstanceinfo().deploymentconfig().deployoptions();
    // The SDK ships elastic options inside the opaque extra_config JSON map;
    // surface them as sibling deploy options so the flag check below sees them
    // without requiring every frontend to flatten the map (2026-09-15: the
    // wheel frontend forwards body.extra_config verbatim as one option).
    if (options.find(SCHEDULER_ELASTIC_RESOURCES) == options.end()) {
        if (auto extra = options.find(CONTAINER_EXTRA_CONFIG); extra != options.end()) {
            try {
                auto parsed = nlohmann::json::parse(extra->second);
                if (parsed.is_object()) {
                    for (const auto &item : parsed.items()) {
                        options[item.key()] = item.value().is_string()
                            ? item.value().get<std::string>()
                            : item.value().dump();
                    }
                }
            } catch (const std::exception &) {
                // Not a JSON object: leave options unchanged; the plain lookup
                // below keeps its original semantics.
            }
        }
    }
    const auto enabled = options.find(SCHEDULER_ELASTIC_RESOURCES);
    if (enabled == options.end() || enabled->second == "false") {
        return Status::OK();
    }
    if (enabled->second != "true" || start->runtime() != "runsc") {
        return Status(StatusCode::ERR_PARAM_INVALID, "elastic resources require true and runtime runsc");
    }
    const auto &resources = request->runtimeinstanceinfo().runtimeconfig().resources().resources();
    nlohmann::json policy = {{"version", 1}};
    // A named reusable-snapshot restore carries a restoreSnapshotID on the
    // runtime instance info. Its physical restore loading (sentry page
    // loading, gofer, kernel state deserialization) transiently needs more
    // memory than the steady-state working set; the authorized elastic
    // ceiling was already reserved by the scheduler's admission. Create the
    // physical cgroup at that ceiling. The elastic policy label still
    // records the ORIGINAL initial/ceiling: the existing shrink path
    // returns the limit to initial after the restore completes.
    const bool isNamedRestore = !request->runtimeinstanceinfo().restoresnapshotid().empty();
    for (const auto &name : {CPU_RESOURCE_NAME, MEMORY_RESOURCE_NAME}) {
        auto it = resources.find(name);
        if (it == resources.end() || it->second.type() != ValueType::Value_Type_SCALAR
            || !it->second.has_scalar()) {
            return Status(StatusCode::ERR_PARAM_INVALID, "elastic resources require explicit CPU and Memory scalars");
        }
        const double initial = it->second.scalar().value();
        const double limit = it->second.scalar().limit();
        const double ceiling = limit == 0 ? initial : limit;
        if (!std::isfinite(initial) || !std::isfinite(ceiling) || initial <= 0 || ceiling < initial
            || std::floor(initial) != initial || std::floor(ceiling) != ceiling) {
            return Status(StatusCode::ERR_PARAM_INVALID, "elastic resource limits must be positive integers with ceiling >= initial");
        }
        if (isNamedRestore && name == MEMORY_RESOURCE_NAME) {
            // ceiling==initial is a valid finite policy: provisioning at
            // ceiling is the same as provisioning at initial in that case.
            // No additional rejection here — scheduling-side admission
            // already applies the standard positive/finite/ceiling>=initial
            // contract uniformly at entry.
            (*start->mutable_resources())[name] = ceiling;
        } else {
            (*start->mutable_resources())[name] = initial;
        }
        policy[name] = {{"initial", initial}, {"ceiling", ceiling}};
    }
    // Persist with sandbox metadata and checkpoints; limits are not lost when
    // the effective cgroup limit changes. CPU is millicores, Memory is MiB.
    (*start->mutable_labels())["akernel.scheduler/elastic-resources"] = policy.dump();
    return Status::OK();
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
    if (!std::isfinite(storage) || storage <= 0 || std::floor(storage) != storage
        || storage > static_cast<double>(std::numeric_limits<uint64_t>::max())) {
        return Status(StatusCode::ERR_PARAM_INVALID, "storage resource must be a positive integer number of bytes");
    }
    const auto storageBytes = static_cast<uint64_t>(storage);
    // Field 19 is sandboxd's canonical hard quota. Keep the rootfs field 6
    // compatibility alias populated for older sandboxd deployments.
    start->set_writable_layer_limit_bytes(storageBytes);
    start->mutable_rootfs()->set_writable_layer_size_bytes(storageBytes);
    return Status::OK();
}

Status SandboxdRequestBuilder::ApplyXpuAllocations(const Envs &envs, runtime::v1::StartRequest *start) const
{
    const auto gpuIter = envs.userEnvs.find(GPU_DEVICE_IDS);
    const auto npuIter = envs.userEnvs.find(NPU_DEVICE_IDS);
    const bool hasGPU = gpuIter != envs.userEnvs.end();
    const bool hasNPU = npuIter != envs.userEnvs.end();
    if (hasGPU && hasNPU) {
        return Status(StatusCode::ERR_PARAM_INVALID, "GPU and NPU allocations cannot be requested together");
    }
    if (!hasGPU && !hasNPU) {
        return Status::OK();
    }

    auto *allocation = start->add_xpu_allocations();
    const auto &iter = hasNPU ? npuIter : gpuIter;
    allocation->set_type(hasNPU ? "npu" : "gpu");
    if (auto status = ParseDeviceIDs(iter->second, allocation); !status.IsOk()) {
        start->mutable_xpu_allocations()->RemoveLast();
        return status;
    }
    return Status::OK();
}

void SandboxdRequestBuilder::ApplyEnvsAndLogs(const Envs &envs, const std::string &runtimeID,
                                              runtime::v1::StartRequest *start) const
{
    const auto &config = cmdBuilder_.GetConfig();
    const std::string logDir = litebus::os::Join(config.runtimeLogPath, config.runtimeStdLogDir);

    // Flat request carries a single envs map (no separate runtime/user envs).
    const auto combined = cmdBuilder_.CombineEnvs(envs);
    for (const auto &[key, value] : combined) {
        // Physical IDs and provider-owned visibility variables are consumed by
        // sandboxd's XPU provider. Forwarding either would let a caller
        // override the trusted OCI edits and is rejected by sandboxd.
        if (key != GPU_DEVICE_IDS && key != NPU_DEVICE_IDS && key != CUDA_VISIBLE_DEVICES &&
            key != ASCEND_VISIBLE_DEVICES && key != ASCEND_RT_VISIBLE_DEVICES) {
            (*start->mutable_envs())[key] = value;
        }
    }
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

Status SandboxdRequestBuilder::ApplyNetworkPolicy(const std::shared_ptr<messages::StartInstanceRequest> &request,
                                                  const std::vector<std::string> &portMappings,
                                                  runtime::v1::StartRequest *start) const
{
    const auto &opts = request->runtimeinstanceinfo().deploymentconfig().deployoptions();

    try {
        auto iter = opts.find(CONTAINER_NETWORK_POLICY);
        if (iter == opts.end() || iter->second.empty()) {
            return Status::OK();
        }
        auto policy = nlohmann::json::parse(iter->second);
        if (auto status = ValidateJSONKeys(policy, "network_policy",
                                           { "blockNetwork", "dnsBlacklist", "schemaVersion", "traffic", "dns" });
            !status.IsOk()) {
            return status;
        }

        const auto addProtectedRules = [&](runtime::v1::TrafficPolicy *traffic, bool schemaV2) -> Status {
            const auto &config = cmdBuilder_.GetConfig();
            in_addr address{};
            if (inet_pton(AF_INET, config.proxyIP.c_str(), &address) != 1) {
                return Status(StatusCode::ERR_PARAM_INVALID,
                              fmt::format("proxy IP '{}' is not a valid IPv4 address", config.proxyIP));
            }
            uint32_t port = 0;
            const auto &portText = config.proxyGrpcServerPort;
            const auto parsed = std::from_chars(portText.data(), portText.data() + portText.size(), port);
            if (parsed.ec != std::errc() || parsed.ptr != portText.data() + portText.size() || port == 0
                || port > MAX_NETWORK_PORT) {
                return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("proxy gRPC port '{}' is invalid", portText));
            }

            if (traffic->rules_size() >= static_cast<int>(MAX_NETWORK_RULES)) {
                return Status(StatusCode::ERR_PARAM_INVALID,
                              fmt::format("network policy leaves no room for the protected Function Proxy rule; "
                                          "the combined limit is {} rules",
                                          MAX_NETWORK_RULES));
            }
            auto *rule = traffic->add_rules();
            rule->set_action(runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
            rule->set_direction(runtime::v1::NETWORK_DIRECTION_BOTH);
            rule->set_protocol(runtime::v1::NETWORK_PROTOCOL_TCP);
            if (schemaV2) {
                rule->set_priority(PLATFORM_NETWORK_RULE_PRIORITY);
                rule->mutable_peer()->set_cidr(config.proxyIP + "/32");
                rule->mutable_peer()->mutable_port_range()->set_first(port);
                rule->mutable_peer()->mutable_port_range()->set_last(port);
            } else {
                rule->mutable_peer()->set_address(config.proxyIP);
                rule->mutable_peer()->set_port(port);
            }

            std::unordered_set<std::string> publishedTargets;
            for (const auto &mapping : portMappings) {
                const auto firstSeparator = mapping.find(':');
                const auto secondSeparator =
                    firstSeparator == std::string::npos ? std::string::npos : mapping.find(':', firstSeparator + 1);
                if (firstSeparator == std::string::npos || secondSeparator == std::string::npos
                    || mapping.find(':', secondSeparator + 1) != std::string::npos) {
                    return Status(StatusCode::ERR_PARAM_INVALID,
                                  fmt::format("invalid sandboxd port mapping '{}'", mapping));
                }
                const auto protocolText = mapping.substr(0, firstSeparator);
                runtime::v1::NetworkProtocol protocol = runtime::v1::NETWORK_PROTOCOL_UNSPECIFIED;
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
                if (targetParsed.ec != std::errc() || targetParsed.ptr != targetText.data() + targetText.size()
                    || targetPort == 0 || targetPort > MAX_NETWORK_PORT) {
                    return Status(StatusCode::ERR_PARAM_INVALID,
                                  fmt::format("invalid sandboxd target port in mapping '{}'", mapping));
                }
                const auto targetKey = protocolText + ":" + targetText;
                if (!publishedTargets.insert(targetKey).second) {
                    continue;
                }
                if (traffic->rules_size() >= static_cast<int>(MAX_NETWORK_RULES)) {
                    return Status(StatusCode::ERR_PARAM_INVALID,
                                  fmt::format("network policy plus protected published ports exceeds {} rules",
                                              MAX_NETWORK_RULES));
                }
                auto *published = traffic->add_rules();
                published->set_action(runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
                published->set_direction(runtime::v1::NETWORK_DIRECTION_INGRESS);
                published->set_protocol(protocol);
                if (schemaV2) {
                    published->set_priority(PLATFORM_NETWORK_RULE_PRIORITY);
                    published->mutable_sandbox_port_range()->set_first(targetPort);
                    published->mutable_sandbox_port_range()->set_last(targetPort);
                } else {
                    published->set_sandbox_port(targetPort);
                }
            }
            return Status::OK();
        };

        if (policy.contains("schemaVersion")) {
            if (!policy.at("schemaVersion").is_number_integer() || policy.at("schemaVersion").get<int64_t>() != 2) {
                return Status(StatusCode::ERR_PARAM_INVALID, "network_policy.schemaVersion must be 2");
            }
            if (policy.contains("blockNetwork") || policy.contains("dnsBlacklist")) {
                return Status(StatusCode::ERR_PARAM_INVALID,
                              "legacy and schema v2 network policy fields cannot be combined");
            }
            if (!policy.contains("traffic") && !policy.contains("dns")) {
                return Status::OK();
            }
            auto *networkPolicy = start->mutable_network_policy();
            networkPolicy->set_schema_version(2);
            if (policy.contains("traffic")) {
                auto *traffic = networkPolicy->mutable_traffic();
                if (auto status = ParseTrafficPolicy(policy.at("traffic"), traffic); !status.IsOk()) {
                    return status;
                }
                if (auto status = addProtectedRules(traffic, true); !status.IsOk()) {
                    return status;
                }
            }
            if (policy.contains("dns")) {
                if (auto status = ParseDNSPolicy(policy.at("dns"), networkPolicy->mutable_dns()); !status.IsOk()) {
                    return status;
                }
            }
            return Status::OK();
        }

        if (policy.contains("traffic") || policy.contains("dns")) {
            return Status(StatusCode::ERR_PARAM_INVALID, "schema v2 network fields require schemaVersion 2");
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
                if (!pattern.is_string() || !IsValidDomainPattern(pattern.get<std::string>())) {
                    return Status(StatusCode::ERR_PARAM_INVALID,
                                  "network_policy.dnsBlacklist entries must be valid domain patterns");
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
            auto *traffic = networkPolicy->mutable_traffic();
            traffic->set_default_action(runtime::v1::NETWORK_POLICY_ACTION_DENY);
            traffic->set_mode(runtime::v1::TRAFFIC_POLICY_MODE_STATEFUL);
            if (auto status = addProtectedRules(traffic, false); !status.IsOk()) {
                return status;
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
        return Status(StatusCode::ERR_PARAM_INVALID, fmt::format("failed to parse network_policy JSON: {}", e.what()));
    }
    return Status::OK();
}

Status SandboxdRequestBuilder::BuildNetworkPolicy(const std::string &policyJSON,
                                                   const std::vector<std::string> &portMappings,
                                                   runtime::v1::NetworkPolicy *networkPolicy) const
{
    if (networkPolicy == nullptr) {
        return Status(StatusCode::ERR_PARAM_INVALID, "network policy output is null");
    }
    auto request = std::make_shared<messages::StartInstanceRequest>();
    auto *options = request->mutable_runtimeinstanceinfo()
                        ->mutable_deploymentconfig()
                        ->mutable_deployoptions();
    (*options)[CONTAINER_NETWORK_POLICY] = policyJSON;
    runtime::v1::StartRequest start;
    auto status = ApplyNetworkPolicy(request, portMappings, &start);
    if (!status.IsOk()) {
        return status;
    }
    networkPolicy->Clear();
    if (start.has_network_policy()) {
        networkPolicy->CopyFrom(start.network_policy());
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
