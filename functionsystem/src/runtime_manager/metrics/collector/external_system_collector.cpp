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
#include "external_system_collector.h"

#include <algorithm>
#include <cctype>
#include <limits>
#include <unordered_set>

#include "curl/curl.h"
#include "nlohmann/json.hpp"

#include "common/resource_view/resource_type.h"
#include "utils/os_utils.hpp"
namespace functionsystem::runtime_manager {
const uint32_t CPU_SCALE = 1000;
const uint32_t MEMORY_SCALE = 1024 * 1024;
constexpr long HTTP_OK_STATUS = 200;
constexpr int64_t MAX_EXACT_DOUBLE_INTEGER = 9007199254740992;

namespace {
std::string ToLower(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

litebus::Option<DevClusterMetrics> ParseExternalXpuDevice(const nlohmann::json &xpu)
{
    if (!xpu.contains("product_model") || !xpu["product_model"].is_string() ||
        !xpu.contains("device_ids") || !xpu["device_ids"].is_array()) {
        return {};
    }

    auto model = ToLower(xpu["product_model"].get<std::string>());
    if (model.empty()) {
        return {};
    }

    std::vector<int> deviceIDs;
    std::unordered_set<int> uniqueIDs;
    for (const auto &id : xpu["device_ids"]) {
        if (!id.is_number_integer()) {
            return {};
        }
        const auto deviceID = id.get<int64_t>();
        if (deviceID < 0 || deviceID > std::numeric_limits<int>::max()) {
            return {};
        }
        const auto normalizedID = static_cast<int>(deviceID);
        if (!uniqueIDs.insert(normalizedID).second) {
            return {};
        }
        deviceIDs.emplace_back(normalizedID);
    }
    if (deviceIDs.empty()) {
        return {};
    }

    // Preserve reported order because realIDs-to-cardIDs mapping is positional.
    DevClusterMetrics device;
    device.count = deviceIDs.size();
    device.strInfo[dev_metrics_type::PRODUCT_MODEL_KEY] = model;
    device.intsInfo[resource_view::HETEROGENEOUS_CARDNUM_KEY] = std::vector<int>(deviceIDs.size(), 1);
    device.intsInfo[resource_view::IDS_KEY] = std::move(deviceIDs);
    return device;
}
}  // namespace

litebus::Option<Metric> ParseExternalXpuMetric(const std::string &response, const std::string &expectedType)
{
    try {
        auto root = nlohmann::json::parse(response);
        if (!root.contains("xpu") || !root["xpu"].is_array()) {
            return {};
        }
        if (root["xpu"].empty()) {
            return Metric{};
        }

        litebus::Option<Metric> xpuMetric;
        size_t matchingEntryCount = 0;
        const auto normalizedExpectedType = ToLower(expectedType);
        if (normalizedExpectedType != "gpu" && normalizedExpectedType != "npu") {
            return {};
        }
        for (const auto &xpu : root["xpu"]) {
            if (!xpu.is_object() || !xpu.contains("type") || !xpu["type"].is_string()) {
                return {};
            }

            auto type = ToLower(xpu["type"].get<std::string>());
            if (type != "gpu" && type != "npu") {
                return {};
            }
            if (type != normalizedExpectedType) {
                continue;
            }
            ++matchingEntryCount;
            if (matchingEntryCount > 1) {
                return {};
            }
            auto device = ParseExternalXpuDevice(xpu);
            if (device.IsNone()) {
                return {};
            }

            auto parsedDevice = device.Get();
            Metric metric;
            metric.value = static_cast<double>(parsedDevice.count);
            metric.devClusterMetrics = std::move(parsedDevice);
            xpuMetric = std::move(metric);
        }
        // A syntactically valid response without this type is an explicit
        // absence and must clear the previous capacity for this collector.
        return matchingEntryCount == 1 ? xpuMetric : litebus::Option<Metric>{Metric{}};
    } catch (const std::exception &e) {
        YRLOG_DEBUG_COUNT_60("Failed to parse external XPU response: {}, error: {}", response, e.what());
    }
    return {};
}

litebus::Option<Metric> ParseExternalGpuMetric(const std::string &response)
{
    return ParseExternalXpuMetric(response, "gpu");
}

litebus::Option<Metric> ParseExternalStorageMetric(const std::string &response)
{
    try {
        const auto root = nlohmann::json::parse(response);
        if (!root.contains(metrics_type::STORAGE) || !root[metrics_type::STORAGE].is_number_integer()) {
            return {};
        }
        const auto storage = root[metrics_type::STORAGE].get<int64_t>();
        if (storage < 0 || storage > MAX_EXACT_DOUBLE_INTEGER) {
            return {};
        }
        return Metric{ { static_cast<double>(storage) }, {}, {}, {} };
    } catch (const std::exception &e) {
        YRLOG_DEBUG_COUNT_60("Failed to parse external storage response: {}, error: {}", response, e.what());
    }
    return {};
}

size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* userp)
{
    size_t totalSize = size * nmemb;
    userp->append(static_cast<char*>(contents), totalSize);
    return totalSize;
}

CurlHelper::CurlHelper(const std::string &name, const std::string &externEndpoint, const std::string &url)
    : litebus::ActorBase(name), externEndpoint_(externEndpoint), url_(url)
{
    auto error = curl_global_init(CURL_GLOBAL_ALL);
    if (error) {
        std::cerr << "<CurlHelper> failed to initialize global curl!" << std::endl;
        return;
    }
    curl_ = curl_easy_init();
    if (!curl_) {
        curl_global_cleanup();
        std::cerr << "<CurlHelper> failed to initialize easy curl!" << std::endl;
        return;
    }
};

CurlHelper::~CurlHelper()
{
    curl_easy_cleanup(curl_);
    curl_global_cleanup();
    if (curl_ != nullptr) {
        curl_ = nullptr;
    }
}

litebus::Future<std::string> CurlHelper::Query()
{
    if (!curl_) {
        YRLOG_ERROR("Failed to initialize libcurl");
        return "";
    }
    curl_easy_reset(curl_);
    std::string response;
    curl_easy_setopt(curl_, CURLOPT_UNIX_SOCKET_PATH, externEndpoint_.c_str());
    curl_easy_setopt(curl_, CURLOPT_URL, url_.c_str());
    curl_easy_setopt(curl_, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response);
    CURLcode res = curl_easy_perform(curl_);
    if (res != CURLE_OK) {
        YRLOG_DEBUG_COUNT_60("curl_easy_perform() failed: {}", curl_easy_strerror(res));
        return "";
    }
    long httpCode = 0;
    curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &httpCode);
    if (httpCode != HTTP_OK_STATUS) {
        YRLOG_DEBUG_COUNT_60("{} for {} with status code: {}, response: {}", externEndpoint_, url_, httpCode,
                             response);
        return "";
    }
    return response;
}

litebus::Future<std::string> ExternalSystemCollector::CollectFromExternal() const
{
    return litebus::Async(curlActorRef_->GetAID(), &CurlHelper::Query);
}

litebus::Future<Metric> ExternalSystemCPUCollector::GetUsage() const
{
    return CollectFromExternal().Then([previous(previous_), totalPtr(total_)](const std::string& response) -> litebus::Future<Metric> {
        YRLOG_DEBUG_COUNT_60("Received CPU response: {}", response);
        // Unknown observations (empty reply = transport error or non-200
        // such as the daemon's stale-snapshot 503, unknown sample, parse
        // failure) return an explicitly-unknown metric: no value, no
        // refreshed clock, no stale usage.
        const auto unknown = [] { return Metric{}; };
        if (response.empty()) {
            return unknown();
        }
        try {
            // Only an explicit, known physical sample may update usage.
            // The legacy cpu field means AVAILABLE and is never read as
            // usage.
            auto sample = ParseExternalNodeResource(response);
            if (sample.known) {
                auto metric = Metric{};
                metric.value = sample.cpuUsedCores * CPU_SCALE;
                metric.observationKnown = true;
                *previous = metric;
                if (totalPtr != nullptr) {
                    auto totalMetric = std::make_shared<Metric>();
                    totalMetric->value = sample.cpuTotalCores * CPU_SCALE;
                    totalMetric->observationKnown = true;
                    *totalPtr = std::move(*totalMetric);
                }
                return metric;
            }
            return unknown();
        } catch (const std::exception& e) {
            YRLOG_DEBUG_COUNT_60("Failed to parse JSON response: {}, error: {}", response, e.what());
        }
        return unknown();
    });
}

Metric ExternalSystemCPUCollector::GetLimit() const
{
    if (total_ != nullptr && total_->value.IsSome()) {
        return *total_;
    }
    if (previous_ == nullptr) {
        YRLOG_DEBUG_COUNT_60("Failed to Get CPU Limit, fallback to default");
        return this->GetLimit();
    }
    return *previous_;
}

litebus::Future<Metric> ExternalSystemMemoryCollector::GetUsage() const
{
    return CollectFromExternal().Then([previous(previous_), totalPtr(total_)](const std::string& response) -> litebus::Future<Metric> {
        YRLOG_DEBUG_COUNT_60("Received Memory response: {}", response);
        const auto unknown = [] { return Metric{}; };
        if (response.empty()) {
            return unknown();
        }
        try {
            auto sample = ParseExternalNodeResource(response);
            if (sample.known) {
                auto metric = Metric{};
                metric.value = sample.memUsedMb;
                metric.observationKnown = true;
                *previous = metric;
                if (totalPtr != nullptr) {
                    auto totalMetric = std::make_shared<Metric>();
                    totalMetric->value = sample.memTotalMb;
                    totalMetric->observationKnown = true;
                    *totalPtr = std::move(*totalMetric);
                }
                return metric;
            }
            return unknown();
        } catch (const std::exception& e) {
            YRLOG_DEBUG_COUNT_60("Failed to parse JSON response: {}, error: {}", response, e.what());
        }
        return unknown();
    });
}
Metric ExternalSystemMemoryCollector::GetLimit() const
{
    if (total_ != nullptr && total_->value.IsSome()) {
        return *total_;
    }
    if (previous_ == nullptr) {
        YRLOG_DEBUG_COUNT_60("Failed to Get Memory Limit, fallback to default");
        return this->GetLimit();
    }
    return *previous_;
}

ExternalSystemXPUCollector::ExternalSystemXPUCollector(const MetricsType &metricsType,
                                                       const std::string &externalType,
                                                       const litebus::ActorReference &curlActorRef)
    : ExternalSystemCollector(0, metricsType, curlActorRef), externalType_(ToLower(externalType)),
      previous_(std::make_shared<Metric>())
{
    uuid_ = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
}

litebus::Future<Metric> ExternalSystemXPUCollector::GetUsage() const
{
    return CollectFromExternal().Then(
        [previous(previous_), uuid(uuid_), externalType(externalType_)](
            const std::string &response) -> litebus::Future<Metric> {
            YRLOG_DEBUG_COUNT_60("Received external {} response: {}", externalType, response);
            if (response.empty()) {
                return previous != nullptr ? *previous : Metric{};
            }
            auto parsed = ParseExternalXpuMetric(response, externalType);
            if (parsed.IsNone()) {
                return previous != nullptr ? *previous : Metric{};
            }
            auto metric = parsed.Get();
            if (metric.devClusterMetrics.IsSome()) {
                auto device = metric.devClusterMetrics.Get();
                device.uuid = uuid;
                metric.devClusterMetrics = std::move(device);
            }
            *previous = metric;
            return metric;
        });
}

Metric ExternalSystemXPUCollector::GetLimit() const
{
    return previous_ != nullptr ? *previous_ : Metric{};
}

ExternalSystemStorageCollector::ExternalSystemStorageCollector(const litebus::ActorReference &curlActorRef)
    : ExternalSystemCollector(0, metrics_type::STORAGE, curlActorRef), previous_(std::make_shared<Metric>())
{
}

litebus::Future<Metric> ExternalSystemStorageCollector::GetUsage() const
{
    return CollectFromExternal().Then(
        [previous(previous_)](const std::string &response) -> litebus::Future<Metric> {
            YRLOG_DEBUG_COUNT_60("Received storage response: {}", response);
            if (!response.empty()) {
                auto parsed = ParseExternalStorageMetric(response);
                if (parsed.IsSome()) {
                    *previous = parsed.Get();
                }
            }
            return Metric{ { 0.0 }, {}, {}, {} };
        });
}

Metric ExternalSystemStorageCollector::GetLimit() const
{
    return previous_ != nullptr ? *previous_ : Metric{};
}
ExternalNodeResourceSample ParseExternalNodeResource(const std::string &response)
{
    ExternalNodeResourceSample sample;
    try {
        auto j = nlohmann::json::parse(response);
        if (!j.is_object()) {
            return sample;
        }
        // Protocol: totalCpu/totalMem/usedCpu/usedMem (always emitted) plus
        // usedKnown. Anything less is an old server or an unknown physical
        // read; both are unknown samples, never available-as-usage.
        if (!j.contains("usedKnown") || !j["usedKnown"].get<bool>()) {
            return sample;
        }
        const bool cpuOk = j.contains("usedCpu") && j["usedCpu"].is_number()
            && j.contains("totalCpu") && j["totalCpu"].is_number();
        const bool memOk = j.contains("usedMem") && j["usedMem"].is_number()
            && j.contains("totalMem") && j["totalMem"].is_number();
        if (!cpuOk || !memOk) {
            return sample;
        }
        sample.cpuUsedCores = j["usedCpu"].get<double>();
        sample.cpuTotalCores = j["totalCpu"].get<double>();
        sample.memUsedMb = double(j["usedMem"].get<int64_t>()) / MEMORY_SCALE;
        sample.memTotalMb = double(j["totalMem"].get<int64_t>()) / MEMORY_SCALE;
        sample.known = true;
        return sample;
    } catch (const std::exception &) {
        return sample;
    }
}

}  // namespace functionsystem::runtime_manager
