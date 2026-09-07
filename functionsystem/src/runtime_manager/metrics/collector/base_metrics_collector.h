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

#ifndef RUNTIME_MANAGER_METRICS_COLLECTOR_BASE_METRICS_COLLECTOR_H
#define RUNTIME_MANAGER_METRICS_COLLECTOR_BASE_METRICS_COLLECTOR_H

#include <async/future.hpp>
#include <string>
#include <chrono>
#include <cstdint>

#include "common/logs/logging.h"
#include "common/resource_view/resource_type.h"
#include "common/utils/proc_fs_tools.h"

namespace functionsystem::runtime_manager {

using MetricsType = std::string;
namespace metrics_type {
const MetricsType CPU = resource_view::CPU_RESOURCE_NAME;
const MetricsType MEMORY = resource_view::MEMORY_RESOURCE_NAME;
const MetricsType GPU = resource_view::GPU_RESOURCE_NAME;
const MetricsType NPU = resource_view::NPU_RESOURCE_NAME;
const MetricsType NUMA = resource_view::NUMA_RESOURCE_NAME;
const MetricsType LABELS = resource_view::INIT_LABELS_RESOURCE_NAME;
const MetricsType DISK = resource_view::DISK_RESOURCE_NAME;
const MetricsType STORAGE = "storage";
const std::vector<MetricsType> METRICS_TYPES = { CPU, MEMORY, DISK, GPU, NPU, NUMA, LABELS };
}  // namespace metrics_type

using CollectorType = std::string;
namespace collector_type {
const CollectorType SYSTEM = "system";
const CollectorType INSTANCE = "instance";
const CollectorType NODE = "node";
}  // namespace collector_type

namespace dev_metrics_type {
const std::string VENDOR_KEY = "vendor";
const std::string PRODUCT_MODEL_KEY = "product_model";

const std::string PARTITION_KEY = "partition";
const std::string TOTAL_MEMORY_KEY = "memory";  // from Memory-Usage(MB)

const std::string USED_MEM_KEY = "usedMemory";  // from Memory-Usage(MB)
const std::string USED_HBM_KEY = "usedHBM";     // from HBM-Usage(MB)
const std::string USED_STREAM_KEY = "usedStream";
const std::string USED_LATENCY_KEY = "usedLatency";
}  // namespace dev_metrics_type

struct DevClusterMetrics {
    std::string uuid;
    size_t count;  // GPU/NPU count
    std::unordered_map<std::string, std::string> strInfo;  // partition: topology info, hbm info ...
    std::unordered_map<std::string, std::vector<int>> intsInfo;

    std::vector<resource_view::ResourceExtension> extensionInfo;
};

struct Metrics {
    litebus::Option<double> usage;
    litebus::Option<double> limit;
    litebus::Option<std::string> instanceID;
    litebus::Option<std::unordered_map<std::string, std::string>> initLabels;
    MetricsType metricsType;
    CollectorType collectorType;
    litebus::Option<DevClusterMetrics> devClusterMetrics;
    int64_t collectionStartUnixMs = 0;
};

struct Metric {
    litebus::Option<double> value;
    litebus::Option<std::string> instanceID;
    litebus::Option<DevClusterMetrics> devClusterMetrics;
    litebus::Option<std::unordered_map<std::string, std::string>> initLabels;
    int64_t sampleStartUnixMs = 0; // optional conservative start of a counter window
};

class BaseMetricsCollector {
public:
    BaseMetricsCollector(const MetricsType &metricsType, const CollectorType &collectorType)
        : BaseMetricsCollector(metricsType, collectorType, std::make_shared<ProcFSTools>())
    {
    }

    BaseMetricsCollector(const MetricsType &metricsType, const CollectorType &collectorType,
                         const std::shared_ptr<ProcFSTools> procFSTools)
        : metricsType_(metricsType), collectorType_(collectorType), procFSTools_(procFSTools)
    {
    }

    virtual ~BaseMetricsCollector() = default;

    litebus::Future<Metrics> GetMetrics() const
    {
        const auto collectionStartUnixMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        litebus::Promise<Metrics> promise;
        (void)GetUsage().OnComplete(
            [promise, collectionStartUnixMs, metricsType = metricsType_, collectorType = collectorType_,
             func = std::bind(&BaseMetricsCollector::GetLimit, this)](const litebus::Future<Metric> &future) {
                Metrics metrics;
                metrics.collectionStartUnixMs = collectionStartUnixMs;
                auto usage = future.Get();
                if (usage.sampleStartUnixMs > 0) {
                    metrics.collectionStartUnixMs = usage.sampleStartUnixMs;
                }
                metrics.usage = usage.value;
                metrics.devClusterMetrics = usage.devClusterMetrics;
                auto limit = func();
                metrics.limit = limit.value;
                metrics.instanceID = limit.instanceID;
                metrics.initLabels = limit.initLabels;
                metrics.metricsType = metricsType;
                metrics.collectorType = collectorType;
                if (metrics.instanceID.IsSome() && metrics.usage.IsSome() && metrics.limit.IsSome()) {
                    const int printFrequency = 10;
                    YRLOG_DEBUG_COUNT(printFrequency,
                                      "timer print metrics collector, instanceID: {}, type: {}, "
                                      "usage: {}, limit: {}",
                                      metrics.instanceID.Get(), metrics.metricsType, metrics.usage.Get(),
                                      metrics.limit.Get());
                }
                promise.SetValue(metrics);
            });
        return promise.GetFuture();
    }

    virtual litebus::Future<Metric> GetUsage() const = 0;
    virtual Metric GetLimit() const = 0;
    virtual std::string GenFilter() const = 0;

protected:
    MetricsType metricsType_{};
    CollectorType collectorType_{};
    std::shared_ptr<ProcFSTools> procFSTools_;
};

}  // namespace functionsystem::runtime_manager

#endif  // RUNTIME_MANAGER_METRICS_COLLECTOR_BASE_METRICS_COLLECTOR_H
