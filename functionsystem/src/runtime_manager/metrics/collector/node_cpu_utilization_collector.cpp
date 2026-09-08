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

#include "node_cpu_utilization_collector.h"

#include <algorithm>
#include <chrono>
#include <unistd.h>

#include "utils/utils.h"

namespace functionsystem::runtime_manager {

NodeCPUUtilizationCollector::NodeCPUUtilizationCollector()
    : NodeCPUUtilizationCollector(std::make_shared<ProcFSTools>())
{
}

NodeCPUUtilizationCollector::NodeCPUUtilizationCollector(const std::shared_ptr<ProcFSTools> procFSTools)
    : BaseMetricsCollector(metrics_type::CPU, collector_type::NODE, procFSTools)
{
}

litebus::Future<Metric> NodeCPUUtilizationCollector::GetUsage() const
{
    std::lock_guard<std::mutex> lock(usageMutex_);
    Metric metric;
    if (procFSTools_ == nullptr) {
        usageWindow_.Reset();
        return metric;
    }
    const auto content = procFSTools_->Read("/proc/stat");
    const auto online = procFSTools_->Read("/sys/devices/system/cpu/online");
    const auto count = online.IsSome() ? CountOnlineCPUs(online.Get()) : std::optional<uint64_t>{};
    if (content.IsNone() || !count) {
        usageWindow_.Reset();
        return metric;
    }
    const auto atNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    const auto atMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const auto sample = usageWindow_.Observe(content.Get(), atNs, atMs, sysconf(_SC_CLK_TCK));
    if (sample) {
        // The generic monitor exports percent, whereas admission uses millicores.
        metric.value = std::min(100.0, sample->millicores / (10.0 * static_cast<double>(*count)));
        metric.sampleStartUnixMs = sample->startUnixMs;
    }
    // An initial/malformed/reset sample is unknown, never fabricated idle CPU.
    return metric;
}

Metric NodeCPUUtilizationCollector::GetLimit() const
{
    YRLOG_DEBUG_COUNT_60("system memory collector get usage.");
    litebus::Promise<Metric> promise;
    Metric metric;
    return metric;
}

std::string NodeCPUUtilizationCollector::GenFilter() const
{
    // node-cpu
    return litebus::os::Join(collectorType_, metricsType_, '-');
}

std::vector<unsigned long> NodeCPUUtilizationCollector::CalCPUTime(const std::shared_ptr<ProcFSTools> procFSTools) const
{
    auto cpuinfoOption = procFSTools_->Read("/proc/stat");
    if (cpuinfoOption.IsNone()) {
        YRLOG_WARN("can not get info from /proc/stat");
        return {};
    }

    auto content = cpuinfoOption.Get();

    // content first line is like 'cpu  37277813 25 553311 226585434 38509 386500 107559 0 0 0\n'
    size_t pos = content.find('\n');
    if (pos == std::string::npos) {
        YRLOG_WARN("invalid /proc/stat content");
        return {};
    }

    std::string firstLine = content.substr(0, pos);
    std::istringstream iss(firstLine);
    std::string label;
    iss >> label; // drop 'cpu' string

    std::vector<unsigned long> cpuStats;
    unsigned long value;
    while (iss >> value) {
        cpuStats.push_back(value);
    }
    if (cpuStats.size() < MIN_VALID_TIME_ITEM_) {
        YRLOG_WARN("invalid cpu info, less than four param");
        return {};
    }

    return cpuStats;
}
}