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

#include "node_cpu_collector.h"

#include <unistd.h>

#include "common/constants/constants.h"
#include "common/logs/logging.h"

namespace functionsystem::runtime_manager {
constexpr double CPU_CALC_BASE = 1000.0;

NodeCPUCollector::NodeCPUCollector() : NodeCPUCollector(std::make_shared<ProcFSTools>())
{
}

NodeCPUCollector::NodeCPUCollector(const std::shared_ptr<ProcFSTools> procFSTools, double overheadCPU)
    : BaseMetricsCollector(metrics_type::CPU, collector_type::NODE, procFSTools), overheadCPU_(overheadCPU)
{
}

litebus::Future<Metric> NodeCPUCollector::GetUsage() const
{
    std::lock_guard<std::mutex> lock(usageMutex_);
    Metric metric;
    const auto sampleUnixMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const auto sampleNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    const auto content = procFSTools_ ? procFSTools_->Read("/proc/stat") : litebus::Option<std::string>{};
    if (content.IsNone()) {
        usageWindow_.Reset();
        return metric;
    }
    const auto sample = usageWindow_.Observe(content.Get(), sampleNs, sampleUnixMs, sysconf(_SC_CLK_TCK));
    if (sample) {
        metric.value = sample->millicores;
        metric.sampleStartUnixMs = sample->startUnixMs;
    }
    return metric;
}

Metric NodeCPUCollector::GetLimit() const
{
    Metric metric;
    if (!procFSTools_ || !std::isfinite(overheadCPU_) || overheadCPU_ < 0) { return metric; }
    const auto online = procFSTools_->Read("/sys/devices/system/cpu/online");
    if (online.IsNone()) { return metric; }
    const auto count = CountOnlineCPUs(online.Get());
    if (!count) { return metric; }
    const double capacity = static_cast<double>(*count) * CPU_CALC_BASE - overheadCPU_;
    if (capacity <= 0) { return metric; }
    metric.value = capacity;
    return metric;
}

std::string NodeCPUCollector::GenFilter() const
{
    // node-cpu
    return litebus::os::Join(collectorType_, metricsType_, '-');
}

}  // namespace functionsystem::runtime_manager