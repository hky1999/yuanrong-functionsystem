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

#include "system_proc_memory_collector.h"

#include <filesystem>

#include "common/logs/logging.h"

namespace functionsystem::runtime_manager {

SystemProcMemoryCollector::SystemProcMemoryCollector(const double &limit, const CallBackFunc &callback,
                                                     const std::string &sandboxCgroupDir)
    : BaseSystemProcCollector(limit, callback),
      BaseMetricsCollector(metrics_type::MEMORY, collector_type::SYSTEM, nullptr),
      sandboxCgroupDir_(sandboxCgroupDir)
{
}

std::string SystemProcMemoryCollector::GenFilter() const
{
    return litebus::os::Join(collectorType_, metricsType_, '-');
}

double SystemProcMemoryCollector::SumSandboxCgroupUsageMb() const
{
    namespace fs = std::filesystem;
    std::error_code ec;
    if (!fs::is_directory(sandboxCgroupDir_, ec)) {
        return -1.0;
    }
    ProcFSTools tools;
    double totalMb = 0;
    auto it = fs::directory_iterator(sandboxCgroupDir_, ec);
    for (; !ec && it != fs::directory_iterator(); it.increment(ec)) {
        if (!it->is_directory(ec) || ec) {
            continue;
        }
        auto content = tools.Read((it->path() / "memory.current").string());
        if (content.IsNone() || content.Get().empty()) {
            continue;
        }
        try {
            auto raw = content.Get();
            totalMb += std::stod(litebus::strings::Trim(raw)) / static_cast<double>(1 << 20);
        } catch (const std::exception &e) {
            YRLOG_DEBUG_COUNT_60("parse sandbox memory.current fail, error:{}", e.what());
        }
    }
    if (ec) {
        return -1.0;
    }
    return totalMb;
}

litebus::Future<Metric> SystemProcMemoryCollector::GetUsage() const
{
    // W2 步2：优先用沙箱 cgroup 求和口径；目录缺失（非 runsc 部署/布局变化）
    // 时回退旧的 instance 指标求和，保持行为兼容。
    if (!sandboxCgroupDir_.empty()) {
        auto sandboxUsage = SumSandboxCgroupUsageMb();
        if (sandboxUsage >= 0) {
            return { Metric{ { sandboxUsage }, {}, {}, {} } };
        }
        YRLOG_DEBUG_COUNT_60("sandbox cgroup dir {} unusable, fall back to instance sum.", sandboxCgroupDir_);
    }
    auto metricses = getInstanceMetricsesCallBack_();
    double usage = 0;
    for (auto &futureMetrics : metricses) {
        auto metrics = futureMetrics.Get();
        if (metrics.instanceID.IsNone()) {
            continue;
        }

        if (!(metrics.metricsType == metrics_type::MEMORY)) {
            continue;
        }

        if (metrics.usage.IsSome()) {
            usage += metrics.usage.Get();
        }
    }
    return { Metric{ { usage }, {}, {}, {} } };
}

Metric SystemProcMemoryCollector::GetLimit() const
{
    Metric metric;
    metric.value = limit_;
    return metric;
}

}  // namespace functionsystem::runtime_manager