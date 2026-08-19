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

#include "system_memory_collector.h"

#include <cstring>
#include <regex>

#include "common/constants/constants.h"
#include "common/logs/logging.h"

namespace functionsystem::runtime_manager {

SystemMemoryCollector::SystemMemoryCollector(const std::shared_ptr<ProcFSTools> procFSTools)
    : BaseMetricsCollector(metrics_type::MEMORY, collector_type::SYSTEM, procFSTools)
{
}

SystemMemoryCollector::SystemMemoryCollector() : SystemMemoryCollector(std::make_shared<ProcFSTools>())
{
}

Metric SystemMemoryCollector::GetLimit() const
{
    YRLOG_DEBUG_COUNT_60("system memory collector get limit.");
    auto metric = GetMemoryMetrics(system_metrics::MEMORY_LIMIT_PATH);
    if (metric.value.IsNone() || metric.value.Get() <= 0) {
        // cgroup v1 controller absent: MemTotal is the physical node limit
        double totalMb = 0;
        double availableMb = 0;
        if (GetMeminfoMb(totalMb, availableMb)) {
            metric.value = totalMb;
        }
    }
    return metric;
}

litebus::Future<Metric> SystemMemoryCollector::GetUsage() const
{
    YRLOG_DEBUG_COUNT_60("system memory collector get usage.");
    litebus::Promise<Metric> promise;
    auto metric = GetMemoryMetrics(system_metrics::MEMORY_USAGE_PATH);
    if (metric.value.IsNone() || metric.value.Get() <= 0) {
        // cgroup v1 controller absent (unified v2 host): real node usage is
        // MemTotal - MemAvailable; on a dedicated node that is exactly what
        // usage-aware admission must see
        double totalMb = 0;
        double availableMb = 0;
        if (GetMeminfoMb(totalMb, availableMb)) {
            metric.value = totalMb - availableMb;
        }
    }
    promise.SetValue({ metric });
    return promise.GetFuture();
}

bool SystemMemoryCollector::GetMeminfoMb(double &totalMb, double &availableMb) const
{
    if (procFSTools_ == nullptr) {
        return false;
    }
    auto content = procFSTools_->Read(system_metrics::MEMINFO_PATH);
    if (content.IsNone() || content.Get().empty()) {
        YRLOG_DEBUG_COUNT_60("read content from {} failed.", system_metrics::MEMINFO_PATH);
        return false;
    }
    totalMb = 0;
    availableMb = 0;
    // lines look like "MemTotal:       61860 kB" (value always in kB)
    auto body = content.Get();
    try {
        size_t pos = 0;
        while (pos < body.size()) {
            auto end = body.find('\n', pos);
            auto line = body.substr(pos, end == std::string::npos ? end : end - pos);
            pos = (end == std::string::npos) ? body.size() : end + 1;
            if (line.rfind("MemTotal:", 0) == 0) {
                totalMb = std::stod(line.substr(strlen("MemTotal:"))) / 1024.0;
            } else if (line.rfind("MemAvailable:", 0) == 0) {
                availableMb = std::stod(line.substr(strlen("MemAvailable:"))) / 1024.0;
            }
        }
    } catch (const std::exception &e) {
        YRLOG_DEBUG_COUNT_60("parse meminfo fail, error:{}", e.what());
    }
    return totalMb > 0;
}

std::string SystemMemoryCollector::GenFilter() const
{
    // system-memory
    return litebus::os::Join(collectorType_, metricsType_, '-');
}

Metric SystemMemoryCollector::GetMemoryMetrics(const std::string &path) const
{
    if (procFSTools_ == nullptr) {
        YRLOG_ERROR("can not read content, procFSTool is nullptr.");
        return Metric{};
    }

    auto content = procFSTools_->Read(path);
    if (content.IsNone() || content.Get().empty()) {
        YRLOG_ERROR("read content from {} failed.", path);
        return Metric{};
    }
    auto status = content.Get();
    status = litebus::strings::Trim(status);
    double data = 0;
    try {
        data = std::stod(status);
    } catch (const std::exception &e) {
        YRLOG_ERROR("stod fail, error:{}", e.what());
        return Metric{};
    }
    YRLOG_DEBUG_COUNT_60("get status: {}, from {}.", data, path);

    return Metric{ data / static_cast<double>(system_metrics::MEMORY_SCALE), {}, {}, {} };
}
}  // namespace functionsystem::runtime_manager