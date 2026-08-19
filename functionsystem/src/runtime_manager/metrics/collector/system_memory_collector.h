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

#ifndef RUNTIME_MANAGER_METRICS_COLLECTOR_SYSTEM_MEMORY_COLLECTOR_H
#define RUNTIME_MANAGER_METRICS_COLLECTOR_SYSTEM_MEMORY_COLLECTOR_H

#include "base_metrics_collector.h"
namespace functionsystem::runtime_manager {

namespace system_metrics {
const uint32_t MEMORY_SCALE = 1 << 20; // MB
const std::string MEMORY_USAGE_PATH = "/sys/fs/cgroup/memory/memory.usage_in_bytes";
const std::string MEMORY_LIMIT_PATH = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
const std::string MEMINFO_PATH = "/proc/meminfo";
}

class SystemMemoryCollector : public BaseMetricsCollector {
public:
    SystemMemoryCollector();
    explicit SystemMemoryCollector(const std::shared_ptr<ProcFSTools> procFSTools);
    ~SystemMemoryCollector() override = default;
    Metric GetLimit() const override;
    litebus::Future<Metric> GetUsage() const override;
    std::string GenFilter() const override;

private:
     Metric GetMemoryMetrics(const std::string &path) const;

     // cgroup v1 hosts have no memory.usage_in_bytes; fall back to
     // /proc/meminfo (usage = MemTotal - MemAvailable, limit = MemTotal),
     // which on a dedicated node is exactly the real node usage the
     // usage-aware admission needs.
     bool GetMeminfoMb(double &totalMb, double &availableMb) const;
};

}


#endif // RUNTIME_MANAGER_METRICS_COLLECTOR_SYSTEM_MEMORY_COLLECTOR_H