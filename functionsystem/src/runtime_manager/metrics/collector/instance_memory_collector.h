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

#ifndef RUNTIME_MANAGER_METRICS_COLLECTOR_INSTANCE_MEMORY_COLLECTOR_H
#define RUNTIME_MANAGER_METRICS_COLLECTOR_INSTANCE_MEMORY_COLLECTOR_H

#include "base_instance_collector.h"

namespace functionsystem::runtime_manager {

namespace instance_metrics {
const std::string PROCESS_STATUS_PATH_EXPRESS = "/proc/?/status";
const std::string MEMORY_SIZE_KEY = "VmRSS:";
const uint64_t MEMORY_SCALE = 1 << 10; // KB
const std::string CGROUP_PATH_EXPRESS = "/proc/?/cgroup";
const std::string CGROUP_V2_LINE_PREFIX = "0::";
const std::string CGROUP_MEMORY_FILE = "memory.current";
const uint64_t CGROUP_MEMORY_SCALE = 1 << 20; // bytes -> MB
// gVisor sandbox: sentry RSS != sandbox memory; cgroup memory.current is the
// authoritative per-sandbox accounting (aligned with the external sampler).
const std::string MEMORY_SOURCE_VMRSS = "vmrss";
const std::string MEMORY_SOURCE_CGROUP = "cgroup";
const std::string MEMORY_SOURCE_AUTO = "auto";
const std::string DEFAULT_CGROUP_ROOT = "/sys/fs/cgroup";
}

class InstanceMemoryCollector : public BaseInstanceCollector, public BaseMetricsCollector {
public:
    InstanceMemoryCollector(const pid_t &pid, const std::string &instanceID, const double &limit,
                            const std::string &deployDir);
    InstanceMemoryCollector(const pid_t &pid, const std::string &instanceID, const double &limit,
                            const std::string &deployDir, const std::shared_ptr<ProcFSTools> procFSTools);
    InstanceMemoryCollector(const pid_t &pid, const std::string &instanceID, const double &limit,
                            const std::string &deployDir, const std::shared_ptr<ProcFSTools> procFSTools,
                            const std::string &memorySource, const std::string &cgroupRoot);
    ~InstanceMemoryCollector() override = default;
    Metric GetLimit() const override;
    virtual litebus::Future<Metric> GetUsage() const override;
    std::string GenFilter() const override;

private:
    // cgroup v2 memory.current of the sandbox (MB). None = resolve/read failed
    // (no "0::" line, cgroup v1 host, or unreadable file).
    litebus::Option<double> GetUsageFromCgroup() const;

    std::string memorySource_ = instance_metrics::MEMORY_SOURCE_VMRSS;
    std::string cgroupRoot_ = instance_metrics::DEFAULT_CGROUP_ROOT;
};

}

#endif // RUNTIME_MANAGER_METRICS_COLLECTOR_INSTANCE_MEMORY_COLLECTOR_H