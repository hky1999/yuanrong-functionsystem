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

#ifndef RUNTIME_MANAGER_METRICS_COLLECTOR_NODE_CPU_COLLECTOR_H
#define RUNTIME_MANAGER_METRICS_COLLECTOR_NODE_CPU_COLLECTOR_H

#include "base_metrics_collector.h"
#include "node_cpu_usage_window.h"
#include <mutex>

namespace functionsystem::runtime_manager {

class NodeCPUCollector : public BaseMetricsCollector {
public:
    NodeCPUCollector();
    explicit NodeCPUCollector(const std::shared_ptr<ProcFSTools> procFSTools, double overheadCPU = 0.0);
    ~NodeCPUCollector() override = default;
    litebus::Future<Metric> GetUsage() const override;
    Metric GetLimit() const override;
    std::string GenFilter() const override;
private:
    double overheadCPU_;
    mutable std::mutex usageMutex_;
    mutable NodeCPUUsageWindow usageWindow_;
};

}

#endif // RUNTIME_MANAGER_METRICS_COLLECTOR_NODE_CPU_COLLECTOR_H