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

#ifndef RUNTIME_MANAGER_METRICS_COLLECTOR_SYSTEM_PROC_MEMORY_COLLECTOR_H
#define RUNTIME_MANAGER_METRICS_COLLECTOR_SYSTEM_PROC_MEMORY_COLLECTOR_H

#include <string>

#include "base_system_proc_collector.h"

namespace functionsystem::runtime_manager {

class SystemProcMemoryCollector : public BaseSystemProcCollector, public BaseMetricsCollector {
public:
    SystemProcMemoryCollector(const double &limit, const CallBackFunc &callback,
                              const std::string &sandboxCgroupDir = "");
    ~SystemProcMemoryCollector() override = default;
    std::string GenFilter() const override;
    litebus::Future<Metric> GetUsage() const override;
    Metric GetLimit() const override;

private:
    // W2 步2 cgroup 口径：runsc 沙箱 cgroup 挂在 <cgroupRoot>/akernel/<id>/，
    // 与 runtime 进程 cgroup（system.slice/yuanrong.service）是兄弟关系，
    // 求和 instance 指标读不到沙箱真实占用。Σ 沙箱 memory.current 才是
    // "已准入负载的真实节点内存"。目录不存在/列举失败返回 -1（回退旧口径）。
    double SumSandboxCgroupUsageMb() const;

    std::string sandboxCgroupDir_;
};

}

#endif // RUNTIME_MANAGER_METRICS_COLLECTOR_SYSTEM_PROC_MEMORY_COLLECTOR_H
