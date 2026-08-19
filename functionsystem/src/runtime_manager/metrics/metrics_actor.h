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

#ifndef RUNTIME_MANAGER_METRICS_ACTOR_H
#define RUNTIME_MANAGER_METRICS_ACTOR_H

#include <actor/actor.hpp>
#include <async/future.hpp>

#include <set>

#include "collector/base_metrics_collector.h"
#include "collector/heterogeneous_collector/topo_info.h"
#include "common/proto/pb/message_pb.h"
#include "common/status/status.h"
#include "common/utils/proc_fs_tools.h"
#include "runtime_manager/config/flags.h"

namespace functionsystem::runtime_manager {


const int16_t UPDATE_METRICS_DURATION = 5000;
const int16_t DEFAULT_PROC_CPU_METRIC = 1000;
const int16_t DEFAULT_PROC_MEMORY_METRIC = 4000;

struct MetricsConfig {
    std::string metricsCollectorType = "proc";
    std::string heteroLdLibraryPath = "";
    double procMetricsCPU = DEFAULT_PROC_CPU_METRIC;
    double procMetricsMemory = DEFAULT_PROC_MEMORY_METRIC;
    double overheadCPU = 0.0;
    double overheadMemory = 0.0;
};

struct DiskUsageMonitorConfig {
    std::string description;
    int checkDiskUsageLimit = -1;
    std::vector<std::string> checkDiskUsageDirs{};
};

struct RuntimeOomMonitorConfig {
    bool enable{false};
    int memoryDetectionInterval{1000};
    int controlLimit{0};
    int consecutiveDetectionCount{3};
};

using RuntimeMemoryExceedLimitCallbackFunc =
    std::function<void(const std::string &, const std::string &, const std::string &)>;

class MetricsActor : public litebus::ActorBase {
public:
    explicit MetricsActor(const std::string &name);

    ~MetricsActor() override = default;

    /**
     * Inform Metrics actor to add collector to collect metrics
     *
     * @param deployDir the destination dir of function deployment
     * @param instanceID the ID of created instance
     * @param pid process pid mapping to instance ID
     * @param cpuLimit cpu limit
     * @param memLimit memory limit
     * @return
     */
    Status AddInstance(const messages::RuntimeInstanceInfo &instanceInfo, const pid_t &pid,
                       const double &cpuLimit, const double &memLimit);

    /**
     * Inform Metrics actor to delete collector mapping the instance ID
     *
     * @param deployDir the destination dir of function deployment
     * @param instanceID the ID of created instance
     * @return
     */
    Status DeleteInstance(const std::string &deployDir, const std::string &instanceID);

    /**
     * Start update system resources and function resources to function agent.
     */
    void StartUpdateMetrics();

    /**
     * Start update metrics to function agent.
     */
    void BeginUpdateMetrics();

    /**
     * Stop update system resources and function resources to function agent.
     *
     */
    void StopUpdateMetrics();

    /**
     * Get the metrics resources unit from collectors
     *
     * @return
     */
    resources::ResourceUnit GetResourceUnit();

    // Initializes the process-lifetime sandbox runtime capability snapshot.
    // Repeating the same snapshot is idempotent; changing it is rejected.
    Status InitializeSandboxRuntimeCapabilities(const std::set<std::string> &runtimes);

    void UpdateAgentInfo(const litebus::AID &agent);

    void UpdateRuntimeManagerInfo(const litebus::AID &runtimeManagerAID);

    void StartDiskUsageMonitor();

    void StopDiskUsageMonitor();

    void StartRuntimeMemoryLimitMonitor();

    void StopRuntimeMemoryLimitMonitor();

    void SetConfig(const Flags &flags);

    std::vector<int> GetCardIDs();

    void SetRuntimeMemoryExceedLimitCallback(
        const RuntimeMemoryExceedLimitCallbackFunc &runtimeMemoryExceedLimitCallback);

protected:
    std::vector<int> cardIDs_;
    void Init() override;
    void Finalize() override;
    void RetryUpdateRuntimeStatus(const messages::UpdateRuntimeStatusRequest &req, int retryTime);
    void UpdateRuntimeStatusResponse(const litebus::AID &from, std::string &&name, std::string &&msg);
    void ReportInstanceMetrics(const std::vector<litebus::Future<Metrics>> &metricses);
    virtual std::string BuildUpdateMetricsRequest(const std::vector<litebus::Future<Metrics>> &metricses);
    resources::ResourceUnit BuildResourceUnit(const std::vector<litebus::Future<Metrics>> &metricses);
    resources::ResourceUnit BuildResourceUnitWithInstance(const std::vector<litebus::Future<Metrics>> &metricses);
    resources::ResourceUnit BuildResourceUnitWithSystem(const std::vector<litebus::Future<Metrics>> &metricses);
private:
    void AddSystemMetricsCollector(const Flags &flags);
    void AddCpuMemoryAndLabelCollectors(const Flags &flags);
    void AddGpuNumaNpuCollectors(const Flags &flags);
    void AddNodeMetricsCollectors();
    void ResolveCustomResourceMetricsCollector(const std::string &customResource);
    void ResolveDiskResourceMetricsCollector(const std::string &diskResource);
    bool IsDiskUsageBelowLimit(const DiskUsageMonitorConfig &config) const;
    std::vector<litebus::Future<Metrics>> GenAllMetrics() const;
    std::vector<litebus::Future<Metrics>> GenTotalMetrics() const;
    std::vector<litebus::Future<Metrics>> GenAllMetricsWithoutSystem() const;
    void BuildHeteroDevClusterResource(Metrics &metrics, resources::Resource &resource);
    void BuildDiskDevClusterResource(Metrics &metrics, resources::Resource &resource);
    void BuildNUMAResource(Metrics &metrics, resources::Resource &resource);
    void TransitionToVectors(const std::string &key, Metrics &metrics, resources::Resource &resource) const;
    void BuildResource(Metrics &metrics, resources::Resource &resource, const resources::Value_Type &type);
    void RuntimeMemoryMetricsProcess(const std::vector<litebus::Future<Metrics>> &metrics);
    litebus::Future<Status> NotifyInstancesDiskUsageExceedLimit(const DiskUsageMonitorConfig &config) const;
    void SendAgentDiskUsageExceedLimit(const DiskUsageMonitorConfig &config);

    std::unordered_map<std::string, std::shared_ptr<BaseMetricsCollector>> filter_;
    std::unordered_map<std::string, std::shared_ptr<BaseMetricsCollector>> metricsFilter_;
    std::shared_ptr<XPUCollectorParams> npuCollectorParams_{ nullptr };
    std::shared_ptr<BaseMetricsCollector> runtimeMemoryLimitCollector_{ nullptr };
    std::unordered_map<std::string, messages::RuntimeInstanceInfo> instanceInfos_;
    std::shared_ptr<ProcFSTools> procFSTools_{ nullptr };
    litebus::Timer updateMetricsTimer_;
    litebus::Timer pushMetricsTimer_;
    MetricsConfig metricsConfig_;

    litebus::AID agentAid_;
    litebus::AID runtimeManagerAID_;
    litebus::Timer updateRuntimeStatusRetryTimer_;
    litebus::Timer diskUsageMonitorTimer_;
    litebus::Timer runtimeMemoryLimitMonitorTimer_;
    std::string nodeID = "";
    std::string instanceMemorySource_ = "vmrss"; // vmrss | cgroup | auto
    std::string instanceCgroupRoot_ = "/sys/fs/cgroup";
    std::vector<DiskUsageMonitorConfig> diskUsageMonitorConfigs_;
    int checkDiskUsageMonitorDuration_ = -1;
    bool diskUsageMonitorNotifyFailureEnable_{ false };

    // runtime OOM monitor
    RuntimeOomMonitorConfig runtimeOomMonitorConfig_;
    // Store the consecutive anomaly counts for each instanceID
    std::unordered_map<std::string, int> anomalyCounts_;
    RuntimeMemoryExceedLimitCallbackFunc runtimeMemoryExceedLimitCallback_;
    bool sandboxRuntimeCapabilitiesInitialized_{false};
    std::set<std::string> sandboxRuntimeCapabilities_;
};
}  // namespace functionsystem::runtime_manager

#endif  // RUNTIME_MANAGER_METRICS_ACTOR_H
