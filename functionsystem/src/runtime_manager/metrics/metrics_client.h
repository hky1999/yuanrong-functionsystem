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

#ifndef RUNTIME_MANAGER_METRICS_METRICS_CLIENT_H
#define RUNTIME_MANAGER_METRICS_METRICS_CLIENT_H

#include <set>

#include "async/future.hpp"
#include "common/proto/pb/message_pb.h"
#include "runtime_manager/config/flags.h"
#include "metrics_actor.h"

namespace functionsystem::runtime_manager {

class MetricsClient {
public:
    explicit MetricsClient();

    ~MetricsClient();

    void CreateInstanceMetrics(const litebus::Future<::messages::StartInstanceResponse> &response,
                               const std::shared_ptr<messages::StartInstanceRequest> &request);

    void DeleteInstanceMetrics(const std::string &deployDir,
                               const std::string &instanceID) const;

    // D-7: feed per-instance sandbox memory (MB) into the reported resource
    // view (runsc reports no pid, so the pid-based collector cannot)
    void UpdateInstanceMemoryUsage(const std::string &instanceID, double memoryMb) const;

    resources::ResourceUnit GetResourceUnit() const;

    litebus::Future<Status> InitializeSandboxRuntimeCapabilities(const std::set<std::string> &runtimes) const;

    void StartUpdateResource() const;

    void BeginUpdateMetrics() const;

    void StopUpdateResource() const;

    void UpdateAgentInfo(const litebus::AID &agent) const;

    void UpdateRuntimeManagerInfo(const litebus::AID &agent) const;

    void StartDiskUsageMonitor() const;

    void StopDiskUsageMonitor() const;

    void StartRuntimeMemoryLimitMonitor() const;

    void StopRuntimeMemoryLimitMonitor() const;

    void SetConfig(const Flags &flag) const;

    std::vector<int> GetCardIDs();

    void SetRuntimeMemoryExceedLimitCallback(
        const RuntimeMemoryExceedLimitCallbackFunc &runtimeMemoryExceedLimitCallback);

private:
    std::shared_ptr<MetricsActor> actor_;
    std::vector<int> cardIDs_{}; // cached
    bool firstGetCardID_ = true;
};
}  // namespace functionsystem::runtime_manager

#endif  // RUNTIME_MANAGER_METRICS_METRICS_CLIENT_H
