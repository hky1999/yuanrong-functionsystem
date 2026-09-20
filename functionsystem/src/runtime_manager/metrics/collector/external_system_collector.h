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

#ifndef RUNTIME_MANAGER_METRICS_COLLECTOR_EXTERNAL_SYSTEM_COLLECTOR_H
#define RUNTIME_MANAGER_METRICS_COLLECTOR_EXTERNAL_SYSTEM_COLLECTOR_H

#include <string>

#include "async/async.hpp"
#include "utils/os_utils.hpp"

#include "base_metrics_collector.h"

namespace functionsystem::runtime_manager {
// A valid inventory without the requested type returns an empty Metric;
// malformed input or a malformed requested entry returns None.
litebus::Option<Metric> ParseExternalXpuMetric(const std::string &response, const std::string &expectedType);
litebus::Option<Metric> ParseExternalGpuMetric(const std::string &response);
litebus::Option<Metric> ParseExternalStorageMetric(const std::string &response);

class CurlHelper : public litebus::ActorBase {
public:
    static std::shared_ptr<CurlHelper> NewCurlHelper()
    {
        std::cout << "Creating CurlHelper actor" << std::endl;
        std::string endpoint = "/var/run/resource.sock";
        if (auto ep = litebus::os::GetEnv("EXTERNAL_RESOURCE_EP"); ep.IsSome()) {
            endpoint = ep.Get();
        }
        std::string url = "http://localhost/resource";
        if (auto url = litebus::os::GetEnv("EXTERNAL_RESOURCE_URL"); url.IsSome()) {
            url = url.Get();
        }
        std::string actorName = "CurlHelper-" + litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        auto helper = std::make_shared<CurlHelper>(actorName, endpoint, url);
        litebus::Spawn(helper);
        std::cout << "Created CurlHelper actor" << std::endl;
        return helper;
    }
    CurlHelper() = default;
    CurlHelper(const std::string &name, const std::string &externEndpoint, const std::string &url);
    ~CurlHelper() override;
    litebus::Future<std::string> Query();

private:
    std::string externEndpoint_;
    std::string url_;
    void *curl_{ nullptr };
};

class ExternalSystemCollector : public BaseMetricsCollector {
public:
    ExternalSystemCollector() = default;
    explicit ExternalSystemCollector(const double &limit, const MetricsType &metricsType,
                                     const litebus::ActorReference &curlActorRef)
        : BaseMetricsCollector(metricsType, collector_type::SYSTEM), limit_(limit), curlActorRef_(curlActorRef) {};
    ~ExternalSystemCollector() override = default;

    litebus::Future<Metric> GetUsage() const override
    {
        return {};
    }

    Metric GetLimit() const override
    {
        Metric metric;
        metric.value = limit_;
        return metric;
    }
    std::string GenFilter() const override
    {
        return litebus::os::Join(collectorType_, metricsType_, '-');
    }

protected:
    litebus::Future<std::string> CollectFromExternal() const;

private:
    double limit_{0.0};
    litebus::ActorReference curlActorRef_;
};

class ExternalSystemCPUCollector : public ExternalSystemCollector {
public:
    ExternalSystemCPUCollector() = default;
    explicit ExternalSystemCPUCollector(const double &limit, const litebus::ActorReference &curlActorRef)
        : ExternalSystemCollector(limit, metrics_type::CPU, curlActorRef), previous_(std::make_shared<Metric>()) {};
    ~ExternalSystemCPUCollector() override = default;
    litebus::Future<Metric> GetUsage() const override;
    Metric GetLimit() const override;
private:
    std::shared_ptr<Metric> previous_;
    // Total reported by the node-resource endpoint when it provides an
    // explicit used/total pair; empty until the first such sample.
    mutable std::shared_ptr<Metric> total_;
};

class ExternalSystemMemoryCollector : public ExternalSystemCollector {
public:
    ExternalSystemMemoryCollector() = default;
    explicit ExternalSystemMemoryCollector(const double &limit, const litebus::ActorReference &curlActorRef)
        : ExternalSystemCollector(limit, metrics_type::MEMORY, curlActorRef), previous_(std::make_shared<Metric>()) {};
    ~ExternalSystemMemoryCollector() override = default;
    litebus::Future<Metric> GetUsage() const override;
    Metric GetLimit() const override;
private:
    std::shared_ptr<Metric> previous_;
    mutable std::shared_ptr<Metric> total_;
};

class ExternalSystemXPUCollector : public ExternalSystemCollector {
public:
    ExternalSystemXPUCollector() = default;
    ExternalSystemXPUCollector(const MetricsType &metricsType, const std::string &externalType,
                               const litebus::ActorReference &curlActorRef);
    ~ExternalSystemXPUCollector() override = default;

    litebus::Future<Metric> GetUsage() const override;
    Metric GetLimit() const override;

private:
    std::string externalType_;
    std::string uuid_;
    std::shared_ptr<Metric> previous_;
};

class ExternalSystemStorageCollector : public ExternalSystemCollector {
public:
    ExternalSystemStorageCollector() = default;
    explicit ExternalSystemStorageCollector(const litebus::ActorReference &curlActorRef);
    ~ExternalSystemStorageCollector() override = default;

    litebus::Future<Metric> GetUsage() const override;
    Metric GetLimit() const override;

private:
    std::shared_ptr<Metric> previous_;
};
}  // namespace functionsystem::runtime_manager

#endif  // RUNTIME_MANAGER_METRICS_COLLECTOR_EXTERNAL_SYSTEM_COLLECTOR_H
