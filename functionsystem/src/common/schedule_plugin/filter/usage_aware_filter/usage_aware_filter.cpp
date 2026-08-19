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

#include "usage_aware_filter.h"

#include <algorithm>

#include "common/logs/logging.h"
#include "common/resource_view/resource_tool.h"
#include "common/schedule_plugin/common/constants.h"
#include "common/schedule_plugin/common/plugin_register.h"

namespace functionsystem::schedule_plugin::filter {

std::mutex UsageAwareFilter::configMutex_;
double UsageAwareFilter::safety_ = 0.9;
double UsageAwareFilter::floorMb_ = 2048.0;

void UsageAwareFilter::SetConfig(double safety, double floorMb)
{
    std::lock_guard<std::mutex> lock(configMutex_);
    safety_ = safety;
    floorMb_ = floorMb;
    YRLOG_INFO("usage aware filter config: safety={}, floorMb={}", safety, floorMb);
}

double UsageAwareFilter::GetSafety()
{
    std::lock_guard<std::mutex> lock(configMutex_);
    return safety_;
}

double UsageAwareFilter::GetFloorMb()
{
    std::lock_guard<std::mutex> lock(configMutex_);
    return floorMb_;
}

std::string UsageAwareFilter::GetPluginName()
{
    return USAGE_AWARE_FILTER_NAME;
}

schedule_framework::Filtered UsageAwareFilter::Filter(const std::shared_ptr<schedule_framework::ScheduleContext> &ctx,
                                                      const resource_view::InstanceInfo &instance,
                                                      const resource_view::ResourceUnit &resourceUnit)
{
    (void)ctx;
    const auto &actualResources = resourceUnit.actualuse().resources();
    auto actualMem = actualResources.find(resource_view::MEMORY_RESOURCE_NAME);
    auto capMem = resourceUnit.capacity().resources().find(resource_view::MEMORY_RESOURCE_NAME);
    // no real-usage signal (e.g. metrics not propagated yet): stay neutral and
    // let DefaultFilter decide on the booked allocatable (guaranteed fallback)
    if (actualMem == actualResources.end() || actualMem->second.scalar().value() <= 0
        || capMem == resourceUnit.capacity().resources().end() || capMem->second.scalar().value() <= 0) {
        YRLOG_DEBUG("{}|usage aware filter has no actual use signal on unit {}, fallback to booked check",
                    instance.requestid(), resourceUnit.id());
        return schedule_framework::Filtered{ Status::OK(), false, -1 };
    }

    double used = actualMem->second.scalar().value();
    double total = capMem->second.scalar().value();
    double safety = GetSafety();
    // reserve a slice for the incoming instance, capped by its own request:
    // a small instance must not be blocked by a fixed large floor
    double reserve = GetFloorMb();
    auto reqMem = instance.resources().resources().find(resource_view::MEMORY_RESOURCE_NAME);
    if (reqMem != instance.resources().resources().end() && reqMem->second.scalar().value() > 0) {
        reserve = std::min(reserve, reqMem->second.scalar().value());
    }

    double allowed = total * safety;
    if (used + reserve > allowed) {
        YRLOG_INFO("{}|usage aware filter rejects unit {}: used {} + reserve {} > capacity {} * safety {}",
                   instance.requestid(), resourceUnit.id(), used, reserve, total, safety);
        return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, "Memory: Actual Use Exceeds Safety Line"),
                                             false, -1 };
    }
    YRLOG_DEBUG("{}|usage aware filter passes unit {}: used {} + reserve {} <= allowed {}", instance.requestid(),
                resourceUnit.id(), used, reserve, allowed);
    return schedule_framework::Filtered{ Status::OK(), false, -1 };
}

std::shared_ptr<schedule_framework::SchedulePolicyPlugin> UsageAwareFilterCreator()
{
    return std::make_shared<UsageAwareFilter>();
}

REGISTER_SCHEDULER_PLUGIN(USAGE_AWARE_FILTER_NAME, UsageAwareFilterCreator);

}  // namespace functionsystem::schedule_plugin::filter
