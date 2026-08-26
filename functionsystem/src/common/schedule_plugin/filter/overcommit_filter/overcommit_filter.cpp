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

#include "overcommit_filter.h"

#include "common/logs/logging.h"
#include "common/resource_view/resource_tool.h"
#include "common/schedule_plugin/common/constants.h"
#include "common/schedule_plugin/common/plugin_register.h"

namespace functionsystem::schedule_plugin::filter {

std::mutex OvercommitFilter::configMutex_;
double OvercommitFilter::ratio_ = 1.0;

namespace {

// effective ceiling demand of a scalar resource: the limit when it exceeds
// the value (best-effort burst headroom), otherwise the booked value itself
double EffectiveCeiling(const resource_view::Resource &res)
{
    if (res.type() != resource_view::ValueType::Value_Type_SCALAR || !res.has_scalar()) {
        return 0.0;
    }
    const double value = res.scalar().value();
    const double limit = res.scalar().limit();
    return (limit > value && limit > 0) ? limit : value;
}

}  // namespace

void OvercommitFilter::SetConfig(double ratio)
{
    std::lock_guard<std::mutex> lock(configMutex_);
    ratio_ = ratio;
    YRLOG_INFO("overcommit filter config: ratio={}", ratio);
}

double OvercommitFilter::GetRatio()
{
    std::lock_guard<std::mutex> lock(configMutex_);
    return ratio_;
}

std::string OvercommitFilter::GetPluginName()
{
    return OVERCOMMIT_FILTER_NAME;
}

schedule_framework::Filtered OvercommitFilter::Filter(const std::shared_ptr<schedule_framework::ScheduleContext> &ctx,
                                                      const resource_view::InstanceInfo &instance,
                                                      const resource_view::ResourceUnit &resourceUnit)
{
    const auto preContext = std::dynamic_pointer_cast<schedule_framework::PreAllocatedContext>(ctx);
    const auto &required = instance.resources().resources();
    for (auto &req : required) {
        if (resource_view::IsHeterogeneousResource(req.first) || resource_view::IsDiskResource(req.first)) {
            continue;
        }
        // guaranteed request (no burst headroom): booked check in DefaultFilter is authoritative
        if (req.second.type() != resource_view::ValueType::Value_Type_SCALAR || !req.second.has_scalar()
            || !(req.second.scalar().limit() > req.second.scalar().value()) || req.second.scalar().limit() <= 0) {
            continue;
        }

        auto cap = resourceUnit.capacity().resources().find(req.first);
        if (cap == resourceUnit.capacity().resources().end() || cap->second.scalar().value() <= 0) {
            continue;  // unknown capacity: leave the verdict to DefaultFilter
        }
        double ceiling = cap->second.scalar().value() * GetRatio();

        double used = 0.0;
        for (auto &instIter : resourceUnit.instances()) {
            auto instRes = instIter.second.resources().resources().find(req.first);
            if (instRes != instIter.second.resources().resources().end()) {
                used += EffectiveCeiling(instRes->second);
            }
        }
        // pending pre-allocations on this unit only track summed booked values
        // (limit headroom is not accumulated there) — count them at their
        // booked value, a lower bound that keeps the window conservative-small
        if (preContext != nullptr) {
            if (auto iter = preContext->allocated.find(resourceUnit.id()); iter != preContext->allocated.end()) {
                auto pendRes = iter->second.resource.resources().find(req.first);
                if (pendRes != iter->second.resource.resources().end()) {
                    used += pendRes->second.scalar().value();
                }
            }
        }

        double requestCeiling = EffectiveCeiling(req.second);
        if (used + requestCeiling > ceiling) {
            YRLOG_INFO("{}|overcommit filter rejects unit {}: ceiling used {} + request {} > capacity {} * ratio {}",
                       instance.requestid(), resourceUnit.id(), used, requestCeiling, cap->second.scalar().value(),
                       GetRatio());
            return schedule_framework::Filtered{
                Status(RESOURCE_NOT_ENOUGH, req.first + ": Limit Sum Exceeds Overcommit Ceiling"), false, -1
            };
        }
        YRLOG_DEBUG("{}|overcommit filter passes unit {}: ceiling used {} + request {} <= {}", instance.requestid(),
                    resourceUnit.id(), used, requestCeiling, ceiling);
    }
    return schedule_framework::Filtered{ Status::OK(), false, -1 };
}

std::shared_ptr<schedule_framework::SchedulePolicyPlugin> OvercommitFilterCreator()
{
    return std::make_shared<OvercommitFilter>();
}

REGISTER_SCHEDULER_PLUGIN(OVERCOMMIT_FILTER_NAME, OvercommitFilterCreator);

}  // namespace functionsystem::schedule_plugin::filter
