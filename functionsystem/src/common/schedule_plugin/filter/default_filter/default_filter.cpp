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

#include "default_filter.h"

#include <limits>

#include "common/constants/constants.h"
#include "common/logs/logging.h"
#include "common/resource_view/resource_tool.h"
#include "common/resource_view/scala_resource_tool.h"
#include "common/schedule_plugin/common/constants.h"
#include "common/schedule_plugin/common/plugin_register.h"
#include "utils/string_utils.hpp"

namespace functionsystem::schedule_plugin::filter {

const int32_t MAX_INT_32 = std::numeric_limits<int32_t>::max();

const std::unordered_map<std::string, std::string> RESOURCE_UNIT = { { "CPU", "m" }, { "Memory", "MB" } };
const std::string STORAGE_RESOURCE_NAME = "storage";
constexpr double BYTES_PER_MIB = 1024.0 * 1024.0;

// Format only the requirement attached to scheduling failure messages. Resource values remain in their internal
// scheduling units for capacity and allocatable comparisons.
std::string FormatResourceRequirementForError(const std::string &name, const resource_view::Resource &resource)
{
    auto value = resource.scalar().value();
    if (name == STORAGE_RESOURCE_NAME) {
        value /= BYTES_PER_MIB;
    }

    auto requestResource = name + ": " + std::to_string(static_cast<int64_t>(value));
    if (name == STORAGE_RESOURCE_NAME) {
        return requestResource + "MiB";
    }
    if (auto iter = RESOURCE_UNIT.find(name); iter != RESOURCE_UNIT.end()) {
        requestResource += iter->second;
    }
    return requestResource;
}

std::string FormatMonopolyRequirementForError(double cpu, double memory)
{
    return "(" + std::to_string(static_cast<int64_t>(cpu)) + ", " +
           std::to_string(static_cast<int64_t>(memory)) + ")";
}

std::string DefaultFilter::GetPluginName()
{
    return DEFAULT_FILTER_NAME;
}

Status DefaultFilter::MonopolyFilter(const std::shared_ptr<schedule_framework::PreAllocatedContext> &preContext,
                                     const resource_view::InstanceInfo &instance,
                                     const resource_view::ResourceUnit &unit)
{
    // if pod is selected, can not be used
    const auto &instanceResources = instance.resources().resources();
    double instanceMem = instanceResources.at(resource_view::MEMORY_RESOURCE_NAME).scalar().value();
    double instanceCpu = instanceResources.at(resource_view::CPU_RESOURCE_NAME).scalar().value();
    const auto &selectedSet = preContext->preAllocatedSelectedFunctionAgentSet;

    if (selectedSet.find(unit.id()) != selectedSet.end()) {
        return Status(RESOURCE_NOT_ENOUGH,
                      FormatMonopolyRequirementForError(instanceCpu, instanceMem) + " Already Allocated To Other");
    }

    const auto &fragmentResources = unit.allocatable().resources();
    double fragmentMem = fragmentResources.at(resource_view::MEMORY_RESOURCE_NAME).scalar().value();
    double fragmentCpu = fragmentResources.at(resource_view::CPU_RESOURCE_NAME).scalar().value();
    // monopoly need to be match precisely
    if (abs(instanceMem - fragmentMem) > EPSINON || abs(instanceCpu - fragmentCpu) > EPSINON) {
        return Status(RESOURCE_NOT_ENOUGH,
                      FormatMonopolyRequirementForError(instanceCpu, instanceMem) + " Don't Match Precisely");
    }

    if (abs(instanceCpu) < EPSINON) {
        return Status(INVALID_RESOURCE_PARAMETER, "Invalid CPU: " + std::to_string(instanceCpu));
    }
    auto bucketIndexesIter(unit.bucketindexs().find(std::to_string(instanceMem / instanceCpu)));
    if (bucketIndexesIter == unit.bucketindexs().end()) {
        return Status(RESOURCE_NOT_ENOUGH,
                      FormatMonopolyRequirementForError(instanceCpu, instanceMem) + " Not Found");
    }
    auto bucketsIter(bucketIndexesIter->second.buckets().find(std::to_string(fragmentMem)));
    if (bucketsIter == bucketIndexesIter->second.buckets().end()) {
        return Status(RESOURCE_NOT_ENOUGH,
                      FormatMonopolyRequirementForError(instanceCpu, instanceMem) + " Not Found");
    }

    // if mono num is 0 in node, the set of feasible node is empty
    if (bucketsIter->second.total().monopolynum() == 0) {
        return Status(RESOURCE_NOT_ENOUGH,
                      FormatMonopolyRequirementForError(instanceCpu, instanceMem) + " Not Enough");
    }
    return Status::OK();
}

schedule_framework::Filtered DefaultFilter::ResourceFilter(
    const std::shared_ptr<schedule_framework::PreAllocatedContext> &preContext,
    const resource_view::InstanceInfo &instance, const resource_view::ResourceUnit &unit)
{
    auto available = unit.allocatable();
    // calculate new available
    if (auto iter(preContext->allocated.find(unit.id())); iter != preContext->allocated.end()) {
        available = unit.allocatable() - iter->second.resource;
        if (!resource_view::IsValid(available)) {
            YRLOG_WARN("Invalid available resource, unit {}", unit.id());
            return schedule_framework::Filtered{ Status{ StatusCode::RESOURCE_NOT_ENOUGH, "No Resources Available" },
                                                 false, -1 };
        }
    }
    const auto &required = instance.resources().resources();
    const auto &capacity = unit.capacity();
    int32_t maxAllocatable = MAX_INT_32;
    for (auto &req : required) {
        // hetero resource filter in hetero filter, disk resource filter in disk filter
        if (resource_view::IsHeterogeneousResource(req.first) || resource_view::IsDiskResource(req.first)) {
            continue;
        }

        // required number is zero
        if (resource_view::ScalaValueIsEmpty(req.second)) {
            YRLOG_DEBUG("{}|{} req({}) in unit({}) is 0 for schedule.", instance.requestid(), req.first,
                        req.second.scalar().value(), unit.id());
            continue;
        }
        auto requestResource = FormatResourceRequirementForError(req.first, req.second);
        // Find the same type of resource as the request, like CPU and MEM.
        auto cap = capacity.resources().find(req.first);
        if (cap == capacity.resources().end()) {
            return schedule_framework::Filtered{ Status(PARAMETER_ERROR, req.first + ": Not Found"), false, -1,
                                                 std::move(requestResource) };
        }

        if (req.second.scalar().value() > cap->second.scalar().value()) {
            return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, req.first + ": Out Of Capacity"), false,
                                                 -1, std::move(requestResource) };
        }

        auto avail = available.resources().find(req.first);
        if (avail == available.resources().end()) {
            return schedule_framework::Filtered{ Status(PARAMETER_ERROR, req.first + ": Not Found"), false, -1,
                                                 std::move(requestResource) };
        }
        // available resources not meet requirements.
        if (!(req.second <= avail->second)) {
            return schedule_framework::Filtered{ Status(RESOURCE_NOT_ENOUGH, req.first + ": Not Enough"), false, -1,
                                                 std::move(requestResource) };
        }
        auto availValue = avail->second.scalar().value();
        auto requireValue = req.second.scalar().value();
        int32_t canAllocatableNum = int32_t(availValue / requireValue);
        maxAllocatable = std::min(maxAllocatable, canAllocatableNum);
    }
    if (maxAllocatable == MAX_INT_32 || maxAllocatable <= 0) {
        YRLOG_WARN("failed to calculate maxAllocatable Num");
        maxAllocatable = 1;
    }
    return schedule_framework::Filtered{ Status::OK(), false, maxAllocatable };
}

schedule_framework::Filtered DefaultFilter::Filter(const std::shared_ptr<schedule_framework::ScheduleContext> &ctx,
                                                   const resource_view::InstanceInfo &instance,
                                                   const resource_view::ResourceUnit &resourceUnit)
{
    const auto preContext = std::dynamic_pointer_cast<schedule_framework::PreAllocatedContext>(ctx);
    // note that if it is a monopoly instance, we don't verify other resources except CPU and MEM
    if (instance.scheduleoption().schedpolicyname() == MONOPOLY_MODE) {
        auto status = MonopolyFilter(preContext, instance, resourceUnit);
        if (status.IsError()) {
            return schedule_framework::Filtered{ status, false, -1 };
        }
        return schedule_framework::Filtered{ status, false, 1 };
    }
    return ResourceFilter(preContext, instance, resourceUnit);
}

std::shared_ptr<schedule_framework::SchedulePolicyPlugin> DefaultFilterCreator()
{
    return std::make_shared<DefaultFilter>();
}

REGISTER_SCHEDULER_PLUGIN(DEFAULT_FILTER_NAME, DefaultFilterCreator);

}  // namespace functionsystem::schedule_plugin::filter
