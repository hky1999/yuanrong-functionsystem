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

#include "default_prefilter.h"

#include "common/schedule_plugin/common/plugin_register.h"

namespace functionsystem::schedule_plugin::prefilter {

std::string FormatMonopolyRequirementForError(double cpu, double memory)
{
    return "(" + std::to_string(static_cast<int64_t>(cpu)) + ", " +
           std::to_string(static_cast<int64_t>(memory)) + ")";
}

Status CheckParam(const std::shared_ptr<schedule_framework::PreAllocatedContext> &ctx,
                  const resource_view::InstanceInfo &instance)
{
    if (ctx == nullptr) {
        YRLOG_WARN("{}|(schedule)invalid context, ctx is nullptr", instance.requestid());
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "Invalid Schedule Context");
    }

    if (!resource_view::IsValid(instance.resources())) {
        YRLOG_WARN("{}|(schedule)invalid resource value", instance.requestid());
        return Status(StatusCode::INVALID_RESOURCE_PARAMETER, "Invalid Instance Resource Value");
    }
    return Status(StatusCode::SUCCESS);
}

std::shared_ptr<schedule_framework::PreFilterResult> DefaultPreFilter::PreFilter(
    const std::shared_ptr<schedule_framework::ScheduleContext> &ctx, const resource_view::InstanceInfo &instance,
    const resource_view::ResourceUnit &resourceUnit)
{
    const auto preContext = std::dynamic_pointer_cast<schedule_framework::PreAllocatedContext>(ctx);
    if (auto status(CheckParam(preContext, instance)); status != StatusCode::SUCCESS) {
        return std::make_shared<schedule_framework::ProtoMapPreFilterResult<resource_view::ResourceUnit>>(
            resourceUnit.fragment(), status);
    }

    // calculate proportion
    auto &instanceResources = instance.resources().resources();
    InstanceInfo inst;
    inst.instanceID = instance.instanceid();
    inst.requestID = instance.requestid();
    inst.policy = instance.scheduleoption().schedpolicyname();
    inst.memVal = instanceResources.at(resource_view::MEMORY_RESOURCE_NAME).scalar().value();
    inst.cpuVal = instanceResources.at(resource_view::CPU_RESOURCE_NAME).scalar().value();

    YRLOG_DEBUG("(schedule)request({}) of instance({}), mem: {}, cpu: {}", inst.requestID, inst.instanceID, inst.memVal,
                inst.cpuVal);

    if (inst.policy == MONOPOLY_MODE) {
        // find proportion from fragment index. if proportion exist and the memory meets the requirement,
        // the bucket is directly selected.
        return PrecisePreFilter(preContext, resourceUnit, inst);
    }
    return CommonPreFilter(preContext, resourceUnit, inst);
}

std::shared_ptr<schedule_framework::PreFilterResult> DefaultPreFilter::PrecisePreFilter(
    const std::shared_ptr<schedule_framework::PreAllocatedContext> &ctx,
    const resource_view::ResourceUnit &resourceUnit, const InstanceInfo &inst) const
{
    YRLOG_DEBUG("{}|(schedule)use precise preFilter", inst.requestID);
    if (resourceUnit.bucketindexs_size() == 0) {
        YRLOG_WARN("(schedule)bucket indexes is empty");
        return std::make_shared<schedule_framework::ProtoMapPreFilterResult<resource_view::ResourceUnit>>(
            resourceUnit.fragment(), Status{ StatusCode::RESOURCE_NOT_ENOUGH, "No Resource In Cluster" });
    }
    const auto &bucketIndexes(resourceUnit.bucketindexs());

    if (abs(inst.cpuVal) < EPSINON) {
        std::string errMsg = "Invalid CPU: " + std::to_string(inst.cpuVal);
        return std::make_shared<schedule_framework::ProtoMapPreFilterResult<resource_view::ResourceUnit>>(
            resourceUnit.fragment(), Status{ StatusCode::INVALID_RESOURCE_PARAMETER, errMsg });
    }
    auto proportionStr = std::to_string(inst.memVal / inst.cpuVal);
    auto bucketIndexesIter(bucketIndexes.find(proportionStr));
    if (bucketIndexesIter == bucketIndexes.end()) {
        YRLOG_WARN("{}|(schedule)the proportion({}) of instance({}) isn't found", inst.requestID, proportionStr,
                   inst.instanceID);
        std::string errMsg = FormatMonopolyRequirementForError(inst.cpuVal, inst.memVal) + " Not Found";
        return std::make_shared<schedule_framework::ProtoMapPreFilterResult<resource_view::ResourceUnit>>(
            resourceUnit.fragment(), Status{ StatusCode::RESOURCE_NOT_ENOUGH, errMsg });
    }
    const auto &buckets(bucketIndexesIter->second.buckets());
    auto memIndex = std::to_string(inst.memVal);
    auto bucketsIter(buckets.find(memIndex));
    if (bucketsIter == buckets.end()) {
        YRLOG_WARN("{}|(schedule)the mem({}) of instance({}) isn't found", inst.requestID, std::to_string(inst.memVal),
                   inst.instanceID);
        std::string errMsg = FormatMonopolyRequirementForError(inst.cpuVal, inst.memVal) + " Not Found";
        return std::make_shared<schedule_framework::ProtoMapPreFilterResult<resource_view::ResourceUnit>>(
            resourceUnit.fragment(), Status{ StatusCode::RESOURCE_NOT_ENOUGH, errMsg });
    }

    YRLOG_DEBUG("{}|(schedule)|instance({}) exact match success", inst.requestID, inst.instanceID);
    // if mono num is 0 in node, the set of feasible node is empty
    const auto &total(bucketsIter->second.total());
    if (total.monopolynum() == 0) {
        YRLOG_WARN("{}|(schedule)the num of pod([{}, {}]) required by the instance({}) is 0", inst.requestID,
                   inst.memVal, inst.cpuVal, inst.instanceID);
        std::string errMsg = FormatMonopolyRequirementForError(inst.cpuVal, inst.memVal) + " Not Enough";
        return std::make_shared<schedule_framework::ProtoMapPreFilterResult<resource_view::ResourceUnit>>(
            resourceUnit.fragment(), Status{ StatusCode::RESOURCE_NOT_ENOUGH, errMsg });
    }

    return std::make_shared<schedule_framework::ProtoMapPreFilterResult<resource_view::BucketInfo>>(
        bucketsIter->second.allocatable(), Status::OK());
}

std::shared_ptr<schedule_framework::PreFilterResult> DefaultPreFilter::CommonPreFilter(
    const std::shared_ptr<schedule_framework::PreAllocatedContext> &ctx,
    const resource_view::ResourceUnit &resourceUnit, const InstanceInfo &inst) const
{
    YRLOG_DEBUG("{}|(schedule)use common preFilter", inst.requestID);
    auto status = Status::OK();
    if (resourceUnit.fragment_size() == 0) {
        YRLOG_WARN("{}|(schedule)fragment in resourceUnit is empty", inst.requestID);
        status = Status{ StatusCode::RESOURCE_NOT_ENOUGH, "No Resource In Cluster" };
    }
    return std::make_shared<schedule_framework::ProtoMapPreFilterResult<resource_view::ResourceUnit>>(
        resourceUnit.fragment(), status);
}

std::shared_ptr<schedule_framework::SchedulePolicyPlugin> DefaultPreFilterCreator()
{
    return std::make_shared<DefaultPreFilter>();
}

REGISTER_SCHEDULER_PLUGIN(DEFAULT_PREFILTER_NAME, DefaultPreFilterCreator);

}  // namespace functionsystem::schedule_plugin::prefilter
