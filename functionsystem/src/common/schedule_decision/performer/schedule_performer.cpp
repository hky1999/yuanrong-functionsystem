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

#include "schedule_performer.h"

#include "common/schedule_plugin/common/round_allocation_context.h"

namespace functionsystem::schedule_decision {

void UpdateInstanceWithVectorAllocations(const ScheduleResult &schedResult, resource_view::InstanceInfo &instance)
{
    auto *resources = instance.mutable_resources()->mutable_resources();
    // Set heterogeneous allocation info (will be merged into vectorAllocations processing below)
    for (const auto &allocated : schedResult.allocatedVectors) {
        auto *vectors = (*resources)[allocated.first].mutable_vectors();
        (*resources)[allocated.first].set_name(allocated.first);
        (*resources)[allocated.first].set_type(resource_view::ValueType::Value_Type_VECTORS);
        for (const auto &value : allocated.second.values()) {
            (*vectors->mutable_values())[value.first] = value.second;
        }
    }
    // Set allocation info for vector-type resource (generic implementation)
    for (const auto &vectorAllocation : schedResult.vectorAllocations) {
        auto *vectors = (*resources)[vectorAllocation.type].mutable_vectors();
        (*resources)[vectorAllocation.type].set_name(vectorAllocation.type);
        (*resources)[vectorAllocation.type].set_type(resource_view::ValueType::Value_Type_VECTORS);
        for (const auto &value : vectorAllocation.allocationValues.values()) {
            (*vectors->mutable_values())[value.first] = value.second;
        }
    }
}

void SchedulePerformer::Allocate(const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
                                 const std::string &selected, const std::string &requestID,
                                 resource_view::InstanceInfo ins,
                                 ScheduleResult &schedResult)
{
    const auto instanceID = ins.instanceid();
    if (type_ == AllocateType::ALLOCATION) {
        schedResult.allocatedPromise = std::make_shared<litebus::Promise<Status>>();
        ASSERT_IF_NULL(resourceView_);
        (void)resourceView_->AddInstances(
            { { ins.instanceid(), resource_view::InstanceAllocatedInfo{ ins, schedResult.allocatedPromise } } });
    }
    if (auto *roundContext = dynamic_cast<schedule_framework::RoundAllocationContext *>(context.get());
        roundContext != nullptr && roundContext->snapshot != nullptr) {
        // The round context owns both the reservation and its overlay entry so
        // it can later remove an exact mutation without rebuilding all pending
        // reservations. Move the prepared resources into the journal payload
        // after applying them to the overlay, avoiding a second protobuf copy.
        roundContext->RecordReservation(selected, requestID, std::move(ins));
        return;
    } else {
        context->allocated[selected].resource = context->allocated[selected].resource.resources().size() == 0
                                                    ? ins.resources()
                                                    : context->allocated[selected].resource + ins.resources();
        context->InvalidateEffectiveAllocatable(selected);

        context->allocatedLabels[selected] =
            context->allocatedLabels[selected] + ToLabelKVs(ins.labels()) + ToLabelKVs(ins.kvlabels());
        context->RecordRequestReservation(requestID, selected, ins);
    }

    // local and domain need to mark agent is selected to avoid select same agent
    // while two instance scheduling in a short time
    context->preAllocatedSelectedFunctionAgentMap[ins.instanceid()] = selected;
    context->RememberRestoreMemory(ins);
    context->preAllocatedSelectedFunctionAgentSet.insert(selected);
}

void SchedulePerformer::PreAllocated(const resource_view::InstanceInfo &ins,
                                     const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
                                     const std::string &requestID, const std::string &traceID,
                                     ScheduleResult &schedResult)
{
    if (schedResult.code != static_cast<int32_t>(StatusCode::SUCCESS)) {
        return;
    }
    auto selected = schedResult.unitID;
    YRLOG_INFO("{}|{}|scheduler {} is selected.", traceID, requestID, selected);
    DoPreAllocated(ins, requestID, context, selected, schedResult);
}

void SchedulePerformer::DoPreAllocated(const resource_view::InstanceInfo &ins, const std::string &requestID,
                                       const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
                                       const std::string &selected, ScheduleResult &schedResult)
{
    auto backupIns = ins;
    const auto &required = ins.resources().resources();
    ASSERT_IF_NULL(resourceView_);
    for (auto &req : required) {
        auto resourceName = req.first;
        if (resource_view::IsHeterogeneousResource(resourceName) || resource_view::IsDiskResource(resourceName)) {
            backupIns.mutable_resources()->mutable_resources()->erase(req.first);
        }
    }
    UpdateInstanceWithVectorAllocations(schedResult, backupIns);
    (*backupIns.mutable_schedulerchain()->Add()) = selected;
    backupIns.set_unitid(selected);
    Allocate(context, selected, requestID, std::move(backupIns), schedResult);
}

std::string SchedulePerformer::GetAlreadyScheduledResult(const std::string &requestID,
                                                         const resource_view::ResourceViewInfo &resourceInfo) const
{
    return GetAlreadyScheduledResult(requestID, resource_view::ScheduleResourceView(resourceInfo));
}

std::string SchedulePerformer::GetAlreadyScheduledResult(
    const std::string &requestID, const resource_view::ScheduleResourceView &resourceView) const
{
    std::string alreadyScheduledResult = "";
    const auto &placements = resourceView.GetRequestPlacements();
    if (placements.find(requestID) == placements.end()) {
        return alreadyScheduledResult;
    }

    alreadyScheduledResult = placements.at(requestID);
    if (type_ == AllocateType::ALLOCATION) {
        return alreadyScheduledResult;
    }
    const auto *unit = resourceView.FindUnit(alreadyScheduledResult);
    if (unit == nullptr) {
        YRLOG_ERROR("resource view does not have a agent unit with ID {}.", alreadyScheduledResult);
        return "";
    }

    alreadyScheduledResult = unit->ownerid();
    return alreadyScheduledResult;
}

void SchedulePerformer::RollBackAllocated(const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
                                          const std::string &selected, const resource_view::InstanceInfo &ins,
                                          const std::shared_ptr<resource_view::ResourceView> &resourceView)
{
    if (context->allocated.find(selected) != context->allocated.end()) {
        context->allocated[selected].resource = context->allocated[selected].resource - ins.resources();
        context->InvalidateEffectiveAllocatable(selected);
    }
    if (context->allocatedLabels.find(selected) != context->allocatedLabels.end()) {
        context->allocatedLabels[selected] =
            context->allocatedLabels[selected] - ToLabelKVs(ins.labels()) - ToLabelKVs(ins.kvlabels());
    }
    const auto selectedInstance = context->preAllocatedSelectedFunctionAgentMap.find(ins.instanceid());
    if (selectedInstance != context->preAllocatedSelectedFunctionAgentMap.end() && selectedInstance->second == selected) {
        context->preAllocatedSelectedFunctionAgentMap.erase(selectedInstance);
        context->preAllocatedRestoreMemory.erase(ins.instanceid());
    }
    context->preAllocatedSelectedFunctionAgentSet.erase(selected); // need to free pod while rollback
    if (auto *roundContext = dynamic_cast<schedule_framework::RoundAllocationContext *>(context.get());
        roundContext != nullptr) {
        roundContext->RemoveReservation(ins.instanceid());
    }
    // rollback the preAllocated instance
    if (type_ == AllocateType::ALLOCATION) {
        ASSERT_IF_NULL(resourceView);
        resourceView->DeleteInstances({ ins.instanceid() }, true);
    }
}

void SchedulePerformer::RollBackGroupAllocated(const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
                                               const std::list<ScheduleResult> &results,
                                               const std::vector<std::shared_ptr<messages::ScheduleRequest>> &requests,
                                               const std::shared_ptr<resource_view::ResourceView> &resourceView,
                                               AllocateType type)
{
    auto index = 0;
    for (auto result : results) {
        // rollback successful schedule result
        if (result.code != static_cast<int32_t>(StatusCode::SUCCESS)) {
            index++;
            continue;
        }
        auto request = requests[index];
        auto selectedId = result.id;
        if (type == AllocateType::PRE_ALLOCATION) {
            selectedId = context->preAllocatedSelectedFunctionAgentMap[request->instance().instanceid()];
        }
        YRLOG_INFO("{}|{}|rollback instance({}) of group({}) schedule result, which selected({})", request->traceid(),
                   request->requestid(), request->instance().instanceid(), request->instance().groupid(), selectedId);
        RollBackAllocated(context, selectedId, request->instance(), resourceView);
        index++;
    }
}

bool SchedulePerformer::IsScheduled(const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
                                    const resource_view::ResourceViewInfo &resourceInfo,
                                    const std::shared_ptr<InstanceItem> &instanceItem, ScheduleResult &result,
                                    std::unordered_map<std::string, int32_t> &preAllocatedSelected)
{
    return IsScheduled(context, resource_view::ScheduleResourceView(resourceInfo), instanceItem, result,
                       preAllocatedSelected);
}

bool SchedulePerformer::IsScheduled(const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
                                    const resource_view::ScheduleResourceView &resourceView,
                                    const std::shared_ptr<InstanceItem> &instanceItem, ScheduleResult &result,
                                    std::unordered_map<std::string, int32_t> &preAllocatedSelected)
{
    auto scheReq = instanceItem->scheduleReq;
    auto requestID = scheReq->requestid();
    auto traceID = scheReq->traceid();
    if (auto roundContext = std::dynamic_pointer_cast<schedule_framework::RoundAllocationContext>(context);
        roundContext != nullptr) {
        const auto *reservation = roundContext->FindReservationByRequest(requestID);
        const auto *unit = reservation == nullptr ? nullptr : resourceView.FindUnit(reservation->unitID);
        if (unit != nullptr) {
            const auto selected = type_ == AllocateType::PRE_ALLOCATION ? unit->ownerid() : reservation->unitID;
            result = ScheduleResult{ selected, static_cast<int32_t>(StatusCode::INSTANCE_ALLOCATED),
                                     "request is already reserved to " + selected, {}, "", {} };
            result.unitID = reservation->unitID;
            return true;
        }
    }
    const auto &groupContext = scheReq->contexts();
    if (type_ == AllocateType::PRE_ALLOCATION
        && groupContext.find(GROUP_SCHEDULE_CONTEXT) != groupContext.end()
        && !groupContext.at(GROUP_SCHEDULE_CONTEXT).groupschedctx().reserved().empty()) {
        auto &unitID = groupContext.at(GROUP_SCHEDULE_CONTEXT).groupschedctx().reserved();
        const auto *unit = resourceView.FindUnit(unitID);
        if (unit == nullptr) {
            return false;
        }
        result.code = static_cast<int32_t>(StatusCode::SUCCESS);
        result.id = unitID;
        result.unitID = unitID;
        if (preAllocatedSelected.find(unitID) == preAllocatedSelected.end()) {
            preAllocatedSelected[unitID] = 0;
        }

        YRLOG_WARN("{}|request {}. request is already reserved to {}", traceID, requestID, result.id);
        auto alreadyScheduledResult = GetAlreadyScheduledResult(requestID, resourceView);
        if (alreadyScheduledResult.empty()) {
            preAllocatedSelected[unitID] += 1;
            PreAllocated(scheReq->instance(), context, requestID, traceID, result);
        }
        context->preAllocatedSelectedFunctionAgentMap[scheReq->instance().instanceid()] = result.id;
        context->RememberRestoreMemory(scheReq->instance());
        context->preAllocatedSelectedFunctionAgentSet.insert(result.id);
        result.unitID = result.id;
        result.id = unit->ownerid();
        return true;
    }
    auto alreadyScheduledResult = GetAlreadyScheduledResult(requestID, resourceView);
    if (!alreadyScheduledResult.empty()) {
        YRLOG_WARN("{}|request {}. request is already scheduled to {}", traceID, requestID, alreadyScheduledResult);
        result = ScheduleResult{ alreadyScheduledResult, static_cast<int32_t>(StatusCode::INSTANCE_ALLOCATED),
                                 "request is already scheduled to " + alreadyScheduledResult, {}, "", {}};
        return true;
    }
    return false;
}

ScheduleResult SchedulePerformer::DoSelectOne(const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
                                              const resource_view::ResourceViewInfo &resourceInfo,
                                              const std::shared_ptr<InstanceItem> &instanceItem)
{
    return DoSelectOne(context, resource_view::ScheduleResourceView(resourceInfo), instanceItem);
}

ScheduleResult SchedulePerformer::DoSelectOne(const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
                                              const resource_view::ScheduleResourceView &resourceView,
                                              const std::shared_ptr<InstanceItem> &instanceItem)
{
    context->pluginCtx = instanceItem->scheduleReq->mutable_contexts();
    auto result = ScheduleResult{};
    std::unordered_map<std::string, int32_t> _;
    if (IsScheduled(context, resourceView, instanceItem, result, _)) {
        return result;
    }
    ASSERT_IF_NULL(framework_);
    auto results = framework_->SelectFeasible(context, instanceItem->scheduleReq->instance(), resourceView, 1);
    if (results.code != static_cast<int32_t>(StatusCode::SUCCESS)) {
        return ScheduleResult{ "", results.code, results.reason, {}, "", {}, {} };
    }
    if (!instanceItem->conflictedUnitID.empty()) {
        std::priority_queue<schedule_framework::NodeScore> candidates;
        while (!results.sortedFeasibleNodes.empty()) {
            auto candidate = results.sortedFeasibleNodes.top();
            results.sortedFeasibleNodes.pop();
            if (candidate.name != instanceItem->conflictedUnitID) {
                candidates.emplace(std::move(candidate));
            }
        }
        results.sortedFeasibleNodes = std::move(candidates);
        if (results.sortedFeasibleNodes.empty()) {
            return ScheduleResult{ "", static_cast<int32_t>(StatusCode::RESOURCE_NOT_ENOUGH),
                                   "no alternative Unit after Local conflict", {}, "", {}, {} };
        }
    }
    return SelectFromResults(context, resourceView, instanceItem, { results.sortedFeasibleNodes, _, false });
}

bool SchedulePerformer::IsScheduleResultNeedPreempt(const ScheduleResult &result)
{
    return preemptInstanceCallback_ != nullptr &&
           (result.code == static_cast<int32_t>(StatusCode::RESOURCE_NOT_ENOUGH) ||
            result.code == static_cast<int32_t>(StatusCode::AFFINITY_SCHEDULE_FAILED));
}

GroupScheduleResult SchedulePerformer::DoCollectGroupResult(const std::list<ScheduleResult> &results)
{
    GroupScheduleResult groupResult;
    groupResult.code = static_cast<int32_t>(StatusCode::SUCCESS);
    auto index = 0;
    for (auto result : results) {
        groupResult.results.emplace_back(result);
        // if any instance fails to be scheduled, error codes need to be set in groups.
        if (result.code != static_cast<int32_t>(StatusCode::SUCCESS) &&
            result.code != static_cast<int32_t>(StatusCode::INSTANCE_ALLOCATED)) {
            groupResult.code = result.code;
            groupResult.reason = "\n" + result.reason + "";
        }
        index++;
    }
    return groupResult;
}

ScheduleResult SchedulePerformer::SelectFromResults(
    const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
    const resource_view::ResourceViewInfo &resourceInfo, const std::shared_ptr<InstanceItem> &instanceItem,
    std::priority_queue<schedule_framework::NodeScore> &candidateNode,
    std::unordered_map<std::string, int32_t> &preAllocatedSelected)
{
    return SelectFromResults(context, resource_view::ScheduleResourceView(resourceInfo), instanceItem,
                             { candidateNode, preAllocatedSelected, false });
}

bool SchedulePerformer::PrepareCandidate(CandidateSelection &selection,
                                         schedule_framework::NodeScore &nodeScore)
{
    const auto selected = selection.preAllocatedSelected.find(nodeScore.name);
    if (selected != selection.preAllocatedSelected.end()) {
        nodeScore.availableForRequest -= selected->second;
        selection.preAllocatedSelected.erase(selected);
        if (nodeScore.availableForRequest <= 0) {
            selection.candidateNode.pop();
            return false;
        }
    }
    selection.candidateNode.pop();
    if (nodeScore.availableForRequest > 0) {
        --nodeScore.availableForRequest;
    }
    return true;
}

void SchedulePerformer::RefreshCandidate(
    const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
    const resource_view::ScheduleResourceView &resourceView,
    const std::shared_ptr<InstanceItem> &instanceItem,
    const schedule_framework::NodeScore &nodeScore, CandidateSelection &selection)
{
    if (!selection.refreshSelectedUnit) {
        return;
    }
    ASSERT_IF_NULL(framework_);
    const auto &request = instanceItem->scheduleReq;
    auto refreshed = framework_->EvaluateUnit(context, request->instance(), resourceView, nodeScore.name);
    if (refreshed.feasible) {
        refreshed.score.placementCount = nodeScore.placementCount;
        if (placementPolicy_ == "spread") {
            ++refreshed.score.placementCount;
        }
        selection.candidateNode.emplace(std::move(refreshed.score));
        return;
    }
    if (refreshed.fatal) {
        YRLOG_ERROR("{}|{}|failed to reevaluate selected Unit({}): {}", request->traceid(), request->requestid(),
                    nodeScore.name, refreshed.status.ToString());
    }
}

ScheduleResult SchedulePerformer::CommitCandidate(
    const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
    const resource_view::ScheduleResourceView &resourceView,
    const std::shared_ptr<InstanceItem> &instanceItem,
    schedule_framework::NodeScore &nodeScore, CandidateSelection &selection)
{
    auto result = ScheduleResult{ nodeScore.name, 0, "", nodeScore.realIDs, nodeScore.heteroProductName,
                                  nodeScore.allocatedVectors, nodeScore.vectorAllocations };
    if (!selection.refreshSelectedUnit && nodeScore.availableForRequest > 0) {
        if (placementPolicy_ == "spread") {
            ++nodeScore.placementCount;
        }
        selection.candidateNode.emplace(nodeScore);
    }
    result.unitID = result.id;
    const auto *unit = resourceView.FindUnit(result.unitID);
    if (unit == nullptr) {
        return ScheduleResult{ "", static_cast<int32_t>(StatusCode::RESOURCE_NOT_ENOUGH),
                               "selected resource unit disappeared from the pinned view", {}, "", {}, {} };
    }
    result.id = unit->ownerid();
    const auto &request = instanceItem->scheduleReq;
    PreAllocated(request->instance(), context, request->requestid(), request->traceid(), result);
    RefreshCandidate(context, resourceView, instanceItem, nodeScore, selection);
    return result;
}

ScheduleResult SchedulePerformer::SelectFromResults(
    const std::shared_ptr<schedule_framework::PreAllocatedContext> &context,
    const resource_view::ScheduleResourceView &resourceView, const std::shared_ptr<InstanceItem> &instanceItem,
    CandidateSelection selection)
{
    auto scheReq = instanceItem->scheduleReq;
    auto result = ScheduleResult{};
    if (IsScheduled(context, resourceView, instanceItem, result, selection.preAllocatedSelected)) {
        return result;
    }
    // reuse spec context
    schedule_framework::CopyPluginContext(*scheReq->mutable_contexts(), *context->pluginCtx);
    while (!selection.candidateNode.empty()) {
        auto nodeScore = selection.candidateNode.top();
        // availableForRequest should never be 0
        ASSERT_FS(nodeScore.availableForRequest == -1 || nodeScore.availableForRequest > 0);
        if (nodeScore.availableForRequest == -1 && !selection.refreshSelectedUnit) {
            return ScheduleResult{ nodeScore.name, 0, "", nodeScore.realIDs, nodeScore.heteroProductName,
                                   nodeScore.allocatedVectors, nodeScore.vectorAllocations };
        }
        // preAllocate is used for range scheduling. After range scheduling fails, some requests that are successfully
        // reserved are not rolled back. Only the requests that fail and after failed request are rolled
        // back.
        if (!PrepareCandidate(selection, nodeScore)) {
            continue;
        }
        return CommitCandidate(context, resourceView, instanceItem, nodeScore, selection);
    }
    return ScheduleResult{ "",
                           static_cast<int32_t>(StatusCode::RESOURCE_NOT_ENOUGH),
                           "no available resource that meets the request requirements",
                           {},
                           "",
                           {},
                           {}};
}
}
