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

#include "resource_view_actor.h"

#include <utility>
#include <chrono>
#include <cmath>

#include "async/async.hpp"
#include "litebus.hpp"
#include "async/asyncafter.hpp"
#include "async/uuid_generator.hpp"
#include "common/constants/constants.h"
#include "common/logs/logging.h"
#include "common/metrics/metrics_adapter.h"
#include "common/status/status.h"
#include "common/types/instance_state.h"
#include "resource_tool.h"
#include "resource_sample_freshness.h"
#include "memory_elasticity_fence.h"
#include "restore_admission.h"
#include "scala_resource_tool.h"
#include "usage_trend_tracker.h"
#include "utils/string_utils.hpp"

namespace functionsystem::resource_view {

namespace {
constexpr uint32_t SNAPSHOT_PUBLICATION_MUTATION_LIMIT = 32;
constexpr uint64_t SNAPSHOT_PUBLICATION_MAX_DELAY_MS = 1;
}  // namespace
static const int32_t DEFAULT_PRINT_RESOURCE_VIEW_TIMER_COUNT = 60;
static const std::string NEED_RECOVER_VIEW = "needRecoverView";
static const std::string IDLE_TO_RECYCLE = "yr-idle-to-recycle";
// 当变更数量超过此阈值时，使用异步处理
static const int64_t ASYNC_MERGE_THRESHOLD = 50;

ResourceUnit CollectRemovedResources(const ResourceUnit &current, const Resources &updatedCapacity,
                                     Resources &retainedCapacity)
{
    ResourceUnit removedResources;
    for (const auto &[resourceName, resource] : current.capacity().resources()) {
        if (updatedCapacity.resources().contains(resourceName)) {
            continue;
        }
        (*removedResources.mutable_capacity()->mutable_resources())[resourceName] = resource;
        retainedCapacity.mutable_resources()->erase(resourceName);
        auto allocatable = current.allocatable().resources().find(resourceName);
        if (allocatable != current.allocatable().resources().end()) {
            (*removedResources.mutable_allocatable()->mutable_resources())[resourceName] = allocatable->second;
        }
    }
    return removedResources;
}

ResourceUnit CollectReportedRemovedResources(const ResourceUnit &current, const CapacityChange &capacityChange)
{
    ResourceUnit removedResources;
    for (const auto &resourceName : capacityChange.removedresourcenames()) {
        auto capacity = current.capacity().resources().find(resourceName);
        if (capacity == current.capacity().resources().end()) {
            continue;
        }
        (*removedResources.mutable_capacity()->mutable_resources())[resourceName] = capacity->second;
        auto allocatable = current.allocatable().resources().find(resourceName);
        if (allocatable != current.allocatable().resources().end()) {
            (*removedResources.mutable_allocatable()->mutable_resources())[resourceName] = allocatable->second;
        }
    }
    return removedResources;
}

ResourceViewActor::ResourceViewActor(const std::string &name, std::string id, const Param &param)
    : BasisActor(name), unitID_(std::move(id)), isLocal_(param.isLocal),
      tenantPodReuseTimeWindow_(param.tenantPodReuseTimeWindow)
{
    enableTenantAffinity_ = param.enableTenantAffinity;
    scheduleSnapshotEnabled_ = param.enableScheduleSnapshot;
    scheduleSnapshotStore_ = std::make_shared<ScheduleSnapshotStore>();
    if (scheduleSnapshotEnabled_) {
        scheduleSnapshotBuilder_ = std::make_unique<ScheduleSnapshotBuilder>(scheduleSnapshotStore_);
    }
    if (auto pos = name.find_last_of('-'); pos != std::string::npos) {
        actorSuffix_ = name.substr(pos);
    }
    YRLOG_DEBUG("construct resource view actor. {}", name);
    // ResourcePoller and ResourceView who register the sendPull have the same life cycle.
    auto sendPull = [this](const std::string &id) { SendPullResource(id); };
    auto delegateReset = [this](const std::string &id) {
        litebus::Async(GetAID(), &ResourceViewActor::DelegateResetPull, id);
    };
    auto defer = [this](uint64_t duration) {
        litebus::AsyncAfter(duration, GetAID(), &ResourceViewActor::TriggerTryPull);
    };
    poller_ = std::make_shared<ResourcePoller>(sendPull, delegateReset, defer, 0);
}

void ResourceViewActor::Init()
{
    YRLOG_DEBUG("resource view actor alloc resource view memory");
    view_ = std::make_shared<ResourceUnit>(std::move(InitResource(unitID_)));
    if (scheduleSnapshotEnabled_) {
        snapshotDirtySet_.structure = true;
        snapshotDirtySet_.requestPlacements = true;
        snapshotDirtySet_.ownerLabels = true;
        snapshotDirtySet_.monopolyIndex = true;
        snapshotDirtySet_.metadata = true;
        CommitScheduleSnapshot();
    }
    Receive("PullResource", &ResourceViewActor::PullResource);
    Receive("ReportResource", &ResourceViewActor::ReportResource);
}

void ResourceViewActor::Finalize()
{
    if (snapshotFlushScheduled_) {
        litebus::TimerTools::Cancel(snapshotFlushTimer_);
        snapshotFlushScheduled_ = false;
    }
    if (poller_) {
        poller_->Stop();
    }
    YRLOG_DEBUG("Finalize resource view actor");
}

ResourceViewActor::SnapshotMutationGuard::SnapshotMutationGuard(ResourceViewActor &actor) : actor_(actor)
{
    actor_.BeginSnapshotMutation();
}

ResourceViewActor::SnapshotMutationGuard::~SnapshotMutationGuard()
{
    actor_.EndSnapshotMutation();
}

void ResourceViewActor::BeginSnapshotMutation()
{
    if (!scheduleSnapshotEnabled_) {
        return;
    }
    ++snapshotMutationDepth_;
}

void ResourceViewActor::EndSnapshotMutation()
{
    if (!scheduleSnapshotEnabled_) {
        return;
    }
    if (snapshotMutationDepth_ == 0) {
        return;
    }
    --snapshotMutationDepth_;
    if (snapshotMutationDepth_ == 0 && !snapshotDirtySet_.Empty()) {
        ++snapshotPendingMutationCount_;
        if (snapshotDirtySet_.structure ||
            snapshotPendingMutationCount_ >= SNAPSHOT_PUBLICATION_MUTATION_LIMIT) {
            CommitScheduleSnapshot();
        } else {
            ScheduleSnapshotFlush();
        }
    }
}

void ResourceViewActor::MarkSnapshotUnitDirty(const std::string &unitID)
{
    if (scheduleSnapshotEnabled_ && !unitID.empty()) {
        snapshotDirtySet_.unitIDs.emplace(unitID);
    }
}

void ResourceViewActor::MarkSnapshotStructureDirty(const std::string &unitID)
{
    if (!scheduleSnapshotEnabled_) {
        return;
    }
    snapshotDirtySet_.structure = true;
    MarkSnapshotUnitDirty(unitID);
}

void ResourceViewActor::MarkSnapshotIndexesDirty(bool requestPlacements, bool ownerLabels)
{
    if (!scheduleSnapshotEnabled_) {
        return;
    }
    snapshotDirtySet_.requestPlacements = snapshotDirtySet_.requestPlacements || requestPlacements;
    snapshotDirtySet_.ownerLabels = snapshotDirtySet_.ownerLabels || ownerLabels;
}

void ResourceViewActor::MarkSnapshotMonopolyIndexDirty()
{
    if (scheduleSnapshotEnabled_) {
        snapshotDirtySet_.monopolyIndex = true;
    }
}

void ResourceViewActor::MarkSnapshotMetadataDirty()
{
    if (scheduleSnapshotEnabled_) {
        snapshotDirtySet_.metadata = true;
    }
}

void ResourceViewActor::MarkSnapshotRequestMutation(const std::string &requestID)
{
    if (scheduleSnapshotEnabled_ && scheduleSnapshotBuilder_ != nullptr && !requestID.empty()) {
        (void)scheduleSnapshotBuilder_->RecordRequestMutation(requestID);
    }
}

void ResourceViewActor::ScheduleSnapshotFlush()
{
    if (!scheduleSnapshotEnabled_ || snapshotFlushScheduled_) {
        return;
    }
    snapshotFlushScheduled_ = true;
    snapshotFlushTimer_ = litebus::AsyncAfter(
        SNAPSHOT_PUBLICATION_MAX_DELAY_MS, GetAID(), &ResourceViewActor::FlushScheduleSnapshot);
}

void ResourceViewActor::FlushScheduleSnapshot()
{
    snapshotFlushScheduled_ = false;
    if (!snapshotDirtySet_.Empty()) {
        CommitScheduleSnapshot();
    }
}

void ResourceViewActor::CommitScheduleSnapshot()
{
    if (!scheduleSnapshotEnabled_ || view_ == nullptr || scheduleSnapshotBuilder_ == nullptr) {
        return;
    }
    if (snapshotFlushScheduled_) {
        litebus::TimerTools::Cancel(snapshotFlushTimer_);
        snapshotFlushScheduled_ = false;
    }
    if (isLocal_) {
        OwnerLabelIndex labels;
        labels.emplace(view_->id(), view_->nodelabels());
        scheduleSnapshotBuilder_->BuildAndPublish(*view_, GetCurrentSchedulerLevel(), reqIDToUnitIDMap_, labels,
                                                  snapshotDirtySet_);
    } else {
        // The builder copies labels only when this field is dirty. Avoid
        // cloning the complete Domain label index for unrelated updates.
        scheduleSnapshotBuilder_->BuildAndPublish(*view_, GetCurrentSchedulerLevel(), reqIDToUnitIDMap_,
                                                  allLocalLabels_, snapshotDirtySet_);
    }
    snapshotDirtySet_.Clear();
    snapshotPendingMutationCount_ = 0;
    if (snapshotNotificationPending_) {
        snapshotNotificationPending_ = false;
        NotifyResourceUpdated();
    }
}
void ResourceViewActor::DeleteInstancesBySubUnit(ResourceUnit &view, const ResourceUnit &subUnit)
{
    for (auto &valueInstIter : subUnit.instances()) {
        auto viewInstIter = view.mutable_instances()->find(valueInstIter.first);
        if (viewInstIter == view.mutable_instances()->end()) {
            YRLOG_WARN("delete unknown instance {} from resource unit {}.", valueInstIter.first, subUnit.id());
            continue;
        }
        (void)view.mutable_instances()->erase(viewInstIter);
    }
}

void ResourceViewActor::AddInstancesBySubUnit(ResourceUnit &view, const ResourceUnit &subUnit)
{
    for (auto &valueInstIter : subUnit.instances()) {
        auto viewInstIter = view.instances().find(valueInstIter.first);
        if (viewInstIter != view.instances().end()) {
            YRLOG_WARN("add duplicated instance {} from resource unit {}.", valueInstIter.first, subUnit.id());
            continue;
        }
        (*view.mutable_instances())[valueInstIter.first] = valueInstIter.second;
    }
}

void ResourceViewActor::AddResourceBySubUnit(ResourceUnit &view, const ResourceUnit &value)
{
    // Add capacity resources to the upper-level resource view.
    if (view.has_capacity() && view.capacity().resources_size() > 0) {
        // 2. branch resources have no set/sets. --> assign
        // 3. branch resources only have set. --> not exist
        // 4. branch resources only have sets --> do addition
        *view.mutable_capacity() = view.capacity() + value.capacity();
    } else {
        // 1. branch resources not exist or empty. --> assign
        *view.mutable_capacity() = value.capacity();
    }

    // Add allocatable resources to the upper-level resource view.
    if (view.has_allocatable() && view.allocatable().resources_size() > 0) {
        // 2. branch resources have no set/sets. --> assign
        // 3. branch resources only have set. --> not exist
        // 4. branch resources only have sets --> do addition
        *view.mutable_allocatable() = view.allocatable() + value.allocatable();
    } else {
        // 1. branch resources not exist or empty. --> assign
        *view.mutable_allocatable() = value.allocatable();
    }

    if (IsValid(value.actualuse())) {
        // Add actual use resources to the upper-level resource view.
        if (view.has_actualuse() && view.actualuse().resources_size() > 0) {
            // 2. branch resources have no set/sets. --> assign
            // 3. branch resources only have set. --> not exist
            // 4. branch resources only have sets --> do addition
            *view.mutable_actualuse() = view.actualuse() + value.actualuse();
        } else {
            // 1. branch resources not exist or empty. --> assign
            *view.mutable_actualuse() = value.actualuse();
        }
    }

    // add labels to top level unit
    (*view.mutable_nodelabels()) = view.nodelabels() + value.nodelabels();
}

Status ResourceViewActor::AddResourceUnit(const ResourceUnit &value)
{
    SnapshotMutationGuard snapshotGuard(*this);
    if (!IsValidUnit(value)) {
        YRLOG_ERROR("add invalid resource unit.");
        return Status(PARAMETER_ERROR, "add invalid resource unit ");
    }

    ASSERT_IF_NULL(view_);
    if (view_->fragment().find(value.id()) != view_->fragment().end()) {
        YRLOG_ERROR("add duplicated resource unit.");
        return Status(PARAMETER_ERROR, "add duplicated resource unit ");
    }

    AddInstancesBySubUnit(*view_, value);
    AddResourceBySubUnit(*view_, value);
    AddBucketIndexBySubUnit(*view_, value);

    // add unit to top level's fragment
    auto fragment = view_->mutable_fragment();
    YRLOG_DEBUG("add unit({}) to top level's fragment", value.id());
    (*fragment)[value.id()] = value;

    // map request id to unit id
    for (auto &valueInstIter : value.instances()) {
        if (!valueInstIter.second.requestid().empty()) {
            (void)reqIDToUnitIDMap_.emplace(valueInstIter.second.requestid(), value.id());
        }
    }

    UpdateTime();
    MarkResourceUpdated();
    view_->set_revision(view_->revision() + 1);
    YRLOG_INFO("add one resource unit, resource unit id = {}, resource capacity = {} allocatable = {}, "
               "current revision = {}", value.id(), ToString(value.capacity()), ToString(value.allocatable()),
               view_->revision());

    if (isLocal_) {
        Addition addition;
        (*addition.mutable_resourceunit()) = value;
        // for reporting resource, the owner should be transferred to local id which is used by uplayer dispatching
        // schedule request
        addition.mutable_resourceunit()->set_ownerid(view_->id());
        ResourceUnitChange resourceUnitChange;
        resourceUnitChange.set_resourceunitid(value.id());
        *resourceUnitChange.mutable_addition() = addition;
        StoreChange(view_->revision(), resourceUnitChange);

        fragment->at(value.id()).set_ownerid(value.ownerid().empty() ? value.id() : value.ownerid());
        if (value.status() == static_cast<uint32_t>(UnitStatus::NORMAL)) {
            PodRecycler(value);
        }
    }
    MarkSnapshotStructureDirty(value.id());
    MarkSnapshotIndexesDirty(true, true);
    MarkSnapshotMonopolyIndexDirty();
    MarkSnapshotMetadataDirty();
    return Status::OK();
}

Status ResourceViewActor::AddResourceUnitWithUrl(const ResourceUnit &value, const std::string &url)
{
    SnapshotMutationGuard snapshotGuard(*this);
    if (localInfoMap_.find(value.id()) != localInfoMap_.end()) {
        YRLOG_ERROR("add duplicated local resource unit, resource unit id = {}", value.id());
        return Status(PARAMETER_ERROR, "add duplicated local resource unit");
    }

    for (auto &agentFragmentIter : value.fragment()) {
        auto status = AddResourceUnit(agentFragmentIter.second);
        if (status.IsError()) {
            (void)localInfoMap_.erase(value.id());
            YRLOG_ERROR("failed to add local resource unit, resource unit id = {}", value.id());
            return status;
        }
        view_->mutable_fragment()->at(agentFragmentIter.second.id()).set_ownerid(value.id());
        localInfoMap_[value.id()].agentIDs.insert(agentFragmentIter.second.id());
        if (isHeader_) {
            if (view_->fragment().contains(agentFragmentIter.second.id())) {
                functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetPodResource(
                    agentFragmentIter.second.id(), view_->fragment().at(agentFragmentIter.second.id()));
            }
        }
    }

    urls_[value.id()] = url;
    poller_->Add(value.id());
    allLocalLabels_[value.id()] = value.nodelabels();
    localInfoMap_[value.id()].localRevisionInDomain = value.revision();
    localInfoMap_[value.id()].localViewInitTime = value.viewinittime();
    MarkSnapshotIndexesDirty(false, true);
    YRLOG_INFO("register one local scheduler to domain resourceview, resource unit id = {}, current revision = {}",
               value.id(), localInfoMap_[value.id()].localRevisionInDomain);
    NotifyResourceUpdated();
    return Status::OK();
}

void ResourceViewActor::AddBucketIndexBySubUnit(ResourceUnit &view, const ResourceUnit &fragmentUnit)
{
    // add bucketIndex to top level unit
    for (auto [proportion, bucketIndex] : fragmentUnit.bucketindexs()) {
        auto &topBucketIndex = (*view.mutable_bucketindexs())[proportion];
        for (auto [mem, bucket] : bucketIndex.buckets()) {
            auto &topBucket = (*topBucketIndex.mutable_buckets())[mem];
            auto &info = (*topBucket.mutable_allocatable())[fragmentUnit.id()];
            auto &total = *topBucket.mutable_total();
            total.set_sharednum((total.sharednum() - info.sharednum()) + bucket.total().sharednum());
            total.set_monopolynum((total.monopolynum() - info.monopolynum()) + bucket.total().monopolynum());
            info = bucket.total();
        }
    }
}

void ResourceViewActor::DeleteResourceBySubUnit(ResourceUnit &view, ResourceUnit &value)
{
    // Identifies the resource as expired.
    auto *capacity = value.mutable_capacity();
    for (auto &resources : *capacity->mutable_resources()) {
        resources.second.set_expired(true);
    }

    auto *allocatable = value.mutable_allocatable();
    for (auto &resources : *allocatable->mutable_resources()) {
        resources.second.set_expired(true);
    }

    auto *actualUse = value.mutable_actualuse();
    for (auto &resources : *actualUse->mutable_resources()) {
        resources.second.set_expired(true);
    }

    // vectors: delete from the upper-level resource view.
    // scala or other: subtract from the upper-level resource view.
    (*view.mutable_capacity()) = view.capacity() - value.capacity();
    (*view.mutable_allocatable()) = view.allocatable() - value.allocatable();
    if (IsValid(value.actualuse())) {
        (*view.mutable_actualuse()) = view.actualuse() - value.actualuse();
    }
    (*view.mutable_nodelabels()) = view.nodelabels() - value.nodelabels();
}

void ResourceViewActor::PruneRemovedResources(ResourceUnit &view, const ResourceUnit &removedResources)
{
    for (const auto &[resourceName, resource] : removedResources.capacity().resources()) {
        (void)resource;
        bool hasCapacityContributor = false;
        bool hasAllocatableContributor = false;
        for (const auto &[fragmentId, fragment] : view.fragment()) {
            (void)fragmentId;
            hasCapacityContributor =
                hasCapacityContributor || fragment.capacity().resources().contains(resourceName);
            hasAllocatableContributor =
                hasAllocatableContributor || fragment.allocatable().resources().contains(resourceName);
        }
        if (!hasCapacityContributor) {
            view.mutable_capacity()->mutable_resources()->erase(resourceName);
        }
        if (!hasAllocatableContributor) {
            view.mutable_allocatable()->mutable_resources()->erase(resourceName);
        }
    }
}

Status ResourceViewActor::ClearLocalSchedulerAgentsInDomain(const std::string &localID)
{
    SnapshotMutationGuard snapshotGuard(*this);
    if (localInfoMap_.find(localID) == localInfoMap_.end()) {
        YRLOG_WARN("domain resource view has no information about the local named {}.", localID);
        return Status(PARAMETER_ERROR, "domain resource view has no information about the local.");
    }

    ASSERT_IF_NULL(view_);
    auto &agentIDs = localInfoMap_[localID].agentIDs;
    for (const auto& agentID : agentIDs) {
        auto agentFragmentIter = view_->mutable_fragment()->find(agentID);
        if (agentFragmentIter == view_->mutable_fragment()->end()) {
            YRLOG_WARN("domain resource view does not have a resource unit with ID {}.", agentID);
            continue;
        }
        allLocalLabels_[localID] = allLocalLabels_[localID] - agentFragmentIter->second.nodelabels();
        if (auto status = DeleteResourceUnit(agentID); status.IsError()) {
            YRLOG_WARN("Failed to delete agent resource view named {} from domain resource view.", agentID);
        }
        if (isHeader_) {
            metrics::MetricsAdapter::GetInstance().GetMetricsContext().DeletePodResource(agentID);
        }
    }
    localInfoMap_[localID].agentIDs.clear();
    return Status::OK();
};

Status ResourceViewActor::DeleteLocalResourceView(const std::string &localID)
{
    SnapshotMutationGuard snapshotGuard(*this);
    if (localID.empty()) {
        YRLOG_WARN("delete local resourceview with empty ID.");
        return Status(PARAMETER_ERROR, "delete local resourceview with empty ID.");
    }

    if (auto status = ClearLocalSchedulerAgentsInDomain(localID); status.IsError()) {
        YRLOG_WARN("failed to clear all agent in domain, local id is {}", localID);
        return status;
    }

    (void)localInfoMap_.erase(localID);
    (void)allLocalLabels_.erase(localID);
    MarkSnapshotIndexesDirty(false, true);
    (void)urls_.erase(localID);
    poller_->Del(localID);
    YRLOG_INFO("Successfully deleted local resource view named {} from domain resource view.", localID);
    NotifyResourceUpdated();
    return Status::OK();
}

Status ResourceViewActor::DeleteResourceUnit(const std::string &unitID)
{
    SnapshotMutationGuard snapshotGuard(*this);
    if (unitID.empty()) {
        YRLOG_WARN("delete resource unit with empty ID.");
        return Status(PARAMETER_ERROR, "delete resource unit with empty ID.");
    }

    ASSERT_IF_NULL(view_);
    auto fragment = view_->mutable_fragment();
    auto fragmentIter = fragment->find(unitID);
    if (fragmentIter == fragment->end()) {
        YRLOG_WARN("resource view does not have a resource unit with ID {}.", unitID);
        return Status(PARAMETER_ERROR, "delete resource unit with unknown ID.");
    }

    DeleteResourceBySubUnit(*view_, fragmentIter->second);
    DeleteBucketIndexBySubUnit(*view_, fragmentIter->second);
    DeleteInstancesBySubUnit(*view_, fragmentIter->second);
    for (const auto &entry : fragmentIter->second.instances()) { resourceAdmissionTimes_.erase(entry.first); }

    // unmap request id to unit id
    for (auto &valueInstIter : fragmentIter->second.instances()) {
        if (!valueInstIter.second.requestid().empty()) {
            (void)reqIDToUnitIDMap_.erase(valueInstIter.second.requestid());
        }
    }
    (void)fragment->erase(fragmentIter);

    view_->set_revision(view_->revision() + 1);
    UpdateTime();

    if (isLocal_) {
        (void)agentCacheMap_.erase(unitID);

        Deletion deletion;
        ResourceUnitChange resourceUnitChange;
        resourceUnitChange.set_resourceunitid(unitID);
        *resourceUnitChange.mutable_deletion() = deletion;
        StoreChange(view_->revision(), resourceUnitChange);
    }
    MarkResourceUpdated();
    MarkSnapshotStructureDirty(unitID);
    MarkSnapshotIndexesDirty(true, true);
    MarkSnapshotMonopolyIndexDirty();
    MarkSnapshotMetadataDirty();
    YRLOG_INFO("delete {} resource unit from resource view, current revision = {}", unitID, view_->revision());
    return Status::OK();
}

void ResourceViewActor::DeleteBucketIndexBySubUnit(ResourceUnit &view, const ResourceUnit &fragmentUnit)
{
    for (auto [proportion, bucketIndex] : fragmentUnit.bucketindexs()) {
        auto &topBucketIndex = (*view.mutable_bucketindexs())[proportion];
        for (auto [mem, bucket] : bucketIndex.buckets()) {
            [[maybe_unused]] const auto& unused = bucket;
            auto &topBucket = (*topBucketIndex.mutable_buckets())[mem];
            auto &info = (*topBucket.mutable_allocatable())[fragmentUnit.id()];
            auto &total = *topBucket.mutable_total();
            total.set_sharednum(total.sharednum() - info.sharednum());
            total.set_monopolynum(total.monopolynum() - info.monopolynum());
            (void)(*topBucket.mutable_allocatable()).erase(fragmentUnit.id());
        }
    }
}

Status ResourceViewActor::UpdateResourceUnit(const std::shared_ptr<ResourceUnit> &value, const UpdateType &type)
{
    SnapshotMutationGuard snapshotGuard(*this);
    if (value == nullptr) {
        YRLOG_ERROR("update null resources unit");
        return Status(PARAMETER_ERROR, "update null resources unit");
    }

    if (value->id().empty() || !value->has_capacity() || !value->has_allocatable() || !IsValid(value->capacity()) ||
        !IsValid(value->allocatable())) {
        YRLOG_ERROR("update invalid resource unit.");
        return Status(PARAMETER_ERROR, "update invalid resource unit");
    }

    ASSERT_IF_NULL(view_);
    auto unit = view_->mutable_fragment()->find(value->id());
    if (unit == view_->mutable_fragment()->end()) {
        YRLOG_ERROR("resource view does not have a resource unit with ID {}.", value->id());
        return Status(PARAMETER_ERROR, "update resource unit with unknown ID.");
    }

    switch (type) {
        case UpdateType::UPDATE_ACTUAL:
            UpdateResourceUnitActual(value);
            MarkSnapshotUnitDirty(value->id());
            MarkSnapshotMetadataDirty();
            break;
        case UpdateType::UPDATE_DYNAMIC:
            UpdateResourceUnitDynamic(value);
            MarkSnapshotUnitDirty(value->id());
            MarkSnapshotMetadataDirty();
            break;
        case UpdateType::UPDATE_STATIC:
        case UpdateType::UPDATE_UNDEFINED:
        default:
            YRLOG_ERROR("resource view does not support current update operation : {}.", fmt::underlying(type));
            return Status(PARAMETER_ERROR, "not support current update operation");
    }

    return Status::OK();
}

Status ResourceViewActor::UpdateUnitStatus(const std::string &unitID, UnitStatus status)
{
    SnapshotMutationGuard snapshotGuard(*this);
    ASSERT_IF_NULL(view_);
    YRLOG_INFO("update unit({}) status {}", unitID, fmt::underlying(status));
    auto unit = view_->mutable_fragment()->find(unitID);
    if (unit == view_->mutable_fragment()->end()) {
        YRLOG_ERROR("failed to update unit({}) status({}), unit not found.", unitID, fmt::underlying(status));
        return Status(PARAMETER_ERROR, "update resource unit with unknown ID.");
    }
    uint32_t lastStatus = unit->second.status();
    if (lastStatus == static_cast<uint32_t>(UnitStatus::RECOVERING) && status == UnitStatus::NORMAL) {
        PodRecycler(unit->second);
    }
    unit->second.set_status(static_cast<uint32_t>(status));
    view_->set_revision(view_->revision() + 1);

    Modification modification;
    modification.mutable_statuschange()->set_status(static_cast<uint32_t>(status));

    ResourceUnitChange resourceUnitChange;
    resourceUnitChange.set_resourceunitid(unitID);
    *resourceUnitChange.mutable_modification() = modification;
    StoreChange(view_->revision(), resourceUnitChange);
    MarkSnapshotUnitDirty(unitID);
    MarkSnapshotMetadataDirty();
    return Status::OK();
}

Status ResourceViewActor::BindInstanceRuntime(const InstanceInfo &instance, const std::string &expectedRuntimeID)
{
    const auto unitID = instance.unitid().empty() ? instance.functionagentid() : instance.unitid();
    if (!view_ || instance.instanceid().empty() || instance.requestid().empty()
        || unitID.empty() || instance.runtimeid().empty()
        || (!instance.functionagentid().empty() && instance.functionagentid() != unitID)) {
        return Status(ERR_PARAM_INVALID, "runtime binding requires complete deployment identity");
    }
    auto top = view_->mutable_instances()->find(instance.instanceid());
    auto unit = view_->mutable_fragment()->find(unitID);
    if (top == view_->mutable_instances()->end() || unit == view_->mutable_fragment()->end()) {
        return Status(ERR_PARAM_INVALID, "runtime binding allocation no longer exists");
    }
    auto leaf = unit->second.mutable_instances()->find(instance.instanceid());
    const auto matches = [&](const InstanceInfo &allocated) {
        return allocated.instanceid() == instance.instanceid() && allocated.requestid() == instance.requestid()
            && allocated.unitid() == unitID
            && (allocated.runtimeid() == expectedRuntimeID || allocated.runtimeid() == instance.runtimeid());
    };
    if (leaf == unit->second.mutable_instances()->end() || !matches(top->second) || !matches(leaf->second)
        || top->second.runtimeid() != leaf->second.runtimeid()) {
        return Status(ERR_PARAM_INVALID, "runtime binding belongs to a stale allocation or incarnation");
    }
    // Validate both indexes before changing either. This operation changes no
    // resource booking and may not resurrect an instance removed during deploy.
    // A runtime rebind across incarnations must not silently drop the prior
    // incarnation's ledger holds: without a physical-retirement receipt the old
    // generation's credit survives into reconciliation and the rebind refuses.
    for (const auto *allocated : {&top->second, &leaf->second}) {
        if (allocated->has_sandbox_resource_sample()
            && allocated->sandbox_resource_sample().runtime_id() != instance.runtimeid()
            && HasResizeBudgetEntries(instance.instanceid())) {
            return Status(ERR_INSTANCE_BUSY,
                "runtime rebind requires physical retirement of the prior incarnation");
        }
    }
    for (auto *allocated : {&top->second, &leaf->second}) {
        allocated->set_runtimeid(instance.runtimeid());
        // Allocation precedes SetScheduleResult; bind its registered agent route as well.
        allocated->set_functionagentid(unitID);
        if (allocated->has_sandbox_resource_sample()
            && allocated->sandbox_resource_sample().runtime_id() != instance.runtimeid()) {
            allocated->clear_sandbox_resource_sample();
            allocated->clear_actualuse();
            // The retired incarnation's ledger entries (if any) are NOT
            // dropped here: only ApplyPhysicalRetirement may settle them.
        }
    }
    YRLOG_INFO("bind instance runtime instance={} request={} unit={} runtime={}", instance.instanceid(),
               instance.requestid(), unitID, instance.runtimeid());
    return Status::OK();
}

void ResourceViewActor::SimplifyInstanceInfo(const InstanceInfo &instance, InstanceInfo &simplifiedInstance) const
{
    simplifiedInstance.set_instanceid(instance.instanceid());
    simplifiedInstance.set_requestid(instance.requestid());
    simplifiedInstance.set_runtimeid(instance.runtimeid());
    // Preserve only the explicit elastic contract here; admission bookkeeping
    // is reconstructed by the owning view, never accepted from the caller.
    const auto elastic = instance.createoptions().find(SCHEDULER_ELASTIC_RESOURCES);
    if (elastic != instance.createoptions().end()) {
        (*simplifiedInstance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = elastic->second;
    }
    simplifiedInstance.set_resource_admission_pending(UsesElasticAdmission(instance));
    simplifiedInstance.set_runtimeaddress(instance.runtimeaddress());
    simplifiedInstance.set_functionagentid(instance.functionagentid());
    simplifiedInstance.set_unitid(instance.unitid().empty() ? instance.functionagentid() : instance.unitid());
    simplifiedInstance.set_function(instance.function());
    simplifiedInstance.mutable_resources()->CopyFrom(instance.resources());
    simplifiedInstance.mutable_snapshotinfo()->CopyFrom(instance.snapshotinfo());
    simplifiedInstance.mutable_actualuse()->CopyFrom(instance.actualuse());
    simplifiedInstance.mutable_scheduleoption()->CopyFrom(instance.scheduleoption());
    simplifiedInstance.mutable_labels()->CopyFrom(instance.labels());
    simplifiedInstance.mutable_schedulerchain()->CopyFrom(instance.schedulerchain());
    simplifiedInstance.set_starttime(instance.starttime());
    simplifiedInstance.set_storagetype(instance.storagetype());
    simplifiedInstance.set_tenantid(instance.tenantid());
}

Status ResourceViewActor::ValidateInstanceAllocation(const std::string &instanceID,
                                                     const InstanceInfo &instance) const
{
    const auto &selected = instance.unitid();
    const auto unit = view_->fragment().find(selected);
    if (unit == view_->fragment().end() ||
        unit->second.status() != static_cast<uint32_t>(UnitStatus::NORMAL)) {
        YRLOG_WARN("unable to allocate instances({}). the ({}) is unavailable", instanceID, selected);
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR);
    }

    // Ignore zero-valued resources such as nvidia.com/gpu:0 when checking
    // capacity, because a Unit may legitimately omit that resource type.
    auto checkResources = instance.resources();
    for (auto iter = checkResources.mutable_resources()->begin();
         iter != checkResources.mutable_resources()->end();) {
        if (ScalaValueIsEmpty(iter->second)) {
            iter = checkResources.mutable_resources()->erase(iter);
        } else {
            ++iter;
        }
    }
    if (checkResources > unit->second.allocatable()) {
        YRLOG_WARN("unable to allocate instances({}). the ({}) has insufficient resources may caused by "
                   "capacity decrement", instanceID, selected);
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR);
    }
    return Status::OK();
}

// only add in local
// never executed on domain
Status ResourceViewActor::AddInstances(const std::map<std::string, InstanceAllocatedInfo> &insts)
{
    SnapshotMutationGuard snapshotGuard(*this);
    if (!IsValidInstances(insts)) {
        YRLOG_WARN("try to add invalid instances to resource view.");
        return Status(PARAMETER_ERROR, "add invalid instances.");
    }

    ASSERT_IF_NULL(view_);
    YRLOG_INFO("add instances to resource view, instances size = {}.", insts.size());
    if (!insts.empty()) {
        view_->set_revision(view_->revision() + 1);
    }
    for (auto &inst : insts) {
        InstanceInfo simplifyInstance;
        SimplifyInstanceInfo(inst.second.instanceInfo, simplifyInstance);
        if (inst.second.allocatedPromise == nullptr) {
            AddInstance(simplifyInstance);
            MarkSnapshotUnitDirty(simplifyInstance.unitid());
            MarkSnapshotIndexesDirty(true, true);
            MarkSnapshotMonopolyIndexDirty();
            continue;
        }
        auto selected = simplifyInstance.unitid();
        if (view_->fragment().find(selected) == view_->fragment().end()
            || view_->fragment().at(selected).status() != static_cast<uint32_t>(UnitStatus::NORMAL)) {
            YRLOG_WARN("unable to allocate instances({}). the ({}) is unavailable", inst.first, selected);
            inst.second.allocatedPromise->SetValue(Status(StatusCode::ERR_INNER_SYSTEM_ERROR));
            continue;
        }
        // This actor serializes actual allocations across scheduling windows.
        // Recheck restore reservations after earlier allocations became visible.
        const auto &selectedUnit = view_->fragment().at(selected);
        // Cold allocations from independent scheduling windows must also
        // obey the instance cap, even before the first resource sample.
        const auto maxInstances = GetUsageAdmissionSettings().maxInstances;
        if (maxInstances > 0 && selectedUnit.instances().size() >= static_cast<size_t>(maxInstances)) {
            inst.second.allocatedPromise->SetValue(
                Status(RESOURCE_NOT_ENOUGH, "Instances: Node Instance Cap Reached"));
            continue;
        }
        bool restoreAdmission = !simplifyInstance.snapshotinfo().checkpointid().empty()
            || simplifyInstance.resource_admission_pending();
        for (const auto &entry : selectedUnit.instances()) {
            restoreAdmission = restoreAdmission || !entry.second.snapshotinfo().checkpointid().empty()
                || entry.second.resource_admission_pending();
        }
        const auto resizeReserve = OutstandingResize(selected);
        if (restoreAdmission || resizeReserve.cpuMillicores > 0 || resizeReserve.memoryMb > 0) {
            const auto verdict = CheckRestoreAllocation(simplifyInstance, selectedUnit, resizeReserve);
            if (!verdict.IsOk()) {
                inst.second.allocatedPromise->SetValue(verdict);
                continue;
            }
        }
        // Check capacity: skip resources with value 0 (e.g. nvidia.com/gpu:0) to avoid
        // false "insufficient resource" when the node has no such resource type at all.
        auto checkResources = simplifyInstance.resources();
        for (auto it = checkResources.mutable_resources()->begin();
             it != checkResources.mutable_resources()->end();) {
            if (ScalaValueIsEmpty(it->second)) {
                it = checkResources.mutable_resources()->erase(it);
            } else {
                ++it;
            }
        }
        if (checkResources > view_->fragment().at(selected).allocatable()) {
            YRLOG_WARN(
                "unable to allocate instances({}). the ({}) has insufficient resources may caused by capacity "
                "decrement",
                inst.first, selected);
            inst.second.allocatedPromise->SetValue(Status(StatusCode::ERR_INNER_SYSTEM_ERROR));
            continue;
        }
        AddInstance(simplifyInstance);
        MarkSnapshotUnitDirty(simplifyInstance.unitid());
        MarkSnapshotIndexesDirty(true, true);
        MarkSnapshotMonopolyIndexDirty();
        inst.second.allocatedPromise->SetValue(Status::OK());
    }
    if (!insts.empty()) {
        MarkSnapshotMetadataDirty();
    }
    return Status::OK();
}

Status ResourceViewActor::DeleteInstances(const std::vector<std::string> &instIDs, bool isVirtualInstance)
{
    SnapshotMutationGuard snapshotGuard(*this);
    if (instIDs.empty()) {
        YRLOG_WARN("Instance ids is empty, deletion failed");
        return Status(PARAMETER_ERROR, "instance ids is empty, deletion failed");
    }
    ASSERT_IF_NULL(view_);
    for (auto &id : instIDs) {
        if (!IsInstanceInResourceView(id)) {
            YRLOG_ERROR("failed to delete instance({}) in resource unit, not found", id);
            return Status(PARAMETER_ERROR, "failed to delete instance in resource unit, not found " + id);
        }
        // Final guardrail for every generic deletion path: an elastic
        // incarnation that reached the physical layer, or owns any ledger
        // entry, may only leave through ApplyPhysicalRetirement with a
        // sandboxd receipt. Missing call sites fail closed here instead of
        // releasing credit early.
        if (RequiresPhysicalRetirement(view_->instances().at(id))) {
            YRLOG_WARN("generic deletion refused for elastic instance({}); physical retirement receipt required",
                       id);
            return Status(ERR_INSTANCE_BUSY,
                "elastic instance " + id + " requires a physical retirement receipt before deletion");
        }
    }
    view_->set_revision(view_->revision() + 1);
    for (auto &id : instIDs) {
        DeleteInstance(id, isVirtualInstance);
        MarkSnapshotUnitDirty(unitID);
        MarkSnapshotIndexesDirty(true, true);
        MarkSnapshotMonopolyIndexDirty();
    }
    MarkSnapshotMetadataDirty();
    return Status::OK();
}

std::shared_ptr<ResourceUnitChanges> ResourceViewActor::GetResourceViewChanges()
{
    if (view_ == nullptr) {
        return {};
    }

    ResourceUnitChanges changes;
    MergeResourceViewChanges(lastReportedRevision_, view_->revision(), changes);
    lastReportedRevision_ = view_->revision();
    changes.set_localviewinittime(view_->viewinittime());

    auto result = std::make_shared<ResourceUnitChanges>(std::move(changes));
    return result;
}

std::shared_ptr<ResourceUnit> ResourceViewActor::GetResourceViewCopy()
{
    FlushScheduleSnapshot();
    if (view_ == nullptr) {
        return {};
    }
    auto view = std::make_shared<ResourceUnit>(*view_);
    for (auto &[id, frag] : *view->mutable_fragment()) {
        [[maybe_unused]] const auto& unused = id;
        frag.clear_instances();
        frag.clear_bucketindexs();
    }
    return view;
}

std::shared_ptr<ResourceUnit> ResourceViewActor::GetFullResourceView()
{
    if (view_ == nullptr) {
        return {};
    }
    auto view = std::make_shared<ResourceUnit>(*view_);
    return view;
}

void ResourceViewActor::UpdateDomainUrlForLocal(const std::string &addr)
{
    SnapshotMutationGuard snapshotGuard(*this);
    if (domainUrlForLocal_ == addr) {
        YRLOG_DEBUG("Local received a matching domain URL({}) update", addr);
        return;
    }
    // Excluding the first initialization: Potential domain switch detected.
    // To maintain resource view consistency, change viewInitTime for a full update.
    if (!domainUrlForLocal_.empty()) {
        litebus::uuid_generator::UUID uuid = litebus::uuid_generator::UUID::GetRandomUUID();
        view_->set_viewinittime(uuid.ToString());
        MarkSnapshotMetadataDirty();
        YRLOG_INFO("Potential domain switch detected, new viewInitTime is {}", view_->viewinittime());
    }
    domainUrlForLocal_ = addr;
    YRLOG_INFO("Local updates the domain URL to {}", addr);
}

void ResourceViewActor::UpdateIsHeader(bool isHeader)
{
    SnapshotMutationGuard snapshotGuard(*this);
    isHeader_ = isHeader;
    MarkSnapshotMetadataDirty();
}

ResourceViewInfo ResourceViewActor::GetResourceInfo()
{
    if (view_ == nullptr) {
        return ResourceViewInfo{};
    }
    using LableProtoMap = ::google::protobuf::Map<std::string, ValueCounter>;
    return ResourceViewInfo{
        *view_, GetCurrentSchedulerLevel(), reqIDToUnitIDMap_,
        isLocal_ ? std::unordered_map<std::string, LableProtoMap>{ { view_->id(), view_->nodelabels() } }
                 : allLocalLabels_
    };
}

std::shared_ptr<ResourceUnit> ResourceViewActor::GetResourceView()
{
    return view_;
}

std::string ResourceViewActor::GetSerializedResourceView()
{
    ASSERT_IF_NULL(view_);
    if (getResourceViewCount_ % DEFAULT_PRINT_RESOURCE_VIEW_TIMER_COUNT == 0) {
        YRLOG_INFO("timer print resource view id:{} capacity:{} allocatable:{} instance num:{}", view_->id(),
                   ToString(view_->capacity()), ToString(view_->allocatable()), view_->instances().size());
    }
    getResourceViewCount_++;
    return view_->SerializeAsString();
}

litebus::Option<ResourceUnit> ResourceViewActor::GetResourceUnit(const std::string &unitID)
{
    ASSERT_IF_NULL(view_);
    auto it = view_->fragment().find(unitID);
    if (it == view_->fragment().end()) {
        YRLOG_WARN("try to get resource unit by invalid id, id = {}.", unitID);
        return litebus::None();
    }
    YRLOG_INFO("get resource unit id {}.", unitID);
    return it->second;
}

litebus::Option<std::string> ResourceViewActor::GetUnitByInstReqID(const std::string &instReqID)
{
    auto iter = reqIDToUnitIDMap_.find(instReqID);
    if (iter == reqIDToUnitIDMap_.end()) {
        return litebus::None();
    }
    return iter->second;
}

void ResourceViewActor::ClearResourceView()
{
    SnapshotMutationGuard snapshotGuard(*this);
    view_.reset(new ResourceUnit());
    resourceAdmissionTimes_.clear();
    reqIDToUnitIDMap_.clear();
    MarkSnapshotStructureDirty("");
    MarkSnapshotIndexesDirty(true, true);
    MarkSnapshotMonopolyIndexDirty();
    MarkSnapshotMetadataDirty();
}

void ResourceViewActor::AddResourceUpdateHandler(const ResourceUpdateHandler &handler)
{
    YRLOG_INFO("add a update handler to resource view.");
    updateHandler_.push_back(handler);
}

void ResourceViewActor::UpdateTime()
{
    litebus::uuid_generator::UUID uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    updateTime_ = uuid.ToString();
}

void ResourceViewActor::OnUpdate()
{
    YRLOG_DEBUG("resource view update , update time = {}", updateTime_);
    for (const auto &handler : updateHandler_) {
        handler();
    }
}

bool ResourceViewActor::IsInstanceInResourceView(const std::string &instID)
{
    ASSERT_IF_NULL(view_);
    if (view_->instances().find(instID) == view_->instances().end()) {
        return false;
    }

    auto instance = view_->mutable_instances()->at(instID);
    auto agentFragmentIter = view_->mutable_fragment()->find(instance.unitid());
    if (agentFragmentIter == view_->mutable_fragment()->end()) {
        return false;
    }

    if (agentFragmentIter->second.instances().find(instance.instanceid()) ==
        agentFragmentIter->second.instances().end()) {
        return false;
    }

    return true;
}

bool ResourceViewActor::IsValidUnit(const ResourceUnit &unit) const
{
    if (unit.id().empty() || !unit.has_capacity() || !unit.has_allocatable() || !IsValid(unit.capacity()) ||
        !IsValid(unit.allocatable())) {
        return false;
    }
    return true;
}

bool ResourceViewActor::IsValidInstance(const InstanceInfo &instance)
{
    if (instance.instanceid().empty() || !instance.has_resources() || !IsValid(instance.resources()) ||
        instance.unitid().empty()) {
        YRLOG_ERROR("instance has invalid id or resources ...");
        return false;
    }

    ASSERT_IF_NULL(view_);
    if (view_->instances().find(instance.instanceid()) != view_->instances().end()) {
        YRLOG_ERROR("has duplicate instance {} in local resource view.", instance.instanceid());
        return false;
    }

    auto agentFragmentIter = view_->mutable_fragment()->find(instance.unitid());
    if (agentFragmentIter == view_->mutable_fragment()->end()) {
        YRLOG_WARN("resource view does not have a agent unit with ID {}.", instance.unitid());
        return false;
    }

    if (agentFragmentIter->second.instances().find(instance.instanceid()) !=
        agentFragmentIter->second.instances().end()) {
        YRLOG_ERROR("has duplicate instance {} in agent resource view.", instance.instanceid());
        return false;
    }

    return true;
}

bool ResourceViewActor::IsValidInstances(const std::map<std::string, InstanceAllocatedInfo> &instances)
{
    if (instances.size() == 0) {
        YRLOG_INFO("invalid instances, size = 0.");
        return false;
    }

    for (auto &inst : instances) {
        if ((inst.first != inst.second.instanceInfo.instanceid()) || !IsValidInstance(inst.second.instanceInfo)) {
            if (inst.second.allocatedPromise != nullptr) {
                inst.second.allocatedPromise->SetValue(Status(StatusCode::ERR_INNER_SYSTEM_ERROR));
            }
            return false;
        }
    }
    return true;
}

void ResourceViewActor::AddLabel(
    const InstanceInfo &instance, ::google::protobuf::Map<std::string, ValueCounter> &nodeLabels,
    bool enableTenantAffinity)
{
    for (const auto &label : instance.labels()) {
        resources::Value::Counter cnter;
        (void)cnter.mutable_items()->insert({ label, 1 });
        if (nodeLabels.find(AFFINITY_SCHEDULE_LABELS) != nodeLabels.end()) {
            nodeLabels[AFFINITY_SCHEDULE_LABELS] = nodeLabels[AFFINITY_SCHEDULE_LABELS] + cnter;
        } else {
            (void)nodeLabels.insert({ AFFINITY_SCHEDULE_LABELS, cnter });
        }
        auto kv = ToLabelKV(label);
        if (enableTenantAffinity && kv.count(TENANT_ID) != 0) {
            auto tenantKv = nodeLabels.find(TENANT_ID);
            if (tenantKv == nodeLabels.end() ||
                tenantKv->second.items().count(kv[TENANT_ID].items().begin()->first) == 0) {
                YRLOG_INFO("first time to ADD LABEL: {}", label);
                // labels are added twice to ensure that the labels are not cleared after instance clearing.
                nodeLabels = nodeLabels + kv;
            }
        }
        nodeLabels = nodeLabels + kv;
    }
    for (const auto &[key, value] : instance.kvlabels()) {
        resources::Value::Counter defaultCnt;
        (void)defaultCnt.mutable_items()->insert({ value, 1 });
        MapCounter result;
        result[key] = defaultCnt;
        nodeLabels = nodeLabels + result;
    }
    return;
}

void ResourceViewActor::AddLabels(const InstanceInfo &instance)
{
    ASSERT_IF_NULL(view_);
    auto nodeLabels = view_->mutable_nodelabels();
    AddLabel(instance, *nodeLabels, enableTenantAffinity_);
    auto fragment = view_->mutable_fragment();
    if (auto iter = fragment->find(instance.unitid()); iter != fragment->end()) {
        AddLabel(instance, *iter->second.mutable_nodelabels(), enableTenantAffinity_);
    }
}

void ResourceViewActor::StoreChange(int64_t revision, const ResourceUnitChange& change)
{
    if (versionChanges_.find(revision) == versionChanges_.end()) {
        versionChanges_[revision] = change;
        return;
    }
    auto mergeChange = MergeResourceUnitChanges(versionChanges_[revision], change, enableTenantAffinity_);
    if (IsResourceUnitChangeEmpty(mergeChange)) {
        versionChanges_.erase(revision);
    } else {
        versionChanges_[revision] = mergeChange;
    }
}

inline void ResourceViewActor::CancelAgentReuseTimer(const std::string &functionAgentID)
{
    if (reuseTimers_.count(functionAgentID) > 0) {
        YRLOG_DEBUG("cancel timer of disable agent({})", functionAgentID);
        litebus::TimerTools::Cancel(reuseTimers_[functionAgentID]);
    }
}

inline void ResourceViewActor::DisableAgent(const std::string &functionAgentID)
{
    // double check
    if (agentCacheMap_.count(functionAgentID) != 0 && agentCacheMap_[functionAgentID].size() > 0) {
        YRLOG_WARN("functionAgentID({}) instances not empty", functionAgentID);
        CancelAgentReuseTimer(functionAgentID);
        return;
    }

    YRLOG_INFO("Disable functionAgent({})!", functionAgentID);
    (void)UpdateUnitStatus(functionAgentID, UnitStatus::TO_BE_DELETED);
    (void)DeleteResourceUnit(functionAgentID);
    if (disableExecFunc_) {
        disableExecFunc_(functionAgentID);
    }
    reuseTimers_.erase(functionAgentID);
    agentUsedMap_.erase(functionAgentID);
}

inline void ResourceViewActor::ClearAgentTenantLabels(const std::string &functionAgentID)
{
    ASSERT_IF_NULL(view_);
    auto nodeLabels = view_->mutable_nodelabels();
    auto tenantKv = nodeLabels->find(TENANT_ID);
    if (tenantKv != nodeLabels->end()) {
        YRLOG_INFO("Clear functionAgent({}) tenant labels", functionAgentID);
        (void)nodeLabels->erase(TENANT_ID);
    }
}

inline void ResourceViewActor::SetAgentReuseTimer(const std::string &functionAgentID, int32_t recycleTime)
{
    // Cancel the timer if it already exists
    CancelAgentReuseTimer(functionAgentID);

    YRLOG_DEBUG("set timer to disable agent({}) in {}s", functionAgentID, recycleTime);
    reuseTimers_[functionAgentID] = litebus::AsyncAfter(
        recycleTime * litebus::SECTOMILLI, GetAID(), &ResourceViewActor::DisableAgent, functionAgentID);
}

inline void ResourceViewActor::OnTenantInstanceInAgentAllDeleted(const std::string &functionAgentID,
                                                                 int32_t recycleTime)
{
    // be careful, idlePod recycle and tenantAffinity use this function together,
    // make sure that recycleTime > 0 in idlePod recycle scene
    if (recycleTime == 0) {
        YRLOG_INFO("Disable the agent({}) immediately.", functionAgentID);
        DisableAgent(functionAgentID);
    } else if (recycleTime > 0) {
        YRLOG_DEBUG("wait to disable agent({}) in {}s", functionAgentID, tenantPodReuseTimeWindow_);
        SetAgentReuseTimer(functionAgentID, recycleTime);
    } else if (recycleTime == -1) {
        ClearAgentTenantLabels(functionAgentID);
    } else {
        YRLOG_ERROR("Invalid tenantPodReuseTimeWindow({})", tenantPodReuseTimeWindow_);
    }
}

inline void ResourceViewActor::AddInstanceAgentCache(const InstanceInfo &instInfo, bool forceAdd)
{
    // resource view tenant cache not care about system tenant(0)
    const auto &tenantID = instInfo.tenantid();
    // if forceAdd is true, mean that is in idle pod recycle, don't need to return;
    // else is in tenant pod recycle, should judge tenantID whether to be Added
    if (!forceAdd && (tenantID.empty() || instInfo.issystemfunc())) {
        return;
    }

    // Resource view only focuses on local agent events
    const auto &functionAgentID = instInfo.unitid();
    const auto &instanceID = instInfo.instanceid();
    YRLOG_DEBUG("resource view receive add instance event functionAgentID({})/instanceID({})", functionAgentID,
                instanceID);
    agentCacheMap_[functionAgentID].insert(instanceID);
    CancelAgentReuseTimer(functionAgentID);
}

void ResourceViewActor::AddInstance(const InstanceInfo &instance)
{
    ASSERT_IF_NULL(view_);
    YRLOG_INFO("add instance {} to resource view named {}, current revision = {}",
               instance.instanceid(), instance.unitid(), view_->revision());
    InstanceChange instanceChange;
    instanceChange.set_changetype(InstanceChange::ADD);
    instanceChange.set_instanceid(instance.instanceid());
    instanceChange.mutable_instance()->CopyFrom(instance);

    Modification modification;
    *modification.add_instancechanges() = instanceChange;

    ResourceUnitChange resourceUnitChange;
    resourceUnitChange.set_resourceunitid(instance.unitid());
    *resourceUnitChange.mutable_modification() = modification;
    StoreChange(view_->revision(), resourceUnitChange);

    AddInstanceToView(instance);
    if (!instance.snapshotinfo().checkpointid().empty() || instance.resource_admission_pending()) {
        resourceAdmissionTimes_[instance.instanceid()] = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }


    const auto &functionAgentID = instance.unitid();
    auto agent = view_->mutable_fragment()->find(functionAgentID);
    if (agent != view_->mutable_fragment()->end()) {
        auto recycleTime = ParseRecyclePodLabel(agent->second);
        if (recycleTime == -1) { // pod not need to recycled
            return;
        }
        if (recycleTime > 0) {
            AddInstanceAgentCache(instance, true);
            return;
        }
    }
    if (enableTenantAffinity_) {
        // update tenant cache
        AddInstanceAgentCache(instance, false);
    }
}

Resources ResourceViewActor::AddInstanceToAgentView(const InstanceInfo &instance, resources::ResourceUnit &unit,
                                                    bool enableTenantAffinity)
{
    auto nodeLabels = unit.mutable_nodelabels();
    AddLabel(instance, *nodeLabels, enableTenantAffinity);
    // while monopolized schedule, the allocatable of selected minimum unit(function agent)
    // should be substracted to zero
    auto substraction = instance.resources();
    if (instance.scheduleoption().schedpolicyname() == MONOPOLY_SCHEDULE) {
        substraction = unit.allocatable();
    }
    (*unit.mutable_allocatable()) = unit.allocatable() - substraction;
    // add instance to agent resourceunit
    (void)unit.mutable_instances()->insert({ instance.instanceid(), instance });
    return substraction;
};

void ResourceViewActor::AddInstanceToView(const InstanceInfo &instance)
{
    ASSERT_IF_NULL(view_);
    auto nodeLabels = view_->mutable_nodelabels();
    AddLabel(instance, *nodeLabels, enableTenantAffinity_);

    auto agentId = instance.unitid();
    auto agentFragmentIter = view_->mutable_fragment()->find(agentId);
    if (agentFragmentIter == view_->mutable_fragment()->end()) {
        YRLOG_WARN("resource view does not have a agent unit with ID {}.", agentId);
        return;
    }
    auto &agentResourceUnit = agentFragmentIter->second;
    auto substraction = AddInstanceToAgentView(instance, agentResourceUnit, enableTenantAffinity_);

    (*view_->mutable_allocatable()) = view_->allocatable() - substraction;
    // add instance to top level resourceunit
    (void)view_->mutable_instances()->insert({ instance.instanceid(), instance });

    UpdateBucketInfoAddInstance(instance, agentResourceUnit.capacity(), agentResourceUnit.instances_size(), *view_);
    UpdateBucketInfoAddInstance(instance, agentResourceUnit.capacity(), agentResourceUnit.instances_size(),
                                agentResourceUnit);

    if (!instance.requestid().empty()) {
        (void)reqIDToUnitIDMap_.emplace(instance.requestid(), agentId);
        MarkSnapshotRequestMutation(instance.requestid());
    }
}

inline void ResourceViewActor::DeleteInstanceAgentCache(const InstanceInfo &instInfo, bool needToRecycle,
                                                        int32_t recycleTime, bool forceDelete)
{
    const auto &tenantID = instInfo.tenantid();
    // if forceDelete is true, mean that is in idle pod recycle, don't need to return;
    // else is in tenant pod recycle, should judge tenantID whether to be delete
    if (!forceDelete && (tenantID.empty() || instInfo.issystemfunc())) {
        return;
    }

    const auto &functionAgentID = instInfo.unitid();
    const auto &instanceID = instInfo.instanceid();
    if (agentCacheMap_.count(functionAgentID)) {
        YRLOG_DEBUG("resource view receive delete instance event functionAgentID({})/instanceID({})",
            functionAgentID, instanceID);
        (void)agentCacheMap_[functionAgentID].erase(instanceID);

        if (agentCacheMap_[functionAgentID].empty() && needToRecycle) {
            OnTenantInstanceInAgentAllDeleted(functionAgentID, recycleTime);

            (void)agentCacheMap_.erase(functionAgentID);
            YRLOG_DEBUG("Clear cache entry: functionAgentID({})", functionAgentID);
        }
    }
}

void ResourceViewActor::DeleteInstance(const std::string &instID, bool isVirtualInstance)
{
    ASSERT_IF_NULL(view_);
    // delete instance info in top unit
    if (view_->instances().find(instID) == view_->instances().end()) {
        YRLOG_ERROR("failed to delete instance({}) in resource unit, not found", instID);
        return;
    }

    auto instInfo = view_->mutable_instances()->at(instID);
    YRLOG_INFO("delete instance {} from resource unit named {}, current revision = {}",
               instID, instInfo.unitid(), view_->revision());

    InstanceChange instanceChange;
    instanceChange.set_changetype(InstanceChange::DELETE);
    instanceChange.set_instanceid(instID);
    instanceChange.mutable_instance()->CopyFrom(instInfo);

    Modification modification;
    *modification.add_instancechanges() = instanceChange;

    ResourceUnitChange resourceUnitChange;
    resourceUnitChange.set_resourceunitid(instInfo.unitid());
    *resourceUnitChange.mutable_modification() = modification;
    StoreChange(view_->revision(), resourceUnitChange);

    (void)DeleteInstanceFromView(instInfo);
    auto iter = view_->fragment().find(instInfo.unitid());
    if (iter == view_->fragment().end()) {
        YRLOG_WARN("{}|{}| can not find {} in resource view, pod may be recycle", instInfo.requestid(),
                   instInfo.instanceid(), instInfo.unitid());
        return;
    }
    auto recycleTime = ParseRecyclePodLabel(iter->second);
    YRLOG_DEBUG("set pod {} recycler, recycle time: {}", iter->second.id(), recycleTime);
    if (recycleTime == -1) {  // pod not need to recycled
        return;
    }
    if (recycleTime > 0) {
        DeleteInstanceAgentCache(instInfo, true, recycleTime, true);
        return;
    }
    if (enableTenantAffinity_) {
        // isVirtualInstance is false, must be recycled
        // isVirtualInstance is true but pod is used before, must be recycled
        if (!isVirtualInstance) {
            agentUsedMap_[instInfo.unitid()] = true;
        }
        auto needToRecycle = agentUsedMap_.find(instInfo.unitid()) != agentUsedMap_.end();
        DeleteInstanceAgentCache(instInfo, needToRecycle, tenantPodReuseTimeWindow_, false);
    }
}

void ResourceViewActor::DeleteLabels(const InstanceInfo &instInfo)
{
    ASSERT_IF_NULL(view_);
    auto nodeLabels = view_->mutable_nodelabels();
    DeleteLabel(instInfo, *nodeLabels);
    auto fragment = view_->mutable_fragment();
    if (auto iter = fragment->find(instInfo.unitid()); iter != fragment->end()) {
        DeleteLabel(instInfo, *iter->second.mutable_nodelabels());
    }
}

void ResourceViewActor::DeleteInstanceFromView(const InstanceInfo &instance)
{
    resourceAdmissionTimes_.erase(instance.instanceid());
    // Ledger entries (baselines included) never follow a generic logical
    // deletion: only a matching physical-retirement receipt may settle them.
    ASSERT_IF_NULL(view_);
    auto nodeLabels = view_->mutable_nodelabels();
    DeleteLabel(instance, *nodeLabels);

    auto agentId = instance.unitid();
    auto agentFragmentIter = view_->mutable_fragment()->find(agentId);
    if (agentFragmentIter == view_->mutable_fragment()->end()) {
        YRLOG_WARN("domain resource view does not have a resource unit with ID {}.", agentId);
        return;
    }
    auto &agentResourceUnit = agentFragmentIter->second;
    auto addend = DeleteInstanceFromAgentView(instance, agentResourceUnit);

    (*view_->mutable_allocatable()) = view_->allocatable() + addend;

    UpdateBucketInfoDelInstance(instance, agentResourceUnit.capacity(), agentResourceUnit.instances_size(), *view_);
    UpdateBucketInfoDelInstance(instance, agentResourceUnit.capacity(), agentResourceUnit.instances_size(),
                                agentResourceUnit);
    (void)view_->mutable_instances()->erase(instance.instanceid());
    if (!instance.requestid().empty()) {
        MarkSnapshotRequestMutation(instance.requestid());
        (void)reqIDToUnitIDMap_.erase(instance.requestid());
    }
}

void ResourceViewActor::UpdateResourceUnitDynamic(const std::shared_ptr<ResourceUnit> &value)
{
    ASSERT_IF_NULL(view_);
    auto unit = view_->mutable_fragment()->find(value->id());
    if (unit == view_->mutable_fragment()->end()) {
        YRLOG_ERROR("resource view does not have a resource unit with ID {}.", value->id());
        return;
    }
    if (value->capacity() == unit->second.capacity()) {
        return;
    }
    YRLOG_DEBUG("for test: Unit({}) current:{} allocatable:{} received:{}", value->id(),
                unit->second.capacity().ShortDebugString(), unit->second.allocatable().ShortDebugString(),
                value->capacity().ShortDebugString());
    /*
        unit capacity : 上一次的容量
        value capacity: 本次更新的容量
        delta: 两次容量的差值
        capacityChange.increment(): 本次容量是否上升
    */
    Resources retainedCapacity = unit->second.capacity();
    auto removedResources = CollectRemovedResources(unit->second, value->capacity(), retainedCapacity);
    if (!removedResources.capacity().resources().empty()) {
        DeleteResourceBySubUnit(*view_, removedResources);
    }

    // cap decreased
    CapacityChange capacityChange;
    auto delta = capacityChange.mutable_delta();
    delta->CopyFrom(value->capacity() - retainedCapacity);
    for (const auto &[resourceName, resource] : removedResources.capacity().resources()) {
        (void)resource;
        capacityChange.add_removedresourcenames(resourceName);
    }
    YRLOG_DEBUG("for test: Unit({}) delta:{}", value->id(), delta->ShortDebugString());

    (*view_->mutable_capacity()) = view_->capacity() + *delta;

    // maybe negative allocatable
    // todo: while negative allocatable, need to trigger some alert event to evict/migrate instances
    (*view_->mutable_allocatable()) = view_->allocatable() + *delta;

    // update in fragment
    *unit->second.mutable_capacity() = value->capacity();
    *unit->second.mutable_allocatable() = unit->second.allocatable() + *delta;
    for (const auto &[resourceName, resource] : removedResources.capacity().resources()) {
        (void)resource;
        unit->second.mutable_allocatable()->mutable_resources()->erase(resourceName);
    }
    PruneRemovedResources(*view_, removedResources);

    // make change record
    Modification modification;
    *modification.mutable_capacitychange() = std::move(capacityChange);
    ResourceUnitChange resourceUnitChange;
    resourceUnitChange.set_resourceunitid(value->id());
    *resourceUnitChange.mutable_modification() = std::move(modification);
    view_->set_revision(view_->revision() + 1);
    StoreChange(view_->revision(), resourceUnitChange);
}

bool ResourceViewActor::SampleUpdateAllowed(const InstanceInfo &allocated, const InstanceInfo &reported) const
{
    if (!reported.has_sandbox_resource_sample()) { return true; }
    const auto &incoming = reported.sandbox_resource_sample();
    // Samples from revision-incapable sources (old daemons, pre-contract
    // incarnations) must not change stored state while the feature is on.
    if (!incoming.resource_revision_known()) {
        YRLOG_DEBUG("resource sample without revision rejected instance={}", allocated.instanceid());
        return false;
    }
    if (!allocated.has_sandbox_resource_sample()
        || !allocated.sandbox_resource_sample().resource_revision_known()) {
        // Recovery window: the first authoritative reading establishes the
        // per-instance revision baseline.
        return true;
    }
    const auto &stored = allocated.sandbox_resource_sample();
    if (stored.resource_generation() != incoming.resource_generation()) {
        YRLOG_WARN("resource sample generation conflict instance={} stored={} incoming={}",
                   allocated.instanceid(), stored.resource_generation(), incoming.resource_generation());
        return false;
    }
    if (incoming.resource_revision() < stored.resource_revision()) {
        YRLOG_DEBUG("stale resource sample rejected instance={} revision={} confirmed={}",
                    allocated.instanceid(), incoming.resource_revision(), stored.resource_revision());
        return false;
    }
    if (incoming.resource_revision() == stored.resource_revision()) {
        // Limits are fixed within a revision, and equality is only provable
        // from the explicit known flags, never inferred from values. A
        // contradiction keeps the confirmed value and asks for reconciliation
        // instead of overwriting; operation-id/pending metadata may
        // legitimately evolve in place.
        if (!incoming.cpu_limits_known() || !incoming.memory_limit_known()) {
            YRLOG_DEBUG("resource sample with unverifiable limits rejected instance={} revision={}",
                        allocated.instanceid(), stored.resource_revision());
            return false;
        }
        if (incoming.cpu_quota_usec() != stored.cpu_quota_usec()
            || incoming.cpu_period_usec() != stored.cpu_period_usec()
            || incoming.memory_limit_bytes() != stored.memory_limit_bytes()) {
            YRLOG_WARN("contradictory limits at revision {} instance={} triggers reconcile",
                       stored.resource_revision(), allocated.instanceid());
            return false;
        }
        return true;
    }
    // A newer revision is only authoritative when it maps to an operation this
    // view dispatched for the same incarnation; unauthorized telemetry cannot
    // establish a revision on its own.
    if (!MapsToDispatchedResize(allocated.instanceid(), incoming.resource_generation(),
        incoming.resource_revision())) {
        YRLOG_WARN("resource revision {} without a dispatched operation instance={}",
                   incoming.resource_revision(), allocated.instanceid());
        return false;
    }
    return true;
}

void ResourceViewActor::UpdateResourceUnitActual(const std::shared_ptr<ResourceUnit> &value)
{
    ASSERT_IF_NULL(view_);
    auto unit = view_->mutable_fragment()->find(value->id());
    if (value->memory_sample_unix_ms() > 0
        && unit->second.memory_sample_unix_ms() > value->memory_sample_unix_ms()) {
        return; // delayed resource reports must not roll back node usage
    }
    // update top level actual use
    if (view_->has_actualuse()) {
        if (!IsValid(view_->actualuse() - unit->second.actualuse())) {
            YRLOG_DEBUG_COUNT_60("Unit({}) actual use exceeds view value", value->id());
            return;
        }
        // For Set and Vectors, old data must be deleted before new data can be added.
        // For example: {A, B, C} - {B, C} + {B, C} = {A, B, C}, but {A, B, C} + {B, C} - {B, C} = {A}
        (*view_->mutable_actualuse()) = view_->actualuse() - unit->second.actualuse() + value->actualuse();
    } else {
        (*view_->mutable_actualuse()) = value->actualuse();
    }

    // update fragment
    *unit->second.mutable_actualuse() = std::move(*value->mutable_actualuse());
    unit->second.set_memory_sample_unix_ms(value->memory_sample_unix_ms());
    unit->second.set_cpu_sample_unix_ms(value->cpu_sample_unix_ms());

    // D-7: merge the reported per-instance actualuse into the view. The
    // collectors already report it (BuildResourceUnitWithInstance), but
    // dropping it here left every view instance at useMb=0, which disabled
    // the pressure-park victim's "reclaim the most" tiebreak. Only instances
    // already tracked by the view are refreshed -- a lagging report must not
    // resurrect a deleted instance. Both stores are updated: the unit fragment
    // and the view-level instance index (consumers like the pressure monitor
    // read the latter).
    for (const auto &reportedIt : value->instances()) {
        auto viewInst = unit->second.mutable_instances()->find(reportedIt.first);
        if (viewInst == unit->second.mutable_instances()->end()) {
            continue;
        }
        const auto &reported = reportedIt.second;
        if (reported.has_sandbox_resource_sample()
            && !viewInst->second.runtimeid().empty()
            && viewInst->second.runtimeid() != reported.sandbox_resource_sample().runtime_id()) {
            continue;
        }
        if (reported.has_sandbox_resource_sample() && viewInst->second.has_sandbox_resource_sample()
            && reported.sandbox_resource_sample().sampled_at_unix_ms()
                <= viewInst->second.sandbox_resource_sample().sampled_at_unix_ms()) {
            continue;
        }
        if (MemoryElasticityFenceEnabled() && !SampleUpdateAllowed(viewInst->second, reported)) {
            // Revision/generation fenced telemetry must not change the
            // confirmed state of this incarnation in either index.
            continue;
        }
        (*viewInst->second.mutable_actualuse()) = reported.actualuse();
        if (reported.has_sandbox_resource_sample()) {
            viewInst->second.mutable_sandbox_resource_sample()->CopyFrom(reported.sandbox_resource_sample());
        } else if (!MemoryElasticityFenceEnabled() || !viewInst->second.has_sandbox_resource_sample()
            || !viewInst->second.sandbox_resource_sample().resource_revision_known()) {
            viewInst->second.clear_sandbox_resource_sample();
        }
        const auto observationNowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        if ((!viewInst->second.snapshotinfo().checkpointid().empty() || viewInst->second.resource_admission_pending())
            && resourceAdmissionTimes_.find(reportedIt.first) == resourceAdmissionTimes_.end()) {
            // A recovered view must receive a new physical sample before
            // dropping a reservation whose original admission time is unknown.
            resourceAdmissionTimes_[reportedIt.first] = observationNowMs;
        }
        const auto admitted = resourceAdmissionTimes_.find(reportedIt.first);
        if (admitted != resourceAdmissionTimes_.end() && reported.has_sandbox_resource_sample()) {
            const auto &sample = reported.sandbox_resource_sample();
            const auto cpuObservedMs = UsableNodeCPUSampleTime(*value, observationNowMs, DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS);
            const auto memoryObservedMs = UsableNodeMemorySampleTime(*value, observationNowMs, DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS);
            if (!sample.resource_generation().empty() && !sample.runtime_id().empty()
                && viewInst->second.runtimeid() == sample.runtime_id()
                && reported.runtimeid() == sample.runtime_id() && !sample.sandbox_id().empty()
                && sample.cpu_usage_known() && std::isfinite(sample.cpu_usage_cores()) && sample.cpu_usage_cores() >= 0
                && sample.sampled_at_unix_ms() > admitted->second
                && observationNowMs - sample.sampled_at_unix_ms() <= DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS
                && cpuObservedMs > admitted->second
                && memoryObservedMs >= sample.sampled_at_unix_ms()) {
                // Physical Stats and both node CPU/Memory windows postdate admission.
                // Release the temporary restore ceiling, not the normal booking.
                viewInst->second.clear_snapshotinfo();
                viewInst->second.set_resource_admission_pending(false);
                resourceAdmissionTimes_.erase(admitted);
                if (MemoryElasticityFenceEnabled()) {
                    // Restart recovery: the grow ledger is gone, so hold the
                    // full current max as a conservative commitment instead of
                    // re-issuing the headroom to other instances.
                    EstablishRecoveryBaseline(value->id(), viewInst->second, sample);
                }
            }
        }
        auto topInst = view_->mutable_instances()->find(reportedIt.first);
        if (topInst != view_->mutable_instances()->end() && topInst->second.unitid() == value->id()) {
            (*topInst->second.mutable_actualuse()) = viewInst->second.actualuse();
            topInst->second.mutable_snapshotinfo()->CopyFrom(viewInst->second.snapshotinfo());
            topInst->second.set_resource_admission_pending(viewInst->second.resource_admission_pending());
            if (viewInst->second.has_sandbox_resource_sample()) {
                topInst->second.mutable_sandbox_resource_sample()->CopyFrom(viewInst->second.sandbox_resource_sample());
            } else {
                topInst->second.clear_sandbox_resource_sample();
            }
        }
    }

    ReconcileResize(unit->second);

    // D-form admission input: feed the per-unit memory trend so the
    // usage-aware filter can predict bursts instead of chasing last sample
    // The incoming protobuf was moved into the fragment above; depending on
    // arena ownership it may now contain the old fragment rather than the report.
    const auto &actualResources = unit->second.actualuse().resources();
    auto memIt = actualResources.find(MEMORY_RESOURCE_NAME);
    if (memIt != actualResources.end()) {
        UsageTrendTracker::Record(value->id(), memIt->second.scalar().value());
    }
}

void ResourceViewActor::PrintResourceView()
{
    ASSERT_IF_NULL(view_);
    std::ostringstream ss;
    ss << "[id:" << view_->id();
    ss << "|total_inst:" << view_->instances_size() << "]";
    ss << "fragments:[";
    for (auto frag : view_->fragment()) {
        ss << "[fragID:" << frag.second.id();
        ss << " instNum:" << frag.second.instances_size();
        ss << " instRequest:{";
        for (auto inst : frag.second.instances()) {
            ss << inst.second.requestid() << ",";
        }
        ss << "}]";
    }
    YRLOG_DEBUG("current resource view: {}", ss.str());
}

void ResourceViewActor::PullResource(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    if (!IsReady()) {
        YRLOG_WARN("ResourceView is not ready, ignore pull resource");
        return;
    }
    if (domainUrlForLocal_ != from.Url()) {
        YRLOG_WARN("Received a resource update request from an illegal domain. "
                   "Current Domain URL is {}. The illegal domain name is {} and the URL is {}.",
                   domainUrlForLocal_, from.Name(), from.Url());
        return;
    }
    PullResourceRequest pullRequest;
    if (!pullRequest.ParseFromString(msg)) {
        YRLOG_WARN("invalid PullResource request, empty msg or invalid format {}", msg);
        return;
    }
    ASSERT_IF_NULL(view_);
    // lastReportedRevision_ is only used for obtaining incremental updates for scheduling requests
    lastReportedRevision_ = view_->revision();
    auto viewInitTimeStoredInDomain = pullRequest.localviewinittime();
    bool isViewConsistent = viewInitTimeStoredInDomain == view_->viewinittime();
    bool hasNoNewChanges = pullRequest.version() == view_->revision();
    if (isViewConsistent && hasNoNewChanges) {
        Send(from, "ReportResource", "");
        return;
    }

    if (!isViewConsistent) {
        // 全量同步，直接处理
        ResourceUnitChanges result;
        result.set_localviewinittime(view_->viewinittime());
        ConvertFullResourceviewToChanges(result);
        Send(from, "ReportResource", result.SerializeAsString());
        return;
    }

    // 增量同步：根据变更数量决定同步或异步处理
    auto startRevision = pullRequest.version();
    auto endRevision = view_->revision();
    auto itStart = versionChanges_.upper_bound(startRevision);
    auto itEnd = versionChanges_.upper_bound(endRevision);
    auto changeCount = std::distance(itStart, itEnd);
    if (changeCount <= ASYNC_MERGE_THRESHOLD) {
        // 变更数量少，同步处理
        ResourceUnitChanges result;
        result.set_localviewinittime(view_->viewinittime());
        MergeResourceViewChanges(startRevision, endRevision, result);
        DelChanges(startRevision);
        Send(from, "ReportResource", result.SerializeAsString());
    } else {
        // 变更数量多，异步处理避免阻塞 Actor
        YRLOG_INFO("PullResource: {} changes to merge, using async processing", changeCount);
        MergeResourceViewChangesAsync(startRevision, endRevision, view_->viewinittime(), view_->id(), from);
        DelChanges(startRevision);
    }
}

std::string GetUnitIDFromAID(const litebus::AID &from)
{
    auto name = from.Name();
    auto pos = name.find_last_of('-');
    if (pos == std::string::npos) {
        return "";
    }
    return name.substr(0, pos);
}

void ResourceViewActor::ReportResource(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    auto changes = std::make_shared<ResourceUnitChanges>();
    auto localId = GetUnitIDFromAID(from);
    if (localId.empty()) {
        YRLOG_ERROR("empty localId!");
    }
    if (!changes->ParseFromString(msg) || msg.empty()) {
        poller_->Reset(localId);
        return;
    }

    UpdateResourceUnitDelta(changes);
}

void ResourceViewActor::SendPullResource(const std::string &id)
{
    if (urls_.find(id) == urls_.end() || localInfoMap_.find(id) == localInfoMap_.end()) {
        YRLOG_WARN("{} was not found, retry to pull", id);
        poller_->Reset(id);
        return;
    }
    litebus::AID toPull(id + actorSuffix_, urls_[id]);
    PullResourceRequest pullRequest;
    pullRequest.set_version(localInfoMap_[id].localRevisionInDomain);
    pullRequest.set_localviewinittime(localInfoMap_[id].localViewInitTime);
    auto msg = pullRequest.SerializeAsString();
    Send(toPull, "PullResource", std::move(msg));
}

void ResourceViewActor::TriggerTryPull()
{
    poller_->TryPullResource();
}

void ResourceViewActor::DelegateResetPull(const std::string &id)
{
    poller_->Reset(id);
}

void ResourceViewActor::RegisterUnitDisableFunc(const std::function<void(const std::string &)> &func)
{
    disableExecFunc_ = func;
}

void ResourceViewActor::DelChanges(int64_t newStartRevision)
{
    auto it = versionChanges_.lower_bound(newStartRevision);
    versionChanges_.erase(versionChanges_.begin(), it);
}

bool ResourceViewActor::IsResourceUnitChangeEmpty(const ResourceUnitChange &change)
{
    return !(change.has_addition() || change.has_deletion() || change.has_modification());
}

bool ResourceViewActor::IsModifyEmpty(const ResourceUnitChange &modify)
{
    return !modify.modification().has_statuschange() && !modify.modification().has_capacitychange() &&
        modify.modification().instancechanges().empty();
}

void ResourceViewActor::MergeResourceViewChanges(int64_t startRevision, int64_t endRevision,
                                                 ResourceUnitChanges &result)
{
    ASSERT_IF_NULL(view_);
    MergeResourceViewChanges(versionChanges_, RevisionRange{startRevision, endRevision}, view_->id(), result,
                             enableTenantAffinity_);
}

void ResourceViewActor::MergeResourceViewChanges(const std::map<int64_t, ResourceUnitChange> &changes,
                                                 const RevisionRange &range, const std::string &localId,
                                                 ResourceUnitChanges &result, bool enableTenantAffinity)
{
    std::unordered_map<std::string, ResourceUnitChange> summarizedChanges;
    std::vector<std::string> orderedIds;
    auto itEnd = changes.upper_bound(range.end);
    for (auto it = changes.upper_bound(range.start); it != itEnd; ++it) {
        const auto& change = it->second;
        const auto& resourceUnitId = change.resourceunitid();
        auto iter = summarizedChanges.find(resourceUnitId);
        if (iter == summarizedChanges.end()) {
            summarizedChanges.emplace(resourceUnitId, change);
            orderedIds.push_back(resourceUnitId);
            continue;
        }
        auto mergeChange = MergeResourceUnitChanges(iter->second, change, enableTenantAffinity);
        if (IsResourceUnitChangeEmpty(mergeChange)) {
            summarizedChanges.erase(iter);
            orderedIds.erase(std::remove(orderedIds.begin(), orderedIds.end(), resourceUnitId), orderedIds.end());
        } else {
            iter->second = std::move(mergeChange);
        }
    }
    result.mutable_changes()->Reserve(static_cast<int>(summarizedChanges.size()));
    for (const auto& id : orderedIds) {
        auto it = summarizedChanges.find(id);
        if (it != summarizedChanges.end()) {
            *result.add_changes() = std::move(it->second);
        }
    }
    result.set_startrevision(range.start);
    result.set_endrevision(range.end);
    result.set_localid(localId);
}

void ResourceViewActor::MergeResourceViewChangesAsync(int64_t startRevision, int64_t endRevision,
                                                      const std::string &viewInitTime, const std::string &localId,
                                                      const litebus::AID &replyTo)
{
    // 使用迭代器范围构造拷贝需要处理的 changes 数据
    auto itStart = versionChanges_.upper_bound(startRevision);
    auto itEnd = versionChanges_.upper_bound(endRevision);
    auto changesCopy = std::make_shared<std::map<int64_t, ResourceUnitChange>>(itStart, itEnd);

    // 懒初始化 asyncWorker_
    if (asyncWorker_ == nullptr) {
        asyncWorker_ = std::make_shared<ActorWorker>();
    }

    auto selfAid = GetAID();
    // capture the value on the actor thread, the static merge functions don't rely on any instance state
    auto enableTenantAffinity = enableTenantAffinity_;
    // 在 Worker 中执行合并，完成后通过 Async 回调到 Actor 线程发送结果
    asyncWorker_->AsyncWork([changesCopy, startRevision, endRevision, viewInitTime, localId, replyTo, selfAid,
                             enableTenantAffinity]() {
        auto result = std::make_shared<ResourceUnitChanges>() ;
        result->set_localviewinittime(viewInitTime);
        MergeResourceViewChanges(*changesCopy, RevisionRange{startRevision, endRevision}, localId, *result,
                                 enableTenantAffinity);
        // 回调到 Actor 线程发送结果
        litebus::Async(selfAid, &ResourceViewActor::ToReportResourceViewChanges, replyTo, result);
    });
}

void ResourceViewActor::ToReportResourceViewChanges(const litebus::AID &dst,
                                                    const std::shared_ptr<ResourceUnitChanges> &changes)
{
    Send(dst, "ReportResource", changes->SerializeAsString());
}

void ResourceViewActor::MergeCapacityChange(ResourceUnitChange &previous, const ResourceUnitChange &current)
{
    if (!current.has_modification() || !current.modification().has_capacitychange()) {
        return;
    }
    if (!previous.has_modification() || !previous.modification().has_capacitychange()) {
        previous.mutable_modification()->mutable_capacitychange()->CopyFrom(current.modification().capacitychange());
        return;
    }
    auto previousCapacityChange = previous.mutable_modification()->mutable_capacitychange();
    auto currentCapacityChange = current.modification().capacitychange();
    for (const auto &resourceName : currentCapacityChange.removedresourcenames()) {
        auto *removedResourceNames = previousCapacityChange->mutable_removedresourcenames();
        if (std::find(removedResourceNames->begin(), removedResourceNames->end(), resourceName) ==
            removedResourceNames->end()) {
            previousCapacityChange->add_removedresourcenames(resourceName);
        }
        previousCapacityChange->mutable_delta()->mutable_resources()->erase(resourceName);
    }
    (*previousCapacityChange->mutable_delta()) =
        previousCapacityChange->delta() + currentCapacityChange.delta();
}

ResourceUnitChange ResourceViewActor::MergeResourceUnitChanges(ResourceUnitChange &previous,
                                                               const ResourceUnitChange &current,
                                                               bool enableTenantAffinity)
{
    /**
     * 1.add         + modify       --> add
     * 2.add         + del          --> No change, Delete the existing add
     * 3.modify      + del          --> del
     * 4.modify      + modify       --> modify
     * 5.del         + any changes  --x Non-existent combination
     * 6.any changes + add          --x Non-existent combination
     */
    ASSERT_FS(previous.resourceunitid() == current.resourceunitid());
    if (previous.has_addition() && current.has_modification()) {
        return MergeAddAndModify(previous, current, enableTenantAffinity);
    }

    if (previous.has_addition() && current.has_deletion()) {
        return {};
    }

    if (previous.has_modification() && current.has_deletion()) {
        return current;
    }

    MergeCapacityChange(previous, current);
    return MergeTwoModifies(previous, current);
}

ResourceUnitChange ResourceViewActor::MergeAddAndModify(ResourceUnitChange &previous, const ResourceUnitChange &current,
                                                        bool enableTenantAffinity)
{
    auto previousResourceUnit = previous.mutable_addition()->mutable_resourceunit();

    if (current.modification().has_capacitychange()) {
        const auto &capacityChange = current.modification().capacitychange();
        for (const auto &resourceName : capacityChange.removedresourcenames()) {
            previousResourceUnit->mutable_capacity()->mutable_resources()->erase(resourceName);
            previousResourceUnit->mutable_allocatable()->mutable_resources()->erase(resourceName);
        }
        (*previousResourceUnit->mutable_capacity()) =
            previousResourceUnit->capacity() + capacityChange.delta();
        (*previousResourceUnit->mutable_allocatable()) =
            previousResourceUnit->allocatable() + capacityChange.delta();
    }

    if (current.modification().has_statuschange()) {
        previousResourceUnit->set_status(static_cast<uint32_t>(current.modification().statuschange().status()));
    }

    for (int i = 0; i < current.modification().instancechanges_size(); ++i) {
        auto &instanceChange = current.modification().instancechanges(i);
        auto currentInsId = instanceChange.instanceid();
        auto instance = instanceChange.instance();

        if (instanceChange.changetype() == InstanceChange::ADD) {
            (void)AddInstanceToAgentView(instance, *previousResourceUnit, enableTenantAffinity);
            UpdateBucketInfoAddInstance(instance, previousResourceUnit->capacity(),
                                        previousResourceUnit->instances_size(), *previousResourceUnit);
        } else if (instanceChange.changetype() == InstanceChange::DELETE) {
            (void)DeleteInstanceFromAgentView(instance, *previousResourceUnit);
            UpdateBucketInfoDelInstance(instance, previousResourceUnit->capacity(),
                                        previousResourceUnit->instances_size(), *previousResourceUnit);
        } else {
            YRLOG_WARN("Unable to merge instance({}) changes: The change type is unavailable.", currentInsId);
        }
    }

    return previous;
}

ResourceUnitChange ResourceViewActor::MergeTwoModifies(ResourceUnitChange &previous, const ResourceUnitChange &current)
{
    if (current.modification().has_statuschange()) {
        previous.mutable_modification()->mutable_statuschange()->CopyFrom(current.modification().statuschange());
    }

    if (!current.modification().instancechanges().empty()) {
        MergeInstanceChanges(*previous.mutable_modification(), current.modification());
    }

    if (IsModifyEmpty(previous)) {
        return {};
    }

    return previous;
}

bool ResourceViewActor::IsValidChangeCombination(const InstanceChange& previous, const InstanceChange& current)
{
    if (previous.instanceid() != current.instanceid()) {
        return false;
    }

    if (previous.changetype() == InstanceChange::ADD && current.changetype() == InstanceChange::ADD) {
        return false;
    }

    if (previous.changetype() == InstanceChange::DELETE && current.changetype() == InstanceChange::DELETE) {
        return false;
    }

    return true;
}

bool ResourceViewActor::ShouldMergeInstanceChange(const InstanceChange& previous, const InstanceChange& current)
{
    return previous.changetype() == InstanceChange::ADD && current.changetype() == InstanceChange::DELETE;
}

bool ResourceViewActor::ShouldRetainInstanceChange(const InstanceChange& previous, const InstanceChange& current)
{
    return previous.changetype() == InstanceChange::DELETE && current.changetype() == InstanceChange::ADD;
}

void ResourceViewActor::MergeInstanceChanges(Modification &previous, const Modification &current)
{
    /**
     * 1.add/delete instance1  + add/delete instance2  --> add/delete instance1 + add/delete instance2
     * 2.delete instance1      + add instance1         --> delete instance1 + add instance1
     * 3.add instance1         + delete instance1      --> no changes(remove previous change)
     * 5.add instance1         + add instance1         --x Non-existent combination
     * 6.delete instance1      + delete instance1      --x Non-existent combination
     */
    std::vector<std::pair<std::string, InstanceChange>> instanceChanges;
    for (const auto& change : previous.instancechanges()) {
        instanceChanges.push_back({change.instanceid(), change});
    }

    for (const auto& change : current.instancechanges()) {
        auto it = std::find_if(instanceChanges.begin(), instanceChanges.end(),
                               [&change](const auto& pair) { return pair.first == change.instanceid(); });
        if (it == instanceChanges.end()) {
            instanceChanges.push_back({change.instanceid(), change});
            continue;
        }

        // instance1 with DELETE and ADD changes
        auto itSecond = std::find_if(it + 1, instanceChanges.end(),
                                     [&change](const auto &pair) { return pair.first == change.instanceid(); });
        if (itSecond != instanceChanges.end()) {
            if (it->second.changetype() == InstanceChange::DELETE
                && itSecond->second.changetype() == InstanceChange::ADD) {
                it = itSecond;
            } else {
                YRLOG_WARN("invalid instance({}) resource update, multiple invalid change: {}, {}",
                           it->second.instanceid(), fmt::underlying(it->second.changetype()),
                           fmt::underlying(itSecond->second.changetype()));
                continue;
            }
        }

        auto previousChange = it->second;
        if (!IsValidChangeCombination(previousChange, change)) {
            YRLOG_WARN("Non-existent combination, instance({}) change type: {}", previousChange.instanceid(),
                       fmt::underlying(previousChange.changetype()));
            continue;
        }

        if (ShouldMergeInstanceChange(previousChange, change)) {
            instanceChanges.erase(it);
        }

        if (ShouldRetainInstanceChange(previousChange, change)) {
            instanceChanges.push_back({change.instanceid(), change});
        }
    }

    previous.clear_instancechanges();
    for (const auto& change : instanceChanges) {
        *previous.add_instancechanges() = change.second;
    }
}

void ResourceViewActor::ConvertFullResourceviewToChanges(ResourceUnitChanges &result)
{
    ASSERT_IF_NULL(view_);
    for (auto iter : view_->fragment()) {
        Addition addition;
        (*addition.mutable_resourceunit()) = iter.second;
        addition.mutable_resourceunit()->set_ownerid(view_->id());
        ResourceUnitChange resourceUnitChange;
        resourceUnitChange.set_resourceunitid(iter.second.id());
        *resourceUnitChange.mutable_addition() = addition;
        *result.add_changes() = resourceUnitChange;
    }
    result.set_startrevision(0);
    result.set_endrevision(view_->revision());
    result.set_localid(view_->id());
}

Status ResourceViewActor::HandleReportedAddition(const ResourceUnitChange &change)
{
    auto &agentResourceUnit = change.addition().resourceunit();
    auto agentId = change.resourceunitid();

    auto ownerid = agentResourceUnit.ownerid();
    if (ownerid.empty()) {
        YRLOG_WARN("resource unit named {} does not have ownerid", agentId);
        return Status(FAILED);
    }

    if (auto status = AddResourceUnit(agentResourceUnit); status.IsError()) {
        YRLOG_WARN("Failed to add agent resource view named {} to domain resource view.", agentId);
        return Status(FAILED);
    }

    localInfoMap_[ownerid].agentIDs.insert(agentId);
    allLocalLabels_[ownerid] = allLocalLabels_[ownerid] + agentResourceUnit.nodelabels();
    return Status::OK();
}

Status ResourceViewActor::HandleReportedDeletion(const ResourceUnitChange &change)
{
    auto agentId = change.resourceunitid();
    if (agentId.empty()) {
        YRLOG_WARN("domain resource view delete resource unit with empty ID.");
        return Status(FAILED);
    }

    ASSERT_IF_NULL(view_);
    auto agentFragmentIter = view_->mutable_fragment()->find(agentId);
    if (agentFragmentIter == view_->mutable_fragment()->end()) {
        YRLOG_WARN("domain resource view does not have a resource unit with ID {}.", agentId);
        return Status(FAILED);
    }

    auto &agentResourceUnit = agentFragmentIter->second;
    auto ownerid = agentResourceUnit.ownerid();
    auto agentNodelabels = agentResourceUnit.nodelabels();
    if (ownerid.empty()) {
        YRLOG_WARN("resource unit named {} does not have ownerid", agentId);
        return Status(FAILED);
    }

    if (auto status = DeleteResourceUnit(agentId); status.IsError()) {
        YRLOG_WARN("Failed to delete agent resource view named {} from domain resource view.", agentId);
        return Status(FAILED);
    }

    localInfoMap_[ownerid].agentIDs.erase(agentId);
    allLocalLabels_[ownerid] = allLocalLabels_[ownerid] - agentNodelabels;
    return Status::OK();
}

Status ResourceViewActor::HandleReportedAddInstacne(const InstanceInfo &instance)
{
    auto agentId = instance.unitid();
    YRLOG_DEBUG("domain add instance({}) to agent resource view named {}", instance.instanceid(), agentId);
    ASSERT_IF_NULL(view_);
    auto agentFragmentIter = view_->mutable_fragment()->find(agentId);
    if (agentFragmentIter == view_->mutable_fragment()->end()) {
        YRLOG_WARN("domain resource view does not have a resource unit with ID {}.", agentId);
        return Status(FAILED);
    }
    auto ownerid = agentFragmentIter->second.ownerid();
    if (ownerid.empty()) {
        YRLOG_WARN("resource unit named {} does not have ownerid", agentId);
        return Status(FAILED);
    }

    if (!IsValidInstance(instance)) {
        YRLOG_WARN("try to add invalid instances{} to resource view.", instance.instanceid());
        return Status(FAILED);
    }
    (void)AddInstanceToView(instance);
    AddLabel(instance, allLocalLabels_[ownerid], enableTenantAffinity_);
    MarkResourceUpdated();
    return Status::OK();
}

Status ResourceViewActor::HandleReportedDeleteInstacne(const InstanceInfo &instance)
{
    auto agentId = instance.unitid();
    YRLOG_DEBUG("domain delete instance({}) from agent resource view named {}", instance.instanceid(), agentId);
    ASSERT_IF_NULL(view_);
    auto agentFragmentIter = view_->mutable_fragment()->find(agentId);
    if (agentFragmentIter == view_->mutable_fragment()->end()) {
        YRLOG_WARN("domain resource view does not have a resource unit with ID {}.", agentId);
        return Status(FAILED);
    }
    auto ownerid = agentFragmentIter->second.ownerid();
    if (ownerid.empty()) {
        YRLOG_WARN("resource unit named {} does not have ownerid", agentId);
        return Status(FAILED);
    }

    if (!IsInstanceInResourceView(instance.instanceid())) {
        YRLOG_ERROR("domain failed to delete instance({}) from resource unit named {}, not found",
                    instance.instanceid(), agentId);
        return Status(FAILED);
    }
    (void)DeleteInstanceFromView(instance);
    DeleteLabel(instance, allLocalLabels_[ownerid]);
    MarkResourceUpdated();
    return Status::OK();
}

void ResourceViewActor::ApplyReportedCapacityChange(ResourceUnit &agentResourceUnit,
                                                    const CapacityChange &capacityChange)
{
    auto removedResources = CollectReportedRemovedResources(agentResourceUnit, capacityChange);
    if (!removedResources.capacity().resources().empty()) {
        DeleteResourceBySubUnit(*view_, removedResources);
        for (const auto &[resourceName, resource] : removedResources.capacity().resources()) {
            (void)resource;
            agentResourceUnit.mutable_capacity()->mutable_resources()->erase(resourceName);
            agentResourceUnit.mutable_allocatable()->mutable_resources()->erase(resourceName);
        }
    }

    const auto &delta = capacityChange.delta();
    YRLOG_DEBUG("for test: Unit({}) delta:{}", agentResourceUnit.id(), delta.ShortDebugString());
    (*agentResourceUnit.mutable_capacity()) = agentResourceUnit.capacity() + delta;
    (*agentResourceUnit.mutable_allocatable()) = agentResourceUnit.allocatable() + delta;
    (*view_->mutable_capacity()) = view_->capacity() + delta;
    (*view_->mutable_allocatable()) = view_->allocatable() + delta;
    PruneRemovedResources(*view_, removedResources);
    MarkResourceUpdated();
}

Status ResourceViewActor::HandleReportedModification(const ResourceUnitChange &change)
{
    ASSERT_IF_NULL(view_);
    auto modification = change.modification();
    auto agentId = change.resourceunitid();
    if (agentId.empty()) {
        YRLOG_WARN("domain resource view modify resource unit with empty ID.");
        return Status(FAILED);
    }

    auto agentFragmentIter = view_->mutable_fragment()->find(agentId);
    if (agentFragmentIter == view_->mutable_fragment()->end()) {
        YRLOG_WARN("domain resource view does not have a resource unit with ID {}.", agentId);
        return Status(FAILED);
    }
    auto &agentResourceUnit = agentFragmentIter->second;

    if (modification.has_statuschange()) {
        agentResourceUnit.set_status(static_cast<uint32_t>(modification.statuschange().status()));
        MarkResourceUpdated();
    }

    if (modification.has_capacitychange()) {
        ApplyReportedCapacityChange(agentResourceUnit, modification.capacitychange());
    }

    if (modification.instancechanges().empty()) {
        return Status::OK();
    }
    for (const auto& instanceChange : modification.instancechanges()) {
        auto instance = instanceChange.instance();
        switch (instanceChange.changetype()) {
            case InstanceChange::ADD:
                if (auto status = HandleReportedAddInstacne(instance); status.IsError()) {
                    return status;
                }
                break;
            case InstanceChange::DELETE:
                if (auto status = HandleReportedDeleteInstacne(instance); status.IsError()) {
                    return status;
                }
                break;
            default:
                break;
        }
    }
    return Status::OK();
}

bool ResourceViewActor::CheckLatestRevision(const std::shared_ptr<ResourceUnitChanges> &changes)
{
    /**
     * The stored unupdated request represents the latest local resource view in the domain.
     * So if there is a stored request, changes needs to be compared with the stored request.
     * Otherwise, it needs to be compared with the domain's stored information about the local.
     */
    auto localId = changes->localid();
    auto changesStartRevision = changes->startrevision();
    auto changesEndRevision = changes->endrevision();
    auto revisionMatch = true;

    bool hasPendingUpdate =
        latestReportedResourceViewChanges_.find(localId) != latestReportedResourceViewChanges_.end();
    auto isViewConsistent = changes->localviewinittime() == localInfoMap_[localId].localViewInitTime;
    // the new reported update request is reported before the local sheduler restart
    if (hasPendingUpdate && isViewConsistent &&
        changes->localviewinittime() != latestReportedResourceViewChanges_[localId]->localviewinittime()) {
        return false;
    }

    if (!isViewConsistent) {
        return (changesStartRevision == 0) ? true : false;
    }

    // Compare the changes' revision to determine if the reported update request is the latest.
    if (hasPendingUpdate) {
        auto startRevisionMatch = changesStartRevision == latestReportedResourceViewChanges_[localId]->startrevision();
        auto endRevisionMatch = changesEndRevision > latestReportedResourceViewChanges_[localId]->endrevision();
        revisionMatch = startRevisionMatch && endRevisionMatch;
    } else {
        auto startRevisionMatch = changesStartRevision == localInfoMap_[localId].localRevisionInDomain;
        auto endRevisionMatch = changesEndRevision > localInfoMap_[localId].localRevisionInDomain;
        revisionMatch = startRevisionMatch && endRevisionMatch;
    }
    return revisionMatch;
}

Status ResourceViewActor::UpdateResourceUnitDelta(const std::shared_ptr<ResourceUnitChanges> &changes)
{
    auto localId = changes->localid();
    if (localInfoMap_.find(localId) == localInfoMap_.end()) {
        YRLOG_WARN("Domain update resource failed because the domain does not have info about the local named {}",
                   localId);
        return Status(PARAMETER_ERROR, "domain does not have info about the local.");
    }

    if (!CheckLatestRevision(changes)) {
        return Status(PARAMETER_ERROR, "reported update request is not the latest.");
    }
    // If the cache exists, the update is not complete. Only the cache is updated.
    if (latestReportedResourceViewChanges_.find(localId) != latestReportedResourceViewChanges_.end()) {
        latestReportedResourceViewChanges_[localId] = changes;
        return Status::OK();
    }
    (void)latestReportedResourceViewChanges_.emplace(localId, changes);
    litebus::Async(GetAID(), &ResourceViewActor::DoUpdateResourceUnitDelta, localId);
    return Status::OK();
}

bool ResourceViewActor::HandleReportedChanges(const std::shared_ptr<ResourceUnitChanges> &resourceUnitChanges)
{
    bool isHandleSuccessful = true;
    for (int i = 0; i < resourceUnitChanges->changes_size(); ++i) {
        const ResourceUnitChange &change = resourceUnitChanges->changes(i);
        auto status = Status::OK();
        switch (change.Changed_case()) {
            case ResourceUnitChange::kAddition:
                status = HandleReportedAddition(change);
                break;
            case ResourceUnitChange::kDeletion:
                status = HandleReportedDeletion(change);
                break;
            case ResourceUnitChange::kModification:
                status = HandleReportedModification(change);
                break;
            default:
                break;
        }
        isHandleSuccessful &= status.IsOk();
    }
    return isHandleSuccessful;
}

void ResourceViewActor::MarkReportedChangesSnapshotDirty(const ResourceUnitChanges &resourceUnitChanges)
{
    bool ownerLabelsDirty = false;
    for (const auto &change : resourceUnitChanges.changes()) {
        MarkSnapshotUnitDirty(change.resourceunitid());
        if (change.has_addition() || change.has_deletion()) {
            MarkSnapshotStructureDirty(change.resourceunitid());
            MarkSnapshotMonopolyIndexDirty();
            ownerLabelsDirty = true;
            continue;
        }
        if (!change.has_modification() || change.modification().instancechanges_size() == 0) {
            continue;
        }
        MarkSnapshotMonopolyIndexDirty();
        for (const auto &instanceChange : change.modification().instancechanges()) {
            const auto &instance = instanceChange.instance();
            ownerLabelsDirty = ownerLabelsDirty || instance.labels_size() > 0 || instance.kvlabels_size() > 0;
        }
    }
    MarkSnapshotIndexesDirty(true, ownerLabelsDirty);
    MarkSnapshotMetadataDirty();
}

void ResourceViewActor::DoUpdateResourceUnitDelta(const std::string localId)
{
    SnapshotMutationGuard snapshotGuard(*this);
    if (latestReportedResourceViewChanges_.find(localId) == latestReportedResourceViewChanges_.end()) {
        return;
    }

    if (localInfoMap_.find(localId) == localInfoMap_.end()) {
        YRLOG_WARN("Domain update resource failed because the domain does not have info about the local named {}",
                   localId);
        (void)latestReportedResourceViewChanges_.erase(localId);
        return;
    }

    const auto &resourceUnitChanges = latestReportedResourceViewChanges_[localId]; // latest
    if (resourceUnitChanges->localviewinittime() != localInfoMap_[localId].localViewInitTime) {
        YRLOG_WARN("domain resourceview is inconsistent with local resourceview, local id is {}, "
                   "old init time is {}, new init time is {}", localId, localInfoMap_[localId].localViewInitTime,
                   resourceUnitChanges->localviewinittime());
        if (auto status = ClearLocalSchedulerAgentsInDomain(localId); status.IsError()) {
            YRLOG_WARN("failed to clear all agent in domain, local id is {}", localId);
        }
    }

    YRLOG_INFO("domain receive a update request from local({}), localRevisionInDomain is {}, "
               "the start revision of update is {}, the end revision of update is {}",
               localId, localInfoMap_[localId].localRevisionInDomain,
               resourceUnitChanges->startrevision(), resourceUnitChanges->endrevision());

    ASSERT_IF_NULL(view_);
    view_->set_revision(view_->revision() + 1);

    localInfoMap_[localId].localRevisionInDomain = resourceUnitChanges->endrevision();
    localInfoMap_[localId].localViewInitTime = resourceUnitChanges->localviewinittime();
    MarkReportedChangesSnapshotDirty(*resourceUnitChanges);
    bool isHandleSuccessful = HandleReportedChanges(resourceUnitChanges);
    if (!isHandleSuccessful) {
        YRLOG_ERROR("domain needs to recover the local({}) resourceview", localId);
        (void)latestReportedResourceViewChanges_.erase(localId);
        localInfoMap_[localId].localViewInitTime = NEED_RECOVER_VIEW;
        return;
    }
    NotifyResourceUpdated();
    if (isHeader_) {
        SetResourceMetricsContext(resourceUnitChanges);
    }

    // delete after set billing
    (void)latestReportedResourceViewChanges_.erase(localId);
    poller_->Reset(localId);
}

void ResourceViewActor::SetResourceMetricsContext(const std::shared_ptr<ResourceUnitChanges> &resourceUnitChanges) const
{
    for (int i = 0; i < resourceUnitChanges->changes_size(); ++i) {
        const ResourceUnitChange &change = resourceUnitChanges->changes(i);
        switch (change.Changed_case()) {
            case ResourceUnitChange::kAddition:
                if (view_->fragment().contains(change.addition().resourceunit().id())) {
                    metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetPodResource(
                        change.resourceunitid(), view_->fragment().at(change.addition().resourceunit().id()));
                }
                break;
            case ResourceUnitChange::kDeletion:
                metrics::MetricsAdapter::GetInstance().GetMetricsContext().DeletePodResource(change.resourceunitid());
                break;
            case ResourceUnitChange::kModification:
                if (view_->fragment().contains(change.resourceunitid())) {
                    metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetPodResource(
                        change.resourceunitid(), view_->fragment().at(change.resourceunitid()));
                }
                break;
            default:
                break;
        }
    }
}

void ResourceViewActor::MarkResourceUpdated()
{
    hasResourceUpdated_ = true;
}

void ResourceViewActor::NotifyResourceUpdated()
{
    if (!hasResourceUpdated_) {
        return;
    }
    // Snapshot consumers must observe the published update when they wake up.
    if (scheduleSnapshotEnabled_ && !snapshotDirtySet_.Empty()) {
        snapshotNotificationPending_ = true;
        return;
    }
    OnUpdate();
    hasResourceUpdated_ = false;
}

void ResourceViewActor::PodRecycler(const ResourceUnit &unit)
{
    int recycleTime = ParseRecyclePodLabel(unit);
    YRLOG_DEBUG("set pod {} recycler, recycle time: {}", unit.id(), recycleTime);
    if (recycleTime == -1) { // -1 mean pod can not be recycled
        return;
    }
    if (recycleTime > 0) { // > 0 mean pod need to be recycled
        SetAgentReuseTimer(unit.id(), recycleTime);
        return;
    }
    if (enableTenantAffinity_ && unit.instances_size() != 0) { // for recovering when instance existed in pod
        YRLOG_DEBUG("set pod {} recycler, enableTenantAffinity: {}, instance size: {}", unit.id(),
                    enableTenantAffinity_, unit.instances_size());
        SetAgentReuseTimer(unit.id(), tenantPodReuseTimeWindow_);
        agentUsedMap_[unit.id()] = true;
    }
}

int32_t ResourceViewActor::ParseRecyclePodLabel(const ResourceUnit &unit)
{
    if (auto it = unit.nodelabels().find(IDLE_TO_RECYCLE); it != unit.nodelabels().end()) {
        if (it->second.items_size() != 1) {
            YRLOG_WARN("ParseRecyclePodLabel get more value, thinks as closed");
            return 0;
        }
        auto firstElement = it->second.items().begin();
        if (firstElement->first == "unlimited") {
            return -1;
        }
        try {
            int32_t recycleTime = std::stoi(firstElement->first);
            return recycleTime > 0 ?  recycleTime : 0;
        } catch (std::exception &e) {
            YRLOG_WARN("{} time ({}) parse failed, err:{}", IDLE_TO_RECYCLE, firstElement->first, e.what());
            return 0;
        }
    }
    return 0;
}

inline SCHEDULER_LEVEL ResourceViewActor::GetCurrentSchedulerLevel(void)
{
    if (isLocal_) {
        return SCHEDULER_LEVEL::LOCAL;
    }
    if (isHeader_) {
        return SCHEDULER_LEVEL::ROOT_DOMAIN;
    }
    return SCHEDULER_LEVEL::NON_ROOT_DOMAIN;
}

void ResourceViewActor::TryDelResourceUnitChange(uint64_t revision, const std::string &viewInitTime)
{
    if (viewInitTime == view_->viewinittime() && revision <= lastReportedRevision_) {
        DelChanges(revision);
    }
}

litebus::Future<PullResourceRequest> ResourceViewActor::GetUnitSnapshotInfo(const std::string &unitID)
{
    PullResourceRequest snapshot;
    if (localInfoMap_.find(unitID) == localInfoMap_.end()) {
        YRLOG_WARN("{} was not found, snapshot return empty", unitID);
        return snapshot;
    }
    snapshot.set_version(localInfoMap_[unitID].localRevisionInDomain);
    snapshot.set_localviewinittime(localInfoMap_[unitID].localViewInitTime);
    return snapshot;
}
}  // namespace functionsystem::resource_view
