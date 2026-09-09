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

#include "resource_view.h"

#include "async/async.hpp"
#include "common/constants/constants.h"
#include "common/logs/logging.h"
#include "litebus.hpp"

namespace functionsystem::resource_view {

ResourceView::ResourceView(const std::shared_ptr<ResourceViewActor> &actor)
    : ActorDriver(actor), implActor_(actor)
{
    YRLOG_INFO("Create resource view Actor : {}", std::string(implActor_->GetAID()));
}

ResourceView::~ResourceView()
{
}

std::unique_ptr<ResourceView> ResourceView::CreateResourceView(
    const std::string &id, const ResourceViewActor::Param &param, const std::string &tag)
{
    std::string aid = id + "-ResourceViewActor";
    if (!tag.empty()) {
        aid = id + "-" + tag + "ResourceViewActor";
    }
    auto implActor = std::make_shared<ResourceViewActor>(aid, id, param);
    litebus::Spawn(implActor, false);
    return std::make_unique<ResourceView>(implActor);
}

litebus::Future<Status> ResourceView::AddResourceUnit(const ResourceUnit &value)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::AddResourceUnit, value);
}

litebus::Future<Status> ResourceView::AddResourceUnitWithUrl(const ResourceUnit &value, const std::string &url)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::AddResourceUnitWithUrl, value, url);
}

litebus::Future<Status> ResourceView::DeleteResourceUnit(const std::string &unitID)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::DeleteResourceUnit, unitID);
}

litebus::Future<Status> ResourceView::DeleteLocalResourceView(const std::string &localID)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::DeleteLocalResourceView, localID);
}

litebus::Future<Status> ResourceView::UpdateResourceUnit(const std::shared_ptr<ResourceUnit> &value,
                                                         const UpdateType &type)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::UpdateResourceUnit, value, type);
}

litebus::Future<Status> ResourceView::UpdateResourceUnitDelta(const std::shared_ptr<ResourceUnitChanges> &changes)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::UpdateResourceUnitDelta, changes);
}

litebus::Future<Status> ResourceView::BindInstanceRuntime(const InstanceInfo &instance,
                                                         const std::string &expectedRuntimeID)
{
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::BindInstanceRuntime, instance, expectedRuntimeID);
}

litebus::Future<Status> ResourceView::ReserveResize(const messages::ResizeSandboxRequest &request)
{
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::ReserveResize, request);
}
litebus::Future<Status> ResourceView::ObserveResize(const messages::ResizeSandboxResponse &response)
{
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::ObserveResize, response);
}
litebus::Future<Status> ResourceView::CancelResizeBeforeDispatch(const std::string &reservationID)
{
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::CancelResizeBeforeDispatch, reservationID);
}

litebus::Future<Status> ResourceView::ApplyPhysicalRetirement(const PhysicalRetirementReceipt &receipt)
{
    // One actor hop: budget settlement and any logical deletion happen inside a
    // single ResourceViewActor turn with no interleaved resize registration.
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::ApplyPhysicalRetirement, receipt);
}

AdmissionReserve ResourceView::GetOutstandingResizeForTest(const std::string &unitID)
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->GetOutstandingResizeForTest(unitID);
}

size_t ResourceView::GetResizeBudgetEntryCountForTest()
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->GetResizeBudgetEntryCountForTest();
}

litebus::Future<Status> ResourceView::AddInstances(const std::map<std::string, InstanceAllocatedInfo> &insts)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::AddInstances, insts);
}

litebus::Future<Status> ResourceView::DeleteInstances(const std::vector<std::string> &instIDs,
                                                      bool isVirtualInstance)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::DeleteInstances, instIDs, isVirtualInstance);
}

litebus::Future<std::shared_ptr<ResourceUnitChanges>> ResourceView::GetResourceViewChanges()
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::GetResourceViewChanges);
}

litebus::Future<std::shared_ptr<ResourceUnit>> ResourceView::GetResourceViewCopy()
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::GetResourceViewCopy);
}

litebus::Future<std::shared_ptr<ResourceUnit>> ResourceView::GetResourceView()
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::GetResourceView);
}

litebus::Future<std::string> ResourceView::GetSerializedResourceView()
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::GetSerializedResourceView);
}

litebus::Future<litebus::Option<ResourceUnit>> ResourceView::GetResourceUnit(const std::string &unitID)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::GetResourceUnit, unitID);
}

litebus::Future<litebus::Option<std::string>> ResourceView::GetUnitByInstReqID(const std::string &instReqID)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::GetUnitByInstReqID, instReqID);
}

void ResourceView::ClearResourceView()
{
    ASSERT_IF_NULL(implActor_);
    litebus::Async(implActor_->GetAID(), &ResourceViewActor::ClearResourceView);
}

void ResourceView::AddResourceUpdateHandler(const ResourceUpdateHandler &handler)
{
    ASSERT_IF_NULL(implActor_);
    litebus::Async(implActor_->GetAID(), &ResourceViewActor::AddResourceUpdateHandler, handler);
}

void ResourceView::PrintResourceView() const
{
    ASSERT_IF_NULL(implActor_);
    litebus::Async(implActor_->GetAID(), &ResourceViewActor::PrintResourceView);
}

litebus::Future<ResourceViewInfo> ResourceView::GetResourceInfo()
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::GetResourceInfo);
}

std::shared_ptr<ScheduleSnapshotStore> ResourceView::GetScheduleSnapshotStore() const
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->GetScheduleSnapshotStore();
}

void ResourceView::TriggerTryPull()
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::TriggerTryPull);
}

litebus::Future<Status> ResourceView::UpdateUnitStatus(const std::string &unitID, UnitStatus status)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::UpdateUnitStatus, unitID, status);
}

litebus::Future<std::shared_ptr<ResourceUnit>> ResourceView::GetFullResourceView()
{
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::GetFullResourceView);
}

// for test
std::unordered_map<std::string, std::unordered_set<std::string>> ResourceView::GetAgentCache()
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->GetAgentCache();
}

// for test
std::unordered_map<std::string, litebus::Timer> ResourceView::GetReuseTimers()
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->GetReuseTimers();
}

// for test
std::map<int64_t, ResourceUnitChange> ResourceView::GetVersionChanges()
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->GetVersionChanges();
}

// for test
void ResourceView::MergeLocalResourceViewChanges(int64_t startRevision, int64_t endRevision,
                                                 ResourceUnitChanges &result)
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->MergeLocalResourceViewChanges(startRevision, endRevision, result);
}

// for test
bool ResourceView::CheckLocalExistInDomainView(std::string localId)
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->CheckLocalExistInDomainView(localId);
}

// for test
LocalResourceViewInfo ResourceView::GetLocalInfoInDomain(std::string localId)
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->GetLocalInfoInDomain(localId);
}

// for test
std::shared_ptr<ResourceUnitChanges> ResourceView::GetLatestReportChanges(const std::string& localId)
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->GetLatestReportChanges(localId);
}

// for test
void ResourceView::SetLatestReportChanges(const std::string& localId,  ResourceUnitChanges changes)
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->SetLatestReportChanges(localId, changes);
}

// for test
void ResourceView::SetEnableTenantAffinity(bool enable)
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->SetEnableTenantAffinity(enable);
}

// for test
AgentCacheMap ResourceView::GetAgentCacheMap()
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->GetAgentCacheMap();
}

// for test
std::unordered_map<std::string, bool> ResourceView::GetAgentUsedMap()
{
    ASSERT_IF_NULL(implActor_);
    return implActor_->GetAgentUsedMap();
}

void ResourceView::RegisterUnitDisableFunc(const std::function<void(const std::string &)> &func)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::RegisterUnitDisableFunc, func);
}

void ResourceView::UpdateDomainUrlForLocal(const std::string &addr)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::UpdateDomainUrlForLocal, addr);
}

void ResourceView::UpdateIsHeader(bool isHeader)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::UpdateIsHeader, isHeader);
}

void ResourceView::TryDelResourceUnitChange(uint64_t version, const std::string &viewInitTime)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::TryDelResourceUnitChange, version, viewInitTime);
}

litebus::Future<PullResourceRequest> ResourceView::GetUnitSnapshotInfo(const std::string &id)
{
    ASSERT_IF_NULL(implActor_);
    return litebus::Async(implActor_->GetAID(), &ResourceViewActor::GetUnitSnapshotInfo, id);
}

}  // namespace functionsystem::resource_view
