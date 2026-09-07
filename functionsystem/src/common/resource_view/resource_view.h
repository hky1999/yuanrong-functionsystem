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

#ifndef COMMON_RESOURCE_VIEW_RESOURCE_VIEW_H
#define COMMON_RESOURCE_VIEW_RESOURCE_VIEW_H


#include <memory>

#include "async/future.hpp"
#include "async/option.hpp"
#include "common/status/status.h"
#include "common/utils/actor_driver.h"
#include "resource_type.h"
#include "resource_view_actor.h"

namespace functionsystem::resource_view {

using AgentCacheMap =
    std::unordered_map<std::string, std::unordered_set<std::string>>;
class ResourceViewActor;

const ResourceViewActor::Param VIEW_ACTOR_DEFAULT_PARAM = {
    .isLocal = false,
    .enableTenantAffinity = true,
    .tenantPodReuseTimeWindow = 10,
    .enableScheduleSnapshot = false
};

class ResourceView : public ActorDriver {
public:
    explicit ResourceView(const std::shared_ptr<ResourceViewActor> &actor);
    virtual ~ResourceView() override;
    ResourceView(const ResourceView &) = delete;
    ResourceView &operator=(const ResourceView &) = delete;

    /**
     * @brief Create a resource view instance.
     * @return unique_ptr<ResourceView>
     */
    static std::unique_ptr<ResourceView> CreateResourceView(
        const std::string &id, const ResourceViewActor::Param &param = VIEW_ACTOR_DEFAULT_PARAM,
        const std::string &tag = "");

    /**
     * @brief Add a resource unit to the resource view.
     * @param unit The resource unit can be function agent / local scheduler / domain scheduler etc.
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    virtual litebus::Future<Status> AddResourceUnit(const ResourceUnit &value);

    virtual litebus::Future<Status> AddResourceUnitWithUrl(const ResourceUnit &value, const std::string &url);

    /**
     * @brief Delete a resource unit from the resource view.
     * @param ID resource unit identifier
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    virtual litebus::Future<Status> DeleteResourceUnit(const std::string &unitID);

    /**
     * @brief Delete local resource view from the domain resource view.
     * @param ID local resource view identifier
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    virtual litebus::Future<Status> DeleteLocalResourceView(const std::string &localID);

    /**
     * @brief Update resource unit, including static resource descriptions and current resource usage
     * @param value Resource unit to be updated
     * @param type Specify update static resource descriptions or current resource usage
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    virtual litebus::Future<Status> UpdateResourceUnit(const std::shared_ptr<ResourceUnit> &value,
                                                       const UpdateType &type);

    /**
     * @brief Updates the delta of resource units
     * @param changes Resource unit delta to be updated
     */
    virtual litebus::Future<Status> UpdateResourceUnitDelta(const std::shared_ptr<ResourceUnitChanges> &changes);

    /**
     * @brief Add instances to deduct the corresponding resource from the resource view
     * @param insts a group of instances
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    virtual litebus::Future<Status> ReserveResize(const messages::ResizeSandboxRequest &request);
    virtual litebus::Future<Status> ObserveResize(const messages::ResizeSandboxResponse &response);
    virtual litebus::Future<Status> CancelResizeBeforeDispatch(const std::string &reservationID);

    virtual litebus::Future<Status> AddInstances(const std::map<std::string, InstanceAllocatedInfo> &insts);

    /**
     * @brief Delete instances to add the corresponding resource from the resource view
     * @param instIDs a group of instances
     * @param isVirtualInstance used for gang scheduling, indicating that the instance is not actually deployed
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    virtual litebus::Future<Status> DeleteInstances(const std::vector<std::string> &instIDs,
                                                    bool isVirtualInstance = false);

    /**
     * brief Get the changes in resourceview since the last report to the domain
     * @return The changes in resourceview since the last report to the domain
     */
    virtual litebus::Future<std::shared_ptr<ResourceUnitChanges>> GetResourceViewChanges();

    /**
     * brief Get current resource view
     * @return The copy of the resource view.
     */
    virtual litebus::Future<std::shared_ptr<ResourceUnit>> GetResourceViewCopy();

    /**
     * brief Get current resource view
     * @return The shared pointer of the resource view. User cannot modify it, otherwise it needs to be copied
     */
    virtual litebus::Future<std::shared_ptr<ResourceUnit>> GetResourceView();

    /**
     * brief Get current resource view which is serialized
     * @return The serialized resource view.
     */
    virtual litebus::Future<std::string> GetSerializedResourceView();

    /**
     * @briedf Get one ResourceUnit by ID
     * @param ID Specify one resource unit，may be function agent / local scheduler / domain scheduler etc
     * @return value of resource unit
     */
    virtual litebus::Future<litebus::Option<ResourceUnit>> GetResourceUnit(const std::string &unitID);

    /**
     * @brief Query the resource unit by instance's request ID
     * @param instReqID instance's request ID
     * @return resource unit ID or None()
     */
    virtual litebus::Future<litebus::Option<std::string>> GetUnitByInstReqID(const std::string &instReqID);

    virtual litebus::Future<ResourceViewInfo> GetResourceInfo();

    virtual std::shared_ptr<ScheduleSnapshotStore> GetScheduleSnapshotStore() const;

    /**
     * @brief clear all resource unit from view
     */
    virtual void ClearResourceView();

    /**
     * @brief When resource view update lik add/remove/update resource unit, will call resource update handler
     * @param handler resource update callback
     */
    virtual void AddResourceUpdateHandler(const ResourceUpdateHandler &handler);

    void PrintResourceView() const;

    virtual void TriggerTryPull();

    /**
     * @brief Update unit status
     * @param unitID: resource unit ID
     * @param status: 0 is normal; 1 is
     * @return Status
     */
    virtual litebus::Future<Status> UpdateUnitStatus(const std::string &unitID, UnitStatus status);

    virtual litebus::Future<std::shared_ptr<ResourceUnit>> GetFullResourceView();

    virtual void RegisterUnitDisableFunc(const std::function<void(const std::string &)> &func);

    virtual void UpdateDomainUrlForLocal(const std::string &addr);

    virtual void UpdateIsHeader(bool isHeader);

    virtual void TryDelResourceUnitChange(uint64_t version, const std::string &viewInitTime);

    virtual litebus::Future<PullResourceRequest> GetUnitSnapshotInfo(const std::string &id);

    // for test
    std::unordered_map<std::string, std::unordered_set<std::string>> GetAgentCache();

    // for test
    std::unordered_map<std::string, litebus::Timer> GetReuseTimers();

    // for test
    std::map<int64_t, ResourceUnitChange> GetVersionChanges();

    // for test
    void MergeLocalResourceViewChanges(int64_t startRevision, int64_t endRevision, ResourceUnitChanges &result);

    // for test
    bool CheckLocalExistInDomainView(std::string localId);

    // for test
    LocalResourceViewInfo GetLocalInfoInDomain(std::string localId);

    // for test
    std::shared_ptr<ResourceUnitChanges> GetLatestReportChanges(const std::string& localId);

    // for test
    void SetLatestReportChanges(const std::string& localId,  ResourceUnitChanges changes);

    // for test
    void SetEnableTenantAffinity(bool enable);
    // for test
    AgentCacheMap GetAgentCacheMap();
    // for test
    std::unordered_map<std::string, bool> GetAgentUsedMap();

private:
    std::shared_ptr<ResourceViewActor> implActor_;
};

}  // namespace functionsystem::resource_view
#endif  // COMMON_RESOURCE_VIEW_RESOURCE_VIEW_H
