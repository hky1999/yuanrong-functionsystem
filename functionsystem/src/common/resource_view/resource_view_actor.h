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

#ifndef COMMON_RESOURCE_VIEW_RESOURCE_VIEW_ACTOR_H
#define COMMON_RESOURCE_VIEW_RESOURCE_VIEW_ACTOR_H

#include <list>
#include <unordered_map>

#include "async/future.hpp"
#include "async/option.hpp"
#include "common/utils/actor_driver.h"
#include "common/utils/actor_worker.h"
#include "common/status/status.h"
#include "resource_type.h"
#include "resource_poller.h"

namespace functionsystem::resource_view {

// 合并的 revision 范围，左开右闭：start < revision <= end
struct RevisionRange {
    int64_t start;
    int64_t end;
};

struct InstanceAllocatedInfo {
    const InstanceInfo instanceInfo;
    std::shared_ptr<litebus::Promise<Status>> allocatedPromise;
};

struct LocalResourceViewInfo {
    uint64_t localRevisionInDomain;
    std::unordered_set<std::string> agentIDs;
    std::string localViewInitTime;
};

class ResourceViewActor : public BasisActor {
public:
    struct Param {
        bool isLocal{true};
        bool enableTenantAffinity{true};
        int32_t tenantPodReuseTimeWindow{10};
    };
    ResourceViewActor(const std::string &name, std::string id, const Param &param);
    ~ResourceViewActor() override = default;

    /**
     * @brief Add a resource unit to the resource view.
     * @param unit The resource unit can be function agent / local scheduler / domain scheduler etc.
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    Status AddResourceUnit(const ResourceUnit &value);

    /**
     * @brief Add a resource unit to the resource view.
     * @param unit The resource unit can be function agent / local scheduler / domain scheduler etc.
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    Status AddResourceUnitWithUrl(const ResourceUnit &value, const std::string &url);

    /**
     * @brief Delete a resource unit from the resource view.
     * @param ID resource unit identifier
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    Status DeleteResourceUnit(const std::string &unitID);

    Status ClearLocalSchedulerAgentsInDomain(const std::string &localID);

    /**
     * @brief Delete local resource view from the domain resource view.
     * @param localID local resource view identifier
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    Status DeleteLocalResourceView(const std::string &localID);

    /**
     * @brief Update resource unit, including static resource descriptions and current resource usage
     * @param value Resource unit to be updated
     * @param type Specify update static resource descriptions or current resource usage
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    Status UpdateResourceUnit(const std::shared_ptr<ResourceUnit> &value, const UpdateType &type);

    /**
     * @brief Updates the delta of resource units
     * @param changes Resource unit delta to be updated
     */
    Status UpdateResourceUnitDelta(const std::shared_ptr<ResourceUnitChanges> &changes);

    /**
     * @brief Add instances to deduct the corresponding resource from the resource view
     * @param insts a group of instances
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    Status AddInstances(const std::map<std::string, InstanceAllocatedInfo> &insts);

    /**
     * @brief Delete instances to add the corresponding resource from the resource view
     * @param instIDs a group of instances
     * @param isVirtualInstance used for gang scheduling, indicating that the instance is not actually deployed
     * @return Return OK if successful, otherwise return the corresponding error code
     */
    Status DeleteInstances(const std::vector<std::string> &instIDs, bool isVirtualInstance = false);

    /**
     * brief Get current resource view
     * @return The copy of the resource view.
     */
    std::shared_ptr<ResourceUnit> GetResourceViewCopy();

    ResourceViewInfo GetResourceInfo();

    /**
     * brief Get the changes in resourceview since the last report to the domain
     * @return The changes in resourceview since the last report to the domain
     */
    std::shared_ptr<ResourceUnitChanges> GetResourceViewChanges();

    /**
     * brief Get current resource view
     * @return The shared pointer of the resource view. User cannot modify it, otherwise it needs to be copied
     */
    std::shared_ptr<ResourceUnit> GetResourceView();

    /**
     * brief Get current resource view which is serialized
     * @return The serialized resource view.
     */
    std::string GetSerializedResourceView();

    /**
     * @briedf Get one ResourceUnit by ID
     * @param ID Specify one resource unit，may be function agent / local scheduler / domain scheduler etc
     * @return value of resource unit
     */
    litebus::Option<ResourceUnit> GetResourceUnit(const std::string &unitID);

    /**
     * @brief clear all resource unit from view
     */
    virtual void ClearResourceView();

    /**
     * @brief When resource view update lik add/remove/update resource unit, will call resource update handler
     * @param handler resource update callback
     */
    void AddResourceUpdateHandler(const ResourceUpdateHandler &handler);

    /**
     * @brief Query the resource unit by instance's request ID
     * @param instReqID instance's request ID
     * @return resource unit ID or None()
     */
    litebus::Option<std::string> GetUnitByInstReqID(const std::string &instReqID);

    /**
     * @brief Update unit status
     * @param unitID: resource unit ID
     * @param status: 0 is normal; 1 is
     * @return Status
     */
    Status UpdateUnitStatus(const std::string &unitID, UnitStatus status);

    /* *
     * @brief request to pull the resource unit by Uplayer
     * @param msg: Serialized PullResourceRequest
     */
    void PullResource(const litebus::AID &from, std::string &&name, std::string &&msg);

    /* *
    * @brief report updated Resource unit to Uplayer
    * @param msg: if updated msg is Serialized PullResourceRequest else empty
    */
    void ReportResource(const litebus::AID &from, std::string &&name, std::string &&msg);

    void TriggerTryPull();

    void PrintResourceView();

    void RegisterUnitDisableFunc(const std::function<void(const std::string &)> &func);

    std::shared_ptr<ResourceUnit> GetFullResourceView();

    Status DeleteSubUnit(const std::string &unitID);

    void UpdateDomainUrlForLocal(const std::string &addr);

    void UpdateIsHeader(bool isHeader);

    void TryDelResourceUnitChange(uint64_t revision, const std::string &viewInitTime);

    void ToReportResourceViewChanges(const litebus::AID &dst, const std::shared_ptr<ResourceUnitChanges> &changes);

    // used by domain
    litebus::Future<PullResourceRequest> GetUnitSnapshotInfo(const std::string &unitID);

    // for test
    [[maybe_unused]] std::unordered_map<std::string, std::unordered_set<std::string>> GetAgentCache()
    {
        return agentCacheMap_;
    }

    // for test
    [[maybe_unused]] std::unordered_map<std::string, litebus::Timer> GetReuseTimers()
    {
        return reuseTimers_;
    }

    // for test
    [[maybe_unused]] std::map<int64_t, ResourceUnitChange> GetVersionChanges()
    {
        return versionChanges_;
    }

    // for test
    [[maybe_unused]] void MergeLocalResourceViewChanges(int64_t startRevision, int64_t endRevision,
                                                        ResourceUnitChanges &result)
    {
        MergeResourceViewChanges(startRevision, endRevision, result);
    }

    // for test
    [[maybe_unused]] bool CheckLocalExistInDomainView(std::string localId)
    {
        return localInfoMap_.find(localId) != localInfoMap_.end();
    }

    // for test
    [[maybe_unused]] LocalResourceViewInfo GetLocalInfoInDomain(std::string localId)
    {
        return localInfoMap_[localId];
    }

    // for test
    [[maybe_unused]] std::shared_ptr<ResourceUnitChanges> GetLatestReportChanges(const std::string& localId)
    {
        auto it = latestReportedResourceViewChanges_.find(localId);
        if (it != latestReportedResourceViewChanges_.end()) {
            return it->second;
        }
        return nullptr;
    }

    // for test
    [[maybe_unused]] void SetLatestReportChanges(const std::string& localId, ResourceUnitChanges changes)
    {
        latestReportedResourceViewChanges_[localId] = std::make_shared<ResourceUnitChanges>(changes);
    }

    // for test
    [[maybe_unused]] void SetEnableTenantAffinity(bool enable)
    {
        enableTenantAffinity_ = enable;
    }

    // for test
    [[maybe_unused]] std::unordered_map<std::string, std::unordered_set<std::string>> GetAgentCacheMap()
    {
        return agentCacheMap_;
    }

    // for test
    [[maybe_unused]] std::unordered_map<std::string, bool> GetAgentUsedMap()
    {
        return agentUsedMap_;
    }

    // for test
    [[maybe_unused]] int32_t TestParseRecyclePodLabel(const ResourceUnit &unit)
    {
        return ParseRecyclePodLabel(unit);
    }

protected:
    void Init() override;
    void Finalize() override;

private:
    static void AddResourceBySubUnit(ResourceUnit &view, const ResourceUnit &value);
    static void DeleteResourceBySubUnit(ResourceUnit &view, ResourceUnit &value);
    static void PruneRemovedResources(ResourceUnit &view, const ResourceUnit &removedResources);
    static void AddInstancesBySubUnit(ResourceUnit &view, const ResourceUnit &subUnit);
    static void DeleteInstancesBySubUnit(ResourceUnit &view, const ResourceUnit &subUnit);
    static void AddBucketIndexBySubUnit(ResourceUnit &view, const ResourceUnit &fragmentUnit);
    static void DeleteBucketIndexBySubUnit(ResourceUnit &view, const ResourceUnit &fragmentUnit);

    static Resources AddInstanceToAgentView(const InstanceInfo &instance, resources::ResourceUnit &unit,
                                            bool enableTenantAffinity);
    void AddInstanceToView(const InstanceInfo &instance);
    void DeleteInstanceFromView(const InstanceInfo &instance);

    void SendPullResource(const std::string &id);
    void DelegateResetPull(const std::string &id);

    void UpdateTime();
    void OnUpdate();

    bool IsInstanceInResourceView(const std::string &instID);
    bool IsValidUnit(const ResourceUnit &unit) const;
    bool IsValidInstance(const InstanceInfo &instance);
    bool IsValidInstances(const std::map<std::string, InstanceAllocatedInfo> &instances);

    void AddInstance(const InstanceInfo &instance);
    void DeleteInstance(const std::string &instID, bool isVirtualInstance);

    void AddInstanceAgentCache(const InstanceInfo &instInfo, bool forceAdd);
    void DeleteInstanceAgentCache(const InstanceInfo &instInfo, bool needToRecycle, int32_t recycleTime,
                                  bool forceDelete);

    void UpdateActualUse(const Resources &oldActualUse, const Resources &newActualUse, resources::ResourceUnit &view);
    void UpdateResourceUnitActual(const std::shared_ptr<ResourceUnit> &value);
    void UpdateResourceUnitDynamic(const std::shared_ptr<ResourceUnit> &value);
    void DeleteLabels(const InstanceInfo &instInfo);
    void AddLabels(const InstanceInfo &instance);

    static void AddLabel(const InstanceInfo &instance,
                         ::google::protobuf::Map<std::string, ValueCounter> &nodeLabels,
                         bool enableTenantAffinity);

    /**
     * @brief local scheduler merge of changes from multiple revisions, the range is: (startRevision, endRevision]
     */
    void MergeResourceViewChanges(int64_t startRevision, int64_t endRevision, ResourceUnitChanges &result);

    /**
     * @brief Static version for async merge
     */
    static void MergeResourceViewChanges(const std::map<int64_t, ResourceUnitChange> &changes,
                                         const RevisionRange &range, const std::string &localId,
                                         ResourceUnitChanges &result, bool enableTenantAffinity);

    /**
     * @brief Async version: copy changes and merge in background, then send result via callback
     */
    void MergeResourceViewChangesAsync(int64_t startRevision, int64_t endRevision,
                                       const std::string &viewInitTime, const std::string &localId,
                                       const litebus::AID &replyTo);

    void StoreChange(int64_t revision, const ResourceUnitChange& change);
    void DelChanges(int64_t newStartRevision);
    static ResourceUnitChange MergeResourceUnitChanges(ResourceUnitChange &previous, const ResourceUnitChange &current,
                                                       bool enableTenantAffinity);
    static ResourceUnitChange MergeAddAndModify(ResourceUnitChange &previous, const ResourceUnitChange &current,
                                                bool enableTenantAffinity);
    static ResourceUnitChange MergeTwoModifies(ResourceUnitChange &previous, const ResourceUnitChange &current);
    static void MergeCapacityChange(ResourceUnitChange &previous, const ResourceUnitChange &current);
    static void MergeInstanceChanges(Modification &previous, const Modification &current);
    static bool IsValidChangeCombination(const InstanceChange& previous, const InstanceChange& current);
    static bool ShouldMergeInstanceChange(const InstanceChange& previous, const InstanceChange& current);
    static bool ShouldRetainInstanceChange(const InstanceChange& previous, const InstanceChange& current);
    static bool IsResourceUnitChangeEmpty(const ResourceUnitChange& change);
    static bool IsModifyEmpty(const ResourceUnitChange &modify);
    bool CheckLatestRevision(const std::shared_ptr<ResourceUnitChanges> &changes);
    void DoUpdateResourceUnitDelta(const std::string localId);
    void ConvertFullResourceviewToChanges(ResourceUnitChanges &result);
    Status HandleReportedAddition(const ResourceUnitChange &change);
    Status HandleReportedDeletion(const ResourceUnitChange &change);
    Status HandleReportedModification(const ResourceUnitChange &change);
    void ApplyReportedCapacityChange(ResourceUnit &agentResourceUnit, const CapacityChange &capacityChange);
    Status HandleReportedAddInstacne(const InstanceInfo &instance);
    Status HandleReportedDeleteInstacne(const InstanceInfo &instance);
    void SimplifyInstanceInfo(const InstanceInfo &instance, InstanceInfo &simplifiedInstance) const;
    bool HandleReportedChanges(const std::shared_ptr<ResourceUnitChanges> &resourceUnitChanges);

    void MarkResourceUpdated();
    void NotifyResourceUpdated();

    void PodRecycler(const ResourceUnit &unit);
    int32_t ParseRecyclePodLabel(const ResourceUnit &unit);

    inline void ClearAgentTenantLabels(const std::string &functionAgentID);
    inline void CancelAgentReuseTimer(const std::string &functionAgentID);
    inline void DisableAgent(const std::string &functionAgentID);
    inline void SetAgentReuseTimer(const std::string &functionAgentID, int32_t recycleTime);
    inline void OnTenantInstanceInAgentAllDeleted(const std::string &functionAgentID, int32_t recycleTime);
    inline SCHEDULER_LEVEL GetCurrentSchedulerLevel(void);

    std::string unitID_;
    std::shared_ptr<ResourceUnit> view_;
    std::shared_ptr<ResourcePoller> poller_;
    std::unordered_map<std::string, std::string> urls_;
    std::unordered_map<std::string, std::string> reqIDToUnitIDMap_;
    // key : ResourceUnit.id
    std::unordered_map<std::string, std::shared_ptr<ResourceUnitChanges>> latestReportedResourceViewChanges_;
    std::list<ResourceUpdateHandler> updateHandler_;
    std::function<void(const std::string&)> disableExecFunc_;
    std::string updateTime_ = "";
    uint64_t getResourceViewCount_ = 0;
    uint64_t lastReportedRevision_ = 0;

    bool isLocal_;
    bool isHeader_ = false;
    bool enableTenantAffinity_ = true;
    int32_t tenantPodReuseTimeWindow_;
    bool hasResourceUpdated_ = false;

    // key: agent id, value: instance id set
    std::unordered_map<std::string, std::unordered_set<std::string>> agentCacheMap_;

    // key: agent id
    std::unordered_map<std::string, litebus::Timer> reuseTimers_;
    // key: agent id
    std::unordered_map<std::string, bool> agentUsedMap_;

    // key: revision, value: changes in the current revision
    std::map<int64_t, ResourceUnitChange> versionChanges_;

    // Only used in domain
    std::unordered_map<std::string, LocalResourceViewInfo> localInfoMap_;
    // Only used in domain; key: localId; value: all instance label
    std::unordered_map<std::string, ::google::protobuf::Map<std::string, ValueCounter>> allLocalLabels_;

    std::string domainUrlForLocal_;
    std::string actorSuffix_;
    std::shared_ptr<ActorWorker> asyncWorker_;
    void SetResourceMetricsContext(const std::shared_ptr<ResourceUnitChanges> &resourceUnitChanges) const;
};

}  // namespace functionsystem::resource_view

#endif  // COMMON_RESOURCE_VIEW_RESOURCE_VIEW_ACTOR_H
