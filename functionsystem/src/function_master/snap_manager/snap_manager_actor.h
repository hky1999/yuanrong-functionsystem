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

#ifndef FUNCTION_MASTER_SNAP_MANAGER_SNAP_MANAGER_ACTOR_H
#define FUNCTION_MASTER_SNAP_MANAGER_SNAP_MANAGER_ACTOR_H

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <memory>

#include "actor/actor.hpp"
#include "async/future.hpp"

#include "common/constants/actor_name.h"
#include "common/explorer/explorer.h"
#include "common/leader/business_policy.h"
#include "common/status/status.h"
#include "common/proto/pb/posix/message.pb.h"
#include "meta_store_client/meta_store_client.h"
#include "meta_store_client/meta_store_struct.h"
#include "function_master/global_scheduler/global_sched.h"

#include "snapshot_cache.h"
#include "snapshot_scheduler.h"

namespace functionsystem::snap_manager {

using namespace functionsystem::explorer;
using GlobalScheduler = functionsystem::global_scheduler::GlobalSched;

// etcd key prefix for snapshot metadata
const std::string SNAPSHOT_KEY_PREFIX = "/yr/snapshot/";

// Snapshot metadata stored in etcd (protobuf)
using SnapshotMetadata = ::messages::SnapshotMetadata;

/**
 * Configuration for SnapManagerActor
 */
struct SnapManagerConfig {
    int64_t defaultTTLSeconds{7 * 24 * 3600};      // 默认7天过期
    int64_t maxSnapshotsPerFunction{10};           // 每个函数最多保留快照数
    int64_t cleanupIntervalMs{3600 * 1000};        // 清理间隔（1小时）
};

/**
 * SnapManagerActor manages snapshot metadata in function_master.
 * Supports master/slave mode with MetaStoreClient for persistence.
 */
class SnapManagerActor : public litebus::ActorBase, public std::enable_shared_from_this<SnapManagerActor> {
public:
    struct SnapStartResponse {
        litebus::AID to;
        std::string requestID{};
        int32_t code{0};
        std::string message{};
        std::string instanceID{};
        ::messages::SnapstartInfo snapstartInfo{};
    };

    SnapManagerActor() = delete;

    /**
     * Constructor
     * @param metaClient MetaStoreClient for etcd operations
     * @param globalScheduler Global scheduler for instance scheduling
     * @param config Configuration for snap manager
     */
    SnapManagerActor(const std::shared_ptr<MetaStoreClient> &metaClient,
                     const std::shared_ptr<GlobalScheduler> &globalScheduler,
                     const SnapManagerConfig &config = SnapManagerConfig{});

    ~SnapManagerActor() override = default;

    /**
     * Update leader info and switch business mode
     * @param leaderInfo Current leader information
     * @return true if update successful
     */
    bool UpdateLeaderInfo(const LeaderInfo &leaderInfo);

    /**
     * Record snapshot metadata (called via message)
     */
    void RecordSnapshotMetadata(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * Handle snapstart request (called via message)
     */
    void SnapStartCheckpoint(const litebus::AID &from, std::string &&name, std::string &&msg);
    void ListSnapshotsByFunctionKeyMessage(const litebus::AID &from, std::string &&name, std::string &&msg);
    void ListSnapshotsByTenantMessage(const litebus::AID &from, std::string &&name, std::string &&msg);
    void DeleteSnapshotMessage(const litebus::AID &from, std::string &&name, std::string &&msg);

    /**
     * Query snapshot by ID
     */
    litebus::Future<litebus::Option<SnapshotMetadata>> GetSnapshotMetadata(const std::string &snapshotID);

    /**
     * List all snapshots for a function
     */
    litebus::Future<std::vector<SnapshotMetadata>> ListSnapshotsByFunction(const std::string &functionID);

    /**
     * List all snapshots for a (tenantID, functionType, optional ns)
     */
    litebus::Future<std::vector<SnapshotMetadata>> ListSnapshotsByFunctionKey(const std::string &tenantID,
                                                                               const std::string &functionType,
                                                                               const std::string &ns = "");

    /**
     * List checkpoint IDs for a (tenantID, functionType, optional ns)
     */
    litebus::Future<std::vector<std::string>> ListCheckpointIDsByFunctionKey(const std::string &tenantID,
                                                                            const std::string &functionType,
                                                                            const std::string &ns = "");

    /**
     * List all snapshots for a tenant (all functionTypes)
     */
    litebus::Future<std::vector<SnapshotMetadata>> ListSnapshotsByTenant(const std::string &tenantID);

    /**
     * List checkpoint IDs for a tenant (all functionTypes)
     */
    litebus::Future<std::vector<std::string>> ListCheckpointIDsByTenant(const std::string &tenantID);

    /**
     * Delete a snapshot
     */
    litebus::Future<Status> DeleteSnapshot(const std::string &snapshotID);

    void SendRecordSnapshotResponse(const litebus::AID &to,
                                    const std::string &requestID,
                                    int32_t code,
                                    const std::string &message);

    void SendSnapStartResponse(const SnapStartResponse &response);
    void SendListSnapshotsByFunctionKeyResponse(const litebus::AID &to,
                                                const ::messages::ListSnapshotsByFunctionKeyResponse &rsp);
    void SendListSnapshotsByTenantResponse(const litebus::AID &to,
                                           const ::messages::ListSnapshotsByTenantResponse &rsp);
    void SendDeleteSnapshotResponse(const litebus::AID &to,
                                    const ::messages::DeleteSnapshotResponse &rsp);
protected:
    void Init() override;
    void Finalize() override;

private:
    // Internal member struct for shared state
    struct Member {
        std::shared_ptr<MetaStoreClient> client{nullptr};
        std::shared_ptr<GlobalScheduler> globalScheduler{nullptr};
        SnapManagerConfig config;
        LeaderInfo leaderInfo;

        // Snapshot cache and scheduler (refactored components)
        SnapshotCache cache;
        std::unique_ptr<SnapshotScheduler> scheduler{nullptr};

        // W10-2: requestIDs of SnapStarts whose schedule is in flight; resent
        // duplicates (local timeout retry) are absorbed instead of minting a
        // second synthetic restore instance for the same checkpoint.
        std::unordered_set<std::string> inFlightSnapStarts;

        // Watcher for etcd
        std::shared_ptr<Watcher> snapshotWatcher{nullptr};

        // Cleanup timer
        litebus::Timer cleanupTimer;
    };

    /**
     * Business policy base class for master/slave pattern
     */
    class Business : public leader::BusinessPolicy {
    public:
        Business(const std::shared_ptr<Member> &member, const std::shared_ptr<SnapManagerActor> &actor)
            : member_(member), actor_(actor) {}
        ~Business() override = default;

        virtual void RecordSnapshotMetadata(const litebus::AID &from, std::string &&name, std::string &&msg) = 0;
        virtual void SnapStartCheckpoint(const litebus::AID &from, std::string &&name, std::string &&msg) = 0;
        virtual litebus::Future<Status> DeleteSnapshot(const std::string &snapshotID) = 0;
        virtual void CleanupExpiredSnapshots() = 0;

    protected:
        std::shared_ptr<Member> member_;
        std::weak_ptr<SnapManagerActor> actor_;
    };

    /**
     * Master business handles write operations
     */
    class MasterBusiness : public Business {
    public:
        MasterBusiness(const std::shared_ptr<Member> &member, const std::shared_ptr<SnapManagerActor> &actor)
            : Business(member, actor) {}
        ~MasterBusiness() override = default;

        void OnChange() override;

        void RecordSnapshotMetadata(const litebus::AID &from, std::string &&name, std::string &&msg) override;
        void SnapStartCheckpoint(const litebus::AID &from, std::string &&name, std::string &&msg) override;
        litebus::Future<Status> DeleteSnapshot(const std::string &snapshotID) override;
        void CleanupExpiredSnapshots() override;

    private:
        void HandleRecordSnapshot(const litebus::AID &from, messages::RecordSnapshotRequest &&req);
        void HandleSnapStart(const litebus::AID &from, std::shared_ptr<messages::RestoreSnapshotRequest> req);

        litebus::Future<Status> SaveMetadataToEtcd(const SnapshotMetadata &meta);
        litebus::Future<Status> DeleteMetadataFromEtcd(const std::string &snapshotID);
        void EnforceSnapshotQuota(const std::string &functionID);

        Status ValidateSnapshot(const SnapshotMetadata &meta, int64_t currentTime) const;

        void SendRecordSnapshotResponse(const litebus::AID &to, const std::string &requestID,
                                        int32_t code, const std::string &message) const;
        void SendSnapStartResponse(const SnapStartResponse &response) const;
    };

    /**
     * Slave business forwards requests to master
     */
    class SlaveBusiness : public Business {
    public:
        SlaveBusiness(const std::shared_ptr<Member> &member, const std::shared_ptr<SnapManagerActor> &actor)
            : Business(member, actor) {}
        ~SlaveBusiness() override = default;

        void OnChange() override;

        void RecordSnapshotMetadata(const litebus::AID &from, std::string &&name, std::string &&msg) override;
        void SnapStartCheckpoint(const litebus::AID &from, std::string &&name, std::string &&msg) override;
        litebus::Future<Status> DeleteSnapshot(const std::string &snapshotID) override;
        void CleanupExpiredSnapshots() override {}
    };

    // Etcd watch and sync methods
    void GetAndWatchSnapshots();
    void OnSnapshotWatchEvent(const std::vector<WatchEvent> &events, bool synced);
    litebus::Future<SyncResult> OnSnapshotSyncer(const std::shared_ptr<GetResponse> &getResponse);
    void OnSnapshotWatch(const std::shared_ptr<Watcher> &watcher);

    // Helper methods
    SnapshotMetadata ParseSnapshotFromKV(const std::string &key, const std::string &value) const;

    // Periodic cleanup task
    void ScheduleCleanupTask();
    void DoCleanupExpiredSnapshots();

    std::shared_ptr<Member> member_{nullptr};
    std::unordered_map<std::string, std::shared_ptr<Business>> businesses_;
    std::string curStatus_;
    std::shared_ptr<Business> business_{nullptr};

    friend class SnapManagerActorTest;
};

}  // namespace functionsystem::snap_manager

#endif  // FUNCTION_MASTER_SNAP_MANAGER_SNAP_MANAGER_ACTOR_H
