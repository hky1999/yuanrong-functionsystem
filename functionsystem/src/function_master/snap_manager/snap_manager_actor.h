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
#include "function_master/instance_manager/instance_manager.h"

#include "restore_replay_ledger.h"
#include "snapshot_cache.h"
#include "snapshot_scheduler.h"
#include "reusable_snapshot_store.h"

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
        ::core_service::SnapStartedInfo snapstartInfo{};
    };

    // Replay-ledger context for one SnapStart request, captured before the
    // async scheduling boundary. Every request is tracked: a completion may
    // only settle the entry whose identity tuple and ticket it carries.
    struct SnapStartLedgerContext {
        RestoreReplayLedger::Key key{};
        RestoreReplayLedger::Ticket ticket{0};
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
                     const SnapManagerConfig &config = SnapManagerConfig{},
                     const std::shared_ptr<instance_manager::InstanceManager> &instanceManager = nullptr);

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

    void BeginReusableSnapshotMessage(const litebus::AID &from, std::string &&name, std::string &&msg);
    void CommitReusableSnapshotMessage(const litebus::AID &from, std::string &&name, std::string &&msg);
    void FailReusableSnapshotMessage(const litebus::AID &from, std::string &&name, std::string &&msg);
    void ResolveReusableSnapshotForCreateMessage(
        const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<::messages::BeginReusableSnapshotResponse> BeginReusableSnapshot(
        const ::messages::BeginReusableSnapshotRequest &request);
    litebus::Future<::messages::CommitReusableSnapshotResponse> CommitReusableSnapshot(
        const ::messages::CommitReusableSnapshotRequest &request);
    litebus::Future<::messages::FailReusableSnapshotResponse> FailReusableSnapshot(
        const ::messages::FailReusableSnapshotRequest &request);
    litebus::Future<::messages::GetReusableSnapshotResponse> GetReusableSnapshot(
        const ::messages::GetReusableSnapshotRequest &request);
    litebus::Future<::messages::ListReusableSnapshotsResponse> ListReusableSnapshots(
        const ::messages::ListReusableSnapshotsRequest &request);
    litebus::Future<::messages::ResolveReusableSnapshotForCreateResponse> ResolveReusableSnapshotForCreate(
        const ::messages::ResolveReusableSnapshotForCreateRequest &request);
    litebus::Future<::messages::DeleteReusableSnapshotResponse> DeleteReusableSnapshot(
        const ::messages::DeleteReusableSnapshotRequest &request);
    litebus::Future<Status> DeleteLocalSnapshotsForSource(
        const std::string &tenantID, const std::string &sourceInstanceID);
    void SetReusableSnapshotArtifactDeleter(ReusableSnapshotStore::ArtifactDeleter deleter);

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

    /**
     * Single completion path for every SnapStart request. Actor-thread only:
     * first validates the response's requester identity and requestID against
     * the captured ledger context, then serializes the exact wire response,
     * stores it in the replay ledger (ticket-validated; first completion
     * wins) BEFORE sending it, so a lost response replays byte-identically on
     * retry instead of scheduling a second restore. Every non-STORED outcome
     * — mismatched identity, unknown entry, stale/double completion, or an
     * oversized response — fails closed: nothing is overwritten and nothing
     * is sent, so no larger-than-bound wire response escapes and a stale
     * callback cannot poison the original entry's replay. The identity check
     * is bookkeeping only; it proves nothing about the runtime's physical
     * execution identity.
     */
    void CompleteSnapStart(const SnapStartLedgerContext &context, const SnapStartResponse &response);

    /**
     * Send already-serialized RestoreSnapshotResponse bytes (replay path).
     */
    void SendSnapStartResponseBytes(const litebus::AID &to, std::string &&wireResponse);
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
        std::shared_ptr<instance_manager::InstanceManager> instanceManager{nullptr};
        SnapManagerConfig config;
        LeaderInfo leaderInfo;

        // Snapshot cache and scheduler (refactored components)
        SnapshotCache cache;
        std::unique_ptr<SnapshotScheduler> scheduler{nullptr};
        std::shared_ptr<ReusableSnapshotStore> reusableSnapshotStore{nullptr};

        // Bounded replay ledger for SnapStart requests, keyed by
        // (caller AID name, url, requestID). Keeps the exact request and the
        // exact terminal response of every tracked request so a lost response
        // replays byte-identically instead of scheduling a second restore.
        // Process-local only: survives master/slave role changes within this
        // Member/process, but NOT a process restart; requests retried across
        // a restart are treated as fresh. No eviction/TTL/persistence, so a
        // safe retirement/persistence design is still required before full
        // stress readiness (every distinct restore request occupies a slot
        // forever; a pressure campaign issues far more restores than trials).
        RestoreReplayLedger restoreLedger;

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
    class MasterBusiness : public Business,
                           public std::enable_shared_from_this<MasterBusiness> {
    public:
        struct PauseResumeResult {
            int32_t code{common::ERR_NONE};
            std::string message;
            ::core_service::SnapStartedInfo snapstartInfo;
        };

        MasterBusiness(const std::shared_ptr<Member> &member, const std::shared_ptr<SnapManagerActor> &actor)
            : Business(member, actor) {}
        ~MasterBusiness() override = default;

        void OnChange() override;

        void RecordSnapshotMetadata(const litebus::AID &from, std::string &&name, std::string &&msg) override;
        void SnapStartCheckpoint(const litebus::AID &from, std::string &&name, std::string &&msg) override;
        litebus::Future<Status> DeleteSnapshot(const std::string &snapshotID) override;
        void CleanupExpiredSnapshots() override;

    private:
        struct PauseResumeAttempt {
            std::string fingerprint;
            std::shared_ptr<litebus::Promise<PauseResumeResult>> result;
        };

        void HandleRecordSnapshot(const litebus::AID &from, messages::RecordSnapshotRequest &&req);
        void HandleSnapStart(const litebus::AID &from, std::shared_ptr<messages::RestoreSnapshotRequest> req);
        litebus::Future<PauseResumeResult> StartPauseResume(
            const std::shared_ptr<messages::RestoreSnapshotRequest> &req);

        static std::string BuildPauseResumeFingerprint(const messages::RestoreSnapshotRequest &req);
        static Status ValidatePauseResumeInstance(const resources::InstanceInfo &instance,
                                                  const std::string &logicalInstanceID);
        void HandleSnapStart(const litebus::AID &from,
                             std::shared_ptr<messages::RestoreSnapshotRequest> req,
                             std::string wireRequest);
        void CompleteSnapStart(const SnapStartLedgerContext &context, const SnapStartResponse &response) const;

        litebus::Future<Status> SaveMetadataToEtcd(const SnapshotMetadata &meta);
        litebus::Future<Status> DeleteMetadataFromEtcd(const std::string &snapshotID);
        void EnforceSnapshotQuota(const std::string &functionID);

        Status ValidateSnapshot(const SnapshotMetadata &meta, int64_t currentTime) const;

        void SendRecordSnapshotResponse(const litebus::AID &to, const std::string &requestID,
                                        int32_t code, const std::string &message) const;
        void SendSnapStartResponse(const SnapStartResponse &response) const;

        std::unordered_map<std::string, PauseResumeAttempt> pauseResumeAttempts_;
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

    // Shared serialization for the SnapStart response wire format
    static std::string SerializeSnapStartResponse(const SnapStartResponse &response);

    std::shared_ptr<Member> member_{nullptr};
    std::unordered_map<std::string, std::shared_ptr<Business>> businesses_;
    std::string curStatus_;
    std::shared_ptr<Business> business_{nullptr};

    friend class SnapManagerActorTest;
};

}  // namespace functionsystem::snap_manager

#endif  // FUNCTION_MASTER_SNAP_MANAGER_SNAP_MANAGER_ACTOR_H
