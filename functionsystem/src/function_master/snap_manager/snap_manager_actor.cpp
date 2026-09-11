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

#include "snap_manager_actor.h"

#include <chrono>
#include "async/async.hpp"
#include "async/defer.hpp"
#include "common/constants/actor_name.h"
#include "common/proto/pb/posix/common.pb.h"
#include "common/logs/logging.h"
#include "common/types/instance_state.h"
#include "common/utils/generate_message.h"
#include "common/utils/resume_identity.h"
#include "common/utils/struct_transfer.h"

namespace functionsystem::snap_manager {

using namespace functionsystem::explorer;
using namespace functionsystem::leader;
using namespace std::placeholders;

namespace {

std::string ResumeRouteAddress(const resources::InstanceInfo &instance)
{
    return instance.runtimeaddress().empty() ? instance.proxygrpcaddress()
                                             : instance.runtimeaddress();
}

void PopulateResumeWinner(::core_service::SnapStartedInfo *result,
                          const resources::InstanceInfo &winner)
{
    result->set_instanceid(winner.instanceid());
    result->set_routeaddress(ResumeRouteAddress(winner));
    result->set_functionproxyid(winner.functionproxyid());
    result->set_nodeid(winner.functionproxyid());
    const auto portMappings = winner.extensions().find(PORT_FORWARD_KEY);
    if (portMappings != winner.extensions().end()) {
        result->set_portmappings(portMappings->second);
    }
    YRLOG_INFO("resume winner response instance({}) owner({}) route({}) portMappings({})",
               winner.instanceid(), winner.functionproxyid(), result->routeaddress(), result->portmappings());
}

}  // namespace

// ===========================================
// SnapManagerActor Constructor and Lifecycle
// ===========================================

SnapManagerActor::SnapManagerActor(const std::shared_ptr<MetaStoreClient> &metaClient,
                                   const std::shared_ptr<GlobalScheduler> &globalScheduler,
                                   const SnapManagerConfig &config,
                                   const std::shared_ptr<instance_manager::InstanceManager> &instanceManager)
    : ActorBase(SNAP_MANAGER_ACTOR_NAME)
{
    member_ = std::make_shared<Member>();
    member_->client = metaClient;
    member_->globalScheduler = globalScheduler;
    member_->instanceManager = instanceManager;
    member_->config = config;
    member_->scheduler = std::make_unique<SnapshotScheduler>(globalScheduler);
    member_->reusableSnapshotStore = std::make_shared<ReusableSnapshotStore>(
        std::make_shared<EtcdReusableSnapshotPersistence>(metaClient));
    if (member_->instanceManager != nullptr) {
        const auto instanceManager = member_->instanceManager;
        member_->reusableSnapshotStore->SetArtifactDeleter(
            [instanceManager](const ::messages::DeleteReusableSnapshotArtifactRequest &request) {
                return instanceManager->DeleteReusableSnapshotArtifact(request);
            });
    }
}

bool SnapManagerActor::UpdateLeaderInfo(const LeaderInfo &leaderInfo)
{
    litebus::AID masterAID(SNAP_MANAGER_ACTOR_NAME, leaderInfo.address);
    member_->leaderInfo = leaderInfo;

    auto newStatus = GetStatus(GetAID(), masterAID, curStatus_);
    if (newStatus.empty()) {
        return true;  // No change
    }

    if (businesses_.find(newStatus) == businesses_.end()) {
        YRLOG_WARN("SnapManagerActor UpdateLeaderInfo new status({}) business don't exist", newStatus);
        return false;
    }

    business_ = businesses_[newStatus];
    ASSERT_IF_NULL(business_);
    business_->OnChange();
    curStatus_ = newStatus;
    YRLOG_INFO("SnapManagerActor switched to {} mode", curStatus_);
    return true;
}

void SnapManagerActor::Init()
{
    YRLOG_INFO("init SnapManagerActor");
    ASSERT_IF_NULL(member_);
    ASSERT_IF_NULL(member_->client);
    ASSERT_IF_NULL(member_->globalScheduler);

    // Create master and slave business
    auto masterBusiness = std::make_shared<MasterBusiness>(member_, shared_from_this());
    auto slaveBusiness = std::make_shared<SlaveBusiness>(member_, shared_from_this());

    (void)businesses_.emplace(MASTER_STATUS, masterBusiness);
    (void)businesses_.emplace(SLAVE_STATUS, slaveBusiness);

    // Default to slave mode
    curStatus_ = SLAVE_STATUS;
    business_ = slaveBusiness;

    // Register message handlers
    Receive("RecordSnapshotMetadata", &SnapManagerActor::RecordSnapshotMetadata);
    Receive("SnapStartCheckpoint", &SnapManagerActor::SnapStartCheckpoint);
    Receive("ListSnapshotsByFunctionKey", &SnapManagerActor::ListSnapshotsByFunctionKeyMessage);
    Receive("ListSnapshotsByTenant", &SnapManagerActor::ListSnapshotsByTenantMessage);
    Receive("DeleteSnapshot", &SnapManagerActor::DeleteSnapshotMessage);
    Receive("BeginReusableSnapshot", &SnapManagerActor::BeginReusableSnapshotMessage);
    Receive("CommitReusableSnapshot", &SnapManagerActor::CommitReusableSnapshotMessage);
    Receive("FailReusableSnapshot", &SnapManagerActor::FailReusableSnapshotMessage);
    Receive("ResolveReusableSnapshotForCreate", &SnapManagerActor::ResolveReusableSnapshotForCreateMessage);

    // Register leader change callback
    (void)Explorer::GetInstance().AddLeaderChangedCallback(
        "SnapManager", [aid(GetAID())](const LeaderInfo &leaderInfo) {
            litebus::Async(aid, &SnapManagerActor::UpdateLeaderInfo, leaderInfo);
        });

    // Start watching snapshots from etcd
    GetAndWatchSnapshots();

    // Schedule periodic cleanup task
    ScheduleCleanupTask();
}

void SnapManagerActor::Finalize()
{
    YRLOG_INFO("finalize SnapManagerActor");
    litebus::TimerTools::Cancel(member_->cleanupTimer);
}

// ===========================================
// Public API
// ===========================================

void SnapManagerActor::RecordSnapshotMetadata(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    ASSERT_IF_NULL(business_);
    business_->RecordSnapshotMetadata(from, std::move(name), std::move(msg));
}

void SnapManagerActor::SnapStartCheckpoint(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    ASSERT_IF_NULL(business_);
    business_->SnapStartCheckpoint(from, std::move(name), std::move(msg));
}

void SnapManagerActor::ListSnapshotsByFunctionKeyMessage(const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::ListSnapshotsByFunctionKeyRequest req;
    ::messages::ListSnapshotsByFunctionKeyResponse rsp;
    if (!req.ParseFromString(msg)) {
        rsp.set_code(common::ERR_PARAM_INVALID);
        rsp.set_message("failed to parse ListSnapshotsByFunctionKeyRequest");
        SendListSnapshotsByFunctionKeyResponse(from, rsp);
        return;
    }
    rsp.set_requestid(req.requestid());
    rsp.set_code(common::ERR_NONE);
    rsp.set_message("success");
    for (const auto &id : member_->cache.GetByFunctionKeyCheckpointIDs(
        req.functionkey().tenantid(), req.functionkey().functiontype(), req.functionkey().namespace_())) {
        rsp.add_checkpointids(id);
    }
    SendListSnapshotsByFunctionKeyResponse(from, rsp);
}

void SnapManagerActor::ListSnapshotsByTenantMessage(const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::ListSnapshotsByTenantRequest req;
    ::messages::ListSnapshotsByTenantResponse rsp;
    if (!req.ParseFromString(msg)) {
        rsp.set_code(common::ERR_PARAM_INVALID);
        rsp.set_message("failed to parse ListSnapshotsByTenantRequest");
        SendListSnapshotsByTenantResponse(from, rsp);
        return;
    }
    rsp.set_requestid(req.requestid());
    rsp.set_code(common::ERR_NONE);
    rsp.set_message("success");
    for (const auto &id : member_->cache.GetByTenantCheckpointIDs(req.tenantid())) {
        rsp.add_checkpointids(id);
    }
    SendListSnapshotsByTenantResponse(from, rsp);
}

void SnapManagerActor::DeleteSnapshotMessage(const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::DeleteSnapshotRequest req;
    if (!req.ParseFromString(msg)) {
        ::messages::DeleteSnapshotResponse rsp;
        rsp.set_code(common::ERR_PARAM_INVALID);
        rsp.set_message("failed to parse DeleteSnapshotRequest");
        SendDeleteSnapshotResponse(from, rsp);
        return;
    }
    DeleteSnapshot(req.checkpointid()).OnComplete([aid(GetAID()), from, req](const litebus::Future<Status> &future) {
        ::messages::DeleteSnapshotResponse rsp;
        rsp.set_requestid(req.requestid());
        if (future.IsError()) {
            rsp.set_code(common::ERR_INNER_SYSTEM_ERROR);
            rsp.set_message("delete snapshot request failed");
        } else {
            rsp.set_code(static_cast<common::ErrorCode>(future.Get().StatusCode()));
            rsp.set_message(future.Get().RawMessage());
        }
        litebus::Async(aid, &SnapManagerActor::SendDeleteSnapshotResponse, from, rsp);
    });
}

void SnapManagerActor::BeginReusableSnapshotMessage(
    const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::BeginReusableSnapshotRequest request;
    if (!request.ParseFromString(msg)) {
        ::messages::BeginReusableSnapshotResponse response;
        response.set_code(common::ERR_PARAM_INVALID);
        response.set_message("failed to parse BeginReusableSnapshotRequest");
        Send(from, "BeginReusableSnapshotResponse", response.SerializeAsString());
        return;
    }
    BeginReusableSnapshot(request).OnComplete(
        [weak = weak_from_this(), from, requestID = request.requestid()](
            const litebus::Future<::messages::BeginReusableSnapshotResponse> &future) {
            auto actor = weak.lock();
            if (actor == nullptr) {
                return;
            }
            ::messages::BeginReusableSnapshotResponse response;
            if (future.IsError()) {
                response.set_requestid(requestID);
                response.set_code(common::ERR_INNER_SYSTEM_ERROR);
                response.set_message("reusable Snapshot begin failed");
            } else {
                response = future.Get();
            }
            actor->Send(from, "BeginReusableSnapshotResponse", response.SerializeAsString());
        });
}

void SnapManagerActor::CommitReusableSnapshotMessage(
    const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::CommitReusableSnapshotRequest request;
    if (!request.ParseFromString(msg)) {
        ::messages::CommitReusableSnapshotResponse response;
        response.set_code(common::ERR_PARAM_INVALID);
        response.set_message("failed to parse CommitReusableSnapshotRequest");
        Send(from, "CommitReusableSnapshotResponse", response.SerializeAsString());
        return;
    }
    CommitReusableSnapshot(request).OnComplete(
        [weak = weak_from_this(), from, requestID = request.requestid()](
            const litebus::Future<::messages::CommitReusableSnapshotResponse> &future) {
            auto actor = weak.lock();
            if (actor == nullptr) {
                return;
            }
            ::messages::CommitReusableSnapshotResponse response;
            if (future.IsError()) {
                response.set_requestid(requestID);
                response.set_code(common::ERR_INNER_SYSTEM_ERROR);
                response.set_message("reusable Snapshot commit failed");
            } else {
                response = future.Get();
            }
            actor->Send(from, "CommitReusableSnapshotResponse", response.SerializeAsString());
        });
}

void SnapManagerActor::FailReusableSnapshotMessage(
    const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::FailReusableSnapshotRequest request;
    if (!request.ParseFromString(msg)) {
        ::messages::FailReusableSnapshotResponse response;
        response.set_code(common::ERR_PARAM_INVALID);
        response.set_message("failed to parse FailReusableSnapshotRequest");
        Send(from, "FailReusableSnapshotResponse", response.SerializeAsString());
        return;
    }
    FailReusableSnapshot(request).OnComplete(
        [weak = weak_from_this(), from, requestID = request.requestid()](
            const litebus::Future<::messages::FailReusableSnapshotResponse> &future) {
            auto actor = weak.lock();
            if (actor == nullptr) {
                return;
            }
            ::messages::FailReusableSnapshotResponse response;
            if (future.IsError()) {
                response.set_requestid(requestID);
                response.set_code(common::ERR_INNER_SYSTEM_ERROR);
                response.set_message("reusable Snapshot failure reconciliation failed");
            } else {
                response = future.Get();
            }
            actor->Send(from, "FailReusableSnapshotResponse", response.SerializeAsString());
        });
}

void SnapManagerActor::ResolveReusableSnapshotForCreateMessage(
    const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::ResolveReusableSnapshotForCreateRequest request;
    if (!request.ParseFromString(msg)) {
        ::messages::ResolveReusableSnapshotForCreateResponse response;
        response.set_code(common::ERR_PARAM_INVALID);
        response.set_message("failed to parse ResolveReusableSnapshotForCreateRequest");
        Send(from, "ResolveReusableSnapshotForCreateResponse", response.SerializeAsString());
        return;
    }
    ResolveReusableSnapshotForCreate(request).OnComplete(
        [weak = weak_from_this(), from, requestID = request.requestid()](
            const litebus::Future<::messages::ResolveReusableSnapshotForCreateResponse> &future) {
            auto actor = weak.lock();
            if (actor == nullptr) {
                return;
            }
            ::messages::ResolveReusableSnapshotForCreateResponse response;
            if (future.IsError()) {
                response.set_requestid(requestID);
                response.set_code(common::ERR_INNER_SYSTEM_ERROR);
                response.set_message("reusable Snapshot resolve failed");
            } else {
                response = future.Get();
            }
            actor->Send(from, "ResolveReusableSnapshotForCreateResponse", response.SerializeAsString());
        });
}

litebus::Future<::messages::BeginReusableSnapshotResponse> SnapManagerActor::BeginReusableSnapshot(
    const ::messages::BeginReusableSnapshotRequest &request)
{
    ASSERT_IF_NULL(member_->reusableSnapshotStore);
    return member_->reusableSnapshotStore->Begin(request);
}

litebus::Future<::messages::CommitReusableSnapshotResponse> SnapManagerActor::CommitReusableSnapshot(
    const ::messages::CommitReusableSnapshotRequest &request)
{
    ASSERT_IF_NULL(member_->reusableSnapshotStore);
    return member_->reusableSnapshotStore->Commit(request);
}

litebus::Future<::messages::FailReusableSnapshotResponse> SnapManagerActor::FailReusableSnapshot(
    const ::messages::FailReusableSnapshotRequest &request)
{
    ASSERT_IF_NULL(member_->reusableSnapshotStore);
    return member_->reusableSnapshotStore->Fail(request);
}

litebus::Future<::messages::GetReusableSnapshotResponse> SnapManagerActor::GetReusableSnapshot(
    const ::messages::GetReusableSnapshotRequest &request)
{
    ASSERT_IF_NULL(member_->reusableSnapshotStore);
    return member_->reusableSnapshotStore->Get(request);
}

litebus::Future<::messages::ListReusableSnapshotsResponse> SnapManagerActor::ListReusableSnapshots(
    const ::messages::ListReusableSnapshotsRequest &request)
{
    ASSERT_IF_NULL(member_->reusableSnapshotStore);
    return member_->reusableSnapshotStore->List(request);
}

litebus::Future<::messages::ResolveReusableSnapshotForCreateResponse>
SnapManagerActor::ResolveReusableSnapshotForCreate(
    const ::messages::ResolveReusableSnapshotForCreateRequest &request)
{
    ASSERT_IF_NULL(member_->reusableSnapshotStore);
    return member_->reusableSnapshotStore->Resolve(request);
}

litebus::Future<::messages::DeleteReusableSnapshotResponse> SnapManagerActor::DeleteReusableSnapshot(
    const ::messages::DeleteReusableSnapshotRequest &request)
{
    ASSERT_IF_NULL(member_->reusableSnapshotStore);
    return member_->reusableSnapshotStore->Delete(request);
}

litebus::Future<Status> SnapManagerActor::DeleteLocalSnapshotsForSource(
    const std::string &tenantID, const std::string &sourceInstanceID)
{
    ASSERT_IF_NULL(member_->reusableSnapshotStore);
    return member_->reusableSnapshotStore->DeleteLocalSnapshotsForSource(tenantID, sourceInstanceID);
}

void SnapManagerActor::SetReusableSnapshotArtifactDeleter(ReusableSnapshotStore::ArtifactDeleter deleter)
{
    ASSERT_IF_NULL(member_->reusableSnapshotStore);
    member_->reusableSnapshotStore->SetArtifactDeleter(std::move(deleter));
}

litebus::Future<litebus::Option<SnapshotMetadata>> SnapManagerActor::GetSnapshotMetadata(const std::string &snapshotID)
{
    auto meta = member_->cache.Get(snapshotID);
    if (meta.has_value()) {
        return litebus::Option<SnapshotMetadata>(meta.value());
    }
    return litebus::None();
}

litebus::Future<std::vector<SnapshotMetadata>> SnapManagerActor::ListSnapshotsByFunction(const std::string &functionID)
{
    return member_->cache.GetByFunction(functionID);
}

litebus::Future<std::vector<SnapshotMetadata>> SnapManagerActor::ListSnapshotsByFunctionKey(
    const std::string &tenantID, const std::string &functionType, const std::string &ns)
{
    return member_->cache.GetByFunctionKey(tenantID, functionType, ns);
}

litebus::Future<std::vector<std::string>> SnapManagerActor::ListCheckpointIDsByFunctionKey(
    const std::string &tenantID, const std::string &functionType, const std::string &ns)
{
    return member_->cache.GetByFunctionKeyCheckpointIDs(tenantID, functionType, ns);
}

litebus::Future<std::vector<SnapshotMetadata>> SnapManagerActor::ListSnapshotsByTenant(const std::string &tenantID)
{
    return member_->cache.GetByTenant(tenantID);
}

litebus::Future<std::vector<std::string>> SnapManagerActor::ListCheckpointIDsByTenant(const std::string &tenantID)
{
    return member_->cache.GetByTenantCheckpointIDs(tenantID);
}

litebus::Future<Status> SnapManagerActor::DeleteSnapshot(const std::string &snapshotID)
{
    ASSERT_IF_NULL(business_);
    return business_->DeleteSnapshot(snapshotID);
}

// ===========================================
// Etcd Watch and Sync
// ===========================================

void SnapManagerActor::GetAndWatchSnapshots()
{
    auto observer = [aid(GetAID())](const std::vector<WatchEvent> &events, bool synced) -> bool {
        litebus::Async(aid, &SnapManagerActor::OnSnapshotWatchEvent, events, synced);
        return true;
    };

    auto syncer = [aid(GetAID())](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<SyncResult> {
        return litebus::Async(aid, &SnapManagerActor::OnSnapshotSyncer, getResponse);
    };

    (void)member_->client
        ->GetAndWatch(SNAPSHOT_KEY_PREFIX, {.prefix = true, .prevKv = true}, observer, syncer)
        .Then([aid(GetAID())](const std::shared_ptr<Watcher> &watcher) -> litebus::Future<Status> {
            litebus::Async(aid, &SnapManagerActor::OnSnapshotWatch, watcher);
            return Status::OK();
        });
}

void SnapManagerActor::OnSnapshotWatchEvent(const std::vector<WatchEvent> &events, bool synced)
{
    for (const auto &event : events) {
        switch (event.eventType) {
            case EVENT_TYPE_PUT: {
                auto meta = ParseSnapshotFromKV(event.kv.key(), event.kv.value());
                const auto &snapshotID = meta.snapshotinfo().checkpointid();
                if (!snapshotID.empty()) {
                    member_->cache.Put(snapshotID, meta);
                    YRLOG_DEBUG("snapshot {} put event processed", snapshotID);
                }
                break;
            }
            case EVENT_TYPE_DELETE: {
                std::string snapshotID = event.kv.key().substr(SNAPSHOT_KEY_PREFIX.length());
                member_->cache.Remove(snapshotID);
                YRLOG_DEBUG("snapshot {} delete event processed", snapshotID);
                break;
            }
            default:
                break;
        }
    }
}

litebus::Future<SyncResult> SnapManagerActor::OnSnapshotSyncer(const std::shared_ptr<GetResponse> &getResponse)
{
    if (getResponse == nullptr) {
        YRLOG_ERROR("OnSnapshotSyncer: getResponse is null");
        return SyncResult{Status(StatusCode::FAILED, "getResponse is null")};
    }

    YRLOG_INFO("syncing {} snapshots from etcd", getResponse->kvs.size());

    for (const auto &kv : getResponse->kvs) {
        auto meta = ParseSnapshotFromKV(kv.key(), kv.value());
        const auto &snapshotID = meta.snapshotinfo().checkpointid();
        if (!snapshotID.empty()) {
            member_->cache.Put(snapshotID, meta);
        }
    }

    YRLOG_INFO("snapshot sync completed, total {} snapshots in cache", member_->cache.Size());
    return SyncResult{Status::OK()};
}

void SnapManagerActor::OnSnapshotWatch(const std::shared_ptr<Watcher> &watcher)
{
    member_->snapshotWatcher = watcher;
}

// ===========================================
// Helper Methods
// ===========================================

SnapshotMetadata SnapManagerActor::ParseSnapshotFromKV(const std::string &key, const std::string &value) const
{
    SnapshotMetadata meta;
    if (!meta.ParseFromString(value)) {
        YRLOG_ERROR("failed to parse snapshot metadata from key {}", key);
        return meta;
    }
    if (meta.snapshotinfo().checkpointid().empty() && key.size() > SNAPSHOT_KEY_PREFIX.size()) {
        meta.mutable_snapshotinfo()->set_checkpointid(key.substr(SNAPSHOT_KEY_PREFIX.size()));
    }
    return meta;
}

void SnapManagerActor::ScheduleCleanupTask()
{
    member_->cleanupTimer = litebus::AsyncAfter(
        member_->config.cleanupIntervalMs,
        GetAID(),
        &SnapManagerActor::DoCleanupExpiredSnapshots);
}

void SnapManagerActor::DoCleanupExpiredSnapshots()
{
    ASSERT_IF_NULL(business_);
    business_->CleanupExpiredSnapshots();

    // Reschedule next cleanup
    ScheduleCleanupTask();
}

void SnapManagerActor::SendRecordSnapshotResponse(const litebus::AID &to,
                                                  const std::string &requestID,
                                                  int32_t code,
                                                  const std::string &message)
{
    messages::RecordSnapshotResponse rsp;
    rsp.set_requestid(requestID);
    rsp.set_code(code);
    rsp.set_message(message);
    Send(to, "RecordSnapshotMetadataResponse", rsp.SerializeAsString());
}

std::string SnapManagerActor::SerializeSnapStartResponse(const SnapStartResponse &response)
{
    messages::RestoreSnapshotResponse rsp;
    rsp.set_requestid(response.requestID);
    rsp.set_code(response.code);
    rsp.set_message(response.message);
    if (response.snapstartInfo.ByteSizeLong() > 0) {
        rsp.mutable_snapstartinfo()->CopyFrom(response.snapstartInfo);
    }
    return rsp.SerializeAsString();
}

void SnapManagerActor::SendSnapStartResponse(const SnapStartResponse &response)
{
    Send(response.to, "SnapStartCheckpointResponse", SerializeSnapStartResponse(response));
}

void SnapManagerActor::SendSnapStartResponseBytes(const litebus::AID &to, std::string &&wireResponse)
{
    Send(to, "SnapStartCheckpointResponse", std::move(wireResponse));
}

void SnapManagerActor::CompleteSnapStart(const SnapStartLedgerContext &context,
                                         const SnapStartResponse &response)
{
    // The response may only settle the ledger entry it was minted for. Check
    // the captured requester identity tuple and requestID before any ledger
    // mutation or send: a stale or mismatched callback must not poison the
    // entry's replay. This is bookkeeping identity only; it proves nothing
    // about the runtime's physical execution identity.
    if (!RestoreReplayLedger::MatchesResponder(context.key, response.to.Name(), response.to.Url(),
                                               response.requestID)) {
        YRLOG_WARN("ignoring snapstart completion that does not match its ledger entry, requestID={}",
                   context.key.requestID);
        return;
    }

    std::string wireResponse = SerializeSnapStartResponse(response);
    auto status = member_->restoreLedger.Complete(context.key, context.ticket, wireResponse);
    if (status != RestoreReplayLedger::CompleteStatus::STORED) {
        // Fail closed on every non-first completion: the stored response is
        // never overwritten and no duplicate response is sent. An oversized
        // response is neither stored nor sent — the entry stays in flight
        // holding its reservation, so retries keep being absorbed instead of
        // dispatching a second restore — and no larger-than-bound wire
        // response escapes the master.
        YRLOG_WARN("ignoring snapstart completion for requestID {}, ledger status={}",
                   context.key.requestID, static_cast<int>(status));
        return;
    }
    SendSnapStartResponseBytes(response.to, std::move(wireResponse));
}

void SnapManagerActor::SendListSnapshotsByFunctionKeyResponse(
    const litebus::AID &to, const ::messages::ListSnapshotsByFunctionKeyResponse &rsp)
{
    Send(to, "ListSnapshotsByFunctionKeyResponse", rsp.SerializeAsString());
}

void SnapManagerActor::SendListSnapshotsByTenantResponse(
    const litebus::AID &to, const ::messages::ListSnapshotsByTenantResponse &rsp)
{
    Send(to, "ListSnapshotsByTenantResponse", rsp.SerializeAsString());
}

void SnapManagerActor::SendDeleteSnapshotResponse(const litebus::AID &to,
                                                  const ::messages::DeleteSnapshotResponse &rsp)
{
    Send(to, "DeleteSnapshotResponse", rsp.SerializeAsString());
}

// ===========================================
// MasterBusiness Implementation
// ===========================================

void SnapManagerActor::MasterBusiness::OnChange()
{
    YRLOG_INFO("SnapManagerActor switched to MASTER mode");
}

void SnapManagerActor::MasterBusiness::RecordSnapshotMetadata(const litebus::AID &from,
                                                              std::string &&name,
                                                              std::string &&msg)
{
    messages::RecordSnapshotRequest req;
    if (!req.ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse RecordSnapshotRequest");
        SendRecordSnapshotResponse(from, "", common::ERR_PARAM_INVALID, "failed to parse request");
        return;
    }
    HandleRecordSnapshot(from, std::move(req));
}

void SnapManagerActor::MasterBusiness::SnapStartCheckpoint(const litebus::AID &from,
                                                           std::string &&name,
                                                           std::string &&msg)
{
    auto req = std::make_shared<messages::RestoreSnapshotRequest>();
    if (!req->ParseFromString(msg)) {
        // Unparseable messages are answered directly: no requestID can be
        // extracted, so no ledger entry exists and the scheduler is never
        // reached.
        YRLOG_ERROR("failed to parse RestoreSnapshotRequest");
        SendSnapStartResponse({from, "", common::ERR_PARAM_INVALID, "failed to parse request"});
        return;
    }
    // Hand down the exact wire bytes: the replay ledger compares retries
    // against them, so a requestID reused with a different checkpoint/options
    // payload surfaces as CONFLICT instead of being silently absorbed.
    HandleSnapStart(from, req, std::move(msg));
}

litebus::Future<Status> SnapManagerActor::MasterBusiness::DeleteSnapshot(const std::string &snapshotID)
{
    YRLOG_INFO("deleting snapshot: {}", snapshotID);
    return DeleteMetadataFromEtcd(snapshotID);
}

void SnapManagerActor::MasterBusiness::CleanupExpiredSnapshots()
{
    YRLOG_INFO("starting cleanup of expired snapshots");

    int64_t currentTime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    std::vector<std::string> expiredSnapshots;

    // Scan all snapshots for expiration
    auto allSnapshots = member_->cache.GetAllSnapshotsWithTime();
    for (const auto &[createTime, snapshotID, meta] : allSnapshots) {
        Status validationStatus = ValidateSnapshot(meta, currentTime);
        if (validationStatus.IsError()) {
            expiredSnapshots.push_back(snapshotID);
        }
    }

    YRLOG_INFO("found {} expired snapshots to delete", expiredSnapshots.size());

    for (const auto &snapshotID : expiredSnapshots) {
        DeleteMetadataFromEtcd(snapshotID)
            .Then([snapshotID](const Status &status) -> Status {
                if (status.IsOk()) {
                    YRLOG_INFO("expired snapshot {} deleted", snapshotID);
                } else {
                    YRLOG_ERROR("failed to delete expired snapshot {}: {}", snapshotID, status.GetMessage());
                }
                return Status::OK();
            });
    }
}

void SnapManagerActor::MasterBusiness::HandleRecordSnapshot(const litebus::AID &from,
                                                            messages::RecordSnapshotRequest &&req)
{
    // Build snapshot metadata
    SnapshotMetadata meta;
    *meta.mutable_snapshotinfo() = std::move(*req.mutable_snapshotinfo());

    // Set TTL if not already set
    if (meta.snapshotinfo().ttlseconds() <= 0) {
        meta.mutable_snapshotinfo()->set_ttlseconds(static_cast<int32_t>(member_->config.defaultTTLSeconds));
    }

    *meta.mutable_instanceinfo() = std::move(*req.mutable_instanceinfo());

    // Populate FunctionKey: prefer request-supplied values, fall back to instanceInfo
    auto *fk = meta.mutable_functionkey();
    fk->set_tenantid(meta.instanceinfo().tenantid());
    if (req.has_functionkey() && !req.functionkey().functiontype().empty()) {
        fk->set_functiontype(req.functionkey().functiontype());
    } else {
        fk->set_functiontype(meta.instanceinfo().function());
    }
    if (req.has_functionkey() && !req.functionkey().namespace_().empty()) {
        fk->set_namespace_(req.functionkey().namespace_());
    }

    const auto &requestID = req.requestid();

    // Validate required fields
    const auto &snapshotID = meta.snapshotinfo().checkpointid();
    if (snapshotID.empty()) {
        YRLOG_ERROR("RecordSnapshotMetadata: snapshotID is empty, requestID={}", requestID);
        SendRecordSnapshotResponse(from, requestID, common::ERR_PARAM_INVALID, "snapshotID is required");
        return;
    }

    YRLOG_INFO("recording snapshot metadata: snapshotID={}, storage={}, size={}",
               snapshotID, meta.snapshotinfo().storage(), meta.snapshotinfo().size());

    // Save to etcd
    SaveMetadataToEtcd(meta)
        .OnComplete([weakActor(actor_), from, snapshotID, requestID](const litebus::Future<Status> &future) {
            ASSERT_FS(future.IsOK());
            auto actor = weakActor.lock();
            if (!actor) {
                return;
            }
            auto status = future.Get();
            if (status.IsOk()) {
                YRLOG_INFO("snapshot metadata recorded successfully: {}, requestID={}", snapshotID, requestID);
            } else {
                YRLOG_ERROR("failed to record snapshot metadata: {}, requestID={}, error: {}",
                            snapshotID, requestID, status.GetMessage());
            }
            litebus::Async(actor->GetAID(), &SnapManagerActor::SendRecordSnapshotResponse,
                           from, requestID, status.StatusCode(), status.RawMessage());
        });
}

void SnapManagerActor::MasterBusiness::HandleSnapStart(const litebus::AID &from,
                                                       std::shared_ptr<messages::RestoreSnapshotRequest> req,
                                                       std::string wireRequest)
{
    if (req->snapstartoptions().type() == common::PAUSE_RESUME) {
        StartPauseResume(req).OnComplete(
            [weakActor(actor_), from, req](const litebus::Future<PauseResumeResult> &future) {
                auto actor = weakActor.lock();
                if (!actor) {
                    return;
                }
                PauseResumeResult result;
                if (future.IsError()) {
                    result.code = future.GetErrorCode();
                    result.message = "failed to process pause resume request";
                } else {
                    result = future.Get();
                }
                litebus::Async(actor->GetAID(), &SnapManagerActor::SendSnapStartResponse,
                               SnapStartResponse{from, req->requestid(), result.code, result.message,
                                                 result.snapstartInfo});
            });
        return;
    }

    const auto &snapshotID = req->checkpointid();
    // Capture by value: everything crossing the async scheduling boundary
    // below is immutable copied state, never views into the shared request.
    std::string requestID = req->requestid();
    YRLOG_INFO("processing snapstart request for snapshot: {}", snapshotID);

    // Consult the replay ledger BEFORE metadata cache validation so a
    // completed restore still replays even if the snapshot metadata has since
    // expired or been removed. Every request must Begin here — including an
    // empty requestID, which the ledger REJECTS before any metadata lookup or
    // scheduler call, so nothing schedules without replay protection.
    // Keyed by (caller name, url, requestID): retries of the same request
    // share the entry, distinct callers are isolated.
    SnapStartLedgerContext ctx;
    ctx.key = RestoreReplayLedger::Key{from.Name(), from.Url(), requestID};
    auto begin = member_->restoreLedger.Begin(ctx.key, wireRequest);
    switch (begin.status) {
        case RestoreReplayLedger::BeginStatus::REPLAY:
            // Same request already completed: resend the exact stored wire
            // bytes; no new schedule is dispatched.
            YRLOG_INFO("snapstart requestID {} for snapshot {} replaying stored response",
                       requestID, snapshotID);
            if (auto actor = actor_.lock(); actor) {
                actor->SendSnapStartResponseBytes(from, std::move(begin.response));
            }
            return;
        case RestoreReplayLedger::BeginStatus::IN_FLIGHT:
            // Same logical restore already executing (e.g. local timeout
            // retry): absorb the resend; the first attempt's completion
            // already carries this retry's answer.
            YRLOG_WARN("snapstart requestID {} for snapshot {} resent while in flight, absorbing duplicate",
                       requestID, snapshotID);
            return;
        case RestoreReplayLedger::BeginStatus::CONFLICT:
            // Same requestID with a different payload. Generic error only:
            // no stored request/response payload is echoed back.
            YRLOG_WARN("snapstart requestID {} resent with a different payload, rejecting", requestID);
            SendSnapStartResponse({from, requestID, common::ERR_PARAM_INVALID,
                                   "requestID already used by a different snapstart request"});
            return;
        case RestoreReplayLedger::BeginStatus::REJECTED:
            // Malformed or oversized request identity (e.g. empty requestID).
            // Generic error only; no metadata or scheduler call follows.
            YRLOG_WARN("snapstart request for snapshot {} rejected: invalid request identity", snapshotID);
            SendSnapStartResponse({from, requestID, common::ERR_PARAM_INVALID,
                                   "snapstart requestID is required and must be bounded"});
            return;
        case RestoreReplayLedger::BeginStatus::FULL:
            // The bounded in-memory ledger is exhausted. Old entries are
            // never evicted to make room: the caller sees busy. This needs a
            // safe retirement/persistence design before full stress runs.
            YRLOG_WARN("snapstart replay ledger full, refusing requestID {} for snapshot {}",
                       requestID, snapshotID);
            SendSnapStartResponse({from, requestID, common::ERR_INSTANCE_BUSY,
                                   "snapstart replay ledger is full"});
            return;
        case RestoreReplayLedger::BeginStatus::NEW:
            ctx.ticket = begin.ticket;
            break;
    }

    // Look up snapshot metadata from cache. Terminal validation failures are
    // completed through the ledger too: a retry of the same request replays
    // the cached failure instead of scheduling again.
    auto metaOpt = member_->cache.Get(snapshotID);
    if (!metaOpt.has_value()) {
        YRLOG_ERROR("snapshot not found: {}", snapshotID);
        CompleteSnapStart(ctx, {from, requestID, common::ERR_INSTANCE_NOT_FOUND, "snapshot not found"});
        return;
    }

    const auto &meta = metaOpt.value();

    // Validate snapshot (check expiration)
    int64_t currentTime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    Status validationStatus = ValidateSnapshot(meta, currentTime);
    if (validationStatus.IsError()) {
        YRLOG_ERROR("snapshot validation failed: {}", validationStatus.GetMessage());
        CompleteSnapStart(ctx, {from, requestID, common::ERR_PARAM_INVALID, validationStatus.GetMessage()});
        return;
    }

    // Build ScheduleRequest from snapshot metadata
    auto scheduleReq = member_->scheduler->BuildScheduleRequest(meta, *req);

    // Invoke global scheduler. Everything the response depends on (requester
    // identity tuple, ledger ticket, requestID value, built schedule request,
    // snapshot metadata) is captured here, before the async boundary: a later
    // retry must never rebuild the stored response from fresh mutable request
    // or instance state.
    member_->scheduler->Schedule(scheduleReq).OnComplete(
        [weakActor(actor_), ctx, from, requestID, scheduleReq, meta](const litebus::Future<Status> &future) {
            auto actor = weakActor.lock();
            if (!actor) {
                return;  // stale callback: fail closed, nothing is sent or stored
            }
            // An unknown scheduling failure is cached as this request's
            // terminal answer; this makes no claim about whether the restore
            // physically executed on the node.
            auto code = future.IsError() ? future.GetErrorCode() : future.Get().StatusCode();
            auto message = future.IsError() ? "failed to schedule." : future.Get().RawMessage();
            ::core_service::SnapStartedInfo info;
            info.set_instanceid(scheduleReq->instance().instanceid());
            if (!scheduleReq->instance().runtimeaddress().empty()) {
                info.set_routeaddress(scheduleReq->instance().runtimeaddress());
            }
            if (!scheduleReq->instance().functionproxyid().empty()) {
                info.set_functionproxyid(scheduleReq->instance().functionproxyid());
                info.set_nodeid(scheduleReq->instance().functionproxyid());
            }
            if (meta.has_functionkey() && !meta.functionkey().namespace_().empty()) {
                info.set_namespace_(meta.functionkey().namespace_());
            }
            // Single completion path, always posted to the actor thread (no
            // raw cross-thread ledger mutation): the requester identity is
            // re-validated against the captured context, and the exact wire
            // response is stored in the ledger, ticket-validated, BEFORE it
            // is sent.
            ::core_service::SnapStartedInfo startedInfo = info;
            startedInfo.set_instanceid(scheduleReq->instance().instanceid());
            litebus::Async(actor->GetAID(), &SnapManagerActor::CompleteSnapStart,
                           ctx, SnapStartResponse{from, requestID, code, message, startedInfo});
        });
}

std::string SnapManagerActor::MasterBusiness::BuildPauseResumeFingerprint(
    const messages::RestoreSnapshotRequest &req)
{
    const auto schedulingOptions = req.snapstartoptions().scheduleopts().SerializeAsString();
    return std::to_string(req.checkpointid().size()) + ":" + req.checkpointid() + ":" +
           std::to_string(static_cast<int32_t>(req.snapstartoptions().type())) + ":" +
           std::to_string(schedulingOptions.size()) + ":" + schedulingOptions;
}

Status SnapManagerActor::MasterBusiness::ValidatePauseResumeInstance(
    const resources::InstanceInfo &instance, const std::string &logicalInstanceID)
{
    if (instance.instanceid() != logicalInstanceID) {
        return Status(StatusCode::ERR_PARAM_INVALID, "authoritative instance ID mismatch");
    }
    if (instance.instancestatus().code() != static_cast<int32_t>(InstanceState::PAUSED)) {
        return Status(StatusCode::ERR_PARAM_INVALID, "authoritative instance is not paused");
    }
    if (!resume_identity::IsAuthoritativePausedControlIdentity(instance)) {
        return Status(StatusCode::ERR_PARAM_INVALID, "paused instance control identity is invalid");
    }
    if (instance.requestid().empty()) {
        return Status(StatusCode::ERR_PARAM_INVALID, "logical request ID is empty");
    }
    if (instance.tenantid().empty()) {
        return Status(StatusCode::ERR_PARAM_INVALID, "tenant ID is empty");
    }
    if (instance.version() <= 0) {
        return Status(StatusCode::ERR_PARAM_INVALID, "instance version is invalid");
    }
    if (!instance.has_snapshotinfo()) {
        return Status(StatusCode::ERR_PARAM_INVALID, "snapshot info is missing");
    }
    if (!resume_identity::IsCompleteReadySnapshot(instance.snapshotinfo())) {
        return Status(StatusCode::ERR_PARAM_INVALID, "snapshot metadata is incomplete");
    }
    return Status::OK();
}

litebus::Future<SnapManagerActor::MasterBusiness::PauseResumeResult>
SnapManagerActor::MasterBusiness::StartPauseResume(
    const std::shared_ptr<messages::RestoreSnapshotRequest> &req)
{
    if (req == nullptr || req->requestid().empty() || req->checkpointid().empty()) {
        return PauseResumeResult{common::ERR_PARAM_INVALID, "pause resume identity is empty", {}};
    }

    const auto fingerprint = BuildPauseResumeFingerprint(*req);
    auto existing = pauseResumeAttempts_.find(req->requestid());
    if (existing != pauseResumeAttempts_.end()) {
        if (existing->second.fingerprint != fingerprint) {
            return PauseResumeResult{static_cast<int32_t>(StatusCode::SCHEDULE_CONFLICTED),
                                     "target attempt conflicts with an existing pause resume request", {}};
        }
        return existing->second.result->GetFuture();
    }

    auto resultPromise = std::make_shared<litebus::Promise<PauseResumeResult>>();
    auto resultFuture = resultPromise->GetFuture();
    pauseResumeAttempts_.emplace(req->requestid(), PauseResumeAttempt{fingerprint, resultPromise});
    std::weak_ptr<MasterBusiness> weakSelf = shared_from_this();
    auto complete = [weakSelf, attemptID(req->requestid()), resultPromise](PauseResumeResult result) {
        if (auto self = weakSelf.lock()) {
            auto iter = self->pauseResumeAttempts_.find(attemptID);
            if (iter != self->pauseResumeAttempts_.end() && iter->second.result == resultPromise) {
                self->pauseResumeAttempts_.erase(iter);
            }
        }
        resultPromise->SetValue(std::move(result));
    };
    if (member_->instanceManager == nullptr) {
        complete(PauseResumeResult{
            common::ERR_INNER_SYSTEM_ERROR, "instance manager is unavailable", {}});
        return resultFuture;
    }

    member_->instanceManager->GetInstanceInfoByInstanceID(req->checkpointid()).OnComplete(
        [member(member_), req, complete](const litebus::Future<instance_manager::InstanceKeyInfoPair> &future) {
            if (future.IsError()) {
                complete(PauseResumeResult{
                    future.GetErrorCode(), "failed to read authoritative instance", {}});
                return;
            }
            const auto &instance = future.Get().second;
            if (instance == nullptr) {
                complete(PauseResumeResult{
                    common::ERR_INSTANCE_NOT_FOUND, "authoritative instance not found", {}});
                return;
            }
            if (resume_identity::IsCommittedResumeWinner(
                    *instance, req->checkpointid(), req->requestid())) {
                if (instance->has_snapshotinfo()
                    && resume_identity::IsCompleteReadySnapshot(instance->snapshotinfo())) {
                    auto replay = member->scheduler->BuildPauseResumeScheduleRequest(*instance, *req);
                    (void)member->scheduler->Schedule(replay);
                }
                PauseResumeResult result;
                result.code = common::ERR_NONE;
                result.message = "pause resume attempt already committed";
                PopulateResumeWinner(&result.snapstartInfo, *instance);
                complete(std::move(result));
                return;
            }
            auto validation = ValidatePauseResumeInstance(*instance, req->checkpointid());
            if (validation.IsError()) {
                complete(PauseResumeResult{
                    common::ERR_PARAM_INVALID, validation.RawMessage(), {}});
                return;
            }

            auto scheduleReq = member->scheduler->BuildPauseResumeScheduleRequest(*instance, *req);
            member->scheduler->Schedule(scheduleReq).OnComplete(
                [member, scheduleReq, req, complete](const litebus::Future<Status> &scheduleFuture) {
                    if (scheduleFuture.IsError()) {
                        complete(PauseResumeResult{scheduleFuture.GetErrorCode(),
                                                   "failed to schedule pause resume attempt", {}});
                        return;
                    }
                    const auto &scheduleStatus = scheduleFuture.Get();
                    if (scheduleStatus.IsError()) {
                        complete(PauseResumeResult{static_cast<int32_t>(scheduleStatus.StatusCode()),
                                                   scheduleStatus.RawMessage(), {}});
                        return;
                    }
                    // Cross-node scheduling does not mutate the Master's ScheduleRequest.
                    // Re-read the CAS winner so the lifecycle response is built from the
                    // authoritative target route, never from the source-stripped request.
                    member->instanceManager->GetInstanceInfoByInstanceID(req->checkpointid()).OnComplete(
                        [scheduleReq, req, complete](
                            const litebus::Future<instance_manager::InstanceKeyInfoPair> &winnerFuture) {
                            if (winnerFuture.IsError()) {
                                complete(PauseResumeResult{winnerFuture.GetErrorCode(),
                                                           "failed to read committed resume winner", {}});
                                return;
                            }
                            const auto &winner = winnerFuture.Get().second;
                            if (winner == nullptr
                                || !resume_identity::IsCommittedResumeWinner(
                                    *winner, req->checkpointid(), req->requestid())
                                || winner->requestid() != scheduleReq->instance().requestid()
                                || winner->tenantid() != scheduleReq->instance().tenantid()
                                || winner->version() <= scheduleReq->instance().version()) {
                                complete(PauseResumeResult{
                                    static_cast<int32_t>(StatusCode::SCHEDULE_CONFLICTED),
                                    "scheduled resume has no authoritative running winner", {}});
                                return;
                            }
                            PauseResumeResult result;
                            result.code = common::ERR_NONE;
                            result.message = "pause resume committed";
                            PopulateResumeWinner(&result.snapstartInfo, *winner);
                            complete(std::move(result));
                        });
                });
        });
    return resultFuture;
}

litebus::Future<Status> SnapManagerActor::MasterBusiness::SaveMetadataToEtcd(const SnapshotMetadata &meta)
{
    ASSERT_IF_NULL(member_->client);

    std::string key = SNAPSHOT_KEY_PREFIX + meta.snapshotinfo().checkpointid();
    std::string value = meta.SerializeAsString();
    PutOption option;
    return member_->client->Put(key, value, option)
        .Then([](const std::shared_ptr<PutResponse> &response) -> Status {
            if (response && response->status.IsOk()) {
                return Status::OK();
            }
            return Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "failed to put snapshot metadata");
        });
}

litebus::Future<Status> SnapManagerActor::MasterBusiness::DeleteMetadataFromEtcd(const std::string &snapshotID)
{
    ASSERT_IF_NULL(member_->client);

    std::string key = SNAPSHOT_KEY_PREFIX + snapshotID;
    DeleteOption option;
    return member_->client->Delete(key, option)
        .Then([](const std::shared_ptr<DeleteResponse> &response) -> Status {
            if (response && response->status.IsOk()) {
                return Status::OK();
            }
            return Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "failed to delete snapshot metadata");
        });
}

void SnapManagerActor::MasterBusiness::EnforceSnapshotQuota(const std::string &functionID)
{
    auto snapshotsWithTime = member_->cache.GetSnapshotsWithTime(functionID);
    if (static_cast<int64_t>(snapshotsWithTime.size()) <= member_->config.maxSnapshotsPerFunction) {
        return;
    }

    // Delete oldest snapshots to meet quota
    int64_t toDelete = static_cast<int64_t>(snapshotsWithTime.size()) - member_->config.maxSnapshotsPerFunction;
    for (int64_t i = 0; i < toDelete && i < static_cast<int64_t>(snapshotsWithTime.size()); ++i) {
        const auto &snapshotID = std::get<1>(snapshotsWithTime[i]);
        YRLOG_INFO("enforcing quota: deleting old snapshot {} for function {}", snapshotID, functionID);
        DeleteMetadataFromEtcd(snapshotID);
    }
}

Status SnapManagerActor::MasterBusiness::ValidateSnapshot(const SnapshotMetadata &meta, int64_t currentTime) const
{
    const auto &createTimeStr = meta.snapshotinfo().createtime();
    if (createTimeStr.empty()) {
        return Status::OK();  // No timestamp, skip validation
    }

    int64_t createTime = 0;
    try {
        createTime = std::stoll(createTimeStr);
    } catch (const std::exception &) {
        return Status(StatusCode::ERR_PARAM_INVALID, "invalid snapshot create time");
    }

    int32_t ttlSeconds = meta.snapshotinfo().ttlseconds();
    if (ttlSeconds > 0 && createTime > 0 && (currentTime - createTime) > ttlSeconds) {
        return Status(StatusCode::ERR_PARAM_INVALID, "snapshot has expired");
    }

    return Status::OK();
}

void SnapManagerActor::MasterBusiness::SendRecordSnapshotResponse(const litebus::AID &to,
                                                                  const std::string &requestID,
                                                                  int32_t code,
                                                                  const std::string &message) const
{
    if (auto actor = actor_.lock(); actor) {
        actor->SendRecordSnapshotResponse(to, requestID, code, message);
    }
}

void SnapManagerActor::MasterBusiness::SendSnapStartResponse(const SnapStartResponse &response) const
{
    if (auto actor = actor_.lock(); actor) {
        actor->SendSnapStartResponse(response);
    }
}

void SnapManagerActor::MasterBusiness::CompleteSnapStart(const SnapStartLedgerContext &context,
                                                         const SnapStartResponse &response) const
{
    if (auto actor = actor_.lock(); actor) {
        actor->CompleteSnapStart(context, response);
    }
}

// ===========================================
// SlaveBusiness Implementation
// ===========================================

void SnapManagerActor::SlaveBusiness::OnChange()
{
    YRLOG_INFO("SnapManagerActor switched to SLAVE mode");
}

void SnapManagerActor::SlaveBusiness::RecordSnapshotMetadata(const litebus::AID &from,
                                                             std::string &&name,
                                                             std::string &&msg)
{
    YRLOG_WARN("SlaveBusiness: RecordSnapshotMetadata called on slave, operation not allowed");
}

void SnapManagerActor::SlaveBusiness::SnapStartCheckpoint(const litebus::AID &from,
                                                          std::string &&name,
                                                          std::string &&msg)
{
    YRLOG_WARN("SlaveBusiness: SnapStartCheckpoint called on slave, operation not allowed");
}

litebus::Future<Status> SnapManagerActor::SlaveBusiness::DeleteSnapshot(const std::string &snapshotID)
{
    // Slave cannot delete, return error
    YRLOG_WARN("SlaveBusiness: DeleteSnapshot called on slave, operation not allowed");
    return Status(StatusCode::FAILED, "operation not allowed on slave");
}

}  // namespace functionsystem::snap_manager
