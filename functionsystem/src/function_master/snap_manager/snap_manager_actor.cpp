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
#include "common/utils/generate_message.h"

namespace functionsystem::snap_manager {

using namespace functionsystem::explorer;
using namespace functionsystem::leader;
using namespace std::placeholders;

// ===========================================
// SnapManagerActor Constructor and Lifecycle
// ===========================================

SnapManagerActor::SnapManagerActor(const std::shared_ptr<MetaStoreClient> &metaClient,
                                   const std::shared_ptr<GlobalScheduler> &globalScheduler,
                                   const SnapManagerConfig &config)
    : ActorBase(SNAP_MANAGER_ACTOR_NAME)
{
    member_ = std::make_shared<Member>();
    member_->client = metaClient;
    member_->globalScheduler = globalScheduler;
    member_->config = config;
    member_->scheduler = std::make_unique<SnapshotScheduler>(globalScheduler);
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

void SnapManagerActor::SendSnapStartResponse(const SnapStartResponse &response)
{
    messages::RestoreSnapshotResponse rsp;
    rsp.set_requestid(response.requestID);
    rsp.set_code(response.code);
    rsp.set_message(response.message);
    if (!response.instanceID.empty()) {
        rsp.set_instanceid(response.instanceID);
    }
    if (response.snapstartInfo.ByteSizeLong() > 0) {
        rsp.mutable_snapstartinfo()->CopyFrom(response.snapstartInfo);
    }
    Send(response.to, "SnapStartCheckpointResponse", rsp.SerializeAsString());
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
        YRLOG_ERROR("failed to parse RestoreSnapshotRequest");
        SendSnapStartResponse({from, "", common::ERR_PARAM_INVALID, "failed to parse request"});
        return;
    }
    HandleSnapStart(from, req);
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
                                                       std::shared_ptr<messages::RestoreSnapshotRequest> req)
{
    const auto &snapshotID = req->checkpointid();
    YRLOG_INFO("processing snapstart request for snapshot: {}", snapshotID);

    // Look up snapshot metadata from cache
    auto metaOpt = member_->cache.Get(snapshotID);
    if (!metaOpt.has_value()) {
        YRLOG_ERROR("snapshot not found: {}", snapshotID);
        SendSnapStartResponse({from, req->requestid(), common::ERR_INSTANCE_NOT_FOUND, "snapshot not found"});
        return;
    }

    const auto &meta = metaOpt.value();

    // Validate snapshot (check expiration)
    int64_t currentTime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    Status validationStatus = ValidateSnapshot(meta, currentTime);
    if (validationStatus.IsError()) {
        YRLOG_ERROR("snapshot validation failed: {}", validationStatus.GetMessage());
        SendSnapStartResponse({from, req->requestid(), common::ERR_PARAM_INVALID, validationStatus.GetMessage()});
        return;
    }

    // Build ScheduleRequest from snapshot metadata
    auto scheduleReq = member_->scheduler->BuildScheduleRequest(meta, *req);

    // W10-2: dedupe by requestID. The local side's timeout retry re-SENDS the
    // same RestoreSnapshotRequest while the original schedule is still in
    // flight; every send mints a fresh synthetic "ckpt-" instance and restores
    // a DUPLICATE sandbox whose loser fails with an identity/replay conflict
    // (W9-2 v4: one noisy failure per successful restore). Same requestID is
    // by definition the same logical restore: absorb the resend and let the
    // first attempt's response answer it.
    const auto &dedupeKey = req->requestid();
    if (!dedupeKey.empty() && member_->inFlightSnapStarts.count(dedupeKey) > 0) {
        YRLOG_WARN("snapstart requestID {} for snapshot {} resent while in flight, absorbing duplicate",
                   dedupeKey, snapshotID);
        return;
    }
    if (!dedupeKey.empty()) {
        member_->inFlightSnapStarts.insert(dedupeKey);
    }

    // Invoke global scheduler
    auto weakActor = actor_;
    auto member = member_;
    member_->scheduler->Schedule(scheduleReq).OnComplete(
        [weakActor, member, req, from, scheduleReq, meta, dedupeKey](const litebus::Future<Status> &future) {
            auto actor = weakActor.lock();
            if (!actor) {
                return;
            }
            // W10-2: settle the in-flight marker before responding (posted to
            // the actor thread; the map is otherwise touched in HandleSnapStart)
            if (!dedupeKey.empty()) {
                litebus::Async(actor->GetAID(), [member, key(dedupeKey)]() {
                    member->inFlightSnapStarts.erase(key);
                });
            }
            auto code = future.IsError() ? future.GetErrorCode() : future.Get().StatusCode();
            auto message = future.IsError() ? "failed to schedule." : future.Get().RawMessage();
            ::messages::SnapstartInfo info;
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
            litebus::Async(actor->GetAID(), &SnapManagerActor::SendSnapStartResponse,
                           SnapStartResponse{from, req->requestid(), code, message,
                                             scheduleReq->instance().instanceid(), info});
        });
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
