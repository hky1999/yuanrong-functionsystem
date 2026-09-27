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

#include "agent_service_actor.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <memory>

#include "async/asyncafter.hpp"
#include "async/defer.hpp"
#include "common/logs/logging.h"
#include "common/snapshot_storage/snapshot_artifact_publisher.h"
#include "common/utils/resume_identity.h"
#include "runtime_manager/ckpt/checkpoint_plan.h"

namespace functionsystem::function_agent {
namespace {
constexpr uint32_t SNAPSHOT_ATTEMPT_PROTOCOL_VERSION = 1;
constexpr size_t SNAPSHOT_ID_DIGEST_LENGTH = 40;

Status EnsureLocalSnapshotID(messages::SnapshotRuntimeRequest &request)
{
    if (request.snapshotid().empty()) {
        const auto identity = request.requestid() + std::string(1, '\0')
            + request.instanceid();
        request.set_snapshotid(
            "ckpt-" + resume_identity::Sha256Hex(identity).substr(0, SNAPSHOT_ID_DIGEST_LENGTH));
    }
    if (!runtime_manager::IsSafeCheckpointIdentityComponent(request.snapshotid())) {
        return Status(StatusCode::ERR_PARAM_INVALID,
                      "local snapshot identity is invalid");
    }
    return Status::OK();
}

LocalSnapshotCommitRequest MakeLocalSnapshotCommitRequest(
    const messages::SnapshotRuntimeRequest &request, int64_t createdAtUnixSeconds)
{
    LocalSnapshotCommitRequest commit;
    commit.snapshotID = request.snapshotid();
    commit.recoveryCandidate = request.localrecoverycandidate();
    commit.instanceID = request.instanceid();
    commit.tenantHash = snapshot_storage::StableTenantHash(request.tenantid());
    commit.sourceRuntimeID = request.runtimeid();
    commit.sourceSandboxID = request.containerid().empty() ? request.runtimeid() : request.containerid();
    commit.sourceInstanceVersion = request.sourceversion();
    commit.createdAtUnixSeconds = createdAtUnixSeconds;
    return commit;
}

void CopyLocalSnapshotMetadata(const LocalSnapshotDescriptor &source,
                               ::messages::LocalSnapshotMetadata &target)
{
    target.set_snapshotid(source.snapshotID);
    target.set_localrecoverycandidate(source.recoveryCandidate);
    target.set_instanceid(source.instanceID);
    target.set_size(source.size);
    target.set_createdatunixseconds(source.createdAtUnixSeconds);
    target.set_sourceruntimeid(source.sourceRuntimeID);
    target.set_sourcesandboxid(source.sourceSandboxID);
    target.set_sourceinstanceversion(source.sourceInstanceVersion);
    target.set_tenanthash(source.tenantHash);
    target.set_storagebackend(source.storageBackend);
    target.set_objectkey(source.objectKey);
}

snapshot_storage::ArtifactPublishRequest BuildManagedSnapshotPublishRequest(
    const messages::SnapshotRuntimeRequest &request, const std::string &sourceFile,
    int64_t createdAtUnixSeconds)
{
    snapshot_storage::ArtifactPublishRequest publishRequest;
    publishRequest.sourceFile = sourceFile;
    publishRequest.temporaryKey = request.artifacttemporaryobjectkey();
    publishRequest.finalKey = request.artifactobjectkey();
    publishRequest.snapshotID = request.snapshotid();
    publishRequest.sourceInstanceVersion = request.sourceversion();
    publishRequest.createdAtUnixSeconds = createdAtUnixSeconds;
    publishRequest.ttlSeconds = request.ttl();
    publishRequest.compress = true;
    return publishRequest;
}

litebus::Future<snapshot_storage::ArtifactPublishResult> PublishManagedSnapshotArtifact(
    const std::shared_ptr<snapshot_storage::SnapshotStorage> &storage,
    const std::shared_ptr<ActorWorker> &worker,
    const snapshot_storage::ArtifactPublishRequest &request)
{
    return std::make_shared<snapshot_storage::SnapshotArtifactPublisher>(storage, worker)->Publish(request);
}

}  // namespace

void AgentServiceActor::SnapshotRuntime(const litebus::AID &from, std::string &&name, std::string &&msg)
{
    auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
    if (!request->ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse SnapshotRuntimeRequest");
        return;
    }

    const std::string &instanceID = request->instanceid();
    const std::string &runtimeID = request->runtimeid();
    YRLOG_INFO("{}|received SnapshotRuntime request for instance({}), runtime({})",
               request->requestid(), instanceID, runtimeID);

    // Prepare response
    messages::SnapshotRuntimeResponse response;
    response.set_requestid(request->requestid());
    response.set_checkpointnotstarted(true);

    if (from != localSchedFuncAgentMgrAID_) {
        YRLOG_WARN("{}|reject SnapshotRuntime from untrusted sender {}",
                   request->requestid(), std::string(from));
        return;
    }

    // Check if agent is registered
    if (!registerRuntimeMgr_.registered || !isRegisterCompleted_) {
        response.set_code(static_cast<int32_t>(StatusCode::FUNC_AGENT_NOT_REGISTERED));
        response.set_message("function agent is not registered");
        YRLOG_ERROR("{}|registration is not complete, ignore snapshot request for instance({})",
                    request->requestid(), instanceID);
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }

    const auto runtimeManagerAID = litebus::AID(registerRuntimeMgr_.name, registerRuntimeMgr_.address);
    const auto identityStatus = EnsureLocalSnapshotID(*request);
    if (identityStatus.IsError()) {
        response.set_code(static_cast<int32_t>(identityStatus.StatusCode()));
        response.set_message(identityStatus.RawMessage());
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }
    const bool missingDistributedStorageDependency =
        !request->internalcheckpoint() && UsesDistributedStorage(snapshotStorageMode_)
        && (snapshotStorage_ == nullptr || snapshotWorker_ == nullptr
            || request->artifactobjectkey().empty()
            || request->artifacttemporaryobjectkey().empty());
    if (request->requestid().empty() || request->instanceid().empty() || request->runtimeid().empty()
        || request->snapshotid().empty() || request->tenantid().empty()
        || request->sourceversion() <= 0
        || missingDistributedStorageDependency) {
        response.set_code(static_cast<int32_t>(StatusCode::ERR_PARAM_INVALID));
        response.set_message("snapshot data-plane identity or dependency is missing");
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }
    const auto existing = snapshotRequests_.find(request->requestid());
    if (existing != snapshotRequests_.end()) {
        if (existing->second.request.SerializeAsString() == request->SerializeAsString()) {
            existing->second.caller = from;
            if (existing->second.localReady && !request->internalcheckpoint()) {
                YRLOG_INFO("{}|replay LOCAL_READY SnapshotRuntime response", request->requestid());
                Send(from, "SnapshotRuntimeResponse",
                     existing->second.localReadyResponse.SerializeAsString());
            } else if (existing->second.completed) {
                YRLOG_INFO("{}|replay completed SnapshotRuntime response", request->requestid());
                Send(from, "SnapshotRuntimeResponse",
                     existing->second.completedResponse.SerializeAsString());
            } else {
                YRLOG_INFO("{}|reuse in-flight SnapshotRuntime request", request->requestid());
            }
            return;
        }
        response.set_code(static_cast<int32_t>(StatusCode::PARAMETER_ERROR));
        response.set_message("conflicting in-flight snapshot request ID");
        response.set_checkpointnotstarted(false);
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }

    PendingSnapshotRequest pending;
    pending.caller = from;
    pending.runtimeManagerAID = runtimeManagerAID;
    pending.runtimeManagerID = registerRuntimeMgr_.id;
    pending.createdAtUnixSeconds = static_cast<int64_t>(std::time(nullptr));
    if (localSnapshotStore_ == nullptr) {
        response.set_code(static_cast<int32_t>(StatusCode::ERR_PARAM_INVALID));
        response.set_message("local snapshot store or node architecture is unavailable");
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }
    pending.localCommitRequest = MakeLocalSnapshotCommitRequest(
        *request, pending.createdAtUnixSeconds);
    pending.artifactObjectKey = request->artifactobjectkey();
    if (request->internalcheckpoint()) {
        pending.storageBackend = "local";
        pending.artifactObjectKey = request->snapshotid();
    } else if (UsesDistributedStorage(snapshotStorageMode_)) {
        auto backendStatus = snapshot_storage::ResolveStorageBackend(
            snapshotStorage_, snapshotStorageBackend_, pending.storageBackend);
        if (backendStatus.IsError()) {
            response.set_code(static_cast<int32_t>(backendStatus.StatusCode()));
            response.set_message(backendStatus.ToString());
            Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
            return;
        }
    } else {
        pending.storageBackend = "local";
    }

    const auto prepared = localSnapshotStore_->Prepare(pending.localCommitRequest);
    if (prepared.status.IsError()) {
        response.set_code(static_cast<int32_t>(prepared.status.StatusCode()));
        response.set_message(prepared.status.RawMessage());
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }
    request->set_checkpointdir(prepared.directory.string());
    pending.request = *request;
    pending.artifactPath = prepared.directory.string();
    if (!snapshotRequests_.emplace(request->requestid(), std::move(pending)).second) {
        response.set_code(static_cast<int32_t>(StatusCode::PARAMETER_ERROR));
        response.set_message("duplicate in-flight snapshot request ID");
        response.set_checkpointnotstarted(false);
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }

    if (prepared.replayed) {
        LocalSnapshotDescriptor descriptor;
        const auto replay = localSnapshotStore_->ValidateForRestore(request->snapshotid(), descriptor);
        if (replay.IsError()) {
            response.set_code(static_cast<int32_t>(replay.StatusCode()));
            response.set_message(replay.RawMessage());
            Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
            snapshotRequests_.erase(request->requestid());
            return;
        }
        auto &stored = snapshotRequests_.at(request->requestid());
        CopyLocalSnapshotMetadata(descriptor, stored.localSnapshot);
        stored.createdAtUnixSeconds = descriptor.createdAtUnixSeconds;
        response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        response.set_checkpointnotstarted(false);
        response.mutable_snapshotinfo()->set_checkpointid(descriptor.snapshotID);
        response.mutable_snapshotinfo()->set_size(static_cast<int64_t>(descriptor.size));
        ContinueSnapshotAfterLocalCommit(request->requestid(), std::move(response));
        return;
    }

    YRLOG_INFO("{}|forward SnapshotRuntime request to RuntimeManager({}-{}) for instance({}), runtime({})",
               request->requestid(), registerRuntimeMgr_.name, registerRuntimeMgr_.address, instanceID, runtimeID);
    ForwardSnapshotRuntimeRequest(request->requestid());
}

void AgentServiceActor::ForwardSnapshotRuntimeRequest(const std::string &requestID)
{
    const auto iter = snapshotRequests_.find(requestID);
    if (iter == snapshotRequests_.end()) {
        return;
    }
    Send(iter->second.runtimeManagerAID, "SnapshotRuntime", iter->second.request.SerializeAsString());
}

void AgentServiceActor::SnapshotRuntimeResponse(const litebus::AID &from, std::string &&, std::string &&msg)
{
    messages::SnapshotRuntimeResponse response;
    if (!response.ParseFromString(msg)) {
        YRLOG_ERROR("failed to parse SnapshotRuntimeResponse");
        return;
    }

    const std::string &requestID = response.requestid();
    auto iter = snapshotRequests_.find(requestID);
    if (iter == snapshotRequests_.end()) {
        YRLOG_WARN("{}|snapshot request not found in snapshotRequests_", requestID);
        return;
    }

    auto &pending = iter->second;
    if (from != pending.runtimeManagerAID) {
        YRLOG_WARN("{}|ignore mismatched SnapshotRuntime response", requestID);
        return;
    }

    YRLOG_INFO("{}|received SnapshotRuntimeResponse from RuntimeManager, code: {}", requestID, response.code());

    if (response.code() != static_cast<int32_t>(StatusCode::SUCCESS)) {
        if (response.resultunknown()) {
            response.clear_physicalfact();
        } else if (localSnapshotStore_ != nullptr) {
            (void)localSnapshotStore_->DiscardPrepared(pending.request.snapshotid());
        }
        Send(pending.caller, "SnapshotRuntimeResponse", response.SerializeAsString());
        snapshotRequests_.erase(iter);
        return;
    }

    if (response.snapshotinfo().checkpointid() != pending.request.snapshotid()) {
        response.set_code(static_cast<int32_t>(StatusCode::ERR_PARAM_INVALID));
        response.set_message("runtime checkpoint response identity is invalid");
        Send(pending.caller, "SnapshotRuntimeResponse", response.SerializeAsString());
        (void)localSnapshotStore_->DiscardPrepared(pending.request.snapshotid());
        snapshotRequests_.erase(iter);
        return;
    }

    const auto committed = localSnapshotStore_->Commit(pending.localCommitRequest);
    if (committed.status.IsError()) {
        response.set_code(static_cast<int32_t>(committed.status.StatusCode()));
        response.set_message(committed.status.RawMessage());
        Send(pending.caller, "SnapshotRuntimeResponse", response.SerializeAsString());
        (void)localSnapshotStore_->DiscardPrepared(pending.request.snapshotid());
        snapshotRequests_.erase(iter);
        return;
    }
    CopyLocalSnapshotMetadata(committed.descriptor, pending.localSnapshot);
    pending.artifactPath = (std::filesystem::path(managedCheckpointRoot_)
                            / pending.request.snapshotid()).string();
    auto *snapshotInfo = response.mutable_snapshotinfo();
    snapshotInfo->set_checkpointid(committed.descriptor.snapshotID);
    snapshotInfo->set_size(static_cast<int64_t>(committed.descriptor.size));
    ContinueSnapshotAfterLocalCommit(requestID, std::move(response));
}

void AgentServiceActor::ContinueSnapshotAfterLocalCommit(
    const std::string &requestID, ::messages::SnapshotRuntimeResponse response,
    bool publishRequested)
{
    auto iter = snapshotRequests_.find(requestID);
    if (iter == snapshotRequests_.end()) {
        return;
    }
    auto &pending = iter->second;
    response.mutable_localsnapshot()->CopyFrom(pending.localSnapshot);
    const bool returnArtifact = pending.request.returnartifact();
    if (!publishRequested && pending.request.internalcheckpoint()) {
        snapshot_storage::SnapshotObjectMetadata metadata;
        metadata.snapshotID = pending.request.snapshotid();
        metadata.sourceInstanceVersion = pending.request.sourceversion();
        metadata.size = pending.localSnapshot.size();
        metadata.complete = true;
        pending.storageBackend = "local";
        pending.artifactObjectKey = pending.request.snapshotid();
        CompletePauseSnapshot(requestID, metadata);
        return;
    }
    const bool deferredPublication = pending.request.type() == common::SNAPSHOT
        || pending.request.type() == common::PAUSE_RESUME;
    if (!publishRequested && deferredPublication) {
        pending.localReady = true;
        pending.localReadyResponse = response;
        Send(pending.caller, "SnapshotRuntimeResponse", response.SerializeAsString());
        return;
    }
    if (returnArtifact) {
        // Preserve the committed local checkpoint facts while immutable
        // publication is in flight so exact cleanup remains possible.
        pending.completedResponse = response;
    }
    if (!UsesDistributedStorage(snapshotStorageMode_)) {
        snapshot_storage::SnapshotObjectMetadata metadata;
        metadata.snapshotID = pending.request.snapshotid();
        metadata.sourceInstanceVersion = pending.request.sourceversion();
        metadata.size = pending.localSnapshot.size();
        metadata.complete = true;
        pending.storageBackend = "local";
        pending.artifactObjectKey = pending.request.snapshotid();
        if (returnArtifact) {
            CompleteReusableSnapshot(requestID, metadata);
        } else {
            CompletePauseSnapshot(requestID, metadata);
        }
        return;
    }
    const auto publishRequest = BuildManagedSnapshotPublishRequest(
        pending.request, pending.artifactPath, pending.createdAtUnixSeconds);
    auto publishFuture = PublishManagedSnapshotArtifact(
        snapshotStorage_, snapshotWorker_, publishRequest);
    if (returnArtifact) {
        publishFuture.OnComplete(litebus::Defer(
            GetAID(), &AgentServiceActor::OnReusableArtifactPublished,
            requestID, std::placeholders::_1));
    } else {
        publishFuture.OnComplete(litebus::Defer(
            GetAID(), &AgentServiceActor::OnPauseArtifactPublished,
            requestID, std::placeholders::_1));
    }
}

void AgentServiceActor::PublishSnapshotArtifact(
    const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::PublishSnapshotArtifactRequest request;
    ::messages::SnapshotRuntimeResponse response;
    if (!request.ParseFromString(msg)) {
        return;
    }
    response.set_requestid(request.requestid());
    if (from != localSchedFuncAgentMgrAID_) {
        return;
    }
    auto iter = snapshotRequests_.find(request.requestid());
    if (iter == snapshotRequests_.end()
        || iter->second.request.snapshotid() != request.snapshotid()
        || iter->second.request.instanceid() != request.instanceid()) {
        response.set_code(static_cast<int32_t>(StatusCode::ERR_PARAM_INVALID));
        response.set_message("local-ready snapshot publication identity is invalid");
        Send(from, "PublishSnapshotArtifactResponse", response.SerializeAsString());
        return;
    }
    auto &pending = iter->second;
    pending.publishCaller = from;
    if (pending.completed) {
        Send(from, "PublishSnapshotArtifactResponse",
             pending.completedResponse.SerializeAsString());
        return;
    }
    if (!pending.localReady) {
        response.set_code(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY));
        response.set_message("snapshot has not reached LOCAL_READY");
        Send(from, "PublishSnapshotArtifactResponse", response.SerializeAsString());
        return;
    }
    if (pending.publicationStarted) {
        return;
    }
    pending.publicationStarted = true;
    ContinueSnapshotAfterLocalCommit(
        request.requestid(), pending.localReadyResponse, true);
}

void AgentServiceActor::SendSnapshotPhaseResponse(
    PendingSnapshotRequest &pending,
    const ::messages::SnapshotRuntimeResponse &response)
{
    const bool publication = pending.publicationStarted;
    Send(publication ? pending.publishCaller : pending.caller,
         publication ? "PublishSnapshotArtifactResponse" : "SnapshotRuntimeResponse",
         response.SerializeAsString());
}

void AgentServiceActor::ListLocalSnapshots(
    const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::ListLocalSnapshotsRequest request;
    if (!request.ParseFromString(msg) || request.requestid().empty()) {
        YRLOG_WARN("reject invalid ListLocalSnapshots request from {}", from.HashString());
        return;
    }
    if (from != localSchedFuncAgentMgrAID_) {
        YRLOG_WARN("{}|reject ListLocalSnapshots from untrusted sender {}",
                   request.requestid(), from.HashString());
        return;
    }
    ::messages::ListLocalSnapshotsResponse response;
    response.set_requestid(request.requestid());
    if (localSnapshotStore_ == nullptr) {
        response.set_code(static_cast<int32_t>(StatusCode::ERR_PARAM_INVALID));
        response.set_message("local snapshot store is unavailable");
    } else {
        response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        uint64_t totalBytes = 0;
        for (const auto &descriptor : localSnapshotStore_->List()) {
            totalBytes += descriptor.size;
            CopyLocalSnapshotMetadata(descriptor, *response.add_snapshots());
        }
        YRLOG_INFO("{}|listed {} committed local snapshots using {} bytes",
                   request.requestid(), response.snapshots_size(), totalBytes);
    }
    Send(from, "ListLocalSnapshotsResponse", response.SerializeAsString());
}

void AgentServiceActor::DeleteLocalSnapshot(
    const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::DeleteLocalSnapshotRequest request;
    if (!request.ParseFromString(msg) || request.requestid().empty()) {
        YRLOG_WARN("reject invalid DeleteLocalSnapshot request from {}", from.HashString());
        return;
    }
    if (from != localSchedFuncAgentMgrAID_) {
        YRLOG_WARN("{}|reject DeleteLocalSnapshot from untrusted sender {}",
                   request.requestid(), from.HashString());
        return;
    }
    ::messages::DeleteLocalSnapshotResponse response;
    response.set_requestid(request.requestid());
    Status status(StatusCode::ERR_PARAM_INVALID, "local snapshot store is unavailable");
    if (localSnapshotStore_ != nullptr) {
        LocalSnapshotDeleteIdentity identity;
        identity.snapshotID = request.snapshotid();
        status = localSnapshotStore_->Delete(identity);
    }
    response.set_code(static_cast<int32_t>(status.StatusCode()));
    response.set_message(status.IsError() ? status.RawMessage() : "");
    Send(from, "DeleteLocalSnapshotResponse", response.SerializeAsString());
}

void AgentServiceActor::OnReusableArtifactPublished(
    const std::string &requestID,
    const litebus::Future<snapshot_storage::ArtifactPublishResult> &publishFuture)
{
    auto iter = snapshotRequests_.find(requestID);
    if (iter == snapshotRequests_.end()) {
        return;
    }
    const auto completeError = [this, requestID](int32_t code, const std::string &message,
                                                 bool resultUnknown,
                                                 const snapshot_storage::SnapshotObjectMetadata *metadata) {
        auto pending = snapshotRequests_.find(requestID);
        if (pending == snapshotRequests_.end()) {
            return;
        }
        uint64_t size = metadata == nullptr ? 0 : metadata->size;
        std::string sha256 = metadata == nullptr ? "" : metadata->sha256;
        if ((size == 0 || sha256.empty()) && pending->second.completedResponse.has_snapshotinfo()) {
            const auto &runtimeSnapshot = pending->second.completedResponse.snapshotinfo();
            if (runtimeSnapshot.size() > 0 && !runtimeSnapshot.sha256().empty()) {
                size = static_cast<uint64_t>(runtimeSnapshot.size());
                sha256 = runtimeSnapshot.sha256();
            }
        }

        ::messages::SnapshotRuntimeResponse response;
        response.set_requestid(pending->second.request.requestid());
        response.set_code(code);
        response.set_message(message);
        response.set_resultunknown(resultUnknown);
        response.mutable_localsnapshot()->CopyFrom(pending->second.localSnapshot);
        const bool exactArtifact = size > 0 && !sha256.empty()
            && !pending->second.storageBackend.empty()
            && !pending->second.artifactObjectKey.empty();
        if (exactArtifact) {
            auto *artifact = response.mutable_reusablesnapshotartifact();
            artifact->set_storagebackend(pending->second.storageBackend);
            artifact->set_objectkey(pending->second.artifactObjectKey);
            artifact->set_size(static_cast<int64_t>(size));
            artifact->set_sha256(sha256);
            artifact->set_format("sandboxd-checkpoint");
            artifact->set_formatversion(1);
            pending->second.completed = true;
            pending->second.completedResponse = response;
        }
        SendSnapshotPhaseResponse(pending->second, response);
        if (!exactArtifact) {
            // Keep the Master PUBLISHING record, but forget this dead local
            // in-flight entry so the same deterministic request can replay
            // checkpoint/inspection instead of hanging forever. No physical
            // artifact is deleted without exact facts.
            snapshotRequests_.erase(pending);
        }
    };
    if (publishFuture.IsError()) {
        completeError(static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION),
                      "reusable snapshot artifact publisher future failed", true, nullptr);
        return;
    }
    const auto &result = publishFuture.Get();
    if (result.status.IsError()) {
        completeError(static_cast<int32_t>(result.status.StatusCode()),
                      result.status.RawMessage(), result.resultUnknown, &result.metadata);
        return;
    }
    CompleteReusableSnapshot(requestID, result.metadata);
}

void AgentServiceActor::OnPauseArtifactPublished(
    const std::string &requestID,
    const litebus::Future<snapshot_storage::ArtifactPublishResult> &publishFuture)
{
    if (snapshotRequests_.find(requestID) == snapshotRequests_.end()) {
        return;
    }
    if (publishFuture.IsError()) {
        CompletePauseSnapshotError(requestID, static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION),
                                   "snapshot artifact publisher future failed", true);
        return;
    }
    const auto &result = publishFuture.Get();
    if (result.status.IsError()) {
        CompletePauseSnapshotError(requestID, static_cast<int32_t>(result.status.StatusCode()),
                                   result.status.RawMessage(), result.resultUnknown);
        return;
    }
    CompletePauseSnapshot(requestID, result.metadata);
}

void AgentServiceActor::CompletePauseSnapshot(
    const std::string &requestID, const snapshot_storage::SnapshotObjectMetadata &metadata)
{
    auto iter = snapshotRequests_.find(requestID);
    if (iter == snapshotRequests_.end()) {
        return;
    }
    messages::SnapshotRuntimeResponse response;
    response.set_requestid(iter->second.request.requestid());
    response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    response.set_message("");
    response.set_resultunknown(false);
    auto *snapshot = response.mutable_snapshotinfo();
    snapshot->set_checkpointid(iter->second.request.snapshotid());
    snapshot->set_storage(iter->second.storageBackend);
    snapshot->set_size(static_cast<int64_t>(metadata.size));
    snapshot->set_sha256(metadata.sha256);
    snapshot->set_createtime(std::to_string(iter->second.localSnapshot.createdatunixseconds()));
    snapshot->set_ttlseconds(iter->second.request.ttl());
    snapshot->set_status(resources::SNAPSHOT_READY);
    iter->second.localSnapshot.set_storagebackend(iter->second.storageBackend);
    iter->second.localSnapshot.set_objectkey(iter->second.artifactObjectKey);
    if (localSnapshotStore_ != nullptr) {
        (void)localSnapshotStore_->SetStorageLocation(
            iter->second.request.snapshotid(), iter->second.storageBackend,
            iter->second.artifactObjectKey);
    }
    response.mutable_localsnapshot()->CopyFrom(iter->second.localSnapshot);
    response.clear_physicalfact();
    iter->second.completed = true;
    iter->second.completedResponse = response;
    if (!iter->second.request.internalcheckpoint()
        && !KeepsLocalSnapshot(snapshotStorageMode_) && localSnapshotStore_ != nullptr) {
        (void)localSnapshotStore_->EvictLocalArtifact(iter->second.request.snapshotid());
    }
    SendSnapshotPhaseResponse(iter->second, response);
}

void AgentServiceActor::CompleteReusableSnapshot(
    const std::string &requestID, const snapshot_storage::SnapshotObjectMetadata &metadata)
{
    auto iter = snapshotRequests_.find(requestID);
    if (iter == snapshotRequests_.end()) {
        return;
    }
    messages::SnapshotRuntimeResponse response;
    response.set_requestid(iter->second.request.requestid());
    response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    response.set_message("");
    response.set_resultunknown(false);
    auto *snapshot = response.mutable_snapshotinfo();
    snapshot->set_checkpointid(iter->second.request.snapshotid());
    snapshot->set_storage(iter->second.storageBackend);
    snapshot->set_size(static_cast<int64_t>(metadata.size));
    snapshot->set_sha256(metadata.sha256);
    snapshot->set_createtime(std::to_string(iter->second.localSnapshot.createdatunixseconds()));
    snapshot->set_ttlseconds(0);
    snapshot->set_status(resources::SNAPSHOT_READY);
    iter->second.localSnapshot.set_storagebackend(iter->second.storageBackend);
    iter->second.localSnapshot.set_objectkey(iter->second.artifactObjectKey);
    if (localSnapshotStore_ != nullptr) {
        (void)localSnapshotStore_->SetStorageLocation(
            iter->second.request.snapshotid(), iter->second.storageBackend,
            iter->second.artifactObjectKey);
    }
    auto *artifact = response.mutable_reusablesnapshotartifact();
    artifact->set_storagebackend(iter->second.storageBackend);
    artifact->set_objectkey(iter->second.artifactObjectKey);
    artifact->set_size(static_cast<int64_t>(metadata.size));
    artifact->set_sha256(metadata.sha256);
    artifact->set_format("sandboxd-checkpoint");
    artifact->set_formatversion(1);
    response.mutable_localsnapshot()->CopyFrom(iter->second.localSnapshot);
    response.clear_physicalfact();
    iter->second.completed = true;
    iter->second.completedResponse = response;
    if (!KeepsLocalSnapshot(snapshotStorageMode_) && localSnapshotStore_ != nullptr) {
        (void)localSnapshotStore_->EvictLocalArtifact(iter->second.request.snapshotid());
    }
    SendSnapshotPhaseResponse(iter->second, response);
}

void AgentServiceActor::CompletePauseSnapshotError(const std::string &requestID, int32_t code,
                                                   const std::string &message, bool resultUnknown)
{
    auto iter = snapshotRequests_.find(requestID);
    if (iter == snapshotRequests_.end()) {
        return;
    }
    messages::SnapshotRuntimeResponse response;
    response.set_requestid(iter->second.request.requestid());
    response.set_code(code);
    response.set_message(message);
    response.set_resultunknown(resultUnknown);
    if (resultUnknown) {
        response.clear_physicalfact();
    }
    SendSnapshotPhaseResponse(iter->second, response);
    snapshotRequests_.erase(iter);
}

void AgentServiceActor::SnapshotAttemptFinalize(
    const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::SnapshotAttemptFinalizeRequest request;
    if (!request.ParseFromString(msg)) {
        return;
    }
    if (from != localSchedFuncAgentMgrAID_) {
        YRLOG_WARN("{}|reject physical snapshot finalize from untrusted sender {}",
                   request.attemptid(), std::string(from));
        return;
    }
    if (request.protocolversion() != SNAPSHOT_ATTEMPT_PROTOCOL_VERSION
        || request.attemptid().empty() || request.snapshotid().empty()
        || (request.deleteruntime() && request.runtimeid().empty())
        || (request.deletelocalartifact() && localSnapshotStore_ == nullptr)
        || (UsesDistributedStorage(snapshotStorageMode_)
            && request.deleteremoteobjectkeys_size() > 0 && snapshotStorage_ == nullptr)) {
        SendSnapshotAttemptFinalizeResponse(
            from, request.attemptid(), static_cast<int32_t>(StatusCode::ERR_PARAM_INVALID),
            "invalid physical snapshot finalize request", false, false, false);
        return;
    }

    if (request.deletelocalartifact()) {
        const auto local = localSnapshotStore_->Delete({request.snapshotid()});
        if (local.IsError()) {
            SendSnapshotAttemptFinalizeResponse(
                from, request.attemptid(), static_cast<int32_t>(local.StatusCode()),
                local.RawMessage(), false, false, false);
            return;
        }
    }
    snapshotRequests_.erase(request.attemptid());

    if (request.deleteruntime()) {
        if (!registerRuntimeMgr_.registered || !isRegisterCompleted_) {
            SendSnapshotAttemptFinalizeResponse(
                from, request.attemptid(), static_cast<int32_t>(StatusCode::FUNC_AGENT_NOT_REGISTERED),
                "RuntimeManager is not registered for physical runtime cleanup", false, true, false);
            return;
        }
        const auto cleanupRequestID = "snapshot-finalize/" + request.attemptid();
        snapshotRuntimeFinalizations_[cleanupRequestID] = {from, request, request.runtimeid()};
        messages::StopInstanceRequest stop;
        stop.set_runtimeid(request.runtimeid());
        stop.set_requestid(cleanupRequestID);
        stop.set_traceid(cleanupRequestID);
        stop.set_executortype(static_cast<int32_t>(EXECUTOR_TYPE::SANDBOXD));
        Send(litebus::AID(registerRuntimeMgr_.name, registerRuntimeMgr_.address),
             "StopInstance", stop.SerializeAsString());
        return;
    }
    ContinuePhysicalSnapshotFinalize(from, request, 0);
}

void AgentServiceActor::ContinuePhysicalSnapshotFinalize(
    const litebus::AID &caller, const ::messages::SnapshotAttemptFinalizeRequest &request,
    size_t remoteKeyIndex)
{
    if (!UsesDistributedStorage(snapshotStorageMode_)
        || remoteKeyIndex >= static_cast<size_t>(request.deleteremoteobjectkeys_size())) {
        SendSnapshotAttemptFinalizeResponse(
            caller, request.attemptid(), static_cast<int32_t>(StatusCode::SUCCESS),
            "", false, true, true);
        return;
    }
    snapshotStorage_->Delete(request.deleteremoteobjectkeys(static_cast<int>(remoteKeyIndex)))
        .OnComplete(litebus::Defer(
            GetAID(), &AgentServiceActor::OnPhysicalSnapshotObjectDeleted,
            caller, request, remoteKeyIndex, std::placeholders::_1));
}

void AgentServiceActor::OnPhysicalSnapshotObjectDeleted(
    const litebus::AID &caller, const ::messages::SnapshotAttemptFinalizeRequest &request,
    size_t remoteKeyIndex, const litebus::Future<Status> &future)
{
    if (future.IsError() || (future.Get().IsError()
        && future.Get().StatusCode() != StatusCode::FILE_NOT_FOUND
        && future.Get().StatusCode() != StatusCode::BP_DATASYSTEM_ERROR)) {
        SendSnapshotAttemptFinalizeResponse(
            caller, request.attemptid(),
            future.IsError() ? static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION)
                             : static_cast<int32_t>(future.Get().StatusCode()),
            future.IsError() ? "snapshot storage Delete future failed" : future.Get().RawMessage(),
            future.IsError(), true, false);
        return;
    }
    ContinuePhysicalSnapshotFinalize(caller, request, remoteKeyIndex + 1);
}

void AgentServiceActor::DeleteReusableSnapshotArtifact(
    const litebus::AID &from, std::string &&, std::string &&msg)
{
    ::messages::DeleteReusableSnapshotArtifactRequest request;
    if (!request.ParseFromString(msg)) {
        SendDeleteReusableSnapshotArtifactResponse(
            from, "", common::ERR_PARAM_INVALID,
            "failed to parse reusable Snapshot artifact delete request");
        return;
    }
    if (from != localSchedFuncAgentMgrAID_) {
        YRLOG_WARN("{}|reject reusable Snapshot artifact delete from untrusted sender {}",
                   request.requestid(), std::string(from));
        return;
    }

    const auto &artifact = request.artifact();
    if (artifact.storagebackend() == "local") {
        if (request.requestid().empty() || request.tenantid().empty()
            || request.snapshotid().empty() || localSnapshotStore_ == nullptr) {
            SendDeleteReusableSnapshotArtifactResponse(
                from, request.requestid(), common::ERR_PARAM_INVALID,
                "local reusable Snapshot delete identity is incomplete");
            return;
        }
        // A lifecycle-bound local Snapshot may still be pinned by an in-flight
        // restore. Mark it for deletion and let the final unpin remove it.
        const auto status = localSnapshotStore_->EvictLocalArtifact(request.snapshotid());
        SendDeleteReusableSnapshotArtifactResponse(
            from, request.requestid(),
            status.IsOk() || status.StatusCode() == StatusCode::FILE_NOT_FOUND
                ? common::ERR_NONE : Status::GetPosixErrorCode(status.StatusCode()),
            status.IsOk() ? "" : status.RawMessage());
        return;
    }
    std::string actualBackend;
    const auto backend = snapshot_storage::ResolveStorageBackend(
        snapshotStorage_, snapshotStorageBackend_, actualBackend);
    const auto canonicalKey = snapshot_storage::BuildReusableSnapshotKey(
        snapshot_storage::StableTenantHash(request.tenantid()),
        request.snapshotid());
    if (request.requestid().empty() || request.tenantid().empty()
        || request.snapshotid().empty() || snapshotStorage_ == nullptr
        || backend.IsError() || actualBackend != artifact.storagebackend()
        || artifact.objectkey() != canonicalKey) {
        SendDeleteReusableSnapshotArtifactResponse(
            from, request.requestid(), common::ERR_PARAM_INVALID,
            backend.IsError() ? backend.GetMessage()
                              : "reusable Snapshot artifact delete identity is not canonical and frozen");
        return;
    }

    if (localSnapshotStore_ != nullptr) {
        auto localStatus = localSnapshotStore_->Delete({request.snapshotid()});
        if (localStatus.IsError() && localStatus.StatusCode() != StatusCode::FILE_NOT_FOUND) {
            YRLOG_WARN("{}|reusable Snapshot({}) local cache cleanup is best-effort: {}",
                       request.requestid(), request.snapshotid(), localStatus.ToString());
        }
    }

    snapshotStorage_->Delete(canonicalKey).OnComplete(
        litebus::Defer(GetAID(), &AgentServiceActor::OnReusableSnapshotArtifactDeleted,
                       from, request, std::placeholders::_1));
}

void AgentServiceActor::OnReusableSnapshotArtifactDeleted(
    const litebus::AID &caller,
    const ::messages::DeleteReusableSnapshotArtifactRequest &request,
    const litebus::Future<Status> &future)
{
    if (future.IsError()) {
        SendDeleteReusableSnapshotArtifactResponse(
            caller, request.requestid(), common::ERR_INNER_COMMUNICATION,
            "reusable Snapshot artifact Delete future failed");
        return;
    }
    if (future.Get().IsError()
        && future.Get().StatusCode() != StatusCode::FILE_NOT_FOUND
        && future.Get().StatusCode() != StatusCode::BP_DATASYSTEM_ERROR) {
        SendDeleteReusableSnapshotArtifactResponse(
            caller, request.requestid(),
            static_cast<int32_t>(Status::GetPosixErrorCode(future.Get().StatusCode())),
            future.Get().ToString());
        return;
    }
    if (future.Get().StatusCode() == StatusCode::BP_DATASYSTEM_ERROR) {
        YRLOG_WARN("{}|reusable Snapshot({}) DataSystem artifact cleanup is best-effort: {}",
                   request.requestid(), request.snapshotid(), future.Get().ToString());
    }
    SendDeleteReusableSnapshotArtifactResponse(
        caller, request.requestid(), common::ERR_NONE, "");
}

void AgentServiceActor::SendDeleteReusableSnapshotArtifactResponse(
    const litebus::AID &caller, const std::string &requestID,
    int32_t code, const std::string &message)
{
    ::messages::DeleteReusableSnapshotArtifactResponse response;
    response.set_requestid(requestID);
    response.set_code(code);
    response.set_message(message);
    (void)Send(caller, "DeleteReusableSnapshotArtifactResponse", response.SerializeAsString());
}

void AgentServiceActor::SendSnapshotAttemptFinalizeResponse(
    const litebus::AID &caller, const std::string &attemptID, int32_t code, const std::string &message,
    bool resultUnknown, bool localComplete, bool remoteComplete)
{
    ::messages::SnapshotAttemptFinalizeResponse response;
    response.set_code(code);
    response.set_message(message);
    response.set_resultunknown(resultUnknown);
    response.set_localcleanupcomplete(localComplete);
    response.set_remotecleanupcomplete(remoteComplete);
    response.set_attemptid(attemptID);
    Send(caller, "SnapshotAttemptFinalizeResponse", response.SerializeAsString());
}

}  // namespace functionsystem::function_agent
