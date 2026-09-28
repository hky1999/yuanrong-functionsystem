/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "reusable_snapshot_store.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <limits>
#include <utility>

#include "common/utils/resume_identity.h"
#include "meta_store_client/txn_transaction.h"

namespace functionsystem::snap_manager {
namespace {

constexpr char REUSABLE_SNAPSHOT_PREFIX[] = "/yr/snapshots/v1/";
constexpr char SNAPSHOT_ID_PREFIX[] = "snap-";
constexpr uint32_t DEFAULT_PAGE_SIZE = 100;
constexpr uint32_t MAX_PAGE_SIZE = 1000;

Status Invalid(const std::string &message)
{
    return Status(StatusCode::ERR_PARAM_INVALID, message);
}

Status NotFound(const std::string &message)
{
    return Status(StatusCode::ERR_CHECKPOINT_NOT_FOUND, message);
}

Status Conflict(const std::string &message)
{
    return Status(StatusCode::SCHEDULE_CONFLICTED, message);
}

bool ParseMetadata(const std::string &value, ::messages::ReusableSnapshotMetadata *metadata)
{
    return metadata != nullptr && metadata->ParseFromString(value) && !metadata->snapshotid().empty()
        && !metadata->tenantid().empty();
}

bool ArtifactEquals(const ::messages::SnapshotArtifact &left, const ::messages::SnapshotArtifact &right)
{
    return left.storagebackend() == right.storagebackend() && left.objectkey() == right.objectkey()
        && left.size() == right.size() && left.sha256() == right.sha256() && left.format() == right.format()
        && left.formatversion() == right.formatversion() && left.sourcenodeid() == right.sourcenodeid();
}

template <typename Response>
Response Success(const std::string &requestID)
{
    Response response;
    response.set_requestid(requestID);
    response.set_code(common::ERR_NONE);
    response.set_message("success");
    return response;
}

}  // namespace

EtcdReusableSnapshotPersistence::EtcdReusableSnapshotPersistence(std::shared_ptr<MetaStoreClient> client)
    : client_(std::move(client))
{
}

litebus::Future<ReusableSnapshotReadResult> EtcdReusableSnapshotPersistence::Read(const std::string &key)
{
    if (client_ == nullptr) {
        return litebus::Future<ReusableSnapshotReadResult>(
            ReusableSnapshotReadResult{ Status(StatusCode::POINTER_IS_NULL, "meta store client is null"), {} });
    }
    return client_->Get(key, GetOption{})
        .Then([](const std::shared_ptr<GetResponse> &response) {
            ReusableSnapshotReadResult result;
            if (response == nullptr) {
                result.status = Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "null meta store get response");
                return result;
            }
            result.status = response->status;
            if (result.status.IsOk() && !response->kvs.empty()) {
                result.value = response->kvs.front().value();
            }
            return result;
        });
}

litebus::Future<ReusableSnapshotListRecordsResult> EtcdReusableSnapshotPersistence::List(
    const std::string &prefix)
{
    if (client_ == nullptr) {
        return litebus::Future<ReusableSnapshotListRecordsResult>(ReusableSnapshotListRecordsResult{
            Status(StatusCode::POINTER_IS_NULL, "meta store client is null"), {} });
    }
    return client_->Get(prefix, GetOption{ .prefix = true, .sortOrder = SortOrder::ASCEND,
                                           .sortTarget = SortTarget::KEY })
        .Then([](const std::shared_ptr<GetResponse> &response) {
            ReusableSnapshotListRecordsResult result;
            if (response == nullptr) {
                result.status = Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "null meta store list response");
                return result;
            }
            result.status = response->status;
            if (result.status.IsError()) {
                return result;
            }
            result.values.reserve(response->kvs.size());
            for (const auto &kv : response->kvs) {
                result.values.emplace_back(kv.value());
            }
            return result;
        });
}

litebus::Future<ReusableSnapshotCasResult> EtcdReusableSnapshotPersistence::CompareAndSwap(
    const std::string &key, const std::optional<std::string> &expected,
    const std::optional<std::string> &replacement)
{
    if (client_ == nullptr) {
        return litebus::Future<ReusableSnapshotCasResult>(ReusableSnapshotCasResult{
            Status(StatusCode::POINTER_IS_NULL, "meta store client is null"), false });
    }
    auto transaction = client_->BeginTransaction();
    if (transaction == nullptr) {
        return litebus::Future<ReusableSnapshotCasResult>(ReusableSnapshotCasResult{
            Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "failed to begin meta store transaction"), false });
    }
    if (expected.has_value()) {
        transaction->If(meta_store::TxnCompare::OfValue(
            key, meta_store::CompareOperator::EQUAL, expected.value()));
    } else {
        transaction->If(meta_store::TxnCompare::OfVersion(key, meta_store::CompareOperator::EQUAL, 0));
    }
    if (replacement.has_value()) {
        transaction->Then(meta_store::TxnOperation::Create(
            key, replacement.value(), PutOption{ .leaseId = 0, .prevKv = false, .asyncBackup = false }));
    } else {
        transaction->Then(meta_store::TxnOperation::Create(
            key, DeleteOption{ .prevKv = false, .prefix = false, .asyncBackup = false }));
    }
    return transaction->Commit().Then([](const std::shared_ptr<TxnResponse> &response) {
        ReusableSnapshotCasResult result;
        if (response == nullptr) {
            result.status = Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "null meta store transaction response");
            return result;
        }
        result.status = response->status;
        result.swapped = response->status.IsOk() && response->success;
        return result;
    });
}

ReusableSnapshotStore::ReusableSnapshotStore(std::shared_ptr<ReusableSnapshotPersistence> persistence,
                                             Clock clock)
    : persistence_(std::move(persistence)), clock_(std::move(clock))
{
    if (!clock_) {
        clock_ = [] {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        };
    }
}

std::string ReusableSnapshotStore::TenantPrefix(const std::string &tenantID)
{
    return std::string(REUSABLE_SNAPSHOT_PREFIX) + resume_identity::Sha256Hex(tenantID) + "/";
}

std::string ReusableSnapshotStore::RecordKey(const std::string &tenantID, const std::string &snapshotID)
{
    return TenantPrefix(tenantID) + snapshotID;
}

std::string ReusableSnapshotStore::SnapshotID(const ::messages::BeginReusableSnapshotRequest &request)
{
    std::string identity;
    identity.reserve(request.tenantid().size() + request.sourceinstanceid().size() + request.requestid().size() + 3);
    identity.append(request.tenantid()).push_back('\0');
    identity.append(request.sourceinstanceid()).push_back('\0');
    identity.append(request.requestid());
    return std::string(SNAPSHOT_ID_PREFIX) + resume_identity::Sha256Hex(identity).substr(0, 32);
}

::resources::InstanceInfo ReusableSnapshotStore::SanitizeInstanceTemplate(const ::resources::InstanceInfo &source)
{
    ::resources::InstanceInfo target;
    target.set_function(source.function());
    target.set_restartpolicy(source.restartpolicy());
    *target.mutable_resources() = source.resources();
    *target.mutable_scheduleoption() = source.scheduleoption();
    *target.mutable_createoptions() = source.createoptions();
    target.mutable_labels()->CopyFrom(source.labels());
    target.set_storagetype(source.storagetype());
    target.mutable_args()->CopyFrom(source.args());
    target.set_detached(source.detached());
    target.set_gracefulshutdowntime(source.gracefulshutdowntime());
    target.set_lowreliability(source.lowreliability());
    *target.mutable_kvlabels() = source.kvlabels();
    target.set_trafficreporttype(source.trafficreporttype());
    target.set_executortype(source.executortype());
    target.set_failover(source.failover());
    // Extensions are an open-ended transport/status channel. An exclusion
    // list would silently persist every future physical identity, credential,
    // timestamp or scheduler marker added by another component. The first
    // reusable-Snapshot contract has no extension that is both required for
    // Restore and safe to clone, so the explicit allowlist is empty. Stable
    // workload inputs are carried by the typed fields and createOptions above;
    // normal Create reconstructs its own transport extensions.
    return target;
}

Status ReusableSnapshotStore::ValidateBegin(const ::messages::BeginReusableSnapshotRequest &request)
{
    if (request.requestid().empty() || request.tenantid().empty() || request.sourceinstanceid().empty()) {
        return Invalid("requestID, tenantID and sourceInstanceID are required");
    }
    if (request.names_size() > 1 || (request.names_size() == 1 && request.names(0).empty())) {
        return Invalid("reusable Snapshot supports zero or one non-empty name");
    }
    return Status::OK();
}

Status ReusableSnapshotStore::ValidateArtifact(const ::messages::SnapshotArtifact &artifact)
{
    const bool local = artifact.storagebackend() == "local";
    const bool supportedBackend = local || artifact.storagebackend() == "obs"
        || artifact.storagebackend() == "datasystem";
    const bool safeObjectKey = !artifact.objectkey().empty() && artifact.objectkey().front() != '/'
        && artifact.objectkey().find("../") == std::string::npos
        && artifact.objectkey().find("/..") == std::string::npos;
    const bool validSha256 = artifact.sha256().size() == 64
        && std::all_of(artifact.sha256().begin(), artifact.sha256().end(), [](unsigned char value) {
            return std::isxdigit(value) != 0;
        });
    if (!supportedBackend || !safeObjectKey || artifact.size() <= 0
        || (local ? artifact.sourcenodeid().empty() : !validSha256)
        || artifact.format() != "sandboxd-checkpoint"
        || artifact.formatversion() != 1) {
        return Invalid("reusable Snapshot artifact is incomplete or unsupported");
    }
    return Status::OK();
}

bool ReusableSnapshotStore::HasName(const ::messages::ReusableSnapshotMetadata &metadata,
                                    const std::string &name)
{
    return name.empty() || std::find(metadata.names().begin(), metadata.names().end(), name) != metadata.names().end();
}

::core_service::SnapshotInfo ReusableSnapshotStore::PublicInfo(
    const ::messages::ReusableSnapshotMetadata &metadata)
{
    ::core_service::SnapshotInfo info;
    info.set_snapshotid(metadata.snapshotid());
    info.mutable_names()->CopyFrom(metadata.names());
    return info;
}

int64_t ReusableSnapshotStore::Now() const
{
    return clock_();
}

litebus::Future<::messages::BeginReusableSnapshotResponse> ReusableSnapshotStore::Begin(
    const ::messages::BeginReusableSnapshotRequest &request)
{
    const auto validation = ValidateBegin(request);
    if (validation.IsError() || persistence_ == nullptr) {
        const auto status = validation.IsError()
            ? validation
            : Status(StatusCode::POINTER_IS_NULL, "persistence is null");
        return litebus::Future<::messages::BeginReusableSnapshotResponse>(
            ErrorResponse<::messages::BeginReusableSnapshotResponse>(request.requestid(), status));
    }
    const auto snapshotID = SnapshotID(request);
    const auto key = RecordKey(request.tenantid(), snapshotID);
    return persistence_->Read(key).Then([this, request, snapshotID, key](const ReusableSnapshotReadResult &read)
        -> litebus::Future<::messages::BeginReusableSnapshotResponse> {
        if (read.status.IsError()) {
            return litebus::Future<::messages::BeginReusableSnapshotResponse>(
                ErrorResponse<::messages::BeginReusableSnapshotResponse>(request.requestid(), read.status));
        }
        if (read.value.has_value()) {
            ::messages::ReusableSnapshotMetadata current;
            if (!ParseMetadata(read.value.value(), &current)) {
                return litebus::Future<::messages::BeginReusableSnapshotResponse>(
                    ErrorResponse<::messages::BeginReusableSnapshotResponse>(
                        request.requestid(), Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "invalid Snapshot record")));
            }
            if (current.createrequestid() != request.requestid()) {
                return litebus::Future<::messages::BeginReusableSnapshotResponse>(
                    ErrorResponse<::messages::BeginReusableSnapshotResponse>(
                        request.requestid(), Conflict("Snapshot request ID conflict")));
            }
            auto response = Success<::messages::BeginReusableSnapshotResponse>(request.requestid());
            response.set_snapshotid(current.snapshotid());
            response.set_phase(current.phase());
            if (current.phase() == ::messages::REUSABLE_SNAPSHOT_READY) {
                *response.mutable_snapshotinfo() = PublicInfo(current);
            }
            return litebus::Future<::messages::BeginReusableSnapshotResponse>(response);
        }
        ::messages::ReusableSnapshotMetadata created;
        created.set_snapshotid(snapshotID);
        created.mutable_names()->CopyFrom(request.names());
        created.set_tenantid(request.tenantid());
        created.set_createrequestid(request.requestid());
        created.set_sourceinstanceid(request.sourceinstanceid());
        created.set_phase(::messages::REUSABLE_SNAPSHOT_PUBLISHING);
        created.set_createtime(Now());
        created.set_updatetime(created.createtime());
        created.set_version(1);
        return persistence_->CompareAndSwap(key, std::nullopt, created.SerializeAsString())
            .Then([request, created](const ReusableSnapshotCasResult &cas) {
                if (cas.status.IsError()) {
                    return ErrorResponse<::messages::BeginReusableSnapshotResponse>(request.requestid(), cas.status);
                }
                if (!cas.swapped) {
                    return ErrorResponse<::messages::BeginReusableSnapshotResponse>(
                        request.requestid(), Conflict("concurrent Snapshot begin won the CAS"));
                }
                auto response = Success<::messages::BeginReusableSnapshotResponse>(request.requestid());
                response.set_snapshotid(created.snapshotid());
                response.set_phase(created.phase());
                return response;
            });
    });
}

litebus::Future<::messages::CommitReusableSnapshotResponse> ReusableSnapshotStore::Commit(
    const ::messages::CommitReusableSnapshotRequest &request)
{
    const auto artifactStatus = ValidateArtifact(request.artifact());
    if (request.requestid().empty() || request.tenantid().empty() || request.snapshotid().empty()
        || artifactStatus.IsError() || persistence_ == nullptr) {
        const auto status = artifactStatus.IsError() ? artifactStatus
            : Invalid("commit requestID, tenantID and snapshotID are required");
        return litebus::Future<::messages::CommitReusableSnapshotResponse>(
            ErrorResponse<::messages::CommitReusableSnapshotResponse>(request.requestid(), status));
    }
    const auto key = RecordKey(request.tenantid(), request.snapshotid());
    return persistence_->Read(key).Then([this, request, key](const ReusableSnapshotReadResult &read)
        -> litebus::Future<::messages::CommitReusableSnapshotResponse> {
        if (read.status.IsError() || !read.value.has_value()) {
            return litebus::Future<::messages::CommitReusableSnapshotResponse>(
                ErrorResponse<::messages::CommitReusableSnapshotResponse>(request.requestid(),
                    read.status.IsError() ? read.status : NotFound("Snapshot begin record not found")));
        }
        ::messages::ReusableSnapshotMetadata current;
        if (!ParseMetadata(read.value.value(), &current)) {
            return litebus::Future<::messages::CommitReusableSnapshotResponse>(
                ErrorResponse<::messages::CommitReusableSnapshotResponse>(request.requestid(),
                    Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "invalid Snapshot record")));
        }
        if (current.createrequestid() != request.requestid()) {
            return litebus::Future<::messages::CommitReusableSnapshotResponse>(
                ErrorResponse<::messages::CommitReusableSnapshotResponse>(
                    request.requestid(), Conflict("Snapshot commit request ID conflict")));
        }
        if (current.phase() == ::messages::REUSABLE_SNAPSHOT_READY) {
            if (!ArtifactEquals(current.artifact(), request.artifact())) {
                return litebus::Future<::messages::CommitReusableSnapshotResponse>(
                    ErrorResponse<::messages::CommitReusableSnapshotResponse>(
                        request.requestid(), Conflict("READY Snapshot artifact is immutable")));
            }
            auto response = Success<::messages::CommitReusableSnapshotResponse>(request.requestid());
            *response.mutable_snapshotinfo() = PublicInfo(current);
            response.set_version(current.version());
            return litebus::Future<::messages::CommitReusableSnapshotResponse>(response);
        }
        if (current.phase() != ::messages::REUSABLE_SNAPSHOT_PUBLISHING) {
            return litebus::Future<::messages::CommitReusableSnapshotResponse>(
                ErrorResponse<::messages::CommitReusableSnapshotResponse>(
                    request.requestid(), Conflict("Snapshot is not publishing")));
        }
        auto ready = current;
        *ready.mutable_instancetemplate() = SanitizeInstanceTemplate(request.sourceinstanceinfo());
        *ready.mutable_artifact() = request.artifact();
        ready.set_phase(::messages::REUSABLE_SNAPSHOT_READY);
        ready.set_updatetime(Now());
        ready.set_version(current.version() + 1);
        return persistence_->CompareAndSwap(key, read.value, ready.SerializeAsString())
            .Then([request, ready](const ReusableSnapshotCasResult &cas) {
                if (cas.status.IsError() || !cas.swapped) {
                    return ErrorResponse<::messages::CommitReusableSnapshotResponse>(request.requestid(),
                        cas.status.IsError() ? cas.status : Conflict("concurrent Snapshot commit won the CAS"));
                }
                auto response = Success<::messages::CommitReusableSnapshotResponse>(request.requestid());
                *response.mutable_snapshotinfo() = PublicInfo(ready);
                response.set_version(ready.version());
                return response;
            });
    });
}

litebus::Future<::messages::FailReusableSnapshotResponse> ReusableSnapshotStore::Fail(
    const ::messages::FailReusableSnapshotRequest &request)
{
    if (request.requestid().empty() || request.tenantid().empty() || request.snapshotid().empty()
        || persistence_ == nullptr) {
        return litebus::Future<::messages::FailReusableSnapshotResponse>(
            ErrorResponse<::messages::FailReusableSnapshotResponse>(
                request.requestid(), Invalid("invalid fail request")));
    }
    const auto key = RecordKey(request.tenantid(), request.snapshotid());
    return persistence_->Read(key).Then([this, request, key](const ReusableSnapshotReadResult &read)
        -> litebus::Future<::messages::FailReusableSnapshotResponse> {
        if (read.status.IsError()) {
            return litebus::Future<::messages::FailReusableSnapshotResponse>(
                ErrorResponse<::messages::FailReusableSnapshotResponse>(request.requestid(), read.status));
        }
        if (!read.value.has_value()) {
            return litebus::Future<::messages::FailReusableSnapshotResponse>(
                Success<::messages::FailReusableSnapshotResponse>(request.requestid()));
        }
        ::messages::ReusableSnapshotMetadata current;
        if (!ParseMetadata(read.value.value(), &current)) {
            return litebus::Future<::messages::FailReusableSnapshotResponse>(
                ErrorResponse<::messages::FailReusableSnapshotResponse>(request.requestid(),
                    Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "invalid Snapshot record")));
        }
        if (current.createrequestid() != request.requestid()
            || current.phase() != ::messages::REUSABLE_SNAPSHOT_PUBLISHING) {
            return litebus::Future<::messages::FailReusableSnapshotResponse>(
                ErrorResponse<::messages::FailReusableSnapshotResponse>(request.requestid(),
                    Conflict("only the matching PUBLISHING Snapshot can fail")));
        }
        return persistence_->CompareAndSwap(key, read.value, std::nullopt)
            .Then([request](const ReusableSnapshotCasResult &cas) {
                if (cas.status.IsError() || !cas.swapped) {
                    return ErrorResponse<::messages::FailReusableSnapshotResponse>(request.requestid(),
                        cas.status.IsError() ? cas.status : Conflict("concurrent Snapshot transition won the CAS"));
                }
                return Success<::messages::FailReusableSnapshotResponse>(request.requestid());
            });
    });
}

litebus::Future<::messages::GetReusableSnapshotResponse> ReusableSnapshotStore::Get(
    const ::messages::GetReusableSnapshotRequest &request)
{
    if (request.tenantid().empty() || request.snapshotid().empty() || persistence_ == nullptr) {
        return litebus::Future<::messages::GetReusableSnapshotResponse>(
            ErrorResponse<::messages::GetReusableSnapshotResponse>(
                request.requestid(), Invalid("tenantID and snapshotID are required")));
    }
    return persistence_->Read(RecordKey(request.tenantid(), request.snapshotid()))
        .Then([request](const ReusableSnapshotReadResult &read) {
            if (read.status.IsError() || !read.value.has_value()) {
                return ErrorResponse<::messages::GetReusableSnapshotResponse>(request.requestid(),
                    read.status.IsError() ? read.status : NotFound("Snapshot not found"));
            }
            ::messages::ReusableSnapshotMetadata metadata;
            if (!ParseMetadata(read.value.value(), &metadata)
                || metadata.phase() != ::messages::REUSABLE_SNAPSHOT_READY) {
                return ErrorResponse<::messages::GetReusableSnapshotResponse>(
                    request.requestid(), NotFound("READY Snapshot not found"));
            }
            auto response = Success<::messages::GetReusableSnapshotResponse>(request.requestid());
            *response.mutable_snapshotinfo() = PublicInfo(metadata);
            return response;
        });
}

litebus::Future<::messages::ListReusableSnapshotsResponse> ReusableSnapshotStore::List(
    const ::messages::ListReusableSnapshotsRequest &request)
{
    if (request.tenantid().empty() || persistence_ == nullptr) {
        return litebus::Future<::messages::ListReusableSnapshotsResponse>(
            ErrorResponse<::messages::ListReusableSnapshotsResponse>(
                request.requestid(), Invalid("tenantID is required")));
    }
    if (request.pagesize() > MAX_PAGE_SIZE) {
        return litebus::Future<::messages::ListReusableSnapshotsResponse>(
            ErrorResponse<::messages::ListReusableSnapshotsResponse>(
                request.requestid(), Invalid("pageSize exceeds limit")));
    }
    return persistence_->List(TenantPrefix(request.tenantid()))
        .Then([request](const ReusableSnapshotListRecordsResult &listed) {
            if (listed.status.IsError()) {
                return ErrorResponse<::messages::ListReusableSnapshotsResponse>(request.requestid(), listed.status);
            }
            std::vector<::messages::ReusableSnapshotMetadata> records;
            records.reserve(listed.values.size());
            for (const auto &value : listed.values) {
                ::messages::ReusableSnapshotMetadata metadata;
                if (!ParseMetadata(value, &metadata)) {
                    return ErrorResponse<::messages::ListReusableSnapshotsResponse>(request.requestid(),
                        Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "invalid Snapshot record in tenant list"));
                }
                if (metadata.phase() == ::messages::REUSABLE_SNAPSHOT_READY && HasName(metadata, request.name())) {
                    records.emplace_back(std::move(metadata));
                }
            }
            std::sort(records.begin(), records.end(), [](const auto &left, const auto &right) {
                return left.snapshotid() < right.snapshotid();
            });
            auto response = Success<::messages::ListReusableSnapshotsResponse>(request.requestid());
            const uint32_t pageSize = request.pagesize() == 0 ? DEFAULT_PAGE_SIZE : request.pagesize();
            auto iter = request.pagetoken().empty() ? records.begin()
                : std::upper_bound(records.begin(), records.end(), request.pagetoken(),
                    [](const std::string &token, const auto &metadata) { return token < metadata.snapshotid(); });
            uint32_t count = 0;
            for (; iter != records.end() && count < pageSize; ++iter, ++count) {
                *response.add_snapshotinfos() = PublicInfo(*iter);
            }
            if (iter != records.end() && response.snapshotinfos_size() > 0) {
                response.set_nextpagetoken(response.snapshotinfos(response.snapshotinfos_size() - 1).snapshotid());
            }
            return response;
        });
}

litebus::Future<::messages::ResolveReusableSnapshotForCreateResponse> ReusableSnapshotStore::Resolve(
    const ::messages::ResolveReusableSnapshotForCreateRequest &request)
{
    if (request.tenantid().empty() || request.snapshotid().empty() || persistence_ == nullptr) {
        return litebus::Future<::messages::ResolveReusableSnapshotForCreateResponse>(
            ErrorResponse<::messages::ResolveReusableSnapshotForCreateResponse>(request.requestid(),
                Invalid("tenantID and snapshotID are required")));
    }
    return persistence_->Read(RecordKey(request.tenantid(), request.snapshotid()))
        .Then([request](const ReusableSnapshotReadResult &read) {
            if (read.status.IsError() || !read.value.has_value()) {
                return ErrorResponse<::messages::ResolveReusableSnapshotForCreateResponse>(request.requestid(),
                    read.status.IsError() ? read.status : NotFound("Snapshot not found"));
            }
            ::messages::ReusableSnapshotMetadata metadata;
            if (!ParseMetadata(read.value.value(), &metadata)
                || metadata.phase() != ::messages::REUSABLE_SNAPSHOT_READY) {
                return ErrorResponse<::messages::ResolveReusableSnapshotForCreateResponse>(
                    request.requestid(), Conflict("Snapshot is not READY"));
            }
            auto response = Success<::messages::ResolveReusableSnapshotForCreateResponse>(request.requestid());
            *response.mutable_instancetemplate() = metadata.instancetemplate();
            response.mutable_reusablesnapshotrestore()->set_snapshotid(metadata.snapshotid());
            *response.mutable_reusablesnapshotrestore()->mutable_artifact() = metadata.artifact();
            response.mutable_reusablesnapshotrestore()->set_allowlogicalinstanceidrebind(true);
            response.set_snapshotversion(metadata.version());
            return response;
        });
}

litebus::Future<::messages::BeginDeleteReusableSnapshotResponse> ReusableSnapshotStore::BeginDelete(
    const ::messages::BeginDeleteReusableSnapshotRequest &request)
{
    if (request.tenantid().empty() || request.snapshotid().empty() || persistence_ == nullptr) {
        return litebus::Future<::messages::BeginDeleteReusableSnapshotResponse>(
            ErrorResponse<::messages::BeginDeleteReusableSnapshotResponse>(request.requestid(),
                Invalid("tenantID and snapshotID are required")));
    }
    const auto key = RecordKey(request.tenantid(), request.snapshotid());
    return persistence_->Read(key).Then([this, request, key](const ReusableSnapshotReadResult &read)
        -> litebus::Future<::messages::BeginDeleteReusableSnapshotResponse> {
        if (read.status.IsError()) {
            return litebus::Future<::messages::BeginDeleteReusableSnapshotResponse>(
                ErrorResponse<::messages::BeginDeleteReusableSnapshotResponse>(request.requestid(), read.status));
        }
        if (!read.value.has_value()) {
            auto response = Success<::messages::BeginDeleteReusableSnapshotResponse>(request.requestid());
            response.set_alreadydeleted(true);
            return litebus::Future<::messages::BeginDeleteReusableSnapshotResponse>(response);
        }
        ::messages::ReusableSnapshotMetadata current;
        if (!ParseMetadata(read.value.value(), &current)) {
            return litebus::Future<::messages::BeginDeleteReusableSnapshotResponse>(
                ErrorResponse<::messages::BeginDeleteReusableSnapshotResponse>(request.requestid(),
                    Status(StatusCode::ERR_ETCD_OPERATION_ERROR, "invalid Snapshot record")));
        }
        if (current.phase() == ::messages::REUSABLE_SNAPSHOT_DELETING) {
            auto response = Success<::messages::BeginDeleteReusableSnapshotResponse>(request.requestid());
            *response.mutable_artifact() = current.artifact();
            response.set_expectedversion(current.version());
            return litebus::Future<::messages::BeginDeleteReusableSnapshotResponse>(response);
        }
        if (current.phase() != ::messages::REUSABLE_SNAPSHOT_READY) {
            return litebus::Future<::messages::BeginDeleteReusableSnapshotResponse>(
                ErrorResponse<::messages::BeginDeleteReusableSnapshotResponse>(request.requestid(),
                    Conflict("only a READY Snapshot can begin deletion")));
        }
        auto deleting = current;
        deleting.set_phase(::messages::REUSABLE_SNAPSHOT_DELETING);
        deleting.set_updatetime(Now());
        deleting.set_version(current.version() + 1);
        return persistence_->CompareAndSwap(key, read.value, deleting.SerializeAsString())
            .Then([this, request, deleting](const ReusableSnapshotCasResult &cas)
                -> litebus::Future<::messages::BeginDeleteReusableSnapshotResponse> {
                if (cas.status.IsError()) {
                    return litebus::Future<::messages::BeginDeleteReusableSnapshotResponse>(
                        ErrorResponse<::messages::BeginDeleteReusableSnapshotResponse>(request.requestid(),
                            cas.status));
                }
                if (!cas.swapped) {
                    // The compare failed only because another writer changed
                    // the exact record read above. Re-read that authoritative
                    // winner: DELETING joins its physical cleanup and absence
                    // is an already-completed idempotent Delete.
                    return BeginDelete(request);
                }
                auto response = Success<::messages::BeginDeleteReusableSnapshotResponse>(request.requestid());
                *response.mutable_artifact() = deleting.artifact();
                response.set_expectedversion(deleting.version());
                return litebus::Future<::messages::BeginDeleteReusableSnapshotResponse>(response);
            });
    });
}

litebus::Future<::messages::CompleteDeleteReusableSnapshotResponse> ReusableSnapshotStore::CompleteDelete(
    const ::messages::CompleteDeleteReusableSnapshotRequest &request)
{
    if (request.tenantid().empty() || request.snapshotid().empty() || request.expectedversion() == 0
        || persistence_ == nullptr) {
        return litebus::Future<::messages::CompleteDeleteReusableSnapshotResponse>(
            ErrorResponse<::messages::CompleteDeleteReusableSnapshotResponse>(request.requestid(),
                Invalid("tenantID, snapshotID and expectedVersion are required")));
    }
    const auto key = RecordKey(request.tenantid(), request.snapshotid());
    return persistence_->Read(key).Then([this, request, key](const ReusableSnapshotReadResult &read)
        -> litebus::Future<::messages::CompleteDeleteReusableSnapshotResponse> {
        if (read.status.IsError()) {
            return litebus::Future<::messages::CompleteDeleteReusableSnapshotResponse>(
                ErrorResponse<::messages::CompleteDeleteReusableSnapshotResponse>(request.requestid(), read.status));
        }
        if (!read.value.has_value()) {
            return litebus::Future<::messages::CompleteDeleteReusableSnapshotResponse>(
                Success<::messages::CompleteDeleteReusableSnapshotResponse>(request.requestid()));
        }
        ::messages::ReusableSnapshotMetadata current;
        if (!ParseMetadata(read.value.value(), &current)
            || current.phase() != ::messages::REUSABLE_SNAPSHOT_DELETING
            || current.version() != request.expectedversion()) {
            return litebus::Future<::messages::CompleteDeleteReusableSnapshotResponse>(
                ErrorResponse<::messages::CompleteDeleteReusableSnapshotResponse>(request.requestid(),
                    Conflict("Snapshot deleting version changed")));
        }
        return persistence_->CompareAndSwap(key, read.value, std::nullopt)
            .Then([this, request, key](const ReusableSnapshotCasResult &cas)
                -> litebus::Future<::messages::CompleteDeleteReusableSnapshotResponse> {
                if (cas.status.IsError()) {
                    return litebus::Future<::messages::CompleteDeleteReusableSnapshotResponse>(
                        ErrorResponse<::messages::CompleteDeleteReusableSnapshotResponse>(request.requestid(),
                            cas.status));
                }
                if (cas.swapped) {
                    return litebus::Future<::messages::CompleteDeleteReusableSnapshotResponse>(
                        Success<::messages::CompleteDeleteReusableSnapshotResponse>(request.requestid()));
                }

                // A concurrent completion may have deleted the exact same
                // DELETING record between our Read and CAS. Treat only
                // authoritative absence as idempotent success; a replacement
                // record remains a real version conflict.
                return persistence_->Read(key).Then([request](const ReusableSnapshotReadResult &winner) {
                    if (winner.status.IsError()) {
                        return ErrorResponse<::messages::CompleteDeleteReusableSnapshotResponse>(
                            request.requestid(), winner.status);
                    }
                    if (!winner.value.has_value()) {
                        return Success<::messages::CompleteDeleteReusableSnapshotResponse>(request.requestid());
                    }
                    return ErrorResponse<::messages::CompleteDeleteReusableSnapshotResponse>(request.requestid(),
                        Conflict("concurrent Snapshot completion changed the deleting record"));
                });
            });
    });
}

litebus::Future<::messages::DeleteReusableSnapshotResponse> ReusableSnapshotStore::Delete(
    const ::messages::DeleteReusableSnapshotRequest &request)
{
    if (request.tenantid().empty() || request.snapshotid().empty() || persistence_ == nullptr) {
        return litebus::Future<::messages::DeleteReusableSnapshotResponse>(
            ErrorResponse<::messages::DeleteReusableSnapshotResponse>(request.requestid(),
                Invalid("tenantID and snapshotID are required")));
    }
    // An active DELETE must never publish DELETING unless there is a real
    // physical artifact deletion path that can drive it to completion. The
    // dispatcher is deliberately injected by the Master integration layer;
    // this store does not fabricate a successful physical delete.
    if (!artifactDeleter_) {
        return persistence_->Read(RecordKey(request.tenantid(), request.snapshotid()))
            .Then([request](const ReusableSnapshotReadResult &read) {
                if (read.status.IsError()) {
                    return ErrorResponse<::messages::DeleteReusableSnapshotResponse>(request.requestid(), read.status);
                }
                if (!read.value.has_value()) {
                    return Success<::messages::DeleteReusableSnapshotResponse>(request.requestid());
                }
                return ErrorResponse<::messages::DeleteReusableSnapshotResponse>(request.requestid(),
                    Status(StatusCode::RUNTIME_MANAGER_NOT_IMPLEMENTED,
                           "reusable Snapshot artifact deleter is not bound"));
            });
    }
    ::messages::BeginDeleteReusableSnapshotRequest begin;
    begin.set_requestid(request.requestid());
    begin.set_tenantid(request.tenantid());
    begin.set_snapshotid(request.snapshotid());
    return BeginDelete(begin).Then([this, request](const ::messages::BeginDeleteReusableSnapshotResponse &pending)
        -> litebus::Future<::messages::DeleteReusableSnapshotResponse> {
        if (pending.code() != common::ERR_NONE) {
            return litebus::Future<::messages::DeleteReusableSnapshotResponse>(
                ErrorResponse<::messages::DeleteReusableSnapshotResponse>(request.requestid(),
                    Status(StatusCode::FAILED, pending.message())));
        }
        if (pending.alreadydeleted()) {
            return litebus::Future<::messages::DeleteReusableSnapshotResponse>(
                Success<::messages::DeleteReusableSnapshotResponse>(request.requestid()));
        }
        ::messages::DeleteReusableSnapshotArtifactRequest physical;
        physical.set_requestid(request.requestid());
        physical.set_tenantid(request.tenantid());
        physical.set_snapshotid(request.snapshotid());
        *physical.mutable_artifact() = pending.artifact();
        return artifactDeleter_(physical)
            .Then([this, request, pending](const ::messages::DeleteReusableSnapshotArtifactResponse &deleted)
                -> litebus::Future<::messages::DeleteReusableSnapshotResponse> {
                if (deleted.code() != common::ERR_NONE) {
                    return litebus::Future<::messages::DeleteReusableSnapshotResponse>(
                        ErrorResponse<::messages::DeleteReusableSnapshotResponse>(request.requestid(),
                            Status(StatusCode::FAILED, deleted.message())));
                }
                ::messages::CompleteDeleteReusableSnapshotRequest complete;
                complete.set_requestid(request.requestid());
                complete.set_tenantid(request.tenantid());
                complete.set_snapshotid(request.snapshotid());
                complete.set_expectedversion(pending.expectedversion());
                return CompleteDelete(complete)
                    .Then([request](const ::messages::CompleteDeleteReusableSnapshotResponse &response) {
                        if (response.code() != common::ERR_NONE) {
                            return ErrorResponse<::messages::DeleteReusableSnapshotResponse>(request.requestid(),
                                Status(StatusCode::FAILED, response.message()));
                        }
                        return Success<::messages::DeleteReusableSnapshotResponse>(request.requestid());
                    });
            });
    });
}

litebus::Future<Status> ReusableSnapshotStore::DeleteLocalSnapshotsForSource(
    const std::string &tenantID, const std::string &sourceInstanceID)
{
    if (tenantID.empty() || sourceInstanceID.empty() || persistence_ == nullptr) {
        return litebus::Future<Status>(
            Invalid("tenantID and sourceInstanceID are required for local Snapshot cleanup"));
    }
    return persistence_->List(TenantPrefix(tenantID))
        .Then([](const ReusableSnapshotListRecordsResult &records)
            -> litebus::Future<Status> {
            if (records.status.IsError()) {
                return litebus::Future<Status>(records.status);
            }
            // A named reusable Snapshot owns its lifecycle independently of its
            // source instance. Source-exit cleanup must NOT delete it:
            //   - READY survives until an explicit Delete (the only sanctioned
            //     removal); no TTL GC currently covers /yr/snapshots/v1/.
            //   - PUBLISHING keeps its record/ownership so a legal Commit can
            //     still publish READY after the source is gone.
            //   - DELETING belongs to its already-authorized delete attempt.
            //     Re-reading the key from here cannot distinguish a fresh
            //     recreate-and-READY from the record this scan saw, so this
            //     path must not issue any identity-less delete; the explicit
            //     Delete retry drives completion instead.
            return litebus::Future<Status>(Status::OK());
        });
}

void ReusableSnapshotStore::SetArtifactDeleter(ArtifactDeleter deleter)
{
    artifactDeleter_ = std::move(deleter);
}

litebus::Future<std::optional<::messages::ReusableSnapshotMetadata>> ReusableSnapshotStore::ReadForTest(
    const std::string &tenantID, const std::string &snapshotID)
{
    if (persistence_ == nullptr) {
        return litebus::Future<std::optional<::messages::ReusableSnapshotMetadata>>(std::nullopt);
    }
    return persistence_->Read(RecordKey(tenantID, snapshotID)).Then([](const ReusableSnapshotReadResult &read) {
        std::optional<::messages::ReusableSnapshotMetadata> result;
        ::messages::ReusableSnapshotMetadata metadata;
        if (read.status.IsOk() && read.value.has_value() && ParseMetadata(read.value.value(), &metadata)) {
            result = std::move(metadata);
        }
        return result;
    });
}

}  // namespace functionsystem::snap_manager
