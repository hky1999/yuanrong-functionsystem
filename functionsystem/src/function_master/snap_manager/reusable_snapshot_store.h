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

#ifndef FUNCTION_MASTER_SNAP_MANAGER_REUSABLE_SNAPSHOT_STORE_H
#define FUNCTION_MASTER_SNAP_MANAGER_REUSABLE_SNAPSHOT_STORE_H

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "async/future.hpp"
#include "common/proto/pb/posix/message.pb.h"
#include "common/status/status.h"
#include "meta_store_client/meta_store_client.h"

namespace functionsystem::snap_manager {

struct ReusableSnapshotReadResult {
    Status status{ Status::OK() };
    std::optional<std::string> value;
};

struct ReusableSnapshotListRecordsResult {
    Status status{ Status::OK() };
    std::vector<std::string> values;
};

struct ReusableSnapshotCasResult {
    Status status{ Status::OK() };
    bool swapped{ false };
};

class ReusableSnapshotPersistence {
public:
    virtual ~ReusableSnapshotPersistence() = default;

    virtual litebus::Future<ReusableSnapshotReadResult> Read(const std::string &key) = 0;
    virtual litebus::Future<ReusableSnapshotListRecordsResult> List(const std::string &prefix) = 0;
    virtual litebus::Future<ReusableSnapshotCasResult> CompareAndSwap(
        const std::string &key, const std::optional<std::string> &expected,
        const std::optional<std::string> &replacement) = 0;
};

class EtcdReusableSnapshotPersistence final : public ReusableSnapshotPersistence {
public:
    explicit EtcdReusableSnapshotPersistence(std::shared_ptr<MetaStoreClient> client);

    litebus::Future<ReusableSnapshotReadResult> Read(const std::string &key) override;
    litebus::Future<ReusableSnapshotListRecordsResult> List(const std::string &prefix) override;
    litebus::Future<ReusableSnapshotCasResult> CompareAndSwap(
        const std::string &key, const std::optional<std::string> &expected,
        const std::optional<std::string> &replacement) override;

private:
    std::shared_ptr<MetaStoreClient> client_;
};

class ReusableSnapshotStore : public std::enable_shared_from_this<ReusableSnapshotStore> {
public:
    using Clock = std::function<int64_t()>;
    using ArtifactDeleter = std::function<litebus::Future<::messages::DeleteReusableSnapshotArtifactResponse>(
        const ::messages::DeleteReusableSnapshotArtifactRequest &)>;

    explicit ReusableSnapshotStore(std::shared_ptr<ReusableSnapshotPersistence> persistence,
                                   Clock clock = nullptr);

    static std::string TenantPrefix(const std::string &tenantID);
    static std::string RecordKey(const std::string &tenantID, const std::string &snapshotID);
    static std::string SnapshotID(const ::messages::BeginReusableSnapshotRequest &request);
    static ::resources::InstanceInfo SanitizeInstanceTemplate(const ::resources::InstanceInfo &source);

    litebus::Future<::messages::BeginReusableSnapshotResponse> Begin(
        const ::messages::BeginReusableSnapshotRequest &request);
    litebus::Future<::messages::CommitReusableSnapshotResponse> Commit(
        const ::messages::CommitReusableSnapshotRequest &request);
    litebus::Future<::messages::FailReusableSnapshotResponse> Fail(
        const ::messages::FailReusableSnapshotRequest &request);
    litebus::Future<::messages::GetReusableSnapshotResponse> Get(
        const ::messages::GetReusableSnapshotRequest &request);
    litebus::Future<::messages::ListReusableSnapshotsResponse> List(
        const ::messages::ListReusableSnapshotsRequest &request);
    litebus::Future<::messages::ResolveReusableSnapshotForCreateResponse> Resolve(
        const ::messages::ResolveReusableSnapshotForCreateRequest &request);
    litebus::Future<::messages::BeginDeleteReusableSnapshotResponse> BeginDelete(
        const ::messages::BeginDeleteReusableSnapshotRequest &request);
    litebus::Future<::messages::CompleteDeleteReusableSnapshotResponse> CompleteDelete(
        const ::messages::CompleteDeleteReusableSnapshotRequest &request);
    litebus::Future<::messages::DeleteReusableSnapshotResponse> Delete(
        const ::messages::DeleteReusableSnapshotRequest &request);
    // Source-exit retention scan. A named reusable Snapshot owns its own
    // lifecycle: this path NEVER deletes records bound to the exited source
    // (READY survives for explicit Delete; PUBLISHING keeps ownership for a
    // later Commit; DELETING stays with its authorized delete attempt — a
    // key re-read from here cannot be identity-pinned to that attempt).
    // Only persistence readability is validated; anonymous/park checkpoint
    // cleanup lives outside this store and is unaffected.
    litebus::Future<Status> DeleteLocalSnapshotsForSource(
        const std::string &tenantID, const std::string &sourceInstanceID);

    void SetArtifactDeleter(ArtifactDeleter deleter);

    // Test/diagnostic read of the single authoritative record. Public HTTP
    // handlers never expose this internal metadata.
    litebus::Future<std::optional<::messages::ReusableSnapshotMetadata>> ReadForTest(
        const std::string &tenantID, const std::string &snapshotID);

private:
    static Status ValidateBegin(const ::messages::BeginReusableSnapshotRequest &request);
    static Status ValidateArtifact(const ::messages::SnapshotArtifact &artifact);
    static bool HasName(const ::messages::ReusableSnapshotMetadata &metadata, const std::string &name);
    static ::core_service::SnapshotInfo PublicInfo(const ::messages::ReusableSnapshotMetadata &metadata);

    template <typename Response>
    static Response ErrorResponse(const std::string &requestID, const Status &status)
    {
        Response response;
        response.set_requestid(requestID);
        response.set_code(static_cast<int32_t>(Status::GetPosixErrorCode(status.StatusCode())));
        response.set_message(status.GetMessage());
        return response;
    }

    int64_t Now() const;

    std::shared_ptr<ReusableSnapshotPersistence> persistence_;
    Clock clock_;
    ArtifactDeleter artifactDeleter_;
};

}  // namespace functionsystem::snap_manager

#endif  // FUNCTION_MASTER_SNAP_MANAGER_REUSABLE_SNAPSHOT_STORE_H
