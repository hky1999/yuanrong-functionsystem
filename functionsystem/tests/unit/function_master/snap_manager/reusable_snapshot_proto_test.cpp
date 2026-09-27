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

#include <gtest/gtest.h>

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/proto/pb/posix/core_service.pb.h"
#include "common/proto/pb/posix/message.pb.h"
#include "common/proto/pb/posix/resource.pb.h"

namespace functionsystem::snap_manager {
namespace {

std::set<std::string> FieldNames(const google::protobuf::Descriptor *descriptor)
{
    std::set<std::string> names;
    for (int i = 0; i < descriptor->field_count(); ++i) {
        names.emplace(descriptor->field(i)->name());
    }
    return names;
}

void ExpectFieldNumbers(const google::protobuf::Descriptor *descriptor,
                        const std::vector<std::pair<std::string, int>> &expected)
{
    ASSERT_NE(descriptor, nullptr);
    ASSERT_EQ(descriptor->field_count(), static_cast<int>(expected.size()));
    for (const auto &[name, number] : expected) {
        const auto *field = descriptor->FindFieldByName(name);
        ASSERT_NE(field, nullptr) << name;
        EXPECT_EQ(field->number(), number) << name;
    }
}

TEST(ReusableSnapshotProtoContractTest, FreezesPublicAndInternalShapeWithoutChangingPauseResumeEnums)
{
    EXPECT_EQ(::resources::SNAPSHOT_STATUS_UNSPECIFIED, 0);
    EXPECT_EQ(::resources::SNAPSHOT_READY, 1);
    EXPECT_EQ(::resources::SNAPSHOT_DELETING, 2);

    EXPECT_EQ(::messages::REUSABLE_SNAPSHOT_PHASE_UNSPECIFIED, 0);
    EXPECT_EQ(::messages::REUSABLE_SNAPSHOT_PUBLISHING, 1);
    EXPECT_EQ(::messages::REUSABLE_SNAPSHOT_READY, 2);
    EXPECT_EQ(::messages::REUSABLE_SNAPSHOT_DELETING, 3);
    const auto *phase = ::messages::ReusableSnapshotPhase_descriptor();
    ASSERT_NE(phase, nullptr);
    EXPECT_EQ(phase->value_count(), 4);
    EXPECT_EQ(phase->FindValueByName("REUSABLE_SNAPSHOT_FAILED"), nullptr);
    EXPECT_EQ(phase->FindValueByName("REUSABLE_SNAPSHOT_DELETED"), nullptr);

    EXPECT_EQ(FieldNames(::core_service::SnapshotInfo::descriptor()),
              (std::set<std::string>{ "snapshotID", "names" }));
    const auto *createSnapshotID = ::core_service::CreateRequest::descriptor()->FindFieldByName("snapshotID");
    ASSERT_NE(createSnapshotID, nullptr);
    EXPECT_EQ(createSnapshotID->number(), 9);
    const auto *snapshotName = ::core_service::SnapOptions::descriptor()->FindFieldByName("name");
    ASSERT_NE(snapshotName, nullptr);
    EXPECT_EQ(snapshotName->number(), 5);

    EXPECT_EQ(FieldNames(::messages::SnapshotArtifact::descriptor()),
              (std::set<std::string>{ "storageBackend", "objectKey", "size", "sha256", "format",
                                      "formatVersion", "sourceNodeID" }));
    EXPECT_EQ(FieldNames(::messages::ReusableSnapshotMetadata::descriptor()),
              (std::set<std::string>{ "snapshotID", "names", "instanceTemplate", "artifact", "tenantID",
                                      "createRequestID", "phase", "createTime",
                                      "updateTime", "version", "sourceInstanceID" }));
    ExpectFieldNumbers(::messages::ReusableSnapshotMetadata::descriptor(),
                       { { "snapshotID", 1 }, { "names", 2 }, { "instanceTemplate", 3 }, { "artifact", 4 },
                         { "tenantID", 5 }, { "createRequestID", 6 }, { "phase", 7 }, { "createTime", 8 },
                         { "updateTime", 9 }, { "version", 10 }, { "sourceInstanceID", 11 } });
    EXPECT_EQ(FieldNames(::messages::ReusableSnapshotRestore::descriptor()),
              (std::set<std::string>{ "snapshotID", "artifact", "allowLogicalInstanceIDRebind" }));

    const auto *deployRestore = ::messages::DeployInstanceRequest::descriptor()->FindFieldByName(
        "reusableSnapshotRestore");
    ASSERT_NE(deployRestore, nullptr);
    EXPECT_EQ(deployRestore->number(), 41);
    const auto *runtimeRestore = ::messages::RuntimeInstanceInfo::descriptor()->FindFieldByName(
        "reusableSnapshotRestore");
    ASSERT_NE(runtimeRestore, nullptr);
    EXPECT_EQ(runtimeRestore->number(), 13);

    ExpectFieldNumbers(::messages::BeginReusableSnapshotRequest::descriptor(),
                       { { "requestID", 1 }, { "tenantID", 2 }, { "sourceInstanceID", 3 },
                         { "names", 4 } });
    ExpectFieldNumbers(::messages::BeginReusableSnapshotResponse::descriptor(),
                       { { "code", 1 }, { "message", 2 }, { "requestID", 3 }, { "snapshotID", 4 },
                         { "phase", 5 }, { "snapshotInfo", 6 } });
    ExpectFieldNumbers(::messages::CommitReusableSnapshotRequest::descriptor(),
                       { { "requestID", 1 }, { "tenantID", 2 }, { "snapshotID", 3 },
                         { "sourceInstanceInfo", 4 }, { "artifact", 5 } });
    ExpectFieldNumbers(::messages::CommitReusableSnapshotResponse::descriptor(),
                       { { "code", 1 }, { "message", 2 }, { "requestID", 3 }, { "snapshotInfo", 4 },
                         { "version", 5 } });
    ExpectFieldNumbers(::messages::FailReusableSnapshotRequest::descriptor(),
                       { { "requestID", 1 }, { "tenantID", 2 }, { "snapshotID", 3 },
                         { "reason", 4 } });
    ExpectFieldNumbers(::messages::FailReusableSnapshotResponse::descriptor(),
                       { { "code", 1 }, { "message", 2 }, { "requestID", 3 } });
    ExpectFieldNumbers(::messages::GetReusableSnapshotRequest::descriptor(),
                       { { "requestID", 1 }, { "tenantID", 2 }, { "snapshotID", 3 } });
    ExpectFieldNumbers(::messages::GetReusableSnapshotResponse::descriptor(),
                       { { "code", 1 }, { "message", 2 }, { "requestID", 3 }, { "snapshotInfo", 4 } });
    ExpectFieldNumbers(::messages::ListReusableSnapshotsRequest::descriptor(),
                       { { "requestID", 1 }, { "tenantID", 2 }, { "name", 3 }, { "pageToken", 4 },
                         { "pageSize", 5 } });
    ExpectFieldNumbers(::messages::ListReusableSnapshotsResponse::descriptor(),
                       { { "code", 1 }, { "message", 2 }, { "requestID", 3 }, { "snapshotInfos", 4 },
                         { "nextPageToken", 5 } });
    ExpectFieldNumbers(::messages::BeginDeleteReusableSnapshotRequest::descriptor(),
                       { { "requestID", 1 }, { "tenantID", 2 }, { "snapshotID", 3 } });
    ExpectFieldNumbers(::messages::BeginDeleteReusableSnapshotResponse::descriptor(),
                       { { "code", 1 }, { "message", 2 }, { "requestID", 3 }, { "artifact", 4 },
                         { "expectedVersion", 5 }, { "alreadyDeleted", 6 } });
    ExpectFieldNumbers(::messages::CompleteDeleteReusableSnapshotRequest::descriptor(),
                       { { "requestID", 1 }, { "tenantID", 2 }, { "snapshotID", 3 },
                         { "expectedVersion", 4 } });
    ExpectFieldNumbers(::messages::CompleteDeleteReusableSnapshotResponse::descriptor(),
                       { { "code", 1 }, { "message", 2 }, { "requestID", 3 } });
    ExpectFieldNumbers(::messages::DeleteReusableSnapshotRequest::descriptor(),
                       { { "requestID", 1 }, { "tenantID", 2 }, { "snapshotID", 3 } });
    ExpectFieldNumbers(::messages::DeleteReusableSnapshotResponse::descriptor(),
                       { { "code", 1 }, { "message", 2 }, { "requestID", 3 } });
    ExpectFieldNumbers(::messages::DeleteReusableSnapshotArtifactRequest::descriptor(),
                       { { "requestID", 1 }, { "tenantID", 2 }, { "snapshotID", 3 }, { "artifact", 4 } });
    ExpectFieldNumbers(::messages::DeleteReusableSnapshotArtifactResponse::descriptor(),
                       { { "code", 1 }, { "message", 2 }, { "requestID", 3 } });
    ExpectFieldNumbers(::messages::ResolveReusableSnapshotForCreateRequest::descriptor(),
                       { { "requestID", 1 }, { "tenantID", 2 }, { "snapshotID", 3 } });
    ExpectFieldNumbers(::messages::ResolveReusableSnapshotForCreateResponse::descriptor(),
                       { { "code", 1 }, { "message", 2 }, { "requestID", 3 },
                         { "instanceTemplate", 4 }, { "reusableSnapshotRestore", 5 },
                         { "snapshotVersion", 6 } });

    const auto *leaveRunning = ::messages::SnapshotRuntimeRequest::descriptor()->FindFieldByName("leaveRunning");
    ASSERT_NE(leaveRunning, nullptr);
    // INTERNAL SnapshotRuntimeRequest wire (proxy<->agent, same image both
    // sides): the 2026-09-11 scheduler-line rebase (b24dcff0) moved this
    // from 12 to 23 and this test — whose target stopped compiling after
    // the G3b backport — was never updated. Frozen to the value the
    // RUNNING baseline 1bfe1bb9 itself carries; the freeze semantics are
    // unchanged (any future move still fails here).
    EXPECT_EQ(leaveRunning->number(), 23);
    const auto *runtimeArtifact =
        ::messages::SnapshotRuntimeResponse::descriptor()->FindFieldByName("reusableSnapshotArtifact");
    ASSERT_NE(runtimeArtifact, nullptr);
    EXPECT_EQ(runtimeArtifact->number(), 9);

    EXPECT_EQ(::messages::REUSABLE_SNAPSHOT_COMMITTED, 6);
    EXPECT_EQ(::messages::REUSABLE_SNAPSHOT_ABORTED, 7);
}

}  // namespace
}  // namespace functionsystem::snap_manager
