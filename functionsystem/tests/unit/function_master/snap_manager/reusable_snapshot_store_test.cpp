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

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "function_master/snap_manager/reusable_snapshot_store.h"

namespace functionsystem::snap_manager {
namespace {

class MemoryReusableSnapshotPersistence final : public ReusableSnapshotPersistence {
public:
    litebus::Future<ReusableSnapshotReadResult> Read(const std::string &key) override
    {
        readKeys.emplace_back(key);
        ReusableSnapshotReadResult result;
        result.status = readStatus;
        if (auto iter = values.find(key); iter != values.end()) {
            result.value = iter->second;
        }
        return litebus::Future<ReusableSnapshotReadResult>(result);
    }

    litebus::Future<ReusableSnapshotListRecordsResult> List(const std::string &prefix) override
    {
        listPrefixes.emplace_back(prefix);
        ReusableSnapshotListRecordsResult result;
        result.status = readStatus;
        for (const auto &[key, value] : values) {
            if (key.rfind(prefix, 0) == 0) {
                result.values.emplace_back(value);
            }
        }
        return litebus::Future<ReusableSnapshotListRecordsResult>(result);
    }

    litebus::Future<ReusableSnapshotCasResult> CompareAndSwap(
        const std::string &key, const std::optional<std::string> &expected,
        const std::optional<std::string> &replacement) override
    {
        casKeys.emplace_back(key);
        ReusableSnapshotCasResult result;
        result.status = writeStatus;
        if (writeStatus.IsError() || forceCasMiss) {
            if (forceCasMiss && installReplacementOnCasMiss && replacement.has_value()) {
                values[key] = replacement.value();
            }
            if (forceCasMiss && eraseRecordOnCasMiss) {
                values.erase(key);
            }
            result.swapped = false;
            return litebus::Future<ReusableSnapshotCasResult>(result);
        }
        const auto iter = values.find(key);
        const bool matches = expected.has_value()
            ? iter != values.end() && iter->second == expected.value()
            : iter == values.end();
        result.swapped = matches;
        if (matches) {
            if (replacement.has_value()) {
                values[key] = replacement.value();
            } else {
                values.erase(key);
            }
        }
        return litebus::Future<ReusableSnapshotCasResult>(result);
    }

    std::map<std::string, std::string> values;
    std::vector<std::string> readKeys;
    std::vector<std::string> listPrefixes;
    std::vector<std::string> casKeys;
    Status readStatus{ Status::OK() };
    Status writeStatus{ Status::OK() };
    bool forceCasMiss{ false };
    bool installReplacementOnCasMiss{ false };
    bool eraseRecordOnCasMiss{ false };
};

::messages::SnapshotArtifact ReadyArtifact()
{
    ::messages::SnapshotArtifact artifact;
    artifact.set_storagebackend("obs");
    artifact.set_objectkey("reusable/v1/tenant/snap/checkpoint.img");
    artifact.set_size(4096);
    artifact.set_sha256(std::string(64, 'a'));
    artifact.set_format("sandboxd-checkpoint");
    artifact.set_formatversion(1);
    return artifact;
}

::messages::SnapshotArtifact LocalArtifact()
{
    auto artifact = ReadyArtifact();
    artifact.set_storagebackend("local");
    artifact.set_objectkey("local-snapshot");
    artifact.clear_sha256();
    artifact.set_sourcenodeid("source-proxy");
    return artifact;
}

::resources::InstanceInfo SourceInstance()
{
    ::resources::InstanceInfo source;
    source.set_instanceid("source-logical");
    source.set_requestid("source-request");
    source.set_runtimeid("source-runtime");
    source.set_runtimeaddress("10.0.0.8:8000");
    source.set_functionagentid("source-agent");
    source.set_functionproxyid("source-proxy");
    source.set_function("rrt-function");
    source.set_restartpolicy("Never");
    (*source.mutable_resources()->mutable_resources())["CPU"].mutable_scalar()->set_value(2000);
    (*source.mutable_resources()->mutable_resources())["Memory"].mutable_scalar()->set_value(4096);
    (*source.mutable_createoptions())["workdir"] = "/workspace";
    source.add_labels("workload");
    source.set_starttime("source-start-time");
    source.mutable_instancestatus()->set_code(3);
    source.set_parentid("source-parent");
    source.set_parentfunctionproxyaid("source-parent-proxy");
    source.set_storagetype("local");
    source.add_args()->set_value("arg");
    source.set_version(17);
    source.set_tenantid("tenant-a");
    source.set_unitid("source-unit");
    source.set_containerid("source-container");
    source.set_proxygrpcaddress("10.0.0.8:9000");
    source.set_failover(true);
    (*source.mutable_extensions())["portForward"] = "source-host-port";
    (*source.mutable_extensions())["yr.internal.resume.target_attempt_id"] = "source-attempt";
    (*source.mutable_extensions())["future.physical.or.secret.key"] = "must-not-be-cloned";
    return source;
}

::messages::BeginReusableSnapshotRequest BeginRequest()
{
    ::messages::BeginReusableSnapshotRequest request;
    request.set_requestid("create-snapshot-request");
    request.set_tenantid("tenant-a");
    request.set_sourceinstanceid("source-logical");
    request.add_names("python-ready");
    return request;
}

::messages::CommitReusableSnapshotRequest CommitRequest(const std::string &snapshotID)
{
    ::messages::CommitReusableSnapshotRequest request;
    request.set_requestid("create-snapshot-request");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(snapshotID);
    *request.mutable_sourceinstanceinfo() = SourceInstance();
    *request.mutable_artifact() = ReadyArtifact();
    return request;
}

class ReusableSnapshotStoreTest : public testing::Test {
protected:
    void SetUp() override
    {
        persistence = std::make_shared<MemoryReusableSnapshotPersistence>();
        store = std::make_shared<ReusableSnapshotStore>(persistence, [] { return 123456; });
    }

    ::messages::BeginReusableSnapshotResponse Begin()
    {
        return store->Begin(BeginRequest()).Get();
    }

    ::messages::CommitReusableSnapshotResponse Commit(const std::string &snapshotID)
    {
        return store->Commit(CommitRequest(snapshotID)).Get();
    }

    std::shared_ptr<MemoryReusableSnapshotPersistence> persistence;
    std::shared_ptr<ReusableSnapshotStore> store;

    std::string CommitReadyLocal()
    {
        const auto begin = Begin();
        auto commit = CommitRequest(begin.snapshotid());
        *commit.mutable_artifact() = LocalArtifact();
        EXPECT_EQ(store->Commit(commit).Get().code(), 0);
        return begin.snapshotid();
    }
};

TEST_F(ReusableSnapshotStoreTest, UsesExactlyOneTenantScopedEtcdKey)
{
    const auto response = Begin();
    ASSERT_EQ(response.code(), 0);
    ASSERT_EQ(persistence->values.size(), 1U);
    EXPECT_EQ(persistence->values.begin()->first,
              ReusableSnapshotStore::RecordKey("tenant-a", response.snapshotid()));
    EXPECT_EQ(persistence->values.begin()->first.rfind("/yr/snapshots/v1/", 0), 0U);
}

TEST_F(ReusableSnapshotStoreTest, BeginCreatesPublishingVersionOne)
{
    const auto response = Begin();
    ASSERT_EQ(response.code(), 0);
    EXPECT_EQ(response.phase(), ::messages::REUSABLE_SNAPSHOT_PUBLISHING);
    const auto record = store->ReadForTest("tenant-a", response.snapshotid()).Get();
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->version(), 1U);
    EXPECT_EQ(record->createtime(), 123456);
    EXPECT_EQ(record->updatetime(), 123456);
    EXPECT_EQ(record->sourceinstanceid(), "source-logical");
}

TEST_F(ReusableSnapshotStoreTest, BeginDeterministicallyReplaysSameRequest)
{
    const auto first = Begin();
    ASSERT_EQ(first.code(), 0);
    const auto casCount = persistence->casKeys.size();
    const auto replay = Begin();
    EXPECT_EQ(replay.code(), 0);
    EXPECT_EQ(replay.snapshotid(), first.snapshotid());
    EXPECT_EQ(persistence->casKeys.size(), casCount);
}

TEST_F(ReusableSnapshotStoreTest, BeginReportsCasLoserWithoutOverwritingWinner)
{
    persistence->forceCasMiss = true;
    EXPECT_NE(Begin().code(), 0);
    EXPECT_TRUE(persistence->values.empty());
}

TEST_F(ReusableSnapshotStoreTest, CommitPublishesReadyArtifactWithVersionCas)
{
    const auto begin = Begin();
    const auto commit = Commit(begin.snapshotid());
    ASSERT_EQ(commit.code(), 0);
    EXPECT_EQ(commit.version(), 2U);
    const auto record = store->ReadForTest("tenant-a", begin.snapshotid()).Get();
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->phase(), ::messages::REUSABLE_SNAPSHOT_READY);
    EXPECT_EQ(record->artifact().objectkey(), ReadyArtifact().objectkey());
}

TEST_F(ReusableSnapshotStoreTest, CommitRejectsIncompleteArtifact)
{
    const auto begin = Begin();
    auto request = CommitRequest(begin.snapshotid());
    request.mutable_artifact()->clear_sha256();
    EXPECT_NE(store->Commit(request).Get().code(), 0);
}

TEST_F(ReusableSnapshotStoreTest, CommitRejectsUntrustedArtifactLocationAndDigest)
{
    const auto begin = Begin();
    auto request = CommitRequest(begin.snapshotid());
    request.mutable_artifact()->set_objectkey("../source-node/checkpoint.img");
    request.mutable_artifact()->set_sha256(std::string(64, 'z'));
    EXPECT_NE(store->Commit(request).Get().code(), 0);
}

TEST_F(ReusableSnapshotStoreTest, CommitReplayReturnsExistingReadyRecord)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    const auto casCount = persistence->casKeys.size();
    EXPECT_EQ(Commit(begin.snapshotid()).code(), 0);
    EXPECT_EQ(persistence->casKeys.size(), casCount);
}

TEST_F(ReusableSnapshotStoreTest, SanitizedTemplateDropsLogicalAndPhysicalIdentity)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    const auto record = store->ReadForTest("tenant-a", begin.snapshotid()).Get().value();
    const auto &value = record.instancetemplate();
    EXPECT_TRUE(value.instanceid().empty());
    EXPECT_TRUE(value.requestid().empty());
    EXPECT_TRUE(value.runtimeid().empty());
    EXPECT_TRUE(value.runtimeaddress().empty());
    EXPECT_TRUE(value.functionagentid().empty());
    EXPECT_TRUE(value.functionproxyid().empty());
    EXPECT_TRUE(value.containerid().empty());
    EXPECT_TRUE(value.unitid().empty());
    EXPECT_TRUE(value.proxygrpcaddress().empty());
}

TEST_F(ReusableSnapshotStoreTest, SanitizedTemplateDropsStateTimingAndSourcePortFacts)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    const auto value = store->ReadForTest("tenant-a", begin.snapshotid()).Get()->instancetemplate();
    EXPECT_TRUE(value.starttime().empty());
    EXPECT_EQ(value.version(), 0);
    EXPECT_FALSE(value.has_snapshotinfo());
    EXPECT_EQ(value.extensions().count("portForward"), 0U);
    EXPECT_EQ(value.extensions().count("yr.internal.resume.target_attempt_id"), 0U);
}

TEST_F(ReusableSnapshotStoreTest, SanitizedTemplatePreservesWorkloadAndDeclaredResources)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    const auto value = store->ReadForTest("tenant-a", begin.snapshotid()).Get()->instancetemplate();
    EXPECT_EQ(value.function(), "rrt-function");
    EXPECT_EQ(value.resources().resources().at("Memory").scalar().value(), 4096);
    EXPECT_EQ(value.createoptions().at("workdir"), "/workspace");
    EXPECT_TRUE(value.failover());
    EXPECT_TRUE(value.extensions().empty());
    EXPECT_EQ(value.scheduleoption().affinity().nodeaffinity().affinity().count("source-proxy"), 0U);
}

TEST_F(ReusableSnapshotStoreTest, FailDeletesOnlyMatchingPublishingRecord)
{
    const auto begin = Begin();
    ::messages::FailReusableSnapshotRequest request;
    request.set_requestid("create-snapshot-request");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());
    ASSERT_EQ(store->Fail(request).Get().code(), 0);
    EXPECT_TRUE(persistence->values.empty());
}

TEST_F(ReusableSnapshotStoreTest, FailNeverDeletesReadyRecord)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::FailReusableSnapshotRequest request;
    request.set_requestid("create-snapshot-request");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());
    EXPECT_NE(store->Fail(request).Get().code(), 0);
    EXPECT_EQ(persistence->values.size(), 1U);
}

TEST_F(ReusableSnapshotStoreTest, GetReturnsOnlyPublicReadySnapshotInfo)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::GetReusableSnapshotRequest request;
    request.set_requestid("get");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());
    const auto response = store->Get(request).Get();
    ASSERT_EQ(response.code(), 0);
    EXPECT_EQ(response.snapshotinfo().snapshotid(), begin.snapshotid());
    ASSERT_EQ(response.snapshotinfo().names_size(), 1);
    EXPECT_EQ(response.snapshotinfo().names(0), "python-ready");
}

TEST_F(ReusableSnapshotStoreTest, GetHidesPublishingRecord)
{
    const auto begin = Begin();
    ::messages::GetReusableSnapshotRequest request;
    request.set_requestid("get");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());
    EXPECT_NE(store->Get(request).Get().code(), 0);
}

TEST_F(ReusableSnapshotStoreTest, ListReturnsOnlyReadyRecordsForTenant)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::ListReusableSnapshotsRequest request;
    request.set_requestid("list");
    request.set_tenantid("tenant-a");
    const auto response = store->List(request).Get();
    ASSERT_EQ(response.code(), 0);
    ASSERT_EQ(response.snapshotinfos_size(), 1);
    EXPECT_EQ(response.snapshotinfos(0).snapshotid(), begin.snapshotid());
    ASSERT_EQ(persistence->listPrefixes.size(), 1U);
    EXPECT_EQ(persistence->listPrefixes[0], ReusableSnapshotStore::TenantPrefix("tenant-a"));
}

TEST_F(ReusableSnapshotStoreTest, ListSupportsNameFilter)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::ListReusableSnapshotsRequest request;
    request.set_requestid("list");
    request.set_tenantid("tenant-a");
    request.set_name("other");
    EXPECT_EQ(store->List(request).Get().snapshotinfos_size(), 0);
}

TEST_F(ReusableSnapshotStoreTest, ListUsesStablePageTokenAndPageSize)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    auto second = BeginRequest();
    second.set_requestid("create-snapshot-request-2");
    const auto begin2 = store->Begin(second).Get();
    auto commit2 = CommitRequest(begin2.snapshotid());
    commit2.set_requestid(second.requestid());
    ASSERT_EQ(store->Commit(commit2).Get().code(), 0);
    ::messages::ListReusableSnapshotsRequest request;
    request.set_requestid("list");
    request.set_tenantid("tenant-a");
    request.set_pagesize(1);
    const auto firstPage = store->List(request).Get();
    ASSERT_EQ(firstPage.snapshotinfos_size(), 1);
    ASSERT_FALSE(firstPage.nextpagetoken().empty());
    request.set_pagetoken(firstPage.nextpagetoken());
    EXPECT_EQ(store->List(request).Get().snapshotinfos_size(), 1);
}

TEST_F(ReusableSnapshotStoreTest, ResolveInjectsFrozenTemplateArtifactAndVersion)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::ResolveReusableSnapshotForCreateRequest request;
    request.set_requestid("clone-create");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());
    const auto response = store->Resolve(request).Get();
    ASSERT_EQ(response.code(), 0);
    EXPECT_EQ(response.instancetemplate().function(), "rrt-function");
    EXPECT_EQ(response.reusablesnapshotrestore().snapshotid(), begin.snapshotid());
    EXPECT_TRUE(response.reusablesnapshotrestore().allowlogicalinstanceidrebind());
    EXPECT_EQ(response.snapshotversion(), 2U);
}

TEST_F(ReusableSnapshotStoreTest, ResolveRejectsDeletingRecord)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::BeginDeleteReusableSnapshotRequest deleting;
    deleting.set_requestid("delete");
    deleting.set_tenantid("tenant-a");
    deleting.set_snapshotid(begin.snapshotid());
    ASSERT_EQ(store->BeginDelete(deleting).Get().code(), 0);
    ::messages::ResolveReusableSnapshotForCreateRequest request;
    request.set_requestid("clone-create");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());
    EXPECT_NE(store->Resolve(request).Get().code(), 0);
}

TEST_F(ReusableSnapshotStoreTest, BeginDeleteTransitionsReadyToDeletingByCas)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::BeginDeleteReusableSnapshotRequest request;
    request.set_requestid("delete");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());
    const auto response = store->BeginDelete(request).Get();
    ASSERT_EQ(response.code(), 0);
    EXPECT_EQ(response.expectedversion(), 3U);
    EXPECT_EQ(response.artifact().objectkey(), ReadyArtifact().objectkey());
    EXPECT_FALSE(response.alreadydeleted());
}

TEST_F(ReusableSnapshotStoreTest, BeginDeleteIsIdempotentWhenRecordIsAbsent)
{
    ::messages::BeginDeleteReusableSnapshotRequest request;
    request.set_requestid("delete");
    request.set_tenantid("tenant-a");
    request.set_snapshotid("missing");
    const auto response = store->BeginDelete(request).Get();
    EXPECT_EQ(response.code(), 0);
    EXPECT_TRUE(response.alreadydeleted());
}

TEST_F(ReusableSnapshotStoreTest, BeginDeleteCasLoserJoinsDeletingWinner)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    persistence->forceCasMiss = true;
    persistence->installReplacementOnCasMiss = true;
    ::messages::BeginDeleteReusableSnapshotRequest request;
    request.set_requestid("delete-loser");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());

    const auto response = store->BeginDelete(request).Get();

    ASSERT_EQ(response.code(), 0);
    EXPECT_FALSE(response.alreadydeleted());
    EXPECT_EQ(response.expectedversion(), 3U);
    EXPECT_EQ(response.artifact().objectkey(), ReadyArtifact().objectkey());
}

TEST_F(ReusableSnapshotStoreTest, BeginDeleteCasLoserAcceptsAlreadyCompletedWinner)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    persistence->forceCasMiss = true;
    persistence->eraseRecordOnCasMiss = true;
    ::messages::BeginDeleteReusableSnapshotRequest request;
    request.set_requestid("delete-loser");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());

    const auto response = store->BeginDelete(request).Get();

    ASSERT_EQ(response.code(), 0);
    EXPECT_TRUE(response.alreadydeleted());
}

TEST_F(ReusableSnapshotStoreTest, CompleteDeleteCasRemovesOnlyExpectedDeletingVersion)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::BeginDeleteReusableSnapshotRequest beginDelete;
    beginDelete.set_requestid("delete");
    beginDelete.set_tenantid("tenant-a");
    beginDelete.set_snapshotid(begin.snapshotid());
    const auto pending = store->BeginDelete(beginDelete).Get();
    ::messages::CompleteDeleteReusableSnapshotRequest complete;
    complete.set_requestid("delete");
    complete.set_tenantid("tenant-a");
    complete.set_snapshotid(begin.snapshotid());
    complete.set_expectedversion(pending.expectedversion());
    EXPECT_EQ(store->CompleteDelete(complete).Get().code(), 0);
    EXPECT_TRUE(persistence->values.empty());
}

TEST_F(ReusableSnapshotStoreTest, CompleteDeleteCasLoserAcceptsAlreadyCompletedWinner)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::BeginDeleteReusableSnapshotRequest beginDelete;
    beginDelete.set_requestid("delete");
    beginDelete.set_tenantid("tenant-a");
    beginDelete.set_snapshotid(begin.snapshotid());
    const auto pending = store->BeginDelete(beginDelete).Get();
    ASSERT_EQ(pending.code(), 0);
    persistence->forceCasMiss = true;
    persistence->eraseRecordOnCasMiss = true;
    ::messages::CompleteDeleteReusableSnapshotRequest complete;
    complete.set_requestid("delete-loser");
    complete.set_tenantid("tenant-a");
    complete.set_snapshotid(begin.snapshotid());
    complete.set_expectedversion(pending.expectedversion());

    const auto response = store->CompleteDelete(complete).Get();

    EXPECT_EQ(response.code(), 0);
    EXPECT_TRUE(persistence->values.empty());
}

TEST_F(ReusableSnapshotStoreTest, DeleteRequiresRealPhysicalArtifactDeleter)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::DeleteReusableSnapshotRequest request;
    request.set_requestid("delete");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());
    EXPECT_NE(store->Delete(request).Get().code(), 0);
    EXPECT_EQ(persistence->values.size(), 1U);
    const auto record = store->ReadForTest("tenant-a", begin.snapshotid()).Get();
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->phase(), ::messages::REUSABLE_SNAPSHOT_READY);
}

TEST_F(ReusableSnapshotStoreTest, DeleteMissingRecordIsIdempotentWithoutPhysicalDeleter)
{
    ::messages::DeleteReusableSnapshotRequest request;
    request.set_requestid("delete");
    request.set_tenantid("tenant-a");
    request.set_snapshotid("missing");
    EXPECT_EQ(store->Delete(request).Get().code(), 0);
    EXPECT_TRUE(persistence->values.empty());
}

TEST_F(ReusableSnapshotStoreTest, DeleteUsesPhysicalSeamThenCompletesExactRecord)
{
    const auto begin = Begin();
    ASSERT_EQ(Commit(begin.snapshotid()).code(), 0);
    ::messages::DeleteReusableSnapshotArtifactRequest observed;
    store->SetArtifactDeleter([&observed](const ::messages::DeleteReusableSnapshotArtifactRequest &request) {
        observed = request;
        ::messages::DeleteReusableSnapshotArtifactResponse response;
        response.set_requestid(request.requestid());
        return litebus::Future<::messages::DeleteReusableSnapshotArtifactResponse>(response);
    });
    ::messages::DeleteReusableSnapshotRequest request;
    request.set_requestid("delete");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(begin.snapshotid());
    EXPECT_EQ(store->Delete(request).Get().code(), 0);
    EXPECT_EQ(observed.snapshotid(), begin.snapshotid());
    EXPECT_EQ(observed.artifact().objectkey(), ReadyArtifact().objectkey());
    EXPECT_TRUE(persistence->values.empty());
}

namespace {

::messages::DeleteReusableSnapshotArtifactResponse OkArtifactDelete(
    const ::messages::DeleteReusableSnapshotArtifactRequest &request)
{
    ::messages::DeleteReusableSnapshotArtifactResponse response;
    response.set_requestid(request.requestid());
    return response;
}

std::shared_ptr<std::vector<std::string>> InstallRecordingDeleter(
    const std::shared_ptr<ReusableSnapshotStore> &target)
{
    // The recorder must own its storage for as long as the store may invoke
    // the deleter; a shared_ptr captured by value cannot dangle.
    auto deleted = std::make_shared<std::vector<std::string>>();
    target->SetArtifactDeleter([deleted](const ::messages::DeleteReusableSnapshotArtifactRequest &request) {
        deleted->emplace_back(request.snapshotid());
        return litebus::Future<::messages::DeleteReusableSnapshotArtifactResponse>(
            OkArtifactDelete(request));
    });
    return deleted;
}

}  // namespace

// Source-exit retention contract: a named reusable Snapshot owns its own
// lifecycle. DeleteLocalSnapshotsForSource must never issue a delete — the
// pc-snapshot-term-1 physical case proved the old cascade destroyed the only
// READY restore source 17s before the restore create resolved it.

TEST_F(ReusableSnapshotStoreTest, SourceExitKeepsReadyLocalSnapshotResolvable)
{
    const auto snapshotID = CommitReadyLocal();
    auto deleted = InstallRecordingDeleter(store);

    EXPECT_TRUE(store->DeleteLocalSnapshotsForSource("tenant-a", "source-logical").Get().IsOk());

    EXPECT_TRUE(deleted->empty());
    const auto record = store->ReadForTest("tenant-a", snapshotID).Get();
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->phase(), ::messages::REUSABLE_SNAPSHOT_READY);
    ::messages::ResolveReusableSnapshotForCreateRequest resolve;
    resolve.set_requestid("resolve-after-exit");
    resolve.set_tenantid("tenant-a");
    resolve.set_snapshotid(snapshotID);
    const auto resolved = store->Resolve(resolve).Get();
    EXPECT_EQ(resolved.code(), 0);
    EXPECT_EQ(resolved.reusablesnapshotrestore().snapshotid(), snapshotID);
    ::messages::GetReusableSnapshotRequest get;
    get.set_requestid("get-after-exit");
    get.set_tenantid("tenant-a");
    get.set_snapshotid(snapshotID);
    const auto gotten = store->Get(get).Get();
    EXPECT_EQ(gotten.code(), 0);
    EXPECT_EQ(gotten.snapshotinfo().snapshotid(), snapshotID);
}

TEST_F(ReusableSnapshotStoreTest, ExplicitDeleteRemovesReadyLocalSnapshotAfterSourceExit)
{
    const auto snapshotID = CommitReadyLocal();
    auto deleted = InstallRecordingDeleter(store);

    EXPECT_TRUE(store->DeleteLocalSnapshotsForSource("tenant-a", "source-logical").Get().IsOk());
    ::messages::DeleteReusableSnapshotRequest request;
    request.set_requestid("explicit-delete");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(snapshotID);
    EXPECT_EQ(store->Delete(request).Get().code(), 0);

    ASSERT_EQ(deleted->size(), 1U);
    EXPECT_EQ(deleted->front(), snapshotID);
    EXPECT_FALSE(store->ReadForTest("tenant-a", snapshotID).Get().has_value());
}

TEST_F(ReusableSnapshotStoreTest, SourceExitKeepsPublishingRecordForLaterCommit)
{
    const auto begin = Begin();
    auto deleted = InstallRecordingDeleter(store);

    EXPECT_TRUE(store->DeleteLocalSnapshotsForSource("tenant-a", "source-logical").Get().IsOk());

    EXPECT_TRUE(deleted->empty());
    auto record = store->ReadForTest("tenant-a", begin.snapshotid()).Get();
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->phase(), ::messages::REUSABLE_SNAPSHOT_PUBLISHING);

    auto commit = CommitRequest(begin.snapshotid());
    *commit.mutable_artifact() = LocalArtifact();
    EXPECT_EQ(store->Commit(commit).Get().code(), 0);
    record = store->ReadForTest("tenant-a", begin.snapshotid()).Get();
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->phase(), ::messages::REUSABLE_SNAPSHOT_READY);
}

TEST_F(ReusableSnapshotStoreTest, SourceExitLeavesDeletingRecordToExplicitRetry)
{
    const auto snapshotID = CommitReadyLocal();
    ::messages::BeginDeleteReusableSnapshotRequest beginDelete;
    beginDelete.set_requestid("authorized-delete");
    beginDelete.set_tenantid("tenant-a");
    beginDelete.set_snapshotid(snapshotID);
    ASSERT_EQ(store->BeginDelete(beginDelete).Get().code(), 0);

    auto deleted = InstallRecordingDeleter(store);
    EXPECT_TRUE(store->DeleteLocalSnapshotsForSource("tenant-a", "source-logical").Get().IsOk());
    EXPECT_TRUE(deleted->empty());
    auto record = store->ReadForTest("tenant-a", snapshotID).Get();
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->phase(), ::messages::REUSABLE_SNAPSHOT_DELETING);

    // The authorized attempt completes via explicit retry, not via the
    // source-exit scan (whose re-read cannot be identity-pinned to it).
    ::messages::DeleteReusableSnapshotRequest retry;
    retry.set_requestid("explicit-retry");
    retry.set_tenantid("tenant-a");
    retry.set_snapshotid(snapshotID);
    EXPECT_EQ(store->Delete(retry).Get().code(), 0);
    ASSERT_EQ(deleted->size(), 1U);
    EXPECT_FALSE(store->ReadForTest("tenant-a", snapshotID).Get().has_value());
}

TEST_F(ReusableSnapshotStoreTest, SourceExitIssuesNoWritesWhenCommitMayStillLand)
{
    // The stale-race shape from pc-snapshot-term-1: the scan observes a
    // PUBLISHING record, a legal Commit lands afterwards. With zero writes
    // from the scan the concurrently-committed READY must survive.
    const auto begin = Begin();
    InstallRecordingDeleter(store);
    const auto casWritesBefore = persistence->casKeys.size();

    EXPECT_TRUE(store->DeleteLocalSnapshotsForSource("tenant-a", "source-logical").Get().IsOk());
    EXPECT_EQ(persistence->casKeys.size(), casWritesBefore);

    auto commit = CommitRequest(begin.snapshotid());
    *commit.mutable_artifact() = LocalArtifact();
    EXPECT_EQ(store->Commit(commit).Get().code(), 0);
    const auto record = store->ReadForTest("tenant-a", begin.snapshotid()).Get();
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->phase(), ::messages::REUSABLE_SNAPSHOT_READY);
}

TEST_F(ReusableSnapshotStoreTest, SourceExitRetainsLocalDistributedAndForeignSourceRecords)
{
    const auto localID = CommitReadyLocal();

    auto distributedBeginRequest = BeginRequest();
    distributedBeginRequest.set_requestid("distributed-snapshot-request");
    const auto distributedBegin = store->Begin(distributedBeginRequest).Get();
    auto distributedCommit = CommitRequest(distributedBegin.snapshotid());
    distributedCommit.set_requestid(distributedBeginRequest.requestid());
    ASSERT_EQ(store->Commit(distributedCommit).Get().code(), 0);

    auto foreignBeginRequest = BeginRequest();
    foreignBeginRequest.set_requestid("foreign-source-request");
    foreignBeginRequest.set_sourceinstanceid("another-source");
    const auto foreignBegin = store->Begin(foreignBeginRequest).Get();
    auto foreignCommit = CommitRequest(foreignBegin.snapshotid());
    foreignCommit.set_requestid(foreignBeginRequest.requestid());
    auto foreignSource = SourceInstance();
    foreignSource.set_instanceid("another-source");
    *foreignCommit.mutable_sourceinstanceinfo() = foreignSource;
    *foreignCommit.mutable_artifact() = LocalArtifact();
    ASSERT_EQ(store->Commit(foreignCommit).Get().code(), 0);

    auto deleted = InstallRecordingDeleter(store);
    EXPECT_TRUE(store->DeleteLocalSnapshotsForSource("tenant-a", "source-logical").Get().IsOk());

    EXPECT_TRUE(deleted->empty());
    EXPECT_TRUE(store->ReadForTest("tenant-a", localID).Get().has_value());
    EXPECT_TRUE(store->ReadForTest("tenant-a", distributedBegin.snapshotid()).Get().has_value());
    EXPECT_TRUE(store->ReadForTest("tenant-a", foreignBegin.snapshotid()).Get().has_value());
    EXPECT_EQ(persistence->listPrefixes.size(), 1U);
    EXPECT_EQ(persistence->listPrefixes.front(), ReusableSnapshotStore::TenantPrefix("tenant-a"));
}

TEST_F(ReusableSnapshotStoreTest, FailedPhysicalDeleteKeepsRecordDeletingAndRetrySucceeds)
{
    const auto snapshotID = CommitReadyLocal();
    store->SetArtifactDeleter([](const ::messages::DeleteReusableSnapshotArtifactRequest &request) {
        ::messages::DeleteReusableSnapshotArtifactResponse response;
        response.set_requestid(request.requestid());
        response.set_code(common::ERR_INNER_COMMUNICATION);
        response.set_message("physical delete failed");
        return litebus::Future<::messages::DeleteReusableSnapshotArtifactResponse>(response);
    });
    ::messages::DeleteReusableSnapshotRequest request;
    request.set_requestid("failing-delete");
    request.set_tenantid("tenant-a");
    request.set_snapshotid(snapshotID);
    EXPECT_NE(store->Delete(request).Get().code(), 0);
    auto record = store->ReadForTest("tenant-a", snapshotID).Get();
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->phase(), ::messages::REUSABLE_SNAPSHOT_DELETING);

    auto deleted = InstallRecordingDeleter(store);
    ::messages::DeleteReusableSnapshotRequest retry;
    retry.set_requestid("retry-delete");
    retry.set_tenantid("tenant-a");
    retry.set_snapshotid(snapshotID);
    EXPECT_EQ(store->Delete(retry).Get().code(), 0);
    ASSERT_EQ(deleted->size(), 1U);
    EXPECT_FALSE(store->ReadForTest("tenant-a", snapshotID).Get().has_value());
}

}  // namespace
}  // namespace functionsystem::snap_manager
