/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */

#include <filesystem>
#include <fstream>
#include <cstdlib>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "common/proto/pb/message_pb.h"
#include "function_agent/snapshot/local_snapshot_store.h"

namespace functionsystem::function_agent {
namespace {
namespace fs = std::filesystem;

TEST(LocalSnapshotProtoTest, UsesStableInternalFieldNumbers)
{
    const auto *candidate = messages::SnapshotRuntimeRequest::descriptor()
        ->FindFieldByName("localRecoveryCandidate");
    ASSERT_NE(candidate, nullptr);
    EXPECT_EQ(candidate->number(), 13);
    const auto *localSnapshot = messages::SnapshotRuntimeResponse::descriptor()
        ->FindFieldByName("localSnapshot");
    ASSERT_NE(localSnapshot, nullptr);
    EXPECT_EQ(localSnapshot->number(), 10);
}

class LocalSnapshotStoreTest : public testing::Test {
protected:
    void SetUp() override
    {
        auto pattern = (fs::temp_directory_path() / "local-snapshot-store-test-XXXXXX").string();
        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');
        root_ = mkdtemp(buffer.data());
        store_ = std::make_unique<LocalSnapshotStore>(root_);
    }

    void TearDown() override
    {
        store_.reset();
        std::error_code error;
        fs::remove_all(root_, error);
    }

    static LocalSnapshotCommitRequest Request(const std::string &snapshotID)
    {
        LocalSnapshotCommitRequest request;
        request.snapshotID = snapshotID;
        request.recoveryCandidate = true;
        request.instanceID = "instance-a";
        request.tenantHash = "tenant-hash";
        request.sourceRuntimeID = "runtime-a";
        request.sourceSandboxID = "sandbox-a";
        request.sourceInstanceVersion = 12;
        request.createdAtUnixSeconds = 1787670000;
        return request;
    }

    static void Write(const fs::path &path, const std::string &contents)
    {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output.is_open());
        output << contents;
        output.close();
        ASSERT_TRUE(output.good());
    }

    LocalSnapshotDescriptor Commit(const std::string &snapshotID, const std::string &contents)
    {
        const auto request = Request(snapshotID);
        const auto prepared = store_->Prepare(request);
        EXPECT_TRUE(prepared.status.IsOk()) << prepared.status.ToString();
        Write(prepared.directory / "checkpoint.img", contents);
        const auto committed = store_->Commit(request);
        EXPECT_TRUE(committed.status.IsOk()) << committed.status.ToString();
        return committed.descriptor;
    }

    fs::path root_;
    std::unique_ptr<LocalSnapshotStore> store_;
};

TEST_F(LocalSnapshotStoreTest, CommitsOpaqueCheckpointWithoutSidecarManifest)
{
    const auto descriptor = Commit("checkpoint-a", "payload");
    EXPECT_TRUE(descriptor.recoveryCandidate);
    EXPECT_EQ(descriptor.size, 7U);
    EXPECT_EQ(store_->List().size(), 1U);
    EXPECT_TRUE(fs::is_regular_file(root_ / "checkpoint-a" / "checkpoint.img"));
    EXPECT_FALSE(fs::exists(root_ / "checkpoint-a" / "snapshot.meta"));
}

TEST_F(LocalSnapshotStoreTest, CommitsAndDeletesOpaqueMultiFileDirectory)
{
    const auto request = Request("runsc-checkpoint");
    const auto prepared = store_->Prepare(request);
    ASSERT_TRUE(prepared.status.IsOk()) << prepared.status.ToString();
    EXPECT_EQ(prepared.directory.filename(), "runsc-checkpoint");
    EXPECT_TRUE(fs::is_directory(root_ / "runsc-checkpoint"));

    Write(prepared.directory / "checkpoint.img", "state");
    Write(prepared.directory / "pages_meta.img", "metadata");
    Write(prepared.directory / "pages.img", "pages");
    fs::create_directories(prepared.directory / "nested");
    Write(prepared.directory / "nested" / "runtime.img", "nested-state");

    const auto committed = store_->Commit(request);
    ASSERT_TRUE(committed.status.IsOk()) << committed.status.ToString();
    EXPECT_EQ(committed.descriptor.size, 5U + 8U + 5U + 12U);
    EXPECT_TRUE(fs::is_regular_file(root_ / "runsc-checkpoint" / "pages.img"));
    EXPECT_TRUE(fs::is_regular_file(root_ / "runsc-checkpoint" / "nested" / "runtime.img"));

    ASSERT_TRUE(store_->Delete({"runsc-checkpoint"}).IsOk());
    EXPECT_FALSE(fs::exists(root_ / "runsc-checkpoint"));
}

TEST_F(LocalSnapshotStoreTest, RejectsUnknownNonRegularEntryAnywhereInDirectory)
{
    const auto request = Request("checkpoint-link");
    const auto prepared = store_->Prepare(request);
    ASSERT_TRUE(prepared.status.IsOk());
    Write(prepared.directory / "checkpoint.img", "payload");
    Write(root_ / "outside.img", "outside");
    fs::create_directories(prepared.directory / "nested");
    fs::create_symlink(root_ / "outside.img", prepared.directory / "nested" / "pages.img");

    EXPECT_EQ(store_->Commit(request).status.StatusCode(), StatusCode::ERR_PARAM_INVALID);
    ASSERT_TRUE(store_->DiscardPrepared(request.snapshotID).IsOk());
    EXPECT_FALSE(fs::exists(prepared.directory));
    EXPECT_TRUE(fs::is_regular_file(root_ / "outside.img"));
}

TEST_F(LocalSnapshotStoreTest, ExistingRegularCheckpointIsAnIdempotentReplay)
{
    Commit("checkpoint-a", "payload");
    const auto replay = store_->Prepare(Request("checkpoint-a"));
    ASSERT_TRUE(replay.status.IsOk()) << replay.status.ToString();
    EXPECT_TRUE(replay.replayed);
}

TEST_F(LocalSnapshotStoreTest, RejectsSymlinkedCheckpoint)
{
    const auto request = Request("checkpoint-link");
    const auto prepared = store_->Prepare(request);
    ASSERT_TRUE(prepared.status.IsOk());
    Write(root_ / "outside.img", "payload");
    fs::create_symlink(root_ / "outside.img", prepared.directory / "checkpoint.img");
    EXPECT_TRUE(store_->Commit(request).status.IsError());
}

TEST_F(LocalSnapshotStoreTest, DeletesByExactSnapshotIDAndIsIdempotent)
{
    Commit("checkpoint-a", "payload");
    EXPECT_TRUE(store_->Delete({"checkpoint-a"}).IsOk());
    EXPECT_FALSE(fs::exists(root_ / "checkpoint-a"));
    EXPECT_TRUE(store_->Delete({"checkpoint-a"}).IsOk());
}

TEST_F(LocalSnapshotStoreTest, ByteBudgetEvictsLeastRecentlyUsedArtifact)
{
    store_ = std::make_unique<LocalSnapshotStore>(root_, 8);
    Commit("checkpoint-a", "12345");
    Commit("checkpoint-b", "67890");
    EXPECT_FALSE(fs::exists(root_ / "checkpoint-a"));
    EXPECT_TRUE(fs::is_regular_file(root_ / "checkpoint-b" / "checkpoint.img"));
}

TEST_F(LocalSnapshotStoreTest, RestorePinProtectsArtifactUntilLastRelease)
{
    store_ = std::make_unique<LocalSnapshotStore>(root_, 8);
    Commit("checkpoint-a", "12345");
    ASSERT_TRUE(store_->PinForRestore("checkpoint-a").IsOk());
    Commit("checkpoint-b", "67890");

    EXPECT_TRUE(fs::is_regular_file(root_ / "checkpoint-a" / "checkpoint.img"));
    EXPECT_TRUE(fs::is_regular_file(root_ / "checkpoint-b" / "checkpoint.img"));

    ASSERT_TRUE(store_->UnpinAfterRestore("checkpoint-a", false).IsOk());
    EXPECT_FALSE(fs::exists(root_ / "checkpoint-a"));
    EXPECT_TRUE(fs::is_regular_file(root_ / "checkpoint-b" / "checkpoint.img"));
}

TEST_F(LocalSnapshotStoreTest, DistributedOnlyEvictionWaitsForAllRestorePins)
{
    Commit("checkpoint-a", "payload");
    ASSERT_TRUE(store_->PinForRestore("checkpoint-a").IsOk());
    ASSERT_TRUE(store_->PinForRestore("checkpoint-a").IsOk());
    ASSERT_TRUE(store_->EvictLocalArtifact("checkpoint-a").IsOk());
    EXPECT_TRUE(fs::is_regular_file(root_ / "checkpoint-a" / "checkpoint.img"));

    ASSERT_TRUE(store_->UnpinAfterRestore("checkpoint-a", true).IsOk());
    EXPECT_TRUE(fs::is_regular_file(root_ / "checkpoint-a" / "checkpoint.img"));
    ASSERT_TRUE(store_->UnpinAfterRestore("checkpoint-a", true).IsOk());
    EXPECT_FALSE(fs::exists(root_ / "checkpoint-a"));
    EXPECT_TRUE(store_->List().empty());
}

TEST_F(LocalSnapshotStoreTest, ExplicitDeleteDefersUntilRestorePinIsReleased)
{
    Commit("checkpoint-a", "payload");
    ASSERT_TRUE(store_->PinForRestore("checkpoint-a").IsOk());
    EXPECT_TRUE(store_->Delete({"checkpoint-a"}).IsOk());
    EXPECT_TRUE(fs::exists(root_ / "checkpoint-a"));
    ASSERT_TRUE(store_->UnpinAfterRestore("checkpoint-a", false).IsOk());
    EXPECT_FALSE(fs::exists(root_ / "checkpoint-a"));
}

TEST_F(LocalSnapshotStoreTest, InstanceCleanupDeletesOnlyOwnedRecoveryCandidates)
{
    Commit("checkpoint-a", "payload-a");

    auto other = Request("checkpoint-b");
    other.instanceID = "instance-b";
    auto prepared = store_->Prepare(other);
    ASSERT_TRUE(prepared.status.IsOk());
    Write(prepared.directory / "checkpoint.img", "payload-b");
    ASSERT_TRUE(store_->Commit(other).status.IsOk());

    auto reusable = Request("reusable");
    reusable.recoveryCandidate = false;
    prepared = store_->Prepare(reusable);
    ASSERT_TRUE(prepared.status.IsOk());
    Write(prepared.directory / "checkpoint.img", "reusable");
    ASSERT_TRUE(store_->Commit(reusable).status.IsOk());

    ASSERT_TRUE(store_->DeleteRecoveryCandidatesForInstance("instance-a").IsOk());
    EXPECT_FALSE(fs::exists(root_ / "checkpoint-a"));
    EXPECT_TRUE(fs::exists(root_ / "checkpoint-b"));
    EXPECT_TRUE(fs::exists(root_ / "reusable"));
}

TEST_F(LocalSnapshotStoreTest, LruDoesNotKeepPhantomRemoteRecord)
{
    store_ = std::make_unique<LocalSnapshotStore>(root_, 8);
    Commit("checkpoint-a", "12345");
    ASSERT_TRUE(store_->SetStorageLocation(
        "checkpoint-a", "datasystem", "snapshots/checkpoint-a").IsOk());
    Commit("checkpoint-b", "67890");

    ASSERT_EQ(store_->List().size(), 1U);
    EXPECT_EQ(store_->List().front().snapshotID, "checkpoint-b");
}

// ── daemon atomic-publish leaf contract (sandboxd checkpoint store) ─────────
// The daemon's PublishAt creates the final artifact directory ATOMICALLY from
// a staging sibling and rejects a pre-existing incomplete final directory
// with failed-precondition ("checkpoint directory is incomplete" — the
// pc-snapshot-67e5-1 physical failure). Its own real-storage Go tests
// (internal/checkpoint/store_test.go: PublishAt replay on complete dirs,
// rejection on pre-existing/incomplete and foreign-identity dirs) pin the
// daemon side; these tests pin the Agent side of that boundary.

TEST_F(LocalSnapshotStoreTest, PhysicalPrepareLeavesFinalLeafAbsent)
{
    // First physical checkpoint on a clean id: Prepare succeeds WITHOUT
    // creating the final directory — only the managed root is ensured.
    const auto prepared = store_->Prepare(
        Request("snap-phys-1"), LocalSnapshotStore::PrepareLeaf::LeaveAbsentForDaemon);
    ASSERT_TRUE(prepared.status.IsOk()) << prepared.status.ToString();
    EXPECT_FALSE(prepared.replayed);
    EXPECT_EQ(prepared.directory, root_ / "snap-phys-1");
    EXPECT_TRUE(fs::is_directory(root_));
    EXPECT_FALSE(fs::exists(root_ / "snap-phys-1"))
        << "a pre-created leaf is exactly the incomplete state the daemon must reject";
    // DiscardPrepared on a never-created leaf is an idempotent no-op
    EXPECT_TRUE(store_->DiscardPrepared("snap-phys-1").IsOk());
    EXPECT_FALSE(fs::exists(root_ / "snap-phys-1"));
}

TEST_F(LocalSnapshotStoreTest, PhysicalPrepareReplaysCompleteCommittedArtifact)
{
    // A complete, committed artifact replays identically in physical mode —
    // the daemon's InspectAt/MatchSource acceptance path.
    Commit("snap-phys-2", "payload");
    const auto replay = store_->Prepare(
        Request("snap-phys-2"), LocalSnapshotStore::PrepareLeaf::LeaveAbsentForDaemon);
    ASSERT_TRUE(replay.status.IsOk()) << replay.status.ToString();
    EXPECT_TRUE(replay.replayed);
}

TEST_F(LocalSnapshotStoreTest, PhysicalPrepareRejectsForeignOrIncompleteLeaf)
{
    // A leaf WITHOUT a committed record conflicts in BOTH modes: the
    // residue/foreign-identity guards stay exactly as before, and the leaf
    // is never removed to force progress.
    const auto request = Request("snap-phys-3");
    const auto prepared = store_->Prepare(request);  // materialization mode creates it
    ASSERT_TRUE(prepared.status.IsOk());
    Write(prepared.directory / "checkpoint.img", "partial");

    const auto physical = store_->Prepare(
        request, LocalSnapshotStore::PrepareLeaf::LeaveAbsentForDaemon);
    EXPECT_FALSE(physical.status.IsOk());
    EXPECT_FALSE(physical.replayed);
    const auto materialAgain = store_->Prepare(request);
    EXPECT_FALSE(materialAgain.status.IsOk()) << "materialization guard is unchanged";

    EXPECT_TRUE(fs::exists(prepared.directory))
        << "the foreign leaf stays in place for inspection";
}

TEST_F(LocalSnapshotStoreTest, MaterializationPrepareStillCreatesLeaf)
{
    // Remote materialization (distributed-storage download) genuinely writes
    // into the directory and keeps the original create-leaf behavior.
    const auto prepared = store_->Prepare(Request("snap-remote-1"));
    ASSERT_TRUE(prepared.status.IsOk()) << prepared.status.ToString();
    EXPECT_FALSE(prepared.replayed);
    EXPECT_TRUE(fs::is_directory(root_ / "snap-remote-1"));
}

}  // namespace
}  // namespace functionsystem::function_agent
