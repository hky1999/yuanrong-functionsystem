/*
 * Service-boundary ownership tests for the managed/anonymous checkpoint
 * sub-root split. A REAL spawned CkptFileManagerActor owns ONLY
 * <base>/anonymous; the agent's managed artifacts live in the sibling
 * <base>/managed and must be structurally unreachable by the TTL cleanup
 * and by the restart adoption scan. These tests fail if the product ever
 * points the anonymous manager at the shared base (both scan and cleanup
 * would then reach managed/), or makes the scan recursive.
 */
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "async/async.hpp"
#include "runtime_manager/ckpt/ckpt_file_manager_actor.h"

namespace functionsystem::test {

using functionsystem::runtime_manager::CkptFileManagerActor;

namespace {
void WriteFile(const std::string &path, const std::string &content)
{
    std::filesystem::create_directories(std::filesystem::path(path).parent_path());
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    f << content;
}
}  // namespace

class CkptOwnershipIsolationTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        base_ = "/tmp/ckpt_owner_" + litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        anonRoot_ = base_ + "/anonymous";
        managedRoot_ = base_ + "/managed";
        // the actor is constructed with the ANONYMOUS sub-root only
        actor_ = std::make_shared<CkptFileManagerActor>(
            "CkptOwner_" + litebus::uuid_generator::UUID::GetRandomUUID().ToString(), anonRoot_);
        actor_->cleanupEnabled_ = false;
        litebus::Spawn(actor_);
        litebus::Async(actor_->GetAID(), &CkptFileManagerActor::CleanupExpiredFiles).Get();
    }

    void TearDown() override
    {
        if (actor_) {
            litebus::Terminate(actor_->GetAID());
            litebus::Await(actor_->GetAID());
        }
        actor_ = nullptr;
        std::filesystem::remove_all(base_);
    }

    std::string base_;
    std::string anonRoot_;
    std::string managedRoot_;
    std::shared_ptr<CkptFileManagerActor> actor_{ nullptr };
};

// The production failure mode observed live on BM1 (1741Z TTL observation):
// registration starts at refCount=0 with an active TTL, source retirement's
// RemoveReference is a no-op ("reference count already 0"), and the TTL
// cleanup then DELETES the anonymous artifact. The same cleanup pass must
// never touch the managed sibling.
TEST_F(CkptOwnershipIsolationTest, TtlCleanupDeletesAnonymousOnlyNeverManagedSibling)
{
    const std::string anonID = "ckpt-anon-ttl";
    WriteFile(anonRoot_ + "/" + anonID + "/checkpoint.img", "anonymous payload");
    const std::string managedID = "snap-agent-owned";
    WriteFile(managedRoot_ + "/" + managedID + "/checkpoint.img", "managed payload");

    auto registered = litebus::Async(actor_->GetAID(), &CkptFileManagerActor::RegisterCheckpoint,
                                     anonID, anonRoot_ + "/" + anonID, anonID, 1).Get();
    EXPECT_EQ(registered.storageUrl, anonID + ".zip");

    // source retirement on an already-zero refcount: no-op, TTL keeps running
    EXPECT_TRUE(litebus::Async(actor_->GetAID(), &CkptFileManagerActor::RemoveReference, anonID).Get().IsOk());

    std::this_thread::sleep_for(std::chrono::milliseconds(1300));
    const auto deleted = litebus::Async(actor_->GetAID(), &CkptFileManagerActor::CleanupExpiredFiles).Get();
    EXPECT_GT(deleted, 0);

    EXPECT_FALSE(std::filesystem::exists(anonRoot_ + "/" + anonID))
        << "anonymous TTL artifact must be deleted (production semantics)";
    EXPECT_TRUE(std::filesystem::exists(managedRoot_ + "/" + managedID))
        << "managed artifact must survive the anonymous TTL cleanup untouched";
    // the managed artifact was never registered: it is not addressable and
    // therefore not owned by this manager
    EXPECT_FALSE(litebus::Async(actor_->GetAID(), &CkptFileManagerActor::AddReference, managedID).Get().IsOk());
    // base and the managed root itself were never candidates for deletion
    EXPECT_TRUE(std::filesystem::exists(anonRoot_));
    EXPECT_TRUE(std::filesystem::exists(managedRoot_));
}

// Restart adoption (RestoreCheckpointsFromLocal): an anonymous leftover
// directory is re-adopted with a default TTL; a managed sibling directory is
// NEVER adopted (the agent's store keeps sole ownership) and no pass ever
// deletes it or the parent directories.
TEST_F(CkptOwnershipIsolationTest, RestartScanAdoptsAnonymousOnlyNeverManagedSibling)
{
    const std::string anonID = "ckpt-anon-restart";
    const std::string managedID = "snap-agent-restart";
    WriteFile(anonRoot_ + "/" + anonID + "/artifact.bin", "anonymous leftover");
    WriteFile(managedRoot_ + "/" + managedID + "/checkpoint.img", "managed leftover");

    // fresh actor over the same roots simulates the proxy restart scan
    auto restarted = std::make_shared<CkptFileManagerActor>(
        "CkptOwnerR_" + litebus::uuid_generator::UUID::GetRandomUUID().ToString(), anonRoot_);
    restarted->cleanupEnabled_ = false;
    litebus::Spawn(restarted);
    litebus::Async(restarted->GetAID(), &CkptFileManagerActor::CleanupExpiredFiles).Get();

    EXPECT_TRUE(litebus::Async(restarted->GetAID(), &CkptFileManagerActor::AddReference, anonID).Get().IsOk())
        << "anonymous leftover must be re-adopted by the restart scan";
    EXPECT_FALSE(litebus::Async(restarted->GetAID(), &CkptFileManagerActor::AddReference, managedID).Get().IsOk())
        << "managed artifact must never enter the anonymous table";

    // aggressive cleanup right after adoption must still delete nothing:
    // fresh TTL, and the managed tree is out of reach
    const auto deleted = litebus::Async(restarted->GetAID(), &CkptFileManagerActor::CleanupExpiredFiles).Get();
    EXPECT_EQ(deleted, 0);
    EXPECT_TRUE(std::filesystem::exists(anonRoot_ + "/" + anonID));
    EXPECT_TRUE(std::filesystem::exists(managedRoot_ + "/" + managedID));
    EXPECT_TRUE(std::filesystem::exists(managedRoot_));

    litebus::Terminate(restarted->GetAID());
    litebus::Await(restarted->GetAID());
}

}  // namespace functionsystem::test
