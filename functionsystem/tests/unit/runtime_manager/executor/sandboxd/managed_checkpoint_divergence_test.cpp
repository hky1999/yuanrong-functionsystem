/*
 * Service-boundary divergence tests: managed vs anonymous checkpoint
 * completion in the REAL SandboxdCheckpointOrchestrator backed by a REAL
 * spawned CkptFileManagerActor on the anonymous/ sub-root.
 *
 * managed=true (caller supplied snapshotid+checkpointdir):
 *   - success after generation echo + birth-identity recheck, with the
 *     checkpoint-file manager NULL: the anonymous chain would abort on
 *     ASSERT_IF_NULL(ckptFileManager_) / null deref, so a clean return is
 *     structural proof that RegisterCheckpoint was never called;
 *   - no runtimeID->checkpointID mapping is recorded (no SetCheckpointID),
 *     so source retirement can never ReleaseRef the managed artifact;
 *   - the artifact is not addressable in the anonymous table.
 * managed=false (legacy anonymous): RegisterCheckpoint runs through the real
 *   actor exactly once, the mapping IS recorded, the artifact becomes
 *   addressable — original behavior regression.
 * Guards (generation echo, birth identity) reject managed requests too.
 */
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "async/async.hpp"
#include "runtime_manager/ckpt/ckpt_file_manager.h"
#include "runtime_manager/ckpt/ckpt_file_manager_actor.h"
#include "runtime_manager/executor/sandboxd/sandboxd_checkpoint_orchestrator.h"

namespace {

using functionsystem::StatusCode;
using functionsystem::runtime_manager::CkptFileManager;
using functionsystem::runtime_manager::CkptFileManagerActor;
using functionsystem::runtime_manager::RuntimeStateManager;
using functionsystem::runtime_manager::SandboxdCheckpointOrchestrator;
using functionsystem::runtime_manager::SandboxInfo;

constexpr char RUNTIME_ID[]  = "runtime-div";
constexpr char SANDBOX_ID[]  = "sandbox-div";
constexpr char REQUEST_ID[]  = "request-div";
constexpr char GENERATION[]  = "gen-div-1";

class ManagedCheckpointDivergenceTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        base_ = "/tmp/ckpt_div_" + litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        anonRoot_ = base_ + "/anonymous";
        managedRoot_ = base_ + "/managed";

        auto actor = std::make_shared<CkptFileManagerActor>(
            "CkptDiv_" + litebus::uuid_generator::UUID::GetRandomUUID().ToString(), anonRoot_);
        actor->cleanupEnabled_ = false;
        litebus::Spawn(actor);
        ckptActorAID_ = actor->GetAID();
        fileManager_ = std::make_shared<CkptFileManager>(actor);

        // null sandboxd client + REAL file manager; the hop target is the
        // live ckpt actor (any live actor works for the Defer continuation)
        orchestrator_ = std::make_shared<SandboxdCheckpointOrchestrator>(
            ckptActorAID_, nullptr, fileManager_, stateManager_, anonRoot_);
    }

    void TearDown() override
    {
        orchestrator_.reset();
        fileManager_.reset();
        litebus::Terminate(ckptActorAID_);
        litebus::Await(ckptActorAID_);
        std::filesystem::remove_all(base_);
    }

    void RegisterSource()
    {
        SandboxInfo info;
        info.runtimeID = RUNTIME_ID;
        info.sandboxID = SANDBOX_ID;
        info.resourceGeneration = GENERATION;
        stateManager_.Register(std::move(info));
    }

    SandboxdCheckpointOrchestrator::SnapshotContext MakeContext(const std::string &checkpointID,
                                                                const std::string &checkpointPath,
                                                                bool managed) const
    {
        const auto info = stateManager_.Find(RUNTIME_ID);
        EXPECT_TRUE(info.has_value());
        SandboxdCheckpointOrchestrator::SnapshotContext context;
        context.requestID          = REQUEST_ID;
        context.runtimeID          = RUNTIME_ID;
        context.checkpointID       = checkpointID;
        context.checkpointPath     = checkpointPath;
        context.sandboxID          = info->sandboxID;
        context.ttl                = 60;
        context.registrationID     = info->registrationID;
        context.resourceGeneration = info->resourceGeneration;
        context.managed            = managed;
        return context;
    }

    static runtime::v1::CheckpointResponse SuccessEcho(const std::string &generation)
    {
        runtime::v1::CheckpointResponse rsp;
        rsp.set_success(true);
        rsp.set_source_generation(generation);
        return rsp;
    }

    std::string base_;
    std::string anonRoot_;
    std::string managedRoot_;
    litebus::AID ckptActorAID_;
    std::shared_ptr<CkptFileManager> fileManager_{ nullptr };
    RuntimeStateManager stateManager_;
    std::shared_ptr<SandboxdCheckpointOrchestrator> orchestrator_;
};

// MANAGED completion with a NULL checkpoint-file manager: returns SUCCESS
// directly — the anonymous branch would have died on the null client, so
// this passing proves no RegisterCheckpoint/AddRef/SetCheckpointID call.
TEST_F(ManagedCheckpointDivergenceTest, ManagedCompletionSkipsAnonymousCacheEntirely)
{
    RegisterSource();
    const std::string managedID = "snap-managed-1";
    const auto context = MakeContext(managedID, managedRoot_ + "/" + managedID, /*managed=*/true);

    orchestrator_.reset();  // drop the real file manager reference first
    fileManager_.reset();
    auto bare = std::make_shared<SandboxdCheckpointOrchestrator>(
        ckptActorAID_, nullptr, nullptr, stateManager_, anonRoot_);

    const auto response = bare->OnCheckpointDone(SuccessEcho(GENERATION), context).Get();

    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(response.snapshotinfo().checkpointid(), managedID);
    EXPECT_EQ(response.snapshotinfo().sandbox_id(), SANDBOX_ID);
    EXPECT_EQ(response.snapshotinfo().source_generation(), GENERATION);
    // no runtime->checkpoint mapping: retirement cannot release what was
    // never registered
    EXPECT_TRUE(stateManager_.GetCheckpointID(RUNTIME_ID).empty());
}

// MANAGED artifact stays unowned by the anonymous actor: not registered,
// not addressable — and remains on disk.
TEST_F(ManagedCheckpointDivergenceTest, ManagedArtifactNeverEntersAnonymousTable)
{
    RegisterSource();
    const std::string managedID = "snap-managed-2";
    const std::string dir = managedRoot_ + "/" + managedID;
    std::filesystem::create_directories(dir);
    { std::ofstream f(dir + "/checkpoint.img", std::ios::binary); f << "payload"; }

    const auto context = MakeContext(managedID, dir, /*managed=*/true);
    EXPECT_EQ(orchestrator_->OnCheckpointDone(SuccessEcho(GENERATION), context).Get().code(),
              static_cast<int32_t>(StatusCode::SUCCESS));

    auto addRef = litebus::Async(ckptActorAID_, &CkptFileManagerActor::AddReference, managedID);
    addRef.Wait();
    EXPECT_TRUE(addRef.IsError()) << "managed artifact must not be addressable in the anonymous table";
    EXPECT_TRUE(std::filesystem::exists(dir + "/checkpoint.img"));
}

// ANONYMOUS regression: RegisterCheckpoint through the REAL actor exactly
// once; the mapping is recorded; the artifact becomes addressable.
TEST_F(ManagedCheckpointDivergenceTest, AnonymousCompletionRegistersOnceAndMapsRuntime)
{
    RegisterSource();
    const std::string anonID = "ckpt-anon-1";
    const std::string dir = anonRoot_ + "/" + anonID;
    std::filesystem::create_directories(dir);
    { std::ofstream f(dir + "/checkpoint.img", std::ios::binary); f << "anonymous payload"; }

    const auto context = MakeContext(anonID, dir, /*managed=*/false);
    const auto response = orchestrator_->OnCheckpointDone(SuccessEcho(GENERATION), context).Get();

    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(response.snapshotinfo().checkpointid(), anonID);
    EXPECT_EQ(response.snapshotinfo().storage(), anonID + ".zip");
    EXPECT_EQ(stateManager_.GetCheckpointID(RUNTIME_ID), anonID);
    EXPECT_TRUE(litebus::Async(ckptActorAID_, &CkptFileManagerActor::AddReference, anonID).Get().IsOk());
}

// Guards stay active for managed requests: wrong generation echo and a
// replaced registration are both rejected before any success path.
TEST_F(ManagedCheckpointDivergenceTest, ManagedCompletionStillRejectsGuardViolations)
{
    RegisterSource();
    const auto context = MakeContext("snap-managed-3", managedRoot_ + "/snap-managed-3", /*managed=*/true);

    auto wrongEcho = orchestrator_->OnCheckpointDone(SuccessEcho("other-generation"), context).Get();
    EXPECT_EQ(wrongEcho.code(), static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CHECKPOINT_FAILED));

    RegisterSource();  // replacement: fresh registrationID
    auto replaced = orchestrator_->OnCheckpointDone(SuccessEcho(GENERATION), context).Get();
    EXPECT_EQ(replaced.code(), static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CHECKPOINT_FAILED));
    EXPECT_TRUE(stateManager_.GetCheckpointID(RUNTIME_ID).empty());
}

}  // namespace
