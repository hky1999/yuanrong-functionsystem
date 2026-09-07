/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Direct-callback unit tests for the birth-identity guards in
 * SandboxdCheckpointOrchestrator. The orchestrator is built with null gRPC /
 * checkpoint-file clients: every case must refuse (or finish its local
 * bookkeeping) before any client use, which doubles as the proof that no RPC,
 * upload, or reference release happened. These tests do not cover the async
 * upload lifecycle, gRPC transport, or actor scheduling.
 *
 * OnCheckpointDone / OnRegisterDone and the SnapshotContext struct are private
 * and are reached directly via the test target's -fno-access-control.
 */

#include "runtime_manager/executor/sandboxd/sandboxd_checkpoint_orchestrator.h"

#include <memory>
#include <string>

#include "gtest/gtest.h"

namespace {

using functionsystem::StatusCode;
using functionsystem::runtime_manager::CheckpointUploadResult;
using functionsystem::runtime_manager::RuntimeStateManager;
using functionsystem::runtime_manager::SandboxdCheckpointOrchestrator;
using functionsystem::runtime_manager::SandboxInfo;

constexpr char RUNTIME_ID[]  = "runtime";
constexpr char SANDBOX_ID[]  = "sandbox";
constexpr char REQUEST_ID[]  = "request";
constexpr char INSTANCE_ID[] = "instance";
constexpr char GENERATION[]  = "gen-1";

class SandboxdCheckpointIdentityTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        orchestrator_ = std::make_shared<SandboxdCheckpointOrchestrator>(
            litebus::AID("checkpoint-test"), nullptr, nullptr, stateManager_, "/tmp/checkpoints");
    }

    // Registers a complete birth identity for RUNTIME_ID. Register() always
    // overwrites registrationID with a fresh random UUID, so a replacement is
    // told apart from the captured original by value, never by a fixed string.
    void RegisterSource(const std::string &resourceGeneration = GENERATION)
    {
        SandboxInfo info;
        info.runtimeID = RUNTIME_ID;
        info.sandboxID = SANDBOX_ID;
        info.resourceGeneration = resourceGeneration;
        stateManager_.Register(std::move(info));
    }

    std::shared_ptr<messages::SnapshotRuntimeRequest> MakeRequest() const
    {
        auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
        request->set_runtimeid(RUNTIME_ID);
        request->set_requestid(REQUEST_ID);
        request->set_instanceid(INSTANCE_ID);
        request->set_ttl(60);
        return request;
    }

    // Mirrors the single Find() capture TakeSnapshot performs; the context
    // carries the registered birth identity in every field.
    SandboxdCheckpointOrchestrator::SnapshotContext MakeContext() const
    {
        const auto info = stateManager_.Find(RUNTIME_ID);
        EXPECT_TRUE(info.has_value());
        SandboxdCheckpointOrchestrator::SnapshotContext context;
        if (!info.has_value()) {
            return context;
        }
        context.requestID          = REQUEST_ID;
        context.runtimeID          = RUNTIME_ID;
        context.checkpointID       = "ckpt-fixture";
        context.checkpointPath     = "/tmp/checkpoints/ckpt-fixture";
        context.sandboxID          = info->sandboxID;
        context.ttl                = 60;
        context.registrationID     = info->registrationID;
        context.resourceGeneration = info->resourceGeneration;
        return context;
    }

    // Declared before orchestrator_ so the referenced state outlives it.
    RuntimeStateManager stateManager_;
    std::shared_ptr<SandboxdCheckpointOrchestrator> orchestrator_;
};

// Empty expected generation must fail locally without requiring a bound
// client: the guard runs before the null-client assertion.
TEST_F(SandboxdCheckpointIdentityTest, DoCheckpointRejectsEmptyGenerationWithoutClient)
{
    auto req = std::make_shared<runtime::v1::CheckpointRequest>();
    req->set_id(SANDBOX_ID);
    req->set_trace_id(REQUEST_ID);

    const auto response = orchestrator_->DoCheckpoint(req).Get();

    EXPECT_FALSE(response.success());
    EXPECT_NE(response.message().find("expected generation"), std::string::npos);
}

// A registered sandbox whose birth generation was never recorded is refused
// before any RPC is attempted (the null client would otherwise abort).
TEST_F(SandboxdCheckpointIdentityTest, TakeSnapshotRejectsMissingStoredGeneration)
{
    SandboxInfo info;
    info.runtimeID          = RUNTIME_ID;
    info.sandboxID          = SANDBOX_ID;
    info.resourceGeneration = "";  // birth generation never recorded
    // registrationID is auto-generated by Register(); only the generation is missing
    stateManager_.Register(std::move(info));

    const auto response = orchestrator_->TakeSnapshot(MakeRequest()).Get();

    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CHECKPOINT_FAILED));
    EXPECT_NE(response.message().find("birth identity"), std::string::npos);
}

// The expected pair must be supplied together: either half alone is ambiguous.
TEST_F(SandboxdCheckpointIdentityTest, TakeSnapshotRejectsHalfSuppliedExpectedPair)
{
    RegisterSource();

    auto sandboxOnly = MakeRequest();
    sandboxOnly->set_expected_sandbox_id(SANDBOX_ID);
    auto response = orchestrator_->TakeSnapshot(sandboxOnly).Get();
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::PARAMETER_ERROR));

    auto generationOnly = MakeRequest();
    generationOnly->set_expected_resource_generation(GENERATION);
    response = orchestrator_->TakeSnapshot(generationOnly).Get();
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::PARAMETER_ERROR));
}

// A complete expected pair that disagrees with the registered birth identity
// (stale caller view) is refused before any RPC.
TEST_F(SandboxdCheckpointIdentityTest, TakeSnapshotRejectsExpectedIdentityMismatch)
{
    RegisterSource();

    auto wrongSandbox = MakeRequest();
    wrongSandbox->set_expected_sandbox_id("other-sandbox");
    wrongSandbox->set_expected_resource_generation(GENERATION);
    EXPECT_EQ(orchestrator_->TakeSnapshot(wrongSandbox).Get().code(),
              static_cast<int32_t>(StatusCode::PARAMETER_ERROR));

    auto wrongGeneration = MakeRequest();
    wrongGeneration->set_expected_sandbox_id(SANDBOX_ID);
    wrongGeneration->set_expected_resource_generation("other-generation");
    EXPECT_EQ(orchestrator_->TakeSnapshot(wrongGeneration).Get().code(),
              static_cast<int32_t>(StatusCode::PARAMETER_ERROR));
}

// A daemon success echo that is missing or that names another generation is
// refused before the (null) upload client is touched, and the response does
// not adopt the daemon identity.
TEST_F(SandboxdCheckpointIdentityTest, OnCheckpointDoneRejectsGenerationEchoMismatch)
{
    RegisterSource();
    const auto context = MakeContext();

    runtime::v1::CheckpointResponse missing;
    missing.set_success(true);  // no source_generation echo
    auto response = orchestrator_->OnCheckpointDone(missing, context).Get();
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CHECKPOINT_FAILED));
    EXPECT_TRUE(response.snapshotinfo().source_generation().empty());

    runtime::v1::CheckpointResponse wrong;
    wrong.set_success(true);
    wrong.set_source_generation("other-generation");
    response = orchestrator_->OnCheckpointDone(wrong, context).Get();
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CHECKPOINT_FAILED));
}

// Even a matching generation echo is refused when the sandbox was
// re-registered (fresh registrationID) while the checkpoint RPC was in
// flight: the captured identity no longer maps to a live sandbox, and the
// replacement entry is left untouched.
TEST_F(SandboxdCheckpointIdentityTest, OnCheckpointDoneRejectsReplacedRegistrationDespiteMatchingGeneration)
{
    RegisterSource();
    const auto context = MakeContext();

    RegisterSource();  // same runtime/sandbox/generation, fresh auto-generated registration

    runtime::v1::CheckpointResponse ckpt;
    ckpt.set_success(true);
    ckpt.set_source_generation(context.resourceGeneration);

    const auto response = orchestrator_->OnCheckpointDone(ckpt, context).Get();

    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CHECKPOINT_FAILED));
    const auto current = stateManager_.Find(RUNTIME_ID);
    ASSERT_TRUE(current.has_value());
    EXPECT_FALSE(current->registrationID.empty());
    EXPECT_NE(current->registrationID, context.registrationID);
    EXPECT_TRUE(stateManager_.GetCheckpointID(RUNTIME_ID).empty());
}

// The post-upload registration only records the checkpoint when the exact
// captured incarnation is still current: a matching context succeeds, a
// replacement keeps its own checkpoint mapping, and an absent entry rejects.
TEST_F(SandboxdCheckpointIdentityTest, OnRegisterDoneGuardsStateAgainstReplacementAndAbsence)
{
    RegisterSource();
    const auto context = MakeContext();

    CheckpointUploadResult upload;
    upload.storageUrl = "storage://fixture";

    messages::SnapshotRuntimeResponse ok;
    ok.set_requestid(context.requestID);
    const auto positive = orchestrator_->OnRegisterDone(upload, ok, context).Get();
    EXPECT_EQ(positive.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(positive.snapshotinfo().checkpointid(), context.checkpointID);
    EXPECT_EQ(stateManager_.GetCheckpointID(RUNTIME_ID), context.checkpointID);

    RegisterSource();
    stateManager_.SetCheckpointID(RUNTIME_ID, "ckpt-replacement");

    messages::SnapshotRuntimeResponse stale;
    stale.set_requestid(context.requestID);
    const auto rejected = orchestrator_->OnRegisterDone(upload, stale, context).Get();
    EXPECT_NE(rejected.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(stateManager_.GetCheckpointID(RUNTIME_ID), "ckpt-replacement");
    const auto current = stateManager_.Find(RUNTIME_ID);
    ASSERT_TRUE(current.has_value());
    EXPECT_FALSE(current->registrationID.empty());
    EXPECT_NE(current->registrationID, context.registrationID);

    stateManager_.Unregister(RUNTIME_ID);
    messages::SnapshotRuntimeResponse absent;
    absent.set_requestid(context.requestID);
    const auto rejectedAgain = orchestrator_->OnRegisterDone(upload, absent, context).Get();
    EXPECT_NE(rejectedAgain.code(), static_cast<int32_t>(StatusCode::SUCCESS));
}

}  // namespace
