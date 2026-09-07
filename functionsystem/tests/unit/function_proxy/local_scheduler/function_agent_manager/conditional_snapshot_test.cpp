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

// Direct-callback tests for the generation-fenced snapshot relay. The actor is
// never spawned: every case either preseeds the private pending map (default
// timer, dispatch tail never reached) or stops at an early guard, so no real
// transport, timer or Stats query runs. Private members below rely on the test
// target compiling with -fno-access-control.

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "function_proxy/local_scheduler/function_agent_manager/function_agent_mgr_actor.h"

namespace {
using functionsystem::local_scheduler::FunctionAgentMgrActor;

namespace resource_view = functionsystem::resource_view;

const std::string kAgentID = "func-agent-1";
const std::string kInstanceID = "instance-1";
const std::string kRuntimeID = "runtime-1";
const std::string kExpectedSandbox = "sandbox-expected";
const std::string kExpectedGeneration = "generation-7";
const std::string kAgentAIDName = "agent";

resource_view::InstanceInfo BuildInstanceInfo()
{
    resource_view::InstanceInfo info;
    info.set_instanceid(kInstanceID);
    info.set_runtimeid(kRuntimeID);
    info.set_functionagentid(kAgentID);
    return info;
}

std::shared_ptr<messages::SnapshotRuntimeRequest> BuildRequest(const std::string &requestID)
{
    auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
    request->set_requestid(requestID);
    request->set_instanceid(kInstanceID);
    request->set_runtimeid(kRuntimeID);
    request->set_expected_sandbox_id(kExpectedSandbox);
    request->set_expected_resource_generation(kExpectedGeneration);
    return request;
}

void RegisterAgent(FunctionAgentMgrActor &actor)
{
    FunctionAgentMgrActor::FuncAgentInfo agentInfo;
    agentInfo.isEnable = true;
    agentInfo.isInit = true;
    agentInfo.recoverPromise = std::make_shared<litebus::Promise<bool>>();
    agentInfo.aid = litebus::AID(kAgentAIDName);
    actor.funcAgentTable_[kAgentID] = agentInfo;
}

// Insert a pending entry under a fixed private wire ID. The timer stays
// default-constructed: these tests never arm one, they drive the explicit
// timeout method instead. The returned future is captured before insertion.
litebus::Future<messages::SnapshotRuntimeResponse> SeedPending(FunctionAgentMgrActor &actor,
                                                               const std::string &wireID,
                                                               const messages::SnapshotRuntimeRequest &request)
{
    FunctionAgentMgrActor::SnapshotIfGenerationPending pending;
    pending.funcAgentID = kAgentID;
    pending.agent = litebus::AID(kAgentAIDName);
    pending.request = request;
    auto future = pending.completion.GetFuture();
    actor.snapshotIfGenerationPending_.emplace(wireID, std::move(pending));
    return future;
}

std::string BuildSerializedSuccess(const std::string &privateRequestID, const std::string &checkpointID,
                                   const std::string &sandboxID, const std::string &generation)
{
    messages::SnapshotRuntimeResponse response;
    response.set_requestid(privateRequestID);
    response.set_code(0);
    auto *snapshot = response.mutable_snapshotinfo();
    snapshot->set_checkpointid(checkpointID);
    snapshot->set_source_runtime_id(kRuntimeID);
    snapshot->set_sandbox_id(sandboxID);
    snapshot->set_source_generation(generation);
    return response.SerializeAsString();
}

void DeliverResponse(FunctionAgentMgrActor &actor, const std::string &senderName, const std::string &payload)
{
    actor.SnapshotRuntimeIfGenerationResponse(litebus::AID(senderName), std::string(), std::string(payload));
}

class ConditionalSnapshotTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        actor_ = std::make_unique<FunctionAgentMgrActor>("cond-snap-actor", FunctionAgentMgrActor::Param{},
                                                         "node-test", nullptr);
        RegisterAgent(*actor_);
    }

    std::unique_ptr<FunctionAgentMgrActor> actor_;
};

// An exact duplicate joins the original future; a payload conflict with the
// same caller request ID is rejected without disturbing the original; the
// original's timeout then fulfills both the original and the joined future.
TEST_F(ConditionalSnapshotTest, ExactDuplicateJoinsOriginalFutureAndConflictPreservesIt)
{
    auto request = BuildRequest("caller-1");
    auto original = SeedPending(*actor_, "wire-1", *request);

    auto joined = actor_->SnapshotRuntimeIfGeneration(BuildInstanceInfo(), request);
    EXPECT_FALSE(joined.IsOK());
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1u);

    auto ttlConflict = BuildRequest("caller-1");
    ttlConflict->set_ttl(5);
    auto ttlRejected = actor_->SnapshotRuntimeIfGeneration(BuildInstanceInfo(), ttlConflict);
    ASSERT_TRUE(ttlRejected.IsOK());
    auto ttlResp = ttlRejected.Get();
    EXPECT_NE(ttlResp.code(), 0);
    EXPECT_EQ(ttlResp.requestid(), "caller-1");

    auto leaveRunningConflict = BuildRequest("caller-1");
    leaveRunningConflict->set_leaverunning(true);
    auto leaveRejected = actor_->SnapshotRuntimeIfGeneration(BuildInstanceInfo(), leaveRunningConflict);
    ASSERT_TRUE(leaveRejected.IsOK());
    EXPECT_NE(leaveRejected.Get().code(), 0);

    // the conflicts resolved as fresh failures; the original pending entry and
    // its unfulfilled future are untouched
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1u);
    EXPECT_FALSE(original.IsOK());

    actor_->SnapshotRuntimeIfGenerationTimeout("wire-1");
    ASSERT_TRUE(original.IsOK());
    auto originalResp = original.Get();
    EXPECT_NE(originalResp.code(), 0);
    EXPECT_EQ(originalResp.requestid(), "caller-1");
    EXPECT_NE(originalResp.message().find("timeout"), std::string::npos);

    // the joined call returned the original future: it resolves with the
    // original's timeout completion, not with a conflict rejection
    ASSERT_TRUE(joined.IsOK());
    auto joinedResp = joined.Get();
    EXPECT_EQ(joinedResp.code(), originalResp.code());
    EXPECT_EQ(joinedResp.requestid(), "caller-1");
    EXPECT_EQ(joinedResp.message(), originalResp.message());
    EXPECT_TRUE(actor_->snapshotIfGenerationPending_.empty());
}

// Same request ID and payload, but the InstanceInfo view disagrees on runtime
// or instance identity: validation rejects before the duplicate join can run.
TEST_F(ConditionalSnapshotTest, DuplicateWithMismatchedInstanceInfoIsRejectedBeforeJoin)
{
    auto request = BuildRequest("caller-2");
    auto original = SeedPending(*actor_, "wire-2", *request);

    auto runtimeView = BuildInstanceInfo();
    runtimeView.set_runtimeid("runtime-other");
    auto runtimeRejected = actor_->SnapshotRuntimeIfGeneration(runtimeView, request);
    ASSERT_TRUE(runtimeRejected.IsOK());
    EXPECT_NE(runtimeRejected.Get().code(), 0);
    EXPECT_EQ(runtimeRejected.Get().requestid(), "caller-2");

    auto instanceView = BuildInstanceInfo();
    instanceView.set_instanceid("instance-other");
    auto instanceRejected = actor_->SnapshotRuntimeIfGeneration(instanceView, request);
    ASSERT_TRUE(instanceRejected.IsOK());
    EXPECT_NE(instanceRejected.Get().code(), 0);

    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1u);
    EXPECT_FALSE(original.IsOK());

    actor_->SnapshotRuntimeIfGenerationTimeout("wire-2");
    ASSERT_TRUE(original.IsOK());
    EXPECT_NE(original.Get().code(), 0);
    EXPECT_EQ(original.Get().requestid(), "caller-2");
}

// A reply from the wrong sender, with the registered route missing, or for an
// unknown private ID neither completes nor erases the pending entry; the
// explicit timeout then reports the caller's original request ID.
TEST_F(ConditionalSnapshotTest, UnroutableResponseLeavesPendingUntilTimeout)
{
    auto request = BuildRequest("caller-3");
    auto future = SeedPending(*actor_, "wire-3", *request);
    const auto success = BuildSerializedSuccess("wire-3", "ckpt-3", kExpectedSandbox, kExpectedGeneration);

    DeliverResponse(*actor_, "other-agent", success);
    EXPECT_FALSE(future.IsOK());
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1u);

    (void)actor_->funcAgentTable_.erase(kAgentID);
    DeliverResponse(*actor_, kAgentAIDName, success);
    EXPECT_FALSE(future.IsOK());
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1u);

    RegisterAgent(*actor_);
    DeliverResponse(*actor_, kAgentAIDName, BuildSerializedSuccess("wire-unknown", "ckpt-x", kExpectedSandbox,
                                                                   kExpectedGeneration));
    EXPECT_FALSE(future.IsOK());
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1u);

    actor_->SnapshotRuntimeIfGenerationTimeout("wire-3");
    ASSERT_TRUE(future.IsOK());
    EXPECT_NE(future.Get().code(), 0);
    EXPECT_EQ(future.Get().requestid(), "caller-3");
    EXPECT_TRUE(actor_->snapshotIfGenerationPending_.empty());
}

// A fully matching success reply restores the caller request ID and consumes
// only its own map entry; the sibling entry stays pending.
TEST_F(ConditionalSnapshotTest, MatchingSuccessCompletesOnlyItsOwnEntry)
{
    auto first = SeedPending(*actor_, "wire-4a", *BuildRequest("caller-4a"));
    auto second = SeedPending(*actor_, "wire-4b", *BuildRequest("caller-4b"));

    DeliverResponse(*actor_, kAgentAIDName, BuildSerializedSuccess("wire-4a", "ckpt-4a", kExpectedSandbox,
                                                                   kExpectedGeneration));
    ASSERT_TRUE(first.IsOK());
    auto response = first.Get();
    EXPECT_EQ(response.code(), 0);
    EXPECT_EQ(response.requestid(), "caller-4a");
    ASSERT_TRUE(response.has_snapshotinfo());
    EXPECT_EQ(response.snapshotinfo().checkpointid(), "ckpt-4a");
    EXPECT_EQ(response.snapshotinfo().sandbox_id(), kExpectedSandbox);

    EXPECT_FALSE(second.IsOK());
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1u);
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.count("wire-4b"), 1u);

    actor_->SnapshotRuntimeIfGenerationTimeout("wire-4b");
    ASSERT_TRUE(second.IsOK());
    EXPECT_EQ(second.Get().requestid(), "caller-4b");
    EXPECT_TRUE(actor_->snapshotIfGenerationPending_.empty());
}

// A code-0 reply with a mismatched generation or a missing checkpoint ID is
// converted to a nonzero error and its snapshot payload is cleared; neither
// case may ever deliver a false success.
TEST_F(ConditionalSnapshotTest, InvalidSuccessPayloadBecomesNonZeroError)
{
    auto wrongGeneration = SeedPending(*actor_, "wire-5a", *BuildRequest("caller-5a"));
    DeliverResponse(*actor_, kAgentAIDName,
                    BuildSerializedSuccess("wire-5a", "ckpt-5a", kExpectedSandbox, "generation-wrong"));
    ASSERT_TRUE(wrongGeneration.IsOK());
    auto generationResponse = wrongGeneration.Get();
    EXPECT_NE(generationResponse.code(), 0);
    EXPECT_EQ(generationResponse.requestid(), "caller-5a");
    EXPECT_FALSE(generationResponse.has_snapshotinfo());
    EXPECT_FALSE(generationResponse.message().empty());

    auto missingCheckpoint = SeedPending(*actor_, "wire-5b", *BuildRequest("caller-5b"));
    DeliverResponse(*actor_, kAgentAIDName,
                    BuildSerializedSuccess("wire-5b", "", kExpectedSandbox, kExpectedGeneration));
    ASSERT_TRUE(missingCheckpoint.IsOK());
    auto checkpointResponse = missingCheckpoint.Get();
    EXPECT_NE(checkpointResponse.code(), 0);
    EXPECT_EQ(checkpointResponse.requestid(), "caller-5b");
    EXPECT_FALSE(checkpointResponse.has_snapshotinfo());

    EXPECT_TRUE(actor_->snapshotIfGenerationPending_.empty());
}

// A retry reuses the caller request ID after the old private entry timed out.
// The stale wire ID's timer fire and its late reply must not touch the new
// pending entry; only the new entry's own timeout finishes it, again with the
// caller's ID.
TEST_F(ConditionalSnapshotTest, StalePrivateIdCannotFinishRetriedPending)
{
    auto oldFuture = SeedPending(*actor_, "wire-6-old", *BuildRequest("caller-6"));

    actor_->SnapshotRuntimeIfGenerationTimeout("wire-6-old");
    ASSERT_TRUE(oldFuture.IsOK());
    EXPECT_NE(oldFuture.Get().code(), 0);
    EXPECT_EQ(oldFuture.Get().requestid(), "caller-6");
    ASSERT_TRUE(actor_->snapshotIfGenerationPending_.empty());

    auto newFuture = SeedPending(*actor_, "wire-6-new", *BuildRequest("caller-6"));

    actor_->SnapshotRuntimeIfGenerationTimeout("wire-6-old");
    EXPECT_FALSE(newFuture.IsOK());
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1u);

    DeliverResponse(*actor_, kAgentAIDName,
                    BuildSerializedSuccess("wire-6-old", "ckpt-6", kExpectedSandbox, kExpectedGeneration));
    EXPECT_FALSE(newFuture.IsOK());
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1u);

    actor_->SnapshotRuntimeIfGenerationTimeout("wire-6-new");
    ASSERT_TRUE(newFuture.IsOK());
    EXPECT_NE(newFuture.Get().code(), 0);
    EXPECT_EQ(newFuture.Get().requestid(), "caller-6");
    EXPECT_TRUE(actor_->snapshotIfGenerationPending_.empty());
}

// With 1024 distinct pending entries the map is full: an exact duplicate still
// joins (before the capacity check), while a fresh request is rejected.
TEST_F(ConditionalSnapshotTest, FullPendingMapJoinsDuplicateButRejectsFresh)
{
    for (int i = 0; i < 1024; ++i) {
        (void)SeedPending(*actor_, "wire-cap-" + std::to_string(i), *BuildRequest("caller-cap-" + std::to_string(i)));
    }
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1024u);

    auto joined = actor_->SnapshotRuntimeIfGeneration(BuildInstanceInfo(), BuildRequest("caller-cap-0"));
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1024u);
    EXPECT_FALSE(joined.IsOK());

    auto rejected = actor_->SnapshotRuntimeIfGeneration(BuildInstanceInfo(), BuildRequest("caller-cap-fresh"));
    ASSERT_TRUE(rejected.IsOK());
    EXPECT_NE(rejected.Get().code(), 0);
    EXPECT_EQ(rejected.Get().requestid(), "caller-cap-fresh");
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1024u);

    // the duplicate really joined entry 0: its timeout fulfills the joined future
    actor_->SnapshotRuntimeIfGenerationTimeout("wire-cap-0");
    ASSERT_TRUE(joined.IsOK());
    EXPECT_EQ(joined.Get().requestid(), "caller-cap-0");
    EXPECT_NE(joined.Get().code(), 0);
    EXPECT_EQ(actor_->snapshotIfGenerationPending_.size(), 1023u);
}
}  // namespace
