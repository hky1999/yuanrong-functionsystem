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

/*
 * Unit coverage for the stage guards of the physical-hold park preparation
 * state machine in PressureMonitorActor: stale attempt UUIDs must never
 * retire a replacement attempt's record, erasure on the Prepare/Snapshot/
 * Release callbacks is stage-gated, unconfirmed outcomes retain the record,
 * and BeginParkRelease must not touch the proxy once the checkpoint is
 * dispatched.
 *
 * Every test drives the private callbacks directly on a local, never-spawned
 * actor with hand-seeded ParkPreparation records and already-completed
 * litebus value futures -- no runtime/CPU bindings, no actor system, no real
 * HTTP. The unit target compiles with -fno-access-control, so the private
 * map, nested struct and callbacks need no friend scaffolding.
 */

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "common/proto/pb/posix_pb.h"
#include "local_scheduler/pressure_monitor/pressure_monitor_actor.h"

namespace functionsystem::local_scheduler {
namespace {

using Stage = PressureMonitorActor::ParkPreparation::Stage;

/** Fresh preparation record with a valid hold identity, chosen stage/token. */
PressureMonitorActor::ParkPreparation MakePreparation(const std::string &attemptID, Stage stage,
                                                      const std::string &token = "")
{
    PressureMonitorActor::ParkPreparation preparation;
    preparation.attemptID = attemptID;
    preparation.identity.instanceID = "inst-1";
    preparation.identity.sandboxID = "sbx-1";
    preparation.identity.runtimeID = "rt-1";
    preparation.identity.generation = "gen-1";
    preparation.token = token;
    preparation.stage = stage;
    return preparation;
}

PhysicalHoldResult Hold(PhysicalHoldOutcome outcome, const std::string &token = "")
{
    PhysicalHoldResult result;
    result.outcome = outcome;
    result.token = token;
    return result;
}

KillResponse SnapshotResponseOK()
{
    KillResponse rsp;
    rsp.set_code(common::ERR_NONE);
    return rsp;
}

KillResponse SnapshotResponseFailed()
{
    KillResponse rsp;
    rsp.set_code(common::ERR_LOCAL_SCHEDULER_ABNORMAL);
    return rsp;
}

class PhysicalLLMParkPreparationTest : public ::testing::Test {
protected:
    // Never Init()/Spawn()ed: the callbacks under test are invoked directly,
    // so no litebus actor machinery is needed.
    PressureMonitorActor actor_{"pressure-park-test", "node-ut", PressureMonitorConfig{}};
};

// A late callback carrying a stale attempt UUID must not erase (or mutate)
// the current record, even when its outcome would otherwise be terminal.
TEST_F(PhysicalLLMParkPreparationTest, StaleAttemptUuidNeverErasesCurrentRecord)
{
    actor_.parkPreparations_["inst-prepare"] = MakePreparation("current", Stage::PREPARING);
    actor_.OnParkPrepared("inst-prepare", "stale",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::LOCAL_REJECTED)));
    auto prepared = actor_.parkPreparations_.find("inst-prepare");
    ASSERT_NE(actor_.parkPreparations_.end(), prepared);
    EXPECT_EQ("current", prepared->second.attemptID);
    EXPECT_EQ(Stage::PREPARING, prepared->second.stage);

    actor_.parkPreparations_["inst-snap"] = MakePreparation("current", Stage::SNAPSHOT, "held-token");
    actor_.OnParkSnapshotDone("inst-snap", "stale", litebus::Future<KillResponse>(SnapshotResponseOK()));
    auto snapped = actor_.parkPreparations_.find("inst-snap");
    ASSERT_NE(actor_.parkPreparations_.end(), snapped);
    EXPECT_EQ("current", snapped->second.attemptID);
    EXPECT_EQ(Stage::SNAPSHOT, snapped->second.stage);

    actor_.parkPreparations_["inst-release"] = MakePreparation("current", Stage::RELEASING, "held-token");
    actor_.OnParkReleased("inst-release", "stale",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::RELEASED, "held-token")));
    auto released = actor_.parkPreparations_.find("inst-release");
    ASSERT_NE(actor_.parkPreparations_.end(), released);
    EXPECT_EQ("current", released->second.attemptID);
    EXPECT_EQ(Stage::RELEASING, released->second.stage);
}

// LOCAL_REJECTED and an initial UNCHANGED (no hold of ours to unwind) erase
// the record only at PREPARING; a record already in SNAPSHOT survives both.
TEST_F(PhysicalLLMParkPreparationTest, PrepareRejectedOrUnchangedErasesOnlyPreparingStage)
{
    actor_.parkPreparations_["inst-rejected"] = MakePreparation("a1", Stage::PREPARING);
    actor_.OnParkPrepared("inst-rejected", "a1",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::LOCAL_REJECTED)));
    EXPECT_EQ(0U, actor_.parkPreparations_.count("inst-rejected"));

    actor_.parkPreparations_["inst-unchanged"] = MakePreparation("a2", Stage::PREPARING);
    actor_.OnParkPrepared("inst-unchanged", "a2",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::UNCHANGED)));
    EXPECT_EQ(0U, actor_.parkPreparations_.count("inst-unchanged"));

    actor_.parkPreparations_["inst-snap-rejected"] = MakePreparation("a3", Stage::SNAPSHOT, "held-token");
    actor_.OnParkPrepared("inst-snap-rejected", "a3",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::LOCAL_REJECTED)));
    EXPECT_EQ(1U, actor_.parkPreparations_.count("inst-snap-rejected"));
    EXPECT_EQ(Stage::SNAPSHOT, actor_.parkPreparations_["inst-snap-rejected"].stage);

    actor_.parkPreparations_["inst-snap-unchanged"] = MakePreparation("a4", Stage::SNAPSHOT, "held-token");
    actor_.OnParkPrepared("inst-snap-unchanged", "a4",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::UNCHANGED)));
    EXPECT_EQ(1U, actor_.parkPreparations_.count("inst-snap-unchanged"));
    EXPECT_EQ(Stage::SNAPSHOT, actor_.parkPreparations_["inst-snap-unchanged"].stage);
}

// UNKNOWN (and the nonsensical RELEASED-at-prepare) means a hold may exist
// whose token was never confirmed: the record is retained at its original
// stage with the token unclobbered. A transport-failed future (!IsOK())
// takes the same retained early return.
TEST_F(PhysicalLLMParkPreparationTest, PrepareUnconfirmedOutcomeRetainsPreparation)
{
    actor_.parkPreparations_["inst-unknown"] = MakePreparation("a1", Stage::PREPARING);
    actor_.OnParkPrepared("inst-unknown", "a1",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::UNKNOWN)));
    auto unknown = actor_.parkPreparations_.find("inst-unknown");
    ASSERT_NE(actor_.parkPreparations_.end(), unknown);
    EXPECT_EQ(Stage::PREPARING, unknown->second.stage);
    EXPECT_TRUE(unknown->second.token.empty());

    actor_.parkPreparations_["inst-released-at-prepare"] = MakePreparation("a2", Stage::PREPARING);
    actor_.OnParkPrepared("inst-released-at-prepare", "a2",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::RELEASED, "ghost-token")));
    auto released = actor_.parkPreparations_.find("inst-released-at-prepare");
    ASSERT_NE(actor_.parkPreparations_.end(), released);
    EXPECT_EQ(Stage::PREPARING, released->second.stage);
    EXPECT_TRUE(released->second.token.empty());

    litebus::Future<PhysicalHoldResult> failed;
    failed.SetFailed(-1);
    actor_.parkPreparations_["inst-failed-future"] = MakePreparation("a3", Stage::PREPARING);
    actor_.OnParkPrepared("inst-failed-future", "a3", failed);
    auto failedRecord = actor_.parkPreparations_.find("inst-failed-future");
    ASSERT_NE(actor_.parkPreparations_.end(), failedRecord);
    EXPECT_EQ(Stage::PREPARING, failedRecord->second.stage);
    EXPECT_TRUE(failedRecord->second.token.empty());
}

// A successful KillResponse retires the record only at SNAPSHOT; a nonzero
// code leaves the hold and record for reconciliation, and a PREPARING record
// is not retired by a snapshot answer at all.
TEST_F(PhysicalLLMParkPreparationTest, SnapshotDoneErasesOnlySnapshotStageOnSuccess)
{
    actor_.parkPreparations_["inst-ok"] = MakePreparation("a1", Stage::SNAPSHOT, "held-token");
    actor_.OnParkSnapshotDone("inst-ok", "a1", litebus::Future<KillResponse>(SnapshotResponseOK()));
    EXPECT_EQ(0U, actor_.parkPreparations_.count("inst-ok"));

    actor_.parkPreparations_["inst-failed"] = MakePreparation("a2", Stage::SNAPSHOT, "held-token");
    actor_.OnParkSnapshotDone("inst-failed", "a2", litebus::Future<KillResponse>(SnapshotResponseFailed()));
    EXPECT_EQ(1U, actor_.parkPreparations_.count("inst-failed"));
    EXPECT_EQ(Stage::SNAPSHOT, actor_.parkPreparations_["inst-failed"].stage);

    actor_.parkPreparations_["inst-preparing"] = MakePreparation("a3", Stage::PREPARING);
    actor_.OnParkSnapshotDone("inst-preparing", "a3", litebus::Future<KillResponse>(SnapshotResponseOK()));
    EXPECT_EQ(1U, actor_.parkPreparations_.count("inst-preparing"));
    EXPECT_EQ(Stage::PREPARING, actor_.parkPreparations_["inst-preparing"].stage);
}

// The release acknowledgement erases only at RELEASING and only when the
// acknowledged token is exactly our own; a foreign token, another stage, or
// an unconfirmed outcome keeps the record.
TEST_F(PhysicalLLMParkPreparationTest, ReleasedAckErasesOnlyReleasingStageWithExactToken)
{
    actor_.parkPreparations_["inst-exact"] = MakePreparation("a1", Stage::RELEASING, "held-token");
    actor_.OnParkReleased("inst-exact", "a1",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::RELEASED, "held-token")));
    EXPECT_EQ(0U, actor_.parkPreparations_.count("inst-exact"));

    actor_.parkPreparations_["inst-wrong-token"] = MakePreparation("a2", Stage::RELEASING, "held-token");
    actor_.OnParkReleased("inst-wrong-token", "a2",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::RELEASED, "other-token")));
    EXPECT_EQ(1U, actor_.parkPreparations_.count("inst-wrong-token"));
    EXPECT_EQ(Stage::RELEASING, actor_.parkPreparations_["inst-wrong-token"].stage);

    actor_.parkPreparations_["inst-wrong-stage"] = MakePreparation("a3", Stage::SNAPSHOT, "held-token");
    actor_.OnParkReleased("inst-wrong-stage", "a3",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::RELEASED, "held-token")));
    EXPECT_EQ(1U, actor_.parkPreparations_.count("inst-wrong-stage"));
    EXPECT_EQ(Stage::SNAPSHOT, actor_.parkPreparations_["inst-wrong-stage"].stage);

    actor_.parkPreparations_["inst-unconfirmed"] = MakePreparation("a4", Stage::RELEASING, "held-token");
    actor_.OnParkReleased("inst-unconfirmed", "a4",
                          litebus::Future<PhysicalHoldResult>(Hold(PhysicalHoldOutcome::UNKNOWN)));
    EXPECT_EQ(1U, actor_.parkPreparations_.count("inst-unconfirmed"));
    EXPECT_EQ(Stage::RELEASING, actor_.parkPreparations_["inst-unconfirmed"].stage);
}

// BeginParkRelease is pre-snapshot cleanup: with the checkpoint dispatched
// (SNAPSHOT) or a release already in flight (RELEASING) it must be a no-op --
// no proxy request, no stage change, record intact. The final direct Prepare
// proves the counting sender is genuinely wired through the bound client, so
// the zeros above mean "guard held", not "client inert".
TEST_F(PhysicalLLMParkPreparationTest, BeginParkReleaseAfterDispatchNeverInvokesProxy)
{
    auto senderCalls = std::make_shared<int>(0);
    auto client = std::make_shared<PhysicalLLMClient>("http://127.0.0.1:9", std::string(48, 'k'), 2000,
        [senderCalls](const litebus::http::Request &) {
            ++(*senderCalls);
            return litebus::Future<litebus::http::Response>(litebus::http::Response{});
        });
    actor_.BindPhysicalLLMClient(client);

    actor_.parkPreparations_["inst-snap"] = MakePreparation("a1", Stage::SNAPSHOT, "held-token");
    actor_.BeginParkRelease("inst-snap", "a1");
    EXPECT_EQ(0, *senderCalls);
    EXPECT_EQ(1U, actor_.parkPreparations_.count("inst-snap"));
    EXPECT_EQ(Stage::SNAPSHOT, actor_.parkPreparations_["inst-snap"].stage);

    actor_.parkPreparations_["inst-releasing"] = MakePreparation("a2", Stage::RELEASING, "held-token");
    actor_.BeginParkRelease("inst-releasing", "a2");
    EXPECT_EQ(0, *senderCalls);
    EXPECT_EQ(1U, actor_.parkPreparations_.count("inst-releasing"));
    EXPECT_EQ(Stage::RELEASING, actor_.parkPreparations_["inst-releasing"].stage);

    PhysicalLLMIdentity identity;
    identity.instanceID = "inst-ctl";
    identity.sandboxID = "sbx-ctl";
    identity.runtimeID = "rt-ctl";
    identity.generation = "gen-ctl";
    (void)client->Prepare(identity);
    EXPECT_EQ(1, *senderCalls);
}

}  // namespace
}  // namespace functionsystem::local_scheduler
