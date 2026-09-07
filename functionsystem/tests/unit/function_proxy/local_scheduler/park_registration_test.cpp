#include "local_scheduler/snap_ctrl/snap_ctrl_actor.h"
#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

namespace functionsystem::test {
namespace {
local_scheduler::SnapshotResult ParkedResult(const std::string &checkpointID)
{
    local_scheduler::SnapshotResult result;
    result.code = common::ERR_NONE;
    result.snapshotInfo.set_checkpointid(checkpointID);
    result.snapshotInfo.set_sandbox_id("source-sandbox");
    result.snapshotInfo.set_source_runtime_id("source-runtime");
    result.snapshotInfo.set_source_generation("source-generation");
    return result;
}

KillResponse SuccessResponse()
{
    KillResponse rsp;
    rsp.set_code(common::ERR_NONE);
    return rsp;
}
}  // namespace

TEST(ParkRegistrationTest, RepeatedParksMintDistinctNonemptyRegistrations)
{
    local_scheduler::SnapCtrlActor actor("park-mint-test", "node");
    ASSERT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    const auto first = actor.parkedInstances_.at("instance").registrationID;
    // same checkpoint and identical physical source, still a fresh identity
    ASSERT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    const auto second = actor.parkedInstances_.at("instance").registrationID;
    EXPECT_FALSE(first.empty());
    EXPECT_FALSE(second.empty());
    EXPECT_NE(first, second);
}

TEST(ParkRegistrationTest, LeaveRunningErrorAndStaleCompletionsDoNotMintOrOverwrite)
{
    local_scheduler::SnapCtrlActor actor("park-mint-reject-test", "node");
    EXPECT_EQ(actor.OnHandleSnapshot("instance", true, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    EXPECT_EQ(actor.parkedInstances_.count("instance"), size_t{0});
    local_scheduler::SnapshotResult failed;
    failed.code = common::ERR_INNER_SYSTEM_ERROR;
    EXPECT_NE(actor.OnHandleSnapshot("instance", false, failed).code(), common::ERR_NONE);
    EXPECT_EQ(actor.parkedInstances_.count("instance"), size_t{0});
    const auto stale = actor.snapshotAttempts_.Begin("instance").value();
    actor.snapshotAttempts_.Finish("instance", stale, true);
    const auto current = actor.snapshotAttempts_.Begin("instance").value();
    auto &entry = actor.parkedInstances_["instance"];
    entry.checkpointID = "current-checkpoint";
    entry.registrationID = "current-registration";
    EXPECT_NE(actor.OnHandleSnapshot("instance", false, ParkedResult("old-checkpoint"), "", stale).code(),
              common::ERR_NONE);
    EXPECT_EQ(entry.checkpointID, "current-checkpoint");
    EXPECT_EQ(entry.registrationID, "current-registration");
    EXPECT_TRUE(actor.snapshotAttempts_.Matches("instance", current));
    EXPECT_NE(actor.OnHandleSnapshot("instance", false, failed).code(), common::ERR_NONE);
    EXPECT_EQ(actor.OnHandleSnapshot("instance", true, ParkedResult("ignored")).code(), common::ERR_NONE);
    EXPECT_NE(actor.OnHandleSnapshot("instance", false, ParkedResult("")).code(), common::ERR_NONE);
    EXPECT_EQ(entry.checkpointID, "current-checkpoint");
    EXPECT_EQ(entry.registrationID, "current-registration");
}

TEST(ParkRegistrationTest, CapturedCompletionClearsTheOriginalRegistration)
{
    local_scheduler::SnapCtrlActor actor("park-clear-test", "node");
    ASSERT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    const auto captured = actor.CaptureParkedRegistrations("checkpoint");
    ASSERT_EQ(captured.size(), size_t{1});
    ASSERT_EQ(captured[0].first, "instance");
    ASSERT_EQ(captured[0].second, actor.parkedInstances_.at("instance").registrationID);
    actor.ForgetParkedRegistrations(captured);
    EXPECT_TRUE(actor.parkedInstances_.empty());
}

TEST(ParkRegistrationTest, CapturedCompletionSparesReplacementWithSameCheckpointAndSource)
{
    local_scheduler::SnapCtrlActor actor("park-replace-test", "node");
    ASSERT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    const auto original = actor.parkedInstances_.at("instance").registrationID;
    const auto captured = actor.CaptureParkedRegistrations("checkpoint");
    ASSERT_EQ(captured.size(), size_t{1});
    ASSERT_EQ(captured[0].second, original);
    // the entry re-parked with the identical checkpoint and physical source
    ASSERT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    const auto replacement = actor.parkedInstances_.at("instance").registrationID;
    ASSERT_NE(replacement, original);
    actor.ForgetParkedRegistrations(captured);
    ASSERT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    EXPECT_EQ(actor.parkedInstances_.at("instance").registrationID, replacement);
    EXPECT_EQ(actor.parkedInstances_.at("instance").checkpointID, "checkpoint");
    // the replacement's own captured completion still clears it
    actor.ForgetParkedRegistrations(actor.CaptureParkedRegistrations("checkpoint"));
    EXPECT_TRUE(actor.parkedInstances_.empty());
}

TEST(ParkRegistrationTest, RegistrationAbsentAtDispatchSurvivesCompletion)
{
    local_scheduler::SnapCtrlActor actor("park-absent-test", "node");
    const auto captured = actor.CaptureParkedRegistrations("checkpoint");
    ASSERT_TRUE(captured.empty());
    ASSERT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    actor.ForgetParkedRegistrations(captured);
    EXPECT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    EXPECT_FALSE(actor.parkedInstances_.at("instance").registrationID.empty());
}

TEST(ParkRegistrationTest, EmptyRegistrationOrCaptureCannotClearEntry)
{
    local_scheduler::SnapCtrlActor actor("park-empty-test", "node");
    auto &entry = actor.parkedInstances_["instance"];  // legacy seeding: unfenced entry
    entry.checkpointID = "checkpoint";
    entry.waking = true;
    entry.wakeAttempt = 1;
    EXPECT_TRUE(actor.CaptureParkedRegistrations("checkpoint").empty());
    actor.ForgetParkedRegistrations(actor.CaptureParkedRegistrations("checkpoint"));
    const std::vector<local_scheduler::SnapCtrlActor::ParkedRegistration> emptyID = {{"instance", ""}};
    actor.ForgetParkedRegistrations(emptyID);
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", 1, "", SuccessResponse()).code(),
              common::ERR_NONE);
    ASSERT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    EXPECT_TRUE(actor.parkedInstances_.at("instance").waking);
    EXPECT_TRUE(actor.parkedInstances_.at("instance").registrationID.empty());
}

TEST(ParkRegistrationTest, StaleWakeRegistrationCannotSettleSameCheckpointNewPark)
{
    local_scheduler::SnapCtrlActor actor("wake-stale-reg-test", "node");
    ASSERT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    const auto staleRegistration = actor.parkedInstances_.at("instance").registrationID;
    // re-parked with the same checkpoint while the first wake's restore ran
    ASSERT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    auto &entry = actor.parkedInstances_.at("instance");
    entry.waking = true;
    entry.wakeAttempt = 2;
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", 2, staleRegistration, SuccessResponse()).code(),
              common::ERR_NONE);
    ASSERT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    EXPECT_TRUE(actor.parkedInstances_.at("instance").waking);
    EXPECT_EQ(actor.parkedInstances_.at("instance").wakeAttempt, uint64_t{2});
}

TEST(ParkRegistrationTest, MatchingRegistrationAndWakeFenceClearsEntry)
{
    local_scheduler::SnapCtrlActor actor("wake-clear-reg-test", "node");
    ASSERT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    auto &entry = actor.parkedInstances_.at("instance");
    entry.waking = true;
    entry.wakeAttempt = 1;
    const auto registration = entry.registrationID;
    ASSERT_FALSE(registration.empty());
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", 1, registration, SuccessResponse()).code(),
              common::ERR_NONE);
    EXPECT_TRUE(actor.parkedInstances_.empty());
}

TEST(ParkRegistrationTest, MatchingRegistrationStillRequiresWakeCheckpointAndAttempt)
{
    local_scheduler::SnapCtrlActor actor("wake-fence-reg-test", "node");
    ASSERT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    auto &entry = actor.parkedInstances_.at("instance");
    entry.waking = true;
    entry.wakeAttempt = 2;
    const auto registration = entry.registrationID;
    // registration matches but the underlying wake fence does not
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "other-checkpoint", 2, registration, SuccessResponse()).code(),
              common::ERR_NONE);
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", 1, registration, SuccessResponse()).code(),
              common::ERR_NONE);
    ASSERT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    EXPECT_TRUE(actor.parkedInstances_.at("instance").waking);
    EXPECT_EQ(actor.parkedInstances_.at("instance").wakeAttempt, uint64_t{2});
}
}  // namespace functionsystem::test
