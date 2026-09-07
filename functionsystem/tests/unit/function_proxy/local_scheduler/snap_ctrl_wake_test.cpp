#include "local_scheduler/snap_ctrl/snap_ctrl_actor.h"
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace functionsystem::test {
TEST(SnapCtrlWakeTest, DirectedWakeValidatesIdentityBeforeChangingParkedEntry)
{
    local_scheduler::SnapCtrlActor actor("directed-wake-test", "node");
    auto &entry = actor.parkedInstances_["instance"];
    entry.checkpointID = "checkpoint";
    entry.sourceSandboxID = "sandbox";
    entry.sourceRuntimeID = "runtime";
    entry.sourceGeneration = "generation";
    entry.waking = true; // a valid identity reaches the existing single-flight guard
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    nlohmann::json command = {{"schema", 1}, {"sandbox_id", "sandbox"}, {"runtime_id", "runtime"},
        {"resource_generation", "generation"}, {"deadline_unix_ms", nowMs + 30000}};
    EXPECT_EQ(actor.HandleDirectedWake("r", "instance", command.dump()).Get().code(), common::ERR_INSTANCE_EXITED);
    for (const auto *field : {"sandbox_id", "runtime_id", "resource_generation"}) {
        auto wrong = command;
        wrong[field] = "other";
        EXPECT_EQ(actor.HandleDirectedWake("r", "instance", wrong.dump()).Get().code(), common::ERR_PARAM_INVALID);
    }
    for (const auto &payload : {std::string(""), std::string("{}"), std::string("[]"), std::string(4097, 'x')}) {
        EXPECT_EQ(actor.HandleDirectedWake("r", "instance", payload).Get().code(), common::ERR_PARAM_INVALID);
    }
    command["deadline_unix_ms"] = nowMs - 1;
    EXPECT_EQ(actor.HandleDirectedWake("r", "instance", command.dump()).Get().code(), common::ERR_PARAM_INVALID);
    EXPECT_TRUE(entry.waking);
    EXPECT_EQ(entry.wakeAttempt, 0);
}

TEST(SnapCtrlWakeTest, DirectedWakeRequiresTheParkedHoldAttempt)
{
    local_scheduler::SnapCtrlActor actor("hold-wake-test", "node");
    auto &entry = actor.parkedInstances_["instance"];
    entry.checkpointID = "checkpoint";
    entry.sourceSandboxID = "sandbox";
    entry.sourceRuntimeID = "runtime";
    entry.sourceGeneration = "generation";
    entry.holdToken = "physical-hold-token-2";
    entry.waking = true;
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    nlohmann::json command = {{"schema", 2}, {"sandbox_id", "sandbox"}, {"runtime_id", "runtime"},
        {"resource_generation", "generation"}, {"deadline_unix_ms", nowMs + 30000}};
    EXPECT_EQ(actor.HandleDirectedWake("r", "instance", command.dump()).Get().code(), common::ERR_PARAM_INVALID);
    command["hold_token"] = "physical-hold-token-1";
    EXPECT_EQ(actor.HandleDirectedWake("r", "instance", command.dump()).Get().code(), common::ERR_PARAM_INVALID);
    command["hold_token"] = entry.holdToken;
    EXPECT_EQ(actor.HandleDirectedWake("r", "instance", command.dump()).Get().code(), common::ERR_INSTANCE_EXITED);
    command["schema"] = 1;
    EXPECT_EQ(actor.HandleDirectedWake("r", "instance", command.dump()).Get().code(), common::ERR_PARAM_INVALID);
    EXPECT_EQ(entry.wakeAttempt, 0);
}

TEST(SnapCtrlWakeTest, SnapshotCompletionPreservesPhysicalSourceIdentity)
{
    local_scheduler::SnapCtrlActor actor("wake-identity-test", "node");
    local_scheduler::SnapshotResult result;
    result.code = common::ERR_NONE;
    result.snapshotInfo.set_checkpointid("checkpoint");
    result.snapshotInfo.set_sandbox_id("source-sandbox");
    result.snapshotInfo.set_source_runtime_id("source-runtime");
    result.snapshotInfo.set_source_generation("source-generation");
    auto response = actor.OnHandleSnapshot("instance", false, result, "physical-hold-token-1");
    EXPECT_EQ(response.code(), common::ERR_NONE);
    const auto &entry = actor.parkedInstances_.at("instance");
    EXPECT_EQ(entry.sourceSandboxID, "source-sandbox");
    EXPECT_EQ(entry.sourceRuntimeID, "source-runtime");
    EXPECT_EQ(entry.sourceGeneration, "source-generation");
    EXPECT_EQ(entry.holdToken, "physical-hold-token-1");
}

TEST(SnapCtrlWakeTest, SnapshotSingleFlightRejectsBeforeDependencyCalls)
{
    local_scheduler::SnapCtrlActor actor("snapshot-flight-test", "node");
    const auto attempt = actor.snapshotAttempts_.Begin("instance");
    ASSERT_TRUE(attempt.has_value());
    EXPECT_EQ(actor.HandleSnapshot("r", "instance", "").Get().code(),
        static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)));
    actor.snapshotAttempts_.Finish("instance", attempt.value(), true);
    actor.parkedInstances_["instance"].checkpointID = "current";
    EXPECT_EQ(actor.HandleSnapshot("r", "instance", "").Get().code(),
        static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)));
}

TEST(SnapCtrlWakeTest, StaleSnapshotCannotReplaceParkedIdentityOrToken)
{
    local_scheduler::SnapCtrlActor actor("snapshot-stale-test", "node");
    const auto old = actor.snapshotAttempts_.Begin("instance").value();
    actor.snapshotAttempts_.Finish("instance", old, true);
    const auto current = actor.snapshotAttempts_.Begin("instance").value();
    auto &entry = actor.parkedInstances_["instance"];
    entry.checkpointID = "new-checkpoint";
    entry.holdToken = "new-physical-hold-token";
    local_scheduler::SnapshotResult result;
    result.code = common::ERR_NONE;
    result.snapshotInfo.set_checkpointid("old-checkpoint");
    EXPECT_NE(actor.OnHandleSnapshot("instance", false, result, "old-physical-hold-token", old).code(), common::ERR_NONE);
    EXPECT_EQ(entry.checkpointID, "new-checkpoint");
    EXPECT_EQ(entry.holdToken, "new-physical-hold-token");
    auto dispatched = std::make_shared<std::atomic<bool>>(true);
    KillResponse response;
    response.set_code(common::ERR_NONE);
    actor.FinishSnapshotAttempt("instance", old, false, dispatched, litebus::Future<KillResponse>(response));
    EXPECT_TRUE(actor.snapshotAttempts_.Matches("instance", current));
}

TEST(SnapCtrlWakeTest, SnapshotDispatchedFailureRetainsOwnership)
{
    local_scheduler::SnapCtrlActor actor("snapshot-outcome-test", "node");
    const auto attempt = actor.snapshotAttempts_.Begin("instance").value();
    auto dispatched = std::make_shared<std::atomic<bool>>(true);
    litebus::Future<KillResponse> failed;
    failed.SetFailed(123);
    actor.FinishSnapshotAttempt("instance", attempt, false, dispatched, failed);
    EXPECT_TRUE(actor.snapshotAttempts_.Unknown("instance"));
    EXPECT_FALSE(actor.snapshotAttempts_.Begin("instance").has_value());
    const auto before = actor.snapshotAttempts_.Begin("before").value();
    auto unsent = std::make_shared<std::atomic<bool>>(false);
    actor.FinishSnapshotAttempt("before", before, false, unsent, failed);
    EXPECT_FALSE(actor.snapshotAttempts_.Busy("before"));
    local_scheduler::SnapshotResult invalid;
    invalid.code = common::ERR_NONE;
    EXPECT_NE(actor.OnHandleSnapshot("missing", false, invalid).code(), common::ERR_NONE);
    EXPECT_EQ(actor.parkedInstances_.count("missing"), size_t{0});
}

TEST(SnapCtrlWakeTest, DuplicateFailureSettlesOnlyOnce)
{
    local_scheduler::SnapCtrlActor actor("wake-test", "node");
    auto &entry = actor.parkedInstances_["instance"];
    entry.checkpointID = "checkpoint";
    entry.waking = true;
    entry.wakeAttempt = 1;
    KillResponse failed;
    failed.set_code(common::ERR_INNER_SYSTEM_ERROR);
    failed.set_message("transient backend failure");
    actor.OnWakeComplete("instance", "checkpoint", 1, failed);
    ASSERT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    EXPECT_EQ(actor.parkedInstances_.at("instance").wakeFails, 1);
    actor.OnWakeComplete("instance", "checkpoint", 1, failed);
    EXPECT_EQ(actor.parkedInstances_.at("instance").wakeFails, 1);
}

TEST(SnapCtrlWakeTest, OldCompletionAndRetryCannotTouchNewParkOrAttempt)
{
    local_scheduler::SnapCtrlActor actor("wake-fence-test", "node");
    auto &entry = actor.parkedInstances_["instance"];
    entry.checkpointID = "new-checkpoint";
    entry.waking = true;
    entry.wakeAttempt = 2;
    KillResponse success;
    success.set_code(common::ERR_NONE);
    actor.OnWakeComplete("instance", "old-checkpoint", 1, success);
    actor.OnWakeComplete("instance", "new-checkpoint", 1, success);
    ASSERT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    EXPECT_TRUE(actor.parkedInstances_.at("instance").waking);
    entry.waking = false; // a newer attempt has settled before the old timer fires
    actor.RetryWakeIfCurrent("instance", "old-checkpoint", 1);
    actor.RetryWakeIfCurrent("instance", "new-checkpoint", 1);
    EXPECT_EQ(actor.retrySeq_, 0);
    entry.waking = true;
    actor.OnWakeComplete("instance", "new-checkpoint", 2, success);
    EXPECT_TRUE(actor.parkedInstances_.empty());
}
}  // namespace functionsystem::test
