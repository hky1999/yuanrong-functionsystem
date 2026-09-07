// F8 phase 1: an error wake completion is unresolved, not retirement. Direct
// method/state tests on the real actor; no sandbox, LLM, network or waits.
#include "local_scheduler/snap_ctrl/snap_ctrl_actor.h"
#include <gtest/gtest.h>

#include <string>
#include <limits>

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

KillResponse ErrorResponse(common::ErrorCode code, const std::string &message)
{
    KillResponse rsp;
    rsp.set_code(code);
    rsp.set_message(message);
    return rsp;
}

// Registers a parked entry through the real completion path and arms it as if
// HandleWake had dispatched (waking/attempt cannot be set by OnHandleSnapshot).
std::string ArmWakedEntry(local_scheduler::SnapCtrlActor &actor, const std::string &instanceID,
                          uint64_t attempt, const std::string &holdToken = "hold-token")
{
    EXPECT_EQ(actor.OnHandleSnapshot(instanceID, false, ParkedResult("checkpoint"), holdToken).code(),
              common::ERR_NONE);
    auto &entry = actor.parkedInstances_.at(instanceID);
    entry.waking = true;
    entry.wakeAttempt = attempt;
    return entry.registrationID;
}

void ExpectIdentityRetained(local_scheduler::SnapCtrlActor &actor, const std::string &instanceID,
                            const std::string &registrationID, const std::string &holdToken)
{
    const auto entry = actor.parkedInstances_.find(instanceID);
    ASSERT_NE(entry, actor.parkedInstances_.end());
    EXPECT_EQ(entry->second.checkpointID, "checkpoint");
    EXPECT_EQ(entry->second.registrationID, registrationID);
    EXPECT_EQ(entry->second.sourceSandboxID, "source-sandbox");
    EXPECT_EQ(entry->second.sourceRuntimeID, "source-runtime");
    EXPECT_EQ(entry->second.sourceGeneration, "source-generation");
    EXPECT_EQ(entry->second.holdToken, holdToken);
}
}  // namespace

TEST(WakeUnknownTest, MisleadingErrorSubstringsRetainOwnership)
{
    for (const auto *message : {"backend route not found", "transport credential has expired",
                                "you are not allowed to create instance with the same instance id"}) {
        SCOPED_TRACE(message);
        local_scheduler::SnapCtrlActor actor("wake-text-unknown", "node");
        const auto registration = ArmWakedEntry(actor, "instance", uint64_t{1});
        EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", uint64_t{1}, registration,
                                                ErrorResponse(common::ERR_INNER_SYSTEM_ERROR, message))
                      .code(),
                  common::ERR_INNER_SYSTEM_ERROR);
        ExpectIdentityRetained(actor, "instance", registration, "hold-token");
        const auto &entry = actor.parkedInstances_.at("instance");
        EXPECT_TRUE(entry.wakeUnknown);
        EXPECT_FALSE(entry.waking);
        EXPECT_EQ(entry.wakeFails, uint32_t{1});
    }
}

TEST(WakeUnknownTest, RepeatedDistinctErrorsRetainOwnershipWithoutGiveUp)
{
    local_scheduler::SnapCtrlActor actor("wake-count-unknown", "node");
    const auto registration = ArmWakedEntry(actor, "instance", uint64_t{1});
    const auto error = ErrorResponse(common::ERR_INNER_SYSTEM_ERROR, "transport deadline exceeded; result unknown");
    for (uint64_t attempt = 1; attempt <= 4; ++attempt) {
        auto &entry = actor.parkedInstances_.at("instance");
        entry.waking = true;  // inject a distinct completion; this is not an allowed automatic retry
        entry.wakeAttempt = attempt;
        EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", attempt, registration, error).code(),
                  common::ERR_INNER_SYSTEM_ERROR);
        ASSERT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    }
    ExpectIdentityRetained(actor, "instance", registration, "hold-token");
    const auto &entry = actor.parkedInstances_.at("instance");
    EXPECT_TRUE(entry.wakeUnknown);
    EXPECT_FALSE(entry.waking);
    EXPECT_EQ(entry.wakeFails, uint32_t{4});  // saturating count only, never a drop
}

TEST(WakeUnknownTest, HandleWakeRejectsUnknownWithoutDispatching)
{
    local_scheduler::SnapCtrlActor actor("wake-reject-unknown", "node");
    const auto registration = ArmWakedEntry(actor, "instance", uint64_t{1});
    (void)registration;
    auto &entry = actor.parkedInstances_.at("instance");
    entry.wakeUnknown = true;
    entry.waking = false;
    const auto sequenceBefore = actor.wakeAttemptSeq_;
    // localSchedSrv_ is unbound: reaching HandleSnapStart would abort, so the
    // busy return also proves no dependency was touched.
    const auto response = actor.HandleWake("r", "instance");
    ASSERT_TRUE(response.IsOK());
    EXPECT_EQ(response.Get().code(),
              static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)));
    EXPECT_EQ(actor.wakeAttemptSeq_, sequenceBefore);
    EXPECT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    EXPECT_TRUE(actor.parkedInstances_.at("instance").wakeUnknown);
    EXPECT_FALSE(actor.parkedInstances_.at("instance").waking);
    EXPECT_EQ(actor.parkedInstances_.at("instance").wakeAttempt, uint64_t{1});
}

TEST(WakeUnknownTest, StaleRetryCallbackCannotRestartUnknownEntry)
{
    local_scheduler::SnapCtrlActor actor("wake-retry-unknown", "node");
    auto &entry = actor.parkedInstances_["instance"];
    entry.checkpointID = "checkpoint";
    entry.registrationID = "registration";
    entry.wakeAttempt = uint64_t{1};
    entry.wakeUnknown = true;
    actor.RetryWakeIfCurrent("instance", "checkpoint", uint64_t{1});
    actor.RetryWakeIfCurrent("instance", "other-checkpoint", uint64_t{1});
    actor.RetryWakeIfCurrent("instance", "checkpoint", uint64_t{2});
    EXPECT_EQ(actor.retrySeq_, uint64_t{0});  // RetryWake was never reached
    EXPECT_TRUE(actor.parkedInstances_.at("instance").wakeUnknown);
}

TEST(WakeUnknownTest, FifoPickerExcludesUnknownButKeepsReadyEntries)
{
    local_scheduler::SnapCtrlActor actor("wake-fifo-unknown", "node");
    auto &unknown = actor.parkedInstances_["unknown-instance"];
    unknown.checkpointID = "checkpoint-a";
    unknown.registrationID = "registration-a";
    unknown.wakeUnknown = true;
    auto &inFlight = actor.parkedInstances_["inflight-instance"];
    inFlight.checkpointID = "checkpoint-b";
    inFlight.registrationID = "registration-b";
    inFlight.waking = true;
    auto &ready = actor.parkedInstances_["ready-instance"];
    ready.checkpointID = "checkpoint-c";
    ready.registrationID = "registration-c";
    const auto candidates = actor.GetParkedInstances();
    ASSERT_EQ(candidates.size(), size_t{1});
    EXPECT_EQ(candidates[0].first, "ready-instance");
    EXPECT_EQ(candidates[0].second.checkpointID, "checkpoint-c");
}

TEST(WakeUnknownTest, DuplicateErrorCompletionCountsOnce)
{
    local_scheduler::SnapCtrlActor actor("wake-duplicate-unknown", "node");
    const auto registration = ArmWakedEntry(actor, "instance", uint64_t{1});
    const auto error = ErrorResponse(common::ERR_INNER_SYSTEM_ERROR, "transient backend failure");
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", uint64_t{1}, registration, error).code(),
              common::ERR_INNER_SYSTEM_ERROR);
    ASSERT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    EXPECT_EQ(actor.parkedInstances_.at("instance").wakeFails, uint32_t{1});
    // duplicate delivery of the same settled completion bounces off the guard
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", uint64_t{1}, registration, error).code(),
              common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(actor.parkedInstances_.at("instance").wakeFails, uint32_t{1});
    EXPECT_TRUE(actor.parkedInstances_.at("instance").wakeUnknown);
}

TEST(WakeUnknownTest, StaleCallbacksCannotMarkNewerEntryUnknown)
{
    local_scheduler::SnapCtrlActor actor("wake-stale-unknown", "node");
    const auto staleRegistration = ArmWakedEntry(actor, "instance", uint64_t{1});
    // replacement park of the same checkpoint while the first wake was out
    EXPECT_EQ(actor.OnHandleSnapshot("instance", false, ParkedResult("checkpoint")).code(), common::ERR_NONE);
    auto &entry = actor.parkedInstances_.at("instance");
    const auto registration = entry.registrationID;
    ASSERT_NE(registration, staleRegistration);
    entry.waking = true;
    entry.wakeAttempt = uint64_t{5};
    const auto error = ErrorResponse(common::ERR_INNER_SYSTEM_ERROR, "unrelated failure");
    // stale registration never delegates
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", uint64_t{5}, staleRegistration, error).code(),
              common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_FALSE(entry.wakeUnknown);
    EXPECT_TRUE(entry.waking);
    EXPECT_EQ(entry.wakeFails, uint32_t{0});
    // delegation happens, but the checkpoint/attempt fence rejects
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "old-checkpoint", uint64_t{5}, registration, error).code(),
              common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", uint64_t{4}, registration, error).code(),
              common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_FALSE(entry.wakeUnknown);
    EXPECT_TRUE(entry.waking);
    EXPECT_EQ(entry.wakeFails, uint32_t{0});
}

TEST(WakeUnknownTest, MatchingSuccessStillClearsEntry)
{
    local_scheduler::SnapCtrlActor actor("wake-success-unknown", "node");
    const auto registration = ArmWakedEntry(actor, "instance", uint64_t{1});
    KillResponse success;
    success.set_code(common::ERR_NONE);
    EXPECT_EQ(actor.OnWakeCompleteIfCurrent("instance", "checkpoint", uint64_t{1}, registration, success).code(),
              common::ERR_NONE);
    EXPECT_TRUE(actor.parkedInstances_.empty());
}

TEST(WakeUnknownTest, ExistingInFlightGuardRetainsBehavior)
{
    local_scheduler::SnapCtrlActor actor("wake-inflight-unknown", "node");
    auto &entry = actor.parkedInstances_["instance"];
    entry.checkpointID = "checkpoint";
    entry.registrationID = "registration";
    entry.waking = true;
    const auto sequenceBefore = actor.wakeAttemptSeq_;
    const auto response = actor.HandleWake("r", "instance");
    ASSERT_TRUE(response.IsOK());
    EXPECT_EQ(response.Get().code(),
              static_cast<common::ErrorCode>(StatusCode::ERR_INSTANCE_EXITED));
    EXPECT_EQ(actor.wakeAttemptSeq_, sequenceBefore);
    EXPECT_TRUE(actor.parkedInstances_.at("instance").waking);
    EXPECT_FALSE(actor.parkedInstances_.at("instance").wakeUnknown);
}
TEST(WakeUnknownTest, FailureCounterSaturatesWithoutDroppingOwnership)
{
    local_scheduler::SnapCtrlActor actor("wake-counter-saturation", "node");
    const auto registration = ArmWakedEntry(actor, "instance", uint64_t{1});
    actor.parkedInstances_.at("instance").wakeFails = std::numeric_limits<uint32_t>::max();
    actor.OnWakeCompleteIfCurrent("instance", "checkpoint", uint64_t{1}, registration,
                                  ErrorResponse(common::ERR_INNER_SYSTEM_ERROR, "unknown"));
    ASSERT_EQ(actor.parkedInstances_.count("instance"), size_t{1});
    EXPECT_EQ(actor.parkedInstances_.at("instance").wakeFails, std::numeric_limits<uint32_t>::max());
    EXPECT_TRUE(actor.parkedInstances_.at("instance").wakeUnknown);
    ExpectIdentityRetained(actor, "instance", registration, "hold-token");
}

TEST(WakeUnknownTest, DirectRestoreCannotBypassUnknownCheckpoint)
{
    local_scheduler::SnapCtrlActor actor("direct-restore-unknown", "node");
    const auto registration = ArmWakedEntry(actor, "instance", uint64_t{1});
    actor.parkedInstances_.at("instance").wakeUnknown = true;
    actor.parkedInstances_.at("instance").waking = false;
    const auto response = actor.HandleSnapStart("new-request", "checkpoint", "");
    ASSERT_TRUE(response.IsOK());
    EXPECT_EQ(response.Get().code(),
              static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY)));
    EXPECT_TRUE(actor.restoringCheckpoints_.empty());
    ExpectIdentityRetained(actor, "instance", registration, "hold-token");
}

}  // namespace functionsystem::test
