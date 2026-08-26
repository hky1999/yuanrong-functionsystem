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

#include "function_proxy/local_scheduler/instance_control/idle/idle_actor.h"
#include "function_proxy/local_scheduler/instance_control/instance_ctrl_actor.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "async/async.hpp"
#include "mocks/mock_instance_control_view.h"
#include "mocks/mock_instance_state_machine.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {

using namespace local_scheduler;
using namespace ::testing;

class IdleActorTest : public ::testing::Test {
public:
    void SetUp() override
    {
        IdleActor::SetOrphanGraceSec(0);  // static: reset between cases
        idleViewMock_ = std::make_shared<MockInstanceControlView>(NODE_ID);
        facadeViewMock_ = std::make_shared<MockInstanceControlView>(NODE_ID);

        facadeActor_ = std::make_shared<InstanceCtrlActor>("facade", NODE_ID, InstanceCtrlConfig{});
        facadeActor_->BindInstanceControlView(facadeViewMock_);
        litebus::Spawn(facadeActor_);

        idleActor_ = std::make_shared<IdleActor>("idle", NODE_ID, idleViewMock_, facadeActor_->GetAID());
        litebus::Spawn(idleActor_);
    }

    void TearDown() override
    {
        litebus::Terminate(idleActor_->GetAID());
        litebus::Await(idleActor_->GetAID());
        litebus::Terminate(facadeActor_->GetAID());
        litebus::Await(facadeActor_->GetAID());
    }

    /**
     * Build a MockInstanceStateMachine in RUNNING state on NODE_ID.
     * @param idleTimeoutSec  >= 1: sets "idleTimeout" createoption; -1: no key (no timer).
     */
    std::shared_ptr<MockInstanceStateMachine> MakeInstance(InstanceState state, int64_t idleTimeoutSec)
    {
        auto sm = std::make_shared<MockInstanceStateMachine>(NODE_ID);
        resources::InstanceInfo info;
        info.set_functionproxyid(NODE_ID);
        info.mutable_instancestatus()->set_code(static_cast<int32_t>(state));
        if (idleTimeoutSec >= 0) {
            (*info.mutable_createoptions())["idle_timeout"] = std::to_string(idleTimeoutSec);
        }
        EXPECT_CALL(*sm, GetInstanceInfo()).WillRepeatedly(Return(info));
        EXPECT_CALL(*sm, GetInstanceState()).WillRepeatedly(Return(state));
        return sm;
    }

    std::shared_ptr<MockInstanceStateMachine> MakeRunningInstance(int64_t idleTimeoutSec)
    {
        return MakeInstance(InstanceState::RUNNING, idleTimeoutSec);
    }

protected:
    static constexpr const char *NODE_ID = "test-node";
    static constexpr const char *INST_ID = "inst-001";

    std::shared_ptr<MockInstanceControlView> idleViewMock_;    // IdleActor's own view
    std::shared_ptr<MockInstanceControlView> facadeViewMock_;  // InstanceCtrlActor's view
    std::shared_ptr<InstanceCtrlActor> facadeActor_;
    std::shared_ptr<IdleActor> idleActor_;
};

/**
 * Feature: idle instance with no active sessions triggers eviction after timeout.
 * Steps:
 *   1. idleViewMock_.GetInstance returns RUNNING sm with idleTimeout=1s.
 *   2. TrafficReport(0) — traffic idle, no sessions → timer starts.
 *   3. After ~1s, HandleIdleTimeout fires → EvictByIdleTimeout dispatched.
 *   4. EvictByIdleTimeout calls facadeViewMock_.GetInstance as first side-effect.
 * Expectation: facadeViewMock_.GetInstance called exactly once.
 */
TEST_F(IdleActorTest, TrafficIdle_NoSessions_TimerFires_EvictsInstance)
{
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    std::atomic<int> callCount{0};
    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID))
        .WillOnce(Invoke([&](const std::string &) {
            callCount++;
            return nullptr;
        }));

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));

    ASSERT_AWAIT_TRUE([&]() { return callCount > 0; });
}

/**
 * Feature: if the initial idle traffic report arrives before the local state
 * machine reaches RUNNING, the RUNNING transition reconciles the recorded idle
 * state and starts the timer.
 * Steps:
 *   1. TrafficReport(0) records traffic idle while GetInstance returns CREATING.
 *   2. The same instance later becomes RUNNING.
 *   3. OnInstanceRunning reconciles the recorded idle state.
 * Expectation: idle timer starts and eventually evicts the instance.
 */
TEST_F(IdleActorTest, RunningTransition_ReconcilesLostInitialIdleReport)
{
    auto creatingSm = MakeInstance(InstanceState::CREATING, 1);
    auto runningSm = MakeRunningInstance(1);

    std::atomic<bool> running{false};
    std::atomic<bool> sawCreatingFetch{false};
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID))
        .WillRepeatedly(Invoke([&](const std::string &) {
            if (!running.load()) {
                sawCreatingFetch.store(true);
            }
            return running.load() ? runningSm : creatingSm;
        }));

    std::atomic<int> callCount{0};
    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID))
        .WillOnce(Invoke([&](const std::string &) {
            callCount++;
            return nullptr;
        }));

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    ASSERT_AWAIT_TRUE([&]() { return sawCreatingFetch.load(); });

    running.store(true);
    litebus::Async(idleActor_->GetAID(), &IdleActor::OnInstanceRunning, std::string(INST_ID));

    ASSERT_AWAIT_TRUE([&]() { return callCount > 0; });
}

/**
 * Feature: busy traffic cancels the pending idle timer — no eviction.
 * Steps:
 *   1. TrafficReport(0) starts timer.
 *   2. TrafficReport(1) cancels timer before it fires.
 *   3. Wait 3s (> idleTimeout of 1s).
 * Expectation: facadeViewMock_.GetInstance never called.
 */
TEST_F(IdleActorTest, TrafficBusy_CancelsTimer)
{
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID)).Times(0);

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(1));

    std::this_thread::sleep_for(std::chrono::seconds(3));
}

/**
 * Feature: a new exec session cancels the idle timer — no eviction.
 * Steps:
 *   1. TrafficReport(0) starts timer.
 *   2. SessionCountDelta(+1) → sessions 0→1 edge → CancelIdleTimer.
 *   3. Wait 3s.
 * Expectation: facadeViewMock_.GetInstance never called.
 */
TEST_F(IdleActorTest, SessionStart_CancelsTimer)
{
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID)).Times(0);

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    litebus::Async(idleActor_->GetAID(), &IdleActor::SessionCountDelta, std::string(INST_ID), 1);

    std::this_thread::sleep_for(std::chrono::seconds(3));
}

/**
 * Feature: session end with traffic already idle restarts the timer → eviction.
 * Steps:
 *   1. TrafficReport(0) — traffic idle.
 *   2. SessionCountDelta(+1) — sessions go 0→1, timer cancelled.
 *   3. SessionCountDelta(-1) — sessions go 1→0, traffic idle → StartIdleTimer.
 *   4. Timer fires → EvictByIdleTimeout.
 * Expectation: facadeViewMock_.GetInstance called once.
 */
TEST_F(IdleActorTest, SessionEnd_WithTrafficIdle_StartsTimer)
{
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    std::atomic<int> callCount{0};
    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID))
        .WillOnce(Invoke([&](const std::string &) {
            callCount++;
            return nullptr;
        }));

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    litebus::Async(idleActor_->GetAID(), &IdleActor::SessionCountDelta, std::string(INST_ID), 1);
    litebus::Async(idleActor_->GetAID(), &IdleActor::SessionCountDelta, std::string(INST_ID), -1);

    ASSERT_AWAIT_TRUE([&]() { return callCount > 0; });
}

/**
 * Feature: session end when traffic is NOT idle does not start the timer.
 * Steps:
 *   1. SessionCountDelta(+1) — session starts (no prior TrafficReport(0)).
 *   2. SessionCountDelta(-1) — session ends; traffic is not idle → no timer.
 *   3. Wait 3s.
 * Expectation: facadeViewMock_.GetInstance never called.
 */
TEST_F(IdleActorTest, SessionEnd_WithTrafficBusy_NoTimer)
{
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID)).Times(0);

    litebus::Async(idleActor_->GetAID(), &IdleActor::SessionCountDelta, std::string(INST_ID), 1);
    litebus::Async(idleActor_->GetAID(), &IdleActor::SessionCountDelta, std::string(INST_ID), -1);

    std::this_thread::sleep_for(std::chrono::seconds(3));
}

/**
 * Feature: stale-generation callback does not evict; only the newest timer evicts.
 * Steps:
 *   1. TrafficReport(0) → timer gen=1 starts.
 *   2. TrafficReport(1) → CancelIdleTimer increments gen to 2.
 *   3. TrafficReport(0) → new timer gen=3 starts.
 *   4. Gen=1 callback (if it fires) must be rejected (gen mismatch).
 *   5. Gen=3 fires → evicts.
 * Expectation: facadeViewMock_.GetInstance called exactly once.
 */
TEST_F(IdleActorTest, StaleGeneration_NewTimerFires_EvictsOnce)
{
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    std::atomic<int> callCount{0};
    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID))
        .WillOnce(Invoke([&](const std::string &) {
            callCount++;
            return nullptr;
        }));

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(1));
    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));

    ASSERT_AWAIT_TRUE([&]() { return callCount > 0; });

    // Allow extra time and verify no second call arrives
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_EQ(callCount.load(), 1);
}

/**
 * Feature: active sessions at the moment of timeout veto eviction in IdleActor.
 * Steps:
 *   1. TrafficReport(0) starts timer (idleTimeout=1s).
 *   2. SessionCountDelta(+1) cancels the timer before it fires.
 *   3. Wait 2s.
 * Expectation: facadeViewMock_.GetInstance never called (timer was cancelled).
 */
TEST_F(IdleActorTest, ActiveSessions_AtTimeout_VetoEviction)
{
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID)).Times(0);

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    // Inject session start quickly after — timer is cancelled via CancelIdleTimer
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    litebus::Async(idleActor_->GetAID(), &IdleActor::SessionCountDelta, std::string(INST_ID), 1);

    std::this_thread::sleep_for(std::chrono::seconds(2));
}

/**
 * Feature: instance with no idleTimeout configured never gets a timer.
 * Steps:
 *   1. idleViewMock_.GetInstance returns sm with NO "idleTimeout" createoption.
 *   2. TrafficReport(0) — StartIdleTimer calls GetIdleTimeout → returns -1 → no timer.
 *   3. Wait 3s.
 * Expectation: facadeViewMock_.GetInstance never called.
 */
TEST_F(IdleActorTest, NoIdleTimeout_NoTimer)
{
    auto sm = MakeRunningInstance(-1);  // no idleTimeout key
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID)).Times(0);

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));

    std::this_thread::sleep_for(std::chrono::seconds(3));
}

/**
 * Feature: Ph0.2 create-race orphan — a never-used instance gets an idle
 * timer from the RUNNING transition alone.
 * Steps:
 *   1. RUNNING sm with idleTimeout=1s, NO TrafficReport ever sent (the
 *      orphan case: client abandoned the create before any exec).
 *   2. OnInstanceRunning — no traffic record counts as idle.
 * Expectation: timer starts and evicts the orphan.
 */
TEST_F(IdleActorTest, Orphan_NoTrafficReport_RunningStartsTimer_Evicts)
{
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    std::atomic<int> callCount{0};
    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID))
        .WillOnce(Invoke([&](const std::string &) {
            callCount++;
            return nullptr;
        }));

    litebus::Async(idleActor_->GetAID(), &IdleActor::OnInstanceRunning, std::string(INST_ID));

    ASSERT_AWAIT_TRUE([&]() { return callCount > 0; });
}

/**
 * Feature: orphan grace window shortens reclamation for never-used instances.
 * Steps:
 *   1. SetOrphanGraceSec(1); RUNNING sm with idleTimeout=5s (would take 5s).
 *   2. OnInstanceRunning — never used -> min(1, 5) = 1s applies.
 * Expectation: eviction well within 5s.
 */
TEST_F(IdleActorTest, OrphanGrace_ShorterThanIdleTimeout_EvictsEarly)
{
    IdleActor::SetOrphanGraceSec(1);
    auto sm = MakeRunningInstance(5);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    std::atomic<int> callCount{0};
    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID))
        .WillOnce(Invoke([&](const std::string &) {
            callCount++;
            return nullptr;
        }));

    auto start = std::chrono::steady_clock::now();
    litebus::Async(idleActor_->GetAID(), &IdleActor::OnInstanceRunning, std::string(INST_ID));

    ASSERT_AWAIT_TRUE([&]() { return callCount > 0; });
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - start).count();
    EXPECT_LT(elapsed, 5);  // orphan grace (1s), not the idle timeout (5s)
}

/**
 * Feature: once an instance has been used (a session came through), the
 * orphan grace no longer applies — the full idle timeout governs.
 * Steps:
 *   1. SetOrphanGraceSec(10); idleTimeout=1s.
 *   2. TrafficReport(0) -> orphan timer 10s.
 *   3. SessionCountDelta(+1) marks ever-used and cancels.
 *   4. SessionCountDelta(-1) with traffic idle -> timer restarts at 1s.
 * Expectation: eviction within the await window (a stuck 10s orphan timer
 * would not fire).
 */
TEST_F(IdleActorTest, EverUsed_Instance_FallsBackToFullIdleTimeout)
{
    IdleActor::SetOrphanGraceSec(10);
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    std::atomic<int> callCount{0};
    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID))
        .WillOnce(Invoke([&](const std::string &) {
            callCount++;
            return nullptr;
        }));

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    litebus::Async(idleActor_->GetAID(), &IdleActor::SessionCountDelta, std::string(INST_ID), 1);
    litebus::Async(idleActor_->GetAID(), &IdleActor::SessionCountDelta, std::string(INST_ID), -1);

    ASSERT_AWAIT_TRUE([&]() { return callCount > 0; });
}

/**
 * Feature: grace disabled and no idleTimeout configured — the RUNNING
 * transition still must not arm a timer (legacy NoIdleTimeout semantics).
 * Steps:
 *   1. RUNNING sm with NO idle_timeout option, orphan grace = 0.
 *   2. OnInstanceRunning with no traffic record.
 * Expectation: no eviction within 3s.
 */
TEST_F(IdleActorTest, GraceDisabled_NoIdleTimeout_RunningStillNoTimer)
{
    auto sm = MakeRunningInstance(-1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID)).Times(0);

    litebus::Async(idleActor_->GetAID(), &IdleActor::OnInstanceRunning, std::string(INST_ID));

    std::this_thread::sleep_for(std::chrono::seconds(3));
}

/**
 * Feature: an explicit client-action mark (SDK exec rides the Invoke channel,
 * not ExecStreamService) promotes the instance out of the orphan grace window
 * and re-arms an already-running grace timer at the full idle timeout.
 * Steps:
 *   1. SetOrphanGraceSec(10); idleTimeout=1s.
 *   2. TrafficReport(0) -> orphan timer 10s (would not fire in the window).
 *   3. MarkInstanceUsed -> flag set, timer re-armed at 1s.
 * Expectation: eviction within the await window (the stale 10s grace deadline
 * must not survive the promotion).
 */
TEST_F(IdleActorTest, MarkInstanceUsed_RearmsGraceTimerAtFullIdleTimeout)
{
    IdleActor::SetOrphanGraceSec(10);
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    std::atomic<int> callCount{0};
    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID))
        .WillOnce(Invoke([&](const std::string &) {
            callCount++;
            return nullptr;
        }));

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    // let the orphan timer arm before promoting
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    litebus::Async(idleActor_->GetAID(), &IdleActor::MarkInstanceUsed, std::string(INST_ID));

    ASSERT_AWAIT_TRUE([&]() { return callCount > 0; });
}

/**
 * Feature: control-plane traffic (frontend heartbeat invokes) does not mark
 * an instance ever-used — only a real exec session does. A busy->idle cycle
 * re-arms the short orphan window, so a create whose owner went silent is
 * still reclaimed quickly even if a stray invoke landed once.
 * Steps:
 *   1. SetOrphanGraceSec(1); idleTimeout=5s.
 *   2. TrafficReport(0) -> orphan timer 1s.
 *   3. TrafficReport(1) -> cancel; TrafficReport(0) -> re-arm (still grace).
 * Expectation: eviction within the await window (a promoted 5s timer plus
 * the cancel would not fire this fast deterministically... 1s grace fires).
 */
TEST_F(IdleActorTest, TrafficBusy_DoesNotMarkEverUsed_GraceStillApplies)
{
    IdleActor::SetOrphanGraceSec(1);
    auto sm = MakeRunningInstance(5);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    std::atomic<int> callCount{0};
    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID))
        .WillOnce(Invoke([&](const std::string &) {
            callCount++;
            return nullptr;
        }));

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(1));
    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));

    auto start = std::chrono::steady_clock::now();
    ASSERT_AWAIT_TRUE([&]() { return callCount > 0; });
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - start).count();
    EXPECT_LT(elapsed, 5);  // orphan grace (1s) still governs after busy->idle
}

/**
 * Feature (D-5 F1): a timer armed while RUNNING must not evict once the
 * instanceID re-appears in a non-RUNNING state (parked -> restore-in-flight).
 * Steps:
 *   1. TrafficReport(0) arms the 1s timer against a RUNNING sm.
 *   2. Before it fires, the sm flips to CREATING (restore reusing the ID).
 *   3. Timer fires; HandleIdleTimeout must skip the eviction.
 * Expectation: facadeViewMock_.GetInstance never called.
 */
TEST_F(IdleActorTest, TimeoutFires_NonRunningState_NoEviction)
{
    auto runningSm = MakeRunningInstance(1);
    auto creatingSm = MakeInstance(InstanceState::CREATING, 1);

    std::atomic<bool> flipped{false};
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID))
        .WillRepeatedly(Invoke([&](const std::string &) {
            return flipped.load() ? creatingSm : runningSm;
        }));

    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID)).Times(0);

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    flipped.store(true);
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

/**
 * Feature (D-5 F1): OnInstanceParked clears all per-ID bookkeeping so the
 * restored (ID-reusing) instance starts a fresh idle lifecycle.
 * Steps:
 *   1. TrafficReport(0) arms the timer; OnInstanceParked clears state.
 *   2. No eviction fires off the stale timer.
 *   3. GetIdleInstances no longer lists the parked ID.
 *   4. OnInstanceRunning (restore complete) re-arms and evicts normally.
 * Expectation: no eviction before the RUNNING re-arm; exactly one after.
 */
TEST_F(IdleActorTest, OnInstanceParked_ClearsState_RestoreLifecycleIsFresh)
{
    auto sm = MakeRunningInstance(1);
    EXPECT_CALL(*idleViewMock_, GetInstance(INST_ID)).WillRepeatedly(Return(sm));

    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, std::string(INST_ID), static_cast<size_t>(0));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    litebus::Async(idleActor_->GetAID(), &IdleActor::OnInstanceParked, std::string(INST_ID));

    // parked: no longer an idle (park victim) candidate, and the stale 1s
    // timer must not evict anything
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto idleSet = litebus::Async(idleActor_->GetAID(), &IdleActor::GetIdleInstances).Get();
    EXPECT_EQ(std::count(idleSet.begin(), idleSet.end(), INST_ID), 0);

    std::atomic<int> callCount{0};
    EXPECT_CALL(*facadeViewMock_, GetInstance(INST_ID))
        .WillOnce(Invoke([&](const std::string &) {
            callCount++;
            return nullptr;
        }));
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_EQ(callCount.load(), 0);  // stale timer dead (would fire ~1.2s)

    // restore complete: fresh RUNNING lifecycle, eviction works again
    litebus::Async(idleActor_->GetAID(), &IdleActor::OnInstanceRunning, std::string(INST_ID));
    ASSERT_AWAIT_TRUE([&]() { return callCount > 0; });
}

}  // namespace functionsystem::test
