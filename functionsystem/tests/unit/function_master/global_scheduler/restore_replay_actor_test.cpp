/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

// Actor-level integration coverage for the SnapStart replay ledger: real
// MasterBusiness dispatch on a real litebus actor thread, real actor-message
// round trips, and the real SnapManagerActor::CompleteSnapStart completion
// path. No runtime, SDK, etcd or cloud dependency: the metadata client is
// null, the global scheduler is a fake returning controllable promises, and
// snapshot metadata is seeded directly into the actor's cache.

#include "function_master/snap_manager/snap_manager_actor.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "async/async.hpp"
#include "async/uuid_generator.hpp"
#include "common/proto/pb/posix/message.pb.h"
#include "common/status/status.h"
#include "function_master/global_scheduler/global_sched.h"
#include "litebus.hpp"

namespace functionsystem::test {

using functionsystem::snap_manager::RestoreReplayLedger;
using functionsystem::snap_manager::SnapManagerActor;
using functionsystem::snap_manager::SnapManagerConfig;
using functionsystem::snap_manager::SnapshotMetadata;

namespace {

constexpr std::chrono::milliseconds kMaxWait{5000};  // every wait is bounded

// ----------------------------------------------------------------------------
// Fakes and harness
// ----------------------------------------------------------------------------

// Counts real Schedule calls and hands out controllable futures, so tests
// decide on the test thread when scheduling settles and with what result.
class FakeGlobalSched : public functionsystem::global_scheduler::GlobalSched {
public:
    litebus::Future<Status> Schedule(const std::shared_ptr<messages::ScheduleRequest> &req) override
    {
        (void)req;
        auto promise = std::make_shared<litebus::Promise<Status>>();
        std::lock_guard<std::mutex> lock(mutex_);
        ++scheduleCalls_;
        promise_ = promise;
        cv_.notify_all();
        return promise->GetFuture();
    }

    bool WaitForScheduleCalls(size_t count)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, kMaxWait, [this, count]() { return scheduleCalls_ >= count; });
    }

    size_t ScheduleCalls()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return scheduleCalls_;
    }

    // Completes the most recent schedule attempt from the test thread; the
    // actor's OnComplete then posts its completion to the actor thread.
    void CompleteLast(Status status)
    {
        std::shared_ptr<litebus::Promise<Status>> promise;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            promise = promise_;
        }
        if (promise != nullptr) {
            promise->SetValue(std::move(status));
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    size_t scheduleCalls_{0};
    std::shared_ptr<litebus::Promise<Status>> promise_{nullptr};
};

// Real reply actor: records the raw wire bytes of every
// SnapStartCheckpointResponse it receives and can re-send requests.
class SnapStartReplyActor : public litebus::ActorBase {
public:
    explicit SnapStartReplyActor(const std::string &name) : ActorBase(name) {}

    void Init() override
    {
        Receive("SnapStartCheckpointResponse", &SnapStartReplyActor::OnSnapStartResponse);
    }

    void OnSnapStartResponse(const litebus::AID &, std::string &&, std::string &&msg)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        responses_.push_back(std::move(msg));
        cv_.notify_all();
    }

    // Invoke through litebus::Async on this actor so the request travels the
    // real actor-message path with this actor as the requester.
    void SendSnapStart(const litebus::AID &to, const std::string &wire)
    {
        Send(to, "SnapStartCheckpoint", std::string(wire));
    }

    bool WaitForResponses(size_t count)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, kMaxWait, [this, count]() { return responses_.size() >= count; });
    }

    size_t ResponseCount()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return responses_.size();
    }

    std::vector<std::string> Responses()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return responses_;
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::string> responses_;
};

// Real SnapManagerActor with the production master SnapStart wiring. Init()
// registers only the SnapStartCheckpoint handler and binds MasterBusiness:
// no etcd watcher, leader-change callback or cleanup timer.
class TestSnapManagerActor : public SnapManagerActor {
public:
    explicit TestSnapManagerActor(const std::shared_ptr<FakeGlobalSched> &sched)
        : SnapManagerActor(nullptr, sched, SnapManagerConfig{})
    {
    }

    void Init() override
    {
        Receive("SnapStartCheckpoint", &SnapManagerActor::SnapStartCheckpoint);
        business_ = std::make_shared<MasterBusiness>(member_, shared_from_this());
    }

    void Finalize() override {}
};

// Bounded barrier notified from inside a lambda posted to an actor thread.
class ActorBarrier {
public:
    void Notify()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            done_ = true;
        }
        cv_.notify_all();
    }

    bool Wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, kMaxWait, [this]() { return done_; });
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool done_{false};
};

std::string MakeSnapStartWire(const std::string &requestID, const std::string &checkpointID,
                              bool withOptions = false)
{
    messages::RestoreSnapshotRequest req;
    req.set_requestid(requestID);
    req.set_checkpointid(checkpointID);
    if (withOptions) {
        req.mutable_snapstartoptions()->set_type(::common::SNAPSHOT);
    }
    return req.SerializeAsString();
}

SnapshotMetadata MakeSnapshotMetadata(const std::string &checkpointID, const std::string &instanceID)
{
    // createTime is left empty on purpose: production ValidateSnapshot accepts
    // snapshots without a timestamp.
    SnapshotMetadata meta;
    meta.mutable_snapshotinfo()->set_checkpointid(checkpointID);
    meta.mutable_instanceinfo()->set_instanceid(instanceID);
    return meta;
}

}  // namespace

// ----------------------------------------------------------------------------
// Fixture
// ----------------------------------------------------------------------------

class RestoreReplayActorTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        fakeSched_ = std::make_shared<FakeGlobalSched>();
        actor_ = std::make_shared<TestSnapManagerActor>(fakeSched_);
        // Seed snapshot metadata before Spawn: no actor thread exists yet.
        actor_->member_->cache.Put(kCheckpointID, MakeSnapshotMetadata(kCheckpointID, kInstanceID));
        litebus::Spawn(actor_);
        SyncActor(actor_->GetAID());

        reply_ = std::make_shared<SnapStartReplyActor>(
            "SnapStartReply_" + litebus::uuid_generator::UUID::GetRandomUUID().ToString());
        litebus::Spawn(reply_);
        SyncActor(reply_->GetAID());
    }

    void TearDown() override
    {
        if (reply_ != nullptr) {
            litebus::Terminate(reply_->GetAID());
            litebus::Await(reply_->GetAID());
            reply_ = nullptr;
        }
        if (actor_ != nullptr) {
            litebus::Terminate(actor_->GetAID());
            litebus::Await(actor_->GetAID());
            actor_ = nullptr;
        }
    }

    // Re-send a SnapStart request through the real actor messaging path; the
    // requester identity is the reply actor's own AID.
    void SendSnapStart(const std::string &wire)
    {
        auto reply = reply_;
        auto target = actor_->GetAID();
        litebus::Async(reply->GetAID(), [reply, target, wire]() { reply->SendSnapStart(target, wire); });
    }

    // Barrier on the snap manager actor thread (Init is done when it returns).
    void SyncActor(const litebus::AID &aid)
    {
        auto barrier = std::make_shared<ActorBarrier>();
        litebus::Async(aid, [barrier]() { barrier->Notify(); });
        ASSERT_TRUE(barrier->Wait()) << "actor barrier did not settle";
    }

    // Barrier on both actors; used after SendSnapStart so the request was sent
    // (reply actor) and fully processed (snap manager actor).
    void SyncBoth()
    {
        SyncActor(reply_->GetAID());
        SyncActor(actor_->GetAID());
        SyncActor(reply_->GetAID());  // drain responses emitted before the manager barrier
    }

    // Read ledger state on the actor thread; the ledger is never touched from
    // the test thread after Spawn.
    void AssertLedgerSize(size_t expected)
    {
        auto future = litebus::Async(actor_->GetAID(), [this]() -> size_t {
            return actor_->member_->restoreLedger.Size();
        });
        ASSERT_TRUE(future.WaitFor(5000).IsOK()) << "ledger size read did not settle";
        EXPECT_EQ(future.Get(), expected);
    }

    static constexpr const char *kCheckpointID = "ckpt-actor-1";
    static constexpr const char *kInstanceID = "inst-actor-orig-1";

    std::shared_ptr<FakeGlobalSched> fakeSched_{nullptr};
    std::shared_ptr<TestSnapManagerActor> actor_{nullptr};
    std::shared_ptr<SnapStartReplyActor> reply_{nullptr};
};

// ----------------------------------------------------------------------------
// Cases
// ----------------------------------------------------------------------------

// A duplicate send while the schedule is still pending is absorbed: exactly
// one Schedule call and exactly one response once it settles.
TEST_F(RestoreReplayActorTest, PendingDuplicateAbsorbedSchedulesOnce)
{
    const auto wire = MakeSnapStartWire("req-dup", kCheckpointID);
    SendSnapStart(wire);
    ASSERT_TRUE(fakeSched_->WaitForScheduleCalls(1));
    SendSnapStart(wire);
    SyncBoth();
    EXPECT_EQ(fakeSched_->ScheduleCalls(), 1U);

    fakeSched_->CompleteLast(Status::OK());
    ASSERT_TRUE(reply_->WaitForResponses(1));
    SyncBoth();
    EXPECT_EQ(reply_->ResponseCount(), 1U);
    EXPECT_EQ(fakeSched_->ScheduleCalls(), 1U);
}

// After a successful completion the first response is delivered; modeling its
// loss (recorded but the caller re-sends anyway) and removing the snapshot
// metadata, the identical retry replays the EXACT stored wire bytes with no
// second Schedule call.
TEST_F(RestoreReplayActorTest, CompletedSuccessReplaysExactBytesAfterCacheRemoval)
{
    const auto wire = MakeSnapStartWire("req-replay", kCheckpointID);
    SendSnapStart(wire);
    ASSERT_TRUE(fakeSched_->WaitForScheduleCalls(1));
    fakeSched_->CompleteLast(Status::OK());
    ASSERT_TRUE(reply_->WaitForResponses(1));
    auto responses = reply_->Responses();
    ASSERT_EQ(responses.size(), 1U);

    messages::RestoreSnapshotResponse first;
    ASSERT_TRUE(first.ParseFromString(responses[0]));
    EXPECT_EQ(first.requestid(), "req-replay");
    EXPECT_EQ(first.code(), static_cast<int32_t>(common::ERR_NONE));
    EXPECT_EQ(first.instanceid(), kInstanceID);

    // Drop the snapshot metadata as if it expired or was deleted: the retry
    // must still be answered from the ledger, before any cache validation.
    litebus::Async(actor_->GetAID(), [this]() { actor_->member_->cache.Remove(kCheckpointID); });
    SyncActor(actor_->GetAID());

    SendSnapStart(wire);  // identical wire request after modeled response loss
    ASSERT_TRUE(reply_->WaitForResponses(2));
    responses = reply_->Responses();
    ASSERT_EQ(responses.size(), 2U);
    EXPECT_EQ(responses[1], responses[0]);  // byte-identical replay
    SyncBoth();
    EXPECT_EQ(fakeSched_->ScheduleCalls(), 1U);
}

// A failed schedule result is cached as the terminal answer: the identical
// retry replays the exact failure bytes without a second Schedule call.
TEST_F(RestoreReplayActorTest, FailedScheduleResponseReplaysWithoutRedispatch)
{
    const auto wire = MakeSnapStartWire("req-fail", kCheckpointID);
    SendSnapStart(wire);
    ASSERT_TRUE(fakeSched_->WaitForScheduleCalls(1));
    fakeSched_->CompleteLast(Status(StatusCode::FAILED, "injected schedule failure"));
    ASSERT_TRUE(reply_->WaitForResponses(1));

    messages::RestoreSnapshotResponse first;
    ASSERT_TRUE(first.ParseFromString(reply_->Responses()[0]));
    EXPECT_EQ(first.requestid(), "req-fail");
    EXPECT_EQ(first.code(), static_cast<int32_t>(StatusCode::FAILED));
    EXPECT_EQ(first.message(), "injected schedule failure");

    SendSnapStart(wire);
    ASSERT_TRUE(reply_->WaitForResponses(2));
    EXPECT_EQ(reply_->Responses()[1], reply_->Responses()[0]);
    SyncBoth();
    EXPECT_EQ(fakeSched_->ScheduleCalls(), 1U);
}

// Reusing a requestID with a changed checkpoint or changed SnapStartOptions is
// rejected with a generic parameter error and never reaches the scheduler;
// the original request still completes and replays.
TEST_F(RestoreReplayActorTest, ChangedCheckpointOrOptionsConflictRejected)
{
    const auto wireA = MakeSnapStartWire("req-conflict", kCheckpointID);
    SendSnapStart(wireA);
    ASSERT_TRUE(fakeSched_->WaitForScheduleCalls(1));

    // Same requestID, different checkpoint payload.
    SendSnapStart(MakeSnapStartWire("req-conflict", "ckpt-actor-other"));
    ASSERT_TRUE(reply_->WaitForResponses(1));
    // Same requestID and checkpoint, different SnapStartOptions payload.
    SendSnapStart(MakeSnapStartWire("req-conflict", kCheckpointID, true));
    ASSERT_TRUE(reply_->WaitForResponses(2));

    auto responses = reply_->Responses();
    for (const auto &raw : responses) {
        messages::RestoreSnapshotResponse rsp;
        ASSERT_TRUE(rsp.ParseFromString(raw));
        EXPECT_EQ(rsp.requestid(), "req-conflict");
        EXPECT_EQ(rsp.code(), static_cast<int32_t>(common::ERR_PARAM_INVALID));
    }

    fakeSched_->CompleteLast(Status::OK());
    ASSERT_TRUE(reply_->WaitForResponses(3));
    SendSnapStart(wireA);  // original payload still replays
    ASSERT_TRUE(reply_->WaitForResponses(4));
    EXPECT_EQ(reply_->Responses()[3], reply_->Responses()[2]);
    SyncBoth();
    EXPECT_EQ(fakeSched_->ScheduleCalls(), 1U);
}

// A request without a requestID is rejected before any metadata or scheduler
// call and leaves no ledger entry.
TEST_F(RestoreReplayActorTest, EmptyRequestIDRejectedBeforeScheduler)
{
    SendSnapStart(MakeSnapStartWire("", kCheckpointID));
    ASSERT_TRUE(reply_->WaitForResponses(1));

    messages::RestoreSnapshotResponse rsp;
    ASSERT_TRUE(rsp.ParseFromString(reply_->Responses()[0]));
    EXPECT_EQ(rsp.requestid(), "");
    EXPECT_EQ(rsp.code(), static_cast<int32_t>(common::ERR_PARAM_INVALID));

    SyncBoth();
    EXPECT_EQ(fakeSched_->ScheduleCalls(), 0U);
    AssertLedgerSize(0);
}

// Completions whose destination identity, requestID or ticket do not match the
// ledger entry can neither settle it nor emit a response (verified through an
// actor barrier and recorded counts); the subsequent valid completion still
// succeeds and replays.
TEST_F(RestoreReplayActorTest, MismatchedCompletionCannotPoisonLedger)
{
    const auto wire = MakeSnapStartWire("req-poison", kCheckpointID);
    SendSnapStart(wire);
    ASSERT_TRUE(fakeSched_->WaitForScheduleCalls(1));
    SyncBoth();
    AssertLedgerSize(1);

    RestoreReplayLedger::Key key{reply_->GetAID().Name(), reply_->GetAID().Url(), "req-poison"};

    // Wrong ticket with the correct destination and requestID.
    SnapManagerActor::SnapStartLedgerContext stale{};
    stale.key = key;
    stale.ticket = 0xDEADBEEFULL;
    SnapManagerActor::SnapStartResponse wrongTicket{reply_->GetAID(), "req-poison", 0, "poison", {}};
    litebus::Async(actor_->GetAID(),
                   [this, stale, wrongTicket]() { actor_->CompleteSnapStart(stale, wrongTicket); });

    // Correct ticket guess but a destination AID that is not the requester.
    SnapManagerActor::SnapStartLedgerContext stranger{};
    stranger.key = key;
    stranger.ticket = 1;
    SnapManagerActor::SnapStartResponse wrongDestination{litebus::AID("Stranger", "tcp://127.0.0.1:1"),
                                                         "req-poison", 0, "poison", {}};
    litebus::Async(actor_->GetAID(),
                   [this, stranger, wrongDestination]() { actor_->CompleteSnapStart(stranger, wrongDestination); });

    // Correct destination but a response minted for another requestID.
    SnapManagerActor::SnapStartLedgerContext otherRequest{};
    otherRequest.key = key;
    otherRequest.ticket = 1;
    SnapManagerActor::SnapStartResponse wrongRequestID{reply_->GetAID(), "req-other", 0, "poison", {}};
    litebus::Async(actor_->GetAID(),
                   [this, otherRequest, wrongRequestID]() { actor_->CompleteSnapStart(otherRequest, wrongRequestID); });

    SyncActor(actor_->GetAID());
    SyncActor(reply_->GetAID());
    EXPECT_EQ(reply_->ResponseCount(), 0U);  // barriered, recorded: nothing sent
    AssertLedgerSize(1);

    // The real completion (posted by the schedule callback) still succeeds.
    fakeSched_->CompleteLast(Status::OK());
    ASSERT_TRUE(reply_->WaitForResponses(1));
    messages::RestoreSnapshotResponse first;
    ASSERT_TRUE(first.ParseFromString(reply_->Responses()[0]));
    EXPECT_EQ(first.instanceid(), kInstanceID);

    SendSnapStart(wire);
    ASSERT_TRUE(reply_->WaitForResponses(2));
    EXPECT_EQ(reply_->Responses()[1], reply_->Responses()[0]);
    SyncBoth();
    EXPECT_EQ(fakeSched_->ScheduleCalls(), 1U);
}

// A terminal response larger than the ledger bound is neither stored nor sent
// (actor barrier + recorded counts, not timed absence); the entry stays
// absorbing, so the identical retry causes no second dispatch and no response.
TEST_F(RestoreReplayActorTest, OversizedResponseNotSentEntryStaysAbsorbing)
{
    const auto wire = MakeSnapStartWire("req-oversize", kCheckpointID);
    SendSnapStart(wire);
    ASSERT_TRUE(fakeSched_->WaitForScheduleCalls(1));

    // The schedule status message becomes the response message: >64KiB wire
    // response against the production default 64KiB ledger message bound.
    fakeSched_->CompleteLast(Status(StatusCode::FAILED, std::string(70 * 1024, 'e')));
    SyncActor(actor_->GetAID());
    SyncActor(reply_->GetAID());
    EXPECT_EQ(reply_->ResponseCount(), 0U);
    AssertLedgerSize(1);

    SendSnapStart(wire);
    SyncBoth();
    EXPECT_EQ(reply_->ResponseCount(), 0U);  // absorbed, not answered
    EXPECT_EQ(fakeSched_->ScheduleCalls(), 1U);
}

}  // namespace functionsystem::test
