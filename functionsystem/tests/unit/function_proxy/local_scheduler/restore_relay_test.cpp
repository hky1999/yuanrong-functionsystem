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

// Actor-level coverage for LocalSchedSrvActor's bounded SnapStart (restore)
// relay: real actor threads, real litebus actor-message round trips and the
// actor's real operation map. No etcd/domain registration and no global
// deployment: a test subclass enables only the SnapStartCheckpointResponse
// receiver, and a locally spawned stub plays the pinned SnapManager
// destination, replying only when the test decides an operation completes.
// Adversarial senders/IDs/bodies are fed through the real response receiver
// invoked on the actor thread. Every wait is bounded (condition variables and
// actor queue barriers); there are no sleeps.
//
#include "local_scheduler/local_scheduler_service/local_sched_srv_actor.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <stdexcept>
#include <vector>

#include "async/async.hpp"
#include "common/constants/actor_name.h"
#include "common/explorer/explorer.h"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix_pb.h"
#include "litebus.hpp"

namespace functionsystem::test {

using namespace functionsystem::local_scheduler;

namespace {

constexpr uint64_t kMaxWaitMs = 5000;   // every wait is bounded
constexpr uint64_t kPendingCheckMs = 200;  // bounded "still unsettled" probe

// Bounded barrier notified from a lambda posted onto an actor thread; proves
// posted work ran instead of testing for the absence of effects with sleeps.
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
        return cv_.wait_for(lock, std::chrono::milliseconds(kMaxWaitMs), [this]() { return done_; });
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool done_{false};
};

// The pinned SnapManager destination: records the exact wire and requester of
// every SnapStartCheckpoint it receives, and replies only when a test
// explicitly resolves one recorded request (in flight means no reply, exactly
// like a master whose restore is still executing).
class SnapManagerStubActor : public litebus::ActorBase {
public:
    struct Received {
        litebus::AID from{};
        std::string wire{};
    };

    explicit SnapManagerStubActor(const std::string &name) : ActorBase(name)
    {
    }

    void Init() override
    {
        Receive("SnapStartCheckpoint", &SnapManagerStubActor::OnSnapStart);
    }

    void OnSnapStart(const litebus::AID &from, std::string &&, std::string &&msg)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        received_.push_back(Received{from, std::move(msg)});
        cv_.notify_all();
    }

    bool WaitForRequests(size_t count)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::milliseconds(kMaxWaitMs),
                            [this, count]() { return received_.size() >= count; });
    }

    size_t RequestCount()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return received_.size();
    }

    std::vector<std::string> Wires()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> wires;
        wires.reserve(received_.size());
        for (const auto &rec : received_) {
            wires.push_back(rec.wire);
        }
        return wires;
    }

    // Reply to the recorded request whose wire carries requestID, echoing that
    // requestID back (what the real master does). Must be invoked on this
    // actor's thread via litebus::Async so the response travels the real
    // actor-message path.
    void ReplyEcho(const std::string &requestID, int32_t code, const std::string &message,
                   const std::string &instanceID)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = received_.rbegin(); it != received_.rend(); ++it) {
            ::messages::RestoreSnapshotRequest req;
            if (!req.ParseFromString(it->wire) || req.requestid() != requestID) {
                continue;
            }
            ::messages::RestoreSnapshotResponse rsp;
            rsp.set_requestid(requestID);
            rsp.set_code(code);
            rsp.set_message(message);
            if (!instanceID.empty()) {
                rsp.set_instanceid(instanceID);
            }
            Send(it->from, "SnapStartCheckpointResponse", rsp.SerializeAsString());
            return;
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<Received> received_;
};

// Real LocalSchedSrvActor with only the restore-relay receiver enabled: no
// explorer leader callback, no registration timers, no other handlers.
class RestoreRelaySrvActor : public LocalSchedSrvActor {
public:
    RestoreRelaySrvActor() : LocalSchedSrvActor(LocalSchedSrvActor::Param{.nodeID = "restore-relay-test"})
    {
    }

    void Init() override
    {
        Receive("SnapStartCheckpointResponse", &LocalSchedSrvActor::OnSnapStartCheckpointResponse);
    }

    void Finalize() override
    {
    }
};

std::string MakeResponseWire(const std::string &requestID, int32_t code, const std::string &instanceID,
                             const std::string &message = "")
{
    ::messages::RestoreSnapshotResponse rsp;
    rsp.set_requestid(requestID);
    rsp.set_code(code);
    rsp.set_instanceid(instanceID);
    rsp.set_message(message);
    return rsp.SerializeAsString();
}

}  // namespace

class LocalSchedRestoreRelayTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        dstActor_ = std::make_shared<RestoreRelaySrvActor>();
        litebus::Spawn(dstActor_);
        SyncActor(dstActor_->GetAID());

        snapMgr_ = std::make_shared<SnapManagerStubActor>(SNAP_MANAGER_ACTOR_NAME);
        litebus::Spawn(snapMgr_);
        SyncActor(snapMgr_->GetAID());
    }

    void TearDown() override
    {
        litebus::Terminate(dstActor_->GetAID());
        litebus::Await(dstActor_->GetAID());
        litebus::Terminate(snapMgr_->GetAID());
        litebus::Await(snapMgr_->GetAID());
        dstActor_ = nullptr;
        snapMgr_ = nullptr;
    }

    // Points the relay's master registration at the stub actor's address; the
    // relay then pins SnapManagerActor at exactly this url for new operations.
    void PinMasterToStub()
    {
        explorer::LeaderInfo leader{.name = snapMgr_->GetAID().Name(), .address = snapMgr_->GetAID().Url()};
        litebus::Async(dstActor_->GetAID(), &LocalSchedSrvActor::UpdateMasterInfo, leader);
        SyncActor(dstActor_->GetAID());
    }

    // Breaks the master registration through the real update path.
    void LoseMaster()
    {
        explorer::LeaderInfo leader{.name = "gone", .address = ""};
        litebus::Async(dstActor_->GetAID(), &LocalSchedSrvActor::UpdateMasterInfo, leader);
        SyncActor(dstActor_->GetAID());
    }

    // Barrier on an actor thread: everything posted before it has run.
    void SyncActor(const litebus::AID &aid)
    {
        auto barrier = std::make_shared<ActorBarrier>();
        litebus::Async(aid, [barrier]() { barrier->Notify(); });
        if (!barrier->Wait()) {
            throw std::runtime_error("actor barrier did not settle");
        }
        // Drain the receiver too before asserting that a relay sent nothing.
        if (snapMgr_ != nullptr && dstActor_ != nullptr && aid == dstActor_->GetAID()) {
            SyncActor(snapMgr_->GetAID());
        }
    }

    // The production invocation path for the relay entry point (this is how
    // LocalSchedSrv reaches the actor).
    litebus::Future<::messages::RestoreSnapshotResponse> Begin(const std::string &requestID,
                                                               const std::string &checkpointID)
    {
        auto req = std::make_shared<::messages::RestoreSnapshotRequest>();
        req->set_requestid(requestID);
        req->set_checkpointid(checkpointID);
        return litebus::Async(dstActor_->GetAID(), &LocalSchedSrvActor::SnapStartCheckpoint, req);
    }

    // Reads relay state on the actor thread; the map is never touched from
    // the test thread while the actor is spawned.
    size_t RelaySize()
    {
        auto future = litebus::Async(dstActor_->GetAID(), [this]() -> size_t {
            return dstActor_->snapStartRelayOps_.size();
        });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("relay state query did not settle");
        }
        return future.Get();
    }

    bool RelayCompleted(const std::string &callerRequestID)
    {
        auto future = litebus::Async(dstActor_->GetAID(), [this, callerRequestID]() -> bool {
            return dstActor_->snapStartRelayOps_.at(callerRequestID)->completed;
        });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("relay state query did not settle");
        }
        return future.Get();
    }

    std::string TransportIDOf(const std::string &callerRequestID)
    {
        auto future = litebus::Async(dstActor_->GetAID(), [this, callerRequestID]() -> std::string {
            return dstActor_->snapStartRelayOps_.at(callerRequestID)->transportRequestID;
        });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("relay state query did not settle");
        }
        return future.Get();
    }

    // Fires the retry path for one operation exactly as the 30s retry timer
    // would, without waiting for the timer.
    void FireRetry(const std::string &callerRequestID, const std::string &transportRequestID)
    {
        litebus::Async(dstActor_->GetAID(), &LocalSchedSrvActor::DispatchSnapStartCheckpoint, callerRequestID,
                       transportRequestID);
        SyncActor(dstActor_->GetAID());
    }

    // Resolves one recorded request from the stub through the real reply path.
    void ReplyFromMaster(const std::string &transportRequestID, int32_t code, const std::string &instanceID)
    {
        litebus::Async(snapMgr_->GetAID(), [this, transportRequestID, code, instanceID]() {
            snapMgr_->ReplyEcho(transportRequestID, code, "test reply", instanceID);
        });
        SyncActor(snapMgr_->GetAID());
    }

    // Delivers a response to the real receiver with a fabricated sender,
    // covering sender/ID/message validation without a second process. The
    // receiver's parameters ARE its trust boundary.
    void DeliverResponse(const litebus::AID &from, const std::string &wire)
    {
        litebus::Async(dstActor_->GetAID(), [this, from, wire]() {
            dstActor_->OnSnapStartCheckpointResponse(from, std::string("SnapStartCheckpointResponse"),
                                                     std::string(wire));
        });
        SyncActor(dstActor_->GetAID());
    }

    std::shared_ptr<RestoreRelaySrvActor> dstActor_{nullptr};
    std::shared_ptr<SnapManagerStubActor> snapMgr_{nullptr};
};

// After Begin, mutating the caller's request changes nothing that is stored
// or re-sent, and a master topology change cannot rebind the pinned
// destination: the retry delivers byte-identical wire to the original
// SnapManager address, and no outcome is synthesized while it is pending.
TEST_F(LocalSchedRestoreRelayTest, RetrySendsExactStoredWireToPinnedDestination)
{
    PinMasterToStub();

    auto req = std::make_shared<::messages::RestoreSnapshotRequest>();
    req->set_requestid("caller-1");
    req->set_checkpointid("ckpt-1");
    auto future = litebus::Async(dstActor_->GetAID(), &LocalSchedSrvActor::SnapStartCheckpoint, req);
    ASSERT_TRUE(snapMgr_->WaitForRequests(1));

    // Caller mutates its request after Begin.
    req->set_checkpointid("ckpt-MUTATED");

    // Master registration moves elsewhere.
    explorer::LeaderInfo leader{.name = "other", .address = "127.0.0.1:1"};
    litebus::Async(dstActor_->GetAID(), &LocalSchedSrvActor::UpdateMasterInfo, leader);
    SyncActor(dstActor_->GetAID());

    const auto transportID = TransportIDOf("caller-1");
    ASSERT_FALSE(transportID.empty());
    EXPECT_NE(transportID, "caller-1");

    FireRetry("caller-1", transportID);  // exactly what the retry timer does
    ASSERT_TRUE(snapMgr_->WaitForRequests(2));

    auto wires = snapMgr_->Wires();
    ASSERT_EQ(wires.size(), 2U);
    EXPECT_EQ(wires[1], wires[0]);  // byte-identical: the exact stored wire

    ::messages::RestoreSnapshotRequest sent;
    ASSERT_TRUE(sent.ParseFromString(wires[0]));
    EXPECT_EQ(sent.requestid(), transportID);  // private id on the wire
    EXPECT_NE(sent.requestid(), "caller-1");   // the caller's identifier never travels
    EXPECT_EQ(sent.checkpointid(), "ckpt-1");  // payload from Begin time, not the mutation

    // Still pending: neither the mutation, the topology change nor the retry
    // synthesized an outcome.
    EXPECT_FALSE(future.WaitFor(kPendingCheckMs).IsOK());
    EXPECT_FALSE(RelayCompleted("caller-1"));
}

// Identical retries share the retained promise future while in flight, the
// first response (with the caller's requestID restored) is immutable, and a
// retry after completion replays the first result without a new send.
TEST_F(LocalSchedRestoreRelayTest, IdenticalRetriesShareRetainedPromiseAndFirstResult)
{
    PinMasterToStub();

    auto future1 = Begin("caller-2", "ckpt-2");
    ASSERT_TRUE(snapMgr_->WaitForRequests(1));

    // In-flight identical retry: same retained future, no new dispatch.
    auto future2 = Begin("caller-2", "ckpt-2");
    SyncActor(dstActor_->GetAID());
    EXPECT_EQ(snapMgr_->RequestCount(), 1U);

    const auto transportID = TransportIDOf("caller-2");
    ReplyFromMaster(transportID, common::ERR_NONE, "inst-2");

    ASSERT_TRUE(future1.WaitFor(kMaxWaitMs).IsOK());
    ASSERT_TRUE(future2.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future1.Get().code(), static_cast<int32_t>(common::ERR_NONE));
    EXPECT_EQ(future1.Get().requestid(), "caller-2");  // private id rewritten back
    EXPECT_EQ(future1.Get().instanceid(), "inst-2");
    EXPECT_EQ(future2.Get().instanceid(), "inst-2");  // both futures share one promise

    // A late duplicate reply must not change the retained first response.
    ReplyFromMaster(transportID, common::ERR_INNER_SYSTEM_ERROR, "inst-bad");
    SyncActor(dstActor_->GetAID());
    EXPECT_EQ(future1.Get().code(), static_cast<int32_t>(common::ERR_NONE));
    EXPECT_EQ(future1.Get().instanceid(), "inst-2");

    // Completed identical retry replays the first result, dispatching nothing.
    auto future3 = Begin("caller-2", "ckpt-2");
    SyncActor(dstActor_->GetAID());
    EXPECT_EQ(snapMgr_->RequestCount(), 1U);
    ASSERT_TRUE(future3.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future3.Get().instanceid(), "inst-2");
    EXPECT_EQ(future3.Get().code(), static_cast<int32_t>(common::ERR_NONE));
}

// A different payload under the same requestID is a generic parameter error
// that neither dispatches nor disturbs the original operation.
TEST_F(LocalSchedRestoreRelayTest, ChangedPayloadConflictsWithoutTouchingOriginal)
{
    PinMasterToStub();

    auto future1 = Begin("caller-3", "ckpt-3");
    ASSERT_TRUE(snapMgr_->WaitForRequests(1));

    auto conflict = Begin("caller-3", "ckpt-OTHER");
    ASSERT_TRUE(conflict.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(conflict.Get().code(), static_cast<int32_t>(common::ERR_PARAM_INVALID));
    EXPECT_EQ(conflict.Get().requestid(), "caller-3");
    EXPECT_EQ(snapMgr_->RequestCount(), 1U);

    // The original entry is untouched: retry and completion still work.
    const auto transportID = TransportIDOf("caller-3");
    FireRetry("caller-3", transportID);
    ASSERT_TRUE(snapMgr_->WaitForRequests(2));
    auto wires = snapMgr_->Wires();
    ::messages::RestoreSnapshotRequest parsed;
    ASSERT_TRUE(parsed.ParseFromString(wires[1]));
    EXPECT_EQ(parsed.checkpointid(), "ckpt-3");

    ReplyFromMaster(transportID, common::ERR_NONE, "inst-3");
    ASSERT_TRUE(future1.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future1.Get().instanceid(), "inst-3");
}

// Only the exact pinned sender AND the entry's private transport ID can
// settle a promise; wrong name, wrong url, unknown ID, malformed and
// oversized bodies are all ignored, and the matching response still settles
// afterwards.
TEST_F(LocalSchedRestoreRelayTest, OnlyPinnedSenderAndTransportIDCanSettle)
{
    PinMasterToStub();

    auto future = Begin("caller-4", "ckpt-4");
    ASSERT_TRUE(snapMgr_->WaitForRequests(1));
    const auto transportID = TransportIDOf("caller-4");

    const auto pinnedSender = litebus::AID(SNAP_MANAGER_ACTOR_NAME, snapMgr_->GetAID().Url());
    const auto wire = MakeResponseWire(transportID, common::ERR_NONE, "inst-poison");

    DeliverResponse(litebus::AID("NotTheSnapManager", snapMgr_->GetAID().Url()), wire);  // wrong name
    DeliverResponse(litebus::AID(SNAP_MANAGER_ACTOR_NAME, "127.0.0.1:2"), wire);           // wrong url
    DeliverResponse(pinnedSender, MakeResponseWire("no-such-transport-id", common::ERR_NONE, "inst-x"));
    DeliverResponse(pinnedSender, "");  // empty body
    DeliverResponse(pinnedSender, "not-a-proto");  // malformed body
    DeliverResponse(pinnedSender,
                    MakeResponseWire(transportID, common::ERR_NONE, "inst-poison",
                                     std::string(70 * 1024, 'e')));  // oversized body

    EXPECT_FALSE(future.WaitFor(kPendingCheckMs).IsOK());
    EXPECT_FALSE(RelayCompleted("caller-4"));

    // The pinned sender with the matching transport ID still settles it, and
    // the API response carries the caller's identifier again.
    ReplyFromMaster(transportID, common::ERR_NONE, "inst-4");
    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future.Get().requestid(), "caller-4");
    EXPECT_EQ(future.Get().instanceid(), "inst-4");
}

// Begin rejects null requests, empty/oversized requestIDs and oversized
// serialized bodies before creating state or dispatching anything.
TEST_F(LocalSchedRestoreRelayTest, BeginValidationRejectsBadInputWithoutSideEffects)
{
    PinMasterToStub();

    std::shared_ptr<::messages::RestoreSnapshotRequest> nullReq = nullptr;
    auto nullFuture = litebus::Async(dstActor_->GetAID(), &LocalSchedSrvActor::SnapStartCheckpoint, nullReq);
    ASSERT_TRUE(nullFuture.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(nullFuture.Get().code(), static_cast<int32_t>(common::ERR_PARAM_INVALID));

    auto emptyID = Begin("", "ckpt");
    ASSERT_TRUE(emptyID.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(emptyID.Get().code(), static_cast<int32_t>(common::ERR_PARAM_INVALID));

    auto longID = Begin(std::string(SNAP_START_RELAY_MAX_ID_BYTES + 1, 'r'), "ckpt");
    ASSERT_TRUE(longID.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(longID.Get().code(), static_cast<int32_t>(common::ERR_PARAM_INVALID));

    auto hugeBody = Begin("caller-big", std::string(SNAP_START_RELAY_MAX_MESSAGE_BYTES, 'c'));
    ASSERT_TRUE(hugeBody.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(hugeBody.Get().code(), static_cast<int32_t>(common::ERR_PARAM_INVALID));

    SyncActor(dstActor_->GetAID());
    EXPECT_EQ(snapMgr_->RequestCount(), 0U);  // nothing dispatched
    EXPECT_EQ(RelaySize(), 0U);               // nothing retained
}

// A new operation needs a usable master registration, but retries of existing
// operations do not: they keep sharing the retained promise (and replaying
// its result) across master registration changes.
TEST_F(LocalSchedRestoreRelayTest, NewOperationNeedsMasterButRetriesDoNot)
{
    // No master registration yet: fail closed, dispatch nothing.
    auto refused = Begin("caller-6", "ckpt-6");
    ASSERT_TRUE(refused.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(refused.Get().code(), static_cast<int32_t>(common::ERR_INNER_COMMUNICATION));
    EXPECT_EQ(snapMgr_->RequestCount(), 0U);

    PinMasterToStub();
    auto future = Begin("caller-6", "ckpt-6");
    ASSERT_TRUE(snapMgr_->WaitForRequests(1));

    // Registration disappears again: an in-flight identical retry still shares
    // the retained promise instead of failing or redispatching.
    LoseMaster();
    auto retry = Begin("caller-6", "ckpt-6");
    SyncActor(dstActor_->GetAID());
    EXPECT_EQ(snapMgr_->RequestCount(), 1U);

    const auto transportID = TransportIDOf("caller-6");
    ReplyFromMaster(transportID, common::ERR_NONE, "inst-6");
    ASSERT_TRUE(retry.WaitFor(kMaxWaitMs).IsOK());
    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(retry.Get().instanceid(), "inst-6");

    // A completed retry also survives the missing master and replays.
    auto replay = Begin("caller-6", "ckpt-6");
    ASSERT_TRUE(replay.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(replay.Get().instanceid(), "inst-6");
    EXPECT_EQ(snapMgr_->RequestCount(), 1U);
}

// Private transport IDs isolate operations that share a checkpoint: each wire
// carries its own opaque id and each reply settles only its own promise.
TEST_F(LocalSchedRestoreRelayTest, TransportIDsIsolateOperations)
{
    PinMasterToStub();

    auto futureA = Begin("caller-a", "shared-ckpt");
    auto futureB = Begin("caller-b", "shared-ckpt");
    ASSERT_TRUE(snapMgr_->WaitForRequests(2));

    const auto idA = TransportIDOf("caller-a");
    const auto idB = TransportIDOf("caller-b");
    EXPECT_FALSE(idA.empty());
    EXPECT_FALSE(idB.empty());
    EXPECT_NE(idA, idB);

    auto wires = snapMgr_->Wires();
    ::messages::RestoreSnapshotRequest parsedA;
    ::messages::RestoreSnapshotRequest parsedB;
    ASSERT_TRUE(parsedA.ParseFromString(wires[0]));
    ASSERT_TRUE(parsedB.ParseFromString(wires[1]));
    EXPECT_EQ(parsedA.requestid(), idA);
    EXPECT_EQ(parsedB.requestid(), idB);
    EXPECT_NE(parsedA.requestid(), "caller-a");
    EXPECT_NE(parsedB.requestid(), "caller-b");

    ReplyFromMaster(idA, common::ERR_NONE, "inst-A");
    ASSERT_TRUE(futureA.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(futureA.Get().requestid(), "caller-a");
    EXPECT_EQ(futureA.Get().instanceid(), "inst-A");
    EXPECT_FALSE(futureB.WaitFor(kPendingCheckMs).IsOK());  // B untouched by A's reply

    ReplyFromMaster(idB, common::ERR_NONE, "inst-B");
    ASSERT_TRUE(futureB.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(futureB.Get().requestid(), "caller-b");
    EXPECT_EQ(futureB.Get().instanceid(), "inst-B");
}

// A retry firing after completion (or keyed to a stale/unknown operation) is
// a no-op: no re-send, no new logical restore.
TEST_F(LocalSchedRestoreRelayTest, LateOrStaleRetryAfterCompletionIsNoOp)
{
    PinMasterToStub();

    auto future = Begin("caller-8", "ckpt-8");
    ASSERT_TRUE(snapMgr_->WaitForRequests(1));
    const auto transportID = TransportIDOf("caller-8");
    ReplyFromMaster(transportID, common::ERR_NONE, "inst-8");
    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());

    FireRetry("caller-8", transportID);        // completed entry
    FireRetry("caller-8", "stale-transport");  // superseded transport id
    FireRetry("unknown-caller", transportID);  // no such entry
    EXPECT_EQ(snapMgr_->RequestCount(), 1U);
    EXPECT_EQ(future.Get().instanceid(), "inst-8");
}

// The relay's capacity bound refuses NEW work without evicting anything and
// without dispatching the refused request, while an existing identical
// request still returns its retained (still settle-able) future. Per-entry
// reservation is dominated by the 64KiB response allowance, so the total
// serialized-payload bound trips well before the 4096 entry bound.
TEST_F(LocalSchedRestoreRelayTest, CapacityBoundRefusesNewWorkWithoutEvicting)
{
    PinMasterToStub();

    struct FillResult {
        size_t accepted{0};
        int32_t refusedCode{-1};
        std::string refusedID{};
    };

    // Fill through the real Begin path on the actor thread until a new entry
    // is refused; a pending return means accepted, a settled one means refused.
    auto fill = litebus::Async(dstActor_->GetAID(), [this]() -> FillResult {
        FillResult result;
        for (size_t i = 0; i < SNAP_START_RELAY_MAX_ENTRIES + 16; i++) {
            auto req = std::make_shared<::messages::RestoreSnapshotRequest>();
            req->set_requestid("fill-" + std::to_string(i));
            req->set_checkpointid("ckpt");
            auto future = dstActor_->SnapStartCheckpoint(req);
            if (future.IsInit()) {
                result.accepted++;
                continue;
            }
            result.refusedCode = future.Get().code();
            result.refusedID = future.Get().requestid();
            break;
        }
        return result;
    });
    ASSERT_TRUE(fill.WaitFor(kMaxWaitMs).IsOK()) << "relay fill did not settle";
    const auto result = fill.Get();

    ASSERT_GE(result.accepted, 2U);
    ASSERT_LT(result.accepted, SNAP_START_RELAY_MAX_ENTRIES + 16U);  // the bound actually refused
    EXPECT_EQ(result.refusedCode, static_cast<int32_t>(common::ERR_INSTANCE_BUSY));

    // Every accepted begin dispatched exactly once; the refused one never.
    ASSERT_TRUE(snapMgr_->WaitForRequests(result.accepted));
    EXPECT_EQ(snapMgr_->RequestCount(), result.accepted);
    EXPECT_EQ(RelaySize(), result.accepted);  // nothing was evicted

    // While full, an identical retry of an existing entry still returns the
    // retained future and settles through the pinned sender.
    auto retained = Begin("fill-0", "ckpt");
    SyncActor(dstActor_->GetAID());
    EXPECT_EQ(snapMgr_->RequestCount(), result.accepted);  // no new dispatch

    const auto transportID = TransportIDOf("fill-0");
    ReplyFromMaster(transportID, common::ERR_NONE, "inst-fill");
    ASSERT_TRUE(retained.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(retained.Get().instanceid(), "inst-fill");
}

// An incoming response within the wire bound can exceed it after restoring a
// longer caller ID. Such a response must not consume more than the reservation.
TEST_F(LocalSchedRestoreRelayTest, RewrittenResponseMustFitReservation)
{
    PinMasterToStub();
    const std::string callerID(SNAP_START_RELAY_MAX_ID_BYTES, 'r');
    auto future = Begin(callerID, "checkpoint");
    ASSERT_TRUE(snapMgr_->WaitForRequests(1));
    const auto transportID = TransportIDOf(callerID);
    const auto wire = MakeResponseWire(transportID, common::ERR_NONE, "instance",
                                       std::string(SNAP_START_RELAY_MAX_MESSAGE_BYTES - 100, 'x'));
    ASSERT_LE(wire.size(), SNAP_START_RELAY_MAX_MESSAGE_BYTES);
    ::messages::RestoreSnapshotResponse rewritten;
    ASSERT_TRUE(rewritten.ParseFromString(wire));
    rewritten.set_requestid(callerID);
    ASSERT_GT(rewritten.ByteSizeLong(), SNAP_START_RELAY_MAX_MESSAGE_BYTES);
    DeliverResponse(litebus::AID(SNAP_MANAGER_ACTOR_NAME, snapMgr_->GetAID().Url()), wire);
    EXPECT_TRUE(future.IsInit());
    EXPECT_FALSE(RelayCompleted(callerID));
    ReplyFromMaster(transportID, common::ERR_NONE, "small-result");
    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future.Get().instanceid(), "small-result");
}

}  // namespace functionsystem::test
