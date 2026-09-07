// Actor-level coverage for SnapCtrlActor's direct-restore logical context:
// one retained context per caller requestID with a private nonce, captured
// registrations and a retained promise. Real spawned SnapCtrl actor, real
// litebus round trips, and a controlled LocalSchedSrv subclass overriding
// SnapStartCheckpoint (null actor: no external services, no relay). Every
// wait is bounded (condition variables and actor queue barriers); there are
// no sleeps, no physical restore, no LLM and no deployment. Process-local
// only: authoritative physical reconciliation and restart recovery remain
// open and are not claimed here.
#include "local_scheduler/snap_ctrl/snap_ctrl_actor.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix_pb.h"
#include "litebus.hpp"

namespace functionsystem::test {

using namespace functionsystem::local_scheduler;

namespace {

constexpr uint64_t kMaxWaitMs = 5000;     // every wait is bounded
constexpr uint64_t kPendingCheckMs = 200; // bounded "still unsettled" probe
constexpr uint64_t kFillWaitMs = 10000;   // bounded capacity fill

common::ErrorCode BusyCode()
{
    return static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY));
}

common::ErrorCode ParamInvalidCode()
{
    return static_cast<common::ErrorCode>(static_cast<int32_t>(StatusCode::ERR_PARAM_INVALID));
}

// Bounded barrier notified from a lambda posted onto the actor thread.
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

// Controlled relay hop: records every RestoreSnapshotRequest and hands the
// actor a pending promise per dispatch; the test decides when and how an
// operation settles. The actor passed to the base is null, so no other
// LocalSchedSrv entry point can reach an external service.
class ControlledSchedSrv : public LocalSchedSrv {
public:
    ControlledSchedSrv() : LocalSchedSrv(std::shared_ptr<LocalSchedSrvActor>()) {}

    litebus::Future<messages::RestoreSnapshotResponse> SnapStartCheckpoint(
        const std::shared_ptr<messages::RestoreSnapshotRequest> &req) override
    {
        std::shared_ptr<litebus::Promise<messages::RestoreSnapshotResponse>> promise =
            std::make_shared<litebus::Promise<messages::RestoreSnapshotResponse>>();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            requests_.push_back(req);
            ops_.push_back(promise);
        }
        cv_.notify_all();
        return promise->GetFuture();
    }

    bool WaitForDispatch(size_t count)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::milliseconds(kMaxWaitMs),
                            [this, count]() { return requests_.size() >= count; });
    }

    size_t DispatchCount()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return requests_.size();
    }

    std::string DispatchedRequestID(size_t index)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return requests_.at(index)->requestid();
    }

    void SettleValue(size_t index, const messages::RestoreSnapshotResponse &rsp)
    {
        std::shared_ptr<litebus::Promise<messages::RestoreSnapshotResponse>> promise;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            promise = ops_.at(index);
        }
        promise->SetValue(rsp);
    }

    void SettleFailed(size_t index)
    {
        std::shared_ptr<litebus::Promise<messages::RestoreSnapshotResponse>> promise;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            promise = ops_.at(index);
        }
        promise->SetFailed(litebus::Status::KERROR);
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::shared_ptr<messages::RestoreSnapshotRequest>> requests_;
    std::vector<std::shared_ptr<litebus::Promise<messages::RestoreSnapshotResponse>>> ops_;
};

// Minimal spawned SnapCtrl: Init/Finalize are neutralized so the global
// parked-registry wake provider is never installed by a test.
class TestSnapCtrlActor : public SnapCtrlActor {
public:
    TestSnapCtrlActor() : SnapCtrlActor("direct-restore-ctx-test", "node") {}

    void Init() override {}
    void Finalize() override {}
};

}  // namespace

class DirectRestoreContextTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        srv_ = std::make_shared<ControlledSchedSrv>();
        actor_ = std::make_shared<TestSnapCtrlActor>();
        actor_->BindLocalSchedSrv(srv_);
        litebus::Spawn(actor_);
        SyncActor();
    }

    void TearDown() override
    {
        litebus::Terminate(actor_->GetAID());
        litebus::Await(actor_->GetAID());
        actor_ = nullptr;
        srv_ = nullptr;
    }

    // Barrier on the actor thread: everything posted before it has run.
    void SyncActor()
    {
        auto barrier = std::make_shared<ActorBarrier>();
        litebus::Async(actor_->GetAID(), [barrier]() { barrier->Notify(); });
        if (!barrier->Wait()) {
            throw std::runtime_error("actor barrier did not settle");
        }
    }

    // Parks one instance through the real completion path; returns its
    // registration, empty on failure.
    std::string Park(const std::string &instanceID, const std::string &checkpointID)
    {
        auto future = litebus::Async(actor_->GetAID(), [this, instanceID, checkpointID]() -> std::string {
            SnapshotResult snapshot;
            snapshot.code = common::ERR_NONE;
            snapshot.snapshotInfo.set_checkpointid(checkpointID);
            snapshot.snapshotInfo.set_sandbox_id("source-sandbox");
            snapshot.snapshotInfo.set_source_runtime_id("source-runtime");
            snapshot.snapshotInfo.set_source_generation("source-generation");
            if (actor_->OnHandleSnapshot(instanceID, false, snapshot, "hold-token").code() != common::ERR_NONE) {
                return std::string();
            }
            return actor_->parkedInstances_.at(instanceID).registrationID;
        });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("park did not settle");
        }
        return future.Get();
    }

    // The production entry point, executed on the actor thread.
    litebus::Future<KillResponse> Begin(const std::string &requestID, const std::string &checkpointID,
                                        const std::string &payload = "")
    {
        return litebus::Async(actor_->GetAID(), [this, requestID, checkpointID, payload]() {
            return actor_->HandleSnapStart(requestID, checkpointID, payload);
        });
    }

    struct ParkedSnapshot {
        bool exists{false};
        bool waking{false};
        bool wakeUnknown{false};
        uint32_t wakeFails{0};
        uint64_t wakeAttempt{0};
        std::string checkpointID{};
        std::string registrationID{};
        std::string holdToken{};
    };

    ParkedSnapshot Parked(const std::string &instanceID)
    {
        auto future = litebus::Async(actor_->GetAID(), [this, instanceID]() -> ParkedSnapshot {
            ParkedSnapshot snapshot;
            const auto it = actor_->parkedInstances_.find(instanceID);
            if (it == actor_->parkedInstances_.end()) {
                return snapshot;
            }
            snapshot.exists = true;
            snapshot.waking = it->second.waking;
            snapshot.wakeUnknown = it->second.wakeUnknown;
            snapshot.wakeFails = it->second.wakeFails;
            snapshot.wakeAttempt = it->second.wakeAttempt;
            snapshot.checkpointID = it->second.checkpointID;
            snapshot.registrationID = it->second.registrationID;
            snapshot.holdToken = it->second.holdToken;
            return snapshot;
        });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("parked query did not settle");
        }
        return future.Get();
    }

    struct ContextSnapshot {
        bool exists{false};
        bool completed{false};
        std::string checkpointID{};
        std::string payload{};
        size_t captured{0};
    };

    ContextSnapshot Context(const std::string &requestID)
    {
        auto future = litebus::Async(actor_->GetAID(), [this, requestID]() -> ContextSnapshot {
            ContextSnapshot snapshot;
            const auto it = actor_->restoreContexts_.find(requestID);
            if (it == actor_->restoreContexts_.end()) {
                return snapshot;
            }
            snapshot.exists = true;
            snapshot.completed = it->second->completed;
            snapshot.checkpointID = it->second->checkpointID;
            snapshot.payload = it->second->payload;
            snapshot.captured = it->second->captured.size();
            return snapshot;
        });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("context query did not settle");
        }
        return future.Get();
    }

    std::string ContextNonce(const std::string &requestID)
    {
        auto future = litebus::Async(actor_->GetAID(), [this, requestID]() -> std::string {
            const auto it = actor_->restoreContexts_.find(requestID);
            return it == actor_->restoreContexts_.end() ? std::string() : it->second->nonce;
        });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("nonce query did not settle");
        }
        return future.Get();
    }

    size_t ContextCount()
    {
        auto future = litebus::Async(actor_->GetAID(), [this]() -> size_t { return actor_->restoreContexts_.size(); });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("context count query did not settle");
        }
        return future.Get();
    }

    size_t ActiveMarkerCount()
    {
        auto future =
            litebus::Async(actor_->GetAID(), [this]() -> size_t { return actor_->restoringCheckpoints_.size(); });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("marker count query did not settle");
        }
        return future.Get();
    }

    std::string ActiveMarkerNonce(const std::string &checkpointID)
    {
        auto future = litebus::Async(actor_->GetAID(), [this, checkpointID]() -> std::string {
            const auto it = actor_->restoringCheckpoints_.find(checkpointID);
            return it == actor_->restoringCheckpoints_.end() ? std::string() : it->second;
        });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("marker query did not settle");
        }
        return future.Get();
    }

    uint64_t WakeSequence()
    {
        auto future = litebus::Async(actor_->GetAID(), [this]() -> uint64_t { return actor_->wakeAttemptSeq_; });
        if (!future.WaitFor(kMaxWaitMs).IsOK()) {
            throw std::runtime_error("wake sequence query did not settle");
        }
        return future.Get();
    }

    std::shared_ptr<ControlledSchedSrv> srv_{nullptr};
    std::shared_ptr<TestSnapCtrlActor> actor_{nullptr};
};

// A transport-level failure (failed relay future) settles as a bounded
// generic unresolved response; the captured registration is retained and
// marked unknown with its full identity, waking untouched and uncounted
// (no wake wrapper exists on this path).
TEST_F(DirectRestoreContextTest, TransportFailureRetainsRegistrationUnknown)
{
    const auto registration = Park("instance", "checkpoint");
    ASSERT_FALSE(registration.empty());

    auto future = Begin("request-1", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));
    EXPECT_FALSE(future.WaitFor(kPendingCheckMs).IsOK());  // pending until settled

    srv_->SettleFailed(0);
    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future.Get().code(), common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(future.Get().message(), "restore outcome unresolved");
    SyncActor();

    const auto parked = Parked("instance");
    EXPECT_TRUE(parked.exists);
    EXPECT_TRUE(parked.wakeUnknown);
    EXPECT_FALSE(parked.waking);
    EXPECT_EQ(parked.wakeFails, uint32_t{0});
    EXPECT_EQ(parked.registrationID, registration);
    EXPECT_EQ(parked.checkpointID, "checkpoint");
    EXPECT_EQ(parked.holdToken, "hold-token");
    EXPECT_EQ(ActiveMarkerCount(), size_t{0});  // own active marker erased
    const auto context = Context("request-1");
    EXPECT_TRUE(context.exists);
    EXPECT_TRUE(context.completed);
}

// A relay-level error response retains the captured registration as unknown;
// the diagnostic code passes through but the text is a generic unresolved
// message, never the relay's raw string.
TEST_F(DirectRestoreContextTest, RelayErrorRetainsRegistrationUnknown)
{
    const auto registration = Park("instance", "checkpoint");
    ASSERT_FALSE(registration.empty());

    auto future = Begin("request-2", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));

    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_INNER_SYSTEM_ERROR);
    rsp.set_message("relay rejected");
    srv_->SettleValue(0, rsp);

    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future.Get().code(), common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(future.Get().message(), "restore outcome unresolved");
    EXPECT_NE(future.Get().message(), rsp.message());
    SyncActor();

    const auto parked = Parked("instance");
    EXPECT_TRUE(parked.exists);
    EXPECT_TRUE(parked.wakeUnknown);
    EXPECT_FALSE(parked.waking);
    EXPECT_EQ(parked.registrationID, registration);
}

// Identical in-flight requests share the SAME retained future with a single
// dispatch, and the one-active-restore-per-checkpoint guard still refuses a
// different request for the same checkpoint.
TEST_F(DirectRestoreContextTest, DuplicateRequestsShareRetainedFutureAndSingleDispatch)
{
    const auto registration = Park("instance", "checkpoint");
    ASSERT_FALSE(registration.empty());

    auto first = Begin("request-3", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));
    EXPECT_EQ(srv_->DispatchedRequestID(0), "request-3");  // caller ID travels downstream

    auto second = Begin("request-3", "checkpoint");
    SyncActor();
    EXPECT_EQ(srv_->DispatchCount(), 1U);
    // Async creates separate outer futures; compare the actual entry-point
    // futures on the actor thread, including the retained context promise.
    auto shared = litebus::Async(actor_->GetAID(), [this]() {
        auto a = actor_->HandleSnapStart("request-3", "checkpoint", "");
        auto b = actor_->HandleSnapStart("request-3", "checkpoint", "");
        return a == b && a == actor_->restoreContexts_.at("request-3")->promise.GetFuture();
    });
    ASSERT_TRUE(shared.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_TRUE(shared.Get());
    EXPECT_EQ(srv_->DispatchCount(), 1U);

    auto other = Begin("request-3b", "checkpoint");
    ASSERT_TRUE(other.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(other.Get().code(), BusyCode());
    EXPECT_EQ(srv_->DispatchCount(), 1U);

    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_NONE);
    rsp.set_instanceid("inst-3");
    srv_->SettleValue(0, rsp);
    ASSERT_TRUE(first.WaitFor(kMaxWaitMs).IsOK());
    ASSERT_TRUE(second.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(first.Get().code(), common::ERR_NONE);
    EXPECT_EQ(second.Get().code(), common::ERR_NONE);  // one shared promise, one response
    SnapStartedInfo started;
    ASSERT_TRUE(started.ParseFromString(second.Get().payload()));
    EXPECT_EQ(started.instanceid(), "inst-3");
    SyncActor();
    EXPECT_FALSE(Parked("instance").exists);  // captured registration cleared once
}

// A completed context replays its first success without a new dispatch, new
// captures or cleanup: a replacement park of the same checkpoint survives,
// and so does a duplicate settlement delivery.
TEST_F(DirectRestoreContextTest, CompletedSuccessReplayCannotClearLaterRegistration)
{
    const auto original = Park("instance", "checkpoint");
    ASSERT_FALSE(original.empty());

    auto future = Begin("request-4", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));
    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_NONE);
    rsp.set_instanceid("inst-4");
    srv_->SettleValue(0, rsp);
    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    SyncActor();
    EXPECT_FALSE(Parked("instance").exists);

    const auto replacement = Park("instance", "checkpoint");
    ASSERT_NE(replacement, original);

    auto replay = Begin("request-4", "checkpoint");
    ASSERT_TRUE(replay.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(replay.Get().code(), common::ERR_NONE);
    EXPECT_EQ(srv_->DispatchCount(), 1U);

    // A duplicate settlement delivery (same nonce) must do nothing.
    const auto nonce = ContextNonce("request-4");
    ASSERT_FALSE(nonce.empty());
    litebus::Async(actor_->GetAID(), [this, nonce]() {
        messages::RestoreSnapshotResponse duplicate;
        duplicate.set_code(common::ERR_NONE);
        duplicate.set_instanceid("inst-dup");
        litebus::Future<messages::RestoreSnapshotResponse> ready(duplicate);
        actor_->OnRestoreRelayComplete("request-4", nonce, ready);
    });
    SyncActor();
    const auto parked = Parked("instance");
    EXPECT_TRUE(parked.exists);
    EXPECT_EQ(parked.registrationID, replacement);
    EXPECT_EQ(srv_->DispatchCount(), 1U);
}

// A changed checkpoint or payload under the same requestID is a generic
// parameter error that neither dispatches nor disturbs the original.
TEST_F(DirectRestoreContextTest, ChangedRequestConflictsWithoutTouchingOriginal)
{
    ASSERT_FALSE(Park("instance", "checkpoint").empty());

    auto original = Begin("request-5", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));

    auto conflictCkpt = Begin("request-5", "other-checkpoint");
    ASSERT_TRUE(conflictCkpt.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(conflictCkpt.Get().code(), ParamInvalidCode());

    SnapStartOptions options;
    (void)options.mutable_scheduleopts();  // valid nonempty protobuf, different from the original empty payload
    auto conflictPayload = Begin("request-5", "checkpoint", options.SerializeAsString());
    ASSERT_TRUE(conflictPayload.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(conflictPayload.Get().code(), ParamInvalidCode());
    EXPECT_EQ(srv_->DispatchCount(), 1U);
    const auto context = Context("request-5");
    ASSERT_TRUE(context.exists);
    EXPECT_FALSE(context.completed);
    EXPECT_EQ(context.payload, "");

    // The original entry is untouched: it still settles through its promise.
    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_NONE);
    rsp.set_instanceid("inst-5");
    srv_->SettleValue(0, rsp);
    ASSERT_TRUE(original.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(original.Get().code(), common::ERR_NONE);
}

// A settlement whose nonce no longer owns the active checkpoint marker does
// nothing at all: no marker removal, no registration clear, promise kept.
TEST_F(DirectRestoreContextTest, StaleNonceSettlementDoesNothing)
{
    const auto registration = Park("instance", "checkpoint");
    ASSERT_FALSE(registration.empty());

    auto future = Begin("request-6", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));
    const auto nonce = ContextNonce("request-6");
    ASSERT_FALSE(nonce.empty());

    // Fabricate a superseded active marker, as a newer dispatch would leave.
    litebus::Async(actor_->GetAID(), [this]() { actor_->restoringCheckpoints_["checkpoint"] = "newer-nonce"; });
    SyncActor();

    litebus::Async(actor_->GetAID(), [this, nonce]() {
        messages::RestoreSnapshotResponse rsp;
        rsp.set_code(common::ERR_NONE);
        rsp.set_instanceid("inst-6");
        litebus::Future<messages::RestoreSnapshotResponse> ready(rsp);
        actor_->OnRestoreRelayComplete("request-6", nonce, ready);          // real nonce, marker superseded
        actor_->OnRestoreRelayComplete("request-6", "wrong-nonce", ready);  // wrong nonce
    });
    SyncActor();

    EXPECT_FALSE(future.WaitFor(kPendingCheckMs).IsOK());  // promise untouched
    EXPECT_EQ(ActiveMarkerNonce("checkpoint"), "newer-nonce");
    const auto parked = Parked("instance");
    EXPECT_TRUE(parked.exists);
    EXPECT_FALSE(parked.wakeUnknown);
    EXPECT_EQ(parked.registrationID, registration);
    const auto context = Context("request-6");
    EXPECT_TRUE(context.exists);
    EXPECT_FALSE(context.completed);
}

// A completed error context replays its first error and cannot touch a
// replacement park registered afterwards.
TEST_F(DirectRestoreContextTest, LateErrorReplayCannotTouchReplacementPark)
{
    const auto original = Park("instance", "checkpoint");
    ASSERT_FALSE(original.empty());

    auto future = Begin("request-7", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));
    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_INNER_SYSTEM_ERROR);
    rsp.set_message("relay rejected");
    srv_->SettleValue(0, rsp);
    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    SyncActor();
    ASSERT_TRUE(Parked("instance").wakeUnknown);

    const auto replacement = Park("instance", "checkpoint");  // fresh park replaces the entry
    ASSERT_NE(replacement, original);

    auto replay = Begin("request-7", "checkpoint");
    ASSERT_TRUE(replay.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(replay.Get().code(), common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(srv_->DispatchCount(), 1U);
    const auto parked = Parked("instance");
    EXPECT_TRUE(parked.exists);
    EXPECT_EQ(parked.registrationID, replacement);
    EXPECT_FALSE(parked.wakeUnknown);
}

// Bounded admission: empty/oversized requestIDs, empty/oversized checkpoint
// IDs and oversized payloads are rejected before anything is copied, parsed,
// retained or dispatched.
TEST_F(DirectRestoreContextTest, InputBoundsRejectWithoutSideEffects)
{
    auto emptyID = Begin("", "checkpoint");
    ASSERT_TRUE(emptyID.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(emptyID.Get().code(), ParamInvalidCode());

    auto longID = Begin(std::string(SnapCtrlActor::kMaxRestoreRequestIDBytes + 1, 'r'), "checkpoint");
    ASSERT_TRUE(longID.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(longID.Get().code(), ParamInvalidCode());

    auto emptyCkpt = Begin("request", "");
    ASSERT_TRUE(emptyCkpt.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(emptyCkpt.Get().code(), ParamInvalidCode());

    auto longCkpt = Begin("request", std::string(SnapCtrlActor::kMaxRestoreCheckpointIDBytes + 1, 'c'));
    ASSERT_TRUE(longCkpt.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(longCkpt.Get().code(), ParamInvalidCode());

    auto hugePayload = Begin("request", "checkpoint", std::string(SnapCtrlActor::kMaxRestorePayloadBytes + 1, 'p'));
    ASSERT_TRUE(hugePayload.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(hugePayload.Get().code(), ParamInvalidCode());

    SyncActor();
    EXPECT_EQ(srv_->DispatchCount(), 0U);
    EXPECT_EQ(ContextCount(), 0U);
}

// An oversized relay response becomes a bounded unresolved error; the
// oversized body is never retained and the captured entry stays unknown.
TEST_F(DirectRestoreContextTest, OversizedResponseBecomesBoundedUnresolved)
{
    const auto registration = Park("instance", "checkpoint");
    ASSERT_FALSE(registration.empty());

    auto future = Begin("request-8", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));

    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_INNER_SYSTEM_ERROR);
    rsp.set_message(std::string(70 * 1024, 'x'));
    srv_->SettleValue(0, rsp);

    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future.Get().code(), common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(future.Get().message(), "restore response exceeded the bounded size");
    SyncActor();
    const auto parked = Parked("instance");
    EXPECT_TRUE(parked.exists);
    EXPECT_TRUE(parked.wakeUnknown);
    EXPECT_EQ(parked.registrationID, registration);
}

// Transformed-output size boundary: a success response whose serialized
// input lands EXACTLY on the response reservation is admitted, and so is its
// wrapped output. proto3 omits the zero code and empty strings, so the
// arithmetic is: input = instanceID field (3) + snapstartInfo envelope
// (1 + 3 + 65529) = 65536; output = payload envelope (1 + 3 + 65532) = 65536.
// Any off-by-one in the input or transformed-output bound, or envelope
// growth from a schema change, trips here first.
TEST_F(DirectRestoreContextTest, BoundaryResponseAtExactBoundStillSucceeds)
{
    const auto registration = Park("instance", "checkpoint");
    ASSERT_FALSE(registration.empty());

    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_NONE);
    rsp.set_instanceid("x");
    rsp.mutable_snapstartinfo()->set_routeaddress(std::string(65525, 'a'));
    ASSERT_EQ(rsp.ByteSizeLong(), SnapCtrlActor::kMaxRestoreResponseBytes);  // input exactly at bound

    auto future = Begin("request-boundary", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));
    srv_->SettleValue(0, rsp);

    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future.Get().code(), common::ERR_NONE);
    EXPECT_EQ(future.Get().payload().size(), size_t{65532});  // wrapped SnapStartedInfo
    SnapStartedInfo started;
    ASSERT_TRUE(started.ParseFromString(future.Get().payload()));
    EXPECT_EQ(started.instanceid(), "x");
    EXPECT_EQ(started.routeaddress().size(), size_t{65525});
    SyncActor();
    EXPECT_FALSE(Parked("instance").exists);  // success cleared the captured registration
    EXPECT_EQ(ActiveMarkerCount(), size_t{0});
    EXPECT_TRUE(Context("request-boundary").completed);
}

// Generic-error boundary: relay error text never reaches the caller. Even a
// historically "terminal" substring keeps the diagnostic code, returns the
// same bounded generic unresolved message as every other error flavor, and
// retains the registration unknown — no text-driven retirement exists.
TEST_F(DirectRestoreContextTest, RelayErrorTextIsNeverEchoedToCaller)
{
    const auto registration = Park("instance", "checkpoint");
    ASSERT_FALSE(registration.empty());

    auto future = Begin("request-generic", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));
    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_INSTANCE_NOT_FOUND);
    rsp.set_message("checkpoint artifact not found");
    srv_->SettleValue(0, rsp);

    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future.Get().code(), common::ERR_INSTANCE_NOT_FOUND);  // diagnostic code preserved
    EXPECT_EQ(future.Get().message(), "restore outcome unresolved");
    EXPECT_NE(future.Get().message(), rsp.message());  // raw text is never echoed
    SyncActor();
    const auto parked = Parked("instance");
    EXPECT_TRUE(parked.exists);
    EXPECT_TRUE(parked.wakeUnknown);
    EXPECT_EQ(parked.registrationID, registration);
    EXPECT_TRUE(Context("request-generic").completed);
}

// The reserved serialized-payload budget refuses NEW work without evicting
// anything; an identical retry of an accepted entry still shares its
// retained future while the map is full.
TEST_F(DirectRestoreContextTest, PayloadBudgetRefusesNewWorkWithoutEvicting)
{
    struct FillResult {
        size_t accepted{0};
        int32_t refusedCode{-1};
    };
    auto fill = litebus::Async(actor_->GetAID(), [this]() -> FillResult {
        FillResult result;
        for (size_t i = 0; i < SnapCtrlActor::kMaxRestoreContexts + 16; i++) {
            auto future = actor_->HandleSnapStart("fill-" + std::to_string(i), "ckpt-fill-" + std::to_string(i), "");
            if (future.IsInit()) {
                result.accepted++;
                continue;
            }
            result.refusedCode = static_cast<int32_t>(future.Get().code());
            break;
        }
        return result;
    });
    ASSERT_TRUE(fill.WaitFor(kFillWaitMs).IsOK()) << "context fill did not settle";
    const auto result = fill.Get();

    ASSERT_GE(result.accepted, 2U);
    ASSERT_LT(result.accepted, SnapCtrlActor::kMaxRestoreContexts + 16U);  // the bound actually refused
    EXPECT_EQ(result.refusedCode, static_cast<int32_t>(BusyCode()));
    EXPECT_EQ(srv_->DispatchCount(), result.accepted);  // one dispatch per accepted begin
    EXPECT_EQ(ContextCount(), result.accepted);         // nothing was evicted

    auto retained = Begin("fill-0", "ckpt-fill-0");
    SyncActor();
    EXPECT_EQ(srv_->DispatchCount(), result.accepted);       // no new dispatch
    EXPECT_FALSE(retained.WaitFor(kPendingCheckMs).IsOK());  // retained future still pending
}

// A valid success settles with the existing SnapStartedInfo fields, clears
// exactly the captured registration, erases its own active marker and
// completes the context exactly once.
TEST_F(DirectRestoreContextTest, SuccessPreservesSnapStartedFieldsAndSettlesOnce)
{
    ASSERT_FALSE(Park("instance", "checkpoint").empty());

    auto future = Begin("request-10", "checkpoint");
    ASSERT_TRUE(srv_->WaitForDispatch(1));

    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_NONE);
    rsp.set_instanceid("inst-10");
    rsp.set_message("restored successfully");
    auto *info = rsp.mutable_snapstartinfo();
    info->set_routeaddress("10.0.0.1:1010");
    info->set_portmappings("tcp:1010");
    info->set_functionproxyid("proxy-10");
    info->set_nodeid("node-10");
    info->set_namespace_("ns-10");
    srv_->SettleValue(0, rsp);

    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(future.Get().code(), common::ERR_NONE);
    SnapStartedInfo started;
    ASSERT_TRUE(started.ParseFromString(future.Get().payload()));
    EXPECT_EQ(future.Get().message(), "restored successfully");
    EXPECT_EQ(started.instanceid(), "inst-10");
    EXPECT_EQ(started.routeaddress(), "10.0.0.1:1010");
    EXPECT_EQ(started.portmappings(), "tcp:1010");
    EXPECT_EQ(started.functionproxyid(), "proxy-10");
    EXPECT_EQ(started.nodeid(), "node-10");
    EXPECT_EQ(started.namespace_(), "ns-10");

    SyncActor();
    EXPECT_FALSE(Parked("instance").exists);
    EXPECT_EQ(ActiveMarkerCount(), size_t{0});
    EXPECT_TRUE(Context("request-10").completed);
}

// Reusing a wake's caller requestID against a NEW park registration is
// rejected before any waking/sequence mutation and without attaching a new
// completion wrapper: an old cached success must not complete a new wake.
TEST_F(DirectRestoreContextTest, HandleWakeRequestIDReuseRejectsNewRegistration)
{
    ASSERT_FALSE(Park("instance", "checkpoint").empty());

    auto future = Begin("wake-req", "checkpoint");  // the id the wake will reuse
    ASSERT_TRUE(srv_->WaitForDispatch(1));
    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_NONE);
    rsp.set_instanceid("inst-11");
    srv_->SettleValue(0, rsp);
    ASSERT_TRUE(future.WaitFor(kMaxWaitMs).IsOK());
    SyncActor();
    EXPECT_FALSE(Parked("instance").exists);

    const auto replacement = Park("instance", "checkpoint");
    ASSERT_FALSE(replacement.empty());
    const auto sequenceBefore = WakeSequence();

    auto wake = litebus::Async(actor_->GetAID(), [this]() { return actor_->HandleWake("wake-req", "instance"); });
    ASSERT_TRUE(wake.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(wake.Get().code(), ParamInvalidCode());
    EXPECT_EQ(WakeSequence(), sequenceBefore);
    EXPECT_EQ(srv_->DispatchCount(), 1U);  // no new wrapper, no new dispatch
    const auto parked = Parked("instance");
    EXPECT_TRUE(parked.exists);
    EXPECT_FALSE(parked.waking);
    EXPECT_EQ(parked.wakeAttempt, uint64_t{0});
    EXPECT_EQ(parked.registrationID, replacement);
}

// The full wake chain composes once: the context settlement marks the entry
// unknown without counting, and HandleWake's own completion wrapper then
// settles and counts it exactly one time.
TEST_F(DirectRestoreContextTest, WakeErrorSettlesAndCountsOnceThroughWrapper)
{
    const auto registration = Park("instance", "checkpoint");
    ASSERT_FALSE(registration.empty());

    auto wake = litebus::Async(actor_->GetAID(), [this]() { return actor_->HandleWake("wake-req-2", "instance"); });
    ASSERT_TRUE(srv_->WaitForDispatch(1));

    messages::RestoreSnapshotResponse rsp;
    rsp.set_code(common::ERR_INNER_SYSTEM_ERROR);
    rsp.set_message("relay rejected");
    srv_->SettleValue(0, rsp);

    ASSERT_TRUE(wake.WaitFor(kMaxWaitMs).IsOK());
    EXPECT_EQ(wake.Get().code(), common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(wake.Get().message(), "restore outcome unresolved");
    SyncActor();

    const auto parked = Parked("instance");
    EXPECT_TRUE(parked.exists);
    EXPECT_TRUE(parked.wakeUnknown);
    EXPECT_FALSE(parked.waking);
    EXPECT_EQ(parked.wakeFails, uint32_t{1});  // counted exactly once
    EXPECT_EQ(parked.registrationID, registration);
    EXPECT_EQ(ActiveMarkerCount(), size_t{0});
}

}  // namespace functionsystem::test
