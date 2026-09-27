/*
 * REAL SnapCtrlActor public-chain fixture tests for the restored
 * type=SNAPSHOT named-snapshot chain. The actor is INSTANTIATED and driven
 * through litebus (Spawn/Async/Sync) exactly like the historical harness;
 * only mocks sit at the service boundaries (LocalSchedSrv, FunctionAgentMgr,
 * client manager, instance control view). No algorithm is copied into the
 * test and the gate is exercised THROUGH the actor, not directly.
 *
 * Scenarios (each names the behavior it pins):
 *   PublicInFlightBlocksPrivate / PrivateInFlightBlocksPublic
 *   UnknownOutcomeBlocksNewRequests
 *   SameRequestReplaySettlesAuthoritativelyAndReleases
 *   ReplayPrepareSnapFailureRetainsUnknownAndNeverFails   (review counterexample A)
 *   ResultUnknownAndUnprovenFailureKeepPublishingRecord   (review counterexample B)
 */
#include "function_proxy/local_scheduler/snap_ctrl/snap_ctrl_actor.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "async/async.hpp"
#include "common/types/instance_state.h"
#include "function_proxy/common/posix_client/control_plane_client/control_interface_posix_client.h"
#include "function_proxy/common/state_machine/instance_context.h"
#include "mocks/mock_control_interface_client_manager_proxy.h"
#include "mocks/mock_function_agent_mgr.h"
#include "mocks/mock_instance_ctrl.h"
#include "mocks/mock_instance_control_view.h"
#include "mocks/mock_local_sched_srv.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {
namespace {

using namespace ::testing;
using namespace local_scheduler;

constexpr char OWNER_PROXY_ID[] = "public-chain-owner";
constexpr char INSTANCE_ID[] = "public-chain-instance";
constexpr char INSTANCE_REQUEST_ID[] = "create-public-chain-instance";
constexpr int64_t INSTANCE_VERSION = 23;
constexpr char RUNTIME_ID[] = "runtime-public-chain-1";
constexpr char AGENT_ID[] = "agent-public-chain-1";

resources::InstanceInfo MakePublicInstanceInfo()
{
    resources::InstanceInfo info;
    info.set_instanceid(INSTANCE_ID);
    info.set_requestid(INSTANCE_REQUEST_ID);
    info.set_functionproxyid(OWNER_PROXY_ID);
    info.set_tenantid("default");
    info.set_runtimeid(RUNTIME_ID);
    info.set_functionagentid(AGENT_ID);
    info.set_version(INSTANCE_VERSION);
    info.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    return info;
}

class PublicStateMachineProbe final : public InstanceStateMachine {
public:
    explicit PublicStateMachineProbe(const std::shared_ptr<InstanceContext> &context)
        : InstanceStateMachine(OWNER_PROXY_ID, context, false)
    {
    }
};

std::shared_ptr<PublicStateMachineProbe> MakePublicStateMachine()
{
    auto info = MakePublicInstanceInfo();
    auto scheduleRequest = std::make_shared<messages::ScheduleRequest>();
    scheduleRequest->set_requestid(info.requestid());
    scheduleRequest->mutable_instance()->CopyFrom(info);
    auto context = std::make_shared<InstanceContext>(scheduleRequest);
    auto stateMachine = std::make_shared<PublicStateMachineProbe>(context);
    stateMachine->SetVersion(info.version());
    return stateMachine;
}

// Controllable runtime client: the PUBLIC chain's legacy PrepareSnap and
// the CONDITIONAL private path's PrepareSnapForRuntime both arm ONE
// controllable promise; SnapStarted answers immediately. Construction
// mirrors the historical ControllablePrepareSnapClient (dual nullptr
// bases) so the client needs no live transport.
class ControllableRuntimeClient final : public ControlInterfacePosixClient {
public:
    ControllableRuntimeClient() : BaseClient(nullptr), ControlInterfacePosixClient(nullptr) {}

    litebus::Future<runtime::PrepareSnapResponse> PrepareSnap(
        runtime::PrepareSnapRequest &&) override
    {
        return ArmPendingPrepare();
    }

    litebus::Future<runtime::PrepareSnapResponse> PrepareSnapForRuntime(
        runtime::PrepareSnapRequest &&, const std::string &expectedRuntimeID) override
    {
        // the pinned entry point must carry the caller's captured runtime;
        // anything else is refused before arming anything
        if (expectedRuntimeID != RUNTIME_ID) {
            runtime::PrepareSnapResponse response;
            response.set_code(common::ERR_PARAM_INVALID);
            response.set_message("expected runtime mismatch in test client");
            return response;
        }
        return ArmPendingPrepare();
    }

    litebus::Future<runtime::SnapStartedResponse> SnapStarted(
        runtime::SnapStartedRequest &&) override
    {
        runtime::SnapStartedResponse response;
        response.set_code(common::ERR_NONE);
        return response;
    }

    // Block until the actor has actually entered a Prepare — a Sync alone
    // does not prove deferred continuations reached the client.
    bool WaitForPrepareEntered(int timeoutMs = 5000)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (std::chrono::steady_clock::now() < deadline) {
            {
                std::lock_guard<std::mutex> guard(lock_);
                if (pendingPrepare_ != nullptr) {
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return false;
    }

    void SettlePrepare(common::ErrorCode code, const std::string &message)
    {
        std::shared_ptr<litebus::Promise<runtime::PrepareSnapResponse>> promise;
        {
            std::lock_guard<std::mutex> guard(lock_);
            promise = pendingPrepare_;
            pendingPrepare_ = nullptr;
        }
        if (promise != nullptr) {
            runtime::PrepareSnapResponse response;
            response.set_code(code);
            response.set_message(message);
            promise->SetValue(response);
        }
    }

    void FailOutstanding()
    {
        SettlePrepare(common::ERR_INNER_COMMUNICATION, "test cleanup");
    }

private:
    litebus::Future<runtime::PrepareSnapResponse> ArmPendingPrepare()
    {
        std::lock_guard<std::mutex> guard(lock_);
        auto promise = std::make_shared<litebus::Promise<runtime::PrepareSnapResponse>>();
        pendingPrepare_ = promise;
        return promise->GetFuture();
    }

    std::mutex lock_;
    std::shared_ptr<litebus::Promise<runtime::PrepareSnapResponse>> pendingPrepare_;
};

// Agent-side boundary of the public chain: typed checkpoint, artifact
// publish, exact-cleanup finalize. Every outcome is test-controlled.
class PublicChainAgentProbe final : public MockFunctionAgentMgr {
public:
    PublicChainAgentProbe() : MockFunctionAgentMgr("public-chain-agent", nullptr) {}

    litebus::Future<messages::SnapshotRuntimeResponse> SnapshotRuntime(
        const std::string &requestID, const resource_view::InstanceInfo &, int32_t,
        bool) override
    {
        return ProbeResponse("legacy overload unexpectedly used");
    }

    litebus::Future<messages::SnapshotRuntimeResponse> SnapshotRuntime(
        const std::string &requestID, const resource_view::InstanceInfo &, int32_t,
        common::SnapType, const std::string &snapshotID, const std::string &) override
    {
        typedCalls_.fetch_add(1);
        return ProbeResponse(snapshotID);
    }

    litebus::Future<messages::SnapshotRuntimeResponse> PublishSnapshotArtifact(
        const std::string &, const resource_view::InstanceInfo &, const std::string &) override
    {
        publishCalls_.fetch_add(1);
        messages::SnapshotRuntimeResponse response;
        auto *artifact = response.mutable_reusablesnapshotartifact();
        artifact->set_storagebackend("local");
        artifact->set_objectkey("obj-" + std::to_string(publishCalls_.load()));
        artifact->set_size(4096);
        artifact->set_sha256("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
        response.set_code(common::ERR_NONE);
        return response;
    }

    litebus::Future<::messages::SnapshotAttemptFinalizeResponse> FinalizeSnapshotAttempt(
        const resource_view::InstanceInfo &, const ::messages::SnapshotAttemptFinalizeRequest &) override
    {
        finalizeCalls_.fetch_add(1);
        ::messages::SnapshotAttemptFinalizeResponse response;
        response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        response.set_localcleanupcomplete(true);
        response.set_remotecleanupcomplete(true);
        return response;
    }

    struct TypedOutcome {
        common::ErrorCode code = common::ERR_NONE;
        bool resultUnknown = false;
        bool checkpointNotStarted = false;
        bool withLocalSnapshot = true;
    };

    // outcome the next typed checkpoint returns
    void SetTypedOutcome(const TypedOutcome &outcome)
    {
        std::lock_guard<std::mutex> guard(lock_);
        typedOutcome_ = outcome;
    }

    int TypedCalls() const { return typedCalls_.load(); }
    int PublishCalls() const { return publishCalls_.load(); }
    int FinalizeCalls() const { return finalizeCalls_.load(); }

private:
    litebus::Future<messages::SnapshotRuntimeResponse> ProbeResponse(const std::string &snapshotID)
    {
        TypedOutcome outcome;
        {
            std::lock_guard<std::mutex> guard(lock_);
            outcome = typedOutcome_;
        }
        messages::SnapshotRuntimeResponse response;
        response.set_code(outcome.code);
        response.set_resultunknown(outcome.resultUnknown);
        response.set_checkpointnotstarted(outcome.checkpointNotStarted);
        response.set_message(outcome.code == common::ERR_NONE ? "" : "typed checkpoint failed");
        if (outcome.withLocalSnapshot) {
            auto *snapshot = response.mutable_localsnapshot();
            snapshot->set_snapshotid(snapshotID);
            snapshot->set_localrecoverycandidate(true);
            snapshot->set_size(4096);
            snapshot->set_createdatunixseconds(1);
        }
        return response;
    }

    std::atomic<int> typedCalls_ { 0 };
    std::atomic<int> publishCalls_ { 0 };
    std::atomic<int> finalizeCalls_ { 0 };
    std::mutex lock_;
    TypedOutcome typedOutcome_;
};

// Store-side boundary: Begin/Resolve/Commit/Fail, all test-controlled.
class PublicChainSchedProbe final : public MockLocalSchedSrv {
public:
    litebus::Future<::messages::BeginReusableSnapshotResponse> BeginReusableSnapshot(
        const std::shared_ptr<::messages::BeginReusableSnapshotRequest> &request) override
    {
        beginCalls_.fetch_add(1);
        lastBeginRequestID_ = request->requestid();
        ::messages::BeginReusableSnapshotResponse response;
        response.set_code(common::ERR_NONE);
        response.set_snapshotid("snap-stable-1");
        response.set_phase(::messages::REUSABLE_SNAPSHOT_PUBLISHING);
        return response;
    }

    litebus::Future<::messages::ResolveReusableSnapshotForCreateResponse> ResolveReusableSnapshotForCreate(
        const std::shared_ptr<::messages::ResolveReusableSnapshotForCreateRequest> &) override
    {
        resolveCalls_.fetch_add(1);
        ::messages::ResolveReusableSnapshotForCreateResponse response;
        response.set_code(common::ERR_NONE);
        auto *restore = response.mutable_reusablesnapshotrestore();
        auto *artifact = restore->mutable_artifact();
        artifact->set_storagebackend("local");
        artifact->set_objectkey("obj-resolved");
        artifact->set_size(4096);
        artifact->set_sha256("feedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedface");
        return response;
    }

    litebus::Future<::messages::CommitReusableSnapshotResponse> CommitReusableSnapshot(
        const std::shared_ptr<::messages::CommitReusableSnapshotRequest> &) override
    {
        commitCalls_.fetch_add(1);
        ::messages::CommitReusableSnapshotResponse response;
        response.set_code(common::ERR_NONE);
        auto *info = response.mutable_snapshotinfo();
        info->set_snapshotid("snap-stable-1");
        info->add_names("pc-public-chain");
        return response;
    }

    litebus::Future<::messages::FailReusableSnapshotResponse> FailReusableSnapshot(
        const std::shared_ptr<::messages::FailReusableSnapshotRequest> &) override
    {
        failCalls_.fetch_add(1);
        ::messages::FailReusableSnapshotResponse response;
        response.set_code(common::ERR_NONE);
        return response;
    }

    int BeginCalls() const { return beginCalls_.load(); }
    int FailCalls() const { return failCalls_.load(); }
    std::string LastBeginRequestID() const { return lastBeginRequestID_; }

private:
    std::atomic<int> beginCalls_ { 0 };
    std::atomic<int> resolveCalls_ { 0 };
    std::atomic<int> commitCalls_ { 0 };
    std::atomic<int> failCalls_ { 0 };
    std::string lastBeginRequestID_;
};

std::string MakePublicPayload(const std::string &name, uint64_t timeoutMs)
{
    core_service::SnapOptions options;
    options.set_type(common::SNAPSHOT);
    options.set_name(name);
    options.set_checkpointtimeoutms(timeoutMs);
    return options.SerializeAsString();
}

std::string MakePrivateParkPayload()
{
    core_service::SnapOptions options;
    options.set_leaverunning(false);
    options.set_hold_token("hold-token-0123456789abcdef");
    options.set_expected_runtime_id(RUNTIME_ID);
    options.set_expected_sandbox_id("sbox-public-chain-1");
    options.set_expected_resource_generation("gen-public-chain-1");
    return options.SerializeAsString();
}

class SnapCtrlPublicChainTest : public Test {
public:
    void SetUp() override
    {
        stateMachine_ = MakePublicStateMachine();
        instanceControlView_ = std::make_shared<MockInstanceControlView>(OWNER_PROXY_ID);
        ON_CALL(*instanceControlView_, GetInstance(INSTANCE_ID))
            .WillByDefault(Return(stateMachine_));
        EXPECT_CALL(*instanceControlView_, GetInstance(INSTANCE_ID)).Times(AnyNumber());

        runtimeClient_ = std::make_shared<ControllableRuntimeClient>();
        clientManager_ = std::make_shared<MockControlInterfaceClientManagerProxy>();
        ON_CALL(*clientManager_, GetControlInterfacePosixClient(INSTANCE_ID))
            .WillByDefault(Return(runtimeClient_));
        EXPECT_CALL(*clientManager_, GetControlInterfacePosixClient(INSTANCE_ID)).Times(AnyNumber());

        agentProbe_ = std::make_shared<PublicChainAgentProbe>();
        schedProbe_ = std::make_shared<PublicChainSchedProbe>();
        instanceCtrl_ = std::make_shared<MockInstanceCtrl>();
        ON_CALL(*instanceCtrl_, TransInstanceState(_, _))
            .WillByDefault(Invoke([](const std::shared_ptr<InstanceStateMachine> &machine,
                                     const TransContext &context) { return machine->TransitionTo(context); }));

        actor_ = std::make_shared<SnapCtrlActor>("public-chain-test", OWNER_PROXY_ID);
        actor_->BindInstanceControlView(instanceControlView_);
        actor_->BindClientManager(clientManager_);
        actor_->BindFunctionAgentMgr(agentProbe_);
        actor_->BindInstanceCtrl(instanceCtrl_);
        actor_->BindLocalSchedSrv(schedProbe_);
        litebus::Spawn(actor_);
    }

    void TearDown() override
    {
        runtimeClient_->FailOutstanding();
        for (auto &request : requests_) {
            (void)request.WaitFor(5000);
        }
        litebus::Terminate(actor_->GetAID());
        litebus::Await(actor_->GetAID());
        actor_ = nullptr;
    }

protected:
    litebus::Future<KillResponse> PublicSnapshot(const std::string &requestID,
                                                 const std::string &name = "pc-public-chain",
                                                 uint64_t timeoutMs = 300000)
    {
        auto request = litebus::Async(actor_->GetAID(), &SnapCtrlActor::HandleSnapshot, requestID,
                                      std::string(INSTANCE_ID), MakePublicPayload(name, timeoutMs));
        requests_.emplace_back(request);
        return request;
    }

    litebus::Future<KillResponse> PrivatePark(const std::string &requestID)
    {
        auto request = litebus::Async(actor_->GetAID(), &SnapCtrlActor::HandleSnapshot, requestID,
                                      std::string(INSTANCE_ID), MakePrivateParkPayload());
        requests_.emplace_back(request);
        return request;
    }

    void FlushActor()
    {
        auto flushed = litebus::Async(actor_->GetAID(), &SnapCtrlActor::Sync);
        ASSERT_AWAIT_READY(flushed);
        ASSERT_TRUE(flushed.Get().IsOk());
    }

    void SettlePrepareOk()
    {
        ASSERT_TRUE(runtimeClient_->WaitForPrepareEntered());
        runtimeClient_->SettlePrepare(common::ERR_NONE, "");
    }

    void SettlePrepareFail(const std::string &message)
    {
        ASSERT_TRUE(runtimeClient_->WaitForPrepareEntered());
        runtimeClient_->SettlePrepare(common::ERR_INNER_COMMUNICATION, message);
    }

    void UpdateInstanceRuntimeID(const std::string &runtimeID)
    {
        auto info = stateMachine_->GetInstanceInfo();
        info.set_runtimeid(runtimeID);
        stateMachine_->UpdateInstanceInfo(info);
    }

    std::shared_ptr<PublicStateMachineProbe> stateMachine_;
    std::shared_ptr<MockInstanceControlView> instanceControlView_;
    std::shared_ptr<ControllableRuntimeClient> runtimeClient_;
    std::shared_ptr<MockControlInterfaceClientManagerProxy> clientManager_;
    std::shared_ptr<PublicChainAgentProbe> agentProbe_;
    std::shared_ptr<PublicChainSchedProbe> schedProbe_;
    std::shared_ptr<MockInstanceCtrl> instanceCtrl_;
    std::shared_ptr<SnapCtrlActor> actor_;
    std::vector<litebus::Future<KillResponse>> requests_;
};

TEST_F(SnapCtrlPublicChainTest, PublicInFlightBlocksPrivate)
{
    auto first = PublicSnapshot("pub-1");
    FlushActor();  // Begin answered PUBLISHING; PrepareSnap now pending
    ASSERT_GT(schedProbe_->BeginCalls(), 0);

    auto priv = PrivatePark("priv-1");
    FlushActor();
    ASSERT_AWAIT_READY(priv);
    EXPECT_EQ(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY),
              static_cast<int32_t>(priv.Get().code()));

    SettlePrepareOk();
    ASSERT_AWAIT_READY(first);
    EXPECT_EQ(common::ERR_NONE, first.Get().code());
}

TEST_F(SnapCtrlPublicChainTest, PrivateInFlightBlocksPublic)
{
    auto priv = PrivatePark("priv-1");
    FlushActor();  // private park pending at its PrepareSnap
    auto pub = PublicSnapshot("pub-1");
    FlushActor();
    ASSERT_AWAIT_READY(pub);
    EXPECT_EQ(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY),
              static_cast<int32_t>(pub.Get().code()));

    // end the private park safely: a Prepare FAILURE lets its own chain
    // settle without touching any real conditional checkpoint state
    SettlePrepareFail("private park test end");
    (void)priv.WaitFor(10000);
}

TEST_F(SnapCtrlPublicChainTest, UnknownOutcomeBlocksNewRequests)
{
    PublicChainAgentProbe::TypedOutcome unknown;
    unknown.code = common::ERR_INNER_COMMUNICATION;
    unknown.resultUnknown = true;
    agentProbe_->SetTypedOutcome(unknown);

    auto first = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(first);
    EXPECT_NE(common::ERR_NONE, first.Get().code());
    EXPECT_EQ(0, schedProbe_->FailCalls());  // record kept, never Failed

    auto second = PublicSnapshot("pub-2");
    FlushActor();
    ASSERT_AWAIT_READY(second);
    EXPECT_EQ(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY),
              static_cast<int32_t>(second.Get().code()));
}

TEST_F(SnapCtrlPublicChainTest, SameRequestReplaySettlesAuthoritativelyAndReleases)
{
    PublicChainAgentProbe::TypedOutcome unknown;
    unknown.code = common::ERR_INNER_COMMUNICATION;
    unknown.resultUnknown = true;
    agentProbe_->SetTypedOutcome(unknown);

    auto first = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(first);
    EXPECT_NE(common::ERR_NONE, first.Get().code());

    // deterministic replay: the SAME requestID, name, timeout, tenant, source
    PublicChainAgentProbe::TypedOutcome healthy;
    agentProbe_->SetTypedOutcome(healthy);
    auto replay = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(replay);
    EXPECT_EQ(common::ERR_NONE, replay.Get().code());
    EXPECT_GT(agentProbe_->PublishCalls(), 0);
    EXPECT_GT(agentProbe_->FinalizeCalls(), 0);

    // authoritative settlement released the shared slot for NEW requests
    auto fresh = PublicSnapshot("pub-2");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(fresh);
    EXPECT_EQ(common::ERR_NONE, fresh.Get().code());
}

TEST_F(SnapCtrlPublicChainTest, ReplayPrepareSnapFailureRetainsUnknownAndNeverFails)
{
    PublicChainAgentProbe::TypedOutcome unknown;
    unknown.code = common::ERR_INNER_COMMUNICATION;
    unknown.resultUnknown = true;
    agentProbe_->SetTypedOutcome(unknown);
    auto first = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(first);
    EXPECT_NE(common::ERR_NONE, first.Get().code());

    // replay whose PrepareSnap FAILS: the old UNKNOWN must survive, the
    // PUBLISHING record must NOT be Failed away, and the gate stays busy
    // for other requests
    auto replay = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareFail("replay prepare failed");
    ASSERT_AWAIT_READY(replay);
    EXPECT_NE(common::ERR_NONE, replay.Get().code());
    EXPECT_EQ(0, schedProbe_->FailCalls());

    auto other = PublicSnapshot("pub-9");
    FlushActor();
    ASSERT_AWAIT_READY(other);
    EXPECT_EQ(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY),
              static_cast<int32_t>(other.Get().code()));
}

TEST_F(SnapCtrlPublicChainTest, ResultUnknownAndUnprovenFailureKeepPublishingRecord)
{
    // (a) code-bearing failure WITHOUT checkpointNotStarted proof
    PublicChainAgentProbe::TypedOutcome unproven;
    unproven.code = common::ERR_INNER_COMMUNICATION;
    unproven.checkpointNotStarted = false;
    agentProbe_->SetTypedOutcome(unproven);
    auto a = PublicSnapshot("pub-a");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(a);
    EXPECT_NE(common::ERR_NONE, a.Get().code());
    EXPECT_EQ(0, schedProbe_->FailCalls());

    // still busy: unknown retained
    auto blocked = PublicSnapshot("pub-b");
    FlushActor();
    ASSERT_AWAIT_READY(blocked);
    EXPECT_EQ(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY),
              static_cast<int32_t>(blocked.Get().code()));

    // (b) replay settles, then a FIRST-attempt proven-not-started failure
    // on a NEW request legitimately Fails
    PublicChainAgentProbe::TypedOutcome healthy;
    agentProbe_->SetTypedOutcome(healthy);
    auto replay = PublicSnapshot("pub-a");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(replay);
    EXPECT_EQ(common::ERR_NONE, replay.Get().code());

    PublicChainAgentProbe::TypedOutcome proven;
    proven.code = common::ERR_INNER_COMMUNICATION;
    proven.checkpointNotStarted = true;
    agentProbe_->SetTypedOutcome(proven);
    auto c = PublicSnapshot("pub-c");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(c);
    EXPECT_NE(common::ERR_NONE, c.Get().code());
    EXPECT_EQ(1, schedProbe_->FailCalls());  // proven never started: Fail allowed
}

TEST_F(SnapCtrlPublicChainTest, ConcurrentSameRequestReplayIsSingleFlight)
{
    PublicChainAgentProbe::TypedOutcome unknown;
    unknown.code = common::ERR_INNER_COMMUNICATION;
    unknown.resultUnknown = true;
    agentProbe_->SetTypedOutcome(unknown);
    auto first = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(first);
    EXPECT_NE(common::ERR_NONE, first.Get().code());

    PublicChainAgentProbe::TypedOutcome healthy;
    agentProbe_->SetTypedOutcome(healthy);
    auto replayA = PublicSnapshot("pub-1");
    FlushActor();
    ASSERT_TRUE(runtimeClient_->WaitForPrepareEntered());  // replay A in flight

    auto replayB = PublicSnapshot("pub-1");  // same requestID, second re-query
    FlushActor();
    ASSERT_AWAIT_READY(replayB);
    EXPECT_EQ(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY),
              static_cast<int32_t>(replayB.Get().code()));

    runtimeClient_->SettlePrepare(common::ERR_NONE, "");
    ASSERT_AWAIT_READY(replayA);
    EXPECT_EQ(common::ERR_NONE, replayA.Get().code());
}

TEST_F(SnapCtrlPublicChainTest, ReplayWithChangedParametersIsRefused)
{
    PublicChainAgentProbe::TypedOutcome unknown;
    unknown.code = common::ERR_INNER_COMMUNICATION;
    unknown.resultUnknown = true;
    agentProbe_->SetTypedOutcome(unknown);
    auto first = PublicSnapshot("pub-1", "name-original", 300000);
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(first);
    EXPECT_NE(common::ERR_NONE, first.Get().code());

    // same requestID, DIFFERENT name/timeout: never adopted
    auto changed = PublicSnapshot("pub-1", "name-changed", 600000);
    FlushActor();
    ASSERT_AWAIT_READY(changed);
    EXPECT_EQ(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY),
              static_cast<int32_t>(changed.Get().code()));
    EXPECT_EQ(1, schedProbe_->BeginCalls());  // no second Begin was driven
}

TEST_F(SnapCtrlPublicChainTest, ReplayWithChangedSourceRuntimeIsRefused)
{
    PublicChainAgentProbe::TypedOutcome unknown;
    unknown.code = common::ERR_INNER_COMMUNICATION;
    unknown.resultUnknown = true;
    agentProbe_->SetTypedOutcome(unknown);
    auto first = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(first);
    EXPECT_NE(common::ERR_NONE, first.Get().code());

    UpdateInstanceRuntimeID("runtime-replaced-2");
    auto replay = PublicSnapshot("pub-1");
    FlushActor();
    ASSERT_AWAIT_READY(replay);
    EXPECT_EQ(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY),
              static_cast<int32_t>(replay.Get().code()));
    EXPECT_EQ(1, schedProbe_->BeginCalls());
}

TEST_F(SnapCtrlPublicChainTest, ReplayProvenNotStartedRetainsUnknownAndNeverFails)
{
    PublicChainAgentProbe::TypedOutcome unknown;
    unknown.code = common::ERR_INNER_COMMUNICATION;
    unknown.resultUnknown = true;
    agentProbe_->SetTypedOutcome(unknown);
    auto first = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(first);
    EXPECT_NE(common::ERR_NONE, first.Get().code());

    // the replay's checkpoint is PROVEN not started — that only describes
    // this attempt; the OLD unknown outcome and its record must survive
    PublicChainAgentProbe::TypedOutcome provenNotStarted;
    provenNotStarted.code = common::ERR_INNER_COMMUNICATION;
    provenNotStarted.checkpointNotStarted = true;
    agentProbe_->SetTypedOutcome(provenNotStarted);
    auto replay = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(replay);
    EXPECT_NE(common::ERR_NONE, replay.Get().code());
    EXPECT_EQ(0, schedProbe_->FailCalls());

    auto other = PublicSnapshot("pub-9");
    FlushActor();
    ASSERT_AWAIT_READY(other);
    EXPECT_EQ(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY),
              static_cast<int32_t>(other.Get().code()));
}

TEST_F(SnapCtrlPublicChainTest, SettledReplayFreesBothGateAndReplayEntrance)
{
    PublicChainAgentProbe::TypedOutcome unknown;
    unknown.code = common::ERR_INNER_COMMUNICATION;
    unknown.resultUnknown = true;
    agentProbe_->SetTypedOutcome(unknown);
    auto first = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(first);
    EXPECT_NE(common::ERR_NONE, first.Get().code());

    PublicChainAgentProbe::TypedOutcome healthy;
    agentProbe_->SetTypedOutcome(healthy);
    auto replay = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(replay);
    EXPECT_EQ(common::ERR_NONE, replay.Get().code());

    // after the authoritative settlement the SAME requestID is an ordinary
    // NEW public request (fresh attempt, fresh Begin) and succeeds — the
    // stale attempt/entrance cannot linger
    auto again = PublicSnapshot("pub-1");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(again);
    EXPECT_EQ(common::ERR_NONE, again.Get().code());
    EXPECT_EQ(3, schedProbe_->BeginCalls());  // first + replay + fresh
}

// A LATE completion from a STALE attempt context must not disturb the
// CURRENT unknown entry: while a fresh UNKNOWN entry exists (attempt B),
// an old context (attempt A) completes through the REAL actor method —
// the guard must leave the gate busy, the current entry intact, and new
// requests still blocked. Uses -fno-access-control (already set for this
// target) to build a context and call the real completion path on the
// actor thread via litebus::Async; nothing is re-implemented here.
TEST_F(SnapCtrlPublicChainTest, StaleAttemptLateCompletionTouchesNothing)
{
    // 1) a FIRST real public request completes successfully: its attempt id
    //    (read from the live entry machinery) becomes the STALE id below
    PublicChainAgentProbe::TypedOutcome healthy;
    agentProbe_->SetTypedOutcome(healthy);
    auto settled = PublicSnapshot("pub-settled");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(settled);
    EXPECT_EQ(common::ERR_NONE, settled.Get().code());
    FlushActor();
    ASSERT_EQ(0u, actor_->unknownReusable_.size());

    // 2) a SECOND real public request lands UNKNOWN and owns the live slot
    PublicChainAgentProbe::TypedOutcome unknown;
    unknown.code = common::ERR_INNER_COMMUNICATION;
    unknown.resultUnknown = true;
    agentProbe_->SetTypedOutcome(unknown);
    auto second = PublicSnapshot("pub-unknown");
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(second);
    EXPECT_NE(common::ERR_NONE, second.Get().code());
    FlushActor();
    ASSERT_EQ(1u, actor_->unknownReusable_.size());
    const auto liveEntry = actor_->unknownReusable_.at(INSTANCE_ID);
    const uint64_t liveAttempt = liveEntry.attempt;
    const std::string liveRequestID = liveEntry.requestID;

    // 3) the STALE attempt is a DIFFERENT, earlier id — proven, not assumed
    ASSERT_GE(liveAttempt, 2u);  // the settled first request consumed id 1..
    const uint64_t staleAttempt = liveAttempt - 1;
    ASSERT_NE(staleAttempt, liveAttempt);
    auto lateContext = std::make_shared<SnapCtrlActor::ReusableSnapshotContext>();
    lateContext->requestID = "pub-settled";  // the FIRST request's id
    lateContext->instanceID = INSTANCE_ID;
    lateContext->snapshotAttempt = staleAttempt;
    lateContext->sourceInstanceInfo = MakePublicInstanceInfo();
    lateContext->completion = std::make_shared<litebus::Promise<KillResponse>>();
    lateContext->outcomeKnown = true;  // even a KNOWN stale outcome must not release

    auto driven = litebus::Async(actor_->GetAID(), &SnapCtrlActor::CompleteReusableSnapshotRequest,
                                 lateContext, KillResponse{});
    FlushActor();
    ASSERT_AWAIT_READY(driven);
    FlushActor();

    // 4) the live entry survived the stale completion UNCHANGED
    ASSERT_EQ(1u, actor_->unknownReusable_.size());
    const auto afterEntry = actor_->unknownReusable_.at(INSTANCE_ID);
    EXPECT_EQ(liveAttempt, afterEntry.attempt);
    EXPECT_EQ(liveRequestID, afterEntry.requestID);

    // 5) new requests stay busy; the pinned request can still re-query
    auto other = PublicSnapshot("pub-other");
    FlushActor();
    ASSERT_AWAIT_READY(other);
    EXPECT_EQ(static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY),
              static_cast<int32_t>(other.Get().code()));

    agentProbe_->SetTypedOutcome(healthy);
    auto replay = PublicSnapshot(liveRequestID);
    FlushActor();
    SettlePrepareOk();
    ASSERT_AWAIT_READY(replay);
    EXPECT_EQ(common::ERR_NONE, replay.Get().code());
}

}  // namespace
}  // namespace functionsystem::test
