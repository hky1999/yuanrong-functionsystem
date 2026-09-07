#include "local_scheduler/snap_ctrl/snap_ctrl_actor.h"

#include <atomic>
#include <memory>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "mocks/mock_control_interface_client_manager_proxy.h"
#include "mocks/mock_instance_control_view.h"
#include "mocks/mock_instance_state_machine.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {
using namespace ::testing;
using local_scheduler::SnapCtrlActor;

namespace {

using MockMachine = NiceMock<MockInstanceStateMachine>;
using MockView = NiceMock<MockInstanceControlView>;

::resources::InstanceInfo MakeSourceInfo()
{
    ::resources::InstanceInfo info;
    info.set_instanceid("instance");
    info.set_runtimeid("runtime");
    return info;
}

std::shared_ptr<MockMachine> MakeRunningMachine()
{
    auto machine = std::make_shared<MockMachine>("node");
    ON_CALL(*machine, GetInstanceState()).WillByDefault(Return(::functionsystem::InstanceState::RUNNING));
    ON_CALL(*machine, GetInstanceInfo()).WillByDefault(Return(MakeSourceInfo()));
    return machine;
}

// Complete conditional-park payload: full expected tuple plus a valid park
// hold token.
std::string FullExpectedPayload(const std::string &expectedRuntimeID = "runtime")
{
    ::core_service::SnapOptions options;
    options.set_expected_runtime_id(expectedRuntimeID);
    options.set_expected_sandbox_id("sandbox");
    options.set_expected_resource_generation("generation");
    options.set_hold_token("physical-hold-token");
    options.set_leaverunning(false);
    return options.SerializeAsString();
}

// Bind the view so GetInstance("instance") always returns the current value
// of the caller's mutable pointer.
void BindView(SnapCtrlActor &actor, std::shared_ptr<::functionsystem::InstanceStateMachine> &current)
{
    auto view = std::make_shared<MockView>("node");
    ON_CALL(*view, GetInstance("instance")).WillByDefault(Invoke([&current](const std::string &) { return current; }));
    actor.BindInstanceControlView(view);
}

// Only the prepare dispatch is under test: avoid changing the process-wide
// parked-registry wake provider from this isolated actor fixture.
class PrepareDispatchActor : public SnapCtrlActor {
public:
    using SnapCtrlActor::SnapCtrlActor;
    void Init() override {}
    void Finalize() override {}
};

class PrepareDispatchLifetime {
public:
    explicit PrepareDispatchLifetime(std::shared_ptr<PrepareDispatchActor> actor) : actor_(std::move(actor))
    {
        litebus::Spawn(actor_);
    }
    ~PrepareDispatchLifetime()
    {
        litebus::Terminate(actor_->GetAID());
        litebus::Await(actor_);
    }
private:
    std::shared_ptr<PrepareDispatchActor> actor_;
};

// Control-plane client mock for the runtime-pinned prepare dispatch: the
// legacy and pinned entry points are mocked SEPARATELY, so a silent fallback
// to legacy PrepareSnap on the conditional path fails the test instead of
// silently passing it.
class MockPrepareSnapClient : public ControlInterfacePosixClient {
public:
    MockPrepareSnapClient() : BaseClient(nullptr), ControlInterfacePosixClient(nullptr)
    {
    }
    ~MockPrepareSnapClient() override = default;

    MOCK_METHOD(litebus::Future<runtime::PrepareSnapResponse>, PrepareSnap,
                (runtime::PrepareSnapRequest &&request), (override));
    MOCK_METHOD(litebus::Future<runtime::PrepareSnapResponse>, PrepareSnapForRuntime,
                (runtime::PrepareSnapRequest &&request, const std::string &expectedRuntimeID), (override));
};
}  // namespace

TEST(SnapCtrlSourceTest, SourceMatchesRequiresCurrentRunningSourceOnSameRuntime)
{
    SnapCtrlActor actor("source-match-test", "node");
    auto source = MakeRunningMachine();
    std::shared_ptr<::functionsystem::InstanceStateMachine> current = source;
    BindView(actor, current);

    EXPECT_TRUE(actor.SnapshotSourceMatches("instance", source, "runtime"));
    EXPECT_FALSE(actor.SnapshotSourceMatches("instance", nullptr, "runtime"));
    EXPECT_FALSE(actor.SnapshotSourceMatches("instance", source, "other-runtime"));
    EXPECT_FALSE(actor.SnapshotSourceMatches("instance", source, ""));

    ON_CALL(*source, GetInstanceState()).WillByDefault(Return(::functionsystem::InstanceState::EXITED));
    EXPECT_FALSE(actor.SnapshotSourceMatches("instance", source, "runtime"));
    ON_CALL(*source, GetInstanceState()).WillByDefault(Return(::functionsystem::InstanceState::RUNNING));

    // empty view: the lookup finds no machine for the instance
    SnapCtrlActor emptyViewActor("source-empty-view-test", "node");
    std::shared_ptr<::functionsystem::InstanceStateMachine> absent = nullptr;
    BindView(emptyViewActor, absent);
    EXPECT_FALSE(emptyViewActor.SnapshotSourceMatches("instance", source, "runtime"));

    // unbound view rejects instead of touching a null dependency
    SnapCtrlActor unboundActor("source-unbound-view-test", "node");
    EXPECT_FALSE(unboundActor.SnapshotSourceMatches("instance", source, "runtime"));
}

TEST(SnapCtrlSourceTest, ReplacementMachineWithSameIdentityRejectsCapturedSource)
{
    SnapCtrlActor actor("source-replace-test", "node");
    auto source = MakeRunningMachine();
    auto replacement = MakeRunningMachine();
    std::shared_ptr<::functionsystem::InstanceStateMachine> current = source;
    BindView(actor, current);

    EXPECT_TRUE(actor.SnapshotSourceMatches("instance", source, "runtime"));
    current = replacement;  // same instanceID/runtimeID, different machine

    EXPECT_FALSE(actor.SnapshotSourceMatches("instance", source, "runtime"));
    EXPECT_TRUE(actor.SnapshotSourceMatches("instance", replacement, "runtime"));
    auto future = actor.HandleSnapshotIfSource("r", "instance", FullExpectedPayload(), source);
    ASSERT_TRUE(future.IsOK());
    EXPECT_EQ(future.Get().code(), common::ERR_INSTANCE_BUSY);
    // rejection preceded every mutation: gate free, no park bookkeeping, and
    // the unbound client/manager dependencies were never reached (they would
    // have aborted the process)
    EXPECT_FALSE(actor.snapshotAttempts_.Busy("instance"));
    EXPECT_TRUE(actor.parkedInstances_.empty());
}

TEST(SnapCtrlSourceTest, HandleSnapshotRejectsInvalidParkPayloadBeforeViewAssertion)
{
    // no control view bound: each payload rejection below happens before the
    // unbound-view assertion, and the busy gate is never taken
    SnapCtrlActor actor("source-payload-test", "node");
    const auto expectRejected = [&actor](const std::string &payload) {
        auto future = actor.HandleSnapshot("r", "instance", payload);
        ASSERT_TRUE(future.IsOK());
        EXPECT_EQ(future.Get().code(), common::ERR_PARAM_INVALID);
        EXPECT_FALSE(actor.snapshotAttempts_.Busy("instance"));
    };

    ::core_service::SnapOptions tokenOnly;
    tokenOnly.set_hold_token("physical-hold-token");
    expectRejected(tokenOnly.SerializeAsString());  // token without the identity tuple

    ::core_service::SnapOptions partial;
    partial.set_expected_runtime_id("runtime");
    expectRejected(partial.SerializeAsString());  // incomplete tuple

    ::core_service::SnapOptions blank;
    blank.set_expected_runtime_id("runtime");
    blank.set_expected_sandbox_id("   ");
    blank.set_expected_resource_generation("generation");
    expectRejected(blank.SerializeAsString());  // blank tuple field

    ::core_service::SnapOptions oversized;
    oversized.set_expected_runtime_id("runtime");
    oversized.set_expected_sandbox_id(std::string(257, 'x'));
    oversized.set_expected_resource_generation("generation");
    expectRejected(oversized.SerializeAsString());  // unbounded tuple field

    ::core_service::SnapOptions tokenWithLeaveRunning;
    tokenWithLeaveRunning.set_expected_runtime_id("runtime");
    tokenWithLeaveRunning.set_expected_sandbox_id("sandbox");
    tokenWithLeaveRunning.set_expected_resource_generation("generation");
    tokenWithLeaveRunning.set_hold_token("physical-hold-token");
    tokenWithLeaveRunning.set_leaverunning(true);
    expectRejected(tokenWithLeaveRunning.SerializeAsString());  // hold token cannot ride a leave-running snapshot

    expectRejected(std::string("\xff\xff\xff\xff", 4));  // malformed payload
}

TEST(SnapCtrlSourceTest, ConditionalSnapshotRejectsNullSourceAndRuntimeMismatchBeforeGate)
{
    SnapCtrlActor actor("source-reject-test", "node");
    auto source = MakeRunningMachine();
    std::shared_ptr<::functionsystem::InstanceStateMachine> current = source;
    BindView(actor, current);

    auto nullSource = actor.HandleSnapshotIfSource("r", "instance", FullExpectedPayload(), nullptr);
    ASSERT_TRUE(nullSource.IsOK());
    EXPECT_EQ(nullSource.Get().code(), common::ERR_INSTANCE_BUSY);
    EXPECT_FALSE(actor.snapshotAttempts_.Busy("instance"));

    auto mismatch = actor.HandleSnapshotIfSource("r", "instance", FullExpectedPayload("other-runtime"), source);
    ASSERT_TRUE(mismatch.IsOK());
    EXPECT_EQ(mismatch.Get().code(), common::ERR_INSTANCE_BUSY);
    EXPECT_FALSE(actor.snapshotAttempts_.Busy("instance"));
    EXPECT_TRUE(actor.parkedInstances_.empty());
}

TEST(SnapCtrlSourceTest, ValidConditionalEntryStopsAtExistingBusyGate)
{
    SnapCtrlActor actor("source-gate-test", "node");
    auto source = MakeRunningMachine();
    std::shared_ptr<::functionsystem::InstanceStateMachine> current = source;
    BindView(actor, current);

    EXPECT_TRUE(actor.SnapshotSourceMatches("instance", source, "runtime"));

    // preseed the gate so the wrapped HandleSnapshot hits the single-flight
    // guard instead of the unbound physical dependencies; guard-only positive
    // control, NOT a real snapshot success
    const auto preseeded = actor.snapshotAttempts_.Begin("instance");
    ASSERT_TRUE(preseeded.has_value());

    auto future = actor.HandleSnapshotIfSource("r", "instance", FullExpectedPayload(), source);
    ASSERT_TRUE(future.IsOK());
    EXPECT_EQ(future.Get().code(), common::ERR_INSTANCE_BUSY);  // busy gate, not a param rejection
    EXPECT_TRUE(actor.snapshotAttempts_.Matches("instance", preseeded.value()));
    EXPECT_TRUE(actor.parkedInstances_.empty());
}

TEST(SnapCtrlSourceTest, MismatchedSourceFailureRetainsAttemptOwnership)
{
    SnapCtrlActor actor("source-finish-test", "node");
    const auto attempt = actor.snapshotAttempts_.Begin("instance");
    ASSERT_TRUE(attempt.has_value());
    const auto dispatched = std::make_shared<std::atomic<bool>>(false);
    const auto sourceMismatched = std::make_shared<std::atomic<bool>>(true);
    litebus::Future<KillResponse> failed;
    failed.SetFailed(litebus::Status::KERROR);
    // the drain/mark may belong to a replacement source now: even an
    // undispatched failure keeps the attempt fenced for reconciliation
    actor.FinishSnapshotAttempt("instance", attempt.value(), false, dispatched, failed, sourceMismatched);
    EXPECT_TRUE(actor.snapshotAttempts_.Unknown("instance"));
    EXPECT_FALSE(actor.snapshotAttempts_.Begin("instance").has_value());
}

TEST(SnapCtrlSourceTest, ConditionalPrepareSnapDispatchesOnlyTheRuntimePinnedEntry)
{
    // Spawned actor: the client-lookup continuation is deferred onto the
    // actor's AID, so the dispatch chain needs a live actor to run on.
    auto actor = std::make_shared<PrepareDispatchActor>("prepare-dispatch-test", "node");
    auto source = MakeRunningMachine();
    std::shared_ptr<::functionsystem::InstanceStateMachine> current = source;
    BindView(*actor, current);

    auto client = std::make_shared<MockPrepareSnapClient>();
    std::shared_ptr<ControlInterfacePosixClient> controlClient = client;
    auto manager = std::make_shared<MockControlInterfaceClientManagerProxy>();
    EXPECT_CALL(*manager, GetControlInterfacePosixClient("instance")).WillRepeatedly(Return(controlClient));
    actor->BindClientManager(manager);
    // Stop the actor before referenced view state and mocks unwind, even on ASSERT failure.
    PrepareDispatchLifetime lifetime(actor);

    const std::string requestID = "r";
    const std::string instanceID = "instance";
    const std::string runtimeID = "runtime";
    const std::shared_ptr<::functionsystem::InstanceStateMachine> capturedSource = source;
    const std::shared_ptr<::functionsystem::InstanceStateMachine> noSource;
    const std::shared_ptr<std::atomic<bool>> noLatch;

    // conditional dispatch: ONLY the pinned entry, carrying the caller's
    // captured runtime — a legacy fallback fails through Times(0)
    runtime::PrepareSnapResponse ok;
    ok.set_code(common::ERR_NONE);
    EXPECT_CALL(*client, PrepareSnap(_)).Times(0);
    EXPECT_CALL(*client, PrepareSnapForRuntime(_, runtimeID)).WillOnce(Return(ok));
    auto pinned = litebus::Async(actor->GetAID(), &SnapCtrlActor::PrepareSnap, requestID, instanceID, capturedSource,
                                 runtimeID, noLatch);
    ASSERT_AWAIT_READY(pinned);
    EXPECT_TRUE(pinned.Get().IsOk());

    // a pinned rejection (transport retagged/closed under the instance slot)
    // stays a failure: no retry through the legacy entry point
    runtime::PrepareSnapResponse busy;
    busy.set_code(common::ERR_INSTANCE_BUSY);
    busy.set_message("prepare snap rejected: transport no longer bound to the expected runtime");
    EXPECT_CALL(*client, PrepareSnap(_)).Times(0);
    EXPECT_CALL(*client, PrepareSnapForRuntime(_, runtimeID)).WillOnce(Return(busy));
    auto rejected = litebus::Async(actor->GetAID(), &SnapCtrlActor::PrepareSnap, requestID, instanceID, capturedSource,
                                   runtimeID, noLatch);
    ASSERT_AWAIT_READY(rejected);
    EXPECT_TRUE(rejected.Get().IsError());

    // unconditional path (no captured source) keeps the legacy entry point
    EXPECT_CALL(*client, PrepareSnapForRuntime(_, _)).Times(0);
    EXPECT_CALL(*client, PrepareSnap(_)).WillOnce(Return(ok));
    auto legacy = litebus::Async(actor->GetAID(), &SnapCtrlActor::PrepareSnap, requestID, instanceID, noSource,
                                 std::string(), noLatch);
    ASSERT_AWAIT_READY(legacy);
    EXPECT_TRUE(legacy.Get().IsOk());

}
}  // namespace functionsystem::test
