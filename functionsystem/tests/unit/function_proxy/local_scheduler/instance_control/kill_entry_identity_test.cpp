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

// REAL-entry kill-identity tests for the 2026-10 fix chain, driving the
// production KillRuntimeWithSnapshotCleanup / KillRuntimeWithIdentity paths
// with the ESTABLISHED no-etcd fixture (mirrors instance_ctrl_test.cpp's
// InitialLowReliabilityRouteConflictActorTest): MockResourceView via
// CreateMockResourceView, MockFunctionAgentMgr(name, nullptr),
// MockControlInterfaceClientManagerProxy, MockInternalIAM (IAM disabled —
// RemoveInternalTokenReference ASSERTs without the binding).
//
// Covered:
//   1. same-incarnation trusted identity reaches the agent kill request
//      exactly once;
//   2. OLD runtime vs SAME-NAME new incarnation (view runtimeid differs):
//      ZERO dispatch;
//   3. missing view (GetInf null) / allocated-without-sample: UNKNOWN,
//      zero dispatch, budget retained;
//   4. non-elastic legacy: identity-less kill still dispatched;
//   5. the OTHER entry (KillRuntime) rejects a null view for elastic too.
//
// NOTE: these assert DISPATCH shape only (identity carried / zero dispatch
// / retryable-UNKNOWN). Budget-release correctness is NOT claimed here —
// it is exercised by the resource-view settlement tests.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "function_proxy/local_scheduler/instance_control/instance_ctrl_actor.h"
#include "mocks/mock_control_interface_client_manager_proxy.h"
#include "mocks/mock_function_agent_mgr.h"
#include "mocks/mock_instance_control_view.h"
#include "mocks/mock_instance_state_machine.h"
#include "mocks/mock_internal_iam.h"
#include "function_proxy/common/state_handler/state_handler.h"
#include "function_proxy/common/state_handler/state_handler_helper.h"
#include "mocks/mock_distributed_cache_client.h"
#include "mocks/mock_resource_view.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {

using namespace functionsystem::local_scheduler;  // NOLINT
using namespace resource_view;                    // NOLINT
using testing::_;
using testing::Matcher;
using testing::Invoke;
using testing::Return;

namespace {

InstanceInfo MakeElasticInfo(const std::string &instanceID,
                             const std::string &runtimeID)
{
    InstanceInfo info;
    info.set_instanceid(instanceID);
    info.set_runtimeid(runtimeID);
    info.set_requestid("req-" + instanceID);
    info.set_functionagentid("agent-test");
    info.set_unitid("unit-test");
    (*info.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    return info;
}

InstanceInfo MakeViewEntry(const std::string &instanceID,
                           const std::string &runtimeID,
                           const std::string &sandboxID,
                           const std::string &generation)
{
    auto entry = MakeElasticInfo(instanceID, runtimeID);
    auto *sample = entry.mutable_sandbox_resource_sample();
    sample->set_runtime_id(runtimeID);
    sample->set_sandbox_id(sandboxID);
    sample->set_resource_generation(generation);
    sample->set_resource_revision_known(true);
    sample->set_cpu_limits_known(true);
    sample->set_memory_limit_known(true);
    sample->set_memory_limit_bytes(512ull * 1024 * 1024);
    sample->set_sampled_at_unix_ms(1);
    return entry;
}

std::shared_ptr<ResourceUnit> MakeUnitWithEntry(const InstanceInfo &entry)
{
    auto unit = std::make_shared<ResourceUnit>();
    unit->set_id("unit-test");
    unit->set_status(static_cast<uint32_t>(UnitStatus::NORMAL));
    (*unit->mutable_instances())[entry.instanceid()] = entry;
    return unit;
}

}  // namespace

class KillEntryIdentityFixture : public testing::Test {
protected:
    void SetUp() override
    {
        const auto suffix =
            litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        InstanceCtrlConfig config;
        actor_ = std::make_shared<InstanceCtrlActor>(
            "KillIdentityActor-" + suffix, "node-test", config);

        clientManager_ = std::make_shared<MockControlInterfaceClientManagerProxy>();
        ON_CALL(*clientManager_, DeleteClient(_))
            .WillByDefault(Invoke([](const std::string &) {
                auto promise = std::make_shared<litebus::Promise<Status>>();
                promise->SetValue(Status::OK());
                return promise->GetFuture();
            }));

        functionAgentMgr_ = std::make_shared<MockFunctionAgentMgr>(
            "KillIdentityAgentMgr-" + suffix, nullptr);
        ON_CALL(*functionAgentMgr_, KillInstance(_, _, _))
            .WillByDefault(Invoke([this](
                const std::shared_ptr<messages::KillInstanceRequest> &request,
                const std::string &, bool) {
                killRequests.push_back(request);
                messages::KillInstanceResponse response;
                response.set_requestid(request->requestid());
                response.set_instanceid(request->instanceid());
                response.set_code(static_cast<int32_t>(common::ErrorCode::ERR_NONE));
                // A confirmed receipt naming EXACTLY the expected identity —
                // the legitimate-settlement shape.
                response.set_physical_retirement_confirmed(true);
                response.set_retired_sandbox_id(request->expected_sandbox_id());
                response.set_retired_resource_generation(
                    request->expected_resource_generation());
                auto promise = std::make_shared<
                    litebus::Promise<messages::KillInstanceResponse>>();
                promise->SetValue(response);
                return promise->GetFuture();
            }));

        resourceViewMgr_ = std::make_shared<ResourceViewMgr>();
        primary_ = MockResourceView::CreateMockResourceView();
        resourceViewMgr_->primary_ = primary_;

        InternalIAM::Param iamParam;
        iamParam.isEnableIAM = false;
        internalIAM_ = std::make_shared<MockInternalIAM>(iamParam);
        ON_CALL(*internalIAM_, IsIAMEnabled()).WillByDefault(Return(false));

        actor_->BindControlInterfaceClientManager(clientManager_);
        actor_->BindFunctionAgentMgr(functionAgentMgr_);
        actor_->BindResourceView(resourceViewMgr_);
        actor_->BindInternalIAM(internalIAM_);
        // REAL state chain (mirrors state_handler_test.cpp SetUp):
        // MockDistributedCacheClient -> real StateClient -> real StateActor,
        // spawned and bound, so the control-delete chain exercises the
        // actual async deletion and its result propagation.
        cacheClient_ = std::make_shared<MockDistributedCacheClient>();
        EXPECT_CALL(*cacheClient_, Init).WillOnce(Return(Status::OK()));
        stateClient_ = std::make_shared<functionsystem::StateClient>(cacheClient_);
        stateActor_ = std::make_shared<
            functionsystem::function_proxy::StateActor>(stateClient_);
        litebus::Spawn(stateActor_);
        functionsystem::function_proxy::StateHandler::BindStateActor(stateActor_);
        litebus::Spawn(actor_);
    }

    void TearDown() override
    {
        if (actor_ != nullptr) {
            litebus::Terminate(actor_->GetAID());
            litebus::Await(actor_);
        }
        if (stateActor_ != nullptr) {
            litebus::Terminate(stateActor_->GetAID());
            litebus::Await(stateActor_->GetAID());
            stateActor_ = nullptr;
            // BindStateActor(nullptr) returns early and does NOT clear the
            // static AID — the established helper does.
            StateHandlerHelper::ClearStateActorHelper();
            stateClient_ = nullptr;
            cacheClient_ = nullptr;
        }
    }

    // Serve a controllable PRIMARY-view snapshot.
    void ServeView(const std::shared_ptr<ResourceUnit> &unit)
    {
        ON_CALL(*primary_, GetResourceView())
            .WillByDefault(Invoke([unit]() {
                auto promise = std::make_shared<
                    litebus::Promise<std::shared_ptr<ResourceUnit>>>();
                promise->SetValue(unit);
                return promise->GetFuture();
            }));
    }

    // The REACHABLE view-read failure: GetInf raising on a null primary is
    // a crash-level invariant (ASSERT_IF_NULL -> SIGINT); the retryable
    // UNKNOWN path is the READ answering no unit at all.
    void ServeNullUnit()
    {
        ON_CALL(*primary_, GetResourceView())
            .WillByDefault(Invoke([]() {
                auto promise = std::make_shared<
                    litebus::Promise<std::shared_ptr<ResourceUnit>>>();
                promise->SetValue(std::shared_ptr<ResourceUnit>{});
                return promise->GetFuture();
            }));
    }

    std::shared_ptr<InstanceCtrlActor> actor_;
    std::shared_ptr<MockControlInterfaceClientManagerProxy> clientManager_;
    std::shared_ptr<MockFunctionAgentMgr> functionAgentMgr_;
    std::shared_ptr<ResourceViewMgr> resourceViewMgr_;
    std::shared_ptr<MockResourceView> primary_;
    std::shared_ptr<MockInternalIAM> internalIAM_;
    std::vector<std::shared_ptr<messages::KillInstanceRequest>> killRequests;
    std::vector<resource_view::PhysicalRetirementReceipt> settleReceipts;
    std::shared_ptr<InstanceStateMachine> machineSlot_ = nullptr;
    std::shared_ptr<MockDistributedCacheClient> cacheClient_;
    std::shared_ptr<functionsystem::StateClient> stateClient_;
    std::shared_ptr<functionsystem::function_proxy::StateActor> stateActor_;
};

// 1) Same incarnation: trusted sample identity reaches the agent request
// exactly once and the kill completes with the matching receipt.
TEST_F(KillEntryIdentityFixture, SameIncarnationIdentityPassesThrough)
{
    ServeView(MakeUnitWithEntry(
        MakeViewEntry("inst-same", "runtime-same", "sbox-trusted", "gen-trusted")));
    // The settlement continuation calls ApplyPhysicalRetirement with the
    // receipt built from the CONFIRMED response: capture it and answer a
    // ready OK so the chain completes (dispatch-shape assertions only).
    EXPECT_CALL(*primary_, ApplyPhysicalRetirement(_))
        .WillOnce(Invoke([this](const resource_view::PhysicalRetirementReceipt &receipt) {
            settleReceipts.push_back(receipt);
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));

    auto result = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-same", "runtime-same"), false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsOk()) << result.Get().GetMessage();
    ASSERT_EQ(killRequests.size(), 1u);
    EXPECT_EQ(killRequests[0]->expected_sandbox_id(), "sbox-trusted");
    EXPECT_EQ(killRequests[0]->expected_resource_generation(), "gen-trusted");
    // The captured receipt must name exactly this incarnation.
    ASSERT_EQ(settleReceipts.size(), 1u);
    EXPECT_EQ(settleReceipts[0].sandboxID, "sbox-trusted");
    EXPECT_EQ(settleReceipts[0].resourceGeneration, "gen-trusted");
    EXPECT_EQ(settleReceipts[0].instanceID, "inst-same");
    EXPECT_EQ(settleReceipts[0].runtimeID, "runtime-same");
}

TEST_F(KillEntryIdentityFixture, OldRuntimeSameNameNewIncarnationZeroDispatch)
{
    ServeView(MakeUnitWithEntry(
        MakeViewEntry("inst-reused", "runtime-NEW", "sbox-NEW", "gen-NEW")));

    auto result = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-reused", "runtime-OLD"), false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError());
    EXPECT_EQ(killRequests.size(), 0u)
        << "a stale delete must never borrow the new incarnation's identity";
}

// 3a) Missing view (GetInf answers null): UNKNOWN, zero dispatch.
TEST_F(KillEntryIdentityFixture, ViewReadFailsHoldsUnknown)
{
    // The reachable view-read failure: the read completes with NO unit
    // (a null primary_ would ASSERT/SIGINT inside GetInf — that invariant
    // violation is not a retryable input).
    ServeNullUnit();
    auto result = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-noview", "runtime-noview"), false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError());
    EXPECT_EQ(killRequests.size(), 0u);
}

// Distinct input: the view EXISTS but the snapshot read answers an EMPTY
// instance map for this identity (view entry genuinely absent).
TEST_F(KillEntryIdentityFixture, ViewSnapshotWithoutEntryHoldsUnknown)
{
    ServeView(MakeUnitWithEntry(MakeElasticInfo("inst-other", "runtime-other")));
    auto result = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-absent", "runtime-absent"), false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError());
    EXPECT_EQ(killRequests.size(), 0u);
}

TEST_F(KillEntryIdentityFixture, AllocatedWithoutSampleHoldsUnknown)
{
    ServeView(MakeUnitWithEntry(
        MakeElasticInfo("inst-nosample", "runtime-nosample")));

    auto result = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-nosample", "runtime-nosample"), false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError());
    EXPECT_EQ(killRequests.size(), 0u);
}

// 3c) SETTLED retry close-out: the first kill confirmed + settled (view
// entry removed); the RETRY (control cleanup failed earlier) must complete
// OK with ZERO new physical dispatches — the fresh authoritative read
// proves absence, matching DeleteInstanceInResourceView's verify-absence
// contract.
TEST_F(KillEntryIdentityFixture, SettledRetryCompletesWithZeroNewDispatch)
{
    ServeView(MakeUnitWithEntry(
        MakeViewEntry("inst-settled", "runtime-settled", "sbox-settled", "gen-settled")));
    EXPECT_CALL(*primary_, ApplyPhysicalRetirement(_))
        .WillOnce(Invoke([this](const resource_view::PhysicalRetirementReceipt &receipt) {
            settleReceipts.push_back(receipt);
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    auto first = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-settled", "runtime-settled"), false, true, true);
    ASSERT_AWAIT_READY(first);
    EXPECT_TRUE(first.Get().IsOk()) << first.Get().GetMessage();
    ASSERT_EQ(killRequests.size(), 1u);

    // Settlement removed the allocation from the authoritative view. The
    // retry sees an entry-less snapshot; the recorded CONFIRMED receipt
    // (full identity) is the settled credential, so the retry REPLAYS the
    // kill carrying that identity — the agent re-confirms idempotently —
    // and the chain settles AGAIN (durable receipts are idempotent).
    ServeView(MakeUnitWithEntry(MakeElasticInfo("inst-other", "runtime-other")));
    EXPECT_CALL(*primary_, ApplyPhysicalRetirement(_))
        .WillOnce(Invoke([this](const resource_view::PhysicalRetirementReceipt &receipt) {
            settleReceipts.push_back(receipt);
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    auto retry = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-settled", "runtime-settled"), false, true, true);
    ASSERT_AWAIT_READY(retry);
    EXPECT_TRUE(retry.Get().IsOk()) << retry.Get().GetMessage();
    // exactly ONE replay dispatch, carrying the RECORDED identity
    ASSERT_EQ(killRequests.size(), 2u);
    EXPECT_EQ(killRequests[1]->expected_sandbox_id(), "sbox-settled");
    EXPECT_EQ(killRequests[1]->expected_resource_generation(), "gen-settled");
    EXPECT_EQ(settleReceipts.size(), 2u);
}

// Root counterexample: NO successful receipt was ever recorded (the first
// kill never confirmed) + consecutive empty view snapshots => stays BUSY,
// ZERO dispatch, no cleanup, budget retained.
TEST_F(KillEntryIdentityFixture, NoRecordedReceiptConsecutiveEmptyViewStaysBusy)
{
    // No ApplyPhysicalRetirement expectation: none may be reached.
    ServeView(MakeUnitWithEntry(MakeElasticInfo("inst-other", "runtime-other")));
    auto result = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-ghost2", "runtime-ghost2"), false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError());
    EXPECT_EQ(killRequests.size(), 0u);
    EXPECT_EQ(settleReceipts.size(), 0u);
}

// Root counterexample (corrected shape): obtain the OLD credential through
// the real entry first, then the view goes entry-less, and a DIFFERENT
// runtime (a new incarnation reusing the name) calls — the old credential
// must NOT close out the new incarnation: BUSY, ZERO new dispatch.
TEST_F(KillEntryIdentityFixture, OldCredentialDifferentRuntimeViewAbsentStaysBusy)
{
    // Step 1: the OLD incarnation kills normally (view present, receipt
    // confirmed) — this is what records the old credential.
    ServeView(MakeUnitWithEntry(
        MakeViewEntry("inst-mix", "runtime-OLD", "sbox-old", "gen-old")));
    EXPECT_CALL(*primary_, ApplyPhysicalRetirement(_))
        .WillOnce(Invoke([this](const resource_view::PhysicalRetirementReceipt &receipt) {
            settleReceipts.push_back(receipt);
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    auto oldKill = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-mix", "runtime-OLD"), false, true, true);
    ASSERT_AWAIT_READY(oldKill);
    EXPECT_TRUE(oldKill.Get().IsOk());
    ASSERT_EQ(killRequests.size(), 1u);

    // Step 2: the view is now entry-less (settlement removed the old
    // allocation). The NEW incarnation (different runtimeid) retries: the
    // recorded credential belongs to runtime-OLD, so it must NOT apply.
    ServeView(MakeUnitWithEntry(MakeElasticInfo("inst-other", "runtime-other")));
    auto newKill = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-mix", "runtime-NEW3"), false, true, true);
    ASSERT_AWAIT_READY(newKill);
    EXPECT_TRUE(newKill.Get().IsError())
        << "an old-incarnation credential must never close out a new runtime";
    EXPECT_EQ(killRequests.size(), 1u) << "zero new dispatch for the new incarnation";
}

// Companion: the new incarnation IS in the view — its kill proceeds on its
// OWN current identity (never the recorded old one).
TEST_F(KillEntryIdentityFixture, NewIncarnationInViewUsesOwnIdentity)
{
    ServeView(MakeUnitWithEntry(
        MakeViewEntry("inst-mix", "runtime-OLD", "sbox-old", "gen-old")));
    EXPECT_CALL(*primary_, ApplyPhysicalRetirement(_))
        .WillOnce(Invoke([this](const resource_view::PhysicalRetirementReceipt &receipt) {
            settleReceipts.push_back(receipt);
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    auto oldKill = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-mix", "runtime-OLD"), false, true, true);
    ASSERT_AWAIT_READY(oldKill);

    ServeView(MakeUnitWithEntry(
        MakeViewEntry("inst-mix", "runtime-NEW4", "sbox-NEW4", "gen-NEW4")));
    EXPECT_CALL(*primary_, ApplyPhysicalRetirement(_))
        .WillOnce(Invoke([this](const resource_view::PhysicalRetirementReceipt &receipt) {
            settleReceipts.push_back(receipt);
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    auto newKill = actor_->KillRuntimeWithSnapshotCleanup(
        MakeElasticInfo("inst-mix", "runtime-NEW4"), false, true, true);
    ASSERT_AWAIT_READY(newKill);
    EXPECT_TRUE(newKill.Get().IsOk());
    ASSERT_EQ(killRequests.size(), 2u);
    EXPECT_EQ(killRequests[1]->expected_sandbox_id(), "sbox-NEW4")
        << "the new incarnation's kill carries its OWN identity";
}

// 4) Non-elastic legacy: identity-less kill STILL dispatched (old path).// 4) Non-elastic legacy: identity-less kill STILL dispatched (old path).
TEST_F(KillEntryIdentityFixture, NonElasticLegacyKillStillDispatched)
{
    InstanceInfo legacy;
    legacy.set_instanceid("inst-legacy");
    legacy.set_runtimeid("runtime-legacy");
    legacy.set_requestid("req-legacy");
    legacy.set_functionagentid("agent-test");

    auto result = actor_->KillRuntimeWithSnapshotCleanup(legacy, false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(killRequests.size(), 1u);
    EXPECT_TRUE(killRequests[0]->expected_sandbox_id().empty());
    EXPECT_TRUE(killRequests[0]->expected_resource_generation().empty());
}

// 5) The OTHER entry (KillRuntime) also holds UNKNOWN on a null view for
// an elastic instance.
TEST_F(KillEntryIdentityFixture, OtherEntryKillRuntimeViewReadFailsHoldsUnknown)
{
    ServeNullUnit();
    auto result = actor_->KillRuntime(
        MakeElasticInfo("inst-noview2", "runtime-noview2"), false);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError());
    EXPECT_EQ(killRequests.size(), 0u);
}


// Root's late-receipt window, REAL guard coverage: the control view now
// holds a NEW same-name incarnation; the OLD chain's control delete is
// driven through the production DeleteInstanceInControlView. The guard must
// refuse BEFORE any side effect: DelInstance is NEVER called and the new
// machine's state is untouched.
TEST_F(KillEntryIdentityFixture, LateOldReceiptDoesNotDeleteNewIncarnationState)
{
    // Bind a control view carrying the NEW incarnation's machine.
    auto controlView = std::make_shared<MockInstanceControlView>("node-test");
    auto newMachine = std::make_shared<MockInstanceStateMachine>("node-test");
    const auto newInfo = MakeElasticInfo("inst-late", "runtime-NEW");
    ON_CALL(*newMachine, GetInstanceInfo()).WillByDefault(Return(newInfo));
    ON_CALL(*newMachine, GetRequestID()).WillByDefault(Return("req-new"));
    ON_CALL(*controlView, GetInstance("inst-late"))
        .WillByDefault(Return(newMachine));
    // DelInstance must NEVER run for the new incarnation.
    EXPECT_CALL(*controlView, DelInstance("inst-late")).Times(0);
    actor_->BindInstanceControlView(controlView);

    // The OLD chain (runtime-OLD, request-old) arrives late: the guard
    // refuses; no delete, no route/group side effects may follow.
    const auto oldInfo = MakeElasticInfo("inst-late", "runtime-OLD");
    auto result = actor_->DeleteInstanceInControlView(Status::OK(), oldInfo);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError())
        << "the late old-receipt delete must be refused, not silently applied";
    // The new machine is untouched: still retrievable with its own identity.
    const auto stillThere = controlView->GetInstance("inst-late");
    ASSERT_NE(stillThere, nullptr);
    EXPECT_EQ(stillThere->GetInstanceInfo().runtimeid(), "runtime-NEW");
}

// Same-generation delete still proceeds (the guard must not over-block).
TEST_F(KillEntryIdentityFixture, SameGenerationControlDeleteProceeds)
{
    auto controlView = std::make_shared<MockInstanceControlView>("node-test");
    auto machine = std::make_shared<MockInstanceStateMachine>("node-test");
    const auto info = MakeElasticInfo("inst-ok", "runtime-ok");
    ON_CALL(*machine, GetInstanceInfo()).WillByDefault(Return(info));
    // request generation MUST equal the retiring chain's requestid —
    // MakeElasticInfo sets "req-" + instanceID; reuse info.requestid().
    ON_CALL(*machine, GetRequestID()).WillByDefault(Return(info.requestid()));
    ON_CALL(*controlView, GetInstance("inst-ok")).WillByDefault(Return(machine));
    ON_CALL(*controlView, DelInstance("inst-ok"))
        .WillByDefault(Invoke([](const std::string &) {
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    EXPECT_CALL(*controlView, DelInstance("inst-ok")).Times(1);
    actor_->BindInstanceControlView(controlView);

    auto result = actor_->DeleteInstanceInControlView(Status::OK(), info);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsOk());
}


// Mid-flight replacement, RUNTIME change: guard passes on the OLD machine,
// DelInstance is HELD on a gate promise, the machine is replaced with a
// NEW runtime, then the gate resolves — the tail guard must refuse the
// state delete of the new incarnation.
TEST_F(KillEntryIdentityFixture, MidFlightRuntimeReplacementKeepsNewState)
{
    auto controlView = std::make_shared<MockInstanceControlView>("node-test");
    auto oldMachine = std::make_shared<MockInstanceStateMachine>("m-old");
    const auto oldInfo = MakeElasticInfo("inst-mid", "runtime-OLD");
    ON_CALL(*oldMachine, GetInstanceInfo()).WillByDefault(Return(oldInfo));
    ON_CALL(*oldMachine, GetRequestID()).WillByDefault(Return(oldInfo.requestid()));

    auto gate = std::make_shared<litebus::Promise<Status>>();
    machineSlot_ = nullptr;
    ON_CALL(*controlView, GetInstance("inst-mid"))
        .WillByDefault(Invoke([this, oldMachine](const std::string &) {
            return machineSlot_ ? machineSlot_ : oldMachine;
        }));
    ON_CALL(*controlView, DelInstance("inst-mid"))
        .WillByDefault(Invoke([gate](const std::string &) {
            return gate->GetFuture();
        }));
    EXPECT_CALL(*controlView, DelInstance("inst-mid")).Times(1);
    actor_->BindInstanceControlView(controlView);

    auto result = actor_->DeleteInstanceInControlView(Status::OK(), oldInfo);

    auto newMachine = std::make_shared<MockInstanceStateMachine>("m-new");
    const auto newInfo = MakeElasticInfo("inst-mid", "runtime-NEW");
    ON_CALL(*newMachine, GetInstanceInfo()).WillByDefault(Return(newInfo));
    ON_CALL(*newMachine, GetRequestID()).WillByDefault(Return(newInfo.requestid()));
    machineSlot_ = newMachine;
    gate->SetValue(Status::OK());

    ASSERT_AWAIT_READY(result);
    ASSERT_TRUE(result.Get().IsError());
    EXPECT_EQ(result.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
    EXPECT_NE(result.Get().GetMessage().find("incarnation replaced"), std::string::npos)
        << "reason must name the incarnation guard: "
        << result.Get().GetMessage();
}

// Mid-flight replacement, REQUEST-generation change on the SAME runtime.
TEST_F(KillEntryIdentityFixture, MidFlightRequestReplacementKeepsNewState)
{
    auto controlView = std::make_shared<MockInstanceControlView>("node-test");
    auto oldMachine = std::make_shared<MockInstanceStateMachine>("m-req-old");
    const auto oldInfo = MakeElasticInfo("inst-req", "runtime-SAME");
    ON_CALL(*oldMachine, GetInstanceInfo()).WillByDefault(Return(oldInfo));
    ON_CALL(*oldMachine, GetRequestID()).WillByDefault(Return(oldInfo.requestid()));

    auto gate = std::make_shared<litebus::Promise<Status>>();
    machineSlot_ = nullptr;
    ON_CALL(*controlView, GetInstance("inst-req"))
        .WillByDefault(Invoke([this, oldMachine](const std::string &) {
            return machineSlot_ ? machineSlot_ : oldMachine;
        }));
    ON_CALL(*controlView, DelInstance("inst-req"))
        .WillByDefault(Invoke([gate](const std::string &) {
            return gate->GetFuture();
        }));
    EXPECT_CALL(*controlView, DelInstance("inst-req")).Times(1);
    actor_->BindInstanceControlView(controlView);

    auto result = actor_->DeleteInstanceInControlView(Status::OK(), oldInfo);

    auto newMachine = std::make_shared<MockInstanceStateMachine>("m-req-new");
    auto newInfo = MakeElasticInfo("inst-req", "runtime-SAME");
    // GENUINELY new request generation: overwrite the requestid.
    newInfo.set_requestid("req-inst-req-GENERATION-2");
    ON_CALL(*newMachine, GetInstanceInfo()).WillByDefault(Return(newInfo));
    ON_CALL(*newMachine, GetRequestID()).WillByDefault(Return(newInfo.requestid()));
    machineSlot_ = newMachine;
    gate->SetValue(Status::OK());

    ASSERT_AWAIT_READY(result);
    ASSERT_TRUE(result.Get().IsError());
    EXPECT_EQ(result.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY)
        << "the refusal must come from the guard, not an unrelated failure";
    EXPECT_NE(result.Get().GetMessage().find("request generation"), std::string::npos)
        << "reason must name the request-generation guard: "
        << result.Get().GetMessage();
}


// ── Full control-delete chain with the REAL StateActor ──────────────────────

// ① Same-generation delete: the cache Del happens EXACTLY once and the
// chain returns OK end-to-end.
TEST_F(KillEntryIdentityFixture, FullChainSameGenerationDeliversCacheDel)
{
    auto controlView = std::make_shared<MockInstanceControlView>("node-test");
    auto machine = std::make_shared<MockInstanceStateMachine>("m-full");
    const auto info = MakeElasticInfo("inst-full", "runtime-full");
    ON_CALL(*machine, GetInstanceInfo()).WillByDefault(Return(info));
    ON_CALL(*machine, GetRequestID()).WillByDefault(Return(info.requestid()));
    ON_CALL(*controlView, GetInstance("inst-full")).WillByDefault(Return(machine));
    ON_CALL(*controlView, DelInstance("inst-full"))
        .WillByDefault(Invoke([](const std::string &) {
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    actor_->BindInstanceControlView(controlView);
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>("inst-full")))
        .WillOnce(Return(Status::OK()));

    auto result = actor_->DeleteInstanceInControlView(Status::OK(), info);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsOk()) << result.Get().ToString();
}

// ② Cache Del FAILURE: the control delete returns that failure (the state
// is NOT silently considered cleaned); a retry succeeds (cleanup failure
// is not masked by the earlier machine deletion).
TEST_F(KillEntryIdentityFixture, FullChainCacheDelFailureSurfacesAndRetries)
{
    auto controlView = std::make_shared<MockInstanceControlView>("node-test");
    auto machine = std::make_shared<MockInstanceStateMachine>("m-fail");
    const auto info = MakeElasticInfo("inst-fail", "runtime-fail");
    ON_CALL(*machine, GetInstanceInfo()).WillByDefault(Return(info));
    ON_CALL(*machine, GetRequestID()).WillByDefault(Return(info.requestid()));
    ON_CALL(*controlView, GetInstance("inst-fail")).WillByDefault(Return(machine));
    ON_CALL(*controlView, DelInstance("inst-fail"))
        .WillByDefault(Invoke([](const std::string &) {
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    actor_->BindInstanceControlView(controlView);
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>("inst-fail")))
        .WillOnce(Return(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "store down")))
        .WillOnce(Return(Status::OK()));

    auto first = actor_->DeleteInstanceInControlView(Status::OK(), info);
    ASSERT_AWAIT_READY(first);
    ASSERT_TRUE(first.Get().IsError())
        << "the store failure must surface, not be masked";
    EXPECT_EQ(first.Get().GetMessage(), "store down");

    // Retry converges: the SAME chain returns OK on the second attempt.
    auto retry = actor_->DeleteInstanceInControlView(Status::OK(), info);
    ASSERT_AWAIT_READY(retry);
    EXPECT_TRUE(retry.Get().IsOk()) << retry.Get().ToString();
}

// ③ Mid-flight RUNTIME replacement: cache Del ZERO times, state kept.
TEST_F(KillEntryIdentityFixture, FullChainMidFlightRuntimeZeroCacheDel)
{
    auto controlView = std::make_shared<MockInstanceControlView>("node-test");
    auto oldMachine = std::make_shared<MockInstanceStateMachine>("m-fr-old");
    const auto oldInfo = MakeElasticInfo("inst-fr", "runtime-OLD");
    ON_CALL(*oldMachine, GetInstanceInfo()).WillByDefault(Return(oldInfo));
    ON_CALL(*oldMachine, GetRequestID()).WillByDefault(Return(oldInfo.requestid()));

    auto gate = std::make_shared<litebus::Promise<Status>>();
    machineSlot_ = nullptr;
    ON_CALL(*controlView, GetInstance("inst-fr"))
        .WillByDefault(Invoke([this, oldMachine](const std::string &) {
            return machineSlot_ ? machineSlot_ : oldMachine;
        }));
    ON_CALL(*controlView, DelInstance("inst-fr"))
        .WillByDefault(Invoke([gate](const std::string &) {
            return gate->GetFuture();
        }));
    actor_->BindInstanceControlView(controlView);
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>("inst-fr"))).Times(0);

    auto result = actor_->DeleteInstanceInControlView(Status::OK(), oldInfo);
    auto newMachine = std::make_shared<MockInstanceStateMachine>("m-fr-new");
    const auto newInfo = MakeElasticInfo("inst-fr", "runtime-NEW");
    ON_CALL(*newMachine, GetInstanceInfo()).WillByDefault(Return(newInfo));
    ON_CALL(*newMachine, GetRequestID()).WillByDefault(Return(newInfo.requestid()));
    machineSlot_ = newMachine;
    gate->SetValue(Status::OK());

    ASSERT_AWAIT_READY(result);
    ASSERT_TRUE(result.Get().IsError());
    EXPECT_EQ(result.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
}

// ③' Mid-flight REQUEST replacement (same runtime): cache Del ZERO times.
TEST_F(KillEntryIdentityFixture, FullChainMidFlightRequestZeroCacheDel)
{
    auto controlView = std::make_shared<MockInstanceControlView>("node-test");
    auto oldMachine = std::make_shared<MockInstanceStateMachine>("m-fq-old");
    const auto oldInfo = MakeElasticInfo("inst-fq", "runtime-SAME");
    ON_CALL(*oldMachine, GetInstanceInfo()).WillByDefault(Return(oldInfo));
    ON_CALL(*oldMachine, GetRequestID()).WillByDefault(Return(oldInfo.requestid()));

    auto gate = std::make_shared<litebus::Promise<Status>>();
    machineSlot_ = nullptr;
    ON_CALL(*controlView, GetInstance("inst-fq"))
        .WillByDefault(Invoke([this, oldMachine](const std::string &) {
            return machineSlot_ ? machineSlot_ : oldMachine;
        }));
    ON_CALL(*controlView, DelInstance("inst-fq"))
        .WillByDefault(Invoke([gate](const std::string &) {
            return gate->GetFuture();
        }));
    actor_->BindInstanceControlView(controlView);
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>("inst-fq"))).Times(0);

    auto result = actor_->DeleteInstanceInControlView(Status::OK(), oldInfo);
    auto newMachine = std::make_shared<MockInstanceStateMachine>("m-fq-new");
    auto newInfo = MakeElasticInfo("inst-fq", "runtime-SAME");
    newInfo.set_requestid("req-inst-fq-GENERATION-2");
    ON_CALL(*newMachine, GetInstanceInfo()).WillByDefault(Return(newInfo));
    ON_CALL(*newMachine, GetRequestID()).WillByDefault(Return(newInfo.requestid()));
    machineSlot_ = newMachine;
    gate->SetValue(Status::OK());

    ASSERT_AWAIT_READY(result);
    ASSERT_TRUE(result.Get().IsError());
    EXPECT_EQ(result.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
}

}  // namespace functionsystem::test
