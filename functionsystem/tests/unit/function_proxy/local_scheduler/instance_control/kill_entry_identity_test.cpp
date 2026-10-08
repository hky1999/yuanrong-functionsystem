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
//   5. the OTHER entry (KillRuntime) rejects a null view for elastic too;
//   6. control-view deletes through the OWNERSHIP-FENCED DelInstance
//      overload — mock-view tests gate THAT overload (never the legacy
//      single-arg one, which production no longer calls here), and the
//      machine-level fence (identity checked under the machine lock at the
//      metadata-read point) is exercised on REAL views with real in-place
//      ObserverActor-shaped updates;
//   7. the REAL internal exit chain (production exit handler installed by
//      Init) drives kill -> settlement -> control-delete — NOT a verified
//      frontend kill (final combination case must go through KillFrontend
//      with a real receipt + real settlement);
//   8. Traefik route retirement is fenced by the registry's recorded
//      incarnation (stale chains strip nothing).
//
// NOTE: these assert DISPATCH shape only (identity carried / zero dispatch
// / retryable-UNKNOWN). Budget-release correctness is NOT claimed here —
// it is exercised by the resource-view settlement tests.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "function_proxy/local_scheduler/instance_control/instance_ctrl_actor.h"
#include "function_proxy/local_scheduler/traefik_registry/traefik_registry.h"
#include "mocks/mock_control_interface_client_manager_proxy.h"
#include "mocks/mock_function_agent_mgr.h"
#include "mocks/mock_instance_control_view.h"
#include "mocks/mock_instance_state_machine.h"
#include "mocks/mock_internal_iam.h"
#include "mocks/fake_etcd_store.h"
#include "mocks/mock_meta_storage_accessor.h"
#include "function_proxy/common/state_handler/state_handler.h"
#include "common/state_machine/instance_control_view.h"
#include "mocks/mock_meta_store_client.h"
#include "mocks/mock_txn_transaction.h"
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

// Delete-success transaction response (the DelInstanceTest shape): the
// real InstanceStateMachine::DelInstance runs its version-fenced compare
// and-delete through this transport mock.
std::shared_ptr<TxnResponse> MakeDeleteOkTxnResponse()
{
    auto deleteTxnResp = TxnOperationResponse();
    auto deleteResponse = DeleteResponse();
    deleteResponse.deleted = true;
    deleteTxnResp.operationType = TxnOperationType::OPERATION_DELETE;
    deleteTxnResp.response = deleteResponse;
    auto deleteOk = std::make_shared<TxnResponse>();
    deleteOk->success = true;
    deleteOk->responses.emplace_back(deleteTxnResp);
    deleteOk->responses.emplace_back(deleteTxnResp);
    return deleteOk;
}

// A REAL view wired to a mock meta-store returning delete-success
// transactions (the convergence-test recipe), plus the bound txn mock so
// tests can gate Commit.
struct RealViewHarness {
    std::shared_ptr<InstanceControlView> view;
    std::shared_ptr<MockMetaStoreClient> metaClient;
    std::shared_ptr<MockTxnTransaction> txn;
};

RealViewHarness MakeRealView()
{
    RealViewHarness h;
    h.view = std::make_shared<InstanceControlView>("node-test", false);
    h.metaClient = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");
    h.view->BindMetaStoreClient(h.metaClient);
    h.txn = std::make_shared<MockTxnTransaction>(litebus::AID());
    EXPECT_CALL(*h.metaClient, BeginTransaction).WillRepeatedly(Return(h.txn));
    EXPECT_CALL(*h.txn, Commit).WillRepeatedly(
        Return(litebus::Future<std::shared_ptr<TxnResponse>>(MakeDeleteOkTxnResponse())));
    return h;
}

}  // namespace

class KillEntryIdentityFixture : public testing::Test {
protected:
    void SetUp() override
    {
        const auto suffix =
            litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        InstanceCtrlConfig config;
        // The traefik registration path derives the route host IP from this
        // address; without it the entry early-OKs and never registers.
        config.proxyGrpcAddress = "10.1.1.1:8443";
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
        // EXECUTION-POINT executors (root 16:5x): call-time view binding so
        // per-test BindInstanceControlView is honored; the guarded store
        // operation runs under the view lock — same lock as generation
        // changes.
        stateActor_->SetSaveExecutor([this](const std::string &instanceID,
                                            const std::string &expectedRuntimeID,
                                            const functionsystem::function_proxy::StateActor::GuardedStoreOp &op) {
            return actor_->GetInstanceControlView()->SaveStateGuarded(instanceID, expectedRuntimeID, op);
        });
        stateActor_->SetDeleteExecutor([this](const std::string &instanceID,
                                              const std::string &expectedRuntimeID,
                                              const functionsystem::function_proxy::StateActor::GuardedStoreOp &op) {
            return actor_->GetInstanceControlView()->DeleteStateGuarded(instanceID, expectedRuntimeID, op);
        });
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
// refuse BEFORE any side effect: the FENCED DelInstance overload is NEVER
// called and the new machine's state is untouched.
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
    // NEITHER DelInstance form may run for the new incarnation — in
    // particular the FENCED overload the production chain calls. The old
    // chain carries ("inst-late", "runtime-OLD", "req-inst-late").
    EXPECT_CALL(*controlView, DelInstance("inst-late")).Times(0);
    EXPECT_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-late"),
                                          Matcher<const std::string &>("runtime-OLD"),
                                          Matcher<const std::string &>("req-inst-late")))
        .Times(0);
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

// Same-generation delete still proceeds through the FENCED overload with
// the retiring chain's exact identity (the guard must not over-block).
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
    ON_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-ok"),
                                      Matcher<const std::string &>("runtime-ok"),
                                      Matcher<const std::string &>(info.requestid())))
        .WillByDefault(Invoke([](const std::string &, const std::string &, const std::string &) {
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    EXPECT_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-ok"),
                                          Matcher<const std::string &>("runtime-ok"),
                                          Matcher<const std::string &>(info.requestid())))
        .Times(1);
    // The lock-held snapshot the tail re-verification reads.
    ON_CALL(*controlView, GetIncarnationSnapshot(Matcher<const std::string &>("inst-ok")))
        .WillByDefault(Return(std::make_pair(info.runtimeid(), info.requestid())));
    // State delete via the REAL StateActor: succeed once.
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-ok", "req-inst-ok"))))
        .WillOnce(Return(Status::OK()));
    actor_->BindInstanceControlView(controlView);

    auto result = actor_->DeleteInstanceInControlView(Status::OK(), info);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsOk());
}


// Mid-flight replacement, RUNTIME change: guard passes on the OLD machine,
// the FENCED DelInstance is HELD on a gate promise, the machine is replaced
// with a NEW runtime, then the gate resolves — the tail guard must refuse
// the state delete of the new incarnation.
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
    // GATE THE FENCED OVERLOAD — the one the production chain actually
    // calls. The legacy single-arg form must never fire here.
    ON_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-mid"),
                                      Matcher<const std::string &>("runtime-OLD"),
                                      Matcher<const std::string &>(oldInfo.requestid())))
        .WillByDefault(Invoke([gate](const std::string &, const std::string &, const std::string &) {
            return gate->GetFuture();
        }));
    EXPECT_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-mid"),
                                          Matcher<const std::string &>("runtime-OLD"),
                                          Matcher<const std::string &>(oldInfo.requestid())))
        .Times(1);
    EXPECT_CALL(*controlView, DelInstance("inst-mid")).Times(0);
    // The snapshot follows the slot: OLD identity before the swap, NEW
    // after — exactly what the real view's lock-held read would answer.
    ON_CALL(*controlView, GetIncarnationSnapshot(Matcher<const std::string &>("inst-mid")))
        .WillByDefault(Invoke([this, oldMachine](const std::string &) {
            const auto machine = machineSlot_ ? machineSlot_ : oldMachine;
            return std::make_pair(machine->GetInstanceInfo().runtimeid(), machine->GetRequestID());
        }));
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
    ON_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-req"),
                                      Matcher<const std::string &>("runtime-SAME"),
                                      Matcher<const std::string &>(oldInfo.requestid())))
        .WillByDefault(Invoke([gate](const std::string &, const std::string &, const std::string &) {
            return gate->GetFuture();
        }));
    EXPECT_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-req"),
                                          Matcher<const std::string &>("runtime-SAME"),
                                          Matcher<const std::string &>(oldInfo.requestid())))
        .Times(1);
    ON_CALL(*controlView, GetIncarnationSnapshot(Matcher<const std::string &>("inst-req")))
        .WillByDefault(Invoke([this, oldMachine](const std::string &) {
            const auto machine = machineSlot_ ? machineSlot_ : oldMachine;
            return std::make_pair(machine->GetInstanceInfo().runtimeid(), machine->GetRequestID());
        }));
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
    ON_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-full"),
                                      Matcher<const std::string &>("runtime-full"),
                                      Matcher<const std::string &>(info.requestid())))
        .WillByDefault(Invoke([](const std::string &, const std::string &, const std::string &) {
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    EXPECT_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-full"),
                                          Matcher<const std::string &>("runtime-full"),
                                          Matcher<const std::string &>(info.requestid())))
        .Times(1);
    ON_CALL(*controlView, GetIncarnationSnapshot(Matcher<const std::string &>("inst-full")))
        .WillByDefault(Return(std::make_pair(info.runtimeid(), info.requestid())));
    actor_->BindInstanceControlView(controlView);
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-full", "req-inst-full"))))
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
    ON_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-fail"),
                                      Matcher<const std::string &>("runtime-fail"),
                                      Matcher<const std::string &>(info.requestid())))
        .WillByDefault(Invoke([](const std::string &, const std::string &, const std::string &) {
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    ON_CALL(*controlView, GetIncarnationSnapshot(Matcher<const std::string &>("inst-fail")))
        .WillByDefault(Return(std::make_pair(info.runtimeid(), info.requestid())));
    actor_->BindInstanceControlView(controlView);
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-fail", "req-inst-fail"))))
        .WillOnce(Return(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "store down")))
        .WillOnce(Return(Status::OK()));

    auto first = actor_->DeleteInstanceInControlView(Status::OK(), info);
    ASSERT_AWAIT_READY(first);
    ASSERT_TRUE(first.Get().IsError())
        << "the store failure must surface, not be masked";
    EXPECT_EQ(first.Get().StatusCode(), StatusCode::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(first.Get().RawMessage(), "store down");

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
    ON_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-fr"),
                                      Matcher<const std::string &>("runtime-OLD"),
                                      Matcher<const std::string &>(oldInfo.requestid())))
        .WillByDefault(Invoke([gate](const std::string &, const std::string &, const std::string &) {
            return gate->GetFuture();
        }));
    ON_CALL(*controlView, GetIncarnationSnapshot(Matcher<const std::string &>("inst-fr")))
        .WillByDefault(Invoke([this, oldMachine](const std::string &) {
            const auto machine = machineSlot_ ? machineSlot_ : oldMachine;
            return std::make_pair(machine->GetInstanceInfo().runtimeid(), machine->GetRequestID());
        }));
    actor_->BindInstanceControlView(controlView);
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-fr", "req-inst-fr")))).Times(0);

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
    ON_CALL(*controlView, DelInstance(Matcher<const std::string &>("inst-fq"),
                                      Matcher<const std::string &>("runtime-SAME"),
                                      Matcher<const std::string &>(oldInfo.requestid())))
        .WillByDefault(Invoke([gate](const std::string &, const std::string &, const std::string &) {
            return gate->GetFuture();
        }));
    ON_CALL(*controlView, GetIncarnationSnapshot(Matcher<const std::string &>("inst-fq")))
        .WillByDefault(Invoke([this, oldMachine](const std::string &) {
            const auto machine = machineSlot_ ? machineSlot_ : oldMachine;
            return std::make_pair(machine->GetInstanceInfo().runtimeid(), machine->GetRequestID());
        }));
    actor_->BindInstanceControlView(controlView);
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-fq", "req-inst-fq")))).Times(0);

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


// ── Real-view convergence: state-delete failure AFTER machine removal ────────
// NOTE (root 15:43): the FIRST delete below drives DeleteInstanceInControlView
// directly — that is RETRY-ENTRY coverage only. The real-first-kill-entry
// variant is RealFirstKillEntryThroughExitHandlerRecordsPending.
TEST_F(KillEntryIdentityFixture, StateDeleteFailureAfterMachineRemovalRetryConverges)
{
    auto realView = std::make_shared<InstanceControlView>("node-test", false);
    // Real meta-store dependency, mocked transport (no etcd needed).
    auto mockMetaClient = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");
    realView->BindMetaStoreClient(mockMetaClient);
    auto mockTxn = std::make_shared<MockTxnTransaction>(litebus::AID());
    EXPECT_CALL(*mockMetaClient, BeginTransaction).WillRepeatedly(Return(mockTxn));
    // Delete-success transaction response (the DelInstanceTest shape).
    auto deleteTxnResp = TxnOperationResponse();
    auto deleteResponse = DeleteResponse();
    deleteResponse.deleted = true;
    deleteTxnResp.operationType = TxnOperationType::OPERATION_DELETE;
    deleteTxnResp.response = deleteResponse;
    auto deleteOk = std::make_shared<TxnResponse>();
    deleteOk->success = true;
    deleteOk->responses.emplace_back(deleteTxnResp);
    deleteOk->responses.emplace_back(deleteTxnResp);
    EXPECT_CALL(*mockTxn, Commit).WillRepeatedly(
        Return(litebus::Future<std::shared_ptr<TxnResponse>>(deleteOk)));

    // The info MUST carry the owner proxy id and a legal function key so
    // the real ownership guard passes.
    auto info = MakeElasticInfo("inst-conv", "runtime-conv");
    info.set_functionproxyid("node-test");
    info.set_function("default/0-test-helloWorld/$latest");
    realView->GenerateStateMachine(info.instanceid(), info);
    ASSERT_NE(realView->GetInstance("inst-conv"), nullptr);
    actor_->BindInstanceControlView(realView);

    // First attempt: machine delete SUCCEEDS through the real path; the
    // state delete FAILS once.
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-conv", "req-inst-conv"))))
        .WillOnce(Return(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "store down")))
        .WillOnce(Return(Status::OK()));

    auto first = actor_->DeleteInstanceInControlView(Status::OK(), info);
    ASSERT_AWAIT_READY(first);
    ASSERT_TRUE(first.Get().IsError());
    EXPECT_EQ(first.Get().StatusCode(), StatusCode::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(first.Get().RawMessage(), "store down");
    // The machine is ALREADY gone (the real DelInstance + OnDelInstance ran
    // BEFORE the state-delete failure).
    EXPECT_EQ(realView->GetInstance("inst-conv"), nullptr);

    // Retry from the SAME real entry converges: missing machine => real
    // DelInstance returns OK (idempotent), the guard's GetInstance nullptr
    // goes straight to the state delete, which succeeds on attempt two.
    auto retry = actor_->DeleteInstanceInControlView(Status::OK(), info);
    ASSERT_AWAIT_READY(retry);
    EXPECT_TRUE(retry.Get().IsOk()) << retry.Get().ToString();
    EXPECT_EQ(realView->GetInstance("inst-conv"), nullptr);
}


// Real frontend-entry retry: the first control delete removes the machine
// (real view) but the state delete fails; the second call goes through
// the REAL KillFrontend (tenant-checked frontend path), finds the pending
// record, and completes the cleanup — NOT a NOT_FOUND bounce.
// NOTE (root 15:43): first delete is the direct helper — RETRY-ENTRY
// coverage; the REAL first kill entry is covered separately below.
TEST_F(KillEntryIdentityFixture, RealKillFrontendRetryCompletesPendingCleanup)
{
    auto realView = std::make_shared<InstanceControlView>("node-test", false);
    auto mockMetaClient = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");
    realView->BindMetaStoreClient(mockMetaClient);
    auto mockTxn = std::make_shared<MockTxnTransaction>(litebus::AID());
    EXPECT_CALL(*mockMetaClient, BeginTransaction).WillRepeatedly(Return(mockTxn));
    auto deleteTxnResp = TxnOperationResponse();
    auto deleteResponse = DeleteResponse();
    deleteResponse.deleted = true;
    deleteTxnResp.operationType = TxnOperationType::OPERATION_DELETE;
    deleteTxnResp.response = deleteResponse;
    auto deleteOk = std::make_shared<TxnResponse>();
    deleteOk->success = true;
    deleteOk->responses.emplace_back(deleteTxnResp);
    deleteOk->responses.emplace_back(deleteTxnResp);
    EXPECT_CALL(*mockTxn, Commit).WillRepeatedly(
        Return(litebus::Future<std::shared_ptr<TxnResponse>>(deleteOk)));

    auto info = MakeElasticInfo("inst-fk", "runtime-fk");
    info.set_functionproxyid("node-test");
    info.set_function("default/0-test-helloWorld/$latest");
    info.set_tenantid("tenant-test");
    realView->GenerateStateMachine(info.instanceid(), info);
    actor_->BindInstanceControlView(realView);

    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-fk", "req-inst-fk"))))
        .WillOnce(Return(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "store down")))
        .WillOnce(Return(Status::OK()));

    auto first = actor_->DeleteInstanceInControlView(Status::OK(), info);
    ASSERT_AWAIT_READY(first);
    ASSERT_TRUE(first.Get().IsError());
    EXPECT_EQ(realView->GetInstance("inst-fk"), nullptr);

    // The REAL frontend kill entry — completes via the pending record.
    auto killReq = std::make_shared<KillRequest>();
    killReq->set_instanceid("inst-fk");
    killReq->set_requestid("req-fk");
    auto second = actor_->KillFrontend("tenant-test", killReq);
    ASSERT_AWAIT_READY(second);
    EXPECT_EQ(static_cast<int>(second.Get().code()), static_cast<int>(common::ERR_NONE))
        << second.Get().message();

    // A THIRD call with no pending record: genuine NOT_FOUND (unchanged).
    auto third = actor_->KillFrontend("tenant-test", killReq);
    ASSERT_AWAIT_READY(third);
    EXPECT_EQ(static_cast<int>(third.Get().code()),
              static_cast<int>(common::ERR_INSTANCE_NOT_FOUND));
}


// Root-required negatives for the missing-machine retry branch:
// empty tenant and wrong tenant are REFUSED with zero cache Del and the
// pending record RETAINED.
TEST_F(KillEntryIdentityFixture, PendingCleanupTenantGateRefused)
{
    // Real delete-shaped transaction responses (the DelInstanceTest
    // recipe): the machine delete must SUCCEED so the tenant-gated retry
    // path is the one under test.
    auto harness = MakeRealView();
    auto realView = harness.view;

    auto info = MakeElasticInfo("inst-tg", "runtime-tg");
    info.set_functionproxyid("node-test");
    info.set_function("default/0-test-helloWorld/$latest");
    info.set_tenantid("tenant-real");
    realView->GenerateStateMachine(info.instanceid(), info);
    actor_->BindInstanceControlView(realView);

    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-tg", "req-inst-tg"))))
        .WillOnce(Return(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "store down")))
        .WillOnce(Return(Status::OK()));

    auto first = actor_->DeleteInstanceInControlView(Status::OK(), info);
    ASSERT_AWAIT_READY(first);
    ASSERT_TRUE(first.Get().IsError());
    EXPECT_EQ(realView->GetInstance("inst-tg"), nullptr);

    auto killReq = std::make_shared<KillRequest>();
    killReq->set_instanceid("inst-tg");
    killReq->set_requestid("req-tg");

    // EMPTY tenant: refused, zero extra cache Del, record retained.
    auto empty = actor_->KillFrontend("", killReq);
    ASSERT_AWAIT_READY(empty);
    EXPECT_EQ(static_cast<int>(empty.Get().code()),
              static_cast<int>(common::ERR_AUTHORIZE_FAILED));

    // WRONG tenant: same refusal.
    auto wrong = actor_->KillFrontend("tenant-other", killReq);
    ASSERT_AWAIT_READY(wrong);
    EXPECT_EQ(static_cast<int>(wrong.Get().code()),
              static_cast<int>(common::ERR_AUTHORIZE_FAILED));

    // Exactly ONE cache Del so far (the failed first attempt): the two
    // refusals dispatched nothing.
    // (Mock count check via the sequence expectation below.)

    // The CORRECT tenant converges.
    auto ok = actor_->KillFrontend("tenant-real", killReq);
    ASSERT_AWAIT_READY(ok);
    EXPECT_EQ(static_cast<int>(ok.Get().code()), static_cast<int>(common::ERR_NONE));
}

// Late old callback must NOT erase (or overwrite) a record rewritten
// during the wait. The barrier is the REAL cache Del running on the
// StateActor thread (Del is synchronous — it returns a Status): the
// Invoke signals arrival, then BLOCKS until the test rewrites the record,
// so the rewrite provably happens while the deletion is in flight.
TEST_F(KillEntryIdentityFixture, LateOldCallbackDoesNotEraseRewrittenRecord)
{
    auto harness = MakeRealView();
    actor_->BindInstanceControlView(harness.view);

    auto info = MakeElasticInfo("inst-late", "runtime-late");
    info.set_functionproxyid("node-test");
    info.set_function("default/0-test-helloWorld/$latest");
    info.set_tenantid("tenant-late");
    harness.view->GenerateStateMachine(info.instanceid(), info);

    auto reached = std::make_shared<std::promise<void>>();
    auto release = std::make_shared<std::promise<void>>();
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-late", "req-inst-late"))))
        .WillOnce(Invoke([reached, release](const std::string &) {
            reached->set_value();
            // Bounded block: an early test abort must still free the actor
            // thread for teardown.
            (void)release->get_future().wait_for(std::chrono::seconds(10));
            return Status::OK();
        }));

    auto result = actor_->DeleteInstanceInControlView(Status::OK(), info);

    // Wait until the deletion has ACTUALLY reached the store layer, THEN
    // rewrite the record mid-flight (a NEW generation's failed cleanup).
    ASSERT_EQ(reached->get_future().wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    actor_->settledCleanupPending_["inst-late"] = {
        "tenant-late", "runtime-NEW", "req-CREATION-NEW", "rewritten"};
    release->set_value();

    ASSERT_AWAIT_READY(result);
    // The REWRITTEN record survives the old chain's success callback
    // (identity compare before erase).
    const auto after = actor_->settledCleanupPending_.find("inst-late");
    ASSERT_NE(after, actor_->settledCleanupPending_.end());
    EXPECT_EQ(after->second.runtimeID, "runtime-NEW");
    EXPECT_EQ(after->second.requestID, "req-CREATION-NEW");
    EXPECT_EQ(after->second.reason, "rewritten");
}

// Companion (failure leg): while THIS chain's own state delete is in
// flight, a DIFFERENT generation's failed-cleanup record lands under the
// same name — this chain's FAILURE callback must NOT overwrite it.
TEST_F(KillEntryIdentityFixture, LateFailureCallbackDoesNotOverwriteRewrittenRecord)
{
    auto harness = MakeRealView();
    actor_->BindInstanceControlView(harness.view);

    auto info = MakeElasticInfo("inst-ovw", "runtime-ovw");
    info.set_functionproxyid("node-test");
    info.set_function("default/0-test-helloWorld/$latest");
    info.set_tenantid("tenant-ovw");
    harness.view->GenerateStateMachine(info.instanceid(), info);

    auto reached = std::make_shared<std::promise<void>>();
    auto release = std::make_shared<std::promise<void>>();
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-ovw", "req-inst-ovw"))))
        .WillOnce(Invoke([reached, release](const std::string &) {
            reached->set_value();
            (void)release->get_future().wait_for(std::chrono::seconds(10));
            return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "store down");
        }));

    auto result = actor_->DeleteInstanceInControlView(Status::OK(), info);

    ASSERT_EQ(reached->get_future().wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    actor_->settledCleanupPending_["inst-ovw"] = {
        "tenant-NEW", "runtime-NEW", "req-CREATION-NEW", "rewritten-failure"};
    release->set_value();

    ASSERT_AWAIT_READY(result);
    ASSERT_TRUE(result.Get().IsError());
    // The newer generation's record is NOT overwritten by this chain's
    // failure write (identity-guarded insertion).
    const auto after = actor_->settledCleanupPending_.find("inst-ovw");
    ASSERT_NE(after, actor_->settledCleanupPending_.end());
    EXPECT_EQ(after->second.runtimeID, "runtime-NEW");
    EXPECT_EQ(after->second.requestID, "req-CREATION-NEW");
    EXPECT_EQ(after->second.reason, "rewritten-failure");
}


// ── SAME-REQUEST DIFFERENT-RUNTIME WINDOW (root 16:48/16:5x follow-up) ───────
// Idempotent, noexcept barrier release: the test body and the destructor
// share ONE Release() — double fulfillment is impossible, and EVERY exit
// path (ASSERT included) releases the barrier so no thread dangles.
struct ScopedBarrierRelease {
    std::shared_ptr<std::promise<void>> release;
    std::atomic<bool> done{ false };
    void Release() noexcept
    {
        if (!done.exchange(true)) {
            try { release->set_value(); } catch (...) {}  // never throw out
        }
    }
    ~ScopedBarrierRelease() { Release(); }
};

// Joins the thread on every exit path (noexcept).
struct ThreadJoiner {
    std::thread &thread;
    ~ThreadJoiner() { if (thread.joinable()) { thread.join(); } }
};

// Failover keeps the CREATION REQUEST but replaces the RUNTIME — both
// compose the SAME state key. The barrier sits at the backend Set INSIDE
// the two-lock guarded execution. The publish under test is the REAL
// in-place rewrite `machine->UpdateInstanceInfo(R2)` — the path that takes
// ONLY the machine lock and previously bypassed the view-lock guard. It
// must be MUTEX-blocked while the old save's store op is in flight; the
// release completes the old save, then the in-place publish lands, then
// the new save writes — the FINAL value is the successor's, and a later
// old-stream save refuses with zero writes.
TEST_F(KillEntryIdentityFixture, SameRequestBarrierBlocksInPlaceRuntimeSwapSave)
{
    auto harness = MakeRealView();
    actor_->BindInstanceControlView(harness.view);

    auto infoOld = MakeElasticInfo("inst-bar", "runtime-R1");
    infoOld.set_functionproxyid("node-test");
    infoOld.set_function("default/0-test-helloWorld/$latest");
    harness.view->GenerateStateMachine("inst-bar", infoOld);
    const auto machine = harness.view->GetInstance("inst-bar");
    ASSERT_NE(machine, nullptr);

    const std::string key = StateClient::ComposeStateKey("inst-bar", "req-inst-bar");
    auto stored = std::make_shared<std::string>();
    auto reached = std::make_shared<std::promise<void>>();
    auto release = std::make_shared<std::promise<void>>();
    ScopedBarrierRelease releaser{ release };
    // The barrier's wait RESULT is captured and asserted below — a timeout
    // is an explicit failure, never silently treated as mutex proof.
    auto barrierWaitOk = std::make_shared<std::atomic<bool>>(false);
    EXPECT_CALL(*cacheClient_, Set(Matcher<const std::string &>(key), _))
        .WillOnce(Invoke([stored, reached, release, barrierWaitOk](const std::string &, const std::string &val) {
            reached->set_value();
            barrierWaitOk->store(release->get_future().wait_for(std::chrono::seconds(10))
                                     == std::future_status::ready);
            *stored = val;
            return Status::OK();
        }))
        .WillOnce(Invoke([stored](const std::string &, const std::string &val) {
            *stored = val;
            return Status::OK();
        }));

    auto oldMsg = std::make_shared<runtime_rpc::StreamingMessage>();
    oldMsg->mutable_savereq()->set_state("OLD-STATE");
    auto oldSave = std::make_shared<litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>>>(
        function_proxy::StateHandler::SaveState(
            grpc::PosixStreamIdentity{ "inst-bar", "runtime-R1", "req-inst-bar" }, oldMsg));
    ASSERT_EQ(reached->get_future().wait_for(std::chrono::seconds(5)), std::future_status::ready);

    // The REAL in-place runtime publish (machine lock only — the bypass
    // path root identified) must be BLOCKED by the guarded store op. The
    // publisher CONFIRMS it has started before the blocked-assertion.
    auto published = std::make_shared<std::atomic<bool>>(false);
    auto publisherStarted = std::make_shared<std::promise<void>>();
    auto infoNew = MakeElasticInfo("inst-bar", "runtime-R2");
    infoNew.set_functionproxyid("node-test");
    infoNew.set_function("default/0-test-helloWorld/$latest");
    std::thread publisher([machine, infoNew, published, publisherStarted]() {
        publisherStarted->set_value();
        machine->UpdateInstanceInfo(infoNew);  // direct in-place rewrite
        published->store(true);
    });
    ThreadJoiner joiner{ publisher };
    // Retrieve the started-future ONCE and reuse it (a second get_future()
    // on the same promise throws "Future already retrieved").
    auto startedFuture = publisherStarted->get_future();
    ASSERT_EQ(startedFuture.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "publisher thread must confirm start";
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(published->load()) << "the in-place runtime swap must be blocked by the guarded op";

    // NORMAL PATH: release, then WAIT for BOTH the old op's completion and
    // the publisher's completion (bounded) BEFORE asserting any state.
    releaser.Release();
    ASSERT_AWAIT_READY(*oldSave);
    bool publishedInTime = false;
    for (int i = 0; i < 500 && !(publishedInTime = published->load()); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(publishedInTime) << "publisher must complete after release (bounded 5s)";
    EXPECT_TRUE(barrierWaitOk->load()) << "the barrier wait itself must not have timed out";
    EXPECT_EQ(machine->GetInstanceInfo().runtimeid(), "runtime-R2");
    EXPECT_EQ(*stored, "OLD-STATE");  // old save happened-before the publish
    (void)joiner;

    // The successor's save lands AFTER the publish — final value is NEW.
    auto newMsg = std::make_shared<runtime_rpc::StreamingMessage>();
    newMsg->mutable_savereq()->set_state("NEW-STATE");
    auto newSave = function_proxy::StateHandler::SaveState(
        grpc::PosixStreamIdentity{ "inst-bar", "runtime-R2", "req-inst-bar" }, newMsg);
    ASSERT_AWAIT_READY(newSave);
    EXPECT_EQ(newSave.Get()->saversp().code(), common::ERR_NONE);
    EXPECT_EQ(*stored, "NEW-STATE");

    // Any LATER old-stream save refuses at the execution point: zero writes.
    auto staleMsg = std::make_shared<runtime_rpc::StreamingMessage>();
    staleMsg->mutable_savereq()->set_state("STALE-STATE");
    auto staleSave = function_proxy::StateHandler::SaveState(
        grpc::PosixStreamIdentity{ "inst-bar", "runtime-R1", "req-inst-bar" }, staleMsg);
    ASSERT_AWAIT_READY(staleSave);
    EXPECT_NE(staleSave.Get()->saversp().code(), common::ERR_NONE);
    EXPECT_EQ(*stored, "NEW-STATE");
}

// DELETE leg, same barrier: the retiring chain's guarded Del of the shared
// key blocks the in-place runtime swap while in flight; after the swap the
// OLD chain's Del refuses (zero Del) and the NEW chain's Del proceeds.
TEST_F(KillEntryIdentityFixture, SameRequestBarrierBlocksInPlaceRuntimeSwapDelete)
{
    auto harness = MakeRealView();
    actor_->BindInstanceControlView(harness.view);

    auto infoOld = MakeElasticInfo("inst-bd", "runtime-R1");
    infoOld.set_functionproxyid("node-test");
    infoOld.set_function("default/0-test-helloWorld/$latest");
    harness.view->GenerateStateMachine("inst-bd", infoOld);
    const auto machine = harness.view->GetInstance("inst-bd");
    ASSERT_NE(machine, nullptr);

    const std::string key = StateClient::ComposeStateKey("inst-bd", "req-inst-bd");
    auto delCount = std::make_shared<std::atomic<int>>(0);
    auto reached = std::make_shared<std::promise<void>>();
    auto release = std::make_shared<std::promise<void>>();
    ScopedBarrierRelease releaser{ release };
    auto barrierWaitOk = std::make_shared<std::atomic<bool>>(false);
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(key)))
        .WillOnce(Invoke([delCount, reached, release, barrierWaitOk](const std::string &) {
            reached->set_value();
            barrierWaitOk->store(release->get_future().wait_for(std::chrono::seconds(10))
                                     == std::future_status::ready);
            delCount->fetch_add(1);
            return Status::OK();
        }))
        .WillRepeatedly(Invoke([delCount](const std::string &) {
            delCount->fetch_add(1);
            return Status::OK();
        }));

    auto oldDel = std::make_shared<litebus::Future<Status>>(
        function_proxy::StateHandler::DeleteState("inst-bd", "req-inst-bd", "runtime-R1"));
    ASSERT_EQ(reached->get_future().wait_for(std::chrono::seconds(5)), std::future_status::ready);

    auto published = std::make_shared<std::atomic<bool>>(false);
    auto publisherStarted = std::make_shared<std::promise<void>>();
    auto infoNew = MakeElasticInfo("inst-bd", "runtime-R2");
    infoNew.set_functionproxyid("node-test");
    infoNew.set_function("default/0-test-helloWorld/$latest");
    std::thread publisher([machine, infoNew, published, publisherStarted]() {
        publisherStarted->set_value();
        machine->UpdateInstanceInfo(infoNew);
        published->store(true);
    });
    ThreadJoiner joiner{ publisher };
    auto startedFuture = publisherStarted->get_future();
    ASSERT_EQ(startedFuture.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "publisher thread must confirm start";
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_FALSE(published->load()) << "the in-place runtime swap must be blocked by the guarded Del";

    // NORMAL PATH: release, wait for BOTH completions (bounded), THEN assert.
    releaser.Release();
    ASSERT_AWAIT_READY(*oldDel);
    bool publishedInTime = false;
    for (int i = 0; i < 500 && !(publishedInTime = published->load()); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(publishedInTime) << "publisher must complete after release (bounded 5s)";
    EXPECT_TRUE(barrierWaitOk->load()) << "the barrier wait itself must not have timed out";
    (void)joiner;
    ASSERT_AWAIT_READY(*oldDel);
    EXPECT_TRUE(oldDel->Get().IsOk());
    EXPECT_EQ(delCount->load(), 1);

    // After the swap, the OLD chain's Del refuses (zero new Del)…
    auto staleDel = function_proxy::StateHandler::DeleteState("inst-bd", "req-inst-bd", "runtime-R1");
    ASSERT_AWAIT_READY(staleDel);
    EXPECT_EQ(staleDel.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
    EXPECT_EQ(delCount->load(), 1);
    // …and the NEW chain's Del proceeds.
    auto newDel = function_proxy::StateHandler::DeleteState("inst-bd", "req-inst-bd", "runtime-R2");
    ASSERT_AWAIT_READY(newDel);
    EXPECT_TRUE(newDel.Get().IsOk());
    EXPECT_EQ(delCount->load(), 2);
}

// ── REAL RegisterTraefikRoute ENTRY (root 16:35/follow-ups) ──────────────────
// Real actor entry over the shared fake etcd: normal create publishes the
// route+marker; a stale (old-runtime) request re-registering after an
// in-place failover swap must carry the SNAPSHOT identity (new runtime),
// not the stale payload's; an owner change to another proxy refuses; a
// valid empty refresh cleans the old keys; a corrupt payload (mixed valid
// and invalid entries) changes NOTHING.
TEST_F(KillEntryIdentityFixture, RegisterTraefikRouteRealEntryFourStates)
{
    auto store = std::make_shared<FakeStore>();
    auto metaClient = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");
    ON_CALL(*metaClient, BeginTransaction).WillByDefault(Invoke([store]() {
        return std::shared_ptr<functionsystem::meta_store::TxnTransaction>(
            std::make_shared<FakeEvalTxn>(store));
    }));
    auto accessor = std::make_shared<MockMetaStorageAccessor>(metaClient);
    ON_CALL(*accessor, Put(_, _)).WillByDefault(Invoke([store](const std::string &k, const std::string &v) {
        store->Put(k, v);
        return Status::OK();
    }));
    ON_CALL(*accessor, Get(_)).WillByDefault(Invoke([store](const std::string &k) {
        bool found = false;
        const std::string value = store->Get(k, &found);
        return found ? litebus::Option<std::string>(litebus::Some(value)) : litebus::Option<std::string>();
    }));
    ON_CALL(*accessor, GetAllWithPrefix(_)).WillByDefault(Invoke([store](const std::string &prefix) {
        std::vector<std::pair<std::string, std::string>> out;
        std::lock_guard<std::mutex> lock(store->mutex);
        for (const auto &kv : store->kv) {
            if (kv.first.rfind(prefix, 0) == 0) { out.emplace_back(kv.first, kv.second); }
        }
        return litebus::Option<std::vector<std::pair<std::string, std::string>>>(litebus::Some(out));
    }));
    actor_->SetTraefikRegistry(std::make_shared<TraefikRegistry>(accessor, "yr/tk", "web", false, ""));

    auto harness = MakeRealView();
    actor_->BindInstanceControlView(harness.view);
    const std::string ext = R"(["http:40001:8080"])";
    auto info = MakeElasticInfo("inst-rt4", "runtime-R1");
    info.set_functionproxyid("node-test");
    info.set_function("default/0-test-helloWorld/$latest");
    (*info.mutable_extensions())[PORT_FORWARD_KEY] = ext;
    harness.view->GenerateStateMachine("inst-rt4", info);
    // Seed the DURABLE instance record (same identity) into the fake store
    // and stamp the machine with its revision — the register's commit-time
    // record-mod-revision condition reads exactly this.
    const std::string recordKey = InstanceStateMachine::GetInstanceRecordKey(info, false);
    ASSERT_FALSE(recordKey.empty());
    auto bumpRecord = [&](const std::string &runtime) {
        store->Put(recordKey, "instance-record|" + runtime + "|req-inst-rt4");
        harness.view->GetInstance("inst-rt4")->SetModRevision(store->modRevision[recordKey]);
    };
    bumpRecord("runtime-R1");

    // 1) Normal create: route + marker land under the SNAPSHOT identity.
    auto created = actor_->RegisterTraefikRoute(info);
    ASSERT_AWAIT_READY(created);
    EXPECT_TRUE(created.Get().IsOk()) << created.Get().GetMessage();
    EXPECT_EQ(store->kv.count("yr/tk/http/routers/inst-rt4-p8080/rule"), 1u);
    const auto marker1 = TraefikRegistry::ParseMarker(
        store->kv.at("yr/tk/http/instances/inst-rt4/generation"));
    ASSERT_TRUE(marker1.valid);
    EXPECT_EQ(marker1.runtimeID, "runtime-R1");

    // 2) Failover (same request, new runtime) published IN PLACE; the OLD
    //    payload re-registers — the snapshot identity (R2) must be used.
    auto swapped = MakeElasticInfo("inst-rt4", "runtime-R2");
    swapped.set_functionproxyid("node-test");
    swapped.set_function("default/0-test-helloWorld/$latest");
    (*swapped.mutable_extensions())[PORT_FORWARD_KEY] = ext;
    harness.view->Update("inst-rt4", swapped, true);
    bumpRecord("runtime-R2");
    auto staleRegister = actor_->RegisterTraefikRoute(info);  // OLD payload
    ASSERT_AWAIT_READY(staleRegister);
    EXPECT_TRUE(staleRegister.Get().IsOk()) << staleRegister.Get().GetMessage();
    const auto marker2 = TraefikRegistry::ParseMarker(
        store->kv.at("yr/tk/http/instances/inst-rt4/generation"));
    ASSERT_TRUE(marker2.valid);
    EXPECT_EQ(marker2.runtimeID, "runtime-R2") << "the snapshot identity wins, not the stale payload";

    // 3) Owner migrated to another proxy: refused, routes untouched.
    auto migrated = swapped;
    migrated.set_functionproxyid("other-proxy");
    harness.view->Update("inst-rt4", migrated, true);
    bumpRecord("runtime-R2");
    auto foreignOwner = actor_->RegisterTraefikRoute(swapped);
    ASSERT_AWAIT_READY(foreignOwner);
    EXPECT_TRUE(foreignOwner.Get().IsError());
    EXPECT_EQ(store->kv.count("yr/tk/http/routers/inst-rt4-p8080/rule"), 1u);
    harness.view->Update("inst-rt4", swapped, true);  // restore ownership
    bumpRecord("runtime-R2");

    // 4) Valid empty refresh: the old key list is atomically cleaned.
    auto zeroed = swapped;
    (*zeroed.mutable_extensions())[PORT_FORWARD_KEY] = "[]";
    harness.view->Update("inst-rt4", zeroed, true);
    bumpRecord("runtime-R2");
    auto emptied = actor_->RegisterTraefikRoute(zeroed);
    ASSERT_AWAIT_READY(emptied);
    EXPECT_TRUE(emptied.Get().IsOk()) << emptied.Get().GetMessage();
    EXPECT_EQ(store->kv.count("yr/tk/http/routers/inst-rt4-p8080/rule"), 0u)
        << "a valid empty refresh must clean the previous own-key list";
    EXPECT_EQ(store->kv.count("yr/tk/http/instances/inst-rt4/generation"), 1u);

    // 5) Corrupt payload (mixed valid + invalid entries): nothing changes.
    (*swapped.mutable_extensions())[PORT_FORWARD_KEY] = R"(["http:40002:8081","garbage"])";
    harness.view->Update("inst-rt4", swapped, true);
    bumpRecord("runtime-R2");
    const auto before = store->kv;
    auto corrupt = actor_->RegisterTraefikRoute(swapped);
    ASSERT_AWAIT_READY(corrupt);
    EXPECT_TRUE(corrupt.Get().IsError()) << "a mixed payload must be rejected whole";
    EXPECT_EQ(store->kv, before) << "existing routes are kept untouched";
}

// ── MACHINE-LEVEL FENCE (#1, root 15:12): identity at the metadata read ─────

// The control-view lock ends BEFORE the machine reads its own metadata, and
// ObserverActor-shaped updates rewrite the SAME machine object in place —
// so the authoritative check must live under the MACHINE lock where the
// delete transaction is actually built. Here: a REAL view + REAL machine,
// the identity swapped in place via the real Update path. The view-level
// call is refused by the prefilter; the MACHINE's own fenced DelInstance
// (the authoritative point) refuses independently; and the fence is
// identity-CONDITIONAL — the NEW generation's own delete still runs.
TEST_F(KillEntryIdentityFixture, MachineFenceRefusesInPlaceSwappedIdentity)
{
    auto harness = MakeRealView();
    actor_->BindInstanceControlView(harness.view);

    auto info = MakeElasticInfo("inst-fence", "runtime-OLD");
    info.set_functionproxyid("node-test");
    info.set_function("default/0-test-helloWorld/$latest");
    harness.view->GenerateStateMachine(info.instanceid(), info);

    // The ObserverActor shape: a forced Update lands on the SAME machine
    // object and CopyFrom-replaces the info — new runtime AND new request
    // generation under the same instanceID.
    auto swapped = MakeElasticInfo("inst-fence", "runtime-NEW");
    swapped.set_functionproxyid("node-test");
    swapped.set_function("default/0-test-helloWorld/$latest");
    swapped.set_requestid("req-inst-fence-GENERATION-2");
    harness.view->Update("inst-fence", swapped, true);
    auto machine = harness.view->GetInstance("inst-fence");
    ASSERT_NE(machine, nullptr);
    ASSERT_EQ(machine->GetInstanceInfo().runtimeid(), "runtime-NEW");

    // 1) View-level fenced delete with the OLD identity: refused, zero
    //    metadata transactions begun.
    auto refused = harness.view->DelInstance("inst-fence", "runtime-OLD",
                                             "req-inst-fence");
    ASSERT_AWAIT_READY(refused);
    ASSERT_TRUE(refused.Get().IsError());
    EXPECT_EQ(refused.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);

    // 2) MACHINE-level fence, driven directly on the machine object (the
    //    exact point the metadata and version are read): OLD identity
    //    refused, still zero transactions.
    auto machineRefused = machine->DelInstance("inst-fence", "runtime-OLD",
                                               "req-inst-fence");
    ASSERT_AWAIT_READY(machineRefused);
    ASSERT_TRUE(machineRefused.Get().IsError());
    EXPECT_EQ(machineRefused.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);

    // 3) The fence is identity-conditional: the NEW generation's OWN
    //    delete proceeds through the real metadata transaction.
    auto proceed = machine->DelInstance("inst-fence", "runtime-NEW",
                                        "req-inst-fence-GENERATION-2");
    ASSERT_AWAIT_READY(proceed);
    EXPECT_TRUE(proceed.Get().IsOk()) << proceed.Get().ToString();

    // The refused deletes never touched the view's machine entry (a direct
    // machine delete retires METADATA; the view's own erase is
    // OnDelInstance's business) and no instance-state delete leaked.
    EXPECT_NE(harness.view->GetInstance("inst-fence"), nullptr);
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-fence", "req-inst-fence")))).Times(0);
}

// The REAL WINDOW end-to-end (root's counterexample shape, real objects
// everywhere; only the meta-store transport is a mock): the machine
// metadata delete transaction is HELD in flight (gated Commit); during
// the hold, a forced Update swaps the identity in place; the txn then
// completes. OnDelInstance must refuse the erase (the request generation
// no longer matches) and the TAIL guard must refuse the state delete —
// the new generation keeps its machine AND its state.
TEST_F(KillEntryIdentityFixture, RealWindowInPlaceUpdateDuringGatedMachineDelete)
{
    auto harness = MakeRealView();
    // Gate the metadata transaction itself.
    auto gate = std::make_shared<litebus::Promise<std::shared_ptr<TxnResponse>>>();
    EXPECT_CALL(*harness.txn, Commit).WillOnce(
        Invoke([gate]() { return gate->GetFuture(); }))
        .WillRepeatedly(
            Return(litebus::Future<std::shared_ptr<TxnResponse>>(MakeDeleteOkTxnResponse())));
    actor_->BindInstanceControlView(harness.view);

    auto info = MakeElasticInfo("inst-rw", "runtime-OLD");
    info.set_functionproxyid("node-test");
    info.set_function("default/0-test-helloWorld/$latest");
    info.set_tenantid("tenant-rw");
    harness.view->GenerateStateMachine(info.instanceid(), info);

    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-rw", "req-inst-rw")))).Times(0);

    auto result = actor_->DeleteInstanceInControlView(Status::OK(), info);

    // While the version-fenced delete txn is in flight, the SAME machine
    // object is rewritten in place with a NEW generation's identity.
    auto swapped = MakeElasticInfo("inst-rw", "runtime-NEW");
    swapped.set_functionproxyid("node-test");
    swapped.set_function("default/0-test-helloWorld/$latest");
    swapped.set_requestid("req-inst-rw-GENERATION-2");
    harness.view->Update("inst-rw", swapped, true);

    // The held transaction completes (it carried the OLD version; with a
    // real store an in-place persist of the new version would have made it
    // fail the compare — the transport mock cannot express that, which is
    // exactly why the tail guards below must exist).
    gate->SetValue(MakeDeleteOkTxnResponse());

    ASSERT_AWAIT_READY(result);
    ASSERT_TRUE(result.Get().IsError());
    EXPECT_EQ(result.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);

    // OnDelInstance refused the erase: the machine SURVIVES with the NEW
    // generation's identity, and its state was never deleted.
    const auto machine = harness.view->GetInstance("inst-rw");
    ASSERT_NE(machine, nullptr);
    EXPECT_EQ(machine->GetInstanceInfo().runtimeid(), "runtime-NEW");
    EXPECT_EQ(machine->GetRequestID(), "req-inst-rw-GENERATION-2");
}


// ── REAL INTERNAL EXIT-CHAIN ENTRY (root 15:43 #4, relabeled 15:5x) ──────────

// The production exit handler installed by Init() is the REAL internal
// exit chain: ShutDownInstance -> KillRuntime (elastic identity gate ->
// agent kill -> receipt settlement) -> DeleteInstanceInResourceView ->
// DeleteInstanceInControlView. Driving IT (not the helper) with the state
// delete failing once must leave the pending record; the REAL KillFrontend
// retry then converges, and a third call is the genuine NOT_FOUND.
// SCOPE LABEL (root): this is INTERNAL-exit-chain coverage — it does NOT
// pass through KillFrontend -> Kill -> state transition -> request
// receipt, so it is NOT a verified real first FRONTEND kill. The final
// combination case must call KillFrontend first and verify the receipt
// (with a REAL ApplyPhysicalRetirement, not the mock used here).
TEST_F(KillEntryIdentityFixture, RealFirstKillEntryThroughExitHandlerRecordsPending)
{
    // The production exit handler (static wiring) is ALREADY installed for
    // THIS fixture actor: litebus::Spawn runs Init at startup, which binds
    // exitHandler_ to the live actor. Calling Init() again here would
    // double-register the Receive handlers and abort the actor framework.
    auto harness = MakeRealView();
    actor_->BindInstanceControlView(harness.view);

    auto entry = MakeViewEntry("inst-rfe", "runtime-rfe", "sbox-rfe", "gen-rfe");
    entry.set_functionproxyid("node-test");
    entry.set_function("default/0-test-helloWorld/$latest");
    entry.set_tenantid("tenant-rfe");
    harness.view->GenerateStateMachine(entry.instanceid(), entry);

    // The authoritative view answers the sampled allocation; settlement
    // (ApplyPhysicalRetirement) removes it — swap the served unit INSIDE
    // the settlement continuation, exactly what the real atomic
    // retirement does to the view.
    ServeView(MakeUnitWithEntry(entry));
    EXPECT_CALL(*primary_, ApplyPhysicalRetirement(_))
        .WillOnce(Invoke([this, entry](const resource_view::PhysicalRetirementReceipt &receipt) {
            settleReceipts.push_back(receipt);
            ServeView(MakeUnitWithEntry(MakeElasticInfo("inst-other", "runtime-other")));
            auto promise = std::make_shared<litebus::Promise<Status>>();
            promise->SetValue(Status::OK());
            return promise->GetFuture();
        }));
    // ShutDownInstance's client probe: no client -> graceful skip -> OK.
    ON_CALL(*clientManager_, GetControlInterfacePosixClient(_))
        .WillByDefault(Invoke([](const std::string &) {
            auto promise = std::make_shared<
                litebus::Promise<std::shared_ptr<ControlInterfacePosixClient>>>();
            promise->SetValue(std::shared_ptr<ControlInterfacePosixClient>{});
            return promise->GetFuture();
        }));

    // State delete fails once (the pending-record trigger), succeeds after.
    EXPECT_CALL(*cacheClient_, Del(Matcher<const std::string &>(StateClient::ComposeStateKey("inst-rfe", "req-inst-rfe"))))
        .WillOnce(Return(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "store down")))
        .WillOnce(Return(Status::OK()));

    // The REAL first entry: the production exit handler for a
    // machine-tracked instance with an agent (not the helper).
    auto first = actor_->exitHandler_(entry);
    ASSERT_AWAIT_READY(first);
    ASSERT_TRUE(first.Get().IsError())
        << "the failed state cleanup must surface through the real entry: "
        << first.Get().ToString();
    // The physical retirement DID happen through the real chain.
    ASSERT_EQ(killRequests.size(), 1u);
    EXPECT_EQ(killRequests[0]->expected_sandbox_id(), "sbox-rfe");
    EXPECT_EQ(settleReceipts.size(), 1u);
    // Machine removed by the real DelInstance+OnDelInstance; pending record
    // written with THIS incarnation's full identity.
    EXPECT_EQ(harness.view->GetInstance("inst-rfe"), nullptr);
    const auto pending = actor_->settledCleanupPending_.find("inst-rfe");
    ASSERT_NE(pending, actor_->settledCleanupPending_.end());
    EXPECT_EQ(pending->second.runtimeID, "runtime-rfe");
    EXPECT_EQ(pending->second.requestID, entry.requestid());
    EXPECT_EQ(pending->second.tenantID, "tenant-rfe");

    // REAL KillFrontend retry completes via the record.
    auto killReq = std::make_shared<KillRequest>();
    killReq->set_instanceid("inst-rfe");
    killReq->set_requestid("req-rfe");
    auto second = actor_->KillFrontend("tenant-rfe", killReq);
    ASSERT_AWAIT_READY(second);
    EXPECT_EQ(static_cast<int>(second.Get().code()), static_cast<int>(common::ERR_NONE))
        << second.Get().message();

    // No record anymore: the genuine NOT_FOUND.
    auto third = actor_->KillFrontend("tenant-rfe", killReq);
    ASSERT_AWAIT_READY(third);
    EXPECT_EQ(static_cast<int>(third.Get().code()),
              static_cast<int>(common::ERR_INSTANCE_NOT_FOUND));
}


// ── TRAEFIK OWNERSHIP WIRING (#3) ────────────────────────────────────────────
// NOTE: registry-side semantics (marker txn, all red cases incl. the
// barrier/ordering/takeover/collision/neighbor shapes) live in
// traefik_registry_test.cpp against the FakeEvalTxn executing
// production-assembled transactions. The two former actor-wiring tests here
// used the superseded in-memory-map API and were removed rather than
// ported-to-green; the actor->registry wiring re-test (UnregisterTraefikRouteFenced
// carries tenant+runtime+request+revision+record-key) is pending in the
// unified build round.

}  // namespace functionsystem::test
