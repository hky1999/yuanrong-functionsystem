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
#include "mocks/mock_internal_iam.h"
#include "mocks/mock_resource_view.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {

using namespace functionsystem::local_scheduler;  // NOLINT
using namespace resource_view;                    // NOLINT
using testing::_;
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
        litebus::Spawn(actor_);
    }

    void TearDown() override
    {
        if (actor_ != nullptr) {
            litebus::Terminate(actor_->GetAID());
            litebus::Await(actor_);
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

    std::shared_ptr<InstanceCtrlActor> actor_;
    std::shared_ptr<MockControlInterfaceClientManagerProxy> clientManager_;
    std::shared_ptr<MockFunctionAgentMgr> functionAgentMgr_;
    std::shared_ptr<ResourceViewMgr> resourceViewMgr_;
    std::shared_ptr<MockResourceView> primary_;
    std::shared_ptr<MockInternalIAM> internalIAM_;
    std::vector<std::shared_ptr<messages::KillInstanceRequest>> killRequests;
    std::vector<resource_view::PhysicalRetirementReceipt> settleReceipts;
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
TEST_F(KillEntryIdentityFixture, MissingViewHoldsUnknown)
{
    // SetUp binds a primary view; the MISSING-VIEW input requires it gone.
    resourceViewMgr_->primary_ = nullptr;
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

// 4) Non-elastic legacy: identity-less kill STILL dispatched (old path).
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
TEST_F(KillEntryIdentityFixture, OtherEntryKillRuntimeNullViewHoldsUnknown)
{
    resourceViewMgr_->primary_ = nullptr;
    auto result = actor_->KillRuntime(
        MakeElasticInfo("inst-noview2", "runtime-noview2"), false);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError());
    EXPECT_EQ(killRequests.size(), 0u);
}

}  // namespace functionsystem::test
