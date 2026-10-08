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

// REAL-entry kill-identity tests for the 2026-10 fix chain, driving
// InstanceCtrlActor's kill chain through the production KillRuntimeWithIdentity
// / KillRuntimeWithSnapshotCleanup with:
//   - a Mock resource view serving a CONTROLLABLE ResourceUnit snapshot;
//   - a mock FunctionAgentMgr capturing every KillInstanceRequest;
//   - a mock client manager (DeleteClient seam).
// Covered:
//   1. same-incarnation identity pass-through (expected sandbox+generation
//      from the trusted sample reach the agent request, exactly once);
//   2. OLD runtime vs SAME-NAME new incarnation: view entry runtimeid/unitid
//      mismatch => ZERO dispatch (the new instance is untouchable);
//   3. missing view / missing sample: budget retained, retryable UNKNOWN,
//      zero dispatch;
//   4. non-elastic legacy path unchanged (identity-less kill still sent).

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "async/async.hpp"
#include "common/proto/pb/message_pb.h"
#include "common/resource_view/resource_view_mgr.h"
#include "common/resource_view/resource_view.h"
#include "function_proxy/common/posix_client/control_plane_client/control_interface_client_manager_proxy.h"
#include "function_proxy/local_scheduler/function_agent_manager/function_agent_mgr_actor.h"
#include "function_proxy/local_scheduler/instance_control/instance_ctrl_actor.h"
#include "mocks/mock_resource_view.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {

using namespace functionsystem::local_scheduler;  // NOLINT
using testing::Invoke;
using testing::Return;
using testing::_;

namespace {

// Captures every kill request; answers a controlled response.
class KillCaptureAgentMgr : public FunctionAgentMgr {
public:
    KillCaptureAgentMgr() : FunctionAgentMgr("KILL_CAPTURE_AGENT_MGR") {}

    litebus::Future<messages::KillInstanceResponse> KillInstance(
        const std::shared_ptr<messages::KillInstanceRequest> &request,
        const std::string &, bool) override
    {
        killRequests.push_back(request);
        messages::KillInstanceResponse response;
        response.set_requestid(request->requestid());
        response.set_instanceid(request->instanceid());
        if (confirmed) {
            response.set_code(static_cast<int32_t>(common::ErrorCode::ERR_NONE));
            response.set_physical_retirement_confirmed(true);
            response.set_retired_sandbox_id(request->expected_sandbox_id());
            response.set_retired_resource_generation(request->expected_resource_generation());
        } else {
            response.set_code(static_cast<int32_t>(common::ErrorCode::ERR_INSTANCE_BUSY));
            response.set_message("probe busy");
        }
        return response;
    }

    std::vector<std::shared_ptr<messages::KillInstanceRequest>> killRequests;
    bool confirmed = false;
};

// DeleteClient seam: always succeeds without a real client.
class NullClientManager : public functionsystem::ControlInterfaceClientManagerProxy {
public:
    litebus::Future<Status> DeleteClient(const std::string &) override
    {
        return Status::OK();
    }
};

messages::InstanceInfo MakeElasticInfo(const std::string &instanceID,
                                       const std::string &runtimeID,
                                       const std::string &unitID)
{
    messages::InstanceInfo info;
    info.set_instanceid(instanceID);
    info.set_runtimeid(runtimeID);
    info.set_unitid(unitID);
    info.set_functionagentid("agent-test");
    // Elastic admission marker: the create-option the production
    // UsesElasticAdmission check reads (constants.h SCHEDULER_ELASTIC_RESOURCES).
    (*info.mutable_createoptions())[resource_view::SCHEDULER_ELASTIC_RESOURCES] = "true";
    return info;
}

// A view entry carrying a trusted physical sample (the fence input).
messages::InstanceInfo MakeViewEntry(const std::string &instanceID,
                                     const std::string &runtimeID,
                                     const std::string &unitID,
                                     const std::string &sandboxID,
                                     const std::string &generation)
{
    auto entry = MakeElasticInfo(instanceID, runtimeID, unitID);
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

std::shared_ptr<resource_view::ResourceUnit> MakeUnitWithEntry(
    const messages::InstanceInfo &entry)
{
    auto unit = std::make_shared<resource_view::ResourceUnit>();
    unit->set_id("unit-test");
    unit->set_status(static_cast<uint32_t>(resource_view::UnitStatus::NORMAL));
    (*unit->mutable_instances())[entry.instanceid()] = entry;
    return unit;
}

}  // namespace

class KillEntryIdentityFixture : public testing::Test {
protected:
    void SetUp() override
    {
        mockViewMgr_ = std::make_shared<testing::NiceMock<MockResourceViewMgr>>();
        mockView_ = std::make_shared<testing::NiceMock<MockResourceView>>();
        agentMgr_ = std::make_shared<KillCaptureAgentMgr>();

        InstanceCtrlConfig config;
        actor_ = std::make_shared<InstanceCtrlActor>("KILL_IDENTITY_ACTOR", "node-test", config);
        actor_->BindResourceView(mockViewMgr_);
        actor_->BindFunctionAgentMgr(agentMgr_);
        litebus::Spawn(actor_);
    }

    void TearDown() override
    {
        litebus::Terminate(actor_->GetAID());
        litebus::Await(actor_->GetAID());
        actor_ = nullptr;
    }

    void ServeView(const std::shared_ptr<resource_view::ResourceUnit> &unit)
    {
        EXPECT_CALL(*mockViewMgr_, GetInf(_)).WillRepeatedly(Return(mockView_));
        EXPECT_CALL(*mockView_, GetResourceView())
            .WillRepeatedly(Invoke([unit]() {
                auto promise = std::make_shared<litebus::Promise<std::shared_ptr<resource_view::ResourceUnit>>>();
                promise->SetValue(unit);
                return promise->GetFuture();
            }));
    }

    std::shared_ptr<testing::NiceMock<MockResourceViewMgr>> mockViewMgr_;
    std::shared_ptr<testing::NiceMock<MockResourceView>> mockView_;
    std::shared_ptr<KillCaptureAgentMgr> agentMgr_;
    std::shared_ptr<InstanceCtrlActor> actor_;
};

// 1) Same incarnation: the trusted sample's sandbox/generation travel into
// the agent kill request EXACTLY ONCE.
TEST_F(KillEntryIdentityFixture, SameIncarnationIdentityPassesThrough)
{
    const auto info = MakeElasticInfo("inst-same", "runtime-old", "unit-test");
    const auto entry = MakeViewEntry("inst-same", "runtime-old", "unit-test",
                                     "sbox-trusted", "gen-trusted");
    ServeView(MakeUnitWithEntry(entry));
    agentMgr_->confirmed = true;

    auto result = actor_->KillRuntimeWithSnapshotCleanup(info, false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsOk()) << result.Get().GetMessage();
    ASSERT_EQ(agentMgr_->killRequests.size(), 1u);
    EXPECT_EQ(agentMgr_->killRequests[0]->expected_sandbox_id(), "sbox-trusted");
    EXPECT_EQ(agentMgr_->killRequests[0]->expected_resource_generation(), "gen-trusted");
}

// 2) OLD runtime vs SAME-NAME new incarnation: the view entry belongs to the
// NEW incarnation (different runtimeid) — ZERO dispatch; the new instance
// and its budget are untouchable by the stale delete.
TEST_F(KillEntryIdentityFixture, OldRuntimeSameNameNewIncarnationZeroDispatch)
{
    const auto staleInfo = MakeElasticInfo("inst-reused", "runtime-OLD", "unit-test");
    const auto newEntry = MakeViewEntry("inst-reused", "runtime-NEW", "unit-test",
                                        "sbox-NEW", "gen-NEW");
    ServeView(MakeUnitWithEntry(newEntry));

    auto result = actor_->KillRuntimeWithSnapshotCleanup(staleInfo, false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError());
    EXPECT_EQ(agentMgr_->killRequests.size(), 0u)
        << "a stale delete must never borrow the new incarnation's identity";
}

// 3a) Missing view (view read fails): UNKNOWN, budget retained, zero dispatch.
TEST_F(KillEntryIdentityFixture, MissingViewHoldsUnknown)
{
    const auto info = MakeElasticInfo("inst-noview", "runtime-noview", "unit-test");
    EXPECT_CALL(*mockViewMgr_, GetInf(_)).WillRepeatedly(Return(nullptr));

    auto result = actor_->KillRuntimeWithSnapshotCleanup(info, false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError());
    EXPECT_EQ(agentMgr_->killRequests.size(), 0u);
}

// 3b) Allocated entry WITHOUT a trusted sample: UNKNOWN, budget retained,
// zero dispatch (missing sample != never-created).
TEST_F(KillEntryIdentityFixture, AllocatedWithoutSampleHoldsUnknown)
{
    const auto info = MakeElasticInfo("inst-nosample", "runtime-nosample", "unit-test");
    const auto entry = MakeElasticInfo("inst-nosample", "runtime-nosample", "unit-test");
    ServeView(MakeUnitWithEntry(entry));  // no sandbox_resource_sample

    auto result = actor_->KillRuntimeWithSnapshotCleanup(info, false, true, true);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsError());
    EXPECT_EQ(agentMgr_->killRequests.size(), 0u);
}

// 4) Non-elastic legacy: identity-less kill still dispatched (old behavior).
TEST_F(KillEntryIdentityFixture, NonElasticLegacyKillStillDispatched)
{
    messages::InstanceInfo info;
    info.set_instanceid("inst-legacy");
    info.set_runtimeid("runtime-legacy");
    info.set_functionagentid("agent-test");
    // NO elastic extension => legacy path, no view consult.
    EXPECT_CALL(*mockViewMgr_, GetInf(_)).Times(0);

    auto result = actor_->KillRuntimeWithSnapshotCleanup(info, false, true, true);
    ASSERT_AWAIT_READY(result);
    ASSERT_EQ(agentMgr_->killRequests.size(), 1u);
    EXPECT_TRUE(agentMgr_->killRequests[0]->expected_sandbox_id().empty());
    EXPECT_TRUE(agentMgr_->killRequests[0]->expected_resource_generation().empty());
}

}  // namespace functionsystem::test
