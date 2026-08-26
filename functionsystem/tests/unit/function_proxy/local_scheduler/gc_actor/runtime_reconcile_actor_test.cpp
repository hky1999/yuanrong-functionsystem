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

#include "function_proxy/local_scheduler/gc_actor/runtime_reconcile_actor.h"

#include <atomic>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <memory>
#include <string>
#include <unordered_map>

#include "async/async.hpp"
#include "common/constants/actor_name.h"
#include "common/types/instance_state.h"
#include "common/utils/struct_transfer.h"
#include "mocks/mock_function_agent_mgr.h"
#include "mocks/mock_instance_control_view.h"
#include "mocks/mock_instance_ctrl.h"
#include "mocks/mock_instance_state_machine.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {

using namespace local_scheduler;
using namespace ::testing;

static const std::string TEST_NODE_ID = "reconcile-test-node";
static const std::string TEST_AGENT_ID = "agent-001";
static const std::string TEST_INSTANCE_ID_1 = "inst-001";
static const std::string TEST_INSTANCE_ID_2 = "inst-002";
static const std::string TEST_RUNTIME_ID_1 = "rt-001";
static const std::string TEST_RUNTIME_ID_2 = "rt-002";
static const std::string TEST_CONTAINER_ID_1 = "ctr-001";
static const std::string TEST_CONTAINER_ID_2 = "ctr-002";

/**
 * Helper: build an InstanceInfo proto with the given fields.
 */
static resources::InstanceInfo MakeInstanceInfo(const std::string &agentID,
                                                 const std::string &runtimeID,
                                                 const std::string &containerID)
{
    resources::InstanceInfo info;
    info.set_functionagentid(agentID);
    info.set_functionproxyid(TEST_NODE_ID);
    info.set_runtimeid(runtimeID);
    info.set_containerid(containerID);
    return info;
}

static litebus::Future<messages::ReconcileRuntimesResponse> MakeSuccessReconcileResponse(
    const std::shared_ptr<messages::ReconcileRuntimesRequest> &request)
{
    messages::ReconcileRuntimesResponse resp;
    resp.set_requestid(request->requestid());
    resp.set_code(0);
    return AsyncReturn(resp);
}

/**
 * Helper: build instances map with mock state machines.
 */
static std::unordered_map<std::string, std::shared_ptr<InstanceStateMachine>>
MakeInstancesMap(const std::vector<std::tuple<std::string, std::string, std::string, std::string>> &entries)
{
    std::unordered_map<std::string, std::shared_ptr<InstanceStateMachine>> instances;
    for (const auto &[instanceID, agentID, runtimeID, containerID] : entries) {
        auto sm = std::make_shared<MockInstanceStateMachine>(TEST_NODE_ID);
        auto info = MakeInstanceInfo(agentID, runtimeID, containerID);
        ON_CALL(*sm, GetInstanceInfo()).WillByDefault(Return(info));
        ON_CALL(*sm, GetInstanceState()).WillByDefault(Return(InstanceState::RUNNING));
        instances[instanceID] = sm;
    }
    return instances;
}

/**
 * Test fixture for RuntimeReconcileActor (trigger-once model).
 */
class RuntimeReconcileActorTest : public ::testing::Test {
public:
    void SetUp() override
    {
        mockFunctionAgentMgr_ = std::make_shared<MockFunctionAgentMgr>("test-agent-mgr", nullptr);
        mockInstanceCtrl_ = std::make_shared<MockInstanceCtrl>();
        mockInstanceControlView_ = std::make_shared<MockInstanceControlView>(TEST_NODE_ID);
        EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(TEST_NODE_ID, _))
            .Times(AnyNumber())
            .WillRepeatedly(Invoke([](const std::string &,
                                      const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
                return MakeSuccessReconcileResponse(request);
            }));
    }

    void TearDown() override
    {
        if (reconcileActor_ != nullptr) {
            litebus::Terminate(reconcileActor_->GetAID());
            litebus::Await(reconcileActor_->GetAID());
            reconcileActor_ = nullptr;
        }
        mockFunctionAgentMgr_ = nullptr;
        mockInstanceCtrl_ = nullptr;
        mockInstanceControlView_ = nullptr;
    }

    void CreateAndSpawnActor()
    {
        reconcileActor_ = std::make_shared<RuntimeReconcileActor>(
            RUNTIME_RECONCILE_ACTOR_NAME, TEST_NODE_ID);
        reconcileActor_->BindInstanceControlView(mockInstanceControlView_);
        reconcileActor_->BindInstanceCtrl(mockInstanceCtrl_);
        reconcileActor_->BindFunctionAgentMgr(mockFunctionAgentMgr_);
        litebus::Spawn(reconcileActor_);
    }

protected:
    std::shared_ptr<MockFunctionAgentMgr> mockFunctionAgentMgr_;
    std::shared_ptr<MockInstanceCtrl> mockInstanceCtrl_;
    std::shared_ptr<MockInstanceControlView> mockInstanceControlView_;
    std::shared_ptr<RuntimeReconcileActor> reconcileActor_;
};

// ---------------------------------------------------------------------------

/**
 * Feature: Reconciliation sends expected runtime entries to agent.
 * Description:
 *   When instances exist in the InstanceControlView, the reconcile actor
 *   should call ReconcileRuntimes on FunctionAgentMgr with the correct
 *   {runtimeID, containerID} entries.
 */
TEST_F(RuntimeReconcileActorTest, SendsCorrectEntriesToAgent)
{
    auto instances = MakeInstancesMap({
        {TEST_INSTANCE_ID_1, TEST_AGENT_ID, TEST_RUNTIME_ID_1, TEST_CONTAINER_ID_1},
        {TEST_INSTANCE_ID_2, TEST_AGENT_ID, TEST_RUNTIME_ID_2, TEST_CONTAINER_ID_2},
    });

    EXPECT_CALL(*mockInstanceControlView_, GetInstances())
        .WillRepeatedly(Return(instances));

    std::atomic<bool> called{false};
    EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(TEST_AGENT_ID, _))
        .WillRepeatedly(Invoke([&called](const std::string &,
                                          const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
            EXPECT_EQ(request->entries_size(), 2);
            std::unordered_set<std::string> runtimeIDs;
            std::unordered_set<std::string> containerIDs;
            for (const auto &entry : request->entries()) {
                runtimeIDs.insert(entry.runtimeid());
                containerIDs.insert(entry.containerid());
            }
            EXPECT_TRUE(runtimeIDs.count(TEST_RUNTIME_ID_1) > 0);
            EXPECT_TRUE(runtimeIDs.count(TEST_RUNTIME_ID_2) > 0);
            EXPECT_TRUE(containerIDs.count(TEST_CONTAINER_ID_1) > 0);
            EXPECT_TRUE(containerIDs.count(TEST_CONTAINER_ID_2) > 0);

            called = true;
            return MakeSuccessReconcileResponse(request);
        }));

    CreateAndSpawnActor();
    reconcileActor_->TriggerOnce(TEST_AGENT_ID);
    ASSERT_AWAIT_TRUE([&called]() { return called.load(); });
}

TEST_F(RuntimeReconcileActorTest, SendsPersistedPortMappingsToAgent)
{
    const std::string portMappings =
        R"(["public+http:21006:18080","direct+http:21007:50090"])";
    auto stateMachine = std::make_shared<MockInstanceStateMachine>(TEST_NODE_ID);
    auto info = MakeInstanceInfo(TEST_AGENT_ID, TEST_RUNTIME_ID_1, TEST_CONTAINER_ID_1);
    (*info.mutable_extensions())[PORT_FORWARD_KEY] = portMappings;
    ON_CALL(*stateMachine, GetInstanceInfo()).WillByDefault(Return(info));
    ON_CALL(*stateMachine, GetInstanceState()).WillByDefault(Return(InstanceState::RUNNING));

    std::unordered_map<std::string, std::shared_ptr<InstanceStateMachine>> instances{
        {TEST_INSTANCE_ID_1, stateMachine},
    };
    EXPECT_CALL(*mockInstanceControlView_, GetInstances())
        .WillRepeatedly(Return(instances));

    std::atomic<bool> called{false};
    EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(TEST_AGENT_ID, _))
        .WillRepeatedly(Invoke([&called, &portMappings](
                                   const std::string &,
                                   const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
            EXPECT_EQ(request->entries_size(), 1);
            if (request->entries_size() == 1) {
                EXPECT_EQ(request->entries(0).runtimeid(), TEST_RUNTIME_ID_1);
                EXPECT_EQ(request->entries(0).portmappings(), portMappings);
            }
            called = true;
            return MakeSuccessReconcileResponse(request);
        }));

    CreateAndSpawnActor();
    reconcileActor_->TriggerOnce(TEST_AGENT_ID);
    ASSERT_AWAIT_TRUE([&called]() { return called.load(); });
}

/**
 * Feature: Ghost instances are force-deleted.
 * Description:
 *   When the executor reports missingIDs in the reconcile response,
 *   the reconcile actor should call ForceDeleteInstance for the
 *   corresponding proxy instances.
 */
TEST_F(RuntimeReconcileActorTest, GhostInstancesAreForceDeleted)
{
    auto instances = MakeInstancesMap({
        {TEST_INSTANCE_ID_1, TEST_AGENT_ID, TEST_RUNTIME_ID_1, TEST_CONTAINER_ID_1},
    });

    EXPECT_CALL(*mockInstanceControlView_, GetInstances())
        .WillRepeatedly(Return(instances));

    EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(TEST_AGENT_ID, _))
        .WillRepeatedly(Invoke([](const std::string &,
                                   const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
            messages::ReconcileRuntimesResponse resp;
            resp.set_requestid(request->requestid());
            resp.set_code(0);
            resp.set_orphanscleaned(0);
            resp.add_missingids(TEST_CONTAINER_ID_1);
            return AsyncReturn(resp);
        }));

    std::atomic<bool> deleted{false};
    EXPECT_CALL(*mockInstanceCtrl_, ForceDeleteInstance(TEST_INSTANCE_ID_1))
        .WillRepeatedly(Invoke([&deleted](const std::string &) {
            deleted = true;
            return AsyncReturn(Status::OK());
        }));

    CreateAndSpawnActor();
    reconcileActor_->TriggerOnce(TEST_AGENT_ID);
    ASSERT_AWAIT_TRUE([&deleted]() { return deleted.load(); });
}

/**
 * Feature: No action when all containers match.
 * Description:
 *   When the reconcile response has no missingIDs and no orphans,
 *   ForceDeleteInstance should not be called.
 */
TEST_F(RuntimeReconcileActorTest, NoActionWhenAllContainersMatch)
{
    auto instances = MakeInstancesMap({
        {TEST_INSTANCE_ID_1, TEST_AGENT_ID, TEST_RUNTIME_ID_1, TEST_CONTAINER_ID_1},
    });

    EXPECT_CALL(*mockInstanceControlView_, GetInstances())
        .WillRepeatedly(Return(instances));

    std::atomic<bool> reconciled{false};
    EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(TEST_AGENT_ID, _))
        .WillRepeatedly(Invoke([&reconciled](const std::string &,
                                               const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
            messages::ReconcileRuntimesResponse resp;
            resp.set_requestid(request->requestid());
            resp.set_code(0);
            reconciled = true;
            return AsyncReturn(resp);
        }));

    EXPECT_CALL(*mockInstanceCtrl_, ForceDeleteInstance(_)).Times(0);

    CreateAndSpawnActor();
    reconcileActor_->TriggerOnce(TEST_AGENT_ID);
    ASSERT_AWAIT_TRUE([&reconciled]() { return reconciled.load(); });
}

/**
 * Feature: First-pass with empty instances still triggers reconcile.
 * Description:
 *   When proxy has no local instance for the agent (e.g. proxy restart with
 *   empty etcd sync), first-pass reconcile should still send an empty-entries
 *   request so the executor can detect and clean orphans from its own view.
 */
TEST_F(RuntimeReconcileActorTest, EmptyInstancesStillTriggersReconcile)
{
    std::unordered_map<std::string, std::shared_ptr<InstanceStateMachine>> emptyInstances;
    EXPECT_CALL(*mockInstanceControlView_, GetInstances())
        .WillRepeatedly(Return(emptyInstances));

    std::atomic<bool> called{false};
    EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(TEST_AGENT_ID, _))
        .WillRepeatedly(Invoke([&called](const std::string &,
                                          const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
            EXPECT_EQ(request->entries_size(), 0);
            messages::ReconcileRuntimesResponse resp;
            resp.set_requestid(request->requestid());
            resp.set_code(0);
            called = true;
            return AsyncReturn(resp);
        }));

    CreateAndSpawnActor();
    reconcileActor_->TriggerOnce(TEST_AGENT_ID);
    ASSERT_AWAIT_TRUE([&called]() { return called.load(); });
}

/**
 * Feature: Trigger-once model only runs when triggered.
 * Description:
 *   The reconcile actor does not automatically run cycles.
 *   Only TriggerOnce() initiates reconciliation.
 */
TEST_F(RuntimeReconcileActorTest, OnlyRunsOnTrigger)
{
    auto instances = MakeInstancesMap({
        {TEST_INSTANCE_ID_1, TEST_AGENT_ID, TEST_RUNTIME_ID_1, TEST_CONTAINER_ID_1},
    });

    EXPECT_CALL(*mockInstanceControlView_, GetInstances())
        .WillRepeatedly(Return(instances));

    std::atomic<int> reconcileCount{0};
    EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(TEST_AGENT_ID, _))
        .WillRepeatedly(Invoke([&reconcileCount](const std::string &,
                                                   const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
            messages::ReconcileRuntimesResponse resp;
            resp.set_requestid(request->requestid());
            resp.set_code(0);
            reconcileCount++;
            return AsyncReturn(resp);
        }));

    CreateAndSpawnActor();

    // No automatic reconciliation
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(reconcileCount.load(), 0);

    // Trigger manually
    reconcileActor_->TriggerOnce(TEST_AGENT_ID);
    ASSERT_AWAIT_TRUE([&reconcileCount]() { return reconcileCount.load() > 0; });

    // Should not trigger again automatically
    int countAfterTrigger = reconcileCount.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(reconcileCount.load(), countAfterTrigger);
}

/**
 * Feature: Reconcile response error code does not crash, no ghost cleanup.
 * Description:
 *   When ReconcileRuntimes returns a non-zero error code, the actor logs the
 *   error and skips ghost cleanup. UnitStatus is NOT mutated; the next periodic
 *   cycle will retry the reconcile.
 */
TEST_F(RuntimeReconcileActorTest, ErrorResponseNoCrashNoGhostCleanup)
{
    auto instances = MakeInstancesMap({
        {TEST_INSTANCE_ID_1, TEST_AGENT_ID, TEST_RUNTIME_ID_1, TEST_CONTAINER_ID_1},
    });

    EXPECT_CALL(*mockInstanceControlView_, GetInstances())
        .WillRepeatedly(Return(instances));

    std::atomic<bool> reconciled{false};
    EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(TEST_AGENT_ID, _))
        .WillRepeatedly(Invoke([&reconciled](const std::string &,
                                               const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
            messages::ReconcileRuntimesResponse resp;
            resp.set_requestid(request->requestid());
            resp.set_code(1);  // error
            resp.set_message("agent not registered");
            reconciled = true;
            return AsyncReturn(resp);
        }));

    EXPECT_CALL(*mockInstanceCtrl_, ForceDeleteInstance(_)).Times(0);

    CreateAndSpawnActor();
    reconcileActor_->TriggerOnce(TEST_AGENT_ID);
    ASSERT_AWAIT_TRUE([&reconciled]() { return reconciled.load(); });
}

/**
 * Feature: Multiple agents are reconciled independently.
 * Description:
 *   When instances belong to different agents, each agent should
 *   receive its own ReconcileRuntimes call with only its entries.
 */
TEST_F(RuntimeReconcileActorTest, MultipleAgentsReconciledIndependently)
{
    const std::string AGENT_ID_2 = "agent-002";

    auto instances = MakeInstancesMap({
        {TEST_INSTANCE_ID_1, TEST_AGENT_ID, TEST_RUNTIME_ID_1, TEST_CONTAINER_ID_1},
        {TEST_INSTANCE_ID_2, AGENT_ID_2, TEST_RUNTIME_ID_2, TEST_CONTAINER_ID_2},
    });

    EXPECT_CALL(*mockInstanceControlView_, GetInstances())
        .WillRepeatedly(Return(instances));

    std::atomic<bool> agent1Called{false};
    std::atomic<bool> agent2Called{false};

    EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(TEST_AGENT_ID, _))
        .WillRepeatedly(Invoke([&agent1Called](const std::string &,
                                                const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
            EXPECT_EQ(request->entries_size(), 1);
            EXPECT_EQ(request->entries(0).runtimeid(), TEST_RUNTIME_ID_1);
            messages::ReconcileRuntimesResponse resp;
            resp.set_requestid(request->requestid());
            resp.set_code(0);
            agent1Called = true;
            return AsyncReturn(resp);
        }));

    EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(AGENT_ID_2, _))
        .WillRepeatedly(Invoke([&agent2Called](const std::string &,
                                                const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
            EXPECT_EQ(request->entries_size(), 1);
            EXPECT_EQ(request->entries(0).runtimeid(), TEST_RUNTIME_ID_2);
            messages::ReconcileRuntimesResponse resp;
            resp.set_requestid(request->requestid());
            resp.set_code(0);
            agent2Called = true;
            return AsyncReturn(resp);
        }));

    CreateAndSpawnActor();
    // Trigger for both agents
    reconcileActor_->TriggerOnce(TEST_AGENT_ID);
    reconcileActor_->TriggerOnce(AGENT_ID_2);
    ASSERT_AWAIT_TRUE([&]() { return agent1Called.load() && agent2Called.load(); });
}

/**
 * Feature: Orphan cleanup is reported by executor.
 * Description:
 *   When the executor reports orphansCleaned > 0, the reconcile actor
 *   should log it but take no further action (cleanup is done by executor).
 */
TEST_F(RuntimeReconcileActorTest, OrphanCleanupReportedByExecutor)
{
    auto instances = MakeInstancesMap({
        {TEST_INSTANCE_ID_1, TEST_AGENT_ID, TEST_RUNTIME_ID_1, TEST_CONTAINER_ID_1},
    });

    EXPECT_CALL(*mockInstanceControlView_, GetInstances())
        .WillRepeatedly(Return(instances));

    std::atomic<bool> called{false};
    EXPECT_CALL(*mockFunctionAgentMgr_, ReconcileRuntimes(TEST_AGENT_ID, _))
        .WillRepeatedly(Invoke([&called](const std::string &,
                                          const std::shared_ptr<messages::ReconcileRuntimesRequest> &request) {
            messages::ReconcileRuntimesResponse resp;
            resp.set_requestid(request->requestid());
            resp.set_code(0);
            resp.set_orphanscleaned(3);
            resp.add_orphanids("orphan-1");
            resp.add_orphanids("orphan-2");
            resp.add_orphanids("orphan-3");
            called = true;
            return AsyncReturn(resp);
        }));

    // No ForceDeleteInstance should be called (orphans are handled by executor)
    EXPECT_CALL(*mockInstanceCtrl_, ForceDeleteInstance(_)).Times(0);

    CreateAndSpawnActor();
    reconcileActor_->TriggerOnce(TEST_AGENT_ID);
    ASSERT_AWAIT_TRUE([&called]() { return called.load(); });
}

}  // namespace functionsystem::test
