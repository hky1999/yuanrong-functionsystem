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

#include <gtest/gtest.h>

#define private public
#include "common/etcd_service/etcd_service_driver.h"
#include "common/logs/logging.h"
#include "common/metrics/metrics_adapter.h"
#include "common/proto/pb/message_pb.h"
#include "common/utils/actor_worker.h"
#include "function_proxy/common/state_machine/instance_state_machine.h"
#include "function_proxy/config/direct_routing_config.h"
#include "function_proxy/local_scheduler/instance_control/instance_ctrl_actor.h"
#include "mocks/mock_instance_operator.h"
#include "mocks/mock_instance_state_machine.h"
#include "mocks/mock_observer.h"
#include "utils/future_test_helper.h"
#include "utils/port_helper.h"

namespace functionsystem::test {

using namespace testing;

const std::string TEST_NODE_ID = "test node id";

class InstanceStateMachineTest : public ::testing::Test {
protected:
    [[maybe_unused]] static void SetUpTestSuite()
    {
        etcdSrvDriver_ = std::make_unique<meta_store::test::EtcdServiceDriver>();
        int metaStoreServerPort = functionsystem::test::FindAvailablePort();
        metaStoreServerHost_ = "127.0.0.1:" + std::to_string(metaStoreServerPort);
        etcdSrvDriver_->StartServer(metaStoreServerHost_);
        metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetEnabledInstruments(
            { functionsystem::metrics::YRInstrument::YR_INSTANCE_RUNNING_DURATION });
    }

    [[maybe_unused]] static void TearDownTestSuite()
    {
        etcdSrvDriver_->StopServer();
        InstanceStateMachine::UnBindControlPlaneObserver();
        metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetEnabledInstruments({});
    }

protected:
    inline static std::unique_ptr<meta_store::test::EtcdServiceDriver> etcdSrvDriver_;
    inline static std::string metaStoreServerHost_;
};

TEST_F(InstanceStateMachineTest, TransitionStateSuccessFromNew)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);
    auto ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().preState.IsSome());
    EXPECT_EQ(ret.Get().preState.Get(), InstanceState::NEW);
}

TEST_F(InstanceStateMachineTest, TransitionStateFailedFromNew)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);
    auto ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::CREATING, 0, "", false });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().preState.IsNone());
    ASSERT_TRUE(ret.Get().status.IsError());
}

TEST_F(InstanceStateMachineTest, LowReliabilityTypeTransitionStateToRunning)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("low-reliability-request");
    scheduleReq->mutable_instance()->set_requestid("low-reliability-request");
    scheduleReq->mutable_instance()->set_instanceid("low-reliability-instance");
    scheduleReq->mutable_instance()->set_functionproxyid(TEST_NODE_ID);
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::CREATING));
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->mutable_createoptions()->operator[](functionsystem::RELIABILITY_TYPE) = "low";
    scheduleReq->mutable_instance()->set_lowreliability(true);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, true);

    auto mockControlPlaneObserver = std::make_shared<MockObserver>();
    instanceStateMachine->BindControlPlaneObserver(mockControlPlaneObserver);
    EXPECT_CALL(*mockControlPlaneObserver, WatchInstance).Times(1).WillRepeatedly(Return());
    EXPECT_CALL(*mockControlPlaneObserver, PutInstanceEvent).Times(1).WillRepeatedly(Return(Status::OK()));

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    EXPECT_CALL(*mockInstanceOpt, Create(_, _, true))
        .WillOnce(Return(OperateResult{ Status::OK(), "", 0, 11 }));
    EXPECT_CALL(*mockInstanceOpt, Modify)
        .WillOnce(Return(OperateResult{ Status(StatusCode::FAILED), "", 1, 12 }));

    instanceStateMachine->SetDataSystemHost("127.0.0.1");
    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 0, "" });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().preState.IsSome());
    ASSERT_TRUE(!ret.Get().status.IsError());
    EXPECT_EQ(ret.Get().preState, InstanceState::CREATING);
    EXPECT_EQ(ret.Get().version, 1);
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 0, "" });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().preState.IsSome());
    ASSERT_TRUE(!ret.Get().status.IsError());
    EXPECT_EQ(ret.Get().preState, InstanceState::RUNNING);
    EXPECT_TRUE(ret.Get().status.IsOk());
    EXPECT_EQ(instanceStateMachine->GetLastSaveFailedState(), -1);
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::FATAL, 0, "" });
    ASSERT_AWAIT_READY(ret);
    EXPECT_EQ(instanceStateMachine->GetLastSaveFailedState(), 6);
    instanceStateMachine->UnBindControlPlaneObserver();
}

TEST_F(InstanceStateMachineTest, LowReliabilityInitialRunningConflictReturnsResolvedWinner)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    const std::string instanceID = "shared-low-reliability-instance";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("loser-request");
    auto loserInfo = scheduleReq->mutable_instance();
    loserInfo->set_requestid("loser-request");
    loserInfo->set_instanceid(instanceID);
    loserInfo->set_function(function);
    loserInfo->set_functionproxyid(TEST_NODE_ID);
    loserInfo->set_tenantid("tenant");
    loserInfo->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::CREATING));
    (*loserInfo->mutable_createoptions())[functionsystem::RELIABILITY_TYPE] = "low";
    loserInfo->set_lowreliability(true);

    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(
        TEST_NODE_ID, std::make_shared<InstanceContext>(scheduleReq), true);
    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;

    resources::InstanceInfo winnerInfo = *loserInfo;
    winnerInfo.set_requestid("winner-request");
    winnerInfo.set_functionproxyid("winner-proxy");
    winnerInfo.set_runtimeid("winner-runtime");
    winnerInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    winnerInfo.set_version(1);
    resources::RouteInfo routeInfo;
    TransToRouteInfoFromInstanceInfo(winnerInfo, routeInfo);
    std::string routeJson;
    ASSERT_TRUE(TransToJsonFromRouteInfo(routeJson, routeInfo));

    EXPECT_CALL(*mockInstanceOpt, Create(_, _, true))
        .WillOnce(Return(OperateResult{
            Status(StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION), "", 0, 0 }));
    EXPECT_CALL(*mockInstanceOpt, GetInstance(GenInstanceRouteKey(instanceID)))
        .WillOnce(Return(OperateResult{ Status::OK(), routeJson, 0, 17 }));

    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 0, "" });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().preState.IsNone());
    EXPECT_EQ(ret.Get().status.StatusCode(), StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION);
    EXPECT_EQ(ret.Get().savedInfo.instanceid(), winnerInfo.instanceid());
    EXPECT_EQ(ret.Get().savedInfo.requestid(), winnerInfo.requestid());
    EXPECT_EQ(ret.Get().savedInfo.functionproxyid(), winnerInfo.functionproxyid());
    EXPECT_EQ(ret.Get().savedInfo.instancestatus().code(), static_cast<int32_t>(InstanceState::RUNNING));
    EXPECT_TRUE(ret.Get().savedInfo.runtimeid().empty());
    EXPECT_EQ(ret.Get().previousInfo.requestid(), "loser-request");
    EXPECT_EQ(ret.Get().currentModRevision, 17);
}

TEST_F(InstanceStateMachineTest, LowReliabilityInitialRunningConflictRejectsInvalidRoute)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    const std::string instanceID = "shared-low-reliability-instance";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("loser-request");
    auto loserInfo = scheduleReq->mutable_instance();
    loserInfo->set_requestid("loser-request");
    loserInfo->set_instanceid(instanceID);
    loserInfo->set_function(function);
    loserInfo->set_functionproxyid(TEST_NODE_ID);
    loserInfo->set_tenantid("tenant");
    loserInfo->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::CREATING));
    (*loserInfo->mutable_createoptions())[functionsystem::RELIABILITY_TYPE] = "low";
    loserInfo->set_lowreliability(true);

    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(
        TEST_NODE_ID, std::make_shared<InstanceContext>(scheduleReq), true);
    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;

    resources::InstanceInfo winnerInfo = *loserInfo;
    winnerInfo.set_requestid("winner-request");
    winnerInfo.set_functionproxyid("winner-proxy");
    winnerInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    resources::RouteInfo routeInfo;
    TransToRouteInfoFromInstanceInfo(winnerInfo, routeInfo);
    routeInfo.clear_functionproxyid();
    std::string routeJson;
    ASSERT_TRUE(TransToJsonFromRouteInfo(routeJson, routeInfo));

    EXPECT_CALL(*mockInstanceOpt, Create(_, _, true))
        .WillOnce(Return(OperateResult{
            Status(StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION), "", 0, 0 }));
    EXPECT_CALL(*mockInstanceOpt, GetInstance(GenInstanceRouteKey(instanceID)))
        .WillOnce(Return(OperateResult{ Status::OK(), routeJson, 0, 18 }));

    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 0, "" });
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().preState.IsNone());
    EXPECT_TRUE(ret.Get().savedInfo.instanceid().empty());
    EXPECT_EQ(ret.Get().status.StatusCode(), StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION);
}

TEST_F(InstanceStateMachineTest, LowReliabilityInitialRunningConflictUsesSharedRouteOnly)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    const std::string instanceID = "shared-low-reliability-instance";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto loserInfo = scheduleReq->mutable_instance();
    loserInfo->set_requestid("loser-request");
    loserInfo->set_instanceid(instanceID);
    loserInfo->set_function(function);
    loserInfo->set_functionproxyid(TEST_NODE_ID);
    loserInfo->set_tenantid("tenant");
    loserInfo->set_lowreliability(true);
    loserInfo->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    (*loserInfo->mutable_createoptions())[functionsystem::RELIABILITY_TYPE] = "low";

    auto stateMachine = std::make_shared<InstanceStateMachine>(
        TEST_NODE_ID, std::make_shared<InstanceContext>(scheduleReq), true);
    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    stateMachine->instanceOpt_ = mockInstanceOpt;

    auto winnerInfo = *loserInfo;
    winnerInfo.set_requestid("winner-request");
    winnerInfo.set_functionproxyid("winner-proxy");
    winnerInfo.set_version(1);
    resources::RouteInfo routeInfo;
    TransToRouteInfoFromInstanceInfo(winnerInfo, routeInfo);
    std::string routeJson;
    ASSERT_TRUE(TransToJsonFromRouteInfo(routeJson, routeInfo));

    EXPECT_CALL(*mockInstanceOpt, GetInstance(GenInstanceRouteKey(instanceID)))
        .WillOnce(Return(OperateResult{ Status::OK(), routeJson, 0, 17 }));

    auto ret = stateMachine->ResolveInitialLowReliabilityCreateConflict(
        *loserInfo, *loserInfo,
        Status(StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION, "route exists"));
    ASSERT_AWAIT_READY(ret);
    EXPECT_EQ(ret.Get().savedInfo.instanceid(), winnerInfo.instanceid());
    EXPECT_EQ(ret.Get().savedInfo.requestid(), winnerInfo.requestid());
    EXPECT_EQ(ret.Get().savedInfo.functionproxyid(), winnerInfo.functionproxyid());
    EXPECT_TRUE(ret.Get().savedInfo.runtimeid().empty());
    EXPECT_EQ(ret.Get().currentModRevision, 17);
    EXPECT_EQ(ret.Get().status.StatusCode(), StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION);
}

TEST_F(InstanceStateMachineTest, LowReliabilityInitialRunningCreateIsNarrowlyScoped)
{
    const bool previousForceLowReliability = IsForceLowReliabilityInstanceEnabled();
    SetForceLowReliabilityInstance(false);
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto instance = scheduleReq->mutable_instance();
    instance->set_requestid("scope-request");
    instance->set_instanceid("scope-instance");
    instance->set_function("default/0-test-scope/$latest");
    instance->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    (*instance->mutable_createoptions())[functionsystem::RELIABILITY_TYPE] = "low";
    auto stateMachine = std::make_shared<InstanceStateMachine>(
        TEST_NODE_ID, std::make_shared<InstanceContext>(scheduleReq), true);

    EXPECT_TRUE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 0));
    EXPECT_FALSE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::SCHEDULING, 0));
    EXPECT_FALSE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 1));

    (*instance->mutable_createoptions())[functionsystem::RELIABILITY_TYPE] = "high";
    EXPECT_FALSE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 0));
    SetForceLowReliabilityInstance(true);
    EXPECT_TRUE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 0));
    SetForceLowReliabilityInstance(previousForceLowReliability);
    (*instance->mutable_createoptions())[functionsystem::RELIABILITY_TYPE] = "low";

    instance->set_groupid("group");
    EXPECT_FALSE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 0));
    instance->clear_groupid();

    (*instance->mutable_createoptions())[functionsystem::APP_ENTRYPOINT] = "driver.py";
    EXPECT_FALSE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 0));
    instance->mutable_createoptions()->erase(functionsystem::APP_ENTRYPOINT);

    (*instance->mutable_createoptions())[functionsystem::RESOURCE_OWNER_KEY] =
        functionsystem::STATIC_FUNCTION_OWNER_VALUE;
    EXPECT_FALSE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 0));
    instance->mutable_createoptions()->erase(functionsystem::RESOURCE_OWNER_KEY);

    instance->set_ischeckpointed(true);
    EXPECT_FALSE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 0));
    instance->set_ischeckpointed(false);

    instance->mutable_snapshotinfo();
    EXPECT_FALSE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 0));
    instance->clear_snapshotinfo();

    (*instance->mutable_createoptions())[functionsystem::RECOVER_RETRY_TIMES_KEY] = "1";
    EXPECT_FALSE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 0));
    instance->mutable_createoptions()->erase(functionsystem::RECOVER_RETRY_TIMES_KEY);

    SetForceLowReliabilityInstance(previousForceLowReliability);
    auto drGuard = function_proxy::DirectRoutingConfig::EnableForTest();
    EXPECT_FALSE(stateMachine->IsInitialLowReliabilityRunningPersistence(
        *instance, InstanceState::CREATING, 0));
}


TEST_F(InstanceStateMachineTest, HighReliabilityTypeTransitionStateToRunning)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->mutable_createoptions()->operator[](functionsystem::RELIABILITY_TYPE) = "high";
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    EXPECT_CALL(*mockInstanceOpt, Create).WillOnce(Return(OperateResult{ Status::OK(), "", 0 }));
    EXPECT_CALL(*mockInstanceOpt, Modify)
        .WillOnce(Return(OperateResult{ Status::OK(), "", 1 }))
        .WillOnce(Return(OperateResult{ Status::OK(), "", 2 }))
        .WillOnce(Return(OperateResult{ Status(StatusCode::FAILED), "", 3 }));

    instanceStateMachine->SetDataSystemHost("127.0.0.1");
    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "" });
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::CREATING, 0, "" });
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 0, "" });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().preState.IsSome());
    ASSERT_TRUE(!ret.Get().status.IsError());
    EXPECT_EQ(ret.Get().preState, InstanceState::CREATING);

    EXPECT_EQ(instanceStateMachine->GetLastSaveFailedState(), -1);
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::FATAL, 0, "" });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().status.IsError());
    EXPECT_EQ(instanceStateMachine->GetLastSaveFailedState(), 6);
}

TEST_F(InstanceStateMachineTest, DRModeHighReliabilitySkipsSchedulingCreatingWriteOnlyRunning)
{
    auto drGuard = function_proxy::DirectRoutingConfig::EnableForTest();

    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->mutable_createoptions()->operator[](functionsystem::RELIABILITY_TYPE) = "high";
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;

    // DR mode: SCHEDULING and CREATING are PERSISTENT_NOT (no etcd call).
    // First etcd write happens at RUNNING (Create), subsequent writes use Modify.
    EXPECT_CALL(*mockInstanceOpt, Create).WillOnce(Return(OperateResult{ Status::OK(), "", 0 }));
    EXPECT_CALL(*mockInstanceOpt, Modify)
        .WillOnce(Return(OperateResult{ Status(StatusCode::FAILED), "", 1 }));  // FATAL fails

    instanceStateMachine->SetDataSystemHost("127.0.0.1");

    // SCHEDULING: PERSISTENT_NOT in DR mode → no etcd call
    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "" });
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(!ret.Get().status.IsError());

    // CREATING: PERSISTENT_NOT in DR mode → no etcd call
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::CREATING, 0, "" });
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(!ret.Get().status.IsError());

    // RUNNING: first etcd write → Create (version == 0 → IsFirstPersistence returns true)
    ret = instanceStateMachine->TransitionTo(
        TransContext{ InstanceState::RUNNING, instanceStateMachine->GetVersion(), "" });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(!ret.Get().status.IsError());
    EXPECT_EQ(ret.Get().preState, InstanceState::CREATING);

    // FATAL: update existing entry → Modify (version > 0 → IsFirstPersistence returns false)
    EXPECT_EQ(instanceStateMachine->GetLastSaveFailedState(), -1);
    ret = instanceStateMachine->TransitionTo(
        TransContext{ InstanceState::FATAL, instanceStateMachine->GetVersion(), "" });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().status.IsError());
    EXPECT_EQ(instanceStateMachine->GetLastSaveFailedState(), 6);  // FATAL state code
}

TEST_F(InstanceStateMachineTest, NonDRModeUnchangedByDRConfig)
{
    // Verify non-DR mode is unaffected: behavior identical to HighReliabilityTypeTransitionStateToRunning
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->mutable_createoptions()->operator[](functionsystem::RELIABILITY_TYPE) = "high";
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    // Non-DR: Create at SCHEDULING, Modify at CREATING, RUNNING, FATAL (3 Modify calls)
    EXPECT_CALL(*mockInstanceOpt, Create).WillOnce(Return(OperateResult{ Status::OK(), "", 0 }));
    EXPECT_CALL(*mockInstanceOpt, Modify)
        .WillOnce(Return(OperateResult{ Status::OK(), "", 1 }))
        .WillOnce(Return(OperateResult{ Status::OK(), "", 2 }))
        .WillOnce(Return(OperateResult{ Status(StatusCode::FAILED), "", 3 }));

    instanceStateMachine->SetDataSystemHost("127.0.0.1");
    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "" });
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::CREATING, 0, "" });
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 0, "" });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(!ret.Get().status.IsError());
    EXPECT_EQ(ret.Get().preState, InstanceState::CREATING);

    EXPECT_EQ(instanceStateMachine->GetLastSaveFailedState(), -1);
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::FATAL, 0, "" });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().status.IsError());
    EXPECT_EQ(instanceStateMachine->GetLastSaveFailedState(), 6);
}

TEST_F(InstanceStateMachineTest, ExitRunningInstanceHandlerIsNull)
{
    InstanceStateMachine::SetExitHandler(nullptr);
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    instanceStateMachine->TransitionTo(TransContext{ InstanceState::CREATING, 0, "", false });
    instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 0, "", false });
    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    EXPECT_CALL(*mockInstanceOpt, Modify).WillOnce(Return(OperateResult{ Status::OK(), "", 1 }));

    auto promise = std::make_shared<litebus::Promise<Status>>();
    auto killContext = std::make_shared<KillContext>();
    killContext->instanceContext = instanceStateMachine->GetInstanceContextCopy();
    instanceStateMachine->TryExitInstance(promise, killContext);
    ASSERT_AWAIT_READY(promise->GetFuture());
    EXPECT_EQ(promise->GetFuture().Get().IsOk(), false);
}

/**
 * Feature: Exit old state which is exiting
 * Description: Exit old state which is exiting
 * Steps:
 * 1. Create context which instance status is exiting
 * 2. intance state machine try exit this instance
 *
 * Expectation:
 * 1. Status is Ok
 */
TEST_F(InstanceStateMachineTest, ExitOldStateExiting)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(5);
    scheduleReq->mutable_instance()->set_function(function);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);

    auto promise = std::make_shared<litebus::Promise<Status>>();
    auto killContext = std::make_shared<KillContext>();
    killContext->instanceContext = instanceStateMachine.GetInstanceContextCopy();
    instanceStateMachine.TryExitInstance(promise, killContext);
    ASSERT_AWAIT_READY(promise->GetFuture());
    ASSERT_TRUE(promise->GetFuture().Get().IsOk());
}

TEST_F(InstanceStateMachineTest, ExitRunningInstance)
{
    InstanceStateMachine::SetExitHandler(
        [](const resources::InstanceInfo &instanceInfo) -> litebus::Future<Status> { return Status::OK(); });
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    instanceStateMachine->TransitionTo(TransContext{ InstanceState::CREATING, 0, "", false });
    instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 0, "", false });

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    EXPECT_CALL(*mockInstanceOpt, Modify).WillOnce(Return(OperateResult{ Status::OK(), "", 1 }));

    auto promise = std::make_shared<litebus::Promise<Status>>();
    auto killContext = std::make_shared<KillContext>();
    killContext->instanceContext = instanceStateMachine->GetInstanceContextCopy();
    instanceStateMachine->TryExitInstance(promise, killContext);
    ASSERT_AWAIT_READY(promise->GetFuture());
    ASSERT_TRUE(promise->GetFuture().Get().IsOk());
}

TEST_F(InstanceStateMachineTest, ExitCreatingInstance)
{
    InstanceStateMachine::SetExitHandler(
        [](const resources::InstanceInfo &instanceInfo) -> litebus::Future<Status> { return Status::OK(); });
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    EXPECT_CALL(*mockInstanceOpt, Modify).WillOnce(Return(OperateResult{ Status::OK(), "", 1 }));

    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    ASSERT_AWAIT_READY(ret);
    EXPECT_EQ(ret.Get().preState, InstanceState::NEW);
    ASSERT_TRUE(!ret.Get().status.IsError());

    auto promise = std::make_shared<litebus::Promise<Status>>();
    auto killContext = std::make_shared<KillContext>();
    killContext->instanceContext = instanceStateMachine->GetInstanceContextCopy();
    instanceStateMachine->TryExitInstance(promise, killContext);
    ASSERT_AWAIT_READY(promise->GetFuture());
    ASSERT_TRUE(promise->GetFuture().Get().IsOk());
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::CREATING, 0, "", false });
    EXPECT_TRUE(ret.Get().preState.IsNone());
    ASSERT_TRUE(ret.Get().status.IsError());
}

TEST_F(InstanceStateMachineTest, ExitRunningInstanceWhenExitHandlerIsNULL)
{
    InstanceStateMachine::SetExitHandler(nullptr);
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    instanceStateMachine->TransitionTo(TransContext{ InstanceState::CREATING, 0, "", false });
    instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 0, "", false });

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    EXPECT_CALL(*mockInstanceOpt, Modify).WillOnce(Return(OperateResult{ Status::OK(), "", 1 }));

    auto promise = std::make_shared<litebus::Promise<Status>>();
    auto killContext = std::make_shared<KillContext>();
    killContext->instanceContext = instanceStateMachine->GetInstanceContextCopy();
    instanceStateMachine->TryExitInstance(promise, killContext);
    ASSERT_AWAIT_READY(promise->GetFuture());
    ASSERT_TRUE(promise->GetFuture().Get().IsError());
    ASSERT_TRUE(promise->GetFuture().Get() == StatusCode::ERR_STATE_MACHINE_ERROR);
    ASSERT_TRUE(promise->GetFuture().Get().GetMessage().find("failed to exit instance, exit handler is null") != std::string::npos);
    std::cout << promise->GetFuture().Get() << std::endl;
    InstanceStateMachine::SetExitHandler(
        [](const resources::InstanceInfo &instanceInfo) -> litebus::Future<Status> { return Status::OK(); });
}

TEST_F(InstanceStateMachineTest, StateChangeCallback)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->set_requestid("requestId");
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    litebus::Promise<resources::InstanceInfo> promise;
    std::unordered_set<InstanceState> statesConcerned = { InstanceState::RUNNING };
    instanceStateMachine->AddStateChangeCallback(
        statesConcerned, [promise](const resources::InstanceInfo &instanceInfo) { promise.SetValue(instanceInfo); },
        "key");

    auto actor = std::make_shared<local_scheduler::InstanceCtrlActor>("InstanceCtrlActor", "nodeID", local_scheduler::InstanceCtrlConfig{});
    actor->TransInstanceState(instanceStateMachine, TransContext{ InstanceState::SCHEDULING, 0, "", false });
    actor->TransInstanceState(instanceStateMachine, TransContext{ InstanceState::CREATING, 0, "", false });
    actor->TransInstanceState(instanceStateMachine, TransContext{ InstanceState::RUNNING, 0, "", false });
    auto future = promise.GetFuture();
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get().instancestatus().code(), static_cast<int32_t>(InstanceState::RUNNING));
}

TEST_F(InstanceStateMachineTest, ChangeSameStateTest)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->set_requestid("requestId");
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    instanceStateMachine->BindMetaStoreClient(
        MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ }));

    litebus::Promise<resources::InstanceInfo> promise;
    std::unordered_set<InstanceState> statesConcerned = { InstanceState::RUNNING };
    instanceStateMachine->AddStateChangeCallback(
        statesConcerned, [promise](const resources::InstanceInfo &instanceInfo) { promise.SetValue(instanceInfo); },
        "key");

    auto mockObserver = std::make_shared<MockObserver>();
    InstanceStateMachine::BindControlPlaneObserver(mockObserver);
    EXPECT_CALL(*mockObserver, WatchInstance).WillRepeatedly(Return());
    // only PutInstance 3 times, repeat trans state RUNNING doesn't trigger put
    EXPECT_CALL(*mockObserver, PutInstanceEvent)
        .WillOnce(testing::Return(Status::OK()))
        .WillOnce(testing::Return(Status::OK()))
        .WillOnce(testing::Return(Status::OK()));

    auto actor = std::make_shared<local_scheduler::InstanceCtrlActor>("InstanceCtrlActor-ChangeSameStateTest", "nodeID",
                                                                      local_scheduler::InstanceCtrlConfig{});
    litebus::Spawn(actor);
    actor->TransInstanceState(instanceStateMachine, TransContext{ InstanceState::SCHEDULING, 0, "", true })
        .Then([&](const TransitionResult &) {
            return actor->TransInstanceState(instanceStateMachine,
                                             TransContext{ InstanceState::CREATING, 1, "", true });
        })
        .Then([&](const TransitionResult &) {
            return actor->TransInstanceState(instanceStateMachine, TransContext{ InstanceState::RUNNING, 2, "", true });
        })
        .Then([&](const TransitionResult &) {
            return actor->TransInstanceState(instanceStateMachine, TransContext{ InstanceState::RUNNING, 3, "", true });
        });

    auto future = promise.GetFuture();
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get().instancestatus().code(), static_cast<int32_t>(InstanceState::RUNNING));
}

TEST_F(InstanceStateMachineTest, TransitionFromFatalToFailed)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);

    auto ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::CREATING, 0, "", false });
    ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::RUNNING, 0, "", false });
    ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::FATAL, 0, "", false });
    ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::FAILED, 0, "", false });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().preState.IsNone());
    ASSERT_TRUE(ret.Get().status.IsError());
}

TEST_F(InstanceStateMachineTest, TransitionFromExitingToFatal)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);

    auto ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::CREATING, 0, "", false });
    ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::RUNNING, 0, "", false });
    ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::FATAL, 0, "", false });
    ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::EXITING, 0, "", false });
    ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::FATAL, 0, "", false });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().preState.IsNone());
    ASSERT_TRUE(ret.Get().status.IsError());
}

TEST_F(InstanceStateMachineTest, TransitionPersistenceFromFatalToFailed)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->set_instanceid("instanceID");
    scheduleReq->mutable_instance()->set_requestid("requestID");
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    instanceStateMachine->BindMetaStoreClient(
        MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ }));

    auto mockControlPlaneObserver = std::make_shared<MockObserver>();
    instanceStateMachine->BindControlPlaneObserver(mockControlPlaneObserver);
    EXPECT_CALL(*mockControlPlaneObserver, WatchInstance("instanceID", _)).WillOnce(Return());

    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::SCHEDULING, 1, "", true });
    ret = instanceStateMachine->GetSavingFuture()
              .Then([instanceStateMachine](const bool &) {
                  return instanceStateMachine->TransitionTo(TransContext{ InstanceState::CREATING, 2, "", true });
              })
              .Then([instanceStateMachine](const TransitionResult &) {
                  return instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 3, "", true });
              })
              .Then([instanceStateMachine](const TransitionResult &) {
                  return instanceStateMachine->TransitionTo(TransContext{ InstanceState::EVICTING, 4, "", true });
              })
              .Then([instanceStateMachine](const TransitionResult &) {
                  return instanceStateMachine->TransitionTo(TransContext{ InstanceState::EVICTED, 5, "", true });
              });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().preState.IsNone());
    ASSERT_TRUE(ret.Get().status.IsError());
}

/**
 * Feature: TransitionTo with no context
 * Description: TransitionTo with no context
 * Steps:
 * 1. Set context null
 * 2. Call instanceStateMachine transitionTo
 *
 * Expectation:
 * 1. Future is None
 */
TEST_F(InstanceStateMachineTest, ErrTransitionTo)
{
    std::shared_ptr<InstanceContext> context = nullptr;
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);
    auto fut = instanceStateMachine.TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    ASSERT_AWAIT_READY(fut);
    ASSERT_TRUE(fut.Get().preState.IsNone());
    ASSERT_TRUE(fut.Get().status.IsError());
    auto status = instanceStateMachine.DelInstance("instance");
    EXPECT_TRUE(status.Get().IsError());
}

/**
 * Feature: DelInstanceSuccess
 * Description: DelInstanceSuccess
 * Steps:
 * 1. Delete instance
 *
 * Expectation:
 * 1. Status is Ok
 */
TEST_F(InstanceStateMachineTest, DelInstanceSuccess)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->set_functionagentid("funcAgent");
    scheduleReq->mutable_instance()->set_instanceid("instanceA");
    scheduleReq->mutable_instance()->set_functionproxyid("test node id");
    scheduleReq->mutable_instance()->set_requestid("req");

    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    auto mockControlPlaneObserver = std::make_shared<MockObserver>();
    instanceStateMachine->BindControlPlaneObserver(mockControlPlaneObserver);

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    EXPECT_CALL(*mockInstanceOpt, Delete).WillOnce(Return(OperateResult{ Status::OK(), "", 1 }));
    EXPECT_CALL(*mockControlPlaneObserver, CancelWatchInstance("instanceA")).WillOnce(Return());

    litebus::Future<Status> res = instanceStateMachine->DelInstance("instanceA");
    EXPECT_TRUE(res.Get().IsOk());
}

/**
 * Feature: UpdateInstanceContext with no context
 * Description: UpdateInstanceContext with no context
 * Steps:
 * 1. Set context null
 * 2. Delete instance
 *
 * Expectation:
 * 1. Status is Failed
 */
TEST_F(InstanceStateMachineTest, DelInstanceFailed)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    std::string instanceID = "instanceA";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(5);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->set_functionagentid("funcAgent");
    scheduleReq->mutable_instance()->set_instanceid("instanceA");
    scheduleReq->mutable_instance()->set_functionproxyid("test node id");
    scheduleReq->mutable_instance()->set_requestid("req");

    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    EXPECT_CALL(*mockInstanceOpt, Delete).WillOnce(Return(OperateResult{ Status(StatusCode::FAILED), "", 0 }));

    auto res = instanceStateMachine->DelInstance(instanceID);
    EXPECT_TRUE(res.Get().IsError());
    EXPECT_EQ(instanceStateMachine->lastSaveFailedState_.first, static_cast<int32_t>(InstanceState::EXITED));

    context = nullptr;
    instanceStateMachine->UpdateInstanceContext(context);
    res = instanceStateMachine->DelInstance(instanceID);
    EXPECT_TRUE(res.Get().IsError());
}

/**
 * Feature: SetScheduleTimes and GetScheduleTimes
 * Description: SetScheduleTimes and GetScheduleTimes
 * Steps:
 * 1. SetScheduleTimes
 * 2. Get correct ScheduleTimes
 *
 * Expectation:
 * 1. result is right
 */
TEST_F(InstanceStateMachineTest, SetScheduleTimes)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);

    std::string state = "scheduling";

    instanceStateMachine.SetScheduleTimes(1);
    instanceStateMachine.SetDeployTimes(1);
    EXPECT_EQ(instanceStateMachine.GetScheduleTimes(), 1);
    EXPECT_EQ(instanceStateMachine.GetDeployTimes(), 1);

    auto res = instanceStateMachine.GetInstanceInfo();
    EXPECT_EQ(res.function(), function);

    instanceStateMachine.ReleaseOwner();
    EXPECT_EQ(instanceStateMachine.GetOwner(), "");

    resources::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("change_instanceid");
    instanceStateMachine.UpdateInstanceInfo(instanceInfo);
    EXPECT_EQ(instanceStateMachine.GetInstanceInfo().instanceid(), "change_instanceid");
}

/**
 * Feature: SetScheduleTimes and GetScheduleTimes
 * Description: SetScheduleTimes and GetScheduleTimes
 * Steps:
 * 1. SetScheduleTimes
 * 2. Get correct ScheduleTimes
 *
 * Expectation:
 * 1. result is right
 */
TEST_F(InstanceStateMachineTest, ScheduleMutableSetters)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    resource_view::Resource resource;
    resource.set_name(resource_view::NPU_RESOURCE_NAME + "/"
                      + DEFAULT_NPU_PRODUCT + "/" + resource_view::HETEROGENEOUS_MEM_KEY);
    resource.set_type(resources::Value_Type_SCALAR);
    (*(scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()))
        [resource_view::NPU_RESOURCE_NAME + "/"
         + DEFAULT_NPU_PRODUCT + "/" + resource_view::HETEROGENEOUS_MEM_KEY] = resource;

    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);

    ScheduleResult result;
    result.id = "agent-id-0";
    result.realIDs = {0, 2, 5};
    auto name = resource_view::NPU_RESOURCE_NAME + "/310";
    auto &vectors = result.allocatedVectors[name];
    auto &cg = (*vectors.mutable_values())[resource_view::HETEROGENEOUS_MEM_KEY];
    for (int i = 0; i< 8; i++) {
        (*cg.mutable_vectors())["uuid"].add_values(1010);
    }

    instanceStateMachine.SetFunctionAgentIDAndHeteroConfig(result);
    ASSERT_TRUE(instanceStateMachine.GetScheduleRequest()->mutable_instance()->functionagentid() == "agent-id-0");
    EXPECT_EQ(instanceStateMachine.GetScheduleRequest()->instance().schedulerchain().size(), 1);
    EXPECT_EQ(instanceStateMachine.GetScheduleRequest()->instance().schedulerchain().Get(0), "agent-id-0");

    auto resources = instanceStateMachine.GetScheduleRequest()->instance().resources().resources();
    EXPECT_EQ(resources.at(name).type(), resources::Value_Type::Value_Type_VECTORS);
    EXPECT_EQ(resources.at(name).name(), name);
    EXPECT_EQ(resources.at(name).vectors().values().at(resource_view::HETEROGENEOUS_MEM_KEY)
                  .vectors().at("uuid").values().at(0), 1010);

    ASSERT_TRUE(instanceStateMachine.GetScheduleRequest()->instance().createoptions().at("func-NPU-DEVICE-IDS")
                == "0,2,5");

    instanceStateMachine.SetRuntimeAddress("runtime-address-0");
    ASSERT_TRUE(instanceStateMachine.GetScheduleRequest()->mutable_instance()->runtimeaddress() == "runtime-address-0");

    instanceStateMachine.SetRuntimeID("runtime-id-0");
    ASSERT_TRUE(instanceStateMachine.GetScheduleRequest()->mutable_instance()->runtimeid() == "runtime-id-0");

    instanceStateMachine.SetStartTime("runtime-start-time-0");
    ASSERT_TRUE(instanceStateMachine.GetScheduleRequest()->mutable_instance()->starttime() == "runtime-start-time-0");

    instanceStateMachine.IncreaseScheduleRound();
    ASSERT_TRUE(instanceStateMachine.GetScheduleRequest()->scheduleround() == 1);
}

TEST_F(InstanceStateMachineTest, CarriesSelectedPhysicalGpuIdsInCreateOptions)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto &resource = (*scheduleReq->mutable_instance()->mutable_resources()->mutable_resources())
        ["GPU/A10/count"];
    resource.set_name("GPU/A10/count");
    resource.set_type(resources::Value_Type_SCALAR);
    resource.mutable_scalar()->set_value(2);

    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);
    ScheduleResult result;
    result.id = "gpu-agent";
    result.realIDs = { 0, 2 };
    auto &count = (*result.allocatedVectors["GPU/A10"].mutable_values())
                      [resource_view::HETEROGENEOUS_CARDNUM_KEY];
    auto &vector = (*count.mutable_vectors())["gpu-agent"];
    vector.add_values(1);
    vector.add_values(0);
    vector.add_values(1);
    vector.add_values(0);

    instanceStateMachine.SetFunctionAgentIDAndHeteroConfig(result);

    EXPECT_EQ(instanceStateMachine.GetScheduleRequest()->instance().functionagentid(), "gpu-agent");
    EXPECT_EQ(instanceStateMachine.GetScheduleRequest()->instance().createoptions().at("func-GPU-DEVICE-IDS"),
              "0,2");
    const auto &allocated = instanceStateMachine.GetScheduleRequest()
                                ->instance()
                                .resources()
                                .resources()
                                .at("GPU/A10")
                                .vectors()
                                .values()
                                .at(resource_view::HETEROGENEOUS_CARDNUM_KEY)
                                .vectors()
                                .at("gpu-agent")
                                .values();
    EXPECT_EQ(std::vector<double>(allocated.begin(), allocated.end()), (std::vector<double>{ 1, 0, 1, 0 }));
}

/**
 * Feature: Multiple Try ExitInstance
 * Steps:
 * 1. mutliple exit
 * 2. exitFailedHandler expected called 3 times
 * 3. exitHandler expected called once
 * Expectation:
 * 1. result is right
 */
TEST_F(InstanceStateMachineTest, MultipleTryExitInstance)
{
    auto promise = std::make_shared<litebus::Promise<bool>>();
    InstanceStateMachine::SetExitHandler(
        [promise](const resources::InstanceInfo &instanceInfo) -> litebus::Future<Status> {
            promise->SetValue(true);
            return Status::OK();
        });
    auto count = std::make_shared<int>(0);
    InstanceStateMachine::SetExitFailedHandler([count](const TransitionResult &result){
        *count = *count + 1;
    });
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    instanceStateMachine->TransitionTo(TransContext{ InstanceState::CREATING, 0, "", false });
    instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, 0, "", false });
    auto instanceInfo = instanceStateMachine->GetInstanceInfo();
    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    EXPECT_CALL(*mockInstanceOpt, Modify).WillRepeatedly(Return(OperateResult{ Status(StatusCode::FAILED), "", 0 }));
    {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        auto killContext = std::make_shared<KillContext>();
        killContext->instanceContext = instanceStateMachine->GetInstanceContextCopy();
        instanceStateMachine->TryExitInstance(promise,killContext);
        ASSERT_AWAIT_READY(promise->GetFuture());
        EXPECT_EQ(promise->GetFuture().Get().IsError(), true);
        instanceStateMachine->UpdateInstanceInfo(instanceInfo);
    }
    {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        auto killContext = std::make_shared<KillContext>();
        killContext->instanceContext = instanceStateMachine->GetInstanceContextCopy();
        instanceStateMachine->TryExitInstance(promise,killContext);
        ASSERT_AWAIT_READY(promise->GetFuture());
        EXPECT_EQ(promise->GetFuture().Get().IsError(), true);
        instanceStateMachine->UpdateInstanceInfo(instanceInfo);
    }
    {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        auto killContext = std::make_shared<KillContext>();
        killContext->instanceContext = instanceStateMachine->GetInstanceContextCopy();
        instanceStateMachine->TryExitInstance(promise,killContext);
        ASSERT_AWAIT_READY(promise->GetFuture());
        EXPECT_EQ(promise->GetFuture().Get().IsError(), true);
        instanceStateMachine->UpdateInstanceInfo(instanceInfo);
    }
    {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        auto killContext = std::make_shared<KillContext>();
        killContext->instanceContext = instanceStateMachine->GetInstanceContextCopy();
        instanceStateMachine->TryExitInstance(promise,killContext);
        ASSERT_AWAIT_READY(promise->GetFuture());
        EXPECT_EQ(promise->GetFuture().Get().IsOk(), true);
    }
    ASSERT_AWAIT_READY(promise->GetFuture());
    EXPECT_EQ(*count, 3);
}

TEST_F(InstanceStateMachineTest, TransitionFailedWhenLocalAbnormal)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(3);
    scheduleReq->mutable_instance()->set_function(function);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);
    instanceStateMachine.SetLocalAbnormal();
    auto ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::EXITING, 0, "", false });
    ASSERT_AWAIT_READY(ret);
    ASSERT_TRUE(ret.Get().preState.IsNone());
    ASSERT_TRUE(ret.Get().status.IsError());
}

/**
 * Feature: PrepareTransitionInfoTest
 * Description: PrepareTransitionInfoTest
 * Steps:
 * 1. test scheduleReq is nullptr and set instanceState successfully
 * 2. test scheduleReq is not nullptr and set instanceState successfully
 *
 * Expectation:
 * 1. result is right
 */
TEST_F(InstanceStateMachineTest, PrepareTransitionInfoTest)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::NEW));
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);

    // Test when scheduleReq is not nullptr then set the state, exitCode, msg, type of scheduleReq.
    auto contextWithScheduleReq = TransContext{ InstanceState::SCHEDULING, 1, "running", true, 0, 0, 0};
    contextWithScheduleReq.scheduleReq = std::make_shared<messages::ScheduleRequest>();
    contextWithScheduleReq.scheduleReq->mutable_instance()->set_version(1);
    resources::InstanceInfo instanceInfo;
    resources::InstanceInfo previousInfo;
    instanceStateMachine.PrepareTransitionInfo(contextWithScheduleReq, instanceInfo, previousInfo);

    EXPECT_EQ(instanceInfo.instancestatus().code(), static_cast<int32_t>(contextWithScheduleReq.newState));
    EXPECT_EQ(instanceInfo.instancestatus().errcode(), contextWithScheduleReq.errCode);
    EXPECT_EQ(instanceInfo.instancestatus().exitcode(), contextWithScheduleReq.exitCode);
    EXPECT_EQ(instanceInfo.instancestatus().msg(), contextWithScheduleReq.msg);
    EXPECT_EQ(instanceInfo.instancestatus().type(), contextWithScheduleReq.type);
    EXPECT_TRUE(instanceInfo.extensions().find(CREATE_TIME_STAMP) != instanceInfo.extensions().end());

    EXPECT_EQ(previousInfo.instancestatus().code(), static_cast<int32_t>(context->GetInstanceInfo().instancestatus().code()));
    EXPECT_EQ(previousInfo.instancestatus().errcode(), context->GetInstanceInfo().instancestatus().errcode());
    EXPECT_EQ(previousInfo.instancestatus().exitcode(), context->GetInstanceInfo().instancestatus().exitcode());
    EXPECT_EQ(previousInfo.instancestatus().msg(), context->GetInstanceInfo().instancestatus().msg());
    EXPECT_EQ(previousInfo.instancestatus().type(), context->GetInstanceInfo().instancestatus().type());

    instanceStateMachine.UpdateInstanceVersion(contextWithScheduleReq, instanceInfo);

    EXPECT_EQ(contextWithScheduleReq.version+1, contextWithScheduleReq.scheduleReq->instance().version());
    EXPECT_EQ(instanceInfo.version(), contextWithScheduleReq.scheduleReq->instance().version());

    auto transContext = TransContext{ InstanceState::FATAL, 0, "fatal", true, 1007, 512, 1};
    // Test when scheduleReq is nullptr then set the state, exitCode, msg, type of instanceContext_.
    instanceStateMachine.PrepareTransitionInfo(transContext, instanceInfo, previousInfo);

    EXPECT_EQ(instanceInfo.instancestatus().code(), static_cast<int32_t>(transContext.newState));
    EXPECT_EQ(instanceInfo.instancestatus().errcode(), transContext.errCode);
    EXPECT_EQ(instanceInfo.instancestatus().exitcode(), transContext.exitCode);
    EXPECT_EQ(instanceInfo.instancestatus().msg(), transContext.msg);
    EXPECT_EQ(instanceInfo.instancestatus().type(), transContext.type);

    EXPECT_EQ(previousInfo.instancestatus().code(), 0);
    EXPECT_EQ(previousInfo.instancestatus().errcode(), 0);
    EXPECT_EQ(previousInfo.instancestatus().exitcode(), 0);
    EXPECT_EQ(previousInfo.instancestatus().msg(), "");
    EXPECT_EQ(previousInfo.instancestatus().type(), 0);

    instanceStateMachine.UpdateInstanceVersion(transContext, instanceInfo);
    EXPECT_EQ(transContext.version+1, instanceInfo.version());
    EXPECT_EQ(instanceInfo.version(), instanceStateMachine.GetVersion());

    auto tmp = instanceStateMachine.GetInstanceInfo();
}

/**
 * Feature: Concurrent Execute State Change call back
 * Steps:
 * 1. add state change callback
 * 2. async to execute
 * 3. execute
 * Expectation:
 * 1. result is right
 */
TEST_F(InstanceStateMachineTest, ConcurrentExecureStateChangeCb)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    std::string reqID = "requestId";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->set_requestid(reqID);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    litebus::Promise<resources::InstanceInfo> promise;
    std::unordered_set<InstanceState> statesConcerned = { InstanceState::RUNNING };
    instanceStateMachine->AddStateChangeCallback(
        statesConcerned, [promise](const resources::InstanceInfo &instanceInfo) { promise.SetValue(instanceInfo); },
        "key");
    auto actor = std::make_shared<ActorWorker>();
    (void)actor->AsyncWork([reqID, instanceStateMachine](){
        instanceStateMachine->ExecuteStateChangeCallback(reqID, InstanceState::RUNNING);
    });
    instanceStateMachine->ExecuteStateChangeCallback(reqID, InstanceState::RUNNING);
    ASSERT_AWAIT_READY(promise.GetFuture());
}
/**
 * when instance status is transitioned to Fatal, mark instance billing end
 */
TEST_F(InstanceStateMachineTest, TransitionStateFatalFromRunning)
{
    const std::string instanceId = "instanceID";
    std::map<std::string, std::string> createOptions = {};
    resources::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(instanceId);
    instanceInfo.set_functionagentid("");
    instanceInfo.set_issystemfunc(false);
    metrics::MetricsAdapter::GetInstance().GetMetricsContext().InitBillingInstance(instanceInfo, createOptions);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_instanceid(instanceId);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);
    instanceStateMachine.TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    instanceStateMachine.TransitionTo(TransContext{ InstanceState::CREATING, 0, "", false });
    instanceStateMachine.TransitionTo(TransContext{ InstanceState::RUNNING, 0, "", false });
    auto ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::FATAL, 0, "", false });
    ASSERT_AWAIT_READY(ret);
    auto billingInstanceMap = metrics::MetricsAdapter::GetInstance().GetMetricsContext().GetBillingInstanceMap();
    ASSERT_TRUE(billingInstanceMap.find(instanceId)->second.endTimeMillis != 0);
}

/**
 * when instance status is transitioned to Failed, mark instance billing end
 */
TEST_F(InstanceStateMachineTest, TransitionStateFailedFromRunning)
{
    const std::string instanceId = "instanceID";
    std::map<std::string, std::string> createOptions = {};
    resources::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(instanceId);
    instanceInfo.set_functionagentid("");
    instanceInfo.set_issystemfunc(false);
    metrics::MetricsAdapter::GetInstance().GetMetricsContext().InitBillingInstance(instanceInfo, createOptions);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_instanceid(instanceId);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    InstanceStateMachine instanceStateMachine(TEST_NODE_ID, context, false);
    instanceStateMachine.TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", false });
    instanceStateMachine.TransitionTo(TransContext{ InstanceState::CREATING, 0, "", false });
    instanceStateMachine.TransitionTo(TransContext{ InstanceState::RUNNING, 0, "", false });
    auto ret = instanceStateMachine.TransitionTo(TransContext{ InstanceState::FAILED, 0, "", false });
    ASSERT_AWAIT_READY(ret);
    auto billingInstanceMap = metrics::MetricsAdapter::GetInstance().GetMetricsContext().GetBillingInstanceMap();
    ASSERT_TRUE(billingInstanceMap.find(instanceId)->second.endTimeMillis != 0);
}

/**
 * Feature: ForceDelInstance
 * Description: ForceDelInstance
 * Steps:
 * 1. Force Delete instance
 *
 * Expectation:
 * 1. Status is Ok
 */
TEST_F(InstanceStateMachineTest, ForceDelInstance)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(5);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->set_functionagentid("funcAgent");
    scheduleReq->mutable_instance()->set_instanceid("instanceA");
    scheduleReq->mutable_instance()->set_functionproxyid("test node id");
    scheduleReq->mutable_instance()->set_requestid("req");

    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);

    auto mockInstanceOpt = std::make_shared<MockInstanceOperator>();
    instanceStateMachine->instanceOpt_ = mockInstanceOpt;
    EXPECT_CALL(*mockInstanceOpt, ForceDelete).WillOnce(Return(OperateResult{ Status::OK(), "", 1 }));

    auto res = instanceStateMachine->ForceDelInstance();
    EXPECT_TRUE(res.Get().IsOk());

    EXPECT_CALL(*mockInstanceOpt, ForceDelete).WillOnce(Return(OperateResult{ Status(StatusCode::FAILED), "", 0 }));

    res = instanceStateMachine->ForceDelInstance();
    EXPECT_TRUE(res.Get().IsError());
    EXPECT_EQ(instanceStateMachine->lastSaveFailedState_.first, static_cast<int32_t>(InstanceState::EXITED));
}

TEST_F(InstanceStateMachineTest, TransitionStateFailedAfterForceDelInstance)
{
    const std::string function = "default/0-test-helloWorld/$latest";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->set_functionagentid("funcAgent");
    scheduleReq->mutable_instance()->set_instanceid("instanceA");
    scheduleReq->mutable_instance()->set_functionproxyid("test node id");
    scheduleReq->mutable_instance()->set_requestid("req");
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    instanceStateMachine->BindMetaStoreClient(
        MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ }));
    auto ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::SCHEDULING, 0, "", true });
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().status.IsOk());
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::CREATING, instanceStateMachine->GetVersion(), "", true });
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().status.IsOk());
    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::RUNNING, instanceStateMachine->GetVersion(), "", true });
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().status.IsOk());
    EXPECT_TRUE(instanceStateMachine->GetInstanceState() == InstanceState::RUNNING);

    auto res = instanceStateMachine->ForceDelInstance();
    ASSERT_AWAIT_READY(res);
    EXPECT_TRUE(res.Get().IsOk());

    ret = instanceStateMachine->TransitionTo(TransContext{ InstanceState::EXITING, instanceStateMachine->GetVersion(), "", true });
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().status.IsError());
}

TEST_F(InstanceStateMachineTest, TestGetInstanceContextCopy)
{
    const std::string instanceId = "instanceID";
	auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
	scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(0);
	scheduleReq->mutable_instance()->set_instanceid(instanceId);
    scheduleReq->mutable_instance()->set_functionproxyid("functionproxyid1");
	auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    std::shared_ptr<InstanceContext> instanceContext = instanceStateMachine->GetInstanceContextCopy();
    scheduleReq->mutable_instance()->set_functionproxyid("functionproxyid2");
    ASSERT_TRUE(instanceContext->GetInstanceInfo().functionproxyid() == "functionproxyid1");
}

TEST_F(InstanceStateMachineTest, TestTagStop)
{
    const std::string instanceId = "instanceID";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    auto stop = instanceStateMachine->IsStopped();
    EXPECT_EQ(stop, false);
    instanceStateMachine->TagStop();
    stop = instanceStateMachine->IsStopped();
    EXPECT_EQ(stop, true);
}

TEST_F(InstanceStateMachineTest, TestTagSaved)
{
    const std::string instanceId = "instanceID";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto instanceStateMachine = std::make_shared<InstanceStateMachine>(TEST_NODE_ID, context, false);
    instanceStateMachine->NewSavingPomise();
    EXPECT_EQ(instanceStateMachine->IsSaving(), true);
    auto saving = instanceStateMachine->GetSavingFuture();
    EXPECT_EQ(saving.IsInit(), true);
    instanceStateMachine->TagSaved();
    EXPECT_EQ(saving.IsInit(), false);
    EXPECT_EQ(instanceStateMachine->IsSaving(), false);
}
}  // namespace functionsystem::test
