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

#include "function_proxy/busproxy/instance_view/instance_view.h"

#include <gtest/gtest.h>

#include <nlohmann/json.hpp>

#include "common/proto/pb/posix/resource.pb.h"
#include "common/resource_view/resource_type.h"
#include "common/types/instance_state.h"
#include "function_proxy/common/parked_instance_registry/parked_instance_registry.h"
#include "mocks/mock_shared_client.h"
#include "mocks/mock_shared_client_manager_proxy.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {
using namespace busproxy;
using namespace ::testing;
const std::string nodeID = "local";

inline resource_view::InstanceInfo GenInstanceInfo(const std::string &instanceID, const std::string &parent,
                                                   const std::string &node, InstanceState instanceStatus,
                                                   int32_t scheduleRound = 0)
{
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(instanceID);
    instanceInfo.set_parentid(parent);
    instanceInfo.set_functionproxyid(node);
    instanceInfo.mutable_instancestatus()->set_code(int32_t(instanceStatus));
    instanceInfo.set_scheduletimes(3 - scheduleRound);
    return instanceInfo;
}

class InstanceViewTest : public ::testing::Test {
public:
    void SetUp() override
    {
        instanceView_ = std::make_shared<InstanceView>(nodeID);
        proxyView_ = std::make_shared<ProxyView>();
        mockSharedClientManagerProxy_ = std::make_shared<MockSharedClientManagerProxy>();
        instanceView_->BindProxyView(proxyView_);
        instanceView_->BindDataInterfaceClientManager(mockSharedClientManagerProxy_);
        auto address = litebus::GetLitebusAddress();
        url_ = address.ip + ":" + std::to_string(address.port);
    }

    void TearDown() override
    {
        instanceView_->BindDataInterfaceClientManager(nullptr);
        instanceView_ = nullptr;
        mockSharedClientManagerProxy_ = nullptr;
    }

    void UpdateInstance(const std::string &instanceID, const std::string &parent, const std::string &receiveNode,
                        const std::string &locationNode, int32_t scheduleRound = 0)
    {
        auto instanceInfo = GenInstanceInfo(instanceID, parent, receiveNode, InstanceState::SCHEDULING, scheduleRound);
        instanceInfo.set_version(0);
        instanceView_->Update(instanceID, instanceInfo, false);

        instanceInfo = GenInstanceInfo(instanceID, parent, locationNode, InstanceState::CREATING, scheduleRound);
        instanceInfo.set_version(1);
        instanceView_->Update(instanceID, instanceInfo, false);
        // lower version duplicate update
        instanceInfo.set_version(0);
        instanceView_->Update(instanceID, instanceInfo, false);

        instanceInfo = GenInstanceInfo(instanceID, parent, locationNode, InstanceState::RUNNING, scheduleRound);
        instanceInfo.set_version(3);
        auto mockSharedClient = std::make_shared<MockSharedClient>();
        EXPECT_CALL(*mockSharedClientManagerProxy_, NewDataInterfacePosixClient(_, _, _))
            .WillRepeatedly(Return(mockSharedClient));
        instanceView_->Update(instanceID, instanceInfo, false);
        if (locationNode == nodeID) {
            litebus::AID aid(instanceID, url_);
            EXPECT_NE(litebus::GetActor(aid), nullptr);
        }
    }

protected:
    std::shared_ptr<InstanceView> instanceView_;
    std::shared_ptr<ProxyView> proxyView_;
    std::shared_ptr<MockSharedClientManagerProxy> mockSharedClientManagerProxy_;
    std::string url_;
};

class MockDataPlaneObserver : public function_proxy::DataPlaneObserver {
public:
    MockDataPlaneObserver(const std::shared_ptr<InstanceView> &view)
        : function_proxy::DataPlaneObserver(nullptr), instanceView_(view){};
    ~MockDataPlaneObserver() = default;
    litebus::Future<Status> SubscribeInstanceEvent(const std::string &subscriber, const std::string &targetInstance,
                                                   bool ignoreNonExist = false)
    {
        return instanceView_->SubscribeInstanceEvent(subscriber, targetInstance, ignoreNonExist);
    }
    void NotifyMigratingRequest(const std::string &instanceID)
    {
        instanceView_->NotifyMigratingRequest(instanceID);
    }

private:
    std::shared_ptr<InstanceView> instanceView_;
};

TEST_F(InstanceViewTest, InstanceStateChange)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto localClient = std::make_shared<proxy::Client>(litebus::AID());
    proxyView_->Update(nodeID, localClient);
    std::string parent = "parent";
    UpdateInstance(parent, "driver", nodeID, nodeID);
    std::string childA = "childA";
    UpdateInstance(childA, parent, nodeID, nodeID);

    // not in local
    std::string childB = "childB";
    UpdateInstance(childB, parent, nodeID, "remote1");
    auto client = std::make_shared<proxy::Client>(litebus::AID());
    proxyView_->Update("remote1", client);

    // instance located in another node would not spawn proxy actor
    litebus::AID aid(childB, url_);
    EXPECT_EQ(litebus::GetActor(aid), nullptr);

    auto instanceInfo = GenInstanceInfo(childB, parent, "remote1", InstanceState::FATAL);
    instanceView_->Update(childB, instanceInfo, false);
    instanceView_->SubscribeInstanceEvent(childA, childB, true);

    instanceInfo = GenInstanceInfo(childA, parent, nodeID, InstanceState::FAILED);
    instanceView_->Update(childA, instanceInfo, false);
    instanceView_->SubscribeInstanceEvent(childA, childB);

    // invalid subscribe
    std::string invalidSubscriber = "invalidSubscriber";
    auto ret = instanceView_->SubscribeInstanceEvent(invalidSubscriber, childB);
    EXPECT_EQ(ret.IsOk(), false);

    // migrating to another node
    auto clientB = std::make_shared<proxy::Client>(litebus::AID());
    proxyView_->Update("remote2", clientB);
    UpdateInstance(childA, parent, nodeID, "remote2", 1);
    aid.SetName(childA);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
    instanceView_->Delete(childA, -1);
    instanceView_->Delete(childB, -1);
    instanceView_->Delete(parent, -1);
    aid.SetName(parent);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
    aid.SetName(childA);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
    aid.SetName(childB);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
}

/**
 * Feature: Subscribe to instance in EVICTING state
 * Description: Verify subscribing to an instance that is in EVICTING state
 * Steps:
 * 1. Create parent instance
 * 2. Create child instance in EVICTING state
 * 3. Subscribe parent to child
 * Expectation: Subscriber receives reject notification
 */
TEST_F(InstanceViewTest, SubscribeToEvictingInstance)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto localClient = std::make_shared<proxy::Client>(litebus::AID());
    proxyView_->Update(nodeID, localClient);

    std::string parent = "parent-evicting";
    std::string child = "child-evicting";
    
    // Create parent instance
    UpdateInstance(parent, "driver", nodeID, nodeID);
    
    // Create child in EVICTING state
    auto instanceInfo = GenInstanceInfo(child, parent, nodeID, InstanceState::EVICTING);
    instanceInfo.mutable_instancestatus()->set_errcode(common::ERR_INSTANCE_EVICTED);
    instanceInfo.mutable_instancestatus()->set_msg("Instance is being evicted");
    instanceView_->Update(child, instanceInfo, false);

    // Subscribe parent to evicting child
    auto ret = instanceView_->SubscribeInstanceEvent(parent, child, false);
    EXPECT_TRUE(ret.IsOk());

    // Cleanup
    instanceView_->Delete(child, -1);
    instanceView_->Delete(parent, -1);
}

class TimerContextActor : public litebus::ActorBase {
public:
    explicit TimerContextActor(const std::string &name) : litebus::ActorBase(name) {}

protected:
    void Init() override {}
};

class ParkedInstanceViewTest : public InstanceViewTest {
public:
    void SetUp() override
    {
        InstanceViewTest::SetUp();
        timerActor_ = std::make_shared<TimerContextActor>("parked-timer-ctx");
        litebus::Spawn(timerActor_);
        instanceView_->BindTimerContext(timerActor_->GetAID());
        auto &registry = function_proxy::ParkedInstanceRegistry::Instance();
        registry.Configure(300, true);
        registry.ConfigureDrain(10000, true, false);
        parkedInstanceID_ = "inst-parked";
        registry.Clear(parkedInstanceID_);
    }

    void TearDown() override
    {
        // BindTimerContext (in SetUp) registered drain providers bound to this
        // InstanceView: drop them before the view goes away
        auto &registry = function_proxy::ParkedInstanceRegistry::Instance();
        registry.SetDrainProvider(nullptr);
        registry.SetReleaseDrainProvider(nullptr);
        registry.Clear(parkedInstanceID_);
        litebus::Terminate(timerActor_->GetAID());
        litebus::Await(timerActor_);
        timerActor_ = nullptr;
        InstanceViewTest::TearDown();
    }

protected:
    std::shared_ptr<TimerContextActor> timerActor_;
    std::string parkedInstanceID_;
};

TEST_F(ParkedInstanceViewTest, ParkedDeleteKeepsRoutingActor)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto &registry = function_proxy::ParkedInstanceRegistry::Instance();

    UpdateInstance(parkedInstanceID_, "driver", nodeID, nodeID);
    litebus::AID aid(parkedInstanceID_, url_);
    ASSERT_NE(litebus::GetActor(aid), nullptr);

    // park: mark + delete keeps the routing actor alive
    registry.MarkParked(parkedInstanceID_);
    instanceView_->Delete(parkedInstanceID_, -1);
    EXPECT_NE(litebus::GetActor(aid), nullptr);
    EXPECT_TRUE(registry.IsParked(parkedInstanceID_));

    // restore: RUNNING event clears the park bookkeeping, actor stays alive
    auto instanceInfo = GenInstanceInfo(parkedInstanceID_, "driver", nodeID, InstanceState::RUNNING);
    instanceInfo.set_version(4);
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewDataInterfacePosixClient(_, _, _))
        .WillRepeatedly(Return(mockSharedClient));
    instanceView_->Update(parkedInstanceID_, instanceInfo, false);
    EXPECT_NE(litebus::GetActor(aid), nullptr);
    EXPECT_FALSE(registry.IsParked(parkedInstanceID_));

    instanceView_->Delete(parkedInstanceID_, -1);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
}

TEST_F(ParkedInstanceViewTest, ParkedFatalHoldsDispatcherInsteadOfFailing)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto &registry = function_proxy::ParkedInstanceRegistry::Instance();

    UpdateInstance(parkedInstanceID_, "driver", nodeID, nodeID);
    litebus::AID aid(parkedInstanceID_, url_);
    ASSERT_NE(litebus::GetActor(aid), nullptr);

    // park mark first (as of snapshot entry), then the FATAL event the deliberate
    // sandbox kill produces: must hold, not fail held invokes / drop the actor.
    registry.MarkParked(parkedInstanceID_);
    auto fatalInfo = GenInstanceInfo(parkedInstanceID_, "driver", nodeID, InstanceState::FATAL);
    fatalInfo.mutable_instancestatus()->set_errcode(common::ERR_INSTANCE_EXITED);
    fatalInfo.set_version(4);
    instanceView_->Update(parkedInstanceID_, fatalInfo, false);
    EXPECT_NE(litebus::GetActor(aid), nullptr);
    EXPECT_TRUE(registry.IsParked(parkedInstanceID_));

    // restore RUNNING clears the hold bookkeeping, actor stays alive
    auto runningInfo = GenInstanceInfo(parkedInstanceID_, "driver", nodeID, InstanceState::RUNNING);
    runningInfo.set_version(5);
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewDataInterfacePosixClient(_, _, _))
        .WillRepeatedly(Return(mockSharedClient));
    instanceView_->Update(parkedInstanceID_, runningInfo, false);
    EXPECT_NE(litebus::GetActor(aid), nullptr);
    EXPECT_FALSE(registry.IsParked(parkedInstanceID_));

    instanceView_->Delete(parkedInstanceID_, -1);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
}

TEST_F(ParkedInstanceViewTest, RealDeleteWithoutParkMarkTerminatesActor)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto &registry = function_proxy::ParkedInstanceRegistry::Instance();

    UpdateInstance(parkedInstanceID_, "driver", nodeID, nodeID);
    litebus::AID aid(parkedInstanceID_, url_);
    ASSERT_NE(litebus::GetActor(aid), nullptr);

    // no park mark: legacy delete path, actor terminated
    ASSERT_FALSE(registry.IsParked(parkedInstanceID_));
    instanceView_->Delete(parkedInstanceID_, -1);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
}

TEST_F(ParkedInstanceViewTest, DisabledDeferBehavesLikeLegacyDelete)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto &registry = function_proxy::ParkedInstanceRegistry::Instance();

    UpdateInstance(parkedInstanceID_, "driver", nodeID, nodeID);
    litebus::AID aid(parkedInstanceID_, url_);
    ASSERT_NE(litebus::GetActor(aid), nullptr);

    registry.Configure(300, false);
    registry.MarkParked(parkedInstanceID_);  // no-op while disabled
    instanceView_->Delete(parkedInstanceID_, -1);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
    registry.Configure(300, true);
}

TEST_F(ParkedInstanceViewTest, HoldTtlExpiryTearsActorDown)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto &registry = function_proxy::ParkedInstanceRegistry::Instance();
    registry.Configure(1, true);  // 1s hold

    UpdateInstance(parkedInstanceID_, "driver", nodeID, nodeID);
    litebus::AID aid(parkedInstanceID_, url_);
    ASSERT_NE(litebus::GetActor(aid), nullptr);

    registry.MarkParked(parkedInstanceID_);
    instanceView_->Delete(parkedInstanceID_, -1);
    EXPECT_NE(litebus::GetActor(aid), nullptr);

    // expiry timer re-enters through the timer-context actor mailbox
    ASSERT_AWAIT_TRUE_FOR([=]() -> bool { return litebus::GetActor(aid) == nullptr; }, 5U);
    EXPECT_FALSE(registry.IsParked(parkedInstanceID_));
    registry.Configure(300, true);
}

TEST_F(ParkedInstanceViewTest, DrainIdleInstanceSucceedsAndReleaseRestoresReady)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto &registry = function_proxy::ParkedInstanceRegistry::Instance();
    registry.ConfigureDrain(2000, true, false);

    UpdateInstance(parkedInstanceID_, "driver", nodeID, nodeID);
    litebus::AID aid(parkedInstanceID_, url_);
    ASSERT_NE(litebus::GetActor(aid), nullptr);
    auto actor = std::dynamic_pointer_cast<InstanceProxy>(litebus::GetActor(aid));
    ASSERT_TRUE(actor != nullptr);
    ASSERT_AWAIT_TRUE([actor]() -> bool { return actor->selfDispatcher_->isReady_; });

    // idle instance: no in-flight invokes, the drain resolves OK on the first poll
    auto drainFut = registry.DrainInstance(parkedInstanceID_);
    ASSERT_AWAIT_READY(drainFut);
    EXPECT_TRUE(drainFut.Get().IsOk());
    ASSERT_AWAIT_TRUE([actor]() -> bool { return !actor->selfDispatcher_->isReady_; });

    // park abandoned after the drain: rollback flips the dispatcher back to ready
    registry.ReleaseDrain(parkedInstanceID_);
    ASSERT_AWAIT_TRUE([actor]() -> bool { return actor->selfDispatcher_->isReady_; });

    instanceView_->Delete(parkedInstanceID_, -1);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
}

TEST_F(ParkedInstanceViewTest, DrainTimeoutAbandonsParkAndRollsBackDispatcher)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto &registry = function_proxy::ParkedInstanceRegistry::Instance();
    registry.ConfigureDrain(300, true, false);  // short drain window for the test

    UpdateInstance(parkedInstanceID_, "driver", nodeID, nodeID);
    litebus::AID aid(parkedInstanceID_, url_);
    ASSERT_NE(litebus::GetActor(aid), nullptr);
    auto actor = std::dynamic_pointer_cast<InstanceProxy>(litebus::GetActor(aid));
    ASSERT_TRUE(actor != nullptr);
    ASSERT_AWAIT_TRUE([actor]() -> bool { return actor->selfDispatcher_->isReady_; });

    // fabricate a stuck in-flight invoke: a call context already delivered to the
    // runtime (OnResp) whose response never arrives within the drain window
    auto ctx = std::make_shared<CallRequestContext>();
    ctx->from = "caller";
    ctx->requestID = "stuck-inflight";
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    auto callReq = request->mutable_callreq();
    callReq->set_requestid(ctx->requestID);
    callReq->set_senderid(ctx->from);
    ctx->callRequest = request;
    actor->selfDispatcher_->callCache_->Push(ctx);
    actor->selfDispatcher_->callCache_->MoveToOnResp(ctx->requestID);
    EXPECT_EQ(actor->selfDispatcher_->callCache_->InFlightCount(), 1U);

    auto drainFut = registry.DrainInstance(parkedInstanceID_);
    ASSERT_AWAIT_READY(drainFut);
    EXPECT_TRUE(drainFut.Get().IsError());
    EXPECT_EQ(drainFut.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);

    // rollback: dispatcher ready again, the stuck context survives (not failed)
    ASSERT_AWAIT_TRUE([actor]() -> bool { return actor->selfDispatcher_->isReady_; });
    EXPECT_NE(actor->selfDispatcher_->callCache_->FindCallRequestContext("stuck-inflight"), nullptr);

    // drop the fabricated context before teardown: a non-empty cache makes the final
    // delete run SendNotify through InvocationHandler, whose instance proxy wrapper is
    // not bound in this test binary (ASSERT_IF_NULL raises SIGINT there) — unrelated
    // to the drain semantics under test
    actor->selfDispatcher_->callCache_->DeleteReqOnResp("stuck-inflight");

    instanceView_->Delete(parkedInstanceID_, -1);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
}

TEST_F(ParkedInstanceViewTest, DrainDisabledLeavesDispatcherReady)
{
    InstanceProxy::BindObserver(std::make_shared<MockDataPlaneObserver>(instanceView_));
    auto &registry = function_proxy::ParkedInstanceRegistry::Instance();
    registry.ConfigureDrain(2000, false, false);

    UpdateInstance(parkedInstanceID_, "driver", nodeID, nodeID);
    litebus::AID aid(parkedInstanceID_, url_);
    ASSERT_NE(litebus::GetActor(aid), nullptr);
    auto actor = std::dynamic_pointer_cast<InstanceProxy>(litebus::GetActor(aid));
    ASSERT_TRUE(actor != nullptr);
    ASSERT_AWAIT_TRUE([actor]() -> bool { return actor->selfDispatcher_->isReady_; });

    auto drainFut = registry.DrainInstance(parkedInstanceID_);
    ASSERT_AWAIT_READY(drainFut);
    EXPECT_TRUE(drainFut.Get().IsOk());
    // drain skipped entirely: the dispatcher was never flipped
    EXPECT_TRUE(actor->selfDispatcher_->isReady_);

    instanceView_->Delete(parkedInstanceID_, -1);
    ASSERT_AWAIT_TRUE([=]() -> bool { return litebus::GetActor(aid) == nullptr; });
}
}  // namespace functionsystem::test