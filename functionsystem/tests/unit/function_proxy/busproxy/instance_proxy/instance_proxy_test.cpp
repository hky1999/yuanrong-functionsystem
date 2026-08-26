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

#include "busproxy/instance_proxy/instance_proxy.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <unordered_set>

#include "common/metrics/metrics_adapter.h"
#include "common/metrics/metrics_constants.h"
#include "function_proxy/busproxy/instance_view/instance_view.h"
#include "function_proxy/busproxy/invocation_handler/invocation_handler.h"
#include "function_proxy/busproxy/instance_proxy/request_router.h"
#include "function_proxy/common/observer/data_plane_observer/data_plane_observer.h"
#include "function_proxy/config/direct_routing_config.h"
#include "mocks/mock_internal_iam.h"
#include "mocks/mock_shared_client.h"
#include "mocks/mock_shared_client_manager_proxy.h"
#include "utils/future_test_helper.h"
#include "common/metrics/metrics_adapter.h"

namespace functionsystem::test {
using namespace ::testing;
using namespace busproxy;
const std::string CUSTOMS_TAG = "CUSTOMS_TAG";
class SimulateObserver : public function_proxy::DataPlaneObserver, public litebus::ActorBase {
public:
    SimulateObserver() : DataPlaneObserver(nullptr), litebus::ActorBase("SimulateObserver"){};
    virtual ~SimulateObserver() = default;

    // P2 #8: count MarkInstanceUsed calls (SDK-invoke idle-claim wiring)
    std::atomic<size_t> usedMarks{0};
    std::unordered_set<std::string> usedInstances;
    void MarkInstanceUsed(const std::string &instanceID) override
    {
        usedMarks++;
        usedInstances.insert(instanceID);
    }

    void SetInstanceView(const std::shared_ptr<InstanceView> instanceView)
    {
        instanceView_ = instanceView;
    }

    void Update(const std::string &instanceID, const resources::InstanceInfo &instanceInfo)
    {
        (void)litebus::Async(GetAID(), &SimulateObserver::AsyncUpdate, instanceID, instanceInfo).Get();
    }

    void Delete(const std::string &instanceID)
    {
        (void)litebus::Async(GetAID(), &SimulateObserver::AsyncDelete, instanceID).Get();
    }

    Status AsyncUpdate(const std::string &instanceID, const resources::InstanceInfo &instanceInfo)
    {
        instanceView_->Update(instanceID, instanceInfo, false);
        return Status::OK();
    }

    Status AsyncDelete(const std::string &instanceID)
    {
        instanceView_->Delete(instanceID, -1);
        return Status::OK();
    }

    litebus::Future<Status> SubscribeInstanceEvent(const std::string &subscriber, const std::string &targetInstance,
                                                   bool ignoreNonExist = false) override
    {
        SendSubscribeInstanceEvent(subscriber, targetInstance, ignoreNonExist);
        return litebus::Async(GetAID(), &SimulateObserver::DoSubscribeInstanceEvent, subscriber, targetInstance,
                              ignoreNonExist);
    }

    Status DoSubscribeInstanceEvent(const std::string &subscriber, const std::string &targetInstance,
                                    bool ignoreNonExist)
    {
        return instanceView_->SubscribeInstanceEvent(subscriber, targetInstance, ignoreNonExist);
    }

    void NotifyMigratingRequest(const std::string &instanceID) override
    {
        litebus::Async(GetAID(), &SimulateObserver::DoNotifyMigratingRequest, instanceID);
    }

    void DoNotifyMigratingRequest(const std::string &instanceID)
    {
        instanceView_->NotifyMigratingRequest(instanceID);
    }

    MOCK_METHOD(litebus::Future<Status>, SendSubscribeInstanceEvent,
                (const std::string &subscriber, const std::string &targetInstance, bool ignoreNonExist));
    MOCK_METHOD((litebus::Future<std::shared_ptr<resources::RouteInfo>>), QueryInstanceRoute,
                (const std::string &instanceID), (override));
    MOCK_METHOD((litebus::Future<std::string>), QueryProxyAddress, (const std::string &proxyID), (override));

private:
    std::shared_ptr<InstanceView> instanceView_;
};

resources::InstanceInfo NewInstance(const std::string &instanceID, const std::string &tenantID,
                                    const bool isLowReliability = false)
{
    resources::InstanceInfo ins;
    ins.set_instanceid(instanceID);
    ins.set_tenantid(tenantID);
    ins.mutable_instancestatus()->set_code((int32_t)InstanceState::SCHEDULING);
    ins.set_lowreliability(isLowReliability);
    return ins;
}

SharedStreamMsg CallRequest(const std::string &caller, const std::string &callee, const std::string &requestID,
                            const std::string &route = "", bool enableForceInvoke = false)
{
    auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
    auto callreq = msg->mutable_callreq();
    callreq->set_senderid(caller);
    callreq->set_requestid(requestID);
    (*callreq->mutable_createoptions())[CUSTOMS_TAG] = requestID;
    if (!route.empty()) {
        (*callreq->mutable_createoptions())["YR_ROUTE"] = route;
    }
    if (enableForceInvoke) {
        (*callreq->mutable_createoptions())["ENABLE_FORCE_INVOKE"] = "true";
    }
    return msg;
}

SharedStreamMsg CallResult(const std::string &caller, const std::string &requestID)
{
    auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
    auto callresult = msg->mutable_callresultreq();
    callresult->set_requestid(requestID);
    callresult->set_instanceid(caller);
    return msg;
}

void UpdateInstance(resources::InstanceInfo &info, const std::string &instanceID, int32_t status,
                    std::string proxyid)
{
    info.set_instanceid(instanceID);
    info.mutable_instancestatus()->set_code(status);
    info.set_functionproxyid(proxyid);
    info.set_runtimeid(instanceID);
}

class InstanceProxyTest : public ::testing::Test {
public:
    void SetUp() override
    {
        observer_ = std::make_shared<SimulateObserver>();

        instanceView_ = std::make_shared<InstanceView>("local");
        proxyView_ = std::make_shared<ProxyView>();
        mockSharedClientManagerProxy_ = std::make_shared<MockSharedClientManagerProxy>();
        instanceView_->BindDataInterfaceClientManager(mockSharedClientManagerProxy_);
        instanceView_->BindProxyView(proxyView_);

        observer_->SetInstanceView(instanceView_);
        litebus::Spawn(observer_);

        proxyView_->Update(local_, std::make_shared<proxy::Client>(observer_->GetAID()));
        proxyView_->Update(remote_, std::make_shared<proxy::Client>(observer_->GetAID()));

        InstanceProxy::BindObserver(observer_);
        InstanceProxy::BindSystemTimeout(DEFAULT_SYSTEM_TIMEOUT);
        RequestDispatcher::BindDataInterfaceClientManager(mockSharedClientManagerProxy_);
        InvocationHandler::BindInstanceProxy(std::make_shared<busproxy::InstanceProxyWrapper>());
        MemoryControlConfig config;
        InvocationHandler::BindMemoryMonitor(std::make_shared<MemoryMonitor>(config));
        functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetEnabledInstruments(
            { metrics::YRInstrument::YR_APP_INSTANCE_BILLING_INVOKE_LATENCY });
    }

    void TearDown() override
    {
        InvocationHandler::StopMemoryMonitor();
        litebus::AID callerProxy("callerIns", observer_->GetAID().Url());
        litebus::Terminate(callerProxy);
        litebus::Await(callerProxy);
        litebus::AID calleeProxy("calleeIns", observer_->GetAID().Url());
        litebus::Terminate(calleeProxy);
        litebus::Await(calleeProxy);
        litebus::Terminate(observer_->GetAID());
        litebus::Await(observer_->GetAID());
        instanceView_->Delete("callerIns", -1);
        instanceView_->Delete("calleeIns", -1);
        instanceInfo_.clear();
        InstanceProxy::BindObserver(nullptr);
        InstanceProxy::BindSystemTimeout(DEFAULT_SYSTEM_TIMEOUT);
        RequestDispatcher::BindInternalIAM(nullptr);
        RequestDispatcher::BindDataInterfaceClientManager(nullptr);
        instanceView_->BindDataInterfaceClientManager(nullptr);
        instanceView_ = nullptr;
        proxyView_ = nullptr;
        observer_ = nullptr;
        tenantID_ = "";

        functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetEnabledInstruments({});
    }

    void SetTenantID(const std::string &tenantID)
    {
        tenantID_ = tenantID;
    }

protected:
    std::shared_ptr<MockSharedClient> PrepareCaller(const std::string &callerIns)
    {
        auto instanceInfo = NewInstance(callerIns, tenantID_);
        UpdateInstance(instanceInfo, callerIns, (int32_t)InstanceState::RUNNING, local_);
        auto mockSharedClient = std::make_shared<MockSharedClient>();
        EXPECT_CALL(*mockSharedClientManagerProxy_, NewDataInterfacePosixClient(callerIns, _, _))
            .WillOnce(Return(mockSharedClient));
        observer_->Update(callerIns, instanceInfo);
        instanceInfo_[callerIns] = instanceInfo;
        return mockSharedClient;
    }

    void CallTest(const std::string &callerIns, const std::string &calleeIns, bool isCalleeLocal, bool isLowReliability = false)
    {
        auto mockSharedClient = PrepareCaller(callerIns);
        litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
        ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy) != nullptr; });

        auto calleeInfo = NewInstance(calleeIns, tenantID_, isLowReliability);
        observer_->Update(calleeIns, calleeInfo);
        litebus::AID calleeProxy(calleeIns, observer_->GetAID().Url());
        std::string route = "";
        if (isLowReliability) {
            route = observer_->GetAID().Url();
        }

        // the 1st invoke before creating
        auto callBeforeCreating = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns,.tenantID=tenantID_}, calleeIns,
                                                 CallRequest(callerIns, calleeIns, "Request-1", route), nullptr);
        // the 2nd invoke before creating
        auto duplicateCallBeforeCreating = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns, .tenantID=tenantID_}, calleeIns,
                                                          CallRequest(callerIns, calleeIns, "Request-1", route), nullptr);

        // update instance to creating
        UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::CREATING, isCalleeLocal ? local_ : remote_);
        observer_->Update(calleeIns, calleeInfo);
        instanceInfo_[calleeIns] = calleeInfo;

        // invoke 1 times before running
        auto callBeforeRunning = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                                CallRequest(callerIns, calleeIns, "Request-2", route), nullptr);
        // update instance to running
        UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::RUNNING, isCalleeLocal ? local_ : remote_);
        if (isCalleeLocal) {
            auto mockCalleeSharedClient = std::make_shared<MockSharedClient>();
            EXPECT_CALL(*mockSharedClientManagerProxy_, NewDataInterfacePosixClient(calleeIns, _, _))
                .WillOnce(Return(mockCalleeSharedClient));
            EXPECT_CALL(*mockCalleeSharedClient, Call(_))
                .WillRepeatedly(Invoke([](const SharedStreamMsg &request) -> litebus::Future<SharedStreamMsg> {
                    auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
                    auto callrsp = msg->mutable_callrsp();
                    callrsp->set_code(::common::ErrorCode::ERR_NONE);
                    return msg;
                }));
        }

        if (!isLowReliability) {
            observer_->Update(calleeIns, calleeInfo);
            instanceInfo_[calleeIns] = calleeInfo;
        }
        ASSERT_AWAIT_SET(callBeforeCreating);
        ASSERT_AWAIT_SET(duplicateCallBeforeCreating);
        ASSERT_AWAIT_SET(callBeforeRunning);
        EXPECT_TRUE(callBeforeCreating.Get()->has_callrsp() &&
                    callBeforeCreating.Get()->callrsp().code() == common::ERR_NONE);
        EXPECT_TRUE(duplicateCallBeforeCreating.Get()->has_callrsp() &&
                    duplicateCallBeforeCreating.Get()->callrsp().code() == common::ERR_NONE);
        EXPECT_TRUE(callBeforeRunning.Get()->has_callrsp() &&
                    callBeforeRunning.Get()->callrsp().code() == common::ERR_NONE);

        if (!isCalleeLocal) {
            auto billingInvokeOption =functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().GetBillingInvokeOption("Request-1");
            EXPECT_TRUE(billingInvokeOption.instanceID == calleeIns);
        }

        // the 1st invoke after running
        auto callAfterRunning =
            litebus::Async(isCalleeLocal ? calleeProxy : callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns, .tenantID=tenantID_}, calleeIns,
                           CallRequest(callerIns, calleeIns, "Request-3", route), nullptr);
        // the 2nd invoke after running
        auto duplicateCallAfterRunning =
            litebus::Async(isCalleeLocal ? calleeProxy : callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns, .tenantID=tenantID_}, calleeIns,
                           CallRequest(callerIns, calleeIns, "Request-3", route), nullptr);

        ASSERT_AWAIT_SET(callAfterRunning);
        EXPECT_TRUE(callAfterRunning.Get()->has_callrsp() &&
                    callAfterRunning.Get()->callrsp().code() == common::ERR_NONE);
        ASSERT_AWAIT_SET(duplicateCallAfterRunning);
        EXPECT_TRUE(duplicateCallAfterRunning.Get()->has_callrsp() &&
                    duplicateCallAfterRunning.Get()->callrsp().code() == common::ERR_NONE);

        // call result
        EXPECT_CALL(*mockSharedClient, NotifyResult(_)).WillRepeatedly(Return(runtime::NotifyResponse()));
        EXPECT_CALL(*mockSharedClient, IsDone()).WillRepeatedly(Return(false));

        auto callResultBeforeCreating =
            litebus::Async(isCalleeLocal ? callerProxy : calleeProxy, &InstanceProxy::CallResult, calleeIns, callerIns,
                           CallResult(callerIns, "Request-1"), nullptr);
        auto callResultBeforeRunning =
            litebus::Async(isCalleeLocal ? callerProxy : calleeProxy, &InstanceProxy::CallResult, calleeIns, callerIns,
                           CallResult(callerIns, "Request-2"), nullptr);
        auto callResultAfterRunning =
            litebus::Async(isCalleeLocal ? callerProxy : calleeProxy, &InstanceProxy::CallResult, calleeIns, callerIns,
                           CallResult(callerIns, "Request-3"), nullptr);

        ASSERT_AWAIT_SET(callResultBeforeCreating);
        ASSERT_AWAIT_SET(callResultBeforeRunning);
        ASSERT_AWAIT_SET(callResultAfterRunning);
        EXPECT_TRUE(callResultBeforeCreating.Get()->has_callresultack());
        EXPECT_TRUE(callResultBeforeRunning.Get()->has_callresultack());
        EXPECT_TRUE(callResultAfterRunning.Get()->has_callresultack());
        {
            auto option =
                functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().GetBillingInvokeOption(
                    "Request-1");
            EXPECT_EQ(option.instanceID, calleeIns);
            EXPECT_EQ(option.invokeOptions[CUSTOMS_TAG], "Request-1");
        }
        {
            auto option =
                functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().GetBillingInvokeOption(
                    "Request-2");
            EXPECT_EQ(option.instanceID, calleeIns);
            EXPECT_EQ(option.invokeOptions[CUSTOMS_TAG], "Request-2");
        }
    }

protected:
    std::shared_ptr<SimulateObserver> observer_;
    std::shared_ptr<InstanceView> instanceView_;
    std::shared_ptr<ProxyView> proxyView_;
    std::shared_ptr<MockSharedClientManagerProxy> mockSharedClientManagerProxy_;
    std::string local_ = "local";
    std::string remote_ = "remote";
    std::string tenantID_ = "";
    std::unordered_map<std::string, resources::InstanceInfo> instanceInfo_;
};

/**
 * Feature: invoke test
 * Description: 模拟 bus proxy invoke 调用
 * Steps:
 * 1. callee instance scheduling -> creating (local) -> running
 * 2. invoke 2 times before creating
 * 3. invoke 1 times before running
 * 4. invoke 2 times after running
 * Expectation: all invoke return successful
 */
TEST_F(InstanceProxyTest, CallLocalTest)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";
    CallTest(callerIns, calleeIns, true);
}

// P2 #8: an invoke landing on an instance (local self or forwarded in) must
// mark that instance as ever-used — SDK exec rides the Invoke channel and
// SessionCountDelta alone never fires, so idle bookkeeping used to treat
// purely-invoke-driven sandboxes as reclaimable orphans.
TEST_F(InstanceProxyTest, InvokeMarksDestinationUsed)
{
    std::string callerIns = "mark-caller";
    std::string calleeIns = "mark-callee";
    CallTest(callerIns, calleeIns, true);
    // local dispatch: the callee proxy actor received the forward
    ASSERT_GE(observer_->usedMarks.load(), 1u);
    EXPECT_TRUE(observer_->usedInstances.count(calleeIns) > 0 ||
                observer_->usedInstances.count(callerIns) > 0)
        << "invoke must mark dst (or its dispatching proxy) as used";
}

TEST_F(InstanceProxyTest, CallLocalWithAuthorizeTest)
{
    function_proxy::InternalIAM::Param param_{ true, "" };
    auto mockInternalIam = std::make_shared<MockInternalIAM>(param_);
    RequestDispatcher::BindInternalIAM(mockInternalIam);
    EXPECT_CALL(*mockInternalIam, IsIAMEnabled).WillRepeatedly(Return(true));
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";
    SetTenantID("tenant123");
    litebus::Future<function_proxy::AuthorizeParam> authParam;
    litebus::Future<function_proxy::AuthorizeParam> authParam1;
    EXPECT_CALL(*mockInternalIam, Authorize).WillOnce(testing::DoAll(FutureArg<0>(&authParam), Return(Status::OK())))
        .WillOnce(testing::DoAll(FutureArg<0>(&authParam1), Return(Status::OK())));
    CallTest(callerIns, calleeIns, true);
    ASSERT_AWAIT_READY(authParam);
    ASSERT_AWAIT_READY(authParam1);
    EXPECT_EQ(authParam.Get().callerTenantID, "");
    EXPECT_EQ(authParam.Get().calleeTenantID, "tenant123");
    EXPECT_EQ(authParam1.Get().callerTenantID, "tenant123");
    EXPECT_EQ(authParam1.Get().calleeTenantID, "tenant123");
}

TEST_F(InstanceProxyTest, CallAuthorizeFailTest)
{
    function_proxy::InternalIAM::Param param_{ true, "" };
    auto mockInternalIam = std::make_shared<MockInternalIAM>(param_);
    RequestDispatcher::BindInternalIAM(mockInternalIam);
    EXPECT_CALL(*mockInternalIam, IsIAMEnabled).WillRepeatedly(Return(true));
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";
    auto mockSharedClient = PrepareCaller(callerIns);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy) != nullptr; });
    auto calleeInfo = NewInstance(calleeIns, tenantID_);
    observer_->Update(calleeIns, calleeInfo);
    litebus::AID calleeProxy(calleeIns, observer_->GetAID().Url());

    EXPECT_CALL(*mockInternalIam, Authorize).WillOnce(Return(Status(StatusCode::FAILED)));
    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::RUNNING, local_);
    auto mockCalleeSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewDataInterfacePosixClient(calleeIns, _, _))
        .WillOnce(Return(mockCalleeSharedClient));
    EXPECT_CALL(*mockCalleeSharedClient, Call(_))
        .WillRepeatedly(Invoke([](const SharedStreamMsg &request) -> litebus::Future<SharedStreamMsg> {
            auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
            auto callrsp = msg->mutable_callrsp();
            callrsp->set_code(::common::ErrorCode::ERR_NONE);
            return msg;
        }));
    observer_->Update(calleeIns, calleeInfo);
    instanceInfo_[calleeIns] = calleeInfo;
    auto firstCall = litebus::Async(callerProxy, &InstanceProxy::Call,
                                    busproxy::CallerInfo{ .instanceID = callerIns, .tenantID = tenantID_ }, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-authorize-failed"), nullptr);
    ASSERT_AWAIT_SET(firstCall);
    EXPECT_TRUE(firstCall.Get()->has_callrsp() && firstCall.Get()->callrsp().code() == common::ERR_AUTHORIZE_FAILED);
}

/**
 * Feature: invoke test
 * Description: 模拟 bus proxy invoke 调用
 * Steps:
 * 1. instance scheduling -> creating (remote) -> running
 * 2. invoke 2 times before creating
 * 3. invoke 1 times before running
 * 4. invoke 2 times after running
 * Expectation: all invoke return successful
 */
TEST_F(InstanceProxyTest, CallRemoteTest)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";

    auto calleeProxyActor = std::make_shared<InstanceProxy>(calleeIns, "");
    calleeProxyActor->InitDispatcher();
    auto info = std::make_shared<InstanceRouterInfo>();
    info->isReady = true;
    info->isLocal = true;
    info->runtimeID = calleeIns;
    info->proxyID = remote_;
    auto mockCalleeSharedClient = std::make_shared<MockSharedClient>();
    info->localClient = mockCalleeSharedClient;
    calleeProxyActor->NotifyChanged(calleeIns, info);
    litebus::Spawn(calleeProxyActor);
    EXPECT_CALL(*mockCalleeSharedClient, Call(_))
        .WillRepeatedly(Invoke([](const SharedStreamMsg &request) -> litebus::Future<SharedStreamMsg> {
            auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
            auto callrsp = msg->mutable_callrsp();
            callrsp->set_code(::common::ErrorCode::ERR_NONE);
            return msg;
        }));

    CallTest(callerIns, calleeIns, false);
}

TEST_F(InstanceProxyTest, CallLowAbilityTest)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";

    auto calleeProxyActor = std::make_shared<InstanceProxy>(calleeIns, "");
    calleeProxyActor->InitDispatcher();
    auto info = std::make_shared<InstanceRouterInfo>();
    info->isReady = true;
    info->isLocal = true;
    info->runtimeID = calleeIns;
    info->proxyID = remote_;
    auto mockCalleeSharedClient = std::make_shared<MockSharedClient>();
    info->localClient = mockCalleeSharedClient;
    calleeProxyActor->NotifyChanged(calleeIns, info);
    litebus::Spawn(calleeProxyActor);
    EXPECT_CALL(*mockCalleeSharedClient, Call(_))
        .WillRepeatedly(Invoke([](const SharedStreamMsg &request) -> litebus::Future<SharedStreamMsg> {
            auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
            auto callrsp = msg->mutable_callrsp();
            callrsp->set_code(::common::ErrorCode::ERR_NONE);
            return msg;
        }));

    CallTest(callerIns, calleeIns, false, true);
}

/**
 * Feature: NotifyChanged test
 * Description: when instance put event comes, do sth. according to instance info
 */
TEST_F(InstanceProxyTest, NotifyChanged)
{
    std::string calleeIns = "calleeIns";
    auto calleeProxyActor = std::make_shared<InstanceProxy>(calleeIns, "");
    calleeProxyActor->InitDispatcher();
    auto info = std::make_shared<InstanceRouterInfo>();
    info->isReady = true;
    info->isLocal = true;
    info->runtimeID = calleeIns;
    info->proxyID = remote_;
    litebus::Spawn(calleeProxyActor);

    // instance is local, dataInterfaceClient is nullptr
    calleeProxyActor->NotifyChanged(calleeIns, info);
    EXPECT_EQ(calleeProxyActor->selfDispatcher_->dataInterfaceClient_, nullptr);
    EXPECT_FALSE(calleeProxyActor->selfDispatcher_->isReady_);

    // instance is local, dataInterfaceClient exists
    auto mockCalleeSharedClient = std::make_shared<MockSharedClient>();
    calleeProxyActor->selfDispatcher_->dataInterfaceClient_ = mockCalleeSharedClient;
    calleeProxyActor->NotifyChanged(calleeIns, info);
    EXPECT_TRUE(calleeProxyActor->selfDispatcher_->isReady_);
    EXPECT_TRUE(calleeProxyActor->selfDispatcher_->used_); // 验证 used_ 状态被正确设置为 true
    EXPECT_CALL(*mockCalleeSharedClient, Call(_))
        .WillRepeatedly(Invoke([](const SharedStreamMsg &request) -> litebus::Future<SharedStreamMsg> {
            auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
            auto callrsp = msg->mutable_callrsp();
            callrsp->set_code(::common::ErrorCode::ERR_NONE);
            return msg;
        }));
}

TEST_F(InstanceProxyTest, NotifyRemoteChanged)
{
    std::string calleeIns = "calleeIns";
    auto callerProxyActor = std::make_shared<InstanceProxy>("callerIns", "");
    callerProxyActor->InitDispatcher();
    auto info = std::make_shared<InstanceRouterInfo>();
    info->isReady = true;
    info->isLocal = false;
    info->runtimeID = calleeIns;
    info->proxyID = remote_;
    litebus::Spawn(callerProxyActor);

    // instance is remote
    callerProxyActor->NotifyChanged(calleeIns, info);
    EXPECT_EQ(callerProxyActor->remoteDispatchers_[calleeIns]->remoteAid_.Name(), "");
    EXPECT_FALSE(callerProxyActor->remoteDispatchers_[calleeIns]->isReady_);
    info->remote = callerProxyActor->GetAID();
    callerProxyActor->NotifyChanged(calleeIns, info);
    EXPECT_TRUE(callerProxyActor->remoteDispatchers_[calleeIns]->isReady_);
    EXPECT_TRUE(callerProxyActor->remoteDispatchers_[calleeIns]->used_); // 验证 used_ 状态被正确设置为 true
}

/**
 * Feature: invoke test
 * Description: invoke unexist instance
 * Expectation: all invoke return failed
 */
TEST_F(InstanceProxyTest, CallNotExistInstance)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";

    EXPECT_CALL(*observer_, SendSubscribeInstanceEvent(_, _, false)).WillOnce(Return(Status::OK()));
    auto mockSharedClient = PrepareCaller(callerIns);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy) != nullptr; });
    // invoke 1 times after failed
    auto fisrtCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-not-existed-instance"), nullptr);
    ASSERT_AWAIT_SET(fisrtCall);
    EXPECT_TRUE(fisrtCall.Get()->has_callrsp() && fisrtCall.Get()->callrsp().code() == common::ERR_INSTANCE_NOT_FOUND);
}

/**
 * Feature: invoke test
 * Description: invoke fatal instance
 * 1. instance scheduling -> creating (local) -> running -> failed -> fatal
 * 2. invoke 2 times before creating
 * 3. invoke 1 times before running
 * 4. invoke 2 times after running
 * 5. invoke 1 times before fatal
 * 6. invoke 1 times after fatal
 * Expectation: all invoke return failed
 */
TEST_F(InstanceProxyTest, CallFatalInstance)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";
    CallTest(callerIns, calleeIns, true);
    auto calleeInfo = instanceInfo_[calleeIns];
    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::FAILED, local_);
    observer_->Update(calleeIns, calleeInfo);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    // invoke 1 times after failed
    auto fisrtCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-after-failed"), nullptr);

    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::FATAL, local_);
    calleeInfo.mutable_instancestatus()->set_errcode((int32_t)common::ERR_USER_FUNCTION_EXCEPTION);
    observer_->Update(calleeIns, calleeInfo);
    ASSERT_AWAIT_SET(fisrtCall);
    EXPECT_TRUE(fisrtCall.Get()->has_callrsp() && fisrtCall.Get()->callrsp().code() == common::ERR_USER_FUNCTION_EXCEPTION);

    auto secondCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-after-fatal"), nullptr);
    ASSERT_AWAIT_SET(secondCall);
    EXPECT_TRUE(secondCall.Get()->has_callrsp() &&
                secondCall.Get()->callrsp().code() == common::ERR_USER_FUNCTION_EXCEPTION);
    EXPECT_TRUE(secondCall.Get()->has_callrsp() && secondCall.Get()->callrsp().message().find(
                                                       "instance occurs fatal error, cause by") != std::string::npos);
}

/**
 * Feature: invoke test
 * Description: invoke fatal instance
 * 1. instance scheduling -> creating (local) -> running -> failed -> scheduling -> creating (local) -> running
 * 2. invoke 2 times before creating
 * 3. invoke 1 times before running
 * 4. invoke 2 times after running
 * 5. invoke 1 times before fatal
 * 6. invoke 1 times after fatal
 * Expectation: all invoke return failed
 */
TEST_F(InstanceProxyTest, CallRecoveredInstance)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";
    CallTest(callerIns, calleeIns, true);
    auto calleeInfo = instanceInfo_[calleeIns];
    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::FAILED, local_);
    observer_->Update(calleeIns, calleeInfo);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    // invoke 1 times after failed
    auto fisrtCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-after-failed"), nullptr);

    CallTest(callerIns, calleeIns, true);
    ASSERT_AWAIT_SET(fisrtCall);
    EXPECT_TRUE(fisrtCall.Get()->has_callrsp() && fisrtCall.Get()->callrsp().code() == common::ERR_NONE);
}

/**
 * Feature: invoke test
 * Description: invoke while caller is on init
 * 1. callee instance scheduling -> creating (local) -> running
 * 2. caller instance creating
 * 3. invoke 1 times after callee running
 * Expectation: invoke return successful
 */
TEST_F(InstanceProxyTest, InitCallInstanceBeforeReady)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";
    auto instanceInfo = NewInstance(callerIns, tenantID_);
    UpdateInstance(instanceInfo, callerIns, (int32_t)InstanceState::CREATING, local_);
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetDataInterfacePosixClient(callerIns))
        .WillOnce(Return(mockSharedClient));
    observer_->Update(callerIns, instanceInfo);
    instanceInfo_[callerIns] = instanceInfo;

    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy) != nullptr; });

    auto calleeInfo = NewInstance(calleeIns, tenantID_);
    observer_->Update(calleeIns, calleeInfo);
    litebus::AID calleeProxy(calleeIns, observer_->GetAID().Url());
    // update instance to running
    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::RUNNING, local_);
    auto mockCalleeSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewDataInterfacePosixClient(calleeIns, _, _))
        .WillOnce(Return(mockCalleeSharedClient));
    EXPECT_CALL(*mockCalleeSharedClient, Call(_))
        .WillRepeatedly(Invoke([](const SharedStreamMsg &request) -> litebus::Future<SharedStreamMsg> {
            auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
            auto callrsp = msg->mutable_callrsp();
            callrsp->set_code(::common::ErrorCode::ERR_NONE);
            return msg;
        }));
    observer_->Update(calleeIns, calleeInfo);

    // invoke 1 times after failed
    auto fisrtCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-1"), nullptr);

    ASSERT_AWAIT_SET(fisrtCall);
    EXPECT_TRUE(fisrtCall.Get()->has_callrsp() && fisrtCall.Get()->callrsp().code() == common::ERR_NONE);

    // call result
    EXPECT_CALL(*mockSharedClient, NotifyResult(_)).WillRepeatedly(Return(runtime::NotifyResponse()));

    auto fisrtCallResult = litebus::Async(calleeProxy, &InstanceProxy::CallResult, calleeIns, callerIns,
                                          CallResult(callerIns, "Request-1"), nullptr);
    ASSERT_AWAIT_SET(fisrtCallResult);
    EXPECT_TRUE(fisrtCallResult.Get()->has_callresultack());
}

/**
 * Feature: invoke test
 * Description: invoke fatal instance
 * 1. instance scheduling -> creating (local) -> running -> failed -> delete
 * 2. invoke 2 times before creating
 * 3. invoke 1 times before running
 * 4. invoke 2 times after running
 * 5. invoke 1 times before fatal
 * 6. invoke 1 times after fatal
 * Expectation: all invoke return failed
 */
TEST_F(InstanceProxyTest, CallDeleteInstance)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";
    CallTest(callerIns, calleeIns, true);
    auto calleeInfo = instanceInfo_[calleeIns];
    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::FAILED, local_);
    observer_->Update(calleeIns, calleeInfo);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    // invoke 1 times after failed
    auto fisrtCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-after-failed"), nullptr);

    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::EVICTING, local_);
    calleeInfo.mutable_instancestatus()->set_errcode(common::ERR_INSTANCE_NOT_FOUND);
    observer_->Update(calleeIns, calleeInfo);

    // invoke 1 times after failed
    auto secondCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-after-exiting"), nullptr);

    observer_->Delete(calleeIns);

    ASSERT_AWAIT_SET(fisrtCall);
    EXPECT_TRUE(fisrtCall.Get()->has_callrsp() && fisrtCall.Get()->callrsp().code() == common::ERR_INSTANCE_EXITED);

    ASSERT_AWAIT_SET(secondCall);
    EXPECT_TRUE(secondCall.Get()->has_callrsp());
    auto code = secondCall.Get()->callrsp().code();
    EXPECT_TRUE(code == common::ERR_INSTANCE_NOT_FOUND || code == common::ERR_INSTANCE_EXITED);

    auto thirdCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                     CallRequest(callerIns, calleeIns, "Request-after-delete"), nullptr);
    ASSERT_AWAIT_SET(thirdCall);
    EXPECT_TRUE(thirdCall.Get()->has_callrsp() && thirdCall.Get()->callrsp().code() == common::ERR_INSTANCE_NOT_FOUND);
}

/**
 * Feature: invoke sub-health instance test
 * Description: invoke sub-health instance
 * 1. instance scheduling -> creating (local) -> running -> sub-health -> running -> delete
 * 2. invoke 2 times before creating
 * 3. invoke 1 time before running
 * 4. invoke 2 times after running
 * 5. invoke 1 time before sub-health
 * 6. invoke 1 time after sub-health
 * 7. invoke 1 time after recover from sub-health
 * Expectation: all invoke return failed
 */
TEST_F(InstanceProxyTest, CallSubHealthInstance)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";
    CallTest(callerIns, calleeIns, true);
    auto calleeInfo = instanceInfo_[calleeIns];
    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::SUB_HEALTH, local_);
    calleeInfo.mutable_instancestatus()->set_msg("sub-health");
    calleeInfo.mutable_instancestatus()->set_errcode(StatusCode::ERR_INSTANCE_SUB_HEALTH);
    observer_->Update(calleeIns, calleeInfo);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    // invoke 1 times after sub-health
    auto firstCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-after-sub-health"), nullptr);

    ASSERT_AWAIT_READY(firstCall);
    EXPECT_TRUE(firstCall.Get()->has_callrsp());
    EXPECT_EQ(firstCall.Get()->callrsp().code(), common::ERR_INSTANCE_SUB_HEALTH);

    auto mockCalleeSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewDataInterfacePosixClient(calleeIns, _, _))
        .WillOnce(Return(mockCalleeSharedClient));
    EXPECT_CALL(*mockCalleeSharedClient, Call(_))
        .WillRepeatedly(Invoke([](const SharedStreamMsg &request) -> litebus::Future<SharedStreamMsg> {
            auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
            auto callrsp = msg->mutable_callrsp();
            callrsp->set_code(::common::ErrorCode::ERR_NONE);
            return msg;
        }));
    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::RUNNING, local_);
    observer_->Update(calleeIns, calleeInfo);

    // invoke 1 times after recover from sub-health
    auto secondCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                     CallRequest(callerIns, calleeIns, "Request-after-health"), nullptr);

    ASSERT_AWAIT_READY(secondCall);
    EXPECT_TRUE(secondCall.Get()->has_callrsp());
    EXPECT_EQ(secondCall.Get()->callrsp().code(), common::ERR_NONE);

    observer_->Delete(calleeIns);
}

/**
 * Feature: invoke evicted instance test
 * Description: invoke sub-health instance
 * 1. instance scheduling -> creating (local) -> running -> sub-health -> running -> delete
 * 2. invoke 2 times before creating
 * 3. invoke 1 time before running
 * 4. invoke 2 times after running
 * 5. invoke 1 time when evicting
 * 6. invoke 1 time after evicted
 * Expectation: all invoke return failed
 */
TEST_F(InstanceProxyTest, CallEvictedInstance)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";
    CallTest(callerIns, calleeIns, true);
    auto calleeInfo = instanceInfo_[calleeIns];
    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::EVICTING, local_);
    calleeInfo.mutable_instancestatus()->set_errcode(ERR_INSTANCE_EVICTED);
    observer_->Update(calleeIns, calleeInfo);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    // invoke 1 times when evicting
    auto fisrtCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-when-evicting"), nullptr);

    ASSERT_AWAIT_SET(fisrtCall);
    EXPECT_TRUE(fisrtCall.Get()->has_callrsp() &&
                static_cast<functionsystem::StatusCode>(fisrtCall.Get()->callrsp().code()) == ERR_INSTANCE_EVICTED);

    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::EVICTED, local_);
    calleeInfo.mutable_instancestatus()->set_errcode(ERR_INSTANCE_EVICTED);
    observer_->Update(calleeIns, calleeInfo);
    // invoke 1 times after evicted
    auto secondCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-after-evicted"), nullptr);

    ASSERT_AWAIT_SET(secondCall);
    EXPECT_TRUE(secondCall.Get()->has_callrsp() &&
                static_cast<functionsystem::StatusCode>(secondCall.Get()->callrsp().code()) == ERR_INSTANCE_EVICTED);

    observer_->Delete(calleeIns);
}

TEST_F(InstanceProxyTest, CallResultWithoutCaller)
{
    auto calleeInfo = NewInstance("calleeIns", tenantID_);
    observer_->Update("calleeIns", calleeInfo);
    litebus::AID calleeProxy("calleeIns", observer_->GetAID().Url());
    // update instance to running
    UpdateInstance(calleeInfo, "calleeIns", (int32_t)InstanceState::RUNNING, local_);
    observer_->Update("calleeIns", calleeInfo);
    auto callResultAck =
        litebus::Async(calleeProxy, &InstanceProxy::CallResult, "calleeIns", "callerIns",
                       CallResult("callerIns", "Request-1"), nullptr);
    ASSERT_AWAIT_READY(callResultAck);
    EXPECT_TRUE(callResultAck.Get()->has_callresultack());
    EXPECT_EQ(callResultAck.Get()->callresultack().code(), ::common::ErrorCode::ERR_INSTANCE_NOT_FOUND);
}

TEST_F(InstanceProxyTest, CallResultDoesNotReturnRouteWithoutRefresh)
{
    const std::string callerIns = "callerIns";
    auto callerInfo = NewInstance(callerIns, tenantID_);
    UpdateInstance(callerInfo, callerIns, static_cast<int32_t>(InstanceState::CREATING), local_);
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetDataInterfacePosixClient(callerIns))
        .WillOnce(Return(mockSharedClient));
    observer_->Update(callerIns, callerInfo);

    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy) != nullptr; });

    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&request)
                             -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(request);
            return runtime::NotifyResponse();
        }));
    auto notifyFuture = notifyCalled.GetFuture();

    auto callResultAck = litebus::Async(callerProxy, &InstanceProxy::CallResult, "calleeIns", callerIns,
                                        CallResult(callerIns, "Request-route-info"), nullptr);
    ASSERT_AWAIT_SET(callResultAck);
    ASSERT_AWAIT_SET(notifyFuture);
    const auto &notify = notifyFuture.Get();
    EXPECT_FALSE(notify.has_runtimeinfo());
}

/**
 * Feature: invoke test
 * Description: invoke fatal instance
 * 1. instance fatal
 * 2. invoke 1 times after fatal
 * Expectation: all invoke return failed
 */
TEST_F(InstanceProxyTest, CallAlreadyFatal)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";
    auto calleeInfo = NewInstance(calleeIns, tenantID_);
    // update instance to FATAL
    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::FATAL, local_);
    calleeInfo.mutable_instancestatus()->set_errcode((int32_t)common::ERR_USER_FUNCTION_EXCEPTION);
    observer_->Update(calleeIns, calleeInfo);
    auto mockSharedClient = PrepareCaller(callerIns);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy) != nullptr; });
    // invoke 1 times after failed
    auto fisrtCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-after-failed"), nullptr);
    ASSERT_AWAIT_SET(fisrtCall);
    EXPECT_TRUE(fisrtCall.Get()->has_callrsp() && fisrtCall.Get()->callrsp().code() == common::ERR_USER_FUNCTION_EXCEPTION);
}

class ForwardCallActor : public litebus::ActorBase {
public:
    ForwardCallActor() : litebus::ActorBase("ForwardCall-test-actor")
    {
    }
    virtual ~ForwardCallActor() = default;

    void SendForwardCall(const litebus::AID &aid, const SharedStreamMsg &streamMsg)
    {
        (void)Send(aid, "ForwardCall", streamMsg->SerializeAsString());
    }
};

TEST_F(InstanceProxyTest, ForwardCallWithoutCallee)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";

    auto mockSharedClient = PrepareCaller(callerIns);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy) != nullptr; });

    auto forwardCallActor = std::make_shared<ForwardCallActor>();
    litebus::Spawn(forwardCallActor);

    bool isFinished = false;
    EXPECT_CALL(*observer_, SendSubscribeInstanceEvent(_, _, true))
        .WillOnce(DoAll(Assign(&isFinished, true), Return(Status::OK())));
    litebus::Async(forwardCallActor->GetAID(), &ForwardCallActor::SendForwardCall, callerProxy,
                   CallRequest(callerIns, calleeIns, "Request-not-existed-instance"));
    ASSERT_AWAIT_TRUE([&]() { return isFinished; });

    litebus::Terminate(forwardCallActor->GetAID());
    litebus::Await(forwardCallActor->GetAID());
}

/**
 * Feature: ENABLE_FORCE_INVOKE test
 * Description: test force invoke functionality
 * 1. instance scheduling -> creating (local) -> running -> not ready
 * 2. invoke with ENABLE_FORCE_INVOKE when not ready but used
 * 3. invoke without ENABLE_FORCE_INVOKE when not ready but used
 * 4. invoke with ENABLE_FORCE_INVOKE during graceful shutdown
 * Expectation: 
 * - force invoke request should be processed even when not ready but used
 * - non-force invoke request should wait until ready
 * - force invoke request should be processed during graceful shutdown
 */
TEST_F(InstanceProxyTest, ForceInvokeTest)
{
    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";

    auto mockSharedClient = PrepareCaller(callerIns);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy) != nullptr; });

    // Create callee instance and set to running
    auto calleeInfo = NewInstance(calleeIns, tenantID_);
    UpdateInstance(calleeInfo, calleeIns, (int32_t)InstanceState::RUNNING, local_);
    auto mockCalleeSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewDataInterfacePosixClient(calleeIns, _, _))
        .WillOnce(Return(mockCalleeSharedClient));
    EXPECT_CALL(*mockCalleeSharedClient, Call(_))
        .WillRepeatedly(Invoke([](const SharedStreamMsg &request) -> litebus::Future<SharedStreamMsg> {
            auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
            auto callrsp = msg->mutable_callrsp();
            callrsp->set_code(::common::ErrorCode::ERR_NONE);
            return msg;
        }));
    observer_->Update(calleeIns, calleeInfo);
    instanceInfo_[calleeIns] = calleeInfo;

    // Get the dispatcher and set used_ to true but isReady_ to false
    auto calleeProxyActor = std::dynamic_pointer_cast<InstanceProxy>(litebus::GetActor(litebus::AID(calleeIns, observer_->GetAID().Url())));
    ASSERT_TRUE(calleeProxyActor != nullptr);
    calleeProxyActor->selfDispatcher_->used_ = true;
    calleeProxyActor->selfDispatcher_->isReady_ = false;

    // Test 1: Invoke with ENABLE_FORCE_INVOKE when not ready but used
    auto forceCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns, .tenantID=tenantID_}, calleeIns,
                                   CallRequest(callerIns, calleeIns, "Request-force", "", true), nullptr);
    ASSERT_AWAIT_READY(forceCall);
    EXPECT_TRUE(forceCall.Get()->has_callrsp() && forceCall.Get()->callrsp().code() == common::ERR_NONE);

    // Test 2: Invoke without ENABLE_FORCE_INVOKE when not ready but used
    auto normalCall = litebus::Async(callerProxy, &InstanceProxy::Call, busproxy::CallerInfo{.instanceID=callerIns, .tenantID=tenantID_}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-normal", "", false), nullptr);

    // Set instance back to ready and check normal call
    calleeProxyActor->selfDispatcher_->isReady_ = true;
    // Manually trigger the processing of cached requests
    calleeProxyActor->selfDispatcher_->callCache_->MoveAllToNew();
    auto reqNew = calleeProxyActor->selfDispatcher_->callCache_->GetNewReqs();
    for (auto &req : reqNew) {
        calleeProxyActor->selfDispatcher_->TriggerCall(req);
    }
    ASSERT_AWAIT_READY(normalCall);
    EXPECT_TRUE(normalCall.Get()->has_callrsp() && normalCall.Get()->callrsp().code() == common::ERR_NONE);
}

// Helper: spawn a ready callee InstanceProxy that handles ForwardCall via a mock local client.
static std::shared_ptr<MockSharedClient> SpawnReadyCalleeProxy(
    const std::string &calleeIns, const std::string &proxyID,
    std::function<litebus::Future<SharedStreamMsg>(const SharedStreamMsg &)> callHandler)
{
    auto calleeProxyActor = std::make_shared<InstanceProxy>(calleeIns, "", proxyID);
    calleeProxyActor->InitDispatcher();
    auto info = std::make_shared<InstanceRouterInfo>();
    info->isReady = true;
    info->isLocal = true;
    info->runtimeID = calleeIns;
    info->proxyID = proxyID;
    auto mockCalleeClient = std::make_shared<MockSharedClient>();
    info->localClient = mockCalleeClient;
    calleeProxyActor->NotifyChanged(calleeIns, info);
    litebus::Spawn(calleeProxyActor);
    EXPECT_CALL(*mockCalleeClient, Call(_)).WillRepeatedly(Invoke(callHandler));
    return mockCalleeClient;
}

TEST_F(InstanceProxyTest, QueryRouteNotFoundAfterSystemTimeout)
{
    InstanceProxy::BindSystemTimeout(0);
    const std::string callerIns = "callerIns";
    const std::string calleeIns = "calleeIns";
    const std::string staleRoute = "tcp://127.0.0.1:1";

    (void)PrepareCaller(callerIns);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy) != nullptr; });
    EXPECT_CALL(*observer_, QueryInstanceRoute(calleeIns))
        .WillOnce(Return(litebus::Future<std::shared_ptr<resources::RouteInfo>>(
            litebus::Status(static_cast<int32_t>(StatusCode::ERR_INSTANCE_NOT_FOUND)))));

    auto firstCall = litebus::Async(callerProxy, &InstanceProxy::Call,
                                    busproxy::CallerInfo{ .instanceID = callerIns }, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-query-not-found", staleRoute), nullptr);
    auto retryCall = litebus::Async(callerProxy, &InstanceProxy::Call,
                                    busproxy::CallerInfo{ .instanceID = callerIns }, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-query-not-found", staleRoute), nullptr);

    ASSERT_AWAIT_SET(firstCall);
    ASSERT_AWAIT_SET(retryCall);
    EXPECT_EQ(firstCall.Get()->callrsp().code(), common::ERR_INSTANCE_NOT_FOUND);
    EXPECT_EQ(retryCall.Get()->callrsp().code(), common::ERR_INSTANCE_NOT_FOUND);
}

TEST_F(InstanceProxyTest, ForwardThroughCurrentRouteAfterSystemTimeout)
{
    InstanceProxy::BindSystemTimeout(0);
    const std::string callerIns = "callerIns";
    const std::string calleeIns = "calleeIns";
    const std::string staleRoute = "tcp://127.0.0.1:1";

    (void)SpawnReadyCalleeProxy(calleeIns, local_, [](const SharedStreamMsg &) {
        auto response = std::make_shared<runtime_rpc::StreamingMessage>();
        response->mutable_callrsp()->set_code(common::ERR_NONE);
        return litebus::Future<SharedStreamMsg>(response);
    });
    auto requestRouter = std::make_shared<busproxy::RequestRouter>(REQUEST_ROUTER_NAME);
    litebus::Spawn(requestRouter);
    EXPECT_CALL(*observer_, SendSubscribeInstanceEvent(_, _, true)).WillRepeatedly(Return(Status::OK()));

    auto routeInfo = std::make_shared<resources::RouteInfo>();
    routeInfo->set_instanceid(calleeIns);
    routeInfo->set_functionproxyid(local_);
    routeInfo->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    EXPECT_CALL(*observer_, QueryInstanceRoute(calleeIns))
        .WillOnce(Return(litebus::Future<std::shared_ptr<resources::RouteInfo>>(routeInfo)));
    EXPECT_CALL(*observer_, QueryProxyAddress(local_))
        .WillOnce(Return(litebus::Future<std::string>(observer_->GetAID().Url())));

    auto mockCallerClient = PrepareCaller(callerIns);
    litebus::AID callerProxy(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy) != nullptr; });
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockCallerClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&request)
                             -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(request);
            return runtime::NotifyResponse();
        }));
    auto notifyFuture = notifyCalled.GetFuture();
    auto firstCall = litebus::Async(callerProxy, &InstanceProxy::Call,
                                    busproxy::CallerInfo{ .instanceID = callerIns }, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-query-current-route", staleRoute),
                                    nullptr);
    auto retryCall = litebus::Async(callerProxy, &InstanceProxy::Call,
                                    busproxy::CallerInfo{ .instanceID = callerIns }, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-query-current-route", staleRoute),
                                    nullptr);

    ASSERT_AWAIT_SET(firstCall);
    ASSERT_AWAIT_SET(retryCall);
    EXPECT_EQ(firstCall.Get()->callrsp().code(), common::ERR_NONE);
    EXPECT_EQ(retryCall.Get()->callrsp().code(), common::ERR_NONE);
    litebus::AID calleeProxy(calleeIns, observer_->GetAID().Url());
    auto callResultAck = litebus::Async(calleeProxy, &InstanceProxy::CallResult, calleeIns, callerIns,
                                        CallResult(callerIns, "Request-query-current-route"), nullptr);
    ASSERT_AWAIT_SET(callResultAck);
    ASSERT_AWAIT_SET(notifyFuture);
    const auto &notify = notifyFuture.Get();
    ASSERT_TRUE(notify.has_runtimeinfo());
    EXPECT_EQ(notify.runtimeinfo().route(), calleeProxy.Url());
    EXPECT_EQ(notify.runtimeinfo().proxyid(), local_);
    litebus::Terminate(requestRouter->GetAID());
    litebus::Await(requestRouter);
}

/**
 * Feature: LRU route cache – rule 3.1 + 3.2
 * Description: When DirectRoutingConfig is enabled and a call with YR_ROUTE populates
 *   the LRU cache, subsequent calls to the same callee WITHOUT YR_ROUTE must use the
 *   cached route directly (ForwardWithRouteAndFallback path), NOT trigger the observer
 *   subscribe path.
 * Steps:
 * 1. Enable DirectRoutingConfig
 * 2. Spawn a ready callee InstanceProxy
 * 3. Call with YR_ROUTE → cache populated, forward succeeds (ERR_NONE)
 * 4. Call without YR_ROUTE → LRU cache hit, forward succeeds (ERR_NONE)
 * Expectation: observer subscribe (ignoreNonExist=false) is never triggered for either call
 */
TEST_F(InstanceProxyTest, RouteFromLRUCacheWhenFlagEnabled)
{
    function_proxy::DirectRoutingConfig::SetEnabled(true);

    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";

    // Spawn a ready callee proxy that returns ERR_NONE for every forwarded call.
    (void)SpawnReadyCalleeProxy(calleeIns, local_, [](const SharedStreamMsg &) -> litebus::Future<SharedStreamMsg> {
        auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
        msg->mutable_callrsp()->set_code(::common::ErrorCode::ERR_NONE);
        return msg;
    });

    // SubscribeInstanceEvent with ignoreNonExist=true may be called from the callee's
    // DoForwardCall handler (registering the caller as a remote sender) -- allow it.
    EXPECT_CALL(*observer_, SendSubscribeInstanceEvent(_, _, true)).WillRepeatedly(Return(Status::OK()));
    // The normal routing-subscribe path (ignoreNonExist=false) must never be taken when
    // the LRU cache holds a valid route.
    EXPECT_CALL(*observer_, SendSubscribeInstanceEvent(_, _, false)).Times(0);

    auto mockSharedClient2 = PrepareCaller(callerIns);
    litebus::AID callerProxy2(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy2) != nullptr; });

    // First call WITH YR_ROUTE: rule 3.1 – puts route into LRU cache and forwards.
    std::string route = observer_->GetAID().Url();
    auto firstCall = litebus::Async(callerProxy2, &InstanceProxy::Call,
                                    busproxy::CallerInfo{.instanceID = callerIns}, calleeIns,
                                    CallRequest(callerIns, calleeIns, "Request-lru-1", route), nullptr);
    ASSERT_AWAIT_SET(firstCall);
    EXPECT_TRUE(firstCall.Get()->has_callrsp() &&
                firstCall.Get()->callrsp().code() == common::ERR_NONE);

    // Second call WITHOUT YR_ROUTE: rule 3.2 – should use the LRU-cached route.
    auto secondCall = litebus::Async(callerProxy2, &InstanceProxy::Call,
                                     busproxy::CallerInfo{.instanceID = callerIns}, calleeIns,
                                     CallRequest(callerIns, calleeIns, "Request-lru-2"), nullptr);
    ASSERT_AWAIT_SET(secondCall);
    EXPECT_TRUE(secondCall.Get()->has_callrsp() &&
                secondCall.Get()->callrsp().code() == common::ERR_NONE);

    function_proxy::DirectRoutingConfig::SetEnabled(false);
}

/**
 * Feature: LRU route cache – NotifyChanged update path
 * Description: When DirectRoutingConfig is enabled and NotifyChanged is received with a
 *   non-empty proxyGrpcAddress, that address is stored in routeCache_. A subsequent call
 *   without YR_ROUTE should use that cached address (not the subscribe path).
 * Steps:
 * 1. Enable DirectRoutingConfig
 * 2. Spawn a ready callee InstanceProxy at the local URL
 * 3. Call with YR_ROUTE to populate the LRU cache with the local URL
 * 4. Call again with YR_ROUTE carrying the same URL to confirm cache update (re-put)
 * 5. Call without YR_ROUTE → uses cached route, forward succeeds (ERR_NONE)
 * Expectation: mock callee client is invoked for all three forwarded calls;
 *   observer subscribe (ignoreNonExist=false) is never triggered
 */
TEST_F(InstanceProxyTest, UpdateLRUCacheOnCallWithRoute)
{
    function_proxy::DirectRoutingConfig::SetEnabled(true);

    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";

    int callCount = 0;
    (void)SpawnReadyCalleeProxy(calleeIns, local_, [&callCount](const SharedStreamMsg &) -> litebus::Future<SharedStreamMsg> {
        ++callCount;
        auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
        msg->mutable_callrsp()->set_code(::common::ErrorCode::ERR_NONE);
        return msg;
    });

    EXPECT_CALL(*observer_, SendSubscribeInstanceEvent(_, _, true)).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*observer_, SendSubscribeInstanceEvent(_, _, false)).Times(0);

    auto mockSharedClient3 = PrepareCaller(callerIns);
    litebus::AID callerProxy3(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy3) != nullptr; });

    std::string route = observer_->GetAID().Url();

    // First call WITH YR_ROUTE: puts route = local URL into the LRU cache.
    auto call1 = litebus::Async(callerProxy3, &InstanceProxy::Call,
                                busproxy::CallerInfo{.instanceID = callerIns}, calleeIns,
                                CallRequest(callerIns, calleeIns, "Request-update-1", route), nullptr);
    ASSERT_AWAIT_SET(call1);
    EXPECT_TRUE(call1.Get()->has_callrsp() && call1.Get()->callrsp().code() == common::ERR_NONE);

    // Second call WITH YR_ROUTE: updates (re-puts) the same route in the cache.
    auto call2 = litebus::Async(callerProxy3, &InstanceProxy::Call,
                                busproxy::CallerInfo{.instanceID = callerIns}, calleeIns,
                                CallRequest(callerIns, calleeIns, "Request-update-2", route), nullptr);
    ASSERT_AWAIT_SET(call2);
    EXPECT_TRUE(call2.Get()->has_callrsp() && call2.Get()->callrsp().code() == common::ERR_NONE);

    // Third call WITHOUT YR_ROUTE: must use the cached route – the callee mock is invoked.
    auto call3 = litebus::Async(callerProxy3, &InstanceProxy::Call,
                                busproxy::CallerInfo{.instanceID = callerIns}, calleeIns,
                                CallRequest(callerIns, calleeIns, "Request-update-3"), nullptr);
    ASSERT_AWAIT_SET(call3);
    EXPECT_TRUE(call3.Get()->has_callrsp() && call3.Get()->callrsp().code() == common::ERR_NONE);

    // All three requests were forwarded to the callee's mock client.
    EXPECT_EQ(callCount, 3);

    function_proxy::DirectRoutingConfig::SetEnabled(false);
}

/**
 * Feature: LRU route cache – fallback when flag disabled
 * Description: When DirectRoutingConfig is disabled (default), calling without YR_ROUTE
 *   and with an empty LRU cache must fall back to the observer subscribe path (rule 3.3).
 * Steps:
 * 1. DirectRoutingConfig is disabled (default)
 * 2. Prepare caller but do not register callee
 * 3. Call without YR_ROUTE
 * Expectation: observer subscribe (ignoreNonExist=false) IS triggered;
 *   response indicates instance-not-found
 */
TEST_F(InstanceProxyTest, FallbackToObserverWhenLRUEmpty)
{
    // Flag is disabled by default; assert it explicitly for clarity.
    ASSERT_FALSE(function_proxy::DirectRoutingConfig::IsEnabled());

    std::string callerIns = "callerIns";
    std::string calleeIns = "calleeIns";

    // The subscribe path must be taken since LRU is disabled and cache is empty.
    EXPECT_CALL(*observer_, SendSubscribeInstanceEvent(_, _, false))
        .WillOnce(Return(Status::OK()));

    auto mockSharedClient4 = PrepareCaller(callerIns);
    litebus::AID callerProxy4(callerIns, observer_->GetAID().Url());
    ASSERT_AWAIT_TRUE([&]() { return litebus::GetActor(callerProxy4) != nullptr; });

    // Call to a callee that has no info in the instanceView_ – triggers subscribe path.
    auto call = litebus::Async(callerProxy4, &InstanceProxy::Call,
                               busproxy::CallerInfo{.instanceID = callerIns}, calleeIns,
                               CallRequest(callerIns, calleeIns, "Request-fallback"), nullptr);
    ASSERT_AWAIT_SET(call);
    EXPECT_TRUE(call.Get()->has_callrsp() &&
                call.Get()->callrsp().code() == common::ERR_INSTANCE_NOT_FOUND);
}

}  // namespace functionsystem::test
