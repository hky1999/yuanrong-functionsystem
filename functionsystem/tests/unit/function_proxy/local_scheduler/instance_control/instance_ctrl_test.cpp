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
#include <atomic>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <async/async.hpp>

#include "common/constants/actor_name.h"
#include "common/constants/signal.h"
#include "common/etcd_service/etcd_service_driver.h"
#include "common/logs/logging.h"
#include "common/metrics/metrics_adapter.h"
#include "common/metadata/metadata.h"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix_pb.h"
#include "common/resource_view/resource_type.h"
#include "common/resource_view/view_utils.h"
#include "common/utils/generate_message.h"
#include "function_proxy/common/posix_client/shared_client/shared_client_manager.h"
#include "function_proxy/common/posix_client/shared_client/posix_stream_manager_proxy.h"
#include "function_proxy/common/state_handler/state_handler.h"
#include "function_proxy/config/direct_routing_config.h"
#include "local_scheduler/instance_control/instance_ctrl_actor.h"
#include "local_scheduler/instance_control/instance_ctrl_message.h"
#include "mocks/mock_distributed_cache_client.h"
#include "mocks/mock_data_obj_client.h"
#include "mocks/mock_function_agent_mgr.h"
#include "mocks/mock_instance_control_view.h"
#include "mocks/mock_instance_operator.h"
#include "mocks/mock_instance_state_machine.h"
#include "mocks/mock_internal_iam.h"
#include "mocks/mock_local_instance_ctrl_actor.h"
#include "mocks/mock_local_sched_srv.h"
#include "mocks/mock_meta_store_client.h"
#include "mocks/mock_observer.h"
#include "mocks/mock_resource_group_ctrl.h"
#include "mocks/mock_resource_view.h"
#include "mocks/mock_scheduler.h"
#include "mocks/mock_shared_client.h"
#include "mocks/mock_shared_client_manager_proxy.h"
#include "mocks/mock_txn_transaction.h"
#include "utils/future_test_helper.h"
#include "local_scheduler/instance_control/instance_ctrl.h"
#include "utils/port_helper.h"

namespace functionsystem::test {
using namespace local_scheduler;
using namespace function_proxy;
using namespace ::testing;
using schedule_decision::ScheduleResult;

const std::string srcInstance = "srcInstance";
const std::string instanceID = "Instance";
const std::string instanceID1 = "InstanceID1";
const std::string runtimeID1 = "runtimeID1";
const std::string proxyID1 = "proxyID1";
const int32_t customSignal = 100;
const std::string mockInstanceCtrlActorName = "mockInstanceCtrlActor";
const RuntimeConfig runtimeConfig{
    .runtimeHeartbeatEnable = "true",
    .runtimeMaxHeartbeatTimeoutTimes = 3,
    .runtimeHeartbeatTimeoutMS = 2000,
    .runtimeInitCallTimeoutMS = 3000,
    .runtimeShutdownTimeoutSeconds = 3,
};

const std::string InstanceCreateFailureAlarmExporterJsonStr = R"(
{
    "enabledMetrics": ["yr_instance_create_failure_alarm"],
    "backends": [{
        "immediatelyExport": {
            "name": "Alarm",
            "enable": true,
            "exporters": [{
                "aomAlarmExporter": {
                    "enable": true,
                    "ip": "127.0.0.1:8080/",
                    "port": 9091
                }
            }]
        }
    }]
}
)";

const TransitionResult NONE_RESULT = TransitionResult{ litebus::None(), InstanceInfo() };
const TransitionResult NEW_RESULT = TransitionResult{ InstanceState::NEW, InstanceInfo() };
const TransitionResult SCHEDULING_RESULT =
    TransitionResult{ InstanceState::SCHEDULING, InstanceInfo(), InstanceInfo(), 1 };
const TransitionResult CREATING_RESULT = TransitionResult{ InstanceState::CREATING, InstanceInfo(), InstanceInfo(), 2 };
const TransitionResult RUNNING_RESULT = TransitionResult{ InstanceState::RUNNING, InstanceInfo(), InstanceInfo(), 3 };
const TransitionResult FAILED_RESULT = TransitionResult{ InstanceState::FAILED, InstanceInfo(), InstanceInfo(), 4 };
const TransitionResult FATAL_RESULT = TransitionResult{ InstanceState::FATAL, InstanceInfo(), InstanceInfo(), 5 };
const TransitionResult EVICTING_RESULT = TransitionResult{ InstanceState::EXITING, InstanceInfo(), InstanceInfo(), 6 };

InstanceCtrlConfig instanceCtrlConfig {
    .maxInstanceReconnectTimes = 2,
    .maxInstanceRedeployTimes = 2,
    .reconnectTimeout = 1,
    .reconnectInterval = 1,
    .connectTimeout = 1,
    .maxGrpcSize = grpc::DEFAULT_MAX_GRPC_SIZE,
    .redeployTimes = 2,
    .waitStatusCodeUpdateMs = 500,
    .minDeployIntervalMs = 100,
    .maxDeployIntervalMs = 101,
    .maxGetLocalAidTimes = 1,
    .cacheStorageHost = "cacheStorageHost",
    .runtimeConfig =
        {
            .runtimeHeartbeatEnable = "true",
            .runtimeMaxHeartbeatTimeoutTimes = 2,
            .runtimeHeartbeatTimeoutMS = 1000,
            .runtimeInitCallTimeoutMS = 1000,
            .runtimeShutdownTimeoutSeconds = 3,
        },
    .isPseudoDataPlane = false,
    .limitResource =
        {
            .minCpu = DEFAULT_MIN_INSTANCE_CPU_SIZE,
            .minMemory = DEFAULT_MIN_INSTANCE_MEMORY_SIZE,
            .maxCpu = DEFAULT_MAX_INSTANCE_CPU_SIZE,
            .maxMemory = DEFAULT_MAX_INSTANCE_MEMORY_SIZE,
        },
    .enableServerMode = false,
    .enableSSL = false,
    .serverRootCert = "",
    .serverNameOverride = "",
    .posixPort = "30001",
    .schedulePlugins =
        R"(["DefaultFilterPlugin", "DefaultScorePlugin", "DefaultPodFilterPlugin", "DefaultPodScorePlugin",
"ResourceSelectorPlugin", "LocalAffinityFilterPlugin", "LocalAffinityScorePlugin", "DefaultHeterogeneousFilterPlugin",
"DefaultHeterogeneousScorePlugin", "LocalLabelAffinityFilterPlugin", "LocalLabelAffinityScorePlugin"])",
    .enableTenantAffinity = true,
    .createLimitationEnable = true,
    .tokenBucketCapacity = 10
};

static InstanceInfo GenInstanceInfo(const std::string &instanceID, const std::string &funcAgentID,
                                    const std::string &function, InstanceState state)
{
    InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(instanceID);
    instanceInfo.set_functionagentid(funcAgentID);
    instanceInfo.set_function(function);
    instanceInfo.mutable_instancestatus()->set_code(int32_t(state));

    return instanceInfo;
}

TEST(InstanceCtrlMessageTest, GenScheduleResponseCarriesOwningProxyNodeID)
{
    messages::ScheduleRequest request;
    request.set_requestid("schedule-request-1");
    request.set_traceid("trace-1");
    request.mutable_instance()->set_instanceid("instance-1");
    request.mutable_instance()->set_functionproxyid("proxy-owner-a");

    auto response = GenScheduleResponse(StatusCode::SUCCESS, "ready", request);

    EXPECT_EQ(response.instanceid(), "instance-1");
    EXPECT_TRUE(response.has_scheduleresult());
    EXPECT_EQ(response.scheduleresult().nodeid(), "proxy-owner-a");
}

static std::shared_ptr<messages::ScheduleRequest> GenScheduleReq(std::shared_ptr<InstanceCtrlActor> actor)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->mutable_instance()->set_parentid("DesignatedParentID");
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(actor->GetAID());
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_function("default/yrf8440ad184-test-wait/$latest");
    (*(scheduleReq->mutable_instance()->mutable_createoptions()))["ConcurrentNum"] = "2";

    resources::Resource validCPU;
    validCPU.mutable_scalar()->set_value(300);
    resources::Resource validMemory;
    validMemory.mutable_scalar()->set_value(128);
    scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](CPU_RESOURCE_NAME) =
        validCPU;
    scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](MEMORY_RESOURCE_NAME) =
        validMemory;

    return scheduleReq;
}

static resources::InstanceInfo GenParentInstanceInfo(const std::string &parentID, const std::string &proxyAID,
                                                     const std::string &tenantID)
{
    resources::InstanceInfo parentInfo;
    parentInfo.set_instanceid(parentID);
    parentInfo.set_requestid(parentID + "-request");
    parentInfo.set_function("default/parent/$latest");
    parentInfo.set_functionproxyid(ExtractProxyIDFromProxyAID(proxyAID));
    parentInfo.set_parentfunctionproxyaid(proxyAID);
    parentInfo.set_tenantid(tenantID);
    parentInfo.set_issystemfunc(false);
    parentInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    return parentInfo;
}

static std::string GetMetricsFilesName(const std::string &backendName)
{
    return backendName + "-metrics.data";
}

class InstanceCtrlTest : public ::testing::Test {
public:
    void SetUp() override
    {
        sharedClientMgr_ = std::make_shared<SharedClientManager>("SharedPosixClientManager");
        litebus::Spawn(sharedClientMgr_);
        auto sharedPosixClientManager = std::make_shared<PosixStreamManagerProxy>(sharedClientMgr_->GetAID());
        auto metaClient = MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ });
        metaStorageAccessor_ = std::make_shared<MetaStorageAccessor>(metaClient);
        InternalIAM::Param internalIAM_Param;
        internalIAM_Param.isEnableIAM = false;
        observerActor_ = std::make_shared<function_proxy::ObserverActor>(
            FUNCTION_PROXY_OBSERVER_ACTOR_NAME, nodeID_, metaStorageAccessor_, function_proxy::ObserverParam{});
        mockInternalIAM_ = std::make_shared<MockInternalIAM>(internalIAM_Param);
        observerActor_->BindDataInterfaceClientManager(sharedPosixClientManager);
        observerActor_->BindInternalIAM(mockInternalIAM_);
        EXPECT_CALL(*mockInternalIAM_, IsSystemTenant).WillRepeatedly(testing::Return(false));
        litebus::Spawn(observerActor_);

        litebus::Async(observerActor_->GetAID(), &function_proxy::ObserverActor::Register);
        observer_ = std::make_shared<function_proxy::ControlPlaneObserver>(observerActor_);
        funcAgentMgr_ = std::make_shared<MockFunctionAgentMgr>("funcAgentMgr", std::move(metaClient));
        resourceViewMgr_ = std::make_shared<resource_view::ResourceViewMgr>();
        resourceViewMgr_->Init(nodeID_, VIEW_ACTOR_PARAM);
        instanceCtrlConfig.runtimeConfig = runtimeConfig;
        instanceCtrlConfig.schedulePlugins = "[plugin]";
        instanceCtrl_ = InstanceCtrl::Create(nodeID_, instanceCtrlConfig);
        instanceControlView_ = std::make_shared<MockInstanceControlView>("nodeID");
        ON_CALL(*instanceControlView_, DelInstance).WillByDefault(Return(Status::OK()));
        instanceCtrl_->BindInstanceControlView(instanceControlView_);
        instanceCtrl_->Start(funcAgentMgr_, resourceViewMgr_, observer_);
        instanceCtrl_->BindFunctionAgentMgr(funcAgentMgr_);
        instanceCtrl_->BindObserver(observer_);
        instanceCtrl_->BindFunctionAgentMgr(funcAgentMgr_);
        instanceCtrl_->BindResourceView(resourceViewMgr_);
        instanceCtrl_->BindInternalIAM(mockInternalIAM_);
        mockSharedClientManagerProxy_ = std::make_shared<MockSharedClientManagerProxy>();
        instanceCtrl_->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);

        instanceCtrlWithMockObserver_ = InstanceCtrl::Create("nodeID", instanceCtrlConfig);
        mockObserver_ = std::shared_ptr<MockObserver>(new MockObserver(), [](MockObserver *) {});
        ::testing::Mock::AllowLeak(mockObserver_.get());
        instanceCtrlWithMockObserver_->BindInstanceControlView(instanceControlView_);
        instanceCtrlWithMockObserver_->Start(funcAgentMgr_, resourceViewMgr_, mockObserver_);
        instanceCtrlWithMockObserver_->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
        instanceCtrlWithMockObserver_->BindInternalIAM(mockInternalIAM_);
        resource_view::Resources metaResources;
        resource_view::Resource resource;
        resource.set_type(resource_view::ValueType::Value_Type_SCALAR);
        resource.mutable_scalar()->set_value(500);
        metaResources.mutable_resources()->operator[](CPU_RESOURCE_NAME).CopyFrom(resource);
        metaResources.mutable_resources()->operator[](MEMORY_RESOURCE_NAME).CopyFrom(resource);
        functionMeta_ = { WarmupType::NONE, FuncMetaData{}, CodeMetaData{}, EnvMetaData{}, metaResources, ExtendedMetaData{}, InstanceMetaData{}, RootfsSpecMeta{}, "" };

        mockResourceViewMgr_ = std::make_shared<resource_view::ResourceViewMgr>();
        primary_ = MockResourceView::CreateMockResourceView();
        virtual_ = MockResourceView::CreateMockResourceView();
        mockResourceViewMgr_->primary_ = primary_;
        mockResourceViewMgr_->virtual_ = virtual_;
    }

    void TearDown() override
    {
        functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetEnabledInstruments({});
        functionsystem::metrics::MetricsAdapter::GetInstance().ClearEnabledInstruments();
        functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetAttr("component_name", "");
        functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().EraseAlarm(
            "YuanrongInstanceCreateFailure00001-requestID_CreateInstanceClientTest");
        functionsystem::metrics::MetricsAdapter::GetInstance().CleanMetrics();

        // clear meta info
        auto client = MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ });
        ASSERT_TRUE(
            client->Delete(GROUP_PATH_PREFIX, DeleteOption{ .prevKv = false, .prefix = true }).Get()->status.IsOk());
        ASSERT_TRUE(
            client->Delete(INSTANCE_PATH_PREFIX, DeleteOption{ .prevKv = false, .prefix = true }).Get()->status.IsOk());
        ASSERT_TRUE(client->Delete(INSTANCE_ROUTE_PATH_PREFIX, DeleteOption{ .prevKv = false, .prefix = true })
                        .Get()
                        ->status.IsOk());

        litebus::Terminate(observerActor_->GetAID());
        litebus::Await(observerActor_);

        litebus::Terminate(sharedClientMgr_->GetAID());
        litebus::Await(sharedClientMgr_);

        observerActor_ = nullptr;
        sharedClientMgr_ = nullptr;

        instanceCtrl_ = nullptr;
        instanceCtrlWithMockObserver_ = nullptr;

        // Explicitly terminate ResourceView actors to prevent name conflicts in subsequent tests.
        // InstanceCtrlActor/ScheduleQueueActor may hold shared_ptr<ResourceView> refs beyond the
        // lifetime of InstanceCtrl, preventing automatic cleanup through ~ResourceView().
        litebus::Terminate(litebus::AID(nodeID_ + "-ResourceViewActor"));
        litebus::Terminate(litebus::AID(nodeID_ + "-virtualResourceViewActor"));
        litebus::Await(litebus::AID(nodeID_ + "-ResourceViewActor"));
        litebus::Await(litebus::AID(nodeID_ + "-virtualResourceViewActor"));
        resourceViewMgr_ = nullptr;

        metaStorageAccessor_ = nullptr;
        observer_ = nullptr;
        mockObserver_ = nullptr;
        funcAgentMgr_ = nullptr;
        mockInternalIAM_ = nullptr;
        mockResourceViewMgr_ = nullptr;
        primary_ = nullptr;
        virtual_ = nullptr;

        // reset static observer to avoid polluting other test suites
        InstanceStateMachine::UnBindControlPlaneObserver();
    }

protected:
    [[maybe_unused]] static void SetUpTestSuite()
    {
        etcdSrvDriver_ = std::make_unique<meta_store::test::EtcdServiceDriver>();
        int metaStoreServerPort = functionsystem::test::FindAvailablePort();
        metaStoreServerHost_ = "127.0.0.1:" + std::to_string(metaStoreServerPort);
        etcdSrvDriver_->StartServer(metaStoreServerHost_);
        InstanceCtrlActor::SetGetLocalInterval(100);
    }

    [[maybe_unused]] static void TearDownTestSuite()
    {
        etcdSrvDriver_->StopServer();
    }

protected:
    std::string nodeID_ = "nodeN";
    std::shared_ptr<InstanceCtrl> instanceCtrl_;
    std::shared_ptr<InstanceCtrl> instanceCtrlWithMockObserver_;
    std::shared_ptr<resource_view::ResourceViewMgr> resourceViewMgr_;
    std::shared_ptr<resource_view::ResourceViewMgr> mockResourceViewMgr_;
    std::shared_ptr<MockResourceView> primary_;
    std::shared_ptr<MockResourceView> virtual_;
    std::shared_ptr<MockSharedClientManagerProxy> mockSharedClientManagerProxy_;
    std::shared_ptr<MockInternalIAM> mockInternalIAM_;
    std::shared_ptr<SharedClientManager> sharedClientMgr_;
    std::shared_ptr<MetaStorageAccessor> metaStorageAccessor_;
    std::shared_ptr<function_proxy::ControlPlaneObserver> observer_;
    std::shared_ptr<function_proxy::ObserverActor> observerActor_;
    std::shared_ptr<MockInstanceControlView> instanceControlView_;

    std::shared_ptr<MockObserver> mockObserver_;
    std::shared_ptr<MockFunctionAgentMgr> funcAgentMgr_;
    inline static std::unique_ptr<meta_store::test::EtcdServiceDriver> etcdSrvDriver_;
    inline static std::string metaStoreServerHost_;

    FunctionMeta functionMeta_;
};

TEST_F(InstanceCtrlTest, ScheduleGetFuncMetaFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceCtrl = InstanceCtrl(actor);
    auto observer = std::make_shared<MockObserver>();
    instanceCtrl.Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillOnce(Return(litebus::None()));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().message(), "failed to find function meta");
}

TEST(InstanceCtrlReadyCallResultTest, FrontendReadyCallResultCallbackFallsBackToInstanceID)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto scheduleReq = GenScheduleReq(actor);
    scheduleReq->set_requestid("frontend-create-request");
    scheduleReq->mutable_instance()->set_instanceid("frontend-created-instance");

    bool callbackCalled = false;
    std::shared_ptr<functionsystem::CallResult> observedResult;
    actor->RegisterReadyCallResultCallback(
        scheduleReq->instance().instanceid(), scheduleReq,
        [&callbackCalled, &observedResult](const std::shared_ptr<functionsystem::CallResult> &callResult) {
            callbackCalled = true;
            observedResult = callResult;
            return litebus::Future<CallResultAck>(CallResultAck());
        });

    auto runtimeReadyResult = std::make_shared<functionsystem::CallResult>();
    runtimeReadyResult->set_requestid("runtime-ready-request");
    runtimeReadyResult->set_instanceid(scheduleReq->instance().instanceid());
    runtimeReadyResult->set_code(common::ERR_NONE);

    auto ack = actor->SendCallResult(scheduleReq->instance().instanceid(), "", "", runtimeReadyResult);
    ASSERT_AWAIT_READY(ack);
    EXPECT_TRUE(callbackCalled);
    ASSERT_NE(observedResult, nullptr);
    EXPECT_EQ(observedResult->requestid(), "runtime-ready-request");
    EXPECT_EQ(observedResult->instanceid(), "frontend-created-instance");
}

TEST(InstanceCtrlReadyCallResultTest, NestedCreateResultDoesNotConsumeParentFrontendCallback)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto scheduleReq = GenScheduleReq(actor);
    scheduleReq->set_requestid("parent-frontend-create-request");
    scheduleReq->mutable_instance()->set_instanceid("parent-frontend-instance");

    bool callbackCalled = false;
    actor->RegisterReadyCallResultCallback(
        scheduleReq->instance().instanceid(), scheduleReq,
        [&callbackCalled](const std::shared_ptr<functionsystem::CallResult> &) {
            callbackCalled = true;
            return litebus::Future<CallResultAck>(CallResultAck());
        });

    auto nestedReadyResult = std::make_shared<functionsystem::CallResult>();
    nestedReadyResult->set_requestid("nested-create-request");
    nestedReadyResult->set_instanceid(scheduleReq->instance().instanceid());
    nestedReadyResult->set_code(common::ERR_NONE);

    auto callback = actor->TakeFrontendReadyCallback("nested-created-instance", nestedReadyResult);
    EXPECT_EQ(callback, nullptr);
    EXPECT_FALSE(callbackCalled);
    EXPECT_EQ(actor->instanceRegisteredReadyCallResultCallback_.count(scheduleReq->requestid()), 1);
    EXPECT_EQ(actor->instanceReadyCallResultCallbackByInstanceID_.count(scheduleReq->instance().instanceid()), 1);
}

TEST(InstanceCtrlReadyCallResultTest, FrontendReadyResultUsesTicketAsRemoteDestination)
{
    auto source = std::make_shared<InstanceCtrlActor>("FrontendReadySourceProxy", "source-node", instanceCtrlConfig);
    auto target = std::make_shared<InstanceCtrlActor>("FrontendReadyTargetProxy", "target-node", instanceCtrlConfig);
    auto sourceView = std::make_shared<MockInstanceControlView>("source-node");
    source->BindInstanceControlView(sourceView);
    source->BindObserver(std::make_shared<NiceMock<MockObserver>>());

    const std::string requestID = "remote-frontend-create-request";
    const std::string instanceID = "remote-frontend-created-instance";
    auto scheduleReq = GenScheduleReq(source);
    scheduleReq->set_requestid(requestID);
    scheduleReq->mutable_instance()->set_instanceid(instanceID);
    scheduleReq->mutable_instance()->clear_parentid();
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(target->GetAID());
    (*scheduleReq->mutable_instance()->mutable_extensions())[CREATE_SOURCE] = FRONTEND_STR;

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("source-node");
    EXPECT_CALL(*sourceView, GetInstance(instanceID))
        .WillOnce(Return(stateMachine))
        .WillOnce(Return(nullptr));
    EXPECT_CALL(*stateMachine, GetInstanceState).WillOnce(Return(InstanceState::RUNNING));

    std::atomic<bool> targetCallbackCalled = false;
    target->RegisterReadyCallResultCallback(
        instanceID, scheduleReq,
        [&targetCallbackCalled](const std::shared_ptr<functionsystem::CallResult> &) {
            targetCallbackCalled = true;
            return litebus::Future<CallResultAck>(CallResultAck());
        });

    litebus::Spawn(source);
    litebus::Spawn(target);

    auto ready = std::make_shared<functionsystem::CallResult>();
    ready->set_requestid(requestID);
    ready->set_instanceid(instanceID);
    ready->set_code(common::ERR_NONE);
    auto sourceReadyCallback = source->RegisterCreateCallResultCallback(scheduleReq);
    auto ack = sourceReadyCallback(ready);
    EXPECT_AWAIT_READY(ack);
    EXPECT_TRUE(targetCallbackCalled);
    EXPECT_TRUE(scheduleReq->instance().parentid().empty());
    if (ack.IsOK()) {
        EXPECT_EQ(ack.Get().code(), common::ERR_NONE);
    }

    litebus::Terminate(source->GetAID());
    litebus::Terminate(target->GetAID());
    litebus::Await(source);
    litebus::Await(target);
}

TEST(InstanceCtrlReadyCallResultTest, FrontendTicketBindsBeforeReadyAndCompletesExactlyOnce)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto scheduleReq = GenScheduleReq(actor);
    scheduleReq->set_requestid("frontend-ticket-request");
    scheduleReq->mutable_instance()->set_instanceid("frontend-ticket-instance");
    int callbackCount = 0;
    ASSERT_TRUE(actor->RegisterFrontendReadyTicket(
        scheduleReq, [&callbackCount](const std::shared_ptr<functionsystem::CallResult> &) {
            ++callbackCount;
            return litebus::Future<CallResultAck>(CallResultAck());
        }));
    ASSERT_TRUE(actor->BindFrontendReadyTicketInstance(scheduleReq->requestid(),
                                                       scheduleReq->instance().instanceid()));

    auto ready = std::make_shared<functionsystem::CallResult>();
    ready->set_requestid("runtime-request-mismatch");
    ready->set_instanceid(scheduleReq->instance().instanceid());
    ready->set_code(common::ERR_NONE);
    ASSERT_AWAIT_READY(actor->SendCallResult(scheduleReq->instance().instanceid(), "", "", ready));
    EXPECT_EQ(callbackCount, 1);
    ASSERT_AWAIT_READY(actor->SendCallResult(scheduleReq->instance().instanceid(), "", "", ready));
    EXPECT_EQ(callbackCount, 1);
    EXPECT_TRUE(actor->instanceRegisteredReadyCallResultCallback_.empty());
    EXPECT_TRUE(actor->instanceReadyCallResultCallbackByInstanceID_.empty());
}

TEST(InstanceCtrlReadyCallResultTest, CancelledFrontendTicketIgnoresLateReady)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto scheduleReq = GenScheduleReq(actor);
    scheduleReq->set_requestid("cancelled-frontend-ticket");
    scheduleReq->mutable_instance()->set_instanceid("cancelled-frontend-instance");
    int callbackCount = 0;
    ASSERT_TRUE(actor->RegisterFrontendReadyTicket(
        scheduleReq, [&callbackCount](const std::shared_ptr<functionsystem::CallResult> &) {
            ++callbackCount;
            return litebus::Future<CallResultAck>(CallResultAck());
        }));
    ASSERT_TRUE(actor->BindFrontendReadyTicketInstance(scheduleReq->requestid(),
                                                       scheduleReq->instance().instanceid()));
    actor->UnregisterFrontendReadyWait(scheduleReq->requestid(), "client cancelled");

    auto lateReady = std::make_shared<functionsystem::CallResult>();
    lateReady->set_requestid(scheduleReq->requestid());
    lateReady->set_instanceid(scheduleReq->instance().instanceid());
    lateReady->set_code(common::ERR_NONE);
    ASSERT_AWAIT_READY(actor->SendCallResult(scheduleReq->instance().instanceid(), "", "", lateReady));
    EXPECT_EQ(callbackCount, 0);
    EXPECT_TRUE(actor->instanceRegisteredReadyCallResultCallback_.empty());
    EXPECT_TRUE(actor->instanceReadyCallResultCallbackByInstanceID_.empty());
}

TEST(InstanceCtrlReadyCallResultTest, LowReliabilityFailuresStillCompleteFrontendWaiter)
{
    struct FailureCase {
        StatusCode code;
        std::string message;
    };
    const std::vector<FailureCase> failureCases = {
        { StatusCode::RUNTIME_MANAGER_CREATE_EXEC_FAILED, "runtime manager failed to create executor" },
        { StatusCode::ERR_AUTHORIZE_FAILED, "authorization failed" },
        { StatusCode::ERR_INNER_COMMUNICATION, "internal communication failed" },
    };

    for (size_t i = 0; i < failureCases.size(); ++i) {
        const auto &failure = failureCases[i];
        SCOPED_TRACE(failure.message);

        auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
        actor->BindInstanceControlView(std::make_shared<InstanceControlView>("nodeID", false));

        const std::string suffix = std::to_string(i);
        const std::string requestID = "frontend-create-request-" + suffix;
        const std::string instanceID = "frontend-created-instance-" + suffix;
        auto scheduleReq = GenScheduleReq(actor);
        scheduleReq->set_requestid(requestID);
        scheduleReq->mutable_instance()->set_instanceid(instanceID);

        bool callbackCalled = false;
        std::shared_ptr<functionsystem::CallResult> observedResult;
        actor->RegisterReadyCallResultCallback(
            instanceID, scheduleReq,
            [&callbackCalled, &observedResult](const std::shared_ptr<functionsystem::CallResult> &callResult) {
                callbackCalled = true;
                observedResult = callResult;
                return litebus::Future<CallResultAck>(CallResultAck());
            });

        ::internal::ForwardCallResultRequest request;
        request.mutable_readyinstance()->set_instanceid(instanceID);
        request.mutable_readyinstance()->set_lowreliability(true);
        request.mutable_req()->set_requestid(requestID);
        request.mutable_req()->set_instanceid(instanceID);
        request.mutable_req()->set_code(static_cast<common::ErrorCode>(failure.code));
        request.mutable_req()->set_message(failure.message);

        actor->ForwardCallResultRequest(litebus::AID(), "", request.SerializeAsString());

        EXPECT_TRUE(callbackCalled);
        ASSERT_NE(observedResult, nullptr);
        EXPECT_EQ(observedResult->code(), static_cast<common::ErrorCode>(failure.code));
        EXPECT_EQ(observedResult->message(), failure.message);
        EXPECT_TRUE(actor->instanceRegisteredReadyCallResultCallback_.empty());
        EXPECT_TRUE(actor->instanceReadyCallResultCallbackByInstanceID_.empty());
    }
}

TEST(InstanceCtrlReadyCallResultTest, LowReliabilitySuccessKeepsStaleInstanceProtection)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindInstanceControlView(std::make_shared<InstanceControlView>("nodeID", false));

    const std::string requestID = "frontend-create-request";
    const std::string instanceID = "frontend-created-instance";
    auto scheduleReq = GenScheduleReq(actor);
    scheduleReq->set_requestid(requestID);
    scheduleReq->mutable_instance()->set_instanceid(instanceID);

    bool callbackCalled = false;
    actor->RegisterReadyCallResultCallback(
        instanceID, scheduleReq,
        [&callbackCalled](const std::shared_ptr<functionsystem::CallResult> &) {
            callbackCalled = true;
            return litebus::Future<CallResultAck>(CallResultAck());
        });

    ::internal::ForwardCallResultRequest request;
    request.mutable_readyinstance()->set_instanceid(instanceID);
    request.mutable_readyinstance()->set_lowreliability(true);
    request.mutable_req()->set_requestid(requestID);
    request.mutable_req()->set_instanceid(instanceID);
    request.mutable_req()->set_code(common::ERR_NONE);

    actor->ForwardCallResultRequest(litebus::AID(), "", request.SerializeAsString());

    EXPECT_FALSE(callbackCalled);
    actor->UnregisterFrontendReadyWait(requestID, "test cleanup");
}

TEST(InstanceCtrlReadyCallResultTest, ResolvedWinnerRelayMarkerBypassesStaleProtection)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindInstanceControlView(std::make_shared<InstanceControlView>("nodeID", false));

    const std::string requestID = "resolved-winner-request";
    const std::string instanceID = "resolved-winner-instance";
    auto scheduleReq = GenScheduleReq(actor);
    scheduleReq->set_requestid(requestID);
    scheduleReq->mutable_instance()->set_instanceid(instanceID);

    bool callbackCalled = false;
    std::shared_ptr<functionsystem::CallResult> observedResult;
    actor->RegisterReadyCallResultCallback(
        instanceID, scheduleReq,
        [&callbackCalled, &observedResult](const std::shared_ptr<functionsystem::CallResult> &callResult) {
            callbackCalled = true;
            observedResult = callResult;
            return litebus::Future<CallResultAck>(CallResultAck());
        });

    ::internal::ForwardCallResultRequest request;
    request.set_instanceid(instanceID);
    request.mutable_req()->set_requestid(requestID);
    request.mutable_req()->set_instanceid(instanceID);
    request.mutable_req()->set_code(common::ERR_NONE);
    request.mutable_req()->mutable_runtimeinfo()->set_route("tcp://winner-route");
    request.mutable_req()->mutable_runtimeinfo()->set_proxyid("winner-proxy");
    auto readyInstance = request.mutable_readyinstance();
    readyInstance->set_instanceid(instanceID);
    (*readyInstance->mutable_extensions())["createConflictArbitratedContender"] = instanceID;

    actor->ForwardCallResultRequest(litebus::AID(), "", request.SerializeAsString());

    EXPECT_TRUE(callbackCalled);
    ASSERT_NE(observedResult, nullptr);
    EXPECT_EQ(observedResult->runtimeinfo().route(), "tcp://winner-route");
    EXPECT_EQ(observedResult->runtimeinfo().proxyid(), "winner-proxy");
    EXPECT_TRUE(actor->instanceRegisteredReadyCallResultCallback_.empty());
    EXPECT_TRUE(actor->instanceReadyCallResultCallbackByInstanceID_.empty());
}

TEST_F(InstanceCtrlTest, ScheduleUpdateInstanceInfoFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto instanceCtrl = InstanceCtrl(actor);
    auto observer = std::make_shared<MockObserver>();
    instanceCtrl.Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));
    EXPECT_CALL(*observer, GetFuncMeta).WillOnce(Return(functionMeta_));

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    GeneratedInstanceStates genStates{ "DesignatedInstanceID", InstanceState::NEW, false };
    EXPECT_CALL(*instanceControlView, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView, GetInstance).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl(_, _, _, _, _)).WillOnce(Return(NONE_RESULT));
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(mockStateMachine, UpdateInstanceInfo).WillRepeatedly(Return());
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().message().find("failed to update instance info") != std::string::npos, true);
}

TEST_F(InstanceCtrlTest, ScheduleInvalidRequest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceCtrl = InstanceCtrl(actor);
    auto observer = std::make_shared<MockObserver>();
    instanceCtrl.Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(FunctionMeta{}));

    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    resources::Resource invalidCPU;
    invalidCPU.mutable_scalar()->set_value(100);
    resources::Resource validCPU;
    validCPU.mutable_scalar()->set_value(300);
    resources::Resource invalidMemory;
    invalidMemory.mutable_scalar()->set_value(100);
    resources::Resource validMemory;
    validMemory.mutable_scalar()->set_value(128);

    scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](CPU_RESOURCE_NAME) =
        invalidCPU;
    scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](MEMORY_RESOURCE_NAME) =
        validMemory;
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().message(), "resources is invalid");

    {
        auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
        scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](CPU_RESOURCE_NAME) =
            validCPU;
        scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](MEMORY_RESOURCE_NAME) =
            invalidMemory;
        runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
        result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
        ASSERT_AWAIT_READY(result);
        EXPECT_EQ(result.Get().message(), "resources is invalid");
    }

    // a invalid request -- the count of device card is 0
    {
        auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
        (*scheduleReq->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(0);
        runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
        result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
        ASSERT_AWAIT_READY(result);
        EXPECT_EQ(result.Get().message(), "resources is invalid");
    }

    // a invalid request -- hbm/latency/stream : 0
    {
        auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
        (*scheduleReq->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(0, 0, 0);
        runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
        result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
        ASSERT_AWAIT_READY(result);
        EXPECT_EQ(result.Get().message(), "resources is invalid");
    }

    // a invalid request -- invalid card type regex
    {
        auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
        (*scheduleReq->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(10, 10, 10, "NPU/(Ascend910");
        auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
        auto result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
        ASSERT_AWAIT_READY(result);
        EXPECT_EQ(result.Get().message(), "resources is invalid");
    }
}

TEST_F(InstanceCtrlTest, ScheduleValidDiskRequest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceCtrl = InstanceCtrl(actor);
    auto observer = std::make_shared<MockObserver>();
    instanceCtrl.Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(FunctionMeta{}));

    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    (*scheduleReq->mutable_instance()) = view_utils::Get1DInstance();
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();

    // a valid request -- disk zise is +100

    resources::Resource validDisk;
    validDisk.mutable_scalar()->set_value(100);
    scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](DISK_RESOURCE_NAME) =
        validDisk;
    runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().message(), "resources is invalid");
}

TEST_F(InstanceCtrlTest, ScheduleInvalidDiskRequest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceCtrl = InstanceCtrl(actor);
    auto observer = std::make_shared<MockObserver>();
    instanceCtrl.Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(FunctionMeta{}));

    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    (*scheduleReq->mutable_instance()) = view_utils::Get1DInstance();
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();

    // a invalid request -- disk zise is 0
    {
        resources::Resource invalidDisk;
        invalidDisk.mutable_scalar()->set_value(0);
        scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](DISK_RESOURCE_NAME) =
            invalidDisk;
        runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
        auto result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
        ASSERT_AWAIT_READY(result);
        EXPECT_EQ(result.Get().message(), "resources is invalid");
    }
    // a invalid request -- disk zise is -100
    {
        resources::Resource invalidDisk;
        invalidDisk.mutable_scalar()->set_value(-100);
        scheduleReq->mutable_instance()->mutable_resources()->mutable_resources()->operator[](DISK_RESOURCE_NAME) =
            invalidDisk;
        runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
        auto result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
        ASSERT_AWAIT_READY(result);
        EXPECT_EQ(result.Get().message(), "resources is invalid");
    }
}

TEST_F(InstanceCtrlTest, ScheduleExistInstance)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto instanceCtrl = InstanceCtrl(actor);
    auto o = std::make_shared<MockObserver>();
    instanceCtrl.Start(nullptr, mockResourceViewMgr_, o);
    ASSERT_TRUE(o != nullptr);

    EXPECT_CALL(*o, GetFuncMeta).WillRepeatedly(Return(FunctionMeta{}));
    EXPECT_CALL(*o, IsSystemFunction).WillRepeatedly(Return(false));

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView, GetInstance).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetOwner()).WillRepeatedly(Return("nodeID"));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("123");
    scheduleReq->mutable_instance()->set_parentid("1234");
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();

    EXPECT_CALL(mockStateMachine, GetInstanceState()).WillOnce(Return(InstanceState::EXITING));
    auto result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), static_cast<int32_t>(StatusCode::ERR_INSTANCE_EXITED));
    EXPECT_THAT(result.Get().message(), HasSubstr("you are not allowed to create instance because of you are exiting"));

    resources::InstanceInfo parentIns;
    parentIns.set_functionproxyid("nodeID");
    parentIns.set_function("0/0-system-faasfrontend/$latest");
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillOnce(Return(parentIns));

    EXPECT_CALL(mockStateMachine, GetInstanceState())
        .WillOnce(Return(InstanceState::RUNNING))
        .WillOnce(Return(InstanceState::RUNNING));
    runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), static_cast<int32_t>(StatusCode::ERR_INSTANCE_DUPLICATED));
    EXPECT_THAT(result.Get().message(), HasSubstr("you are not allowed to create instance with the same instance id"));
    EXPECT_TRUE(scheduleReq->instance().extensions().find("source") != scheduleReq->instance().extensions().end());
    scheduleReq->mutable_instance()->mutable_extensions()->clear();

    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillOnce(Return(parentIns));
    EXPECT_CALL(mockStateMachine, AddStateChangeCallback).WillOnce(Return());
    EXPECT_CALL(mockStateMachine, GetInstanceState())
        .WillOnce(Return(InstanceState::CREATING))
        .WillOnce(Return(InstanceState::CREATING))
        .WillOnce(Return(InstanceState::CREATING));
    // D-6 F3: a schedule for a CREATING machine now rides the machine's own
    // pending request future (no more unconditional code:0 "already been
    // scheduling" which produced phantom restores); inject one and complete
    // it with SUCCESS.
    EXPECT_CALL(mockStateMachine, GetRequestID()).WillRepeatedly(Return("creating-req"));
    auto creatingPromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    instanceControlView->InsertRequestFuture("creating-req", creatingPromise->GetFuture(), creatingPromise);
    runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
    creatingPromise->SetValue(GenScheduleResponse(StatusCode::SUCCESS, "creating", *scheduleReq));
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), static_cast<int32_t>(StatusCode::SUCCESS));

    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillOnce(Return(parentIns));
    EXPECT_CALL(mockStateMachine, GetInstanceState())
        .WillOnce(Return(InstanceState::RUNNING))
        .WillOnce(Return(InstanceState::EXITING));

    runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), static_cast<int32_t>(StatusCode::ERR_INSTANCE_EXITED));
    EXPECT_THAT(result.Get().message(), HasSubstr("you are not allowed to create instance with the same instance id of "
                                                  "an failed instance, please kill first"));
}

TEST_F(InstanceCtrlTest, DeployInstanceRetry)
{
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewControlInterfacePosixClient(_, _, _, _, _, _))
        .WillRepeatedly(Return(mockSharedClient));
    litebus::Promise<Status> promise;
    promise.SetFailed(StatusCode::FAILED);
    EXPECT_CALL(*mockSharedClient, Readiness).WillRepeatedly(Return(promise.GetFuture()));
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor1", "nodeID", instanceCtrlConfig);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    instanceCtrl->BindInternalIAM(mockInternalIAM_);
    litebus::Future<std::string> agentIdFut;
    {
        InSequence s;
        EXPECT_CALL(*mockSharedClient, Readiness)
            .Times(instanceCtrlConfig.maxInstanceRedeployTimes)
            .WillRepeatedly(Return(promise.GetFuture()));
        EXPECT_CALL(*mockSharedClient, Readiness).WillOnce(Invoke([promise, agentIdFut]() {
            agentIdFut.SetValue("888");
            return promise.GetFuture();
        }));
    }
    agentIdFut.OnComplete([instanceCtrl]() {
        instanceCtrl->UpdateInstanceStatusPromise("DesignatedInstanceID",
                                                  "runtimeExit info uploaded by runtimeManager");
    });
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_));
    resources::InstanceInfo instanceInfo;
    instanceInfo.set_functionproxyid("nodeID");
    instanceInfo.set_parentid("parent");
    EXPECT_CALL(*observer, PutInstance).WillRepeatedly(Return(Status::OK()));

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    auto parentStateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto parentInfo = GenParentInstanceInfo("parent", actor->GetAID(), "tenant001");
    EXPECT_CALL(*parentStateMachine, GetInstanceInfo).WillRepeatedly(Return(parentInfo));
    int selfLookupTimes = 0;

    GeneratedInstanceStates genStates{ "DesignatedInstanceID", InstanceState::NEW, false };
    EXPECT_CALL(*instanceControlView, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView, GetInstance(testing::_))
        .WillRepeatedly(Invoke([stateMachine, parentStateMachine, &selfLookupTimes](const std::string &instanceID) {
            if (instanceID == "parent") {
                return std::static_pointer_cast<InstanceStateMachine>(parentStateMachine);
            }
            if (instanceID == "DesignatedInstanceID") {
                return selfLookupTimes++ == 0 ? std::shared_ptr<InstanceStateMachine>(nullptr)
                                              : std::static_pointer_cast<InstanceStateMachine>(stateMachine);
            }
            return std::static_pointer_cast<InstanceStateMachine>(stateMachine);
        }));
    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl)
        .WillOnce(Return(SCHEDULING_RESULT))
        .WillOnce(Return(CREATING_RESULT))
        .WillOnce(Return(CREATING_RESULT));
    EXPECT_CALL(mockStateMachine, GetInstanceState).WillOnce(Return(InstanceState::NEW))
        .WillOnce(Return(InstanceState::NEW))
        .WillRepeatedly(Return(InstanceState::CREATING));
    EXPECT_CALL(mockStateMachine, AddStateChangeCallback).WillRepeatedly(Return());
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));
    EXPECT_CALL(mockStateMachine, GetRuntimeID).WillRepeatedly(Return(""));
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));
    instanceCtrl->BindObserver(observer);

    auto scheduler = std::make_shared<MockScheduler>();
    auto failedAllocated = std::make_shared<litebus::Promise<Status>>();
    failedAllocated->SetValue(Status(StatusCode::FAILED));
    EXPECT_CALL(*scheduler, ScheduleDecision(_))
        .WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, "", {}, "", {}, {}, failedAllocated}))
        .WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, "" }));
    EXPECT_CALL(*scheduler, ScheduleConfirm).WillOnce(Return(Status::OK())).WillOnce(Return(Status::OK()));
    instanceCtrl->BindScheduler(scheduler);

    auto metaClient = MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ });
    auto functionAgentMgr = std::make_shared<MockFunctionAgentMgr>("funcAgentMgr", metaClient);
    messages::DeployInstanceResponse deployInstanceResponse;
    deployInstanceResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*functionAgentMgr, DeployInstance(_, _)).WillRepeatedly(Return(deployInstanceResponse));

    messages::KillInstanceResponse killInstanceRsp;
    killInstanceRsp.set_code(int32_t(common::ErrorCode::ERR_NONE));
    litebus::Future<std::shared_ptr<messages::KillInstanceRequest>> killRequestFuture;
    EXPECT_CALL(*functionAgentMgr, KillInstance(testing::_, testing::_, testing::_))
        .WillOnce(testing::DoAll(test::FutureArg<0>(&killRequestFuture), test::Return(killInstanceRsp)))
        .WillRepeatedly(Return(killInstanceRsp));
    instanceCtrl->BindFunctionAgentMgr(functionAgentMgr);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_traceid("trace-retry-deploy-instance-unit-test");
    scheduleReq->set_requestid("request-retry-deploy-instance-unit-test");
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(actor->GetAID());
    scheduleReq->mutable_instance()->mutable_scheduleoption()->set_schedpolicyname("monopoly");
    scheduleReq->mutable_instance()->set_parentid("parent");
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&request) -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(request);
            return runtime::NotifyResponse();
        }));
    instanceCtrl->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), (int32_t)StatusCode::SUCCESS);
    ASSERT_AWAIT_READY_FOR(notifyCalled.GetFuture(), 30000);
    EXPECT_EQ(static_cast<StatusCode>(notifyCalled.GetFuture().Get().code()),
              StatusCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
    ASSERT_AWAIT_READY(killRequestFuture);
    EXPECT_EQ(killRequestFuture.Get()->ismonopoly(), false);
}

TEST_F(InstanceCtrlTest, ScheduleCancelAfterScheduling)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto instanceCtrl = InstanceCtrl(actor);
    auto observer = std::make_shared<MockObserver>();
    instanceCtrl.Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillOnce(Return(functionMeta_));

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    GeneratedInstanceStates genStates{ "DesignatedInstanceID", InstanceState::NEW, false };
    EXPECT_CALL(*instanceControlView, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView, GetInstance).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockStateMachine, GetInstanceState())
        .WillOnce(Return(InstanceState::SCHEDULING))
        .WillOnce(Return(InstanceState::SCHEDULING));
    EXPECT_CALL(mockStateMachine, TransitionToImpl).WillOnce(Return(NEW_RESULT)).WillOnce(Return(FAILED_RESULT));
    auto cancelFuture = litebus::Future<std::string>();
    cancelFuture.SetValue("cancel");
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillOnce(Return(cancelFuture));

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_))
        .WillOnce(Return(ScheduleResult{ "", StatusCode::RESOURCE_NOT_ENOUGH, "" }));
    instanceCtrl.BindScheduler(scheduler);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), (int32_t)StatusCode::RESOURCE_NOT_ENOUGH);
}

TEST_F(InstanceCtrlTest, ScheduleCancelAfterCreating)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    auto observer = std::make_shared<MockObserver>();
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillOnce(Return(functionMeta_));
    instanceCtrl->BindInternalIAM(mockInternalIAM_);
    instanceCtrl->BindFunctionAgentMgr(funcAgentMgr_);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    GeneratedInstanceStates genStates{ "DesignatedInstanceID", InstanceState::NEW, false };
    EXPECT_CALL(*instanceControlView, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView, GetInstance).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(GenScheduleReq(actor)));
    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl)
        .WillOnce(Return(NEW_RESULT))
        .WillOnce(Return(CREATING_RESULT))
        .WillRepeatedly(Return(FATAL_RESULT));
    auto cancelFuture = litebus::Future<std::string>();
    cancelFuture.SetValue("cancel");
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(cancelFuture));

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_)).WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, ""}));
    instanceCtrl->BindScheduler(scheduler);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), 0);
}

/**
 * CreateInstanceFailedForResourceNotEnough
 * Test Create instance, while resource not enough
 * Steps:
 * 1. MockObserver (GetFuncMeta() => defaultMeta / IsSystemFunction() => False)
 * 2. MockScheduler (ScheduleDecision => RESOURCE_NOT_ENOUGH)
 * 3. MockLocalSchedSrv (ForwardSchedule => RESOURCE_NOT_ENOUGH)
 * 4. MockSharedClient (NotifyResult => capture NotifyRequest)
 *
 * Expectations:
 * 1. instance state in scheduleReq and stateMachine == SCHEDULE_FAILED
 * 2. notifyCalled code == ERR_RESOURCE_NOT_ENOUGH
 */
TEST_F(InstanceCtrlTest, CreateInstanceFailedForResourceNotEnough)
{
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));

    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActorTest", "nodeID", instanceCtrlConfig);
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&request) -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(request);
            return runtime::NotifyResponse();
        })); // for mock SendNotifyResult，capture the NotifyRequestResult

    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);

    auto instanceControlView = std::make_shared<InstanceControlView>("nodeID", false);
    actor->BindInstanceControlView(instanceControlView);

    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);

    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_)); // mock get function successfully
    instanceCtrl->BindInternalIAM(mockInternalIAM_); // mock iam successfully

    auto scheduler = std::make_shared<MockScheduler>();
    // mock schedule failed
    EXPECT_CALL(*scheduler, ScheduleDecision(_))
        .WillOnce(Return(ScheduleResult{ "", StatusCode::RESOURCE_NOT_ENOUGH, "" }));
    instanceCtrl->BindScheduler(scheduler);

    auto metaClient = MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ });
    instanceControlView->BindMetaStoreClient(metaClient);
    instanceControlView->GenerateStateMachine(
        "DesignatedParentID", GenParentInstanceInfo("DesignatedParentID", actor->GetAID(), "tenant-parent"));

    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    messages::ScheduleResponse scheduleResponse;
    scheduleResponse.set_requestid("requestID");
    scheduleResponse.set_message("FAILED");
    scheduleResponse.set_code(StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_CALL(*localSchedSrv, ForwardSchedule).WillRepeatedly(Return(scheduleResponse));
    instanceCtrl->BindLocalSchedSrv(localSchedSrv);

    auto scheduleReq = GenScheduleReq(actor);
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), (int32_t)StatusCode::RESOURCE_NOT_ENOUGH);

    ASSERT_AWAIT_TRUE([&]() {
        return scheduleReq->instance().instancestatus().code() == static_cast<int32_t>(InstanceState::SCHEDULE_FAILED);
    });
    auto machine = instanceControlView->GetInstance("DesignatedInstanceID");
    ASSERT_AWAIT_TRUE([&]() { return machine->GetInstanceState() == InstanceState::SCHEDULE_FAILED; });

    ASSERT_AWAIT_READY(notifyCalled.GetFuture());
    EXPECT_EQ(static_cast<StatusCode>(notifyCalled.GetFuture().Get().code()), StatusCode::ERR_RESOURCE_NOT_ENOUGH);
}

/**
 * CreateInstanceFailedForDeployInstanceFailed
 * Test Create instance while instance deploy failed
 * Steps:
 * 1. MockObserver (GetFuncMeta() => defaultMeta / IsSystemFunction() => False)
 * 2. MockScheduler (ScheduleDecision => SUCCESS)
 * 3. MockFunctionAgentMgr (DeployInstance => LS_DEPLOY_INSTANCE_FAILED)
 * 4. MockSharedClient (NotifyResult => capture NotifyRequest)
 *
 * Expectations:
 * 1. instance state in scheduleReq == CREATING
 * 2  instance state in stateMachine == FATAL
 * 3. notifyCalled code == ERR_INNER_SYSTEM_ERROR
 */
TEST_F(InstanceCtrlTest, CreateInstanceFailedForDeployInstanceFailed)
{
    functionsystem::metrics::MetricsAdapter::GetInstance().InitMetricsFromJson(
        nlohmann::json::parse(InstanceCreateFailureAlarmExporterJsonStr), GetMetricsFilesName, {});
    functionsystem::metrics::MetricsAdapter::GetInstance().SetContextAttr("component_name", "function_proxy");

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActorTest", "nodeID", instanceCtrlConfig);
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));

    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&request) -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(request);
            return runtime::NotifyResponse();
        }));


    auto instanceControlView = std::make_shared<InstanceControlView>("nodeID", false);
    actor->BindInstanceControlView(instanceControlView);

    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    EXPECT_CALL(*mockSharedClient, Heartbeat).WillRepeatedly(Return(Status::OK()));
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_));
    instanceCtrl->BindInternalIAM(mockInternalIAM_);

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_)).WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, "" }));
    EXPECT_CALL(*scheduler, ScheduleConfirm).Times(1);
    instanceCtrl->BindScheduler(scheduler);

    auto metaClient = MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ });
    instanceControlView->BindMetaStoreClient(metaClient);
    instanceControlView->GenerateStateMachine(
        "DesignatedParentID", GenParentInstanceInfo("DesignatedParentID", actor->GetAID(), "tenant-parent"));

    auto functionAgentMgr = std::make_shared<MockFunctionAgentMgr>("funcAgentMgr", metaClient);
    messages::DeployInstanceResponse deployInstanceResponse;
    deployInstanceResponse.set_code(StatusCode::LS_DEPLOY_INSTANCE_FAILED);
    EXPECT_CALL(*functionAgentMgr, DeployInstance).WillOnce(Return(deployInstanceResponse));
    instanceCtrl->BindFunctionAgentMgr(functionAgentMgr);


    auto scheduleReq = GenScheduleReq(actor);
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);

    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), (int32_t)StatusCode::SUCCESS);
    ASSERT_AWAIT_READY(notifyCalled.GetFuture());
    EXPECT_EQ(static_cast<StatusCode>(notifyCalled.GetFuture().Get().code()), StatusCode::ERR_INNER_COMMUNICATION);
    auto selector = scheduleReq->mutable_instance()->mutable_scheduleoption()->mutable_resourceselector();
    EXPECT_EQ(selector->find(RESOURCE_OWNER_KEY) != selector->end(), true);
    ASSERT_AWAIT_TRUE([&]() {
        return scheduleReq->instance().instancestatus().code() == static_cast<int32_t>(InstanceState::CREATING);
    });
    auto machine = instanceControlView->GetInstance("DesignatedInstanceID");
    ASSERT_AWAIT_TRUE([&]() { return machine->GetInstanceState() == InstanceState::FATAL; });

    auto alarmInfoMap = functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().GetAlarmInfoMap();
    ASSERT_TRUE(alarmInfoMap.find("YuanrongInstanceCreateFailure00001-requestID") != alarmInfoMap.end());
    const auto &alarmInfo = alarmInfoMap.at("YuanrongInstanceCreateFailure00001-requestID");
    EXPECT_EQ(alarmInfo.alarmName, "yr_instance_create_failure_alarm");
    EXPECT_EQ(std::get<std::string>(alarmInfo.customOptions.at("stage")), "deploy");
    EXPECT_EQ(std::get<std::string>(alarmInfo.customOptions.at("resource_id")), "DesignatedInstanceID");
    EXPECT_EQ(std::get<std::string>(alarmInfo.customOptions.at("request_id")), "requestID");
}

/**
 * CreateInstanceFailedForInitRuntimeFailed
 * Test Create instance while runtime init failed
 * Steps:
 * 1. MockObserver (GetFuncMeta() => defaultMeta / IsSystemFunction() => False)
 * 2. MockScheduler (ScheduleDecision => SUCCESS)
 * 3. MockFunctionAgentMgr (DeployInstance => SUCCESS)
 * 4. MockSharedClient (initCall => ERR_REQUEST_BETWEEN_RUNTIME_BUS)
 * 5. MockSharedClient (NotifyResult => capture NotifyRequest)
 *
 * Expectations:
 * 1. instance state in scheduleReq == FATAL
 * 2  instance state in stateMachine == FATAL
 * 3. notifyCalled code == ERR_REQUEST_BETWEEN_RUNTIME_BUS
 */
TEST_F(InstanceCtrlTest, CreateInstanceFailedForInitRuntimeFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActorTest", "nodeID", instanceCtrlConfig);

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewControlInterfacePosixClient(_, _, _, _, _, _))
        .WillOnce(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));

    EXPECT_CALL(*mockSharedClient, Readiness).WillOnce(Return(Status::OK()));
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&request) -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(request);
            return runtime::NotifyResponse();
        })); // for mock SendNotifyResult

    EXPECT_CALL(*mockSharedClient, Heartbeat).WillRepeatedly(Return(Status::OK())); // mock hearbeat
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);

    auto instanceControlView = std::make_shared<InstanceControlView>("nodeID", false);
    actor->BindInstanceControlView(instanceControlView);

    auto observer = std::make_shared<MockObserver>();

    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_)); // mock get function successfully

    instanceCtrl->BindInternalIAM(mockInternalIAM_); // mock iam successfully

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_)).WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, "" }));
    EXPECT_CALL(*scheduler, ScheduleConfirm).Times(1); // mock schedule successfully
    instanceCtrl->BindScheduler(scheduler);

    auto metaClient = MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ });

    instanceControlView->BindMetaStoreClient(metaClient);
    instanceControlView->GenerateStateMachine(
        "DesignatedParentID", GenParentInstanceInfo("DesignatedParentID", actor->GetAID(), "tenant-parent"));

    auto functionAgentMgr = std::make_shared<MockFunctionAgentMgr>("funcAgentMgr", metaClient);
    messages::DeployInstanceResponse deployInstanceResponse;
    deployInstanceResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*functionAgentMgr, DeployInstance).WillOnce(Return(deployInstanceResponse));
    instanceCtrl->BindFunctionAgentMgr(functionAgentMgr);

    litebus::Future<runtime::CallResponse> sendRet;
    runtime::CallResponse callRsp;
    callRsp.set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
    std::string runtimeErrMsg = "call runtime failed! client may already closed";
    auto expectMsg = runtimeErrMsg + " on nodeID";
    callRsp.set_message(runtimeErrMsg);
    sendRet.SetValue(callRsp);
    EXPECT_CALL(*mockSharedClient, InitCallWrapper).WillOnce(Return(sendRet));

    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    messages::KillInstanceResponse killInstanceRsp;
    killInstanceRsp.set_code(int32_t(common::ErrorCode::ERR_NONE));
    EXPECT_CALL(*functionAgentMgr, KillInstance).WillRepeatedly(Return(killInstanceRsp));

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto scheduleReq = GenScheduleReq(actor);

    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), 0);
    EXPECT_EQ(runtimePromise->GetFuture().Get().code(), 0);

    ASSERT_AWAIT_READY(notifyCalled.GetFuture());
    EXPECT_EQ(static_cast<StatusCode>(notifyCalled.GetFuture().Get().code()), ERR_REQUEST_BETWEEN_RUNTIME_BUS);
    EXPECT_EQ(notifyCalled.GetFuture().Get().message(), expectMsg);

    ASSERT_AWAIT_TRUE([&]() { return scheduleReq->instance().instancestatus().code() == static_cast<int32_t>(InstanceState::FATAL); });
    auto machine = instanceControlView->GetInstance("DesignatedInstanceID");
    ASSERT_AWAIT_TRUE([&]() { return machine->GetInstanceState() == InstanceState::FATAL; });
    ::testing::Mock::VerifyAndClearExpectations(observer.get());
    ::testing::Mock::AllowLeak(observer.get());
    instanceCtrl->Stop();
    instanceCtrl->Await();
    InstanceStateMachine::UnBindControlPlaneObserver();
    instanceCtrl.reset();
    observer.reset();
}

/**
 * CreateInstanceSuccess
 * Test Create instance successfully
 * Steps:
 * 1. MockObserver (GetFuncMeta() => defaultMeta / IsSystemFunction() => False)
 * 2. MockScheduler (ScheduleDecision => SUCCESS)
 * 3. MockFunctionAgentMgr (DeployInstance => SUCCESS)
 * 4. MockSharedClient (initCall => SUCCESS)
 * 5. MockSharedClient (Checkpoint => ERR_NONE)
 * 6. MockDistributedCacheClient (Init => Success) need start actor
 * 7. MockSharedClient (NotifyResult => capture NotifyRequest)
 *
 * Expectations:
 * 1. instance state in scheduleReq == RUNNING
 * 2  instance state in stateMachine == RUNNING
 * 3. notifyCalled code == SUCCESS
 */
TEST_F(InstanceCtrlTest, CreateInstanceSuccess)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActorTest", "nodeID", instanceCtrlConfig);

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewControlInterfacePosixClient(_, _, _, _, _, _))
        .WillOnce(Return(mockSharedClient));

    EXPECT_CALL(*mockSharedClient, Readiness).WillOnce(Return(Status::OK()));
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&request) -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(request);
            return runtime::NotifyResponse();
        })); // for mock SendNotifyResult

    EXPECT_CALL(*mockSharedClient, Heartbeat).WillRepeatedly(Return(Status::OK())); // mock hearbeat
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);

    auto instanceControlView = std::make_shared<InstanceControlView>("nodeID", false);
    actor->BindInstanceControlView(instanceControlView);

    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);

    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_)); // mock get function successfully
    instanceCtrl->BindInternalIAM(mockInternalIAM_); // mock iam successfully

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_))
        .WillOnce(Return(ScheduleResult{ "agent", StatusCode::SUCCESS, "", {}, "", {}, {}, nullptr, "bundleUnit" }));
    EXPECT_CALL(*scheduler, ScheduleConfirm).Times(1); // mock schedule successfully
    instanceCtrl->BindScheduler(scheduler);

    auto metaClient = MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ });
    instanceControlView->BindMetaStoreClient(metaClient);
    auto parentInfo = GenParentInstanceInfo("DesignatedParentID", actor->GetAID(), "tenant001");
    instanceControlView->GenerateStateMachine("DesignatedParentID", parentInfo);

    auto functionAgentMgr = std::make_shared<MockFunctionAgentMgr>("funcAgentMgr", metaClient);
    messages::DeployInstanceResponse deployInstanceResponse;
    deployInstanceResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*functionAgentMgr, DeployInstance).WillOnce(Return(deployInstanceResponse));
    instanceCtrl->BindFunctionAgentMgr(functionAgentMgr);

    litebus::Future<runtime::CallResponse> sendRet;
    runtime::CallResponse response;
    sendRet.SetValue(response);
    litebus::Future<runtime::CallRequest> call;
    EXPECT_CALL(*mockSharedClient, InitCallWrapper).WillRepeatedly(DoAll(FutureArg<0>(&call), Return(sendRet)));
    call.OnComplete([instanceCtrl]() {
        auto callResult = std::make_shared<functionsystem::CallResult>();
        instanceCtrl->CallResult("DesignatedInstanceID", callResult);
    });

    runtime::CheckpointResponse checkpointRsp;
    checkpointRsp.set_code(common::ErrorCode::ERR_NONE);
    checkpointRsp.set_state("state");
    EXPECT_CALL(*mockSharedClient, Checkpoint).WillOnce(Return(checkpointRsp));

    auto distributedCacheClient = std::make_shared<MockDistributedCacheClient>();
    EXPECT_CALL(*distributedCacheClient, Init).WillOnce(Return(Status::OK()));

    auto stateClient = std::make_shared<StateClient>(distributedCacheClient);
    auto stateActor = std::make_shared<function_proxy::StateActor>(stateClient);
    litebus::Spawn(stateActor);
    function_proxy::StateHandler::BindStateActor(stateActor);

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto scheduleReq = GenScheduleReq(actor);
    (*scheduleReq->mutable_instance()->mutable_createoptions())[RECOVER_RETRY_TIMES_KEY] = "1";

    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), 0);
    EXPECT_EQ(runtimePromise->GetFuture().Get().code(), 0);
    EXPECT_EQ(call.Get().createoptions().size(), (size_t)2);
    ASSERT_AWAIT_READY(notifyCalled.GetFuture());
    EXPECT_EQ(static_cast<StatusCode>(notifyCalled.GetFuture().Get().code()), StatusCode::SUCCESS);
    ASSERT_AWAIT_TRUE([&]() { return scheduleReq->instance().instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING); });
    auto machine = instanceControlView->GetInstance("DesignatedInstanceID");
    ASSERT_AWAIT_TRUE([&]() { return machine->GetInstanceState() == InstanceState::RUNNING; });
    ASSERT_AWAIT_TRUE([&]() { return scheduleReq->instance().unitid() == "bundleUnit"; });
    ASSERT_AWAIT_TRUE([&]() { return machine->GetInstanceInfo().unitid() == "bundleUnit"; });
    EXPECT_TRUE(mockInternalIAM_->VerifyInstance("DesignatedInstanceID"));

    litebus::Terminate(stateActor->GetAID());
    litebus::Await(stateActor->GetAID());
}

TEST_F(InstanceCtrlTest, AuthorizeCreateShouldNotOverrideNormalizedTenantID)
{
    InternalIAM::Param internalIAMParam;
    internalIAMParam.isEnableIAM = true;
    auto mockInternalIAM = std::make_shared<MockInternalIAM>(internalIAMParam);

    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActorTest", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    actor->BindInternalIAM(mockInternalIAM);

    resource_view::InstanceInfo parentInfo;
    parentInfo.set_instanceid("parentID");
    parentInfo.set_function("default/parent/$latest");
    parentInfo.set_tenantid("parentTenant");
    parentInfo.set_issystemfunc(false);

    auto parentStateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView, GetInstance("parentID")).WillRepeatedly(Return(parentStateMachine));
    EXPECT_CALL(*parentStateMachine, GetInstanceInfo).WillRepeatedly(Return(parentInfo));

    FunctionMeta functionMeta = functionMeta_;
    functionMeta.funcMetaData.tenantId = "funcTenant";
    functionMeta.funcMetaData.isSystemFunc = false;

    auto scheduleReq = GenScheduleReq(actor);
    scheduleReq->mutable_instance()->set_parentid("parentID");
    (*scheduleReq->mutable_instance()->mutable_createoptions())[TENANT_ID] = "createOptionTenant";

    EXPECT_CALL(*mockInternalIAM, IsIAMEnabled()).WillRepeatedly(Return(true));
    EXPECT_CALL(*mockInternalIAM, Authorize(_))
        .WillOnce(Invoke([](function_proxy::AuthorizeParam &authorizeParam) -> Status {
            EXPECT_EQ(authorizeParam.callerTenantID, "parentTenant");
            EXPECT_EQ(authorizeParam.calleeTenantID, "funcTenant");
            EXPECT_EQ(authorizeParam.callMethod, function_proxy::CALL_METHOD_CREATE);
            return Status::OK();
        }));

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto authorizeFuture = actor->AuthorizeCreate(functionMeta, scheduleReq, runtimePromise);
    EXPECT_AWAIT_READY(authorizeFuture);
    EXPECT_TRUE(authorizeFuture.Get().IsOk());
    EXPECT_EQ(scheduleReq->instance().tenantid(), "parentTenant");
}

TEST_F(InstanceCtrlTest, ScheduleSuccess)
{
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewControlInterfacePosixClient(_, _, _, _, _, _))
        .WillOnce(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClient, Readiness).WillOnce(Return(Status::OK()));
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&req) -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(req);
            return runtime::NotifyResponse();
        }));
    EXPECT_CALL(*mockSharedClient, Heartbeat).WillRepeatedly(Return(Status::OK()));
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_));
    instanceCtrl->BindInternalIAM(mockInternalIAM_);
    resources::InstanceInfo instanceInfo;
    instanceInfo.set_parentfunctionproxyaid(actor->GetAID());
    instanceInfo.set_parentid("parent");
    (*instanceInfo.mutable_createoptions())[RECOVER_RETRY_TIMES_KEY] = "1";
    EXPECT_CALL(*observer, PutInstance).WillRepeatedly(Return(Status::OK()));
    instanceCtrl->BindObserver(observer);

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_)).WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, "" }));
    EXPECT_CALL(*scheduler, ScheduleConfirm).Times(1);
    instanceCtrl->BindScheduler(scheduler);

    auto metaClient = MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ });
    auto functionAgentMgr = std::make_shared<MockFunctionAgentMgr>("funcAgentMgr", metaClient);
    messages::DeployInstanceResponse deployInstanceResponse;
    deployInstanceResponse.set_code(StatusCode::SUCCESS);

    EXPECT_CALL(*functionAgentMgr, DeployInstance).WillOnce(Return(deployInstanceResponse));
    instanceCtrl->BindFunctionAgentMgr(functionAgentMgr);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    auto parentStateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto parentInfo = GenParentInstanceInfo("DesignatedParentID", actor->GetAID(), "tenant001");
    EXPECT_CALL(*parentStateMachine, GetInstanceInfo).WillRepeatedly(Return(parentInfo));
    int selfLookupTimes = 0;

    GeneratedInstanceStates genStates{ "DesignatedInstanceID", InstanceState::NEW, false };
    EXPECT_CALL(*instanceControlView, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView, GetInstance(testing::_))
        .WillRepeatedly(Invoke([stateMachine, parentStateMachine, &selfLookupTimes](const std::string &instanceID) {
            if (instanceID == "DesignatedParentID") {
                return std::static_pointer_cast<InstanceStateMachine>(parentStateMachine);
            }
            if (instanceID == "DesignatedInstanceID") {
                return selfLookupTimes++ == 0 ? std::shared_ptr<InstanceStateMachine>(nullptr)
                                              : std::static_pointer_cast<InstanceStateMachine>(stateMachine);
            }
            return std::static_pointer_cast<InstanceStateMachine>(stateMachine);
        }));
    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, GetOwner()).WillRepeatedly(Return("nodeID"));
    EXPECT_CALL(mockStateMachine, TransitionToImpl)
        .WillOnce(Return(SCHEDULING_RESULT))
        .WillOnce(Return(CREATING_RESULT))
        .WillOnce(Return(RUNNING_RESULT));
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->mutable_instance()->set_parentid("DesignatedParentID");
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(actor->GetAID());
    (*(scheduleReq->mutable_instance()->mutable_createoptions()))["ConcurrentNum"] = "2";
    scheduleReq->mutable_instance()->set_lowreliability(true);
    scheduleReq->mutable_instance()->mutable_scheduleoption()->set_schedpolicyname("shared");
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));

    litebus::Future<runtime::CallResponse> sendRet;
    runtime::CallResponse response;
    sendRet.SetValue(response);
    litebus::Future<runtime::CallRequest> call;
    EXPECT_CALL(*mockSharedClient, InitCallWrapper).WillRepeatedly(DoAll(FutureArg<0>(&call), Return(sendRet)));
    call.OnComplete([instanceCtrl]() {
        auto callResult = std::make_shared<functionsystem::CallResult>();
        instanceCtrl->CallResult("DesignatedInstanceID", callResult);
    });

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), 0);
    EXPECT_EQ(runtimePromise->GetFuture().Get().code(), 0);
    EXPECT_EQ(call.Get().createoptions().size(), (size_t)1);
    auto instanceRequiredAffinitySize =
        scheduleReq->instance().scheduleoption().affinity().inner().tenant().requiredantiaffinity().condition().subconditions_size();
    EXPECT_TRUE(instanceRequiredAffinitySize > 0);
    EXPECT_EQ(instanceInfo.schedulerchain().size(), 0);
    ASSERT_AWAIT_READY(notifyCalled.GetFuture());
    EXPECT_EQ(static_cast<StatusCode>(notifyCalled.GetFuture().Get().code()), StatusCode::SUCCESS);
    EXPECT_TRUE(!notifyCalled.GetFuture().Get().runtimeinfo().route().empty());
    EXPECT_CALL(mockStateMachine, IsSaving()).WillOnce(Return(false));
    // test update instance status
    EXPECT_CALL(mockStateMachine, TransitionToImpl(InstanceState::FATAL, _, _, _, _)).WillOnce(Return(RUNNING_RESULT));

    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    messages::KillInstanceResponse killInstanceRsp;
    killInstanceRsp.set_code(int32_t(common::ErrorCode::ERR_NONE));
    EXPECT_CALL(*functionAgentMgr, KillInstance).WillRepeatedly(Return(killInstanceRsp));

    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));
    instanceCtrl->BindResourceView(resourceViewMgr);
    (*instanceInfo.mutable_createoptions())[RECOVER_RETRY_TIMES_KEY] = "0";
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));

    litebus::Future<Status> status =
        instanceCtrl->UpdateInstanceStatus(GenInstanceStatusInfo("DesignatedInstanceID", 132, "abnormal"));
    ASSERT_AWAIT_READY(status);
    EXPECT_EQ(status.Get().IsOk(), true);
    EXPECT_CALL(mockStateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl).WillOnce(Return(RUNNING_RESULT));

    status = instanceCtrl->UpdateInstanceStatus(GenInstanceStatusInfo("DesignatedInstanceID", 1, "sighup"));
    ASSERT_AWAIT_READY(status);
    EXPECT_EQ(status.Get().IsOk(), true);
}

TEST_F(InstanceCtrlTest, ScheduleRecoverInstanceSuccess)
{
    std::string state = "state";
    auto distributedCacheClient = std::make_shared<MockDistributedCacheClient>();
    EXPECT_CALL(*distributedCacheClient, Init).WillOnce(Return(Status::OK()));
    auto stateClient = std::make_shared<StateClient>(distributedCacheClient);

    auto stateActor = std::make_shared<function_proxy::StateActor>(stateClient);
    litebus::Spawn(stateActor);
    function_proxy::StateHandler::BindStateActor(stateActor);

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewControlInterfacePosixClient(_, _, _, _, _, _))
        .WillOnce(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClient, Readiness).WillOnce(Return(Status::OK()));
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);

    EXPECT_CALL(*mockSharedClient, Heartbeat).WillRepeatedly(Return(Status::OK()));
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_));
    instanceCtrl->BindInternalIAM(mockInternalIAM_);
    resources::InstanceInfo instanceInfo;
    instanceInfo.set_functionproxyid("nodeID");
    instanceInfo.set_instanceid("DesignatedInstanceID");
    EXPECT_CALL(*observer, PutInstance).WillRepeatedly(Return(Status::OK()));
    instanceCtrl->BindObserver(observer);

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_)).WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, "" }));
    instanceCtrl->BindScheduler(scheduler);

    auto metaClient = MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ });
    auto functionAgentMgr = std::make_shared<MockFunctionAgentMgr>("funcAgentMgr", metaClient);
    messages::DeployInstanceResponse rsp;
    rsp.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*functionAgentMgr, DeployInstance).WillOnce(Return(rsp));
    instanceCtrl->BindFunctionAgentMgr(functionAgentMgr);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;

    GeneratedInstanceStates genStates{ "DesignatedInstanceID", InstanceState::NEW, false };
    EXPECT_CALL(*instanceControlView, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView, GetInstance).WillOnce(Return(nullptr)).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    auto runningPromise = std::make_shared<litebus::Promise<bool>>();
    auto runningFut = runningPromise->GetFuture();
    EXPECT_CALL(mockStateMachine, TransitionToImpl)
        .WillOnce(Return(SCHEDULING_RESULT))
        .WillOnce(Return(CREATING_RESULT))
        .WillOnce(DoAll(InvokeWithoutArgs([runningPromise]() { runningPromise->SetValue(true); }),
                        Return(RUNNING_RESULT)));
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_ischeckpointed(true);
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));

    runtime::RecoverResponse recoverRsp;
    recoverRsp.set_code(common::ErrorCode::ERR_NONE);
    EXPECT_CALL(*mockSharedClient, Recover).WillOnce(Return(recoverRsp));

    litebus::Future<std::string> strFut;
    EXPECT_CALL(*distributedCacheClient,
                Get(Matcher<const std::string &>("DesignatedInstanceID"), Matcher<std::string &>(Eq(""))))
        .WillOnce(DoAll(SetArgReferee<1>(state), FutureArg<0>(&strFut), Return(Status::OK())));
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), 0);
    EXPECT_EQ(runtimePromise->GetFuture().Get().code(), 0);
    ASSERT_AWAIT_READY(strFut);
    ASSERT_AWAIT_READY(runningFut);
    litebus::Terminate(stateActor->GetAID());
    litebus::Await(stateActor->GetAID());
}

TEST_F(InstanceCtrlTest, KillEmptyInstanceID)
{
    auto killReq = GenKillRequest("", SHUT_DOWN_SIGNAL);
    auto srcInstance = "instanceM";
    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_PARAM_INVALID);
    EXPECT_EQ(killRsp.message(), "instanceID is empty");
}

TEST_F(InstanceCtrlTest, KillEmptyInstanceInfo)
{
    auto killReq = GenKillRequest("InstanceA", SHUT_DOWN_SIGNAL);
    auto srcInstance = "instanceM";
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(nullptr));
    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(killRsp.message(), "");
}

TEST_F(InstanceCtrlTest, KillInstanceWithCreating)
{
    const std::string instanceID = "InstanceA";
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    resources::InstanceInfo instance;
    instance.set_instanceid(instanceID);
    instance.set_requestid("request");
    instance.set_functionproxyid("nodeN");
    instance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::CREATING));
    auto readyInstance = instance;
    readyInstance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::EXITED));
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instance);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);

    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetInstanceContextCopy).WillRepeatedly(Return(instanceContext));
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instance));
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));
    EXPECT_CALL(mockStateMachine, AddStateChangeCallback)
        .WillOnce(Invoke([instance](const std::unordered_set<InstanceState> &statesConcerned,
                                    const std::function<void(const resources::InstanceInfo &)> &callback,
                                    const std::string &eventKey) { callback(instance); }))
        .WillOnce(Invoke([](const std::unordered_set<InstanceState> &statesConcerned,
                            const std::function<void(const resources::InstanceInfo &)> &callback,
                            const std::string &eventKey) {
            resources::InstanceInfo exitedInstance;
            exitedInstance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::EXITED));
            callback(exitedInstance);
        }));

    auto killReq = GenKillRequest(instanceID, SHUT_DOWN_SIGNAL);
    auto srcInstance = "instanceM";
    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);
}

TEST_F(InstanceCtrlTest, KillInstanceRemote)
{
    const std::string instanceID = "InstanceA";
    const std::string funcAgentID = "funcAgentA";
    const std::string function = "default/0-test-helloWorld/$latest";
    const std::string runtimeID = "runtimeA";
    const std::string functionProxyID = "nodeB";

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine)).WillRepeatedly(Return(nullptr));

    resources::InstanceInfo instanceInfo;
    instanceInfo.set_functionagentid(funcAgentID);
    instanceInfo.set_instanceid(instanceID);
    instanceInfo.set_function(function);
    instanceInfo.mutable_instancestatus()->set_code(int32_t(InstanceState::RUNNING));
    instanceInfo.set_runtimeid(runtimeID);
    instanceInfo.set_functionproxyid(functionProxyID);
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instanceInfo);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);
    EXPECT_CALL(mockStateMachine, GetInstanceContextCopy).WillRepeatedly(Return(instanceContext));

    auto killReq = GenKillRequest(instanceID, SHUT_DOWN_SIGNAL);
    auto srcInstance = "instanceM";
    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);
}

TEST_F(InstanceCtrlTest, DISABLED_KillInstanceLocal)
{
    const std::string instanceID = "InstanceA";
    const std::string funcAgentID = "funcAgentA";
    const std::string function = "default/0-test-helloWorld/$latest";
    const std::string runtimeID = "runtimeA";
    const std::string functionProxyID = "nodeN";

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    resources::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(instanceID);
    instanceInfo.set_functionagentid(funcAgentID);
    instanceInfo.mutable_instancestatus()->set_code(int32_t(InstanceState::RUNNING));
    instanceInfo.set_function(function);
    instanceInfo.set_runtimeid(runtimeID);
    instanceInfo.set_functionproxyid(functionProxyID);
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instanceInfo);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);
    EXPECT_CALL(mockStateMachine, GetInstanceContextCopy).WillRepeatedly(Return(instanceContext));
    EXPECT_CALL(*funcAgentMgr_, IsFuncAgentRecovering(testing::_)).WillRepeatedly(Return(true));

    EXPECT_CALL(mockStateMachine, IsSaving).WillOnce(Return(false));
    EXPECT_CALL(mockStateMachine, TryExitInstance)
        .WillOnce(Invoke([](const std::shared_ptr<litebus::Promise<Status>> &promise,
                            const std::shared_ptr<KillContext> &killCtx, bool isSynchronized) {
            promise->SetValue(Status::OK());
            return Status::OK();
        }));
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));
    auto killReq = GenKillRequest(instanceID, SHUT_DOWN_SIGNAL);
    auto srcInstance = "instanceM";

    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq);
    auto killRspDup = instanceCtrl_->Kill(srcInstance, killReq);
    ASSERT_AWAIT_READY(killRsp);
    ASSERT_AWAIT_READY(killRspDup);
    EXPECT_EQ(killRsp.Get().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(killRspDup.Get().code(), common::ErrorCode::ERR_NONE);
}

TEST_F(InstanceCtrlTest, KillGroup)
{
    const std::string instanceID = "InstanceA";
    const std::string runtimeID = "runtimeA";
    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    instanceCtrl_->BindLocalSchedSrv(localSchedSrv);

    auto killReq = GenKillRequest(instanceID, SHUT_DOWN_SIGNAL_GROUP);
    auto srcInstance = "instanceM";

    EXPECT_CALL(*localSchedSrv, KillGroup).WillOnce(Return(Status::OK()));
    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);
}

TEST_F(InstanceCtrlTest, KillInstanceByJob)
{
    const std::string jobID = "job";
    auto killReq = GenKillRequest(jobID, SHUT_DOWN_SIGNAL_ALL);

    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    instanceCtrl_->BindLocalSchedSrv(localSchedSrv);

    messages::ForwardKillResponse response;
    response.set_code(common::ErrorCode::ERR_NONE);
    EXPECT_CALL(*localSchedSrv, ForwardKillToInstanceManager).WillOnce(Return(response));
    auto killRsp = instanceCtrl_->Kill("instance", killReq);
    ASSERT_AWAIT_READY(killRsp);
    EXPECT_EQ(killRsp.Get().code(), common::ErrorCode::ERR_NONE);

    response.set_code(common::ErrorCode::ERR_PARAM_INVALID);
    EXPECT_CALL(*localSchedSrv, ForwardKillToInstanceManager).WillOnce(Return(response));
    killRsp = instanceCtrl_->Kill("instance", killReq);
    ASSERT_AWAIT_READY(killRsp);
    EXPECT_EQ(killRsp.Get().code(), common::ErrorCode::ERR_PARAM_INVALID);
}

/**
 * Feature: instance ctrl (direct routing).
 * Description: when direct routing is enabled and a route-less kill targets an
 *              instance that is not in this proxy's local view, the kill must be
 *              forwarded to function_master, and master's result relayed back.
 */
TEST_F(InstanceCtrlTest, KillForwardsToMasterWhenLocalMissAndDirectRoutingEnabled)
{
    auto guard = DirectRoutingConfig::EnableForTest();
    const std::string instanceID = "InstanceNotLocal";
    auto killReq = GenKillRequest(instanceID, SHUT_DOWN_SIGNAL);

    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    instanceCtrl_->BindLocalSchedSrv(localSchedSrv);

    // Instance is unknown locally -> proxy must forward the kill to function_master.
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(nullptr));

    messages::ForwardKillResponse response;
    response.set_code(common::ErrorCode::ERR_NONE);
    EXPECT_CALL(*localSchedSrv, ForwardKillToInstanceManager).WillOnce(Return(response));

    auto killRsp = instanceCtrl_->Kill("src", killReq);
    ASSERT_AWAIT_READY(killRsp);
    EXPECT_EQ(killRsp.Get().code(), common::ErrorCode::ERR_NONE);
}

/**
 * Feature: instance ctrl (direct routing).
 * Description: a route-less kill forwarded to function_master for an instance that
 *              master also cannot find should complete successfully because kill
 *              treats a missing instance as already gone.
 */
TEST_F(InstanceCtrlTest, KillForwardedToMasterRelaysNotFound)
{
    auto guard = DirectRoutingConfig::EnableForTest();
    const std::string instanceID = "InstanceUnknownEverywhere";
    auto killReq = GenKillRequest(instanceID, SHUT_DOWN_SIGNAL);

    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    instanceCtrl_->BindLocalSchedSrv(localSchedSrv);

    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(nullptr));

    messages::ForwardKillResponse response;
    response.set_code(common::ErrorCode::ERR_INSTANCE_NOT_FOUND);
    EXPECT_CALL(*localSchedSrv, ForwardKillToInstanceManager).WillOnce(Return(response));

    auto killRsp = instanceCtrl_->Kill("src", killReq);
    ASSERT_AWAIT_READY(killRsp);
    EXPECT_EQ(killRsp.Get().code(), common::ErrorCode::ERR_NONE);
}

TEST_F(InstanceCtrlTest, FrontendKillLocalMissNeverRelaysOrClaimsAuthoritativeDeletion)
{
    auto guard = DirectRoutingConfig::EnableForTest();
    auto killReq = GenKillRequest("InstanceOwnedByAnotherProxy", SHUT_DOWN_SIGNAL);
    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    instanceCtrl_->BindLocalSchedSrv(localSchedSrv);

    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(nullptr));
    EXPECT_CALL(*localSchedSrv, ForwardKillToInstanceManager).Times(0);

    auto killRsp = instanceCtrl_->KillFrontend("tenant-a", killReq);
    ASSERT_AWAIT_READY(killRsp);
    EXPECT_EQ(killRsp.Get().code(), common::ERR_INSTANCE_NOT_FOUND);
    EXPECT_EQ(killRsp.Get().message(), "frontend proxy is not the owning proxy for this instance");
}

TEST_F(InstanceCtrlTest, FrontendKillRejectsInstanceWithoutMatchingTenant)
{
    auto killReq = GenKillRequest("InstanceWithoutTenant", SHUT_DOWN_SIGNAL);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(killReq->instanceid());
    EXPECT_CALL(*instanceControlView_, GetInstance(killReq->instanceid())).WillOnce(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillOnce(Return(instanceInfo));

    auto killRsp = instanceCtrl_->KillFrontend("tenant-a", killReq);

    ASSERT_AWAIT_READY(killRsp);
    EXPECT_EQ(killRsp.Get().code(), common::ERR_AUTHORIZE_FAILED);
}

TEST_F(InstanceCtrlTest, FrontendKillRejectsEmptyTenant)
{
    auto killReq = GenKillRequest("InstanceWithTenant", SHUT_DOWN_SIGNAL);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance(killReq->instanceid())).WillOnce(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceInfo).Times(0);

    auto killRsp = instanceCtrl_->KillFrontend("", killReq);

    ASSERT_AWAIT_READY(killRsp);
    EXPECT_EQ(killRsp.Get().code(), common::ERR_AUTHORIZE_FAILED);
}

TEST(FrontendKillCleanupSnapshotTest, CompleteRequiresAllFiveObservedCleanupSignals)
{
    FrontendKillCleanupSnapshot snapshot;
    snapshot.requestTicketKnown = true;
    snapshot.requestTicketCleared = true;
    snapshot.instanceTicketKnown = true;
    snapshot.instanceTicketCleared = true;
    snapshot.runtimeState = "terminated";
    snapshot.instanceState = "absent";
    snapshot.pendingInvokeCount = 1;
    EXPECT_FALSE(snapshot.IsComplete());

    snapshot.pendingInvokeCount = 0;
    EXPECT_TRUE(snapshot.IsComplete());

    snapshot.runtimeState = "unknown";
    EXPECT_FALSE(snapshot.IsComplete());
}

TEST(FrontendKillEvidenceTest, OlderKillResultCannotOverwriteNewerRequestEvidence)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    const std::string instanceID = "concurrent-kill-instance";
    actor->frontendKillRuntimeEvidence_[instanceID] = { "newer-request", "terminating" };
    InstanceInfo olderKill;
    olderKill.set_instanceid(instanceID);
    olderKill.set_requestid("older-request");
    messages::KillInstanceResponse response;
    response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));

    auto result = actor->RecordFrontendKillRuntimeResult(olderKill, response);

    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(actor->frontendKillRuntimeEvidence_[instanceID].first, "newer-request");
    EXPECT_EQ(actor->frontendKillRuntimeEvidence_[instanceID].second, "terminating");
}

/**
 * Feature: instance ctrl (direct routing).
 * Description: a kill that already carries routing information must NOT be
 *              forwarded to function_master even when the instance is not local;
 *              it follows the normal routing path (here: not found locally, which
 *              kill treats as already gone).
 */
TEST_F(InstanceCtrlTest, KillWithRouteInfoIsNotForwardedToMaster)
{
    auto guard = DirectRoutingConfig::EnableForTest();
    const std::string instanceID = "InstanceWithRoute";
    auto killReq = GenKillRequest(instanceID, SHUT_DOWN_SIGNAL);
    killReq->set_proxyid("ownerProxy");
    killReq->set_routeaddress("127.0.0.1:1234");

    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    instanceCtrl_->BindLocalSchedSrv(localSchedSrv);

    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(nullptr));
    // The kill carries routing info, so it must not be forwarded to master.
    EXPECT_CALL(*localSchedSrv, ForwardKillToInstanceManager).Times(0);

    auto killRsp = instanceCtrl_->Kill("src", killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);
}


/**
 * Feature: instance ctrl.
 * Description: instance ctrl sync instances successfully and not kill any instance.
 * Steps:
 * 1. Mock GetAgentInstanceInfoByID return instanceInfoMap.
 * 2. send request of sync instances.
 * Expectation: don't send kill request to functionAgentMgr and check consistency successfully.
 */
TEST_F(InstanceCtrlTest, SyncInstanceNoKillSuccess)
{
    auto killResponse = GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID");

    EXPECT_CALL(*funcAgentMgr_.get(), KillInstance(testing::_, testing::_, testing::_)).Times(0);

    litebus::Future<std::string> observerFuncAgentID;
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfo.set_tenantid("tenant1");
    instanceInfoMap.insert({ "instance1", instanceInfo });
    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_)).WillOnce(test::Return(instanceInfoMap));

    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");
    auto instances = resourceUnit->mutable_instances();
    instances->insert({ "instance1", instanceInfo });

    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);

    ASSERT_AWAIT_READY(syncRet);
    EXPECT_EQ(syncRet.Get().StatusCode(), StatusCode::SUCCESS);
}

/**
 * Feature: instance ctrl.
 * Description: instance ctrl sync instances successfully and kill an instance.
 * Steps:
 * 1. Mock KillInstance return killResponse.
 * 2. Mock GetAgentInstanceInfoByID return instanceInfoMap.
 * 3. Mock GetFuncMeta return functionMeta.
 * 4. send request of sync instances.
 * Expectation: send a kill request to functionAgentMgr and check consistency successfully.
 */
TEST_F(InstanceCtrlTest, SyncInstanceKillSuccess)
{
    auto killResponse = GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID");

    litebus::Future<std::shared_ptr<messages::KillInstanceRequest>> killRequestFuture;
    litebus::Future<std::string> funcAgentIDFuture;
    EXPECT_CALL(*funcAgentMgr_.get(), KillInstance(testing::_, testing::_, testing::_))
        .WillOnce(testing::DoAll(test::FutureArg<0>(&killRequestFuture), test::FutureArg<1>(&funcAgentIDFuture),
                                 test::Return(killResponse)));

    litebus::Future<std::string> observerFuncAgentIDFuture;
    function_proxy::InstanceInfoMap instanceInfoMap;
    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_))
        .WillOnce(testing::DoAll(test::FutureArg<0>(&observerFuncAgentIDFuture), test::Return(instanceInfoMap)));

    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");
    auto instances = resourceUnit->mutable_instances();

    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfo.set_functionagentid("funcAgentID");
    instanceInfo.mutable_scheduleoption()->set_schedpolicyname("monopoly");
    instances->insert({ "instance1", instanceInfo });

    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillOnce(Return(Status::OK()));
    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);

    auto observerFuncAgentID = observerFuncAgentIDFuture.Get(1000);
    ASSERT_TRUE(observerFuncAgentID.IsSome());
    EXPECT_STREQ(observerFuncAgentID.Get().c_str(), "funcAgentID");

    auto funcAgentID = funcAgentIDFuture.Get(1000);
    ASSERT_TRUE(funcAgentID.IsSome());
    EXPECT_STREQ(funcAgentID.Get().c_str(), "funcAgentID");

    ASSERT_AWAIT_READY(syncRet);
    EXPECT_EQ(syncRet.Get().StatusCode(), StatusCode::SUCCESS);

    ASSERT_AWAIT_READY(killRequestFuture);
    EXPECT_EQ(killRequestFuture.Get()->ismonopoly(), true);
}

/**
 * Feature: instance ctrl.
 * Description: instance ctrl sync instances successfully, and status is exiting and then kill an instance.
 * Steps:
 * 1. Mock KillInstance return killResponse.
 * 2. Mock GetAgentInstanceInfoByID return instanceInfoMap.
 * 3. Mock GetFuncMeta return functionMeta.
 * 4. send request of sync instances.
 * Expectation: send a kill request to functionAgentMgr and check consistency successfully.
 */
TEST_F(InstanceCtrlTest, SyncInstanceExitingKillSuccess)
{
    auto killResponse = GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID");

    litebus::Future<std::shared_ptr<messages::KillInstanceRequest>> killRequestFuture;
    litebus::Future<std::string> funcAgentIDFuture;
    EXPECT_CALL(*funcAgentMgr_.get(), KillInstance(testing::_, testing::_, testing::_))
        .WillOnce(testing::DoAll(test::FutureArg<0>(&killRequestFuture), test::FutureArg<1>(&funcAgentIDFuture),
                                 test::Return(killResponse)));

    litebus::Future<std::string> observerFuncAgentIDFuture;
    function_proxy::InstanceInfoMap actualInstanceInfoMap;
    resource_view::InstanceInfo actualInstanceInfo;
    actualInstanceInfo.set_function("function");
    actualInstanceInfo.set_instanceid("instance1");
    auto status = actualInstanceInfo.mutable_instancestatus();
    status->set_code(static_cast<int32_t>(InstanceState::EXITING));

    actualInstanceInfoMap.insert({ "instance1", actualInstanceInfo });

    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_))
        .WillOnce(testing::DoAll(test::FutureArg<0>(&observerFuncAgentIDFuture), test::Return(actualInstanceInfoMap)));

    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");
    auto instances = resourceUnit->mutable_instances();

    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfo.set_functionagentid("funcAgentID");
    instances->insert({ "instance1", instanceInfo });
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillOnce(Return(Status::OK()));

    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);

    ASSERT_AWAIT_READY(observerFuncAgentIDFuture);
    EXPECT_STREQ(observerFuncAgentIDFuture.Get().c_str(), "funcAgentID");

    ASSERT_AWAIT_READY(funcAgentIDFuture);
    EXPECT_STREQ(funcAgentIDFuture.Get().c_str(), "funcAgentID");

    ASSERT_AWAIT_READY(killRequestFuture);
    EXPECT_STREQ(killRequestFuture.Get()->instanceid().c_str(), "instance1");

    ASSERT_AWAIT_READY(syncRet);
    EXPECT_EQ(syncRet.Get().StatusCode(), StatusCode::SUCCESS);
}

/**
 * Feature: instance ctrl.
 * Description: instance ctrl sync instances successfully and kill many instance.
 * Steps:
 * 1. Mock KillInstance return killResponse.
 * 2. Mock GetAgentInstanceInfoByID return instanceInfoMap.
 * 3. Mock GetFuncMeta return functionMeta.
 * 4. send request of sync instances.
 * Expectation: send three kill request to functionAgentMgr and check consistency successfully.
 */
TEST_F(InstanceCtrlTest, SyncInstanceKillManySuccess)
{
    auto killResponse = GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID");

    EXPECT_CALL(*funcAgentMgr_.get(), KillInstance(testing::_, testing::_, testing::_))
        .WillRepeatedly(test::Return(killResponse));

    function_proxy::InstanceInfoMap instanceInfoMap;
    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_))
        .WillOnce(test::Return(instanceInfoMap))
        .WillOnce(test::Return(litebus::None()));

    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");
    auto instances = resourceUnit->mutable_instances();

    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_function("function");
    instanceInfo.set_instanceid("instance1");
    instances->insert({ "instance1", instanceInfo });
    instanceInfo.set_instanceid("instance2");
    instances->insert({ "instance2", instanceInfo });
    instanceInfo.set_instanceid("instance3");
    instances->insert({ "instance3", instanceInfo });
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);

    ASSERT_AWAIT_READY(syncRet);
    EXPECT_EQ(syncRet.Get().StatusCode(), StatusCode::SUCCESS);

    syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);

    ASSERT_AWAIT_READY(syncRet);
    EXPECT_EQ(syncRet.Get().StatusCode(), StatusCode::SUCCESS);
}

/**
 * Feature: instance ctrl.
 * Description: instance ctrl get instances info when sync instances.
 * Steps:
 * 1. Mock GetAgentInstanceInfoByID return instanceInfoMap.
 * 2. send request of sync instances.
 * Expectation: return fail.
 */
TEST_F(InstanceCtrlTest, SyncInstanceGetInstanceInfoFail)
{
    litebus::Promise<litebus::Option<function_proxy::InstanceInfoMap>> instanceInfoPromise;
    instanceInfoPromise.SetFailed(StatusCode::FAILED);
    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_))
        .WillOnce(test::Return(instanceInfoPromise.GetFuture()));

    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");
    auto instances = resourceUnit->mutable_instances();

    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_function("function");
    instanceInfo.set_instanceid("instance1");
    instances->insert({ "instance1", instanceInfo });

    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);

    ASSERT_AWAIT_SET(syncRet);
    EXPECT_EQ(syncRet.GetErrorCode(), static_cast<int32_t>(StatusCode::FAILED));
}

/**
 * Feature: instance ctrl.
 * Description: instance ctrl kill instance when sync instances fail.
 * Steps:
 * 1. Mock KillInstance return error killResponse and fail.
 * 2. Mock GetAgentInstanceInfoByID return instanceInfoMap.
 * 3. Mock GetFuncMeta return functionMeta.
 * 4. send request of sync instances.
 * Expectation: return fail.
 */
TEST_F(InstanceCtrlTest, SyncInstanceKillInstanceFail)
{
    function_proxy::InstanceInfoMap instanceInfoMap;
    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_))
        .WillOnce(test::Return(instanceInfoMap))
        .WillOnce(test::Return(instanceInfoMap));

    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");
    auto instances = resourceUnit->mutable_instances();

    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_function("function");
    instanceInfo.set_instanceid("instance1");
    instances->insert({ "instance1", instanceInfo });

    auto killResponse = GenKillInstanceResponse(StatusCode::FAILED, "kill instance successfully", "requestID");
    litebus::Promise<messages::KillInstanceResponse> killPromise;
    killPromise.SetFailed(StatusCode::FAILED);
    EXPECT_CALL(*funcAgentMgr_.get(), KillInstance(testing::_, testing::_, testing::_))
        .WillOnce(test::Return(killResponse));

    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));

    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);
    ASSERT_AWAIT_SET(syncRet);
    EXPECT_EQ(syncRet.GetErrorCode(), static_cast<int32_t>(StatusCode::LS_SYNC_INSTANCE_FAIL));

    syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);
    ASSERT_AWAIT_SET(syncRet);
    EXPECT_EQ(syncRet.GetErrorCode(), 0);
}

resource_view::InstanceInfo GenInstanceInfo(const std::string &instanceID, const std::string &proxyID,
                                            const std::string &runtimeID, int32_t status)
{
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(instanceID);
    instanceInfo.set_functionproxyid(proxyID);
    instanceInfo.set_runtimeid(runtimeID);
    auto instanceStatus = instanceInfo.mutable_instancestatus();
    instanceStatus->set_code(status);
    return instanceInfo;
}

/**
 * Feature: forward custom signal.
 * Description: forward custom signal success and get correct response.
 * Steps:
 * 1. Mock GetInstanceInfoByID(observer) return valid instance info .
 * 2. Mock GetFuncMeta(observer) return function meta info.
 * 3. Mock GetLocalSchedulerAID(observer) return aid of forward local.
 * 4. Mock ForwardCustomSignalRequest(instanceCtrlActor) to send correct response.
 * 5. send request of forward custom signal to mockInstanceCtrlActor.
 * Expectation:
 * 1. return response correctly.
 */
TEST_F(InstanceCtrlTest, ForwardCustomSignalSuccess)
{
    const std::string srcInstance = "srcInstance";
    const std::string instanceID = "Instance";
    const std::string instanceID1 = "InstanceID1";
    const std::string runtimeID1 = "runtimeID1";
    const std::string proxyID1 = "proxyID1";
    const int32_t customSignal = 100;

    const std::string mockInstanceCtrlActorName = "mockInstanceCtrlActor";

    auto mockInstanceCtrlActor =
        std::make_shared<MockInstanceCtrlActor>(mockInstanceCtrlActorName, proxyID1, instanceCtrlConfig);
    litebus::Spawn(mockInstanceCtrlActor);
    ::testing::Mock::AllowLeak(mockInstanceCtrlActor.get());

    auto killReq = GenKillRequest(instanceID1, customSignal);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("proxyID1");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));

    resource_view::InstanceInfo instanceInfo =
        GenInstanceInfo(instanceID1, proxyID1, runtimeID1, static_cast<int32_t>(InstanceState::RUNNING));
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instanceInfo);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);
    EXPECT_CALL(mockStateMachine, GetInstanceContextCopy).WillOnce(Return(instanceContext));

    EXPECT_CALL(*mockObserver_, GetLocalSchedulerAID).WillOnce(Return(mockInstanceCtrlActor->GetAID()));

    auto instanceCtrlHelper = std::make_shared<InstanceCtrlHelper>();
    EXPECT_CALL(*mockInstanceCtrlActor, MockForwardCustomSignalRequest(testing::_, testing::_, testing::_))
        .WillOnce(
            testing::Invoke(instanceCtrlHelper.get(), &InstanceCtrlHelper::MockForwardCustomSignalRequestSuccess));

    auto killRespFuture = instanceCtrlWithMockObserver_->Kill(srcInstance, killReq);

    ASSERT_AWAIT_READY(killRespFuture);

    auto killResp = killRespFuture.Get();
    EXPECT_EQ(killResp.code(), static_cast<int32_t>(common::ErrorCode::ERR_NONE));
    EXPECT_STREQ(killResp.message().c_str(), "");

    litebus::Terminate(mockInstanceCtrlActor->GetAID());
    litebus::Await(mockInstanceCtrlActor);
    mockInstanceCtrlActor.reset();
}

TEST_F(InstanceCtrlTest, ForwardCustomSignalRequestDuplicate)
{
    auto instanceCtrlActor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActorName", proxyID1, instanceCtrlConfig);
    instanceCtrlActor->BindInstanceControlView(instanceControlView_);
    instanceCtrlActor->ToReady();
    litebus::Spawn(instanceCtrlActor);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("proxyID1");
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));

    const std::string srcInstance = "srcInstance";
    const std::string instanceID = "Instance";
    const std::string instanceID1 = "InstanceID1";
    const std::string runtimeID1 = "runtimeID1";
    const std::string proxyID1 = "proxyID1";
    const int32_t customSignal = 100;

    const std::string mockInstanceCtrlActorName = "mockInstanceCtrlActor";
    auto requestID = "test-requestID";

    auto mockInstanceCtrlActor =
        std::make_shared<MockInstanceCtrlActor>(mockInstanceCtrlActorName, proxyID1, instanceCtrlConfig);
    litebus::Spawn(mockInstanceCtrlActor);
    ::testing::Mock::AllowLeak(mockInstanceCtrlActor.get());

    auto promise = litebus::Promise<core_service::KillResponse>();
    instanceCtrlActor->forwardCustomSignalRequestIDs_.emplace(requestID,
                                                              promise.GetFuture());  // if have requestID,
                                                                                     // defer to execute
    auto killReq = GenKillRequest(instanceID1, customSignal);
    auto forwardKillRequest = GenForwardKillRequest(requestID, "srcInstanceID", std::move(*killReq));
    promise.SetValue(KillResponse());
    auto called = std::make_shared<litebus::Promise<Status>>();
    EXPECT_CALL(*mockInstanceCtrlActor, MockForwardCustomSignalResponse)
        .WillOnce(Invoke([called](const litebus::AID &, const std::string &, const std::string &) {
            called->SetValue(Status::OK());
        }));
    instanceCtrlActor->ForwardCustomSignalRequest(mockInstanceCtrlActor->GetAID(), "",
                                                  forwardKillRequest->SerializeAsString());
    ASSERT_AWAIT_READY(called->GetFuture());
    litebus::Terminate(mockInstanceCtrlActor->GetAID());
    litebus::Await(mockInstanceCtrlActor);
    mockInstanceCtrlActor.reset();
    litebus::Terminate(instanceCtrlActor->GetAID());
    litebus::Await(instanceCtrlActor);
    instanceCtrlActor.reset();
}

TEST_F(InstanceCtrlTest, SendForwardCustomSignalRequestDuplicate)
{
    const std::string srcInstance = "srcInstance";
    const std::string instanceID = "Instance";
    const std::string instanceID1 = "InstanceID1";
    const std::string runtimeID1 = "runtimeID1";
    const std::string proxyID1 = "proxyID1";
    const int32_t customSignal = 1;

    const std::string InstanceCtrlActorName = "InstanceCtrlActor";

    auto instanceCtrlActor =
        std::make_shared<InstanceCtrlActor>(InstanceCtrlActorName, proxyID1, instanceCtrlConfig);
    litebus::Spawn(instanceCtrlActor);

    auto killReq = GenKillRequest(instanceID1, customSignal);
    auto requestID(killReq->requestid() + "-" + std::to_string(killReq->signal()));

    auto notifyPromise = std::make_shared<litebus::Promise<KillResponse>>();
    instanceCtrlActor->forwardCustomSignalNotifyPromise_.emplace(requestID, notifyPromise); // mock promise is exist, don't execute
    auto oldFuture = notifyPromise->GetFuture();

    auto srcAID = litebus::AID("srcAID");
    auto future = instanceCtrlActor->SendForwardCustomSignalRequest(srcAID, instanceID, killReq, "request001", false);

    KillResponse resResponse;
    resResponse.set_message("testResponse");
    notifyPromise->SetValue(resResponse);
    EXPECT_EQ(oldFuture.Get().message(), future.Get().message());

    litebus::Terminate(instanceCtrlActor->GetAID());
    litebus::Await(instanceCtrlActor);
    instanceCtrlActor.reset();
}

/**
 * Feature: forward custom signal.
 * Description: forward custom signal success and get error response.
 * Steps:
 * 1. Mock GetInstanceInfoByID(observer) return valid instance info .
 * 2. Mock GetFuncMeta(observer) return function meta info.
 * 3. Mock GetLocalSchedulerAID(observer) return aid of forward local.
 * 4. Mock ForwardCustomSignalRequest(instanceCtrlActor) to send error response.
 * 5. send request of forward custom signal to mockInstanceCtrlActor.
 * Expectation:
 * 1. return response correctly.
 */
TEST_F(InstanceCtrlTest, ForwardCustomSignalFail)
{
    auto mockInstanceCtrlActor =
        std::make_shared<MockInstanceCtrlActor>(mockInstanceCtrlActorName, proxyID1, instanceCtrlConfig);
    litebus::Spawn(mockInstanceCtrlActor);
    ::testing::Mock::AllowLeak(mockInstanceCtrlActor.get());

    auto killReq = GenKillRequest(instanceID1, customSignal);

    resource_view::InstanceInfo instanceInfo =
        GenInstanceInfo(instanceID1, proxyID1, runtimeID1, static_cast<int32_t>(InstanceState::RUNNING));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("proxyID1");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instanceInfo);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);
    EXPECT_CALL(mockStateMachine, GetInstanceContextCopy).WillOnce(Return(instanceContext));

    EXPECT_CALL(*mockObserver_, GetLocalSchedulerAID).WillOnce(Return(mockInstanceCtrlActor->GetAID()));

    auto instanceCtrlHelper = std::make_shared<InstanceCtrlHelper>();
    EXPECT_CALL(*mockInstanceCtrlActor, MockForwardCustomSignalRequest(testing::_, testing::_, testing::_))
        .WillOnce(testing::Invoke(instanceCtrlHelper.get(), &InstanceCtrlHelper::MockForwardCustomSignalRequestFail));

    auto killRespFuture = instanceCtrlWithMockObserver_->Kill(srcInstance, killReq);

    ASSERT_AWAIT_READY(killRespFuture);

    auto killResp = killRespFuture.Get();
    EXPECT_EQ(killResp.code(), static_cast<int32_t>(common::ErrorCode::ERR_INNER_SYSTEM_ERROR));
    EXPECT_STREQ(killResp.message().c_str(), "forward custom signal fail");

    litebus::Terminate(mockInstanceCtrlActor->GetAID());
    litebus::Await(mockInstanceCtrlActor);
    mockInstanceCtrlActor.reset();
}

/**
 * Feature: forward custom signal.
 * Description: retry forward custom signal success.
 * Steps:
 * 1. Mock GetInstanceInfoByID(observer) return valid instance info .
 * 2. Mock GetFuncMeta(observer) return function meta info.
 * 3. Mock GetLocalSchedulerAID(observer) return aid of forward local.
 * 4. Mock ForwardCustomSignalRequest(instanceCtrlActor)
 *    -- don't send response
 *    -- send correct response
 * 5. send request of forward custom signal to mockInstanceCtrlActor.
 * Expectation:
 * 1. return response correctly.
 */
TEST_F(InstanceCtrlTest, RetryForwardCustomSignalSucess)
{
    auto mockInstanceCtrlActor =
        std::make_shared<MockInstanceCtrlActor>(mockInstanceCtrlActorName, proxyID1, instanceCtrlConfig);
    litebus::Spawn(mockInstanceCtrlActor);
    ::testing::Mock::AllowLeak(mockInstanceCtrlActor.get());

    auto killReq = GenKillRequest(instanceID1, customSignal);

    resource_view::InstanceInfo instanceInfo =
        GenInstanceInfo(instanceID1, proxyID1, runtimeID1, static_cast<int32_t>(InstanceState::RUNNING));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>(proxyID1);
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instanceInfo);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);
    EXPECT_CALL(mockStateMachine, GetInstanceContextCopy).WillOnce(Return(instanceContext));

    EXPECT_CALL(*mockObserver_, GetLocalSchedulerAID).WillOnce(Return(mockInstanceCtrlActor->GetAID()));

    auto instanceCtrlHelper = std::make_shared<InstanceCtrlHelper>();
    EXPECT_CALL(*mockInstanceCtrlActor, MockForwardCustomSignalRequest(testing::_, testing::_, testing::_))
        .WillOnce(testing::Return(std::make_pair(false, ::internal::ForwardKillResponse())))
        .WillRepeatedly(
            testing::Invoke(instanceCtrlHelper.get(), &InstanceCtrlHelper::MockForwardCustomSignalRequestSuccess));

    auto killRespFuture = instanceCtrlWithMockObserver_->Kill(srcInstance, killReq);

    ASSERT_AWAIT_READY(killRespFuture);

    auto killResp = killRespFuture.Get();
    EXPECT_EQ(killResp.code(), static_cast<int32_t>(common::ErrorCode::ERR_NONE));
    EXPECT_STREQ(killResp.message().c_str(), "");

    litebus::Terminate(mockInstanceCtrlActor->GetAID());
    litebus::Await(mockInstanceCtrlActor);
    mockInstanceCtrlActor.reset();
}

/**
 * Feature: forward custom signal.
 * Description: retry forward custom signal fail.
 * Steps:
 * 1. Mock GetInstanceInfoByID(observer) return valid instance info .
 * 2. Mock GetFuncMeta(observer) return function meta info.
 * 3. Mock GetLocalSchedulerAID(observer) return aid of forward local.
 * 4. Mock ForwardCustomSignalRequest(instanceCtrlActor)
 *    -- don't send response repeatedly
 * 5. send request of forward custom signal to mockInstanceCtrlActor.
 * Expectation:
 * 1. return error response.
 */
TEST_F(InstanceCtrlTest, RetryForwardCustomSignalFail)
{
    auto mockInstanceCtrlActor =
        std::make_shared<MockInstanceCtrlActor>(mockInstanceCtrlActorName, proxyID1, instanceCtrlConfig);
    litebus::Spawn(mockInstanceCtrlActor);
    ::testing::Mock::AllowLeak(mockInstanceCtrlActor.get());

    auto killReq = GenKillRequest(instanceID1, customSignal);

    resource_view::InstanceInfo instanceInfo =
        GenInstanceInfo(instanceID1, proxyID1, runtimeID1, static_cast<int32_t>(InstanceState::RUNNING));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>(proxyID1);
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instanceInfo);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);
    EXPECT_CALL(mockStateMachine, GetInstanceContextCopy).WillOnce(Return(instanceContext));

    EXPECT_CALL(*mockObserver_, GetLocalSchedulerAID).WillOnce(Return(mockInstanceCtrlActor->GetAID()));

    auto instanceCtrlHelper = std::make_shared<InstanceCtrlHelper>();
    EXPECT_CALL(*mockInstanceCtrlActor, MockForwardCustomSignalRequest(testing::_, testing::_, testing::_))
        .WillRepeatedly(testing::Return(std::make_pair(false, ::internal::ForwardKillResponse())));

    instanceCtrlWithMockObserver_->SetMaxForwardKillRetryTimes(3);
    instanceCtrlWithMockObserver_->SetMaxForwardKillRetryCycleMs(100);
    auto killRespFuture = instanceCtrlWithMockObserver_->Kill(srcInstance, killReq);

    ASSERT_AWAIT_READY(killRespFuture);

    auto killResp = killRespFuture.Get();
    EXPECT_EQ(killResp.code(), static_cast<int32_t>(common::ErrorCode::ERR_INNER_COMMUNICATION));
    EXPECT_STREQ(killResp.message().c_str(), "(custom signal)don't receive response");

    litebus::Terminate(mockInstanceCtrlActor->GetAID());
    litebus::Await(mockInstanceCtrlActor);
    mockInstanceCtrlActor.reset();
}

/**
 * Feature: forward custom signal.
 * Description: receive request of forward custom signal.
 * Steps:
 * 1. Mock GetInstanceInfoByID(observer) return valid instance info .
 * 2. Mock GetFuncMeta(observer) return function meta info.
 * 3. Mock MockGetForwardCustomSignalRequest(MockInstanceCtrlActor) return request of forward custom signal
 * 4. Mock MockForwardCustomSignalResponse(MockInstanceCtrlActor) to receive response
 * 5. Mock GetControlInterfacePosixClient(MockSharedClientManagerProxy) return mockSharedClient
 * 6. Mock Signal(MockSharedClient) return signal response
 * 7. send request of forward custom signal to instanceCtrlWithMockObserver_.
 * Expectation:
 * 1. return error response.
 */
TEST_F(InstanceCtrlTest, ProcessCustomSignalSuccess)
{
    auto mockInstanceCtrlActor =
        std::make_shared<MockInstanceCtrlActor>(mockInstanceCtrlActorName, proxyID1, instanceCtrlConfig);
    litebus::Spawn(mockInstanceCtrlActor);
    ::testing::Mock::AllowLeak(mockInstanceCtrlActor.get());

    auto killReq = GenKillRequest(instanceID1, customSignal);

    resource_view::InstanceInfo instanceInfo =
        GenInstanceInfo(instanceID1, "nodeID", runtimeID1, static_cast<int32_t>(InstanceState::RUNNING));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>(proxyID1);
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instanceInfo);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);
    EXPECT_CALL(mockStateMachine, GetInstanceContextCopy).WillRepeatedly(Return(instanceContext));
    EXPECT_CALL(mockStateMachine, GetRequestID).WillOnce(Return("ins-req001"));
    EXPECT_CALL(*funcAgentMgr_, IsFuncAgentRecovering(testing::_)).WillOnce(Return(true));

    auto requestID(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    auto forwardKillRequest =
        GenForwardKillRequest(requestID, srcInstance, std::move(*GenKillRequest(instanceID1, customSignal)));
    forwardKillRequest->set_instancerequestid("ins-req001");
    EXPECT_CALL(*mockInstanceCtrlActor, MockGetForwardCustomSignalRequest).WillOnce(Return(*forwardKillRequest));

    litebus::Future<std::string> resp;
    EXPECT_CALL(*mockInstanceCtrlActor, MockForwardCustomSignalResponse(testing::_, testing::_, testing::_))
        .WillOnce(testing::DoAll(FutureArg<2>(&resp), Return()));

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));

    runtime::SignalResponse SignalRsp;
    SignalRsp.set_code(common::ErrorCode::ERR_NONE);
    EXPECT_CALL(*mockSharedClient, Signal).WillOnce(Return(SignalRsp));

    litebus::Async(mockInstanceCtrlActor->GetAID(), &MockInstanceCtrlActor::SendForwardCustomSignalRequest,
                   instanceCtrlWithMockObserver_->GetActorAID());

    ASSERT_AWAIT_READY(resp);
    ::internal::ForwardKillResponse forwardKillResponse;
    auto parseRet = forwardKillResponse.ParseFromString(resp.Get());
    ASSERT_TRUE(parseRet);
    EXPECT_STREQ(forwardKillResponse.requestid().c_str(), requestID.c_str());
    EXPECT_EQ(forwardKillResponse.code(), static_cast<int32_t>(common::ErrorCode::ERR_NONE));
    EXPECT_TRUE(forwardKillResponse.message().empty());

    litebus::Terminate(mockInstanceCtrlActor->GetAID());
    litebus::Await(mockInstanceCtrlActor);
    mockInstanceCtrlActor.reset();
}

TEST_F(InstanceCtrlTest, ProcessCustomSignalInstanceNotFound)
{
    auto mockInstanceCtrlActor =
        std::make_shared<MockInstanceCtrlActor>(mockInstanceCtrlActorName, proxyID1, instanceCtrlConfig);
    litebus::Spawn(mockInstanceCtrlActor);
    ::testing::Mock::AllowLeak(mockInstanceCtrlActor.get());
    auto killReq = GenKillRequest(instanceID1, customSignal);
    resource_view::InstanceInfo instanceInfo =
        GenInstanceInfo(instanceID1, "nodeID", runtimeID1, static_cast<int32_t>(InstanceState::RUNNING));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>(proxyID1);
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetRequestID).WillOnce(Return("ins-req002"));

    auto requestID(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    auto forwardKillRequest =
        GenForwardKillRequest(requestID, srcInstance, std::move(*GenKillRequest(instanceID1, customSignal)));
    forwardKillRequest->set_instancerequestid("ins-req001");
    EXPECT_CALL(*mockInstanceCtrlActor, MockGetForwardCustomSignalRequest).WillOnce(Return(*forwardKillRequest));

    litebus::Future<std::string> resp;
    EXPECT_CALL(*mockInstanceCtrlActor, MockForwardCustomSignalResponse(testing::_, testing::_, testing::_))
        .WillOnce(testing::DoAll(FutureArg<2>(&resp), Return()));

    litebus::Async(mockInstanceCtrlActor->GetAID(), &MockInstanceCtrlActor::SendForwardCustomSignalRequest,
                   instanceCtrlWithMockObserver_->GetActorAID());
    ASSERT_AWAIT_READY(resp);
    ::internal::ForwardKillResponse forwardKillResponse;
    auto parseRet = forwardKillResponse.ParseFromString(resp.Get());
    ASSERT_TRUE(parseRet);
    EXPECT_STREQ(forwardKillResponse.requestid().c_str(), requestID.c_str());
    EXPECT_EQ(forwardKillResponse.code(), static_cast<int32_t>(common::ErrorCode::ERR_INSTANCE_NOT_FOUND));
    litebus::Terminate(mockInstanceCtrlActor->GetAID());
    litebus::Await(mockInstanceCtrlActor);
    mockInstanceCtrlActor.reset();
}

/**
 * Feature: CheckpointTest
 * Description: checkpoint, get state from runtime, set into cache
 * Steps:
 * 1. checkpoint success
 * 2. checkpoint get null posix client
 * 3. runtime checkpoint failed
 * 4. set into cache failed
 *
 * Expectation:
 * 1. Success
 * 2-4. Failed
 */
TEST_F(InstanceCtrlTest, CheckpointTest)
{
    auto distributedCacheClient = std::make_shared<MockDistributedCacheClient>();

    EXPECT_CALL(*distributedCacheClient, Init).WillOnce(Return(Status::OK()));
    auto stateClient = std::make_shared<StateClient>(distributedCacheClient);
    auto stateActor = std::make_shared<function_proxy::StateActor>(stateClient);
    litebus::Spawn(stateActor);
    function_proxy::StateHandler::BindStateActor(stateActor);

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    runtime::CheckpointResponse checkpointRsp;
    checkpointRsp.set_code(common::ErrorCode::ERR_NONE);
    checkpointRsp.set_state("state");
    EXPECT_CALL(*mockSharedClient, Checkpoint).WillOnce(Return(checkpointRsp));
    EXPECT_CALL(*distributedCacheClient, Set).WillOnce(Return(Status::OK()));
    auto future = Async(instanceCtrl_->GetActorAID(), &InstanceCtrlActor::Checkpoint, "instance_id");
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get().IsOk());

    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(nullptr));
    future = Async(instanceCtrl_->GetActorAID(), &InstanceCtrlActor::Checkpoint, "instance_id");
    ASSERT_AWAIT_SET(future);
    EXPECT_TRUE(future.IsOK());
    EXPECT_TRUE(future.Get().IsError());

    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    checkpointRsp.set_code(common::ErrorCode::ERR_PARAM_INVALID);
    EXPECT_CALL(*mockSharedClient, Checkpoint).WillOnce(Return(checkpointRsp));
    future = Async(instanceCtrl_->GetActorAID(), &InstanceCtrlActor::Checkpoint, "instance_id");
    ASSERT_AWAIT_SET(future);
    EXPECT_TRUE(future.IsOK());
    EXPECT_TRUE(future.Get().IsError());

    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    checkpointRsp.set_code(common::ErrorCode::ERR_NONE);
    EXPECT_CALL(*mockSharedClient, Checkpoint).WillOnce(Return(checkpointRsp));
    EXPECT_CALL(*distributedCacheClient, Set).WillOnce(Return(Status(StatusCode::FAILED)));
    future = Async(instanceCtrl_->GetActorAID(), &InstanceCtrlActor::Checkpoint, "instance_id");
    ASSERT_AWAIT_SET(future);
    EXPECT_TRUE(future.IsOK());
    EXPECT_TRUE(future.Get().IsError());

    litebus::Terminate(stateActor->GetAID());
    litebus::Await(stateActor->GetAID());
}

/**
 * Feature: RecoverTest
 * Description: recover, get state from cache, runtime recover
 * Steps:
 * 1. recover success
 * 2. recover get null posix client
 * 3. get from cache failed
 * 4. runtime recover failed

 *
 * Expectation:
 * 1. Success
 * 2-4. Failed
 */
TEST_F(InstanceCtrlTest, RecoverTest)
{
    std::string state = "state";
    auto distributedCacheClient = std::make_shared<MockDistributedCacheClient>();
    EXPECT_CALL(*distributedCacheClient, Init).WillOnce(Return(Status::OK()));
    auto stateClient = std::make_shared<StateClient>(distributedCacheClient);
    auto stateActor = std::make_shared<function_proxy::StateActor>(stateClient);

    litebus::Spawn(stateActor);
    function_proxy::StateHandler::BindStateActor(stateActor);

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    EXPECT_CALL(*distributedCacheClient,
                Get(Matcher<const std::string &>("instance_id"), Matcher<std::string &>(Eq(""))))
        .WillOnce(DoAll(SetArgReferee<1>(state), Return(Status::OK())));
    runtime::RecoverResponse recoverRsp;
    recoverRsp.set_code(common::ErrorCode::ERR_NONE);
    EXPECT_CALL(*mockSharedClient, Recover).WillOnce(Return(recoverRsp));

    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance_id");
    auto future = Async(instanceCtrl_->GetActorAID(), &InstanceCtrlActor::Recover, instanceInfo);
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get().IsOk());

    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(nullptr));
    future = Async(instanceCtrl_->GetActorAID(), &InstanceCtrlActor::Recover, instanceInfo);
    ASSERT_AWAIT_SET(future);
    EXPECT_TRUE(future.IsOK());
    EXPECT_TRUE(future.Get().IsError());

    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    EXPECT_CALL(*distributedCacheClient,
                Get(Matcher<const std::string &>("instance_id"), Matcher<std::string &>(Eq(""))))
        .WillOnce(DoAll(SetArgReferee<1>(state), Return(Status(StatusCode::FAILED))));
    future = Async(instanceCtrl_->GetActorAID(), &InstanceCtrlActor::Recover, instanceInfo);
    ASSERT_AWAIT_SET(future);
    EXPECT_TRUE(future.IsOK());
    EXPECT_TRUE(future.Get().IsError());

    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    EXPECT_CALL(*distributedCacheClient,
                Get(Matcher<const std::string &>("instance_id"), Matcher<std::string &>(Eq(""))))
        .WillOnce(DoAll(SetArgReferee<1>(state), Return(Status::OK())));
    recoverRsp.set_code(common::ErrorCode::ERR_PARAM_INVALID);
    EXPECT_CALL(*mockSharedClient, Recover(_, DEFAULT_RECOVER_TIMEOUT_MS)).WillOnce(Return(recoverRsp));
    future = Async(instanceCtrl_->GetActorAID(), &InstanceCtrlActor::Recover, instanceInfo);
    ASSERT_AWAIT_SET(future);
    EXPECT_TRUE(future.IsOK());
    EXPECT_TRUE(future.Get().IsError());

    // recover timeout
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    EXPECT_CALL(*distributedCacheClient,
                Get(Matcher<const std::string &>("instance_id"), Matcher<std::string &>(Eq(""))))
        .WillOnce(DoAll(SetArgReferee<1>(state), Return(Status::OK())));
    EXPECT_CALL(*mockSharedClient, Recover(_, 100)).WillOnce(Return(recoverRsp));
    (*instanceInfo.mutable_createoptions())[RECOVER_RETRY_TIMEOUT_KEY] = "100";
    future = Async(instanceCtrl_->GetActorAID(), &InstanceCtrlActor::Recover, instanceInfo);
    ASSERT_AWAIT_SET(future);
    EXPECT_TRUE(future.IsOK());
    EXPECT_TRUE(future.Get().IsError());

    litebus::Terminate(stateActor->GetAID());
    litebus::Await(stateActor->GetAID());
}

/**
* Feature CreateInstanceClientTest:
* Description try to reconnect runtime, log connection info;
* Steps:
* 1. mock NewControlInterfacePosixClient method to return nullptr client;
* 2. invoke CreateInstanceClient method;.

* Expectation:
* 1. client returned is nullptr;
* 2. reconncetion occurs 3 times.
*/
TEST_F(InstanceCtrlTest, CreateInstanceClientTest)
{
    functionsystem::metrics::MetricsAdapter::GetInstance().InitMetricsFromJson(
        nlohmann::json::parse(InstanceCreateFailureAlarmExporterJsonStr), GetMetricsFilesName, {});
    functionsystem::metrics::MetricsAdapter::GetInstance().SetContextAttr("component_name", "function_proxy");

    // clientManager, funcAgentMgr, scheduler and observer stubs
    auto clientManager = std::make_shared<MockSharedClientManagerProxy>();
    litebus::Future<std::string> fut;
    EXPECT_CALL(*clientManager, NewControlInterfacePosixClient(_, _, _, _, _, _))
        .WillOnce(DoAll(FutureArg<0>(&fut), Return(nullptr)))
        .WillRepeatedly(Return(nullptr));

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*clientManager, GetControlInterfacePosixClient(_)).WillRepeatedly(Return(mockSharedClient));
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&r) -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(r);
            return runtime::NotifyResponse();
        }));

    EXPECT_CALL(*clientManager, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    const std::string funcAgentID = "funcAgentA";
    const std::string function = "default/0-test-helloWorld/$latest";
    const std::string functionProxyID = "nodeN";
    const std::string jobID = "job";
    const std::string requestID = "requestID_CreateInstanceClientTest";
    InstanceState state = InstanceState::NEW;
    const std::string runtimeIDA = "runtimeA";
    auto insInfoA = GenInstanceInfo("", funcAgentID, function, state);
    insInfoA.set_runtimeid(runtimeIDA);
    insInfoA.set_functionproxyid(functionProxyID);
    insInfoA.set_jobid(jobID);
    insInfoA.set_runtimeaddress("requestIDaddress");
    insInfoA.set_requestid(requestID);
    insInfoA.set_parentid("parent");

    auto scheduleReqA = std::make_shared<messages::ScheduleRequest>();
    scheduleReqA->mutable_instance()->CopyFrom(insInfoA);
    scheduleReqA->set_requestid(requestID);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReqA));
    EXPECT_CALL(mockStateMachine, SetRuntimeID).WillRepeatedly(Return());
    EXPECT_CALL(mockStateMachine, SetStartTime).WillRepeatedly(Return());
    EXPECT_CALL(mockStateMachine, SetRuntimeAddress).WillRepeatedly(Return());
    EXPECT_CALL(mockStateMachine, GetInstanceState).WillRepeatedly(Return(InstanceState::NEW));
    EXPECT_CALL(mockStateMachine, GetVersion).WillRepeatedly(Return(0));
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    GeneratedInstanceStates genStates{ "GeneratedInstanceID", InstanceState::NEW, false };

    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl)
        .WillOnce(Return(SCHEDULING_RESULT))
        .WillOnce(Return(CREATING_RESULT))
        .WillOnce(Return(FATAL_RESULT));
    messages::DeployInstanceResponse deployInstanceResponse;
    deployInstanceResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*funcAgentMgr_.get(), DeployInstance).WillRepeatedly(Return(deployInstanceResponse));

    messages::KillInstanceResponse killInstanceRsp;
    killInstanceRsp.set_code(int32_t(common::ErrorCode::ERR_NONE));
    EXPECT_CALL(*funcAgentMgr_, KillInstance).WillRepeatedly(Return(killInstanceRsp));

    ASSERT_TRUE(mockObserver_ != nullptr);
    EXPECT_CALL(*mockObserver_, GetFuncMeta).WillOnce(Return(functionMeta_));
    resources::InstanceInfo instanceInfo;
    instanceInfo.set_functionproxyid("nodeID");
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));
    EXPECT_CALL(*mockObserver_, PutInstance).WillRepeatedly(Return(Status::OK()));
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindControlInterfaceClientManager(clientManager);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto parentStateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto parentInfo = GenParentInstanceInfo("parent", actor->GetAID(), "tenant001");
    EXPECT_CALL(*parentStateMachine, GetInstanceInfo).WillRepeatedly(Return(parentInfo));
    EXPECT_CALL(*instanceControlView, GetInstance("GeneratedInstanceID"))
        .WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*instanceControlView, GetInstance("parent"))
        .WillRepeatedly(Return(parentStateMachine));
    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, mockObserver_);
    instanceCtrl->BindInternalIAM(mockInternalIAM_);
    instanceCtrl->BindFunctionAgentMgr(funcAgentMgr_);
    scheduleReqA->mutable_instance()->set_parentfunctionproxyaid(actor->GetAID());

    EXPECT_CALL(*instanceControlView, TryGenerateNewInstance)
        .WillOnce(DoAll(Invoke([genStates, scheduleReqA](const std::shared_ptr<messages::ScheduleRequest> &req) {
            YRLOG_INFO("mocked TryGenerateNewInstance {}", req->requestid());
            req->mutable_instance()->set_instanceid("GeneratedInstanceID");
            return genStates;
        })));

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_)).WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, "" }));
    EXPECT_CALL(*scheduler, ScheduleConfirm).Times(1);
    instanceCtrl->BindScheduler(scheduler);
    // call method Schedule so that method CreateInstanceClient would be called afterwards.
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();

    fut.OnComplete([instanceCtrl]() {
        instanceCtrl->UpdateInstanceStatusPromise("GeneratedInstanceID", "runtimeExit info uploaded by runtimeManager");
    });
    auto result = instanceCtrl->Schedule(scheduleReqA, runtimePromise);
    // client returned is nullptr therefore code would be ERR_REQUEST_BETWEEN_RUNTIME_BUS;
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), (int32_t)StatusCode::SUCCESS);
    ASSERT_AWAIT_READY_FOR(notifyCalled.GetFuture(), 60000);
    EXPECT_EQ(static_cast<StatusCode>(notifyCalled.GetFuture().Get().code()),
              StatusCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS);

    auto alarmInfoMap = functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().GetAlarmInfoMap();
    ASSERT_TRUE(alarmInfoMap.find("YuanrongInstanceCreateFailure00001-requestID_CreateInstanceClientTest") !=
                alarmInfoMap.end());
    const auto &alarmInfo = alarmInfoMap.at("YuanrongInstanceCreateFailure00001-requestID_CreateInstanceClientTest");
    EXPECT_EQ(alarmInfo.alarmName, "yr_instance_create_failure_alarm");
    EXPECT_EQ(alarmInfo.cause, "unable to init runtime, because connect runtime failed and not received exit info of runtime");
    EXPECT_EQ(std::get<std::string>(alarmInfo.customOptions.at("resource_id")), "GeneratedInstanceID");
    EXPECT_EQ(std::get<std::string>(alarmInfo.customOptions.at("request_id")), requestID);
    EXPECT_EQ(std::get<std::string>(alarmInfo.customOptions.at("runtime_id")), runtimeIDA);
    EXPECT_EQ(std::get<std::string>(alarmInfo.customOptions.at("stage")), "check_readiness");
    EXPECT_EQ(std::get<int64_t>(alarmInfo.customOptions.at("status_code")),
              int64_t(StatusCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS));
}

TEST_F(InstanceCtrlTest, TransitionStateToSchedulingFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor1", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto instanceCtrl = InstanceCtrl(actor);
    auto observer = std::make_shared<MockObserver>();
    instanceCtrl.Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillOnce(Return(functionMeta_));
    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));

    auto stateMachine = std::make_shared<MockInstanceStateMachine>(proxyID1);
    auto &mockStateMachine = *stateMachine;
    GeneratedInstanceStates genStates{ "DesignatedInstanceID", InstanceState::NEW, false };
    EXPECT_CALL(*instanceControlView, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView, GetInstance).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl).WillOnce(Return(NONE_RESULT));
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));

    auto result = instanceCtrl.Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), StatusCode::ERR_ETCD_OPERATION_ERROR);
}

TEST_F(InstanceCtrlTest, UpdateInstanceStatusWithoutStateMachine)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor1", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto instanceCtrl = InstanceCtrl(actor);
    auto observer = std::make_shared<MockObserver>();
    instanceCtrl.Start(nullptr, mockResourceViewMgr_, observer);
    litebus::Future<Status> status =
        instanceCtrl.UpdateInstanceStatus(GenInstanceStatusInfo("this is a never exist id", 0, "ok"));
    EXPECT_EQ(status.Get().StatusCode(), StatusCode::ERR_INSTANCE_NOT_FOUND);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView, GetInstance("instanceWithScheduling")).WillOnce(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetInstanceState).WillOnce(Return(InstanceState::SCHEDULING));
    status =
        instanceCtrl.UpdateInstanceStatus(GenInstanceStatusInfo("instanceWithScheduling", 132, "abnormal"));
    ASSERT_AWAIT_READY(status);
    EXPECT_EQ(status.Get().StatusCode(), StatusCode::ERR_INNER_SYSTEM_ERROR);
}

TEST_F(InstanceCtrlTest, CreateRateLimitTest_Rescheduled)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor1", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView, GetInstance).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*instanceControlView, IsRescheduledRequest).WillRepeatedly(Return(true)); // rescheduled

    // tenantA create rate limited
    const std::string tenantID = "tenantA";
    const std::string instanceID = "instanceA";
    const std::string funcAgentID = "funcAgentA";
    const std::string parentID = "parentID";
    const std::string function = "default/0-test-helloWorld/$latest";
    InstanceState state = InstanceState::NEW;
    auto insInfo = GenInstanceInfo(instanceID, funcAgentID, function, state);
    insInfo.set_functionproxyid("nodeID");
    insInfo.set_tenantid(tenantID);
    insInfo.set_parentid(parentID);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(insInfo);
    auto notLimited = actor->DoRateLimit(scheduleReq);
    EXPECT_TRUE(notLimited);
}

TEST_F(InstanceCtrlTest, TenantCreateRateLimitTest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor1", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView, GetInstance).WillRepeatedly(Return(stateMachine));

    // tenantA create rate limited
    const std::string tenantIDA = "tenantA";
    const std::string instanceIDA = "instanceA";
    const std::string funcAgentIDA = "funcAgentA";
    const std::string parentID = "parentID";
    const std::string function = "default/0-test-helloWorld/$latest";
    InstanceState state = InstanceState::NEW;
    auto insInfoA = GenInstanceInfo(instanceIDA, funcAgentIDA, function, state);
    insInfoA.set_functionproxyid("nodeID");
    insInfoA.set_tenantid(tenantIDA);
    insInfoA.set_parentid(parentID);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(insInfoA);

    bool notLimited;
    for (int i = 0; i < 10; i++) {
        notLimited = actor->DoRateLimit(scheduleReq);
        EXPECT_TRUE(notLimited);
    }
    notLimited = actor->DoRateLimit(scheduleReq);
    EXPECT_TRUE(!notLimited);

    // tenantB on same node
    const std::string tenantIDB = "tenantB";
    const std::string instanceIDB = "instanceB";
    const std::string funcAgentIDB = "funcAgentB";
    auto insInfoB = GenInstanceInfo(instanceIDB, funcAgentIDB, function, state);
    insInfoB.set_functionproxyid("nodeID");
    insInfoB.set_tenantid(tenantIDB);
    insInfoB.set_parentid(parentID);
    auto scheduleReq2 = std::make_shared<messages::ScheduleRequest>();
    scheduleReq2->mutable_instance()->CopyFrom(insInfoB);
    notLimited = actor->DoRateLimit(scheduleReq2);
    EXPECT_TRUE(notLimited);

    // system function tenant
    const std::string instanceID = "0-system-faascontroller-0";
    const std::string funcAgentID = "funcAgentA";
    const std::string systemFunction = "0/0-system-faascontroller/$latest";
    auto systemInsInfo = GenInstanceInfo(instanceID, funcAgentID, systemFunction, state);
    systemInsInfo.set_functionproxyid("nodeID");
    systemInsInfo.set_tenantid("0");
    systemInsInfo.set_parentid("");
    auto scheduleReq3 = std::make_shared<messages::ScheduleRequest>();
    scheduleReq3->mutable_instance()->CopyFrom(systemInsInfo);

    for (int i = 0; i < 10; i++) {
        notLimited = actor->DoRateLimit(scheduleReq3);
        EXPECT_TRUE(notLimited);
    }
    notLimited = actor->DoRateLimit(scheduleReq3); // not limit
    EXPECT_TRUE(notLimited);
}

TEST_F(InstanceCtrlTest, KillInvalidSignal)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor1", "nodeID", instanceCtrlConfig);
    auto instanceCtrl = InstanceCtrl(actor);
    auto observer = std::make_shared<MockObserver>();
    instanceCtrl.Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);

    auto killReq = GenKillRequest(instanceID1, -1);

    auto future = instanceCtrl.Kill(instanceID1, killReq);
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get().code(), common::ErrorCode::ERR_PARAM_INVALID);
}

/**
 * Feature: instance ctrl.
 * Description: instance ctrl sync instances successfully and not recover any instance.
 * Steps:
 * 1. Mock GetAgentInstanceInfoByID return instanceInfoMap.
 * 2. send request of sync instances.
 * Expectation: don't invoke Reschedule method to functionAgentMgr and check consistency successfully.
 */
TEST_F(InstanceCtrlTest, SyncInstanceNoRecoverSuccess)
{
    auto killResponse = GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID");

    EXPECT_CALL(*funcAgentMgr_.get(), KillInstance(testing::_, testing::_, testing::_)).Times(0);

    litebus::Future<std::string> observerFuncAgentID;
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfoMap.insert({ "instance1", instanceInfo });
    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_)).WillOnce(test::Return(instanceInfoMap));

    FunctionMeta functionMeta;
    functionMeta.codeMetaData.storageType = "local";
    EXPECT_CALL(*mockObserver_.get(), GetFuncMeta(testing::_)).Times(0);

    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");
    auto instances = resourceUnit->mutable_instances();
    instances->insert({ "instance1", instanceInfo });

    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);

    ASSERT_AWAIT_READY(syncRet);
    EXPECT_EQ(syncRet.Get().StatusCode(), StatusCode::SUCCESS);
}
TEST_F(InstanceCtrlTest, SyncInstanceRecoverFailed)
{
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    instanceInfoMap.insert({ "instance1", instanceInfo });
    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_)).WillOnce(test::Return(instanceInfoMap));

    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");

    std::string state = "state";
    auto distributedCacheClient = std::make_shared<MockDistributedCacheClient>();
    EXPECT_CALL(*distributedCacheClient, Init).WillOnce(Return(Status::OK()));
    auto stateClient = std::make_shared<StateClient>(distributedCacheClient);
    auto stateActor = std::make_shared<function_proxy::StateActor>(stateClient);
    litebus::Spawn(stateActor);

    function_proxy::StateHandler::BindStateActor(stateActor);

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*distributedCacheClient, Get(Matcher<const std::string &>("instance1"), Matcher<std::string &>(Eq(""))))
        .WillRepeatedly(DoAll(SetArgReferee<1>(state), Return(Status::OK())));
    runtime::RecoverResponse recoverRsp;
    recoverRsp.set_code(common::ErrorCode::ERR_NONE);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_scheduletimes(0);
    scheduleReq->mutable_instance()->set_deploytimes(0);
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN", context);
    EXPECT_CALL(*instanceControlView_, GetInstance(testing::_)).WillRepeatedly(Return(nullptr));
    EXPECT_CALL(*stateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, TransitionToImpl).WillRepeatedly(Return(SCHEDULING_RESULT));

    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);
    ASSERT_AWAIT_SET(syncRet);
    EXPECT_EQ(syncRet.GetErrorCode(), static_cast<int32_t>(StatusCode::LS_SYNC_RESCHEDULE_INSTANCE_FAIL));
    litebus::Terminate(stateActor->GetAID());
    litebus::Await(stateActor->GetAID());
}
/**
 * Feature: instance ctrl.
 * Description: instance ctrl sync instances successfully and recover an instance.
 * Steps:
 * 1. Mock GetAgentInstanceInfoByID return instanceInfoMap.
 * 2. send request of sync instances.
 * Expectation: invoke Recover method second times and check consistency successfully.
 */
TEST_F(InstanceCtrlTest, SyncInstanceRecoverSuccess)
{
    InternalIAM::Param internalIAM_Param;
    internalIAM_Param.isEnableIAM = true;
    auto mockInternalIAM = std::make_shared<MockInternalIAM>(internalIAM_Param);
    instanceCtrlWithMockObserver_->BindInternalIAM(mockInternalIAM);
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfo.set_tenantid("tenant0");
    instanceInfo.set_runtimeid("runtime-1");
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::CREATING));
    instanceInfo.add_args();
    instanceInfoMap.insert({ "instance1", instanceInfo });
    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_)).WillOnce(test::Return(instanceInfoMap));

    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");

    std::string state = "state";
    auto distributedCacheClient = std::make_shared<MockDistributedCacheClient>();
    EXPECT_CALL(*distributedCacheClient, Init).WillOnce(Return(Status::OK()));
    auto stateClient = std::make_shared<StateClient>(distributedCacheClient);

    auto stateActor = std::make_shared<function_proxy::StateActor>(stateClient);
    litebus::Spawn(stateActor);
    function_proxy::StateHandler::BindStateActor(stateActor);

    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));
    auto unit = std::make_shared<resource_view::ResourceUnit>();
    EXPECT_CALL(*primary, GetFullResourceView).WillRepeatedly(Return(unit));
    instanceCtrlWithMockObserver_->BindResourceView(resourceViewMgr);

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));

    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    messages::ScheduleResponse scheduleResponse;
    scheduleResponse.set_requestid("requestID");
    scheduleResponse.set_message("SUCCESS");
    scheduleResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*localSchedSrv, ForwardSchedule).WillRepeatedly(Return(scheduleResponse));
    instanceCtrlWithMockObserver_->BindLocalSchedSrv(localSchedSrv);
    EXPECT_CALL(*funcAgentMgr_.get(), KillInstance(testing::_, testing::_, testing::_))
        .WillRepeatedly(
            Return(GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID")));
    EXPECT_CALL(*distributedCacheClient, Get(Matcher<const std::string &>("instance1"), Matcher<std::string &>(Eq(""))))
        .WillRepeatedly(DoAll(SetArgReferee<1>(state), Return(Status::OK())));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_scheduletimes(1);
    scheduleReq->mutable_instance()->set_deploytimes(0);
    scheduleReq->mutable_instance()->set_tenantid("tenant001");
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::CREATING));
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN", context);
    EXPECT_CALL(*instanceControlView_, GetInstance(testing::_)).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, TransitionToImpl).WillRepeatedly(Return(SCHEDULING_RESULT));
    EXPECT_CALL(*stateMachine, ReleaseOwner).WillRepeatedly(Return());
    EXPECT_CALL(*stateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(*stateMachine, SetScheduleTimes).WillRepeatedly(Invoke([scheduleReq](const int32_t &times) {
        scheduleReq->mutable_instance()->set_scheduletimes(times);
    }));
    EXPECT_CALL(*stateMachine, GetDeployTimes).WillRepeatedly(Invoke([scheduleReq]() -> int32_t {
        return scheduleReq->instance().deploytimes();
    }));
    EXPECT_CALL(*stateMachine, GetScheduleTimes).WillRepeatedly(Invoke([scheduleReq]() -> int32_t {
        return scheduleReq->instance().scheduletimes();
    }));
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));
    EXPECT_CALL(*stateMachine, AddStateChangeCallback).WillOnce(Return());
    EXPECT_CALL(*stateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));
    EXPECT_CALL(*mockObserver_.get(), GetFuncMeta(testing::_)).WillOnce(test::Return(FunctionMeta{}));
    messages::DeployInstanceResponse deployInstanceResponse;
    deployInstanceResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*funcAgentMgr_.get(), DeployInstance(testing::_, testing::_)).WillOnce(Return(deployInstanceResponse));
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewControlInterfacePosixClient(_, _, _, _, _, _))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClient, Readiness).WillRepeatedly(Return(Status::OK()));
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, Heartbeat).WillRepeatedly(Return(Status::OK()));
    litebus::Future<runtime::CallResponse> sendRet;
    runtime::CallResponse callRsp;
    callRsp.set_code(common::ERR_NONE);
    callRsp.set_message("call runtime failed! client may already closed");
    sendRet.SetValue(callRsp);
    EXPECT_CALL(*mockSharedClient, InitCallWrapper).WillOnce(Return(sendRet));

    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);

    ASSERT_AWAIT_READY(syncRet);
    EXPECT_EQ(syncRet.Get().StatusCode(), StatusCode::SUCCESS);
    litebus::Terminate(stateActor->GetAID());
    litebus::Await(stateActor->GetAID());
}

/**
 * Feature: instance ctrl.
 * Description: instance ctrl sync instances successfully and recover many instances.
 * Steps:
 * Expectation: invoke Recover method second times and check consistency successfully.
 */
TEST_F(InstanceCtrlTest, SyncInstanceResheduleManySuccess)
{
    litebus::Future<std::string> observerFuncAgentIDFuture;
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo1, instanceInfo2, instanceInfo3;
    instanceInfo1.set_instanceid("instance1");
    instanceInfo1.set_function("function1");
    instanceInfo1.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));

    instanceInfo2.set_function("function2");
    instanceInfo2.set_instanceid("instance2");
    instanceInfo2.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));

    instanceInfo3.set_function("function3");
    instanceInfo3.set_instanceid("instance3");
    instanceInfo3.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));

    instanceInfoMap.insert({ "instance1", instanceInfo1 });
    instanceInfoMap.insert({ "instance2", instanceInfo2 });
    instanceInfoMap.insert({ "instance3", instanceInfo3 });
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*primary, AddInstances).WillRepeatedly(Return(Status::OK()));
    auto unit = std::make_shared<resource_view::ResourceUnit>();
    EXPECT_CALL(*primary, GetFullResourceView).WillRepeatedly(Return(unit));
    instanceCtrlWithMockObserver_->BindResourceView(resourceViewMgr);
    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    messages::ScheduleResponse scheduleResponse;
    scheduleResponse.set_requestid("requestID");
    scheduleResponse.set_message("SUCCESS");
    scheduleResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*localSchedSrv, ForwardSchedule).WillRepeatedly(Return(scheduleResponse));
    instanceCtrlWithMockObserver_->BindLocalSchedSrv(localSchedSrv);

    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_)).WillOnce(test::Return(instanceInfoMap));

    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*funcAgentMgr_.get(), KillInstance(testing::_, testing::_, testing::_))
        .WillRepeatedly(
            Return(GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID")));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_scheduletimes(1);
    scheduleReq->mutable_instance()->set_deploytimes(0);
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN", context);
    EXPECT_CALL(*instanceControlView_, GetInstance(testing::_)).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, TransitionToImpl).WillRepeatedly(Return(SCHEDULING_RESULT));
    EXPECT_CALL(*stateMachine, ReleaseOwner).WillRepeatedly(Return());
    EXPECT_CALL(*stateMachine, IncreaseScheduleRound).Times(0);
    EXPECT_CALL(*stateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(*stateMachine, SetScheduleTimes).WillRepeatedly(Invoke([scheduleReq](const int32_t &scheduleTimes) {
        scheduleReq->mutable_instance()->set_scheduletimes(scheduleTimes);
    }));
    EXPECT_CALL(*stateMachine, GetDeployTimes).WillRepeatedly(Invoke([scheduleReq]() -> int32_t {
        return scheduleReq->instance().deploytimes();
    }));
    EXPECT_CALL(*stateMachine, GetScheduleTimes).WillRepeatedly(Invoke([scheduleReq]() -> int32_t {
        return scheduleReq->instance().scheduletimes();
    }));
    EXPECT_CALL(*stateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    std::string state = "state";
    auto distributedCacheClient = std::make_shared<MockDistributedCacheClient>();

    EXPECT_CALL(*distributedCacheClient, Init).WillOnce(Return(Status::OK()));
    auto stateClient = std::make_shared<StateClient>(distributedCacheClient);
    auto stateActor = std::make_shared<function_proxy::StateActor>(stateClient);
    litebus::Spawn(stateActor);
    function_proxy::StateHandler::BindStateActor(stateActor);

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*distributedCacheClient, Get(testing::_, Matcher<std::string &>(Eq(""))))
        .WillRepeatedly(DoAll(SetArgReferee<1>(state), Return(Status::OK())));
    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");
    auto instances = resourceUnit->mutable_instances();
    instances->insert({ "instance3", instanceInfo3 });

    EXPECT_CALL(*mockObserver_.get(), GetFuncMeta(testing::_)).WillRepeatedly(test::Return(FunctionMeta{}));
    EXPECT_CALL(*mockObserver_.get(), IsSystemFunction(testing::_)).WillRepeatedly(test::Return(false));
    messages::DeployInstanceResponse deployInstanceResponse;
    deployInstanceResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*funcAgentMgr_.get(), DeployInstance(testing::_, testing::_))
        .WillRepeatedly(Return(deployInstanceResponse));

    EXPECT_CALL(*mockSharedClientManagerProxy_, NewControlInterfacePosixClient(_, _, _, _, _, _))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClient, Readiness).WillRepeatedly(Return(Status::OK()));
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, Heartbeat).WillRepeatedly(Return(Status::OK()));
    litebus::Future<runtime::CallResponse> sendRet;
    runtime::CallResponse callRsp;
    callRsp.set_code(common::ERR_NONE);
    callRsp.set_message("call runtime failed! client may already closed");
    sendRet.SetValue(callRsp);

    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);

    ASSERT_AWAIT_READY(syncRet);
    EXPECT_EQ(syncRet.Get().StatusCode(), StatusCode::SUCCESS);

    litebus::Terminate(stateActor->GetAID());
    litebus::Await(stateActor->GetAID());
}

/**
 * Feature: instance ctrl.
 * Description: instance ctrl sync instances successfully and recover many instances.
 * Steps:
 * Expectation: invoke Recover method second times and check consistency successfully.
 */
TEST_F(InstanceCtrlTest, PutFailedInstanceStatusByAgentIdSuccess)
{
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo1, instanceInfo2, instanceInfo3;
    instanceInfo1.set_instanceid("instance1");
    instanceInfo1.set_function("function1");
    instanceInfo1.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));

    instanceInfo2.set_instanceid("instance2");
    instanceInfo2.set_function("function2");
    instanceInfo2.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));

    instanceInfo3.set_instanceid("instance3");
    instanceInfo3.set_function("function3");
    instanceInfo3.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));

    instanceInfoMap.insert({ "instance1", instanceInfo1 });
    instanceInfoMap.insert({ "instance2", instanceInfo2 });
    instanceInfoMap.insert({ "instance3", instanceInfo3 });
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_))
        .Times(4)
        .WillOnce(test::Return(litebus::None()))
        .WillRepeatedly(test::Return(instanceInfoMap));
    EXPECT_CALL(*instanceControlView_, GetInstance)
        .Times(7)
        .WillOnce(Return(nullptr))
        .WillRepeatedly(Return(stateMachine));
    instanceCtrlWithMockObserver_->PutFailedInstanceStatusByAgentId("agent");
    instanceCtrlWithMockObserver_->PutFailedInstanceStatusByAgentId("agent");
    instanceCtrlWithMockObserver_->PutFailedInstanceStatusByAgentId("agent");
    testing::internal::SleepMilliseconds(10);  // leave time for runtimeRecoverEnable to stay being true.
    instanceCtrlWithMockObserver_->PutFailedInstanceStatusByAgentId("agent");
}

/**
 * Feature RescheduleTest:
 * Description try to reconnect runtime, log connection info;
 * Steps:
 * 1. mock mockSharedClientManagerProxy_ DeleteClient method to return Status::OK();
 * 2. mock resourceViewMgr_ DeleteInstances method to return Status::OK();
 * 3. mock MockLocalSchedSrv ForwardSchedule method to return ScheduleResponse with StatusCode::SUCCESS;
 * 4. mock MockFunctionAgentMgr KillInstance method to return killInstanceResponse with StatusCode::SUCCESS;
 * 5. set scheduleReq scheduleTimes 0, deployTimes 0;.
 * 6. invoke instanceCtrl->Reschedule(Status(StatusCode::FAILED), scheduleReq) <Fail: times all 0>;
 * 7. set scheduleReq scheduleTimes 1, deployTimes 0;
 * 8. invoke instanceCtrl->Reschedule(Status(StatusCode::FAILED), scheduleReq) <Success ForwardSchedule success>;
 * 9. mock MockLocalSchedSrv ForwardSchedule method to return ScheduleResponse with StatusCode::FAIL;
 * 10. invoke instanceCtrl->Reschedule(Status(StatusCode::FAILED), scheduleReq) <Fail ForwardSchedule fail>;
 */
TEST_F(InstanceCtrlTest, RescheduleTest)
{
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient).WillRepeatedly(Return(Status::OK()));
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    functionMeta_.codeMetaData.storageType = "S3";
    instanceCtrl->BindInternalIAM(mockInternalIAM_);
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));
    auto unit = std::make_shared<resource_view::ResourceUnit>();
    EXPECT_CALL(*primary, GetFullResourceView).WillRepeatedly(Return(unit));
    instanceCtrl->BindResourceView(resourceViewMgr);

    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    messages::ScheduleResponse scheduleResponse;
    scheduleResponse.set_requestid("requestID");
    scheduleResponse.set_message("SUCCESS");
    scheduleResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*localSchedSrv, ForwardSchedule).WillRepeatedly(Return(scheduleResponse));
    instanceCtrl->BindLocalSchedSrv(localSchedSrv);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl).WillRepeatedly(Return(SCHEDULING_RESULT));
    EXPECT_CALL(mockStateMachine, ReleaseOwner).WillRepeatedly(Return());
    EXPECT_CALL(mockStateMachine, IncreaseScheduleRound).Times(0);
    EXPECT_CALL(mockStateMachine, GetInstanceState).WillRepeatedly(Return(InstanceState::CREATING));
    EXPECT_CALL(mockStateMachine, SetScheduleTimes).WillRepeatedly(Invoke([scheduleReq](const int32_t &scheduleTimes) {
        scheduleReq->mutable_instance()->set_scheduletimes(scheduleTimes);
    }));
    EXPECT_CALL(mockStateMachine, GetDeployTimes).WillRepeatedly(Invoke([scheduleReq]() -> int32_t {
        return scheduleReq->instance().deploytimes();
    }));
    EXPECT_CALL(mockStateMachine, GetScheduleTimes).WillRepeatedly(Invoke([scheduleReq]() -> int32_t {
        return scheduleReq->instance().scheduletimes();
    }));
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));
    instanceCtrl->BindInstanceControlView(instanceControlView_);

    auto metaClient = MetaStoreClient::Create({ .etcdAddress = metaStoreServerHost_ });
    auto functionAgentMgr = std::make_shared<MockFunctionAgentMgr>("funcAgentMgr", metaClient);

    messages::KillInstanceResponse killInstanceResponse;
    killInstanceResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*functionAgentMgr, KillInstance).WillRepeatedly(Return(killInstanceResponse));
    instanceCtrl->BindFunctionAgentMgr(functionAgentMgr);

    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_scheduletimes(0);
    scheduleReq->mutable_instance()->set_deploytimes(0);

    auto result = instanceCtrl->Reschedule(Status::OK(), scheduleReq);
    ASSERT_AWAIT_READY(result);
    EXPECT_TRUE(result.Get().IsOk());

    auto rescheduleResult = instanceCtrl->Reschedule(Status(StatusCode::FAILED), scheduleReq);
    ASSERT_AWAIT_READY(rescheduleResult);
    EXPECT_FALSE(rescheduleResult.Get().IsOk());

    scheduleReq->mutable_instance()->set_scheduletimes(1);
    scheduleReq->mutable_instance()->set_deploytimes(0);
    rescheduleResult = instanceCtrl->Reschedule(Status(StatusCode::FAILED), scheduleReq);
    ASSERT_AWAIT_READY(rescheduleResult);
    EXPECT_TRUE(rescheduleResult.Get().IsOk());

    ::messages::ScheduleResponse scheduleResponseFail = scheduleResponse;
    scheduleResponseFail.set_code(StatusCode::FAILED);
    EXPECT_CALL(*localSchedSrv, ForwardSchedule)
        .WillOnce(Return(scheduleResponseFail))
        .WillOnce(Return(scheduleResponseFail));
    instanceCtrl->BindLocalSchedSrv(localSchedSrv);
    scheduleReq->mutable_instance()->set_scheduletimes(2);
    scheduleReq->mutable_instance()->set_deploytimes(0);
    rescheduleResult = instanceCtrl->Reschedule(Status(StatusCode::FAILED), scheduleReq);
    ASSERT_AWAIT_READY(rescheduleResult);
    EXPECT_FALSE(rescheduleResult.Get().IsOk());

    EXPECT_CALL(*localSchedSrv, ForwardSchedule)
        .WillOnce(Return(scheduleResponseFail))
        .WillOnce(Return(scheduleResponseFail))
        .WillOnce(Return(scheduleResponse));
    instanceCtrl->BindLocalSchedSrv(localSchedSrv);
    scheduleReq->mutable_instance()->set_scheduletimes(3);
    scheduleReq->mutable_instance()->set_deploytimes(0);
    rescheduleResult = instanceCtrl->Reschedule(Status(StatusCode::FAILED), scheduleReq);
    ASSERT_AWAIT_READY(rescheduleResult);
    EXPECT_TRUE(rescheduleResult.Get().IsOk());
}

TEST_F(InstanceCtrlTest, ShutDownInstanceTest)
{
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient).WillRepeatedly(Return(Status::OK()));
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);

    resources::InstanceInfo instance;
    instance.set_instanceid(instanceID);
    instance.set_requestid("request");
    instance.set_functionproxyid("nodeID");
    instance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::SCHEDULE_FAILED));
    instance.mutable_instancestatus()->set_errcode(static_cast<int32_t>(StatusCode::ERR_RESOURCE_NOT_ENOUGH));
    instance.mutable_instancestatus()->set_msg("state changed");

    runtime::ShutdownResponse shutdownResponse;
    shutdownResponse.set_code(common::ErrorCode::ERR_NONE);

    EXPECT_CALL(*mockSharedClient, Shutdown).WillOnce(Return(shutdownResponse));
    auto createCallResultPromise =
        std::make_shared<litebus::Promise<std::shared_ptr<functionsystem::CallResult>>>();
    actor->syncCreateCallResultPromises_[instanceID] = createCallResultPromise;
    auto fut1 = actor->ShutDownInstance(instance, instanceCtrlConfig.runtimeConfig.runtimeShutdownTimeoutSeconds);
    ASSERT_AWAIT_READY(fut1);
    ASSERT_AWAIT_READY(createCallResultPromise->GetFuture());
    EXPECT_TRUE(actor->syncCreateCallResultPromises_.find(instanceID) == actor->syncCreateCallResultPromises_.end());
    EXPECT_EQ(fut1.Get().StatusCode(), StatusCode::SUCCESS);

    shutdownResponse.set_code(common::ErrorCode::ERR_INNER_COMMUNICATION);
    EXPECT_CALL(*mockSharedClient, Shutdown).WillOnce(Return(shutdownResponse));
    auto fut2 = actor->ShutDownInstance(instance, instanceCtrlConfig.runtimeConfig.runtimeShutdownTimeoutSeconds);

    ASSERT_AWAIT_READY(fut2);
    EXPECT_EQ(fut2.Get().StatusCode(), StatusCode::SUCCESS);

    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(nullptr));
    auto fut3 = actor->ShutDownInstance(instance, instanceCtrlConfig.runtimeConfig.runtimeShutdownTimeoutSeconds);
    ASSERT_AWAIT_READY(fut3);
    EXPECT_EQ(fut3.Get().StatusCode(), StatusCode::SUCCESS);
}

/**
 * Test schedule instance, local resource not enough, and remote also resource not enough
 * Steps:
 * 1. MockObserver (GetFuncMeta() => defaultMeta / IsSystemFunction() => False)
 * 2. MockResourceView (DeleteInstances => OK)
 * 3. MockInstanceCtrlActor (SendCallResult => record the callResult request)
 * 4. MockLocalSchedSrv (ForwardSchedule => return RESOURCE_NOT_ENOUGH schedule response)
 * 5. MockStateMachine (TransitionTo => record the new state, return NEW)
 * 6. MockInstanceCtrlView (NewInstance => return instanceID / GetInstance => return mockStateMachine in step 5)
 * 7. MockScheduler (ScheduleDecision => return RESOURCE_NOT_ENOUGH)
 * 8. start instanceCtrl with above mockers
 * 9. send schedule request
 *
 * Expectations:
 * 1. get ScheduleResponse with code SUCCESS
 * 2. mockStateMachine state == Scheduling
 * 3. sendCallResult is called, and callResult code is ERR_RESOURCE_NOT_ENOUGH
 */
TEST_F(InstanceCtrlTest, CreateLocalNotEnoughAndRemoteNotEnough)
{
    auto observer = std::make_shared<MockObserver>();
    ASSERT_IF_NULL(observer);
    EXPECT_CALL(*observer, GetFuncMeta).WillOnce(Return(functionMeta_));
    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));

    auto actor = std::make_shared<MockInstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    litebus::Future<std::shared_ptr<core_service::CallResult>> callResult;
    core_service::CallResultAck callResultAck;
    EXPECT_CALL(*actor, MockSendCallResult).WillOnce(testing::DoAll(FutureArg<3>(&callResult), Return(callResultAck)));

    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    ASSERT_IF_NULL(instanceCtrl);

    instanceCtrl->Start(nullptr, resourceViewMgr, observer);
    instanceCtrl->BindInternalIAM(mockInternalIAM_);

    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    messages::ScheduleResponse scheduleResponse;
    scheduleResponse.set_requestid("request-id-CreateLocalNotEnoughAndRemoteNotEnough");
    scheduleResponse.set_message("resource not enough in remote also");
    scheduleResponse.set_code(StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_CALL(*localSchedSrv, ForwardSchedule).WillRepeatedly(Return(scheduleResponse));

    instanceCtrl->BindLocalSchedSrv(localSchedSrv);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");

    auto &mockStateMachine = *stateMachine;
    InstanceState mockStateMachineState;
    std::string mockStateMachineInstanceStatusMsg;

    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl)
        .WillRepeatedly(testing::DoAll(SaveArg<0>(&mockStateMachineState),
                                       SaveArg<1>(&mockStateMachineInstanceStatusMsg), Return(NEW_RESULT)));

    EXPECT_CALL(mockStateMachine, GetInstanceState())
        .WillOnce(Return(InstanceState::SCHEDULING))
        .WillOnce(Return(InstanceState::SCHEDULING));
    EXPECT_CALL(mockStateMachine, ReleaseOwner).WillRepeatedly(Return());
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    GeneratedInstanceStates genStates{ "instance-id-CreateLocalNotEnoughAndRemoteNotEnough", InstanceState::NEW,
                                       false };
    EXPECT_CALL(*instanceControlView_, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(nullptr)).WillRepeatedly(Return(stateMachine));
    instanceCtrl->BindInstanceControlView(instanceControlView_);

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_))
        .WillOnce(Return(ScheduleResult{ "", StatusCode::RESOURCE_NOT_ENOUGH, "" }));
    instanceCtrl->BindScheduler(scheduler);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("request-id-CreateLocalNotEnoughAndRemoteNotEnough");
    scheduleReq->set_traceid("trace-id-CreateLocalNotEnoughAndRemoteNotEnough");
    scheduleReq->mutable_instance()->set_instanceid("instance-id-CreateLocalNotEnoughAndRemoteNotEnough");
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    auto runtimeFuture = runtimePromise->GetFuture();
    ASSERT_AWAIT_READY(runtimeFuture);
    YRLOG_INFO("Result: {}", result.Get().SerializeAsString());
    EXPECT_EQ(result.Get().code(), (int32_t)StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_EQ(runtimeFuture.Get().code(), 0);
    EXPECT_EQ(runtimeFuture.Get().instanceid(), "instance-id-CreateLocalNotEnoughAndRemoteNotEnough");
    EXPECT_EQ(static_cast<int32_t>(mockStateMachineState), static_cast<int32_t>(InstanceState::SCHEDULE_FAILED));

    ASSERT_AWAIT_READY(callResult);

    EXPECT_EQ(callResult.Get()->code(), static_cast<int32_t>(StatusCode::ERR_RESOURCE_NOT_ENOUGH));
    EXPECT_EQ(callResult.Get()->message(), "resource not enough in remote also");
}

/**
 * Test frontend create, local resource not enough, but remote resource enough
 * Steps:
 * 1. MockObserver (GetFuncMeta() => defaultMeta / IsSystemFunction() => False)
 * 2. MockResourceView (DeleteInstances => OK)
 * 3. MockInstanceCtrlActor (SendCallResult => record the callResult request)
 * 4. MockLocalSchedSrv (ForwardSchedule => return SUCCESS schedule response)
 * 5. MockStateMachine (TransitionTo => record the new state, return NEW)
 * 6. MockInstanceCtrlView (NewInstance => return instanceID / GetInstance => return mockStateMachine in step 5)
 * 7. MockScheduler (ScheduleDecision => return RESOURCE_NOT_ENOUGH)
 * 8. start instanceCtrl with above mockers
 * 9. send frontend schedule request and report the remote instance ready
 *
 * Expectations:
 * 1. frontend gets the accepted ScheduleResponse with code SUCCESS
 * 2. mockStateMachine state == Scheduling
 * 3. frontend ready callback remains registered and receives the remote ready CallResult
 */
TEST_F(InstanceCtrlTest, FrontendCreateLocalNotEnoughAndRemoteEnoughWaitsForReady)
{
    auto observer = std::make_shared<MockObserver>();
    ASSERT_IF_NULL(observer);
    EXPECT_CALL(*observer, GetFuncMeta).WillOnce(Return(functionMeta_));
    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));

    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));

    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);

    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    messages::ScheduleResponse scheduleResponse;
    scheduleResponse.set_requestid("request-id-CreateLocalNotEnoughAndRemoteEnough");
    scheduleResponse.set_message("everything is fine in remote");
    scheduleResponse.set_code(StatusCode::SUCCESS);
    litebus::Future<std::shared_ptr<messages::ScheduleRequest>> request;
    EXPECT_CALL(*localSchedSrv, ForwardSchedule).WillOnce(DoAll(FutureArg<0>(&request), Return(scheduleResponse)));

    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    ASSERT_IF_NULL(instanceCtrl);
    instanceCtrl->Start(nullptr, resourceViewMgr, observer);
    instanceCtrl->BindInternalIAM(mockInternalIAM_);

    instanceCtrl->BindLocalSchedSrv(localSchedSrv);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");

    auto &mockStateMachine = *stateMachine;
    InstanceState mockStateMachineState;
    std::string mockStateMachineInstanceStatusMsg;
    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));

    EXPECT_CALL(mockStateMachine, TransitionToImpl)
        .WillRepeatedly(testing::DoAll(SaveArg<0>(&mockStateMachineState),
                                       SaveArg<1>(&mockStateMachineInstanceStatusMsg), Return(NEW_RESULT)));

    EXPECT_CALL(mockStateMachine, ReleaseOwner).WillRepeatedly(Return());
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    GeneratedInstanceStates genStates{ "instance-id-CreateLocalNotEnoughAndRemoteEnough", InstanceState::NEW, false };
    EXPECT_CALL(*instanceControlView_, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(nullptr)).WillRepeatedly(Return(stateMachine));
    instanceCtrl->BindInstanceControlView(instanceControlView_);

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_))
        .WillOnce(Return(ScheduleResult{ "", StatusCode::RESOURCE_NOT_ENOUGH, "" }));
    instanceCtrl->BindScheduler(scheduler);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("request-id-CreateLocalNotEnoughAndRemoteEnough");
    scheduleReq->set_traceid("trace-id-CreateLocalNotEnoughAndRemoteEnough");
    scheduleReq->mutable_instance()->set_instanceid("instance-id-CreateLocalNotEnoughAndRemoteEnough");
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    int readyCallbackCount = 0;
    std::shared_ptr<functionsystem::CallResult> observedReadyResult;
    auto result = instanceCtrl->ScheduleFrontendAndWaitReady(
        scheduleReq, runtimePromise,
        [&readyCallbackCount, &observedReadyResult](
            const std::shared_ptr<functionsystem::CallResult> &callResult) {
            ++readyCallbackCount;
            observedReadyResult = callResult;
            return litebus::Future<CallResultAck>(CallResultAck());
        });
    ASSERT_AWAIT_READY(result);
    auto runtimeFuture = runtimePromise->GetFuture();
    ASSERT_AWAIT_READY(runtimeFuture);
    YRLOG_INFO("Result: {}", result.Get().SerializeAsString());
    EXPECT_EQ(result.Get().code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(runtimeFuture.Get().code(), 0);
    EXPECT_EQ(runtimeFuture.Get().instanceid(), "instance-id-CreateLocalNotEnoughAndRemoteEnough");
    EXPECT_EQ(mockStateMachineState, InstanceState::SCHEDULING);
    ASSERT_AWAIT_READY(request);

    auto readyResult = std::make_shared<functionsystem::CallResult>();
    readyResult->set_requestid(scheduleReq->requestid());
    readyResult->set_instanceid(scheduleReq->instance().instanceid());
    readyResult->set_code(common::ERR_NONE);
    auto readyAck = litebus::Async(actor->GetAID(), &InstanceCtrlActor::SendCallResult,
                                   scheduleReq->instance().instanceid(), "", "", readyResult);
    ASSERT_AWAIT_READY(readyAck);
    EXPECT_EQ(readyCallbackCount, 1);
    EXPECT_EQ(observedReadyResult, readyResult);
}

/**
 * Test schedule instance, local resource not enough, but local is not the first scheduler, so won't forward
 * Steps:
 * 1. MockObserver (GetFuncMeta() => defaultMeta / IsSystemFunction() => False)
 * 2. MockResourceView (DeleteInstances => OK)
 * 3. MockInstanceCtrlActor (SendCallResult => expect called 0 times)
 * 4. MockLocalSchedSrv (ForwardSchedule => expect called 0 times)
 * 5. MockStateMachine (TransitionTo => record the new state, return SCHEDULING)
 * 6. MockInstanceCtrlView (NewInstance => return instanceID / GetInstance => return mockStateMachine in step 5)
 * 7. MockScheduler (ScheduleDecision => return RESOURCE_NOT_ENOUGH)
 * 8. start instanceCtrl with above mockers
 * 9. send schedule request
 *
 * Expectations:
 * 1. get ScheduleResponse with code SUCCESS
 * 2. mockStateMachine state == Scheduling
 * 3. ForwardSchedule/SendCallResult expect 0 times calls
 */
TEST_F(InstanceCtrlTest, CreateLocalNotEnoughButNotForward)
{
    auto observer = std::make_shared<MockObserver>();
    ASSERT_IF_NULL(observer);
    EXPECT_CALL(*observer, GetFuncMeta).WillOnce(Return(functionMeta_));
    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));

    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));

    auto actor = std::make_shared<MockInstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    EXPECT_CALL(*actor, MockSendCallResult).Times(0);

    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    ASSERT_IF_NULL(instanceCtrl);
    instanceCtrl->Start(nullptr, resourceViewMgr, observer);
    instanceCtrl->BindInternalIAM(mockInternalIAM_);

    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    EXPECT_CALL(*localSchedSrv, ForwardSchedule).Times(0);

    instanceCtrl->BindLocalSchedSrv(localSchedSrv);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");

    auto &mockMachine = *stateMachine;
    InstanceState mockStateMachineState;
    std::string mockStateMachineInstanceStatusMsg;
    EXPECT_CALL(mockMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockMachine, TransitionToImpl)
        .WillRepeatedly(testing::DoAll(SaveArg<0>(&mockStateMachineState),
                                       SaveArg<1>(&mockStateMachineInstanceStatusMsg), Return(SCHEDULING_RESULT)));

    EXPECT_CALL(mockMachine, ReleaseOwner).WillRepeatedly(Return());

    GeneratedInstanceStates genStates{ "instance-id-CreateLocalNotEnoughButNotForward", InstanceState::NEW, false };
    EXPECT_CALL(*instanceControlView_, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(nullptr)).WillRepeatedly(Return(stateMachine));
    instanceCtrl->BindInstanceControlView(instanceControlView_);

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_))
        .WillOnce(Return(ScheduleResult{ "", StatusCode::RESOURCE_NOT_ENOUGH, "" }));
    instanceCtrl->BindScheduler(scheduler);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("request-id-CreateLocalNotEnoughButNotForward");
    scheduleReq->set_traceid("trace-id-CreateLocalNotEnoughButNotForward");
    EXPECT_CALL(mockMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    scheduleReq->mutable_instance()->set_instanceid("instance-id-CreateLocalNotEnoughButNotForward");
    EXPECT_CALL(mockMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(mockMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    YRLOG_INFO("Result: {}", result.Get().SerializeAsString());
    EXPECT_EQ(result.Get().code(), (int32_t)StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_EQ(mockStateMachineState, InstanceState::SCHEDULING);
}

/**
 * Feature: new instance while request id is duplicate
 * Steps:
 * 1. mocked new instance return duplicate instance
 * 2. mocked state chanage call back register
 * Expectation:
 */
TEST_F(InstanceCtrlTest, NewInstanceWithDuplicate)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_));
    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));

    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    instanceCtrl->BindInternalIAM(mockInternalIAM_);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    GeneratedInstanceStates genStates{ "DesignatedInstanceID", InstanceState::NEW, true };

    EXPECT_CALL(*instanceControlView, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView, GetInstance).WillRepeatedly(Return(stateMachine));
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&request) -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(request);
            return runtime::NotifyResponse();
        }));
    instanceCtrl->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    resources::InstanceInfo instance;
    instance.set_instanceid(instanceID);
    instance.set_requestid("request");
    instance.set_functionproxyid("nodeID");
    instance.set_parentfunctionproxyaid(actor->GetAID());
    instance.set_parentid("parent");
    instance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::SCHEDULE_FAILED));
    instance.mutable_instancestatus()->set_errcode(static_cast<int32_t>(StatusCode::ERR_RESOURCE_NOT_ENOUGH));
    instance.mutable_instancestatus()->set_msg("state changed");
    EXPECT_CALL(mockStateMachine,
                AddStateChangeCallback(UnorderedElementsAre(InstanceState::RUNNING, InstanceState::SCHEDULE_FAILED,
                                                            InstanceState::EXITING, InstanceState::FATAL),
                                       _, _))
        .WillOnce(Invoke([instance](const std::unordered_set<InstanceState> &statesConcerned,
                                    const std::function<void(const resources::InstanceInfo &)> &callback,
                                    const std::string &eventKey) { callback(instance); }));
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("request-id-NewInstanceWithDuplicate");
    scheduleReq->set_traceid("trace-id-NewInstanceWithDuplicate");
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(actor->GetAID());
    scheduleReq->mutable_instance()->set_parentid("parent");
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instance));
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), (int32_t)StatusCode::SUCCESS);
    ASSERT_AWAIT_READY(runtimePromise->GetFuture());
    EXPECT_EQ(runtimePromise->GetFuture().Get().code(), 0);
    ASSERT_AWAIT_READY(notifyCalled.GetFuture());
    EXPECT_EQ(static_cast<int32_t>(notifyCalled.GetFuture().Get().code()),
              static_cast<int32_t>(StatusCode::ERR_RESOURCE_NOT_ENOUGH));
    EXPECT_EQ(notifyCalled.GetFuture().Get().message(), "state changed");
}

/**
 * Feature: scheduling while request id is duplicate
 * Steps:
 * 1. mocked new instance return duplicate instance
 * 2. mocked state chanage call back register
 * Expectation:
 */
TEST_F(InstanceCtrlTest, DISABLED_SchedulingWithDuplicate)
{
    auto observer = std::make_shared<MockObserver>();
    ASSERT_IF_NULL(observer);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_));
    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));

    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));

    auto actor = std::make_shared<MockInstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);

    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    ASSERT_IF_NULL(instanceCtrl);

    instanceCtrl->Start(nullptr, resourceViewMgr, observer);
    instanceCtrl->BindInternalIAM(mockInternalIAM_);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");

    auto &mockStateMachine = *stateMachine;
    InstanceState mockStateMachineState;
    std::string mockStateMachineInstanceStatusMsg;
    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl)
        .WillRepeatedly(testing::DoAll(SaveArg<0>(&mockStateMachineState),
                                       SaveArg<1>(&mockStateMachineInstanceStatusMsg), Return(SCHEDULING_RESULT)));
    EXPECT_CALL(mockStateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    EXPECT_CALL(mockStateMachine, ReleaseOwner).WillOnce(Return());
    GeneratedInstanceStates genStates{ "DesignatedInstanceID", InstanceState::SCHEDULING, false };
    EXPECT_CALL(*instanceControlView_, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(nullptr)).WillRepeatedly(Return(stateMachine));
    instanceCtrl->BindInstanceControlView(instanceControlView_);

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_))
        .WillOnce(Return(ScheduleResult{ "", StatusCode::RESOURCE_NOT_ENOUGH, "" }));
    instanceCtrl->BindScheduler(scheduler);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("request-id-SchedulingWithDuplicate");
    scheduleReq->set_traceid("trace-id-SchedulingWithDuplicate");
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    EXPECT_CALL(mockStateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    auto duplicateRuntimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(1);
    auto duplicateResult = instanceCtrl->Schedule(scheduleReq, duplicateRuntimePromise);

    ASSERT_AWAIT_READY(result);
    ASSERT_AWAIT_READY(duplicateResult);
    YRLOG_INFO("Result: {}", result.Get().SerializeAsString());
    EXPECT_EQ(result.Get().code(), (int32_t)StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_EQ(duplicateResult.Get().code(), StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_EQ(mockStateMachineState, InstanceState::SCHEDULING);

    ASSERT_AWAIT_READY(runtimePromise->GetFuture());
    EXPECT_EQ(runtimePromise->GetFuture().Get().code(), 0);
    ASSERT_AWAIT_READY(duplicateRuntimePromise->GetFuture());
    EXPECT_EQ(duplicateRuntimePromise->GetFuture().Get().code(), 0);
}

/**
 * Feature: instance ctrl.
 * Description: kill instance with empty id.
 * Steps:
 * Expectation: return ERR_PARAM_INVALID.
 */
TEST_F(InstanceCtrlTest, KillInstancesOfJobWithEmptyID)
{
    auto actor = std::make_shared<MockInstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto killReq = std::make_shared<KillRequest>();
    auto res = actor->KillInstancesOfJob(killReq);
    EXPECT_EQ(res.Get().code(), static_cast<common::ErrorCode>(StatusCode::ERR_PARAM_INVALID));
}

/**
 * Feature: instance ctrl.
 * Description: CheckFuncMeta with different funcMeta.
 * Steps:
 * 1. mock a empty funcMeta
 * 2. mock a none empty funcMeta
 * Expectation: return ERR_INSTANCE_NOT_FOUND.
 */
TEST_F(InstanceCtrlTest, CheckFuncMetaTest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto res1 = actor->CheckFuncMeta(litebus::None(), std::make_shared<messages::ScheduleRequest>());
    EXPECT_EQ(res1.Get().StatusCode(), StatusCode::FAILED);
    FunctionMeta meta{};
    auto res2 = actor->CheckFuncMeta(meta, std::make_shared<messages::ScheduleRequest>());
    EXPECT_EQ(res2.Get().StatusCode(), StatusCode::SUCCESS);
}

/**
 * Feature: instance ctrl.
 * Description: Redeploy with failed status.
 * Steps:
 * 1. DoLocalRedeploy with failed status
 * 2. Redeploy with failed status
 * Expectation: return FAILED.
 */
TEST_F(InstanceCtrlTest, DoLocalRedeployFailed)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto res1 = actor->DoLocalRedeploy(
        Status(StatusCode::FAILED), scheduleReq,
        std::make_shared<InstanceStateMachine>("nodeID", std::make_shared<InstanceContext>(scheduleReq), false));
    EXPECT_EQ(res1.Get().StatusCode(), StatusCode::FAILED);
    auto res2 = actor->Redeploy(Status(StatusCode::FAILED), std::make_shared<messages::ScheduleRequest>());
    EXPECT_EQ(res2.Get().StatusCode(), StatusCode::FAILED);

    scheduleReq->mutable_instance()->set_instanceid("aaaa");
    actor->redeployTimesMap_["aaaa"] = 3;
    auto res3 = actor->Redeploy(Status(StatusCode::FAILED), scheduleReq);
    EXPECT_EQ(res3.Get().StatusCode(), StatusCode::FAILED);

    actor->instanceControlView_->NewInstance(scheduleReq);
    auto res4 = actor->Redeploy(Status(StatusCode::FAILED), scheduleReq);
    EXPECT_EQ(res4.Get().StatusCode(), StatusCode::FAILED);
    actor->redeployTimesMap_["aaaa"] = 0;
}

/**
 * Feature: instance ctrl.
 * Description: SendSignal with different context.
 * Steps:
 * 1. build context with errcode
 * 2. set instanceIsFailed true
 * Expectation: return ERR_PARAM_INVALID and ERR_REQUEST_BETWEEN_RUNTIME_BUS.
 */
TEST_F(InstanceCtrlTest, SendSignalWithFailedRsp)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto killCtx = std::make_shared<KillContext>();
    auto &killRsp = killCtx->killRsp;
    killRsp = GenKillResponse(common::ErrorCode::ERR_PARAM_INVALID, "instanceID is empty");
    auto res1 = actor->SendSignal(killCtx, "111", std::make_shared<KillRequest>());
    EXPECT_EQ(res1.Get().code(), common::ErrorCode::ERR_PARAM_INVALID);
    killRsp = GenKillResponse(common::ErrorCode::ERR_NONE, "");
    killCtx->instanceIsFailed = true;
    auto res2 = actor->SendSignal(killCtx, "111", std::make_shared<KillRequest>());
    EXPECT_EQ(res2.Get().code(), common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS);

    auto clientManager = std::make_shared<MockSharedClientManagerProxy>();
    EXPECT_CALL(*clientManager, GetControlInterfacePosixClient(_)).WillRepeatedly(Return(nullptr));
    actor->BindControlInterfaceClientManager(clientManager);
    killRsp = GenKillResponse(common::ErrorCode::ERR_NONE, "");
    killCtx->instanceIsFailed = false;
    auto request = std::make_shared<messages::ScheduleRequest>();
    request->mutable_instance()->set_instanceid("instanceid");
    killCtx->instanceContext = std::make_shared<InstanceContext>(request);
    auto res3 = actor->SendSignal(killCtx, "111", std::make_shared<KillRequest>());
    EXPECT_TRUE(res3.Get().code() == common::ErrorCode::ERR_INSTANCE_NOT_FOUND);
    EXPECT_TRUE(res3.Get().message().find("posix connection is not found") != std::string::npos);
    std::cout << res3.Get().message() << std::endl;
}

/**
 * Feature: instance ctrl.
 * Description: CheckGeneratedInstanceID with different scheduleReq.
 * Steps:
 * 1. invoke with empty scheduleReq
 * 2. invoke with  non-existent instance id
 * Expectation: return ERR_INSTANCE_EXITED.
 */
TEST_F(InstanceCtrlTest, CheckGeneratedInstanceIDFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto res1 =
        actor->CheckGeneratedInstanceID(GeneratedInstanceStates{}, std::make_shared<messages::ScheduleRequest>(),
                                        std::make_shared<litebus::Promise<messages::ScheduleResponse>>());
    EXPECT_EQ(res1.Get().code(), StatusCode::ERR_INSTANCE_INFO_INVALID);
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(nullptr));
    const std::string instanceID = "instance id";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid(instanceID);
    actor->RegisterStateChangeCallback(scheduleReq, std::make_shared<litebus::Promise<messages::ScheduleResponse>>());
    auto res2 = actor->CheckGeneratedInstanceID(GeneratedInstanceStates{ "111" }, scheduleReq,
                                                std::make_shared<litebus::Promise<messages::ScheduleResponse>>());
    EXPECT_EQ(res2.Get().code(), StatusCode::ERR_INSTANCE_EXITED);
}

/**
 * Feature: instance ctrl (D-6 F3 phantom restore).
 * Description: RegisterStateChangeCallback against a stale CREATING machine
 *              (leftover of a failed restore) without a pending schedule
 *              future must fail the promise instead of faking a code:0
 *              SUCCESS ("instance has already been scheduling") that made
 *              the wake caller drop the parked entry while nothing deploys.
 * Expectation: promise resolves with FAILED and a "stale" message.
 */
TEST_F(InstanceCtrlTest, RegisterStateChangeCallbackStaleCreatingMachineFailsPromise)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto view = std::make_shared<MockInstanceControlView>("nodeID");
    actor->instanceControlView_ = view;
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*stateMachine, GetInstanceState()).WillRepeatedly(Return(InstanceState::CREATING));
    EXPECT_CALL(*stateMachine, GetRequestID()).WillRepeatedly(Return("rq-stale"));
    EXPECT_CALL(*view, GetInstance("phantom-iid")).WillRepeatedly(Return(stateMachine));
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("rq-new");
    scheduleReq->mutable_instance()->set_instanceid("phantom-iid");
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    actor->RegisterStateChangeCallback(scheduleReq, runtimePromise);
    auto rsp = runtimePromise->GetFuture().Get();
    EXPECT_EQ(rsp.code(), static_cast<int32_t>(StatusCode::FAILED));
    EXPECT_NE(rsp.message().find("stale"), std::string::npos);
}

/**
 * Feature: instance ctrl (D-6 F3 phantom restore).
 * Description: RegisterStateChangeCallback against a CREATING machine that
 *              still has a pending schedule future must ride that future
 *              (same as SCHEDULING), so a duplicate/concurrent schedule
 *              observes the real deployment outcome.
 * Expectation: promise resolves exactly when the pending future completes.
 */
TEST_F(InstanceCtrlTest, RegisterStateChangeCallbackCreatingMachineRidesPendingFuture)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto view = std::make_shared<MockInstanceControlView>("nodeID");
    actor->instanceControlView_ = view;
    auto sourcePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    view->InsertRequestFuture("rq-live", sourcePromise->GetFuture(), sourcePromise);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*stateMachine, GetInstanceState()).WillRepeatedly(Return(InstanceState::CREATING));
    EXPECT_CALL(*stateMachine, GetRequestID()).WillRepeatedly(Return("rq-live"));
    EXPECT_CALL(*view, GetInstance("live-iid")).WillRepeatedly(Return(stateMachine));
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("rq-dup");
    scheduleReq->mutable_instance()->set_instanceid("live-iid");
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    actor->RegisterStateChangeCallback(scheduleReq, runtimePromise);
    auto future = runtimePromise->GetFuture();
    EXPECT_TRUE(future.IsInit());  // rides the pending future, not completed yet
    auto expected = GenScheduleResponse(StatusCode::SUCCESS, "restore done", *scheduleReq);
    sourcePromise->SetValue(expected);
    auto rsp = future.Get();
    EXPECT_EQ(rsp.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(rsp.message(), "restore done");
}

/**
 * Feature: instance ctrl (D-6 F3 phantom restore).
 * Description: CleanupFailedSnapStart removes the CREATING state machine of
 *              a failed restore deploy (so the next wake schedules a fresh
 *              restore instead of hitting a stale machine), and refuses to
 *              touch machines it does not own (other request / advanced
 *              state).
 * Expectation: DelInstance invoked only for the owned CREATING machine.
 */
TEST_F(InstanceCtrlTest, CleanupFailedSnapStartRemovesOwnedCreatingMachineOnly)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto view = std::make_shared<MockInstanceControlView>("nodeID");
    actor->instanceControlView_ = view;
    EXPECT_CALL(*view, DelInstance).WillRepeatedly(Return(Status::OK()));

    auto creatingSm = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*creatingSm, GetInstanceState()).WillRepeatedly(Return(InstanceState::CREATING));
    EXPECT_CALL(*creatingSm, GetRequestID()).WillRepeatedly(Return("rq-1"));
    auto runningSm = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*runningSm, GetInstanceState()).WillRepeatedly(Return(InstanceState::RUNNING));
    EXPECT_CALL(*runningSm, GetRequestID()).WillRepeatedly(Return("rq-1"));
    auto foreignSm = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*foreignSm, GetInstanceState()).WillRepeatedly(Return(InstanceState::CREATING));
    EXPECT_CALL(*foreignSm, GetRequestID()).WillRepeatedly(Return("rq-other"));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("rq-1");
    scheduleReq->mutable_instance()->set_instanceid("iid");

    // owned CREATING machine: full removal executed
    EXPECT_CALL(*view, GetInstance("iid"))
        .WillOnce(Return(creatingSm))
        .WillOnce(Return(runningSm))
        .WillOnce(Return(foreignSm))
        .WillRepeatedly(Return(nullptr));
    EXPECT_CALL(*view, DelInstance("iid")).Times(1).WillOnce(Return(Status::OK()));
    actor->CleanupFailedSnapStart(scheduleReq, "deploy failed: 80004");
    // advanced state (e.g. restore finished concurrently): untouched
    actor->CleanupFailedSnapStart(scheduleReq, "deploy failed: 80004");
    // machine owned by another request: untouched
    actor->CleanupFailedSnapStart(scheduleReq, "deploy failed: 80004");
    // machine already gone: no-op
    actor->CleanupFailedSnapStart(scheduleReq, "deploy failed: 80004");
}

/**
 * Feature: instance ctrl.
 * Description: CheckSchedRequestValid with different scheduleReq.
 * Steps:
 * 1. invoke with invalid cpu
 * 2. invoke with invalid memory
 * Expectation: return ERR_INSTANCE_EXITED.
 */
TEST_F(InstanceCtrlTest, CheckSchedRequestValidFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto req = std::make_shared<messages::ScheduleRequest>();
    req->set_requestid("rq1");
    req->set_traceid("id1");
    req->mutable_instance()->set_function("rq1");
    auto res1 = actor->CheckSchedRequestValid(req);
    EXPECT_EQ(res1.StatusCode(), StatusCode::ERR_FUNCTION_META_NOT_FOUND);

    auto meta = std::make_shared<FunctionMeta>();
    resource_view::Resources rs;
    resource_view::Resource r1;
    resource_view::Resource r2;
    rs.mutable_resources()->insert({ "CPU", r1 });
    rs.mutable_resources()->insert({ "Memory", r2 });
    meta->resources = rs;
    actor->funcMetaMap_["rq1"] = *meta;

    Resource resourceCPU;
    resourceCPU.set_name("CPU");
    resourceCPU.set_type(ValueType::Value_Type_SCALAR);
    resourceCPU.mutable_scalar()->set_value(1.1);

    resource_view::Resources resources;
    auto resourcesMap = resources.mutable_resources();
    (*resourcesMap)["CPU"] = resourceCPU;

    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_function("rq1");
    auto instanceRes = instanceInfo.mutable_resources();
    *instanceRes = resources;
    *(req->mutable_instance()) = instanceInfo;

    auto res2 = actor->CheckSchedRequestValid(req);
    EXPECT_EQ(res2.StatusCode(), StatusCode::ERR_RESOURCE_CONFIG_ERROR);

    Resource resourceCPU1;
    resourceCPU1.set_name("CPU");
    resourceCPU1.set_type(ValueType::Value_Type_SCALAR);
    resourceCPU1.mutable_scalar()->set_value(400.1);

    Resource resourceMemory1;
    resourceMemory1.set_name("Memory");
    resourceMemory1.set_type(ValueType::Value_Type_SCALAR);
    resourceMemory1.mutable_scalar()->set_value(111.0);

    (*resourcesMap)["CPU"] = resourceCPU1;
    (*resourcesMap)["Memory"] = resourceMemory1;
    *instanceRes = resources;
    *(req->mutable_instance()) = instanceInfo;
    auto res3 = actor->CheckSchedRequestValid(req);
    EXPECT_EQ(res3.StatusCode(), StatusCode::ERR_RESOURCE_CONFIG_ERROR);
}

TEST_F(InstanceCtrlTest, CheckLowReliabilityNoRecover)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto req = std::make_shared<messages::ScheduleRequest>();
    req->set_requestid("rq1");
    req->set_traceid("id1");
    auto meta = std::make_shared<FunctionMeta>();
    resource_view::Resources rs;
    resource_view::Resource r1;
    resource_view::Resource r2;
    rs.mutable_resources()->insert({ "CPU", r1 });
    rs.mutable_resources()->insert({ "Memory", r2 });
    meta->resources = rs;
    actor->funcMetaMap_["rq1"] = *meta;

    Resource resourceCPU;
    resourceCPU.set_name("CPU");
    resourceCPU.set_type(ValueType::Value_Type_SCALAR);
    resourceCPU.mutable_scalar()->set_value(1000.0);

    Resource resourceMemory;
    resourceMemory.set_name("Memory");
    resourceMemory.set_type(ValueType::Value_Type_SCALAR);
    resourceMemory.mutable_scalar()->set_value(1024.0);

    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_function("rq1");
    instanceInfo.set_lowreliability(true);
    (*instanceInfo.mutable_createoptions())["RecoverRetryTimes"] = "3";

    resource_view::Resources resources;
    auto resourcesMap = resources.mutable_resources();
    (*resourcesMap)["CPU"] = resourceCPU;
    (*resourcesMap)["Memory"] = resourceMemory;
    auto instanceRes = instanceInfo.mutable_resources();
    *instanceRes = resources;
    *(req->mutable_instance()) = instanceInfo;

    auto res3 = actor->CheckSchedRequestValid(req);
    EXPECT_EQ(res3.StatusCode(), StatusCode::ERR_PARAM_INVALID);
    (*instanceInfo.mutable_createoptions())["RecoverRetryTimes"] = "0";
    (*instanceInfo.mutable_createoptions())["DELEGATE_POD_LABELS"] = "{}";
    res3 = actor->CheckSchedRequestValid(req);
    EXPECT_EQ(res3.StatusCode(), StatusCode::ERR_PARAM_INVALID);
}

/**
 * Feature: instance ctrl.
 * Description: CheckHeteroResourceValid.
 * Steps:
 * Expectation: return bool.
 */
TEST_F(InstanceCtrlTest, CheckHeteroResourceValid)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto req = std::make_shared<messages::ScheduleRequest>();
    req->set_requestid("rq1");
    req->set_traceid("id1");

    // a invalid request -- hbm is 0
    (*req->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(0, 1, 1);
    auto res = actor->CheckHeteroResourceValid(req);
    EXPECT_FALSE(res);

    // a invalid request -- latency is 0
    (*req->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(1, 0, 1);
    res = actor->CheckHeteroResourceValid(req);
    EXPECT_FALSE(res);

    // a invalid request -- stream is 0
    (*req->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(1, 1, 0);
    res = actor->CheckHeteroResourceValid(req);
    EXPECT_FALSE(res);

    // a invalid request -- invalid card type regex
    (*req->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(1, 1, 1, "NPU/(Ascend910");
    res = actor->CheckHeteroResourceValid(req);
    EXPECT_FALSE(res);

    // a valid request -- hbm/latency/stream is 1
    (*req->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(1, 1, 1);
    res = actor->CheckHeteroResourceValid(req);
    EXPECT_TRUE(res);

    // a invalid request -- count is 0
    (*req->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(0);
    res = actor->CheckHeteroResourceValid(req);
    EXPECT_FALSE(res);

    // a valid request -- count is 1
    (*req->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(1);
    res = actor->CheckHeteroResourceValid(req);
    EXPECT_TRUE(res);

    // a valid request -- valid card type regex
    (*req->mutable_instance()) = view_utils::Get1DInstanceWithNpuResource(1, 1, 1, "NPU/Ascend910.*");
    res = actor->CheckHeteroResourceValid(req);
    EXPECT_TRUE(res);
}

/**
 * Feature: instance ctrl.
 * Description: CheckHeteroResourceValid.
 * Steps:
 * Expectation: return bool.
 */
TEST_F(InstanceCtrlTest, CheckDiskResourceValid)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto req = std::make_shared<messages::ScheduleRequest>();
    req->set_requestid("rq1");
    req->set_traceid("id1");

    // a valid request -- disk size is 100
    resources::Resource validDisk;
    validDisk.mutable_scalar()->set_value(100);
    req->mutable_instance()->mutable_resources()->mutable_resources()->operator[](DISK_RESOURCE_NAME) =
        validDisk;
    auto res = actor->CheckHeteroResourceValid(req);
    EXPECT_FALSE(res);

    // a invalid request -- disk size is 0
    validDisk.mutable_scalar()->set_value(0);
    req->mutable_instance()->mutable_resources()->mutable_resources()->operator[](DISK_RESOURCE_NAME) =
        validDisk;
    res = actor->CheckHeteroResourceValid(req);
    EXPECT_FALSE(res);

    // a invalid request -- disk size is -100
    validDisk.mutable_scalar()->set_value(-100);
    req->mutable_instance()->mutable_resources()->mutable_resources()->operator[](DISK_RESOURCE_NAME) =
        validDisk;
    res = actor->CheckHeteroResourceValid(req);
    EXPECT_FALSE(res);

}

/**
 * Feature: instance ctrl.
 * Description: DeployInstance after maxInstanceRedeployTimes.
 * Steps:
 * Expectation: return LS_DEPLOY_INSTANCE_FAILED.
 */
TEST_F(InstanceCtrlTest, DeployInstanceAfterRetryTimes)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto res = actor->DeployInstance(std::make_shared<messages::ScheduleRequest>(), 100, litebus::None());
    EXPECT_EQ(res.Get().StatusCode(), StatusCode::LS_DEPLOY_INSTANCE_FAILED);
}

/**
 * Feature: instance ctrl.
 * Description: HandleCallResultTimeout with different para.
 * Steps:
 * 1. invoke with CreateRequest do not exist
 * 2. invoke with callResultPromise future ok
 * Expectation: return OK.
 */
TEST_F(InstanceCtrlTest, HandleCallResultTimeoutFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto req = std::make_shared<messages::ScheduleRequest>();
    req->set_requestid("rq1");
    req->set_traceid("id1");
    req->mutable_instance()->set_instanceid("noneexistid");
    auto res = actor->HandleCallResultTimeout(req);
    EXPECT_EQ(res.Get().IsOk(), true);
    auto createCallResultPromise = std::make_shared<litebus::Promise<std::shared_ptr<functionsystem::CallResult>>>();
    actor->syncCreateCallResultPromises_[req->instance().instanceid()] = createCallResultPromise;
    auto callResult = std::make_shared<functionsystem::CallResult>();
    callResult->set_code(common::ErrorCode::ERR_NONE);
    callResult->set_message("success");
    createCallResultPromise->SetValue(callResult);
    auto res1 = actor->HandleCallResultTimeout(req);
    EXPECT_EQ(res1.Get().IsOk(), true);
}

/**
 * Feature: instance ctrl.
 * Description: ForwardCallResultResponse with result msg.
 * Steps:
 * Expectation: CallResultPromise deleted after invoke.
 */
TEST_F(InstanceCtrlTest, ForwardCallResultResponseFullTest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    litebus::AID global;
    actor->ForwardCallResultResponse(global, "", "");
    ::internal::ForwardCallResultResponse response;
    response.set_requestid("id1");
    actor->ForwardCallResultResponse(global, "", response.SerializeAsString());
    actor->forwardCallResultPromise_["id2"] =
        std::make_shared<litebus::Promise<::internal::ForwardCallResultResponse>>();
    ::internal::ForwardCallResultResponse response2;
    response2.set_requestid("id2");
    actor->ForwardCallResultResponse(global, "", response2.SerializeAsString());
    EXPECT_EQ(actor->syncCreateCallResultPromises_["id2"], nullptr);
}

/**
 * Feature: instance ctrl.
 * Description: SendForwardCallResultRequest with empty proxy opt.
 * Steps:
 * Expectation: return ERR_INNER_SYSTEM_ERROR.
 */
TEST_F(InstanceCtrlTest, ForwardCallResultRequest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindObserver(mockObserver_);
    actor->ForwardCallResultRequest(litebus::AID(), "", "");

    ::internal::ForwardCallResultRequest req{};
    ::core_service::CallResult callResult{};
    callResult.set_requestid("reqid");
    *req.mutable_req() = std::move(callResult);
    req.set_instanceid("instid");
    req.mutable_readyinstance()->set_instanceid("instid");
    req.mutable_readyinstance()->mutable_instancestatus()->set_code(3);

    EXPECT_CALL(*mockObserver_, FastPutRemoteInstanceEvent).WillOnce(Return());
    actor->ForwardCallResultRequest(litebus::AID(), "", req.SerializeAsString());
    actor->instanceControlView_ = std::make_shared<InstanceControlView>("node1", false);

    litebus::AID aid;
    auto res = actor->SendForwardCallResultRequest(aid, std::make_shared<::internal::ForwardCallResultRequest>());
    EXPECT_FALSE(res.IsOK());
    // call SendForwardCallResultRequest again and forwardCallResultPromise_.emplace will fail.
    res = actor->SendForwardCallResultRequest(aid, std::make_shared<::internal::ForwardCallResultRequest>());
    EXPECT_FALSE(res.IsOK());
}

/**
 * Feature: instance ctrl.
 * Description: GetDeployInstanceReq test.
 * Steps:
 * 1. build funcMeta and request
 * 2. invoke GetDeployInstanceReq
 * Expectation: return what we set.
 */
TEST_F(InstanceCtrlTest, GetDeployInstanceReqTest)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("id3");
    scheduleReq->set_traceid("id4");
    scheduleReq->set_requestid("id5");
    scheduleReq->mutable_instance()->set_function("function");
    scheduleReq->mutable_instance()->mutable_createoptions()->insert({ "k", "v" });
    scheduleReq->mutable_instance()->mutable_scheduleoption()->set_schedpolicyname("mm");

    FuncMetaData funcMetaData{};
    funcMetaData.hookHandler["key"] = "value";
    Layer layer{ appID : "a", bucketID : "b", objectID : "c", bucketURL : "d", sha256 : "e" };
    CodeMetaData codeMetaData{};
    codeMetaData.storageType = "nsp";
    codeMetaData.bucketUrl = "https://**.cn:***";
    codeMetaData.layers.push_back(layer);
    EnvMetaData envMetaData{};
    MountUser mountUser{ userID : 123, groupID : 456 };
    FuncMount mount1{ mountType : "x", mountResource : "y", mountSharePath : "z", localMountPath : "a", status : "b" };
    MountConfig mountConfig{ mountUser : mountUser };
    mountConfig.funcMounts.push_back(mount1);
    ExtendedMetaData extendedMetaData{};
    extendedMetaData.mountConfig = mountConfig;

    auto meta = std::make_shared<FunctionMeta>();
    meta->codeMetaData = codeMetaData;
    meta->funcMetaData = funcMetaData;
    meta->envMetaData = envMetaData;
    meta->extendedMetaData = extendedMetaData;

    auto observer = std::make_shared<MockObserver>();
    ASSERT_IF_NULL(observer);
    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(true));

    auto req = GetDeployInstanceReq(*meta, scheduleReq);
    EXPECT_EQ(req->funcdeployspec().bucketurl(), "https://**.cn:***");
    EXPECT_EQ(req->instanceid(), "id3");
}

/**
 * Feature: instance ctrl.
 * Description: CollectInstanceResources test.
 * Steps:
 * 1. build InstanceInfo
 * 2. invoke CollectInstanceResources
 * Expectation:
 */
TEST_F(InstanceCtrlTest, CollectInstanceResourcesTest)
{
    std::string endPoint = "127.0.0.1:4317";
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);

    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("id3");
    auto instanceRes = instanceInfo.mutable_resources();
    resource_view::Resources resources;
    Resource resourceCPU1;
    resourceCPU1.set_name("CPU");
    resourceCPU1.set_type(ValueType::Value_Type_SCALAR);
    resourceCPU1.mutable_scalar()->set_value(400);

    Resource resourceMemory1;
    resourceMemory1.set_name("Memory");
    resourceMemory1.set_type(ValueType::Value_Type_SCALAR);
    resourceMemory1.mutable_scalar()->set_value(1024);
    auto resourcesMap = resources.mutable_resources();
    (*resourcesMap)["CPU"] = resourceCPU1;
    (*resourcesMap)["Memory"] = resourceMemory1;
    *instanceRes = resources;
    actor->CollectInstanceResources(instanceInfo);
    EXPECT_EQ(instanceInfo.instanceid(), "id3");
}

/**
 * Feature: instance ctrl.
 * Description: InvalidCallResultTest test.
 * Steps:
 * 1. build invalid para
 * 2. invoke
 * Expectation:
 */
TEST_F(InstanceCtrlTest, InvalidCallResultTest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    litebus::AID aid;
    actor->ForwardCustomSignalRequest(aid, "", "");
    actor->ForwardCustomSignalResponse(aid, "", "");
    auto forwardKillRequest = GenForwardKillRequest("requestID", "srcInstance", std::move(*GenKillRequest("instanceID1", customSignal)));
    actor->ForwardCustomSignalRequest(aid, "", forwardKillRequest->SerializeAsString());
    auto res = actor->RescheduleWithID("nojbk");
    EXPECT_EQ(res.Get().IsOk(), false);
    auto result = std::make_shared<functionsystem::CallResult>();
    auto res1 = actor->CallResult("", result);
    EXPECT_EQ(res1.Get().code(), static_cast<common::ErrorCode>(StatusCode::LS_REQUEST_NOT_FOUND));
    auto res2 = actor->SendNotifyResult(nullptr, "", "", result);
    EXPECT_EQ(res2.Get().code(), common::ERR_INNER_COMMUNICATION);

    actor->ToReady();
    actor->ForwardCustomSignalRequest(aid, "", forwardKillRequest->SerializeAsString());
    actor->BindInstanceControlView(instanceControlView_);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine));
    auto res3 = actor->SendNotifyResult(nullptr, "", "", result);
    EXPECT_EQ(res3.Get().code(), common::ERR_INNER_COMMUNICATION);

    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(nullptr));
    auto res4 = actor->SendNotifyResult(nullptr, "", "", result);
    EXPECT_EQ(res4.Get().code(), common::ERR_INSTANCE_NOT_FOUND);
}

/**
 * Feature: instance ctrl.
 * Description: received callresult from an exiting instance.
 */
TEST_F(InstanceCtrlTest, CallResultFromExistingInstanceTest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindInstanceControlView(instanceControlView_);
    actor->ToReady();
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceState).WillRepeatedly(Return(InstanceState::EXITING));
    auto result = std::make_shared<functionsystem::CallResult>();
    auto res1 = actor->CallResult("instance", result);
    EXPECT_EQ(res1.Get().code(), static_cast<common::ErrorCode>(StatusCode::ERR_INSTANCE_EVICTED));
}

/**
 * Feature: instance ctrl.
 * Description: HandleHeartbeatLost query success.
 */
TEST_F(InstanceCtrlTest, HandleHeartbeatLostQueryExceptionSuccess)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindFunctionAgentMgr(funcAgentMgr_);
    actor->BindInstanceControlView(instanceControlView_);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    actor->BindResourceView(resourceViewMgr);
    actor->BindInternalIAM(mockInternalIAM_);
    litebus::Spawn(actor);
    actor->AddHeartbeatTimer("instanceid");
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient).WillOnce(Return(nullptr));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));

    litebus::Promise<messages::InstanceStatusInfo> instanceStatusInfoPromise;
    messages::InstanceStatusInfo instanceStatusInfo;
    instanceStatusInfo.set_instancemsg("mock test");
    instanceStatusInfo.set_type(static_cast<int32_t>(EXIT_TYPE::EXCEPTION_INFO));
    instanceStatusInfoPromise.SetValue(instanceStatusInfo);
    EXPECT_CALL(*funcAgentMgr_, QueryInstanceStatusInfo("functionAgentID", "instanceid", "runtimeid"))
        .WillOnce(Return(instanceStatusInfoPromise.GetFuture()));
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instanceid");
    instanceInfo.set_runtimeid("runtimeid");
    instanceInfo.set_functionagentid("functionAgentID");
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));
    litebus::Future<std::vector<std::string>> deleteInstance;
    InstanceState mockStateMachineState;
    int32_t errCode;
    EXPECT_CALL(mockStateMachine, GetOwner).WillRepeatedly(Return("nodeID"));
    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl)
        .WillRepeatedly(testing::DoAll(SaveArg<0>(&mockStateMachineState), SaveArg<4>(&errCode), Return(NEW_RESULT)));
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*funcAgentMgr_, KillInstance(testing::_, testing::_, testing::_))
        .WillRepeatedly(
            Return(GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID")));
    EXPECT_CALL(*primary, DeleteInstances)
        .WillRepeatedly(DoAll(FutureArg<0>(&deleteInstance), Return(Status::OK())));
    litebus::Async(actor->GetAID(), &InstanceCtrlActor::HandleRuntimeHeartbeatLost, "instanceid", "runtimeid");
    ASSERT_AWAIT_READY(deleteInstance);
    EXPECT_EQ(mockStateMachineState, InstanceState::FATAL);
    EXPECT_EQ(errCode, common::ErrorCode::ERR_USER_FUNCTION_EXCEPTION);
    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

TEST_F(InstanceCtrlTest, HandleHeartbeatLostQueryExceptionFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindFunctionAgentMgr(funcAgentMgr_);
    actor->BindInstanceControlView(instanceControlView_);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    actor->BindResourceView(resourceViewMgr);
    actor->BindInternalIAM(mockInternalIAM_);
    litebus::Spawn(actor);
    actor->AddHeartbeatTimer("instanceid");
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient).WillOnce(Return(nullptr));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));

    litebus::Promise<messages::InstanceStatusInfo> instanceStatusInfoPromise;
    EXPECT_CALL(*funcAgentMgr_, QueryInstanceStatusInfo("functionAgentID", "instanceid", "runtimeid"))
        .WillOnce(Return(instanceStatusInfoPromise.GetFuture()));
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instanceid");
    instanceInfo.set_runtimeid("runtimeid");
    instanceInfo.set_functionagentid("functionAgentID");
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));
    litebus::Future<std::vector<std::string>> deleteInstance;
    InstanceState mockStateMachineState;
    int32_t errCode;
    EXPECT_CALL(mockStateMachine, GetOwner).WillRepeatedly(Return("nodeID"));
    EXPECT_CALL(mockStateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl)
        .WillRepeatedly(testing::DoAll(SaveArg<0>(&mockStateMachineState), SaveArg<4>(&errCode), Return(NEW_RESULT)));
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*funcAgentMgr_, KillInstance(testing::_, testing::_, testing::_))
        .WillRepeatedly(
            Return(GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID")));
    EXPECT_CALL(*primary, DeleteInstances)
        .WillRepeatedly(DoAll(FutureArg<0>(&deleteInstance), Return(Status::OK())));
    litebus::Async(actor->GetAID(), &InstanceCtrlActor::HandleRuntimeHeartbeatLost, "instanceid", "runtimeid");
    instanceStatusInfoPromise.SetFailed(-1);
    ASSERT_AWAIT_READY(deleteInstance);
    EXPECT_EQ(mockStateMachineState, InstanceState::FATAL);
    EXPECT_EQ(errCode, common::ErrorCode::ERR_INSTANCE_EXITED);
    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

/**
 * Feature: instance ctrl.
 * Description: HandleHeartbeatLost instance info change
 */
TEST_F(InstanceCtrlTest, HandleHeartbeatLostInstanceInfoChange)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindFunctionAgentMgr(funcAgentMgr_);
    actor->BindInstanceControlView(instanceControlView_);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    actor->BindResourceView(resourceViewMgr);
    actor->BindInternalIAM(mockInternalIAM_);
    litebus::Spawn(actor);
    actor->AddHeartbeatTimer("instanceidA");
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient).WillOnce(Return(nullptr));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    bool isFinished = false;
    EXPECT_CALL(*instanceControlView_, GetInstance)
        .WillOnce(Return(stateMachine))
        .WillOnce(DoAll(Assign(&isFinished, true), Return(stateMachine)));
    resource_view::InstanceInfo instanceInfoA;
    instanceInfoA.set_instanceid("instanceidA");
    instanceInfoA.set_runtimeid("runtimeidA");
    instanceInfoA.set_functionagentid("functionAgentIDA");
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillOnce(Return(instanceInfoA));
    EXPECT_CALL(mockStateMachine, GetOwner).WillOnce(Return("nodeID1")).WillOnce(Return("nodeID1"));
    litebus::Async(actor->GetAID(), &InstanceCtrlActor::HandleRuntimeHeartbeatLost, "instanceidA", "runtimeidA");
    ASSERT_AWAIT_TRUE([&]() { return isFinished; });
    actor->AddHeartbeatTimer("instanceidA1");
    isFinished = false;
    EXPECT_CALL(*instanceControlView_, GetInstance)
        .WillOnce(Return(stateMachine))
        .WillOnce(DoAll(Assign(&isFinished, true), Return(stateMachine)));
    EXPECT_CALL(mockStateMachine, GetOwner).WillOnce(Return("nodeID"));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient).WillOnce(Return(nullptr));
    instanceInfoA.set_instanceid("instanceidA1");
    instanceInfoA.set_runtimeid("runtimeidA1");
    instanceInfoA.set_functionagentid("functionAgentIDA1");
    resource_view::InstanceInfo instanceInfoB;
    instanceInfoB.set_instanceid("instanceidA1");
    instanceInfoB.set_runtimeid("runtimeidA2");
    instanceInfoB.set_functionagentid("functionAgentIDA2");
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillOnce(Return(instanceInfoA)).WillOnce(Return(instanceInfoB));
    litebus::Async(actor->GetAID(), &InstanceCtrlActor::HandleRuntimeHeartbeatLost, "instanceidA1", "runtimeidA1");
    ASSERT_AWAIT_TRUE([&]() { return isFinished; });
    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

TEST_F(InstanceCtrlTest, StartHeartBeatTest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->FcAccessorHeartbeatEnable(false);
    actor->StartHeartbeat("functionaccessor", 1, "runtimeID");

    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(nullptr));
    actor->StartHeartbeat("instanceID", 1, "runtimeID");

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClient, Heartbeat(_)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    actor->StartHeartbeat("instanceID", 1, "runtimeID");

    litebus::Future<Status> status;
    status.SetFailed(StatusCode::FAILED);
    EXPECT_CALL(*mockSharedClient, Heartbeat(_)).WillOnce(Return(status));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(mockSharedClient));
    actor->StartHeartbeat("instanceID", 2, "runtimeID");
    testing::internal::SleepMilliseconds(5);  // leave time for method Heartbeat to return.

    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

/**
 * Feature: HeartbeatHealthCheckTest
 * Description: heartbeat health check test
 * Steps:
 * 1. StartHeartbeat, failed to get client
 * 2. StartHeartbeat, return INSTANCE_HEALTH_CHECK_ERROR(health check failed)
 * 3. StartHeartbeat, return health check success
 * 4. StartHeartbeat, return INSTANCE_SUB_HEALTH
 *
 * Expectation:
 * 1. invoke HandleRuntimeHeartbeatLost
 * 2. invoke HandleRuntimeHeartbeatLost
 * 3. if current state is healthy, don't do anything; if current state is sub-health, invoke HandleInstanceHealthChange
 * 4. if current state is sub-health, don't do anything; if current state is healthy, invoke HandleInstanceHealthChange
 */
TEST_F(InstanceCtrlTest, HeartbeatHealthCheckTest)
{
    auto instanceCtrlActor = std::make_shared<MockInstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    instanceCtrlActor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto aid = instanceCtrlActor->GetAID();
    litebus::Spawn(instanceCtrlActor);

    bool isFinished = false;
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient).WillOnce(Return(nullptr));
    EXPECT_CALL(*instanceCtrlActor, HandleRuntimeHeartbeatLost("instanceID1", _)).WillOnce(Assign(&isFinished, true));
    litebus::Async(aid, &InstanceCtrlActor::StartHeartbeat, "instanceID1", 1, "runtimeID", StatusCode::SUCCESS);
    ASSERT_AWAIT_TRUE([&isFinished]() { return isFinished; });

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient)
        .WillRepeatedly(Return(mockSharedClient));

    isFinished = false;
    litebus::Future<Status> healthCheckFailedStatus;
    healthCheckFailedStatus.SetFailed(StatusCode::INSTANCE_HEALTH_CHECK_ERROR);
    EXPECT_CALL(*mockSharedClient, Heartbeat).WillOnce(Return(healthCheckFailedStatus));
    EXPECT_CALL(*instanceCtrlActor, HandleRuntimeHeartbeatLost("instanceID2", _)).WillOnce(Assign(&isFinished, true));
    litebus::Async(aid, &InstanceCtrlActor::StartHeartbeat, "instanceID2", 1, "runtimeID", StatusCode::SUCCESS);
    ASSERT_AWAIT_TRUE([&isFinished]() { return isFinished; });

    // sub-health to health
    isFinished = false;
    litebus::Future<Status> healthCheckSuccessStatus;
    healthCheckSuccessStatus.SetValue(Status::OK());
    EXPECT_CALL(*mockSharedClient, Heartbeat)
        .WillOnce(Return(healthCheckSuccessStatus))
        .WillOnce(Return(healthCheckSuccessStatus));
    EXPECT_CALL(*instanceCtrlActor, HandleInstanceHealthChange("instanceID3", StatusCode::SUCCESS))
        .WillOnce(Assign(&isFinished, true));
    litebus::Async(aid, &InstanceCtrlActor::StartHeartbeat, "instanceID3", 1, "runtimeID", StatusCode::SUCCESS);
    litebus::Async(aid, &InstanceCtrlActor::StartHeartbeat, "instanceID3", 1, "runtimeID",
                   StatusCode::INSTANCE_SUB_HEALTH);
    ASSERT_AWAIT_TRUE([&isFinished]() { return isFinished; });

    // health to sub-health
    isFinished = false;
    litebus::Future<Status> healthCheckSubHealthStatus;
    healthCheckSubHealthStatus.SetValue(Status(StatusCode::INSTANCE_SUB_HEALTH));
    EXPECT_CALL(*mockSharedClient, Heartbeat)
        .WillOnce(Return(healthCheckSubHealthStatus))
        .WillOnce(Return(healthCheckSubHealthStatus));
    EXPECT_CALL(*instanceCtrlActor, HandleInstanceHealthChange("instanceID4", StatusCode::INSTANCE_SUB_HEALTH))
        .WillOnce(Assign(&isFinished, true));
    litebus::Async(aid, &InstanceCtrlActor::StartHeartbeat, "instanceID4", 1, "runtimeID",
                   StatusCode::INSTANCE_SUB_HEALTH);
    litebus::Async(aid, &InstanceCtrlActor::StartHeartbeat, "instanceID4", 1, "runtimeID", StatusCode::SUCCESS);
    ASSERT_AWAIT_TRUE([&isFinished]() { return isFinished; });

    litebus::Terminate(instanceCtrlActor->GetAID());
    litebus::Await(instanceCtrlActor->GetAID());
}

/**
 * Feature: instance ctrl.
 * Description: Recover instances which state is scheduling.
 * Steps:
 * 1. The instance status read from etcd is scheduling.
 * 2. Sync recover instance  read from etcd
 */
TEST_F(InstanceCtrlTest, RecoverScheduleFailedInstanceWithoutAgent)
{
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfo.set_parentid("parentID");
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::SCHEDULE_FAILED));
    instanceInfo.mutable_instancestatus()->set_errcode(static_cast<int32_t>(StatusCode::ERR_RESOURCE_NOT_ENOUGH));
    instanceInfo.set_parentfunctionproxyaid(instanceCtrlWithMockObserver_->GetActorAID());
    instanceInfoMap.insert({ "instance1", instanceInfo });

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_function("function");
    scheduleReq->mutable_instance()->set_parentid("parent");
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::SCHEDULE_FAILED));
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(instanceCtrlWithMockObserver_->GetActorAID());
    auto context = std::make_shared<InstanceContext>(scheduleReq);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID", context);
    EXPECT_CALL(*instanceControlView_, GetInstance(testing::_)).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*mockObserver_, GetLocalInstanceInfo).WillRepeatedly(Return(instanceInfoMap));
    EXPECT_CALL(*stateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(*stateMachine, GetOwner).WillRepeatedly(Return("nodeID"));
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&re) -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(re);
            return runtime::NotifyResponse();
        }));
    std::unordered_map<std::string, messages::FuncAgentRegisInfo> agentMap;
    auto fut = instanceCtrlWithMockObserver_->SyncAgent(agentMap);
    ASSERT_AWAIT_SET(fut);
    ASSERT_AWAIT_READY(notifyCalled.GetFuture());
    EXPECT_EQ(notifyCalled.GetFuture().Get().code(), common::ErrorCode::ERR_RESOURCE_NOT_ENOUGH);
}

TEST_F(InstanceCtrlTest, RecoverExistedInstanceWithInvalidAgent_UnRecover)
{
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfo.set_functionagentid("agentID");
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    instanceInfoMap.insert({ "instance1", instanceInfo });
    EXPECT_CALL(*mockObserver_, GetLocalInstanceInfo).WillRepeatedly(Return(instanceInfoMap));

    messages::FuncAgentRegisInfo info;
    info.set_statuscode(static_cast<int32_t>(FunctionAgentMgrActor::RegisStatus::FAILED));
    info.set_agentaidname("agentID");
    std::unordered_map<std::string, messages::FuncAgentRegisInfo> agentMap;
    agentMap["agentID"] = info;

    // no recover to fatal
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance(testing::_)).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, TransitionToImpl).WillOnce(Return(FATAL_RESULT));
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    instanceCtrlWithMockObserver_->BindResourceView(resourceViewMgr);
    litebus::Future<std::vector<std::string>> deleteInstance;
    EXPECT_CALL(*primary, DeleteInstances)
        .WillRepeatedly(DoAll(FutureArg<0>(&deleteInstance), Return(Status::OK())));

    auto fut = instanceCtrlWithMockObserver_->SyncAgent(agentMap);
    ASSERT_AWAIT_SET(fut);
    ASSERT_AWAIT_READY(deleteInstance);
    EXPECT_EQ(deleteInstance.Get()[0], "instance1");
}

TEST_F(InstanceCtrlTest, RecoverExistedInstanceWithInvalidAgent_Recover)
{
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfo.set_functionagentid("agentID");
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    (*instanceInfo.mutable_createoptions())[RECOVER_RETRY_TIMES_KEY] = "3";
    instanceInfoMap.insert({ "instance1", instanceInfo });
    EXPECT_CALL(*mockObserver_, GetLocalInstanceInfo).WillRepeatedly(Return(instanceInfoMap));

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance(testing::_)).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, IsSaving()).WillRepeatedly(Return(false));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("instance1");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_function("function");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::FAILED));
    scheduleReq->mutable_instance()->set_scheduletimes(3);

    instanceCtrlWithMockObserver_->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    instanceCtrlWithMockObserver_->BindFunctionAgentMgr(funcAgentMgr_);
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*funcAgentMgr_, KillInstance(testing::_, testing::_, testing::_))
        .WillRepeatedly(Return(GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance", "requestID")));

    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(instanceCtrlWithMockObserver_->GetActorAID());
    EXPECT_CALL(*stateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(*stateMachine, GetScheduleTimes).WillRepeatedly(Return(3));
    EXPECT_CALL(*stateMachine, GetDeployTimes).WillRepeatedly(Return(3));
    EXPECT_CALL(*stateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, GetInstanceState).WillOnce(Return(InstanceState::FAILED));
    EXPECT_CALL(*stateMachine, TransitionToImpl).WillRepeatedly(Return(SCHEDULING_RESULT));
    EXPECT_CALL(*mockObserver_, GetLocalInstanceInfo).WillRepeatedly(Return(instanceInfoMap));
    litebus::Future<std::string> function;
    EXPECT_CALL(*mockObserver_, GetFuncMeta).WillOnce(DoAll(FutureArg<0>(&function), Return(litebus::None())));
    std::unordered_map<std::string, messages::FuncAgentRegisInfo> agentMap;
    auto fut = instanceCtrlWithMockObserver_->SyncAgent(agentMap);
    ASSERT_AWAIT_SET(fut);
    ASSERT_AWAIT_READY(function);
    EXPECT_EQ(function.Get(), "function");
}

TEST_F(InstanceCtrlTest, RecoverExitingInstanceWithoutAgent)
{
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance");
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::EXITING));

    resource_view::InstanceInfo instanceInfo1;
    instanceInfo1.set_instanceid("instance1");
    instanceInfo1.set_functionagentid("agentID");
    instanceInfo1.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::EVICTING));

    instanceInfoMap.insert({ "instance", instanceInfo });
    instanceInfoMap.insert({ "instance1", instanceInfo1 });
    EXPECT_CALL(*mockObserver_, GetLocalInstanceInfo).WillRepeatedly(Return(instanceInfoMap));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance(testing::_)).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));

    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    instanceCtrlWithMockObserver_->BindResourceView(resourceViewMgr);
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));
    litebus::Future<std::string> deleteInstance;
    EXPECT_CALL(*instanceControlView_, DelInstance).WillOnce(DoAll(FutureArg<0>(&deleteInstance), Return(Status::OK())));

    litebus::Future<InstanceState> context;
    EXPECT_CALL(*stateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, TransitionToImpl).WillOnce(DoAll(FutureArg<0>(&context), Return(EVICTING_RESULT)));

    std::unordered_map<std::string, messages::FuncAgentRegisInfo> agentMap;
    auto fut = instanceCtrlWithMockObserver_->SyncAgent(agentMap);
    ASSERT_AWAIT_SET(fut);
    ASSERT_AWAIT_READY(deleteInstance);
    EXPECT_EQ(deleteInstance.Get(), "instance");
    ASSERT_AWAIT_READY(context);
    EXPECT_EQ(context.Get(), InstanceState::EVICTED);
}

TEST_F(InstanceCtrlTest, RecoverSchedulingInstanceWithoutAgent)
{
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfo.set_parentid("parentID");
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::SCHEDULING));
    instanceInfoMap.insert({ "instance1", instanceInfo });
    EXPECT_CALL(*mockObserver_, GetLocalInstanceInfo).WillRepeatedly(Return(instanceInfoMap));

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance(testing::_)).WillRepeatedly(Return(stateMachine));

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("instance1");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_parentid("parentID");
    scheduleReq->mutable_instance()->set_function("function");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::SCHEDULING));
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(instanceCtrlWithMockObserver_->GetActorAID());
    EXPECT_CALL(*stateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(*stateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, TransitionToImpl).WillRepeatedly(Return(CREATING_RESULT));
    EXPECT_CALL(*stateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));
    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_)).WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, "" }));
    EXPECT_CALL(*scheduler, ScheduleConfirm).Times(1); // mock schedule successfully
    instanceCtrlWithMockObserver_->BindScheduler(scheduler);

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillOnce(Invoke([notifyCalled](runtime::NotifyRequest &&request) -> litebus::Future<runtime::NotifyResponse> {
            notifyCalled.SetValue(request);
            return runtime::NotifyResponse();
        }));

    EXPECT_CALL(*mockObserver_, GetLocalInstanceInfo).WillRepeatedly(Return(instanceInfoMap));
    std::unordered_map<std::string, messages::FuncAgentRegisInfo> agentMap;
    auto fut = instanceCtrlWithMockObserver_->SyncAgent(agentMap);
    ASSERT_AWAIT_SET(fut);
    ASSERT_AWAIT_READY(notifyCalled.GetFuture());
    EXPECT_EQ(static_cast<StatusCode>(notifyCalled.GetFuture().Get().code()), StatusCode::ERR_FUNCTION_META_NOT_FOUND);
}

TEST_F(InstanceCtrlTest, RescheduleAfterJudgeRecoverableTest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindInstanceControlView(instanceControlView_);
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    actor->BindResourceView(resourceViewMgr);
    actor->BindInternalIAM(mockInternalIAM_);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    actor->BindFunctionAgentMgr(funcAgentMgr_);
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*funcAgentMgr_, KillInstance(testing::_, testing::_, testing::_))
        .WillRepeatedly(Return(GenKillInstanceResponse(StatusCode::FAILED, "kill instance failed", "requestID")));
    litebus::Spawn(actor);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));

    int32_t errCode;
    InstanceState instanceState;
    EXPECT_CALL(*stateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, GetOwner()).WillRepeatedly(Return("nodeID"));
    EXPECT_CALL(*stateMachine, TransitionToImpl)
        .WillRepeatedly(testing::DoAll(SaveArg<0>(&instanceState), SaveArg<4>(&errCode), Return(NEW_RESULT)));
    EXPECT_CALL(*stateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    resources::InstanceInfo instanceInfo;
    (*instanceInfo.mutable_createoptions())[RECOVER_RETRY_TIMES_KEY] = "1";
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillOnce(Return(instanceInfo)).WillRepeatedly(Return(instanceInfo));
    EXPECT_CALL(*stateMachine, GetCancelFuture).WillOnce(Return(litebus::Future<std::string>()));
    actor->RescheduleAfterJudgeRecoverable("instanceid", "").Get();

    EXPECT_EQ(instanceState, InstanceState::FAILED);

    (*instanceInfo.mutable_createoptions())[RECOVER_RETRY_TIMES_KEY] = "0";
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillOnce(Return(instanceInfo)).WillRepeatedly(Return(instanceInfo));
    EXPECT_CALL(*stateMachine, GetCancelFuture).WillOnce(Return(litebus::Future<std::string>()));
    actor->RescheduleAfterJudgeRecoverable("instanceid", "").Get();

    EXPECT_EQ(instanceState, InstanceState::FATAL);

    (*instanceInfo.mutable_createoptions())[RECOVER_RETRY_TIMES_KEY] = "1";
    actor->redeployTimesMap_["instanceid"] = 1;
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillOnce(Return(instanceInfo));
    actor->RescheduleAfterJudgeRecoverable("instanceid", "").Get();
    EXPECT_EQ(instanceState, InstanceState::FATAL);

    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

TEST_F(InstanceCtrlTest, DeleteDriverClient)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindObserver(mockObserver_);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    actor->BindInternalIAM(mockInternalIAM_);
    EXPECT_CALL(*mockObserver_, DelInstance).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*mockInternalIAM_, IsIAMEnabled).WillRepeatedly(Return(true));
    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    actor->BindLocalSchedSrv(localSchedSrv);

    messages::ForwardKillResponse response;
    response.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<messages::ForwardKillRequest>> request;
    EXPECT_CALL(*localSchedSrv, ForwardKillToInstanceManager).WillOnce(DoAll(FutureArg<0>(&request), Return(response)));

    litebus::Spawn(actor);
    litebus::Async(actor->GetAID(), &InstanceCtrlActor::DeleteDriverClient, "driverID", "jobID");
    ASSERT_AWAIT_READY(request);
    EXPECT_EQ(request.Get()->req().instanceid(), "jobID");
    EXPECT_EQ(request.Get()->req().signal(), SHUT_DOWN_SIGNAL_ALL);
    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

TEST_F(InstanceCtrlTest, GracefulShutdown)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindObserver(mockObserver_);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    actor->BindInternalIAM(mockInternalIAM_);
    EXPECT_CALL(*mockObserver_, DelInstance).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(*mockInternalIAM_, IsIAMEnabled).WillRepeatedly(Return(true));
    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();
    actor->BindLocalSchedSrv(localSchedSrv);

    messages::ForwardKillResponse response;
    response.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<messages::ForwardKillRequest>> request;
    EXPECT_CALL(*localSchedSrv, ForwardKillToInstanceManager).WillRepeatedly(Return(response));
    actor->connectedDriver_["driver1"] = "job1";
    actor->connectedDriver_["driver2"] = "job2";
    actor->connectedDriver_["driver3"] = "job3";
    litebus::Spawn(actor);
    auto future = litebus::Async(actor->GetAID(), &InstanceCtrlActor::GracefulShutdown);
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get().IsOk(), true);
    EXPECT_EQ(actor->connectedDriver_.empty(), true);
    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

TEST_F(InstanceCtrlTest, SetAbnormal)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->SetAbnormal();
    EXPECT_TRUE(actor->isAbnormal_);
}

TEST_F(InstanceCtrlTest, ScheduleParentIDNotEmpty)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    scheduleReq->mutable_instance()->set_parentid("parentID");

    resources::InstanceInfo instance;
    instance.set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(actor->GetAID());

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(mockStateMachine, GetInstanceState).WillOnce(Return(InstanceState::EXITING));
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(instance));
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    actor->BindInstanceControlView(instanceControlView_);
    auto fut = actor->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(fut);
    EXPECT_EQ(fut.Get().code(), static_cast<int32_t>(StatusCode::ERR_INSTANCE_EXITED));
}

TEST_F(InstanceCtrlTest, SendForwardCallResultResponse)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    CallResultAck ack;
    litebus::AID from;
    auto ret = actor->SendForwardCallResultResponse(ack, from, "requestID", "instanceID");
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().IsOk());
}

TEST_F(InstanceCtrlTest, ScheduleConfirmed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleConfirm).WillOnce(Return(Status::OK()));
    actor->BindScheduler(scheduler);
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_instanceid("instanceID");

    auto ret = actor->ScheduleConfirmed(Status::OK(), scheduleReq);
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().IsOk());
}

TEST_F(InstanceCtrlTest, NotifyDsWorkerHealthy)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindFunctionAgentMgr(funcAgentMgr_);
    actor->BindInternalIAM(mockInternalIAM_);
    actor->BindInstanceControlView(instanceControlView_);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    actor->BindResourceView(resourceViewMgr);
    actor->NotifyDsHealthy(true);
    litebus::Spawn(actor);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("scheduling");
    instanceInfo.set_function("function");
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::SCHEDULING));
    instanceInfoMap.insert({ "scheduling", instanceInfo });
    EXPECT_CALL(*instanceControlView_, GetInstancesWithStatus(InstanceState::SCHEDULING))
        .WillOnce(Return(instanceInfoMap));
    EXPECT_CALL(*instanceControlView_, GetInstance("scheduling")).WillOnce(Return(nullptr));

    function_proxy::InstanceInfoMap instanceInfoMapCreating;
    instanceInfo.set_instanceid("creating");
    instanceInfo.set_function("function");
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::CREATING));
    instanceInfoMapCreating.insert({ "creating", instanceInfo });
    EXPECT_CALL(*instanceControlView_, GetInstancesWithStatus(InstanceState::CREATING))
        .WillOnce(Return(instanceInfoMapCreating));
    EXPECT_CALL(*instanceControlView_, GetInstance("creating")).WillOnce(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, AddStateChangeCallback).WillOnce(Return());

    function_proxy::InstanceInfoMap instanceInfoMapRunning;
    instanceInfo.set_instanceid("running");
    instanceInfo.set_function("function");
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    instanceInfoMapRunning.insert({ "running", instanceInfo });
    EXPECT_CALL(*instanceControlView_, GetInstancesWithStatus(InstanceState::RUNNING))
        .WillOnce(Return(instanceInfoMapRunning));
    EXPECT_CALL(*instanceControlView_, GetInstance("running")).WillOnce(Return(stateMachine));

    litebus::Future<std::vector<std::string>> deleteInstance;
    EXPECT_CALL(mockStateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl).WillOnce(Return(FATAL_RESULT));
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*funcAgentMgr_, KillInstance(testing::_, testing::_, testing::_))
        .WillRepeatedly(
            Return(GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID")));
    EXPECT_CALL(*primary, DeleteInstances)
        .WillRepeatedly(DoAll(FutureArg<0>(&deleteInstance), Return(Status::OK())));
    litebus::Async(actor->GetAID(), &InstanceCtrlActor::NotifyDsHealthy, false);
    ASSERT_AWAIT_READY(deleteInstance);
    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

/**
 * Feature: InstanceCtrlTest Evict Instance
 * Description: evict instance on agent
 * case1: no instance on agent
 * case2: evict running instance
 * case3: evict exiting instance
 * case4: evict creating instance
 */
TEST_F(InstanceCtrlTest, EvictInstanceOnAgent)
{
    auto req = std::make_shared<messages::EvictAgentRequest>();
    req->set_agentid("agentID");
    req->set_requestid("agentID");
    req->set_timeoutsec(1);
    {
        function_proxy::InstanceInfoMap instanceInfoMap;
        EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_)).WillOnce(test::Return(instanceInfoMap));
        auto future = instanceCtrlWithMockObserver_->EvictInstanceOnAgent(req);
        EXPECT_AWAIT_READY(future);
        EXPECT_EQ(future.Get().IsOk(), true);
    }

    {
        function_proxy::InstanceInfoMap instanceInfoMap;
        resource_view::InstanceInfo instanceInfo;
        instanceInfoMap.insert({ "instance1", instanceInfo });
        instanceInfoMap.insert({ "instance2", instanceInfo });
        instanceInfoMap.insert({ "instance3", instanceInfo });
        instanceInfoMap.insert({ "instance4", instanceInfo });

        EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_)).WillOnce(test::Return(instanceInfoMap));
        auto stateRuningMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
        EXPECT_CALL(*instanceControlView_, GetInstance(Eq("instance1"))).WillOnce(Return(stateRuningMachine));
        EXPECT_CALL(*stateRuningMachine, GetInstanceState).WillRepeatedly(Return(InstanceState::RUNNING));
        resource_view::InstanceInfo runningInstanceInfo;
        runningInstanceInfo.set_instanceid("instance1");
        EXPECT_CALL(*stateRuningMachine, GetInstanceInfo).WillRepeatedly(Return(runningInstanceInfo));
        EXPECT_CALL(*stateRuningMachine, GetVersion).WillRepeatedly(Return(0));
        EXPECT_CALL(*stateRuningMachine, IsSaving()).WillRepeatedly(Return(false));
        EXPECT_CALL(*stateRuningMachine, TransitionToImpl)
            .WillOnce(Return(RUNNING_RESULT))
            .WillOnce(Return(EVICTING_RESULT));
        auto mockRuningSharedClient = std::make_shared<MockSharedClient>();
        EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(Eq("instance1")))
            .WillOnce(Return(mockRuningSharedClient));
        runtime::ShutdownResponse shutdownResponse;
        shutdownResponse.set_code(common::ErrorCode::ERR_NONE);
        EXPECT_CALL(*mockRuningSharedClient, Shutdown).WillOnce(Return(shutdownResponse));

        auto stateExitingMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
        EXPECT_CALL(*instanceControlView_, GetInstance(Eq("instance2"))).WillOnce(Return(stateExitingMachine));
        EXPECT_CALL(*stateExitingMachine, GetInstanceState).WillRepeatedly(Return(InstanceState::EXITING));
        resource_view::InstanceInfo instance;
        instance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::EVICTED));
        EXPECT_CALL(*stateExitingMachine,
                    AddStateChangeCallback(UnorderedElementsAre(InstanceState::FATAL, InstanceState::RUNNING,
                                                                InstanceState::EXITED, InstanceState::EVICTED),
                                           _, _))
            .WillOnce(Invoke([instance](const std::unordered_set<InstanceState> &,
                                        const std::function<void(const resources::InstanceInfo &)> &callback,
                                        const std::string &eventKey) { callback(instance); }));

        auto stateCreatingMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
        EXPECT_CALL(*instanceControlView_, GetInstance(Eq("instance3"))).WillOnce(Return(stateCreatingMachine));
        EXPECT_CALL(*stateCreatingMachine, GetInstanceState).WillRepeatedly(Return(InstanceState::CREATING));
        instance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
        resource_view::InstanceInfo creatingInstanceInfo;
        creatingInstanceInfo.set_instanceid("instance3");
        EXPECT_CALL(*stateCreatingMachine,
                    AddStateChangeCallback(UnorderedElementsAre(InstanceState::FATAL, InstanceState::RUNNING,
                                                                InstanceState::EXITED, InstanceState::EVICTED),
                                           _, _))
            .WillOnce(
                Invoke([creatingInstanceInfo](const std::unordered_set<InstanceState> &,
                                              const std::function<void(const resources::InstanceInfo &)> &callback,
                                              const std::string &eventKey) { callback(creatingInstanceInfo); }));
        EXPECT_CALL(*stateCreatingMachine, GetInstanceInfo).WillRepeatedly(Return(creatingInstanceInfo));
        EXPECT_CALL(*stateCreatingMachine, GetVersion).WillRepeatedly(Return(0));
        EXPECT_CALL(*stateCreatingMachine, IsSaving()).WillRepeatedly(Return(false));
        EXPECT_CALL(*stateCreatingMachine, TransitionToImpl)
            .WillOnce(Return(RUNNING_RESULT))
            .WillOnce(Return(EVICTING_RESULT));
        auto mockCreatingSharedClient = std::make_shared<MockSharedClient>();
        EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(Eq("instance3")))
            .WillOnce(Return(mockCreatingSharedClient));
        EXPECT_CALL(*mockCreatingSharedClient, Shutdown).WillOnce(Return(shutdownResponse));

        EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
        messages::KillInstanceResponse killInstanceRsp;
        killInstanceRsp.set_code(int32_t(common::ErrorCode::ERR_NONE));
        EXPECT_CALL(*funcAgentMgr_, KillInstance).WillRepeatedly(Return(killInstanceRsp));

        EXPECT_CALL(*instanceControlView_, GetInstance(Eq("instance4"))).WillOnce(Return(nullptr));

        auto future = instanceCtrlWithMockObserver_->EvictInstanceOnAgent(req);
        ASSERT_AWAIT_READY(future);
        EXPECT_EQ(future.Get().IsOk(), true);
    }
    {
        auto request = std::make_shared<messages::EvictAgentRequest>();
        request->set_agentid("agentID");
        request->set_requestid("agentID");
        request->set_timeoutsec(1);
        request->add_instances("ins001");
        request->add_instances("ins001");
        request->add_instances("ins002");
        request->add_instances("ins003");
        std::unordered_set<std::string> instanceSet{"ins001", "ins002","ins003" };
        EXPECT_CALL(*instanceControlView_, GetInstance).Times(3).WillRepeatedly(Return(nullptr));
        auto future = instanceCtrlWithMockObserver_->EvictInstances(instanceSet, request, false);
        ASSERT_AWAIT_READY(future);
        EXPECT_EQ(future.Get().IsOk(), true);
    }
}

/**
 * Feature: HandleInstanceHealthChangeTest
 * Description: handle instance health change, and change instance status
 * Steps:
 * 1. handle change to healthy
 * 2. handle change to sub-healthy
 *
 * Expectation:
 * 1. change status to running
 * 2. change status to subHealth
 */
TEST_F(InstanceCtrlTest, HandleInstanceHealthChangeTest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    litebus::Spawn(actor);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView, GetInstance).WillOnce(nullptr).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, IsSaving).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, UpdateInstanceInfo).WillRepeatedly(Return());
    EXPECT_CALL(*stateMachine, SetVersion).WillRepeatedly(Return());
    actor->AddHeartbeatTimer("instanceID1");
    litebus::Async(actor->GetAID(), &InstanceCtrlActor::HandleInstanceHealthChange, "instanceID1", StatusCode::SUCCESS);

    bool isFinished = false;
    EXPECT_CALL(*stateMachine, TransitionToImpl(InstanceState::RUNNING, _, "running", _, StatusCode::SUCCESS))
        .WillOnce(DoAll(Assign(&isFinished, true), Return(RUNNING_RESULT)));
    actor->AddHeartbeatTimer("instanceID2");
    litebus::Async(actor->GetAID(), &InstanceCtrlActor::HandleInstanceHealthChange, "instanceID2", StatusCode::SUCCESS);
    ASSERT_AWAIT_TRUE([&isFinished]() { return isFinished; });

    isFinished = false;
    EXPECT_CALL(*stateMachine,
                TransitionToImpl(InstanceState::SUB_HEALTH, _, "subHealth", _, StatusCode::ERR_INSTANCE_SUB_HEALTH))
        .WillOnce(DoAll(Assign(&isFinished, true), Return(RUNNING_RESULT)));
    actor->AddHeartbeatTimer("instanceID3");
    litebus::Async(actor->GetAID(), &InstanceCtrlActor::HandleInstanceHealthChange, "instanceID3",
                   StatusCode::INSTANCE_SUB_HEALTH);
    ASSERT_AWAIT_TRUE([&isFinished]() { return isFinished; });

    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

TEST_F(InstanceCtrlTest, GetDataAffinityTest)
{
    auto mockDataClient = std::make_shared<MockDataObjClient>();
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindDataObjClient(mockDataClient);
    litebus::Spawn(actor);

    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_instanceid("instanceID");
    scheduleReq->mutable_instance()->set_tenantid("tenantID");
    auto args = scheduleReq->mutable_instance()->mutable_args();
    ::common::Arg arg{};
    arg.set_type(::common::Arg_ArgType_VALUE);
    arg.mutable_nested_refs()->Add("123");
    arg.mutable_nested_refs()->Add("1234");
    args->Add(std::move(arg));

    auto status =
        litebus::Async(actor->GetAID(), &InstanceCtrlActor::GetAffinity, Status(StatusCode::FAILED), scheduleReq);
    EXPECT_AWAIT_READY(status);
    EXPECT_TRUE(status.Get().IsError());

    // data affinity disabled, preemption affinity disable
    scheduleReq->mutable_instance()->mutable_scheduleoption()->set_scheduletimeoutms(30);
    actor->config_.maxPriority = 0;
    status = litebus::Async(actor->GetAID(), &InstanceCtrlActor::GetAffinity, Status::OK(), scheduleReq);
    EXPECT_AWAIT_READY(status);
    EXPECT_TRUE(status.Get().IsOk());
    EXPECT_FALSE(scheduleReq->instance().scheduleoption().affinity().inner().data().has_preferredaffinity());
    EXPECT_EQ(scheduleReq->instance().scheduleoption().scheduletimeoutms(), 30);

    // preemption affinity enable
    (*scheduleReq->mutable_instance()->mutable_createoptions())["DATA_AFFINITY_ENABLED"] = "FALSE";
    scheduleReq->mutable_instance()->mutable_scheduleoption()->set_scheduletimeoutms(30);
    actor->config_.maxPriority = 1;
    actor->config_.enablePreemption = true;
    status = litebus::Async(actor->GetAID(), &InstanceCtrlActor::GetAffinity, Status::OK(), scheduleReq);
    EXPECT_AWAIT_READY(status);
    EXPECT_TRUE(status.Get().IsOk());
    EXPECT_FALSE(scheduleReq->instance().scheduleoption().affinity().inner().data().has_preferredaffinity());
    EXPECT_EQ(scheduleReq->instance().scheduleoption().scheduletimeoutms(), 30); // clear schedule timeouts

    // get list failed
    litebus::Future<std::vector<std::string>> list;
    list.SetFailed(-1);
    litebus::Future<std::vector<std::string>> objIDs;
    EXPECT_CALL(*mockDataClient, GetObjNodeList("tenantID", _))
        .WillOnce(testing::DoAll(FutureArg<1>(&objIDs), Return(list)));
    (*scheduleReq->mutable_instance()->mutable_createoptions())["DATA_AFFINITY_ENABLED"] = "true";
    status = litebus::Async(actor->GetAID(), &InstanceCtrlActor::GetAffinity, Status::OK(), scheduleReq);
    ASSERT_AWAIT_READY(objIDs);
    ASSERT_EQ(objIDs.Get().size(), static_cast<uint32_t>(2));
    EXPECT_EQ(objIDs.Get()[0], "123");
    EXPECT_EQ(objIDs.Get()[1], "1234");

    ASSERT_AWAIT_READY(status);
    EXPECT_TRUE(status.Get().IsOk());
    EXPECT_FALSE(scheduleReq->instance().scheduleoption().affinity().inner().data().has_preferredaffinity());

    ::common::Arg arg2{};
    arg2.set_type(::common::Arg_ArgType_OBJECT_REF);
    arg2.set_value("12345");
    args->Add(std::move(arg2));

    objIDs = {};
    list = {};
    list.SetValue({"123", "234", "345"});
    EXPECT_CALL(*mockDataClient, GetObjNodeList("tenantID", _))
        .WillOnce(testing::DoAll(FutureArg<1>(&objIDs), Return(list)));
    status = litebus::Async(actor->GetAID(), &InstanceCtrlActor::GetAffinity, Status::OK(), scheduleReq);
    EXPECT_AWAIT_READY(objIDs);
    ASSERT_EQ(objIDs.Get().size(), static_cast<uint32_t>(3));
    EXPECT_EQ(objIDs.Get()[0], "123");
    EXPECT_EQ(objIDs.Get()[1], "1234");
    EXPECT_EQ(objIDs.Get()[2], "12345");

    EXPECT_AWAIT_READY(status);
    EXPECT_TRUE(status.Get().IsOk());
    EXPECT_TRUE(scheduleReq->instance().scheduleoption().affinity().inner().data().has_preferredaffinity());
    auto dataPreferredaffinity = scheduleReq->instance().scheduleoption().affinity().inner().data().preferredaffinity();
    EXPECT_EQ(dataPreferredaffinity.condition().subconditions_size(), 3);

    EXPECT_EQ(dataPreferredaffinity.condition().subconditions(0).weight(), 100);
    ASSERT_EQ(dataPreferredaffinity.condition().subconditions(0).expressions().size(), 1);
    EXPECT_EQ(dataPreferredaffinity.condition().subconditions(0).expressions(0).key(), "HOST_IP");
    EXPECT_TRUE(dataPreferredaffinity.condition().subconditions(0).expressions(0).op().has_in());
    ASSERT_EQ(dataPreferredaffinity.condition().subconditions(0).expressions(0).op().in().values_size(), 1);
    EXPECT_EQ(dataPreferredaffinity.condition().subconditions(0).expressions(0).op().in().values()[0], "123");

    EXPECT_EQ(dataPreferredaffinity.condition().subconditions(1).weight(), 90);
    EXPECT_EQ(dataPreferredaffinity.condition().subconditions(1).expressions(0).op().in().values()[0], "234");

    EXPECT_EQ(dataPreferredaffinity.condition().subconditions(2).weight(), 80);
    EXPECT_EQ(dataPreferredaffinity.condition().subconditions(2).expressions(0).op().in().values()[0], "345");

    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

// To scheduling failed by parent exiting
TEST_F(InstanceCtrlTest, ToSchedulingFailedByParentExiting)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_function("function");
    scheduleReq->mutable_instance()->set_parentid("parentID");
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::SCHEDULING));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance("parentID")).WillOnce(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceState).WillOnce(Return(InstanceState::EXITING));

    auto future = instanceCtrl_->ToScheduling(scheduleReq);
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.IsOK(), true);
    EXPECT_EQ(future.Get().StatusCode(), static_cast<int32_t>(StatusCode::ERR_INSTANCE_EXITED));
    EXPECT_EQ(scheduleReq->mutable_instance()->functionproxyid(), "nodeID");
}

// to scheduling failed by instance already exit
TEST_F(InstanceCtrlTest, ToSchedulingFailedByInstanceExisted)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_function("function");
    scheduleReq->mutable_instance()->set_parentid("parentID");
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::NEW));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance("parentID")).WillOnce(Return(nullptr));
    EXPECT_CALL(*instanceControlView_, GetInstance("DesignatedInstanceID")).WillOnce(Return(stateMachine));
    auto future = instanceCtrl_->ToScheduling(scheduleReq);
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.IsOK(), true);
    EXPECT_EQ(future.Get().StatusCode(), static_cast<int32_t>(StatusCode::ERR_INSTANCE_DUPLICATED));
}

// to scheduling success
TEST_F(InstanceCtrlTest, ToSchedulingSuccessful)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_function("function");
    scheduleReq->mutable_instance()->set_parentid("parentID");
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::NEW));
    auto parentStateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    resource_view::InstanceInfo parentInfo;
    parentInfo.set_instanceid("parentID");
    parentInfo.set_tenantid("testTenant");
    parentInfo.set_issystemfunc(false);
    EXPECT_CALL(*parentStateMachine, GetInstanceInfo).WillRepeatedly(Return(parentInfo));
    EXPECT_CALL(*instanceControlView_, GetInstance("parentID"))
        .WillRepeatedly(Return(parentStateMachine));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance("DesignatedInstanceID"))
        .WillOnce(Return(nullptr))
        .WillOnce(Return(stateMachine));
    EXPECT_CALL(*mockObserver_, GetFuncMeta).WillOnce(Return(functionMeta_));
    GeneratedInstanceStates genStates{ "DesignatedInstanceID", InstanceState::NEW, false };
    EXPECT_CALL(*instanceControlView_, TryGenerateNewInstance).WillOnce(Return(genStates));
    EXPECT_CALL(*mockObserver_, PutInstanceEvent).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*mockObserver_, WatchInstance).WillOnce(Return());

    auto future = instanceCtrlWithMockObserver_->ToScheduling(scheduleReq);
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.IsOK(), true);
    EXPECT_EQ(future.Get().StatusCode(), StatusCode::SUCCESS);
    EXPECT_EQ(scheduleReq->mutable_instance()->mutable_instancestatus()->code(),
              static_cast<int32_t>(InstanceState::SCHEDULING));
}

// to Creating without statemachine
TEST_F(InstanceCtrlTest, ToCreatingWithoutStateMachine)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->set_traceid("traceID");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_function("function");
    scheduleReq->mutable_instance()->set_parentid("parentID");
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(instanceCtrlWithMockObserver_->GetActorAID());
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::SCHEDULING));
    scheduleReq->set_requestid("request-" + litebus::uuid_generator::UUID::GetRandomUUID().ToString());

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance("DesignatedInstanceID"))
        .WillOnce(Return(nullptr))
        .WillOnce(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceState)
        .WillOnce(Return(InstanceState::SCHEDULING))
        .WillOnce(Return(InstanceState::SCHEDULING));
    auto registerReadyCallbackPromise = std::make_shared<litebus::Promise<bool>>();
    EXPECT_CALL(*stateMachine, AddStateChangeCallback)
        .WillOnce(DoAll(Invoke([registerReadyCallbackPromise](
                                   const std::unordered_set<InstanceState> &statesConcerned,
                                   const std::function<void(const resources::InstanceInfo &)> &callback,
                                   const std::string &eventKey) { registerReadyCallbackPromise->SetValue(true); })));
    auto callback = std::make_shared<litebus::Promise<Status>>();
    instanceCtrlWithMockObserver_->RegisterReadyCallback(
        "DesignatedInstanceID", scheduleReq, [callback](const Status &status) -> litebus::Future<Status>{
            callback->SetValue(status);
            return Status::OK();
        });
    ASSERT_AWAIT_READY(registerReadyCallbackPromise->GetFuture());

    EXPECT_CALL(*instanceControlView_, GetInstance("DesignatedInstanceID"))
        .WillOnce(Return(nullptr))
        .WillOnce(Return(stateMachine))
        // deploy
        .WillOnce(Return(stateMachine))
        // update instance
        .WillOnce(Return(stateMachine))
        .WillOnce(Return(stateMachine))
        // CreateInstanceClient
        .WillOnce(Return(stateMachine))
        // SendInitRuntime
        .WillOnce(Return(stateMachine))
        // call result
        .WillOnce(Return(stateMachine))
        // call result
        .WillOnce(Return(stateMachine));

    EXPECT_CALL(*stateMachine, IsSaving()).WillRepeatedly(Return(false));
    EXPECT_CALL(*stateMachine, TransitionToImpl(InstanceState::CREATING, _, _, _, _))
        .WillOnce(Return(TransitionResult{ InstanceState::SCHEDULING, InstanceInfo(), InstanceInfo(), 1 }));
    EXPECT_CALL(*mockObserver_, GetFuncMeta).WillOnce(Return(functionMeta_));
    // deploy
    EXPECT_CALL(*stateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillRepeatedly(Return(scheduleReq->instance()));
    EXPECT_CALL(*stateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    messages::DeployInstanceResponse deployInstanceResponse;
    deployInstanceResponse.set_code(StatusCode::SUCCESS);

    auto deployInstanceRequestTraceIDPromise = std::make_shared<litebus::Promise<std::string>>();
    EXPECT_CALL(*funcAgentMgr_, DeployInstance)
        .WillOnce(
            DoAll(Invoke([deployInstanceRequestTraceIDPromise](
                             const std::shared_ptr<messages::DeployInstanceRequest> &request,
                             const std::string &funcAgentID) -> litebus::Future<messages::DeployInstanceResponse> {
                      deployInstanceRequestTraceIDPromise->SetValue(request->traceid());
                      return messages::DeployInstanceResponse();
                  }),
                  Return(deployInstanceResponse)));


    // create client
    EXPECT_CALL(*stateMachine, GetInstanceState)
        .WillOnce(Return(InstanceState::SCHEDULING))
        .WillOnce(Return(InstanceState::SCHEDULING))
        .WillRepeatedly(Return(InstanceState::CREATING));
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, NewControlInterfacePosixClient(_, _, _, _, _, _))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    // readiness
    EXPECT_CALL(*mockSharedClient, Readiness).WillRepeatedly(Return(Status::OK()));
    // heartbeat
    EXPECT_CALL(*mockSharedClient, Heartbeat).WillRepeatedly(Return(Status::OK()));
    // initcall
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    runtime::CallResponse callRsp;
    callRsp.set_code(common::ERR_NONE);
    litebus::Future<runtime::CallRequest> call;
    EXPECT_CALL(*mockSharedClient, InitCallWrapper).WillRepeatedly(DoAll(FutureArg<0>(&call), Return(callRsp)));
    // callresult && to running
    EXPECT_CALL(*stateMachine, TransitionToImpl(InstanceState::RUNNING, _, _, _, _))
        .WillOnce(Return(TransitionResult{ InstanceState::CREATING, InstanceInfo(), InstanceInfo(), 2 }));

    call.OnComplete([=]() {
        YRLOG_INFO("receive call rsp");
        auto callResult = std::make_shared<functionsystem::CallResult>();
        callResult->set_requestid(scheduleReq->requestid());
        callResult->set_instanceid(scheduleReq->instance().parentid());
        instanceCtrlWithMockObserver_->CallResult("DesignatedInstanceID", callResult);
    });

    auto future =
        instanceCtrlWithMockObserver_->ToCreating(scheduleReq, schedule_decision::ScheduleResult{ "agent", 0, {} });
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.IsOK(), true);
    EXPECT_EQ(future.Get().StatusCode(), StatusCode::SUCCESS);
    ASSERT_AWAIT_READY(callback->GetFuture());
    EXPECT_EQ(callback->GetFuture().IsOK(), true);
    EXPECT_EQ(callback->GetFuture().Get().StatusCode(), StatusCode::SUCCESS);

    ASSERT_AWAIT_READY(deployInstanceRequestTraceIDPromise->GetFuture());
    const auto& deployReqTraceID = deployInstanceRequestTraceIDPromise->GetFuture().Get();
    EXPECT_EQ(deployReqTraceID, "traceID");
}

// to Creating failed by etcd error
TEST_F(InstanceCtrlTest, ToCreatingFailedByETCD)
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid("DesignatedInstanceID");
    scheduleReq->set_requestid("requestID");
    scheduleReq->mutable_instance()->set_runtimeid("runtimeid");
    scheduleReq->mutable_instance()->set_functionproxyid("nodeID");
    scheduleReq->mutable_instance()->set_function("function");
    scheduleReq->mutable_instance()->set_parentid("parentID");
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::SCHEDULING));
    scheduleReq->set_requestid("request-" + litebus::uuid_generator::UUID::GetRandomUUID().ToString());

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance("DesignatedInstanceID"))
        .WillOnce(Return(nullptr))
        .WillOnce(Return(stateMachine));
    EXPECT_CALL(*mockObserver_, GetFuncMeta).WillOnce(Return(functionMeta_));
    EXPECT_CALL(*stateMachine, GetScheduleRequest).WillRepeatedly(Return(scheduleReq));
    EXPECT_CALL(*stateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(*stateMachine, TransitionToImpl(_, _, _, _, _))
        .WillOnce(Return(TransitionResult{ InstanceState::SCHEDULING, InstanceInfo(), InstanceInfo(), 0 }));
    auto future =
        instanceCtrlWithMockObserver_->ToCreating(scheduleReq, schedule_decision::ScheduleResult{ "agent", 0, {} });
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.IsOK(), true);
    EXPECT_EQ(future.Get().StatusCode(), StatusCode::ERR_ETCD_OPERATION_ERROR);
}

// force delete without agent
TEST_F(InstanceCtrlTest, ForceDeleteInstanceWithoutAgent)
{
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance("instanceID"))
        .WillRepeatedly(Return(stateMachine));
    {
        EXPECT_CALL(*stateMachine, GetInstanceInfo).WillOnce(Return(InstanceInfo()));
        resource_view::InstanceInfo instance;
        instance.set_instanceid("instanceID");
        instance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
        EXPECT_CALL(*stateMachine, AddStateChangeCallback)
            .WillOnce(Invoke([instance](const std::unordered_set<InstanceState> &statesConcerned,
                                        const std::function<void(const resources::InstanceInfo &)> &callback,
                                        const std::string &eventKey) { callback(instance); }));
        auto callPromise = std::make_shared<litebus::Promise<bool>>();
        EXPECT_CALL(*instanceControlView_, DelInstance)
            .WillRepeatedly(Return(Status::OK()));
        auto future = instanceCtrlWithMockObserver_->ForceDeleteInstance("instanceID");
        ASSERT_AWAIT_READY(future);
        EXPECT_EQ(future.IsOK(), true);
    }

    {
        EXPECT_CALL(*stateMachine, GetInstanceInfo).WillOnce(Return(InstanceInfo()));
        resource_view::InstanceInfo instance;
        instance.set_instanceid("instanceID");
        instance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::EXITED));
        EXPECT_CALL(*stateMachine, AddStateChangeCallback)
            .WillOnce(Invoke([instance](const std::unordered_set<InstanceState> &statesConcerned,
                                        const std::function<void(const resources::InstanceInfo &)> &callback,
                                        const std::string &eventKey) { callback(instance); }));
        auto future = instanceCtrlWithMockObserver_->ForceDeleteInstance("instanceID");
        ASSERT_AWAIT_READY(future);
        EXPECT_EQ(future.IsOK(), true);
    }
}

// force delete
TEST_F(InstanceCtrlTest, ForceDeleteInstance)
{
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    instanceCtrlWithMockObserver_->BindResourceView(resourceViewMgr);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance("instanceID"))
        .WillRepeatedly(Return(stateMachine));
    auto instance = InstanceInfo();
    instance.set_functionagentid("agentID");
    instance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillOnce(Return(instance));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_)).WillOnce(Return(nullptr));
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    messages::KillInstanceResponse killInstanceRsp;
    killInstanceRsp.set_code(int32_t(common::ErrorCode::ERR_NONE));
    EXPECT_CALL(*funcAgentMgr_, KillInstance).WillRepeatedly(Return(killInstanceRsp));
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));
    auto callPromise = std::make_shared<litebus::Promise<bool>>();
    EXPECT_CALL(*instanceControlView_, DelInstance)
        .WillOnce(DoAll(Invoke([callPromise](const std::string &instanceID) {
            callPromise->SetValue(true);
            return Status::OK();
        })));
    instanceCtrlWithMockObserver_->ForceDeleteInstance("instanceID");
    ASSERT_AWAIT_READY(callPromise->GetFuture());
    EXPECT_EQ(callPromise->GetFuture().IsOK(), true);
}

TEST_F(InstanceCtrlTest, DeleteSchedulingInstance)
{
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    instanceCtrlWithMockObserver_->BindResourceView(resourceViewMgr);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance("instanceID"))
        .WillOnce(Return(nullptr))
        .WillOnce(Return(stateMachine));
    auto instance = InstanceInfo();
    instance.set_functionagentid("agentID");
    instance.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::SCHEDULING));
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillOnce(Return(instance));
    EXPECT_CALL(*stateMachine, GetModRevision).WillOnce(Return(1000));
    auto callPromise = std::make_shared<litebus::Promise<bool>>();
    EXPECT_CALL(*mockObserver_, DelInstanceEvent)
        .WillOnce(DoAll(Invoke([callPromise](const std::string &instanceID, int64_t) {
            callPromise->SetValue(true);
            return Status::OK();
        })));
    instanceCtrlWithMockObserver_->DeleteSchedulingInstance("instanceID", "req-1");
    instanceCtrlWithMockObserver_->DeleteSchedulingInstance("instanceID", "req-2");
    ASSERT_AWAIT_READY(callPromise->GetFuture());
    EXPECT_EQ(callPromise->GetFuture().IsOK(), true);
}

/**
 * Feature: instance ctrl.
 * Description: instance ctrl sync instances successfully and recover creating instance.
 * Steps:
 * 1. Mock GetAgentInstanceInfoByID return instanceInfoMap.
 * 2. send request of sync instances.
 * Expectation: don't invoke Reschedule method to functionAgentMgr and check consistency successfully.
 */
TEST_F(InstanceCtrlTest, SyncInstanceKillCreating)
{
    litebus::Future<std::string> observerFuncAgentID;
    function_proxy::InstanceInfoMap instanceInfoMap;
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid("instance1");
    instanceInfo.set_function("function");
    instanceInfo.clear_args();
    instanceInfo.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::CREATING));
    instanceInfoMap.insert({ "instance1", instanceInfo });
    EXPECT_CALL(*mockObserver_.get(), GetAgentInstanceInfoByID(testing::_)).WillOnce(test::Return(instanceInfoMap));

    FunctionMeta functionMeta;
    functionMeta.codeMetaData.storageType = "local";
    EXPECT_CALL(*mockObserver_.get(), GetFuncMeta(testing::_)).Times(0);

    auto resourceUnit = std::make_shared<resource_view::ResourceUnit>();
    resourceUnit->set_id("funcAgentID");
    auto instances = resourceUnit->mutable_instances();
    instances->insert({ "instance1", instanceInfo });
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillOnce(Return(Status::OK()));
    auto killResponse = GenKillInstanceResponse(StatusCode::SUCCESS, "kill instance successfully", "requestID");
    EXPECT_CALL(*funcAgentMgr_.get(), KillInstance(testing::_, testing::_, testing::_))
        .WillOnce(test::Return(killResponse));
    auto syncRet = instanceCtrlWithMockObserver_->SyncInstances(resourceUnit);
    ASSERT_AWAIT_READY(syncRet);
    EXPECT_EQ(syncRet.Get().StatusCode(), StatusCode::SUCCESS);
}

TEST_F(InstanceCtrlTest, OnHealthyStatusTest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    auto mockInstanceOperator = std::make_shared<MockInstanceOperator>();
    actor->instanceOpt_ = mockInstanceOperator;
    actor->BindInstanceControlView(instanceControlView);
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    auto observer = std::make_shared<MockObserver>();
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_));
    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);

    instanceCtrl->OnHealthyStatus(Status(StatusCode::FAILED));
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    function_proxy::InstanceInfoMap instanceInfoMap;

    // not belong to this node
    resource_view::InstanceInfo info1;
    info1.set_instanceid("instance1");
    info1.set_function("function");
    info1.set_functionproxyid("fake_node");
    info1.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::CREATING));
    instanceInfoMap.insert({ "instance1", info1 });
    EXPECT_CALL(*observer, GetAllInstanceInfos).WillRepeatedly(Return(instanceInfoMap));
    instanceCtrl->OnHealthyStatus(Status::OK());

    // state machine nullptr
    instanceInfoMap.clear();
    resource_view::InstanceInfo info2;
    info2.set_instanceid("instance2");
    info2.set_function("default/0-test-helloWorld/$latest");
    info2.set_functionproxyid("nodeID");
    info2.set_jobid("job");
    info2.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::CREATING));
    instanceInfoMap.insert({ "instance2", info2 });
    EXPECT_CALL(*instanceControlView, GetInstance("instance2")).WillRepeatedly(Return(nullptr));

    litebus::Future<OperateResult> result;
    result.SetValue({});
    bool deleted = false;
    EXPECT_CALL(*mockInstanceOperator, ForceDelete).WillRepeatedly(DoAll(Assign(&deleted, true), Return(result)));
    EXPECT_CALL(*observer, GetAllInstanceInfos).WillRepeatedly(Return(instanceInfoMap));
    instanceCtrl->OnHealthyStatus(Status::OK());
    ASSERT_AWAIT_TRUE([&]() { return deleted; });

    instanceInfoMap.clear();
    info2.set_instanceid("instance3");
    instanceInfoMap.insert({ "instance3", info2 });
    EXPECT_CALL(*instanceControlView, GetInstance("instance3")).WillRepeatedly(Return(stateMachine));
    bool skipped = false;
    EXPECT_CALL(*stateMachine, GetLastSaveFailedState).WillOnce(DoAll(Assign(&skipped, true), Return(-1)));
    EXPECT_CALL(*observer, GetAllInstanceInfos).WillOnce(Return(instanceInfoMap));
    instanceCtrl->OnHealthyStatus(Status::OK());
    ASSERT_AWAIT_TRUE([&]() { return skipped; });

    instanceInfoMap.clear();
    info2.set_instanceid("instance4");
    instanceInfoMap.insert({ "instance4", info2 });
    EXPECT_CALL(*instanceControlView, GetInstance("instance4")).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetLastSaveFailedState).WillOnce(Return(3));
    EXPECT_CALL(*stateMachine, ResetLastSaveFailedState).WillOnce(Return());
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillOnce(Return(info2));
    EXPECT_CALL(*stateMachine, UpdateInstanceInfo).WillOnce(Return());
    EXPECT_CALL(*stateMachine, GetOwner).WillOnce(Return("nodeID"));
    EXPECT_CALL(*stateMachine, GetInstanceState()).WillOnce(Return(InstanceState::CREATING));
    EXPECT_CALL(*stateMachine, IsSaving).WillOnce(Return(false));
    EXPECT_CALL(*stateMachine, GetVersion()).WillOnce(Return(0));

    bool fatalTrans = false;
    EXPECT_CALL(*stateMachine, TransitionToImpl(InstanceState::FATAL, _, _, _, _))
        .WillOnce(DoAll(Assign(&fatalTrans, true),
                        Return(TransitionResult{ InstanceState::SUB_HEALTH, InstanceInfo(), InstanceInfo(), 0 })));
    EXPECT_CALL(*observer, GetAllInstanceInfos).WillOnce(Return(instanceInfoMap));
    instanceCtrl->OnHealthyStatus(Status::OK());

    ASSERT_AWAIT_TRUE([&]() { return fatalTrans; });
}

TEST_F(InstanceCtrlTest, InstanceRouteInfoSyncerTest)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<MockInstanceControlView>("nodeID");
    actor->BindInstanceControlView(instanceControlView);
    auto mockInstanceOperator = std::make_shared<MockInstanceOperator>();
    actor->instanceOpt_ = mockInstanceOperator;
    auto observer = std::make_shared<MockObserver>();
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_));
    EXPECT_CALL(*observer, IsSystemFunction).WillRepeatedly(Return(false));
    litebus::Spawn(actor);

    bool fatalTrans = false;
    resource_view::InstanceInfo instanceInfo;
    resource_view::RouteInfo routeInfo;
    routeInfo.set_instanceid("test_id");
    routeInfo.set_requestid("123");
    routeInfo.set_function("0/test/version");
    routeInfo.mutable_instancestatus()->set_code(2);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*stateMachine, GetOwner).WillRepeatedly(Return("nodeID"));
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillRepeatedly(Return(instanceInfo));
    EXPECT_CALL(*stateMachine, GetLastSaveFailedState)
        .WillOnce(Return(2))    // SUB_HEALTH
        .WillOnce(Return(-1));  // INVALID and different state

    litebus::Future<OperateResult> result;
    result.SetValue({});
    EXPECT_CALL(*mockInstanceOperator, ForceDelete).WillOnce(Return(result));
    EXPECT_CALL(*instanceControlView, GetInstance).WillOnce(Return(nullptr));

    // non-exist need force delete
    auto future = actor->InstanceRouteInfoSyncer(routeInfo);
    ASSERT_AWAIT_READY(future);
    ASSERT_FALSE(future.Get().IsOk());

    EXPECT_CALL(*instanceControlView, GetInstance).WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, ResetLastSaveFailedState).WillOnce(Return());
    EXPECT_CALL(*stateMachine, UpdateInstanceInfo).WillOnce(Return());
    EXPECT_CALL(*stateMachine, GetInstanceState()).WillOnce(Return(InstanceState::SUB_HEALTH));
    EXPECT_CALL(*stateMachine, IsSaving).WillOnce(Return(false));
    EXPECT_CALL(*stateMachine, GetVersion()).WillOnce(Return(0));
    EXPECT_CALL(*stateMachine, TransitionToImpl(InstanceState::FATAL, _, _, _, _))
        .WillOnce(DoAll(Assign(&fatalTrans, true),
                        Return(TransitionResult{ InstanceState::SUB_HEALTH, InstanceInfo(), InstanceInfo(), 0 })));

    future = actor->InstanceRouteInfoSyncer(routeInfo);
    ASSERT_AWAIT_READY(future);
    ASSERT_TRUE(future.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() { return fatalTrans; });

    fatalTrans = false;
    EXPECT_CALL(*stateMachine, GetInstanceState())
        .WillOnce(Return(InstanceState::SUB_HEALTH))
        .WillOnce(Return(InstanceState::SUB_HEALTH));
    EXPECT_CALL(*stateMachine, IsSaving).WillOnce(Return(false));
    EXPECT_CALL(*stateMachine, GetVersion()).WillOnce(Return(1)).WillOnce(Return(1));
    EXPECT_CALL(*stateMachine, TransitionToImpl(InstanceState::SUB_HEALTH, _, _, _, _))
        .WillOnce(DoAll(Assign(&fatalTrans, true),
                        Return(TransitionResult{ InstanceState::FATAL, InstanceInfo(), InstanceInfo(), 0 })));
    EXPECT_CALL(*stateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));

    // different status update etcd
    routeInfo.mutable_instancestatus()->set_code(3);
    future = actor->InstanceRouteInfoSyncer(routeInfo);
    ASSERT_AWAIT_READY(future);
    ASSERT_TRUE(future.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() { return fatalTrans; });

    litebus::Terminate(actor->GetAID());
    litebus::Await(actor);
}

TEST_F(InstanceCtrlTest, KillToFatalTest)
{
    const std::string instanceID = "InstanceA";
    const std::string funcAgentID = "funcAgentA";
    const std::string function = "default/0-test-helloWorld/$latest";
    const std::string runtimeID = "runtimeA";
    const std::string functionProxyID = "nodeID";

    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    instanceCtrlWithMockObserver_->BindResourceView(resourceViewMgr);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance)
        .WillRepeatedly(Return(stateMachine));
    resources::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(instanceID);
    instanceInfo.mutable_instancestatus()->set_code(int32_t(InstanceState::RUNNING));
    instanceInfo.set_functionagentid(funcAgentID);
    instanceInfo.set_function(function);
    instanceInfo.set_runtimeid(runtimeID);
    instanceInfo.set_functionproxyid(functionProxyID);
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instanceInfo);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);
    EXPECT_CALL(*stateMachine, GetInstanceContextCopy).WillRepeatedly(Return(instanceContext));
    EXPECT_CALL(*funcAgentMgr_, IsFuncAgentRecovering(testing::_)).WillOnce(Return(true));

    EXPECT_CALL(*stateMachine, GetVersion).WillRepeatedly(Return(0));
    EXPECT_CALL(*stateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(*stateMachine, TransitionToImpl).WillOnce(Return(FATAL_RESULT));
    EXPECT_CALL(*stateMachine, GetCancelFuture).WillOnce(Return(litebus::Future<std::string>()));

    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    runtime::ShutdownResponse shutdownResponse;
    shutdownResponse.set_code(common::ErrorCode::ERR_NONE);
    EXPECT_CALL(*mockSharedClient, Shutdown).WillOnce(Return(shutdownResponse));
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    messages::KillInstanceResponse killInstanceRsp;
    killInstanceRsp.set_code(int32_t(common::ErrorCode::ERR_NONE));
    EXPECT_CALL(*funcAgentMgr_, KillInstance).WillRepeatedly(Return(killInstanceRsp));
    EXPECT_CALL(*primary, DeleteInstances).WillRepeatedly(Return(Status::OK()));
    auto callPromise = std::make_shared<litebus::Promise<bool>>();
    auto killReq = std::make_shared<KillRequest>();
    killReq->set_instanceid("instanceID");
    killReq->set_signal(5);
    auto future = instanceCtrlWithMockObserver_->Kill("src", killReq);
    ASSERT_AWAIT_READY(future);
    auto resp = future.Get();
    EXPECT_EQ(resp.code(), ::common::ErrorCode::ERR_GROUP_EXIT_TOGETHER);
}

TEST_F(InstanceCtrlTest, ForwardCallResultRequestForLowReliability)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActor", "nodeID", instanceCtrlConfig);
    actor->BindObserver(mockObserver_);
    litebus::Spawn(actor);

    ::internal::ForwardCallResultRequest req{};
    ::core_service::CallResult callResult{};
    callResult.set_requestid("reqid");
    *req.mutable_req() = std::move(callResult);
    req.mutable_readyinstance()->set_lowreliability(true);

    actor->ForwardCallResultRequest(litebus::AID(), "", req.SerializeAsString());

    auto instanceControlView = std::make_shared<InstanceControlView>("node1", false);
    actor->BindInstanceControlView(instanceControlView);

    const std::string function = "default/0-test-helloWorld/$latest";
    const std::string instanceID = "instance id";
    const std::string requestID = "request id";
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->set_instanceid(instanceID);
    scheduleReq->mutable_instance()->set_requestid(requestID);
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::SCHEDULING));
    scheduleReq->mutable_instance()->set_function(function);
    scheduleReq->mutable_instance()->set_functionproxyid("1");
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(5);
    auto request = std::make_shared<messages::ScheduleRequest>();
    request->set_requestid(scheduleReq->instance().requestid());
    request->set_traceid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    request->mutable_instance()->CopyFrom(scheduleReq->instance());

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    auto &mockStateMachine = *stateMachine;
    stateMachine->instanceContext_ = std::make_shared<InstanceContext>(request);
    EXPECT_CALL(mockStateMachine, GetInstanceInfo).WillRepeatedly(Return(request->instance()));
    EXPECT_CALL(mockStateMachine, DelInstance).WillRepeatedly(Return(Status::OK()));
    EXPECT_CALL(mockStateMachine, GetRequestID).WillRepeatedly(Return(requestID));

    ::internal::ForwardCallResultResponse response;
    response.set_code(static_cast<common::ErrorCode>(StatusCode::ERR_INSTANCE_EXITED));
    response.set_requestid(requestID);
    response.set_instanceid(instanceID);
    using ForwardCallResultPromise = litebus::Promise<::internal::ForwardCallResultResponse>;
    auto promise = std::make_shared<ForwardCallResultPromise>();
    actor->forwardCallResultPromise_[requestID] = promise;
    actor->instanceControlView_->machines_[instanceID] = stateMachine;

    litebus::AID aid;
    actor->ForwardCallResultResponse(aid, "", std::move(response.SerializeAsString()));
    ASSERT_AWAIT_TRUE([&]() { return actor->instanceControlView_->GetInstance(instanceID) == nullptr; });

    litebus::Terminate(actor->GetAID());
    litebus::Await(actor);
}

TEST_F(InstanceCtrlTest, KillFatalInstance)
{
    const std::string instanceID = "InstanceA";
    const std::string funcAgentID = "funcAgentA";
    const std::string function = "default/0-test-helloWorld/$latest";
    const std::string runtimeID = "runtimeA";
    const std::string functionProxyID = "InstanceManagerOwner";

    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    resourceViewMgr->virtual_ = MockResourceView::CreateMockResourceView();
    auto localSchedSrv = std::make_shared<MockLocalSchedSrv>();

    instanceCtrlWithMockObserver_->BindLocalSchedSrv(localSchedSrv);
    instanceCtrlWithMockObserver_->BindResourceView(resourceViewMgr);

    instanceCtrlWithMockObserver_->instanceCtrlActor_->observer_ = mockObserver_;
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance)
        .WillRepeatedly(Return(stateMachine));
    resources::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(instanceID);
    instanceInfo.set_functionagentid(funcAgentID);
    instanceInfo.set_function(function);
    instanceInfo.mutable_instancestatus()->set_code(int32_t(InstanceState::FATAL));
    instanceInfo.set_runtimeid(runtimeID);
    instanceInfo.set_functionproxyid(functionProxyID);
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instanceInfo);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);
    EXPECT_CALL(*stateMachine, GetInstanceContextCopy)
        .WillOnce(Return(instanceContext))
        .WillOnce(Return(instanceContext));
    EXPECT_CALL(*stateMachine, GetCancelFuture).WillRepeatedly(Return(litebus::Future<std::string>()));
    EXPECT_CALL(*stateMachine, GetVersion).WillRepeatedly(Return(0));

    // instance kill success
    messages::ForwardKillResponse response;
    response.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<messages::ForwardKillRequest>> request;
    EXPECT_CALL(*localSchedSrv, ForwardKillToInstanceManager).WillOnce(DoAll(FutureArg<0>(&request), Return(response)));

    EXPECT_CALL(*mockObserver_, DelInstanceEvent).WillOnce(Return(Status::OK()));
    auto callPromise = std::make_shared<litebus::Promise<bool>>();
    auto killReq = std::make_shared<KillRequest>();
    killReq->set_instanceid(instanceID);
    killReq->set_signal(3);
    auto future = instanceCtrlWithMockObserver_->Kill("src", killReq);
    ASSERT_AWAIT_READY(future);
    auto resp = future.Get();
    EXPECT_EQ(resp.code(), (int32_t)StatusCode::SUCCESS);

    // instance kill failed
    response.set_code(StatusCode::GRPC_DEADLINE_EXCEEDED);
    EXPECT_CALL(*localSchedSrv, ForwardKillToInstanceManager).WillOnce(DoAll(FutureArg<0>(&request), Return(response)));
    future = instanceCtrlWithMockObserver_->Kill("src", killReq);
    ASSERT_AWAIT_READY(future);
    resp = future.Get();
    EXPECT_EQ(resp.code(), (int32_t)StatusCode::ERR_INNER_SYSTEM_ERROR);
}

/**
 * PersistentNewToSchedulingFailed
 * Test Create instance, transition New to Scheduling failed
 * Steps:
 * 1. MockObserver (GetFuncMeta() => defaultMeta / IsSystemFunction() => False)
 * 2. MockTxnTransaction (Commit => false)
 * 3. MockMetaStoreClient (BeginTransaction => mockTxnTransaction)
 * 4. MockSharedClient (NotifyResult => capture NotifyRequest)
 *
 * Expectations:
 * 1. instance state in scheduleReq == SCHEDULING
 * 2  instance state in stateMachine == NEW
 * 3. result.code() == FAILED
 * 4. runtimePromise.code() == FAILED
 */
TEST_F(InstanceCtrlTest, PersistentNewToSchedulingFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActorTest", "nodeID", instanceCtrlConfig);
    auto instanceControlView = std::make_shared<InstanceControlView>("nodeID", false);
    actor->BindInstanceControlView(instanceControlView);

    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_)); // mock get function successfully
    instanceCtrl->BindInternalIAM(mockInternalIAM_); // mock iam successfully

    auto scheduler = std::make_shared<MockScheduler>();
    instanceCtrl->BindScheduler(scheduler);

    auto metaClient = std::make_shared<MockMetaStoreClient>(metaStoreServerHost_);
    instanceControlView->BindMetaStoreClient(metaClient);
    auto mockTxnTransaction = std::make_shared<MockTxnTransaction>(litebus::AID());
    EXPECT_CALL(*metaClient, BeginTransaction).WillRepeatedly(testing::Return(mockTxnTransaction));

    auto txnResponseSuccess = std::make_shared<TxnResponse>();
    txnResponseSuccess->success = false;
    txnResponseSuccess->responses.emplace_back(TxnOperationResponse());
    txnResponseSuccess->responses.emplace_back(TxnOperationResponse());
    EXPECT_CALL(*mockTxnTransaction, Commit)
        .WillOnce(Return(litebus::Future<std::shared_ptr<TxnResponse>>(txnResponseSuccess)));

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto scheduleReq = GenScheduleReq(actor);
    scheduleReq->mutable_instance()->clear_parentid();

    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    auto runtimeFuture = runtimePromise->GetFuture();
    ASSERT_AWAIT_READY(runtimeFuture);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), StatusCode::ERR_ETCD_OPERATION_ERROR);
    EXPECT_EQ(runtimePromise->GetFuture().Get().code(), StatusCode::ERR_ETCD_OPERATION_ERROR);

    EXPECT_EQ(scheduleReq->instance().instancestatus().code(), static_cast<int32_t>(InstanceState::SCHEDULING));
    auto machine = instanceControlView->GetInstance("DesignatedInstanceID");
    EXPECT_EQ(machine->GetInstanceState(), InstanceState::NEW); // Pointer points to the same address
}

/**
 * PersistentSchedulingToCreatingFailed
 * Test Create instance, transition Scheduling to Creating failed
 * Steps:
 * 1. MockObserver (GetFuncMeta() => defaultMeta / IsSystemFunction() => False)
 * 2. MockScheduler (ScheduleDecision => SUCCESS)
 * 3. MockTxnTransaction (Commit => true => false)
 * 4. MockMetaStoreClient (BeginTransaction => mockTxnTransaction)
 *
 * Expectations:
 * 1. instance state in scheduleReq == CREATING
 * 2  instance state in stateMachine == SCHEDULING
 * 3. result.code() == 0
 * 4. runtimePromise.code() == 0
 *
 * Notice:
 * If this error occurs in the current process, the notifyResult message is not sent.
 * Because DeployInstance return StatusCode::LS_UPDATE_INSTANCE_FAIL,
 * which causes no notifyResult to be send in ScheduleEnd
 */
TEST_F(InstanceCtrlTest, DISABLED_PersistentSchedulingToCreatingFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActorTest", "nodeID", instanceCtrlConfig);
    auto mockSharedClient = std::make_shared<MockSharedClient>();

    auto instanceControlView = std::make_shared<InstanceControlView>("nodeID", false);
    actor->BindInstanceControlView(instanceControlView);
    EXPECT_CALL(*mockSharedClient, Heartbeat).WillRepeatedly(Return(Status::OK())); // mock hearbeat
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);

    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);
    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);

    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_)); // mock get function successfully
    instanceCtrl->BindInternalIAM(mockInternalIAM_); // mock iam successfully

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_)).WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, "" }));
    instanceCtrl->BindScheduler(scheduler);

    auto metaClient = std::make_shared<MockMetaStoreClient>(metaStoreServerHost_);
    instanceControlView->BindMetaStoreClient(metaClient);

    auto mockTxnTransaction = std::make_shared<MockTxnTransaction>(litebus::AID());
    EXPECT_CALL(*metaClient, BeginTransaction).WillRepeatedly(testing::Return(mockTxnTransaction));

    auto txnResponseSuccess = std::make_shared<TxnResponse>();
    txnResponseSuccess->success = true;
    txnResponseSuccess->responses.emplace_back(TxnOperationResponse());
    txnResponseSuccess->responses.emplace_back(TxnOperationResponse());

    auto txnResponseFail = std::make_shared<TxnResponse>();
    txnResponseFail->success = false;
    txnResponseFail->responses.emplace_back(TxnOperationResponse());
    txnResponseFail->responses.emplace_back(TxnOperationResponse());

    EXPECT_CALL(*mockTxnTransaction, Commit)
        .WillOnce(Return(litebus::Future<std::shared_ptr<TxnResponse>>(txnResponseSuccess)))
        .WillOnce(Return(litebus::Future<std::shared_ptr<TxnResponse>>(txnResponseFail)))
        .WillOnce(Return(litebus::Future<std::shared_ptr<TxnResponse>>(txnResponseSuccess)));

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto scheduleReq = GenScheduleReq(actor);
    scheduleReq->mutable_instance()->clear_parentid();

    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY(result);
    auto runtimeFuture = runtimePromise->GetFuture();
    ASSERT_AWAIT_READY(runtimeFuture);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), StatusCode::ERR_ETCD_OPERATION_ERROR);
    EXPECT_EQ(runtimePromise->GetFuture().Get().code(), 0);

    ASSERT_AWAIT_TRUE([&]() {
        return scheduleReq->instance().instancestatus().code() == static_cast<int32_t>(InstanceState::FATAL);
    });
    auto machine = instanceControlView->GetInstance("DesignatedInstanceID");
    EXPECT_EQ(machine->GetInstanceState(), InstanceState::FATAL);
}


/**
 * PersistentCreatingToRunningFailed
 * Test Create instance, transition Creating to Running failed
 * Steps:
 * 1. MockObserver (GetFuncMeta() => defaultMeta / IsSystemFunction() => False)
 * 2. MockScheduler (ScheduleDecision => SUCCESS)
 * 3. MockTxnTransaction (Commit => true => true => false)
 * 4. MockMetaStoreClient (BeginTransaction => mockTxnTransaction)
 * 5. MockSharedClient (NotifyResult => capture NotifyRequest)
 *
 * Expectations:
 * 1. instance state in scheduleReq == RUNNING
 * 2  instance state in stateMachine == CREATING
 * 3. result.code() == 0
 * 4. runtimePromise.code() == 0
 * 5. NotifyRequest == ERR_ETCD_OPERATION_ERROR
 */
TEST_F(InstanceCtrlTest, PersistentCreatingToRunningFailed)
{
    auto actor = std::make_shared<InstanceCtrlActor>("InstanceCtrlActorTest", "nodeID", instanceCtrlConfig);
    auto mockSharedClient = std::make_shared<MockSharedClient>();

    EXPECT_CALL(*mockSharedClientManagerProxy_, NewControlInterfacePosixClient(_, _, _, _, _, _))
        .WillOnce(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));

    EXPECT_CALL(*mockSharedClient, Readiness).WillOnce(Return(Status::OK()));
    litebus::Promise<runtime::NotifyRequest> notifyCalled;
    EXPECT_CALL(*mockSharedClient, NotifyResult(_))
        .WillRepeatedly(
            Invoke([notifyCalled](runtime::NotifyRequest &&request) -> litebus::Future<runtime::NotifyResponse> {
                notifyCalled.SetValue(request);
                return runtime::NotifyResponse();
            }));  // for mock SendNotifyResult

    EXPECT_CALL(*mockSharedClient, Heartbeat).WillRepeatedly(Return(Status::OK())); // mock hearbeat
    actor->BindControlInterfaceClientManager(mockSharedClientManagerProxy_);
    auto instanceControlView = std::make_shared<InstanceControlView>("nodeID", false);
    actor->BindInstanceControlView(instanceControlView);

    auto observer = std::make_shared<MockObserver>();
    auto instanceCtrl = std::make_shared<InstanceCtrl>(actor);

    instanceCtrl->Start(nullptr, mockResourceViewMgr_, observer);
    ASSERT_TRUE(observer != nullptr);
    EXPECT_CALL(*observer, GetFuncMeta).WillRepeatedly(Return(functionMeta_)); // mock get function successfully
    instanceCtrl->BindInternalIAM(mockInternalIAM_); // mock iam successfully

    auto scheduler = std::make_shared<MockScheduler>();
    EXPECT_CALL(*scheduler, ScheduleDecision(_)).WillOnce(Return(ScheduleResult{ "", StatusCode::SUCCESS, "" }));
    EXPECT_CALL(*scheduler, ScheduleConfirm).Times(1); // mock schedule successfully
    instanceCtrl->BindScheduler(scheduler);

    auto metaClient = std::make_shared<MockMetaStoreClient>(metaStoreServerHost_);
    instanceControlView->BindMetaStoreClient(metaClient);
    instanceControlView->GenerateStateMachine(
        "DesignatedParentID", GenParentInstanceInfo("DesignatedParentID", actor->GetAID(), "tenant-parent"));

    auto txnResponseSuccess = std::make_shared<TxnResponse>();
    txnResponseSuccess->success = true;
    txnResponseSuccess->responses.emplace_back(TxnOperationResponse());
    txnResponseSuccess->responses.emplace_back(TxnOperationResponse());

    auto mockTxnTransaction = std::make_shared<MockTxnTransaction>(litebus::AID());
    EXPECT_CALL(*metaClient, BeginTransaction).WillRepeatedly(testing::Return(mockTxnTransaction));

    auto txnResponseFail = std::make_shared<TxnResponse>();
    txnResponseFail->success = false;
    txnResponseFail->responses.emplace_back(TxnOperationResponse());
    txnResponseFail->responses.emplace_back(TxnOperationResponse());

    EXPECT_CALL(*mockTxnTransaction, Commit)
        .WillOnce(Return(litebus::Future<std::shared_ptr<TxnResponse>>(txnResponseSuccess)))
        .WillOnce(Return(litebus::Future<std::shared_ptr<TxnResponse>>(txnResponseSuccess)))
        .WillOnce(Return(litebus::Future<std::shared_ptr<TxnResponse>>(txnResponseFail)))
        .WillOnce(Return(litebus::Future<std::shared_ptr<TxnResponse>>(txnResponseFail)));

    auto functionAgentMgr = std::make_shared<MockFunctionAgentMgr>("funcAgentMgr", metaClient);
    messages::DeployInstanceResponse deployInstanceResponse;
    deployInstanceResponse.set_code(StatusCode::SUCCESS);
    EXPECT_CALL(*functionAgentMgr, DeployInstance).WillOnce(Return(deployInstanceResponse));
    instanceCtrl->BindFunctionAgentMgr(functionAgentMgr);

    litebus::Future<runtime::CallResponse> sendRet;
    runtime::CallResponse response;
    sendRet.SetValue(response);
    litebus::Future<runtime::CallRequest> call;
    EXPECT_CALL(*mockSharedClient, InitCallWrapper).WillRepeatedly(DoAll(FutureArg<0>(&call), Return(sendRet)));
    call.OnComplete([instanceCtrl]() {
        auto callResult = std::make_shared<functionsystem::CallResult>();
        instanceCtrl->CallResult("DesignatedInstanceID", callResult);
        instanceCtrl->CallResult("DesignatedInstanceID", callResult);
    });

    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto scheduleReq = GenScheduleReq(actor);

    auto result = instanceCtrl->Schedule(scheduleReq, runtimePromise);
    auto runtimeFuture = runtimePromise->GetFuture();
    ASSERT_AWAIT_READY(runtimeFuture);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), 0);
    EXPECT_EQ(runtimePromise->GetFuture().Get().code(), 0);

    ASSERT_AWAIT_READY(notifyCalled.GetFuture());
    EXPECT_EQ(static_cast<int32_t>(notifyCalled.GetFuture().Get().code()), static_cast<int32_t>(StatusCode::ERR_ETCD_OPERATION_ERROR));
    ASSERT_AWAIT_TRUE([&]() { return scheduleReq->instance().instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING); });
    auto machine = instanceControlView->GetInstance("DesignatedInstanceID");
    EXPECT_EQ(machine->GetInstanceState(), InstanceState::CREATING);
}

TEST_F(InstanceCtrlTest, KillResourceGroup)
{
    auto killReq = GenKillRequest("rg", REMOVE_RESOURCE_GROUP);
    auto srcInstance = "instanceM";

    auto mockResourceGroupCtrl = std::make_shared<MockResourceGroupCtrl>();

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeID");
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine));
    resource_view::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(srcInstance);
    instanceInfo.set_tenantid("tenantID");
    EXPECT_CALL(*stateMachine, GetInstanceInfo).WillOnce(Return(instanceInfo));
    instanceCtrl_->BindResourceGroupCtrl(mockResourceGroupCtrl);
    EXPECT_CALL(*mockResourceGroupCtrl, Kill(StrEq(srcInstance), StrEq(instanceInfo.tenantid()), _))
        .WillOnce(testing::Return(KillResponse()));

    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);
}

TEST_F(InstanceCtrlTest, KillInstanceWithTransSuspend)
{
    const std::string instanceID = "InstanceA";
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(nullptr));
    auto killReq = GenKillRequest(instanceID, INSTANCE_TRANS_SUSPEND_SIGNAL);
    auto srcInstance = "instanceM";
    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_INSTANCE_NOT_FOUND);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
    auto &mockStateMachine = *stateMachine;

    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetInstanceState()).WillOnce(Return(InstanceState::EXITING));
    killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_STATE_MACHINE_ERROR);

    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetInstanceState()).WillOnce(Return(InstanceState::SUSPEND));
    killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);

    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetInstanceState()).WillOnce(Return(InstanceState::RUNNING));
    EXPECT_CALL(mockStateMachine, IsSaving).WillOnce(Return(false));
    EXPECT_CALL(mockStateMachine, TransitionToImpl(InstanceState::SUSPEND, _, _, _, _))
        .WillOnce(Return(TransitionResult{ InstanceState::RUNNING, InstanceInfo(), InstanceInfo(), 1 }));
    EXPECT_CALL(*mockSharedClientManagerProxy_, DeleteClient(_)).WillRepeatedly(Return(Status::OK()));
    auto resourceViewMgr = std::make_shared<resource_view::ResourceViewMgr>();
    auto primary = MockResourceView::CreateMockResourceView();
    resourceViewMgr->primary_ = primary;
    EXPECT_CALL(*primary, DeleteInstances).WillOnce(Return(Status::OK()));
    instanceCtrl_->BindResourceView(resourceViewMgr);
    messages::KillInstanceResponse killInstanceRsp;
    killInstanceRsp.set_code(int32_t(common::ErrorCode::ERR_NONE));
    EXPECT_CALL(*funcAgentMgr_, KillInstance).WillRepeatedly(Return(killInstanceRsp));
    killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);
}

TEST_F(InstanceCtrlTest, KillInstanceWithCheckpoint)
{
    const std::string instanceID = "InstanceA";
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(nullptr));
    auto killReq = GenKillRequest(instanceID, INSTANCE_CHECKPOINT_SIGNAL);
    auto srcInstance = "instanceM";
    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_INSTANCE_NOT_FOUND);

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
    auto &mockStateMachine = *stateMachine;

    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetInstanceState()).WillOnce(Return(InstanceState::EXITING));
    killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_STATE_MACHINE_ERROR);

    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetInstanceState()).WillOnce(Return(InstanceState::SUSPEND));
    killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);

    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetInstanceState()).WillOnce(Return(InstanceState::RUNNING));
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    runtime::CheckpointResponse checkpointRsp;
    checkpointRsp.set_code(common::ErrorCode::ERR_NONE);
    checkpointRsp.set_state("");
    EXPECT_CALL(*mockSharedClient, Checkpoint).WillOnce(Return(checkpointRsp));
    killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);
}

TEST_F(InstanceCtrlTest, KillInstanceWithCheckpointErrorRetry)
{
    auto srcInstance = "instanceM";
    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillOnce(Return(stateMachine));
    EXPECT_CALL(mockStateMachine, GetInstanceState()).WillOnce(Return(InstanceState::RUNNING));
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    runtime::CheckpointResponse busyCheckpointRsp;
    busyCheckpointRsp.set_code(common::ErrorCode::ERR_INSTANCE_BUSY);
    busyCheckpointRsp.set_state("");
    runtime::CheckpointResponse normalCheckpointRsp;
    normalCheckpointRsp.set_code(common::ErrorCode::ERR_NONE);
    normalCheckpointRsp.set_state("");
    EXPECT_CALL(*mockSharedClient, Checkpoint).WillOnce(Return(busyCheckpointRsp)).WillOnce(Return(busyCheckpointRsp))
        .WillOnce(Return(normalCheckpointRsp));
    auto killReq = GenKillRequest(instanceID, INSTANCE_CHECKPOINT_SIGNAL);
    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq).Get();
    EXPECT_EQ(killRsp.code(), common::ErrorCode::ERR_NONE);
}

TEST_F(InstanceCtrlTest, KillDriverInstance)
{
    const std::string instanceID = "driver-job_12345";
    const std::string function = "default/0-test-helloWorld/$latest";
    const std::string runtimeID = "runtimeA";
    const std::string functionProxyID = "nodeN";

    auto stateMachine = std::make_shared<MockInstanceStateMachine>("nodeN");
    auto &mockStateMachine = *stateMachine;
    EXPECT_CALL(*instanceControlView_, GetInstance).WillRepeatedly(Return(stateMachine));
    resources::InstanceInfo instanceInfo;
    instanceInfo.set_instanceid(instanceID);
    instanceInfo.mutable_instancestatus()->set_code(int32_t(InstanceState::RUNNING));
    instanceInfo.set_function(function);
    instanceInfo.set_runtimeid(runtimeID);
    instanceInfo.set_functionproxyid(functionProxyID);
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    scheduleReq->mutable_instance()->CopyFrom(instanceInfo);
    auto instanceContext = std::make_shared<InstanceContext>(scheduleReq);
    EXPECT_CALL(mockStateMachine, GetInstanceContextCopy).WillRepeatedly(Return(instanceContext));
    EXPECT_CALL(mockStateMachine, TagStop).WillRepeatedly(Return());
    EXPECT_CALL(*funcAgentMgr_, IsFuncAgentRecovering(testing::_)).WillRepeatedly(Return(true));
    auto mockSharedClient = std::make_shared<MockSharedClient>();
    EXPECT_CALL(*mockSharedClientManagerProxy_, GetControlInterfacePosixClient(_))
        .WillRepeatedly(Return(mockSharedClient));
    EXPECT_CALL(*mockSharedClient, IsDone).WillOnce(Return(false));
    auto killReq = GenKillRequest(instanceID, SHUT_DOWN_SIGNAL);
    auto srcInstance = "instanceM";

    auto killRsp = instanceCtrl_->Kill(srcInstance, killReq);
    ASSERT_AWAIT_READY(killRsp);
    EXPECT_EQ(static_cast<int>(killRsp.Get().code()), static_cast<int>(common::ErrorCode::ERR_NONE));
}

class InitialLowReliabilityRouteConflictActorTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        previousDirectRoutingEnabled_ = DirectRoutingConfig::IsEnabled();
        previousForceLowReliabilityEnabled_ = IsForceLowReliabilityInstanceEnabled();
        DirectRoutingConfig::SetEnabled(false);
        SetForceLowReliabilityInstance(false);

        auto suffix = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        actor_ = std::make_shared<MockInstanceCtrlActor>(
            "InitialLowReliabilityRouteConflictActor-" + suffix, LOSER_PROXY_ID, instanceCtrlConfig);
        instanceControlView_ = std::make_shared<MockInstanceControlView>(LOSER_PROXY_ID);
        clientManager_ = std::make_shared<MockSharedClientManagerProxy>();
        functionAgentMgr_ = std::make_shared<MockFunctionAgentMgr>("RouteConflictFunctionAgentMgr-" + suffix, nullptr);
        resourceViewMgr_ = std::make_shared<resource_view::ResourceViewMgr>();
        primary_ = MockResourceView::CreateMockResourceView();
        virtual_ = MockResourceView::CreateMockResourceView();
        resourceViewMgr_->primary_ = primary_;
        resourceViewMgr_->virtual_ = virtual_;
        observer_ = std::make_shared<MockObserver>();
        InternalIAM::Param internalIAMParam;
        internalIAMParam.isEnableIAM = false;
        internalIAM_ = std::make_shared<MockInternalIAM>(internalIAMParam);
        ON_CALL(*internalIAM_, IsIAMEnabled()).WillByDefault(Return(false));

        actor_->BindInstanceControlView(instanceControlView_);
        actor_->BindControlInterfaceClientManager(clientManager_);
        actor_->BindFunctionAgentMgr(functionAgentMgr_);
        actor_->BindResourceView(resourceViewMgr_);
        actor_->BindInternalIAM(internalIAM_);
        // Binding a MockObserver registers several unrelated callbacks. The lightweight
        // conflict helper only needs winner proxy resolution, so set it directly.
        actor_->observer_ = observer_;
        litebus::Spawn(actor_);

        auto &metricsContext = metrics::MetricsAdapter::GetInstance().GetMetricsContext();
        previousEnabledInstruments_ = metricsContext.GetEnabledInstruments();
        metricsContext.SetEnabledInstruments({ metrics::YRInstrument::YR_INSTANCE_RUNNING_DURATION });
    }

    void TearDown() override
    {
        if (actor_ != nullptr) {
            litebus::Terminate(actor_->GetAID());
            litebus::Await(actor_);
        }
        auto &metricsContext = metrics::MetricsAdapter::GetInstance().GetMetricsContext();
        metricsContext.EraseBillingInstanceItem(INSTANCE_ID);
        metricsContext.SetEnabledInstruments(previousEnabledInstruments_);
        DirectRoutingConfig::SetEnabled(previousDirectRoutingEnabled_);
        SetForceLowReliabilityInstance(previousForceLowReliabilityEnabled_);
    }

    InstanceInfo MakeLoserInfo() const
    {
        InstanceInfo loser;
        loser.set_instanceid(INSTANCE_ID);
        loser.set_requestid(LOSER_REQUEST_ID);
        loser.set_runtimeid(LOSER_RUNTIME_ID);
        loser.set_functionagentid(LOSER_AGENT_ID);
        loser.set_functionproxyid(LOSER_PROXY_ID);
        loser.set_function(FUNCTION_KEY);
        loser.set_tenantid(TENANT_ID);
        loser.set_jobid(JOB_ID);
        loser.set_parentid(PARENT_INSTANCE_ID);
        loser.set_parentfunctionproxyaid(PARENT_PROXY_AID);
        loser.set_version(0);
        loser.set_lowreliability(true);
        loser.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::CREATING));
        (*loser.mutable_createoptions())[RELIABILITY_TYPE] = "low";
        return loser;
    }

    InstanceInfo MakeWinnerInfo() const
    {
        auto winner = MakeLoserInfo();
        winner.set_requestid(WINNER_REQUEST_ID);
        winner.set_runtimeid("winner-runtime");
        winner.set_functionagentid("winner-agent");
        winner.set_functionproxyid(WINNER_PROXY_ID);
        winner.set_version(1);
        winner.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
        return winner;
    }

    std::shared_ptr<functionsystem::CallResult> MakeCallResult() const
    {
        auto callResult = std::make_shared<functionsystem::CallResult>();
        callResult->set_code(common::ErrorCode::ERR_NONE);
        callResult->set_requestid(ORIGINAL_CALL_RESULT_REQUEST_ID);
        callResult->set_instanceid(INSTANCE_ID);
        callResult->set_message("loser runtime payload");
        callResult->add_smallobjects()->set_value("loser result");
        callResult->add_stacktraceinfos();
        callResult->mutable_runtimeinfo()->set_serveripaddr("192.0.2.10");
        callResult->mutable_runtimeinfo()->set_serverport(18080);
        callResult->mutable_runtimeinfo()->set_route("tcp://192.0.2.10:18081");
        callResult->mutable_runtimeinfo()->set_proxyid(LOSER_PROXY_ID);
        return callResult;
    }

    TransitionResult MakeConflictResult(const InstanceInfo &loser, const InstanceInfo &winner) const
    {
        return TransitionResult{ litebus::None(), winner, loser, 0,
                                 Status(StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION, "route already exists") };
    }

    void PrimeLoserLifecycle(const InstanceInfo &loser)
    {
        actor_->AddHeartbeatTimer(loser.instanceid());
        actor_->instanceStatusPromises_[loser.instanceid()] = litebus::Promise<Status>();
        auto &metricsContext = metrics::MetricsAdapter::GetInstance().GetMetricsContext();
        metricsContext.InitBillingInstance(loser, {});
        EXPECT_EQ(metricsContext.GetBillingInstance(loser.instanceid()).endTimeMillis, 0);
    }

    void ExpectExactLoserCleanup(
        const InstanceInfo &loser,
        litebus::Future<std::shared_ptr<messages::KillInstanceRequest>> &killRequestFuture,
        litebus::Future<std::vector<std::string>> &deletedInstancesFuture)
    {
        EXPECT_CALL(*clientManager_, DeleteClient(loser.instanceid())).WillOnce(Return(Status::OK()));

        messages::KillInstanceResponse killResponse;
        killResponse.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        killResponse.set_requestid(loser.requestid());
        EXPECT_CALL(*functionAgentMgr_, KillInstance(_, loser.functionagentid(), false))
            .WillOnce(DoAll(FutureArg<0>(&killRequestFuture), Return(killResponse)));

        EXPECT_CALL(*primary_, DeleteInstances(ElementsAre(loser.instanceid()), false))
            .WillOnce(DoAll(FutureArg<0>(&deletedInstancesFuture), Return(Status::OK())));
        EXPECT_CALL(*virtual_, DeleteInstances).Times(0);
        // The shared instance ID now represents the winner; cleanup must never delete its state machine.
        EXPECT_CALL(*instanceControlView_, DelInstance).Times(0);
    }

    void AssertExactLoserCleanup(
        const InstanceInfo &loser,
        const litebus::Future<std::shared_ptr<messages::KillInstanceRequest>> &killRequestFuture,
        const litebus::Future<std::vector<std::string>> &deletedInstancesFuture)
    {
        ASSERT_AWAIT_READY(killRequestFuture);
        ASSERT_AWAIT_READY(deletedInstancesFuture);
        ASSERT_NE(killRequestFuture.Get(), nullptr);
        EXPECT_EQ(killRequestFuture.Get()->instanceid(), loser.instanceid());
        EXPECT_EQ(killRequestFuture.Get()->requestid(), loser.requestid());
        EXPECT_EQ(killRequestFuture.Get()->runtimeid(), loser.runtimeid());
        ASSERT_THAT(deletedInstancesFuture.Get(), ElementsAre(loser.instanceid()));
        EXPECT_EQ(actor_->GetHeartbeatTimers().count(loser.instanceid()), 0);
        EXPECT_EQ(actor_->instanceStatusPromises_.count(loser.instanceid()), 0);
        EXPECT_GT(metrics::MetricsAdapter::GetInstance()
                      .GetMetricsContext()
                      .GetBillingInstance(loser.instanceid())
                      .endTimeMillis,
                  0);
    }

    inline static const std::string INSTANCE_ID = "initial-low-route-conflict-instance";
    inline static const std::string LOSER_REQUEST_ID = "loser-create-request";
    inline static const std::string ORIGINAL_CALL_RESULT_REQUEST_ID = "runtime-call-result-request";
    inline static const std::string WINNER_REQUEST_ID = "winner-create-request";
    inline static const std::string LOSER_RUNTIME_ID = "loser-runtime-exact";
    inline static const std::string LOSER_AGENT_ID = "loser-agent-exact";
    inline static const std::string LOSER_PROXY_ID = "loser-proxy";
    inline static const std::string WINNER_PROXY_ID = "winner-proxy";
    inline static const std::string PARENT_INSTANCE_ID = "original-parent-instance";
    inline static const std::string PARENT_PROXY_AID = "original-parent-proxy-aid";
    inline static const std::string FUNCTION_KEY = "default/0-test-helloWorld/$latest";
    inline static const std::string TENANT_ID = "tenant-a";
    inline static const std::string JOB_ID = "job-a";
    inline static const std::string ARBITRATED_CONTENDER_MARKER =
        "createConflictArbitratedContender";

    bool previousDirectRoutingEnabled_ = false;
    bool previousForceLowReliabilityEnabled_ = false;
    std::unordered_set<metrics::YRInstrument> previousEnabledInstruments_;
    std::shared_ptr<MockInstanceCtrlActor> actor_;
    std::shared_ptr<MockInstanceControlView> instanceControlView_;
    std::shared_ptr<MockSharedClientManagerProxy> clientManager_;
    std::shared_ptr<MockFunctionAgentMgr> functionAgentMgr_;
    std::shared_ptr<resource_view::ResourceViewMgr> resourceViewMgr_;
    std::shared_ptr<MockResourceView> primary_;
    std::shared_ptr<MockResourceView> virtual_;
    std::shared_ptr<MockObserver> observer_;
    std::shared_ptr<MockInternalIAM> internalIAM_;
};

TEST_F(InitialLowReliabilityRouteConflictActorTest, ReturnsWinnerAndReclaimsExactLoser)
{
    const auto loser = MakeLoserInfo();
    const auto winner = MakeWinnerInfo();
    const auto winnerAID = litebus::AID("winner-local-scheduler", "tcp://127.0.0.1:39091");
    auto callResult = MakeCallResult();
    auto resolver = std::make_shared<InstanceGenerationConflictResolver>(loser, true);
    auto cleanupState = resolver->GetCleanupState();

    PrimeLoserLifecycle(loser);
    litebus::Future<std::shared_ptr<messages::KillInstanceRequest>> killRequestFuture;
    litebus::Future<std::vector<std::string>> deletedInstancesFuture;
    ExpectExactLoserCleanup(loser, killRequestFuture, deletedInstancesFuture);

    EXPECT_CALL(*observer_, GetLocalSchedulerAID(winner.functionproxyid()))
        .WillOnce(Return(litebus::Option<litebus::AID>(winnerAID)));
    CallResultAck ack;
    ack.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<functionsystem::CallResult>> sentCallResultFuture;
    EXPECT_CALL(*actor_, MockSendCallResult(loser.instanceid(), loser.parentid(),
                                            loser.parentfunctionproxyaid(), _))
        .WillOnce(DoAll(FutureArg<3>(&sentCallResultFuture), Return(ack)));

    auto result = actor_->HandleCreateRouteConflict(
        loser, MakeConflictResult(loser, winner), callResult, resolver);

    ASSERT_AWAIT_READY(result);
    ASSERT_AWAIT_READY(sentCallResultFuture);
    AssertExactLoserCleanup(loser, killRequestFuture, deletedInstancesFuture);
    EXPECT_EQ(result.Get().code(), common::ErrorCode::ERR_NONE);
    ASSERT_NE(sentCallResultFuture.Get(), nullptr);
    EXPECT_EQ(sentCallResultFuture.Get(), callResult);
    const auto &sent = *sentCallResultFuture.Get();
    EXPECT_EQ(sent.requestid(), ORIGINAL_CALL_RESULT_REQUEST_ID);
    EXPECT_EQ(sent.instanceid(), loser.instanceid());
    EXPECT_EQ(sent.code(), common::ErrorCode::ERR_NONE);
    EXPECT_TRUE(sent.message().empty());
    EXPECT_EQ(sent.smallobjects_size(), 0);
    EXPECT_EQ(sent.stacktraceinfos_size(), 0);
    ASSERT_TRUE(sent.has_runtimeinfo());
    EXPECT_TRUE(sent.runtimeinfo().serveripaddr().empty());
    EXPECT_EQ(sent.runtimeinfo().serverport(), 0);
    EXPECT_EQ(sent.runtimeinfo().route(), winnerAID.Url());
    EXPECT_EQ(sent.runtimeinfo().proxyid(), winner.functionproxyid());
    EXPECT_EQ(resolver->ResolvedWinner().requestid(), winner.requestid());
    EXPECT_TRUE(cleanupState->localCleanupPrepared);
    ASSERT_NE(cleanupState->attempt, nullptr);
    ASSERT_AWAIT_READY(*cleanupState->attempt);
    EXPECT_TRUE(cleanupState->attempt->Get().IsOk());
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, IncompleteLocalRuntimeViewStillMatchesContender)
{
    const auto contender = MakeLoserInfo();
    auto localView = contender;
    localView.clear_runtimeid();
    auto resolver = std::make_shared<InstanceGenerationConflictResolver>(contender, true);

    EXPECT_TRUE(resolver->MatchesContenderGenerationView(localView));

    localView.set_runtimeid("replacement-runtime");
    EXPECT_FALSE(resolver->MatchesContenderGenerationView(localView));

    localView = contender;
    localView.clear_runtimeid();
    localView.set_requestid("replacement-request");
    EXPECT_FALSE(resolver->MatchesContenderGenerationView(localView));
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, RegisteredCallbackRejectsChangedLocalGenerationWithoutCleanup)
{
    const auto loser = MakeLoserInfo();
    const auto winner = MakeWinnerInfo();
    auto replacement = loser;
    replacement.set_runtimeid("replacement-runtime");
    replacement.set_functionagentid("replacement-agent");
    replacement.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    auto scheduleRequest = std::make_shared<messages::ScheduleRequest>();
    scheduleRequest->set_requestid(loser.requestid());
    scheduleRequest->mutable_instance()->CopyFrom(loser);
    auto callResult = MakeCallResult();
    auto stateMachine = std::make_shared<MockInstanceStateMachine>(LOSER_PROXY_ID);

    EXPECT_CALL(*instanceControlView_, GetInstance(loser.instanceid()))
        .Times(2)
        .WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceState()).WillOnce(Return(InstanceState::CREATING));
    EXPECT_CALL(*stateMachine, GetVersion()).WillOnce(Return(0)).WillOnce(Return(1));
    EXPECT_CALL(*stateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(*stateMachine, GetInstanceInfo())
        .WillOnce(Return(loser))
        .WillOnce(Return(loser))
        .WillRepeatedly(Return(replacement));
    auto conflictResult = MakeConflictResult(loser, winner);
    conflictResult.currentModRevision = 23;
    EXPECT_CALL(*stateMachine,
                TransitionToImpl(InstanceState::RUNNING, 0, "running", true, 0))
        .WillOnce(Return(conflictResult));
    {
        InSequence sequence;
        EXPECT_CALL(*stateMachine, UpdateInstanceInfo(Truly([loser](const InstanceInfo &actual) {
            return actual.SerializeAsString() == loser.SerializeAsString();
        })));
        EXPECT_CALL(*stateMachine, UpdateInstanceInfo(Truly([winner](const InstanceInfo &actual) {
            return actual.SerializeAsString() == winner.SerializeAsString();
        })));
        EXPECT_CALL(*stateMachine, SetVersion(0));
        EXPECT_CALL(*stateMachine, SetModRevision(23));
    }
    PrimeLoserLifecycle(loser);
    EXPECT_CALL(*clientManager_, DeleteClient).Times(0);
    EXPECT_CALL(*functionAgentMgr_, KillInstance).Times(0);
    EXPECT_CALL(*primary_, DeleteInstances).Times(0);
    EXPECT_CALL(*virtual_, DeleteInstances).Times(0);
    EXPECT_CALL(*instanceControlView_, DelInstance).Times(0);
    EXPECT_CALL(*observer_, GetLocalSchedulerAID).Times(0);
    CallResultAck ack;
    ack.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<functionsystem::CallResult>> sentCallResultFuture;
    EXPECT_CALL(*actor_, MockSendCallResult(loser.instanceid(), loser.parentid(),
                                            loser.parentfunctionproxyaid(), _))
        .WillOnce(DoAll(FutureArg<3>(&sentCallResultFuture), Return(ack)));

    auto callback = actor_->RegisterCreateCallResultCallback(scheduleRequest);
    auto result = callback(callResult);

    ASSERT_AWAIT_READY(result);
    ASSERT_AWAIT_READY(sentCallResultFuture);
    EXPECT_EQ(result.Get().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(sentCallResultFuture.Get()->code(), common::ErrorCode::ERR_ETCD_OPERATION_ERROR);
    EXPECT_THAT(sentCallResultFuture.Get()->message(), HasSubstr("generation changed"));
    EXPECT_FALSE(sentCallResultFuture.Get()->has_runtimeinfo());
    EXPECT_EQ(actor_->GetHeartbeatTimers().count(loser.instanceid()), 1);
    EXPECT_EQ(metrics::MetricsAdapter::GetInstance()
                  .GetMetricsContext()
                  .GetBillingInstance(loser.instanceid())
                  .endTimeMillis,
              0);
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, PostTransitionRuntimeGenerationChangeSkipsExactCleanup)
{
    const auto loser = MakeLoserInfo();
    auto replacement = loser;
    replacement.set_runtimeid("replacement-runtime");
    replacement.mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::RUNNING));
    auto scheduleRequest = std::make_shared<messages::ScheduleRequest>();
    scheduleRequest->set_requestid(loser.requestid());
    scheduleRequest->mutable_instance()->CopyFrom(loser);
    auto callResult = MakeCallResult();
    auto stateMachine = std::make_shared<MockInstanceStateMachine>(LOSER_PROXY_ID);

    EXPECT_CALL(*instanceControlView_, GetInstance(loser.instanceid()))
        .Times(2)
        .WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceState()).WillOnce(Return(InstanceState::CREATING));
    EXPECT_CALL(*stateMachine, GetVersion()).WillOnce(Return(0)).WillOnce(Return(1));
    EXPECT_CALL(*stateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(*stateMachine, GetInstanceInfo())
        .WillOnce(Return(loser))
        .WillOnce(Return(loser))
        .WillOnce(Return(replacement));
    TransitionResult persistenceFailure{
        litebus::None(), {}, loser, 0,
        Status(StatusCode::INSTANCE_TRANSACTION_GET_INFO_FAILED, "route is missing")
    };
    EXPECT_CALL(*stateMachine,
                TransitionToImpl(InstanceState::RUNNING, 0, "running", true, 0))
        .WillOnce(Return(persistenceFailure));
    EXPECT_CALL(*stateMachine, UpdateInstanceInfo(Truly([loser](const InstanceInfo &actual) {
        return actual.SerializeAsString() == loser.SerializeAsString();
    })));
    EXPECT_CALL(*clientManager_, DeleteClient).Times(0);
    EXPECT_CALL(*functionAgentMgr_, KillInstance).Times(0);
    EXPECT_CALL(*primary_, DeleteInstances).Times(0);
    EXPECT_CALL(*virtual_, DeleteInstances).Times(0);
    EXPECT_CALL(*instanceControlView_, DelInstance).Times(0);
    EXPECT_CALL(*observer_, GetLocalSchedulerAID).Times(0);

    CallResultAck ack;
    ack.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<functionsystem::CallResult>> sentCallResultFuture;
    EXPECT_CALL(*actor_, MockSendCallResult(loser.instanceid(), loser.parentid(),
                                            loser.parentfunctionproxyaid(), _))
        .WillOnce(DoAll(FutureArg<3>(&sentCallResultFuture), Return(ack)));

    auto callback = actor_->RegisterCreateCallResultCallback(scheduleRequest);
    auto result = callback(callResult);

    ASSERT_AWAIT_READY(result);
    ASSERT_AWAIT_READY(sentCallResultFuture);
    EXPECT_EQ(result.Get().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(sentCallResultFuture.Get()->code(), common::ErrorCode::ERR_ETCD_OPERATION_ERROR);
    EXPECT_THAT(sentCallResultFuture.Get()->message(), HasSubstr("generation changed"));
    EXPECT_FALSE(sentCallResultFuture.Get()->has_runtimeinfo());
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, FailedRuntimeCleanupIsReportedAndRetried)
{
    const auto loser = MakeLoserInfo();
    const auto winner = MakeWinnerInfo();
    const auto winnerAID = litebus::AID("winner-local-scheduler", "tcp://127.0.0.1:39091");
    auto callResult = MakeCallResult();
    auto resolver = std::make_shared<InstanceGenerationConflictResolver>(loser, true);
    auto cleanupState = resolver->GetCleanupState();
    PrimeLoserLifecycle(loser);

    EXPECT_CALL(*clientManager_, DeleteClient(loser.instanceid()))
        .Times(2)
        .WillRepeatedly(Return(Status::OK()));
    messages::KillInstanceResponse failedKill;
    failedKill.set_code(static_cast<int32_t>(StatusCode::FAILED));
    messages::KillInstanceResponse successfulKill;
    successfulKill.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_CALL(*functionAgentMgr_, KillInstance(_, loser.functionagentid(), false))
        .WillOnce(Return(failedKill))
        .WillOnce(Return(successfulKill));
    EXPECT_CALL(*primary_, DeleteInstances(ElementsAre(loser.instanceid()), false))
        .WillOnce(Return(Status::OK()));
    EXPECT_CALL(*virtual_, DeleteInstances).Times(0);
    EXPECT_CALL(*instanceControlView_, DelInstance).Times(0);

    CallResultAck ack;
    ack.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<functionsystem::CallResult>> secondSent;
    EXPECT_CALL(*actor_, MockSendCallResult(loser.instanceid(), loser.parentid(),
                                            loser.parentfunctionproxyaid(), _))
        .WillOnce(DoAll(FutureArg<3>(&secondSent), Return(ack)));
    EXPECT_CALL(*observer_, GetLocalSchedulerAID(winner.functionproxyid()))
        .WillOnce(Return(litebus::Option<litebus::AID>(winnerAID)));

    auto first = actor_->HandleCreateRouteConflict(
        loser, MakeConflictResult(loser, winner), callResult, resolver);
    ASSERT_AWAIT_READY(first);
    ASSERT_NE(cleanupState->attempt, nullptr);
    ASSERT_AWAIT_READY(*cleanupState->attempt);
    EXPECT_TRUE(cleanupState->attempt->Get().IsError());
    EXPECT_NE(first.Get().code(), common::ErrorCode::ERR_NONE);
    EXPECT_THAT(first.Get().message(), HasSubstr("failed to reclaim"));

    auto second = actor_->ReclaimCreateContenderAndSendWinner(
        loser, winner, callResult, resolver);
    ASSERT_AWAIT_READY(second);
    ASSERT_AWAIT_READY(secondSent);
    ASSERT_NE(cleanupState->attempt, nullptr);
    ASSERT_AWAIT_READY(*cleanupState->attempt);
    EXPECT_TRUE(cleanupState->attempt->Get().IsOk());
    EXPECT_EQ(secondSent.Get()->code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(secondSent.Get()->runtimeinfo().route(), winnerAID.Url());
    EXPECT_EQ(secondSent.Get()->runtimeinfo().proxyid(), winner.functionproxyid());
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, CallResultKeepsCallbackAndRetriesFailedCleanup)
{
    const auto loser = MakeLoserInfo();
    const auto winner = MakeWinnerInfo();
    const auto winnerAID = litebus::AID("winner-local-scheduler", "tcp://127.0.0.1:39091");
    auto scheduleRequest = std::make_shared<messages::ScheduleRequest>();
    scheduleRequest->set_requestid(loser.requestid());
    scheduleRequest->mutable_instance()->CopyFrom(loser);
    auto stateMachine = std::make_shared<MockInstanceStateMachine>(LOSER_PROXY_ID);

    EXPECT_CALL(*instanceControlView_, GetInstance(loser.instanceid()))
        .Times(5)
        .WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceState())
        .Times(7)
        .WillRepeatedly(Return(InstanceState::CREATING));
    EXPECT_CALL(*stateMachine, GetVersion()).WillOnce(Return(0)).WillOnce(Return(1));
    EXPECT_CALL(*stateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(*stateMachine, GetInstanceInfo())
        .WillOnce(Return(loser))
        .WillOnce(Return(loser))
        .WillOnce(Return(winner))
        .WillOnce(Return(winner));
    auto conflictResult = MakeConflictResult(loser, winner);
    conflictResult.currentModRevision = 31;
    EXPECT_CALL(*stateMachine,
                TransitionToImpl(InstanceState::RUNNING, 0, "running", true, 0))
        .WillOnce(Return(conflictResult));
    {
        InSequence sequence;
        EXPECT_CALL(*stateMachine, UpdateInstanceInfo(Truly([loser](const InstanceInfo &actual) {
            return actual.SerializeAsString() == loser.SerializeAsString();
        })));
        EXPECT_CALL(*stateMachine, UpdateInstanceInfo(Truly([winner](const InstanceInfo &actual) {
            return actual.SerializeAsString() == winner.SerializeAsString();
        })));
        EXPECT_CALL(*stateMachine, SetVersion(0));
        EXPECT_CALL(*stateMachine, SetModRevision(31));
    }

    PrimeLoserLifecycle(loser);
    EXPECT_CALL(*clientManager_, DeleteClient(loser.instanceid()))
        .Times(2)
        .WillRepeatedly(Return(Status::OK()));
    messages::KillInstanceResponse failedKill;
    failedKill.set_code(static_cast<int32_t>(StatusCode::FAILED));
    messages::KillInstanceResponse successfulKill;
    successfulKill.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_CALL(*functionAgentMgr_, KillInstance(_, loser.functionagentid(), false))
        .WillOnce(Return(failedKill))
        .WillOnce(Return(successfulKill));
    EXPECT_CALL(*primary_, DeleteInstances(ElementsAre(loser.instanceid()), false))
        .WillOnce(Return(Status::OK()));
    EXPECT_CALL(*virtual_, DeleteInstances).Times(0);
    EXPECT_CALL(*instanceControlView_, DelInstance).Times(0);
    EXPECT_CALL(*observer_, GetLocalSchedulerAID(winner.functionproxyid()))
        .WillOnce(Return(litebus::Option<litebus::AID>(winnerAID)));

    CallResultAck delivered;
    delivered.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<functionsystem::CallResult>> sentCallResultFuture;
    EXPECT_CALL(*actor_, MockSendCallResult(loser.instanceid(), loser.parentid(),
                                            loser.parentfunctionproxyaid(), _))
        .WillOnce(DoAll(FutureArg<3>(&sentCallResultFuture), Return(delivered)));

    actor_->RegisterCreateCallResultCallback(scheduleRequest);
    actor_->syncCreateCallResultPromises_[loser.instanceid()] =
        std::make_shared<litebus::Promise<std::shared_ptr<functionsystem::CallResult>>>();

    auto firstCallResult = MakeCallResult();
    auto first = actor_->CallResult(loser.instanceid(), firstCallResult);
    ASSERT_AWAIT_READY(first);
    EXPECT_NE(first.Get().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(actor_->createCallResultCallback_.count(loser.instanceid()), 1);

    auto retryCallResult = MakeCallResult();
    auto second = actor_->CallResult(loser.instanceid(), retryCallResult);
    ASSERT_AWAIT_READY(second);
    ASSERT_AWAIT_READY(sentCallResultFuture);
    EXPECT_EQ(second.Get().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(actor_->createCallResultCallback_.count(loser.instanceid()), 0);
    EXPECT_EQ(sentCallResultFuture.Get()->runtimeinfo().route(), winnerAID.Url());
    EXPECT_EQ(sentCallResultFuture.Get()->runtimeinfo().proxyid(), winner.functionproxyid());
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, DuplicateLoserCallResultIsAcknowledgedWithoutWinnerForward)
{
    auto callResult = MakeCallResult();
    actor_->createConflictCallResultTombstones_[INSTANCE_ID] = callResult->requestid();
    EXPECT_CALL(*instanceControlView_, GetInstance(INSTANCE_ID)).WillOnce(Return(nullptr));
    EXPECT_CALL(*actor_, MockSendCallResult).Times(0);

    auto result = actor_->CallResult(INSTANCE_ID, callResult);
    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), common::ErrorCode::ERR_NONE);
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, StaleForwardAckDoesNotApplyGenericCleanupToWinner)
{
    const std::string requestID = "loser-forward-request";
    auto promise = std::make_shared<InstanceCtrlActor::ForwardCallResultPromise>();
    actor_->forwardCallResultPromise_[requestID] = promise;
    actor_->createConflictForwardContenders_[requestID] = INSTANCE_ID;
    EXPECT_CALL(*instanceControlView_, GetInstance).Times(0);

    ::internal::ForwardCallResultResponse response;
    response.set_requestid(requestID);
    response.set_instanceid(INSTANCE_ID);
    response.set_code(static_cast<common::ErrorCode>(StatusCode::ERR_INSTANCE_EXITED));
    auto message = response.SerializeAsString();
    actor_->ForwardCallResultResponse(litebus::AID("remote-parent"), std::string(), std::move(message));

    ASSERT_AWAIT_READY(promise->GetFuture());
    EXPECT_EQ(actor_->createConflictForwardContenders_.count(requestID), 0);
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, ResolvedWinnerForwardCarriesOnlyRelayMarker)
{
    auto callResult = MakeCallResult();
    actor_->createConflictForwardContenders_[callResult->requestid()] = INSTANCE_ID;

    EXPECT_CALL(*instanceControlView_, GetInstance).Times(0);
    auto request = actor_->BuildForwardCallResultRequest(
        INSTANCE_ID, PARENT_INSTANCE_ID, PARENT_PROXY_AID, callResult);

    ASSERT_NE(request, nullptr);
    ASSERT_TRUE(request->has_readyinstance());
    const auto &readyInstance = request->readyinstance();
    EXPECT_EQ(readyInstance.instanceid(), INSTANCE_ID);
    EXPECT_EQ(readyInstance.extensions_size(), 1);
    ASSERT_NE(readyInstance.extensions().find(ARBITRATED_CONTENDER_MARKER),
              readyInstance.extensions().end());
    EXPECT_EQ(readyInstance.extensions().at(ARBITRATED_CONTENDER_MARKER), INSTANCE_ID);
    EXPECT_TRUE(readyInstance.runtimeid().empty());
    EXPECT_TRUE(readyInstance.functionagentid().empty());
    EXPECT_TRUE(readyInstance.functionproxyid().empty());
    EXPECT_FALSE(readyInstance.lowreliability());
    EXPECT_EQ(request->instanceid(), PARENT_INSTANCE_ID);
    EXPECT_EQ(request->functionproxyid(), PARENT_PROXY_AID);
    EXPECT_EQ(request->req().runtimeinfo().route(), callResult->runtimeinfo().route());
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, FrontendArbitratedResultUsesRequestTicketAsRemoteDestination)
{
    auto contender = MakeLoserInfo();
    contender.clear_parentid();
    contender.set_parentfunctionproxyaid(PARENT_PROXY_AID);
    (*contender.mutable_extensions())[CREATE_SOURCE] = FRONTEND_STR;
    auto callResult = MakeCallResult();

    CallResultAck delivered;
    delivered.set_code(common::ErrorCode::ERR_NONE);
    EXPECT_CALL(*actor_, MockSendCallResult(contender.instanceid(), callResult->requestid(),
                                            contender.parentfunctionproxyaid(), callResult))
        .WillOnce(Return(delivered));

    auto result = actor_->SendCreateArbitratedCallResult(contender, callResult, true);

    ASSERT_AWAIT_READY(result);
    EXPECT_EQ(result.Get().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(actor_->createConflictForwardContenders_.at(callResult->requestid()), contender.instanceid());
    EXPECT_EQ(actor_->createConflictCallResultTombstones_.at(contender.instanceid()), callResult->requestid());
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, ArbitratedMarkerIsReestablishedForMultiHopRelay)
{
    auto callResult = MakeCallResult();
    actor_->createConflictForwardContenders_[callResult->requestid()] = INSTANCE_ID;
    EXPECT_CALL(*instanceControlView_, GetInstance).Times(0);

    auto firstHop = actor_->BuildForwardCallResultRequest(
        INSTANCE_ID, PARENT_INSTANCE_ID, PARENT_PROXY_AID, callResult);
    ASSERT_NE(firstHop, nullptr);
    ASSERT_TRUE(firstHop->has_readyinstance());
    actor_->createConflictForwardContenders_.clear();

    litebus::Promise<CallResultAck> downstreamPromise;
    EXPECT_CALL(*actor_, MockSendCallResult(INSTANCE_ID, PARENT_INSTANCE_ID, PARENT_PROXY_AID, _))
        .WillOnce(Return(downstreamPromise.GetFuture()));
    auto firstHopMessage = firstHop->SerializeAsString();
    actor_->ForwardCallResultRequest(
        litebus::AID("upstream-proxy"), std::string(), std::move(firstHopMessage));

    ASSERT_EQ(actor_->createConflictForwardContenders_.at(callResult->requestid()), INSTANCE_ID);
    auto secondHop = actor_->BuildForwardCallResultRequest(
        INSTANCE_ID, "next-parent-instance", "next-parent-proxy", callResult);
    ASSERT_NE(secondHop, nullptr);
    ASSERT_TRUE(secondHop->has_readyinstance());
    const auto &relayedReadyInstance = secondHop->readyinstance();
    EXPECT_EQ(relayedReadyInstance.instanceid(), INSTANCE_ID);
    ASSERT_NE(relayedReadyInstance.extensions().find(ARBITRATED_CONTENDER_MARKER),
              relayedReadyInstance.extensions().end());
    EXPECT_EQ(relayedReadyInstance.extensions().at(ARBITRATED_CONTENDER_MARKER), INSTANCE_ID);
    EXPECT_TRUE(relayedReadyInstance.runtimeid().empty());
    EXPECT_TRUE(relayedReadyInstance.functionproxyid().empty());

    CallResultAck delivered;
    delivered.set_code(common::ErrorCode::ERR_NONE);
    downstreamPromise.SetValue(delivered);
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, UnresolvedRouteConflictReclaimsExactLoser)
{
    const auto loser = MakeLoserInfo();
    auto scheduleRequest = std::make_shared<messages::ScheduleRequest>();
    scheduleRequest->set_requestid(loser.requestid());
    scheduleRequest->mutable_instance()->CopyFrom(loser);
    auto callResult = MakeCallResult();
    auto stateMachine = std::make_shared<MockInstanceStateMachine>(LOSER_PROXY_ID);

    EXPECT_CALL(*instanceControlView_, GetInstance(loser.instanceid()))
        .Times(2)
        .WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceState()).WillOnce(Return(InstanceState::CREATING));
    EXPECT_CALL(*stateMachine, GetVersion()).WillOnce(Return(0)).WillOnce(Return(1));
    EXPECT_CALL(*stateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(*stateMachine, GetInstanceInfo())
        .Times(3)
        .WillRepeatedly(Return(loser));
    TransitionResult persistenceFailure{
        litebus::None(), {}, loser, 0,
        Status(StatusCode::INSTANCE_TRANSACTION_WRONG_VERSION, "route conflict winner is unavailable")
    };
    EXPECT_CALL(*stateMachine,
                TransitionToImpl(InstanceState::RUNNING, 0, "running", true, 0))
        .WillOnce(Return(persistenceFailure));
    EXPECT_CALL(*stateMachine, UpdateInstanceInfo(Truly([loser](const InstanceInfo &actual) {
        return actual.SerializeAsString() == loser.SerializeAsString();
    })));

    PrimeLoserLifecycle(loser);
    litebus::Future<std::shared_ptr<messages::KillInstanceRequest>> killRequestFuture;
    litebus::Future<std::vector<std::string>> deletedInstancesFuture;
    ExpectExactLoserCleanup(loser, killRequestFuture, deletedInstancesFuture);
    EXPECT_CALL(*observer_, GetLocalSchedulerAID).Times(0);
    CallResultAck ack;
    ack.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<functionsystem::CallResult>> sentCallResultFuture;
    EXPECT_CALL(*actor_, MockSendCallResult(loser.instanceid(), loser.parentid(),
                                            loser.parentfunctionproxyaid(), _))
        .WillOnce(DoAll(FutureArg<3>(&sentCallResultFuture), Return(ack)));

    auto callback = actor_->RegisterCreateCallResultCallback(scheduleRequest);
    auto result = callback(callResult);

    ASSERT_AWAIT_READY(result);
    ASSERT_AWAIT_READY(sentCallResultFuture);
    AssertExactLoserCleanup(loser, killRequestFuture, deletedInstancesFuture);
    EXPECT_EQ(result.Get().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(sentCallResultFuture.Get()->code(), common::ErrorCode::ERR_ETCD_OPERATION_ERROR);
    EXPECT_THAT(sentCallResultFuture.Get()->message(), HasSubstr("failed to resolve route conflict"));
    EXPECT_FALSE(sentCallResultFuture.Get()->has_runtimeinfo());
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, MissingRoutePersistenceFailureReclaimsExactLoser)
{
    const auto loser = MakeLoserInfo();
    auto scheduleRequest = std::make_shared<messages::ScheduleRequest>();
    scheduleRequest->set_requestid(loser.requestid());
    scheduleRequest->mutable_instance()->CopyFrom(loser);
    auto callResult = MakeCallResult();
    auto stateMachine = std::make_shared<MockInstanceStateMachine>(LOSER_PROXY_ID);

    EXPECT_CALL(*instanceControlView_, GetInstance(loser.instanceid()))
        .Times(2)
        .WillRepeatedly(Return(stateMachine));
    EXPECT_CALL(*stateMachine, GetInstanceState()).WillOnce(Return(InstanceState::CREATING));
    EXPECT_CALL(*stateMachine, GetVersion()).WillOnce(Return(0)).WillOnce(Return(1));
    EXPECT_CALL(*stateMachine, IsSaving()).WillOnce(Return(false));
    EXPECT_CALL(*stateMachine, GetInstanceInfo()).Times(3).WillRepeatedly(Return(loser));
    TransitionResult persistenceFailure{
        litebus::None(), {}, loser, 0,
        Status(StatusCode::INSTANCE_TRANSACTION_GET_INFO_FAILED, "route key is missing")
    };
    EXPECT_CALL(*stateMachine, TransitionToImpl(InstanceState::RUNNING, 0, "running", true, 0))
        .WillOnce(Return(persistenceFailure));
    EXPECT_CALL(*stateMachine, UpdateInstanceInfo(Truly([loser](const InstanceInfo &actual) {
        return actual.SerializeAsString() == loser.SerializeAsString();
    })));

    PrimeLoserLifecycle(loser);
    litebus::Future<std::shared_ptr<messages::KillInstanceRequest>> killRequestFuture;
    litebus::Future<std::vector<std::string>> deletedInstancesFuture;
    ExpectExactLoserCleanup(loser, killRequestFuture, deletedInstancesFuture);
    EXPECT_CALL(*observer_, GetLocalSchedulerAID).Times(0);
    CallResultAck ack;
    ack.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<functionsystem::CallResult>> sentCallResultFuture;
    EXPECT_CALL(*actor_, MockSendCallResult(loser.instanceid(), loser.parentid(),
                                            loser.parentfunctionproxyaid(), _))
        .WillOnce(DoAll(FutureArg<3>(&sentCallResultFuture), Return(ack)));

    auto callback = actor_->RegisterCreateCallResultCallback(scheduleRequest);
    auto result = callback(callResult);

    ASSERT_AWAIT_READY(result);
    ASSERT_AWAIT_READY(sentCallResultFuture);
    AssertExactLoserCleanup(loser, killRequestFuture, deletedInstancesFuture);
    EXPECT_EQ(result.Get().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(sentCallResultFuture.Get()->code(), common::ErrorCode::ERR_ETCD_OPERATION_ERROR);
    EXPECT_THAT(sentCallResultFuture.Get()->message(), HasSubstr("route is missing"));
    EXPECT_FALSE(sentCallResultFuture.Get()->has_runtimeinfo());
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, InvalidWinnerReturnsEtcdErrorAndReclaimsExactLoser)
{
    const auto loser = MakeLoserInfo();
    auto invalidWinner = MakeWinnerInfo();
    invalidWinner.clear_functionproxyid();
    auto callResult = MakeCallResult();
    auto resolver = std::make_shared<InstanceGenerationConflictResolver>(loser, true);
    auto cleanupState = resolver->GetCleanupState();

    PrimeLoserLifecycle(loser);
    litebus::Future<std::shared_ptr<messages::KillInstanceRequest>> killRequestFuture;
    litebus::Future<std::vector<std::string>> deletedInstancesFuture;
    ExpectExactLoserCleanup(loser, killRequestFuture, deletedInstancesFuture);

    EXPECT_CALL(*observer_, GetLocalSchedulerAID).Times(0);
    CallResultAck ack;
    ack.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<functionsystem::CallResult>> sentCallResultFuture;
    EXPECT_CALL(*actor_, MockSendCallResult(loser.instanceid(), loser.parentid(),
                                            loser.parentfunctionproxyaid(), _))
        .WillOnce(DoAll(FutureArg<3>(&sentCallResultFuture), Return(ack)));

    auto result = actor_->HandleCreateRouteConflict(
        loser, MakeConflictResult(loser, invalidWinner), callResult, resolver);

    ASSERT_AWAIT_READY(result);
    ASSERT_AWAIT_READY(sentCallResultFuture);
    AssertExactLoserCleanup(loser, killRequestFuture, deletedInstancesFuture);
    EXPECT_EQ(result.Get().code(), common::ErrorCode::ERR_NONE);
    ASSERT_NE(sentCallResultFuture.Get(), nullptr);
    EXPECT_EQ(sentCallResultFuture.Get(), callResult);
    const auto &sent = *sentCallResultFuture.Get();
    EXPECT_EQ(sent.requestid(), ORIGINAL_CALL_RESULT_REQUEST_ID);
    EXPECT_EQ(sent.code(), common::ErrorCode::ERR_ETCD_OPERATION_ERROR);
    EXPECT_THAT(sent.message(), HasSubstr("invalid winner instance"));
    EXPECT_EQ(sent.smallobjects_size(), 0);
    EXPECT_EQ(sent.stacktraceinfos_size(), 0);
    EXPECT_FALSE(sent.has_runtimeinfo());
    EXPECT_FALSE(resolver->HasResolvedWinner());
    EXPECT_TRUE(cleanupState->localCleanupPrepared);
    ASSERT_NE(cleanupState->attempt, nullptr);
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, LocalWinnerReturnsErrorWithoutSharedCleanup)
{
    const auto loser = MakeLoserInfo();
    auto localWinner = MakeWinnerInfo();
    localWinner.set_functionproxyid(LOSER_PROXY_ID);
    auto callResult = MakeCallResult();
    auto resolver = std::make_shared<InstanceGenerationConflictResolver>(loser, true);
    auto cleanupState = resolver->GetCleanupState();

    PrimeLoserLifecycle(loser);
    EXPECT_CALL(*clientManager_, DeleteClient).Times(0);
    EXPECT_CALL(*functionAgentMgr_, KillInstance).Times(0);
    EXPECT_CALL(*primary_, DeleteInstances).Times(0);
    EXPECT_CALL(*virtual_, DeleteInstances).Times(0);
    EXPECT_CALL(*instanceControlView_, DelInstance).Times(0);
    EXPECT_CALL(*observer_, GetLocalSchedulerAID).Times(0);
    CallResultAck ack;
    ack.set_code(common::ErrorCode::ERR_NONE);
    litebus::Future<std::shared_ptr<functionsystem::CallResult>> sentCallResultFuture;
    EXPECT_CALL(*actor_, MockSendCallResult(loser.instanceid(), loser.parentid(),
                                            loser.parentfunctionproxyaid(), _))
        .WillOnce(DoAll(FutureArg<3>(&sentCallResultFuture), Return(ack)));

    auto result = actor_->HandleCreateRouteConflict(
        loser, MakeConflictResult(loser, localWinner), callResult, resolver);

    ASSERT_AWAIT_READY(result);
    ASSERT_AWAIT_READY(sentCallResultFuture);
    EXPECT_EQ(sentCallResultFuture.Get()->code(), common::ErrorCode::ERR_ETCD_OPERATION_ERROR);
    EXPECT_THAT(sentCallResultFuture.Get()->message(), HasSubstr("local winner"));
    EXPECT_EQ(actor_->GetHeartbeatTimers().count(loser.instanceid()), 1);
    EXPECT_FALSE(cleanupState->localCleanupPrepared);
    EXPECT_EQ(cleanupState->attempt, nullptr);
}

TEST_F(InitialLowReliabilityRouteConflictActorTest, ResolverFreezesFirstResolvedWinner)
{
    const auto contender = MakeLoserInfo();
    auto firstWinner = MakeWinnerInfo();
    auto laterWinner = firstWinner;
    laterWinner.set_requestid("later-winner-request");
    laterWinner.set_functionproxyid("later-winner-proxy");

    auto resolver = std::make_shared<InstanceGenerationConflictResolver>(contender, true);
    resolver->RecordResolvedWinner(firstWinner);
    resolver->RecordResolvedWinner(laterWinner);

    ASSERT_TRUE(resolver->HasResolvedWinner());
    EXPECT_EQ(resolver->ResolvedWinner().requestid(), firstWinner.requestid());
    EXPECT_EQ(resolver->ResolvedWinner().functionproxyid(), firstWinner.functionproxyid());
}

}  // namespace functionsystem::test
