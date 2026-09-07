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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/proto/pb/message_pb.h"
#include "common/types/instance_state.h"
#include "function_agent/code_deployer/s3_deployer.h"
#include "kv_service_accessor_actor.h"
#include "kv_service_actor.h"
#include "lease_service_actor.h"
#include "function_proxy/local_scheduler/function_agent_manager/function_agent_mgr.h"
#include "function_proxy/local_scheduler/function_agent_manager/function_agent_mgr_actor.h"
#include "mocks/mock_function_agent.h"
#include "mocks/mock_meta_store_client.h"
#include "utils/future_test_helper.h"
#include "utils/port_helper.h"

namespace functionsystem::test {
using std::make_shared;
using std::shared_ptr;
using std::string;
using namespace local_scheduler;
using namespace ::testing;

const local_scheduler::FunctionAgentMgrActor::Param PARAM = {
    .retryTimes = 3,
    .retryCycleMs = 100,
    .pingTimes = 3,
    .pingCycleMs = 500,
    .enableTenantAffinity = true,
    .tenantPodReuseTimeWindow = 3,
    .enableIpv4TenantIsolation = true,
    .getAgentInfoRetryMs = 100,
    .invalidAgentGCInterval = 100,
};

const string TENANT_ID1 = "tenant1";
const string TENANT_ID2 = "tenant2";
const string FUNC_PROXY_ID1 = "node1";
const string FUNC_PROXY_ID2 = "node2";
const string FUNC_AGENT_ID1 = "agent1";
const string FUNC_AGENT_ID2 = "agent2";
const string FUNC_INSTANCE_ID1 = "instance1";
const string FUNC_INSTANCE_ID2 = "instance2";

class MockAgentActor : public litebus::ActorBase {
public:
    MockAgentActor() : litebus::ActorBase("mock-agent")
    {
    }

    MOCK_METHOD(void, SetNetworkIsolationRequest, (const litebus::AID &from, std::string &&name, std::string &&msg));

protected:
    void Init() override
    {
        Receive("SetNetworkIsolationRequest", &MockAgentActor::SetNetworkIsolationRequest);
    }
};

class AnonymousSnapshotCaptureActor final : public litebus::ActorBase {
public:
    AnonymousSnapshotCaptureActor() : litebus::ActorBase("anonymous-snapshot-capture") {}

    litebus::Promise<messages::SnapshotRuntimeRequest> captured;

protected:
    void Init() override
    {
        Receive("SnapshotRuntime", &AnonymousSnapshotCaptureActor::SnapshotRuntime);
    }

private:
    void SnapshotRuntime(const litebus::AID &from, std::string &&, std::string &&msg)
    {
        messages::SnapshotRuntimeRequest request;
        if (!request.ParseFromString(msg)) {
            return;
        }
        captured.SetValue(request);
        messages::SnapshotRuntimeResponse response;
        response.set_requestid(request.requestid());
        response.set_code(static_cast<int32_t>(StatusCode::FAILED));
        Send(from, "SnapshotRuntimeResponse", response.SerializeAsString());
    }
};

class FuncAgentMgrActorHelper : public FunctionAgentMgrActor {
public:
    FuncAgentMgrActorHelper(const std::string &metaStoreAddress)
        : FunctionAgentMgrActor("funcAgentMgr", PARAM, "nodeID",
                                std::make_shared<MockMetaStoreClient>(metaStoreAddress))
    {
    }

    litebus::Future<Status> SyncInstancesWithEmptyUnit()
    {
        std::shared_ptr<resource_view::ResourceUnit> resourceUnit = std::make_shared<resource_view::ResourceUnit>();
        resourceUnit->set_id("funcAgentMgr");
        return SyncInstances(resourceUnit);
    }
    litebus::Future<Status> SyncInstancesWithEmptyInstanceCtl()
    {
        std::shared_ptr<resource_view::ResourceUnit> resourceUnit = std::make_shared<resource_view::ResourceUnit>();
        resourceUnit->set_id("funcAgentMgr");
        auto instances = resourceUnit->mutable_instances();
        resource_view::InstanceInfo instanceInfo;
        instanceInfo.set_instanceid("funcAgentMgr_instance_id");
        instances->insert({ "funcAgentMgr_instance_id", instanceInfo });
        return SyncInstances(resourceUnit);
    }
};

class SnapshotRegistrationOrderActor final : public FunctionAgentMgrActor {
public:
    explicit SnapshotRegistrationOrderActor(const std::string &metaStoreAddress)
        : FunctionAgentMgrActor(
              "snapshot-registration-order", [] {
                  auto param = PARAM;
                  param.enableCoProcessMode = true;
                  return param;
              }(), "nodeID", std::make_shared<MockMetaStoreClient>(metaStoreAddress))
    {
    }

    litebus::Future<messages::ListLocalSnapshotsResponse> ListLocalSnapshots(
        const std::string &) override
    {
        operations.emplace_back("list-snapshots");
        messages::ListLocalSnapshotsResponse response;
        response.set_code(listFailuresRemaining == 0
                              ? static_cast<int32_t>(StatusCode::SUCCESS)
                              : static_cast<int32_t>(StatusCode::GRPC_UNAVAILABLE));
        if (listFailuresRemaining > 0) {
            --listFailuresRemaining;
        }
        return response;
    }

    litebus::Future<messages::DeleteLocalSnapshotResponse> DeleteLocalSnapshot(
        const std::string &, const messages::DeleteLocalSnapshotRequest &request) override
    {
        deletedSnapshots.emplace_back(request.snapshotid());
        messages::DeleteLocalSnapshotResponse response;
        response.set_requestid(request.requestid());
        response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        return response;
    }

    std::vector<std::string> operations;
    std::vector<std::string> deletedSnapshots;
    int listFailuresRemaining{0};
};

messages::LocalSnapshotMetadata MakeLocalSnapshot(
    const std::string &snapshotID, uint64_t generation)
{
    messages::LocalSnapshotMetadata snapshot;
    snapshot.set_snapshotid(snapshotID);
    snapshot.set_localrecoverycandidate(true);
    snapshot.set_instanceid("sandbox-a");
    snapshot.set_size(4096);
    snapshot.set_createdatunixseconds(static_cast<int64_t>(generation));
    return snapshot;
}

class FuncAgentMgrActorTest : public ::testing::Test {
protected:
    [[maybe_unused]] static void SetUpTestSuite()
    {
        metaStoreAddress_ = "127.0.0.1:" + std::to_string(FindAvailablePort());
    }

    void SetUp() override
    {
        agentMgrActorHelper_ = make_shared<FuncAgentMgrActorHelper>(metaStoreAddress_);
    }

    inline static std::string metaStoreAddress_;
    shared_ptr<FuncAgentMgrActorHelper> agentMgrActorHelper_;
};

TEST(FunctionAgentMgrTest, RegistrationListsSnapshotsBeforeFirstReconcile)
{
    const auto metaStoreAddress = "127.0.0.1:" + std::to_string(FindAvailablePort());
    auto actor = std::make_shared<SnapshotRegistrationOrderActor>(metaStoreAddress);
    FunctionAgentMgrActor::FuncAgentInfo info;
    info.isInit = true;
    info.recoverPromise = std::make_shared<litebus::Promise<bool>>();
    info.aid = litebus::AID("agent-a", "127.0.0.1:31003");
    actor->InsertAgent("agent-a", info);
    actor->SetCoProcessReconcileCallback([actor](const std::string &) {
        actor->operations.emplace_back("reconcile");
    });
    litebus::Spawn(actor, true);
    struct ActorGuard {
        std::shared_ptr<SnapshotRegistrationOrderActor> actor;
        ~ActorGuard()
        {
            litebus::Terminate(actor->GetAID());
            litebus::Await(actor);
        }
    } guard{actor};

    auto listed = litebus::Async(
        actor->GetAID(), &FunctionAgentMgrActor::RebuildLocalSnapshotView,
        Status::OK(), std::string("agent-a"));
    ASSERT_AWAIT_READY_FOR(listed, 5'000);
    ASSERT_TRUE(listed.Get().IsOk()) << listed.Get().ToString();
    litebus::Future<Status> completed(listed.Get());
    auto enabled = litebus::Async(
        actor->GetAID(), &FunctionAgentMgrActor::EnableFuncAgent,
        completed, std::string("agent-a"));
    ASSERT_AWAIT_READY_FOR(enabled, 5'000);
    ASSERT_TRUE(enabled.Get().IsOk()) << enabled.Get().ToString();

    EXPECT_EQ(actor->operations,
              (std::vector<std::string>{"list-snapshots", "reconcile"}));
}

TEST(FunctionAgentMgrTest, LocalSnapshotCommitDeletesPreviousAnonymousSnapshot)
{
    const auto metaStoreAddress = "127.0.0.1:" + std::to_string(FindAvailablePort());
    auto actor = std::make_shared<SnapshotRegistrationOrderActor>(metaStoreAddress);
    const litebus::AID agentAID("agent-a", "127.0.0.1:31004");
    FunctionAgentMgrActor::FuncAgentInfo info;
    info.isEnable = true;
    info.isInit = true;
    info.aid = agentAID;
    actor->InsertAgent("agent-a", info);
    actor->aidTable_[agentAID] = "agent-a";
    litebus::Spawn(actor, true);
    struct ActorGuard {
        std::shared_ptr<SnapshotRegistrationOrderActor> actor;
        ~ActorGuard()
        {
            litebus::Terminate(actor->GetAID());
            litebus::Await(actor);
        }
    } guard{actor};

    const auto complete = [&](const std::string &requestID,
                              const messages::LocalSnapshotMetadata &snapshot) {
        actor->snapshotRuntimeExpectedAgent_[requestID] = agentAID;
        auto result = actor->snapshotRuntimeSync_.AddSynchronizer(requestID);
        messages::SnapshotRuntimeResponse response;
        response.set_requestid(requestID);
        response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        *response.mutable_localsnapshot() = snapshot;
        auto serialized = response.SerializeAsString();
        actor->SnapshotRuntimeResponse(
            agentAID, std::string("SnapshotRuntimeResponse"), std::move(serialized));
        ASSERT_AWAIT_READY_FOR(result, 5'000);
        EXPECT_EQ(result.Get().code(), static_cast<int32_t>(StatusCode::SUCCESS));
    };

    complete("snapshot-old", MakeLocalSnapshot("old", 1));
    complete("snapshot-new", MakeLocalSnapshot("new", 2));

    ASSERT_AWAIT_TRUE_FOR([actor]() { return actor->deletedSnapshots.size() == 1; }, 5'000);
    EXPECT_EQ(actor->deletedSnapshots, (std::vector<std::string>{"old"}));
    const auto latest = litebus::Async(
        actor->GetAID(), &FunctionAgentMgrActor::LatestAnonymousSnapshot,
        std::string("sandbox-a"));
    ASSERT_AWAIT_READY_FOR(latest, 5'000);
    ASSERT_TRUE(latest.Get().has_value());
    EXPECT_EQ(latest.Get()->snapshotid(), "new");
}

TEST(FunctionAgentMgrTest, RegistrationRetriesSnapshotListBeforeReconcile)
{
    const auto metaStoreAddress = "127.0.0.1:" + std::to_string(FindAvailablePort());
    auto actor = std::make_shared<SnapshotRegistrationOrderActor>(metaStoreAddress);
    actor->listFailuresRemaining = 1;
    FunctionAgentMgrActor::FuncAgentInfo info;
    info.isInit = true;
    info.recoverPromise = std::make_shared<litebus::Promise<bool>>();
    info.aid = litebus::AID("agent-a", "127.0.0.1:31005");
    actor->InsertAgent("agent-a", info);
    actor->SetCoProcessReconcileCallback([actor](const std::string &) {
        actor->operations.emplace_back("reconcile");
    });
    litebus::Spawn(actor, true);
    struct ActorGuard {
        std::shared_ptr<SnapshotRegistrationOrderActor> actor;
        ~ActorGuard()
        {
            litebus::Terminate(actor->GetAID());
            litebus::Await(actor);
        }
    } guard{actor};

    auto listed = litebus::Async(
        actor->GetAID(), &FunctionAgentMgrActor::RebuildLocalSnapshotView,
        Status::OK(), std::string("agent-a"));
    ASSERT_AWAIT_READY_FOR(listed, 5'000);
    litebus::Future<Status> completed(listed.Get());
    auto enabled = litebus::Async(
        actor->GetAID(), &FunctionAgentMgrActor::EnableFuncAgent,
        completed, std::string("agent-a"));
    ASSERT_AWAIT_READY_FOR(enabled, 5'000);
    ASSERT_TRUE(enabled.Get().IsOk()) << enabled.Get().ToString();
    EXPECT_EQ(actor->operations,
              (std::vector<std::string>{"list-snapshots", "list-snapshots", "reconcile"}));
}

TEST(FunctionAgentMgrTest, AnonymousCheckpointPreflightRejectionIsNotStarted)
{
    auto manager = std::make_shared<FunctionAgentMgrActor>(
        "anonymous-preflight-manager", PARAM, "nodeID", std::make_shared<MockMetaStoreClient>("127.0.0.1:1"));
    resources::InstanceInfo instance;
    instance.set_functionagentid("missing-agent");
    auto missing = manager->SnapshotRuntimeAnonymous("missing-agent-request", instance, "anon-missing");
    ASSERT_TRUE(missing.IsOK());
    EXPECT_NE(missing.Get().code(), common::ERR_NONE);
    EXPECT_TRUE(missing.Get().checkpointnotstarted());

    FunctionAgentMgrActor::FuncAgentInfo agent;
    agent.isEnable = true;
    agent.isInit = true;
    manager->InsertAgent("agent-a", agent);
    instance.set_functionagentid("agent-a");
    (*instance.mutable_createoptions())["YR_CHECKPOINT_TIMEOUT_MS"] = "0";
    auto invalid = manager->SnapshotRuntimeAnonymous("invalid-timeout-request", instance, "anon-invalid");
    ASSERT_TRUE(invalid.IsOK());
    EXPECT_EQ(invalid.Get().code(), StatusCode::ERR_PARAM_INVALID);
    EXPECT_TRUE(invalid.Get().checkpointnotstarted());
}

TEST(FunctionAgentMgrTest, AnonymousSnapshotRequestCarriesOnlyLocalCheckpointIntent)
{
    const auto metaStoreAddress = "127.0.0.1:" + std::to_string(FindAvailablePort());
    auto manager = std::make_shared<FunctionAgentMgrActor>(
        "anonymous-snapshot-manager", PARAM, "nodeID",
        std::make_shared<MockMetaStoreClient>(metaStoreAddress));
    auto agent = std::make_shared<AnonymousSnapshotCaptureActor>();
    FunctionAgentMgrActor::FuncAgentInfo info;
    info.isEnable = true;
    info.isInit = true;
    info.aid = agent->GetAID();
    manager->InsertAgent("agent-a", info);
    manager->aidTable_[agent->GetAID()] = "agent-a";
    litebus::Spawn(manager, true);
    litebus::Spawn(agent, true);
    struct ActorGuard {
        std::shared_ptr<FunctionAgentMgrActor> manager;
        std::shared_ptr<AnonymousSnapshotCaptureActor> agent;
        ~ActorGuard()
        {
            litebus::Terminate(manager->GetAID());
            litebus::Terminate(agent->GetAID());
            litebus::Await(manager);
            litebus::Await(agent);
        }
    } guard{manager, agent};

    resources::InstanceInfo instance;
    instance.set_instanceid("sandbox-a");
    instance.set_runtimeid("runtime-a");
    instance.set_containerid("container-a");
    instance.set_functionagentid("agent-a");
    instance.set_tenantid("tenant-a");
    instance.set_version(7);
    auto result = litebus::Async(
        manager->GetAID(), &FunctionAgentMgrActor::SnapshotRuntimeAnonymous,
        std::string("anonymous-request"), instance, std::string("anon-1"));

    ASSERT_AWAIT_READY_FOR(agent->captured.GetFuture(), 5'000);
    const auto request = agent->captured.GetFuture().Get();
    EXPECT_EQ(request.type(), common::DUMPSTATE);
    EXPECT_TRUE(request.localrecoverycandidate());
    EXPECT_TRUE(request.leaverunning());
    EXPECT_TRUE(request.checkpointdir().empty());
    EXPECT_EQ(request.snapshotid(), "anon-1");
    ASSERT_AWAIT_READY_FOR(result, 5'000);
}

TEST_F(FuncAgentMgrActorTest, EmptyResourceUnit)
{
    auto status = agentMgrActorHelper_->SyncInstancesWithEmptyUnit();
    EXPECT_EQ(status.Get().StatusCode(), StatusCode::SUCCESS);
}

TEST_F(FuncAgentMgrActorTest, EmptyInstanceCtl)
{
    auto status = agentMgrActorHelper_->SyncInstancesWithEmptyInstanceCtl();
    EXPECT_EQ(status.GetErrorCode(), StatusCode::LS_SYNC_INSTANCE_FAIL);
}

TEST_F(FuncAgentMgrActorTest, AddFuncAgentFailed)
{
    auto mockMetaStoreClient_ = std::make_shared<MockMetaStoreClient>("111111");
    auto funcAgentMgr =
        make_shared<local_scheduler::FunctionAgentMgr>(make_shared<local_scheduler::FunctionAgentMgrActor>(
            "RecoverHeartBeatSuccessActor", PARAM, "nodeID", mockMetaStoreClient_));
    auto r = std::make_shared<resource_view::ResourceUnit>();
    auto r2 = std::make_shared<resource_view::ResourceUnit>();
    litebus::Promise<std::shared_ptr<resource_view::ResourceUnit>> p;
    funcAgentMgr->actor_->funcAgentResUpdatedMap_["id1"] = p;
    funcAgentMgr->SetFuncAgentUpdateMapPromise("id1", r);
    auto actor = make_shared<local_scheduler::FunctionAgentMgrActor>("RecoverHeartBeatSuccessActor", PARAM, "nodeID",
                                                                     mockMetaStoreClient_);
    auto res = actor->AddFuncAgent(Status(StatusCode::SUCCESS), "", r2);
    EXPECT_EQ(res.Get().IsError(), false);
}

TEST_F(FuncAgentMgrActorTest, TimeoutEventTest)
{
    auto mockMetaStoreClient_ = std::make_shared<MockMetaStoreClient>("111111");
    auto actor = make_shared<local_scheduler::FunctionAgentMgrActor>("RecoverHeartBeatSuccessActor", PARAM, "nodeID",
                                                                     mockMetaStoreClient_);
    actor->TimeoutEvent("id1");
    EXPECT_EQ(actor->funcAgentTable_.count("id1"), size_t(0));

    actor->funcAgentTable_["id1"] = {
        .isEnable =  false,
        .isInit =  false,
        .recoverPromise =  std::make_shared<litebus::Promise<bool>>(),
        .aid =  "aid1",
        .instanceIDs =  {}
    };
    EXPECT_EQ(actor->funcAgentTable_.count("id1"), size_t(1));
    actor->TimeoutEvent("id1");
    EXPECT_EQ(actor->funcAgentTable_.count("id1"), size_t(0));
}

/**
 * Test query instance status info
 */
TEST_F(FuncAgentMgrActorTest, DoAddFuncAgent)
{
    auto mockMetaStoreClient_ = std::make_shared<MockMetaStoreClient>("111111");
    auto actor = make_shared<local_scheduler::FunctionAgentMgrActor>("RecoverHeartBeatSuccessActor", PARAM, "nodeID",
                                                                     mockMetaStoreClient_);

    auto future = actor->DoAddFuncAgent(Status::OK(), "mock-agent-id");
}

/**
 * Test query instance status info
 */
TEST_F(FuncAgentMgrActorTest, QueryInstanceStatusInfo)
{
    auto mockMetaStoreClient_ = std::make_shared<MockMetaStoreClient>("111111");
    auto actor = make_shared<local_scheduler::FunctionAgentMgrActor>("RecoverHeartBeatSuccessActor", PARAM, "nodeID",
                                                                     mockMetaStoreClient_);

    auto future = actor->QueryInstanceStatusInfo("mock-agent-name", "mock-instance-id", "mock-runtime-id");

    messages::QueryInstanceStatusResponse rsp;
    actor->QueryInstanceStatusInfoResponse("mock-agent-name", "", rsp.SerializeAsString());
}

TEST_F(FuncAgentMgrActorTest, QueryDebugInstanceInfos)
{
    // start no mock metastore service
    auto kvServiceActor = std::make_shared<functionsystem::meta_store::KvServiceActor>();
    litebus::Spawn(kvServiceActor);
    litebus::AID kvServerAccessorAID =
        litebus::Spawn(std::make_shared<meta_store::KvServiceAccessorActor>(kvServiceActor->GetAID()));
    auto leaseServiceActor = std::make_shared<functionsystem::meta_store::LeaseServiceActor>(kvServiceActor->GetAID());
    litebus::Spawn(leaseServiceActor);
    leaseServiceActor->Start();
    kvServiceActor->AddLeaseServiceActor(leaseServiceActor->GetAID());
    uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
    std::string addr = "127.0.0.1:" + std::to_string(port);
    functionsystem::MetaStoreConfig metaStoreConfig{ .etcdAddress = addr,
                                                     .metaStoreAddress = addr,
                                                     .enableMetaStore = true };
    auto metaStoreClient = std::make_shared<functionsystem::MetaStoreClient>(
        metaStoreConfig, functionsystem::GrpcSslConfig{}, functionsystem::MetaStoreTimeoutOption());
    metaStoreClient->Init();

    auto funcAgentMgrActor = make_shared<local_scheduler::FunctionAgentMgrActor>("functionAgentMgrActor", PARAM, "nodeID",
                                                                     metaStoreClient);
    S3Config s3Config;
    messages::CodePackageThresholds codePackageThresholds;
    auto agentServiceActor = make_shared<MockFunctionAgent>("agentName", "agentID",
                                                    "testLocalScheduler_01-32379", s3Config, codePackageThresholds);
    funcAgentMgrActor->funcAgentTable_["agentID"] = {
        .isEnable = true,
        .isInit = false,
        .recoverPromise = std::make_shared<litebus::Promise<bool>>(),
        .aid = agentServiceActor->GetAID(),
        .instanceIDs = {}
    };
    litebus::Spawn(funcAgentMgrActor);
    litebus::Spawn(agentServiceActor);


    messages::QueryDebugInstanceInfosResponse rsp;
    rsp.set_code(0);
    auto insInfo1 = rsp.add_debuginstanceinfos();
    insInfo1->set_instanceid("test_instID1");
    insInfo1->set_pid(100);
    insInfo1->set_debugserver("test_gdbserverAddr");
    insInfo1->set_status("S");

    EXPECT_CALL(*agentServiceActor.get(), MockQueryDebugInstanceInfos).WillOnce(Return(rsp));
    auto future = funcAgentMgrActor->QueryDebugInstanceInfos();
    EXPECT_EQ(future.Get().StatusCode(),StatusCode::SUCCESS);
    auto response = metaStoreClient->Get("/yr/debug/",{.prefix = true}).Get();
    EXPECT_EQ(response->kvs.size(), static_cast<uint32_t>(1));
    EXPECT_EQ(response->kvs[0].key(), "/yr/debug/test_instID1");
    messages::DebugInstanceInfo info;
    (void)google::protobuf::util::JsonStringToMessage(response->kvs[0].value(),&info);
    EXPECT_EQ(info.instanceid(),"test_instID1");
    EXPECT_EQ(info.debugserver(),"test_gdbserverAddr");

    litebus::Terminate(funcAgentMgrActor->GetAID());
    litebus::Await(funcAgentMgrActor);
    litebus::Terminate(agentServiceActor->GetAID());
    litebus::Await(agentServiceActor);
    litebus::Terminate(kvServerAccessorAID);
    litebus::Await(kvServerAccessorAID);
    litebus::Terminate(kvServiceActor->GetAID());
    litebus::Await(kvServiceActor);
    litebus::Terminate(leaseServiceActor->GetAID());
    litebus::Await(leaseServiceActor);

}

TEST_F(FuncAgentMgrActorTest, TenantEventCase2)
{
    // same node
    TenantEvent event1 = {
        .tenantID = TENANT_ID1,
        .functionProxyID = "nodeID",
        .functionAgentID = FUNC_AGENT_ID1,
        .instanceID = FUNC_INSTANCE_ID1,
        .agentPodIp = "10.42.1.221",
        .code = static_cast<int32_t>(InstanceState::RUNNING),
    };
    agentMgrActorHelper_->OnTenantUpdateInstance(event1);

    // same tenant on other node
    TenantEvent event2 = {
        .tenantID = TENANT_ID1,
        .functionProxyID = FUNC_PROXY_ID2,
        .functionAgentID = FUNC_AGENT_ID2,
        .instanceID = FUNC_INSTANCE_ID2,
        .agentPodIp = "10.42.2.222",
        .code = static_cast<int32_t>(InstanceState::RUNNING),
    };
    agentMgrActorHelper_->OnTenantUpdateInstance(event2);

    // another instance but same pod in this node
    TenantEvent event3 = {
        .tenantID = TENANT_ID1,
        .functionProxyID = "nodeID",
        .functionAgentID = FUNC_AGENT_ID1,
        .instanceID = FUNC_INSTANCE_ID2,
        .agentPodIp = "10.42.1.221",
        .code = static_cast<int32_t>(InstanceState::RUNNING),
    };
    agentMgrActorHelper_->OnTenantUpdateInstance(event3);

    auto tenantCacheMap = agentMgrActorHelper_->GetTenantCacheMap();
    auto tenantCache = tenantCacheMap[event1.tenantID];
    EXPECT_EQ(tenantCache->podIps.size(), 2u);

    agentMgrActorHelper_->OnTenantDeleteInstance(event3);
    EXPECT_EQ(tenantCache->podIps.size(), 2u);

    agentMgrActorHelper_->OnTenantDeleteInstance(event1);
    EXPECT_EQ(tenantCache->podIps.size(), 1u);
    EXPECT_FALSE(tenantCache->functionAgentCacheMap[event1.functionAgentID].isAgentOnThisNode);
}

TEST_F(FuncAgentMgrActorTest, TenantEventCase3)
{
    auto mockAgent = std::make_shared<MockAgentActor>();
    litebus::Spawn(mockAgent);

    FuncAgentMgrActorHelper::FuncAgentInfo mockAgentInfo;
    mockAgentInfo.aid = mockAgent->GetAID();

    EXPECT_CALL(*mockAgent, SetNetworkIsolationRequest).WillOnce(Return()).WillOnce(Return()).WillOnce(Return());

    agentMgrActorHelper_->InsertAgent(FUNC_AGENT_ID1, mockAgentInfo);
    agentMgrActorHelper_->InsertAgent(FUNC_AGENT_ID2, mockAgentInfo);

    // same tenant on other node
    TenantEvent event2 = {
        .tenantID = TENANT_ID1,
        .functionProxyID = FUNC_PROXY_ID2,
        .functionAgentID = FUNC_AGENT_ID2,
        .instanceID = FUNC_INSTANCE_ID2,
        .agentPodIp = "10.42.2.222",
        .code = static_cast<int32_t>(InstanceState::RUNNING),
    };
    agentMgrActorHelper_->OnTenantUpdateInstance(event2);

    // same node
    TenantEvent event1 = {
        .tenantID = TENANT_ID1,
        .functionProxyID = "nodeID",
        .functionAgentID = FUNC_AGENT_ID1,
        .instanceID = FUNC_INSTANCE_ID1,
        .agentPodIp = "10.42.1.221",
        .code = static_cast<int32_t>(InstanceState::RUNNING),
    };
    agentMgrActorHelper_->OnTenantUpdateInstance(event1);

    TenantEvent event3 = {
        .tenantID = TENANT_ID1,
        .functionProxyID = "nodeID",
        .functionAgentID = "fake_agent_id",
        .instanceID = FUNC_INSTANCE_ID1,
        .agentPodIp = "10.42.1.221",
        .code = static_cast<int32_t>(InstanceState::RUNNING),
    };
    agentMgrActorHelper_->OnTenantUpdateInstance(event3);

    auto tenantCacheMap = agentMgrActorHelper_->GetTenantCacheMap();
    auto tenantCache = tenantCacheMap[event1.tenantID];
    EXPECT_EQ(tenantCache->podIps.size(), 2u);

    agentMgrActorHelper_->OnTenantDeleteInstance(event1);
    EXPECT_EQ(tenantCache->podIps.size(), 1u);
    EXPECT_FALSE(tenantCache->functionAgentCacheMap[event2.functionAgentID].isAgentOnThisNode);

    litebus::Terminate(mockAgent->GetAID());
    litebus::Await(mockAgent->GetAID());
}

class SnapshotFinalizeAgentStub final : public litebus::ActorBase {
public:
    SnapshotFinalizeAgentStub() : ActorBase("snapshot-finalize-agent-stub")
    {
    }

    void Init() override
    {
        Receive("SnapshotAttemptFinalize", &SnapshotFinalizeAgentStub::SnapshotAttemptFinalize);
    }

    void SnapshotAttemptFinalize(const litebus::AID &from, std::string &&, std::string &&msg)
    {
        ::messages::SnapshotAttemptFinalizeRequest request;
        if (!request.ParseFromString(msg)) {
            return;
        }
        requests_.push_back(request);
        ::messages::SnapshotAttemptFinalizeResponse response;
        response.set_attemptid(request.attemptid());
        response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        response.set_remotecleanupcomplete(true);
        (void)Send(from, "SnapshotAttemptFinalizeResponse", response.SerializeAsString());
    }

    std::vector<::messages::SnapshotAttemptFinalizeRequest> requests_;
};

class SnapshotFinalizeMasterStub final : public litebus::ActorBase {
public:
    SnapshotFinalizeMasterStub() : ActorBase("snapshot-finalize-master-stub")
    {
    }

    void Init() override
    {
        Receive("FinalizePausedSnapshotDeleteResponse",
                &SnapshotFinalizeMasterStub::FinalizePausedSnapshotDeleteResponse);
    }

    litebus::Future<::messages::SnapshotAttemptFinalizeResponse> DeletePausedSnapshot(
        const litebus::AID &proxy,
        const ::messages::SnapshotAttemptFinalizeRequest &request)
    {
        response_ = litebus::Promise<::messages::SnapshotAttemptFinalizeResponse>();
        (void)Send(proxy, "FinalizePausedSnapshotDelete", request.SerializeAsString());
        return response_.GetFuture();
    }

    void FinalizePausedSnapshotDeleteResponse(const litebus::AID &, std::string &&, std::string &&msg)
    {
        ::messages::SnapshotAttemptFinalizeResponse response;
        if (response.ParseFromString(msg)) {
            response_.SetValue(response);
        }
    }

private:
    litebus::Promise<::messages::SnapshotAttemptFinalizeResponse> response_;
};

TEST_F(FuncAgentMgrActorTest, PausedDeleteGatewayUsesExistingAgentFinalizePath)
{
    auto agent = std::make_shared<SnapshotFinalizeAgentStub>();
    auto proxy = std::make_shared<FunctionAgentMgrActor>(
        "paused-delete-proxy", PARAM, "paused-delete-node",
        std::make_shared<MockMetaStoreClient>(metaStoreAddress_));
    auto master = std::make_shared<SnapshotFinalizeMasterStub>();
    proxy->funcAgentTable_["agent-healthy"] = {
        .isEnable = true,
        .isInit = true,
        .recoverPromise = std::make_shared<litebus::Promise<bool>>(),
        .aid = agent->GetAID(),
        .instanceIDs = {}
    };
    ASSERT_TRUE(litebus::Spawn(agent).OK());
    ASSERT_TRUE(litebus::Spawn(proxy).OK());
    ASSERT_TRUE(litebus::Spawn(master).OK());

    ::messages::SnapshotAttemptFinalizeRequest request;
    request.set_protocolversion(1);
    request.set_operation(::messages::PAUSED_DELETED);
    request.set_attemptid("paused-delete/instance/snapshot");
    request.set_tenantid("tenant");
    request.set_instanceid("instance");
    request.set_snapshotid("snapshot");
    request.set_expectedstorage("obs");
    request.set_expectedsize(4096);
    request.set_expectedsha256("sha256");

    auto response = master->DeletePausedSnapshot(proxy->GetAID(), request);
    ASSERT_AWAIT_READY(response);
    EXPECT_EQ(response.Get().code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_TRUE(response.Get().remotecleanupcomplete());
    ASSERT_EQ(agent->requests_.size(), size_t{ 1 });
    EXPECT_EQ(agent->requests_[0].attemptid(), request.attemptid());
    EXPECT_EQ(agent->requests_[0].operation(), ::messages::PAUSED_DELETED);

    litebus::Terminate(master->GetAID());
    litebus::Await(master->GetAID());
    litebus::Terminate(proxy->GetAID());
    litebus::Await(proxy->GetAID());
    litebus::Terminate(agent->GetAID());
    litebus::Await(agent->GetAID());
}

class ReusableDeleteAgentStub final : public litebus::ActorBase {
public:
    explicit ReusableDeleteAgentStub(const std::string &name) : ActorBase(name)
    {
    }

    void Init() override
    {
        Receive("DeleteReusableSnapshotArtifact", &ReusableDeleteAgentStub::DeleteArtifact);
    }

    void DeleteArtifact(const litebus::AID &from, std::string &&, std::string &&msg)
    {
        ::messages::DeleteReusableSnapshotArtifactRequest request;
        if (!request.ParseFromString(msg)) {
            return;
        }
        requests_.push_back(request);
        ::messages::DeleteReusableSnapshotArtifactResponse response;
        response.set_requestid(request.requestid());
        response.set_code(common::ERR_NONE);
        (void)Send(from, "DeleteReusableSnapshotArtifactResponse", response.SerializeAsString());
    }

    std::vector<::messages::DeleteReusableSnapshotArtifactRequest> requests_;
};

class ReusableDeleteMasterStub final : public litebus::ActorBase {
public:
    ReusableDeleteMasterStub() : ActorBase("reusable-delete-master-stub")
    {
    }

    void Init() override
    {
        Receive("DeleteReusableSnapshotArtifactResponse",
                &ReusableDeleteMasterStub::DeleteArtifactResponse);
    }

    litebus::Future<::messages::DeleteReusableSnapshotArtifactResponse> DeleteArtifact(
        const litebus::AID &proxy,
        const ::messages::DeleteReusableSnapshotArtifactRequest &request)
    {
        response_ = litebus::Promise<::messages::DeleteReusableSnapshotArtifactResponse>();
        (void)Send(proxy, "DeleteReusableSnapshotArtifact", request.SerializeAsString());
        return response_.GetFuture();
    }

    void DeleteArtifactResponse(const litebus::AID &, std::string &&, std::string &&msg)
    {
        ::messages::DeleteReusableSnapshotArtifactResponse response;
        if (response.ParseFromString(msg)) {
            response_.SetValue(response);
        }
    }

private:
    litebus::Promise<::messages::DeleteReusableSnapshotArtifactResponse> response_;
};

TEST_F(FuncAgentMgrActorTest, ReusableDeleteGatewaySelectsOnlyHealthyInitializedAgent)
{
    auto disabledAgent = std::make_shared<ReusableDeleteAgentStub>("reusable-delete-disabled-agent");
    auto healthyAgent = std::make_shared<ReusableDeleteAgentStub>("reusable-delete-healthy-agent");
    auto proxy = std::make_shared<FunctionAgentMgrActor>(
        "reusable-delete-proxy", PARAM, "reusable-delete-node",
        std::make_shared<MockMetaStoreClient>(metaStoreAddress_));
    auto master = std::make_shared<ReusableDeleteMasterStub>();
    proxy->funcAgentTable_["agent-a-disabled"] = {
        .isEnable = false,
        .isInit = true,
        .recoverPromise = std::make_shared<litebus::Promise<bool>>(),
        .aid = disabledAgent->GetAID(),
        .instanceIDs = {}
    };
    proxy->funcAgentTable_["agent-b-healthy"] = {
        .isEnable = true,
        .isInit = true,
        .recoverPromise = std::make_shared<litebus::Promise<bool>>(),
        .aid = healthyAgent->GetAID(),
        .instanceIDs = {}
    };
    ASSERT_TRUE(litebus::Spawn(disabledAgent).OK());
    ASSERT_TRUE(litebus::Spawn(healthyAgent).OK());
    ASSERT_TRUE(litebus::Spawn(proxy).OK());
    ASSERT_TRUE(litebus::Spawn(master).OK());

    ::messages::DeleteReusableSnapshotArtifactRequest request;
    request.set_requestid("reusable-delete-correlation");
    request.set_tenantid("tenant");
    request.set_snapshotid("snapshot");
    request.mutable_artifact()->set_storagebackend("obs");
    request.mutable_artifact()->set_objectkey("reusable/v1/hash/snapshot/checkpoint.img");
    request.mutable_artifact()->set_size(4096);
    request.mutable_artifact()->set_sha256(std::string(64, 'a'));
    request.mutable_artifact()->set_format("sandboxd-checkpoint");
    request.mutable_artifact()->set_formatversion(1);

    auto response = master->DeleteArtifact(proxy->GetAID(), request);
    ASSERT_AWAIT_READY(response);
    EXPECT_EQ(response.Get().requestid(), request.requestid());
    EXPECT_EQ(response.Get().code(), common::ERR_NONE);
    EXPECT_TRUE(disabledAgent->requests_.empty());
    ASSERT_EQ(healthyAgent->requests_.size(), size_t{ 1 });
    EXPECT_EQ(healthyAgent->requests_[0].SerializeAsString(), request.SerializeAsString());

    litebus::Terminate(master->GetAID());
    litebus::Await(master->GetAID());
    litebus::Terminate(proxy->GetAID());
    litebus::Await(proxy->GetAID());
    litebus::Terminate(healthyAgent->GetAID());
    litebus::Await(healthyAgent->GetAID());
    litebus::Terminate(disabledAgent->GetAID());
    litebus::Await(disabledAgent->GetAID());
}

}  // namespace functionsystem::test
TEST_F(FuncAgentMgrActorTest, ResizeDuplicateJoinsButConflictingReservationStaysUnknown)
{
    auto request = std::make_shared<messages::ResizeSandboxRequest>();
    request->set_requestid("reservation-1");
    request->set_instanceid("instance");
    request->set_runtimeid("runtime");
    request->mutable_resize()->set_sandbox_id("sandbox");
    request->mutable_resize()->set_resource_generation("generation");
    FunctionAgentMgrActor::ResizePending pending{litebus::AID("agent", "127.0.0.1:1"), *request, {}, {}};
    auto original = pending.completion.GetFuture();
    agentMgrActorHelper_->resizePending_.emplace("wire-1", pending);
    auto joined = agentMgrActorHelper_->ResizeSandbox({}, request);
    EXPECT_FALSE(joined.IsOK());
    EXPECT_EQ(agentMgrActorHelper_->resizePending_.size(), 1u);
    auto conflict = std::make_shared<messages::ResizeSandboxRequest>(*request);
    conflict->mutable_resize()->mutable_target()->set_memory_limit_bytes(2048);
    EXPECT_EQ(agentMgrActorHelper_->ResizeSandbox({}, conflict).Get().outcome(),
              messages::ResizeSandboxResponse::UNKNOWN);
    EXPECT_FALSE(original.IsOK());
    agentMgrActorHelper_->ResizeSandboxTimeout("wire-1");
    EXPECT_EQ(original.Get().outcome(), messages::ResizeSandboxResponse::UNKNOWN);
    EXPECT_EQ(joined.Get().outcome(), messages::ResizeSandboxResponse::UNKNOWN);
    EXPECT_TRUE(agentMgrActorHelper_->resizePending_.empty());
}

TEST_F(FuncAgentMgrActorTest, ResizeReplyMustMatchSenderGenerationAndFiniteReadback)
{
    messages::ResizeSandboxRequest request;
    request.set_requestid("reservation");
    request.set_instanceid("instance");
    request.set_runtimeid("runtime");
    request.mutable_resize()->set_sandbox_id("sandbox");
    request.mutable_resize()->set_resource_generation("generation");
    const litebus::AID agent("agent", "127.0.0.1:1");
    FunctionAgentMgrActor::ResizePending pending{agent, request, {}, {}};
    pending.dispatched = true;
    auto future = pending.completion.GetFuture();
    agentMgrActorHelper_->resizePending_.emplace("wire-current", pending);
    messages::ResizeSandboxResponse response;
    response.set_requestid("wire-current");
    response.set_instanceid(request.instanceid());
    response.set_runtimeid(request.runtimeid());
    response.set_sandboxid(request.resize().sandbox_id());
    response.set_resourcegeneration(request.resize().resource_generation());
    response.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
    agentMgrActorHelper_->ResizeSandboxResponse(litebus::AID("other", "127.0.0.1:1"), "", response.SerializeAsString());
    EXPECT_FALSE(future.IsOK());
    response.set_resourcegeneration("old-generation");
    agentMgrActorHelper_->ResizeSandboxResponse(agent, "", response.SerializeAsString());
    EXPECT_FALSE(future.IsOK());
    response.set_resourcegeneration(request.resize().resource_generation());
    // An OBSERVED envelope without actual limits must not look like success.
    agentMgrActorHelper_->ResizeSandboxResponse(agent, "", response.SerializeAsString());
    EXPECT_EQ(future.Get().outcome(), messages::ResizeSandboxResponse::UNKNOWN);
    EXPECT_EQ(future.Get().requestid(), request.requestid());
    EXPECT_TRUE(agentMgrActorHelper_->resizePending_.empty());

    FunctionAgentMgrActor::ResizePending next{agent, request, {}, {}};
    next.dispatched = true;
    auto nextFuture = next.completion.GetFuture();
    agentMgrActorHelper_->resizePending_.emplace("wire-next", next);
    agentMgrActorHelper_->ResizeSandboxTimeout("wire-current");
    agentMgrActorHelper_->ResizeSandboxResponse(agent, "", response.SerializeAsString());
    EXPECT_FALSE(nextFuture.IsOK());
    agentMgrActorHelper_->ResizeSandboxTimeout("wire-next");
    EXPECT_EQ(nextFuture.Get().outcome(), messages::ResizeSandboxResponse::UNKNOWN);
}

TEST_F(FuncAgentMgrActorTest, ResizeBudgetRejectionPreservesAdmissionStatus)
{
    messages::ResizeSandboxRequest request;
    request.set_requestid("caller-reservation");
    FunctionAgentMgrActor::ResizePending pending{litebus::AID("agent", "127.0.0.1:1"), request, {}, {}};
    auto future = pending.completion.GetFuture();
    agentMgrActorHelper_->resizePending_.emplace("budget-wire", pending);
    agentMgrActorHelper_->OnResizeReserved("budget-wire", litebus::Future<Status>(Status(RESOURCE_NOT_ENOUGH, "CPU budget")), nullptr);
    ASSERT_TRUE(future.IsOK());
    EXPECT_EQ(future.Get().code(), static_cast<int32_t>(RESOURCE_NOT_ENOUGH));
    EXPECT_EQ(future.Get().outcome(), messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH);
    EXPECT_TRUE(agentMgrActorHelper_->resizePending_.empty());
}
TEST_F(FuncAgentMgrActorTest, RepeatedBudgetCallbackCannotRejectAnAlreadyDispatchedCommand)
{
    messages::ResizeSandboxRequest request;
    request.set_requestid("caller-reservation");
    FunctionAgentMgrActor::ResizePending pending{litebus::AID("agent", "127.0.0.1:1"), request, {}, {}};
    pending.dispatched = true;
    auto future = pending.completion.GetFuture();
    agentMgrActorHelper_->resizePending_.emplace("budget-wire", pending);
    agentMgrActorHelper_->OnResizeReserved("budget-wire", litebus::Future<Status>(Status(RESOURCE_NOT_ENOUGH)), nullptr);
    EXPECT_FALSE(future.IsOK());
    EXPECT_EQ(agentMgrActorHelper_->resizePending_.size(), size_t{1});
    agentMgrActorHelper_->ResizeSandboxTimeout("budget-wire");
    EXPECT_EQ(future.Get().outcome(), messages::ResizeSandboxResponse::UNKNOWN);
}

}  // namespace functionsystem::test
