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

// REAL-STREAM identity-binding tests (#S, open gate 1): a REAL gRPC server
// hosting PosixService, a REAL ControlClient connecting with its metadata,
// and the PRODUCTION posix dispatch chain (PosixStream::Receiver →
// PosixClient::handlers_ → StateHandler::SaveState → StateActor). The
// accept-time resolver must bind the stream to the machine's creation
// generation so the save lands on the COMPOSED key; after an in-place
// failover swap, the STILL-CONNECTED old stream's late save is refused at
// the guarded executor with ZERO cache writes.

#include <grpcpp/grpcpp.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "common/posix_service/posix_service.h"
#include "common/rpc/stream/posix/control_client.h"
#include "common/utils/meta_store_kv_operation.h"
#include "function_proxy/common/iam/internal_iam.h"
#include "function_proxy/common/state_handler/state_handler.h"
#include "function_proxy/common/state_handler/state_handler_helper.h"
#include "function_proxy/common/state_handler/state_isolation_support.h"
#include "function_proxy/common/state_machine/instance_control_view.h"
#include "mocks/mock_distributed_cache_client.h"
#include "mocks/mock_meta_store_client.h"
#include "mocks/mock_txn_transaction.h"
#include "mocks/mock_runtime_client.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {

using namespace functionsystem::function_proxy;
using namespace testing;

class PosixStreamBindingFixture : public ::testing::Test {
protected:
    void SetUp() override
    {
        // Real state chain: mock cache -> real StateClient -> real StateActor
        // with the guarded executors bound to a REAL view.
        cache_ = std::make_shared<MockDistributedCacheClient>();
        EXPECT_CALL(*cache_, Init).WillOnce(Return(Status::OK()));
        stateActor_ = std::make_shared<StateActor>(std::make_shared<StateClient>(cache_));
        litebus::Spawn(stateActor_);
        StateHandler::BindStateActor(stateActor_);
        view_ = std::make_shared<InstanceControlView>("node-test", false);
        stateActor_->SetSaveExecutor(
            [view = view_](const std::string &id, const std::string &runtime, const std::string &req,
                           const StateActor::GuardedStoreOp &op) {
                return view->SaveStateGuarded(id, runtime, req, op);
            });
        stateActor_->SetDeleteExecutor(
            [view = view_](const std::string &id, const std::string &runtime, const std::string &req,
                           const StateActor::GuardedStoreOp &op) {
                return view->DeleteStateGuarded(id, runtime, req, op);
            });
        // v3: bind the save registry to the REAL map transaction model
        // (no wildcard-success transactions) and seed the authoritative
        // record for the machine created below.
        metaCore_ = std::make_shared<
            functionsystem::function_proxy::test::MapMetaStoreCore>();
        registryMeta_ = std::make_shared<
            functionsystem::function_proxy::test::MapMetaStore>(metaCore_);
        stateActor_->BindSaveRegistry(registryMeta_, [](const std::string &, const std::string &, const std::string &) { return "default/0-test-helloWorld/$latest"; });

        // Real gRPC server hosting the production PosixService with the
        // production resolver shape (view snapshot; runtime must match).
        posixService_ = std::make_shared<PosixService>();
        InternalIAM::Param iamParam;
        iamParam.isEnableIAM = false;
        internalIAM_ = std::make_shared<InternalIAM>(iamParam);
        posixService_->BindInternalIAM(internalIAM_);
        posixService_->SetStreamIdentityResolver(
            [view = view_](const std::string &instanceID, const std::string &runtimeID) {
                const auto snapshot = view->GetIncarnationSnapshot(instanceID);
                if (snapshot.second.empty()) { return std::string{}; }
                if (!snapshot.first.empty()) {
                    return (runtimeID.empty() || snapshot.first != runtimeID) ? std::string{}
                                                                              : snapshot.second;
                }
                // FIRST-CREATION window: machine has the creation request
                // but no runtime yet — bind the generation (saves still
                // re-check runtime equality at execution time).
                return snapshot.second;
            });
        builder_.RegisterService(posixService_.get());
        int port = 0;
        builder_.AddListeningPort("127.0.0.1:0", ::grpc::InsecureServerCredentials(), &port);
        server_ = builder_.BuildAndStart();
        ASSERT_NE(server_, nullptr);
        target_ = "127.0.0.1:" + std::to_string(port);

        // Production marks the instance CONNECTABLE (AddTokenReference →
        // IAM verified set) when its client is created; the accept path
        // enforces this. Mirror it for the machine under test.
        internalIAM_->Insert("inst-stream");

        // The machine the stream must bind to — and its authoritative
        // record (Phase 0 verifies against it).
        resources::InstanceInfo info;
        info.set_instanceid("inst-stream");
        info.set_runtimeid("rt-1");
        info.set_requestid("req-gen-1");
        info.set_function("default/0-test-helloWorld/$latest");
        view_->GenerateStateMachine("inst-stream", info);
        std::string recordJson;
        if (TransToJsonFromInstanceInfo(recordJson, info)) {
            const auto keyOpt = GenInstanceKey("default/0-test-helloWorld/$latest",
                                               "inst-stream", "req-gen-1");
            if (!keyOpt.IsNone()) {
                metaCore_->Seed(keyOpt.Get(), recordJson);
            }
        }
    }

    void TearDown() override
    {
        client_ = nullptr;
        // The service keeps a STATIC ready-client registry: clear this
        // instance so later fixtures/other suites start clean.
        PosixService::DeleteClient("inst-stream");
        if (server_) { server_->Shutdown(); }
        if (stateActor_ != nullptr) {
            litebus::Terminate(stateActor_->GetAID());
            litebus::Await(stateActor_->GetAID());
            StateHandlerHelper::ClearStateActorHelper();
        }
    }

    std::shared_ptr<MockRuntimeClient> Connect(const std::string &instanceID, const std::string &runtimeID)
    {
        RuntimeClientConfig config;
        config.serverAddress = target_;
        config.serverName = "daylight";
        config.instanceID = instanceID;
        config.runtimeID = runtimeID;
        auto client = std::make_shared<MockRuntimeClient>(config);
        client->Start();
        // BOUNDED accept confirmation via the service's own registry.
        bool accepted = false;
        for (int i = 0; i < 100 && !(accepted = PosixService::CheckClientIsReady(instanceID)); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        EXPECT_TRUE(accepted) << "server did not accept the stream within 5s";
        return client;
    }

    // Bounded response hand-off keyed by message id: the mock's receive
    // callback completes the promise for OUR sent id.
    std::shared_ptr<litebus::Promise<runtime_rpc::StreamingMessage>> responsePromise_;
    std::string awaitedMessageId_;

    std::shared_ptr<runtime_rpc::StreamingMessage> SaveMessage(const std::string &state)
    {
        auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
        msg->mutable_savereq()->set_state(state);
        msg->set_messageid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
        return msg;
    }

    std::shared_ptr<MockDistributedCacheClient> cache_;
    std::shared_ptr<functionsystem::function_proxy::test::MapMetaStoreCore> metaCore_;
    std::shared_ptr<functionsystem::function_proxy::test::MapMetaStore> registryMeta_;
    std::shared_ptr<StateActor> stateActor_;
    std::shared_ptr<InstanceControlView> view_;
    std::shared_ptr<PosixService> posixService_;
    std::shared_ptr<InternalIAM> internalIAM_;
    ::grpc::ServerBuilder builder_;
    std::unique_ptr<::grpc::Server> server_;
    std::shared_ptr<MockRuntimeClient> client_;
    std::string target_;
};

// A FAILED actor-chain future must surface on the REAL stream as an
// EXPLICIT RPC error response (the handler-level OnComplete bridge) —
// never a hang, a dropped stream, or a fake success — with ZERO cache
// writes.
TEST_F(PosixStreamBindingFixture, FailedFutureSurfacesAsExplicitStreamError)
{
    client_ = Connect("inst-stream", "rt-1");
    ASSERT_NE(client_, nullptr);

    auto msg = SaveMessage("stream-state-failed-future");
    awaitedMessageId_ = msg->messageid();
    responsePromise_ = std::make_shared<litebus::Promise<runtime_rpc::StreamingMessage>>();
    EXPECT_CALL(*client_, MockReceiver(_))
        .WillRepeatedly(Invoke([this](const std::shared_ptr<runtime_rpc::StreamingMessage> &recv) {
            if (recv->messageid() == awaitedMessageId_) {
                responsePromise_->SetValue(*recv);
            }
        }));
    EXPECT_CALL(*cache_, Set(Matcher<const std::string &>(_), Matcher<const std::string &>(_)))
        .Times(0);

    // The authoritative-record read fails AS A FUTURE inside the actor
    // chain: every Then past it is skipped (litebus future_base); the
    // bridge must convert that into an explicit error response.
    metaCore_->forcedGets.push_back(
        { GenInstanceKey("default/0-test-helloWorld/$latest", "inst-stream", "req-gen-1").Get(),
          functionsystem::function_proxy::test::GetOutcome::FAIL_FUTURE });

    auto sent = client_->Send(msg);
    ASSERT_AWAIT_READY(sent);
    EXPECT_TRUE(sent.Get());
    auto rsp = responsePromise_->GetFuture();
    ASSERT_AWAIT_READY(rsp);
    EXPECT_NE(rsp.Get().saversp().code(), common::ERR_NONE);
    EXPECT_THAT(rsp.Get().saversp().message(), testing::HasSubstr("future failed"))
        << "the RPC must carry the explicit chain-failure message, got: "
        << rsp.Get().saversp().message();
}

// The connected stream saves under the COMPOSED key of the machine's
// creation generation — the accept-time binding flowed end-to-end through
// the real dispatch chain.
TEST_F(PosixStreamBindingFixture, RealStreamSaveLandsOnComposedGenerationKey)
{
    client_ = Connect("inst-stream", "rt-1");
    ASSERT_NE(client_, nullptr);

    auto msg = SaveMessage("stream-state");
    awaitedMessageId_ = msg->messageid();
    responsePromise_ = std::make_shared<litebus::Promise<runtime_rpc::StreamingMessage>>();
    EXPECT_CALL(*client_, MockReceiver(_))
        .WillRepeatedly(Invoke([this](const std::shared_ptr<runtime_rpc::StreamingMessage> &recv) {
            if (recv->messageid() == awaitedMessageId_) {
                responsePromise_->SetValue(*recv);
            }
        }));

    const std::string composed = StateClient::ComposeStateKey("inst-stream", "req-gen-1");
    (void)composed;  // v3: saves land on per-save SNAPSHOT keys, not the legacy composed key
    EXPECT_CALL(*cache_, Set(Matcher<const std::string &>(_), Matcher<const std::string &>(_)))
        .WillOnce(Return(Status::OK()));
    auto sent = client_->Send(msg);
    ASSERT_AWAIT_READY(sent);
    EXPECT_TRUE(sent.Get());
    auto rsp = responsePromise_->GetFuture();
    ASSERT_AWAIT_READY(rsp);
    EXPECT_EQ(rsp.Get().saversp().code(), common::ERR_NONE) << rsp.Get().saversp().message();
    EXPECT_TRUE(functionsystem::StateClient::IsSnapshotKey(rsp.Get().saversp().checkpointid()))
        << "v3: expected snapshot namespace, got: " << rsp.Get().saversp().checkpointid();
}

// FIRST-CREATION ordering (root creation-order evidence): the stream
// connects while the machine is CREATING (runtimeid empty, requestid
// present) — the accept binds the creation generation; when the deploy
// response publishes THIS runtime, the still-connected stream's save lands
// on that generation's SNAPSHOT key. A stale previous-generation runtime
// sneaking into the same window is refused at execution time.
TEST_F(PosixStreamBindingFixture, EarlyConnectionDuringCreationSavesAfterRuntimePublish)
{
    // Re-create the machine in the CREATING shape: requestid, EMPTY runtime.
    resources::InstanceInfo creating;
    creating.set_instanceid("inst-stream");
    creating.set_runtimeid("");
    creating.set_requestid("req-gen-2");
    view_->GenerateStateMachine("inst-stream", creating);

    client_ = Connect("inst-stream", "rt-9");
    ASSERT_NE(client_, nullptr);

    auto msg = SaveMessage("creation-state");
    awaitedMessageId_ = msg->messageid();
    responsePromise_ = std::make_shared<litebus::Promise<runtime_rpc::StreamingMessage>>();
    EXPECT_CALL(*client_, MockReceiver(_))
        .WillRepeatedly(Invoke([this](const std::shared_ptr<runtime_rpc::StreamingMessage> &recv) {
            if (recv->messageid() == awaitedMessageId_) {
                responsePromise_->SetValue(*recv);
            }
        }));

    // The deploy response publishes the runtime of the SAME generation —
    // and (production invariant) the authoritative record follows the
    // published incarnation, so Phase-0 ownership verification passes for
    // the still-connected stream.
    resources::InstanceInfo published;
    published.set_instanceid("inst-stream");
    published.set_runtimeid("rt-9");
    published.set_requestid("req-gen-2");
    published.set_function("default/0-test-helloWorld/$latest");
    view_->Update("inst-stream", published, true);
    {
        std::string recordJson;
        if (TransToJsonFromInstanceInfo(recordJson, published)) {
            const auto keyOpt = GenInstanceKey("default/0-test-helloWorld/$latest",
                                               "inst-stream", "req-gen-2");
            if (!keyOpt.IsNone()) {
                metaCore_->Seed(keyOpt.Get(), recordJson);
            }
        }
    }

    EXPECT_CALL(*cache_, Set(Matcher<const std::string &>(_), Matcher<const std::string &>(_)))
        .WillOnce(Return(Status::OK()));
    auto sent = client_->Send(msg);
    ASSERT_AWAIT_READY(sent);
    EXPECT_TRUE(sent.Get());
    auto rsp = responsePromise_->GetFuture();
    ASSERT_AWAIT_READY(rsp);
    EXPECT_EQ(rsp.Get().saversp().code(), common::ERR_NONE) << rsp.Get().saversp().message();
    EXPECT_TRUE(functionsystem::StateClient::IsSnapshotKey(rsp.Get().saversp().checkpointid()))
        << "v3: expected snapshot namespace, got: " << rsp.Get().saversp().checkpointid();
}

// Same window, hostile variant: a stream from the OLD runtime (rt-1)
// connects while the machine is CREATING rt-9 — the binding may hold, but
// execution-time runtime verification refuses every write (zero Set).
TEST_F(PosixStreamBindingFixture, StaleRuntimeInCreationWindowRefusedAtExecution)
{
    resources::InstanceInfo creating;
    creating.set_instanceid("inst-stream");
    creating.set_runtimeid("");
    creating.set_requestid("req-gen-3");
    view_->GenerateStateMachine("inst-stream", creating);

    client_ = Connect("inst-stream", "rt-stale");
    ASSERT_NE(client_, nullptr);

    auto msg = SaveMessage("stale-creation");
    awaitedMessageId_ = msg->messageid();
    responsePromise_ = std::make_shared<litebus::Promise<runtime_rpc::StreamingMessage>>();
    EXPECT_CALL(*client_, MockReceiver(_))
        .WillRepeatedly(Invoke([this](const std::shared_ptr<runtime_rpc::StreamingMessage> &recv) {
            if (recv->messageid() == awaitedMessageId_) {
                responsePromise_->SetValue(*recv);
            }
        }));

    resources::InstanceInfo published;
    published.set_instanceid("inst-stream");
    published.set_runtimeid("rt-real");
    published.set_requestid("req-gen-3");
    published.set_function("default/0-test-helloWorld/$latest");
    view_->Update("inst-stream", published, true);
    // The published incarnation's authoritative record exists — the stale
    // runtime fails Phase-0 verification against it (an EARLIER, equally
    // zero-write refusal than the executor-phase one).
    {
        std::string recordJson;
        if (TransToJsonFromInstanceInfo(recordJson, published)) {
            const auto keyOpt = GenInstanceKey("default/0-test-helloWorld/$latest",
                                               "inst-stream", "req-gen-3");
            if (!keyOpt.IsNone()) {
                metaCore_->Seed(keyOpt.Get(), recordJson);
            }
        }
    }

    EXPECT_CALL(*cache_, Set(Matcher<const std::string &>(_), Matcher<const std::string &>(_))).Times(0);
    auto sent = client_->Send(msg);
    ASSERT_AWAIT_READY(sent);
    EXPECT_TRUE(sent.Get());
    auto rsp = responsePromise_->GetFuture();
    ASSERT_AWAIT_READY(rsp);
    EXPECT_NE(rsp.Get().saversp().code(), common::ERR_NONE)
        << "the stale runtime's save must be refused (ownership verification)";
    EXPECT_THAT(rsp.Get().saversp().message(), HasSubstr("ownership verification failed"));
}

// In-place failover swaps the machine to rt-2; the STILL-CONNECTED old
// stream (bound at accept time to rt-1/req-gen-1) sends a late save — the
// guarded executor refuses with ZERO cache writes.
TEST_F(PosixStreamBindingFixture, LateOldStreamSaveAfterFailoverZeroWrites)
{
    client_ = Connect("inst-stream", "rt-1");
    ASSERT_NE(client_, nullptr);

    auto msg = SaveMessage("stale-state");
    awaitedMessageId_ = msg->messageid();
    responsePromise_ = std::make_shared<litebus::Promise<runtime_rpc::StreamingMessage>>();
    EXPECT_CALL(*client_, MockReceiver(_))
        .WillRepeatedly(Invoke([this](const std::shared_ptr<runtime_rpc::StreamingMessage> &recv) {
            if (recv->messageid() == awaitedMessageId_) {
                responsePromise_->SetValue(*recv);
            }
        }));

    // Failover: same request generation, NEW runtime.
    resources::InstanceInfo swapped;
    swapped.set_instanceid("inst-stream");
    swapped.set_runtimeid("rt-2");
    swapped.set_requestid("req-gen-1");
    view_->Update("inst-stream", swapped, true);

    EXPECT_CALL(*cache_, Set(Matcher<const std::string &>(_), Matcher<const std::string &>(_))).Times(0);
    auto sent = client_->Send(msg);
    ASSERT_AWAIT_READY(sent);
    EXPECT_TRUE(sent.Get());
    auto rsp = responsePromise_->GetFuture();
    ASSERT_AWAIT_READY(rsp);
    EXPECT_NE(rsp.Get().saversp().code(), common::ERR_NONE)
        << "the late old-stream save must be refused";
    // The refusal reason comes from the MACHINE-level execution guard
    // (ExecuteStateOpUnderLock) — the authoritative fence.
    EXPECT_THAT(rsp.Get().saversp().message(), HasSubstr("UNKNOWN"));
}

}  // namespace functionsystem::test
