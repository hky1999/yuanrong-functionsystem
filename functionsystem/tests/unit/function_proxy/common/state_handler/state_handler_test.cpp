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

#include "function_proxy/common/state_handler/state_handler.h"
#include "function_proxy/common/state_machine/instance_control_view.h"

#include "common/hex/hex.h"
#include "common/utils/files.h"
#include "gtest/gtest.h"
#include "mocks/mock_distributed_cache_client.h"
#include "mocks/mock_meta_store_client.h"
#include "mocks/mock_txn_transaction.h"
#include "state_handler_helper.h"
#include "utils/future_test_helper.h"

namespace functionsystem::function_proxy::test {

using namespace functionsystem::test;
using namespace testing;

class StateHandlerTest : public ::testing::Test {
public:
    void SetUp() override
    {
        distributedCacheClient_ = std::make_shared<MockDistributedCacheClient>();
        EXPECT_CALL(*distributedCacheClient_, Init).WillOnce(Return(Status::OK()));

        stateClient_ = std::make_shared<StateClient>(distributedCacheClient_);
        stateActor_ = std::make_shared<StateActor>(stateClient_);
        litebus::Spawn(stateActor_);
        StateHandler::BindStateActor(stateActor_);
        // NEW CONTRACT (#S): saves/deletes execute under the guarded view
        // executors — an unbound actor refuses by design. The fixture's
        // view starts EMPTY (deletes = retire completion allowed); tests
        // that need a live machine generate one on it.
        view_ = std::make_shared<functionsystem::InstanceControlView>("node-test", false);
        stateActor_->SetSaveExecutor(
            [view = view_](const std::string &id, const std::string &runtime,
                           const StateActor::GuardedStoreOp &op) {
                return view->SaveStateGuarded(id, runtime, op);
            });
        stateActor_->SetDeleteExecutor(
            [view = view_](const std::string &id, const std::string &runtime,
                           const StateActor::GuardedStoreOp &op) {
                return view->DeleteStateGuarded(id, runtime, op);
            });
        // v3: bind the save registry (a mock meta-store returning ready
        // success transactions — the DelInstanceTest recipe).
        registryMeta_ = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");
        auto mockTxn = std::make_shared<MockTxnTransaction>(litebus::AID());
        EXPECT_CALL(*registryMeta_, BeginTransaction).WillRepeatedly(Return(mockTxn));
        EXPECT_CALL(*mockTxn, Commit).WillRepeatedly(
            Return(litebus::Future<std::shared_ptr<TxnResponse>>(
                []() {
                    auto ok = std::make_shared<TxnResponse>();
                    ok->success = true;
                    return ok;
                }())));
        stateActor_->BindSaveRegistry(registryMeta_);
    }

    void TearDown() override
    {
        stateClient_ = nullptr;
        litebus::Terminate(stateActor_->GetAID());
        litebus::Await(stateActor_->GetAID());
        stateActor_ = nullptr;
        distributedCacheClient_ = nullptr;
    }

protected:
    std::shared_ptr<MockDistributedCacheClient> distributedCacheClient_;
    std::shared_ptr<MockMetaStoreClient> registryMeta_;
    std::shared_ptr<StateActor> stateActor_;
    std::shared_ptr<StateClient> stateClient_;
    std::shared_ptr<functionsystem::InstanceControlView> view_;

    // Registers the machine a guarded save must match.
    void RegisterMachine(const std::string &id, const std::string &runtime, const std::string &req)
    {
        resources::InstanceInfo info;
        info.set_instanceid(id);
        info.set_runtimeid(runtime);
        info.set_requestid(req);
        view_->GenerateStateMachine(id, info);
    }
};

/**
 * Feature: RetryInitTest
 * Description: retry test
 * Steps:
 * 1. init failed
 * 2. retry init
 *
 * Expectation:
 * 1. finished
 */
TEST_F(StateHandlerTest, RetryInitTest)
{
    litebus::Terminate(stateActor_->GetAID());
    litebus::Await(stateActor_->GetAID());

    distributedCacheClient_ = std::make_shared<MockDistributedCacheClient>();
    bool isFinished = false;
    EXPECT_CALL(*distributedCacheClient_, Init)
        .WillOnce(Return(Status(StatusCode::FAILED)))
        .WillOnce(DoAll(Assign(&isFinished, true), Return(Status::OK())));
    stateClient_ = std::make_shared<StateClient>(distributedCacheClient_);
    stateActor_ = std::make_shared<StateActor>(stateClient_);
    litebus::Spawn(stateActor_);
    StateHandler::BindStateActor(stateActor_);
    EXPECT_AWAIT_TRUE([&isFinished]() -> bool { return isFinished; });

    litebus::Terminate(stateActor_->GetAID());
    litebus::Await(stateActor_->GetAID());
}

/**
 * Feature: SaveStateFailed
 * Description: save state failed
 * Steps:
 * 1. save state with empty instance id
 * 2. save state set to cache failed
 *
 * Expectation:
 * 1. StatusCode::ERR_PARAM_INVALID
 * 2. StatusCode::ERR_INNER_SYSTEM_ERROR
 */
TEST_F(StateHandlerTest, SaveStateFailed)
{
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    auto response = StateHandler::SaveState(grpc::PosixStreamIdentity{}, request);
    EXPECT_AWAIT_READY(response);
    EXPECT_EQ(response.Get()->saversp().code(), common::ErrorCode::ERR_PARAM_INVALID);
    EXPECT_EQ(response.Get()->saversp().message(), "save state failed: empty instance id");

    RegisterMachine("instance_id", "rt-1", "req-gen-1");
    EXPECT_CALL(*distributedCacheClient_, Set).WillOnce(Return(Status(StatusCode::FAILED)));
    response = StateHandler::SaveState(grpc::PosixStreamIdentity{"instance_id", "rt-1", "req-gen-1"}, request);
    EXPECT_AWAIT_READY(response);
    EXPECT_EQ(response.Get()->saversp().code(), common::ErrorCode::ERR_INNER_SYSTEM_ERROR);
    EXPECT_THAT(response.Get()->saversp().message(), testing::HasSubstr("save state failed: [code: -1"));
}

/**
* Feature:
* Description: DeleteStateSuccess
* Steps:
* 1. Delete state

* Expectation:
* 1. StatusCode::ERR_NONE
*/
TEST_F(StateHandlerTest, DeleteState)
{
    auto distributedCacheClient1_ = std::make_shared<MockDistributedCacheClient>();
    EXPECT_CALL(*distributedCacheClient1_, Init).WillOnce(Return(Status::OK()));
    EXPECT_CALL(*distributedCacheClient1_, Del(Matcher<const std::string &>("instanceID"))).WillOnce(Return(Status::OK()));

    auto stateClient1_ = std::make_shared<StateClient>(distributedCacheClient1_);
    stateClient1_->Init();
    auto status = stateClient1_->Del("instanceID");
    EXPECT_TRUE(status.IsOk());
}

/**
 * Feature: SaveStateSuccess
 * Description: save state success
 * Steps:
 * 1. save state
 *
 * Expectation:
 * 1. StatusCode::ERR_NONE
 */
TEST_F(StateHandlerTest, SaveStateSuccess)
{
    RegisterMachine("instance_id", "rt-1", "req-gen-1");
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    EXPECT_CALL(*distributedCacheClient_, Set).WillOnce(Return(Status::OK()));
    auto response = StateHandler::SaveState(grpc::PosixStreamIdentity{"instance_id", "rt-1", "req-gen-1"}, request);
    EXPECT_AWAIT_READY(response);
    EXPECT_EQ(response.Get()->saversp().code(), common::ErrorCode::ERR_NONE);
    // The save returns the VERSIONED checkpoint id (composed key).
    EXPECT_EQ(response.Get()->saversp().checkpointid(),
              StateClient::ComposeStateKey("instance_id", "req-gen-1"));
}

// Save → recover BY VERSIONED CHECKPOINT REFERENCE: the save returns the
// composed checkpoint id; a later load naming exactly that reference reads
// the saved bytes back (the cross-recovery contract).
TEST_F(StateHandlerTest, SaveThenLoadByCheckpointReferenceRoundTrip)
{
    RegisterMachine("instance_id", "rt-1", "req-gen-1");
    const std::string composed = StateClient::ComposeStateKey("instance_id", "req-gen-1");
    std::string saved;
    auto saveMsg = std::make_shared<runtime_rpc::StreamingMessage>();
    saveMsg->mutable_savereq()->set_state("checkpoint-bytes");
    EXPECT_CALL(*distributedCacheClient_, Set(Matcher<const std::string &>(composed),
                                              Matcher<const std::string &>(_)))
        .WillOnce(DoAll(SaveArg<1>(&saved), Return(Status::OK())));
    auto save = StateHandler::SaveState(grpc::PosixStreamIdentity{"instance_id", "rt-1", "req-gen-1"}, saveMsg);
    EXPECT_AWAIT_READY(save);
    ASSERT_EQ(save.Get()->saversp().code(), common::ErrorCode::ERR_NONE);
    ASSERT_EQ(save.Get()->saversp().checkpointid(), composed);

    auto loadMsg = std::make_shared<runtime_rpc::StreamingMessage>();
    StateLoadRequest loadReq{};
    loadReq.set_checkpointid(save.Get()->saversp().checkpointid());
    *loadMsg->mutable_loadreq() = std::move(loadReq);
    EXPECT_CALL(*distributedCacheClient_,
                Get(Matcher<const std::string &>(composed), Matcher<std::string &>(_)))
        .WillOnce(DoAll(SetArgReferee<1>(saved), Return(Status::OK())));
    // A SUCCESSOR generation (new runtime AND new request) recovers the
    // ORIGINALLY SAVED reference: loads are reference-driven, the caller
    // identity is not a read restriction (the explicit-reference contract).
    auto load = StateHandler::LoadState(grpc::PosixStreamIdentity{"instance_id", "rt-2", "req-gen-2"}, loadMsg);
    EXPECT_AWAIT_READY(load);
    EXPECT_EQ(load.Get()->loadrsp().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(load.Get()->loadrsp().state(), "checkpoint-bytes");
}

// OLD-CACHE MIGRATION GAP (kept honest, not silently "supported"): a load
// by a BARE legacy reference is refused — the backfill migration owns old
// data; this refusal is migration-pending, not a permanent semantic.
TEST_F(StateHandlerTest, LoadBareLegacyReferenceRefusedPendingMigration)
{
    auto loadMsg = std::make_shared<runtime_rpc::StreamingMessage>();
    StateLoadRequest loadReq{};
    loadReq.set_checkpointid("instance_id");
    *loadMsg->mutable_loadreq() = std::move(loadReq);
    EXPECT_CALL(*distributedCacheClient_, Get(Matcher<const std::string &>(_), Matcher<std::string &>(_))).Times(0);
    auto load = StateHandler::LoadState(grpc::PosixStreamIdentity{"instance_id", "rt-1", "req-gen-1"}, loadMsg);
    EXPECT_AWAIT_READY(load);
    EXPECT_EQ(load.Get()->loadrsp().code(), common::ErrorCode::ERR_PARAM_INVALID);
    EXPECT_THAT(load.Get()->loadrsp().message(), testing::HasSubstr("not versioned"));
}

// NEW CONTRACT negative: no machine (or a superseded runtime) refuses with
// ZERO cache writes.
TEST_F(StateHandlerTest, SaveStateRefusedWithoutMatchingMachine)
{
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    EXPECT_CALL(*distributedCacheClient_, Set).Times(0);
    auto response = StateHandler::SaveState(grpc::PosixStreamIdentity{"instance_id", "rt-1", "req-gen-1"}, request);
    EXPECT_AWAIT_READY(response);
    EXPECT_NE(response.Get()->saversp().code(), common::ErrorCode::ERR_NONE);
    EXPECT_THAT(response.Get()->saversp().message(),
                testing::HasSubstr("stream runtime superseded or unknown"));
}

/**
 * Feature: LoadStateFailed
 * Description: load state failed
 * Steps:
 * 1. load state with empty instance id
 * 2. load state with empty checkpoint id
 * 3. load state get from cache failed
 *
 * Expectation:
 * 1. StatusCode::ERR_PARAM_INVALID
 * 2. StatusCode::ERR_PARAM_INVALID
 * 3. StatusCode::ERR_INNER_SYSTEM_ERROR
 */
TEST_F(StateHandlerTest, LoadStateFailed)
{
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    auto response = StateHandler::LoadState(grpc::PosixStreamIdentity{}, request);
    EXPECT_AWAIT_READY(response);
    EXPECT_EQ(response.Get()->loadrsp().code(), common::ErrorCode::ERR_PARAM_INVALID);
    EXPECT_EQ(response.Get()->loadrsp().message(), "load state failed: empty instance id");

    StateLoadRequest loadReq{};
    loadReq.set_checkpointid("");
    *request->mutable_loadreq() = std::move(loadReq);
    response = StateHandler::LoadState(grpc::PosixStreamIdentity{"instance_id", "rt-1", "req-gen-1"}, request);
    EXPECT_AWAIT_READY(response);
    EXPECT_EQ(response.Get()->loadrsp().code(), common::ErrorCode::ERR_PARAM_INVALID);
    EXPECT_EQ(response.Get()->loadrsp().message(), "load state failed: empty checkpoint id");

    std::string state = "";
    EXPECT_CALL(*distributedCacheClient_,
                Get(Matcher<const std::string &>(StateClient::ComposeStateKey("instance_id", "req-gen-1")), Matcher<std::string &>(Eq(""))))
        .WillOnce(DoAll(SetArgReferee<1>(state), Return(Status(StatusCode::FAILED))));

    loadReq.set_checkpointid(StateClient::ComposeStateKey("instance_id", "req-gen-1"));
    *request->mutable_loadreq() = std::move(loadReq);
    response = StateHandler::LoadState(grpc::PosixStreamIdentity{"instance_id", "rt-1", "req-gen-1"}, request);
    EXPECT_AWAIT_READY(response);
    EXPECT_EQ(response.Get()->loadrsp().code(), common::ErrorCode::ERR_INNER_SYSTEM_ERROR);
    EXPECT_THAT(response.Get()->loadrsp().message(), testing::HasSubstr("load state failed: [code: -1"));
}

/**
 * Feature: LoadStateSuccess
 * Description: load state success
 * Steps:
 * 1. load state
 *
 * Expectation:
 * 1. StatusCode::ERR_NONE
 */
TEST_F(StateHandlerTest, LoadStateSuccess)
{
    std::string state = "state";
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    EXPECT_CALL(*distributedCacheClient_,
                Get(Matcher<const std::string &>(StateClient::ComposeStateKey("instance_id", "req-gen-1")), Matcher<std::string &>(Eq(""))))
        .WillOnce(DoAll(SetArgReferee<1>(state), Return(Status::OK())));

    StateLoadRequest loadReq{};
    loadReq.set_checkpointid(StateClient::ComposeStateKey("instance_id", "req-gen-1"));
    *request->mutable_loadreq() = std::move(loadReq);
    auto response = StateHandler::LoadState(grpc::PosixStreamIdentity{"instance_id", "rt-1", "req-gen-1"}, request);
    EXPECT_AWAIT_READY(response);
    EXPECT_EQ(response.Get()->loadrsp().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(response.Get()->loadrsp().state(), state);
}

/**
 * Feature: SaveStateFailed
 * Description: save state failed
 * Steps:
 * 1. clear aid
 * 2. save state
 *
 * Expectation:
 * 1. StatusCode::ERR_INNER_SYSTEM_ERROR
 */
TEST_F(StateHandlerTest, SaveStateFailedWithInValidAID)
{
    StateHandlerHelper::ClearStateActorHelper();
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    auto response = StateHandler::SaveState(grpc::PosixStreamIdentity{"instance_id", "rt-1", "req-gen-1"}, request);

    EXPECT_AWAIT_READY(response);
    EXPECT_EQ(response.Get()->saversp().code(), common::ErrorCode::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(response.Get()->saversp().message(), "save state failed: don't init state actor");
}

/**
 * Feature: LoadStateFailed
 * Description: load state failed
 * Steps:
 * 1. clear aid
 * 2. save state
 *
 * Expectation:
 * 1. StatusCode::ERR_INNER_SYSTEM_ERROR
 */
TEST_F(StateHandlerTest, LoadStateFailedWithInValidAID)
{
    StateHandlerHelper::ClearStateActorHelper();
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    auto response = StateHandler::LoadState(grpc::PosixStreamIdentity{"instance_id", "rt-1", "req-gen-1"}, request);

    EXPECT_AWAIT_READY(response);
    EXPECT_EQ(response.Get()->saversp().code(), common::ErrorCode::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(response.Get()->saversp().message(), "save state failed: don't init state actor");
}

/**
 * Feature: SaveStateSuccess
 * Description: save state success
 * Steps:
 * 1. save state
 *
 * Expectation:
 * 1. StatusCode::ERR_NONE
 */
TEST_F(StateHandlerTest, DeleteStateSuccess)
{
    EXPECT_CALL(*distributedCacheClient_, Del(Matcher<const std::string &>(
        StateClient::ComposeStateKey("DeleteStateSuccess_instance_id", "req-gen"))))
            .WillOnce(Return(Status::OK()));

    auto status = StateHandler::DeleteState("DeleteStateSuccess_instance_id", "req-gen", "rt-1");
    EXPECT_TRUE(status.Get().IsOk());
}

/**
 * Feature: DeleteStateFailed
 * Description: delete state failed
 * Steps:
 * 1. delete state with empty instance id
 * 2. delete state from cache failed
 *
 * Expectation:
 * 1. StatusCode::ERR_INSTANCE_INFO_INVALID
 * 2. StatusCode::FAILED
 */
TEST_F(StateHandlerTest, DeleteStateFailed)
{
    auto status = StateHandler::DeleteState("", "req-gen", "rt-1");
    EXPECT_AWAIT_READY(status);
    EXPECT_FALSE(status.Get());

    EXPECT_CALL(*distributedCacheClient_, Del(Matcher<const std::string &>(
        StateClient::ComposeStateKey("DeleteStateFailed_instance_id", "req-gen"))))
    .WillOnce(Return(Status(StatusCode::FAILED, "something was wrong")));

    status = StateHandler::DeleteState("DeleteStateFailed_instance_id", "req-gen", "rt-1");
    EXPECT_AWAIT_READY(status);
    EXPECT_EQ(status.Get().StatusCode(), StatusCode::FAILED);

}


/**
 * Feature: DeleteStateFailed
 * Description: delete state failed
 * Steps:
 * 1. clear aid
 * 2. delete state
 *
 * Expectation:
 * 1. StatusCode::ERR_INNER_SYSTEM_ERROR
 */
    TEST_F(StateHandlerTest, DeleteStateFailedWithInValidAID)
    {
        StateHandlerHelper::ClearStateActorHelper();

        auto status = StateHandler::DeleteState("instance_id", "req-gen", "rt-1");

        EXPECT_AWAIT_READY(status);
        EXPECT_EQ(status.Get().StatusCode(), StatusCode::ERR_INNER_SYSTEM_ERROR);
    }


// Root-directed 2026-10-08: StateClient::Del forwards the STORE status
// verbatim (ds_cache_client_impl.cpp:86 — no NOT_FOUND→OK mapping). The
// tests below assert the REAL contract: whatever the store answers is
// what the kill chain sees. Any not-found→OK normalization, if wanted,
// belongs in the store layer and must be tested there with its real
// error codes — not fabricated here.

// A not-found error from the store propagates UNCHANGED (verbatim
// forwarding): the kill chain sees the store's own verdict.
TEST_F(StateHandlerTest, DeleteStateForwardsStoreNotFoundVerbatim)
{
    const Status notFound(StatusCode::ERR_ETCD_OPERATION_ERROR, "key not exist");
    EXPECT_CALL(*distributedCacheClient_, Del(Matcher<const std::string &>(
        StateClient::ComposeStateKey("absent-instance", "req-gen"))))
        .WillOnce(Return(notFound));
    auto del = StateHandler::DeleteState("absent-instance", "req-gen", "rt-1");
    ASSERT_AWAIT_READY(del);
    EXPECT_TRUE(del.Get().IsError());
    EXPECT_EQ(del.Get().StatusCode(), notFound.StatusCode());
    EXPECT_EQ(del.Get().GetMessage(), notFound.GetMessage());
}

// A genuine store failure propagates (the kill chain must retry).
TEST_F(StateHandlerTest, DeleteStateStoreFailurePropagates)
{
    const Status storeFailure(StatusCode::ERR_INNER_SYSTEM_ERROR, "store down");
    EXPECT_CALL(*distributedCacheClient_, Del(Matcher<const std::string &>(
        StateClient::ComposeStateKey("failing-instance", "req-gen"))))
        .WillOnce(Return(storeFailure));
    auto del = StateHandler::DeleteState("failing-instance", "req-gen", "rt-1");
    ASSERT_AWAIT_READY(del);
    EXPECT_TRUE(del.Get().IsError());
    // EXACT propagation against the input Status object's own fields.
    EXPECT_EQ(del.Get().StatusCode(), storeFailure.StatusCode());
    EXPECT_EQ(del.Get().GetMessage(), storeFailure.GetMessage());
}

// Positive: a successful Del propagates OK end-to-end through the bound
// actor.
TEST_F(StateHandlerTest, DeleteStateSuccessPropagates)
{
    EXPECT_CALL(*distributedCacheClient_, Del(Matcher<const std::string &>(
        StateClient::ComposeStateKey("present-instance", "req-gen"))))
        .WillOnce(Return(Status::OK()));
    auto del = StateHandler::DeleteState("present-instance", "req-gen", "rt-1");
    ASSERT_AWAIT_READY(del);
    EXPECT_TRUE(del.Get().IsOk()) << del.Get().ToString();
}

}  // namespace functionsystem::function_proxy::test