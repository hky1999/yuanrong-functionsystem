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

// Real-call-path tests for the 2026-10 fix chain: a MISSING runtime whose
// stop carries the COMPLETE expected identity must route to the
// durable-receipt replay (DeleteIfGeneration) against a REAL in-process
// gRPC sandboxd — matched receipt confirms, mismatched/missing stays
// UNKNOWN with no proof. These exercise SandboxdExecutor::StopInstanceWith
// Outcome end-to-end (executor selection, replay dispatch, receipt
// verification), not the helpers.

#include "runtime_manager/executor/sandboxd/sandboxd_executor.h"
#include "common/status/status.h"
#include "runtime_manager/port/port_manager.h"
#include "utils/future_test_helper.h"

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

using namespace functionsystem::runtime_manager;

namespace functionsystem::test {
namespace {

// A controllable in-process sandboxd: the test decides what the durable
// DeleteIfGeneration receipt says BEFORE the stop is issued.
class ControllableSandboxd final : public runtime::v1::SandboxService::Service {
public:
    grpc::Status DeleteIfGeneration(grpc::ServerContext *,
                                    const runtime::v1::DeleteIfGenerationRequest *req,
                                    runtime::v1::DeleteIfGenerationResponse *rsp) override
    {
        deleteCalls.push_back(req->expected_generation());
        if (failNextDelete) {
            failNextDelete = false;
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "no such sandbox");
        }
        if (req->expected_generation() == retiredGeneration) {
            rsp->set_retired_generation(retiredGeneration);
            return grpc::Status::OK;
        }
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "generation mismatch");
    }

    grpc::Status List(grpc::ServerContext *, const runtime::v1::ListSandboxesRequest *,
                      runtime::v1::ListSandboxesResponse *rsp) override
    {
        (void)rsp;
        return grpc::Status::OK;
    }

    std::vector<std::string> deleteCalls;
    std::string retiredGeneration;
    std::atomic<bool> failNextDelete{false};
};

class ReplayRoutingTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        service_ = std::make_unique<ControllableSandboxd>();
        builder_.RegisterService(service_.get());
        int port = 0;
        builder_.AddListeningPort("127.0.0.1:0",
                                  grpc::InsecureServerCredentials(), &port);
        server_ = builder_.BuildAndStart();
        ASSERT_NE(server_, nullptr);
        endpoint_ = "127.0.0.1:" + std::to_string(port);
        executor_ = std::make_unique<SandboxdExecutor>(
            "replay-test", litebus::AID(), "/tmp/replay-test-state");
        executor_->sandboxd_ =
            functionsystem::GrpcClient<runtime::v1::SandboxService>::
                CreateGrpcClient(endpoint_).release();
        ASSERT_NE(executor_->sandboxd_, nullptr);
    }

    void TearDown() override
    {
        if (server_) { server_->Shutdown(); }
    }

    std::unique_ptr<ControllableSandboxd> service_;
    grpc::ServerBuilder builder_;
    std::unique_ptr<grpc::Server> server_;
    std::string endpoint_;
    std::unique_ptr<SandboxdExecutor> executor_;
};

}  // namespace

// The exact a1-0 shape: local runtime state is ALREADY GONE (fast create,
// killed, map cleared), the kill carries the COMPLETE expected identity,
// and the durable receipt names the SAME generation -> the stop MUST
// dispatch DeleteIfGeneration (never the RUNTIME executor's
// can-not-find-pid dead end) and the matched receipt confirms the proof.
TEST_F(ReplayRoutingTest, MissingRuntimeWithCompleteIdentityReplaysDurableReceipt)
{
    service_->retiredGeneration = "gen-a1-0";
    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("runtime-a1-0");
    request->set_requestid("stop-a1-0");
    request->set_expected_sandbox_id("sbox-a1-0");
    request->set_expected_resource_generation("gen-a1-0");
    // NOTE: no stateManager_.Register — the runtime map is empty.

    auto outcome = executor_->StopInstanceWithOutcome(request, false);
    ASSERT_AWAIT_READY(outcome);
    ASSERT_EQ(service_->deleteCalls.size(), 1u)
        << "missing runtime + complete identity must dispatch DeleteIfGeneration";
    EXPECT_EQ(service_->deleteCalls[0], "gen-a1-0");
    EXPECT_TRUE(outcome.Get().status.IsOk()) << outcome.Get().status.GetMessage();
    ASSERT_TRUE(outcome.Get().retirement.confirmed);
    EXPECT_EQ(outcome.Get().retirement.sandboxID, "sbox-a1-0");
    EXPECT_EQ(outcome.Get().retirement.resourceGeneration, "gen-a1-0");
}

// Durable receipt names a DIFFERENT generation: no proof, UNKNOWN, credit
// retained — a retry may reconcile later.
TEST_F(ReplayRoutingTest, ReplayWithMismatchedGenerationStaysUnknown)
{
    service_->retiredGeneration = "gen-somebody-else";
    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("runtime-mismatch");
    request->set_requestid("stop-mismatch");
    request->set_expected_sandbox_id("sbox-mismatch");
    request->set_expected_resource_generation("gen-mismatch");
    auto outcome = executor_->StopInstanceWithOutcome(request, false);
    ASSERT_AWAIT_READY(outcome);
    ASSERT_EQ(service_->deleteCalls.size(), 1u);
    EXPECT_TRUE(outcome.Get().status.IsError());
    EXPECT_FALSE(outcome.Get().retirement.confirmed);
}

// Transport failure (NOT_FOUND from the daemon): UNKNOWN, no proof.
TEST_F(ReplayRoutingTest, ReplayTransportFailureStaysUnknown)
{
    service_->retiredGeneration = "gen-unreachable";
    service_->failNextDelete = true;
    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("runtime-unreachable");
    request->set_requestid("stop-unreachable");
    request->set_expected_sandbox_id("sbox-unreachable");
    request->set_expected_resource_generation("gen-unreachable");
    auto outcome = executor_->StopInstanceWithOutcome(request, false);
    ASSERT_AWAIT_READY(outcome);
    EXPECT_TRUE(outcome.Get().status.IsError());
    EXPECT_FALSE(outcome.Get().retirement.confirmed);
}

}  // namespace functionsystem::test
