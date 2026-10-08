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

// REAL-entry coverage for the 2026-10 fix chain, driving the production
// RuntimeManager::StopInstance message handler through the established
// capability fixture (spawned actors + serialized messages + a
// response-capturing test actor):
//  - a COMPLETE expected identity with an EMPTY runtime map must be routed
//    to the SANDBOXD executor probe (the durable-receipt replay path), and
//    the RUNTIME executor probe must see NOTHING;
//  - a one-sided identity must be refused before ANY dispatch.
// The probes implement the full ExecutorProxy pure interface and override
// the methods the entry actually calls (StopInstanceWithOutcome /
// StopInstance).

#include "runtime_manager/manager/runtime_manager.h"
#include "runtime_manager/executor/executor.h"
#include "common/status/status.h"
#include "utils/future_test_helper.h"
#include "runtime_manager_test_actor.h"

#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace functionsystem;
using namespace functionsystem::runtime_manager;

namespace functionsystem::test {
namespace {

std::string GenerateRandomEntryName(const std::string &prefix)
{
    static std::atomic<uint64_t> counter{0};
    return prefix + std::to_string(::getpid()) + "-"
        + std::to_string(counter.fetch_add(1));
}

// Full-interface probe: records every identity-bearing dispatch.
class EntryRoutingProbe : public ExecutorProxy {
public:
    explicit EntryRoutingProbe(EXECUTOR_TYPE type)
        : ExecutorProxy(nullptr), type_(type) {}

    EXECUTOR_TYPE Type() const { return type_; }

    litebus::Future<::messages::StartInstanceResponse> StartInstance(
        const std::shared_ptr<messages::StartInstanceRequest> &,
        const std::vector<int> &) override
    {
        return litebus::Future<::messages::StartInstanceResponse>(
            ::messages::StartInstanceResponse{});
    }

    litebus::Future<Status> StopInstance(
        const std::shared_ptr<messages::StopInstanceRequest> &request,
        bool) override
    {
        RecordDispatch(request);
        return Status(StatusCode::ERR_INSTANCE_BUSY, "probe stop");
    }

    litebus::Future<StopInstanceOutcome> StopInstanceWithOutcome(
        const std::shared_ptr<messages::StopInstanceRequest> &request,
        bool) override
    {
        RecordDispatch(request);
        return StopInstanceOutcome{
            Status(StatusCode::ERR_INSTANCE_BUSY, "probe stop"),
            PhysicalRetirementProof{}};
    }

    litebus::Future<messages::SnapshotRuntimeResponse> SnapshotRuntime(
        const std::shared_ptr<messages::SnapshotRuntimeRequest> &) override
    {
        return litebus::Future<messages::SnapshotRuntimeResponse>(
            messages::SnapshotRuntimeResponse{});
    }

    litebus::Future<std::map<std::string, messages::RuntimeInstanceInfo>>
    GetRuntimeInstanceInfos() override
    {
        return std::map<std::string, messages::RuntimeInstanceInfo>{};
    }

    // Production StopInstance calls this FIRST; the base implementation
    // dereferences the wrapped executor (null here). A replayed retirement
    // legitimately has no exec — answer a READY empty future, mirroring
    // the unregistered-state contract.
    litebus::Future<std::shared_ptr<litebus::Exec>> GetExecByRuntimeID(
        const std::string &) override
    {
        return std::shared_ptr<litebus::Exec>{};
    }

    void UpdatePrestartRuntimePromise(pid_t) override {}
    void ClearCapability() override {}
    litebus::Future<bool> GracefulShutdown() override { return false; }
    litebus::Future<messages::ReconcileRuntimesResponse> ReconcileRuntimes(
        const std::shared_ptr<messages::ReconcileRuntimesRequest> &) override
    {
        return litebus::Future<messages::ReconcileRuntimesResponse>(
            messages::ReconcileRuntimesResponse{});
    }

    void RecordDispatch(const std::shared_ptr<messages::StopInstanceRequest> &request)
    {
        lastRuntimeID = request->runtimeid();
        lastExpectedSandbox = request->expected_sandbox_id();
        lastExpectedGeneration = request->expected_resource_generation();
        dispatchCount++;
    }

    EXECUTOR_TYPE type_;
    std::atomic<int> dispatchCount{0};
    std::string lastRuntimeID;
    std::string lastExpectedSandbox;
    std::string lastExpectedGeneration;
};

class StopEntryRoutingFixture : public ::testing::Test {
protected:
    void SetUp() override
    {
        manager_ = std::make_shared<RuntimeManager>(GenerateRandomEntryName("StopEntryRM"));
        // The ctor already builds healthCheckClient_ (its actor is spawned
        // inside HealthCheck) — the existing capability fixture relies on
        // the same member, so no extra init is required here.
        manager_->isUnitTestSituation_ = true;
        litebus::Spawn(manager_, true);
        manager_->connected_ = true;
        testActor_ = std::make_shared<RuntimeManagerTestActor>(
            GenerateRandomEntryName("StopEntryActor"));
        litebus::Spawn(testActor_, true);

        sandboxdProbe_ = std::make_shared<EntryRoutingProbe>(EXECUTOR_TYPE::SANDBOXD);
        runtimeProbe_ = std::make_shared<EntryRoutingProbe>(EXECUTOR_TYPE::RUNTIME);
        manager_->executorMap_[EXECUTOR_TYPE::SANDBOXD] = sandboxdProbe_;
        manager_->executorMap_[EXECUTOR_TYPE::RUNTIME] = runtimeProbe_;
    }

    void TearDown() override
    {
        litebus::Terminate(testActor_->GetAID());
        litebus::Await(testActor_->GetAID());
        litebus::Terminate(manager_->GetAID());
        litebus::Await(manager_);
    }

    std::shared_ptr<RuntimeManager> manager_;
    std::shared_ptr<RuntimeManagerTestActor> testActor_;
    std::shared_ptr<EntryRoutingProbe> sandboxdProbe_;
    std::shared_ptr<EntryRoutingProbe> runtimeProbe_;
};

}  // namespace

// a1-0 retry shape THROUGH THE REAL ENTRY: the runtime map is empty, the
// serialized StopInstanceRequest carries the COMPLETE expected identity —
// the production handler must hand it to the SANDBOXD executor (replay
// owner), never the RUNTIME executor's can-not-find-pid path.
TEST_F(StopEntryRoutingFixture, CompleteIdentityRoutesToSandboxdViaRealEntry)
{
    messages::StopInstanceRequest request;
    request.set_runtimeid("runtime-ghost");
    request.set_requestid("stop-ghost");
    request.set_traceid("trace-ghost");
    request.set_expected_sandbox_id("sbox-ghost");
    request.set_expected_resource_generation("gen-ghost");

    testActor_->StopInstance(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([this]() {
        return testActor_->GetIsReceiveStopInstanceResponse();
    });
    // The probe answers BUSY: the assertion is WHERE the dispatch went,
    // not the synthetic status.
    EXPECT_EQ(sandboxdProbe_->dispatchCount.load(), 1)
        << "identity-bearing stop must reach the SANDBOXD executor";
    EXPECT_EQ(sandboxdProbe_->lastRuntimeID, "runtime-ghost");
    EXPECT_EQ(sandboxdProbe_->lastExpectedSandbox, "sbox-ghost");
    EXPECT_EQ(sandboxdProbe_->lastExpectedGeneration, "gen-ghost");
    EXPECT_EQ(runtimeProbe_->dispatchCount.load(), 0)
        << "the RUNTIME executor must never see an identity-bearing stop";
}

// One-sided identity: the production entry refuses BEFORE dispatch —
// neither executor sees anything.
TEST_F(StopEntryRoutingFixture, OneSidedIdentityRefusedBeforeAnyDispatch)
{
    messages::StopInstanceRequest request;
    request.set_runtimeid("runtime-one-sided");
    request.set_requestid("stop-one-sided");
    request.set_traceid("trace-one-sided");
    request.set_expected_sandbox_id("sbox-one-sided");  // generation missing

    testActor_->StopInstance(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([this]() {
        return testActor_->GetIsReceiveStopInstanceResponse();
    });
    const auto response = testActor_->GetStopInstanceResponse();
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(sandboxdProbe_->dispatchCount.load(), 0);
    EXPECT_EQ(runtimeProbe_->dispatchCount.load(), 0);
    EXPECT_NE(response->code(), static_cast<int32_t>(0));
}

}  // namespace functionsystem::test
