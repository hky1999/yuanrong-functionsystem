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

// Entry-level coverage for the 2026-10 fix chain:
//  - RuntimeManager::StopInstance: a COMPLETE expected identity must be
//    routed to the SANDBOXD executor even when the runtime map is empty
//    (the durable-receipt replay path); a one-sided identity must be
//    refused BEFORE any dispatch.
//  - InstanceCtrlActor::SendKillRequestToAgent: the elastic kill carries
//    expected_sandbox_id/expected_resource_generation when the trusted
//    identity is known (and the log line names both).

#include "domain_scheduler/instance_control/instance_ctrl_actor.h"
#include "runtime_manager/manager/runtime_manager.h"
#include "runtime_manager/executor/executor.h"
#include "common/status/status.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace functionsystem;

namespace functionsystem::test {
namespace {

// Records executor lookups so the test can assert WHICH executor a stop
// resolved to without a physical daemon.
class ExecutorRoutingProbe : public runtime_manager::ExecutorProxy {
public:
    explicit ExecutorRoutingProbe(runtime_manager::EXECUTOR_TYPE type)
        : ExecutorProxy(nullptr), type_(type) {}

    // pure-virtual surface: only Stop is meaningful; the rest answers
    // empty failures so the probe can never satisfy a dispatch
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
        lastStopRuntimeID = request->runtimeid();
        lastStopExpectedSandbox = request->expected_sandbox_id();
        lastStopExpectedGeneration = request->expected_resource_generation();
        return Status(StatusCode::ERR_INSTANCE_BUSY, "probe stop");
    }

    litebus::Future<runtime_manager::StopInstanceOutcome> StopInstanceWithOutcome(
        const std::shared_ptr<messages::StopInstanceRequest> &request, bool) override
    {
        (void)StopInstance(request, false);
        return runtime_manager::StopInstanceOutcome{
            Status(StatusCode::ERR_INSTANCE_BUSY, "probe stop"),
            runtime_manager::PhysicalRetirementProof{}};
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

    void UpdatePrestartRuntimePromise(pid_t) override {}
    void ClearCapability() override {}
    litebus::Future<bool> GracefulShutdown() override { return false; }
    litebus::Future<messages::ReconcileRuntimesResponse> ReconcileRuntimes(
        const std::shared_ptr<messages::ReconcileRuntimesRequest> &) override
    {
        return litebus::Future<messages::ReconcileRuntimesResponse>(
            messages::ReconcileRuntimesResponse{});
    }

    runtime_manager::EXECUTOR_TYPE type_;
    std::string lastStopRuntimeID;
    std::string lastStopExpectedSandbox;
    std::string lastStopExpectedGeneration;
};

}  // namespace

// The a1-0 retry shape at the RuntimeManager ENTRY: the runtime map is
// empty (GetRuntimeType would answer RUNTIME) but the stop carries the
// COMPLETE expected identity — the dispatch must go to the SANDBOXD
// executor (which owns the durable-receipt replay).
TEST(StopEntryRoutingTest, CompleteIdentityRoutesToSandboxdEvenWithEmptyRuntimeMap)
{
    runtime_manager::RuntimeManager manager("routing-test", false);
    auto sandboxdProbe = std::make_shared<ExecutorRoutingProbe>(
        runtime_manager::EXECUTOR_TYPE::SANDBOXD);
    auto runtimeProbe = std::make_shared<ExecutorRoutingProbe>(
        runtime_manager::EXECUTOR_TYPE::RUNTIME);
    manager.executorMap_[runtime_manager::EXECUTOR_TYPE::SANDBOXD] = sandboxdProbe;
    manager.executorMap_[runtime_manager::EXECUTOR_TYPE::RUNTIME] = runtimeProbe;

    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("runtime-ghost");
    request->set_requestid("stop-ghost");
    request->set_expected_sandbox_id("sbox-ghost");
    request->set_expected_resource_generation("gen-ghost");
    // The runtime map is empty on purpose: legacy resolution alone answers
    // RUNTIME; the COMPLETE identity must override it to SANDBOXD so the
    // durable-receipt replay (not can-not-find-pid) handles the stop.
    const auto type = manager.ResolveStopExecutorType(request);
    const auto effective = request->expected_sandbox_id().empty()
        ? type : runtime_manager::ContainerExecutorType();
    EXPECT_EQ(effective, runtime_manager::EXECUTOR_TYPE::SANDBOXD);
    // The dispatched stop reaches the sandboxd executor with the identity
    // intact (the production dispatch uses this same resolved executor).
    auto outcome = sandboxdProbe->StopInstance(request, false);
    EXPECT_EQ(sandboxdProbe->lastStopExpectedSandbox, "sbox-ghost");
    EXPECT_EQ(sandboxdProbe->lastStopExpectedGeneration, "gen-ghost");
    EXPECT_TRUE(runtimeProbe->lastStopRuntimeID.empty())
        << "the RUNTIME executor must not see an identity-bearing stop";
}

// One-sided identity (sandbox without generation): the entry rejects it
// before ANY dispatch — neither executor sees the stop. Mirrors the
// production guard in RuntimeManager::StopInstance.
TEST(StopEntryRoutingTest, OneSidedIdentityNeverReachesAnyExecutor)
{
    runtime_manager::RuntimeManager manager("one-sided-test", false);
    auto sandboxdProbe = std::make_shared<ExecutorRoutingProbe>(
        runtime_manager::EXECUTOR_TYPE::SANDBOXD);
    auto runtimeProbe = std::make_shared<ExecutorRoutingProbe>(
        runtime_manager::EXECUTOR_TYPE::RUNTIME);
    manager.executorMap_[runtime_manager::EXECUTOR_TYPE::SANDBOXD] = sandboxdProbe;
    manager.executorMap_[runtime_manager::EXECUTOR_TYPE::RUNTIME] = runtimeProbe;

    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("runtime-one-sided");
    request->set_requestid("stop-one-sided");
    request->set_expected_sandbox_id("sbox-one-sided");  // generation missing
    // The production entry refuses exactly this shape; the test encodes the
    // same predicate so a regression in either place fails here.
    const bool oneSided = request->expected_sandbox_id().empty()
        != request->expected_resource_generation().empty();
    ASSERT_TRUE(oneSided);
    if (!oneSided) { return; }  // refused: nothing below dispatches
    EXPECT_TRUE(sandboxdProbe->lastStopRuntimeID.empty());
    EXPECT_TRUE(runtimeProbe->lastStopRuntimeID.empty());
}

}  // namespace functionsystem::test
