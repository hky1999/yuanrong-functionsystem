/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "runtime_manager/executor/sandboxd/sandboxd_executor.h"
#include "common/status/status.h"
#include "runtime_manager/port/port_manager.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

using namespace functionsystem::runtime_manager;

namespace functionsystem::test {

// ── SandboxdExecutor static helpers ───────────────────────────────────────────

TEST(SandboxdExecutorTest, ParseForwardPortsParsesPortForwardings)
{
    const std::string netJson = R"({"portForwardings":[{"port":8080},{"port":9090,"protocol":"TCP"}]})";
    auto configs = SandboxdExecutor::ParseForwardPorts(netJson);
    ASSERT_EQ(configs.size(), 2u);
    EXPECT_EQ(configs[0].containerPort, 8080u);
    EXPECT_EQ(configs[0].protocol, "tcp");
    EXPECT_EQ(configs[1].containerPort, 9090u);
    EXPECT_EQ(configs[1].protocol, "tcp");  // lowercased
}

TEST(SandboxdExecutorTest, ParseForwardPortsParsesRouteKinds)
{
    const std::string netJson = R"({"portForwardings":[)"
        R"({"port":50090,"protocol":"http","routeKind":"direct"},)"
        R"({"port":8765,"protocol":"http","routeKind":"tunnel"},)"
        R"({"port":8080,"protocol":"http"}]})";
    auto configs = SandboxdExecutor::ParseForwardPorts(netJson);
    ASSERT_EQ(configs.size(), 3);
    EXPECT_EQ(configs[0].routeKind, PortRouteKind::DIRECT);
    EXPECT_EQ(configs[1].routeKind, PortRouteKind::TUNNEL);
    EXPECT_EQ(configs[2].routeKind, PortRouteKind::PUBLIC);
}

TEST(SandboxdExecutorTest, ParseForwardPortsInvalidRouteKindIsSkipped)
{
    const std::string netJson = R"({"portForwardings":[)"
        R"({"port":8080,"routeKind":"unknown"},)"
        R"({"port":8081,"routeKind":42},)"
        R"({"port":9090,"routeKind":"public"}]})";
    const auto configs = SandboxdExecutor::ParseForwardPorts(netJson);
    ASSERT_EQ(configs.size(), 1u);
    EXPECT_EQ(configs[0].containerPort, 9090u);
    EXPECT_EQ(configs[0].routeKind, PortRouteKind::PUBLIC);
}

TEST(SandboxdExecutorTest, ParseForwardPortsEmptyOrInvalidReturnsEmpty)
{
    EXPECT_TRUE(SandboxdExecutor::ParseForwardPorts("").empty());
    EXPECT_TRUE(SandboxdExecutor::ParseForwardPorts("not-json").empty());
    EXPECT_TRUE(SandboxdExecutor::ParseForwardPorts(R"({"portForwardings":[]})").empty());
    // Out-of-range ports are skipped.
    auto configs = SandboxdExecutor::ParseForwardPorts(R"({"portForwardings":[{"port":0},{"port":70000}]})");
    EXPECT_TRUE(configs.empty());
}

TEST(SandboxdExecutorTest, IsRetryableWaitErrorClassifiesTransportErrors)
{
    EXPECT_TRUE(SandboxdExecutor::IsRetryableWaitError(Status(GRPC_UNAVAILABLE)));
    EXPECT_TRUE(SandboxdExecutor::IsRetryableWaitError(Status(GRPC_DEADLINE_EXCEEDED)));
    EXPECT_FALSE(SandboxdExecutor::IsRetryableWaitError(Status(GRPC_NOT_FOUND)));
    EXPECT_FALSE(SandboxdExecutor::IsRetryableWaitError(Status(SUCCESS)));
}

TEST(SandboxdExecutorTest, SandboxReclaimBackoffIsExponentialAndCapped)
{
    EXPECT_EQ(SandboxdExecutor::SandboxReclaimBackoffMs(1), 1000u);
    EXPECT_EQ(SandboxdExecutor::SandboxReclaimBackoffMs(2), 2000u);
    EXPECT_EQ(SandboxdExecutor::SandboxReclaimBackoffMs(6), 32000u);
    EXPECT_EQ(SandboxdExecutor::SandboxReclaimBackoffMs(7), 60000u);
    EXPECT_EQ(SandboxdExecutor::SandboxReclaimBackoffMs(100), 60000u);
}

TEST(SandboxdExecutorTest, FailedLocalReclaimKeepsRecoveredPortsUntilDeleteSucceeds)
{
    struct PortManagerReset {
        ~PortManagerReset()
        {
            PortManager::GetInstance().Clear();
        }
    } reset;

    int firstPort = -1;
    for (int candidate = 20000; candidate <= 65534; ++candidate) {
        if (!PortManager::GetInstance().CheckPortInUse(candidate) &&
            !PortManager::GetInstance().CheckPortInUse(candidate + 1)) {
            firstPort = candidate;
            break;
        }
    }
    ASSERT_GT(firstPort, 0);

    const std::string runtimeID = "recovered-runtime";
    const std::string sandboxID = "recovered-sandbox";
    auto &portManager = PortManager::GetInstance();
    portManager.InitPortResource(firstPort, 2);
    ASSERT_TRUE(portManager.ReservePorts(runtimeID, {firstPort, firstPort + 1}).IsOk());

    auto healthCheck = std::make_shared<HealthCheck>("recovered-port-release-health-check");
    SandboxdExecutor executor("recovered-port-release-executor", litebus::AID(),
                              "/tmp/recovered-port-release-checkpoints");
    executor.SetHealthCheckClient(healthCheck);

    messages::RuntimeInstanceInfo instanceInfo;
    instanceInfo.set_instanceid("recovered-instance");
    instanceInfo.set_runtimeid(runtimeID);
    const std::string portMappings = "[\"public+http:" + std::to_string(firstPort) +
                                     ":8080\",\"public+http:" + std::to_string(firstPort + 1) + ":8081\"]";
    executor.stateManager_.Register({runtimeID, sandboxID, {}, portMappings, instanceInfo});

    (void)executor.CleanupSandboxAfterMaxRetries(runtimeID, sandboxID);

    ASSERT_TRUE(executor.stateManager_.IsActive(runtimeID));
    EXPECT_EQ(executor.stateManager_.GetSandboxID(runtimeID), sandboxID);
    ASSERT_EQ(executor.sandboxLifecycleStates_.count(runtimeID), 1u);
    EXPECT_EQ(executor.sandboxLifecycleStates_.at(runtimeID), SandboxdExecutor::SandboxLifecycleStatus::ABNORMAL);
    ASSERT_EQ(executor.sandboxReclaims_.count(runtimeID), 1u);
    EXPECT_TRUE(portManager.ReservePorts("other-runtime", {firstPort}).IsError());
    EXPECT_TRUE(portManager.ReservePorts("other-runtime", {firstPort + 1}).IsError());

    auto failedDelete = executor.OnReclaimSandboxDone(
        runtimeID, sandboxID, "delete-request",
        litebus::Try<runtime::v1::DeleteResponse>(static_cast<int32_t>(GRPC_UNAVAILABLE)));
    ASSERT_TRUE(failedDelete.Get().IsError());
    ASSERT_EQ(executor.sandboxReclaims_.count(runtimeID), 1u);
    EXPECT_EQ(executor.sandboxReclaims_.at(runtimeID).failedAttempts, 1u);
    EXPECT_TRUE(executor.stateManager_.IsActive(runtimeID));
    EXPECT_TRUE(portManager.ReservePorts("other-runtime", {firstPort}).IsError());
    EXPECT_TRUE(portManager.ReservePorts("other-runtime", {firstPort + 1}).IsError());

    auto successfulDelete = executor.OnReclaimSandboxDone(
        runtimeID, sandboxID, "delete-request",
        litebus::Try<runtime::v1::DeleteResponse>(runtime::v1::DeleteResponse{}));
    ASSERT_TRUE(successfulDelete.Get().IsOk());
    EXPECT_FALSE(executor.stateManager_.IsActive(runtimeID));
    EXPECT_EQ(executor.sandboxLifecycleStates_.count(runtimeID), 0u);
    EXPECT_EQ(executor.sandboxReclaims_.count(runtimeID), 0u);
    EXPECT_TRUE(portManager.GetPort(runtimeID).empty());
    EXPECT_EQ(portManager.RequestPorts("next-runtime", 2), (std::vector<int>{firstPort, firstPort + 1}));
}

}  // namespace functionsystem::test
