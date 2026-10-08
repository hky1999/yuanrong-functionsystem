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
#include "utils/future_test_helper.h"

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
        litebus::Try<runtime::v1::DeleteResponse>(static_cast<int32_t>(GRPC_UNAVAILABLE)),
        executor.stateManager_.Find(runtimeID)->registrationID);
    ASSERT_TRUE(failedDelete.Get().IsError());
    ASSERT_EQ(executor.sandboxReclaims_.count(runtimeID), 1u);
    EXPECT_EQ(executor.sandboxReclaims_.at(runtimeID).failedAttempts, 1u);
    EXPECT_TRUE(executor.stateManager_.IsActive(runtimeID));
    EXPECT_TRUE(portManager.ReservePorts("other-runtime", {firstPort}).IsError());
    EXPECT_TRUE(portManager.ReservePorts("other-runtime", {firstPort + 1}).IsError());

    auto absentWithoutReceipt = executor.OnReclaimSandboxDone(
        runtimeID, sandboxID, "delete-request",
        litebus::Try<runtime::v1::DeleteResponse>(static_cast<int32_t>(GRPC_NOT_FOUND)),
        executor.stateManager_.Find(runtimeID)->registrationID);
    EXPECT_TRUE(absentWithoutReceipt.Get().IsError());
    EXPECT_TRUE(executor.stateManager_.IsActive(runtimeID));

    auto successfulDelete = executor.OnReclaimSandboxDone(
        runtimeID, sandboxID, "delete-request",
        litebus::Try<runtime::v1::DeleteResponse>(runtime::v1::DeleteResponse{}),
        executor.stateManager_.Find(runtimeID)->registrationID);
    ASSERT_TRUE(successfulDelete.Get().IsOk());
    EXPECT_FALSE(executor.stateManager_.IsActive(runtimeID));
    EXPECT_EQ(executor.sandboxLifecycleStates_.count(runtimeID), 0u);
    EXPECT_EQ(executor.sandboxReclaims_.count(runtimeID), 0u);
    EXPECT_TRUE(portManager.GetPort(runtimeID).empty());
    EXPECT_EQ(portManager.RequestPorts("next-runtime", 2), (std::vector<int>{firstPort, firstPort + 1}));
}

TEST(SandboxdExecutorTest, ResizeRejectsMissingOrForeignIdentityBeforeGrpcDispatch)
{
    SandboxdExecutor executor("resize-identity-executor", litebus::AID(), "/tmp/resize-identity-checkpoints");
    auto empty = executor.ResizeSandbox(nullptr).Get();
    EXPECT_EQ(empty.outcome(), messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH);

    messages::RuntimeInstanceInfo info;
    info.set_instanceid("owned-instance");
    info.set_runtimeid("owned-runtime");
    executor.stateManager_.Register({"owned-runtime", "owned-sandbox", {}, {}, info});
    executor.stateManager_.MarkStartDone("owned-runtime");
    auto request = std::make_shared<messages::ResizeSandboxRequest>();
    request->set_requestid("reservation");
    request->set_instanceid("foreign-instance");
    request->set_runtimeid("owned-runtime");
    request->mutable_resize()->set_sandbox_id("owned-sandbox");
    request->mutable_resize()->set_resource_generation("generation");
    request->mutable_resize()->set_deadline_unix_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count() + 15000);
    request->mutable_resize()->mutable_expected()->set_cpu_quota_usec(50000);
    request->mutable_resize()->mutable_expected()->set_cpu_period_usec(100000);
    request->mutable_resize()->mutable_expected()->set_memory_limit_bytes(512 * 1024 * 1024);
    *request->mutable_resize()->mutable_target() = request->resize().expected();
    request->mutable_resize()->mutable_target()->set_memory_limit_bytes(1024 * 1024 * 1024);
    const auto foreign = executor.ResizeSandbox(request).Get();
    EXPECT_EQ(foreign.outcome(), messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH);
    EXPECT_EQ(foreign.requestid(), "reservation");
    EXPECT_FALSE(foreign.has_result());
    EXPECT_NE(foreign.message().find("identity"), std::string::npos);

    request->set_instanceid("owned-instance");
    request->mutable_resize()->set_sandbox_id("foreign-sandbox");
    EXPECT_NE(executor.ResizeSandbox(request).Get().message().find("identity"), std::string::npos);
    request->mutable_resize()->set_sandbox_id("owned-sandbox");
    const auto noClient = executor.ResizeSandbox(request).Get();
    EXPECT_EQ(noClient.outcome(), messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH);
    EXPECT_NE(noClient.message().find("client unavailable"), std::string::npos);
}

TEST(SandboxdExecutorTest, OldDeleteCannotClearSameIDReplacement)
{
    SandboxdExecutor executor("delete-source-executor", litebus::AID(), "/tmp/delete-source-checkpoints");
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("instance");
    executor.stateManager_.Register({"runtime", "sandbox", {}, {}, info});
    const auto old = executor.stateManager_.Find("runtime")->registrationID;
    ASSERT_FALSE(old.empty());
    executor.stateManager_.Register({"runtime", "sandbox", {}, {}, info});
    const auto replacement = executor.stateManager_.Find("runtime")->registrationID;
    EXPECT_NE(old, replacement);
    auto result = executor.OnDeleteDone("runtime", "old-delete", "sandbox", runtime::v1::DeleteResponse{}, old);
    EXPECT_TRUE(result.Get().IsError());
    ASSERT_TRUE(executor.stateManager_.IsActive("runtime"));
    EXPECT_EQ(executor.stateManager_.Find("runtime")->registrationID, replacement);
    executor.sandboxReclaims_["runtime"] = {"sandbox", 0,
        std::make_shared<litebus::Promise<Status>>(), replacement};
    auto late = executor.OnReclaimSandboxDone("runtime", "sandbox", "old-delete",
        litebus::Try<runtime::v1::DeleteResponse>(runtime::v1::DeleteResponse{}), old);
    EXPECT_EQ(executor.sandboxReclaims_.at("runtime").registrationID, replacement);
    EXPECT_EQ(executor.stateManager_.Find("runtime")->registrationID, replacement);
    // An old timer must not issue Delete against the new registration (no gRPC client is bound).
    executor.ReclaimSandbox("runtime", "sandbox", "old-delete", old);
    EXPECT_EQ(executor.sandboxReclaims_.at("runtime").failedAttempts, 0);
}

TEST(SandboxdExecutorTest, BirthGenerationIsStoredAndReplacementChangesRegistration)
{
    RuntimeStateManager state;
    state.Register({"runtime", {}, {}, {}, messages::RuntimeInstanceInfo()});
    state.UpdateSandboxID("runtime", "sandbox", "physical-one");
    const auto first = state.Find("runtime");
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->resourceGeneration, "physical-one");
    state.UpdateSandboxID("runtime", "sandbox", "physical-one");
    EXPECT_EQ(state.Find("runtime")->registrationID, first->registrationID);
    state.UpdateSandboxID("runtime", "sandbox", "physical-two");
    EXPECT_EQ(state.Find("runtime")->resourceGeneration, "physical-two");
    EXPECT_NE(state.Find("runtime")->registrationID, first->registrationID);
    state.UpdateSandboxID("runtime", "other-sandbox");
    EXPECT_TRUE(state.Find("runtime")->resourceGeneration.empty());
}

TEST(SandboxdExecutorTest, ConditionalDeleteRejectsMissingBirthIdentityBeforeGrpc)
{
    SandboxdExecutor executor("delete-birth-identity", litebus::AID(), "/tmp/delete-birth-identity");
    executor.stateManager_.Register({"runtime", "sandbox", {}, {}, messages::RuntimeInstanceInfo()});
    auto command = std::make_shared<runtime::v1::DeleteRequest>();
    command->set_id("sandbox");
    const auto result = executor.DoDelete("instance", "runtime", "request", command);
    EXPECT_TRUE(result.IsError());
    EXPECT_TRUE(executor.stateManager_.IsActive("runtime"));
}

TEST(SandboxdExecutorTest, OrphanCleanupRequiresReceiptAndCannotClearReplacement)
{
    SandboxdExecutor executor("orphan-receipt", litebus::AID(), "/tmp/orphan-receipt");
    messages::RuntimeInstanceInfo info;
    executor.stateManager_.Register({"runtime", "sandbox", {}, {}, info});
    executor.stateManager_.UpdateSandboxID("runtime", "sandbox", "new-generation");
    const auto current = executor.stateManager_.Find("runtime");
    executor.orphanDeletes_["sandbox"] = {"old-generation", "runtime", "old-registration", "request", "ticket"};
    runtime::v1::DeleteIfGenerationResponse receipt;
    receipt.set_retired_generation("old-generation");
    EXPECT_TRUE(executor.OnDeleteSandboxComplete("sandbox", "older-ticket", receipt).IsError());
    EXPECT_EQ(executor.orphanDeletes_.count("sandbox"), 1);
    EXPECT_TRUE(executor.completedOrphanDeletes_.empty());
    EXPECT_TRUE(executor.OnDeleteSandboxComplete("sandbox", "ticket", receipt).IsOk());
    EXPECT_EQ(executor.stateManager_.Find("runtime")->registrationID, current->registrationID);
    EXPECT_EQ(executor.completedOrphanDeletes_.size(), 1);
    auto request = std::make_shared<messages::ReconcileRuntimesRequest>();
    auto actual = std::make_shared<runtime::v1::ListSandboxesResponse>();
    EXPECT_EQ(executor.OnReconcileRuntimes(request, actual).orphanscleaned(), 1);
    EXPECT_EQ(executor.OnReconcileRuntimes(request, actual).orphanscleaned(), 0);
    executor.orphanDeletes_["sandbox"] = {"new-generation", "runtime", current->registrationID, "request", "new-ticket"};
    EXPECT_TRUE(executor.OnDeleteSandboxComplete("sandbox", "new-ticket", receipt).IsError());
    EXPECT_TRUE(executor.completedOrphanDeletes_.empty());
    EXPECT_TRUE(executor.stateManager_.IsActive("runtime"));
    executor.DeleteSandboxAsync("sandbox", "", "request"); // missing generation cannot reach unbound gRPC
    EXPECT_TRUE(executor.orphanDeletes_.empty());
}

TEST(SandboxdExecutorTest, StopDuringCreateWaitsAndDoesNotClaimMissingRuntimeRetired)
{
    SandboxdExecutor executor("deferred-stop", litebus::AID(), "/tmp/deferred-stop");
    EXPECT_TRUE(executor.StopSandbox("absent", "stop", false).Get().IsError());
    messages::RuntimeInstanceInfo info;
    info.set_requestid("create-original");
    executor.stateManager_.Register({"runtime", {}, {}, {}, info});
    litebus::Promise<messages::StartInstanceResponse> creating;
    executor.stateManager_.MarkStartInProgress("runtime", creating.GetFuture());
    auto first = executor.StopSandbox("runtime", "stop-one", false);
    auto duplicate = executor.StopSandbox("runtime", "stop-two", false);
    EXPECT_FALSE(first.IsOK());
    EXPECT_FALSE(first.IsError());
    EXPECT_FALSE(duplicate.IsOK());
    EXPECT_EQ(executor.deferredStops_.size(), 1);
    messages::StartInstanceResponse failed;
    failed.set_requestid("create-original");
    failed.set_code(static_cast<int32_t>(StatusCode::ERR_INNER_SYSTEM_ERROR));
    EXPECT_TRUE(executor.ResumeDeferredStop("runtime", "create-original", "stop-one", false, failed).Get().IsError());
    failed.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    failed.set_requestid("replacement-create");
    EXPECT_TRUE(executor.ResumeDeferredStop("runtime", "create-original", "stop-one", false, failed).Get().IsError());
    // Once the create future has left the dedup table, unresolved retirement
    // still prevents a new creation from replacing its local state.
    executor.stateManager_.MarkStartDone("runtime");
    auto replacement = std::make_shared<messages::StartInstanceRequest>();
    replacement->mutable_runtimeinstanceinfo()->set_runtimeid("runtime");
    replacement->mutable_runtimeinstanceinfo()->set_requestid("replacement-create");
    EXPECT_NE(executor.StartInstance(replacement, {}).Get().code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(executor.stateManager_.Find("runtime")->instanceInfo.requestid(), "create-original");
}

TEST(SandboxdExecutorTest, DeferredStopGcRequiresMatchingSuccessfulRetirement)
{
    SandboxdExecutor executor("deferred-stop-gc", litebus::AID(), "/tmp/deferred-stop-gc");
    auto old = std::make_shared<litebus::Promise<Status>>();
    auto current = std::make_shared<litebus::Promise<Status>>();
    executor.deferredStops_["runtime"] = {"create", current};
    const litebus::Future<Status> success(Status::OK());
    executor.FinishDeferredStop("runtime", old, success);
    EXPECT_EQ(executor.deferredStops_.count("runtime"), 1);
    litebus::Future<Status> unavailable;
    unavailable.SetFailed(123);
    executor.FinishDeferredStop("runtime", current, unavailable);
    EXPECT_EQ(executor.deferredStops_.count("runtime"), 1);
    executor.FinishDeferredStop("runtime", current,
        litebus::Future<Status>(Status(StatusCode::ERR_INSTANCE_BUSY, "unknown")));
    EXPECT_EQ(executor.deferredStops_.count("runtime"), 1);
    executor.stateManager_.Register({"runtime", "sandbox", {}, {}, messages::RuntimeInstanceInfo()});
    executor.FinishDeferredStop("runtime", current, success);
    EXPECT_EQ(executor.deferredStops_.count("runtime"), 1);
    executor.stateManager_.Unregister("runtime");
    executor.FinishDeferredStop("runtime", current, success);
    EXPECT_TRUE(executor.deferredStops_.empty());
    // A completed Future remains readable by existing callers after map retirement.
    current->SetValue(Status::OK());
    EXPECT_TRUE(current->GetFuture().Get().IsOk());
}

// ── ReportSandboxUsageMetrics known-flag gating ───────────────────────────────

namespace {

// Old servers fill the numerics but leave every known flag at its false
// protobuf default, so tests opt into known flags per scenario instead of a
// helper that silently marks every field known.
runtime::v1::StatsResponse MakeUsageStatsSample()
{
    runtime::v1::StatsResponse response;
    response.set_resource_generation("generation-one");
    response.set_cpu_usage_ns(1000000000u);
    response.set_memory_usage_bytes(512u * 1024u * 1024u);
    response.set_memory_limit_bytes(2u * 1024u * 1024u * 1024u);
    response.set_memory_max_usage_bytes(700u * 1024u * 1024u);
    return response;
}

runtime::v1::StatsResponse MakeKnownUsageStatsSample()
{
    auto response = MakeUsageStatsSample();
    response.set_cpu_usage_known(true);
    response.set_memory_usage_known(true);
    response.set_memory_limit_known(true);
    return response;
}

}  // namespace

TEST(SandboxdExecutorTest, ReportSandboxUsageMetricsFirstKnownSampleInvokesReporter)
{
    SandboxdExecutor executor("usage-first-sample", litebus::AID(), "/tmp/usage-first-sample");
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("usage-instance");
    info.set_runtimeid("usage-runtime");
    executor.stateManager_.Register({"usage-runtime", "usage-sandbox", {}, {}, info});

    std::vector<resources::SandboxResourceSample> reported;
    executor.SetInstanceResourceSampleReporter(
        [&reported](const std::string &instanceID, const resources::SandboxResourceSample &sample) {
            EXPECT_EQ(instanceID, "usage-instance");
            reported.push_back(sample);
        });

    executor.ReportSandboxUsageMetrics(info, "usage-runtime", MakeKnownUsageStatsSample(),
                                       std::chrono::steady_clock::now());

    ASSERT_EQ(reported.size(), 1u);
    EXPECT_EQ(reported[0].runtime_id(), "usage-runtime");
    EXPECT_EQ(reported[0].sandbox_id(), "usage-sandbox");
    EXPECT_EQ(reported[0].resource_generation(), "generation-one");
    EXPECT_EQ(reported[0].memory_current_bytes(), 512u * 1024u * 1024u);
    EXPECT_EQ(reported[0].memory_limit_bytes(), 2u * 1024u * 1024u * 1024u);
    EXPECT_EQ(reported[0].memory_peak_bytes(), 700u * 1024u * 1024u);
    // First observation has no CPU baseline, so the rate stays unknown.
    EXPECT_FALSE(reported[0].cpu_usage_known());
    EXPECT_EQ(reported[0].cpu_usage_cores(), 0.0);
    ASSERT_EQ(executor.sandboxStatsSnapshots_.count("usage-runtime"), 1u);
    EXPECT_EQ(executor.sandboxStatsSnapshots_.at("usage-runtime").cpuUsageNs, 1000000000u);
}

TEST(SandboxdExecutorTest, ReportSandboxUsageMetricsUnknownUsageFieldDropsSampleAndBreaksBaseline)
{
    SandboxdExecutor executor("usage-unknown-field", litebus::AID(), "/tmp/usage-unknown-field");
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("usage-instance");
    info.set_runtimeid("usage-runtime");
    executor.stateManager_.Register({"usage-runtime", "usage-sandbox", {}, {}, info});

    std::vector<resources::SandboxResourceSample> reported;
    executor.SetInstanceResourceSampleReporter(
        [&reported](const std::string &, const resources::SandboxResourceSample &sample) { reported.push_back(sample); });

    // Each gating flag must independently drop the whole sample and erase the
    // CPU baseline; the other two flags being known must not rescue it.
    const std::vector<std::function<void(runtime::v1::StatsResponse &)>> unknownVariants = {
        [](runtime::v1::StatsResponse &response) { response.set_cpu_usage_known(false); },
        [](runtime::v1::StatsResponse &response) { response.set_memory_usage_known(false); },
        [](runtime::v1::StatsResponse &response) { response.set_memory_limit_known(false); },
    };
    for (std::size_t variant = 0; variant < unknownVariants.size(); ++variant) {
        SCOPED_TRACE(testing::Message() << "unknown variant " << variant);
        executor.ReportSandboxUsageMetrics(info, "usage-runtime", MakeKnownUsageStatsSample(),
                                           std::chrono::steady_clock::now());
        ASSERT_EQ(executor.sandboxStatsSnapshots_.count("usage-runtime"), 1u);
        const auto reportedBefore = reported.size();

        auto unknown = MakeKnownUsageStatsSample();
        unknownVariants[variant](unknown);
        executor.ReportSandboxUsageMetrics(info, "usage-runtime", unknown, std::chrono::steady_clock::now());

        EXPECT_EQ(reported.size(), reportedBefore);
        EXPECT_EQ(executor.sandboxStatsSnapshots_.count("usage-runtime"), 0u);
    }
}

TEST(SandboxdExecutorTest, ReportSandboxUsageMetricsOldServerDefaultsNeverFeedView)
{
    SandboxdExecutor executor("usage-old-server", litebus::AID(), "/tmp/usage-old-server");
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("usage-instance");
    info.set_runtimeid("usage-runtime");
    executor.stateManager_.Register({"usage-runtime", "usage-sandbox", {}, {}, info});

    std::vector<resources::SandboxResourceSample> reported;
    executor.SetInstanceResourceSampleReporter(
        [&reported](const std::string &, const resources::SandboxResourceSample &sample) { reported.push_back(sample); });

    // Old sandboxd populates numerics but no known flags at all: the protobuf
    // zero defaults must never be mistaken for observations.
    const auto legacy = MakeUsageStatsSample();
    const auto now = std::chrono::steady_clock::now();
    executor.ReportSandboxUsageMetrics(info, "usage-runtime", legacy, now);
    executor.ReportSandboxUsageMetrics(info, "usage-runtime", legacy, now + std::chrono::seconds(1));
    EXPECT_TRUE(reported.empty());
    EXPECT_TRUE(executor.sandboxStatsSnapshots_.empty());
}

TEST(SandboxdExecutorTest, ReportSandboxUsageMetricsBaselineRestartKeepsCpuRateUnknown)
{
    SandboxdExecutor executor("usage-baseline-restart", litebus::AID(), "/tmp/usage-baseline-restart");
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("usage-instance");
    info.set_runtimeid("usage-runtime");
    executor.stateManager_.Register({"usage-runtime", "usage-sandbox", {}, {}, info});

    std::vector<resources::SandboxResourceSample> reported;
    executor.SetInstanceResourceSampleReporter(
        [&reported](const std::string &, const resources::SandboxResourceSample &sample) { reported.push_back(sample); });

    const auto t0 = std::chrono::steady_clock::now();
    executor.ReportSandboxUsageMetrics(info, "usage-runtime", MakeKnownUsageStatsSample(), t0);
    auto unknown = MakeKnownUsageStatsSample();
    unknown.set_cpu_usage_known(false);
    executor.ReportSandboxUsageMetrics(info, "usage-runtime", unknown, t0 + std::chrono::seconds(1));
    ASSERT_EQ(reported.size(), 1u);
    ASSERT_EQ(executor.sandboxStatsSnapshots_.count("usage-runtime"), 0u);

    // The next known sample restarts the baseline: even with a larger counter
    // over a longer window, the interval spanning the unknown sample was never
    // observed, so the CPU rate must stay unknown.
    auto next = MakeKnownUsageStatsSample();
    next.set_cpu_usage_ns(1500000000u);
    executor.ReportSandboxUsageMetrics(info, "usage-runtime", next, t0 + std::chrono::seconds(2));

    ASSERT_EQ(reported.size(), 2u);
    EXPECT_FALSE(reported[1].cpu_usage_known());
    EXPECT_EQ(reported[1].cpu_usage_cores(), 0.0);
    ASSERT_EQ(executor.sandboxStatsSnapshots_.count("usage-runtime"), 1u);
    EXPECT_EQ(executor.sandboxStatsSnapshots_.at("usage-runtime").cpuUsageNs, 1500000000u);
}

TEST(SandboxdExecutorTest, ReportSandboxUsageMetricsSameGenerationCounterYieldsKnownRate)
{
    SandboxdExecutor executor("usage-known-rate", litebus::AID(), "/tmp/usage-known-rate");
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("usage-instance");
    info.set_runtimeid("usage-runtime");
    executor.stateManager_.Register({"usage-runtime", "usage-sandbox", {}, {}, info});

    std::vector<resources::SandboxResourceSample> reported;
    executor.SetInstanceResourceSampleReporter(
        [&reported](const std::string &, const resources::SandboxResourceSample &sample) { reported.push_back(sample); });

    const auto t0 = std::chrono::steady_clock::now();
    executor.ReportSandboxUsageMetrics(info, "usage-runtime", MakeKnownUsageStatsSample(), t0);
    auto next = MakeKnownUsageStatsSample();
    next.set_cpu_usage_ns(1500000000u);  // 0.5 core over the 1s window
    executor.ReportSandboxUsageMetrics(info, "usage-runtime", next, t0 + std::chrono::seconds(1));

    ASSERT_EQ(reported.size(), 2u);
    EXPECT_TRUE(reported[1].cpu_usage_known());
    EXPECT_DOUBLE_EQ(reported[1].cpu_usage_cores(), 0.5);
    EXPECT_EQ(reported[1].cpu_usage_ns(), 1500000000u);
}

TEST(SandboxdExecutorTest, ReportSandboxUsageMetricsAcceptsKnownZeroMemorySample)
{
    SandboxdExecutor executor("usage-known-zero", litebus::AID(), "/tmp/usage-known-zero");
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("usage-instance");
    info.set_runtimeid("usage-runtime");
    executor.stateManager_.Register({"usage-runtime", "usage-sandbox", {}, {}, info});

    std::vector<resources::SandboxResourceSample> reported;
    executor.SetInstanceResourceSampleReporter(
        [&reported](const std::string &, const resources::SandboxResourceSample &sample) { reported.push_back(sample); });

    auto idle = MakeKnownUsageStatsSample();
    idle.set_memory_usage_bytes(0);
    executor.ReportSandboxUsageMetrics(info, "usage-runtime", idle, std::chrono::steady_clock::now());

    ASSERT_EQ(reported.size(), 1u);
    EXPECT_EQ(reported[0].memory_current_bytes(), 0u);
    EXPECT_EQ(reported[0].memory_limit_bytes(), 2u * 1024u * 1024u * 1024u);
}

// ── StopInstanceWithOutcome retirement-receipt semantics (v3) ────────────────

TEST(SandboxdExecutorTest, OutcomeRejectsOneSidedIdentityBeforeAnyDispatch)
{
    SandboxdExecutor executor("outcome-one-sided", litebus::AID(), "/tmp/outcome-one-sided");
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("one-sided-instance");
    info.set_runtimeid("one-sided-runtime");
    SandboxInfo registered;
    registered.runtimeID = "one-sided-runtime";
    registered.sandboxID = "one-sided-sandbox";
    registered.resourceGeneration = "one-sided-generation";
    registered.instanceInfo = info;
    executor.stateManager_.Register(registered);

    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("one-sided-runtime");
    request->set_requestid("one-sided-stop");
    request->set_expected_sandbox_id("one-sided-sandbox");  // generation missing

    auto outcome = executor.StopInstanceWithOutcome(request, false);
    ASSERT_AWAIT_READY(outcome);
    EXPECT_TRUE(outcome.Get().status.IsError());
    EXPECT_EQ(outcome.Get().status.StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
    EXPECT_FALSE(outcome.Get().retirement.confirmed);
    // Nothing was dispatched, so the runtime must still be locally active.
    EXPECT_TRUE(executor.stateManager_.IsActive("one-sided-runtime"));
}

TEST(SandboxdExecutorTest, OutcomeRejectsForeignExpectedIdentityBeforeDispatch)
{
    SandboxdExecutor executor("outcome-foreign", litebus::AID(), "/tmp/outcome-foreign");
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("foreign-instance");
    info.set_runtimeid("foreign-runtime");
    SandboxInfo foreignState;
    foreignState.runtimeID = "foreign-runtime";
    foreignState.sandboxID = "foreign-sandbox";
    foreignState.resourceGeneration = "foreign-generation";
    foreignState.instanceInfo = info;
    executor.stateManager_.Register(foreignState);

    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("foreign-runtime");
    request->set_requestid("foreign-stop");
    request->set_expected_sandbox_id("somebody-elses-sandbox");
    request->set_expected_resource_generation("somebody-elses-generation");

    auto outcome = executor.StopInstanceWithOutcome(request, false);
    ASSERT_AWAIT_READY(outcome);
    EXPECT_TRUE(outcome.Get().status.IsError());
    EXPECT_EQ(outcome.Get().status.StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
    EXPECT_FALSE(outcome.Get().retirement.confirmed);
    EXPECT_TRUE(executor.stateManager_.IsActive("foreign-runtime"));
}

TEST(SandboxdExecutorTest, OutcomeProofOnlyFollowsFencedSuccessfulTerminate)
{
    SandboxdExecutor executor("outcome-proof", litebus::AID(), "/tmp/outcome-proof");
    // OnOutcomeStopDone is the post-TerminateSandbox continuation: a fenced
    // success yields the durable receipt, any failure stays UNKNOWN.
    auto failure = executor.OnOutcomeStopDone("proof-runtime", "proof-request", "proof-sandbox",
        "proof-generation", Status(StatusCode::ERR_INNER_COMMUNICATION, "grpc failed"));
    EXPECT_TRUE(failure.status.IsError());
    EXPECT_FALSE(failure.retirement.confirmed);

    auto success = executor.OnOutcomeStopDone("proof-runtime", "proof-request", "proof-sandbox",
        "proof-generation", Status::OK());
    EXPECT_TRUE(success.status.IsOk());
    ASSERT_TRUE(success.retirement.confirmed);
    EXPECT_EQ(success.retirement.sandboxID, "proof-sandbox");
    EXPECT_EQ(success.retirement.resourceGeneration, "proof-generation");
}

TEST(SandboxdExecutorTest, ReplayConfirmedOnlyOnExactRetiredGeneration)
{
    SandboxdExecutor executor("replay-receipt", litebus::AID(), "/tmp/replay-receipt");
    // Durable complete receipt naming the exact generation authorizes the
    // idempotent proof even with local state already unregistered.
    runtime::v1::DeleteIfGenerationResponse confirmed;
    confirmed.set_retired_generation("replay-generation");
    auto ok = executor.OnReplayRetirementDone("replay-sandbox", "replay-generation", "replay-runtime",
        "replay-request", litebus::Try<runtime::v1::DeleteIfGenerationResponse>(confirmed));
    EXPECT_TRUE(ok.status.IsOk());
    ASSERT_TRUE(ok.retirement.confirmed);
    EXPECT_EQ(ok.retirement.sandboxID, "replay-sandbox");
    EXPECT_EQ(ok.retirement.resourceGeneration, "replay-generation");

    // A receipt naming a different generation is not this retirement.
    runtime::v1::DeleteIfGenerationResponse foreign;
    foreign.set_retired_generation("another-generation");
    auto mismatch = executor.OnReplayRetirementDone("replay-sandbox", "replay-generation", "replay-runtime",
        "replay-request", litebus::Try<runtime::v1::DeleteIfGenerationResponse>(foreign));
    EXPECT_TRUE(mismatch.status.IsError());
    EXPECT_FALSE(mismatch.retirement.confirmed);

    // Transport failure: UNKNOWN, credit retained.
    auto unavailable = executor.OnReplayRetirementDone("replay-sandbox", "replay-generation", "replay-runtime",
        "replay-request",
        litebus::Try<runtime::v1::DeleteIfGenerationResponse>(static_cast<int32_t>(GRPC_UNAVAILABLE)));
    EXPECT_TRUE(unavailable.status.IsError());
    EXPECT_FALSE(unavailable.retirement.confirmed);

    // Missing sandbox without a complete receipt: still UNKNOWN.
    auto missing = executor.OnReplayRetirementDone("replay-sandbox", "replay-generation", "replay-runtime",
        "replay-request",
        litebus::Try<runtime::v1::DeleteIfGenerationResponse>(static_cast<int32_t>(GRPC_NOT_FOUND)));
    EXPECT_TRUE(missing.status.IsError());
    EXPECT_FALSE(missing.retirement.confirmed);
}

TEST(SandboxdExecutorTest, LegacyStopWithoutExpectedIdentityReturnsNoProof)
{
    SandboxdExecutor executor("legacy-stop", litebus::AID(), "/tmp/legacy-stop");
    // Missing runtime without expected identity: legacy UNKNOWN retirement.
    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("ghost-runtime");
    request->set_requestid("ghost-stop");
    auto outcome = executor.StopInstanceWithOutcome(request, false);
    ASSERT_AWAIT_READY(outcome);
    EXPECT_TRUE(outcome.Get().status.IsError());
    EXPECT_FALSE(outcome.Get().retirement.confirmed);
}


// ── Fast-create-then-retire + post-delete retry (kill identity entry) ────────

TEST(SandboxdExecutorTest, MissingRuntimeWithCompleteIdentityReplaysDurableReceipt)
{
    // The a1-0 shape: local state already unregistered, but the kill carries
    // the COMPLETE expected identity and a durable complete receipt exists.
    // The stop MUST go through the replay path, not the RUNTIME executor's
    // can-not-find-pid dead end.
    SandboxdExecutor executor("fast-create", litebus::AID(), "/tmp/fast-create");
    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("fast-create-runtime");
    request->set_requestid("fast-create-stop");
    request->set_expected_sandbox_id("fast-create-sandbox");
    request->set_expected_resource_generation("fast-create-generation");
    // No stateManager_.Register: the runtime map is empty (already deleted).
    auto outcome = executor.StopInstanceWithOutcome(request, false);
    // The replay is async against sandboxd; in the unit harness it resolves
    // to UNKNOWN (no daemon attached) — the contract is: NEVER a plain
    // "can not find pid" failure, and never a confirmed proof without the
    // daemon receipt. The executor selection itself is covered by the
    // RuntimeManager routing test below.
    ASSERT_AWAIT_READY(outcome);
    EXPECT_TRUE(outcome.Get().status.IsError());
    EXPECT_NE(outcome.Get().status.GetMessage().find("replay"), std::string::npos)
        << "missing runtime with complete identity must route to replay";
    EXPECT_FALSE(outcome.Get().retirement.confirmed);
}

TEST(SandboxdExecutorTest, MissingRuntimeWithoutIdentityStaysUnknownNoProof)
{
    // Identity-less stop of a missing runtime: legacy UNKNOWN — never a
    // success that would release an allocation without a receipt.
    SandboxdExecutor executor("legacy-missing", litebus::AID(), "/tmp/legacy-missing");
    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("legacy-missing-runtime");
    request->set_requestid("legacy-missing-stop");
    auto outcome = executor.StopInstanceWithOutcome(request, false);
    ASSERT_AWAIT_READY(outcome);
    EXPECT_TRUE(outcome.Get().status.IsError());
    EXPECT_FALSE(outcome.Get().retirement.confirmed);
}

}  // namespace functionsystem::test
