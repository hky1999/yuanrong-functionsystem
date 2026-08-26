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

#include "runtime_manager/executor/sandboxd/sandboxd_request_builder.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/proto/pb/message_pb.h"
#include "runtime_manager/config/build.h"
#include "runtime_manager/config/command_builder.h"
#include "utils/os_utils.hpp"

using namespace functionsystem::runtime_manager;

namespace functionsystem::test {

// ── SandboxdRequestBuilder tests (flat SandboxService StartRequest) ───────────

class SandboxdRequestBuilderTest : public ::testing::Test {
public:
    void SetUp() override
    {
        RuntimeConfig config;
        config.runtimePath       = "/opt/runtime";
        config.runtimeLogLevel   = "INFO";
        config.runtimeConfigPath = "/etc/runtime";
        config.runtimeLogPath    = "/tmp/sandboxd-request-builder-test";
        config.hostIP            = "127.0.0.1";
        config.proxyIP           = "10.20.30.40";
        config.proxyGrpcServerPort = "31222";

        cmdBuilder_ = std::make_unique<CommandBuilder>(/*execLookPath=*/false);
        cmdBuilder_->SetRuntimeConfig(config);
        builder_    = std::make_unique<SandboxdRequestBuilder>(*cmdBuilder_);
    }

    void TearDown() override
    {
    }

    // Minimal params with no CONTAINER_ROOTFS -> Build succeeds against the
    // container config; flat request should carry sandbox_id/runtime/rootfs.
    SandboxdStartParams MakeMinimalParams() const
    {
        auto req = std::make_shared<messages::StartInstanceRequest>();
        req->mutable_runtimeinstanceinfo()->set_instanceid("test-instance");
        req->mutable_runtimeinstanceinfo()->set_runtimeid("test-runtime");
        req->mutable_runtimeinstanceinfo()->mutable_runtimeconfig()->set_language("python3.9");
        req->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->set_deploydir("/dcache");
        req->mutable_runtimeinstanceinfo()->set_traceid("trace-abc");
        req->mutable_runtimeinstanceinfo()->mutable_container()->set_id("container-001");
        req->mutable_runtimeinstanceinfo()->mutable_container()->set_runtime("runsc");
        auto *rootfs = req->mutable_runtimeinstanceinfo()->mutable_container()->mutable_rootfsconfig();
        rootfs->set_type(runtime::v1::RootfsSrcType::LOCAL);
        rootfs->set_path("/opt/runtime/rootfs.img");
        rootfs->set_readonly(false);

        SandboxdStartParams params;
        params.request   = req;
        params.runtimeID = "test-runtime";
        return params;
    }

    std::unique_ptr<CommandBuilder>         cmdBuilder_;
    std::unique_ptr<SandboxdRequestBuilder> builder_;
};

// Build succeeds and returns a flat StartRequest.
TEST_F(SandboxdRequestBuilderTest, BuildReturnsFlatStartRequest)
{
    auto params = MakeMinimalParams();
    auto [status, startReq] = builder_->Build(params);

    EXPECT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
}

// sandbox_id is left empty: sandboxd generates it and returns it in StartResponse.id.
TEST_F(SandboxdRequestBuilderTest, FlatRequestLeavesSandboxIdEmpty)
{
    auto params = MakeMinimalParams();
    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    EXPECT_TRUE(startReq->sandbox_id().empty());
    EXPECT_EQ(startReq->runtime(), "runsc");
}

// trace_id is the distributed trace ID from the upstream request (not runtimeID).
TEST_F(SandboxdRequestBuilderTest, FlatRequestHasTraceId)
{
    auto params = MakeMinimalParams();
    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->trace_id(), "trace-abc");
}

// The single envs map carries YR_LANGUAGE derived from the runtime config.
TEST_F(SandboxdRequestBuilderTest, FlatRequestEnvsCarriesLanguage)
{
    auto params = MakeMinimalParams();
    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->envs().at("YR_LANGUAGE"), "python3.9");
}

TEST_F(SandboxdRequestBuilderTest, FlatRequestHonorsNoSetCudaVisibleDevices)
{
    litebus::os::SetEnv("YR_NOSET_CUDA_VISIBLE_DEVICES", "1");
    auto params = MakeMinimalParams();
    params.envs.userEnvs["CUDA_VISIBLE_DEVICES"] = "0";

    auto [status, startReq] = builder_->Build(params);
    litebus::os::UnSetEnv("YR_NOSET_CUDA_VISIBLE_DEVICES");

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->envs().at("YR_NOSET_CUDA_VISIBLE_DEVICES"), "1");
    EXPECT_EQ(startReq->envs().count("CUDA_VISIBLE_DEVICES"), 0);
}

TEST_F(SandboxdRequestBuilderTest, SelfContainedBootstrapUsesOnlyBootstrapCommand)
{
    auto params = MakeMinimalParams();
    auto *info = params.request->mutable_runtimeinstanceinfo();
    info->mutable_runtimeconfig()->set_language("rust");
    info->mutable_bootstrapconfig()->set_entrypoint("rrt-runtime --serve");
    (*info->mutable_deploymentconfig()->mutable_deployoptions())["rootfs"] =
        R"({"runtime":"runc","type":"image","imageurl":"runtime:latest"})";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    ASSERT_EQ(startReq->command_size(), 2);
    EXPECT_EQ(startReq->command(0), "rrt-runtime");
    EXPECT_EQ(startReq->command(1), "--serve");
    EXPECT_EQ(startReq->envs().at("YR_LANGUAGE"), "rust");
    EXPECT_EQ(startReq->runtime(), "runc");
}

TEST_F(SandboxdRequestBuilderTest, RuntimeOnlyOverlayInheritsServiceRootfsAndBypassesIncompatibleTemplate)
{
    auto params = MakeMinimalParams();
    params.registeredTemplateIDs.insert("container-001");
    (*params.request->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->mutable_deployoptions())["rootfs"] =
        R"({"runtime":"kata"})";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk()) << status.RawMessage();
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->runtime(), "kata");
    EXPECT_EQ(startReq->rootfs().type(), runtime::v1::RootfsSrcType::LOCAL);
    EXPECT_EQ(startReq->rootfs().path(), "/opt/runtime/rootfs.img");
    EXPECT_FALSE(startReq->rootfs().readonly());
    EXPECT_TRUE(startReq->template_id().empty());
}

TEST_F(SandboxdRequestBuilderTest, RuntimeOnlyOverlayKeepsCompatibleTemplateAndSkipsBootstrapMount)
{
    auto params = MakeMinimalParams();
    params.registeredTemplateIDs.insert("container-001");
    auto *info = params.request->mutable_runtimeinstanceinfo();
    info->mutable_bootstrapconfig()->set_type("erofs");
    info->mutable_bootstrapconfig()->set_root("/opt/runtime/rootfs.img");
    (*info->mutable_deploymentconfig()->mutable_deployoptions())["rootfs"] = R"({"runtime":"runsc"})";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk()) << status.RawMessage();
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->template_id(), "container-001");
    EXPECT_EQ(startReq->mounts_size(), 0);
    EXPECT_EQ(startReq->envs().at("YR_RT_WORKING_DIR"), "/");
}

TEST_F(SandboxdRequestBuilderTest, ReadonlyOverlayPreservesSourceAndBypassesTemplateWithoutBootstrapMount)
{
    auto params = MakeMinimalParams();
    params.registeredTemplateIDs.insert("container-001");
    auto *info = params.request->mutable_runtimeinstanceinfo();
    info->mutable_bootstrapconfig()->set_type("erofs");
    info->mutable_bootstrapconfig()->set_root("/opt/runtime/rootfs.img");
    (*info->mutable_deploymentconfig()->mutable_deployoptions())["rootfs"] = R"({"readonly":true})";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk()) << status.RawMessage();
    ASSERT_NE(startReq, nullptr);
    EXPECT_TRUE(startReq->rootfs().readonly());
    EXPECT_EQ(startReq->rootfs().path(), "/opt/runtime/rootfs.img");
    EXPECT_TRUE(startReq->template_id().empty());
    EXPECT_EQ(startReq->mounts_size(), 0);
}

TEST_F(SandboxdRequestBuilderTest, SourceOverlayAtomicallyReplacesRootfsAndAddsBootstrapMount)
{
    auto params = MakeMinimalParams();
    params.registeredTemplateIDs.insert("container-001");
    auto *info = params.request->mutable_runtimeinstanceinfo();
    info->mutable_bootstrapconfig()->set_type("erofs");
    info->mutable_bootstrapconfig()->set_root("/opt/runtime/rootfs.img");
    (*info->mutable_deploymentconfig()->mutable_deployoptions())["rootfs"] =
        R"({"runtime":"kata","type":"image","imageurl":"ubuntu:24.04"})";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk()) << status.RawMessage();
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->runtime(), "kata");
    EXPECT_EQ(startReq->rootfs().type(), runtime::v1::RootfsSrcType::IMAGE);
    EXPECT_EQ(startReq->rootfs().image_url(), "ubuntu:24.04");
    EXPECT_TRUE(startReq->rootfs().path().empty());
    EXPECT_TRUE(startReq->template_id().empty());
    ASSERT_EQ(startReq->mounts_size(), 1);
    EXPECT_EQ(startReq->mounts(0).host_path(), "/opt/runtime/rootfs.img");
    EXPECT_EQ(startReq->mounts(0).target(), "/__yuanrong");
}

TEST_F(SandboxdRequestBuilderTest, InvalidSourceOverlayFailsBeforeSandboxd)
{
    auto params = MakeMinimalParams();
    (*params.request->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->mutable_deployoptions())["rootfs"] =
        R"({"type":"image","path":"/unexpected"})";

    auto [status, startReq] = builder_->Build(params);

    EXPECT_FALSE(status.IsOk());
    EXPECT_EQ(status.StatusCode(), StatusCode::ERR_PARAM_INVALID);
    EXPECT_EQ(startReq, nullptr);
}

TEST_F(SandboxdRequestBuilderTest, WritableLayerChangeBypassesTemplate)
{
    auto params = MakeMinimalParams();
    params.registeredTemplateIDs.insert("container-001");
    auto *storage = &(*params.request->mutable_runtimeinstanceinfo()
                           ->mutable_runtimeconfig()
                           ->mutable_resources()
                           ->mutable_resources())["storage"];
    storage->set_type(resources::Value_Type_SCALAR);
    storage->mutable_scalar()->set_value(104857600);

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk()) << status.RawMessage();
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->writable_layer_limit_bytes(), 104857600U);
    EXPECT_EQ(startReq->rootfs().writable_layer_size_bytes(), 104857600U);
    EXPECT_TRUE(startReq->template_id().empty());
}

// stdout/stderr log paths are resolved onto the flat request.
TEST_F(SandboxdRequestBuilderTest, FlatRequestHasLogPaths)
{
    auto params = MakeMinimalParams();
    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    EXPECT_FALSE(startReq->stdout().empty());
    EXPECT_FALSE(startReq->stderr().empty());
}

TEST_F(SandboxdRequestBuilderTest, FlatRequestCarriesPhysicalGpuAllocation)
{
    auto params = MakeMinimalParams();
    params.envs.userEnvs["GPU-DEVICE-IDS"] = "3,8";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    ASSERT_EQ(startReq->xpu_allocations_size(), 1);
    const auto &allocation = startReq->xpu_allocations(0);
    EXPECT_EQ(allocation.type(), "gpu");
    ASSERT_EQ(allocation.device_ids_size(), 2);
    EXPECT_EQ(allocation.device_ids(0), 3U);
    EXPECT_EQ(allocation.device_ids(1), 8U);
}

TEST_F(SandboxdRequestBuilderTest, FlatRequestCarriesWritableLayerSize)
{
    auto params = MakeMinimalParams();
    auto *storage = &(*params.request->mutable_runtimeinstanceinfo()
                           ->mutable_runtimeconfig()
                           ->mutable_resources()
                           ->mutable_resources())["storage"];
    storage->set_type(resources::Value_Type_SCALAR);
    storage->mutable_scalar()->set_value(104857600);
    storage->mutable_scalar()->set_limit(209715200);

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->writable_layer_limit_bytes(), 209715200U);
    EXPECT_EQ(startReq->rootfs().writable_layer_size_bytes(), 209715200U);
}

TEST_F(SandboxdRequestBuilderTest, InvalidWritableLayerSizeFailsBuild)
{
    auto params = MakeMinimalParams();
    auto *storage = &(*params.request->mutable_runtimeinstanceinfo()
                           ->mutable_runtimeconfig()
                           ->mutable_resources()
                           ->mutable_resources())["storage"];
    storage->set_type(resources::Value_Type_SCALAR);
    storage->mutable_scalar()->set_value(1.5);

    auto [status, startReq] = builder_->Build(params);

    EXPECT_FALSE(status.IsOk());
    EXPECT_EQ(startReq, nullptr);
}

TEST_F(SandboxdRequestBuilderTest, BlockNetworkAllowsOnlyFunctionProxy)
{
    auto params = MakeMinimalParams();
    (*params.request->mutable_runtimeinstanceinfo()
          ->mutable_deploymentconfig()
          ->mutable_deployoptions())["network_policy"] = R"({"blockNetwork":true})";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    ASSERT_TRUE(startReq->has_network_policy());
    ASSERT_TRUE(startReq->network_policy().has_traffic());
    const auto &traffic = startReq->network_policy().traffic();
    EXPECT_EQ(traffic.default_action(), runtime::v1::NETWORK_POLICY_ACTION_DENY);
    EXPECT_EQ(traffic.mode(), runtime::v1::TRAFFIC_POLICY_MODE_STATEFUL);
    ASSERT_EQ(traffic.rules_size(), 1);
    const auto &rule = traffic.rules(0);
    EXPECT_EQ(rule.action(), runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
    EXPECT_EQ(rule.direction(), runtime::v1::NETWORK_DIRECTION_BOTH);
    EXPECT_EQ(rule.protocol(), runtime::v1::NETWORK_PROTOCOL_TCP);
    EXPECT_EQ(rule.peer().address(), "10.20.30.40");
    EXPECT_EQ(rule.peer().port(), 31222U);
}

TEST_F(SandboxdRequestBuilderTest, BlockNetworkAllowsPublishedSandboxTargetPorts)
{
    auto params = MakeMinimalParams();
    params.portMappings = {
        "tcp:21008:50090",
        "tcp:21009:8765",
        "tcp:21010:8766",
        "udp:21011:5353",
        "tcp:21012:50090",
    };
    (*params.request->mutable_runtimeinstanceinfo()
          ->mutable_deploymentconfig()
          ->mutable_deployoptions())["network_policy"] = R"({"blockNetwork":true})";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    const auto &traffic = startReq->network_policy().traffic();
    EXPECT_EQ(traffic.mode(), runtime::v1::TRAFFIC_POLICY_MODE_STATEFUL);
    ASSERT_EQ(traffic.rules_size(), 5);
    EXPECT_TRUE(traffic.rules(0).has_peer());
    const std::vector<std::pair<runtime::v1::NetworkProtocol, uint32_t>> expected = {
        {runtime::v1::NETWORK_PROTOCOL_TCP, 50090},
        {runtime::v1::NETWORK_PROTOCOL_TCP, 8765},
        {runtime::v1::NETWORK_PROTOCOL_TCP, 8766},
        {runtime::v1::NETWORK_PROTOCOL_UDP, 5353},
    };
    for (size_t i = 0; i < expected.size(); ++i) {
        const auto &rule = traffic.rules(static_cast<int>(i + 1));
        EXPECT_EQ(rule.action(), runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
        EXPECT_EQ(rule.direction(), runtime::v1::NETWORK_DIRECTION_INGRESS);
        EXPECT_EQ(rule.protocol(), expected[i].first);
        EXPECT_EQ(rule.sandbox_port(), expected[i].second);
        EXPECT_FALSE(rule.has_peer());
    }
}

TEST_F(SandboxdRequestBuilderTest, ExtraConfigDoesNotConfigureNetworkPolicy)
{
    auto params = MakeMinimalParams();
    (*params.request->mutable_runtimeinstanceinfo()
          ->mutable_deploymentconfig()
          ->mutable_deployoptions())["extra_config"] =
        R"({"networkPolicy":{"blockNetwork":true},"runtimeOption":"value"})";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    EXPECT_FALSE(startReq->has_network_policy());
    EXPECT_EQ(startReq->extra_config(),
              R"({"networkPolicy":{"blockNetwork":true},"runtimeOption":"value"})");
}

TEST_F(SandboxdRequestBuilderTest, DNSBlacklistBuildsDefaultAllowPolicy)
{
    auto params = MakeMinimalParams();
    (*params.request->mutable_runtimeinstanceinfo()
          ->mutable_deploymentconfig()
          ->mutable_deployoptions())["network_policy"] =
        R"({"dnsBlacklist":["github.com","*.github.com"]})";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    ASSERT_TRUE(startReq->has_network_policy());
    ASSERT_TRUE(startReq->network_policy().has_dns());
    const auto &dns = startReq->network_policy().dns();
    EXPECT_EQ(dns.default_action(), runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
    ASSERT_EQ(dns.rules_size(), 2);
    EXPECT_EQ(dns.rules(0).action(), runtime::v1::NETWORK_POLICY_ACTION_DENY);
    EXPECT_EQ(dns.rules(0).pattern(), "github.com");
    EXPECT_EQ(dns.rules(1).pattern(), "*.github.com");
}

TEST_F(SandboxdRequestBuilderTest, MissingNetworkPolicyLeavesRequestUnrestricted)
{
    auto params = MakeMinimalParams();

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    EXPECT_FALSE(startReq->has_network_policy());
}

TEST_F(SandboxdRequestBuilderTest, InvalidNetworkPolicyFailsBuild)
{
    auto params = MakeMinimalParams();
    (*params.request->mutable_runtimeinstanceinfo()
          ->mutable_deploymentconfig()
          ->mutable_deployoptions())["network_policy"] =
        R"({"blockNetwork":true,"dnsBlacklist":["github.com"]})";

    auto [status, startReq] = builder_->Build(params);

    EXPECT_FALSE(status.IsOk());
    EXPECT_EQ(startReq, nullptr);
}

}  // namespace functionsystem::test
