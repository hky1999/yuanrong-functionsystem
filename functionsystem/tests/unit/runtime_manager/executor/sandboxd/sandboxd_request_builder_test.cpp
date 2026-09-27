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
#include <google/protobuf/descriptor.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <limits>
#include <string>

#include "common/proto/pb/message_pb.h"
#include "runtime_manager/config/build.h"
#include "runtime_manager/config/command_builder.h"
#include "utils/scoped_env.h"
#include "utils/os_utils.hpp"

using namespace functionsystem::runtime_manager;

namespace functionsystem::test {

// ── SandboxdRequestBuilder tests (flat SandboxService StartRequest) ───────────

TEST(SandboxdProtoContractTest, UsesPublicCheckpointAndStartRestore)
{
    const auto *service = google::protobuf::DescriptorPool::generated_pool()->FindServiceByName(
        "runtime.v1.SandboxService");
    ASSERT_NE(service, nullptr);
    // Current wire (sandbox_api.proto in this repo): the generation-fenced
    // checkpoint RPC plus Restore; DeleteCheckpoint does not exist. The old
    // assertions pinned the pre-Restore upstream draft (no Restore, fields
    // 1-5, empty CheckpointResponse) and no longer match the shipped proto.
    EXPECT_NE(service->FindMethodByName("Checkpoint"), nullptr);
    EXPECT_NE(service->FindMethodByName("CheckpointIfGeneration"), nullptr);
    EXPECT_NE(service->FindMethodByName("Restore"), nullptr);
    EXPECT_EQ(service->FindMethodByName("DeleteCheckpoint"), nullptr);

    const auto *checkpoint = runtime::v1::CheckpointRequest::descriptor();
    ASSERT_NE(checkpoint, nullptr);
    ASSERT_NE(checkpoint->FindFieldByName("id"), nullptr);
    ASSERT_NE(checkpoint->FindFieldByName("checkpoint_dir"), nullptr);
    ASSERT_NE(checkpoint->FindFieldByName("timeout_seconds"), nullptr);
    ASSERT_NE(checkpoint->FindFieldByName("compress"), nullptr);
    ASSERT_NE(checkpoint->FindFieldByName("leave_running"), nullptr);
    // Upstream checkpoint contract, renumbered: our wire pins 3-8
    // (timeout, compress, trace_id, checkpoint_id, leave_running,
    // include_filesystem) and appends timeout_seconds/snapshot_type.
    EXPECT_EQ(checkpoint->FindFieldByName("id")->number(), 1);
    EXPECT_EQ(checkpoint->FindFieldByName("checkpoint_dir")->number(), 2);
    EXPECT_EQ(checkpoint->FindFieldByName("timeout")->number(), 3);
    EXPECT_EQ(checkpoint->FindFieldByName("compress")->number(), 4);
    EXPECT_EQ(checkpoint->FindFieldByName("trace_id")->number(), 5);
    EXPECT_EQ(checkpoint->FindFieldByName("checkpoint_id")->number(), 6);
    EXPECT_EQ(checkpoint->FindFieldByName("leave_running")->number(), 7);
    EXPECT_EQ(checkpoint->FindFieldByName("include_filesystem")->number(), 8);
    EXPECT_EQ(checkpoint->FindFieldByName("timeout_seconds")->number(), 9);
    // CheckpointResponse: success/message/artifact_path/artifact_size/
    // artifact_sha256/source_generation
    EXPECT_EQ(runtime::v1::CheckpointResponse::descriptor()->field_count(), 6);
    ASSERT_NE(runtime::v1::CheckpointResponse::descriptor()->FindFieldByName("source_generation"), nullptr);

    // The fence rides the wrapper, never CheckpointRequest itself.
    const auto *wrapper = runtime::v1::CheckpointIfGenerationRequest::descriptor();
    ASSERT_NE(wrapper, nullptr);
    EXPECT_EQ(wrapper->FindFieldByName("checkpoint")->number(), 1);
    EXPECT_EQ(wrapper->FindFieldByName("expected_generation")->number(), 2);

    const auto *start = runtime::v1::StartRequest::descriptor();
    ASSERT_NE(start, nullptr);
    ASSERT_NE(start->FindFieldByName("checkpoint_info"), nullptr);
    EXPECT_EQ(start->FindFieldByName("checkpoint_info")->number(), 21);
}

TEST(SandboxdProtoContractTest, UsesPr44InjectEntrypointField)
{
    const auto *field = runtime::v1::StartRequest::descriptor()->FindFieldByName("inject_entrypoint");
    ASSERT_NE(field, nullptr);
    EXPECT_EQ(field->number(), 22);
    EXPECT_EQ(field->cpp_type(), google::protobuf::FieldDescriptor::CPPTYPE_STRING);
}

class SandboxdRequestBuilderTest : public ::testing::Test {
public:
    void SetUp() override
    {
        RuntimeConfig config;
        config.runtimePath = "/opt/runtime";
        config.runtimeLogLevel = "INFO";
        config.runtimeConfigPath = "/etc/runtime";
        config.runtimeLogPath = "/tmp/sandboxd-request-builder-test";
        config.hostIP = "127.0.0.1";
        config.proxyIP = "10.20.30.40";
        config.proxyGrpcServerPort = "31222";

        cmdBuilder_ = std::make_unique<CommandBuilder>(/*execLookPath=*/false);
        cmdBuilder_->SetRuntimeConfig(config);
        builder_ = std::make_unique<SandboxdRequestBuilder>(*cmdBuilder_);
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
        params.request = req;
        params.runtimeID = "test-runtime";
        return params;
    }

    std::unique_ptr<CommandBuilder> cmdBuilder_;
    std::unique_ptr<SandboxdRequestBuilder> builder_;
};

TEST_F(SandboxdRequestBuilderTest, AttachesCheckpointInfoToStart)
{
    runtime::v1::StartRequest request;
    auto status = SandboxdRequestBuilder::AttachCheckpointInfo(
        request, "/var/lib/akernel/checkpoints/snap-1");

    ASSERT_TRUE(status.IsOk()) << status.ToString();
    ASSERT_TRUE(request.has_checkpoint_info());
    EXPECT_EQ(request.checkpoint_info().checkpoint_dir(),
              "/var/lib/akernel/checkpoints/snap-1");
}

TEST_F(SandboxdRequestBuilderTest, RejectsRelativeCheckpointDirectory)
{
    runtime::v1::StartRequest request;
    auto status = SandboxdRequestBuilder::AttachCheckpointInfo(request, "checkpoints/snap-1");

    EXPECT_TRUE(status.IsError());
    EXPECT_FALSE(request.has_checkpoint_info());
}

// Build succeeds and returns a flat StartRequest.
TEST_F(SandboxdRequestBuilderTest, ElasticStartSeparatesInitialLimitsFromCeilings)
{
    auto params = MakeMinimalParams();
    auto *info = params.request->mutable_runtimeinstanceinfo();
    auto *resources = info->mutable_runtimeconfig()->mutable_resources()->mutable_resources();
    for (const auto &name : {"CPU", "Memory"}) {
        auto &resource = (*resources)[name];
        resource.set_type(::resources::Value_Type_SCALAR);
        resource.mutable_scalar()->set_value(std::string(name) == "CPU" ? 500 : 512);
        resource.mutable_scalar()->set_limit(std::string(name) == "CPU" ? 8000 : 8192);
    }
    auto [legacyStatus, legacy] = builder_->Build(params);
    ASSERT_TRUE(legacyStatus.IsOk());
    EXPECT_EQ(legacy->resources().at("CPU"), 8000);
    EXPECT_EQ(legacy->resources().at("Memory"), 8192);
    (*info->mutable_deploymentconfig()->mutable_deployoptions())["scheduler_elastic_resources"] = "true";
    auto [status, start] = builder_->Build(params);
    ASSERT_TRUE(status.IsOk());
    EXPECT_EQ(start->labels().at("akernel.scheduler/instance-id"), "test-instance");
    EXPECT_EQ(start->labels().at("akernel.scheduler/runtime-id"), "test-runtime");
    EXPECT_EQ(start->resources().at("CPU"), 500);
    EXPECT_EQ(start->resources().at("Memory"), 512);
    const auto policy = nlohmann::json::parse(start->labels().at("akernel.scheduler/elastic-resources"));
    EXPECT_EQ(policy["version"], 1);
    EXPECT_EQ(policy["CPU"]["ceiling"], 8000);
    EXPECT_EQ(policy["Memory"]["initial"], 512);
    EXPECT_EQ(policy["Memory"]["ceiling"], 8192);
    EXPECT_EQ(resources->at("Memory").scalar().limit(), 8192);
    (*resources)["Memory"].mutable_scalar()->set_limit(128);
    EXPECT_FALSE(builder_->Build(params).first.IsOk());
    (*resources)["Memory"].mutable_scalar()->set_limit(std::numeric_limits<double>::quiet_NaN());
    EXPECT_FALSE(builder_->Build(params).first.IsOk());
}

TEST_F(SandboxdRequestBuilderTest, ElasticStartRequiresExplicitResourcesAndSupportedRuntime)
{
    auto params = MakeMinimalParams();
    auto *info = params.request->mutable_runtimeinstanceinfo();
    auto *options = info->mutable_deploymentconfig()->mutable_deployoptions();
    (*options)["scheduler_elastic_resources"] = "true";
    EXPECT_FALSE(builder_->Build(params).first.IsOk());
    info->mutable_container()->set_runtime("kata");
    EXPECT_FALSE(builder_->Build(params).first.IsOk());
    (*options)["scheduler_elastic_resources"] = "typo";
    EXPECT_FALSE(builder_->Build(params).first.IsOk());
}

TEST_F(SandboxdRequestBuilderTest, BuildReturnsFlatStartRequest)
{
    auto params = MakeMinimalParams();
    params.cmdArgs.execPath = "python3.9";
    params.cmdArgs.args = { "--rt_server_address", "127.0.0.1:8080" };
    auto [status, startReq] = builder_->Build(params);

    EXPECT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    ASSERT_EQ(startReq->command_size(), 3);
    EXPECT_EQ(startReq->command(0), "python3.9");
    EXPECT_EQ(startReq->command(1), "--rt_server_address");
    EXPECT_EQ(startReq->command(2), "127.0.0.1:8080");
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

TEST_F(SandboxdRequestBuilderTest, LongRuntimeIdentityStillLeavesSandboxIdToSandboxd)
{
    auto params = MakeMinimalParams();
    params.runtimeID =
        "runtime-default-refactor-isolation-snapshot-source-00003f03e828";
    params.request->mutable_runtimeinstanceinfo()->set_runtimeid(params.runtimeID);

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    EXPECT_TRUE(startReq->sandbox_id().empty());
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

TEST_F(SandboxdRequestBuilderTest, FlatRequestCarriesUnifiedLogDir)
{
    auto params = MakeMinimalParams();
    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->envs().at("GLOG_log_dir"), "/tmp/yuanrong/logs");
    EXPECT_EQ(startReq->envs().at("GOOGLE_LOG_DIR"), "/tmp/yuanrong/logs");
    EXPECT_EQ(startReq->envs().at("GLOG_log_dir"), startReq->envs().at("GOOGLE_LOG_DIR"));
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

TEST_F(SandboxdRequestBuilderTest, InheritedImageEntrypointUsesSandboxdField22AndRuntimeMarker)
{
    auto params = MakeMinimalParams();
    auto *options = params.request->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->mutable_deployoptions();
    (*options)["rootfs"] = R"({"runtime":"runsc","type":"image","imageurl":"example/image:latest"})";
    (*options)["inherit_entrypoint"] = "true";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk()) << status.RawMessage();
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->inject_entrypoint(), "/tmp/yr-image-process.json");
    EXPECT_EQ(startReq->envs().at("YR_IMAGE_PROCESS_CONFIG"), startReq->inject_entrypoint());
}

TEST_F(SandboxdRequestBuilderTest, InheritedImageEntrypointPathCanComeFromDeploymentEnvironment)
{
    ScopedEnv imageProcessConfig("YR_IMAGE_PROCESS_CONFIG");
    imageProcessConfig.Set("/tmp/custom-image-process.json");
    auto params = MakeMinimalParams();
    auto *options = params.request->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->mutable_deployoptions();
    (*options)["rootfs"] = R"({"runtime":"runsc","type":"image","imageurl":"example/image:latest"})";
    (*options)["inherit_entrypoint"] = "true";

    auto [status, startReq] = builder_->Build(params);
    ASSERT_TRUE(status.IsOk()) << status.RawMessage();
    ASSERT_NE(startReq, nullptr);
    EXPECT_EQ(startReq->inject_entrypoint(), "/tmp/custom-image-process.json");
    EXPECT_EQ(startReq->envs().at("YR_IMAGE_PROCESS_CONFIG"), "/tmp/custom-image-process.json");
}

TEST_F(SandboxdRequestBuilderTest, InheritedEntrypointRejectsNonImageAndInvalidBoolean)
{
    for (const std::string &value : { "true", "TRUE", "1" }) {
        auto params = MakeMinimalParams();
        auto *options =
            params.request->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->mutable_deployoptions();
        (*options)["inherit_entrypoint"] = value;
        if (value != "true") {
            (*options)["rootfs"] =
                R"({"runtime":"runsc","type":"image","imageurl":"example/image:latest"})";
        }

        auto [status, startReq] = builder_->Build(params);

        EXPECT_FALSE(status.IsOk()) << value;
        EXPECT_EQ(startReq, nullptr) << value;
    }
}

TEST_F(SandboxdRequestBuilderTest, InheritedEntrypointRejectsInvalidInjectionPath)
{
    ScopedEnv imageProcessConfig("YR_IMAGE_PROCESS_CONFIG");
    for (const std::string &path : { "relative.json", "/", "/tmp/../image-process.json" }) {
        imageProcessConfig.Set(path);
        auto params = MakeMinimalParams();
        auto *options =
            params.request->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->mutable_deployoptions();
        (*options)["rootfs"] = R"({"runtime":"runsc","type":"image","imageurl":"example/image:latest"})";
        (*options)["inherit_entrypoint"] = "true";

        auto [status, startReq] = builder_->Build(params);

        EXPECT_FALSE(status.IsOk()) << path;
        EXPECT_EQ(startReq, nullptr) << path;
    }
}

TEST_F(SandboxdRequestBuilderTest, MissingOrFalseInheritedEntrypointKeepsLegacyRequest)
{
    ScopedEnv imageProcessConfig("YR_IMAGE_PROCESS_CONFIG");
    imageProcessConfig.Set("/tmp/deployment-image-process.json");
    for (const std::string &value : { "", "false" }) {
        auto params = MakeMinimalParams();
        if (!value.empty()) {
            (*params.request->mutable_runtimeinstanceinfo()
                  ->mutable_deploymentconfig()
                  ->mutable_deployoptions())["inherit_entrypoint"] = value;
        }

        auto [status, startReq] = builder_->Build(params);

        ASSERT_TRUE(status.IsOk()) << status.RawMessage();
        ASSERT_NE(startReq, nullptr);
        EXPECT_TRUE(startReq->inject_entrypoint().empty());
        EXPECT_EQ(startReq->envs().count("YR_IMAGE_PROCESS_CONFIG"), 0);
    }
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
    params.envs.userEnvs["CUDA_VISIBLE_DEVICES"] = "0,1";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    ASSERT_EQ(startReq->xpu_allocations_size(), 1);
    const auto &allocation = startReq->xpu_allocations(0);
    EXPECT_EQ(allocation.type(), "gpu");
    ASSERT_EQ(allocation.device_ids_size(), 2);
    EXPECT_EQ(allocation.device_ids(0), 3U);
    EXPECT_EQ(allocation.device_ids(1), 8U);
    EXPECT_EQ(startReq->envs().count("GPU-DEVICE-IDS"), 0U);
    EXPECT_EQ(startReq->envs().count("CUDA_VISIBLE_DEVICES"), 0U);
}

TEST_F(SandboxdRequestBuilderTest, FlatRequestCarriesPhysicalNpuAllocation)
{
    auto params = MakeMinimalParams();
    params.envs.userEnvs["NPU-DEVICE-IDS"] = "1,3";
    params.envs.userEnvs["ASCEND_VISIBLE_DEVICES"] = "1,3";
    params.envs.userEnvs["ASCEND_RT_VISIBLE_DEVICES"] = "0,1";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk()) << status.RawMessage();
    ASSERT_NE(startReq, nullptr);
    ASSERT_EQ(startReq->xpu_allocations_size(), 1);
    const auto &allocation = startReq->xpu_allocations(0);
    EXPECT_EQ(allocation.type(), "npu");
    ASSERT_EQ(allocation.device_ids_size(), 2);
    EXPECT_EQ(allocation.device_ids(0), 1U);
    EXPECT_EQ(allocation.device_ids(1), 3U);
    EXPECT_EQ(startReq->envs().count("NPU-DEVICE-IDS"), 0U);
    EXPECT_EQ(startReq->envs().count("ASCEND_VISIBLE_DEVICES"), 0U);
    EXPECT_EQ(startReq->envs().count("ASCEND_RT_VISIBLE_DEVICES"), 0U);
}

TEST_F(SandboxdRequestBuilderTest, RejectsInvalidOrMixedXpuAllocations)
{
    auto params = MakeMinimalParams();
    params.envs.userEnvs["GPU-DEVICE-IDS"] = "0,0";
    auto [duplicateStatus, duplicateRequest] = builder_->Build(params);
    EXPECT_FALSE(duplicateStatus.IsOk());
    EXPECT_EQ(duplicateRequest, nullptr);

    params = MakeMinimalParams();
    params.envs.userEnvs["GPU-DEVICE-IDS"] = "0";
    params.envs.userEnvs["NPU-DEVICE-IDS"] = "1";
    auto [mixedStatus, mixedRequest] = builder_->Build(params);
    EXPECT_FALSE(mixedStatus.IsOk());
    EXPECT_EQ(mixedRequest, nullptr);
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
        "tcp:21008:50090", "tcp:21009:8765", "tcp:21010:8766", "udp:21011:5353", "tcp:21012:50090",
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
        { runtime::v1::NETWORK_PROTOCOL_TCP, 50090 },
        { runtime::v1::NETWORK_PROTOCOL_TCP, 8765 },
        { runtime::v1::NETWORK_PROTOCOL_TCP, 8766 },
        { runtime::v1::NETWORK_PROTOCOL_UDP, 5353 },
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
    EXPECT_EQ(startReq->extra_config(), R"({"networkPolicy":{"blockNetwork":true},"runtimeOption":"value"})");
}

TEST_F(SandboxdRequestBuilderTest, DNSBlacklistBuildsDefaultAllowPolicy)
{
    auto params = MakeMinimalParams();
    (*params.request->mutable_runtimeinstanceinfo()
          ->mutable_deploymentconfig()
          ->mutable_deployoptions())["network_policy"] = R"({"dnsBlacklist":["github.com","*.github.com"]})";

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

TEST_F(SandboxdRequestBuilderTest, ACLVersion2BuildsGenericPolicyAndProtectedRules)
{
    auto params = MakeMinimalParams();
    params.portMappings = { "tcp:21010:8766" };
    (*params.request->mutable_runtimeinstanceinfo()
          ->mutable_deploymentconfig()
          ->mutable_deployoptions())["network_policy"] = R"({
        "schemaVersion": 2,
        "traffic": {
            "ingressDefaultAction": "allow",
            "egressDefaultAction": "deny",
            "mode": "stateful",
            "rules": [
                {
                    "action": "allow",
                    "direction": "egress",
                    "protocol": "tcp",
                    "peer": {
                        "cidr": "192.0.2.0/24",
                        "portRange": {"first": 80, "last": 443}
                    },
                    "priority": 110
                },
                {
                    "action": "deny",
                    "direction": "egress",
                    "protocol": "udp",
                    "peer": {"domain": "*.example.com"},
                    "sandboxPortRange": {"first": 5000, "last": 5010},
                    "priority": 100
                }
            ]
        },
        "dns": {
            "defaultAction": "allow",
            "rules": [{"action": "deny", "pattern": "blocked.example"}]
        }
    })";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    ASSERT_TRUE(startReq->has_network_policy());
    const auto &policy = startReq->network_policy();
    EXPECT_EQ(policy.schema_version(), 2U);
    ASSERT_TRUE(policy.has_traffic());
    const auto &traffic = policy.traffic();
    EXPECT_EQ(traffic.ingress_default_action(), runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
    EXPECT_EQ(traffic.egress_default_action(), runtime::v1::NETWORK_POLICY_ACTION_DENY);
    EXPECT_EQ(traffic.mode(), runtime::v1::TRAFFIC_POLICY_MODE_STATEFUL);
    ASSERT_EQ(traffic.rules_size(), 4);

    const auto &cidrRule = traffic.rules(0);
    EXPECT_EQ(cidrRule.action(), runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
    EXPECT_EQ(cidrRule.direction(), runtime::v1::NETWORK_DIRECTION_EGRESS);
    EXPECT_EQ(cidrRule.protocol(), runtime::v1::NETWORK_PROTOCOL_TCP);
    EXPECT_EQ(cidrRule.priority(), 110U);
    EXPECT_EQ(cidrRule.peer().cidr(), "192.0.2.0/24");
    EXPECT_EQ(cidrRule.peer().port_range().first(), 80U);
    EXPECT_EQ(cidrRule.peer().port_range().last(), 443U);

    const auto &domainRule = traffic.rules(1);
    EXPECT_EQ(domainRule.action(), runtime::v1::NETWORK_POLICY_ACTION_DENY);
    EXPECT_EQ(domainRule.peer().domain(), "*.example.com");
    EXPECT_EQ(domainRule.sandbox_port_range().first(), 5000U);
    EXPECT_EQ(domainRule.sandbox_port_range().last(), 5010U);

    const auto &proxyRule = traffic.rules(2);
    EXPECT_EQ(proxyRule.priority(), std::numeric_limits<uint32_t>::max());
    EXPECT_EQ(proxyRule.direction(), runtime::v1::NETWORK_DIRECTION_BOTH);
    EXPECT_EQ(proxyRule.peer().cidr(), "10.20.30.40/32");
    EXPECT_EQ(proxyRule.peer().port_range().first(), 31222U);
    EXPECT_EQ(proxyRule.peer().port_range().last(), 31222U);

    const auto &publishedRule = traffic.rules(3);
    EXPECT_EQ(publishedRule.priority(), std::numeric_limits<uint32_t>::max());
    EXPECT_EQ(publishedRule.direction(), runtime::v1::NETWORK_DIRECTION_INGRESS);
    EXPECT_EQ(publishedRule.sandbox_port_range().first(), 8766U);
    EXPECT_EQ(publishedRule.sandbox_port_range().last(), 8766U);

    ASSERT_TRUE(policy.has_dns());
    EXPECT_EQ(policy.dns().default_action(), runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
    ASSERT_EQ(policy.dns().rules_size(), 1);
    EXPECT_EQ(policy.dns().rules(0).action(), runtime::v1::NETWORK_POLICY_ACTION_DENY);
    EXPECT_EQ(policy.dns().rules(0).pattern(), "blocked.example");
}

TEST_F(SandboxdRequestBuilderTest, ACLVersion2SupportsIndependentDefaultsAndStatelessMode)
{
    auto params = MakeMinimalParams();
    (*params.request->mutable_runtimeinstanceinfo()
          ->mutable_deploymentconfig()
          ->mutable_deployoptions())["network_policy"] = R"({
        "schemaVersion": 2,
        "traffic": {
            "ingressDefaultAction": "deny",
            "egressDefaultAction": "allow",
            "mode": "stateless",
            "rules": []
        }
    })";

    auto [status, startReq] = builder_->Build(params);

    ASSERT_TRUE(status.IsOk());
    ASSERT_NE(startReq, nullptr);
    const auto &traffic = startReq->network_policy().traffic();
    EXPECT_EQ(traffic.ingress_default_action(), runtime::v1::NETWORK_POLICY_ACTION_DENY);
    EXPECT_EQ(traffic.egress_default_action(), runtime::v1::NETWORK_POLICY_ACTION_ALLOW);
    EXPECT_EQ(traffic.mode(), runtime::v1::TRAFFIC_POLICY_MODE_STATELESS);
    ASSERT_EQ(traffic.rules_size(), 1);
    EXPECT_EQ(traffic.rules(0).priority(), std::numeric_limits<uint32_t>::max());
}

TEST_F(SandboxdRequestBuilderTest, ACLVersion2ReservesCapacityForProtectedRules)
{
    const auto makePolicy = [](size_t ruleCount) {
        std::string policy =
            R"({"schemaVersion":2,"traffic":{"ingressDefaultAction":"deny","egressDefaultAction":"deny","rules":[)";
        const std::string rule = R"({"action":"allow","direction":"egress","protocol":"tcp"})";
        for (size_t index = 0; index < ruleCount; ++index) {
            if (index != 0) {
                policy += ',';
            }
            policy += rule;
        }
        policy += "]}}";
        return policy;
    };

    {
        auto params = MakeMinimalParams();
        (*params.request->mutable_runtimeinstanceinfo()
              ->mutable_deploymentconfig()
              ->mutable_deployoptions())["network_policy"] = makePolicy(255);

        auto [status, startReq] = builder_->Build(params);

        ASSERT_TRUE(status.IsOk());
        ASSERT_NE(startReq, nullptr);
        EXPECT_EQ(startReq->network_policy().traffic().rules_size(), 256);
    }

    {
        auto params = MakeMinimalParams();
        (*params.request->mutable_runtimeinstanceinfo()
              ->mutable_deploymentconfig()
              ->mutable_deployoptions())["network_policy"] = makePolicy(256);

        auto [status, startReq] = builder_->Build(params);

        EXPECT_FALSE(status.IsOk());
        EXPECT_EQ(startReq, nullptr);
    }

    {
        auto params = MakeMinimalParams();
        params.portMappings = { "tcp:21010:8766" };
        (*params.request->mutable_runtimeinstanceinfo()
              ->mutable_deploymentconfig()
              ->mutable_deployoptions())["network_policy"] = makePolicy(255);

        auto [status, startReq] = builder_->Build(params);

        EXPECT_FALSE(status.IsOk());
        EXPECT_EQ(startReq, nullptr);
    }
}

TEST_F(SandboxdRequestBuilderTest, InvalidACLVersion2FailsBuild)
{
    const std::vector<std::string> invalidPolicies = {
        R"({"schemaVersion":2,"blockNetwork":true})",
        R"({"schemaVersion":3})",
        R"({"schemaVersion":2,"unknown":true})",
        R"({"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny"}})",
        R"({"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","rules":[{"action":"allow","direction":"ingress","protocol":"tcp","peer":{"domain":"example.com"}}]}})",
        R"({"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","rules":[{"action":"allow","direction":"egress","protocol":"any","peer":{"portRange":{"first":443,"last":443}}}]}})",
        R"({"schemaVersion":2,"traffic":{"ingressDefaultAction":"allow","egressDefaultAction":"deny","rules":[{"action":"allow","direction":"egress","protocol":"tcp","priority":4294967295}]}})",
        R"({"schemaVersion":2,"dns":{"defaultAction":"allow","rules":[{"action":"deny","pattern":"example.com.."}]}})",
    };

    for (const auto &policy : invalidPolicies) {
        auto params = MakeMinimalParams();
        (*params.request->mutable_runtimeinstanceinfo()
              ->mutable_deploymentconfig()
              ->mutable_deployoptions())["network_policy"] = policy;

        auto [status, startReq] = builder_->Build(params);

        EXPECT_FALSE(status.IsOk()) << policy;
        EXPECT_EQ(startReq, nullptr) << policy;
    }
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
          ->mutable_deployoptions())["network_policy"] = R"({"blockNetwork":true,"dnsBlacklist":["github.com"]})";

    auto [status, startReq] = builder_->Build(params);

    EXPECT_FALSE(status.IsOk());
    EXPECT_EQ(startReq, nullptr);
}

TEST_F(SandboxdRequestBuilderTest, DynamicNetworkPolicyUsesSharedCompilerAndClears)
{
    const auto policyJSON = R"({
        "schemaVersion": 2,
        "traffic": {
            "ingressDefaultAction": "allow",
            "egressDefaultAction": "deny",
            "mode": "stateful",
            "rules": [{
                "action": "allow",
                "direction": "egress",
                "protocol": "tcp",
                "peer": {"cidr": "10.0.0.0/8"},
                "priority": 100
            }]
        }
    })";
    runtime::v1::NetworkPolicy policy;
    auto status = builder_->BuildNetworkPolicy(
        policyJSON,
        { "tcp:0:8080" }, &policy);

    ASSERT_TRUE(status.IsOk()) << status.RawMessage();
    ASSERT_EQ(policy.schema_version(), 2U);
    ASSERT_TRUE(policy.has_traffic());
    ASSERT_EQ(policy.traffic().rules_size(), 3);
    EXPECT_EQ(policy.traffic().rules(1).priority(), std::numeric_limits<uint32_t>::max());
    EXPECT_EQ(policy.traffic().rules(2).sandbox_port_range().first(), 8080U);

    status = builder_->BuildNetworkPolicy("{}", {}, &policy);

    ASSERT_TRUE(status.IsOk()) << status.RawMessage();
    EXPECT_EQ(policy.ByteSizeLong(), 0U);
}

}  // namespace functionsystem::test
