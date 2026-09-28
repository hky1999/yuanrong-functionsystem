// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// REAL behavior tests for named-restore ceiling admission. All tests call
// production code. Pattern sources: usage_aware_filter_test.cpp:79,
// sandboxd_request_builder_test.cpp:170, resource_view_test.cpp:400.
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <chrono>
#include <memory>
#include <string>

#include "common/resource_view/restore_admission.h"
#include "common/resource_view/view_utils.h"
#include "common/schedule_plugin/filter/usage_aware_filter/usage_aware_filter.h"
#include "common/schedule_plugin/common/preallocated_context.h"
#include "runtime_manager/executor/sandboxd/sandboxd_request_builder.h"

namespace functionsystem::test {
using namespace functionsystem::resource_view;
using namespace functionsystem::schedule_plugin;
using namespace functionsystem::schedule_framework;  // PreAllocatedContext
using namespace functionsystem::runtime_manager;
using namespace functionsystem::test::view_utils;

namespace {
constexpr const char *TRUSTED_EXT = "yr.internal.reusable_snapshot.trusted_restore";
constexpr const char *ELASTIC_KEY = "scheduler_elastic_resources";

ResourceUnit MakeUnit(double memCap, double memUsed)
{
    auto unit = Get1DResourceUnit();
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    unit.set_memory_sample_unix_ms(nowMs);
    unit.set_cpu_sample_unix_ms(nowMs);
    unit.mutable_capacity()->mutable_resources()->at(RESOURCE_MEM_NAME).mutable_scalar()->set_value(memCap);
    unit.mutable_actualuse()->mutable_resources()->at(RESOURCE_MEM_NAME).mutable_scalar()->set_value(memUsed);
    unit.mutable_capacity()->mutable_resources()->at(RESOURCE_CPU_NAME).mutable_scalar()->set_value(16000);
    unit.mutable_actualuse()->mutable_resources()->at(RESOURCE_CPU_NAME).mutable_scalar()->set_value(0);
    return unit;
}

InstanceInfo MakeElasticInstance(double initial, double ceiling, bool pending, bool trusted)
{
    auto inst = Get1DInstance();
    auto &mem = (*inst.mutable_resources()->mutable_resources())["Memory"];
    mem.mutable_scalar()->set_value(initial);
    mem.mutable_scalar()->set_limit(ceiling);
    auto &cpu = (*inst.mutable_resources()->mutable_resources())["CPU"];
    cpu.mutable_scalar()->set_value(500);
    cpu.mutable_scalar()->set_limit(500);
    (*inst.mutable_createoptions())[ELASTIC_KEY] = "true";
    inst.set_resource_admission_pending(pending);
    if (trusted) {
        (*inst.mutable_scheduleoption()->mutable_extension())[TRUSTED_EXT] = "test";
    }
    return inst;
}

resources::SandboxResourceSample MakeSample(const std::string &sandboxId, const std::string &runtimeId,
                                             const std::string &gen, uint64_t limitBytes,
                                             bool usageKnown = true, uint64_t usageBytes = 0,
                                             bool revisionKnown = true)
{
    resources::SandboxResourceSample s;
    s.set_sandbox_id(sandboxId);
    s.set_runtime_id(runtimeId);
    s.set_resource_generation(gen);
    s.set_memory_limit_bytes(limitBytes);
    s.set_memory_limit_known(true);
    s.set_cpu_limits_known(true);
    s.set_cpu_quota_usec(50000);
    s.set_cpu_period_usec(100000);
    s.set_resource_revision_known(revisionKnown);
    s.set_resource_revision(1);
    s.set_memory_usage_known(usageKnown);
    s.set_memory_current_bytes(usageBytes);
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    s.set_sampled_at_unix_ms(nowMs);
    return s;
}

}  // namespace

// ===================== FILTER =====================
class NamedRestoreFilterTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        filter::UsageAwareFilter::SetConfig(0.9, 2048, 0);
    }
};

TEST_F(NamedRestoreFilterTest, NamedRestoreRejectedWhenCeilingExceedsSafetyLine)
{
    filter::UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(1000, 600);  // 600+512 > 900(safety), 600+128 < 900
    auto restoreInst = MakeElasticInstance(128, 512, false, true);
    auto out = filter.Filter(ctx, restoreInst, unit);
    EXPECT_EQ(out.status, StatusCode::RESOURCE_NOT_ENOUGH);
}

TEST_F(NamedRestoreFilterTest, NormalElasticPassesWithSameBudget)
{
    filter::UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(1000, 600);
    auto normalInst = MakeElasticInstance(128, 512, false, false);
    auto out = filter.Filter(ctx, normalInst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
}

TEST_F(NamedRestoreFilterTest, NamedRestorePassesWhenBudgetSufficient)
{
    filter::UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(2000, 600);  // 600+512 = 1112 < 1800
    auto restoreInst = MakeElasticInstance(128, 512, false, true);
    auto out = filter.Filter(ctx, restoreInst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
}

// ===================== REMEMBER =====================
TEST(NamedRestoreRememberTest, NamedRestoreRemembered)
{
    PreAllocatedContext ctx;
    auto inst = MakeElasticInstance(128, 512, false, true);
    ctx.RememberRestoreMemory(inst);
    const auto id = inst.instanceid();  // random UUID — must use actual
    EXPECT_EQ(ctx.preAllocatedRestoreMemory.count(id), 1u);
    const auto &mem = ctx.preAllocatedRestoreMemory.at(id);
    ASSERT_TRUE(mem.has_scalar());
    EXPECT_EQ(mem.scalar().value(), 128);
    EXPECT_EQ(mem.scalar().limit(), 512);
}

TEST(NamedRestoreRememberTest, NormalElasticNotRemembered)
{
    PreAllocatedContext ctx;
    auto inst = MakeElasticInstance(128, 512, false, false);
    ctx.RememberRestoreMemory(inst);
    EXPECT_EQ(ctx.preAllocatedRestoreMemory.count(inst.instanceid()), 0u);
}

// ===================== BUILDER =====================
class NamedRestoreBuilderTest : public ::testing::Test {
public:
    void SetUp() override
    {
        RuntimeConfig config;
        config.runtimePath = "/opt/runtime";
        config.runtimeLogLevel = "INFO";
        config.runtimeConfigPath = "/etc/runtime";
        config.runtimeLogPath = "/tmp/nr-builder-test";
        config.hostIP = "127.0.0.1";
        config.proxyIP = "10.20.30.40";
        config.proxyGrpcServerPort = "31222";
        cmdBuilder_ = std::make_unique<CommandBuilder>(false);
        cmdBuilder_->SetRuntimeConfig(config);
        builder_ = std::make_unique<SandboxdRequestBuilder>(*cmdBuilder_);
    }
    SandboxdStartParams MakeElasticParams(double initialMiB, double ceilingMiB,
                                           const std::string &snapID = "")
    {
        auto req = std::make_shared<messages::StartInstanceRequest>();
        auto *info = req->mutable_runtimeinstanceinfo();
        info->set_instanceid("nr-test");
        info->set_runtimeid("nr-runtime");
        info->mutable_runtimeconfig()->set_language("python3.9");
        info->mutable_deploymentconfig()->set_deploydir("/dcache");
        info->set_traceid("nr-trace");
        info->mutable_container()->set_id("nr-container");
        info->mutable_container()->set_runtime("runsc");
        auto *rootfs = info->mutable_container()->mutable_rootfsconfig();
        rootfs->set_type(runtime::v1::RootfsSrcType::LOCAL);
        rootfs->set_path("/opt/runtime/rootfs.img");
        rootfs->set_readonly(false);
        if (!snapID.empty()) {
            info->set_restoresnapshotid(snapID);
        }
        auto *resources = info->mutable_runtimeconfig()->mutable_resources()->mutable_resources();
        for (const auto &name : {"CPU", "Memory"}) {
            auto &r = (*resources)[name];
            r.set_type(::resources::Value_Type_SCALAR);
            r.mutable_scalar()->set_value(std::string(name) == "CPU" ? 500 : initialMiB);
            r.mutable_scalar()->set_limit(std::string(name) == "CPU" ? 500 : ceilingMiB);
        }
        (*info->mutable_deploymentconfig()->mutable_deployoptions())[ELASTIC_KEY] = "true";
        SandboxdStartParams params;
        params.request = req;
        params.runtimeID = "nr-runtime";
        return params;
    }
    std::unique_ptr<CommandBuilder> cmdBuilder_;
    std::unique_ptr<SandboxdRequestBuilder> builder_;
};

TEST_F(NamedRestoreBuilderTest, PhysicalMemoryIsCeilingForNamedRestore)
{
    auto params = MakeElasticParams(128, 512, "snap-test");
    auto [status, start] = builder_->Build(params);
    ASSERT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(start->resources().at("Memory"), 512);
    EXPECT_EQ(start->resources().at("CPU"), 500);
    const auto policy = nlohmann::json::parse(
        start->labels().at("akernel.scheduler/elastic-resources"));
    EXPECT_EQ(policy["Memory"]["initial"], 128);
    EXPECT_EQ(policy["Memory"]["ceiling"], 512);
}

TEST_F(NamedRestoreBuilderTest, PhysicalMemoryIsInitialForNormalStart)
{
    auto params = MakeElasticParams(128, 512);
    auto [status, start] = builder_->Build(params);
    ASSERT_TRUE(status.IsOk());
    EXPECT_EQ(start->resources().at("Memory"), 128);
}

TEST_F(NamedRestoreBuilderTest, CeilingEqualsInitialAccepted)
{
    auto params = MakeElasticParams(256, 256, "snap-flat");
    auto [status, start] = builder_->Build(params);
    ASSERT_TRUE(status.IsOk());
    EXPECT_EQ(start->resources().at("Memory"), 256);
}

// ===================== ADMISSION =====================
class NamedRestoreAdmissionTest : public ::testing::Test {
protected:
    struct Guard {
        UsageAdmissionSettings saved_;
        Guard() : saved_(GetUsageAdmissionSettings()) { SetUsageAdmissionSettings(0.95, 2048.0, 0); }
        ~Guard() { SetUsageAdmissionSettings(saved_.safety, saved_.floorMb, saved_.maxInstances); }
    } guard_;
};

TEST_F(NamedRestoreAdmissionTest, PendingNamedRestoreRejectedWhenOnlyInitialFits)
{
    auto unit = MakeUnit(1000, 600);
    auto restore = MakeElasticInstance(128, 512, true, true);
    EXPECT_FALSE(CheckRestoreAllocation(restore, unit, {}).IsOk());
}
TEST_F(NamedRestoreAdmissionTest, NormalElasticAdmitted)
{
    auto unit = MakeUnit(1000, 600);
    auto normal = MakeElasticInstance(128, 512, true, false);
    ASSERT_TRUE(UsesElasticAdmission(normal));
    EXPECT_TRUE(CheckRestoreAllocation(normal, unit, {}).IsOk());
}
TEST_F(NamedRestoreAdmissionTest, ReleasedRestoreAdmittedAtInitial)
{
    auto unit = MakeUnit(1000, 600);
    auto released = MakeElasticInstance(128, 512, false, true);
    ASSERT_TRUE(UsesElasticAdmission(released));
    EXPECT_TRUE(CheckRestoreAllocation(released, unit, {}).IsOk());
}
TEST_F(NamedRestoreAdmissionTest, SufficientBudgetAdmitsCeiling)
{
    auto unit = MakeUnit(1000, 400);
    auto restore = MakeElasticInstance(128, 512, true, true);
    EXPECT_TRUE(CheckRestoreAllocation(restore, unit, {}).IsOk());
}

// ===================== ACTOR BASELINE HANDOFF =====================
// These construct a real ResourceViewActor (via ResourceView facade), inject
// a restore instance with a physical sample, and call the ACTUAL
// EstablishRecoveryBaseline through the facade's implActor_ (accessible via
// -fno-access-control). They verify the return value, the resizeBudget_
// contents, and EntryOutstanding.

class NamedRestoreBaselineTest : public ::testing::Test {
protected:
    const ResourceViewActor::Param param_ {.isLocal = true,
        .enableTenantAffinity = false, .tenantPodReuseTimeWindow = 1};
};

// limit==initial(128): NotNeeded — no extra credit required, release is safe.
TEST_F(NamedRestoreBaselineTest, LimitEqualsFloorReturnsNotNeeded)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-floor", "baseline-floor-id", param_);
    auto inst = MakeElasticInstance(128, 128, true, false);
    inst.set_runtimeid("rt-1");
    auto sample = MakeSample("sbx-1", "rt-1", "gen-1", 128 * 1024 * 1024);
    auto result = actor->EstablishRecoveryBaseline("unit-1", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::NotNeeded);
}

// Unknown revision: Deferred — keep the admission reservation.
TEST_F(NamedRestoreBaselineTest, UnknownRevisionReturnsDeferred)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-unknown", "baseline-unknown-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-2");
    auto sample = MakeSample("sbx-2", "rt-2", "gen-2", 512 * 1024 * 1024,
                              true, 100 * 1024 * 1024, /*revisionKnown=*/false);
    auto result = actor->EstablishRecoveryBaseline("unit-2", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Deferred);
}

// cap=512MiB, usage=100MiB → baseline holds 412MiB credit → Established.
TEST_F(NamedRestoreBaselineTest, Cap512Usage100Establishes412)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-412", "baseline-412-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-3");
    auto sample = MakeSample("sbx-3", "rt-3", "gen-3", 512 * 1024 * 1024,
                              true, 100 * 1024 * 1024);
    auto result = actor->EstablishRecoveryBaseline("unit-3", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Established);
    // Verify the entry exists and holds the right credit
    bool found = false;
    for (const auto &[key, entry] : actor->resizeBudget_) {
        if (entry.request.instanceid() == inst.instanceid()
            && entry.request.resize().resource_generation() == "gen-3") {
            found = true;
            const auto outstanding = actor->EntryOutstanding(entry);
            // credit = (512 - max(100, 100)) = 412 MiB
            EXPECT_NEAR(outstanding.memoryMb, 412.0, 0.5);
            break;
        }
    }
    EXPECT_TRUE(found);
}

// Unknown usage → floor=0 → credit = 512MiB (full cap held).
TEST_F(NamedRestoreBaselineTest, UnknownUsageHoldsFullCap)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-full", "baseline-full-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-4");
    auto sample = MakeSample("sbx-4", "rt-4", "gen-4", 512 * 1024 * 1024,
                              /*usageKnown=*/false, 0);
    auto result = actor->EstablishRecoveryBaseline("unit-4", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Established);
    bool found = false;
    for (const auto &[key, entry] : actor->resizeBudget_) {
        if (entry.request.instanceid() == inst.instanceid()) {
            found = true;
            const auto outstanding = actor->EntryOutstanding(entry);
            // credit = (512 - max(0, 0)) = 512 MiB
            EXPECT_NEAR(outstanding.memoryMb, 512.0, 0.5);
            break;
        }
    }
    ASSERT_TRUE(found) << "entry must exist for the established baseline";
}

// Second call with same identity → idempotent Established (same entry found).
TEST_F(NamedRestoreBaselineTest, SameIdentitySecondCallIdempotent)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-idem", "baseline-idem-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-5");
    auto sample = MakeSample("sbx-5", "rt-5", "gen-5", 512 * 1024 * 1024,
                              true, 100 * 1024 * 1024);
    auto first = actor->EstablishRecoveryBaseline("unit-5", inst, sample);
    ASSERT_EQ(first, ResourceViewActor::RecoveryBaselineResult::Established);
    auto second = actor->EstablishRecoveryBaseline("unit-5", inst, sample);
    EXPECT_EQ(second, ResourceViewActor::RecoveryBaselineResult::Established);
}

}  // namespace functionsystem::test
