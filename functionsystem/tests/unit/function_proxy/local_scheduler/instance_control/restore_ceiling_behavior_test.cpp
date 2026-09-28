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
#include <cstring>
#include <memory>
#include <string>
#include <thread>

#include "common/resource_view/memory_elasticity_fence.h"
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



// ===================== BASELINE NEGATIVE CASES =====================
// These exercise the coverage check's rejection paths with REAL actor state.

// Pre-existing entry with insufficient credit (1MiB) against required 412MiB
// → Deferred (keep the admission reservation).
TEST_F(NamedRestoreBaselineTest, InsufficientCredit1MiBReturnsDeferred)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-insuf1", "baseline-insuf1-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-insuf1");
    auto sample = MakeSample("sbx-insuf1", "rt-insuf1", "gen-insuf1", 512 * 1024 * 1024,
                              true, 100 * 1024 * 1024);
    // Manually create a pre-existing entry with 1MiB credit (cap=512, expected=511)
    ResizeBudgetEntry stub;
    stub.unitID = "unit-insuf1";
    stub.request.set_requestid("recovery-baseline:" + inst.instanceid() + ":gen-insuf1");
    stub.request.set_instanceid(inst.instanceid());
    stub.request.set_runtimeid("rt-insuf1");
    stub.request.mutable_resize()->set_sandbox_id("sbx-insuf1");
    stub.request.mutable_resize()->set_resource_generation("gen-insuf1");
    stub.landed.set_memory_limit_bytes(512 * 1024 * 1024);
    stub.request.mutable_resize()->mutable_expected()->set_memory_limit_bytes(511 * 1024 * 1024);
    stub.capMemoryBytes = 512 * 1024 * 1024;
    stub.peakMemoryBytes = 0;
    stub.observed = true;
    stub.expectedRevision = 1;
    stub.confirmedRevision = 1;
    actor->resizeBudget_.emplace(stub.request.requestid(), std::move(stub));

    // Required credit = 512 - max(100, 0) = 412MiB. Existing entry credit
    // = 512 - max(511, 0) = 1MiB. 1 < 412 → Deferred.
    auto result = actor->EstablishRecoveryBaseline("unit-insuf1", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Deferred);
}

// Pre-existing entry with 384MiB credit < required 412MiB → Deferred.
TEST_F(NamedRestoreBaselineTest, InsufficientCredit384MiBReturnsDeferred)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-insuf384", "baseline-insuf384-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-insuf384");
    auto sample = MakeSample("sbx-insuf384", "rt-insuf384", "gen-insuf384", 512 * 1024 * 1024,
                              true, 100 * 1024 * 1024);
    // Pre-existing entry with 384MiB credit (cap=512, expected=128=initial)
    ResizeBudgetEntry stub;
    stub.unitID = "unit-insuf384";
    stub.request.set_requestid("recovery-baseline:" + inst.instanceid() + ":gen-insuf384");
    stub.request.set_instanceid(inst.instanceid());
    stub.request.set_runtimeid("rt-insuf384");
    stub.request.mutable_resize()->set_sandbox_id("sbx-insuf384");
    stub.request.mutable_resize()->set_resource_generation("gen-insuf384");
    stub.landed.set_memory_limit_bytes(512 * 1024 * 1024);
    stub.request.mutable_resize()->mutable_expected()->set_memory_limit_bytes(128 * 1024 * 1024);
    stub.capMemoryBytes = 512 * 1024 * 1024;
    stub.peakMemoryBytes = 0;
    stub.observed = true;
    stub.expectedRevision = 1;
    stub.confirmedRevision = 1;
    actor->resizeBudget_.emplace(stub.request.requestid(), std::move(stub));

    // Required = 412MiB. Existing credit = 512 - max(128, 0) = 384MiB. 384 < 412 → Deferred.
    auto result = actor->EstablishRecoveryBaseline("unit-insuf384", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Deferred);
}

// Wrong sandbox identity: same instanceid+generation but different sandbox_id
// → the loop continues (wrong identity), then the new entry has the same
// deterministic key → emplace conflict → Deferred (not phantom Established).
TEST_F(NamedRestoreBaselineTest, WrongSandboxIdentityCausesKeyConflictDeferred)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-wrongsbx", "baseline-wrongsbx-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-wrongsbx");
    // Pre-existing entry with the SAME instanceid+generation but a DIFFERENT sandbox
    ResizeBudgetEntry stub;
    stub.unitID = "unit-wrongsbx";
    stub.request.set_requestid("recovery-baseline:" + inst.instanceid() + ":gen-wrongsbx");
    stub.request.set_instanceid(inst.instanceid());
    stub.request.set_runtimeid("rt-wrongsbx");
    stub.request.mutable_resize()->set_sandbox_id("sbx-OTHER");  // wrong identity
    stub.request.mutable_resize()->set_resource_generation("gen-wrongsbx");
    stub.landed.set_memory_limit_bytes(512 * 1024 * 1024);
    stub.request.mutable_resize()->mutable_expected()->set_memory_limit_bytes(0);
    stub.capMemoryBytes = 512 * 1024 * 1024;
    stub.peakMemoryBytes = 0;
    stub.observed = true;
    stub.expectedRevision = 1;
    stub.confirmedRevision = 1;
    actor->resizeBudget_.emplace(stub.request.requestid(), std::move(stub));

    // Sample with the CORRECT sandbox → loop continues past the wrong entry,
    // then tries to create a new entry with the same deterministic key →
    // emplace fails → Deferred
    auto sample = MakeSample("sbx-correct", "rt-wrongsbx", "gen-wrongsbx", 512 * 1024 * 1024,
                              true, 100 * 1024 * 1024);
    auto result = actor->EstablishRecoveryBaseline("unit-wrongsbx", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Deferred);
}

// Wrong unit: entry has a different unitID → loop continues, key conflict → Deferred.
TEST_F(NamedRestoreBaselineTest, WrongUnitDeferred)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-wrongunit", "baseline-wrongunit-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-wu");
    ResizeBudgetEntry stub;
    stub.unitID = "unit-OTHER";  // wrong unit
    stub.request.set_requestid("recovery-baseline:" + inst.instanceid() + ":gen-wu");
    stub.request.set_instanceid(inst.instanceid());
    stub.request.set_runtimeid("rt-wu");
    stub.request.mutable_resize()->set_sandbox_id("sbx-wu");
    stub.request.mutable_resize()->set_resource_generation("gen-wu");
    stub.landed.set_memory_limit_bytes(512 * 1024 * 1024);
    stub.request.mutable_resize()->mutable_expected()->set_memory_limit_bytes(0);
    stub.capMemoryBytes = 512 * 1024 * 1024;
    stub.peakMemoryBytes = 0;
    stub.observed = true;
    stub.expectedRevision = 1;
    stub.confirmedRevision = 1;
    actor->resizeBudget_.emplace(stub.request.requestid(), std::move(stub));

    auto sample = MakeSample("sbx-wu", "rt-wu", "gen-wu", 512 * 1024 * 1024, true, 100 * 1024 * 1024);
    auto result = actor->EstablishRecoveryBaseline("unit-CORRECT", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Deferred);
}

// Unconfirmed (not observed) entry with matching identity → Deferred.
TEST_F(NamedRestoreBaselineTest, UnconfirmedEntryDeferred)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-unconf", "baseline-unconf-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-uc");
    ResizeBudgetEntry stub;
    stub.unitID = "unit-uc";
    stub.request.set_requestid("recovery-baseline:" + inst.instanceid() + ":gen-uc");
    stub.request.set_instanceid(inst.instanceid());
    stub.request.set_runtimeid("rt-uc");
    stub.request.mutable_resize()->set_sandbox_id("sbx-uc");
    stub.request.mutable_resize()->set_resource_generation("gen-uc");
    stub.observed = false;  // NOT confirmed
    actor->resizeBudget_.emplace(stub.request.requestid(), std::move(stub));

    auto sample = MakeSample("sbx-uc", "rt-uc", "gen-uc", 512 * 1024 * 1024, true, 100 * 1024 * 1024);
    auto result = actor->EstablishRecoveryBaseline("unit-uc", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Deferred);
}


// ===================== ADDITIONAL BASELINE NEGATIVES =====================

// Wrong runtime: same instanceid+generation but different runtime_id
TEST_F(NamedRestoreBaselineTest, WrongRuntimeIdentityDeferred)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-wrongrt", "baseline-wrongrt-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-correct");
    // Pre-existing entry with WRONG runtime
    ResizeBudgetEntry stub;
    stub.unitID = "unit-wrongrt";
    stub.request.set_requestid("recovery-baseline:" + inst.instanceid() + ":gen-wrongrt");
    stub.request.set_instanceid(inst.instanceid());
    stub.request.set_runtimeid("rt-OTHER");  // wrong runtime
    stub.request.mutable_resize()->set_sandbox_id("sbx-wrongrt");
    stub.request.mutable_resize()->set_resource_generation("gen-wrongrt");
    stub.landed.set_memory_limit_bytes(512 * 1024 * 1024);
    stub.request.mutable_resize()->mutable_expected()->set_memory_limit_bytes(0);
    stub.capMemoryBytes = 512 * 1024 * 1024;
    stub.observed = true;
    actor->resizeBudget_.emplace(stub.request.requestid(), std::move(stub));

    auto sample = MakeSample("sbx-wrongrt", "rt-correct", "gen-wrongrt", 512 * 1024 * 1024, true, 100 * 1024 * 1024);
    auto result = actor->EstablishRecoveryBaseline("unit-wrongrt", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Deferred);
}

// Entry with matching identity and sufficient credit (512MiB against the
// 412MiB requirement), but confirmed at revision 2 while the sample carries
// revision 1. The entry describes a different physical fact than the sample;
// the coverage check must NOT release the admission against it → Deferred.
TEST_F(NamedRestoreBaselineTest, RevisionMismatchDeferred)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-revmis", "baseline-revmis-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-revmis");
    ResizeBudgetEntry stub;
    stub.unitID = "unit-revmis";
    stub.request.set_requestid("recovery-baseline:" + inst.instanceid() + ":gen-revmis");
    stub.request.set_instanceid(inst.instanceid());
    stub.request.set_runtimeid("rt-revmis");
    stub.request.mutable_resize()->set_sandbox_id("sbx-revmis");
    stub.request.mutable_resize()->set_resource_generation("gen-revmis");
    stub.landed.set_memory_limit_bytes(512 * 1024 * 1024);
    stub.request.mutable_resize()->mutable_expected()->set_memory_limit_bytes(0);
    stub.capMemoryBytes = 512 * 1024 * 1024;
    stub.observed = true;
    stub.expectedRevision = 2;   // entry expects revision 2
    stub.confirmedRevision = 2;  // confirmed at revision 2
    actor->resizeBudget_.emplace(stub.request.requestid(), std::move(stub));

    // MakeSample carries revision=1. Credit alone would pass (512 >= 412);
    // the revision gate must keep the reservation (Deferred), because the
    // entry was confirmed at revision 2, not at this sample's revision 1.
    auto sample = MakeSample("sbx-revmis", "rt-revmis", "gen-revmis", 512 * 1024 * 1024, true, 100 * 1024 * 1024);
    auto result = actor->EstablishRecoveryBaseline("unit-revmis", inst, sample);
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Deferred);
}

// Existing entry with sufficient credit AND matching identity → Established (idempotent).
// This is the positive version of the wrong-identity tests.
TEST_F(NamedRestoreBaselineTest, CorrectIdentitySufficientCreditEstablished)
{
    auto actor = std::make_shared<ResourceViewActor>("baseline-ok-idem", "baseline-ok-idem-id", param_);
    auto inst = MakeElasticInstance(128, 512, true, false);
    inst.set_runtimeid("rt-ok");
    // Pre-existing entry with CORRECT identity and sufficient credit (512 - 100 = 412)
    ResizeBudgetEntry stub;
    stub.unitID = "unit-ok";
    stub.request.set_requestid("recovery-baseline:" + inst.instanceid() + ":gen-ok");
    stub.request.set_instanceid(inst.instanceid());
    stub.request.set_runtimeid("rt-ok");
    stub.request.mutable_resize()->set_sandbox_id("sbx-ok");
    stub.request.mutable_resize()->set_resource_generation("gen-ok");
    stub.landed.set_memory_limit_bytes(512 * 1024 * 1024);
    stub.request.mutable_resize()->mutable_expected()->set_memory_limit_bytes(100 * 1024 * 1024);
    stub.capMemoryBytes = 512 * 1024 * 1024;
    stub.peakMemoryBytes = 100 * 1024 * 1024;
    stub.observed = true;
    stub.expectedRevision = 1;
    stub.confirmedRevision = 1;
    actor->resizeBudget_.emplace(stub.request.requestid(), std::move(stub));

    auto sample = MakeSample("sbx-ok", "rt-ok", "gen-ok", 512 * 1024 * 1024, true, 100 * 1024 * 1024);
    auto result = actor->EstablishRecoveryBaseline("unit-ok", inst, sample);
    // Required credit = 512 - 100 = 412. Existing credit = 512 - max(100, 100) = 412. 412 >= 412 → Established.
    EXPECT_EQ(result, ResourceViewActor::RecoveryBaselineResult::Established);
}

// ===================== PUBLISH (UpdateResourceUnitActual) =====================
// Real actor + real view: two Stats rounds through UpdateResourceUnitActual
// drive the admission→baseline handoff exactly as the collector delivers it.
// Round 1 (no physical sample yet) registers the admission window; round 2
// carries the restore-time sample. Established clears resource_admission_
// pending and holds the credit in a recovery-baseline entry; Deferred keeps
// the reservation and still syncs the top-level instance index.
class NamedRestorePublishTest : public ::testing::Test {
protected:
    void SetUp() override { SetMemoryElasticityFenceForTest(true); }
    void TearDown() override { ResetMemoryElasticityFenceOverride(); }
    const ResourceViewActor::Param param_ {.isLocal = true,
        .enableTenantAffinity = false, .tenantPodReuseTimeWindow = 1};

    static int64_t NowMs()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    // Actor with a real view: fragment unit + top-level instance index entry.
    // ONE instance object with a deterministic identity is copied into both
    // indexes (two MakeElasticInstance calls would produce different random
    // instanceids, disagreeing with the map key and with each other).
    // The view-level actualuse rollup starts equal to the unit's actualuse or
    // the subtraction guard rejects every later report.
    std::shared_ptr<ResourceViewActor> MakeViewActor(double initial, double ceiling, bool trusted)
    {
        auto actor = std::make_shared<ResourceViewActor>("publish", "publish-view", param_);
        actor->view_ = std::make_shared<ResourceUnit>(InitResource("publish-view"));
        InstanceInfo inst = MakeElasticInstance(initial, ceiling, true, trusted);
        inst.set_instanceid("publish-inst-1");
        inst.set_runtimeid("rt-pub");
        inst.set_unitid("unit-pub");
        auto unit = MakeUnit(2000, 600);
        unit.set_id("unit-pub");
        (*unit.mutable_instances())["publish-inst-1"] = inst;
        (*actor->view_->mutable_fragment())["unit-pub"] = unit;
        (*actor->view_->mutable_instances())["publish-inst-1"] = inst;
        (*actor->view_->mutable_actualuse()) = unit.actualuse();
        return actor;
    }

    // A collector report for the unit; withSample adds the physical reading.
    ResourceUnit MakeReport(int64_t atMs, bool withSample, bool limitKnown = true,
                            uint64_t limitBytes = 512ULL * 1024 * 1024)
    {
        auto report = MakeUnit(2000, 600);
        report.set_id("unit-pub");
        report.set_memory_sample_unix_ms(atMs);
        report.set_cpu_sample_unix_ms(atMs);
        auto &reported = (*report.mutable_instances())["publish-inst-1"];  // map operator[] is a reference
        reported.set_instanceid("publish-inst-1");
        reported.set_runtimeid("rt-pub");
        if (withSample) {
            auto sample = MakeSample("sbx-pub", "rt-pub", "gen-pub", limitBytes,
                                     true, 100 * 1024 * 1024);
            sample.set_cpu_usage_known(true);
            sample.set_cpu_usage_cores(0.05);
            sample.set_sampled_at_unix_ms(atMs - 100);
            if (!limitKnown) { sample.set_memory_limit_known(false); }
            *reported.mutable_sandbox_resource_sample() = sample;
        }
        return report;
    }
};

// Round 2 with a complete sample: baseline Established → the admission flag
// clears, the window entry is erased, and the credit lands in a synthetic
// recovery-baseline budget entry instead of vanishing. The generated entry
// must carry EXACTLY this instance's identity.
TEST_F(NamedRestorePublishTest, SecondStatsRoundClearsPendingWhenBaselineEstablished)
{
    auto actor = MakeViewActor(128, 512, /*trusted=*/true);
    actor->UpdateResourceUnitActual(std::make_shared<ResourceUnit>(MakeReport(NowMs(), false)));
    // Round 1 registered the admission window; nothing released yet.
    EXPECT_EQ(actor->resourceAdmissionTimes_.count("publish-inst-1"), 1u);
    EXPECT_TRUE(actor->view_->fragment().at("unit-pub").instances().at("publish-inst-1")
        .resource_admission_pending());
    // The release gate needs a sample strictly newer than the admission time.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    actor->UpdateResourceUnitActual(std::make_shared<ResourceUnit>(MakeReport(NowMs(), true)));
    const auto &after = actor->view_->fragment().at("unit-pub").instances().at("publish-inst-1");
    EXPECT_FALSE(after.resource_admission_pending());
    EXPECT_TRUE(after.snapshotinfo().checkpointid().empty());
    EXPECT_EQ(actor->resourceAdmissionTimes_.count("publish-inst-1"), 0u);
    // Top-level index synced (not skipped by the release path).
    EXPECT_FALSE(actor->view_->instances().at("publish-inst-1").resource_admission_pending());
    // The ceiling credit is held by the synthetic baseline entry, not dropped,
    // and the entry's full identity is this instance's incarnation.
    ASSERT_EQ(actor->resizeBudget_.size(), 1u);
    const auto &key = actor->resizeBudget_.begin()->first;
    EXPECT_EQ(key.compare(0, strlen("recovery-baseline:"), "recovery-baseline:"), 0);
    const auto &entry = actor->resizeBudget_.begin()->second;
    EXPECT_EQ(entry.request.instanceid(), "publish-inst-1");
    EXPECT_EQ(entry.request.runtimeid(), "rt-pub");
    EXPECT_EQ(entry.request.resize().sandbox_id(), "sbx-pub");
    EXPECT_EQ(entry.request.resize().resource_generation(), "gen-pub");
    EXPECT_EQ(entry.unitID, "unit-pub");
    EXPECT_NEAR(actor->EntryOutstanding(entry).memoryMb, 412.0, 0.5);
}

// Round 2 with an incomplete sample (memory limit unknown): the baseline
// defers → the pending reservation is RETAINED, no budget entry is created,
// and the top-level sync still runs (Deferred must not skip it).
TEST_F(NamedRestorePublishTest, DeferredBaselineRetainsPendingReservation)
{
    auto actor = MakeViewActor(128, 512, /*trusted=*/true);
    actor->UpdateResourceUnitActual(std::make_shared<ResourceUnit>(MakeReport(NowMs(), false)));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    actor->UpdateResourceUnitActual(
        std::make_shared<ResourceUnit>(MakeReport(NowMs(), true, /*limitKnown=*/false)));
    const auto &after = actor->view_->fragment().at("unit-pub").instances().at("publish-inst-1");
    EXPECT_TRUE(after.resource_admission_pending());
    EXPECT_EQ(actor->resourceAdmissionTimes_.count("publish-inst-1"), 1u);
    EXPECT_TRUE(actor->resizeBudget_.empty());
    // The sample itself was still merged into both indexes.
    EXPECT_EQ(after.sandbox_resource_sample().sandbox_id(), "sbx-pub");
    const auto &top = actor->view_->instances().at("publish-inst-1");
    EXPECT_TRUE(top.resource_admission_pending());
    EXPECT_EQ(top.sandbox_resource_sample().sandbox_id(), "sbx-pub");
}

// Normal start whose ceiling equals the initial request (128/128): the
// sample lands exactly at the floor, the baseline is NotNeeded, and the
// admission flag still clears — with NO budget entry created.
TEST_F(NamedRestorePublishTest, InitialCeilingNotNeededClearsPendingWithoutEntry)
{
    auto actor = MakeViewActor(128, 128, /*trusted=*/false);
    actor->UpdateResourceUnitActual(std::make_shared<ResourceUnit>(MakeReport(NowMs(), false)));
    EXPECT_EQ(actor->resourceAdmissionTimes_.count("publish-inst-1"), 1u);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    // Sample limit == 128MiB floor → NotNeeded → release without any entry.
    actor->UpdateResourceUnitActual(
        std::make_shared<ResourceUnit>(MakeReport(NowMs(), true, /*limitKnown=*/true,
                                                  128ULL * 1024 * 1024)));
    const auto &after = actor->view_->fragment().at("unit-pub").instances().at("publish-inst-1");
    EXPECT_FALSE(after.resource_admission_pending());
    EXPECT_EQ(actor->resourceAdmissionTimes_.count("publish-inst-1"), 0u);
    EXPECT_TRUE(actor->resizeBudget_.empty());
    EXPECT_FALSE(actor->view_->instances().at("publish-inst-1").resource_admission_pending());
}
}  // namespace functionsystem::test
