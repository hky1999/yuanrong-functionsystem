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

#include "common/resource_view/resource_view.h"
#include "common/resource_view/elastic_options.h"
#include "function_proxy/local_scheduler/pressure_monitor/pressure_resize_policy.h"

#include <gtest/gtest.h>

#include <map>
#include <chrono>
#include <thread>
#include "common/schedule_plugin/filter/usage_aware_filter/usage_aware_filter.h"

#include "common/constants/constants.h"
#include "common/metrics/metrics_adapter.h"
#include "common/resource_view/memory_elasticity_fence.h"
#include "common/resource_view/resource_poller.h"
#include "common/resource_view/resource_tool.h"
#include "common/resource_view/usage_trend_tracker.h"
#include "common/types/instance_state.h"
#include "utils/future_test_helper.h"
#include "view_utils.h"
#include "utils/port_helper.h"

#define private public

namespace functionsystem::test {

using namespace functionsystem::resource_view;
using namespace functionsystem::test::view_utils;

const ResourceViewActor::Param CHILD_PARAM {
    .isLocal = true,
    .enableTenantAffinity = true,
    .tenantPodReuseTimeWindow = 1
};

ResourceViewActor::Param PARENT_PARAM {
    .isLocal = false,
    .enableTenantAffinity = true,
    .tenantPodReuseTimeWindow = 1
};

const std::string LOCAL_RESOUCE_VIEW_ID = "resoure_id";
const std::string DOMAIN_RESOUCE_VIEW_ID = "parent";
static const std::string NEED_RECOVER_VIEW = "needRecoverView";

class ResourceViewTest : public ::testing::Test {
protected:
    [[maybe_unused]] static void SetUpTestSuite()
    {
    }

    [[maybe_unused]] static void TearDownTestSuite()
    {
    }

    void SetUp()
    {
        uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
        domainUrl_ = "127.0.0.1:" + std::to_string(port);
    }

    void TearDown()
    {
    }

private:
    std::string domainUrl_;
};

TEST_F(ResourceViewTest, RestoreReservationsSpanSeparateAllocationCalls)
{
    using schedule_plugin::filter::UsageAwareFilter;
    struct ConfigGuard {
        double safety = UsageAwareFilter::GetSafety();
        double floor = UsageAwareFilter::GetFloorMb();
        int32_t cap = UsageAwareFilter::GetMaxInstances();
        ~ConfigGuard() { UsageAwareFilter::SetConfig(safety, floor, cap); }
    } guard;
    UsageAwareFilter::SetConfig(0.9, 2048, 0);
    for (const bool cpuBound : {false, true}) {
        SCOPED_TRACE(cpuBound ? "CPU budget" : "Memory budget");
        auto view = resource_view::ResourceView::CreateResourceView(cpuBound ? "restore-cpu" : "restore-memory", CHILD_PARAM);
        auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count(); };
        auto unit = Get1DResourceUnit(cpuBound ? "restore-cpu-agent" : "restore-memory-agent");
        (*unit.mutable_capacity()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(cpuBound ? 8000 : 32000);
        (*unit.mutable_allocatable()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(cpuBound ? 8000 : 32000);
        (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        unit.set_cpu_sample_unix_ms(nowMs());
        (*unit.mutable_capacity()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(40000);
        (*unit.mutable_allocatable()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(40000);
        (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(cpuBound ? 0 : 24000);
        unit.set_memory_sample_unix_ms(nowMs());
        auto added = view->AddResourceUnit(unit);
        ASSERT_AWAIT_READY(added);
        auto makeRestore = [&](const std::string &id) {
            auto instance = Get1DInstance();
            instance.set_instanceid(id);
            instance.set_requestid("request-" + id);
            instance.set_unitid(unit.id());
            instance.set_runtimeid("runtime-" + id);
            instance.mutable_schedulerchain()->Clear();
            *instance.mutable_schedulerchain()->Add() = unit.id();
            auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
            auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
            cpu->set_value(500);
            cpu->set_limit(4000);
            memory->set_value(512);
            memory->set_limit(8192);
            instance.mutable_snapshotinfo()->set_checkpointid("checkpoint-" + id);
            return instance;
        };
        const auto first = makeRestore("first");
        const auto second = makeRestore("second");
        auto allocate = [&](const resources::InstanceInfo &instance) {
            auto promise = std::make_shared<litebus::Promise<Status>>();
            (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
            return promise->GetFuture();
        };
        auto accepted = allocate(first);
        ASSERT_AWAIT_READY(accepted);
        ASSERT_TRUE(accepted.Get().IsOk());
        auto rejected = allocate(second); // a new call, with no shared scheduling context
        ASSERT_AWAIT_READY(rejected);
        EXPECT_FALSE(rejected.Get().IsOk());
        EXPECT_EQ(rejected.Get().RawMessage().find(cpuBound ? "CPU:" : "Memory:"), size_t{0});
        auto before = view->GetResourceView();
        ASSERT_AWAIT_READY(before);
        ASSERT_EQ(before.Get()->instances().count("first"), size_t{1});
        EXPECT_FALSE(before.Get()->instances().at("first").snapshotinfo().checkpointid().empty());

        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(cpuBound ? 1000 : 25000);
        auto &observed = (*report->mutable_instances())["first"];
        observed.set_runtimeid(first.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(first.runtimeid());
        sample->set_sandbox_id("sandbox-first");
        sample->set_resource_generation("generation-first");
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(false);
        auto incomplete = view->UpdateResourceUnit(std::make_shared<ResourceUnit>(*report), UpdateType::UPDATE_ACTUAL);
        ASSERT_AWAIT_READY(incomplete);
        auto held = view->GetResourceView();
        ASSERT_AWAIT_READY(held);
        EXPECT_FALSE(held.Get()->instances().at("first").snapshotinfo().checkpointid().empty());
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        auto refreshed = view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
        ASSERT_AWAIT_READY(refreshed);
        auto after = view->GetResourceView();
        ASSERT_AWAIT_READY(after);
        EXPECT_TRUE(after.Get()->instances().at("first").snapshotinfo().checkpointid().empty());
        accepted = allocate(second);
        ASSERT_AWAIT_READY(accepted);
        EXPECT_TRUE(accepted.Get().IsOk());
    }
}

TEST_F(ResourceViewTest, ElasticColdReservationsSpanCallsAndRequirePhysicalReconciliation)
{
    using schedule_plugin::filter::UsageAwareFilter;
    struct ConfigGuard {
        double safety = UsageAwareFilter::GetSafety();
        double floor = UsageAwareFilter::GetFloorMb();
        int32_t cap = UsageAwareFilter::GetMaxInstances();
        ~ConfigGuard() { UsageAwareFilter::SetConfig(safety, floor, cap); }
    } guard;
    UsageAwareFilter::SetConfig(0.9, 2048, 0);
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    for (const bool cpuBound : {false, true}) {
        SCOPED_TRACE(cpuBound ? "cold CPU" : "cold Memory");
        const std::string id = cpuBound ? "elastic-cold-cpu" : "elastic-cold-memory";
        auto view = resource_view::ResourceView::CreateResourceView(id, CHILD_PARAM);
        auto unit = Get1DResourceUnit(id + "-agent");
        for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
            (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(cpuBound ? 2000 : 32000);
            (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(cpuBound ? 40000 : 4000);
        }
        (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(cpuBound ? 0 : 3000);
        unit.set_memory_sample_unix_ms(nowMs());
        unit.set_cpu_sample_unix_ms(nowMs());
        auto added = view->AddResourceUnit(unit);
        ASSERT_AWAIT_READY(added);
        auto makeInstance = [&](const std::string &name) {
            auto instance = Get1DInstance();
            instance.set_instanceid(name);
            instance.set_requestid("request-" + name);
            instance.set_unitid(unit.id());
            instance.set_runtimeid("runtime-" + name);
            instance.mutable_schedulerchain()->Clear();
            *instance.mutable_schedulerchain()->Add() = unit.id();
            (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
            auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
            cpu->set_value(500);
            cpu->set_limit(8000);
            auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
            memory->set_value(512);
            memory->set_limit(8192);
            return instance;
        };
        auto allocate = [&](const resources::InstanceInfo &instance) {
            auto promise = std::make_shared<litebus::Promise<Status>>();
            (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
            return promise->GetFuture();
        };
        auto first = makeInstance("cold-first");
        auto second = makeInstance("cold-second");
        auto accepted = allocate(first);
        ASSERT_AWAIT_READY(accepted);
        ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
        auto rejected = allocate(second);
        ASSERT_AWAIT_READY(rejected);
        EXPECT_EQ(rejected.Get().RawMessage().find(cpuBound ? "CPU:" : "Memory:"), size_t{0});
        auto before = view->GetResourceView();
        ASSERT_AWAIT_READY(before);
        EXPECT_TRUE(before.Get()->instances().at(first.instanceid()).resource_admission_pending());
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(nowMs());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1050);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(cpuBound ? 50 : 3050);
        auto &observed = (*report->mutable_instances())[first.instanceid()];
        observed.set_runtimeid(first.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(first.runtimeid());
        sample->set_sandbox_id("cold-sandbox");
        sample->set_resource_generation("cold-generation");
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        auto refreshed = view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
        ASSERT_AWAIT_READY(refreshed);
        auto after = view->GetResourceView();
        ASSERT_AWAIT_READY(after);
        EXPECT_FALSE(after.Get()->instances().at(first.instanceid()).resource_admission_pending());
        accepted = allocate(second);
        ASSERT_AWAIT_READY(accepted);
        EXPECT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    }
}

TEST_F(ResourceViewTest, ResizeReservationsShareAdmissionAndRequireOwnUsageToSettle)
{
    using schedule_plugin::filter::UsageAwareFilter;
    struct ConfigGuard {
        double safety = UsageAwareFilter::GetSafety();
        double floor = UsageAwareFilter::GetFloorMb();
        int32_t cap = UsageAwareFilter::GetMaxInstances();
        ~ConfigGuard() { UsageAwareFilter::SetConfig(safety, floor, cap); }
    } guard;
    UsageAwareFilter::SetConfig(0.9, 2048, 0);
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    for (const bool cpuBound : {false, true}) {
        SCOPED_TRACE(cpuBound ? "resize CPU" : "resize Memory");
        const std::string id = cpuBound ? "resize-cpu-budget" : "resize-memory-budget";
        auto view = resource_view::ResourceView::CreateResourceView(id, CHILD_PARAM);
        auto unit = Get1DResourceUnit(id + "-agent");
        for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
            (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(cpuBound ? 2000 : 32000);
            (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(cpuBound ? 40000 : 4000);
        }
        (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(3000);
        unit.set_cpu_sample_unix_ms(nowMs());
        unit.set_memory_sample_unix_ms(nowMs());
        auto added = view->AddResourceUnit(unit);
        ASSERT_AWAIT_READY(added);
        auto makeInstance = [&](const std::string &name) {
            auto instance = Get1DInstance();
            instance.set_instanceid(name);
            instance.set_requestid("request-" + name);
            instance.set_unitid(unit.id());
            instance.set_runtimeid("runtime-" + name);
            instance.mutable_schedulerchain()->Clear();
            *instance.mutable_schedulerchain()->Add() = unit.id();
            (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
            auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
            cpu->set_value(500);
            cpu->set_limit(8000);
            auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
            memory->set_value(512);
            memory->set_limit(8192);
            return instance;
        };
        auto allocate = [&](const resources::InstanceInfo &instance) {
            auto promise = std::make_shared<litebus::Promise<Status>>();
            (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
            return promise->GetFuture();
        };
        const auto owner = makeInstance("owner");
        auto accepted = allocate(owner);
        ASSERT_AWAIT_READY(accepted);
        ASSERT_TRUE(accepted.Get().IsOk());
        auto publish = [&](bool raised, bool consumed, bool nodeDropped, bool cpuKnown = true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            auto report = std::make_shared<ResourceUnit>(unit);
            report->set_memory_sample_unix_ms(nowMs());
            report->set_cpu_sample_unix_ms(nowMs());
            (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(
                cpuBound && nodeDropped && !consumed ? 0 : 1000);
            (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(
                !cpuBound && nodeDropped ? 2000 : 3000);
            auto &observed = (*report->mutable_instances())[owner.instanceid()];
            observed.set_runtimeid(owner.runtimeid());
            auto *sample = observed.mutable_sandbox_resource_sample();
            sample->set_runtime_id(owner.runtimeid());
            sample->set_sandbox_id("budget-sandbox");
            sample->set_resource_generation("budget-generation");
            sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
            sample->set_cpu_usage_known(cpuKnown);
            sample->set_cpu_usage_cores(cpuBound && consumed ? 1.0 : 0.05);
            sample->set_cpu_limits_known(true);
            sample->set_cpu_period_usec(100000);
            sample->set_cpu_quota_usec(cpuBound && raised ? 100000 : 50000);
            sample->set_memory_limit_bytes((!cpuBound && raised ? 1024ULL : 512ULL) * 1024 * 1024);
            sample->set_memory_current_bytes((!cpuBound && consumed ? 1024ULL : 400ULL) * 1024 * 1024);
            return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
        };
        auto physical = publish(false, false, false, false);
        ASSERT_AWAIT_READY(physical);
        messages::ResizeSandboxRequest early;
        early.set_requestid("early-cold-lease");
        early.set_instanceid(owner.instanceid());
        early.set_runtimeid(owner.runtimeid());
        auto *earlyResize = early.mutable_resize();
        earlyResize->set_sandbox_id("budget-sandbox");
        earlyResize->set_resource_generation("budget-generation");
        earlyResize->set_deadline_unix_ms(nowMs() + 30000);
        earlyResize->mutable_expected()->set_cpu_quota_usec(50000);
        earlyResize->mutable_expected()->set_cpu_period_usec(100000);
        earlyResize->mutable_expected()->set_memory_limit_bytes(512ULL * 1024 * 1024);
        *earlyResize->mutable_target() = earlyResize->expected();
        earlyResize->mutable_target()->set_cpu_quota_usec(60000);
        earlyResize->mutable_target()->set_memory_limit_bytes(544ULL * 1024 * 1024);
        auto earlyBudget = view->ReserveResize(early);
        ASSERT_AWAIT_READY(earlyBudget);
        EXPECT_TRUE(earlyBudget.Get().IsOk()) << earlyBudget.Get().ToString();
        auto earlyCancelled = view->CancelResizeBeforeDispatch(early.requestid());
        ASSERT_AWAIT_READY(earlyCancelled);
        physical = publish(false, false, false);
        ASSERT_AWAIT_READY(physical);
        messages::ResizeSandboxRequest request;
        request.set_requestid("lease-before-dispatch");
        request.set_instanceid(owner.instanceid());
        request.set_runtimeid(owner.runtimeid());
        auto *resize = request.mutable_resize();
        resize->set_sandbox_id("budget-sandbox");
        resize->set_resource_generation("budget-generation");
        resize->set_deadline_unix_ms(nowMs() + 30000);
        resize->mutable_expected()->set_cpu_quota_usec(50000);
        resize->mutable_expected()->set_cpu_period_usec(100000);
        resize->mutable_expected()->set_memory_limit_bytes(512ULL * 1024 * 1024);
        *resize->mutable_target() = resize->expected();
        resize->mutable_target()->set_cpu_quota_usec(100000);
        resize->mutable_target()->set_memory_limit_bytes(1024ULL * 1024 * 1024);
        auto foreign = request;
        foreign.mutable_resize()->set_resource_generation("retired-generation");
        auto invalid = view->ReserveResize(foreign);
        ASSERT_AWAIT_READY(invalid);
        EXPECT_FALSE(invalid.Get().IsOk());
        foreign = request;
        foreign.set_runtimeid("foreign-runtime");
        invalid = view->ReserveResize(foreign);
        ASSERT_AWAIT_READY(invalid);
        EXPECT_FALSE(invalid.Get().IsOk());
        auto reserved = view->ReserveResize(request);
        ASSERT_AWAIT_READY(reserved);
        ASSERT_TRUE(reserved.Get().IsOk()) << reserved.Get().ToString();
        auto cancelled = view->CancelResizeBeforeDispatch(request.requestid());
        ASSERT_AWAIT_READY(cancelled);
        request.set_requestid("lease-dispatched");
        reserved = view->ReserveResize(request);
        ASSERT_AWAIT_READY(reserved);
        ASSERT_TRUE(reserved.Get().IsOk());
        auto conflicting = request;
        conflicting.set_requestid("another-lease");
        auto blocked = view->ReserveResize(conflicting);
        ASSERT_AWAIT_READY(blocked);
        EXPECT_FALSE(blocked.Get().IsOk());
        auto candidate = makeInstance("candidate");
        auto rejected = allocate(candidate);
        ASSERT_AWAIT_READY(rejected);
        EXPECT_EQ(rejected.Get().RawMessage().find(cpuBound ? "CPU:" : "Memory:"), size_t{0});
        auto restoreCandidate = candidate;
        restoreCandidate.mutable_snapshotinfo()->set_checkpointid("pending-checkpoint");
        (*restoreCandidate.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_limit(500);
        (*restoreCandidate.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_limit(512);
        rejected = allocate(restoreCandidate);
        ASSERT_AWAIT_READY(rejected);
        EXPECT_EQ(rejected.Get().RawMessage().find(cpuBound ? "CPU:" : "Memory:"), size_t{0});
        messages::ResizeSandboxResponse response;
        response.set_requestid(request.requestid());
        response.set_instanceid(owner.instanceid());
        response.set_runtimeid(owner.runtimeid());
        response.set_sandboxid(resize->sandbox_id());
        response.set_resourcegeneration(resize->resource_generation());
        response.set_outcome(messages::ResizeSandboxResponse::UNKNOWN);
        auto unknown = view->ObserveResize(response);
        ASSERT_AWAIT_READY(unknown);
        physical = publish(false, false, true);
        ASSERT_AWAIT_READY(physical);
        // A drop elsewhere does not consume this instance's possible write.
        auto larger = candidate;
        (*larger.mutable_resources()->mutable_resources())[cpuBound ? RESOURCE_CPU_NAME : RESOURCE_MEM_NAME]
            .mutable_scalar()->set_value(1400);
        rejected = allocate(larger);
        ASSERT_AWAIT_READY(rejected);
        EXPECT_FALSE(rejected.Get().IsOk());
        response.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
        auto *actual = response.mutable_result()->mutable_actual();
        actual->set_resource_generation(resize->resource_generation());
        actual->set_cpu_limits_known(true);
        actual->set_cpu_period_usec(100000);
        // Both dimensions were requested; only the binding dimension landed.
        // Releasing the entire reservation on complete=false would over-admit.
        response.mutable_result()->set_complete(false);
        actual->set_cpu_quota_usec(cpuBound ? resize->target().cpu_quota_usec() : resize->expected().cpu_quota_usec());
        actual->set_memory_limit_bytes(cpuBound ? resize->expected().memory_limit_bytes() : resize->target().memory_limit_bytes());
        auto observed = view->ObserveResize(response);
        ASSERT_AWAIT_READY(observed);
        physical = publish(true, false, true);
        ASSERT_AWAIT_READY(physical);
        rejected = allocate(larger);
        ASSERT_AWAIT_READY(rejected);
        EXPECT_FALSE(rejected.Get().IsOk()); // quota readback alone does not spend the promise
        physical = publish(true, true, true);
        ASSERT_AWAIT_READY(physical);
        accepted = allocate(cpuBound ? candidate : larger);
        ASSERT_AWAIT_READY(accepted);
        EXPECT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    }
}

TEST_F(ResourceViewTest, ColdAllocationCapSpansConcurrentRequestsWithoutTelemetry)
{
    using schedule_plugin::filter::UsageAwareFilter;
    struct ConfigGuard {
        double safety = UsageAwareFilter::GetSafety();
        double floor = UsageAwareFilter::GetFloorMb();
        int32_t cap = UsageAwareFilter::GetMaxInstances();
        ~ConfigGuard() { UsageAwareFilter::SetConfig(safety, floor, cap); }
    } guard;
    UsageAwareFilter::SetConfig(0.9, 2048, 1);
    auto view = resource_view::ResourceView::CreateResourceView("cold-cap", CHILD_PARAM);
    auto unit = Get1DResourceUnit("cold-agent");
    unit.clear_actualuse();
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    auto allocate = [&](const std::string &id) {
        auto instance = Get1DInstance();
        instance.set_instanceid(id);
        instance.set_requestid("request-" + id);
        instance.set_unitid(unit.id());
        instance.mutable_schedulerchain()->Clear();
        *instance.mutable_schedulerchain()->Add() = unit.id();
        auto promise = std::make_shared<litebus::Promise<Status>>();
        (void)view->AddInstances({{id, resource_view::InstanceAllocatedInfo{instance, promise}}});
        return promise->GetFuture();
    };
    auto first = allocate("first");
    auto second = allocate("second"); // independently queued before observing either allocation
    ASSERT_AWAIT_READY(first);
    ASSERT_AWAIT_READY(second);
    ASSERT_TRUE(first.Get().IsOk());
    EXPECT_EQ(second.Get().StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
    auto occupied = view->GetResourceView();
    ASSERT_AWAIT_READY(occupied);
    EXPECT_EQ(occupied.Get()->instances().size(), size_t{1});
    auto removed = view->DeleteInstances({"first"});
    ASSERT_AWAIT_READY(removed);
    auto replacement = allocate("replacement");
    ASSERT_AWAIT_READY(replacement);
    EXPECT_TRUE(replacement.Get().IsOk());
}

TEST_F(ResourceViewTest, ActualUpdatesReachAdmissionTrendTracker)
{
    // Other suites disable prediction; this check must not depend on link order.
    struct ParamsGuard {
        int64_t window = UsageTrendTracker::windowMs_;
        int64_t horizon = UsageTrendTracker::horizonMs_;
        ~ParamsGuard() { UsageTrendTracker::SetParams(window, horizon); }
    } guard;
    UsageTrendTracker::SetParams(60000, 30000);
    auto view = resource_view::ResourceView::CreateResourceView("actual-trend", CHILD_PARAM);
    auto unit = Get1DResourceUnit("trend-agent");
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    for (double used : {30.0, 40.0}) {
        auto report = std::make_shared<ResourceUnit>(unit);
        report->mutable_actualuse()->mutable_resources()->at(RESOURCE_MEM_NAME).mutable_scalar()->set_value(used);
        auto updated = view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
        ASSERT_AWAIT_READY(updated);
    }
    EXPECT_GE(UsageTrendTracker::Predict(unit.id()), 40.0);
}

TEST_F(ResourceViewTest, AddResourceUnitSuccess)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;

    auto view = resourceView.GetResourceView();
    EXPECT_EQ(view.Get()->fragment_size(), 0);
    EXPECT_EQ(view.Get()->id(), LOCAL_RESOUCE_VIEW_ID);

    auto unit = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit);
    auto [unitProportion, unitMem] = GetMinimumUnitBucketIndex(unit.allocatable());
    resourceView.AddResourceUnit(unit);

    auto view2 = resourceView.GetResourceView();
    auto view2Ptr = view2.Get();
    ASSERT_EQ(view2Ptr->fragment_size(), 1);
    auto &bucket = view2Ptr->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit.id()).monopolynum(), 1);

    auto str = resourceView.GetSerializedResourceView();
    ASSERT_AWAIT_READY(str);
    ResourceUnit deserialiedUnit;
    EXPECT_EQ(deserialiedUnit.ParseFromString(str.Get()), true);
    EXPECT_EQ(deserialiedUnit.id(), LOCAL_RESOUCE_VIEW_ID);
    ASSERT_EQ(deserialiedUnit.fragment_size(), 1);

    auto rUnit = resourceView.GetResourceUnit(unit.id());
    ASSERT_TRUE(rUnit.Get().IsSome());
    EXPECT_EQ(rUnit.Get().Get().id(), unit.id());
    EXPECT_EQ(rUnit.Get().Get().allocatable().resources().size(), unit.allocatable().resources().size());

    resourceView.ClearResourceView();
    auto view3 = resourceView.GetResourceView();
    EXPECT_EQ(view3.Get()->fragment_size(), 0);
}

TEST_F(ResourceViewTest, AddResourceUnitSum)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;

    auto view = resourceView.GetResourceView();
    ASSERT_EQ(view.Get()->fragment_size(), 0);

    ResourceUnit unit = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit);
    auto [unitProportion, unitMem] = GetMinimumUnitBucketIndex(unit.allocatable());
    auto ret = resourceView.AddResourceUnit(unit);
    ASSERT_AWAIT_READY(ret);
    resourceView.PrintResourceView();
    view = resourceView.GetResourceView();
    EXPECT_EQ(view.Get()->fragment_size(), 1);
    EXPECT_TRUE(view.Get()->fragment().at(unit.id()).id() == unit.id());
    EXPECT_TRUE(!view.Get()->id().empty());
    EXPECT_TRUE(view.Get()->allocatable() == unit.allocatable());
    EXPECT_TRUE(view.Get()->actualuse() == unit.actualuse());
    EXPECT_TRUE(view.Get()->capacity() == unit.capacity());
    auto &bucket = view.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit.id()).monopolynum(), 1);

    ResourceUnit unit2 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit2);
    ret = resourceView.AddResourceUnit(unit2);
    ASSERT_AWAIT_READY(ret);
    view = resourceView.GetResourceView();
    EXPECT_EQ(view.Get()->fragment_size(), 2);
    EXPECT_TRUE(view.Get()->fragment().at(unit2.id()).id() == unit2.id());
    EXPECT_TRUE(!view.Get()->id().empty());
    EXPECT_TRUE(view.Get()->allocatable() == unit.allocatable() + unit2.allocatable());
    EXPECT_TRUE(view.Get()->actualuse() == unit.actualuse() + unit2.actualuse());
    EXPECT_TRUE(view.Get()->capacity() == unit.capacity() + unit2.capacity());
    bucket = view.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 2);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit2.id()).monopolynum(), 1);

    ResourceUnit unit3 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit3);
    resourceView.AddResourceUnit(unit3);
    view = resourceView.GetResourceView();
    EXPECT_EQ(view.Get()->fragment_size(), 3);
    EXPECT_TRUE(view.Get()->fragment().at(unit3.id()).id() == unit3.id());
    EXPECT_TRUE(!view.Get()->id().empty());
    EXPECT_TRUE(view.Get()->allocatable() == unit.allocatable() + unit2.allocatable() + unit3.allocatable());
    bucket = view.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 3);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit3.id()).monopolynum(), 1);

    resourceView.ClearResourceView();
    auto view3 = resourceView.GetResourceView();
    EXPECT_EQ(view3.Get()->fragment_size(), 0);
}

TEST_F(ResourceViewTest, DeleteResourceUnit)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;
    auto init = resourceView.GetResourceView();
    auto initRevision = init.Get()->revision();
    EXPECT_EQ(int(initRevision), 0);

    auto unit1 = view_utils::Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit1);
    auto [unitProportion, unitMem] = GetMinimumUnitBucketIndex(unit1.allocatable());
    auto unit2 = view_utils::Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit2);

    auto ret = resourceView.AddResourceUnit(unit1);
    ASSERT_AWAIT_READY(ret);
    ret = resourceView.AddResourceUnit(unit2);
    ASSERT_AWAIT_READY(ret);
    auto view = resourceView.GetResourceView();
    EXPECT_EQ(view.Get()->fragment_size(), 2);
    EXPECT_TRUE(view.Get()->fragment().at(unit1.id()).capacity() == unit1.capacity());
    EXPECT_TRUE(view.Get()->fragment().at(unit2.id()).capacity() == unit2.capacity());

    ret = resourceView.DeleteResourceUnit(unit1.id());
    ASSERT_AWAIT_READY(ret);
    view = resourceView.GetResourceView();
    EXPECT_EQ(view.Get()->fragment_size(), 1);
    EXPECT_TRUE(view.Get()->fragment().at(unit2.id()).capacity() == unit2.capacity());
    auto &bucket = view.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);

    ret = resourceView.DeleteResourceUnit(unit2.id());
    ASSERT_AWAIT_READY(ret);
    view = resourceView.GetResourceView();
    EXPECT_EQ(view.Get()->fragment_size(), 0);
    EXPECT_EQ(view.Get()->revision(), initRevision + 4);
    bucket = view.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);

    view.Clear();
}

TEST_F(ResourceViewTest, InstanceAddAndDel)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);
    auto init = viewPtr->GetResourceView();
    auto initRevision = init.Get()->revision();
    EXPECT_EQ(int(initRevision), 0);

    auto unit1 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit1);
    auto [unitProportion, unitMem] = GetMinimumUnitBucketIndex(unit1.allocatable());
    auto unit2 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit2);
    auto inst1 = Get1DInstance();
    auto inst2 = Get1DInstance();
    auto inst3 = Get1DInstance();
    auto inst4 = Get1DInstance();
    inst4.mutable_scheduleoption()->set_schedpolicyname(MONOPOLY_SCHEDULE);
    inst1.mutable_labels()->Add("label_inst1");
    inst1.mutable_labels()->Add(TENANT_ID + ":123456");
    inst2.mutable_labels()->Add("label_inst2:instance2");
    inst2.mutable_labels()->Add(TENANT_ID + ":789456");
    inst3.mutable_labels()->Add("label_inst2:instance2");
    inst4.mutable_labels()->Add("label-1");
    (*inst1.mutable_schedulerchain()->Add()) = unit1.id();
    inst1.set_unitid(unit1.id());
    (*inst2.mutable_schedulerchain()->Add()) = unit1.id();
    inst2.set_unitid(unit1.id());
    (*inst3.mutable_schedulerchain()->Add()) = unit2.id();
    inst3.set_unitid(unit2.id());
    (*inst4.mutable_schedulerchain()->Add()) = unit2.id();
    inst4.set_unitid(unit2.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
    instances.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    EXPECT_EQ(instances.size(), static_cast<uint32_t>(1));
    EXPECT_EQ(instances.at(inst1.instanceid()).instanceInfo.schedulerchain().size(), 1);
    EXPECT_EQ(instances.at(inst1.instanceid()).instanceInfo.schedulerchain().at(0), unit1.id());

    // add unit 1, inst 1
    litebus::Future<Status> ret;
    ret = viewPtr->AddResourceUnit(unit1);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    ret = viewPtr->AddInstances(instances);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    EXPECT_EQ(rView.Get()->fragment_size(), 1);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(1));
    auto &rInstances = rView.Get()->instances();
    EXPECT_TRUE(rInstances.find(inst1.instanceid()) != rInstances.end());
    auto &rInst1 = rInstances.at(inst1.instanceid());
    EXPECT_EQ(rInst1.instanceid(), inst1.instanceid());
    EXPECT_TRUE(rInst1.resources() == inst1.resources());
    EXPECT_TRUE(rInst1.actualuse() == inst1.actualuse());
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity());
    EXPECT_TRUE(rView.Get()->allocatable() == (unit1.capacity() - inst1.resources()));
    auto fragInst = rView.Get()->fragment().at(unit1.id());
    EXPECT_TRUE(fragInst.capacity() == unit1.capacity());
    EXPECT_TRUE(fragInst.allocatable() == unit1.allocatable() - inst1.resources());
    EXPECT_EQ(fragInst.instances().size(), static_cast<uint32_t>(1));
    EXPECT_TRUE(fragInst.instances().find(inst1.instanceid()) != fragInst.instances().end());
    auto &rInstFragInst1 = fragInst.instances().at(inst1.instanceid());
    EXPECT_EQ(rInstFragInst1.instanceid(), inst1.instanceid());
    EXPECT_TRUE(rInstFragInst1.resources() == inst1.resources());
    EXPECT_TRUE(rInstFragInst1.actualuse() == inst1.actualuse());
    auto bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).monopolynum(), 0);
    EXPECT_EQ(bucket.total().sharednum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).sharednum(), 0);
    auto [remainProportion, remainMem] = GetMinimumUnitBucketIndex(unit1.allocatable() - inst1.resources());
    EXPECT_FALSE(rView.Get()->mutable_bucketindexs()->at(remainProportion).mutable_buckets()->contains(remainMem));

    auto affinityLabels = rView.Get()->nodelabels().find(AFFINITY_SCHEDULE_LABELS);
    EXPECT_NE(affinityLabels, rView.Get()->nodelabels().end());
    EXPECT_NE(rView.Get()->nodelabels().find("label_inst1"), rView.Get()->nodelabels().end());
    EXPECT_NE(fragInst.nodelabels().find("label_inst1"), fragInst.nodelabels().end());
    ASSERT_NE(fragInst.nodelabels().find(TENANT_ID), fragInst.nodelabels().end());
    ASSERT_NE(fragInst.nodelabels().at(TENANT_ID).items().count("123456"), static_cast<uint32_t>(0));
    EXPECT_EQ(fragInst.nodelabels().at(TENANT_ID).items().at("123456"), (size_t)2);

    // add inst 2
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
    instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    ret = viewPtr->AddInstances(instances2);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    EXPECT_EQ(rView.Get()->fragment_size(), 1);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(2));
    auto &rInstances2 = rView.Get()->instances();
    EXPECT_TRUE(rInstances2.find(inst2.instanceid()) != rInstances2.end());
    auto &rInst2 = rInstances.at(inst2.instanceid());
    EXPECT_EQ(rInst2.instanceid(), inst2.instanceid());
    EXPECT_TRUE(rInst2.resources() == inst2.resources());
    EXPECT_TRUE(rInst2.actualuse() == inst2.actualuse());
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity());
    EXPECT_TRUE(rView.Get()->allocatable() == ((unit1.capacity() - inst1.resources()) - inst2.resources()));
    fragInst = rView.Get()->fragment().at(unit1.id());
    EXPECT_TRUE(fragInst.capacity() == unit1.capacity());
    EXPECT_TRUE(fragInst.allocatable() == ((unit1.allocatable() - inst1.resources()) - inst2.resources()));
    EXPECT_EQ(fragInst.instances().size(), static_cast<uint32_t>(2));
    EXPECT_TRUE(fragInst.instances().find(inst1.instanceid()) != fragInst.instances().end());
    auto &rInstFragInst2 = fragInst.instances().at(inst2.instanceid());
    EXPECT_EQ(rInstFragInst2.instanceid(), inst2.instanceid());
    EXPECT_TRUE(rInstFragInst2.resources() == inst2.resources());
    EXPECT_TRUE(rInstFragInst2.actualuse() == inst2.actualuse());
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).monopolynum(), 0);
    auto [remain2Proportion, remain2Mem] =
        GetMinimumUnitBucketIndex(unit1.allocatable() - inst1.resources() - inst2.resources());
    EXPECT_FALSE(rView.Get()->mutable_bucketindexs()->at(remain2Proportion).mutable_buckets()->contains(remain2Mem));
    EXPECT_NE(rView.Get()->nodelabels().find("label_inst2"), rView.Get()->nodelabels().end());
    EXPECT_NE(rView.Get()->nodelabels().at("label_inst2").items().find("instance2"), rView.Get()->nodelabels().at("label_inst2").items().end());
    EXPECT_EQ(rView.Get()->nodelabels().at("label_inst2").items().at("instance2"), (size_t)1);
    ASSERT_NE(rView.Get()->nodelabels().find(TENANT_ID), fragInst.nodelabels().end());
    ASSERT_NE(rView.Get()->nodelabels().at(TENANT_ID).items().count("789456"), static_cast<long unsigned int>(0));
    EXPECT_EQ(rView.Get()->nodelabels().at(TENANT_ID).items().at("789456"), (size_t)2);
    EXPECT_NE(fragInst.nodelabels().find("label_inst2"), fragInst.nodelabels().end());
    EXPECT_NE(fragInst.nodelabels().at("label_inst2").items().find("instance2"), fragInst.nodelabels().at("label_inst2").items().end());
    EXPECT_EQ(fragInst.nodelabels().at("label_inst2").items().at("instance2"), (size_t)1);
    EXPECT_EQ(fragInst.nodelabels().at(TENANT_ID).items().at("789456"), (size_t)2);

    // add unit 2, inst 3
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances3;
    auto allocatePromise = std::make_shared<litebus::Promise<Status>>();
    instances3.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, allocatePromise });
    ret = viewPtr->AddResourceUnit(unit2);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    ret = viewPtr->AddInstances(instances3);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_AWAIT_READY(allocatePromise->GetFuture());
    EXPECT_EQ(allocatePromise->GetFuture().Get().IsOk(), true);

    EXPECT_EQ(rView.Get()->fragment_size(), 2);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(3));
    auto &rInstances3 = rView.Get()->instances();
    EXPECT_TRUE(rInstances3.find(inst3.instanceid()) != rInstances3.end());
    auto &rInst3 = rInstances.at(inst3.instanceid());
    EXPECT_EQ(rInst3.instanceid(), inst3.instanceid());
    EXPECT_TRUE(rInst3.resources() == inst3.resources());
    EXPECT_TRUE(rInst3.actualuse() == inst3.actualuse());
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity() + unit2.capacity());
    EXPECT_TRUE(rView.Get()->allocatable() ==
                (unit1.capacity() + unit2.capacity() - inst1.resources() - inst2.resources() - inst3.resources()));
    fragInst = rView.Get()->fragment().at(unit2.id());
    EXPECT_TRUE(fragInst.capacity() == unit2.capacity());
    EXPECT_TRUE(fragInst.allocatable() == (unit2.allocatable() - inst3.resources()));
    EXPECT_EQ(fragInst.instances().size(), static_cast<uint32_t>(1));
    EXPECT_TRUE(fragInst.instances().find(inst3.instanceid()) != fragInst.instances().end());
    auto &rInstFragInst3 = fragInst.instances().at(inst3.instanceid());
    EXPECT_EQ(rInstFragInst3.instanceid(), inst3.instanceid());
    EXPECT_TRUE(rInstFragInst3.resources() == inst3.resources());
    EXPECT_TRUE(rInstFragInst3.actualuse() == inst3.actualuse());
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit2.id()).monopolynum(), 0);
    EXPECT_FALSE(rView.Get()->mutable_bucketindexs()->at(remainProportion).mutable_buckets()->contains(remainMem));
    EXPECT_NE(rView.Get()->nodelabels().find("label_inst2"), rView.Get()->nodelabels().end());
    EXPECT_NE(rView.Get()->nodelabels().at("label_inst2").items().find("instance2"), rView.Get()->nodelabels().at("label_inst2").items().end());
    EXPECT_EQ(rView.Get()->nodelabels().at("label_inst2").items().at("instance2"), static_cast<long unsigned int>(2));
    EXPECT_NE(fragInst.nodelabels().find("label_inst2"), fragInst.nodelabels().end());
    EXPECT_NE(fragInst.nodelabels().at("label_inst2").items().find("instance2"), fragInst.nodelabels().at("label_inst2").items().end());
    EXPECT_EQ(fragInst.nodelabels().at("label_inst2").items().at("instance2"), static_cast<long unsigned int>(1));

    // delete inst 3
    std::vector<std::string> ids1;
    ids1.push_back(inst3.instanceid());
    ret = viewPtr->DeleteInstances(ids1);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    EXPECT_EQ(rView.Get()->fragment_size(), 2);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(2));
    auto &rInstances4 = rView.Get()->instances();
    EXPECT_TRUE(rInstances4.find(inst3.instanceid()) == rInstances4.end());
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity() + unit2.capacity());
    EXPECT_TRUE(rView.Get()->allocatable() ==
                (unit1.capacity() + unit2.capacity() - inst1.resources() - inst2.resources()));
    fragInst = rView.Get()->fragment().at(unit2.id());
    EXPECT_TRUE(fragInst.capacity() == unit2.capacity());
    EXPECT_TRUE(fragInst.allocatable() == unit2.allocatable());
    EXPECT_EQ(fragInst.instances().size(), static_cast<uint32_t>(0));
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit2.id()).monopolynum(), 1);
    EXPECT_FALSE(rView.Get()->mutable_bucketindexs()->at(remainProportion).mutable_buckets()->contains(remainMem));
    EXPECT_NE(rView.Get()->nodelabels().find("label_inst2"), rView.Get()->nodelabels().end());
    EXPECT_NE(rView.Get()->nodelabels().at("label_inst2").items().find("instance2"), rView.Get()->nodelabels().at("label_inst2").items().end());
    EXPECT_EQ(rView.Get()->nodelabels().at("label_inst2").items().at("instance2"), uint64_t{1});
    EXPECT_EQ(fragInst.nodelabels().find("label_inst2"), fragInst.nodelabels().end());

    // delete inst 2
    std::vector<std::string> ids2;
    ids2.push_back(inst2.instanceid());
    ret = viewPtr->DeleteInstances(ids2);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    EXPECT_EQ(rView.Get()->fragment_size(), 2);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(1));
    auto &rInstances5 = rView.Get()->instances();
    EXPECT_TRUE(rInstances5.find(inst2.instanceid()) == rInstances5.end());
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity() + unit2.capacity());
    EXPECT_TRUE(rView.Get()->allocatable() == (unit1.capacity() + unit2.capacity() - inst1.resources()));
    fragInst = rView.Get()->fragment().at(unit1.id());
    EXPECT_TRUE(fragInst.capacity() == unit1.capacity());
    EXPECT_TRUE(fragInst.allocatable() == unit1.allocatable() - inst1.resources());
    EXPECT_EQ(fragInst.instances().size(), static_cast<uint32_t>(1));
    EXPECT_TRUE(fragInst.instances().find(inst2.instanceid()) == fragInst.instances().end());
    EXPECT_TRUE(fragInst.instances().find(inst1.instanceid()) != fragInst.instances().end());
    auto &rInstFragInst5 = fragInst.instances().at(inst1.instanceid());
    EXPECT_EQ(rInstFragInst5.instanceid(), inst1.instanceid());
    EXPECT_TRUE(rInstFragInst5.resources() == inst1.resources());
    EXPECT_TRUE(rInstFragInst5.actualuse() == inst1.actualuse());
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).monopolynum(), 0);
    EXPECT_FALSE(rView.Get()->mutable_bucketindexs()->at(remainProportion).mutable_buckets()->contains(remainMem));
    EXPECT_EQ(rView.Get()->nodelabels().find("label_inst2"), rView.Get()->nodelabels().end());
    EXPECT_EQ(fragInst.nodelabels().find("label_inst2"), fragInst.nodelabels().end());

    // delete inst 1
    std::vector<std::string> ids3;
    ids3.push_back(inst1.instanceid());
    ret = viewPtr->DeleteInstances(ids3);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    EXPECT_EQ(rView.Get()->fragment_size(), 2);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(0));
    auto &rInstances6 = rView.Get()->instances();
    EXPECT_TRUE(rInstances6.find(inst1.instanceid()) == rInstances6.end());
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity() + unit2.capacity());
    EXPECT_TRUE(rView.Get()->allocatable() == (unit1.capacity() + unit2.capacity()));
    fragInst = rView.Get()->fragment().at(unit1.id());
    EXPECT_TRUE(fragInst.capacity() == unit1.capacity());
    EXPECT_TRUE(fragInst.allocatable() == unit1.allocatable());
    EXPECT_EQ(fragInst.instances().size(), static_cast<uint32_t>(0));
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 2);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).monopolynum(), 1);
    EXPECT_FALSE(rView.Get()->mutable_bucketindexs()->at(remainProportion).mutable_buckets()->contains(remainMem));

    ASSERT_NE(rView.Get()->nodelabels().find(TENANT_ID), fragInst.nodelabels().end());
    ASSERT_NE(rView.Get()->nodelabels().at(TENANT_ID).items().count("789456"), (size_t)0);
    ASSERT_NE(rView.Get()->nodelabels().at(TENANT_ID).items().count("123456"), (size_t)0);
    EXPECT_EQ(rView.Get()->nodelabels().at(TENANT_ID).items().at("789456"), (size_t)1);
    EXPECT_EQ(rView.Get()->nodelabels().at(TENANT_ID).items().at("123456"), (size_t)1);

    // add inst 4 with monopoly
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances4;
    instances4.emplace(inst4.instanceid(), resource_view::InstanceAllocatedInfo{ inst4, nullptr });
    ret = viewPtr->AddInstances(instances4);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    EXPECT_EQ(rView.Get()->fragment_size(), 2);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(1));
    auto &rInstances7 = rView.Get()->instances();
    EXPECT_TRUE(rInstances7.find(inst4.instanceid()) != rInstances7.end());
    auto &rInst4 = rInstances7.at(inst4.instanceid());
    EXPECT_EQ(rInst4.instanceid(), inst4.instanceid());
    EXPECT_TRUE(rInst4.resources() == inst4.resources());
    EXPECT_TRUE(rInst4.actualuse() == inst4.actualuse());
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity() + unit2.capacity());
    EXPECT_TRUE(rView.Get()->allocatable() == unit1.capacity());
    fragInst = rView.Get()->fragment().at(unit2.id());
    EXPECT_TRUE(fragInst.capacity() == unit2.capacity());
    EXPECT_TRUE(fragInst.allocatable() == (unit2.allocatable() - unit2.allocatable()));
    EXPECT_EQ(fragInst.instances().size(), static_cast<uint32_t>(1));
    EXPECT_TRUE(fragInst.instances().find(inst4.instanceid()) != fragInst.instances().end());
    auto &rInstFragInst4 = fragInst.instances().at(inst4.instanceid());
    EXPECT_EQ(rInstFragInst4.instanceid(), inst4.instanceid());
    EXPECT_TRUE(rInstFragInst4.resources() == inst4.resources());
    EXPECT_TRUE(rInstFragInst4.actualuse() == inst4.actualuse());
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit2.id()).monopolynum(), 0);
    affinityLabels = rView.Get()->nodelabels().find(AFFINITY_SCHEDULE_LABELS);
    EXPECT_NE(affinityLabels, rView.Get()->nodelabels().end());
    EXPECT_NE(affinityLabels->second.items().find("label-1"), affinityLabels->second.items().end());
    affinityLabels = rView.Get()->nodelabels().find("label-1");
    EXPECT_NE(affinityLabels, rView.Get()->nodelabels().end());
    EXPECT_NE(affinityLabels->second.items().find(""), affinityLabels->second.items().end());

    // delete inst4
    ret = viewPtr->DeleteInstances({ inst4.instanceid() });
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    EXPECT_EQ(rView.Get()->fragment_size(), 2);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(0));
    auto &rInstances8 = rView.Get()->instances();
    EXPECT_TRUE(rInstances8.find(inst4.instanceid()) == rInstances8.end());
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity() + unit2.capacity());
    EXPECT_TRUE(rView.Get()->allocatable() == (unit1.capacity() + unit2.capacity()));
    fragInst = rView.Get()->fragment().at(unit2.id());
    EXPECT_TRUE(fragInst.capacity() == unit2.capacity());
    EXPECT_TRUE(fragInst.allocatable() == unit2.allocatable());
    EXPECT_EQ(fragInst.instances().size(), static_cast<uint32_t>(0));
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit2.id()).monopolynum(), 0);
    affinityLabels = rView.Get()->nodelabels().find(AFFINITY_SCHEDULE_LABELS);
    EXPECT_NE(affinityLabels, rView.Get()->nodelabels().end());
    EXPECT_EQ(affinityLabels->second.items().find("label-1"), affinityLabels->second.items().end());
    affinityLabels = rView.Get()->nodelabels().find("label-1");
    EXPECT_EQ(affinityLabels, rView.Get()->nodelabels().end());

    // delete unit1
    ret = viewPtr->DeleteResourceUnit(unit1.id());
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    EXPECT_EQ(rView.Get()->fragment_size(), 1);
    EXPECT_TRUE(rView.Get()->capacity() == unit2.capacity());
    EXPECT_TRUE(rView.Get()->allocatable() == unit2.capacity());
    EXPECT_TRUE(rView.Get()->fragment().find(unit1.id()) == rView.Get()->fragment().end());

    // delete unit2
    ret = viewPtr->DeleteResourceUnit(unit2.id());
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    EXPECT_EQ(rView.Get()->revision(), initRevision + 12);
    EXPECT_EQ(rView.Get()->fragment_size(), 0);
    EXPECT_TRUE(rView.Get()->capacity() == Get0CpuMemResources());
    EXPECT_TRUE(rView.Get()->allocatable() == Get0CpuMemResources());
    EXPECT_TRUE(rView.Get()->fragment().find(unit2.id()) == rView.Get()->fragment().end());
    // add instance with invalid agent
    std::map<std::string, resource_view::InstanceAllocatedInfo> invalidInstances;
    inst4.set_unitid("xxxxxxx");
    invalidInstances.emplace(inst4.instanceid(), resource_view::InstanceAllocatedInfo{ inst4, std::make_shared<litebus::Promise<Status>>() });
    ret = viewPtr->AddInstances(invalidInstances);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    auto allocatedFuture = invalidInstances[inst4.instanceid()].allocatedPromise->GetFuture();
    ASSERT_AWAIT_READY(allocatedFuture);
    EXPECT_EQ(allocatedFuture.Get().IsOk(), false);
    EXPECT_EQ(allocatedFuture.Get().StatusCode(), StatusCode::ERR_INNER_SYSTEM_ERROR);
}

TEST_F(ResourceViewTest, AgentReuseTimer)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    std::set<std::string> disabledAgent;
    std::atomic<int> count = 0;
    viewPtr->RegisterUnitDisableFunc([&disabledAgent, &count](const std::string &agentID){
        disabledAgent.emplace(agentID);
        count++;
    });
    auto rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);
    auto init = viewPtr->GetResourceView();
    auto initRevision = init.Get()->revision();
    EXPECT_EQ(int(initRevision), 0);

    auto unit1 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit1);
    auto unit2 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit2);
    auto unit3 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit3);
    auto inst1 = Get1DInstance();
    auto inst2 = Get1DInstance();
    auto inst3 = Get1DInstance();
    auto inst4= Get1DInstance();

    inst1.mutable_labels()->Add("label_inst1");
    inst1.mutable_labels()->Add(TENANT_ID + ":123456");
    inst1.set_functionproxyid(LOCAL_RESOUCE_VIEW_ID);
    inst1.set_tenantid("123456");
    inst2.mutable_labels()->Add("label_inst2:instance2");
    inst2.mutable_labels()->Add(TENANT_ID + ":123456");
    inst2.set_tenantid("123456");
    inst2.set_functionproxyid(LOCAL_RESOUCE_VIEW_ID);
    inst3.mutable_labels()->Add("label_inst2:789456");
    inst3.set_functionproxyid(LOCAL_RESOUCE_VIEW_ID);
    inst3.set_tenantid("789456");
    inst4.mutable_labels()->Add("label_inst3:789456");
    inst4.set_functionproxyid(LOCAL_RESOUCE_VIEW_ID);
    inst4.set_tenantid("789456");

    (*inst1.mutable_schedulerchain()->Add()) = unit1.id();
    inst1.set_unitid(unit1.id());
    (*inst2.mutable_schedulerchain()->Add()) = unit1.id();
    inst2.set_unitid(unit1.id());
    (*inst3.mutable_schedulerchain()->Add()) = unit2.id();
    inst3.set_unitid(unit2.id());
    (*inst4.mutable_schedulerchain()->Add()) = unit3.id();
    inst4.set_unitid(unit3.id());

    // add unit 1, inst 1
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
    instances.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    litebus::Future<Status> ret;
    ret = viewPtr->AddResourceUnit(unit1);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    YRLOG_DEBUG("add inst1");
    ret = viewPtr->AddInstances(instances);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    // add inst 2
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
    instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    YRLOG_DEBUG("add inst2");
    ret = viewPtr->AddInstances(instances2);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    auto agentCache = viewPtr->GetAgentCache();
    EXPECT_EQ(agentCache[unit1.id()].size(), size_t{2});

    // add unit 2, inst 3
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances3;
    instances3.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, nullptr });
    ret = viewPtr->AddResourceUnit(unit2);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    YRLOG_DEBUG("add inst3");
    ret = viewPtr->AddInstances(instances3);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    // add unit 3, inst 4
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances4;
    instances4.emplace(inst4.instanceid(), resource_view::InstanceAllocatedInfo{ inst4, nullptr });
    ret = viewPtr->AddResourceUnit(unit3);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    YRLOG_DEBUG("add inst4");
    ret = viewPtr->AddInstances(instances4);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    // delete inst 3
    std::vector<std::string> ids1;
    ids1.push_back(inst3.instanceid());
    YRLOG_DEBUG("delete inst3");
    ret = viewPtr->DeleteInstances(ids1);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    auto reuseTimers = viewPtr->GetReuseTimers();
    EXPECT_TRUE(reuseTimers.count(inst3.unitid()) != 0);

    agentCache = viewPtr->GetAgentCache();
    EXPECT_TRUE(agentCache.count(inst3.unitid()) == 0);

    // delete inst 2
    std::vector<std::string> ids2;
    ids2.push_back(inst2.instanceid());
    YRLOG_DEBUG("delete inst2");
    ret = viewPtr->DeleteInstances(ids2);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    agentCache = viewPtr->GetAgentCache();
    EXPECT_TRUE(agentCache[inst2.unitid()].size() == 1);

    // delete inst 1
    std::vector<std::string> ids3;
    ids3.push_back(inst1.instanceid());
    YRLOG_DEBUG("delete inst1");
    ret = viewPtr->DeleteInstances(ids3);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    reuseTimers = viewPtr->GetReuseTimers();
    EXPECT_TRUE(reuseTimers.count(inst1.unitid()) != 0);

    agentCache = viewPtr->GetAgentCache();
    EXPECT_TRUE(agentCache.count(inst1.unitid()) == 0);

    // check unit status
    ASSERT_AWAIT_TRUE([&]() -> bool { return count == 2; });

    auto rUnit = viewPtr->GetResourceUnit(unit1.id());
    ASSERT_FALSE(rUnit.Get().IsSome());

    rUnit = viewPtr->GetResourceUnit(unit2.id());
    ASSERT_FALSE(rUnit.Get().IsSome());

    // delete inst 4 -- virtual instance
    std::vector<std::string> ids4;
    ids4.push_back(inst4.instanceid());
    YRLOG_DEBUG("delete inst4");
    auto begin = litebus::TimeWatch::Now();
    ret = viewPtr->DeleteInstances(ids4, true);
    ASSERT_AWAIT_READY(ret);
    ret.Get();
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);

    reuseTimers = viewPtr->GetReuseTimers();
    EXPECT_TRUE(reuseTimers.count(inst4.unitid()) == 0);

    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 2000; });

    EXPECT_TRUE(count == 2);
    rUnit = viewPtr->GetResourceUnit(unit3.id());
    ASSERT_TRUE(rUnit.Get().IsSome());
}

TEST_F(ResourceViewTest, Resource2DWithInstances)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);

    auto unit1 = Get2DResourceUnitWithInstances();
    auto inst1 = unit1.fragment().begin()->second.instances().begin()->second;
    auto inst2 = (++unit1.fragment().begin()->second.instances().begin())->second;
    auto inst3 = (++unit1.fragment().begin())->second.instances().begin()->second;
    auto inst4 = ((++unit1.fragment().begin())->second.instances().begin())->second;
    auto subUnit1 = unit1.fragment().begin()->second;
    auto subUnit2 = (++unit1.fragment().begin())->second;
    // add unit 1
    litebus::Future<Status> ret;
    ret = viewPtr->AddResourceUnit(unit1);
    ASSERT_AWAIT_READY(ret);
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    YRLOG_DEBUG("Resource2DWithInstances position 1 : resource unit is {}.", ToString(*rView.Get()));

    // check top level
    EXPECT_EQ(rView.Get()->fragment_size(), 1);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(4));
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity());
    auto rInstances = rView.Get()->instances();
    ASSERT_TRUE(rInstances.find(inst1.instanceid()) != rInstances.end());
    ASSERT_TRUE(rInstances.find(inst2.instanceid()) != rInstances.end());
    ASSERT_TRUE(rInstances.find(inst3.instanceid()) != rInstances.end());
    ASSERT_TRUE(rInstances.find(inst4.instanceid()) != rInstances.end());
    auto rInst1 = rInstances.at(inst1.instanceid());
    EXPECT_EQ(rInst1.instanceid(), inst1.instanceid());
    EXPECT_TRUE(rInst1.resources() == inst1.resources());
    EXPECT_TRUE(rInst1.actualuse() == inst1.actualuse());
    EXPECT_EQ(rInst1.schedulerchain().size(), inst1.schedulerchain().size());
    auto rInst4 = rInstances.at(inst4.instanceid());
    EXPECT_EQ(rInst4.instanceid(), inst4.instanceid());
    EXPECT_TRUE(rInst4.resources() == inst4.resources());
    EXPECT_TRUE(rInst4.actualuse() == inst4.actualuse());
    EXPECT_EQ(rInst4.schedulerchain().size(), inst4.schedulerchain().size());

    ASSERT_TRUE(rView.Get()->fragment().find(unit1.id()) != rView.Get()->fragment().end());
    auto fragInst = rView.Get()->fragment().at(unit1.id());
    EXPECT_TRUE(fragInst.fragment().size() == 2);
    EXPECT_TRUE(fragInst.capacity() == unit1.capacity());
    EXPECT_TRUE(fragInst.instances_size() == 4);
    auto rSubUnit1 = fragInst.fragment().begin()->second;
    auto rSubUnit2 = (++fragInst.fragment().begin())->second;
    EXPECT_TRUE(rSubUnit1.instances().size() == 2);
    EXPECT_TRUE(rSubUnit1.capacity() == subUnit1.capacity());
    EXPECT_TRUE(rSubUnit2.instances().size() == 2);
    EXPECT_TRUE(rSubUnit2.capacity() == subUnit2.capacity());
    auto [unit1RemainProportion, unit1Remain] = GetMinimumUnitBucketIndex(subUnit1.allocatable());
    auto bucket = rView.Get()->mutable_bucketindexs()->at(unit1RemainProportion).mutable_buckets()->at(unit1Remain);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).monopolynum(), 0);
    EXPECT_EQ(bucket.total().sharednum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).sharednum(), 1);

    auto unit2 = Get2DResourceUnitWithInstances();
    inst1 = unit2.fragment().begin()->second.instances().begin()->second;
    inst2 = (++unit2.fragment().begin()->second.instances().begin())->second;
    inst3 = (++unit2.fragment().begin())->second.instances().begin()->second;
    inst4 = ((++unit2.fragment().begin())->second.instances().begin())->second;
    subUnit1 = unit2.fragment().begin()->second;
    subUnit2 = (++unit2.fragment().begin())->second;
    // add unit 2
    ret = viewPtr->AddResourceUnit(unit2);
    ASSERT_AWAIT_READY(ret);
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    YRLOG_DEBUG("Resource2DWithInstances position 2 : resource unit is {}.", ToString(*rView.Get()));

    // check top level
    EXPECT_EQ(rView.Get()->fragment_size(), 2);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(8));
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity() + unit2.capacity());
    rInstances = rView.Get()->instances();
    ASSERT_TRUE(rInstances.find(inst1.instanceid()) != rInstances.end());
    ASSERT_TRUE(rInstances.find(inst2.instanceid()) != rInstances.end());
    ASSERT_TRUE(rInstances.find(inst3.instanceid()) != rInstances.end());
    ASSERT_TRUE(rInstances.find(inst4.instanceid()) != rInstances.end());
    rInst1 = rInstances.at(inst1.instanceid());
    EXPECT_EQ(rInst1.instanceid(), inst1.instanceid());
    EXPECT_TRUE(rInst1.resources() == inst1.resources());
    EXPECT_TRUE(rInst1.actualuse() == inst1.actualuse());
    EXPECT_EQ(rInst1.schedulerchain().size(), inst1.schedulerchain().size());
    rInst4 = rInstances.at(inst4.instanceid());
    EXPECT_EQ(rInst4.instanceid(), inst4.instanceid());
    EXPECT_TRUE(rInst4.resources() == inst4.resources());
    EXPECT_TRUE(rInst4.actualuse() == inst4.actualuse());
    EXPECT_EQ(rInst4.schedulerchain().size(), inst4.schedulerchain().size());

    ASSERT_TRUE(rView.Get()->fragment().find(unit2.id()) != rView.Get()->fragment().end());
    fragInst = rView.Get()->fragment().at(unit2.id());
    EXPECT_TRUE(fragInst.fragment().size() == 2);
    EXPECT_TRUE(fragInst.capacity() == unit1.capacity());
    EXPECT_TRUE(fragInst.instances_size() == 4);
    rSubUnit1 = fragInst.fragment().begin()->second;
    rSubUnit2 = (++fragInst.fragment().begin())->second;
    EXPECT_TRUE(rSubUnit1.instances().size() == 2);
    EXPECT_TRUE(rSubUnit1.capacity() == subUnit1.capacity());
    EXPECT_TRUE(rSubUnit2.instances().size() == 2);
    EXPECT_TRUE(rSubUnit2.capacity() == subUnit2.capacity());
    bucket = rView.Get()->mutable_bucketindexs()->at(unit1RemainProportion).mutable_buckets()->at(unit1Remain);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).monopolynum(), 0);
    EXPECT_EQ(bucket.total().sharednum(), 2);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).sharednum(), 1);

    // delete unit 2
    ret = viewPtr->DeleteResourceUnit(unit2.id());
    ASSERT_AWAIT_READY(ret);
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    YRLOG_DEBUG("Resource2DWithInstances position 3 : resource unit is {}.", ToString(*rView.Get()));

    // check top level
    EXPECT_EQ(rView.Get()->fragment_size(), 1);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(4));
    EXPECT_TRUE(rView.Get()->capacity() == unit1.capacity());
    rInstances = rView.Get()->instances();
    ASSERT_TRUE(rInstances.find(inst1.instanceid()) == rInstances.end());
    ASSERT_TRUE(rInstances.find(inst2.instanceid()) == rInstances.end());
    ASSERT_TRUE(rInstances.find(inst3.instanceid()) == rInstances.end());
    ASSERT_TRUE(rInstances.find(inst4.instanceid()) == rInstances.end());

    ASSERT_TRUE(rView.Get()->fragment().find(unit1.id()) != rView.Get()->fragment().end());
    fragInst = rView.Get()->fragment().at(unit1.id());
    EXPECT_TRUE(fragInst.fragment().size() == 2);
    EXPECT_TRUE(fragInst.capacity() == unit1.capacity());
    EXPECT_TRUE(fragInst.instances_size() == 4);
    rSubUnit1 = fragInst.fragment().begin()->second;
    rSubUnit2 = (++fragInst.fragment().begin())->second;
    EXPECT_TRUE(rSubUnit1.instances().size() == 2);
    EXPECT_TRUE(rSubUnit1.capacity() == subUnit1.capacity());
    EXPECT_TRUE(rSubUnit2.instances().size() == 2);
    EXPECT_TRUE(rSubUnit2.capacity() == subUnit2.capacity());

    // delete unit 1
    ret = viewPtr->DeleteResourceUnit(unit1.id());
    ASSERT_AWAIT_READY(ret);
    rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    YRLOG_DEBUG("Resource2DWithInstances position 4 : resource unit is {}.", ToString(*rView.Get()));

    // check top level
    EXPECT_EQ(rView.Get()->fragment_size(), 0);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(0));
    EXPECT_TRUE(IsEmpty(rView.Get()->capacity()));
    EXPECT_TRUE(IsEmpty(rView.Get()->allocatable()));
    EXPECT_TRUE(IsEmpty(rView.Get()->actualuse()));
}

TEST_F(ResourceViewTest, UpdateResourceUnitWithNpuResource)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto rView = viewPtr->GetResourceView();
    ASSERT_EQ(rView.Get()->fragment_size(), 0);

    auto unit0 = Get1DResourceUnitWithNpu();
    auto unit1 = Get1DResourceUnitWithNpu();
    auto ret = viewPtr->AddResourceUnit(unit0);
    ASSERT_AWAIT_READY(ret);
    ret = viewPtr->AddResourceUnit(unit1);
    ASSERT_AWAIT_READY(ret);

    ASSERT_EQ(rView.Get()->fragment_size(), 2);
    EXPECT_TRUE(rView.Get()->capacity() == (unit1.capacity() + unit0.capacity()));
    EXPECT_TRUE(rView.Get()->allocatable() == (unit1.allocatable() + unit0.allocatable()));
    EXPECT_TRUE(rView.Get()->actualuse() == (unit1.actualuse() + unit0.actualuse()));
    auto res = rView.Get()->capacity().resources()
                   .at(resource_view::NPU_RESOURCE_NAME+ "/310").vectors().values()
                   .at(resource_view::HETEROGENEOUS_MEM_KEY).vectors().begin()->second.values();
    EXPECT_EQ(res.at(0), 100);
    EXPECT_EQ(res.at(1), 100);
    EXPECT_EQ(res.at(7), 100);

    // update actual
    auto unit2 = unit1;
    // keep internal uuid equal
    auto tmp = unit2.mutable_actualuse()->mutable_resources()->at(resource_view::NPU_RESOURCE_NAME+"/310").mutable_vectors()
        ->mutable_values()
        ->at(resource_view::HETEROGENEOUS_MEM_KEY).mutable_vectors()->begin()->second.mutable_values();
    tmp->Clear();
    for (int i=0;i<8;i++){
        tmp->Add(21);
    }
    auto unptr = std::make_shared<ResourceUnit>(unit2);
    ret = viewPtr->UpdateResourceUnit(unptr, UpdateType::UPDATE_ACTUAL);
    ASSERT_AWAIT_READY(ret);
    rView = viewPtr->GetResourceView();
    EXPECT_TRUE(rView.Get()->capacity() == (unit0.capacity() + unit1.capacity()));
    EXPECT_TRUE(rView.Get()->allocatable() == (unit0.allocatable() + unit1.allocatable()));
    EXPECT_TRUE(rView.Get()->actualuse() == (unit0.actualuse() + unit2.actualuse()));
    auto fragInst = rView.Get()->fragment().at(unit2.id());
    EXPECT_TRUE(fragInst.capacity() == unit2.capacity());
    EXPECT_TRUE(fragInst.allocatable() == unit2.allocatable());
    EXPECT_TRUE(fragInst.actualuse() == unit2.actualuse());
}

TEST_F(ResourceViewTest, UpdateResourceUnit)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto rView = viewPtr->GetResourceView();
    ASSERT_EQ(rView.Get()->fragment_size(), 0);

    auto unit1 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit1);
    auto [unitProportion, unitMem] = GetMinimumUnitBucketIndex(unit1.allocatable());
    auto unit2 = Get1DResourceUnit();
    unit2.set_alias("unit2");
    GenerateMinimumUnitBucketInfo(unit2);
    auto ret = viewPtr->AddResourceUnit(unit1);
    ASSERT_AWAIT_READY(ret);
    ret = viewPtr->AddResourceUnit(unit2);
    ASSERT_AWAIT_READY(ret);
    rView = viewPtr->GetResourceView();
    ASSERT_EQ(rView.Get()->fragment_size(), 2);
    EXPECT_TRUE(rView.Get()->capacity() == (unit1.capacity() + unit2.capacity()));
    EXPECT_TRUE(rView.Get()->allocatable() == (unit1.capacity() + unit2.capacity()));
    EXPECT_TRUE(rView.Get()->actualuse() == (unit1.actualuse() + unit2.actualuse()));
    auto bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 2);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).monopolynum(), 1);

    auto unit5 = std::make_shared<ResourceUnit>(unit2);
    auto resources2 = GetCpuResources();
    (*unit5->mutable_capacity()) = resources2;
    (*unit5->mutable_allocatable()) = resources2;
    (*unit5->mutable_actualuse()) = resources2;
    auto unit5Tmp = *unit5;
    ret = viewPtr->UpdateResourceUnit(unit5, UpdateType::UPDATE_ACTUAL);
    ASSERT_AWAIT_READY(ret);
    rView = viewPtr->GetResourceView();
    EXPECT_TRUE(rView.Get()->capacity() == (unit1.capacity() + unit2.capacity()));
    EXPECT_TRUE(rView.Get()->allocatable() == (unit1.allocatable() + unit2.allocatable()));
    EXPECT_TRUE(rView.Get()->actualuse() == (unit1.actualuse() + unit5Tmp.actualuse()));
    auto fragInst = rView.Get()->fragment().at(unit2.id());
    EXPECT_TRUE(fragInst.capacity() == unit2.capacity());
    EXPECT_TRUE(fragInst.allocatable() == unit2.allocatable());
    EXPECT_TRUE(fragInst.actualuse() == unit5Tmp.actualuse());
    EXPECT_TRUE(fragInst.alias() == "unit2");
}

// D-7: UPDATE_ACTUAL must merge the reported per-instance actualuse into the
// view -- dropping it left every view instance at useMb=0 and disabled the
// pressure-park victim's "reclaim the most" tiebreak. A lagging report for
// an instance the view no longer tracks must not resurrect anything.
TEST_F(ResourceViewTest, UpdateActualMergesPerInstanceActualuse)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(
        "update-actual-instances", CHILD_PARAM);
    auto &resourceView = *viewPtr;

    auto unit = Get1DResourceUnitWithInstances();
    // production stamps the owning unit on every instance (see
    // IsInstanceInResourceView); the fixture helper does not
    for (auto &inst : *unit.mutable_instances()) {
        inst.second.set_unitid(unit.id());
        inst.second.set_runtimeid("runtime-1");
    }
    auto ret = resourceView.AddResourceUnit(unit);
    ASSERT_AWAIT_READY(ret);

    const auto knownKey = unit.instances().begin()->first;
    resource_view::Resources used;
    resource_view::Resource mem;
    mem.mutable_scalar()->set_value(1536.0);
    (*used.mutable_resources())[RESOURCE_MEM_NAME] = mem;

    auto report = std::make_shared<resource_view::ResourceUnit>(Get1DResourceUnit(unit.id()));
    report->set_memory_sample_unix_ms(2000);
    auto knownInst = Get1DInstance();
    knownInst.set_instanceid(knownKey);
    (*knownInst.mutable_actualuse()) = used;
    auto *sample = knownInst.mutable_sandbox_resource_sample();
    sample->set_runtime_id("runtime-1");
    sample->set_sandbox_id("sandbox-1");
    sample->set_resource_generation("generation-1");
    sample->set_sampled_at_unix_ms(2000);
    sample->set_memory_limit_bytes(512ULL * 1024 * 1024);
    (*report->mutable_instances())[knownKey] = knownInst;
    auto ghostInst = Get1DInstance();
    ghostInst.set_instanceid("instance-not-in-view");
    (*ghostInst.mutable_actualuse()) = used;
    (*report->mutable_instances())["instance-not-in-view"] = ghostInst;

    ret = resourceView.UpdateResourceUnit(report, resource_view::UpdateType::UPDATE_ACTUAL);
    ASSERT_AWAIT_READY(ret);

    auto rUnit = resourceView.GetResourceUnit(unit.id());
    ASSERT_TRUE(rUnit.Get().IsSome());
    EXPECT_EQ(rUnit.Get().Get().memory_sample_unix_ms(), 2000);
    const auto &fragInstances = rUnit.Get().Get().instances();
    ASSERT_TRUE(fragInstances.contains(knownKey));
    EXPECT_DOUBLE_EQ(fragInstances.at(knownKey).actualuse().resources().at(RESOURCE_MEM_NAME).scalar()
                         .value(), 1536.0);
    EXPECT_FALSE(fragInstances.contains("instance-not-in-view"));

    // the view-level instance index (what e.g. the pressure monitor reads)
    // must carry the same refreshed actualuse, not the stale AddResourceUnit
    // snapshot
    auto view = resourceView.GetResourceView();
    const auto &viewInstances = view.Get()->instances();
    ASSERT_TRUE(viewInstances.contains(knownKey));
    EXPECT_DOUBLE_EQ(viewInstances.at(knownKey).actualuse().resources().at(RESOURCE_MEM_NAME).scalar()
                         .value(), 1536.0);
    EXPECT_EQ(fragInstances.at(knownKey).sandbox_resource_sample().SerializeAsString(), sample->SerializeAsString());
    EXPECT_EQ(viewInstances.at(knownKey).sandbox_resource_sample().SerializeAsString(), sample->SerializeAsString());
    // A late sample must not roll back either index; a newer foreign runtime must not replace it.
    for (const bool foreignRuntime : {false, true}) {
        auto late = std::make_shared<resource_view::ResourceUnit>(Get1DResourceUnit(unit.id()));
        late->set_memory_sample_unix_ms(foreignRuntime ? 3000 : 1000);
        auto changed = knownInst;
        changed.mutable_sandbox_resource_sample()->set_sampled_at_unix_ms(foreignRuntime ? 3000 : 1000);
        if (foreignRuntime) {
            changed.mutable_sandbox_resource_sample()->set_runtime_id("retired-runtime");
        }
        (*changed.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(1);
        (*late->mutable_instances())[knownKey] = changed;
        ret = resourceView.UpdateResourceUnit(late, resource_view::UpdateType::UPDATE_ACTUAL);
        ASSERT_AWAIT_READY(ret);
        auto refreshed = resourceView.GetResourceView();
        EXPECT_EQ(refreshed.Get()->instances().at(knownKey).sandbox_resource_sample().sampled_at_unix_ms(), 2000);
        EXPECT_DOUBLE_EQ(refreshed.Get()->instances().at(knownKey).actualuse().resources()
                             .at(RESOURCE_MEM_NAME).scalar().value(), 1536.0);
        auto fragment = resourceView.GetResourceUnit(unit.id());
        ASSERT_TRUE(fragment.Get().IsSome());
        EXPECT_EQ(fragment.Get().Get().instances().at(knownKey).sandbox_resource_sample().sampled_at_unix_ms(), 2000);
    }
}

TEST_F(ResourceViewTest, DynamicUpdateRemovesMissingVectorResourceWithoutAffectingOtherUnitsOrInstances)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(
        "dynamic-vector-removal", CHILD_PARAM);
    auto firstUnit = Get1DResourceUnitWithGpuCount({ 1, 1 });
    auto secondUnit = Get1DResourceUnitWithGpuCount({ 1 });
    GenerateMinimumUnitBucketInfo(firstUnit);
    GenerateMinimumUnitBucketInfo(secondUnit);

    auto add = viewPtr->AddResourceUnit(firstUnit);
    ASSERT_AWAIT_READY(add);
    ASSERT_TRUE(add.Get().IsOk());
    add = viewPtr->AddResourceUnit(secondUnit);
    ASSERT_AWAIT_READY(add);
    ASSERT_TRUE(add.Get().IsOk());

    auto instance = Get1DInstance();
    instance.set_unitid(firstUnit.id());
    *instance.mutable_schedulerchain()->Add() = firstUnit.id();
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
    instances.emplace(instance.instanceid(), resource_view::InstanceAllocatedInfo{ instance, nullptr });
    auto addInstance = viewPtr->AddInstances(instances);
    ASSERT_AWAIT_READY(addInstance);
    ASSERT_TRUE(addInstance.Get().IsOk());

    auto updateUnit = std::make_shared<ResourceUnit>(firstUnit);
    updateUnit->mutable_capacity()->mutable_resources()->erase(DEFAULT_GPU_TYPE);
    updateUnit->mutable_allocatable()->mutable_resources()->erase(DEFAULT_GPU_TYPE);
    auto update = viewPtr->UpdateResourceUnit(updateUnit, UpdateType::UPDATE_DYNAMIC);
    ASSERT_AWAIT_READY(update);
    ASSERT_TRUE(update.Get().IsOk());

    auto firstPublished = viewPtr->GetResourceUnit(firstUnit.id());
    ASSERT_AWAIT_READY(firstPublished);
    ASSERT_TRUE(firstPublished.Get().IsSome());
    EXPECT_FALSE(firstPublished.Get().Get().capacity().resources().contains(DEFAULT_GPU_TYPE));
    EXPECT_FALSE(firstPublished.Get().Get().allocatable().resources().contains(DEFAULT_GPU_TYPE));
    EXPECT_TRUE(firstPublished.Get().Get().instances().contains(instance.instanceid()));
    EXPECT_DOUBLE_EQ(
        firstPublished.Get().Get().allocatable().resources().at(RESOURCE_CPU_NAME).scalar().value(),
        firstUnit.allocatable().resources().at(RESOURCE_CPU_NAME).scalar().value()
            - instance.resources().resources().at(RESOURCE_CPU_NAME).scalar().value());

    auto secondPublished = viewPtr->GetResourceUnit(secondUnit.id());
    ASSERT_AWAIT_READY(secondPublished);
    ASSERT_TRUE(secondPublished.Get().IsSome());
    EXPECT_TRUE(secondPublished.Get().Get().capacity().resources().contains(DEFAULT_GPU_TYPE));
    EXPECT_TRUE(secondPublished.Get().Get().allocatable().resources().contains(DEFAULT_GPU_TYPE));

    auto view = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(view);
    ASSERT_TRUE(view.Get()->capacity().resources().contains(DEFAULT_GPU_TYPE));
    ASSERT_TRUE(view.Get()->allocatable().resources().contains(DEFAULT_GPU_TYPE));
    const auto &capacityGpu =
        view.Get()->capacity().resources().at(DEFAULT_GPU_TYPE).vectors().values().at(
            HETEROGENEOUS_CARDNUM_KEY);
    const auto &allocatableGpu =
        view.Get()->allocatable().resources().at(DEFAULT_GPU_TYPE).vectors().values().at(
            HETEROGENEOUS_CARDNUM_KEY);
    EXPECT_EQ(capacityGpu.vectors_size(), 1);
    EXPECT_EQ(allocatableGpu.vectors_size(), 1);
    ASSERT_EQ(capacityGpu.vectors().begin()->second.values_size(), 1);
    ASSERT_EQ(allocatableGpu.vectors().begin()->second.values_size(), 1);
    EXPECT_EQ(capacityGpu.vectors().begin()->second.values(0), 1);
    EXPECT_EQ(allocatableGpu.vectors().begin()->second.values(0), 1);
    EXPECT_TRUE(view.Get()->instances().contains(instance.instanceid()));
}

TEST_F(ResourceViewTest, ResourceUnitWithInstances)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;

    auto view = resourceView.GetResourceView();
    EXPECT_EQ(view.Get()->fragment_size(), 0);

    auto unit = Get1DResourceUnitWithInstances();
    auto [unitProportion, unitMem] = GetMinimumUnitBucketIndex(unit.allocatable());
    auto ret = resourceView.AddResourceUnit(unit);
    ASSERT_AWAIT_READY(ret);

    auto view2 = resourceView.GetResourceView();
    ASSERT_EQ(view2.Get()->fragment_size(), 1);
    EXPECT_EQ(view2.Get()->instances().size(), static_cast<uint32_t>(2));
    EXPECT_EQ(view2.Get()->instances().at(unit.instances().begin()->first).instanceid(),
              unit.instances().begin()->second.instanceid());
    EXPECT_EQ(view2.Get()->instances().at((++unit.instances().begin())->first).instanceid(),
              (++unit.instances().begin())->second.instanceid());
    auto bucket = view2.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().sharednum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit.id()).sharednum(), 1);

    auto rUnit = resourceView.GetResourceUnit(unit.id());
    ASSERT_TRUE(rUnit.Get().IsSome());
    EXPECT_EQ(rUnit.Get().Get().id(), unit.id());
    EXPECT_EQ(rUnit.Get().Get().instances().at(unit.instances().begin()->first).instanceid(),
              unit.instances().begin()->second.instanceid());
    EXPECT_EQ((rUnit.Get().Get().instances().at((++unit.instances().begin())->first)).instanceid(),
              (++unit.instances().begin())->second.instanceid());

    // delete unit
    ret = viewPtr->DeleteResourceUnit(unit.id());
    ASSERT_AWAIT_READY(ret);
    auto rView = viewPtr->GetResourceView();
    EXPECT_EQ(rView.Get()->fragment_size(), 0);
    EXPECT_TRUE(rView.Get()->capacity() == Get0CpuMemResources());
    EXPECT_TRUE(rView.Get()->allocatable() == Get0CpuMemResources());
    EXPECT_TRUE(rView.Get()->fragment().find(unit.id()) == rView.Get()->fragment().end());
}

TEST_F(ResourceViewTest, GetUnitByInstReqID)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;

    auto view = resourceView.GetResourceView();
    EXPECT_EQ(view.Get()->fragment_size(), 0);

    auto unit1 = Get1DResourceUnitWithInstances();
    auto unit2 = Get1DResourceUnitWithInstances();
    auto reqID11 = unit1.instances().begin()->second.requestid();
    auto reqID12 = (++unit1.instances().begin())->second.requestid();
    auto reqID21 = unit2.instances().begin()->second.requestid();
    auto reqID22 = (++unit2.instances().begin())->second.requestid();
    auto ret = resourceView.AddResourceUnit(unit1);
    ASSERT_AWAIT_READY(ret);
    ret = resourceView.AddResourceUnit(unit2);
    ASSERT_AWAIT_READY(ret);

    auto view2 = resourceView.GetResourceView();
    ASSERT_EQ(view2.Get()->fragment_size(), 2);
    EXPECT_EQ(view2.Get()->instances().size(), static_cast<uint32_t>(4));
    auto retUnitID = resourceView.GetUnitByInstReqID("");
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_TRUE(retUnitID.Get().IsNone());

    retUnitID = resourceView.GetUnitByInstReqID("X");
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_TRUE(retUnitID.Get().IsNone());

    retUnitID = resourceView.GetUnitByInstReqID(reqID11);
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_TRUE(retUnitID.Get().IsSome());
    EXPECT_TRUE(retUnitID.Get().Get() == unit1.id());

    retUnitID = resourceView.GetUnitByInstReqID(reqID12);
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_TRUE(retUnitID.Get().IsSome());
    EXPECT_TRUE(retUnitID.Get().Get() == unit1.id());

    retUnitID = resourceView.GetUnitByInstReqID(reqID21);
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_TRUE(retUnitID.Get().IsSome());
    EXPECT_TRUE(retUnitID.Get().Get() == unit2.id());

    retUnitID = resourceView.GetUnitByInstReqID(reqID22);
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_TRUE(retUnitID.Get().IsSome());
    EXPECT_TRUE(retUnitID.Get().Get() == unit2.id());

    ret = resourceView.DeleteResourceUnit(unit2.id());
    ASSERT_AWAIT_READY(ret);

    retUnitID = resourceView.GetUnitByInstReqID(reqID11);
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_TRUE(retUnitID.Get().IsSome());
    EXPECT_TRUE(retUnitID.Get().Get() == unit1.id());

    retUnitID = resourceView.GetUnitByInstReqID(reqID12);
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_TRUE(retUnitID.Get().IsSome());
    EXPECT_TRUE(retUnitID.Get().Get() == unit1.id());

    retUnitID = resourceView.GetUnitByInstReqID(reqID21);
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_FALSE(retUnitID.Get().IsSome());

    retUnitID = resourceView.GetUnitByInstReqID(reqID22);
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_FALSE(retUnitID.Get().IsSome());

    ret = resourceView.DeleteResourceUnit(unit1.id());
    ASSERT_AWAIT_READY(ret);

    retUnitID = resourceView.GetUnitByInstReqID(reqID11);
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_FALSE(retUnitID.Get().IsSome());

    retUnitID = resourceView.GetUnitByInstReqID(reqID12);
    ASSERT_AWAIT_READY(retUnitID);
    EXPECT_FALSE(retUnitID.Get().IsSome());

    ret = resourceView.AddResourceUnit(unit1);
    ASSERT_AWAIT_READY(ret);
    ret = resourceView.AddResourceUnit(unit2);
    ASSERT_AWAIT_READY(ret);
}

TEST_F(ResourceViewTest, AddResourceUnitError)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto view = viewPtr->GetResourceView();
    ASSERT_EQ(view.Get()->fragment_size(), 0);

    auto unit = Get1DResourceUnit();
    unit.clear_id();
    EXPECT_FALSE(viewPtr->AddResourceUnit(unit).Get().IsOk());

    unit = Get1DResourceUnit();
    unit.clear_capacity();
    EXPECT_FALSE(viewPtr->AddResourceUnit(unit).Get().IsOk());

    unit = Get1DResourceUnit();
    unit.clear_allocatable();
    EXPECT_FALSE(viewPtr->AddResourceUnit(unit).Get().IsOk());

    unit = Get1DResourceUnit();
    unit.mutable_capacity()->mutable_resources()->clear();
    EXPECT_FALSE(viewPtr->AddResourceUnit(unit).Get().IsOk());

    unit = Get1DResourceUnit();
    unit.mutable_allocatable()->mutable_resources()->clear();
    EXPECT_FALSE(viewPtr->AddResourceUnit(unit).Get().IsOk());

    unit = Get1DResourceUnit();
    unit.mutable_capacity()->mutable_resources()->begin()->second.mutable_scalar()->set_value(-1.0);
    EXPECT_TRUE(viewPtr->AddResourceUnit(unit).Get().IsOk());

    unit = Get1DResourceUnit();
    unit.mutable_allocatable()->mutable_resources()->begin()->second.mutable_scalar()->set_value(-1.0);
    EXPECT_TRUE(viewPtr->AddResourceUnit(unit).Get().IsOk());

    unit = Get1DResourceUnit();
    ASSERT_TRUE(viewPtr->AddResourceUnit(unit).Get().IsOk());
    EXPECT_FALSE(viewPtr->AddResourceUnit(unit).Get().IsOk());
}

TEST_F(ResourceViewTest, DeleteResourceUnitError)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto view = viewPtr->GetResourceView();
    ASSERT_EQ(view.Get()->fragment_size(), 0);

    auto unit = Get1DResourceUnit();
    ASSERT_TRUE(viewPtr->AddResourceUnit(unit).Get().IsOk());

    EXPECT_FALSE(viewPtr->DeleteResourceUnit("").Get().IsOk());
    EXPECT_FALSE(viewPtr->DeleteResourceUnit("X").Get().IsOk());
    EXPECT_FALSE(viewPtr->DeleteResourceUnit(unit.id() + "X").Get().IsOk());
}

TEST_F(ResourceViewTest, UpdateResourceUnitError)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto view = viewPtr->GetResourceView();
    ASSERT_EQ(view.Get()->fragment_size(), 0);

    auto unit = std::make_shared<ResourceUnit>(Get1DResourceUnit());
    unit->clear_id();
    unit->set_revision(unit->revision() + 1);
    EXPECT_FALSE(viewPtr->UpdateResourceUnit(unit, UpdateType::UPDATE_ACTUAL).Get().IsOk());

    unit = std::make_shared<ResourceUnit>(Get1DResourceUnit());
    unit->clear_capacity();
    unit->set_revision(unit->revision() + 1);
    EXPECT_FALSE(viewPtr->UpdateResourceUnit(unit, UpdateType::UPDATE_ACTUAL).Get().IsOk());

    unit = std::make_shared<ResourceUnit>(Get1DResourceUnit());
    unit->clear_allocatable();
    unit->set_revision(unit->revision() + 1);
    EXPECT_FALSE(viewPtr->UpdateResourceUnit(unit, UpdateType::UPDATE_ACTUAL).Get().IsOk());

    unit = std::make_shared<ResourceUnit>(Get1DResourceUnit());
    unit->mutable_capacity()->mutable_resources()->clear();
    unit->set_revision(unit->revision() + 1);
    EXPECT_FALSE(viewPtr->UpdateResourceUnit(unit, UpdateType::UPDATE_ACTUAL).Get().IsOk());

    unit = std::make_shared<ResourceUnit>(Get1DResourceUnit());
    unit->mutable_allocatable()->mutable_resources()->clear();
    unit->set_revision(unit->revision() + 1);
    EXPECT_FALSE(viewPtr->UpdateResourceUnit(unit, UpdateType::UPDATE_ACTUAL).Get().IsOk());

    unit = std::make_shared<ResourceUnit>(Get1DResourceUnit());
    unit->mutable_capacity()->mutable_resources()->begin()->second.mutable_scalar()->set_value(-1.0);
    unit->set_revision(unit->revision() + 1);
    EXPECT_FALSE(viewPtr->UpdateResourceUnit(unit, UpdateType::UPDATE_ACTUAL).Get().IsOk());

    unit = std::make_shared<ResourceUnit>(Get1DResourceUnit());
    unit->mutable_allocatable()->mutable_resources()->begin()->second.mutable_scalar()->set_value(-1.0);
    unit->set_revision(unit->revision() + 1);
    EXPECT_FALSE(viewPtr->UpdateResourceUnit(unit, UpdateType::UPDATE_ACTUAL).Get().IsOk());

    unit = std::make_shared<ResourceUnit>(Get1DResourceUnit());
    ASSERT_TRUE(viewPtr->AddResourceUnit(*unit).Get().IsOk());
    unit->set_revision(unit->revision() + 1);
    EXPECT_FALSE(viewPtr->UpdateResourceUnit(unit, UpdateType::UPDATE_STATIC).Get().IsOk());
    (*unit->mutable_id()) = unit->id() + "X";
    unit->set_revision(unit->revision() + 1);
    EXPECT_FALSE(viewPtr->UpdateResourceUnit(unit, UpdateType::UPDATE_ACTUAL).Get().IsOk());
    unit->mutable_id()->clear();
    unit->set_revision(unit->revision() + 1);
    EXPECT_FALSE(viewPtr->UpdateResourceUnit(unit, UpdateType::UPDATE_ACTUAL).Get().IsOk());
}

TEST_F(ResourceViewTest, AddInstancesError)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);

    auto unit1 = Get1DResourceUnit();
    auto inst = Get1DInstance();
    (*inst.mutable_schedulerchain()->Add()) = unit1.id();
    inst.set_unitid(unit1.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instancesOrig;
    instancesOrig.emplace(inst.instanceid(), resource_view::InstanceAllocatedInfo{ inst, nullptr });
    ASSERT_TRUE(viewPtr->AddResourceUnit(unit1).Get().IsOk());
    ASSERT_TRUE(viewPtr->AddInstances(instancesOrig).Get().IsOk());
    EXPECT_FALSE(viewPtr->AddInstances(instancesOrig).Get().IsOk());
    ASSERT_TRUE(viewPtr->DeleteInstances({ inst.instanceid() }).Get().IsOk());

    std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
    instances = instancesOrig;
    instances.clear();
    EXPECT_FALSE(viewPtr->AddInstances(instances).Get().IsOk());

    instances.clear();
    auto inst1 = inst;
    (*inst1.mutable_instanceid()) = inst1.instanceid() + "X";
    instances.insert({ inst.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr } });
    EXPECT_FALSE(viewPtr->AddInstances(instances).Get().IsOk());

    instances.clear();
    inst1 = inst;
    (*inst1.mutable_instanceid()) = inst1.instanceid() + "X";
    instances.insert({ "X", resource_view::InstanceAllocatedInfo{ inst1, nullptr } });
    EXPECT_FALSE(viewPtr->AddInstances(instances).Get().IsOk());

    instances.clear();
    inst1 = inst;
    instances.insert({ "", resource_view::InstanceAllocatedInfo{ inst1, nullptr } });
    EXPECT_FALSE(viewPtr->AddInstances(instances).Get().IsOk());

    instances.clear();
    inst1 = inst;
    instances.insert({ "X", resource_view::InstanceAllocatedInfo{ inst1, nullptr } });
    EXPECT_FALSE(viewPtr->AddInstances(instances).Get().IsOk());

    instances.clear();
    inst1 = inst;
    inst1.clear_resources();
    instances.insert({ inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr } });
    EXPECT_FALSE(viewPtr->AddInstances(instances).Get().IsOk());

    instances.clear();
    inst1 = inst;
    inst1.mutable_resources()->mutable_resources()->begin()->second.mutable_scalar()->set_value(-1);
    instances.insert({ inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr } });
    EXPECT_TRUE(viewPtr->AddInstances(instances).Get().IsOk());

    instances.clear();
    inst1 = inst;
    inst1.set_unitid("");
    instances.insert({ inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr } });
    EXPECT_FALSE(viewPtr->AddInstances(instances).Get().IsOk());

    instances.clear();
    inst1 = inst;
    inst1.set_unitid("tmp");
    instances.insert({ inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr } });
    EXPECT_FALSE(viewPtr->AddInstances(instances).Get().IsOk());
}

TEST_F(ResourceViewTest, DeleteInstancesError)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);

    auto unit1 = Get1DResourceUnit();
    auto inst = Get1DInstance();
    std::vector<std::string> idsOri;
    idsOri.push_back(inst.instanceid());
    (*inst.mutable_schedulerchain()->Add()) = unit1.id();
    inst.set_unitid(unit1.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instancesOrig;
    instancesOrig.emplace(inst.instanceid(), resource_view::InstanceAllocatedInfo{ inst, nullptr });
    ASSERT_TRUE(viewPtr->AddResourceUnit(unit1).Get().IsOk());
    ASSERT_TRUE(viewPtr->AddInstances(instancesOrig).Get().IsOk());
    ASSERT_TRUE(viewPtr->DeleteInstances(idsOri).Get().IsOk());
    ASSERT_TRUE(viewPtr->AddInstances(instancesOrig).Get().IsOk());

    auto ids = idsOri;
    ids.clear();
    ASSERT_FALSE(viewPtr->DeleteInstances(ids).Get().IsOk());

    ids = idsOri;
    ids[0] = ids[0] + "X";
    ASSERT_FALSE(viewPtr->DeleteInstances(ids).Get().IsOk());

    ids = idsOri;
    ASSERT_TRUE(viewPtr->DeleteInstances(ids).Get().IsOk());
}

TEST_F(ResourceViewTest, GetResourceUnitError)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto rView = viewPtr->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);

    auto unit1 = Get1DResourceUnit();
    auto inst = Get1DInstance();
    (*inst.mutable_schedulerchain()->Add()) = unit1.id();
    inst.set_unitid(unit1.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instancesOrig;
    instancesOrig.emplace(inst.instanceid(), resource_view::InstanceAllocatedInfo{ inst, nullptr });
    ASSERT_TRUE(viewPtr->AddResourceUnit(unit1).Get().IsOk());
    ASSERT_TRUE(viewPtr->AddInstances(instancesOrig).Get().IsOk());
    ASSERT_TRUE(viewPtr->GetResourceUnit(unit1.id()).Get().IsSome());

    std::string unitID;
    ASSERT_FALSE(viewPtr->GetResourceUnit(unitID).Get().IsSome());

    unitID = unit1.id() + "X";
    ASSERT_FALSE(viewPtr->GetResourceUnit(unitID).Get().IsSome());
}

TEST_F(ResourceViewTest, ResourceUnitIncreamentChange)
{
    litebus::Future<Status> ret;
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;

    auto view = resourceView.GetResourceView();
    auto initRevision = view.Get()->revision();
    EXPECT_EQ(int(initRevision), 0);

    // 1.add resourceunit
    auto unit = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit);
    ret = resourceView.AddResourceUnit(unit);
    ASSERT_AWAIT_READY(ret);

    auto changes = resourceView.GetVersionChanges();
    auto current_revision = 1;
    size_t changeTimes = 1;
    EXPECT_EQ(changes.size(), changeTimes);
    EXPECT_EQ(changes[current_revision].Changed_case(), ResourceUnitChange::kAddition);
    EXPECT_EQ(changes[current_revision].addition().resourceunit().id(), unit.id());

     // 2.add instance
     auto inst1 = Get1DInstance();
    (*inst1.mutable_schedulerchain()->Add()) = unit.id();
    inst1.set_unitid(unit.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
    instances.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    ret = resourceView.AddInstances(instances);
    ASSERT_AWAIT_READY(ret);

    changes = resourceView.GetVersionChanges();
    current_revision += 1;
    changeTimes += 1;
    EXPECT_EQ(changes.size(), changeTimes);
    EXPECT_EQ(changes[current_revision].Changed_case(), ResourceUnitChange::kModification);
    EXPECT_EQ(changes[current_revision].resourceunitid(), unit.id());
    EXPECT_EQ(changes[current_revision].modification().instancechanges(0).instance().unitid(), unit.id());
    EXPECT_EQ(changes[current_revision].modification().instancechanges(0).changetype(), InstanceChange::ADD);

    // 3.del instance
    std::vector<std::string> ids1;
    ids1.push_back(inst1.instanceid());
    ret = resourceView.DeleteInstances(ids1);
    ASSERT_AWAIT_READY(ret);

    changes = resourceView.GetVersionChanges();
    current_revision += 1;
    changeTimes += 1;
    EXPECT_EQ(changes.size(), changeTimes);
    EXPECT_EQ(changes[current_revision].Changed_case(), ResourceUnitChange::kModification);
    EXPECT_EQ(changes[current_revision].resourceunitid(), unit.id());
    EXPECT_EQ(changes[current_revision].modification().instancechanges(0).changetype(), InstanceChange::DELETE);

    // 4.update status
    ret = resourceView.UpdateUnitStatus(unit.id(), resource_view::UnitStatus::EVICTING);
    ASSERT_AWAIT_READY(ret);

    changes = resourceView.GetVersionChanges();
    current_revision += 1;
    changeTimes += 1;
    EXPECT_EQ(changes.size(), changeTimes);
    EXPECT_EQ(changes[current_revision].Changed_case(), ResourceUnitChange::kModification);
    EXPECT_EQ(changes[current_revision].resourceunitid(), unit.id());
    EXPECT_EQ(changes[current_revision].modification().statuschange().status(),
        static_cast<uint32_t>(resource_view::UnitStatus::EVICTING));

    // 5.del resourceunit
    ret = resourceView.DeleteResourceUnit(unit.id());
    ASSERT_AWAIT_READY(ret);

    changes = resourceView.GetVersionChanges();
    current_revision += 1;
    changeTimes += 1;
    EXPECT_EQ(changes.size(), changeTimes);
    EXPECT_EQ(changes[current_revision].Changed_case(), ResourceUnitChange::kDeletion);
    EXPECT_EQ(changes[current_revision].resourceunitid(), unit.id());
}

TEST_F(ResourceViewTest, MergeResourceViewChanges)
{
    litebus::Future<Status> ret;
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;

    auto initRevision = 0;
    auto masterRevisionInPullRequest = 0;
    uint64_t currentLocalRevision = 0;

    // Modify the local resourceview
    // 1.add resourceunit
    auto currentRevision1 = 1;
    auto unit = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit);
    ret = resourceView.AddResourceUnit(unit);
    ASSERT_AWAIT_READY(ret);

    // 2.modify resourceunit -- add instance1
    auto currentRevision2 = 2;
    auto inst1 = Get1DInstance();
    (*inst1.mutable_schedulerchain()->Add()) = unit.id();
    inst1.mutable_scheduleoption()->set_schedpolicyname(MONOPOLY_SCHEDULE);
    inst1.set_unitid(unit.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances1;
    instances1.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    ret = resourceView.AddInstances(instances1);
    ASSERT_AWAIT_READY(ret);

    // 3.modify resourceunit -- add instance2
    auto currentRevision3 = 3;
    auto inst2 = Get1DInstance();
    (*inst2.mutable_schedulerchain()->Add()) = unit.id();
    inst2.mutable_scheduleoption()->set_schedpolicyname(MONOPOLY_SCHEDULE);
    inst2.set_unitid(unit.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
    instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    ret = resourceView.AddInstances(instances2);
    ASSERT_AWAIT_READY(ret);

    // 4.modify resourceunit -- delete instance2
    auto currentRevision4 = 4;
    std::vector<std::string> ids;
    ids.push_back(inst2.instanceid());
    ret = resourceView.DeleteInstances(ids);
    ASSERT_AWAIT_READY(ret);

    // 5.modify resourceunit -- add instance2
    auto currentRevision5 = 5;
    ret = resourceView.AddInstances(instances2);
    ASSERT_AWAIT_READY(ret);

    // 6.modify resourceunit -- modify status
    auto currentRevision6 = 6;
    ret = resourceView.UpdateUnitStatus(unit.id(), resource_view::UnitStatus::EVICTING);
    ASSERT_AWAIT_READY(ret);

    // 7.delete resourceunit
    auto currentRevision7 = 7;
    ret = resourceView.DeleteResourceUnit(unit.id());
    ASSERT_AWAIT_READY(ret);

    // There are multiple situations to summarize the local resource view
    // Situation 1: add resourceunit
    ResourceUnitChanges result1;
    masterRevisionInPullRequest = initRevision;
    currentLocalRevision = currentRevision1;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result1);
    const ResourceUnitChange &change1 = result1.changes(0);
    EXPECT_EQ(result1.endrevision(), currentLocalRevision);
    EXPECT_EQ(result1.changes_size(), 1);
    EXPECT_EQ(change1.resourceunitid(), unit.id());
    EXPECT_EQ(change1.Changed_case(), ResourceUnitChange::kAddition);
    EXPECT_EQ(change1.addition().resourceunit().id(), unit.id());

    // Situation 2: modify resourceunit -- add instance
    ResourceUnitChanges result2;
    masterRevisionInPullRequest = currentRevision1;
    currentLocalRevision = currentRevision2;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result2);
    const ResourceUnitChange &change2 = result2.changes(0);
    EXPECT_EQ(result2.endrevision(), currentLocalRevision);
    EXPECT_EQ(result2.changes_size(), 1);
    EXPECT_EQ(change2.resourceunitid(), unit.id());
    EXPECT_EQ(change2.Changed_case(), ResourceUnitChange::kModification);
    EXPECT_EQ(change2.modification().instancechanges(0).changetype(), InstanceChange::ADD);
    EXPECT_EQ(change2.modification().instancechanges(0).instance().instanceid(), inst1.instanceid());

    // Situation 3: del resourceunit
    ResourceUnitChanges result3;
    masterRevisionInPullRequest = currentRevision6;
    currentLocalRevision = currentRevision7;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result3);
    const ResourceUnitChange &change3 = result3.changes(0);
    EXPECT_EQ(result3.endrevision(), currentLocalRevision);
    EXPECT_EQ(result3.changes_size(), 1);
    EXPECT_EQ(change3.resourceunitid(), unit.id());
    EXPECT_EQ(change3.Changed_case(), ResourceUnitChange::kDeletion);

    // Situation 4: add resourceunit + modify resourceunit(modify status)
    //          --> add resourceunit
    ResourceUnitChanges result4;
    masterRevisionInPullRequest = initRevision;
    currentLocalRevision = currentRevision6;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result4);
    const ResourceUnitChange &change4 = result4.changes(0);
    EXPECT_EQ(result4.endrevision(), currentLocalRevision);
    EXPECT_EQ(result4.changes_size(), 1);
    EXPECT_EQ(change4.resourceunitid(), unit.id());
    EXPECT_EQ(change4.Changed_case(), ResourceUnitChange::kAddition);
    EXPECT_EQ(change4.addition().resourceunit().status(),  static_cast<uint32_t>(resource_view::UnitStatus::EVICTING));

    // Situation 5: add resourceunit + any changes + delete resourceunit
    //          --> no changes
    ResourceUnitChanges result5;
    masterRevisionInPullRequest = initRevision;
    currentLocalRevision = currentRevision7;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result5);
    EXPECT_EQ(result5.endrevision(), currentLocalRevision);
    EXPECT_EQ(result5.changes_size(), 0);

    // Situation 6: modify resourceunit(add instance) + modify resourceunit(delete instance)
    //          --> no changes
    ResourceUnitChanges result6;
    masterRevisionInPullRequest = currentRevision2;
    currentLocalRevision = currentRevision4;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result6);
    EXPECT_EQ(result6.endrevision(), currentLocalRevision);
    EXPECT_EQ(result6.changes_size(), 0);

    // Situation 7: modify resourceunit(modify status) + delete resourceunit
    //          --> delete resourceunit
    ResourceUnitChanges result7;
    masterRevisionInPullRequest = currentRevision4;
    currentLocalRevision = currentRevision7;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result7);
    const ResourceUnitChange &change7 = result7.changes(0);
    EXPECT_EQ(result7.endrevision(), currentLocalRevision);
    EXPECT_EQ(result7.changes_size(), 1);
    EXPECT_EQ(change7.resourceunitid(), unit.id());
    EXPECT_EQ(change7.Changed_case(), ResourceUnitChange::kDeletion);

    // Situation 8: modify resourceunit(add instance1) + modify resourceunit(add instance2) +
    //              modify resourceunit(delete instance2)
    //          --> modify resourceunit(add instance1)
    ResourceUnitChanges result8;
    masterRevisionInPullRequest = currentRevision1;
    currentLocalRevision = currentRevision4;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result8);
    const ResourceUnitChange &change8 = result8.changes(0);
    EXPECT_EQ(result8.endrevision(), currentLocalRevision);
    EXPECT_EQ(change8.Changed_case(), ResourceUnitChange::kModification);
    EXPECT_EQ(change8.modification().instancechanges(0).changetype(), InstanceChange::ADD);
    EXPECT_EQ(change8.modification().instancechanges(0).instance().instanceid(), inst1.instanceid());

    // Situation 9: modify resourceunit(delete instance2) + modify resourceunit(add instance2)
    //          --> modify resourceunit(delete instance2 + add instance2)
    ResourceUnitChanges result9;
    masterRevisionInPullRequest = currentRevision3;
    currentLocalRevision = currentRevision5;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result9);
    EXPECT_EQ(result9.changes_size(), 1);
    const ResourceUnitChange &change9 = result9.changes(0);
    EXPECT_EQ(result9.endrevision(), currentLocalRevision);
    EXPECT_EQ(change9.Changed_case(), ResourceUnitChange::kModification);
    EXPECT_EQ(change9.modification().instancechanges(0).changetype(), InstanceChange::DELETE);
    EXPECT_EQ(change9.modification().instancechanges(0).instance().instanceid(), inst2.instanceid());
    EXPECT_EQ(change9.modification().instancechanges(1).changetype(), InstanceChange::ADD);
    EXPECT_EQ(change9.modification().instancechanges(1).instance().instanceid(), inst2.instanceid());
}

TEST_F(ResourceViewTest, MergeInstanceChange)
{
    litebus::Future<Status> ret;
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;

    auto masterRevisionInPullRequest = 0;
    uint64_t currentLocalRevision = 0;

    // Modify the local resourceview
    // 1.add resourceunit
    auto unit = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit);
    ret = resourceView.AddResourceUnit(unit);
    ASSERT_AWAIT_READY(ret);

    // 2.modify resourceunit -- add instance1
    auto inst1 = Get1DInstance();
    (*inst1.mutable_schedulerchain()->Add()) = unit.id();
    inst1.mutable_scheduleoption()->set_schedpolicyname(MONOPOLY_SCHEDULE);
    inst1.set_unitid(unit.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances1;
    instances1.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    ret = resourceView.AddInstances(instances1);
    ASSERT_AWAIT_READY(ret);

    // 3.modify resourceunit -- add instance2
    auto currentRevision3 = 3;
    auto inst2 = Get1DInstance();
    (*inst2.mutable_schedulerchain()->Add()) = unit.id();
    inst2.mutable_scheduleoption()->set_schedpolicyname(MONOPOLY_SCHEDULE);
    inst2.set_unitid(unit.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
    instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    ret = resourceView.AddInstances(instances2);
    ASSERT_AWAIT_READY(ret);

    // 4.modify resourceunit -- delete instance2
    auto currentRevision4 = 4;
    std::vector<std::string> ids;
    ids.push_back(inst2.instanceid());
    ret = resourceView.DeleteInstances(ids);
    ASSERT_AWAIT_READY(ret);

    // 5.modify resourceunit -- add instance2
    auto currentRevision5 = 5;
    ret = resourceView.AddInstances(instances2);
    ASSERT_AWAIT_READY(ret);

    // 6.modify resourceunit -- delete instance2
    auto currentRevision6 = 6;
    std::vector<std::string> ids1;
    ids1.push_back(inst2.instanceid());
    ret = resourceView.DeleteInstances(ids1);
    ASSERT_AWAIT_READY(ret);

    // 7.modify resourceunit -- add instance2
    auto currentRevision7 = 7;
    ret = resourceView.AddInstances(instances2);
    ASSERT_AWAIT_READY(ret);

    // 8.modify resourceunit -- delete instance2
    auto currentRevision8 = 8;
    std::vector<std::string> ids2;
    ids2.push_back(inst2.instanceid());
    ret = resourceView.DeleteInstances(ids2);
    ASSERT_AWAIT_READY(ret);

    // Situation 1: delete instance2 + add instance2
    //          --> delete instance2 + add instance2
    auto result = ResourceUnitChanges{};
    masterRevisionInPullRequest = currentRevision3;
    currentLocalRevision = currentRevision5;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result);
    EXPECT_EQ(result.endrevision(), currentLocalRevision);
    EXPECT_EQ(result.changes_size(), 1);
    auto change = result.changes(0);
    EXPECT_EQ(change.Changed_case(), ResourceUnitChange::kModification);
    EXPECT_EQ(change.modification().instancechanges(0).changetype(), InstanceChange::DELETE);
    EXPECT_EQ(change.modification().instancechanges(0).instance().instanceid(), inst2.instanceid());
    EXPECT_EQ(change.modification().instancechanges(1).changetype(), InstanceChange::ADD);
    EXPECT_EQ(change.modification().instancechanges(1).instance().instanceid(), inst2.instanceid());

    // Situation 2: add instance2 + delete instance2
    //          --> no changes
    result = ResourceUnitChanges{};
    masterRevisionInPullRequest = currentRevision4;
    currentLocalRevision = currentRevision6;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result);
    EXPECT_EQ(result.endrevision(), currentLocalRevision);
    EXPECT_EQ(result.changes_size(), 0);

    // Situation 3: delete instance2 + add instance2 + delete instance2
    //          --> delete instance2
    result = ResourceUnitChanges{};
    masterRevisionInPullRequest = currentRevision3;
    currentLocalRevision = currentRevision6;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result);
    EXPECT_EQ(result.changes_size(), 1);
    change = result.changes(0);
    EXPECT_EQ(result.endrevision(), currentLocalRevision);
    EXPECT_EQ(change.Changed_case(), ResourceUnitChange::kModification);
    EXPECT_EQ(change.modification().instancechanges(0).changetype(), InstanceChange::DELETE);
    EXPECT_EQ(change.modification().instancechanges(0).instance().instanceid(), inst2.instanceid());

    // Situation 4: add instance2 + delete instance2 + add instance2
    //          --> add instance2
    result = ResourceUnitChanges{};
    masterRevisionInPullRequest = currentRevision4;
    currentLocalRevision = currentRevision7;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result);
    EXPECT_EQ(result.changes_size(), 1);
    change = result.changes(0);
    EXPECT_EQ(result.endrevision(), currentLocalRevision);
    EXPECT_EQ(change.Changed_case(), ResourceUnitChange::kModification);
    EXPECT_EQ(change.modification().instancechanges(0).changetype(), InstanceChange::ADD);
    EXPECT_EQ(change.modification().instancechanges(0).instance().instanceid(), inst2.instanceid());

    // Situation 5: delete instance2 + add instance2 + delete instance2 + add instance2
    //          --> delete instance2 + add instance2
    result = ResourceUnitChanges{};
    masterRevisionInPullRequest = currentRevision3;
    currentLocalRevision = currentRevision7;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result);
    EXPECT_EQ(result.changes_size(), 1);
    change = result.changes(0);
    EXPECT_EQ(change.Changed_case(), ResourceUnitChange::kModification);
    EXPECT_EQ(change.modification().instancechanges(0).changetype(), InstanceChange::DELETE);
    EXPECT_EQ(change.modification().instancechanges(0).instance().instanceid(), inst2.instanceid());
    EXPECT_EQ(change.modification().instancechanges(1).changetype(), InstanceChange::ADD);
    EXPECT_EQ(change.modification().instancechanges(1).instance().instanceid(), inst2.instanceid());

    // Situation 6: add instance2 + delete instance2 + add instance2 + delete instance2
    //          --> no changes
    result = ResourceUnitChanges{};
    masterRevisionInPullRequest = currentRevision4;
    currentLocalRevision = currentRevision8;
    resourceView.MergeLocalResourceViewChanges(masterRevisionInPullRequest, currentLocalRevision, result);
    EXPECT_EQ(result.changes_size(), 0);
}

TEST_F(ResourceViewTest, MaintainsResourceUnitChangesOrderDuringMerge)
{
    litebus::Future<Status> ret;
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;

    // 1.add resourceunit 1
    auto currentRevision1 = 1;
    auto unit1 = Get1DResourceUnit();
    ret = resourceView.AddResourceUnit(unit1);
    ASSERT_AWAIT_READY(ret);

    // 2.add resourceunit 2
    auto unit2 = Get1DResourceUnit();
    ret = resourceView.AddResourceUnit(unit2);
    ASSERT_AWAIT_READY(ret);

    // 3.del resourceunit 1
    ret = resourceView.DeleteResourceUnit(unit1.id());
    ASSERT_AWAIT_READY(ret);

    // 4.add resourceunit 3
    auto currentRevision4 = 4;
    auto unit3 = Get1DResourceUnit();
    ret = resourceView.AddResourceUnit(unit3);
    ASSERT_AWAIT_READY(ret);

    // resourceunit included in the change: add resourceunit 2, del resourceunit 1, add resourceunit 3
    ResourceUnitChanges result;
    resourceView.MergeLocalResourceViewChanges(currentRevision1, currentRevision4, result);

    EXPECT_EQ(result.changes_size(), 3);
    EXPECT_EQ(result.changes(0).resourceunitid(), unit2.id());
    EXPECT_EQ(result.changes(0).Changed_case(), ResourceUnitChange::kAddition);
    EXPECT_EQ(result.changes(0).addition().resourceunit().id(), unit2.id());

    EXPECT_EQ(result.changes(1).resourceunitid(), unit1.id());
    EXPECT_EQ(result.changes(1).Changed_case(), ResourceUnitChange::kDeletion);

    EXPECT_EQ(result.changes(2).resourceunitid(), unit3.id());
    EXPECT_EQ(result.changes(2).Changed_case(), ResourceUnitChange::kAddition);
    EXPECT_EQ(result.changes(2).addition().resourceunit().id(), unit3.id());
}

TEST_F(ResourceViewTest, MaintainsInstanceChangesOrderDuringMerge)
{
    litebus::Future<Status> ret;
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;

    // 1.add resourceunit
    auto unit = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit);
    ret = resourceView.AddResourceUnit(unit);
    ASSERT_AWAIT_READY(ret);

    // 2.add inst1
    auto currentRevision2 = 2;
    auto inst1 = Get1DInstance();
    (*inst1.mutable_schedulerchain()->Add()) = unit.id();
    inst1.set_unitid(unit.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances1;
    instances1.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    ret = resourceView.AddInstances(instances1);
    ASSERT_AWAIT_READY(ret);

    // 3.add inst2
    auto inst2 = Get1DInstance();
    (*inst2.mutable_schedulerchain()->Add()) = unit.id();
    inst2.set_unitid(unit.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
    instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    ret = resourceView.AddInstances(instances2);
    ASSERT_AWAIT_READY(ret);

    // 4.add inst3
    auto inst3 = Get1DInstance();
    (*inst3.mutable_schedulerchain()->Add()) = unit.id();
    inst3.set_unitid(unit.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances3;
    instances3.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, nullptr });
    ret = resourceView.AddInstances(instances3);
    ASSERT_AWAIT_READY(ret);

    // 5.del inst1
    std::vector<std::string> ids1;
    ids1.push_back(inst1.instanceid());
    ret = resourceView.DeleteInstances(ids1);
    ASSERT_AWAIT_READY(ret);

    // 6.del inst3
    std::vector<std::string> ids2;
    ids2.push_back(inst3.instanceid());
    ret = resourceView.DeleteInstances(ids2);
    ASSERT_AWAIT_READY(ret);

    // 7.del inst2
    std::vector<std::string> ids3;
    ids3.push_back(inst2.instanceid());
    ret = resourceView.DeleteInstances(ids3);
    ASSERT_AWAIT_READY(ret);

    // 8.add inst4
    auto inst4 = Get1DInstance();
    (*inst4.mutable_schedulerchain()->Add()) = unit.id();
    inst4.set_unitid(unit.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances4;
    instances4.emplace(inst4.instanceid(), resource_view::InstanceAllocatedInfo{ inst4, nullptr });
    ret = resourceView.AddInstances(instances4);
    ASSERT_AWAIT_READY(ret);

    // 9.add inst5
    auto currentRevision9 = 9;
    auto inst5 = Get1DInstance();
    (*inst5.mutable_schedulerchain()->Add()) = unit.id();
    inst5.set_unitid(unit.id());
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances5;
    instances5.emplace(inst5.instanceid(), resource_view::InstanceAllocatedInfo{ inst5, nullptr });
    ret = resourceView.AddInstances(instances5);
    ASSERT_AWAIT_READY(ret);

    // Instances included in the change: delete instance1, add instance4, add instance5
    ResourceUnitChanges result;
    resourceView.MergeLocalResourceViewChanges(currentRevision2, currentRevision9, result);

    EXPECT_EQ(result.changes_size(), 1);
    const ResourceUnitChange &change = result.changes(0);
    EXPECT_EQ(change.modification().instancechanges_size(), 3);
    EXPECT_EQ(change.modification().instancechanges(0).changetype(), InstanceChange::DELETE);
    EXPECT_EQ(change.modification().instancechanges(0).instance().instanceid(), inst1.instanceid());

    EXPECT_EQ(change.modification().instancechanges(1).changetype(), InstanceChange::ADD);
    EXPECT_EQ(change.modification().instancechanges(1).instance().instanceid(), inst4.instanceid());

    EXPECT_EQ(change.modification().instancechanges(2).changetype(), InstanceChange::ADD);
    EXPECT_EQ(change.modification().instancechanges(2).instance().instanceid(), inst5.instanceid());
}

TEST_F(ResourceViewTest, DISABLED_PullResourceUnitTest)
{
    class MockUpdateHandler {
    public:
        MOCK_METHOD(void, Update, (), ());
    };
    auto handler = std::make_shared<MockUpdateHandler>();
    EXPECT_CALL(*handler, Update).WillRepeatedly(testing::Return());
    ResourcePoller::SetInterval(100);
    auto parent = resource_view::ResourceView::CreateResourceView(DOMAIN_RESOUCE_VIEW_ID, PARENT_PARAM);
    parent->AddResourceUpdateHandler([handler](){ handler->Update();});
    parent->TriggerTryPull();
    std::string childNode = LOCAL_RESOUCE_VIEW_ID;
    auto child = resource_view::ResourceView::CreateResourceView(childNode, CHILD_PARAM);
    child->ToReady();
    child->UpdateDomainUrlForLocal(domainUrl_);
    auto rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);
    auto add = parent->AddResourceUnitWithUrl(*child->GetFullResourceView().Get(),
                                              litebus::GetActor(childNode +"-ResourceViewActor")->GetAID().Url());
    ASSERT_TRUE(add.Get().IsOk());
    add = parent->AddResourceUnitWithUrl(*child->GetFullResourceView().Get(),
                                         litebus::GetActor(childNode + "-ResourceViewActor")->GetAID().Url());
    ASSERT_FALSE(add.Get().IsOk());

    auto childView = child->GetFullResourceView().Get();
    auto curSnap = parent->GetUnitSnapshotInfo(childNode).Get();
    EXPECT_EQ(curSnap.version(), childView->revision());
    EXPECT_EQ(curSnap.localviewinittime(), childView->viewinittime());

    // 1.add resourceunit
    uint64_t currentRevision1 = 1;
    auto unit = Get1DResourceUnit();
    auto agentId = unit.id();
    GenerateMinimumUnitBucketInfo(unit);
    auto [unitProportion, unitMem] = GetMinimumUnitBucketIndex(unit.allocatable());
    auto begin = litebus::TimeWatch::Now();
    ASSERT_TRUE(child->AddResourceUnit(unit).Get().IsOk());

    // wait until the domain has completed the resourceview update
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 1);
    EXPECT_TRUE(rView.Get()->capacity() == unit.capacity());
    auto rUnitCapacity = rView.Get()->fragment().at(agentId).capacity();
    EXPECT_TRUE(rUnitCapacity == unit.capacity());
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localRevisionInDomain, currentRevision1);
    auto &bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(agentId).monopolynum(), 1);
    bucket = rView.Get()->fragment().at(agentId).bucketindexs().at(unitProportion).buckets().at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.allocatable().at(agentId).monopolynum(), 1);

    auto emptySnap = parent->GetUnitSnapshotInfo("");
    EXPECT_EQ(emptySnap.Get().version(), uint64_t{0});
    EXPECT_TRUE(emptySnap.Get().localviewinittime().empty());

    childView = child->GetFullResourceView().Get();
    curSnap = parent->GetUnitSnapshotInfo(childNode).Get();
    EXPECT_EQ(curSnap.version(), childView->revision());
    EXPECT_EQ(curSnap.localviewinittime(), childView->viewinittime());

    // 2.modify resourceunit -- add instance1(shared)
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    auto inst1 = Get1DInstance();
    (*inst1.mutable_schedulerchain()->Add()) = agentId;
    inst1.set_unitid(agentId);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances1;
    instances1.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    begin = litebus::TimeWatch::Now();
    ASSERT_TRUE(child->AddInstances(instances1).Get().IsOk());

    // wait until the domain has completed the resourceview update
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(1));
    EXPECT_TRUE(rUnitCapacity - rView.Get()->allocatable() == inst1.resources());
    auto &rInstances1 = rView.Get()->instances();
    EXPECT_TRUE(rInstances1.find(inst1.instanceid()) != rInstances1.end());
    auto &rInst1 = rInstances1.at(inst1.instanceid());
    EXPECT_EQ(rInst1.instanceid(), inst1.instanceid());
    auto &rAgentInstances1 = rView.Get()->fragment().at(agentId).instances();
    EXPECT_TRUE(rAgentInstances1.find(inst1.instanceid()) != rAgentInstances1.end());

    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(agentId).monopolynum(), 0);
    bucket = rView.Get()->fragment().at(agentId).bucketindexs().at(unitProportion).buckets().at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.allocatable().at(agentId).monopolynum(), 0);
    childView = child->GetFullResourceView().Get();
    curSnap = parent->GetUnitSnapshotInfo(childNode).Get();
    EXPECT_EQ(curSnap.version(), childView->revision());
    EXPECT_EQ(curSnap.localviewinittime(), childView->viewinittime());

    // 3.modify resourceunit -- add instance2(shared)
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    auto rOriginAllocatable = rView.Get()->allocatable();
    auto inst2 = Get1DInstance();
    (*inst2.mutable_schedulerchain()->Add()) = agentId;
    inst2.set_unitid(agentId);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
    instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    begin = litebus::TimeWatch::Now();
    ASSERT_TRUE(child->AddInstances(instances2).Get().IsOk());

    // wait until the domain has completed the resourceview update
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(2));
    auto rNewAllocatable = rView.Get()->allocatable();
    EXPECT_TRUE(rOriginAllocatable - rNewAllocatable == inst2.resources());
    auto &rInstances2 = rView.Get()->instances();
    EXPECT_TRUE(rInstances2.find(inst2.instanceid()) != rInstances2.end());
    auto &rInst2 = rInstances2.at(inst2.instanceid());
    EXPECT_EQ(rInst2.instanceid(), inst2.instanceid());
    auto &rAgentInstances2 = rView.Get()->fragment().at(agentId).instances();
    EXPECT_TRUE(rAgentInstances2.find(inst2.instanceid()) != rAgentInstances2.end());

    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(agentId).monopolynum(), 0);

    childView = child->GetFullResourceView().Get();
    curSnap = parent->GetUnitSnapshotInfo(childNode).Get();
    EXPECT_EQ(curSnap.version(), childView->revision());
    EXPECT_EQ(curSnap.localviewinittime(), childView->viewinittime());

    // 4.del instance1(shared)
    std::vector<std::string> ids1;
    ids1.push_back(inst1.instanceid());
    begin = litebus::TimeWatch::Now();
    ASSERT_TRUE(child->DeleteInstances(ids1).Get().IsOk());

    // wait until the domain has completed the resourceview update
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    EXPECT_EQ(rView.Get()->instances().size(), size_t{1});
    EXPECT_EQ(rView.Get()->fragment().at(agentId).instances().size(), size_t{1});
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(agentId).monopolynum(), 0);
    bucket = rView.Get()->fragment().at(agentId).bucketindexs().at(unitProportion).buckets().at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.allocatable().at(agentId).monopolynum(), 0);

    childView = child->GetFullResourceView().Get();
    curSnap = parent->GetUnitSnapshotInfo(childNode).Get();
    EXPECT_EQ(curSnap.version(), childView->revision());
    EXPECT_EQ(curSnap.localviewinittime(), childView->viewinittime());

    // 5.del instance2(shared)
    std::vector<std::string> ids2;
    ids2.push_back(inst2.instanceid());
    begin = litebus::TimeWatch::Now();
    ASSERT_TRUE(child->DeleteInstances(ids2).Get().IsOk());

    // wait until the domain has completed the resourceview update
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    EXPECT_EQ(rView.Get()->instances().size(), size_t{0});
    EXPECT_EQ(rView.Get()->fragment().at(agentId).instances().size(), size_t{0});
    EXPECT_TRUE(rUnitCapacity == rView.Get()->allocatable());
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(agentId).monopolynum(), 1);
    bucket = rView.Get()->fragment().at(agentId).bucketindexs().at(unitProportion).buckets().at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.allocatable().at(agentId).monopolynum(), 1);

    childView = child->GetFullResourceView().Get();
    curSnap = parent->GetUnitSnapshotInfo(childNode).Get();
    EXPECT_EQ(curSnap.version(), childView->revision());
    EXPECT_EQ(curSnap.localviewinittime(), childView->viewinittime());

    // 6.update status
    uint64_t currentRevision6 = 6;
    begin = litebus::TimeWatch::Now();
    ASSERT_TRUE(child->UpdateUnitStatus(agentId, resource_view::UnitStatus::EVICTING).Get().IsOk());

    // wait until the domain has completed the resourceview update
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    EXPECT_EQ(rView.Get()->fragment().at(agentId).status(),
        static_cast<uint32_t>(resource_view::UnitStatus::EVICTING));
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localRevisionInDomain, currentRevision6);

    childView = child->GetFullResourceView().Get();
    curSnap = parent->GetUnitSnapshotInfo(childNode).Get();
    EXPECT_EQ(curSnap.version(), childView->revision());
    EXPECT_EQ(curSnap.localviewinittime(), childView->viewinittime());

    // 7.modify resourceunit -- add instance3(monopoly)
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    auto inst3 = Get1DInstance();
    (*inst3.mutable_schedulerchain()->Add()) = agentId;
    inst3.mutable_scheduleoption()->set_schedpolicyname(MONOPOLY_SCHEDULE);
    inst3.set_unitid(agentId);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances3;
    instances3.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, nullptr });
    begin = litebus::TimeWatch::Now();
    ASSERT_TRUE(child->AddInstances(instances3).Get().IsOk());

    // wait until the domain has completed the resourceview update
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    EXPECT_EQ(rView.Get()->instances().size(), static_cast<uint32_t>(1));
    EXPECT_TRUE(IsEmpty(rView.Get()->allocatable()));
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(agentId).monopolynum(), 0);

    childView = child->GetFullResourceView().Get();
    curSnap = parent->GetUnitSnapshotInfo(childNode).Get();
    EXPECT_EQ(curSnap.version(), childView->revision());
    EXPECT_EQ(curSnap.localviewinittime(), childView->viewinittime());

    // 8.del resourceunit
    begin = litebus::TimeWatch::Now();
    ASSERT_TRUE(child->DeleteResourceUnit(agentId).Get().IsOk());

    // wait until the domain has completed the resourceview update
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);
    bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);

    childView = child->GetFullResourceView().Get();
    curSnap = parent->GetUnitSnapshotInfo(childNode).Get();
    EXPECT_EQ(curSnap.version(), childView->revision());
    EXPECT_EQ(curSnap.localviewinittime(), childView->viewinittime());
}

TEST_F(ResourceViewTest, PullResourceUnitWithSwitchDomainUrlTest)
{
    ResourcePoller::SetInterval(50);
    auto parent = resource_view::ResourceView::CreateResourceView(DOMAIN_RESOUCE_VIEW_ID, PARENT_PARAM);
    parent->TriggerTryPull();
    std::string childNode = LOCAL_RESOUCE_VIEW_ID;
    auto child = resource_view::ResourceView::CreateResourceView(childNode, CHILD_PARAM);
    child->ToReady();
    auto rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);
    auto add = parent->AddResourceUnitWithUrl(*child->GetFullResourceView().Get(),
                                              litebus::GetActor(childNode +"-ResourceViewActor")->GetAID().Url());
    ASSERT_TRUE(add.Get().IsOk());

    // local add resourceunit
    uint64_t currentRevision1 = 1;
    auto unit = Get1DResourceUnit();
    auto agentId = unit.id();
    GenerateMinimumUnitBucketInfo(unit);
    auto [unitProportion, unitMem] = GetMinimumUnitBucketIndex(unit.allocatable());
    auto begin = litebus::TimeWatch::Now();
    ASSERT_TRUE(child->AddResourceUnit(unit).Get().IsOk());

    // 1.The domain URL in local is empty
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);

    // 2.The domain URL in local does not match the actual domain URL
    child->UpdateDomainUrlForLocal("fake_url");
    begin = litebus::TimeWatch::Now();
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);

    // 3.The domain URL in local is not empty
    // wait until the domain has completed the resourceview update
    child->UpdateDomainUrlForLocal(domainUrl_);
    begin = litebus::TimeWatch::Now();
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 1);
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localRevisionInDomain, currentRevision1);
}

TEST_F(ResourceViewTest, TestSwitchDomainUrl)
{
    std::string node = LOCAL_RESOUCE_VIEW_ID;
    auto view = resource_view::ResourceView::CreateResourceView(node, CHILD_PARAM);
    view->ToReady();

    auto rView = view->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    auto originChildViewInitTime = rView.Get()->viewinittime();

    // 1.initialize the domain URL for the first time
    view->UpdateDomainUrlForLocal(domainUrl_);
    rView = view->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->viewinittime(), originChildViewInitTime);

    // 2.Update with the different domain URL
    view->UpdateDomainUrlForLocal("new_url");
    rView = view->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_NE(rView.Get()->viewinittime(), originChildViewInitTime);
    originChildViewInitTime = rView.Get()->viewinittime();

    // 3.Update with the same domain URL
    view->UpdateDomainUrlForLocal("new_url");
    rView = view->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->viewinittime(), originChildViewInitTime);
}

TEST_F(ResourceViewTest, AddEmptyUnitWithUrl)
{
    auto parent = resource_view::ResourceView::CreateResourceView(DOMAIN_RESOUCE_VIEW_ID, PARENT_PARAM);
    // adding a empty local view
    std::string childNode = LOCAL_RESOUCE_VIEW_ID;
    auto child = resource_view::ResourceView::CreateResourceView(childNode, CHILD_PARAM);
    child->ToReady();

    auto add = parent->AddResourceUnitWithUrl(*child->GetFullResourceView().Get(),
                                              litebus::GetActor(childNode +"-ResourceViewActor")->GetAID().Url());
    ASSERT_TRUE(add.Get().IsOk());

    ASSERT_TRUE(parent->CheckLocalExistInDomainView(childNode));
    auto localViewInfo = parent->GetLocalInfoInDomain(childNode);
    EXPECT_EQ(localViewInfo.localViewInitTime, child->GetResourceView().Get()->viewinittime());
    EXPECT_EQ(localViewInfo.localRevisionInDomain, uint64_t{0});
}

TEST_F(ResourceViewTest, AddNonEmptyUnitWithUrl)
{
    auto parent = resource_view::ResourceView::CreateResourceView(DOMAIN_RESOUCE_VIEW_ID, PARENT_PARAM);
    std::string childNode = LOCAL_RESOUCE_VIEW_ID;
    auto child = resource_view::ResourceView::CreateResourceView(childNode, CHILD_PARAM);
    child->ToReady();

    // adding 3 agent units to local ivew, with the revision of local view being 3
    auto unit1 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit1);
    auto [unitProportion, unitMem] = GetMinimumUnitBucketIndex(unit1.allocatable());
    ASSERT_TRUE(child->AddResourceUnit(unit1).Get().IsOk());
    auto unit2 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit2);
    ASSERT_TRUE(child->AddResourceUnit(unit2).Get().IsOk());
    auto unit3 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit3);
    ASSERT_TRUE(child->AddResourceUnit(unit3).Get().IsOk());

    // modify resourceunit1 -- add instance1(shared)
    // monopoly num : 1 --> 0
    // the revision of local view being 4
    auto unit1Id = unit1.id();
    auto inst1 = Get1DInstance();
    (*inst1.mutable_schedulerchain()->Add()) = unit1Id;
    inst1.set_unitid(unit1Id);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances1;
    instances1.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    ASSERT_TRUE(child->AddInstances(instances1).Get().IsOk());

    // modify resourceunit2 -- add instance1(shared) and delete instance1(shared)
    // monopoly num : 1 --> 0 --> 1
    // the revision of local view being 6
    auto unit2Id = unit2.id();
    auto inst2 = Get1DInstance();
    (*inst2.mutable_schedulerchain()->Add()) = unit2Id;
    inst2.set_unitid(unit2Id);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
    instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    ASSERT_TRUE(child->AddInstances(instances2).Get().IsOk());

    std::vector<std::string> ids2;
    ids2.push_back(inst2.instanceid());
    ASSERT_TRUE(child->DeleteInstances(ids2).Get().IsOk());

    // regist local view to domain view
    auto add = parent->AddResourceUnitWithUrl(*child->GetFullResourceView().Get(),
                                              litebus::GetActor(childNode +"-ResourceViewActor")->GetAID().Url());
    ASSERT_TRUE(add.Get().IsOk());

    ASSERT_TRUE(parent->CheckLocalExistInDomainView(childNode));
    auto localViewInfo = parent->GetLocalInfoInDomain(childNode);
    EXPECT_EQ(localViewInfo.localRevisionInDomain, uint64_t{6});
    EXPECT_EQ(localViewInfo.agentIDs.size(), size_t{3});

    auto rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 3);
    EXPECT_TRUE(rView.Get()->capacity() == (unit1.capacity()+unit2.capacity()+unit3.capacity()));
    auto &bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 2);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit2.id()).monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit3.id()).monopolynum(), 1);

    auto runit1 = rView.Get()->fragment().at(unit1.id());
    bucket = runit1.mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit1.id()).monopolynum(), 0);

    auto runit2 = rView.Get()->fragment().at(unit2.id());
    bucket = runit2.mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit2.id()).monopolynum(), 1);

    auto runit3 = rView.Get()->fragment().at(unit3.id());
    bucket = runit3.mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(unit3.id()).monopolynum(), 1);
}

TEST_F(ResourceViewTest, CheckLatestReportUpdateInDomain)
{
    auto parent = resource_view::ResourceView::CreateResourceView(DOMAIN_RESOUCE_VIEW_ID, PARENT_PARAM);
    std::string childNode = LOCAL_RESOUCE_VIEW_ID;
    auto child = resource_view::ResourceView::CreateResourceView(childNode, CHILD_PARAM);
    child->ToReady();

    auto add = parent->AddResourceUnitWithUrl(*child->GetFullResourceView().Get(),
                                              litebus::GetActor(childNode +"-ResourceViewActor")->GetAID().Url());
    ASSERT_TRUE(add.Get().IsOk());

    ASSERT_TRUE(parent->CheckLocalExistInDomainView(childNode));
    auto localViewInfo = parent->GetLocalInfoInDomain(childNode);
    auto localRevisionInDomain = localViewInfo.localRevisionInDomain;
    auto localViewInitTimeOrigin = child->GetResourceView().Get()->viewinittime();
    EXPECT_EQ(localViewInfo.localViewInitTime, child->GetResourceView().Get()->viewinittime());
    EXPECT_EQ(localRevisionInDomain, uint64_t{0});

    // revision of local info in domain is 3
    std::shared_ptr<ResourceUnitChanges> resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(childNode);
    resourceUnitChanges->set_localviewinittime(localViewInitTimeOrigin);
    resourceUnitChanges->set_startrevision(0);
    resourceUnitChanges->set_endrevision(3);
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());

    // 1.
    // localInfoInDomain:     revision      : 3       ; localViewInitTime = localViewInitTimeOrigin
    // no pending update reqeust in domain
    // reportedUpdateReqInfo: RevisionRange : [0, 10] ; localViewInitTime = localViewInitTimeOrigin
    // --> discarding the new reported request
    resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(childNode);
    resourceUnitChanges->set_localviewinittime(localViewInitTimeOrigin);
    resourceUnitChanges->set_startrevision(0);
    resourceUnitChanges->set_endrevision(10);
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_FALSE(add.Get().IsOk());

    std::shared_ptr<ResourceUnitChanges> latestReportChanges = parent->GetLatestReportChanges(LOCAL_RESOUCE_VIEW_ID);
    EXPECT_EQ(latestReportChanges, nullptr);

    // add a pending update request, the revision range is: [3, 10]
    resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(childNode);
    resourceUnitChanges->set_localviewinittime(localViewInitTimeOrigin);
    resourceUnitChanges->set_startrevision(3);
    resourceUnitChanges->set_endrevision(10);
    parent->SetLatestReportChanges(childNode, *resourceUnitChanges);

    // 2.
    // localInfoInDomain:     revision      : 3       ; localViewInitTime = localViewInitTimeOrigin
    // pendingUpdateReqInfo:  RevisionRange : [3, 10] ; localViewInitTime = localViewInitTimeOrigin
    // reportedUpdateReqInfo: RevisionRange : [3, 15] ; localViewInitTime = localViewInitTimeOrigin
    // --> keeping the new reported request
    resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(childNode);
    resourceUnitChanges->set_localviewinittime(localViewInitTimeOrigin);
    resourceUnitChanges->set_startrevision(3);
    resourceUnitChanges->set_endrevision(15);
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());

    latestReportChanges = parent->GetLatestReportChanges(LOCAL_RESOUCE_VIEW_ID);
    EXPECT_EQ(latestReportChanges->startrevision(), uint64_t{3});
    EXPECT_EQ(latestReportChanges->endrevision(), uint64_t{15});

    // 3.
    // localInfoInDomain:     revision      : 3       ; localViewInitTime = localViewInitTimeOrigin
    // pendingUpdateReqInfo:  RevisionRange : [3, 15] ; localViewInitTime = localViewInitTimeOrigin
    // reportedUpdateReqInfo: RevisionRange : [0, 5]  ; localViewInitTime = localViewInitTimeNew1
    // --> keeping the new reported request
    resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(childNode);
    resourceUnitChanges->set_startrevision(0);
    resourceUnitChanges->set_endrevision(5);
    std::string localViewInitTimeNew1 = "localViewInitTimeNew1";
    resourceUnitChanges->set_localviewinittime(localViewInitTimeNew1);
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());

    latestReportChanges = parent->GetLatestReportChanges(LOCAL_RESOUCE_VIEW_ID);
    EXPECT_EQ(latestReportChanges->startrevision(), uint64_t{0});
    EXPECT_EQ(latestReportChanges->endrevision(), uint64_t{5});
    EXPECT_EQ(latestReportChanges->localviewinittime(), localViewInitTimeNew1);

    // 4.
    // localInfoInDomain:     revision      : 3       ; localViewInitTime = localViewInitTimeOrigin
    // pendingUpdateReqInfo:  RevisionRange : [0, 5]  ; localViewInitTime = localViewInitTimeNew1
    // reportedUpdateReqInfo: RevisionRange : [0, 7]  ; localViewInitTime = localViewInitTimeNew2
    // --> keeping the new reported request
    resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(childNode);
    resourceUnitChanges->set_startrevision(0);
    resourceUnitChanges->set_endrevision(7);
    std::string localViewInitTimeNew2 = "localViewInitTimeNew2";
    resourceUnitChanges->set_localviewinittime(localViewInitTimeNew2);
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());

    latestReportChanges = parent->GetLatestReportChanges(LOCAL_RESOUCE_VIEW_ID);
    EXPECT_EQ(latestReportChanges->startrevision(), uint64_t{0});
    EXPECT_EQ(latestReportChanges->endrevision(), uint64_t{7});
    EXPECT_EQ(latestReportChanges->localviewinittime(), localViewInitTimeNew2);

    // 5.
    // localInfoInDomain:     revision      : 3       ; localViewInitTime = localViewInitTimeOrigin
    // pendingUpdateReqInfo:  RevisionRange : [0, 7]  ; localViewInitTime = localViewInitTimeNew2
    // reportedUpdateReqInfo: RevisionRange : [3, 20] ; localViewInitTime = localViewInitTimeOrigin
    // --> discarding the new reported request
    resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(childNode);
    resourceUnitChanges->set_startrevision(3);
    resourceUnitChanges->set_endrevision(20);
    resourceUnitChanges->set_localviewinittime(localViewInitTimeOrigin);
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_FALSE(add.Get().IsOk());

    latestReportChanges = parent->GetLatestReportChanges(LOCAL_RESOUCE_VIEW_ID);
    EXPECT_EQ(latestReportChanges->startrevision(), uint64_t{0});
    EXPECT_EQ(latestReportChanges->endrevision(), uint64_t{7});
    EXPECT_EQ(latestReportChanges->localviewinittime(), localViewInitTimeNew2);

    // 6.
    // localInfoInDomain:     revision      : 3       ; localViewInitTime = localViewInitTimeOrigin
    // pendingUpdateReqInfo:  RevisionRange : [0, 7]  ; localViewInitTime = localViewInitTimeNew2
    // reportedUpdateReqInfo: RevisionRange : [0, 20] ; localViewInitTime = localViewInitTimeNew2
    // --> keeping the new reported request
    resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(childNode);
    resourceUnitChanges->set_startrevision(0);
    resourceUnitChanges->set_endrevision(20);
    resourceUnitChanges->set_localviewinittime(localViewInitTimeNew2);
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());

    latestReportChanges = parent->GetLatestReportChanges(LOCAL_RESOUCE_VIEW_ID);
    EXPECT_EQ(latestReportChanges->startrevision(), uint64_t{0});
    EXPECT_EQ(latestReportChanges->endrevision(), uint64_t{20});
    EXPECT_EQ(latestReportChanges->localviewinittime(), localViewInitTimeNew2);

    // 7.
    // localInfoInDomain:     revision      : 3       ; localViewInitTime = localViewInitTimeOrigin
    // pendingUpdateReqInfo:  RevisionRange : [0, 20] ; localViewInitTime = localViewInitTimeNew2
    // reportedUpdateReqInfo: RevisionRange : [5, 15] ; localViewInitTime = localViewInitTimeNew3
    // --> discarding the new reported request
    resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(childNode);
    resourceUnitChanges->set_startrevision(5);
    resourceUnitChanges->set_endrevision(15);
    std::string localViewInitTimeNew3 = "localViewInitTimeNew3";
    resourceUnitChanges->set_localviewinittime(localViewInitTimeNew3);
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_FALSE(add.Get().IsOk());

    latestReportChanges = parent->GetLatestReportChanges(LOCAL_RESOUCE_VIEW_ID);
    EXPECT_EQ(latestReportChanges->startrevision(), uint64_t{0});
    EXPECT_EQ(latestReportChanges->endrevision(), uint64_t{20});
    EXPECT_EQ(latestReportChanges->localviewinittime(), localViewInitTimeNew2);
}

TEST_F(ResourceViewTest, PullMultiResourceUnitChanges)
{
    ResourcePoller::SetInterval(1000);
    auto parent = resource_view::ResourceView::CreateResourceView("parent", PARENT_PARAM);
    parent->TriggerTryPull();
    std::string childNode = LOCAL_RESOUCE_VIEW_ID;
    auto child = resource_view::ResourceView::CreateResourceView(childNode, CHILD_PARAM);
    child->ToReady();
    child->UpdateDomainUrlForLocal(domainUrl_);
    auto rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);

    auto add = parent->AddResourceUnitWithUrl(*child->GetFullResourceView().Get(),
                                              litebus::GetActor(childNode +"-ResourceViewActor")->GetAID().Url());
    ASSERT_TRUE(add.Get().IsOk());

    // 1.add resourceunit1
    auto unit1 = Get1DResourceUnit();
    auto agentId1 = unit1.id();
    GenerateMinimumUnitBucketInfo(unit1);
    auto [unitProportion, unitMem] = GetMinimumUnitBucketIndex(unit1.allocatable());
    auto begin = litebus::TimeWatch::Now();
    ASSERT_TRUE(child->AddResourceUnit(unit1).Get().IsOk());

    // 2.add resourceunit2
    auto unit2 = Get1DResourceUnit();
    auto agentId2 = unit2.id();
    GenerateMinimumUnitBucketInfo(unit2);
    ASSERT_TRUE(child->AddResourceUnit(unit2).Get().IsOk());

    // 3.modify resourceunit1 -- add instance1(shared)
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    auto inst1 = Get1DInstance();
    (*inst1.mutable_schedulerchain()->Add()) = agentId1;
    inst1.set_unitid(agentId1);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances1;
    instances1.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    ASSERT_TRUE(child->AddInstances(instances1).Get().IsOk());

    // 4.modify resourceunit -- add instance2(shared)
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    auto inst2 = Get1DInstance();
    (*inst2.mutable_schedulerchain()->Add()) = agentId1;
    inst2.set_unitid(agentId1);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
    instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    ASSERT_TRUE(child->AddInstances(instances2).Get().IsOk());

    // 5.del instance1(shared)
    std::vector<std::string> ids1;
    ids1.push_back(inst1.instanceid());
    ASSERT_TRUE(child->DeleteInstances(ids1).Get().IsOk());

    // 6.modify resourceunit -- add instance3(shared)
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    auto inst3 = Get1DInstance();
    (*inst3.mutable_schedulerchain()->Add()) = agentId1;
    inst3.set_unitid(agentId1);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances3;
    instances3.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, nullptr });
    ASSERT_TRUE(child->AddInstances(instances3).Get().IsOk());

    // 7.modify resourceunit2 -- add instance4(monopoly)
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    auto inst4 = Get1DInstance();
    (*inst4.mutable_schedulerchain()->Add()) = agentId2;
    inst4.set_unitid(agentId2);
    inst3.mutable_scheduleoption()->set_schedpolicyname(MONOPOLY_SCHEDULE);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances4;
    instances4.emplace(inst4.instanceid(), resource_view::InstanceAllocatedInfo{ inst4, nullptr });
    ASSERT_TRUE(child->AddInstances(instances4).Get().IsOk());

    // 8.add resourceunit3
    auto unit3 = Get1DResourceUnit();
    auto agentId3 = unit3.id();
    GenerateMinimumUnitBucketInfo(unit3);
    ASSERT_TRUE(child->AddResourceUnit(unit3).Get().IsOk());

    // result in domain
    // wait until the domain has completed the resourceview update
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 2000; });
    rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 3);
    EXPECT_TRUE(rView.Get()->capacity() == (unit1.capacity()+unit2.capacity()+unit3.capacity()));
    auto rUnitCapacity = rView.Get()->fragment().at(agentId1).capacity();
    EXPECT_TRUE(rUnitCapacity == unit1.capacity());
    rUnitCapacity = rView.Get()->fragment().at(agentId2).capacity();
    EXPECT_TRUE(rUnitCapacity == unit2.capacity());
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localRevisionInDomain, uint64_t{8});
    auto &bucket = rView.Get()->mutable_bucketindexs()->at(unitProportion).mutable_buckets()->at(unitMem);
    EXPECT_EQ(bucket.total().monopolynum(), 1);
    EXPECT_EQ(bucket.mutable_allocatable()->at(agentId1).monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(agentId2).monopolynum(), 0);
    EXPECT_EQ(bucket.mutable_allocatable()->at(agentId3).monopolynum(), 1);
}

void CheckRevionChanges(std::unique_ptr<ResourceView> &parent, std::unique_ptr<ResourceView> &child,
                        uint64_t currentRevisionInLocal)
{
    auto begin = litebus::TimeWatch::Now();
    // wait until the domain has completed the resourceview update
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 200; });
    ASSERT_EQ(parent->GetLocalInfoInDomain(LOCAL_RESOUCE_VIEW_ID).localRevisionInDomain, currentRevisionInLocal);

    auto changes = child->GetResourceViewChanges();
    ASSERT_EQ(changes.Get()->startrevision(), currentRevisionInLocal);
    ASSERT_EQ(changes.Get()->endrevision(), currentRevisionInLocal);
}

TEST_F(ResourceViewTest, DISABLED_UpdateLastReportedRevision)
{
    ResourcePoller::SetInterval(100);
    auto parent = resource_view::ResourceView::CreateResourceView(DOMAIN_RESOUCE_VIEW_ID, PARENT_PARAM);
    parent->TriggerTryPull();
    std::string childNode = LOCAL_RESOUCE_VIEW_ID;
    auto child = resource_view::ResourceView::CreateResourceView(childNode, CHILD_PARAM);
    child->ToReady();
    child->UpdateDomainUrlForLocal(domainUrl_);
    auto rView = parent->GetResourceView();
    ASSERT_AWAIT_READY(rView);
    ASSERT_EQ(rView.Get()->fragment_size(), 0);

    auto add = parent->AddResourceUnitWithUrl(*child->GetFullResourceView().Get(),
                                              litebus::GetActor(childNode +"-ResourceViewActor")->GetAID().Url());
    ASSERT_TRUE(add.Get().IsOk());

    // 1.add resourceunit1
    uint64_t currentRevisionInLocal = 1;
    auto unit1 = Get1DResourceUnit();
    auto agentId1 = unit1.id();
    GenerateMinimumUnitBucketInfo(unit1);
    ASSERT_TRUE(child->AddResourceUnit(unit1).Get().IsOk());
    CheckRevionChanges(parent, child, currentRevisionInLocal);

    // 2.modify resourceunit1 -- add instance1(shared)
    auto inst1 = Get1DInstance();
    (*inst1.mutable_schedulerchain()->Add()) = agentId1;
    inst1.set_unitid(agentId1);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances1;
    instances1.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    ASSERT_TRUE(child->AddInstances(instances1).Get().IsOk());
    currentRevisionInLocal += 1;
    CheckRevionChanges(parent, child, currentRevisionInLocal);

    // 3.modify resourceunit -- add instance2(shared)
    auto inst2 = Get1DInstance();
    (*inst2.mutable_schedulerchain()->Add()) = agentId1;
    inst2.set_unitid(agentId1);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
    instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    ASSERT_TRUE(child->AddInstances(instances2).Get().IsOk());
    currentRevisionInLocal += 1;
    CheckRevionChanges(parent, child, currentRevisionInLocal);

    // 4.modify resourceunit -- add instance3(shared) + update instance1 status
    // 4.1 add instance3(shared)
    auto inst3 = Get1DInstance();
    (*inst3.mutable_schedulerchain()->Add()) = agentId1;
    inst3.set_unitid(agentId1);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances3;
    instances3.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, nullptr });
    ASSERT_TRUE(child->AddInstances(instances3).Get().IsOk());
    // check lastReportedRevision -- LastReportedRevision = currentRevisionInLocal
    auto changes = child->GetResourceViewChanges();
    ASSERT_EQ(changes.Get()->startrevision(), currentRevisionInLocal);
    ASSERT_EQ(changes.Get()->endrevision(), currentRevisionInLocal + 1);
    currentRevisionInLocal += 1;

    // 4.2 update instance1 status
    ASSERT_TRUE(child->UpdateUnitStatus(agentId1, resource_view::UnitStatus::EVICTING).Get().IsOk());
    // check lastReportedRevision -- LastReportedRevision = currentRevisionInLocal + 1
    changes = child->GetResourceViewChanges();
    ASSERT_EQ(changes.Get()->startrevision(), currentRevisionInLocal);
    ASSERT_EQ(changes.Get()->endrevision(), currentRevisionInLocal + 1);
    currentRevisionInLocal += 1;

    // 5.empty changes -- delete fake instance
    std::vector<std::string> ids;
    ids.push_back("fake_instance");
    ASSERT_FALSE(child->DeleteInstances(ids).Get().IsOk());
    currentRevisionInLocal += 1;
    CheckRevionChanges(parent, child, currentRevisionInLocal);
}

void RecoverResourceView(std::unique_ptr<ResourceView> &parent, std::string localViewInitTimeOrigin,
                         ResourceUnit &unit)
{
    auto begin = litebus::TimeWatch::Now();
    auto resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(LOCAL_RESOUCE_VIEW_ID);
    resourceUnitChanges->set_localviewinittime(localViewInitTimeOrigin);
    resourceUnitChanges->set_startrevision(0);
    resourceUnitChanges->set_endrevision(1);

    Addition addition;
    (*addition.mutable_resourceunit()) = unit;
    ResourceUnitChange resourceUnitChange;
    resourceUnitChange.set_resourceunitid(unit.id());
    *resourceUnitChange.mutable_addition() = addition;

    *resourceUnitChanges->add_changes() = resourceUnitChange;
    auto add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 100; });
    ASSERT_EQ(parent->GetLocalInfoInDomain(LOCAL_RESOUCE_VIEW_ID).localViewInitTime, localViewInitTimeOrigin);
}

TEST_F(ResourceViewTest, DISABLED_PullResourceUnitChangesError)
{
    auto parent = resource_view::ResourceView::CreateResourceView(DOMAIN_RESOUCE_VIEW_ID, PARENT_PARAM);
    std::string childNode = LOCAL_RESOUCE_VIEW_ID;
    auto child = resource_view::ResourceView::CreateResourceView(childNode, CHILD_PARAM);
    auto add = parent->AddResourceUnitWithUrl(*child->GetFullResourceView().Get(),
                                              litebus::GetActor(childNode +"-ResourceViewActor")->GetAID().Url());
    ASSERT_TRUE(add.Get().IsOk());

    // revision of local info in domain is 0, localViewInitTime is localViewInitTimeOrigin
    ASSERT_TRUE(parent->CheckLocalExistInDomainView(childNode));
    auto localViewInfo = parent->GetLocalInfoInDomain(childNode);
    auto localRevisionInDomain = localViewInfo.localRevisionInDomain;
    auto localViewInitTimeOrigin = child->GetResourceView().Get()->viewinittime();
    EXPECT_EQ(localViewInfo.localViewInitTime, child->GetResourceView().Get()->viewinittime());
    EXPECT_EQ(localRevisionInDomain, uint64_t{0});

    auto unit = Get1DResourceUnit();
    unit.set_ownerid(childNode);

    // 1.add resourceunit ok
    RecoverResourceView(parent, localViewInitTimeOrigin, unit);

    // 2.add resourceunit fail -- ownerid of resourceunit is emtpty
    auto begin = litebus::TimeWatch::Now();
    auto resourceUnitChanges = std::make_shared<ResourceUnitChanges>();
    resourceUnitChanges->set_localid(childNode);
    resourceUnitChanges->set_localviewinittime(localViewInitTimeOrigin);
    resourceUnitChanges->set_startrevision(1);
    resourceUnitChanges->set_endrevision(2);

    Addition addition;
    (*addition.mutable_resourceunit()) = unit;
    // ownerid of resourceunit is emtpty
    addition.mutable_resourceunit()->clear_ownerid();
    ResourceUnitChange resourceUnitChange;
    resourceUnitChange.set_resourceunitid(unit.id());
    *resourceUnitChange.mutable_addition() = addition;
    *resourceUnitChanges->add_changes() = resourceUnitChange;

    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 100; });
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localViewInitTime, NEED_RECOVER_VIEW);

    // recover resourceview
    RecoverResourceView(parent, localViewInitTimeOrigin, unit);

    // 3.add resourceunit fail -- AddResourceUnit error(unitId is empty)
    begin = litebus::TimeWatch::Now();
    auto change = *(resourceUnitChanges->mutable_changes(0));
    change.mutable_addition()->mutable_resourceunit()->set_ownerid(childNode);
    // unitId is empty
    change.mutable_addition()->mutable_resourceunit()->clear_id();
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 100; });
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localViewInitTime, NEED_RECOVER_VIEW);

    // recover resourceview
    RecoverResourceView(parent, localViewInitTimeOrigin, unit);

    // 4.add resourceunit fail -- AddResourceUnit error
    begin = litebus::TimeWatch::Now();
    change = *(resourceUnitChanges->mutable_changes(0));
    change.mutable_addition()->mutable_resourceunit()->set_id(unit.id());
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 100; });
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localViewInitTime, NEED_RECOVER_VIEW);

    // recover resourceview
    RecoverResourceView(parent, localViewInitTimeOrigin, unit);

    // 4.delete resourceunit fail -- unitId is empty
    begin = litebus::TimeWatch::Now();
    Deletion deletion;
    // resourceUnitId of resourceUnitChange is empty
    resourceUnitChange = ResourceUnitChange{};
    *resourceUnitChange.mutable_deletion() = deletion;

    resourceUnitChanges->clear_changes();
    *resourceUnitChanges->add_changes() = resourceUnitChange;
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 100; });
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localViewInitTime, NEED_RECOVER_VIEW);

    // recover resourceview
    RecoverResourceView(parent, localViewInitTimeOrigin, unit);

    // 5.delete resourceunit fail -- agent not found
    begin = litebus::TimeWatch::Now();
    resourceUnitChange.set_resourceunitid("fake_agent_id");
    resourceUnitChanges->clear_changes();
    *resourceUnitChanges->add_changes() = resourceUnitChange;
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 100; });
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localViewInitTime, NEED_RECOVER_VIEW);

    // recover resourceview
    RecoverResourceView(parent, localViewInitTimeOrigin, unit);

    // 6.add instance fail  -- agent not found
    begin = litebus::TimeWatch::Now();
    auto inst1 = Get1DInstance();
    inst1.set_unitid("fake_agent_id");
    InstanceChange instanceChange;
    instanceChange.set_changetype(InstanceChange::ADD);
    instanceChange.set_instanceid(inst1.instanceid());
    instanceChange.mutable_instance()->CopyFrom(inst1);

    Modification modification;
    *modification.add_instancechanges() = instanceChange;

    resourceUnitChange = ResourceUnitChange{};
    resourceUnitChange.set_resourceunitid("fake_agent_id");
    *resourceUnitChange.mutable_modification() = modification;
    resourceUnitChanges->clear_changes();
    *resourceUnitChanges->add_changes() = resourceUnitChange;
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 100; });
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localViewInitTime, NEED_RECOVER_VIEW);

    // recover resourceview
    RecoverResourceView(parent, localViewInitTimeOrigin, unit);

    // 7.delete instance fail  -- instance not found
    begin = litebus::TimeWatch::Now();
    inst1.set_unitid(unit.id());
    instanceChange.set_changetype(InstanceChange::DELETE);
    instanceChange.mutable_instance()->CopyFrom(inst1);

    modification.clear_instancechanges();
    *modification.add_instancechanges() = instanceChange;

    resourceUnitChange = ResourceUnitChange{};
    resourceUnitChange.set_resourceunitid(unit.id());
    *resourceUnitChange.mutable_modification() = modification;
    resourceUnitChanges->clear_changes();
    *resourceUnitChanges->add_changes() = resourceUnitChange;
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 100; });
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localViewInitTime, NEED_RECOVER_VIEW);

    // recover resourceview
    RecoverResourceView(parent, localViewInitTimeOrigin, unit);

    // 7.add instance fail  -- unitid is empty
    begin = litebus::TimeWatch::Now();
    instanceChange.set_changetype(InstanceChange::ADD);
    inst1.clear_unitid();
    instanceChange.mutable_instance()->CopyFrom(inst1);

    modification.clear_instancechanges();
    *modification.add_instancechanges() = instanceChange;

    resourceUnitChange = ResourceUnitChange{};
    resourceUnitChange.set_resourceunitid(unit.id());
    *resourceUnitChange.mutable_modification() = modification;
    resourceUnitChanges->clear_changes();
    *resourceUnitChanges->add_changes() = resourceUnitChange;
    add = parent->UpdateResourceUnitDelta(resourceUnitChanges);
    ASSERT_TRUE(add.Get().IsOk());
    ASSERT_AWAIT_TRUE([&]() -> bool { return (litebus::TimeWatch::Now() - begin) > 100; });
    ASSERT_EQ(parent->GetLocalInfoInDomain(childNode).localViewInitTime, NEED_RECOVER_VIEW);
}

std::vector<ValueCounter> GenerateIdleToRecycleLabel()
{
    resources::Value::Counter unitCount1;
    (*unitCount1.mutable_items())["unlimited"] = 1;

    resources::Value::Counter unitCount2;
    (*unitCount2.mutable_items())["0"] = 1;

    resources::Value::Counter unitCount3;
    (*unitCount3.mutable_items())["1"] = 1;

    resources::Value::Counter unitCount4;
    (*unitCount4.mutable_items())["-2"] = 1;

    resources::Value::Counter unitCount5;
    (*unitCount5.mutable_items())["XXX"] = 1;

    resources::Value::Counter unitCount6;
    (*unitCount6.mutable_items())["XXX"] = 1;
    (*unitCount6.mutable_items())["000"] = 1;

    return std::vector<ValueCounter>{unitCount1, unitCount2, unitCount3, unitCount4, unitCount5, unitCount6};
}

TEST_F(ResourceViewTest, idleToRecyclePodLabelParse)
{
    std::string aid = LOCAL_RESOUCE_VIEW_ID + "-ResourceViewActor";
    auto implActor = std::make_shared<ResourceViewActor>(aid, LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto idleToRecycleLabels = GenerateIdleToRecycleLabel();
    static const std::string IDLE_TO_RECYCLE = "yr-idle-to-recycle";
    auto unit1 = Get1DResourceUnit();
    EXPECT_EQ(implActor->TestParseRecyclePodLabel(unit1), 0);
    (*unit1.mutable_nodelabels())[IDLE_TO_RECYCLE] = idleToRecycleLabels[0];
    EXPECT_EQ(implActor->TestParseRecyclePodLabel(unit1), -1);
    (*unit1.mutable_nodelabels())[IDLE_TO_RECYCLE] = idleToRecycleLabels[1];
    EXPECT_EQ(implActor->TestParseRecyclePodLabel(unit1), 0);
    (*unit1.mutable_nodelabels())[IDLE_TO_RECYCLE] = idleToRecycleLabels[2];
    EXPECT_EQ(implActor->TestParseRecyclePodLabel(unit1), 1);
    (*unit1.mutable_nodelabels())[IDLE_TO_RECYCLE] = idleToRecycleLabels[3];
    EXPECT_EQ(implActor->TestParseRecyclePodLabel(unit1), 0);
    (*unit1.mutable_nodelabels())[IDLE_TO_RECYCLE] = idleToRecycleLabels[4];
    EXPECT_EQ(implActor->TestParseRecyclePodLabel(unit1), 0);
    (*unit1.mutable_nodelabels())[IDLE_TO_RECYCLE] = idleToRecycleLabels[5];
    EXPECT_EQ(implActor->TestParseRecyclePodLabel(unit1), 0);
}

/*
 * test for idleToRecyclePodCreate while add agent
 * // pod status is normal
 * 1. pod has idleToRecyclePod, value is -1 -> timer not set
 * 2. pod has idleToRecyclePod, value is 0 -> timer not set
 * 3. pod has idleToRecyclePod, value is > 0 -> timer is set
 * 4. pod does not have idleToRecyclePod and not enable tenant affinity -> timer not set
 * 5. idleToRecyclePod value is invalid-> timer not set
 * 6. pod does not have idleToRecyclePod and enable tenant affinity but not instance in it-> timer not set
 */
TEST_F(ResourceViewTest, idleToRecyclePodCreate)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;
    auto view = resourceView.GetResourceView();
    static const std::string IDLE_TO_RECYCLE = "yr-idle-to-recycle";
    auto idleToRecycleLabels = GenerateIdleToRecycleLabel();
    std::set<std::string> disabledAgent;
    std::atomic<int> count = 0;
    viewPtr->RegisterUnitDisableFunc([&disabledAgent, &count](const std::string &agentID){
        disabledAgent.emplace(agentID);
        count++;
    });

    {
        // 1. pod has idleToRecyclePod, value is -1 -> timer not set
        auto unit1 = Get1DResourceUnit();
        unit1.mutable_nodelabels()->insert({IDLE_TO_RECYCLE, idleToRecycleLabels[0]});
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit1));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{0});
    }
    {
        // 2. pod has idleToRecyclePod, value is 0 -> timer not set
        auto unit2 = Get1DResourceUnit();
        unit2.mutable_nodelabels()->insert({IDLE_TO_RECYCLE, idleToRecycleLabels[1]});
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit2));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit2.id()), size_t{0});
    }
    {
        // 3. pod has idleToRecyclePod, value is > 0 -> timer is set
        auto unit3 = Get1DResourceUnit();
        unit3.mutable_nodelabels()->insert({IDLE_TO_RECYCLE, idleToRecycleLabels[2]});
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit3));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{1});
        EXPECT_AWAIT_TRUE([&] () -> bool { return resourceView.GetReuseTimers().count(unit3.id()) == 0; });
        EXPECT_EQ(count,1);
        EXPECT_EQ(disabledAgent.count(unit3.id()), size_t{1});
    }
    {
        // 4. pod does not have idleToRecyclePod and not enable tenant affinity -> timer not set
        auto unit3 = Get1DResourceUnit();
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit3));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{0});
    }
    {
        // 5. idleToRecyclePod value is invalid-> timer not set
        auto unit3 = Get1DResourceUnit();
        unit3.mutable_nodelabels()->insert({IDLE_TO_RECYCLE, idleToRecycleLabels[3]});
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit3));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{0});
        auto unit4 = Get1DResourceUnit();
        unit4.mutable_nodelabels()->insert({IDLE_TO_RECYCLE, idleToRecycleLabels[4]});
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit4));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit4.id()), size_t{0});
    }
    {
        // 6. pod does not have idleToRecyclePod and enable tenant affinity but not instance in it-> timer not set
        auto unit3 = Get1DResourceUnit();
        viewPtr->SetEnableTenantAffinity(true);
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit3));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{0});
    }
}

/*
 * test for idleToRecyclePodCreate while add agent
 * 1. pod has idleToRecyclePod, value is > 0, but pod is Recovering -> timer not set
 * 1.1 pod is idleToRecyclePod, pod is Recovering and change to running -> timer is set according idleToRecyclePod
 * 2. pod not idleToRecyclePod,  enable tenant affinity, but pod is Recovering -> timer not set
 * 2.1 pod not idleToRecyclePod, enable tenant affinity, pod is Recovering and change to running: have instance in it -> timer is set else not set
 */
TEST_F(ResourceViewTest, idleToRecyclePodRecovering)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;
    auto view = resourceView.GetResourceView();
    static const std::string IDLE_TO_RECYCLE = "yr-idle-to-recycle";
    auto idleToRecycleLabels = GenerateIdleToRecycleLabel();

    std::set<std::string> disabledAgent;
    std::atomic<int> count = 0;
    viewPtr->RegisterUnitDisableFunc([&disabledAgent, &count](const std::string &agentID){
        disabledAgent.emplace(agentID);
        count++;
    });

    {
        // 1. pod has idleToRecyclePod, value is > 0, but pod is Recovering -> timer not set
        auto unit1 = Get1DResourceUnit();
        unit1.set_status(static_cast<uint32_t>(UnitStatus::RECOVERING));
        unit1.mutable_nodelabels()->insert({IDLE_TO_RECYCLE, idleToRecycleLabels[0]});
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit1));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{0});

        // pod is idleToRecyclePod, pod is Recovering and change to running -> timer is set according idleToRecyclePod
        ASSERT_AWAIT_READY(resourceView.UpdateUnitStatus(unit1.id(), UnitStatus::NORMAL));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{0});
    }
    {
        // 1. pod has idleToRecyclePod, value is > 0, but pod is Recovering -> timer not set
        auto unit1 = Get1DResourceUnit();
        unit1.set_status(static_cast<uint32_t>(UnitStatus::RECOVERING));
        unit1.mutable_nodelabels()->insert({IDLE_TO_RECYCLE, idleToRecycleLabels[2]});
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit1));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{0});

        // pod is idleToRecyclePod, pod is Recovering and change to running -> timer is set according idleToRecyclePod
        ASSERT_AWAIT_READY(resourceView.UpdateUnitStatus(unit1.id(), UnitStatus::NORMAL));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{1});
        EXPECT_AWAIT_TRUE([&] () -> bool { return resourceView.GetReuseTimers().count(unit1.id()) == 0; });
        EXPECT_EQ(count,1);
        EXPECT_EQ(disabledAgent.count(unit1.id()), size_t{1});
    }
    {
        // pod not idleToRecyclePod,  enable tenant affinity, but pod is Recovering -> timer not set
        auto unit3 = Get1DResourceUnit();
        unit3.set_status(static_cast<uint32_t>(UnitStatus::RECOVERING));
        viewPtr->SetEnableTenantAffinity(true);
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit3));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{0});

        // pod not idleToRecyclePod, enable tenant affinity, pod is Recovering and change to running: don't have instance in it -> timer not set
        ASSERT_AWAIT_READY(resourceView.UpdateUnitStatus(unit3.id(), UnitStatus::NORMAL));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{0});
    }
    {
        // pod not idleToRecyclePod,  enable tenant affinity, but pod is Recovering -> timer not set
        auto unit3 = Get1DResourceUnit();
        unit3.set_status(static_cast<uint32_t>(UnitStatus::RECOVERING));
        auto inst3 = Get1DInstance();
        inst3.set_functionproxyid(LOCAL_RESOUCE_VIEW_ID);
        viewPtr->SetEnableTenantAffinity(true);
        inst3.set_unitid(unit3.id());
        std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
        instances.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, nullptr });
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit3));
        ASSERT_AWAIT_READY(resourceView.AddInstances(instances));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{0});

        // pod not idleToRecyclePod, enable tenant affinity, pod is Recovering and change to running: have instance in it -> timer is set
        ASSERT_AWAIT_READY(resourceView.UpdateUnitStatus(unit3.id(), UnitStatus::NORMAL));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{1});
        EXPECT_EQ(resourceView.GetAgentUsedMap().count(unit3.id()), size_t{1});
        EXPECT_AWAIT_TRUE([&] () -> bool { return resourceView.GetReuseTimers().count(unit3.id()) == 0; });
        EXPECT_EQ(resourceView.GetAgentUsedMap().count(unit3.id()), size_t{0});
        EXPECT_EQ(count,2);
        EXPECT_EQ(disabledAgent.count(unit3.id()), size_t{1});
    }
}

/*
 * test for idleToRecyclePodCreate while add instance
 * pod has been set timer and in recycle, if not addInstance, pod will be recycled;
 * else agentCacheMap_ have instance
 */
TEST_F(ResourceViewTest, idleToRecyclePodAddInstance)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;
    auto view = resourceView.GetResourceView();
    static const std::string IDLE_TO_RECYCLE = "yr-idle-to-recycle";
    auto idleToRecycleLabels = GenerateIdleToRecycleLabel();
    std::set<std::string> disabledAgent;
    std::atomic<int> count = 0;
    viewPtr->RegisterUnitDisableFunc([&disabledAgent, &count](const std::string &agentID){
        disabledAgent.emplace(agentID);
        count++;
    });

    {
        // pod has been set timer and in recycle, if not addInstance, pod will be recycled;
        auto unit3 = Get1DResourceUnit();
        unit3.mutable_nodelabels()->insert({IDLE_TO_RECYCLE, idleToRecycleLabels[2]});
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit3));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{1});
        // wait for timer is clear
        EXPECT_AWAIT_TRUE([&] () -> bool { return resourceView.GetReuseTimers().count(unit3.id()) == 0; });
        EXPECT_EQ(count,1);
        EXPECT_EQ(disabledAgent.count(unit3.id()), size_t{1});
    }
    {
        // pod has been set timer and in recycle, addInstance, pod will not be recycled;
        auto unit3 = Get1DResourceUnit();
        unit3.mutable_nodelabels()->insert({IDLE_TO_RECYCLE, idleToRecycleLabels[2]});
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit3));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{1});

        auto inst3 = Get1DInstance();
        inst3.set_unitid(unit3.id());
        std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
        instances.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, nullptr });
        auto ret = resourceView.AddInstances(instances);
        ASSERT_AWAIT_READY(ret);
        EXPECT_TRUE(ret.Get().IsOk());

        auto cache = resourceView.GetAgentCacheMap();
        EXPECT_TRUE(cache.find(unit3.id()) != cache.end());
        EXPECT_TRUE(cache.find(unit3.id())->second.find(inst3.instanceid()) != cache.find(unit3.id())->second.end());
    }
}

/*
 * test for idleToRecyclePodCreate while delete instance
 * 1.pod have instance, delete instance, and still have instance, not delete
 * 2.pod have instance, delete instance, and do not have instance, recycle instance
 */
TEST_F(ResourceViewTest, idleToRecyclePodDeleteInstance)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;
    auto view = resourceView.GetResourceView();
    static const std::string IDLE_TO_RECYCLE = "yr-idle-to-recycle";
    auto idleToRecycleLabels = GenerateIdleToRecycleLabel();
    std::set<std::string> disabledAgent;
    std::atomic<int> count = 0;
    viewPtr->RegisterUnitDisableFunc([&disabledAgent, &count](const std::string &agentID) {
        disabledAgent.emplace(agentID);
        count++;
    });

    // pod has been set timer and in recycle, addInstance, pod will not be recycled;
    auto unit3 = Get1DResourceUnit();
    unit3.mutable_nodelabels()->insert({ IDLE_TO_RECYCLE, idleToRecycleLabels[2] });
    ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit3));
    EXPECT_EQ(resourceView.GetReuseTimers().count(unit3.id()), size_t{1});

    auto inst3 = Get1DInstance();
    auto inst2 = Get1DInstance();
    auto inst1 = Get1DInstance();
    inst1.set_unitid(unit3.id());
    inst2.set_unitid(unit3.id());
    inst3.set_unitid(unit3.id());

    std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
    instances.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    instances.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    instances.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, nullptr });

    auto ret = resourceView.AddInstances(instances);
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().IsOk());

    // pod have instance, delete instance, and still have instance, not delete
    std::vector<std::string> ids1;
    ids1.push_back(inst1.instanceid());
    ret = resourceView.DeleteInstances(ids1);
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().IsOk());
    auto cache = resourceView.GetAgentCacheMap();
    EXPECT_TRUE(cache.find(unit3.id()) != cache.end());
    EXPECT_TRUE(cache.find(unit3.id())->second.find(inst1.instanceid()) == cache.find(unit3.id())->second.end());
    EXPECT_EQ(count, 0);
    EXPECT_EQ(disabledAgent.count(unit3.id()), size_t{0});

    // pod have instance, delete instance, and still have instance, not delete
    std::vector<std::string> ids2;
    ids2.push_back(inst2.instanceid());
    ret = resourceView.DeleteInstances(ids2);
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().IsOk());
    cache = resourceView.GetAgentCacheMap();
    EXPECT_TRUE(cache.find(unit3.id()) != cache.end());
    EXPECT_TRUE(cache.find(unit3.id())->second.find(inst2.instanceid()) == cache.find(unit3.id())->second.end());
    EXPECT_EQ(count, 0);
    EXPECT_EQ(disabledAgent.count(unit3.id()), size_t{0});

    // pod delete instance, and do not have instance, recycle instance
    std::vector<std::string> ids3;
    ids3.push_back(inst3.instanceid());
    ret = resourceView.DeleteInstances(ids3);
    ASSERT_AWAIT_READY(ret);
    EXPECT_TRUE(ret.Get().IsOk());
    EXPECT_AWAIT_TRUE([&]() -> bool { return resourceView.GetReuseTimers().count(unit3.id()) == 0; });
    cache = resourceView.GetAgentCacheMap();
    EXPECT_TRUE(cache.find(unit3.id()) == cache.end());
    EXPECT_EQ(count, 1);
    EXPECT_EQ(disabledAgent.count(unit3.id()), size_t{1});
}

/*
 * test for tenantAffinityPod while delete instance
 * 1.pod not set idlePod, enable tenantAffinity and delete actually instance -> set timer and recycle
 * 2.pod not set idlePod, enable tenantAffinity and delete actually instance, set timer and
 *   then add instance and then delete virtual instance -> set timer and recycle
 * 3.pod not set idlePod, enable tenantAffinity and add unit and then delete virtual instance -> not set timer and not recycle
 * 4.pod not set idlePod, enable tenantAffinity and add unit and then delete instance and add virtual instance,
 *   timer is executed, and then virtual instance is deleted  -> pod need to be recycled
 */
TEST_F(ResourceViewTest, tenantAffinityRecyclePodDeleteInstance)
{
    auto viewPtr = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    auto &resourceView = *viewPtr;
    auto view = resourceView.GetResourceView();
    static const std::string IDLE_TO_RECYCLE = "yr-idle-to-recycle";
    auto idleToRecycleLabels = GenerateIdleToRecycleLabel();
    viewPtr->SetEnableTenantAffinity(true);
    std::set<std::string> disabledAgent;
    std::atomic<int> count = 0;
    viewPtr->RegisterUnitDisableFunc([&disabledAgent, &count](const std::string &agentID) {
        disabledAgent.emplace(agentID);
        count++;
    });

    {
        // 1.pod not set idlePod, enable tenantAffinity and delete actually instance -> set timer and recycle
        auto unit1 = Get1DResourceUnit();
        unit1.mutable_nodelabels()->insert({ IDLE_TO_RECYCLE, idleToRecycleLabels[1] });
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit1));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{0});

        auto inst3 = Get1DInstance();
        auto inst2 = Get1DInstance();
        auto inst1 = Get1DInstance();
        inst1.set_unitid(unit1.id());
        inst2.set_unitid(unit1.id());
        inst3.set_unitid(unit1.id());
        inst1.set_tenantid("123456");
        inst2.set_tenantid("123456");
        inst3.set_tenantid("123456");

        std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
        instances.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
        instances.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
        instances.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, nullptr });

        auto ret = resourceView.AddInstances(instances);
        ASSERT_AWAIT_READY(ret);
        EXPECT_TRUE(ret.Get().IsOk());

        std::vector<std::string> ids1{ inst1.instanceid() };
        ret = resourceView.DeleteInstances(ids1);
        ASSERT_AWAIT_READY(ret);
        EXPECT_TRUE(ret.Get().IsOk());
        auto cache = resourceView.GetAgentCacheMap();
        EXPECT_TRUE(cache.find(unit1.id()) != cache.end());
        EXPECT_TRUE(cache.find(unit1.id())->second.find(inst1.instanceid()) == cache.find(unit1.id())->second.end());
        EXPECT_EQ(count, 0);
        EXPECT_EQ(disabledAgent.count(unit1.id()), size_t{0});

        std::vector<std::string> ids3;
        ids3.push_back(inst3.instanceid());
        ids3.push_back(inst2.instanceid());
        ret = resourceView.DeleteInstances(ids3);
        ASSERT_AWAIT_READY(ret);
        EXPECT_TRUE(ret.Get().IsOk());
        EXPECT_AWAIT_TRUE([&]() -> bool { return resourceView.GetReuseTimers().count(unit1.id()) == 0; });

        cache = resourceView.GetAgentCacheMap();
        EXPECT_TRUE(cache.find(unit1.id()) == cache.end());
        EXPECT_EQ(count, 1);
        EXPECT_EQ(disabledAgent.count(unit1.id()), size_t{1});
    }

    {
        // pod not set idlePod, enable tenantAffinity and delete actually instance,
        // set timer and then add instance and then delete virtual instance -> set timer and recycle
        auto unit1 = Get1DResourceUnit();
        unit1.mutable_nodelabels()->insert({ IDLE_TO_RECYCLE, idleToRecycleLabels[1] });
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit1));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{0});
        count = 0;  // reset

        // mock instance is actually use, and delete
        auto inst2 = Get1DInstance();
        auto inst1 = Get1DInstance();
        inst1.set_unitid(unit1.id());
        inst2.set_unitid(unit1.id());
        inst1.set_tenantid("123456");
        inst2.set_tenantid("123456");

        std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
        instances.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
        auto ret = resourceView.AddInstances(instances);
        ASSERT_AWAIT_READY(ret);

        std::vector<std::string> ids1{ inst1.instanceid() };
        ret = resourceView.DeleteInstances(ids1);
        ASSERT_AWAIT_READY(ret);

        auto cache = resourceView.GetAgentCacheMap();
        // DisableAgent function is not called, but inst1 is removed
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{1});
        EXPECT_TRUE(cache.find(unit1.id()) == cache.end());
        EXPECT_EQ(count, 0);
        EXPECT_EQ(disabledAgent.count(unit1.id()), size_t{0});

        // mock instance is virtual use, and need to be recycled because agent has been used
        std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
        instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
        ret = resourceView.AddInstances(instances2);
        ASSERT_AWAIT_READY(ret);

        std::vector<std::string> ids2{ inst2.instanceid() };
        ASSERT_AWAIT_READY(resourceView.DeleteInstances(ids2, true));

        EXPECT_AWAIT_TRUE([&]() -> bool { return resourceView.GetReuseTimers().count(unit1.id()) == 0; });
        cache = resourceView.GetAgentCacheMap();
        EXPECT_TRUE(cache.find(unit1.id()) == cache.end());
        EXPECT_EQ(count, 1);
        EXPECT_EQ(disabledAgent.count(unit1.id()), size_t{1});
    }

    {
        // pod not set idlePod, enable tenantAffinity and add unit and then delete virtual instance -> not set timer and
        // not recycle
        auto unit1 = Get1DResourceUnit();
        unit1.mutable_nodelabels()->insert({ IDLE_TO_RECYCLE, idleToRecycleLabels[1] });
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit1));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{0});
        count = 0;  // reset

        // mock instance is virtual use, and delete
        auto inst2 = Get1DInstance();
        auto inst1 = Get1DInstance();
        inst1.set_unitid(unit1.id());
        inst2.set_unitid(unit1.id());
        inst1.set_tenantid("123456");
        inst2.set_tenantid("123456");

        std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
        instances.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
        auto ret = resourceView.AddInstances(instances);
        ASSERT_AWAIT_READY(ret);
        std::vector<std::string> ids1{ inst1.instanceid() };
        ret = resourceView.DeleteInstances(ids1, true);
        ASSERT_AWAIT_READY(ret);
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{0});

        std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
        instances.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
        ret = resourceView.AddInstances(instances2);
        ASSERT_AWAIT_READY(ret);
        std::vector<std::string> ids2{ inst2.instanceid() };
        ret = resourceView.DeleteInstances(ids2, true);
        ASSERT_AWAIT_READY(ret);
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{0});
    }

    {
        // pod not set idlePod, enable tenantAffinity and add unit and then delete instance and add virtual instance,
        // timer is executed, and then virtual instance is deleted  -> pod need to be recycled
        auto unit1 = Get1DResourceUnit();
        unit1.mutable_nodelabels()->insert({ IDLE_TO_RECYCLE, idleToRecycleLabels[1] });
        ASSERT_AWAIT_READY(resourceView.AddResourceUnit(unit1));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{0});
        count = 0;  // reset
        auto inst2 = Get1DInstance();
        auto inst1 = Get1DInstance();
        inst1.set_unitid(unit1.id());
        inst2.set_unitid(unit1.id());
        inst1.set_tenantid("123456");
        inst2.set_tenantid("123456");

        // mock instance is actually use, and delete
        std::map<std::string, resource_view::InstanceAllocatedInfo> instances;
        instances.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
        auto ret = resourceView.AddInstances(instances);
        ASSERT_AWAIT_READY(ret);
        std::vector<std::string> ids1{ inst1.instanceid() };
        ret = resourceView.DeleteInstances(ids1, false);
        ASSERT_AWAIT_READY(ret);
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{1});  // timer is set
        EXPECT_EQ(resourceView.GetAgentUsedMap().count(unit1.id()), size_t{1});

        // mock virtual instance add
        std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
        instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
        ret = resourceView.AddInstances(instances2);
        ASSERT_AWAIT_READY(ret);
        // sleep 1.1s wait for timer executed finished, but no pod will be recycled
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        EXPECT_EQ(resourceView.GetReuseTimers().count(unit1.id()), size_t{1});
        EXPECT_EQ(resourceView.GetAgentUsedMap().count(unit1.id()), size_t{1});
        auto cache = resourceView.GetAgentCacheMap();
        EXPECT_TRUE(cache.find(unit1.id()) != cache.end());

        // delete virtual instance , and pod is need to be recycled
        std::vector<std::string> ids2{ inst2.instanceid() };
        ret = resourceView.DeleteInstances(ids2, true);
        ASSERT_AWAIT_READY(ret);
        EXPECT_AWAIT_TRUE([&]() -> bool { return resourceView.GetReuseTimers().count(unit1.id()) == 0; });
        cache = resourceView.GetAgentCacheMap();
        EXPECT_TRUE(cache.find(unit1.id()) == cache.end());
        EXPECT_EQ(count, 1);
        EXPECT_EQ(disabledAgent.count(unit1.id()), size_t{1});
        EXPECT_EQ(resourceView.GetAgentUsedMap().count(unit1.id()), size_t{0});
    }
}

TEST_F(ResourceViewTest, SetBillingPodResourceInstance)
{
    const std::string podMetricsJson = R"(
{
	"enabledMetrics": ["yr_pod_resource"],
	"backends": [{
		"immediatelyExport": {
			"name": "LakeHouse",
			"enable": true,
			"exporters": [{
				"aomAlarmExporter": {
					"enable": true,
					"ip": "127.0.0.1:8080/",
					"port": 9091
				}
			}]
		}
	}]
})";
    metrics::MetricsAdapter::GetInstance().InitMetricsFromJson(nlohmann::json::parse(podMetricsJson),
                                                               [](std::string backendName) { return "123"; }, {});

    auto parent = resource_view::ResourceView::CreateResourceView(DOMAIN_RESOUCE_VIEW_ID, PARENT_PARAM);
    std::string childNode = LOCAL_RESOUCE_VIEW_ID;
    auto child = resource_view::ResourceView::CreateResourceView(childNode, CHILD_PARAM);
    child->ToReady();

    auto add = parent->AddResourceUnitWithUrl(*child->GetFullResourceView().Get(),
                                              litebus::GetActor(childNode + "-ResourceViewActor")->GetAID().Url());
    ASSERT_TRUE(add.Get().IsOk());
    auto map = metrics::MetricsAdapter::GetInstance().GetMetricsContext().GetPodResourceMap();
    EXPECT_EQ(map.size(), size_t{0});

    ASSERT_TRUE(parent->CheckLocalExistInDomainView(childNode));
    auto localViewInfo = parent->GetLocalInfoInDomain(childNode);
    auto localViewInitTimeOrigin = child->GetResourceView().Get()->viewinittime();

    // enable update
    parent->UpdateIsHeader(true);
    auto unit = Get1DResourceUnit();
    unit.set_ownerid(childNode);
    RecoverResourceView(parent, localViewInitTimeOrigin, unit);

    ASSERT_AWAIT_TRUE(
        []() { return metrics::MetricsAdapter::GetInstance().GetMetricsContext().GetPodResourceMap().size() == 1; });
    map = metrics::MetricsAdapter::GetInstance().GetMetricsContext().GetPodResourceMap();
    EXPECT_NE(map.find(unit.id()), map.end());
}

TEST_F(ResourceViewTest, ValidateSchedulerLevel)
{
    // 1.scheduler_level = Non-root domain
    auto parent = resource_view::ResourceView::CreateResourceView(DOMAIN_RESOUCE_VIEW_ID, PARENT_PARAM);
    EXPECT_EQ(parent->GetResourceInfo().Get().schedulerLevel, SCHEDULER_LEVEL::NON_ROOT_DOMAIN);

    // 2.scheduler_level = root domain
    parent->UpdateIsHeader(true);
    EXPECT_EQ(parent->GetResourceInfo().Get().schedulerLevel, SCHEDULER_LEVEL::ROOT_DOMAIN);

    // 3.scheduler_level = local
    auto child = resource_view::ResourceView::CreateResourceView(LOCAL_RESOUCE_VIEW_ID, CHILD_PARAM);
    EXPECT_EQ(child->GetResourceInfo().Get().schedulerLevel, SCHEDULER_LEVEL::LOCAL);
}

TEST_F(ResourceViewTest, TryDelResourceUnitChange)
{

    std::string childNode = LOCAL_RESOUCE_VIEW_ID;
    auto child = resource_view::ResourceView::CreateResourceView(childNode, CHILD_PARAM);

    // 1.add resourceunit
    auto unit = Get1DResourceUnit();
    auto agentId = unit.id();
    GenerateMinimumUnitBucketInfo(unit);
    ASSERT_TRUE(child->AddResourceUnit(unit).Get().IsOk());

    // 2.add resourceunit
    auto unit2 = Get1DResourceUnit();
    GenerateMinimumUnitBucketInfo(unit2);
    auto agentId2 = unit2.id();
    ASSERT_TRUE(child->AddResourceUnit(unit2).Get().IsOk());

    // 3.modify resourceunit -- add instance1(shared)
    auto inst1 = Get1DInstance();
    (*inst1.mutable_schedulerchain()->Add()) = agentId;
    inst1.set_unitid(agentId);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances1;
    instances1.emplace(inst1.instanceid(), resource_view::InstanceAllocatedInfo{ inst1, nullptr });
    ASSERT_TRUE(child->AddInstances(instances1).Get().IsOk());

    // 4.modify resourceunit -- add instance2(shared)
    auto inst2 = Get1DInstance();
    (*inst2.mutable_schedulerchain()->Add()) = agentId;
    inst2.set_unitid(agentId);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances2;
    instances2.emplace(inst2.instanceid(), resource_view::InstanceAllocatedInfo{ inst2, nullptr });
    ASSERT_TRUE(child->AddInstances(instances2).Get().IsOk());

    auto change = child->GetResourceViewChanges();
    auto childView = child->GetFullResourceView().Get();
    EXPECT_EQ(change.Get()->startrevision(), uint64_t{0});
    EXPECT_EQ(change.Get()->endrevision(), childView->revision());

    // 5.del instance1(shared)
    std::vector<std::string> ids1;
    ids1.push_back(inst1.instanceid());
    ASSERT_TRUE(child->DeleteInstances(ids1).Get().IsOk());

    // 6.del instance2(shared)
    std::vector<std::string> ids2;
    ids2.push_back(inst2.instanceid());
    ASSERT_TRUE(child->DeleteInstances(ids2).Get().IsOk());

    // Test for not del because condition is not satisfied
    child->TryDelResourceUnitChange(2, "AAA");
    childView = child->GetFullResourceView().Get();
    EXPECT_TRUE(child->implActor_->versionChanges_.find(1) != child->implActor_->versionChanges_.end());

    child->TryDelResourceUnitChange(100, childView->viewinittime());
    childView = child->GetFullResourceView().Get();
    EXPECT_TRUE(child->implActor_->versionChanges_.find(1) != child->implActor_->versionChanges_.end());

    child->TryDelResourceUnitChange(2, childView->viewinittime());
    childView = child->GetFullResourceView().Get(); // need get lastest version
    EXPECT_TRUE(child->implActor_->versionChanges_.find(1) == child->implActor_->versionChanges_.end());

    // 7.update status
    ASSERT_TRUE(child->UpdateUnitStatus(agentId, resource_view::UnitStatus::EVICTING).Get().IsOk());

    // 8.modify resourceunit -- add instance3(monopoly)
    auto inst3 = Get1DInstance();
    (*inst3.mutable_schedulerchain()->Add()) = agentId2;
    inst3.mutable_scheduleoption()->set_schedpolicyname(MONOPOLY_SCHEDULE);
    inst3.set_unitid(agentId);
    std::map<std::string, resource_view::InstanceAllocatedInfo> instances3;
    instances3.emplace(inst3.instanceid(), resource_view::InstanceAllocatedInfo{ inst3, nullptr });
    ASSERT_TRUE(child->AddInstances(instances3).Get().IsOk());

    // 9.del resourceunit
    ASSERT_TRUE(child->DeleteResourceUnit(agentId).Get().IsOk());
    child->TryDelResourceUnitChange(4, childView->viewinittime());

    childView = child->GetFullResourceView().Get(); // need get lastest version
    EXPECT_TRUE(child->implActor_->versionChanges_.find(3) == child->implActor_->versionChanges_.end());
    EXPECT_TRUE(child->implActor_->versionChanges_.find(4) != child->implActor_->versionChanges_.end());

}

TEST_F(ResourceViewTest, BindInstanceRuntimeCompletesColdDeployWithoutPreseededRuntime)
{
    using schedule_plugin::filter::UsageAwareFilter;
    struct ConfigGuard {
        double safety = UsageAwareFilter::GetSafety();
        double floor = UsageAwareFilter::GetFloorMb();
        int32_t cap = UsageAwareFilter::GetMaxInstances();
        ~ConfigGuard() { UsageAwareFilter::SetConfig(safety, floor, cap); }
    } guard;
    UsageAwareFilter::SetConfig(0.9, 2048, 0);
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    for (const bool cpuBound : {false, true}) {
        SCOPED_TRACE(cpuBound ? "bind cold CPU" : "bind cold Memory");
        const std::string id = cpuBound ? "bind-cold-cpu" : "bind-cold-memory";
        auto view = resource_view::ResourceView::CreateResourceView(id, CHILD_PARAM);
        auto unit = Get1DResourceUnit(id + "-agent");
        for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
            (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(cpuBound ? 2000 : 32000);
            (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(cpuBound ? 40000 : 4000);
        }
        (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(3000);
        unit.set_memory_sample_unix_ms(nowMs());
        unit.set_cpu_sample_unix_ms(nowMs());
        auto added = view->AddResourceUnit(unit);
        ASSERT_AWAIT_READY(added);
        auto first = Get1DInstance();
        first.set_instanceid("bind-cold-first");
        first.set_requestid("request-bind-cold-first");
        first.set_unitid(unit.id());
        first.mutable_schedulerchain()->Clear();
        *first.mutable_schedulerchain()->Add() = unit.id();
        (*first.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
        auto *cpu = (*first.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
        cpu->set_value(500);
        cpu->set_limit(8000);
        auto *memory = (*first.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
        memory->set_value(512);
        memory->set_limit(8192);
        first.clear_runtimeid(); // elastic cold deploy starts with no runtime identity
        auto allocate = [&](const resources::InstanceInfo &instance) {
            auto promise = std::make_shared<litebus::Promise<Status>>();
            (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
            return promise->GetFuture();
        };
        auto accepted = allocate(first);
        ASSERT_AWAIT_READY(accepted);
        ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
        auto pending = view->GetResourceView();
        ASSERT_AWAIT_READY(pending);
        EXPECT_TRUE(pending.Get()->instances().at(first.instanceid()).runtimeid().empty());
        EXPECT_TRUE(pending.Get()->instances().at(first.instanceid()).resource_admission_pending());
        // Telemetry naming an unknown runtime must never select the incarnation.
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto ghost = std::make_shared<ResourceUnit>(unit);
        ghost->set_memory_sample_unix_ms(nowMs());
        ghost->set_cpu_sample_unix_ms(ghost->memory_sample_unix_ms());
        auto &ghostObserved = (*ghost->mutable_instances())[first.instanceid()];
        ghostObserved.set_runtimeid("runtime-bind-cold-ghost");
        auto *ghostSample = ghostObserved.mutable_sandbox_resource_sample();
        ghostSample->set_runtime_id(ghostObserved.runtimeid());
        ghostSample->set_sandbox_id("bind-cold-ghost-sandbox");
        ghostSample->set_resource_generation("bind-cold-ghost-generation");
        ghostSample->set_sampled_at_unix_ms(ghost->memory_sample_unix_ms());
        ghostSample->set_cpu_usage_known(true);
        ghostSample->set_cpu_usage_cores(0.05);
        ghostSample->set_cpu_limits_known(true);
        ghostSample->set_cpu_period_usec(100000);
        ghostSample->set_cpu_quota_usec(50000);
        ghostSample->set_memory_limit_bytes(512ULL * 1024 * 1024);
        ghostSample->set_memory_current_bytes(400ULL * 1024 * 1024);
        auto ghostReport = view->UpdateResourceUnit(ghost, UpdateType::UPDATE_ACTUAL);
        ASSERT_AWAIT_READY(ghostReport);
        auto unbound = view->GetResourceView();
        ASSERT_AWAIT_READY(unbound);
        EXPECT_TRUE(unbound.Get()->instances().at(first.instanceid()).runtimeid().empty());
        EXPECT_TRUE(unbound.Get()->instances().at(first.instanceid()).resource_admission_pending());
        // Authoritative deployment completion binds the runtime allocated for this request.
        auto deployed = first;
        deployed.set_runtimeid("runtime-bind-cold-first");
        EXPECT_TRUE(unbound.Get()->instances().at(first.instanceid()).functionagentid().empty());
        deployed.set_functionagentid(unit.id());
        auto bind = view->BindInstanceRuntime(deployed, "");
        ASSERT_AWAIT_READY(bind);
        EXPECT_TRUE(bind.Get().IsOk()) << bind.Get().ToString();
        auto bound = view->GetResourceView();
        ASSERT_AWAIT_READY(bound);
        EXPECT_EQ(bound.Get()->instances().at(first.instanceid()).runtimeid(), deployed.runtimeid());
        EXPECT_EQ(bound.Get()->fragment().at(unit.id()).instances().at(first.instanceid()).runtimeid(),
            deployed.runtimeid());
        EXPECT_EQ(bound.Get()->instances().at(first.instanceid()).functionagentid(), unit.id());
        EXPECT_EQ(bound.Get()->fragment().at(unit.id()).instances().at(first.instanceid()).functionagentid(),
            unit.id());
        // The incompatible ghost sample is dropped; admission still needs fresh physical truth.
        EXPECT_TRUE(bound.Get()->instances().at(first.instanceid()).sandbox_resource_sample().runtime_id().empty());
        EXPECT_TRUE(bound.Get()->instances().at(first.instanceid()).resource_admission_pending());
        // Re-binding the exact runtime that is already bound stays idempotent.
        auto again = view->BindInstanceRuntime(deployed, "");
        ASSERT_AWAIT_READY(again);
        EXPECT_TRUE(again.Get().IsOk()) << again.Get().ToString();
        again = view->BindInstanceRuntime(deployed, deployed.runtimeid());
        ASSERT_AWAIT_READY(again);
        EXPECT_TRUE(again.Get().IsOk()) << again.Get().ToString();
        // Physical reconciliation of the bound runtime releases admission and feeds the resize budget.
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        // Keep node totals steady to isolate binding from the separate growth-forecast guard.
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(3000);
        auto &observed = (*report->mutable_instances())[first.instanceid()];
        observed.set_runtimeid(deployed.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(deployed.runtimeid());
        sample->set_sandbox_id("bind-cold-sandbox");
        sample->set_resource_generation("bind-cold-generation");
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(512ULL * 1024 * 1024);
        sample->set_memory_current_bytes(430ULL * 1024 * 1024);
        sample->set_elastic_policy(R"({"version":1,"CPU":{"initial":500,"ceiling":8000},"Memory":{"initial":512,"ceiling":8192}})");
        auto refreshed = view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
        ASSERT_AWAIT_READY(refreshed);
        auto after = view->GetResourceView();
        ASSERT_AWAIT_READY(after);
        EXPECT_FALSE(after.Get()->instances().at(first.instanceid()).resource_admission_pending());
        EXPECT_EQ(after.Get()->instances().at(first.instanceid()).runtimeid(), deployed.runtimeid());
        EXPECT_EQ(after.Get()->fragment().at(unit.id()).instances().at(first.instanceid()).runtimeid(),
            deployed.runtimeid());
        EXPECT_EQ(after.Get()->instances().at(first.instanceid()).sandbox_resource_sample().runtime_id(),
            deployed.runtimeid());
        // The bound runtime can lease the policy target for resize.
        messages::ResizeSandboxRequest request;
        request.set_requestid("bind-cold-lease");
        request.set_instanceid(first.instanceid());
        request.set_runtimeid(deployed.runtimeid());
        auto *resize = request.mutable_resize();
        resize->set_sandbox_id("bind-cold-sandbox");
        resize->set_resource_generation("bind-cold-generation");
        resize->set_deadline_unix_ms(nowMs() + 30000);
        resize->mutable_expected()->set_cpu_quota_usec(50000);
        resize->mutable_expected()->set_cpu_period_usec(100000);
        resize->mutable_expected()->set_memory_limit_bytes(512ULL * 1024 * 1024);
        const auto &eligible = after.Get()->instances().at(first.instanceid());
        auto target = local_scheduler::PressureResizeTarget(eligible, nullptr, nowMs(), 30000,
            true, true, 256ULL * 1024 * 1024, 1.5, 4096ULL * 1024 * 1024, 200000);
        ASSERT_TRUE(target.has_value());
        EXPECT_EQ(target->memory_limit_bytes(), 768ULL * 1024 * 1024);
        *resize->mutable_target() = *target;
        auto unboundIdentity = eligible;
        unboundIdentity.clear_runtimeid();
        EXPECT_FALSE(local_scheduler::PressureResizeTarget(unboundIdentity, nullptr, nowMs(), 30000,
            true, true, 256ULL * 1024 * 1024, 1.5).has_value());
        auto reserved = view->ReserveResize(request);
        ASSERT_AWAIT_READY(reserved);
        EXPECT_TRUE(reserved.Get().IsOk()) << reserved.Get().ToString();
    }
}

TEST_F(ResourceViewTest, BindInstanceRuntimeRejectsStaleIdentitiesAndConflictingExpectations)
{
    auto view = resource_view::ResourceView::CreateResourceView("bind-reject", CHILD_PARAM);
    auto unit = Get1DResourceUnit("bind-reject-agent");
    auto otherUnit = Get1DResourceUnit("bind-reject-other-agent");
    for (auto *candidate : {&unit, &otherUnit}) {
        for (auto *resources : {candidate->mutable_capacity(), candidate->mutable_allocatable()}) {
            (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
            (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(40000);
        }
    }
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    added = view->AddResourceUnit(otherUnit);
    ASSERT_AWAIT_READY(added);
    auto target = Get1DInstance();
    target.set_instanceid("bind-target");
    target.set_requestid("request-bind-target");
    target.set_unitid(unit.id());
    target.mutable_schedulerchain()->Clear();
    *target.mutable_schedulerchain()->Add() = unit.id();
    target.clear_runtimeid();
    auto allocate = [&](const resources::InstanceInfo &instance) {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
        return promise->GetFuture();
    };
    auto accepted = allocate(target);
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    auto deployed = target;
    deployed.set_runtimeid("runtime-bind-target");
    auto bind = view->BindInstanceRuntime(deployed, "");
    ASSERT_AWAIT_READY(bind);
    EXPECT_TRUE(bind.Get().IsOk()) << bind.Get().ToString();
    auto storedRuntime = [&](std::string &runtime) {
        auto current = view->GetResourceView();
        ASSERT_AWAIT_READY(current);
        runtime = current.Get()->instances().at(target.instanceid()).runtimeid();
    };
    std::string runtime;
    storedRuntime(runtime);
    EXPECT_EQ(runtime, deployed.runtimeid());
    // Unknown instance identity is refused.
    auto unknown = deployed;
    unknown.set_instanceid("bind-never-allocated");
    auto refused = view->BindInstanceRuntime(unknown, "");
    ASSERT_AWAIT_READY(refused);
    EXPECT_FALSE(refused.Get().IsOk());
    // A request other than the one that owns the allocation cannot bind.
    auto staleRequest = deployed;
    staleRequest.set_requestid("request-bind-stranger");
    refused = view->BindInstanceRuntime(staleRequest, "");
    ASSERT_AWAIT_READY(refused);
    EXPECT_FALSE(refused.Get().IsOk());
    // A unit other than the allocation's unit cannot bind.
    auto staleUnit = deployed;
    staleUnit.set_unitid(otherUnit.id());
    staleUnit.mutable_schedulerchain()->Clear();
    *staleUnit.mutable_schedulerchain()->Add() = otherUnit.id();
    refused = view->BindInstanceRuntime(staleUnit, "");
    ASSERT_AWAIT_READY(refused);
    EXPECT_FALSE(refused.Get().IsOk());
    auto staleRoute = deployed;
    staleRoute.set_functionagentid(otherUnit.id());
    refused = view->BindInstanceRuntime(staleRoute, "");
    ASSERT_AWAIT_READY(refused);
    EXPECT_FALSE(refused.Get().IsOk());
    // A wrong previous-runtime expectation is a conflicting CAS, not an overwrite.
    auto usurper = deployed;
    usurper.set_runtimeid("runtime-bind-usurper");
    refused = view->BindInstanceRuntime(usurper, "runtime-bind-wrong-previous");
    ASSERT_AWAIT_READY(refused);
    EXPECT_FALSE(refused.Get().IsOk());
    storedRuntime(runtime);
    EXPECT_EQ(runtime, deployed.runtimeid());
    // Binding with the exact previous runtime rotates the incarnation.
    auto rotated = usurper;
    auto rotation = view->BindInstanceRuntime(rotated, deployed.runtimeid());
    ASSERT_AWAIT_READY(rotation);
    EXPECT_TRUE(rotation.Get().IsOk()) << rotation.Get().ToString();
    storedRuntime(runtime);
    EXPECT_EQ(runtime, rotated.runtimeid());
    auto current = view->GetResourceView();
    ASSERT_AWAIT_READY(current);
    EXPECT_EQ(current.Get()->fragment().at(unit.id()).instances().at(target.instanceid()).runtimeid(),
        rotated.runtimeid());
    // Re-binding the rotated runtime stays idempotent under either expectation form.
    auto again = view->BindInstanceRuntime(rotated, deployed.runtimeid());
    ASSERT_AWAIT_READY(again);
    EXPECT_TRUE(again.Get().IsOk()) << again.Get().ToString();
    again = view->BindInstanceRuntime(rotated, rotated.runtimeid());
    ASSERT_AWAIT_READY(again);
    EXPECT_TRUE(again.Get().IsOk()) << again.Get().ToString();
    // None of the refusals above disturbed the stored incarnation.
    storedRuntime(runtime);
    EXPECT_EQ(runtime, rotated.runtimeid());
}

TEST_F(ResourceViewTest, BindInstanceRuntimeSurvivesDeleteRecreateAndForeignTelemetry)
{
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    auto view = resource_view::ResourceView::CreateResourceView("bind-recreate", CHILD_PARAM);
    auto unit = Get1DResourceUnit("bind-recreate-agent");
    for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
        (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
        (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(40000);
    }
    unit.set_memory_sample_unix_ms(nowMs());
    unit.set_cpu_sample_unix_ms(nowMs());
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    auto makeInstance = [&](const std::string &request) {
        auto instance = Get1DInstance();
        instance.set_instanceid("bind-recreated");
        instance.set_requestid(request);
        instance.set_unitid(unit.id());
        instance.mutable_schedulerchain()->Clear();
        *instance.mutable_schedulerchain()->Add() = unit.id();
        instance.clear_runtimeid();
        return instance;
    };
    auto allocate = [&](const resources::InstanceInfo &instance) {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
        return promise->GetFuture();
    };
    auto publish = [&](const std::string &observedRuntime, const std::string &sandbox, const std::string &generation) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        auto &observed = (*report->mutable_instances())["bind-recreated"];
        observed.set_runtimeid(observedRuntime);
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(observedRuntime);
        sample->set_sandbox_id(sandbox);
        sample->set_resource_generation(generation);
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(512ULL * 1024 * 1024);
        sample->set_memory_current_bytes(400ULL * 1024 * 1024);
        return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
    };
    const auto originalRequest = "request-bind-recreate-original";
    auto first = makeInstance(originalRequest);
    auto accepted = allocate(first);
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    auto deployedFirst = first;
    deployedFirst.set_runtimeid("runtime-recreate-first");
    auto bind = view->BindInstanceRuntime(deployedFirst, "");
    ASSERT_AWAIT_READY(bind);
    EXPECT_TRUE(bind.Get().IsOk()) << bind.Get().ToString();
    auto reconciled = publish(deployedFirst.runtimeid(), "recreate-sandbox", "recreate-generation");
    ASSERT_AWAIT_READY(reconciled);
    // Delete the incarnation and recreate the same instance identity for a new request.
    auto removed = view->DeleteInstances({"bind-recreated"});
    ASSERT_AWAIT_READY(removed);
    EXPECT_TRUE(removed.Get().IsOk());
    auto recreated = makeInstance("request-bind-recreate-second");
    accepted = allocate(recreated);
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    // The retired incarnation's bind must not touch the fresh allocation.
    auto stale = view->BindInstanceRuntime(deployedFirst, "");
    ASSERT_AWAIT_READY(stale);
    EXPECT_FALSE(stale.Get().IsOk());
    stale = view->BindInstanceRuntime(deployedFirst, deployedFirst.runtimeid());
    ASSERT_AWAIT_READY(stale);
    EXPECT_FALSE(stale.Get().IsOk());
    auto current = view->GetResourceView();
    ASSERT_AWAIT_READY(current);
    EXPECT_TRUE(current.Get()->instances().at("bind-recreated").runtimeid().empty());
    // The new request binds its own runtime.
    auto deployedSecond = recreated;
    deployedSecond.set_runtimeid("runtime-recreate-second");
    bind = view->BindInstanceRuntime(deployedSecond, "");
    ASSERT_AWAIT_READY(bind);
    EXPECT_TRUE(bind.Get().IsOk()) << bind.Get().ToString();
    current = view->GetResourceView();
    ASSERT_AWAIT_READY(current);
    EXPECT_EQ(current.Get()->instances().at("bind-recreated").runtimeid(), deployedSecond.runtimeid());
    EXPECT_EQ(current.Get()->fragment().at(unit.id()).instances().at("bind-recreated").runtimeid(),
        deployedSecond.runtimeid());
    reconciled = publish(deployedSecond.runtimeid(), "recreate-sandbox", "recreate-generation");
    ASSERT_AWAIT_READY(reconciled);
    // Foreign telemetry cannot overwrite the bound incarnation or its physical sample.
    auto foreign = publish("runtime-recreate-foreign", "foreign-sandbox", "foreign-generation");
    ASSERT_AWAIT_READY(foreign);
    current = view->GetResourceView();
    ASSERT_AWAIT_READY(current);
    const auto &stored = current.Get()->instances().at("bind-recreated");
    EXPECT_EQ(stored.runtimeid(), deployedSecond.runtimeid());
    EXPECT_EQ(stored.sandbox_resource_sample().runtime_id(), deployedSecond.runtimeid());
    EXPECT_EQ(stored.sandbox_resource_sample().sandbox_id(), "recreate-sandbox");
    const auto &storedFragment = current.Get()->fragment().at(unit.id()).instances().at("bind-recreated");
    EXPECT_EQ(storedFragment.runtimeid(), deployedSecond.runtimeid());
}

// ---------------------------------------------------------------------------
// M2 bidirectional memory-elasticity budget and sample fences. These tests run
// with the in-process fence override; production keeps the env opt-in. All
// assertions go through public admission behaviour and stored view state.
// ---------------------------------------------------------------------------

namespace {
// MiB helpers keep the credit arithmetic readable against the plan sample.
constexpr uint64_t MiB(uint64_t value) { return value * 1024ULL * 1024ULL; }
// The daemon's fixed soft-limit derivation: floor(0.8 * max / 4096) * 4096.
constexpr uint64_t HighBytes(uint64_t maxBytes) { return (maxBytes * 4 / 5 / 4096) * 4096; }
}  // namespace

TEST_F(ResourceViewTest, ShrinkBudgetTrimsGrowCreditsOnlyAfterConfirmedContract)
{
    using schedule_plugin::filter::UsageAwareFilter;
    struct ConfigGuard {
        double safety = UsageAwareFilter::GetSafety();
        double floor = UsageAwareFilter::GetFloorMb();
        int32_t cap = UsageAwareFilter::GetMaxInstances();
        ~ConfigGuard() { UsageAwareFilter::SetConfig(safety, floor, cap); }
    } guard;
    UsageAwareFilter::SetConfig(0.9, 2048, 0);
    struct FenceGuard {
        FenceGuard() { SetMemoryElasticityFenceForTest(true); }
        ~FenceGuard() { SetMemoryElasticityFenceForTest(false); }
    } fenceGuard;
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    auto view = resource_view::ResourceView::CreateResourceView("shrink-budget", CHILD_PARAM);
    auto unit = Get1DResourceUnit("shrink-budget-agent");
    for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
        (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
        (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(4096);
    }
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
    unit.set_cpu_sample_unix_ms(nowMs());
    unit.set_memory_sample_unix_ms(nowMs());
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    auto owner = Get1DInstance();
    owner.set_instanceid("ledger-owner");
    owner.set_requestid("request-ledger-owner");
    owner.set_unitid(unit.id());
    owner.set_runtimeid("runtime-ledger-owner");
    owner.mutable_schedulerchain()->Clear();
    *owner.mutable_schedulerchain()->Add() = unit.id();
    (*owner.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    auto *ownerCpu = (*owner.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
    ownerCpu->set_value(500);
    ownerCpu->set_limit(8000);
    auto *ownerMem = (*owner.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
    ownerMem->set_value(128);
    ownerMem->set_limit(8192);
    auto allocate = [&](const resources::InstanceInfo &instance) {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
        return promise->GetFuture();
    };
    auto accepted = allocate(owner);
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    // One physical Stats shape for the whole test; only the contract fields vary.
    auto publish = [&](uint64_t maxBytes, uint64_t currentBytes, uint64_t revision, const std::string &operationId,
                       bool operationPending, bool frozen, const std::string &sandbox = "ledger-sandbox",
                       const std::string &generation = "ledger-generation") {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
        auto &observed = (*report->mutable_instances())[owner.instanceid()];
        observed.set_runtimeid(owner.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(owner.runtimeid());
        sample->set_sandbox_id(sandbox);
        sample->set_resource_generation(generation);
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(maxBytes);
        sample->set_memory_limit_known(true);
        sample->set_memory_usage_known(true);
        sample->set_memory_current_bytes(currentBytes);
        sample->set_resource_revision(revision);
        sample->set_resource_revision_known(true);
        sample->set_memory_high_bytes(HighBytes(maxBytes));
        sample->set_memory_high_known(true);
        sample->set_memory_high_unlimited(false);
        sample->set_frozen(frozen);
        sample->set_frozen_known(true);
        sample->set_resource_operation_id(operationId);
        sample->set_resource_operation_pending(operationPending);
        return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
    };
    auto reserve = [&](const std::string &requestID, const std::string &operationID, uint64_t expectedBytes,
                       uint64_t targetBytes, uint64_t expectedRevision, bool shrink) {
        messages::ResizeSandboxRequest request;
        request.set_requestid(requestID);
        request.set_instanceid(owner.instanceid());
        request.set_runtimeid(owner.runtimeid());
        request.set_shrink(shrink);
        auto *resize = request.mutable_resize();
        resize->set_sandbox_id("ledger-sandbox");
        resize->set_resource_generation("ledger-generation");
        resize->set_operation_id(operationID);
        resize->set_expected_revision(expectedRevision);
        resize->set_deadline_unix_ms(nowMs() + 30000);
        resize->mutable_expected()->set_cpu_quota_usec(50000);
        resize->mutable_expected()->set_cpu_period_usec(100000);
        resize->mutable_expected()->set_memory_limit_bytes(expectedBytes);
        resize->mutable_target()->set_cpu_quota_usec(50000);
        resize->mutable_target()->set_cpu_period_usec(100000);
        resize->mutable_target()->set_memory_limit_bytes(targetBytes);
        return view->ReserveResize(request);
    };
    auto observe = [&](const std::string &requestID, const std::string &operationID, uint64_t revision,
                       uint64_t actualBytes, bool complete, bool operationPending, bool frozen,
                       messages::ResizeSandboxResponse::Outcome outcome) {
        messages::ResizeSandboxResponse response;
        response.set_requestid(requestID);
        response.set_instanceid(owner.instanceid());
        response.set_runtimeid(owner.runtimeid());
        response.set_sandboxid("ledger-sandbox");
        response.set_resourcegeneration("ledger-generation");
        response.set_outcome(outcome);
        auto *result = response.mutable_result();
        result->set_complete(complete);
        auto *actual = result->mutable_actual();
        actual->set_resource_generation("ledger-generation");
        actual->set_cpu_limits_known(true);
        actual->set_cpu_quota_usec(50000);
        actual->set_cpu_period_usec(100000);
        actual->set_memory_limit_bytes(actualBytes);
        actual->set_memory_limit_known(true);
        actual->set_resource_revision(revision);
        actual->set_resource_revision_known(true);
        actual->set_resource_operation_id(operationID);
        actual->set_resource_operation_pending(operationPending);
        actual->set_frozen(frozen);
        actual->set_frozen_known(true);
        actual->set_memory_high_bytes(HighBytes(actualBytes));
        actual->set_memory_high_known(true);
        return view->ObserveResize(response);
    };
    auto probeStatus = [&](const std::string &name, double memoryMb) -> Status {
        auto instance = Get1DInstance();
        instance.set_instanceid(name);
        instance.set_requestid("request-" + name);
        instance.set_unitid(unit.id());
        instance.set_runtimeid("runtime-" + name);
        instance.mutable_schedulerchain()->Clear();
        *instance.mutable_schedulerchain()->Add() = unit.id();
        (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
        auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
        cpu->set_value(500);
        cpu->set_limit(8000);
        auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
        memory->set_value(memoryMb);
        memory->set_limit(8192);
        auto future = allocate(instance);
        if (!future.WaitFor(TEST_AWAIT_TIMEOUT).IsOK()) {
            return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "probe future did not resolve");
        }
        return future.Get();
    };
    auto probeRejected = [&](const std::string &name, double memoryMb) {
        EXPECT_FALSE(probeStatus(name, memoryMb).IsOk()) << "probe " << name << " must be rejected";
    };
    auto probeAdmitted = [&](const std::string &name, double memoryMb) {
        const auto status = probeStatus(name, memoryMb);
        EXPECT_TRUE(status.IsOk()) << "probe " << name << " must be admitted: " << status.ToString();
        auto removed = view->DeleteInstances({name});
        ASSERT_AWAIT_READY(removed);
    };

    // Birth state: revision 1 at the 128MiB request; no grow ledger, no baseline.
    auto physical = publish(MiB(128), MiB(100), 1, "", false, false);
    ASSERT_AWAIT_READY(physical);
    probeAdmitted("probe-fresh", 600); // 2800 + 0 held

    // grow 128->256 then 256->512; each settle holds landed - expected.
    auto reserved = reserve("ledger-grow-1", "op-grow-1", MiB(128), MiB(256), 1, false);
    ASSERT_AWAIT_READY(reserved);
    EXPECT_TRUE(reserved.Get().IsOk()) << reserved.Get().ToString();
    auto observed = observe("ledger-grow-1", "op-grow-1", 2, MiB(256), true, false, false,
        messages::ResizeSandboxResponse::OBSERVED);
    ASSERT_AWAIT_READY(observed);
    EXPECT_TRUE(observed.Get().IsOk()) << observed.Get().ToString();
    reserved = reserve("ledger-grow-2", "op-grow-2", MiB(256), MiB(512), 2, false);
    ASSERT_AWAIT_READY(reserved);
    EXPECT_TRUE(reserved.Get().IsOk()) << reserved.Get().ToString();
    observed = observe("ledger-grow-2", "op-grow-2", 3, MiB(512), true, false, false,
        messages::ResizeSandboxResponse::OBSERVED);
    ASSERT_AWAIT_READY(observed);
    EXPECT_TRUE(observed.Get().IsOk()) << observed.Get().ToString();
    probeRejected("probe-two-grows", 600);  // 2800 + 384 held
    probeAdmitted("probe-two-grows-fit", 400);

    // Own peak 269MiB consumes the first entry and most of the second.
    physical = publish(MiB(512), MiB(269), 3, "op-grow-2", false, false);
    ASSERT_AWAIT_READY(physical);
    probeRejected("probe-after-peak", 700); // 2800 + 243 held
    probeAdmitted("probe-after-peak-fit", 600);

    // A pending shrink never pre-releases the remaining 243MiB.
    reserved = reserve("ledger-shrink-1", "op-shrink-1", MiB(512), MiB(256), 3, true);
    ASSERT_AWAIT_READY(reserved);
    EXPECT_TRUE(reserved.Get().IsOk()) << reserved.Get().ToString();
    observed = observe("ledger-shrink-1", "op-shrink-1", 0, 0, false, false, false,
        messages::ResizeSandboxResponse::UNKNOWN);
    ASSERT_AWAIT_READY(observed);
    EXPECT_TRUE(observed.Get().IsOk()) << observed.Get().ToString();
    probeRejected("probe-pending-shrink", 700);
    // Same-revision contradictory limits and a frozen mid-operation terminal
    // state must not close the operation by numeric equality.
    physical = publish(MiB(256), MiB(269), 3, "op-grow-2", false, false);
    ASSERT_AWAIT_READY(physical);
    probeRejected("probe-numeric-close", 700);
    physical = publish(MiB(256), MiB(269), 4, "op-shrink-1", true, true);
    ASSERT_AWAIT_READY(physical);
    probeRejected("probe-frozen-close", 700);

    // Complete, unfrozen, matched op/revision readback trims the cap and
    // releases the credit; a later identical shrink cannot release again.
    observed = observe("ledger-shrink-1", "op-shrink-1", 4, MiB(256), true, false, false,
        messages::ResizeSandboxResponse::OBSERVED);
    ASSERT_AWAIT_READY(observed);
    EXPECT_TRUE(observed.Get().IsOk()) << observed.Get().ToString();
    probeAdmitted("probe-after-shrink", 700); // 2800 + 0 held
    reserved = reserve("ledger-shrink-2", "op-shrink-2", MiB(256), MiB(128), 4, true);
    ASSERT_AWAIT_READY(reserved);
    EXPECT_TRUE(reserved.Get().IsOk()) << reserved.Get().ToString();
    observed = observe("ledger-shrink-2", "op-shrink-2", 5, MiB(128), true, false, false,
        messages::ResizeSandboxResponse::OBSERVED);
    ASSERT_AWAIT_READY(observed);
    EXPECT_TRUE(observed.Get().IsOk()) << observed.Get().ToString();
    probeAdmitted("probe-after-second-shrink", 700);

    // Regrow builds a new entry; replays with a stale revision or a reused
    // operation id are refused even when the numbers match.
    reserved = reserve("ledger-grow-3", "op-grow-3", MiB(128), MiB(512), 5, false);
    ASSERT_AWAIT_READY(reserved);
    EXPECT_TRUE(reserved.Get().IsOk()) << reserved.Get().ToString();
    probeRejected("probe-after-regrow", 700); // 2800 + 384 held
    auto replayed = reserve("ledger-grow-replay", "op-grow-replay", MiB(256), MiB(512), 2, false);
    ASSERT_AWAIT_READY(replayed);
    EXPECT_FALSE(replayed.Get().IsOk());
    replayed = reserve("ledger-op-reuse", "op-grow-3", MiB(128), MiB(256), 5, false);
    ASSERT_AWAIT_READY(replayed);
    EXPECT_FALSE(replayed.Get().IsOk());
}

TEST_F(ResourceViewTest, SampleFenceRejectsStaleAndUnauthorizedReadingsInBothIndexes)
{
    struct FenceGuard {
        FenceGuard() { SetMemoryElasticityFenceForTest(true); }
        ~FenceGuard() { SetMemoryElasticityFenceForTest(false); }
    } fenceGuard;
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    auto view = resource_view::ResourceView::CreateResourceView("sample-fence", CHILD_PARAM);
    auto unit = Get1DResourceUnit("sample-fence-agent");
    for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
        (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
        (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(4096);
    }
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
    unit.set_cpu_sample_unix_ms(nowMs());
    unit.set_memory_sample_unix_ms(nowMs());
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    auto instance = Get1DInstance();
    instance.set_instanceid("fence-instance");
    instance.set_requestid("request-fence-instance");
    instance.set_unitid(unit.id());
    instance.set_runtimeid("runtime-fence-instance");
    instance.mutable_schedulerchain()->Clear();
    *instance.mutable_schedulerchain()->Add() = unit.id();
    (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
    cpu->set_value(500);
    cpu->set_limit(8000);
    auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
    memory->set_value(128);
    memory->set_limit(8192);
    auto promise = std::make_shared<litebus::Promise<Status>>();
    (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
    auto accepted = promise->GetFuture();
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    // reportedUsageMB doubles as the per-instance actualuse marker: a rejected
    // report must not change it, an accepted one must.
    auto publish = [&](uint64_t revision, bool revisionKnown, uint64_t limitBytes, uint64_t currentBytes,
                       double reportedUsageMb, const std::string &generation = "fence-generation") {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
        auto &observed = (*report->mutable_instances())[instance.instanceid()];
        observed.set_runtimeid(instance.runtimeid());
        (*observed.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(50);
        (*observed.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(
            reportedUsageMb);
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(instance.runtimeid());
        sample->set_sandbox_id("fence-sandbox");
        sample->set_resource_generation(generation);
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(limitBytes);
        sample->set_memory_limit_known(true);
        sample->set_memory_usage_known(true);
        sample->set_memory_current_bytes(currentBytes);
        sample->set_resource_revision(revision);
        sample->set_resource_revision_known(revisionKnown);
        sample->set_memory_high_bytes(HighBytes(limitBytes));
        sample->set_memory_high_known(true);
        sample->set_memory_high_unlimited(false);
        sample->set_frozen(false);
        sample->set_frozen_known(true);
        sample->set_resource_operation_id("");
        sample->set_resource_operation_pending(false);
        return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
    };
    auto storedSample = [&](resources::SandboxResourceSample &top, resources::SandboxResourceSample &leaf) {
        auto current = view->GetResourceView();
        ASSERT_AWAIT_READY(current);
        top = current.Get()->instances().at(instance.instanceid()).sandbox_resource_sample();
        leaf = current.Get()->fragment().at(unit.id()).instances().at(instance.instanceid()).sandbox_resource_sample();
    };
    auto storedUsageMb = [&](double &topUsage, double &leafUsage) {
        auto current = view->GetResourceView();
        ASSERT_AWAIT_READY(current);
        topUsage = current.Get()->instances().at(instance.instanceid())
            .actualuse().resources().at(RESOURCE_MEM_NAME).scalar().value();
        leafUsage = current.Get()->fragment().at(unit.id()).instances().at(instance.instanceid())
            .actualuse().resources().at(RESOURCE_MEM_NAME).scalar().value();
    };

    // Recovery window: the first revision-known reading is adopted as baseline.
    auto physical = publish(3, true, MiB(400), MiB(100), 111);
    ASSERT_AWAIT_READY(physical);
    resources::SandboxResourceSample top;
    resources::SandboxResourceSample leaf;
    ASSERT_NO_FATAL_FAILURE(storedSample(top, leaf));
    EXPECT_EQ(top.resource_revision(), 3);
    EXPECT_EQ(top.memory_limit_bytes(), MiB(400));
    EXPECT_EQ(leaf.resource_revision(), 3);
    EXPECT_EQ(leaf.memory_limit_bytes(), MiB(400));

    // Older revision, even with a fresh timestamp.
    physical = publish(2, true, MiB(400), MiB(90), 222);
    ASSERT_AWAIT_READY(physical);
    // Unauthorized newer revision: no dispatched operation owns revision 6.
    physical = publish(6, true, MiB(400), MiB(90), 222);
    ASSERT_AWAIT_READY(physical);
    // Same revision with contradictory limits: keep the confirmed value.
    physical = publish(3, true, MiB(512), MiB(90), 222);
    ASSERT_AWAIT_READY(physical);
    // Same revision but a foreign generation.
    physical = publish(3, true, MiB(400), MiB(90), 222, "fence-foreign-generation");
    ASSERT_AWAIT_READY(physical);
    // Revision-less sample from a pre-contract source.
    physical = publish(0, false, MiB(400), MiB(90), 222);
    ASSERT_AWAIT_READY(physical);
    ASSERT_NO_FATAL_FAILURE(storedSample(top, leaf));
    EXPECT_EQ(top.resource_revision(), 3);
    EXPECT_EQ(top.memory_limit_bytes(), MiB(400));
    EXPECT_EQ(top.memory_current_bytes(), MiB(100));
    EXPECT_EQ(top.resource_generation(), "fence-generation");
    EXPECT_EQ(leaf.resource_revision(), 3);
    EXPECT_EQ(leaf.memory_limit_bytes(), MiB(400));
    EXPECT_EQ(leaf.memory_current_bytes(), MiB(100));
    double topUsage = 0;
    double leafUsage = 0;
    ASSERT_NO_FATAL_FAILURE(storedUsageMb(topUsage, leafUsage));
    EXPECT_DOUBLE_EQ(topUsage, 111);
    EXPECT_DOUBLE_EQ(leafUsage, 111);

    // A legitimate same-revision reading refreshes usage and metadata only.
    physical = publish(3, true, MiB(400), MiB(150), 333);
    ASSERT_AWAIT_READY(physical);
    ASSERT_NO_FATAL_FAILURE(storedSample(top, leaf));
    EXPECT_EQ(top.resource_revision(), 3);
    EXPECT_EQ(top.memory_limit_bytes(), MiB(400));
    EXPECT_EQ(top.memory_current_bytes(), MiB(150));
    EXPECT_EQ(leaf.memory_current_bytes(), MiB(150));
    ASSERT_NO_FATAL_FAILURE(storedUsageMb(topUsage, leafUsage));
    EXPECT_DOUBLE_EQ(topUsage, 333);
    EXPECT_DOUBLE_EQ(leafUsage, 333);
}

TEST_F(ResourceViewTest, RestartRecoveryHoldsFullCurrentMaxUntilShrinkTrimsIt)
{
    using schedule_plugin::filter::UsageAwareFilter;
    struct ConfigGuard {
        double safety = UsageAwareFilter::GetSafety();
        double floor = UsageAwareFilter::GetFloorMb();
        int32_t cap = UsageAwareFilter::GetMaxInstances();
        ~ConfigGuard() { UsageAwareFilter::SetConfig(safety, floor, cap); }
    } guard;
    UsageAwareFilter::SetConfig(0.9, 2048, 0);
    struct FenceGuard {
        FenceGuard() { SetMemoryElasticityFenceForTest(true); }
        ~FenceGuard() { SetMemoryElasticityFenceForTest(false); }
    } fenceGuard;
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    // A fresh view stands in for the restarted process: the grow ledger is gone.
    auto view = resource_view::ResourceView::CreateResourceView("recovery-budget", CHILD_PARAM);
    auto unit = Get1DResourceUnit("recovery-budget-agent");
    for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
        (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
        (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(4096);
    }
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
    unit.set_cpu_sample_unix_ms(nowMs());
    unit.set_memory_sample_unix_ms(nowMs());
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    auto makeOwner = [&](const std::string &request) {
        auto owner = Get1DInstance();
        owner.set_instanceid("recovery-owner");
        owner.set_requestid(request);
        owner.set_unitid(unit.id());
        owner.set_runtimeid("runtime-recovery-owner");
        owner.mutable_schedulerchain()->Clear();
        *owner.mutable_schedulerchain()->Add() = unit.id();
        (*owner.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
        auto *cpu = (*owner.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
        cpu->set_value(500);
        cpu->set_limit(8000);
        auto *memory = (*owner.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
        memory->set_value(128);
        memory->set_limit(8192);
        return owner;
    };
    auto allocate = [&](const resources::InstanceInfo &instance) {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
        return promise->GetFuture();
    };
    auto owner = makeOwner("request-recovery-owner");
    auto accepted = allocate(owner);
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    auto publish = [&](uint64_t revision, bool revisionKnown, uint64_t maxBytes, uint64_t currentBytes) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
        auto &observed = (*report->mutable_instances())[owner.instanceid()];
        observed.set_runtimeid(owner.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(owner.runtimeid());
        sample->set_sandbox_id("recovery-sandbox");
        sample->set_resource_generation("recovery-generation");
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(maxBytes);
        sample->set_memory_limit_known(true);
        sample->set_memory_usage_known(true);
        sample->set_memory_current_bytes(currentBytes);
        sample->set_resource_revision(revision);
        sample->set_resource_revision_known(revisionKnown);
        sample->set_memory_high_bytes(HighBytes(maxBytes));
        sample->set_memory_high_known(true);
        sample->set_memory_high_unlimited(false);
        sample->set_frozen(false);
        sample->set_frozen_known(true);
        sample->set_resource_operation_id("op-prior-grow");
        sample->set_resource_operation_pending(false);
        return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
    };
    auto probeStatus = [&](const std::string &name, double memoryMb) -> Status {
        auto instance = Get1DInstance();
        instance.set_instanceid(name);
        instance.set_requestid("request-" + name);
        instance.set_unitid(unit.id());
        instance.set_runtimeid("runtime-" + name);
        instance.mutable_schedulerchain()->Clear();
        *instance.mutable_schedulerchain()->Add() = unit.id();
        (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
        auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
        cpu->set_value(500);
        cpu->set_limit(8000);
        auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
        memory->set_value(memoryMb);
        memory->set_limit(8192);
        auto future = allocate(instance);
        if (!future.WaitFor(TEST_AWAIT_TIMEOUT).IsOK()) {
            return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "probe future did not resolve");
        }
        return future.Get();
    };

    // Revision-less telemetry must not clear admission or seed state.
    auto physical = publish(0, false, MiB(512), MiB(269));
    ASSERT_AWAIT_READY(physical);
    auto current = view->GetResourceView();
    ASSERT_AWAIT_READY(current);
    EXPECT_TRUE(current.Get()->instances().at(owner.instanceid()).resource_admission_pending());
    EXPECT_FALSE(current.Get()->instances().at(owner.instanceid()).has_sandbox_resource_sample());

    // The authoritative post-restart reading clears admission and holds the
    // full current max: 512 - 269 = 243MiB stay committed.
    physical = publish(5, true, MiB(512), MiB(269));
    ASSERT_AWAIT_READY(physical);
    current = view->GetResourceView();
    ASSERT_AWAIT_READY(current);
    EXPECT_FALSE(current.Get()->instances().at(owner.instanceid()).resource_admission_pending());
    EXPECT_EQ(current.Get()->instances().at(owner.instanceid()).sandbox_resource_sample().resource_revision(), 5);
    const auto heldRejected = probeStatus("probe-held", 700);
    EXPECT_FALSE(heldRejected.IsOk()) << heldRejected.ToString(); // 2800 + 243 held
    const auto heldAdmitted = probeStatus("probe-held-fit", 600);
    EXPECT_TRUE(heldAdmitted.IsOk()) << heldAdmitted.ToString();
    auto removed = view->DeleteInstances({"probe-held-fit"});
    ASSERT_AWAIT_READY(removed);

    // A confirmed shrink trims the baseline cap to 256 and drops the hold.
    messages::ResizeSandboxRequest shrink;
    shrink.set_requestid("recovery-shrink");
    shrink.set_instanceid(owner.instanceid());
    shrink.set_runtimeid(owner.runtimeid());
    shrink.set_shrink(true);
    auto *resize = shrink.mutable_resize();
    resize->set_sandbox_id("recovery-sandbox");
    resize->set_resource_generation("recovery-generation");
    resize->set_operation_id("op-recovery-shrink");
    resize->set_expected_revision(5);
    resize->set_deadline_unix_ms(nowMs() + 30000);
    resize->mutable_expected()->set_cpu_quota_usec(50000);
    resize->mutable_expected()->set_cpu_period_usec(100000);
    resize->mutable_expected()->set_memory_limit_bytes(MiB(512));
    resize->mutable_target()->set_cpu_quota_usec(50000);
    resize->mutable_target()->set_cpu_period_usec(100000);
    resize->mutable_target()->set_memory_limit_bytes(MiB(256));
    auto reserved = view->ReserveResize(shrink);
    ASSERT_AWAIT_READY(reserved);
    EXPECT_TRUE(reserved.Get().IsOk()) << reserved.Get().ToString();
    messages::ResizeSandboxResponse response;
    response.set_requestid(shrink.requestid());
    response.set_instanceid(owner.instanceid());
    response.set_runtimeid(owner.runtimeid());
    response.set_sandboxid("recovery-sandbox");
    response.set_resourcegeneration("recovery-generation");
    response.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
    response.mutable_result()->set_complete(true);
    auto *actual = response.mutable_result()->mutable_actual();
    actual->set_resource_generation("recovery-generation");
    actual->set_cpu_limits_known(true);
    actual->set_cpu_quota_usec(50000);
    actual->set_cpu_period_usec(100000);
    actual->set_memory_limit_bytes(MiB(256));
    actual->set_memory_limit_known(true);
    actual->set_resource_revision(6);
    actual->set_resource_revision_known(true);
    actual->set_resource_operation_id("op-recovery-shrink");
    actual->set_resource_operation_pending(false);
    actual->set_frozen(false);
    actual->set_frozen_known(true);
    actual->set_memory_high_bytes(HighBytes(MiB(256)));
    actual->set_memory_high_known(true);
    auto observed = view->ObserveResize(response);
    ASSERT_AWAIT_READY(observed);
    EXPECT_TRUE(observed.Get().IsOk()) << observed.Get().ToString();
    const auto released = probeStatus("probe-released", 700);
    EXPECT_TRUE(released.IsOk()) << released.ToString(); // 2800 + 0 held
    removed = view->DeleteInstances({"probe-released"});
    ASSERT_AWAIT_READY(removed);

    // Retirement drops the synthetic hold; a recreated allocation rebuilds it.
    // v3: an elastic incarnation that reached the physical layer leaves only
    // through a matching physical-retirement receipt, never a generic delete.
    resource_view::PhysicalRetirementReceipt ownerReceipt;
    ownerReceipt.requestID = owner.requestid();
    ownerReceipt.unitID = unit.id();
    ownerReceipt.instanceID = owner.instanceid();
    ownerReceipt.runtimeID = owner.runtimeid();
    ownerReceipt.sandboxID = "recovery-sandbox";
    ownerReceipt.resourceGeneration = "recovery-generation";
    auto retired = view->ApplyPhysicalRetirement(ownerReceipt);
    ASSERT_AWAIT_READY(retired);
    EXPECT_TRUE(retired.Get().IsOk()) << retired.Get().ToString();
    const auto afterDelete = probeStatus("probe-after-retire", 700);
    EXPECT_TRUE(afterDelete.IsOk()) << afterDelete.ToString();
    removed = view->DeleteInstances({"probe-after-retire"});
    ASSERT_AWAIT_READY(removed);
    owner = makeOwner("request-recovery-owner-2");
    accepted = allocate(owner);
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    physical = publish(7, true, MiB(512), MiB(269));
    ASSERT_AWAIT_READY(physical);
    const auto reheld = probeStatus("probe-reheld", 700);
    EXPECT_FALSE(reheld.IsOk()) << reheld.ToString();
}

TEST_F(ResourceViewTest, ShrinkReserveRejectsContractViolationsBeforeAnyHeadroomMath)
{
    struct FenceGuard {
        FenceGuard() { SetMemoryElasticityFenceForTest(true); }
        ~FenceGuard() { SetMemoryElasticityFenceForTest(false); }
    } fenceGuard;
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    auto view = resource_view::ResourceView::CreateResourceView("shrink-reject", CHILD_PARAM);
    auto unit = Get1DResourceUnit("shrink-reject-agent");
    for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
        (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
        (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(4096);
    }
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
    unit.set_cpu_sample_unix_ms(nowMs());
    unit.set_memory_sample_unix_ms(nowMs());
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    auto instance = Get1DInstance();
    instance.set_instanceid("shrink-reject-instance");
    instance.set_requestid("request-shrink-reject");
    instance.set_unitid(unit.id());
    instance.set_runtimeid("runtime-shrink-reject");
    instance.mutable_schedulerchain()->Clear();
    *instance.mutable_schedulerchain()->Add() = unit.id();
    (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
    cpu->set_value(500);
    cpu->set_limit(8000);
    auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
    memory->set_value(128);
    memory->set_limit(8192);
    auto promise = std::make_shared<litebus::Promise<Status>>();
    (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
    auto accepted = promise->GetFuture();
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    auto publish = [&](bool frozen, bool operationPending) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
        auto &observed = (*report->mutable_instances())[instance.instanceid()];
        observed.set_runtimeid(instance.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(instance.runtimeid());
        sample->set_sandbox_id("reject-sandbox");
        sample->set_resource_generation("reject-generation");
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(MiB(512));
        sample->set_memory_limit_known(true);
        sample->set_memory_usage_known(true);
        sample->set_memory_current_bytes(MiB(64));
        sample->set_resource_revision(2);
        sample->set_resource_revision_known(true);
        sample->set_memory_high_bytes(MiB(410));
        sample->set_memory_high_known(true);
        sample->set_memory_high_unlimited(false);
        sample->set_frozen(frozen);
        sample->set_frozen_known(true);
        sample->set_resource_operation_id("op-previous");
        sample->set_resource_operation_pending(operationPending);
        return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
    };
    auto physical = publish(false, false);
    ASSERT_AWAIT_READY(physical);
    auto buildShrink = [&](const std::string &requestID, const std::string &operationID, uint64_t expectedRevision,
                           uint64_t expectedBytes, uint64_t targetBytes, int64_t targetCpuQuota) {
        messages::ResizeSandboxRequest request;
        request.set_requestid(requestID);
        request.set_instanceid(instance.instanceid());
        request.set_runtimeid(instance.runtimeid());
        request.set_shrink(true);
        auto *resize = request.mutable_resize();
        resize->set_sandbox_id("reject-sandbox");
        resize->set_resource_generation("reject-generation");
        resize->set_operation_id(operationID);
        resize->set_expected_revision(expectedRevision);
        resize->set_deadline_unix_ms(nowMs() + 30000);
        resize->mutable_expected()->set_cpu_quota_usec(50000);
        resize->mutable_expected()->set_cpu_period_usec(100000);
        resize->mutable_expected()->set_memory_limit_bytes(expectedBytes);
        resize->mutable_target()->set_cpu_quota_usec(targetCpuQuota);
        resize->mutable_target()->set_cpu_period_usec(100000);
        resize->mutable_target()->set_memory_limit_bytes(targetBytes);
        return request;
    };
    auto reserveStatus = [&](const messages::ResizeSandboxRequest &request) {
        auto future = view->ReserveResize(request);
        if (!future.WaitFor(TEST_AWAIT_TIMEOUT).IsOK()) {
            return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "reserve future did not resolve");
        }
        return future.Get();
    };
    // CPU must not move in either direction.
    auto refused = reserveStatus(buildShrink("reject-cpu-up", "op-cpu-up", 2, MiB(512), MiB(256), 60000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
    refused = reserveStatus(buildShrink("reject-cpu-down", "op-cpu-down", 2, MiB(512), MiB(256), 40000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
    // Floor gate: 64MiB is below the 128MiB birth request.
    refused = reserveStatus(buildShrink("reject-floor", "op-floor", 2, MiB(512), MiB(64), 50000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
    // A grow disguised as shrink, and a target above the expected limit.
    refused = reserveStatus(buildShrink("reject-increase", "op-increase", 2, MiB(512), MiB(768), 50000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
    // Stale revision (ABA replay) and missing operation identity.
    refused = reserveStatus(buildShrink("reject-stale-revision", "op-stale", 1, MiB(512), MiB(256), 50000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
    refused = reserveStatus(buildShrink("reject-no-op-id", "", 2, MiB(512), MiB(256), 50000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
    // Expected limits must match the confirmed sample.
    refused = reserveStatus(buildShrink("reject-wrong-expected", "op-wrong-expected", 2, MiB(256), MiB(128), 50000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
    // A daemon-side unfinished operation or a frozen sandbox blocks new work.
    physical = publish(false, true);
    ASSERT_AWAIT_READY(physical);
    refused = reserveStatus(buildShrink("reject-daemon-pending", "op-daemon-pending", 2, MiB(512), MiB(256), 50000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
    physical = publish(true, false);
    ASSERT_AWAIT_READY(physical);
    refused = reserveStatus(buildShrink("reject-frozen", "op-frozen", 2, MiB(512), MiB(256), 50000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
    // Shrink is not reachable with the feature disabled.
    SetMemoryElasticityFenceForTest(false);
    refused = reserveStatus(buildShrink("reject-feature-off", "op-feature-off", 2, MiB(512), MiB(256), 50000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
    SetMemoryElasticityFenceForTest(true);
    // The well-formed shrink leases without consuming headroom.
    physical = publish(false, false);
    ASSERT_AWAIT_READY(physical);
    const auto valid = reserveStatus(buildShrink("reject-valid", "op-valid", 2, MiB(512), MiB(256), 50000));
    EXPECT_TRUE(valid.IsOk()) << valid.ToString();
    // And the instance stays single-operation until it settles.
    refused = reserveStatus(buildShrink("reject-second-op", "op-second", 2, MiB(512), MiB(256), 50000));
    EXPECT_FALSE(refused.IsOk()) << refused.ToString();
}

TEST_F(ResourceViewTest, UnknownGrowHoldsSingleOperationUntilContractCloses)
{
    struct FenceGuard {
        FenceGuard() { SetMemoryElasticityFenceForTest(true); }
        ~FenceGuard() { SetMemoryElasticityFenceForTest(false); }
    } fenceGuard;
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    auto view = resource_view::ResourceView::CreateResourceView("unknown-grow", CHILD_PARAM);
    auto unit = Get1DResourceUnit("unknown-grow-agent");
    for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
        (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
        (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(4096);
    }
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
    unit.set_cpu_sample_unix_ms(nowMs());
    unit.set_memory_sample_unix_ms(nowMs());
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    auto instance = Get1DInstance();
    instance.set_instanceid("unknown-grow-instance");
    instance.set_requestid("request-unknown-grow");
    instance.set_unitid(unit.id());
    instance.set_runtimeid("runtime-unknown-grow");
    instance.mutable_schedulerchain()->Clear();
    *instance.mutable_schedulerchain()->Add() = unit.id();
    (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
    cpu->set_value(500);
    cpu->set_limit(8000);
    auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
    memory->set_value(128);
    memory->set_limit(8192);
    auto promise = std::make_shared<litebus::Promise<Status>>();
    (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
    auto accepted = promise->GetFuture();
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    auto publish = [&](uint64_t limitBytes, uint64_t revision, const std::string &operationId, bool operationPending,
                       bool frozen, bool highKnown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
        auto &observed = (*report->mutable_instances())[instance.instanceid()];
        observed.set_runtimeid(instance.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(instance.runtimeid());
        sample->set_sandbox_id("unknown-sandbox");
        sample->set_resource_generation("unknown-generation");
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(limitBytes);
        sample->set_memory_limit_known(true);
        sample->set_memory_usage_known(true);
        sample->set_memory_current_bytes(MiB(100));
        sample->set_resource_revision(revision);
        sample->set_resource_revision_known(true);
        if (highKnown) {
            sample->set_memory_high_bytes(HighBytes(limitBytes));
            sample->set_memory_high_known(true);
            sample->set_memory_high_unlimited(false);
        }
        sample->set_frozen(frozen);
        sample->set_frozen_known(true);
        sample->set_resource_operation_id(operationId);
        sample->set_resource_operation_pending(operationPending);
        return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
    };
    auto buildGrow = [&](const std::string &requestID, const std::string &operationID, uint64_t expectedRevision,
                         uint64_t expectedBytes, uint64_t targetBytes) {
        messages::ResizeSandboxRequest request;
        request.set_requestid(requestID);
        request.set_instanceid(instance.instanceid());
        request.set_runtimeid(instance.runtimeid());
        request.set_shrink(false);
        auto *resize = request.mutable_resize();
        resize->set_sandbox_id("unknown-sandbox");
        resize->set_resource_generation("unknown-generation");
        resize->set_operation_id(operationID);
        resize->set_expected_revision(expectedRevision);
        resize->set_deadline_unix_ms(nowMs() + 30000);
        resize->mutable_expected()->set_cpu_quota_usec(50000);
        resize->mutable_expected()->set_cpu_period_usec(100000);
        resize->mutable_expected()->set_memory_limit_bytes(expectedBytes);
        resize->mutable_target()->set_cpu_quota_usec(50000);
        resize->mutable_target()->set_cpu_period_usec(100000);
        resize->mutable_target()->set_memory_limit_bytes(targetBytes);
        return request;
    };
    auto reserveStatus = [&](const messages::ResizeSandboxRequest &request) {
        auto future = view->ReserveResize(request);
        if (!future.WaitFor(TEST_AWAIT_TIMEOUT).IsOK()) {
            return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "reserve future did not resolve");
        }
        return future.Get();
    };
    auto physical = publish(MiB(128), 1, "", false, false, true);
    ASSERT_AWAIT_READY(physical);
    const auto reserved = reserveStatus(buildGrow("unknown-grow-1", "op-unknown-grow", 1, MiB(128), MiB(256)));
    EXPECT_TRUE(reserved.IsOk()) << reserved.ToString();
    // The instance is single-operation while the outcome is unresolved.
    auto blocked = reserveStatus(buildGrow("unknown-grow-2", "op-unknown-grow-2", 1, MiB(256), MiB(512)));
    EXPECT_FALSE(blocked.IsOk()) << blocked.ToString();
    messages::ResizeSandboxResponse response;
    response.set_requestid("unknown-grow-1");
    response.set_instanceid(instance.instanceid());
    response.set_runtimeid(instance.runtimeid());
    response.set_sandboxid("unknown-sandbox");
    response.set_resourcegeneration("unknown-generation");
    response.set_outcome(messages::ResizeSandboxResponse::UNKNOWN);
    auto observed = view->ObserveResize(response);
    ASSERT_AWAIT_READY(observed);
    blocked = reserveStatus(buildGrow("unknown-grow-2", "op-unknown-grow-2", 1, MiB(256), MiB(512)));
    EXPECT_FALSE(blocked.IsOk()) << blocked.ToString();
    // Numeric target equality at the right revision but the wrong operation,
    // or a partial readback, does not close the UNKNOWN.
    physical = publish(MiB(256), 2, "op-someone-else", false, false, true);
    ASSERT_AWAIT_READY(physical);
    blocked = reserveStatus(buildGrow("unknown-grow-2", "op-unknown-grow-2", 1, MiB(256), MiB(512)));
    EXPECT_FALSE(blocked.IsOk()) << blocked.ToString();
    physical = publish(MiB(256), 2, "op-unknown-grow", false, false, false); // high unknown
    ASSERT_AWAIT_READY(physical);
    blocked = reserveStatus(buildGrow("unknown-grow-2", "op-unknown-grow-2", 1, MiB(256), MiB(512)));
    EXPECT_FALSE(blocked.IsOk()) << blocked.ToString();
    // The matched terminal state closes the operation and frees the slot.
    physical = publish(MiB(256), 2, "op-unknown-grow", false, false, true);
    ASSERT_AWAIT_READY(physical);
    // Refresh the stored reading time so the closure runs on a newer sample.
    physical = publish(MiB(256), 2, "op-unknown-grow", false, false, true);
    ASSERT_AWAIT_READY(physical);
    const auto followUp = reserveStatus(buildGrow("unknown-grow-2", "op-unknown-grow-2", 2, MiB(256), MiB(512)));
    EXPECT_TRUE(followUp.IsOk()) << followUp.ToString();
}

TEST_F(ResourceViewTest, LateStatsCannotRollBackConfirmedRpcReadback)
{
    struct FenceGuard {
        FenceGuard() { SetMemoryElasticityFenceForTest(true); }
        ~FenceGuard() { SetMemoryElasticityFenceForTest(false); }
    } fenceGuard;
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    auto view = resource_view::ResourceView::CreateResourceView("late-stats", CHILD_PARAM);
    auto unit = Get1DResourceUnit("late-stats-agent");
    for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
        (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
        (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(4096);
    }
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
    unit.set_cpu_sample_unix_ms(nowMs());
    unit.set_memory_sample_unix_ms(nowMs());
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    auto instance = Get1DInstance();
    instance.set_instanceid("late-stats-instance");
    instance.set_requestid("request-late-stats");
    instance.set_unitid(unit.id());
    instance.set_runtimeid("runtime-late-stats");
    instance.mutable_schedulerchain()->Clear();
    *instance.mutable_schedulerchain()->Add() = unit.id();
    (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
    cpu->set_value(500);
    cpu->set_limit(8000);
    auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
    memory->set_value(128);
    memory->set_limit(8192);
    auto promise = std::make_shared<litebus::Promise<Status>>();
    (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
    auto accepted = promise->GetFuture();
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    auto publish = [&](uint64_t limitBytes, uint64_t currentBytes, uint64_t revision, bool revisionKnown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
        auto &observed = (*report->mutable_instances())[instance.instanceid()];
        observed.set_runtimeid(instance.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(instance.runtimeid());
        sample->set_sandbox_id("late-sandbox");
        sample->set_resource_generation("late-generation");
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(limitBytes);
        sample->set_memory_limit_known(true);
        sample->set_memory_usage_known(true);
        sample->set_memory_current_bytes(currentBytes);
        sample->set_resource_revision(revision);
        sample->set_resource_revision_known(revisionKnown);
        sample->set_memory_high_bytes(HighBytes(limitBytes));
        sample->set_memory_high_known(true);
        sample->set_memory_high_unlimited(false);
        sample->set_frozen(false);
        sample->set_frozen_known(true);
        sample->set_resource_operation_id(revision == 1 ? "" : "op-late-grow");
        sample->set_resource_operation_pending(false);
        return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
    };
    auto physical = publish(MiB(128), MiB(100), 1, true);
    ASSERT_AWAIT_READY(physical);
    messages::ResizeSandboxRequest request;
    request.set_requestid("late-grow");
    request.set_instanceid(instance.instanceid());
    request.set_runtimeid(instance.runtimeid());
    auto *resize = request.mutable_resize();
    resize->set_sandbox_id("late-sandbox");
    resize->set_resource_generation("late-generation");
    resize->set_operation_id("op-late-grow");
    resize->set_expected_revision(1);
    resize->set_deadline_unix_ms(nowMs() + 30000);
    resize->mutable_expected()->set_cpu_quota_usec(50000);
    resize->mutable_expected()->set_cpu_period_usec(100000);
    resize->mutable_expected()->set_memory_limit_bytes(MiB(128));
    resize->mutable_target()->set_cpu_quota_usec(50000);
    resize->mutable_target()->set_cpu_period_usec(100000);
    resize->mutable_target()->set_memory_limit_bytes(MiB(256));
    auto reserved = view->ReserveResize(request);
    ASSERT_AWAIT_READY(reserved);
    EXPECT_TRUE(reserved.Get().IsOk()) << reserved.Get().ToString();
    const auto reservationFinishedAt = nowMs();
    std::this_thread::sleep_for(std::chrono::milliseconds(3));
    messages::ResizeSandboxResponse response;
    response.set_requestid(request.requestid());
    response.set_instanceid(instance.instanceid());
    response.set_runtimeid(instance.runtimeid());
    response.set_sandboxid("late-sandbox");
    response.set_resourcegeneration("late-generation");
    response.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
    response.mutable_result()->set_complete(true);
    auto *actual = response.mutable_result()->mutable_actual();
    actual->set_resource_generation("late-generation");
    actual->set_cpu_limits_known(true);
    actual->set_cpu_quota_usec(50000);
    actual->set_cpu_period_usec(100000);
    actual->set_memory_limit_bytes(MiB(256));
    actual->set_memory_limit_known(true);
    actual->set_memory_usage_known(true);
    actual->set_memory_usage_bytes(MiB(120));
    actual->set_memory_peak_known(true);
    actual->set_memory_max_usage_bytes(MiB(130));
    actual->set_cpu_usage_known(true);
    actual->set_cpu_usage_ns(123000000);
    actual->set_resource_revision(2);
    actual->set_resource_revision_known(true);
    actual->set_resource_operation_id("op-late-grow");
    actual->set_resource_operation_pending(false);
    actual->set_frozen(false);
    actual->set_frozen_known(true);
    actual->set_memory_high_bytes(HighBytes(MiB(256)));
    actual->set_memory_high_known(true);
    auto observed = view->ObserveResize(response);
    ASSERT_AWAIT_READY(observed);
    EXPECT_TRUE(observed.Get().IsOk()) << observed.Get().ToString();
    // A queued pre-operation Stats reading (revision 1, old limits) arrives
    // with a fresh collection clock; the revision fence keeps the readback.
    physical = publish(MiB(128), MiB(100), 1, true);
    ASSERT_AWAIT_READY(physical);
    auto current = view->GetResourceView();
    ASSERT_AWAIT_READY(current);
    const auto &top = current.Get()->instances().at(instance.instanceid()).sandbox_resource_sample();
    const auto &leaf = current.Get()->fragment().at(unit.id()).instances().at(instance.instanceid())
        .sandbox_resource_sample();
    EXPECT_EQ(top.resource_revision(), 2);
    EXPECT_EQ(top.memory_limit_bytes(), MiB(256));
    EXPECT_EQ(leaf.resource_revision(), 2);
    EXPECT_EQ(leaf.memory_limit_bytes(), MiB(256));
    EXPECT_EQ(top.memory_current_bytes(), MiB(120));
    EXPECT_EQ(leaf.memory_current_bytes(), MiB(120));
    EXPECT_TRUE(top.memory_usage_known());
    EXPECT_EQ(top.memory_peak_bytes(), MiB(130));
    EXPECT_FALSE(top.cpu_usage_known()); // cumulative RPC usage is not a CPU rate
    EXPECT_LE(top.sampled_at_unix_ms(), reservationFinishedAt);
    // Same-revision telemetry keeps flowing normally afterwards.
    physical = publish(MiB(256), MiB(150), 2, true);
    ASSERT_AWAIT_READY(physical);
    current = view->GetResourceView();
    ASSERT_AWAIT_READY(current);
    EXPECT_EQ(current.Get()->instances().at(instance.instanceid()).sandbox_resource_sample()
        .memory_current_bytes(), MiB(150));
}

TEST_F(ResourceViewTest, ShrinkTerminalContractRequiresExactHighAndCoversFailedTerminal)
{
    using schedule_plugin::filter::UsageAwareFilter;
    struct ConfigGuard {
        double safety = UsageAwareFilter::GetSafety();
        double floor = UsageAwareFilter::GetFloorMb();
        int32_t cap = UsageAwareFilter::GetMaxInstances();
        ~ConfigGuard() { UsageAwareFilter::SetConfig(safety, floor, cap); }
    } guard;
    UsageAwareFilter::SetConfig(0.9, 2048, 0);
    struct FenceGuard {
        FenceGuard() { SetMemoryElasticityFenceForTest(true); }
        ~FenceGuard() { SetMemoryElasticityFenceForTest(false); }
    } fenceGuard;
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    auto view = resource_view::ResourceView::CreateResourceView("shrink-terminal", CHILD_PARAM);
    auto unit = Get1DResourceUnit("shrink-terminal-agent");
    for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
        (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
        (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(4096);
    }
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
    unit.set_cpu_sample_unix_ms(nowMs());
    unit.set_memory_sample_unix_ms(nowMs());
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    auto instance = Get1DInstance();
    instance.set_instanceid("terminal-instance");
    instance.set_requestid("request-terminal-instance");
    instance.set_unitid(unit.id());
    instance.set_runtimeid("runtime-terminal-instance");
    instance.mutable_schedulerchain()->Clear();
    *instance.mutable_schedulerchain()->Add() = unit.id();
    (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
    cpu->set_value(500);
    cpu->set_limit(8000);
    auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
    memory->set_value(128);
    memory->set_limit(8192);
    auto promise = std::make_shared<litebus::Promise<Status>>();
    (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
    auto accepted = promise->GetFuture();
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    auto publish = [&](uint64_t maxBytes, uint64_t currentBytes, uint64_t revision) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
        auto &observed = (*report->mutable_instances())[instance.instanceid()];
        observed.set_runtimeid(instance.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(instance.runtimeid());
        sample->set_sandbox_id("terminal-sandbox");
        sample->set_resource_generation("terminal-generation");
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(maxBytes);
        sample->set_memory_limit_known(true);
        sample->set_memory_usage_known(true);
        sample->set_memory_current_bytes(currentBytes);
        sample->set_resource_revision(revision);
        sample->set_resource_revision_known(true);
        sample->set_memory_high_bytes(HighBytes(maxBytes));
        sample->set_memory_high_known(true);
        sample->set_memory_high_unlimited(false);
        sample->set_frozen(false);
        sample->set_frozen_known(true);
        sample->set_resource_operation_id("");
        sample->set_resource_operation_pending(false);
        return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
    };
    auto buildRequest = [&](const std::string &requestID, const std::string &operationID, uint64_t expectedRevision,
                            uint64_t expectedBytes, uint64_t targetBytes, bool shrink) {
        messages::ResizeSandboxRequest request;
        request.set_requestid(requestID);
        request.set_instanceid(instance.instanceid());
        request.set_runtimeid(instance.runtimeid());
        request.set_shrink(shrink);
        auto *resize = request.mutable_resize();
        resize->set_sandbox_id("terminal-sandbox");
        resize->set_resource_generation("terminal-generation");
        resize->set_operation_id(operationID);
        resize->set_expected_revision(expectedRevision);
        resize->set_deadline_unix_ms(nowMs() + 30000);
        resize->mutable_expected()->set_cpu_quota_usec(50000);
        resize->mutable_expected()->set_cpu_period_usec(100000);
        resize->mutable_expected()->set_memory_limit_bytes(expectedBytes);
        resize->mutable_target()->set_cpu_quota_usec(50000);
        resize->mutable_target()->set_cpu_period_usec(100000);
        resize->mutable_target()->set_memory_limit_bytes(targetBytes);
        return request;
    };
    auto reserveStatus = [&](const messages::ResizeSandboxRequest &request) {
        auto future = view->ReserveResize(request);
        if (!future.WaitFor(TEST_AWAIT_TIMEOUT).IsOK()) {
            return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "reserve future did not resolve");
        }
        return future.Get();
    };
    auto observeStatus = [&](const std::string &requestID, const messages::ResizeSandboxRequest &request,
                             uint64_t revision, uint64_t actualBytes, uint64_t highBytes, bool complete) {
        messages::ResizeSandboxResponse response;
        response.set_requestid(requestID);
        response.set_instanceid(request.instanceid());
        response.set_runtimeid(request.runtimeid());
        response.set_sandboxid(request.resize().sandbox_id());
        response.set_resourcegeneration(request.resize().resource_generation());
        response.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
        response.mutable_result()->set_complete(complete);
        auto *actual = response.mutable_result()->mutable_actual();
        actual->set_resource_generation(request.resize().resource_generation());
        actual->set_cpu_limits_known(true);
        actual->set_cpu_quota_usec(50000);
        actual->set_cpu_period_usec(100000);
        actual->set_memory_limit_bytes(actualBytes);
        actual->set_memory_limit_known(true);
        actual->set_resource_revision(revision);
        actual->set_resource_revision_known(true);
        actual->set_resource_operation_id(request.resize().operation_id());
        actual->set_resource_operation_pending(false);
        actual->set_frozen(false);
        actual->set_frozen_known(true);
        actual->set_memory_high_bytes(highBytes);
        actual->set_memory_high_known(true);
        auto future = view->ObserveResize(response);
        if (!future.WaitFor(TEST_AWAIT_TIMEOUT).IsOK()) {
            return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "observe future did not resolve");
        }
        return future.Get();
    };
    auto probeRejected = [&](const std::string &name, double memoryMb) {
        auto probe = Get1DInstance();
        probe.set_instanceid(name);
        probe.set_requestid("request-" + name);
        probe.set_unitid(unit.id());
        probe.set_runtimeid("runtime-" + name);
        probe.mutable_schedulerchain()->Clear();
        *probe.mutable_schedulerchain()->Add() = unit.id();
        (*probe.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
        auto *probeCpu = (*probe.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
        probeCpu->set_value(500);
        probeCpu->set_limit(8000);
        auto *probeMem = (*probe.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
        probeMem->set_value(memoryMb);
        probeMem->set_limit(8192);
        auto probePromise = std::make_shared<litebus::Promise<Status>>();
        (void)view->AddInstances({{probe.instanceid(), resource_view::InstanceAllocatedInfo{probe, probePromise}}});
        auto future = probePromise->GetFuture();
        ASSERT_TRUE(future.WaitFor(TEST_AWAIT_TIMEOUT).IsOK());
        EXPECT_FALSE(future.Get().IsOk()) << "probe " << name << " must be rejected: " << future.Get().ToString();
    };

    auto physical = publish(MiB(128), MiB(100), 1);
    ASSERT_AWAIT_READY(physical);
    // One settled grow entry holds 128MiB of credit for the whole test.
    auto grow = buildRequest("terminal-grow-1", "op-terminal-grow-1", 1, MiB(128), MiB(256), false);
    EXPECT_TRUE(reserveStatus(grow).IsOk());
    EXPECT_TRUE(observeStatus(grow.requestid(), grow, 2, MiB(256), HighBytes(MiB(256)), true).IsOk());
    probeRejected("terminal-held", 800); // 2800 + 128 held

    // A complete readback whose high misses the fixed 0.8 page-aligned value
    // does not close the shrink, even with every other field matching.
    auto shrink = buildRequest("terminal-shrink-1", "op-terminal-shrink-1", 2, MiB(256), MiB(128), true);
    EXPECT_TRUE(reserveStatus(shrink).IsOk());
    const auto wrongHigh = observeStatus(shrink.requestid(), shrink, 3, MiB(128), HighBytes(MiB(128)) + 4096, true);
    EXPECT_FALSE(wrongHigh.IsOk()) << wrongHigh.ToString();
    probeRejected("terminal-wrong-high", 800);

    // A shrink that provably terminated with the original limits restored
    // closes its slot and adopts the new revision, yet returns no grow credit.
    const auto failedTerminal = observeStatus(shrink.requestid(), shrink, 3, MiB(256), HighBytes(MiB(256)), false);
    EXPECT_TRUE(failedTerminal.IsOk()) << failedTerminal.ToString();
    probeRejected("terminal-failed-terminal", 800); // still 2800 + 128 held

    // The slot is free again: a retry at the new revision is accepted, and its
    // confirmed completion finally trims and releases the grow credit.
    auto retry = buildRequest("terminal-shrink-2", "op-terminal-shrink-2", 3, MiB(256), MiB(128), true);
    EXPECT_TRUE(reserveStatus(retry).IsOk());
    EXPECT_TRUE(observeStatus(retry.requestid(), retry, 4, MiB(128), HighBytes(MiB(128)), true).IsOk());
    auto admitted = Get1DInstance();
    admitted.set_instanceid("terminal-released");
    admitted.set_requestid("request-terminal-released");
    admitted.set_unitid(unit.id());
    admitted.set_runtimeid("runtime-terminal-released");
    admitted.mutable_schedulerchain()->Clear();
    *admitted.mutable_schedulerchain()->Add() = unit.id();
    (*admitted.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    auto *admittedCpu = (*admitted.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
    admittedCpu->set_value(500);
    admittedCpu->set_limit(8000);
    auto *admittedMem = (*admitted.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
    admittedMem->set_value(800);
    admittedMem->set_limit(8192);
    auto admittedPromise = std::make_shared<litebus::Promise<Status>>();
    (void)view->AddInstances(
        {{admitted.instanceid(), resource_view::InstanceAllocatedInfo{admitted, admittedPromise}}});
    auto admittedFuture = admittedPromise->GetFuture();
    ASSERT_AWAIT_READY(admittedFuture);
    EXPECT_TRUE(admittedFuture.Get().IsOk()) << admittedFuture.Get().ToString(); // 2800 + 0 held
    auto removed = view->DeleteInstances({"terminal-released"});
    ASSERT_AWAIT_READY(removed);

    // A surviving trimmed entry (landed above the current limit) must not
    // block the next shrink at the confirmed revision.
    auto regrow = buildRequest("terminal-grow-3", "op-terminal-grow-3", 4, MiB(128), MiB(512), false);
    EXPECT_TRUE(reserveStatus(regrow).IsOk());
    EXPECT_TRUE(observeStatus(regrow.requestid(), regrow, 5, MiB(512), HighBytes(MiB(512)), true).IsOk());
    auto partial = buildRequest("terminal-shrink-3", "op-terminal-shrink-3", 5, MiB(512), MiB(384), true);
    EXPECT_TRUE(reserveStatus(partial).IsOk());
    EXPECT_TRUE(observeStatus(partial.requestid(), partial, 6, MiB(384), HighBytes(MiB(384)), true).IsOk());
    auto followUp = buildRequest("terminal-shrink-4", "op-terminal-shrink-4", 6, MiB(384), MiB(256), true);
    const auto followUpStatus = reserveStatus(followUp);
    EXPECT_TRUE(followUpStatus.IsOk()) << followUpStatus.ToString(); // landed 512 > expected 384 must not block
}

TEST_F(ResourceViewTest, UnitMutationGateBlocksUntilEveryManagedInstanceReconciled)
{
    struct FenceGuard {
        FenceGuard() { SetMemoryElasticityFenceForTest(true); }
        ~FenceGuard() { SetMemoryElasticityFenceForTest(false); }
    } fenceGuard;
    auto nowMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(); };
    auto view = resource_view::ResourceView::CreateResourceView("mutation-gate", CHILD_PARAM);
    auto unit = Get1DResourceUnit("mutation-gate-agent");
    for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
        (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
        (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(4096);
    }
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
    (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
    unit.set_cpu_sample_unix_ms(nowMs());
    unit.set_memory_sample_unix_ms(nowMs());
    auto added = view->AddResourceUnit(unit);
    ASSERT_AWAIT_READY(added);
    auto makeInstance = [&](const std::string &name, double memoryMb) {
        auto instance = Get1DInstance();
        instance.set_instanceid(name);
        instance.set_requestid("request-" + name);
        instance.set_unitid(unit.id());
        instance.set_runtimeid("runtime-" + name);
        instance.mutable_schedulerchain()->Clear();
        *instance.mutable_schedulerchain()->Add() = unit.id();
        (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
        auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
        cpu->set_value(500);
        cpu->set_limit(8000);
        auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
        memory->set_value(memoryMb);
        memory->set_limit(8192);
        return instance;
    };
    auto allocate = [&](const resources::InstanceInfo &instance) {
        auto promise = std::make_shared<litebus::Promise<Status>>();
        (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
        return promise->GetFuture();
    };
    auto owner = makeInstance("gate-owner", 128);
    auto cold = makeInstance("gate-cold", 256);
    auto accepted = allocate(owner);
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    accepted = allocate(cold);
    ASSERT_AWAIT_READY(accepted);
    ASSERT_TRUE(accepted.Get().IsOk()) << accepted.Get().ToString();
    auto publishCold = [&](bool operationPending) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
        auto &observed = (*report->mutable_instances())[cold.instanceid()];
        observed.set_runtimeid(cold.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(cold.runtimeid());
        sample->set_sandbox_id("gate-cold-sandbox");
        sample->set_resource_generation("gate-cold-generation");
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(MiB(256));
        sample->set_memory_limit_known(true);
        sample->set_memory_usage_known(true);
        sample->set_memory_current_bytes(MiB(64));
        sample->set_resource_revision(1);
        sample->set_resource_revision_known(true);
        sample->set_memory_high_bytes(HighBytes(MiB(256)));
        sample->set_memory_high_known(true);
        sample->set_memory_high_unlimited(false);
        sample->set_frozen(false);
        sample->set_frozen_known(true);
        sample->set_resource_operation_id(operationPending ? "op-foreign" : "");
        sample->set_resource_operation_pending(operationPending);
        return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
    };
    auto publishOwner = [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(nowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(2800);
        auto &observed = (*report->mutable_instances())[owner.instanceid()];
        observed.set_runtimeid(owner.runtimeid());
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(owner.runtimeid());
        sample->set_sandbox_id("gate-owner-sandbox");
        sample->set_resource_generation("gate-owner-generation");
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(MiB(128));
        sample->set_memory_limit_known(true);
        sample->set_memory_usage_known(true);
        sample->set_memory_current_bytes(MiB(64));
        sample->set_resource_revision(2);
        sample->set_resource_revision_known(true);
        sample->set_memory_high_bytes(HighBytes(MiB(128)));
        sample->set_memory_high_known(true);
        sample->set_memory_high_unlimited(false);
        sample->set_frozen(false);
        sample->set_frozen_known(true);
        sample->set_resource_operation_id("");
        sample->set_resource_operation_pending(false);
        return view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
    };
    auto reserveGrow = [&](const std::string &requestID) {
        messages::ResizeSandboxRequest request;
        request.set_requestid(requestID);
        request.set_instanceid(owner.instanceid());
        request.set_runtimeid(owner.runtimeid());
        auto *resize = request.mutable_resize();
        resize->set_sandbox_id("gate-owner-sandbox");
        resize->set_resource_generation("gate-owner-generation");
        resize->set_operation_id("op-" + requestID);
        resize->set_expected_revision(2);
        resize->set_deadline_unix_ms(nowMs() + 30000);
        resize->mutable_expected()->set_cpu_quota_usec(50000);
        resize->mutable_expected()->set_cpu_period_usec(100000);
        resize->mutable_expected()->set_memory_limit_bytes(MiB(128));
        resize->mutable_target()->set_cpu_quota_usec(50000);
        resize->mutable_target()->set_cpu_period_usec(100000);
        resize->mutable_target()->set_memory_limit_bytes(MiB(256));
        auto future = view->ReserveResize(request);
        if (!future.WaitFor(TEST_AWAIT_TIMEOUT).IsOK()) {
            return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "reserve future did not resolve");
        }
        return future.Get();
    };

    // The reconciled owner alone is not enough: a managed instance that never
    // produced a revision-known sample keeps the whole unit closed.
    auto physical = publishOwner();
    ASSERT_AWAIT_READY(physical);
    auto blocked = reserveGrow("gate-blocked-cold");
    EXPECT_FALSE(blocked.IsOk()) << blocked.ToString();
    // The birth sample of the new instance reopens the unit.
    physical = publishCold(false);
    ASSERT_AWAIT_READY(physical);
    auto admitted = reserveGrow("gate-open");
    EXPECT_TRUE(admitted.IsOk()) << admitted.ToString();
    auto cancelled = view->CancelResizeBeforeDispatch("gate-open");
    ASSERT_AWAIT_READY(cancelled);
    // A daemon-side pending operation on any managed instance closes it again.
    physical = publishCold(true);
    ASSERT_AWAIT_READY(physical);
    blocked = reserveGrow("gate-blocked-pending");
    EXPECT_FALSE(blocked.IsOk()) << blocked.ToString();
    physical = publishCold(false);
    ASSERT_AWAIT_READY(physical);
    admitted = reserveGrow("gate-reopen");
    EXPECT_TRUE(admitted.IsOk()) << admitted.ToString();
    cancelled = view->CancelResizeBeforeDispatch("gate-reopen");
    ASSERT_AWAIT_READY(cancelled);
}

// ── Physical retirement settlement (v3 resizeBudget clearing) ────────────────

// Shared fixture for receipt tests: one unit, N elastic owners with publishable
// revision-known physical samples; a published max above the birth floor holds
// a recovery-baseline credit that only a matching retirement receipt releases.
class PhysicalRetirementTest : public ResourceViewTest {
protected:
    struct Guards {
        using UsageAwareFilter = schedule_plugin::filter::UsageAwareFilter;
        double safety = UsageAwareFilter::GetSafety();
        double floor = UsageAwareFilter::GetFloorMb();
        int32_t cap = UsageAwareFilter::GetMaxInstances();
        bool fence = false;
        ~Guards()
        {
            UsageAwareFilter::SetConfig(safety, floor, cap);
            SetMemoryElasticityFenceForTest(fence);
        }
    };
    static int64_t NowMs()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
    struct Owner {
        std::string instanceID;
        std::string requestID;
        std::string runtimeID;
        std::string sandboxID;
        std::string generation;
    };
    // Builds the view/unit and allocates the given elastic owners at a 128 MiB
    // birth floor with an 8 GiB ceiling.
    void Setup(const std::string &name, const std::vector<Owner> &owners, bool fence)
    {
        SetMemoryElasticityFenceForTest(fence);
        schedule_plugin::filter::UsageAwareFilter::SetConfig(0.9, 2048, 0);
        view = resource_view::ResourceView::CreateResourceView(name, CHILD_PARAM);
        unit = Get1DResourceUnit(name + "-agent");
        for (auto *resources : {unit.mutable_capacity(), unit.mutable_allocatable()}) {
            (*resources->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(32000);
            (*resources->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(32768);
        }
        (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*unit.mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(512);
        unit.set_cpu_sample_unix_ms(NowMs());
        unit.set_memory_sample_unix_ms(NowMs());
        auto added = view->AddResourceUnit(unit);
        ASSERT_AWAIT_READY(added);
        for (const auto &owner : owners) {
            auto instance = Get1DInstance();
            instance.set_instanceid(owner.instanceID);
            instance.set_requestid(owner.requestID);
            instance.set_unitid(unit.id());
            instance.set_runtimeid(owner.runtimeID);
            instance.mutable_schedulerchain()->Clear();
            *instance.mutable_schedulerchain()->Add() = unit.id();
            (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
            auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
            cpu->set_value(500);
            cpu->set_limit(8000);
            auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
            memory->set_value(128);
            memory->set_limit(8192);
            auto promise = std::make_shared<litebus::Promise<Status>>();
            (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
            auto future = promise->GetFuture();
            ASSERT_AWAIT_READY(future);
            ASSERT_TRUE(future.Get().IsOk()) << future.Get().ToString();
        }
    }
    // Publishes a revision-known sample; a max above the floor creates the
    // recovery baseline credit (max - current) held under the exact identity.
    void Publish(const Owner &owner, uint64_t maxBytes, uint64_t currentBytes, uint64_t revision = 5,
                 const std::string &operationId = "op-prior", bool pending = false)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        auto report = std::make_shared<ResourceUnit>(unit);
        report->set_memory_sample_unix_ms(NowMs());
        report->set_cpu_sample_unix_ms(report->memory_sample_unix_ms());
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar()->set_value(1000);
        (*report->mutable_actualuse()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar()->set_value(512);
        auto &observed = (*report->mutable_instances())[owner.instanceID];
        observed.set_runtimeid(owner.runtimeID);
        auto *sample = observed.mutable_sandbox_resource_sample();
        sample->set_runtime_id(owner.runtimeID);
        sample->set_sandbox_id(owner.sandboxID);
        sample->set_resource_generation(owner.generation);
        sample->set_sampled_at_unix_ms(report->memory_sample_unix_ms());
        sample->set_cpu_usage_known(true);
        sample->set_cpu_usage_cores(0.05);
        sample->set_cpu_limits_known(true);
        sample->set_cpu_period_usec(100000);
        sample->set_cpu_quota_usec(50000);
        sample->set_memory_limit_bytes(maxBytes);
        sample->set_memory_limit_known(true);
        sample->set_memory_usage_known(true);
        sample->set_memory_current_bytes(currentBytes);
        sample->set_resource_revision(revision);
        sample->set_resource_revision_known(true);
        sample->set_memory_high_bytes(HighBytes(maxBytes));
        sample->set_memory_high_known(true);
        sample->set_memory_high_unlimited(false);
        sample->set_frozen(false);
        sample->set_frozen_known(true);
        sample->set_resource_operation_id(operationId);
        sample->set_resource_operation_pending(pending);
        auto updated = view->UpdateResourceUnit(report, UpdateType::UPDATE_ACTUAL);
        ASSERT_AWAIT_READY(updated);
    }
    static resource_view::PhysicalRetirementReceipt ReceiptFor(const Owner &owner, const std::string &unitID,
                                                               const std::string &requestID = "receipt-audit")
    {
        resource_view::PhysicalRetirementReceipt receipt;
        receipt.requestID = requestID;
        receipt.unitID = unitID;
        receipt.instanceID = owner.instanceID;
        receipt.runtimeID = owner.runtimeID;
        receipt.sandboxID = owner.sandboxID;
        receipt.resourceGeneration = owner.generation;
        return receipt;
    }
    bool Allocated(const std::string &instanceID)
    {
        auto current = view->GetResourceView();
        EXPECT_AWAIT_READY(current);
        return current.Get()->instances().count(instanceID) == 1;
    }
    std::shared_ptr<resource_view::ResourceView> view;
    ResourceUnit unit;
};

TEST_F(PhysicalRetirementTest, SettlesOnlyTheReceiptIdentityAndKeepsNeighbours)
{
    Guards guards;
    const Owner a{"retire-a", "request-a", "runtime-a", "sandbox-a", "generation-a"};
    const Owner b{"retire-b", "request-b", "runtime-b", "sandbox-b", "generation-b"};
    Setup("retire-exact", {a, b}, true);
    Publish(a, MiB(512), MiB(269));  // holds 512-269 = 243 MiB
    Publish(b, MiB(384), MiB(269));  // holds 384-269 = 115 MiB
    const auto before = view->GetOutstandingResizeForTest(unit.id());
    EXPECT_NEAR(before.memoryMb, 243.0 + 115.0, 0.01);
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{2});
    // Mismatching receipts must clear nothing (each flips one identity field).
    resource_view::PhysicalRetirementReceipt drift = ReceiptFor(a, unit.id());
    drift.sandboxID = "sandbox-b";
    auto settled = view->ApplyPhysicalRetirement(drift);
    ASSERT_AWAIT_READY(settled);
    EXPECT_TRUE(settled.Get().IsOk());
    drift = ReceiptFor(a, unit.id());
    drift.resourceGeneration = "generation-b";
    settled = view->ApplyPhysicalRetirement(drift);
    ASSERT_AWAIT_READY(settled);
    drift = ReceiptFor(a, unit.id());
    drift.runtimeID = "runtime-b";
    settled = view->ApplyPhysicalRetirement(drift);
    ASSERT_AWAIT_READY(settled);
    drift = ReceiptFor(a, unit.id());
    drift.unitID = "some-other-unit";
    settled = view->ApplyPhysicalRetirement(drift);
    ASSERT_AWAIT_READY(settled);
    drift = ReceiptFor(a, unit.id());
    drift.instanceID = "retire-b";
    settled = view->ApplyPhysicalRetirement(drift);
    ASSERT_AWAIT_READY(settled);
    EXPECT_NEAR(view->GetOutstandingResizeForTest(unit.id()).memoryMb, before.memoryMb, 0.01);
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{2});
    EXPECT_TRUE(Allocated(a.instanceID));
    EXPECT_TRUE(Allocated(b.instanceID));
    // Incomplete identity is rejected outright.
    resource_view::PhysicalRetirementReceipt empty;
    auto rejected = view->ApplyPhysicalRetirement(empty);
    ASSERT_AWAIT_READY(rejected);
    EXPECT_FALSE(rejected.Get().IsOk());
    // The exact receipt clears A only: 243 MiB released, B untouched.
    auto exact = view->ApplyPhysicalRetirement(ReceiptFor(a, unit.id()));
    ASSERT_AWAIT_READY(exact);
    EXPECT_TRUE(exact.Get().IsOk());
    const auto after = view->GetOutstandingResizeForTest(unit.id());
    EXPECT_NEAR(after.memoryMb, 115.0, 0.01);
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{1});
    EXPECT_FALSE(Allocated(a.instanceID));
    EXPECT_TRUE(Allocated(b.instanceID));
    // Replay of the same receipt is idempotent: nothing further released.
    auto replay = view->ApplyPhysicalRetirement(ReceiptFor(a, unit.id(), "receipt-audit-2"));
    ASSERT_AWAIT_READY(replay);
    EXPECT_TRUE(replay.Get().IsOk());
    EXPECT_NEAR(view->GetOutstandingResizeForTest(unit.id()).memoryMb, 115.0, 0.01);
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{1});
    // Retiring B empties the ledger exactly.
    auto final = view->ApplyPhysicalRetirement(ReceiptFor(b, unit.id()));
    ASSERT_AWAIT_READY(final);
    EXPECT_TRUE(final.Get().IsOk());
    EXPECT_NEAR(view->GetOutstandingResizeForTest(unit.id()).memoryMb, 0.0, 0.001);
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{0});
    EXPECT_FALSE(Allocated(b.instanceID));
}

TEST_F(PhysicalRetirementTest, GenericDeletionRequiresReceiptForPhysicalElastic)
{
    Guards guards;
    const Owner a{"guard-a", "request-guard-a", "runtime-a", "sandbox-a", "generation-a"};
    const Owner cold{"guard-cold", "request-guard-cold", "runtime-cold", "sandbox-cold", "generation-cold"};
    Setup("retire-guard", {a, cold}, true);
    Publish(a, MiB(512), MiB(269));
    // Elastic with a physical sample: generic deletion fails closed.
    auto refused = view->DeleteInstances({a.instanceID});
    ASSERT_AWAIT_READY(refused);
    EXPECT_FALSE(refused.Get().IsOk());
    EXPECT_TRUE(Allocated(a.instanceID));
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{1});
    // An elastic allocation that never reached the physical layer rolls back.
    auto coldDeleted = view->DeleteInstances({cold.instanceID});
    ASSERT_AWAIT_READY(coldDeleted);
    EXPECT_TRUE(coldDeleted.Get().IsOk());
    EXPECT_FALSE(Allocated(cold.instanceID));
    // After the receipt settles, the allocation is already gone.
    auto settled = view->ApplyPhysicalRetirement(ReceiptFor(a, unit.id()));
    ASSERT_AWAIT_READY(settled);
    EXPECT_TRUE(settled.Get().IsOk());
    EXPECT_FALSE(Allocated(a.instanceID));
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{0});
}

TEST_F(PhysicalRetirementTest, OldReceiptNeverTouchesReplacementGeneration)
{
    Guards guards;
    const Owner first{"aba-instance", "request-aba-1", "runtime-1", "sandbox-1", "generation-1"};
    Setup("retire-aba", {first}, true);
    Publish(first, MiB(512), MiB(269));
    EXPECT_NEAR(view->GetOutstandingResizeForTest(unit.id()).memoryMb, 243.0, 0.01);
    auto settled = view->ApplyPhysicalRetirement(ReceiptFor(first, unit.id()));
    ASSERT_AWAIT_READY(settled);
    EXPECT_TRUE(settled.Get().IsOk());
    // The logical ID returns as a replacement incarnation.
    const Owner second{"aba-instance", "request-aba-2", "runtime-2", "sandbox-2", "generation-2"};
    auto instance = Get1DInstance();
    instance.set_instanceid(second.instanceID);
    instance.set_requestid(second.requestID);
    instance.set_unitid(unit.id());
    instance.set_runtimeid(second.runtimeID);
    instance.mutable_schedulerchain()->Clear();
    *instance.mutable_schedulerchain()->Add() = unit.id();
    (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    auto *memory = (*instance.mutable_resources()->mutable_resources())[RESOURCE_MEM_NAME].mutable_scalar();
    memory->set_value(128);
    memory->set_limit(8192);
    auto *cpu = (*instance.mutable_resources()->mutable_resources())[RESOURCE_CPU_NAME].mutable_scalar();
    cpu->set_value(500);
    cpu->set_limit(8000);
    auto promise = std::make_shared<litebus::Promise<Status>>();
    (void)view->AddInstances({{instance.instanceid(), resource_view::InstanceAllocatedInfo{instance, promise}}});
    auto allocated = promise->GetFuture();
    ASSERT_AWAIT_READY(allocated);
    ASSERT_TRUE(allocated.Get().IsOk()) << allocated.Get().ToString();
    Publish(second, MiB(640), MiB(269));  // replacement holds 371 MiB
    EXPECT_NEAR(view->GetOutstandingResizeForTest(unit.id()).memoryMb, 371.0, 0.01);
    // The stale generation-1 receipt replay must clear nothing and keep the
    // replacement allocation and its credit.
    auto staleReplay = view->ApplyPhysicalRetirement(ReceiptFor(first, unit.id()));
    ASSERT_AWAIT_READY(staleReplay);
    EXPECT_TRUE(staleReplay.Get().IsOk());
    EXPECT_NEAR(view->GetOutstandingResizeForTest(unit.id()).memoryMb, 371.0, 0.01);
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{1});
    EXPECT_TRUE(Allocated(second.instanceID));
    // The replacement's own receipt settles it in full.
    auto secondSettled = view->ApplyPhysicalRetirement(ReceiptFor(second, unit.id()));
    ASSERT_AWAIT_READY(secondSettled);
    EXPECT_TRUE(secondSettled.Get().IsOk());
    EXPECT_NEAR(view->GetOutstandingResizeForTest(unit.id()).memoryMb, 0.0, 0.001);
    EXPECT_FALSE(Allocated(second.instanceID));
}

TEST_F(PhysicalRetirementTest, FailedRebindKeepsPriorIncarnationLedger)
{
    Guards guards;
    const Owner a{"rebind-instance", "request-rebind", "runtime-1", "sandbox-1", "generation-1"};
    Setup("retire-rebind", {a}, true);
    Publish(a, MiB(512), MiB(269));
    const size_t entriesBefore = view->GetResizeBudgetEntryCountForTest();
    ASSERT_EQ(entriesBefore, size_t{1});
    // A rebind onto a different runtime is refused as a stale incarnation; it
    // must not drop the prior incarnation's ledger hold on its way out.
    auto rebound = Get1DInstance();
    rebound.set_instanceid(a.instanceID);
    rebound.set_requestid(a.requestID);
    rebound.set_unitid(unit.id());
    rebound.set_runtimeid("runtime-2");
    auto bind = view->BindInstanceRuntime(rebound, "runtime-2");
    ASSERT_AWAIT_READY(bind);
    EXPECT_FALSE(bind.Get().IsOk());
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), entriesBefore);
    EXPECT_NEAR(view->GetOutstandingResizeForTest(unit.id()).memoryMb, 243.0, 0.01);
    // Only the receipt releases the hold; after settlement the allocation is
    // gone and a fresh deployment binds cleanly.
    auto settled = view->ApplyPhysicalRetirement(ReceiptFor(a, unit.id()));
    ASSERT_AWAIT_READY(settled);
    EXPECT_TRUE(settled.Get().IsOk());
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{0});
}

TEST_F(PhysicalRetirementTest, SettledPendingReservationCannotBeResurrected)
{
    Guards guards;
    const Owner a{"resurrect-instance", "request-resurrect", "runtime-a", "sandbox-a", "generation-a"};
    Setup("retire-resurrect", {a}, true);
    Publish(a, MiB(256), MiB(269), 5, "op-birth");
    messages::ResizeSandboxRequest reserve;
    reserve.set_requestid("grow-pending");
    reserve.set_instanceid(a.instanceID);
    reserve.set_runtimeid(a.runtimeID);
    auto *resize = reserve.mutable_resize();
    resize->set_sandbox_id(a.sandboxID);
    resize->set_resource_generation(a.generation);
    resize->set_operation_id("op-grow-1");
    resize->set_expected_revision(5);
    resize->set_deadline_unix_ms(NowMs() + 30000);
    resize->mutable_expected()->set_cpu_quota_usec(50000);
    resize->mutable_expected()->set_cpu_period_usec(100000);
    resize->mutable_expected()->set_memory_limit_bytes(MiB(256));
    resize->mutable_target()->set_cpu_quota_usec(50000);
    resize->mutable_target()->set_cpu_period_usec(100000);
    resize->mutable_target()->set_memory_limit_bytes(MiB(384));
    auto reserved = view->ReserveResize(reserve);
    ASSERT_AWAIT_READY(reserved);
    EXPECT_TRUE(reserved.Get().IsOk()) << reserved.Get().ToString();
    // The reservation holds 128 MiB on top of any baseline state.
    const auto held = view->GetOutstandingResizeForTest(unit.id());
    EXPECT_NEAR(held.memoryMb, 128.0, 0.01);
    // Retire the incarnation while the operation is still UNKNOWN.
    auto settled = view->ApplyPhysicalRetirement(ReceiptFor(a, unit.id()));
    ASSERT_AWAIT_READY(settled);
    EXPECT_TRUE(settled.Get().IsOk());
    EXPECT_NEAR(view->GetOutstandingResizeForTest(unit.id()).memoryMb, 0.0, 0.001);
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{0});
    EXPECT_FALSE(Allocated(a.instanceID));
    // A late OBSERVED completion for the settled request cannot resurrect the
    // entry or its credit.
    messages::ResizeSandboxResponse late;
    late.set_requestid("grow-pending");
    late.set_instanceid(a.instanceID);
    late.set_runtimeid(a.runtimeID);
    late.set_sandboxid(a.sandboxID);
    late.set_resourcegeneration(a.generation);
    late.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
    auto *result = late.mutable_result();
    result->set_complete(true);
    auto *actual = result->mutable_actual();
    actual->set_resource_generation(a.generation);
    actual->set_cpu_limits_known(true);
    actual->set_cpu_quota_usec(50000);
    actual->set_cpu_period_usec(100000);
    actual->set_memory_limit_bytes(MiB(384));
    actual->set_memory_limit_known(true);
    actual->set_resource_revision(6);
    actual->set_resource_revision_known(true);
    actual->set_resource_operation_id("op-grow-1");
    actual->set_resource_operation_pending(false);
    actual->set_frozen(false);
    actual->set_frozen_known(true);
    actual->set_memory_high_bytes(HighBytes(MiB(384)));
    actual->set_memory_high_known(true);
    auto observed = view->ObserveResize(late);
    ASSERT_AWAIT_READY(observed);
    EXPECT_TRUE(observed.Get().IsOk());
    EXPECT_NEAR(view->GetOutstandingResizeForTest(unit.id()).memoryMb, 0.0, 0.001);
    EXPECT_EQ(view->GetResizeBudgetEntryCountForTest(), size_t{0});
}

// Elastic create option normalization: the SDK ships the marker nested inside
// the opaque extra_config JSON; admission must see the canonical flat key.
namespace {
InstanceInfo MakeElasticNormalizeInstance()
{
    InstanceInfo instance;
    instance.set_instanceid("elastic-norm");
    auto *resources = instance.mutable_resources()->mutable_resources();
    auto *cpu = (*resources)[resource_view::CPU_RESOURCE_NAME].mutable_scalar();
    cpu->set_value(500);
    cpu->set_limit(500);
    auto *memory = (*resources)[resource_view::MEMORY_RESOURCE_NAME].mutable_scalar();
    memory->set_value(128);
    memory->set_limit(512);
    return instance;
}
}  // namespace

TEST(ElasticOptionsTest, NestedMarkerSurfacesAsFlatOption)
{
    auto instance = MakeElasticNormalizeInstance();
    (*instance.mutable_createoptions())["extra_config"] = R"({"scheduler_elastic_resources":"true"})";
    bool elastic = false;
    auto origin = resource_view::ElasticOptionOrigin::NONE;
    auto status = resource_view::NormalizeElasticCreateOptions(&instance, &elastic, &origin);
    ASSERT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_TRUE(elastic);
    EXPECT_EQ(origin, resource_view::ElasticOptionOrigin::NESTED_EXTRA_CONFIG);
    EXPECT_EQ(instance.createoptions().at(SCHEDULER_ELASTIC_RESOURCES), "true");
    // The opaque payload itself must survive untouched.
    EXPECT_EQ(instance.createoptions().at("extra_config"), R"({"scheduler_elastic_resources":"true"})");
    // Idempotent: a second pass keeps the surfaced key stable.
    auto again = resource_view::NormalizeElasticCreateOptions(&instance, &elastic, &origin);
    EXPECT_TRUE(again.IsOk());
    EXPECT_EQ(instance.createoptions().at(SCHEDULER_ELASTIC_RESOURCES), "true");
    EXPECT_EQ(origin, resource_view::ElasticOptionOrigin::FLAT);
}

TEST(ElasticOptionsTest, NestedFalseMarkerAndJsonBoolAreAccepted)
{
    auto instance = MakeElasticNormalizeInstance();
    (*instance.mutable_createoptions())["extra_config"] = R"({"scheduler_elastic_resources":false})";
    bool elastic = true;
    auto status = resource_view::NormalizeElasticCreateOptions(&instance, &elastic);
    ASSERT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_FALSE(elastic);
    EXPECT_EQ(instance.createoptions().at(SCHEDULER_ELASTIC_RESOURCES), "false");
    // Explicit false never requires elastic resource scalars.
    instance.mutable_resources()->mutable_resources()->erase(resource_view::MEMORY_RESOURCE_NAME);
    EXPECT_TRUE(resource_view::NormalizeElasticCreateOptions(&instance).IsOk());
}

TEST(ElasticOptionsTest, FlatMarkerAloneAndLegacyRequestsStayUnchanged)
{
    auto instance = MakeElasticNormalizeInstance();
    (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    auto status = resource_view::NormalizeElasticCreateOptions(&instance);
    ASSERT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(instance.createoptions().at(SCHEDULER_ELASTIC_RESOURCES), "true");

    InstanceInfo legacy;
    (*legacy.mutable_createoptions())["unrelated"] = "keep";
    EXPECT_TRUE(resource_view::NormalizeElasticCreateOptions(&legacy).IsOk());
    EXPECT_EQ(legacy.createoptions().size(), size_t{1});
    EXPECT_FALSE(resource_view::UsesElasticAdmission(legacy));
}

TEST(ElasticOptionsTest, ConflictingOrMalformedMarkersAreRejected)
{
    auto instance = MakeElasticNormalizeInstance();
    (*instance.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "false";
    (*instance.mutable_createoptions())["extra_config"] = R"({"scheduler_elastic_resources":"true"})";
    EXPECT_FALSE(resource_view::NormalizeElasticCreateOptions(&instance).IsOk());

    auto badValue = MakeElasticNormalizeInstance();
    (*badValue.mutable_createoptions())["extra_config"] = R"({"scheduler_elastic_resources":"typo"})";
    EXPECT_FALSE(resource_view::NormalizeElasticCreateOptions(&badValue).IsOk());

    auto badType = MakeElasticNormalizeInstance();
    (*badType.mutable_createoptions())["extra_config"] = R"({"scheduler_elastic_resources":7})";
    EXPECT_FALSE(resource_view::NormalizeElasticCreateOptions(&badType).IsOk());

    auto badFlat = MakeElasticNormalizeInstance();
    (*badFlat.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "yes";
    EXPECT_FALSE(resource_view::NormalizeElasticCreateOptions(&badFlat).IsOk());
}

TEST(ElasticOptionsTest, OpaqueNonJsonExtraConfigKeepsOriginalSemantics)
{
    auto instance = MakeElasticNormalizeInstance();
    (*instance.mutable_createoptions())["extra_config"] = "not-a-json-object";
    auto status = resource_view::NormalizeElasticCreateOptions(&instance);
    ASSERT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(instance.createoptions().size(), size_t{1});

    auto flatWithGarbage = MakeElasticNormalizeInstance();
    (*flatWithGarbage.mutable_createoptions())["extra_config"] = "not-a-json-object";
    (*flatWithGarbage.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    EXPECT_TRUE(resource_view::NormalizeElasticCreateOptions(&flatWithGarbage).IsOk());
}

TEST(ElasticOptionsTest, ElasticTrueRequiresUsableExplicitScalars)
{
    auto missingMemory = MakeElasticNormalizeInstance();
    missingMemory.mutable_resources()->mutable_resources()->erase(resource_view::MEMORY_RESOURCE_NAME);
    (*missingMemory.mutable_createoptions())["extra_config"] = R"({"scheduler_elastic_resources":"true"})";
    auto status = resource_view::NormalizeElasticCreateOptions(&missingMemory);
    EXPECT_FALSE(status.IsOk());
    // Rejected requests must not leave a half-normalized marker behind.
    EXPECT_EQ(missingMemory.createoptions().count(SCHEDULER_ELASTIC_RESOURCES), size_t{0});

    auto inverted = MakeElasticNormalizeInstance();
    (*inverted.mutable_resources()->mutable_resources())[resource_view::MEMORY_RESOURCE_NAME]
        .mutable_scalar()->set_limit(64);
    (*inverted.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    EXPECT_FALSE(resource_view::NormalizeElasticCreateOptions(&inverted).IsOk());

    auto fractional = MakeElasticNormalizeInstance();
    (*fractional.mutable_resources()->mutable_resources())[resource_view::CPU_RESOURCE_NAME]
        .mutable_scalar()->set_value(500.5);
    (*fractional.mutable_createoptions())[SCHEDULER_ELASTIC_RESOURCES] = "true";
    EXPECT_FALSE(resource_view::NormalizeElasticCreateOptions(&fractional).IsOk());
}

}  // namespace functionsystem::test
