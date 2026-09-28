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

// Real behavior tests for the named-restore ceiling admission. These call
// the actual CheckRestoreAllocation with constructed ResourceUnits where the
// remaining budget is deliberately between initial(128) and ceiling(512):
//   - A pending named restore must reserve at ceiling, not initial.
//   - A normal elastic instance still reserves at initial.
//   - A released named restore (pending cleared) reserves at initial.
//   - A node with budget for initial but NOT ceiling must reject the named
//     restore while admitting the normal elastic create.
#include <gtest/gtest.h>
#include "common/resource_view/restore_admission.h"
#include <chrono>
#include <string>

namespace functionsystem::test {
using namespace resource_view;

namespace {

struct AdmissionSettingsGuard {
    UsageAdmissionSettings saved_;
    AdmissionSettingsGuard() : saved_(GetUsageAdmissionSettings())
    {
        SetUsageAdmissionSettings(0.95, 2048.0, 0);
    }
    ~AdmissionSettingsGuard() { SetUsageAdmissionSettings(saved_.safety, saved_.floorMb, saved_.maxInstances); }
};

static int unitCounter = 0;

ResourceUnit MakeUnit(double memCapMb, double memUsedMb)
{
    ResourceUnit u;
    u.set_id("ceiling-test-unit-" + std::to_string(++unitCounter));
    auto *memCap = (*u.mutable_capacity()->mutable_resources())["Memory"].mutable_scalar();
    memCap->set_value(memCapMb);
    auto *memUsed = (*u.mutable_actualuse()->mutable_resources())["Memory"].mutable_scalar();
    memUsed->set_value(memUsedMb);
    // CPU: ample headroom so memory is the binding constraint
    auto *cpuCap = (*u.mutable_capacity()->mutable_resources())["CPU"].mutable_scalar();
    cpuCap->set_value(16000);
    auto *cpuUsed = (*u.mutable_actualuse()->mutable_resources())["CPU"].mutable_scalar();
    cpuUsed->set_value(0);
    // Fresh samples
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    u.set_memory_sample_unix_ms(nowMs);
    u.set_cpu_sample_unix_ms(nowMs);
    return u;
}

InstanceInfo MakeElasticInstance(const std::string &id, double initialMiB, double ceilingMiB,
                                  bool pending, bool withTrustedRestore)
{
    InstanceInfo info;
    info.set_instanceid(id);
    (*info.mutable_createoptions())["scheduler.elastic_resources"] = "true";
    auto &mem = (*info.mutable_resources()->mutable_resources())["Memory"];
    mem.mutable_scalar()->set_value(initialMiB);
    mem.mutable_scalar()->set_limit(ceilingMiB);
    auto &cpu = (*info.mutable_resources()->mutable_resources())["CPU"];
    cpu.mutable_scalar()->set_value(500);
    cpu.mutable_scalar()->set_limit(500);
    info.set_resource_admission_pending(pending);
    if (withTrustedRestore) {
        (*info.mutable_scheduleoption()->mutable_extension())
            ["yr.internal.reusable_snapshot.trusted_restore"] = "test";
    }
    return info;
}

}  // namespace

class NamedRestoreCeilingAdmission : public ::testing::Test {
protected:
    AdmissionSettingsGuard guard_;
};

// A pending named restore (extension + pending) must be admitted at ceiling.
// Construct a node where 400MiB remains: initial(128) fits, ceiling(512) does not.
TEST_F(NamedRestoreCeilingAdmission, PendingNamedRestoreRejectedWhenBudgetFitsInitialOnly) {
    // capacity=1000, used=600 => 400 remaining. 128 fits, 512 does not.
    auto unit = MakeUnit(1000, 600);
    auto restore = MakeElasticInstance("inst-restore", 128, 512, true, true);
    const auto verdict = CheckRestoreAllocation(restore, unit, {});
    EXPECT_FALSE(verdict.IsOk()) << "pending named restore must reserve 512, not 128";
}

// A normal elastic create (no extension) with the same budget must be admitted
// at initial(128), proving the rejection is restore-specific.
TEST_F(NamedRestoreCeilingAdmission, NormalElasticAdmittedWithSameBudget) {
    auto unit = MakeUnit(1000, 600);
    auto normal = MakeElasticInstance("inst-normal", 128, 512, true, false);
    const auto verdict = CheckRestoreAllocation(normal, unit, {});
    EXPECT_TRUE(verdict.IsOk()) << "normal elastic reserves 128 and 400 is available";
}

// After release (pending cleared), the lingering extension must not force
// ceiling: the instance books at initial again.
TEST_F(NamedRestoreCeilingAdmission, ReleasedNamedRestoreAdmittedAtInitial) {
    auto unit = MakeUnit(1000, 600);
    auto released = MakeElasticInstance("inst-released", 128, 512, false, true);
    const auto verdict = CheckRestoreAllocation(released, unit, {});
    EXPECT_TRUE(verdict.IsOk()) << "released restore (pending=false) reserves 128";
}

// A node with enough budget for ceiling(512) must admit the named restore.
TEST_F(NamedRestoreCeilingAdmission, PendingNamedRestoreAdmittedWhenBudgetFitsCeiling) {
    // capacity=1000, used=400 => 600 remaining. 512 fits.
    auto unit = MakeUnit(1000, 400);
    auto restore = MakeElasticInstance("inst-restore-ok", 128, 512, true, true);
    const auto verdict = CheckRestoreAllocation(restore, unit, {});
    EXPECT_TRUE(verdict.IsOk()) << "600 remaining should admit a 512 ceiling restore";
}

}  // namespace functionsystem::test
