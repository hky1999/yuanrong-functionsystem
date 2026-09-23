/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Success -> unknown (empty reply / unknown sample / bad JSON) -> recovery
// through the REAL ExternalSystem CPU/Memory collectors and the REAL
// MetricsActor::BuildResourceUnitWithSystem: an unknown observation yields
// no usage entry and no sample clock (capacity is retained), so stale
// usage can never refresh admission freshness and CheckResourceHeadroom
// rejects until a fresh known observation arrives.
#include "runtime_manager/metrics/collector/external_system_collector.h"
#include "runtime_manager/metrics/metrics_actor.h"
#include "common/resource_view/restore_admission.h"
#include <gtest/gtest.h>

namespace functionsystem::test {
namespace {
using runtime_manager::ExternalSystemCPUCollector;
using runtime_manager::ExternalSystemMemoryCollector;
using runtime_manager::MetricsActor;

std::function<litebus::Future<std::string>()> Reply(const std::string &payload)
{
    return [payload]() -> litebus::Future<std::string> {
        litebus::Promise<std::string> promise;
        promise.SetValue(payload);
        return promise.GetFuture();
    };
}

// 201326592 bytes = exactly 192 MiB total; usedMem 8 MiB / 16 MiB.
const char *kKnownA =
    R"({"cpu":99,"mem":194347270144,"totalCpu":104,"totalMem":201326592,"usedCpu":2.5,"usedMem":8388608,"usedKnown":true})";
const char *kKnownB =
    R"({"cpu":99,"mem":194347270144,"totalCpu":104,"totalMem":201326592,"usedCpu":4.125,"usedMem":16777216,"usedKnown":true})";

std::shared_ptr<MetricsActor> UnitBuilder()
{
    // BuildResourceUnitWithSystem only reads the metric futures; a bare
    // actor instance suffices (no Init, no loops).
    return std::make_shared<MetricsActor>("unknown-propagation-test");
}

resources::ResourceUnit BuildUnit(const std::vector<litebus::Future<runtime_manager::Metrics>> &futures)
{
    return UnitBuilder()->BuildResourceUnitWithSystem(futures);
}

struct AdmissionGuard {
    resource_view::UsageAdmissionSettings saved = resource_view::GetUsageAdmissionSettings();
    ~AdmissionGuard() { resource_view::SetUsageAdmissionSettings(saved.safety, saved.floorMb, saved.maxInstances); }
};

TEST(ExternalCollectorUnknownPropagation, RealMemoryCollectorUnknownShapesCarryNoUsage)
{
    const char *unknownShapes[] = {
        "", // transport error or daemon 503 mapped to empty
        R"({"cpu":99,"mem":194347270144})", // legacy available-only
        R"({"totalCpu":104,"totalMem":201326592,"usedCpu":2.5,"usedKnown":true})", // partial
        R"({"totalCpu":104,"totalMem":201326592,"usedCpu":2.5,"usedMem":8,"usedKnown":false})",
        "not-json",
    };
    for (const auto *shape : unknownShapes) {
        ExternalSystemMemoryCollector collector(Reply(shape));
        auto metrics = collector.GetMetrics();
        ASSERT_TRUE(metrics.IsOK()) << shape;
        // Unknown observation: no usage value. (The collection timestamp
        // measures when this collection ran, not sample freshness; the
        // binding contract is the unit sample clock below.)
        EXPECT_TRUE(metrics.Get().usage.IsNone()) << shape;
        auto unit = BuildUnit({metrics.Get()});
        EXPECT_EQ(unit.memory_sample_unix_ms(), 0) << "unknown must not stamp a sample clock: " << shape;
        if (unit.actualuse().resources().contains(resource_view::MEMORY_RESOURCE_NAME)) {
            const auto &entry = unit.actualuse().resources().at(resource_view::MEMORY_RESOURCE_NAME);
            EXPECT_FALSE(entry.has_scalar() && entry.scalar().value() > 0)
                << "no fabricated known usage: " << shape;
        }
    }
}

TEST(ExternalCollectorUnknownPropagation, MemoryUnknownThenRecoveryThroughAdmission)
{
    AdmissionGuard guard;
    resource_view::SetUsageAdmissionSettings(0.95, 2048, 0);
    const resource_view::AdmissionReserve reserve{500, 16};
    // Headroom requires fresh CPU AND fresh Memory: a known CPU metric
    // rides along in every phase so only the memory side varies.
    ExternalSystemCPUCollector cpu(Reply(kKnownA));

    ExternalSystemMemoryCollector collector(Reply(kKnownA));
    auto cpuKnown = cpu.GetMetrics();
    auto knownMetrics = collector.GetMetrics();
    ASSERT_TRUE(cpuKnown.IsOK() && knownMetrics.IsOK());
    EXPECT_DOUBLE_EQ(knownMetrics.Get().usage.Get(), 8.0);
    auto knownUnit = BuildUnit({cpuKnown.Get(), knownMetrics.Get()});
    EXPECT_GT(knownUnit.memory_sample_unix_ms(), 0);
    // Known fresh sample with tiny usage: headroom passes.
    EXPECT_TRUE(resource_view::CheckResourceHeadroom(knownUnit, reserve).IsOk());

    // Unknown (empty reply = 503/transport): no known usage and no
    // sample clock; the stale previous_ must NOT leak back in.
    collector.SetTestTransport(Reply(""));
    auto unknownMetrics = collector.GetMetrics();
    ASSERT_TRUE(unknownMetrics.IsOK());
    EXPECT_TRUE(unknownMetrics.Get().usage.IsNone());
    auto unknownUnit = BuildUnit({cpuKnown.Get(), unknownMetrics.Get()});
    EXPECT_EQ(unknownUnit.memory_sample_unix_ms(), 0);
    // No KNOWN usage: either the entry is absent or it carries no usage
    // value — the binding contract is "no fabricated known usage".
    if (unknownUnit.actualuse().resources().contains(resource_view::MEMORY_RESOURCE_NAME)) {
        const auto &entry = unknownUnit.actualuse().resources().at(resource_view::MEMORY_RESOURCE_NAME);
        EXPECT_FALSE(entry.has_scalar() && entry.scalar().value() > 0)
            << "unknown observation must not fabricate known usage";
    }
    // Stale/absent fresh sample: admission must reject.
    EXPECT_FALSE(resource_view::CheckResourceHeadroom(unknownUnit, reserve).IsOk());

    // Recovery with DIFFERENT values proves a fresh observation.
    collector.SetTestTransport(Reply(kKnownB));
    auto recoveredMetrics = collector.GetMetrics();
    ASSERT_TRUE(recoveredMetrics.IsOK());
    EXPECT_DOUBLE_EQ(recoveredMetrics.Get().usage.Get(), 16.0);
    auto recoveredUnit = BuildUnit({cpuKnown.Get(), recoveredMetrics.Get()});
    EXPECT_GT(recoveredUnit.memory_sample_unix_ms(), 0);
    EXPECT_TRUE(resource_view::CheckResourceHeadroom(recoveredUnit, reserve).IsOk());
}

TEST(ExternalCollectorUnknownPropagation, CpuUnknownKeepsContractAndRecoversNewValue)
{
    AdmissionGuard guard;
    resource_view::SetUsageAdmissionSettings(0.95, 2048, 0);
    const resource_view::AdmissionReserve reserve{500, 16};

    ExternalSystemCPUCollector cpu(Reply(kKnownA));
    ExternalSystemMemoryCollector mem(Reply(kKnownA));
    auto cpuKnown = cpu.GetMetrics();
    auto memKnown = mem.GetMetrics();
    ASSERT_TRUE(cpuKnown.IsOK() && memKnown.IsOK());
    auto unit = BuildUnit({cpuKnown.Get(), memKnown.Get()});
    EXPECT_GT(unit.cpu_sample_unix_ms(), 0);
    EXPECT_GT(unit.memory_sample_unix_ms(), 0);
    EXPECT_TRUE(resource_view::CheckResourceHeadroom(unit, reserve).IsOk());

    // Only CPU unknown: CPU clock drops to 0 -> admission rejects.
    cpu.SetTestTransport(Reply(R"({"usedKnown":false})"));
    auto cpuUnknown = cpu.GetMetrics();
    ASSERT_TRUE(cpuUnknown.IsOK());
    EXPECT_TRUE(cpuUnknown.Get().usage.IsNone());
    auto cpuUnknownUnit = BuildUnit({cpuUnknown.Get(), memKnown.Get()});
    EXPECT_EQ(cpuUnknownUnit.cpu_sample_unix_ms(), 0);
    EXPECT_GT(cpuUnknownUnit.memory_sample_unix_ms(), 0);
    EXPECT_FALSE(resource_view::CheckResourceHeadroom(cpuUnknownUnit, reserve).IsOk());

    // CPU recovers with a DIFFERENT value (4.125 cores in kKnownB): the
    // explicit new-value assertion guards against a mere clock refresh
    // being mistaken for recovery.
    cpu.SetTestTransport(Reply(kKnownB));
    auto cpuRecovered = cpu.GetMetrics();
    ASSERT_TRUE(cpuRecovered.IsOK());
    ASSERT_TRUE(cpuRecovered.Get().usage.IsSome());
    EXPECT_DOUBLE_EQ(cpuRecovered.Get().usage.Get(), 4125.0); // usedCpu 4.125 cores * CPU_SCALE 1000
    auto recoveredUnit = BuildUnit({cpuRecovered.Get(), memKnown.Get()});
    EXPECT_GT(recoveredUnit.cpu_sample_unix_ms(), 0);
    EXPECT_TRUE(resource_view::CheckResourceHeadroom(recoveredUnit, reserve).IsOk());
}

}  // namespace
}  // namespace functionsystem::test
