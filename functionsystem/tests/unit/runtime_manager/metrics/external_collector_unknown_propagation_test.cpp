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
// through the REAL ExternalSystem CPU/Memory collectors: an unknown
// observation yields no usage, no limit and no collection clock, so stale
// usage can never be stamped as a fresh node sample.
#include "runtime_manager/metrics/collector/external_system_collector.h"
#include <gtest/gtest.h>

namespace functionsystem::test {
namespace {
using runtime_manager::ExternalSystemCPUCollector;
using runtime_manager::ExternalSystemMemoryCollector;

std::function<litebus::Future<std::string>()> Reply(const std::string &payload)
{
    return [payload]() -> litebus::Future<std::string> {
        litebus::Promise<std::string> promise;
        promise.SetValue(payload);
        return promise.GetFuture();
    };
}

const char *kKnownA =
    R"({"cpu":99,"mem":194347270144,"totalCpu":104,"totalMem":201220395008,"usedCpu":2.5,"usedMem":8388608,"usedKnown":true})";
const char *kKnownB =
    R"({"cpu":99,"mem":194347270144,"totalCpu":104,"totalMem":201220395008,"usedCpu":4.125,"usedMem":16777216,"usedKnown":true})";

TEST(ExternalCollectorUnknownPropagation, RealMemoryCollectorUnknownShapesCarryNoClock)
{
    const char *unknownShapes[] = {
        "", // transport error or daemon 503 mapped to empty
        R"({"cpu":99,"mem":194347270144})", // legacy available-only
        R"({"totalCpu":104,"totalMem":201220395008,"usedCpu":2.5,"usedKnown":true})", // partial
        R"({"totalCpu":104,"totalMem":201220395008,"usedCpu":2.5,"usedMem":8,"usedKnown":false})",
        "not-json",
    };
    for (const auto *shape : unknownShapes) {
        ExternalSystemMemoryCollector collector(Reply(shape));
        auto metrics = collector.GetMetrics();
        ASSERT_TRUE(metrics.IsOK()) << shape;
        // Unknown: no usage, no limit, NO collection clock.
        EXPECT_TRUE(metrics.Get().usage.IsNone()) << shape;
        EXPECT_TRUE(metrics.Get().limit.IsNone()) << shape;
        EXPECT_EQ(metrics.Get().collectionStartUnixMs, 0) << shape;
    }
}

TEST(ExternalCollectorUnknownPropagation, RealMemoryCollectorSuccessUnknownRecovery)
{
    ExternalSystemMemoryCollector collector(Reply(kKnownA));
    auto known = collector.GetMetrics();
    ASSERT_TRUE(known.IsOK());
    EXPECT_DOUBLE_EQ(known.Get().usage.Get(), 8.0); // 8 MiB used
    EXPECT_DOUBLE_EQ(known.Get().limit.Get(), 192.0); // total MiB
    EXPECT_GT(known.Get().collectionStartUnixMs, 0);

    // A stale previous_ must NOT leak into an unknown observation (the
    // old bug returned *previous with a fresh wall clock).
    collector.SetTestTransport(Reply(""));
    auto unknown = collector.GetMetrics();
    ASSERT_TRUE(unknown.IsOK());
    EXPECT_TRUE(unknown.Get().usage.IsNone());
    EXPECT_TRUE(unknown.Get().limit.IsNone());
    EXPECT_EQ(unknown.Get().collectionStartUnixMs, 0);

    // Recovery with DIFFERENT values proves a fresh observation.
    collector.SetTestTransport(Reply(kKnownB));
    auto recovered = collector.GetMetrics();
    ASSERT_TRUE(recovered.IsOK());
    EXPECT_DOUBLE_EQ(recovered.Get().usage.Get(), 16.0);
    EXPECT_DOUBLE_EQ(recovered.Get().limit.Get(), 192.0);
    EXPECT_GT(recovered.Get().collectionStartUnixMs, 0);
}

TEST(ExternalCollectorUnknownPropagation, RealCPUCollectorSameContract)
{
    ExternalSystemCPUCollector collector(Reply(kKnownA));
    auto known = collector.GetMetrics();
    ASSERT_TRUE(known.IsOK());
    EXPECT_TRUE(known.Get().usage.IsSome());

    collector.SetTestTransport(Reply(R"({"usedKnown":false})"));
    auto unknown = collector.GetMetrics();
    ASSERT_TRUE(unknown.IsOK());
    EXPECT_TRUE(unknown.Get().usage.IsNone());
    EXPECT_EQ(unknown.Get().collectionStartUnixMs, 0);
}

}  // namespace
}  // namespace functionsystem::test
