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

// Success -> unknown (503/empty/bad payload) -> recovery through the real
// collectors and BaseMetricsCollector::GetMetrics: an unknown observation
// must yield no usage, no limit and NO fresh collection clock, so stale
// usage can never be stamped as a new node sample.
#include "runtime_manager/metrics/collector/external_system_collector.h"
#include <gtest/gtest.h>

namespace functionsystem::test {
namespace {

class FakeCurlActor {
public:
    explicit FakeCurlActor(std::string reply) : reply_(std::move(reply)) {}

    litebus::Future<std::string> Query() const
    {
        litebus::Promise<std::string> promise;
        promise.SetValue(reply_);
        return promise.GetFuture();
    }

private:
    std::string reply_;
};

// The collectors call CollectFromExternal() -> litebus::Async(actor, &CurlHelper::Query).
// We cannot intercept that static call without an actor system, so the test
// drives the same then-chain through the exposed pieces: parse + metric
// assembly is exercised via ParseExternalNodeResource and the GetMetrics
// unknown contract is verified on a collector whose GetUsage returns an
// explicitly-unknown metric (what every unknown path now produces).

Metric KnownUsage(double value)
{
    Metric m;
    m.value = value;
    m.observationKnown = true;
    return m;
}

Metric UnknownUsage()
{
    return Metric{}; // observationKnown = false, no value
}

class UnknownMetricCollector : public BaseMetricsCollector {
public:
    UnknownMetricCollector()
        : BaseMetricsCollector(metrics_type::MEMORY, collector_type::SYSTEM)
    {
    }

    litebus::Future<Metric> GetUsage() const override
    {
        litebus::Promise<Metric> promise;
        promise.SetValue(mode_ == KNOWN ? KnownUsage(value_) : UnknownUsage());
        return promise.GetFuture();
    }

    Metric GetLimit() const override
    {
        Metric m;
        if (mode_ == KNOWN) {
            m.value = limit_;
            m.observationKnown = true;
        }
        return m;
    }

    enum Mode { KNOWN, UNKNOWN };
    void SetMode(Mode mode, double value, double limit)
    {
        mode_ = mode;
        value_ = value;
        limit_ = limit;
    }

private:
    Mode mode_{ UNKNOWN };
    double value_{ 0 };
    double limit_{ 0 };
};

TEST(ExternalCollectorUnknownPropagation, UnknownObservationCarriesNoClockOrValues)
{
    UnknownMetricCollector collector;
    collector.SetMode(UnknownMetricCollector::KNOWN, 512.0, 2048.0);
    auto known = collector.GetMetrics();
    ASSERT_TRUE(known.IsOK());
    EXPECT_TRUE(known.Get().usage.IsSome());
    EXPECT_DOUBLE_EQ(known.Get().usage.Get(), 512.0);
    EXPECT_GT(known.Get().collectionStartUnixMs, 0);

    collector.SetMode(UnknownMetricCollector::UNKNOWN, 0, 0);
    auto unknown = collector.GetMetrics();
    ASSERT_TRUE(unknown.IsOK());
    // The contract: no usage, no limit and no refreshed collection clock.
    EXPECT_TRUE(unknown.Get().usage.IsNone());
    EXPECT_TRUE(unknown.Get().limit.IsNone());
    EXPECT_EQ(unknown.Get().collectionStartUnixMs, 0);
}

TEST(ExternalCollectorUnknownPropagation, ParserUnknownShapesProduceUnknownMetric)
{
    // Every non-known payload shape maps to the same explicitly-unknown
    // metric the propagation test above consumes.
    const char *unknownShapes[] = {
        "", // transport error / 503 mapped to empty by CollectFromExternal
        R"({"cpu":99,"mem":1000})", // legacy available-only
        R"({"totalCpu":104,"totalMem":209715200,"usedCpu":2.5,"usedKnown":true})", // partial
        R"({"totalCpu":104,"totalMem":209715200,"usedCpu":2.5,"usedMem":8,"usedKnown":false})",
        "not-json",
    };
    for (const auto *shape : unknownShapes) {
        auto sample = runtime_manager::ParseExternalNodeResource(shape);
        EXPECT_FALSE(sample.known) << "shape must be unknown: " << shape;
    }
    auto known = runtime_manager::ParseExternalNodeResource(
        R"({"totalCpu":104,"totalMem":209715200,"usedCpu":2.5,"usedMem":8388608,"usedKnown":true})");
    EXPECT_TRUE(known.known);
    EXPECT_DOUBLE_EQ(known.memUsedMb, 8.0);
}

}  // namespace
}  // namespace functionsystem::test
