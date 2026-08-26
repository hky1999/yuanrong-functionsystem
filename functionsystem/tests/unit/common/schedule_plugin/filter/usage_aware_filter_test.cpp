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

#include "common/schedule_plugin/filter/usage_aware_filter/usage_aware_filter.h"

#include <chrono>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/resource_view/commitment_ledger.h"
#include "common/resource_view/usage_trend_tracker.h"
#include "common/resource_view/view_utils.h"
#include "common/schedule_plugin/common/preallocated_context.h"

namespace functionsystem::test {
using namespace ::testing;
using namespace functionsystem::schedule_plugin::filter;
using namespace functionsystem::schedule_framework;

class UsageAwareFilterTest : public Test {
protected:
    void SetUp() override
    {
        // deterministic knobs for every case (maxInstances 0 = unlimited)
        UsageAwareFilter::SetConfig(0.9, 2048.0, 0);
        // trend prediction off unless a case opts in (predictor state is
        // process-global; horizon 0 makes Predict a no-op)
        resource_view::UsageTrendTracker::SetParams(60000, 0);
        // commitment ledger is process-global too
        resource_view::CommitmentLedger::ResetForTest();
    }

    static resource_view::ResourceUnit MakeUnit(double capacityMem, double actualMem)
    {
        auto unit = view_utils::Get1DResourceUnit();
        unit.mutable_capacity()->mutable_resources()->at(view_utils::RESOURCE_MEM_NAME).mutable_scalar()->set_value(
            capacityMem);
        unit.mutable_actualuse()->mutable_resources()->at(view_utils::RESOURCE_MEM_NAME).mutable_scalar()->set_value(
            actualMem);
        return unit;
    }

    static resource_view::InstanceInfo MakeInstance(double requestMem)
    {
        auto inst = view_utils::Get1DInstance();
        inst.mutable_resources()->mutable_resources()->at(view_utils::RESOURCE_MEM_NAME).mutable_scalar()->set_value(
            requestMem);
        return inst;
    }
};

// admission formula: actualuse + min(floor, request) <= capacity * safety
TEST_F(UsageAwareFilterTest, PassWhenUsageWellBelowLine)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    // 40000 total, safety 0.9 -> 36000 allowed; 10000 used + 2048 reserve
    auto unit = MakeUnit(40000, 10000);
    auto inst = MakeInstance(32000);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
    EXPECT_FALSE(out.isFatalErr);
}

TEST_F(UsageAwareFilterTest, RejectWhenUsageExceedsLine)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    // 35000 + 2048 > 36000
    auto unit = MakeUnit(40000, 35000);
    auto inst = MakeInstance(32000);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::RESOURCE_NOT_ENOUGH);
    // DefaultFilter convention: -1 = not applicable on rejection
    EXPECT_EQ(out.availableForRequest, -1);
}

TEST_F(UsageAwareFilterTest, BoundaryExactlyAtLine)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    // 33952 + 2048 == 36000 exactly: pass (not strictly greater)
    auto unit = MakeUnit(40000, 33952);
    auto inst = MakeInstance(32000);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
}

TEST_F(UsageAwareFilterTest, FloorCappedBySmallRequest)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    // request 512 caps the reserve: 35488 + 512 == 36000 exactly
    auto unit = MakeUnit(40000, 35488);
    auto inst = MakeInstance(512);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
    // but 35500 + 512 > 36000 rejects
    unit = MakeUnit(40000, 35500);
    out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::RESOURCE_NOT_ENOUGH);
}

// no real-usage signal: stay neutral, booked fallback decides in DefaultFilter
TEST_F(UsageAwareFilterTest, PassWhenActualUseMissing)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = view_utils::Get1DResourceUnit(); // actualuse = 0
    auto inst = MakeInstance(32000);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
}

TEST_F(UsageAwareFilterTest, ConfigKnobsTakeEffect)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(40000, 35000);
    auto inst = MakeInstance(32000);
    // 35000 + 2048 = 37048 > 36000 with default 0.9
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::RESOURCE_NOT_ENOUGH);
    // safety 0.98 -> 39200 allowed: 37048 <= 39200 passes
    UsageAwareFilter::SetConfig(0.98, 2048.0, 0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
    // back to 0.9 with a tiny floor: 35000 + 512 = 35512 <= 36000 passes
    UsageAwareFilter::SetConfig(0.9, 512.0, 0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
    UsageAwareFilter::SetConfig(0.9, 2048.0, 0);
}

// D-form: trend prediction raises effective usage above the admission line
TEST_F(UsageAwareFilterTest, TrendPredictionRaisesUsage)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(40000, 33000);  // 33000 + 2048 = 35048 <= 36000: passes cold
    auto inst = MakeInstance(32000);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);

    // enable prediction for this unit's id: rising samples push the estimate
    // past the line (slope 1000 MB / step; horizon multiplies the worst slope).
    // Records are spaced with real sleeps: same-ms samples carry no slope.
    resource_view::UsageTrendTracker::SetParams(60000, 30000);
    resource_view::UsageTrendTracker::Record(unit.id(), 33000);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    resource_view::UsageTrendTracker::Record(unit.id(), 34000);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    resource_view::UsageTrendTracker::Record(unit.id(), 35000);
    EXPECT_GT(resource_view::UsageTrendTracker::Predict(unit.id()), 35000.0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::RESOURCE_NOT_ENOUGH);

    // horizon 0 disables prediction: back to the cold verdict
    resource_view::UsageTrendTracker::SetParams(60000, 0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
}

// D-6 F2: the slope term is recency-bounded -- a burst that ended a few
// samples ago stops inflating the estimate even though its samples are
// still inside the 60s window (live: a dd ramp kept wake/restore admission
// rejected long after the sandboxes were parked and node use had collapsed)
TEST_F(UsageAwareFilterTest, TrendSlopeExpiresOnceRampEnds)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    // 40000 total, safety 0.9 -> 36000 allowed; 33000 + 2048 = 35048 passes
    auto unit = MakeUnit(40000, 33000);
    auto inst = MakeInstance(32000);

    resource_view::UsageTrendTracker::SetParams(60000, 30000);
    // ramp: 4 rising samples, slope 1000 MB / 10 ms = 100 MB/ms
    for (double mb = 30000.0; mb <= 33000.0; mb += 1000.0) {
        resource_view::UsageTrendTracker::Record(unit.id(), mb);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_GT(resource_view::UsageTrendTracker::Predict(unit.id()), 34000.0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::RESOURCE_NOT_ENOUGH);

    // ramp ended: 5 flat samples push every rising pair out of the recent
    // window -> prediction collapses back to the latest sample -> admission
    // no longer blocked by the dead burst
    for (int i = 0; i < 5; ++i) {
        resource_view::UsageTrendTracker::Record(unit.id(), 33000);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_NEAR(resource_view::UsageTrendTracker::Predict(unit.id()), 33000.0, 1.0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);

    resource_view::UsageTrendTracker::SetParams(60000, 0);
}

// D-form: per-node instance count cap rejects before the memory arithmetic
TEST_F(UsageAwareFilterTest, InstanceCapReached)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(40000, 10000);  // plenty of memory headroom
    auto inst = MakeInstance(32000);
    // cap 0 (default): passes
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);

    UsageAwareFilter::SetConfig(0.9, 2048.0, 2);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
    // fill the unit to the cap with live instances
    (*unit.mutable_instances())["inst-a"] = MakeInstance(1024);
    (*unit.mutable_instances())["inst-b"] = MakeInstance(1024);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::RESOURCE_NOT_ENOUGH);
    // one slot frees up: passes again
    unit.mutable_instances()->erase("inst-a");
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
    UsageAwareFilter::SetConfig(0.9, 2048.0, 0);
}

// Ph0.1 TOCTOU: same-window in-flight instances hold a floor-style reserve
// even though node actualuse has not moved yet — the burst cannot all pass
TEST_F(UsageAwareFilterTest, InflightPendingReserveClosesToctouWindow)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    // 40000 total, safety 0.9 -> 36000 allowed; usage snapshot stuck at 10000
    // (metrics cycle has not run since the burst started)
    auto unit = MakeUnit(40000, 10000);
    auto inst = MakeInstance(32000);
    // cold: 10000 + 2048 <= 36000 passes
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);

    // simulate in-flight admissions onto this unit in the same context, one
    // at a time: 11 in-flight -> 10000 + 2048 + 11*2048 = 34576 <= 36000
    // still passes; the 12th -> 36624 > 36000 rejects while actualuse is
    // frozen at the same stale snapshot
    auto &alloc = ctx->allocated[unit.id()];
    (*alloc.resource.mutable_resources())[view_utils::RESOURCE_MEM_NAME] =
        MakeInstance(32000).resources().resources().at(view_utils::RESOURCE_MEM_NAME);
    for (int i = 0; i < 12; i++) {
        ctx->preAllocatedSelectedFunctionAgentMap["inflight-" + std::to_string(i)] = unit.id();
        // grow the summed booked accumulator alongside (booked stays above
        // the floor-sum so min() picks the floor-sum side)
        alloc.resource.mutable_resources()->at(view_utils::RESOURCE_MEM_NAME).mutable_scalar()->set_value(
            (i + 1) * 32000.0);
        auto verdict = filter.Filter(ctx, inst, unit).status;
        EXPECT_EQ(verdict, i < 11 ? StatusCode::SUCCESS : StatusCode::RESOURCE_NOT_ENOUGH);
    }

    // small instances: booked sum caps the pending reserve below the
    // floor-sum — 2 * 512 booked with 2 in-flight reserves min(2*2048, 1024)
    auto ctx2 = std::make_shared<PreAllocatedContext>();
    auto &alloc2 = ctx2->allocated[unit.id()];
    (*alloc2.resource.mutable_resources())[view_utils::RESOURCE_MEM_NAME] =
        MakeInstance(32000).resources().resources().at(view_utils::RESOURCE_MEM_NAME);
    alloc2.resource.mutable_resources()->at(view_utils::RESOURCE_MEM_NAME).mutable_scalar()->set_value(1024.0);
    ctx2->preAllocatedSelectedFunctionAgentMap["inflight-a"] = unit.id();
    ctx2->preAllocatedSelectedFunctionAgentMap["inflight-b"] = unit.id();
    // 10000 + 2048 + 1024 = 13072 <= 36000: passes despite floor-sum 4096
    EXPECT_EQ(filter.Filter(ctx2, inst, unit).status, StatusCode::SUCCESS);
}

// P2.0 commitment-aware admission: promised-but-unrealized ladder headroom
// rides the same safety line as actual usage
TEST_F(UsageAwareFilterTest, CommitmentPushesOverLine)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    // 40000 total, safety 0.9 -> 36000 allowed; 33000 + 2048 = 35048 passes
    // on usage alone
    auto unit = MakeUnit(40000, 33000);
    unit.set_id("commit-node");
    auto inst = MakeInstance(32000);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
    // a 2 GiB ladder raise books 2048 MB of promise: 33000 + 2048 + 2048
    // = 37296 > 36000 rejects
    resource_view::CommitmentLedger::OnRaise("commit-node", 2ULL * 1024 * 1024 * 1024, 0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::RESOURCE_NOT_ENOUGH);
}

// control arm: the ledger disabled reproduces the pre-P2.0 verdict exactly
TEST_F(UsageAwareFilterTest, CommitmentDisabledKeepsOldBehavior)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(40000, 33000);
    unit.set_id("commit-node");
    auto inst = MakeInstance(32000);
    resource_view::CommitmentLedger::OnRaise("commit-node", 2ULL * 1024 * 1024 * 1024, 0);
    resource_view::CommitmentLedger::SetEnabled(false);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
}
}  // namespace functionsystem::test
