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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

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
        // deterministic knobs for every case
        UsageAwareFilter::SetConfig(0.9, 2048.0);
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
TEST(UsageAwareFilterTest, PassWhenUsageWellBelowLine)
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

TEST(UsageAwareFilterTest, RejectWhenUsageExceedsLine)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    // 35000 + 2048 > 36000
    auto unit = MakeUnit(40000, 35000);
    auto inst = MakeInstance(32000);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_EQ(out.availableForRequest, 0);
}

TEST(UsageAwareFilterTest, BoundaryExactlyAtLine)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    // 33952 + 2048 == 36000 exactly: pass (not strictly greater)
    auto unit = MakeUnit(40000, 33952);
    auto inst = MakeInstance(32000);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
}

TEST(UsageAwareFilterTest, FloorCappedBySmallRequest)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    // request 512 caps the reserve: 35600 + 512 <= 36000
    auto unit = MakeUnit(40000, 35600);
    auto inst = MakeInstance(512);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
    // but 35700 + 512 > 36000 rejects
    unit = MakeUnit(40000, 35700);
    out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::RESOURCE_NOT_ENOUGH);
}

// no real-usage signal: stay neutral, booked fallback decides in DefaultFilter
TEST(UsageAwareFilterTest, PassWhenActualUseMissing)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = view_utils::Get1DResourceUnit(); // actualuse = 0
    auto inst = MakeInstance(32000);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
}

TEST(UsageAwareFilterTest, ConfigKnobsTakeEffect)
{
    UsageAwareFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(40000, 38000);
    auto inst = MakeInstance(32000);
    // 38000 + 2048 > 36000 with default 0.9
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::RESOURCE_NOT_ENOUGH);
    // safety 0.98 -> 39200 allowed, passes
    UsageAwareFilter::SetConfig(0.98, 2048.0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
    // back to 0.9 with a tiny floor also passes
    UsageAwareFilter::SetConfig(0.9, 512.0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
    UsageAwareFilter::SetConfig(0.9, 2048.0);
}
}  // namespace functionsystem::test
