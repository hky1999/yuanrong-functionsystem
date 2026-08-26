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

#include "common/schedule_plugin/filter/overcommit_filter/overcommit_filter.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/resource_view/view_utils.h"
#include "common/schedule_plugin/common/preallocated_context.h"

namespace functionsystem::test {
using namespace ::testing;
using namespace functionsystem::schedule_plugin::filter;
using namespace functionsystem::schedule_framework;

class OvercommitFilterTest : public Test {
protected:
    void SetUp() override
    {
        OvercommitFilter::SetConfig(1.0);
    }

    static resource_view::ResourceUnit MakeUnit(double capacityMem)
    {
        auto unit = view_utils::Get1DResourceUnit();
        unit.mutable_capacity()->mutable_resources()->at(view_utils::RESOURCE_MEM_NAME).mutable_scalar()->set_value(
            capacityMem);
        unit.mutable_allocatable()->mutable_resources()->at(view_utils::RESOURCE_MEM_NAME).mutable_scalar()->set_value(
            capacityMem);
        return unit;
    }

    // resident instance on the unit: request=requestMem, limit=limitMb (0 = guaranteed)
    static void AddResident(resource_view::ResourceUnit &unit, const std::string &id, double requestMem,
                            double limitMb)
    {
        auto inst = view_utils::Get1DInstance();
        inst.set_instanceid(id);
        auto scalar = inst.mutable_resources()->mutable_resources()->at(view_utils::RESOURCE_MEM_NAME).mutable_scalar();
        scalar->set_value(requestMem);
        if (limitMb > 0) {
            scalar->set_limit(limitMb);
        }
        (*unit.mutable_instances())[id] = inst;
    }

    static resource_view::InstanceInfo MakeInstance(double requestMem, double limitMb)
    {
        auto inst = view_utils::Get1DInstance();
        // the fixture's CPU scalar (value 1000.1 vs unit capacity 100.1) would
        // dominate the verdict; this suite is about the Memory ceiling only
        inst.mutable_resources()->mutable_resources()->erase(view_utils::RESOURCE_CPU_NAME);
        auto scalar = inst.mutable_resources()->mutable_resources()->at(view_utils::RESOURCE_MEM_NAME).mutable_scalar();
        scalar->set_value(requestMem);
        if (limitMb > 0) {
            scalar->set_limit(limitMb);
        }
        return inst;
    }
};

// request without limit (guaranteed class): filter must stay neutral even on a full node
TEST_F(OvercommitFilterTest, NoOpForGuaranteedRequest)
{
    OvercommitFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(41240);
    AddResident(unit, "r1", 32768, 0);
    auto inst = MakeInstance(32768, 0);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
}

// Σ ceiling: 32768(resident limit) + 32768(request limit) = 65536 > 41240 * 1.0
TEST_F(OvercommitFilterTest, RejectWhenCeilingSumExceedsCapacity)
{
    OvercommitFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(41240);
    AddResident(unit, "r1", 8192, 32768);
    auto inst = MakeInstance(8192, 32768);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_EQ(out.availableForRequest, -1);
}

// ratio 2.0 lifts the ceiling to 82480: 65536 fits
TEST_F(OvercommitFilterTest, RatioLiftsCeiling)
{
    OvercommitFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(41240);
    AddResident(unit, "r1", 8192, 32768);
    auto inst = MakeInstance(8192, 32768);
    OvercommitFilter::SetConfig(2.0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
    OvercommitFilter::SetConfig(1.0);
}

// resident without limit counts at its booked value, not at some default ceiling
TEST_F(OvercommitFilterTest, GuaranteedResidentCountsAtBookedValue)
{
    OvercommitFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(41240);
    AddResident(unit, "r1", 40000, 0);  // booked 40000, no limit
    auto inst = MakeInstance(8192, 32768);
    // 40000 + 32768 = 72768 > 41240 -> reject at ratio 1.0
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::RESOURCE_NOT_ENOUGH);
    // ratio 2.0 -> ceiling 82480, passes
    OvercommitFilter::SetConfig(2.0);
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::SUCCESS);
    OvercommitFilter::SetConfig(1.0);
}

// boundary: used + request exactly at the ceiling passes (not strictly greater)
TEST_F(OvercommitFilterTest, BoundaryExactlyAtCeiling)
{
    OvercommitFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(41240);
    AddResident(unit, "r1", 8192, 32768);
    // remaining ceiling = 41240 - 32768 = 8472; request ceiling exactly 8472
    auto inst = MakeInstance(1024, 8472);
    auto out = filter.Filter(ctx, inst, unit);
    EXPECT_EQ(out.status, StatusCode::SUCCESS);
}

// pending pre-allocation on the same unit adds its booked value to the ceiling sum
TEST_F(OvercommitFilterTest, PendingAllocationCounted)
{
    OvercommitFilter filter;
    auto ctx = std::make_shared<PreAllocatedContext>();
    auto unit = MakeUnit(41240);
    unit.set_id("unit-1");
    AddResident(unit, "r1", 8192, 32768);
    auto &pend = ctx->allocated["unit-1"];
    (*pend.resource.mutable_resources())[view_utils::RESOURCE_MEM_NAME].mutable_scalar()->set_value(8192);
    auto inst = MakeInstance(1024, 4120);
    // 32768 + 8192(pending) + 4120 = 45080 > 41240
    EXPECT_EQ(filter.Filter(ctx, inst, unit).status, StatusCode::RESOURCE_NOT_ENOUGH);
}
}  // namespace functionsystem::test
