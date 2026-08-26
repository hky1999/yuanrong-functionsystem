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

#include "common/schedule_plugin/filter/default_filter/default_filter.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/resource_view/view_utils.h"
#include "common/schedule_plugin/common/plugin_utils.h"
#include "common/schedule_plugin/common/preallocated_context.h"

namespace functionsystem::test::schedule_plugin::filter {
using namespace ::testing;
using namespace schedule_framework;
class DefaultFilterTest : public Test {};

/**
 * Description: Test default filter with MonopolyFilter is Error
 * Steps:
 * 2. MONOPOLY_MODE, pod is selected in context -> RESOURCE_NOT_ENOUGH
 * 3. MONOPOLY_MODE, pod resource is not match precisely -> RESOURCE_NOT_ENOUGH
 * 4. MONOPOLY_MODE, instance cpu is very small -> INVALID_RESOURCE_PARAMETER
 * 5. MONOPOLY_MODE, total monopoly num is 0 -> RESOURCE_NOT_ENOUGH
 * 6. MONOPOLY_MODE, total monopoly num is 1 -> SUCCESS
 */
TEST_F(DefaultFilterTest, MonopolyFilterTest)
{
    resource_view::ResourceUnit unit;
    resource_view::InstanceInfo ins = GetInstance("instance1", "monopoly", 512, 500);
    functionsystem::schedule_plugin::filter::DefaultFilter filter;
    auto preAllocated = std::make_shared<PreAllocatedContext>();

    // MONOPOLY_MODE, pod is selected in context -> RESOURCE_NOT_ENOUGH
    unit.set_status(0);
    preAllocated->preAllocatedSelectedFunctionAgentSet.insert(unit.id());
    {
        auto res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[(500, 512) Already Allocated To Other]");
        EXPECT_EQ(res.availableForRequest, -1);
    }
    // MONOPOLY_MODE, pod resource is not match precisely -> RESOURCE_NOT_ENOUGH
    preAllocated->preAllocatedSelectedFunctionAgentSet.erase(unit.id());
    {
        unit = GetAgentResourceUnit(1000, 512, 1);
        auto res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[(500, 512) Don't Match Precisely]");
        EXPECT_EQ(res.availableForRequest, -1);

        unit = GetAgentResourceUnit(500, 500, 1);
        res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[(500, 512) Don't Match Precisely]");
        EXPECT_EQ(res.availableForRequest, -1);
    }
    // MONOPOLY_MODE, instance cpu is very small -> INVALID_RESOURCE_PARAMETER
    {
        unit = GetAgentResourceUnit(0, 512, 1);
        ins = GetInstance("instance1", "monopoly", 512, 0);
        auto res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::INVALID_RESOURCE_PARAMETER);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[Invalid CPU: 0.000000]");
        EXPECT_EQ(res.availableForRequest, -1);
    }
    // MONOPOLY_MODE, total monopoly num is 0 -> RESOURCE_NOT_ENOUGH
    {
        unit = GetAgentResourceUnit(500, 512, 0);
        ins = GetInstance("instance1", "monopoly", 512, 500);
        auto res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[(500, 512) Not Enough]");
        EXPECT_EQ(res.availableForRequest, -1);
    }
    // MONOPOLY_MODE, total monopoly num is 1 -> SUCCESS
    {
        unit = GetAgentResourceUnit(500, 512, 1);
        ins = GetInstance("instance1", "monopoly", 512, 500);
        auto res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::SUCCESS);
        EXPECT_EQ(res.availableForRequest, 1);
    }
}

/**
 * Description: Test default filter with ResourceFilter
 * precondition: MonopolyFilter Return SUCCESS or schedule policy is not monopoly
 * 1. current resource - used resource and resource is invalid -> RESOURCE_NOT_ENOUGH
 * 2. instance request resource not found in unit  --> PARAMETER_ERROR
 * 3. instance request resource > unit capacity --> RESOURCE_NOT_ENOUGH
 * 4. instance request resource > unit available --> RESOURCE_NOT_ENOUGH
 * 5. else return SUCCESS
 */
TEST_F(DefaultFilterTest, ResourceFilterTest)
{
    resource_view::ResourceUnit unit;
    resource_view::InstanceInfo ins;
    functionsystem::schedule_plugin::filter::DefaultFilter filter;
    auto preAllocated = std::make_shared<PreAllocatedContext>();

    // current resource - used resource and resource is invalid -> RESOURCE_NOT_ENOUGH
    {
        unit = GetAgentResourceUnit(500, 512, 1);
        ins = GetInstance("instance1", "shared", 512, 500);
        resource_view::Resources rs = view_utils::GetCpuMemResources();
        rs.mutable_resources()->at(resource_view::CPU_RESOURCE_NAME).mutable_scalar()->set_value(512);
        rs.mutable_resources()->at(resource_view::MEMORY_RESOURCE_NAME).mutable_scalar()->set_value(512);
        preAllocated->allocated[unit.id()].resource = std::move(rs);

        auto res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_NE(res.status.GetMessage().find("Not Enough]"), std::string::npos);
        EXPECT_EQ(res.availableForRequest, -1);
    }
    //  instance request resource not found in unit  --> PARAMETER_ERROR
    {
        unit = GetAgentResourceUnit(500, 512, 1);
        ins = GetInstance("instance1", "shared", 512, 500);
        (*ins.mutable_resources()->mutable_resources())["NotFoundResource"] =
            view_utils::GetNameResourceWithValue("NotFoundResource", 100);

        auto res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::PARAMETER_ERROR);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[NotFoundResource: Not Found]");
        EXPECT_STREQ(res.required.c_str(), "NotFoundResource: 100");

        (*unit.mutable_capacity()->mutable_resources())["NotFoundResource"] =
            view_utils::GetNameResourceWithValue("NotFoundResource", 200);

        res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::PARAMETER_ERROR);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[NotFoundResource: Not Found]");
        EXPECT_STREQ(res.required.c_str(), "NotFoundResource: 100");
        EXPECT_EQ(res.availableForRequest, -1);
    }
    // instance request resource > unit capacity --> RESOURCE_NOT_ENOUGH
    {
        unit = GetAgentResourceUnit(500, 512, 1);
        ins = GetInstance("instance1", "shared", 512, 1000);
        auto res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[CPU: Out Of Capacity]");
        EXPECT_STREQ(res.required.c_str(), "CPU: 1000m");

        ins = GetInstance("instance1", "shared", 1000, 500);
        res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[Memory: Out Of Capacity]");
        EXPECT_STREQ(res.required.c_str(), "Memory: 1000MB");
        EXPECT_EQ(res.availableForRequest, -1);
    }
    // instance request resource > unit available --> RESOURCE_NOT_ENOUGH
    {
        unit = GetAgentResourceUnit(600, 612, 1);
        ins = GetInstance("instance1", "shared", 512, 500);
        resource_view::Resources rs = view_utils::GetCpuMemResources();
        rs.mutable_resources()->at(resource_view::CPU_RESOURCE_NAME).mutable_scalar()->set_value(200);
        rs.mutable_resources()->at(resource_view::MEMORY_RESOURCE_NAME).mutable_scalar()->set_value(100);
        preAllocated->allocated[unit.id()].resource = rs;
        auto res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[CPU: Not Enough]");
        EXPECT_STREQ(res.required.c_str(), "CPU: 500m");

        rs.mutable_resources()->at(resource_view::CPU_RESOURCE_NAME).mutable_scalar()->set_value(100);
        rs.mutable_resources()->at(resource_view::MEMORY_RESOURCE_NAME).mutable_scalar()->set_value(200);
        preAllocated->allocated[unit.id()].resource = rs;
        res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(res.status.GetMessage().c_str(), "[Memory: Not Enough]");
        EXPECT_STREQ(res.required.c_str(), "Memory: 512MB");
        EXPECT_EQ(res.availableForRequest, -1);
    }
    // filter SUCCESS
    {
        int32_t num = 5;
        unit = GetAgentResourceUnit(500 * num, 512 * num, 1);
        ins = GetInstance("instance1", "shared", 512, 500);
        (*ins.mutable_resources()->mutable_resources())["ZeroResource"] =
            view_utils::GetNameResourceWithValue("ZeroResource", 0);  // test for required number is zero
        auto npuKey = resource_view::NPU_RESOURCE_NAME + "/" + "910" + "/" + resource_view::HETEROGENEOUS_MEM_KEY;
        (*ins.mutable_resources()->mutable_resources())[npuKey] = view_utils::GetNpuResource();  // test for HETERO_RESOURCE

        auto res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::SUCCESS);
        EXPECT_EQ(res.availableForRequest, num);

        resource_view::Resources rs = view_utils::GetCpuMemResources();
        rs.mutable_resources()->at(resource_view::CPU_RESOURCE_NAME).mutable_scalar()->set_value(700);
        rs.mutable_resources()->at(resource_view::MEMORY_RESOURCE_NAME).mutable_scalar()->set_value(100);
        preAllocated->allocated[unit.id()].resource = std::move(rs);

        res = filter.Filter(preAllocated, ins, unit);
        EXPECT_EQ(res.status.StatusCode(), StatusCode::SUCCESS);
        EXPECT_EQ(res.availableForRequest, num - 2);
    }
}

TEST_F(DefaultFilterTest, StorageFailureRequirementUsesMiBWithoutInt32Overflow)
{
    constexpr int64_t bytesPerMiB = 1024 * 1024;
    constexpr int64_t storageMiB = 153600;
    auto unit = GetAgentResourceUnit(500, 512, 1);
    auto ins = GetInstance("instance1", "shared", 512, 500);
    (*ins.mutable_resources()->mutable_resources())["storage"] =
        view_utils::GetNameResourceWithValue("storage", static_cast<double>(storageMiB * bytesPerMiB));
    functionsystem::schedule_plugin::filter::DefaultFilter filter;
    auto preAllocated = std::make_shared<PreAllocatedContext>();

    auto res = filter.Filter(preAllocated, ins, unit);

    EXPECT_EQ(res.status.StatusCode(), StatusCode::PARAMETER_ERROR);
    EXPECT_STREQ(res.status.GetMessage().c_str(), "[storage: Not Found]");
    EXPECT_STREQ(res.required.c_str(), "storage: 153600MiB");
}

TEST_F(DefaultFilterTest, MonopolyFailureRequirementDoesNotNarrowToInt32)
{
    constexpr double memory = 2147483648.0;
    auto unit = GetAgentResourceUnit(500, memory, 1);
    auto ins = GetInstance("instance1", "monopoly", memory, 500);
    functionsystem::schedule_plugin::filter::DefaultFilter filter;
    auto preAllocated = std::make_shared<PreAllocatedContext>();
    preAllocated->preAllocatedSelectedFunctionAgentSet.insert(unit.id());

    auto res = filter.Filter(preAllocated, ins, unit);

    EXPECT_EQ(res.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_STREQ(res.status.GetMessage().c_str(), "[(500, 2147483648) Already Allocated To Other]");
}

}  // namespace functionsystem::test::schedule_plugin::filter
