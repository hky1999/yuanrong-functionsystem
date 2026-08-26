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
#include "common/schedule_plugin/prefilter/default_prefilter/default_prefilter.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/resource_view/view_utils.h"
#include "common/schedule_plugin/common/plugin_utils.h"
#include "common/schedule_plugin/common/preallocated_context.h"

namespace functionsystem::test::schedule_plugin::prefilter {
using std::pair;
using std::string;
using std::vector;
using namespace ::testing;
using namespace schedule_framework;
class DefaultPrefilterTest : public Test {};

/**
 * Description: Test PreFilterWithInvalidParam
 * Steps:
 * 1. input instance with nullptr ctx  -> return ERR_INNER_SYSTEM_ERROR
 * 2. input instance with invalid resource -> return INVALID_RESOURCE_PARAMETER
 */
TEST_F(DefaultPrefilterTest, PreFilterWithInvalidParam)
{
    functionsystem::schedule_plugin::prefilter::DefaultPreFilter filter;
    auto errPreAllocated = std::make_shared<ScheduleContext>();
    auto correctPreAllocated = std::make_shared<PreAllocatedContext>();
    resource_view::ResourceUnit unit;
    resource_view::InstanceInfo ins;

    // input instance with  nullptr ctx  -> return ERR_INNER_SYSTEM_ERROR
    {
        auto filterRet = filter.PreFilter(nullptr, ins, unit);
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::ERR_INNER_SYSTEM_ERROR);
        EXPECT_STREQ(filterRet->status().GetMessage().c_str(), "[Invalid Schedule Context]");
    }
    // input instance with invalid resource -> return INVALID_RESOURCE_PARAMETER
    {
        auto filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::INVALID_RESOURCE_PARAMETER);
        EXPECT_STREQ(filterRet->status().GetMessage().c_str(), "[Invalid Instance Resource Value]");
    }
}

/**
 * Description: Test CommonPreFilter
 * Steps:
 * 1. input instance is not MONOPOLY_MODE and ResourceUnit.fragment is empty  -> return RESOURCE_NOT_ENOUGH
 * 2. input instance with ResourceUnit.fragment is not empty  -> return OK and all fragment
 */
TEST_F(DefaultPrefilterTest, CommonPreFilterTest)
{
    functionsystem::schedule_plugin::prefilter::DefaultPreFilter filter;
    auto correctPreAllocated = std::make_shared<PreAllocatedContext>();
    resource_view::ResourceUnit unit;
    resource_view::InstanceInfo ins;

    // input instance is not MONOPOLY_MODE and ResourceUnit.fragment is empty  -> return RESOURCE_NOT_ENOUGH
    {
        ins = GetInstance("instance1", "shared", 512, 500);
        auto filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(filterRet->status().GetMessage().c_str(), "[No Resource In Cluster]");

        ins = GetInstance("instance1", "", 512, 500);
        filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(filterRet->status().GetMessage().c_str(), "[No Resource In Cluster]");

        ins = GetInstance("instance1", "alter", 512, 500);
        filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(filterRet->status().GetMessage().c_str(), "[No Resource In Cluster]");
    }
    // input instance with invalid resource -> return INVALID_RESOURCE_PARAMETER
    {
        ins = GetInstance("instance1", "shared", 512, 500);
        unit = GetNewLocalResourceUnit();
        auto filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::SUCCESS);
        int cnt = 0;
        auto frag = unit.fragment();
        while (!filterRet->end()) {
            EXPECT_TRUE(frag.find(filterRet->current()) != frag.end());
            cnt++;
            filterRet->next();
        }
        EXPECT_TRUE(cnt == static_cast<int>(frag.size()));
    }
}

/**
 * Description: Test PrecisePreFilter
 * Steps:
 * 1. input instance is not MONOPOLY_MODE and ResourceUnit.fragment is empty  -> return RESOURCE_NOT_ENOUGH
 * 2. input instance with ResourceUnit.fragment is not empty  -> return OK and all fragment
 */
TEST_F(DefaultPrefilterTest, PrecisePreFilter)
{
    functionsystem::schedule_plugin::prefilter::DefaultPreFilter filter;
    auto correctPreAllocated = std::make_shared<PreAllocatedContext>();
    resource_view::ResourceUnit unit;
    resource_view::InstanceInfo ins;

    // input instance is MONOPOLY_MODE and no bucketindexs in ResourceView -> return RESOURCE_NOT_ENOUGH
    {
        ins = GetInstance("instance1", "monopoly", 512, 500);
        auto filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_STREQ(filterRet->status().GetMessage().c_str(), "[No Resource In Cluster]");
    }
    // input instance is MONOPOLY_MODE and cpu is 0 -> return INVALID_RESOURCE_PARAMETER
    {
        ins = GetInstance("instance1", "monopoly", 512, 0);
        unit = GetNewLocalResourceUnit();
        auto filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::INVALID_RESOURCE_PARAMETER);
        std::string errMsg = "[Invalid CPU: 0.000000]";
        EXPECT_EQ(filterRet->status().GetMessage(), errMsg);
    }
    // input instance with no proportion bucketIndex in ResourceView-> return RESOURCE_NOT_ENOUGH
    {
        ins = GetInstance("instance1", "monopoly", 512, 500);
        unit = GetNewLocalResourceUnit(true, false, false, 1);
        auto filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        std::string errMsg = "[(500, 512) Not Found]";
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_EQ(filterRet->status().GetMessage(), errMsg);
    }
    // input instance with no mem bucketInfo in ResourceView-> return RESOURCE_NOT_ENOUGH
    {
        ins = GetInstance("instance1", "monopoly", 512, 500);
        unit = GetNewLocalResourceUnit(true, true, false, 1);
        auto filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        std::string errMsg = "[(500, 512) Not Found]";
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_EQ(filterRet->status().GetMessage(), errMsg);
    }
    // input instance with monopoly num is 0 in ResourceView-> return RESOURCE_NOT_ENOUGH
    {
        ins = GetInstance("instance1", "monopoly", 512, 500);
        unit = GetNewLocalResourceUnit(true, true, true, 0);
        auto filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        std::string errMsg = "[(500, 512) Not Enough]";
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
        EXPECT_EQ(filterRet->status().GetMessage(), errMsg);
    }

    // input instance and get BucketInfo successfully -> return SUCCESS
    {
        ins = GetInstance("instance1", "monopoly", 512, 500);
        unit = GetNewLocalResourceUnit(true, true, true, 1);
        auto filterRet = filter.PreFilter(correctPreAllocated, ins, unit);
        EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::SUCCESS);
        int cnt = 0;
        const auto &bucketIndexes(unit.bucketindexs());
        auto bucketIndexesIter(bucketIndexes.find("1.024000"));
        const auto &buckets(bucketIndexesIter->second.buckets());
        auto bucketsIter(buckets.find("512.000000"));
        auto allocatable = bucketsIter->second.allocatable();
        while (!filterRet->end()) {
            EXPECT_TRUE(allocatable.find(filterRet->current()) != allocatable.end());
            cnt++;
            filterRet->next();
        }
        EXPECT_TRUE(cnt == static_cast<int>(allocatable.size()));
    }
}

TEST_F(DefaultPrefilterTest, FailureRequirementDoesNotNarrowToInt32)
{
    constexpr double memory = 2147483648.0;
    functionsystem::schedule_plugin::prefilter::DefaultPreFilter filter;
    auto preAllocated = std::make_shared<PreAllocatedContext>();
    auto unit = GetNewLocalResourceUnit(true, false, false, 1);
    auto ins = GetInstance("instance1", "monopoly", memory, 500);

    auto filterRet = filter.PreFilter(preAllocated, ins, unit);

    EXPECT_EQ(filterRet->status().StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_EQ(filterRet->status().GetMessage(), "[(500, 2147483648) Not Found]");
}

}  // namespace functionsystem::test::schedule_plugin::prefilter
