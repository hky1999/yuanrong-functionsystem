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

#include "runtime_manager/metrics/collector/external_system_collector.h"
#include <gtest/gtest.h>

namespace functionsystem::test {
using runtime_manager::ParseExternalNodeResource;

TEST(ExternalNodeResourceParseTest, KnownSampleParsesUsedAndTotal)
{
    auto s = ParseExternalNodeResource(
        R"({"cpu":99,"mem":1000,"totalCpu":104,"totalMem":209715200,"usedCpu":2.5,"usedMem":8388608,"usedKnown":true})");
    EXPECT_TRUE(s.known);
    EXPECT_DOUBLE_EQ(s.cpuUsedCores, 2.5);
    EXPECT_DOUBLE_EQ(s.cpuTotalCores, 104.0);
    EXPECT_DOUBLE_EQ(s.memUsedMb, 8.0);
    EXPECT_DOUBLE_EQ(s.memTotalMb, 200.0);
}

TEST(ExternalNodeResourceParseTest, ZeroUsageIsKnownNotUnknown)
{
    auto s = ParseExternalNodeResource(
        R"({"cpu":99,"mem":1000,"totalCpu":104,"totalMem":209715200,"usedCpu":0,"usedMem":0,"usedKnown":true})");
    EXPECT_TRUE(s.known);
    EXPECT_DOUBLE_EQ(s.cpuUsedCores, 0.0);
    EXPECT_DOUBLE_EQ(s.memUsedMb, 0.0);
}

TEST(ExternalNodeResourceParseTest, UnknownFlagMeansNoSample)
{
    auto s = ParseExternalNodeResource(
        R"({"cpu":99,"mem":1000,"totalCpu":104,"totalMem":209715200,"usedCpu":0,"usedMem":0,"usedKnown":false})");
    EXPECT_FALSE(s.known);
}

TEST(ExternalNodeResourceParseTest, LegacyAvailableOnlyIsNeverUsage)
{
    // Old server shape: cpu/mem are AVAILABLE. The parser must not invent a
    // usage sample from them.
    auto s = ParseExternalNodeResource(R"({"cpu":99,"mem":192199786496})");
    EXPECT_FALSE(s.known);
}

TEST(ExternalNodeResourceParseTest, PartialFieldsAreUnknown)
{
    auto s = ParseExternalNodeResource(
        R"({"totalCpu":104,"totalMem":209715200,"usedCpu":2.5,"usedKnown":true})");
    EXPECT_FALSE(s.known);
    (void)s;
    auto garbage = ParseExternalNodeResource("not-json");
    EXPECT_FALSE(garbage.known);
    auto array = ParseExternalNodeResource("[1,2,3]");
    EXPECT_FALSE(array.known);
}

}  // namespace functionsystem::test
