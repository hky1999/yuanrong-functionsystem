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

#include <gtest/gtest.h>
#include <map>
#include "runtime_manager/metrics/collector/node_cpu_usage_window.h"
#include "common/resource_view/resource_sample_freshness.h"

namespace functionsystem::test {
using runtime_manager::NodeCPUUsageWindow;
TEST(NodeCPUUsageWindowTest, OnlineRangesAreCountedWithoutExpansionOrOverlap)
{
    EXPECT_EQ(runtime_manager::CountOnlineCPUs("0-31\n"), 32u);
    EXPECT_EQ(runtime_manager::CountOnlineCPUs("0-3,8,10-11"), 7u);
    for (const auto &text : {"", "0-", "1-0", "0-3,3-4", "0,0", "0,", "-1", "0-4294967296", "cpu 32"}) {
        EXPECT_FALSE(runtime_manager::CountOnlineCPUs(text));
    }
}
TEST(NodeCPUUsageWindowTest, ReportsActualWindowMillicoresWithoutGuestDoubleCounting)
{
    NodeCPUUsageWindow window;
    EXPECT_FALSE(window.Observe("cpu 100 0 0 100 0 0 0 0 80 0", 1000000000, 1000, 100));
    // 100 user + 50 system + 50 steal ticks in two seconds = one core.
    auto value = window.Observe("cpu 200 0 50 300 50 0 0 50 180 0\ncpu0 ignored", 3000000000, 3000, 100);
    ASSERT_TRUE(value);
    EXPECT_DOUBLE_EQ(value->millicores, 1000);
    EXPECT_EQ(value->startUnixMs, 1000);
    EXPECT_EQ(value->durationNs, 2000000000);
}
TEST(NodeCPUUsageWindowTest, IdleIsKnownZeroAndCounterResetIsUnknown)
{
    NodeCPUUsageWindow window;
    window.Observe("cpu 100 0 0 100 0 0 0 0", 1000000000, 1000, 100);
    auto idle = window.Observe("cpu 100 0 0 200 0 0 0 0", 2000000000, 2000, 100);
    ASSERT_TRUE(idle);
    EXPECT_DOUBLE_EQ(idle->millicores, 0);
    EXPECT_FALSE(window.Observe("cpu 90 0 50 300 0 0 0 0", 3000000000, 3000, 100));
    auto reset = window.Observe("cpu 190 0 50 400 0 0 0 0", 4000000000, 4000, 100);
    ASSERT_TRUE(reset);
    EXPECT_DOUBLE_EQ(reset->millicores, 1000);
}
TEST(NodeCPUUsageWindowTest, InvalidReadBreaksTheSampleChain)
{
    for (const auto &text : {"", "cpu0 1 2 3 4 5 6 7 8", "cpu 1 2 3 4", "cpu -1 0 0 0 0 0 0 0",
                             "cpu 1x 0 0 0 0 0 0 0", "cpu 18446744073709551616 0 0 0 0 0 0 0"}) {
        NodeCPUUsageWindow window;
        window.Observe("cpu 0 0 0 0 0 0 0 0", 1000000000, 1000, 100);
        EXPECT_FALSE(window.Observe(text, 2000000000, 2000, 100));
        EXPECT_FALSE(window.Observe("cpu 100 0 0 100 0 0 0 0", 3000000000, 3000, 100));
    }
}
TEST(NodeCPUUsageWindowTest, RejectsStaleAndReversedClocks)
{
    NodeCPUUsageWindow window;
    window.Observe("cpu 0 0 0 0 0 0 0 0", 1000000000, 1000, 100);
    EXPECT_FALSE(window.Observe("cpu 100 0 0 100 0 0 0 0", 32000000000, 32000, 100));
    EXPECT_FALSE(window.Observe("cpu 200 0 0 200 0 0 0 0", 33000000000, 31000, 100));
    EXPECT_FALSE(window.Observe("cpu 300 0 0 300 0 0 0 0", 33000000000, 33000, 100));
    EXPECT_FALSE(window.Observe("cpu 400 0 0 400 0 0 0 0", 34000000000, 34000, 0));
}
TEST(NodeCPUUsageWindowTest, RejectsSummedCounterOverflow)
{
    NodeCPUUsageWindow window;
    window.Observe("cpu 0 0 0 0 0 0 0 0", 1000000000, 1000, 100);
    EXPECT_FALSE(window.Observe("cpu 18446744073709551615 1 0 0 0 0 0 0", 2000000000, 2000, 100));
}
struct CPUFreshnessUnit {
    int64_t stamp = 0;
    std::map<std::string, CPUFreshnessUnit> leaves;
    int64_t cpu_sample_unix_ms() const { return stamp; }
    const auto &fragment() const { return leaves; }
};
TEST(NodeCPUUsageWindowTest, AggregateRequiresEveryCPUWindowFresh)
{
    using resource_view::UsableNodeCPUSampleTime;
    CPUFreshnessUnit root;
    root.leaves["a"].stamp = 900;
    root.leaves["b"].stamp = 800;
    EXPECT_EQ(UsableNodeCPUSampleTime(root, 1000, 200), 800);
    root.leaves["b"].stamp = 0;
    EXPECT_EQ(UsableNodeCPUSampleTime(root, 1000, 200), 0);
    root.leaves["b"].stamp = 799;
    EXPECT_EQ(UsableNodeCPUSampleTime(root, 1000, 200), 0);
    root.leaves["b"].stamp = 1001;
    EXPECT_EQ(UsableNodeCPUSampleTime(root, 1000, 200), 0);
}
}
