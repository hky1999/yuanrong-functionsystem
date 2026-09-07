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
#include <gmock/gmock.h>
#include "runtime_manager/metrics/collector/node_cpu_collector.h"

namespace functionsystem::test {
class MockNodeCPUProcFSTools : public ProcFSTools {
public:
    MOCK_METHOD(litebus::Option<std::string>, Read, (const std::string &path), (override));
};
TEST(NodeCPUCollectorTest, GenFilter)
{
    runtime_manager::NodeCPUCollector collector;
    EXPECT_EQ(collector.GenFilter(), "node-CPU");
}
TEST(NodeCPUCollectorTest, FullOnlineCPUCapacityDoesNotDependOnCpuinfoReadSize)
{
    auto tools = std::make_shared<testing::StrictMock<MockNodeCPUProcFSTools>>();
    EXPECT_CALL(*tools, Read("/sys/devices/system/cpu/online"))
        .WillOnce(testing::Return(litebus::Option<std::string>{"0-31\n"}));
    runtime_manager::NodeCPUCollector collector(tools, 100.0);
    auto limit = collector.GetLimit();
    ASSERT_TRUE(limit.value.IsSome());
    EXPECT_DOUBLE_EQ(limit.value.Get(), 31900.0);
    EXPECT_TRUE(limit.instanceID.IsNone());
}
TEST(NodeCPUCollectorTest, InvalidOrUnavailableCapacityRemainsUnknown)
{
    auto tools = std::make_shared<testing::StrictMock<MockNodeCPUProcFSTools>>();
    EXPECT_CALL(*tools, Read("/sys/devices/system/cpu/online"))
        .WillOnce(testing::Return(litebus::Option<std::string>{}))
        .WillOnce(testing::Return(litebus::Option<std::string>{"0-1,"}))
        .WillOnce(testing::Return(litebus::Option<std::string>{"0-1"}));
    runtime_manager::NodeCPUCollector collector(tools, 2000.0);
    EXPECT_TRUE(collector.GetLimit().value.IsNone());
    EXPECT_TRUE(collector.GetLimit().value.IsNone());
    EXPECT_TRUE(collector.GetLimit().value.IsNone());
    runtime_manager::NodeCPUCollector invalid(tools, -1.0);
    EXPECT_TRUE(invalid.GetLimit().value.IsNone());
}
TEST(NodeCPUCollectorTest, FirstMetricsHasCapacityButUnknownUsage)
{
    auto tools = std::make_shared<testing::StrictMock<MockNodeCPUProcFSTools>>();
    EXPECT_CALL(*tools, Read("/proc/stat"))
        .WillOnce(testing::Return(litebus::Option<std::string>{"cpu 1 2 3 4 5 6 7 8"}));
    EXPECT_CALL(*tools, Read("/sys/devices/system/cpu/online"))
        .WillOnce(testing::Return(litebus::Option<std::string>{"0-1"}));
    runtime_manager::NodeCPUCollector collector(tools, 100.0);
    const auto metrics = collector.GetMetrics().Get();
    EXPECT_DOUBLE_EQ(metrics.limit.Get(), 1900.0);
    EXPECT_TRUE(metrics.usage.IsNone());
    EXPECT_TRUE(metrics.instanceID.IsNone());
}
}
