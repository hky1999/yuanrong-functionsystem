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
#include <gmock/gmock-actions.h>
#include <gmock/gmock.h>
#include "runtime_manager/metrics/collector/node_cpu_utilization_collector.h"

namespace functionsystem::test {
class MockProcFSTools : public ProcFSTools {
public:
    MOCK_METHOD(litebus::Option<std::string>, Read, (const std::string &path), (override));
};

class NodeCPUUtilizationCollectorTest : public ::testing::Test {};


TEST_F(NodeCPUUtilizationCollectorTest, FirstUnknownThenAdjacentSampleWithoutWaiting)
{
    auto tools = std::make_shared<MockProcFSTools>();
    EXPECT_CALL(*tools, Read("/sys/devices/system/cpu/online"))
        .WillRepeatedly(testing::Return(litebus::Option<std::string>{"0-3\n"}));
    EXPECT_CALL(*tools, Read("/proc/stat"))
        .WillRepeatedly(testing::Return(litebus::Option<std::string>{"cpu 100 0 0 100 0 0 0 0\n"}));
    runtime_manager::NodeCPUUtilizationCollector collector(tools);
    const auto start = std::chrono::steady_clock::now();
    auto first = collector.GetUsage();
    ASSERT_TRUE(first.IsOK());
    EXPECT_TRUE(first.Get().value.IsNone());
    auto second = collector.GetUsage();
    ASSERT_TRUE(second.IsOK());
    ASSERT_TRUE(second.Get().value.IsSome());
    EXPECT_DOUBLE_EQ(second.Get().value.Get(), 0.0);
    EXPECT_GT(second.Get().sampleStartUnixMs, 0);
    EXPECT_LT(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count(), 0.1);
}

TEST_F(NodeCPUUtilizationCollectorTest, MissingAndRegressingSamplesBreakTheDeltaChain)
{
    auto tools = std::make_shared<MockProcFSTools>();
    EXPECT_CALL(*tools, Read("/sys/devices/system/cpu/online"))
        .WillRepeatedly(testing::Return(litebus::Option<std::string>{"0\n"}));
    EXPECT_CALL(*tools, Read("/proc/stat"))
        .WillOnce(testing::Return(litebus::Option<std::string>{"cpu 100 0 0 100 0 0 0 0\n"}))
        .WillOnce(testing::Return(litebus::Option<std::string>{}))
        .WillOnce(testing::Return(litebus::Option<std::string>{"cpu 100 0 0 100 0 0 0 0\n"}))
        .WillOnce(testing::Return(litebus::Option<std::string>{"cpu 90 0 0 100 0 0 0 0\n"}))
        .WillOnce(testing::Return(litebus::Option<std::string>{"cpu 90 0 0 100 0 0 0 0\n"}));
    runtime_manager::NodeCPUUtilizationCollector collector(tools);
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(collector.GetUsage().Get().value.IsNone());
    }
    EXPECT_TRUE(collector.GetUsage().Get().value.IsSome());
}

TEST_F(NodeCPUUtilizationCollectorTest, MissingToolsIsUnknown)
{
    runtime_manager::NodeCPUUtilizationCollector collector(nullptr);
    EXPECT_TRUE(collector.GetUsage().Get().value.IsNone());
}
}
