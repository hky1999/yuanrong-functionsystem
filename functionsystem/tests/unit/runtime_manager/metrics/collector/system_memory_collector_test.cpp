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
#include "runtime_manager/metrics/collector/system_memory_collector.h"

namespace functionsystem::test {

class MockProcFSTools : public ProcFSTools {
public:
    MOCK_METHOD(litebus::Option<std::string>, Read, (const std::string &path), (override));
};

class SystemMemoryCollectorTest : public ::testing::Test {};

/**
 * Feature: SystemMemoryCollector
 * Description: Generate filter
 * Steps:
 * Expectation:
 * system-memory
 */
TEST_F(SystemMemoryCollectorTest, GenFilter)
{
    auto collector = std::make_shared<runtime_manager::SystemMemoryCollector>();
    EXPECT_EQ(collector->GenFilter(), "system-Memory");
}

/**
 * Feature: SystemMemoryCollector
 * Description: Get Limit
 * Steps:
 * Expectation:
 */
TEST_F(SystemMemoryCollectorTest, GetLimit)
{
    auto tools = std::make_shared<MockProcFSTools>();
    EXPECT_CALL(*tools.get(), Read)
        .WillOnce(testing::Return(litebus::Option<std::string>{
            "1051648"
        }));

    auto collector = std::make_shared<runtime_manager::SystemMemoryCollector>(tools);
    auto limit = collector->GetLimit();
    EXPECT_EQ(limit.value, 1.0029296875);
    EXPECT_EQ(limit.instanceID.IsNone(), true);
}

/**
 * Feature: SystemMemoryCollector
 * Description: Get Limit
 * Steps: Give empty content
 * Expectation:
 * {}
 */
TEST_F(SystemMemoryCollectorTest, GetLimitWithEmptyContent)
{
    auto tools = std::make_shared<MockProcFSTools>();
    // cgroup v1 path absent AND meminfo unreadable: no signal at all
    EXPECT_CALL(*tools.get(), Read(runtime_manager::system_metrics::MEMORY_LIMIT_PATH))
        .WillOnce(testing::Return(litebus::Option<std::string>{}));
    EXPECT_CALL(*tools.get(), Read(runtime_manager::system_metrics::MEMINFO_PATH))
        .WillRepeatedly(testing::Return(litebus::Option<std::string>{}));

    auto collector = std::make_shared<runtime_manager::SystemMemoryCollector>(tools);
    auto limit = collector->GetLimit();
    EXPECT_EQ(limit.value.IsNone(), true);
    EXPECT_EQ(limit.instanceID.IsNone(), true);
}

/**
 * Feature: SystemMemoryCollector
 * Description: Get Limit on a cgroup v2 (unified) host
 * Steps: cgroup v1 path absent, /proc/meminfo readable
 * Expectation: limit = MemTotal (in MB)
 */
TEST_F(SystemMemoryCollectorTest, GetLimitMeminfoFallback)
{
    auto tools = std::make_shared<MockProcFSTools>();
    EXPECT_CALL(*tools.get(), Read(runtime_manager::system_metrics::MEMORY_LIMIT_PATH))
        .WillOnce(testing::Return(litebus::Option<std::string>{}));
    EXPECT_CALL(*tools.get(), Read(runtime_manager::system_metrics::MEMINFO_PATH))
        .WillRepeatedly(testing::Return(litebus::Option<std::string>{
            "MemTotal:       63390720 kB\nMemFree:         1234 kB\nMemAvailable:    60467200 kB\n"}));

    auto collector = std::make_shared<runtime_manager::SystemMemoryCollector>(tools);
    auto limit = collector->GetLimit();
    ASSERT_EQ(limit.value.IsNone(), false);
    EXPECT_DOUBLE_EQ(limit.value.Get(), 61905.0); // 63390720 kB / 1024
    EXPECT_EQ(limit.instanceID.IsNone(), true);
}

/**
 * Feature: SystemMemoryCollector
 * Description: Get Usage on a cgroup v2 (unified) host
 * Steps: cgroup v1 path absent, /proc/meminfo readable
 * Expectation: usage = MemTotal - MemAvailable (in MB)
 */
TEST_F(SystemMemoryCollectorTest, GetUsageMeminfoFallback)
{
    auto tools = std::make_shared<MockProcFSTools>();
    EXPECT_CALL(*tools.get(), Read(runtime_manager::system_metrics::MEMORY_USAGE_PATH))
        .WillOnce(testing::Return(litebus::Option<std::string>{}));
    EXPECT_CALL(*tools.get(), Read(runtime_manager::system_metrics::MEMINFO_PATH))
        .WillRepeatedly(testing::Return(litebus::Option<std::string>{
            "MemTotal:       63390720 kB\nMemAvailable:    60467200 kB\n"}));

    auto collector = std::make_shared<runtime_manager::SystemMemoryCollector>(tools);
    auto usage = collector->GetUsage().Get();
    ASSERT_EQ(usage.value.IsNone(), false);
    EXPECT_DOUBLE_EQ(usage.value.Get(), 61905.0 - 59050.0); // 2855 MB real usage
    EXPECT_EQ(usage.instanceID.IsNone(), true);
}

/**
 * Feature: SystemMemoryCollector
 * Description: Get Usage
 * Steps:
 * Expectation:
 */
TEST_F(SystemMemoryCollectorTest, GetUsage)
{
    auto tools = std::make_shared<MockProcFSTools>();
    EXPECT_CALL(*tools.get(), Read)
        .WillOnce(testing::Return(litebus::Option<std::string>{
            "1051648"
        }));

    auto collector = std::make_shared<runtime_manager::SystemMemoryCollector>(tools);
    auto usage = collector->GetUsage().Get();
    EXPECT_EQ(usage.value, 1.0029296875);
    EXPECT_EQ(usage.instanceID.IsNone(), true);
}

/**
 * Feature: SystemMemoryCollector
 * Description: Get Usage
 * Steps: Give empty content
 * Expectation:
 */
TEST_F(SystemMemoryCollectorTest, GetUsageWithEmptyContent)
{
    auto tools = std::make_shared<MockProcFSTools>();
    // cgroup v1 path absent AND meminfo unreadable: no signal at all
    EXPECT_CALL(*tools.get(), Read(runtime_manager::system_metrics::MEMORY_USAGE_PATH))
        .WillOnce(testing::Return(litebus::Option<std::string>{}));
    EXPECT_CALL(*tools.get(), Read(runtime_manager::system_metrics::MEMINFO_PATH))
        .WillRepeatedly(testing::Return(litebus::Option<std::string>{}));

    auto collector = std::make_shared<runtime_manager::SystemMemoryCollector>(tools);
    auto usage = collector->GetUsage().Get();
    EXPECT_EQ(usage.value.IsNone(), true);
    EXPECT_EQ(usage.instanceID.IsNone(), true);
}

}