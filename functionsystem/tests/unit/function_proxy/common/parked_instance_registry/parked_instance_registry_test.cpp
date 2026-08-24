/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "function_proxy/common/parked_instance_registry/parked_instance_registry.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace functionsystem::test {
using function_proxy::ParkedInstanceRegistry;

class ParkedInstanceRegistryTest : public ::testing::Test {
public:
    void SetUp() override
    {
        ParkedInstanceRegistry::Instance().Configure(300, true);
        ParkedInstanceRegistry::Instance().Clear("inst-a");
        ParkedInstanceRegistry::Instance().Clear("inst-b");
    }
};

TEST_F(ParkedInstanceRegistryTest, MarkAndClear)
{
    auto &registry = ParkedInstanceRegistry::Instance();
    ASSERT_FALSE(registry.IsParked("inst-a"));
    registry.MarkParked("inst-a");
    ASSERT_TRUE(registry.IsParked("inst-a"));
    ASSERT_FALSE(registry.IsParked("inst-b"));
    registry.Clear("inst-a");
    ASSERT_FALSE(registry.IsParked("inst-a"));
}

TEST_F(ParkedInstanceRegistryTest, DisabledRegistryNeverMarks)
{
    auto &registry = ParkedInstanceRegistry::Instance();
    registry.Configure(300, false);
    ASSERT_FALSE(registry.Enabled());
    registry.MarkParked("inst-a");
    ASSERT_FALSE(registry.IsParked("inst-a"));
    ASSERT_EQ(registry.Size(), 0U);
    registry.Configure(300, true);
}

TEST_F(ParkedInstanceRegistryTest, HoldWindowExpiry)
{
    auto &registry = ParkedInstanceRegistry::Instance();
    registry.Configure(1, true);  // 1s hold for the test
    registry.MarkParked("inst-a");
    ASSERT_TRUE(registry.IsParked("inst-a"));
    // Wait past the hold window: the entry counts as not parked anymore.
    std::this_thread::sleep_for(std::chrono::milliseconds(1300));
    ASSERT_FALSE(registry.IsParked("inst-a"));
    // Renewal restarts the deadline.
    registry.MarkParked("inst-a");
    ASSERT_TRUE(registry.IsParked("inst-a"));
    registry.Configure(300, true);
}

TEST_F(ParkedInstanceRegistryTest, HoldSecondsEcho)
{
    auto &registry = ParkedInstanceRegistry::Instance();
    registry.Configure(42, true);
    ASSERT_EQ(registry.HoldSeconds(), 42U);
}
}  // namespace functionsystem::test
