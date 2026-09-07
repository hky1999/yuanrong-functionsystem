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

#include "runtime_manager/executor/sandboxd/sandbox_stats_polls.h"
#include <gtest/gtest.h>

namespace functionsystem::test {
using runtime_manager::SandboxStatsPolls;

TEST(SandboxStatsPollsTest, DuplicateTimerAndCompletionCannotCreateTwoPollChains)
{
    SandboxStatsPolls polls;
    const auto first = polls.Start("runtime");
    EXPECT_FALSE(polls.Complete("runtime", first).has_value());
    ASSERT_TRUE(polls.Begin("runtime", first));
    EXPECT_FALSE(polls.Begin("runtime", first));
    const auto next = polls.Complete("runtime", first);
    ASSERT_TRUE(next.has_value());
    EXPECT_FALSE(polls.Complete("runtime", first).has_value());
    EXPECT_FALSE(polls.Begin("runtime", first));
    EXPECT_TRUE(polls.Begin("runtime", *next));
}

TEST(SandboxStatsPollsTest, DeletedRuntimeAndReusedIdentityRejectOldCallbacks)
{
    SandboxStatsPolls polls;
    const auto old = polls.Start("runtime");
    ASSERT_TRUE(polls.Begin("runtime", old));
    polls.Stop("runtime");
    EXPECT_FALSE(polls.Complete("runtime", old).has_value());
    const auto restored = polls.Start("runtime");
    EXPECT_NE(old, restored);
    ASSERT_TRUE(polls.Begin("runtime", restored));
    EXPECT_FALSE(polls.Complete("runtime", old).has_value());
    EXPECT_FALSE(polls.Begin("runtime", old));
    EXPECT_TRUE(polls.Complete("runtime", restored).has_value());
}

TEST(SandboxStatsPollsTest, RestartInvalidatesAnAlreadyScheduledTimer)
{
    SandboxStatsPolls polls;
    const auto oldTimer = polls.Start("runtime");
    const auto newTimer = polls.Start("runtime");
    EXPECT_FALSE(polls.Begin("runtime", oldTimer));
    EXPECT_TRUE(polls.Begin("runtime", newTimer));
}

TEST(SandboxStatsPollsTest, SlowInstanceDoesNotBlockOtherInstances)
{
    SandboxStatsPolls polls;
    const auto slow = polls.Start("slow");
    const auto fast = polls.Start("fast");
    ASSERT_TRUE(polls.Begin("slow", slow));
    ASSERT_TRUE(polls.Begin("fast", fast));
    const auto nextFast = polls.Complete("fast", fast);
    ASSERT_TRUE(nextFast.has_value());
    EXPECT_TRUE(polls.Begin("fast", *nextFast));
    EXPECT_FALSE(polls.Begin("slow", slow));
    EXPECT_FALSE(polls.Complete("slow", fast).has_value());
    EXPECT_TRUE(polls.Complete("slow", slow).has_value());
}

TEST(SandboxStatsPollsTest, CadenceAccountsForRpcTimeWithoutCatchupBursts)
{
    EXPECT_EQ(SandboxStatsPolls::NextDelayMs(0, 1000), 1000);
    EXPECT_EQ(SandboxStatsPolls::NextDelayMs(40, 1000), 960);
    EXPECT_EQ(SandboxStatsPolls::NextDelayMs(1000, 1000), 100);
    EXPECT_EQ(SandboxStatsPolls::NextDelayMs(5000, 1000), 100);
    EXPECT_EQ(SandboxStatsPolls::NextDelayMs(-1, 1000), 1000);
    EXPECT_EQ(SandboxStatsPolls::NextDelayMs(std::numeric_limits<int64_t>::max(), 1000), 100);
}
}  // namespace functionsystem::test
