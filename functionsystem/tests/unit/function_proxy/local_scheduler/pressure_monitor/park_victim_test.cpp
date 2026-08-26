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

#include "local_scheduler/pressure_monitor/park_victim.h"

#include <algorithm>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace functionsystem::test {

using namespace local_scheduler;

namespace {

/**
 * Feature: the lowest-priority instance is parked first regardless of
 * reclaim size.
 * Steps: high-priority instance with the largest use vs low-priority one.
 * Expectation: the low-priority instance is selected.
 */
TEST(ParkVictimTest, LowestPriorityWinsOverLargerReclaim)
{
    std::vector<ParkCandidate> candidates = { { "p-high", 9, 3072.0 }, { "p-low", 1, 1536.0 } };
    auto victim = SelectParkVictim(std::move(candidates));
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(victim->instanceID, "p-low");
}

/**
 * Feature: same priority degrades to the historical largest-reclaim order.
 * Steps: three priority-0 instances with increasing use.
 * Expectation: the largest use is selected.
 */
TEST(ParkVictimTest, SamePriorityPicksLargestReclaim)
{
    std::vector<ParkCandidate> candidates = { { "a", 0, 512.0 }, { "b", 0, 4096.0 }, { "c", 0, 1024.0 } };
    auto victim = SelectParkVictim(std::move(candidates));
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(victim->instanceID, "b");
}

/**
 * Feature: an empty candidate set parks nothing.
 * Expectation: nullopt.
 */
TEST(ParkVictimTest, EmptyCandidatesParksNothing)
{
    auto victim = SelectParkVictim({});
    EXPECT_FALSE(victim.has_value());
}

/**
 * Feature: a single candidate is returned as-is.
 * Expectation: that instance with its fields intact.
 */
TEST(ParkVictimTest, SingleCandidateIsReturned)
{
    auto victim = SelectParkVictim({ { "solo", 4, 256.0 } });
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(victim->instanceID, "solo");
    EXPECT_EQ(victim->priority, 4);
    EXPECT_DOUBLE_EQ(victim->useMb, 256.0);
}

/**
 * Feature: negative priorities park before zero (unset) ones.
 * Steps: priority 0 and priority -3.
 * Expectation: the -3 instance is selected.
 */
TEST(ParkVictimTest, NegativePriorityParksFirst)
{
    std::vector<ParkCandidate> candidates = { { "unset", 0, 8192.0 }, { "batch", -3, 64.0 } };
    auto victim = SelectParkVictim(std::move(candidates));
    ASSERT_TRUE(victim.has_value());
    EXPECT_EQ(victim->instanceID, "batch");
}

/**
 * Feature: repeatedly selecting (park one, drop it, select again) drains in
 * priority order — a high-priority instance is only touched when nothing
 * cheaper is left.
 * Steps: sort a 3-instance field by popping the selection each round.
 * Expectation: order low -> mid -> high.
 */
TEST(ParkVictimTest, RepeatedSelectionDrainsInPriorityOrder)
{
    std::vector<ParkCandidate> candidates = { { "mid", 5, 100.0 }, { "high", 9, 999.0 }, { "low", 1, 50.0 } };
    std::vector<std::string> order;
    while (!candidates.empty()) {
        auto victim = SelectParkVictim(candidates);
        ASSERT_TRUE(victim.has_value());
        order.push_back(victim->instanceID);
        candidates.erase(std::remove_if(candidates.begin(), candidates.end(),
                                        [&](const ParkCandidate &c) { return c.instanceID == victim->instanceID; }),
                         candidates.end());
    }
    EXPECT_EQ((std::vector<std::string>{ "low", "mid", "high" }), order);
}

}  // namespace

}  // namespace functionsystem::test
