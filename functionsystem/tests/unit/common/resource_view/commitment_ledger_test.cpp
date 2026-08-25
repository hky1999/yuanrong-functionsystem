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

#include "common/resource_view/commitment_ledger.h"

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

namespace functionsystem::test {

using namespace resource_view;

namespace {

constexpr uint64_t GiB = 1024ULL * 1024ULL * 1024ULL;

class CommitmentLedgerTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        CommitmentLedger::ResetForTest(); // process-global state
    }
};

/**
 * Feature: P2.0 — a raise enters the ledger and node-usage growth cashes it
 * in, so realized growth is never double-counted with the promise.
 * Steps: OnRaise(8G @ use 40G); OnUsage(44G) (grew 4G); OnUsage(48G) (grew 4G more).
 * Expectation: outstanding 8G -> 4G -> 0.
 */
TEST_F(CommitmentLedgerTest, GrowthCashesInCommitment)
{
    CommitmentLedger::OnRaise("n1", 8 * GiB, 40 * GiB);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 8 * GiB);
    CommitmentLedger::OnUsage("n1", 44 * GiB);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 4 * GiB);
    CommitmentLedger::OnUsage("n1", 48 * GiB);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 0u);
    // fully cashed: a later drop does not resurrect the promise
    CommitmentLedger::OnUsage("n1", 46 * GiB);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 0u);
}

/**
 * Feature: a usage drop (park/evict/free) eats the promise — departed usage
 * takes its headroom with it — but only after the view-lag grace expires:
 * the first post-raise samples trail kernel reality and may sit below the
 * anchor with nothing having departed.
 * Steps: OnRaise(8G @ use 40G); OnUsage(38G) inside grace (no eat); burn the
 * grace with steady samples; OnUsage(38G); OnUsage(30G).
 * Expectation: 8G through grace, then 6G, then 0.
 */
TEST_F(CommitmentLedgerTest, DropEatsCommitmentAfterGrace)
{
    CommitmentLedger::OnRaise("n1", 8 * GiB, 40 * GiB);
    CommitmentLedger::OnUsage("n1", 38 * GiB); // grace: view lag, not a departure
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 8 * GiB);
    CommitmentLedger::OnUsage("n1", 40 * GiB);
    CommitmentLedger::OnUsage("n1", 40 * GiB);
    CommitmentLedger::OnUsage("n1", 40 * GiB); // grace (3 samples) burned
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 8 * GiB);
    CommitmentLedger::OnUsage("n1", 38 * GiB);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 6 * GiB);
    CommitmentLedger::OnUsage("n1", 30 * GiB);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 0u);
}

/**
 * Feature: consecutive raises accumulate on one anchor window; an
 * incremental raise ratchets the window's high water to its booking use,
 * immediately cashing in the growth the window has already seen; the
 * control arm (SetEnabled(false)) reads zero everywhere.
 * Steps: OnRaise(2G @ 40G); OnRaise(2G @ 41G); then disable and re-check.
 * Expectation: 3G outstanding (4G promised, 1G of seen growth cashed);
 * disabled -> Outstanding 0 even with bytes on the books; a raise while
 * disabled does not book.
 */
TEST_F(CommitmentLedgerTest, RaisesAccumulateAndControlArmZeroes)
{
    CommitmentLedger::OnRaise("n1", 2 * GiB, 40 * GiB);
    CommitmentLedger::OnRaise("n1", 2 * GiB, 41 * GiB);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 3 * GiB);
    CommitmentLedger::SetEnabled(false);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 0u);
    // a raise while disabled does not book
    CommitmentLedger::OnRaise("n1", 2 * GiB, 41 * GiB);
    CommitmentLedger::SetEnabled(true);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 3 * GiB);
}

/**
 * Feature: P2.0 — sample-to-sample oscillation around a steady level does
 * not cash the promise in; only net movement beyond the anchor window does
 * (live 2026-08-24: a 2G promise decayed to 0 in seven samples on a node
 * flat at 5.6G — every dip and every rebound charged it once each).
 * Steps: OnRaise(2G @ 4G); oscillating samples 4.8 / 4.5 / 4.9 / 4.4.
 * Expectation: outstanding = 2G - max(growth 0.9, drop 0) = 1.1G — the
 * 0.9G of genuine growth above the anchor is cashed in, the sawtooth is not.
 */
TEST_F(CommitmentLedgerTest, OscillationDoesNotDoubleEat)
{
    CommitmentLedger::OnRaise("n1", 2 * GiB, 4 * GiB);
    CommitmentLedger::OnUsage("n1", uint64_t(4.8 * GiB));
    CommitmentLedger::OnUsage("n1", uint64_t(4.5 * GiB));
    CommitmentLedger::OnUsage("n1", uint64_t(4.9 * GiB));
    CommitmentLedger::OnUsage("n1", uint64_t(4.4 * GiB));
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 2 * GiB - uint64_t(0.9 * GiB));
}

/**
 * Feature: unknown unit / zero raise are inert.
 */
TEST_F(CommitmentLedgerTest, UnknownUnitAndZeroRaise)
{
    EXPECT_EQ(CommitmentLedger::Outstanding("nope"), 0u);
    CommitmentLedger::OnRaise("n1", 0, 40 * GiB);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 0u);
    // usage feed for a unit without a promise is a no-op
    CommitmentLedger::OnUsage("n1", 50 * GiB);
    EXPECT_EQ(CommitmentLedger::Outstanding("n1"), 0u);
}

}  // namespace

}  // namespace functionsystem::test
