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

#include "local_scheduler/pressure_monitor/upgrade_ladder.h"

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>

#include <gtest/gtest.h>

#include "common/resource_view/commitment_ledger.h"

namespace functionsystem::test {

using namespace local_scheduler;

namespace {

constexpr uint64_t GiB = 1024ULL * 1024ULL * 1024ULL;

// fake cgroup pool root: <root>/<name>/{memory.max,memory.high} as plain files
class UpgradeLadderTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        resource_view::CommitmentLedger::ResetForTest(); // process-global ledger
        char pattern[] = "/tmp/upgrade_ladder_test_XXXXXX";
        char *made = mkdtemp(pattern);
        ASSERT_NE(made, nullptr);
        root_ = made;  // copy: `made` points into SetUp's stack frame
    }

    void TearDown() override
    {
        if (!root_.empty()) {
            std::error_code ec;
            (void)std::filesystem::remove_all(root_, ec);
        }
    }

    const std::string &Root() const
    {
        return root_;
    }

    void AddSandbox(const std::string &name, uint64_t maxBytes, uint64_t highBytes)
    {
        std::error_code ec;
        ASSERT_TRUE(std::filesystem::create_directories(root_ + "/" + name, ec)) << ec.message();
        Write(name, "memory.max", maxBytes);
        Write(name, "memory.high", highBytes);
    }

    void Write(const std::string &name, const std::string &file, uint64_t value)
    {
        std::ofstream out(root_ + "/" + name + "/" + file, std::ios::trunc);
        out << value;
    }

    uint64_t Read(const std::string &name, const std::string &file)
    {
        std::ifstream in(root_ + "/" + name + "/" + file);
        uint64_t value = 0;
        in >> value;
        return value;
    }

    UpgradeLadderConfig DefaultConfig()
    {
        UpgradeLadderConfig config;
        config.enabled = true;
        config.stepBytes = 8 * GiB;
        config.capBytes = 32 * GiB;
        config.safetyRatio = 0.9;
        config.highRatio = 0.9;
        return config;
    }

    std::string root_;
};

/**
 * Feature: a throttle event under low node usage raises memory.max by one
 * rung and re-arms memory.high at highRatio * new max.
 * Steps: sandbox max 8G, node use 4G / cap 64G; OnThrottleEvent.
 * Expectation: max -> 16G, high -> 0.9*16G, nothing deferred.
 */
TEST_F(UpgradeLadderTest, LowUsageRaisesOneRung)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(4 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(Read("sbx-a", "memory.max"), 16 * GiB);
    EXPECT_EQ(Read("sbx-a", "memory.high"), static_cast<uint64_t>(0.9 * 16 * GiB));
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: the same event under a hot node is deferred, not written.
 * Steps: use 55G + step 8G > 0.9*64G; OnThrottleEvent.
 * Expectation: max unchanged, one deferred entry.
 */
TEST_F(UpgradeLadderTest, HotNodeDefers)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(55 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(Read("sbx-a", "memory.max"), 8 * GiB);
    EXPECT_EQ(ladder.DeferredCount(), 1u);
}

/**
 * Feature: a deferred rung is released by a later usage refresh.
 * Steps: defer under 55G; OnNodeUsage(10G/64G) refresh.
 * Expectation: max -> 16G, high re-armed, deferred set empty.
 */
TEST_F(UpgradeLadderTest, UsageDropReleasesDeferred)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(55 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-a", 1);
    ASSERT_EQ(ladder.DeferredCount(), 1u);

    ladder.OnNodeUsage(10 * GiB, 64 * GiB);

    EXPECT_EQ(Read("sbx-a", "memory.max"), 16 * GiB);
    EXPECT_EQ(Read("sbx-a", "memory.high"), static_cast<uint64_t>(0.9 * 16 * GiB));
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: raises never cross the per-sandbox cap.
 * Steps: sandbox already at cap (32G); throttle event under low usage.
 * Expectation: no write, nothing deferred.
 */
TEST_F(UpgradeLadderTest, CapIsRespected)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 32 * GiB, 28 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(4 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(Read("sbx-a", "memory.max"), 32 * GiB);
    EXPECT_EQ(Read("sbx-a", "memory.high"), 28 * GiB);
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: a raise is clamped to the cap when the rung would overshoot.
 * Steps: sandbox max 28G (28+8=36 > cap 32) under low usage.
 * Expectation: max -> cap (32G), high -> 0.9*32G.
 */
TEST_F(UpgradeLadderTest, RaiseClampsToCap)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 28 * GiB, 25 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(4 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(Read("sbx-a", "memory.max"), 32 * GiB);
    EXPECT_EQ(Read("sbx-a", "memory.high"), static_cast<uint64_t>(0.9 * 32 * GiB));
}

/**
 * Feature: a deferred dir whose cgroup vanished is dropped on the next
 * refresh instead of clogging the deferred set.
 * Steps: defer sbx-a; remove its directory; OnNodeUsage refresh.
 * Expectation: deferred set empty, no crash.
 */
TEST_F(UpgradeLadderTest, VanishedDirIsDropped)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(55 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-a", 1);
    ASSERT_EQ(ladder.DeferredCount(), 1u);

    std::error_code ec;
    (void)std::filesystem::remove_all(Root() + "/sbx-a", ec);
    ladder.OnNodeUsage(10 * GiB, 64 * GiB);

    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: a disabled ladder is a no-op.
 * Steps: disabled config, throttle event, low usage.
 * Expectation: no write, nothing deferred.
 */
TEST_F(UpgradeLadderTest, DisabledLadderIsNoOp)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));

    auto config = DefaultConfig();
    config.enabled = false;
    UpgradeLadder ladder("node-1", config, Root());
    ladder.OnNodeUsage(4 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(Read("sbx-a", "memory.max"), 8 * GiB);
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: fail-closed — without a capacity sample no raise happens.
 * Steps: OnThrottleEvent before any OnNodeUsage.
 * Expectation: no write (deferred until usage arrives).
 */
TEST_F(UpgradeLadderTest, NoUsageSampleDefers)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(Read("sbx-a", "memory.max"), 8 * GiB);
    EXPECT_EQ(ladder.DeferredCount(), 1u);
}

/**
 * Feature: D-3① — admission re-reads <dir>/memory.current and uses the max of
 * the (stale) view sample and the kernel aggregate, so a burst that started
 * after the last sample still tightens the decision.
 * Steps: sandbox max 8G with memory.current = 55G; stale sample says 4G/64G
 * (which alone would admit); OnThrottleEvent.
 * Expectation: denied — max unchanged, one deferred entry.
 */
TEST_F(UpgradeLadderTest, FreshCurrentTightensAdmission)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));
    Write("sbx-a", "memory.current", 55 * GiB);

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(4 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(Read("sbx-a", "memory.max"), 8 * GiB);
    EXPECT_EQ(ladder.DeferredCount(), 1u);
}

/**
 * Feature: D-3③ — the rung is capped at the sandbox's own current max, so a
 * small cgroup climbs proportionally instead of jumping the global 8G step.
 * Steps: sandbox max 2G; low node usage; OnThrottleEvent.
 * Expectation: max 2G -> 4G (+2G, not +8G), high -> 0.9*4G.
 */
TEST_F(UpgradeLadderTest, SmallSandboxTakesProportionalStep)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-small", 2 * GiB, 1 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(4 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-small", 1);

    EXPECT_EQ(Read("sbx-small", "memory.max"), 4 * GiB);
    EXPECT_EQ(Read("sbx-small", "memory.high"), static_cast<uint64_t>(0.9 * 4 * GiB));
}

/**
 * Feature: D-3② — OnNodeUsage reports whether this call raised a rung, letting
 * the actor cap ladder work at one rung per sample.
 * Steps: defer sbx-a under a hot node, then release with a cool refresh and
 * refresh again on the now-empty deferred set.
 * Expectation: the releasing call returns true, the idle call false.
 */
TEST_F(UpgradeLadderTest, OnNodeUsageReportsRaise)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(55 * GiB, 64 * GiB);
    EXPECT_FALSE(ladder.OnNodeUsage(55 * GiB, 64 * GiB));
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_TRUE(ladder.OnNodeUsage(10 * GiB, 64 * GiB));
    EXPECT_FALSE(ladder.OnNodeUsage(10 * GiB, 64 * GiB));
    EXPECT_EQ(Read("sbx-a", "memory.max"), 16 * GiB);
}

/**
 * Feature: P2.0 — ladder self-admission accounts for the outstanding
 * commitment, so a second raise cannot spend promised-but-unrealized
 * headroom to amplify itself past the safety line.
 * Steps: two 8G sandboxes, use 44G / cap 64G (line 57.6G); raise sbx-a
 * (44+0+8 = 52 admits, books 8G commitment at 44G); throttle sbx-b before
 * any usage refresh (44 + 8 + 8 = 60 > 57.6).
 * Expectation: sbx-a raised, sbx-b deferred with max unchanged.
 */
TEST_F(UpgradeLadderTest, CommitmentBlocksSecondRaise)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-b", 8 * GiB, 7 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(44 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-a", 1);
    EXPECT_EQ(Read("sbx-a", "memory.max"), 16 * GiB);

    ladder.OnThrottleEvent("sbx-b", 1);
    EXPECT_EQ(Read("sbx-b", "memory.max"), 8 * GiB);
    EXPECT_EQ(ladder.DeferredCount(), 1u);
}

/**
 * Feature: P2.0 — node-usage change decays the promise, releasing the
 * deferred raise once the headroom is realized (or freed).
 * Steps: continue from CommitmentBlocksSecondRaise with use dropping to 10G.
 * Expectation: sbx-b raises on the refresh.
 */
TEST_F(UpgradeLadderTest, CommitmentDecayedByUsageRefreshReleases)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-b", 8 * GiB, 7 * GiB));

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(44 * GiB, 64 * GiB);
    ladder.OnThrottleEvent("sbx-a", 1);
    ladder.OnThrottleEvent("sbx-b", 1);
    ASSERT_EQ(ladder.DeferredCount(), 1u);

    ladder.OnNodeUsage(10 * GiB, 64 * GiB);

    EXPECT_EQ(Read("sbx-b", "memory.max"), 16 * GiB);
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: P2.0 — the commitment anchor takes the kernel scope aggregate
 * (where the lagging view will settle), and the ledger grace-periods the
 * first post-raise samples so the stale view reads below the anchor are not
 * booked as a drop (live 2026-08-24 v3: booked 2048MB at kernel 6076MB,
 * first view sample 4196MB, promise zeroed in two samples on a flat node;
 * v4: kernel 6409MB at raise, view settled 6533MB — the aggregate IS the
 * view's landing point).
 * Steps: sbx-a current 12G + sbx-b current 28G = 40G kernel aggregate,
 * stale view sample 5G/64G; throttle sbx-a -> raise still happens (admission
 * uses the tightened 40G, 40+8 < 57.6G line) and the 8G promise anchors at
 * 40G; then the view feed lags (6G, 7G — inside grace), then catches up to
 * 44G.
 * Expectation: outstanding stays 8G through the lag, then 8G - 4G = 4G.
 */
TEST_F(UpgradeLadderTest, CommitmentAnchorsOnScopeAggregateWithGrace)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 8 * GiB, 7 * GiB));
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-b", 8 * GiB, 7 * GiB));
    Write("sbx-a", "memory.current", 12 * GiB);
    Write("sbx-b", "memory.current", 28 * GiB);

    UpgradeLadder ladder("node-1", DefaultConfig(), Root());
    ladder.OnNodeUsage(5 * GiB, 64 * GiB); // stale sample, well below reality
    ladder.OnThrottleEvent("sbx-a", 1);
    EXPECT_EQ(Read("sbx-a", "memory.max"), 16 * GiB);
    EXPECT_EQ(resource_view::CommitmentLedger::Outstanding("node-1"), 8 * GiB);

    // view lags far below the kernel anchor: grace holds the promise
    ladder.OnNodeUsage(6 * GiB, 64 * GiB);
    EXPECT_EQ(resource_view::CommitmentLedger::Outstanding("node-1"), 8 * GiB);
    ladder.OnNodeUsage(7 * GiB, 64 * GiB);
    EXPECT_EQ(resource_view::CommitmentLedger::Outstanding("node-1"), 8 * GiB);

    // catch-up lands 4G above the anchor: that much is real growth
    ladder.OnNodeUsage(44 * GiB, 64 * GiB);
    EXPECT_EQ(resource_view::CommitmentLedger::Outstanding("node-1"), 4 * GiB);
}

}  // namespace

}  // namespace functionsystem::test
