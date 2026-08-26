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

#include "local_scheduler/pressure_monitor/cpu_upgrade_ladder.h"

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>

#include <gtest/gtest.h>

namespace functionsystem::test {

using namespace local_scheduler;

namespace {

// fake cgroup pool root: <root>/<name>/{cpu.max,cpu.stat} as plain files.
// The clock is injected so the rate window (delta usage_usec over wall time)
// is driven by the test, never by real sleeps.
class CpuUpgradeLadderTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        char pattern[] = "/tmp/cpu_upgrade_ladder_test_XXXXXX";
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

    void AddSandbox(const std::string &name, uint64_t quotaUsec, uint64_t usageUsec = 0,
                    uint64_t nrThrottled = 0)
    {
        std::error_code ec;
        ASSERT_TRUE(std::filesystem::create_directories(root_ + "/" + name, ec)) << ec.message();
        WriteMax(name, quotaUsec);
        WriteStat(name, usageUsec, nrThrottled);
    }

    void WriteMax(const std::string &name, uint64_t quotaUsec, uint64_t periodUsec = 100000)
    {
        std::ofstream out(root_ + "/" + name + "/cpu.max", std::ios::trunc);
        out << quotaUsec << " " << periodUsec;
    }

    void WriteStat(const std::string &name, uint64_t usageUsec, uint64_t nrThrottled)
    {
        std::ofstream out(root_ + "/" + name + "/cpu.stat", std::ios::trunc);
        out << "usage_usec " << usageUsec << "\n"
            << "nr_periods 10\n"
            << "nr_throttled " << nrThrottled << "\n"
            << "throttled_usec 0\n";
    }

    std::string ReadMax(const std::string &name)
    {
        std::ifstream in(root_ + "/" + name + "/cpu.max");
        std::string quota, period;
        in >> quota >> period;
        return quota + " " + period;
    }

    CpuUpgradeLadderConfig DefaultConfig()
    {
        CpuUpgradeLadderConfig config;
        config.enabled = true;
        config.stepRatio = 1.5;
        config.capQuotaUsec = 800000;   // 8 cpus @ 100ms period
        config.safetyRatio = 0.9;
        config.nodeCapacityMilli = 16000; // 16-core node
        config.minRateWindowUsec = 1000000;
        return config;
    }

    std::string root_;
    uint64_t clockUsec_ = 0; // injected monotonic clock
};

/**
 * Feature: a nr_throttled event with node headroom raises the cpu.max quota
 * by one proportional rung (x1.5), period untouched.
 * Steps: sandbox quota 50000 (500 milli) @100ms; arm the baseline at t=0,
 * advance 1.5s with ~0 usage growth; OnThrottleEvent.
 * Expectation: quota -> 75000 (x1.5), period still 100000, nothing deferred.
 */
TEST_F(CpuUpgradeLadderTest, LowUsageRaisesOneProportionalRung)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 100000));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnSample(); // arms the usage baseline
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 110000, 1); // ~66 milli over the window
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(ReadMax("sbx-a"), "75000 100000");
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: the OnSample scan IS the event source on cgroupfs (cpu.stat emits
 * no inotify) — nr_throttled growth between two samples asks for a rung with
 * no OnThrottleEvent call at all.
 * Steps: seed via OnSample at t=0; advance 1.5s, nr_throttled 0 -> 7; OnSample.
 * Expectation: returns true, quota -> 75000.
 */
TEST_F(CpuUpgradeLadderTest, ThrottleGrowthBetweenSamplesRaises)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 100000));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    EXPECT_FALSE(ladder.OnSample()); // seeds the scope + throttle baselines
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 110000, 7);
    EXPECT_TRUE(ladder.OnSample());

    EXPECT_EQ(ReadMax("sbx-a"), "75000 100000");
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: a pre-existing throttle count only seeds the scan baseline (a
 * ladder restart must not raise on stale pressure); only growth raises.
 * Steps: sandbox starts at nr_throttled 5; first OnSample seeds; a second
 * OnSample without growth must not raise.
 * Expectation: quota unchanged after both samples.
 */
TEST_F(CpuUpgradeLadderTest, PreExistingThrottleCountOnlySeeds)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 100000, 5));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    EXPECT_FALSE(ladder.OnSample());
    clockUsec_ = 1500000;
    EXPECT_FALSE(ladder.OnSample()); // same nr_throttled: no growth, no rung

    EXPECT_EQ(ReadMax("sbx-a"), "50000 100000");
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: the same event on a busy node is deferred, not written.
 * Steps: scope usage grows 21.3s of CPU-time over 1.5s (14200 milli) —
 * 14200 + 250 > 0.9*16000; OnThrottleEvent.
 * Expectation: quota unchanged, one deferred entry.
 */
TEST_F(CpuUpgradeLadderTest, HotNodeDefers)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 0));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnSample();
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 21300000, 1);
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(ReadMax("sbx-a"), "50000 100000");
    EXPECT_EQ(ladder.DeferredCount(), 1u);
}

/**
 * Feature: a deferred rung is released by a later cool sample.
 * Steps: defer as above; advance 1s with ~0 growth; OnSample.
 * Expectation: quota -> 75000, deferred set empty, OnSample returned true.
 */
TEST_F(CpuUpgradeLadderTest, UsageDropReleasesDeferred)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 0));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnSample();
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 21300000, 1);
    ladder.OnThrottleEvent("sbx-a", 1);
    ASSERT_EQ(ladder.DeferredCount(), 1u);

    clockUsec_ = 2500000;
    WriteStat("sbx-a", 21310000, 2);
    EXPECT_TRUE(ladder.OnSample());

    EXPECT_EQ(ReadMax("sbx-a"), "75000 100000");
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: OnSample reports whether it raised (one rung per sample contract).
 */
TEST_F(CpuUpgradeLadderTest, OnSampleReportsRaise)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 0));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    EXPECT_FALSE(ladder.OnSample());
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 21300000, 1);
    ladder.OnThrottleEvent("sbx-a", 1);

    clockUsec_ = 2500000;
    WriteStat("sbx-a", 21310000, 2);
    EXPECT_TRUE(ladder.OnSample());   // releases the deferred rung
    EXPECT_FALSE(ladder.OnSample());  // nothing left to do
}

/**
 * Feature: raises never cross the per-sandbox cap.
 * Steps: sandbox already at cap (800000 usec); throttle event under low usage.
 * Expectation: no write, nothing deferred.
 */
TEST_F(CpuUpgradeLadderTest, CapIsRespected)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 800000, 100000));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnSample();
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 110000, 1);
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(ReadMax("sbx-a"), "800000 100000");
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: a raise is clamped to the cap when the rung would overshoot.
 * Steps: quota 600000 (x1.5 -> 900000 > cap 800000) under low usage.
 * Expectation: quota -> cap (800000).
 */
TEST_F(CpuUpgradeLadderTest, RaiseClampsToCap)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 600000, 100000));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnSample();
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 110000, 1);
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(ReadMax("sbx-a"), "800000 100000");
}

/**
 * Feature: an unlimited ("max") quota is treated as already at cap.
 * Expectation: no write, nothing deferred.
 */
TEST_F(CpuUpgradeLadderTest, UnlimitedQuotaIsAtCap)
{
    std::error_code ec;
    ASSERT_TRUE(std::filesystem::create_directories(Root() + "/sbx-a", ec)) << ec.message();
    {
        std::ofstream out(Root() + "/sbx-a/cpu.max", std::ios::trunc);
        out << "max 100000";
    }
    WriteStat("sbx-a", 100000, 0);

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnSample();
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 110000, 1);
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(ReadMax("sbx-a"), "max 100000");
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: a disabled ladder is a no-op.
 */
TEST_F(CpuUpgradeLadderTest, DisabledLadderIsNoOp)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 100000));

    auto config = DefaultConfig();
    config.enabled = false;
    CpuUpgradeLadder ladder("node-1", config, Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnSample();
    clockUsec_ = 1500000;
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(ReadMax("sbx-a"), "50000 100000");
    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

/**
 * Feature: fail-closed — without a trusted rate window no raise happens.
 * Steps: OnThrottleEvent before any baseline sample.
 * Expectation: no write, deferred (released once a window is measured).
 */
TEST_F(CpuUpgradeLadderTest, NoRateSampleDefers)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 100000));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnThrottleEvent("sbx-a", 1);

    EXPECT_EQ(ReadMax("sbx-a"), "50000 100000");
    EXPECT_EQ(ladder.DeferredCount(), 1u);
}

/**
 * Feature: fail-closed — a zero node capacity config never raises.
 */
TEST_F(CpuUpgradeLadderTest, ZeroNodeCapacityFailsClosed)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 100000));

    auto config = DefaultConfig();
    config.nodeCapacityMilli = 0;
    CpuUpgradeLadder ladder("node-1", config, Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnSample();
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 110000, 1);
    ladder.OnThrottleEvent("sbx-a", 1);
    clockUsec_ = 2500000;
    ladder.OnSample();

    EXPECT_EQ(ReadMax("sbx-a"), "50000 100000");
}

/**
 * Feature: a window shorter than minRateWindowUsec does not refresh (and
 * never first-trusts) the rate — the previous measurement stays sticky.
 * Steps: hot window (defer), then a 0.5s window showing cool usage: the
 * stale hot rate must still deny.
 * Expectation: quota unchanged, still deferred.
 */
TEST_F(CpuUpgradeLadderTest, ShortWindowKeepsStaleRate)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 0));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnSample();
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 21300000, 1);
    ladder.OnThrottleEvent("sbx-a", 1);
    ASSERT_EQ(ladder.DeferredCount(), 1u);

    clockUsec_ = 2000000; // only 0.5s since the last read
    WriteStat("sbx-a", 21300000, 2);
    EXPECT_FALSE(ladder.OnSample());

    EXPECT_EQ(ReadMax("sbx-a"), "50000 100000");
    EXPECT_EQ(ladder.DeferredCount(), 1u);
}

/**
 * Feature: a deferred dir whose cgroup vanished is dropped on the next
 * sample instead of clogging the deferred set.
 */
TEST_F(CpuUpgradeLadderTest, VanishedDirIsDropped)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a", 50000, 0));

    CpuUpgradeLadder ladder("node-1", DefaultConfig(), Root(), [this] { return clockUsec_; });
    clockUsec_ = 0;
    ladder.OnSample();
    clockUsec_ = 1500000;
    WriteStat("sbx-a", 21300000, 1);
    ladder.OnThrottleEvent("sbx-a", 1);
    ASSERT_EQ(ladder.DeferredCount(), 1u);

    std::error_code ec;
    (void)std::filesystem::remove_all(Root() + "/sbx-a", ec);
    clockUsec_ = 2500000;
    ladder.OnSample();

    EXPECT_EQ(ladder.DeferredCount(), 0u);
}

}  // namespace

}  // namespace functionsystem::test
