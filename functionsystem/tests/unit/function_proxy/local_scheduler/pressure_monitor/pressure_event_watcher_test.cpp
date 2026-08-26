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

#include "local_scheduler/pressure_monitor/pressure_event_watcher.h"

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>

#include <gtest/gtest.h>

#include "utils/future_test_helper.h"

namespace functionsystem::test {

using namespace local_scheduler;
using namespace std::chrono_literals;

// fake cgroup pool root: <root>/<name>/memory.events
class PressureEventWatcherTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        char pattern[] = "/tmp/pressure_watcher_test_XXXXXX";
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

    void AddSandbox(const std::string &name, uint64_t high = 0)
    {
        std::error_code ec;
        ASSERT_TRUE(std::filesystem::create_directories(Root() + "/" + name, ec)) << ec.message();
        WriteEvents(name, high);
    }

    // rewrite the file the way the kernel would on a counter update
    void WriteEvents(const std::string &name, uint64_t high)
    {
        std::ofstream file(Root() + "/" + name + "/memory.events", std::ios::trunc);
        file << "low 0\n"
             << "high " << high << "\n"
             << "max 0\n"
             << "oom 0\n"
             << "oom_kill 0\n"
             << "oom_group_kill 0\n";
    }

    std::string root_;
};

/**
 * Feature: a memory.events "high" increment in an existing sandbox wakes the
 * monitor immediately.
 * Steps: sandbox sbx-a exists at Start; rewrite events with high 0 -> 1.
 * Expectation: callback fires within the await window.
 */
TEST_F(PressureEventWatcherTest, HighIncrementFiresCallback)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a"));

    std::atomic<int> calls{0};
    PressureEventWatcher watcher(Root(), 0, [&calls](const std::string &, uint64_t) { ++calls; });
    ASSERT_TRUE(watcher.Start());
    std::this_thread::sleep_for(200ms);  // let the scan arm the watch

    WriteEvents("sbx-a", 1);
    ASSERT_AWAIT_TRUE([&] { return calls.load() > 0; });
    watcher.Stop();
}

/**
 * Feature: counter churn without a "high" delta does not wake the monitor.
 * Steps: rewrite events with unchanged high (oom grows instead).
 * Expectation: no callback within 1s.
 */
TEST_F(PressureEventWatcherTest, NonHighChangeDoesNotFire)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a"));

    std::atomic<int> calls{0};
    PressureEventWatcher watcher(Root(), 0, [&calls](const std::string &, uint64_t) { ++calls; });
    ASSERT_TRUE(watcher.Start());
    std::this_thread::sleep_for(200ms);

    std::ofstream file(Root() + "/sbx-a/memory.events", std::ios::trunc);
    file << "low 0\nhigh 0\nmax 3\noom 1\noom_kill 0\noom_group_kill 0\n";

    std::this_thread::sleep_for(1s);
    EXPECT_EQ(calls.load(), 0);
    watcher.Stop();
}

/**
 * Feature: a sandbox created after Start is picked up (IN_CREATE + pending
 * retry) and its later high increments wake the monitor.
 */
TEST_F(PressureEventWatcherTest, SandboxCreatedAfterStartIsWatched)
{
    std::atomic<int> calls{0};
    PressureEventWatcher watcher(Root(), 0, [&calls](const std::string &, uint64_t) { ++calls; });
    ASSERT_TRUE(watcher.Start());
    std::this_thread::sleep_for(100ms);

    // dir first, file later: exercises the pending-retry path
    std::error_code ec;
    ASSERT_TRUE(std::filesystem::create_directories(Root() + "/sbx-late", ec)) << ec.message();
    std::this_thread::sleep_for(50ms);
    WriteEvents("sbx-late", 0);
    std::this_thread::sleep_for(1s);  // > pending retry interval

    WriteEvents("sbx-late", 2);
    ASSERT_AWAIT_TRUE([&] { return calls.load() > 0; });
    watcher.Stop();
}

/**
 * Feature: throttle storms are debounced — two rapid high increments inside
 * the gap window yield exactly one callback.
 */
TEST_F(PressureEventWatcherTest, RapidIncrementsAreDebounced)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a"));

    std::atomic<int> calls{0};
    PressureEventWatcher watcher(Root(), 60000,
                                 [&calls](const std::string &, uint64_t) { ++calls; });  // gap > test duration
    ASSERT_TRUE(watcher.Start());
    std::this_thread::sleep_for(200ms);

    WriteEvents("sbx-a", 1);
    ASSERT_AWAIT_TRUE([&] { return calls.load() == 1; });
    WriteEvents("sbx-a", 5);
    std::this_thread::sleep_for(1s);
    EXPECT_EQ(calls.load(), 1);  // suppressed by the debounce window
    watcher.Stop();
}

/**
 * Feature: a second high increment after the debounce window fires again
 * (debounce merges storms, it does not latch forever).
 */
TEST_F(PressureEventWatcherTest, IncrementAfterGapWindowFiresAgain)
{
    ASSERT_NO_FATAL_FAILURE(AddSandbox("sbx-a"));

    std::atomic<int> calls{0};
    PressureEventWatcher watcher(Root(), 300, [&calls](const std::string &, uint64_t) { ++calls; });
    ASSERT_TRUE(watcher.Start());
    std::this_thread::sleep_for(200ms);

    WriteEvents("sbx-a", 1);
    ASSERT_AWAIT_TRUE([&] { return calls.load() == 1; });
    std::this_thread::sleep_for(600ms);  // let the gap window elapse
    WriteEvents("sbx-a", 2);
    ASSERT_AWAIT_TRUE([&] { return calls.load() == 2; });
    watcher.Stop();
}

/**
 * Feature: a missing cgroup root disables the watcher (Start false) instead
 * of failing startup — the monitor stays on polling.
 */
TEST_F(PressureEventWatcherTest, MissingRootDisablesWatcher)
{
    PressureEventWatcher watcher("/tmp/definitely_not_a_cgroup_pool_42", 0, [](const std::string &, uint64_t) {});
    EXPECT_FALSE(watcher.Start());
}

/**
 * Feature: W-CPUL — the watcher is counter-generic: a second instance on
 * cpu.stat fires on "nr_throttled" growth, and churn in other cpu.stat
 * counters stays silent.
 * Steps: sandbox with cpu.stat nr_throttled 0 -> 1 (only usage_usec churn
 * first, which must NOT fire).
 * Expectation: callback fires only on the nr_throttled delta.
 */
TEST_F(PressureEventWatcherTest, CpuStatNrThrottledFiresCallback)
{
    std::error_code ec;
    ASSERT_TRUE(std::filesystem::create_directories(Root() + "/sbx-cpu", ec)) << ec.message();
    auto writeStat = [this](uint64_t usageUsec, uint64_t nrThrottled) {
        std::ofstream file(Root() + "/sbx-cpu/cpu.stat", std::ios::trunc);
        file << "usage_usec " << usageUsec << "\n"
             << "nr_periods 10\n"
             << "nr_throttled " << nrThrottled << "\n"
             << "throttled_usec 0\n";
    };
    writeStat(1000, 0);

    std::atomic<int> calls{0};
    PressureEventWatcher watcher(Root(), 0, [&calls](const std::string &, uint64_t) { ++calls; }, "cpu.stat",
                                 "nr_throttled");
    ASSERT_TRUE(watcher.Start());
    std::this_thread::sleep_for(200ms);

    writeStat(999999, 0);  // usage churn only
    std::this_thread::sleep_for(500ms);
    EXPECT_EQ(calls.load(), 0);

    writeStat(1500000, 1);  // the quota ran out
    ASSERT_AWAIT_TRUE([&] { return calls.load() > 0; });
    watcher.Stop();
}

}  // namespace functionsystem::test
