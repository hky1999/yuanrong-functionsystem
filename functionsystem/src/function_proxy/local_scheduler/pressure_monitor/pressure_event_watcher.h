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

#ifndef LOCAL_SCHEDULER_PRESSURE_EVENT_WATCHER_H
#define LOCAL_SCHEDULER_PRESSURE_EVENT_WATCHER_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace functionsystem::local_scheduler {

/**
 * D-2 event-driven watermarks: watches the pooled sandbox cgroup tree and
 * wakes the pressure monitor as soon as any sandbox's memory.events "high"
 * counter grows (the sandbox crossed its memory.high soft limit — the leading
 * indicator of node pressure), instead of waiting for the next polling cycle.
 *
 * W-CPUL: the watcher is counter-generic — a second instance watches
 * <root>/<pool>/cpu.stat "nr_throttled" (the sandbox burned through its
 * cpu.max quota) to drive the CPU upgrade ladder the same way.
 *
 * The watched layout is <root>/<pool-id>/<fileName> where <root> is the
 * sandboxd cgroup pool root ("/sys/fs/cgroup/sandbox" upstream, overridden per
 * deployment — "/sys/fs/cgroup/akernel" on the w2 standalone). Pool ids are
 * recycled sandboxd-internal names with no mapping to instance ids on this
 * side; the event only means "sample the node watermark NOW", so no identity
 * is attached. The polling cycle in PressureMonitorActor stays as the
 * debounce/fallback path.
 *
 * The callback fires on the watcher's own thread; it must only enqueue (e.g.
 * litebus::Async onto the pressure monitor actor), never touch actor state.
 */
class PressureEventWatcher {
public:
    /**
     * @param cgroupRoot      sandbox cgroup pool root (contains one subdir per sandbox)
     * @param minGapMs        callbacks are suppressed within this window (throttle storms)
     * @param callback        invoked off-thread as (poolDir, increment) when the
     *                        counter increments (D-3: the throttle event itself
     *                        is the upgrade-as-signal input)
     * @param fileName        counter file watched inside each pool dir
     *                        (default memory.events)
     * @param counterKey      the "key value" line inside that file whose growth
     *                        fires the callback (default "high"; the CPU instance
     *                        passes cpu.stat / nr_throttled)
     */
    PressureEventWatcher(std::string cgroupRoot, uint32_t minGapMs,
                         std::function<void(const std::string &, uint64_t)> callback,
                         std::string fileName = "memory.events", std::string counterKey = "high");
    ~PressureEventWatcher();

    PressureEventWatcher(const PressureEventWatcher &) = delete;
    PressureEventWatcher &operator=(const PressureEventWatcher &) = delete;

    /**
     * Start watching. Returns false (after logging) when the root is missing —
     * the caller degrades to pure polling; never throws or blocks startup.
     */
    bool Start();

    void Stop();

private:
    void Run();
    void ScanExisting();
    void RetryPendingDirs();
    bool AddSandboxDir(const std::string &name);
    void RemoveSandboxDir(int wd);
    void HandleModify(int wd);
    void OnHighIncrement(const std::string &name, uint64_t increment);
    std::string CounterPath(const std::string &name) const;

    std::string cgroupRoot_;
    uint32_t minGapMs_;
    std::function<void(const std::string &, uint64_t)> callback_;
    std::string fileName_;   // counter file inside each pool dir (memory.events / cpu.stat)
    std::string counterKey_; // whose growth fires the callback ("high" / "nr_throttled")

    int inotifyFD_{ -1 };
    int wakePipe_[2]{ -1, -1 };  // write end poked by Stop to release the read loop

    // guarded by the watcher thread only (Run/ScanExisting/AddSandboxDir...)
    std::unordered_map<int, std::string> wdToName_;       // memory.events watch
    std::unordered_map<std::string, uint64_t> highBase_;  // last seen "high" count
    // sandbox dirs seen via IN_CREATE whose memory.events was not there yet;
    // retried on the poll timeout (real cgroupfs populates files with the
    // mkdir, this only guards slow filesystems/tests)
    std::unordered_set<std::string> pendingDirs_;

    std::thread thread_;
    std::atomic<bool> running_{ false };
    // debounce state (watcher thread only)
    std::chrono::steady_clock::time_point lastNotify_{};
    bool notifiedOnce_{ false };
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_PRESSURE_EVENT_WATCHER_H
