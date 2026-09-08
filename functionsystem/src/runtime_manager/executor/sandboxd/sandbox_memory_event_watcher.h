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

#ifndef RUNTIME_MANAGER_EXECUTOR_SANDBOXD_SANDBOX_MEMORY_EVENT_WATCHER_H
#define RUNTIME_MANAGER_EXECUTOR_SANDBOXD_SANDBOX_MEMORY_EVENT_WATCHER_H

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/inotify.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace functionsystem::runtime_manager {

/**
 * Event-driven hint source for one sandbox cgroup v2 dir's memory.events file.
 *
 * Contract (the executor stays authoritative):
 * - The callback is only a "sample this runtime now" hint carrying the
 *   registered identity — never fabricated resource usage. The caller is
 *   expected to just Async the hint into its actor and let a fresh stats read
 *   decide; the existing 200ms stats poll remains the debounce/fallback path.
 * - One thread, one epoll: the inotify fd plus an eventfd used only to stop.
 *   Stop writes the eventfd, joins the thread, and only then closes the fds;
 *   nothing is detached, so destruction cannot leave a dangling this.
 * - One live identity per watch descriptor and per runtime ID. Replacing a
 *   registration (same runtime ID) or Unwatch retires the old descriptor for
 *   good: its number is never mapped again on this inotify instance, so
 *   queued events or a late IN_IGNORED cannot resurrect the old physical
 *   identity behind a recycled wd.
 * - Registration is bounded (kMaxWatches). IN_Q_OVERFLOW is only counted;
 *   recovery is left to the stats poll — no per-event node rescan.
 * - cgroupPath must be absolute, free of ".." components, resolve (realpath)
 *   under the configured cgroup root, and <cgroup>/memory.events must be a
 *   regular file. cgroupfs pseudo-files stat as regular files with a special
 *   st_dev and are accepted; arbitrary paths outside the root are not.
 * - Failures return false and bump a diagnostics counter; nothing here may
 *   terminate the process (Start false => caller stays on pure polling). The
 *   watcher only reads kernel file metadata; it never writes resource state.
 * - Start/Stop/Watch/Unwatch are meant to be called from the executor actor
 *   thread; the mutex synchronizes them against the watcher thread, which
 *   never invokes the callback while holding it.
 */
class SandboxMemoryEventWatcher {
public:
    struct Identity {
        std::string runtimeID;
        std::string sandboxID;
        std::string resourceGeneration;
        std::string cgroupPath;  // absolute v2 cgroup dir (not the events file) under the root
    };

    using Callback = std::function<void(const Identity &)>;

    struct Diagnostics {
        uint64_t startFailures = 0;     // fd/thread setup failed; caller is on polling only
        uint64_t queueOverflows = 0;    // IN_Q_OVERFLOW seen; stats poll is the fallback
        uint64_t rejectedWatches = 0;   // bad path / not started / cap reached / wd reuse refused
        uint64_t failedAddWatches = 0;  // inotify_add_watch(2) itself failed
        uint64_t staleEventsDropped = 0;  // hints for a retired or not-yet-published watch
        uint64_t callbackErrors = 0;    // exceptions swallowed out of the callback
        uint64_t threadExited = 0;      // watcher thread left its loop after a fatal fd error
    };

    static constexpr std::size_t kMaxWatches = 4096;
    static constexpr std::size_t kMaxDescriptorHistory = 65536;
    static constexpr const char *kDefaultCgroupRoot = "/sys/fs/cgroup";

    explicit SandboxMemoryEventWatcher(Callback callback, std::string cgroupRoot = kDefaultCgroupRoot)
        : callback_(std::move(callback)), cgroupRoot_(std::move(cgroupRoot))
    {
    }

    ~SandboxMemoryEventWatcher()
    {
        Stop();
    }

    SandboxMemoryEventWatcher(const SandboxMemoryEventWatcher &) = delete;
    SandboxMemoryEventWatcher &operator=(const SandboxMemoryEventWatcher &) = delete;

    /**
     * Spawns the watcher thread. Returns false (with a counter bumped) when the
     * inotify/epoll/eventfd setup or the thread spawn fails — the caller then
     * degrades to pure stats polling. Idempotent: true when already running.
     */
    bool Start();

    /** Wakes the thread via the eventfd, joins it, then closes all fds. Idempotent. */
    void Stop();

    /**
     * Registers the single active watch for identity.runtimeID. Returns false
     * (diagnosable via Stats()) when the path fails validation, the watcher is
     * not running, the registration cap is hit, or the descriptor the kernel
     * handed back cannot be proven fresh.
     */
    bool Watch(const Identity &identity);

    /** Retires the runtime's watch; its identity can no longer be notified. */
    void Unwatch(const std::string &runtimeID);

    Diagnostics Stats() const
    {
        Diagnostics d;
        d.startFailures = startFailures_.load(std::memory_order_relaxed);
        d.queueOverflows = queueOverflows_.load(std::memory_order_relaxed);
        d.rejectedWatches = rejectedWatches_.load(std::memory_order_relaxed);
        d.failedAddWatches = failedAddWatches_.load(std::memory_order_relaxed);
        d.staleEventsDropped = staleEventsDropped_.load(std::memory_order_relaxed);
        d.callbackErrors = callbackErrors_.load(std::memory_order_relaxed);
        d.threadExited = threadExited_.load(std::memory_order_relaxed);
        return d;
    }

private:
    static constexpr uint32_t kWatchMask = IN_MODIFY | IN_ATTRIB | IN_DELETE_SELF | IN_MOVE_SELF;

    void Run();
    void HandleNotifyEvent(const struct inotify_event *event);
    bool ResolveEventsPath(const std::string &cgroupPath, std::string &eventsPath) const;
    void RetireWdLocked(int wd);  // caller holds mutex_

    // Set once at construction and never mutated afterwards: safe for the
    // watcher thread to invoke without holding mutex_.
    Callback callback_;
    std::string cgroupRoot_;

    // Guards the identity maps and the fd-for-registrants lifetime. The three
    // fds are immutable while running_ is true (filled by Start before the
    // thread exists, cleared by Stop after joining it).
    std::mutex mutex_;
    std::unordered_map<int, Identity> wdToEntry_;        // one live identity per watch descriptor
    std::unordered_map<std::string, int> runtimeToWd_;   // at most one watch per runtime ID
    std::unordered_set<int> retiredWds_;                 // poisoned: never remapped on this instance

    int inotifyFD_ = -1;
    int epollFD_ = -1;
    int stopFD_ = -1;
    std::thread thread_;
    std::atomic<bool> running_{ false };

    std::atomic<uint64_t> startFailures_{ 0 };
    std::atomic<uint64_t> queueOverflows_{ 0 };
    std::atomic<uint64_t> rejectedWatches_{ 0 };
    std::atomic<uint64_t> failedAddWatches_{ 0 };
    std::atomic<uint64_t> staleEventsDropped_{ 0 };
    std::atomic<uint64_t> callbackErrors_{ 0 };
    std::atomic<uint64_t> threadExited_{ 0 };
};

inline bool SandboxMemoryEventWatcher::Start()
{
    std::lock_guard<std::mutex> guard(mutex_);
    if (running_.load()) {
        return true;
    }
    if (thread_.joinable()) {
        // A failed worker must be joined via Stop before it can be restarted.
        startFailures_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    const int inotifyFD = ::inotify_init1(IN_CLOEXEC | IN_NONBLOCK);
    const int epollFD = ::epoll_create1(EPOLL_CLOEXEC);
    const int stopFD = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (inotifyFD < 0 || epollFD < 0 || stopFD < 0) {
        if (inotifyFD >= 0) {
            (void)::close(inotifyFD);
        }
        if (epollFD >= 0) {
            (void)::close(epollFD);
        }
        if (stopFD >= 0) {
            (void)::close(stopFD);
        }
        startFailures_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    struct epoll_event ev {};
    ev.events = EPOLLIN;
    ev.data.fd = inotifyFD;
    bool armed = ::epoll_ctl(epollFD, EPOLL_CTL_ADD, inotifyFD, &ev) == 0;
    ev.data.fd = stopFD;
    armed = armed && ::epoll_ctl(epollFD, EPOLL_CTL_ADD, stopFD, &ev) == 0;
    if (!armed) {
        (void)::close(inotifyFD);
        (void)::close(epollFD);
        (void)::close(stopFD);
        startFailures_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    inotifyFD_ = inotifyFD;
    epollFD_ = epollFD;
    stopFD_ = stopFD;
    running_.store(true);
    try {
        thread_ = std::thread([this] { Run(); });
    } catch (...) {
        // resource exhaustion must degrade to polling, not terminate startup
        running_.store(false);
        (void)::close(inotifyFD_);
        inotifyFD_ = -1;
        (void)::close(epollFD_);
        epollFD_ = -1;
        (void)::close(stopFD_);
        stopFD_ = -1;
        startFailures_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

inline void SandboxMemoryEventWatcher::Stop()
{
    std::thread local;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        const bool wasRunning = running_.exchange(false);
        if (wasRunning && stopFD_ >= 0) {
            const uint64_t wake = 1;
            const ssize_t written = ::write(stopFD_, &wake, sizeof(wake));
            (void)written;
        }
        local = std::move(thread_);
        if (!local.joinable() && inotifyFD_ < 0 && epollFD_ < 0 && stopFD_ < 0) {
            return;  // never started or already stopped
        }
    }
    // Join outside the lock: the watcher thread takes mutex_ while dispatching.
    if (local.joinable()) {
        local.join();
    }
    std::lock_guard<std::mutex> guard(mutex_);
    if (inotifyFD_ >= 0) {
        (void)::close(inotifyFD_);
        inotifyFD_ = -1;
    }
    if (epollFD_ >= 0) {
        (void)::close(epollFD_);
        epollFD_ = -1;
    }
    if (stopFD_ >= 0) {
        (void)::close(stopFD_);
        stopFD_ = -1;
    }
    wdToEntry_.clear();
    runtimeToWd_.clear();
    // the inotify instance is gone; descriptors of a future Start are unrelated
    retiredWds_.clear();
}

inline void SandboxMemoryEventWatcher::Run()
{
    std::vector<char> buffer(64 * 1024);
    while (running_.load()) {
        struct epoll_event events[2];
        const int ready = ::epoll_wait(epollFD_, events, 2, -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            running_.store(false);
            threadExited_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        for (int i = 0; i < ready; ++i) {
            if ((events[i].events & EPOLLIN) == 0) {
                continue;
            }
            if (events[i].data.fd == stopFD_) {
                return;  // woken by Stop: leave before dispatching anything else
            }
            if (events[i].data.fd != inotifyFD_) {
                continue;
            }
            const ssize_t bytesRead = ::read(inotifyFD_, buffer.data(), buffer.size());
            if (bytesRead <= 0) {
                if (bytesRead < 0 && (errno == EINTR || errno == EAGAIN)) {
                    continue;
                }
                // fatal on our side: stop hinting; the stats poll keeps working
                running_.store(false);
                threadExited_.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            for (ssize_t offset = 0; offset + static_cast<ssize_t>(sizeof(struct inotify_event)) <= bytesRead;) {
                const auto *event = reinterpret_cast<const struct inotify_event *>(buffer.data() + offset);
                offset += sizeof(struct inotify_event) + event->len;
                HandleNotifyEvent(event);
            }
        }
    }
}

inline void SandboxMemoryEventWatcher::HandleNotifyEvent(const struct inotify_event *event)
{
    if ((event->mask & IN_Q_OVERFLOW) != 0) {
        // queue dropped events: count only, the stats poll is the fallback
        queueOverflows_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    const bool removal = (event->mask & (IN_DELETE_SELF | IN_MOVE_SELF | IN_IGNORED)) != 0;
    const bool hint = (event->mask & (IN_MODIFY | IN_ATTRIB)) != 0;
    Identity snapshot;
    bool fire = false;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        const auto it = wdToEntry_.find(event->wd);
        if (it == wdToEntry_.end()) {
            // no live mapping: either the registration was retired (the
            // IN_IGNORED from that removal is expected and harmless) or the
            // event predates publication of a fresh registration.
            if (hint) {
                staleEventsDropped_.fetch_add(1, std::memory_order_relaxed);
            }
            return;
        }
        if (removal) {
            // kernel dropped the watch (cgroup dir deleted/moved/removed):
            // forget the mapping now so the late IN_IGNORED that follows finds
            // nothing to resurrect; its wd stays poisoned
            runtimeToWd_.erase(it->second.runtimeID);
            retiredWds_.insert(event->wd);
            wdToEntry_.erase(it);
            return;
        }
        if (hint) {
            snapshot = it->second;  // copy the identity while holding the lock...
            fire = true;
        }
    }  // ...but invoke the callback only after releasing it
    if (fire && callback_) {
        try {
            callback_(snapshot);
        } catch (...) {
            callbackErrors_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

inline bool SandboxMemoryEventWatcher::Watch(const Identity &identity)
{
    if (identity.runtimeID.empty() || identity.sandboxID.empty() || identity.resourceGeneration.empty()) {
        rejectedWatches_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    std::string eventsPath;
    if (!ResolveEventsPath(identity.cgroupPath, eventsPath)) {
        rejectedWatches_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    if (inotifyFD_ < 0 || !running_.load()) {
        // not started (or the watcher thread died): caller stays on polling
        rejectedWatches_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    // one live watch per runtime: replacing a registration retires the old
    // descriptor first, so its queued events can no longer reach an identity
    const auto existing = runtimeToWd_.find(identity.runtimeID);
    if (existing != runtimeToWd_.end()) {
        const auto &registered = wdToEntry_.at(existing->second);
        if (registered.sandboxID == identity.sandboxID
            && registered.resourceGeneration == identity.resourceGeneration
            && registered.cgroupPath == identity.cgroupPath) { return true; }
        RetireWdLocked(existing->second);
        runtimeToWd_.erase(existing);
    } else if (wdToEntry_.size() >= kMaxWatches) {
        rejectedWatches_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (retiredWds_.size() + wdToEntry_.size() >= kMaxDescriptorHistory) {
        rejectedWatches_.fetch_add(1, std::memory_order_relaxed);
        return false; // bounded history; existing watches and periodic Stats continue
    }
    const int wd = ::inotify_add_watch(inotifyFD_, eventsPath.c_str(), kWatchMask);
    if (wd < 0) {
        failedAddWatches_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (retiredWds_.find(wd) != retiredWds_.end()) {
        // the kernel reused a retired descriptor number: events still queued
        // for the old watch are indistinguishable from this one's, so refuse
        (void)::inotify_rm_watch(inotifyFD_, wd);
        rejectedWatches_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (wdToEntry_.find(wd) != wdToEntry_.end()) {
        // same memory.events already watched for another runtime: one watch,
        // one runtime — keep the first registration, reject the newcomer
        rejectedWatches_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    wdToEntry_.emplace(wd, identity);
    runtimeToWd_.emplace(identity.runtimeID, wd);
    return true;
}

inline void SandboxMemoryEventWatcher::Unwatch(const std::string &runtimeID)
{
    std::lock_guard<std::mutex> guard(mutex_);
    const auto it = runtimeToWd_.find(runtimeID);
    if (it == runtimeToWd_.end()) {
        return;
    }
    RetireWdLocked(it->second);
    runtimeToWd_.erase(it);
}

inline void SandboxMemoryEventWatcher::RetireWdLocked(int wd)
{
    retiredWds_.insert(wd);  // never map this descriptor number again
    wdToEntry_.erase(wd);
    if (inotifyFD_ >= 0) {
        // queues a late IN_IGNORED that finds no mapping to resurrect
        (void)::inotify_rm_watch(inotifyFD_, wd);
    }
}

inline bool SandboxMemoryEventWatcher::ResolveEventsPath(const std::string &cgroupPath,
                                                         std::string &eventsPath) const
{
    if (cgroupPath.empty() || cgroupPath.front() != '/' || cgroupRoot_.empty() ||
        cgroupRoot_.front() != '/') {
        return false;
    }
    // reject ".." components outright instead of relying on realpath to dilute them
    for (std::size_t pos = 0; pos < cgroupPath.size();) {
        const std::size_t next = cgroupPath.find('/', pos);
        const std::size_t length = next == std::string::npos ? std::string::npos : next - pos;
        if (cgroupPath.substr(pos, length) == "..") {
            return false;
        }
        if (next == std::string::npos) {
            break;
        }
        pos = next + 1;
    }
    char rootBuf[PATH_MAX];
    if (::realpath(cgroupRoot_.c_str(), rootBuf) == nullptr) {
        return false;
    }
    char dirBuf[PATH_MAX];
    if (::realpath(cgroupPath.c_str(), dirBuf) == nullptr) {
        return false;  // missing or unreachable: not a known cgroup
    }
    const std::string root(rootBuf);
    const std::string dir(dirBuf);
    const bool underRoot = dir == root || (dir.size() > root.size() &&
                                           dir.compare(0, root.size(), root) == 0 && dir[root.size()] == '/');
    if (!underRoot) {
        return false;
    }
    eventsPath = dir + "/memory.events";
    struct stat st {};
    if (::stat(eventsPath.c_str(), &st) != 0) {
        return false;
    }
    // cgroupfs pseudo-files are regular files with a special st_dev: accepted;
    // anything that is not a regular file is not a v2 events file
    return S_ISREG(st.st_mode) != 0;
}

}  // namespace functionsystem::runtime_manager
#endif  // RUNTIME_MANAGER_EXECUTOR_SANDBOXD_SANDBOX_MEMORY_EVENT_WATCHER_H
