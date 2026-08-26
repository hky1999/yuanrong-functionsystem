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

#include "pressure_event_watcher.h"

#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/inotify.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>

#include "common/logs/logging.h"

namespace functionsystem::local_scheduler {

namespace {

constexpr uint32_t SANDBOX_DIR_WATCH_MASK = IN_CREATE | IN_DELETE_SELF;
constexpr uint32_t MEMORY_EVENTS_WATCH_MASK = IN_MODIFY | IN_DELETE_SELF | IN_IGNORED;
// poll timeout: also the retry cadence for sandbox dirs whose memory.events
// had not appeared at IN_CREATE time
constexpr int PENDING_RETRY_INTERVAL_MS = 500;

// Read a single "key value" counter out of a cgroup stat/events file.
// Returns false when the file is unreadable or lacks the key.
bool ReadCgroupCounter(const std::string &path, const std::string &key, uint64_t &out)
{
    std::ifstream file(path);
    if (!file.is_open()) {
        return false;
    }
    std::string name;
    uint64_t value = 0;
    while (file >> name >> value) {
        if (name == key) {
            out = value;
            return true;
        }
    }
    return false;
}

}  // namespace

PressureEventWatcher::PressureEventWatcher(std::string cgroupRoot, uint32_t minGapMs,
                                           std::function<void(const std::string &, uint64_t)> callback,
                                           std::string fileName, std::string counterKey)
    : cgroupRoot_(std::move(cgroupRoot)), minGapMs_(minGapMs), callback_(std::move(callback)),
      fileName_(std::move(fileName)), counterKey_(std::move(counterKey))
{
}

std::string PressureEventWatcher::CounterPath(const std::string &name) const
{
    return cgroupRoot_ + "/" + name + "/" + fileName_;
}

PressureEventWatcher::~PressureEventWatcher()
{
    Stop();
}

bool PressureEventWatcher::Start()
{
    DIR *dir = opendir(cgroupRoot_.c_str());
    if (dir == nullptr) {
        YRLOG_WARN("pressure event watcher: cgroup root {} unavailable ({}), staying on polling only",
                   cgroupRoot_, std::strerror(errno));
        return false;
    }
    closedir(dir);

    inotifyFD_ = inotify_init1(IN_CLOEXEC);
    if (inotifyFD_ < 0) {
        YRLOG_WARN("pressure event watcher: inotify_init failed ({}), staying on polling only",
                   std::strerror(errno));
        return false;
    }
    if (pipe2(wakePipe_, O_CLOEXEC | O_NONBLOCK) != 0) {
        YRLOG_WARN("pressure event watcher: wake pipe creation failed ({}), staying on polling only",
                   std::strerror(errno));
        close(inotifyFD_);
        inotifyFD_ = -1;
        return false;
    }

    const int rootWD = inotify_add_watch(inotifyFD_, cgroupRoot_.c_str(), SANDBOX_DIR_WATCH_MASK);
    if (rootWD < 0) {
        YRLOG_WARN("pressure event watcher: cannot watch {} ({}), staying on polling only", cgroupRoot_,
                   std::strerror(errno));
        close(wakePipe_[0]);
        close(wakePipe_[1]);
        wakePipe_[0] = wakePipe_[1] = -1;
        close(inotifyFD_);
        inotifyFD_ = -1;
        return false;
    }
    // nameless marker for the pool root: subdir creations are tracked here and
    // never resolved through wdToName_
    (void)rootWD;

    running_.store(true);
    thread_ = std::thread([this] { Run(); });
    YRLOG_INFO("pressure event watcher: watching {} (min gap {}ms) for {} {} increments",
               cgroupRoot_, minGapMs_, fileName_, counterKey_);
    return true;
}

void PressureEventWatcher::Stop()
{
    if (!running_.exchange(false)) {
        return;
    }
    if (wakePipe_[1] >= 0) {
        const char wake = 1;
        const ssize_t written = write(wakePipe_[1], &wake, sizeof(wake));
        (void)written;
    }
    if (thread_.joinable()) {
        thread_.join();
    }
    if (inotifyFD_ >= 0) {
        close(inotifyFD_);
        inotifyFD_ = -1;
    }
    for (int fd : wakePipe_) {
        if (fd >= 0) {
            close(fd);
        }
    }
    wakePipe_[0] = wakePipe_[1] = -1;
}

void PressureEventWatcher::Run()
{
    // pick up sandboxes that already exist before the first event; the root
    // watch covers later IN_CREATEs (AddSandboxDir is idempotent via the wd map)
    ScanExisting();
    char buffer[64 * 1024];
    while (running_.load()) {
        pollfd fds[2] = { { .fd = inotifyFD_, .events = POLLIN, .revents = 0 },
                          { .fd = wakePipe_[0], .events = POLLIN, .revents = 0 } };
        const int ready = poll(fds, 2, PENDING_RETRY_INTERVAL_MS);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            YRLOG_WARN("pressure event watcher: poll failed ({}), watcher exits", std::strerror(errno));
            return;
        }
        if ((fds[1].revents & POLLIN) != 0) {
            return;  // woken by Stop
        }
        if (ready == 0) {
            RetryPendingDirs();  // poll timeout: retry dirs whose file lagged
            continue;
        }
        if ((fds[0].revents & POLLIN) == 0) {
            continue;
        }
        const ssize_t bytesRead = read(inotifyFD_, buffer, sizeof(buffer));
        if (bytesRead <= 0) {
            if (bytesRead < 0 && (errno == EINTR || errno == EAGAIN)) {
                continue;
            }
            YRLOG_WARN("pressure event watcher: inotify read failed ({}), watcher exits", std::strerror(errno));
            return;
        }
        for (ssize_t offset = 0; offset + static_cast<ssize_t>(sizeof(inotify_event)) <= bytesRead;) {
            const auto *event = reinterpret_cast<const inotify_event *>(buffer + offset);
            offset += sizeof(inotify_event) + event->len;
            if ((event->mask & IN_Q_OVERFLOW) != 0) {
                YRLOG_WARN("pressure event watcher: queue overflow, rescan");
                ScanExisting();
                continue;
            }
            if ((event->mask & IN_IGNORED) != 0) {
                RemoveSandboxDir(event->wd);
                continue;
            }
            const auto watched = wdToName_.find(event->wd);
            if (watched == wdToName_.end()) {
                // the pool root: a new sandbox cgroup appeared (or vanished)
                if (event->len > 0 && (event->mask & (IN_CREATE | IN_ISDIR)) == (IN_CREATE | IN_ISDIR)) {
                    if (!AddSandboxDir(event->name)) {
                        pendingDirs_.insert(event->name);
                    }
                }
                continue;
            }
            if ((event->mask & (IN_MODIFY | IN_DELETE_SELF)) != 0) {
                HandleModify(event->wd);
            }
        }
        RetryPendingDirs();
    }
}

void PressureEventWatcher::RetryPendingDirs()
{
    for (auto it = pendingDirs_.begin(); it != pendingDirs_.end();) {
        if (AddSandboxDir(*it)) {
            it = pendingDirs_.erase(it);
            continue;
        }
        // drop entries whose directory vanished before the file ever appeared
        DIR *dir = opendir((cgroupRoot_ + "/" + *it).c_str());
        if (dir == nullptr) {
            it = pendingDirs_.erase(it);
            continue;
        }
        closedir(dir);
        ++it;
    }
}

void PressureEventWatcher::ScanExisting()
{
    DIR *dir = opendir(cgroupRoot_.c_str());
    if (dir == nullptr) {
        return;
    }
    while (auto *entry = readdir(dir)) {
        if (entry->d_type == DT_DIR && std::strcmp(entry->d_name, ".") != 0 &&
            std::strcmp(entry->d_name, "..") != 0) {
            (void)AddSandboxDir(entry->d_name);
        }
    }
    closedir(dir);
}

bool PressureEventWatcher::AddSandboxDir(const std::string &name)
{
    if (highBase_.find(name) != highBase_.end()) {
        return true;  // already watched
    }
    const std::string eventsPath = CounterPath(name);
    uint64_t baseline = 0;
    if (!ReadCgroupCounter(eventsPath, counterKey_, baseline)) {
        return false;  // not a sandbox cgroup (or gone already)
    }
    const int wd = inotify_add_watch(inotifyFD_, eventsPath.c_str(), MEMORY_EVENTS_WATCH_MASK);
    if (wd < 0) {
        return false;
    }
    wdToName_[wd] = name;
    highBase_[name] = baseline;
    YRLOG_DEBUG("pressure event watcher: watching {} ({}={})", eventsPath, counterKey_, baseline);
    return true;
}

void PressureEventWatcher::RemoveSandboxDir(int wd)
{
    const auto it = wdToName_.find(wd);
    if (it == wdToName_.end()) {
        return;
    }
    YRLOG_DEBUG("pressure event watcher: stopped watching sandbox cgroup {}", it->second);
    (void)highBase_.erase(it->second);
    wdToName_.erase(it);
}

void PressureEventWatcher::HandleModify(int wd)
{
    const auto it = wdToName_.find(wd);
    if (it == wdToName_.end()) {
        return;
    }
    const std::string name = it->second;
    const auto baseIt = highBase_.find(name);
    if (baseIt == highBase_.end()) {
        return;
    }
    uint64_t current = 0;
    const std::string eventsPath = CounterPath(name);
    if (!ReadCgroupCounter(eventsPath, counterKey_, current)) {
        return;  // transiently unreadable (cgroup being torn down)
    }
    if (current > baseIt->second) {
        const uint64_t increment = current - baseIt->second;
        baseIt->second = current;
        OnHighIncrement(name, increment);
    }
}

void PressureEventWatcher::OnHighIncrement(const std::string &name, uint64_t increment)
{
    const auto now = std::chrono::steady_clock::now();
    if (notifiedOnce_ &&
        std::chrono::duration_cast<std::chrono::milliseconds>(now - lastNotify_).count() < minGapMs_) {
        YRLOG_DEBUG("pressure event watcher: sandbox {} {} +{} suppressed (debounce)", name, counterKey_, increment);
        return;
    }
    notifiedOnce_ = true;
    lastNotify_ = now;
    YRLOG_INFO("pressure event watcher: sandbox cgroup {} crossed {} {} (+{}), waking pressure monitor",
               name, fileName_, counterKey_, increment);
    if (callback_) {
        callback_(name, increment);
    }
}

}  // namespace functionsystem::local_scheduler
