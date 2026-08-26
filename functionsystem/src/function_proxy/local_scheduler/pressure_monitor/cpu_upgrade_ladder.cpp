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

#include "cpu_upgrade_ladder.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>

#include "common/logs/logging.h"

namespace functionsystem::local_scheduler {

namespace {

// cpu.stat layout: "usage_usec N\nnr_periods N\nnr_throttled N\n..."; read
// one counter out of it. Returns false when unreadable or the key is absent.
bool ReadCpuStatCounter(const std::string &path, const std::string &key, uint64_t &out)
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

// cpu.max layout: "<quota> <period>" on one line, quota is a number or "max".
struct CpuMax {
    uint64_t quotaUsec = 0;
    uint64_t periodUsec = 0;
    bool unlimited = false;
};

bool ReadCpuMax(const std::string &path, CpuMax &out)
{
    std::ifstream file(path);
    if (!file.is_open()) {
        return false;
    }
    std::string quota;
    if (!(file >> quota)) {
        return false;
    }
    uint64_t period = 0;
    if (!(file >> period) || period == 0) {
        return false; // kernel always writes a period; bogus file -> skip
    }
    out.periodUsec = period;
    if (quota == "max") {
        out.unlimited = true;
        out.quotaUsec = UINT64_MAX;
        return true;
    }
    try {
        out.quotaUsec = std::stoull(quota);
    } catch (const std::exception &) {
        return false;
    }
    return true;
}

bool WriteCpuMax(const std::string &path, const CpuMax &value)
{
    std::ofstream file(path, std::ios::trunc);
    if (!file.is_open()) {
        return false;
    }
    if (value.unlimited) {
        file << "max " << value.periodUsec;
    } else {
        file << value.quotaUsec << " " << value.periodUsec;
    }
    return file.good();
}

uint64_t QuotaToMilli(uint64_t quotaUsec, uint64_t periodUsec)
{
    return quotaUsec * 1000ULL / periodUsec; // e.g. 50000/100000 -> 500 milli
}

}  // namespace

CpuUpgradeLadder::CpuUpgradeLadder(const std::string &nodeID, CpuUpgradeLadderConfig config,
                                   std::string cgroupRoot, std::function<uint64_t()> nowUsec)
    : nodeID_(nodeID), config_(config), cgroupRoot_(std::move(cgroupRoot)),
      nowUsec_(nowUsec ? std::move(nowUsec) : DefaultClock)
{
}

bool CpuUpgradeLadder::ReadScopeUsage(uint64_t &usageUsec)
{
    usageUsec = 0;
    bool any = false;
    std::error_code ec;
    for (const auto &entry : std::filesystem::directory_iterator(cgroupRoot_, ec)) {
        if (!entry.is_directory()) {
            continue;
        }
        uint64_t current = 0;
        if (ReadCpuStatCounter(entry.path().string() + "/cpu.stat", "usage_usec", current)) {
            usageUsec += current;
            any = true;
        }
    }
    if (!any) {
        return false;
    }
    const uint64_t now = nowUsec_();
    if (haveBaseline_ && now > lastScopeReadUsec_) {
        const uint64_t elapsed = now - lastScopeReadUsec_;
        if (elapsed >= config_.minRateWindowUsec) {
            // cores = delta_usage_usec / delta_elapsed_usec; milli = *1000.
            // A recycle can shrink the aggregate; clamp the delta at 0.
            const uint64_t delta = usageUsec > lastScopeUsageUsec_ ? usageUsec - lastScopeUsageUsec_ : 0;
            lastRateMilli_ = delta * 1000ULL / elapsed;
            rateTrusted_ = true;
        }
    }
    haveBaseline_ = true;
    lastScopeUsageUsec_ = usageUsec;
    lastScopeReadUsec_ = now;
    return true;
}

void CpuUpgradeLadder::OnThrottleEvent(const std::string &poolDir, uint64_t increment)
{
    if (!config_.enabled || poolDir.empty()) {
        return;
    }
    uint64_t scopeUsage = 0;
    (void)ReadScopeUsage(scopeUsage); // fresh recheck: rate from the ladder's own reads
    if (TryRaise(poolDir)) {
        return;
    }
    const size_t before = deferredDirs_.size();
    deferredDirs_.insert(poolDir);
    if (deferredDirs_.size() != before) {
        YRLOG_INFO("cpu upgrade ladder on node {}: sandbox {} throttled on quota (+{}) deferred by admission "
                   "(usage {} milli / cap {} milli, safety {})",
                   nodeID_, poolDir, increment, lastRateMilli_, config_.nodeCapacityMilli, config_.safetyRatio);
    }
}

bool CpuUpgradeLadder::OnSample()
{
    if (!config_.enabled) {
        return false;
    }
    uint64_t scopeUsage = 0;
    (void)ReadScopeUsage(scopeUsage);
    // cgroupfs cpu.stat cannot be inotify-watched (the kernel calls
    // cgroup_file_notify for memory.events & friends only), so the sampling
    // cadence IS the event source: scan the pool dirs for nr_throttled
    // growth and ask for one rung per newly throttled sandbox.
    bool raised = false;
    std::error_code ec;
    std::unordered_set<std::string> liveDirs;
    for (const auto &entry : std::filesystem::directory_iterator(cgroupRoot_, ec)) {
        if (!entry.is_directory()) {
            continue;
        }
        const std::string name = entry.path().filename().string();
        liveDirs.insert(name);
        uint64_t throttled = 0;
        if (!ReadCpuStatCounter(entry.path().string() + "/cpu.stat", "nr_throttled", throttled)) {
            continue; // no cpu.stat yet: nothing to say about this dir
        }
        auto it = lastThrottled_.find(name);
        if (it == lastThrottled_.end()) {
            lastThrottled_.emplace(name, throttled);
            continue; // first sighting only seeds the baseline
        }
        if (throttled <= it->second) {
            if (throttled < it->second) {
                it->second = throttled; // counter went backwards: recycled id
            }
            continue;
        }
        const uint64_t increment = throttled - it->second;
        it->second = throttled;
        if (deferredDirs_.count(name) != 0) {
            continue; // its rung is already pending in the deferred set
        }
        if (TryRaise(name)) {
            raised = true;
        } else {
            deferredDirs_.insert(name);
            YRLOG_INFO("cpu upgrade ladder on node {}: sandbox {} throttled on quota (+{}) deferred by admission "
                       "(usage {} milli / cap {} milli, safety {})",
                       nodeID_, name, increment, lastRateMilli_, config_.nodeCapacityMilli, config_.safetyRatio);
        }
    }
    for (auto it = lastThrottled_.begin(); it != lastThrottled_.end();) {
        if (liveDirs.count(it->first) == 0) {
            it = lastThrottled_.erase(it); // vanished pool dir (recycled id)
        } else {
            ++it;
        }
    }
    if (raised) {
        return true;
    }
    if (deferredDirs_.empty()) {
        return false;
    }
    // at most one release per sample (D-3②)
    for (const auto &poolDir : deferredDirs_) {
        if (TryRaise(poolDir)) {
            deferredDirs_.erase(poolDir);
            return true;
        }
        CpuMax current;
        if (!ReadCpuMax(cgroupRoot_ + "/" + poolDir + "/cpu.max", current)) {
            deferredDirs_.erase(poolDir); // vanished pool dir (recycled id)
            return false;
        }
        if (current.unlimited || current.quotaUsec >= config_.capQuotaUsec) {
            deferredDirs_.erase(poolDir); // at cap: stop deferring
            return false;
        }
        return false; // still over budget — retry on the next sample
    }
    return false;
}

bool CpuUpgradeLadder::TryRaise(const std::string &poolDir)
{
    if (config_.nodeCapacityMilli == 0) {
        return false; // fail closed without a node capacity config
    }
    if (!rateTrusted_) {
        return false; // fail closed until one trusted rate window is measured
    }
    const std::string dir = cgroupRoot_ + "/" + poolDir;
    CpuMax current;
    if (!ReadCpuMax(dir + "/cpu.max", current)) {
        YRLOG_DEBUG("cpu upgrade ladder on node {}: sandbox {} has no readable cpu.max, skip", nodeID_, poolDir);
        return false;
    }
    if (current.unlimited) {
        YRLOG_INFO("cpu upgrade ladder on node {}: sandbox {} already unlimited, skip", nodeID_, poolDir);
        return true; // nothing to defer either
    }
    if (current.quotaUsec >= config_.capQuotaUsec) {
        YRLOG_INFO("cpu upgrade ladder on node {}: sandbox {} already at cap {} usec, skip", nodeID_, poolDir,
                   config_.capQuotaUsec);
        return true;
    }
    const uint64_t newQuota = std::min(
        static_cast<uint64_t>(current.quotaUsec * config_.stepRatio + 0.5), config_.capQuotaUsec);
    if (newQuota <= current.quotaUsec) {
        return true; // ratio rounding left nothing to add
    }
    const uint64_t stepMilli = QuotaToMilli(newQuota, current.periodUsec) -
                               QuotaToMilli(current.quotaUsec, current.periodUsec);
    if (lastRateMilli_ + stepMilli >
        static_cast<uint64_t>(config_.safetyRatio * config_.nodeCapacityMilli)) {
        return false; // node admission denies this rung
    }
    CpuMax updated;
    updated.quotaUsec = newQuota;
    updated.periodUsec = current.periodUsec;
    if (!WriteCpuMax(dir + "/cpu.max", updated)) {
        YRLOG_WARN("cpu upgrade ladder on node {}: failed to raise cpu.max for sandbox {}, will retry", nodeID_,
                   poolDir);
        return false;
    }
    YRLOG_INFO("cpu upgrade ladder on node {}: raised sandbox {} cpu quota {} -> {} usec / period {} (usage {} "
               "milli / cap {} milli, safety {})",
               nodeID_, poolDir, current.quotaUsec, newQuota, current.periodUsec, lastRateMilli_,
               config_.nodeCapacityMilli, config_.safetyRatio);
    return true;
}

}  // namespace functionsystem::local_scheduler
