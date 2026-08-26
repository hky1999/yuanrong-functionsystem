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

#include "upgrade_ladder.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>

#include "common/logs/logging.h"
#include "common/resource_view/commitment_ledger.h"

namespace functionsystem::local_scheduler {

namespace {

// Plain-text readers/writers for the fake-cgroup layout (kernel cgroupfs and
// the unit-test stand-in both speak newline-free decimal bytes).
bool ReadUint64File(const std::string &path, uint64_t &out)
{
    std::ifstream file(path);
    if (!file.is_open()) {
        return false;
    }
    uint64_t value = 0;
    if (!(file >> value)) {
        return false;
    }
    out = value;
    return true;
}

bool WriteUint64File(const std::string &path, uint64_t value)
{
    std::ofstream file(path, std::ios::trunc);
    if (!file.is_open()) {
        return false;
    }
    file << value;
    return file.good();
}

// P2.0: kernel-maintained scope aggregate = sum of memory.current over the
// immediate pool dirs. The ledger's decay feed (OnNodeUsage) speaks the
// view's series, so the anchor must not sit below the real aggregate -- a
// low anchor turns the view's later catch-up into phantom "growth" that
// cashes the promise back in (seen live 2026-08-24: 2G commitment decayed
// to 124MB with no real growth). No dir exposes memory.current (fake trees
// in unit tests) -> false, caller falls back to the view sample.
bool ReadScopeCurrentBytes(const std::string &root, uint64_t &out)
{
    out = 0;
    bool any = false;
    std::error_code ec;
    for (const auto &entry : std::filesystem::directory_iterator(root, ec)) {
        if (!entry.is_directory()) {
            continue;
        }
        uint64_t current = 0;
        if (ReadUint64File(entry.path().string() + "/memory.current", current)) {
            out += current;
            any = true;
        }
    }
    return any && out > 0;
}

}  // namespace

UpgradeLadder::UpgradeLadder(const std::string &nodeID, UpgradeLadderConfig config, std::string cgroupRoot)
    : nodeID_(nodeID), config_(config), cgroupRoot_(std::move(cgroupRoot))
{
}

void UpgradeLadder::OnThrottleEvent(const std::string &poolDir, uint64_t increment)
{
    if (!config_.enabled || poolDir.empty()) {
        return;
    }
    if (TryRaise(poolDir, lastActualUseBytes_, lastCapacityBytes_)) {
        return;
    }
    // denied (or transiently unwritable): park in the deferred set; the
    // sandbox keeps running throttled at its current memory.high
    const size_t before = deferredDirs_.size();
    deferredDirs_.insert(poolDir);
    if (deferredDirs_.size() != before) {
        YRLOG_INFO("upgrade ladder on node {}: sandbox {} throttled at high (+{}) deferred by watermark "
                   "(use {} / cap {} bytes, safety {})",
                   nodeID_, poolDir, increment, lastActualUseBytes_, lastCapacityBytes_, config_.safetyRatio);
    }
}

bool UpgradeLadder::OnNodeUsage(uint64_t actualUseBytes, uint64_t capacityBytes)
{
    if (!config_.enabled) {
        return false;
    }
    lastActualUseBytes_ = actualUseBytes;
    lastCapacityBytes_ = capacityBytes;
    // P2.0: every sample cashes the promise ledger in (growth eats
    // commitment) even when nothing is deferred
    resource_view::CommitmentLedger::OnUsage(nodeID_, actualUseBytes);
    if (deferredDirs_.empty()) {
        return false;
    }
    // at most one release per sample: simultaneous warm-ups re-queue instead
    // of bursting through the node budget in a single window
    for (const auto &poolDir : deferredDirs_) {
        if (TryRaise(poolDir, actualUseBytes, capacityBytes)) {
            deferredDirs_.erase(poolDir);
            return true;
        }
        // vanished pool dir (recycled id) or dir already at cap: stop deferring
        uint64_t currentMax = 0;
        if (!ReadUint64File(cgroupRoot_ + "/" + poolDir + "/memory.max", currentMax)) {
            deferredDirs_.erase(poolDir);
            return false;
        }
        return false; // still over budget — retry on the next sample
    }
    return false;
}

bool UpgradeLadder::TryRaise(const std::string &poolDir, uint64_t actualUseBytes, uint64_t capacityBytes)
{
    if (capacityBytes == 0) {
        return false; // fail closed without a usage sample
    }
    const std::string dir = cgroupRoot_ + "/" + poolDir;
    uint64_t currentMax = 0;
    if (!ReadUint64File(dir + "/memory.max", currentMax)) {
        YRLOG_DEBUG("upgrade ladder on node {}: sandbox {} has no readable memory.max, skip", nodeID_, poolDir);
        return false;
    }
    if (currentMax >= config_.capBytes) {
        YRLOG_INFO("upgrade ladder on node {}: sandbox {} already at cap {} bytes, skip", nodeID_, poolDir,
                   config_.capBytes);
        return true; // nothing to defer either
    }
    // D-3①/P2.0: the caller's node numbers are one view sample old; re-read
    // the kernel-maintained scope aggregate (sum over pool dirs) and tighten
    // admission AND the commitment anchor to the max of both. The aggregate
    // is where the lagging view will settle (live 2026-08-24 v4: kernel
    // 6409MB at raise, view settled at 6533MB), so a promise anchored there
    // survives the view's catch-up; the ledger separately grace-periods the
    // first post-raise samples so the stale view reads below the anchor are
    // not booked as a drop. The fake cgroup tree has no memory.current ->
    // read failure falls back to the sample, preserving unit-test behavior.
    uint64_t freshUseBytes = 0;
    if (ReadScopeCurrentBytes(cgroupRoot_, freshUseBytes)) {
        actualUseBytes = std::max(actualUseBytes, freshUseBytes);
    }
    // D-3③: one rung never exceeds the sandbox's own current max, so a small
    // cgroup climbs proportionally (2 GiB -> +2 GiB) instead of jumping the
    // global step (8 GiB) past its booked size in one go; large sandboxes are
    // unaffected (step < currentMax there).
    const uint64_t stepBytes = std::min(config_.stepBytes, currentMax);
    const uint64_t newMax = std::min(currentMax + stepBytes, config_.capBytes);
    // P2.0: self-admission must not spend promised-but-unrealized headroom
    // on another rung — the ledger's outstanding bytes ride the same safety
    // line, so ladder raises cannot amplify each other into overcommit
    const uint64_t outstanding = resource_view::CommitmentLedger::Outstanding(nodeID_);
    if (actualUseBytes + outstanding + stepBytes > static_cast<uint64_t>(config_.safetyRatio * capacityBytes)) {
        return false; // node admission denies this rung
    }
    if (!WriteUint64File(dir + "/memory.max", newMax)) {
        YRLOG_WARN("upgrade ladder on node {}: failed to raise memory.max for sandbox {}, will retry", nodeID_,
                   poolDir);
        return false;
    }
    const uint64_t newHigh = static_cast<uint64_t>(config_.highRatio * newMax);
    if (!WriteUint64File(dir + "/memory.high", newHigh)) {
        // max is already raised; a stale (lower) high only keeps the throttle
        // a bit longer, so log and treat the rung as done
        YRLOG_WARN("upgrade ladder on node {}: raised memory.max of sandbox {} to {} but failed to set high {}",
                   nodeID_, poolDir, newMax, newHigh);
        return true;
    }
    YRLOG_INFO("upgrade ladder on node {}: raised sandbox {} max {} -> {} bytes, high -> {} (use {} / cap {}"
               ", commitment {})",
               nodeID_, poolDir, currentMax, newMax, newHigh, actualUseBytes, capacityBytes, newMax - currentMax);
    // P2.0: publish the promised headroom; admission (filter + this ladder)
    // accounts for it until node usage growth cashes it in
    resource_view::CommitmentLedger::OnRaise(nodeID_, newMax - currentMax, actualUseBytes);
    return true;
}

}  // namespace functionsystem::local_scheduler
