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

#include "usage_trend_tracker.h"

#include <algorithm>
#include <chrono>

#include "common/logs/logging.h"

namespace functionsystem::resource_view {

int64_t UsageTrendTracker::windowMs_ = 60000;
int64_t UsageTrendTracker::horizonMs_ = 30000;

std::mutex &UsageTrendTracker::Mutex()
{
    static std::mutex m;
    return m;
}

std::map<std::string, std::deque<UsageTrendTracker::Sample>> &UsageTrendTracker::History()
{
    static std::map<std::string, std::deque<Sample>> h;
    return h;
}

void UsageTrendTracker::SetParams(int64_t windowMs, int64_t horizonMs)
{
    std::lock_guard<std::mutex> lock(Mutex());
    if (windowMs > 0) {
        windowMs_ = windowMs;
    }
    // horizon 0 disables prediction (Predict keeps returning the latest sample)
    horizonMs_ = std::max<int64_t>(horizonMs, 0);
    YRLOG_INFO("usage trend tracker params: windowMs={}, horizonMs={}", windowMs_, horizonMs_);
}

void UsageTrendTracker::Record(const std::string &unitId, double usedMb)
{
    auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count();
    std::lock_guard<std::mutex> lock(Mutex());
    auto &deq = History()[unitId];
    deq.push_back(Sample{nowMs, usedMb});
    // bound memory: drop samples older than the window, then hard-cap length
    // (clock non-monotonic edge or very short window)
    while (!deq.empty() && nowMs - deq.front().tsMs > windowMs_) {
        deq.pop_front();
    }
    while (deq.size() > MAX_SAMPLES) {
        deq.pop_front();
    }
}

double UsageTrendTracker::Predict(const std::string &unitId)
{
    if (horizonMs_ == 0) {
        return -1.0;
    }
    std::lock_guard<std::mutex> lock(Mutex());
    auto it = History().find(unitId);
    if (it == History().end() || it->second.size() < 2) {
        return -1.0;
    }
    const auto &deq = it->second;
    // worst-case (max) slope over consecutive pairs: catches a burst that is
    // still ramping instead of averaging it away
    double worstSlopeMbPerMs = 0.0;
    // D-6 F2: only the most recent pairs set the slope -- a burst that ended
    // tens of seconds ago must stop inflating the estimate while its samples
    // still sit inside the window (live-observed: a dd fill ramp kept the
    // wake/restore admission rejected long after the sandboxes were parked)
    constexpr size_t RECENT_PAIRS = 4;
    const size_t first = deq.size() > RECENT_PAIRS ? deq.size() - RECENT_PAIRS : 1;
    for (size_t i = first; i < deq.size(); ++i) {
        auto dt = deq[i].tsMs - deq[i - 1].tsMs;
        if (dt <= 0) {
            continue;
        }
        double slope = (deq[i].usedMb - deq[i - 1].usedMb) / static_cast<double>(dt);
        worstSlopeMbPerMs = std::max(worstSlopeMbPerMs, slope);
    }
    double latest = deq.back().usedMb;
    if (worstSlopeMbPerMs <= 0.0) {
        return latest;
    }
    return latest + worstSlopeMbPerMs * static_cast<double>(horizonMs_);
}

}  // namespace functionsystem::resource_view
