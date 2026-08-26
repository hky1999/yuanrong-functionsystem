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

#ifndef FUNCTIONSYSTEM_USAGE_TREND_TRACKER_H
#define FUNCTIONSYSTEM_USAGE_TREND_TRACKER_H

#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <string>

namespace functionsystem::resource_view {

// Rolling per-unit memory actualuse history, fed from ResourceView
// UPDATE_ACTUAL and consumed by UsageAwareFilter (D-form admission:
// account for where usage is heading, not just the last sample, so a
// burst that outruns the sampling interval cannot slip through admission).
class UsageTrendTracker {
public:
    // set once by the scheduler driver before any traffic; defaults keep
    // the tracker self-contained (60s window, 30s look-ahead)
    static void SetParams(int64_t windowMs, int64_t horizonMs);

    // record one actualuse sample (node memory, MB) for the unit; thread-safe
    static void Record(const std::string &unitId, double usedMb);

    // conservative prediction: latest sample + worst consecutive-pair slope
    // over the recent pairs (D-6 F2: recency-bounded so an ended burst stops
    // inflating the estimate), extrapolated by the horizon; never below the
    // latest sample. Returns -1.0 when there is not enough history to predict.
    static double Predict(const std::string &unitId);

private:
    struct Sample {
        int64_t tsMs;
        double usedMb;
    };

    static std::mutex &Mutex();
    static std::map<std::string, std::deque<Sample>> &History();

    static int64_t windowMs_;
    static int64_t horizonMs_;
    static constexpr size_t MAX_SAMPLES = 16;
};
}  // namespace functionsystem::resource_view

#endif  // FUNCTIONSYSTEM_USAGE_TREND_TRACKER_H
