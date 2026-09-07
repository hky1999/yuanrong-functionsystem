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

#ifndef FUNCTIONSYSTEM_RESOURCE_SAMPLE_FRESHNESS_H
#define FUNCTIONSYSTEM_RESOURCE_SAMPLE_FRESHNESS_H

#include <algorithm>
#include <cstdint>
#include <limits>

namespace functionsystem::resource_view {
constexpr int64_t DEFAULT_RESOURCE_SAMPLE_MAX_AGE_MS = 30000;

// Aggregate usage is usable only while every contributing leaf is fresh.
// Return the oldest collection time, so an async decision can check age again.
template <typename Unit>
int64_t UsableNodeMemorySampleTime(const Unit &unit, int64_t nowMs, int64_t maxAgeMs)
{
    if (nowMs <= 0 || maxAgeMs <= 0) { return 0; }
    if (unit.fragment().empty()) {
        const auto sampleMs = unit.memory_sample_unix_ms();
        return sampleMs > 0 && sampleMs <= nowMs && nowMs - sampleMs <= maxAgeMs ? sampleMs : 0;
    }
    int64_t oldest = std::numeric_limits<int64_t>::max();
    for (const auto &entry : unit.fragment()) {
        const auto sampleMs = UsableNodeMemorySampleTime(entry.second, nowMs, maxAgeMs);
        if (sampleMs == 0) { return 0; }
        oldest = std::min(oldest, sampleMs);
    }
    return oldest;
}

// CPU has its own window: a fresh Memory sample cannot make missing or old
// CPU usage safe for admission. Aggregate CPU requires every leaf to be fresh.
template <typename Unit>
int64_t UsableNodeCPUSampleTime(const Unit &unit, int64_t nowMs, int64_t maxAgeMs)
{
    if (nowMs <= 0 || maxAgeMs <= 0) { return 0; }
    if (unit.fragment().empty()) {
        const auto sampleMs = unit.cpu_sample_unix_ms();
        return sampleMs > 0 && sampleMs <= nowMs && nowMs - sampleMs <= maxAgeMs ? sampleMs : 0;
    }
    int64_t oldest = std::numeric_limits<int64_t>::max();
    for (const auto &entry : unit.fragment()) {
        const auto sampleMs = UsableNodeCPUSampleTime(entry.second, nowMs, maxAgeMs);
        if (sampleMs == 0) { return 0; }
        oldest = std::min(oldest, sampleMs);
    }
    return oldest;
}

}  // namespace functionsystem::resource_view
#endif
