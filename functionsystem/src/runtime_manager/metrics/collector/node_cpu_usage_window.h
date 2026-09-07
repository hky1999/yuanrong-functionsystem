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

#ifndef FUNCTIONSYSTEM_NODE_CPU_USAGE_WINDOW_H
#define FUNCTIONSYSTEM_NODE_CPU_USAGE_WINDOW_H

#include <array>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <sstream>
#include <string>

namespace functionsystem::runtime_manager {
// The online list is compact; /proc/cpuinfo can exceed ProcFSTools' 10 KiB
// read bound even on ordinary 32-core nodes. Reject truncated/malformed lists.
inline std::optional<uint64_t> CountOnlineCPUs(std::string text)
{
    if (text.empty() || text.size() >= 10240) { return {}; }
    while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) { text.pop_back(); }
    uint64_t count = 0, previousEnd = 0;
    bool first = true;
    size_t offset = 0;
    const auto number = [](const std::string &part, uint64_t &value) {
        const auto result = std::from_chars(part.data(), part.data() + part.size(), value);
        return result.ec == std::errc{} && result.ptr == part.data() + part.size();
    };
    while (offset < text.size()) {
        const auto comma = text.find(',', offset);
        const auto part = text.substr(offset, comma == std::string::npos ? comma : comma - offset);
        const auto dash = part.find('-');
        uint64_t start = 0, end = 0;
        if (!number(part.substr(0, dash), start)) { return {}; }
        if (dash == std::string::npos) { end = start; }
        else if (!number(part.substr(dash + 1), end)) { return {}; }
        if (end < start || end > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())
            || (!first && start <= previousEnd)) { return {}; }
        count += end - start + 1;
        previousEnd = end;
        first = false;
        if (comma == std::string::npos) { break; }
        offset = comma + 1;
        if (offset == text.size()) { return {}; }
    }
    return count > 0 ? std::optional{count} : std::nullopt;
}

// /proc/stat USER_HZ counters become millicores over the measured monotonic
// interval. guest/guest_nice are already included in user/nice. Steal is busy
// capacity unavailable to this node; idle and iowait are not executing CPU.
// The collector serializes access. A missing sample breaks the delta chain.
class NodeCPUUsageWindow {
public:
    struct Sample { double millicores; int64_t startUnixMs; int64_t durationNs; };
    void Reset() { previous_.reset(); }
    std::optional<Sample> Observe(const std::string &text, int64_t monotonicNs,
                                  int64_t unixMs, long ticksPerSecond)
    {
        const auto counters = Parse(text);
        if (!counters || monotonicNs <= 0 || unixMs <= 0 || ticksPerSecond <= 0) {
            Reset();
            return {};
        }
        Point current{*counters, monotonicNs, unixMs};
        const auto before = previous_;
        previous_ = current;
        if (!before || monotonicNs <= before->monotonicNs || unixMs < before->unixMs) { return {}; }
        const int64_t duration = monotonicNs - before->monotonicNs;
        if (duration > 30000000000LL) { return {}; }
        uint64_t busy = 0;
        for (size_t i = 0; i < current.counters.size(); ++i) {
            if (current.counters[i] < before->counters[i]) { return {}; }
            if (i == 3 || i == 4) { continue; }
            const auto delta = current.counters[i] - before->counters[i];
            if (delta > std::numeric_limits<uint64_t>::max() - busy) { return {}; }
            busy += delta;
        }
        const double rate = static_cast<double>(busy) / static_cast<double>(ticksPerSecond)
                            * 1e12 / static_cast<double>(duration);
        if (!std::isfinite(rate)) { return {}; }
        return Sample{rate, before->unixMs, duration};
    }
private:
    struct Point { std::array<uint64_t, 8> counters; int64_t monotonicNs; int64_t unixMs; };
    static std::optional<std::array<uint64_t, 8>> Parse(const std::string &text)
    {
        if (text.size() > 4 * 1024 * 1024) { return {}; }
        std::istringstream row(text.substr(0, text.find('\n')));
        std::string token;
        if (!(row >> token) || token != "cpu") { return {}; }
        std::array<uint64_t, 8> counters{};
        size_t count = 0;
        while (row >> token) {
            uint64_t value = 0;
            const auto result = std::from_chars(token.data(), token.data() + token.size(), value);
            if (result.ec != std::errc{} || result.ptr != token.data() + token.size() || count >= 10) { return {}; }
            if (count < counters.size()) { counters[count] = value; }
            ++count;
        }
        return count >= counters.size() ? std::optional{counters} : std::nullopt;
    }
    std::optional<Point> previous_;
};
}
#endif
