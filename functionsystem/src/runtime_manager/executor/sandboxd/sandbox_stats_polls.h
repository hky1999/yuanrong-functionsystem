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

#ifndef RUNTIME_MANAGER_EXECUTOR_SANDBOXD_SANDBOX_STATS_POLLS_H
#define RUNTIME_MANAGER_EXECUTOR_SANDBOXD_SANDBOX_STATS_POLLS_H

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace functionsystem::runtime_manager {

// Owned exclusively by the executor actor. Every timer/RPC gets a unique
// ticket, including when a logical runtime ID is reused after restore.
class SandboxStatsPolls {
public:
    uint64_t Start(const std::string &runtimeID)
    {
        const auto ticket = NewTicket();
        polls_[runtimeID] = {ticket, false};
        return ticket;
    }

    bool Begin(const std::string &runtimeID, uint64_t ticket)
    {
        auto it = polls_.find(runtimeID);
        if (it == polls_.end() || it->second.ticket != ticket || it->second.inFlight) {
            return false;
        }
        it->second.inFlight = true;
        return true;
    }

    std::optional<uint64_t> Complete(const std::string &runtimeID, uint64_t ticket)
    {
        auto it = polls_.find(runtimeID);
        if (it == polls_.end() || it->second.ticket != ticket || !it->second.inFlight) {
            return std::nullopt;
        }
        const auto next = NewTicket();
        it->second = {next, false};
        return next;
    }

    void Stop(const std::string &runtimeID)
    {
        polls_.erase(runtimeID);
    }

    static int64_t NextDelayMs(int64_t elapsedMs, int64_t intervalMs)
    {
        // A slow/failed request must not cause a zero-delay retry loop. Negative
        // elapsed values are clamped before subtraction to avoid overflow.
        return std::max<int64_t>(100, std::max<int64_t>(0, intervalMs) - std::max<int64_t>(0, elapsedMs));
    }

private:
    uint64_t NewTicket()
    {
        if (nextTicket_ == std::numeric_limits<uint64_t>::max()) {
            throw std::overflow_error("sandbox stats poll ticket exhausted");
        }
        return ++nextTicket_;
    }

    struct Poll {
        uint64_t ticket;
        bool inFlight;
    };
    uint64_t nextTicket_ = 0;
    std::unordered_map<std::string, Poll> polls_;
};

}  // namespace functionsystem::runtime_manager
#endif
