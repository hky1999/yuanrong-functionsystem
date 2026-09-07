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

#ifndef LOCAL_SCHEDULER_PRESSURE_EVENT_QUEUE_H
#define LOCAL_SCHEDULER_PRESSURE_EVENT_QUEUE_H

#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

namespace functionsystem::local_scheduler {

// Actor-owned FIFO of pressure sources. Repeated events for an already
// pending sandbox coalesce without moving it ahead of other sandboxes.
class PressureEventQueue {
public:
    void Push(const std::string &poolDir, uint64_t increment)
    {
        if (poolDir.empty() || increment == 0) {
            return;
        }
        auto inserted = increments_.emplace(poolDir, increment);
        if (inserted.second) {
            order_.push_back(poolDir);
            return;
        }
        auto &pending = inserted.first->second;
        const auto remaining = std::numeric_limits<uint64_t>::max() - pending;
        pending += increment > remaining ? remaining : increment;
    }

    std::optional<std::pair<std::string, uint64_t>> Pop()
    {
        if (order_.empty()) {
            return std::nullopt;
        }
        auto name = std::move(order_.front());
        order_.pop_front();
        const auto increment = increments_.at(name);
        increments_.erase(name);
        return std::make_pair(std::move(name), increment);
    }

    size_t Size() const
    {
        return order_.size();
    }

private:
    std::deque<std::string> order_;
    std::unordered_map<std::string, uint64_t> increments_;
};

}  // namespace functionsystem::local_scheduler
#endif
