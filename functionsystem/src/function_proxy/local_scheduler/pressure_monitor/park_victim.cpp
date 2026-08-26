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

#include "local_scheduler/pressure_monitor/park_victim.h"

#include <algorithm>

namespace functionsystem::local_scheduler {

std::optional<ParkCandidate> SelectParkVictim(std::vector<ParkCandidate> candidates)
{
    if (candidates.empty()) {
        return std::nullopt;
    }
    std::sort(candidates.begin(), candidates.end(), [](const ParkCandidate &l, const ParkCandidate &r) {
        if (l.priority != r.priority) {
            return l.priority < r.priority;  // lower priority parks first
        }
        return l.useMb > r.useMb;  // same priority: reclaim the most
    });
    return candidates.front();
}

}  // namespace functionsystem::local_scheduler
