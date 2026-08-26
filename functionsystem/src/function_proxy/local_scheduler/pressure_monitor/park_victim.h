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

#ifndef LOCAL_SCHEDULER_PRESSURE_MONITOR_PARK_VICTIM_H
#define LOCAL_SCHEDULER_PRESSURE_MONITOR_PARK_VICTIM_H

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace functionsystem::local_scheduler {

// D-4 eviction order input for one parkable instance. llm_state joins here
// once the Ph1.5 LLM gateway publishes per-instance WAITING/PARKABLE state.
struct ParkCandidate {
    std::string instanceID;
    int32_t priority{ 0 };  // lower parks first (preemption convention)
    double useMb{ 0.0 };    // reclaim proxy; larger breaks priority ties
};

// Pick the next pressure-park victim: lowest priority first, then the larger
// reclaim. Instances that never set a priority default to 0 and degrade to
// the historical largest-reclaim order among themselves.
std::optional<ParkCandidate> SelectParkVictim(std::vector<ParkCandidate> candidates);

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_PRESSURE_MONITOR_PARK_VICTIM_H
