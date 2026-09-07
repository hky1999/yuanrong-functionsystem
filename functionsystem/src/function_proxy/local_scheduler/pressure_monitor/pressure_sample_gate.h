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

#ifndef LOCAL_SCHEDULER_PRESSURE_SAMPLE_GATE_H
#define LOCAL_SCHEDULER_PRESSURE_SAMPLE_GATE_H

#include <cstdint>
#include <optional>

namespace functionsystem::local_scheduler {
// Actor-owned generation fence for the asynchronous sampling chain.
class PressureSampleGate {
public:
    std::optional<uint64_t> Begin()
    {
        if (active_) {
            return std::nullopt;
        }
        active_ = true;
        return ++generation_;
    }

    bool Active(uint64_t generation) const
    {
        return active_ && generation == generation_;
    }

    // Both successful completion and timeout retire only their own cycle.
    bool Finish(uint64_t generation)
    {
        if (!Active(generation)) {
            return false;
        }
        active_ = false;
        return true;
    }

private:
    bool active_ = false;
    uint64_t generation_ = 0;
};
}  // namespace functionsystem::local_scheduler
#endif
