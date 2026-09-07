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

#ifndef LOCAL_SCHEDULER_RESOURCE_SAMPLE_POLICY_H
#define LOCAL_SCHEDULER_RESOURCE_SAMPLE_POLICY_H

#include <cmath>
#include <cstdint>
#include <string>
#include "common/resource_view/resource_sample_freshness.h"

namespace functionsystem::local_scheduler {
using resource_view::UsableNodeMemorySampleTime;

// Conservative eligibility only: LLM/tool/exec state must independently allow park.
// Templated to keep this decision independently testable without actor dependencies.
template <typename Sample>
bool ResourceSampleAllowsPark(const Sample &sample, const std::string &runtimeID, int64_t nowMs,
                             int64_t maxAgeMs, double maxIdleCpuCores)
{
    if (runtimeID.empty() || sample.runtime_id() != runtimeID || sample.sandbox_id().empty()
        || sample.resource_generation().empty() || nowMs <= 0 || maxAgeMs <= 0
        || sample.sampled_at_unix_ms() <= 0 || sample.sampled_at_unix_ms() > nowMs
        || nowMs - sample.sampled_at_unix_ms() > maxAgeMs) {
        return false;
    }
    return sample.cpu_usage_known() && std::isfinite(sample.cpu_usage_cores())
        && sample.cpu_usage_cores() >= 0.0 && std::isfinite(maxIdleCpuCores) && maxIdleCpuCores >= 0.0
        && sample.cpu_usage_cores() <= maxIdleCpuCores;
}
}  // namespace functionsystem::local_scheduler
#endif
