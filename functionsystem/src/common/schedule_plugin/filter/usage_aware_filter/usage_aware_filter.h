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

#ifndef FUNCTIONSYSTEM_USAGE_AWARE_FILTER_H
#define FUNCTIONSYSTEM_USAGE_AWARE_FILTER_H

#include <mutex>

#include "common/proto/pb/posix/resource.pb.h"
#include "common/resource_view/resource_type.h"
#include "common/schedule_plugin/common/preallocated_context.h"
#include "common/scheduler_framework/framework/policy.h"
#include "common/status/status.h"

namespace functionsystem::schedule_plugin::filter {

// Usage-aware admission (gVisor overcommit): pass when the node's real usage
// plus a per-instance reserve fits inside a safety fraction of the node
// capacity, i.e.  actualuse(Memory) + reserve <= capacity(Memory) * safety.
// The booked/allocatable check stays in DefaultFilter as the guaranteed
// fallback; this filter only ever adds constraints on top of it.
class UsageAwareFilter : public schedule_framework::FilterPlugin {
public:
    UsageAwareFilter() = default;
    ~UsageAwareFilter() override = default;
    std::string GetPluginName() override;

    schedule_framework::Filtered Filter(const std::shared_ptr<schedule_framework::ScheduleContext> &ctx,
                                        const resource_view::InstanceInfo &instance,
                                        const resource_view::ResourceUnit &resourceUnit) override;

    // set once by the local/domain scheduler driver before plugin creation
    // (creators take no arguments); defaults keep the filter self-contained.
    static void SetConfig(double safety, double floorMb);

    static double GetSafety();
    static double GetFloorMb();

private:
    static std::mutex configMutex_;
    static double safety_;
    static double floorMb_;
};
}  // namespace functionsystem::schedule_plugin::filter

#endif  // FUNCTIONSYSTEM_USAGE_AWARE_FILTER_H
