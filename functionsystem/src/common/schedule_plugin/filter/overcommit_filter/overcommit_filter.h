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

#ifndef FUNCTIONSYSTEM_OVERCOMMIT_FILTER_H
#define FUNCTIONSYSTEM_OVERCOMMIT_FILTER_H

#include <mutex>

#include "common/proto/pb/posix/resource.pb.h"
#include "common/resource_view/resource_type.h"
#include "common/schedule_plugin/common/preallocated_context.h"
#include "common/scheduler_framework/framework/policy.h"
#include "common/status/status.h"

namespace functionsystem::schedule_plugin::filter {

// Overcommit accounting on the scalar limit field (W2 step 3):
// an instance carrying  limit > value  declares itself best-effort —
// only `value` is booked against allocatable (partial book, DefaultFilter),
// the sandbox cgroup is enforced at `limit` (SandboxdRequestBuilder), and
// this filter bounds the sum of effective ceilings:
//     Σ max(limit, value) over resident instances + max(limit, value) of the
//     request  <=  capacity * overcommit_ratio
// Requests without a limit stay untouched (guaranteed class), so with the
// default ratio 1.0 and limit-less workloads the filter is a no-op.
class OvercommitFilter : public schedule_framework::FilterPlugin {
public:
    OvercommitFilter() = default;
    ~OvercommitFilter() override = default;
    std::string GetPluginName() override;

    schedule_framework::Filtered Filter(const std::shared_ptr<schedule_framework::ScheduleContext> &ctx,
                                        const resource_view::InstanceInfo &instance,
                                        const resource_view::ResourceUnit &resourceUnit) override;

    // set once by the local/domain scheduler driver before plugin creation
    // (creators take no arguments); defaults keep the filter self-contained.
    static void SetConfig(double ratio);

    static double GetRatio();

private:
    static std::mutex configMutex_;
    static double ratio_;
};
}  // namespace functionsystem::schedule_plugin::filter

#endif  // FUNCTIONSYSTEM_OVERCOMMIT_FILTER_H
