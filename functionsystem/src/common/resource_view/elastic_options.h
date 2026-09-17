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

#ifndef FUNCTIONSYSTEM_ELASTIC_OPTIONS_H
#define FUNCTIONSYSTEM_ELASTIC_OPTIONS_H

#include "resource_type.h"
#include "common/status/status.h"

namespace functionsystem::resource_view {

// Where the canonical flat elastic marker came from. NONE means the request
// carries no elastic intent and stays on the legacy contract.
enum class ElasticOptionOrigin { NONE, FLAT, NESTED_EXTRA_CONFIG };

// Surface the SDK elastic marker as a sibling create option before the first
// scheduling admission. The SDK ships scheduler_elastic_resources inside the
// opaque extra_config JSON map, while admission, sampling and retirement read
// the flat key; this makes one canonical marker visible to every consumer.
// Only the elastic marker is extracted - arbitrary extra_config keys are never
// promoted to trusted scheduling options. Requests without elastic intent are
// left unchanged. A marker that is present but malformed, conflicting with the
// flat key, or paired with unusable elastic resources is rejected instead of
// silently downgraded to a plain instance.
Status NormalizeElasticCreateOptions(InstanceInfo *instance, bool *elastic = nullptr,
                                     ElasticOptionOrigin *origin = nullptr);

// Validate the explicit CPU and Memory scalars an elastic request must carry
// (same rules as the birth policy in SandboxdRequestBuilder::ApplyElasticResources).
Status ValidateElasticResources(const InstanceInfo &instance);

}  // namespace functionsystem::resource_view

#endif
