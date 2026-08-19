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


#ifndef FUNCTIONSYSTEM_CONSTANTS_H
#define FUNCTIONSYSTEM_CONSTANTS_H
#include <string>

namespace functionsystem::schedule_plugin {

// prefilter name
const std::string DEFAULT_PREFILTER_NAME = "DefaultPreFilter";

// filter name
const std::string DEFAULT_FILTER_NAME = "DefaultFilter";
const std::string RESOURCE_SELECTOR_FILTER_NAME = "ResourceSelectorFilter";
const std::string DEFAULT_HETEROGENEOUS_FILTER_NAME = "DefaultHeterogeneousFilter";
const std::string LABEL_AFFINITY_FILTER_NAME = "LabelAffinityFilter";
const std::string RELAXED_ROOT_LABEL_AFFINITY_FILTER_NAME = "RelaxedRootLabelAffinityFilter";
const std::string STRICT_ROOT_LABEL_AFFINITY_FILTER_NAME = "StrictRootLabelAffinityFilter";
const std::string RELAXED_NON_ROOT_LABEL_AFFINITY_FILTER_NAME = "RelaxedNonRootLabelAffinityFilter";
const std::string STRICT_NON_ROOT_LABEL_AFFINITY_FILTER_NAME = "StrictNonRootLabelAffinityFilter";
const std::string DISK_FILTER_NAME = "DiskFilter";
const std::string NUMA_AFFINITY_FILTER_NAME = "NUMAAffinityFilter";
const std::string USAGE_AWARE_FILTER_NAME = "UsageAwareFilter";

// scorer name
const std::string DEFAULT_SCORER_NAME = "DefaultScorer";
const std::string DEFAULT_HETEROGENEOUS_SCORER_NAME = "DefaultHeterogeneousScorer";
const std::string LABEL_AFFINITY_SCORER_NAME = "LabelAffinityScorer";
const std::string RELAXED_LABEL_AFFINITY_SCORER_NAME = "RelaxedLabelAffinityScorer";
const std::string STRICT_LABEL_AFFINITY_SCORER_NAME = "StrictLabelAffinityScorer";
const std::string DISK_SCORER_NAME = "DiskScorer";
const std::string NUMA_AFFINITY_SCORER_NAME = "NUMAAffinityScorer";

const int64_t DEFAULT_SCORE = 100;
const float INVALID_SCORE = -1.0f;
const float MIN_SCORE_THRESHOLD = 0.1f;
const float BASE_SCORE_FACTOR = 1.0f;
const int INVALID_INDEX = -1;
const std::string MONOPOLY_MODE = "monopoly";

}
#endif  // FUNCTIONSYSTEM_CONSTANTS_H
