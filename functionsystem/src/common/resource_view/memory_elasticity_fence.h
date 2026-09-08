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

#ifndef FUNCTIONSYSTEM_MEMORY_ELASTICITY_FENCE_H
#define FUNCTIONSYSTEM_MEMORY_ELASTICITY_FENCE_H

namespace functionsystem::resource_view {

// Production gate for the bidirectional memory-elasticity contract (revision
// fences, shrink budget, conservative recovery). Defaults to the process-start
// opt-in in memory_elasticity_config.h; the process-start flag is cached, so
// unit tests flip this override instead. Production code must not call the
// setter.
bool MemoryElasticityFenceEnabled();
void SetMemoryElasticityFenceForTest(bool enabled);

}  // namespace functionsystem::resource_view

#endif  // FUNCTIONSYSTEM_MEMORY_ELASTICITY_FENCE_H
