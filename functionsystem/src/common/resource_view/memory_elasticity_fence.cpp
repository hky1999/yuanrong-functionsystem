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

#include "memory_elasticity_fence.h"

#include <atomic>

#include "memory_elasticity_config.h"

namespace functionsystem::resource_view {
namespace {
// -1 keeps the process-start opt-in; 0/1 are unit-test overrides.
std::atomic<int> g_fenceOverride{-1};
}

bool MemoryElasticityFenceEnabled()
{
    const int overriden = g_fenceOverride.load(std::memory_order_relaxed);
    return overriden >= 0 ? overriden != 0 : MemoryElasticityEnabled();
}

int MemoryElasticityFenceOverrideState()
{
    return g_fenceOverride.load(std::memory_order_relaxed);
}

void SetMemoryElasticityFenceForTest(bool enabled)
{
    g_fenceOverride.store(enabled ? 1 : 0, std::memory_order_relaxed);
}

void ResetMemoryElasticityFenceOverride()
{
    g_fenceOverride.store(-1, std::memory_order_relaxed);
}
}  // namespace functionsystem::resource_view
