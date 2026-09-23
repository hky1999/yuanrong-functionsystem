/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "common/resource_view/memory_elasticity_config.h"
#include <gtest/gtest.h>

namespace functionsystem::test {

// The scheduler-side completion contract must match the daemon's enforced
// memory.high = floor(memory_high_ratio * max) page-aligned down. Default
// ratio 0.9 mirrors the canonical chart sandboxd config; values verified
// against physical readbacks (case-2d 128MiB, case-3 256MiB).
TEST(MemoryHighRatioTest, DefaultRatioMatchesPhysicalReadbacks)
{
    EXPECT_DOUBLE_EQ(resource_view::MemoryHighRatio(), 0.9);
    EXPECT_EQ(resource_view::ExpectedHighBytes(134217728), 120795136u);  // 0.9 * 128MiB
    EXPECT_EQ(resource_view::ExpectedHighBytes(268435456), 241590272u);  // 0.9 * 256MiB
    EXPECT_EQ(resource_view::ExpectedHighBytes(536870912), 483180544u);  // 0.9 * 512MiB
}

TEST(MemoryHighRatioTest, EdgeLimits)
{
    EXPECT_EQ(resource_view::ExpectedHighBytes(0), 0u);
    // one-page max: floor(0.9*4096/4096)=0 -> 0 (no enforced high below a page)
    EXPECT_EQ(resource_view::ExpectedHighBytes(4096), 0u);
}

}  // namespace functionsystem::test
