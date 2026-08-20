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

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

#include "runtime_manager/metrics/collector/system_proc_memory_collector.h"

namespace functionsystem::test {

using runtime_manager::Metrics;
using runtime_manager::metrics_type::MEMORY;
using runtime_manager::collector_type::INSTANCE;

namespace fs = std::filesystem;

namespace {

// one instance MEMORY metric for the legacy-sum fallback path
std::vector<litebus::Future<Metrics>> InstanceMetrics(double usageMb)
{
    litebus::Promise<Metrics> promise;
    promise.SetValue({ { usageMb }, {}, { std::string("inst-1") }, {}, MEMORY, INSTANCE, {} });
    return { promise.GetFuture() };
}

void WriteFile(const fs::path &p, const std::string &content)
{
    std::ofstream f(p);
    f << content;
}

}  // namespace

/**
 * Feature: SystemProcMemoryCollector
 * Description: Generate filter
 * Expectation: system-Memory
 */
TEST(SystemProcMemoryCollectorTest, GenFilter)
{
    runtime_manager::SystemProcMemoryCollector collector(41240.0, [] {
        return std::vector<litebus::Future<Metrics>>{};
    });
    EXPECT_EQ(collector.GenFilter(), "system-Memory");
}

TEST(SystemProcMemoryCollectorTest, GetLimitReturnsBookedQuota)
{
    runtime_manager::SystemProcMemoryCollector collector(41240.0, [] {
        return std::vector<litebus::Future<Metrics>>{};
    });
    auto limit = collector.GetLimit();
    ASSERT_EQ(limit.value.IsNone(), false);
    EXPECT_DOUBLE_EQ(limit.value.Get(), 41240.0);
}

/**
 * Feature: SystemProcMemoryCollector
 * Description: W2 cgroup 口径——usage = Σ (<dir>/akernel/<id>/memory.current)
 * Steps: 两个沙箱 cgroup（2048MB + 1024MB）+ 一个非目录文件；instance 指标 512MB 应被忽略
 * Expectation: usage = 3072MB
 */
TEST(SystemProcMemoryCollectorTest, SandboxCgroupUsageSummed)
{
    auto base = fs::temp_directory_path() / ("w2_spm_" + std::to_string(::getpid()));
    fs::remove_all(base);
    fs::create_directories(base / "akernel" / "sbx1");
    fs::create_directories(base / "akernel" / "sbx2");
    WriteFile(base / "akernel" / "sbx1" / "memory.current", "2147483648\n");
    WriteFile(base / "akernel" / "sbx2" / "memory.current", "1073741824");
    WriteFile(base / "akernel" / "not_a_dir", "x");

    runtime_manager::SystemProcMemoryCollector collector(41240.0,
                                                         [] { return InstanceMetrics(512.0); },
                                                         (base / "akernel").string());
    auto usage = collector.GetUsage().Get();
    ASSERT_EQ(usage.value.IsNone(), false);
    EXPECT_DOUBLE_EQ(usage.value.Get(), 3072.0);

    fs::remove_all(base);
}

/**
 * Feature: SystemProcMemoryCollector
 * Description: 沙箱 cgroup 目录缺失时回退 instance 指标求和（旧口径兼容）
 * Expectation: usage = 512MB
 */
TEST(SystemProcMemoryCollectorTest, FallsBackToInstanceSumWhenDirMissing)
{
    runtime_manager::SystemProcMemoryCollector collector(
        41240.0, [] { return InstanceMetrics(512.0); }, "/nonexistent/w2/akernel");
    auto usage = collector.GetUsage().Get();
    ASSERT_EQ(usage.value.IsNone(), false);
    EXPECT_DOUBLE_EQ(usage.value.Get(), 512.0);
}

/**
 * Feature: SystemProcMemoryCollector
 * Description: 未配置沙箱目录（默认部署）保持旧口径
 * Expectation: usage = 512MB
 */
TEST(SystemProcMemoryCollectorTest, LegacyInstanceSumWithoutSandboxDir)
{
    runtime_manager::SystemProcMemoryCollector collector(41240.0, [] { return InstanceMetrics(512.0); });
    auto usage = collector.GetUsage().Get();
    ASSERT_EQ(usage.value.IsNone(), false);
    EXPECT_DOUBLE_EQ(usage.value.Get(), 512.0);
}

}  // namespace functionsystem::test
