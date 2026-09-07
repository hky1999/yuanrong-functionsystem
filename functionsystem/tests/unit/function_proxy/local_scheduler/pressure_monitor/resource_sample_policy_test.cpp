#include "local_scheduler/pressure_monitor/resource_sample_policy.h"
#include "common/proto/pb/posix/resource.pb.h"

#include <gtest/gtest.h>
#include <limits>

namespace functionsystem::test {
TEST(ResourceSamplePolicyTest, RequiresRecentKnownIdleCpuAndRuntimeIdentity)
{
    resources::SandboxResourceSample sample;
    sample.set_runtime_id("runtime-1");
    sample.set_sandbox_id("sandbox-1");
    sample.set_resource_generation("generation-1");
    sample.set_sampled_at_unix_ms(1000);
    auto eligible = [&](int64_t nowMs = 1001) {
        return local_scheduler::ResourceSampleAllowsPark(sample, "runtime-1", nowMs, 30000, 0.02);
    };
    EXPECT_FALSE(eligible()); // first observation has no CPU interval
    sample.set_cpu_usage_known(true);
    EXPECT_TRUE(eligible());
    EXPECT_TRUE(eligible(31000));
    EXPECT_FALSE(eligible(31001)); // async decision delay rechecks age
    EXPECT_FALSE(eligible(999)); // future timestamp is not a fresh idle sample
    sample.set_cpu_usage_cores(0.02);
    EXPECT_TRUE(eligible());
    sample.set_cpu_usage_cores(0.021);
    EXPECT_FALSE(eligible()); // idle traffic does not imply idle computation
    sample.set_cpu_usage_cores(std::numeric_limits<double>::quiet_NaN());
    EXPECT_FALSE(eligible());
    sample.set_cpu_usage_cores(std::numeric_limits<double>::infinity());
    EXPECT_FALSE(eligible());
    sample.set_cpu_usage_cores(-1.0);
    EXPECT_FALSE(eligible());
    sample.set_cpu_usage_cores(0.0);
    sample.set_runtime_id("old-runtime");
    EXPECT_FALSE(eligible());
    sample.set_runtime_id("runtime-1");
    sample.clear_resource_generation();
    EXPECT_FALSE(eligible());
    sample.set_resource_generation("generation-1");
    sample.clear_sandbox_id();
    EXPECT_FALSE(eligible());
}
TEST(ResourceSamplePolicyTest, EveryAggregateLeafMustHaveFreshMemory)
{
    resources::ResourceUnit unit;
    auto check = [&] { return local_scheduler::UsableNodeMemorySampleTime(unit, 2000, 30000); };
    EXPECT_EQ(check(), 0);
    unit.set_memory_sample_unix_ms(1000);
    EXPECT_EQ(check(), 1000);
    (*unit.mutable_fragment())["a"].set_memory_sample_unix_ms(1500);
    (*unit.mutable_fragment())["b"].set_memory_sample_unix_ms(1000);
    EXPECT_EQ(check(), 1000);
    (*unit.mutable_fragment())["b"].set_memory_sample_unix_ms(2001);
    EXPECT_EQ(check(), 0);
    (*unit.mutable_fragment())["b"].set_memory_sample_unix_ms(0);
    EXPECT_EQ(check(), 0);
}
}  // namespace functionsystem::test
