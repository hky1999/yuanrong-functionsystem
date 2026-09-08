#include <gtest/gtest.h>
#include "function_proxy/local_scheduler/pressure_monitor/pressure_resize_policy.h"

namespace functionsystem::test {
using namespace local_scheduler;
namespace {
constexpr uint64_t MiB = 1048576;
runtime::v1::ResizeResourcesRequest Command()
{
    runtime::v1::ResizeResourcesRequest c;
    c.set_sandbox_id("sandbox"); c.set_resource_generation("generation");
    c.set_operation_id("operation"); c.set_expected_revision(7); c.set_deadline_unix_ms(10000);
    c.mutable_expected()->set_cpu_quota_usec(50000); c.mutable_expected()->set_cpu_period_usec(100000);
    c.mutable_expected()->set_memory_limit_bytes(512 * MiB);
    *c.mutable_target() = c.expected(); c.mutable_target()->set_memory_limit_bytes(256 * MiB);
    return c;
}
resources::SandboxResourceSample Sample(uint64_t max)
{
    resources::SandboxResourceSample s;
    s.set_runtime_id("runtime"); s.set_sandbox_id("sandbox"); s.set_resource_generation("generation");
    s.set_resource_revision_known(true); s.set_resource_revision(8); s.set_resource_operation_id("operation");
    s.set_sampled_at_unix_ms(10100); s.set_memory_limit_known(true); s.set_memory_limit_bytes(max);
    s.set_memory_high_known(true); s.set_memory_high_bytes(static_cast<uint64_t>(0.8 * max / 4096) * 4096);
    s.set_cpu_limits_known(true); s.set_cpu_quota_usec(50000); s.set_cpu_period_usec(100000);
    s.set_frozen_known(true); return s;
}
}
TEST(MemoryResizeObservationTest, ClosedFailedShrinkRequiresExactOriginalControls)
{
    auto c = Command(); auto s = Sample(512 * MiB);
    EXPECT_TRUE(MemoryShrinkFailedTerminal(c, s));
    s.set_resource_operation_pending(true); EXPECT_FALSE(MemoryShrinkFailedTerminal(c, s));
    s.set_resource_operation_pending(false); s.set_memory_high_bytes(s.memory_high_bytes() - 4096);
    EXPECT_FALSE(MemoryShrinkFailedTerminal(c, s));
    s = Sample(512 * MiB); s.set_frozen_known(false); EXPECT_FALSE(MemoryShrinkFailedTerminal(c, s));
    s = Sample(512 * MiB); s.set_resource_revision(7); EXPECT_FALSE(MemoryShrinkFailedTerminal(c, s));
    s = Sample(256 * MiB); EXPECT_FALSE(MemoryShrinkFailedTerminal(c, s));
}
TEST(MemoryResizeObservationTest, NeverStartedRequiresFreshPostDeadlineSameRevision)
{
    auto c = Command(); auto s = Sample(512 * MiB);
    s.set_resource_revision(7); s.set_resource_operation_id("older-operation");
    EXPECT_TRUE(MemoryResizeNeverStarted(c, "runtime", s, 10200, 1000));
    s.set_sampled_at_unix_ms(10000); EXPECT_FALSE(MemoryResizeNeverStarted(c, "runtime", s, 10200, 1000));
    s.set_sampled_at_unix_ms(10100); s.set_resource_operation_pending(true);
    EXPECT_FALSE(MemoryResizeNeverStarted(c, "runtime", s, 10200, 1000));
    s.set_resource_operation_pending(false); s.set_resource_revision(8);
    EXPECT_FALSE(MemoryResizeNeverStarted(c, "runtime", s, 10200, 1000));
    s.set_resource_revision(7); s.set_resource_generation("recreated");
    EXPECT_FALSE(MemoryResizeNeverStarted(c, "runtime", s, 10200, 1000));
}
TEST(MemoryResizeObservationTest, TargetObservationRejectsUnknownAndStaleAba)
{
    ASSERT_TRUE(resource_view::MemoryElasticityEnabled());
    auto c = Command(); auto s = Sample(256 * MiB);
    EXPECT_TRUE(PressureResizeTargetObserved(c, "runtime", s, 10200, 1000));
    s.set_memory_limit_known(false); EXPECT_FALSE(PressureResizeTargetObserved(c, "runtime", s, 10200, 1000));
    s.set_memory_limit_known(true); s.set_resource_operation_id("different-operation");
    EXPECT_FALSE(PressureResizeTargetObserved(c, "runtime", s, 10200, 1000));
    s.set_resource_operation_id("operation"); s.set_resource_revision(10);
    EXPECT_FALSE(PressureResizeTargetObserved(c, "runtime", s, 10200, 1000));
    s.set_resource_revision(8); s.set_frozen(true);
    EXPECT_FALSE(PressureResizeTargetObserved(c, "runtime", s, 10200, 1000));
    s.set_frozen(false); EXPECT_FALSE(PressureResizeTargetObserved(c, "runtime", s, 12000, 1000));
}
}
