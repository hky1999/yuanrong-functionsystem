#include "function_proxy/local_scheduler/pressure_monitor/memory_elasticity_policy.h"
#include <gtest/gtest.h>
namespace functionsystem::test {
using Policy = local_scheduler::MemoryElasticityPolicy;
constexpr uint64_t MB = 1024 * 1024;
static Policy::Sample IdleSample() {
    Policy::Sample s;
    s.generation="generation-1"; s.revision=1; s.sampledMs=1000;
    s.current=64*MB; s.maximum=512*MB; s.high=409*MB; s.floor=128*MB; s.ceiling=4096*MB;
    s.memoryKnown=s.eventsKnown=s.cpuKnown=s.revisionKnown=s.frozenKnown=true;
    return s;
}
TEST(MemoryElasticityPolicy, TwoStagesReturnToFloorAndSameInstanceCanGrowAgain) {
    Policy p; auto s=IdleSample();
    for (int i=0;i<25;++i,s.sampledMs+=200) { EXPECT_FALSE(p.Observe(s,s.sampledMs)); }
    auto down=p.Observe(s,s.sampledMs);
    ASSERT_TRUE(down); EXPECT_TRUE(down->shrink); EXPECT_EQ(down->target,256*MB);
    ++s.revision; s.maximum=256*MB; s.high=204*MB; s.sampledMs+=200;
    for (int i=0;i<25;++i,s.sampledMs+=200) { EXPECT_FALSE(p.Observe(s,s.sampledMs)); }
    down=p.Observe(s,s.sampledMs);
    ASSERT_TRUE(down); EXPECT_EQ(down->target,128*MB);
    ++s.revision; s.maximum=128*MB; s.high=102*MB; s.current=100*MB; s.sampledMs+=200;
    auto up=p.Observe(s,s.sampledMs);
    ASSERT_TRUE(up); EXPECT_FALSE(up->shrink); EXPECT_EQ(up->target,256*MB);
}
TEST(MemoryElasticityPolicy, HeldMemoryDoesNotShrinkEvenWithIdleCPU) {
    Policy p; auto s=IdleSample(); s.current=269*MB;
    for (int i=0;i<100;++i,s.sampledMs+=200) { EXPECT_FALSE(p.Observe(s,s.sampledMs)); }
}
TEST(MemoryElasticityPolicy, MissingSampleOrSpikeRestartsIdleWindow) {
    Policy p; auto s=IdleSample();
    for (int i=0;i<20;++i,s.sampledMs+=200) { EXPECT_FALSE(p.Observe(s,s.sampledMs)); }
    s.memoryKnown=false; EXPECT_FALSE(p.Observe(s,s.sampledMs));
    s.memoryKnown=true; s.sampledMs+=200;
    for (int i=0;i<20;++i,s.sampledMs+=200) { EXPECT_FALSE(p.Observe(s,s.sampledMs)); }
    s.current=269*MB; EXPECT_FALSE(p.Observe(s,s.sampledMs));
    s.current=64*MB; s.sampledMs+=200;
    for (int i=0;i<25;++i,s.sampledMs+=200) { EXPECT_FALSE(p.Observe(s,s.sampledMs)); }
    EXPECT_TRUE(p.Observe(s,s.sampledMs));
}
TEST(MemoryElasticityPolicy, NewRevisionDoesNotReuseOldHighCounterOrIdleWindow) {
    Policy p; auto s=IdleSample();
    for (int i=0;i<20;++i,s.sampledMs+=200) { EXPECT_FALSE(p.Observe(s,s.sampledMs)); }
    ++s.revision; s.highEvents=100000;
    for (int i=0;i<25;++i,s.sampledMs+=200) { EXPECT_FALSE(p.Observe(s,s.sampledMs)); }
    auto down=p.Observe(s,s.sampledMs); ASSERT_TRUE(down); EXPECT_TRUE(down->shrink);
}
TEST(MemoryElasticityPolicy, UnknownCPUHighUsageCanGrowButCannotShrink) {
    Policy p; auto s=IdleSample(); s.cpuKnown=false;
    for (int i=0;i<35;++i,s.sampledMs+=200) { EXPECT_FALSE(p.Observe(s,s.sampledMs)); }
    s.current=400*MB; auto up=p.Observe(s,s.sampledMs); ASSERT_TRUE(up); EXPECT_FALSE(up->shrink);
}
TEST(MemoryElasticityPolicy, FloorCeilingAndFrozenStateAreHardBounds) {
    Policy p; auto s=IdleSample(); s.maximum=s.floor; s.high=102*MB;
    for (int i=0;i<50;++i,s.sampledMs+=200) { EXPECT_FALSE(p.Observe(s,s.sampledMs)); }
    s.maximum=s.ceiling; s.high=3276*MB; s.current=3276*MB;
    EXPECT_FALSE(p.Observe(s,s.sampledMs));
    s.sampledMs+=200; s.maximum=512*MB; s.high=409*MB; s.current=400*MB; s.frozen=true;
    EXPECT_FALSE(p.Observe(s,s.sampledMs));
}
}

namespace functionsystem::test {
TEST(MemoryElasticityPolicy, OomKillAndImpossibleUsageCannotProduceRescueDecision) {
    Policy p; auto s=IdleSample();
    EXPECT_FALSE(p.Observe(s,s.sampledMs));
    s.sampledMs+=200; s.current=400*MB; s.oomKills=1;
    EXPECT_FALSE(p.Observe(s,s.sampledMs));
    s.sampledMs+=200; s.current=~uint64_t{0};
    EXPECT_FALSE(p.Observe(s,s.sampledMs));
}
}
