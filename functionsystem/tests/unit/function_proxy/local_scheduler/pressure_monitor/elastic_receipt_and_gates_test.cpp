// Regressions for the pc-elastic-4 root causes and the fast/slow cadence
// isolation (reviews 1631Z/1639Z + follow-up). Compiles with
// -fno-access-control like the sibling tests.
#include "local_scheduler/pressure_monitor/pressure_monitor_actor.h"
#include "local_scheduler/pressure_monitor/pressure_resize_policy.h"
#include "common/proto/pb/message_pb.h"
#include "common/resource_view/memory_elasticity_config.h"
#include <cmath>
#include <memory>
#include <string>
#include <gtest/gtest.h>

namespace functionsystem::test {
using namespace local_scheduler;
namespace {

constexpr uint64_t MiB = 1048576;

// The receipt validator must carry ONE high-watermark contract:
// ExpectedHighBytes (default ratio 0.9). The old local 0.8 copy in
// OnPressureResize misrecorded every CORRECT 0.9 receipt as unknown.
runtime::v1::ResizeResourcesRequest ReceiptCommand()
{
    runtime::v1::ResizeResourcesRequest command;
    command.mutable_target()->set_memory_limit_bytes(512 * MiB);
    command.mutable_target()->set_cpu_quota_usec(50000);
    command.mutable_target()->set_cpu_period_usec(100000);
    command.set_sandbox_id("sbox-r");
    command.set_resource_generation("gen-r");
    command.set_operation_id("op-r");
    command.set_expected_revision(2);
    command.set_deadline_unix_ms(1000);
    return command;
}

resources::SandboxResourceSample ReceiptSample(
    const runtime::v1::ResizeResourcesRequest &command, uint64_t highBytes)
{
    resources::SandboxResourceSample sample;
    sample.set_runtime_id("rt");
    sample.set_sandbox_id(command.sandbox_id());
    sample.set_resource_generation(command.resource_generation());
    sample.set_resource_revision(command.expected_revision() + 1);
    sample.set_resource_revision_known(true);
    sample.set_resource_operation_id(command.operation_id());
    sample.set_frozen_known(true);
    sample.set_memory_high_known(true);
    sample.set_memory_high_bytes(highBytes);
    sample.set_memory_limit_known(true);
    sample.set_memory_limit_bytes(command.target().memory_limit_bytes());
    sample.set_cpu_limits_known(true);
    sample.set_cpu_quota_usec(command.target().cpu_quota_usec());
    sample.set_cpu_period_usec(command.target().cpu_period_usec());
    sample.set_sampled_at_unix_ms(command.deadline_unix_ms() + 1);
    return sample;
}

TEST(ElasticReceiptTest, CorrectNineTenthsHighObservedWrongRejected)
{
    const auto command = ReceiptCommand();
    const auto receiptHigh = ElasticResizeReceiptHighBytes(command);
    EXPECT_EQ(receiptHigh, resource_view::ExpectedHighBytes(512 * MiB));
    EXPECT_GT(receiptHigh, 0.85 * 512.0 * MiB);  // the 0.9 side, not 0.8
    EXPECT_LT(receiptHigh, 512 * MiB);           // capped below max
    const auto nowMs = command.deadline_unix_ms() + 2;
    EXPECT_TRUE(PressureResizeTargetObserved(command, "rt",
        ReceiptSample(command, receiptHigh), nowMs, 60000));
    const auto stale = static_cast<uint64_t>(std::floor(0.8 * 512.0 * MiB / 4096.0)) * 4096;
    EXPECT_NE(stale, receiptHigh);
    EXPECT_FALSE(PressureResizeTargetObserved(command, "rt",
        ReceiptSample(command, stale), nowMs, 60000));
}

// The ACTUAL OnPressureResize path: a correct 0.9 receipt clears the pending
// entry; a wrong high leaves it unknown for reconciliation.
TEST(ElasticReceiptTest, OnPressureResizeAcceptsCorrectHighAndKeepsWrongUnknown)
{
    PressureMonitorActor monitor("receipt-test", "node-r", PressureMonitorConfig{});
    const std::string instance = "inst-r";
    auto &pending = monitor.pressureResizes_[instance];
    pending.requestID = "req-r";
    auto *resize = pending.request.mutable_resize();
    *resize = ReceiptCommand();

    messages::ResizeSandboxResponse ok;
    ok.set_requestid("req-r");
    ok.set_instanceid(instance);
    ok.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
    auto *result = ok.mutable_result();
    result->set_complete(true);
    auto *actual = result->mutable_actual();
    actual->set_resource_revision_known(true);
    actual->set_resource_revision(ReceiptCommand().expected_revision() + 1);
    actual->set_resource_operation_id("op-r");
    actual->set_frozen_known(true);
    actual->set_memory_high_known(true);
    actual->set_memory_high_bytes(ElasticResizeReceiptHighBytes(ReceiptCommand()));
    actual->set_memory_limit_known(true);
    actual->set_memory_limit_bytes(512 * MiB);
    actual->set_resource_generation("gen-r");
    actual->set_cpu_limits_known(true);
    actual->set_cpu_quota_usec(50000);
    actual->set_cpu_period_usec(100000);
    litebus::Promise<messages::ResizeSandboxResponse> okPromise;
    okPromise.SetValue(ok);
    monitor.OnPressureResize(instance, "req-r", okPromise.GetFuture());
    EXPECT_EQ(0u, monitor.pressureResizes_.count(instance))
        << "a correct 0.9 receipt must REMOVE the pending entry (count, not operator[])";

    // wrong high on a fresh pending: the entry must be marked unknown so
    // reconciliation keeps the reservation instead of trusting a bad receipt
    auto &pending2 = monitor.pressureResizes_[instance];
    pending2.requestID = "req-r";
    *pending2.request.mutable_resize() = ReceiptCommand();
    messages::ResizeSandboxResponse wrong = ok;
    wrong.mutable_result()->mutable_actual()->set_memory_high_bytes(
        static_cast<uint64_t>(std::floor(0.8 * 512.0 * MiB / 4096.0)) * 4096);
    litebus::Promise<messages::ResizeSandboxResponse> wrongPromise;
    wrongPromise.SetValue(wrong);
    monitor.OnPressureResize(instance, "req-r", wrongPromise.GetFuture());
    const auto stillPending = monitor.pressureResizes_.find(instance);
    ASSERT_NE(monitor.pressureResizes_.end(), stillPending);
    EXPECT_TRUE(stillPending->second.unknown)
        << "a wrong (0.8-style) high must leave the attempt unknown";
}

// Fast/slow isolation: separate single-flight gates.
TEST(ElasticCadenceTest, FastGateIndependentOfSlowGateInFlight)
{
    PressureMonitorActor monitor("gate-test", "node-g", PressureMonitorConfig{});
    auto slow = monitor.sampleGate_.Begin();
    ASSERT_TRUE(slow.has_value());
    auto fast = monitor.elasticGate_.Begin();
    ASSERT_TRUE(fast.has_value());            // not blocked by the slow pass
    auto fast2 = monitor.elasticGate_.Begin();
    ASSERT_FALSE(fast2.has_value());          // coalesced by ITS OWN gate
    EXPECT_TRUE(monitor.elasticGate_.Finish(*fast));
    EXPECT_TRUE(monitor.sampleGate_.Active(*slow));
    EXPECT_TRUE(monitor.sampleGate_.Finish(*slow));
}

// A fast timeout retires only the fast gate — the slow loop's park sustain
// counter and in-flight generation are untouched.
TEST(ElasticCadenceTest, FastTimeoutDoesNotTouchSlowParkCounters)
{
    PressureMonitorActor monitor("timeout-test", "node-t", PressureMonitorConfig{});
    auto slow = monitor.sampleGate_.Begin();
    ASSERT_TRUE(slow.has_value());
    monitor.highSamples_ = 1;
    auto fast = monitor.elasticGate_.Begin();
    ASSERT_TRUE(fast.has_value());
    monitor.OnElasticSampleTimeout(*fast);
    EXPECT_FALSE(monitor.elasticGate_.Active(*fast));
    EXPECT_EQ(1u, monitor.highSamples_);
    EXPECT_TRUE(monitor.sampleGate_.Active(*slow));
}


// Real-callback isolation: OnElasticResourceView Finishes only ITS gate's
// generation; a late view for a superseded fast generation cannot retire a
// NEW fast pass, and neither touches the slow gate or its counters.
TEST(ElasticCadenceTest, LateFastCallbackDoesNotRetireNewFastGeneration)
{
    PressureMonitorActor monitor("late-test", "node-l", PressureMonitorConfig{});
    auto slow = monitor.sampleGate_.Begin();
    ASSERT_TRUE(slow.has_value());
    monitor.highSamples_ = 1;

    auto old1 = monitor.elasticGate_.Begin();
    ASSERT_TRUE(old1.has_value());
    // old generation times out; tag dies with it
    monitor.OnElasticSampleTimeout(*old1);
    EXPECT_FALSE(monitor.elasticGate_.Active(*old1));

    auto fresh = monitor.elasticGate_.Begin();
    ASSERT_TRUE(fresh.has_value());
    // LATE callback for the timed-out generation: Finish fails inside
    // OnElasticResourceView, the fresh fast generation must stay active
    monitor.OnElasticResourceView(*old1, nullptr);
    EXPECT_TRUE(monitor.elasticGate_.Active(*fresh));
    // slow counters still untouched by everything above
    EXPECT_EQ(1u, monitor.highSamples_);
    EXPECT_TRUE(monitor.sampleGate_.Active(*slow));
    EXPECT_TRUE(monitor.sampleGate_.Finish(*slow));
}

// The real fast completion path: a view for the ACTIVE fast generation
// retires exactly that generation and leaves the slow pass in flight.
TEST(ElasticCadenceTest, ActiveFastViewRetiresOnlyItsGeneration)
{
    PressureMonitorActor monitor("view-test", "node-v", PressureMonitorConfig{});
    auto slow = monitor.sampleGate_.Begin();
    ASSERT_TRUE(slow.has_value());
    auto fast = monitor.elasticGate_.Begin();
    ASSERT_TRUE(fast.has_value());
    monitor.OnElasticResourceView(*fast, nullptr);  // nullptr view: no policy run
    EXPECT_FALSE(monitor.elasticGate_.Active(*fast));
    EXPECT_TRUE(monitor.sampleGate_.Active(*slow));
    EXPECT_EQ(0u, monitor.highSamples_);
}

}  // namespace
}  // namespace functionsystem::test
