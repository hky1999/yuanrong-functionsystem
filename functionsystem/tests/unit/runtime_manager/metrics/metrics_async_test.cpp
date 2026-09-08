#include "runtime_manager/metrics/metrics_actor.h"
#include <async/async.hpp>
#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <iostream>
using namespace functionsystem::runtime_manager;
namespace functionsystem::test {
using SteadyClock = std::chrono::steady_clock;
class PendingCollector : public BaseMetricsCollector {
public:
    PendingCollector() : BaseMetricsCollector(metrics_type::MEMORY, collector_type::INSTANCE) {}
    mutable litebus::Promise<Metric> result;
    litebus::Future<Metric> GetUsage() const override { return result.GetFuture(); }
    Metric GetLimit() const override { Metric m; m.value=128; m.instanceID=std::string("removed"); return m; }
    std::string GenFilter() const override { return "pending-memory"; }
};
class AsyncMetricsProbe : public MetricsActor {
public:
    explicit AsyncMetricsProbe(const std::string &name) : MetricsActor(name) {}
    double Ping() { return 1.0; }
    int publications=0;
    void Install(const std::shared_ptr<PendingCollector> &collector) {
        filter_.clear(); metricsFilter_.clear();
        filter_[collector->GenFilter()] = collector;
        metricsFilter_[collector->GenFilter()] = collector;
    }
    void Remove() { filter_.clear(); metricsFilter_.clear(); }
protected:
    std::string BuildUpdateMetricsRequest(const std::vector<litebus::Future<Metrics>> &metrics) override {
        ++publications;
        return MetricsActor::BuildUpdateMetricsRequest(metrics);
    }
};
static double PingDelay(const std::shared_ptr<AsyncMetricsProbe> &actor) {
    const auto start=SteadyClock::now();
    litebus::Async(actor->GetAID(), &AsyncMetricsProbe::Ping).Wait();
    return std::chrono::duration<double>(SteadyClock::now()-start).count();
}
TEST(MetricsAsyncRegression, RealProductionCollectorsDoNotBlockActor) {
    auto actor=std::make_shared<AsyncMetricsProbe>("m0-real-collectors");
    litebus::Spawn(actor);
    Flags flags;
    litebus::Async(actor->GetAID(), &MetricsActor::SetConfig, flags);
    litebus::Async(actor->GetAID(), &MetricsActor::BeginUpdateMetrics);
    const double delay=PingDelay(actor);
    std::cout << "M0_REAL_COLLECTOR_PING_SECONDS=" << delay << std::endl;
    EXPECT_LT(delay,0.1);
    litebus::Async(actor->GetAID(), &MetricsActor::StopUpdateMetrics);
    PingDelay(actor);
    litebus::Terminate(actor->GetAID()); litebus::Await(actor->GetAID());
}
TEST(MetricsAsyncRegression, UnresolvedCollectorDoesNotBlockAndDeletionRejectsLateValues) {
    auto actor=std::make_shared<AsyncMetricsProbe>("m0-pending-collector");
    auto collector=std::make_shared<PendingCollector>();
    litebus::Spawn(actor);
    litebus::Async(actor->GetAID(), &AsyncMetricsProbe::Install, collector);
    litebus::Async(actor->GetAID(), &MetricsActor::BeginUpdateMetrics);
    auto result=litebus::Async(actor->GetAID(), &MetricsActor::GetResourceUnit);
    EXPECT_LT(PingDelay(actor),0.1);
    EXPECT_TRUE(result.IsInit());
    litebus::Async(actor->GetAID(), &AsyncMetricsProbe::Remove);
    litebus::Async(actor->GetAID(), &MetricsActor::StopUpdateMetrics);
    PingDelay(actor);
    Metric m; m.value=100;
    collector->result.SetValue(m);
    result.Wait();
    EXPECT_TRUE(result.IsOK());
    if (result.IsOK()) { EXPECT_EQ(result.Get().instances_size(),0); }
    PingDelay(actor);
    EXPECT_EQ(actor->publications,0);
    litebus::Terminate(actor->GetAID()); litebus::Await(actor->GetAID());
}
TEST(MetricsAsyncRegression, FailedCollectorPropagatesFailureWithoutReadingValue) {
    auto actor=std::make_shared<AsyncMetricsProbe>("m0-failed-collector");
    auto collector=std::make_shared<PendingCollector>();
    litebus::Spawn(actor);
    litebus::Async(actor->GetAID(), &AsyncMetricsProbe::Install, collector);
    auto result=litebus::Async(actor->GetAID(), &MetricsActor::GetResourceUnit);
    EXPECT_LT(PingDelay(actor),0.1);
    collector->result.SetFailed(-1);
    result.Wait();
    EXPECT_TRUE(result.IsError());
    litebus::Terminate(actor->GetAID()); litebus::Await(actor->GetAID());
}
}

namespace functionsystem::test {
TEST(MetricsAsyncRegression, RemovedCollectorCannotResurrectInstanceWithoutStoppingActor) {
    auto actor=std::make_shared<AsyncMetricsProbe>("m0-removed-instance");
    auto collector=std::make_shared<PendingCollector>();
    litebus::Spawn(actor);
    litebus::Async(actor->GetAID(), &AsyncMetricsProbe::Install, collector);
    auto result=litebus::Async(actor->GetAID(), &MetricsActor::GetResourceUnit);
    PingDelay(actor);
    litebus::Async(actor->GetAID(), &AsyncMetricsProbe::Remove);
    PingDelay(actor);
    Metric m; m.value=100;
    collector->result.SetValue(m);
    result.Wait();
    EXPECT_TRUE(result.IsOK());
    if (result.IsOK()) { EXPECT_EQ(result.Get().instances_size(),0); }
    litebus::Terminate(actor->GetAID()); litebus::Await(actor->GetAID());
}
}
