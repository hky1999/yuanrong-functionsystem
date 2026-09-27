#include <gtest/gtest.h>
#include "common/resource_view/restore_admission.h"
#include <chrono>
#include <string>

namespace functionsystem::test {
using namespace resource_view;

namespace {
// Saves the current usage-admission settings on construction and restores
// the ORIGINAL values on destruction (not a hardcoded default twice, which
// would clobber whatever a prior test or the production init set).
struct AdmissionSettingsGuard {
    UsageAdmissionSettings saved_;
    AdmissionSettingsGuard() : saved_(GetUsageAdmissionSettings())
    {
        SetUsageAdmissionSettings(0.95, 2048.0, 0);
    }
    ~AdmissionSettingsGuard() { SetUsageAdmissionSettings(saved_.safety, saved_.floorMb, saved_.maxInstances); }
};

static int unitCounter = 0;

ResourceUnit MakeUnit(double memCapMb, double memUsedMb, double cpuCapMc, double cpuUsedMc,
                      int64_t memSampleMs, int64_t cpuSampleMs)
{
    ResourceUnit u;
    u.set_id("admission-test-unit-" + std::to_string(++unitCounter));
    auto *memCap = (*u.mutable_capacity()->mutable_resources())["Memory"].mutable_scalar();
    memCap->set_value(memCapMb);
    auto *cpuCap = (*u.mutable_capacity()->mutable_resources())["CPU"].mutable_scalar();
    cpuCap->set_value(cpuCapMc);
    auto *memUse = (*u.mutable_actualuse()->mutable_resources())["Memory"].mutable_scalar();
    memUse->set_value(memUsedMb);
    auto *cpuUse = (*u.mutable_actualuse()->mutable_resources())["CPU"].mutable_scalar();
    cpuUse->set_value(cpuUsedMc);
    u.set_memory_sample_unix_ms(memSampleMs);
    u.set_cpu_sample_unix_ms(cpuSampleMs);
    return u;
}

int64_t NowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
} // namespace

// One fixture: the guard pins settings for every test in this suite.
class AdmissionRejectReasonTest : public ::testing::Test {
protected:
    AdmissionSettingsGuard guard_;
};

// Memory capacity would fail AND CPU sample is stale — the reason must be
// CapacityExceeded (memory branch fires first in CheckAdmission), NOT
// CPUSampleStale. This is the counterexample for the old post-hoc guessing
// that would have labeled it as observation-stale.
// 100 used + 1000 reserve = 1100 > 1000 * 0.95 = 950 → truly over capacity.
TEST_F(AdmissionRejectReasonTest, MemCapacityFailAndCPUStale_YieldsCapacityExceeded)
{
    auto now = NowMs();
    auto unit = MakeUnit(1000, 100, 8000, 4000, now, now - 60000); // CPU stale
    AdmissionReserve extra{0, 1000}; // 1100 > 950
    auto result = CheckResourceHeadroomWithReason(unit, extra);
    ASSERT_FALSE(result.status.IsOk());
    EXPECT_EQ(result.reason, AdmissionRejectReason::CapacityExceeded);
}

// Memory sample is fresh, CPU sample is stale → CPUSampleStale.
TEST_F(AdmissionRejectReasonTest, MemFreshCPUStale_YieldsCPUSampleStale)
{
    auto now = NowMs();
    auto unit = MakeUnit(1000, 100, 8000, 4000, now, now - 60000);
    AdmissionReserve extra{0, 100}; // 200 < 950, passes memory
    auto result = CheckResourceHeadroomWithReason(unit, extra);
    ASSERT_FALSE(result.status.IsOk());
    EXPECT_EQ(result.reason, AdmissionRejectReason::CPUSampleStale);
}

// Memory sample is stale → MemorySampleStale (the observation guard).
TEST_F(AdmissionRejectReasonTest, MemStale_YieldsMemorySampleStale)
{
    auto now = NowMs();
    auto unit = MakeUnit(1000, 100, 8000, 4000, now - 60000, now);
    AdmissionReserve extra{0, 100};
    auto result = CheckResourceHeadroomWithReason(unit, extra);
    ASSERT_FALSE(result.status.IsOk());
    EXPECT_EQ(result.reason, AdmissionRejectReason::MemorySampleStale);
}

// Everything fresh and within capacity → None (not rejected).
TEST_F(AdmissionRejectReasonTest, FreshAndWithinCapacity_YieldsNone)
{
    auto now = NowMs();
    auto unit = MakeUnit(1000, 100, 8000, 4000, now, now);
    AdmissionReserve extra{0, 100};
    auto result = CheckResourceHeadroomWithReason(unit, extra);
    EXPECT_TRUE(result.status.IsOk());
    EXPECT_EQ(result.reason, AdmissionRejectReason::None);
}

// Status/code contract: asserted against the KNOWN documented StatusCode
// and message text — NOT against the same implementation (which would
// prove nothing).
TEST_F(AdmissionRejectReasonTest, StatusCodeUnchanged)
{
    auto now = NowMs();
    auto unit = MakeUnit(1000, 100, 8000, 4000, now - 60000, now);
    AdmissionReserve extra{0, 100};
    auto result = CheckResourceHeadroomWithReason(unit, extra);
    ASSERT_FALSE(result.status.IsOk());
    EXPECT_EQ(result.status.StatusCode(), StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_NE(result.status.ToString().find("Atomic Admission Rejected"),
              std::string::npos);
}

} // namespace functionsystem::test
