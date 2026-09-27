#include <gtest/gtest.h>
#include "common/resource_view/restore_admission.h"
#include <chrono>
#include <string>

namespace functionsystem::test {
using namespace resource_view;

namespace {
struct AdmissionSettingsGuard {
    AdmissionSettingsGuard() { SetUsageAdmissionSettings(0.95, 2048.0, 0); }
    ~AdmissionSettingsGuard() { SetUsageAdmissionSettings(0.95, 2048.0, 0); }
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

// Memory capacity would fail AND CPU sample is stale — the reason must be
// CapacityExceeded (memory branch fires first), NOT CPUSampleStale. This
// is the counterexample for the old post-hoc guessing that would have
// labeled it as observation-stale.
class AdmissionRejectReasonTest : public ::testing::Test {
protected:
    AdmissionSettingsGuard guard_;
};

TEST_F(AdmissionRejectReasonTest, MemCapacityFailAndCPUStale_YieldsCapacityExceeded)
{
    auto now = NowMs();
    // memory: 100 used + 500 reserve > 1000 * 0.9 safety = 900 → capacity
    auto unit = MakeUnit(1000, 100, 8000, 4000, now, now - 60000); // CPU stale
    AdmissionReserve extra{0, 1000}; // 100 + 1000 = 1100 > 900 → truly over capacity
    auto result = CheckResourceHeadroomWithReason(unit, extra);
    ASSERT_FALSE(result.status.IsOk());
    EXPECT_EQ(result.reason, AdmissionRejectReason::CapacityExceeded);
    EXPECT_NE(result.status.Code(), 0); // original Status/code unchanged
}

// Memory sample is fresh, CPU sample is stale → CPUSampleStale
protected:
    AdmissionSettingsGuard guard_;
};

TEST_F(AdmissionRejectReasonTest, MemFreshCPUStale_YieldsCPUSampleStale)
{
    auto now = NowMs();
    auto unit = MakeUnit(1000, 100, 8000, 4000, now, now - 60000);
    AdmissionReserve extra{0, 100}; // small enough to pass memory
    auto result = CheckResourceHeadroomWithReason(unit, extra);
    ASSERT_FALSE(result.status.IsOk());
    EXPECT_EQ(result.reason, AdmissionRejectReason::CPUSampleStale);
}

// Memory sample is stale → MemorySampleStale (observation guard)
protected:
    AdmissionSettingsGuard guard_;
};

TEST_F(AdmissionRejectReasonTest, MemStale_YieldsMemorySampleStale)
{
    auto now = NowMs();
    auto unit = MakeUnit(1000, 100, 8000, 4000, now - 60000, now);
    AdmissionReserve extra{0, 100};
    auto result = CheckResourceHeadroomWithReason(unit, extra);
    ASSERT_FALSE(result.status.IsOk());
    EXPECT_EQ(result.reason, AdmissionRejectReason::MemorySampleStale);
}

// Everything fresh and within capacity → None (not rejected)
protected:
    AdmissionSettingsGuard guard_;
};

TEST_F(AdmissionRejectReasonTest, FreshAndWithinCapacity_YieldsNone)
{
    auto now = NowMs();
    auto unit = MakeUnit(1000, 100, 8000, 4000, now, now);
    AdmissionReserve extra{0, 100};
    auto result = CheckResourceHeadroomWithReason(unit, extra);
    EXPECT_TRUE(result.status.IsOk());
    EXPECT_EQ(result.reason, AdmissionRejectReason::None);
}

// Status/code contract: the Status is the SAME as before the change —
// the documented StatusCode and message text, asserted against known
// constants rather than against the same implementation (which would
// prove nothing).
protected:
    AdmissionSettingsGuard guard_;
};

TEST_F(AdmissionRejectReasonTest, StatusCodeUnchanged)
{
    auto now = NowMs();
    auto unit = MakeUnit(1000, 100, 8000, 4000, now - 60000, now);
    AdmissionReserve extra{0, 100};
    auto result = CheckResourceHeadroomWithReason(unit, extra);
    ASSERT_FALSE(result.status.IsOk());
    // memory-stale: the documented contract is RESOURCE_NOT_ENOUGH with
    // the "Memory: Atomic Admission Rejected" message
    EXPECT_EQ(result.status.Code(), StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_NE(result.status.ToString().find("Atomic Admission Rejected"),
              std::string::npos);
}
} // namespace functionsystem::test