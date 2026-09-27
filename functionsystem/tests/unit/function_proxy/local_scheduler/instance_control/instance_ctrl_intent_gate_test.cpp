/*
 * Dedicated ACTUALLY-BUILT target for the reusable-snapshot intent gate
 * (pc-snapshot-8b97-2 root cause). Real-entry tests drive the FULL
 * DoAuthorizeCreate chain (authorize -> affinity -> intent gate ->
 * DoCreateInstance); only the resolve seam (LocalSchedSrv) is mocked and the
 * duplicate boundary is intercepted by DERIVING from the REAL
 * InstanceControlView (IsDuplicateRequest is virtual,
 * common/state_machine/instance_control_view.h:59) — the stock mock does not
 * mock it. Helper-level tests are supplements.
 *
 * Waiting uses ASSERT_AWAIT_READY_FOR (litebus Future::WaitFor takes
 * uint64 ms and returns WaitForStatus — never chrono/bool). The accept-path
 * sentinel is an INDEPENDENT pre-completed future (never the caller's own
 * future: the real IsDuplicateRequest only associates it, so
 * self-association would wait forever).
 */
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "async/async.hpp"
#include "common/proto/pb/message_pb.h"
#include "common/resource_view/resource_type.h"
#include "function_proxy/local_scheduler/instance_control/instance_ctrl_actor.h"
#include "mocks/mock_local_sched_srv.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {
namespace {

using namespace functionsystem::local_scheduler;  // NOLINT

static constexpr const char *REQUESTED_ID_EXT = "yr.internal.reusable_snapshot.requested_id";
static constexpr const char *TRUSTED_RESTORE_EXT = "yr.internal.reusable_snapshot.trusted_restore";

// REAL view with ONE virtual overridden: the duplicate boundary counts
// visits (zero visits == zero dispatch past the gate) and can short-circuit
// with an independent pre-completed sentinel future.
class GateProbeView : public InstanceControlView {
public:
    using InstanceControlView::InstanceControlView;

    litebus::Option<litebus::Future<messages::ScheduleResponse>> IsDuplicateRequest(
        const std::shared_ptr<messages::ScheduleRequest> &req,
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> & /*runtimePromise*/) override
    {
        ++duplicateChecks;
        lastRequest = req;
        if (shortCircuit) {
            auto sentinelPromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
            sentinelPromise->SetValue(sentinel);
            return litebus::Option<litebus::Future<messages::ScheduleResponse>>>(
                litebus::Some(sentinelPromise->GetFuture()));
        }
        return litebus::Option<litebus::Future<messages::ScheduleResponse>>{};
    }

    int duplicateChecks{ 0 };
    bool shortCircuit{ false };
    messages::ScheduleResponse sentinel;
    std::shared_ptr<messages::ScheduleRequest> lastRequest;
};

class InstanceCtrlIntentGateTest : public testing::Test {
protected:
    void SetUp() override
    {
        InstanceCtrlConfig config;
        instanceCtrlActor_ = std::make_shared<InstanceCtrlActor>("INTENT_GATE_ACTOR", "node-test", config);
        instanceCtrlActor_->BindInstanceControlView(probeView_);
        instanceCtrlActor_->BindLocalSchedSrv(mockLocalSchedSrv_);
        litebus::Spawn(instanceCtrlActor_);
    }

    void TearDown() override
    {
        litebus::Terminate(instanceCtrlActor_->GetAID());
        litebus::Await(instanceCtrlActor_->GetAID());
        instanceCtrlActor_ = nullptr;
    }

    static std::shared_ptr<messages::ScheduleRequest> MakeRequest(const std::string &snapshotID,
                                                                  bool withInitRequest = false)
    {
        auto req = std::make_shared<messages::ScheduleRequest>();
        req->set_traceid("trace-gate");
        req->set_requestid("req-gate");
        auto *info = req->mutable_instance();
        info->set_instanceid("inst-gate");
        info->set_tenantid("tenant-gate");
        info->set_function("fn-gate");
        info->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::NEW));
        if (!snapshotID.empty()) {
            (*info->mutable_scheduleoption()->mutable_extension())[REQUESTED_ID_EXT] = snapshotID;
        }
        if (withInitRequest) {
            runtime_service::CallRequest stale;
            stale.set_function("fn-gate");
            (*stale.mutable_createoptions())["marker"] = "pre-inheritance-marker";
            req->set_initrequest(stale.SerializeAsString());
        }
        return req;
    }

    static litebus::Option<FunctionMeta> MakeMeta()
    {
        FunctionMeta meta;
        meta.funcMetaData.name = "fn-gate";
        meta.funcMetaData.tenantId = "tenant-gate";
        return litebus::Option<FunctionMeta>(litebus::Some(meta));
    }

    static ::messages::ResolveReusableSnapshotForCreateResponse MakeResolved(
        const std::string &snapshotID, double cpuMillicores, double memoryMb)
    {
        ::messages::ResolveReusableSnapshotForCreateResponse rsp;
        rsp.set_code(static_cast<int32_t>(common::ErrorCode::ERR_NONE));
        auto *tmpl = rsp.mutable_instancetemplate();
        tmpl->set_function("fn-gate");  // matches the request: template adoption
        if (cpuMillicores > 0) {
            (*tmpl->mutable_resources()->mutable_resources())[resource_view::CPU_RESOURCE_NAME]
                .mutable_scalar()->set_value(cpuMillicores);
        }
        if (memoryMb > 0) {
            (*tmpl->mutable_resources()->mutable_resources())[resource_view::MEMORY_RESOURCE_NAME]
                .mutable_scalar()->set_value(memoryMb);
        }
        auto *restore = rsp.mutable_reusablesnapshotrestore();
        restore->set_snapshotid(snapshotID);
        restore->set_allowlogicalinstanceidrebind(true);
        auto *artifact = restore->mutable_artifact();
        artifact->set_storagebackend("local");
        artifact->set_sourcenodeid("node-src");
        artifact->set_objectkey("obj-gate");
        artifact->set_format("sandboxd-checkpoint");
        artifact->set_formatversion(1);
        return rsp;
    }

    std::shared_ptr<GateProbeView> probeView_ = std::make_shared<GateProbeView>("node-test", false);
    std::shared_ptr<MockLocalSchedSrv> mockLocalSchedSrv_ = std::make_shared<MockLocalSchedSrv>();
    std::shared_ptr<InstanceCtrlActor> instanceCtrlActor_;
};

// REAL ENTRY, reject: a snapshot-bearing create whose resolve reports a
// business error is refused by the FULL chain — the caller's promise
// completes with the explicit rejection and NOTHING reaches the create
// boundary (zero duplicate-check visits == zero dispatch).
TEST_F(InstanceCtrlIntentGateTest, RealEntryResolveFailureRefusesCreateZeroDispatch)
{
    auto scheduleReq = MakeRequest("snap-entry-rej");
    EXPECT_CALL(*mockLocalSchedSrv_, ResolveReusableSnapshotForCreate)
        .WillOnce(Invoke([](const std::shared_ptr<::messages::ResolveReusableSnapshotForCreateRequest> &req) {
            EXPECT_EQ(req->snapshotid(), "snap-entry-rej");
            ::messages::ResolveReusableSnapshotForCreateResponse rsp;
            rsp.set_code(static_cast<int32_t>(common::ErrorCode::ERR_INNER_SYSTEM_ERROR));
            rsp.set_message("store unavailable (synthetic)");
            auto p = std::make_shared<litebus::Promise<::messages::ResolveReusableSnapshotForCreateResponse>>();
            p->SetValue(rsp);
            return p->GetFuture();
        }));
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto future = runtimePromise->GetFuture();
    litebus::Async(instanceCtrlActor_->GetAID(), &InstanceCtrlActor::DoAuthorizeCreate, MakeMeta(), scheduleReq,
                   runtimePromise);
    ASSERT_AWAIT_READY_FOR(future, 10000);
    const auto response = future.Get();
    EXPECT_NE(response.code(), static_cast<int32_t>(common::ErrorCode::ERR_NONE));
    EXPECT_NE(response.message().find("reusable snapshot"), std::string::npos);
    EXPECT_EQ(probeView_->duplicateChecks, 0) << "a refused create must never reach the create boundary";
    EXPECT_EQ(scheduleReq->instance().scheduleoption().extension().count(TRUSTED_RESTORE_EXT), 0U);
}

// REAL ENTRY, accept: the chain passes the gate, applies the trusted
// context, syncs initrequest with the inherited values, and reaches
// DoCreateInstance — observed ONLY through the independent sentinel
// short-circuit of the derived real view.
TEST_F(InstanceCtrlIntentGateTest, RealEntryResolvedContextFlowsIntoCreateAndSyncsInitRequest)
{
    probeView_->shortCircuit = true;
    probeView_->sentinel.set_code(static_cast<int32_t>(common::ErrorCode::ERR_NONE));
    probeView_->sentinel.set_message("sentinel: reached create boundary");
    auto scheduleReq = MakeRequest("snap-entry-ok", /*withInitRequest=*/true);
    EXPECT_CALL(*mockLocalSchedSrv_, ResolveReusableSnapshotForCreate)
        .WillOnce(Invoke([this](const std::shared_ptr<::messages::ResolveReusableSnapshotForCreateRequest> &req) {
            auto p = std::make_shared<litebus::Promise<::messages::ResolveReusableSnapshotForCreateResponse>>();
            p->SetValue(MakeResolved(req->snapshotid(), 500.0, 512.0));
            return p->GetFuture();
        }));
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto future = runtimePromise->GetFuture();
    litebus::Async(instanceCtrlActor_->GetAID(), &InstanceCtrlActor::DoAuthorizeCreate, MakeMeta(), scheduleReq,
                   runtimePromise);
    ASSERT_AWAIT_READY_FOR(future, 10000);
    EXPECT_EQ(future.Get().message(), probeView_->sentinel.message())
        << "only the duplicate short-circuit can resolve this future";
    ASSERT_GE(probeView_->duplicateChecks, 1);
    const auto &applied = scheduleReq->instance();
    EXPECT_EQ(applied.scheduleoption().extension().count(REQUESTED_ID_EXT), 0U);
    EXPECT_NE(applied.scheduleoption().extension().count(TRUSTED_RESTORE_EXT), 0U);
    EXPECT_DOUBLE_EQ(applied.resources().resources().at(resource_view::MEMORY_RESOURCE_NAME).scalar().value(),
                     512.0);
    runtime_service::CallRequest synced;
    ASSERT_TRUE(synced.ParseFromString(scheduleReq->initrequest()));
    EXPECT_EQ(synced.function(), "fn-gate");
}

// HELPER, transport failure: a SetFailed resolve future becomes an explicit
// rejection with the caller's promise completed (never a silent skip).
TEST_F(InstanceCtrlIntentGateTest, HelperResolveFutureSetFailedRejects)
{
    auto scheduleReq = MakeRequest("snap-setfailed");
    EXPECT_CALL(*mockLocalSchedSrv_, ResolveReusableSnapshotForCreate)
        .WillOnce(Invoke([](const std::shared_ptr<::messages::ResolveReusableSnapshotForCreateRequest> &) {
            auto p = std::make_shared<litebus::Promise<::messages::ResolveReusableSnapshotForCreateResponse>>();
            p->SetFailed(static_cast<int32_t>(common::ErrorCode::GRPC_UNAVAILABLE));
            return p->GetFuture();
        }));
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto future = runtimePromise->GetFuture();
    litebus::Async(instanceCtrlActor_->GetAID(), &InstanceCtrlActor::ResolveReusableSnapshotIntent, Status::OK(),
                   scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY_FOR(future, 10000);
    EXPECT_NE(future.Get().code(), static_cast<int32_t>(common::ErrorCode::ERR_NONE));
    EXPECT_NE(future.Get().message().find("resolve future failed"), std::string::npos);
    EXPECT_EQ(probeView_->duplicateChecks, 0);
}

// HELPER, watchdog: an unresolved resolve future trips the real 30s
// watchdog — the create is refused with the timeout message, and a LATE
// success after the watchdog settles dispatches NOTHING and injects NO
// context.
TEST_F(InstanceCtrlIntentGateTest, HelperHungResolveTimesOutAndLateSuccessDoesNotDispatch)
{
    auto scheduleReq = MakeRequest("snap-hang");
    auto lateResolve = std::make_shared<litebus::Promise<::messages::ResolveReusableSnapshotForCreateResponse>>();
    EXPECT_CALL(*mockLocalSchedSrv_, ResolveReusableSnapshotForCreate)
        .WillOnce(Invoke([&](const std::shared_ptr<::messages::ResolveReusableSnapshotForCreateRequest> &) {
            return lateResolve->GetFuture();
        }));
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto future = runtimePromise->GetFuture();
    litebus::Async(instanceCtrlActor_->GetAID(), &InstanceCtrlActor::ResolveReusableSnapshotIntent, Status::OK(),
                   scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY_FOR(future, 45000);  // real 30s watchdog + margin
    EXPECT_NE(future.Get().message().find("timed out"), std::string::npos);
    lateResolve->SetValue(MakeResolved("snap-hang", 500.0, 512.0));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(probeView_->duplicateChecks, 0);
    EXPECT_EQ(scheduleReq->instance().scheduleoption().extension().count(TRUSTED_RESTORE_EXT), 0U)
        << "a late resolve after the watchdog must never inject context";
}

// HELPER, resources + elastic re-normalization: a template with legal
// CPU/Memory passes the post-inheritance normalization, absent keys are
// inherited, and an EXPLICIT override (explicit 0 included) is never
// silently replaced.
TEST_F(InstanceCtrlIntentGateTest, HelperLegalResourcesAndExplicitOverrideSurvive)
{
    auto scheduleReq = MakeRequest("snap-elastic");
    (*scheduleReq->mutable_instance()->mutable_resources()->mutable_resources())
        [resource_view::CPU_RESOURCE_NAME].mutable_scalar()->set_value(0.0);
    EXPECT_CALL(*mockLocalSchedSrv_, ResolveReusableSnapshotForCreate)
        .WillOnce(Invoke([this](const std::shared_ptr<::messages::ResolveReusableSnapshotForCreateRequest> &req) {
            auto p = std::make_shared<litebus::Promise<::messages::ResolveReusableSnapshotForCreateResponse>>();
            p->SetValue(MakeResolved(req->snapshotid(), 500.0, 512.0));
            return p->GetFuture();
        }));
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto chain = litebus::Async(instanceCtrlActor_->GetAID(), &InstanceCtrlActor::ResolveReusableSnapshotIntent,
                                Status::OK(), scheduleReq, runtimePromise);
    ASSERT_AWAIT_READY_FOR(chain, 10000);
    EXPECT_TRUE(chain.Get().IsOk()) << chain.Get().RawMessage();
    const auto &applied = scheduleReq->instance().resources().resources();
    EXPECT_DOUBLE_EQ(applied.at(resource_view::MEMORY_RESOURCE_NAME).scalar().value(), 512.0);
    EXPECT_DOUBLE_EQ(applied.at(resource_view::CPU_RESOURCE_NAME).scalar().value(), 0.0)
        << "explicit zero is an override, never replaced by inheritance";
    EXPECT_EQ(probeView_->duplicateChecks, 0);
}

}  // namespace
}  // namespace functionsystem::test
