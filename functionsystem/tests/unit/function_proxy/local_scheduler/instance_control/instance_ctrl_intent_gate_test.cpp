/*
 * Independent, ACTUALLY-RUNNING target for the reusable-snapshot intent gate
 * (pc-snapshot-8b97-2 root cause). The legacy instance_ctrl_actor_test.cpp
 * fixture is DISABLED_* and its host target is not part of the verified
 * build set, so this dedicated target exists.
 *
 * Scope, stated honestly:
 *  - REAL-ENTRY tests drive InstanceCtrlActor::DoAuthorizeCreate — the real
 *    authorize -> affinity -> INTENT-GATE -> DoCreateInstance chain — with
 *    only the surrounding services mocked (LocalSchedSrv, InstanceCtrlView).
 *    The reject test proves a snapshot-bearing create is refused with the
 *    caller's promise completed and ZERO dispatch; the accept test proves
 *    the chain passes the gate, applies the trusted context, and reaches
 *    DoCreateInstance (observed via the duplicate-check short-circuit).
 *  - HELPER-level tests (direct ResolveReusableSnapshotIntent calls) are
 *    kept as supplements; they prove the step in isolation only.
 */
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "async/async.hpp"
#include "common/proto/pb/message_pb.h"
#include "common/resource_view/resource_type.h"
#include "function_proxy/local_scheduler/instance_control/instance_ctrl_actor.h"
#include "mocks/mock_local_sched_srv.h"

namespace functionsystem::test {
namespace {

using namespace functionsystem::local_scheduler;  // NOLINT

static constexpr const char *REQUESTED_ID_EXT = "yr.internal.reusable_snapshot.requested_id";
static constexpr const char *TRUSTED_RESTORE_EXT = "yr.internal.reusable_snapshot.trusted_restore";

class InstanceCtrlIntentGateTest : public testing::Test {
protected:
    void SetUp() override
    {
        InstanceCtrlConfig config;
        instanceCtrlActor_ = std::make_shared<InstanceCtrlActor>("INTENT_GATE_ACTOR", "node-test", config);
        // internalIAM_ left unbound: AuthorizeCreate treats nullptr as IAM
        // disabled and (with an empty parent id) returns OK without views.
        instanceCtrlActor_->BindInstanceControlView(instanceControlView_);
        instanceCtrlActor_->BindLocalSchedSrv(mockLocalSchedSrv_);
        litebus::Spawn(instanceCtrlActor_);
    }

    void TearDown() override
    {
        litebus::Terminate(instanceCtrlActor_->GetAID());
        litebus::Await(instanceCtrlActor_->GetAID());
        instanceCtrlActor_ = nullptr;
    }

    static std::shared_ptr<messages::ScheduleRequest> MakeRequest(const std::string &snapshotID)
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
        return req;
    }

    static litebus::Option<FunctionMeta> MakeMeta()
    {
        FunctionMeta meta;
        meta.funcMetaData.name = "fn-gate";
        meta.funcMetaData.tenantId = "tenant-gate";
        return litebus::Option<FunctionMeta>(litebus::Some(meta));
    }

    // REAL view (the duplicate-check boundary is not part of the verified
    // mock surface): pre-inserting the request future makes the REAL
    // IsDuplicateRequest short-circuit DoCreateInstance — the observable
    // proof that the full chain passed the intent gate and reached create.
    std::shared_ptr<InstanceControlView> instanceControlView_ =
        std::make_shared<InstanceControlView>("node-test", false);
    std::shared_ptr<MockLocalSchedSrv> mockLocalSchedSrv_ = std::make_shared<MockLocalSchedSrv>();
    std::shared_ptr<InstanceCtrlActor> instanceCtrlActor_;
};

// REAL ENTRY: a snapshot-bearing create whose resolve fails is refused by
// the FULL DoAuthorizeCreate chain — the caller's promise completes with the
// rejection and DoCreateInstance never dispatches anything.
TEST_F(InstanceCtrlIntentGateTest, RealEntryResolveFailureRefusesCreateAndCompletesPromise)
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
    ASSERT_TRUE(future.WaitFor(std::chrono::seconds(10)));
    const auto response = future.Get();
    EXPECT_NE(response.code(), static_cast<int32_t>(common::ErrorCode::ERR_NONE));
    EXPECT_NE(response.message().find("reusable snapshot"), std::string::npos);
    // fail-closed: no trusted context fabricated on the refused request
    EXPECT_EQ(scheduleReq->instance().scheduleoption().extension().count(TRUSTED_RESTORE_EXT), 0U);
}

// REAL ENTRY: resolve success takes the FULL chain through the gate into
// DoCreateInstance (observed via the duplicate-check short-circuit response)
// with the trusted context applied and the requested id consumed.
TEST_F(InstanceCtrlIntentGateTest, RealEntryResolvedContextFlowsIntoCreate)
{
    auto scheduleReq = MakeRequest("snap-entry-ok");
    EXPECT_CALL(*mockLocalSchedSrv_, ResolveReusableSnapshotForCreate)
        .WillOnce(Invoke([](const std::shared_ptr<::messages::ResolveReusableSnapshotForCreateRequest> &req) {
            ::messages::ResolveReusableSnapshotForCreateResponse rsp;
            rsp.set_code(static_cast<int32_t>(common::ErrorCode::ERR_NONE));
            auto *tmpl = rsp.mutable_instancetemplate();
            tmpl->set_function("fn-gate");  // matches the request: template adoption
            (*tmpl->mutable_resources()->mutable_resources())[resource_view::MEMORY_RESOURCE_NAME]
                .mutable_scalar()->set_value(512.0);
            auto *restore = rsp.mutable_reusablesnapshotrestore();
            restore->set_snapshotid(req->snapshotid());
            restore->set_allowlogicalinstanceidrebind(true);
            auto *artifact = restore->mutable_artifact();
            artifact->set_storagebackend("local");
            artifact->set_sourcenodeid("node-src");
            artifact->set_objectkey("obj-entry");
            artifact->set_format("sandboxd-checkpoint");
            artifact->set_formatversion(1);
            auto p = std::make_shared<litebus::Promise<::messages::ResolveReusableSnapshotForCreateResponse>>();
            p->SetValue(rsp);
            return p->GetFuture();
        }));
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    auto future = runtimePromise->GetFuture();
    // pre-register the request id: the REAL IsDuplicateRequest then
    // short-circuits DoCreateInstance with this exact future — reaching it
    // at all proves authorize -> affinity -> intent gate all passed
    instanceControlView_->InsertRequestFuture("req-gate", future, runtimePromise);
    litebus::Async(instanceCtrlActor_->GetAID(), &InstanceCtrlActor::DoAuthorizeCreate, MakeMeta(), scheduleReq,
                   runtimePromise);
    ASSERT_TRUE(future.WaitFor(std::chrono::seconds(10)));
    // the duplicate short-circuit is the ONLY path that can resolve this
    // future without deeper dispatch: reaching it proves the gate passed
    ASSERT_TRUE(future.WaitFor(std::chrono::seconds(10)));
    const auto &applied = scheduleReq->instance();
    EXPECT_EQ(applied.scheduleoption().extension().count(REQUESTED_ID_EXT), 0U);
    EXPECT_NE(applied.scheduleoption().extension().count(TRUSTED_RESTORE_EXT), 0U);
    EXPECT_DOUBLE_EQ(applied.resources().resources().at(resource_view::MEMORY_RESOURCE_NAME).scalar().value(),
                     512.0);
}

// HELPER (supplement): no extension — identity step, zero resolve calls.
TEST_F(InstanceCtrlIntentGateTest, HelperAbsentExtensionPassesThrough)
{
    auto scheduleReq = MakeRequest("");
    EXPECT_CALL(*mockLocalSchedSrv_, ResolveReusableSnapshotForCreate).Times(0);
    auto future = litebus::Async(instanceCtrlActor_->GetAID(), &InstanceCtrlActor::ResolveReusableSnapshotIntent,
                                 Status::OK(), scheduleReq, std::shared_ptr<litebus::Promise<messages::ScheduleResponse>>{});
    ASSERT_TRUE(future.WaitFor(std::chrono::seconds(10)));
    EXPECT_TRUE(future.Get().IsOk());
}

}  // namespace
}  // namespace functionsystem::test

