/*
 * Service-boundary dispatch tests for managed restore: the REAL spawned
 * SandboxdExecutor drives its REAL DoRestore -> OnRestoreDone chain against
 * an in-process fake runtime.v1.SandboxService gRPC server on a unix socket.
 *
 * Contract under test:
 *  - a request whose runtimeinstanceinfo.restoresnapshotid is set reaches
 *    sandboxd Restore with checkpoint_dir == <base>/managed/<id> and the
 *    identity quartet (checkpoint_id/sha/size/sandbox_id) filled ALL or
 *    NOTHING — a reusable managed request carries no snapshotinfo, so all
 *    four stay empty (sandboxd legacy dir-verified branch), never a
 *    half-filled strict request (rejected 80004 in production);
 *  - a missing managed artifact fails EXPLICITLY — zero Start AND zero
 *    Restore dispatches (never a silent fresh instance);
 *  - traversal-shaped ids are rejected before any RPC;
 *  - success registers the NEW sandbox id + generation in the state
 *    manager and commits the start guard (state stays active).
 */
#include <grpcpp/server_builder.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "async/async.hpp"
#include "common/proto/pb/posix/sandbox_api.grpc.pb.h"
#include "runtime_manager/executor/sandboxd/sandboxd_executor.h"
#include "runtime_manager/healthcheck/health_check.h"

namespace {

using functionsystem::StatusCode;
using functionsystem::runtime_manager::HealthCheck;
using functionsystem::runtime_manager::SandboxdExecutor;

struct RecordedRestore {
    int restores = 0;
    int starts = 0;
    std::string checkpointDir;
    std::string checkpointId;
    std::string expectedSha256;
    int64_t expectedSize = -1;
    std::string sandboxId;
};

class FakeSandboxdService final : public runtime::v1::SandboxService::Service {
public:
    grpc::Status Restore(grpc::ServerContext *, const runtime::v1::RestoreRequest *req,
                         runtime::v1::RestoreResponse *rsp) override
    {
        const std::lock_guard<std::mutex> lock(mu_);
        ++recorded_.restores;
        recorded_.checkpointDir = req->checkpoint_dir();
        recorded_.checkpointId = req->checkpoint_id();
        recorded_.expectedSha256 = req->expected_sha256();
        recorded_.expectedSize = req->expected_size();
        recorded_.sandboxId = req->sandbox().sandbox_id();
        rsp->set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        rsp->set_id("sbox-restored-1");
        rsp->set_resource_generation("gen-restore-9");
        return grpc::Status::OK;
    }

    grpc::Status Start(grpc::ServerContext *, const runtime::v1::StartRequest *,
                       runtime::v1::StartResponse *rsp) override
    {
        const std::lock_guard<std::mutex> lock(mu_);
        ++recorded_.starts;  // a managed restore must NEVER land here
        rsp->set_code(static_cast<int32_t>(StatusCode::SUCCESS));
        rsp->set_id("sbox-cold-started");
        return grpc::Status::OK;
    }

    grpc::Status Wait(grpc::ServerContext *, const runtime::v1::WaitRequest *,
                      runtime::v1::WaitResponse *rsp) override
    {
        rsp->set_exit_code(0);
        rsp->set_status(0);
        return grpc::Status::OK;
    }

    grpc::Status ListAvailableRuntimes(grpc::ServerContext *, const runtime::v1::ListAvailableRuntimesRequest *,
                                        runtime::v1::ListAvailableRuntimesResponse *rsp) override
    {
        rsp->add_runtime_classes("runsc");
        return grpc::Status::OK;
    }

    RecordedRestore Snapshot() const
    {
        const std::lock_guard<std::mutex> lock(mu_);
        return recorded_;
    }

private:
    mutable std::mutex mu_;
    RecordedRestore recorded_;
};

class ManagedRestoreDispatchTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        const auto uid = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        base_ = "/tmp/managed_restore_" + uid;
        std::filesystem::create_directories(base_ + "/managed/snap-m1");
        { std::ofstream f(base_ + "/managed/snap-m1/checkpoint.img", std::ios::binary); f << "artifact"; }

        const std::string sock = "unix:///tmp/managed_restore_" + uid + ".sock";
        builderSock_ = sock;
        serverBuilder_.AddListeningPort(sock, grpc::InsecureServerCredentials());
        serverBuilder_.RegisterService(&service_);
        server_ = serverBuilder_.BuildAndStart();
        ASSERT_NE(server_, nullptr);

        ::setenv("CONTAINER_EP", sock.c_str(), 1);  // NOLINT(concurrency-mt-unsafe)
        executor_ = std::make_shared<SandboxdExecutor>("SandboxdExec_" + uid, litebus::AID("agent-test"), base_,
                                                       nullptr);
        litebus::Spawn(executor_);
        executor_->SetHealthCheckClient(std::make_shared<HealthCheck>("hc_" + uid));
        litebus::Async(executor_->GetAID(), &SandboxdExecutor::InitConfig).Get();
    }

    void TearDown() override
    {
        if (executor_) {
            litebus::Terminate(executor_->GetAID());
            litebus::Await(executor_->GetAID());
        }
        executor_ = nullptr;
        if (server_) {
            server_->Shutdown();
            server_.reset();
        }
        std::filesystem::remove_all(base_);
    }

    std::shared_ptr<messages::StartInstanceRequest> MakeRequest(const std::string &runtimeID,
                                                                const std::string &restoreID) const
    {
        auto request = std::make_shared<messages::StartInstanceRequest>();
        auto *info = request->mutable_runtimeinstanceinfo();
        info->set_traceid("trace-mr");
        info->set_requestid("req-" + runtimeID);
        info->set_instanceid("inst-mr");
        info->set_runtimeid(runtimeID);
        info->set_restoresnapshotid(restoreID);
        // self-contained bootstrap keeps command building minimal
        (*info->mutable_deploymentconfig()->mutable_deployoptions())["rootfs"] =
            R"({"type":"local","path":"/tmp/fake-rootfs"})";
        info->mutable_bootstrapconfig()->set_entrypoint("/bin/true");
        return request;
    }

    messages::StartInstanceResponse Start(const std::shared_ptr<messages::StartInstanceRequest> &request)
    {
        return litebus::Async(executor_->GetAID(), &SandboxdExecutor::StartInstance, request,
                              std::vector<int>{}).Get();
    }

    std::string base_;
    std::string builderSock_;
    grpc::ServerBuilder serverBuilder_;
    FakeSandboxdService service_;
    std::unique_ptr<grpc::Server> server_;
    std::shared_ptr<SandboxdExecutor> executor_;
};

// Reusable managed request (no snapshotinfo): Restore carries the managed
// directory and an ALL-EMPTY identity quartet; the new sandbox id and
// generation land in the state manager; nothing ever cold-starts.
TEST_F(ManagedRestoreDispatchTest, ReusableManagedRestoreReachesRestoreWithManagedDirAndEmptyQuartet)
{
    const auto response = Start(MakeRequest("rt-managed-1", "snap-m1"));

    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(response.startruntimeinstanceresponse().sandboxid(), "sbox-restored-1");

    const auto recorded = service_.Snapshot();
    EXPECT_EQ(recorded.restores, 1);
    EXPECT_EQ(recorded.starts, 0) << "a managed restore must never fall through to a cold start";
    EXPECT_EQ(recorded.checkpointDir, base_ + "/managed/snap-m1");
    EXPECT_TRUE(recorded.checkpointId.empty());
    EXPECT_TRUE(recorded.expectedSha256.empty());
    EXPECT_EQ(recorded.expectedSize, 0);
    EXPECT_TRUE(recorded.sandboxId.empty());

    const auto info = executor_->stateManager_.Find("rt-managed-1");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->sandboxID, "sbox-restored-1");
    EXPECT_EQ(info->resourceGeneration, "gen-restore-9");
    EXPECT_TRUE(executor_->IsRuntimeActive("rt-managed-1")) << "guard must have committed";
}

// Complete snapshotinfo fills the whole quartet together (strict restore).
TEST_F(ManagedRestoreDispatchTest, CompleteSnapshotInfoFillsIdentityQuartetTogether)
{
    auto request = MakeRequest("rt-managed-2", "snap-m1");
    auto *snap = request->mutable_runtimeinstanceinfo()->mutable_snapshotinfo();
    snap->set_checkpointid("snap-m1");
    snap->set_sha256("aabbccdd");
    snap->set_size(123);
    snap->set_sandbox_id("sbox-src-9");

    EXPECT_EQ(Start(request).code(), static_cast<int32_t>(StatusCode::SUCCESS));

    const auto recorded = service_.Snapshot();
    ASSERT_EQ(recorded.restores, 1);
    EXPECT_EQ(recorded.checkpointDir, base_ + "/managed/snap-m1");
    EXPECT_EQ(recorded.checkpointId, "snap-m1");
    EXPECT_EQ(recorded.expectedSha256, "aabbccdd");
    EXPECT_EQ(recorded.expectedSize, 123);
    EXPECT_EQ(recorded.sandboxId, "sbox-src-9");
}

// Half-supplied snapshotinfo (no checkpointid): the quartet must stay
// ALL-EMPTY — sending sha/size with an empty checkpoint_id is the exact
// half-filled strict request production sandboxd rejects with 80004.
TEST_F(ManagedRestoreDispatchTest, HalfSnapshotInfoKeepsQuartetAllEmpty)
{
    auto request = MakeRequest("rt-managed-3", "snap-m1");
    auto *snap = request->mutable_runtimeinstanceinfo()->mutable_snapshotinfo();
    snap->set_sha256("aabbccdd");
    snap->set_size(123);
    snap->set_sandbox_id("sbox-src-9");  // checkpointid deliberately empty

    EXPECT_EQ(Start(request).code(), static_cast<int32_t>(StatusCode::SUCCESS));

    const auto recorded = service_.Snapshot();
    ASSERT_EQ(recorded.restores, 1);
    EXPECT_TRUE(recorded.checkpointId.empty()) << "half-filled quartet must never be sent";
    EXPECT_TRUE(recorded.expectedSha256.empty());
    EXPECT_EQ(recorded.expectedSize, 0);
    EXPECT_TRUE(recorded.sandboxId.empty());
}

// Missing managed artifact: explicit failure, ZERO dispatches of any kind,
// and the registered runtime state is rolled back.
TEST_F(ManagedRestoreDispatchTest, MissingArtifactFailsExplicitlyWithZeroDispatches)
{
    const auto response = Start(MakeRequest("rt-managed-4", "snap-gone"));

    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CHECKPOINT_FAILED));
    EXPECT_NE(response.message().find("managed restore artifact directory missing"), std::string::npos);

    const auto recorded = service_.Snapshot();
    EXPECT_EQ(recorded.restores, 0);
    EXPECT_EQ(recorded.starts, 0) << "a missing artifact must never degrade into a fresh instance";
    EXPECT_FALSE(executor_->stateManager_.Find("rt-managed-4").has_value());
}

// Traversal-shaped restore ids are rejected before any RPC leaves the
// executor.
TEST_F(ManagedRestoreDispatchTest, TraversalIdentityRejectedBeforeAnyRpc)
{
    const auto response = Start(MakeRequest("rt-managed-5", "../escape"));
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::PARAMETER_ERROR));

    const auto recorded = service_.Snapshot();
    EXPECT_EQ(recorded.restores, 0);
    EXPECT_EQ(recorded.starts, 0);
    EXPECT_FALSE(executor_->stateManager_.Find("rt-managed-5").has_value());
}

}  // namespace
