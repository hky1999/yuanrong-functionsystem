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
 *  - a REAL TakeSnapshot goes out as CheckpointIfGeneration (the wrapper
 *    proto the orchestrator actually sends): the nested CheckpointRequest
 *    carries the agent's checkpoint_id + staging dir + leave_running +
 *    include_filesystem, and expected_generation rides the wrapper —
 *    CheckpointRequest itself has no generation field;
 *  - port-forward bookkeeping follows StartNormal semantics: mappings are
 *    allocated + recorded before dispatch, appear on the Restore RPC's
 *    sandbox, SURVIVE success, are RELEASED in the real PortManager only on
 *    a CONFIRMED sandboxd failure reply, and are RETAINED (explicit UNKNOWN
 *    to the caller) when the reply is lost on the wire — provenance comes
 *    from DoRestoreObserved's replyReceived, not error-code guessing;
 *  - a missing managed artifact fails EXPLICITLY — zero Start AND zero
 *    Restore dispatches (never a silent fresh instance);
 *  - traversal-shaped ids are rejected before any RPC;
 *  - success registers the NEW sandbox id + generation in the state
 *    manager and commits the start guard (state stays active);
 *  - the anonymous snapshot chain (snapshotinfo, no restoresnapshotid)
 *    never allocates ports here, and a failed anonymous restore must not
 *    disturb any PortManager allocation (legacy compatibility).
 */
#include <grpcpp/server_builder.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "async/async.hpp"
#include "common/proto/pb/posix/sandbox_api.grpc.pb.h"
#include "runtime_manager/executor/sandboxd/sandboxd_executor.h"
#include "runtime_manager/healthcheck/health_check.h"
#include "runtime_manager/port/port_manager.h"

namespace {

using functionsystem::StatusCode;
using functionsystem::runtime_manager::HealthCheck;
using functionsystem::runtime_manager::PortManager;
using functionsystem::runtime_manager::SandboxdExecutor;

struct RecordedRestore {
    int restores = 0;
    int starts = 0;
    int checkpoints = 0;
    std::string checkpointDir;
    std::string checkpointId;
    std::string expectedSha256;
    int64_t expectedSize = -1;
    std::string sandboxId;
    std::vector<std::string> restorePorts;
    std::string ckptSandboxId;
    std::string ckptDir;
    std::string ckptCheckpointId;
    std::string ckptExpectedGeneration;
    bool ckptLeaveRunning = false;
    bool ckptIncludeFilesystem = false;
};

class FakeSandboxdService final : public runtime::v1::SandboxService::Service {
public:
    // The REAL physical interface: the orchestrator sends CheckpointIfGeneration
    // with the fence on the wrapper, never a bare Checkpoint.
    grpc::Status CheckpointIfGeneration(grpc::ServerContext *,
                                        const runtime::v1::CheckpointIfGenerationRequest *req,
                                        runtime::v1::CheckpointResponse *rsp) override
    {
        const std::lock_guard<std::mutex> lock(mu_);
        ++recorded_.checkpoints;
        recorded_.ckptSandboxId = req->checkpoint().id();
        recorded_.ckptDir = req->checkpoint().checkpoint_dir();
        recorded_.ckptCheckpointId = req->checkpoint().checkpoint_id();
        recorded_.ckptExpectedGeneration = req->expected_generation();
        recorded_.ckptLeaveRunning = req->checkpoint().leave_running();
        recorded_.ckptIncludeFilesystem = req->checkpoint().include_filesystem();
        rsp->set_success(true);
        // echo the fenced generation: OnCheckpointDone requires an exact match
        rsp->set_source_generation(req->expected_generation());
        return grpc::Status::OK;
    }

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
        recorded_.restorePorts.assign(req->sandbox().ports().begin(), req->sandbox().ports().end());
        if (transportFailRestore_) {
            // the reply is lost on the wire: DoRestore wraps the channel
            // failure as a plain error response — the physical outcome is
            // UNKNOWN even though the request may have executed
            ++transportFailures_;
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "fake transport cut after dispatch");
        }
        if (failRestore_) {
            rsp->set_code(static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CREATE_EXEC_FAILED));
            rsp->set_message("fake restore rejected");
            return grpc::Status::OK;
        }
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

    void SetFailRestore(bool fail)
    {
        const std::lock_guard<std::mutex> lock(mu_);
        failRestore_ = fail;
    }

    void SetTransportFailRestore(bool fail)
    {
        const std::lock_guard<std::mutex> lock(mu_);
        transportFailRestore_ = fail;
    }

    int TransportFailures() const
    {
        const std::lock_guard<std::mutex> lock(mu_);
        return transportFailures_;
    }

private:
    mutable std::mutex mu_;
    RecordedRestore recorded_;
    bool failRestore_ = false;
    bool transportFailRestore_ = false;
    int transportFailures_ = 0;
};

class ManagedRestoreDispatchTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        const auto uid = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        base_ = "/tmp/managed_restore_" + uid;
        std::filesystem::create_directories(base_ + "/managed/snap-m1");
        { std::ofstream f(base_ + "/managed/snap-m1/checkpoint.img", std::ios::binary); f << "artifact"; }
        std::filesystem::create_directories("/tmp/fake-rootfs");  // rootfs path referenced by every request

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
        // InitConfig returns void (no Get() on the wrapper); enqueue it on
        // the ACTOR and never touch actor internals from the test thread.
        // The mailbox is FIFO per AID, so every later Start/Snapshot future
        // resolved in these tests is itself the proof that InitConfig
        // (sandboxd_ client + ckptOrch_ construction) already ran.
        (void)litebus::Async(executor_->GetAID(), &SandboxdExecutor::InitConfig);
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
        // the builder validates a non-empty sandbox runtime before any RPC
        info->mutable_container()->set_runtime("runsc");
        // self-contained bootstrap keeps command building minimal
        (*info->mutable_deploymentconfig()->mutable_deployoptions())["rootfs"] =
            R"({"type":"local","path":"/tmp/fake-rootfs"})";
        info->mutable_bootstrapconfig()->set_entrypoint("/bin/true");
        return request;
    }

    // Adds one tcp port-forwarding entry (container port 18080) in the exact
    // deploy-option shape ApplyPortForwardMappings parses in production.
    static void AddPortForwarding(const std::shared_ptr<messages::StartInstanceRequest> &request)
    {
        (*request->mutable_runtimeinstanceinfo()->mutable_deploymentconfig()->mutable_deployoptions())["network"] =
            R"({"portForwardings":[{"port":18080,"protocol":"tcp"}]})";
    }

    messages::StartInstanceResponse Start(const std::shared_ptr<messages::StartInstanceRequest> &request)
    {
        return litebus::Async(executor_->GetAID(), &SandboxdExecutor::StartInstance, request,
                              std::vector<int>{}).Get();
    }

    // The guard destructor (state rollback) runs when the DoRestore continuation
    // releases its bound copy, which may complete just after the response
    // future resolves — poll briefly instead of asserting a strict ordering.
    static bool WaitUnregistered(SandboxdExecutor &executor, const std::string &runtimeID)
    {
        for (int i = 0; i < 200; ++i) {
            if (!executor.stateManager_.Find(runtimeID).has_value()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
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

// REAL TakeSnapshot chain for a MANAGED request: the physical RPC is
// CheckpointIfGeneration — the wrapper carries the fence, the nested
// CheckpointRequest carries the agent's id/staging dir/physical mode — and
// the completion must return the agent's id (the ov24c-1 failure point).
TEST_F(ManagedRestoreDispatchTest, ManagedTakeSnapshotCarriesAgentIdentityAndDirToCheckpointRpc)
{
    // register a live source sandbox with a birth generation
    functionsystem::runtime_manager::SandboxInfo src;
    src.runtimeID = "rt-snap-1";
    src.sandboxID = "sbox-src-live";
    src.resourceGeneration = "gen-live-1";
    executor_->stateManager_.Register(std::move(src));

    auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
    request->set_runtimeid("rt-snap-1");
    request->set_requestid("req-snap-1");
    request->set_instanceid("inst-mr");
    request->set_snapshotid("snap-agent-77");
    request->set_checkpointdir(base_ + "/managed/snap-agent-77");
    request->set_expected_sandbox_id("sbox-src-live");
    request->set_expected_resource_generation("gen-live-1");
    request->set_leaverunning(true);  // park semantics: keep the source alive

    const auto response =
        litebus::Async(executor_->GetAID(), &SandboxdExecutor::SnapshotRuntime, request).Get();

    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(response.snapshotinfo().checkpointid(), "snap-agent-77");

    const auto recorded = service_.Snapshot();
    EXPECT_EQ(recorded.checkpoints, 1);
    EXPECT_EQ(recorded.ckptSandboxId, "sbox-src-live");
    EXPECT_EQ(recorded.ckptDir, base_ + "/managed/snap-agent-77")
        << "the physical Checkpoint RPC must use the agent's staging directory";
    EXPECT_EQ(recorded.ckptCheckpointId, "snap-agent-77")
        << "sandboxd PR#16 requires the logical checkpoint identity on the wire";
    EXPECT_EQ(recorded.ckptExpectedGeneration, "gen-live-1")
        << "the generation fence rides the CheckpointIfGenerationRequest wrapper";
    EXPECT_TRUE(recorded.ckptLeaveRunning) << "leave_running must pass through from the request";
    EXPECT_TRUE(recorded.ckptIncludeFilesystem)
        << "agent tools mutate the writable rootfs; memory-only park cannot restore that state";
    // managed completion never maps runtime->checkpoint in the state manager
    EXPECT_TRUE(executor_->stateManager_.GetCheckpointID("rt-snap-1").empty());
}

// Half-passed managed identity (id without dir) is rejected BEFORE any RPC.
TEST_F(ManagedRestoreDispatchTest, HalfPassedManagedSnapshotIdentityRejectedBeforeRpc)
{
    functionsystem::runtime_manager::SandboxInfo src;
    src.runtimeID = "rt-snap-2";
    src.sandboxID = "sbox-src-live2";
    src.resourceGeneration = "gen-live-2";
    executor_->stateManager_.Register(std::move(src));

    auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
    request->set_runtimeid("rt-snap-2");
    request->set_requestid("req-snap-2");
    request->set_instanceid("inst-mr");
    request->set_snapshotid("snap-half-88");  // checkpointdir deliberately empty
    request->set_expected_sandbox_id("sbox-src-live2");
    request->set_expected_resource_generation("gen-live-2");

    const auto response =
        litebus::Async(executor_->GetAID(), &SandboxdExecutor::SnapshotRuntime, request).Get();
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::PARAMETER_ERROR));

    const auto recorded = service_.Snapshot();
    EXPECT_EQ(recorded.checkpoints, 0) << "half-passed identity must never reach sandboxd";
}

// ── Managed-restore port-forward bookkeeping (real PortManager, no mocks) ─────

// SUCCESS keeps the allocation: the host port picked by the REAL PortManager
// lands on the Restore RPC's sandbox ("tcp:<host>:18080"), stays recorded in
// the state manager, and the mapping survives into the start response.
TEST_F(ManagedRestoreDispatchTest, ManagedRestoreWithPortsKeepsMappingsOnSuccess)
{
    auto request = MakeRequest("rt-port-ok", "snap-m1");
    AddPortForwarding(request);

    ASSERT_TRUE(PortManager::GetInstance().GetPorts("rt-port-ok").empty()) << "unique runtime must start clean";

    const auto response = Start(request);
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::SUCCESS));

    const auto held = PortManager::GetInstance().GetPorts("rt-port-ok");
    ASSERT_EQ(held.size(), 1UL) << "success must keep the host port allocated";

    const auto recorded = service_.Snapshot();
    ASSERT_EQ(recorded.restores, 1);
    ASSERT_EQ(recorded.restorePorts.size(), 1UL);
    EXPECT_EQ(recorded.restorePorts[0], "tcp:" + std::to_string(held[0]) + ":18080")
        << "the Restore RPC must carry the allocated host port mapping";
    EXPECT_FALSE(executor_->stateManager_.GetPortMappingsJson("rt-port-ok").empty())
        << "the canonical mapping record must survive a successful restore";
    EXPECT_EQ(response.startruntimeinstanceresponse().port(),
              executor_->stateManager_.GetPortMappingsJson("rt-port-ok"));

    PortManager::GetInstance().ReleasePorts("rt-port-ok");  // singleton hygiene
}

// CONFIRMED FAILURE releases the allocation in the REAL PortManager: an
// explicit sandboxd failure reply proves the restore produced no live
// instance, so the host ports allocated before dispatch go back to the pool
// (the start guard destructor alone would leak them until process restart).
TEST_F(ManagedRestoreDispatchTest, ManagedRestoreRpcFailureReleasesAllocatedPorts)
{
    service_.SetFailRestore(true);
    auto request = MakeRequest("rt-port-fail", "snap-m1");
    AddPortForwarding(request);

    const auto response = Start(request);
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CREATE_EXEC_FAILED));

    const auto recorded = service_.Snapshot();
    ASSERT_EQ(recorded.restores, 1) << "the failure must come from the physical Restore RPC";
    ASSERT_EQ(recorded.restorePorts.size(), 1UL) << "ports were allocated and dispatched before the failure";

    EXPECT_TRUE(PortManager::GetInstance().GetPorts("rt-port-fail").empty())
        << "a failed managed restore must return the host port to the pool";
    EXPECT_TRUE(executor_->stateManager_.GetPortMappingsJson("rt-port-fail").empty())
        << "the recorded mapping must be cleared with the allocation";
    EXPECT_TRUE(WaitUnregistered(*executor_, "rt-port-fail"))
        << "guard rollback must eventually unregister the failed runtime";
}

// TRANSPORT UNKNOWN retains the PHYSICAL allocation: the fake service takes
// the real gRPC-level failure path (UNAVAILABLE after dispatch), so
// DoRestoreObserved records replyReceived=false. The restore may have
// physically executed and a live instance may still hold the ports — the
// response must say UNKNOWN explicitly (never a plain failure that reads as
// "physically not executed"), and the ORIGINAL owner keeps the PortManager
// allocation, EXACTLY the port that went out on the dispatched RPC.
//
// Boundary (not a claim of full unknown-state convergence): only the
// PortManager allocation is retained. The LOCAL registration — including its
// port-mappings record — rolls back with the start guard, so the record is
// gone together with the runtime; reconciling a possibly-live physical
// instance against the retained allocation is later reconcile work.
TEST_F(ManagedRestoreDispatchTest, ManagedRestoreTransportFailureRetainsPortsAsUnknown)
{
    service_.SetTransportFailRestore(true);
    auto request = MakeRequest("rt-port-unknown", "snap-m1");
    AddPortForwarding(request);

    const auto response = Start(request);
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::ERR_INSTANCE_BUSY))
        << "an unobserved outcome must surface as explicitly unknown, not a plain create failure";
    EXPECT_NE(response.message().find("outcome unknown"), std::string::npos);
    EXPECT_NE(response.message().find("port ownership retained"), std::string::npos);

    EXPECT_EQ(service_.TransportFailures(), 1);
    const auto recorded = service_.Snapshot();
    ASSERT_EQ(recorded.restores, 1) << "the request DID leave the executor before the reply was lost";
    ASSERT_EQ(recorded.restorePorts.size(), 1UL) << "the dispatched RPC carried the allocated mapping";

    const auto held = PortManager::GetInstance().GetPorts("rt-port-unknown");
    ASSERT_EQ(held.size(), 1UL) << "an unknown outcome must NOT free the host port";
    EXPECT_EQ(recorded.restorePorts[0], "tcp:" + std::to_string(held[0]) + ":18080")
        << "the retained allocation must be EXACTLY the original owner's dispatched port";
    EXPECT_TRUE(WaitUnregistered(*executor_, "rt-port-unknown"))
        << "the local registration rolls back; only the physical allocation is retained";

    PortManager::GetInstance().ReleasePorts("rt-port-unknown");  // singleton hygiene
}

// LEGACY compatibility: the anonymous snapshot chain (snapshotinfo, no
// restoresnapshotid) never allocates ports in this path, and its restore
// failure must not disturb ANY PortManager allocation — an unrelated
// runtime's reserved ports survive untouched.
TEST_F(ManagedRestoreDispatchTest, AnonymousRestoreFailureLeavesPortManagerUntouched)
{
    // a locally registered anonymous artifact so the real chain runs:
    // DownloadForRestore short-circuits to the on-disk copy, AddRef succeeds,
    // and the failure lands in OnRestoreDone with restoresnapshotid EMPTY.
    const std::string anonID = "anon-compat-1";
    const std::string anonDir = base_ + "/anonymous/" + anonID;
    std::filesystem::create_directories(anonDir);
    { std::ofstream f(anonDir + "/checkpoint.img", std::ios::binary); f << "anonymous-artifact"; }
    // register through the executor's OWN manager (KV is uninitialized in the
    // test process, so registration stays local-only): Future<Status>-style
    // semantics — the outcome is a completed result value, not an exception
    const auto registered = executor_->ckptFileManager_->RegisterCheckpoint(anonID, anonDir, anonID, 60).Get();
    ASSERT_EQ(registered.storageUrl, anonID + ".zip") << "registration must succeed before the restore";

    auto request = MakeRequest("rt-anon-fail", "");
    auto *snap = request->mutable_runtimeinstanceinfo()->mutable_snapshotinfo();
    snap->set_checkpointid(anonID);
    snap->set_storage(anonID + ".zip");  // sha/size/sandbox_id empty: legacy quartet

    // an unrelated runtime holds a real allocation that must survive
    const auto bystander = PortManager::GetInstance().RequestPorts("rt-port-bystander", 1);
    ASSERT_EQ(bystander.size(), 1UL);

    service_.SetFailRestore(true);
    const auto response = Start(request);
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CREATE_EXEC_FAILED));

    const auto recorded = service_.Snapshot();
    EXPECT_EQ(recorded.restores, 1);
    EXPECT_TRUE(recorded.restorePorts.empty())
        << "the anonymous chain never puts port mappings on the wire";
    EXPECT_TRUE(PortManager::GetInstance().GetPorts("rt-anon-fail").empty());

    const auto survivor = PortManager::GetInstance().GetPorts("rt-port-bystander");
    ASSERT_EQ(survivor.size(), 1UL);
    EXPECT_EQ(survivor[0], bystander[0]) << "an anonymous restore failure must never free a bystander's port";

    PortManager::GetInstance().ReleasePorts("rt-port-bystander");  // singleton hygiene
}

// The executor-constructed anonymous cache root: the ckpt manager the
// executor spawned owns <base>/anonymous, proving the member-root wiring
// (not just a hand-passed path in the actor tests).
TEST_F(ManagedRestoreDispatchTest, ExecutorConstructsAnonymousCacheOnAnonymousSubRoot)
{
    const std::string expectedRoot = base_ + "/anonymous";
    EXPECT_EQ(executor_->anonymousRoot_.string(), expectedRoot);
    // the spawned actor was constructed with that same root: registering an
    // artifact there succeeds through the executor's own manager
    const std::string anonID = "ckpt-exec-anon";
    std::filesystem::create_directories(expectedRoot + "/" + anonID);
    { std::ofstream f(expectedRoot + "/" + anonID + "/checkpoint.img", std::ios::binary); f << "x"; }
    auto registered = executor_->ckptFileManager_->RegisterCheckpoint(
        anonID, expectedRoot + "/" + anonID, anonID, 60).Get();
    EXPECT_EQ(registered.storageUrl, anonID + ".zip");
}

}  // namespace
