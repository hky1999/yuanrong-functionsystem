/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "sandboxd_checkpoint_orchestrator.h"

#include "common/logs/logging.h"
#include "common/status/status.h"

namespace functionsystem::runtime_manager {

namespace {
constexpr int64_t CHECKPOINT_TIMEOUT_NS = 30000000000;
}

SandboxdCheckpointOrchestrator::SandboxdCheckpointOrchestrator(
    litebus::AID ownerAID, std::shared_ptr<GrpcClient<runtime::v1::SandboxService>> sandboxd,
    std::shared_ptr<CkptFileManager> ckptFileManager, RuntimeStateManager &stateManager,
    std::string checkpointDir)
    : ownerAID_(std::move(ownerAID)),
      sandboxd_(std::move(sandboxd)),
      ckptFileManager_(std::move(ckptFileManager)),
      stateManager_(stateManager),
      checkpointDir_(std::move(checkpointDir))
{
}

// ── TakeSnapshot ──────────────────────────────────────────────────────────────

litebus::Future<messages::SnapshotRuntimeResponse> SandboxdCheckpointOrchestrator::TakeSnapshot(
    const std::shared_ptr<messages::SnapshotRuntimeRequest> &request)
{
    const std::string &runtimeID    = request->runtimeid();
    const std::string &requestID    = request->requestid();
    const std::string &instanceID   = request->instanceid();
    int32_t ttl                     = request->ttl();

    messages::SnapshotRuntimeResponse response;
    response.set_requestid(requestID);

    // Verify the sandbox exists
    if (!stateManager_.HasSandbox(runtimeID)) {
        YRLOG_ERROR("{}|TakeSnapshot: runtime({}) not found", requestID, runtimeID);
        response.set_code(static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_RUNTIME_NOT_FOUND));
        response.set_message(fmt::format("runtime {} not found", runtimeID));
        return response;
    }

    // Validate instanceID to prevent path traversal when building checkpoint path
    if (instanceID.find("..") != std::string::npos || instanceID.find('/') != std::string::npos || instanceID.empty()) {
        YRLOG_ERROR("{}|TakeSnapshot: invalid instanceID({})", requestID, instanceID);
        response.set_code(static_cast<int32_t>(StatusCode::PARAMETER_ERROR));
        response.set_message("invalid instanceID");
        return response;
    }

    // Generate unique checkpoint ID and local path
    const std::string sandboxID = stateManager_.GetSandboxID(runtimeID);
    const std::string checkpointID =
        fmt::format("ckpt-{}-{}", instanceID, std::chrono::system_clock::now().time_since_epoch().count());
    // must live under sandboxd's managed checkpoint root (config.RootDir/checkpoints);
    // the deploy scripts point --checkpoint_dir at a subdirectory of that root
    const std::string checkpointPath = fmt::format("{}/{}", checkpointDir_, checkpointID);

    auto ckptReq = std::make_shared<runtime::v1::CheckpointRequest>();
    ckptReq->set_id(sandboxID);
    ckptReq->set_checkpoint_dir(checkpointPath);
    ckptReq->set_timeout(CHECKPOINT_TIMEOUT_NS);
    ckptReq->set_compress(true);
    ckptReq->set_trace_id(requestID);
    // sandboxd PR#16 requires the logical checkpoint identity and the
    // physical mode (keep source alive or delete after publish)
    ckptReq->set_checkpoint_id(checkpointID);
    ckptReq->set_leave_running(request->leaverunning());

    SnapshotContext context{requestID, runtimeID, checkpointID, checkpointPath, sandboxID, ttl};
    return DoCheckpoint(ckptReq).Then(
        litebus::Defer(ownerAID_,
            [self = shared_from_this(), context](const runtime::v1::CheckpointResponse &response) {
                return self->OnCheckpointDone(response, context);
            }));
}

litebus::Future<messages::SnapshotRuntimeResponse> SandboxdCheckpointOrchestrator::OnCheckpointDone(
    const runtime::v1::CheckpointResponse &ckptResponse,
    const SnapshotContext &context)
{
    messages::SnapshotRuntimeResponse response;
    response.set_requestid(context.requestID);

    if (!ckptResponse.success()) {
        YRLOG_ERROR("{}|checkpoint failed for runtime({}): {}", context.requestID, context.runtimeID,
                    ckptResponse.message());
        response.set_code(static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CHECKPOINT_FAILED));
        response.set_message(ckptResponse.message());
        return response;
    }

    YRLOG_INFO("{}|checkpoint succeeded, uploading checkpoint({}) for runtime({})", context.requestID,
               context.checkpointID, context.runtimeID);

    ASSERT_IF_NULL(ckptFileManager_);
    return ckptFileManager_
        ->RegisterCheckpoint(context.checkpointID, context.checkpointPath, context.checkpointID, context.ttl)
        .Then(litebus::Defer(ownerAID_,
            [self = shared_from_this(), response, context](const CheckpointUploadResult &upload) {
                return self->OnRegisterDone(upload, response, context);
            }));
}

litebus::Future<messages::SnapshotRuntimeResponse> SandboxdCheckpointOrchestrator::OnRegisterDone(
    const CheckpointUploadResult &upload, messages::SnapshotRuntimeResponse response, const SnapshotContext &context)
{
    auto *info = response.mutable_snapshotinfo();
    info->set_checkpointid(context.checkpointID);
    info->set_storage(upload.storageUrl);
    info->set_ttlseconds(context.ttl);
    // 归档的完整性元数据：跨节点 restore 用 expected_sha256/expected_size 校验
    info->set_size(upload.size);
    info->set_createtime(upload.createTime);
    info->set_sha256(upload.sha256);
    // 源沙箱 id：strict restore（identity-checked）路径的 config.sandbox_id 来源
    info->set_sandbox_id(context.sandboxID);

    const std::string &storageUrl = upload.storageUrl;
    if (storageUrl.empty()) {
        YRLOG_ERROR("{}|RegisterCheckpoint returned empty storageUrl for runtime({})", context.requestID,
                    context.runtimeID);
        response.set_code(static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_CHECKPOINT_FAILED));
        response.set_message("checkpoint registration failed: empty storage URL");
        return response;
    }

    // Register in state manager — must happen before returning success so that
    // subsequent StopInstance calls can release the reference.
    stateManager_.SetCheckpointID(context.runtimeID, context.checkpointID);

    YRLOG_INFO("{}|snapshot complete: runtime({}) checkpoint({}) storage({})", context.requestID, context.runtimeID,
               context.checkpointID, storageUrl);
    response.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    response.set_message("snapshot created successfully");
    return response;
}

// ── DownloadForRestore ────────────────────────────────────────────────────────

litebus::Future<std::string> SandboxdCheckpointOrchestrator::DownloadForRestore(const std::string &checkpointID,
                                                                                const std::string &storageUrl,
                                                                                const std::string &requestID,
                                                                                const std::string &expectedSha256,
                                                                                int64_t expectedSize)
{
    YRLOG_INFO("{}|downloading checkpoint({}) from {}", requestID, checkpointID, storageUrl);
    ASSERT_IF_NULL(ckptFileManager_);
    return ckptFileManager_->DownloadCheckpoint(checkpointID, storageUrl, expectedSha256, expectedSize);
}

// ── AddRef ────────────────────────────────────────────────────────────────────

litebus::Future<Status> SandboxdCheckpointOrchestrator::AddRef(const std::string &checkpointID,
                                                               const std::string &runtimeID,
                                                               const std::string &requestID)
{
    ASSERT_IF_NULL(ckptFileManager_);
    return ckptFileManager_->AddReference(checkpointID)
        .Then(litebus::Defer(
            ownerAID_, [self = shared_from_this(), checkpointID, runtimeID, requestID](const Status &s) -> Status {
                if (s.IsError()) {
                    YRLOG_ERROR("{}|AddRef failed for checkpoint({}) runtime({}): {}", requestID, checkpointID,
                                runtimeID, s.RawMessage());
                    return s;
                }
                // Register mapping so StopInstance can release the ref
                self->stateManager_.SetCheckpointID(runtimeID, checkpointID);
                YRLOG_INFO("{}|AddRef succeeded for checkpoint({}) runtime({})", requestID, checkpointID, runtimeID);
                return Status::OK();
            }));
}

// ── ReleaseRef ────────────────────────────────────────────────────────────────

litebus::Future<Status> SandboxdCheckpointOrchestrator::ReleaseRef(const std::string &runtimeID,
                                                                   const std::string &requestID)
{
    std::string checkpointID = stateManager_.GetCheckpointID(runtimeID);
    if (checkpointID.empty()) {
        YRLOG_DEBUG("{}|ReleaseRef: no checkpoint for runtime({}), skipping", requestID, runtimeID);
        return Status::OK();
    }

    YRLOG_INFO("{}|releasing checkpoint({}) ref for runtime({})", requestID, checkpointID, runtimeID);
    // Clear from state first — even if RemoveReference fails we won't double-release
    stateManager_.ClearCheckpointID(runtimeID);

    if (!ckptFileManager_) {
        return Status::OK();
    }
    return ckptFileManager_->RemoveReference(checkpointID)
        .Then([checkpointID, requestID, runtimeID](const Status &s) -> Status {
            if (s.IsError()) {
                YRLOG_WARN("{}|RemoveReference failed for checkpoint({}) runtime({}): {}", requestID, checkpointID,
                           runtimeID, s.RawMessage());
            }
            return Status::OK();
        });
}

// ── gRPC wrapper ─────────────────────────────────────────────────────────────

litebus::Future<runtime::v1::CheckpointResponse> SandboxdCheckpointOrchestrator::DoCheckpoint(
    const std::shared_ptr<runtime::v1::CheckpointRequest> &req)
{
    ASSERT_IF_NULL(sandboxd_);
    auto resp = std::make_shared<runtime::v1::CheckpointResponse>();
    return sandboxd_->CallAsyncX("Checkpoint", *req, resp.get(), &runtime::v1::SandboxService::Stub::AsyncCheckpoint)
        .Then([req, resp](const Status &status) -> litebus::Future<runtime::v1::CheckpointResponse> {
            if (status.IsOk()) {
                return *resp;
            }
            runtime::v1::CheckpointResponse err;
            err.set_success(false);
            err.set_message(fmt::format("checkpoint gRPC failed for sandbox {}: {}", req->id(), status.RawMessage()));
            YRLOG_ERROR("{}|{}", req->trace_id(), err.message());
            return err;
        });
}

}  // namespace functionsystem::runtime_manager
