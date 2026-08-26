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

#ifndef RUNTIME_MANAGER_EXECUTOR_SANDBOXD_SANDBOXD_CHECKPOINT_ORCHESTRATOR_H
#define RUNTIME_MANAGER_EXECUTOR_SANDBOXD_SANDBOXD_CHECKPOINT_ORCHESTRATOR_H

#include <memory>
#include <string>

#include "async/defer.hpp"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix/sandbox_api.grpc.pb.h"
#include "common/rpc/client/grpc_client.h"
#include "common/status/status.h"
#include "runtime_manager/ckpt/ckpt_file_manager.h"
#include "runtime_manager/ckpt/ckpt_file_manager_actor.h"
#include "runtime_manager/executor/sandboxd/runtime_state_manager.h"

namespace functionsystem::runtime_manager {

/**
 * SandboxdCheckpointOrchestrator — checkpoint lifecycle against the sandboxd
 * SandboxService (Checkpoint + Restore RPCs).
 *
 * It is the sandboxd counterpart of CheckpointOrchestrator: same TakeSnapshot /
 * download / ref-count flow, but drives sandboxd's SandboxCheckpoint RPC and
 * reuses the shared CkptFileManager (storage upload/download/ref) and
 * RuntimeStateManager (checkpoint-id tracking).
 */
class SandboxdCheckpointOrchestrator : public std::enable_shared_from_this<SandboxdCheckpointOrchestrator> {
public:
    SandboxdCheckpointOrchestrator(litebus::AID ownerAID,
                                   std::shared_ptr<GrpcClient<runtime::v1::SandboxService>> sandboxd,
                                   std::shared_ptr<CkptFileManager> ckptFileManager, RuntimeStateManager &stateManager,
                                   std::string checkpointDir = DEFAULT_CHECKPOINT_DIR);
    ~SandboxdCheckpointOrchestrator() = default;

    // ── Snapshot ──────────────────────────────────────────────────────────────

    /**
     * Take a checkpoint of a running sandbox.
     * Chain: DoCheckpoint (sandboxd Checkpoint RPC) -> RegisterCheckpoint (upload)
     * -> update state manager.
     */
    litebus::Future<messages::SnapshotRuntimeResponse> TakeSnapshot(
        const std::shared_ptr<messages::SnapshotRuntimeRequest> &request);

    // ── Restore ───────────────────────────────────────────────────────────────

    /**
     * Download a checkpoint and return its local path.
     * expectedSha256/expectedSize come from SnapshotInfo and are verified
     * against the downloaded archive (mismatch = explicit error).
     * Caller follows up with AddRef() + the executor's Restore RPC.
     */
    litebus::Future<std::string> DownloadForRestore(const std::string &checkpointID, const std::string &storageUrl,
                                                    const std::string &requestID,
                                                    const std::string &expectedSha256 = "",
                                                    int64_t expectedSize = 0);

    /**
     * Add a reference for checkpointID. On success: records runtimeID->checkpointID
     * in the state manager so StopInstance can release it.
     */
    litebus::Future<Status> AddRef(const std::string &checkpointID, const std::string &runtimeID,
                                   const std::string &requestID);

    // ── Release ───────────────────────────────────────────────────────────────

    /**
     * Release the checkpoint reference held by runtimeID. Safe no-op if none.
     */
    litebus::Future<Status> ReleaseRef(const std::string &runtimeID, const std::string &requestID);

    // ── gRPC wrapper ──────────────────────────────────────────────────────────

    litebus::Future<runtime::v1::CheckpointResponse> DoCheckpoint(
        const std::shared_ptr<runtime::v1::CheckpointRequest> &req);

private:
    struct SnapshotContext {
        std::string requestID;
        std::string runtimeID;
        std::string checkpointID;
        std::string checkpointPath;
        // 源沙箱 id：park 后 stateManager 会注销该映射，strict restore 需要
        // 它时只能从 SnapshotInfo 读，故随快照持久化
        std::string sandboxID;
        int32_t ttl = 0;
    };

    litebus::Future<messages::SnapshotRuntimeResponse> OnCheckpointDone(
        const runtime::v1::CheckpointResponse &ckptResponse, const SnapshotContext &context);

    litebus::Future<messages::SnapshotRuntimeResponse> OnRegisterDone(const CheckpointUploadResult &upload,
        messages::SnapshotRuntimeResponse response,
        const SnapshotContext &context);

    litebus::AID ownerAID_;
    std::shared_ptr<GrpcClient<runtime::v1::SandboxService>> sandboxd_;
    std::shared_ptr<CkptFileManager> ckptFileManager_;
    RuntimeStateManager &stateManager_;
    std::string checkpointDir_;
};

}  // namespace functionsystem::runtime_manager

#endif  // RUNTIME_MANAGER_EXECUTOR_SANDBOXD_SANDBOXD_CHECKPOINT_ORCHESTRATOR_H
