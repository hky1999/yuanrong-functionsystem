/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FUNCTIONSYSTEM_SRC_RUNTIME_MANAGER_CKPT_CKPT_FILE_MANAGER_ACTOR_H
#define FUNCTIONSYSTEM_SRC_RUNTIME_MANAGER_CKPT_CKPT_FILE_MANAGER_ACTOR_H

#include "actor/actor.hpp"
#include "async/future.hpp"
#include "common/status/status.h"
#include "common/proto/pb/message_pb.h"

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace functionsystem::runtime_manager {

constexpr int32_t DEFAULT_CHECKPOINT_TTL_SECONDS = 3600;
inline constexpr const char* DEFAULT_CHECKPOINT_DIR = "/home/yuanrong/checkpoints";
// sandboxd PR#16 在 restore 时对 checkpoint_dir 下该文件做 expected_sha256/
// expected_size 校验（internal/checkpoint/store.go ImageName），摘要口径必须
// 与之对齐：SnapshotInfo 的 sha/size 描述的是 checkpoint.img，不是 KV 里的 zip。
inline constexpr const char* CHECKPOINT_IMAGE_NAME = "checkpoint.img";

/**
 * CheckpointFileInfo: Checkpoint file metadata with reference counting and TTL
 */
struct CheckpointFileInfo {
    std::string checkpointID;
    std::string localPath;
    std::string storageUrl;
    std::string sha256;   // hex digest of checkpoint.img inside localPath (sandboxd 口径)
    int64_t size = 0;     // checkpoint.img size in bytes
    int32_t refCount;  // Reference count from containers
    std::chrono::steady_clock::time_point createdTime;
    std::chrono::steady_clock::time_point lastAccessTime;
    std::chrono::steady_clock::time_point ttlStartTime;  // When ref count reached 0
    int32_t ttlSeconds;  // TTL in seconds after ref count reaches 0
    bool ttlActive;      // Whether TTL counting is active

    CheckpointFileInfo()
        : refCount(0), ttlSeconds(DEFAULT_CHECKPOINT_TTL_SECONDS), ttlActive(false)
    {
        auto now = std::chrono::steady_clock::now();
        createdTime = now;
        lastAccessTime = now;
        ttlStartTime = now;
    }
};

/**
 * CheckpointUploadResult: what RegisterCheckpoint hands back to the caller.
 * sha256/size describe checkpoint.img inside localPath — the artifact sandboxd
 * verifies at restore (NOT the KV zip archive; the zip is transport only).
 * createTime is unix epoch seconds, consumed by SnapshotInfo.createTime.
 * A registration whose checkpoint.img cannot be digested sets failed=true:
 * such a snapshot can never satisfy the identity-checked restore (sandboxd
 * requires checkpoint_id/sha/size all non-empty), so it must fail loudly
 * instead of poisoning the wake path.
 */
struct CheckpointUploadResult {
    std::string storageUrl;
    std::string sha256;
    int64_t size = 0;
    std::string createTime;
    bool failed = false;
    std::string failMessage;
};

/**
 * CkptFileManagerActor: Manages checkpoint files with download, reference counting, and TTL cleanup
 */
class CkptFileManagerActor : public litebus::ActorBase {
public:
    explicit CkptFileManagerActor(const std::string &name);

    /**
     * Constructor that allows overriding the default checkpoint directory.
     * Intended for use in tests that run in environments without access to the
     * default path.
     * @param name   Actor name
     * @param checkpointDir  Base directory for checkpoint file storage
     */
    CkptFileManagerActor(const std::string &name, const std::string &checkpointDir);

    ~CkptFileManagerActor() override = default;

    /**
     * Download checkpoint file from remote storage
     * @param checkpointID Unique checkpoint identifier
     * @param storageUrl Remote storage URL (used as storage key)
     * @param expectedSha256 sha256 of checkpoint.img from SnapshotInfo; after
     *        extraction the image is verified against it when non-empty
     *        (mismatch = explicit error)
     * @param expectedSize checkpoint.img size from SnapshotInfo; verified when > 0
     * @return Future with local file path
     */
    litebus::Future<std::string> DownloadCheckpoint(const std::string &checkpointID,
                                                     const std::string &storageUrl,
                                                     const std::string &expectedSha256 = "",
                                                     int64_t expectedSize = 0);

    /**
     * Increment reference count for a checkpoint file (container using it)
     * @param checkpointID Checkpoint identifier
     * @return Future with status
     */
    litebus::Future<Status> AddReference(const std::string &checkpointID);

    /**
     * Decrement reference count for a checkpoint file (container stopped using it)
     * @param checkpointID Checkpoint identifier
     * @return Future with status
     */
    litebus::Future<Status> RemoveReference(const std::string &checkpointID);

    /**
     * Register a locally created checkpoint file (from snapshot operation)
     * Digests localPath/checkpoint.img into sha256/size (sandboxd restore
     * verifies exactly this artifact). With the KV client initialized, also
     * zips the directory and uploads it (object key = checkpointID); archive
     * or upload problems degrade to local-only (single-node semantics must
     * not regress). A missing/undigestable checkpoint.img fails the future:
     * such snapshots are unrestorable via the identity-checked path.
     * @param checkpointID Unique checkpoint identifier
     * @param localPath Local checkpoint directory path (must contain checkpoint.img)
     * @param storageUrl Ignored (storageUrl is derived from localPath as parentPath.zip)
     * @param ttl Time to live in seconds (0 means no expiration)
     * @return Future with CheckpointUploadResult (storageUrl + sha256/size/createTime)
     */
    litebus::Future<CheckpointUploadResult> RegisterCheckpoint(const std::string &checkpointID,
                                                    const std::string &localPath,
                                                    const std::string &storageUrl,
                                                    int32_t ttl = 0);

    /**
     * Set TTL for checkpoint files (in seconds)
     * @param ttlSeconds Time to live after reference count reaches 0
     */
    void SetDefaultTTL(int32_t ttlSeconds);

    /**
     * Process-wide default TTL override sourced from the ckpt_default_ttl_sec
     * flag (W2 P0.4: retire the compiled-in 1800s constant). Called once by
     * the RuntimeManagerDriver before any actor is constructed; 0 = keep the
     * compiled-in default.
     */
    static void SetDefaultTtlOverride(int32_t ttlSeconds);
    static int32_t GetDefaultTtlOverride();

    /**
     * Start TTL cleanup timer
     */
    void StartCleanupTimer();

    /**
     * Stop TTL cleanup timer
     */
    void StopCleanupTimer();

    /**
     * Manually trigger cleanup of expired checkpoint files
     * @return Number of files deleted
     */
    litebus::Future<int32_t> CleanupExpiredFiles();

protected:
    void Init() override;
    void Finalize() override;

private:
    /**
     * Perform periodic cleanup of expired checkpoint files
     */
    void PeriodicCleanup();

    /**
     * Check if a checkpoint file has expired
     * @param info Checkpoint file info
     * @return True if expired
     */
    bool IsExpired(const CheckpointFileInfo &info) const;

    /**
     * Delete checkpoint file from local storage
     * @param checkpointID Checkpoint identifier
     * @return Status of deletion
     */
    Status DeleteCheckpointFile(const std::string &checkpointID);

    /**
     * Get local storage path for checkpoint
     * @param checkpointID Checkpoint identifier
     * @return Local file path
     */
    std::string GetLocalPath(const std::string &checkpointID) const;

    /**
     * Restore checkpoint files from local directory on startup
     */
    void RestoreCheckpointsFromLocal();

    /**
     * Handle successful download completion
     */
    void OnDownloadSuccess(const std::string &checkpointID, const CheckpointFileInfo &info);

    /**
     * Handle failed download
     */
    void OnDownloadFailed(const std::string &checkpointID, int32_t errorCode);

    /**
     * Handle successful upload completion
     */
    void OnUploadSuccess(const std::string &checkpointID,
                        const std::string &localPath,
                        const CheckpointUploadResult &result,
                        int32_t ttl,
                        litebus::Promise<CheckpointUploadResult> uploadPromise);

    /**
     * Zip a directory into a zip file
     * @param dirPath Directory to zip
     * @param zipPath Output zip file path
     * @return Status indicating success or failure
     */
    static Status ZipDirectory(const std::string &dirPath, const std::string &zipPath);

    /**
     * Compute a file's sha256 (hex) and byte size with chunked 4MiB reads
     * @param filePath File to digest
     * @param sha256Hex Output hex digest (lowercase)
     * @param sizeBytes Output file size in bytes
     * @return Status indicating success or failure
     */
    static Status ComputeFileSha256(const std::string &filePath, std::string &sha256Hex, int64_t &sizeBytes);

    /**
     * Unzip a file to a target directory
     * @param zipPath Zip file to extract
     * @param targetDir Directory to extract into
     * @return Status indicating success or failure
     */
    static Status UnzipFile(const std::string &zipPath, const std::string &targetDir);

    litebus::AID parentAID_;
    std::unordered_map<std::string, CheckpointFileInfo> checkpointFiles_;
    std::unordered_map<std::string, litebus::Promise<std::string>> pendingDownloads_;  // Track ongoing downloads
    int32_t defaultTTLSeconds_;
    int32_t cleanupIntervalSeconds_;
    litebus::Timer cleanupTimer_;
    std::string checkpointBaseDir_;
    bool cleanupEnabled_;
};

}  // namespace functionsystem::runtime_manager

#endif  // FUNCTIONSYSTEM_SRC_RUNTIME_MANAGER_CKPT_CKPT_FILE_MANAGER_ACTOR_H
