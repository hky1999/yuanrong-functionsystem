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

#ifndef FUNCTIONSYSTEM_SRC_RUNTIME_MANAGER_CKPT_CKPT_FILE_MANAGER_H
#define FUNCTIONSYSTEM_SRC_RUNTIME_MANAGER_CKPT_CKPT_FILE_MANAGER_H

#include "ckpt_file_manager_actor.h"

#include <actor/actor.hpp>
#include <memory>

namespace functionsystem::runtime_manager {

/**
 * CkptFileManager: Wrapper class for CkptFileManagerActor
 * Provides synchronous-style API for checkpoint file management
 */
class CkptFileManager {
public:
    explicit CkptFileManager(const std::shared_ptr<CkptFileManagerActor> &actor);

    ~CkptFileManager();

    /**
     * Download checkpoint file from remote storage
     * @param checkpointID Unique checkpoint identifier
     * @param storageUrl Remote storage URL (used as storage key)
     * @param expectedSha256 sha256 from SnapshotInfo; verified when non-empty
     * @param expectedSize archive size from SnapshotInfo; verified when > 0
     * @return Future with local file path
     */
    litebus::Future<std::string> DownloadCheckpoint(const std::string &checkpointID,
                                                     const std::string &storageUrl,
                                                     const std::string &expectedSha256 = "",
                                                     int64_t expectedSize = 0) const;

    /**
     * Register a locally created checkpoint (zips, uploads to storage, and registers)
     * @param checkpointID Unique checkpoint identifier
     * @param localPath Local checkpoint directory path (may contain multiple files)
     * @param storageUrl Ignored (storageUrl is derived from localPath as parentPath.zip)
     * @param ttl Time to live in seconds (0 means no expiration)
     * @return Future with CheckpointUploadResult (storageUrl + sha256/size/createTime)
     */
    litebus::Future<CheckpointUploadResult> RegisterCheckpoint(const std::string &checkpointID,
                                                    const std::string &localPath,
                                                    const std::string &storageUrl,
                                                    int32_t ttl = 0) const;
    /**
     * Increment reference count for a checkpoint file
     * @param checkpointID Checkpoint identifier
     * @return Future with status
     */
    litebus::Future<Status> AddReference(const std::string &checkpointID) const;

    /**
     * Decrement reference count for a checkpoint file
     * @param checkpointID Checkpoint identifier
     * @return Future with status
     */
    litebus::Future<Status> RemoveReference(const std::string &checkpointID) const;

    /**
     * Set default TTL for checkpoint files
     * @param ttlSeconds Time to live in seconds
     */
    void SetDefaultTTL(int32_t ttlSeconds) const;

    /**
     * Start automatic cleanup timer
     */
    void StartCleanupTimer() const;

    /**
     * Stop automatic cleanup timer
     */
    void StopCleanupTimer() const;

    /**
     * Manually trigger cleanup of expired files
     * @return Future with number of deleted files
     */
    litebus::Future<int32_t> CleanupExpiredFiles() const;

    /**
     * Get the AID of the underlying actor
     * @return Actor ID
     */
    litebus::AID GetAID() const;

private:
    std::shared_ptr<CkptFileManagerActor> actor_;
};

}  // namespace functionsystem::runtime_manager

#endif  // FUNCTIONSYSTEM_SRC_RUNTIME_MANAGER_CKPT_CKPT_FILE_MANAGER_H
