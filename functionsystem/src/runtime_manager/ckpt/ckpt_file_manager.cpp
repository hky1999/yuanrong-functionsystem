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

#include "ckpt_file_manager.h"

#include "async/async.hpp"

namespace functionsystem::runtime_manager {

CkptFileManager::CkptFileManager(const std::shared_ptr<CkptFileManagerActor> &actor) : actor_(actor)
{
}

CkptFileManager::~CkptFileManager()
{
    if (actor_ != nullptr) {
        litebus::Terminate(actor_->GetAID());
    }
}

litebus::Future<std::string> CkptFileManager::DownloadCheckpoint(
    const std::string &checkpointID,
    const std::string &storageUrl,
    const std::string &expectedSha256,
    int64_t expectedSize) const
{
    return litebus::Async(actor_->GetAID(), &CkptFileManagerActor::DownloadCheckpoint,
                          checkpointID, storageUrl, expectedSha256, expectedSize);
}

litebus::Future<CheckpointUploadResult> CkptFileManager::RegisterCheckpoint(
    const std::string &checkpointID,
    const std::string &localPath,
    const std::string &storageUrl,
    int32_t ttl) const
{
    return litebus::Async(actor_->GetAID(), &CkptFileManagerActor::RegisterCheckpoint,
                          checkpointID, localPath, storageUrl, ttl);
}

litebus::Future<Status> CkptFileManager::AddReference(const std::string &checkpointID) const
{
    return litebus::Async(actor_->GetAID(), &CkptFileManagerActor::AddReference, checkpointID);
}

litebus::Future<Status> CkptFileManager::RemoveReference(const std::string &checkpointID) const
{
    return litebus::Async(actor_->GetAID(), &CkptFileManagerActor::RemoveReference, checkpointID);
}

void CkptFileManager::SetDefaultTTL(int32_t ttlSeconds) const
{
    litebus::Async(actor_->GetAID(), &CkptFileManagerActor::SetDefaultTTL, ttlSeconds);
}

void CkptFileManager::StartCleanupTimer() const
{
    litebus::Async(actor_->GetAID(), &CkptFileManagerActor::StartCleanupTimer);
}

void CkptFileManager::StopCleanupTimer() const
{
    litebus::Async(actor_->GetAID(), &CkptFileManagerActor::StopCleanupTimer);
}

litebus::Future<int32_t> CkptFileManager::CleanupExpiredFiles() const
{
    return litebus::Async(actor_->GetAID(), &CkptFileManagerActor::CleanupExpiredFiles);
}

litebus::AID CkptFileManager::GetAID() const
{
    return actor_->GetAID();
}

}  // namespace functionsystem::runtime_manager
