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

#include "ckpt_file_manager_actor.h"

#include <atomic>
#include <filesystem>
#include <fstream>
#include <vector>

#include <openssl/evp.h>

#include "async/asyncafter.hpp"
#include "common/logs/logging.h"
#include "common/utils/files.h"
#include "common/utils/actor_worker.h"
#include "common/file_storage/file_storage_client.h"
#include "common/kv_client/kv_client.h"

namespace functionsystem::runtime_manager {

static const int32_t DEFAULT_TTL_SECONDS = 1800;  // 30 minutes
static const int32_t DEFAULT_CLEANUP_INTERVAL_SECONDS = 300;  // 5 minutes
static constexpr int32_t MILLISECONDS_PER_SECOND = 1000;

// W2 P0.4: flag-driven default TTL (0 = compiled-in default above)
static std::atomic<int32_t> g_defaultTtlOverride{0};

namespace {
// 分块读 4MiB：sha256 与 KV 流式传输共用的内存上界
constexpr std::streamsize DIGEST_CHUNK_SIZE = 4 * 1024 * 1024;
}  // namespace

CkptFileManagerActor::CkptFileManagerActor(const std::string &name)
    : ActorBase(name),
      defaultTTLSeconds_(CkptFileManagerActor::GetDefaultTtlOverride() > 0 ? CkptFileManagerActor::GetDefaultTtlOverride()
                                                                          : DEFAULT_TTL_SECONDS),
      cleanupIntervalSeconds_(DEFAULT_CLEANUP_INTERVAL_SECONDS),
      checkpointBaseDir_(DEFAULT_CHECKPOINT_DIR),
      cleanupEnabled_(true)
{
}

CkptFileManagerActor::CkptFileManagerActor(const std::string &name, const std::string &checkpointDir)
    : ActorBase(name),
      defaultTTLSeconds_(CkptFileManagerActor::GetDefaultTtlOverride() > 0 ? CkptFileManagerActor::GetDefaultTtlOverride()
                                                                          : DEFAULT_TTL_SECONDS),
      cleanupIntervalSeconds_(DEFAULT_CLEANUP_INTERVAL_SECONDS),
      checkpointBaseDir_(checkpointDir),
      cleanupEnabled_(true)
{
}

void CkptFileManagerActor::Init()
{
    YRLOG_INFO("init CkptFileManagerActor {}", GetAID().Name());

    // Create checkpoint base directory if not exists
    std::filesystem::create_directories(checkpointBaseDir_);

    // Restore checkpoint files from local directory
    RestoreCheckpointsFromLocal();

    if (cleanupEnabled_) {
        StartCleanupTimer();
    }
}

void CkptFileManagerActor::Finalize()
{
    YRLOG_INFO("finalize CkptFileManagerActor {}", GetAID().Name());
    StopCleanupTimer();
}

litebus::Future<std::string> CkptFileManagerActor::DownloadCheckpoint(
    const std::string &checkpointID,
    const std::string &storageUrl,
    const std::string &expectedSha256,
    int64_t expectedSize)
{
    YRLOG_INFO("downloading checkpoint: {}, storageUrl: {}", checkpointID, storageUrl);

    // Check if checkpoint already exists locally. The in-memory map alone is not
    // enough: entries restored from a previous run can point at deleted files,
    // so only short-circuit when the artifact is actually on disk.
    auto iter = checkpointFiles_.find(checkpointID);
    if (iter != checkpointFiles_.end()) {
        if (std::filesystem::exists(iter->second.localPath)) {
            YRLOG_INFO("checkpoint {} already exists locally at {}", checkpointID, iter->second.localPath);
            iter->second.lastAccessTime = std::chrono::steady_clock::now();
            return iter->second.localPath;
        }
        // 跨节点/清场后等价场景：map 有记录但工件已不在本地，必须走 KV 重新拉取
        YRLOG_WARN("checkpoint {} is mapped to {} but the file is gone, re-downloading",
                   checkpointID, iter->second.localPath);
    }

    // Check if download is already in progress
    auto pendingIter = pendingDownloads_.find(checkpointID);
    if (pendingIter != pendingDownloads_.end()) {
        YRLOG_INFO("checkpoint {} download already in progress, waiting...", checkpointID);
        return pendingIter->second.GetFuture();
    }

    // Derive parentPath (strip .zip suffix) from storageUrl
    std::string parentPath = storageUrl;
    const std::string zipSuffix = ".zip";
    if (parentPath.size() > zipSuffix.size() &&
        parentPath.compare(parentPath.size() - zipSuffix.size(), zipSuffix.size(), zipSuffix) == 0) {
        parentPath = parentPath.substr(0, parentPath.size() - zipSuffix.size());
    }
    std::string zipLocalPath = checkpointBaseDir_ + "/" + parentPath + ".zip";
    std::string extractedPath = checkpointBaseDir_ + "/" + parentPath;

    // Create promise for this download to coordinate concurrent requests
    litebus::Promise<std::string> downloadPromise;
    pendingDownloads_[checkpointID] = std::move(downloadPromise);
    auto downloadFuture = pendingDownloads_[checkpointID].GetFuture();

    // Create checkpoint info
    CheckpointFileInfo info;
    info.checkpointID = checkpointID;
    info.storageUrl = storageUrl;
    info.localPath = extractedPath;
    info.ttlSeconds = defaultTTLSeconds_;

    // ActorWorker::AsyncWork takes std::function<void()> and the worker's
    // Future<Status> is always OK, so outcome is relayed through shared state.
    auto workStatus = std::make_shared<Status>(Status::OK());
    auto downloadedDigest = std::make_shared<std::string>();
    // Use local AsyncWorker for concurrent downloads
    auto worker = std::make_shared<ActorWorker>();
    worker->AsyncWork([parentPath, zipLocalPath, extractedPath, checkpointID,
                       baseDir = checkpointBaseDir_, expectedSha256, expectedSize,
                       workStatus, downloadedDigest]() {
        // Download the zip file using parentPath as key
        YRLOG_DEBUG("worker thread downloading checkpoint zip: {} with key: {}", zipLocalPath, parentPath);
        file_storage::FileStorageClient client;
        Status status = client.DownloadFile(parentPath, zipLocalPath);
        if (!status.IsOk()) {
            *workStatus = status;
            return;
        }

        // Extract zip to basedir_ first: SnapshotInfo 的 sha/size 描述的是
        // 解压后的 checkpoint.img（sandboxd 校验口径），不是 zip 归档本身
        YRLOG_DEBUG("extracting checkpoint zip {} to {}", zipLocalPath, baseDir);
        status = CkptFileManagerActor::UnzipFile(zipLocalPath, baseDir);

        // Clean up the zip file after extraction
        std::filesystem::remove(zipLocalPath);

        if (!status.IsOk()) {
            *workStatus = status;
            return;
        }

        // 校验解压出的 checkpoint.img 与 SnapshotInfo 一致；expected 缺失
        // （旧元数据）时跳过校验
        std::string sha256Hex;
        int64_t sizeBytes = 0;
        status = ComputeFileSha256(extractedPath + "/" + CHECKPOINT_IMAGE_NAME, sha256Hex, sizeBytes);
        if (!status.IsOk()) {
            *workStatus = status;
            std::filesystem::remove_all(extractedPath);
            return;
        }
        if (!expectedSha256.empty() && sha256Hex != expectedSha256) {
            *workStatus = Status(StatusCode::FAILED, "checkpoint " + checkpointID +
                " sha256 mismatch: expected " + expectedSha256 + ", got " + sha256Hex);
            std::filesystem::remove_all(extractedPath);
            return;
        }
        if (expectedSize > 0 && sizeBytes != expectedSize) {
            *workStatus = Status(StatusCode::FAILED, "checkpoint " + checkpointID +
                " size mismatch: expected " + std::to_string(expectedSize) +
                ", got " + std::to_string(sizeBytes));
            std::filesystem::remove_all(extractedPath);
            return;
        }
        *downloadedDigest = sha256Hex;
    }).OnComplete([worker, checkpointID, info, aid(GetAID()), workStatus, downloadedDigest](
        const litebus::Future<Status> &result) mutable {
        worker->Terminate();
        if (result.IsError() || workStatus->IsError()) {
            auto code = result.IsError() ? result.GetErrorCode() : workStatus->StatusCode();
            YRLOG_ERROR("async download checkpoint {} failed: {}",
                checkpointID, result.IsError() ? "worker actor failed" : workStatus->RawMessage());
            litebus::Async(aid, &CkptFileManagerActor::OnDownloadFailed,
                checkpointID, code);
            return;
        }
        info.sha256 = *downloadedDigest;
        YRLOG_INFO("checkpoint {} downloaded and extracted to {}", checkpointID, info.localPath);
        litebus::Async(aid, &CkptFileManagerActor::OnDownloadSuccess, checkpointID, info);
    });

    return downloadFuture;
}

litebus::Future<Status> CkptFileManagerActor::AddReference(const std::string &checkpointID)
{
    auto iter = checkpointFiles_.find(checkpointID);
    if (iter == checkpointFiles_.end()) {
        YRLOG_ERROR("checkpoint {} not found when adding reference", checkpointID);
        return Status(StatusCode::ERR_CHECKPOINT_NOT_FOUND, "checkpoint not found");
    }

    iter->second.refCount++;
    iter->second.lastAccessTime = std::chrono::steady_clock::now();

    // Stop TTL counting when reference count becomes non-zero
    if (iter->second.refCount == 1 && iter->second.ttlActive) {
        iter->second.ttlActive = false;
        YRLOG_INFO("checkpoint {} reference count increased to {}, TTL stopped",
                   checkpointID, iter->second.refCount);
    } else {
        YRLOG_DEBUG("checkpoint {} reference count increased to {}",
                    checkpointID, iter->second.refCount);
    }

    return Status::OK();
}

litebus::Future<Status> CkptFileManagerActor::RemoveReference(const std::string &checkpointID)
{
    auto iter = checkpointFiles_.find(checkpointID);
    if (iter == checkpointFiles_.end()) {
        YRLOG_WARN("checkpoint {} not found when removing reference", checkpointID);
        return Status::OK();  // Already deleted, not an error
    }

    if (iter->second.refCount > 0) {
        iter->second.refCount--;
        iter->second.lastAccessTime = std::chrono::steady_clock::now();

        // Start TTL counting when reference count reaches 0
        if (iter->second.refCount == 0) {
            iter->second.ttlActive = true;
            iter->second.ttlStartTime = std::chrono::steady_clock::now();
            YRLOG_INFO("checkpoint {} reference count reached 0, starting TTL timer ({} seconds)",
                       checkpointID, iter->second.ttlSeconds);
        } else {
            YRLOG_DEBUG("checkpoint {} reference count decreased to {}",
                        checkpointID, iter->second.refCount);
        }
    } else {
        YRLOG_WARN("checkpoint {} reference count already 0", checkpointID);
    }

    return Status::OK();
}

litebus::Future<CheckpointUploadResult> CkptFileManagerActor::RegisterCheckpoint(
    const std::string &checkpointID,
    const std::string &localPath,
    const std::string & /* storageUrl */,
    int32_t ttl)
{
    // Derive parentPath (directory name) and zip name from localPath
    std::string parentPath = std::filesystem::path(localPath).filename().string();
    std::string derivedStorageUrl = parentPath + ".zip";

    // Use provided TTL if greater than 0, otherwise use default
    int32_t effectiveTTL = (ttl > 0) ? ttl : defaultTTLSeconds_;

    YRLOG_INFO("registering checkpoint: {}, local: {}, storageUrl: {}, ttl: {} seconds",
               checkpointID, localPath, derivedStorageUrl, effectiveTTL);

    // Check if checkpoint already exists
    auto iter = checkpointFiles_.find(checkpointID);
    if (iter != checkpointFiles_.end()) {
        YRLOG_WARN("checkpoint {} already registered", checkpointID);
        litebus::Promise<CheckpointUploadResult> p;
        p.SetFailed(static_cast<int32_t>(StatusCode::ERR_CHECKPOINT_ALREADY_EXISTS));
        return p.GetFuture();
    }

    // Zip the localPath directory and upload the zip to remote storage
    auto worker = std::make_shared<ActorWorker>();
    litebus::Promise<CheckpointUploadResult> uploadPromise;
    auto uploadFuture = uploadPromise.GetFuture();

    // ActorWorker::AsyncWork 丢弃 handler 返回值（其 Future<Status> 恒为 worker
    // 自身的 OK），结果经共享状态带出
    auto result = std::make_shared<CheckpointUploadResult>();
    worker->AsyncWork([localPath, parentPath, derivedStorageUrl, checkpointID, result]() {
        // 与旧路径一致：storageUrl 只由 localPath 推导，不依赖归档/上传成败
        result->storageUrl = derivedStorageUrl;

        // 摘要口径 = sandboxd restore 校验的对象（checkpoint_dir/checkpoint.img），
        // 不是 KV 传输用的 zip 归档；必须先于归档/上传成败独立成立
        const std::string imagePath = localPath + "/" + CHECKPOINT_IMAGE_NAME;
        Status digestStatus = ComputeFileSha256(imagePath, result->sha256, result->size);
        if (!digestStatus.IsOk()) {
            // 无摘要的快照无法通过 identity-checked restore（sandboxd 要求
            // checkpoint_id/sha/size 三元组全非空），静默降级只会造出必死的
            // wake 路径——显式失败，让 park 侧立刻看到
            YRLOG_ERROR("failed to digest checkpoint image {} ({}), failing registration",
                       imagePath, digestStatus.RawMessage());
            result->failed = true;
            result->failMessage = "failed to digest checkpoint image: " + digestStatus.RawMessage();
            return;
        }
        result->createTime = std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

        if (!KVClient::GetInstance().IsInitialized()) {
            // data_system_enable=false：与旧路径一致，只留本地工件（无需归档）
            YRLOG_INFO("kv client not initialized, checkpoint {} stays local-only", checkpointID);
            return;
        }

        // KV 可用才归档上传：zip 只是跨节点传输载体，失败降级为本地保留
        std::string zipPath = localPath + ".zip";
        Status zipStatus = ZipDirectory(localPath, zipPath);
        if (!zipStatus.IsOk()) {
            YRLOG_ERROR("failed to zip checkpoint {} ({}) for KV upload, keeping local-only: {}",
                        checkpointID, localPath, zipStatus.RawMessage());
            return;
        }

        // Upload the zip file with key = parentPath (== checkpointID for sandboxd snapshots)
        YRLOG_DEBUG("worker thread uploading checkpoint zip: {} ({} bytes) with key: {}",
                    zipPath, result->size, parentPath);
        file_storage::FileStorageClient client;
        Status status = client.UploadFile(parentPath, zipPath);

        // Clean up the temporary zip file after upload
        std::filesystem::remove(zipPath);

        if (!status.IsOk()) {
            // 上传失败降级：本地盘保留，单节点语义不回退
            YRLOG_ERROR("failed to upload checkpoint {} ({}), keeping local copy only",
                        checkpointID, status.RawMessage());
            return;
        }
    }).OnComplete([worker, checkpointID, localPath, effectiveTTL, uploadPromise, result, aid(GetAID())](
        const litebus::Future<Status> &asyncResult) mutable {
        worker->Terminate();
        if (asyncResult.IsError()) {
            // Only the async machinery itself can fail here; storage problems
            // were already degraded to ERROR inside the worker.
            YRLOG_ERROR("async upload checkpoint {} failed with error code: {}",
                checkpointID, asyncResult.GetErrorCode());
            uploadPromise.SetFailed(asyncResult.GetErrorCode());
            return;
        }
        if (result->failed) {
            YRLOG_ERROR("registration of checkpoint {} failed: {}",
                checkpointID, result->failMessage);
            uploadPromise.SetFailed(static_cast<int32_t>(StatusCode::FAILED));
            return;
        }
        YRLOG_INFO("checkpoint {} upload phase done, storageUrl: {}, size: {}, sha256: {}",
                   checkpointID, result->storageUrl, result->size, result->sha256);

        // Register checkpoint info after successful upload with TTL
        litebus::Async(aid, &CkptFileManagerActor::OnUploadSuccess,
                       checkpointID, localPath, *result, effectiveTTL, uploadPromise);
    });

    return uploadFuture;
}

void CkptFileManagerActor::OnUploadSuccess(
    const std::string &checkpointID,
    const std::string &localPath,
    const CheckpointUploadResult &result,
    int32_t ttl,
    litebus::Promise<CheckpointUploadResult> uploadPromise)
{
    // Create checkpoint info with initial reference count = 0
    CheckpointFileInfo info;
    info.checkpointID = checkpointID;
    info.localPath = localPath;
    info.storageUrl = result.storageUrl;
    info.sha256 = result.sha256;
    info.size = result.size;
    info.refCount = 0;  // Start with 0, will be incremented when used for restore
    info.ttlSeconds = ttl;  // Set TTL from parameter
    info.ttlActive = true;  // Start TTL immediately since refCount is 0
    info.createdTime = std::chrono::steady_clock::now();
    info.lastAccessTime = info.createdTime;
    info.ttlStartTime = info.createdTime;  // Start TTL timer now

    checkpointFiles_[checkpointID] = info;

    YRLOG_INFO("checkpoint {} registered successfully with initial refCount=0, TTL: {} seconds, storageUrl: {}",
               checkpointID, ttl, result.storageUrl);
    uploadPromise.SetValue(result);
}

void CkptFileManagerActor::SetDefaultTTL(int32_t ttlSeconds)
{
    defaultTTLSeconds_ = ttlSeconds;
    YRLOG_INFO("set default checkpoint TTL to {} seconds", ttlSeconds);
}

void CkptFileManagerActor::SetDefaultTtlOverride(int32_t ttlSeconds)
{
    g_defaultTtlOverride.store(ttlSeconds, std::memory_order_relaxed);
}

int32_t CkptFileManagerActor::GetDefaultTtlOverride()
{
    return g_defaultTtlOverride.load(std::memory_order_relaxed);
}

void CkptFileManagerActor::StartCleanupTimer()
{
    if (!cleanupEnabled_) {
        return;
    }

    YRLOG_INFO("starting checkpoint cleanup timer (interval: {} seconds)", cleanupIntervalSeconds_);
    cleanupTimer_ = litebus::AsyncAfter(cleanupIntervalSeconds_ * MILLISECONDS_PER_SECOND, GetAID(),
                                        &CkptFileManagerActor::PeriodicCleanup);
}

void CkptFileManagerActor::StopCleanupTimer()
{
    litebus::TimerTools::Cancel(cleanupTimer_);
    YRLOG_INFO("checkpoint cleanup timer stopped");
}

void CkptFileManagerActor::PeriodicCleanup()
{
    YRLOG_DEBUG("performing periodic checkpoint cleanup");

    CleanupExpiredFiles().Then([aid(GetAID())](const litebus::Future<int32_t> &result) -> Status {
        if (result.IsError()) {
            YRLOG_ERROR("cleanup failed with error code: {}", result.GetErrorCode());
        } else {
            int32_t deletedCount = result.Get();
            if (deletedCount > 0) {
                YRLOG_INFO("cleaned up {} expired checkpoint files", deletedCount);
            }
        }
        return Status::OK();
    });

    // Schedule next cleanup
    if (cleanupEnabled_) {
        cleanupTimer_ = litebus::AsyncAfter(cleanupIntervalSeconds_ * MILLISECONDS_PER_SECOND, GetAID(),
                                            &CkptFileManagerActor::PeriodicCleanup);
    }
}

litebus::Future<int32_t> CkptFileManagerActor::CleanupExpiredFiles()
{
    int32_t deletedCount = 0;
    std::vector<std::string> toDelete;

    // Find expired checkpoints
    for (const auto &entry : checkpointFiles_) {
        const auto &info = entry.second;
        if (IsExpired(info)) {
            toDelete.push_back(entry.first);
        }
    }

    // Delete expired checkpoints
    for (const auto &checkpointID : toDelete) {
        Status status = DeleteCheckpointFile(checkpointID);
        if (status.IsOk()) {
            deletedCount++;
        }
    }

    return deletedCount;
}

bool CkptFileManagerActor::IsExpired(const CheckpointFileInfo &info) const
{
    // Only check expiration if:
    // 1. Reference count is 0
    // 2. TTL is active
    if (!info.ttlActive || info.refCount > 0) {
        return false;
    }

    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - info.ttlStartTime).count();

    return elapsed >= info.ttlSeconds;
}

Status CkptFileManagerActor::DeleteCheckpointFile(const std::string &checkpointID)
{
    auto iter = checkpointFiles_.find(checkpointID);
    if (iter == checkpointFiles_.end()) {
        return Status(StatusCode::ERR_CHECKPOINT_NOT_FOUND, "checkpoint not found");
    }

    const std::string &localPath = iter->second.localPath;

    try {
        // Delete file or directory
        if (std::filesystem::exists(localPath)) {
            if (std::filesystem::is_directory(localPath)) {
                std::filesystem::remove_all(localPath);
            } else {
                std::filesystem::remove(localPath);
            }
            YRLOG_INFO("deleted checkpoint file: {}", localPath);
        } else {
            YRLOG_WARN("checkpoint file not found: {}", localPath);
        }

        // Remove from map
        checkpointFiles_.erase(iter);
        return Status::OK();
    } catch (const std::filesystem::filesystem_error &e) {
        YRLOG_ERROR("failed to delete checkpoint {}: {}", checkpointID, e.what());
        return Status(StatusCode::ERR_FILE_OPERATION_FAILED, e.what());
    }
}

std::string CkptFileManagerActor::GetLocalPath(const std::string &checkpointID) const
{
    return checkpointBaseDir_ + "/" + checkpointID;
}

void CkptFileManagerActor::RestoreCheckpointsFromLocal()
{
    // todo(lwy): 考虑filesystem的兼容性问题
    try {
        if (!std::filesystem::exists(checkpointBaseDir_)) {
            YRLOG_INFO("checkpoint base directory does not exist, skipping restore");
            return;
        }

        int32_t restoredCount = 0;
        for (const auto &entry : std::filesystem::directory_iterator(checkpointBaseDir_)) {
            if (!entry.is_directory() && !entry.is_regular_file()) {
                continue;
            }

            // Extract checkpoint ID from filename/dirname
            std::string checkpointID = entry.path().filename().string();
            std::string localPath = entry.path().string();

            // Skip hidden files and system files
            if (checkpointID.empty() || checkpointID[0] == '.') {
                continue;
            }

            // Create checkpoint info with default settings
            CheckpointFileInfo info;
            info.checkpointID = checkpointID;
            info.localPath = localPath;
            info.storageUrl = "";  // Unknown after restart
            info.refCount = 0;  // Start with 0 references
            info.ttlActive = true;  // Start TTL immediately
            info.ttlStartTime = std::chrono::steady_clock::now();
            info.ttlSeconds = defaultTTLSeconds_;
            info.createdTime = std::chrono::steady_clock::now();
            info.lastAccessTime = std::chrono::steady_clock::now();

            checkpointFiles_[checkpointID] = info;
            restoredCount++;
            YRLOG_DEBUG("restored checkpoint: {}, path: {}", checkpointID, localPath);
        }

        if (restoredCount > 0) {
            YRLOG_INFO("restored {} checkpoint files from local directory: {}",
                       restoredCount, checkpointBaseDir_);
        } else {
            YRLOG_INFO("no checkpoint files found in local directory: {}", checkpointBaseDir_);
        }
    } catch (const std::filesystem::filesystem_error &e) {
        YRLOG_ERROR("failed to restore checkpoints from local directory: {}", e.what());
    }
}

void CkptFileManagerActor::OnDownloadSuccess(const std::string &checkpointID,
                                             const CheckpointFileInfo &info)
{
    // Store checkpoint info
    checkpointFiles_[checkpointID] = info;

    // Notify all waiting requests about success
    auto promiseIter = pendingDownloads_.find(checkpointID);
    if (promiseIter != pendingDownloads_.end()) {
        promiseIter->second.SetValue(info.localPath);
        pendingDownloads_.erase(promiseIter);
    }
}

void CkptFileManagerActor::OnDownloadFailed(const std::string &checkpointID, int32_t errorCode)
{
    // Notify all waiting requests about failure
    auto promiseIter = pendingDownloads_.find(checkpointID);
    if (promiseIter != pendingDownloads_.end()) {
        promiseIter->second.SetFailed(errorCode);
        pendingDownloads_.erase(promiseIter);
    }
}

Status CkptFileManagerActor::ZipDirectory(const std::string &dirPath, const std::string &zipPath)
{
    std::filesystem::path dir(dirPath);
    if (!std::filesystem::exists(dir) || !std::filesystem::is_directory(dir)) {
        return Status(StatusCode::FAILED, "directory does not exist: " + dirPath);
    }

    std::string parentDir = dir.parent_path().string();
    std::string dirName = dir.filename().string();

    // zip -r <zipPath> <dirName> executed from parentDir
    std::string cmd = "cd " + parentDir + " && zip -r " + zipPath + " " + dirName;
    YRLOG_DEBUG("zip command: {}", cmd);

    int ret = std::system(cmd.c_str());
    if (ret != 0) {
        return Status(StatusCode::FAILED,
                      "zip command failed with exit code " + std::to_string(ret));
    }

    return Status::OK();
}

Status CkptFileManagerActor::ComputeFileSha256(const std::string &filePath, std::string &sha256Hex,
                                               int64_t &sizeBytes)
{
    sha256Hex.clear();
    sizeBytes = 0;

    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        return Status(StatusCode::FAILED, "failed to open file: " + filePath);
    }

    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    if (ctx == nullptr) {
        return Status(StatusCode::FAILED, "failed to create EVP digest context for: " + filePath);
    }
    if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
        EVP_MD_CTX_free(ctx);
        return Status(StatusCode::FAILED, "failed to init sha256 digest for: " + filePath);
    }

    std::vector<char> chunk(DIGEST_CHUNK_SIZE);
    int64_t total = 0;
    for (;;) {
        file.read(chunk.data(), DIGEST_CHUNK_SIZE);
        const std::streamsize got = file.gcount();
        if (got > 0) {
            if (EVP_DigestUpdate(ctx, chunk.data(), static_cast<size_t>(got)) != 1) {
                EVP_MD_CTX_free(ctx);
                return Status(StatusCode::FAILED, "sha256 update failed for: " + filePath);
            }
            total += got;
        }
        if (got < DIGEST_CHUNK_SIZE) {
            break;
        }
    }
    if (file.bad()) {
        EVP_MD_CTX_free(ctx);
        return Status(StatusCode::FAILED, "io error while reading: " + filePath);
    }

    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int mdLen = 0;
    if (EVP_DigestFinal_ex(ctx, md, &mdLen) != 1) {
        EVP_MD_CTX_free(ctx);
        return Status(StatusCode::FAILED, "sha256 finalize failed for: " + filePath);
    }
    EVP_MD_CTX_free(ctx);

    static constexpr char hexDigits[] = "0123456789abcdef";
    sha256Hex.reserve(mdLen * 2);
    for (unsigned int i = 0; i < mdLen; i++) {
        sha256Hex.push_back(hexDigits[md[i] >> 4]);
        sha256Hex.push_back(hexDigits[md[i] & 0xf]);
    }
    sizeBytes = total;
    return Status::OK();
}

Status CkptFileManagerActor::UnzipFile(const std::string &zipPath, const std::string &targetDir)
{
    if (!std::filesystem::exists(zipPath)) {
        return Status(StatusCode::FAILED, "zip file does not exist: " + zipPath);
    }

    std::filesystem::create_directories(targetDir);

    // unzip -o <zipPath> -d <targetDir>
    std::string cmd = "unzip -o " + zipPath + " -d " + targetDir;
    YRLOG_DEBUG("unzip command: {}", cmd);

    int ret = std::system(cmd.c_str());
    if (ret != 0) {
        return Status(StatusCode::FAILED,
                      "unzip command failed with exit code " + std::to_string(ret));
    }

    return Status::OK();
}

}  // namespace functionsystem::runtime_manager
