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

#include "file_storage_client.h"

#include <filesystem>
#include <fstream>
#include <sstream>

#include "common/kv_client/kv_client.h"
#include "common/logs/logging.h"
#include "datasystem/datasystem.h"

namespace functionsystem::file_storage {

namespace {
constexpr int64_t FILE_TRANSFER_CHUNK_SIZE = 4 * 1024 * 1024;  // 4MiB
}

Status FileStorageClient::UploadFile(const std::string &key, const std::string &filePath)
{
    YRLOG_INFO("uploading file: {} with key: {}", filePath, key);

    std::error_code ec;
    const auto size = static_cast<uint64_t>(std::filesystem::file_size(filePath, ec));
    if (ec) {
        YRLOG_ERROR("failed to stat file for upload: {}, error: {}", filePath, ec.message());
        return Status(StatusCode::FAILED, "failed to stat file: " + filePath);
    }

    // 流式上传：旧实现整文件读进内存，600MB+ ckpt 会整份常驻堆上
    Status status = KVClient::GetInstance().PutFile(key, filePath, size);
    if (!status.IsOk()) {
        YRLOG_ERROR("failed to upload file to KV storage with key: {}, error: {}", key, status.GetMessage());
        return status;
    }

    YRLOG_INFO("successfully uploaded file: {} with key: {}, size: {} bytes", filePath, key, size);
    return Status::OK();
}

Status FileStorageClient::DownloadFile(const std::string &key, const std::string &filePath)
{
    YRLOG_INFO("downloading file with key: {} to: {}", key, filePath);

    // Download from KV storage
    auto [status, buffer] = KVClient::GetInstance().Get(key);
    if (!status.IsOk()) {
        YRLOG_ERROR("failed to download file from KV storage with key: {}, error: {}", key, status.GetMessage());
        return status;
    }

    // Write to file, chunked: avoid materializing a second full-size std::string copy of the buffer
    const auto *data = static_cast<const char *>(buffer.ImmutableData());
    const int64_t totalSize = buffer.GetSize();
    std::ofstream file(filePath, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        YRLOG_ERROR("failed to open file for writing: {}", filePath);
        return Status(StatusCode::FAILED, "failed to open file for writing: " + filePath);
    }
    for (int64_t offset = 0; offset < totalSize; offset += FILE_TRANSFER_CHUNK_SIZE) {
        const auto chunkSize =
            static_cast<std::streamsize>(std::min<int64_t>(FILE_TRANSFER_CHUNK_SIZE, totalSize - offset));
        if (!file.write(data + offset, chunkSize)) {
            file.close();
            YRLOG_ERROR("failed to write downloaded content to: {}", filePath);
            return Status(StatusCode::FAILED, "failed to write content to file: " + filePath);
        }
    }
    file.close();

    YRLOG_INFO("successfully downloaded file with key: {} to: {}, size: {} bytes", key, filePath, buffer.GetSize());
    return Status::OK();
}

Status FileStorageClient::DeleteFile(const std::string &key)
{
    YRLOG_INFO("deleting file with key: {}", key);

    Status status = KVClient::GetInstance().Delete(key);
    if (!status.IsOk()) {
        YRLOG_ERROR("failed to delete file from KV storage with key: {}, error: {}", key, status.GetMessage());
        return status;
    }

    YRLOG_INFO("successfully deleted file with key: {}", key);
    return Status::OK();
}

}  // namespace functionsystem::file_storage
