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

#include "common/kv_client/kv_client.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <vector>

namespace functionsystem {

Status KVClient::Init(const std::string &host, int32_t port)
{
    YRLOG_INFO("initializing kv client with host: {}, port: {}", host, port);
    datasystem::ConnectOptions connectOptions;
    connectOptions.host = host;
    connectOptions.port = port;
    dsKvClient_ = std::make_unique<datasystem::KVClient>(connectOptions);
    ::datasystem::Status s = dsKvClient_->Init();
    if (s.IsError()) {
        YRLOG_ERROR("failed to initialize kv client, host: {}, port: {}, error: {}", host, port, s.ToString());
        return Status(StatusCode::BP_DATASYSTEM_ERROR, s.ToString());
    }
    YRLOG_INFO("kv client initialized successfully with host: {}, port: {}", host, port);
    return Status::OK();
}
std::pair<Status, datasystem::ReadOnlyBuffer> KVClient::Get(const std::string &key)
{
    if (dsKvClient_ == nullptr) {
        YRLOG_ERROR("kv client is not initialized");
        return std::make_pair(Status(StatusCode::BP_DATASYSTEM_ERROR, "kv client is not initialized"),
                              datasystem::ReadOnlyBuffer());
    }

    datasystem::Optional<datasystem::ReadOnlyBuffer> buffer;
    datasystem::Status s = dsKvClient_->Get(key, buffer);
    if (s.IsError()) {
        return std::make_pair(Status(StatusCode::BP_DATASYSTEM_ERROR, s.ToString()), datasystem::ReadOnlyBuffer());
    }
    return std::make_pair(Status::OK(), *buffer);
}

Status KVClient::Put(const std::string &key, const std::string &value)
{
    if (dsKvClient_ == nullptr) {
        YRLOG_ERROR("kv client is not initialized");
        return Status(StatusCode::BP_DATASYSTEM_ERROR, "kv client is not initialized");
    }

    datasystem::Status s = dsKvClient_->Set(key, value);
    if (s.IsError()) {
        YRLOG_ERROR("failed to put key: {}, error: {}", key, s.ToString());
        return Status(StatusCode::BP_DATASYSTEM_ERROR, s.ToString());
    }
    YRLOG_DEBUG("successfully put key: {}", key);
    return Status::OK();
}

Status KVClient::Delete(const std::string &key)
{
    if (dsKvClient_ == nullptr) {
        YRLOG_ERROR("kv client is not initialized");
        return Status(StatusCode::BP_DATASYSTEM_ERROR, "kv client is not initialized");
    }

    datasystem::Status s = dsKvClient_->Del(key);
    if (s.IsError()) {
        YRLOG_ERROR("failed to delete key: {}, error: {}", key, s.ToString());
        return Status(StatusCode::BP_DATASYSTEM_ERROR, s.ToString());
    }
    YRLOG_DEBUG("successfully deleted key: {}", key);
    return Status::OK();
}

namespace {
constexpr uint64_t FILE_TRANSFER_CHUNK_SIZE = 4 * 1024 * 1024;  // 4MiB
}

Status KVClient::PutFile(const std::string &key, const std::string &filePath, uint64_t size)
{
    if (dsKvClient_ == nullptr) {
        YRLOG_ERROR("kv client is not initialized");
        return Status(StatusCode::BP_DATASYSTEM_ERROR, "kv client is not initialized");
    }
    if (size == 0) {
        return Status(StatusCode::BP_DATASYSTEM_ERROR, "refuse to upload empty file: " + filePath);
    }

    // Buffer 路径：Create 拿共享内存缓冲后分块灌入，再 Set 发布，全程不持有整文件堆副本
    std::shared_ptr<datasystem::Buffer> buffer;
    datasystem::Status s = dsKvClient_->Create(key, size, datasystem::SetParam {}, buffer);
    if (s.IsError()) {
        YRLOG_ERROR("failed to create buffer for key: {}, size: {}, error: {}", key, size, s.ToString());
        return Status(StatusCode::BP_DATASYSTEM_ERROR, s.ToString());
    }

    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        YRLOG_ERROR("failed to open file for streaming upload: {}", filePath);
        return Status(StatusCode::BP_DATASYSTEM_ERROR, "failed to open file: " + filePath);
    }

    auto *dst = static_cast<uint8_t *>(buffer->MutableData());
    std::vector<char> chunk(FILE_TRANSFER_CHUNK_SIZE);
    uint64_t copied = 0;
    while (copied < size) {
        const uint64_t bytes = std::min<uint64_t>(FILE_TRANSFER_CHUNK_SIZE, size - copied);
        file.read(chunk.data(), static_cast<std::streamsize>(bytes));
        if (static_cast<uint64_t>(file.gcount()) != bytes) {
            YRLOG_ERROR("short read while uploading {}: expected {}, got {}", filePath, bytes, file.gcount());
            return Status(StatusCode::BP_DATASYSTEM_ERROR, "short read on file: " + filePath);
        }
        std::memcpy(dst + copied, chunk.data(), bytes);
        copied += bytes;
    }

    s = dsKvClient_->Set(buffer);
    if (s.IsError()) {
        YRLOG_ERROR("failed to publish buffer for key: {}, error: {}", key, s.ToString());
        return Status(StatusCode::BP_DATASYSTEM_ERROR, s.ToString());
    }
    YRLOG_DEBUG("successfully streamed file {} ({} bytes) to key: {}", filePath, size, key);
    return Status::OK();
}
}  // namespace functionsystem
