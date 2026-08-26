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

#pragma once

#include "common/status/status.h"
#include "common/utils/singleton.h"
#include "datasystem/datasystem.h"
#include "function_agent/flags/function_agent_flags.h"

namespace functionsystem {

class KVClient : public Singleton<KVClient> {
public:
    virtual ~KVClient() = default;
    Status Init(const std::string &host, int32_t port);
    std::pair<Status, datasystem::ReadOnlyBuffer> Get(const std::string &key);
    Status Put(const std::string &key, const std::string &value);
    // 流式上传：Buffer Create + 分块写入 + Set，避免大文件整体读进内存
    Status PutFile(const std::string &key, const std::string &filePath, uint64_t size);
    Status Delete(const std::string &key);

    // data_system_enable=false 时未 Init，调用方据此跳过远端上传/下载
    bool IsInitialized() const
    {
        return dsKvClient_ != nullptr;
    }

private:
    std::unique_ptr<datasystem::KVClient> dsKvClient_;
};
}  // namespace functionsystem
