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

#ifndef FUNCTION_PROXY_COMMON_POSIX_SERVICE_POSIX_SERVICE_H
#define FUNCTION_PROXY_COMMON_POSIX_SERVICE_POSIX_SERVICE_H

#include <async/future.hpp>

#include "function_proxy/common/iam/internal_iam.h"
#include "common/rpc/stream/posix/posix_stream.h"

namespace functionsystem {

using UpdatePosixClientCallback = std::function<void(const std::string &instanceID, const std::string &runtimeID,
                                                     const std::shared_ptr<grpc::PosixClient> &posixClient)>;
struct PosixMetaData {
    std::string instanceID;
    std::string runtimeID;
    std::string sourceID;
    std::string destinationID;
    std::string streamRole;
    std::string token;
    std::string accessKey;
    std::string timestamp;
    std::string signature;
};

class PosixService : public runtime_rpc::RuntimeRPC::CallbackService {
public:
    PosixService() = default;
    ~PosixService() override = default;

    ::grpc::ServerBidiReactor<runtime_rpc::StreamingMessage, runtime_rpc::StreamingMessage> *MessageStream(
        ::grpc::CallbackServerContext *context) override;

    void BindInternalIAM(const std::shared_ptr<function_proxy::InternalIAM> &internalIam)
    {
        internalIam_ = internalIam;
    }

    void RegisterUpdatePosixClientCallback(const UpdatePosixClientCallback &cb)
    {
        updatePosixClientCallback_ = cb;
    }

    static bool CheckClientIsReady(const std::string &instanceID);
    static void DeleteClient(const std::string &instanceID);
    static void UpdateClient(const std::string &instanceID, const std::shared_ptr<grpc::PosixClient> &client);

private:
    std::shared_ptr<function_proxy::InternalIAM> internalIam_;
    // Callback should not cost much time
    UpdatePosixClientCallback updatePosixClientCallback_;

    inline static std::mutex mutex_;
    inline static std::unordered_map<std::string, std::shared_ptr<grpc::PosixClient>> clients_;
    inline static std::unordered_map<std::string, std::shared_ptr<grpc::PosixClient>> eventClients_;
    PosixMetaData GetMetaData(const ::grpc::CallbackServerContext *context) const;
};

} // namespace functionsystem
#endif  // FUNCTION_PROXY_COMMON_POSIX_SERVICE_POSIX_SERVICE_H
