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

#ifndef COMMON_RPC_STREAM_POSIX_CONTROL_CLIENT_H
#define COMMON_RPC_STREAM_POSIX_CONTROL_CLIENT_H

#include <grpcpp/security/credentials.h>

#include <async/future.hpp>
#include <functional>
#include <mutex>

#include "common/proto/pb/posix/runtime_rpc.grpc.pb.h"
#include "common/rpc/stream/posix_reactor.h"
#include "posix_client.h"

namespace functionsystem::grpc {

const int32_t DEFAULT_MAX_GRPC_SIZE = 11;  // MB
const int64_t DEFAULT_TIMEOUT_SEC = 30;   // s

struct ControlClientConfig {
    std::string target;
    std::shared_ptr<::grpc::ChannelCredentials> creds;
    int64_t timeoutSec = DEFAULT_TIMEOUT_SEC;
    int32_t maxGrpcSize = DEFAULT_MAX_GRPC_SIZE;
    // Creation request generation for this proxy-initiated stream, set
    // BEFORE construction (immutable afterwards).
    std::string boundRequestID;
};

class ControlClient : public PosixClient {
public:
    ControlClient() = default;
    ControlClient(const std::string &instanceID, const std::string &runtimeID, const ControlClientConfig &config);
    ~ControlClient() override
    {
        if (reactor_ != nullptr && !reactor_->IsDone() && isRunning_) {
            reactor_->TryStop(context_);
        }

        reactor_ = nullptr;
        stub_ = nullptr;
    };
    void Start() override;
    void Stop() override;
    bool IsDone() override;
    void Receiver(const std::shared_ptr<runtime_rpc::StreamingMessage> &recv);
    void ClientClosedCallback();
    litebus::Future<runtime_rpc::StreamingMessage> Send(
        const std::shared_ptr<runtime_rpc::StreamingMessage> &request) override;

private:
    using ClientReactor =
        PosixReactor<ReactorType::CLIENT, runtime_rpc::StreamingMessage, runtime_rpc::StreamingMessage>;
    std::string instanceID_;
    std::string runtimeID_;
    std::string boundRequestID_;
    std::unique_ptr<runtime_rpc::RuntimeRPC::Stub> stub_ = nullptr;
    ::grpc::ClientContext context_;
    std::shared_ptr<ClientReactor> reactor_ = nullptr;
    std::mutex mut_;
    bool isStopped_ = false;
    bool isRunning_ = false;
    std::unordered_map<std::string, std::shared_ptr<litebus::Promise<runtime_rpc::StreamingMessage>>> promises_;
};

class PosixControlWrapper {
public:
    PosixControlWrapper() = default;
    virtual ~PosixControlWrapper() = default;
    virtual std::shared_ptr<ControlClient> InitPosixStream(const std::string &instanceID, const std::string &runtimeID,
                                                           const ControlClientConfig &config);
};
}  // namespace functionsystem::grpc

#endif  // COMMON_RPC_STREAM_POSIX_CONTROL_CLIENT_H
