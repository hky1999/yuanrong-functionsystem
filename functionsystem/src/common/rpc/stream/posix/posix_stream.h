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

#ifndef COMMON_RPC_STREAM_POSIX_POSIX_STREAM_H
#define COMMON_RPC_STREAM_POSIX_POSIX_STREAM_H

#include "common/proto/pb/posix/runtime_rpc.grpc.pb.h"
#include "common/rpc/stream/posix/posix_client.h"
#include "common/rpc/stream/posix_reactor.h"

namespace functionsystem::grpc {
class PosixStream : public PosixClient, public std::enable_shared_from_this<PosixStream> {
public:
    using ServerReactor =
        PosixReactor<ReactorType::SERVER, runtime_rpc::StreamingMessage, runtime_rpc::StreamingMessage>;

    PosixStream(const std::shared_ptr<ServerReactor> &reactor, ::grpc::CallbackServerContext *context,
                const std::string &instanceID, const std::string &runtimeID,
                const std::string &boundRequestID = "");
    ~PosixStream() override;

    void Start() override;
    void Stop() override;
    bool IsDone() override;
    void PosixStreamClosedCallback();
    void Receiver(const std::shared_ptr<runtime_rpc::StreamingMessage> &recv);
    bool HandlerResponse(const std::shared_ptr<runtime_rpc::StreamingMessage> &recv, const std::string &recvMsgID,
                         bool notHeartbeat);

    litebus::Future<runtime_rpc::StreamingMessage> Send(
        const std::shared_ptr<runtime_rpc::StreamingMessage> &request) override;

private:
    std::shared_ptr<ServerReactor> reactor_;
    ::grpc::CallbackServerContext *context_{ nullptr };
    std::mutex msgMutex_;
    std::unordered_map<std::string, std::shared_ptr<litebus::Promise<runtime_rpc::StreamingMessage>>> msgPromises_;
    std::shared_ptr<litebus::Promise<bool>> isConnected_;
    std::string runtimeID_;
    std::string instanceID_;
    std::string boundRequestID_;
    std::mutex mut_;
    std::atomic<bool> isStarted_;
    bool isStopped_{ false };
};

}  // namespace functionsystem::grpc

#endif  // COMMON_RPC_STREAM_POSIX_POSIX_STREAM_H
