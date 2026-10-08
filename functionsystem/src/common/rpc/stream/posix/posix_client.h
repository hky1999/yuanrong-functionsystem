/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
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

#ifndef COMMON_RPC_STREAM_POSIX_POSIX_CLIENT_H
#define COMMON_RPC_STREAM_POSIX_POSIX_CLIENT_H

#include <functional>

#include "async/future.hpp"
#include "common/proto/pb/posix_pb.h"
#include "common/rpc/stream/posix/auth_interceptor.h"

namespace functionsystem::grpc {

// Immutable identity of the posix stream a control message arrived on,
// bound when the stream was accepted/created — NEVER re-resolved per
// message. `boundRequestID` is the CREATION request generation the proxy
// bound at accept time; empty means "unbound" (mismatch or unknown), which
// state operations must treat as an explicit refusal, never a bare-key
// fallback.
struct PosixStreamIdentity {
    std::string instanceID;
    std::string runtimeID;
    std::string boundRequestID;
};

using PosixFunctionSysControlHandler = std::function<litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>>(
    const PosixStreamIdentity &, const std::shared_ptr<runtime_rpc::StreamingMessage> &)>;
using PosixFunctionSysControlHandlers =
    std::unordered_map<runtime_rpc::StreamingMessage::BodyCase, PosixFunctionSysControlHandler>;
using StreamingMessageAuthInterceptor = std::shared_ptr<AuthInterceptor<runtime_rpc::StreamingMessage>>;

class PosixClient {
public:
    PosixClient() = default;
    virtual ~PosixClient() = default;
    virtual void Start() = 0;
    virtual void Stop() = 0;
    virtual bool IsDone() = 0;
    virtual litebus::Future<runtime_rpc::StreamingMessage> Send(
        const std::shared_ptr<runtime_rpc::StreamingMessage> &request) = 0;

    // if Stop is called, registered call back would not be called
    void RegisterUserCallback(const std::function<void()> &userCallback)
    {
        userCallback_ = userCallback;
    }

    void SetAuthInterceptor(const StreamingMessageAuthInterceptor &interceptor)
    {
        if (interceptor != nullptr) {
            interceptor_ = interceptor;
        }
    }

    inline static void RegisterPosixHandler(runtime_rpc::StreamingMessage::BodyCase type,
                                     const PosixFunctionSysControlHandler &func)
    {
        handlers_[type] = func;
    }

protected:
    StreamingMessageAuthInterceptor interceptor_;
    std::function<void()> userCallback_;
    inline static PosixFunctionSysControlHandlers handlers_;
};

class RegisterFunctionSystemControlHandler {
public:
    RegisterFunctionSystemControlHandler(runtime_rpc::StreamingMessage::BodyCase type,
                                         const PosixFunctionSysControlHandler &func) noexcept
    {
        PosixClient::RegisterPosixHandler(type, func);
    }
    ~RegisterFunctionSystemControlHandler() = default;
};

#define REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(type, func) \
    static functionsystem::grpc::RegisterFunctionSystemControlHandler(REGISTER_VAR)(type, func)

} // namespace functionsystem::grpc

#endif  // COMMON_RPC_STREAM_POSIX_POSIX_CLIENT_H
