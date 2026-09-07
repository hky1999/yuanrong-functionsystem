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

#ifndef FUNCTION_PROXY_COMMON_POSIX_CLIENT_BASE_CLIENT_H
#define FUNCTION_PROXY_COMMON_POSIX_CLIENT_BASE_CLIENT_H

#include <shared_mutex>
#include "common/rpc/stream/posix/posix_client.h"

namespace functionsystem {
class BaseClient {
public:
    // runtimeID labels posix_ with the runtime it was opened for; it is a
    // send-time transport fence only — NOT a sandbox id / resource generation
    // binding (the daemon re-checks those) and NOT a connection lease. Set in
    // the constructor, before any publication, so the binding exists as soon
    // as the client is observable.
    explicit BaseClient(const std::shared_ptr<grpc::PosixClient> &posix, const std::string &runtimeID = "")
        : posix_(posix), runtimeID_(runtimeID)
    {
    }
    virtual ~BaseClient() = default;
    void Start();
    void Close() noexcept;
    virtual bool IsDone();
    void RegisterUserCallback(const std::function<void()> &userCb);
    // The legacy one-argument form (runtimeID defaults to empty) invalidates
    // the binding: an untagged transport may never satisfy a runtime-pinned
    // request.
    void UpdatePosix(const std::shared_ptr<grpc::PosixClient> &posix, const std::string &runtimeID = "");

    virtual litebus::Future<runtime::CallResponse> InitCall(const std::shared_ptr<runtime::CallRequest> &request,
                                                            uint32_t timeOutMs);

    virtual litebus::Future<SharedStreamMsg> Call(const SharedStreamMsg &request);

    virtual litebus::Future<runtime::NotifyResponse> NotifyResult(runtime::NotifyRequest &&request);

    virtual litebus::Future<runtime_rpc::StreamingMessage> Send(
        const std::shared_ptr<runtime_rpc::StreamingMessage> &request, uint32_t retryTimes, uint32_t timeOutMs);

protected:
    std::shared_ptr<grpc::PosixClient> posix_;
    std::function<void()> userCb_;
    // runtime identity posix_ is currently bound to; guarded by rwMut_
    // together with posix_ so a runtime-pinned caller can validate the
    // binding and dispatch under one shared lock.
    std::string runtimeID_;
    std::shared_mutex rwMut_;
};
}  // namespace functionsystem

#endif  // FUNCTION_PROXY_COMMON_POSIX_CLIENT_BASE_CLIENT_H
