/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef FUNCTION_PROXY_LOCAL_SCHEDULER_GRPC_SERVER_FRONTEND_PROXY_SERVICE_FRONTEND_PROXY_SERVICE_H
#define FUNCTION_PROXY_LOCAL_SCHEDULER_GRPC_SERVER_FRONTEND_PROXY_SERVICE_FRONTEND_PROXY_SERVICE_H

#include <cstdint>
#include <functional>
#include <string>

#include <grpcpp/server_context.h>
#include <grpcpp/support/sync_stream.h>

#include "async/future.hpp"
#include "common/proto/pb/posix/frontend_proxy_service.grpc.pb.h"
#include "common/proto/pb/posix_pb.h"
#include "local_scheduler/instance_control/frontend_kill_cleanup_snapshot.h"

namespace functionsystem::local_scheduler {

struct FrontendProxyServiceParam {
    using InvokeDispatcher =
        std::function<litebus::Future<SharedStreamMsg>(const std::string &, const SharedStreamMsg &)>;
    using InvokeTenantAuthorizer = std::function<bool(const std::string &, const std::string &)>;
    using CreateDispatcher =
        std::function<litebus::Future<SharedStreamMsg>(const std::string &, const SharedStreamMsg &)>;
    using CreateReadyDispatcher =
        std::function<litebus::Future<::frontend_proxy::CreateInstanceResponse>(
            const ::frontend_proxy::CreateInstanceRequest &)>;
    using CreateWaitCanceller = std::function<void(const std::string &, const std::string &)>;
    using KillDispatcher =
        std::function<litebus::Future<SharedStreamMsg>(const std::string &, const SharedStreamMsg &)>;
    using KillReadyDispatcher =
        std::function<litebus::Future<::frontend_proxy::KillInstanceResponse>(
            const ::frontend_proxy::KillInstanceRequest &)>;
    using KillCleanupProbe =
        std::function<litebus::Future<FrontendKillCleanupSnapshot>(const std::string &, const std::string &)>;

    std::string nodeID;
    std::string endpointAddress;
    // Optional test seam. Production leaves this empty and dispatches through InvocationHandler::Invoke.
    InvokeDispatcher invokeDispatcher;
    // Production resolves the target instance locally and requires exact tenant
    // ownership before dispatching a frontend-originated invoke.
    InvokeTenantAuthorizer invokeTenantAuthorizer;
    // Optional test seam. Production may wire this to the reviewed create handler only after
    // frontend system-caller create semantics are enabled end-to-end.
    CreateDispatcher createDispatcher;
    // Reviewed create seam: dispatcher must return the frontend-facing final/ready response,
    // not the old immediate POSIX scheduler CreateResponse.
    CreateReadyDispatcher createReadyDispatcher;
    // Removes only the frontend ready waiter. It must never be interpreted as
    // cancelling an instance whose schedule request may already be dispatched.
    CreateWaitCanceller createWaitCanceller;
    // Optional test seam. Production may wire this to the reviewed kill handler only after
    // frontend system-caller lifecycle semantics are enabled end-to-end.
    KillDispatcher killDispatcher;
    // Reviewed kill seam: dispatcher must return the frontend-facing response,
    // not the old POSIX stream KillResponse wrapper.
    KillReadyDispatcher killReadyDispatcher;
    // Read-only production probe. A successful KillResponse is not cleanup evidence.
    KillCleanupProbe killCleanupProbe;
    bool enableCreateDispatch { false };
    bool enableKillDispatch { false };
    bool requireAuthenticatedPeer { false };
    uint64_t invokeResultTimeoutMs { 60000 };
    uint64_t killCleanupTimeoutMs { 2000 };
};

// FrontendProxyService is the same-port frontend entrypoint for faasfrontend.
// It is deliberately separate from PosixService: frontend service callers are
// system services and must not be registered as runtime stream clients.
class FrontendProxyService final : public frontend_proxy::FrontendProxyService::Service {
public:
    inline static constexpr const char *lifecycleTransport = "raw-unary";
    inline static constexpr const char *readyOperation = "ready";

    explicit FrontendProxyService(FrontendProxyServiceParam &&param);
    ~FrontendProxyService() override = default;

    ::grpc::Status InvokeInstance(::grpc::ServerContext *context,
                                  const ::frontend_proxy::InvokeInstanceRequest *request,
                                  ::frontend_proxy::InvokeInstanceResponse *response) override;

    ::grpc::Status InvokeInstanceStream(
        ::grpc::ServerContext *context,
        const ::frontend_proxy::InvokeInstanceRequest *request,
        ::grpc::ServerWriter<::frontend_proxy::InvokeInstanceStreamResponse> *writer) override;

    ::grpc::Status CreateInstance(::grpc::ServerContext *context,
                                  const ::frontend_proxy::CreateInstanceRequest *request,
                                  ::frontend_proxy::CreateInstanceResponse *response) override;

    ::grpc::Status KillInstance(::grpc::ServerContext *context,
                                const ::frontend_proxy::KillInstanceRequest *request,
                                ::frontend_proxy::KillInstanceResponse *response) override;

private:
    bool ValidateInvokeRequest(const ::frontend_proxy::InvokeInstanceRequest &request,
                               ::frontend_proxy::InvokeInstanceResponse &response) const;
    ::grpc::Status DispatchInvokeRequest(::grpc::ServerContext *context,
                                         const ::frontend_proxy::InvokeInstanceRequest &request,
                                         ::frontend_proxy::InvokeInstanceResponse &response);
    ::grpc::Status AwaitInvokeResult(::grpc::ServerContext *context,
                                     const ::frontend_proxy::InvokeInstanceRequest &request,
                                     ::frontend_proxy::InvokeInstanceResponse &response,
                                     const litebus::Future<SharedStreamMsg> &resultFuture);
    bool ValidateCreateRequest(const ::frontend_proxy::CreateInstanceRequest &request,
                               ::frontend_proxy::CreateInstanceResponse &response) const;
    ::grpc::Status DispatchCreateRequest(::grpc::ServerContext *context,
                                         const ::frontend_proxy::CreateInstanceRequest &request,
                                         ::frontend_proxy::CreateInstanceResponse &response);
    ::grpc::Status FinalizeCreateResponse(const ::frontend_proxy::CreateInstanceRequest &request,
                                          ::frontend_proxy::CreateInstanceResponse &response);
    bool ValidateKillRequest(const ::frontend_proxy::KillInstanceRequest &request,
                             ::frontend_proxy::KillInstanceResponse &response) const;
    ::grpc::Status DispatchKillRequest(::grpc::ServerContext *context,
                                       const ::frontend_proxy::KillInstanceRequest &request,
                                       ::frontend_proxy::KillInstanceResponse &response);
    static bool HasRequiredContext(const ::frontend_proxy::FrontendRequestContext &context);
    static bool HasRequiredLifecycleContext(const ::frontend_proxy::FrontendRequestContext &context);
    bool HasAuthenticatedPeer(const ::grpc::ServerContext *context) const;
    static void SetStatus(::frontend_proxy::FrontendProxyStatus *status, common::ErrorCode code,
                          const std::string &message, bool retryable = false,
                          const std::string &retryReason = "");
    FrontendProxyServiceParam param_;
};

}  // namespace functionsystem::local_scheduler

#endif  // FUNCTION_PROXY_LOCAL_SCHEDULER_GRPC_SERVER_FRONTEND_PROXY_SERVICE_FRONTEND_PROXY_SERVICE_H
