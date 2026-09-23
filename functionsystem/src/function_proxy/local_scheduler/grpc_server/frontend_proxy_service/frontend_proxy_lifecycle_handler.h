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

#ifndef FUNCTION_PROXY_LOCAL_SCHEDULER_GRPC_SERVER_FRONTEND_PROXY_SERVICE_FRONTEND_PROXY_LIFECYCLE_HANDLER_H
#define FUNCTION_PROXY_LOCAL_SCHEDULER_GRPC_SERVER_FRONTEND_PROXY_SERVICE_FRONTEND_PROXY_LIFECYCLE_HANDLER_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "common/status/status.h"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix/frontend_proxy_service.pb.h"
#include "common/proto/pb/posix_pb.h"
#include "grpc_server/frontend_proxy_service/frontend_proxy_service.h"
#include "local_scheduler/instance_control/frontend_create_failure_snapshot.h"

namespace functionsystem::local_scheduler {

// Default upper bound (in milliseconds) the frontend unary-create path waits
// for the runtime to report the ready CallResult before the ticket is
// unregistered as timed out.
inline constexpr uint64_t FRONTEND_CREATE_READY_TIMEOUT_MS = 55ULL * 1000ULL;

uint64_t ResolveFrontendCreateReadyTimeoutMs(
    const ::frontend_proxy::CreateInstanceRequest &request,
    uint64_t fallbackTimeoutMs = FRONTEND_CREATE_READY_TIMEOUT_MS);

using FrontendProxyReadyCallback = std::function<litebus::Future<CallResultAck>(
    const std::shared_ptr<functionsystem::CallResult> &)>;
using FrontendProxyReadyRegistrar =
    std::function<void(const std::string &, const std::shared_ptr<messages::ScheduleRequest> &,
                       FrontendProxyReadyCallback)>;
using FrontendProxyCreateReadyScheduler =
    std::function<litebus::Future<messages::ScheduleResponse>(
        const std::shared_ptr<messages::ScheduleRequest> &,
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &,
        FrontendProxyReadyCallback)>;
using FrontendProxyReadyUnregister = std::function<void(const std::string &, const std::string &)>;
// Returns and consumes the last deploy-failure snapshot for a pending frontend
// create (POST /api/agent). present=false when no failure was recorded, in which
// case the ready-timeout closure falls back to the generic timeout string.
using FrontendCreateFailureLookup =
    std::function<litebus::Future<FrontendCreateFailureSnapshot>(const std::string &)>;
using FrontendProxyCreateScheduler =
    std::function<litebus::Future<messages::ScheduleResponse>(
        const std::shared_ptr<messages::ScheduleRequest> &,
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &)>;
using FrontendProxyKillInvoker =
    std::function<litebus::Future<KillResponse>(const std::string &, const std::string &,
                                                const std::shared_ptr<KillRequest> &)>;
using FrontendProxyKillCleanupProbe = FrontendProxyServiceParam::KillCleanupProbe;

struct FrontendProxyServiceBindings {
    bool enableCreateDispatch { false };
    FrontendProxyCreateReadyScheduler scheduler;
    FrontendProxyReadyUnregister readyUnregister;
    FrontendCreateFailureLookup createFailureLookup;
    bool enableKillDispatch { false };
    FrontendProxyKillInvoker killInvoker;
    // Routes through FunctionAgentMgrActor::ResizeSandbox (ReserveResize ->
    // OnResizeReserved -> Send): the ONLY scheduler-authorized resize entry.
    // The implementation resolves the authoritative InstanceInfo from THIS
    // node's InstanceControlView — a caller-supplied identity is never
    // trusted. Takes only the request; identity resolution stays internal.
    std::function<litebus::Future<messages::ResizeSandboxResponse>(
        const std::shared_ptr<messages::ResizeSandboxRequest> &)> resizeInvoker;
    FrontendProxyKillCleanupProbe killCleanupProbe;
};

struct ComponentGrpcEndpoint {
    std::string ip;
    std::string port;
    bool useComponentServer { false };

    std::string Address() const;
};

ComponentGrpcEndpoint ResolveComponentGrpcEndpoint(const std::string &proxyAddress, const std::string &localIP,
                                                   const std::string &posixPort,
                                                   const std::string &componentGrpcPort);

FrontendProxyServiceParam::CreateReadyDispatcher BuildFrontendProxyCreateReadyDispatcher(
    const FrontendProxyCreateReadyScheduler &scheduler,
    const FrontendProxyReadyUnregister &readyUnregister = FrontendProxyReadyUnregister(),
    uint64_t readyTimeoutMs = FRONTEND_CREATE_READY_TIMEOUT_MS,
    const FrontendCreateFailureLookup &createFailureLookup = FrontendCreateFailureLookup());

FrontendProxyServiceParam::KillReadyDispatcher BuildFrontendProxyKillReadyDispatcher(
    const FrontendProxyKillInvoker &killInvoker);

FrontendProxyServiceParam BuildFrontendProxyServiceParam(const std::string &nodeID,
                                                         const FrontendProxyServiceBindings &bindings);

FrontendProxyServiceParam BuildFrontendProxyServiceParam(const std::string &nodeID, bool enableKillDispatch,
                                                         const FrontendProxyKillInvoker &killInvoker);

}  // namespace functionsystem::local_scheduler

#endif  // FUNCTION_PROXY_LOCAL_SCHEDULER_GRPC_SERVER_FRONTEND_PROXY_SERVICE_FRONTEND_PROXY_LIFECYCLE_HANDLER_H
