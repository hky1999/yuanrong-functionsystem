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

#include "frontend_proxy_lifecycle_handler.h"

#include <utility>

#include "common/logs/logging.h"
#include "common/utils/struct_transfer.h"

namespace functionsystem::local_scheduler {
namespace {
constexpr const char *FRONTEND_SYSTEM_CREATE_CALLER = "";
constexpr const char *FRONTEND_SYSTEM_KILL_CALLER = "";

::frontend_proxy::CreateInstanceResponse BuildCreateResponse(common::ErrorCode code, const std::string &message)
{
    ::frontend_proxy::CreateInstanceResponse response;
    response.mutable_create()->set_code(code);
    response.mutable_create()->set_message(message);
    return response;
}

::frontend_proxy::CreateInstanceResponse BuildCreateResponse(const messages::ScheduleResponse &scheduleResponse)
{
    ::frontend_proxy::CreateInstanceResponse response;
    response.mutable_create()->CopyFrom(TransFromScheduleRspToCreateRsp(scheduleResponse));
    return response;
}

void AttachCreateReadyCallResult(::frontend_proxy::CreateInstanceResponse &response,
                                 const core_service::CallResult &callResult)
{
    response.mutable_callresult()->CopyFrom(callResult);
}

::frontend_proxy::CreateInstanceResponse FinalizeCreateReadyResponse(
    ::frontend_proxy::CreateInstanceResponse response, const std::string &requestID,
    const std::shared_ptr<functionsystem::CallResult> &readyResult)
{
    if (readyResult == nullptr) {
        response.mutable_create()->set_code(common::ERR_INNER_SYSTEM_ERROR);
        response.mutable_create()->set_message("frontend proxy create ready call result is null");
        return response;
    }
    if (readyResult->instanceid().empty()) {
        readyResult->set_instanceid(response.create().instanceid());
    }
    AttachCreateReadyCallResult(response, *readyResult);
    // The public unary boundary echoes the frontend schedule ticket rather
    // than a potentially different runtime-internal request id.
    response.mutable_callresult()->set_requestid(requestID);
    if (readyResult->code() != common::ERR_NONE) {
        response.mutable_create()->set_code(readyResult->code());
        response.mutable_create()->set_message(readyResult->message());
    }
    return response;
}

std::shared_ptr<messages::ScheduleRequest> BuildFrontendScheduleRequest(
    const ::frontend_proxy::CreateInstanceRequest &request)
{
    CreateRequest createReq;
    createReq.CopyFrom(request.create());
    if (createReq.requestid().empty()) {
        createReq.set_requestid(request.context().requestid());
    }
    if (createReq.traceid().empty()) {
        createReq.set_traceid(request.context().traceid());
    }
    // Frontend unary create is a system-service request, not a runtime child
    // create. Keep the old runtime parent/sender identity empty so
    // InstanceCtrl does not try to authorize frontendClientID as a runtime
    // instance parent. The reviewed system-caller marker is carried by both
    // createOptions["source"]="frontend" and InstanceInfo.extensions so
    // function master can recognize frontend-created instances without a
    // runtime parent.
    auto scheduleReq = TransFromCreateReqToScheduleReq(std::move(createReq), FRONTEND_SYSTEM_CREATE_CALLER);
    (*scheduleReq->mutable_instance()->mutable_extensions())[CREATE_SOURCE] = FRONTEND_STR;
    return scheduleReq;
}
}

FrontendProxyServiceParam::CreateReadyDispatcher BuildFrontendProxyCreateReadyDispatcher(
    const FrontendProxyCreateReadyScheduler &scheduler, const FrontendProxyReadyUnregister &readyUnregister,
    uint64_t readyTimeoutMs)
{
    return [scheduler, readyUnregister, readyTimeoutMs](const ::frontend_proxy::CreateInstanceRequest &request) {
        if (!scheduler) {
            return litebus::Future<::frontend_proxy::CreateInstanceResponse>(BuildCreateResponse(
                common::ERR_INNER_SYSTEM_ERROR, "frontend proxy create scheduler is not configured"));
        }
        auto scheduleReq = BuildFrontendScheduleRequest(request);
        auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
        auto readyPromise = std::make_shared<litebus::Promise<std::shared_ptr<functionsystem::CallResult>>>();
        auto readyFuture =
            readyPromise->GetFuture().After(
                readyTimeoutMs,
                [scheduleReq, readyUnregister](const litebus::Future<std::shared_ptr<functionsystem::CallResult>> &)
                    -> litebus::Future<std::shared_ptr<functionsystem::CallResult>> {
                    if (readyUnregister) {
                        readyUnregister(scheduleReq->requestid(), "frontend create ready call result timed out");
                    }
                    auto result = std::make_shared<functionsystem::CallResult>();
                    result->set_requestid(scheduleReq->requestid());
                    result->set_code(common::ERR_INNER_SYSTEM_ERROR);
                    result->set_message("frontend proxy create ready call result timed out");
                    return result;
                });
        YRLOG_INFO("{}|frontend system create function({}) from frontendClientID({}), tenantID({})",
                   scheduleReq->requestid(), scheduleReq->instance().function(), request.context().frontendclientid(),
                   request.context().tenantid());
        return scheduler(scheduleReq, runtimePromise,
                         [readyPromise](const std::shared_ptr<functionsystem::CallResult> &readyResult)
                             -> litebus::Future<CallResultAck> {
            readyPromise->SetValue(readyResult);
            CallResultAck ack;
            ack.set_code(common::ERR_NONE);
            ack.set_message("success");
            return ack;
                         })
            .Then([scheduleReq, readyFuture](const messages::ScheduleResponse &scheduleResponse) {
                auto response = BuildCreateResponse(scheduleResponse);
                if (response.create().code() != common::ERR_NONE || response.create().instanceid().empty()) {
                    return litebus::Future<::frontend_proxy::CreateInstanceResponse>(response);
                }

                return readyFuture.Then([response, requestID = scheduleReq->requestid()](
                                            const std::shared_ptr<functionsystem::CallResult> &readyResult) mutable {
                    return FinalizeCreateReadyResponse(std::move(response), requestID, readyResult);
                });
            });
    };
}

FrontendProxyServiceParam::KillReadyDispatcher BuildFrontendProxyKillReadyDispatcher(
    const FrontendProxyKillInvoker &killInvoker)
{
    return [killInvoker](const ::frontend_proxy::KillInstanceRequest &request) {
        ::frontend_proxy::KillInstanceResponse response;
        if (!killInvoker) {
            response.mutable_kill()->set_code(common::ERR_INNER_SYSTEM_ERROR);
            response.mutable_kill()->set_message("frontend proxy kill invoker is not configured");
            return litebus::Future<::frontend_proxy::KillInstanceResponse>(response);
        }

        auto killReq = std::make_shared<KillRequest>();
        killReq->CopyFrom(request.kill());
        if (killReq->requestid().empty()) {
            killReq->set_requestid(request.context().requestid());
        }
        YRLOG_INFO("{}|frontend system kill instance({}) from frontendClientID({}), tenantID({})",
                   killReq->requestid(), killReq->instanceid(), request.context().frontendclientid(),
                   request.context().tenantid());
        return killInvoker(FRONTEND_SYSTEM_KILL_CALLER, request.context().tenantid(), killReq)
            .Then([](const KillResponse &killResponse) {
                ::frontend_proxy::KillInstanceResponse response;
                response.mutable_kill()->CopyFrom(killResponse);
                return response;
            });
    };
}

FrontendProxyServiceParam BuildFrontendProxyServiceParam(const std::string &nodeID,
                                                         const FrontendProxyServiceBindings &bindings)
{
    FrontendProxyServiceParam param;
    param.nodeID = nodeID;
    param.enableCreateDispatch = bindings.enableCreateDispatch;
    if (bindings.enableCreateDispatch) {
        param.createReadyDispatcher =
            BuildFrontendProxyCreateReadyDispatcher(bindings.scheduler, bindings.readyUnregister);
        param.createWaitCanceller = bindings.readyUnregister;
    }
    param.enableKillDispatch = bindings.enableKillDispatch;
    if (bindings.enableKillDispatch) {
        param.killReadyDispatcher = BuildFrontendProxyKillReadyDispatcher(bindings.killInvoker);
        param.killCleanupProbe = bindings.killCleanupProbe;
    }
    return param;
}

FrontendProxyServiceParam BuildFrontendProxyServiceParam(const std::string &nodeID, bool enableKillDispatch,
                                                         const FrontendProxyKillInvoker &killInvoker)
{
    return BuildFrontendProxyServiceParam(
        nodeID, { false, nullptr, nullptr, enableKillDispatch, killInvoker, nullptr });
}

}  // namespace functionsystem::local_scheduler
