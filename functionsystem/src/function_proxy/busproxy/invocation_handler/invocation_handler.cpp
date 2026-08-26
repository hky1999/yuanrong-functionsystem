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

#include "invocation_handler.h"

#include "common/logs/logging.h"
#include "common/rpc/stream/posix/control_client.h"
#include "function_proxy/busproxy/instance_proxy/instance_proxy.h"


namespace functionsystem {
using namespace runtime_rpc;

const uint32_t MSG_ESTIMATED_FACTOR = 2;

REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(StreamingMessage::kInvokeReq, &InvocationHandler::Invoke);
REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(StreamingMessage::kCallResultReq, &InvocationHandler::CallResultAdapter);
REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(StreamingMessage::kEventReq, &InvocationHandler::EventAdapter);

std::shared_ptr<StreamingMessage> InvokeRequestToCallRequest(const std::string &from,
                                                             const std::shared_ptr<InvokeRequest> &request)
{
    ASSERT_IF_NULL(request);
    auto retRequest = std::make_shared<StreamingMessage>();
    auto callRequest = retRequest->mutable_callreq();
    callRequest->set_function(request->function());
    *callRequest->mutable_args() = std::move(*request->mutable_args());
    callRequest->set_requestid(request->requestid());
    callRequest->set_traceid(request->traceid());
    *callRequest->mutable_returnobjectids() = std::move(*request->mutable_returnobjectids());
    callRequest->set_senderid(from);
    *callRequest->mutable_createoptions() = request->invokeoptions().customtag();
    callRequest->set_bypass_datasystem(request->invokeoptions().bypass_datasystem());
    return retRequest;
}

std::shared_ptr<StreamingMessage> CallResponseToInvokeResponse(const std::shared_ptr<StreamingMessage> &response)
{
    ASSERT_IF_NULL(response);
    ASSERT_FS(response->has_callrsp());
    auto msg = std::make_shared<StreamingMessage>();
    auto invokeResponse = msg->mutable_invokersp();
    invokeResponse->set_code(response->callrsp().code());
    invokeResponse->set_message(response->callrsp().message());
    return msg;
}

litebus::Future<std::shared_ptr<StreamingMessage>> InvocationHandler::Invoke(
    const std::string &from, const std::shared_ptr<StreamingMessage> &request)
{
    // Get AID of instance actor from instance view according to instance ID.
    ASSERT_IF_NULL(request);
    ASSERT_FS(request->has_invokereq());
    auto invokeRequest = std::make_shared<InvokeRequest>(std::move(*request->mutable_invokereq()));
    auto instanceID = invokeRequest->instanceid();
    auto recevied =
        isPerf_ ? std::make_shared<busproxy::TimePoint>(std::chrono::high_resolution_clock::now()) : nullptr;
    litebus::AID id(instanceID, localUrl_);
    auto callRequest = InvokeRequestToCallRequest(from, invokeRequest);

    if (litebus::GetActor(id) == nullptr) {
        id.SetName(from);
    }
    if (memoryMonitor_ && memoryMonitor_->IsEnabled() &&
        !memoryMonitor_->Allow(instanceID, invokeRequest->requestid(),
                               static_cast<uint64_t>(invokeRequest->ByteSizeLong() * MSG_ESTIMATED_FACTOR))) {
        YRLOG_ERROR("{}|{}|received Invoke instance({}) from {} via POSIX, memory usage not enough, reject request.",
                    invokeRequest->traceid(), invokeRequest->requestid(), instanceID, from);
        auto response = std::make_shared<StreamingMessage>();
        response->mutable_invokersp()->set_code(common::ERR_INVOKE_RATE_LIMITED);
        response->mutable_invokersp()->set_message("system memory usage not enough, reject invoke request");
        return response;
    }
    if (internalIam_ && internalIam_->IsIAMEnabled()) {
        return Authorize(id, from, instanceID, callRequest, recevied);
    }
    YRLOG_INFO("{}|{}|received Invoke instance({}) from {}, actor({}) will handle it.", invokeRequest->traceid(),
               invokeRequest->requestid(), instanceID, from, id.HashString());
    ASSERT_IF_NULL(instanceProxy_);
    return instanceProxy_
        ->Call(id, busproxy::CallerInfo{ .instanceID = from, .tenantID = "" }, instanceID, callRequest, recevied)
        .Then([](const std::shared_ptr<StreamingMessage> &rsp) { return CallResponseToInvokeResponse(rsp); });
}

litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> InvocationHandler::Authorize(
    const litebus::AID &to, const std::string &srcInstanceID, const std::string &instanceID,
    const SharedStreamMsg &request, const std::shared_ptr<busproxy::TimePoint> &time)
{
    ASSERT_IF_NULL(instanceProxy_);
    litebus::AID src(srcInstanceID, localUrl_);
    if (litebus::GetActor(src) == nullptr) {
        YRLOG_WARN("get actor is null for instance {}", srcInstanceID);

        return instanceProxy_
            ->Call(to, busproxy::CallerInfo{ .instanceID = srcInstanceID, .tenantID = "" }, instanceID, request, time)
            .Then([](const std::shared_ptr<StreamingMessage> &rsp) { return CallResponseToInvokeResponse(rsp); });
    }
    return instanceProxy_->GetTenantID(src).Then(
        [to, srcInstanceID, instanceID, request, time](const std::string &tenantID) {
            return CallWithAuthorize(to, busproxy::CallerInfo{ .instanceID = srcInstanceID, .tenantID = tenantID },
                                     instanceID, request, time);
        });
}

litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> InvocationHandler::CallWithAuthorize(
    const litebus::AID &to, const busproxy::CallerInfo &callerInfo, const std::string &instanceID,
    const SharedStreamMsg &request, const std::shared_ptr<busproxy::TimePoint> &time)
{
    ASSERT_IF_NULL(instanceProxy_);
    return instanceProxy_->Call(to, callerInfo, instanceID, request, time)
        .Then([](const std::shared_ptr<StreamingMessage> &rsp) { return CallResponseToInvokeResponse(rsp); });
}

litebus::Future<std::shared_ptr<StreamingMessage>> InvocationHandler::CallResultAdapter(
    const std::string &from, const std::shared_ptr<StreamingMessage> &request)
{
    ASSERT_IF_NULL(request);
    ASSERT_FS(request->has_callresultreq());
    YRLOG_INFO("{}|received CallResult request from {} via POSIX.", request->callresultreq().requestid(), from);
    if (frontendCallResultReceiver_) {
        auto result = frontendCallResultReceiver_(from, request);
        if (result.first) {
            YRLOG_INFO("{}|CallResult from {} is consumed by frontend proxy service.",
                       request->callresultreq().requestid(), from);
            ASSERT_IF_NULL(instanceProxy_);
            litebus::AID id(from, localUrl_);
            return instanceProxy_->CompleteFrontendCall(id, request, result.second);
        }
    }
    if (createCallResultReceiver_) {
        auto requestIDs = litebus::strings::Split(request->callresultreq().requestid(), "@");
        if (!requestIDs.empty() && *requestIDs.rbegin() == "initcall") {
            auto callResult =
                std::make_shared<functionsystem::CallResult>(std::move(*request->mutable_callresultreq()));
            callResult->set_requestid(requestIDs[0]);
            return createCallResultReceiver_(from, callResult)
                .Then([from, callResult,
                       request](const std::pair<bool, std::shared_ptr<runtime_rpc::StreamingMessage>> &result)
                          -> litebus::Future<std::shared_ptr<StreamingMessage>> {
                    if (result.first) {
                        YRLOG_INFO("{}|request from {} is create request.", callResult->requestid(), from);
                        ASSERT_IF_NULL(result.second);
                        return result.second;
                    }
                    // initcall must be verified by local scheduler
                    ASSERT_IF_NULL(result.second);
                    auto response = result.second;
                    response->set_messageid(request->messageid());
                    auto callResultAck = response->mutable_callresultack();
                    callResultAck->set_code(common::ERR_INNER_COMMUNICATION);
                    return response;
                });
        }
    }
    return CallResult(from, request);
}

litebus::Future<std::shared_ptr<StreamingMessage>> InvocationHandler::EventAdapter(
    const std::string &from, const std::shared_ptr<StreamingMessage> &request)
{
    ASSERT_IF_NULL(request);
    ASSERT_FS(request->has_eventreq());
    if (frontendEventReceiver_ && frontendEventReceiver_(from, request)) {
        YRLOG_DEBUG("{}|Event from {} is consumed by frontend proxy service.",
                    request->eventreq().requestid(), from);
    } else {
        YRLOG_WARN("{}|No frontend stream is waiting for event from {}.",
                   request->eventreq().requestid(), from);
    }
    auto response = std::make_shared<StreamingMessage>();
    auto ack = response->mutable_callresultack();
    ack->set_code(common::ERR_NONE);
    ack->set_message("success");
    return response;
}

litebus::Future<std::shared_ptr<StreamingMessage>> InvocationHandler::CallResult(
    const std::string &from, const std::shared_ptr<StreamingMessage> &request)
{
    auto recevied =
        isPerf_ ? std::make_shared<busproxy::TimePoint>(std::chrono::high_resolution_clock::now()) : nullptr;
    ASSERT_IF_NULL(request);
    ASSERT_FS(request->has_callresultreq());
    auto callResult = request->callresultreq();
    // Get AID of instance actor from instance view according to instance ID.
    litebus::AID id(callResult.instanceid(), localUrl_);
    if (litebus::GetActor(id) == nullptr) {
        id.SetName(from);
    }
    YRLOG_DEBUG("{}|send CallResult to instance({}) from {}", callResult.requestid(), id.HashString(), from);
    ASSERT_IF_NULL(instanceProxy_);
    return instanceProxy_->CallResult(id, from, callResult.instanceid(), request, recevied);
}
}  // namespace functionsystem
