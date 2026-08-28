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
#include "instance_proxy.h"

#include "async/defer.hpp"
#include "common/metrics/metrics_adapter.h"
#include "common/status/status.h"
#include "busproxy/invocation_handler/invocation_handler.h"
#include "function_proxy/config/direct_routing_config.h"

namespace functionsystem::busproxy {

const std::string INSTANCE_EXIT_MESSAGE = "instance has been killed or exited.";
const std::string YR_ROUTE_KEY = "YR_ROUTE";
const std::string RETURN_ROUTE_INFO_KEY = "YR_RETURN_ROUTE_INFO";
const uint32_t MAX_CALL_RESULT_RETRY_TIMES = 3;

common::ErrorCode QueryRouteErrorCode(const int32_t errorCode)
{
    if (errorCode == static_cast<int32_t>(StatusCode::ERR_INSTANCE_NOT_FOUND)) {
        return common::ErrorCode::ERR_INSTANCE_NOT_FOUND;
    }
    return common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS;
}

void InstanceProxy::Init()
{
    ActorBase::Init();
    Receive("ForwardCall", &InstanceProxy::ForwardCall);
    Receive("ResponseForwardCall", &InstanceProxy::ResponseForwardCall);
    Receive("ForwardCallResult", &InstanceProxy::ForwardCallResult);
    Receive("ResponseForwardCallResult", &InstanceProxy::ResponseForwardCallResult);
}

litebus::Future<std::string> InstanceProxy::GetTenantID()
{
    ASSERT_FS(selfDispatcher_);
    return selfDispatcher_->GetTenantID();
}

litebus::Future<size_t> InstanceProxy::GetInFlightCount()
{
    ASSERT_FS(selfDispatcher_);
    return selfDispatcher_->GetInFlightCount();
}

void InstanceProxy::ResumeAfterAbandonedDrain()
{
    ASSERT_FS(selfDispatcher_);
    selfDispatcher_->ResumeAfterAbandonedDrain();
}

litebus::Future<SharedStreamMsg> InstanceProxy::Call(const CallerInfo &callerInfo,
                                                     const std::string &dstInstanceID, const SharedStreamMsg &request,
                                                     const std::shared_ptr<TimePoint> &time)
{
    ASSERT_FS(request->has_callreq());
    const auto &callReq = request->callreq();
    YRLOG_INFO("{}|{}|received call request from {} to {}", callReq.traceid(), callReq.requestid(),
               callerInfo.instanceID, dstInstanceID);
    perf_->Record(callReq, dstInstanceID, time);
    // P2 #8 (idle session-count gap): SDK exec/invoke rides the Invoke channel
    // and never reaches ExecStreamService, so SessionCountDelta never fires and
    // DataPlaneObserver::MarkInstanceUsed had NO caller at all — a sandbox
    // driven purely by sparse frontend invokes (or with only detached
    // background processes inside, e.g. the G3a2 allocators) stayed
    // "never-used", fell out of the orphan grace window and was reclaimed by
    // the idle sweeper. Mark the DESTINATION as ever-used on every real
    // client invoke; the fire-and-forget observer hop re-arms its idle timer
    // at the full timeout.
    if (observer_ != nullptr && dstInstanceID == instanceID_) {
        observer_->MarkInstanceUsed(dstInstanceID);
    }
    // which means the invocation is happening without cross node
    // else it should be transferred by remote dispatcher
    if (dstInstanceID == instanceID_) {
        ASSERT_FS(selfDispatcher_);
        return selfDispatcher_->Call(request, callerInfo)
            .Then([aid(GetAID()), request, selfDispatcher(selfDispatcher_)](const SharedStreamMsg &callRsp) {
                litebus::Async(aid, &InstanceProxy::OnLocalCall, callRsp, request, selfDispatcher);
                return callRsp;
            });
    }
    // If invoke instance with route, just forward it to other node
    if (const auto it = callReq.createoptions().find(YR_ROUTE_KEY);
        it != callReq.createoptions().end() && !it->second.empty()) {
        return CallWithExplicitRoute(callerInfo, dstInstanceID, request, it->second);
    }

    if (function_proxy::DirectRoutingConfig::IsEnabled()) {
        auto cachedRoute = routeCache_.Get(dstInstanceID);
        if (cachedRoute.has_value() && !cachedRoute->empty()) {
            auto remoteAID = litebus::AID(dstInstanceID, *cachedRoute);
            return ForwardWithRouteAndFallback(remoteAID, dstInstanceID, callerInfo, request);
        }
    }

    // If the corresponding instance is not found in the dispatcher,
    // the instance information needs to be subscribed to from the observer.
    auto dispatcher = GetOrCreateRemoteDispatcher(dstInstanceID);
    // remote response received by this actor, so the callback can be called in this actor thread
    auto func = [traceID(callReq.traceid()), requestID(callReq.requestid()),
                 dispatcher](const SharedStreamMsg &callRsp) {
        dispatcher->OnCall(callRsp, traceID, requestID);
        return callRsp;
    };
    return dispatcher->Call(request, callerInfo).Then(func);
}

litebus::Future<SharedStreamMsg> InstanceProxy::CallWithExplicitRoute(
    const CallerInfo &callerInfo, const std::string &dstInstanceID, const SharedStreamMsg &request,
    const std::string &route)
{
    auto remoteAID = litebus::AID(dstInstanceID, route);
    if (function_proxy::DirectRoutingConfig::IsEnabled()) {
        routeCache_.Put(dstInstanceID, route);
        return ForwardWithRouteAndFallback(remoteAID, dstInstanceID, callerInfo, request);
    }

    auto promise = std::make_shared<litebus::Promise<SharedStreamMsg>>();
    SendForwardCall(remoteAID, callerInfo.tenantID, request)
        .OnComplete([messageID(request->messageid()), promise](const litebus::Future<SharedStreamMsg> &future) {
            if (future.IsError()) {
                auto response = std::make_shared<runtime_rpc::StreamingMessage>();
                response->set_messageid(messageID);
                auto callResponse = response->mutable_callrsp();
                callResponse->set_code(Status::GetPosixErrorCode(common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS));
                callResponse->set_message("connection with runtime may be interrupted, please retry.");
                promise->SetValue(response);
                return;
            }
            auto rsp = future.Get();
            rsp->set_messageid(messageID);
            promise->SetValue(rsp);
        });
    return promise->GetFuture();
}

std::shared_ptr<RequestDispatcher> InstanceProxy::GetOrCreateRemoteDispatcher(const std::string &dstInstanceID)
{
    if (remoteDispatchers_.find(dstInstanceID) == remoteDispatchers_.end()) {
        auto dispatcher = std::make_shared<RequestDispatcher>(dstInstanceID, false, "", shared_from_this(), perf_);
        ASSERT_FS(observer_);
        (void)observer_->SubscribeInstanceEvent(instanceID_, dstInstanceID);
        remoteDispatchers_[dstInstanceID] = std::move(dispatcher);
    }
    return remoteDispatchers_[dstInstanceID];
}

litebus::Future<SharedStreamMsg> InstanceProxy::ForwardWithRouteAndFallback(const litebus::AID &remoteAID,
                                                                            const std::string &dstInstanceID,
                                                                            const CallerInfo &callerInfo,
                                                                            const SharedStreamMsg &request)
{
    auto promise = std::make_shared<litebus::Promise<SharedStreamMsg>>();
    auto originalMessageID = request->messageid();
    SendForwardCall(remoteAID, callerInfo.tenantID, request)
        .OnComplete(litebus::Defer(GetAID(), &InstanceProxy::OnForwardResult, dstInstanceID, originalMessageID,
                                   callerInfo, request, promise, std::placeholders::_1));
    return promise->GetFuture();
}

void InstanceProxy::OnForwardResult(const std::string &dstInstanceID, const std::string &originalMessageID,
                                    const CallerInfo &callerInfo, const SharedStreamMsg &request,
                                    std::shared_ptr<litebus::Promise<SharedStreamMsg>> promise,
                                    const litebus::Future<SharedStreamMsg> &future)
{
    if (!future.IsError()) {
        auto rsp = future.Get();
        rsp->set_messageid(originalMessageID);
        promise->SetValue(rsp);
        return;
    }

    YRLOG_WARN("{}|Forward to cached route failed for instance {}, evicting and querying observer",
               request->callreq().traceid(), dstInstanceID);
    routeCache_.Remove(dstInstanceID);
    if (observer_ == nullptr) {
        promise->SetValue(CreateCallResponse(common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                                             "connection with runtime may be interrupted, please retry.",
                                             originalMessageID));
        return;
    }
    observer_->QueryInstanceRoute(dstInstanceID).OnComplete(
        litebus::Defer(GetAID(), &InstanceProxy::OnQueryRouteResult, dstInstanceID, originalMessageID,
                       callerInfo, request, promise, std::placeholders::_1));
}

void InstanceProxy::OnQueryRouteResult(const std::string &dstInstanceID, const std::string &originalMessageID,
                                       const CallerInfo &callerInfo, const SharedStreamMsg &request,
                                       std::shared_ptr<litebus::Promise<SharedStreamMsg>> promise,
                                       const litebus::Future<std::shared_ptr<resources::RouteInfo>> &routeFuture)
{
    if (routeFuture.IsError()) {
        YRLOG_ERROR("{}|QueryInstanceRoute failed for instance {}", request->callreq().traceid(), dstInstanceID);
        auto code = QueryRouteErrorCode(routeFuture.GetErrorCode());
        auto message = code == common::ErrorCode::ERR_INSTANCE_NOT_FOUND
                           ? "instance is not found, maybe be not created or killed"
                           : "connection with runtime may be interrupted, please retry.";
        promise->SetValue(CreateCallResponse(code, message, originalMessageID));
        return;
    }
    if (routeFuture.Get() == nullptr || routeFuture.Get()->proxygrpcaddress().empty()) {
        YRLOG_ERROR("{}|QueryInstanceRoute returned empty route for instance {}", request->callreq().traceid(),
                    dstInstanceID);
        promise->SetValue(CreateCallResponse(common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                                             "connection with runtime may be interrupted, please retry.",
                                             originalMessageID));
        return;
    }

    const auto &route = routeFuture.Get()->proxygrpcaddress();
    routeCache_.Put(dstInstanceID, route);
    auto remoteAID = litebus::AID(dstInstanceID, route);
    (void)SendForwardCall(remoteAID, callerInfo.tenantID, request)
        .OnComplete(litebus::Defer(GetAID(), &InstanceProxy::OnQueryRouteForwardComplete, originalMessageID,
                                   promise, std::placeholders::_1));
}

void InstanceProxy::OnQueryRouteForwardComplete(const std::string &originalMessageID,
                                                std::shared_ptr<litebus::Promise<SharedStreamMsg>> promise,
                                                const litebus::Future<SharedStreamMsg> &future)
{
    if (future.IsError()) {
        promise->SetValue(CreateCallResponse(common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                                             "connection with runtime may be interrupted, please retry.",
                                             originalMessageID));
        return;
    }

    auto rsp = future.Get();
    rsp->set_messageid(originalMessageID);
    promise->SetValue(rsp);
}

void InstanceProxy::OnLocalCall(const litebus::Future<SharedStreamMsg> &callRspFut, const SharedStreamMsg &callReq,
                                const std::shared_ptr<RequestDispatcher> &dispatcher)
{
    ASSERT_FS(!callRspFut.IsError());
    const auto &callRsp = callRspFut.Get();
    auto &requestID = callReq->callreq().requestid();
    perf_->RecordReceivedCallRsp(requestID);
    dispatcher->OnCall(callRsp, callReq->callreq().traceid(), callReq->callreq().requestid());
}

void InstanceProxy::ForwardCall(const litebus::AID &from, std::string &&, std::string &&msg)
{
    // P2 #8: an invoke forwarded INTO this actor's instance is real client use
    // (SDK exec rides this channel, never ExecStreamService) — claim it for
    // the idle bookkeeping so the instance cannot be reclaimed as an orphan
    // while background work keeps running (see InstanceProxy::Call note).
    if (observer_ != nullptr) {
        observer_->MarkInstanceUsed(instanceID_);
    }
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    (void)request->ParseFromString(msg);
    (void)DoForwardCall(from, request);
}

litebus::Future<Status> InstanceProxy::DoForwardCall(const litebus::AID &from,
                                                     const std::shared_ptr<runtime_rpc::StreamingMessage> &request)
{
    ASSERT_FS(request->has_callreq());
    auto callReqWithOptions = request->mutable_callreq();
    if (callReqWithOptions->mutable_createoptions()->erase(RETURN_ROUTE_INFO_KEY) > 0) {
        returnRouteInfoRequestIDs_.insert(callReqWithOptions->requestid());
    }
    auto &srcInstanceID = from.Name();
    const auto &callReq = request->callreq();
    std::string srcTenantID = "";
    // if enable multi tenant, messageid contains tenantID of src instance, {tenantID}{requestID}
    if (request->messageid().length() > callReq.requestid().length()) {
        srcTenantID = request->messageid().substr(0, request->messageid().length() - callReq.requestid().length());
        request->set_messageid(callReq.requestid());
    }
    YRLOG_INFO("{}|{}|received forward Call instance from {} to {}, function name is {}", callReq.traceid(),
               callReq.requestid(), srcInstanceID, instanceID_, callReq.function());
    perf_->Record(callReq, instanceID_, nullptr);

    std::map<std::string, std::string> callCreateOptMap;
    for (const auto &ite : callReq.createoptions()) {
        callCreateOptMap[ite.first] = ite.second;
    }
    functionsystem::metrics::MetricsAdapter::GetInstance().GetMetricsContext().SetBillingInvokeOptions(
        callReq.requestid(), callCreateOptMap, callReq.function(), instanceID_);

    ASSERT_FS(selfDispatcher_);
    (void)selfDispatcher_->Call(request, CallerInfo{ .instanceID = srcInstanceID, .tenantID = srcTenantID })
        .OnComplete(litebus::Defer(GetAID(), &InstanceProxy::OnForwardCall, std::placeholders::_1, from, request,
                                   selfDispatcher_));
    // If the remote dispatcher does not have a corresponding sender instance,
    // we need to generate one and subscribe to it from the observer.
    if ((remoteDispatchers_.find(srcInstanceID) == remoteDispatchers_.end() ||
         remoteDispatchers_[srcInstanceID] == nullptr) &&
        srcInstanceID != instanceID_) {
        auto dispatcher = std::make_shared<RequestDispatcher>(srcInstanceID, false, "", shared_from_this(), perf_);
        ASSERT_FS(observer_);
        (void)observer_->SubscribeInstanceEvent(instanceID_, srcInstanceID, true);
        remoteDispatchers_[srcInstanceID] = dispatcher;
    }

    if (srcInstanceID != instanceID_) {
        // during recover, from.Name == instanceID_, dispatcher will be null
        auto dispatcher = remoteDispatchers_[srcInstanceID];
        dispatcher->UpdateRemoteAID(from);
    }
    return Status::OK();
}

void InstanceProxy::Reject(const std::string &instanceID, const std::string &message, const StatusCode &code)
{
    if (instanceID == instanceID_) {
        ASSERT_FS(selfDispatcher_);
        selfDispatcher_->Reject(message, code);
        return;
    }
    if (remoteDispatchers_.find(instanceID) != remoteDispatchers_.end() && remoteDispatchers_[instanceID]) {
        remoteDispatchers_[instanceID]->Reject(message, code);
    }
}

void InstanceProxy::OnForwardCall(const litebus::Future<SharedStreamMsg> &callRspFut, const litebus::AID &from,
                                  const SharedStreamMsg &callReq, const std::shared_ptr<RequestDispatcher> &dispatcher)
{
    ASSERT_FS(!callRspFut.IsError());
    const auto &callRsp = callRspFut.Get();
    ASSERT_FS(callReq->has_callreq());
    auto call = callReq->callreq();
    auto &requestID = callReq->callreq().requestid();
    perf_->RecordReceivedCallRsp(requestID);
    dispatcher->OnCall(callRsp, call.traceid(), call.requestid());
    if (callRsp->has_callrsp() && callRsp->callrsp().code() != common::ERR_NONE) {
        returnRouteInfoRequestIDs_.erase(requestID);
    }
    callRsp->set_messageid(call.requestid());
    YRLOG_INFO("{}|{}|ready to forward call response", call.traceid(), call.requestid());
    Send(from, "ResponseForwardCall", callRsp->SerializeAsString());
}

void InstanceProxy::ResponseForwardCall(const litebus::AID &from, std::string &&, std::string &&msg)
{
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    (void)request->ParseFromString(msg);
    ASSERT_FS(request->has_callrsp());
    auto &requestID = request->messageid();
    perf_->RecordReceivedCallRsp(requestID);
    YRLOG_INFO("receive forward call response {} from {}", request->messageid(), std::string(from));
    if (auto promise(forwardCallPromises_.find(request->messageid())); promise != forwardCallPromises_.end()) {
        promise->second->SetValue(request);
        (void)forwardCallPromises_.erase(promise);
        EraseForwardCallContext(requestID);
        return;
    }
    YRLOG_WARN("no request {} is waiting for forward call response, ignore it.", request->messageid());
}

litebus::Future<SharedStreamMsg> InstanceProxy::CallResult(const std::string &srcInstanceID,
                                                           const std::string &dstInstanceID,
                                                           const SharedStreamMsg &request,
                                                           const std::shared_ptr<TimePoint> &time)
{
    ASSERT_FS(request->has_callresultreq());
    auto callresult = request->mutable_callresultreq();
    AttachCurrentRouteInfoForRefreshedCall(callresult);
    auto &requestID = callresult->requestid();
    perf_->RecordCallResult(requestID, time);
    // which means the invocation is happening without cross node
    // else it should be transferred by remote dispatcher
    if (dstInstanceID == instanceID_) {
        ASSERT_FS(selfDispatcher_);
        return selfDispatcher_->CallResult(request)
            .Then([aid(GetAID()), request, dstInstanceID, srcInstanceID](const SharedStreamMsg &callResultAck) {
                litebus::Async(aid, &InstanceProxy::OnLocalCallResult, callResultAck, request, dstInstanceID,
                               srcInstanceID);
                return callResultAck;
            });
    }
    if (remoteDispatchers_.find(dstInstanceID) == remoteDispatchers_.end() ||
        remoteDispatchers_[dstInstanceID] == nullptr) {
        auto dispatcher = std::make_shared<RequestDispatcher>(dstInstanceID, false, "", shared_from_this(), perf_);
        ASSERT_FS(observer_);
        // If the destination instance is not found (usually after the proxy is restarted), subscribe to the
        // corresponding instance information and try again.
        auto future = observer_->SubscribeInstanceEvent(instanceID_, dstInstanceID)
                          .Then([aid(GetAID()), srcInstanceID, dstInstanceID, request, time](const Status &) {
                              return litebus::Async(aid, &InstanceProxy::RetryCallResult, srcInstanceID, dstInstanceID,
                                                    request, time);
                          });
        remoteDispatchers_[dstInstanceID] = dispatcher;
        return future;
    }
    auto dispatcher = remoteDispatchers_[dstInstanceID];
    auto callResultCode = callresult->code();
    // remote response received by this actor, so the callback can be called in this actor thread
    auto func = [requestID(callresult->requestid()), dispatcher(selfDispatcher_), callResultCode]
            (const SharedStreamMsg &callResultAck) {
                dispatcher->OnCallResult(callResultAck, requestID, callResultCode);
                return callResultAck;
            };
    return dispatcher->CallResult(request).Then(func);
}

SharedStreamMsg InstanceProxy::CompleteFrontendCall(const SharedStreamMsg &callResult,
                                                    const SharedStreamMsg &callResultAck)
{
    ASSERT_FS(callResult->has_callresultreq());
    ASSERT_FS(callResultAck->has_callresultack());
    ASSERT_FS(selfDispatcher_);
    const auto &result = callResult->callresultreq();
    selfDispatcher_->OnCallResult(callResultAck, result.requestid(), result.code());
    perf_->EndRecord(result.requestid());
    InvocationHandler::ReleaseEstimateMemory(instanceID_, result.requestid());
    return callResultAck;
}

litebus::Future<SharedStreamMsg> InstanceProxy::RetryCallResult(const std::string &srcInstanceID,
                                                                const std::string &dstInstanceID,
                                                                const SharedStreamMsg &request,
                                                                const std::shared_ptr<TimePoint> &time)
{
    if (remoteDispatchers_.find(dstInstanceID) != remoteDispatchers_.end()) {
        failedSubDstRouteOnCallResult_.erase(dstInstanceID);
        return CallResult(srcInstanceID, dstInstanceID, request, time);
    }
    static const uint32_t MAX_FAILED_TIMES = 3;
    static const int64_t DEFER_RETRY = 1000;
    if (failedSubDstRouteOnCallResult_[dstInstanceID] < MAX_FAILED_TIMES) {
        failedSubDstRouteOnCallResult_[dstInstanceID]++;
        YRLOG_WARN("subscribe dstInstance({}) for call result from {} failed {} times, retry again", dstInstanceID,
                   srcInstanceID, failedSubDstRouteOnCallResult_[dstInstanceID]);
        auto promise = std::make_shared<litebus::Promise<SharedStreamMsg>>();
        litebus::AsyncAfter(DEFER_RETRY, GetAID(), &InstanceProxy::DeferRetryCallResult, srcInstanceID,
                            dstInstanceID, request, time, promise);
        return promise->GetFuture();
    }
    YRLOG_ERROR("subscribe dstInstance({}) for call result from {} failed {} times, instance not found", dstInstanceID,
                srcInstanceID, failedSubDstRouteOnCallResult_[dstInstanceID]);
    failedSubDstRouteOnCallResult_.erase(dstInstanceID);
    auto response = std::make_shared<runtime_rpc::StreamingMessage>();
    response->set_messageid(request->messageid());
    auto callResultAck = response->mutable_callresultack();
    callResultAck->set_code(Status::GetPosixErrorCode(StatusCode::ERR_INSTANCE_NOT_FOUND));
    callResultAck->set_message("instance not found or instance may not be recovered");
    return response;
}

void InstanceProxy::DeferRetryCallResult(const std::string &srcInstanceID, const std::string &dstInstanceID,
                                         const SharedStreamMsg &request, const std::shared_ptr<TimePoint> &time,
                                         const std::shared_ptr<litebus::Promise<SharedStreamMsg>> &promise)
{
    auto future = CallResult(srcInstanceID, dstInstanceID, request, time);
    promise->Associate(future);
}

void InstanceProxy::OnLocalCallResult(const litebus::Future<SharedStreamMsg> &callResultAckFut,
                                      const SharedStreamMsg &callResult, const std::string &dstInstance,
                                      const std::string &srcInstance)
{
    ASSERT_FS(!callResultAckFut.IsError());
    const auto &callResultAck = callResultAckFut.Get();

    auto &requestID = callResult->callresultreq().requestid();
    perf_->EndRecord(requestID);
    if (remoteDispatchers_.find(dstInstance) != remoteDispatchers_.end() && remoteDispatchers_[dstInstance]) {
        remoteDispatchers_[dstInstance]->OnCallResult(callResultAck, callResult->callresultreq().requestid(),
                                                      callResult->callresultreq().code());
    }
    if (remoteDispatchers_.find(srcInstance) != remoteDispatchers_.end() && remoteDispatchers_[srcInstance]) {
        remoteDispatchers_[srcInstance]->OnCallResult(callResultAck, callResult->callresultreq().requestid(),
                                                      callResult->callresultreq().code());
    }
    if (srcInstance == instanceID_) {
        ASSERT_FS(selfDispatcher_);
        selfDispatcher_->OnCallResult(callResultAck, callResult->callresultreq().requestid(),
                                      callResult->callresultreq().code());
        InvocationHandler::ReleaseEstimateMemory(srcInstance, callResult->callresultreq().requestid());
        return;
    }
    litebus::AID aid(srcInstance, GetAID().Url());
    litebus::Async(aid, &InstanceProxy::OnLocalCallResult, callResultAckFut, callResult, dstInstance,
        srcInstance);
}

void InstanceProxy::ForwardCallResult(const litebus::AID &from, std::string &&, std::string &&msg)
{
    auto srcInstanceID = from.Name();
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    (void)request->ParseFromString(msg);
    ASSERT_FS(request->has_callresultreq());
    auto callResult = request->callresultreq();
    auto &requestID = request->messageid();
    perf_->RecordCallResult(requestID, nullptr);
    YRLOG_INFO("{}|receive forward call result from {}", callResult.requestid(), std::string(from));
    ASSERT_FS(selfDispatcher_);
    (void)selfDispatcher_->CallResult(request).OnComplete(litebus::Defer(
        GetAID(), &InstanceProxy::OnForwardCallResult, std::placeholders::_1, from, request, srcInstanceID));
}

void InstanceProxy::OnForwardCallResult(const litebus::Future<SharedStreamMsg> &callResultAckFut,
                                        const litebus::AID &from, const SharedStreamMsg &callResult,
                                        const std::string &srcInstance)
{
    ASSERT_FS(!callResultAckFut.IsError());
    const auto &callResultAck = callResultAckFut.Get();
    ASSERT_FS(callResultAck->has_callresultack());
    auto &requestID = callResult->callresultreq().requestid();
    perf_->EndRecord(requestID);
    if (remoteDispatchers_.find(srcInstance) != remoteDispatchers_.end() && remoteDispatchers_[srcInstance]) {
        remoteDispatchers_[srcInstance]->OnCallResult(callResultAck, callResult->callresultreq().requestid(),
                                                      callResult->callresultreq().code());
    }
    if (callResult->callresultreq().instanceid() == instanceID_) {
        InvocationHandler::ReleaseEstimateMemory(from.Name(), callResult->callresultreq().requestid());
    }
    callResultAck->set_messageid(callResult->callresultreq().requestid());
    YRLOG_INFO("{}|ready send forward call result response", callResult->callresultreq().requestid());
    Send(from, "ResponseForwardCallResult", callResultAck->SerializeAsString());
}

void InstanceProxy::ResponseForwardCallResult(const litebus::AID &from, std::string &&, std::string &&msg)
{
    auto request = std::make_shared<runtime_rpc::StreamingMessage>();
    (void)request->ParseFromString(msg);
    ASSERT_FS(request->has_callresultack());
    auto &requestID = request->messageid();
    perf_->EndRecord(requestID);
    YRLOG_INFO("receive forward call result response {} from {}", request->messageid(), std::string(from));
    if (auto promise(forwardCallResultPromises_.find(request->messageid()));
        promise != forwardCallResultPromises_.end()) {
        promise->second->SetValue(request);
        (void)forwardCallResultPromises_.erase(promise);
        return;
    }
    YRLOG_WARN("no request {} is waiting for forward callresult ack, ignore it.", request->messageid());
}

litebus::Future<SharedStreamMsg> InstanceProxy::SendForwardCall(const litebus::AID &aid,
                                                                const std::string &callerTenantID,
                                                                const SharedStreamMsg &request)
{
    ASSERT_FS(request->has_callreq());
    const auto &callReq = request->callreq();
    if (callerTenantID.empty()) {
        request->set_messageid(callReq.requestid());
    } else {
        // if enable multi tenant, messageid contains tenantID of src instance, {tenantID}{requestID}
        request->set_messageid(callerTenantID + callReq.requestid());
    }
    auto promiseIter = forwardCallPromises_.find(callReq.requestid());
    std::shared_ptr<litebus::Promise<SharedStreamMsg>> promise;
    bool firstForwardCall = true;
    if (promiseIter == forwardCallPromises_.end()) {
        promise = std::make_shared<litebus::Promise<SharedStreamMsg>>();
        forwardCallPromises_[callReq.requestid()] = promise;
        forwardCallStartTimes_[callReq.requestid()] = std::chrono::steady_clock::now();
    } else {
        promise = promiseIter->second;
        firstForwardCall = false;
    }
    // 1. client first send
    // 2. reliable instance retry send
    if (firstForwardCall || callReq.createoptions().find(YR_ROUTE_KEY) == callReq.createoptions().end()) {
        (void)Send(aid, "ForwardCall", request->SerializeAsString());
        YRLOG_INFO("{}|{}|(forwardInvoke)send forward call", callReq.traceid(), callReq.requestid());
        return promise->GetFuture();
    }

    // low reliable instance retry send
    YRLOG_INFO("{}|{}|low reliable instance retry to forward call", callReq.traceid(), callReq.requestid());
    const auto startIter = forwardCallStartTimes_.find(callReq.requestid());
    if (observer_ != nullptr && startIter != forwardCallStartTimes_.end()
        && std::chrono::steady_clock::now() - startIter->second >= std::chrono::milliseconds(systemTimeoutMs_)
        && forwardCallRouteQueries_.insert(callReq.requestid()).second) {
        YRLOG_WARN("{}|{}|forward call has not received ACK within system timeout, query route for instance {}",
                   callReq.traceid(), callReq.requestid(), aid.Name());
        observer_->QueryInstanceRoute(aid.Name()).OnComplete(
            litebus::Defer(GetAID(), &InstanceProxy::OnRetryQueryRouteResult, aid.Name(), request,
                           std::placeholders::_1));
        return promise->GetFuture();
    }

    SendForwardCallToRequestRouter(aid, request);
    return promise->GetFuture();
}

void InstanceProxy::SendForwardCallToRequestRouter(const litebus::AID &aid, const SharedStreamMsg &request)
{
    auto newAid = litebus::AID(REQUEST_ROUTER_NAME, aid.Url());
    internal::RouteCallRequest routeReq;
    routeReq.mutable_req()->CopyFrom(*request);
    routeReq.set_instanceid(aid.Name());
    (void)Send(newAid, "ForwardCall", routeReq.SerializeAsString());
}

void InstanceProxy::OnRetryQueryRouteResult(
    const std::string &dstInstanceID, const SharedStreamMsg &request,
    const litebus::Future<std::shared_ptr<resources::RouteInfo>> &routeFuture)
{
    const auto &callReq = request->callreq();
    const auto &requestID = callReq.requestid();
    forwardCallRouteQueries_.erase(requestID);
    if (forwardCallPromises_.find(requestID) == forwardCallPromises_.end()) {
        return;
    }

    if (routeFuture.IsError()) {
        const auto code = QueryRouteErrorCode(routeFuture.GetErrorCode());
        const auto message = code == common::ErrorCode::ERR_INSTANCE_NOT_FOUND
                                 ? "instance is not found, maybe be not created or killed"
                                 : "failed to query instance route, please retry.";
        YRLOG_WARN("{}|{}|failed to query route for instance {}, error code {}", callReq.traceid(), requestID,
                   dstInstanceID, routeFuture.GetErrorCode());
        CompleteForwardCallWithError(requestID, code, message);
        return;
    }

    const auto &routeInfo = routeFuture.Get();
    if (routeInfo == nullptr) {
        CompleteForwardCallWithError(requestID, common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                                     "failed to query instance route, please retry.");
        return;
    }

    const auto state = static_cast<InstanceState>(routeInfo->instancestatus().code());
    if (IsTerminalStatus(state)) {
        auto code = routeInfo->instancestatus().errcode() == 0
                        ? common::ErrorCode::ERR_INSTANCE_NOT_FOUND
                        : static_cast<common::ErrorCode>(routeInfo->instancestatus().errcode());
        auto message = routeInfo->instancestatus().msg().empty()
                           ? "instance has been killed or exited."
                           : routeInfo->instancestatus().msg();
        YRLOG_WARN("{}|{}|instance {} is in terminal state {}", callReq.traceid(), requestID, dstInstanceID,
                   routeInfo->instancestatus().code());
        CompleteForwardCallWithError(requestID, code, message);
        return;
    }

    if (routeInfo->functionproxyid().empty()) {
        CompleteForwardCallWithError(requestID, common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                                     "instance proxy is empty, please retry.");
        return;
    }

    observer_->QueryProxyAddress(routeInfo->functionproxyid())
        .OnComplete(litebus::Defer(GetAID(), &InstanceProxy::OnRetryQueryProxyAddressResult, dstInstanceID, request,
                                   std::placeholders::_1));
}

void InstanceProxy::OnRetryQueryProxyAddressResult(const std::string &dstInstanceID, const SharedStreamMsg &request,
                                                   const litebus::Future<std::string> &addressFuture)
{
    const auto &callReq = request->callreq();
    const auto &requestID = callReq.requestid();
    if (forwardCallPromises_.find(requestID) == forwardCallPromises_.end()) {
        return;
    }
    if (addressFuture.IsError() || addressFuture.Get().empty()) {
        CompleteForwardCallWithError(requestID, common::ErrorCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                                     "instance proxy route is unavailable, please retry.");
        return;
    }

    YRLOG_INFO("{}|{}|forward call through current route {} for instance {}", callReq.traceid(), requestID,
               addressFuture.Get(), dstInstanceID);
    auto routeRefreshRequest = std::make_shared<runtime_rpc::StreamingMessage>(*request);
    (*routeRefreshRequest->mutable_callreq()->mutable_createoptions())[RETURN_ROUTE_INFO_KEY] = "true";
    SendForwardCallToRequestRouter(litebus::AID(dstInstanceID, addressFuture.Get()), routeRefreshRequest);
}

void InstanceProxy::AttachCurrentRouteInfoForRefreshedCall(core_service::CallResult *callResult)
{
    if (callResult == nullptr || returnRouteInfoRequestIDs_.erase(callResult->requestid()) == 0 || proxyID_.empty()) {
        return;
    }
    // This proxy is the target that accepted the refreshed call. Attach its authoritative route to the result;
    // the caller-side libruntime updates its cache when it consumes the result.
    auto runtimeInfo = callResult->mutable_runtimeinfo();
    runtimeInfo->set_route(GetAID().Url());
    runtimeInfo->set_proxyid(proxyID_);
}

void InstanceProxy::CompleteForwardCallWithError(const std::string &requestID, const common::ErrorCode &code,
                                                 const std::string &message)
{
    const auto promiseIter = forwardCallPromises_.find(requestID);
    if (promiseIter == forwardCallPromises_.end()) {
        return;
    }
    auto promise = promiseIter->second;
    forwardCallPromises_.erase(promiseIter);
    EraseForwardCallContext(requestID);
    promise->SetValue(CreateCallResponse(code, message, requestID));
}

void InstanceProxy::EraseForwardCallContext(const std::string &requestID)
{
    forwardCallStartTimes_.erase(requestID);
    forwardCallRouteQueries_.erase(requestID);
}

litebus::Future<SharedStreamMsg> InstanceProxy::SendForwardCallResult(const litebus::AID &aid,
                                                                      const SharedStreamMsg &request)
{
    ASSERT_FS(request->has_callresultreq());
    auto promise = std::make_shared<litebus::Promise<SharedStreamMsg>>();
    request->set_messageid(request->callresultreq().requestid());
    forwardCallResultPromises_[request->messageid()] = promise;
    YRLOG_INFO("{}|(forwardCallResult)send forward callresult to {}", request->callresultreq().requestid(),
               aid.HashString());
    // send forwardCallResult request to another proxy actor
    Send(aid, "ForwardCallResult", request->SerializeAsString());
    return promise->GetFuture();
}

void InstanceProxy::NotifyChanged(const std::string &instanceID, const std::shared_ptr<InstanceRouterInfo> &info)
{
    ASSERT_IF_NULL(info);
    if (function_proxy::DirectRoutingConfig::IsEnabled() && !info->proxyGrpcAddress.empty()) {
        routeCache_.Put(instanceID, info->proxyGrpcAddress);
    }
    if (instanceID == instanceID_) {
        ASSERT_FS(selfDispatcher_);
        selfDispatcher_->UpdateInfo(info);
        if (!info->isLocal && info->isReady) {
            // MigratingRequest is already running,
            // notify observer to terminate actor because of instance has been remote
            YRLOG_INFO("instance {} is already migrated to {}, instance proxy on local should be terminate", instanceID,
                       info->proxyID);
            RETURN_IF_NULL(observer_);
            observer_->NotifyMigratingRequest(instanceID_);
        }
        return;
    }
    if (remoteDispatchers_.find(instanceID) == remoteDispatchers_.end()) {
        auto dispatcher = std::make_shared<RequestDispatcher>(instanceID, false, "", shared_from_this(), perf_);
        remoteDispatchers_[instanceID] = dispatcher;
    }
    remoteDispatchers_[instanceID]->UpdateInfo(info);
}

void InstanceProxy::Fatal(const std::string &instanceID, const std::string &message, const StatusCode &code)
{
    if (instanceID == instanceID_) {
        ASSERT_FS(selfDispatcher_);
        selfDispatcher_->Fatal(message, code);
        return;
    }
    if (remoteDispatchers_.find(instanceID) != remoteDispatchers_.end() && remoteDispatchers_[instanceID]) {
        remoteDispatchers_[instanceID]->Fatal(message, code);
    }
}

std::list<litebus::Future<SharedStreamMsg>> InstanceProxy::GetOnRespFuture()
{
    ASSERT_FS(selfDispatcher_);
    return selfDispatcher_->GetOnRespFuture();
}

void InstanceProxy::DeleteRemoteDispatcher(const std::string &instanceID)
{
    if (remoteDispatchers_.find(instanceID) != remoteDispatchers_.end() && remoteDispatchers_[instanceID]) {
        remoteDispatchers_[instanceID]->Fatal(INSTANCE_EXIT_MESSAGE, StatusCode::ERR_INSTANCE_EXITED);
    }
    (void)remoteDispatchers_.erase(instanceID);
}

bool InstanceProxy::Delete()
{
    ASSERT_FS(selfDispatcher_);
    selfDispatcher_->Fatal(INSTANCE_EXIT_MESSAGE, StatusCode::ERR_INSTANCE_EXITED);
    return true;
}

bool InstanceProxy::FailHeldRetryable(const std::string &message)
{
    ASSERT_FS(selfDispatcher_);
    // W18-P2: ERR_INSTANCE_BUSY — the client stack's resilience layer retries
    // busy invokes in place; the parked instance's wake (directed or FIFO)
    // eventually restores and the retry lands. Never terminal.
    selfDispatcher_->Fatal(message, StatusCode::ERR_INSTANCE_BUSY);
    return true;
}

void InstanceProxy::InitDispatcher()
{
    selfDispatcher_ = std::make_shared<RequestDispatcher>(instanceID_, true, tenantID_, shared_from_this(), perf_);
}

litebus::Future<SharedStreamMsg> InstanceProxyWrapper::Call(const litebus::AID &to,
                                                            const CallerInfo &callerInfo,
                                                            const std::string &instanceID,
                                                            const SharedStreamMsg &request,
                                                            const std::shared_ptr<TimePoint> &time)
{
    return litebus::Async(to, &InstanceProxy::Call, callerInfo, instanceID, request, time);
}

litebus::Future<SharedStreamMsg> InstanceProxyWrapper::CallResult(const litebus::AID &to,
                                                                  const std::string &srcInstanceID,
                                                                  const std::string &dstInstanceID,
                                                                  const SharedStreamMsg &request,
                                                                  const std::shared_ptr<TimePoint> &time)
{
    return litebus::Async(to, &InstanceProxy::CallResult, srcInstanceID, dstInstanceID, request, time);
}

litebus::Future<SharedStreamMsg> InstanceProxyWrapper::CompleteFrontendCall(const litebus::AID &to,
                                                                            const SharedStreamMsg &callResult,
                                                                            const SharedStreamMsg &callResultAck)
{
    return litebus::Async(to, &InstanceProxy::CompleteFrontendCall, callResult, callResultAck);
}

litebus::Future<std::string> InstanceProxyWrapper::GetTenantID(const litebus::AID &to)
{
    return litebus::Async(to, &InstanceProxy::GetTenantID);
}

}  // namespace functionsystem::busproxy
