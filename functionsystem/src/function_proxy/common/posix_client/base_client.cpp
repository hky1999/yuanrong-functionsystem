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
#include "base_client.h"

#include "common/logs/logging.h"
#include "common/status/status.h"

namespace functionsystem {
using namespace runtime_rpc;

const uint32_t MAX_RETRY = 5;
const uint32_t INIT_CALL_TIMEOUT_MS = 5000;

void HandleCallResponse(const litebus::Future<StreamingMessage> &resp,
                        const std::shared_ptr<litebus::Promise<runtime::CallResponse>> &promise)
{
    runtime::CallResponse callRsp{};
    if (resp.IsError()) {
        promise->SetFailed(static_cast<int32_t>(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS));
        return;
    }
    callRsp.CopyFrom(resp.Get().callrsp());
    promise->SetValue(callRsp);
}

SharedStreamMsg CallRspMessage(const StatusCode &code, const std::string &message, const std::string &messageID)
{
    auto msg = std::make_shared<StreamingMessage>();
    msg->set_messageid(messageID);
    auto callRsp = msg->mutable_callrsp();
    callRsp->set_code(Status::GetPosixErrorCode(code));
    callRsp->set_message(message);
    return msg;
}

void BaseClient::Start()
{
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    if (posix_ != nullptr) {
        posix_->Start();
    }
}

void BaseClient::Close() noexcept
{
    std::unique_lock<std::shared_mutex> lock(rwMut_);
    if (posix_ != nullptr) {
        posix_->Stop();
        posix_ = nullptr;
    }
    runtimeID_.clear();
}

void BaseClient::RegisterUserCallback(const std::function<void()> &userCb)
{
    std::unique_lock<std::shared_mutex> lock(rwMut_);
    userCb_ = userCb;
    if (posix_ != nullptr) {
        posix_->RegisterUserCallback(userCb_);
    }
}

void BaseClient::UpdatePosix(const std::shared_ptr<grpc::PosixClient> &posix, const std::string &runtimeID)
{
    std::unique_lock<std::shared_mutex> lock(rwMut_);
    if (posix_ != nullptr) {
        posix_->Stop();
    }
    posix_ = posix;
    if (posix_ != nullptr && userCb_ != nullptr) {
        posix_->RegisterUserCallback(userCb_);
    }
    // Retag atomically with the transport swap: a runtime-pinned send already
    // holding the shared lock sees either the old pair or the new pair, never
    // old transport + new label (or vice versa). An empty runtimeID (legacy
    // one-argument call) deliberately invalidates the binding.
    runtimeID_ = runtimeID;
}

litebus::Future<runtime_rpc::StreamingMessage> BaseClient::Send(
    const std::shared_ptr<runtime_rpc::StreamingMessage> &request, uint32_t retryTimes, uint32_t timeOutMs)
{
    const std::string requestID = request->callreq().requestid();
    auto promise = std::make_shared<litebus::Promise<StreamingMessage>>();
    if (retryTimes > MAX_RETRY) {
        YRLOG_ERROR("{}|failed to send call to runtime, after max retry times({})", requestID, MAX_RETRY);
        promise->SetFailed(static_cast<int32_t>(StatusCode::REQUEST_TIME_OUT));
        return promise->GetFuture();
    }
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    if (posix_ == nullptr) {
        promise->SetFailed(static_cast<int32_t>(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS));
        return promise->GetFuture();
    }
    (void)posix_->Send(request)
        .After(timeOutMs,
               [this, request, retryTimes, timeOutMs, requestID](const litebus::Future<StreamingMessage> &) {
                   YRLOG_ERROR("{}|failed to send call to runtime, reason(timeout), begin to retry, times({})",
                               requestID, retryTimes);
                   const uint32_t times = retryTimes + 1;
                   return Send(const_cast<std::shared_ptr<StreamingMessage> &>(request), times, timeOutMs);
               })
        .OnComplete([promise, requestID](const litebus::Future<StreamingMessage> &resp) {
            if (resp.IsError()) {
                YRLOG_ERROR("{}|failed to send call to runtime, error code({})", requestID, resp.GetErrorCode());
                promise->SetFailed(resp.GetErrorCode());
            } else {
                promise->Associate(resp);
            }
        });
    return promise->GetFuture();
}

litebus::Future<runtime::CallResponse> BaseClient::InitCall(const std::shared_ptr<runtime::CallRequest> &request,
                                                            uint32_t timeOutMs = INIT_CALL_TIMEOUT_MS)
{
    auto promise = std::make_shared<litebus::Promise<runtime::CallResponse>>();

    auto msg = std::make_shared<StreamingMessage>();
    *msg->mutable_callreq() = *request;
    msg->set_messageid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    auto requestID = msg->callreq().requestid();
    msg->mutable_callreq()->set_requestid(requestID + "@initcall");

    (void)Send(msg, 0, timeOutMs).OnComplete(std::bind(&HandleCallResponse, std::placeholders::_1, promise));
    return promise->GetFuture();
}

litebus::Future<SharedStreamMsg> BaseClient::Call(const SharedStreamMsg &request)
{
    auto promise = std::make_shared<litebus::Promise<SharedStreamMsg>>();
    request->set_messageid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    if (posix_ == nullptr) {
        return CallRspMessage(StatusCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                              "connection with runtime may be interrupted, please retry.", request->messageid());
    }
    auto rspFuture = posix_->Send(request);
    (void)rspFuture.OnComplete([promise, request](const litebus::Future<StreamingMessage> &resp) {
        if (resp.IsError()) {
            auto response =
                CallRspMessage(StatusCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS,
                               "connection with runtime may be interrupted, please retry.", request->messageid());
            promise->SetValue(response);
            return;
        }
        auto rsp = std::make_shared<StreamingMessage>(resp.Get());
        promise->SetValue(rsp);
    });
    return promise->GetFuture();
}

litebus::Future<runtime::NotifyResponse> BaseClient::NotifyResult(runtime::NotifyRequest &&request)
{
    auto promise = std::make_shared<litebus::Promise<runtime::NotifyResponse>>();
    auto msg = std::make_shared<StreamingMessage>();
    *msg->mutable_notifyreq() = std::move(request);
    msg->set_messageid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    if (posix_ == nullptr) {
        promise->SetFailed(static_cast<int32_t>(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS));
        return promise->GetFuture();
    }
    auto rspFuture = posix_->Send(msg);
    (void)rspFuture.OnComplete([promise](const litebus::Future<StreamingMessage> &resp) {
        runtime::NotifyResponse notifyRsp{};
        if (resp.IsError()) {
            promise->SetFailed(resp.GetErrorCode());
            return;
        }
        notifyRsp.CopyFrom(resp.Get().notifyrsp());
        promise->SetValue(notifyRsp);
    });
    return promise->GetFuture();
}

bool BaseClient::IsDone()
{
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    if (posix_ == nullptr) {
        return true;
    }
    return posix_->IsDone();
}

}  // namespace functionsystem
