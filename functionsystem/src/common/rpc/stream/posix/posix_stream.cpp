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

#include "posix_stream.h"

#include "common/rpc/stream/posix_reactor.h"
#include "common/status/status.h"

namespace functionsystem::grpc {
using namespace runtime_rpc;

PosixStream::PosixStream(const std::shared_ptr<ServerReactor> &reactor, ::grpc::CallbackServerContext *context,
                         const std::string &instanceID, const std::string &runtimeID)
{
    instanceID_ = instanceID;
    runtimeID_ = runtimeID;
    reactor_ = reactor;
    reactor_->RegisterClosedCallback([this]() { PosixStreamClosedCallback(); });
    reactor_->RegisterReceiver(std::bind(&PosixStream::Receiver, this, std::placeholders::_1));
    reactor_->SetID(runtimeID_);
    reactor_->Read();
    context_ = context;
}

PosixStream::~PosixStream()
{
    Stop();
    context_ = nullptr;
    reactor_ = nullptr;
}

void PosixStream::Start()
{
}

void PosixStream::Stop()
{
    {
        std::unique_lock<std::mutex> lk(mut_);
        isStopped_ = true;
    }
    if (IsDone()) {
        return;
    }

    if (context_ != nullptr && !context_->IsCancelled()) {
        context_->TryCancel();
    }

    if (reactor_ != nullptr) {
        reactor_->Wait();
    }
}

bool PosixStream::IsDone()
{
    return reactor_ == nullptr || reactor_->IsDone() || context_ == nullptr;
}

litebus::Future<StreamingMessage> PosixStream::Send(const std::shared_ptr<StreamingMessage> &request)
{
    ASSERT_IF_NULL(request);
    auto sendMsgID = request->messageid();
    auto bodyType = request->body_case();
    bool notHeartbeat = (bodyType != StreamingMessage::kHeartbeatReq) && (bodyType != StreamingMessage::kHeartbeatRsp);
    YRLOG_DEBUG_IF(notHeartbeat, "{}|{}|posix stream gonna send msg, body type: {}, msgID: {}", instanceID_, runtimeID_,
                   fmt::underlying(bodyType), sendMsgID);
    auto sendPromise = std::make_shared<litebus::Promise<StreamingMessage>>();
    if (reactor_ == nullptr || reactor_->IsDone() || context_ == nullptr) {
        YRLOG_ERROR("{}|{}|posix stream is already failed, unable to send msg", instanceID_, runtimeID_);
        sendPromise->SetFailed(static_cast<int32_t>(StatusCode::GRPC_STREAM_CALL_ERROR));
        return sendPromise->GetFuture();
    }
    {
        std::lock_guard<std::mutex> lock(msgMutex_);
        auto emplaceRes = msgPromises_.emplace(sendMsgID, sendPromise);
        if (!emplaceRes.second) {
            YRLOG_DEBUG("{}|{}|posix stream send duplicate msgID {}, returning previous future", instanceID_,
                        runtimeID_, sendMsgID);
            return msgPromises_[sendMsgID]->GetFuture();
        }
    }

    auto doSend = [self(shared_from_this()), request, bodyType, sendPromise, sendMsgID, notHeartbeat]() {
        return self->reactor_->Write(request, notHeartbeat)
            .Then([self, bodyType, sendPromise, sendMsgID, notHeartbeat](const litebus::Future<bool> &future) {
                if (!future.Get()) {
                    YRLOG_ERROR("{}|{}|posix stream connection failed!", self->instanceID_, self->runtimeID_);
                    if (!sendPromise->GetFuture().IsOK()) {
                        sendPromise->SetFailed(static_cast<int32_t>(StatusCode::GRPC_STREAM_CALL_ERROR));
                    }
                    {
                        std::unique_lock<std::mutex> lock(self->msgMutex_);
                        (void)self->msgPromises_.erase(sendMsgID);
                    }
                } else {
                    YRLOG_DEBUG_IF(notHeartbeat, "{}|{}|posix stream send msg succeed, msg type: {}, msgID: {}.",
                                   self->instanceID_, self->runtimeID_, fmt::underlying(bodyType), sendMsgID);
                }
                return sendPromise->GetFuture();
            });
    };

    if (interceptor_ == nullptr || !notHeartbeat) {
        // interceptor == nullptr or is heartbeat, skip sign
        return doSend();
    }

    return interceptor_->Sign(request).Then([self(shared_from_this()), sendMsgID, sendPromise, doSend](bool ok) {
        if (!ok) {
            YRLOG_ERROR("failed to sign message({})", sendMsgID);
            sendPromise->SetFailed(static_cast<int32_t>(StatusCode::GRPC_UNAUTHENTICATED));
            {
                std::unique_lock<std::mutex> lock(self->msgMutex_);
                (void)self->msgPromises_.erase(sendMsgID);
            }
            return sendPromise->GetFuture();
        }
        return doSend();
    });
}

void PosixStream::Receiver(const std::shared_ptr<StreamingMessage> &recv)
{
    ASSERT_IF_NULL(recv);
    auto recvMsgID = recv->messageid();
    auto bodyType = recv->body_case();
    bool notHeartbeat = (bodyType != StreamingMessage::kHeartbeatReq) && (bodyType != StreamingMessage::kHeartbeatRsp);
    YRLOG_DEBUG_IF(notHeartbeat, "{}|{}|posix stream receive msg, body type: {}, msgID: {}", instanceID_, runtimeID_,
                   fmt::underlying(bodyType), recvMsgID);
    if (reactor_ == nullptr || reactor_->IsDone() || context_ == nullptr) {
        YRLOG_ERROR("{}|{}|posix stream is already failed, unable to receive msg", instanceID_, runtimeID_);
        return;
    }

    if (HandlerResponse(recv, recvMsgID, notHeartbeat)) {
        return;
    }

    if (auto iter(PosixClient::handlers_.find(bodyType)); iter == PosixClient::handlers_.end()) {
        YRLOG_WARN("{}|{}|invalid posix msg type ({}), msgID {}", instanceID_, runtimeID_, fmt::underlying(bodyType),
                   recvMsgID);
        return;
    }

    auto doSend = [instanceID(instanceID_), runtimeID(runtimeID_), recvMsgID, notHeartbeat,
                   self(shared_from_this())](const std::shared_ptr<StreamingMessage> &resp) {
        self->reactor_->Write(resp, notHeartbeat)
            .OnComplete([instanceID, runtimeID, recvMsgID](const litebus::Future<bool> &fut) {
                if (!fut.Get()) {
                    YRLOG_ERROR("{}|{}|posix stream write failed, msgID {}", instanceID, runtimeID, recvMsgID);
                }
            });
    };

    auto doReceive = [instanceID(instanceID_), bodyType, recv, recvMsgID, doSend, notHeartbeat,
                      self(shared_from_this())]() {
        PosixClient::handlers_[bodyType](instanceID, recv)
            .OnComplete([self, notHeartbeat, recvMsgID,
                         doSend](const litebus::Future<std::shared_ptr<StreamingMessage>> &future) {
                if (future.IsError()) {
                    return;
                }
                auto resp = future.Get();
                resp->set_messageid(recvMsgID);

                if (self->interceptor_ == nullptr || !notHeartbeat) {
                    // interceptor == nullptr or is heartbeat, skip sign
                    doSend(resp);
                    return;
                }

                self->interceptor_->Sign(resp).OnComplete([doSend, recvMsgID, resp](const litebus::Future<bool> &ok) {
                    if (ok.IsError() || !ok.Get()) {
                        YRLOG_ERROR("failed to sign response message({})", recvMsgID);
                        return;
                    }
                    doSend(resp);
                });
            });
    };

    if (interceptor_ == nullptr || !notHeartbeat) {
        // interceptor == nullptr or is heartbeat, skip verify
        doReceive();
        return;
    }

    interceptor_->Verify(recv).OnComplete([doReceive, recvMsgID](const litebus::Future<bool> &ok) {
        if (ok.IsError() || !ok.Get()) {
            YRLOG_ERROR("failed to verify message({})", recvMsgID);
            return;
        }
        doReceive();
    });
}

bool PosixStream::HandlerResponse(const std::shared_ptr<runtime_rpc::StreamingMessage> &recv,
                                  const std::string &recvMsgID, bool notHeartbeat)
{
    {
        std::unique_lock<std::mutex> lock(msgMutex_);
        auto it(msgPromises_.find(recvMsgID));
        if (it == msgPromises_.end()) {
            return false;
        }
        if (interceptor_ == nullptr || !notHeartbeat) {
            // interceptor == nullptr or is heartbeat, skip verify
            it->second->SetValue(*recv);
            (void)msgPromises_.erase(it);
            return true;
        }
    }

    interceptor_->Verify(recv).OnComplete([self(shared_from_this()), recv, recvMsgID](const litebus::Future<bool> &ok) {
        if (ok.IsError() || !ok.Get()) {
            YRLOG_ERROR("failed to verify message({})", recvMsgID);
            return;
        }

        {
            std::unique_lock<std::mutex> lock(self->msgMutex_);
            if (auto it(self->msgPromises_.find(recvMsgID)); it != self->msgPromises_.end()) {
                it->second->SetValue(*recv);
                (void)self->msgPromises_.erase(it);
            }
        }
    });
    return true;
}

void PosixStream::PosixStreamClosedCallback()
{
    {
        std::unique_lock<std::mutex> lock(msgMutex_);
        for (auto &[msgID, promise] : msgPromises_) {
            YRLOG_WARN("instance({}) runtime({}) control stream closed, recvMsgID {} failed", instanceID_, runtimeID_,
                       msgID);
            promise->SetFailed(static_cast<int32_t>(StatusCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS));
        }
        msgPromises_.clear();
    }
    if (userCallback_ != nullptr && !isStopped_) {
        userCallback_();
    }
}

}  // namespace functionsystem::grpc
