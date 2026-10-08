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
#include "control_client.h"

#include <grpcpp/create_channel.h>

#include <chrono>
#include <thread>

#include "common/constants/constants.h"
#include "common/logs/logging.h"
#include "common/status/status.h"

using namespace runtime_rpc;

namespace functionsystem::grpc {

const std::string INSTANCE_ID_META = "instance_id";
const int32_t SIZE_MEGA_BYTES = 1024 * 1024;
const int32_t DEFAULT_GRPC_MAX_SIZE = 4;
const int32_t GRPC_MAX_SIZE_LIMIT = 500;

std::shared_ptr<ControlClient> PosixControlWrapper::InitPosixStream(const std::string &instanceID,
                                                                    const std::string &runtimeID,
                                                                    const ControlClientConfig &config)
{
    return std::make_shared<ControlClient>(instanceID, runtimeID, config);
}

ControlClient::ControlClient(const std::string &instanceID, const std::string &runtimeID,
                             const ControlClientConfig &config)
    : instanceID_(instanceID), runtimeID_(runtimeID), boundRequestID_(config.boundRequestID),
      isStopped_(false)
{
    int maxGrpcSize = config.maxGrpcSize;
    if (maxGrpcSize <= 0) {
        YRLOG_WARN("invalid max grpc size {}, smaller than 0, set to default {}", maxGrpcSize, DEFAULT_GRPC_MAX_SIZE);
        maxGrpcSize = DEFAULT_GRPC_MAX_SIZE;
    }
    if (maxGrpcSize > GRPC_MAX_SIZE_LIMIT) {
        YRLOG_WARN("invalid max grpc size {}, bigger than limit({}), set to limit", maxGrpcSize, GRPC_MAX_SIZE_LIMIT);
        maxGrpcSize = DEFAULT_GRPC_MAX_SIZE;
    }
    try {
        ::grpc::ChannelArguments args;
        args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, RECONNECT_BACKOFF_INTERVAL);
        args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, RECONNECT_BACKOFF_INTERVAL);
        args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, RECONNECT_BACKOFF_INTERVAL);
        args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, maxGrpcSize * SIZE_MEGA_BYTES);
        args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, maxGrpcSize * SIZE_MEGA_BYTES);
        auto channel = ::grpc::CreateCustomChannel(config.target, config.creds, args);
        auto tmout = gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC), { config.timeoutSec, 0, GPR_TIMESPAN });
        if (channel == nullptr || !channel->WaitForConnected(tmout)) {
            YRLOG_WARN("channel is nullptr {} or ControlClient WaitForConnected address:{} Failed, tv_sec is {}",
                       channel == nullptr, config.target, tmout.tv_sec);
            reactor_ = nullptr;
            return;
        }
        YRLOG_WARN("ControlClient Connected {} address:{}", runtimeID, config.target);
        reactor_ = std::make_shared<ClientReactor>();
        reactor_->RegisterReceiver(std::bind(&ControlClient::Receiver, this, std::placeholders::_1));
        reactor_->RegisterClosedCallback(std::bind(&ControlClient::ClientClosedCallback, this));
        reactor_->SetID(runtimeID_);
        stub_ = runtime_rpc::RuntimeRPC::NewStub(channel);
        context_.AddMetadata(INSTANCE_ID_META, instanceID);
        stub_->async()->MessageStream(&context_, reactor_.get());
    } catch (std::exception &e) {
        YRLOG_ERROR(
            "failed to establish grpc connection between LocalScheduler and instance({})-runtime({}), exception({})",
            instanceID, runtimeID, e.what());
        reactor_ = nullptr;
    }
}

void ControlClient::Start()
{
    if (reactor_ == nullptr) {
        YRLOG_WARN("posix client is not created {}", runtimeID_);
        return;
    }

    isRunning_ = true;

    // a read-flow and a write-flow taking place outside the
    // reactions, call AddMultipleHolds(2) before StartCall
    reactor_->AddMultipleHolds(2);
    reactor_->Read();
    reactor_->StartCall();
}

void ControlClient::ClientClosedCallback()
{
    std::unique_lock<std::mutex> lock(mut_);
    for (auto &[msgID, promise] : promises_) {
        YRLOG_WARN("instance({}) runtime({}) control stream closed, recvMsgID {} failed", instanceID_, runtimeID_,
                   msgID);
        promise->SetFailed(static_cast<int32_t>(StatusCode::ERR_REQUEST_BETWEEN_RUNTIME_BUS));
    }
    promises_.clear();
    if (userCallback_ && !isStopped_) {
        userCallback_();
    }
}

void ControlClient::Receiver(const std::shared_ptr<StreamingMessage> &recv)
{
    auto recvMsgID = recv->messageid();
    auto bodyType = recv->body_case();
    bool debug = (bodyType != StreamingMessage::kHeartbeatReq) && (bodyType != StreamingMessage::kHeartbeatRsp);
    YRLOG_DEBUG_IF(debug, "{}-{} posix stream msg type, body {} messageID {}", instanceID_, runtimeID_,
                   fmt::underlying(bodyType), recvMsgID);
    if (reactor_ == nullptr || reactor_->IsDone()) {
        YRLOG_ERROR("instance {} {} posix stream is already failed, unable to receive msg", instanceID_, runtimeID_);
        return;
    }
    {
        std::unique_lock<std::mutex> lock(mut_);
        if (auto it(promises_.find(recvMsgID)); it != promises_.end()) {
            it->second->SetValue(*recv);
            (void)promises_.erase(it);
            return;
        }
    }

    if (auto iter(PosixClient::handlers_.find(bodyType)); iter == PosixClient::handlers_.end()) {
        YRLOG_WARN("{} invalid control stream msg type, recvMsgID {}", fmt::underlying(bodyType), recvMsgID);
        return;
    }
    auto future = PosixClient::handlers_[bodyType](
        PosixStreamIdentity{ instanceID_, runtimeID_, boundRequestID_ }, recv);
    (void)future.OnComplete(
        [debug, recvMsgID, reactor(this->reactor_), instanceID(this->instanceID_),
         runtimeID(this->runtimeID_)](const litebus::Future<std::shared_ptr<StreamingMessage>> &future) {
            if (future.IsError()) {
                return;
            }
            const auto &resp = future.Get();
            resp->set_messageid(recvMsgID);
            (void)reactor->Write(resp, debug)
                .OnComplete([instanceID, runtimeID, recvMsgID](const litebus::Future<bool> &fut) {
                    auto isSuccess = fut.Get();
                    if (!isSuccess) {
                        YRLOG_ERROR("{}-{} Write failed, recvMsgID {}", instanceID, runtimeID, recvMsgID);
                    }
                });
        });
}

litebus::Future<StreamingMessage> ControlClient::Send(const std::shared_ptr<StreamingMessage> &request)
{
    auto sendPromise = std::make_shared<litebus::Promise<StreamingMessage>>();
    if (request == nullptr) {
        YRLOG_ERROR("instance {} request is nullptr", instanceID_);
        sendPromise->SetFailed(static_cast<int32_t>(StatusCode::GRPC_STREAM_CALL_ERROR));
        return sendPromise->GetFuture();
    }
    auto bodyType = request->body_case();
    bool debug = (bodyType != StreamingMessage::kHeartbeatReq) && (bodyType != StreamingMessage::kHeartbeatRsp);
    YRLOG_DEBUG_IF(debug, "posix stream send msg to {}-{}, type {} messageID {}", instanceID_, runtimeID_,
                   fmt::underlying(request->body_case()), request->messageid());
    if (reactor_ == nullptr || reactor_->IsDone()) {
        YRLOG_ERROR("instance {} posix stream is already failed, posix reactor is {}, unable to send msg", instanceID_,
                    reactor_ == nullptr ? "Null" : "Done");
        sendPromise->SetFailed(static_cast<int32_t>(StatusCode::GRPC_STREAM_CALL_ERROR));
        return sendPromise->GetFuture();
    }
    {
        std::unique_lock<std::mutex> lk(mut_);
        auto emplaceRes = promises_.emplace(request->messageid(), sendPromise);
        if (!emplaceRes.second) {
            YRLOG_DEBUG("instance {}-{} duplicate request's messageID {}, returning previous future", instanceID_,
                        runtimeID_, request->messageid());
            return promises_[request->messageid()]->GetFuture();
        }
    }
    return reactor_->Write(request, debug)
        .Then([this, bodyType, sendPromise, messageID(request->messageid())](const litebus::Future<bool> &fut) {
            auto isSuccess = fut.Get();
            if (!isSuccess) {
                YRLOG_ERROR("instance {}-{} posix stream connection has been failed!", instanceID_, runtimeID_);
                sendPromise->SetFailed(static_cast<int32_t>(StatusCode::GRPC_STREAM_CALL_ERROR));
                {
                    std::unique_lock<std::mutex> lock(mut_);
                    (void)promises_.erase(messageID);
                }
            }
            YRLOG_DEBUG_IF(bodyType != StreamingMessage::kHeartbeatReq && bodyType != StreamingMessage::kHeartbeatRsp,
                           "posix stream send msg to {}-{}, type {} messageID {} finished. success({})", instanceID_,
                           runtimeID_, fmt::underlying(bodyType), messageID, isSuccess);
            return sendPromise->GetFuture();
        });
}

void ControlClient::Stop()
{
    {
        std::unique_lock<std::mutex> lk(mut_);
        isStopped_ = true;
    }
    if (IsDone() || !isRunning_) {
        return;
    }
    reactor_->TryStop(context_);
}

bool ControlClient::IsDone()
{
    return reactor_ == nullptr || reactor_->IsDone();
}
}  // namespace functionsystem::grpc