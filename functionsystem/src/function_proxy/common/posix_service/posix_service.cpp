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

#include "posix_service.h"

#include "common/aksk/aksk_util.h"
#include "common/logs/logging.h"
#include "common/posix_auth_interceptor/posix_auth_interceptor.h"
#include "common/proto/pb/posix/runtime_rpc.grpc.pb.h"
#include "common/proto/pb/posix_pb.h"

namespace functionsystem {
using namespace runtime_rpc;
namespace {
constexpr const char *FUNCTION_PROXY_STREAM_ID = "function-proxy";
constexpr const char *EVENT_STREAM_ROLE = "event";
}
::grpc::ServerBidiReactor<StreamingMessage, StreamingMessage> *FailureReactor(const ::grpc::Status &status)
{
    class Reactor : public ::grpc::ServerBidiReactor<StreamingMessage, StreamingMessage> {
    public:
        explicit Reactor(const ::grpc::Status &status) { this->Finish(status); }
        void OnDone() override { delete this; }
    };
    return new Reactor(status);
}

::grpc::ServerBidiReactor<StreamingMessage, StreamingMessage> *PosixService::MessageStream(
    ::grpc::CallbackServerContext *context)
{
    if (!context) {
        YRLOG_ERROR("PosixService receive client connect request with null context, reject connect");
        return FailureReactor(::grpc::Status(::grpc::StatusCode::CANCELLED, "nil context"));
    }

    auto metaData = GetMetaData(context);
    if (metaData.instanceID.empty() || metaData.runtimeID.empty()) {
        YRLOG_ERROR(
            "PosixService receive client connect request without instance id({}) or runtime id({}), "
            "reject connect",
            metaData.instanceID, metaData.runtimeID);
        return FailureReactor(::grpc::Status(::grpc::StatusCode::CANCELLED,
                                             "connect request without instance id or runtime id"));
    }

    YRLOG_INFO("PosixService receive MessageStream from instance({}), runtime({})", metaData.instanceID,
               metaData.runtimeID);

    ASSERT_IF_NULL(internalIam_);
    if (!internalIam_->VerifyInstance(metaData.instanceID)) {
        YRLOG_ERROR(
            "client connect request unauthorized, instance may be deleted or invalid, instance id: {}, runtime id: {}",
            metaData.instanceID, metaData.runtimeID);
        return FailureReactor(
            ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "instance of you specified may be deleted or invalid"));
    }
    // VerifyToken(token).Get() is performed in grpc thread��interface constraints must be synchronized
    if (internalIam_->IsIAMEnabled()) {
        auto credType = internalIam_->GetCredType();
        switch (credType) {
            case function_proxy::IAMCredType::TOKEN:
                if (!internalIam_->VerifyToken(metaData.token).Get().IsOk()) {
                    YRLOG_ERROR("client connect request unauthorized, instance id: {}, runtime id: {}",
                                metaData.instanceID, metaData.runtimeID);
                    return FailureReactor(
                        ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "failed to verify token"));
                }
                break;
            case function_proxy::IAMCredType::AK_SK: {
                auto akSkContent = internalIam_->RequireCredentialByAK(metaData.accessKey).Get();
                if (akSkContent == nullptr || akSkContent->IsValid().IsError()
                    || !VerifyTimestamp(akSkContent->accessKey, akSkContent->secretKey, metaData.timestamp,
                                        metaData.signature)) {
                    YRLOG_ERROR("client connect request unauthorized, instance id: {}, runtime id: {}",
                                metaData.instanceID, metaData.runtimeID);
                    return FailureReactor(
                        ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "failed to verify AK/SK."));
                }
                break;
            }
            default:
                YRLOG_ERROR("invalid credType({}) for instance({}), runtime id: {}", static_cast<uint32_t>(credType),
                            metaData.instanceID, metaData.runtimeID);
                return FailureReactor(::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "invalid credType."));
        }
    }

    const bool isEventConnection = metaData.streamRole == EVENT_STREAM_ROLE;
    if (isEventConnection) {
        if (metaData.destinationID != FUNCTION_PROXY_STREAM_ID
            || !PosixService::CheckClientIsReady(metaData.instanceID)) {
            YRLOG_ERROR("invalid event connection for instance({}), destination({})", metaData.instanceID,
                        metaData.destinationID);
            return FailureReactor(::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION,
                                                 "event connection requires a ready control stream"));
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (auto iter = eventClients_.find(metaData.instanceID);
            iter != eventClients_.end() && !iter->second->IsDone()) {
            YRLOG_ERROR("event connection already exists for instance {}", metaData.instanceID);
            return FailureReactor(
                ::grpc::Status(::grpc::StatusCode::ALREADY_EXISTS, "event connection is already existed."));
        }
        auto reactor = std::make_shared<grpc::PosixStream::ServerReactor>();
        auto eventClient = std::make_shared<grpc::PosixStream>(
            reactor, context, metaData.instanceID, metaData.runtimeID);
        eventClients_[metaData.instanceID] = eventClient;
        YRLOG_INFO("accepted event MessageStream from instance({}), runtime({})",
                   metaData.instanceID, metaData.runtimeID);
        return reactor.get();
    }

    if (PosixService::CheckClientIsReady(metaData.instanceID)) {
        YRLOG_ERROR(
            "client connect request unauthorized, instance id: {} already running, can't accept an new connection",
            metaData.instanceID);
        return FailureReactor(::grpc::Status(::grpc::StatusCode::ALREADY_EXISTS, "connection is already existed."));
    }

    auto reactor = std::make_shared<grpc::PosixStream::ServerReactor>();
    std::shared_ptr<grpc::PosixClient> posixClient =
        std::make_shared<grpc::PosixStream>(reactor, context, metaData.instanceID, metaData.runtimeID);
    if (internalIam_->IsIAMEnabled() && internalIam_->GetCredType() == function_proxy::IAMCredType::AK_SK) {
        auto authInterceptor =
            std::make_shared<PosixAuthInterceptor>(metaData.runtimeID, metaData.instanceID, metaData.accessKey);
        authInterceptor->BindInternalIAM(internalIam_);
        posixClient->SetAuthInterceptor(authInterceptor);
    }
    PosixService::UpdateClient(metaData.instanceID, posixClient);
    if (updatePosixClientCallback_) {
        updatePosixClientCallback_(metaData.instanceID, metaData.runtimeID, posixClient);
    }
    return reactor.get();
}

PosixMetaData PosixService::GetMetaData(const ::grpc::CallbackServerContext *context) const
{
    auto metadata = context->client_metadata();
    PosixMetaData metaData;
    for (const auto &metaIte : metadata) {
        auto key = std::string(metaIte.first.data(), metaIte.first.length());
        if (key == "instance_id") {
            metaData.instanceID = std::string(metaIte.second.data(), metaIte.second.length());
        }
        if (key == "runtime_id") {
            metaData.runtimeID = std::string(metaIte.second.data(), metaIte.second.length());
        }
        if (key == "source_id") {
            metaData.sourceID = std::string(metaIte.second.data(), metaIte.second.length());
        }
        if (key == "dst_id") {
            metaData.destinationID = std::string(metaIte.second.data(), metaIte.second.length());
        }
        if (key == "stream_role") {
            metaData.streamRole = std::string(metaIte.second.data(), metaIte.second.length());
        }
        if (key == "authorization") {
            metaData.token = std::string(metaIte.second.data(), metaIte.second.length());
        }
        if (key == "access_key") {
            metaData.accessKey = std::string(metaIte.second.data(), metaIte.second.length());
        }
        if (key == "timestamp") {
            metaData.timestamp = std::string(metaIte.second.data(), metaIte.second.length());
        }
        if (key == "signature") {
            metaData.signature = std::string(metaIte.second.data(), metaIte.second.length());
        }
    }
    return metaData;
}

bool PosixService::CheckClientIsReady(const std::string &instanceID)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (auto iter = clients_.find(instanceID); iter != clients_.end() && !iter->second->IsDone()) {
        return true;
    }
    return false;
}

void PosixService::DeleteClient(const std::string &instanceID)
{
    std::shared_ptr<grpc::PosixClient> controlClient;
    std::shared_ptr<grpc::PosixClient> eventClient;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (auto iter = clients_.find(instanceID); iter != clients_.end()) {
            controlClient = std::move(iter->second);
            (void)clients_.erase(iter);
        }
        if (auto iter = eventClients_.find(instanceID); iter != eventClients_.end()) {
            eventClient = std::move(iter->second);
            (void)eventClients_.erase(iter);
        }
    }
    // Destroy streams after releasing mutex_: stopping a stream may synchronously
    // wait for its reactor callback, which must never run under the registry lock.
    eventClient.reset();
    controlClient.reset();
}

void PosixService::UpdateClient(const std::string &instanceID, const std::shared_ptr<grpc::PosixClient> &client)
{
    std::lock_guard<std::mutex> lock(mutex_);
    clients_[instanceID] = client;
}
}  // namespace functionsystem
