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
#include "control_interface_posix_client.h"

#include "function_proxy/common/state_handler/state_handler.h"

namespace functionsystem {
using namespace runtime_rpc;
using namespace runtime_service;

const uint64_t READINESS_TIMEOUT = 5000;

litebus::Future<Status> ControlInterfacePosixClient::Heartbeat(uint64_t timeMs)
{
    runtime::HeartbeatRequest request;
    auto promise = std::make_shared<litebus::Promise<Status>>();
    auto msg = std::make_shared<StreamingMessage>();
    *msg->mutable_heartbeatreq() = std::move(request);
    msg->set_messageid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    if (posix_ == nullptr) {
        promise->SetFailed(static_cast<int32_t>(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS));
        return promise->GetFuture();
    }
    auto rspFuture = posix_->Send(msg);
    rspFuture
        .After(timeMs,
               [promise](const litebus::Future<StreamingMessage> &future) {
                   promise->SetFailed(static_cast<int32_t>(StatusCode::REQUEST_TIME_OUT));
                   return future;
               })
        .OnComplete([promise, msg](const litebus::Future<StreamingMessage> &future) {
            if (future.IsError() || !future.Get().has_heartbeatrsp()) {
                YRLOG_ERROR("failed to get heart rsp for msg({})", msg->messageid());
                promise->SetFailed(static_cast<int32_t>(StatusCode::INSTANCE_HEARTBEAT_LOST));
                return;
            }

            auto heartbeatRsp = future.Get().heartbeatrsp();
            switch (heartbeatRsp.code()) {
                case common::HealthCheckCode::HEALTHY:
                    promise->SetValue(Status(StatusCode::SUCCESS));
                    break;
                case common::HealthCheckCode::SUB_HEALTH:
                    // don't change to FATAL when receive SUB_HEALTH, using SetValue instead of SetFailed
                    promise->SetValue(Status(StatusCode::INSTANCE_SUB_HEALTH));
                    break;
                case common::HealthCheckCode::HEALTH_CHECK_FAILED:
                    promise->SetFailed(static_cast<int32_t>(StatusCode::INSTANCE_HEALTH_CHECK_ERROR));
                    break;
                default:
                    YRLOG_WARN("unknown heartbeat code({})", fmt::underlying(heartbeatRsp.code()));
            }
        });
    return promise->GetFuture();
}

litebus::Future<Status> ControlInterfacePosixClient::Readiness()
{
    return Heartbeat(READINESS_TIMEOUT);
}

litebus::Future<runtime::ShutdownResponse> ControlInterfacePosixClient::Shutdown(runtime::ShutdownRequest &&request)
{
    auto promise = std::make_shared<litebus::Promise<runtime::ShutdownResponse>>();

    std::shared_lock<std::shared_mutex> lock(rwMut_);
    if (posix_ == nullptr) {
        runtime::ShutdownResponse shutdownRsp{};
        shutdownRsp.set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
        shutdownRsp.set_message("shutdown failed! client may already closed");

        promise->SetValue(shutdownRsp);
        return promise->GetFuture();
    }

    auto msg = std::make_shared<StreamingMessage>();
    *msg->mutable_shutdownreq() = std::move(request);
    msg->set_messageid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    auto rspFuture = posix_->Send(msg);

    (void)rspFuture.OnComplete([promise](const litebus::Future<StreamingMessage> &resp) {
        if (resp.IsError()) {
            runtime::ShutdownResponse shutdownRsp{};
            shutdownRsp.set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
            shutdownRsp.set_message("shutdown failed! failed to get response.");

            promise->SetValue(shutdownRsp);
            return;
        }
        promise->SetValue(resp.Get().shutdownrsp());
    });

    return promise->GetFuture();
}

litebus::Future<runtime::SignalResponse> ControlInterfacePosixClient::Signal(runtime::SignalRequest &&request)
{
    auto promise = std::make_shared<litebus::Promise<runtime::SignalResponse>>();
    auto msg = std::make_shared<StreamingMessage>();
    *msg->mutable_signalreq() = std::move(request);
    msg->set_messageid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    runtime::SignalResponse signalRsp{};
    signalRsp.set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
    signalRsp.set_message("signal failed! client may already closed");
    if (posix_ == nullptr) {
        promise->SetValue(signalRsp);
        return promise->GetFuture();
    }
    auto rspFuture = posix_->Send(msg);
    (void)rspFuture.OnComplete([promise, signalRsp](const litebus::Future<StreamingMessage> &resp) {
        if (resp.IsError()) {
            runtime::SignalResponse signalRsp{};
            signalRsp.set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
            signalRsp.set_message("signal failed! client may already closed");
            promise->SetValue(signalRsp);
            return;
        }
        promise->SetValue(resp.Get().signalrsp());
    });
    return promise->GetFuture();
}

litebus::Future<runtime::CheckpointResponse> ControlInterfacePosixClient::Checkpoint(CheckpointRequest &&request)
{
    auto promise = std::make_shared<litebus::Promise<runtime::CheckpointResponse>>();
    auto msg = std::make_shared<StreamingMessage>();
    *msg->mutable_checkpointreq() = std::move(request);
    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    msg->set_messageid(uuid.ToString());
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    runtime::CheckpointResponse checkpointRsp{};
    checkpointRsp.set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
    checkpointRsp.set_message("checkpoint failed! client may already closed");
    if (posix_ == nullptr) {
        promise->SetValue(checkpointRsp);
        return promise->GetFuture();
    }
    (void)posix_->Send(msg).OnComplete([promise, checkpointRsp](const litebus::Future<StreamingMessage> &rsp) {
        if (rsp.IsError()) {
            YRLOG_ERROR("failed to checkpoint! client may already closed");
            promise->SetValue(checkpointRsp);
            return;
        }
        promise->SetValue(rsp.Get().checkpointrsp());
    });
    return promise->GetFuture();
}

litebus::Future<runtime::RecoverResponse> ControlInterfacePosixClient::Recover(RecoverRequest &&request,
                                                                               uint64_t timeoutMs)
{
    auto promise = std::make_shared<litebus::Promise<runtime::RecoverResponse>>();
    auto msg = std::make_shared<StreamingMessage>();
    *msg->mutable_recoverreq() = std::move(request);
    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    msg->set_messageid(uuid.ToString());
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    auto recoverRsp = std::make_shared<runtime::RecoverResponse>();
    recoverRsp->set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
    recoverRsp->set_message("recover failed! client may already closed");
    if (posix_ == nullptr) {
        promise->SetValue(*recoverRsp);
        return promise->GetFuture();
    }
    (void)posix_->Send(msg)
        .After(timeoutMs,
               [recoverRsp](const litebus::Future<StreamingMessage> &rsp) {
                   YRLOG_ERROR("failed to recover, call recover timeout");
                   recoverRsp->set_code(common::ERR_USER_FUNCTION_EXCEPTION);
                   recoverRsp->set_message("timeout to call recover");
                   rsp.SetFailed(litebus::Status::KERROR);
                   return rsp;
               })
        .OnComplete([promise, recoverRsp](const litebus::Future<StreamingMessage> &rsp) {
            if (rsp.IsError()) {
                YRLOG_ERROR("failed to recover! client may already closed");
                promise->SetValue(*recoverRsp);
                return;
            }
            promise->SetValue(rsp.Get().recoverrsp());
        });
    return promise->GetFuture();
}

litebus::Future<runtime::PrepareSnapResponse> ControlInterfacePosixClient::PrepareSnap(PrepareSnapRequest &&request)
{
    auto promise = std::make_shared<litebus::Promise<runtime::PrepareSnapResponse>>();
    auto msg = std::make_shared<StreamingMessage>();
    *msg->mutable_preparesnapreq() = std::move(request);
    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    msg->set_messageid(uuid.ToString());
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    runtime::PrepareSnapResponse rsp{};
    rsp.set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
    rsp.set_message("prepare snap failed! client may already closed");
    if (posix_ == nullptr) {
        promise->SetValue(rsp);
        return promise->GetFuture();
    }
    (void)posix_->Send(msg).OnComplete([promise, rsp](const litebus::Future<StreamingMessage> &resp) {
        if (resp.IsError() || !resp.Get().has_preparesnaprsp()) {
            YRLOG_ERROR("failed to prepare snap! client may already closed");
            promise->SetValue(rsp);
            return;
        }
        promise->SetValue(resp.Get().preparesnaprsp());
    });
    return promise->GetFuture();
}

litebus::Future<runtime::PrepareSnapResponse> ControlInterfacePosixClient::PrepareSnapForRuntime(
    PrepareSnapRequest &&request, const std::string &expectedRuntimeID)
{
    auto promise = std::make_shared<litebus::Promise<runtime::PrepareSnapResponse>>();
    auto msg = std::make_shared<StreamingMessage>();
    *msg->mutable_preparesnapreq() = std::move(request);
    msg->set_messageid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
    auto rejectBusy = [&promise](const std::string &reason) {
        runtime::PrepareSnapResponse rsp{};
        rsp.set_code(common::ERR_INSTANCE_BUSY);
        rsp.set_message(reason);
        promise->SetValue(rsp);
    };
    // No pin, no dispatch: an empty expectation can never authorize a
    // conditional snapshot, so it is rejected before touching the transport.
    if (expectedRuntimeID.empty()) {
        YRLOG_ERROR("{}|prepare snap for runtime rejected: expected runtime identity is empty", msg->messageid());
        rejectBusy("prepare snap rejected: expected runtime identity is empty");
        return promise->GetFuture();
    }
    runtime::PrepareSnapResponse rsp{};
    rsp.set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
    rsp.set_message("prepare snap failed! client may already closed");
    // One shared lock covers BOTH the binding check and the send: UpdatePosix
    // and Close take the unique lock, so the transport cannot be swapped (or
    // its label changed) between validating the runtime and dispatching. The
    // legacy PrepareSnap must NOT be called after unlocking — it would re-lock
    // and race the very swap this check fences.
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    if (posix_ == nullptr) {
        promise->SetValue(rsp);
        return promise->GetFuture();
    }
    if (runtimeID_.empty() || runtimeID_ != expectedRuntimeID) {
        // Unbound (never tagged, retagged without a label or closed under a
        // prior label) or re-bound to another runtime: the old request is
        // never redirected to whatever transport the slot now holds.
        YRLOG_ERROR("{}|prepare snap rejected: transport bound to runtime({}) does not match expected runtime({})",
                    msg->messageid(), runtimeID_, expectedRuntimeID);
        lock.unlock();
        rejectBusy("prepare snap rejected: transport no longer bound to the expected runtime");
        return promise->GetFuture();
    }
    (void)posix_->Send(msg).OnComplete([promise, rsp](const litebus::Future<StreamingMessage> &resp) {
        if (resp.IsError() || !resp.Get().has_preparesnaprsp()) {
            YRLOG_ERROR("failed to prepare snap! client may already closed");
            promise->SetValue(rsp);
            return;
        }
        promise->SetValue(resp.Get().preparesnaprsp());
    });
    return promise->GetFuture();
}

litebus::Future<runtime::SnapStartedResponse> ControlInterfacePosixClient::SnapStarted(SnapStartedRequest &&request)
{
    auto promise = std::make_shared<litebus::Promise<runtime::SnapStartedResponse>>();
    auto msg = std::make_shared<StreamingMessage>();
    *msg->mutable_snapstartedreq() = std::move(request);
    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    msg->set_messageid(uuid.ToString());
    std::shared_lock<std::shared_mutex> lock(rwMut_);
    runtime::SnapStartedResponse rsp{};
    rsp.set_code(common::ERR_REQUEST_BETWEEN_RUNTIME_BUS);
    rsp.set_message("snap started failed! client may already closed");
    if (posix_ == nullptr) {
        promise->SetValue(rsp);
        return promise->GetFuture();
    }
    (void)posix_->Send(msg).OnComplete([promise, rsp](const litebus::Future<StreamingMessage> &resp) {
        if (resp.IsError() || !resp.Get().has_snapstartedrsp()) {
            YRLOG_ERROR("failed to snap started! client may already closed");
            promise->SetValue(rsp);
            return;
        }
        promise->SetValue(resp.Get().snapstartedrsp());
    });
    return promise->GetFuture();
}
}  // namespace functionsystem