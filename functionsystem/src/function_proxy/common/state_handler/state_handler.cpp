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

#include "state_handler.h"

#include "async/async.hpp"
#include "common/rpc/stream/posix/control_client.h"
#include "common/utils/generate_message.h"

namespace functionsystem::function_proxy {
using namespace core_service;
using namespace runtime_rpc;
REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(StreamingMessage::kSaveReq, &StateHandler::SaveState);
REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(StreamingMessage::kLoadReq, &StateHandler::LoadState);

void StateHandler::BindStateActor(const std::shared_ptr<StateActor> &stateActor)
{
    if (stateActor == nullptr) {
        return;
    }
    stateActorAid_ = stateActor->GetAID();
    litebus::Async(stateActorAid_, &StateActor::InitStateClient);
}

void StateHandler::BindSaveExecutor(const StateActor::SaveExecutor &executor)
{
    if (stateActorAid_.Name().empty()) {
        YRLOG_ERROR("failed to bind save executor: don't init state actor");
        return;
    }
    litebus::Async(stateActorAid_, &StateActor::SetSaveExecutor, executor);
}

void StateHandler::BindDeleteExecutor(const StateActor::DeleteExecutor &executor)
{
    if (stateActorAid_.Name().empty()) {
        YRLOG_ERROR("failed to bind delete executor: don't init state actor");
        return;
    }
    litebus::Async(stateActorAid_, &StateActor::SetDeleteExecutor, executor);
}

void StateHandler::BindSaveRegistry(const std::shared_ptr<MetaStoreClient> &metaStoreClient)
{
    if (stateActorAid_.Name().empty()) {
        YRLOG_ERROR("failed to bind save registry: don't init state actor");
        return;
    }
    litebus::Async(stateActorAid_, &StateActor::BindSaveRegistry, metaStoreClient);
}

litebus::Future<std::shared_ptr<StreamingMessage>> StateHandler::SaveState(
    const grpc::PosixStreamIdentity &identity, const std::shared_ptr<StreamingMessage> &request)
{
    YRLOG_INFO("state handler receive save state from instance({}) runtime({}) bound generation({})",
               identity.instanceID, identity.runtimeID, identity.boundRequestID);
    if (identity.instanceID.empty() || !request) {
        YRLOG_ERROR("failed to save state: empty instance id");
        return GenStateSaveRspStreamMessage(common::ErrorCode::ERR_PARAM_INVALID,
                                            "save state failed: empty instance id");
    }

    if (stateActorAid_.Name().empty()) {
        YRLOG_ERROR("failed to save state: don't init state actor");
        return GenStateSaveRspStreamMessage(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                            "save state failed: don't init state actor");
    }

    auto stateSaveRequest = std::make_shared<StateSaveRequest>(std::move(*request->mutable_savereq()));
    return litebus::Async(stateActorAid_, &StateActor::SaveState, identity.instanceID,
                          identity.runtimeID, identity.boundRequestID, stateSaveRequest)
        .Then([](const StateSaveResponse &rsp) -> litebus::Future<std::shared_ptr<StreamingMessage>> {
            auto response = std::make_shared<StreamingMessage>();
            response->mutable_saversp()->CopyFrom(std::move(rsp));
            return response;
        });
}

void StateHandler::ClearStateActor()
{
    stateActorAid_ = litebus::AID();
}

litebus::Future<std::shared_ptr<StreamingMessage>> StateHandler::LoadState(
    const grpc::PosixStreamIdentity &identity, const std::shared_ptr<StreamingMessage> &request)
{
    YRLOG_INFO("state handler receive load state from instance({}) runtime({})", identity.instanceID,
               identity.runtimeID);
    if (identity.instanceID.empty() || !request) {
        YRLOG_ERROR("failed to load state: empty instance id");
        return GenStateLoadRspStreamMessage(common::ErrorCode::ERR_PARAM_INVALID,
                                            "load state failed: empty instance id");
    }

    if (stateActorAid_.Name().empty()) {
        YRLOG_ERROR("failed to save state: don't init state actor");
        return GenStateSaveRspStreamMessage(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                            "save state failed: don't init state actor");
    }

    auto stateLoadRequest = std::make_shared<StateLoadRequest>(std::move(*request->mutable_loadreq()));
    return litebus::Async(stateActorAid_, &StateActor::LoadState, stateLoadRequest)
        .Then([](const StateLoadResponse &rsp) -> litebus::Future<std::shared_ptr<StreamingMessage>> {
            auto response = std::make_shared<StreamingMessage>();
            response->mutable_loadrsp()->CopyFrom(std::move(rsp));
            return response;
        });
}

litebus::Future<Status> StateHandler::DeleteState(const std::string &instanceId, const std::string &generation,
                                                   const std::string &expectedRuntimeID)
{
    YRLOG_INFO("state handler receive delete state from instance({}) generation({}) runtime({})",
               instanceId, generation, expectedRuntimeID);
    if (instanceId.empty() || generation.empty() || expectedRuntimeID.empty()) {
        YRLOG_ERROR("failed to delete state: empty instance id, generation or runtime");
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID,
                      "delete state failed: incomplete identity");
    }

    if (stateActorAid_.Name().empty()) {
        YRLOG_ERROR("failed to delete state: don't init state actor");
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "delete state failed: don't init state actor");
    }

    return litebus::Async(stateActorAid_, &StateActor::DeleteState, instanceId, generation,
                          expectedRuntimeID);
}

}  // namespace functionsystem::function_proxy