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

#include "state_actor.h"

#include "async/asyncafter.hpp"
#include "common/utils/generate_message.h"

namespace functionsystem::function_proxy {

StateActor::StateActor(const std::shared_ptr<StateClient> &stateClient)
    : litebus::ActorBase("state_actor"), stateClient_(stateClient)
{
}

litebus::Future<StateSaveResponse> StateActor::SaveState(const std::string &instanceId,
                                                         const std::string &boundRuntimeID,
                                                         const std::string &boundRequestID,
                                                         const std::shared_ptr<StateSaveRequest> &request)
{
    if (instanceId.empty() || !stateClient_) {
        YRLOG_ERROR("failed to save state: empty instance id {} or stateClient null", instanceId);
        return GenStateSaveResponse(common::ErrorCode::ERR_PARAM_INVALID, "save state failed: empty instance id");
    }
    // Unbound identity (stream accepted without a resolvable creation
    // generation) must never degrade to a bare-key write: refuse.
    if (boundRequestID.empty()) {
        YRLOG_ERROR("refused to save state for instance({}) runtime({}): stream carries no bound "
                    "creation generation", instanceId, boundRuntimeID);
        return GenStateSaveResponse(common::ErrorCode::ERR_PARAM_INVALID,
                                    "save state failed: unbound stream identity");
    }
    // Commit-point ownership fence: the same creation request under a
    // SUPERSEDED runtime must not overwrite the successor's state (the
    // accept-time binding cannot catch a replacement that lands AFTER the
    // stream was accepted). Refused with zero writes; no relabeling of the
    // old message onto the current runtime.
    if (ownershipResolver_) {
        const std::string currentRuntime = ownershipResolver_(instanceId);
        if (currentRuntime.empty() || currentRuntime != boundRuntimeID) {
            YRLOG_ERROR("refused to save state for instance({}): stream bound to runtime({}) "
                        "but the current machine runs runtime({})",
                        instanceId, boundRuntimeID, currentRuntime.empty() ? "<none>" : currentRuntime);
            return GenStateSaveResponse(common::ErrorCode::ERR_INSTANCE_BUSY,
                                        "save state refused: stream runtime superseded");
        }
    }
    const std::string key = StateClient::ComposeStateKey(instanceId, boundRequestID);
    auto status = stateClient_->Set(key, request->state());
    if (status.IsError()) {
        YRLOG_ERROR("failed to save state, status: {}", status.ToString());
        return GenStateSaveResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                    "save state failed: " + status.ToString());
    }
    YRLOG_INFO("succeed to save instance({}) generation({}) state under versioned checkpoint id",
               instanceId, boundRequestID);
    // The VERSIONED checkpoint id travels back to the runtime: subsequent
    // loads reference it explicitly (plan v2r1 R3).
    return GenStateSaveResponse(common::ErrorCode::ERR_NONE, "", key);
}

litebus::Future<StateLoadResponse> StateActor::LoadState(const std::shared_ptr<StateLoadRequest> &request)
{
    if (request->checkpointid().empty() || !stateClient_) {
        YRLOG_ERROR("failed to load state: empty checkpoint id");
        return GenStateLoadResponse(common::ErrorCode::ERR_PARAM_INVALID, "load state failed: empty checkpoint id");
    }
    // Versioned-reference well-formedness only. The EXISTING load
    // contract — read whatever checkpoint reference the caller supplies —
    // is preserved: no caller-identity restriction is added here (a
    // cross-instance versioned reference loads, exactly as a cross-
    // instance bare one did before). Bare legacy references are refused
    // under the migration contract until backfill completes; that refusal
    // is migration-pending, not a permanent semantic.
    if (StateClient::ParseStateKey(request->checkpointid()).first.empty()) {
        YRLOG_ERROR("refused to load checkpoint({}): not a versioned reference",
                    request->checkpointid());
        return GenStateLoadResponse(common::ErrorCode::ERR_PARAM_INVALID,
                                    "load state failed: checkpoint reference is not versioned");
    }

    std::string state;
    auto status = stateClient_->Get(request->checkpointid(), state);
    if (status.IsError()) {
        YRLOG_ERROR("failed to load state: {}", status.ToString());
        return GenStateLoadResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                    "load state failed: " + status.ToString());
    }
    YRLOG_INFO("succeed to load checkpoint({}) state", request->checkpointid());
    return GenStateLoadResponse(common::ErrorCode::ERR_NONE, "", state);
}

litebus::Future<Status> StateActor::DeleteState(const std::string &instanceId, const std::string &generation)
{
    if (generation.empty()) {
        YRLOG_ERROR("refused to delete state for instance({}): no creation generation carried", instanceId);
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID,
                      "delete state failed: no creation generation carried");
    }
    return stateClient_->Del(StateClient::ComposeStateKey(instanceId, generation));
}

void StateActor::SetOwnershipResolver(std::function<std::string(const std::string &)> resolver)
{
    ownershipResolver_ = std::move(resolver);
}

void StateActor::InitStateClient()
{
    RETURN_IF_NULL(stateClient_);
    if (auto status = stateClient_->Init(); status.IsOk()) {
        YRLOG_INFO("succeed to init state client");
        return;
    }
    YRLOG_WARN("failed to init state client, try to reconnect");
    const uint32_t stateClientInitRetryPeriod = 1000;  // ms
    litebus::AsyncAfter(stateClientInitRetryPeriod, GetAID(), &StateActor::InitStateClient);
}

}  // namespace functionsystem::function_proxy