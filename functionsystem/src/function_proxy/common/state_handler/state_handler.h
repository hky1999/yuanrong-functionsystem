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

#ifndef BUSPROXY_BUSINESS_STATE_ACTOR_STATE_HANDLER_H
#define BUSPROXY_BUSINESS_STATE_ACTOR_STATE_HANDLER_H

#include "common/rpc/stream/posix/posix_client.h"
#include "state_actor.h"

namespace functionsystem::function_proxy {

class StateHandler {
public:
    virtual ~StateHandler();

    static void BindStateActor(const std::shared_ptr<StateActor> &stateActor);
    // Bind the EXECUTION-POINT executors onto the LIVE state actor (driver
    // startup). Each executor must run its GuardedStoreOp under the control
    // view's lock — the same lock generation changes take.
    static void BindSaveExecutor(const StateActor::SaveExecutor &executor);
    static void BindDeleteExecutor(const StateActor::DeleteExecutor &executor);
    // v3: bind the meta-store registry used for registration-first saves.
    static void BindSaveRegistry(const std::shared_ptr<MetaStoreClient> &metaStoreClient);
    // The posix handler signature: the STREAM-BOUND identity (creation
    // generation fixed at accept) travels with the message; an unbound
    // stream is refused by the actor, never silently degraded.
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> SaveState(
        const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);

    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> LoadState(
        const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);

    // Proxy-internal retire-side delete carrying the retiring chain's OWN
    // identity (creation generation + runtime).
    static litebus::Future<Status> DeleteState(const std::string &instanceId, const std::string &generation,
                                               const std::string &expectedRuntimeID);

protected:
    [[maybe_unused]] static void ClearStateActor();

private:
    inline static litebus::AID stateActorAid_{};
};

}  // namespace functionsystem::function_proxy

#endif  // BUSPROXY_BUSINESS_STATE_ACTOR_STATE_HANDLER_H
