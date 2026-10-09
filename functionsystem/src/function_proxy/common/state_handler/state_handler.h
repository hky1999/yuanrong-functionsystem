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
    // v3: bind the meta-store registry AND a function-key resolver (reads
    // each instance's function from the control view) onto the live actor.
    static void BindSaveRegistry(const std::shared_ptr<MetaStoreClient> &metaStoreClient,
                                 const StateActor::FunctionKeyResolver &functionKeyResolver);
    // The posix handler signature: the STREAM-BOUND identity (creation
    // generation fixed at accept) travels with the message; an unbound
    // stream is refused by the actor, never silently degraded.
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> SaveState(
        const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);

    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> LoadState(
        const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);

    // Recover-side: ASYNC resolution of the generation's published
    // checkpoint reference (stateref) through the state actor. The caller
    // feeds the resolved reference into LoadState — it never composes a
    // key itself. Refusal (read error / corrupt / identity mismatch) is
    // surfaced, never degraded; only explicit absence yields the legacy
    // migration key.
    static litebus::Future<StateActor::CheckpointRef> ResolveCheckpointRef(
        const std::string &instanceId, const std::string &creationRequest);

    // Retirement-side: commits the PERSISTENT retirement fence for the
    // generation (conditionally on the ORIGINAL authoritative record —
    // identity verified; replays verify the existing tombstone). Must run
    // BEFORE the control view deletes the authoritative metadata.
    static litebus::Future<StateActor::RetireReport> RetireGeneration(
        const std::string &instanceId, const std::string &creationRequest,
        const std::string &runtimeID);
    // Retirement-side: idempotent, replayable snapshot cleanup gated on
    // the tombstone (the only admissible identity proof once the
    // authoritative record is gone). Enumerates the generation's statereg
    // prefix in the meta-store, exact-Dels each validated snapshot key,
    // conditionally deletes the stateref. Reports per-entry outcomes;
    // tombstone + registrations retained for replay.
    static litebus::Future<StateActor::RetireReport> CleanupRetiredSnapshots(
        const std::string &instanceId, const std::string &creationRequest,
        const std::string &runtimeID);

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
