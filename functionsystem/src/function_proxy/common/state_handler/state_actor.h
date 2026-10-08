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

#ifndef BUSPROXY_BUSINESS_STATE_ACTOR_STATE_ACTOR_H
#define BUSPROXY_BUSINESS_STATE_ACTOR_STATE_ACTOR_H

#include <memory>

#include "actor/actor.hpp"
#include "async/future.hpp"
#include "common/logs/logging.h"
#include "common/proto/pb/posix_pb.h"
#include "state_client.h"

namespace functionsystem::function_proxy {

class StateActor : public litebus::ActorBase {
public:
    explicit StateActor(const std::shared_ptr<StateClient> &stateClient);
    ~StateActor() override = default;

    // #S (plan v2r1 R1/R3): the physical key is generation-qualified with
    // the CREATION request id BOUND AT THE MESSAGE SOURCE (posix stream
    // accept for runtime saves; the proxy-internal caller's instance record
    // otherwise). An unbound identity is an explicit refusal — never a
    // bare-key fallback. The save response carries the full VERSIONED
    // checkpoint id back to the runtime.
    litebus::Future<StateSaveResponse> SaveState(const std::string &instanceId,
                                                 const std::string &boundRuntimeID,
                                                 const std::string &boundRequestID,
                                                 const std::shared_ptr<StateSaveRequest> &request);

    // Loads by EXPLICIT versioned reference (the checkpointid the saver
    // handed out — the composed key itself). The EXISTING load contract —
    // read whatever reference the caller supplies — is preserved (no new
    // caller-identity restriction); only well-formedness of the versioned
    // reference is checked, and bare legacy references are refused under
    // the migration contract.
    litebus::Future<StateLoadResponse> LoadState(const std::shared_ptr<StateLoadRequest> &request);

    // Retire-side delete: the retiring chain's OWN creation generation.
    litebus::Future<Status> DeleteState(const std::string &instanceId, const std::string &generation);

    void InitStateClient();

    // Commit-point ownership fence (#S, root 16:48): answers the CURRENT
    // machine's runtimeid for an instanceID ("" = no machine). A save whose
    // stream was bound to a SUPERSEDED runtime (failover keeps the creation
    // request but replaces the runtime — both would otherwise compose the
    // SAME key) is refused here, at the store boundary, with zero writes.
    // The resolver reads the control view under its own lock (thread-safe);
    // an empty answer is an explicit refusal (unknown ≠ authorized).
    void SetOwnershipResolver(
        std::function<std::string(const std::string &instanceID)> resolver);

private:
    std::shared_ptr<StateClient> stateClient_;
    std::function<std::string(const std::string &instanceID)> ownershipResolver_;
};

}  // namespace functionsystem::function_proxy

#endif  // BUSPROXY_BUSINESS_STATE_ACTOR_STATE_ACTOR_H
