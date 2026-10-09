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
#include <string>
#include <vector>

#include "actor/actor.hpp"
#include "async/future.hpp"
#include "common/logs/logging.h"
#include "common/proto/pb/posix_pb.h"
#include "meta_store_client/meta_store_client.h"
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

    void InitStateClient();

    // EXECUTION-POINT serialization (#S, root 16:5x): the ownership check
    // and the SYNCHRONOUS store operation run together under the caller-
    // supplied executor, which must hold the SAME lock the control view
    // uses for generation changes (observer in-place updates). A resolver-
    // style pre-check is check-then-act by construction and is NOT used:
    // the publish of a replacement runtime cannot interleave between the
    // check and the write. Empty/absent executors REFUSE (unknown ≠ pass).
    using GuardedStoreOp = std::function<Status()>;
    using SaveExecutor = std::function<Status(const std::string &instanceID,
                                              const std::string &expectedRuntimeID,
                                              const GuardedStoreOp &op)>;
    using DeleteExecutor = std::function<Status(const std::string &instanceID,
                                                const std::string &expectedRuntimeID,
                                                const GuardedStoreOp &op)>;
    void SetSaveExecutor(SaveExecutor executor);
    void SetDeleteExecutor(DeleteExecutor executor);

    // Retire-side delete: the retiring chain's OWN identity (creation
    // generation + runtime). The delete of the shared (instance, request)
    // key executes under the delete executor: a machine that now runs a
    // DIFFERENT runtime refuses; a machine absent allows (the retire
    // chain's completion), serialized against same-name inserts.
    litebus::Future<Status> DeleteState(const std::string &instanceId,
                                        const std::string &generation,
                                        const std::string &expectedRuntimeID);

    // ── v3 registration-first isolation ──────────────────────────────────
    // A persistent registry tracks every save operation's lifecycle in the
    // meta-store (etcd) so that a timeout or process death NEVER produces
    // an undiscoverable DS object. The registry is the single source of
    // truth for reclamation — DS has no enumeration API.
    //
    // Registration states: PENDING → (COMMITTED | UNKNOWN).
    //   PENDING  = registered, DS write in flight.
    //   COMMITTED = DS write succeeded AND conditional publish matched.
    //   UNKNOWN  = DS write outcome uncertain (timeout / publish conflict /
    //              process death) — the registration is RETAINED so a
    //              later reconciliation can discover the late object.
    //              UNKNOWN registrations are NEVER cleared by a single
    //              NOT_FOUND — a late write may still arrive after a Del.
    struct SaveRegistration {
        std::string saveOpID;
        std::string snapshotKey;
        std::string instanceID;
        std::string creationRequest;
        std::string runtimeID;
        std::string status;  // PENDING | COMMITTED | UNKNOWN
    };
    // Binds the registry used for v3 saves. Both parameters are required
    // (an unbound actor refuses v3 saves rather than silently falling back
    // to unregistered writes).
    void BindSaveRegistry(const std::shared_ptr<MetaStoreClient> &metaStoreClient);
    // Serializes a registration record (deterministic, escaped).
    static std::string SerializeRegistration(const SaveRegistration &reg);
    // Parses a registration record; `.status` empty on parse failure.
    static SaveRegistration ParseRegistration(const std::string &raw);
    // Registration key in the meta-store for a given save operation.
    static std::string RegistrationKey(const std::string &instanceID,
                                       const std::string &creationRequest,
                                       const std::string &saveOpID);

private:
    // Phase 2 (DS write) + Phase 3 (conditional publish) — shared by both
    // registration paths (guard match and guard creation).
    litebus::Future<StateSaveResponse> ProceedAfterRegistration(
        const std::string &instanceId, const std::string &boundRuntimeID,
        const std::string &snapshotKey, const std::string &saveOpID,
        const std::string &guardKey, const std::string &regKey,
        const std::string &pendingValue,
        const std::shared_ptr<StateSaveRequest> &request);
    // Fire-and-forget conditional UNKNOWN marking (only if the record
    // still holds the captured PENDING value).
    void MarkRegistrationUnknown(const std::string &regKey, const std::string &capturedValue,
                                 const std::string &guardKey, const std::string &runtimeID);

    std::shared_ptr<StateClient> stateClient_;
    SaveExecutor saveExecutor_;
    DeleteExecutor deleteExecutor_;
    std::shared_ptr<MetaStoreClient> registry_;  // v3 save registry
};

}  // namespace functionsystem::function_proxy

#endif  // BUSPROXY_BUSINESS_STATE_ACTOR_STATE_ACTOR_H
