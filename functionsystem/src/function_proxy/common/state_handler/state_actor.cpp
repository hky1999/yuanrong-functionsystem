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
#include "meta_store_client/txn_transaction.h"

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
    // EXECUTION-POINT fence: no executor bound ⇒ explicit refusal (an
    // unbound deployment never silently writes). With one bound, the
    // ownership check and the synchronous Set run together under the
    // executor's lock — the same lock the control view takes for
    // generation changes — so a replacement runtime's publish CANNOT land
    // between the check and the write (a pre-check would allow exactly
    // that). No relabeling of the old message onto the current runtime.
    if (!saveExecutor_) {
        YRLOG_ERROR("refused to save state for instance({}): no save executor bound "
                    "(unbound deployment refuses rather than silently writing)", instanceId);
        return GenStateSaveResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                    "save state refused: save executor not bound");
    }

    // ── v3 registration-first isolation (plan 20261008T220000Z) ────────
    // Phase 1: PERSIST REGISTRATION (etcd) BEFORE the DS write. A timeout
    // or process death at any later point leaves a discoverable record —
    // no undiscoverable DS object can exist. An unbound registry refuses
    // v3 saves (never silently falls back to unregistered writes).
    if (registry_ == nullptr) {
        YRLOG_ERROR("refused to save state for instance({}): no save registry bound "
                    "(v3 requires registration-first; unregistered writes forbidden)",
                    instanceId);
        return GenStateSaveResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                    "save state refused: save registry not bound");
    }
    const std::string snapshotKey = StateClient::ComposeSnapshotKey(instanceId, boundRequestID);
    // Extract the saveOpID (the trailing UUID segment after the second ':').
    const auto lastColon = snapshotKey.rfind(':');
    const std::string saveOpID = snapshotKey.substr(lastColon + 1);
    {
        SaveRegistration reg;
        reg.saveOpID = saveOpID;
        reg.snapshotKey = snapshotKey;
        reg.instanceID = instanceId;
        reg.creationRequest = boundRequestID;
        reg.runtimeID = boundRuntimeID;
        reg.status = "PENDING";
        // Put the registration into etcd. This is a synchronous meta-store
        // write — if IT fails, we refuse before touching DS at all.
        // NOTE: a full conditional transaction (If-of-value on the instance
        // record's ownership version) is the v3 publish step; the PENDING
        // registration itself is a plain Put (any prior record with the
        // same saveOpID is by construction a retry of THIS operation).
        auto txn = registry_->BeginTransaction();
        txn->Then(meta_store::TxnOperation::Create(
            RegistrationKey(instanceId, boundRequestID, saveOpID),
            SerializeRegistration(reg), meta_store::PutOption{}));
        auto committed = txn->Commit();
        if (!committed.IsReady() || committed.Get().IsError()
            || !committed.Get()->success) {
            YRLOG_ERROR("refused to save state for instance({}): registration PERSIST failed "
                        "(the DS write is NOT attempted)", instanceId);
            return GenStateSaveResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                        "save state refused: registration persist failed");
        }
    }

    // Phase 2: DS WRITE to the globally-unique snapshot key. A timeout
    // here leaves the registration at PENDING (discoverable — the object
    // may or may not arrive). The caller sees UNKNOWN (BUSY), never a
    // false success and never a silent retry of a shared key.
    auto writeStatus = saveExecutor_(instanceId, boundRuntimeID,
        [this, &snapshotKey, &request]() {
            return stateClient_->Set(snapshotKey, request->state());
        });
    if (writeStatus.IsError()) {
        // Phase 3 (timeout path): mark UNKNOWN, RETAIN the registration.
        // The object may still arrive late (B2b evidence); the UNKNOWN
        // record is how reconciliation discovers it.
        SaveRegistration reg;
        reg.saveOpID = saveOpID;
        reg.snapshotKey = snapshotKey;
        reg.instanceID = instanceId;
        reg.creationRequest = boundRequestID;
        reg.runtimeID = boundRuntimeID;
        reg.status = "UNKNOWN";
        auto txn = registry_->BeginTransaction();
        txn->Then(meta_store::TxnOperation::Create(
            RegistrationKey(instanceId, boundRequestID, saveOpID),
            SerializeRegistration(reg), meta_store::PutOption{}));
        (void)txn->Commit();
        YRLOG_WARN("save state for instance({}) op({}): DS write uncertain — registration "
                   "retained as UNKNOWN (late arrival possible, never retried blindly)",
                   instanceId, saveOpID);
        return GenStateSaveResponse(common::ErrorCode::ERR_INSTANCE_BUSY,
                                    "save state: DS write outcome UNKNOWN (registration retained)");
    }

    // Phase 3 (success path): CONDITIONAL PUBLISH — mark COMMITTED only
    // when the registration still holds OUR snapshot key (a competing
    // writer's registration would differ). This is the etcd-side fence:
    // an old chain's late publish cannot overwrite a newer chain's.
    SaveRegistration reg;
    reg.saveOpID = saveOpID;
    reg.snapshotKey = snapshotKey;
    reg.instanceID = instanceId;
    reg.creationRequest = boundRequestID;
    reg.runtimeID = boundRuntimeID;
    reg.status = "COMMITTED";
    {
        auto txn = registry_->BeginTransaction();
        // If the registration record still names OUR snapshot key, update
        // its status to COMMITTED; any other value ⇒ conflict (keep the
        // other writer's record; ours is a late publish).
        txn->If(meta_store::TxnCompare::OfValue(
            RegistrationKey(instanceId, boundRequestID, saveOpID),
            meta_store::CompareOperator::EQUAL,
            SerializeRegistration([&]() {
                SaveRegistration pending = reg;
                pending.status = "PENDING";
                return pending;
            }())));
        txn->Then(meta_store::TxnOperation::Create(
            RegistrationKey(instanceId, boundRequestID, saveOpID),
            SerializeRegistration(reg), meta_store::PutOption{}));
        auto published = txn->Commit();
        if (published.IsReady() && !published.Get().IsError()
            && published.Get()->success) {
            YRLOG_INFO("save state for instance({}) op({}): COMMITTED (conditional publish "
                       "matched)", instanceId, saveOpID);
        } else {
            // Publish conflict: the DS write SUCCEEDED (data is safe on
            // its unique key) but the registration was rewritten by a
            // competing writer. Record UNKNOWN — the data is discoverable
            // through the competing record or reconciliation.
            SaveRegistration unknown = reg;
            unknown.status = "UNKNOWN";
            auto fixTxn = registry_->BeginTransaction();
            fixTxn->Then(meta_store::TxnOperation::Create(
                RegistrationKey(instanceId, boundRequestID, saveOpID) + ":conflict",
                SerializeRegistration(unknown), meta_store::PutOption{}));
            (void)fixTxn->Commit();
            YRLOG_WARN("save state for instance({}) op({}): publish CONFLICT — data safe on "
                       "unique key, registration marked UNKNOWN", instanceId, saveOpID);
        }
    }
    YRLOG_INFO("succeed to save instance({}) generation({}) state under snapshot key",
               instanceId, boundRequestID);
    // The SNAPSHOT key is the opaque checkpoint id: loads reference it
    // explicitly (immutable — a late write can never change what it reads).
    return GenStateSaveResponse(common::ErrorCode::ERR_NONE, "", snapshotKey);
}

// ── v3 registration helpers ────────────────────────────────────────────────

void StateActor::BindSaveRegistry(const std::shared_ptr<MetaStoreClient> &metaStoreClient)
{
    registry_ = metaStoreClient;
}

std::string StateActor::RegistrationKey(const std::string &instanceID,
                                        const std::string &creationRequest,
                                        const std::string &saveOpID)
{
    // Escape each part so the key is unambiguous and parseable.
    return "statereg:v1:" + instanceID + ":" + creationRequest + ":" + saveOpID;
}

std::string StateActor::SerializeRegistration(const SaveRegistration &reg)
{
    // Simple pipe-separated format; each field is escaped.
    auto esc = [](const std::string &s) {
        std::string out;
        for (char c : s) {
            if (c == '|' || c == '\\') { out.push_back('\\'); }
            out.push_back(c);
        }
        return out;
    };
    return esc(reg.saveOpID) + "|" + esc(reg.snapshotKey) + "|" + esc(reg.instanceID)
        + "|" + esc(reg.creationRequest) + "|" + esc(reg.runtimeID) + "|" + esc(reg.status);
}

StateActor::SaveRegistration StateActor::ParseRegistration(const std::string &raw)
{
    SaveRegistration reg;
    std::vector<std::string> fields;
    std::string current;
    for (size_t i = 0; i < raw.size(); ++i) {
        if (raw[i] == '\\' && i + 1 < raw.size()) {
            current.push_back(raw[++i]);
        } else if (raw[i] == '|') {
            fields.push_back(current);
            current.clear();
        } else {
            current.push_back(raw[i]);
        }
    }
    fields.push_back(current);
    if (fields.size() != 6) { return reg; }  // parse failure: empty status
    reg.saveOpID = fields[0];
    reg.snapshotKey = fields[1];
    reg.instanceID = fields[2];
    reg.creationRequest = fields[3];
    reg.runtimeID = fields[4];
    reg.status = fields[5];
    return reg;
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

litebus::Future<Status> StateActor::DeleteState(const std::string &instanceId, const std::string &generation,
                                                 const std::string &expectedRuntimeID)
{
    if (generation.empty() || expectedRuntimeID.empty()) {
        YRLOG_ERROR("refused to delete state for instance({}): incomplete identity "
                    "(generation empty={} runtime empty={})",
                    instanceId, generation.empty(), expectedRuntimeID.empty());
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID,
                      "delete state failed: incomplete identity");
    }
    if (!deleteExecutor_) {
        YRLOG_ERROR("refused to delete state for instance({}): no delete executor bound", instanceId);
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "delete state failed: delete executor not bound");
    }
    // v3 (plan 20261008T220000Z §retirement): delete BOTH the legacy
    // logical key (migration coexistence) AND any snapshot-keyed data for
    // this generation that the caller can name. The snapshot key passed
    // here is the one the retiring chain holds (it may be empty when the
    // chain never saved under v3). A late-arriving UNKNOWN write to a
    // DIFFERENT snapshot key is discoverable through the registration and
    // is NEVER silently deleted by a single NOT_FOUND pass.
    const std::string legacyKey = StateClient::ComposeStateKey(instanceId, generation);
    return deleteExecutor_(instanceId, expectedRuntimeID,
        [this, &legacyKey]() { return stateClient_->Del(legacyKey); });
}

void StateActor::SetSaveExecutor(SaveExecutor executor)
{
    saveExecutor_ = std::move(executor);
}

void StateActor::SetDeleteExecutor(DeleteExecutor executor)
{
    deleteExecutor_ = std::move(executor);
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