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

namespace {
// Guard key in the meta-store for the persistent generation guard.
std::string GuardKeyFor(const std::string &instanceID, const std::string &creationRequest)
{
    return "stateguard:v1:" + instanceID + ":" + creationRequest;
}
constexpr const char *GUARD_RETIRED[] = { "RETIRED" };
}  // namespace

StateActor::StateActor(const std::shared_ptr<StateClient> &stateClient)
    : litebus::ActorBase("state_actor"), stateClient_(stateClient)
{
}

// ── v3: fire-and-forget conditional status update ─────────────────────────
// Marks a registration UNKNOWN only if it still holds the CAPTURED value
// (never unconditionally overwrites a rewritten or retired record).
void StateActor::MarkRegistrationUnknown(const std::string &regKey,
                                         const std::string &capturedValue,
                                         const std::string &guardKey,
                                         const std::string &runtimeID)
{
    if (registry_ == nullptr) { return; }
    SaveRegistration unknown;
    auto parsed = ParseRegistration(capturedValue);
    if (parsed.status.empty()) { return; }
    unknown = parsed;
    unknown.status = "UNKNOWN";
    auto txn = registry_->BeginTransaction();
    txn->If(meta_store::TxnCompare::OfValue(regKey, meta_store::CompareOperator::EQUAL, capturedValue));
    txn->Then(meta_store::TxnOperation::Create(regKey, SerializeRegistration(unknown),
                                                meta_store::PutOption{}));
    (void)txn->Commit();
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
    if (boundRequestID.empty()) {
        YRLOG_ERROR("refused to save state for instance({}) runtime({}): stream carries no bound "
                    "creation generation", instanceId, boundRuntimeID);
        return GenStateSaveResponse(common::ErrorCode::ERR_PARAM_INVALID,
                                    "save state failed: unbound stream identity");
    }
    if (!saveExecutor_) {
        YRLOG_ERROR("refused to save state for instance({}): no save executor bound", instanceId);
        return GenStateSaveResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                    "save state refused: save executor not bound");
    }
    if (registry_ == nullptr) {
        YRLOG_ERROR("refused to save state for instance({}): no save registry bound "
                    "(v3 requires registration-first)", instanceId);
        return GenStateSaveResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                    "save state refused: save registry not bound");
    }

    const std::string snapshotKey = StateClient::ComposeSnapshotKey(instanceId, boundRequestID);
    const auto lastColon = snapshotKey.rfind(':');
    const std::string saveOpID = snapshotKey.substr(lastColon + 1);
    const std::string guardKey = GuardKeyFor(instanceId, boundRequestID);
    const std::string regKey = RegistrationKey(instanceId, boundRequestID, saveOpID);

    SaveRegistration reg;
    reg.saveOpID = saveOpID;
    reg.snapshotKey = snapshotKey;
    reg.instanceID = instanceId;
    reg.creationRequest = boundRequestID;
    reg.runtimeID = boundRuntimeID;
    reg.status = "PENDING";
    const std::string pendingValue = SerializeRegistration(reg);

    // ── Phase 1: PERSIST REGISTRATION with the generation guard ─────────
    // Two-step idempotent guard: first try If(guard==ourRuntime), then
    // If(guard absent) with creation. Both are CONDITIONAL — an expired
    // runtime's registration FAILS at the guard, not later.
    auto regTxn = registry_->BeginTransaction();
    regTxn->If(meta_store::TxnCompare::OfValue(guardKey, meta_store::CompareOperator::EQUAL,
                                                boundRuntimeID));
    regTxn->Then(meta_store::TxnOperation::Create(regKey, pendingValue, meta_store::PutOption{}));

    return regTxn->Commit().Then(
        [this, instanceId, boundRuntimeID, boundRequestID, snapshotKey, saveOpID,
         guardKey, regKey, pendingValue, request]
        (const litebus::Future<std::shared_ptr<TxnResponse>> &f) ->
            litebus::Future<StateSaveResponse> {
            const auto &rsp = f.Get();
            if (rsp == nullptr || rsp->status.IsError() || !rsp->success) {
                // Guard mismatch: either a different runtime owns the guard
                // or the guard is absent (first save) — try creation.
                auto createTxn = registry_->BeginTransaction();
                createTxn->If(meta_store::TxnCompare::OfVersion(
                    guardKey, meta_store::CompareOperator::EQUAL, 0));
                createTxn->Then(meta_store::TxnOperation::Create(
                    guardKey, boundRuntimeID, meta_store::PutOption{}));
                createTxn->Then(meta_store::TxnOperation::Create(
                    regKey, pendingValue, meta_store::PutOption{}));
                return createTxn->Commit().Then(
                    [this, instanceId, boundRuntimeID, snapshotKey, saveOpID,
                     guardKey, regKey, pendingValue, request]
                    (const litebus::Future<std::shared_ptr<TxnResponse>> &cf) ->
                        litebus::Future<StateSaveResponse> {
                    const auto &crsp = cf.Get();
                    if (crsp == nullptr || crsp->status.IsError() || !crsp->success) {
                        YRLOG_ERROR("save state for instance({}) op({}): registration REFUSED — "
                                    "generation guard held by another runtime or retired",
                                    instanceId, saveOpID);
                        return GenStateSaveResponse(common::ErrorCode::ERR_INSTANCE_BUSY,
                            "save state refused: generation guard mismatch");
                    }
                    return ProceedAfterRegistration(instanceId, boundRuntimeID, snapshotKey,
                                                    saveOpID, guardKey, regKey, pendingValue, request);
                });
            }
            return ProceedAfterRegistration(instanceId, boundRuntimeID, snapshotKey,
                                            saveOpID, guardKey, regKey, pendingValue, request);
        });
}

// Phase 2 (DS write) + Phase 3 (conditional publish) — shared by both
// registration paths above.
litebus::Future<StateSaveResponse> StateActor::ProceedAfterRegistration(
    const std::string &instanceId, const std::string &boundRuntimeID,
    const std::string &snapshotKey, const std::string &saveOpID,
    const std::string &guardKey, const std::string &regKey,
    const std::string &pendingValue, const std::shared_ptr<StateSaveRequest> &request)
{
    // Phase 2: DS write (synchronous within the guarded executor).
    auto writeStatus = saveExecutor_(instanceId, boundRuntimeID,
        [this, &snapshotKey, &request]() {
            return stateClient_->Set(snapshotKey, request->state());
        });
    if (writeStatus.IsError()) {
        // UNKNOWN: mark the registration (conditional — only if it still
        // holds our PENDING value and the guard still holds our runtime).
        MarkRegistrationUnknown(regKey, pendingValue, guardKey, boundRuntimeID);
        YRLOG_WARN("save state for instance({}) op({}): DS write uncertain — registration "
                   "retained as UNKNOWN", instanceId, saveOpID);
        return GenStateSaveResponse(common::ErrorCode::ERR_INSTANCE_BUSY,
            "save state: DS write outcome UNKNOWN (registration retained)");
    }

    // Phase 3: CONDITIONAL PUBLISH — both the generation guard AND the
    // registration must still hold OUR captured values. An expired
    // runtime's late publish FAILS at the guard (root requirement B).
    SaveRegistration committed;
    committed.saveOpID = saveOpID;
    committed.snapshotKey = snapshotKey;
    committed.instanceID = instanceId;
    committed.creationRequest = boundRuntimeID;  // corrected below
    committed.creationRequest = pendingValue.empty() ? "" : ParseRegistration(pendingValue).creationRequest;
    committed.runtimeID = boundRuntimeID;
    committed.status = "COMMITTED";

    auto pubTxn = registry_->BeginTransaction();
    pubTxn->If(meta_store::TxnCompare::OfValue(guardKey, meta_store::CompareOperator::EQUAL,
                                                boundRuntimeID));
    pubTxn->If(meta_store::TxnCompare::OfValue(regKey, meta_store::CompareOperator::EQUAL,
                                                 pendingValue));
    pubTxn->Then(meta_store::TxnOperation::Create(regKey, SerializeRegistration(committed),
                                                    meta_store::PutOption{}));

    return pubTxn->Commit().Then(
        [this, instanceId, saveOpID, snapshotKey, regKey, pendingValue, guardKey, boundRuntimeID]
        (const litebus::Future<std::shared_ptr<TxnResponse>> &pf) -> StateSaveResponse {
        const auto &prsp = pf.Get();
        if (prsp != nullptr && !prsp->status.IsError() && prsp->success) {
            YRLOG_INFO("save state for instance({}) op({}): COMMITTED", instanceId, saveOpID);
            return GenStateSaveResponse(common::ErrorCode::ERR_NONE, "", snapshotKey);
        }
        // Publish conflict (root requirement C): the DS data IS safe on
        // its unique key, but the response is UNKNOWN — never ERR_NONE.
        MarkRegistrationUnknown(regKey, pendingValue, guardKey, boundRuntimeID);
        YRLOG_WARN("save state for instance({}) op({}): publish CONFLICT — data on unique "
                   "key, outcome UNKNOWN", instanceId, saveOpID);
        return GenStateSaveResponse(common::ErrorCode::ERR_INSTANCE_BUSY,
            "save state: publish conflict (data safe on unique key, outcome UNKNOWN)");
    });
}

litebus::Future<StateLoadResponse> StateActor::LoadState(const std::shared_ptr<StateLoadRequest> &request)
{
    if (request->checkpointid().empty() || !stateClient_) {
        YRLOG_ERROR("failed to load state: empty checkpoint id");
        return GenStateLoadResponse(common::ErrorCode::ERR_PARAM_INVALID, "load state failed: empty checkpoint id");
    }
    // Accept BOTH the v3 snapshot keys (state:snap:v1:...) and the legacy
    // composed keys (state:v1:...) — both are explicit references to
    // immutable data (the legacy key is only written by pre-v3 code that
    // has already stopped by deployment ordering). Bare/foreign references
    // are refused under the migration contract.
    const auto &ref = request->checkpointid();
    const bool isSnapshot = StateClient::IsSnapshotKey(ref);
    const bool isLegacy = !StateClient::ParseStateKey(ref).first.empty();
    if (!isSnapshot && !isLegacy) {
        YRLOG_ERROR("refused to load checkpoint({}): not a versioned reference", ref);
        return GenStateLoadResponse(common::ErrorCode::ERR_PARAM_INVALID,
                                    "load state failed: checkpoint reference is not versioned");
    }

    std::string state;
    auto status = stateClient_->Get(ref, state);
    if (status.IsError()) {
        YRLOG_ERROR("failed to load state: {}", status.ToString());
        return GenStateLoadResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                    "load state failed: " + status.ToString());
    }
    YRLOG_INFO("succeed to load checkpoint({}) state", ref);
    return GenStateLoadResponse(common::ErrorCode::ERR_NONE, "", state);
}

litebus::Future<Status> StateActor::DeleteState(const std::string &instanceId, const std::string &generation,
                                                 const std::string &expectedRuntimeID)
{
    if (generation.empty() || expectedRuntimeID.empty()) {
        YRLOG_ERROR("refused to delete state for instance({}): incomplete identity", instanceId);
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID,
                      "delete state failed: incomplete identity");
    }
    if (!deleteExecutor_) {
        YRLOG_ERROR("refused to delete state for instance({}): no delete executor bound", instanceId);
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "delete state failed: delete executor not bound");
    }
    // v3 retirement Phase 1: PERSISTENTLY BLOCK further publishes for this
    // generation — the guard is set to RETIRED, so any late registration
    // or publish for this (instance, generation) fails at the guard.
    if (registry_ != nullptr) {
        const std::string guardKey = GuardKeyFor(instanceId, generation);
        auto blockTxn = registry_->BeginTransaction();
        // Unconditional Put: retirement ALWAYS takes the guard (the delete
        // executor below already verified the retiring chain's authority).
        blockTxn->Then(meta_store::TxnOperation::Create(
            guardKey, GUARD_RETIRED[0], meta_store::PutOption{}));
        (void)blockTxn->Commit();
        YRLOG_INFO("retirement for instance({}) generation({}): guard set to RETIRED "
                   "(late publishes will fail)", instanceId, generation);
    }
    // v3 retirement Phase 2: delete the LEGACY key (the pre-v3 shared key;
    // pre-v3 writers are stopped by deployment ordering). Snapshot keys
    // for this generation are enumerable from the registration prefix in
    // the meta-store and are deleted by reconciliation — an UNKNOWN write
    // arriving after this Del is discoverable through its registration
    // and is NEVER cleared by a single NOT_FOUND (root requirement E).
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

void StateActor::BindSaveRegistry(const std::shared_ptr<MetaStoreClient> &metaStoreClient)
{
    registry_ = metaStoreClient;
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

// ── v3 registration helpers ────────────────────────────────────────────────

std::string StateActor::RegistrationKey(const std::string &instanceID,
                                        const std::string &creationRequest,
                                        const std::string &saveOpID)
{
    return "statereg:v1:" + instanceID + ":" + creationRequest + ":" + saveOpID;
}

std::string StateActor::SerializeRegistration(const SaveRegistration &reg)
{
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
    if (fields.size() != 6) { return reg; }
    reg.saveOpID = fields[0];
    reg.snapshotKey = fields[1];
    reg.instanceID = fields[2];
    reg.creationRequest = fields[3];
    reg.runtimeID = fields[4];
    reg.status = fields[5];
    return reg;
}

}  // namespace functionsystem::function_proxy
