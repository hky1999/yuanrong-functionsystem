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

#include <cerrno>
#include <cstdlib>

#include "async/asyncafter.hpp"
#include "async/async.hpp"
#include "common/utils/generate_message.h"
#include "common/utils/meta_store_kv_operation.h"
#include "meta_store_client/txn_transaction.h"

namespace functionsystem::function_proxy {
namespace {
// Retirement-replay cadence (root directive: bounded rounds, failures
// retained). The first round waits for the client to settle; the periodic
// cadence re-reads the stateret prefix forever — the replay IS the
// restart/recovery path for pending cleanups. Production defaults live in
// the atomics below; tests may shorten them.
constexpr int kReplayBatchSize = 8;
// HARD cap on CONCURRENT cleanups across all replay rounds, and on DS
// entries processed by ONE cleanup call (the rest of a huge generation
// reconverges on later rounds — CLEANED entries cost no DS work, so the
// window slides). The per-round prefix READ itself is a FULL scan: the
// meta-store GetOption model exposes no range end, so legal server-side
// pagination is impossible today — the full-scan read is a KNOWN,
// explicit gap (cost grows with the total tombstone count), not a
// hidden one.
constexpr int kReplayConcurrencyCap = 8;
constexpr int kMaxCleanupEntriesPerRound = 64;
constexpr const char *kRetirementKeyPrefix = "stateret:v1:";
}  // namespace

std::atomic<uint32_t> StateActor::replayInitialDelayMs{ 3000 };
std::atomic<uint32_t> StateActor::replayPeriodMs{ 60000 };

StateActor::StateActor(const std::shared_ptr<StateClient> &stateClient)
    : litebus::ActorBase("state_actor"), stateClient_(stateClient)
{
}

// ── Phase 0: AUTHORITATIVE OWNERSHIP VERIFICATION ──────────────────────────
// Reads the instance's AUTHORITATIVE etcd record through the REAL
// MetaStoreClient::Get — not the InstanceOperator wrapper (which drops
// create_revision and handles null/count-kvs inconsistently) — verifies
// the persisted standard InstanceInfo, and captures the kv's raw value +
// create_revision + mod_revision as the conditional-comparison inputs for
// BOTH the registration and the publish. An old runtime's late operations
// fail against the CURRENT record, never a self-appointed guard. The
// generation's CURRENT checkpoint reference is read IN PARALLEL and
// captured the same way: two saves of one generation hold the SAME owner
// record conditions, so the captured reference is what orders them — an
// old late publish must not drag the latest reference back. Both reads
// are ISSUED on the actor thread; every Then callback is PURE VALUE
// processing (no actor state).
litebus::Future<StateActor::AuthoritySnapshot> StateActor::VerifyOwnership(
    const std::string &instanceId,
    const std::string &boundRuntimeID,
    const std::string &boundRequestID)
{
    AuthoritySnapshot snapshot;  // verified=false
    if (registry_ == nullptr || !functionKeyResolver_) {
        YRLOG_ERROR("ownership check for instance({}): registry not bound "
                    "(BindSaveRegistry requires both metaClient and functionKeyResolver)",
                    instanceId);
        return snapshot;
    }
    // Resolve the function key for THIS instance+runtime+generation from
    // the control view; the resolver verifies the live machine still
    // matches the BOUND identity before answering.
    const std::string functionKey = functionKeyResolver_(instanceId, boundRuntimeID, boundRequestID);
    if (functionKey.empty()) {
        YRLOG_WARN("ownership check for instance({}) runtime({}) request({}): function "
                   "key resolution failed (machine not found in view, or view identity "
                   "no longer matches the bound identity)",
                   instanceId, boundRuntimeID, boundRequestID);
        return snapshot;
    }
    // Derive the authoritative key via the PRODUCTION path.
    auto keyOpt = GenInstanceKey(functionKey, instanceId, boundRequestID);
    if (keyOpt.IsNone()) {
        YRLOG_ERROR("ownership check for instance({}): GenInstanceKey failed "
                    "(function key malformed: {})", instanceId, functionKey);
        return snapshot;
    }
    snapshot.recordKey = keyOpt.Get();
    const std::string refKey = StateRefKey(instanceId, boundRequestID);
    const std::string fenceKey = RetirementKey(instanceId, boundRequestID);

    // All three reads issued HERE, on the actor thread (parallel, one
    // RTT): the authoritative record, the generation's CURRENT checkpoint
    // reference, and the generation's RETIREMENT FENCE. The fence is
    // checked FIRST — a retired generation refuses saves outright, and an
    // unreadable fence state refuses too (never assume not-retired).
    auto refFuture = registry_->Get(refKey, GetOption{});
    auto recordFuture = registry_->Get(snapshot.recordKey, GetOption{});
    return registry_->Get(fenceKey, GetOption{}).Then(
        [snapshot, instanceId, boundRuntimeID, boundRequestID, refFuture, recordFuture]
        (const std::shared_ptr<GetResponse> &fenceRsp) mutable -> litebus::Future<AuthoritySnapshot> {
            if (fenceRsp == nullptr || fenceRsp->status.IsError()) {
                YRLOG_WARN("ownership check for instance({}) request({}): retirement "
                           "fence state UNREADABLE — refusing save",
                           instanceId, boundRequestID);
                return snapshot;
            }
            if (!fenceRsp->kvs.empty()) {
                YRLOG_WARN("ownership check for instance({}) request({}): generation is "
                           "RETIRED (persistent fence present) — refusing save",
                           instanceId, boundRequestID);
                return snapshot;
            }
            return recordFuture.Then(
                [snapshot, instanceId, boundRuntimeID, boundRequestID, refFuture]
                (const std::shared_ptr<GetResponse> &rsp) mutable -> litebus::Future<AuthoritySnapshot> {
            if (rsp == nullptr || rsp->status.IsError()) {
                YRLOG_WARN("ownership check for instance({}) request({}): authoritative "
                           "record read FAILED — refusing (never self-appointing)",
                           instanceId, boundRequestID);
                return snapshot;
            }
            if (rsp->kvs.size() != 1) {
                YRLOG_WARN("ownership check for instance({}) request({}): authoritative "
                           "record ABSENT or ambiguous (kvs: {}) — refusing",
                           instanceId, boundRequestID, rsp->kvs.size());
                return snapshot;
            }
            const auto &kv = rsp->kvs.front();
            if (kv.mod_revision() <= 0) {
                YRLOG_WARN("ownership check for instance({}) request({}): non-positive "
                           "mod_revision({}) — refusing",
                           instanceId, boundRequestID, kv.mod_revision());
                return snapshot;
            }
            // Standard metadata parse — no hand-guessed JSON fields.
            InstanceInfo persisted;
            if (!TransToInstanceInfoFromJson(persisted, kv.value())) {
                YRLOG_WARN("ownership check for instance({}) request({}): authoritative "
                           "record is not a parsable InstanceInfo — refusing",
                           instanceId, boundRequestID);
                return snapshot;
            }
            // The PERSISTED identity must match the save's BOUND identity.
            if (persisted.instanceid() != instanceId
                || persisted.requestid() != boundRequestID
                || persisted.runtimeid() != boundRuntimeID) {
                YRLOG_WARN("ownership check for instance({}) request({}) runtime({}): "
                           "MISMATCH — persisted record says instance({}) request({}) "
                           "runtime({}); refusing save",
                           instanceId, boundRequestID, boundRuntimeID,
                           persisted.instanceid(), persisted.requestid(),
                           persisted.runtimeid());
                return snapshot;
            }
            // Capture the FULL kv identity for the conditional transactions.
            snapshot.recordValue = kv.value();
            snapshot.createRevision = kv.create_revision();
            snapshot.modRevision = kv.mod_revision();

            // Then capture the generation's CURRENT checkpoint reference.
            return refFuture.Then(
                [snapshot, instanceId, boundRequestID]
                (const std::shared_ptr<GetResponse> &refRsp) mutable -> AuthoritySnapshot {
                    if (refRsp == nullptr || refRsp->status.IsError()) {
                        YRLOG_WARN("ownership check for instance({}) request({}): "
                                   "checkpoint-reference read FAILED — refusing save "
                                   "(a reference we cannot see is one we never "
                                   "blindly overwrite)",
                                   instanceId, boundRequestID);
                        return snapshot;
                    }
                    if (!refRsp->kvs.empty()) {
                        if (refRsp->kvs.size() != 1) {
                            YRLOG_WARN("ownership check for instance({}) request({}): "
                                       "checkpoint reference AMBIGUOUS (kvs: {}) — refusing",
                                       instanceId, boundRequestID, refRsp->kvs.size());
                            return snapshot;
                        }
                        const auto &refKv = refRsp->kvs.front();
                        if (refKv.mod_revision() <= 0) {
                            YRLOG_WARN("ownership check for instance({}) request({}): "
                                       "checkpoint reference non-positive mod_revision({}) "
                                       "— refusing", instanceId, boundRequestID,
                                       refKv.mod_revision());
                            return snapshot;
                        }
                        // The existing reference must be a STRICT snapshot
                        // key naming THIS instance and generation — a
                        // corrupt or foreign reference blocks the save
                        // (it is never silently replaced).
                        const auto parsedRef = StateClient::ParseSnapshotKey(refKv.value());
                        if (parsedRef.saveOpID.empty()) {
                            YRLOG_WARN("ownership check for instance({}) request({}): "
                                       "existing checkpoint reference is not a strict "
                                       "snapshot key — refusing", instanceId, boundRequestID);
                            return snapshot;
                        }
                        if (parsedRef.instanceId != instanceId
                            || parsedRef.creationRequest != boundRequestID) {
                            YRLOG_WARN("ownership check for instance({}) request({}): "
                                       "existing checkpoint reference names instance({}) "
                                       "request({}) — refusing",
                                       instanceId, boundRequestID, parsedRef.instanceId,
                                       parsedRef.creationRequest);
                            return snapshot;
                        }
                        snapshot.refCapture.exists = true;
                        snapshot.refCapture.value = refKv.value();
                        snapshot.refCapture.createRevision = refKv.create_revision();
                        snapshot.refCapture.modRevision = refKv.mod_revision();
                    }
                    // else: explicitly absent — first publication for the
                    // generation guards on version==0.
                    snapshot.verified = true;
                    return snapshot;
                });
                });
        });
}

litebus::Future<StateSaveResponse> StateActor::SaveState(const std::string &instanceId,
                                                         const std::string &boundRuntimeID,
                                                         const std::string &boundRequestID,
                                                         const std::shared_ptr<StateSaveRequest> &request)
{
    if (instanceId.empty() || !stateClient_) {
        return GenStateSaveResponse(common::ErrorCode::ERR_PARAM_INVALID, "save state failed: empty instance id");
    }
    if (boundRequestID.empty()) {
        return GenStateSaveResponse(common::ErrorCode::ERR_PARAM_INVALID,
                                    "save state failed: unbound stream identity");
    }
    if (!saveExecutor_) {
        return GenStateSaveResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                    "save state refused: save executor not bound");
    }
    if (registry_ == nullptr || !functionKeyResolver_) {
        return GenStateSaveResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                    "save state refused: save registry not bound");
    }

    const std::string snapshotKey = StateClient::ComposeSnapshotKey(instanceId, boundRequestID);
    const auto lastColon = snapshotKey.rfind(':');
    const std::string saveOpID = snapshotKey.substr(lastColon + 1);

    // ── Phase 0 hop: the ownership read completes on a future-completion
    // thread; the registration phase that follows touches actor state
    // (registry_), so it is dispatched BACK onto this actor — never
    // [this] on the completion thread. The hop lambda captures values
    // only (the actor AID, strings, the shared_ptr request).
    const litebus::AID self = GetAID();
    return VerifyOwnership(instanceId, boundRuntimeID, boundRequestID).Then(
        [self, instanceId, boundRuntimeID, boundRequestID, snapshotKey, saveOpID, request]
        (const AuthoritySnapshot &authority) -> litebus::Future<StateSaveResponse> {
            return litebus::Async(self, &StateActor::OnOwnershipVerified, instanceId,
                                  boundRuntimeID, boundRequestID, snapshotKey, saveOpID,
                                  request, authority);
        });
}

// ── Phase 1: PERSIST REGISTRATION (actor thread) ───────────────────────────
litebus::Future<StateSaveResponse> StateActor::OnOwnershipVerified(
    const std::string &instanceId, const std::string &boundRuntimeID,
    const std::string &boundRequestID, const std::string &snapshotKey,
    const std::string &saveOpID, const std::shared_ptr<StateSaveRequest> &request,
    const AuthoritySnapshot &authority)
{
    if (!authority.verified) {
        return GenStateSaveResponse(common::ErrorCode::ERR_INSTANCE_BUSY,
            "save state refused: authoritative ownership verification failed");
    }

    // The registration NAMES ITS AUTHORITY: the authoritative record key
    // and the mod_revision it was validated against are persisted with it.
    SaveRegistration reg;
    reg.saveOpID = saveOpID;
    reg.snapshotKey = snapshotKey;
    reg.instanceID = instanceId;
    reg.creationRequest = boundRequestID;
    reg.runtimeID = boundRuntimeID;
    reg.status = "PENDING";
    reg.authorityRecordKey = authority.recordKey;
    reg.authorityModRevision = authority.modRevision;
    const std::string pendingValue = SerializeRegistration(reg);

    // Conditional on the AUTHORITATIVE record still holding BOTH the
    // captured raw value AND the captured mod_revision — an old runtime's
    // registration fails when the record has moved. No guard key, no
    // creation fallback. The generation's checkpoint reference must also
    // still be exactly what Phase 0 captured, the RETIREMENT FENCE must
    // still be absent (a retired generation accepts no new operations,
    // even mid-flight ones), and this operation's OWN registration key
    // must not already exist (an operation never re-registers over an
    // existing key).
    const std::string regKey = RegistrationKey(instanceId, boundRequestID, saveOpID);
    const std::string refKey = StateRefKey(instanceId, boundRequestID);
    const std::string fenceKey = RetirementKey(instanceId, boundRequestID);
    auto regTxn = registry_->BeginTransaction();
    regTxn->If(meta_store::TxnCompare::OfValue(
        authority.recordKey, meta_store::CompareOperator::EQUAL, authority.recordValue));
    regTxn->If(meta_store::TxnCompare::OfModifyVersion(
        authority.recordKey, meta_store::CompareOperator::EQUAL, authority.modRevision));
    AddRefGuardConditions(regTxn, refKey, authority.refCapture);
    regTxn->If(meta_store::TxnCompare::OfVersion(
        fenceKey, meta_store::CompareOperator::EQUAL, 0));
    regTxn->If(meta_store::TxnCompare::OfVersion(
        regKey, meta_store::CompareOperator::EQUAL, 0));
    regTxn->Then(meta_store::TxnOperation::Create(regKey, pendingValue, PutOption{}));

    // The completion callback is PURE (value checks only); the phase that
    // follows hops back onto the actor.
    const litebus::AID self = GetAID();
    return regTxn->Commit().Then(
        [self, instanceId, boundRuntimeID, boundRequestID, snapshotKey, saveOpID,
         regKey, pendingValue, authority, request]
        (const litebus::Future<std::shared_ptr<TxnResponse>> &f) -> litebus::Future<StateSaveResponse> {
            const auto &rsp = f.Get();
            if (rsp == nullptr || rsp->status.IsError() || !rsp->success) {
                YRLOG_WARN("save state for instance({}) op({}): registration "
                           "REFUSED — authoritative record or checkpoint reference "
                           "moved (value/mod_revision mismatch or transport error); "
                           "no creation fallback, no re-read retry",
                           instanceId, saveOpID);
                return GenStateSaveResponse(common::ErrorCode::ERR_INSTANCE_BUSY,
                    "save state refused: registration conditional failed "
                    "(authoritative record or checkpoint reference moved)");
            }
            return litebus::Async(self, &StateActor::OnRegistrationCommitted, instanceId,
                                  boundRuntimeID, boundRequestID, snapshotKey, saveOpID,
                                  regKey, pendingValue, authority, request);
        });
}

// ── Phase 2 (DS write) + Phase 3 (conditional publish) — actor thread ──────
litebus::Future<StateSaveResponse> StateActor::OnRegistrationCommitted(
    const std::string &instanceId, const std::string &boundRuntimeID,
    const std::string &boundRequestID, const std::string &snapshotKey,
    const std::string &saveOpID, const std::string &regKey,
    const std::string &pendingValue, const AuthoritySnapshot &authority,
    const std::shared_ptr<StateSaveRequest> &request)
{
    // Phase 2: DS write (synchronous within the guarded executor). The
    // executor carries the FULL bound identity — runtime AND creation
    // request — so the execution-point guard refuses a same-runtime
    // new-generation machine.
    auto writeStatus = saveExecutor_(instanceId, boundRuntimeID, boundRequestID,
        [this, &snapshotKey, &request]() {
            return stateClient_->Set(snapshotKey, request->state());
        });
    if (writeStatus.IsError()) {
        MarkRegistrationUnknown(regKey, pendingValue);
        YRLOG_WARN("save state for instance({}) op({}): DS write uncertain — "
                   "registration retained as UNKNOWN", instanceId, saveOpID);
        return GenStateSaveResponse(common::ErrorCode::ERR_INSTANCE_BUSY,
            "save state: DS write outcome UNKNOWN (registration retained)");
    }

    // Phase 3: CONDITIONAL PUBLISH. FIVE condition groups, all ANDed:
    //   1. the AUTHORITATIVE record still holds the captured raw value;
    //   2. ...at the captured mod_revision;
    //   3. the registration still holds THIS operation's captured PENDING
    //      value (a foreign or late writer is never published over);
    //   4. the generation's checkpoint reference is still EXACTLY what
    //      Phase 0 captured — absent-as-absent, or the captured value at
    //      the captured mod_revision. THIS is the anti-late-publish bar:
    //      two saves of one generation share the owner-record conditions,
    //      so whichever publish lands FIRST moves the reference and the
    //      loser's publish fails outright — an old late publish can never
    //      drag the reference back to an old snapshot. On conflict the
    //      operation returns UNKNOWN and keeps its registration; it NEVER
    //      re-reads the newer reference to retry the old publish.
    //   5. the RETIREMENT FENCE is still absent — once a generation is
    //      fenced, EVERY late publish fails, including one whose Phase 2
    //      DS write already landed (that snapshot becomes an orphan the
    //      retirement cleanup replays over).
    // Then: the COMMITTED registration and the persistent checkpoint
    // reference move in the SAME transaction — they land together or not
    // at all.
    SaveRegistration committed;
    committed.saveOpID = saveOpID;
    committed.snapshotKey = snapshotKey;
    committed.instanceID = instanceId;
    committed.creationRequest = boundRequestID;
    committed.runtimeID = boundRuntimeID;
    committed.status = "COMMITTED";
    committed.authorityRecordKey = authority.recordKey;
    committed.authorityModRevision = authority.modRevision;

    const std::string refKey = StateRefKey(instanceId, boundRequestID);
    auto pubTxn = registry_->BeginTransaction();
    pubTxn->If(meta_store::TxnCompare::OfValue(
        authority.recordKey, meta_store::CompareOperator::EQUAL, authority.recordValue));
    pubTxn->If(meta_store::TxnCompare::OfModifyVersion(
        authority.recordKey, meta_store::CompareOperator::EQUAL, authority.modRevision));
    pubTxn->If(meta_store::TxnCompare::OfValue(
        regKey, meta_store::CompareOperator::EQUAL, pendingValue));
    AddRefGuardConditions(pubTxn, refKey, authority.refCapture);
    pubTxn->If(meta_store::TxnCompare::OfVersion(
        RetirementKey(instanceId, boundRequestID), meta_store::CompareOperator::EQUAL, 0));
    pubTxn->Then(meta_store::TxnOperation::Create(regKey, SerializeRegistration(committed),
                                                  PutOption{}));
    pubTxn->Then(meta_store::TxnOperation::Create(refKey, snapshotKey, PutOption{}));

    // The completion callback is PURE (publish verdict only); the terminal
    // registration marking hops back onto the actor.
    const litebus::AID self = GetAID();
    return pubTxn->Commit().Then(
        [self, instanceId, saveOpID, snapshotKey, regKey, pendingValue]
        (const litebus::Future<std::shared_ptr<TxnResponse>> &f) -> litebus::Future<StateSaveResponse> {
            const auto &rsp = f.Get();
            const bool published = rsp != nullptr && !rsp->status.IsError() && rsp->success;
            return litebus::Async(self, &StateActor::OnPublishFinished, instanceId, saveOpID,
                                  snapshotKey, regKey, pendingValue, published);
        });
}

// ── Publish terminal (actor thread) ────────────────────────────────────────
StateSaveResponse StateActor::OnPublishFinished(const std::string &instanceId,
                                                const std::string &saveOpID,
                                                const std::string &snapshotKey,
                                                const std::string &regKey,
                                                const std::string &pendingValue,
                                                bool committed)
{
    if (committed) {
        YRLOG_INFO("save state for instance({}) op({}): COMMITTED", instanceId, saveOpID);
        return GenStateSaveResponse(common::ErrorCode::ERR_NONE, "", snapshotKey);
    }
    // Demotion is conditional on the registration still holding OUR
    // captured pendingValue: if the publish actually landed but the
    // response was lost, the registration reads COMMITTED and is NOT
    // downgraded.
    MarkRegistrationUnknown(regKey, pendingValue);
    YRLOG_WARN("save state for instance({}) op({}): publish CONFLICT — authoritative "
               "record, registration, or checkpoint reference moved (a concurrent save "
               "of this generation published first); data safe on unique key, outcome "
               "UNKNOWN, no re-read retry", instanceId, saveOpID);
    return GenStateSaveResponse(common::ErrorCode::ERR_INSTANCE_BUSY,
        "save state: publish conflict (data safe on unique key, outcome UNKNOWN)");
}

litebus::Future<StateLoadResponse> StateActor::LoadState(const std::shared_ptr<StateLoadRequest> &request)
{
    if (request->checkpointid().empty() || !stateClient_) {
        return GenStateLoadResponse(common::ErrorCode::ERR_PARAM_INVALID, "load state failed: empty checkpoint id");
    }
    const auto &ref = request->checkpointid();
    // STRICT acceptance: a fully-parsed snapshot key or a fully-parsed
    // legacy composed key — a scheme prefix alone is never acceptance.
    const bool isSnapshot = !StateClient::ParseSnapshotKey(ref).saveOpID.empty();
    const bool isLegacy = !StateClient::ParseStateKey(ref).first.empty();
    if (!isSnapshot && !isLegacy) {
        return GenStateLoadResponse(common::ErrorCode::ERR_PARAM_INVALID,
                                    "load state failed: checkpoint reference is not versioned");
    }

    std::string state;
    auto status = stateClient_->Get(ref, state);
    if (status.IsError()) {
        return GenStateLoadResponse(common::ErrorCode::ERR_INNER_SYSTEM_ERROR,
                                    "load state failed: " + status.ToString());
    }
    return GenStateLoadResponse(common::ErrorCode::ERR_NONE, "", state);
}

// ── Recover-side reference resolution ───────────────────────────────────────
// Reads the generation's published stateref ASYNC and resolves it to the
// checkpoint reference Recover must load. The ONLY inputs are the original
// instance + creation generation — a successor runtime's ID never
// substitutes for them. The RETIREMENT FENCE is checked FIRST and its
// state is part of the resolution: a retired generation refuses, and an
// UNREADABLE fence refuses too — the legacy fallback never bypasses the
// fence. Reference read errors, ambiguous reads, corrupt values, or
// references naming another instance/generation also REFUSE (ok=false):
// the caller surfaces the failure instead of degrading to a composed
// key. ONLY the fence AND the reference BOTH explicitly absent yields
// the legacy composed key (migration coexistence). Both reads are issued
// on the actor thread; every Then callback is pure value processing.
litebus::Future<StateActor::CheckpointRef> StateActor::ResolveCheckpointRef(
    const std::string &instanceId, const std::string &creationRequest)
{
    CheckpointRef out;  // ok=false
    if (instanceId.empty() || creationRequest.empty()) {
        out.reason = "empty instance or creation generation";
        return out;
    }
    if (registry_ == nullptr) {
        out.reason = "save registry not bound";
        return out;
    }
    const std::string refKey = StateRefKey(instanceId, creationRequest);
    auto refFuture = registry_->Get(refKey, GetOption{});
    return registry_->Get(RetirementKey(instanceId, creationRequest), GetOption{}).Then(
        [instanceId, creationRequest, refFuture]
        (const std::shared_ptr<GetResponse> &fenceRsp) -> litebus::Future<CheckpointRef> {
            CheckpointRef out;
            if (fenceRsp == nullptr || fenceRsp->status.IsError()) {
                out.reason = "retirement state unreadable (no legacy fallback)";
                return out;
            }
            if (!fenceRsp->kvs.empty()) {
                out.reason = "generation retired";
                return out;
            }
            return refFuture.Then(
                [instanceId, creationRequest]
                (const std::shared_ptr<GetResponse> &rsp) -> CheckpointRef {
                    CheckpointRef out;
                    if (rsp == nullptr || rsp->status.IsError()) {
                        out.reason = "reference read failed (no degradation to composed key)";
                        return out;
                    }
                    if (rsp->kvs.empty()) {
                        // EXPLICIT absence only (fence also explicitly
                        // absent): pre-v3 data migrates through the legacy
                        // composed key. No error path reaches this
                        // fallback.
                        out.ok = true;
                        out.legacy = true;
                        out.checkpointRef = StateClient::ComposeStateKey(instanceId, creationRequest);
                        return out;
                    }
                    if (rsp->kvs.size() != 1) {
                        out.reason = "ambiguous reference read";
                        return out;
                    }
                    const auto &kv = rsp->kvs.front();
                    const auto parsed = StateClient::ParseSnapshotKey(kv.value());
                    if (parsed.saveOpID.empty()) {
                        out.reason = "published reference is not a strict snapshot key";
                        return out;
                    }
                    // The published reference must name the ORIGINAL
                    // instance and creation generation being recovered —
                    // never a substituted identity.
                    if (parsed.instanceId != instanceId || parsed.creationRequest != creationRequest) {
                        out.reason = "published reference names a different instance/generation";
                        return out;
                    }
                    out.ok = true;
                    out.checkpointRef = kv.value();
                    return out;
                });
        });
}

// ── v3 persistent retirement fence ──────────────────────────────────────────
// RetireGeneration: fence-only. The tombstone read runs FIRST — a replay
// (authoritative record possibly already deleted by DelInstance) verifies
// the EXISTING tombstone against the caller's bound identity and needs no
// other authority. Only when the tombstone is explicitly absent does the
// first-attempt path derive authority from the LIVE record and commit the
// fence conditionally on its captured raw value + mod_revision (identity
// verified first). No tombstone AND no readable record = REFUSED — never
// an unconditional RETIRED Put after the owner disappeared.
litebus::Future<StateActor::RetireReport> StateActor::RetireGeneration(
    const std::string &instanceId, const std::string &creationRequest,
    const std::string &runtimeID)
{
    RetireReport refused;
    if (instanceId.empty() || creationRequest.empty() || runtimeID.empty()) {
        refused.reason = "incomplete retiring identity";
        return refused;
    }
    if (registry_ == nullptr || !functionKeyResolver_) {
        refused.reason = "registry not bound";
        return refused;
    }
    return registry_->Get(RetirementKey(instanceId, creationRequest), GetOption{}).Then(
        [self = GetAID(), instanceId, creationRequest, runtimeID]
        (const std::shared_ptr<GetResponse> &fenceRsp) -> litebus::Future<RetireReport> {
            if (fenceRsp == nullptr || fenceRsp->status.IsError()) {
                RetireReport refused;
                refused.reason = "tombstone unreadable — retirement refused (fail closed)";
                return refused;
            }
            if (fenceRsp->kvs.size() == 1) {
                // Replay: the tombstone IS the persistent identity proof —
                // accepted only in STRICT canonical RETIRED form.
                FenceRecord fence;
                RetireReport report;
                if (!ParseStrictFence(fenceRsp->kvs.front().value(), fence)
                    || fence.instanceID != instanceId
                    || fence.creationRequest != creationRequest || fence.runtimeID != runtimeID) {
                    report.reason = "existing tombstone not a valid proof for this identity — "
                                    "retirement refused";
                    return report;
                }
                report.fenced = true;  // fenceFresh stays false: verified existing
                return report;
            }
            if (fenceRsp->kvs.size() > 1) {
                RetireReport refused;
                refused.reason = "ambiguous tombstone read — retirement refused";
                return refused;
            }
            // Explicitly absent: first attempt — derive authority from the
            // LIVE record ON THE ACTOR (the resolver reads the control
            // view; never from this completion thread).
            return litebus::Async(self, &StateActor::DeriveRetirementAuthority, instanceId,
                                  creationRequest, runtimeID);
        });
}

// First-attempt path (actor thread): resolver + production key + raw Get +
// identity verify, then the conditional fence transaction.
litebus::Future<StateActor::RetireReport> StateActor::DeriveRetirementAuthority(
    const std::string &instanceId, const std::string &creationRequest,
    const std::string &runtimeID)
{
    RetireReport refused;
    const std::string functionKey = functionKeyResolver_(instanceId, runtimeID, creationRequest);
    if (functionKey.empty()) {
        refused.reason = "function key resolution failed (machine gone or identity mismatch)";
        return refused;
    }
    auto keyOpt = GenInstanceKey(functionKey, instanceId, creationRequest);
    if (keyOpt.IsNone()) {
        refused.reason = "GenInstanceKey failed (function key malformed)";
        return refused;
    }
    const std::string recordKey = keyOpt.Get();
    return registry_->Get(recordKey, GetOption{}).Then(
        [self = GetAID(), instanceId, creationRequest, runtimeID, recordKey]
        (const std::shared_ptr<GetResponse> &rsp) -> litebus::Future<RetireReport> {
            if (rsp == nullptr || rsp->status.IsError() || rsp->kvs.size() != 1) {
                RetireReport refused;
                refused.reason = "authoritative record unreadable/absent and no tombstone — "
                                 "identity unprovable, retirement refused (never "
                                 "self-appointed)";
                return refused;
            }
            const auto &kv = rsp->kvs.front();
            if (kv.mod_revision() <= 0) {
                RetireReport refused;
                refused.reason = "non-positive record mod_revision";
                return refused;
            }
            InstanceInfo persisted;
            if (!TransToInstanceInfoFromJson(persisted, kv.value())) {
                RetireReport refused;
                refused.reason = "authoritative record is not a parsable InstanceInfo";
                return refused;
            }
            if (persisted.instanceid() != instanceId
                || persisted.requestid() != creationRequest
                || persisted.runtimeid() != runtimeID) {
                RetireReport refused;
                refused.reason = "authoritative record identity mismatch — retirement refused";
                return refused;
            }
            return litebus::Async(self, &StateActor::CommitRetirementFence, instanceId,
                                  creationRequest, runtimeID, recordKey, kv.value(),
                                  kv.mod_revision());
        });
}

// Actor phase: the fence transaction — If(record at captured value AND
// mod_revision AND tombstone not exists) Then(Put tombstone naming the
// identity + the authority it was conditioned on).
litebus::Future<StateActor::RetireReport> StateActor::CommitRetirementFence(
    const std::string &instanceId, const std::string &creationRequest,
    const std::string &runtimeID, const std::string &recordKey,
    const std::string &recordValue, int64_t recordModRevision)
{
    FenceRecord fence;
    fence.instanceID = instanceId;
    fence.creationRequest = creationRequest;
    fence.runtimeID = runtimeID;
    fence.authorityRecordKey = recordKey;
    fence.authorityModRevision = recordModRevision;
    fence.status = "RETIRED";

    const std::string fenceKey = RetirementKey(instanceId, creationRequest);
    auto txn = registry_->BeginTransaction();
    txn->If(meta_store::TxnCompare::OfValue(recordKey, meta_store::CompareOperator::EQUAL,
                                            recordValue));
    txn->If(meta_store::TxnCompare::OfModifyVersion(recordKey, meta_store::CompareOperator::EQUAL,
                                                    recordModRevision));
    txn->If(meta_store::TxnCompare::OfVersion(fenceKey, meta_store::CompareOperator::EQUAL, 0));
    txn->Then(meta_store::TxnOperation::Create(fenceKey, SerializeFence(fence), PutOption{}));

    const litebus::AID self = GetAID();
    return txn->Commit().Then(
        [self, instanceId, creationRequest, runtimeID]
        (const litebus::Future<std::shared_ptr<TxnResponse>> &f) -> litebus::Future<RetireReport> {
            const auto &rsp = f.Get();
            if (rsp != nullptr && !rsp->status.IsError() && rsp->success) {
                RetireReport report;
                report.fenced = true;
                report.fenceFresh = true;
                return report;
            }
            // The record moved or a concurrent replier fenced first: the
            // re-verify hops back onto the actor.
            return litebus::Async(self, &StateActor::OnRetireFenceCommitFailed, instanceId,
                                  creationRequest, runtimeID);
        });
}

// Actor phase after a failed fence commit: the ONLY admissible recovery is
// an existing tombstone proving this exact identity.
litebus::Future<StateActor::RetireReport> StateActor::OnRetireFenceCommitFailed(
    const std::string &instanceId, const std::string &creationRequest,
    const std::string &runtimeID)
{
    return registry_->Get(RetirementKey(instanceId, creationRequest), GetOption{}).Then(
        [instanceId, creationRequest, runtimeID]
        (const std::shared_ptr<GetResponse> &fenceRsp) -> RetireReport {
            RetireReport report;
            if (fenceRsp != nullptr && !fenceRsp->status.IsError()
                && fenceRsp->kvs.size() == 1) {
                FenceRecord fence;
                if (ParseStrictFence(fenceRsp->kvs.front().value(), fence)
                    && fence.instanceID == instanceId
                    && fence.creationRequest == creationRequest && fence.runtimeID == runtimeID) {
                    report.fenced = true;  // concurrent replier won; identity verified
                    return report;
                }
                report.reason = "tombstone not a valid proof for this identity — refused";
                return report;
            }
            report.reason = "fence commit failed and no verifiable tombstone — refused";
            return report;
        });
}

// ── v3 retirement snapshot cleanup ──────────────────────────────────────────
// Idempotent, replayable. ORDERING (root defect A): the persistent
// tombstone is confirmed FIRST; only after that confirmation may the
// registration enumeration and reference reads be issued — issuing them
// earlier could enumerate empty, then watch a late old-save registration
// commit, then the fence commit, and report all-cleaned while missing
// that registrant. Post-fence the save-side transactions (which all carry
// the fence-absent condition) can no longer move this generation, so a
// read after the fence sees every registration that will ever exist.
// The DataSystem is NEVER enumerated — only exact per-registration keys
// from the meta-store registration records are deleted, and only after
// every element of the entry validates against THIS generation. The
// stateref delete is CONDITIONAL on the captured value + mod_revision.
// Tombstone and registrations are retained as the replay record.
litebus::Future<StateActor::RetireReport> StateActor::CleanupRetiredSnapshots(
    const std::string &instanceId, const std::string &creationRequest,
    const std::string &runtimeID)
{
    RetireReport refused;
    if (instanceId.empty() || creationRequest.empty() || runtimeID.empty()) {
        refused.reason = "incomplete retiring identity";
        return refused;
    }
    if (registry_ == nullptr || stateClient_ == nullptr) {
        refused.reason = "registry or state client not bound";
        return refused;
    }
    // The tombstone read ALONE first — nothing else is issued until it is
    // confirmed.
    return registry_->Get(RetirementKey(instanceId, creationRequest), GetOption{}).Then(
        [self = GetAID(), instanceId, creationRequest, runtimeID]
        (const std::shared_ptr<GetResponse> &fenceRsp) -> litebus::Future<RetireReport> {
            RetireReport refused;
            if (fenceRsp == nullptr || fenceRsp->status.IsError()) {
                refused.reason = "tombstone unreadable — cleanup refused (identity unproven)";
                return refused;
            }
            if (fenceRsp->kvs.empty()) {
                refused.reason = "no tombstone — cleanup refused (fence first; never "
                                 "self-prove identity)";
                return refused;
            }
            if (fenceRsp->kvs.size() != 1) {
                refused.reason = "ambiguous tombstone read — cleanup refused";
                return refused;
            }
            FenceRecord fence;
            if (!ParseStrictFence(fenceRsp->kvs.front().value(), fence)) {
                refused.reason = "tombstone is not a strict canonical RETIRED record";
                return refused;
            }
            if (fence.instanceID != instanceId || fence.creationRequest != creationRequest
                || fence.runtimeID != runtimeID) {
                refused.reason = "tombstone identity mismatch — cleanup refused";
                return refused;
            }
            // Tombstone confirmed: the enumeration (and reference read) may
            // now be issued — ON the actor, never from this completion
            // thread. The RAW tombstone value travels along: it carries
            // the per-generation cleanup cursor AND conditions the
            // cursor's later persisted advance.
            return litebus::Async(self, &StateActor::EnumerateRetiredGeneration, instanceId,
                                  creationRequest, runtimeID,
                                  fenceRsp->kvs.front().value());
        });
}

// Actor phase, POST-fence: enumeration + reference read issued here (in
// parallel with each other — both are after the tombstone confirmation,
// which is the ordering that matters). The chained callbacks are PURE
// (strict per-entry validation, reference capture); execution hops back
// onto the actor.
litebus::Future<StateActor::RetireReport> StateActor::EnumerateRetiredGeneration(
    const std::string &instanceId, const std::string &creationRequest,
    const std::string &runtimeID, const std::string &fenceValue)
{
    (void)runtimeID;
    const std::string tablePrefix = registry_->GetTablePrefix();
    // The per-generation CURSOR from the tombstone — the entry cap would
    // starve the tail of a huge generation without it (the same head 64
    // would return every round).
    FenceRecord fence = ParseFence(fenceValue);
    const std::string cursor = fence.cleanupCursor;
    GetOption regPrefixOpt;
    regPrefixOpt.prefix = true;
    auto regFuture = registry_->Get(RegistrationPrefix(instanceId, creationRequest), regPrefixOpt);
    auto refFuture = registry_->Get(StateRefKey(instanceId, creationRequest), GetOption{});
    return regFuture.Then(
        [self = GetAID(), instanceId, creationRequest, tablePrefix, refFuture, fenceValue, cursor]
        (const std::shared_ptr<GetResponse> &regRsp) mutable -> litebus::Future<RetireReport> {
            RetireReport refused;
            if (regRsp == nullptr || regRsp->status.IsError()) {
                refused.reason = "registration enumeration failed — retryable";
                return refused;
            }
            // Classify EVERY entry (the read itself is a full prefix scan —
            // see the cadence note: memory for one generation's
            // registrations is unbounded and stated, not hidden).
            std::map<std::string, RegistrationCleanup> sorted;
            for (const auto &kv : regRsp->kvs) {
                // Returned keys carry the meta-store table prefix.
                auto entry = ClassifyRegistrationEntry(TrimKeyPrefix(kv.key(), tablePrefix),
                                                       kv.value(), instanceId, creationRequest);
                sorted.emplace(entry.regKey, std::move(entry));
            }
            // CURSOR-FAIR SELECTION: resume AFTER the cursor, wrap to the
            // head if quota remains. CLEANED and invalid entries pass FREE
            // (no DS quota — a converged head never blocks the tail);
            // PENDING/UNKNOWN/COMMITTED entries consume one DS-work slot
            // each. Quota exhaustion leaves the remainder for the NEXT
            // round, which resumes after everything already processed —
            // bad or permanently-uncertain heads cannot starve the tail.
            RetirementPlan plan;
            plan.tombstoneKey = RetirementKey(instanceId, creationRequest);
            plan.tombstoneValue = fenceValue;
            size_t dsWork = 0;
            std::string lastProcessed;
            bool quotaFull = false;
            auto tryAdd = [&plan, &dsWork, &lastProcessed, &quotaFull](
                              const std::pair<const std::string, RegistrationCleanup> &item) {
                if (quotaFull) { return; }
                if (!item.second.invalid && !item.second.cleanedMarker) {
                    if (dsWork >= static_cast<size_t>(kMaxCleanupEntriesPerRound)) {
                        quotaFull = true;
                        return;
                    }
                    dsWork += 1;
                }
                plan.entries.push_back(item.second);
                lastProcessed = item.first;
            };
            for (auto it = sorted.upper_bound(cursor); it != sorted.end(); ++it) {
                tryAdd(*it);
            }
            if (!quotaFull) {
                for (auto it = sorted.begin();
                     it != sorted.end() && it->first.compare(cursor) <= 0; ++it) {
                    tryAdd(*it);
                }
            }
            plan.truncated = quotaFull;
            // Cursor advance: quota exhaustion keeps the LAST PROCESSED key
            // (the next round resumes after it); a completed full cycle
            // resets to "" so the next round restarts from the head.
            plan.newCursor = quotaFull ? lastProcessed : std::string();
            plan.cursorAdvanced = plan.newCursor != cursor;
            return refFuture.Then(
                [self, instanceId, creationRequest, plan]
                (const std::shared_ptr<GetResponse> &refRsp) mutable -> litebus::Future<RetireReport> {
                    RetirementPlan localPlan = plan;
                    if (refRsp == nullptr || refRsp->status.IsError()) {
                        localPlan.refUnknown = true;
                        localPlan.refDetail = "reference unreadable — conditional delete "
                                              "skipped (replay re-reads)";
                    } else if (refRsp->kvs.empty()) {
                        // explicitly absent — nothing to delete
                    } else if (refRsp->kvs.size() > 1) {
                        localPlan.refUnknown = true;
                        localPlan.refDetail = "ambiguous reference read (kvs>1) — "
                                              "conditional delete refused";
                    } else {
                        // Root defect B: an existing reference is only a
                        // deletion candidate when it STRICTLY parses, names
                        // THIS instance/generation, and carries a positive
                        // mod_revision — corrupt or foreign values are never
                        // conditionally deleted nor reported normal.
                        const auto &refKv = refRsp->kvs.front();
                        const auto parsedRef = StateClient::ParseSnapshotKey(refKv.value());
                        if (refKv.mod_revision() <= 0 || parsedRef.saveOpID.empty()
                            || parsedRef.instanceId != instanceId
                            || parsedRef.creationRequest != creationRequest) {
                            localPlan.refUnknown = true;
                            localPlan.refDetail = "existing reference corrupt or foreign — "
                                                  "conditional delete refused";
                        } else {
                            localPlan.refExists = true;
                            localPlan.refValue = refKv.value();
                            localPlan.refModRevision = refKv.mod_revision();
                        }
                    }
                    return litebus::Async(self, &StateActor::FinishRetirementCleanup,
                                          instanceId, creationRequest, localPlan);
                });
        });
}

// Actor phase: conditional stateref delete (captured value + mod_revision —
// protects any concurrent writer/replayer), then the Del+promote chain.
litebus::Future<StateActor::RetireReport> StateActor::FinishRetirementCleanup(
    const std::string &instanceId, const std::string &creationRequest,
    const RetirementPlan &plan)
{
    if (!plan.refExists) {
        // No reference to conditionally delete. refAbsent is TRUE only
        // for an EXPLICIT absence — an unreadable/corrupt reference
        // (refUnknown) must never masquerade as absent in the report.
        return DelThenPromote(instanceId, creationRequest, plan, false, !plan.refUnknown);
    }
    const std::string refKey = StateRefKey(instanceId, creationRequest);
    auto txn = registry_->BeginTransaction();
    txn->If(meta_store::TxnCompare::OfValue(
        refKey, meta_store::CompareOperator::EQUAL, plan.refValue));
    txn->If(meta_store::TxnCompare::OfModifyVersion(
        refKey, meta_store::CompareOperator::EQUAL, plan.refModRevision));
    txn->Then(meta_store::TxnOperation::Create(refKey, DeleteOption{}));

    // OnComplete bridging (NOT Then): a FAILED commit future skips Then
    // callbacks entirely, and this verdict must be reached on every
    // outcome.
    const litebus::AID self = GetAID();
    auto promise = std::make_shared<litebus::Promise<RetireReport>>();
    txn->Commit().OnComplete(
        [self, promise, instanceId, creationRequest, plan]
        (const litebus::Future<std::shared_ptr<TxnResponse>> &f) mutable {
            const bool failed = f.IsError();
            const auto &rsp = failed ? std::shared_ptr<TxnResponse>() : f.Get();
            const bool deleted = !failed && rsp != nullptr && !rsp->status.IsError()
                && rsp->success;
            RetirementPlan effective = plan;
            if (!deleted && effective.refDetail.empty()) {
                effective.refDetail = failed
                    ? "conditional reference delete future FAILED — replay re-reads"
                    : "conditional reference delete failed (moved or transport) — replay re-reads";
            }
            litebus::Async(self, &StateActor::DelThenPromote, instanceId, creationRequest,
                           effective, deleted, false)
                .OnComplete([promise](const litebus::Future<RetireReport> &fin) {
                    if (fin.IsError()) {
                        promise->SetFailed(fin.GetErrorCode());
                        return;
                    }
                    promise->SetValue(fin.Get());
                });
        });
    return promise->GetFuture();
}

// Actor phase: Del + chain into the awaited marker promotions.
litebus::Future<StateActor::RetireReport> StateActor::DelThenPromote(
    const std::string &instanceId, const std::string &creationRequest,
    const RetirementPlan &plan, bool refDeleted, bool refAbsent)
{
    auto report = DelRetiredSnapshots(instanceId, creationRequest, plan, refDeleted, refAbsent);
    return PromoteNextCleanedMarker(instanceId, creationRequest, std::move(report), 0);
}

// Actor phase: exact DS Del per validated entry. The tombstone and every
// registration are RETAINED — they are the replay record. This phase only
// deletes and classifies; the persistent CLEANED proofs are recorded by
// the AWAITED marker transactions in PromoteNextCleanedMarker, which the
// FinishRetirementCleanup chain continues into. Classification rules:
//   - an existing CLEANED marker proves `deleted` with no further Del
//     (convergent replay; once COMMITTED, no writer ever touches that
//     unique snapshot key again, so CLEANED ⇒ data gone forever);
//   - COMMITTED + Del OK ⇒ deleted, pending its AWAITED marker write;
//   - PENDING/UNKNOWN + Del OK ⇒ at best `absent` — PROVES NOTHING (the
//     save's DS write may still be in flight; a single NOT_FOUND-class
//     result is never promotion material);
//   - Del error ⇒ uncertain, replay.
// UNCONVERGED BOUNDARY (documented, not papered over): if a crash lands
// between a successful Del and its CLEANED marker, the replay re-Dels a
// key whose data is already gone; if the DataSystem SDK folds a
// delete-of-absent into BP_DATASYSTEM_ERROR (the client collapses every
// raw rc into that one code), the replay classifies it uncertain — an
// honest, conservatively-unproven state that repeats until a precise
// not-found distinction exists. It is NEVER reported as cleaned.
StateActor::RetireReport StateActor::DelRetiredSnapshots(const std::string &instanceId,
                                                         const std::string &creationRequest,
                                                         const RetirementPlan &plan,
                                                         bool refDeleted, bool refAbsent)
{
    (void)instanceId;
    (void)creationRequest;
    RetireReport report;
    report.fenced = true;
    report.enumerated = true;
    report.truncated = plan.truncated;
    report.refDeleted = refDeleted;
    report.refAbsent = refAbsent;
    report.refUnknown = plan.refUnknown;
    report.refDetail = plan.refDetail;
    report.tombstoneKey = plan.tombstoneKey;
    report.tombstoneValue = plan.tombstoneValue;
    report.newCursor = plan.newCursor;
    report.cursorAdvanced = plan.cursorAdvanced;
    report.entries = plan.entries;

    for (auto &entry : report.entries) {
        if (entry.invalid) { continue; }  // never delete what did not fully validate
        if (entry.cleanedMarker) {
            entry.deleted = true;
            entry.detail = "proven by persistent CLEANED marker";
            continue;
        }
        auto delStatus = stateClient_->Del(entry.snapshotKey);
        if (delStatus.IsOk()) {
            if (entry.wasCommitted) {
                entry.deleted = true;  // marker written (and awaited) next phase
            } else {
                entry.absent = true;
                entry.detail = "removed-or-never-written (PENDING/UNKNOWN registration; a "
                               "single NOT_FOUND proves nothing — registration retained "
                               "for replay)";
            }
        } else {
            entry.uncertain = true;
            entry.detail = delStatus.ToString();
        }
    }
    return report;  // allCleaned is finalized only after the marker phase
}

// Actor phase: walks entries that deleted COMMITTED data and commits the
// conditional COMMITTED→CLEANED marker transaction one at a time, AWAITING
// each verdict. The persistent proof is recorded ONLY on a confirmed
// success — a null response, a status failure, success=false, or a failed
// future leaves the entry markerPending (replay re-derives; never
// reported as closed). The condition (registration still holding the
// captured value) makes racing replays idempotent: the loser's condition
// simply sees CLEANED and reports the pre-existing proof.
litebus::Future<StateActor::RetireReport> StateActor::PromoteNextCleanedMarker(
    const std::string &instanceId, const std::string &creationRequest,
    RetireReport report, size_t index)
{
    while (index < report.entries.size()) {
        const auto &entry = report.entries[index];
        if (!entry.invalid && entry.deleted && !entry.cleanedMarker
            && entry.wasCommitted && !entry.markerPending) {
            break;  // this entry needs its marker written and awaited
        }
        ++index;
    }
    if (index >= report.entries.size()) {
        // Terminal: finalize allCleaned from the FINAL per-entry states.
        // The reference is handled iff it was conditionally deleted or was
        // EXPLICITLY absent at capture (carried as refAbsent — never
        // inferred from strings); an unresolved or failed one blocks
        // closure.
        const bool refHandled = report.refDeleted || report.refAbsent;
        // A truncated round proves nothing about the entries it never saw.
        bool allCleaned = refHandled && !report.refUnknown && !report.truncated;
        for (const auto &entry : report.entries) {
            if (entry.invalid || entry.absent || entry.uncertain || entry.markerPending) {
                allCleaned = false;
                continue;
            }
            allCleaned = allCleaned && entry.deleted && entry.cleanedMarker;
        }
        report.allCleaned = allCleaned;
        // Persist the CURSOR ADVANCE (conditional on the captured
        // tombstone value — a concurrent replayer cannot clobber a moved
        // tombstone). Progress marker, not a proof: a failed write only
        // means the next round re-processes some already-handled entries
        // (CLEANED ones pass free), so fire-and-report is honest.
        if (report.cursorAdvanced && !report.tombstoneKey.empty() && registry_ != nullptr) {
            auto fence = ParseFence(report.tombstoneValue);
            if (fence.status == "RETIRED") {
                fence.cleanupCursor = report.newCursor;
                auto txn = registry_->BeginTransaction();
                txn->If(meta_store::TxnCompare::OfValue(
                    report.tombstoneKey, meta_store::CompareOperator::EQUAL,
                    report.tombstoneValue));
                txn->Then(meta_store::TxnOperation::Create(
                    report.tombstoneKey, SerializeFence(fence), PutOption{}));
                (void)txn->Commit();
            }
        }
        return report;
    }

    // Build the conditional marker transaction for entries[index].
    const auto &entry = report.entries[index];
    auto parsed = ParseRegistration(entry.capturedValue);
    if (parsed.status != "COMMITTED") {
        // Defensive: the entry claimed a COMMITTED deletion; a mismatched
        // capture stays pending, never fabricated.
        auto next = report;
        next.entries[index].markerPending = true;
        next.entries[index].detail = "captured value is not COMMITTED — marker refused";
        return litebus::Async(GetAID(), &StateActor::PromoteNextCleanedMarker, instanceId,
                              creationRequest, std::move(next), index + 1);
    }
    parsed.status = "CLEANED";
    auto txn = registry_->BeginTransaction();
    txn->If(meta_store::TxnCompare::OfValue(entry.regKey, meta_store::CompareOperator::EQUAL,
                                            entry.capturedValue));
    txn->Then(meta_store::TxnOperation::Create(entry.regKey, SerializeRegistration(parsed),
                                               PutOption{}));

    // OnComplete bridging (NOT Then): the marker verdict must be reached
    // on EVERY outcome — a failed future skips Then callbacks entirely
    // and would lose the report.
    const litebus::AID self = GetAID();
    auto promise = std::make_shared<litebus::Promise<RetireReport>>();
    txn->Commit().OnComplete(
        [self, promise, instanceId, creationRequest, report, index]
        (const litebus::Future<std::shared_ptr<TxnResponse>> &f) mutable {
            RetireReport next = report;
            const auto &rsp = f.Get();
            if (!f.IsError() && rsp != nullptr && !rsp->status.IsError() && rsp->success) {
                next.entries[index].cleanedMarker = true;  // proof PERSISTED
            } else {
                // Null response / status failure / success=false / FAILED
                // future: the DS data is gone but the persistent proof is
                // NOT recorded — replay re-derives; never reported closed.
                next.entries[index].markerPending = true;
                next.entries[index].detail = f.IsError()
                    ? "CLEANED marker future FAILED — replay re-derives"
                    : "DS data deleted but CLEANED marker write failed — replay re-derives";
            }
            litebus::Async(self, &StateActor::PromoteNextCleanedMarker, instanceId,
                           creationRequest, std::move(next), index + 1)
                .OnComplete([promise](const litebus::Future<RetireReport> &fin) {
                    if (fin.IsError()) {
                        promise->SetFailed(fin.GetErrorCode());
                        return;
                    }
                    promise->SetValue(fin.Get());
                });
        });
    return promise->GetFuture();
}

// Pure per-entry validation. EVERYTHING must line up (root defect C):
//   - the key strictly parses AND names this instance/generation;
//   - the value is a COMPLETE tuple: identity fields for this generation,
//     non-empty runtime, non-empty authority record key, positive
//     authority mod_revision, and a status in the allowed set
//     (PENDING/UNKNOWN/COMMITTED/CLEANED);
//   - THREE-WAY saveOpID agreement: key segment == tuple.saveOpID ==
//     strictly-parsed snapshot key's saveOpID (this also transitively
//     enforces the canonical-UUID form of the key segment);
//   - the snapshot key strictly parses to this instance/generation.
// Anything less ⇒ invalid, never deleted.
StateActor::RegistrationCleanup StateActor::ClassifyRegistrationEntry(
    const std::string &regKey, const std::string &rawValue,
    const std::string &instanceId, const std::string &creationRequest)
{
    RegistrationCleanup entry;
    entry.regKey = regKey;
    entry.capturedValue = rawValue;

    const auto keyParts = ParseRegistrationKey(regKey);
    if (keyParts.saveOpID.empty()) {
        entry.invalid = true;
        entry.detail = "registration key does not strictly parse";
        return entry;
    }
    if (keyParts.instanceId != instanceId || keyParts.creationRequest != creationRequest) {
        entry.invalid = true;
        entry.detail = "registration key names a foreign generation (prefix boundary leak?)";
        return entry;
    }
    const auto tuple = ParseRegistration(rawValue);
    if (tuple.status.empty() || tuple.instanceID != instanceId
        || tuple.creationRequest != creationRequest) {
        entry.invalid = true;
        entry.detail = "registration value incomplete or foreign";
        return entry;
    }
    if (tuple.saveOpID != keyParts.saveOpID) {
        entry.invalid = true;
        entry.detail = "tuple saveOpID does not match its registration key";
        return entry;
    }
    if (tuple.runtimeID.empty() || tuple.authorityRecordKey.empty()
        || tuple.authorityModRevision <= 0) {
        entry.invalid = true;
        entry.detail = "registration tuple lacks complete runtime/authority fields";
        return entry;
    }
    if (tuple.status != "PENDING" && tuple.status != "UNKNOWN"
        && tuple.status != "COMMITTED" && tuple.status != "CLEANED") {
        entry.invalid = true;
        entry.detail = "registration status outside the allowed set";
        return entry;
    }
    // Canonical-form round-trip: the raw value must be EXACTLY what
    // SerializeRegistration produces for the parsed tuple — a damaged or
    // non-canonical value is invalid, and invalid entries never widen any
    // deletion.
    if (SerializeRegistration(tuple) != rawValue) {
        entry.invalid = true;
        entry.detail = "registration value is not in canonical serialized form";
        return entry;
    }
    const auto snap = StateClient::ParseSnapshotKey(tuple.snapshotKey);
    if (snap.saveOpID.empty() || snap.instanceId != instanceId
        || snap.creationRequest != creationRequest
        || snap.saveOpID != keyParts.saveOpID) {
        entry.invalid = true;
        entry.detail = "snapshot key does not strictly match the registration";
        return entry;
    }
    entry.snapshotKey = tuple.snapshotKey;
    entry.wasCommitted = (tuple.status == "COMMITTED" || tuple.status == "CLEANED");
    entry.cleanedMarker = (tuple.status == "CLEANED");
    return entry;
}

litebus::Future<Status> StateActor::DeleteState(const std::string &instanceId, const std::string &generation,
                                                 const std::string &expectedRuntimeID)
{
    if (generation.empty() || expectedRuntimeID.empty()) {
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID,
                      "delete state failed: incomplete identity");
    }
    if (!deleteExecutor_) {
        return Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "delete state failed: delete executor not bound");
    }
    // v3 retirement: the delete executor ALREADY verifies the retiring
    // chain's authority (view-level runtime match). The legacy key delete
    // proceeds; snapshot keys for this generation are enumerable from the
    // registration prefix and cleaned by reconciliation.
    const std::string legacyKey = StateClient::ComposeStateKey(instanceId, generation);
    return deleteExecutor_(instanceId, expectedRuntimeID, generation,
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

void StateActor::BindSaveRegistry(const std::shared_ptr<MetaStoreClient> &metaStoreClient,
                                  const FunctionKeyResolver &functionKeyResolver)
{
    registry_ = metaStoreClient;
    functionKeyResolver_ = functionKeyResolver;
    // Startup hook for the persistent retirement replay: this actor — even
    // brand-new, with no volatile tables and no external kill trigger —
    // scans the tombstone prefix and re-drives cleanups. settleCleanup
    // tables elsewhere are an OPTIMIZATION; the store is the recovery
    // truth. Exactly ONE timer chain per actor: repeated binds must not
    // spawn parallel periodic chains.
    if (!replayTimerStarted_) {
        replayTimerStarted_ = true;
        litebus::AsyncAfter(replayInitialDelayMs.load(), GetAID(),
                            &StateActor::RunRetirementReplayRound);
    }
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
    // The SAME unambiguous segment escaping as every other key in the
    // state namespace — a ':'/'@'/'%' inside an ID can never forge a
    // segment boundary.
    return "statereg:v1:" + StateClient::EscapeKeyPart(instanceID) + ":"
        + StateClient::EscapeKeyPart(creationRequest) + ":"
        + StateClient::EscapeKeyPart(saveOpID);
}

std::string StateActor::StateRefKey(const std::string &instanceID,
                                    const std::string &creationRequest)
{
    return "stateref:v1:" + StateClient::EscapeKeyPart(instanceID) + ":"
        + StateClient::EscapeKeyPart(creationRequest);
}

std::string StateActor::RetirementKey(const std::string &instanceID,
                                      const std::string &creationRequest)
{
    return "stateret:v1:" + StateClient::EscapeKeyPart(instanceID) + ":"
        + StateClient::EscapeKeyPart(creationRequest);
}

std::string StateActor::RegistrationPrefix(const std::string &instanceID,
                                           const std::string &creationRequest)
{
    // Trailing ':' bounds the range to EXACTLY this generation: escaped
    // segments can never contain a ':', so no other instance/generation
    // key can carry this prefix.
    return "statereg:v1:" + StateClient::EscapeKeyPart(instanceID) + ":"
        + StateClient::EscapeKeyPart(creationRequest) + ":";
}

StateActor::RegistrationKeyParts StateActor::ParseRegistrationKey(const std::string &key)
{
    RegistrationKeyParts out;
    static const std::string scheme = "statereg:v1:";
    if (key.rfind(scheme, 0) != 0) { return out; }
    const std::string body = key.substr(scheme.size());
    const size_t sep1 = body.find(':');
    if (sep1 == std::string::npos) { return out; }
    const size_t sep2 = body.find(':', sep1 + 1);
    if (sep2 == std::string::npos) { return out; }
    if (body.find(':', sep2 + 1) != std::string::npos) { return out; }
    const std::string rawInstance = body.substr(0, sep1);
    const std::string rawRequest = body.substr(sep1 + 1, sep2 - sep1 - 1);
    const std::string rawOp = body.substr(sep2 + 1);
    // Canonical-escape round-trip: the segment must be EXACTLY what
    // EscapeKeyPart produces for its decoded value (no raw separators, no
    // non-canonical escapes, non-empty).
    if (rawInstance.empty() || rawRequest.empty() || rawOp.empty()) { return out; }
    if (StateClient::EscapeKeyPart(StateClient::UnescapeKeyPart(rawInstance)) != rawInstance
        || StateClient::EscapeKeyPart(StateClient::UnescapeKeyPart(rawRequest)) != rawRequest
        || StateClient::EscapeKeyPart(StateClient::UnescapeKeyPart(rawOp)) != rawOp) {
        return out;
    }
    out.instanceId = StateClient::UnescapeKeyPart(rawInstance);
    out.creationRequest = StateClient::UnescapeKeyPart(rawRequest);
    out.saveOpID = StateClient::UnescapeKeyPart(rawOp);
    return out;
}

StateActor::RegistrationKeyParts StateActor::ParseRetirementKey(const std::string &key)
{
    RegistrationKeyParts out;  // saveOpID stays empty (two-segment key)
    static const std::string scheme = "stateret:v1:";
    if (key.rfind(scheme, 0) != 0) { return out; }
    const std::string body = key.substr(scheme.size());
    const size_t sep = body.find(':');
    if (sep == std::string::npos) { return out; }
    if (body.find(':', sep + 1) != std::string::npos) { return out; }
    const std::string rawInstance = body.substr(0, sep);
    const std::string rawRequest = body.substr(sep + 1);
    if (rawInstance.empty() || rawRequest.empty()) { return out; }
    if (StateClient::EscapeKeyPart(StateClient::UnescapeKeyPart(rawInstance)) != rawInstance
        || StateClient::EscapeKeyPart(StateClient::UnescapeKeyPart(rawRequest)) != rawRequest) {
        return out;
    }
    out.instanceId = StateClient::UnescapeKeyPart(rawInstance);
    out.creationRequest = StateClient::UnescapeKeyPart(rawRequest);
    return out;
}

// ── startup/periodic retirement replay ──────────────────────────────────────
// Bounded rounds, actor-thread continuations, failure-retaining: a failed
// or partial round changes nothing in the store — the next round re-reads
// the same prefix. The replay only ever drives CleanupRetiredSnapshots
// (tombstone-gated, this-generation-only keys, no physical budget) for
// STRICTLY verified tombstones.
void StateActor::RunRetirementReplayRound()
{
    const litebus::AID self = GetAID();
    const uint32_t period = replayPeriodMs.load();
    if (registry_ == nullptr || stateClient_ == nullptr) {
        // Not fully bound yet — keep the cadence.
        litebus::AsyncAfter(period, self, &StateActor::RunRetirementReplayRound);
        return;
    }
    GetOption scanOpt;
    scanOpt.prefix = true;
    // FULL prefix scan (no fake pagination — the option model has no
    // range end); the bounded selection happens client-side below. The
    // next round is scheduled from OnComplete, which fires on success AND
    // on failed futures — a failed scan never stops the replay, and it is
    // scheduled EXACTLY once per round (DispatchRetirementReplay does not
    // schedule anything).
    registry_->Get(kRetirementKeyPrefix, scanOpt)
        .Then([self, tablePrefix = registry_->GetTablePrefix(), cursor = replayCursor_]
              (const std::shared_ptr<GetResponse> &rsp)
                  -> std::pair<std::vector<ReplayTarget>, std::string> {
            std::pair<std::vector<ReplayTarget>, std::string> out;
            if (rsp == nullptr || rsp->status.IsError()) {
                // Read failure: retain everything, keep the cursor; the
                // OnComplete-scheduled next round retries.
                out.second = cursor;
                return out;
            }
            // Strictly validate and SORT by fence key (etcd order).
            std::map<std::string, ReplayTarget> valid;
            for (const auto &kv : rsp->kvs) {
                const std::string key = TrimKeyPrefix(kv.key(), tablePrefix);
                const auto keyParts = ParseRetirementKey(key);
                FenceRecord fence;
                if (keyParts.instanceId.empty() || keyParts.creationRequest.empty()
                    || !ParseStrictFence(kv.value(), fence)) {
                    YRLOG_WARN("retirement replay: tombstone key/value failed strict "
                               "validation ({}), retained for operator inspection", key);
                    continue;
                }
                if (fence.instanceID != keyParts.instanceId
                    || fence.creationRequest != keyParts.creationRequest) {
                    YRLOG_WARN("retirement replay: tombstone key/value identity mismatch "
                               "({}), retained", key);
                    continue;
                }
                ReplayTarget target;
                target.instanceId = fence.instanceID;
                target.creationRequest = fence.creationRequest;
                target.runtimeID = fence.runtimeID;
                valid.emplace(key, std::move(target));
            }
            // Cursor-bounded fair selection: everything AFTER the cursor
            // first, then wrap to the head; never re-dispatch within one
            // round. The cursor advances past every selected key — a
            // permanently-uncertain head cannot starve the tail.
            auto begin = valid.upper_bound(cursor);
            for (auto it = begin; it != valid.end() && out.first.size() < kReplayBatchSize;
                 ++it) {
                out.first.push_back(it->second);
                out.second = it->first;
            }
            if (out.first.size() < kReplayBatchSize) {
                for (auto it = valid.begin();
                     it != valid.end() && out.first.size() < kReplayBatchSize; ++it) {
                    if (it->first <= cursor) {  // wrap-around tail
                        out.first.push_back(it->second);
                        out.second = it->first;
                    }
                }
            }
            return out;
        })
        .Then([self](const std::pair<std::vector<ReplayTarget>, std::string> &selection)
                  -> litebus::Future<bool> {
            // Cursor + dispatch touch actor state — hop back onto the actor.
            return litebus::Async(self, &StateActor::DispatchRetirementReplay,
                                  selection.first, selection.second);
        })
        .OnComplete([self, period](const litebus::Future<bool> &) {
            // Fires on success AND failure — the cadence never dies.
            litebus::AsyncAfter(period, self, &StateActor::RunRetirementReplayRound);
        });
}

bool StateActor::DispatchRetirementReplay(std::vector<ReplayTarget> targets,
                                          const std::string &nextCursor)
{
    // HARD global concurrency cap: at most kReplayConcurrencyCap cleanups
    // in flight across ALL rounds — long-stuck generations cannot
    // accumulate unbounded concurrent work. The cursor advances only past
    // what was ACTUALLY dispatched; un-dispatched selections stay after
    // the cursor for a later round (and wrap-around still reaches them).
    std::string lastDispatchedKey;
    for (const auto &target : targets) {
        if (replayInFlight_.size() >= static_cast<size_t>(kReplayConcurrencyCap)) {
            break;
        }
        const std::string fenceKey = RetirementKey(target.instanceId, target.creationRequest);
        if (replayInFlight_.count(fenceKey) > 0) { continue; }  // already running
        (void)replayInFlight_.insert(fenceKey);
        lastDispatchedKey = fenceKey;
        CleanupRetiredSnapshots(target.instanceId, target.creationRequest, target.runtimeID)
            .OnComplete(litebus::Defer(GetAID(), &StateActor::OnRetirementReplayCleanupComplete,
                                       target.instanceId, target.creationRequest,
                                       target.runtimeID, std::placeholders::_1));
    }
    if (!lastDispatchedKey.empty()) {
        replayCursor_ = lastDispatchedKey;
    } else if (!targets.empty()) {
        // Nothing dispatched (cap saturated by earlier rounds): hold the
        // cursor so the same range is re-selected once slots free up.
        (void)nextCursor;
    } else {
        replayCursor_ = nextCursor;  // empty selection: adopt the computed cursor
    }
    return !lastDispatchedKey.empty();
}

void StateActor::OnRetirementReplayCleanupComplete(
    const std::string &instanceId, const std::string &creationRequest,
    const std::string &runtimeID, const litebus::Future<RetireReport> &cleanup)
{
    (void)replayInFlight_.erase(RetirementKey(instanceId, creationRequest));
    if (cleanup.IsError()) {
        YRLOG_WARN("retirement replay for instance({}) request({}) runtime({}): cleanup "
                   "future FAILED — retained for the next round",
                   instanceId, creationRequest, runtimeID);
        return;
    }
    const auto &report = cleanup.Get();
    if (!report.fenced) {
        YRLOG_WARN("retirement replay for instance({}) request({}): refused ({}) — retained",
                   instanceId, creationRequest, report.reason);
        return;
    }
    YRLOG_INFO("retirement replay for instance({}) request({}): entries({}) allCleaned({}) "
               "refDeleted({}) refAbsent({}) refUnknown({})",
               instanceId, creationRequest, report.entries.size(), report.allCleaned,
               report.refDeleted, report.refAbsent, report.refUnknown);
}

std::string StateActor::SerializeFence(const FenceRecord &fence)
{
    auto esc = [](const std::string &s) {
        std::string out;
        for (char c : s) {
            if (c == '|' || c == '\\') { out.push_back('\\'); }
            out.push_back(c);
        }
        return out;
    };
    return esc(fence.instanceID) + "|" + esc(fence.creationRequest) + "|" + esc(fence.runtimeID)
        + "|" + esc(fence.authorityRecordKey) + "|" + std::to_string(fence.authorityModRevision)
        + "|" + esc(fence.status) + "|" + esc(fence.cleanupCursor);
}

StateActor::FenceRecord StateActor::ParseFence(const std::string &raw)
{
    FenceRecord fence;
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
    if (fields.size() != 7) { return fence; }
    errno = 0;
    char *end = nullptr;
    const long long parsed = std::strtoll(fields[4].c_str(), &end, 10);
    if (errno != 0 || end == nullptr || *end != '\0' || parsed < 0) { return fence; }
    fence.instanceID = fields[0];
    fence.creationRequest = fields[1];
    fence.runtimeID = fields[2];
    fence.authorityRecordKey = fields[3];
    fence.authorityModRevision = static_cast<int64_t>(parsed);
    fence.status = fields[5];
    fence.cleanupCursor = fields[6];
    return fence;
}

// STRICT tombstone acceptance (root defect C): a valid proof is exactly
// RETIRED, with every identity and authority field non-empty, a positive
// authority mod_revision, and the raw value in canonical serialized form
// (round-trip). Callers additionally verify the identity fields against
// their own bound identity.
bool StateActor::ParseStrictFence(const std::string &raw, FenceRecord &out)
{
    const auto fence = ParseFence(raw);
    if (fence.status != "RETIRED") { return false; }
    if (fence.instanceID.empty() || fence.creationRequest.empty() || fence.runtimeID.empty()) {
        return false;
    }
    if (fence.authorityRecordKey.empty() || fence.authorityModRevision <= 0) { return false; }
    if (SerializeFence(fence) != raw) { return false; }  // canonical form only
    out = fence;
    return true;
}

// Anti-late-publish guard shared by both conditional transactions: the
// generation's checkpoint reference must still be EXACTLY what Phase 0
// captured. An explicitly-absent reference conditions on version==0 (the
// etcd "key does not exist" comparison) — a reference published between
// capture and this transaction fails the whole txn. A captured reference
// conditions on its raw value AND its mod_revision.
void StateActor::AddRefGuardConditions(const std::shared_ptr<meta_store::TxnTransaction> &txn,
                                       const std::string &refKey,
                                       const AuthoritySnapshot::StateRefCapture &capture)
{
    if (!capture.exists) {
        txn->If(meta_store::TxnCompare::OfVersion(refKey, meta_store::CompareOperator::EQUAL, 0));
        return;
    }
    txn->If(meta_store::TxnCompare::OfValue(
        refKey, meta_store::CompareOperator::EQUAL, capture.value));
    txn->If(meta_store::TxnCompare::OfModifyVersion(
        refKey, meta_store::CompareOperator::EQUAL, capture.modRevision));
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
        + "|" + esc(reg.creationRequest) + "|" + esc(reg.runtimeID) + "|" + esc(reg.status)
        + "|" + esc(reg.authorityRecordKey) + "|" + std::to_string(reg.authorityModRevision);
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
    if (fields.size() != 8) { return reg; }
    errno = 0;
    char *end = nullptr;
    const long long parsed = std::strtoll(fields[7].c_str(), &end, 10);
    if (errno != 0 || end == nullptr || *end != '\0' || parsed < 0) { return reg; }
    reg.saveOpID = fields[0];
    reg.snapshotKey = fields[1];
    reg.instanceID = fields[2];
    reg.creationRequest = fields[3];
    reg.runtimeID = fields[4];
    reg.status = fields[5];
    reg.authorityRecordKey = fields[6];
    reg.authorityModRevision = static_cast<int64_t>(parsed);
    return reg;
}

// Registration demotion PENDING → UNKNOWN, conditional on the registration
// still holding capturedValue. Actor-thread only (uses registry_).
void StateActor::MarkRegistrationUnknown(const std::string &regKey,
                                         const std::string &capturedValue)
{
    if (registry_ == nullptr) { return; }
    auto parsed = ParseRegistration(capturedValue);
    if (parsed.status.empty()) { return; }
    parsed.status = "UNKNOWN";
    auto txn = registry_->BeginTransaction();
    txn->If(meta_store::TxnCompare::OfValue(regKey, meta_store::CompareOperator::EQUAL,
                                             capturedValue));
    txn->Then(meta_store::TxnOperation::Create(regKey, SerializeRegistration(parsed),
                                                PutOption{}));
    (void)txn->Commit();
}

}  // namespace functionsystem::function_proxy
