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
#include <unordered_set>
#include <vector>

#include "actor/actor.hpp"
#include "async/future.hpp"
#include "common/logs/logging.h"
#include "common/metadata/metadata.h"
#include "common/proto/pb/posix_pb.h"
#include "meta_store_client/meta_store_client.h"
#include "state_client.h"

namespace functionsystem::function_proxy {

class StateActor : public litebus::ActorBase {
public:
    explicit StateActor(const std::shared_ptr<StateClient> &stateClient);
    ~StateActor() override = default;

    litebus::Future<StateSaveResponse> SaveState(const std::string &instanceId,
                                                 const std::string &boundRuntimeID,
                                                 const std::string &boundRequestID,
                                                 const std::shared_ptr<StateSaveRequest> &request);

    litebus::Future<StateLoadResponse> LoadState(const std::shared_ptr<StateLoadRequest> &request);

    // Resolution of the PERSISTENT checkpoint reference for Recover. The
    // generation's original identity (instance + creation request) is the
    // ONLY input — a successor runtime's ID never substitutes for it.
    struct CheckpointRef {
        bool ok = false;
        bool legacy = false;        // explicitly-absent reference → migration key
        std::string checkpointRef;  // published snapshot key (or legacy key)
        std::string reason;         // refusal reason when !ok
    };

    // ASYNC resolution of the generation's published checkpoint reference
    // for the Recover path: reads stateref, STRICTLY parses the snapshot
    // key, and verifies it names THIS instance and creation generation.
    // Read errors, timeouts, corrupt values, or identity mismatches
    // REFUSE (ok=false) — never degrade to a composed key. ONLY explicit
    // absence (kvs empty, no error) returns the legacy composed key for
    // migration coexistence.
    litebus::Future<CheckpointRef> ResolveCheckpointRef(const std::string &instanceId,
                                                        const std::string &creationRequest);

    void InitStateClient();

    using GuardedStoreOp = std::function<Status()>;
    using SaveExecutor = std::function<Status(const std::string &instanceID,
                                              const std::string &expectedRuntimeID,
                                              const std::string &expectedRequestID,
                                              const GuardedStoreOp &op)>;
    using DeleteExecutor = std::function<Status(const std::string &instanceID,
                                                const std::string &expectedRuntimeID,
                                                const std::string &expectedRequestID,
                                                const GuardedStoreOp &op)>;
    void SetSaveExecutor(SaveExecutor executor);
    void SetDeleteExecutor(DeleteExecutor executor);

    litebus::Future<Status> DeleteState(const std::string &instanceId,
                                        const std::string &generation,
                                        const std::string &expectedRuntimeID);

    // ── v3 registration-first isolation ──────────────────────────────────
    struct SaveRegistration {
        std::string saveOpID;
        std::string snapshotKey;
        std::string instanceID;
        std::string creationRequest;
        std::string runtimeID;
        std::string status;  // PENDING | COMMITTED | UNKNOWN
        // The OWNERSHIP this operation was validated against: the
        // authoritative instance-record key and its mod_revision at
        // verification time, persisted WITH the registration so every
        // registration names the authority that granted it.
        std::string authorityRecordKey;
        int64_t authorityModRevision = 0;
    };
    // Binds the meta-store client (for the authoritative instance record)
    // AND a function-key resolver. The resolver receives the FULL bound
    // identity (instance, runtime, creation request) and must return the
    // function key ONLY when the live control-view machine still matches
    // that identity — a newer incarnation's function must never authorize
    // an old stream's save, and vice versa. Without both bindings, v3
    // saves refuse. Binding also starts the persistent retirement replay
    // (cadence below).
    using FunctionKeyResolver = std::function<std::string(const std::string &instanceID,
                                                          const std::string &runtimeID,
                                                          const std::string &requestID)>;
    void BindSaveRegistry(const std::shared_ptr<MetaStoreClient> &metaStoreClient,
                          const FunctionKeyResolver &functionKeyResolver);
    // Replay cadence — static so tests can shorten the waits; production
    // defaults are set in the .cpp and never changed there.
    static std::atomic<uint32_t> replayInitialDelayMs;
    static std::atomic<uint32_t> replayPeriodMs;
    static std::string SerializeRegistration(const SaveRegistration &reg);
    static SaveRegistration ParseRegistration(const std::string &raw);
    static std::string RegistrationKey(const std::string &instanceID,
                                       const std::string &creationRequest,
                                       const std::string &saveOpID);
    // Persistent latest-checkpoint reference: the publish transaction
    // writes it; Recover reads THIS key (never composes a shared key).
    static std::string StateRefKey(const std::string &instanceID,
                                   const std::string &creationRequest);

    // ── v3 persistent retirement fence ────────────────────────────────────
    // The generation-scoped tombstone. Its value names the identity that
    // retired AND the authoritative authority (record key + mod_revision)
    // the fence was conditioned on — the persistent token carried across
    // first attempt, retries, and restarts: once the authoritative record
    // is deleted (DelInstance), the tombstone is the ONLY admissible
    // proof of identity for replays. Never self-derived, never
    // unconditional.
    struct FenceRecord {
        std::string instanceID;
        std::string creationRequest;
        std::string runtimeID;
        std::string authorityRecordKey;
        int64_t authorityModRevision = 0;
        std::string status;  // "RETIRED"
        // Per-generation cleanup cursor: the last registration key whose
        // entry this generation's cleanup already processed. Persisted IN
        // the tombstone so the entry cap cannot starve the tail of a huge
        // generation across rounds (or restarts): each round resumes
        // AFTER the cursor and wraps; CLEANED/invalid entries pass free
        // (no DS quota); a full wrap resets it.
        std::string cleanupCursor;
    };
    static std::string SerializeFence(const FenceRecord &fence);
    static FenceRecord ParseFence(const std::string &raw);
    // STRICT tombstone acceptance: exactly RETIRED, every identity and
    // authority field non-empty, positive authority mod_revision, and the
    // raw value in canonical serialized form (round-trip). Callers still
    // verify the identity fields against their bound identity.
    static bool ParseStrictFence(const std::string &raw, FenceRecord &out);
    // "stateret:v1:<esc(instance)>:<esc(creationRequest)>" — the fence key
    // AND the condition every save-side transaction carries as absent
    // (version==0): once committed, every late registration/publish of
    // this generation fails.
    static std::string RetirementKey(const std::string &instanceID,
                                     const std::string &creationRequest);
    // Exact-generation registration enumeration prefix (trailing ':' —
    // escaped segments contain no ':', so no other generation's keys can
    // fall inside the range).
    static std::string RegistrationPrefix(const std::string &instanceID,
                                          const std::string &creationRequest);
    struct RegistrationKeyParts {
        std::string instanceId;
        std::string creationRequest;
        std::string saveOpID;  // empty ⇒ invalid
    };
    // STRICT inverse of RegistrationKey: exact scheme, exactly three
    // canonically-escaped non-empty segments (escape round-trip).
    static RegistrationKeyParts ParseRegistrationKey(const std::string &key);
    // STRICT inverse of RetirementKey: exact scheme, exactly TWO
    // canonically-escaped non-empty segments. saveOpID is always empty —
    // validity is judged by non-empty instanceId/creationRequest.
    static RegistrationKeyParts ParseRetirementKey(const std::string &key);

    // Per-registration snapshot cleanup outcome. `deleted` requires a
    // COMMITTED registration (its Phase-2 DS write provably landed) plus a
    // successful exact Del — and is then PERSISTED by promoting the
    // registration to CLEANED, so a replay after restart converges on the
    // marker instead of re-deriving proof from a NOT_FOUND-class result.
    // A registration already marked CLEANED proves `deleted` on its own.
    // PENDING/UNKNOWN registrations get at best `absent` — a
    // NOT_FOUND-class result PROVES NOTHING (the save's DS write may still
    // be in flight and land later) and is NEVER promoted to cleaned; those
    // registrations are RETAINED for replay. `uncertain` =
    // transport/timeout, replay.
    struct RegistrationCleanup {
        std::string regKey;
        std::string snapshotKey;
        std::string capturedValue;   // raw registration value (marker rewrite condition)
        bool wasCommitted = false;
        bool cleanedMarker = false;  // registration persistently CLEANED (pre-existing or
                                     // recorded by THIS cleanup's awaited marker txn)
        bool markerPending = false;  // DS data deleted but the CLEANED proof write failed —
                                     // replay re-derives; never reported as closed
        bool deleted = false;
        bool absent = false;
        bool uncertain = false;
        bool invalid = false;  // failed strict validation — never deleted
        std::string detail;
    };
    struct RetireReport {
        bool fenced = false;       // tombstone committed & identity proven
        bool fenceFresh = false;   // committed by THIS call (vs verified existing)
        bool enumerated = false;   // registration prefix enumerated
        bool truncated = false;    // entry cap hit — the rest stay registered
                                   // and reconverge on later replay rounds
        bool refDeleted = false;   // conditional stateref delete committed
        bool refAbsent = false;    // reference explicitly absent at capture
        bool refUnknown = false;   // reference state unresolved — replay re-reads
        std::string refDetail;
        bool allCleaned = false;   // every entry PROVEN deleted (persistent
                                   // CLEANED proof recorded), reference
                                   // proven handled, nothing pending
        std::vector<RegistrationCleanup> entries;
        std::string reason;        // refusal reason when !fenced
        // Internal plumbing (also observable by tests): the per-generation
        // cursor advance persisted into the tombstone at the report's end.
        std::string tombstoneKey;
        std::string tombstoneValue;
        std::string newCursor;
        bool cursorAdvanced = false;
    };
    // FENCE (only): commits the generation tombstone conditionally on the
    // ORIGINAL authoritative record (captured raw value + mod_revision,
    // identity verified first) — or, on replay, verifies the EXISTING
    // tombstone against the caller's bound identity. Must run BEFORE
    // DelInstance deletes the authoritative record. No tombstone and no
    // readable record = REFUSED, never an unconditional RETIRED Put.
    litebus::Future<RetireReport> RetireGeneration(const std::string &instanceId,
                                                   const std::string &creationRequest,
                                                   const std::string &runtimeID);
    // CLEANUP (idempotent, replayable): requires the tombstone (identity
    // proof). Enumerates ONLY this generation's statereg prefix in the
    // meta-store (never the DataSystem), strictly validates every entry,
    // exact-Dels each registration's unique snapshot key, and
    // conditionally deletes the generation's stateref (never a blind
    // delete). Tombstone + all registrations are RETAINED as the replay
    // record; storage failures are reported per-entry and retryable —
    // they never masquerade as all-cleaned.
    litebus::Future<RetireReport> CleanupRetiredSnapshots(const std::string &instanceId,
                                                          const std::string &creationRequest,
                                                          const std::string &runtimeID);

    // AUTHORITATIVE OWNERSHIP SNAPSHOT — captured from the REAL etcd kv
    // (raw value + create_revision + mod_revision) by VerifyOwnership.
    // Drives every conditional transaction in the save chain.
    struct AuthoritySnapshot {
        bool verified = false;
        std::string recordKey;
        std::string recordValue;    // raw etcd kv value (InstanceInfo JSON)
        int64_t createRevision = 0;
        int64_t modRevision = 0;
        // The generation's CURRENT persistent checkpoint reference,
        // captured BEFORE registration. Two saves of the SAME generation
        // capture the SAME owner record — the record conditions alone
        // cannot order them. These captured reference conditions are what
        // stop an old LATE publish from dragging the latest reference back
        // to an old snapshot: the publish requires the reference to still
        // be exactly what THIS save saw at start.
        struct StateRefCapture {
            bool exists = false;
            std::string value;          // raw captured stateref value
            int64_t createRevision = 0;
            int64_t modRevision = 0;
        };
        StateRefCapture refCapture;
    };

    // ── actor-phase handlers ─────────────────────────────────────────────
    // Each runs ON this actor: future continuations that need actor state
    // (registry_/saveExecutor_/stateClient_) hop back via litebus::Async
    // and never touch it on the future-completion thread. Only pure value
    // parsing runs directly in completion callbacks.
    // Phase 1: persist the PENDING registration conditionally on the
    // captured authoritative record (raw value AND mod_revision).
    litebus::Future<StateSaveResponse> OnOwnershipVerified(
        const std::string &instanceId, const std::string &boundRuntimeID,
        const std::string &boundRequestID, const std::string &snapshotKey,
        const std::string &saveOpID, const std::shared_ptr<StateSaveRequest> &request,
        const AuthoritySnapshot &authority);
    // Phase 2 (guarded DS write) + Phase 3 (conditional publish: captured
    // record value + mod_revision + THIS operation's captured PENDING
    // registration; COMMITTED and the persistent stateref move atomically).
    litebus::Future<StateSaveResponse> OnRegistrationCommitted(
        const std::string &instanceId, const std::string &boundRuntimeID,
        const std::string &boundRequestID, const std::string &snapshotKey,
        const std::string &saveOpID, const std::string &regKey,
        const std::string &pendingValue, const AuthoritySnapshot &authority,
        const std::shared_ptr<StateSaveRequest> &request);
    // Publish terminal: COMMITTED response, or conditional UNKNOWN demotion
    // (never downgrades an actually-committed registration).
    StateSaveResponse OnPublishFinished(const std::string &instanceId,
                                        const std::string &saveOpID,
                                        const std::string &snapshotKey,
                                        const std::string &regKey,
                                        const std::string &pendingValue, bool committed);

private:
    // Phase 0: async-reads the authoritative instance record through the
    // REAL MetaStoreClient::Get (no InstanceOperator wrapper — that path
    // drops create_revision and handles null/count-kvs inconsistently),
    // verifies the persisted standard InstanceInfo via
    // TransToInstanceInfoFromJson, and captures the kv's raw value +
    // create_revision + mod_revision; ALSO reads the generation's current
    // checkpoint reference and captures it for the anti-late-publish
    // conditions. Both reads are ISSUED on the actor thread (parallel);
    // every Then callback is PURE VALUE processing — it touches no actor
    // state. Returns a ready verified=false snapshot on ANY read error,
    // absent/ambiguous record, non-positive mod_revision, parse failure,
    // identity mismatch, or a corrupt/unparsable existing reference — the
    // caller refuses, never self-appoints.
    litebus::Future<AuthoritySnapshot> VerifyOwnership(
        const std::string &instanceId,
        const std::string &boundRuntimeID,
        const std::string &boundRequestID);

    // Anti-late-publish guard shared by the registration and publish
    // transactions: the generation's checkpoint reference must still be
    // EXACTLY what Phase 0 captured — an explicitly-absent reference
    // conditions on version==0 (key does not exist); a captured reference
    // conditions on its raw value AND mod_revision. A reference that
    // moved in between fails the whole transaction; the operation NEVER
    // re-reads the new version to retry an old publish.
    static void AddRefGuardConditions(const std::shared_ptr<meta_store::TxnTransaction> &txn,
                                      const std::string &refKey,
                                      const AuthoritySnapshot::StateRefCapture &capture);

    // ── retirement internals ─────────────────────────────────────────────
    // First-attempt path after the tombstone read came back absent: derive
    // authority from the LIVE record (resolver + GenInstanceKey + raw Get +
    // identity verify), then commit the fence conditionally on it.
    litebus::Future<RetireReport> DeriveRetirementAuthority(const std::string &instanceId,
                                                            const std::string &creationRequest,
                                                            const std::string &runtimeID);
    // Actor phase: the conditional fence transaction (record value +
    // mod_revision + tombstone-not-exist). On failure the chain hops to
    // OnRetireFenceCommitFailed (a concurrent replier may have fenced).
    litebus::Future<RetireReport> CommitRetirementFence(const std::string &instanceId,
                                                        const std::string &creationRequest,
                                                        const std::string &runtimeID,
                                                        const std::string &recordKey,
                                                        const std::string &recordValue,
                                                        int64_t recordModRevision);
    // Actor phase after a failed fence commit: re-read the tombstone and
    // verify identity — EXISTING on match, refused otherwise.
    litebus::Future<RetireReport> OnRetireFenceCommitFailed(const std::string &instanceId,
                                                            const std::string &creationRequest,
                                                            const std::string &runtimeID);
    // Validated cleanup plan assembled by the PURE enumeration callbacks;
    // executed on the actor.
    struct RetirementPlan {
        bool refExists = false;
        std::string refValue;
        int64_t refModRevision = 0;
        // Reference state UNRESOLVED (read error, ambiguous, corrupt, or
        // foreign value): NO conditional delete is attempted, and
        // allCleaned stays false — with the reason carried here. An
        // unresolved reference is NEVER reported as explicitly absent.
        bool refUnknown = false;
        std::string refDetail;
        bool truncated = false;  // DS-work quota hit this round (explicit)
        // Per-generation cursor persistence: the tombstone's raw captured
        // value conditions the advance, so the entry quota can never
        // starve the tail across rounds or restarts.
        std::string tombstoneKey;
        std::string tombstoneValue;
        std::string newCursor;
        bool cursorAdvanced = false;
        std::vector<RegistrationCleanup> entries;
    };
    // Actor phase, POST-fence: issues the registration-prefix enumeration
    // and the reference read (both AFTER the tombstone is confirmed — the
    // fence-absent condition in save-side transactions means nothing of
    // this generation can move anymore, so a linearizable read here sees
    // every registration that will ever exist). fenceValue is the RAW
    // captured tombstone — it carries the per-generation cleanup cursor
    // and conditions the cursor's persisted advance.
    litebus::Future<RetireReport> EnumerateRetiredGeneration(const std::string &instanceId,
                                                             const std::string &creationRequest,
                                                             const std::string &runtimeID,
                                                             const std::string &fenceValue);
    // Actor phase: conditional stateref delete, then hop to the report.
    litebus::Future<RetireReport> FinishRetirementCleanup(const std::string &instanceId,
                                                          const std::string &creationRequest,
                                                          const RetirementPlan &plan);
    // Actor phase: exact DS Del per validated entry. Marker promotions are
    // NOT touched here — the chain continues into PromoteNextCleanedMarker.
    RetireReport DelRetiredSnapshots(const std::string &instanceId,
                                     const std::string &creationRequest,
                                     const RetirementPlan &plan, bool refDeleted,
                                     bool refAbsent);
    // Actor phase: Del + chain into the awaited marker promotions. The
    // reference's terminal state (deleted / explicitly absent / failed /
    // unknown) is passed EXPLICITLY — never inferred from strings.
    litebus::Future<RetireReport> DelThenPromote(const std::string &instanceId,
                                                 const std::string &creationRequest,
                                                 const RetirementPlan &plan, bool refDeleted,
                                                 bool refAbsent);
    // Actor phase: walks the report entries that deleted COMMITTED data
    // and, ONE AT A TIME, commits the conditional COMMITTED→CLEANED marker
    // transaction and AWAITS its verdict — the persistent proof is only
    // recorded on a confirmed success (null response, status failure,
    // success=false, or a failed future all leave the entry
    // markerPending: replay re-derives, never reported as closed). The
    // terminal step computes allCleaned from the FINAL per-entry states.
    litebus::Future<RetireReport> PromoteNextCleanedMarker(const std::string &instanceId,
                                                           const std::string &creationRequest,
                                                           RetireReport report, size_t index);
    // Pure validation of one enumerated registration kv. Everything must
    // line up — key parses strictly AND names this generation, value is a
    // complete tuple for this generation, and the tuple's snapshotKey is a
    // strict snapshot key naming the same instance/generation/saveOpID as
    // the key. Anything less marks the entry invalid (never deleted).
    static RegistrationCleanup ClassifyRegistrationEntry(const std::string &regKey,
                                                         const std::string &rawValue,
                                                         const std::string &instanceId,
                                                         const std::string &creationRequest);

    // Registration demotion PENDING → UNKNOWN, conditional on the
    // registration still holding capturedValue (a COMMITTED registration
    // is never downgraded). Actor-thread only.
    void MarkRegistrationUnknown(const std::string &regKey,
                                 const std::string &capturedValue);

    // ── startup/periodic retirement replay ───────────────────────────────
    // A bounded, actor-thread, failure-retaining driver over the
    // persistent stateret prefix: every strictly-verified tombstone gets
    // its CleanupRetiredSnapshots re-driven, so a NEW actor — owner
    // already deleted, no volatile tables, no external kill trigger —
    // still converges its generation's cleanup. Never touches physical
    // budget or any other generation's state; failures simply stay in
    // the store for the next round.
    //
    // FAIRNESS: tombstones are permanent, so a plain prefix+limit read
    // would return the SAME first keys forever. The scan reads the FULL
    // prefix (no fake pagination — the GetOption model has no range end),
    // and the round selects its bounded batch CLIENT-SIDE from a
    // persistent CURSOR that advances past whatever was dispatched,
    // wrapping around: a permanently-uncertain head cannot starve the
    // tail. The next round is scheduled from the scan chain's OnComplete
    // — which fires on success AND on failed futures — so a failed scan
    // never stops the replay.
    struct ReplayTarget {
        std::string instanceId;
        std::string creationRequest;
        std::string runtimeID;
    };
    void RunRetirementReplayRound();
    // Actor phase: records the advanced cursor and dispatches the bounded
    // batch (in-flight marked; already-covered entries skipped WITHOUT
    // stalling the cursor).
    bool DispatchRetirementReplay(std::vector<ReplayTarget> targets,
                                  const std::string &nextCursor);
    // Actor phase: clears the in-flight mark and logs the replay report
    // (failures retained — nothing is erased from the store).
    void OnRetirementReplayCleanupComplete(const std::string &instanceId,
                                           const std::string &creationRequest,
                                           const std::string &runtimeID,
                                           const litebus::Future<RetireReport> &cleanup);

    std::shared_ptr<StateClient> stateClient_;
    SaveExecutor saveExecutor_;
    DeleteExecutor deleteExecutor_;
    std::shared_ptr<MetaStoreClient> registry_;
    FunctionKeyResolver functionKeyResolver_;
    std::unordered_set<std::string> replayInFlight_;  // fence keys being cleaned this round
    std::string replayCursor_;                        // last fence key whose round was dispatched
    bool replayTimerStarted_ = false;                 // exactly ONE periodic chain per actor
};

}  // namespace functionsystem::function_proxy

#endif  // BUSPROXY_BUSINESS_STATE_ACTOR_STATE_ACTOR_H
