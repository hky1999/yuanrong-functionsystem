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

#ifndef RUNTIME_MANAGER_RETIRE_FAULT_GATE_H
#define RUNTIME_MANAGER_RETIRE_FAULT_GATE_H

#include <cstdint>
#include <mutex>
#include <string>

namespace functionsystem::runtime_manager {

// One-shot, default-off fault gate for retirement-chain physical tests
// (development plan 2026-09-17 stage 4). It never authorizes resource
// changes: it either holds a delete dispatch for a fixed delay before it
// reaches sandboxd, or suppresses exactly one confirmed receipt travelling
// upstream. Every decision (armed, prepared, hit, expired, no-match) is
// logged with the matched identity so the fault evidence does not depend on
// test-side expectations.
//
// Two arming forms:
//  1. EXACT: the spec names action+sandbox+generation+operation_id. The
//     operation id must be knowable in advance — usable when the driving
//     request id is externally generated.
//  2. PREPARE (bounded confirmation stop-point): KillInstance generates a
//     random per-attempt forward id, so a test CANNOT name the operation id
//     before the formal kill. A prepare spec (action+sandbox+generation,
//     "prepare":true, "hold_ms":1000..120000) makes the gate hold the FIRST
//     matching message for at most hold_ms (the stop-point), log PREPARED
//     with the observed operation id, and apply the fault only if the
//     control file confirms an EXACT spec carrying that operation id within
//     the hold. Without confirmation the message passes through unchanged
//     (PREP_EXPIRED) — bounded by construction, no deadlock, and the exact
//     match requirement is never relaxed.
class RetireFaultGate {
public:
    static RetireFaultGate &Instance();

    enum class Decision { NoMatch, MatchExact, MatchPrepare };

    struct BlockDecision {
        Decision decision{ Decision::NoMatch };
        int64_t delayMs{ 0 };   // MatchExact: hold the dispatch this long
        int64_t holdMs{ 0 };    // MatchPrepare: confirmation stop-point window
    };

    // Arm from an explicit JSON spec (identical validation to the environment
    // variable). Used by the singleton's environment read and by unit tests;
    // never touches resource or budget state. Re-arming is rejected while an
    // EXACT spec is active AND unfired; a consumed one-shot accepts a fresh
    // spec so the fault matrix can fire repeatedly in one process.
    void ArmFromSpec(const std::string &specJson);

    // Runtime test control (stage-4 requirement): re-check the control file
    // before each match attempt so a gate can be armed, hit, disarmed and
    // re-armed without restarting the process. The file holds the same JSON
    // spec (arm/confirm) or {"disarm":true}; a missing file leaves current
    // state untouched; it is only read, never written, by the gate. Dedup is
    // CONTENT-based (mtime is not trusted: two changes inside one filesystem
    // timestamp tick must both apply), so re-firing the same fault requires a
    // changed file — bump the spec's "control_revision" field. Default path
    // /run/akernel/retire-fault-control.json, override with
    // AKERNEL_RETIRE_FAULT_CONTROL_FILE. Test facility only.
    void RefreshFromControlFile();

    struct State {
        std::string mode;        // off | exact | prep_wait | prep_held
        std::string action;
        std::string sandboxID;
        std::string generation;
        std::string operationID; // exact spec: required id; prep_held: captured
        int64_t delayMs{ 0 };
        int64_t deadlineUnixMs{ 0 };
        int64_t holdMs{ 0 };
        bool enabled{ false };
        bool consumed{ false };
    };
    State GetStateForTest() const;

    // Delete dispatch: exact match fires immediately (delayMs under the same
    // lock that validated the hit). A prepare-armed gate captures this
    // message's operation id and returns MatchPrepare with the bounded
    // stop-point window — the caller MUST then call ResolveBlock when the
    // hold elapses to learn whether the confirmed fault applies.
    BlockDecision BlockDeleteIfMatched(const std::string &sandboxID, const std::string &generation,
                                       const std::string &operationID);
    // Resolution at the END of the prepare hold: MatchExact applies the
    // confirmed fault (delayMs); NoMatch passes the held message through.
    BlockDecision ResolveBlock(const std::string &sandboxID, const std::string &generation,
                               const std::string &operationID);

    // Receipt send: exact match suppresses this one confirmed receipt. A
    // prepare-armed gate captures the operation id WITHOUT suppressing (the
    // durable receipt replays; only a confirmed exact spec drops a later
    // message carrying the captured id).
    bool DropReceiptIfMatched(const std::string &sandboxID, const std::string &generation,
                              const std::string &operationID);

private:
    bool ConsumeExactLocked(const char *action, const std::string &sandboxID,
                            const std::string &generation, const std::string &operationID,
                            int64_t &delayMs);
    void Disarm(const char *reason);
    void DisarmLocked(const char *reason);  // caller holds mutex_
    void ArmLocked(const std::string &specJson);  // caller holds mutex_
    static std::string ControlFilePath();

    mutable std::mutex mutex_;  // mutable: GetStateForTest() const reads under it
    std::string mode_;          // "", "exact", "prep_wait", "prep_held"
    std::string action_;
    std::string sandboxID_;
    std::string generation_;
    std::string operationID_;   // exact: required; prep_held: captured observation
    int64_t delayMs_{ 0 };
    int64_t deadlineUnixMs_{ 0 };
    int64_t holdMs_{ 0 };
    int64_t prepDeadlineUnixMs_{ 0 };
    bool enabled_{ false };
    bool consumed_{ false };
    std::string lastControlContent_;  // last APPLIED control-file content
};

}  // namespace functionsystem::runtime_manager

#endif
