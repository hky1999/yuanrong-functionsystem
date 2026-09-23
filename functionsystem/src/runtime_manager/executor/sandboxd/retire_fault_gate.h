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
// (development plan 2026-09-17 stage 4). It is armed ONLY through the
// AKERNEL_RETIRE_FAULT environment variable or the runtime control file
// (JSON; absent/unparsable means disabled), matches one exact operation
// identity (sandbox + generation + operation id), fires at most once and
// disarms itself at its deadline. It never authorizes resource changes: it
// either holds a delete dispatch for a fixed delay before it reaches sandboxd,
// or suppresses exactly one confirmed receipt travelling upstream. Every
// decision (armed, hit, expired, no-match) is logged with the matched identity
// so the fault evidence does not depend on test-side expectations.
class RetireFaultGate {
public:
    static RetireFaultGate &Instance();

    // Arm from an explicit JSON spec (identical validation to the environment
    // variable). Used by the singleton's environment read and by unit tests;
    // never touches resource or budget state. Re-arming is rejected while a
    // spec is active AND unfired; a consumed (spent) one-shot accepts a fresh
    // spec so the fault matrix can fire repeatedly in one process.
    void ArmFromSpec(const std::string &specJson);

    // Runtime test control (stage-4 requirement): re-check the control file
    // before each match attempt so a gate can be armed, hit, disarmed and
    // re-armed against instances that already exist, without restarting the
    // process. The file holds the same JSON spec (arm) or {"disarm":true};
    // a missing file leaves current state untouched; it is only read, never
    // written, by the gate. Dedup is CONTENT-based (mtime is not trusted:
    // two changes inside one filesystem timestamp tick must both apply), so
    // re-firing the same fault requires a changed file — bump the spec's
    // "control_revision" field. Default path
    // /run/akernel/retire-fault-control.json, override with
    // AKERNEL_RETIRE_FAULT_CONTROL_FILE. Test facility only: never touches
    // resource or budget state.
    void RefreshFromControlFile();
    // Read-only state snapshot for collectors (action/identity/operation/
    // deadline/enabled/consumed); no writer.
    struct State {
        std::string action;
        std::string sandboxID;
        std::string generation;
        std::string operationID;
        int64_t delayMs{ 0 };
        int64_t deadlineUnixMs{ 0 };
        bool enabled{ false };
        bool consumed{ false };
    };
    State GetStateForTest() const;

    // For "block_delete": true when THIS delete (sandbox + generation +
    // operation id of the stop request driving it) must be held for delayMs
    // before dispatch. delayMs is copied under the same lock that validated
    // the hit, so a concurrent disarm/re-arm cannot alter the fired action.
    // Consumes the one shot.
    bool BlockDeleteIfMatched(const std::string &sandboxID, const std::string &generation,
                              const std::string &operationID, int64_t &delayMs);

    // For "drop_receipt": true when THIS confirmed receipt send (sandbox +
    // generation + the response's request id) must be suppressed. Consumes
    // the one shot.
    bool DropReceiptIfMatched(const std::string &sandboxID, const std::string &generation,
                              const std::string &operationID);

private:
    // Caller holds mutex_; validates the exact identity, checks the deadline,
    // consumes the one shot and copies the hit's action parameters (delayMs)
    // before releasing the lock.
    bool ConsumeIfMatchedLocked(const char *action, const std::string &sandboxID,
                                const std::string &generation, const std::string &operationID,
                                int64_t &delayMs);
    void Disarm(const char *reason);
    void DisarmLocked(const char *reason);  // caller holds mutex_
    void ArmLocked(const std::string &specJson);  // caller holds mutex_
    static std::string ControlFilePath();

    mutable std::mutex mutex_;  // mutable: GetStateForTest() const reads under it
    std::string action_;
    std::string sandboxID_;
    std::string generation_;
    std::string operationID_;
    int64_t delayMs_{ 0 };
    int64_t deadlineUnixMs_{ 0 };
    bool enabled_{ false };
    bool consumed_{ false };
    std::string lastControlContent_;  // last APPLIED control-file content
};

}  // namespace functionsystem::runtime_manager

#endif
