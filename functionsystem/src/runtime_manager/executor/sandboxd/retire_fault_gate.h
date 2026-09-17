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
// AKERNEL_RETIRE_FAULT environment variable (JSON; absent/unparsable means
// disabled), matches one exact physical identity, fires at most once and
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
    // never touches resource or budget state.
    void ArmFromSpec(const std::string &specJson);

    // For "block_delete": true when this delete must be held for delayMs
    // before dispatch. Consumes the one shot.
    bool BlockDeleteIfMatched(const std::string &sandboxID, const std::string &generation, int64_t &delayMs);

    // For "drop_receipt": true when this confirmed receipt send must be
    // suppressed. Consumes the one shot.
    bool DropReceiptIfMatched(const std::string &sandboxID, const std::string &generation);

private:
    bool MatchAndConsume(const std::string &action, const std::string &sandboxID,
                         const std::string &generation, bool &armed);

    std::mutex mutex_;
    std::string action_;
    std::string sandboxID_;
    std::string generation_;
    int64_t delayMs_{ 0 };
    int64_t deadlineUnixMs_{ 0 };
    bool enabled_{ false };
    bool consumed_{ false };
};

}  // namespace functionsystem::runtime_manager

#endif
