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

#include "retire_fault_gate.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <nlohmann/json.hpp>

#include "common/logs/logging.h"

namespace functionsystem::runtime_manager {
namespace {

int64_t NowUnixMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

}  // namespace

RetireFaultGate &RetireFaultGate::Instance()
{
    static RetireFaultGate gate;
    static const bool envArmed = [] {
        const char *raw = std::getenv("AKERNEL_RETIRE_FAULT");
        if (raw != nullptr && *raw != '\0') {
            gate.ArmFromSpec(raw);  // production arming; absent variable keeps the gate off
        }
        return true;
    }();
    (void)envArmed;
    return gate;
}

void RetireFaultGate::ArmFromSpec(const std::string &specJson)
{
    std::lock_guard<std::mutex> lock(mutex_);
    ArmLocked(specJson);
}

void RetireFaultGate::ArmLocked(const std::string &specJson)
{
    if (enabled_ && !consumed_) {
        YRLOG_WARN("retire-fault gate re-arm ignored: a spec is already active");
        return;
    }
    // enabled_ && consumed_ (a spent one-shot) FALLS THROUGH: the fault
    // matrix arms several faults against one live proxy process, so a
    // consumed gate must accept a fresh spec without a restart.
    try {
        const auto spec = nlohmann::json::parse(specJson);
        action_ = spec.value("action", "");
        sandboxID_ = spec.value("sandbox", "");
        generation_ = spec.value("generation", "");
        // int64_t defaults: value() returns the default's type, and an int
        // default silently wraps 13-digit unix-ms deadlines negative.
        delayMs_ = spec.value("delay_ms", static_cast<int64_t>(0));
        deadlineUnixMs_ = spec.value("deadline_unix_ms", static_cast<int64_t>(0));
        if ((action_ == "block_delete" || action_ == "drop_receipt") && !sandboxID_.empty()
            && !generation_.empty() && delayMs_ >= 0 && deadlineUnixMs_ > 0) {
            enabled_ = true;
            consumed_ = false;  // fresh one-shot replaces the spent spec
            YRLOG_WARN("retire-fault gate ARMED action({}) sandbox({}) generation({}) delay_ms({}) "
                       "deadline_unix_ms({})",
                       action_, sandboxID_, generation_, delayMs_, deadlineUnixMs_);
        } else {
            YRLOG_WARN("retire-fault gate spec ignored (invalid action/identity/deadline): {}", specJson);
        }
    } catch (const std::exception &error) {
        YRLOG_WARN("retire-fault gate spec unparsable, staying disabled: {}", error.what());
    }
}

std::string RetireFaultGate::ControlFilePath()
{
    const char *raw = std::getenv("AKERNEL_RETIRE_FAULT_CONTROL_FILE");
    return raw != nullptr && *raw != '\0' ? std::string(raw)
                                           : std::string("/run/akernel/retire-fault-control.json");
}

void RetireFaultGate::Disarm(const char *reason)
{
    std::lock_guard<std::mutex> lock(mutex_);
    DisarmLocked(reason);
}

void RetireFaultGate::DisarmLocked(const char *reason)
{
    enabled_ = false;
    consumed_ = false;
    action_.clear();
    sandboxID_.clear();
    generation_.clear();
    delayMs_ = 0;
    deadlineUnixMs_ = 0;
    YRLOG_WARN("retire-fault gate DISARMED reason({})", reason);
}

void RetireFaultGate::RefreshFromControlFile()
{
    const auto path = ControlFilePath();
    std::error_code ec;
    const auto mtime = std::filesystem::last_write_time(path, ec);
    if (ec) {
        return;  // no control file: keep current state
    }
    const auto mtimeMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        mtime.time_since_epoch()).count();
    std::lock_guard<std::mutex> lock(mutex_);
    if (mtimeMs == lastControlMtimeMs_) {
        return;  // already applied
    }
    lastControlMtimeMs_ = mtimeMs;
    std::ifstream input(path);
    if (!input) {
        return;
    }
    std::string spec((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    if (spec.empty()) {
        return;
    }
    try {
        const auto parsed = nlohmann::json::parse(spec);
        if (parsed.is_object() && parsed.value("disarm", false)) {
            if (enabled_ || consumed_) {
                DisarmLocked("control-file");
            }
            return;
        }
    } catch (const std::exception &) {
        // not a disarm object: fall through to spec validation
    }
    ArmLocked(spec);  // caller already holds mutex_; ArmFromSpec would relock
}

RetireFaultGate::State RetireFaultGate::GetStateForTest() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return State{ action_, sandboxID_, generation_, delayMs_, deadlineUnixMs_, enabled_, consumed_ };
}

bool RetireFaultGate::MatchAndConsume(const std::string &action, const std::string &sandboxID,
                                      const std::string &generation, bool &armed)
{
    RefreshFromControlFile();
    std::lock_guard<std::mutex> lock(mutex_);
    armed = enabled_;
    if (!enabled_) {
        return false;
    }
    if (consumed_) {
        return false;  // one shot already fired
    }
    if (action_ != action || sandboxID_ != sandboxID || generation_ != generation) {
        return false;  // exact physical identity only: neighbours never match
    }
    if (NowUnixMs() > deadlineUnixMs_) {
        consumed_ = true;
        YRLOG_WARN("retire-fault gate EXPIRED action({}) sandbox({}) generation({}) deadline_unix_ms({})",
                   action_, sandboxID_, generation_, deadlineUnixMs_);
        return false;
    }
    consumed_ = true;
    // The hit log is the in-process evidence that the intended message was
    // actually intercepted (plan 3.3: trigger-condition + intercepted message).
    YRLOG_WARN("retire-fault gate HIT action({}) sandbox({}) generation({})", action_, sandboxID_, generation_);
    return true;
}

bool RetireFaultGate::BlockDeleteIfMatched(const std::string &sandboxID, const std::string &generation,
                                           int64_t &delayMs)
{
    bool armed = false;
    delayMs = 0;
    if (!MatchAndConsume("block_delete", sandboxID, generation, armed)) {
        return false;
    }
    delayMs = delayMs_;
    return true;
}

bool RetireFaultGate::DropReceiptIfMatched(const std::string &sandboxID, const std::string &generation)
{
    bool armed = false;
    return MatchAndConsume("drop_receipt", sandboxID, generation, armed);
}

}  // namespace functionsystem::runtime_manager
