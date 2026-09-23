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
#include <fstream>

#include <nlohmann/json.hpp>

#include "common/logs/logging.h"

namespace functionsystem::runtime_manager {
namespace {

constexpr int64_t kMaxHoldMs = 120000;  // hard bound of the confirmation stop-point
constexpr int kPollerTickMs = 100;      // in-window control-file observation cadence

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

RetireFaultGate::~RetireFaultGate()
{
    pollerStop_.store(true);
    if (prepPoller_.joinable()) {
        prepPoller_.join();  // bounded: the tick loop sees the stop flag
    }
}

void RetireFaultGate::StartPrepPollerLocked()
{
    if (pollerStarted_.exchange(true)) {
        return;  // the persistent poller re-checks mode/window each tick, so
                 // it automatically watches every capture after the first
    }
    prepPoller_ = std::thread([this]() {
        while (!pollerStop_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(kPollerTickMs));
            if (pollerStop_.load()) {
                break;
            }
            // Applies an in-window confirmation and freezes its arrival
            // moment; a no-op read when no control file exists.
            RefreshFromControlFile();
        }
    });
}

void RetireFaultGate::ArmFromSpec(const std::string &specJson)
{
    std::lock_guard<std::mutex> lock(mutex_);
    ArmLocked(specJson);
}

void RetireFaultGate::ArmLocked(const std::string &specJson)
{
    if (enabled_ && !consumed_ && mode_ == "exact") {
        YRLOG_WARN("retire-fault gate re-arm ignored: an exact spec is already active");
        return;
    }
    // A consumed one-shot, a prepare spec, or a held prepare may be replaced:
    // the fault matrix arms several faults against one live proxy process,
    // and the control file REPLACES a prepare spec with its confirmation.
    try {
        const auto spec = nlohmann::json::parse(specJson);
        const std::string action = spec.value("action", "");
        const std::string sandbox = spec.value("sandbox", "");
        const std::string generation = spec.value("generation", "");
        const std::string operation = spec.value("operation_id", "");
        // int64_t defaults: value() returns the default's type, and an int
        // default silently wraps 13-digit unix-ms deadlines negative.
        const int64_t delayMs = spec.value("delay_ms", static_cast<int64_t>(0));
        const int64_t deadlineUnixMs = spec.value("deadline_unix_ms", static_cast<int64_t>(0));
        const int64_t holdMs = spec.value("hold_ms", static_cast<int64_t>(0));
        const bool prepare = spec.value("prepare", false);
        const bool actionOk = action == "block_delete" || action == "drop_receipt";
        if (!actionOk || sandbox.empty() || generation.empty() || delayMs < 0
            || deadlineUnixMs <= 0) {
            YRLOG_WARN("retire-fault gate spec ignored (invalid action/identity/deadline): {}",
                       specJson);
            return;
        }
        if (prepare) {
            if (!operation.empty()) {
                YRLOG_WARN("retire-fault gate prepare spec must NOT name operation_id (it is "
                           "observed at the stop-point): {}", specJson);
                return;
            }
            if (holdMs < 1000 || holdMs > kMaxHoldMs) {
                YRLOG_WARN("retire-fault gate prepare hold_ms must be 1000..{}: {}", kMaxHoldMs,
                           specJson);
                return;
            }
            mode_ = "prep_wait";
            operationID_.clear();
            holdMs_ = holdMs;
        } else {
            if (operation.empty()) {
                YRLOG_WARN("retire-fault gate exact spec requires operation_id (or use "
                           "prepare): {}", specJson);
                return;
            }
            mode_ = "exact";
            operationID_ = operation;
            holdMs_ = 0;
        }
        // Window freeze: when an exact spec REPLACES a held prepare for the
        // same action+identity (the confirmation), the frozen prep-window
        // deadline survives so a late dispatch cannot apply it out of
        // window. Any other arm leaves the prepare lineage.
        const bool confirmOfHeldPrep = (mode_ == "exact" && fromPrepConfirm_ == false
                                        && prepWindowDeadlineUnixMs_ > 0
                                        && action_ == action && sandboxID_ == sandbox
                                        && generation_ == generation);
        action_ = action;
        sandboxID_ = sandbox;
        generation_ = generation;
        delayMs_ = delayMs;
        deadlineUnixMs_ = deadlineUnixMs;
        if (!confirmOfHeldPrep) {
            prepWindowDeadlineUnixMs_ = 0;
            fromPrepConfirm_ = false;
            confirmArrivalUnixMs_ = 0;
        } else {
            fromPrepConfirm_ = true;
            // Arrival frozen at the moment the poller (or a refresh) applied
            // the confirmation — judged against the frozen window, not
            // against whenever the resolve later runs.
            confirmArrivalUnixMs_ = NowUnixMs();
        }
        enabled_ = true;
        consumed_ = false;  // fresh one-shot replaces the spent/prepare spec
        YRLOG_WARN("retire-fault gate ARMED mode({}) action({}) sandbox({}) generation({}) "
                   "operation({}) delay_ms({}) hold_ms({}) deadline_unix_ms({}) "
                   "prep_window_deadline_unix_ms({}) from_prep_confirm({})",
                   mode_, action_, sandboxID_, generation_, operationID_, delayMs_, holdMs_,
                   deadlineUnixMs_, prepWindowDeadlineUnixMs_, fromPrepConfirm_);
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
    mode_.clear();
    action_.clear();
    sandboxID_.clear();
    generation_.clear();
    operationID_.clear();
    delayMs_ = 0;
    deadlineUnixMs_ = 0;
    holdMs_ = 0;
    prepWindowDeadlineUnixMs_ = 0;
    fromPrepConfirm_ = false;
    confirmArrivalUnixMs_ = 0;
    YRLOG_WARN("retire-fault gate DISARMED reason({})", reason);
}

void RetireFaultGate::RefreshFromControlFile()
{
    const auto path = ControlFilePath();
    std::ifstream input(path);
    if (!input) {
        return;  // no control file: keep current state
    }
    std::string spec((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    if (spec.empty()) {
        return;
    }
    // CONTENT dedup, not mtime: filesystem timestamps can alias two changes
    // into one tick, which would silently drop an arm/disarm. Identical
    // content never re-applies — re-firing the same fault requires a changed
    // file (bump the spec's control_revision field).
    std::lock_guard<std::mutex> lock(mutex_);
    if (spec == lastControlContent_) {
        return;
    }
    lastControlContent_ = spec;
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
    return State{ mode_, action_, sandboxID_, generation_, operationID_, delayMs_,
                  deadlineUnixMs_, holdMs_, prepWindowDeadlineUnixMs_, fromPrepConfirm_,
                  enabled_, consumed_ };
}

bool RetireFaultGate::ConsumeExactLocked(const char *action, const std::string &sandboxID,
                                         const std::string &generation,
                                         const std::string &operationID, int64_t &delayMs)
{
    delayMs = 0;
    if (!enabled_ || consumed_ || mode_ != "exact") {
        return false;
    }
    if (action_ != action || sandboxID_ != sandboxID || generation_ != generation
        || operationID_ != operationID) {
        return false;  // exact operation identity only: neighbours never match
    }
    const int64_t now = NowUnixMs();
    if (fromPrepConfirm_
        && (confirmArrivalUnixMs_ <= 0 || confirmArrivalUnixMs_ > prepWindowDeadlineUnixMs_)) {
        // The exact spec is a confirmation of a held prepare whose ARRIVAL
        // lies outside the frozen window (late write, or arrival never
        // recorded): it must NOT fire, no matter when the resolve runs.
        consumed_ = true;
        YRLOG_WARN("retire-fault gate PREP_WINDOW_EXPIRED action({}) sandbox({}) "
                   "generation({}) operation({}) window_deadline_unix_ms({}) "
                   "confirm_arrival_unix_ms({})",
                   action_, sandboxID_, generation_, operationID_, prepWindowDeadlineUnixMs_,
                   confirmArrivalUnixMs_);
        return false;
    }
    if (now > deadlineUnixMs_) {
        consumed_ = true;
        YRLOG_WARN("retire-fault gate EXPIRED action({}) sandbox({}) generation({}) "
                   "operation({}) deadline_unix_ms({})",
                   action_, sandboxID_, generation_, operationID_, deadlineUnixMs_);
        return false;
    }
    consumed_ = true;
    delayMs = delayMs_;  // copied under THIS lock: a concurrent disarm/re-arm
                         // cannot alter the parameters of the fired action
    // The hit log is the in-process receipt that the intended message was
    // actually intercepted (plan 3.3: trigger-condition + intercepted message).
    YRLOG_WARN("retire-fault gate HIT action({}) sandbox({}) generation({}) operation({})",
               action_, sandboxID_, generation_, operationID_);
    return true;
}

RetireFaultGate::BlockDecision RetireFaultGate::BlockDeleteIfMatched(
    const std::string &sandboxID, const std::string &generation,
    const std::string &operationID)
{
    RefreshFromControlFile();
    std::lock_guard<std::mutex> lock(mutex_);
    BlockDecision decision;
    int64_t delayMs = 0;
    if (ConsumeExactLocked("block_delete", sandboxID, generation, operationID, delayMs)) {
        decision.decision = Decision::MatchExact;
        decision.delayMs = delayMs;
        return decision;
    }
    if (enabled_ && !consumed_ && mode_ == "prep_wait" && action_ == "block_delete"
        && sandboxID_ == sandboxID && generation_ == generation && NowUnixMs() <= deadlineUnixMs_) {
        // Bounded confirmation stop-point: capture the unpredictable
        // operation id, freeze the window, hold the message for holdMs_, and
        // apply the fault only if an exact confirmation naming this id
        // ARRIVES inside the frozen window (observed by the poller).
        mode_ = "prep_held";
        operationID_ = operationID;
        prepWindowDeadlineUnixMs_ = NowUnixMs() + holdMs_;
        fromPrepConfirm_ = false;
        confirmArrivalUnixMs_ = 0;
        StartPrepPollerLocked();
        decision.decision = Decision::MatchPrepare;
        decision.holdMs = holdMs_;
        decision.windowDeadlineUnixMs = prepWindowDeadlineUnixMs_;
        YRLOG_WARN("retire-fault gate PREPARED(block) sandbox({}) generation({}) "
                   "operation({}) hold_ms({}) window_deadline_unix_ms({}) — confirm via "
                   "control file with this exact operation id within the window",
                   sandboxID_, generation_, operationID_, holdMs_, prepWindowDeadlineUnixMs_);
        return decision;
    }
    return decision;
}

RetireFaultGate::BlockDecision RetireFaultGate::ResolveBlock(
    const std::string &sandboxID, const std::string &generation,
    const std::string &operationID)
{
    RefreshFromControlFile();  // may have applied the exact confirmation
    std::lock_guard<std::mutex> lock(mutex_);
    BlockDecision decision;
    int64_t delayMs = 0;
    if (ConsumeExactLocked("block_delete", sandboxID, generation, operationID, delayMs)) {
        // confirmed INSIDE the frozen window: apply the fault to THIS held
        // message (ConsumeExactLocked rejects out-of-window confirmations)
        decision.decision = Decision::MatchExact;
        decision.delayMs = delayMs;
        return decision;
    }
    if (enabled_ && mode_ == "prep_held" && action_ == "block_delete"
        && sandboxID_ == sandboxID && generation_ == generation
        && operationID_ == operationID) {
        YRLOG_WARN("retire-fault gate PREP_EXPIRED(block) sandbox({}) generation({}) "
                   "operation({}): no exact confirmation within hold_ms({}), message "
                   "passes through",
                   sandboxID_, generation_, operationID_, holdMs_);
        DisarmLocked("prepare-window-elapsed");
    }
    return decision;  // NoMatch: pass the held message through, bounded
}

RetireFaultGate::ReceiptGate RetireFaultGate::DropReceiptIfMatched(
    const std::string &sandboxID, const std::string &generation,
    const std::string &operationID)
{
    RefreshFromControlFile();
    std::lock_guard<std::mutex> lock(mutex_);
    ReceiptGate gate;
    int64_t delayMs = 0;
    if (ConsumeExactLocked("drop_receipt", sandboxID, generation, operationID, delayMs)) {
        gate.decision = ReceiptDecision::Drop;
        return gate;
    }
    if (enabled_ && !consumed_ && mode_ == "prep_wait" && action_ == "drop_receipt"
        && sandboxID_ == sandboxID && generation_ == generation && NowUnixMs() <= deadlineUnixMs_) {
        // Hold the ORIGINAL receipt BEFORE its first upstream send: once the
        // first confirmed receipt travels, the budget may already settle
        // upstream and dropping a later replay proves nothing about
        // "deleted but unsettled". The caller keeps this message and calls
        // ResolveReceipt when the frozen window closes. THIS message's hold
        // parameters are returned immutable — no lock-separated re-read.
        mode_ = "prep_held";
        operationID_ = operationID;
        prepWindowDeadlineUnixMs_ = NowUnixMs() + holdMs_;
        fromPrepConfirm_ = false;
        confirmArrivalUnixMs_ = 0;
        StartPrepPollerLocked();
        gate.decision = ReceiptDecision::HoldForConfirm;
        gate.holdMs = holdMs_;
        gate.windowDeadlineUnixMs = prepWindowDeadlineUnixMs_;
        YRLOG_WARN("retire-fault gate PREPARED(receipt) sandbox({}) generation({}) "
                   "operation({}) hold_ms({}) window_deadline_unix_ms({}) — original "
                   "receipt held before first send; confirm via control file with this "
                   "exact operation id within the window to drop it",
                   sandboxID_, generation_, operationID_, holdMs_, prepWindowDeadlineUnixMs_);
        return gate;
    }
    gate.decision = ReceiptDecision::Send;
    return gate;
}

RetireFaultGate::ReceiptDecision RetireFaultGate::ResolveReceipt(
    const std::string &sandboxID, const std::string &generation,
    const std::string &operationID)
{
    RefreshFromControlFile();  // may have applied the exact confirmation
    std::lock_guard<std::mutex> lock(mutex_);
    int64_t delayMs = 0;
    if (ConsumeExactLocked("drop_receipt", sandboxID, generation, operationID, delayMs)) {
        // confirmed INSIDE the frozen window: the ORIGINAL receipt is dropped
        // (0 upstream sends); the durable receipt stays in sandboxd and the
        // caller's retry replays it to settle exactly once.
        return ReceiptDecision::Drop;
    }
    if (enabled_ && mode_ == "prep_held" && action_ == "drop_receipt"
        && sandboxID_ == sandboxID && generation_ == generation
        && operationID_ == operationID) {
        YRLOG_WARN("retire-fault gate PREP_EXPIRED(receipt) sandbox({}) generation({}) "
                   "operation({}): no exact confirmation within hold_ms({}), original "
                   "receipt is sent unchanged",
                   sandboxID_, generation_, operationID_, holdMs_);
        DisarmLocked("prepare-window-elapsed");
    }
    return ReceiptDecision::Send;  // exactly one send: the ORIGINAL message
}

}  // namespace functionsystem::runtime_manager
