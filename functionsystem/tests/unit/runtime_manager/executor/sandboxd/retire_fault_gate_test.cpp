/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "runtime_manager/executor/sandboxd/retire_fault_gate.h"
#include <gtest/gtest.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>

namespace functionsystem::test {
using runtime_manager::RetireFaultGate;

namespace {
int64_t FutureUnixMs(int64_t offsetMs)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count() + offsetMs;
}
std::string BlockSpec(int64_t delayMs, int64_t deadlineUnixMs)
{
    return std::string(R"({"action":"block_delete","sandbox":"sbox-1","generation":"g-7",)"
                       R"("operation_id":"op-1","delay_ms":)")
        + std::to_string(delayMs) + R"(,"deadline_unix_ms":)" + std::to_string(deadlineUnixMs) + "}";
}
std::string PrepareBlockSpec(int64_t holdMs, int64_t deadlineUnixMs)
{
    return std::string(R"({"action":"block_delete","sandbox":"sbox-1","generation":"g-7",)"
                       R"("prepare":true,"hold_ms":)")
        + std::to_string(holdMs) + R"(,"delay_ms":9000,"deadline_unix_ms":)"
        + std::to_string(deadlineUnixMs) + "}";
}
std::string ConfirmBlockSpec(const std::string &operation, int64_t deadlineUnixMs)
{
    return std::string(R"({"action":"block_delete","sandbox":"sbox-1","generation":"g-7",)"
                       R"("operation_id":")" + operation + R"(","delay_ms":7000,)"
                       R"("deadline_unix_ms":)" + std::to_string(deadlineUnixMs) + "}");
}

// The control file is deduplicated by CONTENT (mtime is not trusted), so no
// timestamp forcing is needed or meaningful here: successive writes apply in
// order even inside one filesystem timestamp tick.
struct ControlFile {
    std::filesystem::path path;
    explicit ControlFile(const std::string &name)
        : path(std::filesystem::path(testing::TempDir()) / name)
    {
    }
    void write(const std::string &content)
    {
        std::ofstream out(path, std::ios::trunc);
        out << content;
    }
    void remove()
    {
        std::error_code ec;
        std::filesystem::remove(path, ec);
    }
};

// Points the gate at this process's control file (never the production
// default) and restores the environment on scope exit.
struct ControlFileEnv {
    explicit ControlFileEnv(const std::filesystem::path &path)
    {
        saved = std::getenv("AKERNEL_RETIRE_FAULT_CONTROL_FILE");
        setenv("AKERNEL_RETIRE_FAULT_CONTROL_FILE", path.c_str(), 1);
    }
    ~ControlFileEnv()
    {
        if (saved != nullptr) {
            setenv("AKERNEL_RETIRE_FAULT_CONTROL_FILE", saved, 1);
        } else {
            unsetenv("AKERNEL_RETIRE_FAULT_CONTROL_FILE");
        }
    }
    const char *saved = nullptr;
};
}  // namespace

using Decision = RetireFaultGate::Decision;

TEST(RetireFaultGateTest, DisabledGateNeverMatches)
{
    RetireFaultGate gate;  // no ArmFromSpec: default off
    auto decision = gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1");
    EXPECT_EQ(decision.decision, Decision::NoMatch);
    EXPECT_EQ(decision.delayMs, 0);
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-1", "g-7", "op-1"));
}

TEST(RetireFaultGateTest, BlockDeleteMatchesExactIdentityOnce)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(45000, FutureUnixMs(120000)));
    // Neighbour identities, the other action, and a WRONG OPERATION never
    // consume the one shot.
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-1", "g-7", "op-1"));
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-2", "g-7", "op-1").decision, Decision::NoMatch);
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-8", "op-1").decision, Decision::NoMatch);
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-9").decision, Decision::NoMatch);
    const auto hit = gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1");
    EXPECT_EQ(hit.decision, Decision::MatchExact);
    EXPECT_EQ(hit.delayMs, 45000);
    // One-shot: the retried delete must pass through untouched.
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision, Decision::NoMatch);
}

TEST(RetireFaultGateTest, DropReceiptMatchesOnceAndBlockDoesNot)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"sbox-9","generation":"g-1",)"
                     R"("operation_id":"op-4","delay_ms":0,"deadline_unix_ms":)"
                     + std::to_string(FutureUnixMs(60000)) + "}");
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-9", "g-1", "op-4").decision, Decision::NoMatch);
    EXPECT_TRUE(gate.DropReceiptIfMatched("sbox-9", "g-1", "op-4"));
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-9", "g-1", "op-4"));
}

TEST(RetireFaultGateTest, ExpiredOrInvalidSpecsStayInactive)
{
    RetireFaultGate expired;
    expired.ArmFromSpec(BlockSpec(1000, FutureUnixMs(-1000)));
    EXPECT_EQ(expired.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision, Decision::NoMatch);

    RetireFaultGate garbage;
    garbage.ArmFromSpec("not-json");
    EXPECT_EQ(garbage.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision, Decision::NoMatch);

    RetireFaultGate unknownAction;
    unknownAction.ArmFromSpec(R"({"action":"reboot_world","sandbox":"s","generation":"g",)"
                              R"("operation_id":"o","delay_ms":1,)"
                              R"("deadline_unix_ms":9223372036854775807})");
    EXPECT_FALSE(unknownAction.DropReceiptIfMatched("s", "g", "o"));

    RetireFaultGate partial;
    partial.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"","generation":"g",)"
                        R"("operation_id":"o"})");
    EXPECT_FALSE(partial.DropReceiptIfMatched("", "g", "o"));

    // Missing operation id is an incomplete operation identity: never armed
    // as an exact spec.
    RetireFaultGate noOperation;
    noOperation.ArmFromSpec(R"({"action":"block_delete","sandbox":"s","generation":"g",)"
                            R"("delay_ms":1,"deadline_unix_ms":9223372036854775807})");
    EXPECT_EQ(noOperation.BlockDeleteIfMatched("s", "g", "").decision, Decision::NoMatch);
    EXPECT_FALSE(noOperation.GetStateForTest().enabled);
}

TEST(RetireFaultGateTest, PrepareSpecValidationRejectsBadForms)
{
    // naming the operation defeats the stop-point contract: rejected
    RetireFaultGate withOp;
    withOp.ArmFromSpec(R"({"action":"block_delete","sandbox":"s","generation":"g",)"
                       R"("prepare":true,"operation_id":"op","hold_ms":5000,)"
                       R"("delay_ms":1,"deadline_unix_ms":9223372036854775807})");
    EXPECT_FALSE(withOp.GetStateForTest().enabled);

    // hold_ms below the floor / above the hard cap: rejected
    RetireFaultGate tooShort;
    tooShort.ArmFromSpec(PrepareBlockSpec(100, FutureUnixMs(60000)));
    EXPECT_FALSE(tooShort.GetStateForTest().enabled);
    RetireFaultGate tooLong;
    tooLong.ArmFromSpec(PrepareBlockSpec(120001, FutureUnixMs(60000)));
    EXPECT_FALSE(tooLong.GetStateForTest().enabled);
}

TEST(RetireFaultGateTest, RearmWhileActiveIsRejected)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(1000, FutureUnixMs(60000)));
    gate.ArmFromSpec(BlockSpec(2000, FutureUnixMs(60000)));
    const auto hit = gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1");
    EXPECT_EQ(hit.decision, Decision::MatchExact);
    EXPECT_EQ(hit.delayMs, 1000);  // first spec wins, not the ignored re-arm
}

TEST(RetireFaultGateTest, RearmAfterConsumedShotIsAccepted)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(1000, FutureUnixMs(60000)));
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision,
              Decision::MatchExact);  // spent
    // A consumed one-shot accepts a fresh spec (fault matrix: 3 repeats in
    // one live proxy process); the new parameters fire normally.
    gate.ArmFromSpec(BlockSpec(2000, FutureUnixMs(60000)));
    const auto hit = gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1");
    EXPECT_EQ(hit.decision, Decision::MatchExact);
    EXPECT_EQ(hit.delayMs, 2000);
    const auto state = gate.GetStateForTest();
    EXPECT_TRUE(state.enabled);
    EXPECT_TRUE(state.consumed);
}

TEST(RetireFaultGateTest, StateSnapshotExposesOperationIdentity)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(500, FutureUnixMs(60000)));
    const auto state = gate.GetStateForTest();  // const read must compile+lock
    EXPECT_EQ(state.mode, "exact");
    EXPECT_TRUE(state.enabled);
    EXPECT_FALSE(state.consumed);
    EXPECT_EQ(state.action, "block_delete");
    EXPECT_EQ(state.sandboxID, "sbox-1");
    EXPECT_EQ(state.generation, "g-7");
    EXPECT_EQ(state.operationID, "op-1");
    EXPECT_EQ(state.delayMs, 500);
}

TEST(RetireFaultGateTest, PrepareBlockCapturesHoldsAndAppliesOnConfirm)
{
    // KillInstance assigns a random per-attempt forward id, so the test can
    // only learn the operation at the stop-point: capture -> confirm -> the
    // HELD message itself gets the fault.
    RetireFaultGate gate;
    gate.ArmFromSpec(PrepareBlockSpec(30000, FutureUnixMs(120000)));
    EXPECT_EQ(gate.GetStateForTest().mode, "prep_wait");
    const auto held = gate.BlockDeleteIfMatched("sbox-1", "g-7", "forward-abc");
    EXPECT_EQ(held.decision, Decision::MatchPrepare);
    EXPECT_EQ(held.holdMs, 30000);
    const auto state = gate.GetStateForTest();
    EXPECT_EQ(state.mode, "prep_held");
    EXPECT_EQ(state.operationID, "forward-abc");  // the captured observation
    // confirmation names the EXACT captured operation
    gate.ArmFromSpec(ConfirmBlockSpec("forward-abc", FutureUnixMs(120000)));
    const auto resolved = gate.ResolveBlock("sbox-1", "g-7", "forward-abc");
    EXPECT_EQ(resolved.decision, Decision::MatchExact);
    EXPECT_EQ(resolved.delayMs, 7000);
    // one-shot after apply: a retry of the same op passes through
    EXPECT_EQ(gate.ResolveBlock("sbox-1", "g-7", "forward-abc").decision, Decision::NoMatch);
}

TEST(RetireFaultGateTest, PrepareBlockWrongConfirmNeverFires)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(PrepareBlockSpec(30000, FutureUnixMs(120000)));
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "forward-abc").decision,
              Decision::MatchPrepare);
    // a confirmation naming a DIFFERENT operation does not apply to the held
    // message, and cannot fire on it either
    gate.ArmFromSpec(ConfirmBlockSpec("forward-OTHER", FutureUnixMs(120000)));
    EXPECT_EQ(gate.ResolveBlock("sbox-1", "g-7", "forward-abc").decision, Decision::NoMatch);
    // the different-op confirmation stays armed for ITS operation only
    const auto state = gate.GetStateForTest();
    EXPECT_TRUE(state.enabled);
    EXPECT_EQ(state.operationID, "forward-OTHER");
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "forward-OTHER").decision,
              Decision::MatchExact);
}

TEST(RetireFaultGateTest, PrepareBlockPassesThroughWhenUnconfirmed)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(PrepareBlockSpec(30000, FutureUnixMs(120000)));
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "forward-abc").decision,
              Decision::MatchPrepare);
    // no confirmation written: the held dispatch is released, bounded
    EXPECT_EQ(gate.ResolveBlock("sbox-1", "g-7", "forward-abc").decision, Decision::NoMatch);
    EXPECT_FALSE(gate.GetStateForTest().enabled);
    // and nothing fires afterwards for the same or other operations
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "forward-abc").decision,
              Decision::NoMatch);
}

TEST(RetireFaultGateTest, PrepareReceiptHoldsOriginalThenDropsOnTimelyConfirm)
{
    // The stop-point is on the ORIGINAL receipt BEFORE its first upstream
    // send: once the first confirmed receipt travels the budget may already
    // settle upstream, so "pass first, drop the replay" proves nothing.
    // Decision mapping to sends: HoldForConfirm -> 0 sends; timely confirm ->
    // Drop (still 0 sends of the original; the durable receipt stays in
    // sandboxd for the caller's replay).
    RetireFaultGate gate;
    gate.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"sbox-1","generation":"g-7",)"
                     R"("prepare":true,"hold_ms":30000,"delay_ms":0,"deadline_unix_ms":)"
                     + std::to_string(FutureUnixMs(120000)) + "}");
    EXPECT_EQ(gate.DropReceiptIfMatched("sbox-1", "g-7", "forward-rc1"),
              RetireFaultGate::ReceiptDecision::HoldForConfirm);  // 0 sends
    const auto state = gate.GetStateForTest();
    EXPECT_EQ(state.mode, "prep_held");
    EXPECT_EQ(state.operationID, "forward-rc1");
    EXPECT_GT(state.prepWindowDeadlineUnixMs, 0);
    EXPECT_FALSE(state.fromPrepConfirm);
    // timely exact confirmation (same action+identity): window survives
    gate.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"sbox-1","generation":"g-7",)"
                     R"("operation_id":"forward-rc1","delay_ms":0,"deadline_unix_ms":)"
                     + std::to_string(FutureUnixMs(120000)) + "}");
    EXPECT_TRUE(gate.GetStateForTest().fromPrepConfirm);
    EXPECT_GT(gate.GetStateForTest().prepWindowDeadlineUnixMs, 0);
    EXPECT_EQ(gate.ResolveReceipt("sbox-1", "g-7", "forward-rc1"),
              RetireFaultGate::ReceiptDecision::Drop);  // still 0 sends
    EXPECT_TRUE(gate.GetStateForTest().consumed);
}

TEST(RetireFaultGateTest, PrepareReceiptUnconfirmedSendsOriginalExactlyOnce)
{
    // Unconfirmed stop-point: the ORIGINAL message is sent unchanged —
    // exactly one send, nothing reordered or duplicated.
    RetireFaultGate gate;
    gate.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"sbox-1","generation":"g-7",)"
                     R"("prepare":true,"hold_ms":30000,"delay_ms":0,"deadline_unix_ms":)"
                     + std::to_string(FutureUnixMs(120000)) + "}");
    EXPECT_EQ(gate.DropReceiptIfMatched("sbox-1", "g-7", "forward-rc1"),
              RetireFaultGate::ReceiptDecision::HoldForConfirm);  // 0 sends
    EXPECT_EQ(gate.ResolveReceipt("sbox-1", "g-7", "forward-rc1"),
              RetireFaultGate::ReceiptDecision::Send);            // exactly 1 send
    EXPECT_FALSE(gate.GetStateForTest().enabled);                 // disarmed
    // Nothing fires afterwards: retries all send.
    EXPECT_EQ(gate.DropReceiptIfMatched("sbox-1", "g-7", "forward-rc1"),
              RetireFaultGate::ReceiptDecision::Send);
}

TEST(RetireFaultGateTest, PrepareReceiptLateConfirmRejectedOutsideFrozenWindow)
{
    // The window deadline is FROZEN at capture and survives the confirmation
    // arm: a confirmation that only reaches the gate after the window (event
    // loop scheduling late) must not drop anything — the original is sent.
    RetireFaultGate gate;
    gate.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"sbox-1","generation":"g-7",)"
                     R"("prepare":true,"hold_ms":1000,"delay_ms":0,"deadline_unix_ms":)"
                     + std::to_string(FutureUnixMs(120000)) + "}");
    EXPECT_EQ(gate.DropReceiptIfMatched("sbox-1", "g-7", "forward-rc1"),
              RetireFaultGate::ReceiptDecision::HoldForConfirm);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));  // window closes
    gate.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"sbox-1","generation":"g-7",)"
                     R"("operation_id":"forward-rc1","delay_ms":0,"deadline_unix_ms":)"
                     + std::to_string(FutureUnixMs(120000)) + "}");
    EXPECT_EQ(gate.ResolveReceipt("sbox-1", "g-7", "forward-rc1"),
              RetireFaultGate::ReceiptDecision::Send);  // out-of-window: send original
}

TEST(RetireFaultGateTest, PrepareBlockLateConfirmRejectedOutsideFrozenWindow)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(PrepareBlockSpec(1000, FutureUnixMs(120000)));
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "forward-abc").decision,
              Decision::MatchPrepare);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));  // window closes
    gate.ArmFromSpec(ConfirmBlockSpec("forward-abc", FutureUnixMs(120000)));
    EXPECT_EQ(gate.ResolveBlock("sbox-1", "g-7", "forward-abc").decision,
              Decision::NoMatch);  // late confirm rejected; held delete passes
    // the out-of-window confirmation consumed itself: nothing can fire later
    EXPECT_TRUE(gate.GetStateForTest().consumed);
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "forward-abc").decision,
              Decision::NoMatch);
}

TEST(RetireFaultGateTest, ControlFileArmsAndFiresWithoutRestart)
{
    ControlFile control("gate-arm.json");
    control.remove();
    ControlFileEnv env(control.path);
    RetireFaultGate gate;
    // No control file yet: gate stays off and nothing matches.
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision, Decision::NoMatch);
    control.write(BlockSpec(30000, FutureUnixMs(120000)));
    // The next match attempt rereads the file, arms, and consumes in the
    // same evaluation: stage-4 fault injection without a process restart.
    const auto hit = gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1");
    EXPECT_EQ(hit.decision, Decision::MatchExact);
    EXPECT_EQ(hit.delayMs, 30000);
    const auto state = gate.GetStateForTest();
    EXPECT_TRUE(state.enabled);
    EXPECT_TRUE(state.consumed);
    EXPECT_EQ(state.sandboxID, "sbox-1");
    EXPECT_EQ(state.operationID, "op-1");
    control.remove();
}

TEST(RetireFaultGateTest, ControlFileDisarmClearsActiveGate)
{
    ControlFile control("gate-disarm.json");
    control.remove();
    ControlFileEnv env(control.path);
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(1000, FutureUnixMs(60000)));
    control.write(R"({"disarm":true})");
    // Any match attempt first applies the disarm: enabled/consumed/identity
    // all clear, so a stale spec can never fire after an explicit disarm.
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision, Decision::NoMatch);
    const auto state = gate.GetStateForTest();
    EXPECT_FALSE(state.enabled);
    EXPECT_FALSE(state.consumed);
    EXPECT_TRUE(state.sandboxID.empty());
    EXPECT_TRUE(state.action.empty());
    EXPECT_TRUE(state.operationID.empty());
    control.remove();
}

TEST(RetireFaultGateTest, ControlFileRearmsAfterConsumedHit)
{
    ControlFile control("gate-rearm.json");
    control.remove();
    ControlFileEnv env(control.path);
    RetireFaultGate gate;
    // First shot: block_delete against sbox-1/op-1.
    control.write(BlockSpec(5000, FutureUnixMs(120000)));
    const auto first = gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1");
    EXPECT_EQ(first.decision, Decision::MatchExact);
    EXPECT_EQ(first.delayMs, 5000);
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision,
              Decision::NoMatch);  // consumed
    // Second shot against a DIFFERENT identity: the fault matrix needs
    // repeated faults from one live proxy process.
    control.write(std::string(R"({"action":"drop_receipt","sandbox":"sbox-2","generation":"g-2",)")
                  + R"("operation_id":"op-2","delay_ms":0,"deadline_unix_ms":)")
                  + std::to_string(FutureUnixMs(120000)) + "}");
    EXPECT_TRUE(gate.DropReceiptIfMatched("sbox-2", "g-2", "op-2"));
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-2", "g-2", "op-2"));  // consumed again
    const auto state = gate.GetStateForTest();
    EXPECT_TRUE(state.enabled);
    EXPECT_EQ(state.action, "drop_receipt");
    EXPECT_EQ(state.sandboxID, "sbox-2");
    EXPECT_EQ(state.operationID, "op-2");
    control.remove();
}

TEST(RetireFaultGateTest, ControlFileConfirmsPrepareStopPoint)
{
    // The full stage-4 flow against one live process: prepare via control
    // file, the message held at the stop-point, confirmation naming the
    // captured operation id, the fault applied to the HELD message.
    ControlFile control("gate-prep-confirm.json");
    control.remove();
    ControlFileEnv env(control.path);
    RetireFaultGate gate;
    control.write(PrepareBlockSpec(60000, FutureUnixMs(300000)));
    const auto held = gate.BlockDeleteIfMatched("sbox-1", "g-7", "fwd-random-42");
    EXPECT_EQ(held.decision, Decision::MatchPrepare);
    EXPECT_EQ(held.holdMs, 60000);
    control.write(ConfirmBlockSpec("fwd-random-42", FutureUnixMs(300000)));
    const auto resolved = gate.ResolveBlock("sbox-1", "g-7", "fwd-random-42");
    EXPECT_EQ(resolved.decision, Decision::MatchExact);
    EXPECT_EQ(resolved.delayMs, 7000);
    control.remove();
}

TEST(RetireFaultGateTest, IdenticalControlContentNeverRefires)
{
    ControlFile control("gate-same-content.json");
    control.remove();
    ControlFileEnv env(control.path);
    RetireFaultGate gate;
    const std::string spec = BlockSpec(5000, FutureUnixMs(120000));
    control.write(spec);
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision,
              Decision::MatchExact);
    // Rewriting byte-identical content must NOT re-arm: the caller's retry
    // after the hit has to pass through (one-shot semantics preserved).
    control.write(spec);
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision, Decision::NoMatch);
    // A CHANGED file (bumped control_revision) is the explicit operator
    // intent to fire the same identity again.
    control.write(std::string(R"({"action":"block_delete","sandbox":"sbox-1","generation":"g-7",)")
                  + R"("operation_id":"op-1","control_revision":2,"delay_ms":5000,)"
                  + R"("deadline_unix_ms":)" + std::to_string(FutureUnixMs(120000)) + "}");
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision,
              Decision::MatchExact);
    control.remove();
}

TEST(RetireFaultGateTest, RapidArmDisarmArmAllApplyByContent)
{
    // Same-millisecond changes are the reason dedup is content-based: an
    // arm, a disarm and a re-arm written back-to-back must each apply.
    ControlFile control("gate-rapid.json");
    control.remove();
    ControlFileEnv env(control.path);
    RetireFaultGate gate;
    control.write(BlockSpec(5000, FutureUnixMs(120000)));
    EXPECT_EQ(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1").decision,
              Decision::MatchExact);  // fires
    control.write(R"({"disarm":true})");
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-9", "g-1", "op-4"));  // refresh applies disarm
    EXPECT_FALSE(gate.GetStateForTest().enabled);
    control.write(BlockSpec(6000, FutureUnixMs(120000)));
    const auto again = gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1");
    EXPECT_EQ(again.decision, Decision::MatchExact);
    EXPECT_EQ(again.delayMs, 6000);
    control.remove();
}

TEST(RetireFaultGateTest, MissingControlFileLeavesArmedGateUntouched)
{
    ControlFile control("gate-absent.json");
    control.remove();
    ControlFileEnv env(control.path);
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(7000, FutureUnixMs(60000)));
    const auto hit = gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1");
    EXPECT_EQ(hit.decision, Decision::MatchExact);
    EXPECT_EQ(hit.delayMs, 7000);  // absent control file never disarms or mutates
    control.remove();
}

}  // namespace functionsystem::test
