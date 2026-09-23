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

TEST(RetireFaultGateTest, DisabledGateNeverMatches)
{
    RetireFaultGate gate;  // no ArmFromSpec: default off
    int64_t delayMs = -1;
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    EXPECT_EQ(delayMs, 0);
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-1", "g-7", "op-1"));
}

TEST(RetireFaultGateTest, BlockDeleteMatchesExactIdentityOnce)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(45000, FutureUnixMs(120000)));
    int64_t delayMs = 0;
    // Neighbour identities, the other action, and a WRONG OPERATION never
    // consume the one shot.
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-1", "g-7", "op-1"));
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-2", "g-7", "op-1", delayMs));
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-8", "op-1", delayMs));
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-9", delayMs));
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    EXPECT_EQ(delayMs, 45000);
    // One-shot: the retried delete must pass through untouched.
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
}

TEST(RetireFaultGateTest, DropReceiptMatchesOnceAndBlockDoesNot)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"sbox-9","generation":"g-1",)"
                     R"("operation_id":"op-4","delay_ms":0,"deadline_unix_ms":)"
                     + std::to_string(FutureUnixMs(60000)) + "}");
    int64_t delayMs = 0;
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-9", "g-1", "op-4", delayMs));
    EXPECT_TRUE(gate.DropReceiptIfMatched("sbox-9", "g-1", "op-4"));
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-9", "g-1", "op-4"));
}

TEST(RetireFaultGateTest, ExpiredOrInvalidSpecsStayInactive)
{
    RetireFaultGate expired;
    expired.ArmFromSpec(BlockSpec(1000, FutureUnixMs(-1000)));
    int64_t delayMs = 0;
    EXPECT_FALSE(expired.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));

    RetireFaultGate garbage;
    garbage.ArmFromSpec("not-json");
    EXPECT_FALSE(garbage.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));

    RetireFaultGate unknownAction;
    unknownAction.ArmFromSpec(R"({"action":"reboot_world","sandbox":"s","generation":"g",)"
                              R"("operation_id":"o","delay_ms":1,)"
                              R"("deadline_unix_ms":9223372036854775807})");
    EXPECT_FALSE(unknownAction.DropReceiptIfMatched("s", "g", "o"));

    RetireFaultGate partial;
    partial.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"","generation":"g",)"
                        R"("operation_id":"o"})");
    EXPECT_FALSE(partial.DropReceiptIfMatched("", "g", "o"));

    // Missing operation id is an incomplete operation identity: never armed.
    RetireFaultGate noOperation;
    noOperation.ArmFromSpec(R"({"action":"block_delete","sandbox":"s","generation":"g",)"
                            R"("delay_ms":1,"deadline_unix_ms":9223372036854775807})");
    EXPECT_FALSE(noOperation.BlockDeleteIfMatched("s", "g", "", delayMs));
    EXPECT_FALSE(noOperation.GetStateForTest().enabled);
}

TEST(RetireFaultGateTest, RearmWhileActiveIsRejected)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(1000, FutureUnixMs(60000)));
    gate.ArmFromSpec(BlockSpec(2000, FutureUnixMs(60000)));
    int64_t delayMs = 0;
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    EXPECT_EQ(delayMs, 1000);  // first spec wins, not the ignored re-arm
}

TEST(RetireFaultGateTest, RearmAfterConsumedShotIsAccepted)
{
    RetireFaultGate gate;
    int64_t delayMs = 0;
    gate.ArmFromSpec(BlockSpec(1000, FutureUnixMs(60000)));
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));  // spent
    // A consumed one-shot accepts a fresh spec (fault matrix: 3 repeats in
    // one live proxy process); the new parameters fire normally.
    gate.ArmFromSpec(BlockSpec(2000, FutureUnixMs(60000)));
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    EXPECT_EQ(delayMs, 2000);
    const auto state = gate.GetStateForTest();
    EXPECT_TRUE(state.enabled);
    EXPECT_TRUE(state.consumed);
}

TEST(RetireFaultGateTest, StateSnapshotExposesOperationIdentity)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(500, FutureUnixMs(60000)));
    const auto state = gate.GetStateForTest();  // const read must compile+lock
    EXPECT_TRUE(state.enabled);
    EXPECT_FALSE(state.consumed);
    EXPECT_EQ(state.action, "block_delete");
    EXPECT_EQ(state.sandboxID, "sbox-1");
    EXPECT_EQ(state.generation, "g-7");
    EXPECT_EQ(state.operationID, "op-1");
    EXPECT_EQ(state.delayMs, 500);
}

TEST(RetireFaultGateTest, ControlFileArmsAndFiresWithoutRestart)
{
    ControlFile control("gate-arm.json");
    control.remove();
    ControlFileEnv env(control.path);
    RetireFaultGate gate;
    int64_t delayMs = 0;
    // No control file yet: gate stays off and nothing matches.
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    control.write(BlockSpec(30000, FutureUnixMs(120000)));
    // The next match attempt rereads the file, arms, and consumes in the
    // same evaluation: stage-4 fault injection without a process restart.
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    EXPECT_EQ(delayMs, 30000);
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
    int64_t delayMs = 0;
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
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
    int64_t delayMs = 0;
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    EXPECT_EQ(delayMs, 5000);
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));  // consumed
    // Second shot against a DIFFERENT identity: the fault matrix needs
    // repeated faults from one live proxy process.
    control.write(std::string(R"({"action":"drop_receipt","sandbox":"sbox-2","generation":"g-2",)")
                  + R"("operation_id":"op-2","delay_ms":0,"deadline_unix_ms":)"
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

TEST(RetireFaultGateTest, IdenticalControlContentNeverRefires)
{
    ControlFile control("gate-same-content.json");
    control.remove();
    ControlFileEnv env(control.path);
    RetireFaultGate gate;
    const std::string spec = BlockSpec(5000, FutureUnixMs(120000));
    control.write(spec);
    int64_t delayMs = 0;
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    // Rewriting byte-identical content must NOT re-arm: the caller's retry
    // after the hit has to pass through (one-shot semantics preserved).
    control.write(spec);
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    // A CHANGED file (bumped control_revision) is the explicit operator
    // intent to fire the same identity again.
    control.write(std::string(R"({"action":"block_delete","sandbox":"sbox-1","generation":"g-7",)")
                  + R"("operation_id":"op-1","control_revision":2,"delay_ms":5000,)"
                  + R"("deadline_unix_ms":)" + std::to_string(FutureUnixMs(120000)) + "}");
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
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
    int64_t delayMs = 0;
    control.write(BlockSpec(5000, FutureUnixMs(120000)));
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));  // fires
    control.write(R"({"disarm":true})");
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-9", "g-1", "op-4"));  // refresh applies disarm
    EXPECT_FALSE(gate.GetStateForTest().enabled);
    control.write(BlockSpec(6000, FutureUnixMs(120000)));
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    EXPECT_EQ(delayMs, 6000);
    control.remove();
}

TEST(RetireFaultGateTest, MissingControlFileLeavesArmedGateUntouched)
{
    ControlFile control("gate-absent.json");
    control.remove();
    ControlFileEnv env(control.path);
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(7000, FutureUnixMs(60000)));
    int64_t delayMs = 0;
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", "op-1", delayMs));
    EXPECT_EQ(delayMs, 7000);  // absent control file never disarms or mutates
    control.remove();
}

}  // namespace functionsystem::test
