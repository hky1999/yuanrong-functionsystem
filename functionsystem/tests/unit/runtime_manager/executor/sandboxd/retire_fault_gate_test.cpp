/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "runtime_manager/executor/sandboxd/retire_fault_gate.h"
#include <gtest/gtest.h>
#include <chrono>

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
    return std::string(R"({"action":"block_delete","sandbox":"sbox-1","generation":"g-7","delay_ms":)")
        + std::to_string(delayMs) + R"(,"deadline_unix_ms":)" + std::to_string(deadlineUnixMs) + "}";
}
}  // namespace

TEST(RetireFaultGateTest, DisabledGateNeverMatches)
{
    RetireFaultGate gate;  // no ArmFromSpec: default off
    int64_t delayMs = -1;
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-7", delayMs));
    EXPECT_EQ(delayMs, 0);
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-1", "g-7"));
}

TEST(RetireFaultGateTest, BlockDeleteMatchesExactIdentityOnce)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(45000, FutureUnixMs(120000)));
    int64_t delayMs = 0;
    // Neighbour identities and the other action never consume the one shot.
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-1", "g-7"));
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-2", "g-7", delayMs));
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-8", delayMs));
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", delayMs));
    EXPECT_EQ(delayMs, 45000);
    // One-shot: the retried delete must pass through untouched.
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-1", "g-7", delayMs));
}

TEST(RetireFaultGateTest, DropReceiptMatchesOnceAndBlockDoesNot)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"sbox-9","generation":"g-1","delay_ms":0,"deadline_unix_ms":)"
        + std::to_string(FutureUnixMs(60000)) + "}");
    int64_t delayMs = 0;
    EXPECT_FALSE(gate.BlockDeleteIfMatched("sbox-9", "g-1", delayMs));
    EXPECT_TRUE(gate.DropReceiptIfMatched("sbox-9", "g-1"));
    EXPECT_FALSE(gate.DropReceiptIfMatched("sbox-9", "g-1"));
}

TEST(RetireFaultGateTest, ExpiredOrInvalidSpecsStayInactive)
{
    RetireFaultGate expired;
    expired.ArmFromSpec(BlockSpec(1000, FutureUnixMs(-1000)));
    int64_t delayMs = 0;
    EXPECT_FALSE(expired.BlockDeleteIfMatched("sbox-1", "g-7", delayMs));

    RetireFaultGate garbage;
    garbage.ArmFromSpec("not-json");
    EXPECT_FALSE(garbage.BlockDeleteIfMatched("sbox-1", "g-7", delayMs));

    RetireFaultGate unknownAction;
    unknownAction.ArmFromSpec(R"({"action":"reboot_world","sandbox":"s","generation":"g","delay_ms":1,)"
        R"("deadline_unix_ms":9223372036854775807})");
    EXPECT_FALSE(unknownAction.DropReceiptIfMatched("s", "g"));

    RetireFaultGate partial;
    partial.ArmFromSpec(R"({"action":"drop_receipt","sandbox":"","generation":"g"})");
    EXPECT_FALSE(partial.DropReceiptIfMatched("", "g"));
}

TEST(RetireFaultGateTest, RearmWhileActiveIsRejected)
{
    RetireFaultGate gate;
    gate.ArmFromSpec(BlockSpec(1000, FutureUnixMs(60000)));
    gate.ArmFromSpec(BlockSpec(2000, FutureUnixMs(60000)));
    int64_t delayMs = 0;
    EXPECT_TRUE(gate.BlockDeleteIfMatched("sbox-1", "g-7", delayMs));
    EXPECT_EQ(delayMs, 1000);  // first spec wins, not the ignored re-arm
}

}  // namespace functionsystem::test
