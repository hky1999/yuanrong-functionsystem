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

#include "function_proxy/busproxy/instance_proxy/sandbox_action_sniffer.h"

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

namespace functionsystem::test {

using namespace busproxy;

namespace {

// fixstr token: 0xa0|len followed by the raw bytes.
std::string FixStr(const std::string &s)
{
    return static_cast<char>(0xa0 | s.size()) + s;
}

// str8 token (for names that still fit fixstr but exercise the 0xd9 branch).
std::string Str8(const std::string &s)
{
    return std::string("\xd9", 1) + static_cast<char>(s.size()) + s;
}

// 16-byte libruntime inline-value header (zeros; the sniffer only skips it).
std::string Header()
{
    return std::string(16, '\0');
}

// Envelope as packed by the frontend:
// {"sandbox_method": "sandbox_invoke", "action": <action>, "args": {...}}
std::string Envelope(const std::string &actionToken)
{
    std::string msg;
    msg += static_cast<char>(0x83);                      // fixmap(3)
    msg += FixStr("sandbox_method") + FixStr("sandbox_invoke");
    msg += FixStr("action") + actionToken;
    msg += FixStr("args") + static_cast<char>(0x81)      // fixmap(1)
           + FixStr("cmd") + FixStr("echo hi");
    return Header() + msg;
}

// Serialized libruntime MetaData as the akernel_sdk path packs it into
// arg[0]: MetaData.functionMeta(2) -> FunctionMeta.functionName(3).
std::string MetaDataProto(const std::string &action)
{
    // FunctionMeta: field 3 (functionName), wire type 2: tag 0x1a
    std::string funcMeta = std::string("\x1a", 1);
    funcMeta += static_cast<char>(action.size());
    funcMeta += action;
    // MetaData: field 2 (functionMeta), wire type 2: tag 0x12
    std::string meta = std::string("\x12", 1);
    meta += static_cast<char>(funcMeta.size());
    meta += funcMeta;
    return meta;
}

}  // namespace

/**
 * Feature: the sandbox action name is sniffed from the msgpack envelope in
 * the first invoke arg (past the 16-byte libruntime header) when the function
 * field carries a service ID (frontend sandbox v1 path).
 * Expectation: exec/filesystem/pty-class actions are recognized.
 */
TEST(SandboxActionSnifferTest, SniffsActionFromEnvelope)
{
    const std::string funcID = "default/0-defaultservice-rrt/$latest";
    EXPECT_EQ(SniffSandboxAction(funcID, Envelope(FixStr("cmd_run"))), "cmd_run");
    EXPECT_EQ(SniffSandboxAction(funcID, Envelope(FixStr("cmd_start"))), "cmd_start");
    EXPECT_EQ(SniffSandboxAction(funcID, Envelope(FixStr("fs_write"))), "fs_write");
    EXPECT_EQ(SniffSandboxAction(funcID, Envelope(FixStr("ping"))), "ping");
    EXPECT_EQ(SniffSandboxAction(funcID, Envelope(FixStr("get_info"))), "get_info");
}

/**
 * Feature: akernel_sdk invokes carry the action in arg[0] as a serialized
 * libruntime MetaData protobuf (functionMeta(2).functionName(3) — verified
 * live via invoke_adaptor's ParseMetaData and the runtime log
 * "func name: cmd_run"); the wire walk must extract it while the create-flow
 * probes ping/get_info stay distinguishable.
 */
TEST(SandboxActionSnifferTest, MetadataArgYieldsAction)
{
    const std::string funcID = "default/0-defaultservice-py313/$latest";
    EXPECT_EQ(SniffSandboxAction(funcID, MetaDataProto("cmd_run")), "cmd_run");
    EXPECT_EQ(SniffSandboxAction(funcID, MetaDataProto("fs_read")), "fs_read");
    EXPECT_EQ(SniffSandboxAction(funcID, MetaDataProto("start_tunnel_server")), "start_tunnel_server");
    EXPECT_EQ(SniffSandboxAction(funcID, MetaDataProto("ping")), "ping");
    EXPECT_EQ(SniffSandboxAction(funcID, MetaDataProto("get_info")), "get_info");
}

/**
 * Feature: a MetaData protobuf with the functionName inside other
 * surrounding fields (varint + bytes fields before functionMeta) still
 * parses; garbage that is not a wire message yields "".
 */
TEST(SandboxActionSnifferTest, MetadataWireWalkRobustness)
{
    const std::string funcID = "default/0-defaultservice-py313/$latest";
    // invokeType(1) varint = 2, then functionMeta(2)
    std::string withPrefix = std::string("\x08\x02", 2) + MetaDataProto("cmd_run");
    EXPECT_EQ(SniffSandboxAction(funcID, withPrefix), "cmd_run");
    // cloudpickle-ish garbage: no valid wire tag at offset 0
    EXPECT_EQ(SniffSandboxAction(funcID, std::string("\x80\x05\x95", 3) + std::string(64, '\x80')), "");
    // truncated field header
    EXPECT_EQ(SniffSandboxAction(funcID, "\x12"), "");
    // field 2 declared longer than the buffer
    EXPECT_EQ(SniffSandboxAction(funcID, "\x12\xff\x01abc"), "");
}

/**
 * Feature: a bare RPC function name (no '/') wins without touching args.
 */
TEST(SandboxActionSnifferTest, BareFunctionNameIsTheAction)
{
    EXPECT_EQ(SniffSandboxAction("cmd_run", "anything"), "cmd_run");
    EXPECT_EQ(SniffSandboxAction("fs_read", ""), "fs_read");
    EXPECT_EQ(SniffSandboxAction("ping", ""), "ping");
}

/**
 * Feature: str8-encoded action names decode through the 0xd9 branch.
 */
TEST(SandboxActionSnifferTest, DecodesStr8Action)
{
    const std::string funcID = "default/0-defaultservice-rrt/$latest";
    EXPECT_EQ(SniffSandboxAction(funcID, Envelope(Str8("cmd_run"))), "cmd_run");
}

/**
 * Feature: payloads without a recognizable envelope yield an empty action
 * instead of garbage.
 */
TEST(SandboxActionSnifferTest, NoEnvelopeYieldsEmpty)
{
    EXPECT_EQ(SniffSandboxAction("default/0-defaultservice-rrt/$latest", ""), "");
    EXPECT_EQ(SniffSandboxAction("default/0-defaultservice-rrt/$latest", Header()), "");
    EXPECT_EQ(SniffSandboxAction("default/0-defaultservice-rrt/$latest", "plain bytes, no msgpack"), "");
    // "action" appearing as a plain substring (not a msgpack key) must not match
    EXPECT_EQ(SniffSandboxAction("default/0-defaultservice-rrt/$latest", Header() + "the word action inside text"),
              "");
    EXPECT_EQ(SniffSandboxAction("", ""), "");
}

/**
 * Feature: only lifecycle probes are excluded from usage classification —
 * the create flow polls ping/get_info even for abandoned creates, so they
 * must not claim ownership; everything else does.
 */
TEST(SandboxActionSnifferTest, ClientUsageClassification)
{
    EXPECT_TRUE(IsClientUsageAction("cmd_run"));
    EXPECT_TRUE(IsClientUsageAction("cmd_wait"));
    EXPECT_TRUE(IsClientUsageAction("fs_read"));
    EXPECT_TRUE(IsClientUsageAction("start_tunnel_server"));
    EXPECT_FALSE(IsClientUsageAction("ping"));
    EXPECT_FALSE(IsClientUsageAction("get_info"));
    EXPECT_FALSE(IsClientUsageAction(""));
}

}  // namespace functionsystem::test
