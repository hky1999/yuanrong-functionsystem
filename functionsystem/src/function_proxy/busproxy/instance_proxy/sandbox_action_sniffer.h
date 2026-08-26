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

#ifndef FUNCTION_PROXY_BUSPROXY_INSTANCE_PROXY_SANDBOX_ACTION_SNIFFER_H
#define FUNCTION_PROXY_BUSPROXY_INSTANCE_PROXY_SANDBOX_ACTION_SNIFFER_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace functionsystem::busproxy {

/**
 * Best-effort extraction of the sandbox data-plane action name from the first
 * protobuf arg of an Invoke request.
 *
 * The frontend packs a msgpack envelope
 *   {"sandbox_method": "sandbox_invoke", "action": <name>, "args": {...}}
 * into arg[0] after a 16-byte libruntime inline-value header (zeroed except
 * the little-endian payload length at offset 8). Only the top-level "action"
 * key is of interest, so the scan is limited to a small prefix: the envelope
 * keys are short fixstr tokens; a match deeper than the limit (or a malformed
 * token) simply yields an empty action, which callers treat as "unknown".
 */
constexpr size_t LIBRUNTIME_HEADER_SIZE = 16;
constexpr size_t SANDBOX_ACTION_SNIFF_LIMIT = 128;

/**
 * Decode one msgpack string token (fixstr / str8 / str16) starting at pos.
 * Advances pos past the token. Returns "" for non-string or malformed tokens.
 */
inline std::string DecodeMsgpackString(const std::string &buf, size_t &pos)
{
    if (pos >= buf.size()) {
        return "";
    }
    const unsigned char c = static_cast<unsigned char>(buf[pos]);
    size_t len = 0;
    size_t headerLen = 1;
    if ((c & 0xe0) == 0xa0) {  // fixstr: len in [0, 31]
        len = c & 0x1f;
    } else if (c == 0xd9) {  // str8
        headerLen = 2;
        if (pos + 1 >= buf.size()) {
            return "";
        }
        len = static_cast<unsigned char>(buf[pos + 1]);
    } else if (c == 0xda) {  // str16
        headerLen = 3;
        if (pos + 2 >= buf.size()) {
            return "";
        }
        len = (static_cast<unsigned char>(buf[pos + 1]) << 8) | static_cast<unsigned char>(buf[pos + 2]);
    } else {
        return "";
    }
    if (pos + headerLen + len > buf.size()) {
        return "";
    }
    std::string out = buf.substr(pos + headerLen, len);
    pos += headerLen + len;
    return out;
}

/**
 * Sniff the sandbox action name from the first arg's payload.
 * Returns "" when no recognizable envelope is present.
 */
inline std::string SniffEnvelopeAction(const std::string &argValue)
{
    const size_t bodyStart = argValue.size() > LIBRUNTIME_HEADER_SIZE ? LIBRUNTIME_HEADER_SIZE : 0;
    const size_t scanEnd = argValue.size() < bodyStart + SANDBOX_ACTION_SNIFF_LIMIT
                               ? argValue.size()
                               : bodyStart + SANDBOX_ACTION_SNIFF_LIMIT;
    // fixstr "action" token: 0xa6 + 6 ASCII bytes.
    for (size_t i = bodyStart; i + 7 <= scanEnd; i++) {
        if (static_cast<unsigned char>(argValue[i]) == 0xa6 &&
            std::memcmp(argValue.data() + i + 1, "action", 6) == 0) {
            size_t pos = i + 7;
            return DecodeMsgpackString(argValue, pos);
        }
    }
    return "";
}

/**
 * Whether an action name represents real client usage of the instance.
 *
 * The SDK's create/readiness flow polls "ping" and "get_info" — including
 * for creates the client has already abandoned — so those lifecycle probes
 * must not promote an instance out of the orphan grace window. Every other
 * known action (cmd_* exec, fs_* filesystem, pty, tunnel server) is issued
 * only by an interested client and claims ownership.
 */
inline bool IsClientUsageAction(const std::string &action)
{
    return !action.empty() && action != "ping" && action != "get_info";
}

// --- minimal protobuf wire-format primitives --------------------------------
// Enough to pull MetaData.functionMeta(2).functionName(3) out of an invoke
// arg without depending on generated protos in this header's users.

inline bool PbReadVarint(const std::string &buf, size_t &pos, uint64_t &out)
{
    out = 0;
    for (int shift = 0; pos < buf.size() && shift < 64; shift += 7) {
        const unsigned char c = static_cast<unsigned char>(buf[pos]);
        ++pos;
        out |= static_cast<uint64_t>(c & 0x7f) << shift;
        if ((c & 0x80) == 0) {
            return true;
        }
    }
    return false;
}

inline bool PbSkipField(const std::string &buf, size_t &pos, uint64_t wireType)
{
    switch (wireType) {
        case 0: {
            uint64_t ignored = 0;
            return PbReadVarint(buf, pos, ignored);
        }
        case 1:
            if (buf.size() - pos < 8) {
                return false;
            }
            pos += 8;
            return true;
        case 5:
            if (buf.size() - pos < 4) {
                return false;
            }
            pos += 4;
            return true;
        case 2: {
            uint64_t len = 0;
            if (!PbReadVarint(buf, pos, len) || len > buf.size() - pos) {
                return false;
            }
            pos += static_cast<size_t>(len);
            return true;
        }
        default:
            return false;  // start/end-group (deprecated) or unknown wire type
    }
}

/**
 * Walk a well-formed protobuf buffer and return the payload of the first
 * LEN-delimited field with the given number. Returns false when the buffer
 * is not a valid wire message (fast exit on the first malformed tag).
 */
inline bool PbFindBytesField(const std::string &buf, int wantedField, std::string &out)
{
    size_t pos = 0;
    while (pos < buf.size()) {
        uint64_t tag = 0;
        if (!PbReadVarint(buf, pos, tag)) {
            return false;
        }
        const auto field = static_cast<int>(tag >> 3);
        const auto wireType = static_cast<uint64_t>(tag & 7);
        if (field <= 0) {
            return false;
        }
        if (wireType == 2) {
            uint64_t len = 0;
            if (!PbReadVarint(buf, pos, len) || len > buf.size() - pos) {
                return false;
            }
            if (field == wantedField) {
                out = buf.substr(pos, static_cast<size_t>(len));
                return true;
            }
            pos += static_cast<size_t>(len);
            continue;
        }
        if (!PbSkipField(buf, pos, wireType)) {
            return false;
        }
    }
    return pos == buf.size();
}

/**
 * Extract the yr RPC method name from a serialized libruntime MetaData
 * (libruntime.proto: MetaData.functionMeta(2) -> FunctionMeta.functionName(3)).
 * Returns "" when arg[0] is not a MetaData protobuf.
 */
inline std::string SniffMetadataFunctionName(const std::string &argValue)
{
    std::string funcMeta;
    if (!PbFindBytesField(argValue, 2, funcMeta)) {
        return "";
    }
    std::string name;
    if (!PbFindBytesField(funcMeta, 3, name)) {
        return "";
    }
    return name;
}

/**
 * Resolve the sandbox data-plane action from a Call request.
 *
 * Three shapes reach RequestDispatcher::Call (verified live on the w2
 * standalone, instances 2e74cb1a / 5dbc9436, 2026-08-22):
 *   - akernel_sdk (the harness path): function is the service ID and arg[0]
 *     is a serialized libruntime MetaData protobuf whose
 *     functionMeta(2).functionName(3) IS the _SandboxInstance method
 *     ("cmd_run", "fs_read", "ping", ...); the cloudpickled payload rides
 *     the later args.
 *   - frontend sandbox v1 API: function is the service ID
 *     ("default/0-defaultservice-rrt/$latest") and the action lives in a
 *     msgpack envelope in the first arg (16-byte libruntime header prefix).
 *   - a bare function name (no '/') is taken as the action directly.
 * The MetaData walk is tried first: it is a strict wire parse that fails
 * fast on the zeroed header of the envelope shape, so the two arg formats
 * cannot be confused.
 */
inline std::string SniffSandboxAction(const std::string &functionName, const std::string &firstArgValue)
{
    if (!functionName.empty() && functionName.find('/') == std::string::npos) {
        return functionName;
    }
    auto fromMeta = SniffMetadataFunctionName(firstArgValue);
    if (!fromMeta.empty()) {
        return fromMeta;
    }
    return SniffEnvelopeAction(firstArgValue);
}

}  // namespace functionsystem::busproxy

#endif  // FUNCTION_PROXY_BUSPROXY_INSTANCE_PROXY_SANDBOX_ACTION_SNIFFER_H
