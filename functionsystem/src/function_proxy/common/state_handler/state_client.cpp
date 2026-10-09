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

#include "state_client.h"

#include "async/uuid_generator.hpp"
#include "common/logs/logging.h"

namespace functionsystem {

StateClient::StateClient(const std::shared_ptr<DistributedCacheClient> &distributedCacheClient)
    : distributedCacheClient_(distributedCacheClient), isInited_(false)
{
}

namespace {
constexpr const char *STATE_KEY_SCHEME = "state:v1:";
constexpr const char *SNAPSHOT_KEY_SCHEME = "state:snap:v1:";

int HexDigitValue(char c)
{
    if (c >= '0' && c <= '9') { return c - '0'; }
    if (c >= 'A' && c <= 'F') { return c - 'A' + 10; }
    if (c >= 'a' && c <= 'f') { return c - 'a' + 10; }
    return -1;
}

// A snapshot-key segment is STRICTLY canonical — validated by the
// round-trip EscapeKeyPart(UnescapeKeyPart(raw)) == raw at the
// ParseSnapshotKey call site: one check rejects malformed escapes AND
// non-canonical aliases (e.g. lowercase %3a where EscapeKeyPart emits
// uppercase %3A).

// Canonical textual UUID exactly as litebus::uuid_generator::UUID::
// ToString emits (16 bytes hex, hyphens after bytes 3/5/7/9):
// 36 chars, hyphens at 8/13/18/23, hex digits elsewhere.
bool IsCanonicalUuid(const std::string &s)
{
    if (s.size() != 36) { return false; }
    for (size_t i = 0; i < s.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (s[i] != '-') { return false; }
            continue;
        }
        if (HexDigitValue(s[i]) < 0) { return false; }
    }
    return true;
}
}  // namespace

std::string StateClient::EscapeKeyPart(const std::string &in)
{
    std::string out;
    out.reserve(in.size());
    for (char c : in) {
        if (c == '%' || c == '@' || c == ':') {
            static const char *hex = "0123456789ABCDEF";
            out.push_back('%');
            out.push_back(hex[(static_cast<unsigned char>(c)) >> 4]);
            out.push_back(hex[(static_cast<unsigned char>(c)) & 0xF]);
        } else {
            out.push_back(c);
        }
    }
    return out;
}

std::string StateClient::UnescapeKeyPart(const std::string &in)
{
    std::string out;
    out.reserve(in.size());
    for (size_t i = 0; i < in.size(); ++i) {
        if (in[i] == '%' && i + 2 < in.size()) {
            const int hi = HexDigitValue(in[i + 1]);
            const int lo = HexDigitValue(in[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>((hi << 4) | lo));
                i += 2;
                continue;
            }
        }
        out.push_back(in[i]);
    }
    return out;
}

std::string StateClient::ComposeStateKey(const std::string &instanceId, const std::string &generation)
{
    return std::string(STATE_KEY_SCHEME) + EscapeKeyPart(instanceId) + "@" + EscapeKeyPart(generation);
}

std::pair<std::string, std::string> StateClient::ParseStateKey(const std::string &key)
{
    const size_t schemeLen = std::char_traits<char>::length(STATE_KEY_SCHEME);
    if (key.rfind(STATE_KEY_SCHEME, 0) != 0) {
        return { "", "" };
    }
    // Find the UNESCAPED separator: an '@' not produced by an escape. Since
    // '%' and '@' are escaped inside parts, the first literal '@' after the
    // scheme is the separator.
    const size_t sep = key.find('@', schemeLen);
    if (sep == std::string::npos) {
        return { "", "" };
    }
    const std::string instancePart = UnescapeKeyPart(key.substr(schemeLen, sep - schemeLen));
    const std::string generationPart = UnescapeKeyPart(key.substr(sep + 1));
    if (instancePart.empty() || generationPart.empty()) {
        return { "", "" };
    }
    return { instancePart, generationPart };
}

std::string StateClient::ComposeSnapshotKey(const std::string &instanceId,
                                            const std::string &creationRequest)
{
    // A fresh UUID on EVERY call makes the key globally unique per save
    // operation — the physical isolation invariant (v3 §1.1).
    const auto uuid = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    return std::string(SNAPSHOT_KEY_SCHEME) + EscapeKeyPart(instanceId) + ":"
        + EscapeKeyPart(creationRequest) + ":" + uuid;
}

StateClient::SnapshotKeyParts StateClient::ParseSnapshotKey(const std::string &key)
{
    SnapshotKeyParts out;
    if (key.rfind(SNAPSHOT_KEY_SCHEME, 0) != 0) {
        return out;
    }
    const size_t schemeLen = std::char_traits<char>::length(SNAPSHOT_KEY_SCHEME);
    const std::string body = key.substr(schemeLen);
    // Escaped segments can never contain a literal ':' — a valid key has
    // EXACTLY two separators (three segments), no more, no fewer.
    const size_t sep1 = body.find(':');
    if (sep1 == std::string::npos) { return out; }
    const size_t sep2 = body.find(':', sep1 + 1);
    if (sep2 == std::string::npos) { return out; }
    if (body.find(':', sep2 + 1) != std::string::npos) { return out; }

    const std::string rawInstance = body.substr(0, sep1);
    const std::string rawRequest = body.substr(sep1 + 1, sep2 - sep1 - 1);
    const std::string rawOp = body.substr(sep2 + 1);
    if (rawInstance.empty() || rawRequest.empty()) { return out; }
    // CANONICAL-BYTES check (encoding-alias boundary): a key that merely
    // DECODES to the right identity is NOT acceptable — the physical
    // bytes must be EXACTLY what ComposeSnapshotKey produces. EscapeKeyPart
    // emits UPPERCASE %XX, so an alias like %3a (decoding to the same ':')
    // fails the round-trip: a corrupted registration can never delete a
    // DIFFERENT physical DS key and report CLEANED. The round-trip also
    // subsumes well-formedness (any malformed escape re-escapes unequal).
    if (EscapeKeyPart(UnescapeKeyPart(rawInstance)) != rawInstance
        || EscapeKeyPart(UnescapeKeyPart(rawRequest)) != rawRequest) {
        return out;
    }
    // UUID: the generator (litebus ToString via std::hex) emits lowercase;
    // both hex cases remain accepted — not over-rejected.
    if (!IsCanonicalUuid(rawOp)) { return out; }
    out.instanceId = UnescapeKeyPart(rawInstance);
    out.creationRequest = UnescapeKeyPart(rawRequest);
    out.saveOpID = rawOp;
    return out;
}

bool StateClient::IsSnapshotKey(const std::string &key)
{
    return !ParseSnapshotKey(key).saveOpID.empty();
}

Status StateClient::Init()
{
    RETURN_STATUS_IF_NULL(distributedCacheClient_, StatusCode::FAILED, "distributedCacheClient is nullptr");
    return distributedCacheClient_->Init();
}

Status StateClient::Set(const std::string &key, const std::string &state)
{
    RETURN_STATUS_IF_NULL(distributedCacheClient_, StatusCode::FAILED, "distributedCacheClient is nullptr");
    return distributedCacheClient_->Set(key, state);
}

Status StateClient::Get(const std::string &key, std::string &state)
{
    RETURN_STATUS_IF_NULL(distributedCacheClient_, StatusCode::FAILED, "distributedCacheClient is nullptr");
    return distributedCacheClient_->Get(key, state);
}

Status StateClient::Del(const std::string &key)
{
    RETURN_STATUS_IF_NULL(distributedCacheClient_, StatusCode::FAILED, "distributedCacheClient is nullptr");
    return distributedCacheClient_->Del(key);
}

}  // namespace functionsystem