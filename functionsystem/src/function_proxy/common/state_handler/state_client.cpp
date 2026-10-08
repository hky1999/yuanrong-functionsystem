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

#include "common/logs/logging.h"

namespace functionsystem {

StateClient::StateClient(const std::shared_ptr<DistributedCacheClient> &distributedCacheClient)
    : distributedCacheClient_(distributedCacheClient), isInited_(false)
{
}

namespace {
std::string EscapeKeyPart(const std::string &in)
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

std::string UnescapeKeyPart(const std::string &in)
{
    std::string out;
    out.reserve(in.size());
    for (size_t i = 0; i < in.size(); ++i) {
        if (in[i] == '%' && i + 2 < in.size()) {
            const auto hexVal = [](char c) -> int {
                if (c >= '0' && c <= '9') { return c - '0'; }
                if (c >= 'A' && c <= 'F') { return c - 'A' + 10; }
                if (c >= 'a' && c <= 'f') { return c - 'a' + 10; }
                return -1;
            };
            const int hi = hexVal(in[i + 1]);
            const int lo = hexVal(in[i + 2]);
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

constexpr const char *STATE_KEY_SCHEME = "state:v1:";
}  // namespace

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