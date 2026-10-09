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

#ifndef BUSPROXY_BUSINESS_STATE_ACTOR_STATE_CLIENT_H
#define BUSPROXY_BUSINESS_STATE_ACTOR_STATE_CLIENT_H

#include <memory>

#include "function_proxy/common/distribute_cache_client/distributed_cache_client.h"

namespace functionsystem {

class StateClient {
public:
    explicit StateClient(const std::shared_ptr<DistributedCacheClient> &distributedCacheClient);
    virtual ~StateClient() = default;

    virtual Status Init();

    // Generation-qualified state key (#S, plan v2r1): unambiguous escaped
    // encoding `state:v1:<esc(instanceId)>@<esc(generation)>` where esc
    // percent-escapes '%', '@', ':'. The generation is the CREATION request
    // id bound at the message source (posix stream accept / the proxy-
    // internal caller's instance record) — never re-resolved per message,
    // so a late OLD stream can only ever touch its OWN generation's key.
    static std::string ComposeStateKey(const std::string &instanceId, const std::string &generation);
    // Inverse of ComposeStateKey. `.first` empty ⇒ the reference is NOT a
    // composed key (bare legacy id or foreign string): callers must refuse
    // — the migration contract handles old data, never a silent fallback.
    static std::pair<std::string, std::string> ParseStateKey(const std::string &key);

    // Unambiguous key-segment escaping shared by EVERY composed key in the
    // state namespace (percent-escapes '%', '@', ':' so segments can never
    // contain a literal separator). Exposed so the registry-side keys
    // (stateref:v1:/statereg:v1:) use the SAME escaping as the DS-side
    // keys — one scheme, one ambiguity story.
    static std::string EscapeKeyPart(const std::string &in);
    static std::string UnescapeKeyPart(const std::string &in);

    // The methods below take the FULL physical key (composed by callers or
    // supplied as an explicit versioned checkpoint reference).
    virtual Status Set(const std::string &key, const std::string &state);

    virtual Status Get(const std::string &key, std::string &state);

    virtual Status Del(const std::string &key);

    // ── v3 snapshot primitives (registration-first isolation) ────────────
    // A snapshot key is globally unique per SAVE OPERATION:
    // `state:snap:v1:<esc(instance)>:<esc(creationRequest)>:<saveOpID>`.
    // Two saves — even same incarnation, same millisecond — produce
    // different keys; a late-arriving write can only land on its OWN key
    // (no overwrite target exists). The UUID is generated inside
    // ComposeSnapshotKey on every call.
    static std::string ComposeSnapshotKey(const std::string &instanceId,
                                           const std::string &creationRequest);
    // STRICT inverse of ComposeSnapshotKey: exact scheme, exactly two
    // unescaped ':' separators, strictly-escaped non-empty instance and
    // creation-request segments (every '%' escape decodes to '%', '@' or
    // ':'; no raw '@'/':'), and a CANONICAL UUID saveOpID (36 chars,
    // 8-4-4-4-12, hyphen positions fixed, hex digits elsewhere).
    // `.saveOpID` empty ⇒ NOT a valid snapshot key — a prefix match is
    // never acceptance. Callers verify the parsed identity segments
    // against the expected instance/generation before use.
    struct SnapshotKeyParts {
        std::string instanceId;
        std::string creationRequest;
        std::string saveOpID;
    };
    static SnapshotKeyParts ParseSnapshotKey(const std::string &key);
    // Namespace check, made STRICT: delegates to ParseSnapshotKey — a
    // bare scheme prefix is NOT sufficient.
    static bool IsSnapshotKey(const std::string &key);

private:
    std::shared_ptr<DistributedCacheClient> distributedCacheClient_;
    bool isInited_;
};

}  // namespace functionsystem

#endif  // BUSPROXY_BUSINESS_STATE_ACTOR_STATE_CLIENT_H
