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

#ifndef LOCAL_SCHEDULER_TRAEFIK_REGISTRY_H
#define LOCAL_SCHEDULER_TRAEFIK_REGISTRY_H

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <async/future.hpp>
#include "meta_storage_accessor/meta_storage_accessor.h"
#include "common/status/status.h"

namespace functionsystem::local_scheduler {

// Route registry with STORE-SIDE generation fencing (#T, plan v2r1 R4).
//
// The marker key <keyPrefix>/http/instances/<safeID>/generation is written
// IN THE SAME transaction as the route keys and carries BOTH the complete
// immutable ownership (instance/tenant/runtime/creation request) AND the
// complete own-key list. Retire = single etcd transaction conditioned on
// the marker's full captured value; a same-name NEW generation, a same-
// generation port change, or a concurrent refresh rewrites the marker and
// makes the stale transaction's compare fail at COMMIT TIME (linearized at
// the store — no proxy-side check-then-act window survives). A retry
// always re-compares the CAPTURED OWNERSHIP first and refuses to chase a
// marker that now belongs to a different generation. A MISSING marker is
// an unknown, never a legacy free pass.
class TraefikRegistry {
public:
    struct PortMapping {
        int sandboxPort;
        int hostPort;
        std::string protocol;  // "https" → HTTPS backend, others → HTTP backend
    };

    // Parsed marker payload. `raw` is the exact stored value the compare
    // must equal; `instanceRevision` is the DURABLE AUTHORITY ORDER — the
    // registering machine's etcd instance-record revision at register time.
    // Takeover of a foreign holder is permitted only for a caller whose own
    // authority revision is ≥ the holder's; a late OLD registration always
    // loses this comparison and fails.
    struct IncarnationMarker {
        std::string instanceID;
        std::string tenantID;
        std::string runtimeID;
        std::string requestID;
        uint64_t instanceRevision = 0;
        std::vector<std::string> ownKeys;  // sorted, every route key this incarnation wrote
        std::string raw;
        bool present = false;  // the marker key exists in the store
        bool valid = false;    // …and parsed into this struct
    };

    TraefikRegistry(std::shared_ptr<MetaStorageAccessor> accessor,
                    const std::string& keyPrefix,
                    const std::string& httpEntryPoint = "websecure",
                    bool enableTLS = true,
                    const std::string& serversTransport = "yr-backend-tls@file");
    ~TraefikRegistry() = default;

    // Legacy registration: plain put transaction, NO marker (pre-fencing
    // behavior; retained for callers without identity).
    litebus::Future<Status> RegisterInstance(
        const std::string& instanceID,
        const std::string& hostIP,
        const std::vector<PortMapping>& portMappings);

    // Identity-bearing registration. `authorityRevision` is the caller's
    // machine's etcd instance-record revision — the durable generation
    // order. One transaction that (under the captured marker value, or
    // marker-absence) deletes the previous holder's own-key list (same-
    // generation port removals included), writes the new route keys and
    // the new marker. Taking over a FOREIGN holder requires our authority
    // revision ≥ the holder's; a late old registration provably loses and
    // fails instead of overwriting the newer holder.
    litebus::Future<Status> RegisterInstance(
        const std::string& instanceID,
        const std::string& hostIP,
        const std::vector<PortMapping>& portMappings,
        const std::string& tenantID,
        const std::string& runtimeID,
        const std::string& requestID,
        uint64_t authorityRevision,
        const std::string& instanceRecordKey);

    // Legacy retire: unconditional EXACT-key deletion (enumerated with an
    // anchored segment match — never a bare safeID prefix, which would hit
    // inst-a's delete against inst-ab) plus marker removal.
    litebus::Future<Status> UnregisterInstance(const std::string& instanceID);

    // Fenced retire: requires the COMPLETE identity (instance + runtime +
    // creation request; tenant when the caller has it). Refuses when no
    // marker exists (unknown != legacy), when the marker names a different
    // instance (SanitizeID-collision guard), or when any ownership field
    // differs from the retiring chain's. On a match the single conditional
    // transaction deletes exactly the marker's own-key list and the marker
    // itself; a compare failure retries with a FRESH capture whose
    // ownership is re-compared first (bounded, never chases a foreign
    // marker).
    litebus::Future<Status> UnregisterInstance(const std::string& instanceID,
                                               const std::string& expectedTenantID,
                                               const std::string& expectedRuntimeID,
                                               const std::string& expectedRequestID);

    // Exposed for tests/diagnostics: parse a stored marker value.
    static IncarnationMarker ParseMarker(const std::string& raw);
    // Deterministic marker serialization (escaped fields, sorted keys).
    static std::string SerializeMarker(const std::string& instanceID,
                                       const std::string& tenantID,
                                       const std::string& runtimeID,
                                       const std::string& requestID,
                                       uint64_t instanceRevision,
                                       const std::vector<std::string>& ownKeys);

private:
    static std::string SanitizeID(const std::string& id);
    // Bounded retry driver for the fenced retire (attempt-capped).
    litebus::Future<Status> UnregisterInstanceFenced(const std::string& instanceID,
                                                     const std::string& expectedTenantID,
                                                     const std::string& expectedRuntimeID,
                                                     const std::string& expectedRequestID,
                                                     uint32_t attempt);
    static std::string EscapeField(const std::string& in);
    static std::string UnescapeField(const std::string& in);
    // ^<base>/(routers|services)/<safeID>-p[0-9]+(/|$) — anchored segment
    // match so inst-a never matches inst-ab, and a safeID that itself ends
    // in "-p<digits>" cannot be confused for another's port keys.
    static std::string BuildOwnKeyPattern(const std::string& httpBase, const std::string& safeID);
    static bool IsOwnKey(const std::string& pattern, const std::string& key);

    std::string MarkerKey(const std::string& safeID) const;
    // Read+parse the marker; `.valid=false` covers absent AND unparseable.
    IncarnationMarker ReadMarker(const std::string& instanceID);
    // Enumerate this instance's route keys with the anchored pattern.
    std::vector<std::string> EnumerateOwnKeys(const std::string& instanceID);
    // Full-ownership equality against the caller's identity (the marker's
    // instance field is part of it — a SanitizeID collision must not let
    // one instance retire another's route).
    static bool OwnershipMatches(const IncarnationMarker& marker,
                                 const std::string& instanceID,
                                 const std::string& tenantID,
                                 const std::string& runtimeID,
                                 const std::string& requestID);
    // A foreign holder may be taken over only by the SAME logical
    // instance+tenant whose durable etcd mod revision is strictly newer
    // than the holder's recorded one. Every capture re-decides with THIS
    // rule — a late old registration always fails it against a newer
    // holder; a SanitizeID-colliding stranger never passes it.
    static bool MayTakeOver(const IncarnationMarker& holder,
                            const std::string& instanceID,
                            const std::string& tenantID,
                            uint64_t ourRevision);
    // Single transaction: If(captured marker value / marker absent) Then(
    // delete oldKeys… ; put kvs… ; put marker). Each attempt is a SINGLE
    // read+decide+commit cycle: the commit is authorized by the CAPTURED
    // value plus the caller's authority revision, never by a pre-check.
    litebus::Future<Status> RegisterWithMarker(const std::string& instanceID,
                                               const std::vector<std::pair<std::string, std::string>>& kvs,
                                               const std::string& tenantID,
                                               const std::string& runtimeID,
                                               const std::string& requestID,
                                               uint64_t authorityRevision,
                                               const std::string& instanceRecordKey,
                                               uint32_t attempt);

    std::shared_ptr<MetaStorageAccessor> accessor_;
    std::string keyPrefix_;
    std::string httpEntryPoint_;
    bool enableTLS_;
    std::string serversTransport_;
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_TRAEFIK_REGISTRY_H
