/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "traefik_registry.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <regex>

#include "common/logs/logging.h"
#include "meta_store_client/txn_transaction.h"

namespace functionsystem::local_scheduler {

namespace {
constexpr uint32_t MARKER_TXN_MAX_ATTEMPTS = 3;

DeleteOption ExactDeleteOption()
{
    return DeleteOption{ .prevKv = false, .prefix = false, .asyncBackup = false };
}
}  // namespace

TraefikRegistry::TraefikRegistry(std::shared_ptr<MetaStorageAccessor> accessor,
                                 const std::string& keyPrefix,
                                 const std::string& httpEntryPoint,
                                 bool enableTLS,
                                 const std::string& serversTransport)
    : accessor_(std::move(accessor)),
      keyPrefix_(keyPrefix),
      httpEntryPoint_(httpEntryPoint),
      enableTLS_(enableTLS),
      serversTransport_(serversTransport)
{
    // Validate serversTransport format (needed for 'https' protocol ports)
    if (serversTransport_.empty()) {
        YRLOG_WARN("TraefikRegistry: serversTransport is empty, 'https' protocol ports will not use backend TLS");
    } else {
        // Validate format: should be like "name@provider" (e.g., "yr-backend-tls@file")
        static const std::regex transportPattern(R"(^[^@]+@[^@]+$)");
        if (!std::regex_match(serversTransport_, transportPattern)) {
            YRLOG_ERROR("TraefikRegistry: invalid serversTransport format '{}', "
                        "expected 'name@provider' (e.g., 'yr-backend-tls@file')",
                        serversTransport_);
            throw std::invalid_argument(
                "serversTransport must be in format 'name@provider' (e.g., 'yr-backend-tls@file')");
        }
        YRLOG_INFO("TraefikRegistry: 'https' protocol ports will use backend TLS with ServersTransport: {}",
                   serversTransport_);
    }

    YRLOG_INFO("TraefikRegistry initialized: keyPrefix={}, httpEntryPoint={}, enableTLS={}, serversTransport={}",
               keyPrefix_, httpEntryPoint_, enableTLS_, serversTransport_);

    // Create global StripPrefix middleware for all instances
    // This middleware removes /{instanceID}/{port} prefix using regex
    if (accessor_) {
        accessor_->Put(keyPrefix_ + "/http/middlewares/stripprefix-all/stripPrefixRegex/regex",
                       "^/[^/]+/[0-9]+")
            .Then([](const Status& status) -> Status {
                if (!status.IsOk()) {
                    YRLOG_WARN("Failed to create global StripPrefix middleware: {}", status.GetMessage());
                } else {
                    YRLOG_INFO("Global StripPrefix middleware created successfully");
                }
                return Status::OK();
            });
    }
}

std::string TraefikRegistry::EscapeField(const std::string& in)
{
    std::string out;
    out.reserve(in.size());
    for (char c : in) {
        // '%' escapes itself; '|', '=', ',' are field/list separators.
        if (c == '%' || c == '|' || c == '=' || c == ',') {
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

std::string TraefikRegistry::UnescapeField(const std::string& in)
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

std::string TraefikRegistry::SerializeMarker(const std::string& instanceID,
                                             const std::string& tenantID,
                                             const std::string& runtimeID,
                                             const std::string& requestID,
                                             uint64_t instanceRevision,
                                             const std::vector<std::string>& ownKeys)
{
    std::vector<std::string> sorted(ownKeys);
    std::sort(sorted.begin(), sorted.end());
    sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
    std::ostringstream keys;
    for (size_t i = 0; i < sorted.size(); ++i) {
        if (i > 0) { keys << ","; }
        keys << EscapeField(sorted[i]);
    }
    std::ostringstream out;
    out << "v1"
        << "|i=" << EscapeField(instanceID)
        << "|t=" << EscapeField(tenantID)
        << "|r=" << EscapeField(runtimeID)
        << "|q=" << EscapeField(requestID)
        << "|m=" << instanceRevision
        << "|k=" << keys.str();
    return out.str();
}

TraefikRegistry::IncarnationMarker TraefikRegistry::ParseMarker(const std::string& raw)
{
    IncarnationMarker marker;
    marker.raw = raw;
    marker.present = true;
    std::map<std::string, std::string> fields;
    size_t pos = 0;
    if (raw.rfind("v1|", 0) != 0) {
        return marker;  // unknown schema: present, not valid
    }
    pos = 3;
    while (pos < raw.size()) {
        const auto eq = raw.find('=', pos);
        if (eq == std::string::npos) { return marker; }
        const std::string key = raw.substr(pos, eq - pos);
        const auto sep = raw.find('|', eq + 1);
        const std::string value = (sep == std::string::npos) ? raw.substr(eq + 1)
                                                             : raw.substr(eq + 1, sep - eq - 1);
        if (!fields.emplace(key, value).second) {
            return marker;  // duplicate field: refuse
        }
        pos = (sep == std::string::npos) ? raw.size() : sep + 1;
    }
    const auto it = fields.find("k");
    if (!fields.count("i") || !fields.count("r") || !fields.count("q") || !fields.count("m")
        || it == fields.end()) {
        return marker;
    }
    marker.instanceID = UnescapeField(fields["i"]);
    marker.tenantID = UnescapeField(fields["t"]);  // "t" may be absent (empty tenant)
    marker.runtimeID = UnescapeField(fields["r"]);
    marker.requestID = UnescapeField(fields["q"]);
    try {
        marker.instanceRevision = std::stoull(fields["m"]);
    } catch (const std::exception&) {
        return marker;  // non-numeric authority revision: refuse
    }
    std::istringstream keys(it->second);
    std::string one;
    while (std::getline(keys, one, ',')) {
        if (!one.empty()) {
            marker.ownKeys.push_back(UnescapeField(one));
        }
    }
    std::sort(marker.ownKeys.begin(), marker.ownKeys.end());
    marker.valid = true;
    return marker;
}

std::string TraefikRegistry::MarkerKey(const std::string& safeID) const
{
    return keyPrefix_ + "/http/instances/" + safeID + "/generation";
}

std::string TraefikRegistry::BuildOwnKeyPattern(const std::string& httpBase, const std::string& safeID)
{
    // Regex-escape everything in safeID (it is already sanitized to a small
    // alphabet, but do not rely on that here).
    std::string escaped;
    for (char c : safeID) {
        if (std::isalnum(static_cast<unsigned char>(c)) == 0) { escaped.push_back('\\'); }
        escaped.push_back(c);
    }
    // Anchored SEGMENT match: httpBase already ends in "/http"; the
    // router/service name segment must be exactly <safeID>-p<digits> —
    // "inst-a" never matches "inst-ab", and a safeID that itself ends in
    // "-p<digits>" cannot be confused with another instance's port keys.
    return "^" + httpBase + "/(routers|services)/" + escaped + "-p[0-9]+(/|$)";
}

bool TraefikRegistry::IsOwnKey(const std::string& pattern, const std::string& key)
{
    static std::map<std::string, std::regex> cache;
    auto it = cache.find(pattern);
    if (it == cache.end()) {
        it = cache.emplace(pattern, std::regex(pattern)).first;
    }
    return std::regex_search(key, it->second);
}

TraefikRegistry::IncarnationMarker TraefikRegistry::ReadMarker(const std::string& instanceID)
{
    if (!accessor_) {
        return IncarnationMarker{};
    }
    const auto value = accessor_->Get(MarkerKey(SanitizeID(instanceID)));
    if (value.IsNone()) {
        return IncarnationMarker{};  // absent: present=false, valid=false
    }
    return ParseMarker(value.Get());
}

std::vector<std::string> TraefikRegistry::EnumerateOwnKeys(const std::string& instanceID)
{
    std::vector<std::string> keys;
    if (!accessor_) {
        return keys;
    }
    const std::string safeID = SanitizeID(instanceID);
    const std::string httpBase = keyPrefix_ + "/http";
    const std::string pattern = BuildOwnKeyPattern(httpBase, safeID);
    for (const std::string& prefix : { httpBase + "/routers/" + safeID, httpBase + "/services/" + safeID }) {
        const auto found = accessor_->GetAllWithPrefix(prefix);
        if (found.IsNone()) {
            continue;
        }
        for (const auto& kv : found.Get()) {
            if (IsOwnKey(pattern, kv.first)) {
                keys.push_back(kv.first);
            }
        }
    }
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    return keys;
}

litebus::Future<Status> TraefikRegistry::RegisterInstance(
    const std::string& instanceID,
    const std::string& hostIP,
    const std::vector<PortMapping>& portMappings)
{
    // Legacy: plain put transaction, no marker (pre-fencing behavior,
    // unchanged key set).
    if (!accessor_) {
        return litebus::Future<Status>(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "meta storage accessor is null"));
    }
    if (portMappings.empty()) {
        return Status::OK();
    }
    const std::string safeID = SanitizeID(instanceID);
    std::vector<std::pair<std::string, std::string>> kvs;
    for (const auto& mapping : portMappings) {
        const std::string routerName = safeID + "-p" + std::to_string(mapping.sandboxPort);
        const std::string prefixPath = "/" + safeID + "/" + std::to_string(mapping.sandboxPort);
        std::string protocolLower = mapping.protocol;
        std::transform(protocolLower.begin(), protocolLower.end(), protocolLower.begin(), ::tolower);
        const bool useHttpsForBackend = (protocolLower == "https");
        kvs.push_back({keyPrefix_ + "/http/routers/" + routerName + "/rule", "PathPrefix(`" + prefixPath + "`)"});
        kvs.push_back({keyPrefix_ + "/http/routers/" + routerName + "/service", routerName});
        kvs.push_back({keyPrefix_ + "/http/routers/" + routerName + "/middlewares/0", "stripprefix-all"});
        kvs.push_back({keyPrefix_ + "/http/routers/" + routerName + "/entryPoints/0", httpEntryPoint_});
        if (enableTLS_) {
            kvs.push_back({keyPrefix_ + "/http/routers/" + routerName + "/tls", ""});
        }
        std::ostringstream urlStream;
        urlStream << (useHttpsForBackend ? "https" : "http") << "://" << hostIP << ":" << mapping.hostPort;
        kvs.push_back({keyPrefix_ + "/http/services/" + routerName + "/loadbalancer/servers/0/url",
                       urlStream.str()});
        if (useHttpsForBackend && !serversTransport_.empty()) {
            kvs.push_back({keyPrefix_ + "/http/services/" + routerName + "/loadbalancer/serverstransport",
                           serversTransport_});
        }
    }
    YRLOG_INFO("Registering instance {} to Traefik HTTP (legacy): {} ports, {} keys",
               instanceID, portMappings.size(), kvs.size());
    return accessor_->Txn(kvs)
        .Then([instanceID, ports = portMappings.size()](const Status& status) -> Status {
            if (!status.IsOk()) {
                YRLOG_ERROR("Failed to register instance {} to Traefik: {}", instanceID, status.GetMessage());
                return status;
            }
            YRLOG_INFO("Successfully registered instance {} to Traefik with {} ports", instanceID, ports);
            return Status::OK();
        });
}

litebus::Future<Status> TraefikRegistry::RegisterInstance(
    const std::string& instanceID,
    const std::string& hostIP,
    const std::vector<PortMapping>& portMappings,
    const std::string& tenantID,
    const std::string& runtimeID,
    const std::string& requestID,
    uint64_t authorityRevision,
    const std::string& instanceRecordKey)
{
    if (!accessor_) {
        YRLOG_ERROR("TraefikRegistry: accessor is null, skip registration for instanceID={}", instanceID);
        return litebus::Future<Status>(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "meta storage accessor is null"));
    }
    // NOTE: zero port mappings is NOT an early-OK — a same-generation
    // "ports → zero ports" change must still run the marker transaction so
    // the previous holder's own-key list is atomically cleaned.

    std::string safeID = SanitizeID(instanceID);

    std::vector<std::pair<std::string, std::string>> kvs;
    for (const auto& mapping : portMappings) {
        std::string routerName = safeID + "-p" + std::to_string(mapping.sandboxPort);
        std::string prefixPath = "/" + safeID + "/" + std::to_string(mapping.sandboxPort);

        std::string protocolLower = mapping.protocol;
        std::transform(protocolLower.begin(), protocolLower.end(), protocolLower.begin(), ::tolower);
        bool useHttpsForBackend = (protocolLower == "https");
        std::string scheme = useHttpsForBackend ? "https" : "http";

        kvs.push_back({keyPrefix_ + "/http/routers/" + routerName + "/rule",
                       "PathPrefix(`" + prefixPath + "`)"});
        kvs.push_back({keyPrefix_ + "/http/routers/" + routerName + "/service", routerName});
        kvs.push_back({keyPrefix_ + "/http/routers/" + routerName + "/middlewares/0", "stripprefix-all"});
        kvs.push_back({keyPrefix_ + "/http/routers/" + routerName + "/entryPoints/0", httpEntryPoint_});
        if (enableTLS_) {
            kvs.push_back({keyPrefix_ + "/http/routers/" + routerName + "/tls", ""});
        }
        std::ostringstream urlStream;
        urlStream << scheme << "://" << hostIP << ":" << mapping.hostPort;
        kvs.push_back({keyPrefix_ + "/http/services/" + routerName + "/loadbalancer/servers/0/url",
                       urlStream.str()});
        if (useHttpsForBackend && !serversTransport_.empty()) {
            kvs.push_back({keyPrefix_ + "/http/services/" + routerName + "/loadbalancer/serverstransport",
                           serversTransport_});
        }
    }

    std::vector<std::string> ownKeys;
    ownKeys.reserve(kvs.size());
    for (const auto& kv : kvs) {
        ownKeys.push_back(kv.first);
    }

    YRLOG_INFO("Registering instance {} to Traefik HTTP (fenced): {} ports, {} keys, runtime={}, request={}",
               instanceID, portMappings.size(), kvs.size(), runtimeID, requestID);

    return RegisterWithMarker(instanceID, kvs, tenantID, runtimeID, requestID, authorityRevision,
                              instanceRecordKey, 0)
        .Then([instanceID, ports = portMappings.size()](const Status& status) -> Status {
            if (!status.IsOk()) {
                YRLOG_ERROR("Failed to register instance {} to Traefik: {}", instanceID, status.GetMessage());
                return status;
            }
            YRLOG_INFO("Successfully registered instance {} to Traefik with {} ports", instanceID, ports);
            return Status::OK();
        });
}

litebus::Future<Status> TraefikRegistry::RegisterWithMarker(
    const std::string& instanceID,
    const std::vector<std::pair<std::string, std::string>>& kvs,
    const std::string& tenantID,
    const std::string& runtimeID,
    const std::string& requestID,
    uint64_t authorityRevision,
    const std::string& instanceRecordKey,
    uint32_t attempt)
{
    const std::string safeID = SanitizeID(instanceID);
    const std::string markerKey = MarkerKey(safeID);
    const std::string markerValue = SerializeMarker(instanceID, tenantID, runtimeID, requestID,
        authorityRevision,
        [&kvs]() {
            std::vector<std::string> keys;
            keys.reserve(kvs.size());
            for (const auto& kv : kvs) { keys.push_back(kv.first); }
            return keys;
        }());

    // SINGLE read+decide+commit cycle: every commit is authorized by THIS
    // capture plus the caller's durable authority revision — never by a
    // pre-check from a previous attempt, and never by treating an arbitrary
    // read holder as a permitted predecessor.
    const IncarnationMarker current = ReadMarker(instanceID);
    std::vector<std::string> oldKeys;
    std::shared_ptr<meta_store::TxnCompare> compare;
    if (!current.present) {
        // No marker: fresh name, or legacy routes left without one. The
        // transaction still commits only if the marker REMAINS absent.
        oldKeys = EnumerateOwnKeys(instanceID);
        compare = std::make_shared<meta_store::TxnCompare>(
            meta_store::TxnCompare::OfVersion(markerKey, meta_store::CompareOperator::EQUAL, 0));
    } else if (!current.valid) {
        // Present but unparseable: refuse to guess — operator attention.
        YRLOG_ERROR("TraefikRegistry: unparseable marker for instance({}); refusing registration", instanceID);
        return litebus::Future<Status>(Status(StatusCode::ERR_INNER_SYSTEM_ERROR,
            "traefik marker unparseable; refusing registration"));
    } else if (OwnershipMatches(current, instanceID, tenantID, runtimeID, requestID)) {
        // Our OWN previous registration (refresh/port change): replace its
        // full list atomically under its captured value.
        oldKeys = current.ownKeys;
        compare = std::make_shared<meta_store::TxnCompare>(
            meta_store::TxnCompare::OfValue(markerKey, meta_store::CompareOperator::EQUAL, current.raw));
    } else if (MayTakeOver(current, instanceID, tenantID, authorityRevision)) {
        // A FOREIGN holder that our durable authority revision provably
        // supersedes (legitimate new-generation takeover): delete its full
        // list, write ours + new marker, atomically under its captured
        // value. A late OLD registration fails MayTakeOver against a newer
        // holder and never reaches this branch.
        oldKeys = current.ownKeys;
        compare = std::make_shared<meta_store::TxnCompare>(
            meta_store::TxnCompare::OfValue(markerKey, meta_store::CompareOperator::EQUAL, current.raw));
    } else {
        YRLOG_ERROR("TraefikRegistry: register refused for instance({}): name held by instance({}) "
                    "runtime({}) request({}) with authority revision {}, ours is {} — not provably "
                    "newer; refusing overwrite",
                    instanceID, current.instanceID, current.runtimeID, current.requestID,
                    current.instanceRevision, authorityRevision);
        return litebus::Future<Status>(Status(StatusCode::ERR_INSTANCE_BUSY,
            "traefik route held by a newer incarnation; registration refused"));
    }

    auto txn = accessor_->GetMetaClient()->BeginTransaction();
    txn->If(*compare);
    // DURABLE-OWNERSHIP CONDITION (root ②): the caller's instance record
    // must STILL carry the exact etcd modify revision the registration
    // captured — a replay of a RETIRED generation (record deleted or since
    // rewritten) fails this compare even when the marker is absent, so a
    // once-valid positive revision cannot resurrect routes. Applies to
    // EVERY branch, including marker-absence.
    if (!instanceRecordKey.empty()) {
        txn->If(meta_store::TxnCompare::OfModifyVersion(instanceRecordKey,
            meta_store::CompareOperator::EQUAL, static_cast<int64_t>(authorityRevision)));
    }
    const auto exact = ExactDeleteOption();
    for (const auto& key : oldKeys) {
        txn->Then(meta_store::TxnOperation::Create(key, exact));
    }
    for (const auto& kv : kvs) {
        txn->Then(meta_store::TxnOperation::Create(kv.first, kv.second, PutOption{}));
    }
    txn->Then(meta_store::TxnOperation::Create(markerKey, markerValue, PutOption{}));

    return txn->Commit().Then(
        [this, instanceID, kvs, tenantID, runtimeID, requestID, authorityRevision, instanceRecordKey, attempt]
        (const std::shared_ptr<TxnResponse>& response) -> litebus::Future<Status> {
            if (response->status.IsError()) {
                YRLOG_ERROR("TraefikRegistry: register txn transport failure for instance({}): {}",
                            instanceID, response->status.GetMessage());
                return Status(StatusCode::BP_META_STORAGE_PUT_ERROR, "traefik register txn failed");
            }
            if (response->success) {
                return Status::OK();
            }
            if (attempt + 1 >= MARKER_TXN_MAX_ATTEMPTS) {
                YRLOG_ERROR("TraefikRegistry: register for instance({}) lost the marker race {} times; giving up",
                            instanceID, attempt + 1);
                return Status(StatusCode::ERR_INSTANCE_BUSY, "traefik marker contention");
            }
            // Compare failed at COMMIT TIME. The retry is a FULL fresh
            // read+decide+commit cycle: whatever it reads is judged by the
            // SAME ownership/authority rules above (a foreign newer holder
            // fails MayTakeOver; our own refresh retries). No state is
            // carried over from this attempt's capture.
            return RegisterWithMarker(instanceID, kvs, tenantID, runtimeID, requestID,
                                      authorityRevision, instanceRecordKey, attempt + 1);
        });
}

bool TraefikRegistry::MayTakeOver(const IncarnationMarker& holder,
                                  const std::string& instanceID,
                                  const std::string& tenantID,
                                  uint64_t ourRevision)
{
    // The durable order: a caller may replace a foreign holder only when
    // it is the SAME logical instance+tenant (SanitizeID collisions — e.g.
    // "a_b" vs "a-b" — must never let one steal the other's route) AND its
    // etcd mod revision (extensions[INSTANCE_MOD_REVISION], persisted with
    // the same runtime/request identity in one record) is strictly newer
    // than the holder's recorded one.
    return holder.valid
        && holder.instanceID == instanceID
        && holder.tenantID == tenantID
        && ourRevision > holder.instanceRevision;
}

bool TraefikRegistry::OwnershipMatches(const IncarnationMarker& marker,
                                       const std::string& instanceID,
                                       const std::string& tenantID,
                                       const std::string& runtimeID,
                                       const std::string& requestID)
{
    return marker.valid
        && marker.instanceID == instanceID
        && (tenantID.empty() || marker.tenantID == tenantID)
        && marker.runtimeID == runtimeID
        && marker.requestID == requestID;
}

litebus::Future<Status> TraefikRegistry::UnregisterInstance(const std::string& instanceID)
{
    if (!accessor_) {
        YRLOG_WARN("TraefikRegistry: accessor is null, skip unregistration for instanceID={}", instanceID);
        return litebus::Future<Status>(Status::OK());
    }
    // Legacy (identity-less) retire: an UNKNOWN identity must never delete
    // marker-bearing data — if a marker exists, refuse and require the
    // fenced entry with the original generation's identity. Only pre-
    // fencing (marker-less) routes are cleaned here, by EXACT-key deletion
    // under the anchored segment pattern (never the bare safeID prefix —
    // inst-a's retire must not touch inst-ab).
    if (ReadMarker(instanceID).present) {
        YRLOG_WARN("TraefikRegistry: legacy unregister refused for instance({}): route is marker-bearing; "
                   "the original generation's identity is required", instanceID);
        return litebus::Future<Status>(Status(StatusCode::ERR_INSTANCE_BUSY,
            "traefik route is marker-bearing; identity-less unregister refused"));
    }
    YRLOG_INFO("Unregistering instance {} from Traefik HTTP (legacy exact-key, marker-less)", instanceID);
    const auto ownKeys = EnumerateOwnKeys(instanceID);
    const std::string markerKey = MarkerKey(SanitizeID(instanceID));
    auto txn = accessor_->GetMetaClient()->BeginTransaction();
    // The cleanup is CONDITIONAL on the marker still being absent at
    // COMMIT TIME: a new Register landing between our read and this commit
    // writes a marker, fails this compare, and keeps its routes (root W1).
    txn->If(meta_store::TxnCompare::OfVersion(markerKey, meta_store::CompareOperator::EQUAL, 0));
    const auto exact = ExactDeleteOption();
    for (const auto& key : ownKeys) {
        txn->Then(meta_store::TxnOperation::Create(key, exact));
    }
    return txn->Commit().Then([instanceID](const std::shared_ptr<TxnResponse>& response) -> Status {
        if (response->status.IsError()) {
            YRLOG_WARN("Failed to unregister instance {} from Traefik HTTP: {}", instanceID,
                       response->status.GetMessage());
            return Status::OK();  // transport failure: legacy semantics, never block
        }
        if (!response->success) {
            // Compare failure is PROPAGATED: a marker appeared mid-flight —
            // the routes now belong to a registered generation this
            // identity-less path must not touch.
            YRLOG_WARN("Legacy unregister for instance({}) refused at commit: a marker appeared "
                       "mid-flight", instanceID);
            return Status(StatusCode::ERR_INSTANCE_BUSY,
                "traefik marker appeared during legacy cleanup; identity required");
        }
        YRLOG_INFO("Successfully unregistered instance {} from Traefik HTTP (legacy)", instanceID);
        return Status::OK();
    });
}

litebus::Future<Status> TraefikRegistry::UnregisterInstance(const std::string& instanceID,
                                                            const std::string& expectedTenantID,
                                                            const std::string& expectedRuntimeID,
                                                            const std::string& expectedRequestID)
{
    if (!accessor_) {
        YRLOG_WARN("TraefikRegistry: accessor is null, skip unregistration for instanceID={}", instanceID);
        return litebus::Future<Status>(Status::OK());
    }
    if (expectedRuntimeID.empty() || expectedRequestID.empty()) {
        // The fenced entry requires the COMPLETE identity — a one-sided
        // identity is not a fence.
        YRLOG_WARN("TraefikRegistry: fenced unregister refused for instance({}): incomplete identity "
                   "(runtime empty={} request empty={})",
                   instanceID, expectedRuntimeID.empty(), expectedRequestID.empty());
        return litebus::Future<Status>(Status(StatusCode::ERR_INSTANCE_BUSY,
            "traefik fenced unregister requires complete identity"));
    }
    return UnregisterInstanceFenced(instanceID, expectedTenantID, expectedRuntimeID, expectedRequestID, 0);
}

litebus::Future<Status> TraefikRegistry::UnregisterInstanceFenced(
    const std::string& instanceID, const std::string& expectedTenantID,
    const std::string& expectedRuntimeID, const std::string& expectedRequestID, uint32_t attempt)
{
    const std::string markerKey = MarkerKey(SanitizeID(instanceID));
    const IncarnationMarker marker = ReadMarker(instanceID);
    if (!marker.present) {
        // Unknown is NOT a legacy free pass: nothing proves this route (if
        // any) belongs to the retiring chain.
        YRLOG_WARN("TraefikRegistry: fenced unregister refused for instance({}): no marker on record",
                   instanceID);
        return litebus::Future<Status>(Status(StatusCode::ERR_INSTANCE_BUSY,
            "traefik route ownership unknown (no marker); fenced unregister refused"));
    }
    if (!marker.valid) {
        YRLOG_ERROR("TraefikRegistry: fenced unregister refused for instance({}): marker unparseable",
                    instanceID);
        return litebus::Future<Status>(Status(StatusCode::ERR_INNER_SYSTEM_ERROR,
            "traefik marker unparseable; fenced unregister refused"));
    }
    // OWNERSHIP FIRST, from the CAPTURED value, in FULL — including the
    // marker's own instance field: two different instanceIDs can Sanitize
    // to the same safeID, and one must never retire the other's route.
    if (!OwnershipMatches(marker, instanceID, expectedTenantID, expectedRuntimeID, expectedRequestID)) {
        YRLOG_WARN("TraefikRegistry: fenced unregister refused for instance({}): route belongs to "
                   "instance({}) tenant({}) runtime({}) request({}); retiring chain carries tenant({}) "
                   "runtime({}) request({})",
                   instanceID, marker.instanceID, marker.tenantID, marker.runtimeID, marker.requestID,
                   expectedTenantID, expectedRuntimeID, expectedRequestID);
        return litebus::Future<Status>(Status(StatusCode::ERR_INSTANCE_BUSY,
            "traefik route bound to another incarnation; unregister refused"));
    }

    auto txn = accessor_->GetMetaClient()->BeginTransaction();
    txn->If(meta_store::TxnCompare::OfValue(markerKey, meta_store::CompareOperator::EQUAL, marker.raw));
    const auto exact = ExactDeleteOption();
    for (const auto& key : marker.ownKeys) {
        txn->Then(meta_store::TxnOperation::Create(key, exact));
    }
    txn->Then(meta_store::TxnOperation::Create(markerKey, exact));

    return txn->Commit().Then(
        [this, instanceID, expectedTenantID, expectedRuntimeID, expectedRequestID, markerKey, marker, attempt]
        (const std::shared_ptr<TxnResponse>& response) -> litebus::Future<Status> {
            if (response->status.IsError()) {
                YRLOG_WARN("TraefikRegistry: fenced unregister txn failure for instance({}): {}",
                           instanceID, response->status.GetMessage());
                return Status(StatusCode::BP_META_STORAGE_DELETE_ERROR, "traefik retire txn failed");
            }
            if (response->success) {
                YRLOG_INFO("Successfully unregistered instance {} from Traefik HTTP (fenced)", instanceID);
                return Status::OK();
            }
            if (attempt + 1 >= MARKER_TXN_MAX_ATTEMPTS) {
                YRLOG_WARN("TraefikRegistry: fenced unregister for instance({}) lost the marker race "
                           "{} times; giving up", instanceID, attempt + 1);
                return Status(StatusCode::ERR_INSTANCE_BUSY, "traefik marker contention during retire");
            }
            // The marker changed at COMMIT TIME (same-generation refresh).
            // Re-read and re-compare the FULL ownership; only OUR OWN new
            // value may be retried — never a foreign marker's key list.
            const IncarnationMarker fresh = ReadMarker(instanceID);
            if (!fresh.present || !fresh.valid) {
                return Status(StatusCode::ERR_INSTANCE_BUSY, "traefik marker lost during retire");
            }
            if (!OwnershipMatches(fresh, instanceID, expectedTenantID, expectedRuntimeID,
                                  expectedRequestID)) {
                YRLOG_WARN("TraefikRegistry: retire retry for instance({}) sees a foreign marker "
                           "(instance({}) runtime({}) request({})); refusing to chase",
                           instanceID, fresh.instanceID, fresh.runtimeID, fresh.requestID);
                return Status(StatusCode::ERR_INSTANCE_BUSY,
                    "traefik route bound to another incarnation; retire retry refused");
            }
            if (fresh.raw == marker.raw) {
                // Value unchanged yet the compare failed — do not spin.
                return Status(StatusCode::ERR_INSTANCE_BUSY, "traefik marker compare failed unexpectedly");
            }
            return UnregisterInstanceFenced(instanceID, expectedTenantID, expectedRuntimeID,
                                            expectedRequestID, attempt + 1);
        });
}

std::string TraefikRegistry::SanitizeID(const std::string& id)
{
    constexpr size_t atReplacementLen = 4;   // length of "-at-" replacement string
    constexpr size_t maxRouterNameLen = 200;  // Traefik router name length limit
    std::string result = id;
    // Replace @ with -at- (Traefik doesn't allow @ in router/service names)
    size_t pos = 0;
    while ((pos = result.find('@', pos)) != std::string::npos) {
        result.replace(pos, 1, "-at-", atReplacementLen);
        pos += atReplacementLen; // Skip past the replacement
    }
    // Replace other potentially problematic characters
    // Traefik router/service names should be DNS-compatible
    std::replace(result.begin(), result.end(), '/', '-');
    std::replace(result.begin(), result.end(), '.', '-');
    std::replace(result.begin(), result.end(), '_', '-');
    // Limit length (Traefik may have limits on router names)
    if (result.length() > maxRouterNameLen) {
        result = result.substr(0, maxRouterNameLen);
    }
    return result;
}

}  // namespace functionsystem::local_scheduler
