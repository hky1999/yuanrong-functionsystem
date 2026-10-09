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

#ifndef UT_STATE_ISOLATION_SUPPORT_H
#define UT_STATE_ISOLATION_SUPPORT_H

// Map-backed persistent-transaction and data-plane models for the state
// isolation behavior tests. These are NOT wildcard-success mocks: every
// transaction is EVALUATED against the map through the PRODUCTION
// serializers (TxnCompare::Build / TxnOperation::Build into the etcd
// protobufs), compares are checked exactly (value, version,
// create_revision, mod_revision), and Then-ops apply atomically only when
// every compare holds. Gates allow a test to PARK a matching transaction
// and release (or fail) it later, producing REAL interleavings whose
// conditions are evaluated at release time — not scripted outcomes.

#include <atomic>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "function_proxy/common/distribute_cache_client/distributed_cache_client.h"
#include "meta_store_client/meta_store_client.h"

namespace functionsystem::function_proxy::test {

// ── transaction signature matching ──────────────────────────────────────────
struct TxnSignature {
    std::vector<std::string> compareKeys;
    std::vector<std::string> thenKeys;
};
using TxnMatcher = std::function<bool(const TxnSignature &)>;

inline TxnMatcher OnThenKey(const std::string &key)
{
    return [key](const TxnSignature &s) {
        return std::find(s.thenKeys.begin(), s.thenKeys.end(), key) != s.thenKeys.end();
    };
}

inline TxnMatcher OnCompareKey(const std::string &key)
{
    return [key](const TxnSignature &s) {
        return std::find(s.compareKeys.begin(), s.compareKeys.end(), key) != s.compareKeys.end();
    };
}

// ── map-backed meta store core ──────────────────────────────────────────────
struct MapRecord {
    std::string value;
    int64_t createRevision = 0;
    int64_t modRevision = 0;
    int64_t version = 0;
};

enum class CommitOutcome {
    EVALUATE,          // real compare evaluation + atomic apply
    FAIL_FUTURE,       // the Commit future itself fails
    NULL_RESPONSE,     // ready future carrying a null TxnResponse
    STATUS_ERROR,      // ready response with error status
    SUCCESS_FALSE,     // ready response, status OK, success=false
    APPLY_THEN_FAIL,   // REALLY applies the Then-ops, then reports failure
};

struct ForcedCommit {
    TxnMatcher matcher;
    CommitOutcome outcome;
};

enum class GetOutcome {
    NORMAL,
    FAIL_FUTURE,
    NULL_RESPONSE,
    STATUS_ERROR,
    AMBIGUOUS_TWO_KVS,  // two kvs for an exact read (kvs>1 corruption path)
};

struct ForcedGet {
    std::string key;  // the key (or, for prefix reads, the prefix) targeted
    GetOutcome outcome;
    bool prefixRead = false;  // match prefix scans instead of exact reads
};

class MapMetaStoreCore {
public:
    std::mutex lock;
    std::map<std::string, MapRecord> kv;
    int64_t revision = 0;
    std::vector<std::string> opLog;  // "put:<key>@<rev>" / "del:<key>@<rev>"

    std::vector<ForcedCommit> forcedCommits;
    std::vector<ForcedGet> forcedGets;

    // Gated transactions: a REGISTERED matcher (AddGate, one-shot) parks
    // the first matching Commit; the test releases (or fails) it later —
    // the parked transaction's conditions are evaluated at RELEASE time.
    struct Gated {
        TxnSignature signature;
        std::vector<meta_store::TxnCompare> compares;
        std::vector<meta_store::TxnOperation> thenOps;
        std::shared_ptr<litebus::Promise<std::shared_ptr<TxnResponse>>> promise;
    };
    std::vector<Gated> gated;
    std::vector<TxnMatcher> gateMatchers;
    void AddGate(const TxnMatcher &matcher)
    {
        std::lock_guard<std::mutex> guard(lock);
        gateMatchers.push_back(matcher);
    }

    // ── real evaluation against the map (uses the PRODUCTION serializers) ──
    static bool EvalCompare(const ::etcdserverpb::Compare &cmp,
                            const std::map<std::string, MapRecord> &kv);
    static void EvalResult(const ::etcdserverpb::Compare &cmp, const MapRecord *rec, bool &ok);
    // Applies thenOps atomically; every compare must hold. Returns the
    // TxnResponse (success + per-op responses) and mutates the map only on
    // success.
    std::shared_ptr<TxnResponse> EvaluateAndApply(const std::vector<meta_store::TxnCompare> &compares,
                                                  const std::vector<meta_store::TxnOperation> &thenOps);

    // Test seeding: raw put with explicit revisions.
    void Seed(const std::string &key, const std::string &value);
    // Direct read access (test assertions).
    bool Lookup(const std::string &key, MapRecord &out);
    bool Exists(const std::string &key);
    int64_t LatestRevision();

    // Gate control: release evaluates NOW (or applies the forced outcome).
    void ReleaseGate(const TxnMatcher &matcher, CommitOutcome forced = CommitOutcome::EVALUATE);
    void FailGate(const TxnMatcher &matcher);
    size_t GateCount();
};

// ── map-backed meta store client ────────────────────────────────────────────
class MapMetaStore : public MetaStoreClient {
public:
    explicit MapMetaStore(const std::shared_ptr<MapMetaStoreCore> &core)
        : MetaStoreClient(MetaStoreConfig{ .etcdAddress = "127.0.0.1:1" }), core_(core)
    {
    }

    litebus::Future<std::shared_ptr<GetResponse>> Get(const std::string &key,
                                                      const GetOption &option) override;
    std::shared_ptr<meta_store::TxnTransaction> BeginTransaction() override;

    // Seeding convenience (raw put through the map).
    litebus::Future<std::shared_ptr<PutResponse>> Put(const std::string &key, const std::string &value,
                                                      const PutOption &option) override;
    litebus::Future<std::shared_ptr<DeleteResponse>> Delete(const std::string &key,
                                                            const DeleteOption &option) override;

private:
    std::shared_ptr<MapMetaStoreCore> core_;
};

// Transaction whose Commit runs the REAL compare evaluation against the
// core's map (honoring forced outcomes and gates).
class MapTxnTransaction : public meta_store::TxnTransaction {  // NOLINT
public:
    MapTxnTransaction(const litebus::AID &aid, const std::shared_ptr<MapMetaStoreCore> &core)
        : TxnTransaction(aid), core_(core)
    {
    }

    litebus::Future<std::shared_ptr<TxnResponse>> Commit() const override;

private:
    TxnSignature Signature() const;
    std::shared_ptr<MapMetaStoreCore> core_;
};

// ── map-backed DataSystem client ────────────────────────────────────────────
enum class DsAction { OK, TIMEOUT, NOT_FOUND_FOLDED };

class MapDataStore : public DistributedCacheClient {
public:
    std::mutex lock;
    std::map<std::string, std::string> data;
    std::map<std::string, int> setCalls;
    std::map<std::string, int> delCalls;
    std::map<std::string, int> getCalls;
    std::map<std::string, DsAction> forcedSet;
    std::map<std::string, DsAction> forcedDel;
    // One-shot global injections (the snapshot key is a fresh UUID the
    // test cannot know before the save): the NEXT Set/Del fails with the
    // given action regardless of key.
    std::optional<DsAction> forceNextSet;
    std::optional<DsAction> forceNextDel;

    Status Init() override { return Status::OK(); }
    Status Set(const std::string &key, const std::string &val) override;
    Status Get(const std::string &key, std::string &val) override;
    Status Del(const std::string &key) override;

    Status Get(const std::vector<std::string> &keys, std::vector<std::string> &vals) override
    {
        (void)keys;
        (void)vals;
        return Status::OK();
    }
    Status Del(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys) override
    {
        (void)keys;
        (void)failedKeys;
        return Status::OK();
    }
    Status GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objs,
                          std::vector<ObjMetaInfo> &meta) override
    {
        (void)tenantId;
        (void)objs;
        (void)meta;
        return Status::OK();
    }
    Status GetWorkerAddrByWorkerId(const std::vector<std::string> &workerIds,
                                   std::vector<std::string> &workerAddrs) override
    {
        (void)workerIds;
        (void)workerAddrs;
        return Status::OK();
    }
    Status GetHealthStatus() override { return Status::OK(); }
    Status ShutDown() override { return Status::OK(); }

    int SetCount(const std::string &key);
    int DelCount(const std::string &key);
    int TotalSetCalls();
    int TotalDelCalls();
    void ForceSet(const std::string &key, DsAction action) { forcedSet[key] = action; }
    void ForceDel(const std::string &key, DsAction action) { forcedDel[key] = action; }
};

}  // namespace functionsystem::function_proxy::test

#endif  // UT_STATE_ISOLATION_SUPPORT_H
