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

#ifndef TEST_UNIT_MOCKS_FAKE_ETCD_STORE_H
#define TEST_UNIT_MOCKS_FAKE_ETCD_STORE_H

// Shared in-memory etcd fake (#T tests): MetaStoreClient::BeginTransaction
// hands out FakeEvalTxn whose Commit() evaluates the PRODUCTION-ASSEMBLED
// TxnCompare/TxnOperation objects (built via their Build() into etcd pb)
// against a std::map — the substitute EXECUTES what production assembled.
// The linearization point is FakeEvalTxn::Commit; tests install a barrier
// there. WARNING: the barrier runs SYNCHRONOUSLY on the calling thread —
// any test that must proceed while a transaction waits at the barrier has
// to issue that transaction from a SEPARATE thread.

#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "async/future.hpp"
#include "meta_store_client/txn_transaction.h"
#include "meta_store_client/meta_store_struct.h"

namespace functionsystem::test {

// In-memory etcd model distinguishing etcd's two counters:
//  - version[key]: write count since creation; 0 ⇔ ABSENT (resets on
//    delete-then-recreate) — the VERSION compare target.
//  - modRevision[key]: the GLOBAL store revision at the key's last write —
//    the OfModifyVersion target. Every committed transaction allocates ONE
//    new global revision, so a deleted-then-recreated key NEVER matches its
//    old mod revision.
struct FakeStore {
    std::mutex mutex;
    std::map<std::string, std::string> kv;
    std::map<std::string, int64_t> version;
    std::map<std::string, int64_t> modRevision;
    int64_t globalRevision = 0;

    std::string Get(const std::string &key, bool *found)
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = kv.find(key);
        if (it == kv.end()) {
            *found = false;
            return "";
        }
        *found = true;
        return it->second;
    }
    void Put(const std::string &key, const std::string &value)
    {
        std::lock_guard<std::mutex> lock(mutex);
        kv[key] = value;
        version[key] = version[key] + 1;
        modRevision[key] = ++globalRevision;
    }
    void EraseRange(const std::string &begin, const std::string &end)
    {
        for (auto it = kv.begin(); it != kv.end();) {
            if (it->first >= begin && it->first < end) {
                version.erase(it->first);
                modRevision.erase(it->first);
                it = kv.erase(it);
            } else {
                ++it;
            }
        }
    }
};

class FakeEvalTxn : public functionsystem::meta_store::TxnTransaction {
public:
    FakeEvalTxn(const std::shared_ptr<FakeStore> &store,
                const std::function<void()> &barrier = nullptr)
        : TxnTransaction(litebus::AID()), store_(store), barrier_(barrier)
    {
    }

    litebus::Future<std::shared_ptr<functionsystem::TxnResponse>> Commit() const override
    {
        if (barrier_) { barrier_(); }
        std::lock_guard<std::mutex> lock(store_->mutex);
        for (const auto &compare : compares) {
            ::etcdserverpb::Compare pb;
            compare.Build(pb, "");
            if (!Evaluate(*store_, pb)) {
                auto failed = std::make_shared<functionsystem::TxnResponse>();
                failed->success = false;
                return failed;
            }
        }
        const int64_t commitRevision = store_->globalRevision + 1;
        auto response = std::make_shared<functionsystem::TxnResponse>();
        response->success = true;
        response->header.revision = commitRevision;
        for (const auto &op : thenOps) {
            ::etcdserverpb::RequestOp pb;
            op.Build(pb, "");
            const int64_t affected = Apply(*store_, pb, commitRevision);
            // Per-op responses in the REAL etcd shape with the ACTUAL
            // execution result (deleted/matched counts) — the parsers
            // size-check responses and read these fields; a fixed value
            // would mask a non-executed operation.
            functionsystem::TxnOperationResponse opRsp;
            if (pb.has_request_delete_range()) {
                opRsp.operationType = functionsystem::TxnOperationType::OPERATION_DELETE;
                functionsystem::DeleteResponse del;
                del.deleted = affected;
                opRsp.response = del;
            } else if (pb.has_request_put()) {
                opRsp.operationType = functionsystem::TxnOperationType::OPERATION_PUT;
                opRsp.response = functionsystem::PutResponse{};
            } else {
                opRsp.operationType = functionsystem::TxnOperationType::OPERATION_GET;
                functionsystem::GetResponse get;
                get.count = affected;
                opRsp.response = get;
            }
            response->responses.emplace_back(std::move(opRsp));
        }
        // Advance the global revision (Apply does NOT do this): consecutive
        // transactions must never share a revision — the generation-compare
        // model depends on it.
        store_->globalRevision = commitRevision;
        return response;
    }

private:
    static bool Evaluate(const FakeStore &store, const ::etcdserverpb::Compare &pb)
    {
        const std::string key(pb.key());
        auto it = store.kv.find(key);
        const bool exists = it != store.kv.end();
        bool equal = false;
        switch (pb.target()) {
            case ::etcdserverpb::Compare::VALUE:
                equal = exists && it->second == std::string(pb.value());
                break;
            case ::etcdserverpb::Compare::VERSION:
                equal = (exists ? store.version.at(key) : 0) == pb.version();
                break;
            case ::etcdserverpb::Compare::MOD: {
                const auto modIt = store.modRevision.find(key);
                const int64_t mod = (modIt != store.modRevision.end()) ? modIt->second : 0;
                equal = mod == pb.mod_revision();
                break;
            }
            default:
                equal = false;
                break;
        }
        return pb.result() == ::etcdserverpb::Compare::EQUAL && equal;
    }

    // Returns the number of keys the op actually affected (deleted for
    // delete-range, matched for range-get) — the response fields are the
    // EXECUTION RESULT, never a fixed value.
    static int64_t Apply(FakeStore &store, const ::etcdserverpb::RequestOp &pb, int64_t commitRevision)
    {
        if (pb.has_request_put()) {
            const auto &put = pb.request_put();
            const std::string key(put.key());
            store.kv[key] = std::string(put.value());
            store.version[key] = store.version[key] + 1;
            store.modRevision[key] = commitRevision;
            return 1;
        }
        if (pb.has_request_delete_range()) {
            const auto &del = pb.request_delete_range();
            const std::string begin(del.key());
            const std::string end(del.range_end().empty() ? begin + '\x01' : std::string(del.range_end()));
            int64_t removed = 0;
            for (auto it = store.kv.begin(); it != store.kv.end();) {
                if (it->first >= begin && it->first < end) {
                    store.version.erase(it->first);
                    store.modRevision.erase(it->first);
                    it = store.kv.erase(it);
                    removed++;
                } else {
                    ++it;
                }
            }
            return removed;
        }
        if (pb.has_request_range()) {
            const auto &get = pb.request_range();
            const std::string begin(get.key());
            const std::string end(get.range_end().empty() ? begin + '\x01' : std::string(get.range_end()));
            int64_t matched = 0;
            for (const auto &kv : store.kv) {
                if (kv.first >= begin && kv.first < end) { matched++; }
            }
            return matched;
        }
        return 0;
    }

    std::shared_ptr<FakeStore> store_;
    std::function<void()> barrier_;
};

}  // namespace functionsystem::test

#endif  // TEST_UNIT_MOCKS_FAKE_ETCD_STORE_H
