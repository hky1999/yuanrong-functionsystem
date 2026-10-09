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

#include "state_isolation_support.h"

#include <algorithm>

namespace functionsystem::function_proxy::test {

// ── MapMetaStoreCore ─────────────────────────────────────────────────────────

void MapMetaStoreCore::EvalResult(const ::etcdserverpb::Compare &cmp, const MapRecord *rec,
                                  bool &ok)
{
    bool present = rec != nullptr;
    int64_t actual = 0;
    switch (cmp.target()) {
        case ::etcdserverpb::Compare_CompareTarget_VERSION:
            actual = present ? rec->version : 0;
            ok = ok && (cmp.result() == ::etcdserverpb::Compare_CompareResult_EQUAL
                            ? actual == cmp.version()
                            : cmp.result() == ::etcdserverpb::Compare_CompareResult_NOT_EQUAL
                                ? actual != cmp.version()
                                : cmp.result() == ::etcdserverpb::Compare_CompareResult_GREATER
                                    ? actual > cmp.version()
                                    : actual < cmp.version());
            return;
        case ::etcdserverpb::Compare_CompareTarget_CREATE:
            actual = present ? rec->createRevision : 0;
            ok = ok && (cmp.result() == ::etcdserverpb::Compare_CompareResult_EQUAL
                            ? actual == cmp.create_revision()
                            : cmp.result() == ::etcdserverpb::Compare_CompareResult_NOT_EQUAL
                                ? actual != cmp.create_revision()
                                : cmp.result() == ::etcdserverpb::Compare_CompareResult_GREATER
                                    ? actual > cmp.create_revision()
                                    : actual < cmp.create_revision());
            return;
        case ::etcdserverpb::Compare_CompareTarget_MOD:
            actual = present ? rec->modRevision : 0;
            ok = ok && (cmp.result() == ::etcdserverpb::Compare_CompareResult_EQUAL
                            ? actual == cmp.mod_revision()
                            : cmp.result() == ::etcdserverpb::Compare_CompareResult_NOT_EQUAL
                                ? actual != cmp.mod_revision()
                                : cmp.result() == ::etcdserverpb::Compare_CompareResult_GREATER
                                    ? actual > cmp.mod_revision()
                                    : actual < cmp.mod_revision());
            return;
        case ::etcdserverpb::Compare_CompareTarget_VALUE:
            if (!present) {
                ok = false;  // a value compare against a missing key never holds
                return;
            }
            ok = ok && (cmp.result() == ::etcdserverpb::Compare_CompareResult_EQUAL
                            ? rec->value == cmp.value()
                            : cmp.result() == ::etcdserverpb::Compare_CompareResult_NOT_EQUAL
                                ? rec->value != cmp.value()
                                : false);  // ordering results unused for values
            return;
        default:
            ok = false;
            return;
    }
}

bool MapMetaStoreCore::EvalCompare(const ::etcdserverpb::Compare &cmp,
                                   const std::map<std::string, MapRecord> &kv)
{
    const auto it = kv.find(cmp.key());
    const MapRecord *rec = it == kv.end() ? nullptr : &it->second;
    bool ok = true;
    EvalResult(cmp, rec, ok);
    return ok;
}

std::shared_ptr<TxnResponse> MapMetaStoreCore::EvaluateAndApply(
    const std::vector<meta_store::TxnCompare> &compares,
    const std::vector<meta_store::TxnOperation> &thenOps)
{
    std::lock_guard<std::mutex> guard(lock);
    // Evaluate EVERY compare against the CURRENT map (production serializer).
    bool success = true;
    for (const auto &cmp : compares) {
        ::etcdserverpb::Compare pb;
        cmp.Build(pb, "");  // table prefix is empty in the map model
        if (!EvalCompare(pb, kv)) {
            success = false;
            break;
        }
    }
    auto rsp = std::make_shared<TxnResponse>();
    rsp->status = Status::OK();
    rsp->success = success;
    if (!success) {
        return rsp;  // nothing applied — atomic
    }
    // Apply every Then-op through the production serializer. ONE atomic
    // transaction = ONE global revision: every write it performs (put or
    // an actual delete) shares a single revision, and the counter moves
    // only when at least one write effect occurred.
    const int64_t txnRevision = revision + 1;
    bool wrote = false;
    for (const auto &op : thenOps) {
        ::etcdserverpb::RequestOp pb;
        op.Build(pb, "");
        if (pb.has_request_put()) {
            const auto &put = pb.request_put();
            auto it = kv.find(put.key());
            if (it == kv.end()) {
                MapRecord rec;
                rec.value = put.value();
                rec.createRevision = txnRevision;
                rec.modRevision = txnRevision;
                rec.version = 1;
                kv[put.key()] = rec;
            } else {
                it->second.value = put.value();
                it->second.modRevision = txnRevision;
                it->second.version += 1;
            }
            wrote = true;
            opLog.push_back("put:" + put.key() + "@" + std::to_string(txnRevision));
        } else if (pb.has_request_delete_range()) {
            const auto &del = pb.request_delete_range();
            const auto it = kv.find(del.key());
            if (it != kv.end()) {
                kv.erase(it);
                wrote = true;
                opLog.push_back("del:" + del.key() + "@" + std::to_string(txnRevision));
            }
        } else if (pb.has_request_range()) {
            const auto &get = pb.request_range();
            TxnOperationResponse opRsp;
            opRsp.status = Status::OK();
            opRsp.operationType = TxnOperationType::OPERATION_GET;
            GetResponse getRsp;
            getRsp.status = Status::OK();
            const auto git = kv.find(get.key());
            if (git != kv.end()) {
                mvccpb::KeyValue out;
                out.set_key(git->first);
                out.set_value(git->second.value);
                out.set_create_revision(git->second.createRevision);
                out.set_mod_revision(git->second.modRevision);
                out.set_version(git->second.version);
                getRsp.kvs.push_back(out);
            }
            opRsp.response = std::move(getRsp);
            rsp->responses.push_back(std::move(opRsp));
        }
    }
    if (wrote) {
        revision = txnRevision;
    }
    return rsp;
}

void MapMetaStoreCore::Seed(const std::string &key, const std::string &value)
{
    std::lock_guard<std::mutex> guard(lock);
    revision += 1;
    auto it = kv.find(key);
    if (it == kv.end()) {
        MapRecord rec;
        rec.value = value;
        rec.createRevision = revision;
        rec.modRevision = revision;
        rec.version = 1;
        kv[key] = rec;
    } else {
        it->second.value = value;
        it->second.modRevision = revision;
        it->second.version += 1;
    }
    opLog.push_back("put:" + key + "@" + std::to_string(revision));
}

bool MapMetaStoreCore::Lookup(const std::string &key, MapRecord &out)
{
    std::lock_guard<std::mutex> guard(lock);
    const auto it = kv.find(key);
    if (it == kv.end()) { return false; }
    out = it->second;
    return true;
}

bool MapMetaStoreCore::Exists(const std::string &key)
{
    std::lock_guard<std::mutex> guard(lock);
    return kv.find(key) != kv.end();
}

int64_t MapMetaStoreCore::LatestRevision()
{
    std::lock_guard<std::mutex> guard(lock);
    return revision;
}

void MapMetaStoreCore::ReleaseGate(const TxnMatcher &matcher, CommitOutcome forced)
{
    std::shared_ptr<litebus::Promise<std::shared_ptr<TxnResponse>>> promise;
    std::vector<meta_store::TxnCompare> compares;
    std::vector<meta_store::TxnOperation> thenOps;
    {
        std::lock_guard<std::mutex> guard(lock);
        for (auto it = gated.begin(); it != gated.end(); ++it) {
            if (matcher(it->signature)) {
                promise = it->promise;
                compares = it->compares;
                thenOps = it->thenOps;
                gated.erase(it);
                break;
            }
        }
    }
    if (promise == nullptr) { return; }
    // The failure-shaped outcomes return BEFORE any apply: a transaction
    // that reports failure must leave the map untouched. Only EVALUATE
    // and APPLY_THEN_FAIL run the real apply (the latter then loses the
    // response — the "committed but response lost" scenario).
    if (forced == CommitOutcome::FAIL_FUTURE) {
        promise->SetFailed(litebus::Status::KERROR);
        return;
    }
    if (forced == CommitOutcome::NULL_RESPONSE) {
        promise->SetValue(std::shared_ptr<TxnResponse>());
        return;
    }
    if (forced == CommitOutcome::STATUS_ERROR) {
        auto rsp = std::make_shared<TxnResponse>();
        rsp->status = Status(StatusCode::FAILED, "injected release status error");
        rsp->success = false;
        promise->SetValue(std::move(rsp));
        return;
    }
    if (forced == CommitOutcome::SUCCESS_FALSE) {
        auto rsp = std::make_shared<TxnResponse>();
        rsp->status = Status::OK();
        rsp->success = false;  // conditions/replay said no — NOTHING applied
        promise->SetValue(std::move(rsp));
        return;
    }
    if (forced == CommitOutcome::APPLY_THEN_FAIL) {
        (void)EvaluateAndApply(compares, thenOps);
        auto rsp = std::make_shared<TxnResponse>();
        rsp->status = Status(StatusCode::FAILED, "response lost after apply");
        rsp->success = false;
        promise->SetValue(std::move(rsp));
        return;
    }
    // EVALUATE: real evaluation NOW (release time) — a late publish
    // genuinely fails its conditions against the moved map here.
    promise->SetValue(EvaluateAndApply(compares, thenOps));
}

void MapMetaStoreCore::FailGate(const TxnMatcher &matcher)
{
    ReleaseGate(matcher, CommitOutcome::FAIL_FUTURE);
}

size_t MapMetaStoreCore::GateCount()
{
    std::lock_guard<std::mutex> guard(lock);
    return gated.size();
}

// ── MapMetaStore ─────────────────────────────────────────────────────────────

litebus::Future<std::shared_ptr<GetResponse>> MapMetaStore::Get(const std::string &key,
                                                                const GetOption &option)
{
    std::shared_ptr<litebus::Promise<std::shared_ptr<GetResponse>>> promise =
        std::make_shared<litebus::Promise<std::shared_ptr<GetResponse>>>();
    litebus::Future<std::shared_ptr<GetResponse>> future = promise->GetFuture();

    GetOutcome forced = GetOutcome::NORMAL;
    {
        std::lock_guard<std::mutex> guard(core_->lock);
        for (auto it = core_->forcedGets.begin(); it != core_->forcedGets.end(); ++it) {
            if (it->key == key && it->prefixRead == option.prefix) {
                forced = it->outcome;
                core_->forcedGets.erase(it);  // one-shot
                break;
            }
        }
    }
    if (forced == GetOutcome::FAIL_FUTURE) {
        promise->SetFailed(litebus::Status::KERROR);
        return future;
    }
    auto rsp = std::make_shared<GetResponse>();
    rsp->status = Status::OK();
    if (forced == GetOutcome::NULL_RESPONSE) {
        promise->SetValue(std::shared_ptr<GetResponse>());
        return future;
    }
    if (forced == GetOutcome::STATUS_ERROR) {
        rsp->status = Status(StatusCode::FAILED, "injected get status error");
        promise->SetValue(std::move(rsp));
        return future;
    }
    {
        std::lock_guard<std::mutex> guard(core_->lock);
        if (option.prefix) {
            int emitted = 0;
            for (const auto &entry : core_->kv) {
                if (entry.first.rfind(key, 0) != 0) { continue; }
                if (option.limit > 0 && emitted >= option.limit) { break; }
                mvccpb::KeyValue out;
                out.set_key(entry.first);
                out.set_value(entry.second.value);
                out.set_create_revision(entry.second.createRevision);
                out.set_mod_revision(entry.second.modRevision);
                out.set_version(entry.second.version);
                rsp->kvs.push_back(out);
                emitted += 1;
            }
        } else {
            const auto it = core_->kv.find(key);
            if (it != core_->kv.end()) {
                mvccpb::KeyValue out;
                out.set_key(it->first);
                out.set_value(it->second.value);
                out.set_create_revision(it->second.createRevision);
                out.set_mod_revision(it->second.modRevision);
                out.set_version(it->second.version);
                rsp->kvs.push_back(out);
                if (forced == GetOutcome::AMBIGUOUS_TWO_KVS) {
                    rsp->kvs.push_back(out);  // duplicate ⇒ kvs>1 ambiguity
                }
            }
        }
    }
    promise->SetValue(std::move(rsp));
    return future;
}

std::shared_ptr<meta_store::TxnTransaction> MapMetaStore::BeginTransaction()
{
    return std::make_shared<MapTxnTransaction>(litebus::AID(), core_);
}

litebus::Future<std::shared_ptr<PutResponse>> MapMetaStore::Put(const std::string &key,
                                                                const std::string &value,
                                                                const PutOption &option)
{
    (void)option;
    core_->Seed(key, value);
    auto rsp = std::make_shared<PutResponse>();
    rsp->status = Status::OK();
    return litebus::Future<std::shared_ptr<PutResponse>>(rsp);
}

litebus::Future<std::shared_ptr<DeleteResponse>> MapMetaStore::Delete(const std::string &key,
                                                                      const DeleteOption &option)
{
    (void)option;
    std::lock_guard<std::mutex> guard(core_->lock);
    const auto it = core_->kv.find(key);
    if (it != core_->kv.end()) {
        core_->kv.erase(it);
        core_->revision += 1;
        core_->opLog.push_back("del:" + key + "@" + std::to_string(core_->revision));
    }
    auto rsp = std::make_shared<DeleteResponse>();
    rsp->status = Status::OK();
    return litebus::Future<std::shared_ptr<DeleteResponse>>(rsp);
}

// ── MapTxnTransaction ────────────────────────────────────────────────────────

TxnSignature MapTxnTransaction::Signature() const
{
    TxnSignature sig;
    for (const auto &cmp : compares) {
        ::etcdserverpb::Compare pb;
        cmp.Build(pb, "");
        sig.compareKeys.push_back(pb.key());
    }
    for (const auto &op : thenOps) {
        ::etcdserverpb::RequestOp pb;
        op.Build(pb, "");
        if (pb.has_request_put()) { sig.thenKeys.push_back(pb.request_put().key()); }
        if (pb.has_request_delete_range()) {
            sig.thenKeys.push_back(pb.request_delete_range().key());
        }
    }
    return sig;
}

litebus::Future<std::shared_ptr<TxnResponse>> MapTxnTransaction::Commit() const
{
    auto promise = std::make_shared<litebus::Promise<std::shared_ptr<TxnResponse>>>();
    litebus::Future<std::shared_ptr<TxnResponse>> future = promise->GetFuture();
    const TxnSignature sig = Signature();

    CommitOutcome forced = CommitOutcome::EVALUATE;
    TxnMatcher gateMatcher;
    {
        std::lock_guard<std::mutex> guard(core_->lock);
        for (auto it = core_->forcedCommits.begin(); it != core_->forcedCommits.end(); ++it) {
            if (it->matcher(sig)) {
                forced = it->outcome;
                core_->forcedCommits.erase(it);  // one-shot
                break;
            }
        }
    }
    if (forced == CommitOutcome::FAIL_FUTURE) {
        promise->SetFailed(litebus::Status::KERROR);
        return future;
    }
    if (forced == CommitOutcome::NULL_RESPONSE) {
        promise->SetValue(std::shared_ptr<TxnResponse>());
        return future;
    }
    if (forced == CommitOutcome::STATUS_ERROR) {
        auto rsp = std::make_shared<TxnResponse>();
        rsp->status = Status(StatusCode::FAILED, "injected txn status error");
        rsp->success = false;
        promise->SetValue(std::move(rsp));
        return future;
    }
    if (forced == CommitOutcome::SUCCESS_FALSE) {
        auto rsp = std::make_shared<TxnResponse>();
        rsp->status = Status::OK();
        rsp->success = false;
        promise->SetValue(std::move(rsp));
        return future;
    }
    if (forced == CommitOutcome::APPLY_THEN_FAIL) {
        // REALLY applies (conditions permitting), then reports failure —
        // the publish-unknown-commit scenario.
        auto applied = core_->EvaluateAndApply(compares, thenOps);
        (void)applied;
        auto rsp = std::make_shared<TxnResponse>();
        rsp->status = Status(StatusCode::FAILED, "response lost after apply");
        rsp->success = false;
        promise->SetValue(std::move(rsp));
        return future;
    }
    {
        // A REGISTERED gate matching this signature parks the transaction:
        // no evaluation yet — the conditions are checked when the test
        // releases the gate.
        std::lock_guard<std::mutex> guard(core_->lock);
        for (auto it = core_->gateMatchers.begin(); it != core_->gateMatchers.end(); ++it) {
            if ((*it)(sig)) {
                core_->gateMatchers.erase(it);  // one-shot
                MapMetaStoreCore::Gated gated;
                gated.signature = sig;
                gated.compares = compares;
                gated.thenOps = thenOps;
                gated.promise = promise;
                core_->gated.push_back(std::move(gated));
                return future;
            }
        }
    }
    // Default: real evaluation now.
    promise->SetValue(core_->EvaluateAndApply(compares, thenOps));
    return future;
}

// ── MapDataStore ─────────────────────────────────────────────────────────────

Status MapDataStore::Set(const std::string &key, const std::string &val)
{
    std::lock_guard<std::mutex> guard(lock);
    setCalls[key] += 1;
    if (forceNextSet.has_value()) {
        const auto action = *forceNextSet;
        forceNextSet.reset();
        if (action == DsAction::TIMEOUT || action == DsAction::NOT_FOUND_FOLDED) {
            return Status(StatusCode::BP_DATASYSTEM_ERROR, "injected set failure (one-shot)");
        }
    }
    const auto it = forcedSet.find(key);
    if (it != forcedSet.end()) {
        if (it->second == DsAction::TIMEOUT) {
            return Status(StatusCode::BP_DATASYSTEM_ERROR, "injected set timeout (SDK deadline)");
        }
    }
    data[key] = val;
    return Status::OK();
}

Status MapDataStore::Get(const std::string &key, std::string &val)
{
    std::lock_guard<std::mutex> guard(lock);
    getCalls[key] += 1;
    const auto it = data.find(key);
    if (it == data.end()) {
        return Status(StatusCode::BP_DATASYSTEM_ERROR, "not found (folded by the DS client)");
    }
    val = it->second;
    return Status::OK();
}

Status MapDataStore::Del(const std::string &key)
{
    std::lock_guard<std::mutex> guard(lock);
    delCalls[key] += 1;
    if (forceNextDel.has_value()) {
        const auto action = *forceNextDel;
        forceNextDel.reset();
        if (action == DsAction::TIMEOUT || action == DsAction::NOT_FOUND_FOLDED) {
            return Status(StatusCode::BP_DATASYSTEM_ERROR, "injected del failure (one-shot, folded)");
        }
    }
    const auto it = forcedDel.find(key);
    if (it != forcedDel.end()) {
        if (it->second == DsAction::TIMEOUT || it->second == DsAction::NOT_FOUND_FOLDED) {
            // The production client collapses EVERY raw rc into this one
            // code — a delete-of-absent is indistinguishable from an error.
            return Status(StatusCode::BP_DATASYSTEM_ERROR, "injected del failure (folded)");
        }
    }
    data.erase(key);
    return Status::OK();
}

int MapDataStore::SetCount(const std::string &key)
{
    std::lock_guard<std::mutex> guard(lock);
    const auto it = setCalls.find(key);
    return it == setCalls.end() ? 0 : it->second;
}

int MapDataStore::DelCount(const std::string &key)
{
    std::lock_guard<std::mutex> guard(lock);
    const auto it = delCalls.find(key);
    return it == delCalls.end() ? 0 : it->second;
}

int MapDataStore::TotalSetCalls()
{
    std::lock_guard<std::mutex> guard(lock);
    int total = 0;
    for (const auto &entry : setCalls) { total += entry.second; }
    return total;
}

int MapDataStore::TotalDelCalls()
{
    std::lock_guard<std::mutex> guard(lock);
    int total = 0;
    for (const auto &entry : delCalls) { total += entry.second; }
    return total;
}

}  // namespace functionsystem::function_proxy::test
