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

#include "meta_storage_accessor.h"

namespace functionsystem {
MetaStorageAccessor::MetaStorageAccessor(const std::shared_ptr<MetaStoreClient> &metaClient) : metaClient_(metaClient)
{
    auto uuid = litebus::uuid_generator::UUID::GetRandomUUID();
    leaseActor_ = std::make_shared<LeaseActor>("lease-actor-" + uuid.ToString(), metaClient_);
    litebus::Spawn(leaseActor_);
}

MetaStorageAccessor::~MetaStorageAccessor()
{
    litebus::Terminate(leaseActor_->GetAID());
    litebus::Await(leaseActor_->GetAID());
}

litebus::Future<std::shared_ptr<Watcher>> MetaStorageAccessor::RegisterObserver(
    const std::string &key, const WatchOption &option,
    const std::function<bool(const std::vector<WatchEvent> &events, bool synced)> &observer,
    const SyncerFunction &syncer)
{
    YRLOG_DEBUG("observer({}) watch option: prefix({}), prevKv({}), revision({})", key, option.prefix, option.prevKv,
                option.revision);
    ASSERT_IF_NULL(metaClient_);
    return metaClient_->GetAndWatch(key, option, observer, syncer);
}

std::pair<std::vector<WatchEvent>, int64_t> MetaStorageAccessor::Sync(const std::string &key, bool isPrefix)
{
    auto result = SyncWithStatus(key, isPrefix);
    return { std::move(result.events), result.revision };
}

SnapshotSyncResponse MetaStorageAccessor::SyncWithStatus(const std::string &key, bool isPrefix)
{
    YRLOG_DEBUG("sync from meta store, key: {}", key);
    GetOption opts{
        .prefix = isPrefix,
    };

    SnapshotSyncResponse result;
    ASSERT_IF_NULL(metaClient_);
    auto future = metaClient_->Get(key, opts);
    future.Wait();
    if (future.IsError()) {
        result.status = Status(FAILED, "failed meta store snapshot future");
        return result;
    }
    auto getResponse = future.Get();
    if (getResponse == nullptr || getResponse->status.IsError()) {
        result.status = getResponse == nullptr ? Status(POINTER_IS_NULL, "missing meta store snapshot")
                                               : getResponse->status;
        return result;
    }
    result.revision = getResponse->header.revision;
    result.events.reserve(getResponse->kvs.size());
    for (auto &kv : getResponse->kvs) {
        WatchEvent event{ .eventType = EVENT_TYPE_PUT, .kv = std::move(kv), .prevKv = {} };
        result.events.emplace_back(std::move(event));
    }
    return result;
}

litebus::Future<Status> MetaStorageAccessor::Put(const std::string &key, const std::string &value)
{
    YRLOG_DEBUG("put into meta store, key: {}", key);
    ASSERT_IF_NULL(metaClient_);
    return metaClient_->Put(key, value, {}).Then([key](const std::shared_ptr<PutResponse> &putResponse) {
        if (putResponse->status.IsError()) {
            YRLOG_ERROR("failed to put key {} using meta client, error: {}", key, putResponse->status.GetMessage());
            return Status(StatusCode::BP_META_STORAGE_PUT_ERROR, "failed to put key: " + key);
        }
        return Status::OK();
    });
}

litebus::Future<Status> MetaStorageAccessor::PutWithLease(const std::string &key, const std::string &value,
                                                          const int ttl)
{
    return litebus::Async(leaseActor_->GetAID(), &LeaseActor::PutWithLease, key, value, ttl);
}

litebus::Future<Status> MetaStorageAccessor::Revoke(const std::string &key)
{
    return litebus::Async(leaseActor_->GetAID(), &LeaseActor::Revoke, key);
}

litebus::Option<std::string> MetaStorageAccessor::Get(const std::string &key)
{
    YRLOG_DEBUG("get from meta store, key: {}", key);
    ASSERT_IF_NULL(metaClient_);
    auto getResponse = metaClient_->Get(key, {}).Get();
    if (getResponse->kvs.empty()) {
        YRLOG_ERROR("failed to get key {} from meta storage, result is empty", key);
        return {};
    }

    // Only one result is obtained from meta storage based on the key.
    return getResponse->kvs.front().value();
}

litebus::Future<litebus::Option<std::string>> MetaStorageAccessor::AsyncGet(const std::string &key)
{
    YRLOG_DEBUG("get from meta store, key: {}", key);
    ASSERT_IF_NULL(metaClient_);
    return metaClient_->Get(key, {}).Then(
        [](const std::shared_ptr<GetResponse> &getResponse) -> litebus::Future<litebus::Option<std::string>> {
            if (getResponse->kvs.empty()) {
                return litebus::None();
            }
            return getResponse->kvs.front().value();
        });
}

litebus::Option<std::pair<std::string, std::string>> MetaStorageAccessor::GetWithPrefix(const std::string &prefix)
{
    YRLOG_DEBUG("get k-v with prefix from meta store, prefix: {}", prefix);

    GetOption opts{
        .prefix = true,
    };
    ASSERT_IF_NULL(metaClient_);
    auto getResponse = metaClient_->Get(prefix, opts).Get();
    if (getResponse->kvs.empty()) {
        YRLOG_ERROR("failed to get with prefix {} from meta storage, result is empty", prefix);
        return {};
    }

    // Only one result is obtained from meta storage based on the key.
    return std::make_pair(getResponse->kvs.front().key(), getResponse->kvs.front().value());
}

litebus::Option<std::vector<std::pair<std::string, std::string>>> MetaStorageAccessor::GetAllWithPrefix(
    const std::string &prefix)
{
    YRLOG_DEBUG("get all k-v with prefix from meta store, prefix: {}", prefix);

    GetOption opts;
    opts.prefix = true;
    opts.sortOrder = SortOrder::ASCEND;
    opts.sortTarget = SortTarget::MODIFY;
    ASSERT_IF_NULL(metaClient_);
    auto getResponse = metaClient_->Get(prefix, opts).Get();
    if (getResponse->kvs.empty()) {
        YRLOG_ERROR("failed to get with prefix {} from meta storage, result is empty", prefix);
        return {};
    }
    std::vector<std::pair<std::string, std::string>> result;
    for (auto iter = getResponse->kvs.begin(); iter != getResponse->kvs.end(); ++iter) {
        result.push_back(std::make_pair(iter->key(), iter->value()));
        YRLOG_DEBUG("success to get key-value, kv.key({}) from meta storage", iter->key());
    }
    return result;
}

litebus::Future<Status> MetaStorageAccessor::Delete(const std::string &key)
{
    return Delete(key, false);
}

litebus::Future<Status> MetaStorageAccessor::Delete(const std::string &key, bool isPrefix)
{
    YRLOG_DEBUG("delete from meta store, key: {}, is prefix: {}", key, isPrefix);
    ASSERT_IF_NULL(metaClient_);
    return metaClient_->Delete(key, { false, isPrefix })
        .Then([key](const litebus::Future<std::shared_ptr<DeleteResponse>> &deleteResponseFuture) {
            if (deleteResponseFuture.IsError()) {
                YRLOG_ERROR("failed to delete key {} using meta client, error: {}", key,
                            deleteResponseFuture.GetErrorCode());
                return Status(StatusCode::BP_META_STORAGE_DELETE_ERROR, "key: " + key);
            }
            return Status::OK();
        });
}

litebus::Future<Status> MetaStorageAccessor::Txn(const std::vector<std::pair<std::string, std::string>>& kvs)
{
    YRLOG_DEBUG("txn put {} key-values without lease", kvs.size());
    ASSERT_IF_NULL(metaClient_);
    auto txn = metaClient_->BeginTransaction();
    for (const auto& [key, value] : kvs) {
        txn->Then(meta_store::TxnOperation::Create(key, value, PutOption{}));
    }
    return txn->Commit().Then([kvsSize = kvs.size()](const std::shared_ptr<TxnResponse>& response) {
        if (response->status.IsError()) {
            YRLOG_ERROR("failed to txn put {} keys, error: {}", kvsSize, response->status.GetMessage());
            return Status(StatusCode::BP_META_STORAGE_PUT_ERROR, "failed to txn put keys");
        }
        return Status::OK();
    });
}

litebus::Future<Status> MetaStorageAccessor::TxnWithLease(
    const std::string& groupKey,
    const std::vector<std::pair<std::string, std::string>>& kvs,
    const int ttl)
{
    return litebus::Async(leaseActor_->GetAID(), &LeaseActor::TxnWithLease, groupKey, kvs, ttl);
}

litebus::Future<Status> MetaStorageAccessor::RevokeGroup(const std::string& groupKey)
{
    return litebus::Async(leaseActor_->GetAID(), &LeaseActor::RevokeGroup, groupKey);
}

}  // namespace functionsystem
