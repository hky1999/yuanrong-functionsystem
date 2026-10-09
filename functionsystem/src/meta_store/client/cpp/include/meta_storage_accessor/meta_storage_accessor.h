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

#ifndef COMMON_META_STORAGE_ACCESSOR_H
#define COMMON_META_STORAGE_ACCESSOR_H

#include <actor/actor.hpp>
#include <async/option.hpp>
#include <string>
#include <unordered_map>

#include "meta_storage_accessor/lease_actor.h"
#include "meta_store_client/meta_store_client.h"
#include "common/status/status.h"
#include "common/utils/time_trigger.h"

namespace functionsystem {
struct SnapshotSyncResponse {
    Status status = Status::OK();
    std::vector<WatchEvent> events;
    int64_t revision = 0;
};

class MetaStorageAccessor {
public:
    explicit MetaStorageAccessor(const std::shared_ptr<MetaStoreClient> &metaClient);
    virtual ~MetaStorageAccessor();
    /**
     * Register a meta storage watcher.
     * @param key the key of BusProxy.
     * @param option Watch option.
     * @param observer observer callback function.
     * @param syncer syncer callback function.
     */
    litebus::Future<std::shared_ptr<Watcher>> RegisterObserver(
        const std::string &key, const WatchOption &option,
        const std::function<bool(const std::vector<WatchEvent> &events, bool)> &observer,
        const SyncerFunction &syncer);

    /**
     * Put a key-value without TTL asynchronous. The key-value exists until it is being deleted.
     * @param key the key of BusProxy.
     * @param value the value to update.
     * @return
     */
    virtual litebus::Future<Status> Put(const std::string &key, const std::string &value);

    /**
     * Put a key-value with TTL asynchronous. The key-value will be deleted if the meta storage doesn't receive
     * keepalive message within the TTL.
     * @param key the key of BusProxy.
     * @param value the value to update.
     * @param ttl time to live value, millisecond.
     * @return
     */
    virtual litebus::Future<Status> PutWithLease(const std::string &key, const std::string &value, const int ttl);

    /**
     * Revoke the lease ID according the BusProxy key.
     * @param key the key of BusProxy.
     * @return
     */
    virtual litebus::Future<Status> Revoke(const std::string &key);

    /**
     * Transactional put multiple key-values without lease.
     * All keys will be written atomically in a single transaction.
     * @param kvs Vector of key-value pairs to write.
     * @return
     */
    virtual litebus::Future<Status> Txn(const std::vector<std::pair<std::string, std::string>>& kvs);

    /**
     * Transactional put multiple key-values with a shared lease.
     * All keys will be written atomically in a single transaction and share one lease.
     * @param groupKey Identifier for this group of keys (e.g., instanceID), used for revoke.
     * @param kvs Vector of key-value pairs to write.
     * @param ttl Time to live in milliseconds.
     * @return
     */
    virtual litebus::Future<Status> TxnWithLease(
        const std::string& groupKey,
        const std::vector<std::pair<std::string, std::string>>& kvs,
        const int ttl);

    /**
     * Revoke the lease for a group of keys.
     * @param groupKey The group key used in TxnWithLease.
     * @return
     */
    virtual litebus::Future<Status> RevokeGroup(const std::string& groupKey);

    /**
     * Get a value according to the key synchronous.
     * @param key the key of BusProxy.
     * @return
     */
    virtual litebus::Option<std::string> Get(const std::string &key);

    /**
     * Get a value according to the key async.
     * @param key the key of BusProxy.
     * @return
     */
    virtual litebus::Future<litebus::Option<std::string>> AsyncGet(const std::string &key);

    /**
     * Get a key-value res according to the key's prefix synchronous.
     * @param prefix
     * @return
     */
    virtual litebus::Option<std::pair<std::string, std::string>> GetWithPrefix(const std::string &prefix);

    /**
     * Get all key-value res according to the key's prefix synchronous.
     * @param prefix
     * @return
     */
    virtual litebus::Option<std::vector<std::pair<std::string, std::string>>> GetAllWithPrefix(
        const std::string &prefix);

    /**
     * Delete a value according to the key asynchronous.
     * @param key the key of BusProxy.
     * @return
     */
    virtual litebus::Future<Status> Delete(const std::string &key);

    /**
     * Delete a value according to the key asynchronous.
     * @param key the key of BusProxy.
     * @param isPrefix delete all values with key as prefix.
     * @return
     */
    virtual litebus::Future<Status> Delete(const std::string &key, bool isPrefix);

    std::pair<std::vector<WatchEvent>, int64_t> Sync(const std::string &key, bool isPrefix);
    SnapshotSyncResponse SyncWithStatus(const std::string &key, bool isPrefix);
    std::shared_ptr<MetaStoreClient> GetMetaClient()
    {
        return metaClient_;
    }

private:
    std::shared_ptr<MetaStoreClient> metaClient_;
    std::shared_ptr<LeaseActor> leaseActor_;
};
}  // namespace functionsystem

#endif  // COMMON_META_STORAGE_ACCESSOR_H
