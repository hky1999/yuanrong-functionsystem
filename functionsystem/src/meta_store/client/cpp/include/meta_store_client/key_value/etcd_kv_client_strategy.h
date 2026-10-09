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

#ifndef COMMON_META_STORE_CLIENT_KEY_VALUE_ETCD_KV_CLIENT_STRATEGY_H
#define COMMON_META_STORE_CLIENT_KEY_VALUE_ETCD_KV_CLIENT_STRATEGY_H

#include <chrono>
#include <mutex>
#include <unordered_set>

#include "meta_store_client/key_value/kv_client_strategy.h"
#include "meta_store_client/key_value/prefix_range_cursor.h"
#include "common/rpc/client/grpc_client.h"

namespace functionsystem::meta_store {
class EtcdKvClientStrategy : public KvClientStrategy {
public:
    EtcdKvClientStrategy(const std::string &name, const std::string &address,
                         const MetaStoreTimeoutOption &timeoutOption, const GrpcSslConfig &sslConfig = {},
                         const std::string &etcdTablePrefix = "");
    ~EtcdKvClientStrategy() override = default;
    void Init() override;
    void Finalize() override;

public:
    litebus::Future<std::shared_ptr<PutResponse>> Put(const std::string &key, const std::string &value,
                                                      const PutOption &option) override;
    litebus::Future<std::shared_ptr<DeleteResponse>> Delete(const std::string &key,
                                                            const DeleteOption &option) override;
    litebus::Future<std::shared_ptr<GetResponse>> Get(const std::string &key, const GetOption &option) override;

    litebus::Future<std::shared_ptr<TxnResponse>> CommitTxn(const ::etcdserverpb::TxnRequest &request, bool) override;

    litebus::Future<std::shared_ptr<::etcdserverpb::TxnResponse>> CommitWithReq(
        const ::etcdserverpb::TxnRequest &request, bool) override;

    litebus::Future<std::shared_ptr<Watcher>> Watch(const std::string &key, const WatchOption &option,
                                                    const ObserverFunction &observer, const SyncerFunction &syncer,
                                                    const std::shared_ptr<WatchRecord> &reconnectRecord) override;

    litebus::Future<std::shared_ptr<Watcher>> GetAndWatch(const std::string &key, const WatchOption &option,
                                                          const ObserverFunction &observer,
                                                          const SyncerFunction &syncer,
                                                          const std::shared_ptr<WatchRecord> &reconnectRecord) override;

    litebus::Future<std::shared_ptr<Watcher>> GetAndWatchWithHandler(
        const std::string &key, const WatchOption &option, const GetAndWatchHandlers &handlers,
        const std::shared_ptr<WatchRecord> &reconnectRecord) override;

    void CancelWatch(int64_t watchId) override;

    litebus::Future<bool> IsConnected() override;

    void OnAddressUpdated(const std::string &address) override;

public:
    litebus::Future<std::shared_ptr<::etcdserverpb::TxnResponse>> CommitRaw(const ::etcdserverpb::TxnRequest &request);

    bool TryErr();

    bool ReconnectWatch() override;

    litebus::Future<bool> ReconnectGrpcWatch();

    Status Cancel(const std::shared_ptr<WatchResponse> &rsp);
    [[maybe_unused]] std::vector<std::shared_ptr<WatchRecord>> GetRecords()
    {
        return records_;
    };

private:
    struct PrefixRead {
        PrefixRead(const etcdserverpb::RangeRequest &initial, uint32_t pageSize, uint32_t timeoutSeconds)
            : request(initial), cursor(initial.key(), initial.range_end(), pageSize),
              deadline(std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSeconds))
        {
            request.set_limit(pageSize);
        }
        etcdserverpb::RangeRequest request;
        PrefixRangeCursor cursor;
        std::chrono::steady_clock::time_point deadline;
        std::shared_ptr<GetResponse> output = std::make_shared<GetResponse>();
        litebus::Promise<std::shared_ptr<GetResponse>> promise;
    };

    bool ReadPrefixPage(const std::shared_ptr<PrefixRead> &read);
    bool OnPrefixPage(const std::shared_ptr<PrefixRead> &read,
                      const std::shared_ptr<etcdserverpb::RangeResponse> &page, const Status &status);
    void FailPrefixRead(const std::shared_ptr<PrefixRead> &read, const Status &status);

    void DoPut(const std::shared_ptr<litebus::Promise<functionsystem::Status>> &promise,
               const etcdserverpb::PutRequest &request, const std::shared_ptr<etcdserverpb::PutResponse> &response,
               int retryTimes);
    void DoGet(const std::shared_ptr<litebus::Promise<functionsystem::Status>> &promise,
               const etcdserverpb::RangeRequest &request, const std::shared_ptr<etcdserverpb::RangeResponse> &response,
               int retryTimes,
               std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max());
    void DoCommit(const std::shared_ptr<litebus::Promise<functionsystem::Status>> &promise,
                  const etcdserverpb::TxnRequest &request, const std::shared_ptr<etcdserverpb::TxnResponse> &response,
                  int retryTimes);
    void DoDelete(const std::shared_ptr<litebus::Promise<functionsystem::Status>> &promise,
                  const etcdserverpb::DeleteRangeRequest &request,
                  const std::shared_ptr<etcdserverpb::DeleteRangeResponse> &response, int retryTimes);

    void OnWatch();

    litebus::Future<std::shared_ptr<Watcher>> RetryWatch(const std::string &key, const WatchOption &option,
                                                         const ObserverFunction &observer, const SyncerFunction &syncer,
                                                         const std::shared_ptr<WatchRecord> &reconnectRecord);

    litebus::Future<Status> OnCancel(const std::shared_ptr<WatchResponse> &rsp) override;

    Status OnCreate(const std::shared_ptr<WatchResponse> &response);

private:
    std::unique_ptr<GrpcClient<etcdserverpb::KV>> kvClient_;
    std::mutex prefixReadsMutex_;
    std::unordered_set<std::shared_ptr<PrefixRead>> prefixReads_;

private:
    std::atomic<bool> running_ = true;
    std::unique_ptr<std::thread> watchThread_;
    std::unique_ptr<::grpc::ClientContext> watchContext_;
    std::unique_ptr<::grpc::ClientReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>> watchStream_;

    std::deque<std::shared_ptr<WatchRecord>> pendingRecords_;
};
}  // namespace functionsystem::meta_store

#endif  // COMMON_META_STORE_CLIENT_KEY_VALUE_ETCD_KV_CLIENT_STRATEGY_H
