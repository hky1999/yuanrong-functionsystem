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

#include "etcd_kv_client_strategy.h"

#include <algorithm>

#include "async/asyncafter.hpp"
#include "async/defer.hpp"
#include "meta_store_client/utils/etcd_util.h"
#include "common/utils/random_number.h"

namespace functionsystem::meta_store {
const uint64_t RETRY_INTERVAL = 5000;
const uint64_t CANCEL_TIMEOUT = 1000;  // ms
EtcdKvClientStrategy::EtcdKvClientStrategy(const std::string &name, const std::string &address,
                                           const MetaStoreTimeoutOption &timeoutOption, const GrpcSslConfig &sslConfig,
                                           const std::string &etcdTablePrefix)
    : KvClientStrategy(name, address, timeoutOption, etcdTablePrefix)
{
    kvClient_ = GrpcClient<etcdserverpb::KV>::CreateGrpcClient(address, sslConfig);
    ASSERT_IF_NULL(kvClient_);
}

void EtcdKvClientStrategy::Init()
{
}

void EtcdKvClientStrategy::Finalize()
{
    running_ = false;
    std::unordered_set<std::shared_ptr<PrefixRead>> reads;
    {
        std::lock_guard<std::mutex> lock(prefixReadsMutex_);
        reads.swap(prefixReads_);
    }
    for (const auto &read : reads) {
        auto output = std::make_shared<GetResponse>();
        output->status = Status(GRPC_CANCELLED, "etcd prefix read client stopped");
        read->output.reset();
        read->promise.SetValue(output);
    }

    etcdserverpb::WatchRequest request;
    for (const auto &record : readyRecords_) {
        ::etcdserverpb::WatchCancelRequest *args = request.mutable_cancel_request();
        args->set_watch_id(record.first);
        (void)watchStream_->Write(request);
    }
    readyRecords_.clear();

    records_.clear();
    pendingRecords_.clear();

    if (watchContext_ != nullptr) {
        watchContext_->TryCancel();
    }

    if (watchThread_ != nullptr) {
        watchThread_->join();
    }
}

litebus::Future<std::shared_ptr<PutResponse>> EtcdKvClientStrategy::Put(const std::string &key,
                                                                        const std::string &value,
                                                                        const PutOption &option)
{
    etcdserverpb::PutRequest request;
    request.set_key(GetKeyWithPrefix(key));
    request.set_value(value);
    request.set_lease(option.leaseId);
    request.set_prev_kv(option.prevKv);

    auto response = std::make_shared<etcdserverpb::PutResponse>();
    std::function<std::shared_ptr<functionsystem::PutResponse>(const functionsystem::Status &)> then =
        [request, response](const functionsystem::Status &status) -> std::shared_ptr<functionsystem::PutResponse> {
        auto output = std::make_shared<functionsystem::PutResponse>();
        if (status.IsOk()) {
            Transform(output->header, response->header());
            if (response->has_prev_kv()) {
                output->prevKv = response->prev_kv();
            }
        } else {
            YRLOG_WARN("Failed to Put {}:{}", request.key(), request.value());
            output->status = std::move(status);
        }
        return output;
    };
    auto promise = std::make_shared<litebus::Promise<functionsystem::Status>>();
    DoPut(promise, request, response, 1);  // do retry
    return promise->GetFuture().Then(then);
}

void EtcdKvClientStrategy::DoPut(const std::shared_ptr<litebus::Promise<functionsystem::Status>> &promise,
                                 const etcdserverpb::PutRequest &request,
                                 const std::shared_ptr<etcdserverpb::PutResponse> &response, int retryTimes)
{
    if (healthyStatus_.IsError()) {
        promise->SetValue(
            Status(healthyStatus_.StatusCode(), "[fallbreak] failed to call Put: " + healthyStatus_.GetMessage()));
        return;
    }

    std::function<bool(const functionsystem::Status &status)> then =
        [aid(GetAID()), promise, request, response, retryTimes,
         timeoutOption(timeoutOption_)](const functionsystem::Status &status) -> bool {
        if (status.IsOk()) {
            promise->SetValue(status);
            return true;
        }

        if (retryTimes == timeoutOption.operationRetryTimes) {
            YRLOG_ERROR("Put over times: {}", status.ToString());
            promise->SetValue(status);
        } else {
            YRLOG_WARN("Put error: {}, begin to retry", status.ToString());
            auto nextSleepTime = GenerateRandomNumber(timeoutOption.operationRetryIntervalLowerBound * retryTimes,
                                                      timeoutOption.operationRetryIntervalUpperBound * retryTimes);
            (void)litebus::AsyncAfter(nextSleepTime, aid, &EtcdKvClientStrategy::DoPut, promise, request, response,
                                      retryTimes + 1);
        }
        return false;
    };
    (void)kvClient_
        ->CallAsyncX("Put", request, response.get(), &etcdserverpb::KV::Stub::AsyncPut,
                     timeoutOption_.grpcTimeout * retryTimes)
        .Then(then);
}

litebus::Future<std::shared_ptr<DeleteResponse>> EtcdKvClientStrategy::Delete(const std::string &key,
                                                                              const DeleteOption &option)
{
    // makeup Delete request
    etcdserverpb::DeleteRangeRequest request;
    std::string realKey = GetKeyWithPrefix(key);
    request.set_key(realKey);
    if (option.prefix) {  // prefix
        request.set_range_end(StringPlusOne(realKey));
    }
    request.set_prev_kv(option.prevKv);

    auto response = std::make_shared<etcdserverpb::DeleteRangeResponse>();
    std::function<std::shared_ptr<DeleteResponse>(const functionsystem::Status &)> then =
        [request, response](const functionsystem::Status &status) -> std::shared_ptr<DeleteResponse> {
        auto output = std::make_shared<DeleteResponse>();

        if (status.IsOk()) {
            Transform(output->header, response->header());
            YRLOG_DEBUG("Success Delete {}, {} key-value is deleted", request.key(), response->deleted());
            output->deleted = response->deleted();
            for (int i = 0; i < response->prev_kvs_size(); i++) {
                (void)output->prevKvs.emplace_back(response->prev_kvs(i));
            }
        } else {
            output->status = std::move(status);
        }

        return output;
    };

    auto promise = std::make_shared<litebus::Promise<functionsystem::Status>>();
    DoDelete(promise, request, response, 1);  // do retry
    return promise->GetFuture().Then(then);
}

void EtcdKvClientStrategy::DoDelete(const std::shared_ptr<litebus::Promise<functionsystem::Status>> &promise,
                                    const etcdserverpb::DeleteRangeRequest &request,
                                    const std::shared_ptr<etcdserverpb::DeleteRangeResponse> &response, int retryTimes)
{
    if (healthyStatus_.IsError()) {
        promise->SetValue(
            Status(healthyStatus_.StatusCode(), "[fallbreak] failed to call Delete: " + healthyStatus_.GetMessage()));
        return;
    }

    std::function<bool(const functionsystem::Status &)> then =
        [aid(GetAID()), promise, request, response, retryTimes,
         timeoutOption(timeoutOption_)](const functionsystem::Status &status) -> bool {
        if (status.IsOk()) {
            promise->SetValue(status);
        } else if (retryTimes == timeoutOption.operationRetryTimes) {
            YRLOG_ERROR("Fail to Delete {} after {} times, because: {}", request.key(),
                        timeoutOption.operationRetryTimes, status.ToString());
            promise->SetValue(status);
        } else {
            YRLOG_WARN("Fail to Delete {} because: {}, begin to retry for the {}/{} times", request.key(),
                       status.ToString(), retryTimes, timeoutOption.operationRetryTimes);
            auto nextSleepTime = GenerateRandomNumber(timeoutOption.operationRetryIntervalLowerBound * retryTimes,
                                                      timeoutOption.operationRetryIntervalUpperBound * retryTimes);
            (void)litebus::AsyncAfter(nextSleepTime, aid, &EtcdKvClientStrategy::DoDelete, promise, request, response,
                                      retryTimes + 1);
        }
        return true;
    };

    (void)kvClient_
        ->CallAsyncX("Delete", request, response.get(), &etcdserverpb::KV::Stub::AsyncDeleteRange,
                     timeoutOption_.grpcTimeout * retryTimes)
        .Then(then);
}

litebus::Future<std::shared_ptr<GetResponse>> EtcdKvClientStrategy::Get(const std::string &key, const GetOption &option)
{
    // makeup Get request
    etcdserverpb::RangeRequest request;
    BuildRangeRequest(request, key, option);

    if (option.prefix && option.limit == 0 && !option.countOnly && option.sortOrder == SortOrder::NONE
        && option.sortTarget == SortTarget::KEY) {
        if (timeoutOption_.prefixReadPageSize == 0 || timeoutOption_.prefixReadPageSize > 4096
            || timeoutOption_.prefixReadTimeoutSeconds == 0 || timeoutOption_.prefixReadTimeoutSeconds > 3600) {
            auto output = std::make_shared<GetResponse>();
            output->status = Status(PARAMETER_ERROR, "invalid etcd prefix read budget");
            return output;
        }
        auto read = std::make_shared<PrefixRead>(request, timeoutOption_.prefixReadPageSize,
                                                 timeoutOption_.prefixReadTimeoutSeconds);
        auto future = read->promise.GetFuture();
        {
            std::lock_guard<std::mutex> lock(prefixReadsMutex_);
            if (!running_) {
                auto output = std::make_shared<GetResponse>();
                output->status = Status(GRPC_CANCELLED, "etcd prefix read client stopped");
                return output;
            }
            prefixReads_.emplace(read);
        }
        (void)litebus::Async(GetAID(), &EtcdKvClientStrategy::ReadPrefixPage, read);
        return future;
    }

    auto response = std::make_shared<etcdserverpb::RangeResponse>();
    std::function<std::shared_ptr<GetResponse>(const functionsystem::Status &)> then =
        [request, response](const functionsystem::Status &status) -> std::shared_ptr<GetResponse> {
        auto output = std::make_shared<GetResponse>();
        if (status.IsOk()) {
            Transform(output->header, response->header());
            YRLOG_DEBUG("Success to Get {}, {} key-value is found", request.key(), response->kvs_size());
            for (int i = 0; i < response->kvs_size(); i++) {
                (void)output->kvs.emplace_back(response->kvs(i));
            }
            output->count = response->count();
        } else {
            output->status = std::move(status);
        }
        return output;
    };
    auto promise = std::make_shared<litebus::Promise<functionsystem::Status>>();
    DoGet(promise, request, response, 1);  // do retry
    return promise->GetFuture().Then(then);
}

bool EtcdKvClientStrategy::ReadPrefixPage(const std::shared_ptr<PrefixRead> &read)
{
    if (read->promise.GetFuture().IsOK() || read->promise.GetFuture().IsError()) {
        return false;
    }
    if (!running_) {
        FailPrefixRead(read, Status(GRPC_CANCELLED, "etcd prefix read client stopped"));
        return false;
    }
    auto page = std::make_shared<etcdserverpb::RangeResponse>();
    auto promise = std::make_shared<litebus::Promise<Status>>();
    (void)promise->GetFuture().Then(litebus::Defer(GetAID(), &EtcdKvClientStrategy::OnPrefixPage,
                                                 read, page, std::placeholders::_1));
    DoGet(promise, read->request, page, 1, read->deadline);
    return true;
}

void EtcdKvClientStrategy::FailPrefixRead(const std::shared_ptr<PrefixRead> &read, const Status &status)
{
    auto output = std::make_shared<GetResponse>();
    output->status = status;
    read->output.reset();
    read->promise.SetValue(output);
    std::lock_guard<std::mutex> lock(prefixReadsMutex_);
    prefixReads_.erase(read);
}

bool EtcdKvClientStrategy::OnPrefixPage(const std::shared_ptr<PrefixRead> &read,
                                        const std::shared_ptr<etcdserverpb::RangeResponse> &page, const Status &status)
{
    if (read->promise.GetFuture().IsOK() || read->promise.GetFuture().IsError()) {
        return false;
    }
    if (status.IsError()) {
        FailPrefixRead(read, status);
        return false;
    }
    if (std::chrono::steady_clock::now() >= read->deadline) {
        FailPrefixRead(read, Status(GRPC_DEADLINE_EXCEEDED, "etcd prefix snapshot deadline exceeded"));
        return false;
    }
    std::vector<std::string> keys;
    keys.reserve(page->kvs_size());
    for (const auto &kv : page->kvs()) {
        keys.emplace_back(kv.key());
    }
    bool first = read->cursor.Revision() == 0;
    if (!read->cursor.Advance(page->header().revision(), page->header().cluster_id(), keys, page->more())) {
        FailPrefixRead(read, Status(FAILED, "invalid etcd prefix snapshot page"));
        return false;
    }
    if (first) {
        Transform(read->output->header, page->header());
        read->output->count = page->count();
    }
    for (auto &kv : *page->mutable_kvs()) {
        read->output->kvs.emplace_back(std::move(kv));
    }
    if (!page->more()) {
        if (read->output->count < 0
            || static_cast<uint64_t>(read->output->count) != read->output->kvs.size()) {
            FailPrefixRead(read, Status(FAILED, "incomplete etcd prefix snapshot"));
            return false;
        }
        read->promise.SetValue(read->output);
        std::lock_guard<std::mutex> lock(prefixReadsMutex_);
        prefixReads_.erase(read);
        return true;
    }
    read->request.set_key(read->cursor.Key());
    read->request.set_revision(read->cursor.Revision());
    // Queue the next page rather than retaining a recursive response chain.
    (void)litebus::Async(GetAID(), &EtcdKvClientStrategy::ReadPrefixPage, read);
    return true;
}

void EtcdKvClientStrategy::DoGet(const std::shared_ptr<litebus::Promise<functionsystem::Status>> &promise,
                                 const etcdserverpb::RangeRequest &request,
                                 const std::shared_ptr<etcdserverpb::RangeResponse> &response, int retryTimes,
                                 std::chrono::steady_clock::time_point deadline)
{
    if (std::chrono::steady_clock::now() >= deadline) {
        promise->SetValue(Status(GRPC_DEADLINE_EXCEEDED, "etcd prefix snapshot deadline exceeded"));
        return;
    }
    if (healthyStatus_.IsError()) {
        promise->SetValue(
            Status(healthyStatus_.StatusCode(), "[fallbreak] failed to call Get: " + healthyStatus_.GetMessage()));
        return;
    }

    std::function<bool(const functionsystem::Status &status)> then =
        [aid(GetAID()), promise, request, response, retryTimes, deadline,
         timeoutOption(timeoutOption_)](const functionsystem::Status &status) -> bool {
        if (status.IsOk()) {
            promise->SetValue(status);
        } else if (retryTimes >= timeoutOption.operationRetryTimes || status.StatusCode() == GRPC_OUT_OF_RANGE
                   || status.StatusCode() == GRPC_PERMISSION_DENIED || status.StatusCode() == GRPC_INVALID_ARGUMENT) {
            YRLOG_ERROR("Get over times: {}", status.ToString());
            promise->SetValue(status);
        } else {
            YRLOG_WARN("Get error: {}, begin to retry", status.ToString());
            auto nextSleepTime = GenerateRandomNumber(timeoutOption.operationRetryIntervalLowerBound * retryTimes,
                                                      timeoutOption.operationRetryIntervalUpperBound * retryTimes);
            if (deadline != std::chrono::steady_clock::time_point::max()) {
                auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - std::chrono::steady_clock::now()).count();
                nextSleepTime = std::min(nextSleepTime, std::max<int64_t>(0, remaining));
            }
            (void)litebus::AsyncAfter(nextSleepTime, aid, &EtcdKvClientStrategy::DoGet, promise, request, response,
                                      retryTimes + 1, deadline);
        }
        return true;
    };
    auto timeoutSeconds = timeoutOption_.grpcTimeout * retryTimes;
    if (deadline != std::chrono::steady_clock::time_point::max()) {
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now()).count();
        if (remaining <= 0) {
            promise->SetValue(Status(GRPC_DEADLINE_EXCEEDED, "etcd prefix snapshot deadline exceeded"));
            return;
        }
        // CallAsyncX uses seconds. Round up, then reject late results above.
        timeoutSeconds = std::min(timeoutSeconds, (remaining + 999) / 1000);
    }
    response->Clear();
    (void)kvClient_
        ->CallAsyncX("Get", request, response.get(), &etcdserverpb::KV::Stub::AsyncRange,
                     timeoutSeconds)
        .Then(then);
}

litebus::Future<std::shared_ptr<TxnResponse>> EtcdKvClientStrategy::CommitTxn(const ::etcdserverpb::TxnRequest &request,
                                                                              bool)
{
    auto response = std::make_shared<etcdserverpb::TxnResponse>();
    std::function<std::shared_ptr<TxnResponse>(const functionsystem::Status &)> then =
        [response](const functionsystem::Status &status) -> std::shared_ptr<TxnResponse> {
        std::shared_ptr<TxnResponse> output = std::make_shared<TxnResponse>();
        if (status.IsOk()) {
            if (response->has_header()) {
                Transform(output->header, response->header());
                KvClientStrategy::Convert(response, output);  // convert responses
            } else {
                output->status = Status(StatusCode::GRPC_UNAVAILABLE, "etcd txn fail: no header.");
            }
        } else {
            output->status = std::move(status);
        }
        return output;
    };

    auto promise = std::make_shared<litebus::Promise<functionsystem::Status>>();
    DoCommit(promise, request, response, 1);  // do retry
    return promise->GetFuture().Then(then);
}

litebus::Future<std::shared_ptr<::etcdserverpb::TxnResponse>> EtcdKvClientStrategy::CommitWithReq(
    const etcdserverpb::TxnRequest &request, bool)
{
    auto response = std::make_shared<etcdserverpb::TxnResponse>();
    std::function<std::shared_ptr<::etcdserverpb::TxnResponse>(const functionsystem::Status &)> then =
        [response](const functionsystem::Status &status) -> std::shared_ptr<::etcdserverpb::TxnResponse> {
        if (!status.IsOk()) {
            // only print error log, return empty response
            YRLOG_ERROR("etcd txn fail");
        }
        return response;
    };

    auto promise = std::make_shared<litebus::Promise<functionsystem::Status>>();
    DoCommit(promise, request, response, 1);  // do retry
    return promise->GetFuture().Then(then);
}

void EtcdKvClientStrategy::DoCommit(const std::shared_ptr<litebus::Promise<functionsystem::Status>> &promise,
                                    const etcdserverpb::TxnRequest &request,
                                    const std::shared_ptr<etcdserverpb::TxnResponse> &response, int retryTimes)
{
    if (healthyStatus_.IsError()) {
        promise->SetValue(
            Status(healthyStatus_.StatusCode(), "[fallbreak] failed to call Txn: " + healthyStatus_.GetMessage()));
        return;
    }

    std::function<bool(const functionsystem::Status &status)> then =
        [aid(GetAID()), promise, request, response, retryTimes,
         timeoutOption(timeoutOption_)](const functionsystem::Status &status) -> bool {
        ASSERT_IF_NULL(response);
        if (status.IsOk()) {
            promise->SetValue(status);
        } else if (retryTimes == timeoutOption.operationRetryTimes) {
            YRLOG_ERROR("Txn over times: {}", status.ToString());
            promise->SetValue(status);
        } else {
            YRLOG_WARN("Txn error: {}, begin to retry", status.ToString());
            auto nextSleepTime = GenerateRandomNumber(timeoutOption.operationRetryIntervalLowerBound * retryTimes,
                                                      timeoutOption.operationRetryIntervalUpperBound * retryTimes);
            (void)litebus::AsyncAfter(nextSleepTime, aid, &EtcdKvClientStrategy::DoCommit, promise, request, response,
                                      retryTimes + 1);
        }
        return true;
    };

    (void)kvClient_
        ->CallAsyncX("Txn", request, response.get(), &etcdserverpb::KV::Stub::AsyncTxn,
                     timeoutOption_.grpcTimeout * retryTimes)
        .Then(then);
}

litebus::Future<std::shared_ptr<::etcdserverpb::TxnResponse>> EtcdKvClientStrategy::CommitRaw(
    const ::etcdserverpb::TxnRequest &request)
{
    auto response = std::make_shared<::etcdserverpb::TxnResponse>();
    std::function<std::shared_ptr<::etcdserverpb::TxnResponse>(const functionsystem::Status &)> then =
        [response](const functionsystem::Status &status) -> std::shared_ptr<::etcdserverpb::TxnResponse> {
        ASSERT_IF_NULL(response);
        if (status.IsOk()) {
            if (!response->has_header()) {
                YRLOG_ERROR("etcd txn fail: no header.");
            }
        } else {
            YRLOG_ERROR("etcd txn fail: {}", status.ToString());
        }
        return response;
    };

    auto promise = std::make_shared<litebus::Promise<functionsystem::Status>>();
    DoCommit(promise, request, response, 1);  // do retry
    return promise->GetFuture().Then(then);
}

litebus::Future<std::shared_ptr<Watcher>> EtcdKvClientStrategy::Watch(
    const std::string &key, const WatchOption &option, const ObserverFunction &observer, const SyncerFunction &syncer,
    const std::shared_ptr<WatchRecord> &reconnectRecord)
{
    auto request = Build(key, option);
    if (watchStream_ == nullptr) {
        auto channel = kvClient_->GetChannel();
        watchContext_ = std::make_unique<grpc::ClientContext>();
        watchStream_ = etcdserverpb::Watch::NewStub(channel)->Watch(&(*watchContext_));

        watchThread_ = std::make_unique<std::thread>([this]() { OnWatch(); });
        int ret = pthread_setname_np(watchThread_->native_handle(), "OnWatch");
        if (ret != 0) {
            YRLOG_WARN("failed({}) to set pthread name to ({}).", ret, "OnWatch");
        }
    }
    if (!kvClient_->IsConnected()) {
        return RetryWatch(key, option, observer, syncer, reconnectRecord);
    }

    if (watchStream_->Write(*request)) {
        YRLOG_INFO("Success to watch key({})", key);

        std::shared_ptr<WatchRecord> record;
        if (reconnectRecord == nullptr) {
            record = std::make_shared<WatchRecord>();
            (void)records_.emplace_back(record);
        } else {
            // use origin record while re-watching
            record = reconnectRecord;
        }

        record->key = key;
        record->option = option;
        record->observer = observer;
        record->syncer = syncer;
        if (record->watcher == nullptr) {
            record->watcher = std::make_shared<Watcher>(
                [aid(GetAID())](int64_t watchID) { litebus::Async(aid, &EtcdKvClientStrategy::CancelWatch, watchID); });
        }
        pendingRecords_.push_back(record);
        return record->watcher;
    } else {
        return RetryWatch(key, option, observer, syncer, reconnectRecord);
    }
}

bool EtcdKvClientStrategy::ReconnectWatch()
{
    // skip reconnect watch triggered by health status change,
    // because kv client has to reconnect, while the OnWatch thread is blocked,
    // so it will reconnect by itself
    return true;
}

litebus::Future<bool> EtcdKvClientStrategy::ReconnectGrpcWatch()
{
    if (!running_) {
        return false;
    }

    auto channel = kvClient_->GetChannel();

    if (watchContext_ != nullptr) {
        watchContext_->TryCancel();
    }
    watchContext_ = std::make_unique<grpc::ClientContext>();
    watchStream_ = etcdserverpb::Watch::NewStub(channel)->Watch(&(*watchContext_));

    pendingRecords_.clear();
    return KvClientStrategy::ReconnectWatch();
}

litebus::Future<std::shared_ptr<Watcher>> EtcdKvClientStrategy::RetryWatch(
    const std::string &key, const WatchOption &option, const ObserverFunction &observer, const SyncerFunction &syncer,
    const std::shared_ptr<WatchRecord> &reconnectRecord)
{
    if (!option.keepRetry) {
        YRLOG_INFO("Failed to watch key({})", key);
        return std::make_shared<Watcher>(
            [aid(GetAID())](int64_t watchID) { litebus::Async(aid, &EtcdKvClientStrategy::CancelWatch, watchID); });
    }
    auto promise = std::make_shared<litebus::Promise<std::shared_ptr<Watcher>>>();
    (void)litebus::TimerTools::AddTimer(
        RETRY_INTERVAL, GetAID(), [aid(GetAID()), key, option, observer, syncer, reconnectRecord, promise]() {
            promise->Associate(
                litebus::Async(aid, &EtcdKvClientStrategy::Watch, key, option, observer, syncer, reconnectRecord));
        });
    return promise->GetFuture();
}

void EtcdKvClientStrategy::CancelWatch(int64_t watchId)
{
    KvClientStrategy::CancelWatch(watchId);

    if (watchStream_ != nullptr && watchId != -1) {
        etcdserverpb::WatchRequest request;

        auto *args = request.mutable_cancel_request();
        args->set_watch_id(watchId);

        YRLOG_INFO("Cancel a watcher({})", watchId);
        (void)watchStream_->Write(request);
    }
}

bool EtcdKvClientStrategy::TryErr()
{
    if (watchContext_ != nullptr) {
        watchContext_->TryCancel();
        return true;
    }

    return false;
}

litebus::Future<Status> EtcdKvClientStrategy::OnCancel(const std::shared_ptr<WatchResponse> &rsp)
{
    auto status = KvClientStrategy::OnCancel(rsp);
    if (status.IsOK() && status.Get().IsOk()) {
        return status;
    }

    auto iter = readyRecords_.find(rsp->watch_id());
    if (iter == readyRecords_.end()) {
        return Status::OK();
    }
    YRLOG_WARN(
        "watcher({}) is canceled by server, reason: {}, compact revision: {}, last revision: {}, current revision: {}, "
        "fragment: {}.",
        rsp->watch_id(), rsp->cancel_reason(), rsp->compact_revision(), iter->second->option.revision,
        rsp->header().revision(), rsp->fragment());
    if (rsp->compact_revision() > iter->second->option.revision) {
        return SyncAndReWatch(rsp->watch_id());
    }
    (void)ReWatch(rsp->watch_id());
    return Status(StatusCode::SUCCESS, "try to reconnect all watcher");
}

Status EtcdKvClientStrategy::Cancel(const std::shared_ptr<WatchResponse> &rsp)
{
    auto future = litebus::Async(GetAID(), &EtcdKvClientStrategy::OnCancel, rsp);
    while (running_) {  // wait until all syncer and reconnect done
        auto success = future.Get(CANCEL_TIMEOUT);
        if (success.IsSome()) {
            YRLOG_INFO("Finish sync all data and reconnect");
            return success.Get();
        } else {
            return Status(StatusCode::FAILED, "failed to sync");
        }
        YRLOG_DEBUG("Waiting to sync all data and reconnect");
    }
    return Status(StatusCode::FAILED, "OnWatch thread is stopped");
}

void EtcdKvClientStrategy::OnWatch()
{
    YRLOG_INFO("Start a thread to read watcher's stream");
    std::shared_ptr<etcdserverpb::WatchResponse> response;
    while (running_) {
        response = std::make_shared<etcdserverpb::WatchResponse>();
        if (watchStream_->Read(&(*response))) {
            if (response->created()) {
                (void)litebus::Async(GetAID(), &EtcdKvClientStrategy::OnCreate, response);
            } else if (response->canceled()) {
                // wait until watchStream_ finished re-watch
                Cancel(response);
            } else {
                (void)litebus::Async(GetAID(), &EtcdKvClientStrategy::OnEvent, response, false);
            }
            continue;
        }

        kvClient_->CheckChannelAndWaitForReconnect(running_);
        if (!running_) {
            YRLOG_INFO("Stop to reconnect kv client.");
            break;
        }

        auto connected = litebus::Async(GetAID(), &EtcdKvClientStrategy::ReconnectGrpcWatch).Get(15000);
        if (connected.IsNone() || !connected.Get()) {
            YRLOG_ERROR("Failed to reconnect kv client.");
            break;
        }

        YRLOG_INFO("Success to reconnect kv client.");
    }
    YRLOG_INFO("End a thread to read watcher's stream");
}

litebus::Future<bool> EtcdKvClientStrategy::IsConnected()
{
    ASSERT_IF_NULL(kvClient_);
    return kvClient_->IsConnected();
}

litebus::Future<std::shared_ptr<Watcher>> EtcdKvClientStrategy::GetAndWatch(
    const std::string &key, const WatchOption &option, const ObserverFunction &observer, const SyncerFunction &syncer,
    const std::shared_ptr<WatchRecord> &reconnectRecord)
{
    // if revision eq 0, need to sync
    if (option.revision == 0) {
        GetOption opts{ .prefix = option.prefix };
        return Get(key, opts).Then([aid(GetAID()), key, option, observer, syncer,
                                    reconnectRecord](const std::shared_ptr<GetResponse> &getResponse)
                                    -> litebus::Future<std::shared_ptr<Watcher>> {
            if (getResponse == nullptr || getResponse->status.IsError()) {
                YRLOG_ERROR("failed to read initial snapshot for key {}", key);
                return nullptr;
            }
            std::vector<WatchEvent> events;
            WatchOption watchOption{ option };
            events.reserve(getResponse->kvs.size());
            for (auto &kv : getResponse->kvs) {
                WatchEvent event{ .eventType = EVENT_TYPE_PUT, .kv = kv, .prevKv = {} };
                (void)events.emplace_back(event);
            }
            YRLOG_DEBUG("process get response for key {}, event size: {}", key, events.size());
            if (!observer(events, true)) {
                YRLOG_ERROR("failed to apply initial snapshot for key {}", key);
                return nullptr;
            }
            watchOption.revision = getResponse->header.revision + 1;
            return litebus::Async(aid, &EtcdKvClientStrategy::Watch, key, watchOption, observer, syncer,
                                  reconnectRecord);
        });
    }
    WatchOption watchOption{ option };
    return Watch(key, watchOption, observer, syncer, reconnectRecord);
}

litebus::Future<std::shared_ptr<Watcher>> EtcdKvClientStrategy::GetAndWatchWithHandler(
    const std::string &key, const WatchOption &option, const GetAndWatchHandlers &handlers,
    const std::shared_ptr<WatchRecord> &reconnectRecord)
{
    // if revision eq 0, need to sync
    if (option.revision == 0) {
        GetOption opts{ .prefix = option.prefix };
        return Get(key, opts).Then(
            [aid(GetAID()), key, option, handlers, reconnectRecord](const std::shared_ptr<GetResponse> &getResponse) {
                if (getResponse == nullptr) {
                    return litebus::Future<std::shared_ptr<Watcher>>(nullptr);
                }
                return handlers.responseHandler(getResponse)
                    .Then([=](const litebus::Future<Status> &status) -> litebus::Future<std::shared_ptr<Watcher>> {
                        if (status.IsError() || status.Get().IsError() || getResponse->status.IsError()) {
                            YRLOG_ERROR("process key({}) get response Error", key);
                            return nullptr;
                        }
                        std::vector<WatchEvent> events;
                        WatchOption watchOption{ option };
                        events.reserve(getResponse->kvs.size());
                        for (auto &kv : getResponse->kvs) {
                            WatchEvent event{ .eventType = EVENT_TYPE_PUT, .kv = kv, .prevKv = {} };
                            (void)events.emplace_back(event);
                        }
                        YRLOG_DEBUG("process get response for key {}, event size: {}", key, events.size());
                        if (!handlers.observer(events, true)) {
                            YRLOG_ERROR("failed to apply initial snapshot for key {}", key);
                            return nullptr;
                        }
                        watchOption.revision = getResponse->header.revision + 1;
                        return litebus::Async(aid, &EtcdKvClientStrategy::Watch, key, watchOption, handlers.observer,
                                              handlers.syncer, reconnectRecord);
                    });
            });
    }
    WatchOption watchOption{ option };
    return Watch(key, watchOption, handlers.observer, handlers.syncer, reconnectRecord);
}

void EtcdKvClientStrategy::OnAddressUpdated(const std::string &address)
{
    YRLOG_WARN("etcd kv client doesn't support address update yet");
}

Status EtcdKvClientStrategy::OnCreate(const std::shared_ptr<WatchResponse> &response)
{
    if (pendingRecords_.empty()) {
        YRLOG_ERROR("watcher not found to match {}", response->watch_id());
        return Status(StatusCode::FAILED, "watcher not found");
    }

    auto record = pendingRecords_.front();
    pendingRecords_.pop_front();

    RETURN_STATUS_IF_NULL(record, StatusCode::FAILED, "null record");
    if (record->watcher->IsCanceled()) {
        YRLOG_ERROR("the watcher({}) for key({}) has been canceled", response->watch_id(), record->key);
        return Status(StatusCode::FAILED, "watcher has been canceled");
    }

    const int64_t watchId = response->watch_id();
    record->watcher->SetWatchId(watchId);
    YRLOG_INFO("watcher({}) is created for key({})", watchId, record->key);
    return KvClientStrategy::OnCreate(record);
}
}  // namespace functionsystem::meta_store
