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

#ifndef FUNCTIONSYSTEM_META_STORE_META_STORE_STRUCT_H
#define FUNCTIONSYSTEM_META_STORE_META_STORE_STRUCT_H

#include <string>
#include <unordered_set>
#include <variant>
#include <vector>

#include "async/future.hpp"
#include "common/status/status.h"
#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"

namespace functionsystem {
const int64_t GRPC_TIMEOUT_SECONDS = 5;
const int64_t KV_OPERATE_RETRY_TIMES = 5;
const int64_t KV_DELETE_OPERATE_RETRY_TIMES = 60;
const int64_t KV_OPERATE_RETRY_INTERVAL_LOWER_BOUND = 1000;  // ms
const int64_t KV_OPERATE_RETRY_INTERVAL_UPPER_BOUND = 5000;  // ms
const uint32_t DEFAULT_META_STORE_MAX_FLUSH_CONCURRENCY = 1000;
const uint32_t DEFAULT_META_STORE_MAX_FLUSH_BATCH_SIZE = 100;
const std::string METASTORE_LOCAL_MODE = "local";
const std::string METASTORE_PASSTHROUGH_MODE = "passthrough";
const std::string ETCD_BACKEND = "Etcd";
const std::string METASTORE_BACKEND = "Metastore";

using WatchResponse = etcdserverpb::WatchResponse;
using KeyValue = mvccpb::KeyValue;

struct ResponseHeader {
    uint64_t clusterId = 0;

    uint64_t memberId = 0;

    int64_t revision = 0;

    uint64_t raftTerm = 0;
};

struct PutOption {
    int64_t leaseId = 0;

    // if true, return the overwritten value.
    bool prevKv = false;

    bool asyncBackup = true;
};

struct PutResponse {
    Status status = Status::OK();

    ResponseHeader header;

    // the overridden key-value.
    KeyValue prevKv;
};

struct DeleteOption {
    // if true, return the deleted value.
    bool prevKv = false;  // not used

    bool prefix = false;

    bool asyncBackup = true;
};

struct DeleteResponse {
    Status status = Status::OK();

    ResponseHeader header;

    // the number of keys deleted
    int64_t deleted;

    // the deleted key-value list, not used
    std::vector<KeyValue> prevKvs;
};

enum class SortOrder : int { NONE = 0, ASCEND = 1, DESCEND = 2 };

enum class SortTarget : int { KEY = 0, VERSION = 1, CREATE = 2, MODIFY = 3, VALUE = 4 };

struct GetOption {
    // if true, match key by prefix.
    bool prefix = false;

    // if true, return key only without value.
    bool keysOnly = false;

    // if true, only return count of the keys.
    bool countOnly = false;

    // limit the number of keys to return.
    int limit = 0;

    SortOrder sortOrder = SortOrder::NONE;

    SortTarget sortTarget = SortTarget::KEY;
};

struct GetResponse {
    Status status = Status::OK();

    ResponseHeader header;

    int64_t count = 0;  // for count only

    std::vector<KeyValue> kvs;
};

struct StatusResponse {
    Status status = Status::OK();
};

enum class TxnOperationType { OPERATION_PUT = 0, OPERATION_DELETE = 1, OPERATION_GET = 2 };

struct TxnOperationResponse {
    Status status = Status::OK();

    ResponseHeader header;

    TxnOperationType operationType;

    std::variant<PutResponse, DeleteResponse, GetResponse> response;
};

struct TxnResponse {
    Status status = Status::OK();

    ResponseHeader header;

    bool success = false;

    std::vector<TxnOperationResponse> responses;
};

struct LeaseGrantResponse {
    Status status = Status::OK();

    ResponseHeader header;

    int64_t leaseId = 0;

    // the second to live.
    int64_t ttl = 0;
};

struct LeaseKeepAliveResponse {
    Status status = Status::OK();

    ResponseHeader header;

    int64_t leaseId = 0;

    // the second to live.
    int64_t ttl = 0;
};

struct LeaseRevokeResponse {
    Status status = Status::OK();

    ResponseHeader header;
};

struct LeaderKey {
    // the election identifier that correponds to the leadership key.
    std::string name;

    // an opaque key representing the ownership of the election. If the key is deleted, then leadership is lost.
    std::string key;

    // the creation revision of the key. It can be used to test for ownership of an election during transactions by
    // testing the key's creation revision matches rev.
    int64_t rev;

    // the lease ID of the election leader.
    int64_t lease;
};

struct CampaignResponse {
    Status status = Status::OK();

    ResponseHeader header;

    // the resources used for holding leadereship of the election.
    LeaderKey leader;
};

struct LeaderResponse {
    Status status = Status::OK();

    ResponseHeader header;

    // the latest leader update.
    // key   = "electionkey/706aad..." => "{electionkey}/{leaseID}"
    // value = "theproposal"           => "{proposal}"
    std::pair<std::string, std::string> kv;
};

struct ResignResponse {
    Status status = Status::OK();

    ResponseHeader header;
};

struct WatchOption {
    bool prefix = false;

    bool prevKv = false;

    int64_t revision = 0;

    // if keepRetry is set true, watchStream  will
    // keep trying to write request until writing successfully.
    bool keepRetry = false;
};

enum EventType : int { EVENT_TYPE_PUT = 0, EVENT_TYPE_DELETE = 1 };

struct WatchEvent {
    EventType eventType;

    KeyValue kv;

    KeyValue prevKv;
};

const uint32_t MONITOR_INTERVAL = 10000;  // ms
const uint32_t MONITOR_TIMEOUT = 8000;    // ms
struct MetaStoreMonitorParam {
    uint32_t maxTolerateFailedTimes = 5;
    uint32_t checkIntervalMs = MONITOR_INTERVAL;
    uint32_t timeoutMs = MONITOR_TIMEOUT;
    std::string backend = ETCD_BACKEND; // default storage backend
};

struct MetaStoreConfig {
    std::string etcdAddress = "";
    std::string metaStoreAddress = "";
    bool enableMetaStore = false;
    bool isMetaStorePassthrough = false;
    std::string etcdTablePrefix = "";
    bool enableAutoSync = false;
    uint32_t autoSyncInterval = 0;  // ms
    std::unordered_set<std::string> excludedKeys = {};
};

struct MetaStoreTimeoutOption {
    int64_t operationRetryIntervalLowerBound = KV_OPERATE_RETRY_INTERVAL_LOWER_BOUND;
    int64_t operationRetryIntervalUpperBound = KV_OPERATE_RETRY_INTERVAL_UPPER_BOUND;
    int64_t operationRetryTimes = KV_OPERATE_RETRY_TIMES;
    int64_t grpcTimeout = GRPC_TIMEOUT_SECONDS;

    // Sequential pages for unlimited, default-order etcd prefix reads.
    uint32_t prefixReadPageSize = 256;
    uint32_t prefixReadTimeoutSeconds = 60;
};

struct MetaStoreBackupOption {
    bool enableSyncSysFunc = false;
    uint32_t metaStoreMaxFlushConcurrency = DEFAULT_META_STORE_MAX_FLUSH_CONCURRENCY;
    uint32_t metaStoreMaxFlushBatchSize = DEFAULT_META_STORE_MAX_FLUSH_BATCH_SIZE;
};

template <class TwithStatus>
void MetaStoreFailure(const std::shared_ptr<litebus::Promise<TwithStatus>> &promise, const Status &status,
                      const std::string &describe)
{
    RETURN_IF_NULL(promise);
    TwithStatus ret;
    ret.status = Status(status.StatusCode(), describe + ", caused by:" + status.GetMessage());
    promise->SetValue(std::move(ret));
}
}  // namespace functionsystem

#endif  // FUNCTIONSYSTEM_META_STORE_META_STORE_STRUCT_H
