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

#ifndef COMMON_META_STORE_ADAPTER_INSTANCE_OPERATOR_H
#define COMMON_META_STORE_ADAPTER_INSTANCE_OPERATOR_H

#include "meta_store_client/meta_store_client.h"
#include "common/metadata/metadata.h"

namespace functionsystem {

const int32_t TRANSACTION_ERROR_START = 300;
const int32_t TRANSACTION_ERROR_END = 350;

// instance transaction error coder, range [300, 350)
bool TransactionFailedForEtcd(int32_t errCode);

enum class PersistenceType : int32_t {
    PERSISTENT_NOT,       // Update cache only
    PERSISTENT_INSTANCE,  // Update cache and persistence instanceInfo
    PERSISTENT_ROUTE,     // Update cache and persistence routeInfo
    PERSISTENT_ALL        // Update cache and persistence instanceInfo and routeInfo
};

struct OperateResult {
    Status status;
    std::string value;
    int64_t preKeyVersion;
    int64_t currentModRevision;
};

struct OperateInfo {
    std::string key;
    std::vector<std::string> values;
    uint64_t keySize;
    int64_t version;
    bool isCentOS;
    std::shared_ptr<TxnResponse> response;
};

class InstanceOperator {
public:
    explicit InstanceOperator(const std::shared_ptr<MetaStoreClient> &metaStoreClient);
    virtual ~InstanceOperator() = default;
    virtual litebus::Future<OperateResult> Create(std::shared_ptr<StoreInfo> instanceInfo,
                                                  std::shared_ptr<StoreInfo> routeInfo, bool isLowReliability);
    virtual litebus::Future<OperateResult> Modify(std::shared_ptr<StoreInfo> instanceInfo,
                                                  std::shared_ptr<StoreInfo> routeInfo, const int64_t version,
                                                  bool isLowReliability);
    virtual litebus::Future<OperateResult> Delete(std::shared_ptr<StoreInfo> instanceInfo,
                                                  std::shared_ptr<StoreInfo> routeInfo,
                                                  std::shared_ptr<StoreInfo> debugInstPutInfo,
                                                  const int64_t version,
                                                  bool isLowReliability);
    virtual litebus::Future<OperateResult> GetInstance(const std::string &key);
    virtual litebus::Future<OperateResult> ForceDelete(std::shared_ptr<StoreInfo> instanceInfo,
                                                       std::shared_ptr<StoreInfo> routeInfo,
                                                       std::shared_ptr<StoreInfo> debugInstPutInfo,
                                                       bool isLowReliability);

private:
    std::shared_ptr<MetaStoreClient> client_;
    bool isCentOS_;

    static OperateResult OnDelete(const OperateInfo &operateInfo);
    static OperateResult OnCreate(const OperateInfo &operateInfo);
    static OperateResult ResolveCreateConflict(const OperateInfo &operateInfo);
    static OperateResult OnModify(const OperateInfo &operateInfo);
    static OperateResult OnForceDelete(const OperateInfo &operateInfo);
    static bool CheckGetResponse(const TxnOperationResponse &resp, const std::string &key, const std::string &value,
                                 OperateResult &result);
    static bool PrintResponse(const OperateInfo &operateInfo);
    static void OnPrintResponse(const KeyValue& kv);
};
}  // namespace functionsystem

#endif
