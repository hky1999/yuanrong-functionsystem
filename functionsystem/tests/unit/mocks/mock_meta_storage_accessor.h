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

#ifndef UT_MOCKS_MOCK_META_STORAGE_ACCESSOR_H
#define UT_MOCKS_MOCK_META_STORAGE_ACCESSOR_H

#include <gmock/gmock.h>

#include "meta_storage_accessor/meta_storage_accessor.h"

namespace functionsystem::test {

class MockMetaStorageAccessor : public MetaStorageAccessor {
public:
    explicit MockMetaStorageAccessor(std::shared_ptr<MetaStoreClient> cli) : MetaStorageAccessor(std::move(cli))
    {
    }
    MOCK_METHOD(litebus::Future<Status>, Put, (const std::string &key, const std::string &value),
                (override));
    MOCK_METHOD(litebus::Future<Status>, PutWithLease, (const std::string &key, const std::string &value, const int ttl),
                 (override));
    MOCK_METHOD(litebus::Future<Status>, Delete, (const std::string &key), (override));
    // Prefix delete + Txn are what TraefikRegistry's route retire path uses.
    MOCK_METHOD(litebus::Future<Status>, Delete, (const std::string &key, bool isPrefix), (override));
    MOCK_METHOD(litebus::Future<Status>, Revoke, (const std::string &key), (override));
    MOCK_METHOD(litebus::Option<std::string>, Get, (const std::string &key), (override));
    using TxnKvs = const std::vector<std::pair<std::string, std::string>> &;
    MOCK_METHOD(litebus::Future<Status>, Txn, (TxnKvs), (override));
};

}  // namespace functionsystem::test

#endif  // UT_MOCKS_MOCK_META_STORAGE_ACCESSOR_H
