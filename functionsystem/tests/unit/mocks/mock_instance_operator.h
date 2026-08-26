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

#ifndef UNIT_MOCKS_MOCK_INSTANCE_OPERATOR_H
#define UNIT_MOCKS_MOCK_INSTANCE_OPERATOR_H

#include <gmock/gmock.h>

#include "common/meta_store_adapter/instance_operator.h"

namespace functionsystem::test {

class MockInstanceOperator : public InstanceOperator {  // NOLINT
public:
    MockInstanceOperator() : InstanceOperator(nullptr)
    {
    }

    ~MockInstanceOperator() override
    {
    }

    MOCK_METHOD(litebus::Future<OperateResult>, Create,
                (std::shared_ptr<StoreInfo> instanceInfo, std::shared_ptr<StoreInfo> routeInfo, bool isLowReliability),
                (override));
    MOCK_METHOD(litebus::Future<OperateResult>, Modify,
                (std::shared_ptr<StoreInfo> instanceInfo, std::shared_ptr<StoreInfo> routeInfo, const int64_t version,
                 bool isLowReliability),
                (override));
    MOCK_METHOD(litebus::Future<OperateResult>, Delete,
                (std::shared_ptr<StoreInfo> instanceInfo, std::shared_ptr<StoreInfo> routeInfo,
                 std::shared_ptr<StoreInfo> debugInstPutInfo, const int64_t version,
                 bool isLowReliability),
                (override));
    MOCK_METHOD(litebus::Future<OperateResult>, GetInstance, (const std::string &key), (override));
    MOCK_METHOD(litebus::Future<OperateResult>, ForceDelete,
                (std::shared_ptr<StoreInfo> instanceInfo, std::shared_ptr<StoreInfo> routeInfo,
                 std::shared_ptr<StoreInfo> debugInstPutInfo, bool isLowReliability),
                (override));
};
}  // namespace functionsystem::test

#endif  // UNIT_MOCKS_MOCK_INSTANCE_STATE_MACHINE_H
