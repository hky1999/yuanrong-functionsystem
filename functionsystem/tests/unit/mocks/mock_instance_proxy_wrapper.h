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

#ifndef TEST_UNIT_MOCKS_MOCK_INSTANCE_PROXY_WRAPPER_H
#define TEST_UNIT_MOCKS_MOCK_INSTANCE_PROXY_WRAPPER_H

#include <gmock/gmock.h>

#include "actor/actor.hpp"
#include "function_proxy/busproxy/instance_proxy/instance_proxy.h"

namespace functionsystem::test {

class MockInstanceProxy : public busproxy::InstanceProxyWrapper {
public:
    MockInstanceProxy() : busproxy::InstanceProxyWrapper()
    {
    }
    ~MockInstanceProxy() = default;

    MOCK_METHOD(litebus::Future<SharedStreamMsg>, Call,
                (const litebus::AID &to, const busproxy::CallerInfo &callerInfo,
                 const std::string &instanceID, const SharedStreamMsg &request,
                 const std::shared_ptr<busproxy::TimePoint> &time),
                (override));

    MOCK_METHOD(litebus::Future<SharedStreamMsg>, CallResult,
                (const litebus::AID &to, const std::string &srcInstanceID, const std::string &dstInstanceID,
                 const SharedStreamMsg &request, const std::shared_ptr<busproxy::TimePoint> &time),
                (override));
    MOCK_METHOD(litebus::Future<SharedStreamMsg>, CompleteFrontendCall,
                (const litebus::AID &to, const SharedStreamMsg &callResult, const SharedStreamMsg &callResultAck),
                (override));
    MOCK_METHOD(litebus::Future<std::string>, GetTenantID, (const litebus::AID &to),(override));
};
}  // namespace functionsystem::test
#endif  // TEST_UNIT_MOCKS_MOCK_INSTANCE_PROXY_WRAPPER_H
