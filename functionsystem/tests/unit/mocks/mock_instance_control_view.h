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

#ifndef TEST_UNIT_MOCKS_MOCK_INSTANCE_CONTROL_VIEW_H
#define TEST_UNIT_MOCKS_MOCK_INSTANCE_CONTROL_VIEW_H

#include <gmock/gmock.h>

#include "function_proxy/common/state_machine/instance_control_view.h"

namespace functionsystem::test {
class MockInstanceControlView : public InstanceControlView {
public:
    explicit MockInstanceControlView(const std::string &nodeID) : InstanceControlView(nodeID, false)
    {
    }
    ~MockInstanceControlView() override
    {
    }
    MOCK_METHOD(litebus::Future<GeneratedInstanceStates>, NewInstance,
                (const std::shared_ptr<messages::ScheduleRequest> &scheduleReq), (override));
    MOCK_METHOD(litebus::Future<Status>, DelInstance, (const std::string &instanceID), (override));
    // The ownership-fenced overload the 2026-10 retirement chain actually
    // calls: tests MUST substitute THIS one (gate/expect it) — falling back
    // to the base would read the real empty machines_ map and fake-green.
    MOCK_METHOD(litebus::Future<Status>, DelInstance,
                (const std::string &instanceID, const std::string &expectedRuntimeID,
                 const std::string &expectedRequestID), (override));
    MOCK_METHOD((std::pair<std::string, std::string>), GetIncarnationSnapshot,
                (const std::string &instanceID), (override));
    // Three-state identity snapshot (PostFence strict re-verification).
    // Tests that drive a chain PAST the fence MUST set this (mirroring
    // their machine), otherwise the mock falls back to the REAL base
    // implementation reading the (empty) machines_ map — exists=false —
    // and the PostFence unknown-identity protection is silently bypassed.
    MOCK_METHOD((InstanceControlView::InstanceIdentitySnapshot), GetInstanceIdentitySnapshot,
                (const std::string &instanceID), (override));
    MOCK_METHOD(void, Update,
                (const std::string &instanceID, const resources::InstanceInfo &instanceInfo, bool isForceUpdate),
                (override));
    MOCK_METHOD(void, Delete, (const std::string &instanceID, int64_t modRevision), (override));
    MOCK_METHOD(litebus::Future<Status>, TryExitInstance, (const std::string &instanceID, bool isSynchronized),
                (override));
    MOCK_METHOD(std::shared_ptr<InstanceStateMachine>, GetInstance, (const std::string &instanceID), (override));
    MOCK_METHOD(GeneratedInstanceStates, TryGenerateNewInstance,
                (const std::shared_ptr<messages::ScheduleRequest> &scheduleReq), (override));
    MOCK_METHOD(function_proxy::InstanceInfoMap, GetInstancesWithStatus, (const InstanceState &state), (override));
    MOCK_METHOD((std::unordered_map<std::string, std::shared_ptr<InstanceStateMachine>>), GetInstances, (), (override));
    MOCK_METHOD(bool, IsRescheduledRequest, (const std::shared_ptr<messages::ScheduleRequest> &scheduleReq), ());
    MOCK_METHOD(void, GenerateStateMachine,
                (const std::string &instanceID, const resources::InstanceInfo &instanceInfo), ());
};
}  // namespace functionsystem::test

#endif  // FUNCTIONSYSTEM_TEST_UNIT_MOCKS_MOCK_INSTANCE_CONTROL_VIEW_H
