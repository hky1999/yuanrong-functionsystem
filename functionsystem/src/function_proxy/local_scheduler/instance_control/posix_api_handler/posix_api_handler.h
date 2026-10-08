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

#ifndef LOCAL_SCHEUDLERSERVICE_POSIXAPI_HANDLER_H
#define LOCAL_SCHEUDLERSERVICE_POSIXAPI_HANDLER_H

#include <unordered_map>

#include "actor/actor.hpp"
#include "async/future.hpp"
#include "common/proto/pb/posix_pb.h"
#include "common/rpc/stream/posix/posix_client.h"
#include "local_scheduler/instance_control/instance_ctrl.h"
#include "local_scheduler/local_scheduler_service/local_sched_srv.h"
#include "local_scheduler/local_group_ctrl/local_group_ctrl.h"
#include "local_scheduler/resource_group_controller/resource_group_ctrl.h"

namespace functionsystem::local_scheduler {
class PosixAPIHandler {
public:
    /**
     * create instance from runtime
     * @param from: instanceID
     * @param request: create instance request
     * @return create instance result
     */
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> Create(
        const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);

    /**
     * kill instance from runtime
     * @param from: instanceID
     * @param request: kill instance request
     * @return kill instance result
     */
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> Kill(
        const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);

    /**
     * exit instance
     * @param from: instanceID
     * @param request: exit self request
     * @return kill instance result
     */
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> Exit(
        const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);

    /**
     * receive the create call result
     * @param from: instanceID
     * @param input: received call result
     * @param output: call result ack
     * @return true if the result is an instance creation result.
     */
    static litebus::Future<std::pair<bool, std::shared_ptr<runtime_rpc::StreamingMessage>>> CallResult(
        const grpc::PosixStreamIdentity &identity, std::shared_ptr<functionsystem::CallResult> &callResult);

    /**
     * create group instance from runtime
     * @param from: instanceID
     * @param request: create group request
     * @return create group instance response
     */
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> GroupCreate(
        const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);

    /**
     * create resource group from runtime
     * @param from: instanceID
     * @param request: resource group request
     * @return resource group request response
     */
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> CreateResourceGroup(
        const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);

    /**
     * bind instance control
     * @param instanceCtrl: instance control
     */
    static void BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl)
    {
        instanceCtrl_ = instanceCtrl;
    }

    static void BindControlClientManager(const std::shared_ptr<ControlInterfaceClientManagerProxy> &clientManager);

    /**
     * bind local scheduler service
     * @param localSchedSrv: local scheduler service
     */
    static void BindLocalSchedSrv(const std::shared_ptr<LocalSchedSrv> &localSchedSrv)
    {
        localSchedSrv_ = localSchedSrv;
    }

    /**
     * bind local group control
     * @param localGroupCtrl: local group control
     */
    static void BindLocalGroupCtrl(const std::shared_ptr<LocalGroupCtrl> &localGroupCtrl)
    {
        localGroupCtrl_ = localGroupCtrl;
    }

    static void BindResourceGroupCtrl(const std::shared_ptr<ResourceGroupCtrl> &rGroupCtrl)
    {
        rGroupCtrl_ = rGroupCtrl;
    }

    static void SetMaxPriority(const int16_t maxPriority)
    {
        maxPriority_ = maxPriority;
    }
protected:
    static Status IsValidCreateRequest(const functionsystem::CreateRequest &createReq);

    static Status IsValidCreateRequests(const functionsystem::CreateRequests &createReqs);

private:
    PosixAPIHandler() = default;
    ~PosixAPIHandler() = default;

    inline static std::weak_ptr<InstanceCtrl> instanceCtrl_;
    inline static std::weak_ptr<LocalSchedSrv> localSchedSrv_;
    inline static std::weak_ptr<ControlInterfaceClientManagerProxy> clientManager_;
    inline static std::weak_ptr<LocalGroupCtrl> localGroupCtrl_;
    inline static std::weak_ptr<ResourceGroupCtrl> rGroupCtrl_;
    inline static int16_t maxPriority_ = 0;
};
}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEUDLERSERVICE_POSIXAPI_HANDLER_H
