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

#ifndef BUSPROXY_BUSINESS_INSTANCE_ACTOR_INVOCATION_HANDLER_H
#define BUSPROXY_BUSINESS_INSTANCE_ACTOR_INVOCATION_HANDLER_H

#include <async/future.hpp>
#include <functional>

#include "common/proto/pb/posix_pb.h"
#include "function_proxy/common/iam/internal_iam.h"
#include "function_proxy/common/observer/data_plane_observer/data_plane_observer.h"
#include "function_proxy/busproxy/instance_proxy/instance_proxy.h"
#include "function_proxy/busproxy/memory_monitor/memory_monitor.h"

namespace functionsystem {

class InvocationHandler {
public:
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> Invoke(
        const std::string &from, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> CallResultAdapter(
        const std::string &from, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> CallResult(
        const std::string &from, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);
    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> EventAdapter(
        const std::string &from, const std::shared_ptr<runtime_rpc::StreamingMessage> &request);

    using CreateCallResultReciver =
        std::function<litebus::Future<std::pair<bool, std::shared_ptr<runtime_rpc::StreamingMessage>>>(
            const std::string &, std::shared_ptr<functionsystem::CallResult> &)>;
    static void RegisterCreateCallResultReceiver(const CreateCallResultReciver createCallResult)
    {
        createCallResultReceiver_ = createCallResult;
    }

    using FrontendCallResultReceiver =
        std::function<std::pair<bool, std::shared_ptr<runtime_rpc::StreamingMessage>>(
            const std::string &, const std::shared_ptr<runtime_rpc::StreamingMessage> &)>;
    static void RegisterFrontendCallResultReceiver(const FrontendCallResultReceiver frontendCallResult)
    {
        frontendCallResultReceiver_ = frontendCallResult;
    }
    using FrontendEventReceiver =
        std::function<bool(const std::string &, const std::shared_ptr<runtime_rpc::StreamingMessage> &)>;
    static void RegisterFrontendEventReceiver(const FrontendEventReceiver frontendEvent)
    {
        frontendEventReceiver_ = frontendEvent;
    }
    static void BindUrl(const std::string &url)
    {
        localUrl_ = url;
    }
    static void BindInstanceProxy(const std::shared_ptr<busproxy::InstanceProxyWrapper> &instanceProxy)
    {
        instanceProxy_ = instanceProxy;
    }
    static void UnBindInstanceProxy()
    {
        instanceProxy_ = nullptr;
    }

    static void BindInternalIAM(const std::shared_ptr<function_proxy::InternalIAM> &internalIam)
    {
        internalIam_ = internalIam;
    }

    static void BindMemoryMonitor(const std::shared_ptr<functionsystem::MemoryMonitor> &memoryMonitor)
    {
        memoryMonitor_ = memoryMonitor;
        if (memoryMonitor_ != nullptr && memoryMonitor_->IsEnabled()) {
            memoryMonitor_->RefreshActualMemoryUsage();
        }
    }

    static void StopMemoryMonitor()
    {
        if (memoryMonitor_ != nullptr && memoryMonitor_->IsEnabled()) {
            memoryMonitor_->StopRefreshActualMemoryUsage();
            memoryMonitor_ = nullptr;
        }
    }

    static void ReleaseEstimateMemory(const std::string &instanceID, const std::string &requestID)
    {
        if (memoryMonitor_ != nullptr && memoryMonitor_->IsEnabled()) {
            memoryMonitor_->ReleaseEstimateMemory(instanceID, requestID);
        }
    }

    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> Authorize(
        const litebus::AID &to, const std::string &srcInstanceID, const std::string &instanceID,
        const SharedStreamMsg &request, const std::shared_ptr<busproxy::TimePoint> &time);

    inline static std::map<std::string, std::string> requestTraceMap_;

    static void EnablePerf(bool isEnable)
    {
        isPerf_ = isEnable;
    }

private:
    InvocationHandler() = default;
    ~InvocationHandler() = default;

    static litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> CallWithAuthorize(
        const litebus::AID &to, const busproxy::CallerInfo &callerInfo, const std::string &instanceID,
        const SharedStreamMsg &request, const std::shared_ptr<busproxy::TimePoint> &time);
    inline static CreateCallResultReciver createCallResultReceiver_ = nullptr;
    inline static FrontendCallResultReceiver frontendCallResultReceiver_ = nullptr;
    inline static FrontendEventReceiver frontendEventReceiver_ = nullptr;
    inline static std::string localUrl_;
    inline static std::shared_ptr<busproxy::InstanceProxyWrapper> instanceProxy_{ nullptr };
    inline static std::shared_ptr<functionsystem::MemoryMonitor> memoryMonitor_{ nullptr };
    inline static std::shared_ptr<function_proxy::InternalIAM> internalIam_{ nullptr };
    inline static std::atomic<bool> isPerf_{ false };
};

}  // namespace functionsystem

#endif  // BUSPROXY_BUSINESS_INSTANCE_ACTOR_INVOCATION_HANDLER_H
