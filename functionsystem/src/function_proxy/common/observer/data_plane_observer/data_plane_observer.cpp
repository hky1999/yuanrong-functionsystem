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

#include "data_plane_observer.h"

#include "function_proxy/common/observer/observer_actor.h"

namespace functionsystem::function_proxy {

litebus::Future<Status> DataPlaneObserver::SubscribeInstanceEvent(const std::string &subscriber,
                                                                  const std::string &targetInstance,
                                                                  bool ignoreNonExist)
{
    RETURN_STATUS_IF_NULL(observerActor_, StatusCode::FAILED, "observerActor_ is nullptr");
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::TrySubscribeInstanceEvent, subscriber,
                          targetInstance, ignoreNonExist);
}

litebus::Future<std::shared_ptr<resources::RouteInfo>> DataPlaneObserver::QueryInstanceRoute(
    const std::string &instanceID)
{
    if (observerActor_ == nullptr) {
        return litebus::Future<std::shared_ptr<resources::RouteInfo>>(litebus::Status(-1));
    }
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::QueryInstanceRoute, instanceID);
}

litebus::Future<std::string> DataPlaneObserver::QueryProxyAddress(const std::string &proxyID)
{
    if (observerActor_ == nullptr) {
        return litebus::Future<std::string>(litebus::Status(-1));
    }
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::QueryProxyAddress, proxyID);
}

void DataPlaneObserver::NotifyMigratingRequest(const std::string &instanceID)
{
    RETURN_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::NotifyMigratingRequest, instanceID);
}

void DataPlaneObserver::ReportTraffic(const std::string &instanceID, const size_t &size)
{
    RETURN_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::ReportTraffic, instanceID, size);
}

void DataPlaneObserver::MarkInstanceUsed(const std::string &instanceID)
{
    RETURN_IF_NULL(observerActor_);
    return litebus::Async(observerActor_->GetAID(), &ObserverActor::MarkInstanceUsed, instanceID);
}
}  // namespace functionsystem::function_proxy
