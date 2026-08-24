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

#include "call_cache.h"

namespace functionsystem::busproxy {

void CallCache::ReportTraffic()
{
    if (trafficReport_ == nullptr) {
        return;
    }
    auto size = requestMap_.size();
    trafficReport_(size == 0, size);
}

void CallCache::Push(const std::shared_ptr<CallRequestContext> &context)
{
    if (requestMap_.find(context->requestID) != requestMap_.end()) {
        return;
    }
    (void)reqNew_.insert(context->requestID);
    requestMap_[context->requestID] = context;
    ReportTraffic();
}

void CallCache::PushOnResp(const std::shared_ptr<CallRequestContext> &context)
{
    if (requestMap_.find(context->requestID) != requestMap_.end()) {
        return;
    }
    (void)reqOnResp_.insert(context->requestID);
    requestMap_[context->requestID] = context;
}

void CallCache::MoveToOnResp(const std::string &requestID)
{
    (void)reqNew_.erase(requestID);
    (void)reqOnResp_.insert(requestID);
}

void CallCache::MoveToInProgress(const std::string &requestID)
{
    (void)reqOnResp_.erase(requestID);
    (void)reqInProgress_.insert(requestID);
}

std::shared_ptr<CallRequestContext> CallCache::FindCallRequestContext(const std::string &requestID)
{
    if (requestMap_.find(requestID) == requestMap_.end()) {
        return nullptr;
    }
    return requestMap_.at(requestID);
}

void CallCache::DeleteReqInProgress(const std::string &requestID)
{
    (void)reqInProgress_.erase(requestID);
    (void)requestMap_.erase(requestID);
    ReportTraffic();
}

void CallCache::DeleteReqNew(const std::string &requestID)
{
    (void)reqNew_.erase(requestID);
    (void)requestMap_.erase(requestID);
    ReportTraffic();
}

void CallCache::DeleteReqOnResp(const std::string &requestID)
{
    (void)reqOnResp_.erase(requestID);
    (void)requestMap_.erase(requestID);
    ReportTraffic();
}

std::unordered_set<std::string> CallCache::GetNewReqs()
{
    return reqNew_;
}

std::unordered_set<std::string> CallCache::GetOnResp()
{
    return reqOnResp_;
}

std::unordered_set<std::string> CallCache::GetInProgressReqs()
{
    return reqInProgress_;
}

size_t CallCache::InFlightCount()
{
    return reqOnResp_.size() + reqInProgress_.size();
}

void CallCache::MoveAllToNew()
{
    for (const auto &ele : reqInProgress_) {
        (void)reqNew_.insert(ele);
    }
    for (const auto &ele : reqOnResp_) {
        (void)reqNew_.insert(ele);
    }
    reqInProgress_.clear();
    reqOnResp_.clear();
}

std::list<litebus::Future<SharedStreamMsg>> CallCache::GetOnRespFuture()
{
    std::list<litebus::Future<SharedStreamMsg>> futures;
    for (const auto &req : reqOnResp_) {
        if (requestMap_.find(req) == requestMap_.end()) {
            continue;
        }
        (void)futures.emplace_back(requestMap_[req]->callResponse.GetFuture());
    }
    return futures;
}

}  // namespace functionsystem