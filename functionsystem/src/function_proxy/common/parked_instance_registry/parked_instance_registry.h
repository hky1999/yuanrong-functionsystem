/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef FUNCTION_PROXY_COMMON_PARKED_INSTANCE_REGISTRY_H
#define FUNCTION_PROXY_COMMON_PARKED_INSTANCE_REGISTRY_H

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace functionsystem::function_proxy {

/**
 * ParkedInstanceRegistry records which instances were removed by a successful
 * snapshot with leaveRunning=false (park). Both the local scheduler (SnapCtrl,
 * producer) and the busproxy instance view (consumer) live in the function
 * proxy process, so a plain process-wide singleton is the channel between them.
 *
 * While an instance is parked, InstanceView keeps its InstanceProxy actor alive
 * with a not-ready dispatcher, so data-plane invokes arriving in the park
 * window are held in the dispatcher call cache instead of failing with
 * ERR_INSTANCE_NOT_FOUND; they are flushed when the instance is restored to
 * RUNNING, or failed with ERR_INSTANCE_EXITED once the hold TTL expires.
 */
class ParkedInstanceRegistry {
public:
    static ParkedInstanceRegistry &Instance()
    {
        static ParkedInstanceRegistry registry;
        return registry;
    }

    /**
     * Inject configuration from process flags at startup.
     * @param holdSeconds how long a parked instance keeps holding invokes
     * @param enable master switch; false keeps the registry empty (legacy behavior)
     */
    void Configure(uint32_t holdSeconds, bool enable)
    {
        std::lock_guard<std::mutex> guard(lock_);
        enable_ = enable;
        holdSeconds_ = std::chrono::seconds(holdSeconds);
    }

    bool Enabled() const
    {
        std::lock_guard<std::mutex> guard(lock_);
        return enable_;
    }

    uint32_t HoldSeconds() const
    {
        std::lock_guard<std::mutex> guard(lock_);
        return static_cast<uint32_t>(holdSeconds_.count());
    }

    /**
     * Mark an instance as parked from now until the hold TTL. Idempotent and
     * renewing: a second park of the same instance restarts the deadline.
     */
    void MarkParked(const std::string &instanceID)
    {
        std::lock_guard<std::mutex> guard(lock_);
        if (!enable_) {
            return;
        }
        parkedUntil_[instanceID] = std::chrono::steady_clock::now() + holdSeconds_;
    }

    /**
     * Whether the instance is parked and still inside its hold window.
     * Expired entries are treated as not parked (and lazily dropped).
     */
    bool IsParked(const std::string &instanceID) const
    {
        std::lock_guard<std::mutex> guard(lock_);
        auto iter = parkedUntil_.find(instanceID);
        if (iter == parkedUntil_.end()) {
            return false;
        }
        if (iter->second <= std::chrono::steady_clock::now()) {
            return false;
        }
        return true;
    }

    /**
     * Drop the park mark (instance restored, expired for real, or deleted).
     */
    void Clear(const std::string &instanceID)
    {
        std::lock_guard<std::mutex> guard(lock_);
        parkedUntil_.erase(instanceID);
    }

    size_t Size() const
    {
        std::lock_guard<std::mutex> guard(lock_);
        return parkedUntil_.size();
    }

private:
    ParkedInstanceRegistry() = default;

    mutable std::mutex lock_;
    bool enable_{ true };
    std::chrono::seconds holdSeconds_{ 300 };
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> parkedUntil_;
};
}  // namespace functionsystem::function_proxy

#endif  // FUNCTION_PROXY_COMMON_PARKED_INSTANCE_REGISTRY_H
