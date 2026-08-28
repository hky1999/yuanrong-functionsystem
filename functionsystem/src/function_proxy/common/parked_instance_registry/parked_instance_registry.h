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
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

#include "async/future.hpp"
#include "common/status/status.h"

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
 *
 * The registry also carries the park drain phase: before a park kills the
 * sandbox, SnapCtrl asks the data plane (InstanceView provider) to flip the
 * dispatcher not-ready and wait for already-delivered in-flight invokes to
 * finish, so no invoke ever straddles the sandbox kill (W6 direction A).
 */
class ParkedInstanceRegistry {
public:
    /**
     * Quiesce one instance's data plane: stop accepting new invokes and wait
     * (bounded by timeoutMs) for the outstanding set to reach zero. Returns
     * ERR_INSTANCE_BUSY on timeout (drain incomplete).
     */
    using DrainProvider = std::function<litebus::Future<Status>(const std::string &instanceID, uint64_t timeoutMs)>;
    // W19-1: demand-driven restore hook — fired when a parked hold expires
    // with callers still retrying (the watermark FIFO may starve a parked
    // instance while node pressure stays high; the expired-held retries are
    // the demand signal). The provider (wired by snap_ctrl) wakes the
    // instance from its checkpoint. Fire-and-forget: failures fall back to
    // the existing watermark path.
    using WakeProvider = std::function<void(const std::string &instanceID)>;
    void SetWakeProvider(WakeProvider provider) { wakeProvider_ = std::move(provider); }
    void NotifyHoldExpired(const std::string &instanceID)
    {
        if (wakeProvider_ && IsParked(instanceID)) {
            wakeProvider_(instanceID);
        }
    }

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

    /**
     * Inject drain configuration from process flags at startup.
     * @param timeoutMs how long the drain phase waits for in-flight invokes
     * @param enable master switch for the drain phase
     * @param forceOnTimeout true = proceed with the park even on drain timeout
     *                       (re-introduces the in-flight breakage W6 fixes; experiments only)
     */
    void ConfigureDrain(uint32_t timeoutMs, bool enable, bool forceOnTimeout)
    {
        std::lock_guard<std::mutex> guard(lock_);
        drainTimeoutMs_ = timeoutMs;
        drainEnable_ = enable;
        drainForceOnTimeout_ = forceOnTimeout;
    }

    bool Enabled() const
    {
        std::lock_guard<std::mutex> guard(lock_);
        return enable_;
    }

    bool DrainEnabled() const
    {
        std::lock_guard<std::mutex> guard(lock_);
        return drainEnable_;
    }

    uint32_t DrainTimeoutMs() const
    {
        std::lock_guard<std::mutex> guard(lock_);
        return drainTimeoutMs_;
    }

    bool DrainForceOnTimeout() const
    {
        std::lock_guard<std::mutex> guard(lock_);
        return drainForceOnTimeout_;
    }

    /**
     * Register the data-plane drain implementation (normally InstanceView at
     * BindTimerContext time, when its serializing mailbox becomes available).
     * Without a provider DrainInstance succeeds immediately (e.g. unit tests
     * that never wire an InstanceView).
     */
    void SetDrainProvider(const DrainProvider &provider)
    {
        std::lock_guard<std::mutex> guard(lock_);
        drainProvider_ = provider;
    }

    /**
     * Register the drain rollback: flip a drained (not-ready) dispatcher back to
     * ready. Called when a park is abandoned after the drain phase already ran
     * (drain timeout / PrepareSnap or snapshot failure with the instance still
     * running) — nothing else would ever flip the dispatcher back.
     */
    void SetReleaseDrainProvider(const std::function<void(const std::string &instanceID)> &provider)
    {
        std::lock_guard<std::mutex> guard(lock_);
        releaseDrainProvider_ = provider;
    }

    /**
     * Run the drain phase for an instance about to be parked. Succeeds
     * immediately when drain is disabled or no provider is registered.
     */
    litebus::Future<Status> DrainInstance(const std::string &instanceID)
    {
        DrainProvider provider;
        uint64_t timeoutMs = 0;
        {
            std::lock_guard<std::mutex> guard(lock_);
            if (!drainEnable_ || !drainProvider_) {
                return Status::OK();
            }
            provider = drainProvider_;
            timeoutMs = drainTimeoutMs_;
        }
        return provider(instanceID, timeoutMs);
    }

    /**
     * Roll back a completed drain phase (park abandoned, instance keeps running).
     */
    void ReleaseDrain(const std::string &instanceID)
    {
        std::function<void(const std::string &)> provider;
        {
            std::lock_guard<std::mutex> guard(lock_);
            provider = releaseDrainProvider_;
        }
        if (provider != nullptr) {
            provider(instanceID);
        }
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
    bool drainEnable_{ true };
    uint32_t drainTimeoutMs_{ 10000 };
    bool drainForceOnTimeout_{ false };
    DrainProvider drainProvider_;
    std::function<void(const std::string &)> releaseDrainProvider_;

    WakeProvider wakeProvider_;
};
}  // namespace functionsystem::function_proxy

#endif  // FUNCTION_PROXY_COMMON_PARKED_INSTANCE_REGISTRY_H