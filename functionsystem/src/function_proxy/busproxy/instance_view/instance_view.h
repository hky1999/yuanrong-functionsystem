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
#ifndef FUNCTION_PROXY_BUSPROXY_INSTANCE_VIEW_H
#define FUNCTION_PROXY_BUSPROXY_INSTANCE_VIEW_H

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/data_view/proxy_view/proxy_view.h"
#include "common/posix_client/data_plane_client/data_interface_client_manager_proxy.h"
#include "common/state_machine//instance_listener.h"
#include "common/status/status.h"
#include "common/types/instance_state.h"
#include "function_proxy/busproxy/instance_proxy/instance_proxy.h"
#include "function_proxy/common/parked_instance_registry/parked_instance_registry.h"
#include "timer/timertools.hpp"

namespace functionsystem::busproxy {
using EventHandler = std::function<void(const std::string &, const resources::InstanceInfo &)>;
// InstanceView used for manager lifecycle of instance proxy.
class InstanceView : public InstanceListener {
public:
    explicit InstanceView(const std::string &nodeID);
    ~InstanceView() override;
    void Update(const std::string &instanceID, const resources::InstanceInfo &instanceInfo,
                bool isForceUpdate) override;
    void Delete(const std::string &instanceID, int64_t modRevision = -1) override;

    void BindDataInterfaceClientManager(
        const std::shared_ptr<DataInterfaceClientManagerProxy> &dataInterfaceClientManager)
    {
        dataInterfaceClientManager_ = dataInterfaceClientManager;
    }

    void BindProxyView(const std::shared_ptr<ProxyView> &proxyView)
    {
        proxyView_ = proxyView;
    }

    /**
     * Bind the AID (normally the ObserverActor) whose mailbox serializes the
     * InstanceView bookkeeping; parked-hold expiry timers re-enter through it.
     * Also registers the park drain provider: the mailbox is what makes it safe
     * to touch localInstances_/allInstances_ from the drain path.
     */
    void BindTimerContext(const litebus::AID &timerAid)
    {
        timerAid_ = timerAid;
        auto &registry = function_proxy::ParkedInstanceRegistry::Instance();
        registry.SetDrainProvider([this](const std::string &instanceID,
                                         uint64_t timeoutMs) -> litebus::Future<Status> {
            return DrainInstanceInFlight(instanceID, timeoutMs);
        });
        registry.SetReleaseDrainProvider(
            [this](const std::string &instanceID) { ReleaseDrainInFlight(instanceID); });
    }

    Status SubscribeInstanceEvent(const std::string &subscriber, const std::string &targetInstance,
                                  bool ignoreNonExist = false);

    void NotifyMigratingRequest(const std::string &instanceID);
    void OnNodeAbnormal(const std::string &nodeID);

private:
    void Creating(const std::string &instanceID, const resources::InstanceInfo &instanceInfo);
    void Running(const std::string &instanceID, const resources::InstanceInfo &instanceInfo);
    void Fatal(const std::string &, const resources::InstanceInfo &);
    void Reject(const std::string &, const resources::InstanceInfo &);
    void SpawnInstanceProxy(const std::string &, const resources::InstanceInfo &);
    void ReadyStatusChanged(const std::string &, const resources::InstanceInfo &);
    void NotifyReady(const std::string &, const resources::InstanceInfo &);
    void NotifyChanged(const litebus::AID &aid, const std::string &instanceID, const std::string &functionProxyID,
                       const std::shared_ptr<InstanceRouterInfo> &routeInfo);
    void NotifySubscriberInstanceReady(const std::string &, const resources::InstanceInfo &);
    void TerminateMigratedInstanceProxy(const std::string &instanceID);
    bool IsLocalParkedInstance(const std::string &instanceID) const;
    // Parked delete: keep the InstanceProxy actor alive with a not-ready dispatcher so
    // invokes arriving in the park window are held in the dispatcher call cache.
    void HandleParkedDelete(const std::string &instanceID, const resources::InstanceInfo &lastInfo);
    // Flip a local parked instance's dispatcher to not-ready (hold) and arm the hold
    // TTL. Shared by the parked delete path and the parked FATAL path: the deliberate
    // sandbox kill of a park is reported as an instance exit and arrives as FATAL
    // BEFORE the snapshot completes, so the park mark must already suppress it.
    void HoldParkedInstance(const std::string &instanceID, const resources::InstanceInfo &info);
    // Restore came back (RUNNING): drop the park bookkeeping; NotifyReady flushes held invokes.
    void ClearParked(const std::string &instanceID);
    // Hold TTL expired without restore: tear the actor down for real (held invokes get
    // ERR_INSTANCE_EXITED), matching the legacy delete failure semantics.
    void OnParkedExpired(const std::string &instanceID);
    // Park drain phase (W6 direction A): flip the instance's dispatcher not-ready
    // (stop accepting new sends; arriving invokes land in the call cache like the
    // post-kill hold) and wait, bounded by timeoutMs, for already-delivered in-flight
    // invokes to finish while the sandbox is still alive. ERR_INSTANCE_BUSY on timeout.
    litebus::Future<Status> DrainInstanceInFlight(const std::string &instanceID, uint64_t timeoutMs);
    void PollDrainInFlight(const std::string &instanceID, const litebus::AID &proxyAid,
                           const std::shared_ptr<litebus::Promise<Status>> &promise,
                           const std::chrono::steady_clock::time_point &deadline);
    // Drain rollback (park abandoned, instance still running): flip the dispatcher
    // back to ready so held invokes flush to the live sandbox. Idempotent; skips
    // instances no longer local or not last-known RUNNING.
    void ReleaseDrainInFlight(const std::string &instanceID);

    std::shared_ptr<DataInterfaceClientManagerProxy> dataInterfaceClientManager_ { nullptr };
    std::shared_ptr<ProxyView> proxyView_ { nullptr };
    std::unordered_map<std::string, std::shared_ptr<InstanceProxy>> localInstances_;
    // InstanceInfo should be replaced by shared ptr in future
    std::unordered_map<std::string, resources::InstanceInfo> allInstances_;
    std::unordered_map<std::string, std::unordered_set<std::string>> nodeInstanceMap_;
    // key : subscribed instance value: subscribers
    std::unordered_map<std::string, std::unordered_set<std::string>> subscribedInstances_;
    // key : subscriber value: subscribed instance
    std::unordered_map<std::string, std::unordered_set<std::string>> subscribers_;
    std::unordered_map<InstanceState, EventHandler> eventHandlers_;
    std::string nodeID_;
    // parked instances: instanceID -> hold expiry timer (actor kept alive in localInstances_)
    std::unordered_map<std::string, litebus::Timer> parkedHoldTimers_;
    litebus::AID timerAid_;
};
}  // namespace functionsystem::busproxy
#endif
