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

#ifndef LOCAL_SCHEDULER_TRAEFIK_REGISTRY_H
#define LOCAL_SCHEDULER_TRAEFIK_REGISTRY_H

#include <mutex>
#include <string>
#include <memory>
#include <unordered_map>
#include <vector>
#include <utility>
#include <async/future.hpp>
#include "meta_storage_accessor/meta_storage_accessor.h"
#include "common/status/status.h"

namespace functionsystem::local_scheduler {

class TraefikRegistry {
public:
    struct PortMapping {
        int sandboxPort;
        int hostPort;
        std::string protocol;  // "https" → HTTPS backend, others → HTTP backend
    };

    // The incarnation (runtime + creation request) that registered a route.
    // UnregisterInstance deletes by bare instanceID-derived keys with no
    // generation condition, so a LATE unregister from an old chain could
    // otherwise strip a same-name new generation's router/service keys
    // after the new registration lands.
    struct IncarnationRecord {
        std::string runtimeID;
        std::string requestID;
    };

    TraefikRegistry(std::shared_ptr<MetaStorageAccessor> accessor,
                    const std::string& keyPrefix,
                    const std::string& httpEntryPoint = "websecure",
                    bool enableTLS = true,
                    const std::string& serversTransport = "yr-backend-tls@file");
    ~TraefikRegistry() = default;

    litebus::Future<Status> RegisterInstance(
        const std::string& instanceID,
        const std::string& hostIP,
        const std::vector<PortMapping>& portMappings);

    // Identity-bearing register: records the registering incarnation so a
    // later identity-bearing unregister can refuse a stale chain. The
    // legacy 3-arg form records nothing (behavior unchanged).
    litebus::Future<Status> RegisterInstance(
        const std::string& instanceID,
        const std::string& hostIP,
        const std::vector<PortMapping>& portMappings,
        const std::string& runtimeID,
        const std::string& requestID);

    litebus::Future<Status> UnregisterInstance(const std::string& instanceID);

    // Identity-bearing unregister: refuses with ERR_INSTANCE_BUSY (zero
    // accessor deletes) when the recorded incarnation differs from the
    // expected one; proceeds unchecked when no record exists (route
    // registered before incarnation tracking or by the legacy form).
    litebus::Future<Status> UnregisterInstance(const std::string& instanceID,
                                               const std::string& expectedRuntimeID,
                                               const std::string& expectedRequestID);

private:
    static std::string SanitizeID(const std::string& id);

    // Under incarnationLock_: true when the caller may proceed (no record,
    // or a record whose non-empty expected fields all match); matched
    // receives the recorded entry for later compare-and-forget.
    bool IncarnationAllows(const std::string& instanceID,
                           const std::string& expectedRuntimeID,
                           const std::string& expectedRequestID,
                           IncarnationRecord& matched);
    void RecordIncarnation(const std::string& instanceID,
                           const std::string& runtimeID, const std::string& requestID);
    void ForgetIncarnationIfUnchanged(const std::string& instanceID,
                                      const IncarnationRecord& was);

    std::shared_ptr<MetaStorageAccessor> accessor_;
    std::string keyPrefix_;
    std::string httpEntryPoint_;
    bool enableTLS_;
    std::string serversTransport_;

    std::mutex incarnationLock_;
    std::unordered_map<std::string, IncarnationRecord> incarnations_;
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_TRAEFIK_REGISTRY_H
