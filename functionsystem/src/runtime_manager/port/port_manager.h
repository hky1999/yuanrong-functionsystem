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

#ifndef RUNTIME_MANAGER_PORT_PORT_MANAGER_H
#define RUNTIME_MANAGER_PORT_PORT_MANAGER_H

#include <string>
#include <unordered_map>
#include <map>
#include <vector>
#include "common/status/status.h"
#include "common/utils/singleton.h"

namespace functionsystem::runtime_manager {

class PortManager : public Singleton<PortManager> {
public:
    PortManager();

    ~PortManager() override;

    /**
     * Initialize port manager
     *
     * @param initialPort First port number.
     * @param portNum Port capacity.
     */
    void InitPortResource(int initialPort, int portNum);

    /**
     * Request port resource when start instance.
     *
     * @param runtimeID Received from function agent.
     * @return Port number if port resource is enough.
     */
    std::string RequestPort(const std::string &runtimeID);

    /**
     * Query port number by runtimeID.
     *
     * @param runtimeID runtime id.
     * @return Port number used by the runtime.
     */
    std::string GetPort(const std::string &runtimeID) const;

    /**
     * Release port resource when stop instance.
     *
     * @param runtimeID Runtime id.
     * @return success if found port resource and release.
     */
    int ReleasePort(const std::string &runtimeID);

    /**
     * Request multiple port resources atomically for port forwarding.
     * Rolls back all allocations if count ports cannot be satisfied.
     *
     * @param runtimeID Runtime id to associate ports with.
     * @param count Number of ports to allocate.
     * @return Allocated port numbers, or empty vector if insufficient ports.
     */
    std::vector<int> RequestPorts(const std::string &runtimeID, int count);

    /**
     * Reserve persisted ports for a runtime during proxy reconciliation.
     *
     * The operation is atomic across all supplied ports and idempotent when
     * the same runtime already owns them. A port outside the configured pool
     * or owned by another runtime fails the whole operation.
     *
     * @param runtimeID Runtime that owns the persisted reservations.
     * @param ports Host ports to restore.
     * @return success when every port is reserved by runtimeID.
     */
    Status ReservePorts(const std::string &runtimeID, const std::vector<int> &ports);

    /**
     * Release all port resources associated with a runtimeID.
     * Used when container with multiple forwarded ports is stopped.
     *
     * @param runtimeID Runtime id whose ports should all be freed.
     */
    void ReleasePorts(const std::string &runtimeID);

    /**
     * Clear port map when resource manager is closed.
     */
    void Clear();

    /**
     * Check whether the port can be used by runtime.
     *
     * @param port port number.
     * @return true if port can be used, false if not.
     */
    bool CheckPortInUse(int port) const;

private:
    int initialPort_ = 500;

    int poolSize_ = 2000;

    struct RuntimeInfo {
        std::string runtimeID;
        int port = -1;
        int grpcPort = -1;
        bool used = false;
    };

    std::map<int, RuntimeInfo> portMap_;
};
}

#endif // RUNTIME_MANAGER_PORT_PORT_MANAGER_H
