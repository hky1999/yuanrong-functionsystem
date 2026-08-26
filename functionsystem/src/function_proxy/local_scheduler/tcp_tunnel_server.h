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

#ifndef FUNCTIONSYSTEM_FUNCTION_PROXY_TCP_TUNNEL_SERVER_H
#define FUNCTIONSYSTEM_FUNCTION_PROXY_TCP_TUNNEL_SERVER_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <openssl/ssl.h>

#include "common/state_machine/instance_control_view.h"
#include "function_proxy/local_scheduler/instance_control/idle/idle_mgr.h"

namespace functionsystem::local_scheduler {

struct TcpTunnelServerConfig {
    std::string listenIP;
    uint16_t listenPort{ 0 };
    std::string nodeID;
    bool enableTLS{ false };
    uint32_t maxConnections{ 1024 };
    std::string rootCert;
    std::string moduleCert;
    std::string moduleKey;
};

int ResolvePublishedTCPPort(const std::string &portForwardMetadata, int targetPort, std::string &error);

class TcpTunnelServer {
public:
    TcpTunnelServer(TcpTunnelServerConfig config, std::shared_ptr<InstanceControlView> instanceView,
                    std::shared_ptr<IdleMgr> idleMgr);
    ~TcpTunnelServer();

    bool Start();
    void Stop();

private:
    struct ClientSession {
        SSL *ssl{ nullptr };
        int clientFd{ -1 };
        int backendFd{ -1 };
        std::string instanceID;
        std::string requestID;
        bool counted{ false };
    };

    struct Worker {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> completed;
    };

    void AcceptLoop();
    void HandleClient(int clientFd);
    void ReapWorkers(bool waitForAll = false);
    bool ServeClient(ClientSession &session);
    void CloseClient(ClientSession &session);
    bool ConfigureTLS();
    int ResolveHostPort(const std::string &instanceID, int targetPort, std::string &error) const;
    std::string ResolveContainerIP(const std::string &instanceID, std::string &error) const;
    bool Relay(SSL *ssl, int clientFd, int backendFd) const;

    TcpTunnelServerConfig config_;
    std::shared_ptr<InstanceControlView> instanceView_;
    std::shared_ptr<IdleMgr> idleMgr_;
    SSL_CTX *sslContext_{ nullptr };
    int listenFd_{ -1 };
    std::atomic<bool> running_{ false };
    std::thread acceptThread_;
    std::mutex clientsMutex_;
    std::unordered_set<int> clients_;
    std::mutex workersMutex_;
    std::vector<Worker> workers_;
};

}  // namespace local_scheduler

#endif
