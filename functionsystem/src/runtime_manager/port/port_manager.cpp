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

#include "port_manager.h"

#include <algorithm>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unordered_set>

#include "common/logs/logging.h"

namespace functionsystem::runtime_manager {
const static int MAX_PORT_NUM = 65535;

void PortManager::InitPortResource(int initialPort, int portNum)
{
    YRLOG_INFO("Init port resource, initial port: {}, portNum: {}", initialPort, portNum);
    portMap_.clear();
    nextPort_ = -1;
    ready_ = false;
    while (portNum > 0) {
        if (portNum > MAX_PORT_NUM) {
            YRLOG_ERROR("exceed port number limit. number is {}", portNum);
            return;
        }
        RuntimeInfo info;
        info.port = initialPort;
        portMap_[initialPort] = info;
        initialPort++;
        portNum--;
    }
    if (!portMap_.empty()) {
        nextPort_ = portMap_.begin()->first;
    }
    ready_ = true;
}

void PortManager::BeginReconcile()
{
    ready_ = false;
}

Status PortManager::ReservePorts(const std::string &runtimeID, const std::vector<int> &ports)
{
    if (runtimeID.empty()) {
        return Status(StatusCode::RUNTIME_MANAGER_PORT_UNAVAILABLE,
                      "cannot restore port reservations for an empty runtime ID");
    }

    std::unordered_set<int> uniquePorts;
    for (const int port : ports) {
        if (!uniquePorts.insert(port).second) {
            return Status(StatusCode::RUNTIME_MANAGER_PORT_UNAVAILABLE,
                          "duplicate persisted port " + std::to_string(port) + " for runtime " + runtimeID);
        }
        const auto iter = portMap_.find(port);
        if (iter == portMap_.end()) {
            return Status(StatusCode::RUNTIME_MANAGER_PORT_UNAVAILABLE,
                          "persisted port " + std::to_string(port) + " for runtime " + runtimeID +
                              " is outside the configured port pool");
        }
        if (iter->second.used && iter->second.runtimeID != runtimeID) {
            return Status(StatusCode::RUNTIME_MANAGER_PORT_UNAVAILABLE,
                          "persisted port " + std::to_string(port) + " for runtime " + runtimeID +
                              " conflicts with owner " + iter->second.runtimeID);
        }
    }

    for (const int port : ports) {
        auto &info = portMap_.at(port);
        info.used = true;
        info.runtimeID = runtimeID;
        info.port = port;
    }
    if (!ports.empty()) {
        YRLOG_INFO("restored {} port reservations for runtimeID: {}", ports.size(), runtimeID);
    }
    return Status::OK();
}

bool PortManager::RebuildPorts(const ReservationMap &reservations)
{
    auto rebuilt = portMap_;
    for (auto &[port, info] : rebuilt) {
        info.runtimeID.clear();
        info.port = port;
        info.grpcPort = -1;
        info.used = false;
    }

    std::unordered_set<int> reserved;
    for (const auto &[runtimeID, ports] : reservations) {
        if (runtimeID.empty()) {
            YRLOG_ERROR("authoritative port rebuild contains an empty runtime identity");
            return false;
        }
        for (const int port : ports) {
            const auto iter = rebuilt.find(port);
            if (iter == rebuilt.end()) {
                YRLOG_ERROR("authoritative port {} for runtime({}) is outside the configured pool", port, runtimeID);
                return false;
            }
            if (!reserved.insert(port).second) {
                YRLOG_ERROR("authoritative port {} is reported by more than one runtime", port);
                return false;
            }
            iter->second.runtimeID = runtimeID;
            iter->second.port = port;
            iter->second.used = true;
        }
    }
    portMap_.swap(rebuilt);
    nextPort_ = portMap_.empty() ? -1 : portMap_.begin()->first;
    if (!reserved.empty()) {
        AdvanceAllocationCursor(*std::max_element(reserved.begin(), reserved.end()));
    }
    ready_ = true;
    return true;
}

bool PortManager::IsReady() const
{
    return ready_;
}

std::string PortManager::RequestPort(const std::string &runtimeID)
{
    YRLOG_INFO("runtimeID: {}, request port", runtimeID);
    if (!ready_) {
        YRLOG_ERROR("PortManager is reconciling sandboxd physical facts; reject allocation for runtime({})", runtimeID);
        return "";
    }
    if (portMap_.size() == 0) {
        YRLOG_ERROR("PortManager port map is empty, request port failed");
        return "";
    }
    auto iter = portMap_.lower_bound(nextPort_);
    for (size_t scanned = 0; scanned < portMap_.size(); ++scanned) {
        if (iter == portMap_.end()) {
            iter = portMap_.begin();
        }
        const int port = iter->first;
        auto &info = iter->second;
        ++iter;
        if (info.used) {
            continue;
        }
        if (CheckPortInUse(port)) {
            YRLOG_INFO("port: {} is inuse, continue", port);
            continue;
        }
        info.used = true;
        info.runtimeID = runtimeID;
        info.port = port;
        AdvanceAllocationCursor(port);
        return std::to_string(port);
    }
    return "";
}

bool PortManager::CheckPortInUse(int port) const
{
    int socketFd;
    struct sockaddr_in sin;
    socketFd = socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd == -1) {
        return true;
    }
    bzero(&sin, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(static_cast<unsigned short>(port));
    inet_pton(AF_INET, "0.0.0.0", &sin.sin_addr);

    if (bind(socketFd, (struct sockaddr *)(&sin), sizeof(struct sockaddr)) < 0) {
        close(socketFd);
        return true;
    }
    close(socketFd);
    return false;
}

PortManager::PortManager()
{
    InitPortResource(initialPort_, poolSize_);
}

PortManager::~PortManager()
{
    portMap_.clear();
}

std::string PortManager::GetPort(const std::string &runtimeID) const
{
    for (const auto &iter : portMap_) {
        std::string id = iter.second.runtimeID;
        if (id == runtimeID && iter.second.used) {
            return std::to_string(iter.second.port);
        }
    }
    return "";
}

int PortManager::ReleasePort(const std::string &runtimeID)
{
    for (auto &iter : portMap_) {
        std::string id = iter.second.runtimeID;
        if (id == runtimeID) {
            iter.second.runtimeID = "";
            iter.second.used = false;
            iter.second.port = 0;
            iter.second.grpcPort = 0;
            YRLOG_INFO("port manager release port: {}, runtimeID: {}", iter.first, id);
            return 0;
        }
    }
    YRLOG_ERROR("port manager has not record this runtime resource, id: {}", runtimeID);
    return -1;
}

void PortManager::Clear()
{
    portMap_.clear();
    nextPort_ = -1;
    ready_ = false;
}

std::vector<int> PortManager::RequestPorts(const std::string &runtimeID, int count)
{
    if (!ready_) {
        YRLOG_ERROR("PortManager is reconciling sandboxd physical facts; reject allocation for runtime({})", runtimeID);
        return {};
    }
    if (count <= 0) {
        return {};
    }
    auto existing = GetPorts(runtimeID);
    if (!existing.empty()) {
        if (static_cast<int>(existing.size()) == count) {
            return existing;
        }
        YRLOG_ERROR("RequestPorts conflicts with existing cache: runtimeID({}) count({}) existing({})",
                    runtimeID, count, existing.size());
        return {};
    }
    std::vector<int> allocated;
    allocated.reserve(count);
    auto iter = portMap_.lower_bound(nextPort_);
    for (size_t scanned = 0; scanned < portMap_.size()
         && static_cast<int>(allocated.size()) < count; ++scanned) {
        if (iter == portMap_.end()) {
            iter = portMap_.begin();
        }
        const int port = iter->first;
        auto &info = iter->second;
        ++iter;
        if (info.used) {
            continue;
        }
        if (CheckPortInUse(port)) {
            YRLOG_INFO("port: {} is inuse, skip for runtimeID: {}", port, runtimeID);
            continue;
        }
        info.used = true;
        info.runtimeID = runtimeID;
        info.port = port;
        allocated.push_back(port);
    }
    if (static_cast<int>(allocated.size()) < count) {
        YRLOG_ERROR("RequestPorts failed: needed {} but got {} for runtimeID: {}", count, allocated.size(), runtimeID);
        // Roll back all partial allocations
        for (int port : allocated) {
            auto iter = portMap_.find(port);
            if (iter != portMap_.end()) {
                iter->second.used = false;
                iter->second.runtimeID = "";
                iter->second.port = 0;
                iter->second.grpcPort = 0;
            }
        }
        return {};
    }
    AdvanceAllocationCursor(allocated.back());
    return allocated;
}

bool PortManager::ReconcileRuntimePorts(const std::string &runtimeID, const std::vector<int> &ports)
{
    if (!ready_ || runtimeID.empty()) {
        return false;
    }
    std::unordered_set<int> requested;
    for (const int port : ports) {
        const auto iter = portMap_.find(port);
        if (iter == portMap_.end() || !requested.insert(port).second
            || (iter->second.used && iter->second.runtimeID != runtimeID)) {
            return false;
        }
    }

    auto reconciled = portMap_;
    for (auto &[port, info] : reconciled) {
        if (info.used && info.runtimeID == runtimeID) {
            info.runtimeID.clear();
            info.port = port;
            info.grpcPort = -1;
            info.used = false;
        }
    }
    for (const int port : ports) {
        auto &info = reconciled.at(port);
        info.runtimeID = runtimeID;
        info.port = port;
        info.used = true;
    }
    portMap_.swap(reconciled);
    return true;
}

std::vector<int> PortManager::GetPorts(const std::string &runtimeID) const
{
    std::vector<int> ports;
    for (const auto &[port, info] : portMap_) {
        if (info.used && info.runtimeID == runtimeID) {
            ports.push_back(port);
        }
    }
    return ports;
}
void PortManager::ReleasePorts(const std::string &runtimeID)
{
    for (auto &iter : portMap_) {
        if (iter.second.runtimeID == runtimeID && iter.second.used) {
            YRLOG_INFO("port manager release port: {}, runtimeID: {}", iter.first, runtimeID);
            iter.second.runtimeID = "";
            iter.second.used = false;
            iter.second.port = 0;
            iter.second.grpcPort = 0;
        }
    }
}

void PortManager::AdvanceAllocationCursor(int port)
{
    if (portMap_.empty()) {
        nextPort_ = -1;
        return;
    }
    auto next = portMap_.upper_bound(port);
    if (next == portMap_.end()) {
        next = portMap_.begin();
    }
    nextPort_ = next->first;
}
}  // namespace functionsystem::runtime_manager
