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
#include "instance_clients.h"

#include "common/logs/logging.h"
#include "function_proxy/common/posix_client/shared_client/shared_client.h"

namespace functionsystem {

std::shared_ptr<BaseClient> InstanceClients::InsertClient(const std::string &instanceID,
                                                          const std::shared_ptr<BaseClient> &client)
{
    client->Start();
    instanceClients_[instanceID] = client;
    if (instanceClientPromises_.find(instanceID) != instanceClientPromises_.end()) {
        YRLOG_INFO("insert instance({}) client set promise", instanceID);
        instanceClientPromises_[instanceID].SetValue(client);
        (void)instanceClientPromises_.erase(instanceID);
    }
    return client;
}

Status InstanceClients::DeleteClient(const std::string &instanceID)
{
    if (instanceClientPromises_.find(instanceID) != instanceClientPromises_.end()) {
        std::shared_ptr<BaseClient> nil = nullptr;
        instanceClientPromises_[instanceID].SetValue(nil);
        (void)instanceClientPromises_.erase(instanceID);
    }
    if (instanceClients_.find(instanceID) == instanceClients_.end()) {
        return Status(SUCCESS);
    }
    YRLOG_WARN("delete instance({}) client", instanceID);
    instanceClients_[instanceID]->Close();
    (void)instanceClients_.erase(instanceID);
    return Status(SUCCESS);
}

std::shared_ptr<BaseClient> InstanceClients::GetClient(const std::string &instanceID)
{
    auto ite = instanceClients_.find(instanceID);
    if (ite == instanceClients_.end()) {
        YRLOG_WARN("instance({}) client not found", instanceID);
        return nullptr;
    }
    return ite->second;
}

litebus::Future<std::shared_ptr<BaseClient>> InstanceClients::GetReadyClient(const std::string &instanceID)
{
    if (instanceClients_.find(instanceID) != instanceClients_.end()) {
        YRLOG_DEBUG("get instance({}) client existed", instanceID);
        return instanceClients_[instanceID];
    }
    if (instanceClientPromises_.find(instanceID) != instanceClientPromises_.end()) {
        YRLOG_WARN("get instance({}) client not existed", instanceID);
        return instanceClientPromises_[instanceID].GetFuture();
    }
    YRLOG_WARN("get instance({}) client not existed. new promise", instanceID);
    litebus::Promise<std::shared_ptr<BaseClient>> promise;
    instanceClientPromises_[instanceID] = promise;
    return promise.GetFuture();
}

litebus::Future<std::shared_ptr<ControlInterfacePosixClient>> InstanceClients::UpdateClient(
    const NewClientInfo &newClientInfo, const std::shared_ptr<grpc::PosixClient> &posix)
{
    YRLOG_INFO("update posix for runtime({}) client for instance({}), address {}", newClientInfo.runtimeID,
               newClientInfo.instanceID, newClientInfo.address);
    auto client = GetClient(newClientInfo.instanceID);
    if (client != nullptr) {
        posix->Start();
        // Retag with the runtime this transport was opened for: an in-flight
        // runtime-pinned send either completes on the old transport or is
        // rejected by the refreshed label — it can never be redirected to the
        // new runtime's connection.
        client->UpdatePosix(posix, newClientInfo.runtimeID);
        return std::dynamic_pointer_cast<ControlInterfacePosixClient>(client);
    }
    auto newClient = std::make_shared<SharedClient>(posix, newClientInfo.runtimeID);
    (void)InsertClient(newClientInfo.instanceID, newClient);
    return newClient;
}

InstanceClients::~InstanceClients()
{
    for (const auto &it : instanceClients_) {
        it.second->Close();
    }
    instanceClients_.clear();
}

}  // namespace functionsystem