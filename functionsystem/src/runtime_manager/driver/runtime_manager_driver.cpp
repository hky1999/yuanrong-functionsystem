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

#include "runtime_manager_driver.h"

#include "ckpt/ckpt_file_manager_actor.h"

#include "async/async.hpp"
#include "common/constants/actor_name.h"
#include "common/register/register_helper.h"
#include "port/port_manager.h"

namespace functionsystem::runtime_manager {

const std::string RUNTIME_MANAGER = "runtime-manager";

RuntimeManagerDriver::RuntimeManagerDriver(const Flags &flags, const std::string &componentName)
    : flags_(flags), componentName_(componentName)
{
    // W2 P0.4: flag-driven default checkpoint TTL (0 keeps the compiled-in default)
    CkptFileManagerActor::SetDefaultTtlOverride(flags_.GetCkptDefaultTtlSec());
    actor_ = std::make_shared<RuntimeManager>(flags_.GetNodeID() + RUNTIME_MANAGER_SRV_ACTOR_NAME,
                                              flags.GetLogReuseEnable());
    litebus::Spawn(actor_);
    PortManager::GetInstance().InitPortResource(flags_.GetRuntimeInitialPort(), flags_.GetPortNum());
    // create http server
    httpServer_ = std::make_shared<HttpServer>(RUNTIME_MANAGER);
    apiRouteRegister_ = std::make_shared<DefaultHealthyRouter>(flags_.GetNodeID());
    if (auto registerStatus(httpServer_->RegisterRoute(apiRouteRegister_)); registerStatus != StatusCode::SUCCESS) {
        YRLOG_ERROR("register health check api router failed.");
    }
}

Status RuntimeManagerDriver::Start()
{
    litebus::Async(actor_->GetAID(), &RuntimeManager::SetConfig, flags_);
    litebus::Async(actor_->GetAID(), &RuntimeManager::CollectCpuType);
    auto registerHelper = std::make_shared<RegisterHelper>(flags_.GetNodeID() + RUNTIME_MANAGER_SRV_ACTOR_NAME);
    registerHelper->SetComponentName(componentName_);
    litebus::Async(actor_->GetAID(), &RuntimeManager::SetRegisterHelper, registerHelper);
    litebus::Async(actor_->GetAID(), &RuntimeManager::Start);
    (void)litebus::Spawn(httpServer_);
    return Status::OK();
}

Status RuntimeManagerDriver::Stop()
{
    // handle timeout in GracefulShutdown, so didn't use litebus::After here
    (void)litebus::Async(actor_->GetAID(), &RuntimeManager::GracefulShutdown)
        .OnComplete([actorAID(actor_->GetAID()), httpAID(httpServer_->GetAID())](const litebus::Future<bool> &future) {
            if (future.IsError()) {
                YRLOG_ERROR("failed to GracefulShutdown");
            }
            litebus::Terminate(actorAID);
            litebus::Terminate(httpAID);
        });
    return Status::OK();
}

void RuntimeManagerDriver::Await()
{
    litebus::Await(actor_->GetAID());
    litebus::Await(httpServer_->GetAID());
}
}  // namespace functionsystem::runtime_manager
