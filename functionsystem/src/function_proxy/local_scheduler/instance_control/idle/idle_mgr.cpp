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

#include "idle_mgr.h"

#include <async/async.hpp>

#include "idle_actor.h"

namespace functionsystem::local_scheduler {

IdleMgr::IdleMgr(std::shared_ptr<IdleActor> idleActor) : idleActor_(std::move(idleActor)) {}

IdleMgr::~IdleMgr() = default;

void IdleMgr::Spawn()
{
    litebus::Spawn(idleActor_);
}

void IdleMgr::Stop()
{
    litebus::Terminate(idleActor_->GetAID());
}

void IdleMgr::Await()
{
    litebus::Await(idleActor_->GetAID());
    idleActor_ = nullptr;
}

void IdleMgr::TrafficReport(const std::string &instanceID, const size_t &processingNum)
{
    litebus::Async(idleActor_->GetAID(), &IdleActor::TrafficReport, instanceID, processingNum);
}

void IdleMgr::CommandActivityReport(const std::string &instanceID, const size_t &activeCommands)
{
    litebus::Async(idleActor_->GetAID(), &IdleActor::CommandActivityReport, instanceID, activeCommands);
}

void IdleMgr::GatewayActivityReconcile(const GatewayActivityCounts &activeStreamCounts)
{
    litebus::Async(idleActor_->GetAID(), &IdleActor::GatewayActivityReconcile, activeStreamCounts);
}

void IdleMgr::GatewayActivityUnavailable()
{
    // Startup readiness depends on UNKNOWN being visible before the activity
    // service accepts reports, so wait for the actor transition to complete.
    (void)litebus::Async(idleActor_->GetAID(), &IdleActor::GatewayActivityUnavailable).Get();
}

void IdleMgr::SessionCountDelta(const std::string &instanceID, int delta)
{
    litebus::Async(idleActor_->GetAID(), &IdleActor::SessionCountDelta, instanceID, delta);
}

void IdleMgr::OnInstanceRunning(const resources::InstanceInfo &identity)
{
    litebus::Async(idleActor_->GetAID(), &IdleActor::OnInstanceRunning, identity);
}

litebus::Future<Status> IdleMgr::SetPauseGated(const resources::InstanceInfo &identity, uint64_t token, bool gated)
{
    if (idleActor_ == nullptr) {
        return Status(StatusCode::FAILED, "idle actor is not available");
    }
    return litebus::Async(idleActor_->GetAID(), &IdleActor::SetPauseGated, identity, token, gated);
}

void IdleMgr::MarkInstanceUsed(const std::string &instanceID)
{
    litebus::Async(idleActor_->GetAID(), &IdleActor::MarkInstanceUsed, instanceID);
}

void IdleMgr::OnInstanceParked(const std::string &instanceID)
{
    litebus::Async(idleActor_->GetAID(), &IdleActor::OnInstanceParked, instanceID);
}

litebus::Future<std::vector<std::string>> IdleMgr::GetIdleInstances()
{
    return litebus::Async(idleActor_->GetAID(), &IdleActor::GetIdleInstances);
}

}  // namespace functionsystem::local_scheduler
