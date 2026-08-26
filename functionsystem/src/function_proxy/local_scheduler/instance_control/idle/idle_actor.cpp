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

#include "idle_actor.h"

#include "async/async.hpp"
#include "async/asyncafter.hpp"
#include "common/logs/logging.h"
#include "common/types/instance_state.h"
#include "common/utils/struct_transfer.h"
#include "local_scheduler/instance_control/instance_ctrl_actor.h"

namespace functionsystem::local_scheduler {

namespace {
constexpr int64_t SECONDS_TO_MILLISECONDS = 1000;
}

std::atomic<int64_t> IdleActor::orphanGraceSec_{0};

void IdleActor::SetOrphanGraceSec(int64_t graceSec)
{
    orphanGraceSec_.store(graceSec);
}

int64_t IdleActor::GetOrphanGraceSec()
{
    return orphanGraceSec_.load();
}

IdleActor::IdleActor(const std::string &name,
                     const std::string &nodeID,
                     const std::shared_ptr<InstanceControlView> &instanceControlView,
                     const litebus::AID &facadeAID)
    : BasisActor(name), nodeID_(nodeID), instanceControlView_(instanceControlView), facadeAID_(facadeAID)
{
}

void IdleActor::Init()
{
}

void IdleActor::Finalize()
{
    for (auto &[instanceID, timer] : idleTimers_) {
        litebus::TimerTools::Cancel(timer);
    }
    idleTimers_.clear();
}

void IdleActor::TrafficReport(const std::string &instanceID, const size_t &processingNum)
{
    YRLOG_DEBUG("debug:: instance({}) processing num: {}", instanceID, processingNum);
    bool isIdle = (processingNum == 0);
    ASSERT_IF_NULL(instanceControlView_);
    if (!isIdle) {
        // Busy traffic alone does NOT mark the instance ever-used: control-plane
        // invokes (frontend heartbeat/readiness calls) would immediately promote
        // every fresh instance to the full idle timeout and the orphan window
        // could never apply. Only a real exec session claims ownership
        // (SessionCountDelta); while the frontend keeps invoking, each busy->idle
        // cycle simply re-arms the (short) orphan timer, so an abandoned create
        // whose frontend went silent is reclaimed one grace window later.
        instanceTrafficIdle_.erase(instanceID);
        CancelIdleTimer(instanceID);
        return;
    }

    instanceTrafficIdle_[instanceID] = true;

    // Only start idle timer if both traffic idle AND no active sessions
    bool hasActiveSessions = false;
    auto it = instanceActiveSessions_.find(instanceID);
    if (it != instanceActiveSessions_.end()) {
        hasActiveSessions = it->second;
    }

    if (!hasActiveSessions) {
        StartIdleTimer(instanceID);
    } else {
        YRLOG_DEBUG("instance({}) is idle but has active exec sessions, skip idle timer", instanceID);
    }
}

void IdleActor::SessionCountDelta(const std::string &instanceID, int delta)
{
    if (instanceID.empty() || delta == 0) {
        return;
    }

    auto &count = instanceSessionCounts_[instanceID];
    size_t oldCount = count;

    if (delta > 0) {
        instanceEverUsed_[instanceID] = true;
        count += static_cast<size_t>(delta);
    } else if (delta < 0 && count > 0) {
        size_t dec = static_cast<size_t>(-delta);
        count = (dec >= count) ? 0 : (count - dec);
    }

    size_t newCount = count;
    if (newCount == 0) {
        instanceSessionCounts_.erase(instanceID);
    }

    // Edge detection: 0->N or N->0
    if ((oldCount == 0 && newCount > 0) || (oldCount > 0 && newCount == 0)) {
        bool hasActiveSessions = (newCount > 0);
        YRLOG_INFO("instance({}) session count edge: {} sessions, hasActiveSessions={}",
                   instanceID, newCount, hasActiveSessions);
        SessionAlive(instanceID, hasActiveSessions);
    }
}

void IdleActor::SessionAlive(const std::string &instanceID, bool hasActiveSessions)
{
    YRLOG_INFO("instance({}) session alive status changed: hasActiveSessions={}", instanceID, hasActiveSessions);

    if (hasActiveSessions) {
        instanceActiveSessions_[instanceID] = true;
        // Cancel idle timer when sessions become active
        CancelIdleTimer(instanceID);
    } else {
        instanceActiveSessions_.erase(instanceID);
        // When sessions become inactive, check traffic idle status before starting timer
        bool trafficIdle = false;
        auto trafficIt = instanceTrafficIdle_.find(instanceID);
        if (trafficIt != instanceTrafficIdle_.end()) {
            trafficIdle = trafficIt->second;
        }
        ASSERT_IF_NULL(instanceControlView_);
        auto stateMachine = instanceControlView_->GetInstance(instanceID);
        if (trafficIdle && stateMachine != nullptr) {
            const auto &instanceInfo = stateMachine->GetInstanceInfo();
            if (instanceInfo.functionproxyid() == nodeID_ &&
                instanceInfo.instancestatus().code() == static_cast<int32_t>(InstanceState::RUNNING)) {
                StartIdleTimer(instanceID);
            }
        }
    }
}

void IdleActor::MarkInstanceUsed(const std::string &instanceID)
{
    if (instanceID.empty()) {
        return;
    }
    bool alreadyUsed = false;
    auto usedIt = instanceEverUsed_.find(instanceID);
    if (usedIt != instanceEverUsed_.end()) {
        alreadyUsed = usedIt->second;
    }
    if (alreadyUsed) {
        return;
    }
    instanceEverUsed_[instanceID] = true;
    YRLOG_INFO("instance({}) marked ever-used by client action invoke", instanceID);
    // An armed timer may still carry the short orphan grace deadline; restart
    // it so the full idle timeout governs from now on. If the instance is
    // busy with this very invoke, the upcoming busy TrafficReport cancels the
    // restarted timer anyway.
    if (idleTimers_.find(instanceID) != idleTimers_.end()) {
        CancelIdleTimer(instanceID);
        StartIdleTimer(instanceID);
    }
}

void IdleActor::OnInstanceRunning(const std::string &instanceID)
{
    if (instanceID.empty()) {
        return;
    }
    // No traffic record yet counts as idle: a fresh instance has never
    // served anything. This is the create-race orphan path — the old early
    // return left never-used instances without a timer forever (no exec ->
    // no traffic report -> no eviction), so an abandoned late create held
    // its admission slot indefinitely.
    auto trafficIt = instanceTrafficIdle_.find(instanceID);
    if (trafficIt != instanceTrafficIdle_.end() && !trafficIt->second) {
        return;
    }
    auto sessionIt = instanceActiveSessions_.find(instanceID);
    if (sessionIt != instanceActiveSessions_.end() && sessionIt->second) {
        YRLOG_DEBUG("instance({}) is running and idle but has active exec sessions, skip idle timer", instanceID);
        return;
    }
    StartIdleTimer(instanceID);
}

void IdleActor::OnInstanceParked(const std::string &instanceID)
{
    if (instanceID.empty()) {
        return;
    }
    // D-5 F1: a leaveRunning=false snapshot deleted the instance but the ID
    // comes back at restore. Drop every per-ID trace now, or the pre-park
    // timer fires against the restored instance on its old deadline (and
    // StartIdleTimer no-ops while the stale idleTimers_ entry lingers).
    CancelIdleTimer(instanceID);
    instanceTrafficIdle_.erase(instanceID);
    instanceActiveSessions_.erase(instanceID);
    instanceSessionCounts_.erase(instanceID);
    instanceEverUsed_.erase(instanceID);
    YRLOG_INFO("instance({}) parked: idle bookkeeping cleared for ID reuse", instanceID);
}

void IdleActor::StartIdleTimer(const std::string &instanceID)
{
    if (idleTimers_.find(instanceID) != idleTimers_.end()) {
        return;
    }
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return;
    }
    const auto &instanceInfo = stateMachine->GetInstanceInfo();
    if (instanceInfo.functionproxyid() != nodeID_ ||
        instanceInfo.instancestatus().code() != static_cast<int32_t>(InstanceState::RUNNING)) {
        return;
    }

    // Don't start timer if there are active sessions
    auto it = instanceActiveSessions_.find(instanceID);
    if (it != instanceActiveSessions_.end() && it->second) {
        YRLOG_INFO("skip starting idle timer for instance({}) due to active sessions", instanceID);
        return;
    }

    int64_t idleTimeout = GetIdleTimeout(instanceInfo);
    // Create-race orphan window: an instance that has never been used (no
    // exec session, no busy traffic) is reclaimed after the shorter of the
    // orphan grace and the configured idle timeout. Disabled (0) keeps the
    // plain idle-timeout semantics for every instance.
    bool everUsed = false;
    auto usedIt = instanceEverUsed_.find(instanceID);
    if (usedIt != instanceEverUsed_.end()) {
        everUsed = usedIt->second;
    }
    int64_t grace = GetOrphanGraceSec();
    if (!everUsed && grace > 0 && (idleTimeout <= 0 || grace < idleTimeout)) {
        YRLOG_INFO("instance({}) has never been used, applying orphan grace {}s instead of idle timeout {}s",
                   instanceID, grace, idleTimeout);
        idleTimeout = grace;
    }
    if (idleTimeout <= 0) {
        return;
    }

    // Stamp the timer with current generation to enable stale-callback detection
    auto gen = ++instanceTimerGeneration_[instanceID];
    YRLOG_INFO("start idle timer for instance({}) with timeout {} seconds (gen={})", instanceID, idleTimeout, gen);
    idleTimers_[instanceID] = litebus::AsyncAfter(
        idleTimeout * SECONDS_TO_MILLISECONDS, GetAID(), &IdleActor::HandleIdleTimeout, instanceID, gen);
}

void IdleActor::CancelIdleTimer(const std::string &instanceID)
{
    // Increment generation first: invalidates any in-flight timeout callback
    // that is already queued in this actor's mailbox but hasn't executed yet.
    ++instanceTimerGeneration_[instanceID];

    auto iter = idleTimers_.find(instanceID);
    if (iter == idleTimers_.end()) {
        return;
    }
    YRLOG_INFO("cancel idle timer for instance({})", instanceID);
    litebus::TimerTools::Cancel(iter->second);
    idleTimers_.erase(iter);
}

void IdleActor::HandleIdleTimeout(const std::string &instanceID, uint64_t generation)
{
    // Check whether this callback is stale (generation was incremented by CancelIdleTimer
    // after the timer fired but before this callback executed in the actor mailbox).
    auto genIt = instanceTimerGeneration_.find(instanceID);
    if (genIt != instanceTimerGeneration_.end() && genIt->second != generation) {
        YRLOG_INFO("instance({}) idle timeout callback is stale (gen={} vs current={}), skip",
                   instanceID, generation, genIt->second);
        return;
    }

    idleTimers_.erase(instanceID);
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return;
    }

    // Double-check: ensure no active sessions before requesting eviction
    auto it = instanceActiveSessions_.find(instanceID);
    if (it != instanceActiveSessions_.end() && it->second) {
        YRLOG_INFO("{}|instance({}) idle timeout cancelled due to active sessions",
                   stateMachine->GetInstanceInfo().requestid(), instanceID);
        return;
    }

    const auto &instanceInfo = stateMachine->GetInstanceInfo();
    // D-5 F1: only a live RUNNING instance owned by this node may be evicted.
    // The parked-restore path reuses the instanceID, so a timer armed before
    // the park can fire while the restored instance is still CREATING /
    // SCHEDULING (or against a foreign instance after a failover) -- evicting
    // there killed restore-in-flight instances live (D-4 runs 6/8).
    if (instanceInfo.functionproxyid() != nodeID_
        || stateMachine->GetInstanceState() != InstanceState::RUNNING) {
        YRLOG_INFO("instance({}) idle timeout skipped: state {} is not RUNNING on node {} (parked/restore path)",
                   instanceID, static_cast<int32_t>(stateMachine->GetInstanceState()), nodeID_);
        return;
    }
    YRLOG_INFO("{}|instance({}) idle timeout, requesting eviction via InstanceCtrlActor",
               instanceInfo.requestid(), instanceID);

    litebus::Async(facadeAID_, &InstanceCtrlActor::EvictByIdleTimeout, instanceID);
}

std::vector<std::string> IdleActor::GetIdleInstances()
{
    std::vector<std::string> out;
    out.reserve(instanceTrafficIdle_.size());
    for (const auto &entry : instanceTrafficIdle_) {
        if (!entry.second) {
            continue;
        }
        auto it = instanceActiveSessions_.find(entry.first);
        if (it != instanceActiveSessions_.end() && it->second) {
            continue;
        }
        out.push_back(entry.first);
    }
    return out;
}

}  // namespace functionsystem::local_scheduler
