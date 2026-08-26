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

#include <cstdlib>
#include <vector>

#include "async/async.hpp"
#include "async/asyncafter.hpp"
#include "common/logs/logging.h"
#include "common/types/instance_state.h"
#include "common/utils/struct_transfer.h"
#include "local_scheduler/instance_control/instance_ctrl_actor.h"

namespace functionsystem::local_scheduler {

namespace {
bool IsSamePauseGateIdentity(const resources::InstanceInfo &left, const resources::InstanceInfo &right)
{
    return left.instanceid() == right.instanceid()
        && left.requestid() == right.requestid()
        && left.version() == right.version()
        && left.functionproxyid() == right.functionproxyid()
        && left.runtimeid() == right.runtimeid()
        && left.functionagentid() == right.functionagentid()
        && left.containerid() == right.containerid()
        && left.unitid() == right.unitid()
        && left.tenantid() == right.tenantid()
        && left.runtimeaddress() == right.runtimeaddress()
        && left.instancestatus().code() == right.instancestatus().code();
}

constexpr int64_t SECONDS_TO_MILLISECONDS = 1000;
constexpr int64_t DEFAULT_COMMAND_ACTIVITY_TIMEOUT_SECONDS = 30;

int64_t PositiveEnvSeconds(const char *name, int64_t fallback)
{
    const char *raw = std::getenv(name);
    if (raw == nullptr) {
        return fallback;
    }
    try {
        const auto value = std::stoll(raw);
        return value > 0 ? value : fallback;
    } catch (...) {
        return fallback;
    }
}
}  // namespace

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
    const char *enabled = std::getenv("YR_COMMAND_RECOVERY_ENABLED");
    commandActivityEnabled_ = enabled != nullptr && std::string(enabled) != "0" && std::string(enabled) != "false";
    commandActivityTimeoutSeconds_ =
        PositiveEnvSeconds("YR_COMMAND_ACTIVITY_TIMEOUT_SECS", DEFAULT_COMMAND_ACTIVITY_TIMEOUT_SECONDS);
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
    pauseGatedInstances_.clear();
    for (auto &[instanceID, timer] : commandActivityTimers_) {
        (void)instanceID;
        litebus::TimerTools::Cancel(timer);
    }
    commandActivityTimers_.clear();
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
        if (pauseGatedInstances_.find(instanceID) == pauseGatedInstances_.end()) {
            CancelIdleTimer(instanceID);
        }
        return;
    }

    instanceTrafficIdle_[instanceID] = true;
    TryStartIdleTimer(instanceID);
}

void IdleActor::CommandActivityReport(const std::string &instanceID, const size_t &activeCommands)
{
    if (!commandActivityEnabled_ || instanceID.empty()) {
        return;
    }
    const auto generation = ++commandActivityGenerations_[instanceID];
    auto timer = commandActivityTimers_.find(instanceID);
    if (timer != commandActivityTimers_.end()) {
        litebus::TimerTools::Cancel(timer->second);
    }
    commandActivityTimers_[instanceID] = litebus::AsyncAfter(
        commandActivityTimeoutSeconds_ * SECONDS_TO_MILLISECONDS, GetAID(),
        &IdleActor::HandleCommandActivityTimeout, instanceID, generation);
    instanceCommandCounts_[instanceID] = activeCommands;
    if (activeCommands > 0) {
        CancelIdleTimer(instanceID);
    } else {
        TryStartIdleTimer(instanceID);
    }
}

void IdleActor::HandleCommandActivityTimeout(const std::string &instanceID, uint64_t generation)
{
    const auto current = commandActivityGenerations_.find(instanceID);
    if (current == commandActivityGenerations_.end() || current->second != generation) {
        return;
    }
    commandActivityTimers_.erase(instanceID);
    instanceCommandCounts_.erase(instanceID);
    CancelIdleTimer(instanceID);
    YRLOG_WARN("instance({}) command activity lease expired; pausing idle eviction", instanceID);
}

void IdleActor::GatewayActivityReconcile(const GatewayActivityCounts &activeStreamCounts)
{
    instanceGatewayStreamCounts_ = activeStreamCounts;
    gatewayActivityUnknown_ = false;

    // Re-evaluate every instance whose traffic source is known. This is
    // important when the first valid batch arrives after an outage: an
    // instance may have become traffic-idle while timers were globally paused.
    for (const auto &[instanceID, trafficIdle] : instanceTrafficIdle_) {
        (void)trafficIdle;
        const auto gatewayIt = instanceGatewayStreamCounts_.find(instanceID);
        if (gatewayIt != instanceGatewayStreamCounts_.end() && gatewayIt->second > 0) {
            CancelIdleTimer(instanceID);
        } else {
            TryStartIdleTimer(instanceID);
        }
    }
}

bool IdleActor::GatewayActivityUnavailable()
{
    gatewayActivityUnknown_ = true;
    std::vector<std::string> instances;
    instances.reserve(idleTimers_.size());
    for (const auto &[instanceID, timer] : idleTimers_) {
        (void)timer;
        instances.emplace_back(instanceID);
    }
    for (const auto &instanceID : instances) {
        CancelIdleTimer(instanceID);
    }
    return true;
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
        if (pauseGatedInstances_.find(instanceID) != pauseGatedInstances_.end()) {
            return;
        }
        // Cancel idle timer when sessions become active
        CancelIdleTimer(instanceID);
    } else {
        instanceActiveSessions_.erase(instanceID);
        TryStartIdleTimer(instanceID);
    }
}

void IdleActor::OnInstanceRunning(const resources::InstanceInfo &identity)
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
    const auto &instanceID = identity.instanceid();
    if (instanceID.empty() || instanceControlView_ == nullptr) {
        return;
    }
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return;
    }
    const auto current = stateMachine->GetInstanceInfo();
    if (current.functionproxyid() != nodeID_
        || current.instancestatus().code() != static_cast<int32_t>(InstanceState::RUNNING)
        || !IsSamePauseGateIdentity(identity, current)) {
        return;
    }
    if (auto gate = pauseGatedInstances_.find(instanceID); gate != pauseGatedInstances_.end()) {
        if (IsSamePauseGateIdentity(gate->second.identity, identity)) {
            return;
        }
        pauseGatedInstances_.erase(gate);
    }
    // A freshly RUNNING sandbox is idle until one of the independent busy
    // sources says otherwise. The data-plane path bypasses FunctionProxy, so
    // there may never have been an initial TrafficReport(0) to seed this map.
    // Preserve an already-observed busy report that raced ahead of RUNNING.
    // No traffic record yet counts as idle (create-race orphan path): the
    // old early return left never-used instances without a timer forever,
    // so an abandoned late create held its admission slot indefinitely.
    instanceTrafficIdle_.try_emplace(instanceID, true);
    TryStartIdleTimer(instanceID);
}

void IdleActor::TryStartIdleTimer(const std::string &instanceID)
{
    if (pauseGatedInstances_.find(instanceID) != pauseGatedInstances_.end()) {
        return;
    }
    const auto trafficIt = instanceTrafficIdle_.find(instanceID);
    if (trafficIt != instanceTrafficIdle_.end() && !trafficIt->second) {
        return;
    }
    if (gatewayActivityUnknown_) {
        YRLOG_INFO("instance({}) idle timeout cancelled because gateway activity is unknown", instanceID);
        return;
    }
    if (commandActivityEnabled_) {
        const auto commandIt = instanceCommandCounts_.find(instanceID);
        if (commandIt == instanceCommandCounts_.end() || commandIt->second > 0) {
            return;
        }
    }
    const auto sessionIt = instanceActiveSessions_.find(instanceID);
    if (sessionIt != instanceActiveSessions_.end() && sessionIt->second) {
        return;
    }
    const auto gatewayIt = instanceGatewayStreamCounts_.find(instanceID);
    if (gatewayIt != instanceGatewayStreamCounts_.end() && gatewayIt->second > 0) {
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
    if (pauseGatedInstances_.find(instanceID) != pauseGatedInstances_.end()) {
        return;
    }
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
    auto gatewayIt = instanceGatewayStreamCounts_.find(instanceID);
    if (gatewayIt != instanceGatewayStreamCounts_.end() && gatewayIt->second > 0) {
        YRLOG_INFO("skip starting idle timer for instance({}) due to active gateway streams", instanceID);
        return;
    }
    if (gatewayActivityUnknown_) {
        YRLOG_INFO("skip starting idle timer for instance({}) because gateway activity is unknown", instanceID);
        return;
    }
    if (commandActivityEnabled_) {
        const auto commandIt = instanceCommandCounts_.find(instanceID);
        if (commandIt == instanceCommandCounts_.end() || commandIt->second > 0) {
            YRLOG_INFO("skip starting idle timer for instance({}) due to unknown or active commands", instanceID);
            return;
        }
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
    if (pauseGatedInstances_.find(instanceID) != pauseGatedInstances_.end()) {
        return;
    }
    ASSERT_IF_NULL(instanceControlView_);
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return;
    }

    // Double-check every independent busy source before requesting eviction.
    auto trafficIt = instanceTrafficIdle_.find(instanceID);
    if (trafficIt == instanceTrafficIdle_.end() || !trafficIt->second) {
        return;
    }
    auto it = instanceActiveSessions_.find(instanceID);
    if (it != instanceActiveSessions_.end() && it->second) {
        YRLOG_INFO("{}|instance({}) idle timeout cancelled due to active sessions",
                   stateMachine->GetInstanceInfo().requestid(), instanceID);
        return;
    }
    auto gatewayIt = instanceGatewayStreamCounts_.find(instanceID);
    if (gatewayIt != instanceGatewayStreamCounts_.end() && gatewayIt->second > 0) {
        YRLOG_INFO("{}|instance({}) idle timeout cancelled due to active gateway streams",
                   stateMachine->GetInstanceInfo().requestid(), instanceID);
        return;
    }
    if (gatewayActivityUnknown_) {
        YRLOG_INFO("{}|instance({}) idle timeout cancelled because gateway activity is unknown",
                   stateMachine->GetInstanceInfo().requestid(), instanceID);
        return;
    }
    if (commandActivityEnabled_) {
        const auto commandIt = instanceCommandCounts_.find(instanceID);
        if (commandIt == instanceCommandCounts_.end() || commandIt->second > 0) {
            YRLOG_INFO("{}|instance({}) idle timeout cancelled due to unknown or active commands",
                       stateMachine->GetInstanceInfo().requestid(), instanceID);
            return;
        }
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

Status IdleActor::SetPauseGated(const resources::InstanceInfo &identity, uint64_t token, bool gated)
{
    const auto &instanceID = identity.instanceid();
    if (instanceID.empty()) {
        return Status(StatusCode::PARAMETER_ERROR, "instance ID is empty");
    }
    if (token == 0 || instanceControlView_ == nullptr) {
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID, "idle pause gate identity is not available");
    }
    auto stateMachine = instanceControlView_->GetInstance(instanceID);
    if (stateMachine == nullptr) {
        return Status(StatusCode::ERR_INSTANCE_NOT_FOUND, "idle pause gate instance not found");
    }
    const auto current = stateMachine->GetInstanceInfo();
    if (current.functionproxyid() != nodeID_
        || current.instancestatus().code() != static_cast<int32_t>(InstanceState::RUNNING)
        || !IsSamePauseGateIdentity(identity, current)) {
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID, "idle pause gate identity changed");
    }
    if (gated) {
        auto existing = pauseGatedInstances_.find(instanceID);
        if (existing != pauseGatedInstances_.end()) {
            if (existing->second.token == token && IsSamePauseGateIdentity(existing->second.identity, identity)) {
                return Status::OK();
            }
            return Status(StatusCode::ERR_INSTANCE_BUSY, "idle pause gate belongs to another operation");
        }
        PauseGateRecord record;
        record.identity.CopyFrom(identity);
        record.token = token;
        pauseGatedInstances_.emplace(instanceID, std::move(record));
        CancelIdleTimer(instanceID);
        return Status::OK();
    }

    auto existing = pauseGatedInstances_.find(instanceID);
    if (existing == pauseGatedInstances_.end()) {
        return Status::OK();
    }
    if (existing->second.token != token || !IsSamePauseGateIdentity(existing->second.identity, identity)) {
        return Status(StatusCode::ERR_INSTANCE_INFO_INVALID, "idle pause gate token changed");
    }
    pauseGatedInstances_.erase(existing);
    TryStartIdleTimer(instanceID);
    return Status::OK();
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
