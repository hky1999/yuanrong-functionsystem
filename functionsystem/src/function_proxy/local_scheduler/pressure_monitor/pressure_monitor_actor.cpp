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

#include "pressure_monitor_actor.h"

#include <algorithm>

#include "common/logs/logging.h"
#include "common/proto/pb/posix_pb.h"
#include "common/resource_view/resource_type.h"
#include "common/types/instance_state.h"
#include "local_scheduler/pressure_monitor/park_victim.h"

namespace functionsystem::local_scheduler {

PressureMonitorActor::PressureMonitorActor(const std::string &name, const std::string &nodeID,
                                           const PressureMonitorConfig &config)
    : BasisActor(name), nodeID_(nodeID), config_(config)
{
}

void PressureMonitorActor::Init()
{
    YRLOG_INFO("pressure monitor started on node {}: interval={}ms park={} high={} low={} sustain={} ttl={}s "
               "maxParked={}",
               nodeID_, config_.checkIntervalMs, config_.enablePark, config_.highWatermark, config_.lowWatermark,
               config_.sustainSamples, config_.parkTtlSec, config_.maxParked);
    if (config_.upgradeLadder.enabled) {
        upgradeLadder_ = std::make_unique<UpgradeLadder>(nodeID_, config_.upgradeLadder,
                                                         config_.upgradeLadderCgroupRoot);
        YRLOG_INFO("upgrade ladder enabled on node {}: step={} cap={} safety={} highRatio={} root={}", nodeID_,
                   config_.upgradeLadder.stepBytes, config_.upgradeLadder.capBytes, config_.upgradeLadder.safetyRatio,
                   config_.upgradeLadder.highRatio, config_.upgradeLadderCgroupRoot);
    }
    if (config_.cpuUpgradeLadder.enabled) {
        cpuUpgradeLadder_ = std::make_unique<CpuUpgradeLadder>(nodeID_, config_.cpuUpgradeLadder,
                                                               config_.cpuUpgradeLadderCgroupRoot);
        YRLOG_INFO("cpu upgrade ladder enabled on node {}: stepRatio={} cap={}usec safety={} nodeCap={}milli root={}",
                   nodeID_, config_.cpuUpgradeLadder.stepRatio, config_.cpuUpgradeLadder.capQuotaUsec,
                   config_.cpuUpgradeLadder.safetyRatio, config_.cpuUpgradeLadder.nodeCapacityMilli,
                   config_.cpuUpgradeLadderCgroupRoot);
    }
    (void)litebus::AsyncAfter(config_.checkIntervalMs, GetAID(), &PressureMonitorActor::RunCycle);
}

void PressureMonitorActor::RunCycle()
{
    // reschedule first: a slow cross-actor hop must not stall the cadence
    (void)litebus::AsyncAfter(config_.checkIntervalMs, GetAID(), &PressureMonitorActor::RunCycle);
    SampleNow();
}

void PressureMonitorActor::SampleNow()
{
    if (idleMgr_ == nullptr || resourceView_ == nullptr || snapCtrl_ == nullptr) {
        YRLOG_DEBUG("pressure monitor on node {} not fully bound yet, skip cycle", nodeID_);
        return;
    }
    idleMgr_->GetIdleInstances().Then(
        [aid(GetAID())](const std::vector<std::string> &idleInstances) -> std::vector<std::string> {
            litebus::Async(aid, &PressureMonitorActor::OnIdleSet, idleInstances);
            return idleInstances;
        });
}

void PressureMonitorActor::OnIdleSet(const std::vector<std::string> &idleInstances)
{
    idleInstances_ = std::unordered_set<std::string>(idleInstances.begin(), idleInstances.end());
    resourceView_->GetResourceView().Then(
        [aid(GetAID())](const std::shared_ptr<resource_view::ResourceUnit> &view)
            -> std::shared_ptr<resource_view::ResourceUnit> {
            litebus::Async(aid, &PressureMonitorActor::OnResourceView, view);
            return view;
        });
}

void PressureMonitorActor::OnResourceView(const std::shared_ptr<resource_view::ResourceUnit> &view)
{
    lastRatio_ = -1.0;
    lastInstanceUseMb_.clear();
    if (view == nullptr) {
        Decide({});
        return;
    }

    const auto &actual = view->actualuse().resources();
    const auto &capacity = view->capacity().resources();

    // W-CPUL: the CPU ladder is cgroupfs-self-contained (scope rate from its
    // own cpu.stat reads, node capacity from config), so it ticks on every
    // sample regardless of whether the view carries a usable memory sample
    if (cpuUpgradeLadder_ != nullptr) {
        const bool cpuRaisedThisSample = cpuUpgradeLadder_->OnSample();
        if (!pendingCpuEventDir_.empty() && !cpuRaisedThisSample) {
            cpuUpgradeLadder_->OnThrottleEvent(pendingCpuEventDir_, pendingCpuEventIncrement_);
            pendingCpuEventDir_.clear();
        }
    }

    auto usedMem = actual.find(resource_view::MEMORY_RESOURCE_NAME);
    auto capMem = capacity.find(resource_view::MEMORY_RESOURCE_NAME);
    if (usedMem == actual.end() || capMem == capacity.end() || capMem->second.scalar().value() <= 0) {
        YRLOG_DEBUG("pressure monitor on node {}: no usable memory sample in resource view", nodeID_);
        snapCtrl_->GetParkedInstances().Then([aid(GetAID())](
            const std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> &parked)
            -> std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> {
            litebus::Async(aid, &PressureMonitorActor::Decide, parked);
            return parked;
        });
        return;
    }
    lastRatio_ = usedMem->second.scalar().value() / capMem->second.scalar().value();

    // D-3: the node usage refresh re-checks deferred ladder rungs, and a
    // pending throttle event (D-2 watcher) asks for one new rung; the view's
    // memory numbers are MB
    if (upgradeLadder_ != nullptr) {
        constexpr double bytesPerMb = 1024.0 * 1024.0;
        const auto actualBytes = static_cast<uint64_t>(usedMem->second.scalar().value() * bytesPerMb);
        const auto capacityBytes = static_cast<uint64_t>(capMem->second.scalar().value() * bytesPerMb);
        const bool raisedThisSample = upgradeLadder_->OnNodeUsage(actualBytes, capacityBytes);
        // D-3②: one rung per sample at most — when the deferred release above
        // already raised, the pending throttle event stays pending and is
        // replayed on the next sample instead of stacking a second rung
        if (!pendingEventDir_.empty() && !raisedThisSample) {
            upgradeLadder_->OnThrottleEvent(pendingEventDir_, pendingEventIncrement_);
            pendingEventDir_.clear();
        }
    }

    for (const auto &entry : view->instances()) {
        const auto &instUse = entry.second.actualuse().resources();
        auto mem = instUse.find(resource_view::MEMORY_RESOURCE_NAME);
        if (mem != instUse.end() && mem->second.scalar().value() > 0) {
            lastInstanceUseMb_[entry.first] = mem->second.scalar().value();
        }
    }

    snapCtrl_->GetParkedInstances().Then([aid(GetAID())](
        const std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> &parked)
        -> std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> {
        litebus::Async(aid, &PressureMonitorActor::Decide, parked);
        return parked;
    });
}

void PressureMonitorActor::Decide(
    const std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> &parked)
{
    // tick the wake cooldowns once per sampling cycle (D-4)
    for (auto it = wakeCooldown_.begin(); it != wakeCooldown_.end();) {
        if (--it->second == 0) {
            it = wakeCooldown_.erase(it);
        } else {
            ++it;
        }
    }

    if (lastRatio_ < 0 || !config_.enablePark) {
        // a ladder-only monitor has nobody to park/unpark: the ratio branches
        // below exist solely for the park feature (W-CPUL)
        highSamples_ = 0;
        return;
    }

    if (lastRatio_ >= config_.highWatermark) {
        if (++highSamples_ < config_.sustainSamples) {
            YRLOG_DEBUG("pressure monitor on node {}: ratio {} >= high {} (sample {}/{})", nodeID_, lastRatio_,
                        config_.highWatermark, highSamples_, config_.sustainSamples);
            return;
        }
        highSamples_ = config_.sustainSamples; // cap: stay triggered while high
        if (parked.size() >= config_.maxParked) {
            YRLOG_WARN("pressure monitor on node {}: ratio {} high but parked {} >= maxParked {}, no more parks",
                       nodeID_, lastRatio_, parked.size(), config_.maxParked);
            return;
        }

        // victims: fully idle + local RUNNING; D-4 eviction order = lowest
        // priority first (ties: largest reclaim)
        ASSERT_IF_NULL(instanceControlView_);
        std::vector<ParkCandidate> candidates;
        for (const auto &instanceID : idleInstances_) {
            auto stateMachine = instanceControlView_->GetInstance(instanceID);
            if (stateMachine == nullptr || stateMachine->GetInstanceState() != InstanceState::RUNNING) {
                continue;
            }
            ParkCandidate candidate;
            candidate.instanceID = instanceID;
            candidate.priority = stateMachine->GetInstanceInfo().scheduleoption().priority();
            auto use = lastInstanceUseMb_.find(instanceID);
            candidate.useMb = use == lastInstanceUseMb_.end() ? 0.0 : use->second;
            candidates.push_back(std::move(candidate));
        }
        auto victim = SelectParkVictim(std::move(candidates));
        if (!victim.has_value()) {
            YRLOG_INFO("pressure monitor on node {}: ratio {} high but no idle RUNNING victim", nodeID_, lastRatio_);
            return;
        }
        ParkVictim(victim->instanceID, victim->useMb, victim->priority);
        return;
    }

    highSamples_ = 0;
    if (lastRatio_ <= config_.lowWatermark && !parked.empty()) {
        // oldest parked instance that is not inside a wake cooldown; entries
        // whose restore was just kicked off stay in SnapCtrl's registry until
        // the (async) restore completes, and would otherwise be re-picked
        // every cycle (D-4 restore-storm guard)
        auto oldest = std::min_element(parked.begin(), parked.end(),
                                       [this](const auto &a, const auto &b) {
                                           if (wakeCooldown_.count(a.first) != wakeCooldown_.count(b.first)) {
                                               return wakeCooldown_.count(a.first) < wakeCooldown_.count(b.first);
                                           }
                                           if (wakeCooldown_.count(a.first) != 0) {
                                               return false; // both cooling down: keep order
                                           }
                                           return a.second.parkedAt < b.second.parkedAt;
                                       });
        if (oldest != parked.end() && wakeCooldown_.count(oldest->first) == 0) {
            UnparkOldest(*oldest);
        }
    }
}

void PressureMonitorActor::ParkVictim(const std::string &instanceID, double instanceUseMb, int32_t priority)
{
    ASSERT_IF_NULL(snapCtrl_);
    core_service::SnapOptions options;
    options.set_leaverunning(false);
    options.set_ttl(static_cast<int32_t>(config_.parkTtlSec));
    const auto requestID = "pressure-park-" + std::to_string(requestSeq_++);
    YRLOG_INFO("pressure monitor on node {}: parking instance({}) priority={} use={}MB ratio={} request={}", nodeID_,
               instanceID, priority, instanceUseMb, lastRatio_, requestID);
    snapCtrl_->HandleSnapshot(requestID, instanceID, options.SerializeAsString())
        .Then([instanceID, useMb(instanceUseMb)](const KillResponse &rsp) {
            if (rsp.code() != common::ERR_NONE) {
                YRLOG_WARN("pressure park of instance({}) failed: code={} msg={}", instanceID,
                           static_cast<uint32_t>(rsp.code()), rsp.message());
            } else {
                YRLOG_INFO("pressure park of instance({}) succeeded (use {}MB reclaimed)", instanceID, useMb);
            }
            return rsp;
        });
}

void PressureMonitorActor::UnparkOldest(const std::pair<std::string, SnapCtrlActor::ParkedEntry> &oldest)
{
    ASSERT_IF_NULL(snapCtrl_);
    const auto requestID = "pressure-unpark-" + std::to_string(requestSeq_++);
    wakeCooldown_[oldest.first] = kWakeCooldownCycles; // D-4 restore-storm guard
    YRLOG_INFO("pressure monitor on node {}: ratio {} low, FIFO unpark instance({}) from checkpoint({}) request={}",
               nodeID_, lastRatio_, oldest.first, oldest.second.checkpointID, requestID);
    snapCtrl_->HandleWake(requestID, oldest.first).Then([instanceID(oldest.first)](const KillResponse &rsp) {
        if (rsp.code() != common::ERR_NONE) {
            YRLOG_WARN("pressure FIFO unpark of instance({}) failed: code={} msg={}", instanceID,
                       static_cast<uint32_t>(rsp.code()), rsp.message());
        } else {
            YRLOG_INFO("pressure FIFO unpark of instance({}) succeeded", instanceID);
        }
        return rsp;
    });
}

}  // namespace functionsystem::local_scheduler
