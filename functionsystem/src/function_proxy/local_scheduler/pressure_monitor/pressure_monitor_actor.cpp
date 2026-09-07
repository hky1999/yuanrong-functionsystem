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
#include <cmath>
#include <limits>

#include "common/logs/logging.h"
#include "common/resource_view/restore_admission.h"
#include <async/uuid_generator.hpp>
#include "common/proto/pb/posix_pb.h"
#include "common/resource_view/resource_type.h"
#include "common/types/instance_state.h"
#include "local_scheduler/pressure_monitor/park_victim.h"
#include "local_scheduler/pressure_monitor/resource_sample_policy.h"
#include "local_scheduler/pressure_monitor/pressure_resize_policy.h"

namespace functionsystem::local_scheduler {
namespace {
// Bound on the LLM quiescence observation age re-checked at checkpoint
// dispatch (matches the client-side parse bound of 3000ms).
constexpr int64_t PARK_OBSERVATION_MAX_AGE_MS = 3000;
} // namespace

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
    // All pressure growth uses the generation-conditional, shared-budget RPC.
    // Never instantiate the historical direct-cgroup writers here.
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
    const auto sample = sampleGate_.Begin();
    if (!sample.has_value()) {
        return; // new pressure sources remain in their coalescing queues
    }
    const auto generation = *sample;
    const auto timeoutMs = std::max<uint64_t>(1000, static_cast<uint64_t>(config_.checkIntervalMs) * 2);
    (void)litebus::AsyncAfter(timeoutMs, GetAID(), &PressureMonitorActor::OnSampleTimeout, generation);
    idleMgr_->GetIdleInstances().Then(
        [aid(GetAID()), generation](const std::vector<std::string> &idleInstances) -> std::vector<std::string> {
            litebus::Async(aid, &PressureMonitorActor::OnIdleSet, generation, idleInstances);
            return idleInstances;
        });
}

void PressureMonitorActor::OnSampleTimeout(uint64_t generation)
{
    if (!sampleGate_.Finish(generation)) {
        return;
    }
    lastRatio_ = -1.0;
    lastNodeMemorySampleMs_ = 0;
    highSamples_ = 0;
    YRLOG_WARN("pressure monitor on node {}: sampling generation {} timed out; late callbacks ignored",
               nodeID_, generation);
}

void PressureMonitorActor::OnIdleSet(uint64_t generation, const std::vector<std::string> &idleInstances)
{
    if (!sampleGate_.Active(generation)) {
        return;
    }
    idleInstances_ = std::unordered_set<std::string>(idleInstances.begin(), idleInstances.end());
    resourceView_->GetResourceView().Then(
        [aid(GetAID()), generation](const std::shared_ptr<resource_view::ResourceUnit> &view)
            -> std::shared_ptr<resource_view::ResourceUnit> {
            litebus::Async(aid, &PressureMonitorActor::OnResourceView, generation, view);
            return view;
        });
}

void PressureMonitorActor::OnResourceView(uint64_t generation, const std::shared_ptr<resource_view::ResourceUnit> &view)
{
    if (!sampleGate_.Active(generation)) {
        return;
    }
    lastRatio_ = -1.0;
    lastInstanceSamples_.clear();
    if (view == nullptr) {
        Decide(generation, {});
        return;
    }

    const auto &actual = view->actualuse().resources();
    const auto &capacity = view->capacity().resources();

    const auto observedAtMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    lastNodeMemorySampleMs_ = UsableNodeMemorySampleTime(*view, observedAtMs, config_.resourceSampleMaxAgeMs);
    auto usedMem = actual.find(resource_view::MEMORY_RESOURCE_NAME);
    auto capMem = capacity.find(resource_view::MEMORY_RESOURCE_NAME);
    constexpr double maxSafeMb = static_cast<double>(std::numeric_limits<uint64_t>::max()) / (1024.0 * 1024.0);
    if (lastNodeMemorySampleMs_ == 0 || usedMem == actual.end() || capMem == capacity.end()
        || !std::isfinite(usedMem->second.scalar().value()) || usedMem->second.scalar().value() < 0
        || !std::isfinite(capMem->second.scalar().value()) || capMem->second.scalar().value() <= 0
        || usedMem->second.scalar().value() >= maxSafeMb || capMem->second.scalar().value() >= maxSafeMb) {
        YRLOG_DEBUG("pressure monitor on node {}: no usable memory sample in resource view", nodeID_);
        snapCtrl_->GetParkedInstances().Then([aid(GetAID()), generation](
            const std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> &parked)
            -> std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> {
            litebus::Async(aid, &PressureMonitorActor::Decide, generation, parked);
            return parked;
        });
        return;
    }
    lastRatio_ = usedMem->second.scalar().value() / capMem->second.scalar().value();

    ResizeUnderPressure(*view, observedAtMs);

    for (const auto &entry : view->instances()) {
        if (entry.second.has_sandbox_resource_sample()) {
            lastInstanceSamples_[entry.first] = entry.second.sandbox_resource_sample();
        }
    }

    snapCtrl_->GetParkedInstances().Then([aid(GetAID()), generation](
        const std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> &parked)
        -> std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> {
        litebus::Async(aid, &PressureMonitorActor::Decide, generation, parked);
        return parked;
    });
}

void PressureMonitorActor::ResizeUnderPressure(const resource_view::ResourceUnit &view, int64_t nowMs)
{
    if (!functionAgentMgr_ || !instanceControlView_
        || (!config_.upgradeLadder.enabled && !config_.cpuUpgradeLadder.enabled)) { return; }
    // Watcher paths are hints only; authoritative per-generation Stats drives growth.
    while (pendingMemoryEvents_.Pop().has_value()) {}
    while (pendingCpuEvents_.Pop().has_value()) {}
    // Remove observation baselines, never unresolved operations, for absent instances.
    for (auto it = resizeSamples_.begin(); it != resizeSamples_.end();) {
        if (view.instances().find(it->first) == view.instances().end()) {
            resizeAfterMs_.erase(it->first);
            it = resizeSamples_.erase(it);
        }
        else { ++it; }
    }
    for (const auto &entry : view.instances()) {
        // A pending physical-hold park preparation owns this instance's next
        // transition; a resize proposal racing it would change the physical
        // generation mid-hold.
        if (parkPreparations_.count(entry.first) != 0) { continue; }
        const auto &info = entry.second;
        const auto machine = instanceControlView_->GetInstance(entry.first);
        if (!machine || machine->GetInstanceState() != InstanceState::RUNNING
            || machine->GetInstanceInfo().runtimeid() != info.runtimeid()
            || !resource_view::UsesElasticAdmission(info)) { continue; }
        const auto &sample = info.sandbox_resource_sample();
        const auto previous = resizeSamples_.find(entry.first);
        auto target = PressureResizeTarget(info, previous == resizeSamples_.end() ? nullptr : &previous->second,
            nowMs, config_.resourceSampleMaxAgeMs, config_.upgradeLadder.enabled,
            config_.cpuUpgradeLadder.enabled, config_.upgradeLadder.stepBytes, config_.cpuUpgradeLadder.stepRatio,
            config_.upgradeLadder.capBytes, config_.cpuUpgradeLadder.capQuotaUsec);
        const auto pending = pressureResizes_.find(entry.first);
        if (pending != pressureResizes_.end()) {
            if (!pending->second.unknown || !PressureResizeTargetObserved(pending->second.request.resize(),
                    pending->second.request.runtimeid(), sample,
                    nowMs, config_.resourceSampleMaxAgeMs)) { continue; }
            YRLOG_INFO("pressure resize instance={} request={} target reconciled from post-deadline Stats",
                       entry.first, pending->second.requestID);
            pressureResizes_.erase(pending);
            // Only this monitor's operation gate is cleared; the ResourceView
            // owns the existing reservation and reconciles it independently.
        }
        const auto after = resizeAfterMs_.find(entry.first);
        if (after != resizeAfterMs_.end() && sample.sampled_at_unix_ms() <= after->second) { continue; }
        // A duplicate/stale observation must not issue a second growth proposal.
        if (previous != resizeSamples_.end() && previous->second.resource_generation() == sample.resource_generation()
            && previous->second.sampled_at_unix_ms() >= sample.sampled_at_unix_ms()) { continue; }
        if (resizeSamples_.size() >= 4096 && previous == resizeSamples_.end()) { continue; }
        resizeSamples_[entry.first] = sample;
        if (!target || pressureResizes_.size() >= 1024) { continue; }
        auto request = std::make_shared<messages::ResizeSandboxRequest>();
        request->set_requestid(litebus::uuid_generator::UUID::GetRandomUUID().ToString());
        request->set_instanceid(entry.first);
        request->set_runtimeid(info.runtimeid());
        auto *command = request->mutable_resize();
        command->set_sandbox_id(sample.sandbox_id());
        command->set_resource_generation(sample.resource_generation());
        command->set_deadline_unix_ms(nowMs + 15000);
        command->mutable_expected()->set_cpu_quota_usec(sample.cpu_quota_usec());
        command->mutable_expected()->set_cpu_period_usec(sample.cpu_period_usec());
        command->mutable_expected()->set_memory_limit_bytes(sample.memory_limit_bytes());
        *command->mutable_target() = *target;
        pressureResizes_[entry.first] = {request->requestid(), nowMs, *request, false};
        functionAgentMgr_->ResizeSandbox(info, request).OnComplete(
            [aid = GetAID(), id = entry.first, requestID = request->requestid()](
                const litebus::Future<messages::ResizeSandboxResponse> &result) {
                litebus::Async(aid, &PressureMonitorActor::OnPressureResize, id, requestID, result);
            });
    }
}

void PressureMonitorActor::OnPressureResize(const std::string &instanceID, const std::string &requestID,
    const litebus::Future<messages::ResizeSandboxResponse> &result)
{
    const auto pending = pressureResizes_.find(instanceID);
    if (pending == pressureResizes_.end() || pending->second.requestID != requestID) { return; }
    if (!result.IsOK() || result.Get().requestid() != requestID || result.Get().instanceid() != instanceID
        || (result.Get().outcome() != messages::ResizeSandboxResponse::OBSERVED
            && result.Get().outcome() != messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH)) {
        pending->second.unknown = true;
        YRLOG_WARN("pressure resize instance={} request={} remains unknown; reconciliation required", instanceID, requestID);
        return;
    }
    // Require a sample newer than dispatch before another proposal. Shared
    // ResourceView keeps its own reservation until authoritative readback.
    auto baseline = resizeSamples_.find(instanceID);
    if (baseline != resizeSamples_.end()) {
        resizeAfterMs_[instanceID] = pending->second.dispatchedAtMs;
    }
    YRLOG_INFO("pressure resize instance={} request={} outcome={} code={}", instanceID, requestID,
               static_cast<int>(result.Get().outcome()), result.Get().code());
    pressureResizes_.erase(pending);
}

void PressureMonitorActor::Decide(
    uint64_t generation, const std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>> &parked)
{
    if (!sampleGate_.Finish(generation)) {
        return;
    }

    // tick the wake cooldowns once per sampling cycle (D-4)
    for (auto it = wakeCooldown_.begin(); it != wakeCooldown_.end();) {
        if (--it->second == 0) {
            it = wakeCooldown_.erase(it);
        } else {
            ++it;
        }
    }

    const auto decisionAtMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    if (lastRatio_ < 0 || !config_.enablePark || lastNodeMemorySampleMs_ <= 0
        || lastNodeMemorySampleMs_ > decisionAtMs
        || decisionAtMs - lastNodeMemorySampleMs_ > config_.resourceSampleMaxAgeMs) {
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
        // In-flight preparations count against the cap: without them a burst
        // of prepares/snapshots could park past maxParked before any of them
        // lands in SnapCtrl's registry. The subtraction cannot underflow --
        // the first clause short-circuits parked.size() >= maxParked.
        if (parked.size() >= config_.maxParked
            || parkPreparations_.size() >= config_.maxParked - parked.size()) {
            YRLOG_WARN("pressure monitor on node {}: ratio {} high but parked {} (+{} preparing) >= maxParked {}, "
                       "no more parks", nodeID_, lastRatio_, parked.size(), parkPreparations_.size(),
                       config_.maxParked);
            return;
        }

        // victims: fully idle + local RUNNING; D-4 eviction order = lowest
        // priority first (ties: largest reclaim)
        ASSERT_IF_NULL(instanceControlView_);
        std::vector<ParkCandidate> candidates;
        const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        for (const auto &instanceID : idleInstances_) {
            if (pressureResizes_.count(instanceID) != 0 || parkPreparations_.count(instanceID) != 0) { continue; }
            auto stateMachine = instanceControlView_->GetInstance(instanceID);
            if (stateMachine == nullptr || stateMachine->GetInstanceState() != InstanceState::RUNNING) {
                continue;
            }
            const auto observation = lastInstanceSamples_.find(instanceID);
            if (observation == lastInstanceSamples_.end()
                || !ResourceSampleAllowsPark(observation->second, stateMachine->GetInstanceInfo().runtimeid(), nowMs,
                                            config_.resourceSampleMaxAgeMs, config_.maxIdleCpuCores)) {
                continue;
            }
            ParkCandidate candidate;
            candidate.instanceID = instanceID;
            candidate.priority = stateMachine->GetInstanceInfo().scheduleoption().priority();
            candidate.useMb = static_cast<double>(observation->second.memory_current_bytes()) / (1024.0 * 1024.0);
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
    ASSERT_IF_NULL(instanceControlView_);
    // Fail closed: without the physical LLM hold client the proxy cannot take
    // ownership of the sandbox's LLM delivery, so the historical
    // direct-snapshot park path must not run.
    if (physicalLLMClient_ == nullptr) {
        YRLOG_WARN("pressure monitor on node {}: park of instance({}) skipped, physical LLM client not bound", nodeID_,
                   instanceID);
        return;
    }
    // One active preparation per instance: a duplicate attempt must not
    // create a second hold; the map itself is bounded.
    if (parkPreparations_.count(instanceID) != 0 || parkPreparations_.size() >= kMaxParkPreparations) {
        return;
    }
    const auto source = instanceControlView_->GetInstance(instanceID);
    if (source == nullptr || source->GetInstanceState() != InstanceState::RUNNING) {
        return;
    }
    const auto observation = lastInstanceSamples_.find(instanceID);
    if (observation == lastInstanceSamples_.end()) {
        return; // no resource sample to anchor the park identity to
    }
    PhysicalLLMIdentity identity;
    identity.instanceID = instanceID;
    identity.sandboxID = observation->second.sandbox_id();
    identity.runtimeID = observation->second.runtime_id();
    identity.generation = observation->second.resource_generation();
    if (!identity.Valid() || identity.runtimeID != source->GetInstanceInfo().runtimeid()) {
        return;
    }
    ParkPreparation preparation;
    preparation.attemptID = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
    preparation.identity = identity;
    preparation.source = source;
    preparation.useMb = instanceUseMb;
    preparation.priority = priority;
    const auto attemptID = preparation.attemptID;
    parkPreparations_[instanceID] = std::move(preparation);
    YRLOG_INFO("pressure monitor on node {}: preparing physical hold for instance({}) priority={} use={}MB ratio={} "
               "attempt={}", nodeID_, instanceID, priority, instanceUseMb, lastRatio_, attemptID);
    // Async only: the actor never blocks on the hold exchange. The prepare
    // carries no token, so any hold that comes back is attributable to this
    // attempt alone.
    physicalLLMClient_->Prepare(identity).OnComplete(
        [aid(GetAID()), instanceID, attemptID](const litebus::Future<PhysicalHoldResult> &result) {
            litebus::Async(aid, &PressureMonitorActor::OnParkPrepared, instanceID, attemptID, result);
        });
}

void PressureMonitorActor::OnParkPrepared(const std::string &instanceID, const std::string &attemptID,
    const litebus::Future<PhysicalHoldResult> &result)
{
    const auto pending = parkPreparations_.find(instanceID);
    if (pending == parkPreparations_.end() || pending->second.attemptID != attemptID
        || pending->second.stage != ParkPreparation::Stage::PREPARING) {
        return; // late callback or non-initial stage: never mutates a replacement attempt's entry
    }
    if (!result.IsOK()) {
        YRLOG_WARN("pressure park prepare of instance({}) attempt={} outcome unknown; retained for reconciliation",
                   instanceID, attemptID);
        return;
    }
    const auto &hold = result.Get();
    switch (hold.outcome) {
    case PhysicalHoldOutcome::LOCAL_REJECTED:
    case PhysicalHoldOutcome::UNCHANGED:
        // Rejected before any server write, or "unchanged" on a hold this
        // tokenless Prepare did not create: no hold is ours to unwind.
        YRLOG_INFO("pressure park prepare of instance({}) attempt={} created no hold (outcome {})", instanceID,
                   attemptID, static_cast<int>(hold.outcome));
        parkPreparations_.erase(pending);
        return;
    case PhysicalHoldOutcome::HELD:
        // Busy or otherwise unusable observation, but the token proves our
        // hold exists: roll it back, record survives until RELEASED.
        pending->second.token = hold.token;
        YRLOG_INFO("pressure park prepare of instance({}) attempt={} held but not quiescent, releasing", instanceID,
                   attemptID);
        BeginParkRelease(instanceID, attemptID);
        return;
    case PhysicalHoldOutcome::READY:
        pending->second.token = hold.token;
        pending->second.observedAtMs = hold.observedAtMs;
        pending->second.stage = ParkPreparation::Stage::READY;
        if (idleMgr_ == nullptr) {
            BeginParkRelease(instanceID, attemptID);
            return;
        }
        // Fresh idle snapshot next: the eligibility facts are re-read, not
        // trusted from the decision cycle.
        idleMgr_->GetIdleInstances().OnComplete(
            [aid(GetAID()), instanceID, attemptID](const litebus::Future<std::vector<std::string>> &idleInstances) {
                litebus::Async(aid, &PressureMonitorActor::OnParkIdleChecked, instanceID, attemptID, idleInstances);
            });
        return;
    case PhysicalHoldOutcome::UNKNOWN:
    case PhysicalHoldOutcome::RELEASED:
    default:
        // Unknown transport/malformed outcome (RELEASED is nonsensical for a
        // prepare): a hold may exist whose token was not confirmed. Retain;
        // never release with an empty or foreign token.
        YRLOG_WARN("pressure park prepare of instance({}) attempt={} unconfirmed outcome; retained for reconciliation",
                   instanceID, attemptID);
        return;
    }
}

void PressureMonitorActor::OnParkIdleChecked(const std::string &instanceID, const std::string &attemptID,
    const litebus::Future<std::vector<std::string>> &idleInstances)
{
    const auto pending = parkPreparations_.find(instanceID);
    if (pending == parkPreparations_.end() || pending->second.attemptID != attemptID
        || pending->second.stage != ParkPreparation::Stage::READY) {
        return; // late callback or an attempt already rolling back/dispatching
    }
    const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const auto reject = [&](const char *reason) {
        YRLOG_INFO("pressure park of instance({}) attempt={} recheck failed ({}); releasing hold", instanceID, attemptID,
                   reason);
        BeginParkRelease(instanceID, attemptID);
    };
    if (!idleInstances.IsOK()) {
        reject("idle query failed");
        return;
    }
    const auto &idle = idleInstances.Get();
    if (std::find(idle.begin(), idle.end(), instanceID) == idle.end()) {
        reject("instance no longer fully idle");
        return;
    }
    // Limitation retained on purpose: the idle set is traffic/session idle
    // only. It does not yet fully classify agent tool activity (background
    // shells, sub-agents), so this filter is conservative, not a proof; the
    // unknown-CPU gate below carries the same intent.
    const auto &preparation = pending->second;
    if (preparation.source == nullptr || instanceControlView_ == nullptr
        || instanceControlView_->GetInstance(instanceID) != preparation.source) {
        reject("state machine replaced");
        return;
    }
    if (preparation.source->GetInstanceState() != InstanceState::RUNNING
        || preparation.source->GetInstanceInfo().runtimeid() != preparation.identity.runtimeID) {
        reject("captured source no longer the RUNNING runtime");
        return;
    }
    const auto observation = lastInstanceSamples_.find(instanceID);
    if (observation == lastInstanceSamples_.end()
        || observation->second.sandbox_id() != preparation.identity.sandboxID
        || observation->second.resource_generation() != preparation.identity.generation
        || observation->second.runtime_id() != preparation.identity.runtimeID
        || !ResourceSampleAllowsPark(observation->second, preparation.identity.runtimeID, nowMs,
                                     config_.resourceSampleMaxAgeMs, config_.maxIdleCpuCores)) {
        reject("resource sample stale or diverged from captured identity");
        return;
    }
    if (!std::isfinite(lastRatio_) || !std::isfinite(config_.highWatermark) || lastRatio_ < config_.highWatermark
        || lastNodeMemorySampleMs_ <= 0 || lastNodeMemorySampleMs_ > nowMs
        || nowMs - lastNodeMemorySampleMs_ > config_.resourceSampleMaxAgeMs) {
        reject("node memory pressure no longer fresh above the high watermark");
        return;
    }
    if (pressureResizes_.count(instanceID) != 0) {
        reject("resize in flight");
        return;
    }
    if (preparation.observedAtMs <= 0 || preparation.observedAtMs > nowMs
        || nowMs - preparation.observedAtMs > PARK_OBSERVATION_MAX_AGE_MS) {
        reject("LLM quiescence observation too old");
        return;
    }
    // Every recheck passed: hand the park to SnapCtrl with the hold token and
    // the ORIGINAL captured identity tuple — no new identity query happens
    // here; SnapCtrl re-fences the same source pointer and the daemon re-checks
    // the physical pair under its resource lock.
    core_service::SnapOptions options;
    options.set_leaverunning(false);
    options.set_ttl(static_cast<int32_t>(config_.parkTtlSec));
    options.set_hold_token(preparation.token);
    options.set_expected_runtime_id(preparation.identity.runtimeID);
    options.set_expected_sandbox_id(preparation.identity.sandboxID);
    options.set_expected_resource_generation(preparation.identity.generation);
    pending->second.stage = ParkPreparation::Stage::SNAPSHOT;
    // Private attempt UUID, not the reusable per-process sequence: a retried
    // park must never collide with a completed predecessor's request traces.
    const auto requestID = "pressure-park-" + attemptID;
    YRLOG_INFO("pressure monitor on node {}: parking instance({}) priority={} use={}MB ratio={} attempt={} request={}",
               nodeID_, instanceID, preparation.priority, preparation.useMb, lastRatio_, attemptID, requestID);
    snapCtrl_->HandleSnapshotIfSource(requestID, instanceID, options.SerializeAsString(), preparation.source)
        .OnComplete([aid(GetAID()), instanceID, attemptID](const litebus::Future<KillResponse> &rsp) {
            litebus::Async(aid, &PressureMonitorActor::OnParkSnapshotDone, instanceID, attemptID, rsp);
        });
}

void PressureMonitorActor::OnParkSnapshotDone(const std::string &instanceID, const std::string &attemptID,
    const litebus::Future<KillResponse> &rsp)
{
    const auto pending = parkPreparations_.find(instanceID);
    if (pending == parkPreparations_.end() || pending->second.attemptID != attemptID
        || pending->second.stage != ParkPreparation::Stage::SNAPSHOT) {
        return;
    }
    if (!rsp.IsOK()) {
        YRLOG_WARN("pressure park of instance({}) attempt={} transport outcome unknown; hold retained for "
                   "reconciliation", instanceID, attemptID);
        return;
    }
    const auto &response = rsp.Get();
    if (response.code() == common::ERR_NONE) {
        // Success: SnapCtrl's ParkedEntry now owns the checkpoint and the
        // hold token; this preparation record retires with the handover.
        YRLOG_INFO("pressure park of instance({}) attempt={} succeeded (use {}MB reclaimed)", instanceID, attemptID,
                   pending->second.useMb);
        parkPreparations_.erase(pending);
        return;
    }
    // Nonzero result: the checkpoint may still have physically happened (the
    // source sandbox may be stopped or deleted). Keep the record and the hold
    // for reconciliation instead of blindly releasing.
    YRLOG_WARN("pressure park of instance({}) attempt={} failed code={} msg={}; outcome unknown, hold retained",
               instanceID, attemptID, static_cast<uint32_t>(response.code()), response.message());
}

void PressureMonitorActor::BeginParkRelease(const std::string &instanceID, const std::string &attemptID)
{
    const auto pending = parkPreparations_.find(instanceID);
    if (pending == parkPreparations_.end() || pending->second.attemptID != attemptID) {
        return;
    }
    // Pre-snapshot cleanup only: once the checkpoint is dispatched (SNAPSHOT)
    // the hold's fate belongs to the park outcome/reconciliation, and a
    // release already in flight (RELEASING) must not be re-issued.
    if (pending->second.stage == ParkPreparation::Stage::SNAPSHOT
        || pending->second.stage == ParkPreparation::Stage::RELEASING) {
        return;
    }
    if (physicalLLMClient_ == nullptr || pending->second.token.empty()) {
        // No reliable handle on the hold: keep the record rather than guessing.
        YRLOG_WARN("pressure park preparation of instance({}) attempt={} cannot release; retained for reconciliation",
                   instanceID, attemptID);
        return;
    }
    pending->second.stage = ParkPreparation::Stage::RELEASING;
    // Exact captured identity plus our own token only: no wildcard hold, no
    // touching another attempt's release.
    physicalLLMClient_->Release(pending->second.identity, pending->second.token).OnComplete(
        [aid(GetAID()), instanceID, attemptID](const litebus::Future<PhysicalHoldResult> &result) {
            litebus::Async(aid, &PressureMonitorActor::OnParkReleased, instanceID, attemptID, result);
        });
}

void PressureMonitorActor::OnParkReleased(const std::string &instanceID, const std::string &attemptID,
    const litebus::Future<PhysicalHoldResult> &result)
{
    const auto pending = parkPreparations_.find(instanceID);
    if (pending == parkPreparations_.end() || pending->second.attemptID != attemptID
        || pending->second.stage != ParkPreparation::Stage::RELEASING) {
        return; // late callback: a replacement attempt owns the entry now
    }
    if (result.IsOK() && result.Get().outcome == PhysicalHoldOutcome::RELEASED
        && result.Get().token == pending->second.token) {
        YRLOG_INFO("pressure park hold of instance({}) attempt={} released", instanceID, attemptID);
        parkPreparations_.erase(pending);
        return;
    }
    // Anything else (transport error, malformed body, local rejection) leaves
    // the hold's existence unresolved: the record stays for authoritative
    // reconciliation; nothing here retries it blindly.
    YRLOG_WARN("pressure park release of instance({}) attempt={} unconfirmed; retained for reconciliation", instanceID,
               attemptID);
}

void PressureMonitorActor::UnparkOldest(const std::pair<std::string, SnapCtrlActor::ParkedEntry> &oldest)
{
    ASSERT_IF_NULL(snapCtrl_);
    const auto requestID = "pressure-unpark-" + std::to_string(requestSeq_++);
    wakeCooldown_[oldest.first] = kWakeCooldownCycles; // D-4 restore-storm guard
    YRLOG_INFO("pressure monitor on node {}: ratio {} low, FIFO unpark instance({}) from checkpoint({}) request={}",
               nodeID_, lastRatio_, oldest.first, oldest.second.checkpointID, requestID);
    snapCtrl_->HandleWake(requestID, oldest.first).Then([aid(GetAID()), instanceID(oldest.first)](const KillResponse &rsp) {
        if (rsp.code() != common::ERR_NONE) {
            YRLOG_WARN("pressure FIFO unpark of instance({}) failed: code={} msg={}", instanceID,
                       static_cast<uint32_t>(rsp.code()), rsp.message());
            // W10-1: failure means the entry stayed parked; the full cooldown
            // would defer its retry by 300s. Shorten it (posted back to this
            // actor so wakeCooldown_ stays single-threaded).
            litebus::Async(aid, &PressureMonitorActor::OnWakeFailed, instanceID);
        } else {
            YRLOG_INFO("pressure FIFO unpark of instance({}) succeeded", instanceID);
        }
        return rsp;
    });
}

void PressureMonitorActor::OnWakeFailed(const std::string &instanceID)
{
    // W10-1: the entry stayed parked; retry soon instead of riding the full
    // kWakeCooldownCycles storm guard. SnapCtrl's own wakeFails give-up
    // (D-5 F4) still bounds the retry loop if the failure is persistent.
    auto it = wakeCooldown_.find(instanceID);
    if (it != wakeCooldown_.end()) {
        it->second = kWakeRetryCycles;
        YRLOG_INFO("pressure monitor on node {}: failed wake of instance({}) rescheduled in {} cycles",
                   nodeID_, instanceID, kWakeRetryCycles);
    }
}

}  // namespace functionsystem::local_scheduler
