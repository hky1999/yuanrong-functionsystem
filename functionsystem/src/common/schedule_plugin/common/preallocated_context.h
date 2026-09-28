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

#ifndef DOMAIN_SCHEDULER_SCHEDULER_PREALLOCATED_CONTEXT_H
#define DOMAIN_SCHEDULER_SCHEDULER_PREALLOCATED_CONTEXT_H

#include <algorithm>
#include <chrono>
#include <functional>
#include <queue>
#include <set>
#include <unordered_map>

#include "common/resource_view/resource_type.h"
#include "common/resource_view/resource_tool.h"
#include "common/resource_view/scala_resource_tool.h"
#include "common/scheduler_framework/framework/framework.h"
#include "common/constants/constants.h"

namespace functionsystem::schedule_framework {

struct UnitResource {
    resource_view::Resources resource;
};

struct PodInfo {
    explicit PodInfo(int32_t monoNum = 0, int32_t sharedNum = 0) : monoNum(monoNum), sharedNum(sharedNum)
    {
    }
    explicit PodInfo(const resource_view::BucketInfo &bucketInfo)
        : monoNum(bucketInfo.monopolynum()), sharedNum(bucketInfo.sharednum())
    {
    }

    int32_t monoNum;
    int32_t sharedNum;
};

struct PodSpec {
    PodSpec(const std::string &proportion, const std::string &mem) : proportion(proportion), mem(mem)
    {
    }
    bool operator==(const PodSpec &podSpec) const
    {
        return proportion == podSpec.proportion && mem == podSpec.mem;
    }

    std::string proportion;
    std::string mem;
};

struct PodSpecScore {
    PodSpecScore(double capacityScore, double angleScore) : capacityScore(capacityScore), angleScore(angleScore)
    {
    }
    double capacityScore;
    double angleScore;
};

struct PodSpecHash {
    size_t operator()(const PodSpec &p) const
    {
        return std::hash<std::string>()(p.proportion + p.mem);
    }
};

struct NodeInfos {
    std::vector<std::pair<std::shared_ptr<PodSpec>, PodInfo>> podSpecWithInfo;
    std::map<int64_t, std::shared_ptr<PodSpec>> scoreWithPodSpec;
    std::shared_ptr<PodSpec> selectPodSpec;
    bool selectPodType{ false };  // false: mono, true: shared
};

struct PreAllocatedContext : public schedule_framework::ScheduleContext {
    struct RequestReservation {
        std::string unitID;
        std::string instanceID;
        resource_view::Resources resources;
        ::google::protobuf::Map<std::string, resource_view::ValueCounter> labels;
    };

    resource_view::SCHEDULER_LEVEL schedulerLevel;

    std::unordered_map<std::string, UnitResource> allocated;
    std::set<std::string> conflictNodes;

    // key: instanceID, value: PodSpec
    std::unordered_map<std::string, std::vector<std::shared_ptr<PodSpec>>> instanceFeasiblePodSpec;
    // key: instanceID, value: function_agent selected in PRE_ALLOCATION
    std::unordered_map<std::string, std::string> preAllocatedSelectedFunctionAgentMap;
    // Restore footprints differ from create-time bookings (e.g. 512 MiB -> 8 GiB).
    std::unordered_map<std::string, resource_view::Resource> preAllocatedRestoreMemory;

    void RememberRestoreMemory(const resource_view::InstanceInfo &instance)
    {
        // Detect by extension alone: pre-allocation runs on the new
        // ScheduleRequest instance where resource_admission_pending is not
        // yet set (it becomes true only during allocation).
        const bool namedRestore = instance.scheduleoption().extension().find(
            "yr.internal.reusable_snapshot.trusted_restore")
            != instance.scheduleoption().extension().end();
        if (instance.snapshotinfo().checkpointid().empty() && !namedRestore) {
            preAllocatedRestoreMemory.erase(instance.instanceid());
            return;
        }
        const auto memory = instance.resources().resources().find(resource_view::MEMORY_RESOURCE_NAME);
        preAllocatedRestoreMemory[instance.instanceid()] = memory == instance.resources().resources().end()
            ? resource_view::Resource{} : memory->second;
    }

    // key: function_agent selected in PRE_ALLOCATION
    std::set<std::string> preAllocatedSelectedFunctionAgentSet;
    // key: requestID, value: (key: childNodeId, value: NodeInfo)
    std::unordered_map<std::string, std::unordered_map<std::string, NodeInfos>> instanceFeasibleNodeWithInfo;

    // key: plugin name
    ::google::protobuf::Map<std::string, messages::PluginContext> *pluginCtx;

    // key: unitID value: allocated instance label
    std::unordered_map<std::string, ::google::protobuf::Map<std::string, resource_view::ValueCounter>> allocatedLabels;

    // Legacy PRE_ALLOCATION used to retain only the aggregate deduction. Keep
    // the exact per-request delta as well so a Local conflict can be rolled
    // back before the same request is re-enqueued.
    std::unordered_map<std::string, RequestReservation> requestReservations;

    // key: requestID value:(key: unitID value: default plugin score)
    std::unordered_map<std::string, std::unordered_map<std::string, int64_t>> requestDefaultScores;

    // key: localId value: all instance label
    std::unordered_map<std::string, ::google::protobuf::Map<std::string, resource_view::ValueCounter>> allLocalLabels;

    ::google::protobuf::Map<std::string, resource_view::ValueCounter>* allLabels;

    const resource_view::Resources &EffectiveAllocatable(const resource_view::ResourceUnit &unit)
    {
        if (unitEvaluationActive_ && unit.id() == evaluatedUnitID_) {
            return effectiveAllocatable_.at(unit.id()).resources;
        }
        RebuildEffectiveAllocatable(unit);
        return effectiveAllocatable_.at(unit.id()).resources;
    }

    void BeginUnitEvaluation(const resource_view::ResourceUnit &unit)
    {
        const auto cached = effectiveAllocatable_.find(unit.id());
        if (cached == effectiveAllocatable_.end() || cached->second.source != &unit) {
            RebuildEffectiveAllocatable(unit);
        }
        evaluatedUnitID_ = unit.id();
        unitEvaluationActive_ = true;
    }

    void EndUnitEvaluation()
    {
        unitEvaluationActive_ = false;
        evaluatedUnitID_.clear();
    }

    void InvalidateEffectiveAllocatable(const std::string &unitID)
    {
        effectiveAllocatable_.erase(unitID);
        if (unitID == evaluatedUnitID_) {
            EndUnitEvaluation();
        }
    }

    size_t EffectiveAllocatableBuildCount() const
    {
        return effectiveAllocatableBuildCount_;
    }

    void ClearEffectiveAllocatable()
    {
        EndUnitEvaluation();
        effectiveAllocatable_.clear();
    }

    void RecordRequestReservation(const std::string &requestID, const std::string &unitID,
                                  const resource_view::InstanceInfo &instance)
    {
        RequestReservation reservation;
        reservation.unitID = unitID;
        reservation.instanceID = instance.instanceid();
        reservation.resources = instance.resources();
        reservation.labels = instance.labels().empty() && instance.kvlabels().empty()
                                 ? ::google::protobuf::Map<std::string, resource_view::ValueCounter>{}
                                 : ToLabelKVs(instance.labels()) + ToLabelKVs(instance.kvlabels());
        requestReservations.insert_or_assign(requestID, std::move(reservation));
    }

    bool RemoveRequestReservation(const std::string &requestID)
    {
        const auto reservation = requestReservations.find(requestID);
        if (reservation == requestReservations.end()) {
            return false;
        }
        const auto unitID = reservation->second.unitID;
        if (auto allocatedIter = allocated.find(unitID); allocatedIter != allocated.end()) {
            allocatedIter->second.resource = allocatedIter->second.resource - reservation->second.resources;
        }
        if (!reservation->second.labels.empty()) {
            if (auto labelsIter = allocatedLabels.find(unitID); labelsIter != allocatedLabels.end()) {
                labelsIter->second = labelsIter->second - reservation->second.labels;
            }
        }
        preAllocatedSelectedFunctionAgentMap.erase(reservation->second.instanceID);
        requestReservations.erase(reservation);
        const auto stillReserved = std::any_of(
            requestReservations.begin(), requestReservations.end(), [&unitID](const auto &entry) {
                return entry.second.unitID == unitID;
            });
        if (!stillReserved) {
            preAllocatedSelectedFunctionAgentSet.erase(unitID);
        }
        InvalidateEffectiveAllocatable(unitID);
        return true;
    }

    PreAllocatedContext() = default;
    ~PreAllocatedContext() override = default;

private:
    struct EffectiveAllocatableEntry {
        const resource_view::ResourceUnit *source{ nullptr };
        resource_view::Resources resources;
    };

    void RebuildEffectiveAllocatable(const resource_view::ResourceUnit &unit)
    {
        auto effective = unit.allocatable();
        if (const auto reserved = allocated.find(unit.id()); reserved != allocated.end()) {
            effective = unit.allocatable() - reserved->second.resource;
        }
        effectiveAllocatable_.insert_or_assign(
            unit.id(), EffectiveAllocatableEntry{ &unit, std::move(effective) });
        ++effectiveAllocatableBuildCount_;
    }

    std::unordered_map<std::string, EffectiveAllocatableEntry> effectiveAllocatable_;
    std::string evaluatedUnitID_;
    bool unitEvaluationActive_{ false };
    size_t effectiveAllocatableBuildCount_{ 0 };
};


inline void ClearContext(::google::protobuf::Map<std::string, messages::PluginContext> &pluginCtx)
{
    pluginCtx[LABEL_AFFINITY_PLUGIN].mutable_affinityctx()->mutable_scheduledresult()->clear();
    pluginCtx[LABEL_AFFINITY_PLUGIN].mutable_affinityctx()->mutable_scheduledscore()->clear();
    pluginCtx[DEFAULT_FILTER_PLUGIN].mutable_defaultctx()->mutable_filterctx()->clear();
    pluginCtx[GROUP_SCHEDULE_CONTEXT].mutable_groupschedctx()->clear_reserved();
}

inline void CopyPluginContext(::google::protobuf::Map<std::string, messages::PluginContext> &out,
                              ::google::protobuf::Map<std::string, messages::PluginContext> &in)
{
    out[LABEL_AFFINITY_PLUGIN] = in[LABEL_AFFINITY_PLUGIN];
    out[DEFAULT_FILTER_PLUGIN] = in[DEFAULT_FILTER_PLUGIN];
}

}  // namespace functionsystem::schedule_framework

#endif  // DOMAIN_SCHEDULER_SCHEDULER_PREALLOCATED_CONTEXT_H
