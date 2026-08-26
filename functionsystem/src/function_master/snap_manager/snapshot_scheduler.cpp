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

#include "snapshot_scheduler.h"

#include "async/uuid_generator.hpp"
#include "common/logs/logging.h"
#include "common/proto/pb/posix/common.pb.h"
#include "common/types/instance_state.h"
#include "common/utils/resume_identity.h"
#include "common/utils/struct_transfer.h"

namespace functionsystem::snap_manager {

std::shared_ptr<messages::ScheduleRequest> SnapshotScheduler::BuildScheduleRequest(
    const SnapshotMetadata &meta,
    const messages::RestoreSnapshotRequest &restoreReq) const
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();

    const auto &snapshotID = meta.snapshotinfo().checkpointid();
    // W2 resume semantics: sandboxd state C/R restores the sandbox processes
    // verbatim, so the in-memory libruntime session reconnects under the
    // ORIGINAL instance identity. Re-admitting under a fresh ID makes the
    // proxy reject that reconnect ("instance may be deleted or invalid").
    // Keep the original instanceID when the snapshot metadata carries one;
    // fall back to a generated ID otherwise.
    const std::string &origID = meta.instanceinfo().instanceid();
    std::string newID = origID.empty() ? GenerateInstanceID(snapshotID) : origID;

    scheduleReq->set_requestid(newID);
    scheduleReq->set_traceid(newID);

    // Copy instance info from snapshot metadata
    scheduleReq->mutable_instance()->CopyFrom(meta.instanceinfo());

    // Reset fields that must be regenerated for the new instance
    scheduleReq->mutable_instance()->set_instanceid(newID);
    scheduleReq->mutable_instance()->set_requestid(newID);
    scheduleReq->mutable_instance()->set_functionproxyid("");
    scheduleReq->mutable_instance()->set_functionagentid("");
    scheduleReq->mutable_instance()->set_runtimeid("");
    scheduleReq->mutable_instance()->set_runtimeaddress("");
    // todo(lwy): parentID should be passed from restore request
    // scheduleReq->mutable_instance()->set_parentid("InstanceManagerOwner");
    scheduleReq->mutable_instance()->clear_args();
    scheduleReq->mutable_instance()->set_version(0);
    scheduleReq->mutable_instance()->mutable_snapshotinfo()->CopyFrom(meta.snapshotinfo());

    // Set instance state to NEW for restoration
    scheduleReq->mutable_instance()->mutable_instancestatus()->set_code(
        static_cast<int32_t>(InstanceState::NEW));

    core_service::CreateRequest schedulingRequest;
    schedulingRequest.mutable_schedulingops()->CopyFrom(
        restoreReq.snapstartoptions().scheduleopts());
    resume_identity::StripReservedExtensions(
        schedulingRequest.mutable_schedulingops()->mutable_extension());
    runtime::CallRequest emptyCallRequest;
    if (!restoreReq.snapstartoptions().scheduleopts().resources().empty()) {
        scheduleReq->mutable_instance()->clear_resources();
        SetInstanceInfoResources(scheduleReq->mutable_instance(), schedulingRequest);
    }
    SetInstanceInfoScheduleOptions(
        scheduleReq->mutable_instance(), schedulingRequest, emptyCallRequest);
    SetAffinityOpt(*scheduleReq->mutable_instance(), schedulingRequest, scheduleReq);

    YRLOG_DEBUG("built ScheduleRequest from snapshot {}: instanceID={}", snapshotID, newID);
    return scheduleReq;
}

std::shared_ptr<messages::ScheduleRequest> SnapshotScheduler::BuildPauseResumeScheduleRequest(
    const resources::InstanceInfo &authoritativeInstance,
    const messages::RestoreSnapshotRequest &restoreReq) const
{
    auto scheduleReq = std::make_shared<messages::ScheduleRequest>();
    const auto &targetAttemptID = restoreReq.requestid();
    scheduleReq->set_requestid(targetAttemptID);
    scheduleReq->set_traceid(targetAttemptID);
    scheduleReq->mutable_instance()->CopyFrom(authoritativeInstance);

    auto *instance = scheduleReq->mutable_instance();
    if (authoritativeInstance.instancestatus().code()
            == static_cast<int32_t>(InstanceState::RUNNING)
        && authoritativeInstance.version() > 0) {
        instance->set_version(authoritativeInstance.version() - 1);
        instance->mutable_instancestatus()->set_code(static_cast<int32_t>(InstanceState::PAUSED));
    }
    const auto sourceNodeID = instance->snapshotinfo().sourcenodeid().empty()
                                  ? instance->functionproxyid()
                                  : instance->snapshotinfo().sourcenodeid();

    instance->clear_functionproxyid();
    instance->clear_functionagentid();
    instance->clear_runtimeid();
    instance->clear_runtimeaddress();
    instance->clear_parentid();
    instance->clear_parentfunctionproxyaid();
    instance->clear_unitid();
    instance->clear_containerid();
    instance->clear_containerip();
    instance->clear_proxygrpcaddress();
    instance->clear_schedulerchain();
    resume_identity::StripReservedExtensions(instance->mutable_extensions());

    // Explicit resource overrides replace the previous allocation. With no overrides,
    // retain the authoritative allocation so SDK resume() remains schedulable.
    const bool hasResourceOverrides = !restoreReq.snapstartoptions().scheduleopts().resources().empty();
    if (hasResourceOverrides) {
        instance->clear_resources();
    }
    // ScheduleOption describes placement for the previous run and is always rebuilt.
    instance->clear_scheduleoption();

    core_service::CreateRequest schedulingRequest;
    schedulingRequest.mutable_schedulingops()->CopyFrom(restoreReq.snapstartoptions().scheduleopts());
    resume_identity::StripReservedExtensions(
        schedulingRequest.mutable_schedulingops()->mutable_extension());
    runtime::CallRequest emptyCallRequest;
    if (hasResourceOverrides) {
        SetInstanceInfoResources(instance, schedulingRequest);
    }
    SetInstanceInfoScheduleOptions(instance, schedulingRequest, emptyCallRequest);
    SetAffinityOpt(*instance, schedulingRequest, scheduleReq);

    if (!sourceNodeID.empty()) {
        // SnapshotInfo is the durable placement authority. A cluster may
        // change its global mode after this Snapshot was created.
        (*instance->mutable_scheduleoption()->mutable_affinity()->mutable_nodeaffinity()->mutable_affinity())
            [sourceNodeID] = instance->snapshotinfo().storage() == "local"
                ? resources::RequiredAffinity : resources::PreferredAffinity;
    }
    (*instance->mutable_scheduleoption()->mutable_extension())
        [resume_identity::MASTER_MARKER_EXTENSION] = targetAttemptID;

    YRLOG_DEBUG("built PAUSE_RESUME ScheduleRequest for logical instance {} with attempt {}",
                instance->instanceid(), targetAttemptID);
    return scheduleReq;
}

litebus::Future<Status> SnapshotScheduler::Schedule(
    const std::shared_ptr<messages::ScheduleRequest> &scheduleReq)
{
    if (!globalScheduler_) {
        YRLOG_ERROR("globalScheduler is null");
        return Status(StatusCode::FAILED, "globalScheduler is null");
    }
    return globalScheduler_->Schedule(scheduleReq);
}


std::string SnapshotScheduler::GenerateInstanceID(const std::string &snapshotID)
{
    constexpr size_t kSnapshotIDMaxLen = 50;
    constexpr size_t kSnapshotIDPrefixLen = 13;
    constexpr size_t kSnapshotIDSuffixLen = 8;
    constexpr size_t kUUIDSuffixLen = 8;
    std::string snapshotIDShort = snapshotID;
    if (snapshotID.length() > kSnapshotIDMaxLen) {
        snapshotIDShort = snapshotID.substr(0, kSnapshotIDPrefixLen) + "-" +
                          snapshotID.substr(snapshotID.length() - kSnapshotIDSuffixLen);
    }
    return snapshotIDShort + "-" +
           litebus::uuid_generator::UUID::GetRandomUUID().ToString().substr(0, kUUIDSuffixLen);
}

}  // namespace functionsystem::snap_manager
