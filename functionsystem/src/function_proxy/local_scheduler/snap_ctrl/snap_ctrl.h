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

#ifndef LOCAL_SCHEDULER_SNAP_CTRL_H
#define LOCAL_SCHEDULER_SNAP_CTRL_H

#include <async/future.hpp>
#include <memory>

#include "common/utils/actor_driver.h"
#include "common/status/status.h"
#include "snap_ctrl_actor.h"

namespace functionsystem::local_scheduler {

class FunctionAgentMgr;
class LocalSchedSrv;
class InstanceCtrl;
class IdleMgr;

/**
 * SnapCtrl is the interface class that wraps async calls to SnapCtrlActor.
 * It provides snapshot and snapstart functionality for instance management.
 */
class SnapCtrl : public ActorDriver {
public:
    explicit SnapCtrl(const std::shared_ptr<SnapCtrlActor> &snapCtrlActor);
    ~SnapCtrl() override;

    /**
     * Create a SnapCtrl instance with associated actor
     * @param nodeID: The node ID
     * @return Unique pointer to SnapCtrl
     */
    static std::unique_ptr<SnapCtrl> Create(const std::string &nodeID);

    void Stop() override;
    void Await() override;

    /**
     * Bind the FunctionAgentMgr for sending snapshot requests to agents
     * @param functionAgentMgr: The function agent manager interface
     */
    void BindFunctionAgentMgr(const std::shared_ptr<FunctionAgentMgr> &functionAgentMgr);

    /**
     * Bind the LocalSchedSrv for recording snapshot metadata
     * @param localSchedSrv: The local scheduler service interface
     */
    void BindLocalSchedSrv(const std::shared_ptr<LocalSchedSrv> &localSchedSrv);

    /**
     * Bind the InstanceControlView for accessing instance state machines
     * @param instanceControlView: The instance control view
     */
    void BindInstanceControlView(const std::shared_ptr<InstanceControlView> &instanceControlView);

    /**
     * Bind the InstanceCtrlActor for deleting instances
     * @param instanceCtrl: The instance control actor
     */
    void BindInstanceCtrl(const std::shared_ptr<InstanceCtrl> &instanceCtrl);

    /** Bind the IdleMgr (park hook, D-5 F1). */
    void BindIdleMgr(const std::shared_ptr<IdleMgr> &idleMgr);

    /**
     * Bind the ControlInterfaceClientManagerProxy for accessing instance clients
     * @param clientManager: The client manager
     */
    void BindClientManager(const std::shared_ptr<ControlInterfaceClientManagerProxy> &clientManager);

    /**
     * Handle INSTANCE_SNAPSHOT_SIGNAL
     * Wrap async call to SnapCtrlActor::HandleSnapshot
     * @param requestID: Request ID for tracing
     * @param instanceID: ID of the instance to snapshot
     * @param payload: JSON payload containing snapshot options
     * @return Future of KillResponse
     */
    virtual litebus::Future<KillResponse> HandleSnapshot(const std::string &requestID,
                                                         const std::string &instanceID,
                                                         const std::string &payload);

    /**
     * Handle INSTANCE_SNAPSTART_SIGNAL
     * Wrap async call to SnapCtrlActor::HandleSnapStart
     * @param requestID: Request ID for tracing
     * @param checkpointID: The checkpoint ID to restore from
     * @param payload: JSON payload containing SnapStartOptions
     * @return Future of KillResponse
     */
    virtual litebus::Future<KillResponse> HandleSnapStart(const std::string &requestID,
                                                          const std::string &checkpointID,
                                                          const std::string &payload);

    /**
     * Handle INSTANCE_WAKE_SNAPSHOT_SIGNAL
     * Wrap async call to SnapCtrlActor::HandleWake: restore a parked instance
     * looked up by its ORIGINAL instanceID.
     * @param requestID: Request ID for tracing
     * @param instanceID: ORIGINAL instanceID of the parked instance
     * @return Future of KillResponse
     */
    virtual litebus::Future<KillResponse> HandleWake(const std::string &requestID,
                                                     const std::string &instanceID);

    /**
     * Copy of the actor's parked registry, for the pressure monitor's FIFO
     * fallback unpark.
     */
    litebus::Future<std::vector<std::pair<std::string, SnapCtrlActor::ParkedEntry>>> GetParkedInstances();

    void SnapStart(
        const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> scheduleResp,
        const std::shared_ptr<messages::ScheduleRequest> &scheduleReq, const schedule_decision::ScheduleResult &result,
        const TransitionResult &transResult);

    /**
     * Get the AID of the underlying actor
     * @return Actor ID
     */
    litebus::AID GetAID() const
    {
        return aid_;
    }

private:
    std::shared_ptr<SnapCtrlActor> snapCtrlActor_;
    litebus::AID aid_;
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_SNAP_CTRL_H
