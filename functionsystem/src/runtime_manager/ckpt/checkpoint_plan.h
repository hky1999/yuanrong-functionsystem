/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#ifndef FUNCTIONSYSTEM_SRC_RUNTIME_MANAGER_CKPT_CHECKPOINT_PLAN_H
#define FUNCTIONSYSTEM_SRC_RUNTIME_MANAGER_CKPT_CHECKPOINT_PLAN_H

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <limits>
#include <string>

#include "common/proto/pb/message_pb.h"
#include "common/status/status.h"

namespace functionsystem::runtime_manager {

struct CheckpointPlan {
    std::string sandboxID;
    std::string checkpointID;
    std::string checkpointDirectory;
    int32_t ttlSeconds{ 0 };
    uint32_t timeoutSeconds{ 180 };
    // Runtime checkpointing stays on the latency-sensitive pause path. Any
    // transport compression is performed later by FunctionAgent.
    bool compress{ false };
    bool leaveRuntimeRunning{ false };
};

struct CheckpointResult {
    Status status;
};

inline bool IsSafeCheckpointIdentityComponent(const std::string &component)
{
    return !component.empty() && component != "." && component != ".."
        && component.find('/') == std::string::npos
        && component.find('\\') == std::string::npos
        && component.find('\0') == std::string::npos;
}

inline Status BuildCheckpointPlan(const messages::SnapshotRuntimeRequest &request,
                                  const std::string &sandboxID,
                                  bool leaveRuntimeRunning,
                                  CheckpointPlan &plan)
{
    if (sandboxID.empty()) {
        return Status(StatusCode::PARAMETER_ERROR, "checkpoint sandbox identity is empty");
    }
    if (!IsSafeCheckpointIdentityComponent(request.snapshotid())) {
        return Status(StatusCode::PARAMETER_ERROR, "checkpoint identity is invalid");
    }
    if (request.checkpointdir().empty()) {
        return Status(StatusCode::PARAMETER_ERROR, "checkpoint directory must be resolved before execution");
    }
    const auto directory = std::filesystem::path(request.checkpointdir()).lexically_normal();
    if (!directory.is_absolute()) {
        return Status(StatusCode::PARAMETER_ERROR, "checkpoint directory must be absolute");
    }

    plan.sandboxID = sandboxID;
    plan.checkpointID = request.snapshotid();
    plan.checkpointDirectory = directory.string();
    plan.ttlSeconds = request.ttl();
    plan.leaveRuntimeRunning = leaveRuntimeRunning;
    if (request.timeoutms() > 0) {
        constexpr uint64_t MILLISECONDS_PER_SECOND = 1000;
        const auto seconds = request.timeoutms() / MILLISECONDS_PER_SECOND
            + (request.timeoutms() % MILLISECONDS_PER_SECOND == 0 ? 0 : 1);
        plan.timeoutSeconds = static_cast<uint32_t>(std::min<uint64_t>(
            seconds, std::numeric_limits<uint32_t>::max()));
    }
    return Status::OK();
}

}  // namespace functionsystem::runtime_manager

#endif  // FUNCTIONSYSTEM_SRC_RUNTIME_MANAGER_CKPT_CHECKPOINT_PLAN_H

// Managed-checkpoint target for the sandboxd executor: when the caller
// (FunctionAgent, on behalf of the named reusable-snapshot chain or the
// replay-aware anonymous path) provides a snapshot identity and a
// prepared staging directory, the checkpoint MUST be taken under THAT
// identity and path — the Agent's local-snapshot store prepared the
// staging directory, validates the returned identity against its request
// (agent_service_snapshot.cpp checkpointid==snapshotid guard), and
// commits/publishes under the same id. Ignoring them strands a
// physically-successful checkpoint under an unreachable identity (first
// ov24c physical case: code-0 checkpoint, then agent guard
// ERR_PARAM_INVALID). Legacy callers that pass neither keep the minted
// timestamped id under the configured base directory.
struct ManagedCheckpointTarget {
    std::string checkpointID;
    std::string checkpointPath;
};

inline Status ResolveManagedCheckpointTarget(const messages::SnapshotRuntimeRequest &request,
                                             const std::string &configuredBaseDir,
                                             const std::string &instanceID,
                                             ManagedCheckpointTarget &target)
{
    std::string id = request.snapshotid();
    if (id.empty()) {
        id = "ckpt-" + instanceID + "-"
            + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    }
    if (!IsSafeCheckpointIdentityComponent(id)) {
        return Status(StatusCode::PARAMETER_ERROR, "checkpoint identity is invalid");
    }
    std::string path = request.checkpointdir();
    if (path.empty()) {
        path = configuredBaseDir + "/" + id;
    }
    const auto normalized = std::filesystem::path(path).lexically_normal();
    if (!normalized.is_absolute()) {
        return Status(StatusCode::PARAMETER_ERROR, "checkpoint directory must be absolute");
    }
    target.checkpointID = id;
    target.checkpointPath = normalized.string();
    return Status::OK();
}
