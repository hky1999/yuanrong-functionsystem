/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef FUNCTIONSYSTEM_FUNCTION_AGENT_SNAPSHOT_LOCAL_SNAPSHOT_STORE_H
#define FUNCTIONSYSTEM_FUNCTION_AGENT_SNAPSHOT_LOCAL_SNAPSHOT_STORE_H

#include <cstdint>
#include <filesystem>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status/status.h"

namespace functionsystem::function_agent {

// Process-local artifact index. The committed checkpoint directory is an
// opaque sandboxd artifact; FunctionAgent never assumes file names or count.
struct LocalSnapshotDescriptor {
    std::string snapshotID;
    bool recoveryCandidate{ false };
    std::string instanceID;
    std::string tenantHash;
    std::string sourceRuntimeID;
    std::string sourceSandboxID;
    int64_t sourceInstanceVersion{ 0 };
    uint64_t size{ 0 };  // observability/cache accounting only
    int64_t createdAtUnixSeconds{ 0 };
    std::string storageBackend;
    std::string objectKey;
};

struct LocalSnapshotCommitRequest {
    std::string snapshotID;
    bool recoveryCandidate{ false };
    std::string instanceID;
    std::string tenantHash;
    std::string sourceRuntimeID;
    std::string sourceSandboxID;
    int64_t sourceInstanceVersion{ 0 };
    int64_t createdAtUnixSeconds{ 0 };
};

struct LocalSnapshotPrepareResult {
    Status status;
    std::filesystem::path directory;
    bool replayed{ false };
};

struct LocalSnapshotCommitResult {
    Status status;
    LocalSnapshotDescriptor descriptor;
};

struct LocalSnapshotDeleteIdentity {
    std::string snapshotID;
};

class LocalSnapshotStore {
public:
    explicit LocalSnapshotStore(std::filesystem::path checkpointRoot, uint64_t maxCacheBytes = 0);

    // Leaf policy of Prepare: the final artifact directory is created only
    // when the CALLER materializes the artifact into it (remote download).
    // A local PHYSICAL checkpoint must leave the leaf ABSENT — the daemon
    // (sandboxd checkpoint store PublishAt) creates the final directory
    // atomically from a staging sibling and REJECTS a pre-existing
    // incomplete final directory with failed-precondition. Pre-creating the
    // leaf here is exactly the state the daemon must refuse.
    enum class PrepareLeaf {
        CreateForMaterialization,
        LeaveAbsentForDaemon,
    };

    LocalSnapshotPrepareResult Prepare(const LocalSnapshotCommitRequest &request,
        PrepareLeaf leaf = PrepareLeaf::CreateForMaterialization);
    LocalSnapshotCommitResult Commit(const LocalSnapshotCommitRequest &request);
    std::vector<LocalSnapshotDescriptor> List() const;
    Status ValidateForRestore(const std::string &snapshotID, LocalSnapshotDescriptor &descriptor);
    Status SetStorageLocation(const std::string &snapshotID, const std::string &storageBackend,
                              const std::string &objectKey);
    Status PinForRestore(const std::string &snapshotID);
    Status UnpinAfterRestore(const std::string &snapshotID, bool evictAfterRelease);
    Status EvictLocalArtifact(const std::string &snapshotID);
    Status DiscardPrepared(const std::string &snapshotID);
    Status DeleteRecoveryCandidatesForInstance(const std::string &instanceID);
    Status Delete(const LocalSnapshotDeleteIdentity &identity);

private:
    Status ValidateCommitRequest(const LocalSnapshotCommitRequest &request) const;
    std::filesystem::path SnapshotDirectory(const std::string &snapshotID) const;
    Status InspectArtifact(const std::filesystem::path &directory, uint64_t &size) const;
    void Touch(const std::string &snapshotID);
    void EvictIfNeeded(const std::string &protectedSnapshotID);
    Status DeleteDirectoryUnlocked(const std::filesystem::path &directory);
    Status DeleteUnlocked(const std::string &snapshotID);

    std::filesystem::path checkpointRoot_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, LocalSnapshotDescriptor> records_;
    uint64_t maxCacheBytes_{ 0 };
    uint64_t cachedBytes_{ 0 };
    std::list<std::string> lru_;
    std::unordered_map<std::string, std::list<std::string>::iterator> lruIndex_;
    std::unordered_map<std::string, uint64_t> restorePins_;
    std::unordered_set<std::string> evictAfterUnpin_;
};

}  // namespace functionsystem::function_agent

#endif  // FUNCTIONSYSTEM_FUNCTION_AGENT_SNAPSHOT_LOCAL_SNAPSHOT_STORE_H
