/*
 * Managed-checkpoint target contract tests (ResolveManagedCheckpointTarget,
 * runtime_manager/ckpt/checkpoint_plan.h).
 *
 * Pins the whole-contract fix for the first ov24c physical named-snapshot
 * case: the sandboxd executor must take the checkpoint under the
 * CALLER-PROVIDED identity and prepared staging directory (the Agent
 * prepares the dir, guards the returned id, commits/publishes under it);
 * legacy anonymous callers that pass neither keep the minted timestamped
 * id; HALF-passed pairs (either field alone) are rejected. Path-shape and
 * identity safety are enforced either way.
 *
 * Artifact ownership across source retirement (design note under test at
 * the service boundary, next round): the orchestrator AddRef/
 * SetCheckpointID(runtimeID->checkpointID) mapping makes StopInstance
 * ReleaseRef DECREF the file; RemoveReference only starts a TTL at zero
 * and the named chain carries ttl=0 (non-expiring), so retiring the
 * source does NOT delete the named artifact — the Agent's
 * local-snapshot store owns its lifecycle (Commit/Publish/Discard under
 * the same id). The service-boundary regression will assert: checkpoint
 * lands in the caller's staging dir under the caller's id; agent Commit
 * succeeds; source retirement leaves the artifact restorable.
 */
#include <gtest/gtest.h>

#include <string>

#include "runtime_manager/ckpt/checkpoint_plan.h"

namespace functionsystem::runtime_manager {
namespace {

TEST(ManagedCheckpointTarget, HonorsCallerIdentityAndStagingDirectoryTogether)
{
    messages::SnapshotRuntimeRequest request;
    request.set_snapshotid("snap-3ededf02406a7ee50af2e0deced555d5");
    request.set_checkpointdir("/home/akernel/sandboxd/root/checkpoints/snap-3ededf02406a7ee50af2e0deced555d5");
    ManagedCheckpointTarget target;
    ASSERT_FALSE(ResolveManagedCheckpointTarget(request, "/cfg/base", "inst-1", target).IsError());
    EXPECT_EQ(target.checkpointID, "snap-3ededf02406a7ee50af2e0deced555d5");
    EXPECT_EQ(target.checkpointPath,
              "/home/akernel/sandboxd/root/checkpoints/snap-3ededf02406a7ee50af2e0deced555d5");
}

TEST(ManagedCheckpointTarget, HalfPassedPairsAreRejected)
{
    // id without dir: the Agent contract resolves the directory BEFORE
    // execution (BuildCheckpointPlan); accepting id-only would recreate
    // the ov24c mismatch under a different shape
    messages::SnapshotRuntimeRequest request;
    request.set_snapshotid("ckpt-abc123");
    ManagedCheckpointTarget target;
    ASSERT_TRUE(ResolveManagedCheckpointTarget(request, "/cfg/base", "inst-1", target).IsError());
    // dir without id: symmetric rejection
    messages::SnapshotRuntimeRequest request2;
    request2.set_checkpointdir("/cfg/base/ckpt-abc123");
    ASSERT_TRUE(ResolveManagedCheckpointTarget(request2, "/cfg/base", "inst-1", target).IsError());
}

TEST(ManagedCheckpointTarget, LegacyAnonymousCallerMintsTimestampedId)
{
    messages::SnapshotRuntimeRequest request;  // no snapshotid, no checkpointdir
    ManagedCheckpointTarget target;
    ASSERT_FALSE(ResolveManagedCheckpointTarget(request, "/cfg/base", "inst-1", target).IsError());
    EXPECT_NE(target.checkpointID.find("ckpt-inst-1-"), std::string::npos);
    EXPECT_EQ(target.checkpointPath, "/cfg/base/" + target.checkpointID);
}

TEST(ManagedCheckpointTarget, UnsafeIdentityIsRejected)
{
    messages::SnapshotRuntimeRequest request;
    request.set_snapshotid("../escape");
    ManagedCheckpointTarget target;
    const auto status = ResolveManagedCheckpointTarget(request, "/cfg/base", "inst-1", target);
    ASSERT_TRUE(status.IsError());
}

TEST(ManagedCheckpointTarget, RelativeDirectoryIsRejected)
{
    messages::SnapshotRuntimeRequest request;
    request.set_snapshotid("snap-safe123");
    request.set_checkpointdir("relative/dir");
    ManagedCheckpointTarget target;
    const auto status = ResolveManagedCheckpointTarget(request, "/cfg/base", "inst-1", target);
    ASSERT_TRUE(status.IsError());
}

TEST(ManagedCheckpointTarget, EmptyIdentityComponentShapesRejected)
{
    messages::SnapshotRuntimeRequest request;
    request.set_snapshotid(".");
    ManagedCheckpointTarget target;
    ASSERT_TRUE(ResolveManagedCheckpointTarget(request, "/cfg", "i", target).IsError());
    request.set_snapshotid("..");
    ASSERT_TRUE(ResolveManagedCheckpointTarget(request, "/cfg", "i", target).IsError());
    request.set_snapshotid("has/slash");
    ASSERT_TRUE(ResolveManagedCheckpointTarget(request, "/cfg", "i", target).IsError());
}

}  // namespace
}  // namespace functionsystem::runtime_manager
