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

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include <openssl/sha.h>

#include "gtest/gtest.h"
#include "async/async.hpp"
#include "runtime_manager/ckpt/ckpt_file_manager_actor.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {

using functionsystem::runtime_manager::CkptFileManagerActor;

class CkptFileManagerActorTest : public ::testing::Test {
public:
    void SetUp() override
    {
        // Use a temp path unique per test run; actor's Init() will create it.
        tmpDir_ = std::string("/tmp/ckpt_test_") +
                  litebus::uuid_generator::UUID::GetRandomUUID().ToString();

        std::string name = "CkptFileMgr_" + litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        actor_ = std::make_shared<CkptFileManagerActor>(name, tmpDir_);

        // Disable the periodic cleanup timer so it does not interfere with tests.
        actor_->cleanupEnabled_ = false;

        litebus::Spawn(actor_);

        // Sync with the actor to guarantee Init() has finished before the test body runs.
        litebus::Async(actor_->GetAID(), &CkptFileManagerActor::CleanupExpiredFiles).Get();
    }

    void TearDown() override
    {
        if (actor_) {
            litebus::Terminate(actor_->GetAID());
            litebus::Await(actor_->GetAID());
        }
        actor_ = nullptr;
        std::filesystem::remove_all(tmpDir_);
    }

protected:
    std::string tmpDir_;
    std::shared_ptr<CkptFileManagerActor> actor_{ nullptr };
};

// Verify that Init() creates the configured checkpoint directory even when
// it does not exist prior to spawning the actor.
TEST_F(CkptFileManagerActorTest, CheckpointDirCreatedByInit)
{
    EXPECT_TRUE(std::filesystem::exists(tmpDir_));
    EXPECT_TRUE(std::filesystem::is_directory(tmpDir_));
}

// Verify that checkpoint entries already present on disk when the actor
// starts are loaded into memory and become addressable by their directory names.
TEST_F(CkptFileManagerActorTest, CheckpointRestoredFromPrePopulatedDir)
{
    const std::string ckptId = "ckpt_restore_test";
    std::filesystem::create_directories(tmpDir_ + "/" + ckptId);

    // Re-trigger the restore scan so it picks up the newly created directory.
    // RestoreCheckpointsFromLocal returns void; fire-and-forget then rely on FIFO
    // actor ordering: AddReference will only execute after the restore completes.
    litebus::Async(actor_->GetAID(), &CkptFileManagerActor::RestoreCheckpointsFromLocal);

    // AddReference succeeds only when the checkpoint is known to the actor.
    auto status = litebus::Async(actor_->GetAID(), &CkptFileManagerActor::AddReference, ckptId).Get();
    EXPECT_TRUE(status.IsOk());
}

// Verify that AddReference returns an error for an unknown checkpoint ID.
TEST_F(CkptFileManagerActorTest, AddReferenceFailsForUnknownCheckpoint)
{
    auto status = litebus::Async(
        actor_->GetAID(), &CkptFileManagerActor::AddReference, std::string("no_such_ckpt")).Get();
    EXPECT_FALSE(status.IsOk());
}

// Verify that RemoveReference is a no-op (returns OK) for an unknown checkpoint,
// which can happen when the checkpoint was already deleted.
TEST_F(CkptFileManagerActorTest, RemoveReferenceSucceedsForUnknownCheckpoint)
{
    auto status = litebus::Async(
        actor_->GetAID(), &CkptFileManagerActor::RemoveReference, std::string("no_such_ckpt")).Get();
    EXPECT_TRUE(status.IsOk());
}


// ── ComputeFileSha256 ─────────────────────────────────────────────────────────

namespace {
// One-shot reference digest to cross-check the chunked implementation.
std::string OneShotSha256(const std::string &data)
{
    unsigned char md[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char *>(data.data()), data.size(), md);
    static constexpr char hexDigits[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(SHA256_DIGEST_LENGTH * 2);
    for (unsigned char c : md) {
        hex.push_back(hexDigits[c >> 4]);
        hex.push_back(hexDigits[c & 0xf]);
    }
    return hex;
}

std::string WriteTempFile(const std::string &path, const std::string &content)
{
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.write(content.data(), static_cast<std::streamsize>(content.size()));
    return path;
}
}  // namespace

// Small file: single partial chunk, digest must match the well-known vector.
TEST(CkptFileManagerActorDigestTest, Sha256OfSmallFile)
{
    const std::string path = WriteTempFile("/tmp/ckpt_digest_small", "hello checkpoint digest\n");
    std::string sha256Hex;
    int64_t sizeBytes = -1;
    auto status = CkptFileManagerActor::ComputeFileSha256(path, sha256Hex, sizeBytes);
    EXPECT_TRUE(status.IsOk()) << status.RawMessage();
    EXPECT_EQ(sha256Hex, "8925d3758b19f2b208a719f56082ef3e5b5337c50d8f49a088d63ae9bb9552ae");
    EXPECT_EQ(sizeBytes, 24);
    std::filesystem::remove(path);
}

// File larger than one 4MiB chunk: exercises the chunked update path and must
// agree with a one-shot digest of the same content.
TEST(CkptFileManagerActorDigestTest, Sha256OfMultiChunkFile)
{
    const std::string pattern = "0123456789abcdef";
    std::string content;
    content.reserve(4 * 1024 * 1024 + 12345);
    while (content.size() < 4 * 1024 * 1024 + 12345) {
        content += pattern;
    }
    const std::string path = WriteTempFile("/tmp/ckpt_digest_large", content);
    std::string sha256Hex;
    int64_t sizeBytes = 0;
    auto status = CkptFileManagerActor::ComputeFileSha256(path, sha256Hex, sizeBytes);
    EXPECT_TRUE(status.IsOk()) << status.RawMessage();
    EXPECT_EQ(sha256Hex, OneShotSha256(content));
    EXPECT_EQ(sizeBytes, static_cast<int64_t>(content.size()));
    std::filesystem::remove(path);
}

TEST(CkptFileManagerActorDigestTest, Sha256OfMissingFileFails)
{
    std::string sha256Hex;
    int64_t sizeBytes = 0;
    auto status = CkptFileManagerActor::ComputeFileSha256("/tmp/no_such_digest_file", sha256Hex, sizeBytes);
    EXPECT_FALSE(status.IsOk());
    EXPECT_TRUE(sha256Hex.empty());
}

// ── RegisterCheckpoint / DownloadCheckpoint ───────────────────────────────────

// After registration the future must hand back checkpoint.img's sha256/size
// plus a createTime (epoch seconds) even when the KV client is not initialized
// (data_system_enable=false: local-only, no archive needed). The digest must
// match the sandboxd verification target: checkpoint_dir/checkpoint.img.
TEST_F(CkptFileManagerActorTest, RegisterCheckpointReturnsShaSizeAndCreateTime)
{
    const std::string ckptId = "ckpt_sha_register";
    const std::string ckptDir = tmpDir_ + "/" + ckptId;
    std::filesystem::create_directories(ckptDir);
    const std::string payload = "checkpoint payload bytes";
    WriteTempFile(ckptDir + "/checkpoint.img", payload);

    auto future = litebus::Async(actor_->GetAID(), &CkptFileManagerActor::RegisterCheckpoint,
                                 ckptId, ckptDir, ckptId, 60);
    const auto result = future.Get();
    EXPECT_EQ(result.storageUrl, ckptId + ".zip");
    EXPECT_EQ(result.sha256, OneShotSha256(payload));
    EXPECT_EQ(result.size, static_cast<int64_t>(payload.size()));
    ASSERT_FALSE(result.createTime.empty());
    EXPECT_GT(std::stoll(result.createTime), 0);

    // Registered: reference counting must address it now.
    auto status = litebus::Async(actor_->GetAID(), &CkptFileManagerActor::AddReference, ckptId).Get();
    EXPECT_TRUE(status.IsOk());
}

// A checkpoint directory without checkpoint.img cannot produce the digest that
// the identity-checked restore path requires — registration must fail loudly
// instead of handing back an empty-sha SnapshotInfo (which would poison every
// later wake with sandboxd's "expected_sha256 required" rejection).
TEST_F(CkptFileManagerActorTest, RegisterCheckpointFailsWithoutCheckpointImage)
{
    const std::string ckptId = "ckpt_no_image";
    const std::string ckptDir = tmpDir_ + "/" + ckptId;
    std::filesystem::create_directories(ckptDir);
    WriteTempFile(ckptDir + "/artifact.bin", "payload without image");

    auto future = litebus::Async(actor_->GetAID(), &CkptFileManagerActor::RegisterCheckpoint,
                                 ckptId, ckptDir, ckptId, 60);
    future.Wait();
    EXPECT_TRUE(future.IsError());
}

// Map entry with the local artifact on disk must short-circuit: no KV access
// happens (the KV client is not initialized here, so a download attempt would
// fail), and the registered local path is returned.
TEST_F(CkptFileManagerActorTest, DownloadCheckpointShortCircuitsWhenFileExists)
{
    const std::string ckptId = "ckpt_local_hit";
    const std::string ckptDir = tmpDir_ + "/" + ckptId;
    std::filesystem::create_directories(ckptDir);
    WriteTempFile(ckptDir + "/artifact.bin", "payload");

    ASSERT_TRUE(litebus::Async(actor_->GetAID(), &CkptFileManagerActor::AddReference, ckptId)
                    .Get()
                    .IsError());  // not registered yet

    litebus::Async(actor_->GetAID(), &CkptFileManagerActor::RestoreCheckpointsFromLocal);
    ASSERT_TRUE(litebus::Async(actor_->GetAID(), &CkptFileManagerActor::AddReference, ckptId).Get().IsOk());

    auto future = litebus::Async(actor_->GetAID(), &CkptFileManagerActor::DownloadCheckpoint,
                                 ckptId, ckptId + ".zip", "", 0);
    EXPECT_EQ(future.Get(), ckptDir);
}

// Map entry whose local artifact is gone must NOT short-circuit: it falls
// through to the KV download path, which fails here (client not initialized)
// instead of handing out a stale path.
TEST_F(CkptFileManagerActorTest, DownloadCheckpointRedownloadsWhenFileGone)
{
    const std::string ckptId = "ckpt_stale_entry";
    const std::string ckptDir = tmpDir_ + "/" + ckptId;
    std::filesystem::create_directories(ckptDir);

    litebus::Async(actor_->GetAID(), &CkptFileManagerActor::RestoreCheckpointsFromLocal);
    ASSERT_TRUE(litebus::Async(actor_->GetAID(), &CkptFileManagerActor::AddReference, ckptId).Get().IsOk());

    std::filesystem::remove_all(ckptDir);
    auto future = litebus::Async(actor_->GetAID(), &CkptFileManagerActor::DownloadCheckpoint,
                                 ckptId, ckptId + ".zip", "", 0);
    // 失败经 OnDownloadFailed 异步回投 actor 后才落进 future：先阻塞等
    // 完成（Get() 在 error 态会提前返回不阻塞，必须用 Wait）。
    future.Wait();
    EXPECT_TRUE(future.IsError());
}

}  // namespace functionsystem::test
