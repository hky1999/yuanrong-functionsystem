/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "runtime_manager/executor/sandboxd/sandbox_memory_event_watcher.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace functionsystem::test {
using runtime_manager::SandboxMemoryEventWatcher;
using Identity = SandboxMemoryEventWatcher::Identity;

namespace {

// Collects callbacks on the watcher thread; the first throwFirstN calls raise
// instead, to prove a throwing callback cannot kill the watcher thread.
class Recorder {
public:
    void OnEvent(const Identity &identity)
    {
        if (throwFirstN.fetch_sub(1) > 0) {
            throw std::runtime_error("intentional callback failure");
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            hits_.push_back(identity);
        }
        cv_.notify_all();
    }

    bool WaitFor(std::size_t count, int timeoutMs)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                            [this, count] { return hits_.size() >= count; });
    }

    std::vector<Identity> Hits()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return hits_;
    }

    std::atomic<int> throwFirstN{ 0 };
private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<Identity> hits_;
};

// Fake v2 layout under a temp root: <root>/<name>/memory.events as a regular
// file (inotify on tmpfs/disk files delivers IN_MODIFY the same way cgroupfs
// does; only the watcher's path validation cares where the root sits).
class TempCgroupRoot {
public:
    TempCgroupRoot()
    {
        std::string pattern = "/tmp/sandbox_memory_event_watcher_test_XXXXXX";
        std::vector<char> buf(pattern.begin(), pattern.end());
        buf.push_back('\0');
        const char *dir = ::mkdtemp(buf.data());
        if (dir == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        root_ = dir;
    }

    ~TempCgroupRoot()
    {
        for (const std::string &name : cgroups_) {
            (void)::rmdir(EventsPath(name).c_str());  // memory.events created as a dir
            (void)::unlink(EventsPath(name).c_str());
            (void)::rmdir((root_ + "/" + name).c_str());
        }
        for (const std::string &path : extras_) {
            (void)::unlink(path.c_str());
            (void)::rmdir(path.c_str());
        }
        (void)::rmdir(root_.c_str());
    }

    const std::string &Root() const
    {
        return root_;
    }

    std::string MakeCgroup(const std::string &name)
    {
        const std::string dir = root_ + "/" + name;
        if (::mkdir(dir.c_str(), 0755) != 0) {
            throw std::runtime_error("mkdir cgroup failed");
        }
        cgroups_.push_back(name);
        WriteInitial(name);
        return dir;
    }

    // A dir inside the root without (or with a non-regular) memory.events.
    void MakeBareDir(const std::string &name)
    {
        const std::string dir = root_ + "/" + name;
        if (::mkdir(dir.c_str(), 0755) != 0) {
            throw std::runtime_error("mkdir bare dir failed");
        }
        cgroups_.push_back(name);
    }

    void MakeEventsDir(const std::string &name)
    {
        MakeBareDir(name);
        if (::mkdir(EventsPath(name).c_str(), 0755) != 0) {
            throw std::runtime_error("mkdir memory.events dir failed");
        }
    }

    void MakeSymlink(const std::string &name, const std::string &target)
    {
        const std::string link = root_ + "/" + name;
        if (::symlink(target.c_str(), link.c_str()) != 0) {
            throw std::runtime_error("symlink failed");
        }
        extras_.push_back(link);
    }

    std::string EventsPath(const std::string &name) const
    {
        return root_ + "/" + name + "/memory.events";
    }

    // The stand-in for "the kernel bumped a counter in memory.events".
    void AppendEvent(const std::string &name, const std::string &text)
    {
        const int fd = ::open(EventsPath(name).c_str(), O_WRONLY | O_APPEND);
        if (fd < 0) {
            throw std::runtime_error("open memory.events failed");
        }
        const ssize_t written = ::write(fd, text.data(), text.size());
        (void)written;
        (void)::close(fd);
    }

private:
    void WriteInitial(const std::string &name)
    {
        const int fd = ::open(EventsPath(name).c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            throw std::runtime_error("create memory.events failed");
        }
        const ssize_t written = ::write(fd, "low 0\nhigh 0\n", 13);
        (void)written;
        (void)::close(fd);
    }

    std::string root_;
    std::vector<std::string> cgroups_;
    std::vector<std::string> extras_;
};

Identity MakeIdentity(const std::string &runtimeID, uint64_t generation, const std::string &cgroupPath)
{
    Identity identity;
    identity.runtimeID = runtimeID;
    identity.sandboxID = "sandbox-" + runtimeID;
    identity.resourceGeneration = "generation-" + std::to_string(generation);
    identity.cgroupPath = cgroupPath;
    return identity;
}

}  // namespace

TEST(SandboxMemoryEventWatcherTest, ModifyNotifiesRegisteredIdentity)
{
    TempCgroupRoot root;
    root.MakeCgroup("cg1");
    Recorder recorder;
    SandboxMemoryEventWatcher watcher([&recorder](const Identity &identity) { recorder.OnEvent(identity); },
                                      root.Root());
    ASSERT_TRUE(watcher.Start());
    ASSERT_TRUE(watcher.Watch(MakeIdentity("rt1", 7, root.Root() + "/cg1")));

    root.AppendEvent("cg1", "high 1\n");

    ASSERT_TRUE(recorder.WaitFor(1, 5000));
    const auto hits = recorder.Hits();
    ASSERT_EQ(hits.size(), 1u);
    EXPECT_EQ(hits[0].runtimeID, "rt1");
    EXPECT_EQ(hits[0].sandboxID, "sandbox-rt1");
    EXPECT_EQ(hits[0].resourceGeneration, "generation-7");
    EXPECT_EQ(hits[0].cgroupPath, root.Root() + "/cg1");
}

TEST(SandboxMemoryEventWatcherTest, WatchBeforeStartIsRejected)
{
    TempCgroupRoot root;
    root.MakeCgroup("cg1");
    Recorder recorder;
    SandboxMemoryEventWatcher watcher([&recorder](const Identity &identity) { recorder.OnEvent(identity); },
                                      root.Root());
    EXPECT_FALSE(watcher.Watch(MakeIdentity("rt1", 1, root.Root() + "/cg1")));
    EXPECT_GE(watcher.Stats().rejectedWatches, 1u);
}

TEST(SandboxMemoryEventWatcherTest, InvalidPathsAreRejected)
{
    TempCgroupRoot root;
    root.MakeCgroup("cg1");
    root.MakeBareDir("noevents");
    root.MakeEventsDir("weird");
    root.MakeSymlink("sneaky", "/tmp");
    root.MakeSymlink("alias", root.Root() + "/cg1");

    Recorder recorder;
    SandboxMemoryEventWatcher watcher([&recorder](const Identity &identity) { recorder.OnEvent(identity); },
                                      root.Root());
    ASSERT_TRUE(watcher.Start());

    const std::string badPaths[] = {
        "cg1",                                    // relative
        root.Root() + "/cg1/..",                  // parent hop
        root.Root() + "/nope/../cg1",             // ".." rejected even though it resolves inside
        "/tmp",                                   // exists but outside the root
        root.Root() + "/noevents/../../tmp",      // dot-dot chain out of the root
        root.Root() + "/missing",                 // no such cgroup
        root.Root() + "/noevents",                // no memory.events at all
        root.Root() + "/weird",                   // memory.events is a directory
        root.Root() + "/sneaky",                  // symlink resolving outside the root
    };
    for (const std::string &path : badPaths) {
        EXPECT_FALSE(watcher.Watch(MakeIdentity("rt1", 1, path))) << "path: " << path;
    }
    // A symlink that stays under the root is just a path, not an escape.
    EXPECT_TRUE(watcher.Watch(MakeIdentity("rt1", 1, root.Root() + "/alias")));
    EXPECT_GE(watcher.Stats().rejectedWatches, 9u);
}

TEST(SandboxMemoryEventWatcherTest, UnwatchedCgroupStaysSilent)
{
    TempCgroupRoot root;
    root.MakeCgroup("cg1");
    Recorder recorder;
    SandboxMemoryEventWatcher watcher([&recorder](const Identity &identity) { recorder.OnEvent(identity); },
                                      root.Root());
    ASSERT_TRUE(watcher.Start());
    ASSERT_TRUE(watcher.Watch(MakeIdentity("rt1", 1, root.Root() + "/cg1")));
    watcher.Unwatch("rt1");

    // The kernel watch is gone before the write, so no event can exist.
    root.AppendEvent("cg1", "high 5\n");

    const bool silent = recorder.WaitFor(1, 300);
    EXPECT_FALSE(silent);
    EXPECT_TRUE(recorder.Hits().empty());
    EXPECT_EQ(watcher.Stats().staleEventsDropped, 0u);
}

TEST(SandboxMemoryEventWatcherTest, ReplacedRegistrationKeepsOnlyNewIdentity)
{
    TempCgroupRoot root;
    root.MakeCgroup("cg1");
    Recorder recorder;
    SandboxMemoryEventWatcher watcher([&recorder](const Identity &identity) { recorder.OnEvent(identity); },
                                      root.Root());
    ASSERT_TRUE(watcher.Start());
    ASSERT_TRUE(watcher.Watch(MakeIdentity("rt1", 1, root.Root() + "/cg1")));
    // Same runtime, new resource generation: the old physical identity must die.
    ASSERT_TRUE(watcher.Watch(MakeIdentity("rt1", 2, root.Root() + "/cg1")));

    root.AppendEvent("cg1", "high 9\n");

    ASSERT_TRUE(recorder.WaitFor(1, 5000));
    const auto hits = recorder.Hits();
    for (const Identity &hit : hits) {
        EXPECT_EQ(hit.runtimeID, "rt1");
        EXPECT_EQ(hit.resourceGeneration, "generation-2");
    }
    EXPECT_EQ(watcher.Stats().staleEventsDropped, 0u);
}

TEST(SandboxMemoryEventWatcherTest, RemovedThenReRegisteredUsesFreshWatch)
{
    TempCgroupRoot root;
    root.MakeCgroup("cg1");
    Recorder recorder;
    SandboxMemoryEventWatcher watcher([&recorder](const Identity &identity) { recorder.OnEvent(identity); },
                                      root.Root());
    ASSERT_TRUE(watcher.Start());
    ASSERT_TRUE(watcher.Watch(MakeIdentity("rt1", 1, root.Root() + "/cg1")));
    watcher.Unwatch("rt1");
    ASSERT_TRUE(watcher.Watch(MakeIdentity("rt1", 2, root.Root() + "/cg1")));

    root.AppendEvent("cg1", "high 3\n");

    ASSERT_TRUE(recorder.WaitFor(1, 5000));
    const auto hits = recorder.Hits();
    ASSERT_GE(hits.size(), 1u);
    for (const Identity &hit : hits) {
        EXPECT_EQ(hit.resourceGeneration, "generation-2");
    }
}

TEST(SandboxMemoryEventWatcherTest, StopIsPromptAndSilent)
{
    TempCgroupRoot root;
    root.MakeCgroup("cg1");
    Recorder recorder;
    SandboxMemoryEventWatcher watcher([&recorder](const Identity &identity) { recorder.OnEvent(identity); },
                                      root.Root());
    ASSERT_TRUE(watcher.Start());
    ASSERT_TRUE(watcher.Watch(MakeIdentity("rt1", 1, root.Root() + "/cg1")));
    root.AppendEvent("cg1", "high 1\n");
    ASSERT_TRUE(recorder.WaitFor(1, 5000));
    const std::size_t hitsBefore = recorder.Hits().size();

    watcher.Stop();  // must return promptly (eventfd wake + join), never hang

    root.AppendEvent("cg1", "high 2\n");
    EXPECT_FALSE(recorder.WaitFor(hitsBefore + 1, 300));
    EXPECT_EQ(recorder.Hits().size(), hitsBefore);
    watcher.Stop();  // idempotent
}

TEST(SandboxMemoryEventWatcherTest, RestartWatchesAgain)
{
    TempCgroupRoot root;
    root.MakeCgroup("cg1");
    Recorder recorder;
    SandboxMemoryEventWatcher watcher([&recorder](const Identity &identity) { recorder.OnEvent(identity); },
                                      root.Root());
    ASSERT_TRUE(watcher.Start());
    watcher.Stop();
    ASSERT_TRUE(watcher.Start());
    ASSERT_TRUE(watcher.Watch(MakeIdentity("rt1", 1, root.Root() + "/cg1")));

    root.AppendEvent("cg1", "high 1\n");

    ASSERT_TRUE(recorder.WaitFor(1, 5000));
}

TEST(SandboxMemoryEventWatcherTest, CallbackExceptionDoesNotKillWatcher)
{
    TempCgroupRoot root;
    root.MakeCgroup("cg1");
    Recorder recorder;
    recorder.throwFirstN = 1;  // first hint raises inside the watcher thread
    SandboxMemoryEventWatcher watcher([&recorder](const Identity &identity) { recorder.OnEvent(identity); },
                                      root.Root());
    ASSERT_TRUE(watcher.Start());
    ASSERT_TRUE(watcher.Watch(MakeIdentity("rt1", 1, root.Root() + "/cg1")));

    root.AppendEvent("cg1", "high 1\n");
    // inotify coalesces identical consecutive events: wait for the first
    // (throwing) delivery before writing the second, otherwise a fast host
    // collapses both writes into one notification and the resilience this
    // test proves never gets exercised.
    for (int i = 0; i < 500 && watcher.Stats().callbackErrors < 1; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(watcher.Stats().callbackErrors, 1u);
    root.AppendEvent("cg1", "high 2\n");

    ASSERT_TRUE(recorder.WaitFor(1, 5000));
    EXPECT_EQ(watcher.Stats().callbackErrors, 1u);
    EXPECT_EQ(watcher.Stats().threadExited, 0u);
}

TEST(SandboxMemoryEventWatcherTest, RegistrationIsBounded)
{
    TempCgroupRoot root;
    Recorder recorder;
    SandboxMemoryEventWatcher watcher([&recorder](const Identity &identity) { recorder.OnEvent(identity); },
                                      root.Root());
    ASSERT_TRUE(watcher.Start());

    for (std::size_t i = 0; i < SandboxMemoryEventWatcher::kMaxWatches; ++i) {
        const std::string name = "cg" + std::to_string(i);
        const std::string dir = root.MakeCgroup(name);
        if (!watcher.Watch(MakeIdentity("rt" + std::to_string(i), 1, dir))) {
            ADD_FAILURE() << "Watch rejected inside the cap at " << name;
            break;
        }
    }

    const std::string extraDir = root.MakeCgroup("overflow");
    EXPECT_FALSE(watcher.Watch(MakeIdentity("rt-overflow", 1, extraDir)));
    EXPECT_GE(watcher.Stats().rejectedWatches, 1u);

    // Freeing one slot (and using a fresh path) makes room again.
    watcher.Unwatch("rt0");
    EXPECT_TRUE(watcher.Watch(MakeIdentity("rt-overflow", 1, extraDir)));
}

}  // namespace functionsystem::test
