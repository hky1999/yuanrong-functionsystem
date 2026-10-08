/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

// DS gate-3 B2b controlled-detention ORDER test (root 20:1xZ sequence).
// GATED by DS_GATE3_ORDER=1 (skips otherwise). One C++ process, three
// channels, all timestamps to a JSONL:
//   A. main client -> proxy(31592, hold armed later): init + ONE old Set
//      issued on a detached thread; the call is left to exceed its ~9s
//      deadline WITHOUT asserting or destructing the client (no success
//      ASSERT may cancel the detained request).
//   B. healthy channel -> worker directly (31594): writes the NEW value for
//      the SAME key, reads it back.
//   C. independent raw SDK verifier -> 31594: strict K_NOT_FOUND/Value reads
//      for the final adjudication AFTER the release file lets the old
//      request through.
// The TEST records observations; it never asserts the backend "must" win.
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "datasystem/kv_client.h"
#include "function_proxy/common/distribute_cache_client/ds_cache_client_impl.h"
#include "common/status/status.h"

namespace functionsystem::test {
namespace {
long NowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
std::string EnvOr(const char *name, const std::string &fallback)
{
    const char *value = std::getenv(name);
    return (value == nullptr) ? fallback : std::string(value);
}
}  // namespace

TEST(DsGate3HoldOrder, ControlledDetentionOrderObservation)
{
    if (std::getenv("DS_GATE3_ORDER") == nullptr) {
        GTEST_SKIP() << "DS_GATE3_ORDER not set — order probe skipped";
    }
    const std::string host = EnvOr("DS_GATE3_HOST", "127.0.0.1");
    const std::string routerEtcd = EnvOr("DS_GATE3_ROUTER", "127.0.0.1:31591");
    const std::string proxyPort = EnvOr("DS_GATE3_WORKER_PORT", "31592");
    const std::string directPort = EnvOr("DS_GATE3_DIRECT_PORT", "31594");
    const std::string armFile = EnvOr("DS_GATE3_ARM", "");
    const std::string releaseFile = EnvOr("DS_GATE3_RELEASE", "");
    const std::string outFile = EnvOr("DS_GATE3_OUT", "/tmp/order.jsonl");
    const std::string runId = EnvOr("DS_GATE3_RUNID", std::to_string(NowMs()));
    ASSERT_FALSE(armFile.empty()) << "DS_GATE3_ARM required (hold gate)";
    ASSERT_FALSE(releaseFile.empty()) << "DS_GATE3_RELEASE required";
    std::ofstream jsonl(outFile, std::ios::app);
    auto W = [&](const std::string &op, const std::string &extra = "") {
        jsonl << "{\"op\":\"" << op << "\",\"t_ms\":" << NowMs()
              << ",\"runid\":\"" << runId << "\"" << extra << "}\n";
        jsonl.flush();
    };

    // A. main client through the proxy.
    datasystem::ConnectOptions viaProxy{};
    viaProxy.host = host;
    viaProxy.port = std::stoi(proxyPort);
    functionsystem::RouterConnectOptions router{};
    router.azName = "ds-exp";
    router.etcdAddress = routerEtcd;
    auto main = std::make_unique<functionsystem::DSCacheClientImpl>(viaProxy, router);
    main->SetDSAuthEnable(false);
    main->EnableDSClient(true);
    const auto initStatus = main->Init();
    W("main_init", std::string(",\"ok\":") + (initStatus.IsOk() ? "true" : "false"));
    ASSERT_TRUE(initStatus.IsOk()) << "main init failed (environment, not a conclusion)";

    const std::string key = "gate3:hold:" + runId + ":k0";
    const std::string oldValue = "OLD-" + runId;
    const std::string newValue = "NEW-" + runId;

    // Pre-arm positive control on the SAME key through the proxy.
    const auto ctlSet = main->Set(key, "seed");
    W("seed_set", std::string(",\"ok\":") + (ctlSet.IsOk() ? "true" : "false"));
    ASSERT_TRUE(ctlSet.IsOk()) << "seed Set failed before arming";
    std::string seedBack;
    ASSERT_TRUE(main->Get(key, seedBack).IsOk() && seedBack == "seed");

    // ARM: from here the proxy detains every request packet.
    { std::ofstream a(armFile); a << NowMs(); }
    W("armed");

    // The ONE designated OLD Set — on its own thread; we do NOT wait on it
    // with an ASSERT. The client object stays alive for the whole test.
    std::atomic<bool> oldReturned{false};
    std::string oldCode = "<in-flight>";
    std::thread oldSet([&]() {
        const auto s = main->Set(key, oldValue);
        oldCode = std::to_string(static_cast<int>(s.StatusCode()));
        oldReturned.store(true);
        W("old_set_returned", ",\"code\":\"" + oldCode + "\"");
    });

    // B. healthy channel (direct to the worker, bypassing the proxy) while
    // the old Set is still detained (well past its 9s deadline).
    std::this_thread::sleep_for(std::chrono::seconds(12));
    W("old_deadline_elapsed", ",\"old_returned\":" +
        std::string(oldReturned.load() ? "true" : "false"));
    // (If it already returned the fault window collapsed — record and stop.)
    if (oldReturned.load()) {
        oldSet.join();
        W("aborted_window_collapsed");
        // release + final read anyway so the proxy never stays wedged
        { std::ofstream r(releaseFile); r << NowMs(); }
        std::this_thread::sleep_for(std::chrono::seconds(2));
        GTEST_SKIP() << "old Set returned before the new write — window "
                        "collapsed, evidence recorded";
    }

    datasystem::ConnectOptions direct{};
    direct.host = host;
    direct.port = std::stoi(directPort);
    functionsystem::RouterConnectOptions router2{};
    router2.azName = "ds-exp";
    router2.etcdAddress = routerEtcd;
    auto healthy = std::make_unique<functionsystem::DSCacheClientImpl>(direct, router2);
    healthy->SetDSAuthEnable(false);
    healthy->EnableDSClient(true);
    ASSERT_TRUE(healthy->Init().IsOk()) << "healthy-channel init failed";
    const auto newSet = healthy->Set(key, newValue);
    W("new_set_direct", std::string(",\"ok\":") + (newSet.IsOk() ? "true" : "false"));
    ASSERT_TRUE(newSet.IsOk()) << "new Set on the healthy channel failed";
    std::string newBack;
    ASSERT_TRUE(healthy->Get(key, newBack).IsOk());
    W("new_readback", ",\"value\":\"" + newBack + "\"");

    // C. release the detained OLD request, wait for it to land.
    { std::ofstream r(releaseFile); r << NowMs(); }
    W("released");
    for (int i = 0; i < 100 && !oldReturned.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    W("old_joined", ",\"old_returned\":" +
        std::string(oldReturned.load() ? "true" : "false"));
    if (oldSet.joinable()) { oldSet.join(); }

    // Final adjudication through the INDEPENDENT raw SDK verifier (direct).
    datasystem::KVClient raw(direct);
    ASSERT_TRUE(raw.Init().IsOk());
    std::string final;
    const auto fr = raw.Get(key, final);
    const auto frc = static_cast<int>(fr.GetCode());
    W("final_read", ",\"raw_rc\":" + std::to_string(frc)
        + ",\"value\":\"" + final + "\""
        + ",\"old_wins\":" + (final == oldValue ? "true" : "false")
        + ",\"new_wins\":" + (final == newValue ? "true" : "false"));
    std::cout << "DS_GATE3_ORDER final raw_rc=" << frc << " value=" << final
              << " old_wins=" << (final == oldValue)
              << " new_wins=" << (final == newValue) << std::endl;
    (void)raw.Del(key);
    SUCCEED();
}

}  // namespace functionsystem::test
