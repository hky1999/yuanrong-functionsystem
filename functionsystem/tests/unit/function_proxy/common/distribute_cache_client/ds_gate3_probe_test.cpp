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

// DataSystem gate-3 Phase A probe (root 2026-10-08 experiment plan):
// drives the PRODUCTION DSCacheClientImpl against the ISOLATED instance
// (coordinator 127.0.0.1:31591 / worker 127.0.0.1:31592 started on the
// host, unique key prefix). GATED: the test SKIPS unless DS_GATE3=1 is set
// in the environment — normal component rounds never touch the network.
// Phase A = positive control ONLY: Init + Set/Get/Del ×N with per-op
// timings to stdout (harvested to phaseA.jsonl). Any failure here is an
// environment problem, never a deadline conclusion — Phase B (bounded
// delay / dropped responses at the OPERATION stage) is where deadline and
// late-commit verdicts come from.

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

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

TEST(DsGate3Probe, PhaseARealSetGetDelPositiveControl)
{
    if (std::getenv("DS_GATE3") == nullptr) {
        GTEST_SKIP() << "DS_GATE3 not set — isolated-instance probe skipped";
    }
    const std::string host = EnvOr("DS_GATE3_HOST", "127.0.0.1");
    const std::string routerEtcd = EnvOr("DS_GATE3_ROUTER", "127.0.0.1:31591");
    const std::string workerPort = EnvOr("DS_GATE3_WORKER_PORT", "31592");
    const std::string runId = EnvOr("DS_GATE3_RUNID", std::to_string(NowMs()));
    const std::string outFile = EnvOr("DS_GATE3_OUT", "/tmp/phaseA.jsonl");

    datasystem::ConnectOptions connect{};
    connect.host = host;
    connect.port = std::stoi(workerPort);
    functionsystem::RouterConnectOptions router{};
    router.azName = "ds-exp";
    router.etcdAddress = routerEtcd;

    std::ofstream jsonl(outFile, std::ios::app);
    auto client = std::make_unique<functionsystem::DSCacheClientImpl>(connect, router);
    client->SetDSAuthEnable(false);
    client->EnableDSClient(true);

    const auto initStart = NowMs();
    const auto initStatus = client->Init();
    const auto initMs = NowMs() - initStart;
    std::cout << "DS_GATE3 init rc=" << (initStatus.IsOk() ? 0 : -1) << " ms=" << initMs << std::endl;
    jsonl << "{\"op\":\"init\",\"ok\":" << (initStatus.IsOk() ? "true" : "false")
          << ",\"ms\":" << initMs << ",\"runid\":\"" << runId << "\"}\n";
    ASSERT_TRUE(initStatus.IsOk()) << "init failed: " << initStatus.ToString()
        << " (environment problem — not a deadline conclusion)";

    // STRICT verification endpoint: the wrapper (DSCacheClientImpl) maps
    // every DS error to one code, so absence is decided by the RAW SDK —
    // a datasystem::KVClient whose Get must return the SDK's OWN
    // K_NOT_FOUND for a deleted key. Any OTHER code (timeout, disconnect,
    // internal) is UNKNOWN/failure, never absence. The control key keeps
    // the reader's health observable through the same raw client.
    datasystem::ConnectOptions verifyConnect{};
    verifyConnect.host = host;
    verifyConnect.port = std::stoi(workerPort);
    datasystem::KVClient rawVerifier(verifyConnect);
    const auto rawInit = rawVerifier.Init();
    ASSERT_TRUE(rawInit.IsOk()) << "raw verifier init failed: " << rawInit.ToString()
        << " (environment problem — not a deadline conclusion)";
    const std::string controlKey = "gate3:expire:" + runId + ":control";
    const std::string controlValue = "alive";
    const auto rawCtlSet = rawVerifier.Set(controlKey, controlValue);
    ASSERT_TRUE(rawCtlSet.IsOk()) << "raw control Set failed: " << rawCtlSet.ToString();
    std::string rawCtlBack;
    const auto rawCtlGet = rawVerifier.Get(controlKey, rawCtlBack);
    ASSERT_TRUE(rawCtlGet.IsOk() && rawCtlBack == controlValue)
        << "raw control Get failed: " << rawCtlGet.ToString();

    int iterations = 20;
    // EXPLICIT ARM POINT (root requirement): Init + verifier + control are
    // all green BEFORE any fault can act. When DS_GATE3_ARM names a file,
    // the proxy's fault mode activates only once this file exists — we
    // create it HERE, after init and the control-key write, so delay/drop/
    // hold hit ONLY the operation stage. Recorded in the jsonl.
    const std::string armFile = EnvOr("DS_GATE3_ARM", "");
    if (!armFile.empty()) {
        std::ofstream arm(armFile);
        arm << NowMs();
        arm.close();
        jsonl << "{\"op\":\"armed\",\"file\":\"" << armFile << "\",\"runid\":\"" << runId << "\"}\n";
        jsonl.flush();
        std::cout << "DS_GATE3 armed " << armFile << std::endl;
    }
    for (int i = 0; i < iterations; ++i) {
        const std::string key = "gate3:expire:" + runId + ":k" + std::to_string(i);
        const std::string value = "v" + std::to_string(i) + "-" + std::string(64, 'x');

        // BEGIN rows flushed BEFORE each call; outcome rows (actual
        // code/message/elapsed) AFTER — a mid-op crash leaves the begin row
        // as the record, never a silent gap.
        jsonl << "{\"op\":\"begin\",\"i\":" << i << ",\"phase\":\"set\",\"runid\":\"" << runId << "\"}\n";
        jsonl.flush();
        const auto setStart = NowMs();
        const auto setStatus = client->Set(key, value);
        const auto setMs = NowMs() - setStart;
        jsonl << "{\"op\":\"result\",\"i\":" << i << ",\"phase\":\"set\",\"ok\":"
              << (setStatus.IsOk() ? "true" : "false") << ",\"code\":"
              << static_cast<int>(setStatus.StatusCode()) << ",\"ms\":" << setMs
              << ",\"runid\":\"" << runId << "\"}\n";
        jsonl.flush();
        ASSERT_TRUE(setStatus.IsOk()) << "Set[" << i << "] failed (recorded above): "
                                      << setStatus.ToString();

        jsonl << "{\"op\":\"begin\",\"i\":" << i << ",\"phase\":\"get\",\"runid\":\"" << runId << "\"}\n";
        jsonl.flush();
        std::string got;
        const auto getStart = NowMs();
        const auto getStatus = client->Get(key, got);
        const auto getMs = NowMs() - getStart;
        jsonl << "{\"op\":\"result\",\"i\":" << i << ",\"phase\":\"get\",\"ok\":"
              << (getStatus.IsOk() ? "true" : "false") << ",\"code\":"
              << static_cast<int>(getStatus.StatusCode()) << ",\"ms\":" << getMs
              << ",\"match\":" << (got == value ? "true" : "false")
              << ",\"runid\":\"" << runId << "\"}\n";
        jsonl.flush();
        ASSERT_TRUE(getStatus.IsOk()) << "Get[" << i << "] failed (recorded above): "
                                      << getStatus.ToString();
        ASSERT_EQ(got, value) << "Get[" << i << "] mismatch";

        jsonl << "{\"op\":\"begin\",\"i\":" << i << ",\"phase\":\"del\",\"runid\":\"" << runId << "\"}\n";
        jsonl.flush();
        const auto delStart = NowMs();
        const auto delStatus = client->Del(key);
        const auto delMs = NowMs() - delStart;
        jsonl << "{\"op\":\"result\",\"i\":" << i << ",\"phase\":\"del\",\"ok\":"
              << (delStatus.IsOk() ? "true" : "false") << ",\"code\":"
              << static_cast<int>(delStatus.StatusCode()) << ",\"ms\":" << delMs
              << ",\"runid\":\"" << runId << "\"}\n";
        jsonl.flush();
        ASSERT_TRUE(delStatus.IsOk()) << "Del[" << i << "] failed (recorded above): "
                                      << delStatus.ToString();

        // Miss check through the RAW SDK with the strict criterion: the
        // control read proves health; the target read must return the
        // SDK's OWN K_NOT_FOUND (rc==K_NOT_FOUND) — anything else is
        // UNKNOWN/failure, never absence.
        std::string controlBack2;
        const auto rawCtl2 = rawVerifier.Get(controlKey, controlBack2);
        ASSERT_TRUE(rawCtl2.IsOk() && controlBack2 == controlValue)
            << "raw control re-read failed — reader unhealthy, absence UNDECIDABLE: "
            << rawCtl2.ToString();
        std::string after;
        const auto rawMiss = rawVerifier.Get(key, after);
        const auto rawRc = static_cast<int>(rawMiss.GetCode());
        const bool genuineMiss = (rawRc == static_cast<int>(datasystem::StatusCode::K_NOT_FOUND));
        jsonl << "{\"op\":\"miss_check\",\"i\":" << i
              << ",\"control_ok\":true,\"raw_rc\":" << rawRc
              << ",\"raw_msg\":\"" << rawMiss.GetMsg() << "\""
              << ",\"genuine_miss\":" << (genuineMiss ? "true" : "false")
              << ",\"runid\":\"" << runId << "\"}\n";
        jsonl.flush();
        ASSERT_TRUE(genuineMiss) << "Get-after-Del[" << i << "] must be the RAW SDK "
            << "K_NOT_FOUND, got rc=" << rawRc << " msg=" << rawMiss.GetMsg()
            << " (any other code is unknown/failure, not absence)";

        if (i == 0 || i == iterations - 1) {
            std::cout << "DS_GATE3 cycle[" << i << "] set=" << setMs << "ms get=" << getMs
                      << "ms del=" << delMs << "ms" << std::endl;
        }
    }
    (void)rawVerifier.Del(controlKey);
    SUCCEED();
}

}  // namespace functionsystem::test
