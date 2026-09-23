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

#include "runtime_manager/manager/runtime_manager.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include "common/utils/files.h"
#include "common/utils/resume_identity.h"
#include "exec/exec.hpp"
#include "gtest/gtest.h"
#include "runtime_manager/config/build.h"
#include "runtime_manager/executor/sandboxd/retire_fault_gate.h"
#include "runtime_manager/executor/sandboxd/sandboxd_executor.h"
#include "runtime_manager/port/port_manager.h"
#include "runtime_manager_test_actor.h"
#include "utils/future_test_helper.h"
#include "utils/generate_info.h"
#include "utils/port_helper.h"

using namespace functionsystem::test;
namespace functionsystem::runtime_manager {

TEST(ReusableSnapshotCreateRuntimeConfigTest, TrustedCloneAloneEnablesRrtLogicalInstanceRebind)
{
    RuntimeConfig runtimeConfig;
    runtimeConfig.hostIP = "10.0.0.1";
    auto startReq = std::make_shared<messages::StartInstanceRequest>();
    auto *info = startReq->mutable_runtimeinstanceinfo();
    info->set_instanceid("new-logical-instance");
    info->set_runtimeid("deterministic-runtime");

    const auto ordinary = GeneratePosixEnvs(runtimeConfig, startReq, "21000");
    EXPECT_EQ(ordinary.count("YR_ALLOW_LOGICAL_INSTANCE_ID_REBIND"), 0U);

    auto *restore = info->mutable_reusablesnapshotrestore();
    restore->set_snapshotid("snapshot-42");
    restore->set_allowlogicalinstanceidrebind(true);
    restore->mutable_artifact()->set_objectkey(
        "reusable/v1/tenant-hash/snapshot-42/checkpoint.img");
    const auto clone = GeneratePosixEnvs(runtimeConfig, startReq, "21000");
    ASSERT_NE(clone.find("YR_ALLOW_LOGICAL_INSTANCE_ID_REBIND"), clone.end());
    EXPECT_EQ(clone.at("YR_ALLOW_LOGICAL_INSTANCE_ID_REBIND"), "true");

    restore->set_allowlogicalinstanceidrebind(false);
    const auto untrusted = GeneratePosixEnvs(runtimeConfig, startReq, "21000");
    EXPECT_EQ(untrusted.count("YR_ALLOW_LOGICAL_INSTANCE_ID_REBIND"), 0U);
}

TEST(ReusableSnapshotCreateRuntimeConfigTest, ReusableCloneUsesDeterministicAttemptRuntimeID)
{
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("clone-instance");
    info.set_requestid("clone-attempt");
    EXPECT_EQ(RuntimeManager::RuntimeIDForStart(info, true),
              resume_identity::RuntimeID("clone-instance", "clone-attempt"));
    EXPECT_NE(RuntimeManager::RuntimeIDForStart(info, false),
              resume_identity::RuntimeID("clone-instance", "clone-attempt"));
}

const uint32_t MAX_REGISTER_TEST_TIMES = 5;
const uint32_t INITIAL_PORT = 700;
const uint32_t PORT_NUM = 800;
const std::string testDeployDir = "/tmp/layer/func/bucket-test-log1/yr-test-runtime-manager";
const std::string funcObj = testDeployDir + "/" + "funcObj";
const std::string POST_START_EXEC_REGEX = R"(^(uv )?pip3.[0-9]* install [a-zA-Z0-9\-\s:/\.=_]* && pip3.[0-9]* check$)";

const std::vector<std::string> debugServerScriptContent = {
    "import socket",
    "import threading",
    "import os",
    "import signal",
    "import time",
    "import argparse",

    "def forward_data(source, destination):",
    "    try:",
    "        while True:",
    "            data = source.recv(4096)  # ���ն��������ݣ������н���",
    "            if not data:  # ���ӹر�",
    "                break",
    "            destination.sendall(data)  # ����ԭʼ����������",
    "    except (ConnectionResetError, BrokenPipeError, socket.error) as e:",
    "        print(f\"Connection error in forward_data: {e}\")",
    "    finally:",
    "        source.shutdown(socket.SHUT_RDWR)",
    "        source.close()",
    "        destination.shutdown(socket.SHUT_RDWR)",
    "        destination.close()",

    "def process_b(port):",
    "    # ����socket������",
    "    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)",
    "    server_socket.bind(('localhost', port))",
    "    server_socket.listen(2)  # ��Ҫ����A��C��������",

    "    while True:",
    "        print(\"B�������������ȴ�����...\")",

    "        # ���Ƚ���C���̵����ӺͶ˿���Ϣ",
    "        conn_c, addr_c = server_socket.accept()",
    "        print(f\"B����: �յ�����C���̵����� {addr_c}\")",
    "        # ��C���̽��ն˿���Ϣ",
    "        recved = conn_c.recv(1024).strip().decode()",
    "        c_port, c_pid = recved.split(' ')",
    "        c_port, c_pid = int(c_port), int(c_pid)",
    "        print(f\"B����: C����ʵ�ʶ˿�Ϊ {c_port}�����̺� {c_pid}\")",
    "        conn_c.close()",

    "        # Ȼ�����A���̵�����",
    "        conn_a, addr_a = server_socket.accept()",
    "        print(f\"B����: �յ�����A���̵����� {addr_a}\")",

    "        # ����SIGCONT������C",
    "        os.kill(c_pid, signal.SIGCONT)",
    "        time.sleep(0.5)",

    "        # ���ӵ�C���̵�ʵ�ʶ˿�",
    "        conn_c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)",
    "        conn_c.connect(('localhost', c_port))",
    "        print(\"B����: �����ӵ�C���̵�ʵ�ʶ˿�\")",

    "        t1 = threading.Thread(target=forward_data, args=(conn_c, conn_a))",
    "        t2 = threading.Thread(target=forward_data, args=(conn_a, conn_c))",

    "        t1.start()",
    "        t2.start()",
    "        t1.join()",
    "        t2.join()",

    "    server_socket.close()",

    "if __name__ == \"__main__\":",
    "    parser = argparse.ArgumentParser(description=\"Proxy server\")",
    "    parser.add_argument('--port', type=int, help=\"Port number to listen on\", default=5555)",
    "    args = parser.parse_args()",
    "    process_b(args.port)"
};

class DISABLED_RuntimeManagerTest : public ::testing::Test {
public:
    [[maybe_unused]] static void SetUpTestSuite()
    {
        if (!litebus::os::ExistPath("/tmp/cpp/bin")) {
            litebus::os::Mkdir("/tmp/cpp/bin");
        }

        auto fd = open("/tmp/cpp/bin/runtime", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
        EXPECT_NE(fd, -1);
        close(fd);

        std::ofstream outfile;
        outfile.open("/tmp/cpp/bin/runtime");
        outfile << "sleep 2" << std::endl;
        outfile.close();
    }

    void SetUp() override
    {
        PortManager::GetInstance().InitPortResource(INITIAL_PORT, PORT_NUM);
        (void)litebus::os::Mkdir(testDeployDir);
        (void)TouchFile(funcObj);
        (void)system(
            "echo \"testDeployDir in runtime_manager_test\""
            "> /tmp/layer/func/bucket-test-log1/yr-test-runtime-manager/funcObj");

        runtimeManagerActorName_ = GenerateRandomName("RuntimeManagerActor");
        manager_ = std::make_shared<RuntimeManager>(runtimeManagerActorName_);
        manager_->isUnitTestSituation_ = true;
        litebus::Spawn(manager_, true);
        manager_->connected_ = true;
        manager_->isUnitTestSituation_ = true;
    }

    void TearDown() override
    {
        (void)litebus::os::Rmdir(testDeployDir);
        PortManager::GetInstance().Clear();
        litebus::Terminate(manager_->GetAID());
        litebus::Await(manager_);
    }

    static void sigHandler(int signum)
    {
        sigReceived_.SetValue(true);
    }

    messages::StartInstanceRequest GenStartInstanceRequest()
    {
        messages::StartInstanceRequest startRequest;
        startRequest.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
        auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
        runtimeInfo->set_requestid("test_requestID");
        runtimeInfo->set_instanceid("test_instanceID");
        runtimeInfo->set_traceid("test_traceID");

        auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
        runtimeConfig->set_language("cpp");
        auto userEnvs = runtimeConfig->mutable_userenvs();
        userEnvs->insert({ "user_env1", "user_env1_value" });
        userEnvs->insert({ "user_env2", "user_env2_value" });

        auto deployConfig = runtimeInfo->mutable_deploymentconfig();
        deployConfig->set_objectid("test_objectID");
        deployConfig->set_bucketid("test_bucketID");
        deployConfig->set_deploydir(testDeployDir);
        deployConfig->set_storagetype("s3");
        return startRequest;
    }

protected:
    std::string runtimeManagerActorName_;
    std::shared_ptr<RuntimeManager> manager_;
    std::shared_ptr<RuntimeManagerTestActor> testActor_;
    inline static litebus::Future<bool> sigReceived_;
};

class DISABLED_RuntimeManagerDebugServerTest : public DISABLED_RuntimeManagerTest {
public:
    void SetUp() override
    {
        auto optionEnv = litebus::os::GetEnv("PATH");
        if (optionEnv.IsSome()) {
            env_ = optionEnv.Get();
        }

        DISABLED_RuntimeManagerTest::SetUp();
        litebus::os::SetEnv("PATH", litebus::os::Join("/tmp", env_, ':'));
        (void)litebus::os::Rm("/tmp/gdbserver");
        auto fd = open("/tmp/gdbserver", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
        close(fd);
        EXPECT_AWAIT_TRUE([=]() { return FileExists("/tmp/gdbserver"); });
        (void)litebus::os::Rm("/tmp/python3");
        fd = open("/tmp/python3", O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
        close(fd);
        EXPECT_AWAIT_TRUE([=]() { return FileExists("/tmp/python3"); });

        // python debug server script
         (void)litebus::os::Rm("/tmp/python/fnruntime/debug_server.py");
        if (!litebus::os::ExistPath("/tmp/python/fnruntime")) {
            litebus::os::Mkdir("/tmp/python/fnruntime");
        }
        std::string debugServerScriptPath = litebus::os::Join("/tmp/python/fnruntime", "debug_server.py");
        (void)litebus::os::Rm(debugServerScriptPath);
        TouchFile(debugServerScriptPath);
        std::ofstream outfile(debugServerScriptPath);
        if (outfile.is_open()) {
            for (const auto& line : debugServerScriptContent) {
                outfile << line << std::endl;
            }
            outfile.close();
        } else {
            std::cerr << "cannot open file" << std::endl;
        }

        functionsystem::runtime_manager::Flags flags;
        const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
        const char *argv[] = {
            "/runtime_manager",
            "--node_id=node1",
            "--ip=127.0.0.1",
            "--host_ip=127.0.0.1",
            port,
            "--runtime_initial_port=500",
            "--port_num=2000",
            "--runtime_dir=/tmp",
            const_cast<char *>("123.123.123.123:80"),
            "--runtime_ld_library_path=/tmp",
            "--proc_metrics_cpu=2000",
            "--proc_metrics_memory=2000",
            "--runtime_instance_debug_enable=true",
            R"(--log_config={"filepath": "/tmp/home/yr/log", "level": "DEBUG", "rolling": {"maxsize": 100, "maxfiles": 1},"alsologtostderr":true})"
        };
        flags.ParseFlags(std::size(argv), argv);
        manager_->SetRegisterHelper(std::make_shared<RegisterHelper>("node1-RuntimeManagerSrv"));
        manager_->SetConfig(flags);
    }

    void TearDown() override
    {
        litebus::os::SetEnv("PATH", env_);
        (void)litebus::os::Rm("/tmp/gdbserver");
        (void)litebus::os::Rm("/tmp/python3");
        DISABLED_RuntimeManagerTest::TearDown();
    }

    static inline std::string env_;
};

class RuntimeManagerTypeTest : public DISABLED_RuntimeManagerTest {};

class RuntimeManagerCapabilityTest : public ::testing::Test {
public:
    void SetUp() override
    {
        manager_ = std::make_shared<RuntimeManager>(GenerateRandomName("RuntimeManagerCapabilityTest"));
        manager_->isUnitTestSituation_ = true;
        litebus::Spawn(manager_, true);
        manager_->connected_ = true;
        testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
        litebus::Spawn(testActor_, true);
    }

    void TearDown() override
    {
        litebus::Terminate(testActor_->GetAID());
        litebus::Await(testActor_->GetAID());
        litebus::Terminate(manager_->GetAID());
        litebus::Await(manager_->GetAID());
    }

protected:
    std::shared_ptr<RuntimeManager> manager_;
    std::shared_ptr<RuntimeManagerTestActor> testActor_;
};

TEST_F(RuntimeManagerCapabilityTest, RejectsDisabledDataSystemWithoutBypass)
{
    messages::StartInstanceRequest request;
    request.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = request.mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("invalid-capability-request");
    runtimeInfo->set_instanceid("invalid-capability-instance");
    runtimeInfo->set_traceid("invalid-capability-trace");
    auto envs = runtimeInfo->mutable_runtimeconfig()->mutable_posixenvs();
    (*envs)["YR_DATASYSTEM_DEPLOYED"] = "false";
    (*envs)["YR_BYPASS_DATASYSTEM"] = "false";

    testActor_->StartInstance(manager_->GetAID(), request);

    ASSERT_AWAIT_TRUE([this]() { return testActor_->GetIsReceiveStartInstanceResponse(); });
    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(response->code(), static_cast<int32_t>(RUNTIME_MANAGER_PARAMS_INVALID));
    EXPECT_EQ(response->message(), "YR_DATASYSTEM_DEPLOYED=false requires YR_BYPASS_DATASYSTEM=true");
    EXPECT_TRUE(response->startruntimeinstanceresponse().runtimeid().empty());
}

TEST_F(DISABLED_RuntimeManagerTest, StartInstanceTest)
{
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
    const char *argv[] = {
        "/runtime_manager",           "--node_id=node1", "--ip=127.0.0.1",    "--host_ip=127.0.0.1", port,
        "--runtime_initial_port=500", "--port_num=2000", "--runtime_dir=/tmp"
    };
    functionsystem::runtime_manager::Flags flags;
    flags.ParseFlags(std::size(argv), argv);
    manager_->SetConfig(flags);

    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    auto startRequest = GenStartInstanceRequest();
    auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
    auto resources = runtimeInfo->mutable_runtimeconfig()->mutable_resources()->mutable_resources();
    resource_view::Resource cpuResource;
    cpuResource.set_type(::resources::Value_Type::Value_Type_SCALAR);
    cpuResource.mutable_scalar()->set_value(500.0);
    (*resources)["CPU"] = cpuResource;
    resource_view::Resource memResource;
    memResource.set_type(::resources::Value_Type::Value_Type_SCALAR);
    memResource.mutable_scalar()->set_value(500.0);
    (*resources)["Memory"] = memResource;

    // lost connection with function agent
    manager_->connected_ = false;
    testActor_->StartInstance(manager_->GetAID(), startRequest);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetIsReceiveStartInstanceResponse(); });

    manager_->connected_ = true;
    // repeat
    testActor_->ResetStartInstanceTimes();
    manager_->receivedStartingReq_ = std::unordered_set<std::string>{ "repeat-123" };
    messages::StartInstanceRequest repeatRequest;
    startRequest.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    repeatRequest.mutable_runtimeinstanceinfo()->set_requestid("repeat-123");
    testActor_->StartInstance(manager_->GetAID(), repeatRequest);
    // success
    testActor_->StartInstance(manager_->GetAID(), startRequest);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::SUCCESS, response->code());
    EXPECT_EQ("start instance success", response->message());
    EXPECT_EQ("test_requestID", response->requestid());

    auto instanceResponse = response->startruntimeinstanceresponse();
    EXPECT_TRUE(!instanceResponse.runtimeid().empty());
    EXPECT_EQ(std::to_string(INITIAL_PORT), instanceResponse.port());
    EXPECT_EQ(static_cast<long unsigned int>(1), testActor_->GetStartInstanceTimes());
    manager_->receivedStartingReq_ = {};
    testActor_->ResetMessage();
    testActor_->StartInstance(manager_->GetAID(), startRequest);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });
    response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::RUNTIME_MANAGER_INSTANCE_HAS_BEEN_DEPLOYED, response->code());
    testActor_->ResetMessage();

    startRequest.mutable_runtimeinstanceinfo()->set_requestid("req-111111");
    testActor_->StartInstance(manager_->GetAID(), startRequest);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });
    response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::RUNTIME_MANAGER_INSTANCE_EXIST, response->code());

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerTest, StartInstanceWithPreStartSuccessTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    auto startRequest = GenStartInstanceRequest();
    testActor_->StartInstance(manager_->GetAID(), startRequest);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::SUCCESS, response->code());
    EXPECT_EQ("start instance success", response->message());
    EXPECT_EQ("test_requestID", response->requestid());

    auto instanceResponse = response->startruntimeinstanceresponse();
    EXPECT_TRUE(!instanceResponse.runtimeid().empty());
    EXPECT_EQ(std::to_string(INITIAL_PORT), instanceResponse.port());

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerTest, StartInstance_PosixCustomRuntime_WithEntryfileEmpty)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    auto startRequest = GenStartInstanceRequest();
    auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("posix-custom-runtime");
    testActor_->StartInstance(manager_->GetAID(), startRequest);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::RUNTIME_MANAGER_EXECUTABLE_PATH_INVALID, response->code());
    EXPECT_EQ("[entryFile is empty]", response->message());
    EXPECT_EQ("test_requestID", response->requestid());

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerTest, StartInstanceWithPreStartFailedTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    auto startRequest = GenStartInstanceRequest();
    auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->mutable_posixenvs()->insert({ "POST_START_EXEC", "/usr/bin/cp a b;" });
    testActor_->StartInstance(manager_->GetAID(), startRequest);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::RUNTIME_MANAGER_POST_START_EXEC_FAILED, response->code());
    EXPECT_TRUE(response->message().find("is not match the regular") != std::string::npos);
    EXPECT_EQ("test_requestID", response->requestid());

    auto instanceResponse = response->startruntimeinstanceresponse();
    EXPECT_TRUE(instanceResponse.runtimeid().empty());

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

// Note: this case Connection to pypi.org timed out, set `export NOT_SKIP_LONG_TESTS=1` when run it, and not run on CI by default
TEST_F(DISABLED_RuntimeManagerTest, StartInstance_PosixCustomRuntime_POST_START_EXEC_pip_install_SUCCESS)
{
    const char* skip_test = std::getenv("NOT_SKIP_LONG_TESTS");
    if (skip_test == nullptr || std::string(skip_test) != "1") {
        GTEST_SKIP() << "Long-running tests are skipped by default";
    }

    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    auto startRequest = GenStartInstanceRequest();
    auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("posix-custom-runtime");
    runtimeConfig->set_entryfile("echo hello");
    runtimeConfig->mutable_posixenvs()->insert(
        {"LD_LIBRARY_PATH", "${LD_LIBRARY_PATH}:/opt/buildtools/python3.9/lib/"});
    runtimeConfig->mutable_posixenvs()->insert(
        { "POST_START_EXEC",
          "pip3.9 install pip-licenses==5.0.0 && pip3.9 check" });
    // both UNZIPPED_WORKING_DIR and YR_WORKING_DIR are required.
    runtimeConfig->mutable_posixenvs()->insert({ "UNZIPPED_WORKING_DIR", "/tmp" });
    runtimeConfig->mutable_posixenvs()->insert({ "YR_WORKING_DIR", "file:///tmp/file.zip" });

    testActor_->StartInstance(manager_->GetAID(), startRequest);

    EXPECT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::SUCCESS, response->code());
    EXPECT_EQ("start instance success", response->message());
    EXPECT_EQ("test_requestID", response->requestid());

    auto instanceResponse = response->startruntimeinstanceresponse();
    EXPECT_TRUE(!instanceResponse.runtimeid().empty());
    EXPECT_EQ(std::to_string(INITIAL_PORT), instanceResponse.port());

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

// Note: this case cost long and failed by other tests in CI, set `export NOT_SKIP_LONG_TESTS=1` when run it, and not run on CI by default
TEST_F(DISABLED_RuntimeManagerTest, StartInstance_PosixCustomRuntime_POST_START_EXEC_pip_install_FAIL)
{
    const char* skip_test = std::getenv("NOT_SKIP_LONG_TESTS");
    if (skip_test == nullptr || std::string(skip_test) != "1") {
        GTEST_SKIP() << "Long-running tests are skipped by default";
    }

    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    messages::StartInstanceRequest startRequest;
    startRequest.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("posix-custom-runtime");
    runtimeConfig->set_entryfile("echo hello");
    runtimeConfig->mutable_posixenvs()->insert(
        {"LD_LIBRARY_PATH", "${LD_LIBRARY_PATH}:/opt/buildtools/python3.9/lib/"});
    runtimeConfig->mutable_posixenvs()->insert(
        { "POST_START_EXEC",
          "pip3.9 install pip-licenses==5xxx && pip3.9 check" });
    // both UNZIPPED_WORKING_DIR and YR_WORKING_DIR are required.
    runtimeConfig->mutable_posixenvs()->insert({ "UNZIPPED_WORKING_DIR", "/tmp" });
    runtimeConfig->mutable_posixenvs()->insert({ "YR_WORKING_DIR", "file:///tmp/file.zip" });

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");

    testActor_->StartInstance(manager_->GetAID(), startRequest);

    EXPECT_AWAIT_TRUE_FOR([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); }, 30000);

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::RUNTIME_MANAGER_POST_START_EXEC_FAILED, response->code());
    EXPECT_TRUE(response->message().find("failed to execute POST_START_EXEC command") != std::string::npos);
    EXPECT_EQ("test_requestID", response->requestid());

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

/**
 * Feature: StartInstanceWithInvalidRequestTest
 * Description: start instance when sending invalid message
 * Steps:
 * send invalid requset to start instance
 * Expectation:
 * start instance failed
 */
TEST_F(DISABLED_RuntimeManagerTest, StartInstanceWithInvalidRequestTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    testActor_->StartInstanceWithString(manager_->GetAID(), "");

    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetStartInstanceResponse()->message().empty(); });

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

/**
 * Feature: StartInstanceWithInvalidExecutorTypeTest
 * Description: start instance when sending message with invalid executor type
 * Steps:
 * start instance request with invalid executor type
 * Expectation:
 * return RUNTIME_MANAGER_PARAMS_INVALID code in response
 */
TEST_F(DISABLED_RuntimeManagerTest, StartInstanceWithInvalidExecutorTypeTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    auto startRequest = GenStartInstanceRequest();
    startRequest.set_type(static_cast<int32_t>(EXECUTOR_TYPE::UNKNOWN));
    testActor_->StartInstance(manager_->GetAID(), startRequest);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::RUNTIME_MANAGER_PARAMS_INVALID, response->code());
    EXPECT_EQ("unknown instance type, cannot start instance", response->message());

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerTest, StartInstanceRequestPortFailTest)
{
    // can only support one runtime instance
    PortManager::GetInstance().InitPortResource(0, 1);
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    messages::StartInstanceRequest startRequest;
    startRequest.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");

    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("s3");
    testActor_->StartInstance(manager_->GetAID(), startRequest);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::SUCCESS, response->code());
    EXPECT_EQ("start instance success", response->message());
    EXPECT_EQ("test_requestID", response->requestid());

    startRequest.set_type(static_cast<int32_t>(EXECUTOR_TYPE::UNKNOWN));
    testActor_->StartInstance(manager_->GetAID(), startRequest);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto instanceResponse = response->mutable_startruntimeinstanceresponse();
    EXPECT_TRUE(!instanceResponse->runtimeid().empty());
    EXPECT_EQ("0", instanceResponse->port());

    auto testActorNew = std::make_shared<RuntimeManagerTestActor>("NewRuntimeManagerTestActor");

    litebus::Spawn(testActorNew, true);

    messages::StartInstanceRequest startRequestNew;
    startRequestNew.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfoNew = startRequestNew.mutable_runtimeinstanceinfo();
    runtimeInfoNew->set_requestid("test_requestIDNew");
    runtimeInfoNew->set_instanceid("test_instanceIDNew");
    runtimeInfoNew->set_traceid("test_traceIDNew");

    auto runtimeConfigNew = runtimeInfoNew->mutable_runtimeconfig();
    runtimeConfigNew->set_language("cpp");

    testActorNew->StartInstance(manager_->GetAID(), startRequestNew);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActorNew->GetStartInstanceResponse()->message().empty(); });

    auto responseNew = testActorNew->GetStartInstanceResponse();
    EXPECT_EQ(responseNew->code(), RUNTIME_MANAGER_PORT_UNAVAILABLE);
    EXPECT_EQ(responseNew->message(), "start instance failed");

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
    litebus::Terminate(testActorNew->GetAID());
    litebus::Await(testActorNew->GetAID());

    PortManager::GetInstance().InitPortResource(INITIAL_PORT, PORT_NUM);
}

TEST_F(DISABLED_RuntimeManagerTest, StopInstanceTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    auto startRequest = GenStartInstanceRequest();
    testActor_->StartInstance(manager_->GetAID(), startRequest);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });
    EXPECT_FALSE(manager_->runtimeInstanceDebugEnable_);

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::SUCCESS, response->code());
    EXPECT_EQ("start instance success", response->message());
    EXPECT_EQ("test_requestID", response->requestid());

    auto instanceResponse = response->mutable_startruntimeinstanceresponse();
    auto resRuntimeID = instanceResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());
    EXPECT_EQ(std::to_string(INITIAL_PORT), instanceResponse->port());

    const std::string stopRequestID = "test_requestID";
    messages::StopInstanceRequest request;
    request.set_runtimeid(resRuntimeID);
    request.set_requestid(stopRequestID);
    request.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    // lost connection with function agent
    manager_->connected_ = false;
    testActor_->StopInstance(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetIsReceiveStopInstanceResponse(); });

    manager_->connected_ = true;
    // success
    testActor_->StopInstance(manager_->GetAID(), request);
    EXPECT_TRUE(manager_->healthCheckClient_->actor_->runtimeStatus_.find(resRuntimeID)
                != manager_->healthCheckClient_->actor_->runtimeStatus_.end());
    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetStopInstanceResponse()->requestid() == stopRequestID; });
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->message(), "stop instance success");
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->runtimeid(), resRuntimeID);
    EXPECT_TRUE(manager_->healthCheckClient_->actor_->runtimeStatus_.find(resRuntimeID)
                == manager_->healthCheckClient_->actor_->runtimeStatus_.end());

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerTest, StopInstanceFailTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    messages::StopInstanceRequest request;
    request.set_runtimeid("test_runtimeID");
    request.set_requestid("test_requestID");
    request.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    testActor_->StopInstance(manager_->GetAID(), request);

    auto stopResponse = testActor_->GetStopInstanceResponse();
    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetStopInstanceResponse()->requestid() == "test_requestID"; });
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->runtimeid(), "test_runtimeID");
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->code(),
              static_cast<int32_t>(RUNTIME_MANAGER_RUNTIME_PROCESS_NOT_FOUND));
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->message(), "stop instance failed");

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerTest, StopInstanceFailWithUnknowExecutorTypeTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    messages::StopInstanceRequest request;
    request.set_runtimeid("test_runtimeID");
    request.set_requestid("test_requestID");
    request.set_type(static_cast<int32_t>(EXECUTOR_TYPE::UNKNOWN));
    testActor_->StopInstance(manager_->GetAID(), request);

    auto stopResponse = testActor_->GetStopInstanceResponse();
    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetStopInstanceResponse()->requestid() == "test_requestID"; });
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->runtimeid(), "test_runtimeID");
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->code(), static_cast<int32_t>(RUNTIME_MANAGER_PARAMS_INVALID));
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->message(), "unknown instance type, cannot stop instance");

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

/**
 * Feature: StopInstanceWithInvalidRequestTest
 * Description: Stop Instance When sending invalid request
 * Steps:
 * send invalid request to stop instance
 * Expectation:
 * no response
 */
TEST_F(DISABLED_RuntimeManagerTest, StopInstanceWithInvalidRequestTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    testActor_->StartInstanceWithString(manager_->GetAID(), "");

    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetStartInstanceResponse()->message().empty(); });

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

/**
 * Feature: RegisterToFunctionAgentFailedTest
 * Description: runtime manager register to function agent return error code
 * Steps:
 * runtime manager register to function agent
 * function agent return REGISTER_ERROR in response
 * Expectation:
 * runtime manager print the error in log
 */
TEST_F(DISABLED_RuntimeManagerTest, RegisterToFunctionAgentFailedTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>("AgentServiceActor-RegisterHelper");
    litebus::Spawn(testActor_, true);

    functionsystem::runtime_manager::Flags flags;
    std::string agentAddress = "--agent_address=" + testActor_->GetAID().Url();
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
    const char *argv[] = {
        "/runtime_manager",
        "--node_id=node1",
        "--ip=127.0.0.1",
        "--host_ip=127.0.0.1",
        port,
        "--runtime_initial_port=500",
        "--port_num=2000",
        "--runtime_dir=/tmp",
        const_cast<char *>(agentAddress.c_str()),
        "--runtime_ld_library_path=/tmp",
        "--proc_metrics_cpu=2000",
        "--proc_metrics_memory=2000",
        R"(--log_config={"filepath": "/tmp/home/yr/log", "level": "DEBUG", "rolling": {"maxsize": 100, "maxfiles": 1},"alsologtostderr":true})"
    };
    flags.ParseFlags(std::size(argv), argv);
    manager_->SetRegisterHelper(std::make_shared<RegisterHelper>("node1-RuntimeManagerSrv"));
    manager_->SetConfig(flags);
    messages::RegisterRuntimeManagerResponse registerRuntimeManagerResponse;
    registerRuntimeManagerResponse.set_code(StatusCode::REGISTER_ERROR);
    testActor_->SetRegisterRuntimeManagerResponse(registerRuntimeManagerResponse);
    manager_->Start();

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetRegisterRuntimeManagerRequest()->address().empty(); });
    uint16_t lport = GetPortEnv("LITEBUS_PORT", 8080);
    EXPECT_TRUE(("127.0.0.1:" + std::to_string(lport)) == testActor_->GetRegisterRuntimeManagerRequest()->address());
    EXPECT_TRUE(runtimeManagerActorName_ == testActor_->GetRegisterRuntimeManagerRequest()->name());
    EXPECT_TRUE(testActor_->GetRegisterRuntimeManagerRequest()->mutable_runtimeinstanceinfos()->size() == 0);

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

/**
 * Feature: RegisterToFunctionAgentUnknownErrorTest
 * Description: runtime manager register to function agent return error code
 * Steps:
 * runtime manager register to function agent
 * function agent return unknown Error in response
 * Expectation:
 * runtime manager print the error in log
 */
TEST_F(DISABLED_RuntimeManagerTest, RegisterToFunctionAgentUnknownErrorTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>("AgentServiceActor-RegisterHelper");
    litebus::Spawn(testActor_, true);

    functionsystem::runtime_manager::Flags flags;
    std::string agentAddress = "--agent_address=" + testActor_->GetAID().Url();
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
    const char *argv[] = {
        "/runtime_manager",
        "--node_id=node1",
        "--ip=127.0.0.1",
        "--host_ip=127.0.0.1",
        port,
        "--runtime_initial_port=500",
        "--port_num=2000",
        "--runtime_dir=/tmp",
        const_cast<char *>(agentAddress.c_str()),
        "--runtime_ld_library_path=/tmp",
        "--proc_metrics_cpu=2000",
        "--proc_metrics_memory=2000",
        R"(--log_config={"filepath": "/tmp/home/yr/log", "level": "DEBUG", "rolling": {"maxsize": 100, "maxfiles": 1},"alsologtostderr":true})"
    };
    flags.ParseFlags(std::size(argv), argv);
    manager_->SetRegisterHelper(std::make_shared<RegisterHelper>("node1-RuntimeManagerSrv"));
    manager_->SetConfig(flags);
    messages::RegisterRuntimeManagerResponse registerRuntimeManagerResponse;
    registerRuntimeManagerResponse.set_code(StatusCode::FAILED);
    testActor_->SetRegisterRuntimeManagerResponse(registerRuntimeManagerResponse);
    manager_->Start();

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetRegisterRuntimeManagerRequest()->address().empty(); });
    uint16_t lport = GetPortEnv("LITEBUS_PORT", 8080);
    EXPECT_TRUE(("127.0.0.1:" + std::to_string(lport)) == testActor_->GetRegisterRuntimeManagerRequest()->address());
    EXPECT_TRUE(runtimeManagerActorName_ == testActor_->GetRegisterRuntimeManagerRequest()->name());
    EXPECT_TRUE(testActor_->GetRegisterRuntimeManagerRequest()->mutable_runtimeinstanceinfos()->size() == 0);

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerTest, RegisterToFunctionAgentTimeoutTest)
{
    manager_->isUnitTestSituation_ = false;

    functionsystem::runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
    const char *argv[] = {
        "/runtime_manager",
        "--node_id=node1",
        "--ip=127.0.0.1",
        "--host_ip=127.0.0.1",
        port,
        "--runtime_initial_port=500",
        "--port_num=2000",
        "--runtime_dir=/tmp",
        const_cast<char *>("127.0.0.1:80"),
        "--runtime_ld_library_path=/tmp",
        "--proc_metrics_cpu=2000",
        "--proc_metrics_memory=2000",
        R"(--log_config={"filepath": "/tmp/home/yr/log", "level": "DEBUG", "rolling": {"maxsize": 100, "maxfiles": 1},"alsologtostderr":true})"
    };
    flags.ParseFlags(std::size(argv), argv);
    manager_->SetRegisterHelper(std::make_shared<RegisterHelper>("node1-RuntimeManagerSrv"));
    manager_->SetConfig(flags);
    manager_->SetRegisterInterval(5);
    signal(SIGINT, sigHandler);
    manager_->Start();
    ASSERT_AWAIT_READY(sigReceived_);

    manager_->isUnitTestSituation_ = true;
}

/**
 * Feature: QueryInstanceStatusInfoTest
 * Description: function agent query the instance info
 * Steps:
 * function agent send to runtime manager to query instance info
 * Expectation:
 * get current response
 */
TEST_F(DISABLED_RuntimeManagerTest, QueryInstanceStatusInfoTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    messages::QueryInstanceStatusRequest request;
    request.set_requestid("request_id");
    request.set_instanceid("instance_id");
    request.set_runtimeid("runtime_id");

    // lost connection with function agent
    manager_->connected_ = false;
    testActor_->QueryInstanceStatusInfo(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetIsReceiveQueryInstanceStatusInfoResponse(); });

    manager_->connected_ = true;
    testActor_->QueryInstanceStatusInfo(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetQueryInstanceStatusResponse()->requestid().empty(); });
    EXPECT_EQ(testActor_->GetQueryInstanceStatusResponse()->instancestatusinfo().type(),
              static_cast<int32_t>(EXIT_TYPE::NONE_EXIT));

    manager_->instanceInfoMap_["runtime_id"] = messages::RuntimeInstanceInfo();
    // success
    testActor_->QueryInstanceStatusInfo(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetQueryInstanceStatusResponse()->requestid().empty(); });
    EXPECT_TRUE(testActor_->GetQueryInstanceStatusResponse()->requestid() == "request_id");
    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerTest, CleanStatusTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    testActor_->Send(manager_->GetAID(), "CleanStatus", "invalid msg&&");
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetIsReceiveCleanStatusResponse(); });
    messages::CleanStatusRequest cleanStatusRequest;
    cleanStatusRequest.set_name("invalid RuntimeManagerID");
    testActor_->Send(manager_->GetAID(), "CleanStatus", cleanStatusRequest.SerializeAsString());
    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetIsReceiveCleanStatusResponse(); });
    testActor_->ResetIsReceiveCleanStatusResponse();
    cleanStatusRequest.set_name(manager_->runtimeManagerID_);
    testActor_->Send(manager_->GetAID(), "CleanStatus", cleanStatusRequest.SerializeAsString());
    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetIsReceiveCleanStatusResponse(); });

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

/**
 * Feature: UpdateTokenTest
 * Description: update token for runtime while token is expired, request from InstanceCtrlActor
 * Steps:
 * 1. agent forward UpdateToken Request to RuntimeManager, and then RuntimeManger refresh token for runtime
 * and return UpdateTokenResponse
 */
TEST_F(DISABLED_RuntimeManagerTest, UpdateTokenTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    messages::UpdateCredRequest request;
    manager_->connected_ = false;
    testActor_->Send(manager_->GetAID(), "UpdateCred", "invalid msg#");

    manager_->executorMap_.emplace(EXECUTOR_TYPE::RUNTIME, nullptr);
    testActor_->Send(manager_->GetAID(), "UpdateCred", request.SerializeAsString());
    ASSERT_AWAIT_TRUE([=]() -> bool {
        return testActor_->GetIsReceiveUpdateTokenResponse()->code()
               == static_cast<int32_t>(RUNTIME_MANAGER_PARAMS_INVALID);
    });
    manager_->executorMap_.clear();
    testActor_->Send(manager_->GetAID(), "UpdateCred", request.SerializeAsString());
    ASSERT_AWAIT_TRUE([=]() -> bool {
        return testActor_->GetIsReceiveUpdateTokenResponse()->code() == static_cast<int32_t>(SUCCESS);
    });

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerTest, CollectCpuType)
{
    manager_->CollectCpuType();

    EXPECT_FALSE(manager_->GetCpuType().empty());
}

TEST_F(DISABLED_RuntimeManagerTest, GetCpuTypeByProc)
{
    EXPECT_FALSE(manager_->GetCpuTypeByProc().empty());
}

TEST_F(DISABLED_RuntimeManagerTest, GetCpuTypeByCommand)
{
    EXPECT_FALSE(manager_->GetCpuTypeByCommand().empty());
}

TEST_F(DISABLED_RuntimeManagerTest, EnableDebugInstanceIDTest_NotFound_Gdbserver)
{
    auto optionEnv = litebus::os::GetEnv("PATH");
    std::string env;
    if (optionEnv.IsSome()) {
        env = optionEnv.Get(); // origin env
    }

    litebus::os::SetEnv("PATH", "/tmp/");
    functionsystem::runtime_manager::Flags flags;
    const char *port = ("--port=" + std::to_string(FindAvailablePort())).c_str();
    const char *argv[] = {
        "/runtime_manager",
        "--node_id=node1",
        "--ip=127.0.0.1",
        "--host_ip=127.0.0.1",
        port,
        "--runtime_initial_port=500",
        "--port_num=2000",
        "--runtime_dir=/tmp",
        const_cast<char *>("127.0.0.1:80"),
        "--runtime_ld_library_path=/tmp",
        "--proc_metrics_cpu=2000",
        "--proc_metrics_memory=2000",
        "--runtime_instance_debug_enable=true",
        R"(--log_config={"filepath": "/tmp/home/yr/log", "level": "DEBUG", "rolling": {"maxsize": 100, "maxfiles": 1},"alsologtostderr":true})"
    };
    flags.ParseFlags(std::size(argv), argv);
    manager_->SetRegisterHelper(std::make_shared<RegisterHelper>("node1-RuntimeManagerSrv"));
    manager_->SetConfig(flags);

    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    messages::StartInstanceRequest startRequest;
    startRequest.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");
    auto posixEnvs = runtimeConfig->mutable_posixenvs();
    posixEnvs->insert({ YR_DEBUG_CONFIG, R"({"enable": "true"})" });

    testActor_->StartInstance(manager_->GetAID(), startRequest);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::RUNTIME_MANAGER_DEBUG_SERVER_NOTFOUND, response->code());
    EXPECT_EQ("Debug server components for language cpp not found.", response->message());
    EXPECT_EQ("test_requestID", response->requestid());

    auto instanceResponse = response->mutable_startruntimeinstanceresponse();
    auto resRuntimeID = instanceResponse->runtimeid();
    EXPECT_TRUE(resRuntimeID.empty());
    EXPECT_TRUE(manager_->runtimeInstanceDebugEnable_);
    EXPECT_FALSE(manager_->debugServerMgr_->actor_->languageDebugConfigs_["cpp"].isFound);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetIsReceiveStopInstanceResponse(); });

    const std::string stopRequestID = "test_requestID";
    messages::StopInstanceRequest request;
    request.set_runtimeid(resRuntimeID);
    request.set_requestid(stopRequestID);
    request.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    testActor_->StopInstance(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetStopInstanceResponse()->requestid() == stopRequestID; });
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->code(),
              static_cast<int32_t>(StatusCode::RUNTIME_MANAGER_RUNTIME_PROCESS_NOT_FOUND));
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->message(), "stop instance failed");
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->runtimeid(), resRuntimeID);

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
    litebus::os::SetEnv("PATH", env);
}

TEST_F(DISABLED_RuntimeManagerDebugServerTest, QueryDebugInstanceInfosTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    messages::QueryDebugInstanceInfosRequest request;
    request.set_requestid("request_id");

    // lost connection with function agent
    manager_->connected_ = false;
    testActor_->QueryDebugInstanceInfos(manager_->GetAID(), request);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(testActor_->isReceiveQueryDebugInstanceInfosResponse_, false);

    // resume connection
    manager_->connected_ = true;
    testActor_->QueryDebugInstanceInfos(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->isReceiveQueryDebugInstanceInfosResponse_; });
    ASSERT_AWAIT_TRUE(
        [=]() -> bool { return testActor_->GetQueryDebugInstanceResponse()->requestid() == "request_id"; });
    EXPECT_EQ(testActor_->GetQueryDebugInstanceResponse()->debuginstanceinfos_size(), 0);
    testActor_->ResetMessage();

    // stub pid data
    std::vector<pid_t> testPids;
    pid_t pid = fork();
    std::string port;
    if (pid == 0) {  // child process
        while (true) {
            sleep(2);  // keep alive
        }
        _exit(0);          // Prevents child processes from executing code outside the loop
    } else if (pid > 0) {  // parent process
        testPids.push_back(pid);
        auto runtimeID = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        manager_->debugServerMgr_->actor_->runtime2PID_[runtimeID] = pid;
        manager_->debugServerMgr_->actor_->runtime2DebugServerPID_[runtimeID] = pid;
        auto instanceID = litebus::uuid_generator::UUID::GetRandomUUID().ToString();
        manager_->debugServerMgr_->actor_->runtime2instanceID_[runtimeID] = instanceID;

        // stub port for debug server
        port = PortManager::GetInstance().RequestPort(runtimeID);
        YRLOG_DEBUG("port: {}", port);
        manager_->debugServerMgr_->actor_->hostIP_ = "127.0.0.1";
        manager_->debugServerMgr_->actor_->pid2runtimeID_[pid] = runtimeID;
        manager_->debugServerMgr_->actor_->runtime2DebugServerPort_[runtimeID] = port;
    }

    // success
    testActor_->QueryDebugInstanceInfos(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE(
        [=]() -> bool { return testActor_->GetQueryDebugInstanceResponse()->requestid() == "request_id"; });
    ASSERT_EQ(testActor_->GetQueryDebugInstanceResponse()->debuginstanceinfos_size(), 1);
    EXPECT_EQ(testActor_->GetQueryDebugInstanceResponse()->debuginstanceinfos(0).pid(), pid);
    EXPECT_EQ(testActor_->GetQueryDebugInstanceResponse()->debuginstanceinfos(0).debugserver(), "127.0.0.1:" + port);

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerDebugServerTest, DisableDebugInstanceIDTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    messages::StartInstanceRequest startRequest;
    startRequest.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("cpp");
    auto posixEnvs = runtimeConfig->mutable_posixenvs();
    posixEnvs->insert({ YR_DEBUG_CONFIG, R"({"enable": "false"})" });

    testActor_->StartInstance(manager_->GetAID(), startRequest);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::SUCCESS, response->code());
    EXPECT_EQ("start instance success", response->message());
    EXPECT_EQ("test_requestID", response->requestid());

    auto instanceResponse = response->mutable_startruntimeinstanceresponse();
    auto resRuntimeID = instanceResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());
    EXPECT_EQ(std::to_string(INITIAL_PORT), instanceResponse->port());
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetIsReceiveStopInstanceResponse(); });

    const std::string stopRequestID = "test_requestID";
    messages::StopInstanceRequest request;
    request.set_runtimeid(resRuntimeID);
    request.set_requestid(stopRequestID);
    request.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));

    testActor_->StopInstance(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetStopInstanceResponse()->requestid() == stopRequestID; });
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->message(), "stop instance success");
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->runtimeid(), resRuntimeID);

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerDebugServerTest, EnableDebugInstanceIDTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    auto startRequest = GenStartInstanceRequest();
    auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    auto posixEnvs = runtimeConfig->mutable_posixenvs();
    posixEnvs->insert({ YR_DEBUG_CONFIG, R"({"enable": "true"})" });
    testActor_->StartInstance(manager_->GetAID(), startRequest);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::SUCCESS, response->code());
    EXPECT_EQ("start instance success", response->message());
    EXPECT_EQ("test_requestID", response->requestid());

    auto instanceResponse = response->mutable_startruntimeinstanceresponse();
    auto resRuntimeID = instanceResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());
    EXPECT_EQ(std::to_string(INITIAL_PORT + 1), instanceResponse->port());
    EXPECT_TRUE(manager_->runtimeInstanceDebugEnable_);
    EXPECT_TRUE(manager_->debugServerMgr_->actor_->languageDebugConfigs_["cpp"].isFound);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetIsReceiveStopInstanceResponse(); });

    const std::string stopRequestID = "test_requestID";
    messages::StopInstanceRequest request;
    request.set_runtimeid(resRuntimeID);
    request.set_requestid(stopRequestID);
    request.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));

    testActor_->StopInstance(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetStopInstanceResponse()->requestid() == stopRequestID; });
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->message(), "stop instance success");
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->runtimeid(), resRuntimeID);

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

TEST_F(DISABLED_RuntimeManagerDebugServerTest, EnablePythonDebugInstanceIDTest)
{
    testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RuntimeManagerTestActor"));
    litebus::Spawn(testActor_, true);

    messages::StartInstanceRequest startRequest;
    startRequest.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    auto runtimeInfo = startRequest.mutable_runtimeinstanceinfo();
    runtimeInfo->set_requestid("test_requestID");
    runtimeInfo->set_instanceid("test_instanceID");
    runtimeInfo->set_traceid("test_traceID");

    auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
    runtimeConfig->set_language("python3");
    auto posixEnvs = runtimeConfig->mutable_posixenvs();
    posixEnvs->insert({ YR_DEBUG_CONFIG, R"({"enable": "true"})" });
    auto deployConfig = runtimeInfo->mutable_deploymentconfig();
    deployConfig->set_objectid("test_objectID");
    deployConfig->set_bucketid("test_bucketID");
    deployConfig->set_deploydir(testDeployDir);
    deployConfig->set_storagetype("local");

    testActor_->StartInstance(manager_->GetAID(), startRequest);

    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetStartInstanceResponse()->message().empty(); });

    auto response = testActor_->GetStartInstanceResponse();
    EXPECT_EQ(StatusCode::SUCCESS, response->code());
    EXPECT_EQ("start instance success", response->message());
    EXPECT_EQ("test_requestID", response->requestid());

    auto instanceResponse = response->mutable_startruntimeinstanceresponse();
    auto resRuntimeID = instanceResponse->runtimeid();
    EXPECT_TRUE(!resRuntimeID.empty());
    EXPECT_EQ(std::to_string(INITIAL_PORT + 1), instanceResponse->port());
    EXPECT_TRUE(manager_->runtimeInstanceDebugEnable_);
    EXPECT_TRUE(manager_->debugServerMgr_->actor_->languageDebugConfigs_["python"].isFound);
    ASSERT_AWAIT_TRUE([=]() -> bool { return !testActor_->GetIsReceiveStopInstanceResponse(); });

    const std::string stopRequestID = "test_requestID";
    messages::StopInstanceRequest request;
    request.set_runtimeid(resRuntimeID);
    request.set_requestid(stopRequestID);
    request.set_type(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));

    testActor_->StopInstance(manager_->GetAID(), request);
    ASSERT_AWAIT_TRUE([=]() -> bool { return testActor_->GetStopInstanceResponse()->requestid() == stopRequestID; });
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->message(), "stop instance success");
    EXPECT_EQ(testActor_->GetStopInstanceResponse()->runtimeid(), resRuntimeID);

    litebus::Terminate(testActor_->GetAID());
    litebus::Await(testActor_->GetAID());
}

/**
 * Feature: GetRuntimeType with SUPERVISOR executor type
 * Description: Test GetRuntimeType returns correct executor type from instance response
 */
TEST_F(DISABLED_RuntimeManagerTest, GetRuntimeType_SupervisorExecutor)
{
    // Setup instance info map
    messages::RuntimeInstanceInfo instanceInfo;
    instanceInfo.set_instanceid("test_instance_id");
    instanceInfo.set_runtimeid("test_runtime_id");
    instanceInfo.set_requestid("test_request_id");
    manager_->instanceInfoMap_["test_runtime_id"] = instanceInfo;

    // Setup instance response map with SUPERVISOR executor type
    messages::StartInstanceResponse startResponse;
    startResponse.set_requestid("test_request_id");
    startResponse.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    startResponse.mutable_startruntimeinstanceresponse()->set_runtimeid("test_runtime_id");
    startResponse.mutable_startruntimeinstanceresponse()->set_executortype(
        static_cast<int32_t>(EXECUTOR_TYPE::SUPERVISOR));
    manager_->instanceResponseMap_["test_instance_id"] = startResponse;

    // Get runtime type
    EXECUTOR_TYPE runtimeType = manager_->GetRuntimeType("test_runtime_id");

    EXPECT_EQ(runtimeType, EXECUTOR_TYPE::SUPERVISOR);
}

/**
 * Feature: GetRuntimeType with legacy sandbox/container response
 * Description: Test container runtime falls back to RuntimeInstanceInfo when executor type is absent in response
 */
TEST_F(RuntimeManagerTypeTest, GetRuntimeType_ContainerFallbackWhenResponseExecutorTypeMissing)
{
    messages::RuntimeInstanceInfo instanceInfo;
    instanceInfo.set_instanceid("test_instance_id");
    instanceInfo.set_runtimeid("test_runtime_id");
    instanceInfo.mutable_container()->set_id("test_container_id");
    manager_->instanceInfoMap_["test_runtime_id"] = instanceInfo;

    messages::StartInstanceResponse startResponse;
    startResponse.mutable_startruntimeinstanceresponse()->set_runtimeid("test_runtime_id");
    manager_->instanceResponseMap_["test_instance_id"] = startResponse;

    EXPECT_EQ(manager_->GetRuntimeType("test_runtime_id"), EXECUTOR_TYPE::SANDBOXD);
}

TEST_F(RuntimeManagerTypeTest, ResolvePauseSnapshotExecutorUsesAuthoritativeSandboxRecord)
{
    messages::RuntimeInstanceInfo instanceInfo;
    instanceInfo.set_instanceid("sandbox-instance");
    instanceInfo.set_runtimeid("sandbox-runtime");
    instanceInfo.mutable_container()->set_id("sandbox-container");
    manager_->instanceInfoMap_["sandbox-runtime"] = instanceInfo;

    messages::SnapshotRuntimeRequest request;
    request.set_runtimeid("sandbox-runtime");
    request.set_type(common::PAUSE_RESUME);
    auto executorType = EXECUTOR_TYPE::UNKNOWN;

    const auto status = manager_->ResolveSnapshotExecutorType(request, executorType);

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(executorType, EXECUTOR_TYPE::SANDBOXD);
}

TEST_F(RuntimeManagerTypeTest, ResolvePauseSnapshotExecutorRejectsProcessRuntime)
{
    messages::RuntimeInstanceInfo instanceInfo;
    instanceInfo.set_instanceid("process-instance");
    instanceInfo.set_runtimeid("process-runtime");
    manager_->instanceInfoMap_["process-runtime"] = instanceInfo;

    messages::SnapshotRuntimeRequest request;
    request.set_runtimeid("process-runtime");
    request.set_type(common::PAUSE_RESUME);
    auto executorType = EXECUTOR_TYPE::UNKNOWN;

    const auto status = manager_->ResolveSnapshotExecutorType(request, executorType);

    EXPECT_TRUE(status.IsError());
    EXPECT_EQ(status.StatusCode(), StatusCode::RUNTIME_MANAGER_PARAMS_INVALID);
    EXPECT_EQ(executorType, EXECUTOR_TYPE::UNKNOWN);
}

TEST_F(RuntimeManagerTypeTest, ResolvePauseSnapshotExecutorRejectsMissingRuntime)
{
    messages::SnapshotRuntimeRequest request;
    request.set_runtimeid("missing-runtime");
    request.set_type(common::PAUSE_RESUME);
    auto executorType = EXECUTOR_TYPE::UNKNOWN;

    const auto status = manager_->ResolveSnapshotExecutorType(request, executorType);

    EXPECT_TRUE(status.IsError());
    EXPECT_EQ(status.StatusCode(), StatusCode::RUNTIME_MANAGER_RUNTIME_PROCESS_NOT_FOUND);
    EXPECT_EQ(executorType, EXECUTOR_TYPE::UNKNOWN);
}

TEST_F(RuntimeManagerTypeTest, ResolvePauseSnapshotExecutorUsesReconciledSandboxRecord)
{
    messages::RuntimeInstanceInfo reconciledInfo;
    reconciledInfo.set_instanceid("reconciled-instance");
    reconciledInfo.set_runtimeid("reconciled-runtime");
    reconciledInfo.mutable_container()->set_id("reconciled-container");
    manager_->instanceInfoMap_["reconciled-runtime"] = reconciledInfo;

    messages::SnapshotRuntimeRequest request;
    request.set_runtimeid("reconciled-runtime");
    request.set_type(common::PAUSE_RESUME);
    auto executorType = EXECUTOR_TYPE::UNKNOWN;

    const auto status = manager_->ResolveSnapshotExecutorType(request, executorType);

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(executorType, EXECUTOR_TYPE::SANDBOXD);
}

TEST_F(RuntimeManagerTypeTest, ResolveStopExecutorTypeUsesContainerFallbackForDefaultRuntimeRequest)
{
    messages::RuntimeInstanceInfo instanceInfo;
    instanceInfo.set_instanceid("test_instance_id");
    instanceInfo.set_runtimeid("test_runtime_id");
    instanceInfo.mutable_container()->set_id("test_container_id");
    manager_->instanceInfoMap_["test_runtime_id"] = instanceInfo;

    messages::StartInstanceResponse startResponse;
    startResponse.mutable_startruntimeinstanceresponse()->set_runtimeid("test_runtime_id");
    manager_->instanceResponseMap_["test_instance_id"] = startResponse;

    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("test_runtime_id");
    request->set_executortype(static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));

    EXPECT_EQ(manager_->ResolveStopExecutorType(request), EXECUTOR_TYPE::SANDBOXD);
    EXPECT_EQ(request->executortype(), static_cast<int32_t>(EXECUTOR_TYPE::SANDBOXD));
}

/**
 * Feature: GetRuntimeType with multiple executor types
 * Description: Test GetRuntimeType with multiple runtimes of different executor types
 */
TEST_F(DISABLED_RuntimeManagerTest, GetRuntimeType_MultipleExecutorTypes)
{
    // Setup RUNTIME runtime
    messages::RuntimeInstanceInfo instanceInfo1;
    instanceInfo1.set_instanceid("instance1");
    instanceInfo1.set_runtimeid("runtime1");
    instanceInfo1.set_requestid("request1");
    manager_->instanceInfoMap_["runtime1"] = instanceInfo1;

    messages::StartInstanceResponse startResponse1;
    startResponse1.set_requestid("request1");
    startResponse1.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    startResponse1.mutable_startruntimeinstanceresponse()->set_runtimeid("runtime1");
    startResponse1.mutable_startruntimeinstanceresponse()->set_executortype(
        static_cast<int32_t>(EXECUTOR_TYPE::RUNTIME));
    manager_->instanceResponseMap_["instance1"] = startResponse1;

    // Setup SUPERVISOR runtime
    messages::RuntimeInstanceInfo instanceInfo2;
    instanceInfo2.set_instanceid("instance2");
    instanceInfo2.set_runtimeid("runtime2");
    instanceInfo2.set_requestid("request2");
    manager_->instanceInfoMap_["runtime2"] = instanceInfo2;

    messages::StartInstanceResponse startResponse2;
    startResponse2.set_requestid("request2");
    startResponse2.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    startResponse2.mutable_startruntimeinstanceresponse()->set_runtimeid("runtime2");
    startResponse2.mutable_startruntimeinstanceresponse()->set_executortype(
        static_cast<int32_t>(EXECUTOR_TYPE::SUPERVISOR));
    manager_->instanceResponseMap_["instance2"] = startResponse2;

    // Setup CONTAINER runtime
    messages::RuntimeInstanceInfo instanceInfo3;
    instanceInfo3.set_instanceid("instance3");
    instanceInfo3.set_runtimeid("runtime3");
    instanceInfo3.set_requestid("request3");
    manager_->instanceInfoMap_["runtime3"] = instanceInfo3;

    messages::StartInstanceResponse startResponse3;
    startResponse3.set_requestid("request3");
    startResponse3.set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    startResponse3.mutable_startruntimeinstanceresponse()->set_runtimeid("runtime3");
    startResponse3.mutable_startruntimeinstanceresponse()->set_executortype(
        static_cast<int32_t>(EXECUTOR_TYPE::CONTAINER));
    manager_->instanceResponseMap_["instance3"] = startResponse3;

    // Get runtime types
    EXECUTOR_TYPE type1 = manager_->GetRuntimeType("runtime1");
    EXECUTOR_TYPE type2 = manager_->GetRuntimeType("runtime2");
    EXECUTOR_TYPE type3 = manager_->GetRuntimeType("runtime3");

    EXPECT_EQ(type1, EXECUTOR_TYPE::RUNTIME);
    EXPECT_EQ(type2, EXECUTOR_TYPE::SUPERVISOR);
    EXPECT_EQ(type3, EXECUTOR_TYPE::SANDBOXD);
}

/**
 * Feature: FindExecutor caching
 * Description: Test that FindExecutor caches SUPERVISOR executor correctly
 */
TEST_F(DISABLED_RuntimeManagerTest, FindExecutor_Caching_Supervisor)
{
    // First call - creates executor
    auto executor1 = manager_->FindExecutor(EXECUTOR_TYPE::SUPERVISOR);
    EXPECT_NE(executor1, nullptr);

    // Second call - should return cached executor
    auto executor2 = manager_->FindExecutor(EXECUTOR_TYPE::SUPERVISOR);
    EXPECT_NE(executor2, nullptr);
    EXPECT_EQ(executor1, executor2);

    // Different executor type - should create new executor
    auto runtimeExecutor = manager_->FindExecutor(EXECUTOR_TYPE::RUNTIME);
    EXPECT_NE(runtimeExecutor, nullptr);
    EXPECT_NE(executor1, runtimeExecutor);
}

TEST(RuntimeRetirementFailureTest, FailedStopKeepsIdentityAndFailedFutureDoesNotReadValue)
{
    RuntimeManager manager("retirement-failure-check");
    const litebus::AID recipient("unused", "127.0.0.1:1");
    messages::RuntimeInstanceInfo info;
    info.set_instanceid("instance");
    info.set_runtimeid("runtime");
    manager.instanceInfoMap_["runtime"] = info;
    manager.instanceResponseMap_["instance"] = messages::StartInstanceResponse();
    auto request = std::make_shared<messages::StopInstanceRequest>();
    request->set_runtimeid("runtime");
    request->set_requestid("stop-attempt");
    auto response = std::make_shared<messages::StopInstanceResponse>();
    response->set_runtimeid("runtime");
    response->set_requestid(request->requestid());
    litebus::Future<Status> unavailable;
    unavailable.SetFailed(123);
    // No health/metrics/port client is bound: failure must not run cleanup.
    manager.OnGetRuntimeStatus(recipient, request, response, unavailable);
    EXPECT_NE(response->code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_NE(response->message().find("123"), std::string::npos);
    EXPECT_EQ(manager.instanceInfoMap_.count("runtime"), 1);
    EXPECT_EQ(manager.instanceResponseMap_.count("instance"), 1);
    manager.StopInstanceResponse(recipient, unavailable, request);
    EXPECT_EQ(manager.instanceInfoMap_.count("runtime"), 1);
    const litebus::Future<Status> failed(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "not exited"));
    manager.OnGetRuntimeStatus(recipient, request, response, failed);
    EXPECT_EQ(response->message(), "not exited");
    EXPECT_EQ(manager.instanceInfoMap_.count("runtime"), 1);
    response->set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    manager.SendStopInstanceResponse(recipient, "runtime", response);
    EXPECT_EQ(manager.instanceInfoMap_.count("runtime"), 0);
    EXPECT_EQ(manager.instanceResponseMap_.count("instance"), 0);
}

// ---------------------------------------------------------------------------
// Retirement-receipt send paths: real litebus sends into a spawned receiver
// actor are COUNTED (RuntimeManagerTestActor::GetStopInstanceResponseCount).
// Covers: physical receipt confirmed but the in-memory registration is gone
// (replay path still settles upstream exactly once), and the fault-gate
// receipt stop-point's timed behaviour (0 sends during the window, exactly 1
// on unconfirmed expiry, 0 after a timely in-window confirmation).
// ---------------------------------------------------------------------------
namespace {
// Points the SINGLETON fault gate at a per-test control file and disarms it
// on scope exit, so gate state never leaks between tests.
class RetireGateControlGuard {
public:
    RetireGateControlGuard()
        : path_(std::filesystem::path(testing::TempDir()) / GenerateRandomName("gate-ctrl"))
    {
        saved_ = std::getenv("AKERNEL_RETIRE_FAULT_CONTROL_FILE");
        setenv("AKERNEL_RETIRE_FAULT_CONTROL_FILE", path_.c_str(), 1);
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }
    ~RetireGateControlGuard()
    {
        // disarm while the env still points at our file, then restore
        std::ofstream out(path_, std::ios::trunc);
        out << R"({"disarm":true})";
        out.close();
        RetireFaultGate::Instance().RefreshFromControlFile();
        RetireFaultGate::Instance().RefreshFromControlFile();  // content dedup noop
        if (saved_ != nullptr) {
            setenv("AKERNEL_RETIRE_FAULT_CONTROL_FILE", saved_, 1);
        } else {
            unsetenv("AKERNEL_RETIRE_FAULT_CONTROL_FILE");
        }
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }
    void write(const std::string &content)
    {
        std::ofstream out(path_, std::ios::trunc);
        out << content;
    }
private:
    std::filesystem::path path_;
    const char *saved_ = nullptr;
};

bool WaitForStopResponses(const std::shared_ptr<RuntimeManagerTestActor> &actor,
                          uint32_t expected, int timeoutMs)
{
    const auto deadline = std::chrono::steady_clock::now()
        + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (actor->GetStopInstanceResponseCount() >= expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return actor->GetStopInstanceResponseCount() >= expected;
}

int64_t FutureUnixMs(int64_t offsetMs)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count() + offsetMs;
}

std::shared_ptr<messages::StopInstanceResponse> ConfirmedRetirementResponse(const std::string &requestID)
{
    auto response = std::make_shared<messages::StopInstanceResponse>();
    response->set_runtimeid("runtime-replay");
    response->set_requestid(requestID);
    response->set_code(static_cast<int32_t>(StatusCode::SUCCESS));
    response->set_physical_retirement_confirmed(true);
    response->set_retired_sandbox_id("sbox-rr");
    response->set_retired_resource_generation("gen-rr");
    return response;
}
}  // namespace

class RuntimeRetireReceiptTest : public ::testing::Test {
public:
    void SetUp() override
    {
        manager_ = std::make_shared<RuntimeManager>(GenerateRandomName("RuntimeRetireReceiptTest"));
        manager_->isUnitTestSituation_ = true;
        litebus::Spawn(manager_, true);
        manager_->connected_ = true;
        testActor_ = std::make_shared<RuntimeManagerTestActor>(GenerateRandomName("RetireReceiptActor"));
        litebus::Spawn(testActor_, true);
    }

    void TearDown() override
    {
        litebus::Terminate(testActor_->GetAID());
        litebus::Await(testActor_->GetAID());
        litebus::Terminate(manager_->GetAID());
        litebus::Await(manager_->GetAID());
    }

protected:
    std::shared_ptr<RuntimeManager> manager_;
    std::shared_ptr<RuntimeManagerTestActor> testActor_;
};

TEST_F(RuntimeRetireReceiptTest, PhysicalReceiptWithoutRegistrationStillSends)
{
    // The retry/durable-replay path: sandboxd confirmed physical retirement
    // but the runtime registration is already cleaned. The receipt must
    // STILL travel upstream exactly once — settling is not gated on local
    // table state.
    RetireGateControlGuard guard;
    auto response = ConfirmedRetirementResponse("fwd-replay-1");
    // deliberately NO instanceInfoMap_ entry for "runtime-replay"
    manager_->SendStopInstanceResponse(testActor_->GetAID(), "runtime-replay", response);
    EXPECT_TRUE(WaitForStopResponses(testActor_, 1, 5000));
    EXPECT_EQ(testActor_->GetStopInstanceResponseCount(), 1u);
    // the confirmed receipt arrived intact
    auto received = testActor_->GetStopInstanceResponse();
    ASSERT_NE(received, nullptr);
    EXPECT_TRUE(received->physical_retirement_confirmed());
    EXPECT_EQ(received->retired_sandbox_id(), "sbox-rr");
}

TEST_F(RuntimeRetireReceiptTest, UnconfirmedPrepareSendsOriginalExactlyOnce)
{
    RetireGateControlGuard guard;
    guard.write(R"({"action":"drop_receipt","sandbox":"sbox-rr","generation":"gen-rr",)"
                R"("prepare":true,"hold_ms":1000,"delay_ms":0,"deadline_unix_ms":)"
                + std::to_string(FutureUnixMs(120000)) + "}");
    RetireFaultGate::Instance().RefreshFromControlFile();  // arm before the send
    auto response = ConfirmedRetirementResponse("fwd-hold-1");
    manager_->SendStopInstanceResponse(testActor_->GetAID(), "runtime-replay", response);
    // during the stop-point window: exactly 0 sends
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(testActor_->GetStopInstanceResponseCount(), 0u);
    // unconfirmed expiry: the ORIGINAL receipt is sent exactly once
    EXPECT_TRUE(WaitForStopResponses(testActor_, 1, 5000));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(testActor_->GetStopInstanceResponseCount(), 1u);
    auto received = testActor_->GetStopInstanceResponse();
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->requestid(), "fwd-hold-1");  // the original message
}

TEST_F(RuntimeRetireReceiptTest, TimelyConfirmDropsOriginalAndReplaySettles)
{
    RetireGateControlGuard guard;
    guard.write(R"({"action":"drop_receipt","sandbox":"sbox-rr","generation":"gen-rr",)"
                R"("prepare":true,"hold_ms":1500,"delay_ms":0,"deadline_unix_ms":)"
                + std::to_string(FutureUnixMs(120000)) + "}");
    RetireFaultGate::Instance().RefreshFromControlFile();
    auto response = ConfirmedRetirementResponse("fwd-hold-2");
    manager_->SendStopInstanceResponse(testActor_->GetAID(), "runtime-replay", response);
    // confirm INSIDE the window via the control file (the gate's poller
    // observes it and freezes the arrival moment)
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    guard.write(R"({"action":"drop_receipt","sandbox":"sbox-rr","generation":"gen-rr",)"
                R"("operation_id":"fwd-hold-2","delay_ms":0,"deadline_unix_ms":)"
                + std::to_string(FutureUnixMs(120000)) + "}");
    // past the window: the ORIGINAL receipt was dropped — 0 sends
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    EXPECT_EQ(testActor_->GetStopInstanceResponseCount(), 0u);
    // caller retry replays the durable receipt; the gate is spent, so the
    // replay settles upstream exactly once
    auto replay = ConfirmedRetirementResponse("fwd-hold-2");
    manager_->SendStopInstanceResponse(testActor_->GetAID(), "runtime-replay", replay);
    EXPECT_TRUE(WaitForStopResponses(testActor_, 1, 5000));
    EXPECT_EQ(testActor_->GetStopInstanceResponseCount(), 1u);
}

}  // namespace functionsystem::runtime_manager
