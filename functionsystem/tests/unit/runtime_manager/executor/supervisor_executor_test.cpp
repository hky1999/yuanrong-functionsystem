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

#include "runtime_manager/executor/supervisor_executor.h"

#include <fcntl.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "common/constants/constants.h"
#include "common/proto/pb/message_pb.h"
#include "common/proto/pb/posix/sandbox_api.grpc.pb.h"
#include "common/resource_view/resource_type.h"
#include "common/status/status.h"
#include "common/utils/files.h"
#include "utils/future_test_helper.h"

namespace functionsystem::runtime_manager {

// Bring in the test helper functions for ASSERT_AWAIT_READY / ASSERT_AWAIT_SET macros
using functionsystem::test::AwaitAssertReady;
using functionsystem::test::AwaitAssertSet;

class MockSupervisorExecutor : public SupervisorExecutor {
public:
    MockSupervisorExecutor(const std::string &name, const litebus::AID &functionAgentAID)
        : SupervisorExecutor(name, functionAgentAID)
    {
    }

    // Expose protected/private members for testing
    std::string TestBuildUdsHttpRequest(const std::string &method, const std::string &path, const std::string &body)
    {
        return BuildUdsHttpRequest(method, path, body);
    }

    void TestParseResponse(litebus::Promise<nlohmann::json> promise, std::string response)
    {
        ParseResponse(promise, response);
    }

    int TestConnectUdsSocket(const std::string &socketPath)
    {
        return ConnectUdsSocket(socketPath);
    }

    nlohmann::json TestBuildCommand(const std::shared_ptr<runtime::v1::StartRequest> &start)
    {
        return BuildCommand(start);
    }

    nlohmann::json TestCreateRequest(const std::shared_ptr<messages::StartInstanceRequest> &request)
    {
        return CreateRequest(request);
    }

    messages::StartInstanceResponse TestGenSuccessStartInstanceResponse(
        const std::shared_ptr<messages::StartInstanceRequest> &request, const std::string &sandboxID,
        const std::string &sandboxIP = "")
    {
        return GenSuccessStartInstanceResponse(request, sandboxID, sandboxIP);
    }

    litebus::Future<runtime::v1::StartResponse> TestCreateSandbox(const std::string &runtimeID,
                                                                   const std::string &hostUser = "")
    {
        // CreateSandbox takes a StartInstanceRequest; build a minimal one carrying the
        // runtimeID and (optionally) HOST_USER deploy option the caller asked for.
        auto request = std::make_shared<messages::StartInstanceRequest>();
        request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::SUPERVISOR));
        auto runtimeInfo = request->mutable_runtimeinstanceinfo();
        runtimeInfo->set_runtimeid(runtimeID);
        runtimeInfo->set_requestid("test_request_id");
        runtimeInfo->set_instanceid("test_instance_id");
        runtimeInfo->set_traceid("test_trace_id");
        if (!hostUser.empty()) {
            (*runtimeInfo->mutable_deploymentconfig()->mutable_deployoptions())[HOST_USER] = hostUser;
        }
        return CreateSandbox(request);
    }

    litebus::Future<runtime::v1::StartResponse> TestExecInSandbox(
        const std::string &runtimeID, const std::shared_ptr<runtime::v1::StartRequest> &start,
        const std::string &sandboxId)
    {
        return ExecInSandbox(runtimeID, start, sandboxId);
    }

    litebus::Future<runtime::v1::DeleteResponse> TestDoDeleteSandbox(
        const std::shared_ptr<runtime::v1::DeleteRequest> &req)
    {
        return DoDeleteSandbox(req);
    }

    litebus::Future<runtime::v1::StartResponse> TestStartByRuntimeID(
        const std::shared_ptr<messages::StartInstanceRequest> &request,
        const std::map<std::string, std::string> &startRuntimeParams, const std::vector<std::string> &buildArgs,
        const Envs &envs)
    {
        return StartByRuntimeID(request, startRuntimeParams, buildArgs, envs);
    }

    litebus::Future<messages::StartInstanceResponse> TestStartRuntime(
        const std::shared_ptr<messages::StartInstanceRequest> &request, const std::string &language, const Envs &envs,
        const std::vector<std::string> &args)
    {
        return StartRuntime(request, language, envs, args);
    }

    bool TestIsRuntimeActive(const std::string &runtimeID)
    {
        return IsRuntimeActive(runtimeID);
    }

    std::map<std::string, messages::RuntimeInstanceInfo> TestGetRuntimeInstanceInfos()
    {
        return GetRuntimeInstanceInfos();
    }

    // Set internal state for testing
    void SetRuntimeToSandboxID(const std::string &runtimeID, const std::string &sandboxID)
    {
        runtime2sandboxID_[runtimeID] = sandboxID;
    }

    void SetRuntime2PortMapping(const std::string &runtimeID, const std::string &port)
    {
        runtime2portMappings_[runtimeID] = port;
    }

    size_t GetRuntimeToSandboxIDMapSize() const
    {
        return runtime2sandboxID_.size();
    }

    std::string GetSandboxIDByRuntimeID(const std::string &runtimeID) const
    {
        auto it = runtime2sandboxID_.find(runtimeID);
        return (it != runtime2sandboxID_.end()) ? it->second : "";
    }

    void ClearRuntimeToSandboxIDMap()
    {
        runtime2sandboxID_.clear();
    }
};

class SupervisorExecutorTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        executor_ = std::make_shared<MockSupervisorExecutor>("TestSupervisorExecutor",
                                                             litebus::AID("FunctionAgent", "127.0.0.1:8080"));

        // Create test deploy directory
        testDeployDir_ = "/tmp/test-supervisor-executor";
        litebus::os::Mkdir(testDeployDir_);
        litebus::os::Mkdir(testDeployDir_ + "/layer/func");
        TouchFile(testDeployDir_ + "/layer/func/funcObj");

        // Initialize config
        RuntimeConfig config;
        config.runtimePath = "/tmp";
        config.runtimeLogPath = "/tmp/logs";
        config.runtimeStdLogDir = "std";
        executor_->config_ = config;
    }

    void TearDown() override
    {
        litebus::os::Rmdir(testDeployDir_);
    }

    std::shared_ptr<messages::StartInstanceRequest> GenStartInstanceRequest(const std::string &language = "python3",
                                                                            const std::string &execPath = "")
    {
        auto request = std::make_shared<messages::StartInstanceRequest>();
        request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::SUPERVISOR));

        auto runtimeInfo = request->mutable_runtimeinstanceinfo();
        runtimeInfo->set_requestid("test_request_id");
        runtimeInfo->set_instanceid("test_instance_id");
        runtimeInfo->set_traceid("test_trace_id");
        runtimeInfo->set_runtimeid("test_runtime_id");

        auto runtimeConfig = runtimeInfo->mutable_runtimeconfig();
        runtimeConfig->set_language(language);

        auto deployConfig = runtimeInfo->mutable_deploymentconfig();
        deployConfig->set_objectid("test_object_id");
        deployConfig->set_bucketid("test_bucket_id");
        deployConfig->set_deploydir(testDeployDir_);
        deployConfig->set_storagetype("local");
        if (!execPath.empty()) {
            (*deployConfig->mutable_deployoptions())[EXEC_PATH] = execPath;
        }

        auto containerInfo = runtimeInfo->mutable_container();
        containerInfo->set_mountpoint("/opt/func");

        return request;
    }

    // Set a scalar resource (e.g. "CPU"/"Memory") with value/limit on the request's
    // runtimeconfig.resources map. Matches how SetRuntimeConfig populates resources
    // from schedulingops in production (struct_transfer.h:SetInstanceInfoResources).
    void SetRuntimeResource(const std::shared_ptr<messages::StartInstanceRequest> &request,
                            const std::string &name, double value, double limit = 0)
    {
        auto resource = request->mutable_runtimeinstanceinfo()
                            ->mutable_runtimeconfig()
                            ->mutable_resources()
                            ->mutable_resources();
        resource_view::Resource r;
        r.set_name(name);
        r.set_type(resource_view::ValueType::Value_Type_SCALAR);
        r.mutable_scalar()->set_value(value);
        r.mutable_scalar()->set_limit(limit);
        (*resource)[name] = std::move(r);
    }

    std::shared_ptr<messages::StopInstanceRequest> GenStopInstanceRequest(
        const std::string &runtimeID = "test_runtime_id")
    {
        auto request = std::make_shared<messages::StopInstanceRequest>();
        request->set_runtimeid(runtimeID);
        request->set_requestid("test_stop_request_id");
        request->set_traceid("test_stop_trace_id");
        request->set_type(static_cast<int32_t>(EXECUTOR_TYPE::SUPERVISOR));
        request->set_executortype(static_cast<int32_t>(EXECUTOR_TYPE::SUPERVISOR));

        return request;
    }

    std::shared_ptr<messages::SnapshotRuntimeRequest> GenSnapshotRuntimeRequest()
    {
        auto request = std::make_shared<messages::SnapshotRuntimeRequest>();
        request->set_requestid("test_snapshot_request_id");
        request->set_runtimeid("test_runtime_id");
        request->set_instanceid("test_instance_id");

        return request;
    }

    std::shared_ptr<messages::UpdateCredRequest> GenUpdateCredRequest()
    {
        auto request = std::make_shared<messages::UpdateCredRequest>();
        request->set_requestid("test_update_cred_request_id");
        request->set_runtimeid("test_runtime_id");
        request->set_token("test_token");
        request->set_salt("test_salt");

        return request;
    }

    std::string testDeployDir_;
    std::shared_ptr<MockSupervisorExecutor> executor_;
};

/**
 * Feature: ParseResponse
 * Description: Test parsing HTTP responses from supervisor
 */
TEST_F(SupervisorExecutorTest, ParseResponse_ValidResponse)
{
    litebus::Promise<nlohmann::json> promise;
    litebus::Future<nlohmann::json> future = promise.GetFuture();

    std::string response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 20\r\n"
        "\r\n"
        "{\"id\":\"sandbox123\"}";

    executor_->TestParseResponse(promise, response);

    ASSERT_AWAIT_READY(future);
    auto json = future.Get();
    EXPECT_EQ(json["id"], "sandbox123");
}

TEST_F(SupervisorExecutorTest, ParseResponse_NoHeaderSeparator)
{
    litebus::Promise<nlohmann::json> promise;
    litebus::Future<nlohmann::json> future = promise.GetFuture();

    std::string response = "HTTP/1.1 200 OK";

    executor_->TestParseResponse(promise, response);

    // No "\r\n\r\n" separator -> ParseResponse rejects the response and fails the promise.
    ASSERT_AWAIT_SET(future);
    EXPECT_TRUE(future.IsError());
    EXPECT_EQ(future.GetErrorCode(), static_cast<int>(StatusCode::ERR_INNER_COMMUNICATION));
}

TEST_F(SupervisorExecutorTest, ParseResponse_EmptyBody)
{
    litebus::Promise<nlohmann::json> promise;
    litebus::Future<nlohmann::json> future = promise.GetFuture();

    std::string response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 0\r\n"
        "\r\n";

    executor_->TestParseResponse(promise, response);

    // Empty body -> ParseResponse fails the promise with ERR_INNER_COMMUNICATION.
    ASSERT_AWAIT_SET(future);
    EXPECT_TRUE(future.IsError());
    EXPECT_EQ(future.GetErrorCode(), static_cast<int>(StatusCode::ERR_INNER_COMMUNICATION));
}

TEST_F(SupervisorExecutorTest, ParseResponse_InvalidJson)
{
    litebus::Promise<nlohmann::json> promise;
    litebus::Future<nlohmann::json> future = promise.GetFuture();

    std::string response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 10\r\n"
        "\r\n"
        "{invalid}";

    executor_->TestParseResponse(promise, response);

    // Unparseable JSON body -> ParseResponse catches the exception and fails the promise.
    ASSERT_AWAIT_SET(future);
    EXPECT_TRUE(future.IsError());
    EXPECT_EQ(future.GetErrorCode(), static_cast<int>(StatusCode::ERR_INNER_COMMUNICATION));
}

/**
 * Feature: ConnectUdsSocket
 * Description: Test UDS socket connection with various scenarios
 */
TEST_F(SupervisorExecutorTest, ConnectUdsSocket_InvalidPath)
{
    int fd = executor_->TestConnectUdsSocket("/nonexistent/socket/path");
    EXPECT_EQ(fd, -1);
}

TEST_F(SupervisorExecutorTest, ConnectUdsSocket_PathTooLong)
{
    std::string longPath(500, 'a');  // Exceeds sockaddr_un sun_path length
    int fd = executor_->TestConnectUdsSocket(longPath);
    EXPECT_EQ(fd, -1);
}

TEST_F(SupervisorExecutorTest, ConnectUdsSocket_NoServer)
{
    std::string socketPath = "/tmp/test_supervisor_socket_" + std::to_string(getpid());
    int fd = executor_->TestConnectUdsSocket(socketPath);
    EXPECT_EQ(fd, -1);
}

/**
 * Feature: GenSuccessStartInstanceResponse
 * Description: Test generating success response for start instance
 */
TEST_F(SupervisorExecutorTest, GenSuccessStartInstanceResponse)
{
    auto request = GenStartInstanceRequest();
    std::string sandboxID = "sandbox123";
    executor_->SetRuntime2PortMapping("test_runtime_id", "8080");

    auto response = executor_->TestGenSuccessStartInstanceResponse(request, sandboxID);

    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(response.message(), "start instance success");
    EXPECT_EQ(response.requestid(), "test_request_id");
    EXPECT_EQ(response.startruntimeinstanceresponse().runtimeid(), "test_runtime_id");
    EXPECT_EQ(response.startruntimeinstanceresponse().containerid(), sandboxID);
    EXPECT_EQ(response.startruntimeinstanceresponse().pid(), 0);
    EXPECT_EQ(response.startruntimeinstanceresponse().port(), "8080");
    EXPECT_EQ(response.startruntimeinstanceresponse().executortype(), static_cast<int32_t>(EXECUTOR_TYPE::SUPERVISOR));
}

TEST_F(SupervisorExecutorTest, GenSuccessStartInstanceResponse_NoPortMapping)
{
    auto request = GenStartInstanceRequest();
    std::string sandboxID = "sandbox123";

    auto response = executor_->TestGenSuccessStartInstanceResponse(request, sandboxID);

    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(response.startruntimeinstanceresponse().port(), "");
}

/**
 * Feature: IsRuntimeActive
 * Description: Test checking if runtime is active
 */
TEST_F(SupervisorExecutorTest, IsRuntimeActive_RuntimeExists)
{
    std::string runtimeID = "active_runtime_id";
    executor_->SetRuntimeToSandboxID(runtimeID, "sandbox123");

    bool isActive = executor_->TestIsRuntimeActive(runtimeID);
    EXPECT_TRUE(isActive);
}

TEST_F(SupervisorExecutorTest, IsRuntimeActive_RuntimeNotExists)
{
    std::string runtimeID = "inactive_runtime_id";

    bool isActive = executor_->TestIsRuntimeActive(runtimeID);
    EXPECT_FALSE(isActive);
}

/**
 * Feature: GetRuntimeInstanceInfos
 * Description: Test getting runtime instance infos
 */
TEST_F(SupervisorExecutorTest, GetRuntimeInstanceInfos_Empty)
{
    auto infos = executor_->TestGetRuntimeInstanceInfos();
    EXPECT_TRUE(infos.empty());
}

TEST_F(SupervisorExecutorTest, GetRuntimeInstanceInfos_WithInstances)
{
    messages::RuntimeInstanceInfo info1;
    info1.set_runtimeid("runtime1");
    info1.set_instanceid("instance1");

    messages::RuntimeInstanceInfo info2;
    info2.set_runtimeid("runtime2");
    info2.set_instanceid("instance2");

    auto infos = executor_->TestGetRuntimeInstanceInfos();
    EXPECT_TRUE(infos.empty());  // Empty initially as we're using mock
}

/**
 * Feature: SnapshotRuntime
 * Description: Test snapshot runtime (should be unsupported)
 */
TEST_F(SupervisorExecutorTest, SnapshotRuntime_Unsupported)
{
    auto request = GenSnapshotRuntimeRequest();

    auto responseFuture = executor_->SnapshotRuntime(request);

    ASSERT_AWAIT_READY(responseFuture);
    auto response = responseFuture.Get();
    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::GRPC_UNIMPLEMENTED));
    EXPECT_EQ(response.message(), "Snapshot is not supported for process-based runtime");
    EXPECT_EQ(response.requestid(), "test_snapshot_request_id");
}

/**
 * Feature: UpdateCredForRuntime
 * Description: Test updating credentials for runtime
 */
TEST_F(SupervisorExecutorTest, UpdateCredForRuntime)
{
    auto request = GenUpdateCredRequest();

    auto responseFuture = executor_->UpdateCredForRuntime(request);

    ASSERT_AWAIT_READY(responseFuture);
    auto response = responseFuture.Get();
    EXPECT_EQ(response.code(), 0);
    EXPECT_EQ(response.message(), "update credentials success");
    EXPECT_EQ(response.requestid(), "test_update_cred_request_id");
}

/**
 * Feature: NotifyInstancesDiskUsageExceedLimit
 * Description: Test notifying instances about disk usage exceeding limit
 */
TEST_F(SupervisorExecutorTest, NotifyInstancesDiskUsageExceedLimit)
{
    std::string description = "Disk usage exceeded 80%";
    int limit = 80;

    auto statusFuture = executor_->NotifyInstancesDiskUsageExceedLimit(description, limit);

    ASSERT_AWAIT_READY(statusFuture);
    auto status = statusFuture.Get();
    EXPECT_EQ(status.StatusCode(), StatusCode::SUCCESS);
}

/**
 * Feature: StopInstance
 * Description: Test stopping instance with various scenarios
 */
TEST_F(SupervisorExecutorTest, StopInstance_Success)
{
    std::string runtimeID = "test_runtime_id_success";
    std::string sandboxID = "sandbox_success_123";
    executor_->SetRuntimeToSandboxID(runtimeID, sandboxID);

    auto request = GenStopInstanceRequest(runtimeID);

    auto statusFuture = executor_->StopInstance(request, false);

    ASSERT_AWAIT_READY(statusFuture);
    auto status = statusFuture.Get();
    if (status.IsOk()) {
        EXPECT_EQ(status.StatusCode(), StatusCode::SUCCESS);
    } else {
        EXPECT_EQ(status.StatusCode(), StatusCode::ERR_INNER_COMMUNICATION);
    }
    EXPECT_EQ(executor_->GetSandboxIDByRuntimeID(runtimeID), "");
}

TEST_F(SupervisorExecutorTest, StopInstance_RuntimeNotExists)
{
    auto request = GenStopInstanceRequest("nonexistent_runtime_id");

    auto statusFuture = executor_->StopInstance(request, false);

    ASSERT_AWAIT_READY(statusFuture);
    auto status = statusFuture.Get();
    EXPECT_EQ(status.StatusCode(), StatusCode::SUCCESS);
}

/**
 * Feature: StartInstance with different languages
 * Description: Test start instance with different runtime languages
 */
TEST_F(SupervisorExecutorTest, StartInstance_PythonLanguage)
{
    auto request = GenStartInstanceRequest("python3");
    std::vector<int> cardIDs;

    // This will fail without proper sandbox mocking, but we test the path
    auto responseFuture = executor_->StartInstance(request, cardIDs);
    // In unit test environment, this may not fully succeed due to dependencies
}

TEST_F(SupervisorExecutorTest, StartInstance_CppLanguage)
{
    auto request = GenStartInstanceRequest("cpp");
    std::vector<int> cardIDs;

    auto responseFuture = executor_->StartInstance(request, cardIDs);
    // In unit test environment, this may not fully succeed due to dependencies
}

TEST_F(SupervisorExecutorTest, StartInstance_JavaLanguage)
{
    auto request = GenStartInstanceRequest("java");
    std::vector<int> cardIDs;

    auto responseFuture = executor_->StartInstance(request, cardIDs);
    // In unit test environment, this may not fully succeed due to dependencies
}

/**
 * Feature: Constructor
 * Description: Test supervisor executor constructor
 */
TEST_F(SupervisorExecutorTest, Constructor)
{
    litebus::AID aid("FunctionAgent", "127.0.0.1:8080");
    auto executor = std::make_shared<MockSupervisorExecutor>("TestExecutor", aid);

    EXPECT_EQ(executor->GetAID().Name(), "TestExecutor");
}

/**
 * Feature: GenSuccessStartInstanceResponse with empty sandbox ID
 * Description: Test generating success response with empty sandbox ID
 */
TEST_F(SupervisorExecutorTest, GenSuccessStartInstanceResponse_EmptySandboxID)
{
    auto request = GenStartInstanceRequest();
    std::string sandboxID = "";

    auto response = executor_->TestGenSuccessStartInstanceResponse(request, sandboxID);

    EXPECT_EQ(response.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(response.startruntimeinstanceresponse().containerid(), "");
}

/**
 * Feature: ParseResponse with large body
 * Description: Test parsing response with large body
 */
TEST_F(SupervisorExecutorTest, ParseResponse_LargeBody)
{
    litebus::Promise<nlohmann::json> promise;
    litebus::Future<nlohmann::json> future = promise.GetFuture();

    // Create a large JSON body
    std::string largeJson = R"({"data":[)";
    for (int i = 0; i < 100; ++i) {
        largeJson += R"({"item":"value)" + std::to_string(i) + R"("},)";
    }
    largeJson.pop_back();  // Remove trailing comma
    largeJson += "]}";

    std::string response =
        "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(largeJson.length()) + "\r\n\r\n" + largeJson;

    executor_->TestParseResponse(promise, response);

    ASSERT_AWAIT_READY(future);
    auto json = future.Get();
    EXPECT_TRUE(json.contains("data"));
    EXPECT_EQ(json["data"].size(), 100);
}

/**
 * Feature: IsRuntimeActive with empty runtime ID
 * Description: Test checking if runtime is active with empty runtime ID
 */
TEST_F(SupervisorExecutorTest, IsRuntimeActive_EmptyRuntimeID)
{
    std::string runtimeID = "";

    bool isActive = executor_->TestIsRuntimeActive(runtimeID);
    EXPECT_FALSE(isActive);
}

/**
 * Feature: BuildUdsHttpRequest with special characters in body
 * Description: Test building UDS HTTP request with special characters in body
 */
TEST_F(SupervisorExecutorTest, BuildUdsHttpRequest_SpecialChars)
{
    std::string method = "POST";
    std::string path = "/api/v1/sandboxes";
    std::string body = R"({"key":"value with spaces & special chars !@#$%"})";

    std::string request = executor_->TestBuildUdsHttpRequest(method, path, body);

    EXPECT_THAT(request, testing::HasSubstr("POST /api/v1/sandboxes HTTP/1.1"));
    EXPECT_THAT(request, testing::HasSubstr("Content-Length: " + std::to_string(body.length())));
    EXPECT_THAT(request, testing::HasSubstr(body));
}

/**
 * Feature: GenSuccessStartInstanceResponse with multiple runtimes
 * Description: Test generating success response with port mappings for multiple runtimes
 */
TEST_F(SupervisorExecutorTest, GenSuccessStartInstanceResponse_MultipleRuntimes)
{
    auto request1 = GenStartInstanceRequest();
    request1->mutable_runtimeinstanceinfo()->set_runtimeid("runtime1");
    executor_->SetRuntime2PortMapping("runtime1", "8080");

    auto request2 = GenStartInstanceRequest();
    request2->mutable_runtimeinstanceinfo()->set_runtimeid("runtime2");
    request2->mutable_runtimeinstanceinfo()->set_requestid("test_request_id2");
    request2->mutable_runtimeinstanceinfo()->set_instanceid("test_instance_id2");
    executor_->SetRuntime2PortMapping("runtime2", "8081");

    auto response1 = executor_->TestGenSuccessStartInstanceResponse(request1, "sandbox1");
    auto response2 = executor_->TestGenSuccessStartInstanceResponse(request2, "sandbox2");

    EXPECT_EQ(response1.startruntimeinstanceresponse().runtimeid(), "runtime1");
    EXPECT_EQ(response1.startruntimeinstanceresponse().port(), "8080");
    EXPECT_EQ(response2.startruntimeinstanceresponse().runtimeid(), "runtime2");
    EXPECT_EQ(response2.startruntimeinstanceresponse().port(), "8081");
}

/**
 * Feature: ParseResponse with multiple headers
 * Description: Test parsing response with multiple headers
 */
TEST_F(SupervisorExecutorTest, ParseResponse_MultipleHeaders)
{
    litebus::Promise<nlohmann::json> promise;
    litebus::Future<nlohmann::json> future = promise.GetFuture();

    std::string response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 20\r\n"
        "Connection: close\r\n"
        "Server: Supervisor/1.0\r\n"
        "\r\n"
        "{\"id\":\"sandbox123\"}";

    executor_->TestParseResponse(promise, response);

    ASSERT_AWAIT_READY(future);
    auto json = future.Get();
    EXPECT_EQ(json["id"], "sandbox123");
}

/**
 * Feature: StopInstance with oomKilled flag
 * Description: Test stopping instance with oomKilled flag
 */
TEST_F(SupervisorExecutorTest, StopInstance_OomKilled)
{
    std::string runtimeID = "test_runtime_id_oom_killed";
    std::string sandboxID = "sandbox_oom_123";
    executor_->SetRuntimeToSandboxID(runtimeID, sandboxID);

    auto request = GenStopInstanceRequest(runtimeID);

    auto statusFuture = executor_->StopInstance(request, true);

    // Test with oomKilled flag set to true
    // StopInstance resolves to a Status value; without a supervisor the delete fails and
    // the Status carries ERR_INNER_COMMUNICATION while still cleaning up local mappings.
    ASSERT_AWAIT_READY(statusFuture);
    auto status = statusFuture.Get();
    if (status.IsOk()) {
        EXPECT_EQ(status.StatusCode(), StatusCode::SUCCESS);
    } else {
        EXPECT_EQ(status.StatusCode(), StatusCode::ERR_INNER_COMMUNICATION);
    }
    EXPECT_EQ(executor_->GetSandboxIDByRuntimeID(runtimeID), "");

    // Note: The oomKilled flag is handled internally by StopInstance
}

/**
 * Feature: UpdateCredForRuntime with empty credentials
 * Description: Test updating credentials with empty values
 */
TEST_F(SupervisorExecutorTest, UpdateCredForRuntime_EmptyCredentials)
{
    auto request = std::make_shared<messages::UpdateCredRequest>();
    request->set_requestid("test_request_id");
    request->set_runtimeid("test_runtime_id");
    request->set_token("");
    request->set_salt("");

    auto responseFuture = executor_->UpdateCredForRuntime(request);

    ASSERT_AWAIT_READY(responseFuture);
    auto response = responseFuture.Get();
    EXPECT_EQ(response.code(), 0);
    EXPECT_EQ(response.message(), "update credentials success");
}

/**
 * Feature: NotifyInstancesDiskUsageExceedLimit with zero limit
 * Description: Test notifying with zero disk usage limit
 */
TEST_F(SupervisorExecutorTest, NotifyInstancesDiskUsageExceedLimit_ZeroLimit)
{
    std::string description = "Disk usage at 0%";
    int limit = 0;

    auto statusFuture = executor_->NotifyInstancesDiskUsageExceedLimit(description, limit);

    ASSERT_AWAIT_READY(statusFuture);
    auto status = statusFuture.Get();
    EXPECT_EQ(status.StatusCode(), StatusCode::SUCCESS);
}

/**
 * Feature: IsRuntimeActive after adding and removing runtime
 * Description: Test runtime active status after adding and removing
 */
TEST_F(SupervisorExecutorTest, IsRuntimeActive_AddAndRemove)
{
    std::string runtimeID = "test_runtime_id";

    // Initially not active
    EXPECT_FALSE(executor_->TestIsRuntimeActive(runtimeID));

    // Add runtime
    executor_->SetRuntimeToSandboxID(runtimeID, "sandbox123");
    EXPECT_TRUE(executor_->TestIsRuntimeActive(runtimeID));

    // Remove runtime by clearing the map
    executor_->SetRuntimeToSandboxID(runtimeID, "");  // Simulate removal
    // Note: In real implementation, we'd need to actually remove from the map
}

/**
 * Feature: StopAllSandboxes
 * Description: Test stopping all sandboxes with various scenarios
 */
TEST_F(SupervisorExecutorTest, StopAllSandboxes_NoSandboxes)
{
    // No sandboxes registered
    EXPECT_EQ(executor_->GetRuntimeToSandboxIDMapSize(), 0);

    auto resultFuture = executor_->StopAllSandboxes();

    ASSERT_AWAIT_READY(resultFuture);
    auto result = resultFuture.Get();
    EXPECT_TRUE(result);
    EXPECT_EQ(executor_->GetRuntimeToSandboxIDMapSize(), 0);
}

TEST_F(SupervisorExecutorTest, StopAllSandboxes_StateTransition)
{
    // Test state transition from multiple sandboxes to empty
    executor_->SetRuntimeToSandboxID("runtime1", "sandbox1");
    executor_->SetRuntimeToSandboxID("runtime2", "sandbox2");

    EXPECT_EQ(executor_->GetRuntimeToSandboxIDMapSize(), 2);
    EXPECT_TRUE(executor_->TestIsRuntimeActive("runtime1"));
    EXPECT_TRUE(executor_->TestIsRuntimeActive("runtime2"));

    auto resultFuture = executor_->StopAllSandboxes();
    ASSERT_AWAIT_READY(resultFuture);

    // Verify state changed
    // Note: In real scenario with successful deletions, map would be cleared
}

TEST_F(SupervisorExecutorTest, StopAllSandboxes_EmptyRuntimeID)
{
    // Test with empty runtime ID (edge case)
    executor_->SetRuntimeToSandboxID("", "sandbox_empty");
    executor_->SetRuntimeToSandboxID("runtime1", "sandbox1");

    EXPECT_EQ(executor_->GetRuntimeToSandboxIDMapSize(), 2);

    auto resultFuture = executor_->StopAllSandboxes();
    ASSERT_AWAIT_READY(resultFuture);
}

TEST_F(SupervisorExecutorTest, StopAllSandboxes_EmptySandboxID)
{
    // Test with empty sandbox ID (edge case)
    executor_->SetRuntimeToSandboxID("runtime1", "");
    executor_->SetRuntimeToSandboxID("runtime2", "sandbox2");

    EXPECT_EQ(executor_->GetRuntimeToSandboxIDMapSize(), 2);

    auto resultFuture = executor_->StopAllSandboxes();
    ASSERT_AWAIT_READY(resultFuture);
}

TEST_F(SupervisorExecutorTest, StopAllSandboxes_AfterStopAll)
{
    // Test calling StopAllSandboxes multiple times
    executor_->SetRuntimeToSandboxID("runtime1", "sandbox1");
    executor_->SetRuntimeToSandboxID("runtime2", "sandbox2");

    // First call
    auto resultFuture1 = executor_->StopAllSandboxes();
    ASSERT_AWAIT_READY(resultFuture1);

    // Clear map to simulate successful stop
    executor_->ClearRuntimeToSandboxIDMap();

    // Second call with empty map
    auto resultFuture2 = executor_->StopAllSandboxes();
    ASSERT_AWAIT_READY(resultFuture2);
    auto result2 = resultFuture2.Get();
    EXPECT_TRUE(result2);
    EXPECT_EQ(executor_->GetRuntimeToSandboxIDMapSize(), 0);
}

/**
 * Feature: CreateSandbox
 * Description: Test CreateSandbox failure path - UDS socket unavailable, future should be error
 */
TEST_F(SupervisorExecutorTest, CreateSandbox_FailsWhenSupervisorUnavailable)
{
    std::string runtimeID = "test_cs_runtime_id";
    auto future = executor_->TestCreateSandbox(runtimeID, "host_user");

    ASSERT_AWAIT_READY_FOR(future, TEST_AWAIT_TIMEOUT);
    auto rsp = future.Get();
    // CreateSandbox resolves failures as a value (code != SUCCESS), not SetFailed — mirrors ExecInSandbox
    EXPECT_EQ(rsp.code(), static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION));
    // transport failure (no supervisor): generic message, error_message unavailable
    EXPECT_EQ(rsp.message(), "Failed to create sandbox");
    // No sandbox should be registered on failure
    EXPECT_FALSE(executor_->TestIsRuntimeActive(runtimeID));
}

TEST_F(SupervisorExecutorTest, CreateSandbox_FailsWithEmptyHostUser)
{
    std::string runtimeID = "test_cs_empty_host";
    auto future = executor_->TestCreateSandbox(runtimeID, "");

    ASSERT_AWAIT_READY_FOR(future, TEST_AWAIT_TIMEOUT);
    auto rsp = future.Get();
    EXPECT_EQ(rsp.code(), static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION));
}

/**
 * Feature: DoDeleteSandbox
 * Description: Test DoDeleteSandbox failure path - UDS unavailable, error should propagate via SetFailed
 */
TEST_F(SupervisorExecutorTest, DoDeleteSandbox_FailsWhenSupervisorUnavailable)
{
    auto req = std::make_shared<runtime::v1::DeleteRequest>();
    req->set_id("sandbox_to_delete");

    auto future = executor_->TestDoDeleteSandbox(req);

    ASSERT_AWAIT_SET_FOR(future, TEST_AWAIT_TIMEOUT);
    EXPECT_TRUE(future.IsError());
    EXPECT_EQ(future.GetErrorCode(), static_cast<int>(StatusCode::ERR_INNER_COMMUNICATION));
}

/**
 * Feature: ExecInSandbox
 * Description: Test ExecInSandbox failure path - SendRequestToSupervisor fails, should return
 *              StartResponse with ERR_INNER_COMMUNICATION code and trigger sandbox cleanup
 */
TEST_F(SupervisorExecutorTest, ExecInSandbox_FailsAndTriggersCleanup)
{
    std::string runtimeID = "test_exec_runtime_id";
    std::string sandboxId = "sandbox_exec_failure";

    // Register sandbox so cleanup can be observed via map state
    executor_->SetRuntimeToSandboxID(runtimeID, sandboxId);

    auto start = std::make_shared<runtime::v1::StartRequest>();
    start->add_command("python3");
    start->add_command("-c");
    start->add_command("print('hello')");
    start->mutable_envs()->insert({ "KEY", "VALUE" });

    auto future = executor_->TestExecInSandbox(runtimeID, start, sandboxId);

    ASSERT_AWAIT_READY_FOR(future, TEST_AWAIT_TIMEOUT);
    auto rsp = future.Get();
    // ExecInSandbox swallows future-level error and turns it into failure code
    EXPECT_EQ(rsp.code(), static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION));
    EXPECT_EQ(rsp.message(), "Failed to execute command in sandbox");
    EXPECT_EQ(rsp.id(), "");
    // Cleanup must drop the local runtime->sandbox mapping so the runtime is no longer active
    EXPECT_EQ(executor_->GetSandboxIDByRuntimeID(runtimeID), "");
    EXPECT_FALSE(executor_->TestIsRuntimeActive(runtimeID));
}

/**
 * Feature: StartByRuntimeID
 * Description: Test StartByRuntimeID illegal command path - returns StartResponse with ERR_PARAM_INVALID
 */
TEST_F(SupervisorExecutorTest, StartByRuntimeID_RejectsIllegalCommandChars)
{
    auto request = GenStartInstanceRequest("python3");
    std::map<std::string, std::string> startParams = { { PARAM_EXEC_PATH, "/bin/echo$()" },
                                                       { PARAM_LANGUAGE, "python3" } };
    std::vector<std::string> buildArgs = { "/bin/echo$()", "arg" };
    Envs envs;

    auto future = executor_->TestStartByRuntimeID(request, startParams, buildArgs, envs);

    ASSERT_AWAIT_READY_FOR(future, TEST_AWAIT_TIMEOUT);
    auto rsp = future.Get();
    EXPECT_EQ(rsp.code(), static_cast<int32_t>(StatusCode::ERR_PARAM_INVALID));
    // Message should contain the offending command
    EXPECT_THAT(rsp.message(), testing::HasSubstr("/bin/echo$()"));
}

/**
 * Feature: StartByRuntimeID
 * Description: Test StartByRuntimeID failure path when CreateSandbox fails - should SetValue
 *              a StartResponse with ERR_INNER_COMMUNICATION and "Failed to create sandbox" message
 */
TEST_F(SupervisorExecutorTest, StartByRuntimeID_ReturnsFailureResponseWhenCreateSandboxFails)
{
    auto request = GenStartInstanceRequest("python3");
    std::map<std::string, std::string> startParams = { { PARAM_EXEC_PATH, "/usr/bin/python3" },
                                                       { PARAM_LANGUAGE, "python3" } };
    std::vector<std::string> buildArgs = { "/usr/bin/python3" };
    Envs envs;

    auto future = executor_->TestStartByRuntimeID(request, startParams, buildArgs, envs);

    ASSERT_AWAIT_READY_FOR(future, TEST_AWAIT_TIMEOUT);
    auto rsp = future.Get();
    // CreateSandbox fails (no supervisor) -> returns failure StartResponse (code != SUCCESS),
    // StartByRuntimeID propagates message into its own failure StartResponse
    EXPECT_EQ(rsp.code(), static_cast<int32_t>(StatusCode::ERR_INNER_COMMUNICATION));
    EXPECT_EQ(rsp.message(), "Failed to create sandbox");
}

/**
 * Feature: StartRuntime
 * Description: Test StartRuntime failure path - when underlying StartByRuntimeID returns a failure
 *              code, StartRuntime should produce a failed StartInstanceResponse with
 *              RUNTIME_MANAGER_CREATE_EXEC_FAILED and SUPERVISOR executor type
 */
TEST_F(SupervisorExecutorTest, StartRuntime_PropagatesFailureResponseWithCreateExecFailed)
{
    auto request = GenStartInstanceRequest("python3");
    Envs envs;
    std::vector<std::string> args = { "/usr/bin/python3" };

    auto future = executor_->TestStartRuntime(request, "python3", envs, args);

    ASSERT_AWAIT_READY_FOR(future, TEST_AWAIT_TIMEOUT);
    auto rsp = future.Get();
    // code != SUCCESS branch -> GenFailStartInstanceResponse(RUNTIME_MANAGER_CREATE_EXEC_FAILED, ...)
    EXPECT_NE(rsp.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(rsp.startruntimeinstanceresponse().executortype(), static_cast<int32_t>(EXECUTOR_TYPE::SUPERVISOR));
    // Message should come from the underlying StartResponse (Failed to create sandbox)
    EXPECT_THAT(rsp.message(), testing::HasSubstr("Failed to create sandbox"));
    EXPECT_EQ(rsp.requestid(), "test_request_id");
}

/**
 * Feature: StartRuntime
 * Description: Test StartRuntime with illegal command - should propagate ERR_PARAM_INVALID
 *              from StartByRuntimeID into a failed StartInstanceResponse
 */
TEST_F(SupervisorExecutorTest, StartRuntime_RejectsIllegalCommandChars)
{
    auto request = GenStartInstanceRequest("python3", "/bin/echo$()");
    Envs envs;
    std::vector<std::string> args = { "arg" };

    auto future = executor_->TestStartRuntime(request, "python3", envs, args);

    ASSERT_AWAIT_READY_FOR(future, TEST_AWAIT_TIMEOUT);
    auto rsp = future.Get();
    EXPECT_NE(rsp.code(), static_cast<int32_t>(StatusCode::SUCCESS));
    EXPECT_EQ(rsp.startruntimeinstanceresponse().executortype(), static_cast<int32_t>(EXECUTOR_TYPE::SUPERVISOR));
    // Underlying StartByRuntimeID returns ERR_PARAM_INVALID; GenFailStartInstanceResponse
    // wraps with RUNTIME_MANAGER_CREATE_EXEC_FAILED code, message carries the original
    EXPECT_THAT(rsp.message(), testing::HasSubstr("/bin/echo$()"));
}

/**
 * Feature: CreateRequest env_vars propagation
 * Description: The /api/agent frontend splits env_vars into runtimeconfig.posixenvs.
 *              CreateRequest copies posixenvs into policy.environment at sandbox
 *              creation time (register and inline style both go through CreateSandbox
 *              -> CreateRequest). JIUWENSWARM_HOME from host_user is applied last so
 *              a colliding posixenvs entry can never overwrite the home dir.
 */
TEST_F(SupervisorExecutorTest, CreateRequest_AppliesEnvVarsFromPosixEnvs)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    auto runtimeInfo = request->mutable_runtimeinstanceinfo();
    runtimeInfo->set_runtimeid("rt");
    auto posixEnvs = runtimeInfo->mutable_runtimeconfig()->mutable_posixenvs();
    (*posixEnvs)["AGENT_SERVER_HOST"] = "0.0.0.0";
    (*posixEnvs)["AGENT_SERVER_PORT"] = "18092";
    (*posixEnvs)["YR_TENANT_ID"] = "dev";  // system env carried alongside

    auto req = executor_->TestCreateRequest(request);
    EXPECT_EQ(req["policy_mode"], "append");
    ASSERT_TRUE(req["policy"]["environment"].is_object());
    EXPECT_EQ(req["policy"]["environment"]["AGENT_SERVER_HOST"], "0.0.0.0");
    EXPECT_EQ(req["policy"]["environment"]["AGENT_SERVER_PORT"], "18092");
    EXPECT_EQ(req["policy"]["environment"]["YR_TENANT_ID"], "dev");
    // host_user unset -> defaults to "agentos"; process/namespace still emitted
    EXPECT_EQ(req["policy"]["environment"]["JIUWENSWARM_HOME"], "/home/agentos");
    EXPECT_EQ(req["policy"]["process"]["run_as_user"], "agentos");
    EXPECT_EQ(req["policy"]["process"]["run_as_group"], "agentos");
    EXPECT_EQ(req["policy"]["namespace"]["user"], false);
}

TEST_F(SupervisorExecutorTest, CreateRequest_PosixEnvsCoexistWithHostUser)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    auto runtimeInfo = request->mutable_runtimeinstanceinfo();
    runtimeInfo->set_runtimeid("rt");
    (*runtimeInfo->mutable_deploymentconfig()->mutable_deployoptions())[HOST_USER] = "User9876";
    (*runtimeInfo->mutable_runtimeconfig()->mutable_posixenvs())["AGENT_SERVER_PORT"] = "18092";

    auto req = executor_->TestCreateRequest(request);
    ASSERT_TRUE(req["policy"]["environment"].is_object());
    EXPECT_EQ(req["policy"]["environment"]["AGENT_SERVER_PORT"], "18092");
    EXPECT_EQ(req["policy"]["environment"]["JIUWENSWARM_HOME"], "/home/User9876");
    EXPECT_EQ(req["policy"]["process"]["run_as_user"], "User9876");
    EXPECT_EQ(req["policy"]["process"]["run_as_group"], "User9876");
    EXPECT_EQ(req["policy"]["namespace"]["user"], false);
}

/**
 * Feature: CreateRequest default policy (no posixenvs, no host_user)
 * Description: With neither runtimeconfig.posixenvs nor an explicit host_user,
 *              CreateRequest still emits a minimal sandbox policy: JIUWENSWARM_HOME
 *              and process/namespace entries are always present (host_user defaults
 *              to "agentos"); only the user-supplied environment entries are absent.
 */
TEST_F(SupervisorExecutorTest, CreateRequest_NoEnvVarsNoHostUserHasDefaultPolicy)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->mutable_runtimeinstanceinfo()->set_runtimeid("rt");
    auto req = executor_->TestCreateRequest(request);
    // environment always carries the default home dir; no user envs are merged in
    ASSERT_TRUE(req["policy"]["environment"].is_object());
    EXPECT_EQ(req["policy"]["environment"].size(), 1);
    EXPECT_EQ(req["policy"]["environment"]["JIUWENSWARM_HOME"], "/home/agentos");
    EXPECT_EQ(req["policy"]["process"]["run_as_user"], "agentos");
    EXPECT_EQ(req["policy"]["process"]["run_as_group"], "agentos");
    EXPECT_EQ(req["policy"]["namespace"]["user"], false);
}

/**
 * Feature: CreateRequest cgroup generation
 * Description: CreateRequest reads runtimeconfig.resources (CPU/Memory) and emits
 *              policy.cgroup with cpu_max (millicores -> cores, 600 -> 0.6) and
 *              memory_max (MB -> "NM", 512 -> "512M"). Both inline and registered
 *              mode reach CreateRequest with the same runtimeconfig.resources shape.
 */
TEST_F(SupervisorExecutorTest, CreateRequest_EmitsCgroupFromCpuAndMemory)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->mutable_runtimeinstanceinfo()->set_runtimeid("rt");
    SetRuntimeResource(request, "CPU", 600);
    SetRuntimeResource(request, "Memory", 512);

    auto req = executor_->TestCreateRequest(request);
    ASSERT_TRUE(req["policy"].contains("cgroup"));
    EXPECT_EQ(req["policy"]["cgroup"]["cpu_max"], 0.6);
    EXPECT_EQ(req["policy"]["cgroup"]["memory_max"], "512M");
}

TEST_F(SupervisorExecutorTest, CreateRequest_CpuOnlyEmitsCpuMax)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->mutable_runtimeinstanceinfo()->set_runtimeid("rt");
    SetRuntimeResource(request, "CPU", 1000);  // 1 core

    auto req = executor_->TestCreateRequest(request);
    ASSERT_TRUE(req["policy"].contains("cgroup"));
    EXPECT_EQ(req["policy"]["cgroup"]["cpu_max"], 1.0);
    EXPECT_FALSE(req["policy"]["cgroup"].contains("memory_max"));
}

TEST_F(SupervisorExecutorTest, CreateRequest_MemoryOnlyEmitsMemoryMax)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->mutable_runtimeinstanceinfo()->set_runtimeid("rt");
    SetRuntimeResource(request, "Memory", 2048);

    auto req = executor_->TestCreateRequest(request);
    ASSERT_TRUE(req["policy"].contains("cgroup"));
    EXPECT_EQ(req["policy"]["cgroup"]["memory_max"], "2048M");
    EXPECT_FALSE(req["policy"]["cgroup"].contains("cpu_max"));
}

/**
 * Feature: CreateRequest cgroup skipped on empty/zero resources
 * Description: A zero CPU/Memory value is treated as "not set"; no cgroup section is
 *              emitted so jiuwenbox applies no resource limit (preserves prior behavior).
 */
TEST_F(SupervisorExecutorTest, CreateRequest_ZeroValuesOmitCgroup)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->mutable_runtimeinstanceinfo()->set_runtimeid("rt");
    SetRuntimeResource(request, "CPU", 0);
    SetRuntimeResource(request, "Memory", 0);

    auto req = executor_->TestCreateRequest(request);
    EXPECT_FALSE(req["policy"].contains("cgroup"));
}

TEST_F(SupervisorExecutorTest, CreateRequest_NoResourcesOmitsCgroup)
{
    auto request = std::make_shared<messages::StartInstanceRequest>();
    request->mutable_runtimeinstanceinfo()->set_runtimeid("rt");
    // no resources set at all

    auto req = executor_->TestCreateRequest(request);
    EXPECT_FALSE(req["policy"].contains("cgroup"));
    // base policy still present
    EXPECT_EQ(req["policy"]["environment"]["JIUWENSWARM_HOME"], "/home/agentos");
    EXPECT_EQ(req["policy_mode"], "append");
}

/**
 * Feature: BuildCommand
 * Description: BuildCommand wraps every argv token and redirect path in POSIX single-quotes
 *              so that sh -c takes them literally. A token containing a single quote must
 *              be split via the '\'' sequence; shell metacharacters must survive verbatim
 *              and never be interpreted (no injection).
 */
TEST_F(SupervisorExecutorTest, BuildCommand_ShellQuotesTokensAndRedirects)
{
    auto start = std::make_shared<runtime::v1::StartRequest>();
    start->add_command("python3");
    start->add_command("-c");
    // Contains a single quote and parens — must not be interpreted by sh.
    start->add_command("print('hello; rm -rf /')");
    start->set_stdout("/tmp/log dir/a'b.out");  // space + single quote in path
    start->set_stderr("/tmp/log dir/a'b.err");

    auto command = executor_->TestBuildCommand(start);
    ASSERT_EQ(command.size(), 3);
    EXPECT_EQ(command[0], "sh");
    EXPECT_EQ(command[1], "-c");
    std::string cmdLine = command[2];

    // argv tokens quoted verbatim, no metacharacter interpretation
    EXPECT_THAT(cmdLine, testing::HasSubstr("'python3'"));
    EXPECT_THAT(cmdLine, testing::HasSubstr("'-c'"));
    // single quote inside token escaped via '\'' rather than closing the wrapper
    EXPECT_THAT(cmdLine, testing::HasSubstr("'print('\\''hello; rm -rf /'\\''"));
    // redirect paths quoted too (space + quote survived)
    EXPECT_THAT(cmdLine, testing::HasSubstr(" >'/tmp/log dir/a'\\''b.out'"));
    EXPECT_THAT(cmdLine, testing::HasSubstr(" 2>'/tmp/log dir/a'\\''b.err'"));
    // injection payload must NOT appear as a bare command
    EXPECT_THAT(cmdLine, testing::Not(testing::HasSubstr("; rm -rf /')")));
}

/**
 * Feature: BuildCommand
 * Description: Empty argv and empty redirect paths must not break the shell line shape
 *             (empty tokens still quote to '').
 */
TEST_F(SupervisorExecutorTest, BuildCommand_EmptyTokensAndPaths)
{
    auto start = std::make_shared<runtime::v1::StartRequest>();
    start->add_command("");
    start->add_command("arg with space");

    auto command = executor_->TestBuildCommand(start);
    ASSERT_EQ(command.size(), 3);
    std::string cmdLine = command[2];
    EXPECT_EQ(cmdLine, "'' 'arg with space' >'' 2>''");
}

}  // namespace functionsystem::runtime_manager
