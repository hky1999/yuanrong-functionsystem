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

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <unistd.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/server_builder.h>

#include "common/metadata/metadata.h"
#include "common/proto/pb/posix_pb.h"
#include "function_proxy/busproxy/invocation_handler/invocation_handler.h"
#include "mocks/mock_instance_proxy_wrapper.h"
// Allow test access to private members of FrontendProxyService.
#define private public
#include "grpc_server/frontend_proxy_service/frontend_proxy_service.h"
#undef private

namespace functionsystem::test {
using namespace local_scheduler;

namespace {
class UnixGrpcServer {
public:
    UnixGrpcServer(std::string socketPath, std::unique_ptr<::grpc::Server> server)
        : socketPath_(std::move(socketPath)), server_(std::move(server))
    {
    }

    ~UnixGrpcServer()
    {
        if (server_ != nullptr) {
            server_->Shutdown();
            server_->Wait();
        }
        (void)::unlink(socketPath_.c_str());
    }

    ::grpc::Server *Get() const
    {
        return server_.get();
    }

private:
    std::string socketPath_;
    std::unique_ptr<::grpc::Server> server_;
};

class ScopedInvocationProxy {
public:
    ScopedInvocationProxy() : proxy_(std::make_shared<MockInstanceProxy>())
    {
        ON_CALL(*proxy_, CompleteFrontendCall)
            .WillByDefault([](const litebus::AID &, const SharedStreamMsg &, const SharedStreamMsg &ack) {
                return litebus::Future<SharedStreamMsg>(ack);
            });
        InvocationHandler::BindInstanceProxy(proxy_);
    }

    ~ScopedInvocationProxy()
    {
        InvocationHandler::UnBindInstanceProxy();
    }

private:
    std::shared_ptr<MockInstanceProxy> proxy_;
};
}  // namespace

TEST(FrontendProxyServiceTest, RejectsUnauthenticatedPeerWhenRequired)
{
    FrontendProxyServiceParam param;
    param.requireAuthenticatedPeer = true;
    FrontendProxyService service(std::move(param));

    ::frontend_proxy::InvokeInstanceRequest request;
    ::frontend_proxy::InvokeInstanceResponse response;
    auto status = service.InvokeInstance(nullptr, &request, &response);

    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::UNAUTHENTICATED);
}

TEST(FrontendProxyServiceTest, InvokeTimeoutUsesFunctionTimeoutWithResultBuffer)
{
    FrontendProxyServiceParam param;
    param.invokeResultTimeoutMs = 60000;
    param.invokeResultTimeoutBufferMs = 5000;
    ::frontend_proxy::InvokeInstanceRequest request;

    EXPECT_EQ(FrontendProxyService::ResolveInvokeResultTimeout(request, param).count(), 65000);
    request.set_invoketimeoutms(-1);
    EXPECT_EQ(FrontendProxyService::ResolveInvokeResultTimeout(request, param).count(), 65000);
    request.set_invoketimeoutms(30000);
    EXPECT_EQ(FrontendProxyService::ResolveInvokeResultTimeout(request, param).count(), 35000);
    request.set_invoketimeoutms(std::numeric_limits<int64_t>::max());
    EXPECT_EQ(FrontendProxyService::ResolveInvokeResultTimeout(request, param).count(), 8640005000);
}

TEST(FrontendProxyServiceTest, CreateTimeoutUsesFrontendValueWithLegacyFallback)
{
    FrontendProxyServiceParam param;
    param.invokeResultTimeoutMs = 60000;
    ::frontend_proxy::CreateInstanceRequest request;

    EXPECT_EQ(FrontendProxyService::ResolveCreateResultTimeout(request, param).count(), 60000);
    request.set_createtimeoutms(-1);
    EXPECT_EQ(FrontendProxyService::ResolveCreateResultTimeout(request, param).count(), 60000);
    request.set_createtimeoutms(300000);
    EXPECT_EQ(FrontendProxyService::ResolveCreateResultTimeout(request, param).count(), 300000);
}

TEST(FrontendProxyServiceTest, AwaitInvokeResultUsesRemainingDeadline)
{
    FrontendProxyService service(FrontendProxyServiceParam {});
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_requestid("invoke-before-deadline");
    request.mutable_invoke()->set_instanceid("instance-before-deadline");
    litebus::Promise<SharedStreamMsg> promise;
    auto resultFuture = promise.GetFuture();
    std::thread producer([&promise]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        auto result = std::make_shared<runtime_rpc::StreamingMessage>();
        result->mutable_callresultreq()->set_code(common::ERR_NONE);
        result->mutable_callresultreq()->set_requestid("invoke-before-deadline");
        promise.SetValue(result);
    });
    ::frontend_proxy::InvokeInstanceResponse response;

    auto status = service.AwaitInvokeResult(nullptr, request, response, resultFuture,
                                            std::chrono::steady_clock::now() + std::chrono::milliseconds(200));
    producer.join();

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.status().code(), common::ERR_NONE);
    EXPECT_EQ(response.callresult().requestid(), "invoke-before-deadline");
}

TEST(FrontendProxyServiceTest, AwaitInvokeResultTimesOutAtExpiredDeadline)
{
    FrontendProxyService service(FrontendProxyServiceParam {});
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_requestid("invoke-expired-deadline");
    request.mutable_invoke()->set_instanceid("instance-expired-deadline");
    litebus::Promise<SharedStreamMsg> promise;
    ::frontend_proxy::InvokeInstanceResponse response;

    auto status = service.AwaitInvokeResult(nullptr, request, response, promise.GetFuture(),
                                            std::chrono::steady_clock::now() - std::chrono::milliseconds(1));

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.status().code(), common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_EQ(response.status().retryreason(), "post-dispatch-unknown");
}
TEST(FrontendProxyServiceTest, InvokeUsesFunctionProxyAsRuntimeSender)
{
    std::string capturedCaller;
    SharedStreamMsg capturedRequest;

    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.invokeResultTimeoutMs = 1;
    param.invokeDispatcher = [&capturedCaller, &capturedRequest](const std::string &caller,
                                                                 const SharedStreamMsg &request) {
        capturedCaller = caller;
        capturedRequest = request;
        auto response = std::make_shared<runtime_rpc::StreamingMessage>();
        response->mutable_invokersp()->set_code(common::ERR_NONE);
        return litebus::Future<SharedStreamMsg>(response);
    };
    FrontendProxyService service(std::move(param));

    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-process");
    request.mutable_context()->set_requestid("frontend-proxy-request-1");
    request.mutable_context()->set_traceid("trace-1");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_invoke()->set_instanceid("faas-instance-1");
    request.mutable_invoke()->set_requestid("frontend-proxy-request-1");
    request.mutable_invoke()->set_traceid("trace-1");
    request.mutable_invoke()->set_function("0/tenant/faas/function");

    ::frontend_proxy::InvokeInstanceResponse response;
    auto status = service.InvokeInstance(nullptr, &request, &response);
    EXPECT_TRUE(status.ok());

    ASSERT_NE(capturedRequest, nullptr);
    ASSERT_TRUE(capturedRequest->has_invokereq());
    EXPECT_EQ(capturedCaller, "function-proxy");
    EXPECT_EQ(capturedRequest->invokereq().requestid(), "frontend-proxy-request-1");
}

TEST(FrontendProxyServiceTest, InvokeRejectsTenantThatDoesNotOwnTargetInstance)
{
    bool dispatched = false;
    FrontendProxyServiceParam param;
    param.invokeTenantAuthorizer = [](const std::string &tenantID, const std::string &instanceID) {
        EXPECT_EQ(tenantID, "tenant-a");
        EXPECT_EQ(instanceID, "instance-b");
        return false;
    };
    param.invokeDispatcher = [&dispatched](const std::string &, const SharedStreamMsg &) {
        dispatched = true;
        return litebus::Future<SharedStreamMsg>();
    };
    FrontendProxyService service(std::move(param));

    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-process");
    request.mutable_context()->set_requestid("tenant-mismatch-request");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_invoke()->set_instanceid("instance-b");
    request.mutable_invoke()->set_requestid("tenant-mismatch-request");
    ::frontend_proxy::InvokeInstanceResponse response;

    EXPECT_TRUE(service.InvokeInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_AUTHORIZE_FAILED);
    EXPECT_FALSE(dispatched);
}

TEST(FrontendProxyServiceTest, InvokeTerminalSuccessReturnsDirectResult)
{
    ScopedInvocationProxy invocationProxy;
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.endpointAddress = "10.0.0.11:19090";
    param.invokeResultTimeoutMs = 100;
    param.invokeDispatcher = [](const std::string &, const SharedStreamMsg &request) {
        auto result = std::make_shared<runtime_rpc::StreamingMessage>();
        result->mutable_callresultreq()->set_code(common::ERR_NONE);
        result->mutable_callresultreq()->set_requestid(request->invokereq().requestid());
        result->mutable_callresultreq()->set_instanceid(request->invokereq().instanceid());
        (void)InvocationHandler::CallResultAdapter(grpc::PosixStreamIdentity{ request->invokereq().instanceid(), "", "" }, result);
        auto response = std::make_shared<runtime_rpc::StreamingMessage>();
        response->mutable_invokersp()->set_code(common::ERR_NONE);
        return litebus::Future<SharedStreamMsg>(response);
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("invoke-success");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_invoke()->set_instanceid("instance-a");
    ::frontend_proxy::InvokeInstanceResponse response;

    EXPECT_TRUE(service.InvokeInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_NONE);
    EXPECT_EQ(response.callresult().requestid(), "invoke-success");
    EXPECT_EQ(response.callresult().instanceid(), "instance-a");
}

TEST(FrontendProxyServiceTest, InvokeStreamForwardsEventsBeforeFinalResult)
{
    ScopedInvocationProxy invocationProxy;
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-stream";
    param.endpointAddress = "127.0.0.1:19090";
    param.invokeResultTimeoutMs = 1000;
    param.invokeDispatcher = [](const std::string &caller, const SharedStreamMsg &request) {
        EXPECT_EQ(caller, "function-proxy");

        auto event = std::make_shared<runtime_rpc::StreamingMessage>();
        event->mutable_eventreq()->set_requestid(request->invokereq().requestid());
        event->mutable_eventreq()->set_message(std::string(16, '\0') + "first-event");
        (void)InvocationHandler::EventAdapter(grpc::PosixStreamIdentity{ "runtime-instance", "", "" }, event);

        auto eof = std::make_shared<runtime_rpc::StreamingMessage>();
        eof->mutable_eventreq()->set_requestid(request->invokereq().requestid());
        eof->mutable_eventreq()->set_message(std::string(16, '\0') + "yuanrong_event_EOF");
        (void)InvocationHandler::EventAdapter(grpc::PosixStreamIdentity{ "runtime-instance", "", "" }, eof);

        auto result = std::make_shared<runtime_rpc::StreamingMessage>();
        result->mutable_callresultreq()->set_code(common::ERR_NONE);
        result->mutable_callresultreq()->set_requestid(request->invokereq().requestid());
        result->mutable_callresultreq()->set_instanceid(request->invokereq().instanceid());
        result->mutable_callresultreq()->add_smallobjects()->set_value("final-result");
        (void)InvocationHandler::CallResultAdapter(grpc::PosixStreamIdentity{ request->invokereq().instanceid(), "", "" }, result);

        auto response = std::make_shared<runtime_rpc::StreamingMessage>();
        response->mutable_invokersp()->set_code(common::ERR_NONE);
        return litebus::Future<SharedStreamMsg>(response);
    };
    FrontendProxyService service(std::move(param));
    ::grpc::ServerBuilder builder;
    const auto socketPath = "/tmp/frontend-proxy-invoke-stream-" + std::to_string(::getpid()) + ".sock";
    const auto address = "unix:" + socketPath;
    (void)::unlink(socketPath.c_str());
    builder.AddListeningPort(address, ::grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    UnixGrpcServer server(socketPath, builder.BuildAndStart());
    ASSERT_NE(server.Get(), nullptr);

    auto channel = ::grpc::CreateChannel(address, ::grpc::InsecureChannelCredentials());
    ASSERT_TRUE(channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(2)));
    auto stub = ::frontend_proxy::FrontendProxyService::NewStub(channel);
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("invoke-stream-request");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_invoke()->set_instanceid("instance-a");
    ::grpc::ClientContext clientContext;
    auto reader = stub->InvokeInstanceStream(&clientContext, request);

    ::frontend_proxy::InvokeInstanceStreamResponse frame;
    ASSERT_TRUE(reader->Read(&frame));
    ASSERT_EQ(frame.payload_case(), ::frontend_proxy::InvokeInstanceStreamResponse::kEvent);
    EXPECT_EQ(frame.event(), "first-event");
    ASSERT_TRUE(reader->Read(&frame));
    ASSERT_TRUE(frame.has_final());
    EXPECT_EQ(frame.final().status().code(), common::ERR_NONE);
    ASSERT_EQ(frame.final().callresult().smallobjects_size(), 1);
    EXPECT_EQ(frame.final().callresult().smallobjects(0).value(), "final-result");
    EXPECT_FALSE(reader->Read(&frame));
    EXPECT_TRUE(reader->Finish().ok());
}

TEST(FrontendProxyServiceTest, InvokeStreamCancellationClearsEventAndResultRegistries)
{
    auto pending = std::make_shared<litebus::Promise<SharedStreamMsg>>();
    std::mutex mutex;
    std::condition_variable entered;
    int dispatchCount = 0;
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-stream-cancel";
    param.endpointAddress = "127.0.0.1:19090";
    param.invokeResultTimeoutMs = 5000;
    param.invokeDispatcher = [&](const std::string &, const SharedStreamMsg &) {
        std::lock_guard<std::mutex> lock(mutex);
        ++dispatchCount;
        entered.notify_all();
        if (dispatchCount == 1) {
            return pending->GetFuture();
        }
        auto response = std::make_shared<runtime_rpc::StreamingMessage>();
        response->mutable_invokersp()->set_code(common::ERR_PARAM_INVALID);
        response->mutable_invokersp()->set_message("retry proves stream registries were cleared");
        return litebus::Future<SharedStreamMsg>(response);
    };
    FrontendProxyService service(std::move(param));
    ::grpc::ServerBuilder builder;
    const auto socketPath = "/tmp/frontend-proxy-stream-cancel-" + std::to_string(::getpid()) + ".sock";
    const auto address = "unix:" + socketPath;
    (void)::unlink(socketPath.c_str());
    builder.AddListeningPort(address, ::grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    UnixGrpcServer server(socketPath, builder.BuildAndStart());
    ASSERT_NE(server.Get(), nullptr);

    auto channel = ::grpc::CreateChannel(address, ::grpc::InsecureChannelCredentials());
    ASSERT_TRUE(channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(2)));
    auto stub = ::frontend_proxy::FrontendProxyService::NewStub(channel);
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("invoke-stream-cancel-request");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_invoke()->set_instanceid("instance-a");

    ::grpc::ClientContext firstContext;
    auto firstReader = stub->InvokeInstanceStream(&firstContext, request);
    auto firstCall = std::async(std::launch::async, [&] {
        ::frontend_proxy::InvokeInstanceStreamResponse frame;
        while (firstReader->Read(&frame)) {
        }
        return firstReader->Finish();
    });
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(entered.wait_for(lock, std::chrono::seconds(2), [&] { return dispatchCount == 1; }));
    }
    firstContext.TryCancel();
    EXPECT_EQ(firstCall.get().error_code(), ::grpc::StatusCode::CANCELLED);

    bool retrySucceeded = false;
    const auto retryDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!retrySucceeded && std::chrono::steady_clock::now() < retryDeadline) {
        ::grpc::ClientContext retryContext;
        auto retryReader = stub->InvokeInstanceStream(&retryContext, request);
        ::frontend_proxy::InvokeInstanceStreamResponse retryFrame;
        ASSERT_TRUE(retryReader->Read(&retryFrame));
        ASSERT_TRUE(retryFrame.has_final());
        retrySucceeded = retryFrame.final().status().message() == "retry proves stream registries were cleared";
        EXPECT_FALSE(retryReader->Read(&retryFrame));
        EXPECT_TRUE(retryReader->Finish().ok());
        if (!retrySucceeded) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    EXPECT_TRUE(retrySucceeded);
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(dispatchCount, 2);
}

TEST(FrontendProxyServiceTest, InvokeTerminalSuccessUsesRuntimeSenderWhenResultOmitsInstanceID)
{
    ScopedInvocationProxy invocationProxy;
    FrontendProxyServiceParam param;
    param.invokeResultTimeoutMs = 100;
    param.invokeDispatcher = [](const std::string &, const SharedStreamMsg &request) {
        auto result = std::make_shared<runtime_rpc::StreamingMessage>();
        result->mutable_callresultreq()->set_code(common::ERR_NONE);
        result->mutable_callresultreq()->set_requestid(request->invokereq().requestid());
        (void)InvocationHandler::CallResultAdapter(grpc::PosixStreamIdentity{ request->invokereq().instanceid(), "", "" }, result);
        auto response = std::make_shared<runtime_rpc::StreamingMessage>();
        response->mutable_invokersp()->set_code(common::ERR_NONE);
        return litebus::Future<SharedStreamMsg>(response);
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("invoke-without-result-instance");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_invoke()->set_instanceid("instance-a");
    ::frontend_proxy::InvokeInstanceResponse response;

    EXPECT_TRUE(service.InvokeInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_NONE);
    EXPECT_EQ(response.callresult().requestid(), "invoke-without-result-instance");
    EXPECT_TRUE(response.callresult().instanceid().empty());
}

TEST(FrontendProxyServiceTest, CreateReadyTerminalSuccessCarriesOwningRoute)
{
    EXPECT_STREQ(FrontendProxyService::lifecycleTransport, "raw-unary");
    EXPECT_STREQ(FrontendProxyService::readyOperation, "ready");
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.endpointAddress = "10.0.0.11:19090";
    param.enableCreateDispatch = true;
    param.createReadyDispatcher = [](const ::frontend_proxy::CreateInstanceRequest &) {
        ::frontend_proxy::CreateInstanceResponse response;
        response.mutable_create()->set_code(common::ERR_NONE);
        response.mutable_create()->set_instanceid("created-instance");
        response.set_routeaddress("stale-entry-proxy");
        response.mutable_callresult()->mutable_runtimeinfo()->set_proxyid("final-owner-proxy");
        return litebus::Future<::frontend_proxy::CreateInstanceResponse>(response);
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::CreateInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("create-ready-success");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_create()->set_function("0/tenant-a/faas/function");
    (*request.mutable_create()->mutable_createoptions())["source"] = "frontend";
    ::frontend_proxy::CreateInstanceResponse response;

    EXPECT_TRUE(service.CreateInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_NONE);
    EXPECT_EQ(response.create().instanceid(), "created-instance");
    EXPECT_EQ(response.routeaddress(), "final-owner-proxy");
}

class FrontendProxyCreateRouteTest : public ::testing::TestWithParam<bool> {
protected:
    void SetUp() override
    {
        previous_ = IsForceLowReliabilityInstanceEnabled();
        SetForceLowReliabilityInstance(GetParam());
    }

    void TearDown() override
    {
        SetForceLowReliabilityInstance(previous_);
    }

private:
    bool previous_ = false;
};

TEST_P(FrontendProxyCreateRouteTest, ScheduleOnlyResponseUsesEntryRouteForAsyncCreate)
{
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.enableCreateDispatch = true;
    param.createReadyDispatcher = [](const ::frontend_proxy::CreateInstanceRequest &) {
        ::frontend_proxy::CreateInstanceResponse response;
        response.mutable_create()->set_code(common::ERR_NONE);
        response.mutable_create()->set_instanceid("creating-instance");
        return litebus::Future<::frontend_proxy::CreateInstanceResponse>(response);
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::CreateInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("create-async-no-callresult");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_create()->set_function("0/tenant-a/faas/function");
    (*request.mutable_create()->mutable_createoptions())["source"] = "frontend";
    ::frontend_proxy::CreateInstanceResponse response;

    EXPECT_TRUE(service.CreateInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), GetParam() ? common::ERR_INNER_SYSTEM_ERROR : common::ERR_NONE);
    EXPECT_EQ(response.create().instanceid(), "creating-instance");
    EXPECT_FALSE(response.has_callresult());
    EXPECT_EQ(response.routeaddress(), GetParam() ? "" : "proxy-node-a");
}

TEST_P(FrontendProxyCreateRouteTest, ReadyResponseRequiresFinalOwner)
{
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::CreateInstanceRequest request;
    ::frontend_proxy::CreateInstanceResponse response;
    response.mutable_create()->set_code(common::ERR_NONE);
    response.mutable_create()->set_instanceid("created-instance");
    response.mutable_callresult()->set_code(common::ERR_NONE);

    EXPECT_TRUE(service.FinalizeCreateResponse(request, response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_INNER_SYSTEM_ERROR);
    EXPECT_TRUE(response.routeaddress().empty());

    response.mutable_callresult()->mutable_runtimeinfo()->set_proxyid("final-owner-proxy");
    EXPECT_TRUE(service.FinalizeCreateResponse(request, response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_NONE);
    EXPECT_EQ(response.routeaddress(), "final-owner-proxy");
}

INSTANTIATE_TEST_SUITE_P(ReliabilityModes, FrontendProxyCreateRouteTest, ::testing::Bool());

TEST(FrontendProxyServiceTest, DuplicateRequestIDDoesNotReplaceExistingWaiter)
{
    std::mutex mutex;
    std::condition_variable entered;
    bool firstInvokeDispatched = false;
    bool releaseFirstInvoke = false;
    size_t dispatchCount = 0;

    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.invokeResultTimeoutMs = 20;
    param.invokeDispatcher = [&mutex, &entered, &firstInvokeDispatched, &releaseFirstInvoke,
                              &dispatchCount](const std::string &, const SharedStreamMsg &) {
        std::unique_lock<std::mutex> lock(mutex);
        ++dispatchCount;
        if (dispatchCount == 1) {
            firstInvokeDispatched = true;
            entered.notify_one();
            entered.wait(lock, [&releaseFirstInvoke] { return releaseFirstInvoke; });
        }
        auto response = std::make_shared<runtime_rpc::StreamingMessage>();
        response->mutable_invokersp()->set_code(common::ERR_NONE);
        return litebus::Future<SharedStreamMsg>(response);
    };
    FrontendProxyService service(std::move(param));

    auto makeRequest = [](const std::string &frontendClientID) {
        ::frontend_proxy::InvokeInstanceRequest request;
        request.mutable_context()->set_frontendclientid(frontendClientID);
        request.mutable_context()->set_requestid("duplicate-request-id");
        request.mutable_context()->set_tenantid("tenant-a");
        request.mutable_invoke()->set_instanceid("faas-instance-1");
        request.mutable_invoke()->set_requestid("duplicate-request-id");
        return request;
    };

    auto firstRequest = makeRequest("frontend-process-a");
    ::frontend_proxy::InvokeInstanceResponse firstResponse;
    auto firstCall = std::async(std::launch::async, [&service, &firstRequest, &firstResponse] {
        return service.InvokeInstance(nullptr, &firstRequest, &firstResponse);
    });
    bool firstInvokeEntered = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        firstInvokeEntered = entered.wait_for(lock, std::chrono::seconds(1),
                                              [&firstInvokeDispatched] { return firstInvokeDispatched; });
    }
    EXPECT_TRUE(firstInvokeEntered);
    if (!firstInvokeEntered) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            releaseFirstInvoke = true;
        }
        entered.notify_one();
        (void)firstCall.get();
        return;
    }

    auto duplicateRequest = makeRequest("frontend-process-b");
    ::frontend_proxy::InvokeInstanceResponse duplicateResponse;
    auto duplicateStatus = service.InvokeInstance(nullptr, &duplicateRequest, &duplicateResponse);
    EXPECT_TRUE(duplicateStatus.ok());
    EXPECT_EQ(duplicateResponse.status().code(), common::ERR_PARAM_INVALID);
    EXPECT_EQ(duplicateResponse.status().message(), "frontend proxy invoke requires a globally unique request id");

    {
        std::lock_guard<std::mutex> lock(mutex);
        releaseFirstInvoke = true;
    }
    entered.notify_one();
    EXPECT_TRUE(firstCall.get().ok());
}

TEST(FrontendProxyServiceTest, CreateTimeoutUnregistersReadyWaitWithoutCancellingInstance)
{
    auto pending = std::make_shared<litebus::Promise<::frontend_proxy::CreateInstanceResponse>>();
    std::string cancelledRequestID;
    std::string cancelReason;
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.enableCreateDispatch = true;
    param.invokeResultTimeoutMs = 1;
    param.createReadyDispatcher = [pending](const ::frontend_proxy::CreateInstanceRequest &) {
        return pending->GetFuture();
    };
    param.createWaitCanceller = [&cancelledRequestID, &cancelReason](const std::string &requestID,
                                                                    const std::string &reason) {
        cancelledRequestID = requestID;
        cancelReason = reason;
    };
    FrontendProxyService service(std::move(param));

    ::frontend_proxy::CreateInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-process-a");
    request.mutable_context()->set_requestid("create-timeout-request");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_create()->set_function("0/tenant-a/faas/function");
    (*request.mutable_create()->mutable_createoptions())["source"] = "frontend";
    ::frontend_proxy::CreateInstanceResponse response;

    EXPECT_TRUE(service.CreateInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(cancelledRequestID, "create-timeout-request");
    EXPECT_EQ(cancelReason, "frontend create timed out");
    EXPECT_EQ(response.status().retryreason(), "post-dispatch-unknown");
    EXPECT_FALSE(response.status().retryable());
}

TEST(FrontendProxyServiceTest, CreateRejectsTenantFieldMismatch)
{
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.enableCreateDispatch = true;
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::CreateInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-process-a");
    request.mutable_context()->set_requestid("tenant-mismatch-request");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_create()->set_function("0/tenant-b/faas/function");
    (*request.mutable_create()->mutable_createoptions())["source"] = "frontend";
    (*request.mutable_create()->mutable_createoptions())["tenantID"] = "tenant-b";
    ::frontend_proxy::CreateInstanceResponse response;

    EXPECT_TRUE(service.CreateInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_AUTHORIZE_FAILED);
}

TEST(FrontendProxyServiceTest, RejectsOperationRequestIDMismatchBeforeDispatch)
{
    int invokeDispatches = 0;
    int createDispatches = 0;
    int killDispatches = 0;
    FrontendProxyServiceParam param;
    param.enableCreateDispatch = true;
    param.enableKillDispatch = true;
    param.invokeDispatcher = [&](const std::string &, const SharedStreamMsg &) {
        ++invokeDispatches;
        return litebus::Future<SharedStreamMsg>();
    };
    param.createReadyDispatcher = [&](const ::frontend_proxy::CreateInstanceRequest &) {
        ++createDispatches;
        return litebus::Future<::frontend_proxy::CreateInstanceResponse>();
    };
    param.killReadyDispatcher = [&](const ::frontend_proxy::KillInstanceRequest &) {
        ++killDispatches;
        return litebus::Future<::frontend_proxy::KillInstanceResponse>();
    };
    FrontendProxyService service(std::move(param));

    ::frontend_proxy::InvokeInstanceRequest invoke;
    invoke.mutable_context()->set_frontendclientid("frontend-a");
    invoke.mutable_context()->set_requestid("context-request");
    invoke.mutable_context()->set_tenantid("tenant-a");
    invoke.mutable_invoke()->set_instanceid("instance-a");
    invoke.mutable_invoke()->set_requestid("operation-request");
    ::frontend_proxy::InvokeInstanceResponse invokeResponse;
    EXPECT_TRUE(service.InvokeInstance(nullptr, &invoke, &invokeResponse).ok());
    EXPECT_EQ(invokeResponse.status().code(), common::ERR_PARAM_INVALID);

    ::frontend_proxy::CreateInstanceRequest create;
    create.mutable_context()->set_frontendclientid("frontend-a");
    create.mutable_context()->set_tenantid("tenant-a");
    create.mutable_context()->set_requestid("context-request");
    create.mutable_create()->set_function("0/tenant-a/faas/function");
    create.mutable_create()->set_requestid("operation-request");
    (*create.mutable_create()->mutable_createoptions())["source"] = "frontend";
    ::frontend_proxy::CreateInstanceResponse createResponse;
    EXPECT_TRUE(service.CreateInstance(nullptr, &create, &createResponse).ok());
    EXPECT_EQ(createResponse.status().code(), common::ERR_PARAM_INVALID);

    ::frontend_proxy::KillInstanceRequest kill;
    kill.mutable_context()->set_frontendclientid("frontend-a");
    kill.mutable_context()->set_tenantid("tenant-a");
    kill.mutable_context()->set_requestid("context-request");
    kill.mutable_kill()->set_instanceid("instance-a");
    kill.mutable_kill()->set_requestid("operation-request");
    ::frontend_proxy::KillInstanceResponse killResponse;
    EXPECT_TRUE(service.KillInstance(nullptr, &kill, &killResponse).ok());
    EXPECT_EQ(killResponse.status().code(), common::ERR_PARAM_INVALID);

    EXPECT_EQ(invokeDispatches, 0);
    EXPECT_EQ(createDispatches, 0);
    EXPECT_EQ(killDispatches, 0);
}

TEST(FrontendProxyServiceTest, InvokeTimeoutAfterDispatchIsNonRetryableUnknown)
{
    auto pending = std::make_shared<litebus::Promise<SharedStreamMsg>>();
    FrontendProxyServiceParam param;
    param.invokeResultTimeoutMs = 1;
    param.invokeDispatcher = [pending](const std::string &, const SharedStreamMsg &) {
        return pending->GetFuture();
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("invoke-timeout");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_invoke()->set_instanceid("instance-a");
    ::frontend_proxy::InvokeInstanceResponse response;

    EXPECT_TRUE(service.InvokeInstance(nullptr, &request, &response).ok());
    EXPECT_FALSE(response.status().retryable());
    EXPECT_EQ(response.status().retryreason(), "post-dispatch-unknown");
}

TEST(FrontendProxyServiceTest, InvokeRejectedBeforeRuntimeExecutionIsRetryable)
{
    FrontendProxyServiceParam param;
    param.invokeDispatcher = [](const std::string &, const SharedStreamMsg &) {
        auto response = std::make_shared<runtime_rpc::StreamingMessage>();
        response->mutable_invokersp()->set_code(common::ERR_INSTANCE_NOT_FOUND);
        response->mutable_invokersp()->set_message("runtime did not accept invoke");
        return litebus::Future<SharedStreamMsg>(response);
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("invoke-rejected");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_invoke()->set_instanceid("instance-a");
    ::frontend_proxy::InvokeInstanceResponse response;

    EXPECT_TRUE(service.InvokeInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_INSTANCE_NOT_FOUND);
    EXPECT_TRUE(response.status().retryable());
    EXPECT_EQ(response.status().retryreason(), "call-response-error");
}

TEST(FrontendProxyServiceTest, InvokeRuntimeResultFailureIsSeparatedFromProxyStatus)
{
    ScopedInvocationProxy invocationProxy;
    FrontendProxyServiceParam param;
    param.invokeResultTimeoutMs = 100;
    param.invokeDispatcher = [](const std::string &, const SharedStreamMsg &request) {
        auto result = std::make_shared<runtime_rpc::StreamingMessage>();
        result->mutable_callresultreq()->set_code(common::ERR_INSTANCE_EXITED);
        result->mutable_callresultreq()->set_message("runtime exited during invoke");
        result->mutable_callresultreq()->set_requestid(request->invokereq().requestid());
        (void)InvocationHandler::CallResultAdapter(grpc::PosixStreamIdentity{ request->invokereq().instanceid(), "", "" }, result);
        auto response = std::make_shared<runtime_rpc::StreamingMessage>();
        response->mutable_invokersp()->set_code(common::ERR_NONE);
        return litebus::Future<SharedStreamMsg>(response);
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("invoke-result-failed");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_invoke()->set_instanceid("instance-a");
    ::frontend_proxy::InvokeInstanceResponse response;

    EXPECT_TRUE(service.InvokeInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_NONE);
    EXPECT_FALSE(response.status().retryable());
    EXPECT_TRUE(response.status().retryreason().empty());
    EXPECT_EQ(response.callresult().code(), common::ERR_INSTANCE_EXITED);
    EXPECT_EQ(response.callresult().message(), "runtime exited during invoke");
}

TEST(FrontendProxyServiceTest, KillLocalMissIsTypedAsRetryableStaleRoute)
{
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.enableKillDispatch = true;
    param.killReadyDispatcher = [](const ::frontend_proxy::KillInstanceRequest &) {
        ::frontend_proxy::KillInstanceResponse response;
        response.mutable_kill()->set_code(common::ERR_INSTANCE_NOT_FOUND);
        response.mutable_kill()->set_message("frontend proxy is not the owning proxy for this instance");
        return litebus::Future<::frontend_proxy::KillInstanceResponse>(response);
    };
    FrontendProxyService service(std::move(param));

    ::frontend_proxy::KillInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-process-a");
    request.mutable_context()->set_requestid("kill-stale-route-request");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_kill()->set_instanceid("instance-on-another-proxy");
    ::frontend_proxy::KillInstanceResponse response;

    EXPECT_TRUE(service.KillInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_INSTANCE_NOT_FOUND);
    EXPECT_TRUE(response.status().retryable());
    EXPECT_EQ(response.status().retryreason(), "route-stale");
}

TEST(FrontendProxyServiceTest, InvokeRequiresTenantAndRejectsConflictingTenantLabelBeforeDispatch)
{
    int dispatches = 0;
    FrontendProxyServiceParam param;
    param.invokeDispatcher = [&dispatches](const std::string &, const SharedStreamMsg &) {
        ++dispatches;
        return litebus::Future<SharedStreamMsg>();
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("invoke-tenant");
    request.mutable_invoke()->set_instanceid("instance-a");
    ::frontend_proxy::InvokeInstanceResponse response;

    EXPECT_TRUE(service.InvokeInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_PARAM_INVALID);
    request.mutable_context()->set_tenantid("tenant-a");
    (*request.mutable_context()->mutable_labels())["tenantID"] = "tenant-b";
    response.Clear();
    EXPECT_TRUE(service.InvokeInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_AUTHORIZE_FAILED);
    EXPECT_EQ(dispatches, 0);
}

TEST(FrontendProxyServiceTest, KillUnknownOwnerNotFoundFailsFastWithoutClaimingDeletion)
{
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.enableKillDispatch = true;
    param.killReadyDispatcher = [](const ::frontend_proxy::KillInstanceRequest &) {
        ::frontend_proxy::KillInstanceResponse response;
        response.mutable_kill()->set_code(common::ERR_INSTANCE_NOT_FOUND);
        response.mutable_kill()->set_message("owner could not authoritatively resolve instance");
        return litebus::Future<::frontend_proxy::KillInstanceResponse>(response);
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::KillInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("kill-owner-unknown");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_kill()->set_instanceid("instance-a");
    ::frontend_proxy::KillInstanceResponse response;

    EXPECT_TRUE(service.KillInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_INSTANCE_NOT_FOUND);
    EXPECT_FALSE(response.status().retryable());
    EXPECT_EQ(response.status().retryreason(), "owner-unknown");
}

TEST(FrontendProxyServiceTest, KillAuthoritativeDeletedIsOnlyAcceptedAsExplicitSuccess)
{
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.enableKillDispatch = true;
    param.killReadyDispatcher = [](const ::frontend_proxy::KillInstanceRequest &) {
        ::frontend_proxy::KillInstanceResponse response;
        response.mutable_kill()->set_code(common::ERR_NONE);
        response.mutable_kill()->set_message("authoritative owner confirmed instance already deleted");
        return litebus::Future<::frontend_proxy::KillInstanceResponse>(response);
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::KillInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("kill-authoritative-deleted");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_kill()->set_instanceid("instance-a");
    ::frontend_proxy::KillInstanceResponse response;

    EXPECT_TRUE(service.KillInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_NONE);
    EXPECT_FALSE(response.status().retryable());
    EXPECT_TRUE(response.status().retryreason().empty());
}

TEST(FrontendProxyServiceTest, KillSuccessSchedulesCleanupObservationWithoutWaiting)
{
    int probes = 0;
    auto cleanup = std::make_shared<litebus::Promise<FrontendKillCleanupSnapshot>>();
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.enableKillDispatch = true;
    param.killReadyDispatcher = [](const ::frontend_proxy::KillInstanceRequest &) {
        ::frontend_proxy::KillInstanceResponse response;
        response.mutable_kill()->set_code(common::ERR_NONE);
        return litebus::Future<::frontend_proxy::KillInstanceResponse>(response);
    };
    param.killCleanupProbe = [&probes, cleanup](const std::string &requestID, const std::string &instanceID,
                                               uint64_t delayMs) {
        ++probes;
        EXPECT_EQ(requestID, "kill-cleanup-complete");
        EXPECT_EQ(instanceID, "instance-a");
        EXPECT_EQ(delayMs, 200);
        return cleanup->GetFuture();
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::KillInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("kill-cleanup-complete");
    request.mutable_context()->set_traceid("trace-cleanup-complete");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_kill()->set_instanceid("instance-a");
    ::frontend_proxy::KillInstanceResponse response;

    EXPECT_TRUE(service.KillInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_NONE);
    EXPECT_EQ(probes, 1);
    FrontendKillCleanupSnapshot snapshot;
    snapshot.requestTicketKnown = true;
    snapshot.requestTicketCleared = true;
    snapshot.instanceTicketKnown = true;
    snapshot.instanceTicketCleared = true;
    snapshot.runtimeState = "terminated";
    snapshot.instanceState = "absent";
    cleanup->SetValue(snapshot);
}

TEST(FrontendProxyServiceTest, KillSuccessDoesNotPollIncompleteCleanupEvidence)
{
    size_t probes = 0;
    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.enableKillDispatch = true;
    param.killReadyDispatcher = [](const ::frontend_proxy::KillInstanceRequest &) {
        ::frontend_proxy::KillInstanceResponse response;
        response.mutable_kill()->set_code(common::ERR_NONE);
        return litebus::Future<::frontend_proxy::KillInstanceResponse>(response);
    };
    param.killCleanupProbe = [&probes](const std::string &, const std::string &, uint64_t) {
        ++probes;
        FrontendKillCleanupSnapshot snapshot;
        snapshot.requestTicketKnown = true;
        snapshot.requestTicketCleared = true;
        snapshot.instanceTicketKnown = true;
        snapshot.instanceTicketCleared = false;
        snapshot.runtimeState = "unknown";
        snapshot.instanceState = "exiting";
        return litebus::Future<FrontendKillCleanupSnapshot>(snapshot);
    };
    FrontendProxyService service(std::move(param));
    ::frontend_proxy::KillInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("kill-cleanup-incomplete");
    request.mutable_context()->set_traceid("trace-cleanup-incomplete");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_kill()->set_instanceid("instance-a");
    ::frontend_proxy::KillInstanceResponse response;

    EXPECT_TRUE(service.KillInstance(nullptr, &request, &response).ok());
    EXPECT_EQ(response.status().code(), common::ERR_NONE);
    EXPECT_EQ(probes, static_cast<size_t>(1));
}

TEST(FrontendProxyServiceTest, RealGrpcCancellationRemovesCreateReadyWaiter)
{
    auto pending = std::make_shared<litebus::Promise<::frontend_proxy::CreateInstanceResponse>>();
    std::mutex mutex;
    std::condition_variable entered;
    bool dispatched = false;
    std::string cancelledRequestID;
    std::string cancelReason;

    FrontendProxyServiceParam param;
    param.nodeID = "proxy-node-a";
    param.enableCreateDispatch = true;
    param.invokeResultTimeoutMs = 5000;
    param.createReadyDispatcher = [&](const ::frontend_proxy::CreateInstanceRequest &) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            dispatched = true;
        }
        entered.notify_one();
        return pending->GetFuture();
    };
    param.createWaitCanceller = [&](const std::string &requestID, const std::string &reason) {
        std::lock_guard<std::mutex> lock(mutex);
        cancelledRequestID = requestID;
        cancelReason = reason;
        entered.notify_one();
    };
    FrontendProxyService service(std::move(param));

    ::grpc::ServerBuilder builder;
    const auto socketPath = "/tmp/frontend-proxy-create-cancel-" + std::to_string(::getpid()) + ".sock";
    const auto address = "unix:" + socketPath;
    (void)::unlink(socketPath.c_str());
    builder.AddListeningPort(address, ::grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    UnixGrpcServer server(socketPath, builder.BuildAndStart());
    ASSERT_NE(server.Get(), nullptr);
    auto channel = ::grpc::CreateChannel(address, ::grpc::InsecureChannelCredentials());
    ASSERT_TRUE(channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(2)));
    auto stub = ::frontend_proxy::FrontendProxyService::NewStub(channel);
    ::frontend_proxy::CreateInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-process-a");
    request.mutable_context()->set_requestid("create-cancel-request");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_create()->set_function("0/tenant-a/faas/function");
    (*request.mutable_create()->mutable_createoptions())["source"] = "frontend";
    ::frontend_proxy::CreateInstanceResponse response;
    ::grpc::ClientContext clientContext;

    auto call = std::async(std::launch::async, [&] {
        return stub->CreateInstance(&clientContext, request, &response);
    });
    bool enteredServer = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        enteredServer = entered.wait_for(lock, std::chrono::seconds(2), [&] { return dispatched; });
    }
    EXPECT_TRUE(enteredServer);
    if (!enteredServer) {
        clientContext.TryCancel();
        (void)call.get();
        return;
    }
    clientContext.TryCancel();
    auto status = call.get();
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::CANCELLED);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(entered.wait_for(lock, std::chrono::seconds(2), [&] { return !cancelReason.empty(); }));
        EXPECT_EQ(cancelledRequestID, "create-cancel-request");
        EXPECT_EQ(cancelReason, "grpc client cancelled");
    }
}

TEST(FrontendProxyServiceTest, RealGrpcCancellationStopsInvokeWaitAndClearsRegistry)
{
    auto pending = std::make_shared<litebus::Promise<SharedStreamMsg>>();
    std::mutex mutex;
    std::condition_variable entered;
    int dispatchCount = 0;
    FrontendProxyServiceParam param;
    param.invokeResultTimeoutMs = 5000;
    param.invokeDispatcher = [&](const std::string &, const SharedStreamMsg &) {
        std::lock_guard<std::mutex> lock(mutex);
        ++dispatchCount;
        entered.notify_one();
        if (dispatchCount == 1) {
            return pending->GetFuture();
        }
        auto response = std::make_shared<runtime_rpc::StreamingMessage>();
        response->mutable_invokersp()->set_code(common::ERR_PARAM_INVALID);
        response->mutable_invokersp()->set_message("second dispatch proves registry cleanup");
        return litebus::Future<SharedStreamMsg>(response);
    };
    FrontendProxyService service(std::move(param));
    ::grpc::ServerBuilder builder;
    const auto socketPath = "/tmp/frontend-proxy-invoke-cancel-" + std::to_string(::getpid()) + ".sock";
    const auto address = "unix:" + socketPath;
    (void)::unlink(socketPath.c_str());
    builder.AddListeningPort(address, ::grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    UnixGrpcServer server(socketPath, builder.BuildAndStart());
    ASSERT_NE(server.Get(), nullptr);

    auto channel = ::grpc::CreateChannel(address, ::grpc::InsecureChannelCredentials());
    ASSERT_TRUE(channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(2)));
    auto stub = ::frontend_proxy::FrontendProxyService::NewStub(channel);
    ::frontend_proxy::InvokeInstanceRequest request;
    request.mutable_context()->set_frontendclientid("frontend-a");
    request.mutable_context()->set_requestid("invoke-cancel-request");
    request.mutable_context()->set_tenantid("tenant-a");
    request.mutable_invoke()->set_instanceid("instance-a");
    ::frontend_proxy::InvokeInstanceResponse response;
    ::grpc::ClientContext clientContext;
    auto call = std::async(std::launch::async, [&] {
        return stub->InvokeInstance(&clientContext, request, &response);
    });
    bool dispatched = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        dispatched = entered.wait_for(lock, std::chrono::seconds(2), [&] { return dispatchCount == 1; });
    }
    EXPECT_TRUE(dispatched);
    clientContext.TryCancel();
    EXPECT_EQ(call.get().error_code(), ::grpc::StatusCode::CANCELLED);

    ::frontend_proxy::InvokeInstanceResponse retryResponse;
    const auto cleanupDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        retryResponse.Clear();
        EXPECT_TRUE(service.InvokeInstance(nullptr, &request, &retryResponse).ok());
        if (dispatchCount == 2) {
            break;
        }
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < cleanupDeadline);
    EXPECT_EQ(dispatchCount, 2);
    EXPECT_EQ(retryResponse.status().message(), "second dispatch proves registry cleanup");
}



using namespace local_scheduler;
// ---------------------------------------------------------------------------
// ResizeInstance controlled entry: the scheduler-authorized direct resize.
// Per-test Promise + counter (no shared globals — the 2003Z review found
// shared state leaking across cases).
// ---------------------------------------------------------------------------
namespace {

struct ResizeTestState {
    litebus::Promise<messages::ResizeSandboxResponse> result;
    int dispatchCount = 0;

    FrontendProxyServiceParam MakeParam(
        std::function<bool(const std::string &, const std::string &)> authorizer,
        bool enable = true)
    {
        FrontendProxyServiceParam param;
        param.nodeID = "resize-node";
        param.requireAuthenticatedPeer = false;
        param.enableResizeDispatch = enable;
        param.invokeTenantAuthorizer = authorizer;
        auto *state = this;
        param.resizeReadyDispatcher =
            [state](const ::frontend_proxy::ResizeInstanceRequest &)
            -> litebus::Future<messages::ResizeSandboxResponse> {
            state->dispatchCount++;
            return state->result.GetFuture();
        };
        return param;
    }
};

::frontend_proxy::ResizeInstanceRequest MakeResizeRequest(const std::string &tenant) {
    ::frontend_proxy::ResizeInstanceRequest request;
    auto *context = request.mutable_context();
    context->set_frontendclientid("fc-1");
    context->set_requestid("req-1");
    context->set_tenantid(tenant);
    auto *resize = request.mutable_resize();
    resize->set_requestid("req-1");
    resize->set_instanceid("inst-1");
    resize->set_runtimeid("rt-1");
    auto *command = resize->mutable_resize();
    command->set_sandbox_id("sbx-1");
    command->set_resource_generation("gen-1");
    command->set_operation_id("op-1");
    command->set_deadline_unix_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count() + 30000);
    return request;
}

}  // namespace

TEST(FrontendProxyResizeEntryTest, ResizeWithoutAuthorizerIsDenied)
{
    ResizeTestState state;
    auto param = state.MakeParam(nullptr);
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("tenant-1");
    ::frontend_proxy::ResizeInstanceResponse response;
    auto status = service.ResizeInstance(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(response.status().code(), 0);
    EXPECT_EQ(state.dispatchCount, 0);
}

TEST(FrontendProxyResizeEntryTest, ResizeWithWrongTenantIsDenied)
{
    ResizeTestState state;
    auto param = state.MakeParam(
        [](const std::string &t, const std::string &) { return t == "owner"; });
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("attacker");
    ::frontend_proxy::ResizeInstanceResponse response;
    auto status = service.ResizeInstance(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(response.status().code(), 0);
    EXPECT_EQ(state.dispatchCount, 0);
}

TEST(FrontendProxyResizeEntryTest, ResizeWithSameTenantDispatchesAndObserves)
{
    ResizeTestState state;
    auto param = state.MakeParam(
        [](const std::string &t, const std::string &) { return t == "tenant-1"; });
    messages::ResizeSandboxResponse observed;
    observed.set_code(0);
    observed.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
    state.result.SetValue(observed);
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("tenant-1");
    ::frontend_proxy::ResizeInstanceResponse response;
    auto status = service.ResizeInstance(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.status().code(), 0);
    EXPECT_EQ(response.resize().outcome(), messages::ResizeSandboxResponse::OBSERVED);
    EXPECT_EQ(state.dispatchCount, 1);
}

TEST(FrontendProxyResizeEntryTest, BudgetRejectionPropagates)
{
    ResizeTestState state;
    auto param = state.MakeParam(
        [](const std::string &t, const std::string &) { return t == "tenant-1"; });
    messages::ResizeSandboxResponse rejected;
    rejected.set_code(7);
    rejected.set_outcome(messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH);
    rejected.set_message("budget exhausted");
    state.result.SetValue(rejected);
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("tenant-1");
    ::frontend_proxy::ResizeInstanceResponse response;
    auto status = service.ResizeInstance(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(response.status().code(), 0);
    EXPECT_EQ(response.resize().outcome(),
               messages::ResizeSandboxResponse::REJECTED_BEFORE_DISPATCH);
}

TEST(FrontendProxyResizeEntryTest, NotEnabledDeniesWithAuthorizeFailed)
{
    ResizeTestState state;
    auto param = state.MakeParam(
        [](const std::string &, const std::string &) { return true; }, false);
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("tenant-1");
    ::frontend_proxy::ResizeInstanceResponse response;
    auto status = service.ResizeInstance(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(response.status().code(), 0);
    EXPECT_EQ(state.dispatchCount, 0);
}

TEST(FrontendProxyResizeEntryTest, ExpiredDeadlineIsRejectedBeforeDispatch)
{
    ResizeTestState state;
    auto param = state.MakeParam(
        [](const std::string &, const std::string &) { return true; });
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("tenant-1");
    request.mutable_resize()->mutable_resize()->set_deadline_unix_ms(1);
    ::frontend_proxy::ResizeInstanceResponse response;
    auto status = service.ResizeInstance(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(response.status().code(), 0);
    EXPECT_EQ(state.dispatchCount, 0);
}

TEST(FrontendProxyResizeEntryTest, ShrinkFlagForwardedInDispatchedRequest)
{
    ResizeTestState state;
    ::frontend_proxy::ResizeInstanceRequest seen;
    auto param = state.MakeParam([](const std::string &, const std::string &) { return true; });
    param.resizeReadyDispatcher =
        [&seen, &state](const ::frontend_proxy::ResizeInstanceRequest &req)
            -> litebus::Future<messages::ResizeSandboxResponse> {
        seen = req;
        state.dispatchCount++;
        messages::ResizeSandboxResponse observed;
        observed.set_code(0);
        observed.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
        return litebus::Future<messages::ResizeSandboxResponse>(observed);
    };
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("tenant-1");
    request.mutable_resize()->set_shrink(true);
    ::frontend_proxy::ResizeInstanceResponse response;
    auto status = service.ResizeInstance(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(state.dispatchCount, 1);
    EXPECT_TRUE(seen.resize().shrink());
}

TEST(FrontendProxyResizeEntryTest, PendingFutureYieldsUnknownOutcome)
{
    ResizeTestState state;  // never SetValue: stays pending, WaitFrontendResult times out
    auto param = state.MakeParam([](const std::string &, const std::string &) { return true; });
    // force a very short deadline so the bounded wait exits quickly
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("tenant-1");
    request.mutable_resize()->mutable_resize()->set_deadline_unix_ms(
        request.resize().resize().deadline_unix_ms() - 29900); // ~100ms budget
    ::frontend_proxy::ResizeInstanceResponse response;
    auto started = std::chrono::steady_clock::now();
    auto status = service.ResizeInstance(&context, &request, &response);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started).count();
    // bounded wait: the deadline budget is ~100ms + 2s transport buffer
    // (3s floor); the handler must NOT busy-wait the full 60s default
    EXPECT_LE(elapsed, 5000) << "elapsed " << elapsed << "ms; bounded wait exceeded";
    EXPECT_TRUE(status.ok());
    EXPECT_NE(response.status().code(), 0);
    EXPECT_EQ(response.resize().outcome(), messages::ResizeSandboxResponse::UNKNOWN);
    EXPECT_EQ(state.dispatchCount, 1); // the dispatch DID happen; outcome unknown
}

// Real gRPC cancellation (synchronous stub in std::async, matching the
// proven RealGrpcCancellationStopsInvokeWait pattern): a client TryCancel
// after the dispatcher has entered its pending Future must make the handler
// exit promptly. The elapsed clock runs through the server's Shutdown+Wait
// (inner scope), proving the handler actually returned — not just that the
// client observed CANCELLED.
TEST(FrontendProxyResizeEntryTest, ClientCancellationExitsHandlerPromptly)
{
    auto pending = std::make_shared<litebus::Promise<messages::ResizeSandboxResponse>>();
    std::mutex mutex;
    std::condition_variable entered;
    int dispatchCount = 0;
    FrontendProxyServiceParam param;
    param.nodeID = "resize-cancel-node";
    param.requireAuthenticatedPeer = false;
    param.enableResizeDispatch = true;
    param.invokeTenantAuthorizer = [](const std::string &, const std::string &) { return true; };
    param.resizeReadyDispatcher =
        [&](const ::frontend_proxy::ResizeInstanceRequest &)
            -> litebus::Future<messages::ResizeSandboxResponse> {
        std::lock_guard<std::mutex> lock(mutex);
        ++dispatchCount;
        entered.notify_all();
        return pending->GetFuture(); // stays pending forever
    };
    FrontendProxyService service(std::move(param));
    auto request = MakeResizeRequest("tenant-1");
    // generous deadline: the old busy-wait would hold the handler for the
    // full ~50s budget after cancellation
    request.mutable_resize()->mutable_resize()->set_deadline_unix_ms(
        request.resize().resize().deadline_unix_ms() + 20000);
    ::grpc::Status status;
    ::frontend_proxy::ResizeInstanceResponse response;
    auto started = std::chrono::steady_clock::now();
    {
        ::grpc::ServerBuilder builder;
        const auto socketPath =
            "/tmp/frontend-proxy-resize-cancel-" + std::to_string(::getpid()) + ".sock";
        const auto address = "unix:" + socketPath;
        (void)::unlink(socketPath.c_str());
        builder.AddListeningPort(address, ::grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        UnixGrpcServer server(socketPath, builder.BuildAndStart());
        ASSERT_NE(server.Get(), nullptr);

        auto channel = ::grpc::CreateChannel(address, ::grpc::InsecureChannelCredentials());
        ASSERT_TRUE(channel->WaitForConnected(
            std::chrono::system_clock::now() + std::chrono::seconds(2)));
        auto stub = ::frontend_proxy::FrontendProxyService::NewStub(channel);
        ::grpc::ClientContext clientContext;
        auto call = std::async(std::launch::async, [&] {
            return stub->ResizeInstance(&clientContext, request, &response);
        });
        bool dispatched = false;
        {
            std::unique_lock<std::mutex> lock(mutex);
            dispatched = entered.wait_for(lock, std::chrono::seconds(3),
                                         [&] { return dispatchCount >= 1; });
        }
        ASSERT_TRUE(dispatched);
        clientContext.TryCancel();
        status = call.get();
        EXPECT_EQ(status.error_code(), ::grpc::StatusCode::CANCELLED);
        EXPECT_EQ(dispatchCount, 1);
        // the UnixGrpcServer destructor (inner scope exit) calls
        // Shutdown()+Wait(): the elapsed clock below includes the handler's
        // actual return through the server drain
    }
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started).count();
    EXPECT_LE(elapsedMs, 5000)
        << "handler held " << elapsedMs << "ms through cancel+server-drain; "
        << "must exit promptly, not busy-wait the remaining budget";
}

TEST(FrontendProxyResizeEntryTest, FailedTransportReturnsUnknownImmediately)
{
    ResizeTestState state;
    auto param = state.MakeParam([](const std::string &, const std::string &) { return true; });
    // Simulate a transport failure (litebus Promise::SetFailed): the future
    // is ready-and-failed; WaitFrontendResult must return None IMMEDIATELY
    // (the old code busy-waited on the ready-failed future for the whole
    // budget). EXPECT a fast return, not a multi-second wait.
    state.result.SetFailed(100);  // 1 aliases KINIT and trips the promise assertion
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("tenant-1");
    // give a generous deadline so the old busy-wait would take many seconds
    request.mutable_resize()->mutable_resize()->set_deadline_unix_ms(
        request.resize().resize().deadline_unix_ms() + 20000); // ~50s budget
    ::frontend_proxy::ResizeInstanceResponse response;
    auto started = std::chrono::steady_clock::now();
    auto status = service.ResizeInstance(&context, &request, &response);
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started).count();
    EXPECT_TRUE(status.ok());
    EXPECT_NE(response.status().code(), 0);
    EXPECT_EQ(response.resize().outcome(), messages::ResizeSandboxResponse::UNKNOWN);
    // the fix: a failed future returns in the first poll (~FRONTEND_WAIT_POLL_MS),
    // not after the full deadline budget
    EXPECT_LE(elapsedMs, 5000)
        << "failed transport busy-waited " << elapsedMs << "ms; must fast-exit";
    // the dispatch DID reach the invoker before the transport failed
    EXPECT_EQ(state.dispatchCount, 1);
}

TEST(FrontendProxyResizeEntryTest, ExpectedTargetRevisionForwardedInDispatch)
{
    ResizeTestState state;
    ::frontend_proxy::ResizeInstanceRequest seen;
    auto param = state.MakeParam([](const std::string &, const std::string &) { return true; });
    param.resizeReadyDispatcher =
        [&seen, &state](const ::frontend_proxy::ResizeInstanceRequest &req)
            -> litebus::Future<messages::ResizeSandboxResponse> {
        seen = req;
        state.dispatchCount++;
        messages::ResizeSandboxResponse observed;
        observed.set_code(0);
        observed.set_outcome(messages::ResizeSandboxResponse::OBSERVED);
        return litebus::Future<messages::ResizeSandboxResponse>(observed);
    };
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("tenant-1");
    // fill in real expected/target/revision — the command must round-trip
    auto *command = request.mutable_resize()->mutable_resize();
    command->mutable_expected()->set_cpu_quota_usec(50000);
    command->mutable_expected()->set_cpu_period_usec(100000);
    command->mutable_expected()->set_memory_limit_bytes(128 * 1024 * 1024);
    command->mutable_target()->set_cpu_quota_usec(50000);
    command->mutable_target()->set_cpu_period_usec(100000);
    command->mutable_target()->set_memory_limit_bytes(512 * 1024 * 1024);
    command->set_expected_revision(7);
    ::frontend_proxy::ResizeInstanceResponse response;
    auto status = service.ResizeInstance(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    ASSERT_EQ(state.dispatchCount, 1);
    EXPECT_EQ(seen.resize().resize().expected().cpu_quota_usec(), 50000);
    EXPECT_EQ(seen.resize().resize().expected().cpu_period_usec(), 100000);
    EXPECT_EQ(seen.resize().resize().expected().memory_limit_bytes(), 128 * 1024 * 1024);
    EXPECT_EQ(seen.resize().resize().target().cpu_quota_usec(), 50000);
    EXPECT_EQ(seen.resize().resize().target().cpu_period_usec(), 100000);
    EXPECT_EQ(seen.resize().resize().target().memory_limit_bytes(), 512 * 1024 * 1024);
    EXPECT_EQ(seen.resize().resize().expected_revision(), 7);
    EXPECT_EQ(seen.resize().resize().sandbox_id(), "sbx-1");
    EXPECT_EQ(seen.resize().resize().resource_generation(), "gen-1");
    EXPECT_EQ(seen.resize().resize().operation_id(), "op-1");
}

TEST(FrontendProxyResizeEntryTest, MissingOperationIDIsRejected)
{
    ResizeTestState state;
    auto param = state.MakeParam(
        [](const std::string &, const std::string &) { return true; });
    FrontendProxyService service(std::move(param));
    ::grpc::ServerContext context;
    auto request = MakeResizeRequest("tenant-1");
    request.mutable_resize()->mutable_resize()->clear_operation_id();
    ::frontend_proxy::ResizeInstanceResponse response;
    auto status = service.ResizeInstance(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_NE(response.status().code(), 0);
    EXPECT_EQ(state.dispatchCount, 0);
}

}  // namespace functionsystem::test
