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

#include <gtest/gtest.h>

#include <cstdlib>

#include "async/future.hpp"
#include "meta_store_client/key_value/etcd_kv_client_strategy.h"
#include "grpcpp/server_builder.h"
#include "mocks/mock_etcd_kv_service.h"
#include "utils/future_test_helper.h"
#include "utils/port_helper.h"

namespace functionsystem::meta_store::test {
using namespace functionsystem::test;

class KvClientTest : public ::testing::Test {
public:
    [[maybe_unused]] static void SetUpTestSuite()
    {
        etcdKvService_ = std::make_shared<MockEtcdKvService>();
        int metaStoreServerPort = functionsystem::test::FindAvailablePort();
        etcdAddress_ = "127.0.0.1:" + std::to_string(metaStoreServerPort);

        ::grpc::ServerBuilder builder;
        builder.RegisterService(etcdKvService_.get());
        builder.AddListeningPort(etcdAddress_, grpc::InsecureServerCredentials());
        etcdServer_ = builder.BuildAndStart();
        ASSERT_NE(etcdServer_, nullptr);
    }

    [[maybe_unused]] static void TearDownTestSuite()
    {
        if (etcdServer_ != nullptr) {
            etcdServer_->Shutdown();
            etcdServer_->Wait();
            etcdServer_ = nullptr;
        }
        etcdKvService_ = nullptr;
    }

public:
    inline static std::string etcdAddress_;
    inline static std::shared_ptr<grpc::Server> etcdServer_ = nullptr;
    inline static std::shared_ptr<MockEtcdKvService> etcdKvService_ = nullptr;
};

TEST_F(KvClientTest, CommitRawTest)  // NOLINT
{
    auto aid = litebus::Spawn(std::make_shared<meta_store::EtcdKvClientStrategy>(
        "EtcdKvClientStrategy", etcdAddress_, MetaStoreTimeoutOption{}, GrpcSslConfig{}));

    ::etcdserverpb::TxnResponse response;

    response.mutable_header()->set_cluster_id(100);
    response.mutable_header()->set_revision(100);
    response.set_succeeded(true);

    auto *item = response.mutable_responses()->Add();
    auto *raw = item->mutable_response_delete_range();
    raw->mutable_header()->set_cluster_id(100);
    raw->mutable_header()->set_revision(100);
    raw->set_deleted(1);
    auto *kv = raw->mutable_prev_kvs()->Add();
    kv->set_key("mock-key");
    kv->set_key("mock-value");

    EXPECT_CALL(*etcdKvService_, Txn)
        .WillOnce(testing::DoAll(testing::SetArgPointee<2>(response), testing::Return(::grpc::Status::OK)));

    ::etcdserverpb::TxnRequest request;
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::CommitRaw, request);
    EXPECT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get()->succeeded());
    EXPECT_EQ(future.Get()->responses().size(), 1);

    litebus::Terminate(aid);
    litebus::Await(aid);
}

static etcdserverpb::RangeResponse MakePrefixPage(int64_t revision, int64_t count, bool more,
                                                 const std::vector<std::string> &keys)
{
    etcdserverpb::RangeResponse page;
    page.mutable_header()->set_cluster_id(100);
    page.mutable_header()->set_revision(revision);
    page.set_count(count);
    page.set_more(more);
    for (const auto &key : keys) {
        auto kv = page.add_kvs();
        kv->set_key(key);
        kv->set_value("value");
    }
    return page;
}

static std::shared_ptr<EtcdKvClientStrategy> MakePrefixClient(const std::string &address, uint32_t pageSize = 2,
                                                            uint32_t timeoutSeconds = 60)
{
    MetaStoreTimeoutOption option;
    option.operationRetryTimes = 1;
    option.prefixReadPageSize = pageSize;
    option.prefixReadTimeoutSeconds = timeoutSeconds;
    return std::make_shared<EtcdKvClientStrategy>(
        "prefix-read-" + litebus::uuid_generator::UUID::GetRandomUUID().ToString(), address, option,
        GrpcSslConfig{}, "/sn");
}

TEST_F(KvClientTest, UnlimitedPrefixUsesFixedRevisionAndFixedEnd)
{
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    testing::InSequence sequence;
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::Invoke([](grpc::ServerContext *, const etcdserverpb::RangeRequest *request,
                                     etcdserverpb::RangeResponse *response) {
            EXPECT_EQ(request->key(), "/sn/p/");
            EXPECT_EQ(request->range_end(), "/sn/p0");
            EXPECT_EQ(request->limit(), 2);
            EXPECT_EQ(request->revision(), 0);
            EXPECT_EQ(request->sort_order(), etcdserverpb::RangeRequest::NONE);
            *response = MakePrefixPage(10, 3, true, {"/sn/p/a", "/sn/p/b"});
            return grpc::Status::OK;
        }))
        .WillOnce(testing::Invoke([](grpc::ServerContext *, const etcdserverpb::RangeRequest *request,
                                     etcdserverpb::RangeResponse *response) {
            EXPECT_EQ(request->key(), std::string("/sn/p/b") + '\0');
            EXPECT_EQ(request->range_end(), "/sn/p0");
            EXPECT_EQ(request->limit(), 2);
            EXPECT_EQ(request->revision(), 10);
            *response = MakePrefixPage(12, 1, false, {"/sn/p/c"});
            return grpc::Status::OK;
        }));
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::Get, "/p/", GetOption{ .prefix = true });
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get()->status.IsOk());
    EXPECT_EQ(future.Get()->header.revision, 10);
    EXPECT_EQ(future.Get()->count, 3);
    ASSERT_EQ(future.Get()->kvs.size(), 3);
    EXPECT_EQ(future.Get()->kvs.back().key(), "/sn/p/c");
    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(KvClientTest, CompactedPageDiscardsWholeSnapshot)
{
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::DoAll(testing::SetArgPointee<2>(MakePrefixPage(10, 3, true, {"/sn/p/a", "/sn/p/b"})),
                                testing::Return(grpc::Status::OK)))
        .WillOnce(testing::Return(grpc::Status(grpc::StatusCode::OUT_OF_RANGE, "required revision has been compacted")));
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::Get, "/p/", GetOption{ .prefix = true });
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get()->status.IsError());
    EXPECT_TRUE(future.Get()->kvs.empty());
    EXPECT_EQ(future.Get()->header.revision, 0);
    EXPECT_EQ(future.Get()->count, 0);
    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(KvClientTest, ExplicitLimitRemainsSingleRequest)
{
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::Invoke([](grpc::ServerContext *, const etcdserverpb::RangeRequest *request,
                                     etcdserverpb::RangeResponse *response) {
            EXPECT_EQ(request->limit(), 1);
            *response = MakePrefixPage(10, 3, true, {"/sn/p/a"});
            return grpc::Status::OK;
        }));
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::Get, "/p/", GetOption{ .prefix = true, .limit = 1 });
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get()->status.IsOk());
    EXPECT_EQ(future.Get()->kvs.size(), 1);
    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(KvClientTest, FailedInitialSnapshotDoesNotPublishOrWatch)
{
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::Return(grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "denied")));
    std::atomic<int> observed = 0;
    ObserverFunction observer = [&observed](const std::vector<WatchEvent> &, bool) {
        ++observed;
        return true;
    };
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::GetAndWatch, "/p/",
                                 WatchOption{ .prefix = true }, observer, SyncerFunction{},
                                 std::shared_ptr<WatchRecord>{});
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get(), nullptr);
    EXPECT_EQ(observed, 0);
    EXPECT_TRUE(client->GetRecords().empty());
    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(KvClientTest, CountOnlyRemainsSingleRequest)
{
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::Invoke([](grpc::ServerContext *, const etcdserverpb::RangeRequest *request,
                                     etcdserverpb::RangeResponse *response) {
            EXPECT_TRUE(request->count_only());
            EXPECT_EQ(request->limit(), 0);
            *response = MakePrefixPage(10, 3, false, {});
            return grpc::Status::OK;
        }));
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::Get, "/p/",
                                 GetOption{ .prefix = true, .countOnly = true });
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get()->status.IsOk());
    EXPECT_EQ(future.Get()->count, 3);
    EXPECT_TRUE(future.Get()->kvs.empty());
    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(KvClientTest, OverallDeadlineRejectsSlowPage)
{
    auto client = MakePrefixClient(etcdAddress_, 2, 1);
    auto aid = litebus::Spawn(client);
    litebus::Promise<bool> finished;
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::Invoke([&finished](grpc::ServerContext *, const etcdserverpb::RangeRequest *,
                                             etcdserverpb::RangeResponse *response) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1200));
            *response = MakePrefixPage(10, 1, false, {"/sn/p/a"});
            finished.SetValue(true);
            return grpc::Status::OK;
        }));
    auto started = std::chrono::steady_clock::now();
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::Get, "/p/", GetOption{ .prefix = true });
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get()->status.StatusCode(), GRPC_DEADLINE_EXCEEDED);
    EXPECT_TRUE(future.Get()->kvs.empty());
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(3));
    litebus::Terminate(aid);
    litebus::Await(aid);
    ASSERT_AWAIT_READY(finished.GetFuture());
}

TEST_F(KvClientTest, EmptyPrefixSnapshotIsComplete)
{
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::DoAll(testing::SetArgPointee<2>(MakePrefixPage(10, 0, false, {})),
                                testing::Return(grpc::Status::OK)));
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::Get, "/p/", GetOption{ .prefix = true });
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get()->status.IsOk());
    EXPECT_TRUE(future.Get()->kvs.empty());
    EXPECT_EQ(future.Get()->header.revision, 10);
    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(KvClientTest, InvalidPageBudgetDoesNotSendUnlimitedRange)
{
    auto client = MakePrefixClient(etcdAddress_, 0);
    auto aid = litebus::Spawn(client);
    EXPECT_CALL(*etcdKvService_, Range).Times(0);
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::Get, "/p/", GetOption{ .prefix = true });
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get()->status.StatusCode(), PARAMETER_ERROR);
    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(KvClientTest, IncompleteCountDoesNotPublishPartialSnapshot)
{
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::DoAll(testing::SetArgPointee<2>(MakePrefixPage(10, 2, false, {"/sn/p/a"})),
                                testing::Return(grpc::Status::OK)));
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::Get, "/p/", GetOption{ .prefix = true });
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get()->status.IsError());
    EXPECT_TRUE(future.Get()->kvs.empty());
    EXPECT_EQ(future.Get()->header.revision, 0);
    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(KvClientTest, InitialObserverReceivesOnlyCompleteSnapshot)
{
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::DoAll(testing::SetArgPointee<2>(MakePrefixPage(10, 3, true, {"/sn/p/a", "/sn/p/b"})),
                                testing::Return(grpc::Status::OK)))
        .WillOnce(testing::DoAll(testing::SetArgPointee<2>(MakePrefixPage(12, 1, false, {"/sn/p/c"})),
                                testing::Return(grpc::Status::OK)));
    std::atomic<int> observed = 0;
    ObserverFunction observer = [&observed](const std::vector<WatchEvent> &events, bool synced) {
        ++observed;
        EXPECT_TRUE(synced);
        EXPECT_EQ(events.size(), 3);
        return false;  // A rejected snapshot must not establish a watch.
    };
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::GetAndWatch, "/p/",
                                 WatchOption{ .prefix = true }, observer, SyncerFunction{},
                                 std::shared_ptr<WatchRecord>{});
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get(), nullptr);
    EXPECT_EQ(observed, 1);
    EXPECT_TRUE(client->GetRecords().empty());
    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(KvClientTest, SuccessfulHandlerCannotHideFailedSnapshot)
{
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::Return(grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "denied")));
    std::atomic<int> handled = 0;
    std::atomic<int> observed = 0;
    GetAndWatchHandlers handlers;
    handlers.responseHandler = [&handled](const std::shared_ptr<GetResponse> &response) -> litebus::Future<Status> {
        ++handled;
        EXPECT_TRUE(response->status.IsError());
        return Status::OK();
    };
    handlers.observer = [&observed](const std::vector<WatchEvent> &, bool) {
        ++observed;
        return true;
    };
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::GetAndWatchWithHandler, "/p/",
                                 WatchOption{ .prefix = true }, handlers, std::shared_ptr<WatchRecord>{});
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get(), nullptr);
    EXPECT_EQ(handled, 1);
    EXPECT_EQ(observed, 0);
    litebus::Terminate(aid);
    litebus::Await(aid);
}

TEST_F(KvClientTest, TerminatingClientCompletesPendingSnapshot)
{
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    litebus::Promise<bool> entered;
    EXPECT_CALL(*etcdKvService_, Range)
        .WillOnce(testing::Invoke([&entered](grpc::ServerContext *, const etcdserverpb::RangeRequest *,
                                              etcdserverpb::RangeResponse *response) {
            entered.SetValue(true);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            *response = MakePrefixPage(10, 1, false, {"/sn/p/a"});
            return grpc::Status::OK;
        }));
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::Get, "/p/", GetOption{ .prefix = true });
    ASSERT_AWAIT_READY(entered.GetFuture());
    litebus::Terminate(aid);
    litebus::Await(aid);
    ASSERT_AWAIT_READY(future);
    EXPECT_EQ(future.Get()->status.StatusCode(), GRPC_CANCELLED);
    EXPECT_TRUE(future.Get()->kvs.empty());
    // Keep the fixture alive until its server callback has returned.
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
}

TEST_F(KvClientTest, RealEtcdKeepsSnapshotWhenWritesOccurBetweenPages)
{
    const auto endpoint = std::getenv("ETCD_PREFIX_READ_TEST_ENDPOINT");
    if (endpoint == nullptr) {
        GTEST_SKIP() << "set ETCD_PREFIX_READ_TEST_ENDPOINT to an isolated loopback etcd";
    }
    ASSERT_EQ(std::string(endpoint).find("127.0.0.1:"), 0);
    auto stub = etcdserverpb::KV::NewStub(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));
    auto prefix = "/snapshot-" + litebus::uuid_generator::UUID::GetRandomUUID().ToString() + "/";
    auto realPrefix = "/sn" + prefix;
    auto put = [&](const std::string &key, const std::string &value) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        etcdserverpb::PutRequest request;
        request.set_key(realPrefix + key);
        request.set_value(value);
        etcdserverpb::PutResponse response;
        return stub->Put(&context, request, &response);
    };
    ASSERT_TRUE(put("a", "before").ok());
    ASSERT_TRUE(put("b", "before").ok());
    ASSERT_TRUE(put("c", "before").ok());
    int pages = 0;
    int64_t revision = 0;
    EXPECT_CALL(*etcdKvService_, Range)
        .Times(2)
        .WillRepeatedly(testing::Invoke([&](grpc::ServerContext *, const etcdserverpb::RangeRequest *request,
                                            etcdserverpb::RangeResponse *response) {
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            auto status = stub->Range(&context, *request, response);
            if (++pages == 1 && status.ok()) {
                revision = response->header().revision();
                EXPECT_TRUE(put("b", "after").ok());
                EXPECT_TRUE(put("d", "after").ok());
                grpc::ClientContext deleteContext;
                deleteContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                etcdserverpb::DeleteRangeRequest deletion;
                deletion.set_key(realPrefix + "c");
                etcdserverpb::DeleteRangeResponse deleted;
                EXPECT_TRUE(stub->DeleteRange(&deleteContext, deletion, &deleted).ok());
            } else {
                EXPECT_EQ(request->revision(), revision);
            }
            return status;
        }));
    auto client = MakePrefixClient(etcdAddress_);
    auto aid = litebus::Spawn(client);
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::Get, prefix, GetOption{ .prefix = true });
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get()->status.IsOk());
    EXPECT_EQ(future.Get()->header.revision, revision);
    EXPECT_EQ(future.Get()->count, 3);
    EXPECT_EQ(future.Get()->kvs.size(), 3);
    for (size_t i = 0; i < future.Get()->kvs.size(); ++i) {
        EXPECT_EQ(future.Get()->kvs[i].key(), realPrefix + std::string(1, 'a' + i));
        EXPECT_EQ(future.Get()->kvs[i].value(), "before");
    }
    litebus::Terminate(aid);
    litebus::Await(aid);
    grpc::ClientContext cleanupContext;
    cleanupContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    etcdserverpb::DeleteRangeRequest cleanup;
    cleanup.set_key(realPrefix);
    cleanup.set_range_end(realPrefix.substr(0, realPrefix.size() - 1) + "0");
    etcdserverpb::DeleteRangeResponse deleted;
    EXPECT_TRUE(stub->DeleteRange(&cleanupContext, cleanup, &deleted).ok());
}

TEST_F(KvClientTest, RealEtcdReplaysWriteBetweenSnapshotAndWatch)
{
    const auto endpoint = std::getenv("ETCD_PREFIX_READ_TEST_ENDPOINT");
    if (endpoint == nullptr) {
        GTEST_SKIP() << "set ETCD_PREFIX_READ_TEST_ENDPOINT to an isolated loopback etcd";
    }
    ASSERT_EQ(std::string(endpoint).find("127.0.0.1:"), 0);
    auto stub = etcdserverpb::KV::NewStub(grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));
    auto prefix = "/watch-snapshot-" + litebus::uuid_generator::UUID::GetRandomUUID().ToString() + "/";
    auto realPrefix = "/sn" + prefix;
    auto put = [&](const std::string &key, const std::string &value) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        etcdserverpb::PutRequest request;
        request.set_key(realPrefix + key);
        request.set_value(value);
        etcdserverpb::PutResponse response;
        return stub->Put(&context, request, &response);
    };
    ASSERT_TRUE(put("a", "before").ok());
    ASSERT_TRUE(put("b", "before").ok());
    ASSERT_TRUE(put("c", "before").ok());
    litebus::Promise<bool> replayed;
    ObserverFunction observer = [&](const std::vector<WatchEvent> &events, bool synced) {
        if (synced) {
            EXPECT_EQ(events.size(), 3);
            // This write completes after the snapshot and before Watch is queued.
            EXPECT_TRUE(put("b", "after").ok());
        } else {
            for (const auto &event : events) {
                EXPECT_EQ(event.eventType, EVENT_TYPE_PUT);
                EXPECT_EQ(event.kv.key(), realPrefix + "b");
                EXPECT_EQ(event.kv.value(), "after");
                replayed.SetValue(true);
            }
        }
        return true;
    };
    auto client = MakePrefixClient(endpoint);
    auto aid = litebus::Spawn(client);
    auto future = litebus::Async(aid, &EtcdKvClientStrategy::GetAndWatch, prefix,
                                 WatchOption{ .prefix = true }, observer, SyncerFunction{},
                                 std::shared_ptr<WatchRecord>{});
    EXPECT_AWAIT_READY_FOR(future, 5000);
    if (future.IsOK()) {
        EXPECT_NE(future.Get(), nullptr);
    }
    EXPECT_AWAIT_READY_FOR(replayed.GetFuture(), 5000);
    litebus::Terminate(aid);
    litebus::Await(aid);
    grpc::ClientContext cleanupContext;
    cleanupContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    etcdserverpb::DeleteRangeRequest cleanup;
    cleanup.set_key(realPrefix);
    cleanup.set_range_end(realPrefix.substr(0, realPrefix.size() - 1) + "0");
    etcdserverpb::DeleteRangeResponse deleted;
    EXPECT_TRUE(stub->DeleteRange(&cleanupContext, cleanup, &deleted).ok());
}
}  // namespace functionsystem::meta_store::test
