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

#include "meta_storage_accessor/meta_storage_accessor.h"

#include <gtest/gtest.h>

#include <condition_variable>
#include <memory>
#include <mutex>

#include "mocks/mock_meta_store_client.h"
#include "utils/port_helper.h"

namespace functionsystem::test {

using ::testing::Return;

class MetaStorageAccessorTest : public ::testing::Test {
public:
    void TearDown() override
    {
        // litebus::TerminateAll();
    }
};

class LeaseActorTestPeer : public LeaseActor {
public:
    using LeaseActor::LeaseActor;

    bool Retry(const std::string &key, const std::string &value, int ttl, int64_t expectedLeaseID)
    {
        RetryPutWithLease(key, value, ttl, expectedLeaseID);
        return true;
    }
};

TEST_F(MetaStorageAccessorTest, FailedSnapshotIsNotAnEmptySuccess)
{
    auto client = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");
    auto response = std::make_shared<GetResponse>();
    response->status = Status(GRPC_UNAVAILABLE, "unavailable");
    response->header.revision = 10;
    response->kvs.emplace_back();
    EXPECT_CALL(*client, Get).WillOnce(Return(response));
    MetaStorageAccessor accessor(client);
    auto result = accessor.SyncWithStatus("/instances/", true);
    EXPECT_EQ(result.status.StatusCode(), GRPC_UNAVAILABLE);
    EXPECT_TRUE(result.events.empty());
    EXPECT_EQ(result.revision, 0);
}

TEST_F(MetaStorageAccessorTest, CompleteEmptySnapshotPreservesRevision)
{
    auto client = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");
    auto response = std::make_shared<GetResponse>();
    response->header.revision = 10;
    EXPECT_CALL(*client, Get).WillOnce(Return(response));
    MetaStorageAccessor accessor(client);
    auto result = accessor.SyncWithStatus("/instances/", true);
    EXPECT_TRUE(result.status.IsOk());
    EXPECT_TRUE(result.events.empty());
    EXPECT_EQ(result.revision, 10);
}

TEST_F(MetaStorageAccessorTest, MissingSnapshotPreservesFailure)
{
    auto client = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");
    EXPECT_CALL(*client, Get).WillOnce(Return(std::shared_ptr<GetResponse>{}));
    MetaStorageAccessor accessor(client);
    auto result = accessor.SyncWithStatus("/instances/", true);
    EXPECT_TRUE(result.status.IsError());
    EXPECT_TRUE(result.events.empty());
    EXPECT_EQ(result.revision, 0);
}

TEST_F(MetaStorageAccessorTest, FailedSnapshotFuturePreservesFailure)
{
    auto client = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");
    litebus::Future<std::shared_ptr<GetResponse>> future;
    future.SetFailed(FAILED);
    EXPECT_CALL(*client, Get).WillOnce(Return(future));
    MetaStorageAccessor accessor(client);
    auto result = accessor.SyncWithStatus("/instances/", true);
    EXPECT_TRUE(result.status.IsError());
    EXPECT_TRUE(result.events.empty());
    EXPECT_EQ(result.revision, 0);
}

static void MockMetaStoreClientPutWithLease(std::unique_ptr<MockMetaStoreClient> &mockMetaStoreClient,
                                            const std::string &key, const std::string &value, const int64_t leaseID,
                                            const int ttl)
{
    auto putResponse = std::make_shared<PutResponse>();
    putResponse->prevKv.set_key(key);
    putResponse->prevKv.set_value(value);

    EXPECT_CALL(*mockMetaStoreClient, Put).WillOnce(Return(putResponse));
    litebus::Future<LeaseGrantResponse> leaseGrantResponseFuture;
    leaseGrantResponseFuture.SetValue({ Status::OK(), ResponseHeader(), leaseID, ttl });
    EXPECT_CALL(*mockMetaStoreClient, Grant).WillOnce(Return(leaseGrantResponseFuture));
    litebus::Future<LeaseKeepAliveResponse> leaseKeepAliveResponseFuture;
    leaseKeepAliveResponseFuture.SetValue({ Status::OK(), ResponseHeader(), leaseID, ttl });
    EXPECT_CALL(*mockMetaStoreClient, KeepAliveOnce).WillRepeatedly(Return(leaseKeepAliveResponseFuture));
}

static void MockMetaStoreClientPutWithLeaseTimeout(std::unique_ptr<MockMetaStoreClient> &mockMetaStoreClient,
                                                   const std::string &key, const std::string &value,
                                                   const int64_t leaseID1, const int64_t leaseID2, const int ttl)
{
    auto putResponse = std::make_shared<PutResponse>();
    putResponse->prevKv.set_key(key);
    putResponse->prevKv.set_value(value);

    EXPECT_CALL(*mockMetaStoreClient, Put).WillOnce(Return(putResponse)).WillOnce(Return(putResponse));
    litebus::Future<LeaseGrantResponse> leaseGrantResponseFuture1;
    leaseGrantResponseFuture1.SetValue({ Status::OK(), ResponseHeader(), leaseID1, ttl });
    litebus::Future<LeaseGrantResponse> leaseGrantResponseFuture2;
    leaseGrantResponseFuture2.SetValue({ Status::OK(), ResponseHeader(), leaseID2, ttl });
    EXPECT_CALL(*mockMetaStoreClient, Grant)
        .WillOnce(Return(leaseGrantResponseFuture1))
        .WillOnce(Return(leaseGrantResponseFuture2));
    litebus::Future<LeaseKeepAliveResponse> leaseKeepAliveResponseFutureSuccess1;
    leaseKeepAliveResponseFutureSuccess1.SetValue({ Status::OK(), ResponseHeader(), leaseID1, ttl });
    litebus::Future<LeaseKeepAliveResponse> leaseKeepAliveResponseFutureSuccess2;
    leaseKeepAliveResponseFutureSuccess2.SetValue({ Status::OK(), ResponseHeader(), leaseID2, ttl });
    litebus::Future<LeaseKeepAliveResponse> leaseKeepAliveResponseFutureFailed;
    leaseKeepAliveResponseFutureFailed.SetValue({ Status::OK(), ResponseHeader(), leaseID1, 0 });
    EXPECT_CALL(*mockMetaStoreClient, KeepAliveOnce)
        .WillOnce(Return(leaseKeepAliveResponseFutureSuccess1))
        .WillOnce(Return(leaseKeepAliveResponseFutureSuccess1))
        .WillOnce(Return(leaseKeepAliveResponseFutureFailed))
        .WillRepeatedly(Return(leaseKeepAliveResponseFutureSuccess2));
}

TEST_F(MetaStorageAccessorTest, PutWithoutLease)
{
    const std::string testKey = "test key";
    const std::string testValue = "test value";

    uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
    std::string address = "127.0.0.1:" + std::to_string(port);
    std::unique_ptr<MockMetaStoreClient> mockMetaStoreClient = std::make_unique<MockMetaStoreClient>(address);

    auto putResponse = std::make_shared<PutResponse>();
    putResponse->prevKv.set_key(testKey);
    putResponse->prevKv.set_value(testValue);

    EXPECT_CALL(*mockMetaStoreClient, Put).WillOnce(Return(putResponse));

    MetaStorageAccessor accessor{ std::move(mockMetaStoreClient) };

    auto status = accessor.Put(testKey, testValue).Get();
    EXPECT_TRUE(status.IsOk());
}

TEST_F(MetaStorageAccessorTest, PutWithLease)
{
    const std::string testKey = "testKey";
    const std::string testValue = "testValue";
    const int64_t leaseID = 1;
    const int ttl = 100;

    uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
    std::string address = "127.0.0.1:" + std::to_string(port);
    std::unique_ptr<MockMetaStoreClient> mockMetaStoreClient = std::make_unique<MockMetaStoreClient>(address);

    MockMetaStoreClientPutWithLease(mockMetaStoreClient, testKey, testValue, leaseID, ttl);

    MetaStorageAccessor accessor{ std::move(mockMetaStoreClient) };
    auto status = accessor.PutWithLease(testKey, testValue, ttl).Get();
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    EXPECT_TRUE(status.IsOk());
}

TEST_F(MetaStorageAccessorTest, PutWithLeaseTimeout)
{
    const std::string testKey = "testKey";
    const std::string testValue = "testValue";
    const int64_t leaseID1 = 1;
    const int64_t leaseID2 = 2;
    const int ttl = 100;

    uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
    std::string address = "127.0.0.1:" + std::to_string(port);
    std::unique_ptr<MockMetaStoreClient> mockMetaStoreClient = std::make_unique<MockMetaStoreClient>(address);

    MockMetaStoreClientPutWithLeaseTimeout(mockMetaStoreClient, testKey, testValue, leaseID1, leaseID2, ttl);

    MetaStorageAccessor accessor{ std::move(mockMetaStoreClient) };
    auto status = accessor.PutWithLease(testKey, testValue, ttl).Get();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_TRUE(status.IsOk());
}

TEST_F(MetaStorageAccessorTest, Get)
{
    const std::string testKey = "test key";
    const std::string testValue = "test value";

    uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
    std::string address = "127.0.0.1:" + std::to_string(port);
    std::unique_ptr<MockMetaStoreClient> mockMetaStoreClient = std::make_unique<MockMetaStoreClient>(address);

    litebus::Future<std::shared_ptr<GetResponse>> getResponseFuture;
    KeyValue kv;
    kv.set_key(testKey);
    kv.set_value(testValue);
    auto output = std::make_shared<GetResponse>();
    output->status = Status::OK();
    output->header = ResponseHeader();
    output->count = 1;
    output->kvs = { kv };
    getResponseFuture.SetValue(output);
    EXPECT_CALL(*mockMetaStoreClient, Get).WillOnce(Return(getResponseFuture));

    MetaStorageAccessor accessor{ std::move(mockMetaStoreClient) };
    auto result = accessor.Get(testKey);
    EXPECT_EQ(result.Get(), testValue);
}

TEST_F(MetaStorageAccessorTest, Delete)
{
    uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
    std::string address = "127.0.0.1:" + std::to_string(port);
    std::unique_ptr<MockMetaStoreClient> mockMetaStoreClient = std::make_unique<MockMetaStoreClient>(address);

    auto deleteResponse = std::make_shared<DeleteResponse>();
    EXPECT_CALL(*mockMetaStoreClient, Delete).WillOnce(Return(deleteResponse));

    MetaStorageAccessor accessor{ std::move(mockMetaStoreClient) };
    auto status = accessor.Delete("key").Get();
    EXPECT_TRUE(status.IsOk());
}

TEST_F(MetaStorageAccessorTest, RevokeInvalidKey)
{
    uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
    std::string address = "127.0.0.1:" + std::to_string(port);
    std::unique_ptr<MockMetaStoreClient> mockMetaStoreClient = std::make_unique<MockMetaStoreClient>(address);

    MetaStorageAccessor accessor{ std::move(mockMetaStoreClient) };
    auto status = accessor.Revoke("key").Get();
    EXPECT_EQ(status.StatusCode(), StatusCode::BP_LEASE_ID_NOT_FOUND);
}

TEST_F(MetaStorageAccessorTest, RevokeValidKey)
{
    const std::string testKey = "test key";
    const std::string testValue = "test value";
    const int64_t leaseID = 1;
    const int ttl = 100;

    uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
    std::string address = "127.0.0.1:" + std::to_string(port);
    std::unique_ptr<MockMetaStoreClient> mockMetaStoreClient = std::make_unique<MockMetaStoreClient>(address);

    MockMetaStoreClientPutWithLease(mockMetaStoreClient, testKey, testValue, leaseID, ttl);

    litebus::Future<LeaseRevokeResponse> leaseRevokeResponseFuture;
    leaseRevokeResponseFuture.SetValue({});
    EXPECT_CALL(*mockMetaStoreClient, Revoke).WillOnce(Return(leaseRevokeResponseFuture));

    MetaStorageAccessor accessor{ std::move(mockMetaStoreClient) };
    auto status = accessor.PutWithLease(testKey, testValue, ttl).Get();
    EXPECT_TRUE(status.IsOk());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto result = accessor.Revoke(testKey).Get();
    EXPECT_TRUE(result.IsOk());
}

TEST_F(MetaStorageAccessorTest, RevokeCancelsInitialKeepAliveTimer)
{
    const std::string testKey = "test key";
    const std::string testValue = "test value";
    const int64_t leaseID = 1;
    const int ttl = 600;

    uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
    std::string address = "127.0.0.1:" + std::to_string(port);
    std::unique_ptr<MockMetaStoreClient> mockMetaStoreClient = std::make_unique<MockMetaStoreClient>(address);

    auto putResponse = std::make_shared<PutResponse>();
    EXPECT_CALL(*mockMetaStoreClient, Put).WillOnce(Return(putResponse));
    litebus::Future<LeaseGrantResponse> leaseGrantResponseFuture;
    leaseGrantResponseFuture.SetValue({ Status::OK(), ResponseHeader(), leaseID, ttl });
    EXPECT_CALL(*mockMetaStoreClient, Grant).WillOnce(Return(leaseGrantResponseFuture));
    EXPECT_CALL(*mockMetaStoreClient, KeepAliveOnce).Times(0);

    litebus::Future<LeaseRevokeResponse> leaseRevokeResponseFuture;
    leaseRevokeResponseFuture.SetValue({});
    EXPECT_CALL(*mockMetaStoreClient, Revoke).WillOnce(Return(leaseRevokeResponseFuture));

    MetaStorageAccessor accessor{ std::move(mockMetaStoreClient) };
    EXPECT_TRUE(accessor.PutWithLease(testKey, testValue, ttl).Get().IsOk());
    EXPECT_TRUE(accessor.Revoke(testKey).Get().IsOk());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

TEST_F(MetaStorageAccessorTest, RevokeInvalidatesInFlightKeepAliveResponse)
{
    const std::string testKey = "in-flight-key";
    const std::string testValue = "old-value";
    const int64_t leaseID = 1;
    const int ttl = 100;

    uint16_t port = GetPortEnv("LITEBUS_PORT", 8080);
    std::string address = "127.0.0.1:" + std::to_string(port);
    std::unique_ptr<MockMetaStoreClient> mockMetaStoreClient = std::make_unique<MockMetaStoreClient>(address);

    auto putResponse = std::make_shared<PutResponse>();
    EXPECT_CALL(*mockMetaStoreClient, Put).Times(1).WillOnce(Return(putResponse));
    litebus::Future<LeaseGrantResponse> leaseGrantResponseFuture;
    leaseGrantResponseFuture.SetValue({ Status::OK(), ResponseHeader(), leaseID, ttl });
    EXPECT_CALL(*mockMetaStoreClient, Grant).Times(1).WillOnce(Return(leaseGrantResponseFuture));

    litebus::Promise<LeaseKeepAliveResponse> keepAlivePromise;
    std::mutex keepAliveMutex;
    std::condition_variable keepAliveStarted;
    bool started = false;
    EXPECT_CALL(*mockMetaStoreClient, KeepAliveOnce(leaseID))
        .WillOnce([&](int64_t) {
            {
                std::lock_guard<std::mutex> lock(keepAliveMutex);
                started = true;
            }
            keepAliveStarted.notify_one();
            return keepAlivePromise.GetFuture();
        });

    litebus::Future<LeaseRevokeResponse> leaseRevokeResponseFuture;
    leaseRevokeResponseFuture.SetValue({});
    EXPECT_CALL(*mockMetaStoreClient, Revoke(leaseID)).WillOnce(Return(leaseRevokeResponseFuture));

    MetaStorageAccessor accessor{ std::move(mockMetaStoreClient) };
    ASSERT_TRUE(accessor.PutWithLease(testKey, testValue, ttl).Get().IsOk());
    {
        std::unique_lock<std::mutex> lock(keepAliveMutex);
        ASSERT_TRUE(keepAliveStarted.wait_for(lock, std::chrono::seconds(1), [&] { return started; }));
    }
    ASSERT_TRUE(accessor.Revoke(testKey).Get().IsOk());
    keepAlivePromise.SetValue({ Status::OK(), ResponseHeader(), leaseID, 0 });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

TEST_F(MetaStorageAccessorTest, StaleRetryDoesNotReplaceCurrentLease)
{
    const std::string testKey = "generation-key";
    const std::string currentValue = "current-value";
    const int64_t oldLeaseID = 1;
    const int64_t currentLeaseID = 2;
    const int ttl = 600000;
    auto mockMetaStoreClient = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");

    litebus::Future<LeaseGrantResponse> grantResponse;
    grantResponse.SetValue({ Status::OK(), ResponseHeader(), currentLeaseID, ttl });
    EXPECT_CALL(*mockMetaStoreClient, Grant(ttl / 1000))
        .Times(1)
        .WillOnce(Return(grantResponse));
    auto putResponse = std::make_shared<PutResponse>();
    EXPECT_CALL(*mockMetaStoreClient, Put(testKey, currentValue, ::testing::_))
        .Times(1)
        .WillOnce(Return(putResponse));
    EXPECT_CALL(*mockMetaStoreClient, KeepAliveOnce).Times(0);

    auto actor = std::make_shared<LeaseActorTestPeer>("lease-actor-generation-test", mockMetaStoreClient);
    litebus::Spawn(actor);
    ASSERT_TRUE(
        litebus::Async(actor->GetAID(), &LeaseActorTestPeer::PutWithLease, testKey, currentValue, ttl).Get().IsOk());
    EXPECT_TRUE(litebus::Async(actor->GetAID(), &LeaseActorTestPeer::Retry, testKey, "stale-value", ttl, oldLeaseID)
                    .Get());
    litebus::Terminate(actor->GetAID());
    litebus::Await(actor->GetAID());
}

}  // namespace functionsystem::test
