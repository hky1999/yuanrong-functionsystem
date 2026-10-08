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

// Ownership tests for the Traefik route registry (#3 of the atomic-
// ownership execution-point plan): registration records the registering
// incarnation; an identity-bearing unregister refuses (ZERO accessor
// prefix-deletes) when the recorded incarnation differs, so a late old
// chain cannot strip a same-name new generation's route; the legacy
// single-arg unregister stays unchecked (compat), and a never-recorded
// name proceeds unchecked.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "function_proxy/local_scheduler/traefik_registry/traefik_registry.h"
#include "mocks/mock_meta_storage_accessor.h"
#include "mocks/mock_meta_store_client.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {
namespace {

using testing::_;
using testing::Invoke;
using testing::Matcher;
using testing::Return;

class TraefikRegistryFixture : public ::testing::Test {
protected:
    void SetUp() override
    {
        accessor_ = std::make_shared<MockMetaStorageAccessor>(
            std::make_shared<MockMetaStoreClient>("127.0.0.1:1"));
        // The ctor publishes the global middleware; registrations Txn their
        // route keys; unregisters prefix-delete router + service keys.
        ON_CALL(*accessor_, Put(_, _)).WillByDefault(Return(Status::OK()));
        ON_CALL(*accessor_, Txn(_)).WillByDefault(Return(Status::OK()));
        ON_CALL(*accessor_, Delete(Matcher<const std::string &>(_), Matcher<bool>(_)))
            .WillByDefault(Invoke([this](const std::string &, bool) {
                prefixDeletes_->fetch_add(1);
                return Status::OK();
            }));
        registry_ = std::make_shared<local_scheduler::TraefikRegistry>(
            accessor_, "yr/traefik-ut", "web", false, "");
    }

    std::shared_ptr<local_scheduler::TraefikRegistry> RegisteredRegistry(
        const std::string &instanceID, const std::string &runtimeID,
        const std::string &requestID)
    {
        // The mock accessor resolves synchronously, so the registration
        // (and its incarnation recording) is complete when this returns.
        auto future = registry_->RegisterInstance(
            instanceID, "10.0.0.1",
            { local_scheduler::TraefikRegistry::PortMapping{ 8080, 40001, "http" } },
            runtimeID, requestID);
        EXPECT_TRUE(future.IsReady());
        return registry_;
    }

    std::shared_ptr<MockMetaStorageAccessor> accessor_;
    std::shared_ptr<local_scheduler::TraefikRegistry> registry_;
    std::shared_ptr<std::atomic<int>> prefixDeletes_ =
        std::make_shared<std::atomic<int>>(0);
};

}  // namespace

// Matching identity: the unregister proceeds (router + service prefix
// deletes fire) and the record is forgotten.
TEST_F(TraefikRegistryFixture, MatchingIncarnationUnregisterProceeds)
{
    auto registry = RegisteredRegistry("inst-a", "rt-1", "req-1");
    auto future = registry->UnregisterInstance("inst-a", "rt-1", "req-1");
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get().IsOk()) << future.Get().GetMessage();
    ASSERT_AWAIT_TRUE([this]() { return prefixDeletes_->load() >= 2; });
}

// MISMATCHED identity: refused with ERR_INSTANCE_BUSY and ZERO accessor
// deletes; the record survives so the TRUE owner can still retire.
TEST_F(TraefikRegistryFixture, MismatchedIncarnationUnregisterRefused)
{
    auto registry = RegisteredRegistry("inst-b", "rt-NEW", "req-NEW");
    const int deletesBefore = prefixDeletes_->load();
    auto stale = registry->UnregisterInstance("inst-b", "rt-OLD", "req-OLD");
    ASSERT_AWAIT_READY(stale);
    ASSERT_TRUE(stale.Get().IsError());
    EXPECT_EQ(stale.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
    EXPECT_EQ(prefixDeletes_->load(), deletesBefore)
        << "a stale unregister must not touch the accessor at all";
    // The recorded owner can still retire afterwards.
    auto owner = registry->UnregisterInstance("inst-b", "rt-NEW", "req-NEW");
    ASSERT_AWAIT_READY(owner);
    EXPECT_TRUE(owner.Get().IsOk());
    ASSERT_AWAIT_TRUE([this]() { return prefixDeletes_->load() >= deletesBefore + 2; });
}

// Partial mismatch (runtime matches, request generation differs): still a
// refusal — the request generation is part of the incarnation identity.
TEST_F(TraefikRegistryFixture, RequestGenerationMismatchRefused)
{
    auto registry = RegisteredRegistry("inst-c", "rt-1", "req-GEN1");
    const int deletesBefore = prefixDeletes_->load();
    auto stale = registry->UnregisterInstance("inst-c", "rt-1", "req-GEN2");
    ASSERT_AWAIT_READY(stale);
    EXPECT_EQ(stale.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
    EXPECT_EQ(prefixDeletes_->load(), deletesBefore);
}

// The LEGACY single-arg unregister: unchecked (compat) — it proceeds even
// when a recorded different incarnation exists, and forgets the record.
TEST_F(TraefikRegistryFixture, LegacyUnregisterUncheckedAndForgets)
{
    auto registry = RegisteredRegistry("inst-d", "rt-OLD", "req-OLD");
    auto legacy = registry->UnregisterInstance("inst-d");
    ASSERT_AWAIT_READY(legacy);
    EXPECT_TRUE(legacy.Get().IsOk());
    ASSERT_AWAIT_TRUE([this]() { return prefixDeletes_->load() >= 2; });
    // After the legacy removal forgot the record, an identity-bearing
    // unregister for a DIFFERENT generation proceeds unchecked (no record).
    const int after = prefixDeletes_->load();
    auto next = registry->UnregisterInstance("inst-d", "rt-OTHER", "req-OTHER");
    ASSERT_AWAIT_READY(next);
    EXPECT_TRUE(next.Get().IsOk());
    ASSERT_AWAIT_TRUE([this]() { return prefixDeletes_->load() >= after + 2; });
}

// The legacy 3-arg REGISTER records nothing: a later identity-bearing
// unregister proceeds unchecked (pre-tracking compatibility).
TEST_F(TraefikRegistryFixture, LegacyRegisterRecordsNoIncarnation)
{
    auto future = registry_->RegisterInstance(
        "inst-e", "10.0.0.1",
        { local_scheduler::TraefikRegistry::PortMapping{ 8080, 40002, "http" } });
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get().IsOk());
    auto unregister = registry_->UnregisterInstance("inst-e", "rt-ANY", "req-ANY");
    ASSERT_AWAIT_READY(unregister);
    EXPECT_TRUE(unregister.Get().IsOk());
    ASSERT_AWAIT_TRUE([this]() { return prefixDeletes_->load() >= 2; });
}

// A FAILED registration forgets its tentatively-recorded incarnation: a
// later mismatched unregister is not fenced by a route that never existed.
TEST_F(TraefikRegistryFixture, FailedRegistrationForgetsTentativeRecord)
{
    EXPECT_CALL(*accessor_, Txn(_))
        .WillOnce(Return(Status(StatusCode::ERR_INNER_SYSTEM_ERROR, "etcd down")));
    auto future = registry_->RegisterInstance(
        "inst-f", "10.0.0.1",
        { local_scheduler::TraefikRegistry::PortMapping{ 8080, 40003, "http" } },
        "rt-FAILED", "req-FAILED");
    ASSERT_AWAIT_READY(future);
    ASSERT_TRUE(future.Get().IsError());
    auto unregister = registry_->UnregisterInstance("inst-f", "rt-OTHER", "req-OTHER");
    ASSERT_AWAIT_READY(unregister);
    EXPECT_TRUE(unregister.Get().IsOk())
        << "no route exists for this name; the tentative record must be gone";
}

}  // namespace functionsystem::test
