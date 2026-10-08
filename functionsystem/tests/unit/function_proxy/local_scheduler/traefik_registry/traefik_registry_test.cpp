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

// Store-side fencing tests for the marker-transaction route registry (#T).
// The fake is an in-memory etcd at the MetaStoreClient layer: BeginTransaction
// hands out a FakeEvalTxn whose Commit() evaluates the PRODUCTION-ASSEMBLED
// TxnCompare/TxnOperation objects (built by the registry through If/Then via
// their Build() into etcd pb form) against a std::map — the substitute
// EXECUTES what production assembled, it does not re-implement the
// predicates. The linearization point is FakeEvalTxn::Commit, where the red
// cases install their barrier.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "function_proxy/local_scheduler/traefik_registry/traefik_registry.h"
#include "meta_store_client/txn_transaction.h"
#include "mocks/mock_meta_storage_accessor.h"
#include "mocks/mock_meta_store_client.h"
#include "utils/future_test_helper.h"

namespace functionsystem::test {
namespace {

using testing::_;
using testing::Invoke;
using testing::Return;

// In-memory etcd model. Distinguishes etcd's two counters:
//  - version[key]: times written since creation; 0 ⇔ key ABSENT (resets on
//    delete-then-recreate) — the VERSION compare target.
//  - modRevision[key]: the GLOBAL store revision at the last write of this
//    key (also 0 when absent) — the OfModifyVersion target. Every committed
//    transaction allocates ONE new global revision, so a deleted-then-
//    recreated key NEVER matches its old mod revision.
struct FakeStore {
    std::mutex mutex;
    std::map<std::string, std::string> kv;         // key -> value
    std::map<std::string, int64_t> version;        // key -> create+write count
    std::map<std::string, int64_t> modRevision;    // key -> last-write global revision
    int64_t globalRevision = 0;                    // monotonic store revision

    std::string Get(const std::string &key, bool *found)
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = kv.find(key);
        if (it == kv.end()) {
            *found = false;
            return "";
        }
        *found = true;
        return it->second;
    }
    void Put(const std::string &key, const std::string &value)
    {
        std::lock_guard<std::mutex> lock(mutex);
        kv[key] = value;
        version[key] = version[key] + 1;
        modRevision[key] = ++globalRevision;
    }
    void EraseRange(const std::string &begin, const std::string &end)
    {
        for (auto it = kv.begin(); it != kv.end();) {
            if (it->first >= begin && it->first < end) {
                version.erase(it->first);
                modRevision.erase(it->first);
                it = kv.erase(it);
            } else {
                ++it;
            }
        }
    }
};

// Executes the production-assembled compares/thenOps against FakeStore.
// `barrier` (optional) runs BEFORE evaluation — the store's commit point.
class FakeEvalTxn : public functionsystem::meta_store::TxnTransaction {
public:
    FakeEvalTxn(const std::shared_ptr<FakeStore> &store,
                const std::function<void()> &barrier = nullptr)
        : TxnTransaction(litebus::AID()), store_(store), barrier_(barrier)
    {
    }

    litebus::Future<std::shared_ptr<functionsystem::TxnResponse>> Commit() const override
    {
        if (barrier_) { barrier_(); }
        std::lock_guard<std::mutex> lock(store_->mutex);
        // All compares evaluate against the CURRENT store state (the
        // barrier may have let concurrent writers commit first — exactly
        // the ordering the red cases probe).
        for (const auto &compare : compares) {
            ::etcdserverpb::Compare pb;
            compare.Build(pb, "");
            if (!Evaluate(*store_, pb)) {
                auto failed = std::make_shared<functionsystem::TxnResponse>();
                failed->success = false;
                return failed;
            }
        }
        // ONE global revision per committed transaction; every write in it
        // stamps that revision.
        const int64_t commitRevision = store_->globalRevision + 1;
        for (const auto &op : thenOps) {
            ::etcdserverpb::RequestOp pb;
            op.Build(pb, "");
            Apply(*store_, pb, commitRevision);
        }
        store_->globalRevision = commitRevision;
        auto response = std::make_shared<functionsystem::TxnResponse>();
        response->success = true;
        response->header.revision = commitRevision;
        return response;
    }

private:
    static bool Evaluate(const FakeStore &store, const ::etcdserverpb::Compare &pb)
    {
        const std::string key(pb.key());
        auto it = store.kv.find(key);
        const bool exists = it != store.kv.end();
        bool equal = false;
        switch (pb.target()) {
            case ::etcdserverpb::Compare::VALUE:
                equal = exists && it->second == std::string(pb.value());
                break;
            case ::etcdserverpb::Compare::VERSION:
                // etcd: version counts writes since creation; 0 ⇔ absent.
                equal = (exists ? store.version.at(key) : 0) == pb.version();
                break;
            case ::etcdserverpb::Compare::MOD: {
                // OfModifyVersion: the key's last-write GLOBAL revision;
                // a deleted record (absent → 0) never matches a positive
                // captured revision — the retired-replay fence.
                const auto modIt = store.modRevision.find(key);
                const int64_t mod = (modIt != store.modRevision.end()) ? modIt->second : 0;
                equal = mod == pb.mod_revision();
                break;
            }
            default:
                equal = false;
                break;
        }
        return pb.result() == ::etcdserverpb::Compare::EQUAL && equal;
    }

    static void Apply(FakeStore &store, const ::etcdserverpb::RequestOp &pb, int64_t commitRevision)
    {
        if (pb.has_request_put()) {
            const auto &put = pb.request_put();
            const std::string key(put.key());
            store.kv[key] = std::string(put.value());
            store.version[key] = store.version[key] + 1;
            store.modRevision[key] = commitRevision;
        } else if (pb.has_request_delete_range()) {
            const auto &del = pb.request_delete_range();
            const std::string begin(del.key());
            const std::string end(del.range_end().empty() ? begin + '\x01' : std::string(del.range_end()));
            store.EraseRange(begin, end);
        }
    }

    std::shared_ptr<FakeStore> store_;
    std::function<void()> barrier_;
};

class TraefikRegistryFixture : public ::testing::Test {
protected:
    void SetUp() override
    {
        store_ = std::make_shared<FakeStore>();
        metaClient_ = std::make_shared<MockMetaStoreClient>("127.0.0.1:1");
        ON_CALL(*metaClient_, BeginTransaction).WillByDefault(Invoke([this]() {
            return std::shared_ptr<functionsystem::meta_store::TxnTransaction>(
                std::make_shared<FakeEvalTxn>(store_, nextBarrier_));
        }));
        accessor_ = std::make_shared<MockMetaStorageAccessor>(metaClient_);
        ON_CALL(*accessor_, Put(_, _)).WillByDefault(Invoke([this](const std::string &k, const std::string &v) {
            store_->Put(k, v);
            return Status::OK();
        }));
        ON_CALL(*accessor_, Get(_)).WillByDefault(Invoke([this](const std::string &k) {
            bool found = false;
            const std::string value = store_->Get(k, &found);
            return found ? litebus::Some(value) : litebus::None<std::string>();
        }));
        ON_CALL(*accessor_, GetAllWithPrefix(_)).WillByDefault(Invoke([this](const std::string &prefix) {
            std::vector<std::pair<std::string, std::string>> out;
            std::lock_guard<std::mutex> lock(store_->mutex);
            for (const auto &kv : store_->kv) {
                if (kv.first.rfind(prefix, 0) == 0) { out.emplace_back(kv.first, kv.second); }
            }
            return litebus::Some(out);
        }));
        registry_ = std::make_shared<local_scheduler::TraefikRegistry>(
            accessor_, "yr/traefik-ut", "web", false, "");
    }

    // Seeds the durable instance record (same identity) at a FRESH store
    // revision and registers against it — the legitimate shape.
    std::shared_ptr<Status> Register(const std::string &id, const std::string &tenant,
                                     const std::string &runtime, const std::string &req,
                                     int sandboxPort)
    {
        const std::string recordKey = "rec/" + id;
        store_->Put(recordKey, "instance-record|" + runtime + "|" + req);
        const uint64_t revision = static_cast<uint64_t>(store_->modRevision[recordKey]);
        auto future = registry_->RegisterInstance(
            id, "10.0.0.1",
            { local_scheduler::TraefikRegistry::PortMapping{ sandboxPort, 40000 + sandboxPort, "http" } },
            tenant, runtime, req, revision, recordKey);
        // The fake commits synchronously, so the chain resolves inline.
        EXPECT_TRUE(future.IsReady());
        return std::make_shared<Status>(future.Get());
    }

    std::shared_ptr<FakeStore> store_;
    std::shared_ptr<MockMetaStoreClient> metaClient_;
    std::shared_ptr<MockMetaStorageAccessor> accessor_;
    std::shared_ptr<local_scheduler::TraefikRegistry> registry_;
    std::function<void()> nextBarrier_;  // consumed by the NEXT transaction
};

}  // namespace

TEST_F(TraefikRegistryFixture, MarkerRoundTripParse)
{
    const std::string raw = local_scheduler::TraefikRegistry::SerializeMarker(
        "inst@1", "ten|ant", "rt=x", "req=1", 42, { "b", "a", "b" });
    const auto marker = local_scheduler::TraefikRegistry::ParseMarker(raw);
    ASSERT_TRUE(marker.present);
    ASSERT_TRUE(marker.valid);
    EXPECT_EQ(marker.instanceID, "inst@1");
    EXPECT_EQ(marker.tenantID, "ten|ant");
    EXPECT_EQ(marker.runtimeID, "rt=x");
    EXPECT_EQ(marker.requestID, "req=1");
    EXPECT_EQ(marker.instanceRevision, 42u);
    ASSERT_EQ(marker.ownKeys.size(), 2u);  // sorted + deduped
    EXPECT_EQ(marker.ownKeys[0], "a");
    EXPECT_EQ(marker.ownKeys[1], "b");
}

TEST_F(TraefikRegistryFixture, MatchingRetireDeletesExactlyOwnKeysAndMarker)
{
    ASSERT_TRUE(Register("inst-a", "t1", "rt-1", "req-1", 8080)->IsOk());
    ASSERT_TRUE(store_->kv.count("yr/traefik-ut/http/routers/inst-a-p8080/rule"));
    auto retire = registry_->UnregisterInstance("inst-a", "t1", "rt-1", "req-1");
    ASSERT_AWAIT_READY(retire);
    EXPECT_TRUE(retire.Get().IsOk()) << retire.Get().GetMessage();
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-a-p8080/rule"), 0u);
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/services/inst-a-p8080/loadbalancer/servers/0/url"), 0u);
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/instances/inst-a/generation"), 0u);
}

TEST_F(TraefikRegistryFixture, MismatchedOrMissingOwnershipRefusedZeroDeletes)
{
    ASSERT_TRUE(Register("inst-b", "t1", "rt-NEW", "req-NEW", 8080)->IsOk());
    const auto before = store_->kv;
    auto stale = registry_->UnregisterInstance("inst-b", "t1", "rt-OLD", "req-OLD");
    ASSERT_AWAIT_READY(stale);
    EXPECT_EQ(stale.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
    auto none = registry_->UnregisterInstance("inst-none", "t1", "rt-1", "req-1");
    ASSERT_AWAIT_READY(none);
    EXPECT_EQ(none.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
    EXPECT_EQ(store_->kv, before) << "both refusals must touch nothing";
}

// ROOT RED CASE T: the old chain's retire transaction reaches the store's
// COMMIT BARRIER and hangs; the new generation registers (marker + routes
// commit); the old transaction then evaluates — the VALUE compare fails at
// the store and the new routes survive untouched; the new generation's own
// retire afterwards succeeds.
TEST_F(TraefikRegistryFixture, RedCaseOldRetireBarrierThenNewRegister)
{
    ASSERT_TRUE(Register("inst-r", "t1", "rt-OLD", "req-OLD", 8080)->IsOk());

    auto reached = std::make_shared<std::promise<void>>();
    auto release = std::make_shared<std::promise<void>>();
    nextBarrier_ = [reached, release]() {
        reached->set_value();
        release->get_future().wait();
    };
    auto oldRetire = registry_->UnregisterInstance("inst-r", "t1", "rt-OLD", "req-OLD");
    ASSERT_EQ(reached->get_future().wait_for(std::chrono::seconds(5)), std::future_status::ready);

    // New generation takes the name (higher durable etcd revision).
    nextBarrier_ = nullptr;
    ASSERT_TRUE(Register("inst-r", "t1", "rt-NEW", "req-NEW", 8080)->IsOk());
    const auto newRoute = store_->kv.find("yr/traefik-ut/http/routers/inst-r-p8080/rule");
    ASSERT_NE(newRoute, store_->kv.end());

    release->set_value();
    ASSERT_AWAIT_READY(oldRetire);
    EXPECT_EQ(oldRetire.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY);
    // New routes + marker fully intact.
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-r-p8080/rule"), 1u);
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/services/inst-r-p8080/loadbalancer/servers/0/url"), 1u);
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/instances/inst-r/generation"), 1u);

    auto newRetire = registry_->UnregisterInstance("inst-r", "t1", "rt-NEW", "req-NEW");
    ASSERT_AWAIT_READY(newRetire);
    EXPECT_TRUE(newRetire.Get().IsOk());
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/instances/inst-r/generation"), 0u);
}

// ROOT RED CASE (late old REGISTER): the OLD registration's transaction
// hangs at the commit barrier; the NEW generation registers; the old
// transaction's compare fails AND its bounded retry sees a foreign marker
// with a HIGHER authority revision — the takeover is refused and the new
// holder survives byte-for-byte.
TEST_F(TraefikRegistryFixture, RedCaseLateOldRegisterCannotOverwriteNewHolder)
{
    ASSERT_TRUE(Register("inst-g", "t1", "rt-OLD", "req-OLD", 8080)->IsOk());

    auto reached = std::make_shared<std::promise<void>>();
    auto release = std::make_shared<std::promise<void>>();
    nextBarrier_ = [reached, release]() {
        reached->set_value();
        release->get_future().wait();
    };
    // The OLD generation re-registers (its own refresh path — capture
    // branch would authorize it against its OWN marker value). Its record
    // still exists at the OLD revision (legitimate seeds) — the fence that
    // stops it is the store-side value compare, not the seeding.
    const std::string recordG = "rec/inst-g";
    store_->Put(recordG, "instance-record|rt-OLD|req-OLD");
    const uint64_t revisionG = static_cast<uint64_t>(store_->modRevision[recordG]);
    auto refreshFuture = registry_->RegisterInstance(
        "inst-g", "10.9.9.9",
        { local_scheduler::TraefikRegistry::PortMapping{ 8081, 49081, "http" } },
        "t1", "rt-OLD", "req-OLD", revisionG, recordG);
    ASSERT_EQ(reached->get_future().wait_for(std::chrono::seconds(5)), std::future_status::ready);

    nextBarrier_ = nullptr;
    ASSERT_TRUE(Register("inst-g", "t1", "rt-NEW", "req-NEW", 8080)->IsOk());
    const auto snapshot = store_->kv;

    release->set_value();
    ASSERT_AWAIT_READY(refreshFuture);
    EXPECT_TRUE(refreshFuture.Get().IsError())
        << "the late old refresh must fail, not overwrite the newer holder";
    EXPECT_EQ(store_->kv, snapshot) << "the new holder's keys must be byte-identical";
}

// Takeover authorization: same logical instance+tenant with a strictly newer
// durable revision takes over; a SanitizeID-colliding stranger ("a_b" vs
// "a-b") NEVER takes over regardless of revision.
TEST_F(TraefikRegistryFixture, TakeoverOrderAndSanitizeCollision)
{
    ASSERT_TRUE(Register("inst-k", "t1", "rt-1", "req-1", 8080)->IsOk());
    // Same instance, newer etcd revision: LEGITIMATE takeover completes.
    ASSERT_TRUE(Register("inst-k", "t1", "rt-2", "req-2", 8081)->IsOk());
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-k-p8080/rule"), 0u)
        << "the superseded generation's port key must be atomically cleaned";
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-k-p8081/rule"), 1u);

    // "a_b" and "a-b" both sanitize to "a-b": the stranger is refused and
    // the original holder is untouched.
    ASSERT_TRUE(Register("a_b", "t1", "rt-orig", "req-orig", 8080)->IsOk());
    const auto before = store_->kv;
    const std::string recordS = "rec/a-b";
    store_->Put(recordS, "instance-record|rt-stranger|req-stranger");
    auto stranger = registry_->RegisterInstance(
        "a-b", "10.0.0.1",
        { local_scheduler::TraefikRegistry::PortMapping{ 8082, 49082, "http" } },
        "t1", "rt-stranger", "req-stranger",
        static_cast<uint64_t>(store_->modRevision[recordS]), recordS);
    ASSERT_AWAIT_READY(stranger);
    EXPECT_TRUE(stranger.Get().IsError());
    EXPECT_EQ(store_->kv, before);
}

// Legacy (marker-less) cleanup: exact-key enumeration only, neighbor
// isolation (inst-a never touches inst-ab), and the commit stays
// CONDITIONED on the marker remaining absent — a register landing
// mid-flight fails the compare and keeps its routes.
TEST_F(TraefikRegistryFixture, LegacyCleanupNeighborsAndConditionalCommit)
{
    // Marker-less legacy routes for two prefix-similar instances.
    store_->Put("yr/traefik-ut/http/routers/inst-a-p8080/rule", "legacy-a");
    store_->Put("yr/traefik-ut/http/routers/inst-ab-p8080/rule", "legacy-ab");
    auto legacy = registry_->UnregisterInstance("inst-a");
    ASSERT_AWAIT_READY(legacy);
    EXPECT_TRUE(legacy.Get().IsOk());
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-a-p8080/rule"), 0u);
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-ab-p8080/rule"), 1u)
        << "neighbor route must survive";

    // Mid-flight marker appearance fails the legacy cleanup's compare.
    store_->Put("yr/traefik-ut/http/routers/inst-c-p8080/rule", "legacy-c");
    auto reached = std::make_shared<std::promise<void>>();
    auto release = std::make_shared<std::promise<void>>();
    nextBarrier_ = [reached, release]() {
        reached->set_value();
        release->get_future().wait();
    };
    auto cleanup = registry_->UnregisterInstance("inst-c");
    ASSERT_EQ(reached->get_future().wait_for(std::chrono::seconds(5)), std::future_status::ready);
    nextBarrier_ = nullptr;
    ASSERT_TRUE(Register("inst-c", "t1", "rt-NEW", "req-NEW", 8080)->IsOk());
    release->set_value();
    ASSERT_AWAIT_READY(cleanup);
    EXPECT_EQ(cleanup.Get().StatusCode(), StatusCode::ERR_INSTANCE_BUSY)
        << "the compare failure must propagate, not silently succeed";
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-c-p8080/rule"), 1u)
        << "the registered generation's route survives the stale cleanup";
}

// ROOT RED CASE (retired positive-revision replay): the original instance
// registered with a valid positive record revision, was fully retired
// (record deleted, marker deleted); a LATE replay of the old registration
// carrying its ONCE-VALID revision must issue ZERO routes — the record-key
// modify-revision condition fails at commit (record absent → 0, or
// rewritten by a successor → different revision). A genuinely NEW
// generation with a fresh record registers normally right after.
TEST_F(TraefikRegistryFixture, RedCaseRetiredPositiveRevisionReplayIssuesZeroRoutes)
{
    ASSERT_TRUE(Register("inst-rr", "t1", "rt-1", "req-1", 8080)->IsOk());
    const std::string recordRR = "rec/inst-rr";
    const uint64_t retiredRevision = static_cast<uint64_t>(store_->modRevision[recordRR]);
    // Full retirement: routes + marker + the instance record itself.
    auto retire = registry_->UnregisterInstance("inst-rr", "t1", "rt-1", "req-1");
    ASSERT_AWAIT_READY(retire);
    EXPECT_TRUE(retire.Get().IsOk());
    store_->EraseRange(recordRR, recordRR + '\x01');

    // The stale chain replays with its once-valid revision and no marker.
    auto replay = registry_->RegisterInstance(
        "inst-rr", "10.0.0.1",
        { local_scheduler::TraefikRegistry::PortMapping{ 8080, 48080, "http" } },
        "t1", "rt-1", "req-1", retiredRevision, recordRR);
    ASSERT_AWAIT_READY(replay);
    EXPECT_TRUE(replay.Get().IsError()) << "a retired generation's replay must not resurrect routes";
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-rr-p8080/rule"), 0u);
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/instances/inst-rr/generation"), 0u);

    // Deleted-then-RECREATED record (a successor rewrote it): the old
    // revision still fails (mod revision moved on).
    store_->Put(recordRR, "instance-record|rt-2|req-2");
    auto replay2 = registry_->RegisterInstance(
        "inst-rr", "10.0.0.1",
        { local_scheduler::TraefikRegistry::PortMapping{ 8080, 48080, "http" } },
        "t1", "rt-1", "req-1", retiredRevision, recordRR);
    ASSERT_AWAIT_READY(replay2);
    EXPECT_TRUE(replay2.Get().IsError());
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-rr-p8080/rule"), 0u);

    // A genuinely NEW generation (fresh record revision) registers normally.
    ASSERT_TRUE(Register("inst-rr", "t1", "rt-2", "req-2", 8080)->IsOk());
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-rr-p8080/rule"), 1u);
}

// Same-generation ports→zero change still cleans the previous key list.
TEST_F(TraefikRegistryFixture, ZeroPortRefreshCleansOldKeys)
{
    ASSERT_TRUE(Register("inst-z", "t1", "rt-1", "req-1", 8080)->IsOk());
    const std::string recordZ = "rec/inst-z";
    auto future = registry_->RegisterInstance("inst-z", "10.0.0.1", {}, "t1", "rt-1", "req-1",
        static_cast<uint64_t>(store_->modRevision[recordZ]), recordZ);
    ASSERT_AWAIT_READY(future);
    EXPECT_TRUE(future.Get().IsOk());
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/routers/inst-z-p8080/rule"), 0u);
    EXPECT_EQ(store_->kv.count("yr/traefik-ut/http/instances/inst-z/generation"), 1u)
        << "the marker survives with an empty key list";
}

}  // namespace functionsystem::test
