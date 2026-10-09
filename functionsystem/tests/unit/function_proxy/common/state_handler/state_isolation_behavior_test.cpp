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

// State-isolation behavior tests against the REAL StateActor chain with a
// map-backed persistent transaction model (state_isolation_support): every
// transaction is genuinely evaluated (value/version/revision compares via
// the production serializers), gates produce REAL interleavings, and the
// data plane counts per-key Set/Del. No wildcard-success transactions, no
// shared Set strings.

#include "function_proxy/common/state_handler/state_handler.h"

#include <chrono>
#include <thread>

#include "common/metadata/metadata.h"
#include "common/utils/meta_store_kv_operation.h"
#include "function_proxy/common/state_machine/instance_control_view.h"
#include "gtest/gtest.h"
#include "mocks/mock_instance_state_machine.h"
#include "state_isolation_support.h"

namespace functionsystem::function_proxy::test {

using namespace functionsystem::test;

namespace {

constexpr const char *kFuncKey = "default/0-test-helloWorld/$latest";

template <typename T>
bool WaitReady(const litebus::Future<T> &future, uint32_t timeoutMs = 15000)
{
    return future.WaitFor(timeoutMs).IsOK() && !future.IsError();
}

resources::InstanceInfo MakeInfo(const std::string &instance, const std::string &request,
                                 const std::string &runtime)
{
    resources::InstanceInfo info;
    info.set_instanceid(instance);
    info.set_requestid(request);
    info.set_runtimeid(runtime);
    info.set_function("helloWorld");
    return info;
}

}  // namespace

class StateIsolation : public ::testing::Test {
protected:
    void SetUp() override
    {
        ds_ = std::make_shared<MapDataStore>();
        stateClient_ = std::make_shared<StateClient>(ds_);
        actor_ = std::make_shared<StateActor>(stateClient_);
        litebus::Spawn(actor_);
        StateHandler::BindStateActor(actor_);
        metaCore_ = std::make_shared<MapMetaStoreCore>();
        meta_ = std::make_shared<MapMetaStore>(metaCore_);
        actor_->BindSaveRegistry(meta_, [](const std::string &, const std::string &,
                                           const std::string &) { return kFuncKey; });
        actor_->SetSaveExecutor([](const std::string &, const std::string &, const std::string &,
                                   const StateActor::GuardedStoreOp &op) { return op(); });
        // Keep the replay cadence out of the way unless a test opts in.
        StateActor::replayInitialDelayMs.store(60000);
        StateActor::replayPeriodMs.store(60000);
    }

    void TearDown() override
    {
        StateActor::replayInitialDelayMs.store(3000);
        StateActor::replayPeriodMs.store(60000);
        if (actor_ != nullptr) {
            litebus::Terminate(actor_->GetAID());
            litebus::Await(actor_->GetAID());
            actor_ = nullptr;
        }
    }

    // Seeds the AUTHORITATIVE instance record exactly as the production
    // persistence would hold it.
    void SeedRecord(const std::string &instance, const std::string &request,
                    const std::string &runtime)
    {
        const auto keyOpt = GenInstanceKey(kFuncKey, instance, request);
        ASSERT_TRUE(keyOpt.IsNone() == false);
        std::string json;
        ASSERT_TRUE(TransToJsonFromInstanceInfo(json, MakeInfo(instance, request, runtime)));
        metaCore_->Seed(keyOpt.Get(), json);
    }

    litebus::Future<StateSaveResponse> Save(const std::string &instance, const std::string &runtime,
                                            const std::string &request, const std::string &payload)
    {
        auto msg = std::make_shared<runtime_rpc::StreamingMessage>();
        msg->mutable_savereq()->set_state(payload);
        return StateHandler::SaveState(grpc::PosixStreamIdentity{ instance, runtime, request }, msg)
            .Then([](const std::shared_ptr<runtime_rpc::StreamingMessage> &rsp)
                      -> StateSaveResponse { return rsp->saversp(); });
    }

    // The snapshot key of the (single) registration of a generation, from
    // the persistent record — used when a failed save's response carries
    // no checkpoint id.
    std::string SoleRegisteredSnapshotKey(const std::string &instance, const std::string &request)
    {
        const auto prefix = StateActor::RegistrationPrefix(instance, request);
        std::lock_guard<std::mutex> guard(metaCore_->lock);
        for (const auto &entry : metaCore_->kv) {
            if (entry.first.rfind(prefix, 0) != 0) { continue; }
            const auto tuple = StateActor::ParseRegistration(entry.second.value);
            if (!tuple.snapshotKey.empty()) { return tuple.snapshotKey; }
        }
        return "";
    }

    std::shared_ptr<MapDataStore> ds_;
    std::shared_ptr<StateClient> stateClient_;
    std::shared_ptr<StateActor> actor_;
    std::shared_ptr<MapMetaStoreCore> metaCore_;
    std::shared_ptr<MapMetaStore> meta_;
};

// ── unique-key-roundtrip ────────────────────────────────────────────────────
TEST_F(StateIsolation, UniqueKeyRoundtripAndLatestRefRecovery)
{
    SeedRecord("inst-a", "req-a", "rt-a");
    auto f1 = Save("inst-a", "rt-a", "req-a", "state-one");
    ASSERT_TRUE(WaitReady(f1));
    auto rsp1 = f1.Get();
    ASSERT_EQ(rsp1.code(), common::ErrorCode::ERR_NONE);

    auto f2 = Save("inst-a", "rt-a", "req-a", "state-two");
    ASSERT_TRUE(WaitReady(f2));
    auto rsp2 = f2.Get();
    ASSERT_EQ(rsp2.code(), common::ErrorCode::ERR_NONE);

    // Two DIFFERENT unique snapshot keys; both payloads landed on their
    // EXACT keys (no shared string).
    EXPECT_NE(rsp1.checkpointid(), rsp2.checkpointid());
    EXPECT_EQ(ds_->SetCount(rsp1.checkpointid()), 1);
    EXPECT_EQ(ds_->SetCount(rsp2.checkpointid()), 1);
    std::string out;
    EXPECT_TRUE(ds_->Get(rsp1.checkpointid(), out).IsOk());
    EXPECT_EQ(out, "state-one");
    EXPECT_TRUE(ds_->Get(rsp2.checkpointid(), out).IsOk());
    EXPECT_EQ(out, "state-two");

    // The persistent reference points at the LATEST snapshot; Recover
    // resolves THAT reference and reads the NEW value by exact key.
    auto resolve = StateHandler::ResolveCheckpointRef("inst-a", "req-a");
    ASSERT_TRUE(WaitReady(resolve));
    const auto ref = resolve.Get();
    ASSERT_TRUE(ref.ok);
    EXPECT_FALSE(ref.legacy);
    EXPECT_EQ(ref.checkpointRef, rsp2.checkpointid());
    auto loadMsg = std::make_shared<runtime_rpc::StreamingMessage>();
    loadMsg->mutable_loadreq()->set_checkpointid(ref.checkpointRef);
    auto load = StateHandler::LoadState(
        grpc::PosixStreamIdentity{ "inst-a", "rt-successor", "" }, loadMsg);
    ASSERT_TRUE(WaitReady(load));
    EXPECT_EQ(load.Get()->loadrsp().code(), common::ErrorCode::ERR_NONE);
    EXPECT_EQ(load.Get()->loadrsp().state(), "state-two");
}

// ── late-old-publish ────────────────────────────────────────────────────────
TEST_F(StateIsolation, LateOldPublishFailsAgainstMovedReference)
{
    SeedRecord("inst-b", "req-b", "rt-b");
    const std::string refKey = StateActor::StateRefKey("inst-b", "req-b");

    // Park ONLY the first publish (its Then writes the reference key; the
    // registration transaction does not).
    metaCore_->AddGate(OnThenKey(refKey));

    auto f1 = Save("inst-b", "rt-b", "req-b", "old-state");
    // Save 1's publish is gated; save 2 (fresh capture: reference still
    // absent) publishes FIRST.
    auto f2 = Save("inst-b", "rt-b", "req-b", "new-state");
    ASSERT_TRUE(WaitReady(f2));
    ASSERT_EQ(f2.Get().code(), common::ErrorCode::ERR_NONE);
    const auto newKey = f2.Get().checkpointid();

    // Releasing the OLD publish NOW evaluates its conditions against the
    // MOVED store: the absent-reference guard (version==0) fails.
    metaCore_->ReleaseGate(OnThenKey(refKey));
    ASSERT_TRUE(WaitReady(f1));
    const auto oldRsp = f1.Get();
    EXPECT_NE(oldRsp.code(), common::ErrorCode::ERR_NONE);

    // The new reference survived; the old save's data is safe on its own
    // unique key (an orphan the retirement replay owns); its registration
    // is retained (UNKNOWN), never published.
    MapRecord refRec;
    ASSERT_TRUE(metaCore_->Lookup(refKey, refRec));
    EXPECT_EQ(refRec.value, newKey);
    EXPECT_EQ(ds_->SetCount(oldRsp.checkpointid()), 1);
    // No re-read-and-retry path exists: exactly one publish attempt each.
    EXPECT_EQ(metaCore_->GateCount(), 0);
}

// ── authority-and-registration-failure ─────────────────────────────────────
TEST_F(StateIsolation, AuthorityAndRegistrationFailureZeroDsWrites)
{
    // (a) authoritative record absent.
    auto fa = Save("inst-c", "rt-c", "req-c", "x");
    ASSERT_TRUE(WaitReady(fa));
    EXPECT_NE(fa.Get().code(), common::ErrorCode::ERR_NONE);

    // (b) identity mismatch (record belongs to another runtime).
    SeedRecord("inst-c", "req-c", "rt-OTHER");
    auto fb = Save("inst-c", "rt-c", "req-c", "x");
    ASSERT_TRUE(WaitReady(fb));
    EXPECT_NE(fb.Get().code(), common::ErrorCode::ERR_NONE);

    // (c) corrupt record value.
    const auto keyOpt = GenInstanceKey(kFuncKey, "inst-c2", "req-c2");
    metaCore_->Seed(keyOpt.Get(), "not-an-instance-info");
    auto fc = Save("inst-c2", "rt-c2", "req-c2", "x");
    ASSERT_TRUE(WaitReady(fc));
    EXPECT_NE(fc.Get().code(), common::ErrorCode::ERR_NONE);

    // (d) record read failed future.
    SeedRecord("inst-c3", "req-c3", "rt-c3");
    metaCore_->forcedGets.push_back(
        { GenInstanceKey(kFuncKey, "inst-c3", "req-c3").Get(), GetOutcome::FAIL_FUTURE });
    auto fd = Save("inst-c3", "rt-c3", "req-c3", "x");
    ASSERT_TRUE(WaitReady(fd));
    EXPECT_NE(fd.Get().code(), common::ErrorCode::ERR_NONE);

    // (e) generation already retired (tombstone present).
    SeedRecord("inst-c4", "req-c4", "rt-c4");
    {
        StateActor::FenceRecord fence;
        fence.instanceID = "inst-c4";
        fence.creationRequest = "req-c4";
        fence.runtimeID = "rt-c4";
        fence.authorityRecordKey = GenInstanceKey(kFuncKey, "inst-c4", "req-c4").Get();
        fence.authorityModRevision = 1;
        fence.status = "RETIRED";
        metaCore_->Seed(StateActor::RetirementKey("inst-c4", "req-c4"),
                        StateActor::SerializeFence(fence));
    }
    auto fe = Save("inst-c4", "rt-c4", "req-c4", "x");
    ASSERT_TRUE(WaitReady(fe));
    EXPECT_NE(fe.Get().code(), common::ErrorCode::ERR_NONE);

    // (f..i) registration transaction failures: the registration carries a
    // compare on the fence key (the fence-absent condition) — one-shot
    // forced outcomes hit it before any publish could.
    const auto fenceKeyFor = [](const std::string &i, const std::string &r) {
        return StateActor::RetirementKey(i, r);
    };
    struct Variant { CommitOutcome outcome; };
    const std::vector<Variant> variants = { { CommitOutcome::FAIL_FUTURE },
                                            { CommitOutcome::NULL_RESPONSE },
                                            { CommitOutcome::STATUS_ERROR },
                                            { CommitOutcome::SUCCESS_FALSE } };
    int index = 0;
    for (const auto &variant : variants) {
        const std::string inst = "inst-c5-" + std::to_string(index);
        const std::string req = "req-c5-" + std::to_string(index);
        SeedRecord(inst, req, "rt-c5");
        metaCore_->forcedCommits.push_back({ OnCompareKey(fenceKeyFor(inst, req)),
                                             variant.outcome });
        auto f = Save(inst, "rt-c5", req, "x");
        ASSERT_TRUE(WaitReady(f)) << "variant " << index;
        EXPECT_NE(f.Get().code(), common::ErrorCode::ERR_NONE) << "variant " << index;
        index += 1;
    }

    // EVERY refusal path performed ZERO DataSystem writes.
    EXPECT_EQ(ds_->TotalSetCalls(), 0);
}

// ── same-runtime-new-request (execution point, real machine locks) ─────────
TEST(StateIsolationMachineGuard, SameRuntimeNewRequestRefusedAtExecutionPoint)
{
    auto schedReq = std::make_shared<messages::ScheduleRequest>();
    auto info = MakeInfo("inst-g", "req-old", "rt-g");
    *schedReq->mutable_instance() = info;
    auto context = std::make_shared<InstanceContext>(schedReq);
    MockInstanceStateMachine machine("node-g", context);

    int ran = 0;
    const auto op = [&ran]() {
        ran += 1;
        return Status::OK();
    };
    // Matching identity executes.
    EXPECT_TRUE(machine.ExecuteStateOpUnderLock("rt-g", "req-old", op).IsOk());
    EXPECT_EQ(ran, 1);
    // Same runtime, DIFFERENT bound request: refused, zero writes.
    auto refused = machine.ExecuteStateOpUnderLock("rt-g", "req-new", op);
    EXPECT_TRUE(refused.IsError());
    EXPECT_EQ(ran, 1);
    // The machine's generation moves on; the OLD stream's op is refused.
    schedReq->mutable_instance()->set_requestid("req-new");
    refused = machine.ExecuteStateOpUnderLock("rt-g", "req-old", op);
    EXPECT_TRUE(refused.IsError());
    EXPECT_EQ(ran, 1);
    EXPECT_TRUE(machine.ExecuteStateOpUnderLock("rt-g", "req-new", op).IsOk());
    EXPECT_EQ(ran, 2);
    // An UNKNOWN (empty) current request also refuses a bound caller.
    auto schedReq2 = std::make_shared<messages::ScheduleRequest>();
    auto info2 = MakeInfo("inst-g2", "", "rt-g2");
    *schedReq2->mutable_instance() = info2;
    auto context2 = std::make_shared<InstanceContext>(schedReq2);
    MockInstanceStateMachine machine2("node-g2", context2);
    EXPECT_TRUE(machine2.ExecuteStateOpUnderLock("rt-g2", "req-bound", op).IsError());
    EXPECT_EQ(ran, 2);
}

// ── publish-unknown-commit ─────────────────────────────────────────────────
TEST_F(StateIsolation, PublishAppliedButResponseLostStaysUncertainAndCommitted)
{
    SeedRecord("inst-h", "req-h", "rt-h");
    const std::string refKey = StateActor::StateRefKey("inst-h", "req-h");
    metaCore_->forcedCommits.push_back({ OnThenKey(refKey), CommitOutcome::APPLY_THEN_FAIL });

    auto f = Save("inst-h", "rt-h", "req-h", "state-h");
    ASSERT_TRUE(WaitReady(f));
    // The operation reports UNCERTAIN — never success by default value.
    EXPECT_NE(f.Get().code(), common::ErrorCode::ERR_NONE);
    const auto snapshotKey = f.Get().checkpointid();

    // But the transaction REALLY applied: the registration is COMMITTED
    // and the reference moved, atomically.
    MapRecord refRec;
    ASSERT_TRUE(metaCore_->Lookup(refKey, refRec));
    EXPECT_EQ(refRec.value, snapshotKey);
    // The demotion attempt (conditional on the PENDING value) must NOT
    // downgrade a COMMITTED registration.
    bool foundCommitted = false;
    {
        std::lock_guard<std::mutex> guard(metaCore_->lock);
        for (const auto &entry : metaCore_->kv) {
            if (entry.first.rfind("statereg:v1:", 0) != 0) { continue; }
            const auto tuple = StateActor::ParseRegistration(entry.second.value);
            EXPECT_EQ(tuple.status, "COMMITTED");
            foundCommitted = true;
        }
    }
    EXPECT_TRUE(foundCommitted);
}

// ── retire-before-owner-delete ──────────────────────────────────────────────
TEST_F(StateIsolation, RetireFenceBeforeOwnerDeleteAndRefusedWithoutRecord)
{
    // Fresh retirement commits the tombstone conditionally on the ORIGINAL
    // authoritative record — and strictly BEFORE the caller may delete it.
    SeedRecord("inst-i", "req-i", "rt-i");
    const auto recordKey = GenInstanceKey(kFuncKey, "inst-i", "req-i").Get();
    const std::string fenceKey = StateActor::RetirementKey("inst-i", "req-i");

    auto fence = StateHandler::RetireGeneration("inst-i", "req-i", "rt-i");
    ASSERT_TRUE(WaitReady(fence));
    ASSERT_TRUE(fence.Get().fenced);
    EXPECT_TRUE(fence.Get().fenceFresh);
    EXPECT_TRUE(metaCore_->Exists(fenceKey));
    // The tombstone names the ORIGINAL authority.
    MapRecord rec;
    ASSERT_TRUE(metaCore_->Lookup(fenceKey, rec));
    const auto parsed = StateActor::ParseFence(rec.value);
    EXPECT_EQ(parsed.authorityRecordKey, recordKey);
    EXPECT_GT(parsed.authorityModRevision, 0);
    // The record is STILL there — the fence ran first; the caller deletes
    // it only afterwards (simulating DelInstance).
    EXPECT_TRUE(metaCore_->Exists(recordKey));

    // A MOVED record refuses the fence: no self-appointed tombstone.
    SeedRecord("inst-j", "req-j", "rt-j");
    metaCore_->Seed(GenInstanceKey(kFuncKey, "inst-j", "req-j").Get(), "moved-record");
    auto refused = StateHandler::RetireGeneration("inst-j", "req-j", "rt-j");
    ASSERT_TRUE(WaitReady(refused));
    EXPECT_FALSE(refused.Get().fenced);
    EXPECT_FALSE(metaCore_->Exists(StateActor::RetirementKey("inst-j", "req-j")));

    // Retry after the owner record is gone: the tombstone is the ONLY
    // admissible proof.
    {
        std::lock_guard<std::mutex> guard(metaCore_->lock);
        metaCore_->kv.erase(recordKey);
    }
    auto replay = StateHandler::RetireGeneration("inst-i", "req-i", "rt-i");
    ASSERT_TRUE(WaitReady(replay));
    EXPECT_TRUE(replay.Get().fenced);
    EXPECT_FALSE(replay.Get().fenceFresh);
    auto noProof = StateHandler::RetireGeneration("inst-k", "req-k", "rt-k");
    ASSERT_TRUE(WaitReady(noProof));
    EXPECT_FALSE(noProof.Get().fenced);
}

// ── retire-late-save-delete ────────────────────────────────────────────────
TEST_F(StateIsolation, RetireLateSaveSingleNotFoundNotCleanAndNewGenUntouched)
{
    SeedRecord("inst-l", "req-l", "rt-l");
    const std::string fenceKey = StateActor::RetirementKey("inst-l", "req-l");

    // A PENDING save whose DS write times out (the snapshot key is a fresh
    // UUID, so the injection is one-shot global): registration retained,
    // data uncertain.
    ds_->forceNextSet = DsAction::TIMEOUT;
    auto f = Save("inst-l", "rt-l", "req-l", "state-l");
    ASSERT_TRUE(WaitReady(f));
    EXPECT_NE(f.Get().code(), common::ErrorCode::ERR_NONE);
    const std::string snapKey = SoleRegisteredSnapshotKey("inst-l", "req-l");
    ASSERT_FALSE(snapKey.empty());

    // Retire + cleanup with the DS folding delete-of-absent into an error.
    auto fence = StateHandler::RetireGeneration("inst-l", "req-l", "rt-l");
    ASSERT_TRUE(WaitReady(fence));
    ASSERT_TRUE(fence.Get().fenced);
    ds_->ForceDel(snapKey, DsAction::NOT_FOUND_FOLDED);
    auto clean = StateHandler::CleanupRetiredSnapshots("inst-l", "req-l", "rt-l");
    ASSERT_TRUE(WaitReady(clean));
    const auto report1 = clean.Get();
    ASSERT_TRUE(report1.fenced);
    EXPECT_FALSE(report1.allCleaned);
    ASSERT_EQ(report1.entries.size(), 1);
    bool sawUncertain = false;
    for (const auto &entry : report1.entries) {
        if (entry.uncertain || entry.absent) { sawUncertain = true; }
        EXPECT_FALSE(entry.deleted);  // nothing PROVEN cleaned
    }
    EXPECT_TRUE(sawUncertain);
    // Tombstone and registrations are RETAINED.
    EXPECT_TRUE(metaCore_->Exists(fenceKey));

    // The late SDK write lands on the unique key AFTER the cleanup.
    {
        std::lock_guard<std::mutex> guard(ds_->lock);
        ds_->data[snapKey] = "late-sdk-write";
    }

    // A NEW generation of the SAME instance (different request + runtime)
    // lives beside the retired one — untouched by any replay of the old
    // generation (prefix boundary).
    SeedRecord("inst-l", "req-l2", "rt-l2");
    auto fnew = Save("inst-l", "rt-l2", "req-l2", "state-l2");
    ASSERT_TRUE(WaitReady(fnew));
    ASSERT_EQ(fnew.Get().code(), common::ErrorCode::ERR_NONE);
    const auto newSnap = fnew.Get().checkpointid();

    // Replay the retired generation's cleanup: the specific Del fault is
    // cleared first (a retry would have a healthy store).
    ds_->forcedDel.clear();
    auto clean2 = StateHandler::CleanupRetiredSnapshots("inst-l", "req-l", "rt-l");
    ASSERT_TRUE(WaitReady(clean2));
    EXPECT_FALSE(clean2.Get().allCleaned);  // UNKNOWN entries never proven
    // The LATE old write is explicitly gone after the replay...
    {
        std::lock_guard<std::mutex> guard(ds_->lock);
        EXPECT_EQ(ds_->data.count(snapKey), 0u);
    }
    // ...while the new generation's snapshot, reference, and registration
    // are untouched.
    EXPECT_EQ(ds_->DelCount(newSnap), 0);
    EXPECT_TRUE(metaCore_->Exists(StateActor::StateRefKey("inst-l", "req-l2")));
    {
        // The new generation's registration prefix still holds its entry.
        std::lock_guard<std::mutex> guard(metaCore_->lock);
        size_t newGenRegs = 0;
        for (const auto &entry : metaCore_->kv) {
            if (entry.first.rfind(StateActor::RegistrationPrefix("inst-l", "req-l2"), 0) == 0) {
                newGenRegs += 1;
            }
        }
        EXPECT_EQ(newGenRegs, 1u);
    }
}

// ── restart-replay ─────────────────────────────────────────────────────────
TEST_F(StateIsolation, RestartReplayNewActorCleansFromTombstone)
{
    SeedRecord("inst-m", "req-m", "rt-m");
    auto f = Save("inst-m", "rt-m", "req-m", "state-m");
    ASSERT_TRUE(WaitReady(f));
    ASSERT_EQ(f.Get().code(), common::ErrorCode::ERR_NONE);
    const auto snapKey = f.Get().checkpointid();
    // Park the first CLEANED-marker promotion so the first actor exits
    // with the proof unwritten (crash between Del and CLEANED).
    std::string regKey;
    {
        const auto prefix = StateActor::RegistrationPrefix("inst-m", "req-m");
        std::lock_guard<std::mutex> guard(metaCore_->lock);
        for (const auto &entry : metaCore_->kv) {
            if (entry.first.rfind(prefix, 0) == 0) {
                regKey = entry.first;
                break;
            }
        }
    }
    ASSERT_FALSE(regKey.empty());
    metaCore_->AddGate(OnThenKey(regKey));

    auto fence = StateHandler::RetireGeneration("inst-m", "req-m", "rt-m");
    ASSERT_TRUE(WaitReady(fence));
    ASSERT_TRUE(fence.Get().fenced);

    // Fire the cleanup; the marker promotion parks in the gate — actor 1
    // now "crashes" between the successful Del and the CLEANED write.
    auto clean = StateHandler::CleanupRetiredSnapshots("inst-m", "req-m", "rt-m");
    bool parked = false;
    for (int i = 0; i < 500 && !parked; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        parked = metaCore_->GateCount() == 1;
    }
    ASSERT_TRUE(parked);
    // The data-phase already ran (Del done, marker not written).
    EXPECT_EQ(ds_->DelCount(snapKey), 1);

    // Actor 1 dies with the marker unwritten and the record deleted.
    {
        std::lock_guard<std::mutex> guard(metaCore_->lock);
        metaCore_->kv.erase(GenInstanceKey(kFuncKey, "inst-m", "req-m").Get());
        metaCore_->gated.clear();  // the died process's parked txn evaporates
    }
    (void)clean;
    litebus::Terminate(actor_->GetAID());
    litebus::Await(actor_->GetAID());
    actor_ = nullptr;

    // A NEW actor — no volatile tables, owner record gone — starts on the
    // SAME persistent stores; the real startup wiring must clean the
    // generation (Del of already-absent data returns OK in the map model;
    // the marker then persists and the report converges).
    auto actor2 = std::make_shared<StateActor>(stateClient_);
    litebus::Spawn(actor2);
    StateActor::replayInitialDelayMs.store(10);
    StateActor::replayPeriodMs.store(50);
    actor2->BindSaveRegistry(meta_, [](const std::string &, const std::string &,
                                       const std::string &) { return kFuncKey; });
    actor2->SetSaveExecutor([](const std::string &, const std::string &, const std::string &,
                               const StateActor::GuardedStoreOp &op) { return op(); });

    bool converged = false;
    for (int i = 0; i < 200 && !converged; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        MapRecord rec;
        if (metaCore_->Lookup(regKey, rec)) {
            const auto tuple = StateActor::ParseRegistration(rec.value);
            converged = (tuple.status == "CLEANED");
        }
    }
    litebus::Terminate(actor2->GetAID());
    litebus::Await(actor2->GetAID());
    EXPECT_TRUE(converged);
    // The tombstone itself is retained forever.
    EXPECT_TRUE(metaCore_->Exists(StateActor::RetirementKey("inst-m", "req-m")));
}

// ── registered-prefix-integrity ────────────────────────────────────────────
TEST_F(StateIsolation, RegisteredPrefixIntegrityNeighborsAndCorruption)
{
    const std::string tricky = "in:st@nce%1";
    const std::string request = "re%q:1";
    SeedRecord(tricky, request, "rt-n");
    auto f = Save(tricky, "rt-n", request, "state-n");
    ASSERT_TRUE(WaitReady(f));
    ASSERT_EQ(f.Get().code(), common::ErrorCode::ERR_NONE);
    const auto snapKey = f.Get().checkpointid();

    // Neighbor generations/instances with live state.
    SeedRecord(tricky, "re%q:1x", "rt-n2");
    auto fNeighborGen = Save(tricky, "rt-n2", "re%q:1x", "neighbor-gen");
    ASSERT_TRUE(WaitReady(fNeighborGen));
    ASSERT_EQ(fNeighborGen.Get().code(), common::ErrorCode::ERR_NONE);
    SeedRecord("in:st@nce%12", request, "rt-n3");
    auto fNeighborInst = Save("in:st@nce%12", "rt-n3", request, "neighbor-inst");
    ASSERT_TRUE(WaitReady(fNeighborInst));
    ASSERT_EQ(fNeighborInst.Get().code(), common::ErrorCode::ERR_NONE);

    // Corrupt registrations SEEDED directly inside the target's prefix.
    const auto prefix = StateActor::RegistrationPrefix(tricky, request);
    metaCore_->Seed(prefix + "bad-tuple", "only|six|fields|here|and|five");
    StateActor::SaveRegistration foreign;
    foreign.saveOpID = "11111111-2222-3333-4444-555555555555";
    foreign.snapshotKey = StateClient::ComposeSnapshotKey(tricky, request);
    foreign.instanceID = tricky;
    foreign.creationRequest = request;
    foreign.runtimeID = "rt-n";
    foreign.status = "COMMITTED";
    // key-opID mismatch: key names a different op than the tuple carries.
    metaCore_->Seed(prefix + "99999999-8888-7777-6666-555555555555",
                    StateActor::SerializeRegistration(foreign));
    StateActor::SaveRegistration nonCanonical = foreign;
    nonCanonical.status = "PENDING";
    const std::string canonical = StateActor::SerializeRegistration(nonCanonical);
    metaCore_->Seed(prefix + "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", canonical + " ");

    auto fence = StateHandler::RetireGeneration(tricky, request, "rt-n");
    ASSERT_TRUE(WaitReady(fence));
    ASSERT_TRUE(fence.Get().fenced);

    // Ambiguous reference read: the conditional reference delete must be
    // refused, explicitly unknown.
    metaCore_->forcedGets.push_back({ StateActor::StateRefKey(tricky, request),
                                      GetOutcome::AMBIGUOUS_TWO_KVS });
    auto clean = StateHandler::CleanupRetiredSnapshots(tricky, request, "rt-n");
    ASSERT_TRUE(WaitReady(clean));
    const auto &report = clean.Get();
    ASSERT_TRUE(report.fenced);
    EXPECT_FALSE(report.allCleaned);
    EXPECT_TRUE(report.refUnknown);
    // The one VALID entry was cleaned exactly; the corrupt ones were not.
    EXPECT_EQ(ds_->DelCount(snapKey), 1);
    EXPECT_EQ(ds_->DelCount(fNeighborGen.Get().checkpointid()), 0);
    EXPECT_EQ(ds_->DelCount(fNeighborInst.Get().checkpointid()), 0);
    size_t invalidCount = 0;
    for (const auto &entry : report.entries) {
        if (entry.invalid) { invalidCount += 1; }
    }
    EXPECT_GE(invalidCount, 3);  // bad tuple, opID mismatch, non-canonical
}

// ── recover-ref-fence-errors ───────────────────────────────────────────────
TEST_F(StateIsolation, RecoverRefFenceErrorsRefuseWithoutDsGet)
{
    // Retired generation: refused, tombstone cannot be bypassed.
    SeedRecord("inst-o", "req-o", "rt-o");
    {
        StateActor::FenceRecord fence;
        fence.instanceID = "inst-o";
        fence.creationRequest = "req-o";
        fence.runtimeID = "rt-o";
        fence.authorityRecordKey = GenInstanceKey(kFuncKey, "inst-o", "req-o").Get();
        fence.authorityModRevision = 1;
        fence.status = "RETIRED";
        metaCore_->Seed(StateActor::RetirementKey("inst-o", "req-o"),
                        StateActor::SerializeFence(fence));
    }
    auto retired = StateHandler::ResolveCheckpointRef("inst-o", "req-o");
    ASSERT_TRUE(WaitReady(retired));
    EXPECT_FALSE(retired.Get().ok);

    // Fence read failure: the resolution future itself FAILS (Then
    // callbacks are skipped on failed futures) — the caller must see an
    // explicit failed future, not a hang and not a fake success.
    SeedRecord("inst-p", "req-p", "rt-p");
    auto fp = Save("inst-p", "rt-p", "req-p", "state-p");
    ASSERT_TRUE(WaitReady(fp));
    ASSERT_EQ(fp.Get().code(), common::ErrorCode::ERR_NONE);
    metaCore_->forcedGets.push_back({ StateActor::RetirementKey("inst-p", "req-p"),
                                      GetOutcome::FAIL_FUTURE });
    auto fenceFailed = StateHandler::ResolveCheckpointRef("inst-p", "req-p");
    EXPECT_TRUE(fenceFailed.WaitFor(15000).IsOK());  // settled...
    EXPECT_TRUE(fenceFailed.IsError());              // ...as an explicit failure

    // Reference read failure: same explicit-failure contract.
    metaCore_->forcedGets.push_back({ StateActor::StateRefKey("inst-p", "req-p"),
                                      GetOutcome::FAIL_FUTURE });
    auto refFailed = StateHandler::ResolveCheckpointRef("inst-p", "req-p");
    EXPECT_TRUE(refFailed.WaitFor(15000).IsOK());
    EXPECT_TRUE(refFailed.IsError());
    metaCore_->forcedGets.push_back({ StateActor::StateRefKey("inst-p", "req-p"),
                                      GetOutcome::STATUS_ERROR });
    auto refError = StateHandler::ResolveCheckpointRef("inst-p", "req-p");
    ASSERT_TRUE(WaitReady(refError));
    EXPECT_FALSE(refError.Get().ok);
    metaCore_->Seed(StateActor::StateRefKey("inst-p", "req-p"), "corrupt-ref-value");
    auto refCorrupt = StateHandler::ResolveCheckpointRef("inst-p", "req-p");
    ASSERT_TRUE(WaitReady(refCorrupt));
    EXPECT_FALSE(refCorrupt.Get().ok);

    // No DS Get happened for ANY refusal variant.
    {
        std::lock_guard<std::mutex> guard(ds_->lock);
        EXPECT_TRUE(ds_->getCalls.empty());
    }

    // A successor runtime replaced the machine's runtime: the published
    // reference still names the ORIGINAL generation identity.
    SeedRecord("inst-p", "req-p", "rt-SUCCESSOR");
    metaCore_->Seed(StateActor::StateRefKey("inst-p", "req-p"), fp.Get().checkpointid());
    auto ok = StateHandler::ResolveCheckpointRef("inst-p", "req-p");
    ASSERT_TRUE(WaitReady(ok));
    ASSERT_TRUE(ok.Get().ok);
    const auto parsed = StateClient::ParseSnapshotKey(ok.Get().checkpointRef);
    EXPECT_EQ(parsed.instanceId, "inst-p");
    EXPECT_EQ(parsed.creationRequest, "req-p");

    // Explicit double absence: the legacy migration key, and ONLY then.
    const std::string legacyInst = "inst-q";
    const std::string legacyReq = "req-q";
    auto legacy = StateHandler::ResolveCheckpointRef(legacyInst, legacyReq);
    ASSERT_TRUE(WaitReady(legacy));
    ASSERT_TRUE(legacy.Get().ok);
    EXPECT_TRUE(legacy.Get().legacy);
    EXPECT_EQ(legacy.Get().checkpointRef, StateClient::ComposeStateKey(legacyInst, legacyReq));
}

// ── replay fairness: 17 persistent tombstones, bounded rounds ───────────────
// The first slots contain a permanently-uncertain generation, a corrupt
// tombstone, and an already-CLEANED generation; the batch size is 8. A
// plain prefix+limit scan would return the SAME first 8 forever — the
// cursor must advance so the LAST generation (#16) is eventually cleaned
// while the stuck head stays retained (not starved, not erased).
TEST_F(StateIsolation, ReplayCursorDoesNotStarveLaterTombstones)
{
    constexpr int kTotal = 17;
    std::vector<std::string> lastRegKeys;
    for (int i = 0; i < kTotal; ++i) {
        const std::string inst = "inst-f" + std::to_string(i);
        const std::string req = "req-f" + std::to_string(i);
        SeedRecord(inst, req, "rt-f");
        auto f = Save(inst, "rt-f", req, "state-f" + std::to_string(i));
        ASSERT_TRUE(WaitReady(f));
        ASSERT_EQ(f.Get().code(), common::ErrorCode::ERR_NONE);
        auto fence = StateHandler::RetireGeneration(inst, req, "rt-f");
        ASSERT_TRUE(WaitReady(fence));
        ASSERT_TRUE(fence.Get().fenced);
        std::string regKey;
        {
            const auto prefix = StateActor::RegistrationPrefix(inst, req);
            std::lock_guard<std::mutex> guard(metaCore_->lock);
            for (const auto &entry : metaCore_->kv) {
                if (entry.first.rfind(prefix, 0) == 0) { regKey = entry.first; }
            }
        }
        ASSERT_FALSE(regKey.empty());
        lastRegKeys.push_back(regKey);
        if (i == 0) {
            // The HEAD generation stays permanently uncertain: its DS Del
            // keeps failing (a stuck store).
            MapRecord regRec;
            ASSERT_TRUE(metaCore_->Lookup(regKey, regRec));
            const auto tuple = StateActor::ParseRegistration(regRec.value);
            ds_->ForceDel(tuple.snapshotKey, DsAction::TIMEOUT);
        }
        if (i == 1) {
            // A CORRUPT tombstone beside the valid ones (retained, never
            // processed, must not stall the cursor).
            metaCore_->Seed("stateret:v1:corrupt-zz", "garbage-not-a-fence");
        }
        if (i == 2) {
            // An ALREADY-CONVERGED generation (CLEANED marker in place).
            auto clean = StateHandler::CleanupRetiredSnapshots(inst, req, "rt-f");
            ASSERT_TRUE(WaitReady(clean));
            ASSERT_TRUE(clean.Get().allCleaned);
        }
    }
    // Actor 1 hands the SAME persistent stores to a brand-new actor with a
    // short replay cadence.
    litebus::Terminate(actor_->GetAID());
    litebus::Await(actor_->GetAID());
    actor_ = nullptr;
    auto actor2 = std::make_shared<StateActor>(stateClient_);
    litebus::Spawn(actor2);
    StateActor::replayInitialDelayMs.store(10);
    StateActor::replayPeriodMs.store(50);
    actor2->BindSaveRegistry(meta_, [](const std::string &, const std::string &,
                                       const std::string &) { return kFuncKey; });
    actor2->SetSaveExecutor([](const std::string &, const std::string &, const std::string &,
                               const StateActor::GuardedStoreOp &op) { return op(); });

    // The LAST generation converges (CLEANED) although the stuck head can
    // never clear — the cursor advanced past it over multiple rounds.
    bool tailConverged = false;
    for (int i = 0; i < 600 && !tailConverged; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        MapRecord rec;
        if (metaCore_->Lookup(lastRegKeys[kTotal - 1], rec)) {
            tailConverged = (StateActor::ParseRegistration(rec.value).status == "CLEANED");
        }
    }
    litebus::Terminate(actor2->GetAID());
    litebus::Await(actor2->GetAID());
    EXPECT_TRUE(tailConverged) << "the cursor must reach past the stuck head";

    // The head stays RETAINED and unconverged (uncertain forever is the
    // honest state; nothing was erased to fake progress).
    MapRecord headRec;
    ASSERT_TRUE(metaCore_->Lookup(lastRegKeys[0], headRec));
    EXPECT_NE(StateActor::ParseRegistration(headRec.value).status, "CLEANED");
    EXPECT_TRUE(metaCore_->Exists("stateret:v1:corrupt-zz"));
    EXPECT_TRUE(metaCore_->Exists(
        StateActor::RetirementKey("inst-f0", "req-f0")));
}

// ── replay survives a FAILED scan future ────────────────────────────────────
// The periodic timer is scheduled from the scan chain's OnComplete (which
// fires on failed futures too, unlike Then): one forced FAIL_FUTURE on
// the prefix read must not stop the replay — the NEXT round recovers and
// cleans the generation.
TEST_F(StateIsolation, ReplayRecoversAfterFailedScanFuture)
{
    SeedRecord("inst-r", "req-r", "rt-r");
    auto f = Save("inst-r", "rt-r", "req-r", "state-r");
    ASSERT_TRUE(WaitReady(f));
    ASSERT_EQ(f.Get().code(), common::ErrorCode::ERR_NONE);
    std::string regKey;
    {
        const auto prefix = StateActor::RegistrationPrefix("inst-r", "req-r");
        std::lock_guard<std::mutex> guard(metaCore_->lock);
        for (const auto &entry : metaCore_->kv) {
            if (entry.first.rfind(prefix, 0) == 0) { regKey = entry.first; }
        }
    }
    ASSERT_FALSE(regKey.empty());
    auto fence = StateHandler::RetireGeneration("inst-r", "req-r", "rt-r");
    ASSERT_TRUE(WaitReady(fence));
    ASSERT_TRUE(fence.Get().fenced);

    litebus::Terminate(actor_->GetAID());
    litebus::Await(actor_->GetAID());
    actor_ = nullptr;

    // The FIRST scan round fails as a Future; the cadence must survive it.
    metaCore_->forcedGets.push_back({ "stateret:v1:", GetOutcome::FAIL_FUTURE, true });
    auto actor2 = std::make_shared<StateActor>(stateClient_);
    litebus::Spawn(actor2);
    StateActor::replayInitialDelayMs.store(10);
    StateActor::replayPeriodMs.store(50);
    actor2->BindSaveRegistry(meta_, [](const std::string &, const std::string &,
                                       const std::string &) { return kFuncKey; });
    actor2->SetSaveExecutor([](const std::string &, const std::string &, const std::string &,
                               const StateActor::GuardedStoreOp &op) { return op(); });

    bool converged = false;
    for (int i = 0; i < 600 && !converged; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        MapRecord rec;
        if (metaCore_->Lookup(regKey, rec)) {
            converged = (StateActor::ParseRegistration(rec.value).status == "CLEANED");
        }
    }
    litebus::Terminate(actor2->GetAID());
    litebus::Await(actor2->GetAID());
    EXPECT_TRUE(converged) << "the replay cadence must survive a failed scan future";
}

}  // namespace functionsystem::function_proxy::test
