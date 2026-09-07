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

#include "function_master/snap_manager/restore_replay_ledger.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>

namespace functionsystem::test {

using functionsystem::snap_manager::RestoreReplayLedger;
using BeginStatus = RestoreReplayLedger::BeginStatus;
using CompleteStatus = RestoreReplayLedger::CompleteStatus;

namespace {

constexpr size_t SIZE_MAX_VALUE = std::numeric_limits<size_t>::max();

RestoreReplayLedger::Key MakeKey(const std::string &caller = "local-scheduler-a",
                                 const std::string &url = "tcp://10.0.0.1:12345",
                                 const std::string &requestID = "req-1")
{
    return RestoreReplayLedger::Key{caller, url, requestID};
}

// Stand-in for the serialized RestoreSnapshotRequest wire bytes: the ledger
// only compares them for identity, never interprets them.
std::string MakeWireRequest(const std::string &checkpointID = "ckpt-1",
                            const std::string &options = "opts-a")
{
    return "restore{checkpointID=" + checkpointID + ",options=" + options + "}";
}

std::string MakeWireResponse(const std::string &requestID, int32_t code, const std::string &instanceID = "")
{
    return "response{requestID=" + requestID + ",code=" + std::to_string(code) +
           (instanceID.empty() ? "" : ",instanceID=" + instanceID) + "}";
}

/**
 * Minimal model of the master SnapStart flow over the ledger: only a NEW
 * begin may dispatch a restore, and only the first stored completion sends.
 * Every other outcome must leave both counters untouched.
 */
class DispatchModel {
public:
    explicit DispatchModel(RestoreReplayLedger &ledger) : ledger_(ledger) {}

    RestoreReplayLedger::BeginResult Submit(const RestoreReplayLedger::Key &key, const std::string &request)
    {
        auto result = ledger_.Begin(key, request);
        if (result.status == BeginStatus::NEW) {
            dispatches_++;
        }
        return result;
    }

    CompleteStatus Deliver(const RestoreReplayLedger::Key &key,
                           RestoreReplayLedger::Ticket ticket,
                           const std::string &response)
    {
        auto status = ledger_.Complete(key, ticket, response);
        if (status == CompleteStatus::STORED) {
            sends_++;
        }
        return status;
    }

    int Dispatches() const { return dispatches_; }
    int Sends() const { return sends_; }

private:
    RestoreReplayLedger &ledger_;
    int dispatches_{0};
    int sends_{0};
};

}  // namespace

class RestoreReplayLedgerTest : public ::testing::Test {
protected:
    RestoreReplayLedger::Limits MakeLimits(size_t maxEntries, size_t maxReservedBytes,
                                           size_t maxMessageBytes = 256)
    {
        RestoreReplayLedger::Limits limits;
        limits.maxEntries = maxEntries;
        limits.maxReservedBytes = maxReservedBytes;
        limits.maxMessageBytes = maxMessageBytes;
        return limits;
    }
};

TEST_F(RestoreReplayLedgerTest, EmptyRequestIDIsRejectedBeforeAnythingElse)
{
    RestoreReplayLedger ledger(MakeLimits(4, 8192));
    DispatchModel model(ledger);

    // An empty requestID cannot open an entry and must never schedule.
    auto result = model.Submit(RestoreReplayLedger::Key{"caller-a", "tcp://10.0.0.1:1", ""},
                               MakeWireRequest());
    EXPECT_EQ(result.status, BeginStatus::REJECTED);
    EXPECT_EQ(model.Dispatches(), 0);
    EXPECT_EQ(ledger.Size(), 0U);
    EXPECT_EQ(ledger.ReservedBytes(), 0U);
}

TEST_F(RestoreReplayLedgerTest, InFlightRetryIsAbsorbedWithoutSecondDispatch)
{
    RestoreReplayLedger ledger(MakeLimits(4, 8192));
    DispatchModel model(ledger);
    const auto key = MakeKey();
    const auto request = MakeWireRequest();

    auto first = model.Submit(key, request);
    ASSERT_EQ(first.status, BeginStatus::NEW);
    ASSERT_NE(first.ticket, 0U);  // ticket 0 is never issued

    auto retry = model.Submit(key, request);
    EXPECT_EQ(retry.status, BeginStatus::IN_FLIGHT);
    EXPECT_EQ(retry.ticket, 0U);
    EXPECT_TRUE(retry.response.empty());

    EXPECT_EQ(model.Dispatches(), 1);
    EXPECT_EQ(ledger.Size(), 1U);
}

TEST_F(RestoreReplayLedgerTest, CompletedResponseReplaysIdenticallyAfterResponseLoss)
{
    RestoreReplayLedger ledger(MakeLimits(4, 8192));
    DispatchModel model(ledger);
    const auto key = MakeKey("local-scheduler-a", "tcp://10.0.0.1:12345", "req-replay");
    const auto request = MakeWireRequest("ckpt-42", "opts-a");

    auto begin = model.Submit(key, request);
    ASSERT_EQ(begin.status, BeginStatus::NEW);

    const std::string response = MakeWireResponse("req-replay", 0, "inst-ckpt-42");
    ASSERT_EQ(model.Deliver(key, begin.ticket, response), CompleteStatus::STORED);
    EXPECT_EQ(model.Sends(), 1);
    // Model the response being lost in transit after it was sent.

    auto retry = model.Submit(key, request);
    ASSERT_EQ(retry.status, BeginStatus::REPLAY);
    EXPECT_EQ(retry.response, response);  // byte-identical stored response

    // The retry is answered from the ledger: no second restore dispatched.
    EXPECT_EQ(model.Dispatches(), 1);
}

TEST_F(RestoreReplayLedgerTest, FailureResponseReplaysToo)
{
    RestoreReplayLedger ledger(MakeLimits(4, 8192));
    DispatchModel model(ledger);
    const auto key = MakeKey("local-scheduler-a", "tcp://10.0.0.1:12345", "req-fail");
    const auto request = MakeWireRequest("ckpt-missing");

    auto begin = model.Submit(key, request);
    ASSERT_EQ(begin.status, BeginStatus::NEW);

    const std::string failure = MakeWireResponse("req-fail", 1003);  // snapshot not found
    ASSERT_EQ(model.Deliver(key, begin.ticket, failure), CompleteStatus::STORED);

    // A retry of a terminally-failed request replays the cached failure
    // instead of hitting the scheduler again.
    auto retry = model.Submit(key, request);
    ASSERT_EQ(retry.status, BeginStatus::REPLAY);
    EXPECT_EQ(retry.response, failure);
    EXPECT_EQ(model.Dispatches(), 1);
}

TEST_F(RestoreReplayLedgerTest, ChangedCheckpointOrOptionsConflicts)
{
    RestoreReplayLedger ledger(MakeLimits(4, 8192));
    DispatchModel model(ledger);
    const auto key = MakeKey("local-scheduler-a", "tcp://10.0.0.1:12345", "req-conflict");

    auto begin = model.Submit(key, MakeWireRequest("ckpt-1", "opts-a"));
    ASSERT_EQ(begin.status, BeginStatus::NEW);

    // Same requestID, different options: conflict while in flight.
    EXPECT_EQ(model.Submit(key, MakeWireRequest("ckpt-1", "opts-b")).status, BeginStatus::CONFLICT);
    // Same requestID, different checkpoint: conflict while in flight.
    EXPECT_EQ(model.Submit(key, MakeWireRequest("ckpt-2", "opts-a")).status, BeginStatus::CONFLICT);

    // Conflict neither mutates the entry nor blocks the original request.
    ASSERT_EQ(model.Deliver(key, begin.ticket, MakeWireResponse("req-conflict", 0, "inst-1")),
              CompleteStatus::STORED);
    EXPECT_EQ(model.Submit(key, MakeWireRequest("ckpt-1", "opts-b")).status, BeginStatus::CONFLICT);
    EXPECT_EQ(model.Submit(key, MakeWireRequest("ckpt-1", "opts-a")).status, BeginStatus::REPLAY);
    EXPECT_EQ(model.Dispatches(), 1);
    EXPECT_EQ(ledger.Size(), 1U);
}

TEST_F(RestoreReplayLedgerTest, DistinctCallersAreIsolated)
{
    RestoreReplayLedger ledger(MakeLimits(4, 8192));
    DispatchModel model(ledger);
    const auto request = MakeWireRequest("ckpt-shared");

    // Same requestID from different caller actors: independent entries, both
    // dispatch (the old global requestID marker made them collide).
    auto a = model.Submit(MakeKey("local-scheduler-a", "tcp://10.0.0.1:12345", "req-shared"), request);
    auto b = model.Submit(MakeKey("local-scheduler-b", "tcp://10.0.0.2:12345", "req-shared"), request);
    ASSERT_EQ(a.status, BeginStatus::NEW);
    ASSERT_EQ(b.status, BeginStatus::NEW);
    EXPECT_NE(a.ticket, b.ticket);
    EXPECT_EQ(model.Dispatches(), 2);

    // Same caller name at a different url is still a distinct caller.
    EXPECT_EQ(model.Submit(MakeKey("local-scheduler-a", "tcp://10.0.0.9:12345", "req-shared"), request).status,
              BeginStatus::NEW);
    EXPECT_EQ(model.Dispatches(), 3);

    // Completing one caller's entry does not answer or complete the others.
    ASSERT_EQ(model.Deliver(MakeKey("local-scheduler-a", "tcp://10.0.0.1:12345", "req-shared"),
                            a.ticket, MakeWireResponse("req-shared", 0, "inst-a")),
              CompleteStatus::STORED);
    EXPECT_EQ(model.Submit(MakeKey("local-scheduler-a", "tcp://10.0.0.1:12345", "req-shared"), request).status,
              BeginStatus::REPLAY);
    EXPECT_EQ(model.Submit(MakeKey("local-scheduler-b", "tcp://10.0.0.2:12345", "req-shared"), request).status,
              BeginStatus::IN_FLIGHT);
    EXPECT_EQ(model.Submit(MakeKey("local-scheduler-a", "tcp://10.0.0.9:12345", "req-shared"), request).status,
              BeginStatus::IN_FLIGHT);
}

TEST_F(RestoreReplayLedgerTest, FirstCompletionWinsAndWrongTicketRejected)
{
    RestoreReplayLedger ledger(MakeLimits(4, 8192));
    DispatchModel model(ledger);
    const auto key = MakeKey();
    const auto request = MakeWireRequest();

    auto begin = model.Submit(key, request);
    ASSERT_EQ(begin.status, BeginStatus::NEW);

    // Wrong ticket cannot complete the entry.
    EXPECT_EQ(model.Deliver(key, begin.ticket + 1, MakeWireResponse("req-1", 0, "inst-wrong")),
              CompleteStatus::STALE_TICKET);
    EXPECT_EQ(model.Submit(key, request).status, BeginStatus::IN_FLIGHT);  // still uncompleted

    // Unknown key cannot complete anything.
    EXPECT_EQ(model.Deliver(MakeKey("other", "tcp://10.0.0.1:12345", "req-1"), begin.ticket, "x"),
              CompleteStatus::UNKNOWN_ENTRY);

    const std::string first = MakeWireResponse("req-1", 0, "inst-first");
    ASSERT_EQ(model.Deliver(key, begin.ticket, first), CompleteStatus::STORED);

    // Double completion with the right ticket cannot overwrite the stored
    // response (first completion wins).
    EXPECT_EQ(model.Deliver(key, begin.ticket, MakeWireResponse("req-1", 0, "inst-second")),
              CompleteStatus::ALREADY_COMPLETED);
    auto retry = model.Submit(key, request);
    ASSERT_EQ(retry.status, BeginStatus::REPLAY);
    EXPECT_EQ(retry.response, first);
    EXPECT_EQ(model.Sends(), 1);
}

TEST_F(RestoreReplayLedgerTest, EntryBoundReportsFullButExistingEntriesStillReplay)
{
    RestoreReplayLedger ledger(MakeLimits(2, 8192));
    DispatchModel model(ledger);
    const auto request = MakeWireRequest();

    auto first = model.Submit(MakeKey("caller-a", "tcp://10.0.0.1:1", "req-a"), request);
    auto second = model.Submit(MakeKey("caller-b", "tcp://10.0.0.1:1", "req-b"), request);
    ASSERT_EQ(first.status, BeginStatus::NEW);
    ASSERT_EQ(second.status, BeginStatus::NEW);
    const size_t reservedAfterBegin = ledger.ReservedBytes();

    // Slot bound hit: capacity, not malformed input — and no entry is evicted
    // to make room.
    EXPECT_EQ(model.Submit(MakeKey("caller-c", "tcp://10.0.0.1:1", "req-c"), request).status,
              BeginStatus::FULL);
    EXPECT_EQ(ledger.Size(), 2U);
    EXPECT_EQ(ledger.ReservedBytes(), reservedAfterBegin);

    // The completed entry still replays and the in-flight one is still
    // absorbed; neither was evicted.
    ASSERT_EQ(model.Deliver(MakeKey("caller-a", "tcp://10.0.0.1:1", "req-a"), first.ticket,
                            MakeWireResponse("req-a", 0)),
              CompleteStatus::STORED);
    EXPECT_EQ(model.Submit(MakeKey("caller-a", "tcp://10.0.0.1:1", "req-a"), request).status,
              BeginStatus::REPLAY);
    EXPECT_EQ(model.Submit(MakeKey("caller-b", "tcp://10.0.0.1:1", "req-b"), request).status,
              BeginStatus::IN_FLIGHT);
    EXPECT_EQ(model.Dispatches(), 2);
}

TEST_F(RestoreReplayLedgerTest, ByteBoundReportsFullButReservationIsNeverFreed)
{
    // Tune the byte budget so exactly one entry fits: reservation is
    // key + request + max response bytes.
    const auto key = MakeKey();
    const auto request = MakeWireRequest();
    RestoreReplayLedger::Limits limits;
    limits.maxEntries = 8;
    limits.maxReservedBytes = key.callerName.size() + key.callerUrl.size() + key.requestID.size() +
                              request.size() + limits.maxMessageBytes;
    RestoreReplayLedger ledger(limits);
    DispatchModel model(ledger);

    auto begin = model.Submit(key, request);
    ASSERT_EQ(begin.status, BeginStatus::NEW);
    const size_t reservedAfterBegin = ledger.ReservedBytes();
    EXPECT_GT(reservedAfterBegin, 0U);

    // Byte bound hit: a second (even smaller) entry is refused as capacity.
    EXPECT_EQ(model.Submit(MakeKey("c", "tcp://1.1.1.1:1", "r"), "r").status, BeginStatus::FULL);
    EXPECT_EQ(ledger.Size(), 1U);

    // Completion never frees the reservation and never evicts the entry.
    ASSERT_EQ(model.Deliver(key, begin.ticket, MakeWireResponse("req-1", 0)), CompleteStatus::STORED);
    EXPECT_EQ(ledger.ReservedBytes(), reservedAfterBegin);
    auto retry = model.Submit(key, request);
    ASSERT_EQ(retry.status, BeginStatus::REPLAY);
    EXPECT_FALSE(retry.response.empty());
    EXPECT_EQ(model.Dispatches(), 1);
}

TEST_F(RestoreReplayLedgerTest, OversizedResponseLeavesEntryInFlightWithReservationHeld)
{
    RestoreReplayLedger ledger(MakeLimits(4, 8192, 32));
    DispatchModel model(ledger);
    const auto key = MakeKey();
    const std::string request = "req-a";  // fits the 32-byte message bound

    auto begin = model.Submit(key, request);
    ASSERT_EQ(begin.status, BeginStatus::NEW);
    const size_t reservedAfterBegin = ledger.ReservedBytes();

    const std::string oversized(33, 'x');  // exceeds the 32-byte response bound
    EXPECT_EQ(model.Deliver(key, begin.ticket, oversized), CompleteStatus::RESPONSE_OVERSIZED);
    EXPECT_EQ(model.Sends(), 0);  // never sent: it was never stored

    // Refused without mutation: still in flight, reservation intact, nothing
    // stored, so retries are absorbed instead of dispatching a second restore.
    EXPECT_EQ(model.Submit(key, request).status, BeginStatus::IN_FLIGHT);
    EXPECT_EQ(ledger.ReservedBytes(), reservedAfterBegin);

    // The refused attempt was not a completion: a later bounded response for
    // the same ticket still becomes the first (and final) stored answer.
    const std::string bounded = "resp:0";
    ASSERT_EQ(model.Deliver(key, begin.ticket, bounded), CompleteStatus::STORED);
    auto retry = model.Submit(key, request);
    ASSERT_EQ(retry.status, BeginStatus::REPLAY);
    EXPECT_EQ(retry.response, bounded);
    EXPECT_EQ(model.Dispatches(), 1);
}

TEST_F(RestoreReplayLedgerTest, MalformedIdentityAndOversizedRequestRejectedWithoutMutation)
{
    RestoreReplayLedger ledger(MakeLimits(4, 8192, 32));
    DispatchModel model(ledger);
    const std::string request = "req-a";  // fits the 32-byte message bound

    // Empty key components are rejected as malformed input.
    EXPECT_EQ(model.Submit(RestoreReplayLedger::Key{"", "tcp://10.0.0.1:1", "req-1"}, request).status,
              BeginStatus::REJECTED);
    EXPECT_EQ(model.Submit(RestoreReplayLedger::Key{"caller", "", "req-1"}, request).status,
              BeginStatus::REJECTED);
    EXPECT_EQ(model.Submit(RestoreReplayLedger::Key{"caller", "tcp://10.0.0.1:1", ""}, request).status,
              BeginStatus::REJECTED);
    // Oversized key component (requestID beyond the 32-byte bound).
    EXPECT_EQ(model.Submit(RestoreReplayLedger::Key{"caller", "tcp://10.0.0.1:1", std::string(33, 'r')},
                           request).status,
              BeginStatus::REJECTED);
    // Oversized request bytes.
    EXPECT_EQ(model.Submit(MakeKey(), std::string(33, 'q')).status, BeginStatus::REJECTED);

    // Every rejection happened before any mutation.
    EXPECT_EQ(ledger.Size(), 0U);
    EXPECT_EQ(ledger.ReservedBytes(), 0U);
    EXPECT_EQ(model.Dispatches(), 0);
}

TEST_F(RestoreReplayLedgerTest, HugeConfiguredBoundsOverflowAccountingAndRefuseWithoutMutation)
{
    // maxMessageBytes at SIZE_MAX makes the reservation arithmetic overflow
    // even for a tiny key: the begin must be refused, not wrapped into a
    // bogus acceptance. Only small strings are ever allocated.
    RestoreReplayLedger::Limits limits;
    limits.maxEntries = 4;
    limits.maxReservedBytes = SIZE_MAX_VALUE;
    limits.maxMessageBytes = SIZE_MAX_VALUE;
    RestoreReplayLedger ledger(limits);
    DispatchModel model(ledger);

    EXPECT_EQ(model.Submit(MakeKey(), MakeWireRequest()).status, BeginStatus::FULL);
    EXPECT_EQ(ledger.Size(), 0U);
    EXPECT_EQ(ledger.ReservedBytes(), 0U);
    EXPECT_EQ(model.Dispatches(), 0);
}

TEST_F(RestoreReplayLedgerTest, NearMaxBoundsReserveExactlyThenRefuseSecondEntry)
{
    // Tune maxMessageBytes so one entry's reservation is exactly SIZE_MAX:
    // accepted (no wrap), after which no capacity remains.
    const auto key = MakeKey();
    const auto request = MakeWireRequest();
    const size_t payload = key.callerName.size() + key.callerUrl.size() + key.requestID.size() +
                           request.size();
    RestoreReplayLedger::Limits limits;
    limits.maxEntries = 4;
    limits.maxReservedBytes = SIZE_MAX_VALUE;
    limits.maxMessageBytes = SIZE_MAX_VALUE - payload;
    RestoreReplayLedger ledger(limits);
    DispatchModel model(ledger);

    auto begin = model.Submit(key, request);
    ASSERT_EQ(begin.status, BeginStatus::NEW);
    EXPECT_EQ(ledger.ReservedBytes(), SIZE_MAX_VALUE);

    // Remaining capacity is zero: any further entry is refused as FULL
    // without wraparound or eviction, and the accepted entry is untouched.
    EXPECT_EQ(model.Submit(MakeKey("c2", "tcp://1.1.1.1:1", "r2"), "r").status, BeginStatus::FULL);
    EXPECT_EQ(ledger.Size(), 1U);
    EXPECT_EQ(ledger.ReservedBytes(), SIZE_MAX_VALUE);
    EXPECT_EQ(model.Submit(key, request).status, BeginStatus::IN_FLIGHT);
}

TEST_F(RestoreReplayLedgerTest, DefaultLimitsRejectOversizedMessages)
{
    RestoreReplayLedger ledger;  // production defaults: 4096 slots, 64MiB, 64KiB messages
    DispatchModel model(ledger);

    EXPECT_EQ(model.Submit(MakeKey(), std::string(64ULL * 1024 + 1, 'q')).status, BeginStatus::REJECTED);
    EXPECT_EQ(ledger.Size(), 0U);

    auto begin = model.Submit(MakeKey(), MakeWireRequest());
    ASSERT_EQ(begin.status, BeginStatus::NEW);
    EXPECT_EQ(model.Deliver(MakeKey(), begin.ticket, std::string(64ULL * 1024 + 1, 'r')),
              CompleteStatus::RESPONSE_OVERSIZED);
    EXPECT_EQ(model.Submit(MakeKey(), MakeWireRequest()).status, BeginStatus::IN_FLIGHT);
}

// Covers the pure identity predicate the SnapManagerActor completion path
// uses to reject responses not minted for their ledger entry. It proves
// bookkeeping identity only, never runtime physical identity.
TEST_F(RestoreReplayLedgerTest, MatchesResponderIdentityPredicate)
{
    const auto key = MakeKey("local-scheduler-a", "tcp://10.0.0.1:12345", "req-1");

    EXPECT_TRUE(RestoreReplayLedger::MatchesResponder(key, "local-scheduler-a", "tcp://10.0.0.1:12345",
                                                      "req-1"));
    // Any single component mismatch rejects.
    EXPECT_FALSE(RestoreReplayLedger::MatchesResponder(key, "local-scheduler-b", "tcp://10.0.0.1:12345",
                                                       "req-1"));
    EXPECT_FALSE(RestoreReplayLedger::MatchesResponder(key, "local-scheduler-a", "tcp://10.0.0.9:12345",
                                                       "req-1"));
    EXPECT_FALSE(RestoreReplayLedger::MatchesResponder(key, "local-scheduler-a", "tcp://10.0.0.1:12345",
                                                       "req-2"));
    // Empty responder identity never matches a valid entry key.
    EXPECT_FALSE(RestoreReplayLedger::MatchesResponder(key, "", "", ""));
}

}  // namespace functionsystem::test
