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

#ifndef FUNCTION_MASTER_SNAP_MANAGER_RESTORE_REPLAY_LEDGER_H
#define FUNCTION_MASTER_SNAP_MANAGER_RESTORE_REPLAY_LEDGER_H

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>

namespace functionsystem::snap_manager {

/**
 * Bounded ledger that keeps completed SnapStart (restore) requests replayable.
 *
 * Motivation: MasterBusiness::HandleSnapStart used to track in-flight restores
 * with a plain requestID set and erased the marker before sending the
 * response. When the response was lost, a retry of the same request scheduled
 * a SECOND restore of the same checkpoint; a retry carrying a different
 * payload under the same requestID was silently absorbed as the same request;
 * and two different actors reusing one requestID collided on a single global
 * marker.
 *
 * Semantics:
 * - Entries are keyed by (caller AID name, caller AID url, requestID); all key
 *   components must be nonempty and bounded. Distinct callers are isolated.
 *   An empty or malformed requestID is REJECTED before any other state is
 *   consulted: the caller must never schedule such a request.
 * - An entry stores the EXACT serialized request it was opened with. A retry
 *   with identical bytes shares the entry; different bytes under the same key
 *   is a CONFLICT. CONFLICT only reports a request-payload mismatch: it is not
 *   a source-identity, physical-execution or physical-deletion claim.
 * - Begin reserves the maximum response bytes plus the key/request bytes up
 *   front, so a completion never needs to evict another uncertain or
 *   completed entry. There is no eviction, TTL, or completion-time reset:
 *   entries (and their reservations) live until process exit. Reservation
 *   arithmetic is overflow-checked: with pathological configured bounds the
 *   accounting itself can overflow, and such a Begin is refused as FULL
 *   without mutation rather than wrapping into a bogus acceptance.
 * - The first valid completion wins: its EXACT serialized response is stored
 *   immutably and replayed byte-for-byte to later identical retries. Stale or
 *   double completions cannot overwrite it. An oversized response is refused
 *   without mutation: the entry stays in flight with its reservation held, so
 *   retries keep being absorbed instead of dispatching a second restore.
 * - REJECTED means the caller's input is malformed or oversized. FULL means
 *   the ledger cannot accommodate the entry (slot bound, byte bound, or
 *   accounting/ticket exhaustion) while existing entries are unaffected and
 *   still replay.
 *
 * Limitations (explicit, not solved here): the ledger is process-local
 * in-memory state. It survives in-process master/slave role changes of the
 * owning actor, but NOT a process restart, and it offers no crash recovery or
 * persistence. Because nothing is ever evicted, a long-running master
 * accumulates one entry per distinct restore request; a pressure campaign of
 * hundreds of trials generates far MORE restore requests than trials (every
 * park/restore cycle issues a SnapStart), so the ledger eventually reports
 * FULL and new restores fail busy. A safe retirement/persistence design is
 * required before full stress readiness; a replayed response is also not
 * evidence that the restore physically executed.
 *
 * Concurrency: NOT thread-safe by design. The owner must serialize all
 * Begin/Complete calls on one thread; the SnapManagerActor integration posts
 * every access to its actor thread and never touches the ledger from a
 * callback thread.
 */
class RestoreReplayLedger {
public:
    enum class BeginStatus {
        NEW,       // entry opened; caller owns the ticket and must complete it
        IN_FLIGHT, // identical request already executing; absorb the retry
        REPLAY,    // identical request already completed; response holds stored bytes
        CONFLICT,  // same key replayed with a different serialized request
        REJECTED,  // malformed/oversized key component or request; nothing mutated
        FULL,      // slot/byte/accounting bound hit; new entry refused, nothing mutated
    };

    enum class CompleteStatus {
        STORED,             // first valid completion; exact bytes are now replayable
        ALREADY_COMPLETED,  // first completion wins; this one changed nothing
        STALE_TICKET,       // ticket does not match the entry; changed nothing
        UNKNOWN_ENTRY,      // no entry under this key; changed nothing
        RESPONSE_OVERSIZED, // refused without mutation; entry stays in flight
    };

    using Ticket = uint64_t;  // opaque per-entry token; 0 is never issued

    struct Limits {
        size_t maxEntries{4096};                      // entry slot bound
        size_t maxReservedBytes{64ULL * 1024 * 1024}; // total payload bound (64 MiB)
        size_t maxMessageBytes{64ULL * 1024};         // per key component / request / response bound (64 KiB)
    };

    struct Key {
        std::string callerName{};
        std::string callerUrl{};
        std::string requestID{};

        friend bool operator==(const Key &lhs, const Key &rhs)
        {
            return lhs.callerName == rhs.callerName && lhs.callerUrl == rhs.callerUrl &&
                   lhs.requestID == rhs.requestID;
        }
    };

    struct KeyHash {
        size_t operator()(const Key &key) const
        {
            size_t seed = std::hash<std::string>{}(key.callerName);
            seed ^= std::hash<std::string>{}(key.callerUrl) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
            seed ^= std::hash<std::string>{}(key.requestID) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
            return seed;
        }
    };

    struct BeginResult {
        BeginStatus status{BeginStatus::REJECTED};
        Ticket ticket{0};        // valid iff status == NEW
        std::string response{};  // exact stored bytes iff status == REPLAY
    };

    RestoreReplayLedger() = default;
    explicit RestoreReplayLedger(const Limits &limits) : limits_(limits) {}

    RestoreReplayLedger(const RestoreReplayLedger &) = delete;
    RestoreReplayLedger &operator=(const RestoreReplayLedger &) = delete;

    /**
     * Open or resolve a replayable entry for one caller's SnapStart request.
     * The ledger is mutated only when the returned status is NEW (the new
     * entry and its reservation); every other outcome, including REJECTED and
     * FULL, leaves the ledger untouched.
     * @param key Caller identity plus requestID; components nonempty and bounded
     * @param request Exact serialized request bytes to remember for conflict checks
     */
    BeginResult Begin(const Key &key, const std::string &request)
    {
        BeginResult result;
        if (!KeyIsValid(key) || request.size() > limits_.maxMessageBytes) {
            result.status = BeginStatus::REJECTED;
            return result;
        }

        auto it = entries_.find(key);
        if (it != entries_.end()) {
            if (it->second.request != request) {
                result.status = BeginStatus::CONFLICT;
                return result;
            }
            if (it->second.completed) {
                result.status = BeginStatus::REPLAY;
                result.response = it->second.response;
                return result;
            }
            result.status = BeginStatus::IN_FLIGHT;
            return result;
        }

        // Ticket 0 is never issued; if the 64-bit counter is exhausted the
        // ledger refuses new entries instead of minting a duplicate ticket.
        if (entries_.size() >= limits_.maxEntries || nextTicket_ == 0) {
            result.status = BeginStatus::FULL;
            return result;
        }

        // Steady per-entry reservation: key + request payload plus the maximum
        // response bytes, held from Begin for the entry's whole lifetime so a
        // completion can never require evicting anything. Invariant:
        // reservedBytes_ <= limits_.maxReservedBytes, so the remaining-
        // capacity comparison below cannot wrap.
        size_t cost = 0;
        if (!ReservedCost(key, request, cost) || cost > limits_.maxReservedBytes - reservedBytes_) {
            // Full (or unaccountable under the configured bounds): refuse the
            // new entry. Existing entries keep replaying above.
            result.status = BeginStatus::FULL;
            return result;
        }

        Entry entry;
        entry.ticket = nextTicket_++;
        entry.request = request;
        entry.reserved = cost;
        result.status = BeginStatus::NEW;
        result.ticket = entry.ticket;
        reservedBytes_ += cost;
        entries_.emplace(key, std::move(entry));
        return result;
    }

    /**
     * Store the terminal serialized response for an entry opened by Begin.
     * Only the first valid completion for the entry's ticket is stored;
     * ALREADY_COMPLETED / STALE_TICKET / UNKNOWN_ENTRY / RESPONSE_OVERSIZED
     * never mutate the ledger, never free the reservation, and never change
     * the stored response.
     * @param key Key the entry was opened with
     * @param ticket Ticket returned by the NEW begin for this entry
     * @param response Exact serialized terminal response bytes
     */
    CompleteStatus Complete(const Key &key, Ticket ticket, const std::string &response)
    {
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            return CompleteStatus::UNKNOWN_ENTRY;
        }
        if (it->second.ticket != ticket) {
            return CompleteStatus::STALE_TICKET;
        }
        if (it->second.completed) {
            return CompleteStatus::ALREADY_COMPLETED;
        }
        if (response.size() > limits_.maxMessageBytes) {
            // Refused without mutation: the entry stays in flight holding its
            // reservation, so retries of this request keep being absorbed
            // instead of dispatching a second restore.
            return CompleteStatus::RESPONSE_OVERSIZED;
        }
        it->second.response = response;
        it->second.completed = true;
        return CompleteStatus::STORED;
    }

    /**
     * Pure identity predicate used by the SnapManagerActor completion path: a
     * terminal response may only settle the ledger entry it was minted for.
     * Compares the responder AID name/url and the response requestID against
     * the entry key. This is bookkeeping identity only — it proves nothing
     * about the runtime's physical execution identity.
     */
    static bool MatchesResponder(const Key &key,
                                 const std::string &responderName,
                                 const std::string &responderUrl,
                                 const std::string &responseRequestID)
    {
        return key.callerName == responderName && key.callerUrl == responderUrl &&
               key.requestID == responseRequestID;
    }

    /** Number of tracked entries (in flight and completed). */
    size_t Size() const
    {
        return entries_.size();
    }

    /** Payload bytes currently reserved across all entries; never decreases. */
    size_t ReservedBytes() const
    {
        return reservedBytes_;
    }

private:
    struct Entry {
        Ticket ticket{0};
        std::string request{};   // exact serialized request; immutable once stored
        std::string response{};  // exact serialized response; set by first valid completion
        bool completed{false};
        size_t reserved{0};
    };

    bool KeyIsValid(const Key &key) const
    {
        return !key.callerName.empty() && key.callerName.size() <= limits_.maxMessageBytes &&
               !key.callerUrl.empty() && key.callerUrl.size() <= limits_.maxMessageBytes &&
               !key.requestID.empty() && key.requestID.size() <= limits_.maxMessageBytes;
    }

    // Overflow-checked reservation cost; false means the configured bounds are
    // so large the accounting itself would wrap.
    bool ReservedCost(const Key &key, const std::string &request, size_t &cost) const
    {
        const size_t max = std::numeric_limits<size_t>::max();
        size_t total = limits_.maxMessageBytes;
        for (const std::string *part : {&key.callerName, &key.callerUrl, &key.requestID, &request}) {
            if (part->size() > max - total) {
                return false;
            }
            total += part->size();
        }
        cost = total;
        return true;
    }

    Limits limits_{};
    std::unordered_map<Key, Entry, KeyHash> entries_{};
    Ticket nextTicket_{1};
    size_t reservedBytes_{0};
};

}  // namespace functionsystem::snap_manager

#endif  // FUNCTION_MASTER_SNAP_MANAGER_RESTORE_REPLAY_LEDGER_H
