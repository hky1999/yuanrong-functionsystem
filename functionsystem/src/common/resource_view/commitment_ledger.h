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

#ifndef FUNCTIONSYSTEM_COMMITMENT_LEDGER_H
#define FUNCTIONSYSTEM_COMMITMENT_LEDGER_H

#include <cstdint>
#include <map>
#include <mutex>
#include <string>

namespace functionsystem::resource_view {

// P2.0 commitment-aware admission: the node-level promise ledger.
//
// An upgrade-ladder raise lifts a sandbox's memory.max without touching its
// booked request (booked is the request contract; the raise is deliberate
// overcommit — D-3④). Between the raise and the sandbox actually growing
// into the new headroom, node admission would otherwise keep admitting on
// stale "free" memory that has in fact been promised away. This ledger
// tracks that promised-but-unrealized amount per node:
//
//   OnRaise(raised, use)   raised bytes enter the ledger, anchored at the
//                          current node actualuse;
//   OnUsage(use)           node-usage movement beyond the anchor window
//                          consumes (cashes in) the promise: growth above
//                          the highest use seen since the anchor is real
//                          actualuse now and must not stay double-counted;
//                          a drop below the anchor (park/evict/free) eats
//                          it too, since departed usage takes its headroom
//                          with it. Oscillation inside the window does not
//                          accumulate — decay tracks the extremes, never
//                          the per-sample deltas;
//   Outstanding(unit)      the remaining promise, added to `used` by
//                          UsageAwareFilter and to ladder self-admission.
//
// Node-level by construction: the ladder knows cgroup pool dirs, not
// instance ids (pool ids are sandboxd-internal), and the accounting
// meaning of a promise is the node total anyway. Master-process callers
// see a permanently empty ledger (single-node standalone has exactly one
// local scheduler; cluster-wide plumbing rides P2.1/P3.4 view schema).
class CommitmentLedger {
public:
    // set once by the scheduler driver before any traffic (control arm for
    // acceptance runs: false makes Outstanding() always 0)
    static void SetEnabled(bool enabled);
    static bool IsEnabled();

    // a raise landed: `raisedBytes` of new max headroom was promised.
    static void OnRaise(const std::string &unitId, uint64_t raisedBytes, uint64_t useBytes);

    // one node actualuse sample (same feed the ladder gets).
    static void OnUsage(const std::string &unitId, uint64_t useBytes);

    // remaining promise for the unit (0 when disabled / unknown unit).
    static uint64_t Outstanding(const std::string &unitId);

    // test hook: wipe all state.
    static void ResetForTest();

private:
    struct UnitState {
        uint64_t promisedBytes = 0; // total promised since the window opened
        uint64_t commitmentBytes = 0; // promisedBytes minus excursion decay
        uint64_t baseUseBytes = 0; // node use when the promise was anchored
        // excursion window: the highest / lowest use seen since the anchor.
        // Decay is computed from these extremes, not per-sample deltas, so
        // page-cache oscillation around a steady level cannot cash the
        // promise in once per direction (live 2026-08-24: a 2G promise went
        // 2048 -> 0 MB in 34s on a node whose use was flat at 5.6G).
        uint64_t highWaterBytes = 0;
        uint64_t lowWaterBytes = 0;
        // view-lag grace: the feed (view node actualuse) trails kernel
        // reality by up to ~10s, so the first post-raise samples can sit
        // far below the kernel-currency anchor without any memory having
        // departed. While nonzero, drop-excursions are ignored (growth
        // still applies — real catch-up upward is legitimate cash-in).
        uint64_t graceSamples = 0;
        bool anchored = false;
    };

    // idempotent: outstanding = promised - max(up/down excursion)
    static void ApplyExcursionDecay(UnitState &unit);

    static std::mutex &Mutex();
    static std::map<std::string, UnitState> &Units();
    static bool enabled_;
};

}  // namespace functionsystem::resource_view

#endif  // FUNCTIONSYSTEM_COMMITMENT_LEDGER_H
