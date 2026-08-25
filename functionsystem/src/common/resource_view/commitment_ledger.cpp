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

#include "commitment_ledger.h"

#include "common/logs/logging.h"

namespace functionsystem::resource_view {

namespace {

// samples after a raise during which drop-excursions are ignored (view-lag
// grace; pressure monitor samples every 5s, view trails kernel reality by
// up to ~10s live)
constexpr uint64_t DROP_GRACE_SAMPLES = 3;

}  // namespace

// idempotent decay from the excursion window: outstanding is always
// recomputed as promised - max(up-excursion, down-excursion), never
// subtracted incrementally per sample (level-based excursions would
// otherwise re-charge the same bytes on every sample the level persists —
// live 2026-08-24 v5: 128MB of growth was charged twelve times in a row)
void CommitmentLedger::ApplyExcursionDecay(UnitState &unit)
{
    uint64_t growth = 0;
    if (unit.highWaterBytes > unit.baseUseBytes) {
        growth = unit.highWaterBytes - unit.baseUseBytes;
    }
    uint64_t drop = 0;
    if (unit.baseUseBytes > unit.lowWaterBytes) {
        drop = unit.baseUseBytes - unit.lowWaterBytes;
    }
    const uint64_t eat = growth > drop ? growth : drop;
    unit.commitmentBytes = eat >= unit.promisedBytes ? 0 : unit.promisedBytes - eat;
    unit.anchored = unit.commitmentBytes != 0;
}

bool CommitmentLedger::enabled_ = true;

std::mutex &CommitmentLedger::Mutex()
{
    static std::mutex m;
    return m;
}

std::map<std::string, CommitmentLedger::UnitState> &CommitmentLedger::Units()
{
    static std::map<std::string, UnitState> units;
    return units;
}

void CommitmentLedger::SetEnabled(bool enabled)
{
    std::lock_guard<std::mutex> lock(Mutex());
    enabled_ = enabled;
}

bool CommitmentLedger::IsEnabled()
{
    std::lock_guard<std::mutex> lock(Mutex());
    return enabled_;
}

void CommitmentLedger::OnRaise(const std::string &unitId, uint64_t raisedBytes, uint64_t useBytes)
{
    if (raisedBytes == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(Mutex());
    if (!enabled_) {
        return;
    }
    auto &unit = Units()[unitId];
    if (!unit.anchored || unit.commitmentBytes == 0) {
        // anchor a fresh promise window at the current use
        unit.baseUseBytes = useBytes;
        unit.highWaterBytes = useBytes;
        unit.lowWaterBytes = useBytes;
        unit.anchored = true;
    } else if (useBytes > unit.highWaterBytes) {
        // incremental raise on a live window: the window has already seen
        // this much reality; the next sample will cash it in
        unit.highWaterBytes = useBytes;
    }
    // re-arm the view-lag grace either way: the anchor moved to (or past)
    // kernel-now, and the feed needs a few samples to catch up
    unit.graceSamples = DROP_GRACE_SAMPLES;
    unit.promisedBytes += raisedBytes;
    ApplyExcursionDecay(unit);
    YRLOG_INFO("commitment ledger unit {}: raise booked {} bytes at use {} -> outstanding {} bytes",
               unitId, raisedBytes, useBytes, unit.commitmentBytes);
}

void CommitmentLedger::OnUsage(const std::string &unitId, uint64_t useBytes)
{
    std::lock_guard<std::mutex> lock(Mutex());
    auto it = Units().find(unitId);
    if (it == Units().end() || it->second.commitmentBytes == 0 || !it->second.anchored) {
        return;
    }
    auto &unit = it->second;
    if (useBytes > unit.highWaterBytes) {
        unit.highWaterBytes = useBytes;
    }
    if (unit.graceSamples > 0) {
        --unit.graceSamples;
    } else if (useBytes < unit.lowWaterBytes) {
        unit.lowWaterBytes = useBytes;
    }
    // excursion decay: only net movement beyond the anchor window cashes the
    // promise in, and each excursion level is charged exactly once
    // (outstanding is recomputed from the extremes, never decremented per
    // sample — live 2026-08-24 v5: 128MB of growth was re-charged on twelve
    // consecutive samples of a perfectly flat feed). During the view-lag
    // grace the low water stays at the anchor: a feed that trails kernel
    // reality reads below the anchor right after the raise without any
    // memory having departed (live 2026-08-24 v3: booked 2048MB at kernel
    // 6076MB, first view sample 4196MB, promise zeroed in two samples).
    const uint64_t before = unit.commitmentBytes;
    ApplyExcursionDecay(unit);
    if (unit.commitmentBytes != before) {
        YRLOG_INFO("commitment ledger unit {}: sample {} (anchor {} high {} low {}) cashed {} -> outstanding {} bytes",
                   unitId, useBytes, unit.baseUseBytes, unit.highWaterBytes, unit.lowWaterBytes,
                   before - unit.commitmentBytes, unit.commitmentBytes);
    }
}

uint64_t CommitmentLedger::Outstanding(const std::string &unitId)
{
    std::lock_guard<std::mutex> lock(Mutex());
    if (!enabled_) {
        return 0;
    }
    auto it = Units().find(unitId);
    if (it == Units().end()) {
        return 0;
    }
    return it->second.commitmentBytes;
}

void CommitmentLedger::ResetForTest()
{
    std::lock_guard<std::mutex> lock(Mutex());
    Units().clear();
    enabled_ = true;
}

}  // namespace functionsystem::resource_view
