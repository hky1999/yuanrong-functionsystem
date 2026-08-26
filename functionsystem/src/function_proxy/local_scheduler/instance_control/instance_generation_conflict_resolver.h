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

#ifndef LOCAL_SCHEDULER_INSTANCE_GENERATION_CONFLICT_RESOLVER_H
#define LOCAL_SCHEDULER_INSTANCE_GENERATION_CONFLICT_RESOLVER_H

#include <memory>

#include <async/future.hpp>

#include "common/state_machine/instance_state_machine.h"
#include "common/status/status.h"

namespace functionsystem::local_scheduler {

// Actor-confined context for resolving competing generations of the same
// instance ID.  This class deliberately does not own an Actor or invoke
// external services.  InstanceCtrlActor is the sole owner of mutations and
// re-enters its mailbox before updating this context after an async operation.
class InstanceGenerationConflictResolver {
public:
    struct CleanupState {
        bool localCleanupPrepared = false;
        bool resourceCleanupDone = false;
        bool runtimeCleanupDone = false;
        std::shared_ptr<litebus::Future<Status>> attempt;
    };
    using CleanupStatePtr = std::shared_ptr<CleanupState>;

    InstanceGenerationConflictResolver(const InstanceInfo &contenderSnapshot, bool arbitrationEnabled);
    ~InstanceGenerationConflictResolver() = default;

    // The resolver is generic; this policy gate is currently the only caller
    // that has the complete cleanup and winner-propagation contract.
    static bool IsRouteConflictResolutionCandidate(const InstanceInfo &contenderSnapshot);

    const InstanceInfo &Contender() const;
    bool IsArbitrationEnabled() const;

    bool HasResolvedWinner() const;
    const InstanceInfo &ResolvedWinner() const;
    void RecordResolvedWinner(const InstanceInfo &winnerInfo);

    CleanupStatePtr GetCleanupState() const;

    bool IsExactPersistenceFailure(const TransitionResult &result) const;
    bool IsReusableWinner(const InstanceInfo &winnerInfo, const std::string &localProxyID) const;
    bool MatchesContenderGenerationView(const InstanceInfo &currentInfo) const;

    static bool IsSameGeneration(const InstanceInfo &expectedInfo, const InstanceInfo &currentInfo);

private:
    const InstanceInfo contenderSnapshot_;
    const bool arbitrationEnabled_;
    InstanceInfo resolvedWinner_;
    std::shared_ptr<CleanupState> cleanupState_;
};

}  // namespace functionsystem::local_scheduler

#endif  // LOCAL_SCHEDULER_INSTANCE_GENERATION_CONFLICT_RESOLVER_H
