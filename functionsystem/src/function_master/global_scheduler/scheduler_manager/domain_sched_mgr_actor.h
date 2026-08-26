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

#ifndef FUNCTION_MASTER_GLOBAL_SCHEDULER_DOMAIN_SCHED_MGR_ACTOR_H
#define FUNCTION_MASTER_GLOBAL_SCHEDULER_DOMAIN_SCHED_MGR_ACTOR_H

#include <actor/actor.hpp>
#include <actor/aid.hpp>
#include <async/async.hpp>
#include <async/future_base.hpp>
#include <functional>
#include <vector>

#include "common/explorer/explorer.h"
#include "common/heartbeat/heartbeat_observer.h"
#include "common/leader/business_policy.h"
#include "common/utils/random_number.h"
#include "common/proto/pb/message_pb.h"
#include "common/status/status.h"
#include "common/utils/request_sync_helper.h"

namespace functionsystem::global_scheduler {

using CallbackAddFunc =
    std::function<void(const litebus::AID &from, const std::string &name, const std::string &address)>;
using CallbackDelFunc = std::function<void(const std::string &name, const std::string &ip)>;
using CallbackWorkerFunc = std::function<void(const std::string &ip, const std::string &key, const bool healthy)>;

class DomainSchedMgrActor : public litebus::ActorBase, public std::enable_shared_from_this<DomainSchedMgrActor> {
public:
    explicit DomainSchedMgrActor(const std::string &name, const uint32_t heartbeatTimeoutMs);

    ~DomainSchedMgrActor() override = default;

    void Register(const litebus::AID &from, std::string &&name, std::string &&msg);

    void Registered(const litebus::AID &dst, const litebus::Option<messages::ScheduleTopology> &topology);

    void NotifySchedAbnormal(const litebus::AID &from, std::string &&name, std::string &&msg);

    void NotifyWorkerStatus(const litebus::AID &from, std::string &&name, std::string &&msg);

    void UpdateSchedTopoView(const std::string &name, const std::string &address,
                             const messages::ScheduleTopology &topology);

    Status AddDomainSchedCallback(const CallbackAddFunc &func);

    Status DelDomainSchedCallback(const CallbackDelFunc &func);

    Status DelLocalSchedCallback(const CallbackDelFunc &func);

    Status NotifyWorkerStatusCallback(const CallbackWorkerFunc &func);

    Status Connect(const std::string &name, const std::string &address);

    void Disconnect();

    litebus::Future<Status> Schedule(const std::string &name, const std::string &address,
                                     const std::shared_ptr<messages::ScheduleRequest> &req, const uint32_t retryCycle);

    litebus::Future<messages::GroupResponse> GroupSchedule(const std::string &name, const std::string &address,
                                                           const std::shared_ptr<messages::GroupInfo> &req);

    void ResponseSchedule(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<messages::QueryAgentInfoResponse> QueryAgentInfo(const std::string &name,
        const std::string &address, const std::shared_ptr<messages::QueryAgentInfoRequest> &req);

    litebus::Future<messages::QuerySchedulingQueueResponse> GetSchedulingQueue(
        const std::string &name, const std::string &address,
        const std::shared_ptr<messages::QuerySchedulingQueueRequest> &req);

    void ResponseGetSchedulingQueue(const litebus::AID &from, std::string &&name, std::string &&msg);

    void ResponseQueryAgentInfo(const litebus::AID &from, std::string &&name, std::string &&msg);

    litebus::Future<messages::QueryResourcesInfoResponse> QueryResourcesInfo(const std::string &name,
        const std::string &address, const std::shared_ptr<messages::QueryResourcesInfoRequest> &req);

    void ResponseQueryResourcesInfo(const litebus::AID &from, std::string &&name, std::string &&msg);

    void UpdateLeaderInfo(const explorer::LeaderInfo &leaderInfo);

    void OnForwardGroupSchedule(const litebus::AID &from, std::string &&name, std::string &&msg);
protected:
    void Init() override;

private:
    struct Member {
        std::unordered_map<std::string, litebus::Timer> scheduleTimers;
        std::unordered_map<std::string, std::shared_ptr<litebus::Promise<Status>>> schedulePromises;
    };
    class Business : public leader::BusinessPolicy {
    public:
        Business(const std::shared_ptr<DomainSchedMgrActor> &actor, const std::shared_ptr<Member> &member)
            : actor_(actor), member_{ member }
        {
        }
        ~Business() override = default;
        virtual void Register(const litebus::AID &from, std::string &&name, std::string &&msg) = 0;
        virtual void NotifySchedAbnormal(const litebus::AID &from, std::string &&name, std::string &&msg) = 0;
        virtual void NotifyWorkerStatus(const litebus::AID &from, std::string &&name, std::string &&msg) = 0;
        virtual void ResponseSchedule(const litebus::AID &from, std::string &&name, std::string &&msg) = 0;
        void OnChange() override{};

    protected:
        std::weak_ptr<DomainSchedMgrActor> actor_;
        std::shared_ptr<Member> member_;
    };

    class MasterBusiness : public Business {
    public:
        MasterBusiness(const std::shared_ptr<DomainSchedMgrActor> &actor, const std::shared_ptr<Member> &member)
            : Business(actor, member)
        {
        }
        ~MasterBusiness() override = default;

        void Register(const litebus::AID &from, std::string &&name, std::string &&msg) override;
        void NotifySchedAbnormal(const litebus::AID &from, std::string &&name, std::string &&msg) override;
        void NotifyWorkerStatus(const litebus::AID &from, std::string &&name, std::string &&msg) override;
        void ResponseSchedule(const litebus::AID &from, std::string &&name, std::string &&msg) override;
    };

    class SlaveBusiness : public Business {
    public:
        SlaveBusiness(const std::shared_ptr<DomainSchedMgrActor> &actor, const std::shared_ptr<Member> &member)
            : Business(actor, member)
        {
        }
        ~SlaveBusiness() override = default;
        void Register(const litebus::AID &, std::string &&, std::string &&) override;
        void NotifySchedAbnormal(const litebus::AID &, std::string &&, std::string &&) override;
        void NotifyWorkerStatus(const litebus::AID &, std::string &&, std::string &&) override;
        void ResponseSchedule(const litebus::AID &, std::string &&, std::string &&) override;
    };

private:
    void SendRegisteredMessage(const litebus::AID &dst, const messages::Registered &msg);

private:
    template <typename T>
    Status SetCallback(std::function<void()> &&func, const T &callback) const;

    void SendScheduleRequest(const std::string &name, const std::string &address,
                             const std::shared_ptr<messages::ScheduleRequest> &req, const uint32_t retryCycle,
                             const std::shared_ptr<litebus::Promise<Status>> &promise);
    std::vector<CallbackAddFunc> addDomainSchedCallbacks_;
    CallbackDelFunc delDomainSchedCallback_;
    CallbackDelFunc delLocalSchedCallback_;
    CallbackWorkerFunc notifyWorkerStatusCallback_;

    std::shared_ptr<Member> member_;

    std::unordered_map<std::string, std::shared_ptr<Business>> businesses_;

    std::string curStatus_;
    std::shared_ptr<Business> business_{ nullptr };

    std::shared_ptr<litebus::Promise<messages::QueryAgentInfoResponse>> queryAgentPromise_;

    std::shared_ptr<litebus::Promise<messages::QueryResourcesInfoResponse>> queryResourcePromise_;

    std::shared_ptr<litebus::Promise<messages::QuerySchedulingQueueResponse>> getSchedulingQueuePromise_;

    BACK_OFF_RETRY_HELPER(DomainSchedMgrActor, messages::QueryResourcesInfoResponse, queryResourceHelper_);
    BACK_OFF_RETRY_HELPER(DomainSchedMgrActor, messages::QuerySchedulingQueueResponse,
                          getSchedulingQueueHelper_);
    std::shared_ptr<litebus::AID> domainSchedulerAID_;
    std::shared_ptr<HeartbeatObserveDriver> heartbeatObserveDriver_ = nullptr;

    const uint32_t heartbeatTimeoutMs_ = 1000;

    const uint32_t groupTimeout_ = 10000;
    REQUEST_SYNC_HELPER(DomainSchedMgrActor, messages::GroupResponse, groupTimeout_, requestGroupScheduleMatch_);
};

}  // namespace functionsystem::global_scheduler

#endif  // FUNCTION_MASTER_GLOBAL_SCHEDULER_DOMAIN_SCHED_MGR_ACTOR_H
