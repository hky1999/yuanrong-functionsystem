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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "async/async.hpp"
#include "common/resource_view/view_utils.h"
#include "common/schedule_decision/performer/group_schedule_performer.h"
#include "common/schedule_decision/performer/instance_schedule_performer.h"
#include "mocks/mock_preemption_controller.h"
#include "mocks/mock_resource_view.h"
#include "mocks/mock_schedule_framework.h"
#include "common/schedule_decision/performer/aggregated_schedule_performer.h"

namespace functionsystem::test {
using namespace ::testing;

class SchedulerPerformerTest : public Test {
public:
    void SetUp() override
    {
        Init(schedule_decision::AllocateType::PRE_ALLOCATION);
    }

    void Init(schedule_decision::AllocateType type)
    {
        instanceSchedulerPerformer_ = std::make_shared<schedule_decision::InstanceSchedulePerformer>(type);
        groupSchedulerPerformer_ = std::make_shared<schedule_decision::GroupSchedulePerformer>(type);
        aggregateSchedulerPerformer_ = std::make_shared<schedule_decision::AggregatedSchedulePerformer>(type);
        resourceView_ = resource_view::ResourceView::CreateResourceView(
            "domain", resource_view::ResourceViewActor::Param{ false, false, 0 });
        mockFrameWork_ = std::make_shared<MockSchedulerFramework>();
        instanceSchedulerPerformer_->RegisterScheduleFramework(mockFrameWork_);
        instanceSchedulerPerformer_->BindResourceView(resourceView_);
        aggregateSchedulerPerformer_->RegisterScheduleFramework(mockFrameWork_);
        aggregateSchedulerPerformer_->BindResourceView(resourceView_);
        groupSchedulerPerformer_->RegisterScheduleFramework(mockFrameWork_);
        groupSchedulerPerformer_->BindResourceView(resourceView_);
    }

    void TearDown() override
    {
        instanceSchedulerPerformer_ = nullptr;
        groupSchedulerPerformer_ = nullptr;
        aggregateSchedulerPerformer_ = nullptr;
        resourceView_ = nullptr;
        mockFrameWork_ = nullptr;
    }

    std::shared_ptr<schedule_framework::PreAllocatedContext> GetPreAllocatedContext(const resource_view::ResourceViewInfo &resourceInfo)
    {
        auto preContext = std::make_shared<schedule_framework::PreAllocatedContext>();
        preContext->allLocalLabels = resourceInfo.allLocalLabels;
        return preContext;
    }

    std::shared_ptr<schedule_decision::InstanceItem> GetInstanceItem(int32_t priority,
                                                                     double cpu, double memory)
    {
        auto req = std::make_shared<messages::ScheduleRequest>();
        auto scheduleInstance = view_utils::GetInstanceWithResourceAndPriority(priority, cpu, memory);
        *req->mutable_instance() = scheduleInstance;
        req->set_requestid(scheduleInstance.requestid());
        req->set_traceid("traceID_" + litebus::uuid_generator::UUID::GetRandomUUID().ToString());
        auto promise = std::make_shared<litebus::Promise<schedule_decision::ScheduleResult>>();
        litebus::Promise<std::string> cancel;
        return std::make_shared<schedule_decision::InstanceItem>(req, promise, cancel.GetFuture());
    }


protected:
    std::shared_ptr<schedule_decision::InstanceSchedulePerformer> instanceSchedulerPerformer_;
    std::shared_ptr<schedule_decision::GroupSchedulePerformer> groupSchedulerPerformer_;
    std::shared_ptr<schedule_decision::AggregatedSchedulePerformer> aggregateSchedulerPerformer_;
    std::shared_ptr<resource_view::ResourceView> resourceView_;
    std::shared_ptr<MockSchedulerFramework> mockFrameWork_;
};


// test schedule instance without preemption
TEST_F(SchedulerPerformerTest, ScheduleInstanceWithoutPreemption)
{
    auto agentUnit = view_utils::Get1DResourceUnit("agent001");
    resourceView_->AddResourceUnit(agentUnit);
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    std::priority_queue<NodeScore> candidates;
    auto feasible = NodeScore(agentUnit.id(), 10);
    feasible.availableForRequest = 1;
    candidates.emplace(feasible);
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, 1)).WillOnce(Return(ScheduleResults{0, "", candidates}));
    int32_t cnt = 0;
    auto preemptCallbackFunc =
        [&cnt](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        cnt += preemptResults.size();
        return Status::OK();
    };
    instanceSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    auto ctx = GetPreAllocatedContext(info);
    auto scheduleItem = GetInstanceItem(0, view_utils::INST_SCALA_VALUE, view_utils::INST_SCALA_VALUE);
    auto *restoring = scheduleItem->scheduleReq->mutable_instance();
    restoring->mutable_snapshotinfo()->set_checkpointid("checkpoint-restore");
    restoring->mutable_resources()->mutable_resources()->at(resource_view::MEMORY_RESOURCE_NAME)
        .mutable_scalar()->set_limit(8192);
    auto result = instanceSchedulerPerformer_->DoSchedule(ctx, info, scheduleItem);
    EXPECT_EQ(result.code, 0);
    EXPECT_EQ(cnt, 0);
    ASSERT_EQ(ctx->preAllocatedRestoreMemory.count(restoring->instanceid()), size_t{1});
    EXPECT_DOUBLE_EQ(ctx->preAllocatedRestoreMemory.at(restoring->instanceid()).scalar().limit(), 8192);
    EXPECT_TRUE(ctx->preAllocatedSelectedFunctionAgentSet.find(agentUnit.id()) != ctx->preAllocatedSelectedFunctionAgentSet.end());
    result.id = agentUnit.id();
    result.unitID = agentUnit.id();
    instanceSchedulerPerformer_->RollBack(ctx, scheduleItem, result);
    EXPECT_EQ(ctx->preAllocatedRestoreMemory.count(restoring->instanceid()), size_t{0});
    EXPECT_EQ(ctx->preAllocatedSelectedFunctionAgentMap.count(restoring->instanceid()), size_t{0});
    EXPECT_TRUE(ctx->preAllocatedSelectedFunctionAgentSet.find(agentUnit.id()) == ctx->preAllocatedSelectedFunctionAgentSet.end());
}

// test schedule instance without preemption
TEST_F(SchedulerPerformerTest, AggregateScheduleInstanceWithoutPreemption)
{
    auto agentUnit = view_utils::Get1DResourceUnit("agent001");
    resourceView_->AddResourceUnit(agentUnit);
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    std::priority_queue<NodeScore> candidates;
    auto feasible = NodeScore(agentUnit.id(), 10);
    feasible.availableForRequest = 1;
    candidates.emplace(feasible);
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, 1)).WillOnce(Return(ScheduleResults{0, "", candidates}));
    int32_t cnt = 0;

    auto ctx = GetPreAllocatedContext(info);
    auto scheduleItem = GetInstanceItem(0, view_utils::INST_SCALA_VALUE, view_utils::INST_SCALA_VALUE);
    auto runningQueue_ = std::make_shared<schedule_decision::AggregatedQueue>(10, "relaxed");
    runningQueue_->Enqueue(scheduleItem);
    auto aggregateItem = std::dynamic_pointer_cast<schedule_decision::AggregatedItem>(runningQueue_->Front());
    auto result = aggregateSchedulerPerformer_->DoSchedule(ctx, info, aggregateItem);
    EXPECT_EQ(cnt, 0);
    EXPECT_TRUE(ctx->preAllocatedSelectedFunctionAgentSet.find(agentUnit.id()) != ctx->preAllocatedSelectedFunctionAgentSet.end());
}


// test schedule instance with preemption failed
TEST_F(SchedulerPerformerTest, ScheduleInstanceWithPreemptionFailed)
{
    auto agentUnit = view_utils::Get1DResourceUnit("agent001");
    resourceView_->AddResourceUnit(agentUnit);
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, 1))
        .WillOnce(Return(ScheduleResults{ (int32_t)StatusCode::RESOURCE_NOT_ENOUGH, "", {} }));
    int32_t cnt = 0;
    auto preemptCallbackFunc =
        [&cnt](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        for(auto res:preemptResults) {
            cnt += res.preemptedInstances.size();
        }
        return Status::OK();
    };
    instanceSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    auto scheduleItem = GetInstanceItem(5, 2000.1, 2000.1);
    auto result = instanceSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_EQ(cnt, 0);
}

// test schedule instance with preemption success
TEST_F(SchedulerPerformerTest, ScheduleInstanceWithPreemptionSuccess)
{
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, 1))
        .WillOnce(Return(ScheduleResults{ (int32_t)StatusCode::RESOURCE_NOT_ENOUGH, "", {} }));
    int32_t cnt = 0;
    auto preemptCallbackFunc =
        [&cnt](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        for(auto res:preemptResults) {
            cnt += res.preemptedInstances.size();
        }
        return Status::OK();
    };
    instanceSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    auto pod1 = view_utils::Get1DResourceUnit("unit1");
    resourceView_->AddResourceUnit(pod1);
    auto instance1 = view_utils::GetInstanceWithResourceAndPriority(1, 50.0, 50.0);
    instance1.set_unitid("unit1");
    auto instance2 = view_utils::GetInstanceWithResourceAndPriority(1, 50.0, 50.0);
    instance2.set_unitid("unit1");
    resourceView_->AddInstances(
        { { instance1.instanceid(), { instance1, nullptr } }, { instance2.instanceid(), { instance2, nullptr } } });
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    auto scheduleItem = GetInstanceItem(5, 60.0, 60.0);
    auto result = instanceSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_EQ(cnt, 2);
}

TEST_F(SchedulerPerformerTest, ScheduleGroupWithoutPreemption)
{
    auto pod1 = view_utils::Get1DResourceUnit("unit1");
    resourceView_->AddResourceUnit(pod1);
    auto pod2 = view_utils::Get1DResourceUnit("unit2");
    resourceView_->AddResourceUnit(pod2);
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    std::priority_queue<NodeScore> candidates1;
    auto feasible1 = NodeScore(pod1.id(), 10);
    feasible1.availableForRequest = 1;
    candidates1.emplace(feasible1);
    std::priority_queue<NodeScore> candidates2;
    auto feasible2 = NodeScore(pod2.id(), 10);
    feasible2.availableForRequest = 1;
    candidates2.emplace(feasible2);
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, _))
        .WillOnce(Return(ScheduleResults{ 0, "", candidates1 }))
        .WillOnce(Return(ScheduleResults{ 0, "", candidates2 }));
    auto insItem1 = GetInstanceItem(0, view_utils::INST_SCALA_VALUE, view_utils::INST_SCALA_VALUE);
    auto insItem2 = GetInstanceItem(0, view_utils::INST_SCALA_VALUE, view_utils::INST_SCALA_VALUE);
    std::vector<std::shared_ptr<schedule_decision::InstanceItem>> insItems = {insItem1, insItem2};
    int32_t cnt = 0;
    auto preemptCallbackFunc =
        [&cnt](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        cnt += preemptResults.size();
        return Status::OK();
    };
    groupSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    auto promise = std::make_shared<litebus::Promise<schedule_decision::GroupScheduleResult>>();
    litebus::Promise<std::string> cancel;
    auto ctx = GetPreAllocatedContext(info);
    auto scheduleItem = std::make_shared<schedule_decision::GroupItem>(
        insItems, promise, "group001", cancel.GetFuture(), schedule_decision::GroupSpec::RangeOpt());
    auto result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, 0);
    EXPECT_EQ(cnt, 0);
    groupSchedulerPerformer_->RollBack(ctx, scheduleItem, result);
}

TEST_F(SchedulerPerformerTest, ScheduleGroupWithPreemptionSuccess)
{
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, _))
        .WillRepeatedly(Return(ScheduleResults{ (int32_t)StatusCode::RESOURCE_NOT_ENOUGH, "", {} }));;
    int32_t cnt = 0;
    auto preemptCallbackFunc =
        [&cnt](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        for(auto res:preemptResults) {
            cnt += res.preemptedInstances.size();
        }
        return Status::OK();
    };
    groupSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    auto pod1 = view_utils::Get1DResourceUnit("unit1");
    resourceView_->AddResourceUnit(pod1);
    auto instance1 = view_utils::GetInstanceWithResourceAndPriority(1, 45.0, 45.0);
    instance1.set_unitid("unit1");
    auto instance2 = view_utils::GetInstanceWithResourceAndPriority(1, 45.0, 45.0);
    instance2.set_unitid("unit1");
    resourceView_->AddInstances(
        { { instance1.instanceid(), { instance1, nullptr } }, { instance2.instanceid(), { instance2, nullptr } } });
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    auto insItem1 = GetInstanceItem(5, 40.0, 40.0);
    auto insItem2 = GetInstanceItem(5, 40.0, 40.0);
    std::vector<std::shared_ptr<schedule_decision::InstanceItem>> insItems = {insItem1, insItem2};
    auto promise = std::make_shared<litebus::Promise<schedule_decision::GroupScheduleResult>>();
    litebus::Promise<std::string> cancel;
    auto ctx = GetPreAllocatedContext(info);
    auto scheduleItem = std::make_shared<schedule_decision::GroupItem>(
        insItems, promise, "group001", cancel.GetFuture(), schedule_decision::GroupSpec::RangeOpt());
    auto result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_NE(result.code, 0);
    EXPECT_EQ(cnt, 2);
}

TEST_F(SchedulerPerformerTest, ScheduleGroupWithPreemptionFailed)
{
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, _))
        .WillRepeatedly(Return(ScheduleResults{ (int32_t)StatusCode::RESOURCE_NOT_ENOUGH, "", {} }));
    int32_t cnt = 0;
    auto preemptCallbackFunc =
        [&cnt](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        for(auto res:preemptResults) {
            cnt += res.preemptedInstances.size();
        }
        return Status::OK();
    };
    groupSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    auto pod1 = view_utils::Get1DResourceUnit("unit1");
    resourceView_->AddResourceUnit(pod1);
    auto instance1 = view_utils::GetInstanceWithResourceAndPriority(1, 45.0, 45.0);
    instance1.set_unitid("unit1");
    auto instance2 = view_utils::GetInstanceWithResourceAndPriority(1, 45.0, 45.0);
    instance2.set_unitid("unit1");
    resourceView_->AddInstances(
        { { instance1.instanceid(), { instance1, nullptr } }, { instance2.instanceid(), { instance2, nullptr } } });
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    auto insItem1 = GetInstanceItem(5, 40.0, 40.0);
    auto insItem2 = GetInstanceItem(5, 70.0, 70.0);
    std::vector<std::shared_ptr<schedule_decision::InstanceItem>> insItems = {insItem1, insItem2};
    auto promise = std::make_shared<litebus::Promise<schedule_decision::GroupScheduleResult>>();
    litebus::Promise<std::string> cancel;
    auto ctx = GetPreAllocatedContext(info);
    auto scheduleItem = std::make_shared<schedule_decision::GroupItem>(
        insItems, promise, "group001", cancel.GetFuture(), schedule_decision::GroupSpec::RangeOpt());
    auto result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_NE(result.code, 0);
    EXPECT_EQ(cnt, 0);
}

// case 1:
// range min 5 max 10 step 2
// schedule success 4, index 5 failed && trigger to preempted
// expected err
TEST_F(SchedulerPerformerTest, ScheduleRangeGroupLessMinToPreempted)
{
    auto preemptionController = std::make_shared<MockPreemptionController>();
    auto preemptCallbackFunc =
        [](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        return Status::OK();
    };
    groupSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    groupSchedulerPerformer_->preemptController_ = preemptionController;

    std::priority_queue<NodeScore> candidates;
    auto feasible1 = NodeScore("agent", 10);
    feasible1.availableForRequest = 4;
    candidates.emplace(feasible1);
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, _)).WillOnce(Return(ScheduleResults{0, "", candidates}));

    auto preemtpResult = schedule_decision::PreemptResult();
    preemtpResult.status = Status(StatusCode::DOMAIN_SCHEDULER_NO_PREEMPTABLE_INSTANCE);
    EXPECT_CALL(*preemptionController, PreemptDecision).WillOnce(Return(preemtpResult));

    litebus::Promise<std::string> cancel;
    std::vector<std::shared_ptr<schedule_decision::InstanceItem>> insItems;
    for (int i = 0; i < 10; i++) {
        insItems.emplace_back(GetInstanceItem(5, 1.0, 1.0));
    }
    auto agentUnit = view_utils::Get1DResourceUnit("agent");
    resourceView_->AddResourceUnit(agentUnit);
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    auto range = schedule_decision::GroupSpec::RangeOpt{ .isRange = true, .min = 5, .max = 10, .step = 2 };
    auto promise = std::make_shared<litebus::Promise<schedule_decision::GroupScheduleResult>>();
    auto scheduleItem =
        std::make_shared<schedule_decision::GroupItem>(insItems, promise, "group001", cancel.GetFuture(), range);
    auto result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_EQ(result.results.size(), size_t(5));
}

// case 2:
// range min 5 max 10 step 2
// schedule success 6, index 7 failed && no preempted
// expected  6 success
TEST_F(SchedulerPerformerTest, ScheduleRangeGroupLargerThanMinNoPreempted)
{
    auto preemptionController = std::make_shared<MockPreemptionController>();
    auto preemptCallbackFunc =
        [](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        return Status::OK();
    };
    groupSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    groupSchedulerPerformer_->preemptController_ = preemptionController;

    std::priority_queue<NodeScore> candidates;
    auto feasible1 = NodeScore("agent", 10);
    feasible1.availableForRequest = 6;
    candidates.emplace(feasible1);
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, _)).WillOnce(Return(ScheduleResults{0, "", candidates}));
    litebus::Promise<std::string> cancel;
    std::vector<std::shared_ptr<schedule_decision::InstanceItem>> insItems;
    for (int i = 0; i < 10; i++) {
        insItems.emplace_back(GetInstanceItem(5, 1.0, 1.0));
    }
    auto agentUnit = view_utils::Get1DResourceUnit("agent");
    resourceView_->AddResourceUnit(agentUnit);
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    auto range = schedule_decision::GroupSpec::RangeOpt{ .isRange = true, .min = 5, .max = 10, .step = 2 };
    auto promise = std::make_shared<litebus::Promise<schedule_decision::GroupScheduleResult>>();
    auto scheduleItem =
        std::make_shared<schedule_decision::GroupItem>(insItems, promise, "group001", cancel.GetFuture(), range);
    auto result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, StatusCode::SUCCESS);
    EXPECT_EQ(result.results.size(), size_t(6));
}

// case 3:
// range min 5 max 10 step 2
// schedule success 7, index 8 failed && no preempted
// expected  6 success
TEST_F(SchedulerPerformerTest, ScheduleRangeGroupLargerThanMinNoPreemptedByStep)
{
    auto preemptionController = std::make_shared<MockPreemptionController>();
    auto preemptCallbackFunc =
        [](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        return Status::OK();
    };
    groupSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    groupSchedulerPerformer_->preemptController_ = preemptionController;

    std::priority_queue<NodeScore> candidates;
    auto feasible1 = NodeScore("agent", 10);
    feasible1.availableForRequest = 7;
    candidates.emplace(feasible1);
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, _)).WillOnce(Return(ScheduleResults{0, "", candidates}));

    litebus::Promise<std::string> cancel;
    std::vector<std::shared_ptr<schedule_decision::InstanceItem>> insItems;
    for (int i = 0; i < 10; i++) {
        insItems.emplace_back(GetInstanceItem(5, 1.0, 1.0));
    }
    auto agentUnit = view_utils::Get1DResourceUnit("agent");
    resourceView_->AddResourceUnit(agentUnit);
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    auto range = schedule_decision::GroupSpec::RangeOpt{ .isRange = true, .min = 5, .max = 10, .step = 2 };
    auto promise = std::make_shared<litebus::Promise<schedule_decision::GroupScheduleResult>>();
    auto scheduleItem =
        std::make_shared<schedule_decision::GroupItem>(insItems, promise, "group001", cancel.GetFuture(), range);
    auto result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, StatusCode::SUCCESS);
    EXPECT_EQ(result.results.size(), size_t(6));
}

// case 4:
// range min 5 max 10 step 3
// schedule success 6, index 7 failed && no preempted
// expected  5 success
TEST_F(SchedulerPerformerTest, ScheduleRangeGroupLargerThanMinNoPreemptedByMin)
{
    auto preemptionController = std::make_shared<MockPreemptionController>();
    auto preemptCallbackFunc =
        [](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        return Status::OK();
    };
    groupSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    groupSchedulerPerformer_->preemptController_ = preemptionController;

    std::priority_queue<NodeScore> candidates;
    auto feasible1 = NodeScore("agent", 10);
    feasible1.availableForRequest = 6;
    candidates.emplace(feasible1);
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, _)).WillOnce(Return(ScheduleResults{0, "", candidates}));

    litebus::Promise<std::string> cancel;
    std::vector<std::shared_ptr<schedule_decision::InstanceItem>> insItems;
    for (int i = 0; i < 10; i++) {
        insItems.emplace_back(GetInstanceItem(5, 1.0, 1.0));
    }
    auto agentUnit = view_utils::Get1DResourceUnit("agent");
    resourceView_->AddResourceUnit(agentUnit);
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    auto range = schedule_decision::GroupSpec::RangeOpt{ .isRange = true, .min = 5, .max = 10, .step = 3 };
    auto promise = std::make_shared<litebus::Promise<schedule_decision::GroupScheduleResult>>();
    auto scheduleItem =
        std::make_shared<schedule_decision::GroupItem>(insItems, promise, "group001", cancel.GetFuture(), range);
    auto result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, StatusCode::SUCCESS);
    EXPECT_EQ(result.results.size(), size_t(5));
}

TEST_F(SchedulerPerformerTest, ScheduleRangeGroupLessThanMinNoPreemptedReturnFailed)
{
    auto pod1 = view_utils::Get1DResourceUnit("pod1");
    auto pod2 = view_utils::Get1DResourceUnit("pod2");
    auto pod3 = view_utils::Get1DResourceUnit("pod3");
    resourceView_->AddResourceUnit(pod1);
    resourceView_->AddResourceUnit(pod2);
    resourceView_->AddResourceUnit(pod3);
    std::priority_queue<NodeScore> candidates;
    auto feasible1 = NodeScore(pod1.id(), 100);
    feasible1.availableForRequest = 3;
    auto feasible2 = NodeScore(pod2.id(), 80);
    feasible2.availableForRequest = 2;
    auto feasible3 = NodeScore(pod3.id(), 60);
    feasible3.availableForRequest = 1;
    candidates.emplace(feasible1);
    candidates.emplace(feasible2);
    candidates.emplace(feasible3);
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, _)).WillOnce(Return(ScheduleResults{0, "", candidates}));

    litebus::Promise<std::string> cancel;
    std::vector<std::shared_ptr<schedule_decision::InstanceItem>> insItems;
    for (int i = 0; i < 10; i++) {
        insItems.emplace_back(GetInstanceItem(5, 1.0, 1.0));
    }
    (*insItems[0]->scheduleReq->mutable_contexts())[GROUP_SCHEDULE_CONTEXT].mutable_groupschedctx()->set_reserved(
        "pod1");
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    auto range = schedule_decision::GroupSpec::RangeOpt{ .isRange = true, .min = 7, .max = 15, .step = 1 };
    auto promise = std::make_shared<litebus::Promise<schedule_decision::GroupScheduleResult>>();
    auto scheduleItem =
        std::make_shared<schedule_decision::GroupItem>(insItems, promise, "group001", cancel.GetFuture(), range);
    auto result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, StatusCode::RESOURCE_NOT_ENOUGH);
    EXPECT_NE(result.reason.find("no available resource that meets the request requirements"), std::string::npos);
    EXPECT_EQ(result.results.size(), size_t(7));
}

TEST_F(SchedulerPerformerTest, DuplicateSchedule)
{
    auto pod1 = view_utils::Get1DResourceUnit("unit1");
    pod1.set_ownerid("unit1");
    resourceView_->AddResourceUnit(pod1);
    auto insItem1 = GetInstanceItem(5, 40.0, 40.0);
    insItem1->scheduleReq->mutable_instance()->set_unitid("unit1");
    auto insItem2 = GetInstanceItem(5, 45.0, 45.0);
    insItem2->scheduleReq->mutable_instance()->set_unitid("unit1");
    (*insItem1->scheduleReq->mutable_contexts())[GROUP_SCHEDULE_CONTEXT]
        .mutable_groupschedctx()
        ->set_reserved("unit1");
    (*insItem2->scheduleReq->mutable_contexts())[GROUP_SCHEDULE_CONTEXT]
        .mutable_groupschedctx()
        ->set_reserved("unit1");
    std::vector<std::shared_ptr<schedule_decision::InstanceItem>> insItems = {insItem1, insItem2};
    auto promise = std::make_shared<litebus::Promise<schedule_decision::GroupScheduleResult>>();
    litebus::Promise<std::string> cancel;
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    auto scheduleItem = std::make_shared<schedule_decision::GroupItem>(
        insItems, promise, "group001", cancel.GetFuture(), schedule_decision::GroupSpec::RangeOpt());
    auto result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, 0);
    ASSERT_EQ(result.results.size(), (size_t)2);
    EXPECT_EQ(result.results[0].id, "unit1");

    resourceView_->AddInstances(
        { { insItem1->scheduleReq->instance().instanceid(), { insItem1->scheduleReq->instance(), nullptr } },
          { insItem2->scheduleReq->instance().instanceid(), { insItem2->scheduleReq->instance(), nullptr } } });
    result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, 0);
}

TEST_F(SchedulerPerformerTest, ScheduleGroupWithStrictPack)
{
    auto pod1 = view_utils::Get1DResourceUnit("unit1");
    resourceView_->AddResourceUnit(pod1);
    auto pod2 = view_utils::Get1DResourceUnit("unit2");
    resourceView_->AddResourceUnit(pod2);
    resource_view::ResourceViewInfo info = resourceView_->GetResourceInfo().Get();
    std::priority_queue<NodeScore> candidates1;
    auto feasible1 = NodeScore(pod1.id(), 10);
    feasible1.availableForRequest = 1;
    candidates1.emplace(feasible1);
    EXPECT_CALL(*mockFrameWork_, SelectFeasible(_, _, _, _))
        .WillOnce(Return(ScheduleResults{ 0, "", candidates1 }));
    auto insItem1 = GetInstanceItem(0, view_utils::INST_SCALA_VALUE, view_utils::INST_SCALA_VALUE);
    auto insItem2 = GetInstanceItem(0, view_utils::INST_SCALA_VALUE, view_utils::INST_SCALA_VALUE);
    std::vector<std::shared_ptr<schedule_decision::InstanceItem>> insItems = {insItem1, insItem2};
    int32_t cnt = 0;
    auto preemptCallbackFunc =
        [&cnt](const std::vector<schedule_decision::PreemptResult> &preemptResults) -> litebus::Future<Status> {
        cnt += preemptResults.size();
        return Status::OK();
    };
    groupSchedulerPerformer_->RegisterPreemptInstanceCallback(preemptCallbackFunc);
    auto promise = std::make_shared<litebus::Promise<schedule_decision::GroupScheduleResult>>();
    litebus::Promise<std::string> cancel;
    auto ctx = GetPreAllocatedContext(info);
    auto scheduleItem = std::make_shared<schedule_decision::GroupItem>(
        insItems, promise, "group001", cancel.GetFuture(), schedule_decision::GroupSpec::RangeOpt());
    scheduleItem->groupSchedulePolicy = common::GroupPolicy::StrictPack;
    auto result = groupSchedulerPerformer_->DoSchedule(GetPreAllocatedContext(info), info, scheduleItem);
    EXPECT_EQ(result.code, 0);
    EXPECT_EQ(cnt, 0);
    ASSERT_EQ(result.results.size(), size_t{2});
    EXPECT_EQ(result.results[0].unitID, "unit1");
    EXPECT_EQ(result.results[1].unitID, "unit1");
}

}  // namespace functionsystem::test