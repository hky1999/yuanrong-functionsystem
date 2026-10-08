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

#include "posix_api_handler.h"

#include "async/defer.hpp"
#include "common/logs/logging.h"
#include "common/proto/pb/message_pb.h"
#include "common/resource_view/resource_type.h"
#include "common/rpc/stream/posix/control_client.h"
#include "common/status/status.h"
#include "common/utils/generate_message.h"
#include "common/utils/struct_transfer.h"
#include "common/trace/trace_manager.h"

using namespace runtime_rpc;
using namespace std::placeholders;

namespace functionsystem::local_scheduler {
REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(StreamingMessage::kCreateReq, &PosixAPIHandler::Create);
REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(StreamingMessage::kCreateReqs, &PosixAPIHandler::GroupCreate);
REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(StreamingMessage::kKillReq, &PosixAPIHandler::Kill);
REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(StreamingMessage::kExitReq, &PosixAPIHandler::Exit);
REGISTER_FUNCTION_SYS_POSIX_CONTROL_HANDLER(StreamingMessage::kRGroupReq, &PosixAPIHandler::CreateResourceGroup);

const int32_t MAX_AFFINITY_SIZE = 4;

Status PosixAPIHandler::IsValidCreateRequest(const CreateRequest &createReq)
{
    if (createReq.has_schedulingops() && createReq.schedulingops().priority() > maxPriority_) {
        return Status(ERR_PARAM_INVALID, "invalid instance priority, should < " + std::to_string(maxPriority_));
    }
    if (!createReq.designatedinstanceid().empty() && !IsInstanceIdSecure(createReq.designatedinstanceid())) {
        return Status(ERR_PARAM_INVALID, "invalid designated instanceid");
    }
    return Status::OK();
}

Status PosixAPIHandler::IsValidCreateRequests(const CreateRequests &createReqs)
{
    if (createReqs.requests().empty()) {
        return Status(ERR_PARAM_INVALID, "create group with empty instance, at least one is required.");
    }
    auto priority = createReqs.requests().begin()->schedulingops().priority();
    for (const auto &createReq : createReqs.requests()) {
        if (priority != createReq.schedulingops().priority()) {
            return Status(ERR_PARAM_INVALID, "invalid priority, create group with different instance priority.");
        }
        if (auto status = IsValidCreateRequest(createReq); !status.IsOk()) {
            return status;
        }
    }
    return Status::OK();
}

litebus::Future<std::shared_ptr<StreamingMessage>> PosixAPIHandler::Create(
    const grpc::PosixStreamIdentity &identity, const std::shared_ptr<StreamingMessage> &request)
{
    const std::string from = identity.instanceID; // stream-bound source identity
    auto &createReq = *request->mutable_createreq();
    const auto requestID = createReq.requestid();
    const auto traceID = createReq.traceid();
    auto response = std::make_shared<StreamingMessage>();
    if (auto status = IsValidCreateRequest(createReq); !status.IsOk()) {
        YRLOG_ERROR("{}|{}|failed to create instance from {}, reason: ", traceID, requestID, from, status.GetMessage());
        response->mutable_creatersp()->set_code(Status::GetPosixErrorCode(status.StatusCode()));
        response->mutable_creatersp()->set_message(status.GetMessage());
        return response;
    }
    auto instanceCtrl = instanceCtrl_.lock();
    if (instanceCtrl == nullptr) {
        YRLOG_ERROR("{}|{}|failed to create instance from {}, control is nullptr.", traceID, requestID, from);
        response->mutable_creatersp()->set_code(common::ERR_LOCAL_SCHEDULER_ABNORMAL);
        response->mutable_creatersp()->set_message("instance control is nullptr in local scheduler");
        return response;
    }
    YRLOG_INFO("{}|{}|receive a create instance request from {}.", traceID, createReq.requestid(), from);
    auto traceParent = trace::TraceManager::GetTraceParentFromOptions(
        createReq.createoptions(), createReq.has_schedulingops() ? &createReq.schedulingops().extension() : nullptr);
    auto scheduleReq = TransFromCreateReqToScheduleReq(std::move(createReq), from);
    trace::TraceManager::SpanParam param;
    param.spanName = trace::SpanName::K_CREATE;
    param.spanKey = requestID;
    param.traceID = traceID;
    param.traceParent = traceParent;
    param.function = scheduleReq->instance().function();
    param.instanceID = scheduleReq->instance().instanceid();
    auto span = trace::TraceManager::GetInstance().StartSpanWithRecord(std::move(param));
    trace::TraceManager::PropagateSpanToOptions(span,
                                                scheduleReq->mutable_instance()->mutable_createoptions(),
                                                scheduleReq->mutable_instance()->mutable_scheduleoption()
                                                    ->mutable_extension());
    scheduleReq->mutable_instance()->set_parentfunctionproxyaid(instanceCtrl->GetActorAID());
    auto runtimePromise = std::make_shared<litebus::Promise<messages::ScheduleResponse>>();
    (void)instanceCtrl->Schedule(scheduleReq, runtimePromise);
    return runtimePromise->GetFuture().Then(
        [from, requestID, traceID](const litebus::Future<messages::ScheduleResponse> &future)
            -> litebus::Future<std::shared_ptr<StreamingMessage>> {
            auto createRsp = TransFromScheduleRspToCreateRsp(future.Get());
            auto response = std::make_shared<StreamingMessage>();
            YRLOG_INFO("{}|{}|reply create instance response to {}. code: {}, message: {}", traceID, requestID, from,
                       fmt::underlying(createRsp.code()), createRsp.message());
            *response->mutable_creatersp() = std::move(createRsp);
            return response;
        });
}

litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> PosixAPIHandler::GroupCreate(
    const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request)
{
    const std::string from = identity.instanceID; // stream-bound source identity
    auto createReqs = std::make_shared<CreateRequests>(std::move(*request->mutable_createreqs()));
    const auto &requestID = createReqs->requestid();
    const auto &traceID = createReqs->traceid();
    auto response = std::make_shared<StreamingMessage>();
    if (auto status = IsValidCreateRequests(*createReqs); !status.IsOk()) {
        YRLOG_ERROR("{}|{}|failed to create group from {}, reason: ", traceID, requestID, from, status.GetMessage());
        response->mutable_creatersps()->set_code(Status::GetPosixErrorCode(status.StatusCode()));
        response->mutable_creatersps()->set_message(status.GetMessage());
        return response;
    }
    auto localGroupCtrl = localGroupCtrl_.lock();
    if (localGroupCtrl == nullptr) {
        YRLOG_ERROR("{}|{}|failed to create group instance from {}, group control is nullptr.", traceID, requestID,
                    from);
        response->mutable_creatersps()->set_code(common::ERR_INNER_COMMUNICATION);
        response->mutable_creatersps()->set_message("group control is nullptr in local scheduler");
        return response;
    }
    YRLOG_INFO("{}|{}|receive create group request from {}.", traceID, requestID, from);
    return localGroupCtrl->GroupSchedule(from, createReqs)
        .Then([response](const std::shared_ptr<CreateResponses> &responses)
                  -> litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> {
            response->mutable_creatersps()->CopyFrom(*responses);
            return response;
        });
}

litebus::Future<std::shared_ptr<StreamingMessage>> PosixAPIHandler::Kill(
    const grpc::PosixStreamIdentity &identity, const std::shared_ptr<StreamingMessage> &request)
{
    const std::string from = identity.instanceID; // stream-bound source identity
    ASSERT_IF_NULL(request);
    auto killReq = std::make_shared<KillRequest>(std::move(*request->mutable_killreq()));
    auto response = std::make_shared<StreamingMessage>();
    auto instanceCtrl = instanceCtrl_.lock();
    if (instanceCtrl == nullptr) {
        YRLOG_ERROR("failed to kill instance({}), control is nullptr, signal: {}", killReq->instanceid(),
                    killReq->signal());
        response->mutable_killrsp()->set_code(common::ERR_LOCAL_SCHEDULER_ABNORMAL);
        response->mutable_killrsp()->set_message("instance control is nullptr in local scheduler");
        return response;
    }
    // former compatible
    if (killReq->requestid().empty()) {
        killReq->set_requestid(killReq->instanceid() + "-" + std::to_string(killReq->signal()));
    }

    YRLOG_INFO("{}|receive kill request(signal {}) from instance({}) to instance({}).", killReq->requestid(),
               killReq->signal(), from, killReq->instanceid());
    return instanceCtrl->Kill(from, killReq)
        .Then([from, response](
                  const litebus::Future<KillResponse> &future) -> litebus::Future<std::shared_ptr<StreamingMessage>> {
            response->mutable_killrsp()->CopyFrom(future.Get());
            return response;
        });
}

litebus::Future<std::shared_ptr<StreamingMessage>> PosixAPIHandler::Exit(
    const grpc::PosixStreamIdentity &identity, const std::shared_ptr<StreamingMessage> &request)
{
    const std::string from = identity.instanceID; // stream-bound source identity
    ASSERT_IF_NULL(request);
    auto instanceID = from;
    YRLOG_INFO("receive exit request from instance({})", instanceID);
    auto response = std::make_shared<StreamingMessage>();
    (void)response->mutable_exitrsp();
    auto instanceCtrl = instanceCtrl_.lock();
    if (instanceCtrl == nullptr) {
        YRLOG_ERROR("failed to exit instance({}), instance control is nullptr", instanceID);
        return response;
    }

    auto exitReq = std::make_shared<ExitRequest>(request->exitreq());
    return instanceCtrl->Exit(from, exitReq).Then([response, instanceID](const ExitResponse &resp) {
        YRLOG_INFO("exit instance({}), exit code: {}", instanceID, fmt::underlying(resp.code()));
        response->mutable_exitrsp()->CopyFrom(resp);
        return response;
    });
}

litebus::Future<std::pair<bool, std::shared_ptr<StreamingMessage>>> PosixAPIHandler::CallResult(
    const grpc::PosixStreamIdentity &identity, std::shared_ptr<functionsystem::CallResult> &callResult)
{
    const std::string from = identity.instanceID; // stream-bound source identity
    auto instanceCtrl = instanceCtrl_.lock();
    if (instanceCtrl == nullptr) {
        YRLOG_ERROR("failed to call result, instance control is nullptr, instance({})", from);
        return std::make_pair<bool, std::shared_ptr<StreamingMessage>>(false, nullptr);
    }
    const auto &requestID = callResult->requestid();
    const auto &instanceID = callResult->instanceid();

    YRLOG_DEBUG("{}|receive CallResult for caller({}) from callee({}).", requestID, instanceID, from);
    return instanceCtrl->CallResult(from, callResult).Then([](const CallResultAck &callResultAck) {
        auto output = std::make_shared<runtime_rpc::StreamingMessage>();
        if (static_cast<StatusCode>(callResultAck.code()) == StatusCode::LS_REQUEST_NOT_FOUND) {
            return std::make_pair<bool, std::shared_ptr<StreamingMessage>>(false, std::move(output));
        }
        output->mutable_callresultack()->CopyFrom(callResultAck);
        return std::make_pair<bool, std::shared_ptr<StreamingMessage>>(true, std::move(output));
    });
}

litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> PosixAPIHandler::CreateResourceGroup(
    const grpc::PosixStreamIdentity &identity, const std::shared_ptr<runtime_rpc::StreamingMessage> &request)
{
    const std::string from = identity.instanceID; // stream-bound source identity
    auto rgReq = std::make_shared<CreateResourceGroupRequest>(std::move(*request->mutable_rgroupreq()));
    (*rgReq->mutable_rgroupspec()->mutable_opt()->mutable_extension())["parentId"] = from;
    const auto &requestID = rgReq->requestid();
    const auto &traceID = rgReq->traceid();
    auto response = std::make_shared<runtime_rpc::StreamingMessage>();

    auto rGroupCtrl = rGroupCtrl_.lock();
    if (rGroupCtrl == nullptr) {
        YRLOG_ERROR("{}|{}|failed to create resource group manager from {}, rGroupCtrl is nullptr.",
                    traceID, requestID, from);
        response->mutable_rgrouprsp()->set_code(common::ERR_INNER_SYSTEM_ERROR);
        response->mutable_rgrouprsp()->set_message("resource group manager is nullptr in function proxy");
        return response;
    }
    YRLOG_INFO("{}|{}|receive create resource group request from {}.", traceID, requestID, from);
    return rGroupCtrl->Create(from, rgReq)
        .Then([response](const std::shared_ptr<CreateResourceGroupResponse> &rsp)
                  -> litebus::Future<std::shared_ptr<runtime_rpc::StreamingMessage>> {
            response->mutable_rgrouprsp()->CopyFrom(*rsp);
            return response;
        });
}

void PosixAPIHandler::BindControlClientManager(const std::shared_ptr<ControlInterfaceClientManagerProxy> &clientManager)
{
    clientManager_ = clientManager;
}
}  // namespace functionsystem::local_scheduler
