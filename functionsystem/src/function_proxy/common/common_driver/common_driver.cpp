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
#include "common_driver.h"

#include "common/constants/actor_name.h"
#include "common/datasystem_capability.h"
#include "meta_store_monitor/meta_store_monitor_factory.h"
#include "common/posix_client/shared_client/shared_client_manager.h"
#include "common/posix_client/shared_client/posix_stream_manager_proxy.h"
#include "function_proxy/common/state_handler/state_handler.h"

namespace functionsystem::function_proxy {

void PrepareMetaStoreConfigs(const function_proxy::Flags &flags, MetaStoreTimeoutOption &option,
                             MetaStoreMonitorParam &param, MetaStoreConfig &metaStoreConfig)
{
    param.maxTolerateFailedTimes = flags.GetMaxTolerateMetaStoreFailedTimes();
    param.checkIntervalMs = flags.GetMetaStoreCheckInterval();
    param.timeoutMs = flags.GetMetaStoreCheckTimeout();

    // if enable, metastore address is master ip + global scheduler port; etcd address is used for persistence
    // else metastore address is etcd ip
    metaStoreConfig.enableMetaStore = flags.GetEnableMetaStore();
    metaStoreConfig.isMetaStorePassthrough = flags.GetMetaStoreMode() == "passthrough";
    metaStoreConfig.etcdTablePrefix = flags.GetETCDTablePrefix();
    metaStoreConfig.excludedKeys = flags.GetMetaStoreExcludedKeys();
    if (metaStoreConfig.enableMetaStore) {
        metaStoreConfig.etcdAddress = flags.GetEtcdAddress();
        metaStoreConfig.metaStoreAddress = flags.GetMetaStoreAddress();
    } else {
        metaStoreConfig.etcdAddress = flags.GetMetaStoreAddress();
    }

    if (metaStoreConfig.enableMetaStore && !metaStoreConfig.isMetaStorePassthrough) {
        param.backend = METASTORE_BACKEND;
    }

    // retries must take longer than health check
    option.operationRetryTimes =
        static_cast<int64_t>((param.maxTolerateFailedTimes + 1) * (param.checkIntervalMs + param.timeoutMs) /
                             KV_OPERATE_RETRY_INTERVAL_LOWER_BOUND);
}

std::string GetMonitorAddress(const function_proxy::Flags &flags)
{
    // if enabled, return master address; else return etcd address
    return flags.GetMetaStoreAddress();
}

void CommonDriver::BindDataObjClient()
{
    distributedCacheClient_->EnableRouterClient(true);
    distributedCacheClient_->EnableDSClient(true);
    auto dataObjActor = std::make_shared<DataObjActor>(distributedCacheClient_);
    dataObjClient_ = std::make_shared<DataObjClient>(dataObjActor);
    litebus::Spawn(dataObjActor);
}

void CommonDriver::BindStateActor()
{
    distributedCacheClient_->EnableDSClient(true);
    auto stateClient = std::make_shared<StateClient>(distributedCacheClient_);
    auto stateActor = std::make_shared<function_proxy::StateActor>(stateClient);
    litebus::Spawn(stateActor);
    StateHandler::BindStateActor(stateActor);
}

void CommonDriver::CreateDistributedCacheClient()
{
    datasystem::ConnectOptions connectOptions{};
    RouterConnectOptions routerConnectOptions{};
    DSCacheClientImpl::GetAuthConnectOptions(dsAuthConfig_, connectOptions);
    connectOptions.host = flags_.GetCacheStorageHost();
    connectOptions.port = flags_.GetCacheStoragePort();

    // routerConnectOptions
    routerConnectOptions.azName = flags_.GetCacheStorageInfoPrefix();
    if (flags_.GetEnableMetaStore()) {
        routerConnectOptions.etcdAddress = flags_.GetEtcdAddress();
    } else {
        routerConnectOptions.etcdAddress = flags_.GetMetaStoreAddress();
    }
    auto etcdAuth = GetGrpcTLSConfig(flags_);
    routerConnectOptions.etcdCa = etcdAuth.ca;
    routerConnectOptions.etcdKey = etcdAuth.privateKey;
    routerConnectOptions.etcdCert = etcdAuth.cert;
    distributedCacheClient_ = std::make_shared<DSCacheClientImpl>(connectOptions, routerConnectOptions);
    distributedCacheClient_->SetDSAuthEnable(flags_.GetCacheStorageAuthEnable());
}

bool CommonDriver::NeedBindDs()
{
    if (!datasystem_capability::GetEnvironmentCapability().dataSystemDeployed) {
        YRLOG_INFO("DataSystem is disabled, skip binding DataSystem clients");
        return false;
    }
    bool isDataSystemFeatureUsed = false;
    if (auto dataSystemFeatureUsed = litebus::os::GetEnv("DATA_SYSTEM_FEATURE_USED"); dataSystemFeatureUsed.IsSome()) {
        isDataSystemFeatureUsed = (dataSystemFeatureUsed == DATASYSTEM_FEATURE_USED_STREAM);
    }
    YRLOG_INFO("isDataSystemFeatureUsed {}", isDataSystemFeatureUsed);

    return flags_.GetStateStorageType() == function_proxy::DATA_SYSTEM_STORE || isDataSystemFeatureUsed;
}

void CommonDriver::InitDistributedCache()
{
    if (!datasystem_capability::GetEnvironmentCapability().dataSystemDeployed) {
        YRLOG_INFO("DataSystem is disabled, skip creating the DataSystem cache client");
        return;
    }
    CreateDistributedCacheClient();
    if (!NeedBindDs()) {
        return;
    }
    YRLOG_INFO("need to bind ds client");
    BindStateActor();
    BindDataObjClient();
}

Status CommonDriver::InitMetaStoreClient()
{
    YRLOG_INFO("start to init meta store client");
    MetaStoreTimeoutOption option;
    MetaStoreMonitorParam param;
    MetaStoreConfig metaStoreConfig;
    PrepareMetaStoreConfigs(flags_, option, param, metaStoreConfig);

    while (true) {
        metaStoreClient_ = MetaStoreClient::Create(metaStoreConfig, GetGrpcSSLConfig(flags_), option, true, param);
        auto metaStoreMonitor = MetaStoreMonitorFactory::GetInstance().GetMonitor(GetMonitorAddress(flags_));
        if (metaStoreClient_ == nullptr || metaStoreMonitor == nullptr
            || metaStoreMonitor->CheckMetaStoreConnected().IsError()) {
            return Status(StatusCode::FAILED, "meta store connected failed");
        }
        YRLOG_INFO("successful to init meta store client");
        return Status::OK();
    }
}

Status CommonDriver::CreateInternalIAM(const std::shared_ptr<MetaStorageAccessor> &metaStorageAccessor)
{
    YRLOG_INFO("start to init internal iam");
    auto metaStorageAccessorForIAM = metaStorageAccessor;
    if (!flags_.GetIAMMetaStoreAddress().empty() && flags_.GetMetaStoreAddress() != flags_.GetIAMMetaStoreAddress()) {
        MetaStoreConfig iamMetaStoreConfig{ .etcdAddress = flags_.GetIAMMetaStoreAddress() };
        MetaStoreTimeoutOption option {};
        // retries must take longer than health check
        option.operationRetryTimes =
            static_cast<int64_t>((flags_.GetMaxTolerateMetaStoreFailedTimes() + 1) *
                                 (flags_.GetMetaStoreCheckInterval() + flags_.GetMetaStoreCheckTimeout()) /
                                 KV_OPERATE_RETRY_INTERVAL_LOWER_BOUND);
        MetaStoreMonitorParam param{ .maxTolerateFailedTimes = flags_.GetMaxTolerateMetaStoreFailedTimes(),
                                     .checkIntervalMs = flags_.GetMetaStoreCheckInterval(),
                                     .timeoutMs = flags_.GetMetaStoreCheckTimeout() };
        // iam never enables meta store
        metaStorageAccessorForIAM = std::make_shared<MetaStorageAccessor>(
            MetaStoreClient::Create(iamMetaStoreConfig, GetGrpcSSLConfig(flags_), option, true, param));
        auto metaStoreMonitor = MetaStoreMonitorFactory::GetInstance().GetMonitor(flags_.GetIAMMetaStoreAddress());
        if (metaStoreMonitor == nullptr || metaStoreMonitor->CheckMetaStoreConnected().IsError()) {
            return Status(StatusCode::FAILED, "meta store for iam connected failed");
        }
    }
    auto iamClient = function_proxy::IAMClient::CreateIAMClient(flags_.GetIAMBasePath());
    InternalIAM::Param internalIAMParam = { .isEnableIAM = flags_.GetIsEnableIAM(),
                                            .policyPath = flags_.GetIamPolicyFile(),
                                            .clusterID = flags_.GetClusterID(),
                                            .credType = function_proxy::IAMCredType::TOKEN };
    internalIAM_ = std::make_shared<InternalIAM>(internalIAMParam);
    internalIAM_->BindMetaStorageAccessor(metaStorageAccessorForIAM);
    internalIAM_->BindIAMClient(iamClient);
    if (internalIAM_->IsIAMEnabled() && !internalIAM_->Init()) {
        YRLOG_ERROR("failed to start init internal IAM.");
        return Status(StatusCode::FAILED, "failed to start init internal IAM.");
    }
    YRLOG_INFO("successful to init internal iam");
    return Status::OK();
}

void CommonDriver::CreateDataAndControlInterfaceClient()
{
    YRLOG_INFO("start to create posix interface client");
    static std::shared_ptr<SharedClientManager> sharedClientMgr =
        std::make_shared<SharedClientManager>("SharedPosixClientManager");
    litebus::Spawn(sharedClientMgr);
    auto posixStreamManagerProxy = std::make_shared<PosixStreamManagerProxy>(sharedClientMgr->GetAID());
    posixService_->RegisterUpdatePosixClientCallback(
        std::bind(&PosixStreamManagerProxy::UpdateControlInterfacePosixClient, posixStreamManagerProxy,
                  std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
    dataInterfaceClient_ = posixStreamManagerProxy;
    controlInterfaceClient_ = posixStreamManagerProxy;
    return;
}

void CommonDriver::InitObserver(const std::shared_ptr<MetaStorageAccessor> &metaStorageAccessor)
{
    YRLOG_INFO("start to init observer");
    // create observer actor
    observerActor_ = std::make_shared<function_proxy::ObserverActor>(
        FUNCTION_PROXY_OBSERVER_ACTOR_NAME, flags_.GetNodeID(), metaStorageAccessor,
        ObserverParam{ flags_.GetServicesPath(), flags_.GetLibPath(), flags_.GetFunctionMetaPath(),
                       flags_.GetEnableIpv4TenantIsolation(), flags_.GetEnableTenantAffinity(),
                       flags_.GetEnableMetaStore(), flags_.IsPartialWatchInstances(), flags_.GetServiceTTL() });
    observerActor_->BindDataInterfaceClientManager(dataInterfaceClient_);
    observerActor_->BindInternalIAM(internalIAM_);
    YRLOG_INFO("successful to init observer");
}

Status CommonDriver::Init()
{
    if (auto status = InitMetaStoreClient(); status.IsError()) {
        YRLOG_ERROR("failed to init meta store client. err: {}", status.ToString());
        return status;
    }

    metaStorageAccessor_ = std::make_shared<MetaStorageAccessor>(metaStoreClient_);
    if (auto status = CreateInternalIAM(metaStorageAccessor_); status.IsError()) {
        YRLOG_ERROR("failed to init internal iam. err: {}", status.ToString());
        return status;
    }
    InitDistributedCache();
    // posix stream interface
    CreateDataAndControlInterfaceClient();
    InitObserver(metaStorageAccessor_);
    return Status::OK();
}

Status CommonDriver::Start()
{
    if (observerActor_ != nullptr) {
        litebus::Spawn(observerActor_);
    }
    return Status::OK();
}

Status CommonDriver::Sync()
{
    if (observerActor_ == nullptr) {
        return Status(StatusCode::FAILED, "observer is not init");
    }
    YRLOG_INFO("start to sync observer");
    auto status = litebus::Async(observerActor_->GetAID(), &function_proxy::ObserverActor::Register).Get();
    if (status.IsError()) {
        YRLOG_ERROR("failed to register observer");
        return status;
    }
    YRLOG_INFO("successful to sync observer");
    return Status::OK();
}

Status CommonDriver::Stop()
{
    MetaStoreMonitorFactory::GetInstance().Clear();
    if (observerActor_ != nullptr) {
        litebus::Terminate(observerActor_->GetAID());
    }
    if (dataObjClient_ != nullptr) {
        dataObjClient_->Stop();
    }
    return Status::OK();
}

void CommonDriver::Await()
{
    if (observerActor_ != nullptr) {
        litebus::Await(observerActor_->GetAID());
    }
    if (dataObjClient_ != nullptr) {
        dataObjClient_->Await();
    }
}

}  // namespace functionsystem::function_proxy
