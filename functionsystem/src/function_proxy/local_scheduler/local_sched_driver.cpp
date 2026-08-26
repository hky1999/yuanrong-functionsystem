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

#include "local_sched_driver.h"

#include <charconv>

#include "common/constants/actor_name.h"
#include "local_scheduler/traefik_registry/traefik_registry.h"
#include "meta_store_monitor/meta_store_monitor_factory.h"
#include "common/utils/param_check.h"
#include "local_scheduler/bundle_manager/bundle_mgr_actor.h"
#include "local_scheduler/debug_instance_info_monitor/debug_instance_info_monitor.h"
#include "local_scheduler/instance_control/posix_api_handler/posix_api_handler.h"
#include "local_scheduler/gc_actor/local_gc_actor.h"
#include "local_scheduler/local_group_ctrl/local_group_ctrl_actor.h"
#include "local_scheduler/grpc_server/bus_service/bus_service.h"
#include "local_scheduler/tcp_tunnel_server.h"
#include "local_scheduler/grpc_server/frontend_proxy_service/frontend_proxy_lifecycle_handler.h"
#include "local_scheduler/grpc_server/frontend_proxy_service/frontend_proxy_service.h"

namespace functionsystem::local_scheduler {

const std::string LOCAL_SCHEDULER = "local-scheduler";

namespace {
/**
 * Extract IP address from a full address string (ip:port format).
 * This is used to get the IP from LiteBus address for gRPC servers.
 *
 * @param address Full address in "ip:port" format
 * @return Extracted IP address, or original address if no colon found
 */
std::string ExtractIPFromAddress(const std::string& address)
{
    size_t colonPos = address.find_last_of(':');
    if (colonPos != std::string::npos && colonPos > 0) {
        // Handle IPv6 addresses like [::1]:port
        if (address[0] == '[') {
            size_t closeBracketPos = address.find(']', 1);
            if (closeBracketPos != std::string::npos && closeBracketPos < colonPos) {
                return address.substr(1, closeBracketPos - 1);
            }
        }
        return address.substr(0, colonPos);
    }
    return address;  // fallback: return as-is
}
}  // namespace

LocalSchedDriver::LocalSchedDriver(LocalSchedStartParam &&param,
                                   const std::shared_ptr<MetaStoreClient> metaStoreClient)
    : param_(std::move(param)), metaStoreClient_(metaStoreClient)
{
}

LocalSchedDriver::~LocalSchedDriver() = default;

void LocalSchedDriver::SetRuntimeConfig(InstanceCtrlConfig &config)
{
    ASSERT_IF_NULL(param_.dsAuthConfig);
    RuntimeConfig runtimeConfig{
        .runtimeHeartbeatEnable = param_.runtimeHeartbeatEnable,
        .runtimeMaxHeartbeatTimeoutTimes = param_.runtimeMaxHeartbeatTimeoutTimes,
        .runtimeHeartbeatTimeoutMS = param_.runtimeHeartbeatTimeoutMS,
        .runtimeInitCallTimeoutMS = param_.runtimeInitCallTimeoutMS,
        .runtimeShutdownTimeoutSeconds = param_.runtimeShutdownTimeoutSeconds,
        .runtimeDsAuthEnable = param_.dsAuthConfig->isRuntimeEnable,
        .runtimeDsEncryptEnable = param_.dsAuthConfig->isRuntimeEncryptEnable,
        .dataSystemAccessKey = param_.dsAuthConfig->ak,
        .dataSystemSecurityKey = param_.dsAuthConfig->sk,
        .runtimeDsClientPublicKey = param_.dsAuthConfig->clientPublicKey,
        .runtimeDsClientPrivateKey = param_.dsAuthConfig->clientPrivateKey,
        .runtimeDsServerPublicKey = param_.dsAuthConfig->serverPublicKey,
    };
    YRLOG_INFO(
        "runtime heartbeat config: runtimeHeartbeatEnable: {}, runtimeMaxHeartbeatTimeoutTimes: {}, "
        "runtimeHeartbeatTimeoutMS: {}, runtimeRecoverEnable: {}, runtimeInitCallTimeoutMS:{}, "
        "runtimeShutdownTimeoutSeconds:{} ",
        param_.runtimeHeartbeatEnable, param_.runtimeMaxHeartbeatTimeoutTimes, param_.runtimeHeartbeatTimeoutMS,
        param_.runtimeRecoverEnable, param_.runtimeInitCallTimeoutMS, param_.runtimeShutdownTimeoutSeconds);

    config.runtimeConfig = runtimeConfig;
}

Status LocalSchedDriver::Create()
{
    metaStorageAccessor_ = std::make_shared<MetaStorageAccessor>(metaStoreClient_);
    resourceViewMgr_ = std::make_shared<resource_view::ResourceViewMgr>();
    resourceViewMgr_->Init(param_.nodeID, param_.resourceViewActorParam);

    localSchedSrv_ = LocalSchedSrv::Create(param_.localSchedSrvParam);

    // Set co-process mode: skip Recover internals, let reconcile handle cleanup
    param_.funcAgentMgrParam.enableCoProcessMode = param_.enableMergeProcess;
    funcAgentMgr_ = FunctionAgentMgr::Create(param_.nodeID, param_.funcAgentMgrParam, metaStoreClient_);

    abnormalProcessor_ = AbnormalProcessor::Create(param_.nodeID);

    rGroupCtrl_ = ResourceGroupCtrl::Init();

    InstanceCtrlConfig config{};
    SetRuntimeConfig(config);
    config.maxGrpcSize = param_.maxGrepSize;
    config.connectTimeout = param_.runtimeConnTimeoutSeconds;
    config.isPseudoDataPlane = param_.isPseudoDataPlane;
    config.cacheStorageHost = param_.cacheStorageHost;
    config.limitResource = {
        .minCpu = param_.limitResource.minCpu,
        .minMemory = param_.limitResource.minMemory,
        .maxCpu = param_.limitResource.maxCpu,
        .maxMemory = param_.limitResource.maxMemory,
    };
    config.enableServerMode = param_.enableServerMode;
    config.enableSSL = param_.enableSSL;
    config.serverRootCert = param_.serverRootCert;
    config.serverNameOverride = param_.serverNameOverride;
    config.posixPort = param_.posixPort;
    config.schedulePlugins = param_.schedulePlugins;
    config.enableTenantAffinity = param_.enableTenantAffinity;
    config.createLimitationEnable = param_.createLimitationEnable;
    config.tokenBucketCapacity = param_.tokenBucketCapacity;
    config.isMetaStoreEnabled = param_.isMetaStoreEnabled;
    config.isPartialWatchInstances = param_.isPartialWatchInstances;
    config.maxPriority = param_.maxPriority;
    config.enablePreemption = param_.enablePreemption;
    config.enableFakeSuspendResume = param_.enableFakeSuspendResume;
    config.udsPath = param_.udsPath;
    // Set proxy gRPC address (ip:port format)
    // Use session port when session server is enabled, otherwise use posix port
    // Extract IP from LiteBus address to ensure external connectivity
    std::string externalIP = ExtractIPFromAddress(param_.address);
    if (param_.sessionGrpcPort != "0") {
        config.proxyGrpcAddress = externalIP + ":" + param_.sessionGrpcPort;
    } else {
        config.proxyGrpcAddress = externalIP + ":" + param_.grpcListenPort;
    }
    instanceCtrl_ = InstanceCtrl::Create(param_.nodeID, config);
    PosixAPIHandler::BindInstanceCtrl(instanceCtrl_);
    PosixAPIHandler::BindControlClientManager(param_.controlInterfacePosixMgr);
    PosixAPIHandler::BindLocalSchedSrv(localSchedSrv_);
    PosixAPIHandler::BindResourceGroupCtrl(rGroupCtrl_);
    PosixAPIHandler::SetMaxPriority(param_.maxPriority);

    if (param_.enableTraefikRegistry) {
        traefikRegistry_ = std::make_shared<TraefikRegistry>(
            metaStorageAccessor_,
            param_.traefikEtcdPrefix,
            param_.traefikHttpEntryPoint,
            param_.traefikEnableTLS,
            param_.traefikServersTransport);
        instanceCtrl_->SetTraefikRegistry(traefikRegistry_);
        YRLOG_INFO("TraefikRegistry initialized: prefix={}, entryPoint={}, enableTLS={}, serversTransport={}",
            param_.traefikEtcdPrefix, param_.traefikHttpEntryPoint, param_.traefikEnableTLS,
            param_.traefikServersTransport);
    } else {
        YRLOG_INFO("Traefik registry disabled");
    }

    subscriptionMgr_ = SubscriptionMgr::Init(param_.nodeID,
        SubscriptionMgrConfig{ .isPartialWatchInstances = param_.isPartialWatchInstances });
    subscriptionMgr_->BindInstanceCtrl(instanceCtrl_);
    subscriptionMgr_->BindLocalSchedSrv(localSchedSrv_);

    // create http server
    httpServer_ = std::make_shared<HttpServer>(LOCAL_SCHEDULER);
    apiRouteRegister_ = std::make_shared<DefaultHealthyRouter>(param_.nodeID);
    metaStoreHealthyObserver_ = std::make_shared<InstanceCtrlMetaStoreHealthyObserver>(instanceCtrl_);
    if (auto registerStatus(httpServer_->RegisterRoute(apiRouteRegister_)); registerStatus != StatusCode::SUCCESS) {
        YRLOG_ERROR("failed to register health check api router.");
    }
    localSchedulingApiRouteRegister_ = std::make_shared<LocalSchedulingApiRouter>();
    localSchedulingApiRouteRegister_->InitUpdateSchedulingStatusHandler(resourceViewMgr_);
    if (auto registerStatus(httpServer_->RegisterRoute(localSchedulingApiRouteRegister_));
        registerStatus != StatusCode::SUCCESS) {
        YRLOG_ERROR("failed to register local scheduling api router.");
    }
    return Status::OK();
}

std::string GetMonitorAddress(const LocalSchedStartParam &param)
{
    // if enabled, return master address; else return etcd address
    return param.metaStoreAddress;
}

Status LocalSchedDriver::Start()
{
    YRLOG_INFO(
        "start local scheduler driver, nodeID: {}, global scheduler address: {}, scheduler policy: {}, "
        "meta store address: {}, driver gateway service enable: {}, enablePrintResourceView: {}",
        param_.nodeID, param_.globalSchedulerAddress, param_.schedulePolicy, param_.metaStoreAddress,
        param_.enableDriver, param_.enablePrintResourceView);

    if (auto status(Create()); status != StatusCode::SUCCESS) {
        return status;
    }
    if (!CreatePosixAndDriverServer()) {
        YRLOG_ERROR("failed to start posix and driver server");
        return Status(StatusCode::FAILED);
    }
    BindInstanceCtrl();
    if (param_.enableTcpTunnel) {
        uint16_t parsedPort = 0;
        const auto parsed = std::from_chars(param_.tcpTunnelPort.data(),
                                            param_.tcpTunnelPort.data() + param_.tcpTunnelPort.size(), parsedPort);
        if (param_.tcpTunnelPort.empty() || parsed.ec != std::errc()
            || parsed.ptr != param_.tcpTunnelPort.data() + param_.tcpTunnelPort.size() || parsedPort == 0) {
            return Status(StatusCode::FAILED, "invalid TCP tunnel port");
        }
        tcpTunnelServer_ = std::make_unique<TcpTunnelServer>(
            TcpTunnelServerConfig{ .listenIP = ExtractIPFromAddress(param_.address),
                                   .listenPort = parsedPort,
                                   .nodeID = param_.nodeID,
                                   .enableTLS = param_.tcpTunnelEnableTLS,
                                   .maxConnections = param_.tcpTunnelMaxConnections,
                                   .rootCert = param_.tcpTunnelRootCert,
                                   .moduleCert = param_.tcpTunnelModuleCert,
                                   .moduleKey = param_.tcpTunnelModuleKey },
            instanceCtrl_->GetInstanceControlView(), instanceCtrl_->GetIdleMgr());
        if (!tcpTunnelServer_->Start()) {
            return Status(StatusCode::FAILED, "failed to start TCP tunnel server");
        }
    }

    snapCtrl_ = SnapCtrl::Create(param_.nodeID);
    snapCtrl_->BindFunctionAgentMgr(funcAgentMgr_);
    snapCtrl_->BindLocalSchedSrv(localSchedSrv_);
    snapCtrl_->BindInstanceCtrl(instanceCtrl_);
    snapCtrl_->BindIdleMgr(instanceCtrl_->GetIdleMgr());
    snapCtrl_->BindClientManager(param_.controlInterfacePosixMgr);
    instanceCtrl_->BindSnapCtrl(snapCtrl_);

    // W2 step-5: watermark-driven pressure monitor (opt-in via enable_pressure_park).
    // Parks fully-idle instances at the high watermark (SnapCtrl signal-18 path,
    // leaveRunning=false + explicit ttl) and FIFO-unparks at the low watermark;
    // external response-ready wakes come in as INSTANCE_WAKE_SNAPSHOT_SIGNAL.
    // W-CPUL: the monitor also hosts the memory/cpu upgrade ladders, so it
    // starts when ANY pressure feature is on; park decisions themselves stay
    // gated on enablePressurePark inside the actor.
    if (param_.enablePressurePark || param_.enableUpgradeLadder || param_.enableCpuUpgradeLadder) {
        PressureMonitorConfig pressureConfig;
        pressureConfig.checkIntervalMs = param_.pressureCheckIntervalMs;
        pressureConfig.highWatermark = param_.pressureHighWatermark;
        pressureConfig.lowWatermark = param_.pressureLowWatermark;
        pressureConfig.sustainSamples = param_.pressureSustainSamples;
        pressureConfig.parkTtlSec = param_.pressureParkTtlSec;
        pressureConfig.maxParked = param_.pressureMaxParked;
        pressureConfig.enablePark = param_.enablePressurePark;
        pressureConfig.upgradeLadder.enabled = param_.enableUpgradeLadder;
        pressureConfig.upgradeLadder.stepBytes = param_.upgradeStepMb * 1024ULL * 1024ULL;
        pressureConfig.upgradeLadder.capBytes = param_.upgradeCapMb * 1024ULL * 1024ULL;
        pressureConfig.upgradeLadder.safetyRatio = param_.upgradeSafetyRatio;
        pressureConfig.upgradeLadder.highRatio = param_.upgradeHighRatio;
        pressureConfig.upgradeLadderCgroupRoot = param_.pressureEventCgroupRoot;
        pressureConfig.cpuUpgradeLadder.enabled = param_.enableCpuUpgradeLadder;
        pressureConfig.cpuUpgradeLadder.stepRatio = param_.cpuUpgradeStepRatio;
        // cpu.max speaks quota usec per period; the period stays at the
        // kernel default 100000, so the cap in cpus converts 1:100000
        pressureConfig.cpuUpgradeLadder.capQuotaUsec =
            static_cast<uint64_t>(param_.cpuUpgradeCapCpus * 100000.0);
        pressureConfig.cpuUpgradeLadder.safetyRatio = param_.cpuUpgradeSafetyRatio;
        pressureConfig.cpuUpgradeLadder.nodeCapacityMilli =
            static_cast<uint64_t>(param_.cpuUpgradeNodeCpus * 1000.0);
        pressureConfig.cpuUpgradeLadderCgroupRoot = param_.pressureEventCgroupRoot;
        pressureMonitorActor_ = std::make_shared<PressureMonitorActor>(PRESSURE_MONITOR_ACTOR_NAME, param_.nodeID,
                                                                       pressureConfig);
        pressureMonitorActor_->BindInstanceControlView(instanceCtrl_->GetInstanceControlView());
        pressureMonitorActor_->BindResourceView(resourceViewMgr_->GetInf(resource_view::ResourceType::PRIMARY));
        pressureMonitorActor_->BindSnapCtrl(snapCtrl_);
        pressureMonitorActor_->BindIdleMgr(instanceCtrl_->GetIdleMgr());
        litebus::Spawn(pressureMonitorActor_);

        // D-2 event-driven watermarks: wake the monitor as soon as a sandbox
        // cgroup crosses memory.high instead of waiting for the polling tick.
        // The watcher thread only enqueues onto the actor (litebus::Async).
        if (param_.enablePressureEvent) {
            pressureEventWatcher_ = std::make_unique<PressureEventWatcher>(
                param_.pressureEventCgroupRoot, param_.pressureEventMinGapMs,
                [monitor(pressureMonitorActor_)](const std::string &poolDir, uint64_t increment) {
                    litebus::Async(monitor->GetAID(), &PressureMonitorActor::NotifyPressureEvent, poolDir, increment);
                });
            (void)pressureEventWatcher_->Start();  // logs + degrades to polling on failure
        }

        // W-CPUL: the CPU ladder does NOT get a watcher instance — cgroupfs
        // cpu.stat emits no inotify (cgroup_file_notify covers memory.events
        // & friends only), so an inotify watch would never fire. The ladder
        // scans pool-dir cpu.stat for nr_throttled growth inside OnSample,
        // making the monitor's sampling cadence the event source.
    }

    gcActor_ = std::make_shared<LocalGcActor>(LOCAL_GC_ACTOR_NAME, param_.nodeID);
    gcActor_->BindInstanceControlView(instanceCtrl_->GetInstanceControlView());
    gcActor_->BindInstanceCtrl(instanceCtrl_);
    litebus::Spawn(gcActor_);

    bool isCoProcess = param_.enableMergeProcess;
    if (isCoProcess) {
        runtimeReconcileActor_ = std::make_shared<RuntimeReconcileActor>(
            RUNTIME_RECONCILE_ACTOR_NAME, param_.nodeID);
        runtimeReconcileActor_->BindInstanceControlView(instanceCtrl_->GetInstanceControlView());
        runtimeReconcileActor_->BindInstanceCtrl(instanceCtrl_);
        runtimeReconcileActor_->BindFunctionAgentMgr(funcAgentMgr_);
        runtimeReconcileActor_->BindResourceView(
            resourceViewMgr_->GetInf(resource_view::ResourceType::PRIMARY));
        litebus::Spawn(runtimeReconcileActor_);
        funcAgentMgr_->SetCoProcessReconcileCallback(
            [reconcileActor = runtimeReconcileActor_](const std::string &funcAgentID) {
                if (reconcileActor) {
                    reconcileActor->TriggerOnce(funcAgentID);
                }
            });
    }

    abnormalProcessor_->BindMetaStoreClient(metaStoreClient_);
    abnormalProcessor_->BindObserver(param_.controlPlaneObserver);
    abnormalProcessor_->BindInstanceCtrl(instanceCtrl_);
    abnormalProcessor_->BindRaiseWrapper(std::make_shared<RaiseWrapper>());
    abnormalProcessor_->BindFunctionAgentMgr(funcAgentMgr_);
    localSchedSrv_->Start(instanceCtrl_, resourceViewMgr_);
    funcAgentMgr_->Start(instanceCtrl_, resourceViewMgr_->GetInf(resource_view::ResourceType::PRIMARY));
    abnormalProcessor_->Start();
    localSchedSrv_->BindFunctionAgentMgr(funcAgentMgr_);
    localSchedSrv_->BindSubscriptionMgr(subscriptionMgr_);
    funcAgentMgr_->BindLocalSchedSrv(localSchedSrv_);

    BundleManagerActorParam bundleManagerActorParam = {
        .actorName = BUNDLE_MGR_ACTOR_NAME,
        .nodeId = param_.nodeID,
        .metaStoreClient = metaStoreClient_
    };
    auto bundleMgrActor = std::make_shared<BundleMgrActor>(bundleManagerActorParam);
    bundleMgr_ = std::make_shared<BundleMgr>(bundleMgrActor);
    bundleMgrActor->BindInstanceCtrl(instanceCtrl_);
    bundleMgrActor->BindLocalSchedSrv(localSchedSrv_);
    bundleMgrActor->BindResourceViewMgr(resourceViewMgr_);
    bundleMgrActor->BindScheduler(instanceCtrl_->GetScheduler());
    litebus::Spawn(bundleMgrActor);
    funcAgentMgr_->BindBundleMgr(bundleMgr_);

    param_.controlPlaneObserver->AttachTenantListener(funcAgentMgr_);
    auto localGroupCtrlActor =
        std::make_shared<LocalGroupCtrlActor>(LOCAL_GROUP_CTRL_ACTOR_NAME, param_.nodeID, metaStoreClient_);
    localGroupCtrl_ = std::make_shared<LocalGroupCtrl>(localGroupCtrlActor);
    localGroupCtrlActor->BindScheduler(instanceCtrl_->GetScheduler());
    localGroupCtrlActor->BindLocalSchedSrv(localSchedSrv_);
    localGroupCtrlActor->BindControlInterfaceClientManager(param_.controlInterfacePosixMgr);
    localGroupCtrlActor->BindInstanceCtrl(instanceCtrl_);
    localGroupCtrlActor->BindResourceView(resourceViewMgr_);
    PosixAPIHandler::BindLocalGroupCtrl(localGroupCtrl_);
    litebus::Spawn(localGroupCtrlActor);
    (void)litebus::Spawn(httpServer_);
    if (param_.dataObjClient != nullptr) {
        param_.dataObjClient->InitDistributedCacheClient();
    }

    auto monitor = MetaStoreMonitorFactory::GetInstance().GetMonitor(GetMonitorAddress(param_));
    if (monitor != nullptr) {
        monitor->RegisterHealthyObserver(funcAgentMgr_);
        monitor->RegisterHealthyObserver(metaStoreHealthyObserver_);
        monitor->RegisterHealthyObserver(localGroupCtrl_);
    } else {
        YRLOG_WARN("failed to get monitor of address {}.", GetMonitorAddress(param_));
    }
    resourceViewMgr_->GetInf(resource_view::ResourceType::PRIMARY)
        ->RegisterUnitDisableFunc([localSchedSrv(localSchedSrv_)](const std::string &agentName) {
            localSchedSrv->DeletePod(agentName,
                                     "disable-agent-" + litebus::uuid_generator::UUID::GetRandomUUID().ToString(),
                                     "agent disabled");
        });

    if (param_.distributedCacheClient != nullptr && param_.distributedCacheClient->IsDsClientEnable()) {
        StartDsHealthyCheck();
    }
    if (param_.runtimeInstanceDebugEnable) {
        StartDebugInstanceInfoMonitor();
    }

    isStarted_ = true;
    return Status::OK();
}

Status LocalSchedDriver::Sync()
{
    auto status = ActorSync(
        { abnormalProcessor_, funcAgentMgr_, instanceCtrl_, localGroupCtrl_, localSchedSrv_, bundleMgr_, snapCtrl_ });
    if (status.IsError()) {
        return status;
    }
    YRLOG_INFO("successful to sync state of local scheduler");
    return Status::OK();
}

Status LocalSchedDriver::Recover()
{
    auto status = ActorRecover(
        { abnormalProcessor_, funcAgentMgr_, instanceCtrl_, localGroupCtrl_, localSchedSrv_, bundleMgr_, snapCtrl_ });
    if (status.IsError()) {
        return status;
    }
    YRLOG_INFO("successful to recover local scheduler");
    return Status::OK();
}

void LocalSchedDriver::ToReady()
{
    ActorReady({ abnormalProcessor_, funcAgentMgr_, instanceCtrl_, localGroupCtrl_, localSchedSrv_, bundleMgr_,
                 resourceViewMgr_->GetInf(resource_view::ResourceType::PRIMARY),
                 resourceViewMgr_->GetInf(resource_view::ResourceType::VIRTUAL), snapCtrl_ });
    // Co-process reconciliation is triggered by FunctionAgentMgrActor::EnableFuncAgent callback
    // after agent registration completes and instances are loaded into InstanceControlView.
}

Status LocalSchedDriver::Stop()
{
    if (pressureEventWatcher_) {
        pressureEventWatcher_->Stop();
        pressureEventWatcher_.reset();
    }
    if (tcpTunnelServer_) {
        tcpTunnelServer_->Stop();
        tcpTunnelServer_.reset();
    }
    if (param_.unRegisterWhileStop && localSchedSrv_ != nullptr && isStarted_) {
        // block to wait instance & agent to be cleared
        (void)localSchedSrv_->GracefulShutdown().Get();
    }
    if (execStreamService_) {
        YRLOG_INFO("Closing ExecStreamService sessions");
        execStreamService_->CloseAllSessions();
    }
    if (sessionGrpcServer_) {
        sessionGrpcServer_.reset();
        YRLOG_INFO("session grpc server stopped");
    }
    execStreamService_.reset();
    if (dsHealthyChecker_) {
        litebus::Terminate(dsHealthyChecker_->GetAID());
    }
    if (httpServer_) {
        litebus::Terminate(httpServer_->GetAID());
    }
    if (gcActor_) {
        litebus::Terminate(gcActor_->GetAID());
    }
    if (runtimeReconcileActor_) {
        litebus::Terminate(runtimeReconcileActor_->GetAID());
    }
    StopActor(
        { abnormalProcessor_, funcAgentMgr_, instanceCtrl_, localGroupCtrl_, localSchedSrv_, bundleMgr_, snapCtrl_ });
    return Status::OK();
}

void LocalSchedDriver::Await()
{
    if (dsHealthyChecker_) {
        litebus::Await(dsHealthyChecker_->GetAID());
    }
    if (httpServer_) {
        litebus::Await(httpServer_->GetAID());
    }
    if (gcActor_) {
        litebus::Await(gcActor_->GetAID());
    }
    if (runtimeReconcileActor_) {
        litebus::Await(runtimeReconcileActor_->GetAID());
    }
    AwaitActor(
        { abnormalProcessor_, funcAgentMgr_, instanceCtrl_, localGroupCtrl_, localSchedSrv_, bundleMgr_, snapCtrl_ });
}

void LocalSchedDriver::BindInstanceCtrl()
{
    instanceCtrl_->SetEnablePrintResourceView(param_.enablePrintResourceView);
    instanceCtrl_->Start(funcAgentMgr_, resourceViewMgr_, param_.controlPlaneObserver,
                         param_.aggregatedStrategy_, param_.maxPriority);
    instanceCtrl_->BindControlInterfaceClientManager(param_.controlInterfacePosixMgr);
    instanceCtrl_->BindMetaStoreClient(metaStoreClient_);
    instanceCtrl_->BindLocalSchedSrv(localSchedSrv_);
    instanceCtrl_->BindInternalIAM(param_.internalIAM);
    instanceCtrl_->BindDataObjClient(param_.dataObjClient);
    instanceCtrl_->BindResourceGroupCtrl(rGroupCtrl_);
    instanceCtrl_->BindSubscriptionMgr(subscriptionMgr_);
}

void LocalSchedDriver::StartDsHealthyCheck()
{
    YRLOG_INFO("enable ds healthy checker, check ds api with interval {} by max {} times", param_.dsHealthCheckInterval,
               param_.maxDsHealthCheckTimes);

    dsHealthyChecker_ = std::make_shared<DsHealthyChecker>(param_.dsHealthCheckInterval, param_.maxDsHealthCheckTimes,
                                                           param_.distributedCacheClient);
    dsHealthyChecker_->SubscribeDsHealthy(
        [localSchedSrv(localSchedSrv_), instanceCtrl(instanceCtrl_)](const bool isHealthy) {
            instanceCtrl->NotifyDsHealthy(isHealthy);  // evict instance on local node
            (void)localSchedSrv->NotifyDsHealthy(isHealthy);
        });
    (void) litebus::Spawn(dsHealthyChecker_);
}

void LocalSchedDriver::StartDebugInstanceInfoMonitor()
{
    YRLOG_INFO("enable debug_instance_info_monitor, check debug_instance_info with interval 3000ms");
    auto infoMonitor = std::make_shared<DebugInstanceInfoMonitor>(funcAgentMgr_, QUERY_DEBUG_INSTANCE_INFO_INTERVAL_MS);
    litebus::Spawn(infoMonitor);
    litebus::Async(infoMonitor->GetAID(), &DebugInstanceInfoMonitor::Start);
}

bool LocalSchedDriver::CreatePosixAndDriverServer()
{
    // Port conflict validation: session port must not conflict with posix port
    if (param_.sessionGrpcPort != "0" && param_.sessionGrpcPort == param_.posixPort) {
        YRLOG_ERROR("Session gRPC port ({}) conflicts with POSIX port ({}), cannot start",
                    param_.sessionGrpcPort, param_.posixPort);
        return false;
    }

    // Create POSIX gRPC server
    functionsystem::grpc::CommonGrpcServerConfig serverConfig;
    serverConfig.ip = param_.ip;
    serverConfig.listenPort = param_.posixPort;
    serverConfig.udsPath = param_.udsPath;
    serverConfig.creds = ::grpc::InsecureServerCredentials();
    if (param_.enableSSL) {
        if (param_.creds == nullptr) {
            return false;
        }
        serverConfig.creds = param_.creds;
    }
    posixGrpcServer_ = std::make_shared<functionsystem::grpc::CommonGrpcServer>(serverConfig);
    if (param_.enableServerMode) {
        param_.posixService->BindInternalIAM(param_.internalIAM);
        posixGrpcServer_->RegisterService(param_.posixService);
    }
    BusServiceParam serviceParam{ .nodeID = param_.nodeID,
                                  .controlPlaneObserver = param_.controlPlaneObserver,
                                  .controlInterfaceClientMgr = param_.controlInterfacePosixMgr,
                                  .instanceCtrl = instanceCtrl_,
                                  .localSchedSrv = localSchedSrv_,
                                  .isEnableServerMode = param_.enableServerMode,
                                  .hostIP = param_.ip };
    std::shared_ptr<BusService> busService = std::make_shared<BusService>(std::move(serviceParam));
    posixGrpcServer_->RegisterService(busService);
    FrontendProxyServiceBindings bindings;
    bindings.enableCreateDispatch = true;
    bindings.scheduler =
        [instanceCtrl(instanceCtrl_)](
            const std::shared_ptr<messages::ScheduleRequest> &scheduleReq,
            const std::shared_ptr<litebus::Promise<messages::ScheduleResponse>> &runtimePromise,
            FrontendProxyReadyCallback callback) {
            if (instanceCtrl == nullptr) {
                messages::ScheduleResponse response;
                response.set_code(common::ERR_LOCAL_SCHEDULER_ABNORMAL);
                response.set_message("instance control is nullptr in local scheduler");
                return litebus::Future<messages::ScheduleResponse>(response);
            }
            scheduleReq->mutable_instance()->set_parentfunctionproxyaid(instanceCtrl->GetActorAID());
            return instanceCtrl->ScheduleFrontendAndWaitReady(scheduleReq, runtimePromise, std::move(callback));
        };
    bindings.readyUnregister =
        [instanceCtrl(instanceCtrl_)](const std::string &requestID, const std::string &reason) {
            if (instanceCtrl != nullptr) {
                instanceCtrl->UnregisterFrontendReadyWait(requestID, reason);
            }
        };
    bindings.enableKillDispatch = true;
    bindings.killInvoker =
        [instanceCtrl(instanceCtrl_)](const std::string &caller, const std::string &tenantID,
                                     const std::shared_ptr<KillRequest> &killReq) {
            if (instanceCtrl == nullptr) {
                KillResponse response;
                response.set_code(common::ERR_LOCAL_SCHEDULER_ABNORMAL);
                response.set_message("instance control is nullptr in local scheduler");
                return litebus::Future<KillResponse>(response);
            }
            (void)caller;
            return instanceCtrl->KillFrontend(tenantID, killReq);
        };
    bindings.killCleanupProbe =
        [instanceCtrl(instanceCtrl_)](const std::string &requestID, const std::string &instanceID) {
            if (instanceCtrl == nullptr) {
                FrontendKillCleanupSnapshot snapshot;
                return litebus::Future<FrontendKillCleanupSnapshot>(snapshot);
            }
            return instanceCtrl->ProbeFrontendKillCleanup(requestID, instanceID);
        };
    auto frontendServiceParam = BuildFrontendProxyServiceParam(param_.nodeID, bindings);
    frontendServiceParam.endpointAddress = param_.ip + ":" + param_.posixPort;
    frontendServiceParam.requireAuthenticatedPeer = param_.enableSSL;
    frontendServiceParam.invokeTenantAuthorizer =
        [instanceView(instanceCtrl_->GetInstanceControlView())](const std::string &tenantID,
                                                                const std::string &instanceID) {
            if (tenantID.empty() || instanceView == nullptr) {
                return false;
            }
            auto stateMachine = instanceView->GetInstance(instanceID);
            return stateMachine != nullptr && stateMachine->GetInstanceInfo().tenantid() == tenantID;
        };
    // Lifecycle create/kill use reviewed ready dispatcher seams. Legacy stream dispatchers remain
    // disallowed.
    std::shared_ptr<FrontendProxyService> frontendProxyService =
        std::make_shared<FrontendProxyService>(std::move(frontendServiceParam));
    posixGrpcServer_->RegisterService(frontendProxyService);
    frontendProxyServiceRegistered_ = true;
    YRLOG_INFO("FrontendProxyService registered on existing posix port {}", param_.posixPort);

    // Create ExecStreamService instance
    execStreamService_ = std::make_shared<ExecStreamService>(instanceCtrl_->GetIdleMgr());

    // Register ExecStreamService based on session server configuration
    if (param_.sessionGrpcPort != "0") {
        // Create independent session gRPC server for ExecStreamService
        // Use same IP as LiteBus for external connectivity
        functionsystem::grpc::CommonGrpcServerConfig sessionConfig;
        sessionConfig.ip =
            ExtractIPFromAddress(param_.address);  // Extract IP from LiteBus address to ensure external connectivity
        sessionConfig.listenPort = param_.sessionGrpcPort;
        sessionConfig.creds = ::grpc::InsecureServerCredentials();
        if (param_.enableSSL) {
            sessionConfig.creds = param_.creds;
        }

        sessionGrpcServer_ = std::make_shared<functionsystem::grpc::CommonGrpcServer>(sessionConfig);
        sessionGrpcServer_->RegisterService(execStreamService_);
        sessionGrpcServer_->Start();

        if (!sessionGrpcServer_->WaitServerReady()) {
            YRLOG_ERROR("failed to start session grpc server on port {}", param_.sessionGrpcPort);
            return false;
        }
        YRLOG_INFO("Session gRPC server started on port {}, ExecStreamService listening for connections",
                   param_.sessionGrpcPort);
    } else {
        // Session server disabled, register ExecStreamService on posix server for backward compatibility
        posixGrpcServer_->RegisterService(execStreamService_);
        YRLOG_INFO("ExecStreamService registered on posix port {} (session server disabled)",
                   param_.posixPort);
    }

    // Start POSIX gRPC server
    posixGrpcServer_->Start();

    if (!posixGrpcServer_->WaitServerReady()) {
        frontendProxyServiceRegistered_ = false;
        YRLOG_ERROR("failed to start posix grpc server.");
        return false;
    }
    YRLOG_INFO("POSIX gRPC server started on port {}", param_.posixPort);
    return true;
}
}  // namespace functionsystem::local_scheduler
