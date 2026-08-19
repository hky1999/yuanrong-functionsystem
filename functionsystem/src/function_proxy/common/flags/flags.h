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

#ifndef FUNCTION_PROXY_COMMON_FLAGS_H
#define FUNCTION_PROXY_COMMON_FLAGS_H

#include <cstdint>

#include "common/common_flags/common_flags.h"

namespace functionsystem::function_proxy {

// state store type
const std::string DATA_SYSTEM_STORE = "datasystem";
const std::string DISABLE_STORE = "disable";

class Flags : public CommonFlags {
public:
    Flags();
    ~Flags() override;

    const std::string &GetLogConfig() const
    {
        return logConfig_;
    }

    const std::string &GetNodeID() const
    {
        return nodeId_;
    }

    const std::string &GetAddress() const
    {
        return address_;
    }

    const std::string &GetIP() const
    {
        return ip_;
    }

    const std::string &GetGrpcListenPort() const
    {
        return grpcListenPort_;
    }

    const std::string &GetSessionGrpcPort() const
    {
        return sessionGrpcPort_;
    }

    const std::string &GetMetaStoreAddress() const
    {
        return metaStoreAddress_;
    }

    const std::string &GetIAMMetaStoreAddress() const
    {
        return iamMetastoreAddress_;
    }

    const std::string &GetGlobalSchedulerAddress() const
    {
        return globalSchedulerAddress_;
    }

    const std::string &GetCacheStorageHost() const
    {
        return cacheStorageHost_;
    }

    const int32_t &GetCacheStoragePort() const
    {
        return cacheStoragePort_;
    }

    const bool &GetCacheStorageAuthEnable() const
    {
        return cacheStorageAuthEnable_;
    }

    const std::string &GetCacheStorageAuthType() const
    {
        return cacheStorageAuthType_;
    }

    const std::string &GetCacheStorageAuthAK() const
    {
        return cacheStorageAuthAK_;
    }

    const std::string &GetCacheStorageAuthSK() const
    {
        return cacheStorageAuthSK_;
    }

    const std::string &GetCacheStorageInfoPrefix() const
    {
        return cacheStorageInfoPrefix_;
    }

    const std::string &GetSchedulePolicy() const
    {
        return schedulePolicy_;
    }

    uint32_t GetFuncAgentMgrRetryTimes() const
    {
        return funcAgentMgrRetryTimes_;
    }

    uint32_t GetFuncAgentMgrRetryCycleMs() const
    {
        return funcAgentMgrRetryCycleMs_;
    }

    uint32_t GetServiceRegisterTimes() const
    {
        return serviceRegisterTimes_;
    }

    uint32_t GetServiceRegisterCycleMs() const
    {
        return serviceRegisterCycleMs_;
    }

    uint32_t GetServiceUpdateResourceCycleMs() const
    {
        return updateResourceCycle_;
    }

    const std::string GetServicesPath() const
    {
        return servicesPath_;
    }

    const std::string GetLibPath() const
    {
        return libPath_;
    }

    const std::string GetFunctionMetaPath() const
    {
        return functionMetaPath_;
    }

    uint32_t GetRuntimeMaxHeartbeatTimeoutTimes() const
    {
        return runtimeMaxHeartbeatTimeoutTimes_;
    }

    uint32_t GetRuntimeHeartbeatTimeoutMS() const
    {
        return runtimeHeartbeatTimeoutMS_;
    }

    const std::string GetRuntimeHeartbeatEnable() const
    {
        return runtimeHeartbeatEnable_;
    }

    uint32_t GetRuntimeInitCallTimeoutSeconds() const
    {
        return runtimeInitCallTimeoutSeconds_;
    }

    uint32_t GetRuntimeConnTimeoutSeconds() const
    {
        return runtimeConnTimeoutSeconds_;
    }

    uint32_t GetRuntimeShutdownTimeoutSeconds() const
    {
        return runtimeShutdownTimeoutSeconds_;
    }

    int32_t GetMaxGrpcSize() const
    {
        return maxGrpcSize_;
    }

    bool GetEnableDriver() const
    {
        return enableDriver_;
    }

    bool GetRuntimeRecoverEnable() const
    {
        return runtimeRecoverEnable_;
    }

    bool GetIsPseudoDataPlane() const
    {
        return isPseudoDataPlane_;
    }

    const std::string &GetStateStorageType()
    {
        static bool updateFlags = false;
        if (!updateFlags) {
            if (GetRuntimeRecoverEnable()) {
                stateStorageType_ = DATA_SYSTEM_STORE;
            }
            updateFlags = true;
        }
        return stateStorageType_;
    }

    const std::string &GetElectionMode() const
    {
        return electionMode_;
    }

    bool GetIsEnableIAM() const
    {
        return enableIAM_;
    }

    const std::string GetIAMBasePath() const
    {
        return iamBasePath_;
    }

    const std::string GetIamPolicyFile() const
    {
        return iamPolicyFile_;
    }

    bool GetIsEnableServerMode() const
    {
        return enableServerMode_;
    }

    const float &GetLowMemoryThreshold() const
    {
        return lowMemoryThreshold_;
    }

    const float &GetHighMemoryThreshold() const
    {
        return highMemoryThreshold_;
    }

    const uint64_t &GetMessageSizeThreshold() const
    {
        return messageSizeThreshold_;
    }

    bool GetInvokeLimitationEnable() const
    {
        return invokeLimitationEnable_;
    }

    bool GetCreateLimitationEnable() const
    {
        return createLimitationEnable_;
    }

    const uint32_t &GetTokenBucketCapacity() const
    {
        return tokenBucketCapacity_;
    }

    const std::string &GetDsHealthyPath() const
    {
        return dsHealthCheckPath_;
    }

    const uint64_t &GetDsHealthyCheckInterval() const
    {
        return dsHealthCheckInterval_;
    }

    const uint64_t &GetMaxDsHealthCheckTimes() const
    {
        return maxDsHealthCheckTimes_;
    }

    bool GetEnablePrintResourceView() const
    {
        return enablePrintResourceView_;
    }

    int32_t GetServiceTTL() const
    {
        return serviceTTL_;
    }

    std::string GetSchedulePlugins() const
    {
        return schedulePlugins_;
    }

    double GetUsageAwareSafety() const
    {
        return usageAwareSafety_;
    }

    double GetUsageAwareFloorMb() const
    {
        return usageAwareFloorMb_;
    }

    bool GetEnableIpv4TenantIsolation() const
    {
        return enableIpv4TenantIsolation_;
    }

    bool GetRuntimeDsAuthEnable() const
    {
        return runtimeDsAuthEnable_;
    }
    bool GetRuntimeDsEncryptEnable() const
    {
        return runtimeDsEncryptEnable_;
    }
    const std::string GetRuntimeDsClientPublicKey() const
    {
        return runtimeDsClientPublicKey_;
    }

    const std::string GetRuntimeDsClientPrivateKey() const
    {
        return runtimeDsClientPrivateKey_;
    }

    const std::string GetRuntimeDsServerPublicKey() const
    {
        return runtimeDsServerPublicKey_;
    }

    const std::string GetCurveKeyPath() const
    {
        return curveKeyPath_;
    }

    bool GetEnableTenantAffinity() const
    {
        return enableTenantAffinity_;
    }

    int32_t GetTenantPodReuseTimeWindow() const
    {
        return tenantPodReuseTimeWindow_;
    }

    bool GetEnablePerf() const
    {
        return enablePerf_;
    }

    bool GetEnableDirectRouting() const
    {
        return enableDirectRouting_;
    }

    bool GetForceLowReliabilityInstance() const
    {
        return forceLowReliabilityInstance_;
    }

    bool GetEnableFrontendProxyService() const
    {
        return enableFrontendProxyService_;
    }

    const std::string &GetK8sBasePath() const
    {
        return basePath_;
    }

    const std::string &GetK8sNamespace() const
    {
        return k8sNamespace_;
    }

    uint32_t GetElectKeepAliveInterval() const
    {
        return electKeepAliveInterval_;
    }

    std::string GetRedisConfPath() const
    {
        return redisConfPath_;
    }

    bool GetEnableMetaStore() const
    {
        return enableMetaStore_;
    }

    std::string GetMetaStoreMode() const
    {
        return metaStoreMode_;
    }

    uint32_t GetExpirationSeconds() const
    {
        return expirationSeconds;
    }

    bool GetForwardCompatibility() const
    {
        return forwardCompatibility_;
    }

    bool IsPartialWatchInstances() const
    {
        return isPartialWatchInstances_;
    }

    bool IsRuntimeInstanceDebugEnable() const
    {
        return runtimeInstanceDebugEnable_;
    }

    bool EnableForceDeletePod() const
    {
        return diskUsageMonitorForceDeletePodEnable_;
    }

    bool UnRegisterWhileStop() const
    {
        return unRegisterWhileStop_;
    }

    const bool &GetEnableMergeProcess() const
    {
        return enableMergeProcess_;
    }

    std::string GetDPosixUdsPath() const
    {
        return dPosixUdsPath_;
    }

    bool GetEnableTraefikRegistry() const
    {
        return enableTraefikRegistry_;
    }

    const std::string& GetTraefikEtcdPrefix() const
    {
        return traefikEtcdPrefix_;
    }

    int32_t GetTraefikLeaseTTL() const
    {
        return traefikLeaseTTL_;
    }

    const std::string& GetTraefikHttpEntryPoint() const
    {
        return traefikHttpEntryPoint_;
    }

    bool GetTraefikEnableTLS() const
    {
        return traefikEnableTLS_;
    }

    const std::string& GetTraefikServersTransport() const
    {
        return traefikServersTransport_;
    }

    bool GetEnableTcpTunnel() const
    {
        return enableTcpTunnel_;
    }

    const std::string &GetTcpTunnelPort() const
    {
        return tcpTunnelPort_;
    }

    uint32_t GetTcpTunnelMaxConnections() const
    {
        return tcpTunnelMaxConnections_;
    }

protected:
    void AddRuntimeFlags();
    void AddDSFlags();
    void AddGrpcServerFlags();
    void AddIAMFlags();
    void AddIsolationFlags();
    void AddBusProxyInvokeLimitFlags();
    void AddBusProxyCreatRateLimitFlags();
    void AddElectionFlags();

    std::string electionMode_;
    std::string logConfig_;
    std::string nodeId_;
    std::string address_;
    std::string ip_;
    std::string grpcListenPort_;
    std::string sessionGrpcPort_;
    std::string schedulePolicy_;
    std::string metaStoreAddress_;
    std::string iamMetastoreAddress_;
    std::string globalSchedulerAddress_;
    std::string servicesPath_;
    std::string libPath_;
    std::string functionMetaPath_;
    std::string cacheStorageHost_;
    std::string stateStorageType_;
    int32_t cacheStoragePort_;
    bool cacheStorageAuthEnable_;
    std::string cacheStorageAuthType_;
    std::string cacheStorageAuthAK_;
    std::string cacheStorageAuthSK_;
    std::string cacheStorageInfoPrefix_;
    std::string runtimeHeartbeatEnable_;
    bool runtimeRecoverEnable_;
    uint32_t funcAgentMgrRetryTimes_;
    uint32_t funcAgentMgrRetryCycleMs_;
    uint32_t serviceRegisterTimes_;
    uint32_t serviceRegisterCycleMs_;
    uint32_t updateResourceCycle_;
    uint32_t runtimeMaxHeartbeatTimeoutTimes_;
    uint32_t runtimeHeartbeatTimeoutMS_;
    uint32_t runtimeInitCallTimeoutSeconds_;
    uint32_t runtimeConnTimeoutSeconds_;
    uint32_t runtimeShutdownTimeoutSeconds_;
    int32_t maxGrpcSize_;
    bool enableDriver_;
    bool isPseudoDataPlane_{ false };
    float lowMemoryThreshold_;
    float highMemoryThreshold_;
    uint64_t messageSizeThreshold_;
    bool invokeLimitationEnable_;
    bool createLimitationEnable_;
    uint32_t tokenBucketCapacity_;
    std::string dsHealthCheckPath_;
    uint64_t dsHealthCheckInterval_;
    uint64_t maxDsHealthCheckTimes_;
    bool enableIAM_;
    std::string iamBasePath_;
    std::string iamPolicyFile_;
    std::string iamCredentialType_;
    bool enableServerMode_;
    bool enablePrintResourceView_;
    int32_t serviceTTL_;
    std::string schedulePlugins_;
    double usageAwareSafety_ = 0.9;
    double usageAwareFloorMb_ = 2048.0;
    bool enableIpv4TenantIsolation_;
    bool runtimeDsAuthEnable_;
    bool runtimeDsEncryptEnable_;
    std::string curveKeyPath_;
    std::string runtimeDsClientPublicKey_;
    std::string runtimeDsClientPrivateKey_;
    std::string runtimeDsServerPublicKey_;
    std::string clusterId_;
    bool enablePerf_;
    bool enableTenantAffinity_;
    int32_t tenantPodReuseTimeWindow_;
    std::string k8sNamespace_;
    std::string basePath_;
    uint32_t electKeepAliveInterval_;
    std::string redisConfPath_;
    bool enableMetaStore_;
    std::string metaStoreMode_;
    bool metaStoreNeedExplore_;
    uint32_t expirationSeconds;
    bool forwardCompatibility_;
    bool isPartialWatchInstances_;
    bool runtimeInstanceDebugEnable_{ false };  // deploy in docker only use false, in process use false or true
    bool diskUsageMonitorForceDeletePodEnable_{ false };
    bool unRegisterWhileStop_{ false };
    bool enableMergeProcess_{ false };
    bool enableDirectRouting_{ false };
    bool forceLowReliabilityInstance_{ false };
    bool enableFrontendProxyService_{ false };
    std::string dPosixUdsPath_;
    bool enableTraefikRegistry_{ false };
    std::string traefikEtcdPrefix_ = "traefik";
    int32_t traefikLeaseTTL_ = 300000;
    std::string traefikHttpEntryPoint_ = "websecure";
    bool traefikEnableTLS_ = true;
    std::string traefikServersTransport_ = "yr-backend-tls@file";
    bool enableTcpTunnel_{ false };
    std::string tcpTunnelPort_{ "22775" };
    uint32_t tcpTunnelMaxConnections_{ 1024 };
};

}  // namespace functionsystem::function_proxy

#endif  // FUNCTION_PROXY_COMMON_FLAGS_H
