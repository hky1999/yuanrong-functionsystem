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

#ifndef RUNTIME_MANAGER_CONFIG_FLAG_H
#define RUNTIME_MANAGER_CONFIG_FLAG_H

#include <async/flag_parser_impl.hpp>
#include <async/option.hpp>

#include "common/common_flags/common_flags.h"

namespace functionsystem::runtime_manager {

class Flags : public CommonFlags {
public:
    Flags();

    ~Flags() override = default;

    void AddOomFlags();

    void AddInstanceMetricsFlags();

    void AddDiskUsageMonitorFlags();

    const std::string &GetRuntimePath() const
    {
        return runtimePath_;
    }

    const std::string &GetSnuserLibDir() const
    {
        return snuserLibDir_;
    }

    const std::string &GetRuntimeLogPath() const
    {
        return runtimeLogsPath_;
    }

    int GetRuntimeMaxLogSize() const
    {
        return runtimeMaxLogSize_;
    }

    int GetRuntimeMaxLogFileNum() const
    {
        return runtimeMaxLogFileNum_;
    }

    const bool &GetSetCmdCred() const
    {
        return setCmdCred_;
    }

    const std::string &GetNodeID() const
    {
        return nodeId_;
    }

    const std::string &GetLogConfig() const
    {
        return logConfig_;
    }

    const std::string &GetIP() const
    {
        return ip_;
    }

    const std::string &GetHostIP() const
    {
        return hostIP_;
    }

    const std::string &GetProxyIP() const
    {
        if (proxyIP_.empty()) {
            return hostIP_;
        }
        return proxyIP_;
    }

    const std::string &GetPort() const
    {
        return port_;
    }

    const std::string &GetAgentAddress() const
    {
        return agentAddress_;
    }

    const int &GetRuntimeInitialPort() const
    {
        return initialPort_;
    }

    const int &GetPortNum() const
    {
        return portNum_;
    }

    [[maybe_unused]] const std::string GetMetricsCollectorType() const
    {
        return metricsCollectorType_;
    }

    [[maybe_unused]] const double &GetProcMetricsCPU() const
    {
        return procMetricsCPU_;
    }

    [[maybe_unused]] const double &GetProcMetricsMemory() const
    {
        return procMetricsMemory_;
    }

    [[maybe_unused]] const std::string GetRuntimeLdLibraryPath() const
    {
        return runtimeLdLibraryPath_;
    }

    const std::string &GetRuntimePrestartConfig() const
    {
        return runtimePrestartConfig_;
    }

    const std::string &GetRuntimeDefaultConfig() const
    {
        return runtimeDefaultConfig_;
    }

    const std::string GetRuntimeStdLogDir() const
    {
        return runtimeStdLogDir_;
    }

    const std::string GetRuntimeLogLevel() const
    {
        return runtimeLogLevel_;
    }

    const std::string GetPythonDependencyPath() const
    {
        return pythonDependencyPath_;
    }

    const std::string GetJavaSystemProperty() const
    {
        return javaSystemProperty_;
    }

    const std::string GetPythonLogConfigPath() const
    {
        return pythonLogConfigPath_;
    }

    const std::string GetDataSystemPort() const
    {
        return dataSystemPort_;
    }

    const std::string GetDriverServerPort() const
    {
        return driverServerPort_;
    }

    const std::string GetDiskUsageMonitorPath() const
    {
        return diskUsageMonitorPath_;
    }

    int GetDiskUsageLimit() const
    {
        return diskUsageLimit_;
    }

    int GetTmpDirSizeLimit() const
    {
        return tmpDirSizeLimit_;
    }

    int GetSnuserDirSizeLimit() const
    {
        return snuserDirSizeLimit_;
    }

    int GetDiskUsageMonitorDuration() const
    {
        return diskUsageMonitorDuration_;
    }

    const std::string GetRuntimeConfigPath() const
    {
        return runtimeConfigPath_;
    }

    const std::string GetJavaSystemLibraryPath() const
    {
        return javaSystemLibraryPath_;
    }

    const std::string GetProxyGrpcServerPort() const
    {
        return proxyGrpcServerPort_;
    }

    int GetRuntimeUID() const
    {
        return runtimeUID_;
    }

    int GetRuntimeGID() const
    {
        return runtimeGID_;
    }

    int GetVirtualEnvIdleTimeLimit() const
    {
        return virtualEnvIdleTimeLimit_;
    }

    const std::string GetNpuCollectionMode() const
    {
        return npuCollectionMode_;
    }

    bool GetGpuCollectionEnable() const
    {
        return gpuCollectionEnable_;
    }

    bool GetNumaCollectionEnable() const
    {
        return numaCollectionEnable_;
    }

    bool GetIsProtoMsgToRuntime() const
    {
        return isProtoMsgToRuntime_;
    }

    bool GetMassifEnable() const
    {
        return massifEnable_;
    }

    bool GetEnableDisConvCallStack() const
    {
        return enableDisConvCallStack_;
    }

    bool GetInheritEnv() const
    {
        return inheritEnv_;
    }

    const std::string GetCustomResources() const
    {
        return customResources_;
    }

    bool GetLogExpirationEnable() const
    {
        return logExpirationEnable_;
    }

    bool GetLogReuseEnable() const
    {
        return logReuseEnable_;
    }

    bool GetSeparetedRedirectRuntimeStd() const
    {
        return separatedRedirectRuntimeStd_;
    }

    int GetLogExpirationCleanupInterval() const
    {
        return logExpirationCleanupInterval_;
    }

    int GetLogExpirationTimeThreshold() const
    {
        return logExpirationTimeThreshold_;
    }

    int GetLogExpirationMaxFileCount() const
    {
        return logExpirationMaxFileCount_;
    }

    bool GetRuntimeDirectConnectionEnable() const
    {
        return runtimeDirectConnectionEnable_;
    }
    int32_t GetUserLogAutoFlushIntervalMs() const
    {
        return userLogAutoFlushIntervalMs_;
    }

    int32_t GetUserLogBufferFlushThreshold() const
    {
        return userLogBufferFlushThreshold_;
    }

    unsigned long GetUserLogRollingSizeLimitMb() const
    {
        return userLogRollingSizeLimitMb_;
    }

    unsigned long GetUserLogRollingFileCountLimit() const
    {
        return userLogRollingFileCountLimit_;
    }

    bool GetCleanStreamProducerEnable() const
    {
        return cleanStreamProducerEnable_;
    }

    int GetMemoryDetectionInterval() const
    {
        return memoryDetectionInterval_;
    }

    const std::string &GetInstanceMemorySource() const
    {
        return instanceMemorySource_;
    }

    const std::string &GetInstanceCgroupRoot() const
    {
        return instanceCgroupRoot_;
    }

    bool GetOomKillEnable() const
    {
        return oomKillEnable_;
    }

    int GetOomKillControlLimit() const
    {
        return oomKillControlLimit_;
    }

    int GetOomConsecutiveDetectionCount() const
    {
        return oomConsecutiveDetectionCount_;
    }

    const std::string &GetRuntimeHomeDir() const
    {
        return runtimeHomeDir_;
    }

    const std::string &GetCheckpointDir() const
    {
        return checkpointDir_;
    }

    int32_t GetCkptDefaultTtlSec() const
    {
        return ckptDefaultTtlSec_;
    }

    const std::string &GetNodeJsEntryPath() const
    {
        return nodeJsEntryPath_;
    }

    const std::string &GetResourceLabelPath() const
    {
        return resourceLabelPath_;
    }

    const std::string &GetNpuDeviceInfoPath() const
    {
        return npuDeviceInfoPath_;
    }

    uint32_t GetRuntimeDsConnectTimeout() const
    {
        return runtimeDsConnectTimeout_;
    }

    uint32_t GetKillProcessTimeoutSeconds() const
    {
        return killProcessTimeoutSeconds_;
    }

    double GetOverheadCPU() const
    {
        return overheadCPU_;
    }

    double GetOverheadMemory() const
    {
        return overheadMemory_;
    }

    bool GetDiskUsageMonitorNotifyFailureEnable() const
    {
        return diskUsageMonitorNotifyFailureEnable_;
    }

    bool GetRuntimeInstanceDebugEnable() const
    {
        return runtimeInstanceDebugEnable_;
    }

    std::string GetUserLogExportMode() const
    {
        return userLogExportMode_;
    }

    std::string GetDiskResources() const
    {
        return diskResources_;
    }

protected:
    void AddConfigFlags();

    std::string pythonDependencyPath_;
    std::string javaSystemProperty_;
    std::string javaSystemLibraryPath_ = "/home/snuser/runtime/java/lib";
    bool setCmdCred_ = true;
    std::string runtimePath_ = "/home/snuser";
    std::string snuserLibDir_ = "/home/snuser/lib";
    std::string runtimeLogsPath_ = "/home/snuser";
    std::string runtimeStdLogDir_ = "instances";
    int runtimeMaxLogSize_;
    int runtimeMaxLogFileNum_;
    std::string pythonLogConfigPath_;
    std::string runtimeLdLibraryPath_;
    std::string runtimePrestartConfig_;
    std::string runtimeDefaultConfig_;
    std::string runtimeLogLevel_;
    std::string logConfig_;
    std::string nodeId_;
    std::string ip_;
    std::string hostIP_;
    std::string proxyIP_;
    std::string port_;
    std::string agentAddress_;
    std::string dataSystemPort_;
    std::string driverServerPort_;
    std::string runtimeConfigPath_;
    std::string proxyGrpcServerPort_;
    int initialPort_ = 0;
    int portNum_ = 0;
    std::string metricsCollectorType_;
    double procMetricsCPU_ = 0.0;
    double procMetricsMemory_ = 0.0;
    std::string diskUsageMonitorPath_;
    int diskUsageLimit_ = 0;
    int diskUsageMonitorDuration_ = 0;
    int tmpDirSizeLimit_ = 0;
    int snuserDirSizeLimit_ = 0;
    int runtimeUID_ = 1003;
    int runtimeGID_ = 1003;
    int virtualEnvIdleTimeLimit_ = -1; // ms
    std::string npuCollectionMode_;
    bool gpuCollectionEnable_ = false;
    bool numaCollectionEnable_ = false;
    bool isProtoMsgToRuntime_ = false;
    bool massifEnable_ = false;
    bool inheritEnv_ = false;
    bool logExpirationEnable_ = false;
    bool logReuseEnable_ = false;
    int logExpirationCleanupInterval_ = 0;
    int logExpirationTimeThreshold_ = 0;
    int logExpirationMaxFileCount_ = 0;
    std::string customResources_;
    bool separatedRedirectRuntimeStd_ = false;
    bool runtimeDirectConnectionEnable_ = false;
    bool cleanStreamProducerEnable_ = true;
    int memoryDetectionInterval_ = 1000; // ms
    std::string instanceMemorySource_ = "vmrss"; // vmrss | cgroup | auto
    std::string instanceCgroupRoot_ = "/sys/fs/cgroup";
    bool oomKillEnable_ = false;
    int oomKillControlLimit_ = 0; // MB
    int oomConsecutiveDetectionCount_ = 3;
    std::string runtimeHomeDir_;
    std::string checkpointDir_;
    int32_t ckptDefaultTtlSec_ = 1800;
    std::string nodeJsEntryPath_;
    std::string resourceLabelPath_;
    std::string npuDeviceInfoPath_;
    uint32_t runtimeDsConnectTimeout_;
    uint32_t killProcessTimeoutSeconds_;
    double overheadCPU_ = 0.0;
    double overheadMemory_ = 0.0;
    bool diskUsageMonitorNotifyFailureEnable_{ false };
    bool runtimeInstanceDebugEnable_{ false };
    std::string userLogExportMode_;
    std::string diskResources_;
    bool enableDisConvCallStack_ = false;
    int32_t userLogAutoFlushIntervalMs_;
    int32_t userLogBufferFlushThreshold_;
    unsigned long userLogRollingSizeLimitMb_;
    unsigned long userLogRollingFileCountLimit_;
};
}  // namespace functionsystem::runtime_manager

#endif  // RUNTIME_MANAGER_CONFIG_FLAG_H
