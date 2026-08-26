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

#include "executor.h"

#include <sys/stat.h>

#include "async/async.hpp"
#include "common/constants/constants.h"
#include "common/logs/logging.h"
#include "common/utils/files.h"

namespace functionsystem::runtime_manager {
const std::vector<std::string> logLevels = { "INFO", "DEBUG", "WARN", "ERROR" };
const std::vector<std::string> DEFAULT_JVM_ARGS = { "-XX:InitialRAMPercentage=35.0",
                                                    "-XX:+UseConcMarkSweepGC",
                                                    "-XX:+CMSClassUnloadingEnabled",
                                                    "-XX:+CMSIncrementalMode",
                                                    "-XX:+CMSScavengeBeforeRemark",
                                                    "-XX:+UseCMSInitiatingOccupancyOnly",
                                                    "-XX:CMSInitiatingOccupancyFraction=70",
                                                    "-XX:CMSFullGCsBeforeCompaction=5",
                                                    "-XX:MaxGCPauseMillis=200",
                                                    "-XX:+ExplicitGCInvokesConcurrent",
                                                    "-XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses" };
const std::vector<std::string> DEFAULT_JVM_ARGS_FOR_JAVA11 = { "-XX:MaxRAMPercentage=80.0", "-XX:+UseG1GC",
                                                               "-XX:+TieredCompilation",
                                                               "--add-opens=java.base/java.nio=ALL-UNNAMED" };
const std::vector<std::string> COMMON_JVM_ARGS_ABOVE_17 = { "-XX:+UseZGC",
                                                            "-XX:+AlwaysPreTouch",
                                                            "-XX:+UseCountedLoopSafepoints",
                                                            "-XX:+TieredCompilation",
                                                            "--add-opens=java.base/java.util=ALL-UNNAMED",
                                                            "--add-opens=java.base/java.lang=ALL-UNNAMED",
                                                            "--add-opens=java.base/java.net=ALL-UNNAMED",
                                                            "--add-opens=java.base/java.io=ALL-UNNAMED",
                                                            "--add-opens=java.base/java.math=ALL-UNNAMED",
                                                            "--add-opens=java.base/java.time=ALL-UNNAMED",
                                                            "--add-opens=java.base/java.text=ALL-UNNAMED",
                                                            "--add-opens=java.base/java.nio=ALL-UNNAMED",
                                                            "--enable-preview" };
const std::vector<std::string> DEFAULT_JVM_ARGS_FOR_JAVA17 = [] {
    std::vector<std::string> args;
    args.reserve(COMMON_JVM_ARGS_ABOVE_17.size());
    args.insert(args.end(), COMMON_JVM_ARGS_ABOVE_17.begin(), COMMON_JVM_ARGS_ABOVE_17.end());
    return args;
}();
const std::vector<std::string> DEFAULT_JVM_ARGS_FOR_JAVA21 = [] {
    std::vector<std::string> args;
    args.emplace_back("-XX:+ZGenerational");
    args.reserve(COMMON_JVM_ARGS_ABOVE_17.size());
    args.insert(args.end(), COMMON_JVM_ARGS_ABOVE_17.begin(), COMMON_JVM_ARGS_ABOVE_17.end());
    return args;
}();
const std::set<std::string> REJECTED_JVM_ARGS = { "-XX:+DisableExplicitGC" };
const std::string PRESTART_COUNT_STR = "prestartCount";
const std::string CUSTOM_ARGS_STR = "customArgs";
const int MIN_PRESTART_COUNT = 0;
const int MAX_PRESTART_COUNT = 100;
const std::string JVM_REGEX_PATTERN =
    "^-(?:X{0,2}[\\w-]+):?"                 // Parameter prefix and colon
    "([+-])?"                               // Plus or minus sign (capture group)
    "[\\w/._%-]*"                           // Option name (allowed character set)
    "(?:"                                   // Start key-value pair section
    "="                                     // Equal sign delimiter
    "(?:"                                   // Value or key-value pair list
    "(?:[^=,]+=.*?)(?:,+(?:[^=,]+=.*?)?)*"  // Key-value pair list (key=value,key2=value2) compatible with consecutive
                                            // commas
    "|"                                     // Or
    "[^,]+"                                 // Standalone value (no equal sign)
    ")"
    ")?$";
const std::regex JVM_ARGS_REGEX(JVM_REGEX_PATTERN);
const double JVM_MEMORY_THRESHOLD = 0.8;

Executor::Executor(const std::string &name) : ActorBase(name)
{
}

std::string Executor::ShellQuote(const std::string &token)
{
    // POSIX single-quote escaping: wrap the token in '...', and replace every literal ' in it
    // with the sequence '\'' (close quote, escaped quote, reopen quote). The result is passed
    // through sh -c with zero metacharacter interpretation, so argv tokens and redirect paths
    // are taken literally and cannot inject shell commands.
    std::string escaped = "'";
    for (char ch : token) {
        if (ch == '\'') {
            escaped += "'\\''";
        } else {
            escaped += ch;
        }
    }
    escaped += "'";
    return escaped;
}

bool Executor::IsSafeBindSource(const std::string &path)
{
    // Reject host paths that would let the container/sandbox escape or hijack the daemon.
    if (path.empty() || path == "/") {
        return false;
    }
    static const std::vector<std::string> blocked = {
        "/etc", "/proc", "/sys", "/dev", "/boot", "/var/run/docker.sock", "/run/docker.sock"
    };
    for (const auto &b : blocked) {
        if (path == b || path.rfind(b + "/", 0) == 0) {
            return false;
        }
    }
    if (path.find("..") != std::string::npos) {
        return false;
    }
    return true;
}

double Executor::GetEffectiveScalarLimit(const resource_view::Resource &resource, double defaultValue)
{
    if (resource.type() != resource_view::ValueType::Value_Type_SCALAR) {
        return defaultValue;
    }
    if (resource.scalar().limit() > 0) {
        return resource.scalar().limit();
    }
    if (resource.scalar().value() > 0) {
        return resource.scalar().value();
    }
    return defaultValue;
}

std::string Executor::DockerDaemonMessage(const nlohmann::json &resp)
{
    // ParseDockerResponse stores the 4xx/5xx body two ways: "__docker_error" (parsed JSON; a Docker
    // error body is normally {"message":"..."}) or "__docker_error_raw" (raw text, when the body is
    // not JSON). Prefer the parsed "message" field (clean, the daemon's own explanation); fall back
    // to the raw body for non-standard responses. Type-checked so a malformed field never throws
    // into the logging path.
    if (resp.contains("__docker_error") && resp["__docker_error"].is_object() &&
        resp["__docker_error"].contains("message") && resp["__docker_error"]["message"].is_string()) {
        return resp["__docker_error"]["message"].get<std::string>();
    }
    if (resp.contains("__docker_error_raw") && resp["__docker_error_raw"].is_string()) {
        return resp["__docker_error_raw"].get<std::string>();
    }
    return "";
}

void Executor::ConfigRuntimeRedirectLog(std::string &stdOut, std::string &stdErr,
    const std::string &runtimeID)
{
    auto path = litebus::os::Join(config_.runtimeLogPath, config_.runtimeStdLogDir);
    if (!litebus::os::ExistPath(path)) {
        YRLOG_WARN("{}|std log path {} not found, try to make dir", runtimeID, path);
        if (!litebus::os::Mkdir(path).IsNone()) {
            YRLOG_WARN("{}|failed to make dir {}, msg: {}; log redirect skipped, runtime logs will NOT land on host",
                       runtimeID, path, litebus::os::Strerror(errno));
            return;
        }
    }

    // 目录放权：rwxrwxrwt（sticky bit，等同 /tmp），任意 uid 可建文件但仅可删自己的。
    // 容器内 sh -c ">/out 2>/err" 以容器运行用户身份 open(O_CREAT) 日志文件，属主天然是该 uid。
    constexpr mode_t logDirMode = 01777;  // rwxrwxrwt + sticky bit
    if (::chmod(path.c_str(), logDirMode) != 0) {
        YRLOG_WARN("{}|failed to chmod log dir {} to 1777, msg: {}; log redirect skipped, "
                   "runtime logs will NOT land on host", runtimeID, path, litebus::os::Strerror(errno));
        return;
    }

    // 日志文件由容器内 sh 自建，此处只返回路径。
    stdOut = litebus::os::Join(path, fmt::format("{}.out", runtimeID));
    stdErr = litebus::os::Join(path, fmt::format("{}.err", runtimeID));
}

void Executor::Init()
{
}

void Executor::Finalize()
{
    ActorBase::Finalize();
}

std::map<std::string, ::messages::RuntimeInstanceInfo> Executor::GetRuntimeInstanceInfos()
{
    return std::map<std::string, messages::RuntimeInstanceInfo>();
}

void Executor::SetRuntimeConfig(const Flags &flags)
{
    config_.ip = flags.GetIP();
    config_.hostIP = flags.GetHostIP();
    config_.proxyIP = flags.GetProxyIP();
    config_.nodeID = flags.GetNodeID();
    config_.setCmdCred = flags.GetSetCmdCred();
    config_.runtimePath = flags.GetRuntimePath();
    config_.runtimeStdLogDir = flags.GetRuntimeStdLogDir();
    config_.runtimeConfigPath = flags.GetRuntimeConfigPath();
    config_.runtimeLogPath = flags.GetRuntimeLogPath();
    config_.runtimeMaxLogSize = flags.GetRuntimeMaxLogSize();
    config_.runtimeMaxLogFileNum = flags.GetRuntimeMaxLogFileNum();
    auto logLevel = flags.GetRuntimeLogLevel();
    if (std::find(logLevels.begin(), logLevels.end(), logLevel) == logLevels.end()) {
        YRLOG_ERROR("runtime log level: {} format error, should use INFO/DEBUG/WARN/ERROR. use default DEBUG",
                    logLevel);
        config_.runtimeLogLevel = "DEBUG";
    } else {
        config_.runtimeLogLevel = logLevel;
    }
    config_.runtimeLdLibraryPath = flags.GetRuntimeLdLibraryPath();
    config_.pythonDependencyPath = flags.GetPythonDependencyPath();
    config_.pythonLogConfigPath = flags.GetPythonLogConfigPath();
    config_.javaSystemProperty = flags.GetJavaSystemProperty();
    config_.javaSystemLibraryPath = flags.GetJavaSystemLibraryPath();
    config_.dataSystemPort = flags.GetDataSystemPort();
    config_.snuserLibDir = flags.GetSnuserLibDir();
    config_.driverServerPort = flags.GetDriverServerPort();
    const std::string &defaultConfig = flags.GetRuntimeDefaultConfig();
    InitDefaultArgs(defaultConfig);
    config_.maxJvmMemory = flags.GetProcMetricsMemory() * JVM_MEMORY_THRESHOLD;
    config_.proxyGrpcServerPort = flags.GetProxyGrpcServerPort();
    config_.clusterID = flags.GetClusterID();
    config_.runtimeUID = flags.GetRuntimeUID() > 0 ? flags.GetRuntimeUID() : DEFAULT_USER_ID;
    config_.runtimeGID = flags.GetRuntimeGID() > 0 ? flags.GetRuntimeGID() : DEFAULT_GROUP_ID;
    config_.isProtoMsgToRuntime = flags.GetIsProtoMsgToRuntime();
    config_.massifEnable = flags.GetMassifEnable();
    config_.inheritEnv = flags.GetInheritEnv();
    config_.separatedRedirectRuntimeStd = flags.GetSeparetedRedirectRuntimeStd();
    config_.userLogAutoFlushIntervalMs = flags.GetUserLogAutoFlushIntervalMs();
    config_.userLogBufferFlushThreshold = flags.GetUserLogBufferFlushThreshold();
    config_.userLogRollingSizeLimitMb = flags.GetUserLogRollingSizeLimitMb();
    config_.userLogRollingFileCountLimit = flags.GetUserLogRollingFileCountLimit();
    const std::string &prestartConfig = flags.GetRuntimePrestartConfig();
    if (!prestartConfig.empty() && prestartConfig != "{}") {
        YRLOG_DEBUG("prestart config is not empty, start to parse");
        InitPrestartConfig(prestartConfig);
    }
    config_.enableDisConvCallStack = flags.GetEnableDisConvCallStack();
    config_.runtimeDirectConnectionEnable = flags.GetRuntimeDirectConnectionEnable();
    config_.runtimeHomeDir = flags.GetRuntimeHomeDir();
    config_.nodeJsEntryPath = flags.GetNodeJsEntryPath();
    config_.runtimeDsConnectTimeout = flags.GetRuntimeDsConnectTimeout();
    config_.killProcessTimeoutSeconds = flags.GetKillProcessTimeoutSeconds();
    config_.userLogExportMode = flags.GetUserLogExportMode();
    config_.cleanStreamProducerEnable = flags.GetCleanStreamProducerEnable();
    config_.virtualEnvIdleTimeLimit = flags.GetVirtualEnvIdleTimeLimit();
    InitConfig();
}

void Executor::ParseJvmArgs(
    const std::string &language, const nlohmann::json &confJson, std::vector<std::string> &jvmArgs)
{
    if (confJson[language].is_array()) {
        auto defaultArgs = VerifyCustomJvmArgs(confJson[language].get<std::vector<std::string>>());
        if (!defaultArgs.empty()) {
            jvmArgs = defaultArgs;
        }
        YRLOG_DEBUG("set {} default args done", language);
    }
}

void Executor::InitDefaultArgs(const std::string &configJsonString)
{
    config_.jvmArgs = DEFAULT_JVM_ARGS;
    config_.jvmArgsForJava11 = DEFAULT_JVM_ARGS_FOR_JAVA11;
    config_.jvmArgsForJava17 = DEFAULT_JVM_ARGS_FOR_JAVA17;
    config_.jvmArgsForJava21 = DEFAULT_JVM_ARGS_FOR_JAVA21;

    auto confJson = nlohmann::json::parse(configJsonString, nullptr, false);
    if (confJson.is_null() || confJson.is_discarded()) {
        YRLOG_WARN("failed to parse default config to json");
        return;
    }
    for (const auto &item : confJson.items()) {
        auto language = item.key();
        YRLOG_DEBUG("parse default config language: {}", language);
        if (language.rfind(JAVA_LANGUAGE, 0) == 0) {
            ParseJvmArgs(language, confJson, config_.jvmArgs);
        } else if (language.rfind(JAVA11_LANGUAGE, 0) == 0) {
            ParseJvmArgs(language, confJson, config_.jvmArgsForJava11);
        } else if (language.rfind(JAVA17_LANGUAGE, 0) == 0) {
            ParseJvmArgs(language, confJson, config_.jvmArgsForJava17);
        } else if (language.rfind(JAVA21_LANGUAGE, 0) == 0) {
            ParseJvmArgs(language, confJson, config_.jvmArgsForJava21);
        }
    }
}

void Executor::InitConfig()
{
    YRLOG_INFO("---- InitConfig");
    if (config_.virtualEnvIdleTimeLimit != -1) {
        litebus::Async(GetAID(), &Executor::InitVirtualEnvIdleTimeLimit);
    }
}

void Executor::InitPrestartConfig(const std::string &configJsonString)
{
    auto confJson = nlohmann::json::parse(configJsonString, nullptr, false);
    if (confJson.is_null() || confJson.is_discarded()) {
        YRLOG_WARN("failed to parse prestart config to json");
        return;
    }
    for (const auto &item : confJson.items()) {
        auto language = item.key();
        YRLOG_DEBUG("parse prestart config language: {}", language);
        config_.runtimePrestartConfigs[language] = GetPrestartCountFromConfig(confJson[language]);
        if (language.rfind(JAVA_LANGUAGE, 0) == 0) {
            if (confJson[language].contains(CUSTOM_ARGS_STR) && confJson[language][CUSTOM_ARGS_STR].is_array()) {
                YRLOG_DEBUG("jvm args is overwritten by custom args");
                auto customArgs =
                    VerifyCustomJvmArgs(confJson[language][CUSTOM_ARGS_STR].get<std::vector<std::string>>());
                if (!customArgs.empty()) {
                    config_.jvmArgs = customArgs;
                    config_.jvmArgsForJava11 = customArgs;
                }
            }
        }
    }
    litebus::Async(GetAID(), &Executor::InitPrestartRuntimePool);
}

int Executor::GetPrestartCountFromConfig(const nlohmann::json &configJson) const
{
    if (!configJson.contains(PRESTART_COUNT_STR)) {
        return MIN_PRESTART_COUNT;
    }
    if (!configJson[PRESTART_COUNT_STR].is_number_integer()) {
        return MIN_PRESTART_COUNT;
    }
    int count = configJson[PRESTART_COUNT_STR].get<int>();
    if (count < MIN_PRESTART_COUNT) {
        return MIN_PRESTART_COUNT;
    }
    if (count > MAX_PRESTART_COUNT) {
        return MAX_PRESTART_COUNT;
    }
    return count;
}

std::vector<std::string> Executor::VerifyCustomJvmArgs(const std::vector<std::string> &customArgs)
{
    std::vector<std::string> jvmArgsVector;
    for (auto &jvmArg : customArgs) {
        if (std::regex_match(jvmArg, JVM_ARGS_REGEX) == 1 &&
            REJECTED_JVM_ARGS.find(jvmArg) == REJECTED_JVM_ARGS.end()) {
            (void)jvmArgsVector.emplace_back(jvmArg);
            YRLOG_DEBUG("add jvmArg: {}", jvmArg);
        }
    }
    return jvmArgsVector;
}

std::shared_ptr<litebus::Exec> Executor::GetExecByRuntimeID(const std::string &runtimeID)
{
    for (const auto &pair : runtime2Exec_) {
        if (pair.first != runtimeID) {
            continue;
        }
        YRLOG_DEBUG("find exec by runtimeID: {}", runtimeID);
        return pair.second;
    }
    YRLOG_ERROR("can not find exec by runtimeID: {}", runtimeID);
    return nullptr;
}

bool Executor::IsRuntimeActive(const std::string &runtimeID)
{
    // Note: each implementation class of the Executor interface needs to reflect the startup and destroy of the
    // runtime's lifecycle in a timely manner in the runtime2Exec_ record of Executor base class.
    return runtime2Exec_.find(runtimeID) != runtime2Exec_.end();
}

bool Executor::IsRuntimeActiveByPid(const pid_t &pid)
{
    // Note: each implementation class of the Executor interface needs to reflect the startup and destroy of the
    // runtime's lifecycle in a timely manner in the runtime2Exec_ record of Executor base class.
    return pids_.find(pid) != pids_.end();
}
}  // namespace functionsystem::runtime_manager