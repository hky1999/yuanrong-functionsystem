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

#include "flags.h"

#include "common/constants/constants.h"
#include "common/utils/param_check.h"
#include "startup/busproxy_startup.h"

namespace functionsystem::function_proxy {
namespace {
const uint32_t FC_AGENT_MGR_RETRY_TIMES = 9;
const uint32_t MIN_FC_AGENT_MGR_RETRY_TIMES = 0;
const uint32_t MAX_FC_AGENT_MGR_RETRY_TIMES = 100;

const uint32_t FC_AGENT_MGR_RETRY_CYCLE_MS = 20000;
const uint32_t MIN_FC_AGENT_MGR_RETRY_CYCLE_MS = 5000;
const uint32_t MAX_FC_AGENT_MGR_RETRY_CYCLE_MS = 60000;

const uint32_t FC_AGENT_MGR_PING_TIMES = 12;
const uint32_t FC_AGENT_MGR_PING_CYCLE_MS = 1000;

const uint32_t SERVICE_REGISTER_TIMES = 1000;
const uint32_t MIN_SERVICE_REGISTER_TIMES = 10;
const uint32_t MAX_SERVICE_REGISTER_TIMES = 10000;

const uint32_t SERVICE_REGISTER_CYCLE_MS = 10000;
const uint32_t MIN_SERVICE_REGISTER_CYCLE_MS = 5000;
const uint32_t MAX_SERVICE_REGISTER_CYCLE_MS = 60000;

const uint32_t SERVICE_PING_TIMEOUT = 90000;

const uint32_t DEFAULT_TCP_TUNNEL_MAX_CONNECTIONS = 1024;
const uint32_t MIN_TCP_TUNNEL_MAX_CONNECTIONS = 1;
const uint32_t MAX_TCP_TUNNEL_MAX_CONNECTIONS = 65535;

const uint32_t SERVICE_UPDATE_RESOURCE_CYCLE_MS = 1000;
const uint32_t MIN_SERVICE_UPDATE_RESOURCE_CYCLE_MS = 500;
const uint32_t MAX_SERVICE_UPDATE_RESOURCE_CYCLE_MS = 60000;

const uint32_t RUNTIME_MAX_HEARTBEAT_TIMEOUT_TIMES = 5;
const uint32_t MIN_RUNTIME_MAX_HEARTBEAT_TIMEOUT_TIMES = 3;
const uint32_t MAX_RUNTIME_MAX_HEARTBEAT_TIMEOUT_TIMES = 30;

const uint32_t RUNTIME_HEARTBEAT_TIMEOUT_MS = 5000;
const uint32_t MIN_RUNTIME_HEARTBEAT_TIMEOUT_MS = 1000;
const uint32_t MAX_RUNTIME_HEARTBEAT_TIMEOUT_MS = 300000;

const uint32_t RUNTIME_INIT_CALL_TIMEOUT_SECONDS = 300;
const uint32_t MIN_RUNTIME_INIT_CALL_TIMEOUT_SECONDS = 30;
const uint32_t MAX_RUNTIME_INIT_CALL_TIMEOUT_SECONDS = 1800;

const uint32_t DEFAULT_CONNECT_TIMEOUT_SECONDS = 30;
const uint32_t MIN_CONNECT_TIMEOUT_SECONDS = 5;
const uint32_t MAX_CONNECT_TIMEOUT_SECONDS = 600;

const uint32_t MIN_TOKEN_EXPIRED_TIME_SPAN = 5 * 60 * 60;  // unit: s
const uint32_t MAX_TOKEN_EXPIRED_TIME_SPAN = 7 * 24 * 60 * 60;  // unit: s

const uint32_t DEFAULT_TOKEN_EXPIRED_TIME_SPAN = 24 * 60 * 60 * 1000;  // unit: s
const uint32_t RUNTIME_SHUTDOWN_TIMEOUT_SECONDS = 30;
const uint32_t MIN_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS = 5;
const uint32_t MAX_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS = 1200;

const int32_t DEFAULT_MAX_GRPC_SIZE = 11;
const int32_t MIN_MAX_GRPC_SIZE = 4;
const int32_t MAX_MAX_GRPC_SIZE = 1024 * 10;

const int32_t DEFAULT_OBSERVABILITY_AGENT_GRPC_PORT = 4317;
const float DEFAULT_LOW_MEMORY_THRESHOLD = 0.6;
const float MIN_LOW_MEMORY_THRESHOLD = 0.1;
const float MAX_LOW_MEMORY_THRESHOLD = 0.7;
const float DEFAULT_HIGH_MEMORY_THRESHOLD = 0.8;
const float MIN_HIGH_MEMORY_THRESHOLD = 0.5;
const float MAX_HIGH_MEMORY_THRESHOLD = 1.0;

const uint64_t DEFAULT_MESSAGE_SIZE_THRESHOLD = 20 * 1024;
const uint64_t MIN_MESSAGE_SIZE_THRESHOLD = 5 * 1024;
const uint64_t MAX_MESSAGE_SIZE_THRESHOLD = 100 * 1024;

const uint64_t DEFAULT_DS_HEALTH_CHECK_INTERVAL = 3000;
const uint64_t MIN_DS_HEALTH_CHECK_INTERVAL = 500;
const uint64_t MAX_DS_HEALTH_CHECK_INTERVAL = 60000;

const uint64_t DEFAULT_MAX_DS_HEALTH_CHECK_TIMES = 5;
const uint64_t MIN_MAX_DS_HEALTH_CHECK_TIMES = 3;
const uint64_t MAX_MAX_DS_HEALTH_CHECK_TIMES = 30;
const uint32_t DEFAULT_SERVICE_TTL = 300000;

const std::string DEFAULT_LOCAL_SCHEDULE_PLUGINS =
    R"("["Default", "ResourceSelector", "Label", "Heterogeneous"]")";

const uint32_t PARKED_INVOKE_HOLD_SECONDS = 300;
const uint32_t MIN_PARKED_INVOKE_HOLD_SECONDS = 10;
const uint32_t MAX_PARKED_INVOKE_HOLD_SECONDS = 1800;

const uint32_t PARK_DRAIN_TIMEOUT_MS = 10000;
const uint32_t MIN_PARK_DRAIN_TIMEOUT_MS = 0;
const uint32_t MAX_PARK_DRAIN_TIMEOUT_MS = 60000;

}  // namespace
using namespace litebus::flag;
Flags::Flags()
{
    AddFlag(&Flags::logConfig_, "log_config", "json format string. For log initialization.", DEFAULT_LOG_CONFIG);
    AddFlag(&Flags::nodeId_, "node_id", "vm id");
    AddFlag(&Flags::address_, "address", "address to listen on. example: 127.0.0.1:24032", true,
            FlagCheckWrraper(IsAddressesValid));
    AddGrpcServerFlags();
    AddFlag(&Flags::schedulePolicy_, "schedule_policy", "type of scheduler policy. example: resource", "default");
    AddFlag(&Flags::metaStoreAddress_, "meta_store_address", "for MetaStorage service discover");
    AddFlag(&Flags::globalSchedulerAddress_, "global_scheduler_address", "for global scheduler service discover",
            std::string("127.0.0.1:22770"), FlagCheckWrraper(IsAddressesValid));
    AddFlag(&Flags::funcAgentMgrRetryTimes_, "fc_agent_mgr_retry_times", "for function agent mgr retry request times",
            FC_AGENT_MGR_RETRY_TIMES, NumCheck(MIN_FC_AGENT_MGR_RETRY_TIMES, MAX_FC_AGENT_MGR_RETRY_TIMES));
    AddFlag(&Flags::funcAgentMgrRetryCycleMs_, "fc_agent_mgr_retry_cycle", "for function agent mgr retry request cycle",
            FC_AGENT_MGR_RETRY_CYCLE_MS, NumCheck(MIN_FC_AGENT_MGR_RETRY_CYCLE_MS, MAX_FC_AGENT_MGR_RETRY_CYCLE_MS));
    AddFlag(&Flags::serviceRegisterCycleMs_, "service_register_cycle",
            "cycle for local scheduler to register with domain scheduler (ms)", SERVICE_REGISTER_CYCLE_MS,
            NumCheck(MIN_SERVICE_REGISTER_CYCLE_MS, MAX_SERVICE_REGISTER_CYCLE_MS));
    AddFlag(&Flags::serviceRegisterTimes_, "service_register_times",
            "max times for local scheduler to register with domain scheduler", SERVICE_REGISTER_TIMES,
            NumCheck(MIN_SERVICE_REGISTER_TIMES, MAX_SERVICE_REGISTER_TIMES));
    AddFlag(&Flags::updateResourceCycle_, "update_resource_cycle",
            "cycle for local scheduler updating resource to domain scheduler (ms)", SERVICE_UPDATE_RESOURCE_CYCLE_MS,
            NumCheck(MIN_SERVICE_UPDATE_RESOURCE_CYCLE_MS, MAX_SERVICE_UPDATE_RESOURCE_CYCLE_MS));
    AddFlag(&Flags::servicesPath_, "services_path", "service yaml path", "/");
    AddFlag(&Flags::libPath_, "lib_path", "path of yaml tool lib", "/");
    AddFlag(&Flags::serviceTTL_, "service_ttl", "ttl of busproxy", DEFAULT_SERVICE_TTL);
    AddFlag(&Flags::functionMetaPath_, "function_meta_path", "local function meta path", LOCAL_FUNCTION_META_PATH);
    AddFlag(&Flags::observabilityAgentGrpcPort_, "observability_agent_grpc_port",
            "observability agent's grpc port, default is 4317", DEFAULT_OBSERVABILITY_AGENT_GRPC_PORT);
    AddFlag(&Flags::observabilityPrometheusPort_, "observability_prometheus_port",
            "observability prometheus port, default is 9392", DEFAULT_OBSERVABILITY_PROMETHEUS_PORT);
    AddFlag(&Flags::enableTrace_, "enable_trace", "for trace enable, example: false", false);
    AddFlag(&Flags::enableMergeProcess_, "enable_merge_process",
            "enable function_proxy, function_agent and runtime_manager merge in the same process", false);
    AddFlag(&Flags::isPseudoDataPlane_, "pseudo_data_plane",
            "set the function proxy is Pseudo Data Plane, example: false", false);
    AddFlag(&Flags::enablePrintResourceView_, "enable_print_resource_view",
            "whether enable print resource view, which will affect performance in big scale", false);
    AddFlag(&Flags::schedulePlugins_, "schedule_plugins", "schedule plugins need to be registered",
            DEFAULT_LOCAL_SCHEDULE_PLUGINS);
    AddFlag(&Flags::usageAwareSafety_, "usage_aware_safety",
            "usage-aware admission safety fraction of node memory capacity, used by UsageAwareFilter", 0.9,
            NumCheck(0.1, 1.0));
    AddFlag(&Flags::usageAwareFloorMb_, "usage_aware_floor_mb",
            "per-instance real memory reserve (MB) for usage-aware admission, capped by the instance request", 2048.0,
            NumCheck(0.0, 1048576.0));
    AddFlag(&Flags::parkedInvokeDeferEnable_, "parked_invoke_defer_enable",
            "hold data-plane invokes of parked instances until restore or hold TTL instead of failing them", true);
    AddFlag(&Flags::parkedInvokeHoldSeconds_, "parked_invoke_hold_seconds",
            "how long a parked instance keeps holding data-plane invokes before they fail with instance exited",
            PARKED_INVOKE_HOLD_SECONDS, NumCheck(MIN_PARKED_INVOKE_HOLD_SECONDS, MAX_PARKED_INVOKE_HOLD_SECONDS));
    AddFlag(&Flags::parkDrainEnable_, "park_drain_enable",
            "quiesce in-flight invokes (dispatcher not-ready + bounded wait) before a park kills the sandbox", true);
    AddFlag(&Flags::parkDrainTimeoutMs_, "park_drain_timeout_ms",
            "how long the park drain phase waits for in-flight invokes before the park is abandoned (0 = flip only)",
            PARK_DRAIN_TIMEOUT_MS, NumCheck(MIN_PARK_DRAIN_TIMEOUT_MS, MAX_PARK_DRAIN_TIMEOUT_MS));
    AddFlag(&Flags::parkDrainForceOnTimeout_, "park_drain_force_on_timeout",
            "proceed with the park even when the drain phase times out (in-flight invokes may break; experiments only)",
            false);
    AddFlag(&Flags::enablePerf_, "enable_print_perf", "whether enable print perf", false);
    AddFlag(&Flags::enableMetaStore_, "enable_meta_store", "for meta store enable", false);
    AddFlag(&Flags::metaStoreMode_, "meta_store_mode", "meta-store mode, eg. local", "local");
    AddFlag(&Flags::forwardCompatibility_, "forward_compatibility", "for forward compatible(eg.async function)", false);
    AddFlag(&Flags::isPartialWatchInstances_, "is_partial_watch_instances", "only watch partial instance", false);
    AddFlag(&Flags::diskUsageMonitorForceDeletePodEnable_, "disk_usage_monitor_force_delete_pod_enable",
            "whether disk usage monitor force delete pod", true);
    AddFlag(&Flags::unRegisterWhileStop_, "unregister_while_stop",
            "if true, all instance & agent would be evicted while function-proxy receive SIGTERM/SIGINT", false);
    AddFlag(&Flags::enableDirectRouting_, "enable_direct_routing",
            "enable direct routing read path with LRU cache and on-demand route query", false);
    AddFlag(&Flags::forceLowReliabilityInstance_, "force_low_reliability_instance",
            "force all instances to use low-reliability persistence semantics", false);
    AddFlag(&Flags::enableTcpTunnel_, "enable_tcp_tunnel",
            "enable the TCP tunnel listener", false);
    AddFlag(&Flags::tcpTunnelPort_, "tcp_tunnel_port",
            "listen port for the TCP tunnel", "22775");
    AddFlag(&Flags::tcpTunnelMaxConnections_, "tcp_tunnel_max_connections",
            "maximum concurrent TCP tunnel connections", DEFAULT_TCP_TUNNEL_MAX_CONNECTIONS,
            NumCheck(MIN_TCP_TUNNEL_MAX_CONNECTIONS, MAX_TCP_TUNNEL_MAX_CONNECTIONS));
    AddFlag(&Flags::enableFrontendProxyService_, "enable_frontend_proxy_service",
            "enable faasfrontend gRPC service on the existing proxy POSIX port", false);
    AddElectionFlags();
    AddDSFlags();
    AddRuntimeFlags();
    AddIAMFlags();
    AddIsolationFlags();
    AddBusProxyInvokeLimitFlags();
    AddFlag(&Flags::redisConfPath_, "redis_conf_path", "redis connection conf file path", "/home/sn/conf/conf.json");
    AddBusProxyCreatRateLimitFlags();
    AddFlag(&Flags::enableTraefikRegistry_, "enable_traefik_registry",
            "enable traefik registry for instance route", false);
    AddFlag(&Flags::traefikEtcdPrefix_, "traefik_etcd_prefix",
            "etcd key prefix for traefik configuration", "traefik");
    constexpr int traefikDefaultLeaseTtlMs = 300000;  // 5 minutes in milliseconds
    AddFlag(&Flags::traefikLeaseTTL_, "traefik_lease_ttl",
            "lease TTL for traefik registry (milliseconds)", traefikDefaultLeaseTtlMs);
    AddFlag(&Flags::traefikHttpEntryPoint_, "traefik_http_entrypoint",
            "HTTP entryPoint name for traefik routing", "websecure");
    AddFlag(&Flags::traefikEnableTLS_, "traefik_enable_tls",
            "enable TLS for HTTP routing", true);
    AddFlag(&Flags::traefikServersTransport_, "traefik_servers_transport",
            "ServersTransport name for backend TLS (e.g. yr-backend-tls@file)", "yr-backend-tls@file");
}

void Flags::AddElectionFlags()
{
    AddFlag(&Flags::k8sNamespace_, "k8s_namespace", "k8s cluster namespace", "default");
    AddFlag(&Flags::basePath_, "k8s_base_path", "For k8s service discovery.", "");
    AddFlag(&Flags::electionMode_, "election_mode", "function master election mode, eg: k8s, txn, etcd, standalone",
            std::string("standalone"), WhiteListCheck({ "etcd", "txn", "k8s", "standalone" }));
    AddFlag(&Flags::electKeepAliveInterval_, "elect_keep_alive_interval", "interval of elect's lease keep alive",
            DEFAULT_ELECT_KEEP_ALIVE_INTERVAL,
            NumCheck(MIN_ELECT_KEEP_ALIVE_INTERVAL, MAX_ELECT_KEEP_ALIVE_INTERVAL));
}

void Flags::AddDSFlags()
{
    AddFlag(&Flags::cacheStorageHost_, "cache_storage_host", "for cache storage service discover", "127.0.0.1");
    AddFlag(&Flags::cacheStoragePort_, "cache_storage_port", "for cache storage service discover", "31501");
    AddFlag(&Flags::stateStorageType_, "state_storage_type",
            "set storage type for state of instance, example: datasystem, redis, local, disable", DISABLE_STORE);
    AddFlag(&Flags::cacheStorageAuthEnable_, "cache_storage_auth_enable", "for cache storage service auth", false);
    AddFlag(&Flags::cacheStorageAuthType_, "cache_storage_auth_type",
            "for cache storage service auth type, eg: Noauth, TLS, AK/SK", "Noauth");
    AddFlag(&Flags::cacheStorageAuthAK_, "cache_storage_auth_ak", "for cache storage service auth ak", "");
    AddFlag(&Flags::cacheStorageAuthSK_, "cache_storage_auth_sk", "for cache storage service auth sk", "");
    AddFlag(&Flags::cacheStorageInfoPrefix_, "cache_storage_info_prefix", "for cache storage service info prefix", "");
    AddFlag(&Flags::dsHealthCheckPath_, "ds_health_check_path",
            "path which include healthy file for check ds worker healthy", "");
    AddFlag(&Flags::dsHealthCheckInterval_, "ds_health_check_interval", "for check ds worker healthy interval",
            DEFAULT_DS_HEALTH_CHECK_INTERVAL, NumCheck(MIN_DS_HEALTH_CHECK_INTERVAL, MAX_DS_HEALTH_CHECK_INTERVAL));
    AddFlag(&Flags::maxDsHealthCheckTimes_, "max_ds_health_check_times", "for check ds worker healthy times",
            DEFAULT_MAX_DS_HEALTH_CHECK_TIMES, NumCheck(MIN_MAX_DS_HEALTH_CHECK_TIMES, MAX_MAX_DS_HEALTH_CHECK_TIMES));
    AddFlag(&Flags::runtimeDsAuthEnable_, "runtime_ds_auth_enable",
            "runtime and datasystem authentication enable", false);
    AddFlag(&Flags::runtimeDsEncryptEnable_, "runtime_ds_encrypt_enable", "runtime and datasystem encryption enable",
            false);
    AddFlag(&Flags::curveKeyPath_, "curve_key_path", "curve key path", "");
    AddFlag(&Flags::runtimeDsServerPublicKey_, "runtime_ds_server_public_key",
            "runtime and datasystem authentication server public key file name", "worker.key");
    AddFlag(&Flags::runtimeDsClientPrivateKey_, "runtime_ds_client_private_key",
            "runtime and datasystem authentication client private key file name", "client.key_secret");
    AddFlag(&Flags::runtimeDsClientPublicKey_, "runtime_ds_client_public_key",
            "runtime and datasystem authentication client public key file name", "client.key");
}

void Flags::AddRuntimeFlags()
{
    AddFlag(&Flags::runtimeRecoverEnable_, "runtime_recover_enable", "enable recover runtime", false);
    AddFlag(&Flags::runtimeHeartbeatEnable_, "runtime_heartbeat_enable",
            "enable heartbeat between function_proxy and runtime", "true");
    AddFlag(&Flags::runtimeMaxHeartbeatTimeoutTimes_, "runtime_max_heartbeat_timeout_times",
            "max heartbeat timeout times between function_proxy and runtime", RUNTIME_MAX_HEARTBEAT_TIMEOUT_TIMES,
            NumCheck(MIN_RUNTIME_MAX_HEARTBEAT_TIMEOUT_TIMES, MAX_RUNTIME_MAX_HEARTBEAT_TIMEOUT_TIMES));
    AddFlag(&Flags::runtimeHeartbeatTimeoutMS_, "runtime_heartbeat_timeout_ms",
            "heartbeat timeout between function_proxy and runtime", RUNTIME_HEARTBEAT_TIMEOUT_MS,
            NumCheck(MIN_RUNTIME_HEARTBEAT_TIMEOUT_MS, MAX_RUNTIME_HEARTBEAT_TIMEOUT_MS));
    AddFlag(&Flags::runtimeInitCallTimeoutSeconds_, "runtime_init_call_timeout_seconds",
            "init call timeout between function_proxy and runtime", RUNTIME_INIT_CALL_TIMEOUT_SECONDS,
            NumCheck(MIN_RUNTIME_INIT_CALL_TIMEOUT_SECONDS, MAX_RUNTIME_INIT_CALL_TIMEOUT_SECONDS));
    AddFlag(&Flags::runtimeShutdownTimeoutSeconds_, "runtime_shutdown_timeout_seconds",
            "runtime shutdown timeout seconds", RUNTIME_SHUTDOWN_TIMEOUT_SECONDS,
            NumCheck(MIN_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS, MAX_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS));
    AddFlag(&Flags::runtimeConnTimeoutSeconds_, "runtime_conn_timeout_s",
            "timeout for the first connection between function_proxy and runtime", DEFAULT_CONNECT_TIMEOUT_SECONDS,
            NumCheck(MIN_CONNECT_TIMEOUT_SECONDS, MAX_CONNECT_TIMEOUT_SECONDS));
    AddFlag(&Flags::runtimeInstanceDebugEnable_, "runtime_instance_debug_enable", "runtime instance debug enable",
            false);
}

void Flags::AddGrpcServerFlags()
{
    AddFlag(&Flags::ip_, "ip", "IP address for listening.", "127.0.0.1", FlagCheckWrraper(IsIPValid));
    AddFlag(&Flags::grpcListenPort_, "grpc_listen_port", "For posix server listening. example: 30001",
            std::string("30001"), FlagCheckWrraper(IsPortValid));
    AddFlag(&Flags::sessionGrpcPort_, "session_grpc_port",
            "Session gRPC port for ExecStream. 0 = disabled. example: 30002", std::string("0"));
    AddFlag(&Flags::maxGrpcSize_, "max_grpc_size", "posix max grpc size", DEFAULT_MAX_GRPC_SIZE,
            NumCheck(MIN_MAX_GRPC_SIZE, MAX_MAX_GRPC_SIZE));
    AddFlag(&Flags::enableServerMode_, "enable_server_mode",
            "if on, grpc server will set in proxy and client in runtime", true);
    AddFlag(&Flags::enableDriver_, "enable_driver",
            "Indicates whether to enable the gateway service to discover driver.", false);
    AddFlag(&Flags::dPosixUdsPath_, "dposix_uds_path",
            "dposix uds path for communication between runtime and function proxy", "");
}

void Flags::AddIAMFlags()
{
    AddFlag(&Flags::enableIAM_, "enable_iam", "enable verify and authorize token of internal request", false);
    AddFlag(&Flags::iamBasePath_, "iam_base_path", "iam server base path", "");
    AddFlag(&Flags::iamPolicyFile_, "iam_policy_file", "iam policy file to authorize function request", "");
    AddFlag(&Flags::iamMetastoreAddress_, "iam_meta_store_address", "for iam metaStorage service discover", "");
    AddFlag(&Flags::iamCredentialType_, "iam_credential_type", "credential type for iam", IAM_CREDENTIAL_TYPE_TOKEN,
            WhiteListCheck({ IAM_CREDENTIAL_TYPE_TOKEN, IAM_CREDENTIAL_TYPE_AK_SK }));
}

void Flags::AddIsolationFlags()
{
    AddFlag(&Flags::enableTenantAffinity_,
        "enable_tenant_affinity",
        "Enable tenant affinity for safety: functions belonging to the same tenant will be scheduled to the same pod, "
        "while functions belonging to different tenants will be scheduled to different pods.",
        false);
    AddFlag(&Flags::tenantPodReuseTimeWindow_,
        "tenant_pod_reuse_time_window",
        "Time window for reusing function_agent POD for the same tenant, in seconds. Optional, default value is -1. "
        "When set to 0, the agent will be killed immediately without setting a timer. "
        "When set to -1, kill agent is not enabled and the tenant label will be removed for reuse by other tenants "
        "after the instances in the POD are cleared. Other negative values except -1 are illegal.",
        DEFAULT_TENANT_POD_REUSE_TIME_WINDOW, NumCheck(DEFAULT_TENANT_POD_REUSE_TIME_WINDOW,
        std::numeric_limits<int32_t>::max()));
    AddFlag(&Flags::enableIpv4TenantIsolation_,
        "enable_ipv4_tenant_isolation",
        "It ensures that  IPv4 network traffic between different tenants does not interfere with each other",
        false);
}

void Flags::AddBusProxyInvokeLimitFlags()
{
    AddFlag(&Flags::lowMemoryThreshold_, "low_memory_threshold",
            "memory usage percent to start low level's invoke limitation", DEFAULT_LOW_MEMORY_THRESHOLD,
            NumCheck(MIN_LOW_MEMORY_THRESHOLD, MAX_LOW_MEMORY_THRESHOLD));
    AddFlag(&Flags::highMemoryThreshold_, "high_memory_threshold",
            "memory usage percent to start high level's invoke limitation", DEFAULT_HIGH_MEMORY_THRESHOLD,
            NumCheck(MIN_HIGH_MEMORY_THRESHOLD, MAX_HIGH_MEMORY_THRESHOLD));
    AddFlag(&Flags::messageSizeThreshold_, "message_size_threshold",
            "minimum message size for low level's invoke limitation", DEFAULT_MESSAGE_SIZE_THRESHOLD,
            NumCheck(MIN_MESSAGE_SIZE_THRESHOLD, MAX_MESSAGE_SIZE_THRESHOLD));
    AddFlag(&Flags::invokeLimitationEnable_, "invoke_limitation_enable",
            "enable invoke limitation based on system memory usage", false);
}

void Flags::AddBusProxyCreatRateLimitFlags()
{
    AddFlag(&Flags::createLimitationEnable_,
        "create_limitation_enable",
        "enable POSIX Create request rate limitation based on token bucket rate limiting algorithm",
        false);
    AddFlag(&Flags::tokenBucketCapacity_,
        "token_bucket_capacity",
        "capacity of the token bucket, the value is an integer greater than 0, default value is 1000. "
        "The configuration is simplified. The token refilling rate of the token bucket rate limiting algorithm is "
        "the same as the token bucket capacity configuration.",
        DEFAULT_TENANT_TOKEN_BUCKET_CAPACITY,
        NumCheck(static_cast<uint32_t>(1), std::numeric_limits<uint32_t>::max()));
}

Flags::~Flags()
{
}
}  // namespace functionsystem::function_proxy
