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

#ifndef COMMON_CONSTANTS_CONSTANTS_H
#define COMMON_CONSTANTS_CONSTANTS_H

#include <string>
#include <vector>

namespace functionsystem {

const std::string DEFAULT_LOG_CONFIG =
    R"({"filepath":"/home/yr/log","level":"DEBUG","rolling":{"maxsize":100,"maxfiles":1},"alsologtostderr":true})";

const std::string APPLE = "apple";
const std::string BOY = "boy";
const std::string DOG = "dog";
const std::string EGG = "egg";

const std::string RDO = "rdo";

const std::string ROOT_KEY_VERSION = "v1";
const std::string RESOURCE_DIRECTORY = "resource";

const std::string A_TXT = "a.txt";
const std::string B_TXT = "b.txt";
const std::string D_TXT = "d.txt";
const std::string E_TXT = "e.txt";

const std::string MONOPOLY_SCHEDULE = "monopoly";

const std::string FUNCTION_AGENT_ID_PREFIX = "function-agent-";

const bool DEFAULT_ENABLE_S3 = true;
const double EPSINON = 1e-8;

const int32_t ITERATIONS = 10000;

const int32_t HETERO_RESOURCE_FIELD_NUM = 3;
const uint32_t VENDOR_IDX = 0;
const uint32_t PRODUCT_INDEX = 1;
const uint32_t RESOURCE_IDX = 2;

// grpc channel reconnect intervals(ms)
const int32_t RECONNECT_BACKOFF_INTERVAL = 100;
const int32_t COMPONENT_MAX_LEN = 256;
const int32_t COMPONENT_MIN_LEN = 16;
const int32_t ROOT_KEY_LEN = 32;  // Adapt for go encrypted

const int32_t RESOURCE_PATH_MAX_SIZE = 255;

const int32_t LITEBUS_THREAD_NUM = 20;

const uint32_t DEFAULT_SYSTEM_TIMEOUT = 180000;
const uint64_t DEFAULT_PULL_RESOURCE_INTERVAL = 500;

const uint32_t DEFAULT_DOMAIN_HEARTBEAT_TIMEOUT = 6000;

// SANDBOXD selects the sandboxd-backed SandboxdExecutor (runtime.v1.SandboxService).
// Selection is env-gated (YR_RUNTIME_BACKEND=sandboxd); see ContainerExecutorType().
enum class EXECUTOR_TYPE {
    RUNTIME = 0, CONTAINER = 1, SUPERVISOR = 2, DOCKER = 3, SANDBOXD = 4, CONCH = 5, UNKNOWN = -1
};

const int32_t SYSTEM_FUNCTION_INSTANCE_LEVEL = 1;

const std::string MOUNT_USER = "mount_user";
const std::string MOUNT_USER_ID = "user_id";
const std::string MOUNT_USER_GROUP_ID = "user_group_id";
const std::string FUNC_MOUNTS = "func_mounts";
const std::string FUNC_MOUNT_TYPE = "mount_type";
const std::string FUNC_MOUNT_RESOURCE = "mount_resource";
const std::string FUNC_MOUNT_SHARE_PATH = "mount_share_path";
const std::string FUNC_MOUNT_LOCAL_MOUNT_PATH = "local_mount_path";
const std::string FUNC_MOUNT_STATUS = "status";
const std::string RESOURCE_OWNER_KEY = "resource.owner";
const std::string SANDBOX_RUNTIME_LABEL = "sandbox.runtime";
const std::string SYSTEM_OWNER_VALUE = "system";
const std::string STATIC_FUNCTION_OWNER_VALUE = "static_function";
const std::string DEFAULT_OWNER_VALUE = "default";
const std::string VIRTUAL_TAG = "virtual";
const std::string PRIMARY_TAG = "primary";
const std::string DATASYSTEM_FEATURE_USED = "dataSystemFeatureUsed";
const std::string DATASYSTEM_FEATURE_USED_STREAM = "stream";
const std::string ENABLE_HORIZONTAL_SCALE_KEY = "ENABLE_HORIZONTAL_SCALE";

const std::string DELEGATE_ENCRYPT = "DELEGATE_ENCRYPT";
const std::string DELEGATE_DECRYPT = "DELEGATE_DECRYPT";

const std::string DELEGATE_MOUNT = "DELEGATE_MOUNT";
const std::string DELEGATE_CONTAINER = "DELEGATE_CONTAINER";
const std::string DELEGATE_CONTAINER_ID_KEY = "DELEGATE_CONTAINER_ID";
const std::string ENV_DELEGATE_BOOTSTRAP = "ENV_DELEGATE_BOOTSTRAP";
const std::string ENV_DELEGATE_DOWNLOAD = "ENV_DELEGATE_DOWNLOAD";
const std::string DELEGATE_DOWNLOAD = "DELEGATE_DOWNLOAD";
const std::string DELEGATE_DIRECTORY_INFO = "DELEGATE_DIRECTORY_INFO";
const std::string DELEGATE_DIRECTORY_QUOTA = "DELEGATE_DIRECTORY_QUOTA";
const std::string DELEGATE_SHARED_DIRECTORY = "DELEGATE_SHARED_DIRECTORY";
const std::string DELEGATE_SHARED_DIRECTORY_TTL = "DELEGATE_SHARED_DIRECTORY_TTL";

const std::string AFFINITY_POOL_ID = "AFFINITY_POOL_ID";

const std::string FUNCTION_PROXY_NAME = "function_proxy";
const std::string FUNCTION_MASTER_NAME = "function_master";
const std::string DOMAIN_SCHEDULER_NAME = "domain_scheduler";

const std::string STANDALONE_MODE = "standalone";
const std::string ETCD_ELECTION_MODE = "etcd";
const std::string TXN_ELECTION_MODE = "txn";
const std::string K8S_ELECTION_MODE = "k8s";

const std::string CLUSTER_MODE = "cluster";
const std::string MASTER_BUSINESS = "master";
const std::string SLAVE_BUSINESS = "slave";

const std::string KUBERNETES_SERVICE_HOST = "KUBERNETES_SERVICE_HOST";
const std::string KUBERNETES_SERVICE_PORT = "KUBERNETES_SERVICE_PORT_HTTPS";
const std::string RUNTIME_ENV_PREFIX = "func-";

// observability port
const uint32_t DEFAULT_OBSERVABILITY_AGENT_GRPC_PORT = 4317;
const uint32_t DEFAULT_OBSERVABILITY_PROMETHEUS_PORT = 9392;
const uint32_t DEFAULT_OBSERVABILITY_PUSHGATEWAY_PORT = 9091;

const std::string DS_WORKER_TAINT_KEY = "is-ds-worker-unready";
const std::string FUNCTION_PROXY_TAINT_KEY = "is-function-proxy-unready";

enum EXIT_TYPE : int32_t {
    NONE_EXIT,
    RETURN,
    EXCEPTION_INFO,  // from user function exception log info
    OOM_INFO,        // from dmesg info, oom kill by outer system
    STANDARD_INFO,   // from user function sdtout log info
    UNKNOWN_ERROR,
    KILLED_INFO,                 // after function-proxy state machine set instance FATAL
    RUNTIME_MEMORY_EXCEED_LIMIT  // OOM(RuntimeMemoryExceedLimit) stop by runtime-manager inner
};

const int32_t DEFAULT_USER_ID = 0;
const int32_t DEFAULT_GROUP_ID = 0;
const int32_t DEFAULT_VIRTUAL_ENV_IDLE_TIME_LIMIT = -1;
// default min cpu size of instance (One CPU core corresponds to 1000)
const uint64_t DEFAULT_MIN_INSTANCE_CPU_SIZE = 300;
// max cpu size of instance (One CPU core corresponds to 1000)
const uint64_t DEFAULT_MAX_INSTANCE_CPU_SIZE = 16000;
// default min memory size of instance (MB)
const uint64_t DEFAULT_MIN_INSTANCE_MEMORY_SIZE = 128;
// max memory size of instance (MB)
const uint64_t DEFAULT_MAX_INSTANCE_MEMORY_SIZE = 1024 * 1024 * 1024;
const uint32_t MAX_RETRY_SEND_CLEAN_STATUS_TIMES = 3;
const uint32_t DEFAULT_RETRY_SEND_CLEAN_STATUS_INTERVAL = 5000;

const std::string AFFINITY_SCHEDULE_LABELS = "AFFINITY_SCHEDULE_LABELS";
const std::string INSTANCE_AFFINITY = "instanceAffinity";
const std::string INNER_TENANT_AFFINITY = "innerTenantAffinity";

const int EXIT_COMMAND_MISUSE = 2;
const int EXIT_ABNORMAL = -1;  // 255

const std::string AFFINITY_TOPO_AGENT = "agent";
const std::string AFFINITY_TOPO_NODE = "node";

const std::string HOST_IP_LABEL = "HOST_IP";
const std::string PREFERRED_DATA_AFFINITY = "PreferredDataAffinity";

// tenant
const std::string TENANT_ID = "tenantId";
const int32_t DEFAULT_TENANT_POD_REUSE_TIME_WINDOW = -1;
// This buffer provides a time delay for a local proxy to terminate the scheduling of a POD that is being deleted and
// update the etcd instance creating status.
const int32_t DELETING_POD_STOP_SCHEDULE_TIME_BUFFER = 1;
const std::string YR_TENANT_ID = "YR_TENANT_ID";

const std::string LABEL_AFFINITY_PLUGIN = "LabelAffinitPlugin";
const std::string LABEL_AFFINITY_FILTER_PLUGIN = "LabelAffinityFilterPlugin";
const std::string LOCAL_LABEL_AFFINITY_FILTER_PLUGIN = "LocalLabelAffinityFilterPlugin";
const std::string LABEL_AFFINITY_SCORE_PLUGIN = "LabelAffinityScorePlugin";
const std::string ROOT_LABEL_AFFINITY_SCORE_PLUGIN = "RootLabelAffinityScorePlugin";
const std::string LOCAL_LABEL_AFFINITY_SCORE_PLUGIN = "LocalLabelAffinityScorePlugin";
const std::string GROUP_SCHEDULE_CONTEXT = "GroupScheduleContext";

const std::string DEFAULT_HETEROGENEOUS_FILTER_PLUGIN = "DefaultHeterogeneousFilterPlugin";
const std::string DEFAULT_HETEROGENEOUS_SCORE_PLUGIN = "DefaultHeterogeneousScorePlugin";

const std::string DEFAULT_FILTER_PLUGIN = "DefaultFilterPlugin";
const std::string DEFAULT_SCORE_PLUGIN = "DefaultScorePlugin";

const std::string DISK_FILTER_PLUGIN = "DiskFilterPlugin";
const std::string DISK_SCORE_PLUGIN = "DiskScorePlugin";

const std::string RECOVER_RETRY_TIMES_KEY = "RecoverRetryTimes";
const std::string RECOVER_RETRY_TIMEOUT_KEY = "RECOVER_RETRY_TIMEOUT";
const uint64_t DEFAULT_RECOVER_TIMEOUT_MS = 10 * 60 * 1000;

const uint32_t DEFAULT_ELECT_LEASE_TTL = 10;
const uint32_t DEFAULT_ELECT_OBSERVE_INTERVAL = 2;
const uint32_t DEFAULT_ELECT_KEEP_ALIVE_INTERVAL = 2;
const uint32_t MIN_ELECT_KEEP_ALIVE_INTERVAL = 1;
const uint32_t MAX_ELECT_KEEP_ALIVE_INTERVAL = 60;
const uint32_t MIN_ELECT_LEASE_TTL = 2;
const uint32_t MAX_ELECT_LEASE_TTL = 600;

const std::string SCHEDULE_RETRY_TIMES_KEY = "scheduleRetryTimes";
const uint32_t DEFAULE_SCHEDULE_RETRY_TIMES = 5;

// rate limit
const uint32_t DEFAULT_TENANT_TOKEN_BUCKET_CAPACITY = 1000;

// credential type
const std::string CREDENTIAL_TYPE_ROTATING_CREDENTIALS = "credential_type_rotating_credentials";
const std::string CREDENTIAL_TYPE_PERMANENT_CREDENTIALS = "credential_type_permanent_credentials";

// 24 token rotation
const uint32_t UPDATE_TEMPORARY_ACCESSKEY_INTERVAL_SECONDS = 24 * 60 * 60;  // 24h

// log expiration
const int32_t DEFAULT_LOG_EXPIRATION_CLEANUP_INTERVAL = 10 * 60;         // 10min
const int32_t DEFAULT_LOG_EXPIRATION_TIME_THRESHOLD = 60 * 60 * 24 * 5;  // 5d
const int32_t DEFAULT_LOG_EXPIRATION_MAX_FILE_COUNT = 512;

// unzipped working dir path
const std::string UNZIPPED_WORKING_DIR = "UNZIPPED_WORKING_DIR";
// shared directory
const std::string SHARED_DIRECTORY_PATH = "SHARED_DIRECTORY_PATH";
// origin working dir zip file path
const std::string YR_WORKING_DIR = "YR_WORKING_DIR";
// is job submisson, process started by user: bool, true/false
const std::string YR_APP_MODE = "YR_APP_MODE";

const std::string LOCAL_FUNCTION_META_PATH = "/home/sn/function-metas";

const std::string IAM_CREDENTIAL_TYPE_TOKEN = "token";
const std::string IAM_CREDENTIAL_TYPE_AK_SK = "AK/SK";
const std::string SYSTEM_AUTH_MODE_AK_SK = "AK/SK";

// query debug instance info
const uint64_t QUERY_DEBUG_INSTANCE_INFO_INTERVAL_MS = 3000;

const std::string LITEBUS_DATA_KEY = "LITEBUS_DATA_KEY";
const std::string YR_BUILD_IN_CREDENTIAL = "YR_BUILD_IN_CREDENTIAL";

// memory detection
const int32_t DEFAULT_MEMORY_DETECTION_INTERVAL = 1000;  // ms, 1s
// runtime oom detection
const int32_t DEFAULT_OOM_CONSECUTIVE_DETECTION_COUNT = 3;

// rgroup
const std::string RGROUP = "rgroup";

// aggregate strategy option
const std::string NO_AGGREGATE_STRATEGY = "no_aggregate";
const std::string STRICTLY_AGGREGATE_STRATEGY = "strictly";
const std::string RELAXED_AGGREGATE_STRATEGY = "relaxed";

// runtime remote debug createoption
const std::string YR_DEBUG_CONFIG = "debug_config";

// conda
const std::string CONDA_CONFIG = "CONDA_CONFIG";
const std::string CONDA_COMMAND = "CONDA_COMMAND";
const std::string CONDA_PREFIX = "CONDA_PREFIX";
const std::string CONDA_DEFAULT_ENV = "CONDA_DEFAULT_ENV";

// container
/* Example of rootfs json:
{
    "runtime": "runsc",
    "type": "s3",
    "imageurl": "registry.cn-hangzhou.com/runtime-python3.11:latest",
    "readonly": "true",
    "storageInfo": {
        "endpoint": "cn-hangzhou.alipay.aliyun-inc.com",
        "bucket": "crfs-dev",
        "object": "akernel/test_img/roorfs_202512251644.img"
    }
}
*/
const std::string CONTAINER_ROOTFS = "rootfs";

/* Example of mounts json (array of mount objects):
[
    {
        "type": "bind",
        "target": "/data",
        "host_path": "/mnt/nas/shared-data",
        "options": ["ro"]
    },
    {
        "type": "bind",
        "target": "/models",
        "host_path": "/mnt/models/llama"
    },
    {
        "type": "volume",
        "target": "/cache",
        "s3_config": {
            "endpoint": "cn-hangzhou.alipay.aliyun-inc.com",
            "bucket": "crfs-dev",
            "object": "akernel/cache/cache_v1.img",
            "accessKey": "",
            "secretKey": ""
        }
    },
    {
        "type": "bind",
        "target": "/rootfs",
        "image_url": "registry.cn-hangzhou.com/overlay:latest"
    }
]

Each mount supports one of three source types (mutually exclusive):
  - host_path:  bind mount from host filesystem path
  - s3_config:  S3-based mount source (endpoint, bucket, object, accessKey, secretKey)
  - image_url:  OCI image URL for image-based mount

Fields:
  - type    (string, required): mount type, e.g. "bind", "volume", "tmpfs", "erofs"
  - target  (string, required): mount point inside the container
  - options (array of string, optional): fstab-style mount options, e.g. ["ro"], ["noexec","nosuid"]
*/
const std::string CONTAINER_MOUNTS = "mounts";

const std::string CONTAINER_EXTRA_CONFIG = "extra_config";

const std::string CONTAINER_NETWORK = "network";

const std::string CONTAINER_NETWORK_POLICY = "network_policy";
const std::string SCHEDULER_ELASTIC_RESOURCES = "scheduler_elastic_resources";

const std::string CONTAINER_INHERIT_ENTRYPOINT = "inherit_entrypoint";

// port forwarding internal env vars
const std::string YR_INTERNAL_HOST_IP = "YR_INTERNAL_HOST_IP";
const std::string YR_PORT_FORWARDINGS = "YR_PORT_FORWARDINGS";

const std::string DEFAULT_DOCKER_SOCKET = "/var/run/docker.sock";
const std::string POSIX_LISTEN_ADDR = "POSIX_LISTEN_ADDR";

// hibernate
const std::string ENABLE_SUSPEND_RESUME = "enableSuspendResume";
const std::string IDLE_TO_SUSPEND = "idleToSuspend";

// system function
const std::string FRONTEND_FUNCTION_NAME = "0-system-faasfrontend";

// virtual env
const std::string VIRTUALENV_KIND = "VIRTUALENV_KIND";
const std::string VIRTUALENV_NAME = "VIRTUALENV_NAME";
const std::string VIRTUALENV_COMMAND = "VIRTUALENV_COMMAND";
const std::string VIRTUALENV_PATH = "VIRTUALENV_PATH";

const std::vector<std::string> VIRTUALENV_KEYS = {VIRTUALENV_KIND, VIRTUALENV_NAME, VIRTUALENV_COMMAND,
                                                  VIRTUALENV_PATH};

// deploy options
const std::string EXEC_PATH = "exec_path";
const std::string HOST_USER = "host_user";
const std::string CONTAINER_ENV_VARS = "env_vars";

// runtime log redirect control: when "true", yr runtime emits logs to stdout only (the executor
// redirects stdout/stderr to per-runtime host files via sh -c "... >out 2>err").
const std::string YR_ONLY_STDOUT = "YR_ONLY_STDOUT";

// sandboxType values (function meta sandboxType field)
const std::string SANDBOX_TYPE_DOCKER = "docker";
const std::string SANDBOX_TYPE_SUPERVISOR = "supervisor";
// conch: microVM sandbox via jiuwenbox's conch backend (PR !4817), same jiuwenbox.sock.
const std::string SANDBOX_TYPE_CONCH = "conch";
}  // namespace functionsystem
#endif  // COMMON_CONSTANTS_CONSTANTS_H
