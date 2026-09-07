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
#ifndef METADATA_TYPE_H
#define METADATA_TYPE_H

#include <map>
#include <nlohmann/json.hpp>
#include <string>
#include <utils/os_utils.hpp>
#include <vector>

#include "common/constants/constants.h"
#include "common/resource_view/resource_type.h"

#include "common/status/status.h"
#include "meta_store_client/meta_store_struct.h"
#include "common/constants/warmup_type.h"

namespace functionsystem {
using namespace functionsystem::resource_view;

const std::string LOCAL_STORAGE_TYPE = "local";
const std::string S3_STORAGE_TYPE = "s3";
const std::string COPY_STORAGE_TYPE = "copy";
const std::string WORKING_DIR_STORAGE_TYPE = "working_dir";
const std::string SHARED_DIR_STORAGE_TYPE = "shared_dir";
const std::string DEPLOY_DIR = "/dcache";
const uint8_t VERSION_POSITION = 1;       // function version position from back to front
const uint8_t FUNCTIONNAME_POSITION = 2;  // function name position from back to front
const uint8_t TENANT_POSITION = 3;        // function tenant position from back to front

/**
 * Specifies the instance reliability type. The instance status persistence is optimized.
 * high: persistent all statuses of instance. default value.
 * low: persistent partial status of instance.
 */
const std::string RELIABILITY_TYPE = "ReliabilityType";  // NOLINT

const std::string IDLE_TIMEOUT = "idle_timeout";  // NOLINT

struct ProxyServiceMeta {
    std::string grpcAddress;
    std::string tcpTunnelAddress;
    std::vector<std::string> capabilities;
    std::string version;
    std::string health;
};

struct ProxyMeta {
    std::string node;
    std::string aid;
    std::string ak;
    ProxyServiceMeta proxyService;
};

struct InstanceResource {
    std::string cpu;
    std::string memory;
    std::map<std::string, std::string> customResources;
};

struct InstanceLimitResource {
    uint64_t minCpu = DEFAULT_MIN_INSTANCE_CPU_SIZE;
    uint64_t minMemory = DEFAULT_MIN_INSTANCE_MEMORY_SIZE;
    uint64_t maxCpu = DEFAULT_MAX_INSTANCE_CPU_SIZE;
    uint64_t maxMemory = DEFAULT_MAX_INSTANCE_MEMORY_SIZE;
};

struct FuncMetaData {
    std::string urn;
    std::string runtime;  // Language
    std::string handler;
    std::string codeSha256;
    std::string codeSha512;
    std::string entryFile;
    std::map<std::string, std::string> hookHandler;
    std::string name;
    std::string version;
    std::string tenantId;
    std::string revisionId;
    bool isSystemFunc{ false };
    // static function add
    uint32_t timeout = 0;
    std::string staticHandler = "";
};

struct Layer {
    std::string appID;
    std::string bucketID;
    std::string objectID;
    std::string bucketURL;
    std::string sha256;
    std::string sha512;
    std::string hostName;
    std::string securityToken;
    std::string temporaryAccessKey;
    std::string temporarySecretKey;
    std::string storageType;
    std::string codePath;
};

struct CodeMetaData {
    std::string storageType;
    std::string bucketID;
    std::string objectID;
    std::string bucketUrl;
    std::vector<Layer> layers;
    std::string deployDir;
    std::string sha512;
    std::string appId;
};

struct EnvMetaData {
    std::string envKey;
    std::string envInfo;  // environment
    std::string encryptedUserData;
    std::string cryptoAlgorithm;
};

struct InstanceMetaData {
    int32_t maxInstance;
    int32_t minInstance;
    int32_t concurrentNum;
    int32_t cacheInstance;
    int32_t diskLimit;
    std::string scalePolicy;
};

struct MountUser {
    int32_t userID;
    int32_t groupID;
};

struct FuncMount {
    std::string mountType;
    std::string mountResource;
    std::string mountSharePath;
    std::string localMountPath;
    std::string status;
};

struct MountConfig {
    MountUser mountUser;
    std::vector<FuncMount> funcMounts;
};

struct BootstrapMetaData {
    std::string type;
    std::string root;
    std::string entrypoint;
    std::string cmd;
};

struct DeviceMetaData {
    float hbm = 0;
    float latency = 0;
    uint32_t stream = 0;
    uint32_t count = 0;
    std::string model;
    std::string type;
};

struct RootfsStorageInfo {
    std::string endpoint;
    std::string bucket;
    std::string object;
    std::string accessKey;
    std::string secretKey;
};

enum class RootfsSrcType {
    S3 = 0,
    IMAGE = 1,
    LOCAL = 2,
    INVALID = 255,
};

inline RootfsSrcType StringToRootfsSrcType(const std::string &str)
{
    static std::unordered_map<std::string, RootfsSrcType> rootfsSrcTypeMap = {
        { "s3", RootfsSrcType::S3 },
        { "image", RootfsSrcType::IMAGE },
        { "local", RootfsSrcType::LOCAL },
        { "invalid", RootfsSrcType::INVALID }
    };
    if (auto iter = rootfsSrcTypeMap.find(str); iter != rootfsSrcTypeMap.end()) {
        return iter->second;
    }
    return RootfsSrcType::INVALID;
}

struct RootfsSpecMeta {
    // for runtime etc: runsc, runc ...
    std::string runtime;
    // for rootfs source type
    RootfsSrcType type;
    // to define whether the rootfs is readonly
    bool readonly;

    // for s3 storage info
    RootfsStorageInfo storageInfo;
    // for custom image url
    std::string imageurl;
    // for local path
    std::string path;

    // for mount point
    std::string mountpoint;

    // for container run-as user (sourced from function meta rootfs.user)
    std::string user;
    // for container port forwardings (sourced from function meta rootfs.ports, e.g. "tcp:22")
    std::vector<std::string> ports;
};

struct Initializer {
    std::string handler;
    uint32_t timeout;
};

struct UserAgency {
    std::string accessKey;
    std::string secretKey;
    std::string token;
    std::string securityAk;
    std::string securitySk;
    std::string securityToken;
};

struct CustomGracefulShutdown {
    uint32_t maxShutdownTimeout;
};

struct ExtendedMetaData {
    InstanceMetaData instanceMetaData;
    MountConfig mountConfig;
    DeviceMetaData deviceMetaData;
    Initializer initializer;
    UserAgency userAgency;
    CustomGracefulShutdown customGracefulShutdown;
};

inline WarmupType StringToWarmupType(const std::string &str)
{
    static std::unordered_map<std::string, WarmupType> warmupTypeMap = {
        { "seed", WarmupType::SEED },
        { "preload",  WarmupType::PRELOAD },
        { "none", WarmupType::NONE }
    };
    if (auto iter = warmupTypeMap.find(str); iter != warmupTypeMap.end()) {
        return iter->second;
    }
    return WarmupType::INVALID;
}

struct FunctionMeta {
    WarmupType warmup;
    FuncMetaData funcMetaData;
    CodeMetaData codeMetaData;
    EnvMetaData envMetaData;
    resource_view::Resources resources;
    ExtendedMetaData extendedMetaData;
    InstanceMetaData instanceMetaData;
    RootfsSpecMeta rootfs;
    BootstrapMetaData bootstrap;
    std::string sandboxType;
    std::string rawJsonStr;
};

struct StoreInfo {
    std::string key;
    std::string value;

    StoreInfo(const std::string &k, const std::string &v) : key(k), value(v)
    {
    }

    StoreInfo()
    {
    }
};

struct SyncResult {
    Status status;
};

using SyncerFunction = std::function<litebus::Future<SyncResult>(const std::shared_ptr<GetResponse> &)>;
using ResponseHandler = std::function<litebus::Future<Status>(const std::shared_ptr<GetResponse> &)>;

}  // namespace functionsystem
#endif // METADATA_TYPE_H
