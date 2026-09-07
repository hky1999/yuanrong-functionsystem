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

#ifndef FUNCTION_PROXY_COMMON_POSIX_CLIENT_SHARED_CLIENT_SHARED_CLIENT_H
#define FUNCTION_PROXY_COMMON_POSIX_CLIENT_SHARED_CLIENT_SHARED_CLIENT_H

#include "function_proxy/common/posix_client/control_plane_client/control_interface_posix_client.h"
#include "function_proxy/common/posix_client/data_plane_client/data_interface_posix_client.h"

namespace functionsystem {
class SharedClient : public DataInterfacePosixClient, public ControlInterfacePosixClient {
public:
    // The virtual BaseClient base is initialized here (the intermediate-base
    // initializations are ignored), so the runtime binding is set before the
    // SharedClient is published through InsertClient.
    explicit SharedClient(const std::shared_ptr<grpc::PosixClient> &posix, const std::string &runtimeID = "")
        : BaseClient(posix, runtimeID), DataInterfacePosixClient(posix), ControlInterfacePosixClient(posix)
    {
    }
    ~SharedClient() override = default;
};
}  // namespace functionsystem

#endif  // FUNCTION_PROXY_COMMON_POSIX_CLIENT_SHARED_CLIENT_SHARED_CLIENT_H
