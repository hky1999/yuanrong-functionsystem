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

#ifndef FUNCTIONSYSTEM_RESTORE_ADMISSION_H
#define FUNCTIONSYSTEM_RESTORE_ADMISSION_H
#include "resource_type.h"
#include "common/status/status.h"

namespace functionsystem::resource_view {
// Shared settings live with resource admission, not in the optional plugin library.
struct UsageAdmissionSettings { double safety; double floorMb; int32_t maxInstances; };
void SetUsageAdmissionSettings(double safety, double floorMb, int32_t maxInstances);
UsageAdmissionSettings GetUsageAdmissionSettings();
bool UsesElasticAdmission(const InstanceInfo &instance);
struct AdmissionReserve { double cpuMillicores = 0; double memoryMb = 0; };
Status CheckResourceHeadroom(const ResourceUnit &unit, const AdmissionReserve &extra);
Status CheckRestoreAllocation(const InstanceInfo &instance, const ResourceUnit &unit,
                              const AdmissionReserve &extra = {});
}
#endif
