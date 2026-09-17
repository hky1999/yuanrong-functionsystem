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

#include "elastic_options.h"

#include <cmath>
#include <string>

#include <nlohmann/json.hpp>

#include "common/constants/constants.h"

namespace functionsystem::resource_view {
namespace {

// Accepted boolean spellings for the elastic marker. The SDK sends strings;
// a JSON bool is normalized to the same spelling so wheel and hand-written
// clients stay equivalent.
Status CoerceElasticValue(const nlohmann::json &value, std::string &flag)
{
    if (value.is_string()) {
        flag = value.get<std::string>();
    } else if (value.is_boolean()) {
        flag = value.get<bool>() ? "true" : "false";
    } else {
        return Status(StatusCode::ERR_PARAM_INVALID,
                      "elastic resources require a true/false marker inside extra_config");
    }
    if (flag != "true" && flag != "false") {
        return Status(StatusCode::ERR_PARAM_INVALID, "elastic resources require true and false spellings");
    }
    return Status::OK();
}

}  // namespace

Status ValidateElasticResources(const InstanceInfo &instance)
{
    const auto &resources = instance.resources().resources();
    for (const auto &name : { CPU_RESOURCE_NAME, MEMORY_RESOURCE_NAME }) {
        auto it = resources.find(name);
        if (it == resources.end() || it->second.type() != ValueType::Value_Type_SCALAR
            || !it->second.has_scalar()) {
            return Status(StatusCode::ERR_PARAM_INVALID,
                          "elastic resources require explicit CPU and Memory scalars");
        }
        const double initial = it->second.scalar().value();
        const double limit = it->second.scalar().limit();
        const double ceiling = limit == 0 ? initial : limit;
        if (!std::isfinite(initial) || !std::isfinite(ceiling) || initial <= 0 || ceiling < initial
            || std::floor(initial) != initial || std::floor(ceiling) != ceiling) {
            return Status(StatusCode::ERR_PARAM_INVALID,
                          "elastic resource limits must be positive integers with ceiling >= initial");
        }
    }
    return Status::OK();
}

Status NormalizeElasticCreateOptions(InstanceInfo *instance, bool *elastic, ElasticOptionOrigin *origin)
{
    if (elastic != nullptr) {
        *elastic = false;
    }
    if (origin != nullptr) {
        *origin = ElasticOptionOrigin::NONE;
    }
    if (instance == nullptr) {
        return Status(StatusCode::ERR_PARAM_INVALID, "elastic create options require an instance");
    }
    auto options = instance->mutable_createoptions();
    const auto flat = options->find(SCHEDULER_ELASTIC_RESOURCES);
    const auto extra = options->find(CONTAINER_EXTRA_CONFIG);
    bool surfaced = false;  // we added the flat key from the nested marker
    if (extra == options->end()) {
        if (flat == options->end()) {
            return Status::OK();  // no elastic intent: legacy contract unchanged
        }
        if (flat->second != "true" && flat->second != "false") {
            return Status(StatusCode::ERR_PARAM_INVALID, "elastic resources require true and false spellings");
        }
    } else {
        // Parse the opaque extra_config JSON only to look for the elastic
        // marker; a non-JSON payload keeps its original semantics.
        try {
            auto parsed = nlohmann::json::parse(extra->second);
            const auto nested = parsed.is_object()
                ? parsed.find(SCHEDULER_ELASTIC_RESOURCES) : parsed.end();
            if (nested != parsed.end()) {
                std::string flag;
                auto status = CoerceElasticValue(*nested, flag);
                if (!status.IsOk()) {
                    return status;
                }
                if (flat != options->end() && flat->second != flag) {
                    return Status(StatusCode::ERR_PARAM_INVALID,
                                  "elastic resources marker conflicts between create options and extra_config");
                }
                if (flat == options->end()) {
                    (*options)[SCHEDULER_ELASTIC_RESOURCES] = flag;
                    surfaced = true;
                }
            } else if (flat == options->end()) {
                return Status::OK();  // no elastic intent inside extra_config
            } else if (flat->second != "true" && flat->second != "false") {
                return Status(StatusCode::ERR_PARAM_INVALID, "elastic resources require true and false spellings");
            }
        } catch (const nlohmann::json::parse_error &) {
            if (flat == options->end()) {
                return Status::OK();  // opaque non-JSON payload: unchanged
            }
            if (flat->second != "true" && flat->second != "false") {
                return Status(StatusCode::ERR_PARAM_INVALID, "elastic resources require true and false spellings");
            }
        }
    }
    const auto canonical = options->find(SCHEDULER_ELASTIC_RESOURCES);
    const bool enabled = canonical != options->end() && canonical->second == "true";
    if (enabled) {
        // Reject unusable elastic requests at the schedule entry instead of
        // letting them fail at physical birth after admission. Roll back only
        // the key this call surfaced; caller-provided options stay as given.
        auto status = ValidateElasticResources(*instance);
        if (!status.IsOk()) {
            if (surfaced) {
                options->erase(SCHEDULER_ELASTIC_RESOURCES);
            }
            return status;
        }
    }
    if (elastic != nullptr) {
        *elastic = enabled || (canonical != options->end());
    }
    if (origin != nullptr) {
        if (canonical == options->end()) {
            *origin = ElasticOptionOrigin::NONE;
        } else {
            *origin = (flat == options->end()) ? ElasticOptionOrigin::NESTED_EXTRA_CONFIG
                                               : ElasticOptionOrigin::FLAT;
        }
    }
    return Status::OK();
}

}  // namespace functionsystem::resource_view
