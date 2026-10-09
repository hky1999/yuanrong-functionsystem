/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FUNCTIONSYSTEM_META_STORE_PREFIX_RANGE_CURSOR_H
#define FUNCTIONSYSTEM_META_STORE_PREFIX_RANGE_CURSOR_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace functionsystem::meta_store {
// Only advances after a validated page. The first revision is the snapshot
// revision; subsequent response headers may describe a newer cluster revision.
class PrefixRangeCursor {
public:
    PrefixRangeCursor(std::string prefix, std::string rangeEnd, uint32_t pageSize)
        : prefix_(std::move(prefix)), rangeEnd_(std::move(rangeEnd)), key_(prefix_), pageSize_(pageSize)
    {
    }

    bool Advance(int64_t revision, uint64_t clusterId, const std::vector<std::string> &keys, bool more)
    {
        if (pageSize_ == 0 || keys.size() > pageSize_ || revision <= 0
            || (revision_ != 0 && (revision < revision_ || clusterId != clusterId_))
            || (more && keys.empty())) {
            return false;
        }
        std::string previous;
        for (const auto &key : keys) {
            if (key < key_ || key.compare(0, prefix_.size(), prefix_) != 0
                || (rangeEnd_ != std::string(1, '\0') && key >= rangeEnd_)
                || (!previous.empty() && key <= previous)) {
                return false;
            }
            previous = key;
        }
        if (revision_ == 0) {
            revision_ = revision;
            clusterId_ = clusterId;
        }
        if (more) {
            key_ = keys.back() + '\0';
        }
        return true;
    }

    const std::string &Key() const
    {
        return key_;
    }

    int64_t Revision() const
    {
        return revision_;
    }

private:
    const std::string prefix_;
    const std::string rangeEnd_;
    std::string key_;
    const uint32_t pageSize_;
    int64_t revision_ = 0;
    uint64_t clusterId_ = 0;
};
}  // namespace functionsystem::meta_store
#endif  // FUNCTIONSYSTEM_META_STORE_PREFIX_RANGE_CURSOR_H
