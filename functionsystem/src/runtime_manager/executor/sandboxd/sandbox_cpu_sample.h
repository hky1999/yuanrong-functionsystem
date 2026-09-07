// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
#ifndef RUNTIME_MANAGER_EXECUTOR_SANDBOXD_SANDBOX_CPU_SAMPLE_H
#define RUNTIME_MANAGER_EXECUTOR_SANDBOXD_SANDBOX_CPU_SAMPLE_H
#include <cstdint>
#include <optional>
namespace functionsystem::runtime_manager {
// Stats targets one second. Do not label an arbitrarily long average as a
// current CPU observation after actor stalls. Failed polls also reset the
// executor baseline, independently of this bound.
inline std::optional<double> SandboxCpuCores(uint64_t previousNs, uint64_t currentNs, int64_t windowNs)
{
    constexpr int64_t maxWindowNs = 3000000000LL;
    if (windowNs <= 0 || windowNs > maxWindowNs || currentNs < previousNs) {
        return std::nullopt;
    }
    return static_cast<double>(currentNs - previousNs) / static_cast<double>(windowNs);
}
}
#endif
