#ifndef FUNCTIONSYSTEM_MEMORY_ELASTICITY_CONFIG_H
#define FUNCTIONSYSTEM_MEMORY_ELASTICITY_CONFIG_H

#include <cstdlib>
#include <string_view>

namespace functionsystem::resource_view {
// Explicit process-start opt-in. This is independent of the legacy ladder name.
inline bool MemoryElasticityEnabled()
{
    static const bool enabled = [] {
        const char *value = std::getenv("AKERNEL_MEMORY_ELASTICITY");
        return value != nullptr && std::string_view(value) == "1";
    }();
    return enabled;
}
constexpr int MEMORY_ELASTIC_STATS_INTERVAL_MS = 200;
}
#endif
