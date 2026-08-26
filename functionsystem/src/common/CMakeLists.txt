# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
project(common)

option(ENABLE_NUMA "Enable NUMA support" ON)

configure_file(
    ${CMAKE_CURRENT_LIST_DIR}/utils/version.h.in
    ${CMAKE_CURRENT_LIST_DIR}/utils/version.h
    @ONLY
)

set_property(GLOBAL PROPERTY UTILS_INCLUDE "${CMAKE_CURRENT_LIST_DIR}")
include_directories(${CMAKE_CURRENT_LIST_DIR}/metadata)
include_directories(${CMAKE_CURRENT_LIST_DIR})
add_subdirectory(proto)
add_subdirectory(status)
add_subdirectory(heartbeat)
add_subdirectory(metrics)
add_subdirectory(rpc)
add_subdirectory(common_flags)
add_subdirectory(http)
add_subdirectory(aksk)
add_subdirectory(crypto)
add_subdirectory(explorer)
add_subdirectory(file_monitor)
add_subdirectory(hex)
add_subdirectory(leader)
add_subdirectory(meta_store_adapter)
add_subdirectory(metadata)
add_subdirectory(network)
add_subdirectory(profile)
add_subdirectory(register)
add_subdirectory(resource_view)
add_subdirectory(scheduler_topology)
add_subdirectory(service_json)
add_subdirectory(kube_client)
add_subdirectory(resource_lock)
add_subdirectory(kv_client)
add_subdirectory(file_storage)
add_subdirectory(trace)
add_subdirectory(lru)

#add_subdirectory(redis_client)


add_library(utils STATIC "")
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/utils FS_UTILS_SRCS)
if(NOT ENABLE_NUMA)
    list(REMOVE_ITEM FS_UTILS_SRCS "${CMAKE_CURRENT_LIST_DIR}/utils/numa_dl.cpp")
endif()
add_dependencies(utils status metadata)
target_sources(utils PRIVATE ${FS_UTILS_SRCS})
target_link_libraries(utils PUBLIC ${crypto_LIB} crypto)
target_link_libraries(utils PUBLIC ${ssl_LIB})
target_link_libraries(utils PUBLIC status)
if(ENABLE_NUMA)
    target_compile_definitions(utils PRIVATE ENABLE_NUMA)
    target_link_libraries(utils PRIVATE ${CMAKE_DL_LIBS})
endif()
target_link_libraries(utils PUBLIC cap)
target_link_libraries(utils PUBLIC metadata)

add_library(scheduler STATIC "")
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/scheduler SCHEDULER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_decision SCHEDULER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_decision/scheduler SCHEDULER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_decision/scheduler/priority_policy SCHEDULER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_decision/schedule_recorder SCHEDULER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_decision/queue SCHEDULER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_decision/performer SCHEDULER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_decision/preemption_controller SCHEDULER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/scheduler_framework SCHEDULER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/scheduler_framework/framework SCHEDULER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/scheduler_framework/utils SCHEDULER_SRCS)
add_dependencies(scheduler status posix_pb)
target_sources(scheduler PRIVATE ${SCHEDULER_SRCS})
target_link_libraries(scheduler PUBLIC status posix_pb)

add_library(schedule_plugin_common STATIC "")
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_plugin/common PLUGIN_COMMON_SRCS)
add_dependencies(schedule_plugin_common status posix_pb resource_view)
target_sources(schedule_plugin_common PRIVATE ${PLUGIN_COMMON_SRCS})
target_link_libraries(schedule_plugin_common PUBLIC status posix_pb resource_view)

add_subdirectory(yaml_tool)
add_subdirectory(schedule_plugin/prefilter/default_prefilter)
add_subdirectory(schedule_plugin/filter/default_filter)
add_subdirectory(schedule_plugin/filter/resource_selector_filter)
add_subdirectory(schedule_plugin/filter/usage_aware_filter)
add_subdirectory(schedule_plugin/filter/overcommit_filter)
add_subdirectory(schedule_plugin/scorer/default_scorer)
add_subdirectory(schedule_plugin/filter/default_heterogeneous_filter)
add_subdirectory(schedule_plugin/scorer/default_heterogeneous_scorer)
add_subdirectory(schedule_plugin/filter/label_affinity_filter)
add_subdirectory(schedule_plugin/scorer/label_affinity_scorer)
add_subdirectory(schedule_plugin/filter/disk_filter)
add_subdirectory(schedule_plugin/scorer/disk_scorer)
add_subdirectory(schedule_plugin/filter/numa_affinity_filter)
add_subdirectory(schedule_plugin/scorer/numa_affinity_scorer)

set(PLUGIN
        -Wl,--whole-archive
        default_prefilter default_filter resource_selector_filter default_scorer default_heterogeneous_filter
        default_heterogeneous_scorer label_affinity_filter label_affinity_scorer disk_filter disk_scorer
        numa_affinity_filter numa_affinity_scorer usage_aware_filter overcommit_filter
        -Wl,--no-whole-archive)

set_property(GLOBAL PROPERTY PLUGIN_LIB "${PLUGIN}")
