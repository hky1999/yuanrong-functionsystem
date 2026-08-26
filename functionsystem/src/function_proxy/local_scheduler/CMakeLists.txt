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

project(local_scheduler)

set_property(GLOBAL PROPERTY SRC_LOCAL_SCHEDULER_INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}")

add_library(local_scheduler_lib STATIC "")
target_compile_options(local_scheduler_lib PRIVATE -fPIE)

target_include_directories(local_scheduler_lib PUBLIC ${CMAKE_CURRENT_LIST_DIR})

add_subdirectory(function_agent_manager)
add_subdirectory(instance_control)
add_subdirectory(local_scheduler_service)
add_subdirectory(grpc_server)
add_subdirectory(abnormal_processor)
add_subdirectory(ds_healthy_checker)
add_subdirectory(debug_instance_info_monitor)
add_subdirectory(local_group_ctrl)
add_subdirectory(bundle_manager)
add_subdirectory(resource_group_controller)
add_subdirectory(subscription_manager)
add_subdirectory(snap_ctrl)
add_subdirectory(traefik_registry)
add_subdirectory(gc_actor)
add_subdirectory(pressure_monitor)

aux_source_directory(${CMAKE_CURRENT_LIST_DIR} LOCAL_SCHEDULER_SRC)

target_sources(local_scheduler_lib PRIVATE ${LOCAL_SCHEDULER_SRC})

add_dependencies(local_scheduler_lib http heartbeat meta_store_client explorer scheduler metadata traefik_registry_lib)
target_link_libraries(local_scheduler_lib http heartbeat meta_store_client explorer scheduler metadata traefik_registry_lib)
