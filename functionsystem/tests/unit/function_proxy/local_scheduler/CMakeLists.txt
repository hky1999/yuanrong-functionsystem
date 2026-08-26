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

add_subdirectory(local_scheduler_service)
add_subdirectory(instance_control)
add_subdirectory(function_agent_manager)
add_subdirectory(grpc_server)
add_subdirectory(abnormal_processor)
add_subdirectory(ds_healthy_checker)
add_subdirectory(local_group_ctrl)
add_subdirectory(bundle_manager)
add_subdirectory(resource_group_controller)
add_subdirectory(subscription_manager)
add_subdirectory(gc_actor)
add_subdirectory(pressure_monitor)

target_sources(${UNIT_TEST_MODULE} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/local_sched_driver_test.cpp)

get_property(SRC_LOCAL_SCHEDULER_INCLUDE_DIR GLOBAL PROPERTY "SRC_LOCAL_SCHEDULER_INCLUDE_DIR")

target_include_directories(${UNIT_TEST_MODULE} PRIVATE ${SRC_LOCAL_SCHEDULER_INCLUDE_DIR})