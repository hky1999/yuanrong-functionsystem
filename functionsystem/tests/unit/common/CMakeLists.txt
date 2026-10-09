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
include_directories(${CMAKE_CURRENT_LIST_DIR}) # call in the module

aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/meta_storage_accessor
                     META_STORAGE_ACCESSOR_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/meta_store_client
                     META_STORE_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/metadata META_DATA_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/etcd_service
                     META_STORE_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/status STATUS_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/utils UTILS_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/rpc RPC)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/rpc/client RPC)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/rpc/stream RPC)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/heartbeat HEARTBEAT)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/resource_view RESOURCE_VIEW_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/scheduler_topology
                     SCHEDULER_TOPOLOGY_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_framework
                     SCHEDULE_FRAMEWORK_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_decision SCHEDULE_DECISION_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/register REGISTER_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/yaml_tool YAML_TOOL_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/service_json SERVICE_JSON_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/http HTTP_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/metrics METRICS_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/kube_client KUBE_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/common_flags COMMON_FLAGS_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/leader LEADER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/explorer EXPLORER_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/meta_store_adapter
                     META_STORE_ADAPTER_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/observability
                     OBSERVABILITY_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/file_monitor FILE_MONITOR_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/network NETWORK_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/resource_lock RESOURCE_LOCK_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/aksk AKSK_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/profile PROFILE_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/hex HEX_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/lru LRU_TEST_SRCS)

aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_plugin/prefilter SCHEDULE_PLUGIN_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_plugin/filter SCHEDULE_PLUGIN_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/schedule_plugin/scorer SCHEDULE_PLUGIN_SRCS)

target_sources(
  ${UNIT_TEST_MODULE}
  PRIVATE ${STATUS_TEST_SRCS}
          ${LOGGER_TEST_SRCS}
          ${META_STORAGE_ACCESSOR_TEST_SRCS}
          ${META_DATA_TEST_SRCS}
          ${META_STORE_TEST_SRCS}
          ${UTILS_TEST_SRCS}
          ${RPC}
          ${HEARTBEAT}
          ${RESOURCE_VIEW_SRCS}
          ${SCHEDULER_TOPOLOGY_SRCS}
          ${SCHEDULE_FRAMEWORK_SRCS}
          ${SCHEDULE_DECISION_SRCS}
          ${ZIP_TEST_SRCS}
          ${REGISTER_TEST_SRCS}
          ${YAML_TOOL_SRCS}
          ${SERVICE_JSON_SRCS}
          ${TRACE_SRCS}
          ${HTTP_TEST_SRCS}
          ${METRICS_TEST_SRCS}
          ${KUBE_TEST_SRCS}
          ${COMMON_FLAGS_SRCS}
          ${LEADER_SRCS}
          ${EXPLORER_SRCS}
          ${STS_TEST_SRCS}
          ${META_STORE_ADAPTER_TEST_SRCS}
          ${OBSERVABILITY_TEST_SRCS}
          ${FILE_MONITOR_TEST_SRCS}
          ${NETWORK_TEST_SRCS}
          ${RESOURCE_LOCK_SRCS}
          ${AKSK_TEST_SRCS}
          ${PROFILE_TEST_SRCS}
          ${HEX_TEST_SRCS}
          ${SCHEDULE_PLUGIN_SRCS}
          ${LRU_TEST_SRCS})

# Keep the port-forward grammar regression independently runnable.  The main
# functionsystem_unit_test target pulls in every subsystem and is needlessly
# expensive for this boundary-only parser.
add_executable(port_forward_mapping_test_bin
  ${PROJECT_SOURCE_DIR}/tests/unit/main.cpp
  ${CMAKE_CURRENT_LIST_DIR}/utils/port_forward_mapping_test.cpp)
target_compile_options(port_forward_mapping_test_bin PRIVATE
  -Wno-error
  -fno-access-control
  -Wno-missing-field-initializers)
target_include_directories(port_forward_mapping_test_bin PRIVATE
  ${PROJECT_SOURCE_DIR}/src
  ${gtest_INCLUDE_DIR})
target_link_libraries(port_forward_mapping_test_bin PRIVATE
  utils
  ${litebus_ALL_LIB}
  ${gtest_LIB_A}
  ${gmock_LIB_A}
  pthread)
add_test(NAME port_forward_mapping_test_bin COMMAND port_forward_mapping_test_bin)

# Run etcd snapshot regressions without linking every scheduler and runtime test.
add_executable(etcd_prefix_read_test_bin
  ${PROJECT_SOURCE_DIR}/tests/unit/main.cpp
  ${CMAKE_CURRENT_LIST_DIR}/meta_store_client/kv_client_test.cpp
  ${CMAKE_CURRENT_LIST_DIR}/meta_store_client/prefix_range_cursor_test.cpp
  ${CMAKE_CURRENT_LIST_DIR}/meta_storage_accessor/meta_storage_accessor_test.cpp)
add_dependencies(etcd_prefix_read_test_bin gtest_1_10_0)
target_compile_options(etcd_prefix_read_test_bin PRIVATE
  -Wno-error -fno-access-control -Wno-missing-field-initializers)
target_include_directories(etcd_prefix_read_test_bin PRIVATE
  ${PROJECT_SOURCE_DIR}/src
  ${PROJECT_SOURCE_DIR}/src/meta_store/client/cpp/include
  ${gtest_INCLUDE_DIR})
target_link_libraries(etcd_prefix_read_test_bin PRIVATE
  meta_store_client utils
  ${grpcpp_LIB} ${gpr_LIB} ${crypto_LIB} ${ssl_LIB} ${zlib_LIB}
  ${litebus_ALL_LIB} ${gtest_LIB_A} ${gmock_LIB_A} pthread)
add_test(NAME etcd_prefix_read_test_bin COMMAND etcd_prefix_read_test_bin)
