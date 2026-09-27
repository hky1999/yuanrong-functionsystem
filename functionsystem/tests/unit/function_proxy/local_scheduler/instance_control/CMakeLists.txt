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

aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/posix_api_handler POSIX_API_HANDLER_TEST_SRC)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR} INSTANCE_CONTROL_TEST_SRCS)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/idle IDLE_TEST_SRCS)

target_sources(${UNIT_TEST_MODULE} PRIVATE ${INSTANCE_CONTROL_TEST_SRCS} ${POSIX_API_HANDLER_TEST_SRC} ${IDLE_TEST_SRCS})

# Dedicated ACTUALLY-BUILT target for the reusable-snapshot intent gate
# (pc-snapshot-8b97-2): real-entry tests through DoAuthorizeCreate plus
# helper-level supplements. Deliberately independent of the legacy
# DISABLED_ actor-test fixture and its unverifiable host target.
add_executable(instance_ctrl_intent_gate_test
        ${CMAKE_CURRENT_LIST_DIR}/../../../main.cpp
        instance_ctrl_intent_gate_test.cpp)
target_compile_options(instance_ctrl_intent_gate_test PRIVATE
        -Wno-error
        -fno-access-control
        -Wno-missing-field-initializers)
target_include_directories(instance_ctrl_intent_gate_test PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
        ${CMAKE_CURRENT_LIST_DIR}/../../..          # tests/unit (mocks/)
        ${gtest_INCLUDE_DIR}
        ${SRC_INCLUDE_DIR}
        ${SRC_BUSPROXY_INCLUDE_DIR}
        ${SRC_LOCAL_SCHEDULER_INCLUDE_DIR}
        ${SRC_FUNCTION_PROXY_INCLUDE_DIR}
        ${GLOBAL_SCHEDULER_DIR}
        ${INSTANCE_MANAGER_DIR}
        ${SRC_RUNTIME_MANAGER_INCLUDE_DIR})
target_link_libraries(instance_ctrl_intent_gate_test PRIVATE
        ${crypto_LIB}
        ${ssl_LIB}
        ${zlib_LIB}
        ${protobuf_LIB}
        ${grpcpp_LIB}
        ${gpr_LIB}
        ${etcdapi_LIB_A}
        ${securec_LIB}
        ${gtest_LIB_A}
        ${gmock_LIB_A}
        ${litebus_ALL_LIB}
        ${INSTANCE_MANAGER_LIB}
        ${SNAP_MANAGER_LIB}
        ${GLOBAL_SCHEDULER_LIB}
        snapshot_storage
        busproxy_lib
        local_scheduler_lib
        function_proxy_common_lib
        runtime_manager_lib
        ${META_STORE_LIB}
        scheduler
        schedule_plugin_common
        pthread)
add_test(NAME instance_ctrl_intent_gate_test COMMAND instance_ctrl_intent_gate_test)
