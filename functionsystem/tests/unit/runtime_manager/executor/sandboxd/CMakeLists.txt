# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
aux_source_directory(${CMAKE_CURRENT_LIST_DIR} SANDBOXD_TEST_SRCS)
target_sources(${UNIT_TEST_MODULE} PRIVATE ${SANDBOXD_TEST_SRCS})

# Focused test binary for the sandboxd executor.
add_executable(sandboxd_test_bin
    ${PROJECT_SOURCE_DIR}/tests/unit/main.cpp
    sandboxd_request_builder_test.cpp
    sandboxd_executor_test.cpp
    sandboxd_checkpoint_identity_test.cpp
    sandbox_stats_polls_test.cpp
    runtime_state_manager_test.cpp
    sandbox_command_utils_test.cpp
    retire_fault_gate_test.cpp
    managed_checkpoint_divergence_test.cpp
    managed_restore_dispatch_test.cpp
    ../../ckpt/ckpt_ownership_isolation_test.cpp
)
target_compile_options(sandboxd_test_bin
    PRIVATE
    -Wno-error
    -fno-access-control
    -Wno-missing-field-initializers
)
target_include_directories(sandboxd_test_bin PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}
    ${PROJECT_SOURCE_DIR}/src
    ${SRC_INCLUDE_DIR}
    ${SRC_RUNTIME_MANAGER_INCLUDE_DIR}
    ${gtest_INCLUDE_DIR}
)
target_link_libraries(sandboxd_test_bin PRIVATE
    runtime_manager_lib
    metrics_adapter
    ${grpcpp_LIB}
    ${gpr_LIB}
    ${gtest_LIB_A}
    ${gmock_LIB_A}
    pthread
)
add_test(NAME sandboxd_test_bin COMMAND sandboxd_test_bin)
