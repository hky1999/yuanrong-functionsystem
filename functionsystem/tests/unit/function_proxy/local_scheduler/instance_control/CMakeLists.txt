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


# Focused binary for the real-entry kill-identity tests (2026-10 fix chain).
add_executable(kill_identity_test_bin
    ${PROJECT_SOURCE_DIR}/tests/unit/main.cpp
    kill_entry_identity_test.cpp
)
target_compile_options(kill_identity_test_bin
    PRIVATE
    -Wno-error
    -fno-access-control
    -Wno-missing-field-initializers
)
target_include_directories(kill_identity_test_bin PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}
    ${PROJECT_SOURCE_DIR}/src
    ${SRC_INCLUDE_DIR}
    ${gtest_INCLUDE_DIR}
    ${PROJECT_SOURCE_DIR}/tests/unit
)
target_link_libraries(kill_identity_test_bin PRIVATE
    local_scheduler_lib function_proxy_common_lib
    ${grpcpp_LIB}
    ${gpr_LIB}
    ${gtest_LIB_A}
    ${gmock_LIB_A}
    pthread
)
add_test(NAME kill_identity_test_bin COMMAND kill_identity_test_bin)
