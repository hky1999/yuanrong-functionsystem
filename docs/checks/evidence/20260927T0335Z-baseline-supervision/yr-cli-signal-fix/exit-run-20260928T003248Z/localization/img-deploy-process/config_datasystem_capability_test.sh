#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

set -euo pipefail

BASE_DIR=$(cd "$(dirname "$0")" && pwd)
config_script="${BASE_DIR}/config.sh"
test_tmp_dir=$(mktemp -d)
trap 'rm -rf "${test_tmp_dir}"' EXIT

for client_enabled in false true; do
  output="${test_tmp_dir}/${client_enabled}.out"
  CONFIG_DIR="${BASE_DIR}" CONFIG_SCRIPT="${config_script}" bash -c '
    cd "${CONFIG_DIR}"
    source "${CONFIG_SCRIPT}" "$@"
    export_config
    printf "%s\n%s\n%s\n" \
      "${DATA_SYSTEM_ENABLE}" \
      "${YR_DATASYSTEM_DEPLOYED}" \
      "${YR_BYPASS_DATASYSTEM}"
  ' bash \
    --master \
    --ip_address 127.0.0.1 \
    --only_check_param \
    --deploy_path "${test_tmp_dir}/deploy" \
    --data_system_enable "${client_enabled}" >"${output}"

  mapfile -t values < <(tail -n 3 "${output}")
  test "${values[0]}" = "${client_enabled}"
  test "${values[1]}" = "true"
  test "${values[2]}" = "false"
done
