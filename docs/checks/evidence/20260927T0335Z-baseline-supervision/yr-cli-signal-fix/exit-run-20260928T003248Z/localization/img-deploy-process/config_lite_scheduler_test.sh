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
project_root=$(readlink -f "${BASE_DIR}/../..")
install_script="${project_root}/functionsystem/scripts/deploy/function_system/install.sh"
scheduler_template="${project_root}/go/build/faas/init_scheduler_args.json"
test_tmp_dir=$(mktemp -d)
trap 'rm -rf "${test_tmp_dir}"' EXIT

common_args=(
  --master
  --only_check_param
  --deploy_path "${test_tmp_dir}/deploy"
)

lite_scheduler_args=(
  --lite_scheduler_enable true \
  --lite_scheduler_enable_all_tenants false \
  --lite_scheduler_enabled_tenants $'[\n  "tenant-a",\n  "tenant-b"\n]' \
  --lite_scheduler_enabled_functions $'[\n  "0-defaultservice-rrt"\n]' \
  --lite_scheduler_acquire_wait_timeout_ms 3000
)

valid_output="${test_tmp_dir}/valid.out"
if ! bash "${config_script}" "${common_args[@]}" \
  "${lite_scheduler_args[@]}" >"${valid_output}" 2>&1; then
  cat "${valid_output}" >&2
  echo "valid LiteScheduler process options must be accepted" >&2
  exit 1
fi

function expect_rejected() {
  local description=$1
  shift
  local output="${test_tmp_dir}/rejected.out"
  if bash "${config_script}" "${common_args[@]}" "$@" >"${output}" 2>&1; then
    echo "${description} must be rejected" >&2
    exit 1
  fi
}

expect_rejected "invalid lite_scheduler_enable" \
  --lite_scheduler_enable invalid
expect_rejected "plain string tenant list" \
  --lite_scheduler_enabled_tenants tenant-a
expect_rejected "JSON object tenant list" \
  --lite_scheduler_enabled_tenants '{}'
expect_rejected "non-string function list member" \
  --lite_scheduler_enabled_functions '["0-defaultservice-rrt",1]'
expect_rejected "zero LiteScheduler timeout" \
  --lite_scheduler_acquire_wait_timeout_ms 0
expect_rejected "arithmetic LiteScheduler timeout" \
  --lite_scheduler_acquire_wait_timeout_ms '1+2'
expect_rejected "non-numeric LiteScheduler timeout" \
  --lite_scheduler_acquire_wait_timeout_ms abc

export_output="${test_tmp_dir}/exports.out"
rendered_scheduler_config="${test_tmp_dir}/init_scheduler_args.json"
CONFIG_SCRIPT="${config_script}" \
INSTALL_SCRIPT="${install_script}" \
SCHEDULER_TEMPLATE="${scheduler_template}" \
EXPORT_OUTPUT="${export_output}" \
RENDERED_SCHEDULER_CONFIG="${rendered_scheduler_config}" \
bash -c '
  source "${CONFIG_SCRIPT}" "$@"
  export_config
  printf "%s\n" \
    "${LITE_SCHEDULER_ENABLE}" \
    "${LITE_SCHEDULER_ENABLE_ALL_TENANTS}" \
    "${LITE_SCHEDULER_ENABLED_TENANTS}" \
    "${LITE_SCHEDULER_ENABLED_FUNCTIONS}" \
    "${LITE_SCHEDULER_ACQUIRE_WAIT_TIMEOUT_MS}" >"${EXPORT_OUTPUT}"
  cp "${SCHEDULER_TEMPLATE}" "${RENDERED_SCHEDULER_CONFIG}"
  scheduler_body=$(sed -n \
    "/^function install_function_scheduler()/,/^function install_function_agent_and_runtime_manager_in_the_same_process()/p" \
    "${INSTALL_SCRIPT}")
  inline_renderer_body=$(printf "%s\n" "${scheduler_body}" | sed -n \
    -e "/lite_enabled_tenants=/p" \
    -e "/lite_enabled_functions=/p" \
    -e "/{liteEnable}/p" \
    -e "/{liteEnableAllTenants}/p" \
    -e "/{liteEnabledTenants}/p" \
    -e "/{liteEnabledFunctions}/p" \
    -e "/{liteAcquireWaitTimeoutMs}/p")
  install_init_scheduler_config=${RENDERED_SCHEDULER_CONFIG}
  eval "${inline_renderer_body}"
' "${config_script}" "${common_args[@]}" "${lite_scheduler_args[@]}"

expected_exports=$(cat <<'EOF'
true
false
["tenant-a","tenant-b"]
["0-defaultservice-rrt"]
3000
EOF
)
if [ "$(cat "${export_output}")" != "${expected_exports}" ]; then
  echo "unexpected exported LiteScheduler values:" >&2
  cat "${export_output}" >&2
  exit 1
fi

python3 - "${rendered_scheduler_config}" <<'PY'
import json
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
match = re.search(r'"liteScheduler"\s*:\s*(\{.*?\})', text, re.DOTALL)
if match is None:
    raise AssertionError("liteScheduler object missing from scheduler template")
if "{lite" in match.group(1):
    raise AssertionError(f"LiteScheduler placeholder remains in {match.group(1)}")
lite = json.loads(match.group(1))
assert lite == {
    "enable": True,
    "enableAllTenants": False,
    "enabledTenants": ["tenant-a", "tenant-b"],
    "enabledFunctions": ["0-defaultservice-rrt"],
    "acquireWaitTimeoutMs": 3000,
}, lite
PY
