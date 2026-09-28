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
install_script="${BASE_DIR}/../../functionsystem/scripts/deploy/function_system/install.sh"
test_tmp_dir=$(mktemp -d)
trap 'rm -rf "${test_tmp_dir}"' EXIT

mkdir -p "${test_tmp_dir}/backend-public"
printf '%s\n' "ssh-ed25519 test-backend" >"${test_tmp_dir}/backend-public/authorized_keys"
printf '%s\n' "test-host-private-key" >"${test_tmp_dir}/frontend-host-key"
printf '%s\n' "ssh-ed25519 test-client" >"${test_tmp_dir}/client-authorized-keys"
printf '%s\n' "test-backend-private-key" >"${test_tmp_dir}/frontend-backend-key"
chmod 600 "${test_tmp_dir}/frontend-host-key" "${test_tmp_dir}/client-authorized-keys" \
  "${test_tmp_dir}/frontend-backend-key"

common_args=(
  --master
  --ip_address 127.0.0.1
  --only_check_param
  --deploy_path "${test_tmp_dir}/deploy"
  --enable_faas_frontend true
  --ssh_enable true
  --ssh_backend_public_key_dir "${test_tmp_dir}/backend-public"
  --frontend_ssh_host_key "${test_tmp_dir}/frontend-host-key"
  --frontend_ssh_backend_key "${test_tmp_dir}/frontend-backend-key"
)

function expect_accepted() {
  local description=$1
  shift
  local output="${test_tmp_dir}/accepted.out"
  if ! bash "${config_script}" "${common_args[@]}" "$@" >"${output}" 2>&1; then
    cat "${output}" >&2
    echo "${description} must be accepted" >&2
    exit 1
  fi
}

function expect_rejected() {
  local description=$1
  shift
  local output="${test_tmp_dir}/rejected.out"
  if bash "${config_script}" "${common_args[@]}" "$@" >"${output}" 2>&1; then
    echo "${description} must be rejected" >&2
    exit 1
  fi
}

expect_accepted "SSH client authentication enabled" \
  --frontend_ssh_auth_enable true \
  --frontend_ssh_authorized_keys "${test_tmp_dir}/client-authorized-keys" \
  --frontend_ssh_address ":2222" \
  --frontend_ssh_max_connections 2048

expect_accepted "SSH client authentication explicitly disabled" \
  --frontend_ssh_auth_enable false

expect_accepted "SSH data-plane node without frontend keys" \
  --enable_faas_frontend false \
  --frontend_ssh_auth_enable true \
  --frontend_ssh_host_key "${test_tmp_dir}/missing-host-key" \
  --frontend_ssh_backend_key "${test_tmp_dir}/missing-backend-key"

expect_rejected "invalid SSH client authentication switch" \
  --frontend_ssh_auth_enable invalid
expect_rejected "missing client authorized_keys when authentication is enabled" \
  --frontend_ssh_auth_enable true
expect_rejected "missing frontend host key" \
  --frontend_ssh_auth_enable false \
  --frontend_ssh_host_key "${test_tmp_dir}/missing-host-key"
expect_rejected "missing frontend backend key" \
  --frontend_ssh_auth_enable false \
  --frontend_ssh_backend_key "${test_tmp_dir}/missing-backend-key"
expect_rejected "empty frontend SSH address" \
  --frontend_ssh_auth_enable false \
  --frontend_ssh_address ""

expected_injections=(
  'YR_FRONTEND_SSH_ENABLE="${SSH_ENABLE:-false}"'
  'YR_FRONTEND_SSH_AUTH_ENABLE="${FRONTEND_SSH_AUTH_ENABLE:-true}"'
  'YR_FRONTEND_SSH_ADDR="${FRONTEND_SSH_ADDRESS:-:2222}"'
  'YR_FRONTEND_SSH_HOST_KEY="${FRONTEND_SSH_HOST_KEY}"'
  'YR_FRONTEND_SSH_AUTHORIZED_KEYS="${FRONTEND_SSH_AUTHORIZED_KEYS}"'
  'YR_FRONTEND_SSH_BACKEND_KEY="${FRONTEND_SSH_BACKEND_KEY}"'
  'YR_FRONTEND_SSH_MAX_CONNECTIONS="${FRONTEND_SSH_MAX_CONNECTIONS:-1024}"'
)

frontend_install_body=$(sed -n \
  '/^function install_faas_frontend()/,/^function install_function_scheduler()/p' \
  "${install_script}")
for injection in "${expected_injections[@]}"; do
  if ! grep -Fq "${injection}" <<<"${frontend_install_body}"; then
    echo "install_faas_frontend does not contain ${injection}" >&2
    exit 1
  fi
done

unexpected_injections=(
  "YR_FRONTEND_SSH_ROUTE_WAIT_TIMEOUT_SECONDS"
  "YR_FRONTEND_SSH_BACKEND_RETRY_ATTEMPTS"
  "YR_FRONTEND_SSH_BACKEND_RETRY_INTERVAL_MS"
)
for injection in "${unexpected_injections[@]}"; do
  if grep -Fq "${injection}" <<<"${frontend_install_body}"; then
    echo "install_faas_frontend must not inject ${injection}" >&2
    exit 1
  fi
done
