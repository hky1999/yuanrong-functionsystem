#!/bin/bash
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

FUNCTION_SYSTEM_DEPLOY_DIR=$(dirname "$(readlink -f "$0")")
if [ -n "${BASE_DIR}" ]; then
  FUNCTION_SYSTEM_DEPLOY_DIR=${BASE_DIR}/../../functionsystem/deploy
fi
FUNCTION_SYSTEM_DIR=$(readlink -m "${FUNCTION_SYSTEM_DEPLOY_DIR}/..")
DATA_SYSTEM_DIR=$(readlink -m "${FUNCTION_SYSTEM_DIR}/../datasystem")
DATA_PLANE_GATEWAY_DIR=$(readlink -m "${FUNCTION_SYSTEM_DIR}/../data_plane")
# todo(lwy_robb): for sandbox, the start up script path should be assigned by rootfs
INSTALLED_RUNTIME=$(readlink -m "${FUNCTION_SYSTEM_DIR}/../runtime")
if [ -z "$RUNTIME_HOME_DIR" ]; then
  RUNTIME_HOME_DIR=$INSTALLED_RUNTIME
else
  # Create RUNTIME_HOME_DIR directory if not exists
  if [ ! -d "$RUNTIME_HOME_DIR" ]; then
      mkdir -p "$RUNTIME_HOME_DIR"
      if [ $? -ne 0 ]; then
        log_error "Error: failed to create directory $RUNTIME_HOME_DIR. Please check permissions."
        exit 1
      fi
  fi
  ln -sf $INSTALLED_RUNTIME/service $RUNTIME_HOME_DIR/service
fi

PATTERN_FAAS_HOME_DIR="${PATTERN_FAAS_HOME_DIR:-}"
if [ -z "${PATTERN_FAAS_HOME_DIR}" ]; then
  if [ -d "${FUNCTION_SYSTEM_DIR}/../pattern/pattern_faas" ]; then
    PATTERN_FAAS_HOME_DIR=$(readlink -m "${FUNCTION_SYSTEM_DIR}/../pattern/pattern_faas")
  elif [ -d "${FUNCTION_SYSTEM_DIR}/../faas" ]; then
    PATTERN_FAAS_HOME_DIR=$(readlink -m "${FUNCTION_SYSTEM_DIR}/../faas")
  fi
fi

if [ -z "$FUNCTION_META_PATH" ] && [ -n "${PATTERN_FAAS_HOME_DIR}" ]; then
  FUNCTION_META_PATH="$PATTERN_FAAS_HOME_DIR/executor-meta"
fi

function resolve_faas_config_path() {
  local config_name="$1"
  local functionsystem_config="${FUNCTION_SYSTEM_DIR}/config/${config_name}"
  local faas_config="${PATTERN_FAAS_HOME_DIR}/${config_name}"
  if [ -f "${functionsystem_config}" ]; then
    echo "${functionsystem_config}"
  elif [ -n "${PATTERN_FAAS_HOME_DIR}" ] && [ -f "${faas_config}" ]; then
    echo "${faas_config}"
  else
    echo "${functionsystem_config}"
  fi
}

function get_runtime_checkpoint_dir() {
  local checkpoint_dir
  if [ -n "${CHECKPOINT_DIR:-}" ]; then
    checkpoint_dir=$(readlink -m "${CHECKPOINT_DIR}")
  else
    checkpoint_dir=$(readlink -m "${FUNCTION_SYSTEM_DIR}/../checkpoints")
  fi
  mkdir -p "${checkpoint_dir}"
  echo "${checkpoint_dir}"
}

function install_function_proxy() {
  log_info "start function proxy, proxy_port=${FUNCTION_PROXY_PORT}, grpc_port=${FUNCTION_PROXY_GRPC_PORT}..."
  local bin=${FUNCTION_SYSTEM_DIR}/bin/function_proxy
  # Elasticity-baseline launch configuration (explicit, traceable):
  #   AKERNEL_MEMORY_ELASTICITY: 1 enables the revision-fenced shrink path.
  #     Defaults to 0 — absence means OFF; the baseline chart sets 1.
  #   ENABLE_UPGRADE_LADDER / UPGRADE_STEP_MB / UPGRADE_CAP_MB: the D-3
  #     growth argv (enable_upgrade_ladder is OFF by default in the binary,
  #     so an env-only fix leaves PressureMonitor growth disabled; these
  #     MUST become argv to take effect).
  AKERNEL_MEMORY_ELASTICITY=${AKERNEL_MEMORY_ELASTICITY:-0}
  local ladder_args=""
  if [ "${ENABLE_UPGRADE_LADDER:-false}" = "true" ]; then
    ladder_args="--enable_upgrade_ladder=true --upgrade_step_mb=${UPGRADE_STEP_MB:-512} --upgrade_cap_mb=${UPGRADE_CAP_MB:-32768}"
  fi
  local is_pseudo_data_plane="false"
  if [ ${CPU4COMP} -le 100 ]; then
    is_pseudo_data_plane="true"
  fi

  jemalloc_path=""
  if [ "X${ENABLE_JEMALLOC}" == "Xtrue" ]; then
    jemalloc_path="${JEMALLOC_LIB_PATH}"
    if [ ! -f "${jemalloc_path}" ]; then
      log_warning "jemalloc lib path ${jemalloc_path} does not exist!"
    fi
  fi

  if [ "x${ENABLE_META_STORE}" == "xtrue" ]; then
    META_STORE_ADDRESS="${FUNCTION_MASTER_IP}:${GLOBAL_SCHEDULER_PORT}"
  else
    META_STORE_ADDRESS="${ETCD_CLUSTER_ADDRESS}"
  fi

  local enable_driver="true"
  [[ "X${DRIVER_GATEWAY_ENABLE^^}" == "XFALSE" ]] && { enable_driver="false"; }

  # Set default values for Traefik serversTransport configuration
  TRAEFIK_SERVERS_TRANSPORT="${TRAEFIK_SERVERS_TRANSPORT:-yr-backend-tls@file}"

  local merge_process_args=""
  if [ "X${FUNCTION_PROXY_MERGE_PROCESS_ENABLE^^}" == "XTRUE" ]; then
    local checkpoint_dir
    checkpoint_dir=$(get_runtime_checkpoint_dir)
    local ld_library_path=${LD_LIBRARY_PATH}
    local function_system_ld_library_path=${FUNCTION_SYSTEM_DIR}/lib:${DATA_SYSTEM_DIR}/lib:${ld_library_path}
    local agent_uid=${YR_POD_NAME}
    if [ "x${YR_POD_NAME}" == "x" ]; then
      agent_uid="${NODE_ID}"
    fi
    merge_process_args="--enable_merge_process=true \
    --checkpoint_dir="${checkpoint_dir}" \
    --snapshot_storage_backend="${SNAPSHOT_STORAGE_BACKEND:-datasystem}" \
    --snapshot_storage_mode="${SNAPSHOT_STORAGE_MODE:-local_only}" \
    --snapshot_local_cache_max_bytes="${SNAPSHOT_LOCAL_CACHE_MAX_BYTES:-10737418240}" \
    --snapshot_obs_endpoint="${SNAPSHOT_OBS_ENDPOINT:-}" \
    --snapshot_obs_bucket="${SNAPSHOT_OBS_BUCKET:-}" \
    --snapshot_obs_access_key="${SNAPSHOT_OBS_ACCESS_KEY:-}" \
    --snapshot_obs_secret_key="${SNAPSHOT_OBS_SECRET_KEY:-}" \
    --snapshot_obs_security_token="${SNAPSHOT_OBS_SECURITY_TOKEN:-}" \
    --snapshot_obs_use_https="${SNAPSHOT_OBS_USE_HTTPS:-true}" \
    --snapshot_obs_path_style="${SNAPSHOT_OBS_PATH_STYLE:-false}" \
    --agent_listen_port="${FUNCTION_PROXY_PORT}" \
    --local_scheduler_address="${IP_ADDRESS}:${FUNCTION_PROXY_PORT}" \
    --runtime_dir="${RUNTIME_HOME_DIR}/service" \
    --runtime_home_dir="${RUNTIME_USER_HOME_DIR}" \
    --runtime_logs_dir="${RUNTIME_LOG_PATH}" \
    --runtime_std_log_dir="" \
    --runtime_ld_library_path="${ld_library_path}:${RUNTIME_HOME_DIR}/service/cpp/snlib:${RUNTIME_HOME_DIR}/sdk/cpp/lib" \
    --runtime_log_level="${RUNTIME_LOG_LEVEL}" \
    --runtime_max_log_size="${RUNTIME_LOG_ROLLING_MAX_SIZE}" \
    --runtime_max_log_file_num="${RUNTIME_LOG_ROLLING_MAX_FILES}" \
    --runtime_config_dir="${RUNTIME_HOME_DIR}/service/cpp/config/" \
    --enable_separated_redirect_runtime_std="${SEPARATED_REDIRECT_RUNTIME_STD}" \
    --user_log_export_mode="${USER_LOG_EXPORT_MODE}" \
    --npu_collection_mode="${NPU_COLLECTION_MODE}" \
    --gpu_collection_enable="${GPU_COLLECTION_ENABLE}" \
    --proxy_ip="${LOCAL_IP}" \
    --proxy_grpc_server_port="${FUNCTION_PROXY_GRPC_PORT}" \
    --setCmdCred=false \
    --python_dependency_path="${PYTHONPATH}:${RUNTIME_HOME_DIR}/service/python" \
    --python_log_config_path="${RUNTIME_HOME_DIR}/service/python/config/python-runtime-log.json" \
    --java_system_property="${RUNTIME_HOME_DIR}/service/java/log4j2.xml" \
    --java_system_library_path="${RUNTIME_HOME_DIR}/service/java/lib" \
    --host_ip="${IP_ADDRESS}" \
    --port="${FUNCTION_PROXY_PORT}" \
    --data_system_port="${DS_WORKER_PORT}" \
    --agent_address="${IP_ADDRESS}:${FUNCTION_PROXY_PORT}" \
    --runtime_initial_port="${RUNTIME_INIT_PORT}" \
    --port_num="${RUNTIME_PORT_NUM}" \
    --metrics_collector_type="${METRICS_COLLECTOR_TYPE}" \
    --proc_metrics_cpu="${CPU4COMP}" \
    --custom_resources="${CUSTOM_RESOURCES}" \
    --is_protomsg_to_runtime="${IS_PROTOMSG_TO_RUNTIME}" \
    --massif_enable="${MASSIF_ENABLE}" \
    --enable_inherit_env="${ENABLE_INHERIT_ENV}" \
    --memory_detection_interval="${MEMORY_DETECTION_INTERVAL}" \
    --oom_kill_enable="${OOM_KILL_ENABLE}" \
    --oom_kill_control_limit="${OOM_KILL_CONTROL_LIMIT}" \
    --oom_consecutive_detection_count="${OOM_CONSECUTIVE_DETECTION_COUNT}" \
    --kill_process_timeout_seconds="${KILL_PROCESS_TIMEOUT_SECONDS}" \
    --runtime_ds_connect_timeout="${RUNTIME_DS_CONNECT_TIMEOUT}" \
    --runtime_direct_connection_enable="${RUNTIME_DIRECT_CONNECTION_ENABLE}" \
    --runtime_default_config="${RUNTIME_DEFAULT_CONFIG}" \
    --proc_metrics_memory="${MEM4COMP}" \
    --data_system_enable="${DATA_SYSTEM_ENABLE:-false}" \
    --data_system_host="${IP_ADDRESS}" \
    --agent_uid="${agent_uid}" \
    --alias="${FUNCTION_AGENT_ALIAS}" \
    --log_expiration_enable="${LOG_EXPIRATION_ENABLE}" \
    --log_expiration_time_threshold="${LOG_EXPIRATION_TIME_THRESHOLD}" \
    --log_expiration_cleanup_interval="${LOG_EXPIRATION_CLEANUP_INTERVAL}" \
    --log_expiration_max_file_count="${LOG_EXPIRATION_MAX_FILE_COUNT}""
  fi
  # (switch + ladder argv are set in the explicit block at function start)
  LD_LIBRARY_PATH=${function_system_ld_library_path:-${FUNCTION_SYSTEM_DIR}/lib:${DATA_SYSTEM_DIR}/lib:${LD_LIBRARY_PATH}} \
    PATH="${RUNTIME_HOME_DIR}/service/java/bin:/opt/buildtools/jdk8/bin:${PATH}" \
    LD_PRELOAD="${jemalloc_path}" \
    LOCAL_IP="${LOCAL_IP}" \
    HOST_IP="${IP_ADDRESS}" \
    RUNTIME_METRICS_CONFIG=$RUNTIME_METRICS_CONFIG \
    RUNTIME_METRICS_CONFIG_FILE=$RUNTIME_METRICS_CONFIG_FILE \
    INIT_LABELS=${LABELS} \
    ${bin} ${ladder_args} --address="${IP_ADDRESS}:${FUNCTION_PROXY_PORT}" --meta_store_address="${META_STORE_ADDRESS}" \
    --etcd_address="${ETCD_CLUSTER_ADDRESS}" \
    --node_id="${NODE_ID}" --log_config="${FS_LOG_CONFIG}" \
    --services_path="${SERVICES_PATH}" \
    --lib_path="${FUNCTION_SYSTEM_DIR}/lib" \
    --function_meta_path="${FUNCTION_META_PATH}" \
    --ip="${LOCAL_IP}" \
    --grpc_listen_port="${FUNCTION_PROXY_GRPC_PORT}" \
    --component_grpc_port="${FUNCTION_PROXY_COMPONENT_GRPC_PORT}" \
    --dposix_uds_path="${DPOSIX_UDS_PATH}" \
    --enable_driver="${enable_driver}" \
    --enable_trace="${ENABLE_TRACE}" \
    --trace_config="${TRACE_CONFIG}" \
    --enable_metrics="${ENABLE_METRICS}" \
    --metrics_config="${METRICS_CONFIG}" \
    --metrics_config_file="${METRICS_CONFIG_FILE}" \
    --litebus_thread_num="${FUNCTION_PROXY_LITEBUS_THREAD}" \
    --runtime_heartbeat_enable="${RUNTIME_HEARTBEAT_ENABLE}" \
    --runtime_max_heartbeat_timeout_times="${RUNTIME_MAX_HEARTBEAT_TIMEOUT_TIMES}" \
    --runtime_heartbeat_timeout_ms="${RUNTIME_HEARTBEAT_TIMEOUT_MS}" \
    --runtime_recover_enable="${RUNTIME_RECOVER_ENABLE}" \
    --state_storage_type="${STATE_STORAGE_TYPE}" \
    --runtime_init_call_timeout_seconds="${RUNTIME_INIT_CALL_TIMEOUT_SECONDS}" \
    --global_scheduler_address="${FUNCTION_MASTER_IP}:${GLOBAL_SCHEDULER_PORT}" --update_resource_cycle=1000 \
    --runtime_conn_timeout_s="${RUNTIME_CONN_TIMEOUT_S}" \
    --pseudo_data_plane="${is_pseudo_data_plane}" \
    --system_timeout="${SYSTEM_TIMEOUT}" \
    --cache_storage_host="${IP_ADDRESS}" --cache_storage_port="${DS_WORKER_PORT}" \
    --enable_print_resource_view="${ENABLE_PRINT_RESOURCE_VIEW}" \
    --enable_server_mode="true" \
    --advertise_frontend_proxy_create="${ADVERTISE_FRONTEND_PROXY_CREATE:-true}" \
    --schedule_plugins="${LOCAL_SCHEDULE_PLUGINS}" \
    --enable_unit_scheduler="${ENABLE_UNIT_SCHEDULER:-true}" \
    --schedule_placement_policy="${SCHEDULE_PLACEMENT_POLICY:-binpack}" \
    --aggregated_strategy="no_aggregate" \
    --schedule_relaxed="${SCHEDULE_RELAXED:-128}" \
    --max_priority="${MAX_PRIORITY}" --enable_preemption="${ENABLE_PREEMPTION}" \
    --min_instance_memory_size=${MIN_INSTANCE_MEMORY_SIZE} --min_instance_cpu_size=${MIN_INSTANCE_CPU_SIZE} \
    --max_instance_memory_size=${MAX_INSTANCE_MEMORY_SIZE} --max_instance_cpu_size=${MAX_INSTANCE_CPU_SIZE} \
    --election_mode=${ELECTION_MODE} --unregister_while_stop="${FUNCTION_PROXY_UNREGISTER_WHILE_STOP}" \
    --runtime_ds_encrypt_enable="${RUNTIME_DS_ENCRYPT_ENABLE}" --runtime_ds_auth_enable="${RUNTIME_DS_AUTH_ENABLE}" \
    --curve_key_path="${CURVE_KEY_PATH}" \
    --cache_storage_auth_type="${CACHE_STORAGE_AUTH_TYPE}" --cache_storage_auth_enable="${CACHE_STORAGE_AUTH_ENABLE}" \
    --ssl_downgrade_enable="true" \
    --ssl_enable="${SSL_ENABLE}" --ssl_base_path="${SSL_BASE_PATH}" \
    --ssl_root_file="${SSL_ROOT_FILE}" --ssl_cert_file="${SSL_CERT_FILE}" --ssl_key_file="${SSL_KEY_FILE}" \
    --etcd_auth_type="${ETCD_AUTH_TYPE}" --etcd_root_ca_file="${ETCD_CA_FILE}" --etcd_cert_file="${ETCD_CLIENT_CERT_FILE}" --etcd_key_file="${ETCD_CLIENT_KEY_FILE}" \
    --etcd_ssl_base_path=${ETCD_SSL_BASE_PATH} \
    --etcd_table_prefix="${ETCD_TABLE_PREFIX}" --etcd_target_name_override="${ETCD_TARGET_NAME_OVERRIDE}" \
    --enable_print_perf="${ENABLE_PRINT_PERF}" --is_partial_watch_instances="${IS_PARTIAL_WATCH_INSTANCES}" \
    --enable_meta_store="${ENABLE_META_STORE}" --meta_store_mode="${META_STORE_MODE}" --meta_store_excluded_keys="${META_STORE_EXCLUDED_KEYS}" \
    --runtime_instance_debug_enable="${RUNTIME_INSTANCE_DEBUG_ENABLE}" \
    --enable_traefik_registry="${ENABLE_TRAEFIK_REGISTRY}" \
    --traefik_etcd_prefix="${TRAEFIK_ETCD_PREFIX}" --traefik_lease_ttl="${TRAEFIK_LEASE_TTL}" \
    --traefik_http_entrypoint="${TRAEFIK_HTTP_ENTRYPOINT}" --traefik_enable_tls="${TRAEFIK_ENABLE_TLS}" \
    --traefik_servers_transport="${TRAEFIK_SERVERS_TRANSPORT}" \
    --fc_agent_mgr_retry_times="${FC_AGENT_MGR_RETRY_TIMES}" \
    --fc_agent_mgr_retry_cycle="${FC_AGENT_MGR_RETRY_CYCLE}" \
    --enable_tcp_tunnel="$([ "X${SSH_ENABLE}" = "Xtrue" ] || [ "X${ENABLE_TCP_TUNNEL}" = "Xtrue" ] && echo true || echo false)" \
    --tcp_tunnel_port="${TCP_TUNNEL_PORT:-22775}" \
    --tcp_tunnel_max_connections="${TCP_TUNNEL_MAX_CONNECTIONS:-1024}" \
    --enable_direct_routing="${ENABLE_DIRECT_ROUTING}" \
    --force_low_reliability_instance="${FORCE_LOW_RELIABILITY_INSTANCE}" \
    ${merge_process_args} >>"${FS_LOG_PATH}/${NODE_ID}-function_proxy${STD_LOG_SUFFIX}" 2>&1 &

  FUNCTION_PROXY_PID="$!"
  log_info "succeed to start function proxy, proxy_port=${FUNCTION_PROXY_PORT}, grpc_port=${FUNCTION_PROXY_GRPC_PORT}, pid=${FUNCTION_PROXY_PID}"
}

function install_dashboard() {
  log_info "start dashboard, ip=${IP_ADDRESS}, port=${DASHBOARD_PORT}..."
  local etcd_ssl_enable="${SSL_ENABLE}"
  [ "X${ETCD_AUTH_TYPE}" = "XTLS" ] && etcd_ssl_enable="true"
  dashboard_config=${FUNCTION_SYSTEM_DIR}/config/dashboard_config.json
  install_dashboard_config=${config_install_dir}/dashboard_config.json
  cp "${dashboard_config}" "${install_dashboard_config}"
  sed -i "s/{ip}/${IP_ADDRESS}/g" "${install_dashboard_config}"
  sed -i "s/{port}/${DASHBOARD_PORT}/g" "${install_dashboard_config}"
  sed -i "s*{staticPath}*${FUNCTION_SYSTEM_DIR}/bin/client/dist*g" "${install_dashboard_config}"
  sed -i "s/{grpcIP}/${IP_ADDRESS}/g" "${install_dashboard_config}"
  sed -i "s/{grpcPort}/${DASHBOARD_GRPC_PORT}/g" "${install_dashboard_config}"
  sed -i "s/{functionMasterAddr}/${IP_ADDRESS}:${GLOBAL_SCHEDULER_PORT}/g" "${install_dashboard_config}"
  sed -i "s/{frontendAddr}/${IP_ADDRESS}:${FAAS_FRONTEND_HTTP_PORT}/g" "${install_dashboard_config}"
  sed -i "s/{prometheusAddr}/${PROMETHEUS_ADDRESS}/g" "${install_dashboard_config}"
  sed -i "s/{etcdAddr}/$(echo ${ETCD_CLUSTER_ADDRESS} | sed 's/,/","/g')/g" "${install_dashboard_config}"
  sed -i "s/{etcdSsl}/${etcd_ssl_enable}/g" "${install_dashboard_config}"
  sed -i "s/{etcdAuthType}/${ETCD_AUTH_TYPE}/g" "${install_dashboard_config}"
  sed -i "s*{azPrefix}*${ETCD_TABLE_PREFIX}*g" "${install_dashboard_config}"
  if [ "X${etcd_ssl_enable}" = "Xtrue" ] && [ -n "${ETCD_SSL_BASE_PATH}" ]; then
    sed -i "s*{etcdCAFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CA_FILE}*g" ${install_dashboard_config}
    sed -i "s*{etcdCertFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_CERT_FILE}*g" ${install_dashboard_config}
    sed -i "s*{etcdKeyFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_KEY_FILE}*g" ${install_dashboard_config}
  else
    sed -i "s*{etcdCAFile}**g" ${install_dashboard_config}
    sed -i "s*{etcdCertFile}**g" ${install_dashboard_config}
    sed -i "s*{etcdKeyFile}**g" ${install_dashboard_config}
  fi
  sed -i "s/{functionSystemSsl}/${SSL_ENABLE}/g" "${install_dashboard_config}"
  if [ "X${SSL_ENABLE}" = "Xtrue" ] && [ -n "${SSL_BASE_PATH}" ]; then
    sed -i "s*{functionSystemCAFile}*${SSL_BASE_PATH}/${SSL_ROOT_FILE}*g" ${install_dashboard_config}
    sed -i "s*{functionSystemCertFile}*${SSL_BASE_PATH}/${SSL_CERT_FILE}*g" ${install_dashboard_config}
    sed -i "s*{functionSystemKeyFile}*${SSL_BASE_PATH}/${SSL_KEY_FILE}*g" ${install_dashboard_config}
  else
    sed -i "s*{functionSystemCAFile}**g" ${install_dashboard_config}
    sed -i "s*{functionSystemCertFile}**g" ${install_dashboard_config}
    sed -i "s*{functionSystemKeyFile}**g" ${install_dashboard_config}
  fi
  sed -i "s/{prometheusSsl}/${PROMETHEUS_SSL_ENABLE}/g" "${install_dashboard_config}"
  if [ "X${PROMETHEUS_SSL_ENABLE}" = "Xtrue" ] && [ -n "${PROMETHEUS_SSL_BASE_PATH}" ]; then
    sed -i "s*{prometheusCAFile}*${PROMETHEUS_SSL_BASE_PATH}/${PROMETHEUS_SSL_ROOT_FILE}*g" ${install_dashboard_config}
    sed -i "s*{prometheusCertFile}*${PROMETHEUS_SSL_BASE_PATH}/${PROMETHEUS_SSL_CERT_FILE}*g" ${install_dashboard_config}
    sed -i "s*{prometheusKeyFile}*${PROMETHEUS_SSL_BASE_PATH}/${PROMETHEUS_SSL_KEY_FILE}*g" ${install_dashboard_config}
  else
    sed -i "s*{prometheusCAFile}**g" ${install_dashboard_config}
    sed -i "s*{prometheusCertFile}**g" ${install_dashboard_config}
    sed -i "s*{prometheusKeyFile}**g" ${install_dashboard_config}
  fi
  sed -i "s/{dashboardSsl}/${DASHBOARD_SSL_ENABLE}/g" "${install_dashboard_config}"
  if [ "X${DASHBOARD_SSL_ENABLE}" = "Xtrue" ] && [ -n "${DASHBOARD_SSL_BASE_PATH}" ]; then
    sed -i "s*{dashboardCertFile}*${DASHBOARD_SSL_BASE_PATH}/${DASHBOARD_SSL_CERT_FILE}*g" ${install_dashboard_config}
    sed -i "s*{dashboardKeyFile}*${DASHBOARD_SSL_BASE_PATH}/${DASHBOARD_SSL_KEY_FILE}*g" ${install_dashboard_config}
  else
    sed -i "s*{dashboardCertFile}**g" ${install_dashboard_config}
    sed -i "s*{dashboardKeyFile}**g" ${install_dashboard_config}
  fi
  LD_LIBRARY_PATH=${FUNCTION_SYSTEM_DIR}/lib:${ld_library_path} \
    LOG_CONFIG="${FAAS_LOG_CONFIG}" \
    "${FUNCTION_SYSTEM_DIR}"/bin/dashboard \
    --config_path="${install_dashboard_config}" \
    >>"${FS_LOG_PATH}/${NODE_ID}-dashboard${STD_LOG_SUFFIX}" 2>&1 &
  DASHBOARD_PID=$!
  if dashboard_health_check ${DASHBOARD_PID}; then
    log_info "succeed to start dashboard process, ip=${IP_ADDRESS}, port=${DASHBOARD_PORT} pid=${DASHBOARD_PID}"
    return 0
  fi
  return 1
}

function install_collector() {
  log_info "start collector, port=${COLLECTOR_PORT}..."
  local etcd_ssl_enable="${SSL_ENABLE}"
  [ "X${ETCD_AUTH_TYPE}" = "XTLS" ] && etcd_ssl_enable="true"
  GO_RUNTIME_BIN=${RUNTIME_HOME_DIR}/service/go/bin
  DS_SDK_GO_LIB=${DATA_SYSTEM_DIR}/sdk/go/lib
  LD_LIBRARY_PATH=${GO_RUNTIME_BIN}:${DS_SDK_GO_LIB}:${LD_LIBRARY_PATH} \
  "${FUNCTION_SYSTEM_DIR}"/bin/collector \
    --collect_id="${NODE_ID}" \
    --datasystem_port="${DS_WORKER_PORT}" \
    --ip="${IP_ADDRESS}" \
    --port="${COLLECTOR_PORT}" \
    --log_root="${LOG_ROOT}" \
    --etcd_config_servers="${ETCD_CLUSTER_ADDRESS}" \
    --etcd_config_ssl_enable="${etcd_ssl_enable}" \
    --etcd_config_auth_type="${ETCD_AUTH_TYPE}" \
    --etcd_config_ca_file="${ETCD_SSL_BASE_PATH}/${ETCD_CA_FILE}" \
    --etcd_config_cert_file="${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_CERT_FILE}" \
    --etcd_config_key_file="${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_KEY_FILE}" \
    --function_system_ssl_enable="${SSL_ENABLE}" \
    --function_system_ca_file="${SSL_BASE_PATH}/${SSL_ROOT_FILE}" \
    --function_system_cert_file="${SSL_BASE_PATH}/${SSL_CERT_FILE}" \
    --function_system_key_file="${SSL_BASE_PATH}/${SSL_KEY_FILE}" \
    --manager_address="${IP_ADDRESS}:${DASHBOARD_GRPC_PORT}" > "${FS_LOG_PATH}/${NODE_ID}-collector${STD_LOG_SUFFIX}" 2>&1 &
  COLLECTOR_PID="$!"
  if dashboard_health_check ${COLLECTOR_PID}; then
    log_info "succeed to start collector process, port=${COLLECTOR_PORT} pid=${DASHBOARD_PID}"
    return 0
  fi
}

function wait_rust_gateway_ready() {
  local pid="$1"
  local bind="$2"
  local component="$3"
  local host="${bind%:*}"
  local port="${bind##*:}"
  [ "${host}" = "0.0.0.0" ] && host="127.0.0.1"
  local retry=0
  while [ ${retry} -lt 30 ]; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      log_error "${component} exited before becoming ready"
      return 1
    fi
    if curl -fsS --max-time 1 "http://${host}:${port}/readyz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    retry=$((retry + 1))
  done
  log_error "${component} readiness timed out at ${bind}"
  kill "${pid}" 2>/dev/null || true
  return 1
}

function install_node_proxy() {
  local bin="${DATA_PLANE_GATEWAY_DIR}/bin/yr-node-proxy"
  if [ ! -x "${bin}" ]; then
    log_error "Node Proxy binary is missing or not executable: ${bin}"
    return 1
  fi
  mkdir -p "${NODE_PROXY_ACTIVITY_UDS_DIR}"
  mkdir -p "${DATA_PLANE_LOG_DIR}"
  log_info "start Node Proxy, bind=${NODE_PROXY_BIND}, advertise=${NODE_PROXY_ADVERTISE_ADDRESS}..."
  YR_DATA_PLANE_NODE_PROXY_BIND="${NODE_PROXY_BIND}" \
  YR_DATA_PLANE_NODE_PROXY_ADVERTISE_ADDRESS="${NODE_PROXY_ADVERTISE_ADDRESS}" \
  YR_DATA_PLANE_NODE_PROXY_HEALTH_BIND="${NODE_PROXY_HEALTH_BIND}" \
  YR_DATA_PLANE_ALLOWED_TARGET_CIDRS="${NODE_PROXY_ALLOWED_TARGET_CIDRS}" \
  YR_DATA_PLANE_ALLOWED_EDGE_CIDRS="${NODE_PROXY_ALLOWED_EDGE_CIDRS}" \
  YR_DATA_PLANE_NODE_PROXY_ALLOW_ANY_EDGE="${NODE_PROXY_ALLOW_ANY_EDGE}" \
  YR_DATA_PLANE_EDGE_FRONTEND_NODE_SECURITY_MODE="${NODE_PROXY_SECURITY_MODE}" \
  YR_DATA_PLANE_NODE_PROXY_TLS_CERT="${NODE_PROXY_TLS_CERT}" \
  YR_DATA_PLANE_NODE_PROXY_TLS_KEY="${NODE_PROXY_TLS_KEY}" \
  YR_DATA_PLANE_NODE_PROXY_MTLS_CLIENT_CA="${NODE_PROXY_MTLS_CLIENT_CA}" \
  YR_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR="${NODE_PROXY_ACTIVITY_UDS_DIR}" \
  YR_DATA_PLANE_NODE_PROXY_ACTIVITY_INTERVAL_SEC="${NODE_PROXY_ACTIVITY_INTERVAL_SEC:-30}" \
  YR_DATA_PLANE_NODE_PROXY_MAX_STREAMS="${NODE_PROXY_MAX_STREAMS:-0}" \
  YR_DATA_PLANE_LOG_DIR="${DATA_PLANE_LOG_DIR}" \
  YR_DATA_PLANE_LOG_MAX_SIZE_MB="${DATA_PLANE_LOG_MAX_SIZE_MB}" \
  YR_DATA_PLANE_LOG_MAX_FILES="${DATA_PLANE_LOG_MAX_FILES}" \
  YR_DATA_PLANE_LOG_STDOUT="${DATA_PLANE_LOG_STDOUT}" \
  RUST_LOG="${NODE_PROXY_LOG_LEVEL:-info}" \
    "${bin}" >>"${FS_LOG_PATH}/${NODE_ID}-node-proxy${STD_LOG_SUFFIX}" 2>&1 &
  NODE_PROXY_PID=$!
  wait_rust_gateway_ready "${NODE_PROXY_PID}" "${NODE_PROXY_HEALTH_BIND}" "Node Proxy"
}

function install_edge_frontend() {
  local bin="${DATA_PLANE_GATEWAY_DIR}/bin/yr-edge-frontend"
  if [ ! -x "${bin}" ]; then
    log_error "Edge Frontend binary is missing or not executable: ${bin}"
    return 1
  fi
  mkdir -p "${DATA_PLANE_LOG_DIR}"
  local etcd_tls_ca=""
  local etcd_tls_cert=""
  local etcd_tls_key=""
  if [ "${ETCD_AUTH_TYPE}" = "TLS" ]; then
    etcd_tls_ca="${ETCD_SSL_BASE_PATH}/${ETCD_CA_FILE}"
    etcd_tls_cert="${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_CERT_FILE}"
    etcd_tls_key="${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_KEY_FILE}"
  fi
  log_info "start Edge Frontend, tls=${EDGE_FRONTEND_TLS_BIND}, plain=${EDGE_FRONTEND_PLAIN_BIND}, frontend=${EDGE_FRONTEND_CONTROL_PLANE_ADDRESS}..."
  YR_DATA_PLANE_EDGE_FRONTEND_ETCD_ENDPOINTS="${ETCD_ENDPOINTS_ADDR}" \
  YR_DATA_PLANE_EDGE_FRONTEND_ETCD_TLS_CA="${etcd_tls_ca}" \
  YR_DATA_PLANE_EDGE_FRONTEND_ETCD_TLS_CERT="${etcd_tls_cert}" \
  YR_DATA_PLANE_EDGE_FRONTEND_ETCD_TLS_KEY="${etcd_tls_key}" \
  YR_DATA_PLANE_EDGE_FRONTEND_TLS_BIND="${EDGE_FRONTEND_TLS_BIND}" \
  YR_DATA_PLANE_EDGE_FRONTEND_PLAIN_BIND="${EDGE_FRONTEND_PLAIN_BIND}" \
  YR_DATA_PLANE_EDGE_FRONTEND_HEALTH_BIND="${EDGE_FRONTEND_HEALTH_BIND}" \
  YR_DATA_PLANE_EDGE_FRONTEND_CONTROL_PLANE_ADDRESS="${EDGE_FRONTEND_CONTROL_PLANE_ADDRESS}" \
  YR_DATA_PLANE_EDGE_FRONTEND_CONTROL_PLANE_ROUTES="${EDGE_FRONTEND_CONTROL_PLANE_ROUTES}" \
  YR_DATA_PLANE_EDGE_FRONTEND_TLS_CERT="${EDGE_FRONTEND_TLS_CERT}" \
  YR_DATA_PLANE_EDGE_FRONTEND_TLS_KEY="${EDGE_FRONTEND_TLS_KEY}" \
  YR_DATA_PLANE_EDGE_FRONTEND_VALIDATE_IAM="${EDGE_FRONTEND_VALIDATE_IAM}" \
  YR_DATA_PLANE_EDGE_FRONTEND_IAM_ADDRESS="${EDGE_FRONTEND_IAM_ADDRESS}" \
  YR_DATA_PLANE_EDGE_FRONTEND_ALLOWED_CLIENT_CIDRS="${EDGE_FRONTEND_ALLOWED_CLIENT_CIDRS}" \
  YR_DATA_PLANE_EDGE_FRONTEND_ALLOW_ANY_CLIENT="${EDGE_FRONTEND_ALLOW_ANY_CLIENT}" \
  YR_DATA_PLANE_EDGE_FRONTEND_NODE_SECURITY_MODE="${NODE_PROXY_SECURITY_MODE}" \
  YR_DATA_PLANE_EDGE_FRONTEND_NODE_TLS_CA="${EDGE_FRONTEND_NODE_TLS_CA}" \
  YR_DATA_PLANE_EDGE_FRONTEND_NODE_TLS_SERVER_NAME="${EDGE_FRONTEND_NODE_TLS_SERVER_NAME}" \
  YR_DATA_PLANE_EDGE_FRONTEND_NODE_TLS_CLIENT_CERT="${EDGE_FRONTEND_NODE_TLS_CLIENT_CERT}" \
  YR_DATA_PLANE_EDGE_FRONTEND_NODE_TLS_CLIENT_KEY="${EDGE_FRONTEND_NODE_TLS_CLIENT_KEY}" \
  YR_DATA_PLANE_LOG_DIR="${DATA_PLANE_LOG_DIR}" \
  YR_DATA_PLANE_LOG_MAX_SIZE_MB="${DATA_PLANE_LOG_MAX_SIZE_MB}" \
  YR_DATA_PLANE_LOG_MAX_FILES="${DATA_PLANE_LOG_MAX_FILES}" \
  YR_DATA_PLANE_LOG_STDOUT="${DATA_PLANE_LOG_STDOUT}" \
  YR_DATA_PLANE_EDGE_FRONTEND_ACCESS_LOG_ENABLED="${EDGE_FRONTEND_ACCESS_LOG_ENABLED}" \
  YR_COMMAND_WATCH_MAX_SUBSCRIPTIONS_PER_CONNECTION="${COMMAND_WATCH_MAX_SUBSCRIPTIONS}" \
  YR_COMMAND_WATCH_QUEUE_CAPACITY="${COMMAND_WATCH_QUEUE_CAPACITY}" \
  YR_COMMAND_WATCH_MAX_FRAME_BYTES="${COMMAND_WATCH_MAX_FRAME_BYTES}" \
  YR_COMMAND_WATCH_PING_INTERVAL_SECS="${COMMAND_WATCH_PING_INTERVAL_SECS}" \
  RUST_LOG="${EDGE_FRONTEND_LOG_LEVEL:-info}" \
    "${bin}" >>"${FS_LOG_PATH}/${NODE_ID}-edge-frontend${STD_LOG_SUFFIX}" 2>&1 &
  EDGE_FRONTEND_PID=$!
  wait_rust_gateway_ready "${EDGE_FRONTEND_PID}" "${EDGE_FRONTEND_HEALTH_BIND}" "Edge Frontend"
}

function install_faas_frontend() {
  log_info "start faas frontend, http_ip=${IP_ADDRESS}, http_port=${FAAS_FRONTEND_HTTP_PORT}, grpc_port=${FAAS_FRONTEND_GRPC_PORT}..."
  local meta_service_address="${META_SERVICE_ADDRESS}"
  local auth_provider="${AUTH_PROVIDER:-casdoor}"
  
  local auth_enabled="false"
  local auth_public_url=""
  local auth_internal_url=""
  local auth_realm=""
  local auth_client_id=""
  local auth_client_secret=""

  if [ "$auth_provider" == "casdoor" ]; then
    auth_enabled="${CASDOOR_ENABLED:-false}"
    auth_public_url="${CASDOOR_PUBLIC_ENDPOINT}"
    auth_internal_url="${CASDOOR_ENDPOINT}"
    auth_realm="${CASDOOR_ORGANIZATION:-openyuanrong.org}"
    auth_client_id="${CASDOOR_CLIENT_ID}"
    auth_client_secret="${CASDOOR_CLIENT_SECRET}"
  else
    auth_enabled="${KEYCLOAK_ENABLED:-false}"
    auth_public_url="${KEYCLOAK_URL}"
    auth_internal_url="${KEYCLOAK_INTERNAL_URL:-${KEYCLOAK_URL}}"
    auth_realm="${KEYCLOAK_REALM:-yuanrong}"
    auth_client_id="${KEYCLOAK_CLIENT_ID:-frontend}"
    auth_client_secret="${KEYCLOAK_CLIENT_SECRET}"
  fi

  if [ -z "${meta_service_address}" ]; then
    meta_service_address="${IP_ADDRESS}:${META_SERVICE_PORT}"
  fi
  init_frontend_config=$(resolve_faas_config_path "init_frontend_args.json")
  install_init_frontend_config=${config_install_dir}/init_frontend_args_temp.json
  cp ${init_frontend_config} ${install_init_frontend_config}
  sed -i "s/{etcdAddr}/$(echo ${ETCD_CLUSTER_ADDRESS} | sed 's/,/","/g')/g" ${install_init_frontend_config}
  sed -i "s/{faas_frontend_http_ip}/${IP_ADDRESS}/g" ${install_init_frontend_config}
  sed -i "s/{faas_frontend_http_port}/${FAAS_FRONTEND_HTTP_PORT}/g" ${install_init_frontend_config}
  sed -i "s/{sslEnable}/${SSL_ENABLE}/g" ${install_init_frontend_config}
  sed -i "s/{frontendSslEnable}/${FRONTEND_SSL_ENABLE}/g" ${install_init_frontend_config}
  sed -i "s/RequireAndVerifyClientCert/${FRONTEND_CLIENT_AUTH_TYPE}/g" ${install_init_frontend_config}
  sed -i "s/{sccEnable}/${SCC_ENABLE}/g" ${install_init_frontend_config}
  # sandboxRouter (rrt direct L7 reverse proxy) placeholders; filled here where
  # install_init_frontend_config (the temp config faasfrontend reads) is defined.
  sed -i "s/{sandboxRouterEnable}/${ENABLE_SANDBOX_ROUTER:-false}/g" ${install_init_frontend_config}
  sed -i "s/{sandboxRouterListenPort}/${SANDBOX_ROUTER_LISTEN_PORT:-8080}/g" ${install_init_frontend_config}
  sed -i "s/{sandboxRouterRrtPort}/${SANDBOX_ROUTER_RRT_PORT:-50090}/g" ${install_init_frontend_config}
  sed -i "s/{sandboxRouterEnableJwt}/${SANDBOX_ROUTER_ENABLE_JWT:-true}/g" ${install_init_frontend_config}
  sed -i "s/{sandboxRouterValidateIam}/${SANDBOX_ROUTER_VALIDATE_IAM:-true}/g" ${install_init_frontend_config}
  # Smart IAM routing: prefer local plaintext when co-deployed, else use configured address
  local effective_iam_address="${IAM_SERVER_ADDRESS}"
  if [ -n "${IAM_LOCAL_ADDRESS}" ]; then
    # Co-deployed: use local plaintext address
    effective_iam_address="${IAM_LOCAL_ADDRESS}"
    log_info "frontend using local IAM address: ${effective_iam_address}"
  fi
  sed -i "s/{iam_server_address}/${effective_iam_address}/g" ${install_init_frontend_config}

  # IAM TLS config for frontend -> IAM connections (cert paths reuse global)
  local iam_tls_enable="false"
  local iam_tls_ca_file=""
  local iam_tls_cert_file=""
  local iam_tls_key_file=""
  if [ -z "${IAM_LOCAL_ADDRESS}" ]; then
    # Remote IAM: configure TLS if IAM or global SSL is enabled
    if [ "X${IAM_SSL_ENABLE}" = "Xtrue" ] || [ "X${IAM_SSL_ENABLE}" = "XTRUE" ] || \
       [ "X${SSL_ENABLE}" = "Xtrue" ] || [ "X${SSL_ENABLE}" = "XTRUE" ]; then
      iam_tls_enable="true"
      if [ -n "${SSL_BASE_PATH}" ]; then
        iam_tls_ca_file="${SSL_BASE_PATH}/${SSL_ROOT_FILE}"
        iam_tls_cert_file="${SSL_BASE_PATH}/${SSL_CERT_FILE}"
        iam_tls_key_file="${SSL_BASE_PATH}/${SSL_KEY_FILE}"
      fi
      log_info "frontend IAM TLS enabled, ca=${iam_tls_ca_file}"
    fi
  fi
  sed -i "s/{iam_tls_enable}/${iam_tls_enable}/g" ${install_init_frontend_config}
  sed -i "s*{iam_tls_ca_file}*${iam_tls_ca_file}*g" ${install_init_frontend_config}
  sed -i "s*{iam_tls_cert_file}*${iam_tls_cert_file}*g" ${install_init_frontend_config}
  sed -i "s*{iam_tls_key_file}*${iam_tls_key_file}*g" ${install_init_frontend_config}
  sed -i "s/{meta_service_address}/${meta_service_address}/g" ${install_init_frontend_config}
  sed -i "s/{enable_func_token_auth}/${ENABLE_FUNCTION_TOKEN_AUTH}/g" ${install_init_frontend_config}
  sed -i "s/{frontend_lease_bypass}/${FRONTEND_LEASE_BYPASS}/g" ${install_init_frontend_config}
  sed -i "s/{function_invoke_backend}/${FUNCTION_INVOKE_BACKEND:-0}/g" ${install_init_frontend_config}
  
  # Generic auth placeholders
  sed -i "s/{auth_enabled}/${auth_enabled}/g" ${install_init_frontend_config}
  sed -i "s*{auth_public_url}*${auth_public_url}*g" ${install_init_frontend_config}
  sed -i "s*{auth_internal_url}*${auth_internal_url}*g" ${install_init_frontend_config}
  sed -i "s/{auth_realm}/${auth_realm}/g" ${install_init_frontend_config}
  sed -i "s/{auth_client_id}/${auth_client_id}/g" ${install_init_frontend_config}
  sed -i "s/{auth_client_secret}/${auth_client_secret}/g" ${install_init_frontend_config}

  # Legacy Keycloak placeholders (fallback)
  sed -i "s/{keycloak_enabled}/${auth_enabled}/g" ${install_init_frontend_config}
  sed -i "s*{keycloak_url}*${auth_public_url}*g" ${install_init_frontend_config}
  sed -i "s*{keycloak_internal_url}*${auth_internal_url}*g" ${install_init_frontend_config}
  sed -i "s/{keycloak_realm}/${auth_realm}/g" ${install_init_frontend_config}
  sed -i "s/{keycloak_client_id}/${auth_client_id}/g" ${install_init_frontend_config}
  sed -i "s/{keycloak_client_secret}/${auth_client_secret}/g" ${install_init_frontend_config}
  sed -i "s/{etcdAuthType}/${ETCD_AUTH_TYPE}/g" ${install_init_frontend_config}
  sed -i "s*{azPrefix}*${ETCD_TABLE_PREFIX}*g" ${install_init_frontend_config}
  sed -i "s*{sslBasePath}*${SSL_BASE_PATH}*g" ${install_init_frontend_config}
  sed -i "s*{sccBasePath}*${SCC_BASE_PATH}*g" ${install_init_frontend_config}
  if [ "X${SSL_ENABLE}" = "Xtrue" ] && [ -n "${ETCD_SSL_BASE_PATH}" ]; then
    sed -i "s*{etcdCAFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CA_FILE}*g" ${install_init_frontend_config}
    sed -i "s*{etcdCertFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_CERT_FILE}*g" ${install_init_frontend_config}
    sed -i "s*{etcdKeyFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_KEY_FILE}*g" ${install_init_frontend_config}
  else
    sed -i "s*{etcdCAFile}**g" ${install_init_frontend_config}
    sed -i "s*{etcdCertFile}**g" ${install_init_frontend_config}
    sed -i "s*{etcdKeyFile}**g" ${install_init_frontend_config}
  fi
  GO_RUNTIME_BIN=${RUNTIME_HOME_DIR}/service/go/bin
  POD_NAME="frontend-process" \
  FUNCTION_LIB_PATH=${PATTERN_FAAS_HOME_DIR}/faasfrontend/faasfrontend.so \
  INIT_ARGS_FILE_PATH=${install_init_frontend_config} \
  LD_LIBRARY_PATH=${GO_RUNTIME_BIN}:${FUNCTION_SYSTEM_DIR}/lib:${LD_LIBRARY_PATH} \
  ENABLE_SERVER_MODE="true" \
  INIT_HANDLER="faasfrontend.InitHandler" \
  CALL_HANDLER="faasfrontend.CallHandler" \
  CHECKPOINT_HANDLER="faasfrontend.CheckpointHandler" \
  RECOVER_HANDLER="faasfrontend.RecoverHandler" \
  SHUTDOWN_HANDLER="faasfrontend.ShutdownHandler" \
  SIGNAL_HANDLER="faasfrontend.SignalHandler" \
  HEALTH_CHECK_HANDLER="faasfrontend.HealthCheckHandler" \
  YR_FUNCTION_LIB_PATH=${PATTERN_FAAS_HOME_DIR}/faasfrontend/ \
  GLOG_log_dir="${FS_LOG_PATH}" \
  YR_LOG_LEVEL=${FS_LOG_LEVEL} \
  POD_IP=${IP_ADDRESS} \
  NODE_IP=${IP_ADDRESS} \
  DATASYSTEM_ADDR=${IP_ADDRESS}:${DS_WORKER_PORT} \
  INSTANCE_ID="driver-faas-frontend-${NODE_ID}" \
  FAAS_LOG_PATH=${FS_LOG_PATH} \
  ENABLE_TRACE="${ENABLE_TRACE}" \
  YR_FRONTEND_SSH_ENABLE="${SSH_ENABLE:-false}" \
  YR_FRONTEND_SSH_AUTH_ENABLE="${FRONTEND_SSH_AUTH_ENABLE:-true}" \
  YR_FRONTEND_SSH_ADDR="${FRONTEND_SSH_ADDRESS:-:2222}" \
  YR_FRONTEND_SSH_HOST_KEY="${FRONTEND_SSH_HOST_KEY}" \
  YR_FRONTEND_SSH_AUTHORIZED_KEYS="${FRONTEND_SSH_AUTHORIZED_KEYS}" \
  YR_FRONTEND_SSH_BACKEND_KEY="${FRONTEND_SSH_BACKEND_KEY}" \
  YR_FRONTEND_SSH_MAX_CONNECTIONS="${FRONTEND_SSH_MAX_CONNECTIONS:-1024}" \
  YR_RRT_COMMAND_RESULT_TTL_SECS="${RRT_COMMAND_RESULT_TTL_SECS}" \
  YR_RRT_COMMAND_STDOUT_LIMIT_BYTES="${RRT_COMMAND_STDOUT_LIMIT_BYTES}" \
  YR_RRT_COMMAND_STDERR_LIMIT_BYTES="${RRT_COMMAND_STDERR_LIMIT_BYTES}" \
  YR_RRT_COMMAND_REGISTRY_MAX_RECORDS="${RRT_COMMAND_REGISTRY_MAX_RECORDS}" \
  YR_RRT_COMMAND_REGISTRY_MAX_BYTES="${RRT_COMMAND_REGISTRY_MAX_BYTES}" \
  YR_RRT_COMMAND_REGISTRY_MEMORY_HIGH_WATERMARK_BYTES="${RRT_COMMAND_REGISTRY_MEMORY_HIGH_WATERMARK_BYTES}" \
  YR_RRT_COMMAND_ACTIVITY_HEARTBEAT_SECS="${RRT_COMMAND_ACTIVITY_HEARTBEAT_SECS}" \
  YR_COMMAND_WATCH_MAX_SUBSCRIPTIONS_PER_CONNECTION="${COMMAND_WATCH_MAX_SUBSCRIPTIONS}" \
  YR_COMMAND_WATCH_MAX_FRAME_BYTES="${COMMAND_WATCH_MAX_FRAME_BYTES}" \
  TRACE_CONFIG="${TRACE_CONFIG}" \
  ${GO_RUNTIME_BIN}/goruntime \
  -jobId=${NODE_ID} \
  -runtimeId='faas_frontend_libruntime' \
  -instanceId="driver-faas-frontend-${NODE_ID}" \
  -functionName='0/0-system-faasfrontend/$latest' \
  -logLevel=${FS_LOG_LEVEL} \
  -logPath=${FS_LOG_PATH} \
  -enableMTLS=${ENABLE_MTLS} \
  -privateKeyPath=${PRIVATE_KEY_PATH} \
  -certificateFilePath=${CERTIFICATE_FILE_PATH} \
  -verifyFilePath=${VERIFY_FILE_PATH} \
  -encryptPrivateKeyPasswd=${ENCRYPT_PRIVATE_KEY_PASSWD} \
  -primaryKeyStoreFile=${PRIMARY_KEY_STORE_FILE} \
  -standbyKeyStoreFile=${STANDBY_KEY_STORE_FILE} \
  -enableDsEncrypt=${RUNTIME_DS_ENCRYPT_ENABLE} \
  -functionSystemAddress="${LOCAL_IP}:${FUNCTION_PROXY_GRPC_PORT}" \
  -driverMode true  >> "${FS_LOG_PATH}/${NODE_ID}-faas_frontend${STD_LOG_SUFFIX}"  2>&1 &
  FAAS_FRONTEND_PID="$!"
  log_info "succeed to start faas frontend, http_ip=${IP_ADDRESS}, http_port=${FAAS_FRONTEND_HTTP_PORT}, grpc_port=${FAAS_FRONTEND_GRPC_PORT}, pid=${FAAS_FRONTEND_PID}"
}

function install_function_scheduler() {
  log_info "start scheduler..."
  init_scheduler_config=$(resolve_faas_config_path "init_scheduler_args.json")
  install_init_scheduler_config=${config_install_dir}/init_scheduler_args_temp.json
  cp ${init_scheduler_config} ${install_init_scheduler_config}
  local lite_enabled_tenants
  local lite_enabled_functions
  lite_enabled_tenants=$(printf '%s' "${LITE_SCHEDULER_ENABLED_TENANTS}" | sed 's/[\\&|]/\\&/g')
  lite_enabled_functions=$(printf '%s' "${LITE_SCHEDULER_ENABLED_FUNCTIONS}" | sed 's/[\\&|]/\\&/g')
  sed -i "s/{etcdAddr}/$(echo ${ETCD_CLUSTER_ADDRESS} | sed 's/,/","/g')/g" ${install_init_scheduler_config}
  sed -i "s/{functionSchedulerLeasePort}/${FUNCTION_SCHEDULER_LEASE_PORT}/g" ${install_init_scheduler_config}
  sed -i "s|{liteEnable}|${LITE_SCHEDULER_ENABLE}|g" ${install_init_scheduler_config}
  sed -i "s|{liteEnableAllTenants}|${LITE_SCHEDULER_ENABLE_ALL_TENANTS}|g" ${install_init_scheduler_config}
  sed -i "s|{liteEnabledTenants}|${lite_enabled_tenants}|g" ${install_init_scheduler_config}
  sed -i "s|{liteEnabledFunctions}|${lite_enabled_functions}|g" ${install_init_scheduler_config}
  sed -i "s|{liteAcquireWaitTimeoutMs}|${LITE_SCHEDULER_ACQUIRE_WAIT_TIMEOUT_MS}|g" ${install_init_scheduler_config}
  sed -i "s/{sslEnable}/${SSL_ENABLE}/g" ${install_init_scheduler_config}
  sed -i "s/{sccEnable}/${SCC_ENABLE}/g" ${install_init_scheduler_config}
  sed -i "s/{etcdAuthType}/${ETCD_AUTH_TYPE}/g" ${install_init_scheduler_config}
  sed -i "s*{azPrefix}*${ETCD_TABLE_PREFIX}*g" ${install_init_scheduler_config}
  sed -i "s*{sslBasePath}*${SSL_BASE_PATH}*g" ${install_init_scheduler_config}
  sed -i "s*{sccBasePath}*${SCC_BASE_PATH}*g" ${install_init_scheduler_config}
  if [ "X${SSL_ENABLE}" = "Xtrue" ] && [ -n "${ETCD_SSL_BASE_PATH}" ]; then
    sed -i "s*{etcdCAFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CA_FILE}*g" ${install_init_scheduler_config}
    sed -i "s*{etcdCertFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_CERT_FILE}*g" ${install_init_scheduler_config}
    sed -i "s*{etcdKeyFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_KEY_FILE}*g" ${install_init_scheduler_config}
  else
    sed -i "s*{etcdCAFile}**g" ${install_init_scheduler_config}
    sed -i "s*{etcdCertFile}**g" ${install_init_scheduler_config}
    sed -i "s*{etcdKeyFile}**g" ${install_init_scheduler_config}
  fi
  GO_RUNTIME_BIN=${RUNTIME_HOME_DIR}/service/go/bin
  POD_NAME="scheduler-process" \
  FUNCTION_LIB_PATH=${PATTERN_FAAS_HOME_DIR}/faasscheduler/faasscheduler.so \
  INIT_ARGS_FILE_PATH=${install_init_scheduler_config} \
  LD_LIBRARY_PATH=${GO_RUNTIME_BIN}:${FUNCTION_SYSTEM_DIR}/lib:${LD_LIBRARY_PATH} \
  ENABLE_SERVER_MODE="true" \
  INIT_HANDLER="faasscheduler.InitHandler" \
  CALL_HANDLER="faasscheduler.CallHandler" \
  CHECKPOINT_HANDLER="faasscheduler.CheckpointHandler" \
  RECOVER_HANDLER="faasscheduler.RecoverHandler" \
  SHUTDOWN_HANDLER="faasscheduler.ShutdownHandler" \
  SIGNAL_HANDLER="faasscheduler.SignalHandler" \
  HEALTH_CHECK_HANDLER="faasscheduler.HealthCheckHandler" \
  YR_FUNCTION_LIB_PATH=${PATTERN_FAAS_HOME_DIR}/faasscheduler/ \
  GLOG_log_dir="${FS_LOG_PATH}" \
  YR_LOG_LEVEL=${FS_LOG_LEVEL} \
  POD_IP=${IP_ADDRESS} \
  NODE_IP=${IP_ADDRESS} \
  DATASYSTEM_ADDR=${IP_ADDRESS}:${DS_WORKER_PORT} \
  INSTANCE_ID="driver-scheduler-${NODE_ID}" \
  FAAS_LOG_PATH=${FS_LOG_PATH} \
  ${GO_RUNTIME_BIN}/goruntime \
  -jobId=${NODE_ID} \
  -runtimeId='scheduler_libruntime' \
  -instanceId="driver-scheduler-${NODE_ID}" \
  -functionName='0/0-system-faasscheduler/$latest' \
  -logLevel=${FS_LOG_LEVEL} \
  -logPath=${FS_LOG_PATH} \
  -enableMTLS=${ENABLE_MTLS} \
  -privateKeyPath=${PRIVATE_KEY_PATH} \
  -certificateFilePath=${CERTIFICATE_FILE_PATH} \
  -verifyFilePath=${VERIFY_FILE_PATH} \
  -encryptPrivateKeyPasswd=${ENCRYPT_PRIVATE_KEY_PASSWD} \
  -primaryKeyStoreFile=${PRIMARY_KEY_STORE_FILE} \
  -standbyKeyStoreFile=${STANDBY_KEY_STORE_FILE} \
  -enableDsEncrypt=${RUNTIME_DS_ENCRYPT_ENABLE} \
  -functionSystemAddress="${LOCAL_IP}:${FUNCTION_PROXY_GRPC_PORT}" \
  -driverMode true  >> "${FS_LOG_PATH}/${NODE_ID}-scheduler${STD_LOG_SUFFIX}"  2>&1 &
  SCHEDULER_PID="$!"
  log_info "succeed to scheduler, pid=${SCHEDULER_PID}"
}

function install_function_agent() {
  install_function_agent_and_runtime_manager_in_the_same_process
  return  $?
}

function install_function_agent_and_runtime_manager_in_the_same_process() {
  log_info "start function agent and runtime manager, port=${FUNCTION_AGENT_PORT}..."
  local bin=${FUNCTION_SYSTEM_DIR}/bin/function_agent
  local ld_library_path=${LD_LIBRARY_PATH}
  local function_system_ld_library_path=${FUNCTION_SYSTEM_DIR}/lib:${DATA_SYSTEM_DIR}/lib:${ld_library_path}
  local checkpoint_dir
  checkpoint_dir=$(get_runtime_checkpoint_dir)
  local unique_proxy_option="--local_node_id=${NODE_ID}"
  if [ "X${DEPLOY_FUNCTION_PROXY}" = "Xfalse" ] && [ ! -z "${UNIQUE_NODE_ID}" ]; then
    unique_proxy_option="--local_node_id=${UNIQUE_NODE_ID}"
  fi
  local agent_uid=${YR_POD_NAME}
  if [ "x${YR_POD_NAME}" == "x" ]; then
    agent_uid="${NODE_ID}"
  fi

  # Extract agent arguments into an array for clarity and reuse
  agent_args=(
    --enable_merge_process=true
    --ip="${IP_ADDRESS}"
    --node_id="${NODE_ID}"
    --agent_uid="${agent_uid}"
    --alias="${FUNCTION_AGENT_ALIAS}"
    --log_config="${FS_LOG_CONFIG}"
    --litebus_thread_num="${FUNCTION_AGENT_LITEBUS_THREAD}"
    --local_scheduler_address="${IP_ADDRESS}:${FUNCTION_PROXY_PORT}"
    --agent_listen_port="${FUNCTION_AGENT_PORT}"
    --runtime_dir="${RUNTIME_HOME_DIR}/service"
    --runtime_home_dir="${RUNTIME_USER_HOME_DIR}"
    --runtime_logs_dir="${RUNTIME_LOG_PATH}"
    --runtime_std_log_dir=""
    --checkpoint_dir="${checkpoint_dir}"
    --snapshot_storage_backend="${SNAPSHOT_STORAGE_BACKEND:-datasystem}"
    --snapshot_storage_mode="${SNAPSHOT_STORAGE_MODE:-local_only}"
    --snapshot_local_cache_max_bytes="${SNAPSHOT_LOCAL_CACHE_MAX_BYTES:-10737418240}"
    --snapshot_obs_endpoint="${SNAPSHOT_OBS_ENDPOINT:-}"
    --snapshot_obs_bucket="${SNAPSHOT_OBS_BUCKET:-}"
    --snapshot_obs_access_key="${SNAPSHOT_OBS_ACCESS_KEY:-}"
    --snapshot_obs_secret_key="${SNAPSHOT_OBS_SECRET_KEY:-}"
    --snapshot_obs_security_token="${SNAPSHOT_OBS_SECURITY_TOKEN:-}"
    --snapshot_obs_use_https="${SNAPSHOT_OBS_USE_HTTPS:-true}"
    --snapshot_obs_path_style="${SNAPSHOT_OBS_PATH_STYLE:-false}"
    --runtime_log_level="${RUNTIME_LOG_LEVEL}"
    --runtime_ld_library_path="${ld_library_path}:${RUNTIME_HOME_DIR}/service/cpp/snlib:${RUNTIME_HOME_DIR}/sdk/cpp/lib"
    --runtime_max_log_size="${RUNTIME_LOG_ROLLING_MAX_SIZE}"
    --runtime_max_log_file_num="${RUNTIME_LOG_ROLLING_MAX_FILES}"
    --runtime_config_dir="${RUNTIME_HOME_DIR}/service/cpp/config/"
    --enable_separated_redirect_runtime_std="${SEPARATED_REDIRECT_RUNTIME_STD}"
    --user_log_export_mode="${USER_LOG_EXPORT_MODE}"
    --npu_collection_mode="${NPU_COLLECTION_MODE}"
    --gpu_collection_enable="${GPU_COLLECTION_ENABLE}"
    --proxy_ip="${LOCAL_IP}" \
    --proxy_grpc_server_port="${FUNCTION_PROXY_GRPC_PORT}"
    --setCmdCred=false
    --python_dependency_path="${PYTHONPATH}:${RUNTIME_HOME_DIR}/service/python"
    --python_log_config_path="${RUNTIME_HOME_DIR}/service/python/config/python-runtime-log.json"
    --java_system_property="${RUNTIME_HOME_DIR}/service/java/log4j2.xml"
    --java_system_library_path="${RUNTIME_HOME_DIR}/service/java/lib"
    --host_ip="${IP_ADDRESS}"
    --port="${FUNCTION_AGENT_PORT}"
    --data_system_port="${DS_WORKER_PORT}"
    --agent_address="${IP_ADDRESS}:${FUNCTION_AGENT_PORT}"
    --enable_metrics="${ENABLE_METRICS}"
    --metrics_config="${METRICS_CONFIG}"
    --metrics_config_file="${METRICS_CONFIG_FILE}"
    --runtime_initial_port="${RUNTIME_INIT_PORT}"
    --port_num="${RUNTIME_PORT_NUM}"
    --system_timeout="${SYSTEM_TIMEOUT}"
    --metrics_collector_type="${METRICS_COLLECTOR_TYPE}"
    --proc_metrics_cpu="${CPU4COMP}"
    --custom_resources="${CUSTOM_RESOURCES}"
    --is_protomsg_to_runtime="${IS_PROTOMSG_TO_RUNTIME}"
    --massif_enable="${MASSIF_ENABLE}"
    --enable_inherit_env="${ENABLE_INHERIT_ENV}"
    --memory_detection_interval="${MEMORY_DETECTION_INTERVAL}"
    --oom_kill_enable="${OOM_KILL_ENABLE}"
    --oom_kill_control_limit="${OOM_KILL_CONTROL_LIMIT}"
    --oom_consecutive_detection_count="${OOM_CONSECUTIVE_DETECTION_COUNT}"
    --kill_process_timeout_seconds="${KILL_PROCESS_TIMEOUT_SECONDS}"
    --runtime_ds_connect_timeout="${RUNTIME_DS_CONNECT_TIMEOUT}"
    --runtime_direct_connection_enable="${RUNTIME_DIRECT_CONNECTION_ENABLE}"
    --ssl_enable="${SSL_ENABLE}"
    --ssl_base_path="${SSL_BASE_PATH}"
    --ssl_root_file="${SSL_ROOT_FILE}"
    --ssl_cert_file="${SSL_CERT_FILE}"
    --ssl_key_file="${SSL_KEY_FILE}"
    --etcd_auth_type="${ETCD_AUTH_TYPE}"
    --etcd_root_ca_file="${ETCD_CA_FILE}"
    --etcd_cert_file="${ETCD_CLIENT_CERT_FILE}"
    --etcd_key_file="${ETCD_CLIENT_KEY_FILE}"
    --etcd_ssl_base_path=${ETCD_SSL_BASE_PATH}
    --runtime_default_config="${RUNTIME_DEFAULT_CONFIG}"
    --proc_metrics_memory="${MEM4COMP}"
    --enable_dis_conv_call_stack="${ENABLE_DIS_CONV_CALL_STACK}"
    --data_system_enable="${DATA_SYSTEM_ENABLE:-false}"
    --data_system_host="${IP_ADDRESS}"
    --runtime_instance_debug_enable="${RUNTIME_INSTANCE_DEBUG_ENABLE}"
    --log_expiration_enable="${LOG_EXPIRATION_ENABLE}"
    --log_expiration_time_threshold="${LOG_EXPIRATION_TIME_THRESHOLD}"
    --log_expiration_cleanup_interval="${LOG_EXPIRATION_CLEANUP_INTERVAL}"
    --log_expiration_max_file_count="${LOG_EXPIRATION_MAX_FILE_COUNT}"
    --user_log_auto_flush_interval_ms="${USER_LOG_AUTO_FLUSH_INTERVAL_MS}"
    --user_log_buffer_flush_threshold="${USER_LOG_BUFFER_FLUSH_THRESHOLD}"
    --user_log_rolling_size_limit_mb="${USER_LOG_MAX_ROLLING_FILE_SIZE_MB}"
    --user_log_rolling_file_count_limit="${USER_LOG_MAX_ROLLING_LOG_FILE_NUM}"
    --npu_collection_enable="${NPU_COLLECTION_ENABLE}"
  )

  # Start with or without redirecting stdout/stderr depending on USER_LOG_EXPORT_MODE
  if [ "x${USER_LOG_EXPORT_MODE}" == "xstd" ]; then
    LD_LIBRARY_PATH=${function_system_ld_library_path} \
    PATH="${RUNTIME_HOME_DIR}/service/java/bin:/opt/buildtools/jdk8/bin:${PATH}" \
    LOCAL_IP="${LOCAL_IP}" \
    HOST_IP="${IP_ADDRESS}" \
    RUNTIME_METRICS_CONFIG=$RUNTIME_METRICS_CONFIG \
    RUNTIME_METRICS_CONFIG_FILE=$RUNTIME_METRICS_CONFIG_FILE \
    INIT_LABELS=${LABELS} \
    ${bin} "${agent_args[@]}" ${unique_proxy_option} &
    FUNCTION_AGENT_PID="$!"
  else
    LD_LIBRARY_PATH=${function_system_ld_library_path} \
    PATH="${RUNTIME_HOME_DIR}/service/java/bin:/opt/buildtools/jdk8/bin:${PATH}" \
    LOCAL_IP="${LOCAL_IP}" \
    HOST_IP="${IP_ADDRESS}" \
    RUNTIME_METRICS_CONFIG=$RUNTIME_METRICS_CONFIG \
    RUNTIME_METRICS_CONFIG_FILE=$RUNTIME_METRICS_CONFIG_FILE \
    INIT_LABELS=${LABELS} \
    ${bin} "${agent_args[@]}" ${unique_proxy_option} >>"${FS_LOG_PATH}/${NODE_ID}-function_agent${STD_LOG_SUFFIX}" 2>&1 &
    FUNCTION_AGENT_PID="$!"
  fi
  log_info "succeed to start function agent and runtime manager, port=${FUNCTION_AGENT_PORT} pid=${FUNCTION_AGENT_PID}"
}

function install_function_master() {
  log_info "start function master, port=${GLOBAL_SCHEDULER_PORT}..."
  jemalloc_path=""
  if [ "X${ENABLE_JEMALLOC}" == "Xtrue" ]; then
    jemalloc_path="${JEMALLOC_LIB_PATH}"
    if [ ! -f "${jemalloc_path}" ]; then
      log_warning "jemalloc lib path ${jemalloc_path} does not exist!"
    fi
  fi

  if [ "x${ENABLE_META_STORE}" == "xtrue" ] && [ "x${META_STORE_MODE}" == "xlocal" ]; then
    META_STORE_ADDRESS="${IP_ADDRESS}:${GLOBAL_SCHEDULER_PORT}"
  else
    META_STORE_ADDRESS="${ETCD_CLUSTER_ADDRESS}"
  fi

  if check_port "${FUNCTION_MASTER_IP}" "${GLOBAL_SCHEDULER_PORT}"; then
    OPENSSL_CONF="" LD_LIBRARY_PATH=${FUNCTION_SYSTEM_DIR}/lib:${LD_LIBRARY_PATH} \
      LD_PRELOAD="${jemalloc_path}":"${LD_PRELOAD}" \
      "${FUNCTION_SYSTEM_DIR}"/bin/function_master --ip="${IP_ADDRESS}:${GLOBAL_SCHEDULER_PORT}" \
      --meta_store_address="${META_STORE_ADDRESS}" --log_config="${FS_LOG_CONFIG}" \
      --etcd_address="${ETCD_CLUSTER_ADDRESS}" \
      --node_id="${NODE_ID}" --sys_func_retry_period="${SYS_FUNC_RETRY_PERIOD}" \
      --runtime_recover_enable="${RUNTIME_RECOVER_ENABLE}" \
      --litebus_thread_num="${FUNCTION_MASTER_LITEBUS_THREAD}" \
      --system_timeout="${SYSTEM_TIMEOUT}" --enable_metrics="${ENABLE_METRICS}" \
      --metrics_config="${METRICS_CONFIG}" \
      --metrics_config_file="${METRICS_CONFIG_FILE}" \
      --pull_resource_interval="${PULL_RESOURCE_INTERVAL}" \
      --is_schedule_tolerate_abnormal="${IS_SCHEDULE_TOLERATE_ABNORMAL}" \
      --enable_print_resource_view="${ENABLE_PRINT_RESOURCE_VIEW}" \
      --schedule_plugins="${DOMAIN_SCHEDULE_PLUGINS}" \
      --enable_unit_scheduler="${ENABLE_UNIT_SCHEDULER:-true}" \
      --schedule_placement_policy="${SCHEDULE_PLACEMENT_POLICY:-binpack}" \
      --aggregated_strategy="${AGGREGATED_STRATEGY:-relaxed}" \
      --schedule_relaxed="${SCHEDULE_RELAXED}" \
      --max_priority="${MAX_PRIORITY}" --enable_preemption="${ENABLE_PREEMPTION}" \
      --enable_meta_store="${ENABLE_META_STORE}" \
      --enable_persistence="${ENABLE_PERSISTENCE}" \
      --meta_store_mode="${META_STORE_MODE}" \
      --meta_store_excluded_keys="${META_STORE_EXCLUDED_KEYS}" \
      --election_mode=${ELECTION_MODE} \
      --services_path="${SERVICES_PATH}" \
      --lib_path="${FUNCTION_SYSTEM_DIR}/lib" \
      --ssl_enable="${SSL_ENABLE}" --ssl_base_path="${SSL_BASE_PATH}" \
      --etcd_auth_type="${ETCD_AUTH_TYPE}" --etcd_root_ca_file="${ETCD_CA_FILE}" --etcd_cert_file="${ETCD_CLIENT_CERT_FILE}" --etcd_key_file="${ETCD_CLIENT_KEY_FILE}" \
      --etcd_ssl_base_path=${ETCD_SSL_BASE_PATH} \
      --etcd_table_prefix="${ETCD_TABLE_PREFIX}" --etcd_target_name_override="${ETCD_TARGET_NAME_OVERRIDE}" \
      --ssl_root_file="${SSL_ROOT_FILE}" --ssl_cert_file="${SSL_CERT_FILE}" --ssl_key_file="${SSL_KEY_FILE}" \
      --function_meta_path="${FUNCTION_META_PATH}" \
      --enable_trace=${ENABLE_TRACE} --trace_config="${TRACE_CONFIG}" \
      --meta_store_max_flush_concurrency="${META_STORE_MAX_FLUSH_CONCURRENCY}" --meta_store_max_flush_batch_size="${META_STORE_MAX_FLUSH_BATCH_SIZE}" \
      --system_tenant_id="${SYSTEM_TENANT_ID}" \
      --enable_traefik_provider="${ENABLE_TRAEFIK_PROVIDER}" \
      --traefik_http_entry_point="${TRAEFIK_HTTP_ENTRY_POINT}" \
      --traefik_enable_tls="${TRAEFIK_ENABLE_TLS}" \
      --traefik_servers_transport="${TRAEFIK_SERVERS_TRANSPORT}" \
      --traefik_public_base_domain="${TRAEFIK_PUBLIC_BASE_DOMAIN}" \
      --traefik_forward_timeout_ms="${TRAEFIK_FORWARD_TIMEOUT_MS}" \
      >>"${FS_LOG_PATH}/${NODE_ID}-function_master${STD_LOG_SUFFIX}" 2>&1 &
    FUNCTION_MASTER_PID=$!
    if function_system_health_check ${FUNCTION_MASTER_PID} "${GLOBAL_SCHEDULER_PORT}" "global-scheduler"; then
      log_info "succeed to start function master process, port=${GLOBAL_SCHEDULER_PORT}, pid=${FUNCTION_MASTER_PID}"
      return 0
    fi
    if [ ${FUNCTION_MASTER_PID} -gt 0 ]; then
      log_warning "health check failed, killing function_master process pid: ${FUNCTION_MASTER_PID}"
      kill -9 ${FUNCTION_MASTER_PID}
    fi
  fi
  return 1
}

function install_metaservice() {
  log_info "start metaservice, ip=${IP_ADDRESS}, port=${META_SERVICE_PORT}..."
  metaservice_config=${FUNCTION_SYSTEM_DIR}/config/meta_service/metaservice_config.json
  install_metaservice_config=${config_install_dir}/metaservice_config.json
  cp ${metaservice_config} ${install_metaservice_config}
  sed -i "s/{ip}/${IP_ADDRESS}/g" ${install_metaservice_config}
  sed -i "s/{port}/${META_SERVICE_PORT}/g" ${install_metaservice_config}
  sed -i "s/{functionMasterAddr}/${IP_ADDRESS}:${GLOBAL_SCHEDULER_PORT}/g" ${install_metaservice_config}
  sed -i "s/{frontendAddr}/${IP_ADDRESS}:${FAAS_FRONTEND_HTTP_PORT}/g" ${install_metaservice_config}
  sed -i "s/{etcdAddr}/$(echo ${ETCD_CLUSTER_ADDRESS} | sed 's/,/","/g')/g" ${install_metaservice_config}
  sed -i "s/{sslEnable}/${SSL_ENABLE}/g" ${install_metaservice_config}
  sed -i "s/{metaserviceSslEnable}/${META_SERVICE_SSL_ENABLE}/g" ${install_metaservice_config}
  sed -i "s/RequireAndVerifyClientCert/${META_SERVICE_CLIENT_AUTH_TYPE}/g" ${install_metaservice_config}
  sed -i "s*{azPrefix}*${ETCD_TABLE_PREFIX}*g" ${install_metaservice_config}
  sed -i "s/{etcdAuthType}/${ETCD_AUTH_TYPE}/g" ${install_metaservice_config}
  sed -i "s/{clusters}/${CLUSTER_LIST}/g" ${install_metaservice_config}
  sed -i "s/{sccEnable}/${SCC_ENABLE}/g" ${install_metaservice_config}
  sed -i "s*{sccBasePath}*${SCC_BASE_PATH}*g" ${install_metaservice_config}
  if [ "X${META_SERVICE_SSL_ENABLE}" = "Xtrue" ] && [ -n "${SSL_BASE_PATH}" ]; then
    sed -i "s*{rootCAFile}*${SSL_BASE_PATH}/${SSL_ROOT_FILE}*g" ${install_metaservice_config}
    sed -i "s*{moduleCertFile}*${SSL_BASE_PATH}/${SSL_CERT_FILE}*g" ${install_metaservice_config}
    sed -i "s*{moduleKeyFile}*${SSL_BASE_PATH}/${SSL_KEY_FILE}*g" ${install_metaservice_config}
  else
    sed -i "s*{rootCAFile}**g" ${install_metaservice_config}
    sed -i "s*{moduleCertFile}**g" ${install_metaservice_config}
    sed -i "s*{moduleKeyFile}**g" ${install_metaservice_config}
  fi
  if [ "X${SSL_ENABLE}" = "Xtrue" ] && [ -n "${ETCD_SSL_BASE_PATH}" ]; then
    sed -i "s*{etcdCAFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CA_FILE}*g" ${install_metaservice_config}
    sed -i "s*{etcdCertFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_CERT_FILE}*g" ${install_metaservice_config}
    sed -i "s*{etcdKeyFile}*${ETCD_SSL_BASE_PATH}/${ETCD_CLIENT_KEY_FILE}*g" ${install_metaservice_config}
  else
    sed -i "s*{etcdCAFile}**g" ${install_metaservice_config}
    sed -i "s*{etcdCertFile}**g" ${install_metaservice_config}
    sed -i "s*{etcdKeyFile}**g" ${install_metaservice_config}
  fi
  if [ "X${SCC_ENABLE}" = "Xtrue" ]; then
    sed -i "s*{sslDecryptTool}*${SSL_DECRYPT_TOOL}*g" ${install_metaservice_config}
  else
    sed -i "s*{sslDecryptTool}**g" ${install_metaservice_config}
  fi
  metaservice_log=${FUNCTION_SYSTEM_DIR}/config/meta_service/metaservice_log.json
  metaservice_temp_log=${FUNCTION_SYSTEM_DIR}/config/metaservice_temp_log.json
  cp ${metaservice_log} ${metaservice_temp_log}
  metaservice_log_path=${FS_LOG_PATH}
  sed -i "s*{logConfigPath}*${metaservice_log_path}*g" ${metaservice_temp_log}
  sed -i "s/{logLevel}/${FS_LOG_LEVEL}/g" ${metaservice_temp_log}
  LD_LIBRARY_PATH=${FUNCTION_SYSTEM_DIR}/lib:${ld_library_path} \
    ${FUNCTION_SYSTEM_DIR}/bin/meta_service \
    --config_path="${install_metaservice_config}" \
    --log_config_path="${metaservice_temp_log}" \
    >>"${FS_LOG_PATH}/${NODE_ID}-metaservice${STD_LOG_SUFFIX}" 2>&1 &
    METASERVICE_PID=$!
  if metaservice_health_check ${METASERVICE_PID}; then
    log_info "succeed to start metaservice process, ip=${IP_ADDRESS} port=${META_SERVICE_PORT} pid=${METASERVICE_PID}"
    return 0
  fi
  return 1
}

function install_iam_server() {
  log_info "start iam_server, ip=${IP_ADDRESS}, port=${IAM_SERVER_PORT}..."
  local bin=${FUNCTION_SYSTEM_DIR}/bin/iam_server
  local auth_provider="${AUTH_PROVIDER:-casdoor}"
  
  # Keycloak configs
  local keycloak_enabled="${KEYCLOAK_ENABLED}"
  local keycloak_url="${KEYCLOAK_INTERNAL_URL:-${KEYCLOAK_URL}}"
  local keycloak_issuer_url="${KEYCLOAK_URL}"
  local keycloak_realm="${KEYCLOAK_REALM}"

  # Casdoor configs
  local casdoor_enabled="${CASDOOR_ENABLED:-false}"
  local casdoor_endpoint="${CASDOOR_ENDPOINT:-http://127.0.0.1:8000}"
  local casdoor_public_endpoint="${CASDOOR_PUBLIC_ENDPOINT:-${CASDOOR_ENDPOINT}}"
  local casdoor_client_id="${CASDOOR_CLIENT_ID}"
  local casdoor_client_secret="${CASDOOR_CLIENT_SECRET}"
  local casdoor_org="${CASDOOR_ORGANIZATION:-yuanrong}"
  local casdoor_app="${CASDOOR_APPLICATION:-app-yuanrong}"
  local casdoor_admin_user="${CASDOOR_ADMIN_USER}"
  local casdoor_admin_password="${CASDOOR_ADMIN_PASSWORD}"
  local casdoor_pub_key="${CASDOOR_JWT_PUBLIC_KEY}"

  if [ -z "${keycloak_enabled}" ]; then
    keycloak_enabled="false"
  fi
  if [ -z "${keycloak_url}" ]; then
    keycloak_url="http://127.0.0.1:8080"
  fi
  if [ -z "${keycloak_realm}" ]; then
    keycloak_realm="yuanrong"
  fi

  jemalloc_path=""
  if [ "X${ENABLE_JEMALLOC}" == "Xtrue" ]; then
    jemalloc_path="${JEMALLOC_LIB_PATH}"
    if [ ! -f "${jemalloc_path}" ]; then
      log_warning "jemalloc lib path ${jemalloc_path} does not exist!"
    fi
  fi

  if [ "x${ENABLE_META_STORE}" == "xtrue" ] && [ "x${META_STORE_MODE}" == "xlocal" ]; then
    META_STORE_ADDRESS="${IP_ADDRESS}:${GLOBAL_SCHEDULER_PORT}"
  else
    META_STORE_ADDRESS="${ETCD_CLUSTER_ADDRESS}"
  fi

  # Determine if SSL should be enabled for iam_server
  # IAM_SSL_ENABLE independently toggles SSL for IAM; cert paths reuse global ssl_* values
  local iam_ssl_enable="false"
  if [ "X${IAM_SSL_ENABLE}" = "Xtrue" ] || [ "X${IAM_SSL_ENABLE}" = "XTRUE" ] || \
     [ "X${SSL_ENABLE}" = "Xtrue" ] || [ "X${SSL_ENABLE}" = "XTRUE" ]; then
    iam_ssl_enable="true"
    log_info "iam_server mTLS enabled, ssl_base_path=${SSL_BASE_PATH}"
  fi

  # Pass --iam_ssl_enable only when IAM_SSL_ENABLE is explicitly set (independent from global)
  local iam_ssl_args=""
  if [ -n "${IAM_SSL_ENABLE}" ] && ([ "X${IAM_SSL_ENABLE}" = "Xtrue" ] || [ "X${IAM_SSL_ENABLE}" = "XTRUE" ]); then
    iam_ssl_args="--iam_ssl_enable=true"
    log_info "iam_server independent SSL override enabled"
  fi

  # Local plaintext listener for co-deployed components
  local iam_local_args=""
  if [ -n "${IAM_LOCAL_LISTEN_PORT}" ] && [ "${IAM_LOCAL_LISTEN_PORT}" != "0" ]; then
    local iam_local_ip="${IAM_LOCAL_IP:-127.0.0.1}"
    iam_local_args="--local_ip=${iam_local_ip} --local_listen_port=${IAM_LOCAL_LISTEN_PORT}"
    log_info "iam_server local plaintext listener: ${iam_local_ip}:${IAM_LOCAL_LISTEN_PORT}"
  fi

  if check_port "${IP_ADDRESS}" "${IAM_SERVER_PORT}"; then
    OPENSSL_CONF="" LD_LIBRARY_PATH=${FUNCTION_SYSTEM_DIR}/lib:${LD_LIBRARY_PATH} \
      LD_PRELOAD="${jemalloc_path}":"${LD_PRELOAD}" \
      ${bin} --ip="${IP_ADDRESS}" \
      --http_listen_port="${IAM_SERVER_PORT}" \
      --meta_store_address="${META_STORE_ADDRESS}" \
      --log_config="${FS_LOG_CONFIG}" \
      --node_id="${NODE_ID}" \
      --enable_iam="true" \
      --enable_trace="${ENABLE_TRACE}" \
      --token_expired_time_span="${IAM_TOKEN_EXPIRED_TIME_SPAN}" \
      --iam_credential_type="${IAM_CREDENTIAL_TYPE}" \
      --election_mode=${ELECTION_MODE} \
      --ssl_enable="${iam_ssl_enable}" \
      --ssl_base_path="${SSL_BASE_PATH}" \
      --ssl_root_file="${SSL_ROOT_FILE}" \
      --ssl_cert_file="${SSL_CERT_FILE}" \
      --ssl_key_file="${SSL_KEY_FILE}" \
      ${iam_ssl_args} \
      ${iam_local_args} \
      --auth_provider="${auth_provider}" \
      --keycloak_enabled="${keycloak_enabled}" \
      --keycloak_url="${keycloak_url}" \
      --keycloak_issuer_url="${keycloak_issuer_url}" \
      --keycloak_realm="${keycloak_realm}" \
      --casdoor_enabled="${casdoor_enabled}" \
      --casdoor_endpoint="${casdoor_endpoint}" \
      --casdoor_public_endpoint="${casdoor_public_endpoint}" \
      --casdoor_client_id="${casdoor_client_id}" \
      --casdoor_client_secret="${casdoor_client_secret}" \
      --casdoor_organization="${casdoor_org}" \
      --casdoor_application="${casdoor_app}" \
      --casdoor_admin_user="${casdoor_admin_user}" \
      --casdoor_admin_password="${casdoor_admin_password}" \
      --casdoor_jwt_public_key="${casdoor_pub_key}" \
      >>"${FS_LOG_PATH}/${NODE_ID}-iam_server${STD_LOG_SUFFIX}" 2>&1 &
    IAM_SERVER_PID=$!
    if function_system_health_check "${IAM_SERVER_PID}" "${IAM_SERVER_PORT}" "iam-server" \
      "IAM_SSL_ENABLE" "IAM_LOCAL_LISTEN_PORT" "IAM_LOCAL_IP"; then
      log_info "succeed to start iam_server process, ip=${IP_ADDRESS}, port=${IAM_SERVER_PORT}, pid=${IAM_SERVER_PID}"
      return 0
    fi
    if [ ${IAM_SERVER_PID} -gt 0 ]; then
      log_warning "health check failed, killing iam_server process pid: ${IAM_SERVER_PID}"
      kill -9 ${IAM_SERVER_PID}
    fi
  fi
  return 1
}

function install_function_system() {
  config_install_dir="${INSTALL_DIR_PARENT}/config"
  [ -d "${config_install_dir}" ] || mkdir -p "${config_install_dir}"
  case "$1" in
  function_master)
    install_function_master
    ;;
  function_agent)
    install_function_agent
    ;;
  function_proxy)
    install_function_proxy
    ;;
  dashboard)
    install_dashboard
    ;;
  collector)
    install_collector
    ;;
  faas_frontend)
    install_faas_frontend
    ;;
  function_scheduler)
    install_function_scheduler
    ;;
  meta_service)
    install_metaservice
    ;;
  iam_server)
    install_iam_server
    ;;
  node_proxy)
    install_node_proxy
    ;;
  edge_frontend)
    install_edge_frontend
    ;;
  *)
    log_warning >&2 "Unknown component $1"
    return 1
    ;;
  esac
  return $?
}
