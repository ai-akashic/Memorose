#!/usr/bin/env bash

set -euo pipefail

readonly RED=$'\033[0;31m'
readonly GREEN=$'\033[0;32m'
readonly YELLOW=$'\033[1;33m'
readonly BLUE=$'\033[0;34m'
readonly CYAN=$'\033[0;36m'
readonly NC=$'\033[0m'

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly SERVER_BIN="${ROOT_DIR}/target/debug/memorose-server"
readonly DASHBOARD_DIR="${ROOT_DIR}/dashboard"
readonly BUILD_DASHBOARD_SCRIPT="${SCRIPT_DIR}/build_dashboard.sh"
readonly LOG_DIR="${ROOT_DIR}/logs"
readonly DATA_DIR="${ROOT_DIR}/data"
readonly PID_DIR="${ROOT_DIR}/.pids"
readonly DASHBOARD_PORT=3100
readonly DASHBOARD_PID_FILE="${PID_DIR}/dashboard.pid"
readonly STANDALONE_NODE_SPEC="1:3000:127.0.0.1:5001"
readonly CLUSTER_NODE_SPECS=(
    "1:3000:127.0.0.1:5001"
    "2:3001:127.0.0.1:5002"
    "3:3002:127.0.0.1:5003"
)

COMMAND=""
MODE="cluster"
CLEAN_DATA=false
FORCE_BUILD=false

log_info() {
    printf '%s\n' "${BLUE}==>${NC} $*"
}

log_success() {
    printf '%s\n' "${GREEN}✓${NC} $*"
}

log_warn() {
    printf '%s\n' "${YELLOW}⚠${NC} $*"
}

log_error() {
    printf '%s\n' "${RED}✗${NC} $*" >&2
}

show_help() {
    cat <<'EOF'
start_cluster.sh - Memorose local development launcher

Usage:
  ./scripts/start_cluster.sh COMMAND [OPTIONS]

Commands:
  start       Start Memorose server(s) and dashboard
  stop        Stop all started server(s) and dashboard
  restart     Stop and then start again
  status      Show process status

Options:
  -m, --mode MODE   standalone | cluster (default: cluster)
  -c, --clean       Remove the data directory before start
  -b, --build       Rebuild server and dashboard before start
  -f, --force       Alias of --build
  -h, --help        Show this help message

Examples:
  ./scripts/start_cluster.sh start
  ./scripts/start_cluster.sh start --mode standalone
  ./scripts/start_cluster.sh start --clean --build
  ./scripts/start_cluster.sh status --mode cluster

Notes:
  - This script is for local development only.
  - Cluster mode simulates 3 nodes on one machine.
  - Dashboard runs on http://localhost:3100/dashboard
EOF
}

require_command() {
    local cmd="$1"
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        log_error "Required command not found: ${cmd}"
        exit 1
    fi
}

trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "${value}"
}

load_env() {
    local env_file="${ROOT_DIR}/.env"
    local line key value

    [[ -f "${env_file}" ]] || return 0

    log_info "Loading environment from ${env_file##${ROOT_DIR}/}"
    while IFS= read -r line || [[ -n "${line}" ]]; do
        line="$(trim "${line}")"
        [[ -z "${line}" || "${line}" == \#* ]] && continue
        [[ "${line}" == export\ * ]] && line="${line#export }"
        [[ "${line}" != *=* ]] && continue

        key="$(trim "${line%%=*}")"
        value="${line#*=}"
        value="$(trim "${value}")"

        if [[ ! "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
            log_warn "Skipping invalid env key in .env: ${key}"
            continue
        fi

        if [[ "${value}" =~ ^\".*\"$ || "${value}" =~ ^\'.*\'$ ]]; then
            value="${value:1:${#value}-2}"
        fi

        export "${key}=${value}"
    done < "${env_file}"
}

ensure_runtime_dirs() {
    mkdir -p "${LOG_DIR}" "${PID_DIR}"
}

pid_file_for_node() {
    local node_id="$1"
    printf '%s/node%s.pid' "${PID_DIR}" "${node_id}"
}

log_file_for_node() {
    local node_id="$1"
    if [[ "${MODE}" == "standalone" ]]; then
        printf '%s/standalone.log' "${LOG_DIR}"
    else
        printf '%s/node%s.log' "${LOG_DIR}" "${node_id}"
    fi
}

read_pid_file() {
    local pid_file="$1"
    [[ -f "${pid_file}" ]] || return 0
    cat "${pid_file}"
}

is_process_running() {
    local pid="${1:-}"
    [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

save_pid() {
    local pid_file="$1"
    local pid="$2"
    mkdir -p "${PID_DIR}"
    printf '%s\n' "${pid}" > "${pid_file}"
}

remove_pid_file() {
    local pid_file="$1"
    [[ -f "${pid_file}" ]] && rm -f "${pid_file}"
}

wait_for_http() {
    local url="$1"
    local label="$2"
    local attempts="${3:-30}"
    local attempt=1

    while (( attempt <= attempts )); do
        if curl -fsS --max-time 2 "${url}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        ((attempt++))
    done

    log_error "${label} did not become ready: ${url}"
    return 1
}

tail_log_file() {
    local log_file="$1"
    local lines="${2:-20}"

    [[ -f "${log_file}" ]] || return 0
    echo ""
    log_info "Last ${lines} lines from ${log_file##${ROOT_DIR}/}:"
    tail -n "${lines}" "${log_file}" || true
}

wait_for_pid_and_http() {
    local pid="$1"
    local url="$2"
    local label="$3"
    local log_file="$4"
    local attempts="${5:-30}"
    local attempt=1

    while (( attempt <= attempts )); do
        if ! is_process_running "${pid}"; then
            log_error "${label} exited before becoming ready"
            tail_log_file "${log_file}" 25
            return 1
        fi

        if curl -fsS --max-time 2 "${url}" >/dev/null 2>&1; then
            return 0
        fi

        sleep 1
        ((attempt++))
    done

    log_error "${label} did not become ready: ${url}"
    tail_log_file "${log_file}" 25
    return 1
}

check_prerequisites() {
    require_command cargo
    require_command curl
    require_command pnpm

    local needs_rust_build=false
    local needs_ui_build=false

    if [[ "${FORCE_BUILD}" == "true" ]]; then
        needs_rust_build=true
        needs_ui_build=true
    fi

    if [[ ! -x "${SERVER_BIN}" ]]; then
        needs_rust_build=true
    fi

    if [[ ! -f "${DASHBOARD_DIR}/.next/BUILD_ID" ]]; then
        needs_ui_build=true
    fi

    if [[ "${needs_rust_build}" == "true" ]]; then
        log_info "Building memorose-server..."
        (
            cd "${ROOT_DIR}"
            cargo build -p memorose-server
        )
        log_success "Rust build complete"
    else
        log_success "Rust binary ready: ${SERVER_BIN}"
    fi

    if [[ "${needs_ui_build}" == "true" ]]; then
        if [[ ! -x "${BUILD_DASHBOARD_SCRIPT}" ]]; then
            log_error "Dashboard build script is missing or not executable: ${BUILD_DASHBOARD_SCRIPT}"
            exit 1
        fi
        log_info "Building dashboard..."
        "${BUILD_DASHBOARD_SCRIPT}"
    else
        log_success "Dashboard build ready"
    fi
}

stop_pid_file_if_running() {
    local pid_file="$1"
    local label="$2"
    local pid

    pid="$(read_pid_file "${pid_file}")"
    if [[ -z "${pid}" ]]; then
        remove_pid_file "${pid_file}"
        return 0
    fi

    if ! is_process_running "${pid}"; then
        remove_pid_file "${pid_file}"
        return 0
    fi

    log_info "Stopping ${label} (PID: ${pid})..."
    kill "${pid}" 2>/dev/null || true

    local waited=0
    while (( waited < 10 )); do
        if ! is_process_running "${pid}"; then
            remove_pid_file "${pid_file}"
            return 0
        fi
        sleep 1
        ((waited++))
    done

    log_warn "${label} did not exit gracefully, sending SIGKILL"
    kill -9 "${pid}" 2>/dev/null || true
    remove_pid_file "${pid_file}"
}

stop_dashboard() {
    stop_pid_file_if_running "${DASHBOARD_PID_FILE}" "dashboard"
}

stop_servers() {
    log_info "Stopping Memorose processes..."

    local stopped_any=false
    local node_id pid_file

    if [[ -d "${PID_DIR}" ]]; then
        for pid_file in "${PID_DIR}"/node*.pid; do
            [[ -f "${pid_file}" ]] || continue
            node_id="${pid_file##*/node}"
            node_id="${node_id%.pid}"
            stop_pid_file_if_running "${pid_file}" "node ${node_id}"
            stopped_any=true
        done
    fi

    stop_dashboard

    if pgrep -f "/memorose-server" >/dev/null 2>&1; then
        log_warn "Found memorose-server processes outside tracked PID files, stopping them"
        pkill -9 -f "/memorose-server" 2>/dev/null || true
        stopped_any=true
    fi

    [[ -d "${PID_DIR}" ]] && rm -rf "${PID_DIR}"

    if [[ "${stopped_any}" == "true" ]]; then
        log_success "All tracked processes stopped"
    else
        log_info "No tracked processes were running"
    fi
}

show_node_status() {
    local node_id="$1"
    local port="$2"
    local pid_file
    local pid

    pid_file="$(pid_file_for_node "${node_id}")"
    pid="$(read_pid_file "${pid_file}")"

    if [[ -n "${pid}" ]] && is_process_running "${pid}"; then
        printf '%s\n' "  ${GREEN}●${NC} Node ${node_id} (PID: ${pid})"
        echo "    URL: http://localhost:${port}"
        return 0
    fi

    printf '%s\n' "  ${RED}●${NC} Node ${node_id}: not running"
    return 1
}

show_status() {
    local running=0
    local dashboard_pid

    log_info "Memorose status (${MODE})"
    echo ""

    if [[ "${MODE}" == "standalone" ]]; then
        if show_node_status 1 3000; then
            ((running += 1))
        fi
    else
        printf '%s\n' "  ${YELLOW}Mode:${NC} Simulated cluster (3 local nodes)"
        echo ""
        if show_node_status 1 3000; then ((running += 1)); fi
        if show_node_status 2 3001; then ((running += 1)); fi
        if show_node_status 3 3002; then ((running += 1)); fi
    fi

    echo ""

    dashboard_pid="$(read_pid_file "${DASHBOARD_PID_FILE}")"
    if [[ -n "${dashboard_pid}" ]] && is_process_running "${dashboard_pid}"; then
        printf '%s\n' "  ${GREEN}●${NC} Dashboard (PID: ${dashboard_pid})"
        echo "    URL: http://localhost:${DASHBOARD_PORT}/dashboard"
    else
        printf '%s\n' "  ${RED}●${NC} Dashboard: not running"
    fi

    echo ""
    if (( running > 0 )); then
        log_success "${running} server(s) running"
        log_info "Logs: ${LOG_DIR}/"
    else
        log_warn "No server nodes are running"
    fi
}

start_dashboard() {
    ensure_runtime_dirs

    local existing_pid
    existing_pid="$(read_pid_file "${DASHBOARD_PID_FILE}")"
    if [[ -n "${existing_pid}" ]] && is_process_running "${existing_pid}"; then
        log_info "Dashboard already running (PID: ${existing_pid})"
        return 0
    fi
    remove_pid_file "${DASHBOARD_PID_FILE}"

    local log_file="${LOG_DIR}/dashboard.log"
    log_info "Starting dashboard on port ${DASHBOARD_PORT}..."

    (
        cd "${DASHBOARD_DIR}"
        PORT="${DASHBOARD_PORT}" HOSTNAME="127.0.0.1" pnpm start
    ) >"${log_file}" 2>&1 &

    local pid=$!
    save_pid "${DASHBOARD_PID_FILE}" "${pid}"

    if ! wait_for_pid_and_http \
        "${pid}" \
        "http://127.0.0.1:${DASHBOARD_PORT}/dashboard/login/" \
        "Dashboard" \
        "${log_file}"; then
        remove_pid_file "${DASHBOARD_PID_FILE}"
        log_error "Dashboard failed to start. Check logs: ${log_file}"
        return 1
    fi

    log_success "Dashboard ready at http://localhost:${DASHBOARD_PORT}/dashboard"
}

start_node() {
    local node_id="$1"
    local port="$2"
    local raft_addr="$3"
    local log_file
    local pid_file
    local pid

    ensure_runtime_dirs
    log_file="$(log_file_for_node "${node_id}")"
    pid_file="$(pid_file_for_node "${node_id}")"

    log_info "Starting node ${node_id} on http://localhost:${port} (raft: ${raft_addr})..."
    (
        cd "${ROOT_DIR}"
        env \
            NODE_ID="${node_id}" \
            RAFT_ADDR="${raft_addr}" \
            NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}" \
            no_proxy="${no_proxy:-127.0.0.1,localhost}" \
            "${SERVER_BIN}"
    ) >"${log_file}" 2>&1 &

    pid=$!
    save_pid "${pid_file}" "${pid}"

    if ! wait_for_pid_and_http \
        "${pid}" \
        "http://127.0.0.1:${port}/" \
        "Node ${node_id}" \
        "${log_file}"; then
        remove_pid_file "${pid_file}"
        log_error "Node ${node_id} failed to start. Check logs: ${log_file}"
        return 1
    fi

    log_success "Node ${node_id} is ready"
}

initialize_single_node() {
    log_info "Initializing single-node Raft cluster..."
    local response
    response="$(curl -fsS -X POST "http://127.0.0.1:3000/v1/cluster/initialize" 2>&1 || true)"
    log_info "Initialize response: ${response}"
}

join_cluster_node() {
    local joiner_id="$1"
    local joiner_addr="$2"
    local response

    log_info "Joining node ${joiner_id} (${joiner_addr})..."
    response="$(
        curl -fsS --max-time 15 \
            -X POST "http://127.0.0.1:3000/v1/cluster/join" \
            -H "Content-Type: application/json" \
            -d "{\"node_id\": ${joiner_id}, \"address\": \"${joiner_addr}\"}" 2>&1 || true
    )"
    log_info "Join response for node ${joiner_id}: ${response}"
}

start_standalone() {
    log_info "Starting Memorose in standalone mode"
    start_node 1 3000 "127.0.0.1:5001"
    initialize_single_node
    start_dashboard

    echo ""
    log_success "Standalone environment is ready"
    printf '%s\n' "  ${CYAN}API:${NC}       http://localhost:3000"
    printf '%s\n' "  ${CYAN}Dashboard:${NC} http://localhost:${DASHBOARD_PORT}/dashboard"
    printf '%s\n' "  ${CYAN}Logs:${NC}      ${LOG_DIR}/standalone.log"
}

start_cluster() {
    log_info "Starting Memorose in simulated cluster mode"
    log_warn "This runs 3 local nodes on one machine for development only"

    local warm_restart=false
    if [[ -d "${DATA_DIR}/node-1" || -d "${DATA_DIR}/shard_0" ]]; then
        warm_restart=true
        log_info "Detected existing cluster data; treating as warm restart"
    fi

    local spec node_id port raft_addr
    for spec in "${CLUSTER_NODE_SPECS[@]}"; do
        IFS=':' read -r node_id port raft_addr <<< "${spec}"
        start_node "${node_id}" "${port}" "${raft_addr}"
    done

    if [[ "${warm_restart}" == "true" ]]; then
        log_info "Waiting briefly for leader election..."
        sleep 3
    else
        log_info "Bootstrapping fresh cluster..."
        initialize_single_node
        sleep 3
        join_cluster_node 2 "127.0.0.1:5002"
        sleep 1
        join_cluster_node 3 "127.0.0.1:5003"
        sleep 1
    fi

    start_dashboard

    echo ""
    log_success "Local cluster is ready"
    printf '%s\n' "  ${CYAN}Node 1:${NC}    http://localhost:3000"
    printf '%s\n' "  ${CYAN}Node 2:${NC}    http://localhost:3001"
    printf '%s\n' "  ${CYAN}Node 3:${NC}    http://localhost:3002"
    printf '%s\n' "  ${CYAN}Dashboard:${NC} http://localhost:${DASHBOARD_PORT}/dashboard"
    printf '%s\n' "  ${CYAN}Logs:${NC}      ${LOG_DIR}/"
}

start_servers() {
    load_env
    check_prerequisites

    if [[ "${CLEAN_DATA}" == "true" ]]; then
        log_warn "Removing data directory: ${DATA_DIR}"
        rm -rf "${DATA_DIR}"
        log_success "Data directory removed"
    fi

    case "${MODE}" in
        standalone)
            start_standalone
            ;;
        cluster)
            start_cluster
            ;;
        *)
            log_error "Invalid mode: ${MODE}"
            exit 1
            ;;
    esac
}

parse_args() {
    if [[ $# -gt 0 ]] && [[ "$1" =~ ^(start|stop|restart|status)$ ]]; then
        COMMAND="$1"
        shift
    fi

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -m|--mode)
                if [[ -z "${2:-}" ]] || [[ "${2}" == -* ]]; then
                    log_error "--mode requires an argument"
                    exit 1
                fi
                MODE="$2"
                shift 2
                ;;
            -c|--clean)
                CLEAN_DATA=true
                shift
                ;;
            -b|--build|-f|--force)
                FORCE_BUILD=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                echo ""
                show_help
                exit 1
                ;;
        esac
    done

    if [[ -z "${COMMAND}" ]]; then
        log_error "No command specified"
        echo ""
        show_help
        exit 1
    fi

    if [[ ! "${MODE}" =~ ^(standalone|cluster)$ ]]; then
        log_error "Invalid mode: ${MODE}. Expected standalone or cluster"
        exit 1
    fi
}

main() {
    parse_args "$@"
    cd "${ROOT_DIR}"

    case "${COMMAND}" in
        start)
            start_servers
            ;;
        stop)
            stop_servers
            ;;
        restart)
            stop_servers
            start_servers
            ;;
        status)
            show_status
            ;;
        *)
            log_error "Unsupported command: ${COMMAND}"
            exit 1
            ;;
    esac
}

main "$@"
