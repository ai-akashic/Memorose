#!/usr/bin/env bash

#
# start_cluster.sh - Memorose Local Development Cluster
#
# ⚠️  NOTICE: This is a LOCAL DEVELOPMENT TOOL
#     This script simulates a multi-node cluster on a SINGLE machine for
#     development and testing purposes. It is NOT intended for production use.
#
# Description:
#   Start, stop, or restart Memorose server in standalone or simulated cluster mode
#   - Standalone: Single node on port 3000
#   - Cluster: 3 nodes on ports 3000-3002 (all on localhost)
#
# Usage:
#   ./scripts/start_cluster.sh COMMAND [OPTIONS]
#
# Commands:
#   start       Start the server(s)
#   stop        Stop all running server(s)
#   restart     Stop and then start the server(s)
#   status      Show status of running server(s)
#
# Options:
#   -m, --mode MODE   Set execution mode: standalone or cluster (default: cluster)
#   -c, --clean       Remove data directory before start
#   -b, --build       Rebuild Rust and UI before starting (default: check and build if needed)
#   -f, --force       Force rebuild even if binaries exist
#   -h, --help        Show this help message
#
# Examples:
#   ./scripts/start_cluster.sh start                # Auto-detect and build if needed
#   ./scripts/start_cluster.sh start --build        # Force rebuild everything
#   ./scripts/start_cluster.sh start -b -m standalone
#   ./scripts/start_cluster.sh start --clean --build  # Clean data + rebuild
#   ./scripts/start_cluster.sh stop
#   ./scripts/start_cluster.sh status
#
# Environment:
#   All nodes run on localhost (127.0.0.1)
#   Ports: 3000-3002 (HTTP), 5001-5003 (Raft)
#   Data: ./data/node-*
#   Logs: ./logs/node*.log
#

set -euo pipefail

# Color output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly CYAN='\033[0;36m'
readonly NC='\033[0m' # No Color

# Script directories
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Configuration
readonly SERVER_BIN="${ROOT_DIR}/target/debug/memorose-server"
readonly LOG_DIR="${ROOT_DIR}/logs"
readonly DATA_DIR="${ROOT_DIR}/data"
readonly PID_DIR="${ROOT_DIR}/.pids"
readonly DASHBOARD_DIR="${ROOT_DIR}/crates/memorose-server/static/dashboard"

# Options
COMMAND=""
MODE="cluster"
CLEAN_DATA=false
FORCE_BUILD=false

# Logging functions
log_info() {
    echo -e "${BLUE}==>${NC} $*"
}

log_success() {
    echo -e "${GREEN}✓${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $*"
}

log_error() {
    echo -e "${RED}✗${NC} $*" >&2
}

show_help() {
    sed -n '/^#/,/^$/s/^# \{0,1\}//p' "${BASH_SOURCE[0]}"
    exit 0
}

# Load environment variables from .env file
load_env() {
    local env_file="${ROOT_DIR}/.env"

    if [[ -f "${env_file}" ]]; then
        log_info "Loading environment from .env..."
        # Safe env loading - only export valid variable assignments
        # Use a temporary file instead of process substitution for compatibility
        local temp_file
        temp_file=$(mktemp)
        grep -v '^#' "${env_file}" | grep -v '^[[:space:]]*$' > "${temp_file}"

        while IFS='=' read -r key value; do
            # Skip empty keys
            [[ -z "${key}" ]] && continue

            # Remove quotes from value
            value="${value%\"}"
            value="${value#\"}"

            export "${key}=${value}"
        done < "${temp_file}"

        rm -f "${temp_file}"
    fi
}

# Check if process is running
is_process_running() {
    local pid="$1"
    [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

# Get PID from file
get_pid() {
    local node_id="$1"
    local pid_file="${PID_DIR}/node${node_id}.pid"

    if [[ -f "${pid_file}" ]]; then
        cat "${pid_file}"
    fi
}

# Save PID to file
save_pid() {
    local node_id="$1"
    local pid="$2"
    local pid_file="${PID_DIR}/node${node_id}.pid"

    mkdir -p "${PID_DIR}"
    echo "${pid}" > "${pid_file}"
}

# Remove PID file
remove_pid() {
    local node_id="$1"
    local pid_file="${PID_DIR}/node${node_id}.pid"

    [[ -f "${pid_file}" ]] && rm -f "${pid_file}"
}

# Wait for service to be ready
wait_for_ready() {
    local port="$1"
    local max_attempts=30
    local attempt=0

    while [[ ${attempt} -lt ${max_attempts} ]]; do
        if curl -sf "http://localhost:${port}/" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        ((attempt++))
    done

    log_error "Service on port ${port} did not become ready in ${max_attempts}s"
    return 1
}

# Stop all servers
stop_servers() {
    log_info "Stopping all Memorose processes..."

    local stopped=0
    local pids=()

    # Try to stop via PID files first
    if [[ -d "${PID_DIR}" ]]; then
        for pid_file in "${PID_DIR}"/node*.pid; do
            [[ -f "${pid_file}" ]] || continue

            local pid
            pid=$(cat "${pid_file}")

            if is_process_running "${pid}"; then
                log_info "Stopping process ${pid}..."
                kill "${pid}" 2>/dev/null || true
                pids+=("${pid}")
                ((stopped++))
            fi

            rm -f "${pid_file}"
        done
    fi

    # Wait for graceful shutdown
    if [[ ${#pids[@]} -gt 0 ]]; then
        local waited=0
        while [[ ${waited} -lt 10 ]]; do
            local all_stopped=true

            for pid in "${pids[@]}"; do
                if is_process_running "${pid}"; then
                    all_stopped=false
                    break
                fi
            done

            if [[ "${all_stopped}" == "true" ]]; then
                break
            fi

            sleep 1
            ((waited++))
        done
    fi

    # Force kill if still running
    if pgrep -f "memorose-server" >/dev/null 2>&1; then
        log_warn "Some processes still running, force stopping..."
        pkill -9 -f "memorose-server" 2>/dev/null || true
        ((stopped++))
    fi

    # Clean up PID directory
    [[ -d "${PID_DIR}" ]] && rm -rf "${PID_DIR}"

    if [[ ${stopped} -gt 0 ]]; then
        log_success "Stopped ${stopped} process(es)"
    else
        log_info "No processes were running"
    fi
}

# Show server status
show_status() {
    log_info "Memorose Server Status (Local Development)"
    echo ""

    local running=0

    if [[ "${MODE}" == "standalone" ]]; then
        local pid
        pid=$(get_pid 1)

        if [[ -n "${pid}" ]] && is_process_running "${pid}"; then
            echo -e "  ${GREEN}●${NC} Standalone Server (PID: ${pid})"
            echo "    URL: http://localhost:3000"
            ((running++))
        else
            echo -e "  ${RED}●${NC} Standalone Server: not running"
        fi
    else
        echo -e "  ${YELLOW}Mode:${NC} Simulated Cluster (3 nodes on localhost)"
        echo ""
        for node_id in 1 2 3; do
            local pid
            pid=$(get_pid "${node_id}")
            local port=$((3000 + node_id - 1))

            if [[ -n "${pid}" ]] && is_process_running "${pid}"; then
                echo -e "  ${GREEN}●${NC} Node ${node_id} (PID: ${pid})"
                echo "    URL: http://localhost:${port}"
                ((running++))
            else
                echo -e "  ${RED}●${NC} Node ${node_id}: not running"
            fi
        done
    fi

    echo ""

    if [[ ${running} -gt 0 ]]; then
        log_success "${running} server(s) running"
        echo ""
        log_info "Dashboard: http://localhost:3000/dashboard (admin/admin)"
        log_info "Logs: ${LOG_DIR}/"
        echo ""
        log_warn "ℹ️  This is a local development environment"
    else
        log_warn "No servers running"
    fi

    echo ""
}

# Check and build prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    local needs_rust_build=false
    local needs_ui_build=false

    # Check if Rust rebuild is needed
    if [[ "${FORCE_BUILD}" == "true" ]]; then
        log_info "Force rebuild requested"
        needs_rust_build=true
        needs_ui_build=true
    else
        # Check Rust binary
        if [[ ! -f "${SERVER_BIN}" ]]; then
            log_warn "Server binary not found"
            needs_rust_build=true
        fi

        # Check dashboard
        if [[ ! -d "${DASHBOARD_DIR}" ]] || [[ ! -f "${DASHBOARD_DIR}/index.html" ]]; then
            log_warn "Dashboard not built"
            needs_ui_build=true
        fi
    fi

    # Build Rust if needed
    if [[ "${needs_rust_build}" == "true" ]]; then
        log_info "Building Rust server..."
        cd "${ROOT_DIR}" || exit 1

        if cargo build -p memorose-server; then
            log_success "Rust build complete"
        else
            log_error "Rust build failed"
            exit 1
        fi
    else
        log_success "Rust binary is ready: ${SERVER_BIN}"
    fi

    # Build UI if needed
    if [[ "${needs_ui_build}" == "true" ]]; then
        log_info "Building dashboard UI..."
        if [[ -x "${SCRIPT_DIR}/build_dashboard.sh" ]]; then
            "${SCRIPT_DIR}/build_dashboard.sh"
        else
            log_error "build_dashboard.sh not found or not executable"
            exit 1
        fi
    else
        log_success "Dashboard is ready"
    fi

    log_success "All prerequisites ready"
}

# Start servers in standalone mode
start_standalone() {
    log_info "Starting Memorose in STANDALONE mode (local development)..."
    log_warn "This is a single-node instance running on localhost"
    echo ""

    mkdir -p "${LOG_DIR}"

    local log_file="${LOG_DIR}/standalone.log"
    local node_id=1
    local port=3000
    local raft_addr="127.0.0.1:5001"

    log_info "Starting Node 1 (Port ${port})..."

    # Start server in background
    NODE_ID="${node_id}" PORT="${port}" RAFT_ADDR="${raft_addr}" \
        "${SERVER_BIN}" > "${log_file}" 2>&1 &

    local pid=$!
    save_pid "${node_id}" "${pid}"

    log_info "Waiting for server to be ready..."
    if ! wait_for_ready "${port}"; then
        log_error "Failed to start server"
        log_error "Check logs: ${log_file}"
        exit 1
    fi

    log_info "Initializing cluster..."
    local init_result
    init_result=$(curl -sf -X POST "http://localhost:${port}/v1/cluster/initialize" 2>&1) || true
    log_info "Initialize response: ${init_result}"

    echo ""
    log_success "Memorose Standalone is READY! (Local Development)"
    echo ""
    echo -e "  ${CYAN}Endpoint:${NC}  http://localhost:${port}"
    echo -e "  ${CYAN}Dashboard:${NC} http://localhost:${port}/dashboard ${YELLOW}(admin/admin)${NC}"
    echo -e "  ${CYAN}Logs:${NC}      ${log_file}"
    echo ""
    log_warn "⚠️  This is a local development instance - not for production use"
    echo ""
}

# Start servers in cluster mode
start_cluster() {
    log_info "Starting Memorose in SIMULATED CLUSTER mode (local development)..."
    log_warn "This runs 3 nodes on a SINGLE machine for testing purposes"
    log_warn "NOT suitable for production - all nodes share localhost resources"
    echo ""

    mkdir -p "${LOG_DIR}"

    # Detect warm restart
    local warm_restart=false
    if [[ -d "${DATA_DIR}/node-1" ]] || [[ -d "${DATA_DIR}/shard_0" ]]; then
        warm_restart=true
        log_info "Detected existing data — warm restart"
    fi

    # Start nodes
    local nodes=(
        "1:3000:127.0.0.1:5001"
        "2:3001:127.0.0.1:5002"
        "3:3002:127.0.0.1:5003"
    )

    for node_config in "${nodes[@]}"; do
        IFS=':' read -r node_id port raft_addr <<< "${node_config}"

        local log_file="${LOG_DIR}/node${node_id}.log"

        log_info "Starting Node ${node_id} (Port ${port})..."

        NODE_ID="${node_id}" PORT="${port}" RAFT_ADDR="${raft_addr}" \
            "${SERVER_BIN}" > "${log_file}" 2>&1 &

        local pid=$!
        save_pid "${node_id}" "${pid}"
    done

    log_info "Waiting for all nodes to be ready..."
    for port in 3000 3001 3002; do
        if ! wait_for_ready "${port}"; then
            log_error "Failed to start node on port ${port}"
            log_error "Check logs in: ${LOG_DIR}/"
            stop_servers
            exit 1
        fi
    done

    if [[ "${warm_restart}" == "true" ]]; then
        log_info "Waiting for Raft leader election..."
        sleep 3
        log_success "Cluster resumed from persisted state"
    else
        log_info "Initializing cluster on Node 1..."
        local init_result
        init_result=$(curl -sf -X POST "http://localhost:3000/v1/cluster/initialize" 2>&1) || true
        log_info "Initialize: ${init_result}"

        sleep 3

        log_info "Joining Node 2..."
        local join2_result
        join2_result=$(curl -sf --max-time 15 -X POST "http://localhost:3000/v1/cluster/join" \
            -H "Content-Type: application/json" \
            -d '{"node_id": 2, "address": "127.0.0.1:5002"}' 2>&1) || true
        log_info "Join Node 2: ${join2_result}"

        sleep 1

        log_info "Joining Node 3..."
        local join3_result
        join3_result=$(curl -sf --max-time 15 -X POST "http://localhost:3000/v1/cluster/join" \
            -H "Content-Type: application/json" \
            -d '{"node_id": 3, "address": "127.0.0.1:5003"}' 2>&1) || true
        log_info "Join Node 3: ${join3_result}"
    fi

    echo ""
    log_success "Memorose Local Cluster is READY! (Development/Testing)"
    echo ""
    echo "  ────────────────────────────────────────────────────────"
    echo -e "  ${YELLOW}⚠  LOCAL SIMULATION${NC} - All nodes run on localhost"
    echo "  ────────────────────────────────────────────────────────"
    echo -e "  ${CYAN}Node 1:${NC}    http://localhost:3000"
    echo -e "  ${CYAN}Node 2:${NC}    http://localhost:3001"
    echo -e "  ${CYAN}Node 3:${NC}    http://localhost:3002"
    echo -e "  ${CYAN}Dashboard:${NC} http://localhost:3000/dashboard ${YELLOW}(admin/admin)${NC}"
    echo "  ────────────────────────────────────────────────────────"
    echo -e "  ${CYAN}Logs:${NC}      ${LOG_DIR}/"
    echo ""
    log_warn "⚠️  This simulates a cluster on ONE machine for development only"
    echo ""
}

# Start servers based on mode
start_servers() {
    load_env
    check_prerequisites

    # Clean data if requested
    if [[ "${CLEAN_DATA}" == "true" ]]; then
        log_warn "Cleaning data directory..."
        rm -rf "${DATA_DIR}"
        log_success "Data cleaned"
    fi

    # Start based on mode
    if [[ "${MODE}" == "standalone" ]]; then
        start_standalone
    elif [[ "${MODE}" == "cluster" ]]; then
        start_cluster
    else
        log_error "Invalid mode: ${MODE}"
        exit 1
    fi
}

# Main function
main() {
    # Parse command
    if [[ $# -gt 0 ]] && [[ "$1" =~ ^(start|stop|restart|status)$ ]]; then
        COMMAND="$1"
        shift
    fi

    # Parse options
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -m|--mode)
                if [[ -z "${2:-}" ]] || [[ "$2" =~ ^- ]]; then
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
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                ;;
        esac
    done

    # Validate command
    if [[ -z "${COMMAND}" ]]; then
        log_error "No command specified"
        echo ""
        show_help
    fi

    # Validate mode
    if [[ ! "${MODE}" =~ ^(standalone|cluster)$ ]]; then
        log_error "Invalid mode: ${MODE}. Must be 'standalone' or 'cluster'"
        exit 1
    fi

    # Change to root directory
    cd "${ROOT_DIR}" || {
        log_error "Failed to change to root directory: ${ROOT_DIR}"
        exit 1
    }

    # Execute command
    case "${COMMAND}" in
        start)
            start_servers
            ;;
        stop)
            stop_servers
            ;;
        restart)
            stop_servers
            sleep 2
            start_servers
            ;;
        status)
            show_status
            ;;
    esac
}

# Trap errors
trap 'log_error "Command failed at line $LINENO"' ERR

main "$@"
