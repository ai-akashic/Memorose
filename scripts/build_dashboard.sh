#!/usr/bin/env bash

#
# build_dashboard.sh - Build Memorose Dashboard
#
# Description:
#   Builds the Next.js dashboard for standalone/server runtime
#
# Usage:
#   ./scripts/build_dashboard.sh [OPTIONS]
#
# Options:
#   --skip-install    Skip dependency installation
#   --clean           Clean build artifacts before building
#   -h, --help        Show this help message
#
# Requirements:
#   - pnpm
#   - Node.js 18+
#

set -euo pipefail

# Color output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

# Script directories
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly DASHBOARD_DIR="${ROOT_DIR}/dashboard"
# Options
SKIP_INSTALL=false
CLEAN_BUILD=false

# Logging functions
log_info() {
    echo "${BLUE}==>${NC} $*"
}

log_success() {
    echo "${GREEN}✓${NC} $*"
}

log_warn() {
    echo "${YELLOW}⚠${NC} $*"
}

log_error() {
    echo "${RED}✗${NC} $*" >&2
}

show_help() {
    sed -n '/^#/,/^$/s/^# \{0,1\}//p' "${BASH_SOURCE[0]}"
    exit 0
}

check_requirements() {
    log_info "Checking requirements..."

    if ! command -v pnpm &>/dev/null; then
        log_error "pnpm is required but not installed"
        log_error "Install with: npm install -g pnpm"
        exit 1
    fi

    if ! command -v node &>/dev/null; then
        log_error "Node.js is required but not installed"
        exit 1
    fi

    local node_version
    node_version=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
    if [[ ${node_version} -lt 18 ]]; then
        log_warn "Node.js 18+ is recommended (current: v${node_version})"
    fi

    log_success "All requirements met"
}

build_dashboard() {
    log_info "Building Memorose Dashboard..."

    # Change to dashboard directory
    cd "${DASHBOARD_DIR}" || {
        log_error "Dashboard directory not found: ${DASHBOARD_DIR}"
        exit 1
    }

    # Clean build if requested
    if [[ "${CLEAN_BUILD}" == "true" ]]; then
        log_info "Cleaning previous build artifacts..."
        rm -rf .next out node_modules/.cache
        log_success "Clean complete"
    fi

    # Install dependencies
    if [[ "${SKIP_INSTALL}" != "true" ]]; then
        log_info "Installing dependencies..."
        if pnpm install --frozen-lockfile 2>/dev/null; then
            log_success "Dependencies installed (frozen-lockfile)"
        else
            log_warn "Frozen lockfile failed, installing with regular install..."
            pnpm install
        fi
    else
        log_info "Skipping dependency installation"
    fi

    # Build standalone Next.js app
    log_info "Building dashboard application..."
    if pnpm build; then
        log_success "Build complete"
    else
        log_error "Build failed"
        exit 1
    fi

    # Verify build output
    if [[ ! -f ".next/BUILD_ID" ]]; then
        log_error "Build output '.next/BUILD_ID' not found"
        exit 1
    fi
}

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip-install)
                SKIP_INSTALL=true
                shift
                ;;
            --clean)
                CLEAN_BUILD=true
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

    echo ""
    log_info "Memorose Dashboard Build Script"
    echo ""

    check_requirements
    build_dashboard

    echo ""
    log_success "Dashboard build complete!"
    echo ""
    echo "  Build output: ${DASHBOARD_DIR}/.next"
    echo "  Start app:     cd ${DASHBOARD_DIR} && pnpm start --port 3100"
    echo "  Dashboard URL: http://localhost:3100/dashboard"
    echo ""
}

# Trap errors
trap 'log_error "Build failed at line $LINENO"' ERR

main "$@"
