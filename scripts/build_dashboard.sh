#!/usr/bin/env bash

#
# build_dashboard.sh - Build Memorose Dashboard
#
# Description:
#   Builds the Next.js dashboard and copies static files to the server directory
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
readonly STATIC_DIR="${ROOT_DIR}/crates/memorose-server/static/dashboard"

# Options
SKIP_INSTALL=false
CLEAN_BUILD=false

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

    # Build static export
    log_info "Building static export..."
    if pnpm build; then
        log_success "Build complete"
    else
        log_error "Build failed"
        exit 1
    fi

    # Verify build output
    if [[ ! -d "out" ]]; then
        log_error "Build output directory 'out' not found"
        exit 1
    fi
}

copy_static_files() {
    log_info "Copying static files to server directory..."

    # Remove old static directory
    if [[ -d "${STATIC_DIR}" ]]; then
        rm -rf "${STATIC_DIR}"
    fi

    # Create static directory
    mkdir -p "${STATIC_DIR}"

    # Copy build output
    cp -r "${DASHBOARD_DIR}/out/"* "${STATIC_DIR}/"

    log_success "Static files copied to: ${STATIC_DIR}"
}

create_redirect() {
    # Ensure root index.html exists (redirects to login)
    if [[ ! -f "${STATIC_DIR}/index.html" ]]; then
        log_info "Creating root redirect..."
        cat > "${STATIC_DIR}/index.html" <<'EOF'
<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="0;url=/dashboard/login/">
    <title>Redirecting...</title>
</head>
<body>
    <p>Redirecting to dashboard...</p>
</body>
</html>
EOF
        log_success "Created root redirect index.html"
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
    copy_static_files
    create_redirect

    echo ""
    log_success "Dashboard build complete!"
    echo ""
    echo "  Static files: ${STATIC_DIR}"
    echo "  Start server:  cargo run -p memorose-server"
    echo "  Dashboard URL: http://localhost:3000/dashboard"
    echo ""
}

# Trap errors
trap 'log_error "Build failed at line $LINENO"' ERR

main "$@"
