#!/usr/bin/env bash

set -euo pipefail

readonly RED=$'\033[0;31m'
readonly GREEN=$'\033[0;32m'
readonly YELLOW=$'\033[1;33m'
readonly BLUE=$'\033[0;34m'
readonly NC=$'\033[0m'

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly DOCKERFILE="${ROOT_DIR}/Dockerfile"
readonly CONTEXT_DIR="${ROOT_DIR}"
readonly IMAGE="dylan2024/memorose"
readonly TAGS="latest"

PUSH=true
MERGE=true
LOAD=false
TARGET="unified-runner"
BUILDER="${BUILDER_NAME:-memorose-arm64}"

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
publish_arm64_image.sh - Build Memorose ARM64 image locally and merge manifests

Usage:
  ./scripts/publish_arm64_image.sh [OPTIONS]

Options:
  --target NAME        Docker build target, default: unified-runner
  --builder NAME       Buildx builder name, default: memorose-arm64
  --no-push            Build locally only, do not push
  --load               Load the ARM64 image into the local Docker engine
  --no-merge           Skip manifest merge
  -h, --help           Show this help message

Environment:
  BUILDER_NAME         Overrides the Buildx builder name
  CARGO_REGISTRY_INDEX Optional Cargo registry mirror, e.g. sparse+https://rsproxy.cn/index/
  CARGO_REGISTRY_NAME  Optional Cargo source name for the mirror, default: mirror

Notes:
  - The amd64 workflow publishes matching tags with the suffix -amd64.
  - This script publishes ARM64 tags with the suffix -arm64.
  - Manifest merge combines TAG-amd64 and TAG-arm64 into TAG.
EOF
}

require_command() {
    local cmd="$1"
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        log_error "Required command not found: ${cmd}"
        exit 1
    fi
}

check_docker_daemon() {
    if docker info >/dev/null 2>&1; then
        return
    fi

    log_error "Docker daemon is not reachable."
    log_error "Start Docker Desktop or fix access to the Docker socket, then retry."
    exit 1
}

trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "${value}"
}

ensure_builder() {
    if docker buildx inspect "${BUILDER}" >/dev/null 2>&1; then
        if docker buildx inspect --bootstrap "${BUILDER}" >/dev/null 2>&1; then
            docker buildx use "${BUILDER}" >/dev/null
            return
        fi

        log_warn "Existing builder ${BUILDER} is unhealthy; recreating it"
        docker buildx rm -f "${BUILDER}" >/dev/null 2>&1 || true
    fi

    docker buildx create \
        --name "${BUILDER}" \
        --driver docker-container \
        --bootstrap \
        --use >/dev/null
}

split_tags() {
    local raw="$1"
    local tag
    TAG_ARRAY=()

    IFS=',' read -r -a TAG_ARRAY <<< "${raw}"
    for tag in "${TAG_ARRAY[@]}"; do
        tag="$(trim "${tag}")"
        if [[ -n "${tag}" ]]; then
            NORMALIZED_TAGS+=("${tag}")
        fi
    done
}

build_arm64() {
    local -a build_args
    build_args=(
        buildx build
        --builder "${BUILDER}"
        --platform linux/arm64
        --target "${TARGET}"
        --file "${DOCKERFILE}"
    )

    for tag in "${NORMALIZED_TAGS[@]}"; do
        build_args+=(-t "${IMAGE}:${tag}-arm64")
    done

    if [[ "${PUSH}" == "true" ]]; then
        build_args+=(--push)
    elif [[ "${LOAD}" == "true" ]]; then
        build_args+=(--load)
    else
        log_error "--no-push requires --load so the image has an output"
        exit 1
    fi

    if [[ -n "${CARGO_REGISTRY_INDEX:-}" ]]; then
        build_args+=(--build-arg "CARGO_REGISTRY_INDEX=${CARGO_REGISTRY_INDEX}")
        build_args+=(--build-arg "CARGO_REGISTRY_NAME=${CARGO_REGISTRY_NAME:-mirror}")
    fi

    build_args+=("${CONTEXT_DIR}")

    log_info "Building ARM64 image for ${IMAGE}"
    docker "${build_args[@]}"
    log_success "ARM64 build complete"
}

merge_manifest() {
    if [[ "${PUSH}" != "true" ]]; then
        log_warn "Skipping manifest merge because images were not pushed"
        return
    fi

    for tag in "${NORMALIZED_TAGS[@]}"; do
        local amd64_ref="${IMAGE}:${tag}-amd64"
        local arm64_ref="${IMAGE}:${tag}-arm64"
        local target_ref="${IMAGE}:${tag}"

        log_info "Merging manifest ${target_ref}"
        docker buildx imagetools create \
            -t "${target_ref}" \
            "${amd64_ref}" \
            "${arm64_ref}" >/dev/null
        docker buildx imagetools inspect "${target_ref}" >/dev/null
        log_success "Manifest updated: ${target_ref}"
    done
}

main() {
    require_command docker
    check_docker_daemon

    if [[ "$(uname -s)" != "Darwin" ]]; then
        log_warn "This script is intended for macOS, but continuing anyway"
    fi

    if [[ "$(uname -m)" != "arm64" ]]; then
        log_warn "Host architecture is not arm64; QEMU may be slower"
    fi

    NORMALIZED_TAGS=()

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --target)
                TARGET="$2"
                shift 2
                ;;
            --builder)
                BUILDER="$2"
                shift 2
                ;;
            --no-push)
                PUSH=false
                shift
                ;;
            --load)
                LOAD=true
                PUSH=false
                MERGE=false
                shift
                ;;
            --no-merge)
                MERGE=false
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done

    split_tags "${TAGS}"

    if [[ "${#NORMALIZED_TAGS[@]}" -eq 0 ]]; then
        log_error "At least one tag is required"
        exit 1
    fi

    log_info "Image: ${IMAGE}"
    log_info "Tags: ${NORMALIZED_TAGS[*]}"
    log_info "Target: ${TARGET}"
    log_info "Builder: ${BUILDER}"

    ensure_builder
    build_arm64

    if [[ "${MERGE}" == "true" ]]; then
        merge_manifest
    fi
}

trap 'log_error "Failed at line $LINENO"' ERR

main "$@"
