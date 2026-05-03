# syntax=docker/dockerfile:1.7

# ---- Frontend Builder ----
FROM node:20-alpine AS frontend-builder
WORKDIR /usr/src/app/dashboard

RUN npm install -g pnpm

# Copy package files
COPY dashboard/package.json dashboard/pnpm-lock.yaml ./
# pnpm-workspace.yaml might be needed if it exists
COPY dashboard/pnpm-workspace.yaml* ./

RUN pnpm install --frozen-lockfile

# Copy the rest of the dashboard source
COPY dashboard/ ./
RUN pnpm run build

# ---- Backend Builder ----
FROM rust:1.91.0-bookworm AS backend-builder
ARG APT_DEBIAN_MIRROR=""
ARG APT_SECURITY_MIRROR=""
ARG CARGO_BUILD_JOBS=1

# Install dependencies needed for compiling C/C++ libraries and Protobuf
RUN if [ -n "${APT_DEBIAN_MIRROR}" ]; then \
      sed -i "s|http://deb.debian.org/debian|${APT_DEBIAN_MIRROR}|g" /etc/apt/sources.list /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    fi && \
    if [ -n "${APT_SECURITY_MIRROR}" ]; then \
      sed -i "s|http://deb.debian.org/debian-security|${APT_SECURITY_MIRROR}|g" /etc/apt/sources.list /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    fi && \
    for attempt in 1 2 3 4 5; do \
      apt-get update -o Acquire::Retries=5 -o Acquire::http::Timeout=60 -o Acquire::https::Timeout=60 && \
      apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev cmake libclang-dev && \
      rm -rf /var/lib/apt/lists/* && \
      break; \
      if [ "${attempt}" = "5" ]; then exit 1; fi; \
      sleep $((attempt * 5)); \
    done

WORKDIR /usr/src/app
COPY . .

# Increase recursion limit for lance crate's deeply nested async blocks
ENV RUST_MIN_STACK=8388608
ENV RUSTUP_TOOLCHAIN=1.91.0
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
ENV CARGO_NET_RETRY=10
ENV CARGO_HTTP_TIMEOUT=600
ENV CARGO_HTTP_MULTIPLEXING=false
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

# Verify the toolchain early so LanceDB MSRV failures are obvious in Docker logs.
RUN rustup show active-toolchain && rustc --version && cargo --version

# Fetch dependencies separately so transient registry failures can reuse BuildKit cache on retry.
RUN --mount=type=cache,id=memorose-cargo-registry-arm64,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=memorose-cargo-git-arm64,target=/usr/local/cargo/git,sharing=locked \
    cargo fetch --locked

# Build both binaries
RUN --mount=type=cache,id=memorose-cargo-registry-arm64,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=memorose-cargo-git-arm64,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release --locked --jobs "${CARGO_BUILD_JOBS}" -p memorose-server && \
    cargo build --release --locked --jobs "${CARGO_BUILD_JOBS}" -p memorose-gateway

# ---- Backend Runner ----
FROM debian:bookworm-slim AS backend-runner
ARG APT_DEBIAN_MIRROR=""
ARG APT_SECURITY_MIRROR=""

RUN if [ -n "${APT_DEBIAN_MIRROR}" ]; then \
      sed -i "s|http://deb.debian.org/debian|${APT_DEBIAN_MIRROR}|g" /etc/apt/sources.list /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    fi && \
    if [ -n "${APT_SECURITY_MIRROR}" ]; then \
      sed -i "s|http://deb.debian.org/debian-security|${APT_SECURITY_MIRROR}|g" /etc/apt/sources.list /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    fi && \
    for attempt in 1 2 3 4 5; do \
      apt-get update -o Acquire::Retries=5 -o Acquire::http::Timeout=60 -o Acquire::https::Timeout=60 && \
      apt-get install -y --no-install-recommends ca-certificates openssl && \
      rm -rf /var/lib/apt/lists/* && \
      break; \
      if [ "${attempt}" = "5" ]; then exit 1; fi; \
      sleep $((attempt * 5)); \
    done

WORKDIR /app

# Copy binaries
COPY --from=backend-builder /usr/src/app/target/release/memorose-server /app/
COPY --from=backend-builder /usr/src/app/target/release/memorose-gateway /app/

# Environment variables
ENV RUST_LOG=info

# Expose ports (server: 3000, gateway: 8080)
EXPOSE 3000 8080

# Default command (overridden in compose)
CMD ["/app/memorose-server"]

# ---- Dashboard Runner ----
FROM node:20-alpine AS dashboard-runner

WORKDIR /app/dashboard

ENV NODE_ENV=production
ENV PORT=3100
ENV HOSTNAME=0.0.0.0

COPY --from=frontend-builder /usr/src/app/dashboard/.next/standalone ./
COPY --from=frontend-builder /usr/src/app/dashboard/.next/static ./.next/static
COPY --from=frontend-builder /usr/src/app/dashboard/public ./public
COPY dashboard/server ./server

EXPOSE 3100

CMD ["node", "server/standalone-server.js"]

# ---- Unified Runner (Default for `docker run`) ----
FROM node:20-bookworm-slim AS unified-runner
ARG APT_DEBIAN_MIRROR=""
ARG APT_SECURITY_MIRROR=""

# Install SSL certs (needed for Rust HTTP requests)
RUN if [ -n "${APT_DEBIAN_MIRROR}" ]; then \
      sed -i "s|http://deb.debian.org/debian|${APT_DEBIAN_MIRROR}|g" /etc/apt/sources.list /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    fi && \
    if [ -n "${APT_SECURITY_MIRROR}" ]; then \
      sed -i "s|http://deb.debian.org/debian-security|${APT_SECURITY_MIRROR}|g" /etc/apt/sources.list /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    fi && \
    for attempt in 1 2 3 4 5; do \
      apt-get update -o Acquire::Retries=5 -o Acquire::http::Timeout=60 -o Acquire::https::Timeout=60 && \
      apt-get install -y --no-install-recommends ca-certificates openssl && \
      rm -rf /var/lib/apt/lists/* && \
      break; \
      if [ "${attempt}" = "5" ]; then exit 1; fi; \
      sleep $((attempt * 5)); \
    done

WORKDIR /app

# Copy Rust backend binaries
COPY --from=backend-builder /usr/src/app/target/release/memorose-server /app/
COPY --from=backend-builder /usr/src/app/target/release/memorose-gateway /app/

# Copy Next.js frontend files
COPY --from=frontend-builder /usr/src/app/dashboard/.next/standalone /app/dashboard/
COPY --from=frontend-builder /usr/src/app/dashboard/.next/static /app/dashboard/.next/static
COPY --from=frontend-builder /usr/src/app/dashboard/public /app/dashboard/public
COPY dashboard/server /app/dashboard/server

# Setup environment variables
ENV RUST_LOG=info
ENV NODE_ENV=production
ENV PORT=3100
ENV HOSTNAME=0.0.0.0
# Dashboard uses this to proxy API requests internally
ENV DASHBOARD_API_ORIGIN=http://127.0.0.1:3000

# Expose backend (3000) and frontend (3100) ports
EXPOSE 3000 3100

# Create a startup script that runs both processes
RUN echo '#!/bin/bash\n\
echo "Starting Memorose API Server on port 3000..."\n\
/app/memorose-server &\n\
SERVER_PID=$!\n\
\n\
echo "Starting Memorose Dashboard on port 3100..."\n\
cd /app/dashboard && node server/standalone-server.js &\n\
DASHBOARD_PID=$!\n\
\n\
# Wait for any process to exit\n\
wait -n\n\
exit $?\n\
' > /app/start.sh && chmod +x /app/start.sh

CMD ["/app/start.sh"]
