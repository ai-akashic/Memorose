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
FROM rust:1.88 AS backend-builder

# Install dependencies needed for compiling C/C++ libraries and Protobuf
RUN apt-get update && apt-get install -y protobuf-compiler cmake libclang-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY . .

# Increase recursion limit for lance crate's deeply nested async blocks
ENV RUST_MIN_STACK=8388608

# Build both binaries
RUN cargo build --release -p memorose-server
RUN cargo build --release -p memorose-gateway

# ---- Final Stage ----
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates openssl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries
COPY --from=backend-builder /usr/src/app/target/release/memorose-server /app/
COPY --from=backend-builder /usr/src/app/target/release/memorose-gateway /app/

# Copy frontend static files
COPY --from=frontend-builder /usr/src/app/dashboard/out /app/static/dashboard

# Environment variables
ENV RUST_LOG=info

# Expose ports (server: 3000, gateway: 8080)
EXPOSE 3000 8080

# Default command (overridden in compose)
CMD ["/app/memorose-server"]
