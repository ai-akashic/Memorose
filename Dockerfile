FROM rust:1.77.2 AS builder

# Install dependencies needed for compiling C/C++ libraries and Protobuf
RUN apt-get update && apt-get install -y protobuf-compiler cmake libclang-dev && rm -rf /var/lib/apt/lists/*

# Install nightly toolchain for -Z flags needed by lance crate
RUN rustup toolchain install nightly && rustup default nightly

WORKDIR /usr/src/app
COPY . .

# Increase recursion limit for lance crate's deeply nested async blocks
ENV RUSTFLAGS="-Zrecursion-limit=256"
ENV RUST_MIN_STACK=8388608

# Build both binaries
RUN cargo build --release -p memorose-server
RUN cargo build --release -p memorose-gateway

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates openssl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries
COPY --from=builder /usr/src/app/target/release/memorose-server /app/
COPY --from=builder /usr/src/app/target/release/memorose-gateway /app/

# Environment variables
ENV RUST_LOG=info

# Expose ports (server: 3000, gateway: 8080)
EXPOSE 3000 8080

# Default command (overridden in compose)
CMD ["/app/memorose-server"]
