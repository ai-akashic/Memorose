FROM rust:1.83 as builder

WORKDIR /usr/src/app
COPY . .

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
