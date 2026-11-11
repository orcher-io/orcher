# Build stage
FROM rust:1-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the workspace: every member is needed for workspace resolution, and
# `.sqlx` for the offline query cache.
COPY Cargo.toml Cargo.lock ./
COPY .sqlx ./.sqlx
COPY crates ./crates
COPY services ./services

# Offline mode so the compile-time-checked query builds without a live database.
ENV SQLX_OFFLINE=true

# Build the orchestrator and the migration tool. Shipping both lets the same
# image run the schema migration Job (see infra/k8s/deploy/migrate-job.yaml).
# Every dependency comes from crates.io, so the build needs no credentials.
RUN cargo build --release --bin orchestrator --bin orcher-migrate

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 orchestrator

# Create necessary directories
RUN mkdir -p /etc/orchestrator /var/log/orchestrator && \
    chown -R orchestrator:orchestrator /etc/orchestrator /var/log/orchestrator

# Copy binaries from builder
COPY --from=builder /app/target/release/orchestrator /usr/local/bin/orchestrator
COPY --from=builder /app/target/release/orcher-migrate /usr/local/bin/orcher-migrate
RUN chmod +x /usr/local/bin/orchestrator /usr/local/bin/orcher-migrate

# Switch to non-root user
USER orchestrator

# Expose ports
EXPOSE 50051 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health/live || exit 1

# Set environment defaults
ENV LOG_LEVEL=info \
    LOG_FORMAT=json \
    GRPC_BIND_ADDR=0.0.0.0:50051 \
    HTTP_BIND_ADDR=0.0.0.0:8080

ENTRYPOINT ["orchestrator"]
