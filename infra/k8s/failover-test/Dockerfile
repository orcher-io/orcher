# syntax=docker/dockerfile:1
# Multi-stage image for the k8s failover test.
# Builds BOTH the orchestrator and the orcher-migrate binary from the workspace,
# with the sqlx offline cache so the build needs no live database.
#
# Built by failover-test.sh (build context = the orcher repo root). Manual:
#   docker build -f infra/k8s/failover-test/Dockerfile -t orcher/orchestrator:local .
FROM rust:1-slim-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

# Whole workspace (target/ etc. excluded via .dockerignore). The .sqlx cache +
# SQLX_OFFLINE let the one compile-time-checked query build without a DB.
COPY . .
ENV SQLX_OFFLINE=true

RUN cargo build --release --bin orchestrator --bin orcher-migrate

# ---- runtime ----
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 orchestrator

COPY --from=builder /app/target/release/orchestrator  /usr/local/bin/orchestrator
COPY --from=builder /app/target/release/orcher-migrate /usr/local/bin/orcher-migrate

USER orchestrator
EXPOSE 50051 8080
ENV LOG_LEVEL=info \
    LOG_FORMAT=json \
    GRPC_BIND_ADDR=0.0.0.0:50051 \
    ORCHER_INSECURE=true \
    HTTP_BIND_ADDR=0.0.0.0:8080
ENTRYPOINT ["orchestrator"]
