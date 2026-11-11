# syntax=docker/dockerfile:1.7
#
# The public developer image of the engine: ghcr.io/orcher-io/orcher.
#
#   docker/engine/build-binaries.sh
#   docker buildx build -f docker/engine/Dockerfile \
#     --platform linux/amd64,linux/arm64 -t orcher-engine:dev .
#
# Separate from services/orchestrator/Dockerfile, which builds the image the
# deployments run and keeps its own interface (orchestrator as the entrypoint,
# JSON logs, migrations as a separate Job). This one is for a developer's
# machine: one `docker run` migrates the database and starts the server.
#
# It packages binaries build-binaries.sh has already cross-compiled for both
# platforms (target/engine-image/<arch>/), with the notices it generated for
# the open-source code in them (target/engine-image/THIRD_PARTY_NOTICES), and
# runs nothing on the target platform: the user and the CA bundle are
# prepared on the build platform and copied in. So a multi-platform build needs no QEMU at all, and takes seconds.

ARG BASE_IMAGE=debian:bookworm-slim

# The build platform's Debian, same release as the runtime: its passwd/group
# with the orcher user added, and the CA bundle (architecture-independent).
FROM --platform=$BUILDPLATFORM ${BASE_IMAGE} AS prep
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 orcher \
    && useradd --system --uid 10001 --gid orcher --no-create-home \
         --home-dir /nonexistent --shell /usr/sbin/nologin orcher

FROM ${BASE_IMAGE}

ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown
ARG CREATED=

COPY --from=prep /etc/passwd /etc/group /etc/
COPY --from=prep /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --chmod=0755 target/engine-image/${TARGETARCH}/orchestrator target/engine-image/${TARGETARCH}/orcher-migrate /usr/local/bin/
COPY --chmod=0755 docker/engine/entrypoint.sh /usr/local/bin/orcher-entrypoint
COPY --chmod=0644 LICENSE /LICENSE
COPY --chmod=0644 docker/engine/NOTICE /NOTICE
COPY --chmod=0644 target/engine-image/THIRD_PARTY_NOTICES /THIRD_PARTY_NOTICES
# The dashboard, served on the HTTP port (ui.dir's default), with its own
# LICENSE and THIRD_PARTY_NOTICES: the release ui.lock pins, fetched and
# checked by build-binaries.sh.
COPY target/engine-image/ui /usr/share/orcher/ui

LABEL org.opencontainers.image.title="ORCHER engine" \
      org.opencontainers.image.description="ORCHER durable workflow engine: the orchestrator and its migration tool." \
      org.opencontainers.image.vendor="ORCHER" \
      org.opencontainers.image.url="https://github.com/orcher-io" \
      org.opencontainers.image.source="https://github.com/orcher-io/orcher" \
      org.opencontainers.image.documentation="https://github.com/orcher-io/quickstart" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${CREATED}"

# Developer defaults. Everything here can be overridden with -e.
#   - single node, no auth: no config file is read unless
#     ORCHER_ORCHESTRATOR_CONFIG names one, and the built-in default is
#     auth_mode = none;
#   - human-readable logs on stdout;
#   - a pool that fits a stock Postgres beside other clients.
ENV GRPC_BIND_ADDR=0.0.0.0:50051 \
    HTTP_BIND_ADDR=0.0.0.0:8080 \
    LOG_LEVEL=info \
    LOG_FORMAT=compact \
    ORCHER_PG_MAX_CONNECTIONS=20 \
    ORCHER_PG_MIN_CONNECTIONS=2 \
    ORCHER_AUTO_MIGRATE=true \
    ORCHER_DB_WAIT_SECS=60

USER 10001:10001
WORKDIR /tmp

EXPOSE 50051 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
    CMD ["orcher-entrypoint", "healthcheck"]

ENTRYPOINT ["orcher-entrypoint"]
CMD ["orchestrator"]
