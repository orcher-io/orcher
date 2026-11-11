#!/usr/bin/env bash
#
# Entrypoint of the public engine image.
#
#   (no arguments) | orchestrator [flags]
#       apply migrations to $DATABASE_URL (the database must exist; the
#       postgres image's POSTGRES_DB creates one), then exec the
#       orchestrator. ORCHER_AUTO_MIGRATE=false skips the migration step, for
#       a schema managed some other way.
#   -<flag> ...
#       the same, with the flags passed to the orchestrator.
#   healthcheck
#       exit 0 when the engine answers ready on $HTTP_BIND_ADDR; used by the
#       image's HEALTHCHECK, needs no curl.
#   anything else (orcher-migrate status, bash, ...)
#       run as given.
#
# The orchestrator never migrates on its own: it refuses to start against a
# schema it does not match. Doing it here keeps that property for deployments
# while making a developer's first `docker run` one command.

set -euo pipefail

log() { printf 'orcher-entrypoint: %s\n' "$*" >&2; }

healthcheck() {
  local port="${HTTP_BIND_ADDR##*:}"
  exec 3<>"/dev/tcp/127.0.0.1/${port}" || return 1
  printf 'GET /health/ready HTTP/1.0\r\nHost: localhost\r\n\r\n' >&3
  local status
  IFS=' ' read -r _ status _ <&3 || return 1
  [ "$status" = "200" ]
}

migrate() {
  local deadline=$((SECONDS + ${ORCHER_DB_WAIT_SECS:-60}))
  local out
  # Postgres started beside the engine (docker compose) may not accept
  # connections yet, so a failed attempt is retried until the deadline.
  until out=$(orcher-migrate run 2>&1); do
    if [ "$SECONDS" -ge "$deadline" ]; then
      log "migrations failed after ${ORCHER_DB_WAIT_SECS:-60}s:"
      printf '%s\n' "$out" >&2
      exit 1
    fi
    # The last error line, so a failure that waiting will not cure (a wrong
    # password, a missing database) is visible straight away.
    log "migrations not applied yet, retrying in 2s: $(printf '%s\n' "$out" | grep -E 'ERROR|error' | tail -1 | sed 's/\x1b\[[0-9;]*m//g')"
    sleep 2
  done
  printf '%s\n' "$out" >&2
  log "migrations applied"
}

case "${1:-orchestrator}" in
  healthcheck)
    healthcheck
    exit $?
    ;;
  -*)
    set -- orchestrator "$@"
    ;;
  orchestrator) ;;
  *)
    exec "$@"
    ;;
esac
[ "$#" -gt 0 ] || set -- orchestrator

# `--version` and `--help` need no database.
for arg in "$@"; do
  case "$arg" in
    -V | --version | -h | --help) exec "$@" ;;
  esac
done

if [ -z "${DATABASE_URL:-}" ]; then
  log "DATABASE_URL is not set. Point it at a PostgreSQL 14+ database, e.g."
  log "  -e DATABASE_URL=postgresql://orcher:orcher@postgres:5432/orcher"
  exit 64
fi

if [ "${ORCHER_AUTO_MIGRATE:-true}" = "true" ]; then
  migrate
fi

exec "$@"
