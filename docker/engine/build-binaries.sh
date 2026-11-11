#!/usr/bin/env bash
#
# Compiles the binaries the engine image ships, for both of its platforms:
#
#   target/engine-image/amd64/{orchestrator,orcher-migrate}   x86_64 Linux
#   target/engine-image/arm64/{orchestrator,orcher-migrate}   aarch64 Linux
#   target/engine-image/THIRD_PARTY_NOTICES                  their open-source
#                                                            dependencies' licenses
#
# then `docker buildx build -f docker/engine/Dockerfile --platform
# linux/amd64,linux/arm64 .` packages them.
#
# Both are compiled on this machine's own architecture and cross-linked with
# zig (cargo-zigbuild), against glibc 2.28 so they run on the image's Debian
# (2.36) and anything newer. That keeps a Rust build out of QEMU, and out of a
# Docker VM's smaller share of the machine: on a 4-CPU Docker VM the same
# build, from source inside the Dockerfile, took 45 minutes cold against about
# 20 here, and here the target directory survives between builds.
#
# zig and cargo-zigbuild are used from PATH when present; otherwise the pinned
# versions below are installed into a private virtualenv (ORCHER_ZIG_VENV).

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

ZIG_VERSION=0.16.0
CARGO_ZIGBUILD_VERSION=0.23.4
GLIBC_VERSION=2.28
OUT=target/engine-image
# The platforms to build, of amd64 and arm64: both unless ENGINE_IMAGE_ARCHES
# names fewer. A pull request's image check builds only the runner's own,
# which is all its smoke test runs; a release builds both.
ARCHES="${ENGINE_IMAGE_ARCHES:-amd64 arm64}"

step() { printf '\n==> %s\n' "$*"; }

if ! command -v cargo-zigbuild >/dev/null || ! command -v zig >/dev/null; then
  venv="${ORCHER_ZIG_VENV:-$HOME/.cache/orcher-zig/venv-$ZIG_VERSION-$CARGO_ZIGBUILD_VERSION}"
  if [ ! -x "$venv/bin/cargo-zigbuild" ]; then
    step "installing ziglang $ZIG_VERSION and cargo-zigbuild $CARGO_ZIGBUILD_VERSION into $venv"
    # The first Python new enough for current wheels: a machine's `python3`
    # can be an old framework build whose pip finds no zig past 0.12.
    py=
    for cand in python3.14 python3.13 python3.12 python3.11 python3.10 python3; do
      if command -v "$cand" >/dev/null \
        && "$cand" -c 'import sys; sys.exit(sys.version_info < (3, 10))' 2>/dev/null; then
        py=$cand
        break
      fi
    done
    [ -n "$py" ] || { echo "build-binaries: needs Python 3.10+ to install zig" >&2; exit 1; }
    "$py" -m venv "$venv"
    "$venv/bin/pip" install --quiet --disable-pip-version-check --upgrade pip
    "$venv/bin/pip" install --quiet --disable-pip-version-check \
      "ziglang==$ZIG_VERSION" "cargo-zigbuild==$CARGO_ZIGBUILD_VERSION"
  fi
  export PATH="$venv/bin:$PATH"
fi

step "toolchain"
cargo --version
cargo-zigbuild --version
rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu

export SQLX_OFFLINE=true
target_dir="${CARGO_TARGET_DIR:-target}"
rm -rf "$OUT"

# First, so a dependency under a license the image may not ship fails the
# build in seconds rather than after it.
step "third-party notices"
docker/engine/third-party-notices.sh "$OUT/THIRD_PARTY_NOTICES"

# Also before compiling: the dashboard release ui.lock pins, checked against
# its SHA-256, so a wrong or missing one fails in seconds too.
step "dashboard"
docker/engine/fetch-ui.sh "$OUT"

for pair in amd64:x86_64-unknown-linux-gnu arm64:aarch64-unknown-linux-gnu; do
  arch="${pair%%:*}" triple="${pair#*:}"
  case " $ARCHES " in *" $arch "*) ;; *) continue ;; esac
  step "linux/$arch ($triple, glibc $GLIBC_VERSION)"
  started=$SECONDS
  cargo zigbuild --release --locked \
    --target "$triple.$GLIBC_VERSION" \
    --bin orchestrator --bin orcher-migrate
  mkdir -p "$OUT/$arch"
  cp "$target_dir/$triple/release/orchestrator" "$target_dir/$triple/release/orcher-migrate" "$OUT/$arch/"
  echo "linux/$arch built in $((SECONDS - started))s"
done

step "binaries"
ls -l "$OUT" "$OUT"/*/
