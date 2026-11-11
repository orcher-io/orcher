#!/usr/bin/env bash
#
# Writes target/engine-image/THIRD_PARTY_NOTICES: the open-source crates
# linked into the image's orchestrator and orcher-migrate binaries, for its
# two Linux targets, with their full license texts, then the Rust standard
# library's notices. build-binaries.sh runs it; the Dockerfile ships the
# result as /THIRD_PARTY_NOTICES.
#
# It is generated from Cargo.lock at every image build, never committed, so
# it cannot describe different dependencies from the binaries beside it.
#
# Fails, and so fails the image build, when
#   - a dependency's license is not on the allow-list in about.toml
#     (cargo-about), or
#   - a crate linked into the binaries has no license text.
#
# cargo-about is used from PATH at the pinned version, or installed once into
# ORCHER_CARGO_ABOUT_ROOT.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

CARGO_ABOUT_VERSION=0.9.2
TARGETS=(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu)
# The packages whose binaries the image ships, as build-binaries.sh builds
# them (default features).
PACKAGES=(-p orcher-orchestrator -p orcher-storage)
OUT="${1:-target/engine-image/THIRD_PARTY_NOTICES}"

about=cargo-about
if [ "$(cargo-about --version 2>/dev/null)" != "cargo-about $CARGO_ABOUT_VERSION" ]; then
  root="${ORCHER_CARGO_ABOUT_ROOT:-$HOME/.cache/orcher-cargo-about/$CARGO_ABOUT_VERSION}"
  about="$root/bin/cargo-about"
  if [ ! -x "$about" ]; then
    echo "third-party-notices: installing cargo-about $CARGO_ABOUT_VERSION into $root" >&2
    cargo install --locked --quiet --root "$root" --features cli \
      cargo-about --version "$CARGO_ABOUT_VERSION"
  fi
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# The allow-list is enforced here, over the orchestrator's whole graph
# (orcher-migrate's crates are a subset of it). --fail also fails on a
# license expression cargo-about cannot read.
"$about" about generate --locked --fail \
  -c docker/engine/about.toml \
  -m services/orchestrator/Cargo.toml \
  --format json -o "$work/about.json"

# What is linked: normal dependencies only, without proc-macros (which run
# in the compiler) and build scripts.
#
# One "name vX.Y.Z" per line whatever the environment says: --color never,
# because a CI toolchain action sets CARGO_TERM_COLOR=always and that wraps
# names in ANSI codes; --no-dedupe, so there are no "(*)" markers; sort -u
# dedupes instead. The sed still drops any " (path)" suffix and the parser
# strips ANSI again, so a decorated line cannot pass for a missing crate.
for target in "${TARGETS[@]}"; do
  cargo tree --locked --color never "${PACKAGES[@]}" --target "$target" \
    -e normal,no-proc-macro --prefix none --no-dedupe --format '{p}'
done | sed -e 's/ (.*//' -e '/^$/d' | sort -u >"$work/linked.txt"

rust_copyright="$(rustc --print sysroot)/share/doc/rust/COPYRIGHT-library.html"
[ -s "$rust_copyright" ] || {
  echo "third-party-notices: $rust_copyright is missing" >&2
  exit 1
}

mkdir -p "$(dirname "$OUT")"
python3 docker/engine/third-party-notices.py \
  "$work/about.json" "$work/linked.txt" "$rust_copyright" "$OUT"
