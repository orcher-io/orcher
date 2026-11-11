#!/usr/bin/env bash
# fetch-ui.sh <out dir>: the dashboard release ui.lock pins, checked against
# its SHA-256 and unpacked into <out dir>/ui for the Dockerfile to copy. The
# archive stays at <out dir>/orcher-ui-<version>.tar.gz, so CI can check its
# build provenance (gh attestation verify) as well.
#
# ORCHER_UI_ARCHIVE=<path> uses an archive already on disk instead of
# downloading it; it must still match the pinned SHA-256.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

out=${1:?usage: fetch-ui.sh <out dir>}
# shellcheck source=ui.lock
. docker/engine/ui.lock
: "${ORCHER_UI_VERSION:?ui.lock sets no ORCHER_UI_VERSION}" "${ORCHER_UI_SHA256:?ui.lock sets no ORCHER_UI_SHA256}"

name="orcher-ui-$ORCHER_UI_VERSION"
archive="$out/$name.tar.gz"
mkdir -p "$out"
if [ -n "${ORCHER_UI_ARCHIVE:-}" ]; then
  cp "$ORCHER_UI_ARCHIVE" "$archive"
else
  url="https://github.com/orcher-io/ui/releases/download/v$ORCHER_UI_VERSION/$name.tar.gz"
  echo "downloading $url"
  # GitHub's release downloads answer a burst of 5xx now and then; ride out a
  # couple of minutes of them rather than fail a release build over one.
  curl -fsSL --retry 8 --retry-delay 15 --retry-max-time 300 --retry-all-errors \
    -o "$archive" "$url"
fi

if command -v sha256sum >/dev/null; then
  actual=$(sha256sum "$archive" | cut -d' ' -f1)
else
  actual=$(shasum -a 256 "$archive" | cut -d' ' -f1)
fi
if [ "$actual" != "$ORCHER_UI_SHA256" ]; then
  echo "error: $name.tar.gz has SHA-256 $actual; ui.lock pins $ORCHER_UI_SHA256" >&2
  rm -f "$archive"
  exit 1
fi

rm -rf "$out/ui"
mkdir -p "$out/ui"
tar -xzf "$archive" -C "$out/ui" --strip-components=1
for file in index.html LICENSE THIRD_PARTY_NOTICES; do
  [ -s "$out/ui/$file" ] || { echo "error: $name has no $file" >&2; exit 1; }
done
echo "dashboard $ORCHER_UI_VERSION in $out/ui ($(find "$out/ui" -type f | wc -l | tr -d ' ') files)"
