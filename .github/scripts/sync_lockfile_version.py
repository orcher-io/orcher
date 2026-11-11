#!/usr/bin/env python3
"""Put the workspace version onto every workspace member in Cargo.lock.

CI builds with --locked, so a release that bumps Cargo.toml and leaves the
lockfile behind breaks the next build with "the lock file needs to be updated".
Cargo would fix it in a second, but the release pull request is written by
release-please, which edits files and cannot run cargo — and its file updaters
cannot address a lockfile entry by crate name, so it cannot do this itself.

Hence this: read the version the release just wrote into [workspace.package],
then set it on each member's [[package]] entry. Members only — a dependency
that happens to share a name with nothing here is left alone.

Prints what it changed and exits 0 whether or not anything moved, so a release
with no version change is not a failure.
"""

from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]


def workspace_version(cargo_toml: str) -> str:
    """The version under [workspace.package], which every member inherits."""
    section = re.search(r"^\[workspace\.package\]$(.*?)(?=^\[)", cargo_toml, re.M | re.S)
    if not section:
        sys.exit("Cargo.toml has no [workspace.package] section")
    version = re.search(r'^version\s*=\s*"([^"]+)"', section.group(1), re.M)
    if not version:
        sys.exit("[workspace.package] has no version")
    return version.group(1)


def member_names() -> set[str]:
    """Package names of the workspace members, read from their manifests."""
    names = set()
    for manifest in sorted(ROOT.glob("crates/*/Cargo.toml")) + sorted(ROOT.glob("services/*/Cargo.toml")):
        name = re.search(r'^name\s*=\s*"([^"]+)"', manifest.read_text(), re.M)
        if name:
            names.add(name.group(1))
    return names


def main() -> int:
    cargo_toml = (ROOT / "Cargo.toml").read_text()
    lock_path = ROOT / "Cargo.lock"
    lock = lock_path.read_text()

    version = workspace_version(cargo_toml)
    members = member_names()
    changed = []

    def bump(entry: re.Match[str]) -> str:
        block = entry.group(0)
        name = re.search(r'^name\s*=\s*"([^"]+)"', block, re.M)
        if not name or name.group(1) not in members:
            return block
        current = re.search(r'^version\s*=\s*"([^"]+)"', block, re.M)
        if not current or current.group(1) == version:
            return block
        changed.append(f"{name.group(1)} {current.group(1)} -> {version}")
        return re.sub(r'^version\s*=\s*"[^"]+"', f'version = "{version}"', block, count=1, flags=re.M)

    updated = re.sub(r"\[\[package\]\]\n(?:[^\[]|\[(?!\[))*", bump, lock)

    if not changed:
        print(f"Cargo.lock already on {version}")
        return 0

    lock_path.write_text(updated)
    print(f"Cargo.lock -> {version}")
    for line in changed:
        print(f"  {line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
