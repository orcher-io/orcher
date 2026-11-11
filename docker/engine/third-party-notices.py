#!/usr/bin/env python3
"""Renders the engine image's THIRD_PARTY_NOTICES.

    third-party-notices.py ABOUT_JSON LINKED_LIST RUST_COPYRIGHT_HTML OUTPUT

ABOUT_JSON is `cargo about generate --format json` for the orchestrator's
dependency graph, which has already enforced the license allow-list
(about.toml). LINKED_LIST names the crates actually linked into the image's
binaries, one "name vX.Y.Z" per line, from `cargo tree -e
normal,no-proc-macro`; only those are listed, and every one of them must have
a license text, or this fails. Workspace crates are the engine itself and are
not third-party. RUST_COPYRIGHT_HTML is the building toolchain's
COPYRIGHT-library.html: the standard library is linked into every binary.

Standard library only, and Python 3.7-compatible: it runs on whatever
python3 the build machine has.
"""

import json
import os
import re
import sys
from html.parser import HTMLParser


def fail(msg):
    sys.stderr.write("third-party-notices: %s\n" % msg)
    sys.exit(1)


class _Text(HTMLParser):
    """HTML to plain text, keeping <pre> blocks (license texts) verbatim."""

    BLOCK = {"p", "div", "li", "ul", "ol", "h1", "h2", "h3", "h4", "h5",
             "h6", "tr", "table", "section", "details", "summary", "br"}

    def __init__(self):
        HTMLParser.__init__(self, convert_charrefs=True)
        self.out = []
        self.pre = 0
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("style", "script", "head"):
            self.skip += 1
        elif tag == "pre":
            self.pre += 1
            self.out.append("\n")
        elif tag in self.BLOCK:
            self.out.append("\n")

    def handle_endtag(self, tag):
        if tag in ("style", "script", "head"):
            self.skip = max(0, self.skip - 1)
        elif tag == "pre":
            self.pre = max(0, self.pre - 1)
            self.out.append("\n")
        elif tag in self.BLOCK:
            self.out.append("\n")

    def handle_data(self, data):
        if self.skip:
            return
        if self.pre:
            self.out.append(data)
        else:
            data = re.sub(r"\s+", " ", data)
            if not self.out or self.out[-1].endswith("\n"):
                data = data.lstrip()
            if data:
                self.out.append(data)

    def text(self):
        lines = [line.rstrip() for line in "".join(self.out).splitlines()]
        text = "\n".join(lines)
        return re.sub(r"\n{3,}", "\n\n", text).strip() + "\n"


def crate_key(pkg):
    return "%s v%s" % (pkg["name"], pkg["version"])


_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def linked_key(line):
    """"name vX.Y.Z" from a `cargo tree --format {p}` line, whatever decorated
    it: ANSI colour (CARGO_TERM_COLOR=always), the "(*)" dedupe marker, or a
    "(path)" / "(proc-macro)" suffix."""
    line = _ANSI.sub("", line).strip()
    return re.sub(r"\s+\(.*$", "", line)


def main():
    if len(sys.argv) != 5:
        fail("usage: third-party-notices.py ABOUT_JSON LINKED_LIST "
             "RUST_COPYRIGHT_HTML OUTPUT")
    about_path, linked_path, rust_html_path, out_path = sys.argv[1:]

    with open(about_path, encoding="utf-8") as f:
        about = json.load(f)
    with open(linked_path, encoding="utf-8") as f:
        linked = {k for k in (linked_key(line) for line in f) if k}
    with open(rust_html_path, encoding="utf-8") as f:
        parser = _Text()
        parser.feed(f.read())
        rust_text = parser.text()

    crates = {}
    for c in about["crates"]:
        pkg = c["package"]
        key = crate_key(pkg)
        if key not in linked:
            continue  # proc-macro, or not linked for these targets
        if pkg.get("source") is None:
            continue  # a workspace crate: the engine itself
        crates[key] = {"pkg": pkg, "declared": c["license"], "licenses": []}

    known = {crate_key(c["package"]) for c in about["crates"]}
    missing = sorted(k for k in linked if k not in known)
    if missing:
        fail("linked but absent from cargo-about's graph (scan the package "
             "that pulls them in): " + ", ".join(missing))

    texts = []
    for lic in about["licenses"]:
        users = sorted({crate_key(u["crate"]) for u in lic["used_by"]} & set(crates))
        if not users:
            continue
        for u in users:
            if lic["id"] not in crates[u]["licenses"]:
                crates[u]["licenses"].append(lic["id"])
        texts.append((lic["id"], lic["name"], users, lic["text"]))

    bare = sorted(k for k, v in crates.items() if not v["licenses"])
    if bare:
        fail("no license text found for: " + ", ".join(bare))

    def sort_key(k):
        return (crates[k]["pkg"]["name"].lower(), k)

    counts = {}
    for v in crates.values():
        for lid in v["licenses"]:
            counts[lid] = counts.get(lid, 0) + 1

    rule = "=" * 78
    out = []
    w = out.append
    w("THIRD-PARTY SOFTWARE NOTICES")
    w("ORCHER engine image (ghcr.io/orcher-io/orcher)")
    w("")
    w("The orchestrator and orcher-migrate binaries in this image are built")
    w("with the open-source Rust crates listed below and with the Rust standard")
    w("library. Each remains under its own license, reproduced in full in this")
    w("file; nothing in the image's own license (/LICENSE) limits your rights")
    w("under them. Where a crate offers a choice of licenses, \"Distributed")
    w("under\" names the one it is distributed under here. The source of every")
    w("crate is available at the crates.io address given for it.")
    w("")
    w("The operating-system packages of the Debian base image carry their")
    w("licenses in /usr/share/doc/<package>/copyright.")
    w("")
    w("%d crates. Licenses: %s." % (
        len(crates),
        ", ".join("%s (%d)" % (k, counts[k]) for k in sorted(counts, key=lambda k: (-counts[k], k)))))
    w("")
    w(rule)
    w("CRATES")
    w(rule)
    for k in sorted(crates, key=sort_key):
        v = crates[k]
        pkg = v["pkg"]
        w("")
        w(k)
        w("  Source:            https://crates.io/crates/%s/%s" % (pkg["name"], pkg["version"]))
        if pkg.get("repository"):
            w("  Repository:        %s" % pkg["repository"])
        if pkg.get("authors"):
            w("  Authors:           %s" % ", ".join(pkg["authors"]))
        w("  Declared license:  %s" % re.sub(r"\s+", " ", v["declared"]))
        w("  Distributed under: %s" % " AND ".join(v["licenses"]))
    w("")
    w(rule)
    w("LICENSE TEXTS")
    w(rule)
    for lid, name, users, text in texts:
        w("")
        w("-" * 78)
        w("%s (%s)" % (name, lid))
        w("Used by: %s" % ", ".join(sorted(users, key=sort_key)))
        w("-" * 78)
        w("")
        w(text.strip("\n"))
    # Apache-2.0 section 4(d): a NOTICE file a crate ships travels with it.
    notices = []
    for k in sorted(crates, key=sort_key):
        root = os.path.dirname(crates[k]["pkg"]["manifest_path"])
        for name in sorted(os.listdir(root)):
            path = os.path.join(root, name)
            if name.upper().startswith("NOTICE") and os.path.isfile(path):
                with open(path, encoding="utf-8", errors="replace") as f:
                    notices.append((k, name, f.read()))
    if notices:
        w("")
        w(rule)
        w("NOTICE FILES")
        w(rule)
        for k, name, text in notices:
            w("")
            w("-" * 78)
            w("%s: %s" % (k, name))
            w("-" * 78)
            w("")
            w(text.strip("\n"))
    w("")
    w(rule)
    w("RUST STANDARD LIBRARY")
    w(rule)
    w("")
    w(rust_text.rstrip("\n"))
    w("")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(out))
    sys.stderr.write("third-party-notices: %d crates, %d license texts -> %s\n"
                     % (len(crates), len(texts), out_path))


if __name__ == "__main__":
    main()
