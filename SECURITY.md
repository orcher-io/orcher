# Security policy

## Reporting a vulnerability

Report it privately through GitHub: on this repository's **Security** tab,
choose **Report a vulnerability**. The report is visible only to the
maintainers, and the fix and an advisory are prepared there before anything
is made public.

Please do not open a public issue, discussion or pull request for a
vulnerability.

A useful report says what is affected (the engine, the REST or gRPC API, the
container image), the version or commit, how to reproduce it, and what an
attacker gains.

## What to expect

- An acknowledgement within 3 working days.
- An assessment, and a plan or a reason not to act, within 10 working days.
- Credit in the advisory once a fix is released, unless you prefer not to be
  named.

## Supported versions

Orcher is pre-1.0. Fixes are made on `main` and released in the next version;
earlier releases are not patched. Run the latest release.

## Scope

This repository covers the engine: its gRPC and REST APIs, authentication and
tenancy, storage, and the `ghcr.io/orcher-io/orcher` images built from it. The
SDKs, CLI and protocol definitions have their own repositories; report issues
in those there.

An engine started with `--insecure` (`ORCHER_INSECURE=true`) serves its API
without authentication by design. Exposing one to an untrusted network is a
deployment choice, not a vulnerability.
