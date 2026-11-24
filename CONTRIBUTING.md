# Contributing

Issues and pull requests are welcome. For anything larger than a fix, open an
issue first so the approach can be agreed before you write it.

## What we ask of a pull request

Every change is reviewed to the same standard, whoever wrote it and whatever
tools they used.

- **You answer for the change.** Use any tools you like, AI assistants
  included, but you must understand what you propose, have run it yourself,
  and reply to review comments yourself. A pull request that nobody is
  following up on is closed without review.
- **Say you are taking an issue.** Comment on it before you start, so that two
  people do not do the same work. An issue marked `good first issue` states
  what a fix has to show.
- **One change per pull request**, with a test that fails without it. If this
  is your first contribution here, keep to one open pull request until it is
  merged.
- **Fill in the template.** It asks what you ran, what you saw, and what you
  did not test. "Nothing" is a fine answer to the last one if it is true.

## Build and test

You need a current stable Rust toolchain (`rustup update stable`) and, for
everything beyond the unit tests, PostgreSQL 14 or later. Nothing else: the
build needs no `protoc`, and no database to compile, because the query
metadata is checked in under `.sqlx/`.

```bash
git clone https://github.com/orcher-io/orcher.git
cd orcher
cargo test --workspace --lib -- --test-threads=1   # the unit tests; no database needed
```

A plain `cargo test` also builds the integration tests under `tests/`, and
many of those expect a database, so it fails without one.

`scripts/ci.sh` is the gate CI runs, and the same script runs on your machine:

| Command | What it runs | Needs | Time on a laptop, from a clean clone |
|---------|--------------|-------|--------------------------------------|
| `scripts/ci.sh check` | `cargo fmt`, a warning-free compile, clippy, the unit tests | Rust | about 15 minutes |
| `scripts/ci.sh guards` | the database-backed tests | PostgreSQL and its `psql` client | about 15 minutes |
| `scripts/ci.sh contract` | real workflows against a real engine, then the tenancy checks | the same, and [sdk-rust](https://github.com/orcher-io/sdk-rust) cloned beside this repository | about 6 minutes |
| `scripts/ci.sh` | all three | | |

Later runs are much quicker, since only what changed is rebuilt. A full build
takes about 14 GB under `target/`.

**The database.** The guards and the contract suite create a fresh database
on your local server and drop it when they finish. By default they connect as
your own user to `localhost:5432`; name another server with
`CI_POSTGRES_ADMIN_URL`, or hand them a database of your own with
`CI_DATABASE_URL`. With Docker and no local server:

```bash
docker run -d --name orcher-test-postgres -p 5432:5432 \
  -e POSTGRES_USER=orcher -e POSTGRES_PASSWORD=orcher postgres:17-alpine
export CI_POSTGRES_ADMIN_URL=postgresql://orcher:orcher@localhost:5432/postgres
```

**The contract suite** starts its own engine on ports 50251 and 8201, so it
does not disturb one you are running on the defaults. It expects the Rust SDK
at `../sdk-rust`, or wherever `CONTRACT_SDK_RUST_DIR` points:

```bash
git clone https://github.com/orcher-io/sdk-rust.git ../sdk-rust
```

**One test.** A database-backed test is marked `#[ignore]`, so it runs only
when asked for, against a database that has the schema:

```bash
createdb orcher_dev
export DATABASE_URL=postgresql://localhost/orcher_dev TEST_DATABASE_URL=postgresql://localhost/orcher_dev
cargo run -p orcher-storage --bin orcher-migrate -- run
cargo test -p orcher-core --test child_start -- --ignored --test-threads=1
```

When you add a database-backed test, add it to the `GUARDS` list in
`scripts/ci.sh`: a test outside that list is never run.

To run the engine you built, see "Build from source" in the
[README](README.md).

## Pre-commit hooks

This repo uses [pre-commit](https://pre-commit.com) for fast local checks
(formatting, hygiene, secret scanning, conventional commit messages). Install
them once per clone:

```bash
pipx install pre-commit      # or: brew install pre-commit / pip install pre-commit
pre-commit install           # installs the pre-commit and commit-msg git hooks
```

After that they run automatically on `git commit`. To run them by hand:

```bash
pre-commit run --all-files   # everything
pre-commit run rustfmt       # a single hook
```

The hooks operate on the files you change, so you don't need a pristine tree to
start. In a genuine emergency you can skip them with `git commit --no-verify` —
but CI runs the same checks and is the real gate.

### What runs

| Hook | What it does |
|------|--------------|
| trailing-whitespace, end-of-file-fixer, mixed-line-ending | whitespace hygiene |
| check-merge-conflict, check-added-large-files, check-yaml, check-toml | guardrails |
| detect-private-key, **gitleaks** | secret scanning |
| **typos** | spell-check (allowlist in `_typos.toml`) |
| **rustfmt** | formats changed Rust files (`cargo fmt` style) |
| **conventional-pre-commit** | enforces `type(scope): subject` commit messages |

Heavier checks — the full offline build, and (soon) `clippy -D warnings` — run
in CI, not in the hooks, to keep commits fast.

## Commit messages

We follow [Conventional Commits](https://www.conventionalcommits.org):
`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`, `ci:`, … The
`conventional-pre-commit` hook enforces this.
