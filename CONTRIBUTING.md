# Contributing

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
