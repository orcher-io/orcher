# orcher-config

Configuration types the engine's crates share, with their defaults and validation. The engine's own settings file is described in `services/orchestrator/config.example.toml`.

Part of the Orcher engine workspace; it is built into the `orchestrator` binary and is not published on its own.

## What is where

- `config`: the configuration types
- `error`: configuration errors

## Tests

```bash
cargo test -p orcher-config
```
