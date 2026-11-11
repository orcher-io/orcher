# orcher-common

Types shared by every crate in the engine: identifiers, the error type, payloads, validation and small utilities.

Part of the Orcher engine workspace; it is built into the `orchestrator` binary and is not published on its own.

## What is where

- `types`: identifiers, task and dispatch types, time helpers
- `error`: `OrcherError` and `OrcherResult`, used across the workspace
- `payload`: helpers for the `Payload` messages workflows and tasks exchange
- `validation`: the `Validatable` trait and validation errors
- `utils`: async helpers such as the circuit breaker
- `testing`: fixtures and test helpers (feature `test-utils`)

## Tests

```bash
cargo test -p orcher-common
```
