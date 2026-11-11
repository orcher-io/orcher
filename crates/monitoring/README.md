# orcher-monitoring

Logging, metrics, tracing and health checks for the engine. Metrics are exported for Prometheus, and traces over OTLP or to Jaeger, each behind a feature flag.

Part of the Orcher engine workspace; it is built into the `orchestrator` binary and is not published on its own.

## What is where

- `logging`: log initialization: level, format (compact, json, pretty)
- `metrics`: the engine's metric definitions and labels
- `tracing`: span helpers and trace export setup
- `exporters`: Prometheus, OTLP and Jaeger exporters
- `health`: health checks

## Tests

```bash
cargo test -p orcher-monitoring
```

Tests that bind a network port are marked `#[ignore]`; run them with:

```bash
cargo test -p orcher-monitoring -- --ignored
```
