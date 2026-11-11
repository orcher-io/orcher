# Examples

The Rust SDK's examples live in the SDK repository, where CI builds them
against the current SDK:
[github.com/orcher-io/sdk-rust/tree/main/examples](https://github.com/orcher-io/sdk-rust/tree/main/examples).

This directory holds the TypeScript and Python versions of the durable
execution example, which `scripts/benchmark.sh` runs as workers:

| Directory | SDK |
| --- | --- |
| [`sdk-ts/durable-execution`](sdk-ts/durable-execution/README.md) | TypeScript, built from a sibling `sdk-ts` checkout |
| [`sdk-py/durable-execution`](sdk-py/durable-execution/README.md) | Python |

## Benchmarking

`scripts/benchmark.sh` compares the SDKs on the same payment workflow: three
tasks (charge, reserve, email) on one task queue. The Rust worker is the
`durable_execution` example from an `sdk-rust` checkout beside this repository,
or wherever `SDK_RUST_DIR` points; the script builds it.

```bash
./scripts/benchmark.sh --sdk rust --workers 1,2,4
./scripts/benchmark.sh --sdk all --quick
```
