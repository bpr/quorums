# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                  # compile
cargo test                   # run all tests (includes integration tests)
cargo test <name>            # run a single test by substring match
cargo test -- --nocapture    # show println! output
cargo clippy                 # lint
cargo fmt                    # format
```

## What this is

`quorums` is a Rust clone of the [gorums](https://github.com/relab/gorums) Go framework for building fault-tolerant distributed systems. The core abstraction is the **quorum call**: a single logical RPC that fans out to all nodes in a configuration, collects responses, and lets the caller aggregate them (first, majority, all, or custom threshold).

## Architecture

### Wire protocol

A single bidirectional gRPC stream per node pair (`NodeStream` in `proto/gorums.proto`) carries all call types, multiplexed by sequence number and method name. The `Message` envelope has: `message_seq_no`, `method`, `status_code/message`, `payload` (prost-encoded application message).

This is the key design difference from vanilla gRPC: instead of one stream per method, every RPC type shares one stream, which enables FIFO ordering guarantees per sender.

### Layers

| Layer | Files | Responsibility |
|-------|-------|----------------|
| Wire | `channel.rs`, `router.rs` | Per-node send queue, background sender/receiver tasks, pending-call registry |
| Nodes | `node.rs`, `manager.rs` | `Node` (Arc-backed remote peer), `Manager` (connection pool) |
| Configuration | `config.rs` | Immutable set of nodes; context for fan-out calls |
| Call types | `call_types.rs` | `rpc_call`, `unicast`, `multicast`, `quorum_call` |
| Responses | `responses.rs` | `Responses<T>`: collects quorum-call results; `.first()/.majority()/.all()/.threshold(n)` |
| Server | `server.rs` | tonic service implementing `NodeStream`; FIFO ordering via `OwnedMutexGuard`; generic `register_handler` |

### Call types

- **`rpc_call(ctx, req, method)`** — single-node, awaits one response.
- **`unicast(ctx, req, method)`** — single-node, one-way; blocks until bytes are sent.
- **`multicast(ctx, req, method)`** — fan-out to all nodes in config, no responses collected.
- **`quorum_call(ctx, req, method) -> Responses<T>`** — fan-out, collect; use terminal method to aggregate.

### FIFO ordering (server side)

`Server::node_stream` holds a `tokio::sync::Mutex` guard across each handler dispatch. The guard is passed into `ServerCtx` and dropped when the handler calls `ctx.release()` or returns. This serializes message dispatch per stream while still allowing handlers to run concurrently once they release.

### No `dyn Trait` in the public API

All call-type functions and `register_handler` are fully generic. Internal handler storage uses `Arc<dyn Fn(...)>` (an implementation detail callers never see).

### Planned additions (all additive, no breaking changes)

- **Correctable** (streaming quorum call) — needs `Streaming: bool` flag in `OutboundRequest` and a `Correctable<T>` type.
- **Interceptors** — additive `CallOption` parameter to call types.
- **Lazy sending** — `Responses<T>` wraps a `sendNow()` trigger; async variants call it immediately.
- **Code generation** — a `prost-build` plugin that emits typed wrappers around the generic call-type functions.
