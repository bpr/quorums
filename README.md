# quorums

A Rust implementation of the [gorums](https://github.com/relab/gorums) framework for building fault-tolerant distributed systems.

The core abstraction is the **quorum call**: a single logical RPC that fans out to all nodes in a configuration, collects responses, and lets the caller decide how many must agree before the call succeeds (`first`, `majority`, `all`, or a custom predicate).

All call types share a single bidirectional gRPC stream per node pair, which gives FIFO ordering guarantees across all methods.

## Quick start

Add to your `Cargo.toml`:

```toml
[dependencies]
quorums = { path = "…/quorums" }     # or publish and use a version
tonic = "0.13"
prost = "0.13"
tokio = { version = "1", features = ["full"] }

[build-dependencies]
quorums-build = { path = "…/quorums/quorums-build" }
tonic-build = "0.13"
```

In your `build.rs`:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    quorums_build::configure()
        .method("/mypackage.MyService/Read",  quorums_build::CallType::QuorumCall)
        .method("/mypackage.MyService/Write", quorums_build::CallType::Multicast)
        .compile(&["proto/my_service.proto"], &["proto"])?;
    Ok(())
}
```

This generates typed client functions and a server trait that wrap the generic quorums call-type functions.

## Building

```bash
cargo build          # compile library + quorums-build
cargo test           # run all integration tests
cargo clippy         # lint
cargo fmt            # format
```

## Running the example

The `key_value` example starts three in-process storage nodes and demonstrates
multicast writes, quorum reads, single-node RPCs, metadata, and cancellation.

```bash
cargo run --example key_value
```

Expected output:

```
Starting 3 storage nodes on ports [19001, 19002, 19003]…
Connected to 3 nodes.

=== Multicast write: set 'colour' = 'blue' on all nodes ===
→ /storage.Storage/Write (nodes [1, 2, 3])
  [node] write "colour" = "blue"
  …

=== Quorum read: read 'colour' — accept on majority ===
→ /storage.Storage/Read (nodes [1, 2, 3])
  ok=true, value="blue"

…
Done.
```

## Architecture overview

See [`src/README.md`](src/README.md) for a detailed walkthrough of all public
types and how they compose.

At a high level:

```
Manager ──creates──▶ Configuration ──context()──▶ ConfigContext
                                                        │
                                         quorum_call / multicast /
                                         correctable_call / ordered_quorum_call
                                                        │
                                                    Responses<T>
                                                  Correctable<T>
                                               OrderedResponses<T>
                                                        │
                                              .majority() / .all() /
                                              .threshold(k) / .quorum(f)
                                                        │
                                                     Result<T, Error>


Node ──context()──▶ NodeContext ──▶ rpc_call / unicast


Server
  .register_handler(…)
  .register_streaming_handler(…)
  .with_interceptor(…)
  .serve(addr)
```

## Code generation

`quorums-build` reads `.proto` files and emits:

- A `{service}_client` module with one typed `async fn` per annotated method.
- A `{service}_server` module with a `{Service}Server` trait and a
  `register_{service}(server, Arc<impl Trait>)` helper.

Call types available in `quorums_build::CallType`:

| Variant | Client signature | Direction |
|---------|-----------------|-----------|
| `RpcCall` | `fn(ctx: &NodeContext, req) -> Result<Resp, Error>` | single-node two-way |
| `Unicast` | `fn(ctx: &NodeContext, req) -> Result<(), Error>` | single-node one-way |
| `Multicast` | `fn(ctx: &ConfigContext, req) -> Result<(), Error>` | fan-out one-way |
| `QuorumCall` | `fn(ctx: &ConfigContext, req) -> Result<Responses<Resp>, Error>` | fan-out two-way |
| `OrderedQuorumCall` | `fn(ctx: &ConfigContext, req) -> Result<OrderedResponses<Resp>, Error>` | fan-out, position-tagged |
| `Correctable` | `fn(ctx: &ConfigContext, req) -> Result<Correctable<Resp>, Error>` | fan-out streaming |
