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

## Rust-specific design patterns

The library uses several Rust type-system features to move common mistakes from
runtime to compile time.

### Typed method handles

Call-type functions take a **zero-sized handle** as their last argument rather
than a plain `&str` method path:

```rust
// Old (hypothetical):  quorum_call(&ctx, &req, "/storage.Storage/Read")
// Actual API:
let resp = quorum_call(&ctx, &req, StorageReadMethod).await?;
```

`StorageReadMethod` is a zero-sized struct implementing `QuorumCallMethod`:

```rust
#[derive(Clone, Copy)]
pub struct StorageReadMethod;

impl quorums::QuorumCallMethod for StorageReadMethod {
    type Req  = ReadRequest;
    type Resp = ReadResponse;
    const PATH: &'static str = "/storage.Storage/Read";
}
```

Benefits:
- **No turbofish** — `Req` and `Resp` are associated types, inferred from the
  handle.
- **No magic strings** — the path is a compile-time constant on the trait;
  a typo is a type error, not a runtime `NOT_FOUND`.
- **Zero runtime cost** — the handle is dropped immediately; it carries no
  data.

`quorums-build` generates these structs automatically.  Define them manually
only when not using the code generator.

### `#[must_use]` on response handles

`Responses<T>`, `OrderedResponses<T>`, and `Correctable<T>` are all marked
`#[must_use]`.  If you call `quorum_call(…)` and discard the handle without
calling `.majority()` / `.all()` / etc., the compiler emits a warning.  This
prevents a silent footgun where the fan-out fires but the caller never waits
for the results.

### `Correctable<T>: futures::Stream`

`Correctable<T>` implements `futures::Stream<Item = Result<NodeResponse<T>, Error>>`,
so the entire `StreamExt` combinator ecosystem (`take`, `for_each`, `collect`,
`filter_map`, …) works out of the box:

```rust
use futures::StreamExt as _;

correctable_call(&ctx, &req, MyStreamMethod)
    .await?
    .take(5)
    .for_each(|item| async move { println!("{item:?}") })
    .await;
```

### Typestate `ServerCtx<S>`

Server handlers receive `ServerCtx<Locked>`.  Calling `ctx.release()` consumes
it and returns `ServerCtx<Released>`, releasing the per-stream FIFO ordering
lock at that point.  Both states keep `send()` and `metadata()` available:

```rust
server.register_streaming_handler("/svc/Stream",
    |ctx, req: Req| async move {
        let ctx = ctx.release();   // lock released; next message can dispatch
        ctx.send(Resp { … })?;     // still safe to send
        Ok(())
    });
```

Calling `release()` twice is a **compile error** (the method consumes `self`).
Forgetting to release in a streaming handler is safe — the lock is held by an
`OwnedMutexGuard` inside the struct and is released automatically when `ServerCtx`
drops.

See [`src/README.md`](src/README.md) for the complete API reference.

---

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
