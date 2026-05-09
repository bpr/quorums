# quorums — source overview

This document describes how the public types and functions fit together and
provides a concise API reference.  For build instructions and examples see the
[top-level README](../README.md).

---

## Wire protocol

Every call type — RPC, unicast, multicast, quorum call, correctable — travels
over a **single bidirectional gRPC stream per node pair** (`NodeStream` in
`proto/gorums.proto`).  Each message is wrapped in a `Message` envelope:

| Field | Purpose |
|-------|---------|
| `message_seq_no` | Matches responses to requests; stream-level routing |
| `method` | Full gRPC method path, e.g. `/storage.Storage/Read` |
| `status_code` / `status_message` | gRPC status for error responses (0 = OK) |
| `payload` | prost-encoded application message |
| `metadata` | Per-call key/value map forwarded from client to server |

Multiplexing all methods over one stream per node guarantees **FIFO ordering**
from any given client: messages from a single sender arrive at the server in
the order they were sent.

---

## Layer diagram

```
┌─────────────────────────────────────────────────────────┐
│  Application / generated code (quorums-build)           │
│  storage_client::read(ctx, req)  →  Responses<ReadResp> │
└───────────────────────┬─────────────────────────────────┘
                        │ calls
┌───────────────────────▼─────────────────────────────────┐
│  call_types.rs                                          │
│  rpc_call / unicast / multicast / quorum_call /         │
│  ordered_quorum_call / correctable_call                 │
└──────────┬──────────────────────────────────────────────┘
           │ enqueues OutboundRequest
┌──────────▼──────────┐    ┌───────────────────────────┐
│  channel.rs         │    │  router.rs                │
│  NodeChannel        │    │  Router                   │
│  per-node send queue│◄──►│  pending-call registry    │
│  background tasks   │    │  (oneshot / stream senders)│
└─────────────────────┘    └───────────────────────────┘
           │
┌──────────▼──────────┐
│  node.rs            │    ┌──────────────────────┐
│  Node (Arc-backed)  │    │  manager.rs          │
│  NodeContext        │◄───│  Manager             │
│  NodeStatus watch   │    │  connection pool     │
└─────────────────────┘    └──────────────────────┘
           │
┌──────────▼──────────┐
│  config.rs          │
│  Configuration      │
│  ConfigContext      │
└─────────────────────┘
```

---

## Nodes and Manager

### `Node`

A cheaply cloneable (`Arc`-backed) handle to a remote peer.

```rust
pub struct Node { … }

impl Node {
    pub fn id(&self) -> u32;
    pub fn address(&self) -> &str;
    pub fn status(&self) -> NodeStatus;
    pub fn subscribe_status(&self) -> watch::Receiver<NodeStatus>;
    pub fn context(&self) -> NodeContext;
}
```

`NodeStatus` transitions: `Connecting → Connected → Reconnecting → Unreachable → Closed`.

### `Manager`

The connection pool.  Owns all `Node`s; configurations borrow them by
`Arc`-clone so a configuration keeps working after a node is removed from the
pool.

```rust
pub struct Manager { … }

impl Manager {
    pub fn new() -> Self;
    pub fn add_node(&mut self, id: u32, addr: &str) -> Result<(), String>;
    pub fn add_node_list(&mut self, addrs: &[&str]) -> Result<Configuration, String>;
    pub fn configuration(&self, ids: &[u32]) -> Result<Configuration, String>;
    pub fn all_nodes(&self) -> Configuration;
    pub fn remove_node(&mut self, id: u32) -> bool;
    pub fn with_new_nodes(&mut self, base: &Configuration, addrs: &[&str])
        -> Result<Configuration, String>;
    pub fn on_status_change<F>(&mut self, callback: F)
    where F: Fn(u32, NodeStatus) + Send + Sync + 'static;
    pub fn node(&self, id: u32) -> Option<&Node>;
}
```

---

## Configuration and ConfigContext

### `Configuration`

An immutable set of nodes for fan-out calls.  All view operations create new
`Configuration` values without opening new connections.

```rust
pub struct Configuration { … }

impl Configuration {
    pub fn nodes(&self) -> &[Node];
    pub fn size(&self) -> usize;
    pub fn node_ids(&self) -> Vec<u32>;

    // View constructors — all reuse existing Arc<NodeInner>s
    pub fn without_nodes(&self, ids: &[u32]) -> Configuration;
    pub fn sub_config(&self, ids: &[u32]) -> Configuration;
    pub fn with_additional_nodes(&self, extra: impl IntoIterator<Item = Node>) -> Configuration;
    pub fn merge(&self, other: &Configuration) -> Configuration;
    pub fn intersect(&self, other: &Configuration) -> Configuration;
    pub fn except(&self, other: &Configuration) -> Configuration;

    pub fn context(&self) -> ConfigContext;
}
```

### `ConfigContext`

Builder that attaches per-call options to a `Configuration`.  Pass to all
fan-out call functions.

```rust
pub struct ConfigContext { … }

impl ConfigContext {
    pub fn with_cancel(self, token: CancellationToken) -> Self;
    pub fn with_timeout(self, duration: Duration) -> Self;
    pub fn with_interceptor(self, i: Interceptor) -> Self;
    pub fn with_metadata(self, key: impl Into<String>, value: impl Into<String>) -> Self;
}
```

### `NodeContext`

Same builder pattern for single-node calls.

```rust
pub struct NodeContext { … }  // created via Node::context()

impl NodeContext {
    pub fn with_cancel(self, token: CancellationToken) -> Self;
    pub fn with_timeout(self, duration: Duration) -> Self;
    pub fn with_interceptor(self, i: Interceptor) -> Self;
    pub fn with_metadata(self, key: impl Into<String>, value: impl Into<String>) -> Self;
}
```

---

## Call types (`call_types.rs`)

All functions are fully generic — no `dyn Trait` in the public API.  The
third argument is a **typed method handle** (see
[Typed method handles](#typed-method-handles) below) that carries the method
path and request/response types as compile-time constants.

### `rpc_call` — single-node two-way

```rust
pub async fn rpc_call<M: RpcCallMethod>(
    ctx: &NodeContext,
    req: &M::Req,
    _m: M,
) -> Result<M::Resp, Error>
```

Sends a request to one node and awaits its response.

### `unicast` — single-node one-way

```rust
pub async fn unicast<M: UnicastMethod>(
    ctx: &NodeContext,
    req: &M::Req,
    _m: M,
) -> Result<(), Error>
```

Sends a request to one node.  Returns once the bytes have been handed to the
send queue (not when the server processes them).

### `multicast` — fan-out one-way

```rust
pub async fn multicast<M: MulticastMethod>(
    ctx: &ConfigContext,
    req: &M::Req,
    _m: M,
) -> Result<(), Error>
```

Sends the same request to every node in the configuration.  No responses are
collected.

### `quorum_call` — fan-out two-way

```rust
pub async fn quorum_call<M: QuorumCallMethod>(
    ctx: &ConfigContext,
    req: &M::Req,
    _m: M,
) -> Result<Responses<M::Resp>, Error>
```

Fans out to all nodes and returns a [`Responses<M::Resp>`](#responsest) handle.
The caller drives aggregation with a terminal method.

### `ordered_quorum_call` — fan-out two-way with position tags

```rust
pub async fn ordered_quorum_call<M: OrderedQuorumCallMethod>(
    ctx: &ConfigContext,
    req: &M::Req,
    _m: M,
) -> Result<OrderedResponses<M::Resp>, Error>
```

Like `quorum_call`, but each response is tagged with the node's **position**
(0-based index) in the configuration.  Terminal methods return the value from
the lowest-position node; `.quorum(f)` receives a `&[Option<Resp>]` slice.

### `correctable_call` — fan-out streaming

```rust
pub async fn correctable_call<M: CorrectableMethod>(
    ctx: &ConfigContext,
    req: &M::Req,
    _m: M,
) -> Result<Correctable<M::Resp>, Error>
```

Each node can send **multiple** responses before signalling completion.
Returns a [`Correctable<M::Resp>`](#correctablet) handle.

---

## Response handles

All three response handle types are `#[must_use]`.  The compiler emits a
warning if the handle is dropped without calling a terminal method — which
would silently discard a fan-out that is already in flight.

### `Responses<T>`

```rust
impl<T> Responses<T> {
    pub fn size(&self) -> usize;
    pub async fn first(self)           -> Result<T, Error>;  // any 1 node
    pub async fn majority(self)        -> Result<T, Error>;  // ⌈(n+1)/2⌉ nodes
    pub async fn all(self)             -> Result<T, Error>;  // all n nodes
    pub async fn threshold(self, k)    -> Result<T, Error>;  // at least k nodes
    pub async fn quorum<F>(self, f: F) -> Result<T, Error>;  // custom predicate
}
```

`threshold`, `majority`, and `all` return the **first** successful response
once the threshold is met.  `quorum(f)` calls `f(&[T])` after each success;
resolves when `f` returns `Some`.

### `OrderedResponses<T>`

Identical terminal methods to `Responses<T>`, but threshold-based methods
return the value from the **lowest-position** node.  `quorum(f)` receives
`f(&[Option<T>])` — one slot per configuration node.

### `Correctable<T>`

```rust
impl<T> Correctable<T> {
    pub fn size(&self) -> usize;

    // Manual iteration — receive one response at a time.
    pub async fn next(&mut self) -> Result<Option<NodeResponse<T>>, Error>;

    // Terminal methods — wait for a quorum of distinct nodes.
    pub async fn first(self)           -> Result<T, Error>;
    pub async fn majority(self)        -> Result<T, Error>;
    pub async fn all(self)             -> Result<T, Error>;
    pub async fn threshold(self, k)    -> Result<T, Error>;
    pub async fn quorum<F>(self, f: F) -> Result<T, Error>;
}
```

`next()` returns:
- `Ok(Some(nr))` — a response from one node (possibly the second or later from
  the same node).
- `Ok(None)` — all node streams have closed.
- `Err(Error::Cancelled)` — the context's token fired.

`threshold` counts **distinct nodes** that have sent at least one successful
response, then returns the most recently received successful value.

`Correctable<T>` also implements `futures::Stream<Item = Result<NodeResponse<T>, Error>>`,
giving access to the full `StreamExt` combinator ecosystem
(`take_while`, `for_each`, `collect`, etc.) without needing the manual `next()` loop:

```rust
use futures::StreamExt as _;

let mut stream = correctable_call(&ctx, &req, MyMethod).await?;
while let Some(item) = stream.next().await {
    println!("{:?}", item?);
}
```

---

## Typed method handles

The call-type functions take `_m: M` where `M` is a **typed method handle** —
a zero-sized struct implementing one of six marker traits in `quorums::method`:

| Trait | Used by |
|-------|---------|
| `RpcCallMethod` | `rpc_call` |
| `UnicastMethod` | `unicast` |
| `MulticastMethod` | `multicast` |
| `QuorumCallMethod` | `quorum_call` |
| `OrderedQuorumCallMethod` | `ordered_quorum_call` |
| `CorrectableMethod` | `correctable_call` |

Each trait has three associated items:

```rust
pub trait QuorumCallMethod {
    type Req:  prost::Message;
    type Resp: prost::Message + Default + Send + 'static;
    const PATH: &'static str;   // full gRPC method path, e.g. "/storage.Storage/Read"
}
```

Because `Req` and `Resp` are associated types on the handle, the compiler
infers them from the handle you pass — no turbofish, no magic strings:

```rust
// Define a handle (once, usually in generated code):
#[derive(Clone, Copy)]
pub struct StorageReadMethod;

impl quorums::QuorumCallMethod for StorageReadMethod {
    type Req  = ReadRequest;
    type Resp = ReadResponse;
    const PATH: &'static str = "/storage.Storage/Read";
}

// Use it — types are inferred, path is a compile-time constant:
let responses = quorum_call(&ctx, &req, StorageReadMethod).await?;
//                                        ^^^^^^^^^^^^^^^^
//                                        zero bytes at runtime
```

Pass the handle by value (it has no data — it is zero bytes at runtime and
is immediately dropped).  The `_m` parameter name signals this convention.

**Handle reuse across call types.**  `quorums-build` implements multiple traits
on each handle to allow the same method to be called in more than one mode:

- A `Multicast`-annotated method also implements `UnicastMethod` (same wire
  path, one node instead of all).
- A `QuorumCall`-annotated method also implements `OrderedQuorumCallMethod` and
  `RpcCallMethod` — you can call `rpc_call(..., StorageReadMethod)` on a
  `QuorumCall` handle to read from a single node directly.

**What `quorums-build` generates.**  For each annotated method, the generator
emits a handle struct at file level (same scope as prost message types) plus a
convenience function in the `{service}_client` module:

```rust
// file level — same scope as ReadRequest / ReadResponse
#[derive(Clone, Copy)]
pub struct StorageReadMethod;
impl ::quorums::QuorumCallMethod      for StorageReadMethod { … }
impl ::quorums::OrderedQuorumCallMethod for StorageReadMethod { … }
impl ::quorums::RpcCallMethod          for StorageReadMethod { … }

pub mod storage_client {
    pub async fn read(
        ctx: &::quorums::config::ConfigContext,
        req: &super::ReadRequest,
    ) -> Result<::quorums::Responses<super::ReadResponse>, ::quorums::Error> {
        ::quorums::call_types::quorum_call(ctx, req, super::StorageReadMethod).await
    }
}
```

---

## Server

```rust
pub struct Server { … }

impl Server {
    pub fn new() -> Self;

    // Register a two-way handler (rpc_call / unicast / multicast / quorum_call).
    pub fn register_handler<Req, Resp, F, Fut>(
        &mut self,
        method: impl Into<String>,
        handler: F,
    )
    where
        F: Fn(ServerCtx<Locked>, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Resp>, Status>> + Send + 'static;

    // Register a streaming handler (correctable_call).
    pub fn register_streaming_handler<Req, Resp, F, Fut>(
        &mut self,
        method: impl Into<String>,
        handler: F,
    )
    where
        F: Fn(ServerCtx<Locked>, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Status>> + Send + 'static;

    pub fn with_interceptor(self, interceptor: ServerInterceptor) -> Self;
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error>;
}
```

A built-in health handler at `/_gorums/health` is always registered.

### `ServerCtx<S>` — typestate context

Handlers receive `ServerCtx<Locked>` (written as `ServerCtx` — `Locked` is the
default type parameter).  Calling `release()` transitions to `ServerCtx<Released>`.
Both states support `send()` and `metadata()`.

```
ServerCtx<Locked>  ──.release()──▶  ServerCtx<Released>
       │                                     │
       └── dropped without release ──────────┘
                      │
              OwnedMutexGuard drops,
              ordering lock released
```

```rust
// Marker types — zero-sized, erased at compile time.
pub struct Locked;
pub struct Released;

pub struct ServerCtx<S = Locked> { … }

// Available on both states:
impl<S> ServerCtx<S> {
    pub fn metadata(&self) -> &[(String, String)];
    pub fn metadata_get(&self, key: &str) -> Option<&str>;
    pub fn send<Resp: ProstMessage>(&self, resp: Resp) -> Result<(), Status>;
}

// Only available on the Locked state:
impl ServerCtx<Locked> {
    /// Consumes the locked context, drops the ordering guard, and returns
    /// a Released context with the same send channel and metadata.
    /// Calling release() twice is a compile error (self is consumed).
    pub fn release(self) -> ServerCtx<Released>;
}
```

The ordering lock is held by `OwnedMutexGuard` inside the struct.  No explicit
`Drop` impl is required — the guard's own destructor releases the lock when
either state of `ServerCtx` goes out of scope.  Omitting `impl Drop` is also
what allows `release()` to move fields out of the struct without `unsafe` code
(Rust forbids moving out of types with a `Drop` impl).

**Typical patterns:**

```rust
// Two-way handler — lock held until handler returns.
server.register_handler("/svc/Read",
    |ctx, req: ReadRequest| async move {
        // ctx: ServerCtx<Locked> (= ServerCtx)
        Ok(Some(ReadResponse { value: "hello".into() }))
        // ctx dropped here → lock released
    });

// Streaming handler — release early, keep sending.
server.register_streaming_handler("/svc/Stream",
    |ctx, req: ReadRequest| async move {
        let ctx = ctx.release();   // ctx: ServerCtx<Released>; lock released now
        ctx.send(ReadResponse { value: "first".into() })?;
        do_slow_work().await;      // next message can dispatch during this
        ctx.send(ReadResponse { value: "second".into() })?;
        Ok(())
    });
```
```

**FIFO ordering:** The server holds a `tokio::sync::Mutex` guard for each
inbound message until the handler calls `ctx.release()` or returns.  This
serialises dispatch per stream while allowing handlers to run concurrently once
they release.

---

## Interceptors

### Client-side

```rust
pub type Interceptor = Arc<dyn Fn(CallInfo) -> BoxFuture<…> + Send + Sync>;

pub fn interceptor<F, Fut>(f: F) -> Interceptor
where F: Fn(CallInfo) -> Fut + Send + Sync + 'static,
      Fut: Future<Output = Result<(), Error>> + Send + 'static;

pub struct CallInfo {
    pub method: String,
    pub node_ids: Vec<u32>,
    pub metadata: Vec<(String, String)>,
}
```

Return `Ok(())` to allow the call; `Err(e)` to abort it.  Attach to a context
with `.with_interceptor(i)`.

### Server-side

```rust
pub type ServerInterceptor = Arc<dyn Fn(ServerCallInfo) -> BoxFuture<…> + Send + Sync>;

pub fn server_interceptor<F, Fut>(f: F) -> ServerInterceptor;

pub struct ServerCallInfo {
    pub method: String,
    pub peer_addr: Option<SocketAddr>,
}
```

Return `Ok(())` to allow the call; `Err(Status)` to reject it — the status is
sent back to the client.  Attach to a server with `.with_interceptor(i)`.

---

## Health checking

Active health probes, distinct from the reactive `NodeStatus` watch.

```rust
pub fn check_node(node: Node, config: HealthConfig) -> NodeHealthChecker;

pub struct HealthConfig {
    pub interval: Duration,   // default 5 s
    pub timeout: Duration,    // default 2 s
}

pub struct NodeHealthChecker { … }

impl NodeHealthChecker {
    pub fn status(&self) -> HealthStatus;
    pub fn subscribe(&self) -> watch::Receiver<HealthStatus>;
}

pub enum HealthStatus { Unknown, Healthy, Unhealthy }
```

`check_node` starts a background task that sends a probe immediately, then
repeats every `interval`.  The task is cancelled when `NodeHealthChecker` is
dropped.

---

## Error types

```rust
pub enum Error {
    QuorumCall(QuorumCallError),
    NodeClosed,
    StreamDown,
    Codec(String),
    Cancelled,
    Transport(tonic::Status),
}

pub struct QuorumCallError {
    pub cause: QuorumCallCause,
    pub node_errors: Vec<NodeError>,
}

pub enum QuorumCallCause { Incomplete, SendFailure }

pub struct NodeError {
    pub node_id: u32,
    pub cause: tonic::Status,
}
```

---

## Per-call metadata

Metadata is a `Vec<(String, String)>` attached to a context with
`.with_metadata(key, value)`.  Multiple entries are allowed; duplicate keys are
forwarded (last write wins on the wire since the proto field is a map).

The server receives metadata in `ServerCtx.metadata` as an ordered `Vec`.  Use
`ctx.metadata_get("key")` for a single-key lookup.

---

## CancellationToken

`quorums` re-exports `tokio_util::sync::CancellationToken`.  Every context
carries one; it can be overridden with `.with_cancel(token)` or given a
deadline with `.with_timeout(duration)`.

All blocking operations (`rpc_call`, terminal methods on `Responses`,
`Correctable::next`, etc.) return `Err(Error::Cancelled)` when the token fires.

---

## Code generation (`quorums-build`)

`quorums-build` is a separate build-time crate that hooks into `prost_build`'s
`ServiceGenerator` to emit typed wrappers from `.proto` service definitions.

```
quorums_build::configure()
    .method("/pkg.Svc/Method", quorums_build::CallType::QuorumCall)
    .compile(&["proto/svc.proto"], &["proto"])
    .unwrap();
```

Generated output (written to `OUT_DIR/{package}.rs`):

- `{service}_client` module — one `pub async fn` per annotated method; types
  match the call type (see call-type table in the top-level README).
- `{service}_server` module — `{Service}Server` trait with one method per
  annotated proto method, plus `register_{service}(server, Arc<impl Trait>)`.

Include in your crate:

```rust
mod pb {
    tonic::include_proto!("mypackage");   // or include!(concat!(env!("OUT_DIR"), "/mypackage.rs"))
}
use pb::{my_service_client, my_service_server};
```
