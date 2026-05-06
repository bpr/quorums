use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use prost::Message as ProstMessage;
use tokio::sync::{mpsc, Mutex, OwnedMutexGuard};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::channel::{decode_payload, encode_payload, STATUS_STREAM_DONE};
use crate::health::HEALTH_METHOD;
use crate::interceptor::{run_server, ServerCallInfo, ServerInterceptor};
use crate::proto::gorums::{gorums_server, HealthCheckRequest, HealthCheckResponse, Message};

// ── Typestate markers ─────────────────────────────────────────────────────────
//
// # The typestate pattern
//
// A *typestate* encodes a value's runtime state into its *type* so the
// compiler can enforce state-machine transitions at compile time rather than
// detecting misuse at runtime.
//
// The classic Go gorums API has `ctx.Release()` which releases the per-stream
// ordering lock early.  Calling it twice is harmless but meaningless; never
// calling it delays the next message dispatch unnecessarily; nothing in the
// type system tells you which has happened.
//
// Here, the state is captured in `ServerCtx<S>`:
//
//   ServerCtx<Locked>   — the ordering lock is held
//        │
//        │  .release()   consumes self, drops the lock, returns the next state
//        ▼
//   ServerCtx<Released> — the lock is gone; send() still works
//
// Because `release` takes `self` (not `&mut self`), the compiler enforces:
//   - You cannot call `release` twice (the first call consumes the value).
//   - You cannot use the locked context after releasing it.
//   - The released context is a regular value you can pass around and send on.
//
// Both states share `send()` and `metadata()` via a blanket impl over `<S>`.
// The default type parameter (`S = Locked`) means that plain `ServerCtx` in
// handler signatures continues to mean `ServerCtx<Locked>` — no source
// changes needed for handlers that never call `release`.

/// Marker type: the per-stream ordering lock is held.
///
/// This is the default state parameter of [`ServerCtx`]:
/// `ServerCtx` and `ServerCtx<Locked>` are the same type.
pub struct Locked(());

/// Marker type: the per-stream ordering lock has been released.
///
/// Obtained by calling [`ServerCtx::release`] on a [`ServerCtx<Locked>`].
pub struct Released(());

// ── Internal handler type ─────────────────────────────────────────────────────

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
// HandlerFn always receives a Locked context.  The user handler may call
// release() to transition to Released, but the erased boundary is always Locked.
type HandlerFn =
    Arc<dyn Fn(ServerCtx<Locked>, Message) -> BoxFuture<Option<Message>> + Send + Sync + 'static>;

// ── ServerCtx ────────────────────────────────────────────────────────────────

/// Context passed to every server-side handler.
///
/// The type parameter `S` tracks whether the per-stream ordering lock is held:
///
/// | Type | Lock state |
/// |------|-----------|
/// | `ServerCtx<Locked>` (= `ServerCtx`) | held — next message waits |
/// | `ServerCtx<Released>` | released — next message may dispatch |
///
/// Handlers receive `ServerCtx<Locked>`.  Calling [`release`][Self::release]
/// consumes it and returns `ServerCtx<Released>`, which retains full access to
/// [`send`][Self::send] and [`metadata`][Self::metadata].
///
/// Dropping *either* state releases the lock (if it was still held), so
/// forgetting to call `release` is never a deadlock — it just means the next
/// message will not be dispatched until the current handler returns.
///
/// # Typical patterns
///
/// **Two-way handler (no early release needed):**
/// ```ignore
/// server.register_handler("/svc/Read",
///     |ctx, req: ReadRequest| async move {
///         // ctx is ServerCtx<Locked> (= ServerCtx).  Lock is released on drop.
///         Ok(Some(ReadResponse { value: lookup(&req.key) }))
///     });
/// ```
///
/// **Streaming handler (release early, keep sending):**
/// ```ignore
/// server.register_streaming_handler("/svc/StreamValues",
///     |ctx, req: ReadRequest| async move {
///         // Release the lock first so the next inbound message can be
///         // dispatched while we stream responses back.
///         let ctx = ctx.release();           // ctx is now ServerCtx<Released>
///
///         ctx.send(ReadResponse { value: "first".into() })?;
///         do_slow_work().await;
///         ctx.send(ReadResponse { value: "second".into() })?;
///         Ok(())
///     });
/// ```
///
/// **Accessing metadata before releasing:**
///
/// Because `release` moves `self`, take any values you need from metadata
/// first — or access them afterwards, since metadata is present on both states:
/// ```ignore
/// let ctx = ctx.release();
/// let trace = ctx.metadata_get("trace-id").unwrap_or("-");
/// ctx.send(response)?;
/// ```
pub struct ServerCtx<S = Locked> {
    /// Ordering guard.  Present only on `Locked`; taken on `release()` or drop.
    guard: Option<OwnedMutexGuard<()>>,
    /// Write end of the response channel for this stream.
    pub(crate) send_tx: mpsc::UnboundedSender<Result<Message, Status>>,
    /// Sequence number of the inbound request — stamped on all responses.
    pub(crate) msg_seq_no: u64,
    /// Method string of the inbound request — stamped on all responses.
    pub(crate) method: String,
    /// Per-call metadata forwarded by the client.
    pub metadata: Vec<(String, String)>,
    /// Phantom marker — zero-sized, erased at compile time.
    _state: PhantomData<S>,
}

// ── Methods shared by both states ─────────────────────────────────────────────

impl<S> ServerCtx<S> {
    /// Return the per-call metadata sent by the client, in insertion order.
    pub fn metadata(&self) -> &[(String, String)] {
        &self.metadata
    }

    /// Look up the first value for `key` in the per-call metadata.
    pub fn metadata_get(&self, key: &str) -> Option<&str> {
        self.metadata
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Push one streaming response to the client.
    ///
    /// Available on both [`Locked`] and [`Released`] contexts.  Used by
    /// handlers registered with
    /// [`register_streaming_handler`][Server::register_streaming_handler].
    ///
    /// Returns `Err(Status::unavailable)` if the response stream has closed.
    pub fn send<Resp: ProstMessage>(&self, resp: Resp) -> Result<(), Status> {
        let msg = Message {
            message_seq_no: self.msg_seq_no,
            method: self.method.clone(),
            status_code: 0,
            status_message: String::new(),
            payload: encode_payload(&resp),
            metadata: std::collections::HashMap::new(),
        };
        self.send_tx
            .send(Ok(msg))
            .map_err(|_| Status::unavailable("stream closed"))
    }
}

// ── Locked-only methods ───────────────────────────────────────────────────────

impl ServerCtx<Locked> {
    /// Release the per-stream ordering lock and return a [`Released`] context.
    ///
    /// After this call the server may dispatch the next inbound message on
    /// this stream while the current handler continues running.
    ///
    /// The lock is also released automatically when *any* `ServerCtx` is
    /// dropped, so calling `release` is only necessary when you want the next
    /// message to be dispatched *before* the handler returns.
    ///
    /// # Typestate transition
    ///
    /// `release` consumes `self` so the transition can only happen once — the
    /// compiler rejects a second call:
    ///
    /// ```ignore
    /// let ctx = ctx.release();   // ok: ServerCtx<Locked> → ServerCtx<Released>
    /// let ctx = ctx.release();   // error[E0599]: no method `release` on `ServerCtx<Released>`
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// server.register_streaming_handler("/svc/Stream",
    ///     |ctx, req: MyRequest| async move {
    ///         let ctx = ctx.release();   // unlock; next message may now dispatch
    ///         ctx.send(MyResponse { value: compute_first(&req) })?;
    ///         ctx.send(MyResponse { value: compute_second(&req) })?;
    ///         Ok(())
    ///     });
    /// ```
    pub fn release(mut self) -> ServerCtx<Released> {
        self.guard.take(); // drop the OwnedMutexGuard — lock released here
        ServerCtx {
            guard: None,
            send_tx: self.send_tx,
            msg_seq_no: self.msg_seq_no,
            method: self.method,
            metadata: self.metadata,
            _state: PhantomData,
        }
    }
}

// No explicit Drop impl is needed.  `guard: Option<OwnedMutexGuard<()>>`
// releases the lock through the field's own destructor when either state of
// `ServerCtx` is dropped.  Omitting `impl Drop` is what allows `release()` to
// move fields out of `ServerCtx<Locked>` without unsafe code — Rust forbids
// moving out of types that have a `Drop` impl (E0509).

// ── Server ───────────────────────────────────────────────────────────────────

/// The gorums server.
///
/// 1. Optionally attach server interceptors with [`with_interceptor`][Self::with_interceptor].
/// 2. Call [`register_handler`][Self::register_handler] for each RPC method.
/// 3. Call [`serve`][Self::serve] to start accepting connections.
pub struct Server {
    handlers: HashMap<String, HandlerFn>,
    interceptors: Vec<ServerInterceptor>,
}

impl Server {
    pub fn new() -> Self {
        let mut s = Server { handlers: HashMap::new(), interceptors: Vec::new() };
        // Built-in health check — always registered; used by `check_node`.
        s.register_handler::<HealthCheckRequest, HealthCheckResponse, _, _>(
            HEALTH_METHOD,
            |_ctx, _req| async { Ok(Some(HealthCheckResponse { ok: true })) },
        );
        s
    }

    /// Append a server-side interceptor.
    ///
    /// Interceptors run in registration order before the handler is invoked
    /// for each incoming message.  Returning `Err(Status)` from an interceptor
    /// sends that status back to the client and skips the handler.
    pub fn with_interceptor(mut self, interceptor: ServerInterceptor) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Register an async handler for `method`.
    ///
    /// The handler receives [`ServerCtx<Locked>`][ServerCtx] and the decoded
    /// request.  It must return `Ok(Some(resp))` for two-way methods or
    /// `Ok(None)` for one-way (unicast / multicast) methods.
    ///
    /// The ordering lock is held for the lifetime of the handler unless the
    /// handler explicitly calls [`ctx.release()`][ServerCtx::release].
    pub fn register_handler<Req, Resp, F, Fut>(&mut self, method: impl Into<String>, handler: F)
    where
        Req: ProstMessage + Default + Send + 'static,
        Resp: ProstMessage + Send + 'static,
        F: Fn(ServerCtx<Locked>, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Resp>, Status>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let erased: HandlerFn = Arc::new(move |ctx: ServerCtx<Locked>, wire: Message| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let req = match decode_payload::<Req>(&wire) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("[quorums] decode error for {}: {e}", wire.method);
                        return None;
                    }
                };
                match handler(ctx, req).await {
                    Ok(None) => None,
                    Ok(Some(resp)) => Some(Message {
                        message_seq_no: wire.message_seq_no,
                        method: wire.method.clone(),
                        status_code: 0,
                        status_message: String::new(),
                        payload: encode_payload(&resp),
                        metadata: std::collections::HashMap::new(),
                    }),
                    Err(status) => Some(Message {
                        message_seq_no: wire.message_seq_no,
                        method: wire.method.clone(),
                        status_code: status.code() as u32,
                        status_message: status.message().to_string(),
                        payload: Vec::new(),
                        metadata: std::collections::HashMap::new(),
                    }),
                }
            })
        });

        self.handlers.insert(method.into(), erased);
    }

    /// Register an async streaming handler for `method`.
    ///
    /// Unlike [`register_handler`], the handler sends zero or more responses
    /// via [`ServerCtx::send`] and returns `Ok(())` when done.  An
    /// end-of-stream sentinel is automatically sent to the client after the
    /// handler returns.
    ///
    /// For correctable calls it is almost always correct to release the
    /// ordering lock immediately so that subsequent requests on the same stream
    /// are not blocked behind a long-running streaming response:
    ///
    /// ```ignore
    /// server.register_streaming_handler("/svc/MyCorrectableMethod",
    ///     |ctx, req: MyRequest| async move {
    ///         let ctx = ctx.release();   // next message may dispatch immediately
    ///         ctx.send(MyResponse { value: 1 })?;
    ///         ctx.send(MyResponse { value: 2 })?;
    ///         Ok(())
    ///     });
    /// ```
    pub fn register_streaming_handler<Req, Resp, F, Fut>(
        &mut self,
        method: impl Into<String>,
        handler: F,
    ) where
        Req: ProstMessage + Default + Send + 'static,
        Resp: ProstMessage + Send + 'static,
        F: Fn(ServerCtx<Locked>, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Status>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let erased: HandlerFn = Arc::new(move |ctx: ServerCtx<Locked>, wire: Message| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let req = match decode_payload::<Req>(&wire) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("[quorums] decode error for {}: {e}", wire.method);
                        return None;
                    }
                };

                // Capture the response channel and sequence number before
                // moving ctx into the user handler (which may call release()).
                let resp_tx = ctx.send_tx.clone();
                let msg_seq_no = ctx.msg_seq_no;
                let method_str = ctx.method.clone();

                // Run the user handler; responses go via ctx.send().
                if let Err(status) = handler(ctx, req).await {
                    // Send an error response before the EOS sentinel.
                    let _ = resp_tx.send(Ok(Message {
                        message_seq_no: msg_seq_no,
                        method: method_str.clone(),
                        status_code: status.code() as u32,
                        status_message: status.message().to_string(),
                        payload: Vec::new(),
                        metadata: std::collections::HashMap::new(),
                    }));
                }

                // Always send the end-of-stream sentinel so the client can
                // close its streaming receiver.
                let _ = resp_tx.send(Ok(Message {
                    message_seq_no: msg_seq_no,
                    method: method_str,
                    status_code: STATUS_STREAM_DONE,
                    status_message: String::new(),
                    payload: Vec::new(),
                    metadata: std::collections::HashMap::new(),
                }));

                None // outer dispatch sends nothing extra
            })
        });

        self.handlers.insert(method.into(), erased);
    }

    /// Start serving on `addr`.  Blocks until shutdown.
    pub async fn serve(self, addr: std::net::SocketAddr) -> Result<(), tonic::transport::Error> {
        let handlers = Arc::new(self.handlers);
        let interceptors = Arc::new(self.interceptors);
        tonic::transport::Server::builder()
            .add_service(gorums_server::GorumsServer::new(GorumsService { handlers, interceptors }))
            .serve(addr)
            .await
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tonic service ─────────────────────────────────────────────────────────────

struct GorumsService {
    handlers: Arc<HashMap<String, HandlerFn>>,
    interceptors: Arc<Vec<ServerInterceptor>>,
}

#[tonic::async_trait]
impl gorums_server::Gorums for GorumsService {
    type NodeStreamStream = UnboundedReceiverStream<Result<Message, Status>>;

    async fn node_stream(
        &self,
        request: Request<Streaming<Message>>,
    ) -> Result<Response<Self::NodeStreamStream>, Status> {
        let peer_addr = request.remote_addr();
        let mut inbound = request.into_inner();
        let handlers = Arc::clone(&self.handlers);
        let interceptors = Arc::clone(&self.interceptors);
        let (resp_tx, resp_rx) = mpsc::unbounded_channel::<Result<Message, Status>>();

        tokio::spawn(async move {
            let mutex = Arc::new(Mutex::new(()));

            loop {
                let guard: OwnedMutexGuard<()> = Arc::clone(&mutex).lock_owned().await;

                let msg = match inbound.message().await {
                    Ok(Some(m)) => m,
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("[quorums] server recv error: {e}");
                        break;
                    }
                };

                let method = msg.method.clone();
                let msg_seq_no = msg.message_seq_no;
                let handler = handlers.get(&method).cloned();
                let resp_tx2 = resp_tx.clone();
                let interceptors2 = Arc::clone(&interceptors);

                tokio::spawn(async move {
                    // Run server interceptors before handing off to the handler.
                    if let Err(status) = run_server(
                        &interceptors2,
                        ServerCallInfo { method: method.clone(), peer_addr },
                    )
                    .await
                    {
                        let _ = resp_tx2.send(Ok(Message {
                            message_seq_no: msg_seq_no,
                            method: method.clone(),
                            status_code: status.code() as u32,
                            status_message: status.message().to_string(),
                            payload: Vec::new(),
                            metadata: std::collections::HashMap::new(),
                        }));
                        return;
                    }

                    // Convert the inbound metadata map to an ordered Vec for the handler.
                    let metadata: Vec<(String, String)> =
                        msg.metadata.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

                    // Construct a Locked context.  The guard is moved in and will be
                    // released either by ServerCtx::release() or on drop.
                    let ctx = ServerCtx {
                        guard: Some(guard),
                        send_tx: resp_tx2.clone(),
                        msg_seq_no,
                        method: method.clone(),
                        metadata,
                        _state: PhantomData,
                    };

                    match handler {
                        None => {
                            eprintln!("[quorums] no handler registered for: {method}");
                        }
                        Some(h) => {
                            if let Some(resp_msg) = h(ctx, msg).await {
                                let _ = resp_tx2.send(Ok(resp_msg));
                            }
                        }
                    }
                });
            }
        });

        Ok(Response::new(UnboundedReceiverStream::new(resp_rx)))
    }
}
