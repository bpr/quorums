use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use prost::Message as ProstMessage;
use tokio::sync::{Mutex, OwnedMutexGuard, mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::channel::{STATUS_STREAM_DONE, decode_payload, encode_payload};
use crate::health::HEALTH_METHOD;
use crate::interceptor::{ServerCallInfo, ServerInterceptor, run_server};
use crate::proto::gorums::{HealthCheckRequest, HealthCheckResponse, Message, gorums_server};

// ── Internal handler type ─────────────────────────────────────────────────────

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
type HandlerFn =
    Arc<dyn Fn(ServerCtx, Message) -> BoxFuture<Option<Message>> + Send + Sync + 'static>;

// ── ServerCtx ────────────────────────────────────────────────────────────────

/// Context passed to every server-side handler.
///
/// Dropping this (or calling [`release`][Self::release]) releases the
/// per-stream ordering lock, allowing the server to begin processing the
/// next inbound message.
pub struct ServerCtx {
    /// Ordering guard.  Dropped on `release()` or when `ServerCtx` is dropped.
    guard: Option<OwnedMutexGuard<()>>,
    /// Write end of the response channel for this stream.
    /// Used directly by streaming (correctable) handlers via [`send`][Self::send].
    pub(crate) send_tx: mpsc::UnboundedSender<Result<Message, Status>>,
    /// Sequence number of the inbound request — stamped on all responses.
    pub(crate) msg_seq_no: u64,
    /// Method string of the inbound request — stamped on all responses.
    pub(crate) method: String,
    /// Per-call metadata forwarded by the client via
    /// [`NodeContext::with_metadata`][crate::node::NodeContext::with_metadata] or
    /// [`ConfigContext::with_metadata`][crate::config::ConfigContext::with_metadata].
    pub metadata: Vec<(String, String)>,
}

impl ServerCtx {
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

    /// Release the ordering lock early so that the next inbound message can
    /// be dispatched while this handler continues running.
    ///
    /// Mirrors Go's `ctx.Release()`.  Safe to call multiple times.
    pub fn release(&mut self) {
        self.guard.take();
    }

    /// Push one streaming response to the client.
    ///
    /// Used by handlers registered with
    /// [`register_streaming_handler`][crate::server::Server::register_streaming_handler].
    /// Returns `Err(Status::unavailable)` if the stream has closed.
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

impl Drop for ServerCtx {
    fn drop(&mut self) {
        self.guard.take();
    }
}

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
        let mut s = Server {
            handlers: HashMap::new(),
            interceptors: Vec::new(),
        };
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
    /// `F` takes a [`ServerCtx`] and the decoded `Req`, and returns a future
    /// that resolves to `Ok(Some(resp))` for two-way methods or `Ok(None)` for
    /// one-way (unicast / multicast) methods.
    pub fn register_handler<Req, Resp, F, Fut>(&mut self, method: impl Into<String>, handler: F)
    where
        Req: ProstMessage + Default + Send + 'static,
        Resp: ProstMessage + Send + 'static,
        F: Fn(ServerCtx, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Resp>, Status>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let erased: HandlerFn = Arc::new(move |ctx: ServerCtx, wire: Message| {
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
    /// Typical pattern:
    /// ```ignore
    /// server.register_streaming_handler("/svc/MyCorrectableMethod",
    ///     |mut ctx, req: MyRequest| async move {
    ///         ctx.release(); // let next message dispatch immediately
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
        F: Fn(ServerCtx, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Status>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let erased: HandlerFn = Arc::new(move |ctx: ServerCtx, wire: Message| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let req = match decode_payload::<Req>(&wire) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("[quorums] decode error for {}: {e}", wire.method);
                        return None;
                    }
                };

                // Capture for EOS sentinel before ctx is moved into the handler.
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
            .add_service(gorums_server::GorumsServer::new(GorumsService {
                handlers,
                interceptors,
            }))
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
                        ServerCallInfo {
                            method: method.clone(),
                            peer_addr,
                        },
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
                    let metadata: Vec<(String, String)> = msg
                        .metadata
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();

                    let ctx = ServerCtx {
                        guard: Some(guard),
                        send_tx: resp_tx2.clone(),
                        msg_seq_no,
                        method: method.clone(),
                        metadata,
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
