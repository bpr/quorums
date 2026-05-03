use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use tonic::Status;

use crate::error::Error;

/// Metadata about an outgoing call, passed to each interceptor in the chain.
#[derive(Clone, Debug)]
pub struct CallInfo {
    /// Full gRPC method path, e.g. `"/storage.Storage/Read"`.
    pub method: String,
    /// IDs of the nodes this call is being dispatched to.
    /// Single-node calls (`rpc_call`, `unicast`) have exactly one entry.
    /// Fan-out calls (`multicast`, `quorum_call`, `correctable_call`) have one
    /// entry per node in the configuration.
    pub node_ids: Vec<u32>,
    /// Per-call metadata attached via
    /// [`NodeContext::with_metadata`][crate::node::NodeContext::with_metadata] or
    /// [`ConfigContext::with_metadata`][crate::config::ConfigContext::with_metadata].
    pub metadata: Vec<(String, String)>,
}

/// An async client interceptor.
///
/// Interceptors run in registration order before the call is dispatched.
/// Return `Ok(())` to allow the call to proceed, or `Err(e)` to abort it.
///
/// # Example
/// ```ignore
/// use quorums::{interceptor, CallInfo, Error};
///
/// // Logging interceptor
/// let log = interceptor(|info: CallInfo| async move {
///     println!("→ {} to {:?}", info.method, info.node_ids);
///     Ok(())
/// });
///
/// // Auth interceptor
/// let auth = interceptor(|info: CallInfo| async move {
///     if info.method.starts_with("/internal") {
///         return Err(Error::Cancelled); // re-use Cancelled for simplicity
///     }
///     Ok(())
/// });
///
/// let ctx = cfg.context()
///     .with_interceptor(log)
///     .with_interceptor(auth);
/// ```
pub type Interceptor = Arc<dyn Fn(CallInfo) -> BoxFuture<'static, Result<(), Error>> + Send + Sync>;

/// Construct an [`Interceptor`] from an async closure or function.
///
/// This helper boxes the future so the closure doesn't need to name the type.
pub fn interceptor<F, Fut>(f: F) -> Interceptor
where
    F: Fn(CallInfo) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Error>> + Send + 'static,
{
    Arc::new(move |info| Box::pin(f(info)))
}

/// Run every interceptor in `chain` with a clone of `info`.  Returns the
/// first error encountered, or `Ok(())` if all pass.
pub(crate) async fn run(chain: &[Interceptor], info: CallInfo) -> Result<(), Error> {
    for i in chain {
        i(info.clone()).await?;
    }
    Ok(())
}

// ── Server-side interceptors ──────────────────────────────────────────────────

/// Metadata about an incoming call, passed to each server interceptor.
#[derive(Clone, Debug)]
pub struct ServerCallInfo {
    /// Full gRPC method path, e.g. `"/storage.Storage/Read"`.
    pub method: String,
    /// Remote address of the connecting client, if available.
    pub peer_addr: Option<std::net::SocketAddr>,
}

/// An async server interceptor.
///
/// Interceptors run in registration order before the handler is invoked.
/// Return `Ok(())` to allow the call to proceed, or `Err(status)` to reject
/// it — the status is sent back to the client as an error response.
///
/// # Example
/// ```ignore
/// use quorums::{server_interceptor, ServerCallInfo};
/// use tonic::Status;
///
/// // Reject calls from unknown peers
/// let guard = server_interceptor(|info: ServerCallInfo| async move {
///     if info.peer_addr.is_none() {
///         return Err(Status::unauthenticated("peer address required"));
///     }
///     Ok(())
/// });
///
/// let mut server = quorums::Server::new()
///     .with_interceptor(guard);
/// ```
pub type ServerInterceptor =
    Arc<dyn Fn(ServerCallInfo) -> BoxFuture<'static, Result<(), Status>> + Send + Sync>;

/// Construct a [`ServerInterceptor`] from an async closure or function.
pub fn server_interceptor<F, Fut>(f: F) -> ServerInterceptor
where
    F: Fn(ServerCallInfo) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Status>> + Send + 'static,
{
    Arc::new(move |info| Box::pin(f(info)))
}

/// Run every server interceptor in `chain`.  Returns the first rejection, or
/// `Ok(())` if all pass.
pub(crate) async fn run_server(
    chain: &[ServerInterceptor],
    info: ServerCallInfo,
) -> Result<(), Status> {
    for i in chain {
        i(info.clone()).await?;
    }
    Ok(())
}
