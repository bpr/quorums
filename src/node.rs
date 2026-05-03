use std::sync::Arc;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::channel::NodeChannel;
use crate::interceptor::Interceptor;
use crate::router::Router;

/// Connectivity state of a remote node.
///
/// The state machine transitions are:
///
/// ```text
/// (start) → Connecting → Connected
///                ↑           |
///                |   stream  ↓
///                +── Reconnecting ─→ Unreachable (retry)
///
/// Any state → Closed  (clean shutdown)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    /// Initial connection attempt in progress.
    Connecting,
    /// The gRPC stream is established and healthy.
    Connected,
    /// The stream dropped; attempting to reconnect.
    Reconnecting,
    /// A connection attempt failed; will retry after a backoff.
    Unreachable,
    /// The node's send channel was closed — no further reconnects.
    Closed,
}

/// A remote peer on which RPCs, unicasts, and quorum calls can be invoked.
///
/// `Node` is cheaply cloneable (`Arc`-backed) and safe to share across tasks.
#[derive(Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
}

struct NodeInner {
    id: u32,
    addr: String,
    channel: NodeChannel,
}

impl Node {
    /// Create a new node and start its background channel task.
    pub(crate) fn new(id: u32, addr: String) -> Self {
        let router = Router::new();
        let channel = NodeChannel::connect(id, addr.clone(), router);
        Node {
            inner: Arc::new(NodeInner { id, addr, channel }),
        }
    }

    pub fn id(&self) -> u32 {
        self.inner.id
    }

    pub fn address(&self) -> &str {
        &self.inner.addr
    }

    pub(crate) fn channel(&self) -> &NodeChannel {
        &self.inner.channel
    }

    /// Return the node's current connectivity status.
    pub fn status(&self) -> NodeStatus {
        self.inner.channel.current_status()
    }

    /// Subscribe to status changes.  The returned [`watch::Receiver`] always
    /// holds the latest status and wakes the holder when it changes.
    ///
    /// ```ignore
    /// let mut rx = node.subscribe_status();
    /// while rx.changed().await.is_ok() {
    ///     println!("node {} → {:?}", node.id(), *rx.borrow());
    /// }
    /// ```
    pub fn subscribe_status(&self) -> watch::Receiver<NodeStatus> {
        self.inner.channel.subscribe_status()
    }
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node(id={}, addr={})", self.inner.id, self.inner.addr)
    }
}

/// A per-request handle that bundles a `Node` reference with cancellation,
/// deadline, and interceptor chain context.
///
/// Created via [`Node::context`].
///
/// # Cancellation
///
/// ```ignore
/// let ctx = node.context().with_timeout(Duration::from_millis(200));
/// ```
///
/// # Interceptors
///
/// ```ignore
/// use quorums::interceptor;
/// let ctx = node.context()
///     .with_interceptor(interceptor(|info| async move {
///         println!("→ {}", info.method);
///         Ok(())
///     }));
/// ```
#[derive(Clone)]
pub struct NodeContext {
    pub(crate) node: Node,
    pub(crate) cancel: CancellationToken,
    pub(crate) interceptors: Vec<Interceptor>,
    pub(crate) metadata: Vec<(String, String)>,
}

impl Node {
    /// Wrap this node in a `NodeContext` for a single call.
    pub fn context(&self) -> NodeContext {
        NodeContext {
            node: self.clone(),
            cancel: CancellationToken::new(),
            interceptors: Vec::new(),
            metadata: Vec::new(),
        }
    }
}

impl NodeContext {
    /// Override the cancellation token.
    pub fn with_cancel(mut self, token: CancellationToken) -> Self {
        self.cancel = token;
        self
    }

    /// Set a deadline relative to now.
    pub fn with_timeout(self, duration: std::time::Duration) -> Self {
        let child = self.cancel.child_token();
        let child2 = child.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            child2.cancel();
        });
        NodeContext {
            node: self.node,
            cancel: child,
            interceptors: self.interceptors,
            metadata: self.metadata,
        }
    }

    /// Append an interceptor to the chain for this call.
    pub fn with_interceptor(mut self, i: Interceptor) -> Self {
        self.interceptors.push(i);
        self
    }

    /// Attach a metadata key/value pair to this call.
    ///
    /// Multiple calls accumulate entries.  Duplicate keys are allowed and
    /// are all forwarded to the server (last write wins on the wire if the
    /// same key appears more than once, since the proto field is a map).
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.push((key.into(), value.into()));
        self
    }
}
