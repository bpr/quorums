use std::collections::HashSet;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, NodeError, QuorumCallCause, QuorumCallError};
use crate::responses::NodeResponse;

/// Handle returned by [`correctable_call`][crate::call_types::correctable_call].
///
/// Each node in the configuration can send **multiple** responses before
/// signalling completion.  `Correctable<T>` implements [`futures::Stream`] so
/// callers can consume updates one at a time, or use a terminal method to wait
/// for a quorum of distinct nodes to have responded at least once.
///
/// # Streaming
///
/// `Correctable<T>` implements `futures::Stream<Item = Result<NodeResponse<T>, Error>>`:
///
/// ```ignore
/// use futures::StreamExt;
///
/// while let Some(item) = correctable.next().await {
///     match item {
///         Ok(nr)  => println!("node {}: {:?}", nr.node_id, nr.result),
///         Err(e)  => { /* cancelled */ break; }
///     }
/// }
/// ```
///
/// `Stream::next()` yields `Some(Err(Error::Cancelled))` if the context's
/// cancellation token fires, and `None` once all node streams have closed.
///
/// # Terminal methods
///
/// | Method | Returns when |
/// |--------|-------------|
/// | [`first`][Self::first] | any one node has sent a successful response |
/// | [`majority`][Self::majority] | ⌈(n+1)/2⌉ distinct nodes have responded |
/// | [`all`][Self::all] | all n nodes have responded |
/// | [`threshold`][Self::threshold] | at least k distinct nodes have responded |
/// | [`quorum`][Self::quorum] | user-supplied predicate returns `Some` |
///
/// `first`, `majority`, `all`, and `threshold` count **distinct** nodes.
/// [`quorum`][Self::quorum] sees every successful value in arrival order
/// (including multiple values from the same node).
#[must_use = "call a terminal method or iterate via StreamExt::next() — \
              the fan-out has already been dispatched"]
pub struct Correctable<T> {
    pub(crate) rx: mpsc::UnboundedReceiver<NodeResponse<T>>,
    pub(crate) size: usize,
    pub(crate) cancel: CancellationToken,
}

// ── Stream impl ───────────────────────────────────────────────────────────────

impl<T> Stream for Correctable<T> {
    type Item = Result<NodeResponse<T>, Error>;

    /// Poll for the next item.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(Ok(nr)))` — a response arrived from one of the nodes.
    /// - `Poll::Ready(None)` — every node's stream has closed.
    /// - `Poll::Ready(Some(Err(Error::Cancelled)))` — the context's token fired.
    /// - `Poll::Pending` — waiting for the next message or cancellation.
    ///
    /// Cancellation is checked before the receive queue (biased), matching the
    /// behaviour of the async terminal methods.  When the receive queue is empty,
    /// a lightweight waker task is spawned so that cancellation is delivered
    /// promptly without waiting for the next inbound message.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `Correctable<T>` is Unpin (all fields are Unpin), so get_mut is safe.
        let this = self.get_mut();

        // Biased: deliver cancellation before any queued items.
        if this.cancel.is_cancelled() {
            return Poll::Ready(Some(Err(Error::Cancelled)));
        }

        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(nr)) => Poll::Ready(Some(Ok(nr))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                // While the receive queue is empty, arrange for this task to be
                // woken when the cancellation token fires.  A tiny background task
                // is spawned to do the wakeup; it exits as soon as the token fires
                // or is dropped.  One such task is spawned per poll_next that
                // returns Pending, so in normal usage (one outstanding poll at a
                // time) there is at most one live waker task per stream.
                let cancel = this.cancel.clone();
                let waker = cx.waker().clone();
                tokio::spawn(async move {
                    cancel.cancelled().await;
                    waker.wake();
                });
                Poll::Pending
            }
        }
    }
}

// ── Methods ───────────────────────────────────────────────────────────────────

impl<T> Correctable<T> {
    /// Number of nodes in the configuration this call was sent to.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Wait for the first successful response from any node.
    pub async fn first(self) -> Result<T, Error> {
        self.threshold(1).await
    }

    /// Wait until a simple majority (⌈(n+1)/2⌉) of distinct nodes have each
    /// sent at least one successful response.
    pub async fn majority(self) -> Result<T, Error> {
        let q = self.size / 2 + 1;
        self.threshold(q).await
    }

    /// Wait for **all** nodes to have sent at least one successful response.
    pub async fn all(self) -> Result<T, Error> {
        let n = self.size;
        self.threshold(n).await
    }

    /// Wait until at least `k` **distinct** nodes have each sent at least one
    /// successful response, then return the most recently received successful
    /// value.
    ///
    /// Returns [`Error::QuorumCall`] with cause `Incomplete` if too many nodes
    /// fail before the threshold can be reached, or the channel closes early.
    /// Returns [`Error::Cancelled`] if the context was cancelled.
    pub async fn threshold(mut self, k: usize) -> Result<T, Error> {
        let mut responded: HashSet<u32> = HashSet::new(); // node IDs with ≥1 success
        let mut failed: HashSet<u32> = HashSet::new(); // node IDs that only errored
        let mut node_errors: Vec<NodeError> = Vec::new();

        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(Error::Cancelled),
                maybe = self.rx.recv() => {
                    match maybe {
                        None => break,
                        Some(nr) => match nr.result {
                            Ok(val) => {
                                let is_new = !responded.contains(&nr.node_id);
                                responded.insert(nr.node_id);
                                if is_new && responded.len() >= k {
                                    return Ok(val);
                                }
                            }
                            Err(Error::Transport(ref s)) => {
                                // Only count as a failure if this node has never succeeded.
                                if !responded.contains(&nr.node_id) && failed.insert(nr.node_id) {
                                    node_errors.push(NodeError { node_id: nr.node_id, cause: s.clone() });
                                    // Early exit: not enough nodes left to reach k.
                                    if failed.len() > self.size - k {
                                        return Err(Error::QuorumCall(QuorumCallError {
                                            cause: QuorumCallCause::Incomplete,
                                            node_errors,
                                        }));
                                    }
                                }
                            }
                            Err(_) => {}
                        },
                    }
                }
            }
        }

        Err(Error::QuorumCall(QuorumCallError {
            cause: QuorumCallCause::Incomplete,
            node_errors,
        }))
    }

    /// Drive aggregation with a user-provided quorum function.
    ///
    /// After each successful value (from any node, including repeated values
    /// from the same node), `f` is called with a slice of **all** successful
    /// values received so far, in arrival order.  When `f` returns `Some(value)`
    /// the call resolves immediately; returning `None` continues collecting.
    ///
    /// Returns [`Error::QuorumCall`] with cause `Incomplete` if all node streams
    /// have closed and `f` never returned `Some`, or [`Error::Cancelled`] if the
    /// context was cancelled.
    pub async fn quorum<F>(mut self, mut f: F) -> Result<T, Error>
    where
        F: FnMut(&[T]) -> Option<T>,
    {
        let mut oks: Vec<T> = Vec::new();
        let mut node_errors: Vec<NodeError> = Vec::new();

        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(Error::Cancelled),
                maybe = self.rx.recv() => {
                    match maybe {
                        None => break,
                        Some(nr) => match nr.result {
                            Ok(val) => {
                                oks.push(val);
                                if let Some(result) = f(&oks) {
                                    return Ok(result);
                                }
                            }
                            Err(Error::Transport(ref s)) => {
                                node_errors.push(NodeError { node_id: nr.node_id, cause: s.clone() });
                            }
                            Err(_) => {}
                        },
                    }
                }
            }
        }

        Err(Error::QuorumCall(QuorumCallError {
            cause: QuorumCallCause::Incomplete,
            node_errors,
        }))
    }
}
