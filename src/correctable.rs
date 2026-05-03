use std::collections::HashSet;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, NodeError, QuorumCallCause, QuorumCallError};
use crate::responses::NodeResponse;

/// Handle returned by [`correctable_call`][crate::call_types::correctable_call].
///
/// Each node in the configuration can send **multiple** responses before
/// signalling completion.  `Correctable<T>` lets callers either consume
/// updates one at a time via [`next`][Self::next] or wait for a quorum of
/// distinct nodes to have responded at least once via a terminal method.
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
pub struct Correctable<T> {
    pub(crate) rx: mpsc::UnboundedReceiver<NodeResponse<T>>,
    pub(crate) size: usize,
    pub(crate) cancel: CancellationToken,
}

impl<T> Correctable<T> {
    /// Number of nodes in the configuration this call was sent to.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Return the next response from any node.
    ///
    /// - `Ok(Some(response))` — a response arrived from one of the nodes.
    /// - `Ok(None)` — every node's stream has closed; no more responses will arrive.
    /// - `Err(Error::Cancelled)` — the context's cancellation token fired.
    pub async fn next(&mut self) -> Result<Option<NodeResponse<T>>, Error> {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(Error::Cancelled),
            maybe = self.rx.recv() => Ok(maybe),
        }
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
