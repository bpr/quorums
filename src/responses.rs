use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, NodeError, QuorumCallCause, QuorumCallError};

/// A single response from one node in a quorum call.
#[derive(Debug)]
pub struct NodeResponse<T> {
    pub node_id: u32,
    pub result: Result<T, Error>,
}

/// Collects responses from a quorum call as they arrive.
///
/// Created by [`quorum_call`][crate::call_types::quorum_call] and consumed
/// by a terminal method.
///
/// # Terminal methods
///
/// | Method | Returns when |
/// |--------|-------------|
/// | [`first`][Self::first] | any one node replies successfully |
/// | [`majority`][Self::majority] | ⌈(n+1)/2⌉ nodes reply successfully |
/// | [`all`][Self::all] | all n nodes reply successfully |
/// | [`threshold`][Self::threshold] | at least k nodes reply successfully |
/// | [`quorum`][Self::quorum] | user-supplied predicate returns `Some` |
///
/// `first`, `majority`, `all`, and `threshold` return the **first** successful
/// response received once their threshold is met.  [`quorum`][Self::quorum]
/// returns whatever value the predicate produces.
#[must_use = "call a terminal method (.majority(), .all(), .threshold(k), .quorum(f)) to \
              dispatch the fan-out and collect results"]
pub struct Responses<T> {
    pub(crate) rx: mpsc::Receiver<NodeResponse<T>>,
    pub(crate) size: usize,
    pub(crate) cancel: CancellationToken,
    /// Deferred fan-out.  `None` once `send_now()` has been called.
    pub(crate) launch: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl<T> Responses<T> {
    /// Number of nodes in the configuration this call was sent to.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Dispatch the fan-out immediately without blocking on a result.
    ///
    /// All terminal methods call this automatically on entry; use it explicitly
    /// only when you want to start network I/O before you are ready to block.
    /// Calling `send_now` multiple times is a no-op.
    pub fn send_now(&mut self) {
        if let Some(f) = self.launch.take() {
            f();
        }
    }

    /// Wait for the first successful response from any node.
    pub async fn first(self) -> Result<T, Error> {
        self.threshold(1).await
    }

    /// Wait until a simple majority (⌈(n+1)/2⌉) have replied successfully.
    pub async fn majority(self) -> Result<T, Error> {
        let q = self.size / 2 + 1;
        self.threshold(q).await
    }

    /// Wait for **all** nodes to reply successfully.
    pub async fn all(self) -> Result<T, Error> {
        let n = self.size;
        self.threshold(n).await
    }

    /// Wait for at least `k` successful replies, then return the first one.
    ///
    /// Returns [`Error::QuorumCall`] with cause `Incomplete` if the channel
    /// closes before `k` successes are collected, or [`Error::Cancelled`] if
    /// the context was cancelled.
    pub async fn threshold(mut self, k: usize) -> Result<T, Error> {
        self.send_now();
        let mut count = 0usize;
        let mut first_ok: Option<T> = None;
        let mut node_errors: Vec<NodeError> = Vec::new();

        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(Error::Cancelled),
                maybe = self.rx.recv() => {
                    match maybe {
                        None => break,
                        Some(nr) => match nr.result {
                            Err(Error::Transport(ref s)) => {
                                node_errors.push(NodeError {
                                    node_id: nr.node_id,
                                    cause: s.clone(),
                                });
                            }
                            Err(_) => {
                                node_errors.push(NodeError {
                                    node_id: nr.node_id,
                                    cause: tonic::Status::internal("internal error"),
                                });
                            }
                            Ok(val) => {
                                count += 1;
                                if first_ok.is_none() {
                                    first_ok = Some(val);
                                }
                                if count >= k {
                                    return Ok(first_ok.unwrap());
                                }
                            }
                        },
                    }
                }
            }
        }

        // Channel closed before threshold was reached.
        Err(Error::QuorumCall(QuorumCallError {
            cause: QuorumCallCause::Incomplete,
            node_errors,
        }))
    }

    /// Drive aggregation with a user-provided quorum function.
    ///
    /// After each successful reply, `f` is called with a slice of **all**
    /// successful replies collected so far.  When `f` returns `Some(value)`
    /// the call resolves immediately with that value; returning `None`
    /// continues collecting.
    ///
    /// Returns [`Error::QuorumCall`] with cause `Incomplete` if all nodes have
    /// replied and `f` never returned `Some`, or [`Error::Cancelled`] if the
    /// context was cancelled.
    pub async fn quorum<F>(mut self, mut f: F) -> Result<T, Error>
    where
        F: FnMut(&[T]) -> Option<T>,
    {
        self.send_now();
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
                                node_errors.push(NodeError {
                                    node_id: nr.node_id,
                                    cause: s.clone(),
                                });
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
