use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, NodeError, QuorumCallCause, QuorumCallError};

/// A single response from one node in an ordered quorum call, tagged with
/// the node's **position** in the configuration (0-based index).
#[derive(Debug)]
pub struct OrderedNodeResponse<T> {
    /// Zero-based index of this node in the [`Configuration`][crate::config::Configuration]
    /// that the call was dispatched to.
    pub position: usize,
    pub node_id: u32,
    pub result: Result<T, Error>,
}

/// Collects responses from an [`ordered_quorum_call`][crate::call_types::ordered_quorum_call].
///
/// Identical to [`Responses<T>`][crate::responses::Responses] except that each
/// response is tagged with its node's **position** in the configuration, and
/// the terminal methods exploit that order:
///
/// - `threshold(k)` / `majority()` / `all()` wait for k successes and return
///   the value from the **lowest-position** node among those that responded.
/// - [`quorum`][Self::quorum] calls the predicate with a fixed-length slice
///   `slots: &[Option<T>]` (one slot per configuration node, `None` until that
///   node responds) so the predicate can reason about *which* node replied.
///
/// # Terminal methods
///
/// | Method | Returns when |
/// |--------|-------------|
/// | [`first`][Self::first] | any one node replies |
/// | [`majority`][Self::majority] | ⌈(n+1)/2⌉ nodes reply |
/// | [`all`][Self::all] | all n nodes reply |
/// | [`threshold`][Self::threshold] | at least k nodes reply |
/// | [`quorum`][Self::quorum] | user predicate returns `Some` |
#[must_use = "call a terminal method (.majority(), .all(), .threshold(k), .quorum(f)) to \
              dispatch the fan-out and collect results"]
pub struct OrderedResponses<T> {
    pub(crate) rx: mpsc::Receiver<OrderedNodeResponse<T>>,
    pub(crate) size: usize,
    pub(crate) cancel: CancellationToken,
    /// Deferred fan-out.  `None` once `send_now()` has been called.
    pub(crate) launch: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl<T> OrderedResponses<T> {
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

    /// Wait for the first successful reply; return the value from that node.
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

    /// Wait for at least `k` successful replies, then return the value from
    /// the **lowest-position** node among those that responded.
    ///
    /// Returns [`Error::QuorumCall`] with cause `Incomplete` if the channel
    /// closes before `k` successes arrive, or [`Error::Cancelled`] if the
    /// context's token fires.
    pub async fn threshold(mut self, k: usize) -> Result<T, Error> {
        self.send_now();
        // Track by position: Some(val) once that node has replied successfully.
        let mut slots: Vec<Option<T>> = (0..self.size).map(|_| None).collect();
        let mut count = 0usize;
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
                                slots[nr.position] = Some(val);
                                count += 1;
                                if count >= k {
                                    // Return the value at the lowest position.
                                    for slot in &mut slots {
                                        if let Some(v) = slot.take() {
                                            return Ok(v);
                                        }
                                    }
                                }
                            }
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

    /// Drive aggregation with a user-provided ordered quorum function.
    ///
    /// After each successful reply, `f` is called with `slots: &[Option<T>]` —
    /// a fixed-length slice with one entry per configuration node.  `slots[i]`
    /// is `None` while node `i` has not yet replied and `Some(val)` once it
    /// has.  When `f` returns `Some(value)` the call resolves; returning `None`
    /// continues collecting.
    ///
    /// Unlike [`Responses::quorum`][crate::responses::Responses::quorum], this
    /// predicate sees *which* node produced each value, enabling position-aware
    /// consensus logic:
    ///
    /// ```ignore
    /// // Accept once the primary (position 0) has replied.
    /// ordered_quorum_call(&ctx, &req, METHOD)
    ///     .await?
    ///     .quorum(|slots| slots[0].clone())
    ///     .await?;
    /// ```
    ///
    /// Requires `T: Clone` so values can be passed to the predicate by shared
    /// reference while keeping them in the slot for later predicate invocations.
    pub async fn quorum<F>(mut self, mut f: F) -> Result<T, Error>
    where
        T: Clone,
        F: FnMut(&[Option<T>]) -> Option<T>,
    {
        self.send_now();
        let mut slots: Vec<Option<T>> = (0..self.size).map(|_| None).collect();
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
                                slots[nr.position] = Some(val);
                                if let Some(result) = f(&slots) {
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
