use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};

use crate::error::Error;
use crate::proto::gorums::Message;

/// Internal: distinguishes one-shot (request/response) from streaming
/// (correctable) pending calls.
enum PendingCall {
    Response(oneshot::Sender<Result<Message, Error>>),
    Stream(mpsc::UnboundedSender<Result<Message, Error>>),
}

/// Shared registry of in-flight two-way calls, keyed by sequence number.
///
/// The router is owned by the `Node` and shared into each `NodeChannel` so
/// it survives stream replacement: when a stream is torn down all pending
/// callers are cancelled via `cancel_all` and the registry is ready for the
/// next stream.
#[derive(Clone, Default)]
pub(crate) struct Router {
    inner: Arc<Mutex<HashMap<u64, PendingCall>>>,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store an existing sender (used by `sender_loop` after the request is
    /// dequeued but before the bytes go on the wire).
    pub fn register_with_sender(&self, seq: u64, tx: oneshot::Sender<Result<Message, Error>>) {
        self.inner
            .lock()
            .unwrap()
            .insert(seq, PendingCall::Response(tx));
    }

    /// Pre-allocate a streaming receiver; the sender is stored in the
    /// registry.  Used by `correctable_call` before enqueueing the request.
    pub fn register_stream(&self, seq: u64) -> mpsc::UnboundedReceiver<Result<Message, Error>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner
            .lock()
            .unwrap()
            .insert(seq, PendingCall::Stream(tx));
        rx
    }

    /// Deliver a response to the matching caller.
    ///
    /// For one-shot calls the entry is removed on delivery.
    /// For streaming calls the entry is kept alive until [`close_stream`] is
    /// called or the receiver is dropped.
    pub fn deliver(&self, seq: u64, result: Result<Message, Error>) {
        let mut map = self.inner.lock().unwrap();
        if let Some(PendingCall::Stream(tx)) = map.get(&seq) {
            if tx.send(result).is_err() {
                // Receiver dropped; clean up.
                map.remove(&seq);
            }
            return;
        }
        // One-shot path (Response or no entry).
        if let Some(PendingCall::Response(tx)) = map.remove(&seq) {
            let _ = tx.send(result);
        }
    }

    /// Signal end-of-stream for a correctable call.  Dropping the sender
    /// closes the receiver on the client side.
    pub fn close_stream(&self, seq: u64) {
        self.inner.lock().unwrap().remove(&seq);
    }

    /// Cancel every pending call with the given error.
    pub fn cancel_all(&self, err: Error) {
        let entries: Vec<_> = self.inner.lock().unwrap().drain().map(|(_, v)| v).collect();
        for entry in entries {
            match entry {
                PendingCall::Response(tx) => {
                    let _ = tx.send(Err(err.clone()));
                }
                PendingCall::Stream(tx) => {
                    let _ = tx.send(Err(err.clone()));
                }
            }
        }
    }
}
