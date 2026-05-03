use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use prost::Message as ProstMessage;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Streaming;
use tonic::transport::Channel as TonicChannel;

use crate::error::Error;
use crate::node::NodeStatus;
use crate::proto::gorums::{Message, gorums_client::GorumsClient};
use crate::router::Router;

/// Magic status code sent by the server to signal end-of-stream for a
/// correctable call.  Not a valid gRPC status code (valid range: 0–16).
pub(crate) const STATUS_STREAM_DONE: u32 = 0xFFFF_FFFE;

// ── Request types ────────────────────────────────────────────────────────────

/// A request enqueued on a node's send channel.
pub(crate) struct OutboundRequest {
    /// The wire envelope to send.
    pub msg: Message,
    /// For two-way calls: the response comes back via this sender.
    /// Caller holds the matching `Receiver`. `None` for one-way calls.
    pub response_tx: Option<oneshot::Sender<Result<Message, Error>>>,
    /// For blocking one-way calls: notified when the bytes leave the
    /// send buffer. `None` for fire-and-forget.
    pub send_ack: Option<oneshot::Sender<Result<(), Error>>>,
}

// ── Channel ──────────────────────────────────────────────────────────────────

/// Per-node logical channel. Owns the send queue and the background
/// sender/receiver tasks that manage the gRPC stream.
pub(crate) struct NodeChannel {
    send_tx: mpsc::UnboundedSender<OutboundRequest>,
    seq: Arc<AtomicU64>,
    /// Shared router — also held by the background `run_node` task.
    router: Router,
    /// Current connectivity status; `run_node` writes, callers read/subscribe.
    status_tx: Arc<watch::Sender<NodeStatus>>,
}

impl NodeChannel {
    /// Start the background task for `node_id` at `addr` and return the
    /// channel handle.
    pub fn connect(node_id: u32, addr: String, router: Router) -> Self {
        let (send_tx, send_rx) = mpsc::unbounded_channel();
        let seq = Arc::new(AtomicU64::new(0));
        let (status_tx, _status_rx) = watch::channel(NodeStatus::Connecting);
        let status_tx = Arc::new(status_tx);

        let router_for_task = router.clone();
        let status_for_task = Arc::clone(&status_tx);
        tokio::spawn(run_node(
            node_id,
            addr,
            send_rx,
            router_for_task,
            status_for_task,
        ));

        NodeChannel {
            send_tx,
            seq,
            router,
            status_tx,
        }
    }

    /// Enqueue a request. Returns `Err(Error::NodeClosed)` if the task is gone.
    pub fn enqueue(&self, req: OutboundRequest) -> Result<(), Error> {
        self.send_tx.send(req).map_err(|_| Error::NodeClosed)
    }

    /// Allocate the next sequence number for this node's stream.
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Pre-register a streaming (correctable) call and return the receiver.
    /// Must be called before [`enqueue`] so the router entry exists before
    /// any response could arrive.
    pub fn register_stream(&self, seq: u64) -> mpsc::UnboundedReceiver<Result<Message, Error>> {
        self.router.register_stream(seq)
    }

    /// Return the node's current connectivity status without blocking.
    pub fn current_status(&self) -> NodeStatus {
        *self.status_tx.borrow()
    }

    /// Return a new [`watch::Receiver`] for this node's status stream.
    /// The receiver is immediately initialised with the current status.
    pub fn subscribe_status(&self) -> watch::Receiver<NodeStatus> {
        self.status_tx.subscribe()
    }
}

// ── Background tasks ─────────────────────────────────────────────────────────

async fn run_node(
    node_id: u32,
    addr: String,
    mut send_rx: mpsc::UnboundedReceiver<OutboundRequest>,
    router: Router,
    status_tx: Arc<watch::Sender<NodeStatus>>,
) {
    let mut first_attempt = true;

    loop {
        // Emit Connecting on first attempt, Reconnecting on subsequent ones.
        let connecting_status = if first_attempt {
            NodeStatus::Connecting
        } else {
            NodeStatus::Reconnecting
        };
        let _ = status_tx.send(connecting_status);

        match try_connect(&addr).await {
            Err(e) => {
                eprintln!("[quorums] node {node_id}: connect error: {e}");
                let _ = status_tx.send(NodeStatus::Unreachable);
                first_attempt = false;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok(mut client) => {
                let (wire_tx, wire_rx) = mpsc::unbounded_channel::<Message>();
                let outbound = UnboundedReceiverStream::new(wire_rx);

                match client.node_stream(outbound).await {
                    Err(e) => {
                        eprintln!("[quorums] node {node_id}: stream open error: {e}");
                        let _ = status_tx.send(NodeStatus::Unreachable);
                        first_attempt = false;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    Ok(response) => {
                        let _ = status_tx.send(NodeStatus::Connected);
                        first_attempt = false;

                        let inbound: Streaming<Message> = response.into_inner();

                        let router2 = router.clone();
                        let recv_handle = tokio::spawn(receive_loop(node_id, inbound, router2));

                        let stream_alive = sender_loop(&mut send_rx, &wire_tx, &router).await;

                        recv_handle.abort();

                        if !stream_alive {
                            // Clean shutdown: send_rx was dropped.
                            let _ = status_tx.send(NodeStatus::Closed);
                            return;
                        }

                        // Stream died — reconnect.
                        let _ = status_tx.send(NodeStatus::Reconnecting);
                        router.cancel_all(Error::StreamDown);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
    }
}

async fn try_connect(addr: &str) -> Result<GorumsClient<TonicChannel>, tonic::transport::Error> {
    let uri = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    };
    let ep = tonic::transport::Endpoint::from_shared(uri)
        .expect("invalid endpoint address")
        .timeout(std::time::Duration::from_secs(5));
    let ch = ep.connect().await?;
    Ok(GorumsClient::new(ch))
}

/// Read responses from the inbound half of the stream and deliver them to
/// waiting callers via the router.
async fn receive_loop(node_id: u32, mut inbound: Streaming<Message>, router: Router) {
    loop {
        match inbound.message().await {
            Ok(Some(msg)) => {
                let seq = msg.message_seq_no;

                if msg.status_code == STATUS_STREAM_DONE {
                    // End-of-stream for a correctable call: close the channel.
                    router.close_stream(seq);
                } else {
                    let result = if msg.status_code == 0 {
                        Ok(msg)
                    } else {
                        Err(Error::Transport(tonic::Status::new(
                            tonic::Code::from_i32(msg.status_code as i32),
                            msg.status_message.clone(),
                        )))
                    };
                    router.deliver(seq, result);
                }
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("[quorums] node {node_id}: recv error: {e}");
                router.cancel_all(Error::Transport(e));
                break;
            }
        }
    }
}

/// Drain `send_rx` and write messages to `wire_tx`.
///
/// Returns `true` if the wire channel closed (stream died — reconnect).
/// Returns `false` if `send_rx` closed (clean shutdown).
async fn sender_loop(
    send_rx: &mut mpsc::UnboundedReceiver<OutboundRequest>,
    wire_tx: &mpsc::UnboundedSender<Message>,
    router: &Router,
) -> bool {
    loop {
        let req = match send_rx.recv().await {
            None => return false,
            Some(r) => r,
        };

        let seq = req.msg.message_seq_no;

        if let Some(resp_tx) = req.response_tx {
            router.register_with_sender(seq, resp_tx);
        }

        match wire_tx.send(req.msg) {
            Err(_) => {
                if let Some(ack) = req.send_ack {
                    let _ = ack.send(Err(Error::StreamDown));
                }
                return true;
            }
            Ok(()) => {
                if let Some(ack) = req.send_ack {
                    let _ = ack.send(Ok(()));
                }
            }
        }
    }
}

// ── Codec helpers ─────────────────────────────────────────────────────────────

/// Encode a prost `Message` to a `Vec<u8>` payload.
pub(crate) fn encode_payload<M: ProstMessage>(msg: &M) -> Vec<u8> {
    msg.encode_to_vec()
}

/// Decode the payload of a wire `Message` into type `M`.
pub(crate) fn decode_payload<M: ProstMessage + Default>(
    wire: &Message,
) -> Result<M, prost::DecodeError> {
    M::decode(wire.payload.as_slice())
}
