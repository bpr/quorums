use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use crate::channel::{OutboundRequest, decode_payload, encode_payload};
use crate::config::ConfigContext;
use crate::correctable::Correctable;
use crate::error::{Error, NodeError, QuorumCallCause, QuorumCallError};
use crate::interceptor::{self, CallInfo};
use crate::method::{
    CorrectableMethod, MulticastMethod, OrderedQuorumCallMethod, QuorumCallMethod, RpcCallMethod,
    UnicastMethod,
};
use crate::node::NodeContext;
use crate::ordered_responses::{OrderedNodeResponse, OrderedResponses};
use crate::proto::gorums::Message;
use crate::responses::{NodeResponse, Responses};

// ── Internal helpers ─────────────────────────────────────────────────────────

fn build_wire_message(
    seq: u64,
    method: &str,
    payload: Vec<u8>,
    metadata: HashMap<String, String>,
) -> Message {
    Message {
        message_seq_no: seq,
        method: method.to_string(),
        status_code: 0,
        status_message: String::new(),
        payload,
        metadata,
    }
}

/// Convert the context's ordered metadata Vec into the proto HashMap.
/// Last entry wins when a key appears more than once.
fn metadata_map(entries: &[(String, String)]) -> HashMap<String, String> {
    entries
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

// ── RPC: single-node two-way call ────────────────────────────────────────────

/// Send a request to a single node and await its response.
///
/// Interceptors registered on `ctx` run before the request is dispatched.
/// Returns [`Error::Cancelled`] if the context's token fires before a response
/// arrives.
pub async fn rpc_call<M: RpcCallMethod>(
    ctx: &NodeContext,
    req: &M::Req,
    _m: M,
) -> Result<M::Resp, Error> {
    interceptor::run(
        &ctx.interceptors,
        CallInfo {
            method: M::PATH.to_string(),
            node_ids: vec![ctx.node.id()],
            metadata: ctx.metadata.clone(),
        },
    )
    .await?;

    let ch = ctx.node.channel();
    let seq = ch.next_seq();
    let payload = encode_payload(req);
    let wire = build_wire_message(seq, M::PATH, payload, metadata_map(&ctx.metadata));

    let (resp_tx, resp_rx) = oneshot::channel();

    ch.enqueue(OutboundRequest {
        msg: wire,
        response_tx: Some(resp_tx),
        send_ack: None,
    })?;

    let wire_resp = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => return Err(Error::Cancelled),
        r = resp_rx => r.map_err(|_| Error::NodeClosed)??,
    };
    Ok(decode_payload::<M::Resp>(&wire_resp)?)
}

// ── Unicast: single-node one-way call ────────────────────────────────────────

/// Send a request to a single node.  No response is expected.
///
/// Interceptors registered on `ctx` run before the request is dispatched.
pub async fn unicast<M: UnicastMethod>(
    ctx: &NodeContext,
    req: &M::Req,
    _m: M,
) -> Result<(), Error> {
    interceptor::run(
        &ctx.interceptors,
        CallInfo {
            method: M::PATH.to_string(),
            node_ids: vec![ctx.node.id()],
            metadata: ctx.metadata.clone(),
        },
    )
    .await?;

    let ch = ctx.node.channel();
    let seq = ch.next_seq();
    let payload = encode_payload(req);
    let wire = build_wire_message(seq, M::PATH, payload, metadata_map(&ctx.metadata));

    let (ack_tx, ack_rx) = oneshot::channel();

    ch.enqueue(OutboundRequest {
        msg: wire,
        response_tx: None,
        send_ack: Some(ack_tx),
    })?;

    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(Error::Cancelled),
        r = ack_rx => r.map_err(|_| Error::NodeClosed)?,
    }
}

// ── Multicast: fan-out one-way to all nodes in a configuration ───────────────

/// Send the same request to every node in the configuration.  No responses
/// are collected.
///
/// Interceptors registered on `ctx` run once before any node is contacted.
pub async fn multicast<M: MulticastMethod>(
    ctx: &ConfigContext,
    req: &M::Req,
    _m: M,
) -> Result<(), Error> {
    interceptor::run(
        &ctx.interceptors,
        CallInfo {
            method: M::PATH.to_string(),
            node_ids: ctx.config.node_ids(),
            metadata: ctx.metadata.clone(),
        },
    )
    .await?;

    let nodes = ctx.config.nodes();
    let n = nodes.len();
    let payload = encode_payload(req);
    let cancel = ctx.cancel.clone();
    let meta = metadata_map(&ctx.metadata);

    let (ack_tx, mut ack_rx) = mpsc::channel::<(u32, Result<(), Error>)>(n);

    for node in nodes {
        let node = node.clone();
        let payload = payload.clone();
        let ack_tx = ack_tx.clone();
        let node_id = node.id();
        let meta = meta.clone();

        tokio::spawn(async move {
            let (tx, rx) = oneshot::channel();
            let ch = node.channel();
            let seq = ch.next_seq();
            let wire = build_wire_message(seq, M::PATH, payload, meta);

            let result = match ch.enqueue(OutboundRequest {
                msg: wire,
                response_tx: None,
                send_ack: Some(tx),
            }) {
                Err(e) => Err(e),
                Ok(()) => rx.await.map_err(|_| Error::NodeClosed).and_then(|r| r),
            };

            let _ = ack_tx.send((node_id, result)).await;
        });
    }
    drop(ack_tx);

    let mut node_errors: Vec<NodeError> = Vec::new();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            maybe = ack_rx.recv() => {
                match maybe {
                    None => break,
                    Some((node_id, Err(Error::Transport(s)))) => {
                        node_errors.push(NodeError { node_id, cause: s });
                    }
                    Some(_) => {}
                }
            }
        }
    }

    if node_errors.is_empty() {
        Ok(())
    } else {
        Err(Error::QuorumCall(QuorumCallError {
            cause: QuorumCallCause::SendFailure,
            node_errors,
        }))
    }
}

// ── QuorumCall: fan-out two-way to all nodes, return Responses<T> ────────────

/// Send a request to every node in the configuration and return a
/// [`Responses<Resp>`] handle from which callers retrieve the aggregated
/// result using a terminal method.
///
/// Interceptors registered on `ctx` run once before any node is contacted.
/// Returns `Err` immediately if an interceptor rejects the call.
///
/// # Example
/// ```ignore
/// let resp = quorum_call(&cfg.context(), &req, "/svc/ReadQC")
///     .await?
///     .majority()
///     .await?;
/// ```
pub async fn quorum_call<M: QuorumCallMethod>(
    ctx: &ConfigContext,
    req: &M::Req,
    _m: M,
) -> Result<Responses<M::Resp>, Error> {
    interceptor::run(
        &ctx.interceptors,
        CallInfo {
            method: M::PATH.to_string(),
            node_ids: ctx.config.node_ids(),
            metadata: ctx.metadata.clone(),
        },
    )
    .await?;

    let nodes = ctx.config.nodes();
    let n = nodes.len();
    let payload = encode_payload(req);
    let meta = metadata_map(&ctx.metadata);

    let (result_tx, result_rx) = mpsc::channel::<NodeResponse<M::Resp>>(n);

    for node in nodes {
        let node = node.clone();
        let payload = payload.clone();
        let result_tx = result_tx.clone();
        let node_id = node.id();
        let meta = meta.clone();

        tokio::spawn(async move {
            let result = send_twoway(&node, M::PATH, payload, meta).await;
            let node_resp = match result {
                Err(e) => NodeResponse {
                    node_id,
                    result: Err(e),
                },
                Ok(wire_msg) => match decode_payload::<M::Resp>(&wire_msg) {
                    Err(e) => NodeResponse {
                        node_id,
                        result: Err(e.into()),
                    },
                    Ok(val) => NodeResponse {
                        node_id,
                        result: Ok(val),
                    },
                },
            };
            let _ = result_tx.send(node_resp).await;
        });
    }

    Ok(Responses {
        rx: result_rx,
        size: n,
        cancel: ctx.cancel.clone(),
    })
}

// ── CorrectableCall: fan-out streaming to all nodes ───────────────────────────

/// Send a request to every node in the configuration and return a
/// [`Correctable<Resp>`] handle from which callers read incremental updates.
///
/// Interceptors registered on `ctx` run once before any node is contacted.
/// Returns `Err` immediately if an interceptor rejects the call.
pub async fn correctable_call<M: CorrectableMethod>(
    ctx: &ConfigContext,
    req: &M::Req,
    _m: M,
) -> Result<Correctable<M::Resp>, Error> {
    interceptor::run(
        &ctx.interceptors,
        CallInfo {
            method: M::PATH.to_string(),
            node_ids: ctx.config.node_ids(),
            metadata: ctx.metadata.clone(),
        },
    )
    .await?;

    let nodes = ctx.config.nodes();
    let n = nodes.len();
    let payload = encode_payload(req);
    let meta = metadata_map(&ctx.metadata);

    let (result_tx, result_rx) = mpsc::unbounded_channel::<NodeResponse<M::Resp>>();

    for node in nodes {
        let node = node.clone();
        let payload = payload.clone();
        let result_tx = result_tx.clone();
        let node_id = node.id();
        let meta = meta.clone();

        tokio::spawn(async move {
            let ch = node.channel();
            let seq = ch.next_seq();
            let wire = build_wire_message(seq, M::PATH, payload, meta);

            let mut stream_rx = ch.register_stream(seq);

            if ch
                .enqueue(OutboundRequest {
                    msg: wire,
                    response_tx: None,
                    send_ack: None,
                })
                .is_err()
            {
                let _ = result_tx.send(NodeResponse {
                    node_id,
                    result: Err(Error::NodeClosed),
                });
                return;
            }

            while let Some(wire_result) = stream_rx.recv().await {
                let node_resp = match wire_result {
                    Err(e) => NodeResponse {
                        node_id,
                        result: Err(e),
                    },
                    Ok(msg) => match decode_payload::<M::Resp>(&msg) {
                        Err(e) => NodeResponse {
                            node_id,
                            result: Err(e.into()),
                        },
                        Ok(val) => NodeResponse {
                            node_id,
                            result: Ok(val),
                        },
                    },
                };
                let _ = result_tx.send(node_resp);
            }
        });
    }

    Ok(Correctable {
        rx: result_rx,
        size: n,
        cancel: ctx.cancel.clone(),
    })
}

// ── OrderedQuorumCall: fan-out two-way, responses tagged with position ────────

/// Send a request to every node in the configuration and return an
/// [`OrderedResponses<Resp>`] handle.
///
/// Identical to [`quorum_call`] except each response is tagged with the node's
/// **position** (0-based index) in the configuration.  Terminal methods exploit
/// this to provide deterministic, position-aware aggregation:
///
/// - `threshold(k)` / `majority()` / `all()` return the value from the
///   **lowest-position** node among those that have responded.
/// - `quorum(f)` calls `f` with a `&[Option<Resp>]` slice — `slots[i]` is
///   `Some` once node `i` has replied, `None` until then — letting the
///   predicate reason about *which* node produced each value.
///
/// # Example
/// ```ignore
/// // Accept once the primary (position 0) has replied.
/// let resp = ordered_quorum_call(&ctx, &req, METHOD)
///     .await?
///     .quorum(|slots| slots[0].clone())
///     .await?;
/// ```
pub async fn ordered_quorum_call<M: OrderedQuorumCallMethod>(
    ctx: &ConfigContext,
    req: &M::Req,
    _m: M,
) -> Result<OrderedResponses<M::Resp>, Error> {
    interceptor::run(
        &ctx.interceptors,
        CallInfo {
            method: M::PATH.to_string(),
            node_ids: ctx.config.node_ids(),
            metadata: ctx.metadata.clone(),
        },
    )
    .await?;

    let nodes = ctx.config.nodes();
    let n = nodes.len();
    let payload = encode_payload(req);
    let meta = metadata_map(&ctx.metadata);

    let (result_tx, result_rx) = mpsc::channel::<OrderedNodeResponse<M::Resp>>(n);

    for (position, node) in nodes.iter().enumerate() {
        let node = node.clone();
        let payload = payload.clone();
        let result_tx = result_tx.clone();
        let node_id = node.id();
        let meta = meta.clone();

        tokio::spawn(async move {
            let result = send_twoway(&node, M::PATH, payload, meta).await;
            let node_resp = match result {
                Err(e) => OrderedNodeResponse {
                    position,
                    node_id,
                    result: Err(e),
                },
                Ok(wire_msg) => match decode_payload::<M::Resp>(&wire_msg) {
                    Err(e) => OrderedNodeResponse {
                        position,
                        node_id,
                        result: Err(e.into()),
                    },
                    Ok(val) => OrderedNodeResponse {
                        position,
                        node_id,
                        result: Ok(val),
                    },
                },
            };
            let _ = result_tx.send(node_resp).await;
        });
    }

    Ok(OrderedResponses {
        rx: result_rx,
        size: n,
        cancel: ctx.cancel.clone(),
    })
}

/// Send one two-way request to `node` and await the wire-level response.
async fn send_twoway(
    node: &crate::node::Node,
    method: &str,
    payload: Vec<u8>,
    metadata: HashMap<String, String>,
) -> Result<Message, Error> {
    let ch = node.channel();
    let seq = ch.next_seq();
    let wire = build_wire_message(seq, method, payload, metadata);

    let (resp_tx, resp_rx) = oneshot::channel();

    ch.enqueue(OutboundRequest {
        msg: wire,
        response_tx: Some(resp_tx),
        send_ack: None,
    })?;

    resp_rx.await.map_err(|_| Error::NodeClosed)?
}
