//! Demonstrates a fault-tolerant in-memory key-value store using the quorums
//! gorums runtime.
//!
//! Three storage servers are started in-process.  A `Manager` connects to all
//! three.  The example then shows the major call types:
//!
//! - **Multicast** — write to all three nodes without waiting for a reply.
//! - **Quorum call** — read from all nodes and accept once a majority agrees.
//! - **Correctable call** — stream incremental responses from all nodes.
//! - **rpc_call** — single-node RPC (used here to read from one node only).
//!
//! Run with:
//!   cargo run --example key_value

// Pull in the prost-generated storage message types from the build script.
mod pb {
    tonic::include_proto!("storage");
}

// Pull in the quorums-build generated client/server wrappers.
use pb::{storage_client, storage_server};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tonic::Status;

use quorums::{
    CallInfo, CancellationToken, Manager, Server, ServerCtx, interceptor, server_interceptor,
};

// ── Storage server implementation ────────────────────────────────────────────

/// Simple in-memory store shared between all handler invocations.
#[derive(Clone, Default)]
struct KvStore {
    data: Arc<Mutex<HashMap<String, String>>>,
}

impl storage_server::StorageServer for KvStore {
    async fn read(
        &self,
        _ctx: ServerCtx,
        req: pb::ReadRequest,
    ) -> Result<Option<pb::ReadResponse>, Status> {
        let guard = self.data.lock().unwrap();
        match guard.get(&req.key) {
            Some(v) => Ok(Some(pb::ReadResponse {
                ok: true,
                value: v.clone(),
            })),
            None => Ok(Some(pb::ReadResponse {
                ok: false,
                value: String::new(),
            })),
        }
    }

    async fn write(&self, _ctx: ServerCtx, req: pb::WriteRequest) -> Result<Option<()>, Status> {
        let mut guard = self.data.lock().unwrap();
        println!("  [node] write {:?} = {:?}", req.key, req.value);
        guard.insert(req.key, req.value);
        Ok(None)
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Start a `KvStore` server on `port` in a background task.
fn spawn_node(port: u16, store: KvStore) {
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    tokio::spawn(async move {
        let mut server = Server::new().with_interceptor(server_interceptor(
            |info: quorums::ServerCallInfo| async move {
                if !info.method.contains("health") {
                    println!("  [server:{:?}] {}", info.peer_addr, info.method);
                }
                Ok(())
            },
        ));
        storage_server::register_storage(&mut server, Arc::new(store));
        server.serve(addr).await.expect("server error");
    });
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PORTS: [u16; 3] = [19001, 19002, 19003];

    // ── Start three in-process storage nodes ─────────────────────────────────
    println!("Starting 3 storage nodes on ports {:?}…", PORTS);
    for &port in &PORTS {
        spawn_node(port, KvStore::default());
    }
    // Give the servers a moment to bind.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Connect via the Manager ───────────────────────────────────────────────
    let mut mgr = Manager::new();
    let addrs: Vec<&str> = PORTS
        .iter()
        .map(|p| Box::leak(format!("127.0.0.1:{p}").into_boxed_str()) as &str)
        .collect();
    let cfg = mgr.add_node_list(&addrs)?;
    println!("Connected to {} nodes.\n", cfg.size());

    // A client-side logging interceptor that we'll attach to each call.
    let log = interceptor(|info: CallInfo| async move {
        println!("→ {} (nodes {:?})", info.method, info.node_ids);
        Ok(())
    });

    // ── Multicast write ───────────────────────────────────────────────────────
    // Write the same key to all nodes simultaneously.  No response is awaited.
    println!("=== Multicast write: set 'colour' = 'blue' on all nodes ===");
    storage_client::write(
        &cfg.context().with_interceptor(log.clone()),
        &pb::WriteRequest {
            key: "colour".into(),
            value: "blue".into(),
        },
    )
    .await?;

    // ── Quorum read — majority ────────────────────────────────────────────────
    // Fan-out to all nodes; accept once a majority (≥2 of 3) have replied.
    println!("\n=== Quorum read: read 'colour' — accept on majority ===");
    let resp = storage_client::read(
        &cfg.context().with_interceptor(log.clone()),
        &pb::ReadRequest {
            key: "colour".into(),
        },
    )
    .await?
    .majority()
    .await?;
    println!("  ok={}, value={:?}", resp.ok, resp.value);

    // ── Quorum read — custom predicate ────────────────────────────────────────
    // Use .quorum(f) for custom aggregation logic.  Here we require all three
    // nodes to agree on the same value.
    println!("\n=== Quorum read: require all 3 nodes to agree ===");
    let resp = storage_client::read(
        &cfg.context().with_interceptor(log.clone()),
        &pb::ReadRequest {
            key: "colour".into(),
        },
    )
    .await?
    .quorum(|oks| {
        // Accept once every reply has the same value.
        if oks.len() == 3 && oks.iter().all(|r| r.value == oks[0].value) {
            Some(oks[0].clone())
        } else {
            None
        }
    })
    .await?;
    println!("  all agreed: ok={}, value={:?}", resp.ok, resp.value);

    // ── Single-node rpc_call ──────────────────────────────────────────────────
    // Read from a single node directly.
    let node = cfg.nodes()[0].clone();
    println!("\n=== rpc_call: read from node {} only ===", node.id());
    let ctx = node.context().with_interceptor(log.clone());
    let resp = quorums::call_types::rpc_call::<pb::ReadRequest, pb::ReadResponse>(
        &ctx,
        &pb::ReadRequest {
            key: "colour".into(),
        },
        "/storage.Storage/Read",
    )
    .await?;
    println!("  ok={}, value={:?}", resp.ok, resp.value);

    // ── Per-call metadata ─────────────────────────────────────────────────────
    // Metadata key/value pairs are forwarded in-band with every message.
    // The server can retrieve them via `ctx.metadata_get("key")`.
    println!("\n=== Write with per-call metadata (trace-id) ===");
    storage_client::write(
        &cfg.context()
            .with_metadata("trace-id", "req-42")
            .with_metadata("user", "alice"),
        &pb::WriteRequest {
            key: "traced-key".into(),
            value: "traced-value".into(),
        },
    )
    .await?;
    println!("  write sent with trace-id=req-42");

    // ── Cancellation via CancellationToken ────────────────────────────────────
    println!("\n=== Cancelled read (token fired before reply) ===");
    let token = CancellationToken::new();
    token.cancel(); // cancel immediately
    let result = storage_client::read(
        &cfg.context().with_cancel(token),
        &pb::ReadRequest {
            key: "colour".into(),
        },
    )
    .await?
    .majority()
    .await;
    match result {
        Err(quorums::Error::Cancelled) => println!("  got expected Cancelled error"),
        other => println!("  unexpected result: {other:?}"),
    }

    println!("\nDone.");
    Ok(())
}
