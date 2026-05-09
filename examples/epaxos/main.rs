//! EPaxos normal-path example using the quorums Rust gorums framework.
//!
//! # What is EPaxos?
//!
//! EPaxos (Egalitarian Paxos) is a leaderless consensus protocol where any
//! replica can propose commands without a designated leader.  Commands that
//! commute (touch different keys) can be committed independently on the fast
//! path without coordination.  Commands that conflict (touch overlapping keys)
//! are ordered by a dependency graph (deps) and sequence number (seq), with
//! execution order resolved via SCC decomposition.
//!
//! # Three phases
//!
//! 1. **PreAccept** — the proposer sends its command with initial (seq, deps)
//!    attrs.  Each peer checks its own conflict map and replies PREACCEPTED_EQ
//!    if its local attrs match, or PREACCEPTED if they differ.
//!
//!    Fast path: all `N-1` peers reply PREACCEPTED_EQ → commit immediately.
//!    Slow path: any peer replies PREACCEPTED → proceed to Accept.
//!
//! 2. **Accept** — slow path only.  The leader merges attrs from all replies
//!    and fans out the final (seq, deps) to a majority of peers.
//!
//! 3. **Commit** — the leader applies the command locally and multicasts a
//!    Commit message to peers.  No reply needed.
//!
//! # What this example covers
//!
//! - 3-replica cluster started in-process.
//! - Leader (replica 0) proposes PUT commands.
//! - Both fast path (no conflicts) and slow path (injected conflict) are
//!   demonstrated.
//! - Multiple leaders (replicas 0 and 1) proposing concurrently.
//!
//! # What is omitted
//!
//! - Recovery (explicit prepare / Paxos-Accept recovery after crash).
//! - Execution ordering via strongly-connected component decomposition.
//! - Thrifty mode, leader-local thresholding, or batching.

mod replica;

mod pb {
    tonic::include_proto!("epaxos");
}

use pb::{Command, Operation};

use std::time::Duration;

use quorums::{Configuration, Manager, Server};

use replica::Replica;

// ── Command helpers ───────────────────────────────────────────────────────────

fn put(key: impl Into<String>, val: impl Into<String>) -> Command {
    Command {
        op: Operation::Put as i32,
        key: key.into(),
        value: val.into(),
    }
}

#[allow(dead_code)]
fn get(key: impl Into<String>) -> Command {
    Command {
        op: Operation::Get as i32,
        key: key.into(),
        value: String::new(),
    }
}

// ── Node spawner ──────────────────────────────────────────────────────────────

/// Spawn a replica server on `port`.  Returns the `Replica` handle so the
/// caller can drive proposals and inspect state.
fn spawn_node(port: u16, id: usize, n: usize) -> Replica {
    let replica = Replica::new(id, n);
    let replica_ret = replica.clone();
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    tokio::spawn(async move {
        let mut server = Server::new();
        replica.register(&mut server);
        server.serve(addr).await.expect("server error");
    });
    replica_ret
}

// ── Cluster setup ─────────────────────────────────────────────────────────────

/// Start `n` servers on consecutive ports starting from `base_port`.
///
/// Returns:
/// - `replicas`: one `Replica` handle per server.
/// - `mgr`: the connection manager.
/// - `all_cfg`: configuration containing all N nodes.
/// - `peers_cfgs`: per-replica peer configurations (all nodes except self).
async fn setup(
    n: usize,
    base_port: u16,
) -> (Vec<Replica>, Manager, Configuration, Vec<Configuration>) {
    let ports: Vec<u16> = (0..n).map(|i| base_port + i as u16).collect();

    // Spawn servers.
    let replicas: Vec<Replica> = ports
        .iter()
        .enumerate()
        .map(|(i, &port)| spawn_node(port, i, n))
        .collect();

    // Give servers time to bind.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connect via Manager.
    let mut mgr = Manager::new();
    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{p}")).collect();
    let addr_refs: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
    let all_cfg = mgr.add_node_list(&addr_refs).expect("failed to add nodes");

    // Per-replica peers config = all nodes except self.
    // Node IDs are assigned sequentially starting from 1 by add_node_list.
    let peers_cfgs: Vec<Configuration> = (0..n)
        .map(|i| {
            let self_id = (i + 1) as u32; // IDs are 1-based
            all_cfg.without_nodes(&[self_id])
        })
        .collect();

    (replicas, mgr, all_cfg, peers_cfgs)
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const BASE_PORT: u16 = 19020;
    const N: usize = 3;

    println!("=== EPaxos Normal-Path Example ===");
    println!("Starting {N}-replica cluster on ports {BASE_PORT}..{}", BASE_PORT + N as u16 - 1);

    let (replicas, _mgr, _all_cfg, peers_cfgs) = setup(N, BASE_PORT).await;

    let leader = &replicas[0];
    let peers = &peers_cfgs[0];

    // Propose 3 PUT commands from replica 0.
    let commands = vec![
        ("hello", "world"),
        ("foo", "bar"),
        ("baz", "qux"),
    ];

    for (key, val) in &commands {
        let cmds = vec![put(*key, *val)];
        let info = leader.propose(cmds, peers, &_all_cfg).await?;
        println!(
            "  committed slot={} seq={} fast_path={} key={}",
            info.slot, info.seq, info.fast_path, key
        );
    }

    // Allow peers time to receive commit messages.
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n=== Store state on all replicas ===");
    for (i, replica) in replicas.iter().enumerate() {
        let store = replica.store_snapshot();
        println!("  replica {i}: {store:?}");
    }

    println!("\nDone.");
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_fast_path() {
        const BASE: u16 = 19030;
        let (replicas, _mgr, all_cfg, peers_cfgs) = setup(3, BASE).await;

        let leader = &replicas[0];
        let peers = &peers_cfgs[0];

        let cmds = vec![put("hello", "world")];
        let info = leader.propose(cmds, peers, &all_cfg).await.unwrap();

        assert!(
            info.fast_path,
            "expected fast path but got slow path: seq={} deps={:?}",
            info.seq,
            info.deps
        );

        // Give peers time to receive commit.
        tokio::time::sleep(Duration::from_millis(100)).await;

        for (i, replica) in replicas.iter().enumerate() {
            let val = replica.get_store_value("hello");
            assert_eq!(
                val.as_deref(),
                Some("world"),
                "replica {i} missing 'hello' after commit"
            );
        }
    }

    #[tokio::test]
    async fn test_sequential_commands() {
        const BASE: u16 = 19033;
        let (replicas, _mgr, all_cfg, peers_cfgs) = setup(3, BASE).await;

        let leader = &replicas[0];
        let peers = &peers_cfgs[0];

        let keys: Vec<String> = (0..5).map(|i| format!("k{i}")).collect();
        for key in &keys {
            let cmds = vec![put(key.as_str(), "val")];
            let info = leader.propose(cmds, peers, &all_cfg).await.unwrap();
            assert!(info.slot >= 0, "slot should be non-negative");
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let store = leader.store_snapshot();
        for key in &keys {
            assert!(
                store.contains_key(key.as_str()),
                "key {key} not found in leader store after commit"
            );
        }
    }

    #[tokio::test]
    async fn test_slow_path() {
        const BASE: u16 = 19036;
        let (replicas, _mgr, all_cfg, peers_cfgs) = setup(3, BASE).await;

        let leader = &replicas[0];
        let peers = &peers_cfgs[0];

        // Inject a conflict on peer replica 1: as if peer 1 had previously
        // handled a command for key "x" at seq=5, slot=99 of replica 1.
        // When the leader sends PreAccept{seq=1, deps=[-1,-1,-1]}, peer 1
        // computes local attrs: seq=max(1,5+1)=6, deps[1]=99, which differs
        // from the leader's proposed attrs → peer replies PREACCEPTED → slow path.
        replicas[1].inject_conflict("x", 5, 1, 99);

        let cmds = vec![put("x", "slow")];
        let info = leader.propose(cmds, peers, &all_cfg).await.unwrap();

        assert!(
            !info.fast_path,
            "expected slow path but got fast path: seq={} deps={:?}",
            info.seq,
            info.deps
        );

        // The slow path must still commit correctly.
        tokio::time::sleep(Duration::from_millis(100)).await;

        for (i, replica) in replicas.iter().enumerate() {
            let val = replica.get_store_value("x");
            assert_eq!(
                val.as_deref(),
                Some("slow"),
                "replica {i} missing 'x' after slow-path commit"
            );
        }
    }

    #[tokio::test]
    async fn test_multiple_leaders() {
        const BASE: u16 = 19039;
        let (replicas, _mgr, all_cfg, peers_cfgs) = setup(3, BASE).await;

        // Replica 0 and replica 1 propose concurrently.
        let leader0 = replicas[0].clone();
        let leader1 = replicas[1].clone();
        let peers0 = peers_cfgs[0].clone();
        let peers1 = peers_cfgs[1].clone();
        let all0 = all_cfg.clone();
        let all1 = all_cfg.clone();

        let h0 = tokio::spawn(async move {
            let cmds = vec![put("leader0_key", "v0")];
            leader0.propose(cmds, &peers0, &all0).await
        });

        let h1 = tokio::spawn(async move {
            let cmds = vec![put("leader1_key", "v1")];
            leader1.propose(cmds, &peers1, &all1).await
        });

        let (r0, r1) = tokio::join!(h0, h1);
        r0.expect("join failed").expect("leader0 propose failed");
        r1.expect("join failed").expect("leader1 propose failed");

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Both keys should be committed somewhere.
        let store = replicas[0].store_snapshot();
        assert!(
            store.contains_key("leader0_key"),
            "leader0_key not in replica0 store"
        );
        // leader1_key may only be in replicas 1 and 2 initially if the commit
        // multicast hasn't reached replica 0 yet; check replica 1's store.
        let store1 = replicas[1].store_snapshot();
        assert!(
            store1.contains_key("leader1_key"),
            "leader1_key not in replica1 store"
        );
    }
}
