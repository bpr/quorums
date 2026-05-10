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
//! - **Recovery** — Prepare → analyze COMMITTED / ACCEPTED / PREACCEPTED
//!   replies → TryPreAccept (fast) or Accept+Commit (slow).
//! - **SCC-based execution ordering** — Tarjan's algorithm resolves dependency
//!   cycles; within a cycle commands execute in ascending `seq` order.
//! - **Thrifty mode** — `propose(cmds, peers, thrifty: true)` sends Commit
//!   only to the ⌊N/2⌋ quorum peers that already hold PreAccept/Accept state,
//!   reducing network fan-out at the cost of lazy catch-up for non-participants.
//!
//! # What is omitted
//!
//! - Leader-local thresholding and batching.

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
        let info = leader.propose(cmds, peers, false).await?;
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
        let (replicas, _mgr, _all_cfg, peers_cfgs) = setup(3, BASE).await;

        let leader = &replicas[0];
        let peers = &peers_cfgs[0];

        let cmds = vec![put("hello", "world")];
        let info = leader.propose(cmds, peers, false).await.unwrap();

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
        let (replicas, _mgr, _all_cfg, peers_cfgs) = setup(3, BASE).await;

        let leader = &replicas[0];
        let peers = &peers_cfgs[0];

        let keys: Vec<String> = (0..5).map(|i| format!("k{i}")).collect();
        for key in &keys {
            let cmds = vec![put(key.as_str(), "val")];
            let info = leader.propose(cmds, peers, false).await.unwrap();
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
        let (replicas, _mgr, _all_cfg, peers_cfgs) = setup(3, BASE).await;

        let leader = &replicas[0];
        let peers = &peers_cfgs[0];

        // Inject a conflict on peer replica 1: as if peer 1 had previously
        // handled a command for key "x" at seq=5, slot=99 of replica 1.
        // When the leader sends PreAccept{seq=1, deps=[-1,-1,-1]}, peer 1
        // computes local attrs: seq=max(1,5+1)=6, deps[1]=99, which differs
        // from the leader's proposed attrs → peer replies PREACCEPTED → slow path.
        //
        // The resulting commit will have deps[1]=99.  Inject a committed
        // placeholder at (replica=1, slot=99) on all replicas so the SCC
        // executor doesn't block waiting for a dep that was never proposed.
        for r in &replicas {
            r.inject_committed_instance(1, 99, 5);
        }
        replicas[1].inject_conflict("x", 5, 1, 99);

        let cmds = vec![put("x", "slow")];
        let info = leader.propose(cmds, peers, false).await.unwrap();

        assert!(
            !info.fast_path,
            "expected slow path but got fast path: seq={} deps={:?}",
            info.seq,
            info.deps
        );

        // The slow path must still commit and execute correctly.
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

    // ── SCC execution-ordering tests ──────────────────────────────────────────
    // These tests bypass the network (no cluster setup) and directly commit
    // instances via `commit_instance_raw` to verify that the SCC executor
    // applies commands in the correct dependency order.

    #[tokio::test]
    async fn test_execution_ordering_out_of_order_delivery() {
        // Instance B depends on A.  B is committed first (blocked: A missing).
        // When A commits, both should execute: A first, then B.
        // Final value of "x" must be B's value, not A's.
        let replica = Replica::new(0, 3);

        // Commit B (slot 1) first — depends on A at slot 0.
        replica.commit_instance_raw(0, 1, vec![put("x", "B")], 2, vec![0, -1, -1]);
        assert_eq!(
            replica.get_store_value("x"),
            None,
            "B should be blocked while A is not yet committed"
        );

        // Commit A (slot 0) — no deps.
        replica.commit_instance_raw(0, 0, vec![put("x", "A")], 1, vec![-1, -1, -1]);
        // Executor fires: A executes (x="A"), then B executes (x="B").
        assert_eq!(
            replica.get_store_value("x"),
            Some("B".to_string()),
            "final value must be B's (executed after A)"
        );
    }

    #[tokio::test]
    async fn test_scc_cycle_tie_broken_by_seq() {
        // A (replica 0, slot 0, seq=1) depends on B=(1,0).
        // B (replica 1, slot 0, seq=2) depends on A=(0,0).
        // A and B form an SCC (cycle).  Both commands write "x"; the one with
        // lower seq executes first so the one with higher seq wins.
        let replica = Replica::new(0, 3);

        // Commit A: deps[1]=0 means A depends on (replica=1, slot=0) = B.
        replica.commit_instance_raw(0, 0, vec![put("x", "from_A")], 1, vec![-1, 0, -1]);
        // A is blocked: B=(1,0) not committed yet.
        assert_eq!(replica.get_store_value("x"), None, "A blocked on B");

        // Commit B: deps[0]=0 means B depends on (replica=0, slot=0) = A.
        replica.commit_instance_raw(1, 0, vec![put("x", "from_B")], 2, vec![0, -1, -1]);
        // Executor finds {A, B} — an SCC (cycle).  Sorted by seq: A(1) then B(2).
        // x = "from_A" then x = "from_B".  Final: "from_B".
        assert_eq!(
            replica.get_store_value("x"),
            Some("from_B".to_string()),
            "higher-seq command in the SCC must execute last"
        );
    }

    // ── Recovery tests ────────────────────────────────────────────────────────
    // These tests simulate a leader crash after various phases and verify that
    // another replica can recover the instance via the Prepare protocol.

    /// Slow-path recovery: one peer has ACCEPTED state (from a crashed leader
    /// that completed Accept but not Commit).  The recoverer must find the
    /// ACCEPTED reply, re-do Accept with those attrs, then Commit.
    #[tokio::test]
    async fn test_recovery_accepted() {
        const BASE: u16 = 19042;
        let (replicas, _mgr, _all_cfg, peers_cfgs) = setup(3, BASE).await;

        // Replica 1 received Accept from the crashed leader (replica 0) and
        // transitioned to ACCEPTED.  Replica 0 itself has no state (crashed
        // before recording anything locally).
        let deps = vec![-1i32, -1, -1];
        replicas[1].inject_accepted_instance(0, 0, vec![put("x", "recovered")], 1, deps);

        // Replica 2 is the recoverer.
        let info = replicas[2]
            .recover(0, 0, &peers_cfgs[2])
            .await
            .expect("recovery failed");

        assert!(
            !info.fast_path,
            "ACCEPTED path must use slow recovery: seq={} deps={:?}",
            info.seq,
            info.deps
        );

        tokio::time::sleep(Duration::from_millis(200)).await;

        let val = replicas[2].get_store_value("x");
        assert_eq!(
            val.as_deref(),
            Some("recovered"),
            "replica 2 missing 'x' after accepted-path recovery"
        );
    }

    /// Fast-path recovery via TryPreAccept: both peers have PREACCEPTED state
    /// with equal attrs (crashed leader sent PreAccept but not Accept/Commit).
    /// The recoverer discovers N-1 equal PREACCEPTED replies, confirms no
    /// conflicts via TryPreAccept, and commits on the fast path.
    #[tokio::test]
    async fn test_recovery_preaccepted_fast_path() {
        const BASE: u16 = 19045;
        let (replicas, _mgr, _all_cfg, peers_cfgs) = setup(3, BASE).await;

        // Both peers (0 and 1) received PreAccept from the crashed leader and
        // replied PREACCEPTED_EQ.  Leader crashed before Commit.
        let deps = vec![-1i32, -1, -1];
        replicas[0].inject_preaccepted_instance(
            0, 0, vec![put("x", "fast_recovered")], 1, deps.clone(),
        );
        replicas[1].inject_preaccepted_instance(
            0, 0, vec![put("x", "fast_recovered")], 1, deps.clone(),
        );

        // Replica 2 is the recoverer.  Prepare sees N-1=2 PREACCEPTED replies
        // with identical attrs → TryPreAccept → all ok → fast commit.
        let info = replicas[2]
            .recover(0, 0, &peers_cfgs[2])
            .await
            .expect("recovery failed");

        assert!(
            info.fast_path,
            "expected fast-path recovery but got slow path: seq={} deps={:?}",
            info.seq,
            info.deps
        );

        tokio::time::sleep(Duration::from_millis(200)).await;

        let val = replicas[2].get_store_value("x");
        assert_eq!(
            val.as_deref(),
            Some("fast_recovered"),
            "replica 2 missing 'x' after fast-path recovery"
        );
    }

    /// Thrifty mode: Commit is sent only to the slow-quorum subset of peers
    /// (⌊N/2⌋ = 1 for N=3) rather than all N-1=2 peers.
    ///
    /// After the commit the leader (replica 0) and one quorum peer have
    /// COMMITTED state and execute the command immediately.  The other peer
    /// (the one outside the quorum) remains at PREACCEPTED and has NOT yet
    /// executed — its store should be empty immediately after the commit.
    #[tokio::test]
    async fn test_thrifty_commit() {
        const BASE: u16 = 19048;
        let (replicas, _mgr, _all_cfg, peers_cfgs) = setup(3, BASE).await;

        let leader = &replicas[0];
        let peers = &peers_cfgs[0]; // peers of replica 0: nodes 1 and 2 (IDs 2, 3)

        let cmds = vec![put("thrifty_key", "thrifty_val")];
        let info = leader.propose(cmds, peers, true).await.unwrap();

        assert!(
            info.fast_path,
            "expected fast path in thrifty test: seq={} deps={:?}",
            info.seq,
            info.deps
        );

        // Give the quorum peer time to receive and apply its Commit, and give
        // the non-quorum peer time to NOT receive anything.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Leader (committed locally) must have the key.
        assert_eq!(
            leader.get_store_value("thrifty_key").as_deref(),
            Some("thrifty_val"),
            "leader missing 'thrifty_key'"
        );

        // Exactly one of the two peers received Commit (the first in the config
        // order, which is peers_cfgs[0].nodes()[0] = the peer with the lowest
        // node ID among replica 0's peers).  The other peer did NOT receive a
        // Commit and is still at PREACCEPTED → its store is empty.
        let peer_vals: Vec<Option<String>> = replicas[1..].iter()
            .map(|r| r.get_store_value("thrifty_key"))
            .collect();

        let committed_peers = peer_vals.iter().filter(|v| v.is_some()).count();
        let not_committed_peers = peer_vals.iter().filter(|v| v.is_none()).count();

        assert_eq!(
            committed_peers, 1,
            "thrifty mode must commit to exactly 1 peer (slow_quorum_peers=1 for N=3); \
             got {committed_peers} peers with the key: {peer_vals:?}"
        );
        assert_eq!(
            not_committed_peers, 1,
            "thrifty mode must leave exactly 1 peer without the committed value; \
             got {not_committed_peers}: {peer_vals:?}"
        );
    }

    #[tokio::test]
    async fn test_multiple_leaders() {
        const BASE: u16 = 19039;
        let (replicas, _mgr, _all_cfg, peers_cfgs) = setup(3, BASE).await;

        // Replica 0 and replica 1 propose concurrently.
        let leader0 = replicas[0].clone();
        let leader1 = replicas[1].clone();
        let peers0 = peers_cfgs[0].clone();
        let peers1 = peers_cfgs[1].clone();

        let h0 = tokio::spawn(async move {
            let cmds = vec![put("leader0_key", "v0")];
            leader0.propose(cmds, &peers0, false).await
        });

        let h1 = tokio::spawn(async move {
            let cmds = vec![put("leader1_key", "v1")];
            leader1.propose(cmds, &peers1, false).await
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
