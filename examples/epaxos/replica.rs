//! EPaxos replica state machine and server handler implementation.
//!
//! This module contains:
//! - `ReplicaInner`: mutable state (instances, conflict map, kv store).
//! - `Replica`: a cheaply-cloneable `Arc<Mutex<ReplicaInner>>` handle that
//!   also implements `EPaxosServer` so it can be registered with the quorums
//!   `Server`.
//! - `Replica::propose`: the leader path — runs PreAccept, optionally Accept,
//!   then Commit.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tonic::Status;

use quorums::{Configuration, Server, ServerCtx};

use super::pb::{
    e_paxos_server::{self, EPaxosServer},
    AcceptReply, Command, CommitRequest, PreAcceptRequest, PreAcceptReply,
    AcceptRequest,
    e_paxos_client,
};

// ── Status constants ──────────────────────────────────────────────────────────

pub mod status {
    pub const NONE: i32 = 0;
    pub const PREACCEPTED: i32 = 1;     // attrs differ from leader → slow path
    pub const PREACCEPTED_EQ: i32 = 2;  // attrs equal → fast path eligible
    pub const ACCEPTED: i32 = 3;
    pub const COMMITTED: i32 = 4;
}

// ── Instance record ───────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct Instance {
    pub cmds: Vec<Command>,
    pub seq: i32,
    pub deps: Vec<i32>,
    pub status: i32,
    pub ballot: i32,
}

impl Instance {
    fn new(n: usize) -> Self {
        Instance {
            cmds: Vec::new(),
            seq: 0,
            deps: vec![-1; n],
            status: status::NONE,
            ballot: 0,
        }
    }
}

// ── Conflict map entry ────────────────────────────────────────────────────────

/// Per-key conflict tracking.  `max_seq` is the highest sequence number seen
/// for this key.  `deps[r]` is the last slot on replica `r` that touched this
/// key, or -1 if none.
#[derive(Clone, Debug)]
pub struct ConflictEntry {
    pub max_seq: i32,
    pub deps: Vec<i32>,  // deps[r] = last conflicting slot on replica r, -1 if none
}

// ── ReplicaInner ─────────────────────────────────────────────────────────────

pub struct ReplicaInner {
    pub id: usize,           // this replica's index
    pub n: usize,            // total replicas in cluster
    pub cur_slot: i32,       // next slot to allocate
    pub instances: Vec<Vec<Instance>>,  // instances[replica][slot]
    pub conflicts: HashMap<String, ConflictEntry>,
    pub store: HashMap<String, String>, // committed key-value data
}

impl ReplicaInner {
    fn new(id: usize, n: usize) -> Self {
        ReplicaInner {
            id,
            n,
            cur_slot: 0,
            instances: vec![Vec::new(); n],
            conflicts: HashMap::new(),
            store: HashMap::new(),
        }
    }

    /// Allocate a new slot on this replica and compute initial (seq, deps)
    /// from the conflict map.  Updates the conflict map to record this slot.
    fn start_proposal(&mut self, cmds: &[Command]) -> (i32, i32, Vec<i32>) {
        let slot = self.cur_slot;
        self.cur_slot += 1;

        let mut seq = 1i32;
        let mut deps = vec![-1i32; self.n];

        // Compute attrs from all keys touched by this command batch.
        for cmd in cmds {
            if cmd.key.is_empty() {
                continue;
            }
            if let Some(entry) = self.conflicts.get(&cmd.key) {
                seq = seq.max(entry.max_seq + 1);
                for r in 0..self.n {
                    deps[r] = deps[r].max(entry.deps[r]);
                }
            }
        }

        // Update conflict map with this slot.
        let r = self.id;
        for cmd in cmds {
            if cmd.key.is_empty() {
                continue;
            }
            let entry = self.conflicts.entry(cmd.key.clone()).or_insert(ConflictEntry {
                max_seq: 0,
                deps: vec![-1; self.n],
            });
            entry.max_seq = entry.max_seq.max(seq);
            entry.deps[r] = entry.deps[r].max(slot);
        }

        // Ensure we don't depend on ourselves (self-dep prevention).
        deps[r] = if deps[r] >= slot { slot - 1 } else { deps[r] };

        (slot, seq, deps)
    }

    /// Apply committed commands to the KV store.
    fn apply_cmds(&mut self, cmds: &[Command]) {
        use super::pb::Operation;
        for cmd in cmds {
            match cmd.op() {
                Operation::Put => {
                    self.store.insert(cmd.key.clone(), cmd.value.clone());
                }
                Operation::Get | Operation::Nop => {}
            }
        }
    }

    /// Ensure the instances vec for replica `r` has at least `slot+1` entries.
    fn ensure_slot(&mut self, r: usize, slot: i32) {
        let slot = slot as usize;
        while self.instances[r].len() <= slot {
            self.instances[r].push(Instance::new(self.n));
        }
    }
}

// ── Replica handle ────────────────────────────────────────────────────────────

/// Cheaply-cloneable handle to a replica.  Wraps `Arc<Mutex<ReplicaInner>>`.
///
/// The mutex is NEVER held across await points: lock → compute → unlock →
/// then do network I/O.  This avoids deadlocks and keeps latency low.
#[derive(Clone)]
pub struct Replica {
    pub inner: Arc<Mutex<ReplicaInner>>,
}

impl Replica {
    pub fn new(id: usize, n: usize) -> Self {
        Replica {
            inner: Arc::new(Mutex::new(ReplicaInner::new(id, n))),
        }
    }

    /// Read a value from the committed store (test helper).
    #[cfg(test)]
    pub fn get_store_value(&self, key: &str) -> Option<String> {
        self.inner.lock().unwrap().store.get(key).cloned()
    }

    /// Return the full store snapshot (test helper).
    pub fn store_snapshot(&self) -> HashMap<String, String> {
        self.inner.lock().unwrap().store.clone()
    }

    /// Register this replica as a quorums server handler.
    pub fn register(&self, server: &mut Server) {
        e_paxos_server::register_e_paxos(server, Arc::new(self.clone()));
    }

    // ── Server handler helpers (called with lock released) ────────────────────

    /// Handle a PreAccept message from a leader.
    ///
    /// Computes local (seq, deps) for the keys, checks whether they match the
    /// leader's proposed attrs, and replies PREACCEPTED_EQ (fast path) or
    /// PREACCEPTED (slow path).
    fn handle_pre_accept_inner(inner: &mut ReplicaInner, req: &PreAcceptRequest) -> PreAcceptReply {
        let r = req.replica as usize;
        let slot = req.slot;
        inner.ensure_slot(r, slot);

        let n = inner.n;
        let my_id = inner.id;

        // Compute local seq and deps from the conflict map for these keys.
        let mut local_seq = req.seq;
        let mut local_deps = req.deps.clone();
        // Ensure local_deps has exactly n entries.
        local_deps.resize(n, -1);

        for cmd in &req.cmds {
            if cmd.key.is_empty() {
                continue;
            }
            if let Some(entry) = inner.conflicts.get(&cmd.key) {
                local_seq = local_seq.max(entry.max_seq + 1);
                for i in 0..n {
                    local_deps[i] = local_deps[i].max(entry.deps[i]);
                }
            }
        }

        // Self-dep prevention: a command cannot depend on itself.
        // If we computed local_deps[req.replica] >= req.slot, cap it.
        let ri = req.replica as usize;
        if local_deps[ri] >= slot {
            local_deps[ri] = slot - 1;
        }

        // Update conflict map for the keys in this request.
        for cmd in &req.cmds {
            if cmd.key.is_empty() {
                continue;
            }
            let entry = inner.conflicts.entry(cmd.key.clone()).or_insert(ConflictEntry {
                max_seq: 0,
                deps: vec![-1; n],
            });
            entry.max_seq = entry.max_seq.max(local_seq);
            entry.deps[r] = entry.deps[r].max(slot);
        }

        // Record instance state.
        let inst = &mut inner.instances[r][slot as usize];
        inst.cmds = req.cmds.clone();
        inst.seq = local_seq;
        inst.deps = local_deps.clone();
        inst.status = status::PREACCEPTED;
        inst.ballot = req.ballot;

        // Compare our attrs against the leader's proposed attrs.
        // Pad req.deps to n entries for comparison.
        let mut req_deps_padded = req.deps.clone();
        req_deps_padded.resize(n, -1);

        let equal = local_seq == req.seq && local_deps == req_deps_padded;
        let reply_status = if equal {
            status::PREACCEPTED_EQ
        } else {
            status::PREACCEPTED
        };

        let committed_deps = vec![-1i32; n]; // simplified: no committed-deps tracking

        PreAcceptReply {
            replica: my_id as i32,
            slot,
            status: reply_status,
            ballot: req.ballot,
            seq: local_seq,
            deps: local_deps,
            committed_deps,
        }
    }

    /// Handle an Accept message (slow path phase 2).
    fn handle_accept_inner(inner: &mut ReplicaInner, req: &AcceptRequest) -> AcceptReply {
        let r = req.replica as usize;
        let slot = req.slot;
        inner.ensure_slot(r, slot);

        let inst = &mut inner.instances[r][slot as usize];
        inst.cmds = req.cmds.clone();
        inst.seq = req.seq;
        inst.deps = req.deps.clone();
        inst.status = status::ACCEPTED;
        inst.ballot = req.ballot;

        AcceptReply {
            replica: inner.id as i32,
            slot,
            ok: true,
            ballot: req.ballot,
        }
    }

    /// Handle a Commit message.  Marks the instance COMMITTED and applies
    /// commands to the local KV store.
    fn handle_commit_inner(inner: &mut ReplicaInner, req: &CommitRequest) {
        let r = req.replica as usize;
        let slot = req.slot;
        inner.ensure_slot(r, slot);

        let inst = &mut inner.instances[r][slot as usize];
        inst.cmds = req.cmds.clone();
        inst.seq = req.seq;
        inst.deps = req.deps.clone();
        inst.status = status::COMMITTED;

        inner.apply_cmds(&req.cmds.clone());
    }

    /// Leader-side commit: record + apply without going through the network.
    pub fn commit_local(&self, slot: i32, cmds: Vec<Command>, seq: i32, deps: Vec<i32>) {
        let mut inner = self.inner.lock().unwrap();
        let r = inner.id;
        inner.ensure_slot(r, slot);
        let inst = &mut inner.instances[r][slot as usize];
        inst.cmds = cmds.clone();
        inst.seq = seq;
        inst.deps = deps;
        inst.status = status::COMMITTED;
        inner.apply_cmds(&cmds);
    }

    // ── Leader path ───────────────────────────────────────────────────────────

    /// Propose a batch of commands.
    ///
    /// `peers` is the configuration of the N-1 other replicas (used for
    /// PreAccept and Accept fan-out).  `all` is all N replicas (used for
    /// Commit fan-out, excluding self since we commit locally).
    ///
    /// Returns `CommitInfo` describing how the proposal was committed.
    pub async fn propose(
        &self,
        cmds: Vec<Command>,
        peers: &Configuration,
        _all: &Configuration,
    ) -> Result<CommitInfo, quorums::Error> {
        // ── Phase 1: allocate slot and compute initial attrs ──────────────────
        // Lock, compute, unlock before any network I/O.
        let (my_id, slot, orig_seq, orig_deps, ballot) = {
            let mut inner = self.inner.lock().unwrap();
            let (slot, seq, deps) = inner.start_proposal(&cmds);
            let my_id = inner.id as i32;
            let ballot = 0i32;
            (my_id, slot, seq, deps, ballot)
        };

        let n = peers.size() + 1; // total replicas (peers + self)

        // ── Phase 1: PreAccept fan-out ────────────────────────────────────────
        let req = PreAcceptRequest {
            leader_id: my_id,
            replica: my_id,
            slot,
            ballot,
            seq: orig_seq,
            deps: orig_deps.clone(),
            cmds: cmds.clone(),
        };

        // The quorum function decides whether to take the fast or slow path.
        //
        // Fast path: all f+1 peers reply PREACCEPTED_EQ (attrs identical).
        //   - fast_quorum = n - 1 (all peers)
        //
        // Slow path: floor(N/2) peers have replied (majority of non-leaders).
        //   - slow_quorum_peers = n / 2
        //
        // We keep merging attrs as replies arrive.  Resolution happens as soon
        // as either threshold is met or the fast-path condition is detected.
        //
        // Because quorum()'s closure must return Option<PreAcceptReply>, we
        // communicate fast_path and merged attrs via a shared Arc<Mutex<...>>.

        let fast_quorum = n - 1;
        let slow_quorum_peers = n / 2;

        let orig_deps_cap = orig_deps.clone();
        let orig_seq_cap = orig_seq;

        // Shared side-channel: set by the quorum closure, read after it resolves.
        let decision_cell: Arc<std::sync::Mutex<(i32, Vec<i32>, bool)>> =
            Arc::new(std::sync::Mutex::new((orig_seq, orig_deps.clone(), true)));
        let decision_cell_cap = Arc::clone(&decision_cell);

        let responses = e_paxos_client::pre_accept(&peers.context(), &req).await?;

        // Run the quorum closure.  It returns Some(first_reply) when resolved;
        // the actual decision (seq/deps/fast_path) is written to decision_cell.
        responses
            .quorum(move |replies: &[PreAcceptReply]| {
                let cnt = replies.len();

                // Merge attrs across all replies seen so far.
                let mut merged_seq = orig_seq_cap;
                let mut merged_deps = orig_deps_cap.clone();
                let mut all_eq = true;

                for r in replies {
                    if r.status != status::PREACCEPTED_EQ {
                        all_eq = false;
                    }
                    merged_seq = merged_seq.max(r.seq);
                    for (i, &d) in r.deps.iter().enumerate() {
                        if i < merged_deps.len() {
                            merged_deps[i] = merged_deps[i].max(d);
                        }
                    }
                }

                let resolve = |seq: i32, deps: Vec<i32>, fast: bool, reply: &PreAcceptReply| {
                    *decision_cell_cap.lock().unwrap() = (seq, deps, fast);
                    Some(reply.clone())
                };

                // Fast path: all fast_quorum peers agreed with identical attrs.
                if cnt >= fast_quorum && all_eq {
                    return resolve(orig_seq_cap, orig_deps_cap.clone(), true, &replies[cnt - 1]);
                }

                // Slow path trigger 1: got fast_quorum replies but not all EQ.
                if cnt >= fast_quorum {
                    return resolve(merged_seq, merged_deps, false, &replies[cnt - 1]);
                }

                // Slow path trigger 2: any reply is PREACCEPTED and we have
                // enough for a slow-path quorum.
                let any_not_eq = replies.iter().any(|r| r.status != status::PREACCEPTED_EQ);
                if any_not_eq && cnt >= slow_quorum_peers {
                    return resolve(merged_seq, merged_deps, false, &replies[cnt - 1]);
                }

                None
            })
            .await?;

        let (final_seq, final_deps, fast_path) = decision_cell.lock().unwrap().clone();

        // ── Phase 2: Accept (slow path only) ─────────────────────────────────
        if !fast_path {
            let accept_req = AcceptRequest {
                leader_id: my_id,
                replica: my_id,
                slot,
                ballot,
                seq: final_seq,
                deps: final_deps.clone(),
                cmds: cmds.clone(),
            };
            // Wait for floor(N/2) Accept replies from peers.
            e_paxos_client::accept(&peers.context(), &accept_req)
                .await?
                .threshold(slow_quorum_peers)
                .await?;
        }

        // ── Phase 3: Commit ───────────────────────────────────────────────────
        // Commit locally first (no network), then notify peers via multicast.
        self.commit_local(slot, cmds.clone(), final_seq, final_deps.clone());

        // Commit multicast goes to all N-1 peers (not self — already committed).
        let commit_req = CommitRequest {
            leader_id: my_id,
            replica: my_id,
            slot,
            cmds,
            seq: final_seq,
            deps: final_deps.clone(),
        };
        // Use peers config (not all), since self already committed above.
        e_paxos_client::commit(&peers.context(), &commit_req).await?;

        Ok(CommitInfo {
            replica: my_id as usize,
            slot,
            seq: final_seq,
            deps: final_deps,
            fast_path,
        })
    }

    // ── Test-only helpers ─────────────────────────────────────────────────────

    /// Inject a conflict on this replica for `key` to force the slow path.
    ///
    /// Bumps the conflict map so this replica's PreAccept reply will have
    /// higher attrs than peers, causing mismatched replies → PREACCEPTED status.
    /// Inject a conflict on this replica for `key` to force the slow path.
    ///
    /// `peer_replica` must be a *different* replica's index so the conflict
    /// shows up as a dep on that peer's slot — avoiding the self-dep cap
    /// which would zero out a dep on the leader's own slot.
    #[cfg(test)]
    pub fn inject_conflict(&self, key: &str, seq: i32, peer_replica: usize, peer_slot: i32) {
        let mut inner = self.inner.lock().unwrap();
        let n = inner.n;
        let entry = inner.conflicts.entry(key.to_string()).or_insert(ConflictEntry {
            max_seq: 0,
            deps: vec![-1; n],
        });
        entry.max_seq = entry.max_seq.max(seq);
        if peer_replica < n {
            entry.deps[peer_replica] = entry.deps[peer_replica].max(peer_slot);
        }
    }
}

// ── EPaxosServer impl ─────────────────────────────────────────────────────────

impl EPaxosServer for Replica {
    async fn pre_accept(
        &self,
        _ctx: ServerCtx,
        req: PreAcceptRequest,
    ) -> Result<Option<PreAcceptReply>, Status> {
        // Lock, compute reply, unlock — no awaits while lock is held.
        let reply = {
            let mut inner = self.inner.lock().unwrap();
            Replica::handle_pre_accept_inner(&mut inner, &req)
        };
        Ok(Some(reply))
    }

    async fn accept(
        &self,
        _ctx: ServerCtx,
        req: AcceptRequest,
    ) -> Result<Option<AcceptReply>, Status> {
        let reply = {
            let mut inner = self.inner.lock().unwrap();
            Replica::handle_accept_inner(&mut inner, &req)
        };
        Ok(Some(reply))
    }

    async fn commit(
        &self,
        _ctx: ServerCtx,
        req: CommitRequest,
    ) -> Result<Option<()>, Status> {
        {
            let mut inner = self.inner.lock().unwrap();
            Replica::handle_commit_inner(&mut inner, &req);
        }
        Ok(None)
    }
}

// ── CommitInfo ────────────────────────────────────────────────────────────────

/// Returned by `Replica::propose` on successful commit.
#[derive(Debug)]
#[allow(dead_code)] // `replica` is informative even when tests only inspect other fields
pub struct CommitInfo {
    pub replica: usize,
    pub slot: i32,
    pub seq: i32,
    pub deps: Vec<i32>,
    pub fast_path: bool,
}
