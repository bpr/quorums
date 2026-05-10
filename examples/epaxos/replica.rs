//! EPaxos replica state machine and server handler implementation.
//!
//! This module contains:
//! - [`ReplicaInner`]: mutable state (instances, conflict map, kv store,
//!   execution tracking).
//! - [`Replica`]: a cheaply-cloneable `Arc<Mutex<ReplicaInner>>` handle that
//!   also implements `EPaxosServer` so it can be registered with the quorums
//!   `Server`.
//! - [`Replica::propose`]: the leader path — runs PreAccept, optionally Accept,
//!   then Commit.
//! - [`Replica::recover`]: the recovery path — runs Prepare, optionally
//!   TryPreAccept or Accept, then Commit.
//! - [`execute_all_ready`]: the SCC-based executor — called after every commit.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use petgraph::algo::tarjan_scc;
use petgraph::graph::{DiGraph, NodeIndex};
use tonic::Status;

use quorums::{Configuration, Server, ServerCtx};

use super::pb::{
    e_paxos_client,
    e_paxos_server::{self, EPaxosServer},
    AcceptReply, AcceptRequest, Command, CommitRequest, Operation,
    PreAcceptReply, PreAcceptRequest,
    PrepareReply, PrepareRequest,
    TryPreAcceptReply, TryPreAcceptRequest,
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
    /// deps[r] = last conflicting slot on replica r, or -1 if none.
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
    pub deps: Vec<i32>,
}

// ── ReplicaInner ──────────────────────────────────────────────────────────────

pub struct ReplicaInner {
    pub id: usize,
    pub n: usize,
    pub cur_slot: i32,
    /// instances[replica][slot] — each replica tracks all N replicas' slots.
    pub instances: Vec<Vec<Instance>>,
    /// executed[replica][slot] — whether a slot has been applied to the store.
    pub executed: Vec<Vec<bool>>,
    pub conflicts: HashMap<String, ConflictEntry>,
    pub store: HashMap<String, String>,
}

impl ReplicaInner {
    fn new(id: usize, n: usize) -> Self {
        ReplicaInner {
            id,
            n,
            cur_slot: 0,
            instances: vec![Vec::new(); n],
            executed: vec![Vec::new(); n],
            conflicts: HashMap::new(),
            store: HashMap::new(),
        }
    }

    /// Grow `instances[r]` and `executed[r]` to contain `slot`.
    fn ensure_slot(&mut self, r: usize, slot: i32) {
        let slot = slot as usize;
        while self.instances[r].len() <= slot {
            self.instances[r].push(Instance::new(self.n));
            self.executed[r].push(false);
        }
    }

    /// Allocate a new slot and compute initial (seq, deps) from the conflict map.
    fn start_proposal(&mut self, cmds: &[Command]) -> (i32, i32, Vec<i32>) {
        let slot = self.cur_slot;
        self.cur_slot += 1;

        let mut seq = 1i32;
        let mut deps = vec![-1i32; self.n];

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

        // A command must not depend on itself.
        deps[r] = if deps[r] >= slot { slot - 1 } else { deps[r] };

        (slot, seq, deps)
    }

    /// Apply committed commands to the KV store.  Called only from
    /// `execute_all_ready` after dependency ordering is resolved.
    fn apply_cmds(&mut self, cmds: &[Command]) {
        for cmd in cmds {
            match cmd.op() {
                Operation::Put => {
                    self.store.insert(cmd.key.clone(), cmd.value.clone());
                }
                Operation::Get | Operation::Nop => {}
            }
        }
    }
}

// ── SCC-based executor ────────────────────────────────────────────────────────

/// Run after every commit.  Finds all committed-but-not-yet-executed instances
/// whose entire transitive dependency chain is also committed, builds a directed
/// graph of those instances, runs Tarjan's strongly-connected-component algorithm
/// on it (via `petgraph`), and executes the SCCs in the order Tarjan returns them
/// (post-order: deepest dependencies first).
///
/// Within each SCC commands are executed in ascending `seq` order (the EPaxos
/// paper's tie-breaking rule).
///
/// Instances that are blocked by an uncommitted dependency are left alone; they
/// will be picked up the next time `execute_all_ready` is called (i.e., when
/// that missing dependency commits and its own `handle_commit_inner` fires).
fn execute_all_ready(inner: &mut ReplicaInner) {
    let n = inner.n;

    // ── Step 1: collect all committed-but-unexecuted instances ───────────────
    let mut pending: Vec<(usize, i32)> = Vec::new();
    for r in 0..n {
        for slot in 0..inner.instances[r].len() as i32 {
            let s = slot as usize;
            if !inner.executed[r][s] && inner.instances[r][s].status == status::COMMITTED {
                pending.push((r, slot));
            }
        }
    }
    if pending.is_empty() {
        return;
    }

    // ── Step 2: determine which pending instances are "ready" ────────────────
    // An instance is ready if every instance in its transitive dependency closure
    // is either already executed or committed (and thus also pending/ready).
    // We do a DFS per pending instance; abort the whole instance if any dep is
    // uncommitted or missing.
    let mut ready: HashSet<(usize, i32)> = HashSet::new();

    'outer: for &(r, slot) in &pending {
        let mut stack: Vec<(usize, i32)> = vec![(r, slot)];
        let mut visited: HashSet<(usize, i32)> = HashSet::new();

        while let Some((dr, ds)) = stack.pop() {
            if ds < 0 {
                continue;
            }
            if !visited.insert((dr, ds)) {
                continue;
            }

            let s = ds as usize;

            // Already executed → dep satisfied, stop following this branch.
            if s < inner.executed[dr].len() && inner.executed[dr][s] {
                continue;
            }

            // Missing or uncommitted → this instance is blocked.
            if s >= inner.instances[dr].len()
                || inner.instances[dr][s].status < status::COMMITTED
            {
                continue 'outer; // (r, slot) is not ready
            }

            // Committed, not yet executed → follow its deps.
            let dep_deps = inner.instances[dr][s].deps.clone();
            for dep_r in 0..n {
                let dep_slot = dep_deps[dep_r];
                if dep_slot >= 0 {
                    stack.push((dep_r, dep_slot));
                }
            }
        }

        ready.insert((r, slot));
    }

    if ready.is_empty() {
        return;
    }

    // ── Step 3: build directed graph on the ready set ────────────────────────
    // Edge A → B means "A depends on B" (B must execute before A).
    let ready_vec: Vec<(usize, i32)> = ready.iter().cloned().collect();

    let mut graph: DiGraph<(usize, i32), ()> = DiGraph::new();
    let mut node_idx: HashMap<(usize, i32), NodeIndex> = HashMap::new();

    for &key in &ready_vec {
        let idx = graph.add_node(key);
        node_idx.insert(key, idx);
    }

    for &(r, slot) in &ready_vec {
        let a_idx = node_idx[&(r, slot)];
        let deps = inner.instances[r][slot as usize].deps.clone();
        for dep_r in 0..n {
            let dep_slot = deps[dep_r];
            if dep_slot >= 0 {
                // Only add an edge to the dep if it is also in the ready set.
                // Already-executed deps don't need edges; they have no node.
                if let Some(&b_idx) = node_idx.get(&(dep_r, dep_slot)) {
                    graph.add_edge(a_idx, b_idx, ());
                }
            }
        }
    }

    // ── Step 4: run Tarjan's SCC ──────────────────────────────────────────────
    // `petgraph::algo::tarjan_scc` returns SCCs in post-order: an SCC with no
    // outgoing edges to other SCCs appears first.  With edges meaning "depends
    // on", a node with no outgoing edges has no dependencies — it's the deepest
    // leaf and should execute first.  So we iterate the SCCs in the returned
    // order without reversing.
    let sccs = tarjan_scc(&graph);

    // Snapshot seq for ordering within SCCs (avoids reborrowing inner).
    let seq_of: HashMap<(usize, i32), i32> = ready_vec
        .iter()
        .map(|&(r, s)| ((r, s), inner.instances[r][s as usize].seq))
        .collect();

    // ── Step 5: execute ───────────────────────────────────────────────────────
    for scc in &sccs {
        let mut group: Vec<(usize, i32)> = scc
            .iter()
            .map(|&ni| *graph.node_weight(ni).unwrap())
            .collect();
        // Within a cycle, execute by ascending seq (EPaxos tie-breaking rule).
        group.sort_by_key(|k| seq_of[k]);

        for (r, slot) in group {
            let s = slot as usize;
            if !inner.executed[r][s] {
                let cmds = inner.instances[r][s].cmds.clone();
                inner.apply_cmds(&cmds);
                inner.executed[r][s] = true;
            }
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

    /// Read a value from the committed store.
    #[cfg(test)]
    pub fn get_store_value(&self, key: &str) -> Option<String> {
        self.inner.lock().unwrap().store.get(key).cloned()
    }

    /// Return the full store snapshot.
    pub fn store_snapshot(&self) -> HashMap<String, String> {
        self.inner.lock().unwrap().store.clone()
    }

    /// Register this replica as a quorums server handler.
    pub fn register(&self, server: &mut Server) {
        e_paxos_server::register_e_paxos(server, Arc::new(self.clone()));
    }

    // ── Server handler helpers ────────────────────────────────────────────────

    fn handle_pre_accept_inner(inner: &mut ReplicaInner, req: &PreAcceptRequest) -> PreAcceptReply {
        let r = req.replica as usize;
        let slot = req.slot;
        inner.ensure_slot(r, slot);

        let n = inner.n;
        let my_id = inner.id;

        let mut local_seq = req.seq;
        let mut local_deps = req.deps.clone();
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
        let ri = req.replica as usize;
        if local_deps[ri] >= slot {
            local_deps[ri] = slot - 1;
        }

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

        let inst = &mut inner.instances[r][slot as usize];
        inst.cmds = req.cmds.clone();
        inst.seq = local_seq;
        inst.deps = local_deps.clone();
        inst.status = status::PREACCEPTED;
        inst.ballot = req.ballot;

        let mut req_deps_padded = req.deps.clone();
        req_deps_padded.resize(n, -1);
        let equal = local_seq == req.seq && local_deps == req_deps_padded;
        let reply_status = if equal { status::PREACCEPTED_EQ } else { status::PREACCEPTED };

        PreAcceptReply {
            replica: my_id as i32,
            slot,
            status: reply_status,
            ballot: req.ballot,
            seq: local_seq,
            deps: local_deps,
            committed_deps: vec![-1i32; n],
        }
    }

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

        AcceptReply { replica: inner.id as i32, slot, ok: true, ballot: req.ballot }
    }

    /// Handle a Commit message.  Marks the instance COMMITTED, then runs the
    /// SCC executor to apply any commands that are now unblocked.
    fn handle_commit_inner(inner: &mut ReplicaInner, req: &CommitRequest) {
        let r = req.replica as usize;
        let slot = req.slot;
        inner.ensure_slot(r, slot);

        // Idempotent: ignore duplicate commits.
        if inner.instances[r][slot as usize].status >= status::COMMITTED {
            return;
        }

        {
            let inst = &mut inner.instances[r][slot as usize];
            inst.cmds = req.cmds.clone();
            inst.seq = req.seq;
            inst.deps = req.deps.clone();
            inst.status = status::COMMITTED;
        }

        // Try to execute this instance and any that were waiting on it.
        execute_all_ready(inner);
    }

    fn handle_prepare_inner(inner: &mut ReplicaInner, req: &PrepareRequest) -> PrepareReply {
        let r = req.replica as usize;
        let slot = req.slot;
        let my_id = inner.id;

        if r >= inner.instances.len() || slot as usize >= inner.instances[r].len() {
            return PrepareReply {
                replica: my_id as i32,
                slot,
                ok: true,
                ballot: req.ballot,
                status: status::NONE,
                seq: 0,
                deps: vec![],
                cmds: vec![],
            };
        }

        let inst = &inner.instances[r][slot as usize];
        PrepareReply {
            replica: my_id as i32,
            slot,
            ok: true,
            ballot: req.ballot,
            status: inst.status,
            seq: inst.seq,
            deps: inst.deps.clone(),
            cmds: inst.cmds.clone(),
        }
    }

    fn handle_try_pre_accept_inner(
        inner: &mut ReplicaInner,
        req: &TryPreAcceptRequest,
    ) -> TryPreAcceptReply {
        let r = req.replica as usize;
        let slot = req.slot;
        let my_id = inner.id;
        let n = inner.n;

        // If we already have a committed/accepted instance here, it's safe.
        let s = slot as usize;
        let already_settled = r < inner.instances.len()
            && s < inner.instances[r].len()
            && inner.instances[r][s].status >= status::ACCEPTED;

        if already_settled {
            return TryPreAcceptReply {
                replica: my_id as i32,
                slot,
                ok: true,
                ballot: req.ballot,
                conflict_replica: -1,
                conflict_slot: -1,
            };
        }

        // Check for conflicts: any conflicting command at a slot not covered by deps.
        let mut conflict_replica = -1i32;
        let mut conflict_slot_out = -1i32;

        'outer: for cmd in &req.cmds {
            if cmd.key.is_empty() {
                continue;
            }
            if let Some(entry) = inner.conflicts.get(&cmd.key) {
                for dep_r in 0..n {
                    let known_slot = entry.deps[dep_r];
                    if known_slot < 0 {
                        continue;
                    }
                    let covered = if dep_r < req.deps.len() { req.deps[dep_r] } else { -1 };
                    // Conflict: we know of a slot not covered by the proposed deps,
                    // and it's not the instance being recovered itself.
                    if known_slot > covered && !(dep_r == r && known_slot == slot) {
                        conflict_replica = dep_r as i32;
                        conflict_slot_out = known_slot;
                        break 'outer;
                    }
                }
            }
        }

        TryPreAcceptReply {
            replica: my_id as i32,
            slot,
            ok: conflict_replica == -1,
            ballot: req.ballot,
            conflict_replica,
            conflict_slot: conflict_slot_out,
        }
    }

    /// Leader-side commit: record without going through the network handler,
    /// then run the executor.
    ///
    /// `r` is the replica whose slot is being committed (may differ from `self.id`
    /// during recovery).
    pub fn commit_local(&self, r: usize, slot: i32, cmds: Vec<Command>, seq: i32, deps: Vec<i32>) {
        let mut inner = self.inner.lock().unwrap();
        inner.ensure_slot(r, slot);

        if inner.instances[r][slot as usize].status < status::COMMITTED {
            let inst = &mut inner.instances[r][slot as usize];
            inst.cmds = cmds;
            inst.seq = seq;
            inst.deps = deps;
            inst.status = status::COMMITTED;
        }

        execute_all_ready(&mut inner);
    }

    // ── Leader path ───────────────────────────────────────────────────────────

    /// Propose a batch of commands.
    ///
    /// `peers` is the N-1 other replicas used for PreAccept and Accept fan-out.
    ///
    /// When `thrifty` is `true` the Commit message is sent only to the minimum
    /// quorum of peers (⌊N/2⌋) rather than all N-1.  Those peers already hold
    /// PreAccept or Accept state, so no additional round-trip is needed.
    /// Non-participant replicas learn about the commit lazily (e.g., via a
    /// future recovery Prepare or a read-repair).  When `thrifty` is `false`
    /// (the default) Commit is broadcast to all peers.
    ///
    /// Returns [`CommitInfo`] describing how the proposal was committed.
    pub async fn propose(
        &self,
        cmds: Vec<Command>,
        peers: &Configuration,
        thrifty: bool,
    ) -> Result<CommitInfo, quorums::Error> {
        // Lock, compute attrs, unlock — no awaits while lock is held.
        let (my_id, slot, orig_seq, orig_deps, ballot) = {
            let mut inner = self.inner.lock().unwrap();
            let (slot, seq, deps) = inner.start_proposal(&cmds);
            let my_id = inner.id as i32;
            (my_id, slot, seq, deps, 0i32)
        };

        let n = peers.size() + 1; // total replicas = peers + self
        let fast_quorum = n - 1;
        let slow_quorum_peers = n / 2;

        // Thrifty: commit only to the ⌊N/2⌋ peers that already have state.
        // Non-thrifty: commit to all N-1 peers.
        let commit_peers = if thrifty {
            let ids: Vec<u32> = peers.nodes()[..slow_quorum_peers]
                .iter()
                .map(|node| node.id())
                .collect();
            peers.sub_config(&ids)
        } else {
            peers.clone()
        };

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

        // The quorum closure receives all successful replies accumulated so far.
        // It decides fast vs slow path, writing the final (seq, deps, fast_path)
        // into a shared cell because the closure can only return Option<PreAcceptReply>.
        let orig_deps_cap = orig_deps.clone();
        let decision_cell: Arc<Mutex<(i32, Vec<i32>, bool)>> =
            Arc::new(Mutex::new((orig_seq, orig_deps.clone(), true)));
        let decision_cell_cap = Arc::clone(&decision_cell);

        e_paxos_client::pre_accept(&peers.context(), &req)
            .await?
            .quorum(move |replies: &[PreAcceptReply]| {
                let cnt = replies.len();
                let mut merged_seq = orig_seq;
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

                // Fast path: all peers agreed with identical attrs.
                if cnt >= fast_quorum && all_eq {
                    return resolve(orig_seq, orig_deps_cap.clone(), true, &replies[cnt - 1]);
                }
                // Slow path: enough replies but not all equal.
                if cnt >= fast_quorum {
                    return resolve(merged_seq, merged_deps, false, &replies[cnt - 1]);
                }
                // Slow path early trigger: any non-EQ reply + slow quorum met.
                if !all_eq && cnt >= slow_quorum_peers {
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
            e_paxos_client::accept(&peers.context(), &accept_req)
                .await?
                .threshold(slow_quorum_peers)
                .await?;
        }

        // ── Phase 3: Commit ───────────────────────────────────────────────────
        // Commit on the leader synchronously (no network hop), then notify peers.
        // In thrifty mode only the quorum subset receives Commit; the rest learn
        // lazily (e.g., via recovery or a future read-repair).
        self.commit_local(my_id as usize, slot, cmds.clone(), final_seq, final_deps.clone());

        let commit_req = CommitRequest {
            leader_id: my_id,
            replica: my_id,
            slot,
            cmds,
            seq: final_seq,
            deps: final_deps.clone(),
        };
        e_paxos_client::commit(&commit_peers.context(), &commit_req).await?;

        Ok(CommitInfo { replica: my_id as usize, slot, seq: final_seq, deps: final_deps, fast_path })
    }

    // ── Recovery path ─────────────────────────────────────────────────────────

    /// Recover a crashed replica's instance at `(target_r, target_slot)`.
    ///
    /// The caller is the recoverer.  `peers_cfg` must be the recoverer's peer
    /// configuration (all nodes except self).
    ///
    /// Recovery phases:
    /// 1. **Prepare** — ask all peers what they know about the target instance.
    /// 2. **Analyze** — if any peer has COMMITTED, commit directly.  If any has
    ///    ACCEPTED (highest ballot), Accept + Commit.  If all preaccepted peers
    ///    agree on the same attrs, try fast recovery via TryPreAccept; otherwise
    ///    Accept + Commit with the merged attrs.
    /// 3. **Commit** — commit locally and multicast to peers.
    pub async fn recover(
        &self,
        target_r: usize,
        target_slot: i32,
        peers_cfg: &Configuration,
    ) -> Result<CommitInfo, quorums::Error> {
        let (my_id, n, ballot) = {
            let inner = self.inner.lock().unwrap();
            (inner.id as i32, inner.n, 1i32)
        };

        let slow_quorum_peers = n / 2; // peers needed for Accept majority
        let n_peers = peers_cfg.size();

        // ── Phase 1: Prepare ─────────────────────────────────────────────────
        let prep_req = PrepareRequest {
            leader_id: my_id,
            replica: target_r as i32,
            slot: target_slot,
            ballot,
        };

        let prep_cell: Arc<Mutex<Vec<PrepareReply>>> = Arc::new(Mutex::new(vec![]));
        let prep_cell_cap = Arc::clone(&prep_cell);

        e_paxos_client::prepare(&peers_cfg.context(), &prep_req)
            .await?
            .quorum(move |replies: &[PrepareReply]| {
                if replies.len() >= n_peers {
                    *prep_cell_cap.lock().unwrap() = replies.to_vec();
                    replies.last().cloned()
                } else {
                    None
                }
            })
            .await?;

        let replies = prep_cell.lock().unwrap().clone();

        // ── Phase 2: Analyze replies ─────────────────────────────────────────

        // (a) Any COMMITTED reply → commit directly.
        if let Some(r) = replies.iter().find(|r| r.status == status::COMMITTED) {
            let (cmds, seq, deps) = (r.cmds.clone(), r.seq, r.deps.clone());
            self.commit_local(target_r, target_slot, cmds.clone(), seq, deps.clone());
            let commit_req = CommitRequest {
                leader_id: my_id,
                replica: target_r as i32,
                slot: target_slot,
                cmds,
                seq,
                deps: deps.clone(),
            };
            e_paxos_client::commit(&peers_cfg.context(), &commit_req).await?;
            return Ok(CommitInfo {
                replica: target_r,
                slot: target_slot,
                seq,
                deps,
                fast_path: false,
            });
        }

        // (b) Any ACCEPTED reply → use highest-ballot attrs, Accept + Commit.
        if let Some(r) = replies
            .iter()
            .filter(|r| r.status == status::ACCEPTED)
            .max_by_key(|r| r.ballot)
        {
            let (cmds, seq, deps) = (r.cmds.clone(), r.seq, r.deps.clone());
            let accept_req = AcceptRequest {
                leader_id: my_id,
                replica: target_r as i32,
                slot: target_slot,
                ballot,
                seq,
                deps: deps.clone(),
                cmds: cmds.clone(),
            };
            e_paxos_client::accept(&peers_cfg.context(), &accept_req)
                .await?
                .threshold(slow_quorum_peers)
                .await?;
            self.commit_local(target_r, target_slot, cmds.clone(), seq, deps.clone());
            let commit_req = CommitRequest {
                leader_id: my_id,
                replica: target_r as i32,
                slot: target_slot,
                cmds,
                seq,
                deps: deps.clone(),
            };
            e_paxos_client::commit(&peers_cfg.context(), &commit_req).await?;
            return Ok(CommitInfo {
                replica: target_r,
                slot: target_slot,
                seq,
                deps,
                fast_path: false,
            });
        }

        // (c) PREACCEPTED replies — attempt fast-path recovery if all agree.
        let preaccepted: Vec<&PrepareReply> = replies
            .iter()
            .filter(|r| r.status == status::PREACCEPTED || r.status == status::PREACCEPTED_EQ)
            .collect();

        // Attrs to recover: take first reply's attrs as the reference, or nop.
        let (rec_seq, rec_deps, rec_cmds) = if let Some(first) = preaccepted.first() {
            (first.seq, first.deps.clone(), first.cmds.clone())
        } else {
            (0i32, vec![-1i32; n], vec![])
        };

        // Count how many preaccepted replies agree on (seq, deps).
        let eq_count = preaccepted
            .iter()
            .filter(|r| r.seq == rec_seq && r.deps == rec_deps)
            .count();

        // Fast-path recovery: all N-1 peers replied PREACCEPTED with equal attrs.
        let fast_quorum_peers = n - 1;
        let try_fast = !preaccepted.is_empty() && eq_count >= fast_quorum_peers;

        if try_fast {
            let tpa_req = TryPreAcceptRequest {
                leader_id: my_id,
                replica: target_r as i32,
                slot: target_slot,
                ballot,
                seq: rec_seq,
                deps: rec_deps.clone(),
                cmds: rec_cmds.clone(),
            };

            let ok_cell: Arc<Mutex<bool>> = Arc::new(Mutex::new(true));
            let ok_cell_cap = Arc::clone(&ok_cell);
            let n_peers2 = peers_cfg.size();

            let tpa_result = e_paxos_client::try_pre_accept(&peers_cfg.context(), &tpa_req)
                .await?
                .quorum(move |replies: &[TryPreAcceptReply]| {
                    for r in replies {
                        if !r.ok {
                            *ok_cell_cap.lock().unwrap() = false;
                        }
                    }
                    if replies.len() >= n_peers2 {
                        replies.last().cloned()
                    } else {
                        None
                    }
                })
                .await;

            if tpa_result.is_ok() && *ok_cell.lock().unwrap() {
                // Fast recovery: all peers ok → commit directly.
                self.commit_local(
                    target_r,
                    target_slot,
                    rec_cmds.clone(),
                    rec_seq,
                    rec_deps.clone(),
                );
                let commit_req = CommitRequest {
                    leader_id: my_id,
                    replica: target_r as i32,
                    slot: target_slot,
                    cmds: rec_cmds,
                    seq: rec_seq,
                    deps: rec_deps.clone(),
                };
                e_paxos_client::commit(&peers_cfg.context(), &commit_req).await?;
                return Ok(CommitInfo {
                    replica: target_r,
                    slot: target_slot,
                    seq: rec_seq,
                    deps: rec_deps,
                    fast_path: true,
                });
            }
        }

        // Slow recovery: Accept + Commit.
        let accept_req = AcceptRequest {
            leader_id: my_id,
            replica: target_r as i32,
            slot: target_slot,
            ballot,
            seq: rec_seq,
            deps: rec_deps.clone(),
            cmds: rec_cmds.clone(),
        };
        e_paxos_client::accept(&peers_cfg.context(), &accept_req)
            .await?
            .threshold(slow_quorum_peers)
            .await?;
        self.commit_local(target_r, target_slot, rec_cmds.clone(), rec_seq, rec_deps.clone());
        let commit_req = CommitRequest {
            leader_id: my_id,
            replica: target_r as i32,
            slot: target_slot,
            cmds: rec_cmds,
            seq: rec_seq,
            deps: rec_deps.clone(),
        };
        e_paxos_client::commit(&peers_cfg.context(), &commit_req).await?;
        Ok(CommitInfo {
            replica: target_r,
            slot: target_slot,
            seq: rec_seq,
            deps: rec_deps,
            fast_path: false,
        })
    }

    // ── Test-only helpers ─────────────────────────────────────────────────────

    /// Directly commit an instance (bypassing the network) and run the executor.
    /// Used by unit tests to set up specific scenarios without a live cluster.
    #[cfg(test)]
    pub fn commit_instance_raw(
        &self,
        r: usize,
        slot: i32,
        cmds: Vec<Command>,
        seq: i32,
        deps: Vec<i32>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.ensure_slot(r, slot);
        if inner.instances[r][slot as usize].status < status::COMMITTED {
            let inst = &mut inner.instances[r][slot as usize];
            inst.cmds = cmds;
            inst.seq = seq;
            inst.deps = deps;
            inst.status = status::COMMITTED;
        }
        execute_all_ready(&mut inner);
    }

    /// Inject a pre-committed, pre-executed placeholder instance.
    ///
    /// Used by tests to satisfy deps that point to slots that were never
    /// actually proposed in the test (e.g., the slot injected by
    /// `inject_conflict`).  The placeholder has empty commands so it applies
    /// nothing to the store, but the executor treats it as already done.
    #[cfg(test)]
    pub fn inject_committed_instance(&self, r: usize, slot: i32, seq: i32) {
        let mut inner = self.inner.lock().unwrap();
        let n = inner.n;
        inner.ensure_slot(r, slot);
        let inst = &mut inner.instances[r][slot as usize];
        inst.cmds = vec![];
        inst.seq = seq;
        inst.deps = vec![-1; n];
        inst.status = status::COMMITTED;
        inner.executed[r][slot as usize] = true; // mark done; no commands to apply
    }

    /// Inject a PREACCEPTED instance (simulating a peer that handled PreAccept
    /// for a command that the leader then crashed before committing).
    #[cfg(test)]
    pub fn inject_preaccepted_instance(
        &self,
        r: usize,
        slot: i32,
        cmds: Vec<Command>,
        seq: i32,
        deps: Vec<i32>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.ensure_slot(r, slot);
        let inst = &mut inner.instances[r][slot as usize];
        inst.cmds = cmds;
        inst.seq = seq;
        inst.deps = deps;
        inst.status = status::PREACCEPTED;
        inst.ballot = 0;
    }

    /// Inject an ACCEPTED instance (simulating a peer that received Accept from
    /// a leader that then crashed before committing).
    #[cfg(test)]
    pub fn inject_accepted_instance(
        &self,
        r: usize,
        slot: i32,
        cmds: Vec<Command>,
        seq: i32,
        deps: Vec<i32>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.ensure_slot(r, slot);
        let inst = &mut inner.instances[r][slot as usize];
        inst.cmds = cmds;
        inst.seq = seq;
        inst.deps = deps;
        inst.status = status::ACCEPTED;
        inst.ballot = 0;
    }

    /// Inject a conflict-map entry on this replica for `key`.
    ///
    /// Makes this replica compute higher (seq, deps) attrs than peers when
    /// handling a PreAccept for that key, causing it to reply PREACCEPTED
    /// (not PREACCEPTED_EQ) → triggers the slow path.
    ///
    /// `peer_replica` must be a *different* replica's index to avoid the
    /// self-dep cap zeroing out the injected dep.
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

    async fn commit(&self, _ctx: ServerCtx, req: CommitRequest) -> Result<Option<()>, Status> {
        {
            let mut inner = self.inner.lock().unwrap();
            Replica::handle_commit_inner(&mut inner, &req);
        }
        Ok(None)
    }

    async fn prepare(
        &self,
        _ctx: ServerCtx,
        req: PrepareRequest,
    ) -> Result<Option<PrepareReply>, Status> {
        let reply = {
            let mut inner = self.inner.lock().unwrap();
            Replica::handle_prepare_inner(&mut inner, &req)
        };
        Ok(Some(reply))
    }

    async fn try_pre_accept(
        &self,
        _ctx: ServerCtx,
        req: TryPreAcceptRequest,
    ) -> Result<Option<TryPreAcceptReply>, Status> {
        let reply = {
            let mut inner = self.inner.lock().unwrap();
            Replica::handle_try_pre_accept_inner(&mut inner, &req)
        };
        Ok(Some(reply))
    }
}

// ── CommitInfo ────────────────────────────────────────────────────────────────

/// Returned by [`Replica::propose`] and [`Replica::recover`] on successful commit.
#[derive(Debug)]
#[allow(dead_code)]
pub struct CommitInfo {
    pub replica: usize,
    pub slot: i32,
    pub seq: i32,
    pub deps: Vec<i32>,
    pub fast_path: bool,
}
