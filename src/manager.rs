use std::collections::HashMap;
use std::net;
use std::sync::Arc;

use crate::config::Configuration;
use crate::node::{Node, NodeStatus};

/// A per-node status callback registered via [`Manager::on_status_change`].
type StatusCallback = Arc<dyn Fn(u32, NodeStatus) + Send + Sync>;

/// Connection pool.  Manages a set of `Node`s and vends `Configuration`s.
///
/// The manager owns all nodes and is the single place where gRPC connections
/// are created.  Configurations borrow nodes from the pool by `Arc`-clone, so
/// a configuration continues to work even after the corresponding pool entry
/// is removed.
///
/// # Status callbacks
///
/// Register a callback to be notified whenever any managed node changes
/// connectivity status:
///
/// ```ignore
/// mgr.on_status_change(|node_id, status| {
///     println!("node {node_id}: {status:?}");
/// });
/// ```
pub struct Manager {
    nodes: HashMap<u32, Node>,
    /// Tracks highest assigned ID so `add_node_list` can auto-assign.
    next_id: u32,
    /// Callbacks invoked on every status change for every managed node.
    status_callbacks: Vec<StatusCallback>,
}

impl Manager {
    pub fn new() -> Self {
        Manager {
            nodes: HashMap::new(),
            next_id: 1,
            status_callbacks: Vec::new(),
        }
    }

    /// Add a node with an explicit ID.  Returns an error if the ID or
    /// address is already in use.
    pub fn add_node(&mut self, id: u32, addr: &str) -> Result<(), String> {
        if id == 0 {
            return Err("node ID 0 is reserved".into());
        }
        let norm = normalize_addr(addr)?;
        if self.nodes.contains_key(&id) {
            return Err(format!("node ID {id} already in use"));
        }
        if self.nodes.values().any(|n| n.address() == norm) {
            return Err(format!("address {norm} already in use"));
        }
        let node = Node::new(id, norm);
        // Start watcher tasks for each already-registered callback.
        for cb in &self.status_callbacks {
            spawn_watcher(id, node.subscribe_status(), Arc::clone(cb));
        }
        self.nodes.insert(id, node);
        if id >= self.next_id {
            self.next_id = id + 1;
        }
        Ok(())
    }

    /// Add a list of addresses, auto-assigning sequential IDs.
    pub fn add_node_list(&mut self, addrs: &[&str]) -> Result<Configuration, String> {
        let mut nodes = Vec::with_capacity(addrs.len());
        for addr in addrs {
            let id = self.next_id;
            self.add_node(id, addr)?;
            nodes.push(self.nodes[&id].clone());
        }
        Ok(Configuration::new(nodes))
    }

    /// Build a `Configuration` from a subset of node IDs already in the pool.
    pub fn configuration(&self, ids: &[u32]) -> Result<Configuration, String> {
        let mut nodes = Vec::with_capacity(ids.len());
        for &id in ids {
            let node = self
                .nodes
                .get(&id)
                .ok_or_else(|| format!("node ID {id} not found"))?;
            nodes.push(node.clone());
        }
        Ok(Configuration::new(nodes))
    }

    /// Build a `Configuration` from all nodes in the pool.
    pub fn all_nodes(&self) -> Configuration {
        let mut nodes: Vec<Node> = self.nodes.values().cloned().collect();
        nodes.sort_by_key(|n| n.id());
        Configuration::new(nodes)
    }

    /// Remove a node from the pool by ID.
    ///
    /// Returns `true` if the node was present, `false` if not found.
    ///
    /// Existing `Configuration` and `Node` handles that already hold a clone
    /// of this node are unaffected — the underlying connection stays alive
    /// until every `Arc` is dropped.
    pub fn remove_node(&mut self, id: u32) -> bool {
        self.nodes.remove(&id).is_some()
    }

    /// Create a new configuration by extending `base` with new addresses.
    ///
    /// Each address in `addrs` is added to the manager's pool with an
    /// auto-assigned ID and included in the returned configuration.  The
    /// returned configuration contains all nodes from `base` plus the new
    /// ones.
    pub fn with_new_nodes(
        &mut self,
        base: &Configuration,
        addrs: &[&str],
    ) -> Result<Configuration, String> {
        let mut new_nodes = Vec::with_capacity(addrs.len());
        for addr in addrs {
            let id = self.next_id;
            self.add_node(id, addr)?;
            new_nodes.push(self.nodes[&id].clone());
        }
        Ok(base.with_additional_nodes(new_nodes))
    }

    /// Register a callback that is invoked whenever **any** managed node
    /// changes connectivity status.
    ///
    /// The callback receives the node's ID and new [`NodeStatus`].  It is
    /// called from a dedicated background task (one per node per callback),
    /// so it must be `Send + Sync + 'static`.
    ///
    /// Callbacks registered before nodes are added also apply to nodes added
    /// afterwards — `add_node` and `add_node_list` start watcher tasks for
    /// each registered callback automatically.
    ///
    /// # Example
    ///
    /// ```ignore
    /// mgr.on_status_change(|node_id, status| {
    ///     println!("node {node_id} → {status:?}");
    /// });
    /// ```
    pub fn on_status_change<F>(&mut self, callback: F)
    where
        F: Fn(u32, NodeStatus) + Send + Sync + 'static,
    {
        let cb: StatusCallback = Arc::new(callback);
        // Start watcher tasks for nodes already in the pool.
        for (&id, node) in &self.nodes {
            spawn_watcher(id, node.subscribe_status(), Arc::clone(&cb));
        }
        self.status_callbacks.push(cb);
    }

    pub fn node(&self, id: u32) -> Option<&Node> {
        self.nodes.get(&id)
    }
}

impl Default for Manager {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn a background task that watches `rx` and calls `cb(node_id, status)`
/// on every change until the watch channel closes.
fn spawn_watcher(
    node_id: u32,
    mut rx: tokio::sync::watch::Receiver<NodeStatus>,
    cb: StatusCallback,
) {
    tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            cb(node_id, *rx.borrow());
        }
    });
}

fn normalize_addr(addr: &str) -> Result<String, String> {
    addr.parse::<net::SocketAddr>()
        .map(|a| a.to_string())
        .map_err(|_| format!("invalid address: {addr}"))
}
