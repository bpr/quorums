use std::collections::HashSet;

use tokio_util::sync::CancellationToken;

use crate::interceptor::Interceptor;
use crate::node::Node;

/// An immutable set of nodes on which multicast or quorum calls may be
/// invoked.  Cheap to clone (all nodes are `Arc`-backed).
///
/// # Configuration views
///
/// New configurations can be derived from existing ones without creating new
/// connections.  All views reuse the underlying `Arc<NodeInner>` so the
/// connection tasks keep running as long as any handle to the node exists.
///
/// ```ignore
/// // 5-node base config
/// let all   = mgr.all_nodes();
/// // Remove a failed node
/// let live  = all.without_nodes(&[3]);
/// // Quorum call on the surviving nodes
/// let resp  = quorum_call(&live.context(), &req, METHOD).await?.majority().await?;
/// ```
#[derive(Clone, Debug)]
pub struct Configuration {
    nodes: Vec<Node>,
}

impl Configuration {
    pub(crate) fn new(nodes: Vec<Node>) -> Self {
        Configuration { nodes }
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    pub fn node_ids(&self) -> Vec<u32> {
        self.nodes.iter().map(|n| n.id()).collect()
    }

    // ── View constructors ─────────────────────────────────────────────────────

    /// New configuration containing all nodes in `self` whose ID is **not**
    /// in `ids`.
    pub fn without_nodes(&self, ids: &[u32]) -> Configuration {
        let excluded: HashSet<u32> = ids.iter().copied().collect();
        Configuration {
            nodes: self
                .nodes
                .iter()
                .filter(|n| !excluded.contains(&n.id()))
                .cloned()
                .collect(),
        }
    }

    /// New configuration containing only the nodes in `self` whose ID
    /// appears in `ids`.  IDs not present in `self` are silently ignored.
    /// The order of the resulting nodes follows `ids`.
    pub fn sub_config(&self, ids: &[u32]) -> Configuration {
        let by_id: std::collections::HashMap<u32, &Node> =
            self.nodes.iter().map(|n| (n.id(), n)).collect();
        Configuration {
            nodes: ids
                .iter()
                .filter_map(|id| by_id.get(id).copied().cloned())
                .collect(),
        }
    }

    /// New configuration = nodes in `self` **plus** `extra`, deduplicated by
    /// node ID.  When both `self` and `extra` contain the same ID, `self`'s
    /// version wins and `extra`'s is dropped.
    pub fn with_additional_nodes(&self, extra: impl IntoIterator<Item = Node>) -> Configuration {
        let mut seen: HashSet<u32> = self.nodes.iter().map(|n| n.id()).collect();
        let mut nodes = self.nodes.clone();
        for n in extra {
            if seen.insert(n.id()) {
                nodes.push(n);
            }
        }
        Configuration { nodes }
    }

    /// Union: new configuration containing all nodes from `self` and `other`,
    /// deduplicated by node ID.  `self`'s version wins on conflicts.
    pub fn merge(&self, other: &Configuration) -> Configuration {
        self.with_additional_nodes(other.nodes.iter().cloned())
    }

    /// Intersection: new configuration containing only nodes whose ID appears
    /// in **both** `self` and `other`.  Order follows `self`.
    pub fn intersect(&self, other: &Configuration) -> Configuration {
        let other_ids: HashSet<u32> = other.nodes.iter().map(|n| n.id()).collect();
        Configuration {
            nodes: self
                .nodes
                .iter()
                .filter(|n| other_ids.contains(&n.id()))
                .cloned()
                .collect(),
        }
    }

    /// Difference: new configuration containing nodes in `self` whose ID does
    /// **not** appear in `other`.
    pub fn except(&self, other: &Configuration) -> Configuration {
        let other_ids: HashSet<u32> = other.nodes.iter().map(|n| n.id()).collect();
        Configuration {
            nodes: self
                .nodes
                .iter()
                .filter(|n| !other_ids.contains(&n.id()))
                .cloned()
                .collect(),
        }
    }
}

/// Bundles a `Configuration` with per-call context: cancellation token and
/// interceptor chain.
///
/// Created via [`Configuration::context`].
#[derive(Clone)]
pub struct ConfigContext {
    pub(crate) config: Configuration,
    pub(crate) cancel: CancellationToken,
    pub(crate) interceptors: Vec<Interceptor>,
    pub(crate) metadata: Vec<(String, String)>,
}

impl Configuration {
    /// Wrap this configuration in a `ConfigContext` for a single call.
    pub fn context(&self) -> ConfigContext {
        ConfigContext {
            config: self.clone(),
            cancel: CancellationToken::new(),
            interceptors: Vec::new(),
            metadata: Vec::new(),
        }
    }
}

impl ConfigContext {
    /// Override the cancellation token.  The call will return
    /// [`Error::Cancelled`][crate::error::Error::Cancelled] if `token` is
    /// cancelled before the terminal method completes.
    pub fn with_cancel(mut self, token: CancellationToken) -> Self {
        self.cancel = token;
        self
    }

    /// Set a deadline relative to now.  A child token that is automatically
    /// cancelled after `duration` is attached to this context.
    pub fn with_timeout(self, duration: std::time::Duration) -> Self {
        let child = self.cancel.child_token();
        let child2 = child.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            child2.cancel();
        });
        ConfigContext {
            config: self.config,
            cancel: child,
            interceptors: self.interceptors,
            metadata: self.metadata,
        }
    }

    /// Append an interceptor to the chain for this call.
    pub fn with_interceptor(mut self, i: Interceptor) -> Self {
        self.interceptors.push(i);
        self
    }

    /// Attach a metadata key/value pair to this call.
    ///
    /// Multiple calls accumulate entries.  All entries are forwarded to every
    /// node in the fan-out.  Duplicate keys are allowed; if the same key
    /// appears more than once the last entry wins on the wire (proto map
    /// semantics).
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.push((key.into(), value.into()));
        self
    }
}
