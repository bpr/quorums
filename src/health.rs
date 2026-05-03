use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::call_types::rpc_call;
use crate::node::Node;
use crate::proto::gorums::{HealthCheckRequest, HealthCheckResponse};

/// The gRPC method path used by the built-in health prober.
///
/// Automatically registered on every [`Server`][crate::server::Server].
pub(crate) const HEALTH_METHOD: &str = "/_gorums/health";

// ── HealthStatus ──────────────────────────────────────────────────────────────

/// Health state of a node as observed by the active prober.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// No probe has completed yet since the checker was started.
    Unknown,
    /// The most recent probe succeeded within the configured timeout.
    Healthy,
    /// The most recent probe timed out or returned an error.
    Unhealthy,
}

// ── HealthConfig ──────────────────────────────────────────────────────────────

/// Configuration for an active health prober.
///
/// # Defaults
///
/// | Field | Value |
/// |-------|-------|
/// | `interval` | 5 s |
/// | `timeout`  | 2 s |
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Time between consecutive probes.
    pub interval: Duration,
    /// Maximum time to wait for a probe response before declaring the node
    /// unhealthy.
    pub timeout: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        HealthConfig {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(2),
        }
    }
}

// ── NodeHealthChecker ─────────────────────────────────────────────────────────

/// Active health prober for a single node.
///
/// Created by [`check_node`].  The background probe task runs until
/// `NodeHealthChecker` is dropped, at which point it is cancelled automatically.
///
/// # Example
/// ```ignore
/// let checker = quorums::check_node(node.clone(), quorums::HealthConfig {
///     interval: Duration::from_secs(2),
///     timeout:  Duration::from_millis(500),
/// });
///
/// // Poll current status.
/// println!("{:?}", checker.status());
///
/// // Subscribe to changes.
/// let mut rx = checker.subscribe();
/// while rx.changed().await.is_ok() {
///     println!("health changed → {:?}", *rx.borrow());
/// }
/// ```
pub struct NodeHealthChecker {
    status_rx: watch::Receiver<HealthStatus>,
    cancel: CancellationToken,
}

impl NodeHealthChecker {
    /// Return the most recently observed health status without blocking.
    pub fn status(&self) -> HealthStatus {
        *self.status_rx.borrow()
    }

    /// Return a [`watch::Receiver`] that is notified whenever the health
    /// status changes.
    pub fn subscribe(&self) -> watch::Receiver<HealthStatus> {
        self.status_rx.clone()
    }
}

impl Drop for NodeHealthChecker {
    /// Cancels the background probe task when the checker is dropped.
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// ── check_node ────────────────────────────────────────────────────────────────

/// Start actively probing `node` at the configured interval.
///
/// The first probe is sent immediately; subsequent probes are sent every
/// [`HealthConfig::interval`].  Each probe must complete within
/// [`HealthConfig::timeout`] or the node is marked [`HealthStatus::Unhealthy`].
///
/// The background task is cancelled when the returned [`NodeHealthChecker`] is
/// dropped.
///
/// # Example
/// ```ignore
/// let checker = quorums::check_node(
///     node.clone(),
///     quorums::HealthConfig::default(),
/// );
///
/// // Wait for the first probe to complete.
/// let mut rx = checker.subscribe();
/// rx.changed().await.unwrap();
/// assert_eq!(checker.status(), quorums::HealthStatus::Healthy);
/// ```
pub fn check_node(node: Node, config: HealthConfig) -> NodeHealthChecker {
    let (status_tx, status_rx) = watch::channel(HealthStatus::Unknown);
    let cancel = CancellationToken::new();
    let cancel_bg = cancel.clone();

    tokio::spawn(async move {
        loop {
            // Send a health probe with a hard timeout.
            let ctx = node.context();
            let result = tokio::time::timeout(
                config.timeout,
                rpc_call::<HealthCheckRequest, HealthCheckResponse>(
                    &ctx,
                    &HealthCheckRequest {},
                    HEALTH_METHOD,
                ),
            )
            .await;

            let new_status = match result {
                Ok(Ok(_)) => HealthStatus::Healthy,
                _ => HealthStatus::Unhealthy,
            };

            // Only wake subscribers when the status actually changes.
            if *status_tx.borrow() != new_status {
                let _ = status_tx.send(new_status);
            }

            // Wait for the next interval, or exit if the checker is dropped.
            tokio::select! {
                biased;
                _ = cancel_bg.cancelled() => break,
                _ = tokio::time::sleep(config.interval) => {}
            }
        }
    });

    NodeHealthChecker { status_rx, cancel }
}
