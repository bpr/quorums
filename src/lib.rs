// Internal proto generated code.
pub(crate) mod proto {
    pub(crate) mod gorums {
        tonic::include_proto!("gorums");
    }
}

mod channel;
mod error;
mod manager;
mod responses;
mod router;

pub mod call_types;
pub mod config;
pub mod correctable;
pub mod health;
pub mod interceptor;
pub mod method;
pub mod node;
pub mod ordered_responses;
pub mod server;

// ── Public re-exports ─────────────────────────────────────────────────────────

pub use call_types::{
    correctable_call, multicast, ordered_quorum_call, quorum_call, rpc_call, unicast,
};
pub use method::{
    CorrectableMethod, MulticastMethod, OrderedQuorumCallMethod, QuorumCallMethod, RpcCallMethod,
    UnicastMethod,
};
pub use config::{ConfigContext, Configuration};
pub use correctable::Correctable;
pub use error::{Error, NodeError, QuorumCallCause, QuorumCallError};
pub use health::{HealthConfig, HealthStatus, NodeHealthChecker, check_node};
pub use interceptor::{
    CallInfo, Interceptor, ServerCallInfo, ServerInterceptor, interceptor, server_interceptor,
};
pub use manager::Manager;
pub use node::{Node, NodeContext, NodeStatus};
pub use ordered_responses::{OrderedNodeResponse, OrderedResponses};
pub use responses::{NodeResponse, Responses};
pub use server::{Locked, Released, Server, ServerCtx};
pub use tokio_util::sync::CancellationToken;
