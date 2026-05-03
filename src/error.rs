use std::fmt;

/// Per-node error accumulated inside a [`QuorumCallError`].
#[derive(Debug, Clone)]
pub struct NodeError {
    pub node_id: u32,
    pub cause: tonic::Status,
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node {}: {}", self.node_id, self.cause)
    }
}

/// Returned by quorum-call terminal methods when the call fails.
#[derive(Debug, Clone)]
pub struct QuorumCallError {
    pub cause: QuorumCallCause,
    pub node_errors: Vec<NodeError>,
}

#[derive(Debug, Clone)]
pub enum QuorumCallCause {
    Incomplete,
    SendFailure,
}

impl fmt::Display for QuorumCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cause = match self.cause {
            QuorumCallCause::Incomplete => "incomplete",
            QuorumCallCause::SendFailure => "send failure",
        };
        write!(
            f,
            "quorum call error: {} ({} node errors)",
            cause,
            self.node_errors.len()
        )
    }
}
impl std::error::Error for QuorumCallError {}

/// All errors the quorums runtime can produce.
#[derive(Debug, Clone)]
pub enum Error {
    QuorumCall(QuorumCallError),
    /// The node's channel was closed before the call completed.
    NodeClosed,
    /// The gRPC stream broke.
    StreamDown,
    /// Proto deserialisation error.
    Codec(String),
    /// The operation was cancelled (context/timeout).
    Cancelled,
    /// Transport-level tonic error.
    Transport(tonic::Status),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::QuorumCall(e) => write!(f, "{e}"),
            Error::NodeClosed => write!(f, "node closed"),
            Error::StreamDown => write!(f, "stream down"),
            Error::Codec(e) => write!(f, "codec error: {e}"),
            Error::Cancelled => write!(f, "cancelled"),
            Error::Transport(s) => write!(f, "transport error: {s}"),
        }
    }
}
impl std::error::Error for Error {}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Error::Codec(e.to_string())
    }
}
impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        Error::Transport(s)
    }
}
impl From<QuorumCallError> for Error {
    fn from(e: QuorumCallError) -> Self {
        Error::QuorumCall(e)
    }
}
