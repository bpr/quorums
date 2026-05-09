/// Typed method handles for quorum call types.
///
/// Each trait is implemented by a zero-sized struct that carries the gRPC path
/// and the request/response types as associated types.  Passing a handle to a
/// call-type function eliminates the `method: &str` argument and the turbofish
/// type annotation — type mismatches become compile errors rather than runtime
/// panics.
///
/// # Defining a method handle
///
/// ```ignore
/// // For a two-way fan-out (quorum call):
/// struct MyReadMethod;
/// impl quorums::QuorumCallMethod for MyReadMethod {
///     type Req  = ReadRequest;
///     type Resp = ReadResponse;
///     const PATH: &'static str = "/svc.MyService/Read";
/// }
///
/// // For a one-way fan-out (multicast):
/// struct MyWriteMethod;
/// impl quorums::MulticastMethod for MyWriteMethod {
///     type Req = WriteRequest;
///     const PATH: &'static str = "/svc.MyService/Write";
/// }
/// ```
///
/// # Using a method handle
///
/// ```ignore
/// // Before (verbose, error-prone):
/// quorum_call::<ReadRequest, ReadResponse>(&ctx, &req, "/svc.MyService/Read").await?
///
/// // After (concise, type-safe):
/// quorum_call(&ctx, &req, MyReadMethod).await?
/// ```
///
/// # Code generation
///
/// `quorums-build` emits handle structs and trait impls automatically from
/// `.proto` service definitions.  See the [`quorums-build`] crate for details.
use prost::Message as ProstMessage;

/// Typed handle for [`rpc_call`][crate::call_types::rpc_call]:
/// a single-node two-way RPC.
pub trait RpcCallMethod {
    type Req: ProstMessage;
    type Resp: ProstMessage + Default;
    const PATH: &'static str;
}

/// Typed handle for [`unicast`][crate::call_types::unicast]:
/// a single-node one-way message (fire-and-forget).
pub trait UnicastMethod {
    type Req: ProstMessage;
    const PATH: &'static str;
}

/// Typed handle for [`multicast`][crate::call_types::multicast]:
/// fan-out one-way message to every node in a configuration.
pub trait MulticastMethod {
    type Req: ProstMessage;
    const PATH: &'static str;
}

/// Typed handle for [`quorum_call`][crate::call_types::quorum_call]:
/// fan-out two-way call; results collected into
/// [`Responses<Resp>`][crate::responses::Responses].
pub trait QuorumCallMethod {
    type Req: ProstMessage;
    type Resp: ProstMessage + Default + Send + 'static;
    const PATH: &'static str;
}

/// Typed handle for
/// [`ordered_quorum_call`][crate::call_types::ordered_quorum_call]:
/// fan-out two-way call with position-tagged results.
pub trait OrderedQuorumCallMethod {
    type Req: ProstMessage;
    type Resp: ProstMessage + Default + Send + 'static;
    const PATH: &'static str;
}

/// Typed handle for [`correctable_call`][crate::call_types::correctable_call]:
/// fan-out streaming call; incremental results collected into
/// [`Correctable<Resp>`][crate::correctable::Correctable].
pub trait CorrectableMethod {
    type Req: ProstMessage;
    type Resp: ProstMessage + Default + Send + 'static;
    const PATH: &'static str;
}
