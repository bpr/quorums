use std::collections::HashMap;
use std::fmt::Write as FmtWrite;

use crate::CallType;

/// prost-build service generator that emits gorums-typed client and server code.
pub(crate) struct GorumsGenerator {
    pub methods: HashMap<String, CallType>,
}

impl prost_build::ServiceGenerator for GorumsGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        let svc_name = &service.name; // e.g. "Storage"
        let svc_proto = &service.proto_name; // original proto name
        let package = &service.package; // e.g. "storage"

        let module_base = to_snake_case(svc_name); // e.g. "storage"
        let client_mod = format!("{module_base}_client");
        let server_mod = format!("{module_base}_server");

        let mut handles = String::new();
        let mut client_fns = String::new();
        let mut server_trait_fns = String::new();
        let mut register_calls = String::new();
        let mut any_annotated = false;

        for method in &service.methods {
            let full_path = format!("/{}.{}/{}", package, svc_proto, method.proto_name);

            let Some(call_type) = self.methods.get(&full_path) else {
                let _ = writeln!(buf, "// [quorums] no annotation for {full_path} — skipped");
                continue;
            };
            any_annotated = true;

            let fn_name = &method.name; // already snake_case from prost-build
            // Types with super:: prefix — for use inside sub-modules (client mod).
            let req_ty = format!("super::{}", method.input_type);
            let resp_ty = format!("super::{}", method.output_type);
            // Bare type names — for use at file level (handle impls, register closures).
            let req_bare = &method.input_type;
            let resp_bare = &method.output_type;

            // Handle struct name: e.g. "Storage" + "Read" + "Method" = "StorageReadMethod"
            let handle_name = format!("{}{}{}", svc_name, method.proto_name, "Method");

            // Emit the zero-sized handle struct + trait impl(s).
            // Handle impls are at file level, so use bare type names (no super::).
            emit_handle(
                &mut handles,
                &handle_name,
                call_type,
                req_bare,
                resp_bare,
                &full_path,
            );

            match call_type {
                CallType::RpcCall => {
                    client_fn_rpc(&mut client_fns, fn_name, &req_ty, &resp_ty, &handle_name);
                    server_trait_fn_twoway(&mut server_trait_fns, fn_name, &req_ty, &resp_ty);
                    register_handler(&mut register_calls, &full_path, fn_name, req_bare, false);
                }
                CallType::Unicast => {
                    client_fn_unicast(&mut client_fns, fn_name, &req_ty, &handle_name);
                    server_trait_fn_oneway(&mut server_trait_fns, fn_name, &req_ty);
                    register_handler(&mut register_calls, &full_path, fn_name, req_bare, false);
                }
                CallType::Multicast => {
                    client_fn_multicast(&mut client_fns, fn_name, &req_ty, &handle_name);
                    server_trait_fn_oneway(&mut server_trait_fns, fn_name, &req_ty);
                    register_handler(&mut register_calls, &full_path, fn_name, req_bare, false);
                }
                CallType::QuorumCall => {
                    client_fn_qc(&mut client_fns, fn_name, &req_ty, &resp_ty, &handle_name);
                    server_trait_fn_twoway(&mut server_trait_fns, fn_name, &req_ty, &resp_ty);
                    register_handler(&mut register_calls, &full_path, fn_name, req_bare, false);
                }
                CallType::OrderedQuorumCall => {
                    client_fn_oqc(&mut client_fns, fn_name, &req_ty, &resp_ty, &handle_name);
                    server_trait_fn_twoway(&mut server_trait_fns, fn_name, &req_ty, &resp_ty);
                    register_handler(&mut register_calls, &full_path, fn_name, req_bare, false);
                }
                CallType::Correctable => {
                    client_fn_correctable(&mut client_fns, fn_name, &req_ty, &resp_ty, &handle_name);
                    server_trait_fn_correctable(&mut server_trait_fns, fn_name, &req_ty);
                    register_handler(&mut register_calls, &full_path, fn_name, req_bare, true);
                }
            }
        }

        if !any_annotated {
            return;
        }

        // ── method handles ───────────────────────────────────────────────────
        buf.push_str(&handles);
        let _ = writeln!(buf);

        // ── client module ────────────────────────────────────────────────────
        let _ = writeln!(buf, "pub mod {client_mod} {{");
        let _ = writeln!(buf, "    //! Generated gorums client for `{svc_name}`.");
        buf.push_str(&client_fns);
        let _ = writeln!(buf, "}}");
        let _ = writeln!(buf);

        // ── server module ────────────────────────────────────────────────────
        let _ = writeln!(buf, "pub mod {server_mod} {{");
        let _ = writeln!(
            buf,
            "    //! Generated gorums server trait for `{svc_name}`."
        );
        let _ = writeln!(
            buf,
            "    pub trait {svc_name}Server: ::core::marker::Send + ::core::marker::Sync + 'static {{"
        );
        buf.push_str(&server_trait_fns);
        let _ = writeln!(buf, "    }}");
        let _ = writeln!(buf);
        let _ = writeln!(
            buf,
            "    pub fn register_{module_base}<S: {svc_name}Server>("
        );
        let _ = writeln!(buf, "        server: &mut ::quorums::server::Server,");
        let _ = writeln!(buf, "        svc: ::std::sync::Arc<S>,");
        let _ = writeln!(buf, "    ) {{");
        buf.push_str(&register_calls);
        let _ = writeln!(buf, "    }}");
        let _ = writeln!(buf, "}}");
    }
}

// ── Method handle emitter ─────────────────────────────────────────────────────

/// Emit a zero-sized method handle struct and its trait impl(s).
///
/// Each annotated call type implements the appropriate quorums method trait(s).
/// Multicast and QuorumCall each implement their "sibling" traits so the same
/// handle can be used for single-node calls as well — the protocol is identical.
fn emit_handle(
    out: &mut String,
    handle_name: &str,
    call_type: &CallType,
    req_ty: &str,
    resp_ty: &str,
    path: &str,
) {
    let _ = writeln!(out, "/// Typed method handle for `{path}`.");
    let _ = writeln!(out, "#[derive(Clone, Copy)]");
    let _ = writeln!(out, "pub struct {handle_name};");

    match call_type {
        CallType::RpcCall => {
            emit_rpc_call_impl(out, handle_name, req_ty, resp_ty, path);
        }
        CallType::Unicast => {
            emit_unicast_impl(out, handle_name, req_ty, path);
        }
        CallType::Multicast => {
            // Multicast also implements UnicastMethod — same one-way protocol.
            emit_multicast_impl(out, handle_name, req_ty, path);
            emit_unicast_impl(out, handle_name, req_ty, path);
        }
        CallType::QuorumCall => {
            // QuorumCall also implements OrderedQuorumCallMethod and RpcCallMethod
            // so the same handle can be used for all two-way call patterns.
            emit_qc_impl(out, handle_name, req_ty, resp_ty, path);
            emit_oqc_impl(out, handle_name, req_ty, resp_ty, path);
            emit_rpc_call_impl(out, handle_name, req_ty, resp_ty, path);
        }
        CallType::OrderedQuorumCall => {
            emit_oqc_impl(out, handle_name, req_ty, resp_ty, path);
            emit_qc_impl(out, handle_name, req_ty, resp_ty, path);
            emit_rpc_call_impl(out, handle_name, req_ty, resp_ty, path);
        }
        CallType::Correctable => {
            emit_correctable_impl(out, handle_name, req_ty, resp_ty, path);
        }
    }
}

fn emit_rpc_call_impl(out: &mut String, handle: &str, req: &str, resp: &str, path: &str) {
    let _ = writeln!(out, "impl ::quorums::RpcCallMethod for {handle} {{");
    let _ = writeln!(out, "    type Req = {req};");
    let _ = writeln!(out, "    type Resp = {resp};");
    let _ = writeln!(out, "    const PATH: &'static str = \"{path}\";");
    let _ = writeln!(out, "}}");
}

fn emit_unicast_impl(out: &mut String, handle: &str, req: &str, path: &str) {
    let _ = writeln!(out, "impl ::quorums::UnicastMethod for {handle} {{");
    let _ = writeln!(out, "    type Req = {req};");
    let _ = writeln!(out, "    const PATH: &'static str = \"{path}\";");
    let _ = writeln!(out, "}}");
}

fn emit_multicast_impl(out: &mut String, handle: &str, req: &str, path: &str) {
    let _ = writeln!(out, "impl ::quorums::MulticastMethod for {handle} {{");
    let _ = writeln!(out, "    type Req = {req};");
    let _ = writeln!(out, "    const PATH: &'static str = \"{path}\";");
    let _ = writeln!(out, "}}");
}

fn emit_qc_impl(out: &mut String, handle: &str, req: &str, resp: &str, path: &str) {
    let _ = writeln!(out, "impl ::quorums::QuorumCallMethod for {handle} {{");
    let _ = writeln!(out, "    type Req = {req};");
    let _ = writeln!(out, "    type Resp = {resp};");
    let _ = writeln!(out, "    const PATH: &'static str = \"{path}\";");
    let _ = writeln!(out, "}}");
}

fn emit_oqc_impl(out: &mut String, handle: &str, req: &str, resp: &str, path: &str) {
    let _ = writeln!(out, "impl ::quorums::OrderedQuorumCallMethod for {handle} {{");
    let _ = writeln!(out, "    type Req = {req};");
    let _ = writeln!(out, "    type Resp = {resp};");
    let _ = writeln!(out, "    const PATH: &'static str = \"{path}\";");
    let _ = writeln!(out, "}}");
}

fn emit_correctable_impl(out: &mut String, handle: &str, req: &str, resp: &str, path: &str) {
    let _ = writeln!(out, "impl ::quorums::CorrectableMethod for {handle} {{");
    let _ = writeln!(out, "    type Req = {req};");
    let _ = writeln!(out, "    type Resp = {resp};");
    let _ = writeln!(out, "    const PATH: &'static str = \"{path}\";");
    let _ = writeln!(out, "}}");
}

// ── Per-call-type client function emitters ────────────────────────────────────

fn client_fn_rpc(out: &mut String, fn_name: &str, req_ty: &str, resp_ty: &str, handle: &str) {
    let _ = writeln!(
        out,
        "    pub async fn {fn_name}(ctx: &::quorums::node::NodeContext, req: &{req_ty})"
    );
    let _ = writeln!(
        out,
        "        -> ::core::result::Result<{resp_ty}, ::quorums::Error>"
    );
    let _ = writeln!(out, "    {{");
    let _ = writeln!(
        out,
        "        ::quorums::call_types::rpc_call(ctx, req, super::{handle}).await"
    );
    let _ = writeln!(out, "    }}");
}

fn client_fn_unicast(out: &mut String, fn_name: &str, req_ty: &str, handle: &str) {
    let _ = writeln!(
        out,
        "    pub async fn {fn_name}(ctx: &::quorums::node::NodeContext, req: &{req_ty})"
    );
    let _ = writeln!(
        out,
        "        -> ::core::result::Result<(), ::quorums::Error>"
    );
    let _ = writeln!(out, "    {{");
    let _ = writeln!(
        out,
        "        ::quorums::call_types::unicast(ctx, req, super::{handle}).await"
    );
    let _ = writeln!(out, "    }}");
}

fn client_fn_multicast(out: &mut String, fn_name: &str, req_ty: &str, handle: &str) {
    let _ = writeln!(
        out,
        "    pub async fn {fn_name}(ctx: &::quorums::config::ConfigContext, req: &{req_ty})"
    );
    let _ = writeln!(
        out,
        "        -> ::core::result::Result<(), ::quorums::Error>"
    );
    let _ = writeln!(out, "    {{");
    let _ = writeln!(
        out,
        "        ::quorums::call_types::multicast(ctx, req, super::{handle}).await"
    );
    let _ = writeln!(out, "    }}");
}

fn client_fn_qc(out: &mut String, fn_name: &str, req_ty: &str, resp_ty: &str, handle: &str) {
    let _ = writeln!(
        out,
        "    pub async fn {fn_name}(ctx: &::quorums::config::ConfigContext, req: &{req_ty})"
    );
    let _ = writeln!(
        out,
        "        -> ::core::result::Result<::quorums::Responses<{resp_ty}>, ::quorums::Error>"
    );
    let _ = writeln!(out, "    {{");
    let _ = writeln!(
        out,
        "        ::quorums::call_types::quorum_call(ctx, req, super::{handle}).await"
    );
    let _ = writeln!(out, "    }}");
}

fn client_fn_oqc(out: &mut String, fn_name: &str, req_ty: &str, resp_ty: &str, handle: &str) {
    let _ = writeln!(
        out,
        "    pub async fn {fn_name}(ctx: &::quorums::config::ConfigContext, req: &{req_ty})"
    );
    let _ = writeln!(
        out,
        "        -> ::core::result::Result<::quorums::OrderedResponses<{resp_ty}>, ::quorums::Error>"
    );
    let _ = writeln!(out, "    {{");
    let _ = writeln!(
        out,
        "        ::quorums::call_types::ordered_quorum_call(ctx, req, super::{handle}).await"
    );
    let _ = writeln!(out, "    }}");
}

fn client_fn_correctable(out: &mut String, fn_name: &str, req_ty: &str, resp_ty: &str, handle: &str) {
    let _ = writeln!(
        out,
        "    pub async fn {fn_name}(ctx: &::quorums::config::ConfigContext, req: &{req_ty})"
    );
    let _ = writeln!(
        out,
        "        -> ::core::result::Result<::quorums::Correctable<{resp_ty}>, ::quorums::Error>"
    );
    let _ = writeln!(out, "    {{");
    let _ = writeln!(
        out,
        "        ::quorums::call_types::correctable_call(ctx, req, super::{handle}).await"
    );
    let _ = writeln!(out, "    }}");
}

// ── Per-call-type server trait function emitters ──────────────────────────────
//
// Trait methods use `-> impl Future<...> + Send` so that `register_handler`'s
// `Fut: Send` bound is satisfied.  Plain `async fn` in traits does not
// guarantee `Send` for the returned future.

fn server_trait_fn_twoway(out: &mut String, fn_name: &str, req_ty: &str, resp_ty: &str) {
    let _ = writeln!(
        out,
        "        fn {fn_name}(&self, ctx: ::quorums::server::ServerCtx, req: {req_ty})"
    );
    let _ = writeln!(
        out,
        "            -> impl ::std::future::Future<Output = ::core::result::Result<\
         ::core::option::Option<{resp_ty}>, ::tonic::Status>> + Send;"
    );
}

fn server_trait_fn_oneway(out: &mut String, fn_name: &str, req_ty: &str) {
    let _ = writeln!(
        out,
        "        fn {fn_name}(&self, ctx: ::quorums::server::ServerCtx, req: {req_ty})"
    );
    let _ = writeln!(
        out,
        "            -> impl ::std::future::Future<Output = ::core::result::Result<\
         ::core::option::Option<()>, ::tonic::Status>> + Send;"
    );
}

fn server_trait_fn_correctable(out: &mut String, fn_name: &str, req_ty: &str) {
    let _ = writeln!(
        out,
        "        fn {fn_name}(&self, ctx: ::quorums::server::ServerCtx, req: {req_ty})"
    );
    let _ = writeln!(
        out,
        "            -> impl ::std::future::Future<Output = ::core::result::Result<\
         (), ::tonic::Status>> + Send;"
    );
}

// ── Register call emitter ─────────────────────────────────────────────────────

fn register_handler(out: &mut String, path: &str, fn_name: &str, req_bare: &str, streaming: bool) {
    let register_fn = if streaming {
        "register_streaming_handler"
    } else {
        "register_handler"
    };
    let _ = writeln!(out, "        {{");
    let _ = writeln!(out, "            let s = ::std::sync::Arc::clone(&svc);");
    let _ = writeln!(
        out,
        "            server.{register_fn}(\"{path}\", move |ctx, req: super::{req_bare}| {{"
    );
    let _ = writeln!(out, "                let s = ::std::sync::Arc::clone(&s);");
    let _ = writeln!(
        out,
        "                async move {{ s.{fn_name}(ctx, req).await }}"
    );
    let _ = writeln!(out, "            }});");
    let _ = writeln!(out, "        }}");
}

// ── Utilities ─────────────────────────────────────────────────────────────────

/// Convert PascalCase to snake_case.
/// e.g. `Storage` → `storage`, `MyService` → `my_service`.
fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_ascii_lowercase());
    }
    result
}
