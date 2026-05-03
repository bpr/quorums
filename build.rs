fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the gorums wire-protocol proto (server + client tonic stubs).
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/gorums.proto"], &["proto"])?;

    // Compile test message types AND generate gorums-typed wrappers.
    quorums_build::configure()
        .method("/storage.Storage/Read", quorums_build::CallType::QuorumCall)
        .method("/storage.Storage/Write", quorums_build::CallType::Multicast)
        .compile(&["tests/storage.proto"], &["tests"])?;

    Ok(())
}
