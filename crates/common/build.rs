fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "proto";
    let proto_files = vec![
        "proto/client.proto",
        // "proto/replication.proto",
        // "proto/storage.proto",
    ];

    // Configure tonic to use Bytes for all protobuf bytes fields
    tonic_build::configure()
        // Use `Bytes` for binary fields
        .bytes(["."])
        // Protoc optimizations
        .protoc_arg("--experimental_allow_proto3_optional")
        .protoc_arg("--cpp_opt=speed")
        .build_client(true)
        .build_server(false) // maybe? are going to implement a mock server?
        .emit_rerun_if_changed(true)
        .compile_well_known_types(false)
        .compile_protos(&proto_files, &[proto_dir])?;

    // Recompile if proto files change
    proto_files
        .iter()
        .for_each(|f| println!("cargo:rerun-if-changed={f}"));

    println!("cargo:rerun-if-changed={proto_dir}");
    println!("cargo:rerun-if-changed=Cargo.toml");

    Ok(())
}
