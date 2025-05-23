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
        .compile_protos(&proto_files, &[proto_dir])?;

    // Recompile if proto files change
    proto_files
        .iter()
        .for_each(|f| println!("cargo:rerun-if-changed={}", f));

    Ok(())
}
