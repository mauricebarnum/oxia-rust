// Copyright 2025-2026 Maurice S. Barnum
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO_DIR: &str = "../ext/oxia/common/proto";
    const PROTO_FILES: &[&str] = &["client.proto"];

    // Configure tonic to use Bytes for all protobuf bytes fields
    tonic_prost_build::configure()
        .bytes(".")
        .protoc_arg("--experimental_allow_proto3_optional")
        .protoc_arg("--cpp_opt=speed")
        .build_client(true)
        .build_server(false)
        .emit_rerun_if_changed(true)
        .compile_protos(PROTO_FILES, &[PROTO_DIR])?;

    // Recompile if proto files change
    let proto_dir = Path::new(PROTO_DIR);
    for file in PROTO_FILES {
        let p = Path::join(proto_dir, file);
        println!("cargo:rerun-if-changed={}", p.display());
    }

    println!("cargo:rerun-if-changed={PROTO_DIR}");
    println!("cargo:rerun-if-changed=Cargo.toml");

    Ok(())
}
