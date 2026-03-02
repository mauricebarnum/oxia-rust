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
    const PROTO_DIRS: &[&str] = &["../ext/oxia/common/proto", "../ext/google-protos"];
    const PROTO_FILES: &[&str] = &["client.proto", "admin.proto", "google/rpc/status.proto"];

    // Configure tonic to use Bytes for all protobuf bytes fields
    let mut prost_config = prost_build::Config::new();
    prost_config.enable_type_names();

    tonic_prost_build::configure()
        .bytes(".")
        .protoc_arg("--experimental_allow_proto3_optional")
        .protoc_arg("--cpp_opt=speed")
        .build_client(true)
        .build_server(false)
        .emit_rerun_if_changed(true)
        .compile_with_config(prost_config, PROTO_FILES, PROTO_DIRS)?;

    // Recompile if proto files change
    for file in PROTO_FILES {
        let found_path = PROTO_DIRS
            .iter()
            .map(|dir| Path::new(dir).join(file))
            .find(|path| path.exists());

        if let Some(p) = found_path {
            println!("cargo:rerun-if-changed={}", p.display());
        } else {
            panic!("Proto file not found in any search directory: {}", file);
        }
    }

    println!("cargo:rerun-if-changed=Cargo.toml");

    Ok(())
}
