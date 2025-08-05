// Copyright 2025 Maurice S. Barnum
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

use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

fn get_target_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let profile = env::var("PROFILE")?;
    let mut sub_path = out_dir.as_path();

    while let Some(parent) = sub_path.parent() {
        if parent.ends_with(&profile) {
            // parent is .../target/{profile}
            return Ok(parent.parent().unwrap().to_path_buf());
        }
        sub_path = parent;
    }
    Err("Could not find target directory root".into())
}

fn build_oxia_cli() {
    const OXIA_MOD: &str = "github.com/oxia-db/oxia";

    let target_dir = get_target_dir().unwrap();
    let oxia_path = target_dir.join("oxia-bin");
    let build_oxia = !oxia_path.exists();

    // Let's convert all paths into owned strings up-front: panic early
    let oxia_path = oxia_path.to_str().unwrap().to_string();
    let tools_src_dir = Path::new("go");
    let go_mod = tools_src_dir.join("go.mod").to_str().unwrap().to_string();
    let go_sum = tools_src_dir.join("go.sum").to_str().unwrap().to_string();

    if build_oxia {
        let start = Instant::now();
        let status = Command::new("go")
            .current_dir(tools_src_dir)
            .env("GOPROXY", "off")
            .arg("build")
            .arg("-v")
            .arg("-mod=vendor")
            .arg("-o")
            .arg(&oxia_path)
            .arg(format!("./vendor/{OXIA_MOD}/cmd"))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .expect("failed to run `go build` for oxia CLI");
        let install_time = start.elapsed();
        println!("cargo:warning=oxia build finished in {install_time:.2?} seconds");
        assert!(status.success(), "oxia build failed");
    }
    println!("cargo:rerun-if-changed={go_mod}");
    println!("cargo:rerun-if-changed={go_sum}");
    println!("cargo:rustc-env=OXIA_BIN_PATH={oxia_path}");
}

fn main() {
    build_oxia_cli();
}
