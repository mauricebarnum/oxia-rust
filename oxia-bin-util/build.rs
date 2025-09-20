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
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process;
use std::process::ExitCode;
use std::process::{Command, Stdio};

use is_executable::IsExecutable;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

const OXIA_BIN: &str = "oxia";

// Derive .../target/{profile} from OUT_DIR
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

fn get_oxia_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    get_target_dir().map(|d| d.join("oxia"))
}

fn build_oxia_cli() -> io::Result<OsString> {
    const OXIA_MOD: &str = "github.com/oxia-db/oxia";
    let tools_src_dir = Path::new("go");
    let vendor_root = tools_src_dir.join("vendor");

    let oxia_dir = get_oxia_dir().unwrap();
    let oxia_path = oxia_dir.join(OXIA_BIN);
    let oxia_path_str = oxia_path.to_str().unwrap().to_string();

    let go_mod = tools_src_dir.join("go.mod").to_str().unwrap().to_string();
    let go_sum = tools_src_dir.join("go.sum").to_str().unwrap().to_string();

    // Make Cargo re-run when vendored code or go.mod/go.sum change.
    println!("cargo:rerun-if-changed={}", vendor_root.display());
    println!("cargo:rerun-if-changed={go_mod}");
    println!("cargo:rerun-if-changed={go_sum}");
    println!("cargo:rerun-if-changed={oxia_path_str}");

    // Compute a stable content hash of all relevant Go sources + go.mod/go.sum
    let mut hasher = Sha256::new();

    // Hash go.mod / go.sum
    hasher.update(fs::read(&go_mod)?);
    hasher.update(fs::read(&go_sum)?);

    // Hash all .go files under the vendored oxia module
    for entry in WalkDir::new(&vendor_root)
        .into_iter()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|e| e == "go") {
            hasher.update(fs::read(path)?);
        }
    }
    let new_hash = format!("{:x}", hasher.finalize());
    let hash_path = oxia_path.with_extension("sha256");
    let old_hash = fs::read_to_string(&hash_path).ok();

    let binary_missing = !oxia_path.exists();
    let hash_changed = old_hash.as_deref() != Some(&new_hash);

    if binary_missing || hash_changed {
        let status = Command::new("go")
            .current_dir(tools_src_dir)
            .env("GOPROXY", "off")
            .arg("build")
            .arg("-mod=vendor")
            .arg("-o")
            .arg(&oxia_path_str)
            .arg(format!("./vendor/{OXIA_MOD}/cmd"))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .expect("failed to run `go build` for oxia CLI");

        assert!(status.success(), "oxia build failed");
        fs::write(&hash_path, new_hash)?; // update hash after successful build
    }

    // Keep exporting the path to the built binary for consumers
    Ok(oxia_path_str.into())
}

fn find_oxia_in_path() -> Option<PathBuf> {
    env::split_paths(env::var_os("PATH").as_ref()?)
        .map(|d| d.join(OXIA_BIN))
        .find(|p| p.is_executable())
}

fn main() -> process::ExitCode {
    let oxia_bin = env::var_os("OXIA_BIN")
        .or_else(|| {
            if env::var_os("OXIA_BIN_IGNORE_PATH").is_some() {
                None
            } else {
                find_oxia_in_path().map(PathBuf::into)
            }
        })
        .or_else(|| Some(build_oxia_cli().unwrap()));

    let Some(oxia_bin) = oxia_bin else {
        return ExitCode::FAILURE;
    };

    println!("cargo:rerun-if-env-changed=OXIA_BIN_PATH");
    println!("cargo:rerun-if-env-changed=OXIA_BIN_IGNORE_PATH");
    println!("cargo:rerun-if-env-changed=PATH");
    println!("cargo:rustc-env=OXIA_BIN_PATH={}", oxia_bin.display());
    println!("cargo:warning=OXIA_BIN_PATH={}", oxia_bin.display());

    ExitCode::SUCCESS
}
