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

    // Let's convert all paths into owned strings up-front: panic early
    let tools_src_dir = Path::new("go");
    let go_mod = tools_src_dir.join("go.mod").to_str().unwrap().to_string();
    let go_sum = tools_src_dir.join("go.sum").to_str().unwrap().to_string();

    let target_dir = get_target_dir().unwrap();
    let oxia_path = target_dir.join("oxia-bin").to_str().unwrap().to_string();

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
    assert!(status.success(), "oxia CLI installation failed");
    println!("cargo:warning=oxia CLI build finished in {install_time:.2?} seconds");
    println!("cargo:rerun-if-changed={go_mod}");
    println!("cargo:rerun-if-changed={go_sum}");
    println!("cargo:rustc-env=OXIA_BIN_PATH={oxia_path}");
}

fn main() {
    build_oxia_cli();
}
