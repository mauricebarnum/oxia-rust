use std::env;
use std::fs::create_dir_all;
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
    let tools_target_dir = target_dir.join("oxia-bin");
    let oxia_path = tools_target_dir.join("oxia");

    let tools_src_dir = Path::new("go");

    let go_mod = tools_src_dir.join("go.mod");
    let go_sum = tools_src_dir.join("go.sum");

    let dirs = [
        ("GOCACHE", tools_target_dir.join("cache")),
        ("GOMODCACHE", tools_target_dir.join("mod")),
    ];

    dirs.iter().for_each(|(_, d)| {
        create_dir_all(d).unwrap();
    });

    let envs = {
        let mut envs = vec![("GOPROXY", "off")];
        envs.extend(dirs.iter().map(|(k, d)| (*k, d.to_str().unwrap())));
        envs
    };

    let start = Instant::now();
    let status = Command::new("go")
        .current_dir(tools_src_dir)
        .envs(envs)
        .arg("build")
        .arg("-v")
        .arg("-mod=vendor")
        .arg("-o")
        .arg(oxia_path.to_str().unwrap())
        .arg(format!("./vendor/{OXIA_MOD}/cmd"))
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("failed to run `go build` for oxia CLI");
    let install_time = start.elapsed();
    assert!(status.success(), "oxia CLI installation failed");
    println!("cargo:warning=oxia CLI build finished in {install_time:.2?} seconds");
    println!("cargo:rerun-if-changed={}", go_mod.to_str().unwrap());
    println!("cargo:rerun-if-changed={}", go_sum.to_str().unwrap());
    println!(
        "cargo:rustc-env=OXIA_BIN_PATH={}",
        oxia_path.to_str().unwrap()
    );
}

fn main() {
    build_oxia_cli();
}
