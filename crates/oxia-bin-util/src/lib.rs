use std::path::PathBuf;

pub fn path() -> PathBuf {
    PathBuf::from(env!("OXIA_BIN_PATH"))
}
