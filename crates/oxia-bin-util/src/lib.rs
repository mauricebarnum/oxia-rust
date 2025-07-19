use std::path::Path;

pub fn path() -> &'static Path {
    Path::new(env!("OXIA_BIN_PATH"))
}
