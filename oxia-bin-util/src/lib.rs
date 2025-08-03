use std::path::Path;

#[must_use]
pub fn path() -> &'static Path {
    Path::new(env!("OXIA_BIN_PATH"))
}
