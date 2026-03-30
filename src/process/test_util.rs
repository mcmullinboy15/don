//! Shared test utilities for the process module.

use std::path::{Path, PathBuf};

pub(crate) struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub(crate) fn new(name: &str) -> Self {
        let path = std::env::temp_dir()
            .join("don-test-process")
            .join(name)
            .join(format!("{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
