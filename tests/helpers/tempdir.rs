use std::path::{Path, PathBuf};

/// A temporary directory that is automatically cleaned up on drop.
///
/// Paths are namespaced by PID to avoid collisions between parallel test runs.
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a new temp directory under the system temp dir.
    /// The `name` is used for human-readable identification in the path.
    pub fn new(name: &str) -> Self {
        let temp_root = if cfg!(target_os = "macos") {
            // macOS expands the default per-user temp dir to a long
            // /private/var/folders/... path when canonicalized. Don's API
            // socket lives under this directory in tests, and Darwin Unix
            // socket paths must fit in sockaddr_un.sun_path.
            PathBuf::from("/tmp")
        } else {
            std::env::temp_dir()
        };
        let path = temp_root
            .join("don-integration-test")
            .join(name)
            .join(format!("{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    /// The root path of this temp directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get a path to a child file or directory (does not create it).
    pub fn child(&self, relative: &str) -> PathBuf {
        self.path.join(relative)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
