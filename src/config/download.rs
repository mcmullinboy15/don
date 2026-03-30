use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

use super::platform::Platform;
use super::types::Command;

/// Download configuration with per-platform artifacts.
#[derive(Debug, Clone, Deserialize)]
pub struct DownloadConfig {
    /// Per-platform download artifacts. Keys are "{os}-{arch}" using Rust conventions:
    /// linux-x86_64, linux-aarch64, macos-x86_64, macos-aarch64, windows-x86_64, windows-aarch64.
    pub platform: HashMap<Platform, PlatformDownload>,
}

impl DownloadConfig {
    /// Get the download artifact for a specific platform.
    pub fn for_platform(&self, platform: Platform) -> Option<&PlatformDownload> {
        self.platform.get(&platform)
    }
}

/// A downloadable artifact for a specific platform.
#[derive(Debug, Clone, Deserialize)]
pub struct PlatformDownload {
    /// URL to download the artifact from.
    pub url: String,
    /// SHA-256 hash of the downloaded file.
    pub sha256: String,
    /// Path to the binary inside the archive (for .tar.gz, .zip).
    /// If not set, the downloaded file is treated as the binary itself.
    pub path: Option<String>,
    /// Optional setup command to run after download/extraction.
    /// Executed with cwd set to the cache directory for this artifact.
    /// Only runs once — don writes a marker file after successful setup.
    pub setup: Option<Command>,
}

/// Default base cache directory: .don/cache (project-local).
pub(crate) fn default_cache_base() -> PathBuf {
    PathBuf::from(".don").join("cache")
}

impl PlatformDownload {
    /// The directory where this artifact is cached: `<cache_base>/<sha256>/`.
    pub fn cache_dir(&self, cache_base: &std::path::Path) -> PathBuf {
        cache_base.join(&self.sha256)
    }

    /// The full path to the downloaded binary.
    ///
    /// - If `path` is set (archive): `<cache_base>/<sha256>/<path>`
    /// - If `path` is not set (bare binary): `<cache_base>/<sha256>/<filename from url>`
    ///
    /// Returns `None` if the URL has no path component (shouldn't happen with valid URLs,
    /// but we don't panic on bad input).
    pub fn binary_path(&self, cache_base: &std::path::Path) -> Option<PathBuf> {
        let dir = self.cache_dir(cache_base);
        match &self.path {
            Some(p) => Some(dir.join(p)),
            None => {
                let filename = self.url.rsplit('/').next().filter(|s| !s.is_empty())?;
                Some(dir.join(filename))
            }
        }
    }
}
