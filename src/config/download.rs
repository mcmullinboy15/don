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
    /// Name of the symlink created under `.don/bin/` for this artifact.
    /// When omitted, defaults to the binary's filename. Must be unique across
    /// the config — set this to disambiguate when two services/tasks download
    /// different versions of the same binary.
    pub bin_name: Option<String>,
}

impl DownloadConfig {
    /// Get the download artifact for a specific platform.
    pub fn for_platform(&self, platform: Platform) -> Option<&PlatformDownload> {
        self.platform.get(&platform)
    }

    /// The effective symlink name for this download: the explicit `bin_name`
    /// if set, otherwise the binary filename derived from the platform's
    /// `path` (or URL filename for bare binaries).
    pub fn effective_bin_name(&self, platform: Platform) -> Option<String> {
        if let Some(ref name) = self.bin_name {
            return Some(name.clone());
        }
        let artifact = self.for_platform(platform)?;
        artifact.derived_bin_name()
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
    /// Optional HTTP headers to send with the download request. Useful for
    /// private releases, signed URLs, or content negotiation. Values support
    /// `${VAR}` environment variable expansion.
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// Default base cache directory: .don/cache (project-local).
pub(crate) fn default_cache_base() -> PathBuf {
    PathBuf::from(".don").join("cache")
}

impl PlatformDownload {
    /// Composite cache key: sha256(url + "\n" + declared_sha256).
    ///
    /// Including the URL in the key means a URL change with a stale sha busts
    /// the cache, so the mismatch is caught loudly instead of silently using
    /// stale cached content.
    pub fn composite_hash(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(self.url.as_bytes());
        hasher.update(b"\n");
        hasher.update(self.sha256.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// The directory where this artifact is cached:
    /// `<cache_base>/<owner_name>/<composite_hash>/`.
    ///
    /// `owner_name` is the service or task name that owns this download — it
    /// namespaces the cache so the dir layout mirrors the config layout.
    pub fn cache_dir(&self, cache_base: &std::path::Path, owner_name: &str) -> PathBuf {
        cache_base.join(owner_name).join(self.composite_hash())
    }

    /// The full path to the downloaded binary.
    ///
    /// - If `path` is set (archive): `<cache_base>/<owner>/<hash>/<path>`
    /// - If `path` is not set (bare binary): `<cache_base>/<owner>/<hash>/<url-filename>`
    ///
    /// Returns `None` if the URL has no path component (shouldn't happen with valid URLs,
    /// but we don't panic on bad input).
    pub fn binary_path(
        &self,
        cache_base: &std::path::Path,
        owner_name: &str,
    ) -> Option<PathBuf> {
        let dir = self.cache_dir(cache_base, owner_name);
        match &self.path {
            Some(p) => Some(dir.join(p)),
            None => {
                let filename = self.url.rsplit('/').next().filter(|s| !s.is_empty())?;
                Some(dir.join(filename))
            }
        }
    }

    /// Derive the default symlink name from the binary filename: the last
    /// component of `path` for archives, or the URL's filename for bare
    /// binaries.
    pub fn derived_bin_name(&self) -> Option<String> {
        match &self.path {
            Some(p) => std::path::Path::new(p)
                .file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string()),
            None => self
                .url
                .split(['?', '#'])
                .next()
                .unwrap_or(&self.url)
                .rsplit('/')
                .next()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string()),
        }
    }
}
