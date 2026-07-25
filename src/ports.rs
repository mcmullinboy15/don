//! Runtime port manifest support.
//!
//! Don writes `.don/ports.json` so scripts and humans can discover the
//! addresses actually bound when `fallback_ports = true` or port `0` is used.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// On-disk runtime port manifest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortManifest {
    /// Schema version for future-compatible parsing.
    pub version: u32,
    /// Unix timestamp when the manifest was written.
    pub generated_at_unix_secs: u64,
    /// Per-service runtime port information.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub services: BTreeMap<String, ServicePorts>,
}

/// Runtime ports for one service.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServicePorts {
    /// Don proxy/listenfd public ports.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proxy: Vec<ProxyPort>,
    /// Docker host port mappings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub docker: Vec<DockerPort>,
}

/// One Don proxy/listenfd public port.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyPort {
    /// Address from `don.toml`.
    pub configured_addr: String,
    /// Address Don actually bound.
    pub bound_addr: String,
    /// Proxy mode: `env`, `listenfd`, or `forward`.
    pub mode: String,
    /// Env var name for env-mode proxies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// Fixed backend target for forward-mode proxies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

/// One Docker host port mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DockerPort {
    /// Mapping from `don.toml`, such as `5432:5432`.
    pub configured: String,
    /// Host address Docker actually bound, such as `0.0.0.0:5432`.
    pub host_addr: String,
    /// Container port.
    pub container_port: String,
    /// Transport protocol, currently `tcp` or `udp`.
    pub protocol: String,
}

/// Return `<base_dir>/.don/ports.json`.
pub fn manifest_path(base_dir: &Path) -> PathBuf {
    base_dir.join(".don").join("ports.json")
}

/// Return the Docker container name Don manages for a service.
///
/// Generated names are project-scoped when fallback ports are enabled so
/// concurrent worktrees do not clean up one another's containers.
pub fn managed_docker_container_name(
    base_dir: &Path,
    service_name: &str,
    config: &crate::config::DockerConfig,
    fallback_ports: bool,
) -> String {
    crate::docker::container_name(base_dir, service_name, config, fallback_ports)
}

/// Read the runtime port manifest for `base_dir`.
pub fn read_manifest(base_dir: &Path) -> Result<PortManifest, PortManifestError> {
    let path = manifest_path(base_dir);
    let content = std::fs::read_to_string(&path).map_err(|source| PortManifestError::Read {
        path: path.clone(),
        source,
    })?;
    serde_json::from_str(&content).map_err(|source| PortManifestError::Parse { path, source })
}

/// Write the runtime port manifest for `base_dir`.
///
/// The manifest is serialized to a sibling temporary file and renamed into
/// place so readers never observe a partially-written JSON document.
pub fn write_manifest(
    base_dir: &Path,
    mut manifest: PortManifest,
) -> Result<(), PortManifestError> {
    manifest.version = 1;
    manifest.generated_at_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());

    let path = manifest_path(base_dir);
    let parent = path
        .parent()
        .ok_or_else(|| PortManifestError::InvalidPath(path.clone()))?;
    std::fs::create_dir_all(parent).map_err(|source| PortManifestError::Write {
        path: parent.to_path_buf(),
        source,
    })?;

    let temporary_path = path.with_extension("json.tmp");
    let content = serde_json::to_string_pretty(&manifest)
        .map_err(|source| PortManifestError::Serialize { source })?;
    std::fs::write(&temporary_path, content).map_err(|source| PortManifestError::Write {
        path: temporary_path.clone(),
        source,
    })?;
    std::fs::rename(&temporary_path, &path)
        .map_err(|source| PortManifestError::Write { path, source })
}

/// Remove the runtime port manifest for `base_dir`.
///
/// A missing manifest is already clean and is treated as success.
pub fn remove_manifest(base_dir: &Path) -> Result<(), PortManifestError> {
    let path = manifest_path(base_dir);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(PortManifestError::Remove { path, source }),
    }
}

/// Errors from reading or writing `.don/ports.json`.
#[derive(Debug, thiserror::Error)]
pub enum PortManifestError {
    /// The manifest path had no parent directory.
    #[error("invalid port manifest path: {}", .0.display())]
    InvalidPath(PathBuf),
    /// The manifest could not be read.
    #[error("failed to read port manifest {}: {source}", path.display())]
    Read {
        /// Manifest path.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },
    /// The manifest contained invalid JSON.
    #[error("failed to parse port manifest {}: {source}", path.display())]
    Parse {
        /// Manifest path.
        path: PathBuf,
        /// Underlying JSON error.
        source: serde_json::Error,
    },
    /// The manifest could not be serialized.
    #[error("failed to serialize port manifest: {source}")]
    Serialize {
        /// Underlying JSON error.
        source: serde_json::Error,
    },
    /// The manifest or its parent directory could not be written.
    #[error("failed to write port manifest {}: {source}", path.display())]
    Write {
        /// Path that could not be written.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },
    /// The manifest file could not be removed.
    #[error("failed to remove port manifest {}: {source}", path.display())]
    Remove {
        /// Path that could not be removed.
        path: PathBuf,
        /// Underlying filesystem error.
        source: std::io::Error,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("don-ports-{name}-{}-{nonce}", std::process::id()));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn manifest_round_trip_and_remove() {
        let dir = TempDir::new("round-trip");
        let mut manifest = PortManifest::default();
        manifest.services.insert(
            "api".to_string(),
            ServicePorts {
                proxy: vec![ProxyPort {
                    configured_addr: "127.0.0.1:3000".to_string(),
                    bound_addr: "127.0.0.1:49152".to_string(),
                    mode: "env".to_string(),
                    env: Some("PORT".to_string()),
                    target: None,
                }],
                docker: Vec::new(),
            },
        );

        write_manifest(&dir.path, manifest.clone()).unwrap();
        let actual = read_manifest(&dir.path).unwrap();

        assert_eq!(actual.version, 1);
        assert!(actual.generated_at_unix_secs > 0);
        assert_eq!(actual.services, manifest.services);

        remove_manifest(&dir.path).unwrap();
        assert!(!manifest_path(&dir.path).exists());
        remove_manifest(&dir.path).unwrap();
    }
}
