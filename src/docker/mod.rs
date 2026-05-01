//! Docker service lifecycle — container creation, starting, stopping, and log streaming.
//!
//! Uses the bollard crate to communicate with the Docker daemon via its Unix socket.
//! Each Docker service gets a [`DockerHandle`] that wraps the container ID and
//! provides stop/remove operations analogous to process cleanup.

pub(crate) mod build;
pub(crate) mod parse;
pub(crate) mod stream;

use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, LogsOptionsBuilder, RemoveContainerOptionsBuilder,
    StopContainerOptionsBuilder,
};
use std::collections::HashMap;
use std::time::Duration;

use crate::config::service::DockerConfig;
use crate::process::ChildOutput;
use stream::DockerLogReader;

/// Errors from Docker operations.
#[derive(Debug, thiserror::Error)]
pub enum DockerError {
    #[error("docker API error: {0}")]
    Api(#[from] bollard::errors::Error),
    #[error("docker build failed: {0}")]
    BuildFailed(String),
    #[error("failed to create build context: {0}")]
    Tar(#[source] std::io::Error),
    #[error("invalid port mapping '{0}': {1}")]
    InvalidPort(String, String),
    #[error("env file error: {0}")]
    EnvFile(#[source] std::io::Error),
}

/// Handle to a running Docker container.
///
/// Provides stop/remove operations analogous to [`crate::process::ProcessHandle`].
/// The container is identified by ID and name.
pub struct DockerHandle {
    client: Docker,
    container_id: String,
    container_name: String,
}

impl std::fmt::Debug for DockerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DockerHandle")
            .field("container_id", &self.container_id)
            .field("container_name", &self.container_name)
            .finish()
    }
}

impl DockerHandle {
    /// Stop the container with the given signal and timeout, then remove it.
    pub async fn stop(&mut self, signal: &str, timeout: Duration) -> Result<(), DockerError> {
        let timeout_secs = timeout.as_secs().max(1) as i32;
        let stop_options = StopContainerOptionsBuilder::new()
            .signal(signal)
            .t(timeout_secs)
            .build();
        // Stop error is intentionally ignored — container may already be stopped.
        // We proceed to force-remove regardless.
        let _ = self
            .client
            .stop_container(&self.container_id, Some(stop_options))
            .await;
        let remove_options = RemoveContainerOptionsBuilder::new().force(true).build();
        self.client
            .remove_container(&self.container_id, Some(remove_options))
            .await?;
        Ok(())
    }

    /// Force-remove the container (for cleanup).
    pub async fn remove(&self) -> Result<(), DockerError> {
        let options = RemoveContainerOptionsBuilder::new().force(true).build();
        self.client
            .remove_container(&self.container_id, Some(options))
            .await?;
        Ok(())
    }
}

/// Start a Docker service: clean up stale containers, create, start, stream logs.
///
/// Returns a `DockerHandle` for lifecycle management and a `ChildOutput` for
/// log streaming (compatible with the existing output system).
pub(crate) async fn start_docker_service(
    client: &Docker,
    name: &str,
    config: &DockerConfig,
    service_env: &HashMap<String, String>,
    env_files: &[std::path::PathBuf],
    base_dir: &std::path::Path,
    writer: Option<&crate::output::ServiceWriter>,
) -> Result<(DockerHandle, ChildOutput), DockerError> {
    let container_name = config
        .container
        .clone()
        .unwrap_or_else(|| format!("don-{name}"));

    // Clean up any stale container with the same name.
    cleanup_stale_container(client, &container_name).await?;

    // Build the image if a build config is present.
    if let Some(ref build_config) = config.build
        && let Some(w) = writer
    {
        build::build_image(client, build_config, &config.image, base_dir, w).await?;
    }

    // Build container configuration.
    let (port_bindings, exposed_ports) = parse::parse_port_mappings(&config.ports)?;
    let env_vars = parse::build_env_vars(service_env, env_files)?;

    let container_config = ContainerCreateBody {
        image: Some(config.image.clone()),
        env: Some(env_vars),
        exposed_ports: if exposed_ports.is_empty() {
            None
        } else {
            Some(exposed_ports.into_iter().collect())
        },
        cmd: if config.command.is_empty() {
            None
        } else {
            Some(config.command.clone())
        },
        host_config: Some(HostConfig {
            port_bindings: if port_bindings.is_empty() {
                None
            } else {
                Some(port_bindings)
            },
            binds: if config.volumes.is_empty() {
                None
            } else {
                Some(config.volumes.clone())
            },
            network_mode: config.network.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Create container.
    let create_options = CreateContainerOptionsBuilder::new()
        .name(&container_name)
        .build();
    let response = client
        .create_container(Some(create_options), container_config)
        .await?;
    let container_id = response.id;

    // Start container.
    client.start_container(&container_id, None).await?;

    // Start log streaming.
    let log_options = LogsOptionsBuilder::new()
        .follow(true)
        .stdout(true)
        .stderr(true)
        .build();
    let log_stream = client.logs(&container_id, Some(log_options));
    let log_reader = DockerLogReader::new(Box::pin(log_stream));
    let child_output = ChildOutput::DockerLogs(log_reader);

    let handle = DockerHandle {
        client: client.clone(),
        container_id,
        container_name,
    };

    Ok((handle, child_output))
}

/// Clean up a stale container by name (from a previous don run that crashed).
///
/// Returns `Ok(true)` if a container was found and removed, `Ok(false)` if
/// no container by that name existed.
pub(crate) async fn cleanup_stale_container(
    client: &Docker,
    name: &str,
) -> Result<bool, DockerError> {
    match client.inspect_container(name, None).await {
        Ok(_) => {
            // Container exists — stop and remove it.
            let stop_options = StopContainerOptionsBuilder::new().t(5).build();
            let _ = client.stop_container(name, Some(stop_options)).await;
            let remove_options = RemoveContainerOptionsBuilder::new().force(true).build();
            client.remove_container(name, Some(remove_options)).await?;
            Ok(true)
        }
        Err(bollard::errors::Error::DockerResponseServerError {
            status_code: 404, ..
        }) => Ok(false),
        Err(e) => Err(DockerError::Api(e)),
    }
}
