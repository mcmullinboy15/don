//! Service lifecycle management — start, stop, restart.
//!
//! Services are long-running processes with PID files, output capture,
//! and optional ready checks.

use crate::config::{ReadyCheck, ResolvedService, ShutdownConfig};
use crate::duration::parse_duration;
use crate::process::env::merge_env;
use crate::process::socket::BoundSockets;
use crate::process::{ProcessHandle, SpawnConfig, spawn_process};
use nix::sys::signal::Signal;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

/// A running service handle — either a local process or a Docker container.
pub enum ServiceHandle {
    /// A locally spawned process with its own process group.
    Process(ProcessHandle),
    /// A Docker container managed via the bollard API.
    Docker(crate::docker::DockerHandle),
}

/// Result of starting a service: the handle for lifecycle management
/// and the child's output stream for processing.
pub(crate) struct StartResult {
    pub handle: ServiceHandle,
    pub child_output: crate::process::ChildOutput,
}

/// Errors from service operations.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("process error: {0}")]
    Process(#[from] crate::process::ProcessError),
    #[error("env error: {0}")]
    Env(#[from] crate::process::env::EnvError),
    #[error("ready check failed after {retries} retries")]
    ReadyCheckExhausted { retries: u32 },
    #[error("process exited during ready check")]
    ProcessExitedDuringReadyCheck,
    #[error("ready check error: {0}")]
    ReadyCheckError(String),
    #[error("invalid duration: {0}")]
    Duration(#[from] crate::duration::DurationError),
    #[error("docker error: {0}")]
    Docker(String),
}

/// Start a service: merge env, spawn process.
///
/// Returns a `StartResult` containing the process handle and the child's
/// output stream. The caller is responsible for wiring up output processing,
/// the ready check, and state updates.
pub(crate) async fn start_service(
    name: &str,
    resolved: &ResolvedService,
    base_dir: &Path,
    pid_dir: &Path,
    bound_sockets: Option<&BoundSockets>,
    docker_client: Option<&bollard::Docker>,
    service_writer: Option<&crate::output::ServiceWriter>,
) -> Result<StartResult, ServiceError> {
    // Dispatch based on the service preset.
    if let Some(ref docker_config) = resolved.docker {
        // Docker preset: start a container via the Docker API.
        let client = docker_client.ok_or_else(|| {
            ServiceError::Docker("docker client not available".to_string())
        })?;
        let (handle, child_output) = crate::docker::start_docker_service(
            client,
            name,
            docker_config,
            &resolved.env,
            &resolved.env_file,
            base_dir,
            service_writer,
        )
        .await
        .map_err(|e| ServiceError::Docker(e.to_string()))?;
        return Ok(StartResult {
            handle: ServiceHandle::Docker(handle),
            child_output,
        });
    }

    // Custom preset: spawn a local process.
    let run_cmd = resolved
        .run
        .as_ref()
        .ok_or_else(|| crate::process::ProcessError::Spawn {
            cmd: name.to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "service has no run command (rust preset not yet implemented)",
            ),
        })?;

    // Merge environment. Inject LISTEN_FDS/LISTEN_FDNAMES if sockets are bound.
    let service_dir = resolved.dir.as_deref().unwrap_or(base_dir);
    let injected = bound_sockets
        .map(|s| s.listen_env())
        .unwrap_or_default();
    let (env, _warnings) = merge_env(
        name,
        Some(service_dir),
        &resolved.env_file,
        &resolved.env,
        &injected,
    )?;

    // Build PGID file path.
    std::fs::create_dir_all(pid_dir).map_err(crate::process::ProcessError::Io)?;
    let pgid_file_path = pid_dir.join(name);

    // Get raw fds to pass to the child (empty if no sockets).
    let listen_fds = bound_sockets
        .map(|s| s.raw_fds())
        .unwrap_or_default();

    // Spawn the process. Force pipe mode when passing listen fds
    // (pty-process doesn't expose pre_exec for fd placement).
    let (handle, child_output) = spawn_process(SpawnConfig {
        cmd: &run_cmd.cmd,
        args: &run_cmd.args,
        dir: Some(service_dir),
        env,
        pgid_file_path: Some(pgid_file_path),
        force_pipe: !listen_fds.is_empty(),
        listen_fds,
    })
    .await?;

    Ok(StartResult {
        handle: ServiceHandle::Process(handle),
        child_output,
    })
}

/// Run a ready check with retry loop.
///
/// Checks every `interval` up to `retries` times.
/// Returns `Ok(())` when the check passes, or `Err` when retries are exhausted.
pub(crate) async fn run_ready_check(ready: &ReadyCheck) -> Result<(), ServiceError> {
    let interval = parse_duration(&ready.interval)?;
    let retries = ready.retries;

    for attempt in 0..retries {
        if attempt > 0 {
            tokio::time::sleep(interval).await;
        }

        let check_result = if let Some(ref tcp_addr) = ready.tcp {
            check_tcp(tcp_addr).await
        } else if let Some(ref http_url) = ready.http {
            check_http(http_url).await
        } else if let Some(ref exec_cmd) = ready.exec {
            check_exec(exec_cmd).await
        } else {
            return Ok(());
        };

        if check_result.is_ok() {
            return Ok(());
        }
    }

    Err(ServiceError::ReadyCheckExhausted { retries })
}

/// TCP ready check: attempt to connect to the address.
async fn check_tcp(addr: &str) -> Result<(), ServiceError> {
    tokio::net::TcpStream::connect(addr)
        .await
        .map(|_| ())
        .map_err(|e| ServiceError::ReadyCheckError(format!("tcp connect failed: {e}")))
}

/// HTTP ready check: GET the URL and check for 2xx status.
async fn check_http(url: &str) -> Result<(), ServiceError> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| ServiceError::ReadyCheckError(format!("http request failed: {e}")))?;

    if resp.status().is_success() {
        Ok(())
    } else {
        Err(ServiceError::ReadyCheckError(format!(
            "http status {}",
            resp.status()
        )))
    }
}

/// Exec ready check: run the command, exit code 0 = ready.
async fn check_exec(cmd: &crate::config::Command) -> Result<(), ServiceError> {
    let env: HashMap<String, String> = std::env::vars().collect();
    let (mut handle, _output) = spawn_process(SpawnConfig {
        cmd: &cmd.cmd,
        args: &cmd.args,
        dir: None,
        env,
        pgid_file_path: None,
        force_pipe: true,
        listen_fds: vec![],
    })
    .await?;

    let status = handle.wait().await?;
    if status.success() {
        Ok(())
    } else {
        Err(ServiceError::ReadyCheckError(format!(
            "exec check exited with code {}",
            status.code().unwrap_or(-1)
        )))
    }
}

/// Parse a signal name string (e.g. "SIGTERM") into a nix Signal.
fn parse_signal(s: &str) -> Signal {
    match s {
        "SIGINT" => Signal::SIGINT,
        "SIGQUIT" => Signal::SIGQUIT,
        "SIGHUP" => Signal::SIGHUP,
        "SIGUSR1" => Signal::SIGUSR1,
        "SIGUSR2" => Signal::SIGUSR2,
        _ => Signal::SIGTERM,
    }
}

/// Stop a service: send signal, wait with timeout, escalate to SIGKILL.
pub(crate) async fn stop_service(
    mut handle: ServiceHandle,
    shutdown_config: Option<&ShutdownConfig>,
    force: bool,
) -> Result<(), ServiceError> {
    match handle {
        ServiceHandle::Process(ref mut process) => {
            let (signal, timeout) = if force {
                (Signal::SIGKILL, Duration::from_millis(500))
            } else {
                (
                    shutdown_config
                        .map(|c| parse_signal(&c.signal))
                        .unwrap_or(Signal::SIGTERM),
                    shutdown_config
                        .and_then(|c| parse_duration(&c.timeout).ok())
                        .unwrap_or(Duration::from_secs(10)),
                )
            };
            process.terminate(signal, timeout).await?;
        }
        ServiceHandle::Docker(ref mut docker) => {
            let (signal_name, timeout) = if force {
                ("SIGKILL", Duration::from_millis(500))
            } else {
                (
                    shutdown_config
                        .map(|c| c.signal.as_str())
                        .unwrap_or("SIGTERM"),
                    shutdown_config
                        .and_then(|c| parse_duration(&c.timeout).ok())
                        .unwrap_or(Duration::from_secs(10)),
                )
            };
            docker
                .stop(signal_name, timeout)
                .await
                .map_err(|e| ServiceError::Docker(e.to_string()))?;
        }
    }
    Ok(())
}
