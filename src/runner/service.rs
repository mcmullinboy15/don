//! Service lifecycle management — start, stop, restart.
//!
//! Services are long-running processes with PID files, output capture,
//! and optional ready checks.

use crate::config::service::{GoConfig, RustConfig};
use crate::config::{Platform, ReadyCheck, ResolvedService, ShutdownConfig};
use crate::duration::parse_duration;
use crate::process::env::merge_env;
use crate::process::socket::BoundSockets;
use crate::process::{ProcessHandle, SpawnConfig, spawn_process};
use nix::sys::signal::Signal;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
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
    #[error("config error: {0}")]
    Config(String),
}

/// Start a service: merge env, spawn process.
///
/// Returns a `StartResult` containing the process handle and the child's
/// output stream. The caller is responsible for wiring up output processing,
/// the ready check, and state updates.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_service(
    name: &str,
    resolved: &ResolvedService,
    base_dir: &Path,
    pid_dir: &Path,
    bound_sockets: Option<&BoundSockets>,
    docker_client: Option<&bollard::Docker>,
    service_writer: Option<&crate::output::ServiceWriter>,
    platform: Platform,
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

    // Resolve working directory: join service's `dir` with base_dir so
    // relative paths like `./app` resolve correctly regardless of cwd.
    let service_dir_buf = match resolved.dir.as_deref() {
        Some(d) => base_dir.join(d),
        None => base_dir.to_path_buf(),
    };
    let service_dir = service_dir_buf.as_path();

    // Determine the run command and args based on preset.
    // For rust/go presets, the binary path is relative to the service's
    // working directory (where cargo/go build runs), not base_dir.
    let (cmd, args) = if let Some(ref rust_config) = resolved.rust {
        let binary_path = rust_binary_path(rust_config, service_dir);
        (binary_path.to_string_lossy().into_owned(), vec![])
    } else if let Some(ref go_config) = resolved.go {
        let binary_path = go_binary_path(go_config, name, service_dir);
        (binary_path.to_string_lossy().into_owned(), vec![])
    } else if resolved.run.is_some() {
        let cache_base = base_dir.join(".don").join("cache");
        let (executable, args) = resolved
            .resolved_run_cmd(platform, name, Some(&cache_base))
            .map_err(ServiceError::Config)?;
        (executable.to_string_lossy().into_owned(), args.to_vec())
    } else {
        return Err(crate::process::ProcessError::Spawn {
            cmd: name.to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "service has no run command or preset",
            ),
        }
        .into());
    };
    let injected = bound_sockets
        .map(|s| s.listen_env())
        .unwrap_or_default();
    let (mut env, _warnings) = merge_env(
        name,
        Some(service_dir),
        &resolved.env_file,
        &resolved.env,
        &injected,
    )?;
    // Expose downloaded binaries on PATH so other services/tasks can call them.
    crate::process::env::prepend_to_path(&mut env, &base_dir.join(".don").join("bin"));

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
        cmd: &cmd,
        args: &args,
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

// --- Preset build command and binary path helpers ---

/// Construct `cargo build` arguments from a RustConfig.
pub(crate) fn rust_build_args(config: &RustConfig) -> Vec<String> {
    let mut args = vec!["build".to_string(), "--bin".to_string(), config.binary.clone()];
    if !config.features.is_empty() {
        args.push("--features".to_string());
        args.push(config.features.join(","));
    }
    if config.release {
        args.push("--release".to_string());
    }
    if let Some(ref target_dir) = config.target_dir {
        args.push("--target-dir".to_string());
        args.push(target_dir.to_string_lossy().into_owned());
    }
    args.extend(config.extra_args.clone());
    args
}

/// Resolve the path to the built Rust binary.
pub(crate) fn rust_binary_path(config: &RustConfig, base_dir: &Path) -> PathBuf {
    let target_dir = config
        .target_dir
        .clone()
        .unwrap_or_else(|| base_dir.join("target"));
    let profile = if config.release { "release" } else { "debug" };
    target_dir.join(profile).join(&config.binary)
}

/// Construct `go build` arguments from a GoConfig.
pub(crate) fn go_build_args(config: &GoConfig, output_path: &Path) -> Vec<String> {
    let mut args = vec![
        "build".to_string(),
        "-o".to_string(),
        output_path.to_string_lossy().into_owned(),
    ];
    args.extend(config.build_flags.clone());
    if let Some(ref ldflags) = config.ldflags {
        args.push("-ldflags".to_string());
        args.push(ldflags.clone());
    }
    args.push(config.package.clone());
    args
}

/// Resolve the output path for a Go binary.
///
/// If `output` is set, uses that relative to `.don/bin/`.
/// Otherwise derives from the package path (last component).
pub(crate) fn go_binary_path(config: &GoConfig, service_name: &str, base_dir: &Path) -> PathBuf {
    let bin_dir = base_dir.join(".don").join("bin");
    let binary_name = config.output.clone().unwrap_or_else(|| {
        // Extract last component: "./cmd/api" → "api"
        Path::new(&config.package)
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| service_name.to_string())
    });
    bin_dir.join(binary_name)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_rust_build_args() {
        struct Case {
            name: &'static str,
            config: RustConfig,
            expected: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "minimal",
                config: RustConfig {
                    binary: "myapp".to_string(),
                    features: vec![],
                    release: false,
                    extra_args: vec![],
                    target_dir: None,
                },
                expected: vec!["build", "--bin", "myapp"],
            },
            Case {
                name: "full",
                config: RustConfig {
                    binary: "api".to_string(),
                    features: vec!["feat1".to_string(), "feat2".to_string()],
                    release: true,
                    extra_args: vec!["--jobs".to_string(), "4".to_string()],
                    target_dir: Some(PathBuf::from("./custom-target")),
                },
                expected: vec![
                    "build", "--bin", "api", "--features", "feat1,feat2",
                    "--release", "--target-dir", "./custom-target", "--jobs", "4",
                ],
            },
            Case {
                name: "release only",
                config: RustConfig {
                    binary: "server".to_string(),
                    features: vec![],
                    release: true,
                    extra_args: vec![],
                    target_dir: None,
                },
                expected: vec!["build", "--bin", "server", "--release"],
            },
        ];

        for case in cases {
            let result = rust_build_args(&case.config);
            let expected: Vec<String> = case.expected.iter().map(|s| s.to_string()).collect();
            assert_eq!(result, expected, "{}", case.name);
        }
    }

    #[test]
    fn test_rust_binary_path() {
        struct Case {
            name: &'static str,
            config: RustConfig,
            base_dir: &'static str,
            expected: &'static str,
        }

        let cases = vec![
            Case {
                name: "debug default target",
                config: RustConfig {
                    binary: "myapp".to_string(),
                    features: vec![],
                    release: false,
                    extra_args: vec![],
                    target_dir: None,
                },
                base_dir: "/project",
                expected: "/project/target/debug/myapp",
            },
            Case {
                name: "release default target",
                config: RustConfig {
                    binary: "myapp".to_string(),
                    features: vec![],
                    release: true,
                    extra_args: vec![],
                    target_dir: None,
                },
                base_dir: "/project",
                expected: "/project/target/release/myapp",
            },
            Case {
                name: "custom target dir",
                config: RustConfig {
                    binary: "api".to_string(),
                    features: vec![],
                    release: false,
                    extra_args: vec![],
                    target_dir: Some(PathBuf::from("/tmp/build")),
                },
                base_dir: "/project",
                expected: "/tmp/build/debug/api",
            },
        ];

        for case in cases {
            let result = rust_binary_path(&case.config, Path::new(case.base_dir));
            assert_eq!(result, PathBuf::from(case.expected), "{}", case.name);
        }
    }

    #[test]
    fn test_go_build_args() {
        struct Case {
            name: &'static str,
            config: GoConfig,
            output: &'static str,
            expected: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "minimal",
                config: GoConfig {
                    package: "./cmd/api".to_string(),
                    output: None,
                    build_flags: vec![],
                    ldflags: None,
                },
                output: "/tmp/bin/api",
                expected: vec!["build", "-o", "/tmp/bin/api", "./cmd/api"],
            },
            Case {
                name: "full",
                config: GoConfig {
                    package: "./cmd/server".to_string(),
                    output: Some("server".to_string()),
                    build_flags: vec!["-race".to_string()],
                    ldflags: Some("-X main.version=1.0".to_string()),
                },
                output: "/tmp/bin/server",
                expected: vec![
                    "build", "-o", "/tmp/bin/server", "-race",
                    "-ldflags", "-X main.version=1.0", "./cmd/server",
                ],
            },
        ];

        for case in cases {
            let result = go_build_args(&case.config, Path::new(case.output));
            let expected: Vec<String> = case.expected.iter().map(|s| s.to_string()).collect();
            assert_eq!(result, expected, "{}", case.name);
        }
    }

    #[test]
    fn test_go_binary_path() {
        struct Case {
            name: &'static str,
            config: GoConfig,
            service_name: &'static str,
            expected_suffix: &'static str,
        }

        let cases = vec![
            Case {
                name: "derived from package",
                config: GoConfig {
                    package: "./cmd/api".to_string(),
                    output: None,
                    build_flags: vec![],
                    ldflags: None,
                },
                service_name: "api-svc",
                expected_suffix: ".don/bin/api",
            },
            Case {
                name: "explicit output",
                config: GoConfig {
                    package: "./cmd/server".to_string(),
                    output: Some("my-server".to_string()),
                    build_flags: vec![],
                    ldflags: None,
                },
                service_name: "server",
                expected_suffix: ".don/bin/my-server",
            },
            Case {
                name: "root package falls back to service name",
                config: GoConfig {
                    package: ".".to_string(),
                    output: None,
                    build_flags: vec![],
                    ldflags: None,
                },
                service_name: "myapp",
                // "." has no file_name, but Path::new(".").file_name() is None on some platforms
                // The fallback should use the service name
                expected_suffix: ".don/bin/myapp",
            },
        ];

        for case in cases {
            let base = Path::new("/project");
            let result = go_binary_path(&case.config, case.service_name, base);
            assert!(
                result.ends_with(case.expected_suffix),
                "{}: expected to end with '{}', got '{}'",
                case.name,
                case.expected_suffix,
                result.display()
            );
        }
    }
}
