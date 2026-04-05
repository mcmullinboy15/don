//! Task execution — run one-shot commands with skip detection and timeout.
//!
//! Tasks are short-lived: no PID files, no ready checks. They check
//! `TaskState::needs_run()`, spawn the command, and report success/failure.

use nix::sys::signal::Signal;
use tokio::time;

use crate::config::{Platform, Task};
use crate::duration::parse_duration;
use crate::process::{ChildOutput, SpawnConfig, spawn_process};
use std::collections::HashMap;
use std::path::Path;
use std::process::ExitStatus;
use std::time::Duration;

/// Errors from task execution.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("process error: {0}")]
    Process(#[from] crate::process::ProcessError),
    #[error("timed out after {timeout}")]
    Timeout { timeout: String },
    #[error("invalid duration: {0}")]
    Duration(#[from] crate::duration::DurationError),
}

/// Result of spawning a task process: the handle for waiting and the
/// child's output stream for processing.
pub(crate) struct TaskSpawn {
    pub handle: crate::process::ProcessHandle,
    pub child_output: ChildOutput,
}

/// Spawn a task process. Does not wait for completion.
///
/// Resolves the task's command path using its download config (if any) so
/// that tasks with downloads run the cached binary. The caller is
/// responsible for wiring up output processing and calling `wait_for_task`
/// to get the exit status.
pub(crate) async fn spawn_task(
    task: &Task,
    task_name: &str,
    base_dir: &Path,
    platform: Platform,
) -> Result<TaskSpawn, TaskError> {
    let work_dir = task.dir.as_deref().unwrap_or(base_dir);

    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.extend(task.env.clone());
    // Expose downloaded binaries on PATH.
    crate::process::env::prepend_to_path(&mut env, &base_dir.join(".don").join("bin"));

    // Resolve the command path, using the download binary if configured.
    let cache_base = base_dir.join(".don").join("cache");
    let resolved_cmd = task
        .resolved_cmd(platform, task_name, Some(&cache_base))
        .map_err(|msg| {
            TaskError::Process(crate::process::ProcessError::Spawn {
                cmd: task.cmd.clone(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidInput, msg),
            })
        })?;
    let cmd_str = resolved_cmd.to_string_lossy().into_owned();

    let (handle, child_output) = spawn_process(SpawnConfig {
        cmd: &cmd_str,
        args: &task.args,
        dir: Some(work_dir),
        env,
        pgid_file_path: None,
        force_pipe: false,
        listen_fds: vec![],
    })
    .await?;

    Ok(TaskSpawn {
        handle,
        child_output,
    })
}

/// Wait for a task to complete, with an optional timeout.
///
/// On timeout, the process group is killed and `TaskError::Timeout` is returned.
pub(crate) async fn wait_for_task(
    handle: &mut crate::process::ProcessHandle,
    timeout_str: Option<&str>,
) -> Result<ExitStatus, TaskError> {
    if let Some(timeout_str) = timeout_str {
        let timeout = parse_duration(timeout_str)?;
        match time::timeout(timeout, handle.wait()).await {
            Ok(result) => Ok(result?),
            Err(_elapsed) => {
                let _ = handle
                    .terminate(Signal::SIGKILL, Duration::from_millis(500))
                    .await;
                Err(TaskError::Timeout {
                    timeout: timeout_str.to_string(),
                })
            }
        }
    } else {
        Ok(handle.wait().await?)
    }
}
