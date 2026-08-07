//! Task execution — run one-shot commands with skip detection and timeout.
//!
//! Tasks are short-lived: no PID files, no ready checks. They check
//! `TaskState::needs_run()`, spawn the command, and report success/failure.

use nix::sys::signal::Signal;
use tokio::time;

use crate::config::template::{self, TemplateError};
use crate::config::{Platform, Task};
use crate::duration::parse_duration;
use crate::sys::{ChildOutput, SpawnConfig, spawn_process};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::Duration;

/// Errors from task execution.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("process error: {0}")]
    Process(#[from] crate::sys::ProcessError),
    #[error("timed out after {timeout}")]
    Timeout { timeout: String },
    #[error("invalid duration: {0}")]
    Duration(#[from] crate::duration::DurationError),
    #[error("template error in {field}: {source}")]
    Template {
        field: String,
        #[source]
        source: TemplateError,
    },
}

/// Result of spawning a task process: the handle for waiting and the
/// child's output stream for processing.
pub(crate) struct TaskSpawn {
    pub handle: crate::sys::ProcessHandle,
    pub child_output: ChildOutput,
    pub rendered_cmdline: String,
}

struct PreparedTaskCommand {
    cmd: String,
    args: Vec<String>,
    work_dir: PathBuf,
    env: HashMap<String, String>,
    rendered_cmdline: String,
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
    params: &HashMap<String, String>,
) -> Result<TaskSpawn, TaskError> {
    let prepared = prepare_task_command(task, task_name, base_dir, platform, params)?;

    let (handle, child_output) = spawn_process(SpawnConfig {
        cmd: &prepared.cmd,
        args: &prepared.args,
        dir: Some(prepared.work_dir.as_path()),
        env: prepared.env,
        pgid_file_path: None,
        force_pipe: false,
        listen_fds: vec![],
    })
    .await?;

    Ok(TaskSpawn {
        handle,
        child_output,
        rendered_cmdline: prepared.rendered_cmdline,
    })
}

fn prepare_task_command(
    task: &Task,
    task_name: &str,
    base_dir: &Path,
    platform: Platform,
    params: &HashMap<String, String>,
) -> Result<PreparedTaskCommand, TaskError> {
    let render = |field: &str, s: &str| -> Result<String, TaskError> {
        template::render(s, params).map_err(|source| TaskError::Template {
            field: field.to_string(),
            source,
        })
    };

    // Render templates before resolving paths / env so a `{{name}}` in `dir`
    // flows through the same substitution the command sees.
    let rendered_dir: Option<std::path::PathBuf> = match task.dir.as_deref() {
        Some(d) => {
            let s = d.to_string_lossy();
            Some(render("dir", &s)?.into())
        }
        None => None,
    };
    let work_dir = match rendered_dir.as_deref() {
        Some(d) => base_dir.join(d),
        None => base_dir.to_path_buf(),
    };
    let work_dir = work_dir.as_path();

    let mut env: HashMap<String, String> = std::env::vars().collect();
    for (k, v) in &task.env {
        env.insert(k.clone(), render(&format!("env['{k}']"), v)?);
    }
    // Expose downloaded binaries on PATH.
    crate::sys::env::prepend_to_path(&mut env, &base_dir.join(".don").join("bin"));
    // Expose each param to the child as DON_PARAM_<NAME> so tasks can read
    // their own inputs without re-parsing placeholders. Intentionally
    // separate from the `{{name}}` substitution so the task author can
    // pick whichever interface fits the command better.
    for (k, v) in params {
        env.insert(format!("DON_PARAM_{}", k.to_ascii_uppercase()), v.clone());
    }

    // Resolve the command path, using the download binary if configured.
    let cache_base = base_dir.join(".don").join("cache");
    let resolved_cmd = task
        .resolved_cmd(platform, task_name, Some(&cache_base))
        .map_err(|msg| {
            TaskError::Process(crate::sys::ProcessError::Spawn {
                cmd: task.cmd.clone(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidInput, msg),
            })
        })?;
    // Templates in `cmd` apply to the string literal — when a download is
    // configured, the download binary path wins and placeholders in the
    // config's `cmd` are irrelevant (the binary is fixed).
    let cmd_str = if task.download.is_some() {
        resolved_cmd.to_string_lossy().into_owned()
    } else {
        render("cmd", &task.cmd)?
    };
    // Render each arg through the template engine.
    let args: Vec<String> = task
        .args
        .iter()
        .enumerate()
        .map(|(i, a)| render(&format!("args[{i}]"), a))
        .collect::<Result<_, _>>()?;

    let rendered_cmdline = crate::output::format_cmdline(&cmd_str, &args);
    Ok(PreparedTaskCommand {
        cmd: cmd_str,
        args,
        work_dir: work_dir.to_path_buf(),
        env,
        rendered_cmdline,
    })
}

/// Wait for a task to complete, with an optional timeout.
///
/// On timeout, the process group is killed and `TaskError::Timeout` is returned.
pub(crate) async fn wait_for_task(
    handle: &mut crate::sys::ProcessHandle,
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
