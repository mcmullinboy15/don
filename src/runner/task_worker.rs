use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::service_worker::ensure_download_for_config_worker;
use super::task;
use crate::config::{Platform, TaskAutoRun};
use crate::task_state::TaskState;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Copy)]
pub(in crate::runner) enum TaskRunMode {
    Startup { has_dependents: bool },
    Triggered,
}

pub(in crate::runner) enum TaskRunPrepared {
    PendingRun { message: String },
    Skipped { message: String },
    Spawned(Box<task::TaskSpawn>),
}

pub(in crate::runner) struct TaskWorkerContext {
    pub(in crate::runner) base_dir: PathBuf,
    pub(in crate::runner) platform: Platform,
    pub(in crate::runner) emitter: crate::output::LifecycleEmitter,
    pub(in crate::runner) global_watch_ignore: Vec<String>,
}

pub(in crate::runner) async fn run_task_worker(
    ctx: TaskWorkerContext,
    name: &str,
    task_cfg: &crate::config::Task,
    params: &HashMap<String, String>,
    mode: TaskRunMode,
) -> Result<TaskRunPrepared, String> {
    let TaskWorkerContext {
        base_dir,
        platform,
        emitter,
        global_watch_ignore,
    } = ctx;
    if let TaskRunMode::Startup { has_dependents } = mode {
        let has_watch = !task_cfg.watch.is_empty();
        let watch_base = working_dir_for(&base_dir, task_cfg.dir.as_deref());
        let ignore_patterns = resolve_watch_ignore_patterns(
            &watch_base,
            &task_cfg.ignore,
            &base_dir,
            &global_watch_ignore,
        );
        let task_state = TaskState::new(base_dir.join(".don").join("task-state"));
        let needs_watch_run = if has_watch {
            task_state
                .needs_run(name, &task_cfg.watch, &ignore_patterns, Some(&watch_base))
                .await
                .unwrap_or(true)
        } else {
            false
        };
        let has_success = task_state.has_success(name).await.unwrap_or(false);
        if has_watch && !needs_watch_run {
            return Ok(TaskRunPrepared::Skipped {
                message: "skipped (no changes)".to_string(),
            });
        }

        let should_run_or_prompt = if !task_cfg.params.is_empty() {
            needs_watch_run || (!has_success && has_dependents)
        } else {
            match task_cfg.auto_run {
                TaskAutoRun::Always => !has_watch || needs_watch_run,
                TaskAutoRun::Never => !has_success && (has_dependents || needs_watch_run),
                TaskAutoRun::Once => !has_success || needs_watch_run,
            }
        };
        if !should_run_or_prompt {
            return Ok(TaskRunPrepared::Skipped {
                message: "skipped (not needed)".to_string(),
            });
        }
        if !task_cfg.params.is_empty() {
            return Ok(TaskRunPrepared::PendingRun {
                message: if has_dependents {
                    "pending — required by dependents, task has params".to_string()
                } else {
                    "pending — watch inputs changed, task has params".to_string()
                },
            });
        }
        if !task_cfg.auto_run.runs_automatically_on_startup(has_success) {
            return Ok(TaskRunPrepared::PendingRun {
                message: match task_cfg.auto_run {
                    TaskAutoRun::Always => "pending — run manually".to_string(),
                    TaskAutoRun::Never => {
                        if has_dependents {
                            "pending — required by dependents, run manually".to_string()
                        } else {
                            "pending — watch inputs changed, auto_run = false".to_string()
                        }
                    }
                    TaskAutoRun::Once => {
                        if has_dependents {
                            "pending — required by dependents, auto_run = once".to_string()
                        } else {
                            "pending — watch inputs changed, auto_run = once".to_string()
                        }
                    }
                },
            });
        }
    }

    ensure_download_for_config_worker(
        &base_dir,
        platform,
        name,
        task_cfg.download.as_ref(),
        None,
        &emitter,
    )
    .await
    .map_err(|e| format!("download failed: {e}"))?;

    let spawn = task::spawn_task(task_cfg, name, &base_dir, platform, params)
        .await
        .map_err(|e| e.to_string())?;
    Ok(TaskRunPrepared::Spawned(Box::new(spawn)))
}
