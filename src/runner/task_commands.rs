use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::task;
use super::task_worker::{TaskRunMode, TaskRunPrepared, TaskWorkerContext, run_task_worker};
use super::{
    CommandError, CommandResult, ItemDone, NodeKind, Runner, RunnerEvent, RunnerInternalCommand,
    TaskItemState, TaskRunIntent, resolve_task_params,
};
use crate::config::TaskAutoRun;
use crate::task_state::TaskState;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

impl Runner {
    pub(in crate::runner) fn spawn_task_worker(
        &mut self,
        name: &str,
        task_cfg: crate::config::Task,
        params: HashMap<String, String>,
        mode: TaskRunMode,
        intent: TaskRunIntent,
    ) -> Result<(), CommandError> {
        let Some(rt) = self.tasks.get_mut(name) else {
            return Err(CommandError::UnknownTask {
                name: name.to_string(),
            });
        };
        rt.run_generation = rt.run_generation.saturating_add(1);
        let op_id = rt.run_generation;

        let cmd_tx = self.internal_tx.clone();
        let base_dir = self.base_dir.clone();
        let platform = self.platform;
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let name_owned = name.to_string();
        let task_cfg_for_worker = task_cfg.clone();
        let global_watch_ignore = self.config.watch_ignore.clone();
        let worker = tokio::spawn(async move {
            let ctx = TaskWorkerContext {
                base_dir,
                platform,
                emitter,
                global_watch_ignore,
            };
            let result =
                run_task_worker(ctx, &name_owned, &task_cfg_for_worker, &params, mode).await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::TaskRunPrepared {
                    name: name_owned,
                    op_id,
                    task_cfg: Box::new(task_cfg),
                    intent,
                    result,
                })
                .await;
        });
        rt.run_worker = Some(worker);
        Ok(())
    }

    pub(in crate::runner) async fn handle_task_run_prepared(
        &mut self,
        name: &str,
        op_id: u64,
        task_cfg: &crate::config::Task,
        intent: TaskRunIntent,
        result: Result<TaskRunPrepared, String>,
    ) {
        let is_current = self
            .tasks
            .get(name)
            .is_some_and(|rt| rt.run_generation == op_id);
        if !is_current {
            if let Ok(TaskRunPrepared::Spawned(spawn)) = result {
                let task::TaskSpawn {
                    handle,
                    child_output,
                    rendered_cmdline: _rendered_cmdline,
                } = *spawn;
                drop(child_output);
                tokio::spawn(async move {
                    let mut handle = handle;
                    let _ = handle
                        .terminate(
                            nix::sys::signal::Signal::SIGKILL,
                            std::time::Duration::from_millis(500),
                        )
                        .await;
                });
            }
            return;
        }
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.run_worker = None;
        }

        match result {
            Ok(TaskRunPrepared::PendingRun { message }) => {
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.set_needs_run_now(true);
                }
                self.set_task_state(name, TaskItemState::PendingRun);
                self.output_manager.service_event(name, &message);
                if let TaskRunIntent::Startup { done_tx } = intent {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name.to_string(),
                            kind: NodeKind::Task,
                            success: true,
                            message: None,
                            elapsed: None,
                            task_run_generation: None,
                        })
                        .await;
                }
            }
            Ok(TaskRunPrepared::Skipped { message }) => {
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.set_needs_run_now(false);
                }
                self.set_task_state(name, TaskItemState::Skipped);
                self.output_manager.service_debug_event(name, &message);
                if let TaskRunIntent::Startup { done_tx } = intent {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name.to_string(),
                            kind: NodeKind::Task,
                            success: true,
                            message: None,
                            elapsed: None,
                            task_run_generation: None,
                        })
                        .await;
                }
            }
            Ok(TaskRunPrepared::Spawned(spawn)) => {
                if matches!(intent, TaskRunIntent::Startup { .. })
                    && let Some(rt) = self.tasks.get_mut(name)
                {
                    rt.set_needs_run_now(true);
                }
                self.output_manager.service_debug_event(
                    name,
                    &format!("process spawned (pid {})", spawn.handle.pgid()),
                );
                self.output_manager
                    .service_event(name, &format!("spawn {}", spawn.rendered_cmdline));
                let done_tx = match intent {
                    TaskRunIntent::Startup { done_tx } => {
                        self.output_manager.service_event(name, "running...");
                        self.set_task_state(name, TaskItemState::Running);
                        Some(done_tx)
                    }
                    TaskRunIntent::Background => None,
                };
                self.wire_task_output_and_wait(name, *spawn, task_cfg, done_tx)
                    .await;
            }
            Err(message) => {
                if matches!(intent, TaskRunIntent::Startup { .. })
                    && let Some(rt) = self.tasks.get_mut(name)
                {
                    rt.set_needs_run_now(true);
                }
                self.set_task_state(name, TaskItemState::Failed);
                self.output_manager.service_error_event(name, &message);
                match intent {
                    TaskRunIntent::Startup { done_tx } => {
                        let _ = done_tx
                            .send(ItemDone {
                                name: name.to_string(),
                                kind: NodeKind::Task,
                                success: false,
                                message: Some(message),
                                elapsed: None,
                                task_run_generation: None,
                            })
                            .await;
                    }
                    TaskRunIntent::Background => {
                        let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                            name: name.to_string(),
                            success: false,
                        });
                    }
                }
            }
        }
    }

    /// Wire up a spawned task's output and wait for completion.
    ///
    /// Starts output capture, spawns a background task to wait for exit,
    /// records success in task state, and sends completion events.
    /// - If `done_tx` is `Some`, sends `ItemDone` (initial startup path).
    /// - If `done_tx` is `None`, sends `TaskRerunComplete` (file-watch rerun path).
    async fn wire_task_output_and_wait(
        &mut self,
        name: &str,
        spawn: task::TaskSpawn,
        task_cfg: &crate::config::Task,
        done_tx: Option<mpsc::Sender<ItemDone>>,
    ) {
        let task::TaskSpawn {
            mut handle,
            child_output,
            rendered_cmdline: _rendered_cmdline,
        } = spawn;

        let pgid = handle.pgid();

        // Add OSC response sink if we have a PTY write handle.
        if let Some(pty) = handle.take_pty_write()
            && let Some(osc_handle) = self.output_manager.add_osc_sink(name, pty).await
            && let Some(rt) = self.tasks.get_mut(name)
        {
            rt.osc_sink = Some(osc_handle);
        }

        if let Some(rt) = self.tasks.get_mut(name) {
            rt.pgid = Some(pgid);
        }

        // Fulfill any pending attach waiter for this task.
        self.fulfill_pending_waiter(name).await;

        if let Some(svc_writer) = self.output_manager.service_writer(name) {
            tokio::spawn(async move {
                let _ = svc_writer.process_stream(child_output).await;
            });
        }

        let name_owned = name.to_string();
        let task_cfg_clone = task_cfg.clone();
        let base_dir_owned = self.base_dir.clone();
        let global_watch_ignore = self.config.watch_ignore.clone();
        let task_state = TaskState::new(base_dir_owned.join(".don").join("task-state"));
        let cmd_tx = self.internal_tx.clone();
        let rerun = done_tx.is_none();

        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = task::wait_for_task(&mut handle, task_cfg_clone.timeout.as_deref()).await;
            let elapsed = start.elapsed();

            let (success, message) = match result {
                Ok(status) => {
                    if status.success() {
                        let task_dir =
                            working_dir_for(&base_dir_owned, task_cfg_clone.dir.as_deref());
                        let ignore_patterns = resolve_watch_ignore_patterns(
                            &task_dir,
                            &task_cfg_clone.ignore,
                            &base_dir_owned,
                            &global_watch_ignore,
                        );
                        let _ = task_state
                            .record_success(
                                &name_owned,
                                &task_cfg_clone.watch,
                                &ignore_patterns,
                                Some(&task_dir),
                            )
                            .await;
                        (true, None)
                    } else {
                        let code = status.code().unwrap_or(-1);
                        (false, Some(format!("exit code {code}")))
                    }
                }
                Err(e) => (false, Some(e.to_string())),
            };

            if let Some(done_tx) = done_tx {
                let _ = done_tx
                    .send(ItemDone {
                        name: name_owned,
                        kind: NodeKind::Task,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        task_run_generation: None,
                    })
                    .await;
            } else {
                let _ = cmd_tx
                    .send(RunnerInternalCommand::TaskExited {
                        name: name_owned,
                        pgid,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        rerun,
                    })
                    .await;
            }
        });
    }

    async fn stop_task_pgid(&mut self, name: &str, pgid: i32) -> CommandResult {
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        self.output_manager
            .service_event(name, "stopping... (requested)");

        match nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pgid),
            nix::sys::signal::Signal::SIGKILL,
        ) {
            Ok(()) | Err(nix::Error::ESRCH) => {}
            Err(e) => {
                return Err(CommandError::Failed {
                    name: name.to_string(),
                    message: format!("failed to kill task pgid {pgid}: {e}"),
                });
            }
        }

        Ok(())
    }

    pub(in crate::runner) async fn handle_restart_task_cmd(&mut self, name: &str) -> CommandResult {
        let (task_cfg, last_params, state, pgid) = match self.tasks.get(name) {
            Some(rt) => (
                rt.config.clone(),
                rt.last_params.clone(),
                rt.state(),
                rt.pgid,
            ),
            None => {
                return Err(CommandError::UnknownTask {
                    name: name.to_string(),
                });
            }
        };

        if !task_cfg.params.is_empty() && last_params.len() < task_cfg.params.len() {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "task has params and no previous invocation to restart; use `don run`"
                    .to_string(),
            });
        }

        if matches!(state, TaskItemState::Running | TaskItemState::Building)
            && let Some(pgid) = pgid
        {
            self.stop_task_pgid(name, pgid).await?;
        }

        self.spawn_task_rerun(name, &task_cfg, &last_params, "restarting (manual trigger)")
            .await;
        Ok(())
    }

    /// Handle a file-watch-triggered task re-run.
    ///
    /// Respects the task's auto-run policy — tasks that should not auto-rerun
    /// from a watch event transition to `PendingRun` instead of spawning.
    /// Explicit-run paths (the user triggering a task via `don run <name>` or
    /// `--all-pending`) bypass this gate by calling [`spawn_task_rerun`]
    /// directly.
    pub(in crate::runner) async fn handle_task_rerun(&mut self, name: &str) {
        let task_cfg = match self.tasks.get(name) {
            Some(rt) => rt.config.clone(),
            None => {
                self.output_manager
                    .service_error_event(name, "rerun requested for unknown task");
                let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                    name: name.to_string(),
                    success: false,
                });
                return;
            }
        };

        if self
            .tasks
            .get(name)
            .is_some_and(|rt| rt.state() == TaskItemState::Building)
        {
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        // Skip the needs_run hash check — the file watcher already confirmed
        // a matching file changed. The hash check is only needed at startup
        // (to skip tasks whose inputs haven't changed since the last run).

        // Only `auto_run = true` / `"always"` allows watch-triggered reruns.
        // `"once"` is intentionally startup-only, and `false` / `"never"`
        // keeps the task manual forever.
        if !task_cfg.auto_run.runs_automatically_on_watch() {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.set_needs_run_now(true);
            }
            self.set_task_state(name, TaskItemState::PendingRun);
            let message = match task_cfg.auto_run {
                TaskAutoRun::Always => "files changed (pending)",
                TaskAutoRun::Never => "files changed (pending — auto_run = false)",
                TaskAutoRun::Once => "files changed (pending — auto_run = once)",
            };
            self.output_manager.service_event(name, message);
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        // Tasks that declare params require user-supplied values. File-watch
        // triggers park them in PendingRun so the user can run them explicitly
        // (via the palette's form or `don run <task> --<param>=<value>`).
        if !task_cfg.params.is_empty() {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.set_needs_run_now(true);
            }
            self.set_task_state(name, TaskItemState::PendingRun);
            self.output_manager.service_event(
                name,
                "files changed (pending — task has params, run manually)",
            );
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        self.spawn_task_rerun(
            name,
            &task_cfg,
            &HashMap::new(),
            "re-running (file changed)",
        )
        .await;
    }

    /// Actually spawn a task re-run: release any attach lock, flip to
    /// `Running`, spawn, and wire output. Used by both the file-watch path
    /// ([`handle_task_rerun`]) and the explicit-run paths (`don run <name>`,
    /// `don run --all-pending`).
    ///
    /// `params` is the user-supplied value map; empty for param-less tasks.
    /// Values are substituted into the task's `cmd`/`args`/`env`/`dir` via
    /// `{{name}}` placeholders in [`task::spawn_task`].
    async fn spawn_task_rerun(
        &mut self,
        name: &str,
        task_cfg: &crate::config::Task,
        params: &HashMap<String, String>,
        start_message: &str,
    ) {
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.last_params = params.clone();
            rt.set_needs_run_now(true);
        }
        // Release attach lock and close follow sinks so any active attach
        // session exits cleanly before the new process starts.
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        self.output_manager.service_event(name, start_message);
        self.set_task_state(name, TaskItemState::Running);

        self.output_manager
            .service_debug_event(name, "spawning process...");
        if let Err(e) = self.spawn_task_worker(
            name,
            task_cfg.clone(),
            params.clone(),
            TaskRunMode::Triggered,
            TaskRunIntent::Background,
        ) {
            self.set_task_state(name, TaskItemState::Failed);
            self.output_manager
                .service_error_event(name, &format!("failed to start: {e}"));
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: false,
            });
        }
    }

    /// Run all tasks currently in PendingRun state.
    pub(in crate::runner) async fn handle_run_pending_tasks(
        &mut self,
        reply: oneshot::Sender<CommandResult>,
    ) {
        let pending: Vec<(String, crate::config::Task)> = self
            .tasks
            .iter()
            .filter(|(_, rt)| rt.state() == TaskItemState::PendingRun)
            .map(|(name, rt)| (name.clone(), rt.config.clone()))
            .collect();

        if pending.is_empty() {
            self.output_manager
                .lifecycle_event("no pending tasks to run");
            let _ = reply.send(Ok(()));
            return;
        }

        // Param'd tasks can't be run here — they need user-supplied values.
        // Skip with a note so the user knows to use the palette or `don run`.
        let (runnable, needs_params): (Vec<_>, Vec<_>) = pending
            .into_iter()
            .partition(|(_, cfg)| cfg.params.is_empty());

        for (name, _) in &needs_params {
            self.output_manager
                .service_event(name, "skipped — task has params, run manually");
        }

        if runnable.is_empty() {
            self.output_manager
                .lifecycle_event("no pending tasks to run (param'd tasks skipped)");
            let _ = reply.send(Ok(()));
            return;
        }

        self.output_manager.lifecycle_event(&format!(
            "running {} pending task{}...",
            runnable.len(),
            if runnable.len() == 1 { "" } else { "s" }
        ));

        let empty_params = HashMap::new();
        for (name, cfg) in &runnable {
            // Explicit-run path — bypass the auto_run gate in handle_task_rerun.
            self.spawn_task_rerun(name, cfg, &empty_params, "running (manual trigger)")
                .await;
        }

        let _ = reply.send(Ok(()));
    }

    /// Run a single task by name, bypassing the `auto_run` gate. Used by
    /// `don run <name>`.
    pub(in crate::runner) async fn handle_run_task(
        &mut self,
        name: &str,
        params: HashMap<String, String>,
        reply: oneshot::Sender<CommandResult>,
    ) {
        // Services and unknown names get a dedicated error. Services don't go
        // through "run" at all — that's what start/restart is for.
        if self.services.contains_key(name) {
            let _ = reply.send(Err(CommandError::NotATask {
                name: name.to_string(),
            }));
            return;
        }
        let cfg = match self.tasks.get(name) {
            Some(rt) => rt.config.clone(),
            None => {
                let _ = reply.send(Err(CommandError::UnknownTask {
                    name: name.to_string(),
                }));
                return;
            }
        };

        // Reject while already in flight — otherwise we'd race two spawns of
        // the same task and the output would interleave unpredictably.
        let current = self.tasks.get(name).map(|rt| rt.state());
        if matches!(
            current,
            Some(TaskItemState::Running) | Some(TaskItemState::Building)
        ) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "task is already running".to_string(),
            }));
            return;
        }

        // Resolve params: apply defaults, reject unknown keys, reject
        // missing required values, apply per-kind validation.
        let resolved = match resolve_task_params(name, &cfg, params) {
            Ok(p) => p,
            Err(message) => {
                let _ = reply.send(Err(CommandError::InvalidParams {
                    name: name.to_string(),
                    message,
                }));
                return;
            }
        };

        self.spawn_task_rerun(name, &cfg, &resolved, "running (manual trigger)")
            .await;
        let _ = reply.send(Ok(()));
    }
}
