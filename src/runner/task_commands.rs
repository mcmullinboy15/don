use super::task_supervisor;
use super::task_worker::TaskRunMode;
use super::{
    CommandError, CommandResult, Runner, RunnerEvent, RunnerInternalCommand, TaskRunIntent,
    TaskRunWaiter, TaskState, resolve_task_params,
};
use crate::config::TaskAutoRun;
use crate::duration::parse_duration;
use std::collections::HashMap;
use tokio::sync::oneshot;

impl Runner {
    /// Queue a run on this task's supervisor.
    ///
    /// Returns the run's generation, which now has exactly one remaining job:
    /// identifying which run a `don run --wait` reply belongs to. Deciding
    /// whether a *prepared* run is still current is no longer a question the
    /// runner asks — the supervisor is the only thing that emits
    /// `TaskRunPrepared` for its task, and only for the run it is committed
    /// to.
    pub(in crate::runner) fn spawn_task_worker(
        &mut self,
        name: &str,
        task_cfg: crate::config::Task,
        params: HashMap<String, String>,
        mode: TaskRunMode,
        intent: TaskRunIntent,
    ) -> Result<(), CommandError> {
        // The registry is built from the task map, so a hit here is proof the
        // task exists — no separate existence check needed.
        let Some(handle) = self.task_supervisors.registry().get(name).cloned() else {
            return Err(CommandError::UnknownTask {
                name: name.to_string(),
            });
        };

        let queued = handle.request(task_supervisor::RunRequest {
            task_cfg: Box::new(task_cfg),
            params,
            mode,
            intent,
        });
        if !queued {
            return Err(CommandError::Failed {
                name: name.to_string(),
                message: "task supervisor is shutting down".to_string(),
            });
        }

        Ok(())
    }

    pub(in crate::runner) async fn handle_task_run_prepared(
        &mut self,
        name: &str,
        task_cfg: &crate::config::Task,
        intent: TaskRunIntent,
        result: Result<task_supervisor::TaskRunReport, String>,
    ) {
        if self.shutting_down {
            self.stop_late_task_start(name.to_string(), result).await;
            return;
        }
        match result {
            Ok(task_supervisor::TaskRunReport::PendingRun { message }) => {
                self.settle_task_without_spawn(
                    name,
                    intent,
                    task_supervisor::NoSpawnOutcome::pending_run(message),
                )
                .await;
            }
            Ok(task_supervisor::TaskRunReport::Skipped { message }) => {
                self.settle_task_without_spawn(
                    name,
                    intent,
                    task_supervisor::NoSpawnOutcome::skipped(
                        message.unwrap_or_else(|| "skipped".to_string()),
                    ),
                )
                .await;
            }
            Ok(task_supervisor::TaskRunReport::Running(wired)) => {
                let emitter = self.output_manager.clone_lifecycle_emitter();
                emitter.service_debug_event(name, &format!("process spawned (pid {})", wired.pgid));
                emitter.service_event(name, &format!("spawn {}", wired.rendered_cmdline));
                // An interactive task waits for a user on its PTY; say how
                // to reach it, loudly enough to act on.
                if task_cfg.terminal.is_foreground() {
                    emitter.service_event(
                        name,
                        &format!("waiting for input — run 'don attach {name}'"),
                    );
                }
                // The supervisor holds the process, the reader, and the
                // scheduler answer; this side keeps the shadows attach and
                // status read, and makes the runner-only state transition.
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.pgid = Some(wired.pgid);
                }
                self.begin_task_run(name, intent, Some("running..."));
            }
            Err(message) => {
                self.settle_task_without_spawn(
                    name,
                    intent,
                    task_supervisor::NoSpawnOutcome::failed(message),
                )
                .await;
            }
        }
    }

    /// Mark a freshly-spawned run as live, returning the scheduler's
    /// completion channel if this run answers to one.
    ///
    /// Only a *scheduled* run transitions to `Running` and reports back: a
    /// background `don run` is not something the dependency sweep is waiting
    /// on, and moving the task to `Running` for one would make startup gating
    /// depend on manual activity.
    fn begin_task_run(&mut self, name: &str, intent: TaskRunIntent, running_message: Option<&str>) {
        match intent {
            TaskRunIntent::Scheduled => {
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.set_needs_run_now(true);
                }
                if let Some(message) = running_message {
                    self.output_manager.service_event(name, message);
                }
                self.set_task_state(name, TaskState::Running);
            }
            TaskRunIntent::Background => {}
        }
    }

    /// Apply a prepared run that never spawned a process.
    ///
    /// `PendingRun`, `Skipped` and a preparation failure differ only in the
    /// state they land in, what they tell the dependency scheduler, and how
    /// loudly they say so — all of which live on the outcome. What stays here
    /// is the part only the runner may do: transition process state, which wakes
    /// the cross-process dependency sweep.
    async fn settle_task_without_spawn(
        &mut self,
        name: &str,
        intent: TaskRunIntent,
        outcome: task_supervisor::NoSpawnOutcome,
    ) {
        if let Some(needs_run_now) = outcome.needs_run_now()
            && let Some(rt) = self.tasks.get_mut(name)
        {
            rt.set_needs_run_now(needs_run_now);
        }
        self.set_task_state(name, outcome.state);
        outcome.emit(&self.output_manager.clone_lifecycle_emitter(), name);

        match intent {
            // The transitions above are the whole answer for a scheduled
            // settle: PendingRun/Skipped/Failed all re-schedule the sweep,
            // and the old completion message's fold was a no-op for every
            // settle state.
            TaskRunIntent::Scheduled => {}
            // A deferred or skipped background run has nobody to tell: it is
            // not an outcome anyone is waiting on. Only a failure is.
            TaskRunIntent::Background => {
                if !outcome.success {
                    if let Some(rt) = self.tasks.get_mut(name)
                        && let Some(waiter) = rt.run_waiter.take()
                    {
                        waiter.complete(Err(CommandError::Failed {
                            name: name.to_string(),
                            message: outcome.message.clone(),
                        }));
                    }
                    let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                        name: name.to_string(),
                        success: false,
                    });
                }
            }
        }
    }

    async fn stop_task_pgid(&mut self, name: &str, pgid: i32) -> CommandResult {
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        self.output_manager
            .service_event(name, "stopping... (requested)");
        self.output_manager
            .service_event(name, &format!("send SIGKILL to task pgid {pgid}"));

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

        if matches!(state, TaskState::Running | TaskState::Building)
            && let Some(pgid) = pgid
        {
            self.stop_task_pgid(name, pgid).await?;
        }

        self.spawn_task_rerun(
            name,
            &task_cfg,
            &last_params,
            "restarting (manual trigger)",
            None,
        )
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
            .is_some_and(|rt| rt.state() == TaskState::Building)
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
            self.set_task_state(name, TaskState::PendingRun);
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
            self.set_task_state(name, TaskState::PendingRun);
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
            None,
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
        wait_reply: Option<(oneshot::Sender<CommandResult>, Option<String>)>,
    ) {
        if let Some(rt) = self.tasks.get_mut(name) {
            if let Some(waiter) = rt.run_waiter.take() {
                waiter.complete(Err(CommandError::Failed {
                    name: name.to_string(),
                    message: "task run was superseded".to_string(),
                }));
            }
            rt.last_params = params.clone();
            rt.set_needs_run_now(true);
        }
        // Close follow sinks so any active follower exits cleanly before
        // the new process starts. (Attach cleanup is the supervisor's now.)
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        self.output_manager.service_event(name, start_message);
        self.set_task_state(name, TaskState::Running);

        self.output_manager
            .service_debug_event(name, "spawning process...");
        match self.spawn_task_worker(
            name,
            task_cfg.clone(),
            params.clone(),
            TaskRunMode::Triggered,
            TaskRunIntent::Background,
        ) {
            Ok(()) => {
                if let Some((reply, timeout)) = wait_reply {
                    self.register_task_run_waiter(name, reply, timeout);
                }
            }
            Err(e) => {
                self.set_task_state(name, TaskState::Failed);
                self.output_manager
                    .service_error_event(name, &format!("failed to start: {e}"));
                if let Some((reply, _)) = wait_reply {
                    let _ = reply.send(Err(CommandError::Failed {
                        name: name.to_string(),
                        message: format!("failed to start: {e}"),
                    }));
                }
                let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                    name: name.to_string(),
                    success: false,
                });
            }
        }
    }

    fn register_task_run_waiter(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
        timeout: Option<String>,
    ) {
        let token = match self.tasks.get_mut(name) {
            Some(rt) => {
                rt.waiter_token = rt.waiter_token.saturating_add(1);
                rt.waiter_token
            }
            None => return,
        };
        let timeout_task = timeout.as_ref().and_then(|timeout| {
            let duration = parse_duration(timeout).ok()?;
            let cmd_tx = self.internal_tx.clone();
            let name = name.to_string();
            let timeout = timeout.clone();
            Some(tokio::spawn(async move {
                tokio::time::sleep(duration).await;
                let _ = cmd_tx
                    .send(RunnerInternalCommand::TaskRunWaitTimedOut {
                        name,
                        token,
                        timeout,
                    })
                    .await;
            }))
        });
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.run_waiter = Some(TaskRunWaiter::new(token, reply, timeout_task));
        }
    }

    pub(in crate::runner) fn handle_task_run_wait_timeout(
        &mut self,
        name: &str,
        token: u64,
        timeout: &str,
    ) {
        let Some(rt) = self.tasks.get_mut(name) else {
            return;
        };
        let is_matching_waiter = rt
            .run_waiter
            .as_ref()
            .is_some_and(|waiter| waiter.token() == token);
        if !is_matching_waiter {
            return;
        }
        if let Some(waiter) = rt.run_waiter.take() {
            waiter.complete(Err(CommandError::TimedOut {
                name: name.to_string(),
                timeout: timeout.to_string(),
            }));
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
            .filter(|(_, rt)| rt.state() == TaskState::PendingRun)
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
            self.spawn_task_rerun(name, cfg, &empty_params, "running (manual trigger)", None)
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
        wait: bool,
        wait_timeout: Option<String>,
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
        let already_in_flight = self
            .tasks
            .get(name)
            .is_some_and(|rt| matches!(rt.state(), TaskState::Running | TaskState::Building))
            || self.task_supervisors.registry().is_busy(name);
        if already_in_flight {
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

        let wait = wait || wait_timeout.is_some();
        if let Some(timeout) = wait_timeout.as_deref()
            && let Err(e) = parse_duration(timeout)
        {
            let _ = reply.send(Err(CommandError::InvalidParams {
                name: name.to_string(),
                message: format!("invalid wait timeout: {e}"),
            }));
            return;
        }

        if wait {
            self.spawn_task_rerun(
                name,
                &cfg,
                &resolved,
                "running (manual trigger)",
                Some((reply, wait_timeout)),
            )
            .await;
        } else {
            self.spawn_task_rerun(name, &cfg, &resolved, "running (manual trigger)", None)
                .await;
            let _ = reply.send(Ok(()));
        }
    }
}
