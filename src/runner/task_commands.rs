use super::task_supervisor;
use super::task_worker::TaskRunMode;
use super::{CommandError, CommandResult, Runner, TaskRunIntent, TaskState, resolve_task_params};
use crate::duration::parse_duration;
use std::collections::HashMap;
use tokio::sync::oneshot;

impl Runner {
    /// Queue a run on this task's supervisor.
    ///
    /// Whether a *prepared* run is still current is not a question the runner
    /// asks — the supervisor is the only thing that emits `TaskRunPrepared`
    /// for its task, and only for the run it is committed to.
    pub(in crate::runner) fn spawn_task_worker(
        &mut self,
        name: &str,
        request: task_supervisor::RunRequest,
    ) -> Result<(), CommandError> {
        // The registry is built from the task map, so a hit here is proof the
        // task exists — no separate existence check needed.
        let Some(handle) = self.task_supervisors.registry().get(name).cloned() else {
            return Err(CommandError::UnknownTask {
                name: name.to_string(),
            });
        };

        if !handle.request(task_supervisor::TaskCommand::Run(request)) {
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
                // The supervisor holds the process; for runtime detail the
                // snapshot is the record, not a copy of one.
                self.state.set_task_pid(name, Some(wired.pgid));
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
            // Nobody is waiting on the outcome either way: the transitions
            // above are the whole answer, and the file watcher — the only
            // thing that ever wanted a completion — no longer tracks the
            // cycles it starts.
            TaskRunIntent::Scheduled | TaskRunIntent::Background => {}
        }
    }

    /// Route a task restart to its supervisor.
    ///
    /// Everything a restart needs — the run in flight, the parameters the
    /// last one used, and the process group to end — belongs to the
    /// supervisor, so all this does is address it. The reply rides down with
    /// the command and is answered there.
    pub(in crate::runner) fn send_task_restart(
        &self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if self.shutting_down {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "shutdown in progress".to_string(),
            }));
            return;
        }
        let mut carried = Some(reply);
        let sent = self
            .task_supervisors
            .registry()
            .get(name)
            .is_some_and(|handle| {
                handle.request(task_supervisor::TaskCommand::Restart {
                    reply: carried.take(),
                })
            });
        if !sent && let Some(reply) = carried {
            let _ = reply.send(Err(CommandError::UnknownTask {
                name: name.to_string(),
            }));
        }
    }

    /// Ask a task's supervisor to end the run it is holding, if any.
    ///
    /// Returns the done-signal to join on. Teardown must: the supervisors are
    /// aborted right after, and aborting one that has not read this yet would
    /// leave its child unreaped.
    pub(in crate::runner) fn send_task_kill(&self, name: &str) -> Option<oneshot::Receiver<()>> {
        let handle = self.task_supervisors.registry().get(name)?;
        let (done_tx, done_rx) = oneshot::channel();
        handle
            .request(task_supervisor::TaskCommand::Kill {
                done: Some(done_tx),
            })
            .then_some(done_rx)
    }

    /// Queue a triggered run on this task's supervisor. Used by the
    /// file-watch path ([`handle_task_rerun`]) and the explicit-run paths
    /// (`don run <name>`, `don run --all-pending`).
    ///
    /// `start_message` is what the supervisor announces when it picks the run
    /// up; the flip to `Running` follows from that report, not from here.
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
        let wait = wait_reply.map(|(reply, timeout)| task_supervisor::RunWait {
            reply,
            // An unparseable spelling waits indefinitely, as it did when the
            // old timeout task simply failed to spawn.
            timeout: timeout.and_then(|spelling| {
                parse_duration(&spelling)
                    .ok()
                    .map(|duration| (duration, spelling))
            }),
        });
        let mut carried = Some(wait);
        match self.spawn_task_worker(
            name,
            task_supervisor::RunRequest {
                task_cfg: Box::new(task_cfg.clone()),
                params: params.clone(),
                mode: TaskRunMode::Triggered,
                intent: TaskRunIntent::Background,
                wait: carried.take().flatten(),
                start_message: Some(start_message.to_string()),
            },
        ) {
            Ok(()) => {}
            Err(e) => {
                self.set_task_state(name, TaskState::Failed);
                self.output_manager
                    .service_error_event(name, &format!("failed to start: {e}"));
                if let Some(wait) = carried.take().flatten() {
                    let _ = wait.reply.send(Err(CommandError::Failed {
                        name: name.to_string(),
                        message: format!("failed to start: {e}"),
                    }));
                }
            }
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
