//! The parts of a task run that belong to the task, not to the runner.
//!
//! A task run is a pipeline: prepare (resolve params, hash inputs, decide
//! whether to run at all) → spawn → wire output → wait for exit → record the
//! outcome. Today the runner drives that pipeline, which is why it needs
//! `run_generation`: each stage hands off through a detached task, completions
//! from every task land on one shared channel, and the runner cannot otherwise
//! tell a current completion from a superseded one.
//!
//! This module is where that pipeline moves. It starts with the exit half —
//! the stage that has no runner state to touch at all — as free functions
//! taking owned inputs, so they can run anywhere. What is left in
//! `task_commands` after each piece moves is the part only the runner may do:
//! transition item state, which drives the cross-item dependency scheduler.

use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::{ItemDone, NodeKind, RunnerInternalCommand, TaskExit};
use crate::task_state::{TaskRunInfo, TaskState};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// One request to run a task, as handed to its supervisor.
pub(in crate::runner) struct RunRequest {
    pub(in crate::runner) task_cfg: Box<crate::config::Task>,
    pub(in crate::runner) params: std::collections::HashMap<String, String>,
    pub(in crate::runner) mode: super::task_worker::TaskRunMode,
    pub(in crate::runner) intent: super::TaskRunIntent,
}

/// Send-only handle to one task's run supervisor.
///
/// Clone-able on purpose: this is the *addressing* half. Nothing here can
/// create or destroy a supervisor, so handing one out to the file watcher or
/// the API widens what can ask a task to run without widening what can change
/// the set of tasks.
#[derive(Clone)]
pub(in crate::runner) struct TaskHandle {
    tx: mpsc::UnboundedSender<RunRequest>,
    /// True from the moment a run is queued until the supervisor goes back to
    /// waiting with an empty mailbox. The startup sweep reads it to tell
    /// "this task hasn't been asked to run" from "it is being prepared".
    busy: Arc<AtomicBool>,
}

impl TaskHandle {
    /// Queue a run. Fails only once the supervisor is gone (shutdown).
    ///
    /// Marks the task busy before sending, not after the supervisor picks the
    /// request up — otherwise a caller could queue a run and immediately be
    /// told the task is idle.
    pub(in crate::runner) fn request(&self, request: RunRequest) -> bool {
        self.busy.store(true, Ordering::Relaxed);
        let sent = self.tx.send(request).is_ok();
        if !sent {
            self.busy.store(false, Ordering::Relaxed);
        }
        sent
    }

    /// Whether a run is queued or being prepared.
    pub(in crate::runner) fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Relaxed)
    }
}

/// Every task's mailbox, addressable by name.
///
/// Clone-able and lock-free, and it can be both because **the item set is
/// fixed at construction** (see `setup::build_runtime_maps`). There is no
/// insert and no remove, so there is nothing to synchronise — an
/// `Arc<HashMap<_, _>>` is the whole implementation. If items ever became
/// dynamic this would need the [`StateWriter`]/[`StateReader`] treatment
/// rather than a lock.
///
/// [`StateWriter`]: super::state_store::StateWriter
/// [`StateReader`]: super::StateReader
#[derive(Clone)]
pub(in crate::runner) struct TaskRegistry {
    handles: Arc<HashMap<String, TaskHandle>>,
}

impl TaskRegistry {
    /// The mailbox for `name`, or `None` if it isn't a task.
    pub(in crate::runner) fn get(&self, name: &str) -> Option<&TaskHandle> {
        self.handles.get(name)
    }

    /// Whether `name` has a run queued or being prepared. `false` for an
    /// unknown name, which is what callers asking "can I start this?" want.
    pub(in crate::runner) fn is_busy(&self, name: &str) -> bool {
        self.get(name).is_some_and(TaskHandle::is_busy)
    }

    /// Names with a run queued or being prepared.
    pub(in crate::runner) fn busy_names(&self) -> impl Iterator<Item = &str> {
        self.handles
            .iter()
            .filter(|(_, handle)| handle.is_busy())
            .map(|(name, _)| name.as_str())
    }
}

/// The owner half: the supervisor tasks themselves.
///
/// Split from [`TaskRegistry`] by capability rather than by convenience —
/// addressing a task is something many components should be able to do, while
/// *ending* one is the runner's alone. Holding the join handles here is what
/// makes that true by construction.
pub(in crate::runner) struct TaskSupervisors {
    registry: TaskRegistry,
    joins: Vec<(String, tokio::task::JoinHandle<()>)>,
}

impl TaskSupervisors {
    /// Start one supervisor per task.
    ///
    /// Eager rather than on first use: it makes the registry immutable, which
    /// is what lets it be shared without a lock. An idle supervisor is a
    /// parked task that is never polled until something addresses it.
    pub(in crate::runner) fn spawn_all<'a>(
        names: impl Iterator<Item = &'a String>,
        ctx: &super::task_worker::TaskWorkerContext,
        internal_tx: &mpsc::Sender<RunnerInternalCommand>,
    ) -> Self {
        let mut handles = HashMap::new();
        let mut joins = Vec::new();
        for name in names {
            let (tx, rx) = mpsc::unbounded_channel();
            let busy = Arc::new(AtomicBool::new(false));
            let join = tokio::spawn(supervise(
                name.clone(),
                rx,
                ctx.clone(),
                internal_tx.clone(),
                Arc::clone(&busy),
            ));
            handles.insert(name.clone(), TaskHandle { tx, busy });
            joins.push((name.clone(), join));
        }
        Self {
            registry: TaskRegistry {
                handles: Arc::new(handles),
            },
            joins,
        }
    }

    /// The addressing half, for handing to anything that needs to ask a task
    /// to run.
    pub(in crate::runner) fn registry(&self) -> &TaskRegistry {
        &self.registry
    }

    /// Cancel every supervisor, returning the handles to await.
    ///
    /// Deliberately *not* an `async fn` that also waits: shutdown has to fire
    /// every abort before waiting on any of them, or a project with N tasks
    /// pays the timeout N times over instead of once. Every other teardown
    /// loop in `shutdown.rs` has the same shape.
    ///
    /// Aborting mid-preparation can strand a process the worker had just
    /// spawned — the handle dies with the future. That is pre-existing (the
    /// runner aborted `run_worker` the same way), and shutdown's
    /// `stop_late_task_start` is what catches the case where the spawn
    /// already reported in.
    /// Nothing needs to drop the registry first: the receiver lives inside
    /// the supervisor future, so aborting it drops the receiver, and every
    /// outstanding [`TaskHandle`] — including clones handed to other
    /// components — starts reporting failure from `request`. That matters
    /// once the file watcher holds one.
    pub(in crate::runner) fn abort_all(&mut self) -> Vec<(String, tokio::task::JoinHandle<()>)> {
        let joins = std::mem::take(&mut self.joins);
        for (_, join) in &joins {
            join.abort();
        }
        joins
    }
}

/// Drive one task's runs, strictly in order.
///
/// The shape that matters is that a superseded run is **finished, not
/// aborted**. `run_task_worker` may already have spawned a process by the
/// time a newer request arrives; dropping that future would take the handle
/// with it and leave a child nothing will ever reap. So the worker always
/// runs to completion and the result is then killed off explicitly.
async fn supervise(
    name: String,
    mut rx: mpsc::UnboundedReceiver<RunRequest>,
    ctx: super::task_worker::TaskWorkerContext,
    internal_tx: mpsc::Sender<RunnerInternalCommand>,
    busy: Arc<AtomicBool>,
) {
    let mut pending: Option<RunRequest> = None;
    let mut mailbox_closed = false;

    loop {
        let request = match pending.take() {
            Some(request) => request,
            None => {
                // Idle only here: everywhere else there is work in hand.
                busy.store(false, Ordering::Relaxed);
                match rx.recv().await {
                    Some(request) => {
                        busy.store(true, Ordering::Relaxed);
                        request
                    }
                    None => return,
                }
            }
        };
        let RunRequest {
            task_cfg,
            params,
            mode,
            intent,
        } = request;

        let task_cfg_for_worker = task_cfg.clone();
        let worker = super::task_worker::run_task_worker(
            ctx.clone(),
            &name,
            task_cfg_for_worker.as_ref(),
            &params,
            mode,
        );
        tokio::pin!(worker);

        // Watch for a newer request while the current one prepares, keeping
        // only the most recent — anything older is already superseded too.
        let mut superseded: Option<RunRequest> = None;
        let result = loop {
            tokio::select! {
                result = &mut worker => break result,
                next = rx.recv(), if !mailbox_closed => match next {
                    Some(next) => superseded = Some(next),
                    // Guarded so a closed mailbox doesn't spin this select:
                    // `recv` on a closed channel returns immediately, forever.
                    None => mailbox_closed = true,
                },
            }
        };

        match superseded {
            Some(next) => {
                if let Ok(prepared) = result {
                    kill_superseded_spawn(&ctx.emitter, &name, prepared);
                }
                pending = Some(next);
            }
            None => {
                let sent = internal_tx
                    .send(RunnerInternalCommand::TaskRunPrepared {
                        name: name.clone(),
                        task_cfg,
                        intent,
                        result,
                    })
                    .await;
                if sent.is_err() {
                    return;
                }
            }
        }
    }
}

/// How prominently a settled run's message is reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runner) enum Report {
    /// Normal lifecycle line.
    Info,
    /// Verbose-only — the run was a no-op and nobody asked.
    Debug,
    /// The run failed.
    Error,
}

/// A prepared run that ended without leaving a process behind.
///
/// Three of the five outcomes of preparing a run never spawn: the task is
/// waiting on something (`PendingRun`), its inputs were unchanged so it was
/// skipped, or preparation itself failed. They were three near-identical
/// branches on the runner; the differences between them are exactly the
/// fields here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runner) struct NoSpawnOutcome {
    /// Lifecycle state the task enters.
    pub(in crate::runner) state: super::TaskItemState,
    /// What the dependency scheduler is told. A skipped or deferred task is
    /// still a *success* — it didn't fail, it just didn't run.
    pub(in crate::runner) success: bool,
    pub(in crate::runner) message: String,
    pub(in crate::runner) report: Report,
}

impl NoSpawnOutcome {
    /// The task can't run yet and is waiting on something.
    pub(in crate::runner) fn pending_run(message: String) -> Self {
        Self {
            state: super::TaskItemState::PendingRun,
            success: true,
            message,
            report: Report::Info,
        }
    }

    /// The task's watched inputs were unchanged, so it didn't need to run.
    pub(in crate::runner) fn skipped(message: String) -> Self {
        Self {
            state: super::TaskItemState::Skipped,
            success: true,
            message,
            report: Report::Debug,
        }
    }

    /// Preparing the run failed before anything was spawned.
    pub(in crate::runner) fn failed(message: String) -> Self {
        Self {
            state: super::TaskItemState::Failed,
            success: false,
            message,
            report: Report::Error,
        }
    }

    /// Whether to update `needs_run_now`, and to what. `None` leaves it alone.
    ///
    /// **The `Failed` asymmetry is a suspected bug, preserved verbatim.** A
    /// *scheduled* run that fails to prepare marks the task as still needing
    /// a run; a background `don run` that fails to prepare leaves the flag
    /// untouched, so the failure is invisible to the next startup sweep. That
    /// looks wrong — a task whose preparation failed has not run either way —
    /// but changing it here would bury a behaviour change inside a refactor
    /// that is otherwise a no-op. It gets its own commit and its own test.
    pub(in crate::runner) fn needs_run_now(&self, scheduled: bool) -> Option<bool> {
        match self.state {
            super::TaskItemState::PendingRun => Some(true),
            super::TaskItemState::Skipped => Some(false),
            super::TaskItemState::Failed if scheduled => Some(true),
            _ => None,
        }
    }

    /// The message to hand a caller waiting on this run, if it failed.
    pub(in crate::runner) fn failure_message(&self) -> Option<String> {
        (!self.success).then(|| self.message.clone())
    }

    /// Emit this outcome's message at its own level.
    pub(in crate::runner) fn emit(&self, emitter: &crate::output::LifecycleEmitter, name: &str) {
        match self.report {
            Report::Info => emitter.service_event(name, &self.message),
            Report::Debug => emitter.service_debug_event(name, &self.message),
            Report::Error => emitter.service_error_event(name, &self.message),
        }
    }
}

/// How long a superseded process gets to die politely before SIGKILL lands.
const SUPERSEDED_KILL_GRACE: Duration = Duration::from_millis(500);

/// Kill the process from a run that has been superseded by a newer one.
///
/// A run that loses a race may already have spawned; the process is live and
/// nothing else will ever reap it, so it has to be killed here. Today the
/// runner discovers this by comparing generations after the fact. Once a
/// supervisor owns the run it will call this directly when it cancels one —
/// same work, but as cleanup of something it owns rather than as recovery
/// from a race it could not prevent.
///
/// Detached on purpose: the caller is on the runner's command loop, and
/// waiting out a grace period there would stall every other item.
///
/// Takes the untagged emitter rather than an `ItemOutput` so the kill can
/// never be gated on a name lookup succeeding — failing to log is a cosmetic
/// problem, failing to kill leaks a process nothing will ever reap.
pub(in crate::runner) fn kill_superseded_spawn(
    emitter: &crate::output::LifecycleEmitter,
    name: &str,
    prepared: super::task_worker::TaskRunPrepared,
) {
    use super::task_worker::TaskRunPrepared;

    match prepared {
        TaskRunPrepared::Spawned(spawn) => {
            let super::task::TaskSpawn {
                mut handle,
                child_output,
                rendered_cmdline: _,
            } = *spawn;
            // Drop the read half first: nothing is going to consume it, and
            // holding it open keeps the child's pipe alive.
            drop(child_output);
            emitter.service_event(
                name,
                &format!("send SIGKILL to stale task pgid {}", handle.pgid()),
            );
            tokio::spawn(async move {
                let _ = handle
                    .terminate(nix::sys::signal::Signal::SIGKILL, SUPERSEDED_KILL_GRACE)
                    .await;
            });
        }
        TaskRunPrepared::ForegroundSpawned(spawn) => {
            let super::task::ForegroundTaskSpawn {
                mut handle,
                rendered_cmdline: _,
            } = *spawn;
            emitter.service_event(
                name,
                &format!(
                    "send SIGKILL to stale foreground task pgid {}",
                    handle.pgid()
                ),
            );
            tokio::spawn(async move {
                let _ = handle
                    .terminate(nix::sys::signal::Signal::SIGKILL, SUPERSEDED_KILL_GRACE)
                    .await;
            });
        }
        // Nothing was spawned, so there is nothing to clean up.
        TaskRunPrepared::PendingRun { .. } | TaskRunPrepared::Skipped { .. } => {}
    }
}

/// Everything the exit half of a task run needs, owned outright.
///
/// Owned rather than borrowed because this outlives the runner's command loop
/// — the exit wait is a detached task, and holding a reference into runner
/// state across it is what the whole decomposition is trying to stop.
pub(in crate::runner) struct TaskRunOutcome {
    pub(in crate::runner) name: String,
    pub(in crate::runner) task_cfg: crate::config::Task,
    pub(in crate::runner) base_dir: PathBuf,
    pub(in crate::runner) global_watch_ignore: Vec<String>,
    /// Process group of the run that just ended.
    pub(in crate::runner) pgid: i32,
    /// `Some` for a startup-scheduled run — the dependency scheduler is
    /// waiting on it. `None` for a watch rerun or a background `don run`,
    /// which report through the runner's internal channel instead.
    pub(in crate::runner) done_tx: Option<mpsc::Sender<ItemDone>>,
    pub(in crate::runner) internal_tx: mpsc::Sender<RunnerInternalCommand>,
    /// Whether this run was triggered by a file watch, which decides if a
    /// `TaskRerunComplete` event is broadcast when it lands.
    pub(in crate::runner) rerun: bool,
}

impl TaskRunOutcome {
    /// Record a finished run and send exactly one completion message.
    ///
    /// Both the background and foreground wait paths end here, so the rules
    /// for what counts as success, what gets persisted, and who is told stay
    /// in one place — they were duplicated verbatim before, which is how they
    /// drift.
    ///
    /// A successful run records its watched inputs alongside the run info, so
    /// the next startup can skip it when nothing changed; a failed one records
    /// only the run info, leaving the previous input hashes stale on purpose
    /// so the task is not skipped next time.
    pub(in crate::runner) async fn finish(
        self,
        result: Result<std::process::ExitStatus, super::task::TaskError>,
        elapsed: Duration,
    ) {
        let (success, exit_code, message) = match result {
            Ok(status) if status.success() => (true, status.code(), None),
            Ok(status) => {
                let code = status.code().unwrap_or(-1);
                (false, status.code(), Some(format!("exit code {code}")))
            }
            Err(e) => (false, None, Some(e.to_string())),
        };
        let last_run =
            TaskRunInfo::finished_now(success, Some(elapsed), exit_code, message.clone());

        let task_state = TaskState::new(self.base_dir.join(".don").join("task-state"));
        if success {
            let task_dir = working_dir_for(&self.base_dir, self.task_cfg.dir.as_deref());
            let ignore_patterns = resolve_watch_ignore_patterns(
                &task_dir,
                &self.task_cfg.ignore,
                &self.base_dir,
                &self.global_watch_ignore,
            );
            let _ = task_state
                .record_success_with_info(
                    &self.name,
                    &self.task_cfg.watch,
                    &ignore_patterns,
                    Some(&task_dir),
                    &last_run,
                )
                .await;
        } else {
            let _ = task_state.record_run(&self.name, &last_run).await;
        }

        match self.done_tx {
            Some(done_tx) => {
                let _ = done_tx
                    .send(ItemDone {
                        name: self.name,
                        kind: NodeKind::Task,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        last_run: Some(last_run),
                        service_start_generation: None,
                        task_run_generation: None,
                    })
                    .await;
            }
            None => {
                let _ = self
                    .internal_tx
                    .send(RunnerInternalCommand::TaskExited(TaskExit {
                        name: self.name,
                        pgid: self.pgid,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        last_run: Some(last_run),
                        rerun: self.rerun,
                    }))
                    .await;
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    /// A real parsed task, so the defaults here are the product's defaults.
    fn test_task() -> crate::config::Task {
        let config: crate::config::Config = "[tasks.build]\ncmd = \"true\"\n".parse().unwrap();
        config.tasks.get("build").unwrap().clone()
    }

    fn outcome(
        name: &str,
        base_dir: &std::path::Path,
        done_tx: Option<mpsc::Sender<ItemDone>>,
        internal_tx: mpsc::Sender<RunnerInternalCommand>,
        rerun: bool,
    ) -> TaskRunOutcome {
        TaskRunOutcome {
            name: name.to_string(),
            task_cfg: test_task(),
            base_dir: base_dir.to_path_buf(),
            global_watch_ignore: Vec::new(),
            pgid: 4242,
            done_tx,
            internal_tx,
            rerun,
        }
    }

    #[test]
    fn no_spawn_outcomes_classify_consistently() {
        use super::super::TaskItemState;

        struct Case {
            label: &'static str,
            outcome: NoSpawnOutcome,
            want_state: TaskItemState,
            want_success: bool,
            want_report: Report,
            /// `needs_run_now` for a scheduled run, then for a background one.
            want_needs: (Option<bool>, Option<bool>),
            want_failure_message: Option<&'static str>,
        }

        let cases = vec![
            Case {
                label: "deferred",
                outcome: NoSpawnOutcome::pending_run("waiting on deps".to_string()),
                want_state: TaskItemState::PendingRun,
                // Not a failure: it just hasn't run yet.
                want_success: true,
                want_report: Report::Info,
                want_needs: (Some(true), Some(true)),
                want_failure_message: None,
            },
            Case {
                label: "skipped",
                outcome: NoSpawnOutcome::skipped("no changes".to_string()),
                want_state: TaskItemState::Skipped,
                want_success: true,
                // Verbose-only: nobody asked for a no-op to be announced.
                want_report: Report::Debug,
                want_needs: (Some(false), Some(false)),
                want_failure_message: None,
            },
            Case {
                label: "prepare failed",
                outcome: NoSpawnOutcome::failed("bad param".to_string()),
                want_state: TaskItemState::Failed,
                want_success: false,
                want_report: Report::Error,
                // Pinning current behaviour, not endorsing it: only a
                // scheduled failure marks the task as still needing a run.
                // See `needs_run_now` — the background case is a suspected
                // bug and this expectation should flip when it is fixed.
                want_needs: (Some(true), None),
                want_failure_message: Some("bad param"),
            },
        ];

        for case in cases {
            assert_eq!(case.outcome.state, case.want_state, "{}: state", case.label);
            assert_eq!(
                case.outcome.success, case.want_success,
                "{}: success",
                case.label
            );
            assert_eq!(
                case.outcome.report, case.want_report,
                "{}: report level",
                case.label
            );
            assert_eq!(
                (
                    case.outcome.needs_run_now(true),
                    case.outcome.needs_run_now(false)
                ),
                case.want_needs,
                "{}: needs_run_now (scheduled, background)",
                case.label
            );
            assert_eq!(
                case.outcome.failure_message().as_deref(),
                case.want_failure_message,
                "{}: failure message",
                case.label
            );
        }
    }

    /// The registry is the addressing half and nothing more: a clone can
    /// reach a task, and an unknown name is `None` rather than something
    /// created on demand. If lookups ever started inserting, the map would
    /// need synchronising and the lock-free `Arc<HashMap<_, _>>` would go.
    #[tokio::test]
    async fn the_registry_addresses_tasks_without_creating_them() {
        let (internal_tx, _internal_rx) = mpsc::channel(4);
        let temp = tempfile::tempdir().unwrap();
        let output = crate::output::OutputManager::new(&[], tokio::io::sink())
            .await
            .unwrap();
        let ctx = super::super::task_worker::TaskWorkerContext {
            base_dir: temp.path().to_path_buf(),
            platform: crate::config::Platform::LinuxX86_64,
            emitter: output.clone_lifecycle_emitter(),
            global_watch_ignore: Vec::new(),
            terminal_coordinator: super::super::TerminalCoordinator::detached(),
        };
        let names = ["build".to_string(), "migrate".to_string()];
        let mut supervisors = TaskSupervisors::spawn_all(names.iter(), &ctx, &internal_tx);
        let registry = supervisors.registry().clone();

        assert!(registry.get("build").is_some());
        assert!(registry.get("migrate").is_some());
        assert!(
            registry.get("never-declared").is_none(),
            "an unknown name must not be conjured into existence"
        );
        assert!(
            !registry.is_busy("never-declared"),
            "an unknown name is not busy — callers ask this to decide if they may start it"
        );
        assert!(!registry.is_busy("build"), "nothing queued yet");

        // Aborting drops the receivers, so every outstanding handle — this
        // clone included — reports failure rather than queueing into a void.
        for (_, join) in supervisors.abort_all() {
            let _ = join.await;
        }
        let handle = registry.get("build").unwrap().clone();
        assert!(
            !handle.request(RunRequest {
                task_cfg: Box::new(test_task()),
                params: std::collections::HashMap::new(),
                mode: super::super::task_worker::TaskRunMode::Triggered,
                intent: super::super::TaskRunIntent::Background,
            }),
            "a handle to a stopped supervisor must report the failure"
        );
    }

    /// A scheduled run answers the dependency scheduler; anything else
    /// reports through the runner's internal channel. Exactly one of the two,
    /// never both — a startup task that also emitted `TaskExited` would be
    /// applied twice.
    #[tokio::test]
    async fn a_finished_run_reports_to_exactly_one_place() {
        struct Case {
            name: &'static str,
            scheduled: bool,
            status: std::process::ExitStatus,
            want_success: bool,
            want_message: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "scheduled success",
                scheduled: true,
                status: ExitStatusExt::from_raw(0),
                want_success: true,
                want_message: None,
            },
            Case {
                name: "scheduled failure carries the exit code",
                scheduled: true,
                status: ExitStatusExt::from_raw(3 << 8),
                want_success: false,
                want_message: Some("exit code 3"),
            },
            Case {
                name: "rerun success",
                scheduled: false,
                status: ExitStatusExt::from_raw(0),
                want_success: true,
                want_message: None,
            },
            Case {
                name: "rerun failure",
                scheduled: false,
                status: ExitStatusExt::from_raw(1 << 8),
                want_success: false,
                want_message: Some("exit code 1"),
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let (done_tx, mut done_rx) = mpsc::channel(4);
            let (internal_tx, mut internal_rx) = mpsc::channel(4);
            let scheduled = case.scheduled.then_some(done_tx);

            outcome(
                "build",
                temp.path(),
                scheduled,
                internal_tx,
                !case.scheduled,
            )
            .finish(Ok(case.status), Duration::from_millis(5))
            .await;

            if case.scheduled {
                let done = done_rx.try_recv().expect("scheduled run answers done_tx");
                assert_eq!(done.name, "build", "{}", case.name);
                assert_eq!(done.success, case.want_success, "{}", case.name);
                assert_eq!(done.message.as_deref(), case.want_message, "{}", case.name);
                assert!(
                    internal_rx.try_recv().is_err(),
                    "{}: must not also emit TaskExited",
                    case.name
                );
            } else {
                let Ok(RunnerInternalCommand::TaskExited(exit)) = internal_rx.try_recv() else {
                    panic!("{}: expected a TaskExited", case.name);
                };
                assert_eq!(exit.name, "build", "{}", case.name);
                assert_eq!(exit.pgid, 4242, "{}", case.name);
                assert_eq!(exit.success, case.want_success, "{}", case.name);
                assert_eq!(exit.message.as_deref(), case.want_message, "{}", case.name);
                assert!(exit.rerun, "{}", case.name);
                assert!(
                    done_rx.try_recv().is_err(),
                    "{}: must not also answer done_tx",
                    case.name
                );
            }
        }
    }

    /// Only a successful run records its input hashes. Recording them on
    /// failure would let the next startup skip a task that never worked.
    #[tokio::test]
    async fn only_success_records_watched_inputs() {
        for (label, status, want_success) in [
            ("success", ExitStatusExt::from_raw(0), true),
            ("failure", ExitStatusExt::from_raw(1 << 8), false),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let (internal_tx, _internal_rx) = mpsc::channel(4);
            outcome("build", temp.path(), None, internal_tx, false)
                .finish(Ok(status), Duration::from_millis(1))
                .await;

            let state = TaskState::new(temp.path().join(".don").join("task-state"));
            assert_eq!(
                state.has_success("build").await.unwrap(),
                want_success,
                "{label}: has_success"
            );
            assert!(
                state.last_run("build").await.unwrap().is_some(),
                "{label}: every run records its outcome"
            );
        }
    }
}
