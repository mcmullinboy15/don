//! Per-task run supervision: the whole pipeline — prepare (resolve params,
//! hash inputs, decide whether to run at all) → spawn → wire → wait for
//! exit → record the outcome — owned by one task per task.
//!
//! Being the single producer of a task's messages, on the one lossless
//! report channel, is what deleted the old generation counters: a
//! completion can only arrive after its own prepared report and before
//! anything a later run produces. What remains in `task_commands` is the
//! part only the runner may do: transition process state, which drives the
//! cross-process dependency scheduler.

use super::TaskExit;
use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use crate::task_state::{TaskRunInfo, TaskStateStore};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// One request to run a task, as handed to its supervisor.
pub(crate) struct RunRequest {
    pub(crate) task_cfg: Box<crate::config::Task>,
    pub(crate) params: std::collections::HashMap<String, String>,
    pub(crate) mode: super::task_worker::TaskRunMode,
    pub(crate) intent: super::TaskRunIntent,
}

/// Owner half for tasks. See [`Supervisors`].
///
/// [`Supervisors`]: super::registry::Supervisors
pub(crate) type TaskSupervisors = super::registry::Supervisors<RunRequest>;

/// What the runner receives for a spawned, wired run. The supervisor keeps
/// the process handle and the output reader; this is what the runner's
/// bookkeeping (shadows for attach/status, spawn lines) needs.
pub(crate) struct TaskWired {
    pub(crate) pgid: i32,
    pub(crate) rendered_cmdline: String,
}

/// What a run request settled into, as reported to the runner. The spawned
/// case carries wired metadata, never the process — custody stays here.
pub(crate) enum TaskRunReport {
    PendingRun { message: String },
    Skipped { message: Option<String> },
    Running(TaskWired),
}

/// Start one run supervisor per task.
///
/// Every task gets one up front so the registry is immutable — see
/// [`Supervisors::spawn_all`].
///
/// [`Supervisors::spawn_all`]: super::registry::Supervisors::spawn_all
pub(crate) fn spawn_supervisors<'a>(
    names: impl Iterator<Item = &'a String>,
    ctx: &super::task_worker::TaskWorkerContext,
    outputs: &dyn Fn(&str) -> Option<crate::output::ProcessOutput>,
    config: &dyn Fn(&str) -> Option<StartupConfig>,
    report_tx: &mpsc::UnboundedSender<super::ProcessReport>,
    gates: &mut std::collections::HashMap<String, crate::gate::GateReader>,
) -> TaskSupervisors {
    TaskSupervisors::spawn_all(names, |name, rx, busy| {
        let output = outputs(&name);
        let startup = config(&name);
        let gate = gates.remove(&name);
        supervise(
            name,
            rx,
            ctx.clone(),
            output,
            report_tx.clone(),
            busy,
            startup,
            gate,
        )
    })
}

/// What a task needs to issue its own startup run when permitted.
pub(crate) struct StartupConfig {
    pub(crate) task_cfg: Box<crate::config::Task>,
    /// Whether any *blocking* dependent is waiting on this task. A
    /// non-blocking dependent is happy either way, so counting it would park
    /// a manual task as "required by dependents" and then block the very
    /// dependent that did not care. Fixed at construction, like the name set.
    pub(crate) has_dependents: bool,
}

/// Drive one task's runs, strictly in order.
///
/// The shape that matters is that a superseded run is **finished, not
/// aborted**. `run_task_worker` may already have spawned a process by the
/// time a newer request arrives; dropping that future would take the handle
/// with it and leave a child nothing will ever reap. So the worker always
/// runs to completion and the result is then killed off explicitly.
#[allow(clippy::too_many_arguments)]
async fn supervise(
    name: String,
    mut rx: mpsc::UnboundedReceiver<RunRequest>,
    ctx: super::task_worker::TaskWorkerContext,
    output: Option<crate::output::ProcessOutput>,
    report_tx: mpsc::UnboundedSender<super::ProcessReport>,
    busy: Arc<AtomicBool>,
    startup: Option<StartupConfig>,
    mut gate: Option<crate::gate::GateReader>,
) {
    let service_writer = output.as_ref().map(|output| output.writer());
    let mut pending: Option<RunRequest> = None;
    let mut mailbox_closed = false;
    // A task is wanted from the moment it exists; its startup evaluation
    // decides whether it actually needs to run.
    let mut demand = super::Demand::Scheduled;
    // See `crate::gate`: a level decided before this demand arose cannot have
    // accounted for it.
    let demand_rev: u64 = 0;

    loop {
        let request = match pending.take() {
            Some(request) => request,
            None => {
                // Idle only here: everywhere else there is work in hand.
                busy.store(false, Ordering::Relaxed);
                // Level read, exactly as the service side: permission means
                // "your dependencies are satisfied", and the *decision* to
                // run — skip-if-unchanged, auto_run, params — belongs to the
                // worker below, which already owns it.
                let permitted = startup
                    .as_ref()
                    .filter(|_| {
                        gate.as_ref().is_some_and(|g| {
                            let grant = g.get();
                            grant.rev > demand_rev && demand.permitted_by(grant.level)
                        })
                    })
                    .map(|startup| {
                        // One-shot, like the service side: a run is spent
                        // here, and only a fresh demand re-arms it.
                        demand = super::Demand::None;
                        RunRequest {
                            task_cfg: startup.task_cfg.clone(),
                            params: std::collections::HashMap::new(),
                            mode: super::task_worker::TaskRunMode::Startup {
                                has_dependents: startup.has_dependents,
                            },
                            intent: super::TaskRunIntent::Scheduled,
                        }
                    });
                match permitted {
                    Some(request) => {
                        busy.store(true, Ordering::Relaxed);
                        request
                    }
                    None => {
                        tokio::select! {
                            received = rx.recv() => match received {
                                Some(request) => {
                                    busy.store(true, Ordering::Relaxed);
                                    // A mailbox run supersedes standing
                                    // demand; withdrawing it here keeps the
                                    // task from running twice.
                                    demand = super::Demand::None;
                                    request
                                }
                                None => return,
                            },
                            // Permission changed; loop back to the level read.
                            changed = wait_gate(&mut gate), if gate.is_some() => {
                                if changed.is_none() {
                                    gate = None;
                                }
                                continue;
                            }
                        }
                    }
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

        if let Some(next) = superseded {
            if let Ok(prepared) = result {
                kill_superseded_spawn(&ctx.emitter, &name, prepared);
            }
            pending = Some(next);
            continue;
        }

        // Translate the worker's outcome into the runner-facing report; a
        // spawned run is wired here, by its owner, and held to exit.
        let (report, run) = match result {
            Ok(super::task_worker::TaskRunPrepared::PendingRun { message }) => {
                (Ok(TaskRunReport::PendingRun { message }), None)
            }
            Ok(super::task_worker::TaskRunPrepared::Skipped { message }) => (
                Ok(TaskRunReport::Skipped {
                    message: Some(message),
                }),
                None,
            ),
            Ok(super::task_worker::TaskRunPrepared::Spawned(spawn)) => {
                let super::task::TaskSpawn {
                    mut handle,
                    child_output,
                    rendered_cmdline,
                } = *spawn;
                let pgid = handle.pgid();
                // Wire the spawn: PTY input gate, server-side screen, OSC
                // scanner, output reader — all owned here now.
                let pty_write = handle.take_pty_write();
                let pty_input = match (pty_write, output.as_ref()) {
                    (Some(pty), Some(output)) => {
                        output.register_emulator(80, 24).await;
                        let pty_input = crate::output::spawn_pty_gate(pty);
                        // The scanner handle's drop removes its sink; tying it
                        // to this run's scope is exactly the lifetime we want.
                        let osc = output.add_osc_sink(pty_input.clone()).await;
                        // Attach goes through the output state, not the
                        // runner: register this run's gate for clients.
                        output.set_attach_pty(pty_input.clone()).await;
                        Some((pty_input, osc))
                    }
                    _ => None,
                };
                let reader = service_writer.as_ref().map(|writer| {
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        let _ = writer.process_stream(child_output).await;
                    })
                });
                let osc = pty_input.map(|(_, osc)| osc);
                (
                    Ok(TaskRunReport::Running(TaskWired {
                        pgid,
                        rendered_cmdline,
                    })),
                    Some((handle, reader, osc)),
                )
            }
            Err(message) => (Err(message), None),
        };

        // Everything the exit half needs, owned before the request's parts
        // move into the prepared report.
        let outcome = run.as_ref().map(|(handle, _, _)| TaskRunOutcome {
            name: name.clone(),
            task_cfg: (*task_cfg).clone(),
            base_dir: ctx.base_dir.clone(),
            global_watch_ignore: ctx.global_watch_ignore.clone(),
            pgid: handle.pgid(),
            report_tx: report_tx.clone(),
            rerun: matches!(intent, super::TaskRunIntent::Background),
        });

        if report_tx
            .send(super::ProcessReport::TaskRunPrepared {
                name: name.clone(),
                task_cfg: task_cfg.clone(),
                intent,
                result: report,
            })
            .is_err()
        {
            return;
        }

        // Hold the run to exit. A request arriving mid-run parks and runs
        // strictly after — owning the exit is what makes run N+1 unable to
        // start early, which is the race the old `run_requested` flag and
        // duplicate-pgid guard papered over.
        let Some((mut handle, reader, osc)) = run else {
            continue;
        };
        let Some(outcome) = outcome else { continue };
        let timeout = task_cfg.timeout.clone();
        let start = std::time::Instant::now();
        let wait = super::task::wait_for_task(&mut handle, timeout.as_deref());
        tokio::pin!(wait);
        let result = loop {
            tokio::select! {
                result = &mut wait => break result,
                next = rx.recv(), if !mailbox_closed => match next {
                    Some(next) => pending = Some(next),
                    None => mailbox_closed = true,
                },
            }
        };
        // Drain the reader before reporting, so "complete" never outruns
        // the task's final output. Then the scanner handle drops with this
        // scope, removing its sink.
        if let Some(reader) = reader {
            await_reader(reader).await;
        }
        drop(osc);
        // The run is over: unregister attach so new clients are refused and
        // muted stdout resumes before the completion message lands.
        if let Some(output) = output.as_ref() {
            output.clear_attach().await;
        }
        outcome.finish(result, start.elapsed()).await;
    }
}

/// Wait on a gate slot, parking forever when there is none — so an absent
/// gate never completes and never consumes its `select!` branch.
async fn wait_gate(gate: &mut Option<crate::gate::GateReader>) -> Option<()> {
    match gate.as_mut() {
        Some(gate) => gate.changed().await,
        None => std::future::pending().await,
    }
}

/// Join the finished reader, bounded — a wedged sink must not hold the
/// supervisor hostage.
async fn await_reader(handle: tokio::task::JoinHandle<()>) {
    let mut handle = handle;
    if tokio::time::timeout(std::time::Duration::from_secs(2), &mut handle)
        .await
        .is_err()
    {
        handle.abort();
        let _ = handle.await;
    }
}

/// How prominently a settled run's message is reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Report {
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
pub(crate) struct NoSpawnOutcome {
    /// Lifecycle state the task enters.
    pub(crate) state: super::TaskState,
    /// What the dependency scheduler is told. A skipped or deferred task is
    /// still a *success* — it didn't fail, it just didn't run.
    pub(crate) success: bool,
    pub(crate) message: String,
    pub(crate) report: Report,
}

impl NoSpawnOutcome {
    /// The task can't run yet and is waiting on something.
    pub(crate) fn pending_run(message: String) -> Self {
        Self {
            state: super::TaskState::PendingRun,
            success: true,
            message,
            report: Report::Info,
        }
    }

    /// The task's watched inputs were unchanged, so it didn't need to run.
    pub(crate) fn skipped(message: String) -> Self {
        Self {
            state: super::TaskState::Skipped,
            success: true,
            message,
            report: Report::Debug,
        }
    }

    /// Preparing the run failed before anything was spawned.
    pub(crate) fn failed(message: String) -> Self {
        Self {
            state: super::TaskState::Failed,
            success: false,
            message,
            report: Report::Error,
        }
    }

    /// Whether to update `needs_run_now`, and to what. `None` leaves it alone.
    ///
    /// A run that failed to prepare has not run, however it was triggered, so
    /// the task still needs one. This used to depend on *who asked*: a
    /// scheduled failure set the flag and a background `don run` failure left
    /// it alone, which meant a task could fail under `don run` and the next
    /// startup sweep would see nothing outstanding and skip it.
    pub(crate) fn needs_run_now(&self) -> Option<bool> {
        match self.state {
            super::TaskState::PendingRun | super::TaskState::Failed => Some(true),
            super::TaskState::Skipped => Some(false),
            _ => None,
        }
    }

    /// Emit this outcome's message at its own level.
    pub(crate) fn emit(&self, emitter: &crate::output::LifecycleEmitter, name: &str) {
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
/// waiting out a grace period there would stall every other process.
///
/// Takes the untagged emitter rather than an `ProcessOutput` so the kill can
/// never be gated on a name lookup succeeding — failing to log is a cosmetic
/// problem, failing to kill leaks a process nothing will ever reap.
pub(crate) fn kill_superseded_spawn(
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
        // Nothing was spawned, so there is nothing to clean up.
        TaskRunPrepared::PendingRun { .. } | TaskRunPrepared::Skipped { .. } => {}
    }
}

/// Everything the exit half of a task run needs, owned outright.
///
/// Owned rather than borrowed because this outlives the runner's command loop
/// — the exit wait is a detached task, and holding a reference into runner
/// state across it is what the whole decomposition is trying to stop.
pub(crate) struct TaskRunOutcome {
    pub(crate) name: String,
    pub(crate) task_cfg: crate::config::Task,
    pub(crate) base_dir: PathBuf,
    pub(crate) global_watch_ignore: Vec<String>,
    /// Process group of the run that just ended.
    pub(crate) pgid: i32,
    /// Exit reports for non-scheduled runs travel on the processes' lossless
    /// report channel, like service exits.
    pub(crate) report_tx: mpsc::UnboundedSender<super::ProcessReport>,
    /// Whether this run was triggered by a file watch, which decides if a
    /// `TaskRerunComplete` event is broadcast when it lands.
    pub(crate) rerun: bool,
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
    pub(crate) async fn finish(
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

        let task_state = TaskStateStore::new(self.base_dir.join(".don").join("task-state"));
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

        let _ = self
            .report_tx
            .send(super::ProcessReport::TaskExited(TaskExit {
                name: self.name,
                pgid: self.pgid,
                success,
                message,
                elapsed: Some(elapsed),
                last_run: Some(last_run),
                rerun: self.rerun,
            }));
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
        report_tx: mpsc::UnboundedSender<super::super::ProcessReport>,
        rerun: bool,
    ) -> TaskRunOutcome {
        TaskRunOutcome {
            name: name.to_string(),
            task_cfg: test_task(),
            base_dir: base_dir.to_path_buf(),
            global_watch_ignore: Vec::new(),
            pgid: 4242,
            report_tx,
            rerun,
        }
    }

    #[test]
    fn no_spawn_outcomes_classify_consistently() {
        use super::super::TaskState;

        struct Case {
            label: &'static str,
            outcome: NoSpawnOutcome,
            want_state: TaskState,
            want_success: bool,
            want_report: Report,
            want_needs: Option<bool>,
        }

        let cases = vec![
            Case {
                label: "deferred",
                outcome: NoSpawnOutcome::pending_run("waiting on deps".to_string()),
                want_state: TaskState::PendingRun,
                // Not a failure: it just hasn't run yet.
                want_success: true,
                want_report: Report::Info,
                want_needs: Some(true),
            },
            Case {
                label: "skipped",
                outcome: NoSpawnOutcome::skipped("no changes".to_string()),
                want_state: TaskState::Skipped,
                want_success: true,
                // Verbose-only: nobody asked for a no-op to be announced.
                want_report: Report::Debug,
                want_needs: Some(false),
            },
            Case {
                label: "prepare failed",
                outcome: NoSpawnOutcome::failed("bad param".to_string()),
                want_state: TaskState::Failed,
                want_success: false,
                want_report: Report::Error,
                // A failed run hasn't run, whoever asked for it — so the
                // task still needs one. This used to be `None` for a
                // background `don run`, which let the next startup sweep
                // skip a task that had just failed.
                want_needs: Some(true),
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
                case.outcome.needs_run_now(),
                case.want_needs,
                "{}: needs_run_now",
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
        let temp = tempfile::tempdir().unwrap();
        let output = crate::output::OutputManager::new(&[], tokio::io::sink())
            .await
            .unwrap();
        let ctx = super::super::task_worker::TaskWorkerContext {
            base_dir: temp.path().to_path_buf(),
            platform: crate::config::Platform::LinuxX86_64,
            emitter: output.clone_lifecycle_emitter(),
            global_watch_ignore: Vec::new(),
            endpoints: {
                let (writer, reader) = crate::endpoints::channel();
                // Keep the writer alive for the reader's lifetime.
                std::mem::forget(writer);
                reader
            },
        };
        let names = ["build".to_string(), "migrate".to_string()];
        let (report_tx, _report_rx) = mpsc::unbounded_channel();
        let mut gates = std::collections::HashMap::new();
        let mut supervisors = spawn_supervisors(
            names.iter(),
            &ctx,
            &|_| None,
            &|_| None,
            &report_tx,
            &mut gates,
        );
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

    /// The bug this classifier used to encode, stated as the behaviour a
    /// user would see: a task whose preparation fails under `don run` must
    /// still look outstanding to the next startup sweep. Previously the
    /// background case returned `None`, leaving `needs_run_now` false, so a
    /// task that had just failed was treated as satisfied.
    #[test]
    fn a_failed_run_leaves_the_task_needing_one_however_it_was_triggered() {
        let failed = NoSpawnOutcome::failed("bad param".to_string());
        assert_eq!(
            failed.needs_run_now(),
            Some(true),
            "a failed run has not run, whoever asked for it"
        );
    }

    /// Every finished run reports exactly one `TaskExited` on the report
    /// channel — arrival order there IS the fold order, which is what let
    /// the run/done split (and its generation guard) be deleted.
    #[tokio::test]
    async fn a_finished_run_reports_exactly_once() {
        struct Case {
            name: &'static str,
            rerun: bool,
            status: std::process::ExitStatus,
            want_success: bool,
            want_message: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "scheduled success",
                rerun: false,
                status: ExitStatusExt::from_raw(0),
                want_success: true,
                want_message: None,
            },
            Case {
                name: "scheduled failure carries the exit code",
                rerun: false,
                status: ExitStatusExt::from_raw(3 << 8),
                want_success: false,
                want_message: Some("exit code 3"),
            },
            Case {
                name: "rerun success",
                rerun: true,
                status: ExitStatusExt::from_raw(0),
                want_success: true,
                want_message: None,
            },
            Case {
                name: "rerun failure",
                rerun: true,
                status: ExitStatusExt::from_raw(1 << 8),
                want_success: false,
                want_message: Some("exit code 1"),
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let (report_tx, mut report_rx) = mpsc::unbounded_channel();

            outcome("build", temp.path(), report_tx, case.rerun)
                .finish(Ok(case.status), Duration::from_millis(5))
                .await;

            let Ok(super::super::ProcessReport::TaskExited(exit)) = report_rx.try_recv() else {
                panic!("{}: expected a TaskExited", case.name);
            };
            assert_eq!(exit.name, "build", "{}", case.name);
            assert_eq!(exit.pgid, 4242, "{}", case.name);
            assert_eq!(exit.success, case.want_success, "{}", case.name);
            assert_eq!(exit.message.as_deref(), case.want_message, "{}", case.name);
            assert_eq!(exit.rerun, case.rerun, "{}", case.name);
            assert!(
                report_rx.try_recv().is_err(),
                "{}: exactly one report per run",
                case.name
            );
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
            let (report_tx, _report_rx) = mpsc::unbounded_channel();
            outcome("build", temp.path(), report_tx, false)
                .finish(Ok(status), Duration::from_millis(1))
                .await;

            let state = TaskStateStore::new(temp.path().join(".don").join("task-state"));
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
