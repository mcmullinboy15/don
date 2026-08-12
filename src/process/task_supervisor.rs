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
    /// Someone waiting for this run to finish (`don run --wait`).
    pub(crate) wait: Option<RunWait>,
    /// What to say when this run is picked up, before preparing it.
    ///
    /// Preparation hashes files and resolves downloads, so a triggered run
    /// that said nothing until it was ready to spawn would look ignored.
    /// `None` for the startup sweep, which the scheduler is already
    /// narrating.
    pub(crate) start_message: Option<String>,
}

/// A caller blocked on a run's outcome.
///
/// The supervisor holds this for the run it belongs to, which is what makes
/// the old `waiter_token` unnecessary: it ran one run at a time and the
/// timeout was a detached task reporting through a shared channel, so the
/// runner needed an identity to match answers to askers. One actor holding
/// both the timer and the run needs no such thing.
pub(crate) struct RunWait {
    pub(crate) reply: tokio::sync::oneshot::Sender<crate::command::CommandResult>,
    /// Parsed `--wait` deadline; `None` waits indefinitely.
    pub(crate) timeout: Option<(std::time::Duration, String)>,
}

/// What a task's supervisor can be asked to do.
///
/// A run used to be the only thing in this mailbox, because killing a run and
/// restarting one were done *to* the task by the scheduler: it kept the run's
/// pgid and the parameters the last run used, and signalled the process group
/// itself. Both of those are things the owner of the run already has, so both
/// arrive here now and the scheduler keeps neither.
pub(crate) enum TaskCommand {
    /// Run this task. A run already in flight is superseded.
    Run(RunRequest),
    /// This task's watched files changed.
    ///
    /// Whether that means a run is decided here, from three facts this
    /// supervisor already has: the task's `auto_run` policy, whether it
    /// declares params a watch event cannot supply, and whether its artifact
    /// is still being built. The scheduler used to answer all three, which
    /// meant the file watcher had to wait to hear how it went.
    Rerun,
    /// This task's build-graph definition files changed, so the watch
    /// patterns resolved from them may no longer be right. The supervisor
    /// asks the build manager to re-query and re-runs if they moved.
    BuildGraphChanged,
    /// End the run in flight, if any, then run again with the parameters the
    /// last run used.
    ///
    /// The "no previous invocation to restart" check comes with it: the
    /// parameters it reads about are held here.
    Restart {
        reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
    },
    /// End the run in flight, if any, and do not run again — teardown.
    ///
    /// `done` fires once the run is gone. Teardown waits on it: the
    /// supervisors are aborted immediately afterwards, and aborting one that
    /// has not read this yet would drop a live process on the floor.
    Kill {
        done: Option<tokio::sync::oneshot::Sender<()>>,
    },
}

/// Owner half for tasks. See [`Supervisors`].
///
/// [`Supervisors`]: super::registry::Supervisors
pub(crate) type TaskSupervisors = super::registry::Supervisors<TaskCommand>;

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
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_supervisors<'a>(
    names: impl Iterator<Item = &'a String>,
    ctx: &super::task_worker::TaskWorkerContext,
    outputs: &dyn Fn(&str) -> Option<crate::output::ProcessOutput>,
    config: &dyn Fn(&str) -> Option<StartupConfig>,
    report_tx: &mpsc::UnboundedSender<super::ProcessReport>,
    gates: &mut std::collections::HashMap<String, crate::gate::GateReader>,
    shutdown_rx: &tokio::sync::watch::Receiver<bool>,
    batcher_tx: &mpsc::UnboundedSender<crate::build_tool::batcher::BatchRequest>,
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
            shutdown_rx.clone(),
            batcher_tx.clone(),
        )
    })
}

/// Ask the build manager to re-resolve this task's watch paths.
fn request_requery(
    name: &str,
    task_cfg: &crate::config::Task,
    ctx: &super::task_worker::TaskWorkerContext,
    batcher_tx: &mpsc::UnboundedSender<crate::build_tool::batcher::BatchRequest>,
    outcome: &mpsc::UnboundedSender<crate::build_tool::batch::RequeryOutcome>,
) {
    let working_dir = working_dir_for(&ctx.base_dir, task_cfg.dir.as_deref());
    let ignore_patterns = resolve_watch_ignore_patterns(
        &working_dir,
        &task_cfg.ignore,
        &ctx.base_dir,
        &ctx.global_watch_ignore,
    );
    let _ = batcher_tx.send(crate::build_tool::batcher::BatchRequest::QueueRequery {
        item: crate::build_tool::batch::GraphRequeryRequestItem {
            name: name.to_string(),
            kind: super::ProcessKind::Task,
            bazel: task_cfg.bazel.clone(),
            watch_enabled: task_cfg.build_tool_watch_enabled(),
            working_dir,
            ignore_patterns,
            global_watch_ignore: ctx.global_watch_ignore.clone(),
        },
        outcome: outcome.clone(),
    });
}

/// Ask the build manager for this task's artifact, and tell the scheduler a
/// build is under way. Returns whether a request is now outstanding.
///
/// The service side's rule applies unchanged: asked for at construction, not
/// at gate-open, so the whole workspace coalesces into one invocation. See
/// [`super::service_supervisor`].
fn request_artifact(
    name: &str,
    task_cfg: &crate::config::Task,
    ctx: &super::task_worker::TaskWorkerContext,
    outcome: &mpsc::UnboundedSender<crate::build_tool::batch::PrepareOutcome>,
    report_tx: &mpsc::UnboundedSender<super::ProcessReport>,
    batcher_tx: &mpsc::UnboundedSender<crate::build_tool::batcher::BatchRequest>,
) -> bool {
    if report_tx
        .send(super::ProcessReport::ArtifactBuild {
            name: name.to_string(),
            kind: super::ProcessKind::Task,
            status: super::ArtifactBuildStatus::Started,
        })
        .is_err()
    {
        return false;
    }
    let working_dir = working_dir_for(&ctx.base_dir, task_cfg.dir.as_deref());
    let ignore = resolve_watch_ignore_patterns(
        &working_dir,
        &task_cfg.ignore,
        &ctx.base_dir,
        &ctx.global_watch_ignore,
    );
    batcher_tx
        .send(crate::build_tool::batcher::BatchRequest::QueuePrepare {
            item: Box::new(crate::build_tool::batch::BatchBuildItem {
                name: name.to_string(),
                kind: super::ProcessKind::Task,
                bazel: task_cfg.bazel.clone(),
                watch_enabled: task_cfg.build_tool_watch_enabled(),
                working_dir,
                ignore,
            }),
            outcome: outcome.clone(),
        })
        .is_ok()
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

/// What a command means once resolved against what this supervisor holds —
/// the task's config and the parameters its last run used.
///
/// Resolving up front is what stops the three places a command can arrive
/// (idle, mid-preparation, mid-run) from each re-deriving "what does a
/// restart mean here".
enum Ask {
    /// Start this run.
    Run(RunRequest),
    /// Ask the build manager to re-resolve this task's watch paths.
    Requery,
    /// Do not run; park the task for a manual trigger and say why.
    ///
    /// Reported as an ordinary prepared-run outcome, so the scheduler folds
    /// it into `PendingRun` exactly as it does one this supervisor reaches by
    /// preparing a run and finding nothing to do.
    Park(String),
    /// End the run in hand; start `then` once it is gone, and fire `done`
    /// when it is.
    Cancel {
        then: Option<RunRequest>,
        done: Option<tokio::sync::oneshot::Sender<()>>,
    },
    /// Nothing to do. Any reply has already been answered.
    Nothing,
}

/// Answer a command's reply channel, if it had one.
fn answer(
    reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
    result: crate::command::CommandResult,
) {
    if let Some(reply) = reply {
        let _ = reply.send(result);
    }
}

fn resolve_command(
    command: TaskCommand,
    name: &str,
    startup: Option<&StartupConfig>,
    last_params: &std::collections::HashMap<String, String>,
    awaiting_artifact: bool,
) -> Ask {
    match command {
        TaskCommand::Run(request) => Ask::Run(request),
        TaskCommand::Kill { done } => Ask::Cancel { then: None, done },
        TaskCommand::BuildGraphChanged => Ask::Requery,
        TaskCommand::Rerun => {
            let Some(startup) = startup else {
                return Ask::Nothing;
            };
            // The artifact this run would use is still being built. Its
            // completion starts the run; a second one here would race it.
            if awaiting_artifact {
                return Ask::Nothing;
            }
            // Only `auto_run = true` / `"always"` re-runs from a watch event.
            // `"once"` is startup-only and `false` / `"never"` is manual
            // forever — both park instead.
            if !startup.task_cfg.auto_run.runs_automatically_on_watch() {
                return Ask::Park(
                    match startup.task_cfg.auto_run {
                        crate::config::TaskAutoRun::Always => "files changed (pending)",
                        crate::config::TaskAutoRun::Never => {
                            "files changed (pending — auto_run = false)"
                        }
                        crate::config::TaskAutoRun::Once => {
                            "files changed (pending — auto_run = once)"
                        }
                    }
                    .to_string(),
                );
            }
            // A task with params needs values a file change cannot supply.
            if !startup.task_cfg.params.is_empty() {
                return Ask::Park(
                    "files changed (pending — task has params, run manually)".to_string(),
                );
            }
            // No hash check: the watcher already confirmed a matching file
            // changed. That check exists for startup, to skip a task whose
            // inputs have not moved since its last run.
            Ask::Run(RunRequest {
                task_cfg: startup.task_cfg.clone(),
                params: std::collections::HashMap::new(),
                mode: super::task_worker::TaskRunMode::Triggered,
                intent: super::TaskRunIntent::Background,
                wait: None,
                start_message: Some("re-running (file changed)".to_string()),
            })
        }
        TaskCommand::Restart { reply } => {
            let Some(startup) = startup else {
                answer(
                    reply,
                    Err(crate::command::CommandError::UnknownTask {
                        name: name.to_string(),
                    }),
                );
                return Ask::Nothing;
            };
            // A param'd task has nothing to reuse until it has been run once
            // with values supplied.
            if !startup.task_cfg.params.is_empty()
                && last_params.len() < startup.task_cfg.params.len()
            {
                answer(
                    reply,
                    Err(crate::command::CommandError::InvalidState {
                        name: name.to_string(),
                        message:
                            "task has params and no previous invocation to restart; use `don run`"
                                .to_string(),
                    }),
                );
                return Ask::Nothing;
            }
            // Accepted: answered now, as it was when the scheduler executed
            // the restart itself. The run's own outcome travels separately.
            answer(reply, Ok(()));
            Ask::Cancel {
                done: None,
                then: Some(RunRequest {
                    task_cfg: startup.task_cfg.clone(),
                    params: last_params.clone(),
                    mode: super::task_worker::TaskRunMode::Triggered,
                    intent: super::TaskRunIntent::Background,
                    wait: None,
                    start_message: Some("restarting (manual trigger)".to_string()),
                }),
            }
        }
    }
}

/// SIGKILL a run this supervisor is holding.
///
/// The supervisor owns the process, so it signals the group directly rather
/// than asking anyone: the pgid is its own, and the `wait` it is already
/// parked on is what reaps the result.
fn kill_run(emitter: &crate::output::LifecycleEmitter, name: &str, pgid: i32) {
    emitter.service_event(name, &format!("send SIGKILL to task pgid {pgid}"));
    if let Err(e) = nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(pgid),
        nix::sys::signal::Signal::SIGKILL,
    ) && e != nix::Error::ESRCH
    {
        emitter.service_error_event(name, &format!("failed to kill task pgid {pgid}: {e}"));
    }
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
    mut rx: mpsc::UnboundedReceiver<TaskCommand>,
    ctx: super::task_worker::TaskWorkerContext,
    output: Option<crate::output::ProcessOutput>,
    report_tx: mpsc::UnboundedSender<super::ProcessReport>,
    busy: Arc<AtomicBool>,
    startup: Option<StartupConfig>,
    mut gate: Option<crate::gate::GateReader>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    batcher_tx: mpsc::UnboundedSender<crate::build_tool::batcher::BatchRequest>,
) {
    let service_writer = output.as_ref().map(|output| output.writer());
    let mut pending: Option<RunRequest> = None;
    let mut mailbox_closed = false;
    // The parameters the last run used, for a restart to reuse. Held here
    // because a restart is executed here; the scheduler kept a copy only to
    // hand it back on the way in.
    let mut last_params: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    // Where the build manager delivers this task's artifact. A task with a
    // bazel target needs it built before it runs, exactly like a service.
    let (prepare_tx, mut prepare_rx) =
        mpsc::unbounded_channel::<crate::build_tool::batch::PrepareOutcome>();
    // Where the build manager delivers this task's share of a build-graph
    // re-query.
    let (requery_tx, mut requery_rx) =
        mpsc::unbounded_channel::<crate::build_tool::batch::RequeryOutcome>();
    let mut awaiting_artifact = match startup.as_ref() {
        Some(startup) if startup.task_cfg.bazel.is_some() => request_artifact(
            &name,
            &startup.task_cfg,
            &ctx,
            &prepare_tx,
            &report_tx,
            &batcher_tx,
        ),
        _ => false,
    };
    // A task is wanted from the moment it exists; its startup evaluation
    // decides whether it actually needs to run.
    let mut demand = super::Demand::Scheduled;
    // See `crate::gate`: a level decided before this demand arose cannot have
    // accounted for it.
    let demand_rev: u64 = 0;
    // Whoever is blocked on the run in hand, if anyone.
    let mut waiter: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>> = None;

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
                    .filter(|_| !awaiting_artifact)
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
                            wait: None,
                            task_cfg: startup.task_cfg.clone(),
                            params: std::collections::HashMap::new(),
                            mode: super::task_worker::TaskRunMode::Startup {
                                has_dependents: startup.has_dependents,
                            },
                            intent: super::TaskRunIntent::Scheduled,
                            start_message: None,
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
                                Some(command) => {
                                    // Nothing is in hand, so a cancel has
                                    // nothing to kill — only its follow-up
                                    // run, if it has one, survives.
                                    match resolve_command(
                                        command,
                                        &name,
                                        startup.as_ref(),
                                        &last_params,
                                        awaiting_artifact,
                                    ) {
                                        Ask::Run(request)
                                        | Ask::Cancel { then: Some(request), .. } => {
                                            busy.store(true, Ordering::Relaxed);
                                            // A mailbox run supersedes standing
                                            // demand; withdrawing it here keeps
                                            // the task from running twice.
                                            demand = super::Demand::None;
                                            request
                                        }
                                        Ask::Cancel { then: None, done } => {
                                            // Nothing in hand: the kill is
                                            // already true.
                                            if let Some(done) = done {
                                                let _ = done.send(());
                                            }
                                            continue;
                                        }
                                        Ask::Requery => {
                                            if let Some(startup) = startup.as_ref() {
                                                request_requery(
                                                    &name,
                                                    &startup.task_cfg,
                                                    &ctx,
                                                    &batcher_tx,
                                                    &requery_tx,
                                                );
                                            }
                                            continue;
                                        }
                                        Ask::Park(message) => {
                                            if report_tx
                                                .send(super::ProcessReport::TaskRunPrepared {
                                                    name: name.clone(),
                                                    task_cfg: match startup.as_ref() {
                                                        Some(startup) => {
                                                            startup.task_cfg.clone()
                                                        }
                                                        // `Park` is only ever
                                                        // resolved with one.
                                                        None => continue,
                                                    },
                                                    intent: super::TaskRunIntent::Background,
                                                    result: Ok(TaskRunReport::PendingRun {
                                                        message,
                                                    }),
                                                })
                                                .is_err()
                                            {
                                                return;
                                            }
                                            continue;
                                        }
                                        Ask::Nothing => continue,
                                    }
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
                            // A re-query this supervisor asked for. The new
                            // patterns are already registered; a graph that
                            // moved means the task should run against it.
                            outcome = requery_rx.recv() => {
                                use crate::build_tool::batch::RequeryOutcome;
                                if outcome != Some(RequeryOutcome::Updated) {
                                    continue;
                                }
                                ctx.emitter
                                    .service_event(&name, "build graph changed — re-running");
                                // Same policy a watched-file change goes
                                // through: auto_run and declared params still
                                // decide whether this means a run.
                                match resolve_command(
                                    TaskCommand::Rerun,
                                    &name,
                                    startup.as_ref(),
                                    &last_params,
                                    awaiting_artifact,
                                ) {
                                    Ask::Run(request) => {
                                        busy.store(true, Ordering::Relaxed);
                                        demand = super::Demand::None;
                                        request
                                    }
                                    _ => continue,
                                }
                            }
                            // This task's artifact, from the build manager.
                            outcome = prepare_rx.recv() => {
                                use crate::build_tool::batch::PrepareOutcome;
                                let Some(outcome) = outcome else { continue };
                                let status = match outcome {
                                    // Nothing to record: a task runs the
                                    // command it was configured with, and the
                                    // build only had to make the target exist.
                                    PrepareOutcome::Ready { .. } => {
                                        awaiting_artifact = false;
                                        super::ArtifactBuildStatus::Ready
                                    }
                                    // Sources changed mid-build; the build
                                    // manager said so. Ask again.
                                    PrepareOutcome::Stale => {
                                        awaiting_artifact = match startup.as_ref() {
                                            Some(startup) => request_artifact(
                                                &name,
                                                &startup.task_cfg,
                                                &ctx,
                                                &prepare_tx,
                                                &report_tx,
                                                &batcher_tx,
                                            ),
                                            None => false,
                                        };
                                        continue;
                                    }
                                    PrepareOutcome::Failed(message) => {
                                        awaiting_artifact = false;
                                        // Not retried — see the service side.
                                        demand = super::Demand::None;
                                        super::ArtifactBuildStatus::Failed(message)
                                    }
                                };
                                if report_tx
                                    .send(super::ProcessReport::ArtifactBuild {
                                        name: name.clone(),
                                        kind: super::ProcessKind::Task,
                                        status,
                                    })
                                    .is_err()
                                {
                                    return;
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
            wait: request_wait,
            start_message,
        } = request;

        // A triggered run announces itself before preparing, and takes over
        // as the run a restart would reuse. Both used to happen on the
        // scheduler, which is why it kept a copy of the parameters.
        if matches!(intent, super::TaskRunIntent::Background) {
            last_params = params.clone();
        }
        if let Some(message) = start_message {
            // Any follower of the previous run should end cleanly before the
            // next process starts writing.
            if let Some(writer) = service_writer.as_ref() {
                writer.close_follow_sinks().await;
            }
            if report_tx
                .send(super::ProcessReport::TaskStarting {
                    name: name.clone(),
                    message,
                })
                .is_err()
            {
                return;
            }
        }

        let task_cfg_for_worker = task_cfg.clone();
        let worker = super::task_worker::run_task_worker(
            ctx.clone(),
            &name,
            task_cfg_for_worker.as_ref(),
            &params,
            mode,
        );
        tokio::pin!(worker);

        // Watch for a newer command while the current run prepares, keeping
        // only the most recent — anything older is already superseded too.
        let mut superseded: Option<RunRequest> = None;
        // A cancel that lands mid-preparation cannot stop the spawn (dropping
        // the worker would take the handle with it), so it is recorded and
        // paid out below once preparation has finished.
        let mut abandoned = false;
        let mut cancel_done: Option<tokio::sync::oneshot::Sender<()>> = None;
        let result = loop {
            tokio::select! {
                result = &mut worker => break result,
                next = rx.recv(), if !mailbox_closed => match next {
                    Some(command) => {
                        match resolve_command(
                            command, &name, startup.as_ref(), &last_params, false,
                        ) {
                            Ask::Run(request) => superseded = Some(request),
                            Ask::Cancel { then, done } => {
                                abandoned = true;
                                superseded = then;
                                cancel_done = done.or(cancel_done);
                            }
                            // A run is already being prepared, so there is
                            // nothing to park — it will settle on its own —
                            // and a graph change it should react to arrives
                            // again as its own re-query outcome.
                            Ask::Park(_) | Ask::Requery | Ask::Nothing => {}
                        }
                    }
                    // Guarded so a closed mailbox doesn't spin this select:
                    // `recv` on a closed channel returns immediately, forever.
                    None => mailbox_closed = true,
                },
            }
        };

        if abandoned || superseded.is_some() {
            if let Ok(prepared) = result {
                kill_superseded_spawn(&ctx.emitter, &name, prepared);
            }
            if let Some(done) = cancel_done {
                let _ = done.send(());
            }
            pending = superseded;
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
        // This run supersedes whatever the last one's waiter was told to
        // expect. Answering here rather than leaving it to a fold is what
        // lets the token go: only one run is ever in hand.
        if let Some(previous) = waiter.take() {
            let _ = previous.send(Err(crate::command::CommandError::Failed {
                name: name.clone(),
                message: "task run was superseded".to_string(),
            }));
        }
        let mut wait_deadline = None;
        if let Some(run_wait) = request_wait {
            waiter = Some(run_wait.reply);
            wait_deadline = run_wait.timeout;
        }
        let Some(outcome) = outcome else { continue };
        let timeout = task_cfg.timeout.clone();
        let start = std::time::Instant::now();
        // Captured before the wait borrows the handle: a cancel arriving
        // mid-run signals the group this supervisor owns.
        let pgid = outcome.pgid;
        let mut cancelled = false;
        let wait = super::task::wait_for_task(&mut handle, timeout.as_deref());
        tokio::pin!(wait);
        let deadline = wait_deadline
            .as_ref()
            .map(|(duration, _)| tokio::time::Instant::now() + *duration);
        let result = loop {
            tokio::select! {
                result = &mut wait => break result,
                // The `--wait` deadline. The run itself continues: a caller
                // giving up waiting is not a reason to kill their task.
                () = wait_until(&deadline), if waiter.is_some() && deadline.is_some() => {
                    if let (Some(reply), Some((_, spelling))) =
                        (waiter.take(), wait_deadline.as_ref())
                    {
                        let _ = reply.send(Err(crate::command::CommandError::TimedOut {
                            name: name.clone(),
                            timeout: spelling.clone(),
                        }));
                    }
                }
                // Teardown: answer the caller now, while there is still a
                // channel to answer on.
                _ = shutdown_rx.changed(), if waiter.is_some() => {
                    if *shutdown_rx.borrow()
                        && let Some(reply) = waiter.take()
                    {
                        let _ = reply.send(Err(crate::command::CommandError::Failed {
                            name: name.clone(),
                            message: "run cancelled by shutdown".to_string(),
                        }));
                    }
                }
                next = rx.recv(), if !mailbox_closed => match next {
                    Some(command) => {
                        match resolve_command(
                            command, &name, startup.as_ref(), &last_params, false,
                        ) {
                            // A run queued behind this one starts strictly
                            // after it — owning the exit is what makes that
                            // ordering structural rather than checked.
                            Ask::Run(request) => pending = Some(request),
                            Ask::Cancel { then, done } => {
                                if !cancelled {
                                    cancelled = true;
                                    // A cancel that runs again narrates the
                                    // stop; teardown narrates in bulk.
                                    if then.is_some() {
                                        ctx.emitter
                                            .service_event(&name, "stopping... (requested)");
                                    }
                                    kill_run(&ctx.emitter, &name, pgid);
                                }
                                cancel_done = done.or(cancel_done);
                                pending = then;
                            }
                            // Running now; nothing to park.
                            Ask::Park(_) | Ask::Requery | Ask::Nothing => {}
                        }
                    }
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
        if cancelled {
            // A run somebody ended is not an outcome to fold: its exit status
            // describes the SIGKILL, not the task, and the kill was narrated
            // where it happened. Nothing is recorded either — the scheduler
            // used to reach the same result by dropping the exit report,
            // having compared its pgid against a copy it kept.
            if let Some(reply) = waiter.take() {
                let _ = reply.send(Err(crate::command::CommandError::Failed {
                    name: name.clone(),
                    message: "task run was cancelled".to_string(),
                }));
            }
            if let Some(done) = cancel_done.take() {
                let _ = done.send(());
            }
            continue;
        }
        outcome.finish(result, start.elapsed(), waiter.take()).await;
    }
}

/// Sleep until a `--wait` deadline, parking forever when there is none.
async fn wait_until(deadline: &Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(*deadline).await,
        None => std::future::pending().await,
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
        reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
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
                success,
                message,
                elapsed: Some(elapsed),
                last_run: Some(last_run),
                reply,
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

    /// A task declaring one required param, for the restart-reuse rules.
    fn param_task() -> crate::config::Task {
        let config: crate::config::Config =
            "[tasks.seed]\ncmd = \"true\"\n[[tasks.seed.params]]\nname = \"env\"\nrequired = true\n"
                .parse()
                .unwrap();
        config.tasks.get("seed").unwrap().clone()
    }

    /// Every command, resolved against what the supervisor holds. This is the
    /// whole of what moved off the scheduler: a restart's meaning depends on
    /// the parameters of the last run, which live here now.
    #[tokio::test]
    async fn commands_resolve_against_what_the_supervisor_holds() {
        struct Case {
            name: &'static str,
            command: fn(
                Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
            ) -> TaskCommand,
            task: crate::config::Task,
            last_params: Vec<(&'static str, &'static str)>,
            want: &'static str,
            /// Parameters the resolved run carries, when it makes one.
            want_params: Vec<(&'static str, &'static str)>,
            want_reply: Option<bool>,
        }

        let cases = vec![
            Case {
                name: "a run is a run",
                command: |_| {
                    TaskCommand::Run(RunRequest {
                        task_cfg: Box::new(test_task()),
                        params: std::collections::HashMap::new(),
                        mode: super::super::task_worker::TaskRunMode::Triggered,
                        intent: super::super::TaskRunIntent::Background,
                        wait: None,
                        start_message: None,
                    })
                },
                task: test_task(),
                last_params: vec![],
                want: "run",
                want_params: vec![],
                want_reply: None,
            },
            Case {
                name: "a kill cancels and does not run again",
                command: |_| TaskCommand::Kill { done: None },
                task: test_task(),
                last_params: vec![],
                want: "cancel-only",
                want_params: vec![],
                want_reply: None,
            },
            Case {
                name: "a param-less restart reuses nothing and is accepted",
                command: |reply| TaskCommand::Restart { reply },
                task: test_task(),
                last_params: vec![],
                want: "cancel-then-run",
                want_params: vec![],
                want_reply: Some(true),
            },
            Case {
                name: "a param'd task with a previous run reuses its values",
                command: |reply| TaskCommand::Restart { reply },
                task: param_task(),
                last_params: vec![("env", "staging")],
                want: "cancel-then-run",
                want_params: vec![("env", "staging")],
                want_reply: Some(true),
            },
            Case {
                // The check that used to read the scheduler's copy of the
                // parameters. Nothing to reuse means nothing to restart.
                name: "a param'd task with no previous run is refused",
                command: |reply| TaskCommand::Restart { reply },
                task: param_task(),
                last_params: vec![],
                want: "nothing",
                want_params: vec![],
                want_reply: Some(false),
            },
        ];

        for case in cases {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            let command = (case.command)(Some(reply_tx));
            let startup = StartupConfig {
                task_cfg: Box::new(case.task),
                has_dependents: false,
            };
            let last_params: std::collections::HashMap<String, String> = case
                .last_params
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect();

            let ask = resolve_command(command, "seed", Some(&startup), &last_params, false);
            let (got, params) = match &ask {
                Ask::Run(request) => ("run", Some(request.params.clone())),
                Ask::Cancel { then: None, .. } => ("cancel-only", None),
                Ask::Cancel {
                    then: Some(request),
                    ..
                } => ("cancel-then-run", Some(request.params.clone())),
                Ask::Park(_) => ("park", None),
                Ask::Requery => ("requery", None),
                Ask::Nothing => ("nothing", None),
            };
            assert_eq!(got, case.want, "{}", case.name);
            if let Some(params) = params {
                let want: std::collections::HashMap<String, String> = case
                    .want_params
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect();
                assert_eq!(params, want, "{}: reused params", case.name);
            }
            match case.want_reply {
                Some(ok) => assert_eq!(reply_rx.await.unwrap().is_ok(), ok, "{}: reply", case.name),
                // Nothing answered it, so the sender dropped with the command.
                None => assert!(reply_rx.await.is_err(), "{}: unexpected reply", case.name),
            }
        }
    }

    fn outcome(
        name: &str,
        base_dir: &std::path::Path,
        report_tx: mpsc::UnboundedSender<super::super::ProcessReport>,
    ) -> TaskRunOutcome {
        TaskRunOutcome {
            name: name.to_string(),
            task_cfg: test_task(),
            base_dir: base_dir.to_path_buf(),
            global_watch_ignore: Vec::new(),
            pgid: 4242,
            report_tx,
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
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        std::mem::forget(shutdown_tx);
        let mut supervisors = spawn_supervisors(
            names.iter(),
            &ctx,
            &|_| None,
            &|_| None,
            &report_tx,
            &mut gates,
            &shutdown_rx,
            &{
                let (tx, rx) = mpsc::unbounded_channel();
                // Keep the receiver alive so sends succeed; nothing drains it.
                std::mem::forget(rx);
                tx
            },
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
            !handle.request(TaskCommand::Run(RunRequest {
                wait: None,
                task_cfg: Box::new(test_task()),
                params: std::collections::HashMap::new(),
                mode: super::super::task_worker::TaskRunMode::Triggered,
                intent: super::super::TaskRunIntent::Background,
                start_message: None,
            })),
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
            status: std::process::ExitStatus,
            want_success: bool,
            want_message: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "scheduled success",
                status: ExitStatusExt::from_raw(0),
                want_success: true,
                want_message: None,
            },
            Case {
                name: "scheduled failure carries the exit code",
                status: ExitStatusExt::from_raw(3 << 8),
                want_success: false,
                want_message: Some("exit code 3"),
            },
            Case {
                name: "rerun success",
                status: ExitStatusExt::from_raw(0),
                want_success: true,
                want_message: None,
            },
            Case {
                name: "rerun failure",
                status: ExitStatusExt::from_raw(1 << 8),
                want_success: false,
                want_message: Some("exit code 1"),
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let (report_tx, mut report_rx) = mpsc::unbounded_channel();

            outcome("build", temp.path(), report_tx)
                .finish(Ok(case.status), Duration::from_millis(5), None)
                .await;

            let Ok(super::super::ProcessReport::TaskExited(exit)) = report_rx.try_recv() else {
                panic!("{}: expected a TaskExited", case.name);
            };
            assert_eq!(exit.name, "build", "{}", case.name);
            assert_eq!(exit.success, case.want_success, "{}", case.name);
            assert_eq!(exit.message.as_deref(), case.want_message, "{}", case.name);
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
            outcome("build", temp.path(), report_tx)
                .finish(Ok(status), Duration::from_millis(1), None)
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
