//! Per-service start supervision.
//!
//! The mirror of [`super::task_supervisor`] for the other item kind, and it
//! exists for the same reason: preparing a start is slow (downloads, builds,
//! docker pulls, port allocation) so it has always been detached, and
//! detaching onto a shared completion channel is what forced the runner to
//! ask "is this still the current start?" when the answer landed —
//! `start_generation`, compared on arrival, plus a branch to stop whatever
//! the losing attempt had already brought up.
//!
//! One supervisor per service removes the question. It is the only thing that
//! reports a prepared start for its service, and only for the start it is
//! committed to.
//!
//! Note this owns *preparation only*. Wiring the process up, running the
//! ready check and sequencing shutdown all stay on the runner, because they
//! read cross-item state (dependency order) or are read back from many places
//! — see the task-side measurement in the plan for why moving them is a net
//! loss rather than the obvious next step.

use super::RunnerInternalCommand;
use super::service;
use super::service_worker::{ServiceStartMode, start_service_worker};
use super::{ServiceStartContext, ServiceStartIntent};
use crate::config::ShutdownConfig;
use crate::output::{ItemOutput, LifecycleEmitter};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// One request to start a service, as handed to its supervisor.
pub(in crate::runner) struct StartRequest {
    pub(in crate::runner) context: Box<ServiceStartContext>,
    pub(in crate::runner) mode: ServiceStartMode,
    pub(in crate::runner) intent: ServiceStartIntent,
}

/// Everything a service's supervisor can be asked to do.
pub(in crate::runner) enum ServiceCommand {
    /// Begin a start — or supersede the one being prepared.
    Start(StartRequest),
    /// Take custody of a wired process. Sent by the runner right after it
    /// wires output for a prepared start; from here to reap, the process
    /// handle lives in this supervisor. The round-trip (supervisor →
    /// prepared → runner wires → Adopt) is interim until wiring itself
    /// moves in; its ordering is safe because the runner is one task, so
    /// Adopt is always enqueued before any Stop for the same process.
    Adopt { handle: service::ServiceHandle },
    /// The process's output stream hit EOF — reap it and report the exit.
    /// `pgid` says which process, so a notice that outlives its process
    /// (EOF racing a restart) is ignored by custody, not by a counter.
    ProcessEof { pgid: i32 },
    /// End the held process: graceful signal per the config, bounded wait,
    /// SIGKILL fallback — the body of the old runner-side stop worker.
    Stop(StopRequest),
}

/// Parameters for [`ServiceCommand::Stop`].
pub(in crate::runner) struct StopRequest {
    pub(in crate::runner) config: ShutdownConfig,
    /// Skip the graceful signal entirely (force-shutdown path).
    pub(in crate::runner) force: bool,
    pub(in crate::runner) wait_full_exit: bool,
    /// When set, a mid-stop force request (second Ctrl+C) escalates the
    /// in-flight graceful wait — the manual-stop paths pass the runner's
    /// shutdown flag here.
    pub(in crate::runner) interrupt: Option<tokio::sync::watch::Receiver<bool>>,
    /// Where completion goes; see [`StopNotify`].
    pub(in crate::runner) notify: StopNotify,
}

/// How a stop's completion travels back.
pub(in crate::runner) enum StopNotify {
    /// The manual/restart path: `ServiceStopComplete{op_id}` through the
    /// internal channel, so the runner's control plumbing
    /// (`control_reply`, `stop_action`, `control_generation`) is untouched
    /// by custody.
    Internal { op_id: u64 },
    /// The shutdown path: a plain done-signal the reverse-dependency loop
    /// joins on, per depth. (The internal channel is not read during
    /// shutdown, so it cannot carry this.)
    Done(tokio::sync::oneshot::Sender<()>),
}

/// Everything a supervisor needs that doesn't vary per request.
#[derive(Clone)]
pub(in crate::runner) struct StartEnv {
    pub(in crate::runner) base_dir: PathBuf,
    pub(in crate::runner) pid_dir: PathBuf,
    pub(in crate::runner) platform: crate::config::Platform,
    pub(in crate::runner) docker_client: Option<bollard::Docker>,
    pub(in crate::runner) emitter: LifecycleEmitter,
    /// Global shutdown defaults, for stopping a start that lost a race.
    pub(in crate::runner) shutdown: ShutdownConfig,
}

/// Owner half for services.
pub(in crate::runner) type ServiceStarts = super::supervisor::Supervisors<ServiceCommand>;

/// Start one start-supervisor per service.
pub(in crate::runner) fn spawn_supervisors<'a>(
    names: impl Iterator<Item = &'a String>,
    env: &StartEnv,
    outputs: &dyn Fn(&str) -> Option<ItemOutput>,
    internal_tx: &mpsc::Sender<RunnerInternalCommand>,
    report_tx: &mpsc::UnboundedSender<super::ItemReport>,
) -> ServiceStarts {
    ServiceStarts::spawn_all(names, |name, rx, busy| {
        let output = outputs(&name);
        supervise(
            name,
            rx,
            env.clone(),
            output,
            internal_tx.clone(),
            report_tx.clone(),
            busy,
        )
    })
}

/// Stop a service brought up by a start that has since been superseded.
///
/// The losing attempt may already have a live process; nothing else knows
/// about it, so it has to be stopped here. Detached, because the caller is
/// the supervisor loop and the next start shouldn't wait on the old one's
/// shutdown grace period.
fn stop_superseded_start(
    env: &StartEnv,
    name: &str,
    context: &ServiceStartContext,
    start_result: service::StartResult,
) {
    let shutdown_config = context
        .resolved
        .shutdown
        .clone()
        .map(|shutdown| shutdown.merged_over(&env.shutdown))
        .unwrap_or_else(|| env.shutdown.clone());
    let debug = service::StopDebug::new(name.to_string(), env.emitter.clone());
    tokio::spawn(async move {
        let service::StartResult {
            handle,
            child_output,
        } = start_result;
        // Nothing will consume this, and holding it open keeps the child's
        // pipe alive.
        drop(child_output);
        let _ =
            service::stop_service(handle, Some(&shutdown_config), true, false, Some(debug)).await;
    });
}

/// Drive one service's starts, strictly in order.
///
/// Same rule as the task supervisor: a superseded start is **finished, not
/// aborted**. `start_service_worker` may already have a process up by the
/// time a newer request arrives, and dropping that future would strand it.
async fn supervise(
    name: String,
    mut rx: mpsc::UnboundedReceiver<ServiceCommand>,
    env: StartEnv,
    output: Option<ItemOutput>,
    internal_tx: mpsc::Sender<RunnerInternalCommand>,
    report_tx: mpsc::UnboundedSender<super::ItemReport>,
    busy: Arc<AtomicBool>,
) {
    let service_writer = output.map(|output| output.writer());
    let mut pending: Option<ServiceCommand> = None;
    let mut mailbox_closed = false;
    // The process this supervisor currently owns, from Adopt to reap/stop.
    let mut held: Option<service::ServiceHandle> = None;

    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => {
                busy.store(false, Ordering::Relaxed);
                match rx.recv().await {
                    Some(command) => {
                        busy.store(true, Ordering::Relaxed);
                        command
                    }
                    None => return,
                }
            }
        };
        let StartRequest {
            context,
            mode,
            intent,
        } = match command {
            ServiceCommand::Start(request) => request,
            ServiceCommand::Adopt { handle } => {
                // A stale held handle here would mean a process nobody ever
                // reaped; custody hands over strictly stop/reap-then-adopt,
                // so replace-and-drop is safe (drop does not kill).
                held = Some(handle);
                continue;
            }
            ServiceCommand::ProcessEof { pgid } => {
                reap_if_current(&name, &mut held, pgid, &report_tx).await;
                continue;
            }
            ServiceCommand::Stop(request) => {
                let result = match held.take() {
                    Some(handle) => {
                        let debug = service::StopDebug::new(name.clone(), env.emitter.clone());
                        match request.interrupt {
                            Some(shutdown_rx) => service::stop_service_interruptibly(
                                handle,
                                Some(&request.config),
                                request.wait_full_exit,
                                shutdown_rx,
                                Some(debug),
                            )
                            .await
                            .map_err(|e| e.to_string()),
                            None => service::stop_service(
                                handle,
                                Some(&request.config),
                                request.force,
                                request.wait_full_exit,
                                Some(debug),
                            )
                            .await
                            .map_err(|e| e.to_string()),
                        }
                    }
                    // Nothing held: the process already exited and was
                    // reaped. Stopping something stopped succeeds.
                    None => Ok(()),
                };
                match request.notify {
                    StopNotify::Internal { op_id } => {
                        let sent = internal_tx
                            .send(RunnerInternalCommand::ServiceStopComplete {
                                name: name.clone(),
                                op_id,
                                result,
                            })
                            .await;
                        if sent.is_err() {
                            return;
                        }
                    }
                    StopNotify::Done(done) => {
                        let _ = done.send(());
                    }
                }
                continue;
            }
        };

        // Clone the context the worker borrows so the original can move into
        // the completion message afterwards.
        let context_for_worker = context.clone();
        let worker = start_service_worker(
            &env.base_dir,
            &env.pid_dir,
            env.platform,
            env.docker_client.as_ref(),
            &env.emitter,
            &name,
            context_for_worker.as_ref(),
            mode,
            service_writer.as_ref(),
        );
        tokio::pin!(worker);

        let mut superseded: Option<ServiceCommand> = None;
        let result = loop {
            tokio::select! {
                result = &mut worker => break result,
                next = rx.recv(), if !mailbox_closed => match next {
                    Some(next) => superseded = Some(next),
                    // Guarded so a closed mailbox doesn't spin this select.
                    None => mailbox_closed = true,
                },
            }
        };

        match superseded {
            Some(next) => {
                if let Ok(start_result) = result {
                    stop_superseded_start(&env, &name, context.as_ref(), start_result);
                }
                pending = Some(next);
            }
            None => {
                let sent = internal_tx
                    .send(RunnerInternalCommand::ServiceStartPrepared {
                        name: name.clone(),
                        context,
                        intent,
                        result: result.map(Box::new),
                    })
                    .await;
                if sent.is_err() {
                    return;
                }
            }
        }
    }
}

/// Reap the held process if `pgid` names it, and report the exit.
///
/// A mismatched pgid means the EOF notice outlived its process — the run
/// it belonged to was already stopped or replaced — and is dropped. This
/// is the custody form of the old runner-side pgid currency check.
async fn reap_if_current(
    name: &str,
    held: &mut Option<service::ServiceHandle>,
    pgid: i32,
    report_tx: &mpsc::UnboundedSender<super::ItemReport>,
) {
    let matches = matches!(
        held.as_ref(),
        Some(service::ServiceHandle::Process(p)) if p.pgid() == pgid
    );
    if !matches {
        return;
    }
    let status = match held.take() {
        Some(service::ServiceHandle::Process(mut proc)) => {
            // The EOF notice only fires after the child's output closed, so
            // this wait returns promptly.
            proc.wait().await.ok()
        }
        _ => None,
    };
    let _ = report_tx.send(super::ItemReport::ServiceExited {
        name: name.to_string(),
        pgid,
        status,
    });
}
