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
    /// End the held process: graceful signal per the config, bounded wait,
    /// SIGKILL fallback — the body of the old runner-side stop worker.
    /// The reply waits for the output reader to drain, so "stopped" can
    /// never outrun the process's last lines.
    Stop(StopRequest),
}

/// What the runner receives for a spawned, wired start.
///
/// The supervisor keeps the process handle and the output reader; this is
/// everything the runner's bookkeeping and ready-check paths need —
/// extracted once, at wire time, by the owner.
pub(in crate::runner) struct ServiceWired {
    pub(in crate::runner) identity: super::state::ServiceHandleIdentity,
    pub(in crate::runner) pgid: Option<i32>,
    pub(in crate::runner) docker_port_bindings: Vec<crate::docker::DockerPortBinding>,
    /// OSC response sink handle, for the attach paths' take/restore dance.
    pub(in crate::runner) osc_sink: Option<crate::output::OscSinkHandle>,
    /// Resolves when the output reader ends (process died) — the ready
    /// check races against it. Errs immediately when the service has no
    /// registered output, which matches the old wiring's behavior.
    pub(in crate::runner) ready_exit_rx: tokio::sync::oneshot::Receiver<()>,
    /// Cancellation for the health monitor this spawn may start. The
    /// supervisor holds the sender and drops it on Stop or process death,
    /// so monitor lifetime is tied to custody rather than to per-site
    /// bookkeeping discipline.
    pub(in crate::runner) monitor_cancel_rx: tokio::sync::oneshot::Receiver<()>,
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
    let service_writer = output.as_ref().map(|output| output.writer());
    let mut pending: Option<ServiceCommand> = None;
    let mut mailbox_closed = false;
    // The process this supervisor currently owns, from wire to reap/stop.
    let mut held: Option<service::ServiceHandle> = None;
    // The output reader for the held process, and its end-of-stream signal.
    // Stop drains the reader before notifying; end-of-stream while idle is
    // the crash path (reap + report).
    let mut reader: Option<tokio::task::JoinHandle<()>> = None;
    let mut reader_eof: Option<tokio::sync::oneshot::Receiver<()>> = None;
    // Dropping this ends the held process's health monitor, if one ran.
    let mut monitor_cancel: Option<tokio::sync::oneshot::Sender<()>> = None;

    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => loop {
                busy.store(false, Ordering::Relaxed);
                tokio::select! {
                    received = rx.recv() => match received {
                        Some(command) => {
                            busy.store(true, Ordering::Relaxed);
                            break command;
                        }
                        None => return,
                    },
                    // The held process's output ended — it died. Reap and
                    // report; this is the crash path, and watching our own
                    // reader is what replaced the detached crash watcher.
                    _ = wait_eof(&mut reader_eof), if reader_eof.is_some() => {
                        busy.store(true, Ordering::Relaxed);
                        reader_eof = None;
                        monitor_cancel = None;
                        if let Some(handle) = reader.take() {
                            await_reader(handle).await;
                        }
                        reap_and_report(&name, &mut held, &report_tx).await;
                        continue;
                    }
                }
            },
        };
        let StartRequest {
            context,
            mode,
            intent,
        } = match command {
            ServiceCommand::Start(request) => request,
            ServiceCommand::Stop(request) => {
                reader_eof = None;
                monitor_cancel = None;
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
                // The process is gone; its reader sees EOF and drains. Wait
                // for that before notifying, so "stopped" never outruns the
                // service's final output.
                if let Some(handle) = reader.take() {
                    await_reader(handle).await;
                }
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
                // Wire the spawn here, as its owner: extract what the runner
                // needs, keep the handle and the reader. A failed prepare
                // passes through untouched.
                let wired = match result {
                    Ok(start_result) => Ok(Box::new(
                        wire_spawn(
                            output.as_ref(),
                            service_writer.as_ref(),
                            start_result,
                            &mut held,
                            &mut reader,
                            &mut reader_eof,
                            &mut monitor_cancel,
                        )
                        .await,
                    )),
                    Err(message) => Err(message),
                };
                let sent = internal_tx
                    .send(RunnerInternalCommand::ServiceStartPrepared {
                        name: name.clone(),
                        context,
                        intent,
                        result: wired,
                    })
                    .await;
                if sent.is_err() {
                    return;
                }
            }
        }
    }
}

/// Await the end-of-stream signal without consuming the slot on `None`.
async fn wait_eof(reader_eof: &mut Option<tokio::sync::oneshot::Receiver<()>>) {
    match reader_eof.as_mut() {
        // Err means the reader dropped the sender without sending — same
        // meaning: the stream is over.
        Some(rx) => {
            let _ = rx.await;
        }
        None => std::future::pending().await,
    }
}

/// Join the finished reader, bounded — a wedged sink must not hold the
/// supervisor (and with it shutdown) hostage.
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

/// Take ownership of a fresh spawn: extract everything the runner's
/// bookkeeping needs, start the output reader, and hold the handle.
async fn wire_spawn(
    output: Option<&ItemOutput>,
    service_writer: Option<&crate::output::ServiceWriter>,
    start_result: service::StartResult,
    held: &mut Option<service::ServiceHandle>,
    reader: &mut Option<tokio::task::JoinHandle<()>>,
    reader_eof: &mut Option<tokio::sync::oneshot::Receiver<()>>,
    monitor_cancel: &mut Option<tokio::sync::oneshot::Sender<()>>,
) -> ServiceWired {
    let service::StartResult {
        mut handle,
        child_output,
    } = start_result;

    let (identity, pgid) = match &handle {
        service::ServiceHandle::Process(proc) => (
            super::state::ServiceHandleIdentity::Process { pgid: proc.pgid() },
            Some(proc.pgid()),
        ),
        service::ServiceHandle::Docker(_) => (super::state::ServiceHandleIdentity::Docker, None),
    };
    let docker_port_bindings = match &handle {
        service::ServiceHandle::Docker(docker) => docker.port_bindings().to_vec(),
        service::ServiceHandle::Process(_) => Vec::new(),
    };
    let pty = match &mut handle {
        service::ServiceHandle::Process(process) => process.take_pty_write(),
        service::ServiceHandle::Docker(_) => None,
    };
    let osc_sink = match (pty, output) {
        (Some(pty), Some(output)) => Some(output.add_osc_sink(pty).await),
        _ => None,
    };

    // Fan the reader's end out twice: once to the ready check (races its
    // retry loop), once to this supervisor (the crash path). If there is no
    // registered output, both fire immediately — which is what the old
    // wiring did too.
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
    let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
    *reader = service_writer.map(|writer| {
        let writer = writer.clone();
        tokio::spawn(async move {
            let _ = writer.process_stream(child_output).await;
            let _ = exit_tx.send(());
            let _ = eof_tx.send(());
        })
    });
    *reader_eof = Some(eof_rx);
    *held = Some(handle);
    *monitor_cancel = Some(cancel_tx);

    ServiceWired {
        identity,
        pgid,
        docker_port_bindings,
        osc_sink,
        ready_exit_rx: exit_rx,
        monitor_cancel_rx: cancel_rx,
    }
}

/// Reap the held process after its output ended, and report the exit.
///
/// Docker containers are held but not reaped here — the bollard stream's
/// EOF semantics aren't a death certificate, matching the old crash
/// watcher's docker exclusion.
async fn reap_and_report(
    name: &str,
    held: &mut Option<service::ServiceHandle>,
    report_tx: &mpsc::UnboundedSender<super::ItemReport>,
) {
    if !matches!(held.as_ref(), Some(service::ServiceHandle::Process(_))) {
        return;
    }
    let Some(service::ServiceHandle::Process(mut proc)) = held.take() else {
        return;
    };
    let pgid = proc.pgid();
    // The reader already hit end-of-stream, so this wait returns promptly.
    let status = proc.wait().await.ok();
    let _ = report_tx.send(super::ItemReport::ServiceExited {
        name: name.to_string(),
        pgid,
        status,
    });
}
