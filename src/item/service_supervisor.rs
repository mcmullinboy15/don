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
//! The supervisor owns its service's process from wire to reap — prepare,
//! spawn, output reader, OSC sink, crash detection, stop-with-drain — and
//! its proxy, whose listeners span process generations. The runner keeps
//! what is genuinely cross-item: scheduling, state folds, ready resolution
//! and completion (which cross channels — see the plan's ordering note),
//! restart policy, and shutdown sequencing. Proxy *decisions* likewise stay
//! with the runner and arrive as [`ProxyDirective`]s.

use super::ServiceStartIntent;
use super::service_process as service;
use super::service_worker::ServiceStartContext;
use super::service_worker::{ServiceStartMode, start_service_worker};
use crate::config::ShutdownConfig;
use crate::output::{ItemOutput, LifecycleEmitter};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// One request to start a service, as handed to its supervisor.
pub(crate) struct StartRequest {
    pub(crate) context: Box<ServiceStartContext>,
    pub(crate) mode: ServiceStartMode,
    pub(crate) intent: ServiceStartIntent,
    /// Allocate new ephemeral backend ports before spawning. Set on the
    /// restart path so the new process binds a fresh port while draining
    /// connections to the old one finish undisturbed.
    pub(crate) fresh_backend_ports: bool,
}

/// Everything a service's supervisor can be asked to do.
pub(crate) enum ServiceCommand {
    /// Begin a start — or supersede the one being prepared.
    Start(StartRequest),
    /// End the held process: graceful signal per the config, bounded wait,
    /// SIGKILL fallback — the body of the old runner-side stop worker.
    /// The reply waits for the output reader to drain, so "stopped" can
    /// never outrun the process's last lines.
    Stop(StopRequest),
    /// Adjust the owned proxy. Applied immediately, even while a start is
    /// being prepared — a proxy directive is never a supersession.
    Proxy(ProxyDirective),
}

/// What the runner may ask a supervisor to do with its proxy.
///
/// The runner stays the *decider* — connection policy is derived from
/// lifecycle state it folds, and backend activation on ready is gated by a
/// ready outcome it resolves — but the proxy itself lives here, so decisions
/// arrive as directives. Mailbox FIFO gives the only ordering that matters:
/// a `ClearBackend` sent before a `Stop` is applied before the stop runs.
pub(crate) enum ProxyDirective {
    /// Set the connection policy (serve / lazy-trigger / refuse).
    SetPolicy(crate::proxy::ConnectionPolicy),
    /// Point forwarding backends at their configured addresses.
    SetBackend,
    /// Clear forwarding backends; new connections queue until set again.
    ClearBackend,
    /// Stop listening entirely — teardown is beginning.
    Shutdown,
}

/// A service's bound proxy and its lazy-demand channel, handed to the
/// supervisor at spawn. Bound by the runner during construction so port
/// conflicts still fail startup before anything spawns.
pub(crate) struct ProxyAssets {
    pub(crate) proxy: crate::proxy::ServiceProxy,
    /// The receiving half of the proxy's lazy trigger channel — `Some` only
    /// for lazy services. The supervisor forwards each trigger as
    /// [`super::ItemReport::Demand`].
    pub(crate) demand_rx: Option<mpsc::Receiver<String>>,
}

/// What the runner receives for a spawned, wired start.
///
/// The supervisor keeps the process handle and the output reader; this is
/// everything the runner's bookkeeping and ready-check paths need —
/// extracted once, at wire time, by the owner.
pub(crate) struct ServiceWired {
    pub(crate) identity: super::state::ServiceHandleIdentity,
    pub(crate) pgid: Option<i32>,
    pub(crate) docker_port_bindings: Vec<crate::docker::DockerPortBinding>,
    /// OSC response scanner handle — dropped on restart/stop to end the
    /// scanner and release its gate sender.
    pub(crate) osc_sink: Option<crate::output::OscSinkHandle>,
    /// Sender into this spawn's PTY input gate. `None` for docker and
    /// pipe-mode spawns. Attach bridges clone it; the runner's copy is
    /// cleared on exit so the gate (and the PTY write half) can end.
    pub(crate) pty_input: Option<tokio::sync::mpsc::Sender<crate::output::PtyInput>>,
    /// The proxy's env-mode backend vars this spawn was launched with —
    /// `Some` iff the service has a proxy. The runner refreshes its
    /// `ProxyView` shadow from this, so ready checks written against
    /// `${PORT}` resolve to the port the new process was actually told to
    /// bind. Wiring precedes ready resolution, so the shadow is always
    /// current where it is read.
    pub(crate) proxy_backend_env: Option<std::collections::HashMap<String, String>>,
}

/// Parameters for [`ServiceCommand::Stop`].
pub(crate) struct StopRequest {
    pub(crate) config: ShutdownConfig,
    /// Skip the graceful signal entirely (force-shutdown path).
    pub(crate) force: bool,
    pub(crate) wait_full_exit: bool,
    /// When set, a mid-stop force request (second Ctrl+C) escalates the
    /// in-flight graceful wait — the manual-stop paths pass the runner's
    /// shutdown flag here.
    pub(crate) interrupt: Option<tokio::sync::watch::Receiver<bool>>,
    /// Where completion goes; see [`StopNotify`].
    pub(crate) notify: StopNotify,
}

/// How a stop's completion travels back.
pub(crate) enum StopNotify {
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
pub(crate) struct StartEnv {
    pub(crate) base_dir: PathBuf,
    pub(crate) pid_dir: PathBuf,
    pub(crate) platform: crate::config::Platform,
    pub(crate) docker_client: Option<bollard::Docker>,
    pub(crate) emitter: LifecycleEmitter,
    /// Global shutdown defaults, for stopping a start that lost a race.
    pub(crate) shutdown: ShutdownConfig,
}

/// Owner half for services.
pub(crate) type ServiceStarts = super::registry::Supervisors<ServiceCommand>;

/// Start one start-supervisor per service, each taking ownership of its
/// bound proxy (if any) from `proxies`.
pub(crate) fn spawn_supervisors<'a>(
    names: impl Iterator<Item = &'a String>,
    env: &StartEnv,
    outputs: &dyn Fn(&str) -> Option<ItemOutput>,
    report_tx: &mpsc::UnboundedSender<super::ItemReport>,
    proxies: &mut std::collections::HashMap<String, ProxyAssets>,
) -> ServiceStarts {
    ServiceStarts::spawn_all(names, |name, rx, busy| {
        let output = outputs(&name);
        let assets = proxies.remove(&name);
        supervise(
            name,
            rx,
            env.clone(),
            output,
            report_tx.clone(),
            busy,
            assets,
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
#[allow(clippy::too_many_arguments)]
async fn supervise(
    name: String,
    mut rx: mpsc::UnboundedReceiver<ServiceCommand>,
    env: StartEnv,
    output: Option<ItemOutput>,
    report_tx: mpsc::UnboundedSender<super::ItemReport>,
    busy: Arc<AtomicBool>,
    proxy_assets: Option<ProxyAssets>,
) {
    let service_writer = output.as_ref().map(|output| output.writer());
    let mut pending: Option<ServiceCommand> = None;
    let mut mailbox_closed = false;
    // The proxy outlives individual starts — its listeners span process
    // generations, which is what makes zero-downtime restart possible.
    let (mut proxy, mut demand_rx) = match proxy_assets {
        Some(assets) => (Some(assets.proxy), assets.demand_rx),
        None => (None, None),
    };
    // The process this supervisor currently owns, from wire to reap/stop.
    let mut held: Option<service::ServiceHandle> = None;
    // The output reader for the held process, and its end-of-stream signal.
    // Stop drains the reader before notifying; end-of-stream while idle is
    // the crash path (reap + report).
    let mut reader: Option<tokio::task::JoinHandle<()>> = None;
    let mut reader_eof: Option<tokio::sync::oneshot::Receiver<()>> = None;
    // Dropping this ends the held process's health monitor, if one ran.
    let mut monitor_cancel: Option<tokio::sync::oneshot::Sender<()>> = None;
    // The in-flight ready racer's outcome, forwarded by THIS loop onto the
    // report channel so it always trails its own prepared report (single
    // producer, one channel). Cleared on Start/Stop so a superseded run's
    // outcome can never be forwarded after a newer prepared.
    let mut ready_pending: Option<tokio::sync::oneshot::Receiver<ReadyOutcome>> = None;

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
                    // The ready racer settled — forward through this loop
                    // so the outcome trails its own prepared report.
                    outcome = wait_ready(&mut ready_pending), if ready_pending.is_some() => {
                        ready_pending = None;
                        if let Some(outcome) = outcome
                            && report_tx
                                .send(super::ItemReport::ServiceReady {
                                    name: name.clone(),
                                    success: outcome.success,
                                    message: outcome.message,
                                    had_check: outcome.had_check,
                                })
                                .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    // The lazy proxy saw a connection: demand. The runner
                    // gates on service state, so duplicates are harmless.
                    demand = wait_demand(&mut demand_rx), if demand_rx.is_some() => {
                        match demand {
                            Some(_) => {
                                let _ = report_tx.send(super::ItemReport::Demand {
                                    name: name.clone(),
                                });
                            }
                            // Every trigger sender is gone (proxy shut down);
                            // stop selecting on a closed channel.
                            None => demand_rx = None,
                        }
                        continue;
                    }
                }
            },
        };
        let StartRequest {
            mut context,
            mode,
            intent,
            fresh_backend_ports,
        } = match command {
            ServiceCommand::Start(request) => request,
            ServiceCommand::Proxy(directive) => {
                apply_proxy_directive(&mut proxy, directive);
                continue;
            }
            ServiceCommand::Stop(request) => {
                reader_eof = None;
                monitor_cancel = None;
                ready_pending = None;
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
                        if report_tx
                            .send(super::ItemReport::ServiceStopComplete {
                                name: name.clone(),
                                op_id,
                                result,
                            })
                            .is_err()
                        {
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

        // A new start supersedes the previous run's in-flight ready
        // outcome; forwarding it after this run's prepared report would be
        // exactly the stale-completion race this loop exists to prevent.
        ready_pending = None;

        // The proxy's per-spawn contribution: fresh ephemeral backend ports
        // on restart, the backend/public env vars, and the listenfd sockets
        // the child inherits.
        if let Some(p) = proxy.as_mut() {
            if fresh_backend_ports && let Err(error) = p.reallocate_ephemeral_ports().await {
                if report_tx
                    .send(super::ItemReport::ServiceStartPrepared {
                        name: name.clone(),
                        context,
                        intent,
                        result: Err(format!("failed to allocate ephemeral ports: {error}")),
                    })
                    .is_err()
                {
                    return;
                }
                continue;
            }
            context.resolved.env.extend(p.env_vars());
            context.resolved.env.extend(p.public_env_vars());
            context.listen_fds = p.listenfd_raw_fds();
            context.listen_fds_env = p.listenfd_env();
        }

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
                    // Proxy directives apply immediately, not as a
                    // supersession. A directive queued *behind* a parked
                    // Start/Stop does run ahead of it, which is safe: the
                    // one order-sensitive pair the runner sends is
                    // ClearBackend-then-Stop, and that order is preserved
                    // because the directive comes first in the mailbox.
                    Some(ServiceCommand::Proxy(directive)) => {
                        apply_proxy_directive(&mut proxy, directive);
                    }
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
                let (wired, ready_parts) = match result {
                    Ok(start_result) => {
                        let (wired, exit_rx, cancel_rx) = wire_spawn(
                            output.as_ref(),
                            service_writer.as_ref(),
                            start_result,
                            proxy.as_ref(),
                            &mut held,
                            &mut reader,
                            &mut reader_eof,
                            &mut monitor_cancel,
                        )
                        .await;
                        // Resolution reads this spawn's live proxy and docker
                        // state — authoritative only after the spawn, which is
                        // why it happens here and not at queue time.
                        let ready = resolve_supervisor_ready(
                            &context.resolved,
                            proxy.as_ref(),
                            &wired.docker_port_bindings,
                        );
                        (Ok(Box::new(wired)), Some((ready, exit_rx, cancel_rx)))
                    }
                    Err(message) => (Err(message), None),
                };
                if report_tx
                    .send(super::ItemReport::ServiceStartPrepared {
                        name: name.clone(),
                        context,
                        intent,
                        result: wired,
                    })
                    .is_err()
                {
                    return;
                }
                // Start the ready check — or, with none configured, report
                // ready now. Both flow after the prepared report above on
                // the same channel from this same loop, so the fold always
                // sees prepared first.
                match ready_parts {
                    Some((Some(ready), exit_rx, cancel_rx)) => {
                        ready_pending = Some(spawn_ready_racer(
                            &name, ready, exit_rx, cancel_rx, &report_tx,
                        ));
                    }
                    Some((None, _exit_rx, _cancel_rx))
                        if report_tx
                            .send(super::ItemReport::ServiceReady {
                                name: name.clone(),
                                success: true,
                                message: None,
                                had_check: false,
                            })
                            .is_err() =>
                    {
                        return;
                    }
                    Some((None, ..)) | None => {}
                }
            }
        }
    }
}

/// Apply a runner proxy decision to the owned proxy. No-op for services
/// without one, and after `Shutdown`.
fn apply_proxy_directive(
    proxy: &mut Option<crate::proxy::ServiceProxy>,
    directive: ProxyDirective,
) {
    match directive {
        ProxyDirective::SetPolicy(policy) => {
            if let Some(p) = proxy.as_mut() {
                p.set_policy(policy);
            }
        }
        ProxyDirective::SetBackend => {
            if let Some(p) = proxy.as_ref() {
                p.set_backend();
            }
        }
        ProxyDirective::ClearBackend => {
            if let Some(p) = proxy.as_ref() {
                p.clear_backend();
            }
        }
        ProxyDirective::Shutdown => {
            if let Some(p) = proxy.take() {
                p.shutdown();
            }
        }
    }
}

/// Await a lazy trigger without consuming the select slot on `None`.
async fn wait_demand(demand_rx: &mut Option<mpsc::Receiver<String>>) -> Option<String> {
    match demand_rx.as_mut() {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
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
/// bookkeeping needs, start the output reader, activate the proxy backend,
/// and hold the handle.
#[allow(clippy::too_many_arguments)]
async fn wire_spawn(
    output: Option<&ItemOutput>,
    service_writer: Option<&crate::output::ServiceWriter>,
    start_result: service::StartResult,
    proxy: Option<&crate::proxy::ServiceProxy>,
    held: &mut Option<service::ServiceHandle>,
    reader: &mut Option<tokio::task::JoinHandle<()>>,
    reader_eof: &mut Option<tokio::sync::oneshot::Receiver<()>>,
    monitor_cancel: &mut Option<tokio::sync::oneshot::Sender<()>>,
) -> (
    ServiceWired,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
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
    let (osc_sink, pty_input) = match (pty, output) {
        (Some(pty), Some(output)) => {
            // Feed the server-side screen from process start — a correct
            // repaint on attach requires having seen the setup sequences.
            // Matches the PTY's initial 80x24 size.
            output.register_emulator(80, 24).await;
            // The gate owns the write half for this spawn's lifetime;
            // the scanner and any attach bridges hold senders into it.
            let pty_input = crate::output::spawn_pty_gate(pty);
            let osc_sink = output.add_osc_sink(pty_input.clone()).await;
            (Some(osc_sink), Some(pty_input))
        }
        _ => (None, None),
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

    // Activate forwarding immediately — the proxy's connect loop retries
    // with backoff, so a service that hasn't bound its port yet just makes
    // early connections wait, exactly as when the runner did this on wiring.
    if let Some(p) = proxy {
        p.set_backend();
    }

    (
        ServiceWired {
            identity,
            pgid,
            docker_port_bindings,
            osc_sink,
            pty_input,
            proxy_backend_env: proxy.map(|p| p.env_vars()),
        },
        exit_rx,
        cancel_rx,
    )
}

/// What the ready racer settles into, forwarded by the supervisor loop.
struct ReadyOutcome {
    success: bool,
    message: Option<String>,
    had_check: bool,
}

/// Await the racer's outcome without consuming the slot on `None`.
async fn wait_ready(
    ready_pending: &mut Option<tokio::sync::oneshot::Receiver<ReadyOutcome>>,
) -> Option<ReadyOutcome> {
    match ready_pending.as_mut() {
        Some(rx) => rx.await.ok(),
        None => std::future::pending().await,
    }
}

/// Resolve this spawn's ready check against its live proxy and docker
/// state — the same algorithm the runner's status path runs over shadows.
fn resolve_supervisor_ready(
    resolved: &crate::config::ResolvedService,
    proxy: Option<&crate::proxy::ServiceProxy>,
    docker_bindings: &[crate::docker::DockerPortBinding],
) -> Option<crate::config::ReadyCheck> {
    let backend_env = proxy.map(|p| p.env_vars()).unwrap_or_default();
    let mut public_env = crate::docker::public_env_vars(docker_bindings);
    if let Some(p) = proxy {
        public_env.extend(p.public_env_vars());
    }
    let replacements = super::ready::port_replacements_for(
        proxy.map(|p| p.bindings()).unwrap_or(&[]),
        docker_bindings,
    );
    super::ready::resolve_ready_check(
        resolved.ready.as_ref(),
        &resolved.env,
        &backend_env,
        &public_env,
        &replacements,
    )
}

/// Run the ready check racing this spawn's end-of-stream, then start the
/// health monitor on success (its cancel sender lives with the supervisor,
/// so monitor lifetime stays tied to custody). The outcome goes back to the
/// supervisor loop, which forwards it on the report channel.
fn spawn_ready_racer(
    name: &str,
    ready: crate::config::ReadyCheck,
    exit_rx: tokio::sync::oneshot::Receiver<()>,
    cancel_rx: tokio::sync::oneshot::Receiver<()>,
    report_tx: &mpsc::UnboundedSender<super::ItemReport>,
) -> tokio::sync::oneshot::Receiver<ReadyOutcome> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let monitor_cancel_rx = ready.monitor.then_some(cancel_rx);
    let monitor_report_tx = report_tx.clone();
    let name = name.to_string();
    tokio::spawn(async move {
        let result = tokio::select! {
            result = service::run_ready_check(&ready) => result,
            _ = exit_rx => Err(service::ServiceError::ProcessExitedDuringReadyCheck),
        };
        let success = result.is_ok();
        if success && let Some(cancel_rx) = monitor_cancel_rx {
            let monitor_name = name.clone();
            tokio::spawn(async move {
                super::health::run_health_monitor(
                    monitor_name,
                    ready,
                    monitor_report_tx,
                    cancel_rx,
                )
                .await;
            });
        }
        let _ = ready_tx.send(ReadyOutcome {
            success,
            message: result.err().map(|e| e.to_string()),
            had_check: true,
        });
    });
    ready_rx
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::{LogConfig, Platform, ProxyEntry, ProxyMode};
    use crate::output::OutputManager;
    use crate::proxy::{ConnectionPolicy, ServiceProxy};
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    async fn test_env() -> StartEnv {
        let log_config = LogConfig::Stdout;
        let services = [("svc", &log_config)];
        let output_manager = OutputManager::new(&services, tokio::io::sink())
            .await
            .unwrap();
        StartEnv {
            base_dir: std::env::temp_dir(),
            pid_dir: std::env::temp_dir(),
            platform: Platform::LinuxX86_64,
            docker_client: None,
            emitter: output_manager.clone_lifecycle_emitter(),
            shutdown: ShutdownConfig::default(),
        }
    }

    struct Harness {
        tx: mpsc::UnboundedSender<ServiceCommand>,
        report_rx: mpsc::UnboundedReceiver<super::super::ItemReport>,
        handle: tokio::task::JoinHandle<()>,
    }

    async fn spawn_harness(assets: Option<ProxyAssets>) -> Harness {
        let (tx, rx) = mpsc::unbounded_channel();
        let (report_tx, report_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(supervise(
            "svc".to_string(),
            rx,
            test_env().await,
            None,
            report_tx,
            Arc::new(AtomicBool::new(false)),
            assets,
        ));
        Harness {
            tx,
            report_rx,
            handle,
        }
    }

    async fn bind_env_proxy(lazy_tx: Option<mpsc::Sender<String>>) -> ServiceProxy {
        let entries = vec![ProxyEntry {
            listen: "127.0.0.1:0".to_string(),
            mode: ProxyMode::Env("PORT".to_string()),
        }];
        let log_config = LogConfig::Stdout;
        let services = [("svc", &log_config)];
        let output_manager = OutputManager::new(&services, tokio::io::sink())
            .await
            .unwrap();
        ServiceProxy::bind(
            &entries,
            false,
            lazy_tx,
            "svc",
            output_manager.clone_lifecycle_emitter(),
        )
        .await
        .unwrap()
    }

    /// The supervisor applies each proxy directive to the proxy it owns —
    /// observable from the outside as connection behavior on the public
    /// address.
    #[tokio::test]
    async fn proxy_directives_drive_the_owned_listener() {
        enum Expect {
            /// The connection is closed cleanly (refusal).
            Refused,
            /// Bytes flow through to a live backend.
            Forwarded,
            /// The listener itself is gone.
            ConnectFails,
        }
        struct Case {
            name: &'static str,
            directives: Vec<ProxyDirective>,
            expect: Expect,
        }
        let cases = vec![
            Case {
                name: "refuse policy closes connections",
                directives: vec![ProxyDirective::SetPolicy(ConnectionPolicy::Refuse)],
                expect: Expect::Refused,
            },
            Case {
                name: "set backend forwards to the service",
                directives: vec![ProxyDirective::SetBackend],
                expect: Expect::Forwarded,
            },
            Case {
                name: "clear after set parks, refuse then closes",
                directives: vec![
                    ProxyDirective::SetBackend,
                    ProxyDirective::ClearBackend,
                    ProxyDirective::SetPolicy(ConnectionPolicy::Refuse),
                ],
                expect: Expect::Refused,
            },
            Case {
                name: "shutdown drops the listener",
                directives: vec![ProxyDirective::Shutdown],
                expect: Expect::ConnectFails,
            },
        ];

        for case in cases {
            // The proxy's ephemeral backend port is allocated bind-and-drop,
            // so any other process (or parallel test) can steal it before the
            // stand-in backend below rebinds it. A steal is a setup failure,
            // not a regression — retry with a fresh proxy.
            const SETUP_ATTEMPTS: usize = 10;
            let mut attempt = 0;
            let (proxy, backend) = loop {
                attempt += 1;
                let proxy = bind_env_proxy(None).await;
                let backend_port: u16 = proxy
                    .view()
                    .backend_env
                    .get("PORT")
                    .unwrap()
                    .parse()
                    .unwrap();
                match tokio::net::TcpListener::bind(("127.0.0.1", backend_port)).await {
                    Ok(listener) => break (proxy, listener),
                    Err(error) => assert!(
                        attempt < SETUP_ATTEMPTS,
                        "{}: could not claim backend port: {error}",
                        case.name
                    ),
                }
            };
            let view = proxy.view();
            let public_addr = view.bindings[0].bound_addr;

            // A stand-in service on the ephemeral backend port that writes
            // one byte to every connection.
            let backend_task = tokio::spawn(async move {
                while let Ok((mut conn, _)) = backend.accept().await {
                    use tokio::io::AsyncWriteExt;
                    let _ = conn.write_all(b"x").await;
                }
            });

            let harness = spawn_harness(Some(ProxyAssets {
                proxy,
                demand_rx: None,
            }))
            .await;
            for directive in case.directives {
                harness.tx.send(ServiceCommand::Proxy(directive)).unwrap();
            }

            // Directive application is asynchronous, so a connection can be
            // served under an *intermediate* state of a multi-directive
            // sequence (e.g. accepted after SetBackend but before the
            // ClearBackend behind it). Poll fresh connections until the
            // final state is observed; only never settling is a failure.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{}: directives never took effect",
                    case.name
                );
                match case.expect {
                    Expect::Refused | Expect::Forwarded => {
                        let mut conn = match tokio::net::TcpStream::connect(public_addr).await {
                            Ok(conn) => conn,
                            Err(_) => panic!("{}: listener vanished", case.name),
                        };
                        let mut buf = [0u8; 1];
                        let read =
                            tokio::time::timeout(Duration::from_millis(500), conn.read(&mut buf))
                                .await;
                        match (&case.expect, read) {
                            (Expect::Refused, Ok(Ok(0))) => break,
                            (Expect::Forwarded, Ok(Ok(read))) if &buf[..read] == b"x" => break,
                            // Parked, served under a stale intermediate
                            // state, or errored — not settled yet.
                            _ => {}
                        }
                    }
                    Expect::ConnectFails => {
                        if tokio::net::TcpStream::connect(public_addr).await.is_err() {
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            backend_task.abort();
            harness.handle.abort();
        }
    }

    /// A lazy proxy's trigger reaches the runner as a demand report, and a
    /// closed trigger channel leaves the supervisor alive.
    #[tokio::test]
    async fn lazy_trigger_forwards_as_demand_report() {
        let (lazy_tx, demand_rx) = mpsc::channel(16);
        let proxy = bind_env_proxy(Some(lazy_tx.clone())).await;
        let mut harness = spawn_harness(Some(ProxyAssets {
            proxy,
            demand_rx: Some(demand_rx),
        }))
        .await;

        lazy_tx.send("svc".to_string()).await.unwrap();
        let report = tokio::time::timeout(Duration::from_secs(5), harness.report_rx.recv())
            .await
            .expect("demand should be forwarded")
            .expect("report channel open");
        match report {
            super::super::ItemReport::Demand { name } => assert_eq!(name, "svc"),
            _ => panic!("expected a demand report"),
        }

        // Dropping every trigger sender must not end the supervisor: it
        // still owns the proxy and must keep answering directives. The
        // proxy holds a sender clone, so shut it down first.
        harness
            .tx
            .send(ServiceCommand::Proxy(ProxyDirective::Shutdown))
            .unwrap();
        drop(lazy_tx);
        harness
            .tx
            .send(ServiceCommand::Proxy(ProxyDirective::SetBackend))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !harness.handle.is_finished(),
            "supervisor must survive its demand channel closing"
        );
        harness.handle.abort();
    }
}
