//! Per-service start supervision.
//!
//! The mirror of [`super::task_supervisor`] for the other process kind, and it
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
//! what is genuinely cross-process: scheduling, state folds, ready resolution
//! and completion (which cross channels — see the plan's ordering note),
//! restart policy, and shutdown sequencing. Proxy *decisions* likewise stay
//! with the runner and arrive as [`ProxyDirective`]s.

use super::ServiceStartIntent;
use super::service;
use super::service_worker::ServiceStartContext;
use super::service_worker::{ServiceStartMode, start_service_worker};
use crate::config::ShutdownConfig;
use crate::output::{LifecycleEmitter, ProcessOutput};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// One request to start a service, as handed to its supervisor.
pub(crate) struct StartRequest {
    pub(crate) mode: ServiceStartMode,
    pub(crate) intent: ServiceStartIntent,
    /// Allocate new ephemeral backend ports before spawning. Set on the
    /// restart path so the new process binds a fresh port while draining
    /// connections to the old one finish undisturbed.
    pub(crate) fresh_backend_ports: bool,
}

/// One request to stop and immediately start again, as one operation.
///
/// Restart is a single command rather than a stop the scheduler follows up
/// on, because every step of it belongs to the owner: the proxy whose backend
/// must be cleared first, the process being ended, and the spawn that
/// replaces it. As one mailbox item it also cannot interleave with anything
/// else this service was asked to do.
pub(crate) struct RestartRequest {
    pub(crate) config: ShutdownConfig,
    pub(crate) wait_full_exit: bool,
    /// See [`StopRequest::interrupt`].
    pub(crate) interrupt: Option<tokio::sync::watch::Receiver<bool>>,
    /// Clear forwarding backends before stopping, so connections arriving
    /// mid-restart queue instead of racing the dying process.
    pub(crate) clear_backend_first: bool,
    pub(crate) start_mode: ServiceStartMode,
    /// See [`StartRequest::fresh_backend_ports`].
    pub(crate) fresh_backend_ports: bool,
    pub(crate) intent: ServiceStartIntent,
    /// Answered by the fold when the *stop* half lands; the start that
    /// follows reports its own progress.
    pub(crate) reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
    /// See [`super::ProcessReport::ServiceStarting::restarting`].
    pub(crate) announce_restarting: bool,
    /// Clear the failure history first. An explicitly requested restart is a
    /// fresh chance; the restart policy's *own* retry must not be, or the
    /// streak that bounds a crash loop would be wiped by every attempt it
    /// scheduled — and the loop would never end.
    pub(crate) reset_policy: bool,
}

/// One request to rebuild: produce a fresh artifact, then restart into it.
pub(crate) struct RebuildRequest {
    /// Skip the build tool's up-to-date check — the hard-restart path.
    pub(crate) forced: bool,
    /// Answered as soon as the build is *accepted*, not when it finishes.
    /// A forced rebuild refused because a batch is already running is the
    /// hard-restart path's synchronous "already in progress".
    pub(crate) reply: Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>,
}

/// Everything a service's supervisor can be asked to do.
pub(crate) enum ServiceCommand {
    /// Begin a start — or supersede the one being prepared.
    Start(StartRequest),
    /// End the held process and start a fresh one. See [`RestartRequest`].
    Restart(Box<RestartRequest>),
    /// Build this service's artifact and restart into it. See
    /// [`RebuildRequest`] and the cycle it drives in `supervise`.
    Rebuild(RebuildRequest),
    /// A watched file changed while a rebuild cycle was running, so the
    /// artifact that cycle is about to produce is already out of date. The
    /// cycle skips its restart and lets the follow-up cycle do it.
    MarkStale,
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
    /// [`super::ProcessReport::Demand`].
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
    /// See [`RestartRequest::reset_policy`]. A user stopping a service clears
    /// its failure history; a stop that is one step of a rebuild does not.
    pub(crate) reset_policy: bool,
}

/// How a stop's completion travels back.
pub(crate) enum StopNotify {
    /// The manual path: [`super::ProcessReport::ServiceStopComplete`] on the
    /// report channel, carrying the requester's reply for the fold to answer.
    Reply(Option<tokio::sync::oneshot::Sender<crate::command::CommandResult>>),
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
    /// Whether a bound port may fall back to an ephemeral one. A workspace
    /// constant, so it belongs here rather than on every start request.
    pub(crate) fallback_ports: bool,
    /// Where every peer can be reached, for rendering this service's
    /// `$(peer.KEY)` env references at the moment it starts.
    pub(crate) endpoints: crate::endpoints::EndpointReader,
    /// Set once teardown begins. Checked before every self-started start, so
    /// a supervisor cannot spawn into a shutdown the runner has already
    /// planned around — which is why the gate needs no teardown revocation.
    pub(crate) shutdown_rx: tokio::sync::watch::Receiver<bool>,
    /// The build manager's mailbox. Every build this supervisor needs —
    /// the first one as much as a rebuild — is asked for here, because
    /// coalescing is cross-service (one `bazel build` for N targets) even
    /// though the cycle it feeds belongs to each supervisor.
    pub(crate) batcher_tx: mpsc::UnboundedSender<crate::build_tool::batcher::BatchRequest>,
    /// Project-wide watch-ignore patterns, for the build spec this supervisor
    /// hands the build manager.
    pub(crate) global_watch_ignore: Vec<String>,
}

/// Owner half for services.
pub(crate) type ServiceStarts = super::registry::Supervisors<ServiceCommand>;

/// Start one start-supervisor per service, each taking ownership of its
/// bound proxy (if any) from `proxies`.
pub(crate) fn spawn_supervisors<'a>(
    names: impl Iterator<Item = &'a String>,
    env: &StartEnv,
    outputs: &dyn Fn(&str) -> Option<ProcessOutput>,
    resolved: &dyn Fn(&str) -> Option<crate::config::ResolvedService>,
    report_tx: &mpsc::UnboundedSender<super::ProcessReport>,
    proxies: &mut std::collections::HashMap<String, ProxyAssets>,
    gates: &mut std::collections::HashMap<String, crate::gate::GateReader>,
) -> ServiceStarts {
    ServiceStarts::spawn_all(names, |name, rx, busy| {
        let output = outputs(&name);
        let assets = proxies.remove(&name);
        let resolved = resolved(&name);
        let gate = gates.remove(&name);
        supervise(
            name,
            rx,
            env.clone(),
            output,
            report_tx.clone(),
            busy,
            assets,
            resolved,
            gate,
        )
    })
}

/// Assemble the context for one start.
///
/// Everything here is either the supervisor's own (its resolved config, its
/// last docker mapping) or read from a published projection — which is what
/// lets a supervisor start itself without asking the scheduler for anything.
fn build_context(
    name: &str,
    resolved: &crate::config::ResolvedService,
    batch_built: bool,
    env: &StartEnv,
    last_docker_bindings: &[crate::docker::DockerPortBinding],
) -> Result<Box<ServiceStartContext>, String> {
    let mut resolved = resolved.clone();
    crate::endpoints::render_env(&env.endpoints.snapshot(), name, &mut resolved.env)
        .map_err(|error| error.to_string())?;
    Ok(Box::new(ServiceStartContext {
        resolved,
        batch_built,
        // The proxy's contribution is filled in per spawn below.
        listen_fds: Vec::new(),
        listen_fds_env: std::collections::HashMap::new(),
        fallback_ports: env.fallback_ports,
        prior_docker_port_bindings: last_docker_bindings.to_vec(),
    }))
}

/// Wait on a gate slot, parking forever when there is none — so an absent
/// gate never completes and never consumes its `select!` branch.
async fn wait_gate(gate: &mut Option<crate::gate::GateReader>) -> Option<()> {
    match gate.as_mut() {
        Some(gate) => gate.changed().await,
        None => std::future::pending().await,
    }
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
    output: Option<ProcessOutput>,
    report_tx: mpsc::UnboundedSender<super::ProcessReport>,
    busy: Arc<AtomicBool>,
    proxy_assets: Option<ProxyAssets>,
    resolved: Option<crate::config::ResolvedService>,
    mut gate: Option<crate::gate::GateReader>,
) {
    // Names come from the same map the configs do, so `None` is unreachable;
    // ending the supervisor beats panicking, and callers already treat a dead
    // mailbox as "supervisor is gone".
    let Some(mut resolved) = resolved else { return };
    let service_writer = output.as_ref().map(|output| output.writer());
    // Whether the build manager has produced this service's artifact, so the
    // per-service build inside `start_service_worker` must not run again.
    let mut batch_built = false;
    // Where the build manager delivers this service's artifact.
    let (prepare_tx, mut prepare_rx) =
        mpsc::unbounded_channel::<crate::build_tool::batch::PrepareOutcome>();
    // An artifact request is outstanding. A supervisor that needs an artifact
    // does not spawn until it has one, whatever its gate says — an artifact is
    // as much a precondition as a dependency, and getting it is its own job.
    let mut awaiting_artifact = resolved.is_build_tool_managed()
        && !resolved.lazy
        && request_artifact(&name, &resolved, &env, &prepare_tx, &report_tx);
    // Whether anything wants this service running. One-shot: see `Demand`.
    // A lazy service starts life unwanted and is demanded by its first
    // connection; everything else is wanted from the moment it exists.
    let mut demand = if resolved.lazy {
        super::Demand::None
    } else {
        super::Demand::Scheduled
    };
    // The revision current when this demand arose. A level published before
    // it cannot have taken it into account, so it is not safe to act on —
    // see `crate::gate`. Starts at 0 so the initial set waits for the
    // scheduler's first pass.
    let mut demand_rev: u64 = 0;
    // The docker host-port mapping this supervisor's last spawn got. Retained
    // across stops so the next start can request the same ports.
    let mut last_docker_bindings: Vec<crate::docker::DockerPortBinding> = Vec::new();
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
    // A rebuild cycle in flight: a build was asked for and the restart it
    // implies has not happened yet. `stale` records that a watched file
    // changed *during* the cycle, which is what makes the artifact it is
    // about to produce already out of date.
    let mut cycle: Option<CycleState> = None;
    // A build succeeded but its restart was skipped because the cycle went
    // stale, so the running process is behind the artifact. Up-to-date is
    // measured against the last *build*, not the running process, so the
    // follow-up cycle must restart even when the build tool says there is
    // nothing to do. Cleared only by a restart that actually happens.
    let mut artifact_ahead = false;
    // Where the batcher delivers this service's share of a batch.
    let (rebuild_tx, mut rebuild_rx) =
        mpsc::unbounded_channel::<crate::build_tool::batcher::RebuildItemOutcome>();
    // The OSC response scanner for the held spawn. It holds a sender into the
    // PTY gate, which holds the master's write half, so a stale one keeps the
    // PTY open — it belongs with the process, not with a shadow of it.
    let mut osc_sink: Option<crate::output::OscSinkHandle> = None;
    // Health transitions from the monitor this supervisor spawns. They land
    // here rather than on the report channel so the restart policy sees them
    // before the scheduler does.
    let (health_tx, mut health_rx) = mpsc::unbounded_channel::<bool>();
    // Restart policy. Every input it needs — a failed prepare, a failed ready
    // check, a health transition, how long this spawn lived — is something
    // this loop observed itself, which is what lets the whole of it live here.
    let mut policy =
        super::health::RestartPolicy::new(resolved.on_failure, resolved.lazy && proxy.is_some());
    // When the armed auto-restart is due, and which attempt it is.
    let mut backoff: Option<(tokio::time::Instant, u32)> = None;
    // Facts about the spawn currently held.
    let mut spawned_at: Option<std::time::Instant> = None;
    let mut reached_ready = false;
    // This spawn failed its ready check but was left running (the `notify`
    // policy). Its eventual exit is then old news, not a fresh failure — the
    // scheduler already marked it `Failed` and reported why.
    let mut ready_failed = false;
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
                // Start when something wants this running, its dependencies
                // allow it, and it is holding nothing. The level is read here
                // rather than waited on, so a grant published while this
                // supervisor was busy is not missed.
                //
                // `demand` is cleared in this same step — that one-shot-ness
                // is what stops a crashing service relaunching off a gate
                // that stays open across the crash. See `Demand`.
                if held.is_none()
                    && !awaiting_artifact
                    && !*env.shutdown_rx.borrow()
                    && gate.as_ref().is_some_and(|reader| {
                        let grant = reader.get();
                        // Only a level decided *after* this demand arose can
                        // be trusted to have accounted for it.
                        grant.rev > demand_rev && demand.permitted_by(grant.level)
                    })
                {
                    demand = super::Demand::None;
                    env.emitter
                        .service_debug_event(&name, "start triggered (deps satisfied)");
                    // Tell the scheduler a start is under way, so it can fold
                    // Pending -> Starting. Only this supervisor knows when
                    // demand is actually spent.
                    if report_tx
                        .send(super::ProcessReport::ServiceStarting {
                            name: name.clone(),
                            restarting: false,
                        })
                        .is_err()
                    {
                        return;
                    }
                    busy.store(true, Ordering::Relaxed);
                    break ServiceCommand::Start(StartRequest {
                        mode: ServiceStartMode::Full,
                        intent: super::ServiceStartIntent::Scheduled,
                        fresh_backend_ports: false,
                    });
                }
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
                        osc_sink = None;
                        if let Some(handle) = reader.take() {
                            await_reader(handle).await;
                        }
                        // The spawn is dead: unregister attach so new clients
                        // are refused and muted stdout resumes.
                        if let Some(output) = output.as_ref() {
                            output.clear_attach().await;
                        }
                        if reap_and_report(
                            &name,
                            &mut held,
                            &report_tx,
                            &env,
                            &mut policy,
                            &mut backoff,
                            spawned_at.take(),
                            reached_ready,
                            ready_failed,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                        reached_ready = false;
                        ready_failed = false;
                        continue;
                    }
                    // The ready racer settled — forward through this loop
                    // so the outcome trails its own prepared report.
                    outcome = wait_ready(&mut ready_pending), if ready_pending.is_some() => {
                        ready_pending = None;
                        if let Some(outcome) = outcome {
                            let policy_outcome = if outcome.success {
                                reached_ready = true;
                                policy.on_ready();
                                backoff = None;
                                super::health::PolicyOutcome::None
                            } else {
                                ready_failed = true;
                                let decided = policy.decide(super::health::FailureKind::Ready);
                                arm_backoff(&name, &env, &decided, &mut backoff, outcome.message.as_deref());
                                // A lazy service that failed its ready check
                                // may still be running — it never bound its
                                // port, say. Nothing else will end it, and
                                // while it lives it holds the PTY open.
                                if matches!(decided, super::health::PolicyOutcome::LazyRearm { .. }) {
                                    reader_eof = None;
                                    monitor_cancel = None;
                                    osc_sink = None;
                                    let _ = run_stop(
                                        &name,
                                        &env,
                                        output.as_ref(),
                                        &mut held,
                                        &mut reader,
                                        &effective_shutdown(&resolved, &env),
                                        true,
                                        false,
                                        None,
                                    )
                                    .await;
                                }
                                decided
                            };
                            if report_tx
                                .send(super::ProcessReport::ServiceReady {
                                    name: name.clone(),
                                    success: outcome.success,
                                    message: outcome.message,
                                    had_check: outcome.had_check,
                                    policy: policy_outcome,
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                        continue;
                    }
                    // This service's artifact, from the build manager.
                    // Nothing has spawned yet, and nothing can until this
                    // lands — which is also what puts the watch registrations
                    // this build resolved in place before the first start.
                    outcome = prepare_rx.recv() => {
                        use crate::build_tool::batch::PrepareOutcome;
                        let Some(outcome) = outcome else { continue };
                        busy.store(true, Ordering::Relaxed);
                        match outcome {
                            PrepareOutcome::Ready { binary_path } => {
                                awaiting_artifact = false;
                                batch_built = true;
                                // Written onto the config this supervisor
                                // already holds — the build taught us the path
                                // and nothing else.
                                if let Some(path) = binary_path {
                                    resolved.resolved_binary_path = Some(path);
                                }
                                if report_tx
                                    .send(super::ProcessReport::ArtifactBuild {
                                        name: name.clone(),
                                        kind: super::ProcessKind::Service,
                                        status: super::ArtifactBuildStatus::Ready,
                                    })
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            // A watched file changed while the build ran, so
                            // the artifact is already out of date. The build
                            // manager narrated why; ask again. The service
                            // stays `Building` throughout, so nothing starts
                            // against it.
                            PrepareOutcome::Stale => {
                                awaiting_artifact = request_artifact(
                                    &name, &resolved, &env, &prepare_tx, &report_tx,
                                );
                            }
                            PrepareOutcome::Failed(message) => {
                                awaiting_artifact = false;
                                // The end of the road, not a crash. Retrying a
                                // compile that just failed recompiles the same
                                // broken sources, so withdrawing demand here is
                                // what keeps this away from the restart policy.
                                demand = super::Demand::None;
                                if report_tx
                                    .send(super::ProcessReport::ArtifactBuild {
                                        name: name.clone(),
                                        kind: super::ProcessKind::Service,
                                        status: super::ArtifactBuildStatus::Failed(message),
                                    })
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        continue;
                    }
                    // This service's share of a finished batch. The cycle
                    // continues here rather than in the scheduler, so the
                    // build, the stop and the spawn are one sequence in one
                    // place.
                    outcome = rebuild_rx.recv() => {
                        let Some(outcome) = outcome else { continue };
                        busy.store(true, Ordering::Relaxed);
                        match settle_cycle(
                            &name,
                            &env,
                            outcome,
                            &mut cycle,
                            &mut artifact_ahead,
                            &report_tx,
                            &mut batch_built,
                        ) {
                            CycleNext::Done => continue,
                            CycleNext::Stop => return,
                            CycleNext::Restart => {
                                break ServiceCommand::Restart(Box::new(RestartRequest {
                                    config: effective_shutdown(&resolved, &env),
                                    wait_full_exit: resolved.requires_full_exit_on_restart(),
                                    interrupt: None,
                                    // Connections arriving mid-restart queue
                                    // instead of racing the dying process.
                                    clear_backend_first: true,
                                    start_mode: ServiceStartMode::SpawnOnly,
                                    fresh_backend_ports: true,
                                    intent: super::ServiceStartIntent::Background,
                                    reply: None,
                                    announce_restarting: true,
                                    // A rebuild is not a user giving up on
                                    // the service; the failure history rides
                                    // across it.
                                    reset_policy: false,
                                }));
                            }
                        }
                    }
                    // The monitor this supervisor started saw the service
                    // change health. The policy decides before the scheduler
                    // hears about it.
                    transition = health_rx.recv() => {
                        let Some(healthy) = transition else { continue };
                        busy.store(true, Ordering::Relaxed);
                        let policy_outcome = if healthy {
                            // Recovery clears the backoff counter only; the
                            // rapid-crash streak is cleared by a spawn that
                            // outlives the crash window, not by a transient
                            // return to Ready.
                            policy.on_ready();
                            backoff = None;
                            super::health::PolicyOutcome::None
                        } else {
                            let decided = policy.decide(super::health::FailureKind::Unhealthy);
                            arm_backoff(&name, &env, &decided, &mut backoff, Some("unhealthy"));
                            decided
                        };
                        if report_tx
                            .send(super::ProcessReport::HealthChanged {
                                name: name.clone(),
                                healthy,
                                policy: policy_outcome,
                            })
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    // The armed auto-restart came due. It runs as an ordinary
                    // internal restart, so the stop-then-start sequence and
                    // its reports are the same ones a manual restart makes.
                    () = wait_backoff(&backoff) => {
                        busy.store(true, Ordering::Relaxed);
                        let attempt = backoff.take().map_or(1, |(_, attempt)| attempt);
                        env.emitter
                            .service_event(&name, &format!("auto-restart firing (attempt {attempt})"));
                        break ServiceCommand::Restart(Box::new(RestartRequest {
                            config: effective_shutdown(&resolved, &env),
                            wait_full_exit: false,
                            interrupt: None,
                            clear_backend_first: false,
                            start_mode: ServiceStartMode::Full,
                            fresh_backend_ports: false,
                            intent: super::ServiceStartIntent::Background,
                            reply: None,
                            announce_restarting: false,
                            // This IS the policy retrying. Keeping the streak
                            // is what lets the ceiling ever be reached.
                            reset_policy: false,
                        }));
                    }
                    // Permission changed. Nothing is decided here: the level
                    // is read at the top of this loop, which is the single
                    // place a grant is spent.
                    changed = wait_gate(&mut gate), if gate.is_some() => {
                        // The scheduler is gone. A `watch::Receiver` with no
                        // sender errors immediately and forever, so drop the
                        // slot rather than spin the select.
                        if changed.is_none() {
                            gate = None;
                        }
                        continue;
                    }
                    // The lazy proxy saw a connection: demand. The runner
                    // gates on service state, so duplicates are harmless.
                    trigger = wait_demand(&mut demand_rx), if demand_rx.is_some() => {
                        match trigger {
                            Some(_) => {
                                demand = demand.max(super::Demand::Scheduled);
                                demand_rev = gate.as_ref().map_or(0, |g| g.rev());
                                let _ = report_tx.send(super::ProcessReport::Demand {
                                    name: name.clone(),
                                    demand,
                                });
                                // A lazy service is the one thing that does
                                // not build at construction: nobody had asked
                                // for it yet. Now somebody has — and it still
                                // builds before its dependencies are checked,
                                // for the same reason everything else does.
                                if resolved.is_build_tool_managed()
                                    && !batch_built
                                    && !awaiting_artifact
                                {
                                    env.emitter.service_event(
                                        &name,
                                        "first connection — building before start",
                                    );
                                    awaiting_artifact = request_artifact(
                                        &name, &resolved, &env, &prepare_tx, &report_tx,
                                    );
                                }
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
            mode,
            intent,
            fresh_backend_ports,
        } = match command {
            ServiceCommand::Start(request) => request,
            ServiceCommand::Proxy(directive) => {
                apply_proxy_directive(&name, &env.emitter, &mut proxy, directive);
                continue;
            }
            ServiceCommand::MarkStale => {
                // Only meaningful inside a cycle. Outside one the watcher
                // sends an ordinary Rebuild instead.
                if let Some(cycle) = cycle.as_mut() {
                    cycle.stale = true;
                }
                continue;
            }
            ServiceCommand::Rebuild(request) => {
                // A new cycle: staleness is per-cycle, so it starts clear.
                // `artifact_ahead` deliberately does not — it records that
                // the *process* is behind, which no new request changes.
                cycle = Some(CycleState { stale: false });
                if !resolved.is_build_tool_managed() {
                    // Not batched: run this service's own build command here,
                    // *before* the stop, so a failed build leaves the version
                    // that works still running.
                    env.emitter
                        .service_event(&name, "rebuilding (file changed)");
                    if let Some(reply) = request.reply {
                        let _ = reply.send(Ok(()));
                    }
                    let build = super::service_worker::run_service_build_worker(
                        &env.base_dir,
                        env.docker_client.as_ref(),
                        &env.emitter,
                        &name,
                        &resolved,
                        false,
                        service_writer.as_ref(),
                    );
                    // Raced against the mailbox so a Stop or a shutdown can
                    // cut a slow build short — the build child is
                    // `kill_on_drop`, so abandoning the future ends it.
                    tokio::pin!(build);
                    let mut shutdown_rx = env.shutdown_rx.clone();
                    let outcome = loop {
                        tokio::select! {
                            result = &mut build => break result,
                            // Teardown must not wait out a slow build. The
                            // build child is `kill_on_drop`, so abandoning the
                            // future here ends it.
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    env.emitter.service_event(
                                        &name,
                                        "rebuild cancelled by shutdown",
                                    );
                                    cycle = None;
                                    break Err("shutdown requested".to_string());
                                }
                            }
                            next = rx.recv(), if !mailbox_closed => match next {
                                Some(ServiceCommand::MarkStale) => {
                                    if let Some(cycle) = cycle.as_mut() {
                                        cycle.stale = true;
                                    }
                                }
                                Some(ServiceCommand::Proxy(directive)) => {
                                    apply_proxy_directive(
                                        &name, &env.emitter, &mut proxy, directive,
                                    );
                                }
                                Some(next) => {
                                    // Superseded: drop the build and take the
                                    // newer command.
                                    cycle = None;
                                    pending = Some(next);
                                    break Err("superseded".to_string());
                                }
                                None => mailbox_closed = true,
                            },
                        }
                    };
                    if cycle.is_none() {
                        continue;
                    }
                    let item = match outcome {
                        Ok(()) => crate::build_tool::batcher::RebuildItemOutcome::NotBuilt,
                        Err(message) => {
                            crate::build_tool::batcher::RebuildItemOutcome::Failed(message)
                        }
                    };
                    match settle_cycle(
                        &name,
                        &env,
                        item,
                        &mut cycle,
                        &mut artifact_ahead,
                        &report_tx,
                        &mut batch_built,
                    ) {
                        CycleNext::Done => continue,
                        CycleNext::Stop => return,
                        CycleNext::Restart => {
                            pending = Some(ServiceCommand::Restart(Box::new(RestartRequest {
                                config: effective_shutdown(&resolved, &env),
                                wait_full_exit: resolved.requires_full_exit_on_restart(),
                                interrupt: None,
                                clear_backend_first: true,
                                start_mode: ServiceStartMode::SpawnOnly,
                                fresh_backend_ports: true,
                                intent: super::ServiceStartIntent::Background,
                                reply: None,
                                announce_restarting: true,
                                reset_policy: false,
                            })));
                            continue;
                        }
                    }
                }
                let spec = rebuild_spec_for(&name, &resolved, &env);
                let accepted = queue_build(&env, spec, &request, &rebuild_tx).await;
                if let Some(reply) = request.reply {
                    let _ = reply.send(accepted.clone().map_err(|message| {
                        crate::command::CommandError::InvalidState {
                            name: name.clone(),
                            message,
                        }
                    }));
                }
                if accepted.is_err() {
                    cycle = None;
                }
                continue;
            }
            ServiceCommand::Stop(request) => {
                reader_eof = None;
                monitor_cancel = None;
                osc_sink = None;
                ready_pending = None;
                // A stop withdraws demand: nothing wants this running now, so
                // an open gate cannot undo it. A restart's follow-up start is
                // part of the same command, so it does not consult demand.
                demand = super::Demand::None;
                if request.reset_policy {
                    policy.reset();
                }
                // Whatever the policy had queued is moot: this process is
                // going away by request.
                backoff = None;
                spawned_at = None;
                reached_ready = false;
                ready_failed = false;
                let result = run_stop(
                    &name,
                    &env,
                    output.as_ref(),
                    &mut held,
                    &mut reader,
                    &request.config,
                    request.force,
                    request.wait_full_exit,
                    request.interrupt,
                )
                .await;
                match request.notify {
                    StopNotify::Reply(reply) => {
                        if report_tx
                            .send(super::ProcessReport::ServiceStopComplete {
                                name: name.clone(),
                                result,
                                reply,
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
            ServiceCommand::Restart(request) => {
                reader_eof = None;
                monitor_cancel = None;
                osc_sink = None;
                ready_pending = None;
                demand = super::Demand::None;
                if request.reset_policy {
                    policy.reset();
                }
                backoff = None;
                spawned_at = None;
                reached_ready = false;
                ready_failed = false;
                // Owning the proxy makes this a call rather than a mailbox
                // hop, so it cannot arrive after the stop it must precede.
                if request.clear_backend_first
                    && let Some(proxy) = proxy.as_ref()
                {
                    proxy.clear_backend();
                }
                let stop = run_stop(
                    &name,
                    &env,
                    output.as_ref(),
                    &mut held,
                    &mut reader,
                    &request.config,
                    false,
                    request.wait_full_exit,
                    request.interrupt,
                );
                // Race the mailbox while stopping. A `MarkStale` landing here
                // is the one the runner used to catch by processing
                // `RebuildStale` during a detached stop; without this the
                // change is lost, because the watcher sends no second
                // `Rebuild` for a cycle it believes is already running.
                tokio::pin!(stop);
                let result = loop {
                    tokio::select! {
                        result = &mut stop => break result,
                        next = rx.recv(), if !mailbox_closed => match next {
                            Some(ServiceCommand::MarkStale) => {
                                if let Some(cycle) = cycle.as_mut() {
                                    cycle.stale = true;
                                }
                            }
                            Some(ServiceCommand::Proxy(directive)) => {
                                apply_proxy_directive(
                                    &name, &env.emitter, &mut proxy, directive,
                                );
                            }
                            Some(next) => {
                                pending = Some(next);
                                break (&mut stop).await;
                            }
                            None => mailbox_closed = true,
                        },
                    }
                };
                // A stale cycle skips its spawn: the service stays stopped and
                // the follow-up cycle brings it up on the newer sources.
                let stale_now = cycle.as_ref().is_some_and(|cycle| cycle.stale);
                if stale_now && cycle.take().is_some() {
                    let _ = report_tx.send(super::ProcessReport::RebuildCycleDone {
                        name: name.clone(),
                        success: true,
                    });
                }
                // A failed stop leaves nothing safe to start over, and a
                // teardown that began mid-restart wants no new process.
                let restarting = result.is_ok() && !stale_now && !*env.shutdown_rx.borrow();
                if report_tx
                    .send(super::ProcessReport::ServiceStopComplete {
                        name: name.clone(),
                        result,
                        reply: request.reply,
                    })
                    .is_err()
                {
                    return;
                }
                if !restarting {
                    continue;
                }
                // Reported separately from the stop so the scheduler folds
                // Stopped before Starting — the transition pair a restart has
                // always shown.
                if report_tx
                    .send(super::ProcessReport::ServiceStarting {
                        name: name.clone(),
                        restarting: request.announce_restarting,
                    })
                    .is_err()
                {
                    return;
                }
                // Committing to a spawn brings the process up to the current
                // artifact.
                artifact_ahead = false;
                cycle = None;
                StartRequest {
                    mode: request.start_mode,
                    intent: request.intent,
                    fresh_backend_ports: request.fresh_backend_ports,
                }
            }
        };

        // A new start supersedes the previous run's in-flight ready
        // outcome; forwarding it after this run's prepared report would be
        // exactly the stale-completion race this loop exists to prevent.
        ready_pending = None;

        // Build this start's context here, as the thing that owns the start.
        // `$(peer.KEY)` references resolve against the endpoint projection at
        // *this* moment, so a peer that moved to a new port since the last
        // start is picked up without anyone re-issuing the request.
        let mut context =
            match build_context(&name, &resolved, batch_built, &env, &last_docker_bindings) {
                Ok(context) => context,
                Err(message) => {
                    let decided = match intent {
                        super::ServiceStartIntent::Background => {
                            let decided = policy.decide(super::health::FailureKind::Prepare);
                            arm_backoff(&name, &env, &decided, &mut backoff, Some(&message));
                            decided
                        }
                        _ => {
                            policy.reset();
                            super::health::PolicyOutcome::None
                        }
                    };
                    if report_tx
                        .send(super::ProcessReport::ServiceStartPrepared {
                            name: name.clone(),
                            intent,
                            result: Err(message),
                            policy: decided,
                        })
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
            };

        // The proxy's per-spawn contribution: fresh ephemeral backend ports
        // on restart, the backend/public env vars, and the listenfd sockets
        // the child inherits.
        if let Some(p) = proxy.as_mut() {
            if fresh_backend_ports && let Err(error) = p.reallocate_ephemeral_ports().await {
                let message = format!("failed to allocate ephemeral ports: {error}");
                let decided = match intent {
                    super::ServiceStartIntent::Background => {
                        let decided = policy.decide(super::health::FailureKind::Prepare);
                        arm_backoff(&name, &env, &decided, &mut backoff, Some(&message));
                        decided
                    }
                    _ => {
                        policy.reset();
                        super::health::PolicyOutcome::None
                    }
                };
                if report_tx
                    .send(super::ProcessReport::ServiceStartPrepared {
                        name: name.clone(),
                        intent,
                        result: Err(message),
                        policy: decided,
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
                        apply_proxy_directive(&name, &env.emitter, &mut proxy, directive);
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
                            &mut osc_sink,
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
                        // Remember the mapping so a restart can request the
                        // same host ports. This supervisor produced them, so
                        // its copy is the authoritative one.
                        last_docker_bindings = wired.docker_port_bindings.clone();
                        // The crash ceiling measures from here.
                        spawned_at = Some(std::time::Instant::now());
                        reached_ready = false;
                        ready_failed = false;
                        (Ok(Box::new(wired)), Some((ready, exit_rx, cancel_rx)))
                    }
                    Err(message) => (Err(message), None),
                };
                // A start that could not be prepared is a failure like any
                // other *unless the build tool refused*: retrying a build
                // recompiles sources that have not changed, so no amount of
                // backoff can change the answer. Only a background start is
                // retried at all — the others have someone waiting on a reply.
                let prepare_policy = match (&wired, &intent) {
                    (Err(failure), super::ServiceStartIntent::Background)
                        if !failure.from_build =>
                    {
                        let decided = policy.decide(super::health::FailureKind::Prepare);
                        arm_backoff(&name, &env, &decided, &mut backoff, Some(&failure.message));
                        decided
                    }
                    (Err(_), _) => {
                        policy.reset();
                        backoff = None;
                        super::health::PolicyOutcome::None
                    }
                    (Ok(_), _) => super::health::PolicyOutcome::None,
                };
                if report_tx
                    .send(super::ProcessReport::ServiceStartPrepared {
                        name: name.clone(),
                        intent,
                        result: wired.map_err(|failure| failure.message),
                        policy: prepare_policy,
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
                            ready,
                            exit_rx,
                            cancel_rx,
                            health_tx.clone(),
                        ));
                    }
                    Some((None, _exit_rx, _cancel_rx))
                        if report_tx
                            .send(super::ProcessReport::ServiceReady {
                                name: name.clone(),
                                success: true,
                                message: None,
                                had_check: false,
                                policy: super::health::PolicyOutcome::None,
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

/// A rebuild cycle in flight.
struct CycleState {
    /// A watched file changed since this cycle began.
    stale: bool,
}

/// What a settled batch outcome asks the loop to do next.
enum CycleNext {
    /// The cycle is over; nothing to start.
    Done,
    /// Stop and restart into the artifact.
    Restart,
    /// The report channel closed — the scheduler is gone.
    Stop,
}

/// Ask the build manager for this service's artifact, and tell the scheduler
/// a build is under way. Returns whether a request is now outstanding.
///
/// Sent when the supervisor is *constructed*, not when its gate opens.
/// An artifact does not depend on postgres listening, so building at gate-open
/// would serialise every build along the dependency chain — and hand bazel one
/// invocation per service instead of one for the whole workspace. Dependencies
/// gate *running*.
///
/// The report goes first so that "a build is outstanding" is never claimed
/// without the scheduler having been told; a dead scheduler means this
/// supervisor is about to end anyway.
fn request_artifact(
    name: &str,
    resolved: &crate::config::ResolvedService,
    env: &StartEnv,
    outcome: &mpsc::UnboundedSender<crate::build_tool::batch::PrepareOutcome>,
    report_tx: &mpsc::UnboundedSender<super::ProcessReport>,
) -> bool {
    if report_tx
        .send(super::ProcessReport::ArtifactBuild {
            name: name.to_string(),
            kind: super::ProcessKind::Service,
            status: super::ArtifactBuildStatus::Started,
        })
        .is_err()
    {
        return false;
    }
    let working_dir = super::paths::working_dir_for(&env.base_dir, resolved.dir.as_deref());
    let ignore = super::paths::resolve_watch_ignore_patterns(
        &working_dir,
        &resolved.ignore,
        &env.base_dir,
        &env.global_watch_ignore,
    );
    env.batcher_tx
        .send(crate::build_tool::batcher::BatchRequest::QueuePrepare {
            item: Box::new(crate::build_tool::batch::BatchBuildItem {
                name: name.to_string(),
                kind: super::ProcessKind::Service,
                bazel: resolved.bazel_config().cloned(),
                watch_enabled: resolved.build_tool_watch_enabled(),
                working_dir,
                ignore,
            }),
            outcome: outcome.clone(),
        })
        .is_ok()
}

/// Capture what the batcher needs to rebuild this service. Resolved config is
/// fixed after construction, so a spec built now equals one built at flush
/// time — which is what frees the batcher from reading anyone's state.
fn rebuild_spec_for(
    name: &str,
    resolved: &crate::config::ResolvedService,
    env: &StartEnv,
) -> crate::build_tool::batcher::RebuildSpec {
    use crate::build_tool::batcher::RebuildSpec;
    match &resolved.kind {
        Some(crate::config::ServiceKind::Bazel(bazel)) => {
            RebuildSpec::Bazel(crate::build_tool::batch::BazelRebuildItem {
                name: name.to_string(),
                target: bazel.target.clone(),
                working_dir: super::paths::working_dir_for(&env.base_dir, resolved.dir.as_deref()),
            })
        }
        _ => RebuildSpec::Plain {
            name: name.to_string(),
        },
    }
}

/// Ask the batcher to build this service, forced or coalesced.
///
/// Awaiting the forced reply inline is safe: the batcher never blocks on a
/// send (its outcome channels are unbounded), so the answer is immediate.
async fn queue_build(
    env: &StartEnv,
    spec: crate::build_tool::batcher::RebuildSpec,
    request: &RebuildRequest,
    outcome: &mpsc::UnboundedSender<crate::build_tool::batcher::RebuildItemOutcome>,
) -> Result<(), String> {
    use crate::build_tool::batcher::BatchRequest;
    let gone = || "build batcher is shutting down".to_string();
    if !request.forced {
        return env
            .batcher_tx
            .send(BatchRequest::QueueRebuild {
                spec,
                outcome: outcome.clone(),
            })
            .map_err(|_| gone());
    }
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    env.batcher_tx
        .send(BatchRequest::ForceRebuild {
            spec,
            outcome: outcome.clone(),
            reply: reply_tx,
        })
        .map_err(|_| gone())?;
    match reply_rx.await {
        Ok(result) => result,
        Err(_) => Err(gone()),
    }
}

/// Decide what a batch outcome means for the cycle it belongs to.
///
/// This is the whole of the rebuild state machine, and it reads only state
/// this supervisor owns. The asymmetry between the arms is deliberate and
/// pinned by tests: `UpToDate` consults `artifact_ahead` but not staleness
/// (nothing was built, so nothing went stale), `Built` consults staleness and
/// *sets* `artifact_ahead`, and a pass-through build sets neither.
fn settle_cycle(
    name: &str,
    env: &StartEnv,
    outcome: crate::build_tool::batcher::RebuildItemOutcome,
    cycle: &mut Option<CycleState>,
    artifact_ahead: &mut bool,
    report_tx: &mpsc::UnboundedSender<super::ProcessReport>,
    batch_built: &mut bool,
) -> CycleNext {
    use crate::build_tool::batcher::RebuildItemOutcome as Item;
    let stale = cycle.as_ref().is_some_and(|cycle| cycle.stale);
    let done = |success: bool, cycle: &mut Option<CycleState>| {
        *cycle = None;
        if report_tx
            .send(super::ProcessReport::RebuildCycleDone {
                name: name.to_string(),
                success,
            })
            .is_err()
        {
            return CycleNext::Stop;
        }
        CycleNext::Done
    };

    match outcome {
        Item::Failed(message) => {
            // The old process keeps running: a failed build is no reason to
            // take away the version that works.
            env.emitter.service_error_event(name, &message);
            done(false, cycle)
        }
        Item::Built => {
            *batch_built = true;
            if stale {
                // Skip restarting into an artifact already known to be out of
                // date, but remember that the process is now behind a
                // successful build.
                *artifact_ahead = true;
                return done(true, cycle);
            }
            CycleNext::Restart
        }
        Item::NotBuilt => {
            if stale {
                return done(true, cycle);
            }
            CycleNext::Restart
        }
        Item::UpToDate => {
            if *artifact_ahead {
                env.emitter.service_debug_event(
                    name,
                    "up to date, but process is behind last build — restarting",
                );
                return CycleNext::Restart;
            }
            env.emitter
                .service_debug_event(name, "skipped (no changes)");
            done(true, cycle)
        }
    }
}

/// End the held process and let its reader finish — the body both `Stop` and
/// `Restart` run.
///
/// Attach is unregistered *before* the reader is awaited: the registration
/// holds a PTY-gate sender and the gate holds the master's write half, so the
/// reader cannot see EOF until every sender is dropped. Awaiting it first
/// would deadlock against that until the 2s bound. Waiting for the drain at
/// all is what stops "stopped" outrunning the process's final lines.
#[allow(clippy::too_many_arguments)]
async fn run_stop(
    name: &str,
    env: &StartEnv,
    output: Option<&ProcessOutput>,
    held: &mut Option<service::ServiceHandle>,
    reader: &mut Option<tokio::task::JoinHandle<()>>,
    config: &ShutdownConfig,
    force: bool,
    wait_full_exit: bool,
    interrupt: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<(), String> {
    let result = match held.take() {
        Some(handle) => {
            let debug = service::StopDebug::new(name.to_string(), env.emitter.clone());
            match interrupt {
                Some(shutdown_rx) => service::stop_service_interruptibly(
                    handle,
                    Some(config),
                    wait_full_exit,
                    shutdown_rx,
                    Some(debug),
                )
                .await
                .map_err(|e| e.to_string()),
                None => {
                    service::stop_service(handle, Some(config), force, wait_full_exit, Some(debug))
                        .await
                        .map_err(|e| e.to_string())
                }
            }
        }
        // Nothing held: the process already exited and was reaped. Stopping
        // something stopped succeeds.
        None => Ok(()),
    };
    if let Some(output) = output {
        output.clear_attach().await;
    }
    if let Some(handle) = reader.take() {
        await_reader(handle).await;
    }
    result
}

/// Apply a runner proxy decision to the owned proxy. No-op for services
/// without one, and after `Shutdown`.
///
/// Whether a policy is a *change* is answered by the proxy itself rather than
/// by a shadow of what was last commanded — the owner knows. Narrating the
/// refusal edge belongs here for the same reason.
fn apply_proxy_directive(
    name: &str,
    emitter: &LifecycleEmitter,
    proxy: &mut Option<crate::proxy::ServiceProxy>,
    directive: ProxyDirective,
) {
    match directive {
        ProxyDirective::SetPolicy(policy) => {
            let Some(p) = proxy.as_mut() else { return };
            if !p.set_policy(policy) {
                return;
            }
            // Only the refusal edge is worth a line, and it belongs in the
            // normal log: a dev staring at `ECONNRESET` in their browser
            // shouldn't have to rerun with `--verbose` to find out why.
            match policy {
                crate::proxy::ConnectionPolicy::Refuse => {
                    emitter.service_error_event(name, "proxy refusing connections (service failed)")
                }
                _ => emitter.service_event(name, "proxy accepting connections again"),
            }
        }
        ProxyDirective::SetBackend => {
            if let Some(p) = proxy.as_ref() {
                p.set_backend();
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
    output: Option<&ProcessOutput>,
    service_writer: Option<&crate::output::ServiceWriter>,
    start_result: service::StartResult,
    proxy: Option<&crate::proxy::ServiceProxy>,
    held: &mut Option<service::ServiceHandle>,
    reader: &mut Option<tokio::task::JoinHandle<()>>,
    reader_eof: &mut Option<tokio::sync::oneshot::Receiver<()>>,
    monitor_cancel: &mut Option<tokio::sync::oneshot::Sender<()>>,
    osc_sink: &mut Option<crate::output::OscSinkHandle>,
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
    // Held by this supervisor for the spawn's lifetime. Dropping it ends the
    // scanner and releases its PTY-gate sender, so it must be dropped
    // wherever the process is: stop, reap, or the next wire replacing it.
    *osc_sink = match (pty, output) {
        (Some(pty), Some(output)) => {
            // Feed the server-side screen from process start — a correct
            // repaint on attach requires having seen the setup sequences.
            // Matches the PTY's initial 80x24 size.
            output.register_emulator(80, 24).await;
            // The gate owns the write half for this spawn's lifetime; the
            // scanner, the attach registration and any bridges hold senders
            // into it — the last one dropping (scanner + registration both
            // clear at reap) is what ends the gate.
            let pty_input = crate::output::spawn_pty_gate(pty);
            let osc_sink = output.add_osc_sink(pty_input.clone()).await;
            // Attach goes through the output state, not the runner: register
            // this spawn's gate so any client can attach from here on.
            output.set_attach_pty(pty_input).await;
            Some(osc_sink)
        }
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
    ready: crate::config::ReadyCheck,
    exit_rx: tokio::sync::oneshot::Receiver<()>,
    cancel_rx: tokio::sync::oneshot::Receiver<()>,
    health_tx: mpsc::UnboundedSender<bool>,
) -> tokio::sync::oneshot::Receiver<ReadyOutcome> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let monitor_cancel_rx = ready.monitor.then_some(cancel_rx);
    tokio::spawn(async move {
        let result = tokio::select! {
            result = service::run_ready_check(&ready) => result,
            _ = exit_rx => Err(service::ServiceError::ProcessExitedDuringReadyCheck),
        };
        let success = result.is_ok();
        if success && let Some(cancel_rx) = monitor_cancel_rx {
            // Health transitions go to the supervisor, not the scheduler: the
            // restart policy they feed lives there now.
            tokio::spawn(async move {
                super::health::run_health_monitor(ready, health_tx, cancel_rx).await;
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
#[allow(clippy::too_many_arguments)]
async fn reap_and_report(
    name: &str,
    held: &mut Option<service::ServiceHandle>,
    report_tx: &mpsc::UnboundedSender<super::ProcessReport>,
    env: &StartEnv,
    policy: &mut super::health::RestartPolicy,
    backoff: &mut Option<(tokio::time::Instant, u32)>,
    spawned_at: Option<std::time::Instant>,
    reached_ready: bool,
    ready_failed: bool,
) -> Result<(), ()> {
    if !matches!(held.as_ref(), Some(service::ServiceHandle::Process(_))) {
        return Ok(());
    }
    let Some(service::ServiceHandle::Process(mut proc)) = held.take() else {
        return Ok(());
    };
    let pgid = proc.pgid();
    // The reader already hit end-of-stream, so this wait returns promptly.
    let status = proc.wait().await.ok();
    let clean = status.as_ref().is_some_and(|s| s.success());
    let decided = if clean {
        policy.reset();
        *backoff = None;
        super::health::PolicyOutcome::None
    } else if ready_failed {
        // This spawn already failed its ready check and was reported then.
        // Its exit is the tail of that failure, not a fresh one — counting it
        // again would double-charge the crash ceiling.
        super::health::PolicyOutcome::None
    } else {
        // Narrated here, beside the decision it causes, so "auto-restart in
        // 1s" can never print before the death that prompted it.
        let message = super::health::format_unexpected_exit(status);
        env.emitter.service_error_event(name, &message);
        let decided = policy.decide(super::health::FailureKind::Crash {
            lived: spawned_at.map(|at| at.elapsed()),
            reached_ready,
        });
        arm_backoff(name, env, &decided, backoff, Some(&message));
        decided
    };
    report_tx
        .send(super::ProcessReport::ServiceExited {
            name: name.to_string(),
            pgid,
            status,
            policy: decided,
        })
        .map_err(|_| ())
}

/// Narrate a policy decision and arm the timer it asks for.
///
/// Both halves live here because the decision does: a line that explains a
/// restart belongs next to the code that scheduled it.
fn arm_backoff(
    name: &str,
    env: &StartEnv,
    outcome: &super::health::PolicyOutcome,
    backoff: &mut Option<(tokio::time::Instant, u32)>,
    reason: Option<&str>,
) {
    use super::health::PolicyOutcome;
    let window = super::health::RAPID_CRASH_WINDOW.as_secs();
    match outcome {
        PolicyOutcome::None => {}
        PolicyOutcome::RestartScheduled {
            attempt,
            backoff_secs,
        } => {
            env.emitter.service_error_event(
                name,
                &format!(
                    "{} — auto-restart in {backoff_secs}s (attempt {attempt})",
                    reason.unwrap_or("failed")
                ),
            );
            *backoff = Some((
                tokio::time::Instant::now() + std::time::Duration::from_secs(*backoff_secs),
                *attempt,
            ));
        }
        PolicyOutcome::GaveUpStarting { attempts } => {
            *backoff = None;
            env.emitter.service_error_event(
                name,
                &format!(
                    "{} — giving up after {attempts} failed starts without becoming ready",
                    reason.unwrap_or("failed")
                ),
            );
        }
        PolicyOutcome::GaveUpCrashing { rapid_crashes } => {
            *backoff = None;
            env.emitter.service_error_event(
                name,
                &format!(
                    "crashed within {window}s of starting {rapid_crashes} times in a row — \
                     giving up (not auto-restarting)"
                ),
            );
        }
        PolicyOutcome::LazyRearm {
            give_up,
            rapid_crashes,
        } => {
            *backoff = None;
            if *give_up {
                env.emitter.service_error_event(
                    name,
                    &format!(
                        "crashed within {window}s of starting {rapid_crashes} times in a row — \
                         giving up; not re-arming the lazy trigger \
                         (run `don restart {name}` to retry)"
                    ),
                );
            } else if let Some(message) = reason {
                env.emitter.service_error_event(
                    name,
                    &format!("{message} (will retry on next connection)"),
                );
            }
        }
    }
}

/// This service's shutdown settings layered over the workspace defaults —
/// the supervisor's copy of the runner's `effective_shutdown_config`.
fn effective_shutdown(resolved: &crate::config::ResolvedService, env: &StartEnv) -> ShutdownConfig {
    resolved
        .shutdown
        .clone()
        .map(|shutdown| shutdown.merged_over(&env.shutdown))
        .unwrap_or_else(|| env.shutdown.clone())
}

/// Sleep until an armed auto-restart is due, or pend forever when none is.
async fn wait_backoff(backoff: &Option<(tokio::time::Instant, u32)>) {
    match backoff {
        Some((due, _)) => tokio::time::sleep_until(*due).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::build_tool::batch::{BatchBuildItem, PrepareOutcome};
    use crate::build_tool::batcher::BatchRequest;
    use crate::config::{LogConfig, Platform, ProxyEntry, ProxyMode};
    use crate::output::OutputManager;
    use crate::proxy::{ConnectionPolicy, ServiceProxy};
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    async fn test_env() -> StartEnv {
        test_env_with_batcher().await.0
    }

    /// The same env, with the build manager's mailbox handed back so a test
    /// can see what this supervisor asks for — and answer it.
    async fn test_env_with_batcher() -> (StartEnv, mpsc::UnboundedReceiver<BatchRequest>) {
        let log_config = LogConfig::Stdout;
        let services = [("svc", &log_config)];
        let output_manager = OutputManager::new(&services, tokio::io::sink())
            .await
            .unwrap();
        let (batcher_tx, batcher_rx) = mpsc::unbounded_channel();
        let env = StartEnv {
            batcher_tx,
            base_dir: std::env::temp_dir(),
            pid_dir: std::env::temp_dir(),
            platform: Platform::LinuxX86_64,
            docker_client: None,
            emitter: output_manager.clone_lifecycle_emitter(),
            shutdown: ShutdownConfig::default(),
            fallback_ports: false,
            global_watch_ignore: Vec::new(),
            shutdown_rx: {
                let (tx, rx) = tokio::sync::watch::channel(false);
                std::mem::forget(tx);
                rx
            },
            endpoints: {
                let (writer, reader) = crate::endpoints::channel();
                writer.seed(std::iter::once("svc".to_string()));
                // Keep the writer alive for the reader's lifetime.
                std::mem::forget(writer);
                reader
            },
        };
        (env, batcher_rx)
    }

    /// A minimal service config for the supervisor harness.
    fn test_resolved() -> crate::config::ResolvedService {
        let config: crate::config::Config = "[services.svc]\nrun = { cmd = \"true\" }\n"
            .parse()
            .unwrap();
        config
            .services
            .get("svc")
            .unwrap()
            .resolve(Platform::LinuxX86_64)
    }

    /// A bazel-managed service — one that cannot spawn until the build
    /// manager has produced its artifact.
    fn bazel_resolved(lazy: bool) -> crate::config::ResolvedService {
        let config: crate::config::Config = "[services.svc]\nbazel.target = \"//svc:svc\"\n"
            .parse()
            .unwrap();
        let mut resolved = config
            .services
            .get("svc")
            .unwrap()
            .resolve(Platform::LinuxX86_64);
        resolved.lazy = lazy;
        resolved
    }

    struct Harness {
        tx: mpsc::UnboundedSender<ServiceCommand>,
        report_rx: mpsc::UnboundedReceiver<super::super::ProcessReport>,
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
            Some(test_resolved()),
            None,
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

    /// Take the artifact request this supervisor made, or report that it
    /// never made one.
    async fn next_prepare(
        batcher_rx: &mut mpsc::UnboundedReceiver<BatchRequest>,
    ) -> Option<(BatchBuildItem, mpsc::UnboundedSender<PrepareOutcome>)> {
        match tokio::time::timeout(Duration::from_secs(2), batcher_rx.recv()).await {
            Ok(Some(BatchRequest::QueuePrepare { item, outcome })) => Some((*item, outcome)),
            Ok(Some(_)) => panic!("expected a preparation request"),
            Ok(None) | Err(_) => None,
        }
    }

    /// **Dependencies gate running, not building.** A supervisor asks for its
    /// artifact the moment it is constructed — before any gate has been
    /// published, and without one at all here — because that is what lets one
    /// `bazel build` cover the whole workspace. Asking at gate-open would
    /// serialise every build along the dependency chain.
    ///
    /// A lazy service is the one exception, and for the reason that makes the
    /// rule: nothing wants it yet. It asks on its first connection.
    #[tokio::test]
    async fn an_artifact_is_asked_for_at_construction_but_lazily_on_demand() {
        struct Case {
            name: &'static str,
            lazy: bool,
            /// Whether a request is expected before any demand arrives.
            want_eager: bool,
        }
        let cases = [
            Case {
                name: "an ordinary bazel service builds immediately",
                lazy: false,
                want_eager: true,
            },
            Case {
                name: "a lazy bazel service waits for a connection",
                lazy: true,
                want_eager: false,
            },
        ];

        for case in cases {
            let (env, mut batcher_rx) = test_env_with_batcher().await;
            let (lazy_tx, demand_rx) = mpsc::channel(16);
            let proxy = bind_env_proxy(Some(lazy_tx.clone())).await;
            let (_tx, rx) = mpsc::unbounded_channel();
            let (report_tx, _report_rx) = mpsc::unbounded_channel();
            let handle = tokio::spawn(supervise(
                "svc".to_string(),
                rx,
                env,
                None,
                report_tx,
                Arc::new(AtomicBool::new(false)),
                Some(ProxyAssets {
                    proxy,
                    demand_rx: Some(demand_rx),
                }),
                Some(bazel_resolved(case.lazy)),
                // No gate at all: permission never arrives, and the request
                // must not be waiting on it.
                None,
            ));

            let eager = next_prepare(&mut batcher_rx).await;
            assert_eq!(eager.is_some(), case.want_eager, "{}", case.name);
            if let Some((item, _)) = &eager {
                assert_eq!(item.name, "svc", "{}", case.name);
                assert_eq!(
                    item.bazel.as_ref().map(|bazel| bazel.target.as_str()),
                    Some("//svc:svc"),
                    "{}",
                    case.name
                );
            }

            if !case.want_eager {
                lazy_tx.send("svc".to_string()).await.unwrap();
                assert!(
                    next_prepare(&mut batcher_rx).await.is_some(),
                    "{}: a first connection must ask for the artifact",
                    case.name
                );
            }
            handle.abort();
        }
    }

    /// An artifact is as much a precondition as a dependency, and it is the
    /// supervisor's to obtain — so an open gate does not start a service whose
    /// build is still running. That hold is also what puts the watch paths the
    /// build resolves in place before the first spawn: the build manager
    /// registers them with the watcher before it reports an outcome, and this
    /// supervisor does not move until that outcome arrives.
    #[tokio::test]
    async fn an_open_gate_does_not_start_a_service_still_waiting_on_its_build() {
        let (env, mut batcher_rx) = test_env_with_batcher().await;
        let names = ["svc".to_string()];
        let (mut gate_writer, mut gate_readers) = crate::gate::channel(names.iter());
        gate_writer.arm();
        gate_writer.begin_pass();
        gate_writer.set("svc", crate::gate::Gate::Open);

        let (_tx, rx) = mpsc::unbounded_channel();
        let (report_tx, mut report_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(supervise(
            "svc".to_string(),
            rx,
            env,
            None,
            report_tx,
            Arc::new(AtomicBool::new(false)),
            None,
            Some(bazel_resolved(false)),
            gate_readers.remove("svc"),
        ));

        assert!(
            matches!(
                report_rx.recv().await,
                Some(super::super::ProcessReport::ArtifactBuild {
                    status: super::super::ArtifactBuildStatus::Started,
                    ..
                })
            ),
            "a build must be announced before anything else happens"
        );
        let (_, outcome) = next_prepare(&mut batcher_rx)
            .await
            .expect("a bazel service must ask for its artifact");

        // The gate is Open and demand is standing, yet nothing may spawn.
        assert!(
            tokio::time::timeout(Duration::from_secs(2), report_rx.recv())
                .await
                .is_err(),
            "an open gate must not start a service whose artifact does not exist yet"
        );

        outcome
            .send(PrepareOutcome::Ready { binary_path: None })
            .unwrap();

        assert!(
            matches!(
                report_rx.recv().await,
                Some(super::super::ProcessReport::ArtifactBuild {
                    status: super::super::ArtifactBuildStatus::Ready,
                    ..
                })
            ),
            "expected the artifact to be reported ready"
        );
        match tokio::time::timeout(Duration::from_secs(5), report_rx.recv()).await {
            Ok(Some(super::super::ProcessReport::ServiceStarting { name, .. })) => {
                assert_eq!(name, "svc");
            }
            _ => panic!("the start should follow the artifact, in that order"),
        }
        handle.abort();
    }

    /// The rebuild cycle's decision table, which is the whole of the
    /// staleness machinery now that one owner runs build, stop and spawn.
    ///
    /// These carry the semantics of two runner tests that drove the old
    /// fold directly (`stale_build_then_up_to_date_followup_still_restarts`
    /// and `deferred_restart_survives_watch_retrigger`). The asymmetry
    /// between the arms is the point and is easy to break:
    /// `UpToDate` consults `artifact_ahead` but not staleness — nothing was
    /// built, so nothing went stale — while `Built` consults staleness and is
    /// the only thing that *sets* `artifact_ahead`.
    #[tokio::test]
    async fn the_rebuild_cycle_decides_when_to_restart() {
        use crate::build_tool::batcher::RebuildItemOutcome as Item;

        struct Case {
            name: &'static str,
            /// Outcomes applied in order; `stale` marks the cycle stale
            /// before that step, as a `MarkStale` mid-cycle would.
            steps: Vec<(Item, bool)>,
            want_restart: bool,
            want_artifact_ahead: bool,
        }

        let cases = vec![
            Case {
                name: "a fresh build restarts into it",
                steps: vec![(Item::Built, false)],
                want_restart: true,
                want_artifact_ahead: false,
            },
            Case {
                name: "up to date with nothing pending is a no-op",
                steps: vec![(Item::UpToDate, false)],
                want_restart: false,
                want_artifact_ahead: false,
            },
            Case {
                name: "a build that went stale defers its restart",
                steps: vec![(Item::Built, true)],
                want_restart: false,
                want_artifact_ahead: true,
            },
            Case {
                // The pin: up-to-date is measured against the last *build*,
                // not the running process, so the follow-up must still
                // restart even though the build tool had nothing to do.
                name: "a stale build then up-to-date still restarts",
                steps: vec![(Item::Built, true), (Item::UpToDate, false)],
                want_restart: true,
                want_artifact_ahead: true,
            },
            Case {
                // A re-trigger must not lose the deferred restart: a new
                // cycle clears staleness but never `artifact_ahead`.
                name: "a deferred restart survives a re-trigger",
                steps: vec![
                    (Item::Built, true),
                    (Item::Built, true),
                    (Item::UpToDate, false),
                ],
                want_restart: true,
                want_artifact_ahead: true,
            },
            Case {
                name: "a failed build keeps the old process",
                steps: vec![(Item::Failed("boom".to_string()), false)],
                want_restart: false,
                want_artifact_ahead: false,
            },
            Case {
                name: "a pass-through build restarts",
                steps: vec![(Item::NotBuilt, false)],
                want_restart: true,
                want_artifact_ahead: false,
            },
            Case {
                name: "a stale pass-through does not",
                steps: vec![(Item::NotBuilt, true)],
                want_restart: false,
                want_artifact_ahead: false,
            },
        ];

        for case in cases {
            let env = test_env().await;
            let (report_tx, _report_rx) = mpsc::unbounded_channel();
            let mut artifact_ahead = false;
            let mut batch_built = false;
            let mut restarted = false;
            for (outcome, stale) in case.steps {
                // Each step is its own cycle, as a fresh Rebuild would be.
                let mut cycle = Some(CycleState { stale });
                restarted = matches!(
                    settle_cycle(
                        "svc",
                        &env,
                        outcome,
                        &mut cycle,
                        &mut artifact_ahead,
                        &report_tx,
                        &mut batch_built,
                    ),
                    CycleNext::Restart
                );
                // A restart that happens brings the process up to date.
                if restarted {
                    artifact_ahead = false;
                }
            }
            assert_eq!(restarted, case.want_restart, "{}: restart", case.name);
            assert_eq!(
                artifact_ahead && !restarted,
                case.want_artifact_ahead && !case.want_restart,
                "{}: artifact_ahead",
                case.name
            );
        }
    }

    /// `Restart` is one operation and its reports say so: the stop half
    /// lands first, then the start is announced — the transition pair a
    /// restart has always shown, now produced by one owner instead of a stop
    /// the scheduler followed up on.
    ///
    /// The requester's reply travels *through*, unanswered. Callers read a
    /// stop reply as "the scheduler has applied this" (`don stop` returning
    /// means the service is no longer a satisfied dependency), which only the
    /// fold can promise.
    #[tokio::test]
    async fn restart_reports_its_stop_then_announces_the_start() {
        struct Case {
            name: &'static str,
            clear_backend_first: bool,
            with_reply: bool,
        }
        let cases = [
            Case {
                name: "plain restart",
                clear_backend_first: false,
                with_reply: true,
            },
            Case {
                name: "restart clearing the proxy backend first",
                clear_backend_first: true,
                with_reply: true,
            },
            Case {
                name: "restart nobody is waiting on",
                clear_backend_first: false,
                with_reply: false,
            },
        ];

        for case in cases {
            let mut harness = spawn_harness(None).await;
            let (reply_tx, mut reply_rx) = tokio::sync::oneshot::channel();
            harness
                .tx
                .send(ServiceCommand::Restart(Box::new(RestartRequest {
                    config: ShutdownConfig::default(),
                    wait_full_exit: false,
                    interrupt: None,
                    clear_backend_first: case.clear_backend_first,
                    start_mode: ServiceStartMode::Full,
                    fresh_backend_ports: false,
                    intent: ServiceStartIntent::Background,
                    reply: case.with_reply.then_some(reply_tx),
                    announce_restarting: false,
                    reset_policy: true,
                })))
                .unwrap();

            let carried = match harness.report_rx.recv().await {
                Some(super::super::ProcessReport::ServiceStopComplete {
                    name,
                    result,
                    reply,
                }) => {
                    assert_eq!(name, "svc", "{}", case.name);
                    // Nothing is held, so stopping succeeds trivially.
                    assert!(result.is_ok(), "{}: {result:?}", case.name);
                    assert_eq!(reply.is_some(), case.with_reply, "{}", case.name);
                    reply
                }
                _ => panic!("{}: expected the stop half first", case.name),
            };
            if case.with_reply {
                // Unanswered while its sender is still alive — proof the
                // supervisor handed it on rather than resolving it.
                assert!(
                    matches!(
                        reply_rx.try_recv(),
                        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                    ),
                    "{}: the supervisor answered a reply the fold owes",
                    case.name
                );
            }
            drop(carried);

            match harness.report_rx.recv().await {
                Some(super::super::ProcessReport::ServiceStarting { name, .. }) => {
                    assert_eq!(name, "svc", "{}", case.name);
                }
                _ => panic!("{}: expected the start to be announced", case.name),
            }
            harness.handle.abort();
        }
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
                let backend_port: u16 = proxy.env_vars().get("PORT").unwrap().parse().unwrap();
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
            super::super::ProcessReport::Demand { name, .. } => assert_eq!(name, "svc"),
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
