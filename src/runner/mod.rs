//! Runner — the orchestrator that starts services and tasks in dependency order.
//!
//! The runner builds an execution plan via topological sort, then starts
//! everything whose dependencies are satisfied concurrently using tokio tasks.
//! It owns all service/task state in a plain `HashMap` — no `Arc<Mutex<>>`.
//! Communication uses channels: `mpsc` for commands in, `broadcast` for events out.

mod events;
mod graph;
mod lazy;
mod rebuild;
mod runtime_ports;
mod service_commands;
mod service_health;
mod service_ready;
mod setup;
mod shutdown;
mod startup;
mod state;
pub(crate) mod status;
mod support;
mod task_commands;
mod watch_link;

// The per-process mechanism — supervisors, spawn/stop workers, health monitor,
// ready resolution — lives in `crate::process` and imports nothing from here.
// These aliases keep the runner's internal paths stable and the shared state
// vocabulary on its public `don::runner::…` path.
pub use crate::command::{CommandError, CommandResult};

// The batch-build workers and the batcher actor live in `crate::build_tool`;
// the state projection and its `ProcessStatus` vocabulary at the crate root.
// Aliases keep the runner's internal paths and the public `don::runner::…`
// surface stable.
pub(in crate::runner) use crate::build_tool::{batch as build_tools, batcher as build_batcher};
pub use crate::param_completions::CompletionError;
pub(in crate::runner) use crate::process::{
    Demand, ServiceHandleIdentity, ServiceStartIntent, TaskExit, TaskRunIntent, paths,
    service_supervisor, service_worker, task_supervisor, task_worker,
};
pub(crate) use crate::process::{ProcessKind, ProcessReport};
pub use crate::process::{ServiceState, TaskState};
pub(in crate::runner) use crate::state_store;
pub use crate::state_store::{
    ParamInfo, ProcessStatus, StateReader, StateSnapshot, VerboseInfo, all_services_ready,
};
pub use crate::watch::report::{WatchDir, WatchReport, WatchReportItem};

pub(crate) use crate::process::params::resolve_task_params;

use crate::config::{Config, Platform, ShutdownConfig};
use crate::output::OutputManager;
use crate::proxy::ConnectionPolicy;
use crate::sys::pid_file::PidFile;
use crate::watch::WatchManager;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::time::SystemTime;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};

#[cfg(test)]
use self::build_tools::bazel_graph_requery_group_dir;
#[cfg(test)]
use self::graph::compute_depths;
use self::graph::topological_sort;
#[cfg(test)]
use self::paths::any_glob_path_changed_since;
use self::support::check_gitignore;
use crate::signals::shutdown_requested;

const UPDATE_CHECK_INTERVAL: Duration = Duration::from_secs(60 * 60);
const UPDATE_CHECK_TIMEOUT: Duration = Duration::from_secs(2);
/// Whether a service's proxy is closing connections rather than queuing them.
///
/// A service that failed *and* has no process left has nothing to queue for —
/// a client would hang on a socket nobody will read. Shared by the policy the
/// runner commands and the `status -v` line that explains it, so the two
/// cannot drift.
pub(in crate::runner) fn refusing_connections(state: ServiceState, live: bool) -> bool {
    matches!(state, ServiceState::Failed | ServiceState::DependencyFailed) && !live
}

/// Whether a build-graph re-query should trigger a rebuild for this service.
///
/// Only a service that is actually up: a lazy service nobody has connected to
/// yet sits in `Lazy`, and cold-starting it because a BUILD file moved would
/// build — and run — something the user never asked for.
fn should_rebuild_after_graph_requery(service: &RuntimeService) -> bool {
    matches!(
        service.state(),
        ServiceState::Running | ServiceState::Ready | ServiceState::Unhealthy
    )
}

/// A command sent to the runner via its public `mpsc` channel.
///
/// **Admission test for new variants:** a command belongs here only if its
/// handler mutates scheduling state or must serialize with the fold. Pure
/// reads go through a projection ([`StateReader`], [`crate::output::LogReader`],
/// [`crate::watch::report::WatchStatusReader`]); mechanism that only needs
/// fixed-at-construction config belongs to whoever calls it (see
/// [`crate::param_completions::CompletionResolver`]); and the runner's own
/// deferred ticks are internal, not commands.
pub enum RunnerCommand {
    /// Start a stopped service.
    Start {
        name: String,
        reply: oneshot::Sender<CommandResult>,
    },
    /// Stop a running service.
    Stop {
        name: String,
        reply: oneshot::Sender<CommandResult>,
    },
    /// Restart a service.
    Restart {
        name: String,
        reply: oneshot::Sender<CommandResult>,
    },
    /// Force a rebuild, then start or restart a service.
    HardRestart {
        name: String,
        reply: oneshot::Sender<CommandResult>,
    },
    /// Rebuild a service triggered by a file watch event.
    /// Runs the build command (if any), then restarts the service.
    Rebuild { name: String },
    /// A watched file changed during the current rebuild cycle for a service.
    /// The active build should finish, but any pending restart should be
    /// skipped because the build output is already stale.
    RebuildStale { name: String },
    /// Re-run a task triggered by a file watch event.
    TaskRerun { name: String },
    /// Query the status of all services and tasks. When `name` is `Some`, only
    /// that process is returned, with its full resolved watch path list included.
    Status {
        verbose: bool,
        name: Option<String>,
        reply: oneshot::Sender<Vec<ProcessStatus>>,
    },
    /// Run all tasks currently in PendingRun state.
    RunPendingTasks {
        reply: oneshot::Sender<CommandResult>,
    },
    /// Run a specific task by name, bypassing the `auto_run` gate. Used by
    /// `don run <name>` and the TUI action palette. `params` carries the
    /// user-supplied values for the task's declared params — empty for
    /// tasks that don't declare any. When `wait` is true, the reply is held
    /// until the task process exits.
    RunTask {
        name: String,
        params: HashMap<String, String>,
        wait: bool,
        wait_timeout: Option<String>,
        reply: oneshot::Sender<CommandResult>,
    },
    /// Initiate graceful shutdown.
    Shutdown,
}

/// An event broadcast from the runner for external consumers.
///
/// Serialized as an internally-tagged JSON object (`{"type": "...", ...}`) so
/// the unix-socket API can stream state changes to the web UI and any other
/// consumer over `GET /events`. Variant names are snake_cased on the wire.
///
/// `Deserialize` because the TUI consumes this stream *as a client* — the
/// wire format is the one representation, so client and server cannot drift.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunnerEvent {
    /// A service changed state.
    ServiceStateChanged {
        name: String,
        state: ServiceState,
        pid: Option<i32>,
        /// Root service/task failures when `state` is `DependencyFailed`.
        failed_dependencies: Vec<String>,
    },
    /// A task changed state.
    TaskStateChanged {
        name: String,
        state: TaskState,
        last_run: Option<crate::task_state::TaskRunInfo>,
        /// Root service/task failures when `state` is `DependencyFailed`.
        failed_dependencies: Vec<String>,
    },
    /// A rebuild cycle completed (file watch triggered).
    RebuildComplete { name: String, success: bool },
    /// A task re-run completed (file watch triggered).
    TaskRerunComplete { name: String, success: bool },
    /// The initial startup sweep has decided every process — nothing is left
    /// merely being *considered*. Fires once per run.
    StartupSettled,
    /// Graceful shutdown has started.
    ShutdownStarted,
    /// Shutdown complete.
    ShutdownComplete,
    /// The latest crates.io version changed, or no newer version is available.
    UpdateCheck {
        current_version: String,
        latest_version: Option<String>,
    },
}

/// Errors from runner operations.
#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("dependency cycle detected: {}", cycle.join(" -> "))]
    Cycle { cycle: Vec<String> },
    #[error("another don instance is already running (could not acquire {path})")]
    AlreadyRunning { path: String },
    #[error("process error: {0}")]
    Process(#[from] crate::sys::ProcessError),
    #[error("output error: {0}")]
    Output(#[from] crate::output::OutputError),
    #[error("pid file error: {0}")]
    PidFile(#[from] crate::sys::pid_file::PidFileError),
    #[error("config error: {0}")]
    Config(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub(crate) use state::{RuntimeService, RuntimeTask};

/// The main runner that orchestrates services and tasks.
pub struct Runner {
    config: Config,
    platform: Platform,
    output_manager: OutputManager,
    base_dir: PathBuf,

    /// Consolidated per-service runtime state.
    services: HashMap<String, RuntimeService>,
    /// Consolidated per-task runtime state.
    tasks: HashMap<String, RuntimeTask>,

    /// The receiving half of the processes' lossless report channel — see
    /// [`ProcessReport`]. The runner keeps no sender: every report now
    /// originates in a supervisor, so the senders live with them and the
    /// channel closes exactly when the last one ends.
    report_rx: mpsc::UnboundedReceiver<ProcessReport>,

    /// Signals the API server task to stop accepting connections.
    server_shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,

    /// Docker API client. `Some` if any service uses the docker preset.
    // Channels
    cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
    cmd_rx: mpsc::UnboundedReceiver<RunnerCommand>,
    /// Results from the detached crates.io update checker — the runner's one
    /// remaining detached worker now that building belongs to the build
    /// manager.
    update_tx: mpsc::Sender<Option<crate::update::UpdateAvailable>>,
    update_rx: mpsc::Receiver<Option<crate::update::UpdateAvailable>>,
    event_tx: broadcast::Sender<RunnerEvent>,

    /// The write half of the globally-readable state projection.
    ///
    /// Republished on every state transition, so other components can read
    /// process state — and whether the initial startup sweep has settled —
    /// without a command round trip. Not `Clone`: the runner is the only
    /// writer, and [`state_store`] enforces that by ownership rather than by
    /// convention.
    state: state_store::StateWriter,
    /// Per-process permission to run — the scheduler's whole output. See
    /// [`crate::gate`].
    start_gates: crate::gate::GateWriter,
    /// A start-pending sweep is due. Set by fold transitions (see
    /// `schedule_gate_recompute`), consumed at the top of the main loop —
    /// the runner's own deferred tick, invisible to clients by construction.
    gate_recompute_scheduled: bool,

    // Shutdown signal receiver — wakes the select loop when Ctrl+C is pressed.
    // `Option` because `run()` takes it out at the top to consume in the
    // main `select!`. It's never `None` after construction until `run()`
    // consumes it.
    shutdown_rx: Option<mpsc::Receiver<()>>,

    /// Detached periodic crates.io update checker.
    update_check_handle: Option<tokio::task::JoinHandle<()>>,

    // Don's own PID file
    _don_pid_file: Option<PidFile>,

    /// The link to a running file watcher: revised watch patterns out,
    /// status queries out.
    ///
    /// `None` until one is running, and stays `None` when the config gives it
    /// nothing to watch — so this answers "is there a watcher?" rather than
    /// "has startup got that far?".
    watch: Option<watch_link::WatchHandle>,

    /// One start supervisor per service, plus the registry addressing them.
    ///
    /// Same split as `task_supervisors`: the registry half is clone-able and
    /// send-only, ending a supervisor stays here.
    service_starts: service_supervisor::ServiceStarts,

    /// One run supervisor per task, plus the registry that addresses them.
    ///
    /// Each supervisor owns its task's run preparation and is the only thing
    /// that reports a prepared run, so a superseded one can't reach the
    /// runner at all. The registry half is clone-able and send-only; ending a
    /// supervisor stays here.
    task_supervisors: task_supervisor::TaskSupervisors,

    /// Mailbox of the build-batcher actor, which owns rebuild/re-query
    /// coalescing end to end (see [`build_batcher`]). The runner still
    /// decides *what* a rebuild means — outcomes come back on
    /// [`Self::batch_outcome_rx`] and are applied here.
    batcher_tx: tokio::sync::mpsc::UnboundedSender<build_batcher::BatchRequest>,
    /// Finished batches from the batcher actor, folded in the run loop.
    batch_outcome_rx: tokio::sync::mpsc::UnboundedReceiver<build_batcher::BatchOutcome>,
    /// The batcher task itself; joined (bounded) during shutdown.
    batcher_handle: Option<tokio::task::JoinHandle<()>>,

    /// Task-param completion resolver, held only to be handed to the API
    /// server at bind time — the runner itself never resolves completions.
    completions: crate::param_completions::CompletionResolver,

    /// Name -> kind facts, so a client's 404 never wakes the scheduler.
    catalog: std::sync::Arc<crate::control::ProcessCatalog>,
    /// Where every service can be reached, published for supervisors that
    /// render their own `$(peer.KEY)` env references. See
    /// [`crate::endpoints`].
    endpoints: crate::endpoints::EndpointWriter,

    /// Publisher for the watch manager's query sender; the paired
    /// [`crate::watch::report::WatchStatusReader`] goes to the API server.
    /// Outer `None` until watch setup decides; then `Some(None)` (nothing
    /// to watch) or `Some(Some(sender))`.
    watch_status_tx:
        tokio::sync::watch::Sender<Option<Option<mpsc::Sender<crate::watch::WatchQuery>>>>,
    /// The reader half, cloned out via [`Self::watch_status_reader`].
    watch_status_reader: crate::watch::report::WatchStatusReader,

    /// Internal shutdown flag broadcast to detached control workers so they
    /// can force-kill promptly when don is exiting.
    shutdown_flag_tx: tokio::sync::watch::Sender<bool>,

    /// True after graceful shutdown starts. Used to reject late starts and
    /// to keep final shutdown output ordered after all cleanup work.
    shutting_down: bool,

    /// Sends manifest snapshots / removals to the serialized writer task that
    /// owns `.don/ports.json` filesystem I/O. `None` after shutdown flush.
    manifest_writer_tx: Option<mpsc::UnboundedSender<runtime_ports::ManifestWrite>>,
    /// Live forward-connection counters, by service. Captured once at bind;
    /// `don status -v` reads them.
    proxy_connection_counters: HashMap<String, Arc<AtomicUsize>>,
    /// Join handle for the manifest-writer task, awaited on shutdown so the
    /// final removal is observable once the runner stops.
    manifest_writer_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Runner {
    /// Create a new runner from a validated config.
    ///
    /// `base_dir` is the project root (where `don.toml` lives).
    /// The runner acquires don's PID file at `<base_dir>/.don/don.pid`.
    pub async fn new(
        config: Config,
        platform: Platform,
        output_manager: OutputManager,
        base_dir: PathBuf,
        profile: Option<&str>,
        shutdown_rx: mpsc::Receiver<()>,
        headless: bool,
    ) -> Result<Self, RunnerError> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (update_tx, update_rx) = mpsc::channel(64);
        let (event_tx, _) = broadcast::channel(256);
        let (state, state_reader) = state_store::channel(state_store::StateSnapshot::default());

        let (report_tx, report_rx) = mpsc::unbounded_channel();
        let (shutdown_flag_tx, _shutdown_flag_rx) = tokio::sync::watch::channel(false);

        for outcome in crate::sys::rlimit::raise_soft_resource_limits() {
            if let Some(message) = crate::sys::rlimit::format_outcome(&outcome) {
                output_manager.service_debug_event("don", &message);
            }
        }

        let base_dir = setup::canonicalize_base_dir(&base_dir)?;
        // Resolved once: every supervisor's build spec and the build manager's
        // own build-graph registrations use the same project-wide list.
        let global_watch_ignore = crate::process::paths::resolve_watch_ignore_patterns(
            &base_dir,
            &[],
            &base_dir,
            &config.watch_ignore,
        );
        let (batcher_tx, batch_outcome_rx, batcher_handle) = build_batcher::spawn(
            state_reader,
            output_manager.clone_lifecycle_emitter(),
            build_batcher::WorkspaceContext {
                base_dir: base_dir.clone(),
                global_watch_ignore: global_watch_ignore.clone(),
            },
        );
        let (watch_status_tx, watch_status_reader) = crate::watch::report::status_channel();
        let completions = crate::param_completions::CompletionResolver::new(
            config.tasks.clone(),
            base_dir.clone(),
        );
        let don_dir = setup::ensure_don_dir(&base_dir)?;
        let don_pid_file = setup::acquire_don_pid_file(&don_dir).await?;

        setup::cleanup_stale_state(&config, platform, &base_dir, &output_manager).await;
        if let Err(error) = crate::ports::remove_manifest(&base_dir) {
            output_manager.error_event(&format!("failed to remove stale runtime ports: {error}"));
        }
        let docker_client = setup::connect_docker_if_needed(&config, platform)?;

        let active_processes = setup::resolve_active_processes(&config, platform, profile)?;
        let active_services = setup::filter_active_services(&config, active_processes.as_ref());
        let active_tasks = setup::filter_active_tasks(&config, active_processes.as_ref());

        setup::prune_download_cache(&config, platform, &don_dir, &output_manager);

        let (mut services, tasks) = setup::build_runtime_maps(
            &config,
            platform,
            &base_dir,
            &active_services,
            &active_tasks,
            headless,
        )
        .await;

        // Bind every proxy before any supervisor exists, so a port conflict
        // fails startup before anything spawns — "validate everything before
        // starting anything". Each supervisor takes ownership of its bound
        // proxy at spawn below; the runner keeps only the view. Lazy services
        // get a per-service trigger channel whose receiving half rides along,
        // and the supervisor forwards each trigger as a demand report.
        let catalog = std::sync::Arc::new(crate::control::ProcessCatalog::new(
            &config,
            services.keys().cloned().collect(),
            tasks.keys().cloned().collect(),
        ));

        // One gate per process, created before any supervisor so each can be
        // handed its own reader at spawn.
        let gate_names: Vec<String> = services.keys().chain(tasks.keys()).cloned().collect();
        let (start_gates, mut gate_readers) = crate::gate::channel(gate_names.iter());

        // Publish endpoints before binding: the key set decides which
        // `$(name.key)` tokens count as runtime references at all, and proxy
        // bindings land below — before any supervisor exists, which is what
        // lets a service resolve a peer that has not started yet.
        let (endpoints, endpoints_reader) = crate::endpoints::channel();
        endpoints.seed(services.keys().cloned());

        let mut proxies: HashMap<String, service_supervisor::ProxyAssets> = HashMap::new();
        // Handles to the proxies' live connection counters. Immutable after
        // construction and shared with the accept loops, so this is a way to
        // read one number rather than a copy of it.
        let mut proxy_connection_counters: HashMap<String, Arc<AtomicUsize>> = HashMap::new();
        for (name, rs) in services.iter_mut() {
            if rs.resolved.proxy.is_empty() {
                continue;
            }
            let (lazy_tx, demand_rx) = if rs.resolved.lazy {
                let (tx, rx) = mpsc::channel(16);
                (Some(tx), Some(rx))
            } else {
                (None, None)
            };
            match crate::proxy::ServiceProxy::bind(
                &rs.resolved.proxy,
                config.fallback_ports,
                lazy_tx,
                name,
                output_manager.clone_lifecycle_emitter(),
            )
            .await
            {
                Ok(proxy) => {
                    for message in proxy.fallback_descriptions() {
                        output_manager.service_event(name, &message);
                    }
                    let addrs: Vec<String> =
                        proxy.listen_addrs().iter().map(|a| a.to_string()).collect();
                    output_manager.service_debug_event(
                        name,
                        &format!("proxy listening on {}", addrs.join(", ")),
                    );
                    let view = proxy.view();
                    endpoints.publish_proxy(name, view.bindings.clone());
                    if let Some(counter) = view.connection_counter() {
                        proxy_connection_counters.insert(name.clone(), counter);
                    }
                    let _ = rs;
                    proxies.insert(
                        name.clone(),
                        service_supervisor::ProxyAssets { proxy, demand_rx },
                    );
                }
                Err(e) => {
                    return Err(RunnerError::Config(format!("{name}: {e}")));
                }
            }
        }

        // One supervisor per service, likewise immutable once built.
        let service_starts = service_supervisor::spawn_supervisors(
            services.keys(),
            &service_supervisor::StartEnv {
                batcher_tx: batcher_tx.clone(),
                base_dir: base_dir.clone(),
                pid_dir: base_dir.join(".don").join("pids"),
                platform,
                docker_client: docker_client.clone(),
                emitter: output_manager.clone_lifecycle_emitter(),
                shutdown: config.shutdown.clone(),
                fallback_ports: config.fallback_ports,
                endpoints: endpoints_reader,
                shutdown_rx: shutdown_flag_tx.subscribe(),
                global_watch_ignore: global_watch_ignore.clone(),
            },
            &|name| output_manager.process_output(name),
            &|name| services.get(name).map(|rs| rs.resolved.clone()),
            &report_tx,
            &mut proxies,
            &mut gate_readers,
        );

        // One supervisor per task, started before the runner exists so the
        // registry is immutable and can be shared without a lock.
        // Which names some *blocking* dependent is waiting on. Computed once:
        // group refs are already expanded by `build_runtime_maps`, and the
        // bazel re-resolution later re-expands to the same set, so this
        // cannot go stale.
        let blocking_dependents: std::collections::HashSet<String> = services
            .values()
            .flat_map(|rs| rs.resolved.depends_on.iter())
            .chain(tasks.values().flat_map(|rt| rt.config.depends_on.iter()))
            .filter(|dep| dep.blocking)
            .map(|dep| dep.name.clone())
            .collect();

        let task_supervisors = task_supervisor::spawn_supervisors(
            tasks.keys(),
            &task_worker::TaskWorkerContext {
                base_dir: base_dir.clone(),
                platform,
                emitter: output_manager.clone_lifecycle_emitter(),
                global_watch_ignore: config.watch_ignore.clone(),
                endpoints: endpoints.reader(),
            },
            &|name| output_manager.process_output(name),
            &|name| {
                tasks.get(name).map(|rt| task_supervisor::StartupConfig {
                    task_cfg: Box::new(rt.config.clone()),
                    has_dependents: blocking_dependents.contains(name),
                })
            },
            &report_tx,
            &mut gate_readers,
            &shutdown_flag_tx.subscribe(),
            &batcher_tx,
        );

        let (manifest_writer_tx, manifest_writer_handle) = runtime_ports::spawn_manifest_writer(
            base_dir.clone(),
            output_manager.clone_lifecycle_emitter(),
        );

        let runner = Self {
            config,
            platform,
            output_manager,
            base_dir,
            services,
            tasks,
            report_rx,
            server_shutdown_tx: None,
            cmd_tx,
            cmd_rx,
            update_tx,
            update_rx,
            event_tx,
            state,
            start_gates,
            catalog,
            gate_recompute_scheduled: false,
            shutdown_rx: Some(shutdown_rx),
            _don_pid_file: Some(don_pid_file),
            watch: None,
            update_check_handle: None,
            service_starts,
            task_supervisors,
            batcher_tx,
            batch_outcome_rx,
            batcher_handle: Some(batcher_handle),
            completions,
            endpoints,
            watch_status_tx,
            watch_status_reader,
            shutdown_flag_tx,
            shutting_down: false,
            manifest_writer_tx: Some(manifest_writer_tx),
            proxy_connection_counters,
            manifest_writer_handle: Some(manifest_writer_handle),
        };
        // Seed the projection before anyone can read it. The process set is fixed
        // at construction (see `setup::build_runtime_maps`), so from here on
        // every change is a state transition and republishing on those alone
        // is complete.
        runner.publish_state();
        Ok(runner)
    }

    /// Republish the state projection. Called from the two state-change
    /// funnels below, so the snapshot updates on exactly the transitions that
    /// emit a [`RunnerEvent`] — a client can resync from one after missing the
    /// other and get a consistent answer.
    fn publish_state(&self) {
        self.state.publish_processes(self.status_projection(None));
    }

    /// Get a sender for sending commands to this runner.
    /// Transition a service to a new state and broadcast the change.
    ///
    /// The broadcast is the whole point — `RuntimeService::set_state` is
    /// `#[must_use]` precisely so the event can't be forgotten. No-op if
    /// the service is unknown or already at `new_state`.
    pub(crate) fn set_service_state(&mut self, name: &str, new_state: ServiceState) {
        let previous_state = self.services.get(name).map(RuntimeService::state);
        let changed = self
            .services
            .get_mut(name)
            .and_then(|rs| rs.set_state(new_state));
        if let Some(state) = changed {
            self.sync_proxy_policy(name);
            self.broadcast_service_state(name, state);
            // Every transition, not a chosen subset. A gate is derived
            // purely from peers' states now, so any state change can move
            // some dependent's level — and a level that goes stale is a
            // start that should not have happened. Recomputing is a cheap
            // pure pass that publishes nothing when nothing changed.
            self.schedule_gate_recompute();
            let _ = previous_state;
        }
    }

    /// Keep the proxy's connection policy in step with the service.
    ///
    /// Queuing connections is right while a service is starting or
    /// restarting — the client waits a moment and gets served. It is wrong
    /// once the service has failed *with no process left*: nothing is going
    /// to read that socket, so the connection is refused instead of left
    /// hanging.
    ///
    /// The liveness half matters. `Failed` does not imply "the process is
    /// gone": a service whose ready check fails keeps running under the
    /// default `on_failure = "notify"` and may well be serving traffic. Don
    /// must not close its clients' connections — and in listenfd mode must
    /// not race that live child for accepts. Only a service that has both
    /// failed and lost its process refuses.
    ///
    /// Call this after any change to a service's state *or* its process
    /// handle. It is idempotent.
    fn sync_proxy_policy(&mut self, name: &str) {
        let live = self.service_runtime(name).is_some();
        let Some(rs) = self.services.get(name) else {
            return;
        };
        let policy = if rs.state() == ServiceState::Lazy {
            ConnectionPolicy::LazyTrigger
        } else if refusing_connections(rs.state(), live) {
            ConnectionPolicy::Refuse
        } else {
            ConnectionPolicy::Serve
        };
        if rs.resolved.proxy.is_empty() {
            return;
        }
        // Sent unconditionally: the proxy answers whether this is a change,
        // and narrates the refusal edge. Deduping here would need a shadow of
        // a value the owner already has.
        self.send_proxy_directive(name, service_supervisor::ProxyDirective::SetPolicy(policy));
    }

    /// Hand a proxy decision to the service's supervisor, which owns the
    /// listeners. Fire-and-forget: a closed mailbox means teardown is ahead
    /// of us and the proxy is already gone.
    pub(in crate::runner) fn send_proxy_directive(
        &self,
        name: &str,
        directive: service_supervisor::ProxyDirective,
    ) {
        if let Some(handle) = self.service_starts.registry().get(name) {
            let _ = handle.request(service_supervisor::ServiceCommand::Proxy(directive));
        }
    }

    /// Atomically mark a service as dependency-failed and broadcast changes
    /// to either its lifecycle state or its root-cause detail.
    pub(crate) fn mark_service_dependency_failed(
        &mut self,
        name: &str,
        dependencies: Vec<String>,
    ) -> bool {
        let Some(rs) = self.services.get_mut(name) else {
            return false;
        };
        let state_changed = rs.state() != ServiceState::DependencyFailed;
        if !rs.mark_dependency_failed(dependencies) {
            return false;
        }
        self.sync_proxy_policy(name);
        self.broadcast_service_state(name, ServiceState::DependencyFailed);
        if state_changed {
            self.schedule_gate_recompute();
        }
        state_changed
    }

    fn broadcast_service_state(&self, name: &str, state: ServiceState) {
        let Some(rs) = self.services.get(name) else {
            return;
        };
        let failed_dependencies = rs.failed_dependencies().to_vec();
        self.publish_state();
        // The pid comes from the projection the custody funnels write, so the
        // event and the snapshot cannot disagree about what is running.
        let _ = self.event_tx.send(RunnerEvent::ServiceStateChanged {
            name: name.to_string(),
            state,
            pid: self.service_runtime(name).and_then(|runtime| runtime.pid),
            failed_dependencies,
        });
    }

    /// Transition a task to a new state and broadcast the change.
    pub(crate) fn set_task_state(&mut self, name: &str, new_state: TaskState) {
        let previous_state = self.tasks.get(name).map(RuntimeTask::state);
        let changed = self
            .tasks
            .get_mut(name)
            .and_then(|rt| rt.set_state(new_state));
        if let Some(state) = changed {
            self.broadcast_task_state(name, state);
            // Every transition — see `set_service_state`. A task in the
            // middle of a re-run stops satisfying its dependents, and that
            // must reach their gates before anything acts on a stale level.
            self.schedule_gate_recompute();
            let _ = previous_state;
        }
    }

    /// Atomically mark a task as dependency-failed and broadcast changes to
    /// either its lifecycle state or its root-cause detail.
    pub(crate) fn mark_task_dependency_failed(
        &mut self,
        name: &str,
        dependencies: Vec<String>,
    ) -> bool {
        let Some(rt) = self.tasks.get_mut(name) else {
            return false;
        };
        let state_changed = rt.state() != TaskState::DependencyFailed;
        if !rt.mark_dependency_failed(dependencies) {
            return false;
        }
        self.broadcast_task_state(name, TaskState::DependencyFailed);
        if state_changed {
            self.schedule_gate_recompute();
        }
        state_changed
    }

    fn broadcast_task_state(&self, name: &str, state: TaskState) {
        let Some(rt) = self.tasks.get(name) else {
            return;
        };
        self.publish_state();
        let _ = self.event_tx.send(RunnerEvent::TaskStateChanged {
            name: name.to_string(),
            state,
            last_run: rt.last_run.clone(),
            failed_dependencies: rt.failed_dependencies().to_vec(),
        });
    }

    pub fn command_sender(&self) -> mpsc::UnboundedSender<RunnerCommand> {
        self.cmd_tx.clone()
    }

    pub(crate) fn effective_shutdown_config(&self, name: &str) -> ShutdownConfig {
        self.services
            .get(name)
            .and_then(|rs| rs.resolved.shutdown.clone())
            .map(|shutdown| shutdown.merged_over(&self.config.shutdown))
            .unwrap_or_else(|| self.config.shutdown.clone())
    }

    /// Subscribe to runner events.
    pub fn subscribe(&self) -> broadcast::Receiver<RunnerEvent> {
        self.event_tx.subscribe()
    }

    /// The canonical project root this runner manages.
    ///
    /// Canonical, not as-passed: [`Runner::new`] resolves symlinks and `..`
    /// before storing it, and the daemon derives a project's identity by
    /// hashing this path. A caller registering the project must use this
    /// value rather than the path it handed in, or it will register under one
    /// id and deregister under another.
    pub fn base_dir(&self) -> &std::path::Path {
        &self.base_dir
    }

    /// A cloneable emitter for this runner's lifecycle output.
    pub fn lifecycle_emitter(&self) -> crate::output::LifecycleEmitter {
        self.output_manager.clone_lifecycle_emitter()
    }

    /// The merged-log-stream handle (subscriptions + bounded history), for
    /// a server that hands out a follow per client. See
    /// [`crate::output::OutputManager::log_stream_sender`].
    pub fn log_stream_sender(&self) -> crate::output::MergedLogTap {
        self.output_manager.log_stream_sender()
    }

    /// A cloneable read-only handle to every process's buffered output; see
    /// [`crate::output::LogReader`].
    pub fn log_reader(&self) -> crate::output::LogReader {
        self.output_manager.log_reader()
    }

    /// The task-param completion resolver; see
    /// [`crate::param_completions::CompletionResolver`].
    pub fn completion_resolver(&self) -> crate::param_completions::CompletionResolver {
        self.completions.clone()
    }

    /// A read-only handle for the global watch report; see
    /// [`crate::watch::report::WatchStatusReader`].
    pub fn watch_status_reader(&self) -> crate::watch::report::WatchStatusReader {
        self.watch_status_reader.clone()
    }

    /// The control plane for clients: see [`crate::control::ProcessControl`].
    pub fn process_control(&self) -> crate::control::ProcessControl {
        crate::control::ProcessControl::new(self.catalog.clone(), self.cmd_tx.clone())
    }

    /// Mint the attach handle for the API server; see
    /// [`crate::output::attach::AttachControl`].
    pub fn attach_control(&self) -> crate::output::attach::AttachControl {
        self.output_manager.attach_control()
    }

    /// Handle to the server-side terminal-emulator thread, for the API
    /// server's attach-resize path.
    pub(crate) fn emulator_handle(&self) -> crate::output::emulator::EmulatorHandle {
        self.output_manager.emulator_handle()
    }

    /// The event sender, for a server that hands out a subscription per
    /// connection rather than holding one.
    pub fn subscribe_sender(&self) -> broadcast::Sender<RunnerEvent> {
        self.event_tx.clone()
    }

    /// Hand the runner the signal that stops the API accepting connections.
    ///
    /// Call after [`crate::server::serve_for_runner`]. Without it the API
    /// keeps accepting until the process exits, which only matters for
    /// embedders that serve one.
    pub fn set_api_shutdown(&mut self, shutdown_tx: tokio::sync::watch::Sender<bool>) {
        self.server_shutdown_tx = Some(shutdown_tx);
    }

    /// A read-only view of every process's state, updated on each transition.
    ///
    /// Reads never queue behind the runner's command loop, so this is the
    /// right way to answer "what is running?" — reserve the [`Status`] command
    /// for the verbose view, which needs work the projection deliberately
    /// leaves out.
    ///
    /// [`Status`]: RunnerCommand::Status
    pub fn state_reader(&self) -> StateReader {
        self.state.reader()
    }

    fn start_update_checker(&mut self) {
        if std::env::var_os("DON_NO_UPDATE_CHECK").is_some() {
            return;
        }

        let update_tx = self.update_tx.clone();
        let mut shutdown_rx = self.shutdown_flag_tx.subscribe();
        self.update_check_handle = Some(tokio::spawn(async move {
            loop {
                let check = crate::update::check_crates_io(
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_PKG_VERSION"),
                    UPDATE_CHECK_TIMEOUT,
                );
                tokio::select! {
                    result = check => {
                        if let Ok(update) = result
                            && update_tx.send(update).await.is_err()
                        {
                            break;
                        }
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }

                tokio::select! {
                    _ = tokio::time::sleep(UPDATE_CHECK_INTERVAL) => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        }));
    }

    fn broadcast_update_check(&self, update: Option<crate::update::UpdateAvailable>) {
        let latest_version = update.as_ref().map(|u| u.latest_version.clone());
        let current_version = update
            .map(|u| u.current_version)
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        let _ = self.event_tx.send(RunnerEvent::UpdateCheck {
            current_version,
            latest_version,
        });
    }

    /// Run the orchestrator: start all services and tasks in dependency order.
    ///
    /// This is the main entry point. It:
    /// 1. Builds a topological sort of the dependency graph.
    /// 2. Starts processes in parallel as their dependencies become satisfied.
    /// 3. Processes commands from the mpsc channel.
    /// 4. Handles shutdown signals.
    pub async fn run(mut self) -> Result<(), RunnerError> {
        // Warn if .don/ is not in .gitignore.
        check_gitignore(&self.base_dir, &self.output_manager);

        // Take ownership of the shutdown receiver up front so the slow
        // startup phase (build-tool resolution + batch builds) can `select!`
        // on it without conflicting with `&mut self` borrows. Always `Some`
        // here — the field is set by `Runner::new` and only consumed here.
        let mut shutdown_rx = match self.shutdown_rx.take() {
            Some(rx) => rx,
            None => return Ok(()),
        };

        self.start_update_checker();

        self.output_manager.lifecycle_event("loading don.toml");

        let svc_count = self.services.len();
        let task_count = self.tasks.len();

        self.output_manager.lifecycle_event(&format!(
            "validated {} service{}, {} task{}",
            svc_count,
            if svc_count == 1 { "" } else { "s" },
            task_count,
            if task_count == 1 { "" } else { "s" },
        ));

        // Register the synthetic "bazel" stream so build-tool output
        // gets a color-coded prefix column like real services, instead of
        // riding on `[don]` lifecycle events with a `bazel:` text prefix.
        let has_bazel = self
            .services
            .values()
            .any(|rs| rs.resolved.bazel_config().is_some())
            || self.config.tasks.values().any(|t| t.bazel.is_some());
        if has_bazel {
            self.output_manager.register_build_tool("bazel").await;
        }

        // The proxies were bound during construction (fail-fast) and belong
        // to the supervisors. Set lazy services to `Lazy` here — they won't
        // enter the startup flow until a connection demands them — and
        // publish the initial ports manifest.
        let lazy_names: Vec<String> = self
            .services
            .iter()
            .filter(|(_, rs)| rs.resolved.lazy && !rs.resolved.proxy.is_empty())
            .map(|(name, _)| name.clone())
            .collect();
        for name in lazy_names {
            self.set_service_state(&name, ServiceState::Lazy);
        }
        self.refresh_runtime_port_manifest();

        // Start file watchers before spawning services so we don't miss
        // changes that happen during startup (slow ready checks, long builds, etc.).
        let mut watch_handle: Option<tokio::task::JoinHandle<()>> = None;
        let (watch_update_tx, watch_update_rx) = mpsc::unbounded_channel();
        let (watch_query_tx, watch_query_rx) = mpsc::channel(8);
        // `WatchManager::new` calls `notify::Watcher::watch`, which is
        // synchronous and walks directory trees under the hood — offload
        // to a blocking thread so the runner's main task stays polled.
        // Race it against `shutdown_rx` so Ctrl+C during watch setup
        // shuts down cleanly even if setup ever gets slow again.
        let config_for_watch = self.config.clone();
        let platform_for_watch = self.platform;
        let base_dir_for_watch = self.base_dir.clone();
        // The watcher speaks its own vocabulary; `watch_link` adapts it to the
        // runner's, so `watch` needs no runner types. Subscribe before setup
        // so no completion emitted during it is missed.
        let (watch_signal_tx, watch_signal_rx) = mpsc::unbounded_channel();
        let (watch_outcome_tx, watch_outcome_rx) = mpsc::unbounded_channel();
        let watch_link_handle = watch_link::spawn(
            watch_signal_rx,
            self.cmd_tx.clone(),
            self.batcher_tx.clone(),
            self.requery_catalog(),
            self.event_tx.subscribe(),
            watch_outcome_tx,
        );
        let emitter_for_watch = self.output_manager.clone_lifecycle_emitter();
        let watch_setup_started = Instant::now();
        self.output_manager.debug_event(&format!(
            "watch: scheduling initial setup on blocking worker base={}",
            self.base_dir.display()
        ));
        let mut watch_setup_handle = tokio::task::spawn_blocking(move || {
            WatchManager::new(
                &config_for_watch,
                platform_for_watch,
                &base_dir_for_watch,
                watch_signal_tx,
                watch_outcome_rx,
                watch_update_rx,
                watch_query_rx,
                emitter_for_watch,
            )
        });
        let watch_result = tokio::select! {
            biased;
            _ = shutdown_rx.recv() => {
                watch_setup_handle.abort();
                let _ = watch_setup_handle.await;
                self.finish_runtime_port_manifest().await;
                self.output_manager.shutdown().await;
                return Ok(());
            }
            r = &mut watch_setup_handle => r,
        };
        self.output_manager.debug_event(&format!(
            "watch: initial setup worker finished elapsed={:?}",
            watch_setup_started.elapsed()
        ));
        match watch_result {
            Ok(Ok((watch_mgr, warnings))) => {
                for warning in &warnings {
                    self.output_manager.error_event(warning);
                }
                // Only publish the handle once there is a watcher to
                // reach. With nothing to watch the manager is dropped here,
                // and a `Some` handle would address dead receivers.
                if watch_mgr.has_watches() {
                    // Publish the query sender for the server's WatchStatusReader
                    // — GET /watch talks to the watcher directly from here on.
                    let _ = self
                        .watch_status_tx
                        .send(Some(Some(watch_query_tx.clone())));
                    self.watch = Some(watch_link::WatchHandle::new(
                        watch_update_tx,
                        watch_query_tx,
                    ));
                    watch_handle = Some(tokio::spawn(async move {
                        watch_mgr.run().await;
                    }));
                }
            }
            Ok(Err(e)) => {
                self.output_manager
                    .error_event(&format!("file watcher setup failed: {e}"));
            }
            Err(join_err) => {
                self.output_manager
                    .error_event(&format!("file watcher setup task failed: {join_err}"));
            }
        }

        // Watch setup has decided either way by now; if no query sender was
        // published, tell WatchStatusReader holders "nothing to watch" so
        // `don watch` answers immediately instead of waiting forever.
        if self.watch.is_none() {
            let _ = self.watch_status_tx.send(Some(None));
        }

        // Release the build manager. Every supervisor asked for its artifact
        // as it was constructed, and the manager has been holding those
        // requests for exactly this moment: watch paths are resolved *by*
        // those builds and must reach the watcher, so nothing may build until
        // there is (or provably is not) a watcher to receive them. Holding
        // them this long is also what makes the whole startup burst one
        // `bazel build`.
        let _ = self
            .batcher_tx
            .send(build_batcher::BatchRequest::WatchReady {
                updates: self.watch.as_ref().map(watch_link::WatchHandle::updates),
            });

        // Validate the active dependency graph before starting anything.
        let dep_map = self.build_dep_name_map();
        topological_sort(&dep_map).map_err(|cycle| RunnerError::Cycle { cycle })?;

        // Channel for dependency-scheduled completion notifications. Store the
        // sender on `self` so services requested later use the same path.
        // From here on, decisions are published. Before it, `GateWriter`
        // swallows them — construction and setup cannot grant permission.
        self.start_gates.arm();

        // Initial non-lazy processes already occupy Pending. A lazy connection
        // performs the same state transition and can join this scheduler at
        // any point, including while this first sweep is running.
        self.publish_start_gates().await;
        let mut startup_complete = false;

        // Main loop: wait for completions, commands, and signals.
        if shutdown_requested() {
            self.initiate_shutdown().await;
        } else {
            loop {
                if self.shutting_down {
                    break;
                }
                if shutdown_requested() {
                    self.initiate_shutdown().await;
                    break;
                }

                // Emit "all services running" once when startup is complete.
                if !startup_complete && self.initial_startup_settled() {
                    startup_complete = true;
                    // Release anything waiting for the runner to settle before
                    // issuing a command — see `StateReader::
                    // wait_for_startup_complete`.
                    self.state.set_startup_complete(true);
                    let _ = self.event_tx.send(RunnerEvent::StartupSettled);
                    let starts = self.service_starts.registry();
                    let has_running_services = self.services.iter().any(|(name, rs)| {
                        matches!(
                            rs.state(),
                            ServiceState::Pending
                                | ServiceState::Building
                                | ServiceState::Running
                                | ServiceState::Ready
                                | ServiceState::Starting
                                | ServiceState::Lazy
                        ) || rs.restart_pending
                            || starts.is_busy(name)
                    });

                    if has_running_services {
                        self.output_manager.lifecycle_event("all services running");
                    } else {
                        // No services to keep alive — exit.
                        break;
                    }
                }

                // Deferred scheduling tick: fold transitions request a sweep
                // rather than recursing into one mid-handler; it runs here,
                // between messages, exactly once per batch of transitions.
                if self.gate_recompute_scheduled {
                    self.gate_recompute_scheduled = false;
                    self.publish_start_gates().await;
                }

                tokio::select! {
                    Some(cmd) = self.cmd_rx.recv() => {
                        match cmd {
                            RunnerCommand::Shutdown => {
                                self.initiate_shutdown().await;
                                break;
                            }
                            RunnerCommand::Status { verbose, name, reply } => {
                                let statuses = self.collect_status(verbose, name.as_deref()).await;
                                let _ = reply.send(statuses);
                            }
                            RunnerCommand::Start { name, reply } => {
                                self.handle_start_service_cmd(&name, reply).await;
                            }
                            RunnerCommand::Stop { name, reply } => {
                                self.handle_stop_cmd(&name, reply).await;
                            }
                            RunnerCommand::Restart { name, reply } => {
                                if self.tasks.contains_key(&name) {
                                    let result = self.handle_restart_task_cmd(&name).await;
                                    let _ = reply.send(result);
                                } else {
                                    self.handle_restart_service_cmd(&name, reply).await;
                                }
                            }
                            RunnerCommand::HardRestart { name, reply } => {
                                self.handle_hard_restart_service_cmd(&name, reply).await;
                            }
                            RunnerCommand::Rebuild { name } => {
                                self.send_rebuild(&name, false, None);
                            }
                            RunnerCommand::RebuildStale { name } => {
                                self.send_mark_stale(&name);
                            }
                            RunnerCommand::TaskRerun { name } => {
                                self.handle_task_rerun(&name).await;
                            }
                            RunnerCommand::RunPendingTasks { reply } => {
                                self.handle_run_pending_tasks(reply).await;
                            }
                            RunnerCommand::RunTask {
                                name,
                                params,
                                wait,
                                wait_timeout,
                                reply,
                            } => {
                                self.handle_run_task(&name, params, wait, wait_timeout, reply)
                                    .await;
                            }
                        }
                    }
                    Some(update) = self.update_rx.recv() => {
                        self.broadcast_update_check(update);
                    }
                    Some(report) = self.report_rx.recv() => {
                        match report {
                            // Only the first connection acts: it moves Lazy →
                            // Pending, and the normal dependency scheduler
                            // owns the service from there.
                            ProcessReport::Demand { name, demand } => {
                                self.handle_demand(&name, demand);
                            }
                            ProcessReport::ServiceExited { name, pgid, status, policy } => {
                                self.handle_service_exited(&name, pgid, status, policy).await;
                            }
                            ProcessReport::HealthChanged { name, healthy, policy } => {
                                self.handle_service_health_changed(&name, healthy, policy).await;
                            }
                            ProcessReport::TaskExited(exit) => {
                                self.handle_task_exit(exit);
                            }
                            ProcessReport::RebuildCycleDone { name, success } => {
                                // The supervisor ran the whole cycle; this
                                // closes the watch cycle it opened.
                                let _ = self
                                    .event_tx
                                    .send(RunnerEvent::RebuildComplete { name, success });
                            }
                            ProcessReport::ArtifactBuild { name, kind, status } => {
                                self.handle_artifact_build(&name, kind, status);
                            }
                            ProcessReport::ServiceStarting { name, restarting } => {
                                self.handle_service_starting(&name, restarting);
                            }
                            ProcessReport::ServiceStartPrepared {
                                name,
                                intent,
                                result,
                                policy,
                            } => {
                                self.handle_service_start_prepared(&name, intent, result, policy)
                                    .await;
                            }
                            ProcessReport::ServiceReady {
                                name,
                                success,
                                message,
                                had_check,
                                policy,
                            } => {
                                self.handle_service_ready_report(
                                    &name, success, message, had_check, policy,
                                )
                                .await;
                            }
                            ProcessReport::ServiceStopComplete { name, result, reply, .. } => {
                                self.handle_service_stop_complete(&name, result, reply)
                                    .await;
                            }
                            ProcessReport::TaskRunPrepared {
                                name,
                                task_cfg,
                                intent,
                                result,
                            } => {
                                self.handle_task_run_prepared(&name, &task_cfg, intent, result)
                                    .await;
                            }
                        }
                    }
                    // Finished batches from the batcher actor. The actor has
                    // already released its slot, so anything applied here can
                    // queue follow-up work without being deferred behind a
                    // batch that has already finished.
                    Some(outcome) = self.batch_outcome_rx.recv() => {
                        match outcome {
                            build_batcher::BatchOutcome::Requeries(outcomes) => {
                                self.handle_graph_requery_complete(outcomes).await;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        self.initiate_shutdown().await;
                        break;
                    }
                }
            }
        }

        // Wait for any remaining service exits during shutdown.
        self.wait_for_shutdown().await;

        if let Some(handle) = self.update_check_handle.take() {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }

        // Stop the API server (no-op if already signalled by initiate_shutdown).

        // Abort the watch task so its LifecycleEmitter (which holds a clone of
        // the stdout sink sender) drops. Otherwise the subsequent output
        // shutdown blocks forever waiting for writer tasks to drain.
        if let Some(handle) = watch_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        // Aborting the watcher drops its signal sender, so the link would
        // exit on its own — but only once it is next polled. Ending it here
        // keeps teardown deterministic rather than leaving a task racing the
        // rest of shutdown.
        watch_link_handle.abort();
        let _ = watch_link_handle.await;

        self.finish_runtime_port_manifest().await;
        if self.shutting_down {
            self.output_manager.lifecycle_event("shutdown complete");
        }

        // Shut down the output system — flush all pending messages to sinks.
        self.output_manager.shutdown().await;

        // NOW end the API server, streams included. This must be the last
        // act of teardown, after the output flush: every lifecycle line —
        // "shutdown complete" included — is in the log tap's buffers by this
        // point, and the streaming forwarders drain those buffers on this
        // signal before closing. Flipping any earlier cuts attached clients
        // off mid-narration; not flipping at all deadlocks exit (a follower
        // connection holds ApiState senders — see `ApiState::shutdown`).
        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(true);
            // Wait for the server to actually finish: `closed()` resolves
            // when every receiver is gone — the accept loop's and each
            // streaming connection's `ApiState` clone — which is also when
            // the socket file is removed (SocketGuard). Without this, `run`
            // returning would not imply the API is down, and an embedder
            // (or test) checking `.don/don.sock` right after would race the
            // detached server task. Bounded: a wedged connection must not
            // hold exit hostage — the drain is finite, this is a backstop.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), tx.closed()).await;
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn all_services_ready_table() {
        fn svc(state: ServiceState) -> ProcessStatus {
            ProcessStatus::Service {
                runtime: None,
                name: "s".to_string(),
                state,
                failed_dependencies: Vec::new(),
                verbose: None,
            }
        }
        fn task(state: TaskState) -> ProcessStatus {
            ProcessStatus::Task {
                pid: None,
                name: "t".to_string(),
                state,
                failed_dependencies: Vec::new(),
                last_run: None,
                verbose: None,
            }
        }

        struct Case {
            name: &'static str,
            processes: Vec<ProcessStatus>,
            want: bool,
        }
        let cases = vec![
            Case {
                name: "empty is ready",
                processes: vec![],
                want: true,
            },
            Case {
                name: "all ready",
                processes: vec![svc(ServiceState::Ready), svc(ServiceState::Ready)],
                want: true,
            },
            Case {
                name: "lazy counts as available",
                processes: vec![svc(ServiceState::Lazy), svc(ServiceState::Ready)],
                want: true,
            },
            Case {
                name: "running is not yet ready",
                processes: vec![svc(ServiceState::Ready), svc(ServiceState::Running)],
                want: false,
            },
            Case {
                name: "failed is not ready",
                processes: vec![svc(ServiceState::Failed)],
                want: false,
            },
            Case {
                name: "stopped is not ready",
                processes: vec![svc(ServiceState::Stopped)],
                want: false,
            },
            Case {
                name: "tasks do not gate readiness",
                processes: vec![svc(ServiceState::Ready), task(TaskState::Failed)],
                want: true,
            },
            Case {
                name: "task-only set is ready",
                processes: vec![task(TaskState::Completed)],
                want: true,
            },
        ];
        for c in cases {
            assert_eq!(all_services_ready(&c.processes), c.want, "case: {}", c.name);
        }
    }

    #[test]
    fn item_status_deserializes_without_dependency_failure_detail() {
        let cases = vec![
            r#"{"kind":"service","name":"api","state":"dependencyfailed","verbose":null}"#,
            r#"{"kind":"task","name":"setup","state":"dependency_failed","last_run":null,"verbose":null}"#,
        ];

        for json in cases {
            let status: ProcessStatus = serde_json::from_str(json).unwrap();
            let failed_dependencies = match status {
                ProcessStatus::Service {
                    failed_dependencies,
                    ..
                }
                | ProcessStatus::Task {
                    failed_dependencies,
                    ..
                } => failed_dependencies,
            };
            assert!(failed_dependencies.is_empty(), "json: {json}");
        }
    }

    /// Build a runner with a single watch-enabled bazel service "api", for
    /// exercising the rebuild-batch completion paths directly. Returns the
    /// shutdown sender too so the runner's `shutdown_rx` stays open.
    /// Build a runner from a config string, for exercising the scheduler's
    /// decision functions directly.
    async fn runner_from_toml(toml: &str, temp: &std::path::Path) -> (Runner, mpsc::Sender<()>) {
        use crate::config::types::LogConfig;

        let config: Config = toml.parse().unwrap();
        let log = LogConfig::Stdout;
        let names: Vec<(&str, &LogConfig)> = config
            .services
            .keys()
            .chain(config.tasks.keys())
            .map(|name| (name.as_str(), &log))
            .collect();
        let output_manager = crate::output::OutputManager::new(&names, tokio::io::sink())
            .await
            .unwrap();
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let runner = Runner::new(
            config,
            Platform::LinuxX86_64,
            output_manager,
            temp.to_path_buf(),
            None,
            shutdown_rx,
            true,
        )
        .await
        .unwrap();
        (runner, shutdown_tx)
    }

    /// The dependency gate, stated as a table: a blocking edge opens only on
    /// satisfaction; a non-blocking edge also opens once the dependency has
    /// settled into a state nothing will move on its own.
    #[tokio::test(flavor = "current_thread")]
    async fn dependency_gate_table() {
        struct Case {
            name: &'static str,
            state: ServiceState,
            blocking_open: bool,
            non_blocking_open: bool,
        }

        let cases = vec![
            Case {
                name: "ready satisfies both",
                state: ServiceState::Ready,
                blocking_open: true,
                non_blocking_open: true,
            },
            Case {
                name: "lazy counts as satisfied",
                state: ServiceState::Lazy,
                blocking_open: true,
                non_blocking_open: true,
            },
            Case {
                name: "unhealthy still satisfies (it is up)",
                state: ServiceState::Unhealthy,
                blocking_open: true,
                non_blocking_open: true,
            },
            Case {
                name: "running is not yet settled or satisfied",
                state: ServiceState::Running,
                blocking_open: false,
                non_blocking_open: false,
            },
            Case {
                name: "pending blocks everyone",
                state: ServiceState::Pending,
                blocking_open: false,
                non_blocking_open: false,
            },
            Case {
                name: "failed opens only ordering-only edges",
                state: ServiceState::Failed,
                blocking_open: false,
                non_blocking_open: true,
            },
            Case {
                name: "stopped opens only ordering-only edges",
                state: ServiceState::Stopped,
                blocking_open: false,
                non_blocking_open: true,
            },
        ];

        let temp = tempfile::tempdir().unwrap();
        let (mut runner, _shutdown_tx) = runner_from_toml(
            "[services.dep]\nrun = { cmd = \"sleep\", args = [\"1\"] }\n",
            temp.path(),
        )
        .await;

        for case in cases {
            runner.set_service_state("dep", case.state);
            let blocking = crate::config::Dependency::blocking("dep");
            let non_blocking = crate::config::Dependency {
                name: "dep".to_string(),
                blocking: false,
            };
            assert_eq!(
                runner.is_dep_gate_open(&blocking),
                case.blocking_open,
                "{}: blocking edge",
                case.name
            );
            assert_eq!(
                runner.is_dep_gate_open(&non_blocking),
                case.non_blocking_open,
                "{}: non-blocking edge",
                case.name
            );
        }
    }

    /// Failure blocking as the user sees it: a chain reports the ROOT cause,
    /// non-blocking edges never cascade, and a recovered root returns its
    /// descendants to the scheduler.
    #[tokio::test(flavor = "current_thread")]
    async fn failure_roots_collapse_and_recover() {
        let temp = tempfile::tempdir().unwrap();
        let toml = "\
[services.db]\nrun = { cmd = \"sleep\", args = [\"1\"] }\n\
[services.worker]\nrun = { cmd = \"sleep\", args = [\"1\"] }\ndepends_on = [\"db\"]\n\
[services.api]\nrun = { cmd = \"sleep\", args = [\"1\"] }\ndepends_on = [\"worker\"]\n\
[services.observer]\nrun = { cmd = \"sleep\", args = [\"1\"] }\ndepends_on = [{ name = \"worker\", blocking = false }]\n";
        let (mut runner, _shutdown_tx) = runner_from_toml(toml, temp.path()).await;

        runner.set_service_state("db", ServiceState::Failed);
        runner.publish_start_gates().await;

        // The whole blocking chain collapses to the root cause.
        for name in ["worker", "api"] {
            let rs = runner.services.get(name).unwrap();
            assert_eq!(rs.state(), ServiceState::DependencyFailed, "{name}: state");
            assert_eq!(
                rs.failed_dependencies(),
                &["db".to_string()],
                "{name}: roots collapse transitively to the first cause"
            );
        }
        // A non-blocking edge never cascades.
        assert_eq!(
            runner.services.get("observer").unwrap().state(),
            ServiceState::Pending,
            "non-blocking dependents are not failed by their dependency"
        );

        // Recovery: the root becoming satisfied re-queues the descendants.
        runner.set_service_state("db", ServiceState::Ready);
        runner.publish_start_gates().await;
        for name in ["worker", "api"] {
            assert_ne!(
                runner.services.get(name).unwrap().state(),
                ServiceState::DependencyFailed,
                "{name}: returns to the scheduler once the root recovers"
            );
        }
    }

    /// Startup-settled as a table over the state space.
    #[tokio::test(flavor = "current_thread")]
    async fn startup_settled_table() {
        struct Case {
            name: &'static str,
            service: ServiceState,
            lazy: bool,
            settled: bool,
        }

        let cases = vec![
            Case {
                name: "pending is unsettled work",
                service: ServiceState::Pending,
                lazy: false,
                settled: false,
            },
            Case {
                name: "running means a ready check is still deciding",
                service: ServiceState::Running,
                lazy: false,
                settled: false,
            },
            Case {
                name: "ready settles",
                service: ServiceState::Ready,
                lazy: false,
                settled: true,
            },
            Case {
                name: "failed settles (loudly, but settled)",
                service: ServiceState::Failed,
                lazy: false,
                settled: true,
            },
            Case {
                name: "a lazy service pending its first connection settles",
                service: ServiceState::Pending,
                lazy: true,
                settled: true,
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let (mut runner, _shutdown_tx) = runner_from_toml(
                "[services.api]\nrun = { cmd = \"sleep\", args = [\"1\"] }\n",
                temp.path(),
            )
            .await;
            if let Some(rs) = runner.services.get_mut("api") {
                rs.resolved.lazy = case.lazy;
            }
            runner.set_service_state("api", case.service);
            assert_eq!(
                runner.initial_startup_settled(),
                case.settled,
                "case '{}'",
                case.name
            );
        }
    }

    async fn single_bazel_runner(temp: &std::path::Path) -> (Runner, mpsc::Sender<()>) {
        use crate::config::service::{Service, ServiceKind};
        use crate::config::types::{BazelConfig, LogConfig, LogFilterConfig};

        let config = Config {
            services: [(
                "api".to_string(),
                Service {
                    dir: None,
                    env: HashMap::new(),
                    env_file: Vec::new(),
                    watch: Vec::new(),
                    ignore: Vec::new(),
                    debounce: None,
                    depends_on: Vec::new(),
                    proxy: Vec::new(),
                    lazy: false,
                    download: None,
                    ready: None,
                    shutdown: None,
                    log: LogConfig::Stdout,
                    log_filter: LogFilterConfig::default(),
                    reload: true,
                    tty: true,
                    on_failure: crate::config::OnFailure::Notify,
                    platform: HashMap::new(),
                    hidden: false,
                    auto_filter_on_failure: None,
                    kind: Some(ServiceKind::Bazel(BazelConfig {
                        target: "//api:api".to_string(),
                        watch: true,
                    })),
                },
            )]
            .into_iter()
            .collect(),
            service_groups: HashMap::new(),
            tasks: HashMap::new(),
            profiles: HashMap::new(),
            default_profile: None,
            watch_ignore: Vec::new(),
            shutdown: crate::config::ShutdownConfig::default(),
            log_filter: LogFilterConfig::default(),
            auto_filter_on_failure: true,
            fallback_ports: false,
        };
        let output_manager = crate::output::OutputManager::new_verbose(
            &[("api", &LogConfig::Stdout)],
            tokio::io::sink(),
            false,
        )
        .await
        .unwrap();
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let runner = Runner::new(
            config,
            Platform::LinuxX86_64,
            output_manager,
            temp.to_path_buf(),
            None,
            shutdown_rx,
            true,
        )
        .await
        .unwrap();
        (runner, shutdown_tx)
    }

    /// The scheduler's whole part in a build, as a table: it records what
    /// the supervisor tells it and decides nothing. A `Ready` for a process
    /// that has moved on since — stopped, restarted — is inert; the artifact
    /// is simply there when that process next needs it.
    #[tokio::test(flavor = "current_thread")]
    async fn artifact_build_reports_only_move_a_process_still_waiting() {
        struct Case {
            name: &'static str,
            before: ServiceState,
            status: crate::process::ArtifactBuildStatus,
            want: ServiceState,
        }

        let cases = vec![
            Case {
                name: "a requested build shows as Building",
                before: ServiceState::Pending,
                status: crate::process::ArtifactBuildStatus::Started,
                want: ServiceState::Building,
            },
            Case {
                name: "a lazy service demanded then built returns to the scheduler",
                before: ServiceState::Building,
                status: crate::process::ArtifactBuildStatus::Ready,
                want: ServiceState::Pending,
            },
            Case {
                // Never retried: the restart policy is for failures where
                // waiting can change the answer, and a compile is not one.
                name: "a build failure is terminal",
                before: ServiceState::Building,
                status: crate::process::ArtifactBuildStatus::Failed("boom".to_string()),
                want: ServiceState::Failed,
            },
            Case {
                name: "a service stopped mid-build stays stopped",
                before: ServiceState::Stopped,
                status: crate::process::ArtifactBuildStatus::Ready,
                want: ServiceState::Stopped,
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;
            runner.set_service_state("api", case.before);

            runner.handle_artifact_build("api", ProcessKind::Service, case.status);

            assert_eq!(
                runner.services.get("api").map(|service| service.state()),
                Some(case.want),
                "case '{}'",
                case.name,
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn service_commands_reject_during_lazy_build() {
        #[derive(Clone, Copy)]
        enum Operation {
            Start,
            Restart,
        }

        struct Case {
            name: &'static str,
            operation: Operation,
            expected_message: &'static str,
        }

        let cases = vec![
            Case {
                name: "start",
                operation: Operation::Start,
                expected_message: "cannot start while Building",
            },
            Case {
                name: "restart",
                operation: Operation::Restart,
                expected_message: "cannot restart while Building",
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;
            runner.set_service_state("api", ServiceState::Building);
            let (reply_tx, reply_rx) = oneshot::channel();

            match case.operation {
                Operation::Start => runner.handle_start_service_cmd("api", reply_tx).await,
                Operation::Restart => runner.handle_restart_service_cmd("api", reply_tx).await,
            }

            let result = reply_rx.await.unwrap();
            assert!(
                matches!(
                    &result,
                    Err(CommandError::InvalidState { message, .. })
                        if message == case.expected_message
                ),
                "case '{}' returned {result:?}",
                case.name,
            );
            assert_eq!(
                runner.services.get("api").map(|service| service.state()),
                Some(ServiceState::Building),
                "case '{}'",
                case.name,
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ready_report_folds_only_in_running_state() {
        struct Case {
            name: &'static str,
            state: ServiceState,
            success: bool,
            expected: ServiceState,
        }

        // Staleness by generation is unrepresentable now: the supervisor
        // clears a pending outcome on any newer Start/Stop, and outcomes
        // arrive after their own wired report on one channel. What is left
        // to guard is state: a crash's exit report folding first moves the
        // service out of Running, and the outcome must then be inert.
        let cases = vec![
            Case {
                name: "stopped service ignores a completion",
                state: ServiceState::Stopped,
                success: true,
                expected: ServiceState::Stopped,
            },
            Case {
                name: "failed service ignores a completion",
                state: ServiceState::Failed,
                success: true,
                expected: ServiceState::Failed,
            },
            Case {
                name: "running service accepts success",
                state: ServiceState::Running,
                success: true,
                expected: ServiceState::Ready,
            },
            Case {
                name: "running service accepts failure",
                state: ServiceState::Running,
                success: false,
                expected: ServiceState::Failed,
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;
            runner.set_service_state("api", case.state);

            runner
                .handle_service_ready_report(
                    "api",
                    case.success,
                    None,
                    true,
                    crate::process::health::PolicyOutcome::None,
                )
                .await;

            assert_eq!(
                runner.services.get("api").map(|service| service.state()),
                Some(case.expected),
                "case '{}'",
                case.name,
            );
        }
    }

    #[test]
    fn test_topological_sort() {
        struct Case {
            name: &'static str,
            deps: Vec<(&'static str, Vec<&'static str>)>,
            expect_ok: bool,
        }

        let cases = vec![
            Case {
                name: "linear chain a -> b -> c",
                deps: vec![("a", vec![]), ("b", vec!["a"]), ("c", vec!["b"])],
                expect_ok: true,
            },
            Case {
                name: "diamond: a -> b, a -> c, b -> d, c -> d",
                deps: vec![
                    ("a", vec![]),
                    ("b", vec!["a"]),
                    ("c", vec!["a"]),
                    ("d", vec!["b", "c"]),
                ],
                expect_ok: true,
            },
            Case {
                name: "independent nodes",
                deps: vec![("a", vec![]), ("b", vec![]), ("c", vec![])],
                expect_ok: true,
            },
            Case {
                name: "cycle: a -> b -> c -> a",
                deps: vec![("a", vec!["c"]), ("b", vec!["a"]), ("c", vec!["b"])],
                expect_ok: false,
            },
            Case {
                name: "self-cycle: a -> a",
                deps: vec![("a", vec!["a"])],
                expect_ok: false,
            },
            Case {
                name: "empty graph",
                deps: vec![],
                expect_ok: true,
            },
            Case {
                name: "single node no deps",
                deps: vec![("a", vec![])],
                expect_ok: true,
            },
            // Real-world regression: a stray reference to something that
            // isn't a node in the graph (e.g. an unexpanded service-group
            // ref left over by a code path that re-runs `Service::resolve`)
            // must not blow up topological_sort. Pre-fix, this returned an
            // empty order and the runner's shutdown loop never visited any
            // service, leaving don wedged after "shutting down gracefully".
            Case {
                name: "unknown dep ref is ignored",
                deps: vec![
                    ("a", vec![]),
                    ("b", vec!["a", "ghost-group"]),
                    ("c", vec!["b"]),
                ],
                expect_ok: true,
            },
        ];

        for case in cases {
            let dep_map: HashMap<String, Vec<String>> = case
                .deps
                .iter()
                .map(|(name, ds)| (name.to_string(), ds.iter().map(|d| d.to_string()).collect()))
                .collect();

            let result = topological_sort(&dep_map);

            if case.expect_ok {
                let order = result.unwrap_or_else(|e| {
                    panic!("case '{}': expected Ok, got cycle: {:?}", case.name, e)
                });
                // Verify: every node appears, and every node appears after its deps.
                assert_eq!(
                    order.len(),
                    dep_map.len(),
                    "case '{}': all nodes must appear",
                    case.name
                );
                let positions: HashMap<&str, usize> = order
                    .iter()
                    .enumerate()
                    .map(|(i, n)| (n.as_str(), i))
                    .collect();
                for (name, node_deps) in &dep_map {
                    for dep in node_deps {
                        // Unknown deps (refs to nodes that aren't in the
                        // graph — e.g. unexpanded service-group refs) are
                        // ignored by topological_sort, so they shouldn't
                        // appear in `positions` either. Skip them in the
                        // ordering check.
                        let Some(&dep_pos) = positions.get(dep.as_str()) else {
                            continue;
                        };
                        assert!(
                            dep_pos < positions[name.as_str()],
                            "case '{}': {} should appear before {}",
                            case.name,
                            dep,
                            name
                        );
                    }
                }
            } else {
                assert!(
                    result.is_err(),
                    "case '{}': expected cycle detection",
                    case.name
                );
                let cycle = result.unwrap_err();
                assert!(
                    cycle.len() >= 2,
                    "case '{}': cycle should have at least 2 elements, got {:?}",
                    case.name,
                    cycle
                );
            }
        }
    }

    #[test]
    fn test_service_state_transitions() {
        struct Case {
            name: &'static str,
            from: ServiceState,
            to: ServiceState,
            valid: bool,
        }

        let cases = vec![
            Case {
                name: "lazy -> pending (first connection)",
                from: ServiceState::Lazy,
                to: ServiceState::Pending,
                valid: true,
            },
            Case {
                name: "pending -> starting",
                from: ServiceState::Pending,
                to: ServiceState::Starting,
                valid: true,
            },
            Case {
                name: "starting -> running",
                from: ServiceState::Starting,
                to: ServiceState::Running,
                valid: true,
            },
            Case {
                name: "starting -> failed",
                from: ServiceState::Starting,
                to: ServiceState::Failed,
                valid: true,
            },
            Case {
                name: "running -> ready",
                from: ServiceState::Running,
                to: ServiceState::Ready,
                valid: true,
            },
            Case {
                name: "running -> stopping",
                from: ServiceState::Running,
                to: ServiceState::Stopping,
                valid: true,
            },
            Case {
                name: "running -> stopped",
                from: ServiceState::Running,
                to: ServiceState::Stopped,
                valid: true,
            },
            Case {
                name: "running -> failed",
                from: ServiceState::Running,
                to: ServiceState::Failed,
                valid: true,
            },
            Case {
                name: "ready -> stopping",
                from: ServiceState::Ready,
                to: ServiceState::Stopping,
                valid: true,
            },
            Case {
                name: "ready -> stopped",
                from: ServiceState::Ready,
                to: ServiceState::Stopped,
                valid: true,
            },
            Case {
                name: "stopping -> stopped",
                from: ServiceState::Stopping,
                to: ServiceState::Stopped,
                valid: true,
            },
            Case {
                name: "stopped -> pending (restart)",
                from: ServiceState::Stopped,
                to: ServiceState::Pending,
                valid: true,
            },
            Case {
                name: "failed -> pending (restart)",
                from: ServiceState::Failed,
                to: ServiceState::Pending,
                valid: true,
            },
            Case {
                name: "ready -> unhealthy (monitor failed)",
                from: ServiceState::Ready,
                to: ServiceState::Unhealthy,
                valid: true,
            },
            Case {
                name: "unhealthy -> ready (recovered)",
                from: ServiceState::Unhealthy,
                to: ServiceState::Ready,
                valid: true,
            },
            Case {
                name: "unhealthy -> stopping (manual stop)",
                from: ServiceState::Unhealthy,
                to: ServiceState::Stopping,
                valid: true,
            },
            Case {
                name: "unhealthy -> failed (process exit)",
                from: ServiceState::Unhealthy,
                to: ServiceState::Failed,
                valid: true,
            },
            Case {
                name: "unhealthy -> pending (restart)",
                from: ServiceState::Unhealthy,
                to: ServiceState::Pending,
                valid: true,
            },
            // Invalid transitions
            Case {
                name: "stopped -> ready",
                from: ServiceState::Stopped,
                to: ServiceState::Ready,
                valid: false,
            },
            Case {
                name: "pending -> ready",
                from: ServiceState::Pending,
                to: ServiceState::Ready,
                valid: false,
            },
            Case {
                name: "pending -> running",
                from: ServiceState::Pending,
                to: ServiceState::Running,
                valid: false,
            },
            Case {
                name: "stopped -> running",
                from: ServiceState::Stopped,
                to: ServiceState::Running,
                valid: false,
            },
            Case {
                name: "failed -> ready",
                from: ServiceState::Failed,
                to: ServiceState::Ready,
                valid: false,
            },
        ];

        for case in cases {
            assert_eq!(
                case.from.can_transition_to(case.to),
                case.valid,
                "case '{}': {:?} -> {:?} should be {}",
                case.name,
                case.from,
                case.to,
                if case.valid { "valid" } else { "invalid" }
            );
        }
    }

    #[test]
    fn test_compute_depths() {
        let deps: HashMap<String, Vec<String>> = [
            ("a".to_string(), vec![]),
            ("b".to_string(), vec!["a".to_string()]),
            ("c".to_string(), vec!["a".to_string()]),
            ("d".to_string(), vec!["b".to_string(), "c".to_string()]),
        ]
        .into_iter()
        .collect();

        let order = topological_sort(&deps).unwrap();
        let depths = compute_depths(&order, &deps);

        assert_eq!(depths["a"], 0);
        assert_eq!(depths["b"], 1);
        assert_eq!(depths["c"], 1);
        assert_eq!(depths["d"], 2);
    }

    #[test]
    fn runtime_service_default_state() {
        use crate::config::service::ResolvedService;
        use crate::config::types::{LogConfig, LogFilterConfig};
        use std::collections::HashMap;

        let mut rs = RuntimeService::new(
            ResolvedService {
                dir: None,
                env: HashMap::new(),
                env_file: Vec::new(),
                watch: Vec::new(),
                ignore: Vec::new(),
                debounce: None,
                depends_on: Vec::new(),
                proxy: Vec::new(),
                lazy: false,
                download: None,
                ready: None,
                shutdown: None,
                log: LogConfig::Stdout,
                log_filter: LogFilterConfig::default(),
                reload: true,
                tty: true,
                on_failure: crate::config::OnFailure::Notify,
                auto_filter_on_failure: None,
                kind: None,
                resolved_binary_path: None,
            },
            ServiceState::Pending,
        );

        assert_eq!(rs.state(), ServiceState::Pending);
        assert!(rs.resolved.kind.is_none());

        struct Case {
            name: &'static str,
            dependencies: Vec<String>,
            want_changed: bool,
        }
        let cases = vec![
            Case {
                name: "enter dependency failure",
                dependencies: vec!["db".to_string()],
                want_changed: true,
            },
            Case {
                name: "refresh root cause without a state change",
                dependencies: vec!["cache".to_string()],
                want_changed: true,
            },
            Case {
                name: "identical failure is a no-op",
                dependencies: vec!["cache".to_string()],
                want_changed: false,
            },
        ];
        for case in cases {
            assert_eq!(
                rs.mark_dependency_failed(case.dependencies),
                case.want_changed,
                "case: {}",
                case.name
            );
        }
        assert_eq!(rs.failed_dependencies(), ["cache"]);
        assert_eq!(
            rs.set_state(ServiceState::Pending),
            Some(ServiceState::Pending)
        );
        assert!(rs.failed_dependencies().is_empty());
    }

    #[test]
    fn runtime_task_default_state() {
        use crate::config::types::LogConfig;
        use std::collections::HashMap;

        let mut rt = RuntimeTask::new(
            crate::config::task::Task {
                cmd: "echo".to_string(),
                args: vec!["hello".to_string()],
                dir: None,
                env: HashMap::new(),
                depends_on: Vec::new(),
                watch: Vec::new(),
                ignore: Vec::new(),
                timeout: None,
                log: LogConfig::Stdout,
                terminal: crate::config::TaskTerminal::default(),
                headless: None,
                auto_run: crate::config::TaskAutoRun::Always,
                download: None,
                bazel: None,
                params: Vec::new(),
                hidden: false,
                auto_filter_on_failure: None,
            },
            TaskState::Pending,
            false,
            None,
        );

        assert_eq!(rt.state(), TaskState::Pending);
        assert!(rt.pgid.is_none());
        assert_eq!(rt.config.cmd, "echo");

        assert!(rt.mark_dependency_failed(vec!["setup".to_string()]));
        assert_eq!(rt.failed_dependencies(), ["setup"]);
        assert_eq!(rt.set_state(TaskState::Pending), Some(TaskState::Pending));
        assert!(rt.failed_dependencies().is_empty());
    }

    #[test]
    fn test_should_rebuild_after_graph_requery() {
        use crate::config::service::ResolvedService;
        use crate::config::types::{LogConfig, LogFilterConfig};
        use std::collections::HashMap;

        struct Case {
            name: &'static str,
            state: ServiceState,
            lazy: bool,
            expected: bool,
        }

        let cases = vec![
            Case {
                name: "ready non-lazy rebuilds",
                state: ServiceState::Ready,
                lazy: false,
                expected: true,
            },
            Case {
                name: "running non-lazy rebuilds",
                state: ServiceState::Running,
                lazy: false,
                expected: true,
            },
            Case {
                name: "untouched lazy service does not cold start",
                state: ServiceState::Lazy,
                lazy: true,
                expected: false,
            },
            Case {
                name: "pending service does not rebuild",
                state: ServiceState::Pending,
                lazy: false,
                expected: false,
            },
        ];

        for case in cases {
            let service = RuntimeService::new(
                ResolvedService {
                    dir: None,
                    env: HashMap::new(),
                    env_file: Vec::new(),
                    watch: Vec::new(),
                    ignore: Vec::new(),
                    debounce: None,
                    depends_on: Vec::new(),
                    proxy: Vec::new(),
                    lazy: case.lazy,
                    download: None,
                    ready: None,
                    shutdown: None,
                    log: LogConfig::Stdout,
                    log_filter: LogFilterConfig::default(),
                    reload: true,
                    tty: true,
                    on_failure: crate::config::OnFailure::Notify,
                    auto_filter_on_failure: None,
                    kind: None,
                    resolved_binary_path: None,
                },
                case.state,
            );

            assert_eq!(
                should_rebuild_after_graph_requery(&service),
                case.expected,
                "case: {}",
                case.name
            );
        }
    }

    #[test]
    fn test_bazel_graph_requery_group_dir() {
        struct Case {
            name: &'static str,
            working_dir: PathBuf,
            expected: PathBuf,
        }

        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("repo");
        let nested = workspace.join("services").join("api");
        fs::create_dir_all(&nested).unwrap();
        fs::write(workspace.join("MODULE.bazel"), "").unwrap();

        let no_workspace = temp.path().join("scratch");
        fs::create_dir_all(&no_workspace).unwrap();

        let cases = vec![
            Case {
                name: "walks up to bazel workspace root",
                working_dir: nested.clone(),
                expected: workspace.clone(),
            },
            Case {
                name: "falls back to process dir without workspace marker",
                working_dir: no_workspace.clone(),
                expected: no_workspace.clone(),
            },
        ];

        for case in cases {
            assert_eq!(
                bazel_graph_requery_group_dir(&case.working_dir),
                case.expected,
                "case: {}",
                case.name
            );
        }
    }

    #[test]
    fn test_any_glob_path_changed_since_respects_ignore_patterns() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        fs::create_dir_all(repo.join("src")).unwrap();
        fs::create_dir_all(repo.join("generated")).unwrap();
        fs::create_dir_all(repo.join("generated/nested")).unwrap();
        fs::create_dir_all(repo.join("TARGET")).unwrap();
        fs::write(repo.join("src/app.ts"), "console.log('src');").unwrap();
        fs::write(
            repo.join("generated/schema.ts"),
            "console.log('generated');",
        )
        .unwrap();
        fs::write(
            repo.join("generated/nested/deep.ts"),
            "console.log('deep');",
        )
        .unwrap();
        fs::write(repo.join("TARGET/app.ts"), "console.log('case');").unwrap();

        assert!(any_glob_path_changed_since(
            repo,
            &["src/**".to_string()],
            &[],
            SystemTime::UNIX_EPOCH,
        ));
        assert!(!any_glob_path_changed_since(
            repo,
            &["generated/**".to_string()],
            &["generated/**".to_string()],
            SystemTime::UNIX_EPOCH,
        ));
        assert!(!any_glob_path_changed_since(
            repo,
            &["generated/**/*.ts".to_string()],
            &["generated/*".to_string()],
            SystemTime::UNIX_EPOCH,
        ));
        assert!(any_glob_path_changed_since(
            repo,
            &["TARGET/**".to_string()],
            &["target/**".to_string()],
            SystemTime::UNIX_EPOCH,
        ));
        assert!(!any_glob_path_changed_since(
            repo,
            &["src/**".to_string()],
            &[],
            SystemTime::now() + std::time::Duration::from_secs(60),
        ));
    }

    #[test]
    fn test_any_glob_path_changed_since_star_does_not_cross_separator() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        fs::create_dir_all(repo.join("src/a")).unwrap();
        fs::write(repo.join("src/a/b.ts"), "nested").unwrap();

        // `src/*.ts` matches a direct child of src, not a nested file — mirroring
        // glob::glob's component-wise walk (not Pattern::matches's default).
        assert!(!any_glob_path_changed_since(
            repo,
            &["src/*.ts".to_string()],
            &[],
            SystemTime::UNIX_EPOCH,
        ));

        fs::write(repo.join("src/top.ts"), "top").unwrap();
        assert!(any_glob_path_changed_since(
            repo,
            &["src/*.ts".to_string()],
            &[],
            SystemTime::UNIX_EPOCH,
        ));

        // `**` still crosses separators to reach the nested file.
        assert!(any_glob_path_changed_since(
            repo,
            &["src/**/*.ts".to_string()],
            &[],
            SystemTime::UNIX_EPOCH,
        ));
    }
}
