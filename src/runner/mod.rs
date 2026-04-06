//! Runner — the orchestrator that starts services and tasks in dependency order.
//!
//! The runner builds an execution plan via topological sort, then starts
//! everything whose dependencies are satisfied concurrently using tokio tasks.
//! It owns all service/task state in a plain `HashMap` — no `Arc<Mutex<>>`.
//! Communication uses channels: `mpsc` for commands in, `broadcast` for events out.

pub mod service;
pub mod task;

use crate::config::{Config, Platform};
use crate::output::OutputManager;
use crate::process::pid_file::PidFile;
use crate::runner::service::stop_service;
use crate::task_state::TaskState;
use crate::watch::WatchManager;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinSet;

use self::service::ServiceHandle;

/// Signal counter: 0 = running, 1 = graceful shutdown, 2 = force shutdown.
static SIGNAL_COUNT: AtomicU8 = AtomicU8::new(0);

/// The state of a service in the runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceState {
    Pending,
    Starting,
    Running,
    Ready,
    Stopping,
    Stopped,
    Failed,
}

impl ServiceState {
    /// Whether this state is considered "satisfied" for dependency resolution.
    /// A dependency is satisfied when the service is Ready (or for tasks, completed).
    pub(crate) fn is_satisfied(&self) -> bool {
        matches!(self, Self::Ready)
    }


    /// Valid transitions from one state to another.
    #[cfg(test)]
    pub(crate) fn can_transition_to(&self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Starting)
                | (Self::Starting, Self::Running)
                | (Self::Starting, Self::Failed)
                | (Self::Running, Self::Ready)
                | (Self::Running, Self::Stopping)
                | (Self::Running, Self::Stopped)
                | (Self::Running, Self::Failed)
                | (Self::Ready, Self::Stopping)
                | (Self::Ready, Self::Stopped)
                | (Self::Ready, Self::Failed)
                | (Self::Stopping, Self::Stopped)
                | (Self::Stopping, Self::Failed)
                // Restart: from stopped back to pending
                | (Self::Stopped, Self::Pending)
                | (Self::Failed, Self::Pending)
        )
    }
}

/// The state of a task in the runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskItemState {
    Pending,
    Running,
    Completed,
    Skipped,
    Failed,
    /// Watch files have changed but the task has `auto_rerun = false`,
    /// so it's waiting for a manual trigger.
    PendingRerun,
}

impl TaskItemState {
    pub(crate) fn is_satisfied(&self) -> bool {
        matches!(self, Self::Completed | Self::Skipped | Self::PendingRerun)
    }

}

/// An item in the dependency graph — either a service or a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeKind {
    Service,
    Task,
}

/// Result of a user-initiated command (Start/Stop/Restart).
/// `Ok(())` on success, `Err(String)` with a user-facing error message.
pub type CommandResult = Result<(), CommandError>;

/// Errors returned to API callers for service control commands.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandError {
    /// No service with this name exists in the config.
    UnknownService { name: String },
    /// The name refers to a task, not a service — start/stop/restart only
    /// apply to services.
    NotAService { name: String },
    /// The service is already running (for Start) or already stopped (for Stop).
    InvalidState { name: String, message: String },
    /// The operation itself failed.
    Failed { name: String, message: String },
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownService { name } => write!(f, "unknown service '{name}'"),
            Self::NotAService { name } => {
                write!(f, "'{name}' is a task — start/stop/restart only apply to services")
            }
            Self::InvalidState { name, message } => write!(f, "{name}: {message}"),
            Self::Failed { name, message } => write!(f, "{name}: {message}"),
        }
    }
}

/// A command sent to the runner via its `mpsc` channel.
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
    /// Rebuild a service triggered by a file watch event.
    /// Runs the build command (if any), then restarts the service.
    Rebuild { name: String },
    /// Re-run a task triggered by a file watch event.
    TaskRerun { name: String },
    /// Query the status of all services and tasks.
    Status {
        reply: oneshot::Sender<Vec<ItemStatus>>,
    },
    /// Read the last N lines from a service or task's ring buffer.
    /// Returns None if the name is unknown.
    Logs {
        name: String,
        last_n: usize,
        reply: oneshot::Sender<Option<String>>,
    },
    /// Subscribe to live log output. Returns a receiver preloaded with the
    /// last N lines, then streaming new output. None if name is unknown.
    LogsFollow {
        name: String,
        last_n: usize,
        reply: oneshot::Sender<Option<mpsc::Receiver<crate::output::SinkLine>>>,
    },
    /// Reload the config file (triggered by file watcher on don.toml).
    ConfigReload,
    /// Retry starting any Pending services/tasks whose deps are now satisfied.
    /// Sent by `handle_config_reload` after a delay so newly-spawned deps
    /// have time to pass their ready checks.
    StartPending,
    /// Initiate graceful shutdown.
    Shutdown,
}

/// Status of a single item (service or task) for status queries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ItemStatus {
    Service { name: String, state: ServiceState },
    Task { name: String, state: TaskItemState },
}

/// An event broadcast from the runner for external consumers.
#[derive(Debug, Clone)]
pub enum RunnerEvent {
    /// A service changed state.
    ServiceStateChanged { name: String, state: ServiceState },
    /// A task changed state.
    TaskStateChanged { name: String, state: TaskItemState },
    /// A rebuild cycle completed (file watch triggered).
    RebuildComplete { name: String, success: bool },
    /// A task re-run completed (file watch triggered).
    TaskRerunComplete { name: String, success: bool },
    /// Shutdown complete.
    ShutdownComplete,
}

/// Errors from runner operations.
#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("dependency cycle detected: {}", cycle.join(" -> "))]
    Cycle { cycle: Vec<String> },
    #[error("another don instance is already running (could not acquire {path})")]
    AlreadyRunning { path: String },
    #[error("process error: {0}")]
    Process(#[from] crate::process::ProcessError),
    #[error("output error: {0}")]
    Output(#[from] crate::output::OutputError),
    #[error("pid file error: {0}")]
    PidFile(#[from] crate::process::pid_file::PidFileError),
    #[error("config error: {0}")]
    Config(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Topologically sort a dependency graph.
///
/// Returns node names in an order where every node appears after all
/// its dependencies. Nodes at the same depth can be started in parallel.
///
/// Uses Kahn's algorithm (BFS-based). Returns `Err` with the cycle path
/// if a cycle is detected.
pub(crate) fn topological_sort(
    deps: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>, Vec<String>> {
    // Build in-degree map and reverse adjacency list.
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

    for name in deps.keys() {
        in_degree.entry(name.as_str()).or_insert(0);
    }

    for (name, node_deps) in deps {
        for dep in node_deps {
            in_degree.entry(dep.as_str()).or_insert(0);
            dependents
                .entry(dep.as_str())
                .or_default()
                .push(name.as_str());
            *in_degree.entry(name.as_str()).or_insert(0) += 1;
        }
    }

    // Seed queue with nodes that have no dependencies.
    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(&name, _)| name)
        .collect();

    // Sort the queue for deterministic output.
    let mut sorted_queue: Vec<&str> = queue.drain(..).collect();
    sorted_queue.sort();
    queue.extend(sorted_queue);

    let mut result = Vec::new();

    while let Some(node) = queue.pop_front() {
        result.push(node.to_string());
        if let Some(children) = dependents.get(node) {
            let mut ready_children = Vec::new();
            for &child in children {
                if let Some(deg) = in_degree.get_mut(child) {
                    *deg -= 1;
                    if *deg == 0 {
                        ready_children.push(child);
                    }
                }
            }
            // Sort for determinism.
            ready_children.sort();
            queue.extend(ready_children);
        }
    }

    if result.len() != deps.len() {
        // Cycle detected — find the cycle path for error reporting.
        let remaining: Vec<String> = deps
            .keys()
            .filter(|k| !result.contains(k))
            .cloned()
            .collect();
        // Walk the remaining nodes to find the cycle.
        if let Some(cycle) = find_cycle(deps, &remaining) {
            return Err(cycle);
        }
        // Fallback: return the remaining nodes as the "cycle".
        return Err(remaining);
    }

    Ok(result)
}

/// Find a cycle in the dependency graph among the given candidate nodes.
fn find_cycle(deps: &HashMap<String, Vec<String>>, candidates: &[String]) -> Option<Vec<String>> {
    let candidate_set: HashSet<&str> = candidates.iter().map(|s| s.as_str()).collect();

    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Unvisited,
        Visiting,
        Visited,
    }

    let mut state: HashMap<&str, State> = candidates
        .iter()
        .map(|n| (n.as_str(), State::Unvisited))
        .collect();
    let mut path: Vec<String> = Vec::new();

    fn dfs<'a>(
        node: &'a str,
        deps: &'a HashMap<String, Vec<String>>,
        state: &mut HashMap<&'a str, State>,
        path: &mut Vec<String>,
        candidates: &HashSet<&str>,
    ) -> Option<Vec<String>> {
        if let Some(s) = state.get_mut(node) {
            *s = State::Visiting;
        }
        path.push(node.to_string());

        if let Some(node_deps) = deps.get(node) {
            for dep in node_deps {
                if !candidates.contains(dep.as_str()) {
                    continue;
                }
                match state.get(dep.as_str()) {
                    Some(State::Visiting) => {
                        if let Some(cycle_start) = path.iter().position(|n| n == dep) {
                            let mut cycle: Vec<String> = path[cycle_start..].to_vec();
                            cycle.push(dep.clone());
                            return Some(cycle);
                        }
                    }
                    Some(State::Unvisited) | None => {
                        if let Some(cycle) = dfs(dep, deps, state, path, candidates) {
                            return Some(cycle);
                        }
                    }
                    Some(State::Visited) => {}
                }
            }
        }

        path.pop();
        if let Some(s) = state.get_mut(node) {
            *s = State::Visited;
        }
        None
    }

    for candidate in candidates {
        if state.get(candidate.as_str()) == Some(&State::Unvisited)
            && let Some(cycle) = dfs(candidate, deps, &mut state, &mut path, &candidate_set)
        {
            return Some(cycle);
        }
    }

    None
}

/// Compute the topological depth of each node (for parallel execution ordering).
/// Depth 0 = no dependencies. Higher depth = must wait for deeper nodes.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
pub(crate) fn compute_depths(
    order: &[String],
    deps: &HashMap<String, Vec<String>>,
) -> HashMap<String, usize> {
    let mut depths: HashMap<String, usize> = HashMap::new();
    for name in order {
        let node_deps = deps.get(name).cloned().unwrap_or_default();
        let max_dep_depth = node_deps
            .iter()
            .filter_map(|d| depths.get(d.as_str()))
            .max()
            .copied()
            .unwrap_or(0);
        let depth = if node_deps.is_empty() {
            0
        } else {
            max_dep_depth + 1
        };
        depths.insert(name.clone(), depth);
    }
    depths
}

/// The main runner that orchestrates services and tasks.
pub struct Runner {
    config: Config,
    config_path: PathBuf,
    platform: Platform,
    output_manager: OutputManager,
    base_dir: PathBuf,
    task_state: TaskState,

    // State tracking — owned by the runner, no Arc<Mutex<>>.
    service_states: HashMap<String, ServiceState>,
    task_states: HashMap<String, TaskItemState>,
    service_handles: HashMap<String, ServiceHandle>,

    /// Bound TCP sockets for services with `listen` addresses.
    /// Outlive service restarts — don holds the sockets so ports are never released.
    bound_sockets: HashMap<String, crate::process::socket::BoundSockets>,

    /// PGIDs of running task processes. Tracked so we can kill them on shutdown.
    /// Inserted when a task spawns, removed when it completes.
    running_task_pgids: HashMap<String, i32>,

    /// Signals the API server task to stop accepting connections.
    server_shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,

    /// Docker API client. `Some` if any service uses the docker preset.
    docker_client: Option<bollard::Docker>,

    // Channels
    cmd_tx: mpsc::Sender<RunnerCommand>,
    cmd_rx: mpsc::Receiver<RunnerCommand>,
    event_tx: broadcast::Sender<RunnerEvent>,

    /// Item-completion sender shared between the initial startup and config
    /// reload paths. Ready-check and task-completion callbacks send here.
    /// The main loop's `done_rx` receives these.
    done_tx: Option<mpsc::Sender<ItemDone>>,

    // Shutdown signal receiver — wakes the select loop when Ctrl+C is pressed.
    shutdown_rx: mpsc::Receiver<()>,

    // Don's own PID file
    _don_pid_file: Option<PidFile>,
}

impl Runner {
    /// Create a new runner from a validated config.
    ///
    /// `base_dir` is the project root (where `don.toml` lives).
    /// The runner acquires don's PID file at `<base_dir>/.don/don.pid`.
    pub async fn new(
        config: Config,
        config_path: PathBuf,
        platform: Platform,
        output_manager: OutputManager,
        base_dir: PathBuf,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<Self, RunnerError> {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (event_tx, _) = broadcast::channel(256);

        let don_dir = base_dir.join(".don");
        std::fs::create_dir_all(&don_dir).map_err(RunnerError::Io)?;

        // Acquire don's own PID file.
        let don_pid_path = don_dir.join("don.pid");
        let don_pid_file = PidFile::acquire(don_pid_path.clone(), std::process::id() as i32)
            .await
            .map_err(|e| match e {
                crate::process::pid_file::PidFileError::AlreadyLocked => {
                    RunnerError::AlreadyRunning {
                        path: don_pid_path.display().to_string(),
                    }
                }
                other => RunnerError::PidFile(other),
            })?;

        // Clean up stale state from a previous don run (crashed or killed).
        // Runs after we hold the PID file lock, guaranteeing we're the only
        // don instance. Collects docker container names from config.
        let docker_names: Vec<String> = config
            .services
            .iter()
            .filter_map(|(name, svc)| {
                svc.docker.as_ref().map(|d| {
                    d.container
                        .clone()
                        .unwrap_or_else(|| format!("don-{name}"))
                })
            })
            .collect();
        let cleanup_report =
            crate::process::cleanup::run_cleanup(&base_dir, &docker_names).await;
        if cleanup_report.pid_files_removed > 0
            || cleanup_report.sock_removed
            || cleanup_report.containers_removed > 0
        {
            output_manager.lifecycle_event(&format!("cleaned stale state: {cleanup_report}"));
        }

        let task_state = TaskState::new(don_dir.join("task-state"));

        // Connect to Docker if any service uses the docker preset.
        let has_docker = config.services.values().any(|s| s.docker.is_some());
        let docker_client = if has_docker {
            Some(
                bollard::Docker::connect_with_socket_defaults()
                    .map_err(|e| RunnerError::Config(format!("docker connection failed: {e}")))?,
            )
        } else {
            None
        };

        let mut service_states = HashMap::new();
        for name in config.services.keys() {
            service_states.insert(name.clone(), ServiceState::Pending);
        }

        let mut task_item_states = HashMap::new();
        for name in config.tasks.keys() {
            task_item_states.insert(name.clone(), TaskItemState::Pending);
        }

        // Prune download cache entries that aren't referenced by the current
        // config. Collects (owner_name, composite_hash) pairs.
        let cache_base = don_dir.join("cache");
        let mut keep: HashSet<(String, String)> = HashSet::new();
        for (name, svc) in &config.services {
            let resolved = svc.resolve(platform);
            if let Some(ref dl) = resolved.download {
                for artifact in dl.platform.values() {
                    keep.insert((name.clone(), artifact.composite_hash()));
                }
            }
        }
        for (name, task) in &config.tasks {
            if let Some(ref dl) = task.download {
                for artifact in dl.platform.values() {
                    keep.insert((name.clone(), artifact.composite_hash()));
                }
            }
        }
        if let Ok(removed) = crate::download::prune_cache(&cache_base, &keep)
            && !removed.is_empty()
        {
            output_manager.lifecycle_event(&format!(
                "pruned {} stale cache entr{}",
                removed.len(),
                if removed.len() == 1 { "y" } else { "ies" }
            ));
        }

        Ok(Self {
            config,
            config_path,
            platform,
            output_manager,
            base_dir,
            task_state,
            service_states,
            task_states: task_item_states,
            service_handles: HashMap::new(),
            bound_sockets: HashMap::new(),
            running_task_pgids: HashMap::new(),
            server_shutdown_tx: None,
            docker_client,
            cmd_tx,
            cmd_rx,
            event_tx,
            done_tx: None,
            shutdown_rx,
            _don_pid_file: Some(don_pid_file),
        })
    }

    /// Get a sender for sending commands to this runner.
    pub fn command_sender(&self) -> mpsc::Sender<RunnerCommand> {
        self.cmd_tx.clone()
    }

    /// Subscribe to runner events.
    pub fn subscribe(&self) -> broadcast::Receiver<RunnerEvent> {
        self.event_tx.subscribe()
    }

    /// Build the dependency map from the config (for topological sorting).
    fn build_dep_map(&self) -> HashMap<String, Vec<String>> {
        let mut deps = HashMap::new();
        for (name, svc) in &self.config.services {
            let resolved = svc.resolve(self.platform);
            deps.insert(name.clone(), resolved.depends_on);
        }
        for (name, task) in &self.config.tasks {
            deps.insert(name.clone(), task.depends_on.clone());
        }
        deps
    }

    /// Run the orchestrator: start all services and tasks in dependency order.
    ///
    /// This is the main entry point. It:
    /// 1. Builds a topological sort of the dependency graph.
    /// 2. Starts items in parallel as their dependencies become satisfied.
    /// 3. Processes commands from the mpsc channel.
    /// 4. Handles shutdown signals.
    pub async fn run(mut self) -> Result<(), RunnerError> {
        self.output_manager.lifecycle_event("loading don.toml");

        let svc_count = self.config.services.len();
        let task_count = self.config.tasks.len();

        self.output_manager.lifecycle_event(&format!(
            "validated {} service{}, {} task{}",
            svc_count,
            if svc_count == 1 { "" } else { "s" },
            task_count,
            if task_count == 1 { "" } else { "s" },
        ));

        // Bind the unix socket API synchronously so bind errors surface
        // visibly at startup. Only spawn the accept loop if bind succeeds.
        let socket_path = self.base_dir.join(".don").join("don.sock");
        let socket_display = socket_path.display().to_string();
        match crate::server::bind_api(&socket_path) {
            Ok(listener) => {
                let (server_shutdown_tx, server_shutdown_rx) =
                    tokio::sync::watch::channel(false);
                let cmd_tx_for_server = self.cmd_tx.clone();
                let socket_path_for_server = socket_path.clone();
                tokio::spawn(async move {
                    if let Err(e) = crate::server::serve_api(
                        listener,
                        socket_path_for_server,
                        cmd_tx_for_server,
                        server_shutdown_rx,
                    )
                    .await
                    {
                        eprintln!("[don] api server error: {e}");
                    }
                });
                self.output_manager
                    .lifecycle_event(&format!("api listening on {socket_display}"));
                self.server_shutdown_tx = Some(server_shutdown_tx);
            }
            Err(e) => {
                self.output_manager
                    .error_event(&format!("api server disabled: {e}"));
            }
        }

        // Start file watchers before spawning services so we don't miss
        // changes that happen during startup (slow ready checks, long builds, etc.).
        let mut _watch_handle: Option<tokio::task::JoinHandle<()>> = None;
        match WatchManager::new(
            &self.config,
            self.platform,
            &self.base_dir,
            &self.config_path,
            self.cmd_tx.clone(),
            self.event_tx.subscribe(),
        )
        .await
        {
            Ok((watch_mgr, warnings)) => {
                for warning in &warnings {
                    self.output_manager.error_event(warning);
                }
                if watch_mgr.has_watches() {
                    _watch_handle = Some(tokio::spawn(async move {
                        watch_mgr.run().await;
                    }));
                }
            }
            Err(e) => {
                self.output_manager
                    .error_event(&format!("file watcher setup failed: {e}"));
            }
        }


        // Build dependency map and topological order.
        let dep_map = self.build_dep_map();
        let order = topological_sort(&dep_map).map_err(|cycle| RunnerError::Cycle { cycle })?;

        // Channel for item completion notifications. Store the sender on `self`
        // so config reload can reuse it for newly-started services.
        let (done_tx, mut done_rx) = mpsc::channel::<ItemDone>(64);
        self.done_tx = Some(done_tx.clone());

        // Track which items are in flight.
        let mut pending: HashSet<String> = order.iter().cloned().collect();
        let mut in_flight: HashSet<String> = HashSet::new();

        // Start items whose dependencies are already satisfied.
        self.start_ready_items(&order, &dep_map, &mut pending, &mut in_flight, &done_tx)
            .await?;

        let mut all_started = false;

        // Main loop: wait for completions, commands, and signals.
        loop {
            // Emit "all services running" once when startup is complete.
            if !all_started && pending.is_empty() && in_flight.is_empty() {
                all_started = true;
                let has_running_services = self.service_states.values().any(|s| {
                    matches!(
                        s,
                        ServiceState::Running | ServiceState::Ready | ServiceState::Starting
                    )
                });

                if has_running_services {
                    self.output_manager.lifecycle_event("all services running");
                } else {
                    // No services to keep alive — exit.
                    break;
                }
            }

            tokio::select! {
                Some(item_done) = done_rx.recv() => {
                    in_flight.remove(&item_done.name);
                    self.handle_item_done(&item_done);

                    // Start newly-unblocked items.
                    self.start_ready_items(
                        &order,
                        &dep_map,
                        &mut pending,
                        &mut in_flight,
                        &done_tx,
                    ).await?;
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        RunnerCommand::Shutdown => {
                            self.initiate_shutdown().await;
                            break;
                        }
                        RunnerCommand::Status { reply } => {
                            let statuses = self.collect_status();
                            let _ = reply.send(statuses);
                        }
                        RunnerCommand::Logs { name, last_n, reply } => {
                            let logs = self.output_manager
                                .read_logs(&name, last_n)
                                .await
                                .map(|b| String::from_utf8_lossy(&b).into_owned());
                            let _ = reply.send(logs);
                        }
                        RunnerCommand::LogsFollow { name, last_n, reply } => {
                            // 256-line buffer — slow HTTP clients will drop lines
                            // (and get pruned on disconnect) rather than blocking
                            // service output.
                            let sink = self.output_manager
                                .add_follow_sink(&name, last_n, 256)
                                .await;
                            let _ = reply.send(sink);
                        }
                        RunnerCommand::Start { name, reply } => {
                            let result = self.handle_start_cmd(&name).await;
                            let _ = reply.send(result);
                        }
                        RunnerCommand::Stop { name, reply } => {
                            let result = self.handle_stop_cmd(&name).await;
                            let _ = reply.send(result);
                        }
                        RunnerCommand::Restart { name, reply } => {
                            let result = self.handle_restart_cmd(&name).await;
                            let _ = reply.send(result);
                        }
                        RunnerCommand::Rebuild { name } => {
                            self.handle_rebuild(&name).await;
                        }
                        RunnerCommand::TaskRerun { name } => {
                            self.handle_task_rerun(&name).await;
                        }
                        RunnerCommand::ConfigReload => {
                            self.handle_config_reload().await;
                        }
                        RunnerCommand::StartPending => {
                            self.start_pending_items().await;
                        }
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    self.initiate_shutdown().await;
                    break;
                }
            }
        }

        // Wait for any remaining service exits during shutdown.
        self.wait_for_shutdown().await;

        // Stop the API server (no-op if already signalled by initiate_shutdown).
        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(true);
        }

        // Shut down the output system — flush all pending messages to sinks.
        self.output_manager.shutdown().await;

        Ok(())
    }

    /// Start items whose dependencies are all satisfied.
    async fn start_ready_items(
        &mut self,
        order: &[String],
        dep_map: &HashMap<String, Vec<String>>,
        pending: &mut HashSet<String>,
        in_flight: &mut HashSet<String>,
        done_tx: &mpsc::Sender<ItemDone>,
    ) -> Result<(), RunnerError> {
        // First pass: mark items whose dependencies have failed.
        let failed_items: Vec<String> = order
            .iter()
            .filter(|name| pending.contains(name.as_str()))
            .filter(|name| {
                let node_deps = dep_map.get(name.as_str()).cloned().unwrap_or_default();
                node_deps.iter().any(|dep| self.is_dep_failed(dep))
            })
            .cloned()
            .collect();

        for name in failed_items {
            pending.remove(&name);
            if self.config.services.contains_key(&name) {
                self.service_states
                    .insert(name.clone(), ServiceState::Failed);
            } else {
                self.task_states.insert(name.clone(), TaskItemState::Failed);
            }
            self.output_manager
                .error_event(&format!("{name}: skipped (dependency failed)"));
        }

        // Second pass: start items whose dependencies are all satisfied.
        let ready: Vec<String> = order
            .iter()
            .filter(|name| pending.contains(name.as_str()))
            .filter(|name| {
                let node_deps = dep_map.get(name.as_str()).cloned().unwrap_or_default();
                node_deps.iter().all(|dep| self.is_dep_satisfied(dep))
            })
            .cloned()
            .collect();

        for name in ready {
            pending.remove(&name);
            in_flight.insert(name.clone());

            if self.config.services.contains_key(&name) {
                self.start_service(&name, done_tx.clone()).await?;
            } else if self.config.tasks.contains_key(&name) {
                self.start_task(&name, done_tx.clone()).await?;
            }
        }

        Ok(())
    }

    /// Check if a dependency is satisfied (ready service or completed task).
    fn is_dep_satisfied(&self, dep: &str) -> bool {
        if let Some(state) = self.service_states.get(dep) {
            return state.is_satisfied();
        }
        if let Some(state) = self.task_states.get(dep) {
            return state.is_satisfied();
        }
        false
    }

    /// Check if a dependency has failed.
    fn is_dep_failed(&self, dep: &str) -> bool {
        if let Some(state) = self.service_states.get(dep) {
            return *state == ServiceState::Failed;
        }
        if let Some(state) = self.task_states.get(dep) {
            return *state == TaskItemState::Failed;
        }
        false
    }

    /// Start a service: bind sockets, build, spawn, wire output + ready check.
    async fn start_service(
        &mut self,
        name: &str,
        done_tx: mpsc::Sender<ItemDone>,
    ) -> Result<(), RunnerError> {
        self.service_states
            .insert(name.to_string(), ServiceState::Starting);

        let svc = match self.config.services.get(name) {
            Some(s) => s,
            None => return Err(RunnerError::Config(format!("unknown service: {name}"))),
        };
        let resolved = svc.resolve(self.platform);

        self.output_manager
            .lifecycle_event(&format!("starting {name}..."));

        // Phase 1: Bind listen sockets (idempotent — skips if already bound).
        if let Err(msg) = self.bind_sockets_if_needed(name, &resolved) {
            self.fail_service_start(name, &msg, done_tx).await;
            return Ok(());
        }

        // Phase 1.5: Download artifact (if configured).
        if let Err(e) = self.ensure_download(name, &resolved).await {
            self.fail_service_start(name, &format!("download failed: {e}"), done_tx).await;
            return Ok(());
        }

        // Phase 2: Build (docker, rust, go, or custom build command).
        if let Err(()) = self.run_service_build(name, &resolved).await {
            self.fail_service_start(name, "build failed", done_tx).await;
            return Ok(());
        }

        // Phase 3: Spawn process + wire output + ready check.
        self.spawn_and_wire_service(name, &resolved, Some(done_tx))
            .await
    }

    /// Bind listen sockets for a service if configured and not already bound.
    fn bind_sockets_if_needed(
        &mut self,
        name: &str,
        resolved: &crate::config::ResolvedService,
    ) -> Result<(), String> {
        if resolved.listen.is_empty() || self.bound_sockets.contains_key(name) {
            return Ok(());
        }
        match crate::process::socket::bind_sockets(&resolved.listen) {
            Ok(sockets) => {
                self.output_manager.lifecycle_event(&format!(
                    "{name}: bound {} listen socket{}",
                    sockets.len(),
                    if sockets.len() == 1 { "" } else { "s" }
                ));
                self.bound_sockets.insert(name.to_string(), sockets);
                Ok(())
            }
            Err(e) => {
                self.output_manager.error_event(&format!("{name}: {e}"));
                Err(e.to_string())
            }
        }
    }

    /// Run all build steps for a service based on its preset.
    ///
    /// Handles docker image build, cargo build, go build, and custom build
    /// commands. Returns `Ok(())` on success or if no build is needed.
    /// Returns `Err(())` if the build failed (already logged + events sent).
    async fn run_service_build(
        &self,
        name: &str,
        resolved: &crate::config::ResolvedService,
    ) -> Result<(), ()> {
        // Docker: build image if docker.build is configured.
        if let Some(ref docker_config) = resolved.docker
            && let Some(ref build_config) = docker_config.build
        {
            self.output_manager
                .lifecycle_event(&format!("{name}: building docker image..."));
            if let Some(ref client) = self.docker_client
                && let Some(writer) = self.output_manager.service_writer(name)
            {
                if let Err(e) = crate::docker::build::build_image(
                    client,
                    build_config,
                    &docker_config.image,
                    &self.base_dir,
                    &writer,
                )
                .await
                {
                    self.output_manager
                        .error_event(&format!("{name}: docker build failed: {e}"));
                    return Err(());
                }
                self.output_manager
                    .lifecycle_event(&format!("{name}: docker build succeeded"));
            }
            return Ok(());
        }

        // Rust: cargo build.
        if let Some(ref rust_config) = resolved.rust {
            let build_args = service::rust_build_args(rust_config);
            return self
                .run_preset_build(name, "cargo", &build_args, resolved)
                .await;
        }

        // Go: go build.
        if let Some(ref go_config) = resolved.go {
            let output_path = service::go_binary_path(go_config, name, &self.base_dir);
            if let Some(parent) = output_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let build_args = service::go_build_args(go_config, &output_path);
            return self
                .run_preset_build(name, "go", &build_args, resolved)
                .await;
        }

        // Custom: run the build command if configured.
        if let Some(ref build_cmd) = resolved.build {
            return self
                .run_preset_build(name, &build_cmd.cmd, &build_cmd.args, resolved)
                .await;
        }

        Ok(())
    }

    /// Download the artifact for a service if it has a download config for this platform.
    ///
    /// Skips if no download is configured or if no artifact exists for the current
    /// platform (falls back to `run.cmd` via PATH).
    async fn ensure_download(
        &self,
        name: &str,
        resolved: &crate::config::ResolvedService,
    ) -> Result<(), crate::download::DownloadError> {
        self.ensure_download_for_config(name, resolved.download.as_ref())
            .await
    }

    /// Ensure the download for a task's download config is cached.
    async fn ensure_task_download(
        &self,
        name: &str,
        task: &crate::config::Task,
    ) -> Result<(), crate::download::DownloadError> {
        self.ensure_download_for_config(name, task.download.as_ref())
            .await
    }

    /// Shared download resolution for services and tasks.
    async fn ensure_download_for_config(
        &self,
        name: &str,
        download: Option<&crate::config::DownloadConfig>,
    ) -> Result<(), crate::download::DownloadError> {
        let download = match download {
            Some(dl) => dl,
            None => return Ok(()),
        };
        let artifact = match download.for_platform(self.platform) {
            Some(a) => a,
            None => return Ok(()),
        };
        let cache_base = self.base_dir.join(".don").join("cache");
        let bin_dir = self.base_dir.join(".don").join("bin");
        self.output_manager
            .lifecycle_event(&format!("{name}: ensuring artifact..."));
        let writer = self.output_manager.service_writer(name);
        crate::download::ensure_artifact(artifact, &cache_base, name, writer.as_ref()).await?;
        // Link the binary into .don/bin so other services/tasks can find it on PATH.
        if let Some(bin_name) = download.effective_bin_name(self.platform) {
            crate::download::link_binary(artifact, &cache_base, name, &bin_name, &bin_dir)?;
        }
        self.output_manager
            .lifecycle_event(&format!("{name}: artifact ready"));
        Ok(())
    }

    /// Spawn a service process, wire output capture, and start the ready check.
    ///
    /// If `done_tx` is `Some`, sends `ItemDone` on completion (initial startup).
    /// If `done_tx` is `None`, sends `RebuildComplete` (file-watch rebuild).
    async fn spawn_and_wire_service(
        &mut self,
        name: &str,
        resolved: &crate::config::ResolvedService,
        done_tx: Option<mpsc::Sender<ItemDone>>,
    ) -> Result<(), RunnerError> {
        let pid_dir = self.base_dir.join(".don").join("pids");
        let sockets = self.bound_sockets.get(name);
        let writer = self.output_manager.service_writer(name);

        match service::start_service(
            name,
            resolved,
            &self.base_dir,
            &pid_dir,
            sockets,
            self.docker_client.as_ref(),
            writer.as_ref(),
            self.platform,
        )
        .await
        {
            Ok(start_result) => {
                self.wire_service_output_and_ready_check(
                    name,
                    start_result,
                    resolved,
                    done_tx,
                );
                Ok(())
            }
            Err(e) => {
                self.service_states
                    .insert(name.to_string(), ServiceState::Failed);
                self.output_manager
                    .error_event(&format!("{name}: failed to start: {e}"));

                if let Some(done_tx) = done_tx {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name.to_string(),
                            kind: NodeKind::Service,
                            success: false,
                            message: Some(e.to_string()),
                            elapsed: None,
                        })
                        .await;
                } else {
                    let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                        name: name.to_string(),
                        success: false,
                    });
                }

                Ok(())
            }
        }
    }

    /// Helper: mark a service as failed during startup and notify via done_tx.
    async fn fail_service_start(
        &mut self,
        name: &str,
        message: &str,
        done_tx: mpsc::Sender<ItemDone>,
    ) {
        self.service_states
            .insert(name.to_string(), ServiceState::Failed);
        let _ = done_tx
            .send(ItemDone {
                name: name.to_string(),
                kind: NodeKind::Service,
                success: false,
                message: Some(message.to_string()),
                elapsed: None,
            })
            .await;
    }

    /// Wire up a started service's output and ready check.
    ///
    /// Sets the service to Running, stores the handle, starts output capture,
    /// and spawns the ready check (if configured). On ready check completion:
    /// - If `done_tx` is `Some`, sends `ItemDone` (initial startup path).
    /// - If `done_tx` is `None`, sends `RebuildComplete` (file-watch rebuild path).
    fn wire_service_output_and_ready_check(
        &mut self,
        name: &str,
        start_result: service::StartResult,
        resolved: &crate::config::ResolvedService,
        done_tx: Option<mpsc::Sender<ItemDone>>,
    ) {
        self.service_states
            .insert(name.to_string(), ServiceState::Running);
        self.service_handles
            .insert(name.to_string(), start_result.handle);

        // Wire up output processing. The exit_tx fires when the
        // stream hits EOF (process died), used to cancel the ready check.
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        if let Some(svc_writer) = self.output_manager.service_writer(name) {
            let child_output = start_result.child_output;
            tokio::spawn(async move {
                let _ = svc_writer.process_stream(child_output).await;
                let _ = exit_tx.send(());
            });
        }

        let name_owned = name.to_string();
        let ready_config = resolved.ready.clone();
        let event_tx = self.event_tx.clone();

        if let Some(ready) = ready_config {
            tokio::spawn(async move {
                let ready_result = tokio::select! {
                    result = service::run_ready_check(&ready) => result,
                    _ = exit_rx => {
                        Err(service::ServiceError::ProcessExitedDuringReadyCheck)
                    }
                };

                let success = ready_result.is_ok();
                let state = if success {
                    ServiceState::Ready
                } else {
                    ServiceState::Failed
                };

                let _ = event_tx.send(RunnerEvent::ServiceStateChanged {
                    name: name_owned.clone(),
                    state,
                });

                if let Some(done_tx) = done_tx {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name_owned,
                            kind: NodeKind::Service,
                            success,
                            message: ready_result.err().map(|e| e.to_string()),
                            elapsed: None,
                        })
                        .await;
                } else {
                    let _ = event_tx.send(RunnerEvent::RebuildComplete {
                        name: name_owned,
                        success,
                    });
                }
            });
        } else if done_tx.is_none() {
            // No ready check on rebuild path — mark ready immediately.
            self.service_states
                .insert(name.to_string(), ServiceState::Ready);
            self.output_manager
                .lifecycle_event(&format!("{name}: restarted"));
            let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                name: name.to_string(),
                success: true,
            });
        }
    }

    /// Start a task: check skip, run, handle result.
    async fn start_task(
        &mut self,
        name: &str,
        done_tx: mpsc::Sender<ItemDone>,
    ) -> Result<(), RunnerError> {
        let task_cfg = match self.config.tasks.get(name) {
            Some(t) => t.clone(),
            None => return Err(RunnerError::Config(format!("unknown task: {name}"))),
        };

        // Check if the task needs to run.
        let base_dir = task_cfg.dir.as_deref().unwrap_or(&self.base_dir);
        let needs_run = self
            .task_state
            .needs_run(name, &task_cfg.watch, Some(base_dir))
            .await
            .unwrap_or(true);

        if !needs_run {
            self.task_states
                .insert(name.to_string(), TaskItemState::Skipped);
            self.output_manager
                .lifecycle_event(&format!("running {name}... (skipped, no changes)"));
            let _ = self.event_tx.send(RunnerEvent::TaskStateChanged {
                name: name.to_string(),
                state: TaskItemState::Skipped,
            });
            let _ = done_tx
                .send(ItemDone {
                    name: name.to_string(),
                    kind: NodeKind::Task,
                    success: true,
                    message: None,
                    elapsed: None,
                })
                .await;
            return Ok(());
        }

        let watch_file_count = if task_cfg.watch.is_empty() {
            None
        } else {
            // Count matched files for the lifecycle message.
            let count: usize = task_cfg
                .watch
                .iter()
                .filter_map(|pattern| {
                    let full = base_dir.join(pattern).to_string_lossy().into_owned();
                    glob::glob(&full).ok().map(|g| g.count())
                })
                .sum();
            Some(count)
        };

        let msg = match watch_file_count {
            Some(n) => format!(
                "running {name}... ({n} file{} changed)",
                if n == 1 { "" } else { "s" }
            ),
            None => format!("running {name}..."),
        };
        self.output_manager.lifecycle_event(&msg);

        // Ensure any downloaded artifact is cached before running.
        if let Err(e) = self.ensure_task_download(name, &task_cfg).await {
            self.task_states
                .insert(name.to_string(), TaskItemState::Failed);
            self.output_manager
                .error_event(&format!("{name}: download failed: {e}"));
            let _ = done_tx
                .send(ItemDone {
                    name: name.to_string(),
                    kind: NodeKind::Task,
                    success: false,
                    message: Some(format!("download failed: {e}")),
                    elapsed: None,
                })
                .await;
            return Ok(());
        }

        self.task_states
            .insert(name.to_string(), TaskItemState::Running);

        // Spawn the task process.
        match task::spawn_task(&task_cfg, name, &self.base_dir, self.platform).await {
            Ok(spawn) => {
                self.wire_task_output_and_wait(name, spawn, &task_cfg, Some(done_tx));
                Ok(())
            }
            Err(e) => {
                self.task_states
                    .insert(name.to_string(), TaskItemState::Failed);
                self.output_manager
                    .error_event(&format!("{name}: failed to start: {e}"));
                let _ = done_tx
                    .send(ItemDone {
                        name: name.to_string(),
                        kind: NodeKind::Task,
                        success: false,
                        message: Some(e.to_string()),
                        elapsed: None,
                    })
                    .await;
                Ok(())
            }
        }
    }

    /// Wire up a spawned task's output and wait for completion.
    ///
    /// Starts output capture, spawns a background task to wait for exit,
    /// records success in task state, and sends completion events.
    /// - If `done_tx` is `Some`, sends `ItemDone` (initial startup path).
    /// - If `done_tx` is `None`, sends `TaskRerunComplete` (file-watch rerun path).
    fn wire_task_output_and_wait(
        &mut self,
        name: &str,
        spawn: task::TaskSpawn,
        task_cfg: &crate::config::Task,
        done_tx: Option<mpsc::Sender<ItemDone>>,
    ) {
        let task::TaskSpawn {
            mut handle,
            child_output,
        } = spawn;

        // Track the PGID so we can kill it during shutdown.
        self.running_task_pgids
            .insert(name.to_string(), handle.pgid());

        if let Some(svc_writer) = self.output_manager.service_writer(name) {
            tokio::spawn(async move {
                let _ = svc_writer.process_stream(child_output).await;
            });
        }

        let name_owned = name.to_string();
        let task_cfg_clone = task_cfg.clone();
        let base_dir_owned = self.base_dir.clone();
        let task_state = TaskState::new(base_dir_owned.join(".don").join("task-state"));
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result =
                task::wait_for_task(&mut handle, task_cfg_clone.timeout.as_deref()).await;
            let elapsed = start.elapsed();

            let (success, message) = match result {
                Ok(status) => {
                    if status.success() {
                        let task_dir =
                            task_cfg_clone.dir.as_deref().unwrap_or(&base_dir_owned);
                        let _ = task_state
                            .record_success(
                                &name_owned,
                                &task_cfg_clone.watch,
                                Some(task_dir),
                            )
                            .await;
                        (true, None)
                    } else {
                        let code = status.code().unwrap_or(-1);
                        (false, Some(format!("exit code {code}")))
                    }
                }
                Err(e) => (false, Some(e.to_string())),
            };

            let state = if success {
                TaskItemState::Completed
            } else {
                TaskItemState::Failed
            };

            let _ = event_tx.send(RunnerEvent::TaskStateChanged {
                name: name_owned.clone(),
                state,
            });

            if let Some(done_tx) = done_tx {
                let _ = done_tx
                    .send(ItemDone {
                        name: name_owned,
                        kind: NodeKind::Task,
                        success,
                        message,
                        elapsed: Some(elapsed),
                    })
                    .await;
            } else {
                let _ = event_tx.send(RunnerEvent::TaskRerunComplete {
                    name: name_owned,
                    success,
                });
            }
        });
    }

    /// Emit an error and broadcast a failed `RebuildComplete` event.
    /// Run a preset build command (cargo/go) and stream output.
    /// Returns `Ok(())` on success, `Err(())` on failure (already logged + event sent).
    async fn run_preset_build(
        &self,
        name: &str,
        cmd: &str,
        args: &[String],
        resolved: &crate::config::ResolvedService,
    ) -> Result<(), ()> {
        self.output_manager
            .lifecycle_event(&format!("{name}: running {cmd} build..."));

        let work_dir = resolved.dir.as_deref().unwrap_or(&self.base_dir);
        let mut env: HashMap<String, String> = std::env::vars().collect();
        env.extend(resolved.env.clone());

        match crate::process::spawn_process(crate::process::SpawnConfig {
            cmd,
            args,
            dir: Some(work_dir),
            env,
            pgid_file_path: None,
            force_pipe: true,
            listen_fds: vec![],
        })
        .await
        {
            Ok((mut handle, child_output)) => {
                if let Some(svc_writer) = self.output_manager.service_writer(name) {
                    let output = child_output;
                    tokio::spawn(async move {
                        let _ = svc_writer.process_stream(output).await;
                    });
                }

                match handle.wait().await {
                    Ok(status) if status.success() => {
                        self.output_manager
                            .lifecycle_event(&format!("{name}: {cmd} build succeeded"));
                        Ok(())
                    }
                    Ok(status) => {
                        let code = status.code().unwrap_or(-1);
                        self.fail_rebuild(
                            name,
                            &format!("{name}: {cmd} build failed (exit code {code})"),
                        );
                        Err(())
                    }
                    Err(e) => {
                        self.fail_rebuild(name, &format!("{name}: {cmd} build error: {e}"));
                        Err(())
                    }
                }
            }
            Err(e) => {
                self.fail_rebuild(name, &format!("{name}: failed to start {cmd} build: {e}"));
                Err(())
            }
        }
    }

    fn fail_rebuild(&self, name: &str, message: &str) {
        self.output_manager.error_event(message);
        let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
            name: name.to_string(),
            success: false,
        });
    }

    /// Handle a file-watch-triggered rebuild for a service.
    ///
    /// Handle a config reload triggered by the file watcher on don.toml.
    ///
    /// Parses and validates the new config, diffs it against the running config,
    /// and applies changes: stop removed services, restart changed services,
    /// start new services. If the new config is invalid, logs an error and
    /// keeps running with the old config.
    async fn handle_config_reload(&mut self) {
        let new_config = match Config::from_file(&self.config_path) {
            Ok(c) => c,
            Err(e) => {
                self.output_manager
                    .error_event(&format!("config reload failed: {e}"));
                return;
            }
        };

        if let Err(e) = new_config.validate(self.platform) {
            self.output_manager
                .error_event(&format!("config reload rejected (invalid): {e}"));
            return;
        }

        let diff = crate::config::diff::diff_configs(&self.config, &new_config);
        if diff.is_empty() {
            return;
        }

        self.output_manager
            .lifecycle_event(&format!("config changed: {diff}"));

        // 1. Stop removed services.
        for name in &diff.removed_services {
            if let Some(handle) = self.service_handles.remove(name) {
                let svc = self.config.services.get(name);
                let resolved = svc.map(|s| s.resolve(self.platform));
                let shutdown_config = resolved.as_ref().and_then(|r| r.shutdown.clone());
                self.service_states
                    .insert(name.clone(), ServiceState::Stopping);
                let _ = service::stop_service(handle, shutdown_config.as_ref(), false).await;
                self.output_manager
                    .lifecycle_event(&format!("{name}: stopped (removed from config)"));
            }
            self.service_states.remove(name);
            self.bound_sockets.remove(name);
        }

        // 2. Stop changed services (they'll be restarted with the new config).
        //    Also release bound sockets so they get re-bound with the new
        //    listen addresses (if changed).
        for name in &diff.changed_services {
            if let Some(handle) = self.service_handles.remove(name) {
                let svc = self.config.services.get(name);
                let resolved = svc.map(|s| s.resolve(self.platform));
                let shutdown_config = resolved.as_ref().and_then(|r| r.shutdown.clone());
                self.service_states
                    .insert(name.clone(), ServiceState::Stopping);
                let _ = service::stop_service(handle, shutdown_config.as_ref(), false).await;
            }
            self.bound_sockets.remove(name);
        }

        // 3. Remove state for removed tasks.
        for name in &diff.removed_tasks {
            self.task_states.remove(name);
        }

        // 4. Swap config.
        self.config = new_config;

        // 5. Register new services/tasks in state maps and output manager.
        for name in &diff.added_services {
            self.service_states
                .insert(name.clone(), ServiceState::Pending);
            let log_config = self
                .config
                .services
                .get(name)
                .map(|s| &s.log)
                .cloned()
                .unwrap_or_default();
            self.output_manager
                .register_service(name, &log_config)
                .await;
        }
        for name in &diff.added_tasks {
            self.task_states
                .insert(name.clone(), TaskItemState::Pending);
            let log_config = self
                .config
                .tasks
                .get(name)
                .map(|t| &t.log)
                .cloned()
                .unwrap_or_default();
            self.output_manager
                .register_service(name, &log_config)
                .await;
        }

        // 6. Mark changed services as Pending for restart.
        for name in &diff.changed_services {
            self.service_states
                .insert(name.clone(), ServiceState::Pending);
        }

        // 7. Start all changed + new services and tasks via topo-sorted
        //    dependency graph. Changed services are included so they respect
        //    deps on newly-added services (e.g. add `db`, change `api` to
        //    depend on `db` → db must start before api restarts).
        let names_to_start: Vec<String> = diff
            .changed_services
            .iter()
            .chain(diff.added_services.iter())
            .chain(diff.added_tasks.iter())
            .chain(diff.changed_tasks.iter())
            .cloned()
            .collect();
        if !names_to_start.is_empty() {
            // Build a dep map for just the new/changed items.
            let dep_map = self.build_dep_map();
            // Sort the full graph (including existing items) so we get correct ordering.
            if let Ok(order) = topological_sort(&dep_map) {
                // Filter to only the items we need to start.
                let start_set: HashSet<&str> =
                    names_to_start.iter().map(|s| s.as_str()).collect();
                for name in &order {
                    if !start_set.contains(name.as_str()) {
                        continue;
                    }
                    let deps = dep_map.get(name).cloned().unwrap_or_default();
                    let deps_ok = deps.iter().all(|dep| {
                        self.service_states
                            .get(dep)
                            .is_some_and(|s| s.is_satisfied())
                            || self
                                .task_states
                                .get(dep)
                                .is_some_and(|s| s.is_satisfied())
                    });
                    if !deps_ok {
                        self.output_manager.lifecycle_event(&format!(
                            "{name}: waiting for dependencies"
                        ));
                        continue;
                    }
                    // Service or task?
                    if self.config.services.contains_key(name) {
                        // Use the full start_service flow (bind sockets, download,
                        // build, spawn, wire output + ready check, lifecycle events).
                        if let Some(done_tx) = self.done_tx.clone() {
                            let _ = self.start_service(name, done_tx).await;
                        }
                    } else if self.config.tasks.contains_key(name) {
                        self.task_states
                            .insert(name.clone(), TaskItemState::Running);
                        self.handle_task_rerun(name).await;
                    }
                }
            }
        }

        // If any services/tasks are still Pending (deps not yet Ready),
        // schedule a deferred retry. Their deps were just spawned and need
        // time to pass ready checks before we can start the dependents.
        let has_pending = self
            .service_states
            .values()
            .any(|s| *s == ServiceState::Pending)
            || self
                .task_states
                .values()
                .any(|s| *s == TaskItemState::Pending);
        if has_pending {
            let cmd_tx = self.cmd_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let _ = cmd_tx.send(RunnerCommand::StartPending).await;
            });
        }
    }

    /// Try to start any Pending services/tasks whose dependencies are now satisfied.
    /// Called on a deferred timer after config reload, and re-schedules itself
    /// if items remain pending.
    async fn start_pending_items(&mut self) {
        let dep_map = self.build_dep_map();
        let order = match topological_sort(&dep_map) {
            Ok(o) => o,
            Err(_) => return,
        };

        let mut started_any = false;
        for name in &order {
            let is_pending_svc = self
                .service_states
                .get(name)
                .is_some_and(|s| *s == ServiceState::Pending);
            let is_pending_task = self
                .task_states
                .get(name)
                .is_some_and(|s| *s == TaskItemState::Pending);
            if !is_pending_svc && !is_pending_task {
                continue;
            }

            let deps = dep_map.get(name).cloned().unwrap_or_default();
            let deps_ok = deps.iter().all(|dep| {
                self.service_states
                    .get(dep)
                    .is_some_and(|s| s.is_satisfied())
                    || self
                        .task_states
                        .get(dep)
                        .is_some_and(|s| s.is_satisfied())
            });
            if !deps_ok {
                continue;
            }

            if is_pending_svc {
                if self.config.services.contains_key(name)
                    && let Some(done_tx) = self.done_tx.clone()
                {
                    let _ = self.start_service(name, done_tx).await;
                    started_any = true;
                }
            } else if is_pending_task {
                self.task_states
                    .insert(name.clone(), TaskItemState::Running);
                self.handle_task_rerun(name).await;
                started_any = true;
            }
        }

        // If we started something, schedule another check — the newly-started
        // items might unblock further pending items.
        if started_any {
            let still_pending = self
                .service_states
                .values()
                .any(|s| *s == ServiceState::Pending)
                || self
                    .task_states
                    .values()
                    .any(|s| *s == TaskItemState::Pending);
            if still_pending {
                let cmd_tx = self.cmd_tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    let _ = cmd_tx.send(RunnerCommand::StartPending).await;
                });
            }
        }
    }

    /// Runs the build (if any), stops the old process, starts a new one.
    /// If the build fails, the old process is kept running.
    /// Broadcasts `RebuildComplete` when done.
    async fn handle_rebuild(&mut self, name: &str) {
        let svc = match self.config.services.get(name) {
            Some(s) => s,
            None => {
                self.fail_rebuild(name, &format!("{name}: rebuild requested for unknown service"));
                return;
            }
        };
        let resolved = svc.resolve(self.platform);

        self.output_manager
            .lifecycle_event(&format!("{name}: rebuilding (file changed)"));

        // Build (if any). On failure, keep old process running.
        if let Err(()) = self.run_service_build(name, &resolved).await {
            self.fail_rebuild(name, &format!("{name}: build failed"));
            return;
        }

        // Stop the old service (if running).
        if let Some(handle) = self.service_handles.remove(name) {
            self.service_states
                .insert(name.to_string(), ServiceState::Stopping);
            let shutdown_config = resolved.shutdown.as_ref();
            if let Err(e) = stop_service(handle, shutdown_config, false).await {
                self.output_manager
                    .error_event(&format!("{name}: stop failed during rebuild: {e}"));
            }
        }

        // Start the service again. Sockets are already bound (don holds them).
        let _ = self
            .spawn_and_wire_service(name, &resolved, None)
            .await;
    }

    /// Look up a service by name, distinguishing tasks from unknown names.
    fn lookup_service(&self, name: &str) -> Result<&crate::config::Service, CommandError> {
        if let Some(svc) = self.config.services.get(name) {
            return Ok(svc);
        }
        if self.config.tasks.contains_key(name) {
            return Err(CommandError::NotAService {
                name: name.to_string(),
            });
        }
        Err(CommandError::UnknownService {
            name: name.to_string(),
        })
    }

    /// Handle an API-initiated Start command.
    async fn handle_start_cmd(&mut self, name: &str) -> CommandResult {
        let svc = self.lookup_service(name)?;
        // Block if the service is currently active.
        if self.service_handles.contains_key(name) {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "already running".to_string(),
            });
        }
        let resolved = svc.resolve(self.platform);
        self.output_manager
            .lifecycle_event(&format!("starting {name}... (requested)"));
        self.spawn_and_wire_service(name, &resolved, None)
            .await
            .map_err(|e| CommandError::Failed {
                name: name.to_string(),
                message: e.to_string(),
            })
    }

    /// Handle an API-initiated Stop command.
    async fn handle_stop_cmd(&mut self, name: &str) -> CommandResult {
        let resolved = self.lookup_service(name)?.resolve(self.platform);
        let handle = self.service_handles.remove(name).ok_or_else(|| {
            CommandError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            }
        })?;
        self.service_states
            .insert(name.to_string(), ServiceState::Stopping);
        self.output_manager
            .lifecycle_event(&format!("stopping {name}... (requested)"));
        let shutdown_config = resolved.shutdown.as_ref();
        if let Err(e) = stop_service(handle, shutdown_config, false).await {
            return Err(CommandError::Failed {
                name: name.to_string(),
                message: e.to_string(),
            });
        }
        self.service_states
            .insert(name.to_string(), ServiceState::Stopped);
        let _ = self.event_tx.send(RunnerEvent::ServiceStateChanged {
            name: name.to_string(),
            state: ServiceState::Stopped,
        });
        Ok(())
    }

    /// Handle an API-initiated Restart command: stop then start.
    async fn handle_restart_cmd(&mut self, name: &str) -> CommandResult {
        // If running, stop first (ignore not-running error).
        match self.handle_stop_cmd(name).await {
            Ok(()) => {}
            Err(CommandError::InvalidState { .. }) => {}
            Err(e) => return Err(e),
        }
        self.handle_start_cmd(name).await
    }

    /// Handle a file-watch-triggered task re-run.
    async fn handle_task_rerun(&mut self, name: &str) {
        let task_cfg = match self.config.tasks.get(name) {
            Some(t) => t.clone(),
            None => {
                self.output_manager
                    .error_event(&format!("{name}: rerun requested for unknown task"));
                let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                    name: name.to_string(),
                    success: false,
                });
                return;
            }
        };

        let base_dir = task_cfg.dir.as_deref().unwrap_or(&self.base_dir);
        let needs_run = self
            .task_state
            .needs_run(name, &task_cfg.watch, Some(base_dir))
            .await
            .unwrap_or(true);

        if !needs_run {
            self.output_manager
                .lifecycle_event(&format!("{name}: rerun skipped (no changes)"));
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        // If the task has opted out of auto-reruns, mark it pending and don't spawn.
        // The user will need to trigger a manual rerun when they're ready.
        if !task_cfg.auto_rerun {
            self.task_states
                .insert(name.to_string(), TaskItemState::PendingRerun);
            let _ = self.event_tx.send(RunnerEvent::TaskStateChanged {
                name: name.to_string(),
                state: TaskItemState::PendingRerun,
            });
            self.output_manager.lifecycle_event(&format!(
                "{name}: files changed (pending rerun — auto_rerun = false)"
            ));
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        self.output_manager
            .lifecycle_event(&format!("{name}: re-running (file changed)"));
        self.task_states
            .insert(name.to_string(), TaskItemState::Running);

        match task::spawn_task(&task_cfg, name, &self.base_dir, self.platform).await {
            Ok(spawn) => {
                self.wire_task_output_and_wait(name, spawn, &task_cfg, None);
            }
            Err(e) => {
                self.task_states
                    .insert(name.to_string(), TaskItemState::Failed);
                self.output_manager
                    .error_event(&format!("{name}: failed to start: {e}"));
                let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                    name: name.to_string(),
                    success: false,
                });
            }
        }
    }

    /// Handle an item completion notification.
    fn handle_item_done(&mut self, item: &ItemDone) {
        match item.kind {
            NodeKind::Service => self.handle_service_done(item),
            NodeKind::Task => self.handle_task_done(item),
        }
    }

    fn handle_service_done(&mut self, item: &ItemDone) {
        if item.success {
            self.service_states
                .insert(item.name.clone(), ServiceState::Ready);
            if let Some(svc) = self.config.services.get(&item.name) {
                let resolved = svc.resolve(self.platform);
                let ready_desc = match &resolved.ready {
                    Some(r) if r.tcp.is_some() => {
                        format!(" (tcp {})", r.tcp.as_deref().unwrap_or("unknown"))
                    }
                    Some(r) if r.http.is_some() => {
                        format!(" (http {})", r.http.as_deref().unwrap_or("unknown"))
                    }
                    Some(r) if r.exec.is_some() => " (exec)".to_string(),
                    _ => " started".to_string(),
                };
                self.output_manager.lifecycle_event(&format!(
                    "{}{}",
                    item.name,
                    if resolved.ready.is_some() {
                        format!(" ready{ready_desc}")
                    } else {
                        ready_desc
                    }
                ));
            }
        } else {
            self.service_states
                .insert(item.name.clone(), ServiceState::Failed);
            if let Some(ref msg) = item.message {
                self.output_manager
                    .error_event(&format!("{}: {msg}", item.name));
            }
        }
    }

    fn handle_task_done(&mut self, item: &ItemDone) {
        self.running_task_pgids.remove(&item.name);
        let timing = item.elapsed.map(format_duration).unwrap_or_default();

        if item.success {
            let cur = self.task_states.get(&item.name).copied();
            if cur != Some(TaskItemState::Skipped) {
                self.task_states
                    .insert(item.name.clone(), TaskItemState::Completed);
                let msg = if timing.is_empty() {
                    format!("{} complete", item.name)
                } else {
                    format!("{} complete ({timing})", item.name)
                };
                self.output_manager.lifecycle_event(&msg);
            }
        } else {
            self.task_states
                .insert(item.name.clone(), TaskItemState::Failed);
            if let Some(ref err_msg) = item.message {
                let msg = if timing.is_empty() {
                    format!("{} failed ({err_msg})", item.name)
                } else {
                    format!("{} failed ({err_msg}, {timing})", item.name)
                };
                self.output_manager.error_event(&msg);
            }
        }
    }

    /// Initiate graceful shutdown of all services.
    async fn initiate_shutdown(&mut self) {
        self.output_manager
            .lifecycle_event("shutting down gracefully... (Ctrl+C again to force)");

        // Tell the API server to stop accepting connections.
        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(true);
        }

        // Build reverse dependency order for shutdown.
        // Services at the same depth (no dependency relationship) stop concurrently.
        let dep_map = self.build_dep_map();
        let order = topological_sort(&dep_map).unwrap_or_default();

        // Compute depth of each service node for grouping.
        let mut depths: HashMap<String, usize> = HashMap::new();
        for name in &order {
            let node_deps = dep_map.get(name).cloned().unwrap_or_default();
            let max_dep_depth = node_deps
                .iter()
                .filter_map(|d| depths.get(d))
                .max()
                .copied()
                .unwrap_or(0);
            let depth = if node_deps.is_empty() {
                0
            } else {
                max_dep_depth + 1
            };
            depths.insert(name.clone(), depth);
        }

        // Group running services by depth, then iterate from highest depth
        // (most dependent) to lowest (least dependent).
        let mut by_depth: std::collections::BTreeMap<usize, Vec<String>> =
            std::collections::BTreeMap::new();
        for name in &order {
            if !self.config.services.contains_key(name) {
                continue;
            }
            let state = self.service_states.get(name).copied();
            if !matches!(
                state,
                Some(ServiceState::Running)
                    | Some(ServiceState::Ready)
                    | Some(ServiceState::Starting)
            ) {
                continue;
            }
            let depth = depths.get(name).copied().unwrap_or(0);
            by_depth.entry(depth).or_default().push(name.clone());
        }

        let mut remaining: usize = by_depth.values().map(|v| v.len()).sum();
        let mut force_announced = false;

        // Stop from highest depth to lowest (dependents first).
        for (_depth, names) in by_depth.into_iter().rev() {
            for name in &names {
                self.output_manager
                    .lifecycle_event(&format!("stopping {name}... ({remaining} remaining)"));
            }

            // Collect handles and configs for this depth level.
            let mut join_set: JoinSet<String> = JoinSet::new();
            for name in &names {
                if let Some(handle) = self.service_handles.remove(name) {
                    let svc = self.config.services.get(name);
                    let resolved = svc.map(|s| s.resolve(self.platform));
                    let shutdown_config = resolved.as_ref().and_then(|r| r.shutdown.clone());
                    let force = SIGNAL_COUNT.load(Ordering::SeqCst) >= 2;
                    if force && !force_announced {
                        self.output_manager
                            .lifecycle_event("forcing immediate shutdown");
                        force_announced = true;
                    }
                    let name_owned = name.clone();
                    join_set.spawn(async move {
                        let _ = stop_service(handle, shutdown_config.as_ref(), force).await;
                        name_owned
                    });
                }
            }

            // Stop all services at this depth concurrently, emitting
            // "stopped" for each one as it finishes.
            while let Some(result) = join_set.join_next().await {
                if let Ok(name) = result {
                    self.service_states
                        .insert(name.clone(), ServiceState::Stopped);
                    remaining -= 1;
                    self.output_manager
                        .lifecycle_event(&format!("{name} stopped ({remaining} remaining)"));
                }
            }
        }

        // Kill any still-running task process groups.
        if !self.running_task_pgids.is_empty() {
            self.output_manager.lifecycle_event(&format!(
                "killing {} running task{}",
                self.running_task_pgids.len(),
                if self.running_task_pgids.len() == 1 { "" } else { "s" }
            ));
            for (name, pgid) in self.running_task_pgids.drain() {
                if let Err(e) = nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(pgid),
                    nix::sys::signal::Signal::SIGKILL,
                ) {
                    // ESRCH = already dead, which is fine.
                    if e != nix::Error::ESRCH {
                        self.output_manager
                            .error_event(&format!("{name}: failed to kill task pgid {pgid}: {e}"));
                    }
                }
            }
        }

        self.output_manager.lifecycle_event("shutdown complete");
    }

    /// Wait for remaining async tasks to finish after shutdown.
    async fn wait_for_shutdown(&mut self) {
        // All handles should already be stopped by initiate_shutdown.
        // Drop remaining handles to release PID files.
        self.service_handles.clear();
        // Release bound sockets (closes listening ports).
        self.bound_sockets.clear();
    }

    /// Collect status of all items.
    fn collect_status(&self) -> Vec<ItemStatus> {
        let mut statuses = Vec::new();
        for (name, state) in &self.service_states {
            statuses.push(ItemStatus::Service {
                name: name.clone(),
                state: *state,
            });
        }
        for (name, state) in &self.task_states {
            statuses.push(ItemStatus::Task {
                name: name.clone(),
                state: *state,
            });
        }
        statuses
    }
}

/// Install signal handlers for SIGINT and SIGTERM.
///
/// Returns a receiver that gets a message on each signal. Pass this to `Runner::new()`.
/// First signal triggers graceful shutdown. Second signal sets the force-shutdown flag
/// (checked by `initiate_shutdown` via `SIGNAL_COUNT`).
pub async fn install_signal_handlers() -> Result<mpsc::Receiver<()>, std::io::Error> {
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let (tx, rx) = mpsc::channel(2);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = sigint.recv() => {},
                _ = sigterm.recv() => {},
            }

            let prev = SIGNAL_COUNT.fetch_add(1, Ordering::SeqCst);
            // Notify the runner. If the channel is full or closed, that's fine.
            let _ = tx.try_send(());

            if prev >= 1 {
                // Second signal — force flag is set via SIGNAL_COUNT.
                break;
            }
        }
    });

    Ok(rx)
}

/// Format a duration for human display in lifecycle messages.
/// Examples: "0.3s", "1.2s", "2m 15s"
fn format_duration(d: std::time::Duration) -> String {
    let total_secs = d.as_secs_f64();
    if total_secs < 60.0 {
        format!("{total_secs:.1}s")
    } else {
        let mins = d.as_secs() / 60;
        let secs = d.as_secs() % 60;
        format!("{mins}m {secs}s")
    }
}

/// Completion notification from a spawned item.
struct ItemDone {
    name: String,
    kind: NodeKind,
    success: bool,
    message: Option<String>,
    /// How long the item took (for tasks).
    elapsed: Option<std::time::Duration>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

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
                        assert!(
                            positions[dep.as_str()] < positions[name.as_str()],
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
}
