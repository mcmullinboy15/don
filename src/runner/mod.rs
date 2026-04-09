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
    /// Proxy is bound and accepting connections, but the service process is not
    /// started yet. Will transition to Starting on first incoming connection.
    Lazy,
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
        matches!(self, Self::Ready | Self::Lazy)
    }


    /// Valid transitions from one state to another.
    #[cfg(test)]
    pub(crate) fn can_transition_to(&self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Starting)
                | (Self::Pending, Self::Lazy)
                | (Self::Lazy, Self::Starting)
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
        verbose: bool,
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
    /// Build graph definition files changed (BUILD, package.json, etc.).
    /// Triggers a re-query of the build tool to update watch patterns.
    BuildGraphChanged { name: String },
    /// Retry starting any Pending services/tasks whose deps are now satisfied.
    /// Sent by `handle_config_reload` after a delay so newly-spawned deps
    /// have time to pass their ready checks.
    StartPending,
    /// Request an interactive attach session for a service.
    /// Returns the PTY write handle and a live output receiver, or an error.
    Attach {
        name: String,
        pid: u32,
        reply: oneshot::Sender<Result<AttachSession, CommandError>>,
    },
    /// Release an attach session — return the PTY write handle, clear the lock,
    /// and resume prefixed output.
    Detach {
        name: String,
        pty_write: Option<pty_process::OwnedWritePty>,
    },
    /// Initiate graceful shutdown.
    Shutdown,
}

/// An active attach session returned to the WebSocket handler.
pub struct AttachSession {
    /// The PTY write half for forwarding stdin.
    pub pty_write: pty_process::OwnedWritePty,
    /// Live output receiver (preloaded with ring buffer snapshot).
    pub output_rx: mpsc::Receiver<crate::output::SinkLine>,
}

/// A pending attach waiter — registered when a client wants to attach
/// to a service/task that isn't running yet.
pub(crate) struct AttachWaiter {
    pub(crate) pid: u32,
    pub(crate) reply: oneshot::Sender<Result<AttachSession, CommandError>>,
}

/// Status of a single item (service or task) for status queries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ItemStatus {
    Service {
        name: String,
        state: ServiceState,
        #[serde(skip_serializing_if = "Option::is_none")]
        verbose: Option<VerboseInfo>,
    },
    Task {
        name: String,
        state: TaskItemState,
        #[serde(skip_serializing_if = "Option::is_none")]
        verbose: Option<VerboseInfo>,
    },
}

/// Extended information for verbose status display.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VerboseInfo {
    /// Services/tasks this item depends on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    /// File watch patterns (explicit or resolved from build tool).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub watch: Vec<String>,
    /// Listen addresses (LISTEN_FDS).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listen: Vec<String>,
    /// Proxy listen addresses.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proxy: Vec<String>,
    /// Bazel target (if configured).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bazel_target: Option<String>,
    /// Turbo task (if configured).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turbo_task: Option<String>,
    /// Ready check description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<String>,
    /// Run command.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cmd: Option<String>,
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

/// All per-service runtime state, consolidated into a single struct.
///
/// Each running service gets one `RuntimeService` in `Runner::services`.
/// This replaces the many separate `HashMap<String, _>` fields that previously
/// tracked each aspect of service state independently.
pub(crate) struct RuntimeService {
    /// The fully resolved service config (platform overrides applied once).
    pub resolved: crate::config::service::ResolvedService,
    /// Current lifecycle state (Pending, Starting, Running, Ready, etc.).
    pub state: ServiceState,
    /// Handle to the running process (if spawned).
    pub handle: Option<ServiceHandle>,
    /// OSC query sink for reclaiming PTY write on attach.
    pub osc_sink: Option<crate::output::OscSinkHandle>,
    /// PID of the client holding the interactive attach lock.
    pub attach_lock: Option<u32>,
    /// Pending attach waiter (client waiting for process to start).
    pub attach_waiter: Option<AttachWaiter>,
    /// Bound TCP sockets (LISTEN_FDS) — outlive restarts.
    pub bound_sockets: Option<crate::process::socket::BoundSockets>,
    /// TCP proxy listener — outlives restarts.
    pub proxy: Option<crate::proxy::ServiceProxy>,
    /// Watch paths resolved from build tool queries (bazel/turbo).
    pub resolved_watch_paths: Vec<String>,
    /// Bazel binary path resolved via `bazel cquery --output=files`.
    pub bazel_binary_path: Option<String>,
    /// Whether this service was built during the batch build phase.
    pub batch_built: bool,
}

/// All per-task runtime state, consolidated into a single struct.
///
/// Each task gets one `RuntimeTask` in `Runner::tasks`.
/// This replaces the many separate `HashMap<String, _>` fields that previously
/// tracked each aspect of task state independently.
pub(crate) struct RuntimeTask {
    /// The task config (stored once, no repeated lookups).
    pub config: crate::config::task::Task,
    /// Current lifecycle state (Pending, Running, Completed, etc.).
    pub state: TaskItemState,
    /// Process group ID of the running task (for shutdown kills).
    pub pgid: Option<i32>,
    /// OSC query sink for reclaiming PTY write on attach.
    pub osc_sink: Option<crate::output::OscSinkHandle>,
    /// PID of the client holding the interactive attach lock.
    pub attach_lock: Option<u32>,
    /// Pending attach waiter (client waiting for process to start).
    pub attach_waiter: Option<AttachWaiter>,
    /// Watch paths resolved from build tool queries (bazel/turbo).
    pub resolved_watch_paths: Vec<String>,
}

/// The main runner that orchestrates services and tasks.
pub struct Runner {
    config: Config,
    config_path: PathBuf,
    platform: Platform,
    output_manager: OutputManager,
    base_dir: PathBuf,
    task_state: TaskState,

    /// Consolidated per-service runtime state.
    services: HashMap<String, RuntimeService>,
    /// Consolidated per-task runtime state.
    tasks: HashMap<String, RuntimeTask>,

    /// Receives service names when a lazy service's proxy gets its first connection.
    lazy_start_rx: mpsc::Receiver<String>,
    /// Sender half kept for passing to ServiceProxy::bind.
    lazy_start_tx: mpsc::Sender<String>,

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

    /// Sender for pushing watch pattern updates to the WatchManager.
    /// Used after build tool re-queries to update tier-2 watch patterns.
    watch_update_tx: Option<mpsc::Sender<crate::watch::WatchUpdate>>,

    /// Mutex to serialize Bazel build invocations. Concurrent `bazel build`
    /// commands contend for Bazel's server lock, so we queue them.
    bazel_build_mutex: tokio::sync::Mutex<()>,

    /// Services queued for a batched build-tool rebuild (file watch triggered).
    /// Collected during a short batch window, then flushed as one build command.
    pending_bt_rebuilds: Vec<String>,
    /// Deadline for flushing the pending build-tool rebuild batch.
    /// When this expires, all pending rebuilds are built in one invocation.
    bt_rebuild_deadline: Option<tokio::time::Instant>,

    /// Services/tasks queued for a batched build-graph re-query.
    /// When BUILD/package.json files change, affected items are collected here
    /// and flushed after a short window to avoid redundant concurrent queries.
    pending_graph_requery: Vec<String>,
    /// Deadline for flushing the pending graph re-query batch.
    bt_requery_deadline: Option<tokio::time::Instant>,
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
        profile: Option<&str>,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<Self, RunnerError> {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (event_tx, _) = broadcast::channel(256);
        let (lazy_start_tx, lazy_start_rx) = mpsc::channel(16);

        // Canonicalize base_dir so all downstream path joins produce clean
        // absolute paths (avoids `././app` when base_dir is `.`).
        let base_dir = std::fs::canonicalize(&base_dir).map_err(RunnerError::Io)?;

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

        // Resolve which items to run: all items, or just the profile subset
        // with transitive deps included.
        let active_items: Option<HashSet<String>> = if let Some(profile_name) = profile {
            let prof = config.profiles.get(profile_name).ok_or_else(|| {
                RunnerError::Config(format!("unknown profile '{profile_name}'"))
            })?;
            Some(resolve_profile_items(&config, prof))
        } else {
            None // all items
        };

        let active_services: HashSet<String> = config
            .services
            .keys()
            .filter(|name| active_items.as_ref().is_none_or(|s| s.contains(*name)))
            .cloned()
            .collect();

        let active_tasks: HashSet<String> = config
            .tasks
            .keys()
            .filter(|name| active_items.as_ref().is_none_or(|s| s.contains(*name)))
            .cloned()
            .collect();

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

        // Build consolidated runtime state maps.
        let mut services = HashMap::new();
        for (name, svc) in &config.services {
            if active_services.contains(name) {
                services.insert(
                    name.clone(),
                    RuntimeService {
                        resolved: svc.resolve(platform),
                        state: ServiceState::Pending,
                        handle: None,
                        osc_sink: None,
                        attach_lock: None,
                        attach_waiter: None,
                        bound_sockets: None,
                        proxy: None,
                        resolved_watch_paths: Vec::new(),
                        bazel_binary_path: None,
                        batch_built: false,
                    },
                );
            }
        }

        let mut tasks = HashMap::new();
        for (name, task) in &config.tasks {
            if active_tasks.contains(name) {
                tasks.insert(
                    name.clone(),
                    RuntimeTask {
                        config: task.clone(),
                        state: TaskItemState::Pending,
                        pgid: None,
                        osc_sink: None,
                        attach_lock: None,
                        attach_waiter: None,
                        resolved_watch_paths: Vec::new(),
                    },
                );
            }
        }

        Ok(Self {
            config,
            config_path,
            platform,
            output_manager,
            base_dir,
            task_state,
            services,
            tasks,
            lazy_start_rx,
            lazy_start_tx,
            server_shutdown_tx: None,
            docker_client,
            cmd_tx,
            cmd_rx,
            event_tx,
            done_tx: None,
            shutdown_rx,
            _don_pid_file: Some(don_pid_file),
            watch_update_tx: None,
            bazel_build_mutex: tokio::sync::Mutex::new(()),
            pending_bt_rebuilds: Vec::new(),
            bt_rebuild_deadline: None,
            pending_graph_requery: Vec::new(),
            bt_requery_deadline: None,
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

    fn build_dep_map(&self) -> HashMap<String, Vec<String>> {
        let mut deps = HashMap::new();
        for (name, rs) in &self.services {
            deps.insert(name.clone(), rs.resolved.depends_on.clone());
        }
        for (name, rt) in &self.tasks {
            deps.insert(name.clone(), rt.config.depends_on.clone());
        }
        deps
    }

    /// Look up the attach lock for a service or task by name.
    fn get_attach_lock(&self, name: &str) -> Option<u32> {
        self.services
            .get(name)
            .and_then(|rs| rs.attach_lock)
            .or_else(|| self.tasks.get(name).and_then(|rt| rt.attach_lock))
    }

    /// Get a mutable reference to the OSC sink option for a service or task.
    fn get_osc_sink_mut(&mut self, name: &str) -> Option<&mut Option<crate::output::OscSinkHandle>> {
        if let Some(rs) = self.services.get_mut(name) {
            Some(&mut rs.osc_sink)
        } else if let Some(rt) = self.tasks.get_mut(name) {
            Some(&mut rt.osc_sink)
        } else {
            None
        }
    }

    /// Remove the attach lock for a service or task, returning whether it was set.
    fn remove_attach_lock(&mut self, name: &str) -> bool {
        if let Some(rs) = self.services.get_mut(name) {
            return rs.attach_lock.take().is_some();
        }
        if let Some(rt) = self.tasks.get_mut(name) {
            return rt.attach_lock.take().is_some();
        }
        false
    }

    /// Set the attach lock for a service or task.
    fn set_attach_lock(&mut self, name: &str, pid: u32) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.attach_lock = Some(pid);
        } else if let Some(rt) = self.tasks.get_mut(name) {
            rt.attach_lock = Some(pid);
        }
    }

    /// Check if there is a pending attach waiter for a service or task.
    fn has_attach_waiter(&self, name: &str) -> bool {
        self.services
            .get(name)
            .is_some_and(|rs| rs.attach_waiter.is_some())
            || self
                .tasks
                .get(name)
                .is_some_and(|rt| rt.attach_waiter.is_some())
    }

    /// Take the pending attach waiter for a service or task.
    fn take_attach_waiter(&mut self, name: &str) -> Option<AttachWaiter> {
        if let Some(rs) = self.services.get_mut(name)
            && rs.attach_waiter.is_some()
        {
            return rs.attach_waiter.take();
        }
        if let Some(rt) = self.tasks.get_mut(name) {
            return rt.attach_waiter.take();
        }
        None
    }

    /// Set a pending attach waiter for a service or task.
    fn set_attach_waiter(&mut self, name: &str, waiter: AttachWaiter) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.attach_waiter = Some(waiter);
        } else if let Some(rt) = self.tasks.get_mut(name) {
            rt.attach_waiter = Some(waiter);
        }
    }

    /// Run the orchestrator: start all services and tasks in dependency order.
    ///
    /// This is the main entry point. It:
    /// 1. Builds a topological sort of the dependency graph.
    /// 2. Starts items in parallel as their dependencies become satisfied.
    /// 3. Processes commands from the mpsc channel.
    /// 4. Handles shutdown signals.
    pub async fn run(mut self) -> Result<(), RunnerError> {
        // Warn if .don/ is not in .gitignore.
        check_gitignore(&self.base_dir, &self.output_manager);

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

        // Pre-bind all declared listen ports before starting any services.
        // This catches port conflicts (port already in use, duplicate ports
        // across services) upfront rather than failing mid-startup.
        let listen_service_names: Vec<String> = self
            .services
            .iter()
            .filter(|(_, rs)| !rs.resolved.listen.is_empty())
            .map(|(name, _)| name.clone())
            .collect();
        for name in &listen_service_names {
            if let Err(msg) = self.bind_sockets_if_needed(name) {
                return Err(RunnerError::Config(msg));
            }
        }

        // Pre-bind all proxy listeners. This catches port conflicts upfront
        // and starts the accept loops (connections queue until the service is ready).
        let proxy_service_names: Vec<(String, bool)> = self
            .services
            .iter()
            .filter(|(_, rs)| !rs.resolved.proxy.is_empty())
            .map(|(name, rs)| (name.clone(), rs.resolved.lazy))
            .collect();
        for (name, is_lazy) in &proxy_service_names {
            let proxy_config = match self.services.get(name) {
                Some(rs) => rs.resolved.proxy.clone(),
                None => continue,
            };
            let lazy_tx = if *is_lazy {
                Some(self.lazy_start_tx.clone())
            } else {
                None
            };
            match crate::proxy::ServiceProxy::bind(&proxy_config, lazy_tx, name).await {
                Ok(proxy) => {
                    let addrs: Vec<String> =
                        proxy.listen_addrs().iter().map(|a| a.to_string()).collect();
                    self.output_manager.lifecycle_event(&format!(
                        "{name}: proxy listening on {}",
                        addrs.join(", ")
                    ));
                    if let Some(rs) = self.services.get_mut(name) {
                        rs.proxy = Some(proxy);
                        // Set lazy services to Lazy state (they won't enter the
                        // startup flow until triggered by a connection).
                        if *is_lazy {
                            rs.state = ServiceState::Lazy;
                        }
                    }
                }
                Err(e) => {
                    return Err(RunnerError::Config(format!("{name}: {e}")));
                }
            }
        }

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
        let (watch_update_tx, watch_update_rx) = mpsc::channel(64);
        self.watch_update_tx = Some(watch_update_tx);
        match WatchManager::new(
            &self.config,
            self.platform,
            &self.base_dir,
            &self.config_path,
            self.cmd_tx.clone(),
            self.event_tx.subscribe(),
            watch_update_rx,
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

        // Resolve build tool watch patterns (bazel/turbo) at startup.
        // This queries the build tools concurrently and sends the resolved
        // watch paths to the WatchManager via the update channel.
        let (build_tool_watches, bt_warnings) = self.resolve_build_tool_watches().await;
        for warning in &bt_warnings {
            self.output_manager.error_event(warning);
        }
        if let Some(ref tx) = self.watch_update_tx {
            for (name, (paths, ignore)) in build_tool_watches {
                // Store resolved watch paths on the runtime struct.
                if let Some(rs) = self.services.get_mut(&name) {
                    rs.resolved_watch_paths = paths.clone();
                } else if let Some(rt) = self.tasks.get_mut(&name) {
                    rt.resolved_watch_paths = paths.clone();
                }
                let kind = if self.services.contains_key(&name) {
                    crate::watch::WatchItemKind::Service
                } else {
                    crate::watch::WatchItemKind::Task
                };
                let _ = tx
                    .send(crate::watch::WatchUpdate {
                        name,
                        kind,
                        patterns: paths,
                        ignore_patterns: ignore,
                        base_dir: self.base_dir.clone(),
                    })
                    .await;
            }
        }

        // Run batch builds for build-tool-managed services/tasks.
        // This runs ONE `bazel build` / `turbo run build` with all targets,
        // letting the build tool parallelize internally.
        self.run_batch_builds().await;

        // Resolve Bazel binary paths for services that were successfully built.
        // This uses `bazel cquery --output=files` to find the output binary,
        // so we can run it directly instead of going through `bazel run`.
        self.resolve_bazel_binary_paths().await;

        // Build dependency map and topological order.
        let dep_map = self.build_dep_map();
        let order = topological_sort(&dep_map).map_err(|cycle| RunnerError::Cycle { cycle })?;

        // Channel for item completion notifications. Store the sender on `self`
        // so config reload can reuse it for newly-started services.
        let (done_tx, mut done_rx) = mpsc::channel::<ItemDone>(64);
        self.done_tx = Some(done_tx.clone());

        // Track which items are in flight. Only include items that are in the
        // active set (all items, or profile subset). Items not in service_states
        // or task_states are excluded (e.g. services not in the selected profile).
        let mut pending: HashSet<String> = order
            .iter()
            .filter(|name| {
                self.services.contains_key(*name)
                    || self.tasks.contains_key(*name)
            })
            .cloned()
            .collect();
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
                let has_running_services = self.services.values().any(|rs| {
                    matches!(
                        rs.state,
                        ServiceState::Running
                            | ServiceState::Ready
                            | ServiceState::Starting
                            | ServiceState::Lazy
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
                        RunnerCommand::Status { verbose, reply } => {
                            let statuses = self.collect_status(verbose);
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
                        RunnerCommand::Attach { name, pid, reply } => {
                            self.handle_attach_cmd(&name, pid, reply).await;
                        }
                        RunnerCommand::Detach { name, pty_write } => {
                            self.handle_detach(&name, pty_write).await;
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
                        RunnerCommand::BuildGraphChanged { name } => {
                            self.handle_build_graph_changed(&name).await;
                        }
                        RunnerCommand::StartPending => {
                            self.start_pending_items().await;
                        }
                    }
                }
                Some(name) = self.lazy_start_rx.recv() => {
                    if self.services.get(&name).is_some_and(|rs| rs.state == ServiceState::Lazy) {
                        self.output_manager.lifecycle_event(
                            &format!("{name}: first connection — starting service")
                        );
                        self.start_service(&name, done_tx.clone()).await?;
                    }
                }
                // Flush batched build-tool rebuilds when the batch window expires.
                _ = async {
                    match self.bt_rebuild_deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.flush_pending_rebuilds().await;
                }
                // Flush batched build-graph re-queries when the batch window expires.
                _ = async {
                    match self.bt_requery_deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.flush_pending_graph_requery().await;
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
            if let Some(rs) = self.services.get_mut(&name) {
                rs.state = ServiceState::Failed;
            } else if let Some(rt) = self.tasks.get_mut(&name) {
                rt.state = TaskItemState::Failed;
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
            // Skip lazy services — they start on first proxy connection.
            if self.services.get(&name).is_some_and(|rs| rs.state == ServiceState::Lazy) {
                pending.remove(&name);
                continue;
            }

            pending.remove(&name);
            in_flight.insert(name.clone());

            if self.services.contains_key(&name) {
                self.start_service(&name, done_tx.clone()).await?;
            } else if self.tasks.contains_key(&name) {
                self.start_task(&name, done_tx.clone()).await?;
            }
        }

        Ok(())
    }

    /// Check if a dependency is satisfied (ready service or completed task).
    fn is_dep_satisfied(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return rs.state.is_satisfied();
        }
        if let Some(rt) = self.tasks.get(dep) {
            return rt.state.is_satisfied();
        }
        false
    }

    /// Check if a dependency has failed.
    fn is_dep_failed(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return rs.state == ServiceState::Failed;
        }
        if let Some(rt) = self.tasks.get(dep) {
            return rt.state == TaskItemState::Failed;
        }
        false
    }

    /// Start a service: bind sockets, build, spawn, wire output + ready check.
    async fn start_service(
        &mut self,
        name: &str,
        done_tx: mpsc::Sender<ItemDone>,
    ) -> Result<(), RunnerError> {
        if let Some(rs) = self.services.get_mut(name) {
            rs.state = ServiceState::Starting;
        } else {
            return Err(RunnerError::Config(format!("unknown service: {name}")));
        }

        let mut resolved = match self.services.get(name) {
            Some(rs) => rs.resolved.clone(),
            None => return Err(RunnerError::Config(format!("unknown service: {name}"))),
        };

        self.output_manager
            .lifecycle_event(&format!("starting {name}..."));

        // Phase 1: Bind listen sockets (idempotent — skips if already bound).
        // If the service has proxy entries, bind LISTEN_FDS sockets for the
        // ephemeral ports (for entries without env vars).
        let has_proxy = self.services.get(name).is_some_and(|rs| rs.proxy.is_some());
        if has_proxy {
            let (proxy_env, listen_fds_addrs) = {
                let rs = match self.services.get(name) {
                    Some(rs) => rs,
                    None => return Ok(()),
                };
                let proxy = match rs.proxy.as_ref() {
                    Some(p) => p,
                    None => return Ok(()),
                };
                (proxy.env_vars(), proxy.listen_fds_addrs())
            };
            // Inject proxy env vars (for entries with env: Some("PORT")).
            resolved.env.extend(proxy_env);
            // For LISTEN_FDS mode entries, use the proxy's ephemeral addresses.
            if !listen_fds_addrs.is_empty() {
                // Replace any previously bound sockets with new ephemeral ones.
                if let Some(rs) = self.services.get_mut(name) {
                    rs.bound_sockets = None;
                }
                match crate::process::socket::bind_sockets(&listen_fds_addrs) {
                    Ok(sockets) => {
                        if let Some(rs) = self.services.get_mut(name) {
                            rs.bound_sockets = Some(sockets);
                        }
                    }
                    Err(e) => {
                        self.fail_service_start(
                            name,
                            &format!("failed to bind ephemeral sockets: {e}"),
                            done_tx,
                        )
                        .await;
                        return Ok(());
                    }
                }
            }
        } else if let Err(msg) = self.bind_sockets_if_needed(name) {
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
    ) -> Result<(), String> {
        let listen = match self.services.get(name) {
            Some(rs) if !rs.resolved.listen.is_empty() && rs.bound_sockets.is_none() => {
                rs.resolved.listen.clone()
            }
            _ => return Ok(()),
        };
        match crate::process::socket::bind_sockets(&listen) {
            Ok(sockets) => {
                self.output_manager.lifecycle_event(&format!(
                    "{name}: bound {} listen socket{}",
                    sockets.len(),
                    if sockets.len() == 1 { "" } else { "s" }
                ));
                if let Some(rs) = self.services.get_mut(name) {
                    rs.bound_sockets = Some(sockets);
                }
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
        // Skip build for services already built in the batch phase.
        if self.services.get(name).is_some_and(|rs| rs.batch_built) {
            return Ok(());
        }

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
        let sockets = self.services.get(name).and_then(|rs| rs.bound_sockets.as_ref());
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
                ).await;
                // Fulfill any pending attach waiter now that the handle is stored.
                self.fulfill_pending_waiter(name).await;
                Ok(())
            }
            Err(e) => {
                if let Some(rs) = self.services.get_mut(name) {
                    rs.state = ServiceState::Failed;
                }
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
        if let Some(rs) = self.services.get_mut(name) {
            rs.state = ServiceState::Failed;
        }
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
    async fn wire_service_output_and_ready_check(
        &mut self,
        name: &str,
        start_result: service::StartResult,
        resolved: &crate::config::ResolvedService,
        done_tx: Option<mpsc::Sender<ItemDone>>,
    ) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.state = ServiceState::Running;
            rs.handle = Some(start_result.handle);

            // Add OSC response sink if we have a PTY write handle.
            if let Some(ServiceHandle::Process(process)) = rs.handle.as_mut()
                && let Some(pty) = process.take_pty_write()
                && let Some(osc_handle) = self.output_manager.add_osc_sink(name, pty).await
            {
                rs.osc_sink = Some(osc_handle);
            }
        }

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
        // Expand ${VAR} in ready check fields (tcp, http) so proxy-injected
        // vars like CRDB_PORT resolve to the actual ephemeral port.
        let ready_config = resolved.ready.clone().map(|mut r| {
            if let Some(ref tcp) = r.tcp {
                r.tcp = Some(service::expand_env_vars(tcp, &resolved.env));
            }
            if let Some(ref http) = r.http {
                r.http = Some(service::expand_env_vars(http, &resolved.env));
            }
            r
        });
        let event_tx = self.event_tx.clone();
        let proxy_handle = self
            .services
            .get(name)
            .and_then(|rs| rs.proxy.as_ref())
            .map(|p| p.backend_handle());

        // For proxy services, activate the backend immediately so the proxy
        // can start forwarding. The proxy has connection-level retry with
        // backoff, so it handles the case where the service isn't listening yet.
        if proxy_handle.is_some()
            && let Some(rs) = self.services.get(name)
            && let Some(ref proxy) = rs.proxy
        {
            proxy.set_backend();
        }

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

                // Activate proxy backend once the service is ready.
                if success
                    && let Some(ref handle) = proxy_handle
                {
                    handle.activate();
                }

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
        } else if let Some(done_tx) = done_tx {
            // No ready check, initial startup path — mark ready immediately.
            if let Some(rs) = self.services.get_mut(name) {
                rs.state = ServiceState::Ready;
            }
            self.output_manager
                .lifecycle_event(&format!("{name} started"));
            let _ = done_tx
                .send(ItemDone {
                    name: name.to_string(),
                    kind: NodeKind::Service,
                    success: true,
                    message: None,
                    elapsed: None,
                })
                .await;
        } else {
            // No ready check, rebuild path — mark ready immediately.
            if let Some(rs) = self.services.get_mut(name) {
                rs.state = ServiceState::Ready;
            }
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
        let task_cfg = match self.tasks.get(name) {
            Some(rt) => rt.config.clone(),
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
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.state = TaskItemState::Skipped;
            }
            self.output_manager
                .lifecycle_event(&format!("{name}: skipped (no changes)"));
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
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.state = TaskItemState::Failed;
            }
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

        if let Some(rt) = self.tasks.get_mut(name) {
            rt.state = TaskItemState::Running;
        }

        // Spawn the task process.
        match task::spawn_task(&task_cfg, name, &self.base_dir, self.platform).await {
            Ok(spawn) => {
                self.wire_task_output_and_wait(name, spawn, &task_cfg, Some(done_tx)).await;
                Ok(())
            }
            Err(e) => {
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.state = TaskItemState::Failed;
                }
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
    async fn wire_task_output_and_wait(
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

        let pgid = handle.pgid();

        // Add OSC response sink if we have a PTY write handle.
        if let Some(pty) = handle.take_pty_write()
            && let Some(osc_handle) = self.output_manager.add_osc_sink(name, pty).await
            && let Some(rt) = self.tasks.get_mut(name)
        {
            rt.osc_sink = Some(osc_handle);
        }

        if let Some(rt) = self.tasks.get_mut(name) {
            rt.pgid = Some(pgid);
        }

        // Fulfill any pending attach waiter for this task.
        self.fulfill_pending_waiter(name).await;

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

        let work_dir = match resolved.dir.as_deref() {
            Some(d) => self.base_dir.join(d),
            None => self.base_dir.clone(),
        };
        let work_dir = work_dir.as_path();
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
                // Pipe build output through [don] lifecycle events so it's
                // visually distinct from the service's own output.
                let om = self.output_manager.clone_lifecycle_emitter();
                let build_name = name.to_string();
                tokio::spawn(async move {
                    let mut reader = tokio::io::BufReader::new(child_output);
                    let mut line_buf = Vec::new();
                    loop {
                        line_buf.clear();
                        match tokio::io::AsyncBufReadExt::read_until(
                            &mut reader, b'\n', &mut line_buf,
                        ).await {
                            Ok(0) => break,
                            Ok(_) => {
                                if line_buf.last() == Some(&b'\n') { line_buf.pop(); }
                                if line_buf.last() == Some(&b'\r') { line_buf.pop(); }
                                let text = String::from_utf8_lossy(&line_buf);
                                om.lifecycle_event(&format!("{build_name}: {text}"));
                            }
                            Err(e) if e.raw_os_error() == Some(libc::EIO) => break,
                            Err(_) => break,
                        }
                    }
                });

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

    /// Flush all pending build-tool rebuilds as a single batch.
    ///
    /// Collects Bazel targets and Turbo filters from the queued services,
    /// runs one build per tool, then restarts each affected service.
    async fn flush_pending_rebuilds(&mut self) {
        let names = std::mem::take(&mut self.pending_bt_rebuilds);
        self.bt_rebuild_deadline = None;

        if names.is_empty() {
            return;
        }

        // Partition into Bazel and Turbo groups.
        // Bazel: (service_name, target, query_timeout)
        let mut bazel_items: Vec<(String, String)> = Vec::new();
        let mut bazel_timeout: Option<u64> = None;
        // Turbo: grouped by build_task → (service_name, filter)
        let mut turbo_by_task: HashMap<String, Vec<(String, String)>> = HashMap::new();
        let mut turbo_timeout: Option<u64> = None;
        // Services without a build tool target (shouldn't happen, but handle gracefully)
        let mut plain_rebuilds: Vec<String> = Vec::new();

        for name in &names {
            if let Some(rs) = self.services.get(name) {
                if let Some(ref bazel) = rs.resolved.bazel {
                    bazel_items.push((name.clone(), bazel.target.clone()));
                    if let Some(t) = bazel.query_timeout {
                        bazel_timeout = Some(bazel_timeout.map_or(t, |cur: u64| cur.max(t)));
                    }
                } else if let Some(ref turbo) = rs.resolved.turbo {
                    let build_task = turbo
                        .build_task
                        .clone()
                        .unwrap_or_else(|| "build".to_string());
                    if !build_task.is_empty()
                        && let Some(ref filter) = turbo.filter
                    {
                        turbo_by_task
                            .entry(build_task)
                            .or_default()
                            .push((name.clone(), filter.clone()));
                        if let Some(t) = turbo.query_timeout {
                            turbo_timeout =
                                Some(turbo_timeout.map_or(t, |cur: u64| cur.max(t)));
                        }
                    } else {
                        plain_rebuilds.push(name.clone());
                    }
                } else {
                    plain_rebuilds.push(name.clone());
                }
            }
        }

        // Track which services succeeded the build.
        let mut build_succeeded: HashSet<String> = HashSet::new();

        // Run Bazel batch build.
        if !bazel_items.is_empty() {
            let _guard = self.bazel_build_mutex.lock().await;
            let targets: Vec<String> = bazel_items.iter().map(|(_, t)| t.clone()).collect();
            let target_to_names: HashMap<String, Vec<String>> = {
                let mut m: HashMap<String, Vec<String>> = HashMap::new();
                for (name, target) in &bazel_items {
                    m.entry(target.clone()).or_default().push(name.clone());
                }
                m
            };

            let count = targets.len();
            self.output_manager.lifecycle_event(&format!(
                "bazel: rebuilding {count} target{}...",
                if count == 1 { "" } else { "s" }
            ));

            let om = self.output_manager.clone_lifecycle_emitter();
            let resolver = crate::build_tool::bazel::BazelResolver::new(bazel_timeout);
            match resolver
                .build_targets(&targets, &self.base_dir.clone(), move |line| {
                    om.lifecycle_event(&format!("bazel: {line}"));
                })
                .await
            {
                Ok(result) => {
                    for target in &result.succeeded {
                        if let Some(svc_names) = target_to_names.get(target) {
                            for n in svc_names {
                                build_succeeded.insert(n.clone());
                            }
                        }
                    }
                    for (target, msg) in &result.failed {
                        if let Some(svc_names) = target_to_names.get(target) {
                            for n in svc_names {
                                self.fail_rebuild(n, &format!("{n}: bazel build failed: {msg}"));
                            }
                        }
                    }
                }
                Err(e) => {
                    for (name, _) in &bazel_items {
                        self.fail_rebuild(name, &format!("{name}: bazel build error: {e}"));
                    }
                }
            }
        }

        // Run Turbo batch builds (one per build_task).
        for (build_task, items) in &turbo_by_task {
            let filters: Vec<String> = items.iter().map(|(_, f)| f.clone()).collect();
            let filter_to_names: HashMap<String, Vec<String>> = {
                let mut m: HashMap<String, Vec<String>> = HashMap::new();
                for (name, filter) in items {
                    m.entry(filter.clone()).or_default().push(name.clone());
                }
                m
            };

            let count = filters.len();
            self.output_manager.lifecycle_event(&format!(
                "turbo: rebuilding '{build_task}' for {count} package{}...",
                if count == 1 { "" } else { "s" }
            ));

            let om = self.output_manager.clone_lifecycle_emitter();
            let bt = build_task.clone();
            let resolver = crate::build_tool::turbo::TurboResolver::new(
                build_task,
                None,
                turbo_timeout,
            );
            match resolver
                .build_packages(&bt, &filters, &self.base_dir.clone(), move |line| {
                    om.lifecycle_event(&format!("turbo: {line}"));
                })
                .await
            {
                Ok(result) => {
                    for filter in &result.succeeded {
                        if let Some(svc_names) = filter_to_names.get(filter) {
                            for n in svc_names {
                                build_succeeded.insert(n.clone());
                            }
                        }
                    }
                    for (filter, msg) in &result.failed {
                        if let Some(svc_names) = filter_to_names.get(filter) {
                            for n in svc_names {
                                self.fail_rebuild(n, &format!("{n}: turbo build failed: {msg}"));
                            }
                        }
                    }
                }
                Err(e) => {
                    for (name, _) in items {
                        self.fail_rebuild(name, &format!("{name}: turbo build error: {e}"));
                    }
                }
            }
        }

        // Restart all services whose builds succeeded.
        for name in &build_succeeded {
            self.do_rebuild(name).await;
        }

        // Handle any plain (non-build-tool) services that ended up in the queue.
        for name in &plain_rebuilds {
            self.do_rebuild(name).await;
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
            if let Some(rs) = self.services.get_mut(name)
                && let Some(handle) = rs.handle.take()
            {
                if rs.attach_lock.take().is_some() {
                    self.output_manager.resume_stdout_sink(name).await;
                }
                let shutdown_config = rs.resolved.shutdown.clone();
                rs.state = ServiceState::Stopping;
                let _ = service::stop_service(handle, shutdown_config.as_ref(), false).await;
                if let Some(writer) = self.output_manager.service_writer(name) {
                    writer.close_follow_sinks().await;
                }
                self.output_manager
                    .lifecycle_event(&format!("{name}: stopped (removed from config)"));
            }
            self.services.remove(name);
        }

        // 2. Stop changed services (they'll be restarted with the new config).
        //    Also release bound sockets so they get re-bound with the new
        //    listen addresses (if changed).
        for name in &diff.changed_services {
            if let Some(rs) = self.services.get_mut(name) {
                if let Some(handle) = rs.handle.take() {
                    if rs.attach_lock.take().is_some() {
                        self.output_manager.resume_stdout_sink(name).await;
                    }
                    let shutdown_config = rs.resolved.shutdown.clone();
                    rs.state = ServiceState::Stopping;
                    let _ = service::stop_service(handle, shutdown_config.as_ref(), false).await;
                    if let Some(writer) = self.output_manager.service_writer(name) {
                        writer.close_follow_sinks().await;
                    }
                }
                rs.bound_sockets = None;
                rs.proxy = None;
            }
        }

        // 3. Remove state for removed tasks.
        for name in &diff.removed_tasks {
            self.tasks.remove(name);
        }

        // 4. Swap config.
        self.config = new_config;

        // 5. Register new services/tasks in runtime maps and output manager.
        for name in &diff.added_services {
            if let Some(svc) = self.config.services.get(name) {
                self.services.insert(
                    name.clone(),
                    RuntimeService {
                        resolved: svc.resolve(self.platform),
                        state: ServiceState::Pending,
                        handle: None,
                        osc_sink: None,
                        attach_lock: None,
                        attach_waiter: None,
                        bound_sockets: None,
                        proxy: None,
                        resolved_watch_paths: Vec::new(),
                        bazel_binary_path: None,
                        batch_built: false,
                    },
                );
            }
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
            if let Some(task) = self.config.tasks.get(name) {
                self.tasks.insert(
                    name.clone(),
                    RuntimeTask {
                        config: task.clone(),
                        state: TaskItemState::Pending,
                        pgid: None,
                        osc_sink: None,
                        attach_lock: None,
                        attach_waiter: None,
                        resolved_watch_paths: Vec::new(),
                    },
                );
            }
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

        // 6. Mark changed services as Pending for restart, re-resolve config,
        //    and bind proxies for new/changed services that have proxy config.
        for name in &diff.changed_services {
            if let Some(svc) = self.config.services.get(name)
                && let Some(rs) = self.services.get_mut(name)
            {
                rs.resolved = svc.resolve(self.platform);
                rs.state = ServiceState::Pending;
            }
        }
        // Also update task configs for changed tasks.
        for name in &diff.changed_tasks {
            if let Some(task) = self.config.tasks.get(name)
                && let Some(rt) = self.tasks.get_mut(name)
            {
                rt.config = task.clone();
            }
        }
        let proxy_names: Vec<String> = diff
            .added_services
            .iter()
            .chain(diff.changed_services.iter())
            .cloned()
            .collect();
        for name in &proxy_names {
            let (proxy_config, is_lazy) = match self.services.get(name) {
                Some(rs) if !rs.resolved.proxy.is_empty() => {
                    (rs.resolved.proxy.clone(), rs.resolved.lazy)
                }
                _ => continue,
            };
            let lazy_tx = if is_lazy {
                Some(self.lazy_start_tx.clone())
            } else {
                None
            };
            match crate::proxy::ServiceProxy::bind(&proxy_config, lazy_tx, name).await {
                Ok(proxy) => {
                    let addrs: Vec<String> =
                        proxy.listen_addrs().iter().map(|a| a.to_string()).collect();
                    self.output_manager.lifecycle_event(&format!(
                        "{name}: proxy listening on {}",
                        addrs.join(", ")
                    ));
                    if let Some(rs) = self.services.get_mut(name) {
                        rs.proxy = Some(proxy);
                        if is_lazy {
                            rs.state = ServiceState::Lazy;
                        }
                    }
                }
                Err(e) => {
                    self.output_manager
                        .error_event(&format!("{name}: proxy bind failed: {e}"));
                }
            }
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
                    let deps_ok = deps.iter().all(|dep| self.is_dep_satisfied(dep));
                    if !deps_ok {
                        self.output_manager.lifecycle_event(&format!(
                            "{name}: waiting for dependencies"
                        ));
                        continue;
                    }
                    // Service or task?
                    if self.services.contains_key(name) {
                        // Use the full start_service flow (bind sockets, download,
                        // build, spawn, wire output + ready check, lifecycle events).
                        if let Some(done_tx) = self.done_tx.clone() {
                            let _ = self.start_service(name, done_tx).await;
                        }
                    } else if self.tasks.contains_key(name) {
                        if let Some(rt) = self.tasks.get_mut(name) {
                            rt.state = TaskItemState::Running;
                        }
                        self.handle_task_rerun(name).await;
                    }
                }
            }
        }

        // If any services/tasks are still Pending (deps not yet Ready),
        // schedule a deferred retry. Their deps were just spawned and need
        // time to pass ready checks before we can start the dependents.
        let has_pending = self
            .services
            .values()
            .any(|rs| rs.state == ServiceState::Pending)
            || self
                .tasks
                .values()
                .any(|rt| rt.state == TaskItemState::Pending);
        if has_pending {
            let cmd_tx = self.cmd_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let _ = cmd_tx.send(RunnerCommand::StartPending).await;
            });
        }
    }

    /// Handle a build graph change event (BUILD files, package.json, etc. changed).
    ///
    /// Queues the item for a batched re-query instead of spawning immediately.
    /// This prevents redundant concurrent queries when a single BUILD file
    /// change affects multiple services.
    async fn handle_build_graph_changed(&mut self, name: &str) {
        if !self.pending_graph_requery.contains(&name.to_string()) {
            self.pending_graph_requery.push(name.to_string());
        }
        self.bt_requery_deadline =
            Some(tokio::time::Instant::now() + std::time::Duration::from_millis(100));
    }

    /// Flush all pending build-graph re-queries.
    ///
    /// Runs build tool queries for each queued item and sends updated watch
    /// patterns to the WatchManager. Uses stale-while-revalidate: old watch
    /// patterns remain active during the re-query.
    async fn flush_pending_graph_requery(&mut self) {
        let names = std::mem::take(&mut self.pending_graph_requery);
        self.bt_requery_deadline = None;

        if names.is_empty() {
            return;
        }

        use crate::build_tool::BuildGraphResolver;

        let watch_update_tx = match self.watch_update_tx.clone() {
            Some(tx) => tx,
            None => return,
        };

        self.output_manager.lifecycle_event(&format!(
            "re-querying build tool for {} item{}...",
            names.len(),
            if names.len() == 1 { "" } else { "s" }
        ));

        for name in &names {
            let (bazel_cfg, turbo_cfg, item_dir, ignore_patterns) =
                if let Some(rs) = self.services.get(name) {
                    (
                        rs.resolved.bazel.clone(),
                        rs.resolved.turbo.clone(),
                        rs.resolved.dir.clone(),
                        rs.resolved.ignore.clone(),
                    )
                } else if let Some(rt) = self.tasks.get(name) {
                    (
                        rt.config.bazel.clone(),
                        rt.config.turbo.clone(),
                        rt.config.dir.clone(),
                        rt.config.ignore.clone(),
                    )
                } else {
                    continue;
                };

            let working_dir = match item_dir {
                Some(d) => self.base_dir.join(d),
                None => self.base_dir.clone(),
            };

            let result = if let Some(ref bazel) = bazel_cfg {
                let resolver =
                    crate::build_tool::bazel::BazelResolver::new(bazel.query_timeout);
                resolver.resolve(&bazel.target, &working_dir).await
            } else if let Some(ref turbo) = turbo_cfg {
                let resolver = crate::build_tool::turbo::TurboResolver::new(
                    &turbo.task,
                    turbo.filter.as_deref(),
                    turbo.query_timeout,
                );
                resolver.resolve(&turbo.task, &working_dir).await
            } else {
                continue;
            };

            match result {
                Ok(info) => {
                    let count = info.watch_paths.len();
                    self.output_manager.lifecycle_event(&format!(
                        "{name}: updated watch paths ({count} path{})",
                        if count == 1 { "" } else { "s" }
                    ));
                    if let Some(rs) = self.services.get_mut(name) {
                        rs.resolved_watch_paths = info.watch_paths.clone();
                    } else if let Some(rt) = self.tasks.get_mut(name) {
                        rt.resolved_watch_paths = info.watch_paths.clone();
                    }
                    let kind = if self.services.contains_key(name) {
                        crate::watch::WatchItemKind::Service
                    } else {
                        crate::watch::WatchItemKind::Task
                    };
                    let _ = watch_update_tx
                        .send(crate::watch::WatchUpdate {
                            name: name.clone(),
                            kind,
                            patterns: info.watch_paths,
                            ignore_patterns,
                            base_dir: self.base_dir.clone(),
                        })
                        .await;
                }
                Err(e) => {
                    self.output_manager.error_event(&format!(
                        "{name}: build tool re-query failed: {e} — keeping existing watch patterns"
                    ));
                }
            }
        }
    }

    /// Resolve build tool watch patterns for all services and tasks at startup.
    ///
    /// For each service/task with a `bazel` or `turbo` config, queries the
    /// build tool and injects the resolved watch paths. Returns warnings for
    /// any failures (non-fatal).
    /// Returns (name -> (watch_paths, ignore_patterns), warnings).
    async fn resolve_build_tool_watches(&self) -> (
        HashMap<String, (Vec<String>, Vec<String>)>,
        Vec<String>,
    ) {
        use crate::build_tool::BuildGraphResolver;

        let mut resolved_watches: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
        let mut warnings: Vec<String> = Vec::new();

        // Collect all items that need build tool resolution.
        struct BuildToolItem {
            name: String,
            bazel: Option<crate::config::BazelConfig>,
            turbo: Option<crate::config::TurboConfig>,
            dir: Option<PathBuf>,
            ignore: Vec<String>,
        }
        let mut items: Vec<BuildToolItem> = Vec::new();

        for (name, rs) in &self.services {
            if rs.resolved.bazel.is_some() || rs.resolved.turbo.is_some() {
                items.push(BuildToolItem {
                    name: name.clone(),
                    bazel: rs.resolved.bazel.clone(),
                    turbo: rs.resolved.turbo.clone(),
                    dir: rs.resolved.dir.clone(),
                    ignore: rs.resolved.ignore.clone(),
                });
            }
        }
        for (name, rt) in &self.tasks {
            if rt.config.bazel.is_some() || rt.config.turbo.is_some() {
                items.push(BuildToolItem {
                    name: name.clone(),
                    bazel: rt.config.bazel.clone(),
                    turbo: rt.config.turbo.clone(),
                    dir: rt.config.dir.clone(),
                    ignore: rt.config.ignore.clone(),
                });
            }
        }

        // Resolve each item concurrently.
        let mut join_set = tokio::task::JoinSet::new();
        for BuildToolItem { name, bazel: bazel_cfg, turbo: turbo_cfg, dir: item_dir, ignore } in items {
            let base_dir = self.base_dir.clone();
            let working_dir = match item_dir {
                Some(d) => base_dir.join(d),
                None => base_dir.clone(),
            };

            join_set.spawn(async move {
                let result = if let Some(bazel) = bazel_cfg {
                    let resolver = crate::build_tool::bazel::BazelResolver::new(
                        bazel.query_timeout,
                    );
                    resolver.resolve(&bazel.target, &working_dir).await
                } else if let Some(turbo) = turbo_cfg {
                    let resolver = crate::build_tool::turbo::TurboResolver::new(
                        &turbo.task,
                        turbo.filter.as_deref(),
                        turbo.query_timeout,
                    );
                    resolver.resolve(&turbo.task, &working_dir).await
                } else {
                    return (name, ignore, Err(crate::build_tool::BuildToolError::QueryFailed {
                        tool: "unknown".to_string(),
                        message: "no build tool configured".to_string(),
                    }));
                };
                (name, ignore, result)
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Ok((name, ignore, resolve_result)) = result {
                match resolve_result {
                    Ok(info) => {
                        let count = info.watch_paths.len();
                        self.output_manager.lifecycle_event(
                            &format!("{name}: resolved {count} watch path{} from build tool",
                                if count == 1 { "" } else { "s" })
                        );
                        resolved_watches.insert(name, (info.watch_paths, ignore));
                    }
                    Err(e) => {
                        warnings.push(format!("{name}: build tool query failed: {e}"));
                    }
                }
            }
        }

        (resolved_watches, warnings)
    }

    /// Run batch builds for all configured build tool targets.
    ///
    /// Collects Bazel targets and Turbo tasks from service/task configs,
    /// runs one build command per tool (Bazel and Turbo can run concurrently),
    /// and returns which services had successful builds vs failures.
    ///
    /// Build output is streamed through the OutputManager as lifecycle events.
    /// Resolve Bazel binary paths for all batch-built services.
    ///
    /// After `bazel build`, queries `bazel cquery --output=files` for each
    /// service's target to find the output binary. Services can then be started
    /// by running the binary directly instead of `bazel run`.
    async fn resolve_bazel_binary_paths(&mut self) {
        let mut items: Vec<(String, String, PathBuf)> = Vec::new();
        for (name, rs) in &self.services {
            if !rs.batch_built {
                continue;
            }
            if let Some(ref bazel) = rs.resolved.bazel {
                let working_dir = match rs.resolved.dir.as_deref() {
                    Some(d) => self.base_dir.join(d),
                    None => self.base_dir.clone(),
                };
                items.push((name.clone(), bazel.target.clone(), working_dir));
            }
        }

        for (name, target, working_dir) in items {
            let resolver = crate::build_tool::bazel::BazelResolver::new(None);
            match resolver.resolve_binary_path(&target, &working_dir).await {
                Ok(rel_path) => {
                    // The path is relative to the workspace (e.g. "bazel-out/.../bin/api").
                    // Join with working_dir to make it absolute.
                    let abs_path = working_dir.join(&rel_path);
                    let path_str = abs_path.to_string_lossy().to_string();
                    self.output_manager.lifecycle_event(
                        &format!("{name}: resolved binary {rel_path}")
                    );
                    if let Some(rs) = self.services.get_mut(&name) {
                        rs.bazel_binary_path = Some(path_str.clone());
                        // Also update the resolved config to use the binary directly.
                        if let Some(svc) = self.config.services.get(&name) {
                            rs.resolved = svc.resolve_with_bazel_binary(self.platform, &path_str);
                        }
                    }
                }
                Err(e) => {
                    self.output_manager.error_event(
                        &format!("{name}: could not resolve binary path: {e} — falling back to bazel run")
                    );
                }
            }
        }
    }

    async fn run_batch_builds(&mut self) {
        // Collect Bazel targets: (service_name, bazel_target)
        let mut bazel_targets: Vec<(String, String)> = Vec::new();
        // Collect Turbo filters grouped by build_task: (build_task, vec of (service_name, filter))
        let mut turbo_by_task: std::collections::HashMap<
            String,
            Vec<(String, String)>,
        > = std::collections::HashMap::new();
        let mut bazel_timeout: Option<u64> = None;
        let mut turbo_timeout: Option<u64> = None;

        for (name, rs) in &self.services {
            if let Some(ref bazel) = rs.resolved.bazel {
                bazel_targets.push((name.clone(), bazel.target.clone()));
                if let Some(t) = bazel.query_timeout {
                    bazel_timeout = Some(bazel_timeout.map_or(t, |cur: u64| cur.max(t)));
                }
            } else if let Some(ref turbo) = rs.resolved.turbo {
                let build_task = turbo
                    .build_task
                    .clone()
                    .unwrap_or_else(|| "build".to_string());
                if !build_task.is_empty() {
                    if let Some(ref filter) = turbo.filter {
                        turbo_by_task
                            .entry(build_task)
                            .or_default()
                            .push((name.clone(), filter.clone()));
                    } else {
                        self.output_manager.error_event(&format!(
                            "{name}: turbo.filter is required for batch builds — \
                             skipping batch build for this service"
                        ));
                    }
                }
                if let Some(t) = turbo.query_timeout {
                    turbo_timeout = Some(turbo_timeout.map_or(t, |cur: u64| cur.max(t)));
                }
            }
        }
        for (name, rt) in &self.tasks {
            if let Some(ref bazel) = rt.config.bazel {
                bazel_targets.push((name.clone(), bazel.target.clone()));
                if let Some(t) = bazel.query_timeout {
                    bazel_timeout = Some(bazel_timeout.map_or(t, |cur: u64| cur.max(t)));
                }
            } else if let Some(ref turbo) = rt.config.turbo {
                let build_task = turbo
                    .build_task
                    .clone()
                    .unwrap_or_else(|| "build".to_string());
                if !build_task.is_empty() {
                    if let Some(ref filter) = turbo.filter {
                        turbo_by_task
                            .entry(build_task)
                            .or_default()
                            .push((name.clone(), filter.clone()));
                    } else {
                        self.output_manager.error_event(&format!(
                            "{name}: turbo.filter is required for batch builds — \
                             skipping batch build for this task"
                        ));
                    }
                }
                if let Some(t) = turbo.query_timeout {
                    turbo_timeout = Some(turbo_timeout.map_or(t, |cur: u64| cur.max(t)));
                }
            }
        }

        if bazel_targets.is_empty() && turbo_by_task.is_empty() {
            return;
        }

        // Run Bazel and Turbo builds concurrently (different tools, no contention).
        let mut join_set = JoinSet::new();

        if !bazel_targets.is_empty() {
            let targets: Vec<String> = bazel_targets.iter().map(|(_, t)| t.clone()).collect();
            let target_to_name: std::collections::HashMap<String, Vec<String>> = {
                let mut m: std::collections::HashMap<String, Vec<String>> =
                    std::collections::HashMap::new();
                for (name, target) in &bazel_targets {
                    m.entry(target.clone()).or_default().push(name.clone());
                }
                m
            };
            let count = targets.len();
            let base_dir = self.base_dir.clone();
            let om = self.output_manager.clone_lifecycle_emitter();

            self.output_manager.lifecycle_event(&format!(
                "bazel: building {count} target{}...",
                if count == 1 { "" } else { "s" }
            ));

            join_set.spawn(async move {
                let resolver =
                    crate::build_tool::bazel::BazelResolver::new(bazel_timeout);
                let result = resolver
                    .build_targets(&targets, &base_dir, move |line| {
                        om.lifecycle_event(&format!("bazel: {line}"));
                    })
                    .await;
                // Map build tool targets back to service/task names.
                match result {
                    Ok(batch) => {
                        let mut succeeded = Vec::new();
                        for target in &batch.succeeded {
                            if let Some(names) = target_to_name.get(target) {
                                succeeded.extend(names.clone());
                            }
                        }
                        let mut failed = Vec::new();
                        for (target, msg) in &batch.failed {
                            if let Some(names) = target_to_name.get(target) {
                                for n in names {
                                    failed.push((n.clone(), msg.clone()));
                                }
                            }
                        }
                        Ok(crate::build_tool::BatchBuildResult { succeeded, failed })
                    }
                    Err(e) => Err(e),
                }
            });
        }

        for (build_task, items) in turbo_by_task {
            let filters: Vec<String> = items.iter().map(|(_, f)| f.clone()).collect();
            let filter_to_name: std::collections::HashMap<String, Vec<String>> = {
                let mut m: std::collections::HashMap<String, Vec<String>> =
                    std::collections::HashMap::new();
                for (name, filter) in &items {
                    m.entry(filter.clone()).or_default().push(name.clone());
                }
                m
            };
            let count = filters.len();
            let base_dir = self.base_dir.clone();
            let om = self.output_manager.clone_lifecycle_emitter();
            let bt = build_task.clone();

            self.output_manager.lifecycle_event(&format!(
                "turbo: running '{build_task}' for {count} package{}...",
                if count == 1 { "" } else { "s" }
            ));

            join_set.spawn(async move {
                // TurboResolver needs a task for watch resolution, but here
                // we only care about the build. Use the build_task.
                let resolver = crate::build_tool::turbo::TurboResolver::new(
                    &bt,
                    None,
                    turbo_timeout,
                );
                let result = resolver
                    .build_packages(&bt, &filters, &base_dir, move |line| {
                        om.lifecycle_event(&format!("turbo: {line}"));
                    })
                    .await;
                match result {
                    Ok(batch) => {
                        let mut succeeded = Vec::new();
                        for filter in &batch.succeeded {
                            if let Some(names) = filter_to_name.get(filter) {
                                succeeded.extend(names.clone());
                            }
                        }
                        let mut failed = Vec::new();
                        for (filter, msg) in &batch.failed {
                            if let Some(names) = filter_to_name.get(filter) {
                                for n in names {
                                    failed.push((n.clone(), msg.clone()));
                                }
                            }
                        }
                        Ok(crate::build_tool::BatchBuildResult { succeeded, failed })
                    }
                    Err(e) => Err(e),
                }
            });
        }

        // Collect results from all build tool batches.
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(batch)) => {
                    for name in batch.succeeded {
                        if let Some(rs) = self.services.get_mut(&name) {
                            rs.batch_built = true;
                        }
                    }
                    for (name, msg) in batch.failed {
                        self.output_manager
                            .error_event(&format!("{name}: batch build failed: {msg}"));
                        if let Some(rs) = self.services.get_mut(&name) {
                            rs.state = ServiceState::Failed;
                        }
                        if let Some(rt) = self.tasks.get_mut(&name) {
                            rt.state = TaskItemState::Failed;
                        }
                    }
                }
                Ok(Err(e)) => {
                    self.output_manager
                        .error_event(&format!("batch build error: {e}"));
                }
                Err(e) => {
                    self.output_manager
                        .error_event(&format!("batch build task panicked: {e}"));
                }
            }
        }

        let batch_built_count = self.services.values().filter(|rs| rs.batch_built).count();
        if batch_built_count > 0 {
            self.output_manager.lifecycle_event(&format!(
                "batch build complete: {batch_built_count} item{} built",
                if batch_built_count == 1 { "" } else { "s" }
            ));
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
                .services
                .get(name)
                .is_some_and(|rs| rs.state == ServiceState::Pending);
            let is_pending_task = self
                .tasks
                .get(name)
                .is_some_and(|rt| rt.state == TaskItemState::Pending);
            if !is_pending_svc && !is_pending_task {
                continue;
            }

            let deps = dep_map.get(name).cloned().unwrap_or_default();
            let deps_ok = deps.iter().all(|dep| self.is_dep_satisfied(dep));
            if !deps_ok {
                continue;
            }

            if is_pending_svc {
                if self.services.contains_key(name)
                    && let Some(done_tx) = self.done_tx.clone()
                {
                    let _ = self.start_service(name, done_tx).await;
                    started_any = true;
                }
            } else if is_pending_task {
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.state = TaskItemState::Running;
                }
                self.handle_task_rerun(name).await;
                started_any = true;
            }
        }

        // If we started something, schedule another check — the newly-started
        // items might unblock further pending items.
        if started_any {
            let still_pending = self
                .services
                .values()
                .any(|rs| rs.state == ServiceState::Pending)
                || self
                    .tasks
                    .values()
                    .any(|rt| rt.state == TaskItemState::Pending);
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
    ///
    /// For proxy services: clears the proxy backend (new connections queue),
    /// allocates fresh ephemeral ports, starts the new instance, and sets the
    /// backend once the ready check passes. The proxy never drops — clients
    /// see a brief pause, not a connection refused.
    async fn handle_rebuild(&mut self, name: &str) {
        let rs = match self.services.get(name) {
            Some(rs) => rs,
            None => {
                self.fail_rebuild(name, &format!("{name}: rebuild requested for unknown service"));
                return;
            }
        };

        // For build-tool-managed services, queue the rebuild into a batch.
        // Multiple services sharing the same source files will be batched into
        // one `bazel build //a //b //c` invocation instead of separate builds.
        if rs.resolved.bazel.is_some() || rs.resolved.turbo.is_some() {
            if !self.pending_bt_rebuilds.contains(&name.to_string()) {
                self.pending_bt_rebuilds.push(name.to_string());
            }
            // Set or extend the batch window (50ms). This allows multiple
            // Rebuild commands from the watch module (which fire per-service
            // after their individual debounce timers) to coalesce.
            self.bt_rebuild_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(50));
            return;
        }

        self.do_rebuild(name).await;
    }

    /// Execute a rebuild for a single service: build, stop old, restart.
    ///
    /// This is the core rebuild logic, called either directly (non-build-tool
    /// services) or after a batch build completes (build-tool services).
    async fn do_rebuild(&mut self, name: &str) {
        let resolved = match self.services.get(name) {
            Some(rs) => rs.resolved.clone(),
            None => {
                self.fail_rebuild(name, &format!("{name}: rebuild requested for unknown service"));
                return;
            }
        };
        let mut resolved = resolved;

        self.output_manager
            .lifecycle_event(&format!("{name}: rebuilding (file changed)"));

        // Build (if any). On failure, keep old process running.
        if let Err(()) = self.run_service_build(name, &resolved).await {
            self.fail_rebuild(name, &format!("{name}: build failed"));
            return;
        }

        // For proxy services: clear backend so new connections queue while we
        // restart, and allocate fresh ephemeral ports for the new instance.
        let has_proxy = self.services.get(name).is_some_and(|rs| rs.proxy.is_some());
        if has_proxy
            && let Some(rs) = self.services.get(name)
            && let Some(ref proxy) = rs.proxy
        {
            proxy.clear_backend();
        }

        // Stop the old service (if running).
        if let Some(handle) = self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
            // Release attach lock — the old PTY is gone after restart.
            // This causes the attach session to exit (follow sink closes below).
            if self.remove_attach_lock(name) {
                self.output_manager.resume_stdout_sink(name).await;
            }
            if let Some(rs) = self.services.get_mut(name) {
                rs.state = ServiceState::Stopping;
            }
            let shutdown_config = resolved.shutdown.as_ref();
            if let Err(e) = stop_service(handle, shutdown_config, false).await {
                self.output_manager
                    .error_event(&format!("{name}: stop failed during rebuild: {e}"));
            }
            // Close follow/attach sinks so attached clients and log
            // followers detect the restart and exit cleanly.
            if let Some(writer) = self.output_manager.service_writer(name) {
                writer.close_follow_sinks().await;
            }
        }

        // For proxy services: allocate new ephemeral ports and rebind
        // LISTEN_FDS sockets before spawning the new instance.
        if has_proxy {
            // Step 1: Reallocate ephemeral ports.
            let realloc_result = if let Some(rs) = self.services.get_mut(name) {
                if let Some(ref mut proxy) = rs.proxy {
                    Some(proxy.reallocate_ephemeral_ports().await)
                } else {
                    None
                }
            } else {
                None
            };
            match realloc_result {
                Some(Ok(_old_ports)) => {
                    // Step 2: Extract env vars and listen_fds addrs.
                    let (proxy_env, listen_fds_addrs) = if let Some(rs) = self.services.get(name) {
                        if let Some(ref proxy) = rs.proxy {
                            (proxy.env_vars(), proxy.listen_fds_addrs())
                        } else {
                            (HashMap::new(), Vec::new())
                        }
                    } else {
                        (HashMap::new(), Vec::new())
                    };
                    resolved.env.extend(proxy_env);
                    // Step 3: Rebind LISTEN_FDS sockets for new ephemeral ports.
                    if !listen_fds_addrs.is_empty() {
                        if let Some(rs) = self.services.get_mut(name) {
                            rs.bound_sockets = None;
                        }
                        match crate::process::socket::bind_sockets(&listen_fds_addrs) {
                            Ok(sockets) => {
                                if let Some(rs) = self.services.get_mut(name) {
                                    rs.bound_sockets = Some(sockets);
                                }
                            }
                            Err(e) => {
                                self.fail_rebuild(
                                    name,
                                    &format!("{name}: failed to bind ephemeral sockets: {e}"),
                                );
                                return;
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    self.fail_rebuild(
                        name,
                        &format!("{name}: failed to allocate ephemeral ports: {e}"),
                    );
                    return;
                }
                None => {}
            }
        }

        // Start the service again. Sockets are already bound (don holds them).
        // For proxy services, set_backend will be called when the ready check
        // passes (in handle_service_done).
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
        self.lookup_service(name)?;
        // Block if the service is currently active.
        if self.services.get(name).is_some_and(|rs| rs.handle.is_some()) {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "already running".to_string(),
            });
        }
        // For lazy services in Lazy state, force-start via the normal path.
        if self.services.get(name).is_some_and(|rs| rs.state == ServiceState::Lazy) {
            self.output_manager
                .lifecycle_event(&format!("{name}: starting (requested)"));
            if let Some(done_tx) = self.done_tx.clone() {
                return self.start_service(name, done_tx).await.map_err(|e| {
                    CommandError::Failed {
                        name: name.to_string(),
                        message: e.to_string(),
                    }
                });
            }
        }
        let resolved = match self.services.get(name) {
            Some(rs) => rs.resolved.clone(),
            None => {
                return Err(CommandError::UnknownService {
                    name: name.to_string(),
                })
            }
        };
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
        self.lookup_service(name)?;
        // A lazy service in Lazy state has no process — just mark it Stopped.
        if self.services.get(name).is_some_and(|rs| rs.state == ServiceState::Lazy) {
            if let Some(rs) = self.services.get_mut(name) {
                rs.state = ServiceState::Stopped;
            }
            self.output_manager
                .lifecycle_event(&format!("{name}: stopped (was lazy)"));
            return Ok(());
        }
        let handle = self
            .services
            .get_mut(name)
            .and_then(|rs| rs.handle.take())
            .ok_or_else(|| CommandError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            })?;
        let shutdown_config = self
            .services
            .get(name)
            .and_then(|rs| rs.resolved.shutdown.clone());
        // Release attach lock if held — the PTY write in the attach session
        // becomes invalid once the service stops (process gone).
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.state = ServiceState::Stopping;
        }
        self.output_manager
            .lifecycle_event(&format!("stopping {name}... (requested)"));
        if let Err(e) = stop_service(handle, shutdown_config.as_ref(), false).await {
            return Err(CommandError::Failed {
                name: name.to_string(),
                message: e.to_string(),
            });
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.state = ServiceState::Stopped;
        }
        // Close follow/attach sinks so log followers and attach sessions
        // detect the service stopped instead of blocking forever.
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }
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

    /// Handle an interactive attach request (services or tasks).
    ///
    /// If the process is running, attaches immediately. If not running
    /// (e.g. task between runs), registers a waiter that will be fulfilled
    /// when the process next spawns.
    async fn handle_attach_cmd(
        &mut self,
        name: &str,
        pid: u32,
        reply: oneshot::Sender<Result<AttachSession, CommandError>>,
    ) {
        // Must be a known service or task.
        let is_service = self.services.contains_key(name);
        let is_task = self.tasks.contains_key(name);
        if !is_service && !is_task {
            let _ = reply.send(Err(CommandError::UnknownService {
                name: name.to_string(),
            }));
            return;
        }

        // Check attach lock.
        if let Some(existing_pid) = self.get_attach_lock(name) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: format!("process {existing_pid} is currently attached to '{name}'"),
            }));
            return;
        }

        // Check for a pending waiter (another client already waiting).
        if self.has_attach_waiter(name) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "another client is already waiting to attach".to_string(),
            }));
            return;
        }

        // Check if the process is running.
        let is_running = if is_service {
            self.services.get(name).is_some_and(|rs| rs.handle.is_some())
        } else {
            self.tasks.get(name).is_some_and(|rt| rt.pgid.is_some())
        };

        if !is_running {
            // Not running — register a waiter. The reply will be sent when
            // the process next spawns.
            self.output_manager
                .lifecycle_event(&format!("{name}: waiting for process to start (attach pending)"));
            self.set_attach_waiter(name, AttachWaiter { pid, reply });
            return;
        }

        // Running — attach immediately.
        let result = self.fulfill_attach(name, pid).await;
        let _ = reply.send(result);
    }

    /// Fulfill an attach request for a running process. Assumes the caller
    /// has already validated the name exists and checked the attach lock.
    async fn fulfill_attach(&mut self, name: &str, pid: u32) -> Result<AttachSession, CommandError> {
        // Reclaim the PTY write handle by stopping the OSC sink.
        let osc_handle = self.get_osc_sink_mut(name).and_then(|opt| opt.take());
        let pty_write = match osc_handle {
            Some(osc_handle) => osc_handle.take_pty_write().await,
            None => None,
        };
        let pty_write = pty_write.ok_or_else(|| CommandError::InvalidState {
            name: name.to_string(),
            message: "no PTY available (spawned in pipe mode)".to_string(),
        })?;

        // Set up follow sink for live output (256 lines of headroom).
        let output_rx = self
            .output_manager
            .add_follow_sink(name, 50, 256)
            .await
            .ok_or_else(|| CommandError::Failed {
                name: name.to_string(),
                message: "failed to create output sink".to_string(),
            })?;

        // Pause prefixed stdout for this service.
        self.output_manager.pause_stdout_sink(name).await;

        // Acquire the lock.
        self.set_attach_lock(name, pid);

        self.output_manager
            .lifecycle_event(&format!("{name}: attached (pid {pid})"));

        Ok(AttachSession { pty_write, output_rx })
    }

    /// Release an attach session.
    /// Check for a pending attach waiter and fulfill it if the process
    /// is now running.
    async fn fulfill_pending_waiter(&mut self, name: &str) {
        if let Some(waiter) = self.take_attach_waiter(name) {
            // Check the waiter's reply channel is still alive (client may
            // have disconnected while waiting).
            if waiter.reply.is_closed() {
                return;
            }
            let result = self.fulfill_attach(name, waiter.pid).await;
            let _ = waiter.reply.send(result);
        }
    }

    async fn handle_detach(&mut self, name: &str, pty_write: Option<pty_process::OwnedWritePty>) {
        // Only return the PTY write handle if the attach lock is still held.
        // If the service/task was stopped/restarted while we were attached,
        // the lock was already cleared and the current process has a fresh
        // PTY — setting the stale one would corrupt it.
        if self.get_attach_lock(name).is_some()
            && let Some(pty) = pty_write
        {
            // Restart the OSC response sink with the returned handle.
            if let Some(osc_handle) = self.output_manager.add_osc_sink(name, pty).await
                && let Some(sink_slot) = self.get_osc_sink_mut(name)
            {
                *sink_slot = Some(osc_handle);
            }
        }

        // Release lock.
        self.remove_attach_lock(name);

        // Resume prefixed output.
        self.output_manager.resume_stdout_sink(name).await;

        self.output_manager
            .lifecycle_event(&format!("{name}: detached"));
    }

    /// Handle a file-watch-triggered task re-run.
    async fn handle_task_rerun(&mut self, name: &str) {
        let task_cfg = match self.tasks.get(name) {
            Some(rt) => rt.config.clone(),
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

        // Skip the needs_run hash check — the file watcher already confirmed
        // a matching file changed. The hash check is only needed at startup
        // (to skip tasks whose inputs haven't changed since the last run).

        // If the task has opted out of auto-reruns, mark it pending and don't spawn.
        // The user will need to trigger a manual rerun when they're ready.
        if !task_cfg.auto_rerun {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.state = TaskItemState::PendingRerun;
            }
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

        // Release attach lock and close follow sinks so any active attach
        // session exits cleanly before the new process starts.
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        self.output_manager
            .lifecycle_event(&format!("{name}: re-running (file changed)"));
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.state = TaskItemState::Running;
        }

        self.output_manager
            .debug_event(&format!("{name}: spawning process..."));
        match task::spawn_task(&task_cfg, name, &self.base_dir, self.platform).await {
            Ok(spawn) => {
                self.output_manager
                    .debug_event(&format!("{name}: process spawned (pid {}))", spawn.handle.pgid()));
                self.wire_task_output_and_wait(name, spawn, &task_cfg, None).await;
            }
            Err(e) => {
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.state = TaskItemState::Failed;
                }
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
            if let Some(rs) = self.services.get_mut(&item.name) {
                rs.state = ServiceState::Ready;
                // Activate proxy backend now that the service is ready.
                if let Some(ref proxy) = rs.proxy {
                    proxy.set_backend();
                }
                let ready_desc = match &rs.resolved.ready {
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
                    if rs.resolved.ready.is_some() {
                        format!(" ready{ready_desc}")
                    } else {
                        ready_desc
                    }
                ));
            }
        } else {
            // If a lazy service fails, reset to Lazy so the next connection
            // can re-trigger it instead of leaving it permanently failed.
            if let Some(rs) = self.services.get_mut(&item.name) {
                let is_lazy = rs.resolved.lazy && rs.proxy.is_some();
                if is_lazy {
                    rs.state = ServiceState::Lazy;
                    if let Some(ref msg) = item.message {
                        self.output_manager
                            .error_event(&format!("{}: {msg} (will retry on next connection)", item.name));
                    }
                } else {
                    rs.state = ServiceState::Failed;
                    if let Some(ref msg) = item.message {
                        self.output_manager
                            .error_event(&format!("{}: {msg}", item.name));
                    }
                }
            }
        }
    }

    fn handle_task_done(&mut self, item: &ItemDone) {
        if let Some(rt) = self.tasks.get_mut(&item.name)
            && rt.pgid.take().is_some()
        {
            // Release attach lock if held.
            rt.attach_lock = None;
            // Can't await here (sync fn), but the stdout sink resume
            // will happen naturally when the follow sink closes.
        }
        let timing = item.elapsed.map(format_duration).unwrap_or_default();

        if item.success {
            let cur = self.tasks.get(&item.name).map(|rt| rt.state);
            if cur != Some(TaskItemState::Skipped) {
                if let Some(rt) = self.tasks.get_mut(&item.name) {
                    rt.state = TaskItemState::Completed;
                }
                let msg = if timing.is_empty() {
                    format!("{} complete", item.name)
                } else {
                    format!("{} complete ({timing})", item.name)
                };
                self.output_manager.lifecycle_event(&msg);
            }
        } else {
            if let Some(rt) = self.tasks.get_mut(&item.name) {
                rt.state = TaskItemState::Failed;
            }
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

        // Shut down all proxy listeners first (stop accepting new connections).
        for (_, rs) in self.services.iter_mut() {
            if let Some(proxy) = rs.proxy.take() {
                proxy.shutdown();
            }
        }

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
            if !self.services.contains_key(name) {
                continue;
            }
            let state = self.services.get(name).map(|rs| rs.state);
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

        // Stop from highest depth to lowest (dependents first).
        for (_depth, names) in by_depth.into_iter().rev() {
            for name in &names {
                self.output_manager
                    .lifecycle_event(&format!("stopping {name}... ({remaining} remaining)"));
            }

            // Track PGIDs of services being stopped so we can SIGKILL
            // them if a second Ctrl+C arrives during graceful shutdown.
            let mut stopping_pgids: HashMap<String, i32> = HashMap::new();
            let mut join_set: JoinSet<String> = JoinSet::new();
            for name in &names {
                if let Some(handle) = self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
                    if let ServiceHandle::Process(ref proc) = handle {
                        stopping_pgids.insert(name.clone(), proc.pgid());
                    }
                    let shutdown_config = self
                        .services
                        .get(name)
                        .and_then(|rs| rs.resolved.shutdown.clone());
                    let force = SIGNAL_COUNT.load(Ordering::SeqCst) >= 2;
                    let name_owned = name.clone();
                    join_set.spawn(async move {
                        let _ = stop_service(handle, shutdown_config.as_ref(), force).await;
                        name_owned
                    });
                }
            }

            // Wait for graceful stops, but if a second Ctrl+C arrives,
            // SIGKILL all processes being stopped and abort the futures.
            loop {
                if SIGNAL_COUNT.load(Ordering::SeqCst) >= 2 && !join_set.is_empty() {
                    self.output_manager
                        .lifecycle_event("forcing immediate shutdown");
                    // SIGKILL all processes that are still being stopped.
                    for (name, pgid) in &stopping_pgids {
                        let _ = nix::sys::signal::killpg(
                            nix::unistd::Pid::from_raw(*pgid),
                            nix::sys::signal::Signal::SIGKILL,
                        );
                        if let Some(rs) = self.services.get_mut(name) {
                            rs.state = ServiceState::Stopped;
                        }
                    }
                    join_set.abort_all();
                    while join_set.join_next().await.is_some() {}
                    remaining = 0;
                    break;
                }

                // Poll for the next completed stop, with a short sleep so
                // we can re-check the force flag promptly.
                match tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    join_set.join_next(),
                )
                .await
                {
                    Ok(Some(Ok(name))) => {
                        stopping_pgids.remove(&name);
                        if let Some(rs) = self.services.get_mut(&name) {
                            rs.state = ServiceState::Stopped;
                        }
                        remaining -= 1;
                        self.output_manager
                            .lifecycle_event(&format!("{name} stopped ({remaining} remaining)"));
                    }
                    Ok(Some(Err(_))) => {
                        remaining = remaining.saturating_sub(1);
                    }
                    Ok(None) => break, // All tasks done.
                    Err(_) => continue, // Timeout — re-check force flag.
                }
            }

            if remaining == 0 {
                break;
            }
        }

        // Kill any still-running task process groups.
        let running_task_pgids: Vec<(String, i32)> = self
            .tasks
            .iter()
            .filter_map(|(name, rt)| rt.pgid.map(|pgid| (name.clone(), pgid)))
            .collect();
        if !running_task_pgids.is_empty() {
            self.output_manager.lifecycle_event(&format!(
                "killing {} running task{}",
                running_task_pgids.len(),
                if running_task_pgids.len() == 1 { "" } else { "s" }
            ));
            for (name, pgid) in &running_task_pgids {
                if let Err(e) = nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(*pgid),
                    nix::sys::signal::Signal::SIGKILL,
                ) {
                    // ESRCH = already dead, which is fine.
                    if e != nix::Error::ESRCH {
                        self.output_manager
                            .error_event(&format!("{name}: failed to kill task pgid {pgid}: {e}"));
                    }
                }
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.pgid = None;
                }
            }
        }

        self.output_manager.lifecycle_event("shutdown complete");
    }

    /// Wait for remaining async tasks to finish after shutdown.
    async fn wait_for_shutdown(&mut self) {
        // All handles should already be stopped by initiate_shutdown.
        // Drop remaining handles, release sockets, clear attach state.
        for (_, rs) in self.services.iter_mut() {
            rs.handle = None;
            rs.attach_lock = None;
            rs.attach_waiter = None;
            rs.bound_sockets = None;
        }
        for (_, rt) in self.tasks.iter_mut() {
            rt.attach_lock = None;
            rt.attach_waiter = None;
        }
    }

    /// Collect status of all items.
    fn collect_status(&self, verbose: bool) -> Vec<ItemStatus> {
        let mut statuses = Vec::new();
        for (name, rs) in &self.services {
            let verbose_info = if verbose {
                let resolved = &rs.resolved;
                let ready = resolved.ready.as_ref().map(|r| {
                    if let Some(ref tcp) = r.tcp {
                        format!("tcp {tcp}")
                    } else if let Some(ref http) = r.http {
                        format!("http {http}")
                    } else if let Some(ref exec) = r.exec {
                        format!("{} {}", exec.cmd, exec.args.join(" "))
                    } else {
                        "none".to_string()
                    }
                });
                let cmd = resolved.run.as_ref().map(|r| {
                    if r.args.is_empty() {
                        r.cmd.clone()
                    } else {
                        format!("{} {}", r.cmd, r.args.join(" "))
                    }
                });
                // Use resolved build tool watch paths if explicit ones are empty.
                let watch = if resolved.watch.is_empty() {
                    rs.resolved_watch_paths.clone()
                } else {
                    resolved.watch.clone()
                };
                Some(VerboseInfo {
                    depends_on: resolved.depends_on.clone(),
                    watch,
                    listen: resolved.listen.clone(),
                    proxy: resolved.proxy.iter().map(|p| p.listen.clone()).collect(),
                    bazel_target: resolved.bazel.as_ref().map(|b| b.target.clone()),
                    turbo_task: resolved.turbo.as_ref().map(|t| t.task.clone()),
                    ready,
                    cmd,
                })
            } else {
                None
            };
            statuses.push(ItemStatus::Service {
                name: name.clone(),
                state: rs.state,
                verbose: verbose_info,
            });
        }
        for (name, rt) in &self.tasks {
            let verbose_info = if verbose {
                let task = &rt.config;
                let cmd_str = if task.args.is_empty() {
                    task.cmd.clone()
                } else {
                    format!("{} {}", task.cmd, task.args.join(" "))
                };
                let watch = if task.watch.is_empty() {
                    rt.resolved_watch_paths.clone()
                } else {
                    task.watch.clone()
                };
                Some(VerboseInfo {
                    depends_on: task.depends_on.clone(),
                    watch,
                    listen: Vec::new(),
                    proxy: Vec::new(),
                    bazel_target: task.bazel.as_ref().map(|b| b.target.clone()),
                    turbo_task: task.turbo.as_ref().map(|t| t.task.clone()),
                    ready: None,
                    cmd: Some(cmd_str),
                })
            } else {
                None
            };
            statuses.push(ItemStatus::Task {
                name: name.clone(),
                state: rt.state,
                verbose: verbose_info,
            });
        }
        statuses
    }
}

/// Check if `.don/` is in `.gitignore`. Warns if not — the `.don/` directory
/// contains PID files, sockets, and cached artifacts that shouldn't be committed.
fn check_gitignore(base_dir: &std::path::Path, output: &OutputManager) {
    let gitignore_path = base_dir.join(".gitignore");
    match std::fs::read_to_string(&gitignore_path) {
        Ok(content) => {
            let has_don = content.lines().any(|line| {
                let trimmed = line.trim();
                trimmed == ".don" || trimmed == ".don/" || trimmed == "/.don" || trimmed == "/.don/"
            });
            if !has_don {
                output.error_event(
                    ".don/ is not in .gitignore — add it to avoid committing PID files, sockets, and cached artifacts"
                );
            }
        }
        Err(_) => {
            // No .gitignore or not a git repo — skip silently.
        }
    }
}

/// Resolve a profile into the full set of items (services + tasks) to run,
/// including transitive dependencies. Starting with the profile's explicit
/// services and tasks, walks `depends_on` recursively to include everything
/// needed.
pub fn resolve_profile_items(config: &Config, profile: &crate::config::Profile) -> HashSet<String> {
    let mut result = HashSet::new();
    let mut queue: Vec<String> = profile
        .services
        .iter()
        .chain(profile.tasks.iter())
        .cloned()
        .collect();

    while let Some(name) = queue.pop() {
        if !result.insert(name.clone()) {
            continue; // already visited
        }
        // Follow deps from services.
        if let Some(svc) = config.services.get(&name) {
            for dep in &svc.depends_on {
                if !result.contains(dep) {
                    queue.push(dep.clone());
                }
            }
        }
        // Follow deps from tasks.
        if let Some(task) = config.tasks.get(&name) {
            for dep in &task.depends_on {
                if !result.contains(dep) {
                    queue.push(dep.clone());
                }
            }
        }
    }
    result
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

    #[test]
    fn runtime_service_default_state() {
        use crate::config::service::ResolvedService;
        use crate::config::types::LogConfig;
        use std::collections::HashMap;

        let rs = RuntimeService {
            resolved: ResolvedService {
                dir: None,
                env: HashMap::new(),
                env_file: Vec::new(),
                watch: Vec::new(),
                ignore: Vec::new(),
                debounce: None,
                depends_on: Vec::new(),
                listen: Vec::new(),
                proxy: Vec::new(),
                lazy: false,
                download: None,
                ready: None,
                shutdown: None,
                log: LogConfig::Stdout,
                bazel: None,
                turbo: None,
                docker: None,
                rust: None,
                go: None,
                run: None,
                build: None,
            },
            state: ServiceState::Pending,
            handle: None,
            osc_sink: None,
            attach_lock: None,
            attach_waiter: None,
            bound_sockets: None,
            proxy: None,
            resolved_watch_paths: Vec::new(),
            bazel_binary_path: None,
            batch_built: false,
        };

        assert_eq!(rs.state, ServiceState::Pending);
        assert!(rs.handle.is_none());
        assert!(rs.osc_sink.is_none());
        assert!(rs.attach_lock.is_none());
        assert!(rs.attach_waiter.is_none());
        assert!(rs.bound_sockets.is_none());
        assert!(rs.proxy.is_none());
        assert!(rs.resolved_watch_paths.is_empty());
        assert!(rs.bazel_binary_path.is_none());
        assert!(!rs.batch_built);
        assert!(rs.resolved.run.is_none());
    }

    #[test]
    fn runtime_task_default_state() {
        use crate::config::types::LogConfig;
        use std::collections::HashMap;

        let rt = RuntimeTask {
            config: crate::config::task::Task {
                cmd: "echo".to_string(),
                args: vec!["hello".to_string()],
                dir: None,
                env: HashMap::new(),
                depends_on: Vec::new(),
                watch: Vec::new(),
                ignore: Vec::new(),
                timeout: None,
                log: LogConfig::Stdout,
                auto_rerun: true,
                download: None,
                bazel: None,
                turbo: None,
            },
            state: TaskItemState::Pending,
            pgid: None,
            osc_sink: None,
            attach_lock: None,
            attach_waiter: None,
            resolved_watch_paths: Vec::new(),
        };

        assert_eq!(rt.state, TaskItemState::Pending);
        assert!(rt.pgid.is_none());
        assert!(rt.osc_sink.is_none());
        assert!(rt.attach_lock.is_none());
        assert!(rt.attach_waiter.is_none());
        assert!(rt.resolved_watch_paths.is_empty());
        assert_eq!(rt.config.cmd, "echo");
    }
}
