//! Runner — the orchestrator that starts services and tasks in dependency order.
//!
//! The runner builds an execution plan via topological sort, then starts
//! everything whose dependencies are satisfied concurrently using tokio tasks.
//! It owns all service/task state in a plain `HashMap` — no `Arc<Mutex<>>`.
//! Communication uses channels: `mpsc` for commands in, `broadcast` for events out.

mod attach;
mod build_tools;
mod completions;
mod graph;
mod health;
mod params;
mod paths;
mod profile;
mod rebuild;
mod service_commands;
mod service_worker;
mod shutdown;
mod signals;
mod startup;
mod state;
mod status;
mod support;
mod task_worker;

pub(crate) mod service;
pub(crate) mod task;

pub(crate) use params::resolve_task_params;
pub use profile::resolve_profile_items;
pub use signals::{install_signal_handlers, signal_count};

use crate::config::{Config, Platform, TaskAutoRun};
use crate::output::OutputManager;
use crate::process::pid_file::PidFile;
use crate::task_state::TaskState;
use crate::watch::WatchManager;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
#[cfg(test)]
use std::time::SystemTime;
use tokio::sync::{broadcast, mpsc, oneshot};

#[cfg(test)]
use self::build_tools::bazel_graph_requery_group_dir;
use self::build_tools::{BatchBuildOutcome, GraphRequeryOutcomeItem, RebuildBatchOutcome};
#[cfg(test)]
use self::graph::compute_depths;
use self::graph::topological_sort;
use self::health::{format_unexpected_exit, run_health_monitor, unhealthy_restart_backoff_secs};
#[cfg(test)]
use self::paths::any_glob_path_changed_since;
use self::paths::{resolve_watch_ignore_patterns, working_dir_for};
use self::profile::resolve_profile_items_for_platform;
use self::service::ServiceHandle;
use self::service_worker::{ServiceStartContext, ServiceStartMode};
use self::signals::shutdown_requested;
use self::support::{check_gitignore, format_duration};
use self::task_worker::{TaskRunMode, TaskRunPrepared, TaskWorkerContext, run_task_worker};

enum ServiceStartIntent {
    Startup {
        done_tx: mpsc::Sender<ItemDone>,
    },
    Reply {
        reply: oneshot::Sender<CommandResult>,
    },
    Background,
}

enum TaskRunIntent {
    Startup { done_tx: mpsc::Sender<ItemDone> },
    Background,
}

#[derive(Clone, Copy, Default)]
pub(crate) enum ServiceStopAction {
    #[default]
    None,
    RestartFull,
    RestartSpawnOnly,
}

/// The state of a service in the runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceState {
    Pending,
    /// A batch build (bazel/turbo) is in flight. Transitions to Pending on
    /// success (then the service starts like any other) or Failed on build
    /// error. Only set during the startup-phase batch build; file-watch
    /// rebuilds keep the service in Running/Ready.
    Building,
    /// Proxy is bound and accepting connections, but the service process is not
    /// started yet. Will transition to Starting on first incoming connection.
    Lazy,
    Starting,
    Running,
    Ready,
    /// Process is alive but its health-check monitor is failing. Dependents
    /// are still considered satisfied — we don't tear them down on flap. The
    /// service can recover back to Ready, or it can be restarted (manually
    /// or by `on_failure = "restart"`).
    Unhealthy,
    Stopping,
    Stopped,
    Failed,
    /// A transitive dependency failed, so we never attempted to start this
    /// service. Distinct from `Failed` (which means *this* service itself
    /// blew up) so the UI can highlight the actual culprit — and sort it
    /// above everything that merely got stranded.
    DependencyFailed,
}

impl ServiceState {
    /// Whether this state is considered "satisfied" for dependency resolution.
    /// A dependency is satisfied when the service is Ready, lazy-bound, or
    /// merely Unhealthy (process is still alive — leave dependents alone).
    pub(crate) fn is_satisfied(&self) -> bool {
        matches!(self, Self::Ready | Self::Lazy | Self::Unhealthy)
    }

    /// Valid transitions from one state to another.
    #[cfg(test)]
    pub(crate) fn can_transition_to(&self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Building)
                | (Self::Pending, Self::Starting)
                | (Self::Pending, Self::Lazy)
                | (Self::Building, Self::Pending)
                | (Self::Building, Self::Failed)
                | (Self::Lazy, Self::Building)
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
                | (Self::Ready, Self::Unhealthy)
                | (Self::Unhealthy, Self::Ready)
                | (Self::Unhealthy, Self::Stopping)
                | (Self::Unhealthy, Self::Stopped)
                | (Self::Unhealthy, Self::Failed)
                | (Self::Unhealthy, Self::Pending)
                | (Self::Stopping, Self::Stopped)
                | (Self::Stopping, Self::Failed)
                // Restart: from stopped / failed / dep-failed back to pending.
                | (Self::Stopped, Self::Pending)
                | (Self::Failed, Self::Pending)
                | (Self::DependencyFailed, Self::Pending)
                // A pending item gets marked DependencyFailed when a dep blew up.
                | (Self::Pending, Self::DependencyFailed)
        )
    }
}

/// The state of a task in the runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskItemState {
    Pending,
    /// Waiting on the startup-phase batch build. Transitions to Pending on
    /// success or Failed on build error.
    Building,
    Running,
    Completed,
    Skipped,
    Failed,
    /// A transitive dependency failed, so we never ran this task. See
    /// [`ServiceState::DependencyFailed`] for the rationale.
    DependencyFailed,
    /// The task is waiting for a manual trigger. Dependency satisfaction also
    /// depends on task history and auto-run policy.
    PendingRun,
}

/// An item in the dependency graph — either a service or a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// No task with this name exists in the config.
    UnknownTask { name: String },
    /// The name refers to a task, not a service — start/stop/restart only
    /// apply to services.
    NotAService { name: String },
    /// The name refers to a service, not a task — `run` only applies to tasks.
    NotATask { name: String },
    /// The service is already running (for Start) or already stopped (for Stop).
    InvalidState { name: String, message: String },
    /// The operation itself failed.
    Failed { name: String, message: String },
    /// User supplied params that the task doesn't declare, or the validation
    /// rules on a declared param rejected the value.
    InvalidParams { name: String, message: String },
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownService { name } => write!(f, "unknown service '{name}'"),
            Self::UnknownTask { name } => write!(f, "unknown task '{name}'"),
            Self::NotAService { name } => {
                write!(
                    f,
                    "'{name}' is a task — start/stop/restart only apply to services"
                )
            }
            Self::NotATask { name } => {
                write!(
                    f,
                    "'{name}' is a service — use `don start/stop/restart` instead of `don run`"
                )
            }
            Self::InvalidState { name, message } => write!(f, "{name}: {message}"),
            Self::Failed { name, message } => write!(f, "{name}: {message}"),
            Self::InvalidParams { name, message } => write!(f, "{name}: {message}"),
        }
    }
}

/// Error returned from [`RunnerCommand::ResolveCompletions`].
///
/// The TUI displays `message` inline and, when `log_path` is set, offers
/// the user a way to pull up the full command invocation + stdout/stderr
/// that was saved at that path.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompletionError {
    /// Human-readable summary suitable for a status bar / inline banner.
    pub message: String,
    /// Filesystem path to the saved log file, when one was written.
    /// Absent when the failure happened before the command was invoked
    /// (e.g., unknown task or param).
    pub log_path: Option<std::path::PathBuf>,
}

impl std::fmt::Display for CompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.log_path {
            Some(p) => write!(f, "{} (see {})", self.message, p.display()),
            None => write!(f, "{}", self.message),
        }
    }
}

fn should_rebuild_after_graph_requery(service: &RuntimeService) -> bool {
    if service.resolved.lazy && !service.batch_built {
        return false;
    }

    matches!(
        service.state(),
        ServiceState::Running | ServiceState::Ready | ServiceState::Unhealthy
    )
}

/// A command sent to the runner via its public `mpsc` channel.
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
    /// A watched file changed during the current rebuild cycle for a service.
    /// The active build should finish, but any pending restart should be
    /// skipped because the build output is already stale.
    RebuildStale { name: String },
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
    /// Build graph definition files changed (BUILD, package.json, etc.).
    /// Triggers a re-query of the build tool to update watch patterns.
    BuildGraphChanged { name: String },
    /// Retry starting any Pending services/tasks whose deps are now
    /// satisfied. Sent by [`Self::StartPending`] itself after a delay,
    /// forming a soft poll loop that unblocks dependents as their deps
    /// reach Ready.
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
    /// Run all tasks currently in PendingRun state.
    RunPendingTasks {
        reply: oneshot::Sender<CommandResult>,
    },
    /// Run a specific task by name, bypassing the `auto_run` gate. Used by
    /// `don run <name>` and the TUI action palette. `params` carries the
    /// user-supplied values for the task's declared params — empty for
    /// tasks that don't declare any.
    RunTask {
        name: String,
        params: HashMap<String, String>,
        reply: oneshot::Sender<CommandResult>,
    },
    /// Resolve candidate values for a single param of a task by running
    /// its `completions` command. Used by the TUI form and by shell tab
    /// completion.
    ///
    /// `partial` carries the user's already-entered param values for the
    /// *other* params in the form — exposed to the completion command as
    /// `DON_PARAM_<NAME>=<value>` env vars so one param's candidates can
    /// depend on another. `force_refresh = true` bypasses the cache.
    ResolveCompletions {
        task: String,
        param: String,
        partial: HashMap<String, String>,
        force_refresh: bool,
        reply: oneshot::Sender<Result<Vec<String>, CompletionError>>,
    },
    /// Initiate graceful shutdown.
    Shutdown,
}

/// Runner-private messages emitted by detached workers.
enum RunnerInternalCommand {
    /// Completion from a detached task run worker.
    TaskRunPrepared {
        name: String,
        op_id: u64,
        task_cfg: Box<crate::config::Task>,
        intent: TaskRunIntent,
        result: Result<TaskRunPrepared, String>,
    },
    /// A task process exited after an explicit run/restart.
    TaskExited {
        name: String,
        pgid: i32,
        success: bool,
        message: Option<String>,
        elapsed: Option<std::time::Duration>,
        rerun: bool,
    },
    /// Result of the startup-phase batch build.
    BatchBuildComplete(BatchBuildOutcome),
    /// Result of a detached file-watch build-tool rebuild batch.
    RebuildBatchComplete(RebuildBatchOutcome),
    /// Result of a just-in-time build for a single lazy service.
    LazyBuildComplete {
        name: String,
        outcome: BatchBuildOutcome,
    },
    /// Health-check monitor reported a state transition for a service.
    ServiceHealthChanged { name: String, healthy: bool },
    /// Backoff timer fired for an auto-restart.
    AutoRestart { name: String, attempt: u32 },
    /// A service process exited.
    ServiceExited { name: String, pgid: i32 },
    /// Ready-check completed for a manual-start or rebuild spawn.
    ReadyCheckComplete { name: String, success: bool },
    /// Completion from a detached manual service stop/restart worker.
    ServiceStopComplete {
        name: String,
        op_id: u64,
        result: Result<(), String>,
    },
    /// Completion from a detached service start worker.
    ServiceStartPrepared {
        name: String,
        op_id: u64,
        context: Box<ServiceStartContext>,
        intent: ServiceStartIntent,
        result: Result<Box<service::StartResult>, String>,
    },
    /// Completion from a detached rebuild worker for a single service.
    ServiceRebuildPrepared {
        name: String,
        op_id: u64,
        result: Result<(), String>,
    },
    /// Completion from a detached build-graph re-query worker.
    GraphRequeryComplete(Vec<GraphRequeryOutcomeItem>),
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
    /// Proxy entries, each formatted as `"addr (env=NAME)"` or
    /// `"addr (listenfd)"` for display.
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
    /// Live watch-manager state for this item, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watch_state: Option<String>,
    /// Extra watch diagnostics for this item.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub watch_notes: Vec<String>,
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
    /// Graceful shutdown has started.
    ShutdownStarted,
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
    internal_tx: mpsc::Sender<RunnerInternalCommand>,
    internal_rx: mpsc::Receiver<RunnerInternalCommand>,
    event_tx: broadcast::Sender<RunnerEvent>,

    /// Item-completion sender shared between the initial startup and config
    /// reload paths. Ready-check and task-completion callbacks send here.
    /// The main loop's `done_rx` receives these.
    done_tx: Option<mpsc::Sender<ItemDone>>,

    // Shutdown signal receiver — wakes the select loop when Ctrl+C is pressed.
    // `Option` because `run()` takes it out at the top to consume in the
    // main `select!`. It's never `None` after construction until `run()`
    // consumes it.
    shutdown_rx: Option<mpsc::Receiver<()>>,

    /// Detached batch-build task spawned at startup for services/tasks with
    /// a bazel/turbo config. `Some` until [`RunnerInternalCommand::BatchBuildComplete`]
    /// arrives and the handle is consumed. Wrapped in [`AbortOnDrop`] so
    /// shutting the runner down — or dropping the field before completion —
    /// aborts the task, dropping the in-flight `Child` (with `kill_on_drop`)
    /// and sending SIGKILL to the bazel/turbo client.
    batch_build_handle: Option<crate::build_tool::AbortOnDrop<()>>,

    /// Detached JIT build tasks spawned when a lazy service's proxy gets
    /// its first connection. Keyed by service name. Entries are inserted
    /// on spawn and removed when [`RunnerInternalCommand::LazyBuildComplete`]
    /// arrives. Wrapped in [`AbortOnDrop`] for the same reason as
    /// [`Self::batch_build_handle`]: on shutdown we abort any in-flight
    /// JIT builds so bazel/turbo output stops streaming before
    /// "shutdown complete" is emitted.
    lazy_build_handles: HashMap<String, crate::build_tool::AbortOnDrop<()>>,

    /// Detached file-watch build-tool rebuild batch, if one is in flight.
    rebuild_batch_handle: Option<crate::build_tool::AbortOnDrop<()>>,

    /// Detached build-graph re-query batch, if one is in flight.
    graph_requery_handle: Option<crate::build_tool::AbortOnDrop<()>>,

    // Don's own PID file
    _don_pid_file: Option<PidFile>,

    /// Sender for pushing watch pattern updates to the WatchManager.
    /// Used after build tool re-queries to update tier-2 watch patterns.
    watch_update_tx: Option<mpsc::Sender<crate::watch::WatchUpdate>>,
    /// Sender for querying the live watch manager state for verbose status.
    watch_query_tx: Option<mpsc::Sender<crate::watch::WatchQuery>>,

    /// Mutex to serialize Bazel build invocations. Concurrent `bazel build`
    /// commands contend for Bazel's server lock, so we queue them.
    bazel_build_mutex: std::sync::Arc<tokio::sync::Mutex<()>>,

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

    /// Per-param completion results cache. Populated as the TUI / CLI
    /// resolves completions.
    completion_cache: std::sync::Arc<tokio::sync::RwLock<completions::CompletionCache>>,

    /// Internal shutdown flag broadcast to detached control workers so they
    /// can force-kill promptly when don is exiting.
    shutdown_flag_tx: tokio::sync::watch::Sender<bool>,
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
    ) -> Result<Self, RunnerError> {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (internal_tx, internal_rx) = mpsc::channel(64);
        let (event_tx, _) = broadcast::channel(256);
        let (lazy_start_tx, lazy_start_rx) = mpsc::channel(16);
        let (shutdown_flag_tx, _shutdown_flag_rx) = tokio::sync::watch::channel(false);

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
                if let Some(crate::config::ServiceKind::Docker(d)) = &svc.kind {
                    Some(d.container.clone().unwrap_or_else(|| format!("don-{name}")))
                } else {
                    None
                }
            })
            .collect();
        let cleanup_report = crate::process::cleanup::run_cleanup(&base_dir, &docker_names).await;
        if cleanup_report.pid_files_removed > 0
            || cleanup_report.sock_removed
            || cleanup_report.containers_removed > 0
        {
            output_manager.lifecycle_event(&format!("cleaned stale state: {cleanup_report}"));
        }
        for warning in &cleanup_report.warnings {
            output_manager.error_event(warning);
        }

        // Connect to Docker if any service uses the docker preset.
        let has_docker = config
            .services
            .values()
            .any(|s| matches!(&s.kind, Some(crate::config::ServiceKind::Docker(_))));
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
            let prof = config
                .profiles
                .get(profile_name)
                .ok_or_else(|| RunnerError::Config(format!("unknown profile '{profile_name}'")))?;
            Some(resolve_profile_items_for_platform(&config, prof, platform))
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
                let mut resolved = svc.resolve(platform);
                resolved.depends_on = config.expand_dependency_refs(&resolved.depends_on);
                services.insert(
                    name.clone(),
                    RuntimeService::new(resolved, ServiceState::Pending),
                );
            }
        }

        let task_state = TaskState::new(base_dir.join(".don").join("task-state"));
        let mut tasks = HashMap::new();
        for (name, task) in &config.tasks {
            if active_tasks.contains(name) {
                let mut task = task.clone();
                task.depends_on = config.expand_dependency_refs(&task.depends_on);
                let has_success = task_state.has_success(name).await.unwrap_or(false);
                tasks.insert(
                    name.clone(),
                    RuntimeTask::new(task, TaskItemState::Pending, has_success),
                );
            }
        }

        Ok(Self {
            config,
            platform,
            output_manager,
            base_dir,
            services,
            tasks,
            lazy_start_rx,
            lazy_start_tx,
            server_shutdown_tx: None,
            docker_client,
            cmd_tx,
            cmd_rx,
            internal_tx,
            internal_rx,
            event_tx,
            done_tx: None,
            shutdown_rx: Some(shutdown_rx),
            _don_pid_file: Some(don_pid_file),
            watch_update_tx: None,
            watch_query_tx: None,
            batch_build_handle: None,
            lazy_build_handles: HashMap::new(),
            rebuild_batch_handle: None,
            graph_requery_handle: None,
            bazel_build_mutex: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            pending_bt_rebuilds: Vec::new(),
            bt_rebuild_deadline: None,
            pending_graph_requery: Vec::new(),
            bt_requery_deadline: None,
            completion_cache: std::sync::Arc::new(tokio::sync::RwLock::new(
                completions::CompletionCache::default(),
            )),
            shutdown_flag_tx,
        })
    }

    /// Get a sender for sending commands to this runner.
    /// Transition a service to a new state and broadcast the change.
    ///
    /// The broadcast is the whole point — `RuntimeService::set_state` is
    /// `#[must_use]` precisely so the event can't be forgotten. No-op if
    /// the service is unknown or already at `new_state`.
    pub(crate) fn set_service_state(&mut self, name: &str, new_state: ServiceState) {
        let changed = self
            .services
            .get_mut(name)
            .and_then(|rs| rs.set_state(new_state));
        if let Some(state) = changed {
            let _ = self.event_tx.send(RunnerEvent::ServiceStateChanged {
                name: name.to_string(),
                state,
            });
        }
    }

    /// Transition a task to a new state and broadcast the change.
    pub(crate) fn set_task_state(&mut self, name: &str, new_state: TaskItemState) {
        let changed = self
            .tasks
            .get_mut(name)
            .and_then(|rt| rt.set_state(new_state));
        if let Some(state) = changed {
            let _ = self.event_tx.send(RunnerEvent::TaskStateChanged {
                name: name.to_string(),
                state,
            });
        }
    }

    pub fn command_sender(&self) -> mpsc::Sender<RunnerCommand> {
        self.cmd_tx.clone()
    }

    /// Subscribe to runner events.
    pub fn subscribe(&self) -> broadcast::Receiver<RunnerEvent> {
        self.event_tx.subscribe()
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

        // Take ownership of the shutdown receiver up front so the slow
        // startup phase (build-tool resolution + batch builds) can `select!`
        // on it without conflicting with `&mut self` borrows. Always `Some`
        // here — the field is set by `Runner::new` and only consumed here.
        let mut shutdown_rx = match self.shutdown_rx.take() {
            Some(rx) => rx,
            None => return Ok(()),
        };

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

        // Register synthetic "bazel" / "turbo" streams so build-tool output
        // gets a color-coded prefix column like real services, instead of
        // riding on `[don]` lifecycle events with a `bazel:` text prefix.
        let has_bazel = self
            .services
            .values()
            .any(|rs| rs.resolved.bazel_config().is_some())
            || self.config.tasks.values().any(|t| t.bazel.is_some());
        let has_turbo = self
            .services
            .values()
            .any(|rs| rs.resolved.turbo_config().is_some())
            || self.config.tasks.values().any(|t| t.turbo.is_some());
        if has_bazel {
            self.output_manager.register_build_tool("bazel").await;
        }
        if has_turbo {
            self.output_manager.register_build_tool("turbo").await;
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
            match crate::proxy::ServiceProxy::bind(
                &proxy_config,
                lazy_tx,
                name,
                self.output_manager.clone_lifecycle_emitter(),
            )
            .await
            {
                Ok(proxy) => {
                    let addrs: Vec<String> =
                        proxy.listen_addrs().iter().map(|a| a.to_string()).collect();
                    self.output_manager.service_debug_event(
                        name,
                        &format!("proxy listening on {}", addrs.join(", ")),
                    );
                    if let Some(rs) = self.services.get_mut(name) {
                        rs.proxy = Some(proxy);
                    }
                    // Set lazy services to Lazy state (they won't enter the
                    // startup flow until triggered by a connection).
                    if *is_lazy {
                        self.set_service_state(name, ServiceState::Lazy);
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
                let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::watch::channel(false);
                let cmd_tx_for_server = self.cmd_tx.clone();
                let socket_path_for_server = socket_path.clone();
                let server_emitter = self.output_manager.clone_lifecycle_emitter();
                tokio::spawn(async move {
                    if let Err(e) = crate::server::serve_api(
                        listener,
                        socket_path_for_server,
                        cmd_tx_for_server,
                        server_shutdown_rx,
                    )
                    .await
                    {
                        server_emitter.lifecycle_event(&format!("api server error: {e}"));
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
        let mut watch_handle: Option<tokio::task::JoinHandle<()>> = None;
        let (watch_update_tx, watch_update_rx) = mpsc::channel(64);
        self.watch_update_tx = Some(watch_update_tx);
        let (watch_query_tx, watch_query_rx) = mpsc::channel(8);
        self.watch_query_tx = Some(watch_query_tx);
        // `WatchManager::new` calls `notify::Watcher::watch`, which is
        // synchronous and walks directory trees under the hood — offload
        // to a blocking thread so the runner's main task stays polled.
        // Race it against `shutdown_rx` so Ctrl+C during watch setup
        // shuts down cleanly even if setup ever gets slow again.
        let config_for_watch = self.config.clone();
        let platform_for_watch = self.platform;
        let base_dir_for_watch = self.base_dir.clone();
        let cmd_tx_for_watch = self.cmd_tx.clone();
        let runner_events_for_watch = self.event_tx.subscribe();
        let emitter_for_watch = self.output_manager.clone_lifecycle_emitter();
        let mut watch_setup_handle = tokio::task::spawn_blocking(move || {
            WatchManager::new(
                &config_for_watch,
                platform_for_watch,
                &base_dir_for_watch,
                cmd_tx_for_watch,
                runner_events_for_watch,
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
                self.output_manager.shutdown().await;
                return Ok(());
            }
            r = &mut watch_setup_handle => r,
        };
        match watch_result {
            Ok(Ok((watch_mgr, warnings))) => {
                for warning in &warnings {
                    self.output_manager.error_event(warning);
                }
                if watch_mgr.has_watches() {
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

        // Kick off batch builds (bazel/turbo) as a detached task. The runner
        // keeps processing the main command loop — shutdown signals,
        // connection-triggered lazy starts, and non-build-tool services all
        // stay responsive while bazel crunches. On completion the task posts
        // `RunnerInternalCommand::BatchBuildComplete`, which transitions `Building`
        // items to `Pending`/`Failed` and triggers the ready-item sweep.
        //
        // The handle is stored as `AbortOnDrop` on `self` so `Shutdown` drops
        // the in-flight `Child`, whose `kill_on_drop(true)` sends SIGKILL to
        // the bazel/turbo client.
        let batch_items = self.collect_batch_build_items();
        for item in &batch_items {
            match item.kind {
                NodeKind::Service => self.set_service_state(&item.name, ServiceState::Building),
                NodeKind::Task => self.set_task_state(&item.name, TaskItemState::Building),
            }
        }
        if !batch_items.is_empty() {
            self.spawn_startup_batch_build(batch_items);
        }

        // Build dependency map and topological order.
        let dep_map = self.build_dep_map();
        let order = topological_sort(&dep_map).map_err(|cycle| RunnerError::Cycle { cycle })?;

        // Channel for item completion notifications. Store the sender on `self`
        // so later-started services (lazy starts, pending-sweep) can reuse it.
        let (done_tx, mut done_rx) = mpsc::channel::<ItemDone>(64);
        self.done_tx = Some(done_tx.clone());

        // Track which items are in flight. Only include items that are in the
        // active set (all items, or profile subset). Items not in service_states
        // or task_states are excluded (e.g. services not in the selected profile).
        let mut pending: HashSet<String> = order
            .iter()
            .filter(|name| self.services.contains_key(*name) || self.tasks.contains_key(*name))
            .cloned()
            .collect();
        let mut in_flight: HashSet<String> = HashSet::new();

        // Start items whose dependencies are already satisfied.
        let startup_shutdown_requested = if self
            .start_ready_items(&order, &dep_map, &mut pending, &mut in_flight, &done_tx)
            .await?
        {
            self.initiate_shutdown().await;
            true
        } else {
            false
        };

        let mut all_started = false;

        // Main loop: wait for completions, commands, and signals.
        if !startup_shutdown_requested {
            loop {
                if shutdown_requested() {
                    break;
                }

                // Emit "all services running" once when startup is complete.
                if !all_started && pending.is_empty() && in_flight.is_empty() {
                    all_started = true;
                    let has_running_services = self.services.values().any(|rs| {
                        matches!(
                            rs.state(),
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
                        if self
                            .start_ready_items(
                                &order,
                                &dep_map,
                                &mut pending,
                                &mut in_flight,
                                &done_tx,
                            )
                            .await?
                        {
                            self.initiate_shutdown().await;
                            break;
                        }
                    }
                    Some(cmd) = self.cmd_rx.recv() => {
                        match cmd {
                            RunnerCommand::Shutdown => {
                                self.initiate_shutdown().await;
                                break;
                            }
                            RunnerCommand::Status { verbose, reply } => {
                                let statuses = self.collect_status(verbose).await;
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
                            RunnerCommand::Attach { name, pid, reply } => {
                                self.handle_attach_cmd(&name, pid, reply).await;
                            }
                            RunnerCommand::Detach { name, pty_write } => {
                                self.handle_detach(&name, pty_write).await;
                            }
                            RunnerCommand::Rebuild { name } => {
                                self.handle_rebuild(&name).await;
                            }
                            RunnerCommand::RebuildStale { name } => {
                                self.mark_rebuild_stale(&name);
                            }
                            RunnerCommand::TaskRerun { name } => {
                                self.handle_task_rerun(&name).await;
                            }
                            RunnerCommand::BuildGraphChanged { name } => {
                                self.handle_build_graph_changed(&name).await;
                            }
                            RunnerCommand::StartPending => {
                                self.start_pending_items().await;
                            }
                            RunnerCommand::RunPendingTasks { reply } => {
                                self.handle_run_pending_tasks(reply).await;
                            }
                            RunnerCommand::RunTask { name, params, reply } => {
                                self.handle_run_task(&name, params, reply).await;
                            }
                            RunnerCommand::ResolveCompletions {
                                task,
                                param,
                                partial,
                                force_refresh,
                                reply,
                            } => {
                                self.handle_resolve_completions(
                                    &task,
                                    &param,
                                    partial,
                                    force_refresh,
                                    reply,
                                )
                                .await;
                            }
                        }
                    }
                    Some(cmd) = self.internal_rx.recv() => {
                        match cmd {
                            RunnerInternalCommand::TaskRunPrepared {
                                name,
                                op_id,
                                task_cfg,
                                intent,
                                result,
                            } => {
                                self.handle_task_run_prepared(&name, op_id, &task_cfg, intent, result)
                                    .await;
                            }
                            RunnerInternalCommand::ServiceStopComplete { name, op_id, result } => {
                                self.handle_service_stop_complete(&name, op_id, result).await;
                            }
                            RunnerInternalCommand::ServiceStartPrepared {
                                name,
                                op_id,
                                context,
                                intent,
                                result,
                            } => {
                                self.handle_service_start_prepared(
                                    &name,
                                    op_id,
                                    context,
                                    intent,
                                    result,
                                )
                                .await;
                            }
                            RunnerInternalCommand::ServiceRebuildPrepared {
                                name,
                                op_id,
                                result,
                            } => {
                                self.handle_service_rebuild_prepared(&name, op_id, result)
                                    .await;
                            }
                            RunnerInternalCommand::TaskExited {
                                name,
                                pgid,
                                success,
                                message,
                                elapsed,
                                rerun,
                            } => {
                                self.handle_task_exit(&name, pgid, success, message, elapsed, rerun);
                            }
                            RunnerInternalCommand::ServiceHealthChanged { name, healthy } => {
                                self.handle_service_health_changed(&name, healthy).await;
                            }
                            RunnerInternalCommand::AutoRestart { name, attempt } => {
                                self.handle_auto_restart(&name, attempt).await;
                            }
                            RunnerInternalCommand::ServiceExited { name, pgid } => {
                                self.handle_service_exited(&name, pgid).await;
                            }
                            RunnerInternalCommand::ReadyCheckComplete { name, success } => {
                                self.handle_ready_check_complete(&name, success);
                            }
                            RunnerInternalCommand::BatchBuildComplete(outcome) => {
                                // Drop the abort-on-drop handle: the task is done,
                                // and leaving the handle live would abort after the
                                // task has already returned (harmless but noisy).
                                self.batch_build_handle = None;
                                let replay_items = outcome.replay_items.clone();
                                // Pull failed names out of the pending set before
                                // applying the outcome. `apply_batch_build_outcome`
                                // transitions them to `Failed`, but leaving them in
                                // `pending` would let `start_ready_items` try to
                                // spawn a failed service.
                                for (name, _) in &outcome.failed {
                                    pending.remove(name);
                                }
                                self.apply_batch_build_outcome(outcome);
                                self.schedule_startup_batch_replays(&replay_items);
                                if self
                                    .start_ready_items(
                                        &order,
                                        &dep_map,
                                        &mut pending,
                                        &mut in_flight,
                                        &done_tx,
                                    )
                                    .await?
                                {
                                    self.initiate_shutdown().await;
                                    break;
                                }
                            }
                            RunnerInternalCommand::RebuildBatchComplete(outcome) => {
                                self.rebuild_batch_handle = None;
                                self.handle_rebuild_batch_complete(outcome).await;
                            }
                            RunnerInternalCommand::LazyBuildComplete { name, outcome } => {
                                // Drop the abort-on-drop handle: the task is done,
                                // and leaving it live would abort after the task
                                // has already returned (harmless but noisy).
                                self.lazy_build_handles.remove(&name);
                                // Single-service JIT build triggered by a first
                                // proxy connection. `apply_batch_build_outcome`
                                // flips Building → Pending on success or →
                                // Failed on build error; on success we then
                                // queue the detached service-start worker to
                                // take it through Pending → Starting → Ready
                                // like any cold start.
                                let replay_items = outcome.replay_items.clone();
                                let succeeded = outcome.succeeded.contains(&name);
                                self.apply_batch_build_outcome(outcome);
                                let replayed = replay_items
                                    .iter()
                                    .find(|item| item.name == name)
                                    .is_some_and(|item| self.schedule_lazy_build_replay(item));
                                if succeeded
                                    && !replayed
                                    && self
                                        .services
                                        .get(&name)
                                        .is_some_and(|rs| rs.state() == ServiceState::Pending)
                                {
                                    self.output_manager.service_event(
                                        &name,
                                        "lazy build complete, starting",
                                    );
                                    if let Err(e) = self.queue_startup_service_start(
                                        &name,
                                        done_tx.clone(),
                                        ServiceStartMode::SpawnOnly,
                                    ) {
                                        self.output_manager
                                            .service_error_event(&name, &e.to_string());
                                    }
                                }
                            }
                            RunnerInternalCommand::GraphRequeryComplete(outcomes) => {
                                self.graph_requery_handle = None;
                                self.handle_graph_requery_complete(outcomes).await;
                            }
                        }
                    }
                    Some(name) = self.lazy_start_rx.recv() => {
                        // Only act on the first connection — subsequent connections
                        // (during JIT build or start) find the service in a non-Lazy
                        // state and are ignored. Connections still queue at the
                        // proxy; they get forwarded once the backend is Ready.
                        if !self
                            .services
                            .get(&name)
                            .is_some_and(|rs| rs.state() == ServiceState::Lazy)
                        {
                            continue;
                        }
                        let needs_jit = self
                            .services
                            .get(&name)
                            .is_some_and(|rs| rs.resolved.is_build_tool_managed() && !rs.batch_built);
                        if needs_jit {
                            let item = match self.services.get(&name) {
                                Some(rs) => self.build_batch_item(&name, NodeKind::Service, rs),
                                None => continue,
                            };
                            self.output_manager.service_event(
                                &name,
                                "first connection — building before start",
                            );
                            self.set_service_state(&name, ServiceState::Building);
                            self.spawn_lazy_build(&name, item);
                        } else {
                            self.output_manager
                                .service_event(&name, "first connection — starting service");
                            if let Err(e) = self.queue_startup_service_start(
                                &name,
                                done_tx.clone(),
                                ServiceStartMode::Full,
                            ) {
                                self.output_manager
                                    .service_error_event(&name, &e.to_string());
                            }
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
                    _ = shutdown_rx.recv() => {
                        self.initiate_shutdown().await;
                        break;
                    }
                }
            }
        }

        // Wait for any remaining service exits during shutdown.
        self.wait_for_shutdown().await;

        // Stop the API server (no-op if already signalled by initiate_shutdown).
        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(true);
        }

        // Abort the watch task so its LifecycleEmitter (which holds a clone of
        // the stdout sink sender) drops. Otherwise the subsequent output
        // shutdown blocks forever waiting for writer tasks to drain.
        if let Some(handle) = watch_handle.take() {
            handle.abort();
            let _ = handle.await;
        }

        // Shut down the output system — flush all pending messages to sinks.
        self.output_manager.shutdown().await;

        Ok(())
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
        let mut spawned_pgid: Option<i32> = None;
        if let Some(rs) = self.services.get_mut(name) {
            if let ServiceHandle::Process(ref proc) = start_result.handle {
                spawned_pgid = Some(proc.pgid());
            }
            rs.handle = Some(start_result.handle);

            // Add OSC response sink if we have a PTY write handle.
            if let Some(ServiceHandle::Process(process)) = rs.handle.as_mut()
                && let Some(pty) = process.take_pty_write()
                && let Some(osc_handle) = self.output_manager.add_osc_sink(name, pty).await
            {
                rs.osc_sink = Some(osc_handle);
            }
        }
        if let Some(pgid) = spawned_pgid {
            self.output_manager
                .service_debug_event(name, &format!("spawned pid={pgid}"));
        }
        self.fulfill_pending_waiter(name).await;
        self.set_service_state(name, ServiceState::Running);

        // Wire up output processing. We need to fan the EOF (= process died)
        // out to two independent waiters: the ready check (cancels its
        // retry loop), and the crash watcher (reports the exit upstream so
        // the runner can reap the child and transition state).
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        let (crash_exit_tx, crash_exit_rx) = tokio::sync::oneshot::channel();
        if let Some(svc_writer) = self.output_manager.service_writer(name) {
            let child_output = start_result.child_output;
            tokio::spawn(async move {
                let _ = svc_writer.process_stream(child_output).await;
                let _ = exit_tx.send(());
                let _ = crash_exit_tx.send(());
            });
        }

        // Crash watcher — fires `ServiceExited` to the runner when the
        // child's output stream EOFs. Skipped for Docker because the
        // bollard log stream's EOF semantics aren't yet wired to a status
        // code path. The pgid lets the handler ignore stale events that
        // arrive after the service has already been respawned.
        if let Some(pgid) = spawned_pgid {
            let cmd_tx = self.internal_tx.clone();
            let watch_name = name.to_string();
            tokio::spawn(async move {
                let _ = crash_exit_rx.await;
                let _ = cmd_tx
                    .send(RunnerInternalCommand::ServiceExited {
                        name: watch_name,
                        pgid,
                    })
                    .await;
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
            // If the new instance has `monitor = true`, build the cancel
            // channel up front and stash the sender on the RuntimeService —
            // the spawned task spawns the monitor on Ready and uses the
            // matching receiver. Stop/restart cancels by dropping the sender.
            let monitor_cancel_rx = if ready.monitor {
                if let Some(rs) = self.services.get_mut(name) {
                    rs.stop_health_tracking();
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    rs.monitor_cancel = Some(tx);
                    Some(rx)
                } else {
                    None
                }
            } else {
                None
            };
            let cmd_tx_for_monitor = self.internal_tx.clone();
            let cmd_tx_for_state = self.internal_tx.clone();
            tokio::spawn(async move {
                let ready_result = tokio::select! {
                    result = service::run_ready_check(&ready) => result,
                    _ = exit_rx => {
                        Err(service::ServiceError::ProcessExitedDuringReadyCheck)
                    }
                };

                let success = ready_result.is_ok();

                // Activate proxy backend once the service is ready.
                if success && let Some(ref handle) = proxy_handle {
                    handle.activate();
                }

                // State update:
                //   done_tx path → runner's handle_service_done flips state
                //     via set_service_state (which broadcasts). Don't
                //     duplicate it here.
                //   no-done_tx path (manual start / rebuild) → no
                //     handle_service_done gets called, so send a command so
                //     the runner can flip state on its own task. Without
                //     this, internal state stays at Running and later
                //     health-monitor probes short-circuit.
                if done_tx.is_none() {
                    let _ = cmd_tx_for_state
                        .send(RunnerInternalCommand::ReadyCheckComplete {
                            name: name_owned.clone(),
                            success,
                        })
                        .await;
                }

                // Kick off the long-lived health monitor once Ready, if
                // configured. The cancel rx exists only when ready.monitor
                // was true at wire-up time, so this branch needs no extra check.
                if success && let Some(cancel_rx) = monitor_cancel_rx {
                    let monitor_name = name_owned.clone();
                    tokio::spawn(async move {
                        run_health_monitor(monitor_name, ready, cmd_tx_for_monitor, cancel_rx)
                            .await;
                    });
                }

                if let Some(done_tx) = done_tx {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name_owned,
                            kind: NodeKind::Service,
                            success,
                            message: ready_result.err().map(|e| e.to_string()),
                            elapsed: None,
                            task_run_generation: None,
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
            // No ready check, initial startup path — just signal completion.
            // `handle_service_done` flips state to Ready and emits the
            // "{name} started" lifecycle event; doing either here as well
            // would double-log and duplicate the state transition.
            let _ = done_tx
                .send(ItemDone {
                    name: name.to_string(),
                    kind: NodeKind::Service,
                    success: true,
                    message: None,
                    elapsed: None,
                    task_run_generation: None,
                })
                .await;
        } else {
            // No ready check, rebuild path — mark ready immediately.
            self.set_service_state(name, ServiceState::Ready);
            self.unblock_dependency_failed_items();
            self.output_manager.service_event(name, "restarted");
            let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                name: name.to_string(),
                success: true,
            });
        }
    }

    fn spawn_task_worker(
        &mut self,
        name: &str,
        task_cfg: crate::config::Task,
        params: HashMap<String, String>,
        mode: TaskRunMode,
        intent: TaskRunIntent,
    ) -> Result<(), CommandError> {
        let Some(rt) = self.tasks.get_mut(name) else {
            return Err(CommandError::UnknownTask {
                name: name.to_string(),
            });
        };
        rt.run_generation = rt.run_generation.saturating_add(1);
        let op_id = rt.run_generation;

        let cmd_tx = self.internal_tx.clone();
        let base_dir = self.base_dir.clone();
        let platform = self.platform;
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let name_owned = name.to_string();
        let task_cfg_for_worker = task_cfg.clone();
        let global_watch_ignore = self.config.watch_ignore.clone();
        let worker = tokio::spawn(async move {
            let ctx = TaskWorkerContext {
                base_dir,
                platform,
                emitter,
                global_watch_ignore,
            };
            let result =
                run_task_worker(ctx, &name_owned, &task_cfg_for_worker, &params, mode).await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::TaskRunPrepared {
                    name: name_owned,
                    op_id,
                    task_cfg: Box::new(task_cfg),
                    intent,
                    result,
                })
                .await;
        });
        rt.run_worker = Some(worker);
        Ok(())
    }

    async fn handle_task_run_prepared(
        &mut self,
        name: &str,
        op_id: u64,
        task_cfg: &crate::config::Task,
        intent: TaskRunIntent,
        result: Result<TaskRunPrepared, String>,
    ) {
        let is_current = self
            .tasks
            .get(name)
            .is_some_and(|rt| rt.run_generation == op_id);
        if !is_current {
            if let Ok(TaskRunPrepared::Spawned(spawn)) = result {
                let task::TaskSpawn {
                    handle,
                    child_output,
                    rendered_cmdline: _rendered_cmdline,
                } = *spawn;
                drop(child_output);
                tokio::spawn(async move {
                    let mut handle = handle;
                    let _ = handle
                        .terminate(
                            nix::sys::signal::Signal::SIGKILL,
                            std::time::Duration::from_millis(500),
                        )
                        .await;
                });
            }
            return;
        }
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.run_worker = None;
        }

        match result {
            Ok(TaskRunPrepared::PendingRun { message }) => {
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.set_needs_run_now(true);
                }
                self.set_task_state(name, TaskItemState::PendingRun);
                self.output_manager.service_event(name, &message);
                if let TaskRunIntent::Startup { done_tx } = intent {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name.to_string(),
                            kind: NodeKind::Task,
                            success: true,
                            message: None,
                            elapsed: None,
                            task_run_generation: None,
                        })
                        .await;
                }
            }
            Ok(TaskRunPrepared::Skipped { message }) => {
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.set_needs_run_now(false);
                }
                self.set_task_state(name, TaskItemState::Skipped);
                self.output_manager.service_event(name, &message);
                if let TaskRunIntent::Startup { done_tx } = intent {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name.to_string(),
                            kind: NodeKind::Task,
                            success: true,
                            message: None,
                            elapsed: None,
                            task_run_generation: None,
                        })
                        .await;
                }
            }
            Ok(TaskRunPrepared::Spawned(spawn)) => {
                if matches!(intent, TaskRunIntent::Startup { .. })
                    && let Some(rt) = self.tasks.get_mut(name)
                {
                    rt.set_needs_run_now(true);
                }
                self.output_manager.service_debug_event(
                    name,
                    &format!("process spawned (pid {})", spawn.handle.pgid()),
                );
                self.output_manager
                    .service_event(name, &format!("spawn {}", spawn.rendered_cmdline));
                let done_tx = match intent {
                    TaskRunIntent::Startup { done_tx } => {
                        self.output_manager.service_event(name, "running...");
                        self.set_task_state(name, TaskItemState::Running);
                        Some(done_tx)
                    }
                    TaskRunIntent::Background => None,
                };
                self.wire_task_output_and_wait(name, *spawn, task_cfg, done_tx)
                    .await;
            }
            Err(message) => {
                if matches!(intent, TaskRunIntent::Startup { .. })
                    && let Some(rt) = self.tasks.get_mut(name)
                {
                    rt.set_needs_run_now(true);
                }
                self.set_task_state(name, TaskItemState::Failed);
                self.output_manager.service_error_event(name, &message);
                match intent {
                    TaskRunIntent::Startup { done_tx } => {
                        let _ = done_tx
                            .send(ItemDone {
                                name: name.to_string(),
                                kind: NodeKind::Task,
                                success: false,
                                message: Some(message),
                                elapsed: None,
                                task_run_generation: None,
                            })
                            .await;
                    }
                    TaskRunIntent::Background => {
                        let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                            name: name.to_string(),
                            success: false,
                        });
                    }
                }
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
            rendered_cmdline: _rendered_cmdline,
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
        let global_watch_ignore = self.config.watch_ignore.clone();
        let task_state = TaskState::new(base_dir_owned.join(".don").join("task-state"));
        let cmd_tx = self.internal_tx.clone();
        let rerun = done_tx.is_none();

        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = task::wait_for_task(&mut handle, task_cfg_clone.timeout.as_deref()).await;
            let elapsed = start.elapsed();

            let (success, message) = match result {
                Ok(status) => {
                    if status.success() {
                        let task_dir =
                            working_dir_for(&base_dir_owned, task_cfg_clone.dir.as_deref());
                        let ignore_patterns = resolve_watch_ignore_patterns(
                            &task_dir,
                            &task_cfg_clone.ignore,
                            &base_dir_owned,
                            &global_watch_ignore,
                        );
                        let _ = task_state
                            .record_success(
                                &name_owned,
                                &task_cfg_clone.watch,
                                &ignore_patterns,
                                Some(&task_dir),
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

            if let Some(done_tx) = done_tx {
                let _ = done_tx
                    .send(ItemDone {
                        name: name_owned,
                        kind: NodeKind::Task,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        task_run_generation: None,
                    })
                    .await;
            } else {
                let _ = cmd_tx
                    .send(RunnerInternalCommand::TaskExited {
                        name: name_owned,
                        pgid,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        rerun,
                    })
                    .await;
            }
        });
    }

    async fn stop_task_pgid(&mut self, name: &str, pgid: i32) -> CommandResult {
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        self.output_manager
            .service_event(name, "stopping... (requested)");

        match nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pgid),
            nix::sys::signal::Signal::SIGKILL,
        ) {
            Ok(()) | Err(nix::Error::ESRCH) => {}
            Err(e) => {
                return Err(CommandError::Failed {
                    name: name.to_string(),
                    message: format!("failed to kill task pgid {pgid}: {e}"),
                });
            }
        }

        Ok(())
    }

    async fn handle_restart_task_cmd(&mut self, name: &str) -> CommandResult {
        let (task_cfg, last_params, state, pgid) = match self.tasks.get(name) {
            Some(rt) => (
                rt.config.clone(),
                rt.last_params.clone(),
                rt.state(),
                rt.pgid,
            ),
            None => {
                return Err(CommandError::UnknownTask {
                    name: name.to_string(),
                });
            }
        };

        if !task_cfg.params.is_empty() && last_params.len() < task_cfg.params.len() {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "task has params and no previous invocation to restart; use `don run`"
                    .to_string(),
            });
        }

        if matches!(state, TaskItemState::Running | TaskItemState::Building)
            && let Some(pgid) = pgid
        {
            self.stop_task_pgid(name, pgid).await?;
        }

        self.spawn_task_rerun(name, &task_cfg, &last_params, "restarting (manual trigger)")
            .await;
        Ok(())
    }

    /// Apply a health-monitor probe transition for a service.
    ///
    /// Only acts when the service is in `Ready` (failure → `Unhealthy`)
    /// or `Unhealthy` (recovery → `Ready`). Stale messages from a monitor
    /// task whose service has since stopped/restarted are ignored.
    async fn handle_service_health_changed(&mut self, name: &str, healthy: bool) {
        let current = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        if healthy {
            if current != ServiceState::Unhealthy {
                return;
            }
            self.set_service_state(name, ServiceState::Ready);
            let attempts = self
                .services
                .get(name)
                .map(|rs| rs.restart_attempts)
                .unwrap_or(0);
            if let Some(rs) = self.services.get_mut(name) {
                if let Some(handle) = rs.pending_restart.take() {
                    handle.abort();
                }
                rs.restart_attempts = 0;
            }
            let msg = if attempts > 0 {
                "recovered (cancelled pending restart, attempts reset)"
            } else {
                "recovered (health check passing)"
            };
            self.output_manager.service_event(name, msg);
        } else {
            if current != ServiceState::Ready {
                return;
            }
            self.set_service_state(name, ServiceState::Unhealthy);
            let policy = self
                .services
                .get(name)
                .map(|rs| rs.resolved.on_failure)
                .unwrap_or_default();
            match policy {
                crate::config::OnFailure::Notify => {
                    self.output_manager
                        .service_error_event(name, "unhealthy (health check failing)");
                }
                crate::config::OnFailure::Restart => {
                    self.schedule_auto_restart(name, "unhealthy");
                }
            }
        }
    }

    /// Schedule an automatic restart for a failed service. Used for both
    /// `Unhealthy` (monitor-driven) and `Failed` (crash-driven) failures.
    /// Uses exponential backoff (1, 2, 4, 8, 16, 32, 60s) on consecutive
    /// attempts. Replaces any already-scheduled restart for this service.
    /// `reason` is included verbatim in the lifecycle event so a reader
    /// can tell *why* the restart was scheduled.
    fn schedule_auto_restart(&mut self, name: &str, reason: &str) {
        let attempt = self
            .services
            .get(name)
            .map(|rs| rs.restart_attempts.saturating_add(1))
            .unwrap_or(1);
        let backoff_secs = unhealthy_restart_backoff_secs(attempt);
        self.output_manager.service_error_event(
            name,
            &format!("{reason} — auto-restart in {backoff_secs}s (attempt {attempt})"),
        );
        let cmd_tx = self.internal_tx.clone();
        let name_owned = name.to_string();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::AutoRestart {
                    name: name_owned,
                    attempt,
                })
                .await;
        });
        if let Some(rs) = self.services.get_mut(name)
            && let Some(prev) = rs.pending_restart.replace(handle)
        {
            prev.abort();
        }
    }

    /// Handle an unexpected exit reported by the per-spawn crash watcher.
    ///
    /// The watcher fires whenever the child's output stream EOFs — that
    /// happens for *both* crashes and graceful stops. To distinguish them:
    /// 1. Compare `pgid` against the live handle. A mismatch means the
    ///    service has already been respawned; the event is a leftover
    ///    from the previous instance.
    /// 2. Filter on state. Stop / restart / shutdown paths take the
    ///    handle and transition to `Stopping`/`Stopped`/`Failed` *before*
    ///    the EOF arrives, so we only act if the runner still believes
    ///    the service is `Running`/`Ready`/`Unhealthy`.
    ///
    /// On a real crash, reap the child to get the actual `ExitStatus`,
    /// cancel the monitor + any pending auto-restart, transition to
    /// `Failed`, and emit a lifecycle event with the code or signal.
    async fn handle_service_exited(&mut self, name: &str, pgid: i32) {
        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        if !matches!(
            state,
            ServiceState::Running | ServiceState::Ready | ServiceState::Unhealthy
        ) {
            return;
        }
        let current_pgid = self.services.get(name).and_then(|rs| match &rs.handle {
            Some(ServiceHandle::Process(p)) => Some(p.pgid()),
            _ => None,
        });
        if current_pgid != Some(pgid) {
            // Stale event from a previous spawn. The current handle is a
            // different instance; ignore.
            return;
        }
        let handle = match self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
            Some(h) => h,
            None => return,
        };
        // Reap the child to surface the real exit status. The wait()
        // returns near-instantly because the process is already gone (we
        // got here on EOF) — we're really just collecting the status code.
        let status = if let ServiceHandle::Process(mut proc) = handle {
            proc.wait().await.ok()
        } else {
            None
        };
        if let Some(rs) = self.services.get_mut(name) {
            rs.stop_health_tracking();
        }
        // Exit code 0 = clean self-shutdown; never treated as a failure.
        // The service didn't ask to be restarted — it asked to exit. We
        // mirror the graceful-stop transition (Stopped, no error) and
        // reset restart_attempts in case the service had been flapping
        // before deciding to exit cleanly.
        let clean_exit = status.as_ref().is_some_and(|s| s.success());
        if clean_exit {
            if let Some(rs) = self.services.get_mut(name) {
                rs.restart_attempts = 0;
            }
            self.set_service_state(name, ServiceState::Stopped);
            self.output_manager
                .service_event(name, "exited cleanly (status 0)");
            if let Some(writer) = self.output_manager.service_writer(name) {
                writer.close_follow_sinks().await;
            }
            return;
        }
        self.set_service_state(name, ServiceState::Failed);
        let exit_msg = format_unexpected_exit(status);
        self.output_manager.service_error_event(name, &exit_msg);
        // Apply the on_failure policy. Restart routes through the same
        // backoff machinery as the monitor's Unhealthy → Restart path,
        // so a service that crashes repeatedly backs off the same way.
        let policy = self
            .services
            .get(name)
            .map(|rs| rs.resolved.on_failure)
            .unwrap_or_default();
        if matches!(policy, crate::config::OnFailure::Restart) {
            self.schedule_auto_restart(name, &exit_msg);
        } else if let Some(rs) = self.services.get_mut(name) {
            // Notify policy: nothing else to do, but reset attempts so a
            // later manual restart starts at attempt 1 again.
            rs.restart_attempts = 0;
        }
        // Close follow / attach sinks so anyone watching this service
        // detects the exit instead of blocking forever on the next read.
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }
    }

    /// Handle a backoff-timer-fired auto-restart. Used for both monitor-
    /// driven (`Unhealthy`) and crash-driven (`Failed`) restarts. No-op if
    /// the service has since recovered, stopped, or been restarted manually
    /// — those paths transition state away from Unhealthy/Failed and we
    /// must not re-spawn what the user just stopped.
    async fn handle_auto_restart(&mut self, name: &str, attempt: u32) {
        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        if !matches!(state, ServiceState::Unhealthy | ServiceState::Failed) {
            return;
        }
        if let Some(rs) = self.services.get_mut(name) {
            // Drop the handle to the just-fired pending-restart task — it has
            // already sent the command and completed.
            rs.pending_restart = None;
            rs.restart_attempts = attempt;
        }
        self.output_manager
            .service_event(name, &format!("auto-restart firing (attempt {attempt})"));
        if self
            .services
            .get(name)
            .is_some_and(|rs| rs.handle.is_some())
        {
            let (reply_tx, _reply_rx) = oneshot::channel();
            self.handle_restart_service_cmd(name, reply_tx).await;
        } else {
            let _ = self.queue_background_service_start(name, ServiceStartMode::Full);
        }
    }

    /// Handle a file-watch-triggered task re-run.
    ///
    /// Respects the task's auto-run policy — tasks that should not auto-rerun
    /// from a watch event transition to `PendingRun` instead of spawning.
    /// Explicit-run paths (the user triggering a task via `don run <name>` or
    /// `--all-pending`) bypass this gate by calling [`spawn_task_rerun`]
    /// directly.
    async fn handle_task_rerun(&mut self, name: &str) {
        let task_cfg = match self.tasks.get(name) {
            Some(rt) => rt.config.clone(),
            None => {
                self.output_manager
                    .service_error_event(name, "rerun requested for unknown task");
                let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                    name: name.to_string(),
                    success: false,
                });
                return;
            }
        };

        if self
            .tasks
            .get(name)
            .is_some_and(|rt| rt.state() == TaskItemState::Building)
        {
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        // Skip the needs_run hash check — the file watcher already confirmed
        // a matching file changed. The hash check is only needed at startup
        // (to skip tasks whose inputs haven't changed since the last run).

        // Only `auto_run = true` / `"always"` allows watch-triggered reruns.
        // `"once"` is intentionally startup-only, and `false` / `"never"`
        // keeps the task manual forever.
        if !task_cfg.auto_run.runs_automatically_on_watch() {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.set_needs_run_now(true);
            }
            self.set_task_state(name, TaskItemState::PendingRun);
            let message = match task_cfg.auto_run {
                TaskAutoRun::Always => "files changed (pending)",
                TaskAutoRun::Never => "files changed (pending — auto_run = false)",
                TaskAutoRun::Once => "files changed (pending — auto_run = once)",
            };
            self.output_manager.service_event(name, message);
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        // Tasks that declare params require user-supplied values. File-watch
        // triggers park them in PendingRun so the user can run them explicitly
        // (via the palette's form or `don run <task> --<param>=<value>`).
        if !task_cfg.params.is_empty() {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.set_needs_run_now(true);
            }
            self.set_task_state(name, TaskItemState::PendingRun);
            self.output_manager.service_event(
                name,
                "files changed (pending — task has params, run manually)",
            );
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        self.spawn_task_rerun(
            name,
            &task_cfg,
            &HashMap::new(),
            "re-running (file changed)",
        )
        .await;
    }

    /// Actually spawn a task re-run: release any attach lock, flip to
    /// `Running`, spawn, and wire output. Used by both the file-watch path
    /// ([`handle_task_rerun`]) and the explicit-run paths (`don run <name>`,
    /// `don run --all-pending`).
    ///
    /// `params` is the user-supplied value map; empty for param-less tasks.
    /// Values are substituted into the task's `cmd`/`args`/`env`/`dir` via
    /// `{{name}}` placeholders in [`task::spawn_task`].
    async fn spawn_task_rerun(
        &mut self,
        name: &str,
        task_cfg: &crate::config::Task,
        params: &HashMap<String, String>,
        start_message: &str,
    ) {
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.last_params = params.clone();
            rt.set_needs_run_now(true);
        }
        // Release attach lock and close follow sinks so any active attach
        // session exits cleanly before the new process starts.
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        self.output_manager.service_event(name, start_message);
        self.set_task_state(name, TaskItemState::Running);

        self.output_manager
            .service_debug_event(name, "spawning process...");
        if let Err(e) = self.spawn_task_worker(
            name,
            task_cfg.clone(),
            params.clone(),
            TaskRunMode::Triggered,
            TaskRunIntent::Background,
        ) {
            self.set_task_state(name, TaskItemState::Failed);
            self.output_manager
                .service_error_event(name, &format!("failed to start: {e}"));
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success: false,
            });
        }
    }

    /// Run all tasks currently in PendingRun state.
    async fn handle_run_pending_tasks(&mut self, reply: oneshot::Sender<CommandResult>) {
        let pending: Vec<(String, crate::config::Task)> = self
            .tasks
            .iter()
            .filter(|(_, rt)| rt.state() == TaskItemState::PendingRun)
            .map(|(name, rt)| (name.clone(), rt.config.clone()))
            .collect();

        if pending.is_empty() {
            self.output_manager
                .lifecycle_event("no pending tasks to run");
            let _ = reply.send(Ok(()));
            return;
        }

        // Param'd tasks can't be run here — they need user-supplied values.
        // Skip with a note so the user knows to use the palette or `don run`.
        let (runnable, needs_params): (Vec<_>, Vec<_>) = pending
            .into_iter()
            .partition(|(_, cfg)| cfg.params.is_empty());

        for (name, _) in &needs_params {
            self.output_manager
                .service_event(name, "skipped — task has params, run manually");
        }

        if runnable.is_empty() {
            self.output_manager
                .lifecycle_event("no pending tasks to run (param'd tasks skipped)");
            let _ = reply.send(Ok(()));
            return;
        }

        self.output_manager.lifecycle_event(&format!(
            "running {} pending task{}...",
            runnable.len(),
            if runnable.len() == 1 { "" } else { "s" }
        ));

        let empty_params = HashMap::new();
        for (name, cfg) in &runnable {
            // Explicit-run path — bypass the auto_run gate in handle_task_rerun.
            self.spawn_task_rerun(name, cfg, &empty_params, "running (manual trigger)")
                .await;
        }

        let _ = reply.send(Ok(()));
    }

    /// Run a single task by name, bypassing the `auto_run` gate. Used by
    /// `don run <name>`.
    async fn handle_run_task(
        &mut self,
        name: &str,
        params: HashMap<String, String>,
        reply: oneshot::Sender<CommandResult>,
    ) {
        // Services and unknown names get a dedicated error. Services don't go
        // through "run" at all — that's what start/restart is for.
        if self.services.contains_key(name) {
            let _ = reply.send(Err(CommandError::NotATask {
                name: name.to_string(),
            }));
            return;
        }
        let cfg = match self.tasks.get(name) {
            Some(rt) => rt.config.clone(),
            None => {
                let _ = reply.send(Err(CommandError::UnknownTask {
                    name: name.to_string(),
                }));
                return;
            }
        };

        // Reject while already in flight — otherwise we'd race two spawns of
        // the same task and the output would interleave unpredictably.
        let current = self.tasks.get(name).map(|rt| rt.state());
        if matches!(
            current,
            Some(TaskItemState::Running) | Some(TaskItemState::Building)
        ) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "task is already running".to_string(),
            }));
            return;
        }

        // Resolve params: apply defaults, reject unknown keys, reject
        // missing required values, apply per-kind validation.
        let resolved = match resolve_task_params(name, &cfg, params) {
            Ok(p) => p,
            Err(message) => {
                let _ = reply.send(Err(CommandError::InvalidParams {
                    name: name.to_string(),
                    message,
                }));
                return;
            }
        };

        self.spawn_task_rerun(name, &cfg, &resolved, "running (manual trigger)")
            .await;
        let _ = reply.send(Ok(()));
    }

    /// Resolve candidate values for `param` on `task` by shelling out to its
    /// `completions` command. Does not block the runner's main loop — the
    /// actual command invocation is spawned as a detached tokio task so
    /// slow completions can't freeze status queries or lifecycle events.
    async fn handle_resolve_completions(
        &mut self,
        task: &str,
        param: &str,
        partial: HashMap<String, String>,
        force_refresh: bool,
        reply: oneshot::Sender<Result<Vec<String>, CompletionError>>,
    ) {
        let Some(rt) = self.tasks.get(task) else {
            let _ = reply.send(Err(CompletionError {
                message: format!("unknown task '{task}'"),
                log_path: None,
            }));
            return;
        };
        let param_cfg = rt.config.params.iter().find(|p| p.name == param).cloned();
        let task_env = rt.config.env.clone();
        let Some(p) = param_cfg else {
            let _ = reply.send(Err(CompletionError {
                message: format!("task '{task}' has no param '{param}'"),
                log_path: None,
            }));
            return;
        };
        let Some(completion_cfg) = p.completions.clone() else {
            // Static choices fast-path: return them directly without any
            // shell-out. The TUI form can still fuzzy-filter locally.
            let _ = reply.send(Ok(p.choices.clone()));
            return;
        };

        let cache = self.completion_cache.clone();
        let base_dir = self.base_dir.clone();
        let task_name = task.to_string();
        let param_name = param.to_string();
        tokio::spawn(async move {
            let result = completions::resolve(completions::ResolveRequest {
                cache: &cache,
                task: &task_name,
                param: &param_name,
                completions: &completion_cfg,
                base_dir: &base_dir,
                task_env: &task_env,
                partial: &partial,
                force_refresh,
            })
            .await;
            let _ = reply.send(result);
        });
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
            let message = self
                .services
                .get(&item.name)
                .map(|rs| match &rs.resolved.ready {
                    Some(r) if r.tcp.is_some() => {
                        format!("ready (tcp {})", r.tcp.as_deref().unwrap_or("unknown"))
                    }
                    Some(r) if r.http.is_some() => {
                        format!("ready (http {})", r.http.as_deref().unwrap_or("unknown"))
                    }
                    Some(r) if r.exec.is_some() => "ready (exec)".to_string(),
                    _ => "started".to_string(),
                });
            // Activate proxy backend before state flip so the proxy is ready
            // to forward the moment observers see `Ready`.
            if let Some(rs) = self.services.get(&item.name)
                && let Some(ref proxy) = rs.proxy
            {
                proxy.set_backend();
            }
            self.set_service_state(&item.name, ServiceState::Ready);
            self.unblock_dependency_failed_items();
            if let Some(message) = message {
                self.output_manager.service_event(&item.name, &message);
            }
        } else {
            // If a lazy service fails, reset to Lazy so the next connection
            // can re-trigger it instead of leaving it permanently failed.
            let is_lazy = self
                .services
                .get(&item.name)
                .is_some_and(|rs| rs.resolved.lazy && rs.proxy.is_some());
            if is_lazy {
                self.set_service_state(&item.name, ServiceState::Lazy);
                self.unblock_dependency_failed_items();
                // Re-arm POLLIN watchers on any listenfd proxy entries so
                // the next queued connection re-triggers lazy start.
                if let Some(rs) = self.services.get_mut(&item.name)
                    && let Some(ref mut proxy) = rs.proxy
                {
                    proxy.rearm_lazy_watchers();
                }
                if let Some(ref msg) = item.message {
                    self.output_manager.service_error_event(
                        &item.name,
                        &format!("{msg} (will retry on next connection)"),
                    );
                }
            } else {
                self.set_service_state(&item.name, ServiceState::Failed);
                if let Some(ref msg) = item.message {
                    self.output_manager.service_error_event(&item.name, msg);
                }
            }
        }
    }

    fn handle_task_done(&mut self, item: &ItemDone) {
        if let Some(task_generation) = item.task_run_generation
            && self
                .tasks
                .get(&item.name)
                .is_some_and(|rt| rt.run_generation != task_generation)
        {
            return;
        }
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
            let cur = self.tasks.get(&item.name).map(|rt| rt.state());
            if cur != Some(TaskItemState::Skipped)
                && cur != Some(TaskItemState::PendingRun)
                && let Some(rt) = self.tasks.get_mut(&item.name)
            {
                rt.mark_success();
            }
            if cur != Some(TaskItemState::Skipped)
                && cur != Some(TaskItemState::PendingRun)
                && cur != Some(TaskItemState::Completed)
            {
                self.set_task_state(&item.name, TaskItemState::Completed);
                let msg = if timing.is_empty() {
                    "complete".to_string()
                } else {
                    format!("complete ({timing})")
                };
                self.output_manager.service_event(&item.name, &msg);
            }
            self.unblock_dependency_failed_items();
        } else {
            if let Some(rt) = self.tasks.get_mut(&item.name) {
                rt.set_needs_run_now(true);
            }
            self.set_task_state(&item.name, TaskItemState::Failed);
            if let Some(ref err_msg) = item.message {
                let msg = if timing.is_empty() {
                    format!("failed ({err_msg})")
                } else {
                    format!("failed ({err_msg}, {timing})")
                };
                self.output_manager.service_error_event(&item.name, &msg);
            }
        }
    }

    fn handle_task_exit(
        &mut self,
        name: &str,
        pgid: i32,
        success: bool,
        message: Option<String>,
        elapsed: Option<std::time::Duration>,
        rerun: bool,
    ) {
        if self.tasks.get(name).is_none_or(|rt| rt.pgid != Some(pgid)) {
            return;
        }

        if let Some(rt) = self.tasks.get_mut(name) {
            rt.pgid = None;
            rt.attach_lock = None;
        }

        let timing = elapsed.map(format_duration).unwrap_or_default();
        if success {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.mark_success();
            }
            self.set_task_state(name, TaskItemState::Completed);
            self.unblock_dependency_failed_items();
            let msg = if timing.is_empty() {
                "complete".to_string()
            } else {
                format!("complete ({timing})")
            };
            self.output_manager.service_event(name, &msg);
            let run_generation = self.tasks.get(name).map(|rt| rt.run_generation);
            if let Some(done_tx) = self.done_tx.clone() {
                let name = name.to_string();
                tokio::spawn(async move {
                    let _ = done_tx
                        .send(ItemDone {
                            name,
                            kind: NodeKind::Task,
                            success: true,
                            message: None,
                            elapsed: None,
                            task_run_generation: run_generation,
                        })
                        .await;
                });
            }
        } else {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.set_needs_run_now(true);
            }
            self.set_task_state(name, TaskItemState::Failed);
            if let Some(ref err_msg) = message {
                let msg = if timing.is_empty() {
                    format!("failed ({err_msg})")
                } else {
                    format!("failed ({err_msg}, {timing})")
                };
                self.output_manager.service_error_event(name, &msg);
            }
        }

        if rerun {
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success,
            });
        }
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
    /// Run generation for manually-triggered task completions that need to
    /// re-notify startup dependency resolution. `None` for normal startup
    /// item completions.
    task_run_generation: Option<u64>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn unhealthy_restart_backoff_table() {
        struct Case {
            attempt: u32,
            want_secs: u64,
        }
        let cases = [
            Case {
                attempt: 1,
                want_secs: 1,
            },
            Case {
                attempt: 2,
                want_secs: 2,
            },
            Case {
                attempt: 3,
                want_secs: 4,
            },
            Case {
                attempt: 4,
                want_secs: 8,
            },
            Case {
                attempt: 5,
                want_secs: 16,
            },
            Case {
                attempt: 6,
                want_secs: 32,
            },
            // Cap kicks in at attempt 7 (1<<6 = 64 → clamped to 60).
            Case {
                attempt: 7,
                want_secs: 60,
            },
            Case {
                attempt: 12,
                want_secs: 60,
            },
            Case {
                attempt: u32::MAX,
                want_secs: 60,
            },
            // Defensive: a 0 attempt shouldn't blow up — saturating_sub keeps
            // exp at 0 and the wait at 1s.
            Case {
                attempt: 0,
                want_secs: 1,
            },
        ];
        for c in cases {
            assert_eq!(
                unhealthy_restart_backoff_secs(c.attempt),
                c.want_secs,
                "attempt {}",
                c.attempt
            );
        }
    }

    /// Drive `run_health_monitor` against a controllable TCP target and
    /// verify it emits the right `ServiceHealthChanged` sequence.
    ///
    /// Strategy: bind a real `TcpListener`, point the monitor at its port
    /// with a tiny interval, then close/rebind to flip health. We assert
    /// only the sequence of `healthy` flags, not their timing — the loop
    /// is naturally jittery and exact timings would make the test flaky.
    #[tokio::test(flavor = "current_thread", start_paused = false)]
    async fn run_health_monitor_emits_unhealthy_then_recovers() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let ready = crate::config::ReadyCheck {
            exec: None,
            tcp: Some(format!("127.0.0.1:{port}")),
            http: None,
            interval: "1s".to_string(),
            retries: 1,
            timeout: "100ms".to_string(),
            monitor: true,
            monitor_interval: "20ms".to_string(),
            unhealthy_after: 2,
        };

        let (cmd_tx, mut cmd_rx) = mpsc::channel(8);
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let monitor = tokio::spawn(run_health_monitor(
            "svc".to_string(),
            ready,
            cmd_tx,
            cancel_rx,
        ));

        // Listener is up — the monitor sees only successes and reports nothing.
        // Drain for ~120ms to confirm silence on the happy path.
        let no_msg =
            tokio::time::timeout(std::time::Duration::from_millis(120), cmd_rx.recv()).await;
        assert!(
            no_msg.is_err(),
            "monitor should not emit while target is healthy"
        );

        // Drop the listener so connect() starts failing. After
        // unhealthy_after=2 consecutive failures, expect healthy=false.
        drop(listener);
        let msg = tokio::time::timeout(std::time::Duration::from_millis(500), cmd_rx.recv())
            .await
            .expect("timeout waiting for unhealthy event")
            .expect("monitor channel closed unexpectedly");
        match msg {
            RunnerInternalCommand::ServiceHealthChanged { name, healthy } => {
                assert_eq!(name, "svc");
                assert!(!healthy, "expected unhealthy event first");
            }
            _ => {
                panic!("unexpected command variant — monitor should only send ServiceHealthChanged")
            }
        }

        // Rebind so probes pass again — expect a recovery event.
        let _restored = TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let msg = tokio::time::timeout(std::time::Duration::from_millis(500), cmd_rx.recv())
            .await
            .expect("timeout waiting for recovery event")
            .expect("monitor channel closed unexpectedly");
        match msg {
            RunnerInternalCommand::ServiceHealthChanged { name, healthy } => {
                assert_eq!(name, "svc");
                assert!(healthy, "expected recovery event after rebind");
            }
            _ => {
                panic!("unexpected command variant — monitor should only send ServiceHealthChanged")
            }
        }

        // Tear the monitor down cleanly so the test exits.
        let _ = cancel_tx.send(());
        let _ = tokio::time::timeout(std::time::Duration::from_millis(200), monitor).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_health_monitor_exits_on_cancel() {
        let ready = crate::config::ReadyCheck {
            exec: None,
            tcp: Some("127.0.0.1:1".to_string()),
            http: None,
            interval: "10s".to_string(),
            retries: 1,
            timeout: "100ms".to_string(),
            monitor: true,
            monitor_interval: "10s".to_string(),
            unhealthy_after: 5,
        };
        let (cmd_tx, _cmd_rx) = mpsc::channel(1);
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let monitor = tokio::spawn(run_health_monitor(
            "svc".to_string(),
            ready,
            cmd_tx,
            cancel_rx,
        ));
        // Long monitor_interval — without cancel, the join would hang.
        // Cancel and confirm the task returns within a short window.
        let _ = cancel_tx.send(());
        let result = tokio::time::timeout(std::time::Duration::from_millis(200), monitor).await;
        assert!(result.is_ok(), "monitor should exit promptly after cancel");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn up_to_date_batch_rebuild_still_emits_rebuild_complete() {
        use crate::config::service::{Service, ServiceKind};
        use crate::config::types::{BazelConfig, LogConfig};

        let temp = tempfile::tempdir().unwrap();
        let config = crate::config::Config {
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
                    reload: true,
                    on_failure: crate::config::OnFailure::Notify,
                    platform: HashMap::new(),
                    hidden: false,
                    kind: Some(ServiceKind::Bazel(BazelConfig {
                        target: "//api:api".to_string(),
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
        };
        let output_manager = crate::output::OutputManager::new_verbose(
            &[("api", &LogConfig::Stdout)],
            tokio::io::sink(),
            false,
        )
        .await
        .unwrap();
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let mut runner = Runner::new(
            config,
            Platform::LinuxX86_64,
            output_manager,
            temp.path().to_path_buf(),
            None,
            shutdown_rx,
        )
        .await
        .unwrap();
        let mut events = runner.subscribe();

        runner
            .handle_rebuild_batch_complete(RebuildBatchOutcome {
                build_succeeded: Vec::new(),
                up_to_date: vec!["api".to_string()],
                failed: Vec::new(),
                plain_rebuilds: Vec::new(),
            })
            .await;

        let event = tokio::time::timeout(std::time::Duration::from_millis(200), events.recv())
            .await
            .expect("timeout waiting for RebuildComplete")
            .expect("runner event channel closed unexpectedly");
        match event {
            RunnerEvent::RebuildComplete {
                name,
                success: true,
            } if name == "api" => {}
            other => panic!("unexpected runner event: {other:?}"),
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
        use crate::config::types::LogConfig;
        use std::collections::HashMap;

        let rs = RuntimeService::new(
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
                reload: true,
                on_failure: crate::config::OnFailure::Notify,
                kind: None,
                resolved_binary_path: None,
            },
            ServiceState::Pending,
        );

        assert_eq!(rs.state(), ServiceState::Pending);
        assert!(rs.handle.is_none());
        assert!(rs.osc_sink.is_none());
        assert!(rs.attach_lock.is_none());
        assert!(rs.attach_waiter.is_none());
        assert!(rs.proxy.is_none());
        assert!(rs.resolved_watch_paths.is_empty());
        assert!(rs.bazel_binary_path.is_none());
        assert!(!rs.batch_built);
        assert!(rs.resolved.kind.is_none());
    }

    #[test]
    fn runtime_task_default_state() {
        use crate::config::types::LogConfig;
        use std::collections::HashMap;

        let rt = RuntimeTask::new(
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
                auto_run: crate::config::TaskAutoRun::Always,
                download: None,
                bazel: None,
                turbo: None,
                params: Vec::new(),
                hidden: false,
            },
            TaskItemState::Pending,
            false,
        );

        assert_eq!(rt.state(), TaskItemState::Pending);
        assert!(rt.pgid.is_none());
        assert!(rt.osc_sink.is_none());
        assert!(rt.attach_lock.is_none());
        assert!(rt.attach_waiter.is_none());
        assert!(rt.resolved_watch_paths.is_empty());
        assert_eq!(rt.config.cmd, "echo");
    }

    #[test]
    fn test_should_rebuild_after_graph_requery() {
        use crate::config::service::ResolvedService;
        use crate::config::types::LogConfig;
        use std::collections::HashMap;

        struct Case {
            name: &'static str,
            state: ServiceState,
            lazy: bool,
            batch_built: bool,
            expected: bool,
        }

        let cases = vec![
            Case {
                name: "ready non-lazy rebuilds",
                state: ServiceState::Ready,
                lazy: false,
                batch_built: true,
                expected: true,
            },
            Case {
                name: "running non-lazy rebuilds",
                state: ServiceState::Running,
                lazy: false,
                batch_built: true,
                expected: true,
            },
            Case {
                name: "untouched lazy service does not cold start",
                state: ServiceState::Lazy,
                lazy: true,
                batch_built: false,
                expected: false,
            },
            Case {
                name: "pending service does not rebuild",
                state: ServiceState::Pending,
                lazy: false,
                batch_built: true,
                expected: false,
            },
        ];

        for case in cases {
            let mut service = RuntimeService::new(
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
                    reload: true,
                    on_failure: crate::config::OnFailure::Notify,
                    kind: None,
                    resolved_binary_path: None,
                },
                case.state,
            );
            service.batch_built = case.batch_built;

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
                name: "falls back to item dir without workspace marker",
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
        fs::write(repo.join("src/app.ts"), "console.log('src');").unwrap();
        fs::write(
            repo.join("generated/schema.ts"),
            "console.log('generated');",
        )
        .unwrap();

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
            &["src/**".to_string()],
            &[],
            SystemTime::now() + std::time::Duration::from_secs(60),
        ));
    }
}
