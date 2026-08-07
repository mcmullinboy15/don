//! Runner — the orchestrator that starts services and tasks in dependency order.
//!
//! The runner builds an execution plan via topological sort, then starts
//! everything whose dependencies are satisfied concurrently using tokio tasks.
//! It owns all service/task state in a plain `HashMap` — no `Arc<Mutex<>>`.
//! Communication uses channels: `mpsc` for commands in, `broadcast` for events out.

mod attach;
mod build_tools;
mod completions;
mod env_refs;
mod events;
mod graph;
mod health;
mod lazy;
mod params;
mod paths;
mod profile;
mod rebuild;
mod runtime_ports;
mod service_commands;
mod service_health;
mod service_ready;
mod service_supervisor;
mod service_worker;
mod setup;
mod shutdown;
mod startup;
mod state;
pub(crate) mod state_store;
mod status;
mod supervisor;
mod support;
mod task_commands;
mod task_supervisor;
mod task_worker;
mod watch_link;

pub(crate) mod service;
pub(crate) mod task;

pub use crate::terminal::{TerminalCoordinator, TerminalRequest};
pub(crate) use params::resolve_task_params;
pub use profile::resolve_profile_items;
pub use state_store::{StateReader, StateSnapshot};

use crate::build_tool::manager::{BatchDue, BuildBatcher};
use crate::config::{Config, Platform, ShutdownConfig};
use crate::output::OutputManager;
use crate::process::pid_file::PidFile;
use crate::proxy::ConnectionPolicy;
use crate::watch::WatchManager;
use std::collections::HashMap;
use std::path::PathBuf;
#[cfg(test)]
use std::time::SystemTime;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};

#[cfg(test)]
use self::build_tools::bazel_graph_requery_group_dir;
use self::build_tools::{BatchBuildOutcome, GraphRequeryOutcomeItem, RebuildBatchOutcome};
use self::events::{ItemDone, TaskExit};
#[cfg(test)]
use self::graph::compute_depths;
use self::graph::topological_sort;
#[cfg(test)]
use self::health::run_health_monitor;
#[cfg(test)]
use self::health::unhealthy_restart_backoff_secs;
#[cfg(test)]
use self::paths::any_glob_path_changed_since;
use self::service_worker::ServiceStartContext;
use self::support::check_gitignore;
use self::task_worker::TaskRunPrepared;
use crate::signals::shutdown_requested;

const UPDATE_CHECK_INTERVAL: Duration = Duration::from_secs(60 * 60);
const UPDATE_CHECK_TIMEOUT: Duration = Duration::from_secs(2);

enum ServiceStartIntent {
    Scheduled {
        done_tx: mpsc::Sender<ItemDone>,
    },
    Reply {
        reply: oneshot::Sender<CommandResult>,
    },
    Background,
}

enum TaskRunIntent {
    Scheduled { done_tx: mpsc::Sender<ItemDone> },
    Background,
}

pub(crate) struct TaskRunWaiter {
    generation: u64,
    reply: Option<oneshot::Sender<CommandResult>>,
    timeout_task: Option<tokio::task::JoinHandle<()>>,
}

impl TaskRunWaiter {
    pub(crate) fn new(
        generation: u64,
        reply: oneshot::Sender<CommandResult>,
        timeout_task: Option<tokio::task::JoinHandle<()>>,
    ) -> Self {
        Self {
            generation,
            reply: Some(reply),
            timeout_task,
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn complete(mut self, result: CommandResult) {
        if let Some(timeout_task) = self.timeout_task.take() {
            timeout_task.abort();
        }
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(result);
        }
    }
}

impl Drop for TaskRunWaiter {
    fn drop(&mut self) {
        if let Some(timeout_task) = self.timeout_task.take() {
            timeout_task.abort();
        }
    }
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
    /// A batch or lazy JIT build (bazel) is in flight. Transitions to
    /// Pending on success (then the service starts like any other) or Failed
    /// on build error. File-watch rebuilds keep the service in Running/Ready.
    Building,
    /// Proxy is bound and accepting connections, but the service process is not
    /// requested yet. Transitions to Pending on the first incoming connection.
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
                | (Self::Lazy, Self::Pending)
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
    /// A synchronous `don run --wait --timeout` request stopped waiting.
    TimedOut { name: String, timeout: String },
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
            Self::TimedOut { name, timeout } => {
                write!(f, "{name}: did not finish within {timeout}")
            }
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
    /// that item is returned, with its full resolved watch path list included.
    Status {
        verbose: bool,
        name: Option<String>,
        reply: oneshot::Sender<Vec<ItemStatus>>,
    },
    /// Query the global file-watch state — registered inotify directories and
    /// per-item patterns. Replies `None` when no watches are active.
    WatchStatus {
        reply: oneshot::Sender<Option<WatchReport>>,
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
    /// tasks that don't declare any. When `wait` is true, the reply is held
    /// until the task process exits.
    RunTask {
        name: String,
        params: HashMap<String, String>,
        wait: bool,
        wait_timeout: Option<String>,
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
/// What an item tells the scheduler, on the lossless report channel.
///
/// This is the up-direction of the supervisor architecture: items report,
/// the runner folds. It is `mpsc`, not `broadcast`, because the scheduler
/// must never miss one — lossy observation is for peers and edges, which
/// resync from the snapshot. Starts with lazy demand; per-item lifecycle
/// reports (exit, transitions) migrate here as supervisors absorb them.
#[derive(Debug)]
pub(in crate::runner) enum ItemReport {
    /// A lazy service's proxy saw its first connection. Demand originates
    /// inside the item, but the *reaction* belongs to the scheduler: a lazy
    /// service has dependencies, and starting it is a scheduling decision
    /// like any other.
    Demand { name: String },
    /// A service's process died and its supervisor reaped it. `status` is
    /// the reaped exit status (`None` when the wait itself failed).
    ServiceExited {
        name: String,
        pgid: i32,
        status: Option<std::process::ExitStatus>,
    },
    /// A service's restart backoff elapsed; attempt `attempt` may begin.
    RestartDue { name: String, attempt: u32 },
    /// The health monitor observed a transition. State-guarded on fold;
    /// the monitor itself dies with custody (its cancel lives in the
    /// supervisor), so it cannot outlive its process by more than a probe.
    HealthChanged { name: String, healthy: bool },
}

enum RunnerInternalCommand {
    /// Completion from a detached task run worker.
    TaskRunPrepared {
        name: String,
        task_cfg: Box<crate::config::Task>,
        intent: TaskRunIntent,
        result: Result<TaskRunPrepared, String>,
    },
    /// A task process exited after an explicit run/restart.
    TaskExited(TaskExit),
    /// A manually-triggered task wait exceeded its requested wait deadline.
    TaskRunWaitTimedOut {
        name: String,
        generation: u64,
        timeout: String,
    },
    /// Result of the startup-phase batch build.
    BatchBuildComplete(BatchBuildOutcome),
    /// Result of a detached file-watch build-tool rebuild batch.
    RebuildBatchComplete(RebuildBatchOutcome),
    /// Result of a just-in-time build for a single lazy service.
    LazyBuildComplete {
        name: String,
        generation: u64,
        outcome: BatchBuildOutcome,
    },
    /// Ready-check completed for a manual-start or rebuild spawn.
    ReadyCheckComplete {
        name: String,
        generation: u64,
        success: bool,
        message: Option<String>,
    },
    /// Completion from a detached manual service stop/restart worker.
    ServiceStopComplete {
        name: String,
        op_id: u64,
        result: Result<(), String>,
    },
    /// Completion from a detached service start worker.
    ServiceStartPrepared {
        name: String,
        context: Box<ServiceStartContext>,
        intent: ServiceStartIntent,
        result: Result<Box<service_supervisor::ServiceWired>, String>,
    },
    /// Completion from a detached rebuild worker for a single service.
    ServiceRebuildPrepared {
        name: String,
        op_id: u64,
        result: Result<(), String>,
    },
    /// Completion from a detached build-graph re-query worker.
    GraphRequeryComplete(Vec<GraphRequeryOutcomeItem>),
    /// Result of the periodic crates.io update check.
    UpdateCheckComplete(Option<crate::update::UpdateAvailable>),
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
        /// Root service/task failures blocking this item.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        failed_dependencies: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        verbose: Option<VerboseInfo>,
    },
    Task {
        name: String,
        state: TaskItemState,
        /// Root service/task failures blocking this item.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        failed_dependencies: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_run: Option<crate::task_state::TaskRunInfo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        verbose: Option<VerboseInfo>,
    },
}

/// One task param, as a client needs to see it to build a run form.
///
/// Deliberately not [`crate::config::TaskParam`] itself: that carries the
/// `completions` shell command, which is an implementation detail of the
/// daemon and has no business crossing to a browser. Clients learn only
/// *that* a param has dynamic completions, and ask for the values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ParamInfo {
    /// Param identifier, as passed to `POST /run/:task`.
    pub name: String,
    /// Human-readable label; falls back to `name` when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    /// Whether the task refuses to run without a value.
    pub required: bool,
    /// Value used when the client doesn't supply one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// One of `string`, `int`, `bool`, `choice`.
    pub kind: String,
    /// Fixed candidate values, when the config lists them inline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub choices: Vec<String>,
    /// Whether candidates come from a completion command — fetch them from
    /// the completions endpoint rather than expecting `choices`.
    #[serde(default)]
    pub has_completions: bool,
    /// Lower bound for `int` params.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,
    /// Upper bound for `int` params.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
}

impl ParamInfo {
    /// Project a configured param into its client-visible shape.
    pub(crate) fn from_config(param: &crate::config::TaskParam) -> Self {
        use crate::config::ParamKind;
        Self {
            name: param.name.clone(),
            prompt: param.prompt.clone(),
            required: param.required,
            default: param.default.clone(),
            kind: match param.kind {
                ParamKind::String => "string",
                ParamKind::Int => "int",
                ParamKind::Bool => "bool",
                ParamKind::Choice => "choice",
            }
            .to_string(),
            choices: param.choices.clone(),
            has_completions: param.completions.is_some(),
            min: param.validate.as_ref().and_then(|v| v.min),
            max: param.validate.as_ref().and_then(|v| v.max),
        }
    }
}

/// Extended information for verbose status display.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VerboseInfo {
    /// Services/tasks this item depends on. Non-blocking (ordering-only)
    /// edges serialize as `{ name, blocking = false }`, blocking ones as a
    /// string.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<crate::config::Dependency>,
    /// File watch patterns (explicit or resolved from build tool).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub watch: Vec<String>,
    /// Number of file watch patterns resolved for this item.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub watch_count: usize,
    /// Proxy entries, each formatted as `"addr (env=NAME)"` or
    /// `"addr (listenfd)"` for display.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proxy: Vec<String>,
    /// Active Docker mappings, formatted with actual host addresses.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub docker_ports: Vec<String>,
    /// Active Don-managed proxy connections. Present only for env/forward
    /// proxy entries; listenfd connections are owned by the child process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_active_connections: Option<usize>,
    /// Bazel target (if configured).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bazel_target: Option<String>,
    /// Params a task declares, so a client can render a run form without
    /// reading `don.toml` itself. Empty for services.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<ParamInfo>,
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

fn is_zero(value: &usize) -> bool {
    *value == 0
}

/// A global snapshot of everything the file watcher is monitoring right now,
/// independent of any single service. Returned by `GET /watch` / `don watch`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WatchReport {
    /// The actual inotify registrations — the ground truth of what don is
    /// watching at the OS level. Sorted by path.
    pub directories: Vec<WatchDir>,
    /// Per-item (service/task/build-graph) watch state and patterns, sorted by
    /// name.
    pub items: Vec<WatchReportItem>,
    /// Workspace-wide `watch_ignore` globs that apply to every item.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub global_ignore: Vec<String>,
    /// Count of notify backend errors observed since startup.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub notify_error_count: u64,
    /// Count of runner-event broadcast-lag incidents (a non-zero value means an
    /// item may be stuck mid-rebuild).
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub runner_event_lag_count: u64,
    /// Most recent notify backend error, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_notify_error: Option<String>,
}

/// One inotify registration: a directory and the mode it was registered under.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WatchDir {
    pub path: String,
    /// `"recursive"` or `"non-recursive"`.
    pub mode: String,
}

/// Per-item entry in a [`WatchReport`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WatchReportItem {
    pub name: String,
    /// `"service"`, `"task"`, or `"build_graph"`.
    pub kind: String,
    /// Watch state machine: `"idle"`, `"debouncing"`, or `"rebuilding"`.
    pub state: String,
    pub stale: bool,
    pub debounce_ms: u64,
    /// Absolute glob patterns that trigger a rebuild/rerun for this item.
    pub patterns: Vec<String>,
    /// Item-specific ignore globs (workspace-wide ignores live on the report).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ignore_patterns: Vec<String>,
    /// Last watch-registration error for this item, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Whether every service in `items` has reached an available state — `Ready`
/// (passed its ready check) or `Lazy` (proxy bound, will spawn on first
/// connection). Tasks are ignored: they are one-shot and do not gate stack
/// readiness. An item set with no services is considered ready.
///
/// This is the bool surfaced by `don status --json`, so scripts and agents can
/// poll "is the whole stack up?" without parsing the human-readable table.
/// Status only ever reports the active set (the started profile's subset), so
/// services excluded by a profile do not drag this to `false`.
pub fn all_services_ready(items: &[ItemStatus]) -> bool {
    items.iter().all(|item| match item {
        ItemStatus::Service { state, .. } => {
            matches!(state, ServiceState::Ready | ServiceState::Lazy)
        }
        ItemStatus::Task { .. } => true,
    })
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
        state: TaskItemState,
        last_run: Option<crate::task_state::TaskRunInfo>,
        /// Root service/task failures when `state` is `DependencyFailed`.
        failed_dependencies: Vec<String>,
    },
    /// A rebuild cycle completed (file watch triggered).
    RebuildComplete { name: String, success: bool },
    /// A task re-run completed (file watch triggered).
    TaskRerunComplete { name: String, success: bool },
    /// The initial startup sweep has decided every item — nothing is left
    /// merely being *considered*. Fires once per run.
    StartupSettled,
    /// Graceful shutdown has started.
    ShutdownStarted,
    /// Shutdown complete.
    ShutdownComplete,
    /// The latest crates.io version changed, or no newer version is available.
    UpdateCheckComplete {
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

    /// Sender half handed to ServiceProxy::bind for lazy services. The
    /// proxy speaks plain names, not runner types; a forwarder in `run`
    /// adapts its channel onto the report channel. Moves inside the service
    /// supervisor when it takes proxy ownership.
    lazy_start_tx: mpsc::Sender<String>,
    /// Receiver half, consumed by the forwarder spawned in `run`.
    lazy_start_rx: Option<mpsc::Receiver<String>>,
    /// The items' lossless report channel — see [`ItemReport`].
    report_tx: mpsc::UnboundedSender<ItemReport>,
    report_rx: mpsc::UnboundedReceiver<ItemReport>,

    /// Signals the API server task to stop accepting connections.
    server_shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,

    /// Docker API client. `Some` if any service uses the docker preset.
    docker_client: Option<bollard::Docker>,

    // Channels
    cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
    cmd_rx: mpsc::UnboundedReceiver<RunnerCommand>,
    internal_tx: mpsc::Sender<RunnerInternalCommand>,
    internal_rx: mpsc::Receiver<RunnerInternalCommand>,
    event_tx: broadcast::Sender<RunnerEvent>,

    /// The write half of the globally-readable state projection.
    ///
    /// Republished on every state transition, so other components can read
    /// item state — and whether the initial startup sweep has settled —
    /// without a command round trip. Not `Clone`: the runner is the only
    /// writer, and [`state_store`] enforces that by ownership rather than by
    /// convention.
    state: state_store::StateWriter,

    /// Item-completion sender shared by dependency-scheduled starts and config
    /// reload paths. Ready-check and task-completion callbacks send here.
    /// The main loop's `done_rx` receives these.
    done_tx: Option<mpsc::Sender<ItemDone>>,

    // Shutdown signal receiver — wakes the select loop when Ctrl+C is pressed.
    // `Option` because `run()` takes it out at the top to consume in the
    // main `select!`. It's never `None` after construction until `run()`
    // consumes it.
    shutdown_rx: Option<mpsc::Receiver<()>>,

    /// Detached batch-build task spawned at startup for services/tasks with
    /// a bazel config. `Some` until [`RunnerInternalCommand::BatchBuildComplete`]
    /// arrives and the handle is consumed. Wrapped in [`AbortOnDrop`] so
    /// shutting the runner down — or dropping the field before completion —
    /// aborts the task, dropping the in-flight `Child` (with `kill_on_drop`)
    /// and sending SIGKILL to the bazel client.
    batch_build_handle: Option<crate::build_tool::AbortOnDrop<()>>,

    /// Detached JIT build tasks spawned when a lazy service's proxy gets
    /// its first connection. Keyed by service name. Entries are inserted
    /// on spawn and removed when [`RunnerInternalCommand::LazyBuildComplete`]
    /// arrives. Wrapped in [`AbortOnDrop`] for the same reason as
    /// [`Self::batch_build_handle`]: on shutdown we abort any in-flight
    /// JIT builds so bazel output stops streaming before
    /// "shutdown complete" is emitted.
    lazy_build_handles: HashMap<String, (u64, crate::build_tool::AbortOnDrop<()>)>,

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

    /// Coalescing for build-tool work: the rebuild and graph-re-query queues,
    /// their batch windows, the in-flight batches, and the mutex that
    /// serialises Bazel.
    ///
    /// The runner still decides *what* a rebuild means — which items are
    /// eligible and what happens when one finishes, both of which need item
    /// state. This owns *when* that work runs.
    builds: BuildBatcher,

    /// Per-param completion results cache. Populated as the TUI / CLI
    /// resolves completions.
    completion_cache: std::sync::Arc<tokio::sync::RwLock<completions::CompletionCache>>,

    /// Internal shutdown flag broadcast to detached control workers so they
    /// can force-kill promptly when don is exiting.
    shutdown_flag_tx: tokio::sync::watch::Sender<bool>,

    /// True after graceful shutdown starts. Used to reject late starts and
    /// to keep final shutdown output ordered after all cleanup work.
    shutting_down: bool,

    /// Coordinates terminal handoff with the TUI for foreground tasks.
    /// Detached in non-TUI runs.
    pub(crate) terminal_coordinator: TerminalCoordinator,

    /// Sends manifest snapshots / removals to the serialized writer task that
    /// owns `.don/ports.json` filesystem I/O. `None` after shutdown flush.
    manifest_writer_tx: Option<mpsc::UnboundedSender<runtime_ports::ManifestWrite>>,
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
        terminal_coordinator: TerminalCoordinator,
    ) -> Result<Self, RunnerError> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (internal_tx, internal_rx) = mpsc::channel(64);
        let (event_tx, _) = broadcast::channel(256);
        let (state, _state_reader) = state_store::channel(state_store::StateSnapshot::default());
        let (lazy_start_tx, lazy_start_rx) = mpsc::channel(16);
        let (report_tx, report_rx) = mpsc::unbounded_channel();
        let (shutdown_flag_tx, _shutdown_flag_rx) = tokio::sync::watch::channel(false);

        for outcome in crate::process::rlimit::raise_soft_resource_limits() {
            if let Some(message) = crate::process::rlimit::format_outcome(&outcome) {
                output_manager.service_debug_event("don", &message);
            }
        }

        let base_dir = setup::canonicalize_base_dir(&base_dir)?;
        let don_dir = setup::ensure_don_dir(&base_dir)?;
        let don_pid_file = setup::acquire_don_pid_file(&don_dir).await?;

        setup::cleanup_stale_state(&config, platform, &base_dir, &output_manager).await;
        if let Err(error) = crate::ports::remove_manifest(&base_dir) {
            output_manager.error_event(&format!("failed to remove stale runtime ports: {error}"));
        }
        let docker_client = setup::connect_docker_if_needed(&config, platform)?;

        let active_items = setup::resolve_active_items(&config, platform, profile)?;
        let active_services = setup::filter_active_services(&config, active_items.as_ref());
        let active_tasks = setup::filter_active_tasks(&config, active_items.as_ref());
        let headless = terminal_coordinator.is_detached();

        setup::prune_download_cache(&config, platform, &don_dir, &output_manager);

        let (services, tasks) = setup::build_runtime_maps(
            &config,
            platform,
            &base_dir,
            &active_services,
            &active_tasks,
            headless,
        )
        .await;

        // One supervisor per service, likewise immutable once built.
        let service_starts = service_supervisor::spawn_supervisors(
            services.keys(),
            &service_supervisor::StartEnv {
                base_dir: base_dir.clone(),
                pid_dir: base_dir.join(".don").join("pids"),
                platform,
                docker_client: docker_client.clone(),
                emitter: output_manager.clone_lifecycle_emitter(),
                shutdown: config.shutdown.clone(),
            },
            &|name| output_manager.item_output(name),
            &internal_tx,
            &report_tx,
        );

        // One supervisor per task, started before the runner exists so the
        // registry is immutable and can be shared without a lock.
        let task_supervisors = task_supervisor::spawn_supervisors(
            tasks.keys(),
            &task_worker::TaskWorkerContext {
                base_dir: base_dir.clone(),
                platform,
                emitter: output_manager.clone_lifecycle_emitter(),
                global_watch_ignore: config.watch_ignore.clone(),
                terminal_coordinator: terminal_coordinator.clone(),
            },
            &internal_tx,
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
            lazy_start_tx,
            lazy_start_rx: Some(lazy_start_rx),
            report_tx,
            report_rx,
            server_shutdown_tx: None,
            docker_client,
            cmd_tx,
            cmd_rx,
            internal_tx,
            internal_rx,
            event_tx,
            state,
            done_tx: None,
            shutdown_rx: Some(shutdown_rx),
            _don_pid_file: Some(don_pid_file),
            watch: None,
            batch_build_handle: None,
            lazy_build_handles: HashMap::new(),
            update_check_handle: None,
            service_starts,
            task_supervisors,
            builds: BuildBatcher::new(),
            completion_cache: std::sync::Arc::new(tokio::sync::RwLock::new(
                completions::CompletionCache::default(),
            )),
            shutdown_flag_tx,
            shutting_down: false,
            terminal_coordinator,
            manifest_writer_tx: Some(manifest_writer_tx),
            manifest_writer_handle: Some(manifest_writer_handle),
        };
        // Seed the projection before anyone can read it. The item set is fixed
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
        self.state.publish_items(self.status_projection(None));
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
            if self.done_tx.is_some()
                && (matches!(
                    state,
                    ServiceState::Pending
                        | ServiceState::Lazy
                        | ServiceState::Ready
                        // Stopped opens a non-blocking dependency's gate, so
                        // dependents need a sweep to notice.
                        | ServiceState::Stopped
                        | ServiceState::Failed
                        | ServiceState::DependencyFailed
                ) || matches!(
                    previous_state,
                    Some(ServiceState::Failed | ServiceState::DependencyFailed)
                ))
            {
                self.schedule_start_pending();
            }
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
        let Some(rs) = self.services.get(name) else {
            return;
        };
        let policy = match rs.state() {
            ServiceState::Lazy => ConnectionPolicy::LazyTrigger,
            ServiceState::Failed | ServiceState::DependencyFailed
                if rs.handle_identity.is_none() =>
            {
                ConnectionPolicy::Refuse
            }
            _ => ConnectionPolicy::Serve,
        };
        let Some(rs) = self.services.get_mut(name) else {
            return;
        };
        let Some(view) = rs.proxy_view.as_mut() else {
            return;
        };
        // The runner is the only sender of policy changes, so its shadow is
        // the authority on whether this is a change at all.
        if view.policy == policy {
            return;
        }
        let was_refusing = view.is_refusing();
        view.policy = policy;
        if let Some(proxy) = rs.proxy.as_mut() {
            proxy.set_policy(policy);
        }
        // Only the refusal edge is worth a line, and it belongs in the normal
        // log: a dev staring at `ECONNRESET` in their browser shouldn't have
        // to rerun with `--verbose` to find out why.
        let refusing = policy == ConnectionPolicy::Refuse;
        if refusing == was_refusing {
            return;
        }
        if refusing {
            self.output_manager
                .service_error_event(name, "proxy refusing connections (service failed)");
        } else {
            self.output_manager
                .service_event(name, "proxy accepting connections again");
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
        if state_changed && self.done_tx.is_some() {
            self.schedule_start_pending();
        }
        state_changed
    }

    fn broadcast_service_state(&self, name: &str, state: ServiceState) {
        let Some(rs) = self.services.get(name) else {
            return;
        };
        self.publish_state();
        let _ = self.event_tx.send(RunnerEvent::ServiceStateChanged {
            name: name.to_string(),
            state,
            pid: rs.pgid,
            failed_dependencies: rs.failed_dependencies().to_vec(),
        });
    }

    /// Transition a task to a new state and broadcast the change.
    pub(crate) fn set_task_state(&mut self, name: &str, new_state: TaskItemState) {
        let previous_state = self.tasks.get(name).map(RuntimeTask::state);
        let changed = self
            .tasks
            .get_mut(name)
            .and_then(|rt| rt.set_state(new_state));
        if let Some(state) = changed {
            self.broadcast_task_state(name, state);
            if self.done_tx.is_some()
                && (matches!(
                    state,
                    TaskItemState::Pending
                        | TaskItemState::PendingRun
                        | TaskItemState::Completed
                        | TaskItemState::Skipped
                        | TaskItemState::Failed
                        | TaskItemState::DependencyFailed
                ) || matches!(
                    previous_state,
                    Some(TaskItemState::Failed | TaskItemState::DependencyFailed)
                ))
            {
                self.schedule_start_pending();
            }
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
        let state_changed = rt.state() != TaskItemState::DependencyFailed;
        if !rt.mark_dependency_failed(dependencies) {
            return false;
        }
        self.broadcast_task_state(name, TaskItemState::DependencyFailed);
        if state_changed && self.done_tx.is_some() {
            self.schedule_start_pending();
        }
        state_changed
    }

    fn broadcast_task_state(&self, name: &str, state: TaskItemState) {
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

    /// A read-only view of every item's state, updated on each transition.
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

        let internal_tx = self.internal_tx.clone();
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
                            && internal_tx
                                .send(RunnerInternalCommand::UpdateCheckComplete(update))
                                .await
                                .is_err()
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
        let _ = self.event_tx.send(RunnerEvent::UpdateCheckComplete {
            current_version,
            latest_version,
        });
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
                self.config.fallback_ports,
                lazy_tx,
                name,
                self.output_manager.clone_lifecycle_emitter(),
            )
            .await
            {
                Ok(proxy) => {
                    for message in proxy.fallback_descriptions() {
                        self.output_manager.service_event(name, &message);
                    }
                    let addrs: Vec<String> =
                        proxy.listen_addrs().iter().map(|a| a.to_string()).collect();
                    self.output_manager.service_debug_event(
                        name,
                        &format!("proxy listening on {}", addrs.join(", ")),
                    );
                    if let Some(rs) = self.services.get_mut(name) {
                        rs.proxy_view = Some(proxy.view());
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
        // Adapt the proxies' plain-name lazy channel onto the report
        // channel. Ends when every proxy's sender is gone (shutdown) or the
        // runner drops its report receiver. Lives here, not on a proxy or
        // supervisor, only until supervisors own their proxies.
        if let Some(mut lazy_rx) = self.lazy_start_rx.take() {
            let report_tx = self.report_tx.clone();
            tokio::spawn(async move {
                while let Some(name) = lazy_rx.recv().await {
                    if report_tx.send(ItemReport::Demand { name }).is_err() {
                        return;
                    }
                }
            });
        }

        let (watch_signal_tx, watch_signal_rx) = mpsc::unbounded_channel();
        let (watch_outcome_tx, watch_outcome_rx) = mpsc::unbounded_channel();
        let watch_link_handle = watch_link::spawn(
            watch_signal_rx,
            self.cmd_tx.clone(),
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

        // Kick off batch builds (bazel) as a detached task. The runner
        // keeps processing the main command loop — shutdown signals,
        // connection-triggered lazy starts, and non-build-tool services all
        // stay responsive while bazel crunches. On completion the task posts
        // `RunnerInternalCommand::BatchBuildComplete`, which transitions `Building`
        // items to `Pending`/`Failed` and triggers the ready-item sweep.
        //
        // The handle is stored as `AbortOnDrop` on `self` so `Shutdown` drops
        // the in-flight `Child`, whose `kill_on_drop(true)` sends SIGKILL to
        // the bazel client.
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

        // Validate the active dependency graph before starting anything.
        let dep_map = self.build_dep_name_map();
        topological_sort(&dep_map).map_err(|cycle| RunnerError::Cycle { cycle })?;

        // Channel for dependency-scheduled completion notifications. Store the
        // sender on `self` so services requested later use the same path.
        let (done_tx, mut done_rx) = mpsc::channel::<ItemDone>(64);
        self.done_tx = Some(done_tx);

        // Initial non-lazy items already occupy Pending. A lazy connection
        // performs the same state transition and can join this scheduler at
        // any point, including while this first sweep is running.
        self.start_pending_items().await;
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
                        ) || rs.pending_restart.is_some()
                            || starts.is_busy(name)
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
                        self.handle_item_done(&item_done);
                    }
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
                            RunnerCommand::WatchStatus { reply } => {
                                let report = self.collect_watch_report().await;
                                let _ = reply.send(report);
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
                            RunnerCommand::HardRestart { name, reply } => {
                                self.handle_hard_restart_service_cmd(&name, reply).await;
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
                                task_cfg,
                                intent,
                                result,
                            } => {
                                self.handle_task_run_prepared(&name, &task_cfg, intent, result)
                                    .await;
                            }
                            RunnerInternalCommand::ServiceStopComplete { name, op_id, result } => {
                                self.handle_service_stop_complete(&name, op_id, result).await;
                            }
                            RunnerInternalCommand::ServiceStartPrepared {
                                name,
                                context,
                                intent,
                                result,
                            } => {
                                self.handle_service_start_prepared(&name, context, intent, result)
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
                            RunnerInternalCommand::TaskExited(exit) => {
                                self.handle_task_exit(exit);
                            }
                            RunnerInternalCommand::TaskRunWaitTimedOut {
                                name,
                                generation,
                                timeout,
                            } => {
                                self.handle_task_run_wait_timeout(&name, generation, &timeout);
                            }                            RunnerInternalCommand::ReadyCheckComplete {
                                name,
                                generation,
                                success,
                                message,
                            } => {
                                self.handle_ready_check_complete(
                                    &name,
                                    generation,
                                    success,
                                    message,
                                );
                            }
                            RunnerInternalCommand::BatchBuildComplete(outcome) => {
                                // Drop the abort-on-drop handle: the task is done,
                                // and leaving the handle live would abort after the
                                // task has already returned (harmless but noisy).
                                self.batch_build_handle = None;
                                let replay_items = outcome.replay_items.clone();
                                self.apply_batch_build_outcome(outcome);
                                self.schedule_startup_batch_replays(&replay_items);
                            }
                            RunnerInternalCommand::RebuildBatchComplete(outcome) => {
                                // Release the batch first: handling the outcome
                                // can queue follow-up rebuilds, and those must
                                // see a free slot rather than being deferred
                                // behind a batch that has already finished.
                                self.builds.finish_rebuild_batch();
                                self.handle_rebuild_batch_complete(outcome).await;
                            }
                            RunnerInternalCommand::LazyBuildComplete {
                                name,
                                generation,
                                outcome,
                            } => {
                                self.handle_lazy_build_complete(&name, generation, outcome);
                            }
                            RunnerInternalCommand::GraphRequeryComplete(outcomes) => {
                                self.builds.finish_requery_batch();
                                self.handle_graph_requery_complete(outcomes).await;
                            }
                            RunnerInternalCommand::UpdateCheckComplete(update) => {
                                self.broadcast_update_check(update);
                            }
                        }
                    }
                    Some(report) = self.report_rx.recv() => {
                        match report {
                            // Only the first connection acts: it moves Lazy →
                            // Pending, and the normal dependency scheduler
                            // owns the service from there.
                            ItemReport::Demand { name } => self.handle_lazy_connection(&name),
                            ItemReport::ServiceExited { name, pgid, status } => {
                                self.handle_service_exited(&name, pgid, status).await;
                            }
                            ItemReport::RestartDue { name, attempt } => {
                                self.handle_auto_restart(&name, attempt).await;
                            }
                            ItemReport::HealthChanged { name, healthy } => {
                                self.handle_service_health_changed(&name, healthy).await;
                            }
                        }
                    }
                    // Flush a build-tool batch when its window expires. Never
                    // resolves while both queues are empty.
                    due = self.builds.next_due() => {
                        match due {
                            BatchDue::Rebuilds => self.flush_pending_rebuilds().await,
                            BatchDue::Requeries => self.flush_pending_graph_requery().await,
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
        fn svc(state: ServiceState) -> ItemStatus {
            ItemStatus::Service {
                name: "s".to_string(),
                state,
                failed_dependencies: Vec::new(),
                verbose: None,
            }
        }
        fn task(state: TaskItemState) -> ItemStatus {
            ItemStatus::Task {
                name: "t".to_string(),
                state,
                failed_dependencies: Vec::new(),
                last_run: None,
                verbose: None,
            }
        }

        struct Case {
            name: &'static str,
            items: Vec<ItemStatus>,
            want: bool,
        }
        let cases = vec![
            Case {
                name: "empty is ready",
                items: vec![],
                want: true,
            },
            Case {
                name: "all ready",
                items: vec![svc(ServiceState::Ready), svc(ServiceState::Ready)],
                want: true,
            },
            Case {
                name: "lazy counts as available",
                items: vec![svc(ServiceState::Lazy), svc(ServiceState::Ready)],
                want: true,
            },
            Case {
                name: "running is not yet ready",
                items: vec![svc(ServiceState::Ready), svc(ServiceState::Running)],
                want: false,
            },
            Case {
                name: "failed is not ready",
                items: vec![svc(ServiceState::Failed)],
                want: false,
            },
            Case {
                name: "stopped is not ready",
                items: vec![svc(ServiceState::Stopped)],
                want: false,
            },
            Case {
                name: "tasks do not gate readiness",
                items: vec![svc(ServiceState::Ready), task(TaskItemState::Failed)],
                want: true,
            },
            Case {
                name: "task-only set is ready",
                items: vec![task(TaskItemState::Completed)],
                want: true,
            },
        ];
        for c in cases {
            assert_eq!(all_services_ready(&c.items), c.want, "case: {}", c.name);
        }
    }

    #[test]
    fn item_status_deserializes_without_dependency_failure_detail() {
        let cases = vec![
            r#"{"kind":"service","name":"api","state":"dependencyfailed","verbose":null}"#,
            r#"{"kind":"task","name":"setup","state":"dependency_failed","last_run":null,"verbose":null}"#,
        ];

        for json in cases {
            let status: ItemStatus = serde_json::from_str(json).unwrap();
            let failed_dependencies = match status {
                ItemStatus::Service {
                    failed_dependencies,
                    ..
                }
                | ItemStatus::Task {
                    failed_dependencies,
                    ..
                } => failed_dependencies,
            };
            assert!(failed_dependencies.is_empty(), "json: {json}");
        }
    }

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

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
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
            ItemReport::HealthChanged { name, healthy } => {
                assert_eq!(name, "svc");
                assert!(!healthy, "expected unhealthy event first");
            }
            _ => {
                panic!("unexpected report variant — monitor should only send HealthChanged")
            }
        }

        // Rebind so probes pass again — expect a recovery event.
        //
        // The port had to be genuinely released to make the monitor fail, and
        // the kernel can hand it to any other process in that window, so this
        // bind can lose a race the test can't prevent. Retry briefly: a real
        // regression keeps the port free and binds on the first attempt, while
        // a thief that never leaves fails the test rather than hiding.
        let restored = {
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(300);
            loop {
                match TcpListener::bind(format!("127.0.0.1:{port}")).await {
                    Ok(listener) => break Ok(listener),
                    Err(e) if std::time::Instant::now() < deadline => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        let _ = e;
                    }
                    Err(e) => break Err(e),
                }
            }
        };
        let _restored = restored.expect("another process took the monitored port mid-test");
        let msg = tokio::time::timeout(std::time::Duration::from_millis(500), cmd_rx.recv())
            .await
            .expect("timeout waiting for recovery event")
            .expect("monitor channel closed unexpectedly");
        match msg {
            ItemReport::HealthChanged { name, healthy } => {
                assert_eq!(name, "svc");
                assert!(healthy, "expected recovery event after rebind");
            }
            _ => {
                panic!("unexpected report variant — monitor should only send HealthChanged")
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
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
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
        use crate::config::types::{BazelConfig, LogConfig, LogFilterConfig};

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
        let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let mut runner = Runner::new(
            config,
            Platform::LinuxX86_64,
            output_manager,
            temp.path().to_path_buf(),
            None,
            shutdown_rx,
            TerminalCoordinator::detached(),
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

    /// Build a runner with a single watch-enabled bazel service "api", for
    /// exercising the rebuild-batch completion paths directly. Returns the
    /// shutdown sender too so the runner's `shutdown_rx` stays open.
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
            TerminalCoordinator::detached(),
        )
        .await
        .unwrap();
        (runner, shutdown_tx)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lazy_jit_completion_rechecks_dependencies_before_start() {
        let temp = tempfile::tempdir().unwrap();
        let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;
        if let Some(service) = runner.services.get_mut("api") {
            service.resolved.lazy = true;
            service.resolved.depends_on = vec![crate::config::Dependency::blocking("setup")];
        }
        runner.set_service_state("api", ServiceState::Building);

        runner.handle_lazy_build_complete(
            "api",
            0,
            build_tools::BatchBuildOutcome {
                resolved_watches: Vec::new(),
                warnings: Vec::new(),
                succeeded: ["api".to_string()].into_iter().collect(),
                failed: Vec::new(),
                binary_paths: HashMap::new(),
                replay_items: Vec::new(),
            },
        );
        runner.start_pending_items().await;

        let service = runner.services.get("api").unwrap();
        assert_eq!(service.state(), ServiceState::Pending);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_lazy_jit_completion_does_not_overwrite_newer_service_operation() {
        let temp = tempfile::tempdir().unwrap();
        let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;
        if let Some(service) = runner.services.get_mut("api") {
            service.resolved.lazy = true;
            service.start_generation = 2;
        }
        runner.set_service_state("api", ServiceState::Building);

        runner.handle_lazy_build_complete(
            "api",
            1,
            build_tools::BatchBuildOutcome {
                resolved_watches: Vec::new(),
                warnings: Vec::new(),
                succeeded: std::collections::HashSet::new(),
                failed: vec![("api".to_string(), "stale build failure".to_string())],
                binary_paths: HashMap::new(),
                replay_items: Vec::new(),
            },
        );

        let service = runner.services.get("api").unwrap();
        assert_eq!(service.state(), ServiceState::Building);
        assert!(!service.batch_built);
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
    async fn ready_check_completion_requires_current_running_generation() {
        struct Case {
            name: &'static str,
            state: ServiceState,
            current_generation: u64,
            completion_generation: u64,
            success: bool,
            expected: ServiceState,
        }

        let cases = vec![
            Case {
                name: "stopped service ignores same-generation completion",
                state: ServiceState::Stopped,
                current_generation: 2,
                completion_generation: 2,
                success: true,
                expected: ServiceState::Stopped,
            },
            Case {
                name: "newer running service ignores stale success",
                state: ServiceState::Running,
                current_generation: 2,
                completion_generation: 1,
                success: true,
                expected: ServiceState::Running,
            },
            Case {
                name: "newer running service ignores stale failure",
                state: ServiceState::Running,
                current_generation: 2,
                completion_generation: 1,
                success: false,
                expected: ServiceState::Running,
            },
            Case {
                name: "current running service accepts success",
                state: ServiceState::Running,
                current_generation: 2,
                completion_generation: 2,
                success: true,
                expected: ServiceState::Ready,
            },
            Case {
                name: "current running service accepts failure",
                state: ServiceState::Running,
                current_generation: 2,
                completion_generation: 2,
                success: false,
                expected: ServiceState::Failed,
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;
            if let Some(service) = runner.services.get_mut("api") {
                service.start_generation = case.current_generation;
            }
            runner.set_service_state("api", case.state);

            runner.handle_ready_check_complete(
                "api",
                case.completion_generation,
                case.success,
                None,
            );

            assert_eq!(
                runner.services.get("api").map(|service| service.state()),
                Some(case.expected),
                "case '{}'",
                case.name,
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_service_completion_ignores_stale_generation() {
        let temp = tempfile::tempdir().unwrap();
        let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;
        if let Some(service) = runner.services.get_mut("api") {
            service.start_generation = 2;
        }
        runner.set_service_state("api", ServiceState::Running);

        runner.handle_item_done(&ItemDone {
            name: "api".to_string(),
            kind: NodeKind::Service,
            success: true,
            message: None,
            elapsed: None,
            last_run: None,
            service_start_generation: Some(1),
            task_run_generation: None,
        });

        assert_eq!(
            runner.services.get("api").map(|service| service.state()),
            Some(ServiceState::Running),
        );
    }

    /// Regression: a watched file changes *during* a bazel build. The build
    /// that completes is correct, but its restart is deferred because the item
    /// went stale. The follow-up cycle then finds bazel "up to date" — and must
    /// still restart, because up-to-date is measured against the last *build*,
    /// not against the *running process*. Without the fix the service keeps
    /// running the old binary forever.
    #[tokio::test(flavor = "current_thread")]
    async fn stale_build_then_up_to_date_followup_still_restarts() {
        let temp = tempfile::tempdir().unwrap();
        let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;

        // The service is up, running the pre-edit binary.
        runner.set_service_state("api", ServiceState::Running);

        // A watched file changed mid-build → the watcher marked it stale.
        runner.mark_rebuild_stale("api");

        // The in-flight build completes successfully. Because it's stale, the
        // restart is deferred to the follow-up cycle (process stays Running).
        runner
            .handle_rebuild_batch_complete(RebuildBatchOutcome {
                build_succeeded: vec!["api".to_string()],
                up_to_date: Vec::new(),
                failed: Vec::new(),
                plain_rebuilds: Vec::new(),
            })
            .await;
        assert_eq!(
            runner.services.get("api").map(|rs| rs.state()),
            Some(ServiceState::Running),
            "a stale build should defer the restart, not restart immediately",
        );

        // Follow-up cycle: the artifact is already built, so bazel reports up
        // to date. The running process still predates that build, so don must
        // restart it rather than no-op.
        runner
            .handle_rebuild_batch_complete(RebuildBatchOutcome {
                build_succeeded: Vec::new(),
                up_to_date: vec!["api".to_string()],
                failed: Vec::new(),
                plain_rebuilds: Vec::new(),
            })
            .await;
        assert_eq!(
            runner.services.get("api").map(|rs| rs.state()),
            Some(ServiceState::Starting),
            "up-to-date follow-up after a deferred build must still restart the process",
        );
    }

    /// The deferred-restart flag set by a stale build must survive the
    /// watcher's re-trigger. In production a fresh `handle_rebuild` runs between
    /// the two batch completions (cycle 1 -> watch re-trigger -> cycle 2); it
    /// clears `rebuild_stale` but must NOT clear `artifact_ahead_of_process`, or
    /// the up-to-date follow-up would no-op and strand the old binary. This is
    /// the same scenario as `stale_build_then_up_to_date_followup_still_restarts`
    /// but exercises the intermediate re-trigger the outcome-only test skips.
    #[tokio::test(flavor = "current_thread")]
    async fn deferred_restart_survives_watch_retrigger() {
        let temp = tempfile::tempdir().unwrap();
        let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;
        runner.set_service_state("api", ServiceState::Running);

        // Cycle 1: a change lands mid-build; the successful build defers restart.
        runner.mark_rebuild_stale("api");
        runner
            .handle_rebuild_batch_complete(RebuildBatchOutcome {
                build_succeeded: vec!["api".to_string()],
                up_to_date: Vec::new(),
                failed: Vec::new(),
                plain_rebuilds: Vec::new(),
            })
            .await;
        assert!(
            runner
                .services
                .get("api")
                .is_some_and(|rs| rs.artifact_ahead_of_process),
            "a stale build should mark the process as behind the latest build",
        );

        // The watcher re-triggers a rebuild. handle_rebuild clears rebuild_stale
        // but must leave the deferred-restart flag intact.
        runner.handle_rebuild("api").await;
        assert!(
            runner
                .services
                .get("api")
                .is_some_and(|rs| rs.artifact_ahead_of_process),
            "the watch re-trigger must not drop the deferred-restart flag",
        );

        // Cycle 2: bazel now reports up to date — must still restart.
        runner
            .handle_rebuild_batch_complete(RebuildBatchOutcome {
                build_succeeded: Vec::new(),
                up_to_date: vec!["api".to_string()],
                failed: Vec::new(),
                plain_rebuilds: Vec::new(),
            })
            .await;
        assert_eq!(
            runner.services.get("api").map(|rs| rs.state()),
            Some(ServiceState::Starting),
            "up-to-date follow-up after the re-trigger must still restart",
        );
    }

    /// The point of the batch window: one edit under a shared source tree
    /// fans out into a rebuild request per affected service, and those must
    /// collapse into a single `bazel build` naming every target. Running them
    /// separately contends for Bazel's server lock and is an order of
    /// magnitude slower.
    #[tokio::test(flavor = "current_thread")]
    async fn a_burst_of_rebuild_requests_becomes_one_batch() {
        let temp = tempfile::tempdir().unwrap();
        let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;

        // A second bazel service caught by the same edit.
        let resolved = runner.services.get("api").unwrap().resolved.clone();
        runner.services.insert(
            "web".to_string(),
            RuntimeService::new(resolved, ServiceState::Ready),
        );
        runner.set_service_state("api", ServiceState::Ready);

        for name in ["api", "web", "api", "web", "api"] {
            runner.handle_rebuild(name).await;
        }
        assert!(runner.builds.has_pending_rebuild("api"));
        assert!(runner.builds.has_pending_rebuild("web"));
        assert!(
            !runner.builds.rebuild_in_flight(),
            "queued requests must wait for the window, not build on arrival",
        );

        runner.flush_pending_rebuilds().await;

        assert!(runner.builds.rebuild_in_flight());
        assert!(
            !runner.builds.has_pending_rebuild("api") && !runner.builds.has_pending_rebuild("web"),
            "all five requests went into the one batch — nothing left over to build again",
        );
    }

    /// Regression: a watched file changes while a build-tool service is still in
    /// its initial/JIT build (`Building`). That rebuild request must not be
    /// dropped — it has to be queued and held until the service finishes coming
    /// up, then run. Previously it was swallowed (RebuildComplete with no
    /// rebuild), leaving the service on pre-edit code with no recovery.
    #[tokio::test(flavor = "current_thread")]
    async fn rebuild_during_build_is_queued_not_dropped() {
        let temp = tempfile::tempdir().unwrap();
        let (mut runner, _shutdown_tx) = single_bazel_runner(temp.path()).await;

        // Service is mid-build (initial bazel build in progress).
        runner.set_service_state("api", ServiceState::Building);

        // A watched file changes during the build. The request must be queued.
        runner.handle_rebuild("api").await;
        assert!(
            runner.builds.has_pending_rebuild("api"),
            "a rebuild requested during the build must be queued, not dropped",
        );

        // Flushing while still building defers — don't race the in-flight build
        // or double-start the service before startup attaches a handle.
        runner.flush_pending_rebuilds().await;
        assert!(
            runner.builds.has_pending_rebuild("api"),
            "rebuild should stay deferred while the service is still building",
        );
        assert!(
            !runner.builds.rebuild_in_flight(),
            "no rebuild build should start while the service is still building",
        );

        // Once the service is up, the deferred rebuild proceeds.
        runner.set_service_state("api", ServiceState::Ready);
        runner.flush_pending_rebuilds().await;
        assert!(
            !runner.builds.has_pending_rebuild("api"),
            "deferred rebuild should fire once the service is running",
        );
        assert!(
            runner.builds.rebuild_in_flight(),
            "a rebuild build should start once the service is running",
        );
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
        assert!(rs.handle_identity.is_none());
        assert!(rs.osc_sink.is_none());
        assert!(rs.attach_lock.is_none());
        assert!(rs.attach_waiter.is_none());
        assert!(rs.proxy.is_none());
        assert!(rs.resolved_watch_paths.is_empty());
        assert!(rs.bazel_binary_path.is_none());
        assert!(!rs.batch_built);
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
            TaskItemState::Pending,
            false,
            None,
        );

        assert_eq!(rt.state(), TaskItemState::Pending);
        assert!(rt.pgid.is_none());
        assert!(rt.osc_sink.is_none());
        assert!(rt.attach_lock.is_none());
        assert!(rt.attach_waiter.is_none());
        assert!(rt.resolved_watch_paths.is_empty());
        assert_eq!(rt.config.cmd, "echo");

        assert!(rt.mark_dependency_failed(vec!["setup".to_string()]));
        assert_eq!(rt.failed_dependencies(), ["setup"]);
        assert_eq!(
            rt.set_state(TaskItemState::Pending),
            Some(TaskItemState::Pending)
        );
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
