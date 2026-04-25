//! Runner — the orchestrator that starts services and tasks in dependency order.
//!
//! The runner builds an execution plan via topological sort, then starts
//! everything whose dependencies are satisfied concurrently using tokio tasks.
//! It owns all service/task state in a plain `HashMap` — no `Arc<Mutex<>>`.
//! Communication uses channels: `mpsc` for commands in, `broadcast` for events out.

mod completions;
mod params;
mod state;

pub mod service;
pub mod task;

pub(crate) use params::resolve_task_params;

use crate::config::{Config, Platform};
use crate::output::OutputManager;
use crate::process::pid_file::PidFile;
use crate::runner::service::stop_service;
use crate::task_state::TaskState;
use crate::watch::WatchManager;
use std::collections::{HashMap, HashSet, VecDeque};
use std::os::unix::io::RawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::SystemTime;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinSet;

use self::service::ServiceHandle;

/// Signal counter: 0 = running, 1 = graceful shutdown, 2 = force shutdown.
static SIGNAL_COUNT: AtomicU8 = AtomicU8::new(0);

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

#[derive(Clone, Copy)]
enum TaskRunMode {
    Startup,
    Triggered,
}

#[derive(Clone, Copy)]
enum ServiceStartMode {
    Full,
    SpawnOnly,
}

#[derive(Clone, Copy, Default)]
pub(crate) enum ServiceStopAction {
    #[default]
    None,
    RestartFull,
    RestartSpawnOnly,
}

#[derive(Clone)]
struct ServiceStartContext {
    resolved: crate::config::ResolvedService,
    batch_built: bool,
    listen_fds: Vec<RawFd>,
    listen_fds_env: HashMap<String, String>,
}

struct RebuildBatchRequest {
    bazel_items: Vec<(String, String)>,
    turbo_by_task: HashMap<String, Vec<(String, String)>>,
    plain_rebuilds: Vec<String>,
}

struct RebuildBatchOutcome {
    build_succeeded: Vec<String>,
    failed: Vec<(String, String)>,
    plain_rebuilds: Vec<String>,
}

struct GraphRequeryRequestItem {
    name: String,
    bazel: Option<crate::config::BazelConfig>,
    turbo: Option<crate::config::TurboConfig>,
    working_dir: PathBuf,
    ignore_patterns: Vec<String>,
}

struct GraphRequeryOutcomeItem {
    name: String,
    ignore_patterns: Vec<String>,
    result: Result<crate::build_tool::ResolvedBuildInfo, String>,
}

#[derive(Clone)]
struct BatchBuildReplayItem {
    name: String,
    kind: NodeKind,
    source_changed: bool,
    graph_changed: bool,
}

enum TaskRunPrepared {
    PendingRun { message: String },
    Skipped,
    Spawned(Box<task::TaskSpawn>),
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
    /// The task is waiting for a manual trigger via `don run --all-pending`.
    /// Set at startup when `auto_run = false`, or on file-watch changes for
    /// such tasks.
    PendingRun,
}

impl TaskItemState {
    pub(crate) fn is_satisfied(&self) -> bool {
        matches!(self, Self::Completed | Self::Skipped | Self::PendingRun)
    }
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

async fn ensure_download_for_config_worker(
    base_dir: &std::path::Path,
    platform: Platform,
    name: &str,
    download: Option<&crate::config::DownloadConfig>,
    service_writer: Option<&crate::output::ServiceWriter>,
    emitter: &crate::output::LifecycleEmitter,
) -> Result<(), crate::download::DownloadError> {
    let download = match download {
        Some(dl) => dl,
        None => return Ok(()),
    };
    let artifact = match download.for_platform(platform) {
        Some(a) => a,
        None => return Ok(()),
    };
    let cache_base = base_dir.join(".don").join("cache");
    let bin_dir = base_dir.join(".don").join("bin");
    emitter.service_event(name, "ensuring artifact...");
    crate::download::ensure_artifact(artifact, &cache_base, name, service_writer).await?;
    if let Some(bin_name) = download.effective_bin_name(platform) {
        crate::download::link_binary(artifact, &cache_base, name, &bin_name, &bin_dir)?;
    }
    emitter.service_event(name, "artifact ready");
    Ok(())
}

async fn run_preset_build_worker(
    base_dir: &std::path::Path,
    emitter: &crate::output::LifecycleEmitter,
    name: &str,
    cmd: &str,
    args: &[String],
    resolved: &crate::config::ResolvedService,
) -> Result<(), String> {
    emitter.service_event(name, &format!("running {cmd} build..."));

    let work_dir = match resolved.dir.as_deref() {
        Some(d) => base_dir.join(d),
        None => base_dir.to_path_buf(),
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
            let build_name = name.to_string();
            let emitter_clone = emitter.clone();
            tokio::spawn(async move {
                let mut reader = tokio::io::BufReader::new(child_output);
                let mut line_buf = Vec::new();
                loop {
                    line_buf.clear();
                    match tokio::io::AsyncBufReadExt::read_until(&mut reader, b'\n', &mut line_buf)
                        .await
                    {
                        Ok(0) => break,
                        Ok(_) => {
                            if line_buf.last() == Some(&b'\n') {
                                line_buf.pop();
                            }
                            if line_buf.last() == Some(&b'\r') {
                                line_buf.pop();
                            }
                            let text = String::from_utf8_lossy(&line_buf);
                            emitter_clone.service_event(&build_name, &text);
                        }
                        Err(e) if e.raw_os_error() == Some(libc::EIO) => break,
                        Err(_) => break,
                    }
                }
            });

            match handle.wait().await {
                Ok(status) if status.success() => {
                    emitter.service_event(name, &format!("{cmd} build succeeded"));
                    Ok(())
                }
                Ok(status) => {
                    let code = status.code().unwrap_or(-1);
                    Err(format!("{cmd} build failed (exit code {code})"))
                }
                Err(e) => Err(format!("{cmd} build error: {e}")),
            }
        }
        Err(e) => Err(format!("failed to start {cmd} build: {e}")),
    }
}

async fn run_service_build_worker(
    base_dir: &std::path::Path,
    docker_client: Option<&bollard::Docker>,
    emitter: &crate::output::LifecycleEmitter,
    name: &str,
    resolved: &crate::config::ResolvedService,
    batch_built: bool,
    service_writer: Option<&crate::output::ServiceWriter>,
) -> Result<(), String> {
    if batch_built {
        return Ok(());
    }

    match &resolved.kind {
        Some(crate::config::ServiceKind::Docker(docker_config)) => {
            if let Some(build_config) = &docker_config.build {
                emitter.service_event(name, "building docker image...");
                if let Some(client) = docker_client
                    && let Some(writer) = service_writer
                {
                    crate::docker::build::build_image(
                        client,
                        build_config,
                        &docker_config.image,
                        base_dir,
                        writer,
                    )
                    .await
                    .map_err(|e| format!("docker build failed: {e}"))?;
                    emitter.service_event(name, "docker build succeeded");
                }
            }
            Ok(())
        }
        Some(crate::config::ServiceKind::Rust(rust_config)) => {
            let build_args = service::rust_build_args(rust_config);
            run_preset_build_worker(base_dir, emitter, name, "cargo", &build_args, resolved).await
        }
        Some(crate::config::ServiceKind::Go(go_config)) => {
            let output_path = service::go_binary_path(go_config, name, base_dir);
            if let Some(parent) = output_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let build_args = service::go_build_args(go_config, &output_path);
            run_preset_build_worker(base_dir, emitter, name, "go", &build_args, resolved).await
        }
        Some(crate::config::ServiceKind::Custom { build, .. }) => {
            if let Some(build_cmd) = build {
                run_preset_build_worker(
                    base_dir,
                    emitter,
                    name,
                    &build_cmd.cmd,
                    &build_cmd.args,
                    resolved,
                )
                .await
            } else {
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_service_worker(
    base_dir: &std::path::Path,
    pid_dir: &std::path::Path,
    platform: Platform,
    docker_client: Option<&bollard::Docker>,
    emitter: &crate::output::LifecycleEmitter,
    name: &str,
    context: &ServiceStartContext,
    mode: ServiceStartMode,
    service_writer: Option<&crate::output::ServiceWriter>,
) -> Result<service::StartResult, String> {
    if matches!(mode, ServiceStartMode::Full) {
        ensure_download_for_config_worker(
            base_dir,
            platform,
            name,
            context.resolved.download.as_ref(),
            service_writer,
            emitter,
        )
        .await
        .map_err(|e| format!("download failed: {e}"))?;

        run_service_build_worker(
            base_dir,
            docker_client,
            emitter,
            name,
            &context.resolved,
            context.batch_built,
            service_writer,
        )
        .await?;
    }

    service::start_service(
        name,
        &context.resolved,
        base_dir,
        pid_dir,
        &context.listen_fds,
        &context.listen_fds_env,
        docker_client,
        service_writer,
        platform,
        Some(emitter),
    )
    .await
    .map_err(|e| e.to_string())
}

async fn run_rebuild_batch_worker(
    request: RebuildBatchRequest,
    base_dir: PathBuf,
    emitter: crate::output::LifecycleEmitter,
    bazel_build_mutex: std::sync::Arc<tokio::sync::Mutex<()>>,
) -> RebuildBatchOutcome {
    let mut build_succeeded: HashSet<String> = HashSet::new();
    let mut failed: Vec<(String, String)> = Vec::new();

    if !request.bazel_items.is_empty() {
        let _guard = bazel_build_mutex.lock().await;
        let targets: Vec<String> = request
            .bazel_items
            .iter()
            .map(|(_, target)| target.clone())
            .collect();
        let target_to_names: HashMap<String, Vec<String>> = {
            let mut names: HashMap<String, Vec<String>> = HashMap::new();
            for (name, target) in &request.bazel_items {
                names.entry(target.clone()).or_default().push(name.clone());
            }
            names
        };

        let resolver = crate::build_tool::bazel::BazelResolver::new().with_emitter(emitter.clone());
        let up_to_date = resolver
            .check_up_to_date(&targets, &base_dir)
            .await
            .unwrap_or_default();
        if up_to_date {
            let count = targets.len();
            emitter.bazel_event(&format!(
                "{count} target{} up to date, skipping rebuild",
                if count == 1 { "" } else { "s" }
            ));
        } else {
            let count = targets.len();
            emitter.bazel_event(&format!(
                "rebuilding {count} target{}...",
                if count == 1 { "" } else { "s" }
            ));
            let line_emitter = emitter.clone();
            match resolver
                .build_targets(
                    &targets,
                    &base_dir,
                    move |line| {
                        line_emitter.bazel_event(line);
                    },
                    Some(&emitter),
                )
                .await
            {
                Ok(result) => {
                    for target in &result.succeeded {
                        if let Some(names) = target_to_names.get(target) {
                            for name in names {
                                build_succeeded.insert(name.clone());
                            }
                        }
                    }
                    for (target, message) in &result.failed {
                        if let Some(names) = target_to_names.get(target) {
                            for name in names {
                                failed
                                    .push((name.clone(), format!("bazel build failed: {message}")));
                            }
                        }
                    }
                }
                Err(e) => {
                    for (name, _) in &request.bazel_items {
                        failed.push((name.clone(), format!("bazel build error: {e}")));
                    }
                }
            }
        }
    }

    for (build_task, items) in &request.turbo_by_task {
        let filters: Vec<String> = items.iter().map(|(_, filter)| filter.clone()).collect();
        let filter_to_names: HashMap<String, Vec<String>> = {
            let mut names: HashMap<String, Vec<String>> = HashMap::new();
            for (name, filter) in items {
                names.entry(filter.clone()).or_default().push(name.clone());
            }
            names
        };
        let count = filters.len();
        emitter.turbo_event(&format!(
            "rebuilding '{build_task}' for {count} package{}...",
            if count == 1 { "" } else { "s" }
        ));

        let line_emitter = emitter.clone();
        let resolver = crate::build_tool::turbo::TurboResolver::new(build_task, None);
        match resolver
            .build_packages(
                build_task,
                &filters,
                &base_dir,
                move |line| {
                    line_emitter.turbo_event(line);
                },
                Some(&emitter),
            )
            .await
        {
            Ok(result) => {
                for filter in &result.succeeded {
                    if let Some(names) = filter_to_names.get(filter) {
                        for name in names {
                            build_succeeded.insert(name.clone());
                        }
                    }
                }
                for (filter, message) in &result.failed {
                    if let Some(names) = filter_to_names.get(filter) {
                        for name in names {
                            failed.push((name.clone(), format!("turbo build failed: {message}")));
                        }
                    }
                }
            }
            Err(e) => {
                for (name, _) in items {
                    failed.push((name.clone(), format!("turbo build error: {e}")));
                }
            }
        }
    }

    RebuildBatchOutcome {
        build_succeeded: build_succeeded.into_iter().collect(),
        failed,
        plain_rebuilds: request.plain_rebuilds,
    }
}

fn bazel_graph_requery_group_dir(working_dir: &std::path::Path) -> PathBuf {
    crate::build_tool::bazel::find_workspace_root(working_dir)
        .unwrap_or_else(|| working_dir.to_path_buf())
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

async fn send_watch_update(
    tx: &mpsc::Sender<crate::watch::WatchUpdate>,
    name: String,
    kind: crate::watch::WatchItemKind,
    patterns: Vec<String>,
    ignore_patterns: Vec<String>,
    base_dir: PathBuf,
) {
    let (applied_tx, applied_rx) = oneshot::channel();
    if tx
        .send(crate::watch::WatchUpdate {
            name,
            kind,
            patterns,
            ignore_patterns,
            base_dir,
            applied_tx: Some(applied_tx),
        })
        .await
        .is_ok()
    {
        let _ = applied_rx.await;
    }
}

fn any_glob_path_changed_since(
    base_dir: &std::path::Path,
    patterns: &[String],
    ignore_patterns: &[String],
    since: SystemTime,
) -> bool {
    let absolute_patterns: Vec<glob::Pattern> = patterns
        .iter()
        .filter_map(|pattern| {
            let full_pattern = base_dir.join(pattern);
            glob::Pattern::new(&full_pattern.to_string_lossy()).ok()
        })
        .collect();
    let absolute_ignore: Vec<glob::Pattern> = ignore_patterns
        .iter()
        .filter_map(|pattern| {
            let full_pattern = base_dir.join(pattern);
            glob::Pattern::new(&full_pattern.to_string_lossy()).ok()
        })
        .collect();
    let mut roots: Vec<PathBuf> = patterns
        .iter()
        .map(|pattern| glob_pattern_base_dir(&base_dir.join(pattern)))
        .collect();
    roots.sort();
    roots.dedup();

    roots
        .into_iter()
        .any(|root| scan_tree_for_changes(&root, &absolute_patterns, &absolute_ignore, since))
}

fn scan_tree_for_changes(
    path: &std::path::Path,
    patterns: &[glob::Pattern],
    ignore_patterns: &[glob::Pattern],
    since: SystemTime,
) -> bool {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return false;
    };
    let path_str = path.to_string_lossy();
    let ignored = ignore_patterns
        .iter()
        .any(|ignore| ignore.matches(&path_str));
    if !ignored && patterns.iter().any(|pattern| pattern.matches(&path_str)) {
        let Ok(modified) = metadata.modified() else {
            return false;
        };
        if modified > since {
            return true;
        }
    }

    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return false;
    }

    let Ok(entries) = std::fs::read_dir(path) else {
        return false;
    };
    for entry in entries.flatten() {
        if scan_tree_for_changes(&entry.path(), patterns, ignore_patterns, since) {
            return true;
        }
    }

    false
}

fn glob_pattern_base_dir(pattern: &std::path::Path) -> PathBuf {
    let mut base = PathBuf::new();
    let mut hit_glob = false;
    for component in pattern.components() {
        let s = component.as_os_str().to_string_lossy();
        if s.contains('*') || s.contains('?') || s.contains('[') {
            hit_glob = true;
            break;
        }
        base.push(component);
    }
    if !hit_glob {
        base = base
            .parent()
            .map(std::path::Path::to_path_buf)
            .unwrap_or_default();
    }
    if base.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        base
    }
}

async fn run_graph_requery_worker(
    items: Vec<GraphRequeryRequestItem>,
    emitter: crate::output::LifecycleEmitter,
) -> Vec<GraphRequeryOutcomeItem> {
    use crate::build_tool::BuildGraphResolver;

    let mut outcomes: Vec<Option<GraphRequeryOutcomeItem>> =
        std::iter::repeat_with(|| None).take(items.len()).collect();
    let mut bazel_groups: HashMap<PathBuf, Vec<(usize, GraphRequeryRequestItem)>> = HashMap::new();
    let mut other_items: Vec<(usize, GraphRequeryRequestItem)> = Vec::new();

    for (idx, item) in items.into_iter().enumerate() {
        if item.bazel.is_some() {
            bazel_groups
                .entry(bazel_graph_requery_group_dir(&item.working_dir))
                .or_default()
                .push((idx, item));
        } else {
            other_items.push((idx, item));
        }
    }

    for (working_dir, group) in bazel_groups {
        let targets: Vec<String> = group
            .iter()
            .filter_map(|(_, item)| item.bazel.as_ref().map(|b| b.target.clone()))
            .collect();
        let resolver = crate::build_tool::bazel::BazelResolver::new().with_emitter(emitter.clone());
        match resolver.resolve_per_target(&targets, &working_dir).await {
            Ok(info_by_target) => {
                for (idx, item) in group {
                    let result = if let Some(ref bazel) = item.bazel {
                        Ok(info_by_target.get(&bazel.target).cloned().unwrap_or(
                            crate::build_tool::ResolvedBuildInfo {
                                watch_paths: Vec::new(),
                                dependencies: Vec::new(),
                                graph_definition_globs: Vec::new(),
                            },
                        ))
                    } else {
                        Err("missing bazel config for batched graph re-query".to_string())
                    };
                    outcomes[idx] = Some(GraphRequeryOutcomeItem {
                        name: item.name,
                        ignore_patterns: item.ignore_patterns,
                        result,
                    });
                }
            }
            Err(e) => {
                let message = e.to_string();
                for (idx, item) in group {
                    outcomes[idx] = Some(GraphRequeryOutcomeItem {
                        name: item.name,
                        ignore_patterns: item.ignore_patterns,
                        result: Err(message.clone()),
                    });
                }
            }
        }
    }

    for (idx, item) in other_items {
        let result = if let Some(ref turbo) = item.turbo {
            let resolver =
                crate::build_tool::turbo::TurboResolver::new(&turbo.task, turbo.filter.as_deref());
            resolver
                .resolve(&turbo.task, &item.working_dir)
                .await
                .map_err(|e| e.to_string())
        } else {
            continue;
        };
        outcomes[idx] = Some(GraphRequeryOutcomeItem {
            name: item.name,
            ignore_patterns: item.ignore_patterns,
            result,
        });
    }

    outcomes.into_iter().flatten().collect()
}

async fn run_task_worker(
    base_dir: PathBuf,
    platform: Platform,
    emitter: crate::output::LifecycleEmitter,
    name: &str,
    task_cfg: &crate::config::Task,
    params: &HashMap<String, String>,
    mode: TaskRunMode,
) -> Result<TaskRunPrepared, String> {
    if matches!(mode, TaskRunMode::Startup) {
        if !task_cfg.auto_run {
            return Ok(TaskRunPrepared::PendingRun {
                message: "pending — auto_run = false".to_string(),
            });
        }
        if !task_cfg.params.is_empty() {
            return Ok(TaskRunPrepared::PendingRun {
                message: "pending — task has params, run manually".to_string(),
            });
        }

        let task_state = TaskState::new(base_dir.join(".don").join("task-state"));
        let watch_base = task_cfg.dir.as_deref().unwrap_or(&base_dir);
        let needs_run = task_state
            .needs_run(name, &task_cfg.watch, Some(watch_base))
            .await
            .unwrap_or(true);
        if !needs_run {
            return Ok(TaskRunPrepared::Skipped);
        }
    }

    ensure_download_for_config_worker(
        &base_dir,
        platform,
        name,
        task_cfg.download.as_ref(),
        None,
        &emitter,
    )
    .await
    .map_err(|e| format!("download failed: {e}"))?;

    let spawn = task::spawn_task(task_cfg, name, &base_dir, platform, params)
        .await
        .map_err(|e| e.to_string())?;
    Ok(TaskRunPrepared::Spawned(Box::new(spawn)))
}

/// A command sent to the runner via its `mpsc` channel.
///
/// `BatchBuildComplete` carries a crate-private payload. The enum itself
/// stays `pub` for callers that hold a [`Runner::command_sender`], but the
/// variant is only ever constructed by the runner's own detached task.
#[allow(private_interfaces)]
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
    /// Result of the startup-phase batch build. Sent by the detached
    /// [`run_batch_build_chain`] task. Transitions `Building` items to
    /// `Pending`/`Failed` and re-runs the ready-item sweep.
    BatchBuildComplete(BatchBuildOutcome),
    /// Result of a detached file-watch build-tool rebuild batch.
    RebuildBatchComplete(RebuildBatchOutcome),
    /// Result of a just-in-time build for a single lazy service triggered
    /// by its first proxy connection. The handler applies the outcome and
    /// (on success) calls `start_service` directly — the service is not in
    /// the `pending` startup set, so the usual `start_ready_items` sweep
    /// wouldn't pick it up.
    LazyBuildComplete {
        name: String,
        outcome: BatchBuildOutcome,
    },
    /// Health-check monitor reported a state transition for a service.
    /// Sent by the per-service monitor task spawned after the service
    /// reaches Ready when `ready.monitor = true`. The runner translates
    /// this into Ready ↔ Unhealthy transitions and (optionally) schedules
    /// an auto-restart.
    ServiceHealthChanged {
        name: String,
        /// `true` after a recovery, `false` after `unhealthy_after`
        /// consecutive failures.
        healthy: bool,
    },
    /// Backoff timer fired — restart a service that's `Unhealthy`
    /// (monitor-driven) or `Failed` (crash-driven). Sent by the detached
    /// task spawned in `schedule_auto_restart`. The `attempt` field is
    /// informational (used in the lifecycle event) and matches
    /// `restart_attempts` at scheduling time.
    AutoRestart { name: String, attempt: u32 },
    /// A service process exited (its output stream hit EOF). Sent by the
    /// per-spawn crash-watcher task. The `pgid` identifies *which* spawn
    /// — the runner ignores the event if the current handle has a
    /// different pgid (the service was already restarted) or if the
    /// state is not Running/Ready/Unhealthy (an explicit stop is in
    /// flight). When it does act, the handler reaps the [`Child`] to
    /// read the real [`std::process::ExitStatus`], transitions the
    /// service to `Failed`, and emits a lifecycle event with the code
    /// or terminating signal.
    ///
    /// [`Child`]: tokio::process::Child
    ServiceExited { name: String, pgid: i32 },
    /// Ready-check completed for a manual-start or rebuild spawn (no
    /// `done_tx` path). Sent from the async task inside
    /// `spawn_and_wire_service` so the runner — running on the main task
    /// with exclusive access to `self.services` — can flip internal state
    /// to `Ready`/`Failed`. Without this, observers get the broadcast but
    /// the runner's own state map never updates, and later
    /// `handle_service_health_changed` probes short-circuit because
    /// `current != Ready`.
    ReadyCheckComplete { name: String, success: bool },
    /// Initiate graceful shutdown.
    Shutdown,
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
    /// a bazel/turbo config. `Some` until [`RunnerCommand::BatchBuildComplete`]
    /// arrives and the handle is consumed. Wrapped in [`AbortOnDrop`] so
    /// shutting the runner down — or dropping the field before completion —
    /// aborts the task, dropping the in-flight `Child` (with `kill_on_drop`)
    /// and sending SIGKILL to the bazel/turbo client.
    batch_build_handle: Option<crate::build_tool::AbortOnDrop<()>>,

    /// Detached JIT build tasks spawned when a lazy service's proxy gets
    /// its first connection. Keyed by service name. Entries are inserted
    /// on spawn and removed when [`RunnerCommand::LazyBuildComplete`]
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
                    RuntimeService::new(svc.resolve(platform), ServiceState::Pending),
                );
            }
        }

        let mut tasks = HashMap::new();
        for (name, task) in &config.tasks {
            if active_tasks.contains(name) {
                tasks.insert(
                    name.clone(),
                    RuntimeTask::new(task.clone(), TaskItemState::Pending),
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
            event_tx,
            done_tx: None,
            shutdown_rx: Some(shutdown_rx),
            _don_pid_file: Some(don_pid_file),
            watch_update_tx: None,
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
    fn get_osc_sink_mut(
        &mut self,
        name: &str,
    ) -> Option<&mut Option<crate::output::OscSinkHandle>> {
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
        // `RunnerCommand::BatchBuildComplete`, which transitions `Building`
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
            .start_ready_items(
                &order,
                &dep_map,
                &mut pending,
                &mut in_flight,
                &done_tx,
                &mut shutdown_rx,
            )
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
                if SIGNAL_COUNT.load(Ordering::SeqCst) >= 1 {
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
                                &mut shutdown_rx,
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
                            RunnerCommand::TaskRunPrepared {
                                name,
                                op_id,
                                task_cfg,
                                intent,
                                result,
                            } => {
                                self.handle_task_run_prepared(&name, op_id, &task_cfg, intent, result)
                                    .await;
                            }
                            RunnerCommand::ServiceStopComplete { name, op_id, result } => {
                                self.handle_service_stop_complete(&name, op_id, result).await;
                            }
                            RunnerCommand::ServiceStartPrepared {
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
                            RunnerCommand::ServiceRebuildPrepared {
                                name,
                                op_id,
                                result,
                            } => {
                                self.handle_service_rebuild_prepared(&name, op_id, result)
                                    .await;
                            }
                            RunnerCommand::TaskExited {
                                name,
                                pgid,
                                success,
                                message,
                                elapsed,
                                rerun,
                            } => {
                                self.handle_task_exit(&name, pgid, success, message, elapsed, rerun);
                            }
                            RunnerCommand::ServiceHealthChanged { name, healthy } => {
                                self.handle_service_health_changed(&name, healthy).await;
                            }
                            RunnerCommand::AutoRestart { name, attempt } => {
                                self.handle_auto_restart(&name, attempt).await;
                            }
                            RunnerCommand::ServiceExited { name, pgid } => {
                                self.handle_service_exited(&name, pgid).await;
                            }
                            RunnerCommand::ReadyCheckComplete { name, success } => {
                                self.handle_ready_check_complete(&name, success);
                            }
                            RunnerCommand::Attach { name, pid, reply } => {
                                self.handle_attach_cmd(&name, pid, reply).await;
                            }
                            RunnerCommand::Detach { name, pty_write } => {
                                self.handle_detach(&name, pty_write).await;
                            }
                            RunnerCommand::Rebuild { name } => {
                                self.handle_rebuild(&name, &mut shutdown_rx).await;
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
                            RunnerCommand::BatchBuildComplete(outcome) => {
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
                                        &mut shutdown_rx,
                                    )
                                    .await?
                                {
                                    self.initiate_shutdown().await;
                                    break;
                                }
                            }
                            RunnerCommand::RebuildBatchComplete(outcome) => {
                                self.rebuild_batch_handle = None;
                                self.handle_rebuild_batch_complete(outcome).await;
                            }
                            RunnerCommand::LazyBuildComplete { name, outcome } => {
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
                            RunnerCommand::GraphRequeryComplete(outcomes) => {
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
                        self.flush_pending_rebuilds(&mut shutdown_rx).await;
                    }
                    // Flush batched build-graph re-queries when the batch window expires.
                    _ = async {
                        match self.bt_requery_deadline {
                            Some(d) => tokio::time::sleep_until(d).await,
                            None => std::future::pending().await,
                        }
                    } => {
                        self.flush_pending_graph_requery(&mut shutdown_rx).await;
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

    /// Start items whose dependencies are all satisfied.
    async fn start_ready_items(
        &mut self,
        order: &[String],
        dep_map: &HashMap<String, Vec<String>>,
        pending: &mut HashSet<String>,
        in_flight: &mut HashSet<String>,
        done_tx: &mpsc::Sender<ItemDone>,
        _shutdown_rx: &mut mpsc::Receiver<()>,
    ) -> Result<bool, RunnerError> {
        // First pass: mark pending items as DependencyFailed when an upstream
        // dep failed (including another DependencyFailed — the cascade is
        // transitive). Lazy services are left alone: the failure of one of
        // their deps doesn't mean the lazy service can never start; it'll
        // re-evaluate when an incoming proxy connection fires.
        let failed_items: Vec<String> = order
            .iter()
            .filter(|name| pending.contains(name.as_str()))
            .filter(|name| {
                // Skip services marked lazy in their resolved config.
                !self
                    .services
                    .get(name.as_str())
                    .is_some_and(|rs| rs.resolved.lazy)
            })
            .filter(|name| {
                let node_deps = dep_map.get(name.as_str()).cloned().unwrap_or_default();
                node_deps.iter().any(|dep| self.is_dep_failed(dep))
            })
            .cloned()
            .collect();

        for name in failed_items {
            pending.remove(&name);
            // `name` is either a service or a task — the helpers are no-ops
            // for unknown names, so we can call both unconditionally.
            self.set_service_state(&name, ServiceState::DependencyFailed);
            self.set_task_state(&name, TaskItemState::DependencyFailed);
            self.output_manager
                .service_error_event(&name, "skipped (dependency failed)");
        }

        // Second pass: start items whose dependencies are all satisfied.
        // Items still in `Building` (batch build in flight) stay in `pending`
        // — they get picked up after `BatchBuildComplete` transitions them
        // back to `Pending` and `start_ready_items` is re-run.
        let ready: Vec<String> = order
            .iter()
            .filter(|name| pending.contains(name.as_str()))
            .filter(|name| !self.is_item_building(name))
            .filter(|name| {
                let node_deps = dep_map.get(name.as_str()).cloned().unwrap_or_default();
                node_deps.iter().all(|dep| self.is_dep_satisfied(dep))
            })
            .cloned()
            .collect();

        for name in ready {
            if SIGNAL_COUNT.load(Ordering::SeqCst) >= 1 {
                return Ok(true);
            }

            // Skip lazy services — they're managed exclusively by the
            // `lazy_start_rx` flow (proxy connection triggers JIT build +
            // start). We gate on the CONFIG `resolved.lazy`, not the runtime
            // state: once the first connection fires, state walks
            // Lazy → Building → Pending → Starting → Running → Ready. If
            // this function re-fires while the service is past `Lazy` (e.g.
            // because a dep became Ready after the lazy-triggered start),
            // a state-based check would miss it and we'd queue a second
            // start worker — double-spawn.
            if self.services.get(&name).is_some_and(|rs| rs.resolved.lazy) {
                pending.remove(&name);
                continue;
            }

            pending.remove(&name);
            in_flight.insert(name.clone());

            if self.services.contains_key(&name) {
                self.output_manager
                    .service_debug_event(&name, "start triggered (deps satisfied)");
                if let Err(e) =
                    self.queue_startup_service_start(&name, done_tx.clone(), ServiceStartMode::Full)
                {
                    self.output_manager
                        .service_error_event(&name, &e.to_string());
                }
            } else if self.tasks.contains_key(&name)
                && let Some(task_cfg) = self.tasks.get(&name).map(|rt| rt.config.clone())
                && let Err(e) = self.spawn_task_worker(
                    &name,
                    task_cfg,
                    HashMap::new(),
                    TaskRunMode::Startup,
                    TaskRunIntent::Startup {
                        done_tx: done_tx.clone(),
                    },
                )
            {
                self.output_manager
                    .service_error_event(&name, &e.to_string());
            }
        }

        Ok(false)
    }

    /// True when the service or task is currently blocked on the startup-
    /// phase batch build. Used by [`Runner::start_ready_items`] to keep
    /// `Building` entries parked in `pending` until the batch completes.
    fn is_item_building(&self, name: &str) -> bool {
        if let Some(rs) = self.services.get(name) {
            return rs.state() == ServiceState::Building;
        }
        if let Some(rt) = self.tasks.get(name) {
            return rt.state() == TaskItemState::Building;
        }
        false
    }

    /// Apply the outcome of the detached batch-build chain: mutate the
    /// runtime state (watch paths, binary paths, `batch_built` flag) and
    /// transition `Building` items to `Pending` (on success) or `Failed`
    /// (on build failure). The caller is responsible for dropping its
    /// cached batch-build handle and re-running [`Self::start_ready_items`]
    /// so newly-unblocked items start.
    fn apply_batch_build_outcome(&mut self, outcome: BatchBuildOutcome) {
        for warning in &outcome.warnings {
            self.output_manager.error_event(warning);
        }

        for (name, kind, paths) in outcome.resolved_watches {
            match kind {
                NodeKind::Service => {
                    if let Some(rs) = self.services.get_mut(&name) {
                        rs.resolved_watch_paths = paths;
                    }
                }
                NodeKind::Task => {
                    if let Some(rt) = self.tasks.get_mut(&name) {
                        rt.resolved_watch_paths = paths;
                    }
                }
            }
        }

        // Binary-path resolution only applies to bazel services — swap in
        // the binary-backed resolved config so subsequent spawns go direct
        // instead of through `bazel run`.
        for (name, path_str) in outcome.binary_paths {
            if let Some(rs) = self.services.get_mut(&name) {
                rs.bazel_binary_path = Some(path_str.clone());
                if let Some(svc) = self.config.services.get(&name) {
                    rs.resolved = svc.resolve_with_bazel_binary(self.platform, &path_str);
                }
            }
        }

        for name in outcome.succeeded {
            let was_building = if let Some(rs) = self.services.get_mut(&name) {
                rs.batch_built = true;
                rs.state() == ServiceState::Building
            } else {
                false
            };
            if was_building {
                self.set_service_state(&name, ServiceState::Pending);
                continue;
            }
            if self.tasks.contains_key(&name) {
                self.set_task_state(&name, TaskItemState::Pending);
            }
        }

        for (name, msg) in outcome.failed {
            self.output_manager
                .service_error_event(&name, &format!("batch build failed: {msg}"));
            if self.services.contains_key(&name) {
                self.set_service_state(&name, ServiceState::Failed);
            }
            if self.tasks.contains_key(&name) {
                self.set_task_state(&name, TaskItemState::Failed);
            }
        }
    }

    fn collect_batch_build_item_by_name(&self, name: &str) -> Option<BatchBuildItem> {
        if let Some(rs) = self.services.get(name) {
            if rs.resolved.is_build_tool_managed() {
                return Some(self.build_batch_item(name, NodeKind::Service, rs));
            }
            return None;
        }

        let rt = self.tasks.get(name)?;
        if rt.config.bazel.is_none() && rt.config.turbo.is_none() {
            return None;
        }
        let working_dir = match rt.config.dir.as_deref() {
            Some(d) => self.base_dir.join(d),
            None => self.base_dir.clone(),
        };
        Some(BatchBuildItem {
            name: name.to_string(),
            kind: NodeKind::Task,
            bazel: rt.config.bazel.clone(),
            turbo: rt.config.turbo.clone(),
            working_dir,
            ignore: rt.config.ignore.clone(),
        })
    }

    fn spawn_startup_batch_build(&mut self, items: Vec<BatchBuildItem>) {
        if items.is_empty() {
            return;
        }

        let cmd_tx = self.cmd_tx.clone();
        let base_dir = self.base_dir.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let watch_update_tx = self.watch_update_tx.clone();
        let handle = tokio::spawn(async move {
            let outcome = run_batch_build_chain(items, base_dir, emitter, watch_update_tx).await;
            let _ = cmd_tx
                .send(RunnerCommand::BatchBuildComplete(outcome))
                .await;
        });
        self.batch_build_handle = Some(crate::build_tool::AbortOnDrop::new(handle));
    }

    fn spawn_lazy_build(&mut self, name: &str, item: BatchBuildItem) {
        let cmd_tx = self.cmd_tx.clone();
        let base_dir = self.base_dir.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let watch_update_tx = self.watch_update_tx.clone();
        let svc_name = name.to_string();
        let handle = tokio::spawn(async move {
            let outcome =
                run_batch_build_chain(vec![item], base_dir, emitter, watch_update_tx).await;
            let _ = cmd_tx
                .send(RunnerCommand::LazyBuildComplete {
                    name: svc_name,
                    outcome,
                })
                .await;
        });
        self.lazy_build_handles.insert(
            name.to_string(),
            crate::build_tool::AbortOnDrop::new(handle),
        );
    }

    fn schedule_startup_batch_replays(&mut self, replay_items: &[BatchBuildReplayItem]) {
        let mut replay_batch = Vec::new();

        for replay in replay_items {
            let Some(item) = self.collect_batch_build_item_by_name(&replay.name) else {
                continue;
            };
            let message = match (replay.source_changed, replay.graph_changed, replay.kind) {
                (true, true, NodeKind::Service) => {
                    "files changed during build — rebuilding before start"
                }
                (true, false, NodeKind::Service) => {
                    "source files changed during build — rebuilding before start"
                }
                (false, true, NodeKind::Service) => {
                    "build graph changed during build — rebuilding before start"
                }
                (true, true, NodeKind::Task) => {
                    "files changed during build — re-running build before start"
                }
                (true, false, NodeKind::Task) => {
                    "source files changed during build — re-running build before start"
                }
                (false, true, NodeKind::Task) => {
                    "build graph changed during build — re-running build before start"
                }
                (false, false, _) => continue,
            };
            self.output_manager.service_event(&replay.name, message);
            match replay.kind {
                NodeKind::Service => self.set_service_state(&replay.name, ServiceState::Building),
                NodeKind::Task => self.set_task_state(&replay.name, TaskItemState::Building),
            }
            replay_batch.push(item);
        }

        self.spawn_startup_batch_build(replay_batch);
    }

    fn schedule_lazy_build_replay(&mut self, replay: &BatchBuildReplayItem) -> bool {
        let Some(item) = self.collect_batch_build_item_by_name(&replay.name) else {
            return false;
        };
        let message = match (replay.source_changed, replay.graph_changed) {
            (true, true) => "files changed during build — rebuilding before start",
            (true, false) => "source files changed during build — rebuilding before start",
            (false, true) => "build graph changed during build — rebuilding before start",
            (false, false) => return false,
        };
        self.output_manager.service_event(&replay.name, message);
        self.set_service_state(&replay.name, ServiceState::Building);
        self.spawn_lazy_build(&replay.name, item);
        true
    }

    /// Check if a dependency is satisfied (ready service or completed task).
    fn is_dep_satisfied(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return rs.state().is_satisfied();
        }
        if let Some(rt) = self.tasks.get(dep) {
            return rt.state().is_satisfied();
        }
        false
    }

    /// Check if a dependency has failed (including the transitive
    /// `DependencyFailed` cascade — if A fails, B depends on A, C depends
    /// on B, then C also needs to be marked).
    fn is_dep_failed(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return matches!(
                rs.state(),
                ServiceState::Failed | ServiceState::DependencyFailed
            );
        }
        if let Some(rt) = self.tasks.get(dep) {
            return matches!(
                rt.state(),
                TaskItemState::Failed | TaskItemState::DependencyFailed
            );
        }
        false
    }

    /// Re-queue items stranded in `DependencyFailed` once no upstream
    /// dependency is failed anymore. They remain `Pending` until the normal
    /// pending-item sweep sees all dependencies as satisfied and starts them.
    fn restore_dependency_failed_items(&mut self) -> bool {
        let dep_map = self.build_dep_map();
        let order = match topological_sort(&dep_map) {
            Ok(order) => order,
            Err(_) => return false,
        };

        let mut restored_any = false;
        for name in order {
            let service_dep_failed = self
                .services
                .get(&name)
                .is_some_and(|rs| rs.state() == ServiceState::DependencyFailed);
            let task_dep_failed = self
                .tasks
                .get(&name)
                .is_some_and(|rt| rt.state() == TaskItemState::DependencyFailed);
            if !service_dep_failed && !task_dep_failed {
                continue;
            }

            let deps = dep_map.get(&name).cloned().unwrap_or_default();
            if deps.iter().any(|dep| self.is_dep_failed(dep)) {
                continue;
            }

            if service_dep_failed {
                self.set_service_state(&name, ServiceState::Pending);
            } else if task_dep_failed {
                self.set_task_state(&name, TaskItemState::Pending);
            }

            self.output_manager
                .service_debug_event(&name, "dependency recovered; re-queued");
            restored_any = true;
        }

        restored_any
    }

    /// Ask the runner loop to re-check `Pending` items on its own task.
    fn schedule_start_pending(&self) {
        let cmd_tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            let _ = cmd_tx.send(RunnerCommand::StartPending).await;
        });
    }

    /// Recover any descendants stranded in `DependencyFailed` and then kick
    /// the normal pending-item sweep so newly-unblocked items can start.
    fn unblock_dependency_failed_items(&mut self) {
        if self.restore_dependency_failed_items() {
            self.schedule_start_pending();
        }
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
            let cmd_tx = self.cmd_tx.clone();
            let watch_name = name.to_string();
            tokio::spawn(async move {
                let _ = crash_exit_rx.await;
                let _ = cmd_tx
                    .send(RunnerCommand::ServiceExited {
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
            let cmd_tx_for_monitor = self.cmd_tx.clone();
            let cmd_tx_for_state = self.cmd_tx.clone();
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
                        .send(RunnerCommand::ReadyCheckComplete {
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

        let cmd_tx = self.cmd_tx.clone();
        let base_dir = self.base_dir.clone();
        let platform = self.platform;
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let name_owned = name.to_string();
        let task_cfg_for_worker = task_cfg.clone();
        let worker = tokio::spawn(async move {
            let result = run_task_worker(
                base_dir,
                platform,
                emitter,
                &name_owned,
                &task_cfg_for_worker,
                &params,
                mode,
            )
            .await;
            let _ = cmd_tx
                .send(RunnerCommand::TaskRunPrepared {
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
                        })
                        .await;
                }
            }
            Ok(TaskRunPrepared::Skipped) => {
                self.set_task_state(name, TaskItemState::Skipped);
                self.output_manager
                    .service_event(name, "skipped (no changes)");
                if let TaskRunIntent::Startup { done_tx } = intent {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name.to_string(),
                            kind: NodeKind::Task,
                            success: true,
                            message: None,
                            elapsed: None,
                        })
                        .await;
                }
            }
            Ok(TaskRunPrepared::Spawned(spawn)) => {
                self.output_manager.service_debug_event(
                    name,
                    &format!("process spawned (pid {})", spawn.handle.pgid()),
                );
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
        let cmd_tx = self.cmd_tx.clone();
        let rerun = done_tx.is_none();

        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = task::wait_for_task(&mut handle, task_cfg_clone.timeout.as_deref()).await;
            let elapsed = start.elapsed();

            let (success, message) = match result {
                Ok(status) => {
                    if status.success() {
                        let task_dir = task_cfg_clone.dir.as_deref().unwrap_or(&base_dir_owned);
                        let _ = task_state
                            .record_success(&name_owned, &task_cfg_clone.watch, Some(task_dir))
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
                    })
                    .await;
            } else {
                let _ = cmd_tx
                    .send(RunnerCommand::TaskExited {
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

    /// Flush all pending build-tool rebuilds as a single batch.
    ///
    /// Collects Bazel targets and Turbo filters from the queued services,
    /// runs one build per tool, then restarts each affected service.
    async fn flush_pending_rebuilds(&mut self, _shutdown_rx: &mut mpsc::Receiver<()>) {
        let names = std::mem::take(&mut self.pending_bt_rebuilds);
        self.bt_rebuild_deadline = None;

        if names.is_empty() {
            return;
        }
        if self.rebuild_batch_handle.is_some() {
            for name in names {
                if !self.pending_bt_rebuilds.contains(&name) {
                    self.pending_bt_rebuilds.push(name);
                }
            }
            self.bt_rebuild_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(50));
            return;
        }

        // Partition into Bazel and Turbo groups.
        // Bazel: (service_name, target)
        let mut bazel_items: Vec<(String, String)> = Vec::new();
        // Turbo: grouped by build_task → (service_name, filter)
        let mut turbo_by_task: HashMap<String, Vec<(String, String)>> = HashMap::new();
        // Services without a build tool target (shouldn't happen, but handle gracefully)
        let mut plain_rebuilds: Vec<String> = Vec::new();

        for name in &names {
            if let Some(rs) = self.services.get(name) {
                match &rs.resolved.kind {
                    Some(crate::config::ServiceKind::Bazel(bazel)) => {
                        bazel_items.push((name.clone(), bazel.target.clone()));
                    }
                    Some(crate::config::ServiceKind::Turbo(turbo)) => {
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
                        } else {
                            plain_rebuilds.push(name.clone());
                        }
                    }
                    _ => {
                        plain_rebuilds.push(name.clone());
                    }
                }
            }
        }
        let request = RebuildBatchRequest {
            bazel_items,
            turbo_by_task,
            plain_rebuilds,
        };
        let cmd_tx = self.cmd_tx.clone();
        let base_dir = self.base_dir.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let bazel_build_mutex = self.bazel_build_mutex.clone();
        let handle = tokio::spawn(async move {
            let outcome =
                run_rebuild_batch_worker(request, base_dir, emitter, bazel_build_mutex).await;
            let _ = cmd_tx
                .send(RunnerCommand::RebuildBatchComplete(outcome))
                .await;
        });
        self.rebuild_batch_handle = Some(crate::build_tool::AbortOnDrop::new(handle));
    }

    fn fail_rebuild(&self, name: &str, message: &str) {
        self.output_manager.service_error_event(name, message);
        let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
            name: name.to_string(),
            success: false,
        });
    }

    async fn handle_rebuild_batch_complete(&mut self, outcome: RebuildBatchOutcome) {
        for (name, message) in &outcome.failed {
            self.fail_rebuild(name, message);
        }
        for name in &outcome.build_succeeded {
            self.do_rebuild(name).await;
        }
        for name in &outcome.plain_rebuilds {
            self.do_rebuild(name).await;
        }
        if !self.pending_bt_rebuilds.is_empty() {
            self.bt_rebuild_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(50));
        }
    }

    async fn handle_graph_requery_complete(&mut self, outcomes: Vec<GraphRequeryOutcomeItem>) {
        let watch_update_tx = self.watch_update_tx.clone();
        let mut services_to_rebuild: Vec<String> = Vec::new();
        let mut tasks_to_rerun: Vec<String> = Vec::new();

        for outcome in outcomes {
            match outcome.result {
                Ok(info) => {
                    let count = info.watch_paths.len();
                    self.output_manager.service_event(
                        &outcome.name,
                        &format!(
                            "updated watch paths ({count} path{})",
                            if count == 1 { "" } else { "s" }
                        ),
                    );
                    if let Some(rs) = self.services.get_mut(&outcome.name) {
                        rs.resolved_watch_paths = info.watch_paths.clone();
                    } else if let Some(rt) = self.tasks.get_mut(&outcome.name) {
                        rt.resolved_watch_paths = info.watch_paths.clone();
                    }
                    if let Some(ref tx) = watch_update_tx {
                        let kind = if self.services.contains_key(&outcome.name) {
                            crate::watch::WatchItemKind::Service
                        } else {
                            crate::watch::WatchItemKind::Task
                        };
                        send_watch_update(
                            tx,
                            outcome.name.clone(),
                            kind,
                            info.watch_paths.clone(),
                            outcome.ignore_patterns,
                            self.base_dir.clone(),
                        )
                        .await;
                        send_watch_update(
                            tx,
                            format!("{}__graph", outcome.name),
                            crate::watch::WatchItemKind::BuildGraph,
                            info.graph_definition_globs,
                            Vec::new(),
                            self.base_dir.clone(),
                        )
                        .await;
                    }

                    if let Some(rs) = self.services.get(&outcome.name) {
                        if should_rebuild_after_graph_requery(rs)
                            && !services_to_rebuild.contains(&outcome.name)
                        {
                            services_to_rebuild.push(outcome.name.clone());
                        }
                    } else if self.tasks.contains_key(&outcome.name)
                        && !tasks_to_rerun.contains(&outcome.name)
                    {
                        tasks_to_rerun.push(outcome.name.clone());
                    }
                }
                Err(e) => {
                    self.output_manager.service_error_event(
                        &outcome.name,
                        &format!(
                            "build tool re-query failed: {e} — keeping existing watch patterns"
                        ),
                    );
                }
            }
        }
        if !self.pending_graph_requery.is_empty() {
            self.bt_requery_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(100));
        }

        if !services_to_rebuild.is_empty() {
            for name in services_to_rebuild {
                self.output_manager
                    .service_event(&name, "build graph changed — rebuilding");
                if !self.pending_bt_rebuilds.contains(&name) {
                    self.pending_bt_rebuilds.push(name);
                }
            }
            self.bt_rebuild_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(50));
        }

        for name in tasks_to_rerun {
            self.output_manager
                .service_event(&name, "build graph changed — re-running");
            self.handle_task_rerun(&name).await;
        }
    }

    /// Handle a build graph change event (BUILD files, package.json, etc. changed).
    ///
    /// Queues the item for a batched re-query instead of spawning immediately.
    /// This prevents redundant concurrent queries when a single BUILD file
    /// change affects multiple services.
    async fn handle_build_graph_changed(&mut self, name: &str) {
        if name == crate::watch::WORKSPACE_GRAPH_ITEM_NAME {
            let service_names: Vec<String> = self
                .services
                .iter()
                .filter(|(_, rs)| rs.resolved.is_build_tool_managed())
                .map(|(service_name, _)| service_name.clone())
                .collect();
            let task_names: Vec<String> = self
                .tasks
                .iter()
                .filter(|(_, rt)| rt.config.bazel.is_some() || rt.config.turbo.is_some())
                .map(|(task_name, _)| task_name.clone())
                .collect();
            for item_name in service_names.into_iter().chain(task_names) {
                if !self.pending_graph_requery.contains(&item_name) {
                    self.pending_graph_requery.push(item_name);
                }
            }
        } else if !self.pending_graph_requery.contains(&name.to_string()) {
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
    async fn flush_pending_graph_requery(&mut self, _shutdown_rx: &mut mpsc::Receiver<()>) {
        let names = std::mem::take(&mut self.pending_graph_requery);
        self.bt_requery_deadline = None;

        if names.is_empty() {
            return;
        }
        if self.watch_update_tx.is_none() {
            return;
        }
        if self.graph_requery_handle.is_some() {
            for name in names {
                if !self.pending_graph_requery.contains(&name) {
                    self.pending_graph_requery.push(name);
                }
            }
            self.bt_requery_deadline =
                Some(tokio::time::Instant::now() + std::time::Duration::from_millis(100));
            return;
        }

        self.output_manager.lifecycle_event(&format!(
            "re-querying build tool for {} item{}...",
            names.len(),
            if names.len() == 1 { "" } else { "s" }
        ));

        let mut items = Vec::new();
        for name in &names {
            let (bazel, turbo, item_dir, ignore_patterns) =
                if let Some(rs) = self.services.get(name) {
                    (
                        rs.resolved.bazel_config().cloned(),
                        rs.resolved.turbo_config().cloned(),
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
            if bazel.is_none() && turbo.is_none() {
                continue;
            }
            let working_dir = match item_dir {
                Some(d) => self.base_dir.join(d),
                None => self.base_dir.clone(),
            };
            items.push(GraphRequeryRequestItem {
                name: name.clone(),
                bazel,
                turbo,
                working_dir,
                ignore_patterns,
            });
        }
        if items.is_empty() {
            return;
        }
        let cmd_tx = self.cmd_tx.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let handle = tokio::spawn(async move {
            let outcomes = run_graph_requery_worker(items, emitter).await;
            let _ = cmd_tx
                .send(RunnerCommand::GraphRequeryComplete(outcomes))
                .await;
        });
        self.graph_requery_handle = Some(crate::build_tool::AbortOnDrop::new(handle));
    }

    /// Snapshot of a batch-buildable service or task — everything the
    /// standalone [`run_batch_build_chain`] needs. Taken at startup before
    /// the detached task runs so the task doesn't touch `self`.
    fn collect_batch_build_items(&self) -> Vec<BatchBuildItem> {
        let mut items: Vec<BatchBuildItem> = Vec::new();

        for (name, rs) in &self.services {
            if !rs.resolved.is_build_tool_managed() {
                continue;
            }
            // Lazy bazel/turbo services defer their query+build+cquery to
            // first connection (JIT in the `lazy_start_rx` handler). Pulling
            // them into the startup batch would query and build services
            // the user may never touch this session.
            if rs.resolved.lazy {
                continue;
            }
            items.push(self.build_batch_item(name, NodeKind::Service, rs));
        }
        for (name, rt) in &self.tasks {
            if rt.config.bazel.is_none() && rt.config.turbo.is_none() {
                continue;
            }
            let working_dir = match rt.config.dir.as_deref() {
                Some(d) => self.base_dir.join(d),
                None => self.base_dir.clone(),
            };
            items.push(BatchBuildItem {
                name: name.clone(),
                kind: NodeKind::Task,
                bazel: rt.config.bazel.clone(),
                turbo: rt.config.turbo.clone(),
                working_dir,
                ignore: rt.config.ignore.clone(),
            });
        }

        items
    }

    /// Snapshot a single service into a [`BatchBuildItem`] for the JIT
    /// lazy-build path. Shares the field layout with
    /// [`Self::collect_batch_build_items`] so the chain logic doesn't care
    /// whether the build is startup-batched or JIT.
    fn build_batch_item(&self, name: &str, kind: NodeKind, rs: &RuntimeService) -> BatchBuildItem {
        let working_dir = match rs.resolved.dir.as_deref() {
            Some(d) => self.base_dir.join(d),
            None => self.base_dir.clone(),
        };
        BatchBuildItem {
            name: name.to_string(),
            kind,
            bazel: rs.resolved.bazel_config().cloned(),
            turbo: rs.resolved.turbo_config().cloned(),
            working_dir,
            ignore: rs.resolved.ignore.clone(),
        }
    }

    /// Try to start any Pending services/tasks whose dependencies are now satisfied.
    /// Driven by the `StartPending` command; re-schedules itself if items remain pending.
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
                .is_some_and(|rs| rs.state() == ServiceState::Pending);
            let is_pending_task = self
                .tasks
                .get(name)
                .is_some_and(|rt| rt.state() == TaskItemState::Pending);
            if !is_pending_svc && !is_pending_task {
                continue;
            }

            let deps = dep_map.get(name).cloned().unwrap_or_default();
            let deps_ok = deps.iter().all(|dep| self.is_dep_satisfied(dep));
            if !deps_ok {
                continue;
            }

            if is_pending_svc {
                // Skip lazy services for the same reason as `start_ready_items`:
                // the `lazy_start_rx` flow owns them. A lazy service briefly
                // in `Pending` state during `LazyBuildComplete`'s
                // Building→Pending→Starting transition must not be double-started
                // by a concurrently-scheduled `StartPending`.
                if self.services.get(name).is_some_and(|rs| rs.resolved.lazy) {
                    continue;
                }
                if self.services.contains_key(name)
                    && let Some(done_tx) = self.done_tx.clone()
                {
                    self.output_manager
                        .service_event(name, "start triggered (pending sweep)");
                    if let Err(e) =
                        self.queue_startup_service_start(name, done_tx, ServiceStartMode::Full)
                    {
                        self.output_manager
                            .service_error_event(name, &e.to_string());
                    }
                    started_any = true;
                }
            } else if is_pending_task {
                self.set_task_state(name, TaskItemState::Running);
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
                .any(|rs| rs.state() == ServiceState::Pending)
                || self
                    .tasks
                    .values()
                    .any(|rt| rt.state() == TaskItemState::Pending);
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
    async fn handle_rebuild(&mut self, name: &str, _shutdown_rx: &mut mpsc::Receiver<()>) {
        let rs = match self.services.get(name) {
            Some(rs) => rs,
            None => {
                self.fail_rebuild(name, "rebuild requested for unknown service");
                return;
            }
        };

        if rs.state() == ServiceState::Building {
            let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                name: name.to_string(),
                success: true,
            });
            return;
        }

        // For build-tool-managed services, queue the rebuild into a batch.
        // Multiple services sharing the same source files will be batched into
        // one `bazel build //a //b //c` invocation instead of separate builds.
        if rs.resolved.is_build_tool_managed() {
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
                self.fail_rebuild(name, "rebuild requested for unknown service");
                return;
            }
        };
        // For build-tool-managed services the batch build has already run by
        // the time we reach `do_rebuild`, and the actual restart is surfaced
        // later by `queue_rebuild_service_start`'s "restarting..." event.
        // Emitting another pre-stop "restarting" here just creates log noise.
        //
        // For other kinds, the detached rebuild worker will kick off the
        // build after this lifecycle event, so "rebuilding" still lands
        // before the build output.
        let message = if resolved.is_build_tool_managed() {
            None
        } else {
            Some("rebuilding (file changed)")
        };
        if let Some(message) = message {
            self.output_manager.service_event(name, message);
        }
        if resolved.is_build_tool_managed() {
            self.continue_rebuild_restart(name).await;
            return;
        }
        if let Err(e) = self.spawn_service_rebuild_worker(name, resolved) {
            self.fail_rebuild(name, &e.to_string());
        }
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

    fn service_start_snapshot(&self, name: &str) -> Result<ServiceStartContext, CommandError> {
        let mut resolved = match self.services.get(name) {
            Some(rs) => rs.resolved.clone(),
            None => {
                return Err(CommandError::UnknownService {
                    name: name.to_string(),
                });
            }
        };
        let (listen_fds, listen_fds_env) = if let Some(rs) = self.services.get(name)
            && let Some(ref proxy) = rs.proxy
        {
            resolved.env.extend(proxy.env_vars());
            (proxy.listenfd_raw_fds(), proxy.listenfd_env())
        } else {
            (Vec::new(), HashMap::new())
        };
        let batch_built = self.services.get(name).is_some_and(|rs| rs.batch_built);
        Ok(ServiceStartContext {
            resolved,
            batch_built,
            listen_fds,
            listen_fds_env,
        })
    }

    fn spawn_service_start_worker(
        &mut self,
        name: &str,
        context: ServiceStartContext,
        mode: ServiceStartMode,
        intent: ServiceStartIntent,
    ) -> Result<(), CommandError> {
        let Some(rs) = self.services.get_mut(name) else {
            return Err(CommandError::UnknownService {
                name: name.to_string(),
            });
        };
        rs.start_generation = rs.start_generation.saturating_add(1);
        let op_id = rs.start_generation;

        let cmd_tx = self.cmd_tx.clone();
        let name_owned = name.to_string();
        let base_dir = self.base_dir.clone();
        let pid_dir = self.base_dir.join(".don").join("pids");
        let platform = self.platform;
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let service_writer = self.output_manager.service_writer(name);
        let docker_client = self.docker_client.clone();
        let context_for_worker = context.clone();

        let worker = tokio::spawn(async move {
            let result = start_service_worker(
                &base_dir,
                &pid_dir,
                platform,
                docker_client.as_ref(),
                &emitter,
                &name_owned,
                &context_for_worker,
                mode,
                service_writer.as_ref(),
            )
            .await;

            let _ = cmd_tx
                .send(RunnerCommand::ServiceStartPrepared {
                    name: name_owned,
                    op_id,
                    context: Box::new(context),
                    intent,
                    result: result.map(Box::new),
                })
                .await;
        });
        rs.start_worker = Some(worker);
        Ok(())
    }

    async fn handle_service_start_prepared(
        &mut self,
        name: &str,
        op_id: u64,
        context: Box<ServiceStartContext>,
        intent: ServiceStartIntent,
        result: Result<Box<service::StartResult>, String>,
    ) {
        let is_current = self
            .services
            .get(name)
            .is_some_and(|rs| rs.start_generation == op_id);
        if !is_current {
            if let Ok(start_result) = result {
                let shutdown_config = context.resolved.shutdown.clone();
                tokio::spawn(async move {
                    let start_result = *start_result;
                    let service::StartResult {
                        handle,
                        child_output,
                    } = start_result;
                    drop(child_output);
                    let _ = stop_service(handle, shutdown_config.as_ref(), true, false).await;
                });
            }
            return;
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.start_worker = None;
        }

        match result {
            Ok(start_result) => match intent {
                ServiceStartIntent::Startup { done_tx } => {
                    self.wire_service_output_and_ready_check(
                        name,
                        *start_result,
                        &context.resolved,
                        Some(done_tx),
                    )
                    .await;
                }
                ServiceStartIntent::Reply { reply } => {
                    self.wire_service_output_and_ready_check(
                        name,
                        *start_result,
                        &context.resolved,
                        None,
                    )
                    .await;
                    let _ = reply.send(Ok(()));
                }
                ServiceStartIntent::Background => {
                    self.wire_service_output_and_ready_check(
                        name,
                        *start_result,
                        &context.resolved,
                        None,
                    )
                    .await;
                }
            },
            Err(message) => {
                self.set_service_state(name, ServiceState::Failed);
                self.output_manager.service_error_event(name, &message);
                match intent {
                    ServiceStartIntent::Startup { done_tx } => {
                        let _ = done_tx
                            .send(ItemDone {
                                name: name.to_string(),
                                kind: NodeKind::Service,
                                success: false,
                                message: Some(message),
                                elapsed: None,
                            })
                            .await;
                    }
                    ServiceStartIntent::Reply { reply } => {
                        let _ = reply.send(Err(CommandError::Failed {
                            name: name.to_string(),
                            message,
                        }));
                    }
                    ServiceStartIntent::Background => {}
                }
            }
        }
    }

    fn spawn_service_rebuild_worker(
        &mut self,
        name: &str,
        resolved: crate::config::ResolvedService,
    ) -> Result<(), CommandError> {
        let Some(rs) = self.services.get_mut(name) else {
            return Err(CommandError::UnknownService {
                name: name.to_string(),
            });
        };
        rs.rebuild_generation = rs.rebuild_generation.saturating_add(1);
        let op_id = rs.rebuild_generation;

        let cmd_tx = self.cmd_tx.clone();
        let name_owned = name.to_string();
        let base_dir = self.base_dir.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let docker_client = self.docker_client.clone();
        let service_writer = self.output_manager.service_writer(name);
        let worker = tokio::spawn(async move {
            let result = run_service_build_worker(
                &base_dir,
                docker_client.as_ref(),
                &emitter,
                &name_owned,
                &resolved,
                false,
                service_writer.as_ref(),
            )
            .await;
            let _ = cmd_tx
                .send(RunnerCommand::ServiceRebuildPrepared {
                    name: name_owned,
                    op_id,
                    result,
                })
                .await;
        });
        rs.rebuild_worker = Some(worker);
        Ok(())
    }

    async fn continue_rebuild_restart(&mut self, name: &str) {
        let has_proxy = self.services.get(name).is_some_and(|rs| rs.proxy.is_some());
        if has_proxy
            && let Some(rs) = self.services.get(name)
            && let Some(ref proxy) = rs.proxy
        {
            proxy.clear_backend();
        }

        if let Some(handle) = self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
            if self.remove_attach_lock(name) {
                self.output_manager.resume_stdout_sink(name).await;
            }
            self.set_service_state(name, ServiceState::Stopping);
            let shutdown_config = self
                .services
                .get(name)
                .and_then(|rs| rs.resolved.shutdown.clone());
            let wait_full = self
                .services
                .get(name)
                .and_then(|rs| rs.proxy.as_ref())
                .is_some_and(|p| p.requires_full_exit_on_restart());
            let (reply_tx, _reply_rx) = oneshot::channel();
            self.spawn_manual_service_stop_worker(
                name,
                handle,
                shutdown_config,
                wait_full,
                reply_tx,
                ServiceStopAction::RestartSpawnOnly,
            );
            return;
        }

        if let Err(e) = self.queue_rebuild_service_start(name).await {
            self.fail_rebuild(name, &e.to_string());
        }
    }

    async fn handle_service_rebuild_prepared(
        &mut self,
        name: &str,
        op_id: u64,
        result: Result<(), String>,
    ) {
        let is_current = self
            .services
            .get(name)
            .is_some_and(|rs| rs.rebuild_generation == op_id);
        if !is_current {
            return;
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.rebuild_worker = None;
        }

        match result {
            Ok(()) => self.continue_rebuild_restart(name).await,
            Err(message) if message == "shutdown requested" => {
                self.initiate_shutdown().await;
            }
            Err(message) => self.fail_rebuild(name, &message),
        }
    }

    fn queue_background_service_start(
        &mut self,
        name: &str,
        mode: ServiceStartMode,
    ) -> Result<(), CommandError> {
        let context = self.service_start_snapshot(name)?;
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager.service_event(name, "starting...");
        self.spawn_service_start_worker(name, context, mode, ServiceStartIntent::Background)
    }

    async fn queue_rebuild_service_start(&mut self, name: &str) -> Result<(), CommandError> {
        let realloc_result = if let Some(rs) = self.services.get_mut(name) {
            if let Some(ref mut proxy) = rs.proxy {
                Some(proxy.reallocate_ephemeral_ports().await)
            } else {
                None
            }
        } else {
            return Err(CommandError::UnknownService {
                name: name.to_string(),
            });
        };
        if let Some(Err(e)) = realloc_result {
            return Err(CommandError::Failed {
                name: name.to_string(),
                message: format!("failed to allocate ephemeral ports: {e}"),
            });
        }

        let context = self.service_start_snapshot(name)?;
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager.service_event(name, "restarting...");
        self.spawn_service_start_worker(
            name,
            context,
            ServiceStartMode::SpawnOnly,
            ServiceStartIntent::Background,
        )
    }

    fn queue_startup_service_start(
        &mut self,
        name: &str,
        done_tx: mpsc::Sender<ItemDone>,
        mode: ServiceStartMode,
    ) -> Result<(), CommandError> {
        let context = self.service_start_snapshot(name)?;
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager.service_event(name, "starting...");
        self.spawn_service_start_worker(
            name,
            context,
            mode,
            ServiceStartIntent::Startup { done_tx },
        )
    }

    async fn handle_start_service_cmd(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }
        if self
            .services
            .get(name)
            .is_some_and(|rs| rs.handle.is_some())
        {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "already running".to_string(),
            }));
            return;
        }
        let context = match self.service_start_snapshot(name) {
            Ok(snapshot) => snapshot,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager
            .service_event(name, "starting... (requested)");
        if let Err(e) = self.spawn_service_start_worker(
            name,
            context,
            ServiceStartMode::Full,
            ServiceStartIntent::Reply { reply },
        ) {
            self.output_manager
                .service_error_event(name, &e.to_string());
        }
    }

    fn spawn_manual_service_stop_worker(
        &mut self,
        name: &str,
        handle: ServiceHandle,
        shutdown_config: Option<crate::config::ShutdownConfig>,
        wait_full_exit: bool,
        reply: oneshot::Sender<CommandResult>,
        stop_action: ServiceStopAction,
    ) {
        let Some(rs) = self.services.get_mut(name) else {
            let _ = reply.send(Err(CommandError::UnknownService {
                name: name.to_string(),
            }));
            return;
        };
        rs.control_generation = rs.control_generation.saturating_add(1);
        let op_id = rs.control_generation;
        rs.control_reply = Some(reply);
        rs.stop_action = stop_action;

        let cmd_tx = self.cmd_tx.clone();
        let name_owned = name.to_string();
        let shutdown_rx = self.shutdown_flag_tx.subscribe();
        let worker = tokio::spawn(async move {
            let result = service::stop_service_interruptibly(
                handle,
                shutdown_config.as_ref(),
                wait_full_exit,
                shutdown_rx,
            )
            .await
            .map_err(|e| e.to_string());
            let _ = cmd_tx
                .send(RunnerCommand::ServiceStopComplete {
                    name: name_owned,
                    op_id,
                    result,
                })
                .await;
        });
        rs.control_worker = Some(worker);
    }

    async fn handle_service_stop_complete(
        &mut self,
        name: &str,
        op_id: u64,
        result: Result<(), String>,
    ) {
        let is_current = self
            .services
            .get(name)
            .is_some_and(|rs| rs.control_generation == op_id);
        if !is_current {
            return;
        }

        let (reply, stop_action) = match self.services.get_mut(name) {
            Some(rs) => {
                rs.control_worker = None;
                (rs.control_reply.take(), std::mem::take(&mut rs.stop_action))
            }
            None => return,
        };

        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        match result {
            Ok(()) => {
                self.set_service_state(name, ServiceState::Stopped);
                let next_result = match stop_action {
                    ServiceStopAction::None => Ok(()),
                    ServiceStopAction::RestartFull => {
                        self.queue_background_service_start(name, ServiceStartMode::Full)
                    }
                    ServiceStopAction::RestartSpawnOnly => {
                        self.queue_rebuild_service_start(name).await
                    }
                };
                if let Some(reply) = reply {
                    let _ = reply.send(next_result);
                }
            }
            Err(message) => {
                self.set_service_state(name, ServiceState::Failed);
                self.output_manager.service_error_event(name, &message);
                if let Some(reply) = reply {
                    let _ = reply.send(Err(CommandError::Failed {
                        name: name.to_string(),
                        message,
                    }));
                }
            }
        }
    }

    /// Handle an API-initiated Stop command.
    async fn handle_stop_cmd(&mut self, name: &str, reply: oneshot::Sender<CommandResult>) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }
        // A lazy service in Lazy state has no process — just mark it Stopped.
        if self
            .services
            .get(name)
            .is_some_and(|rs| rs.state() == ServiceState::Lazy)
        {
            self.set_service_state(name, ServiceState::Stopped);
            self.output_manager
                .service_event(name, "stopped (was lazy)");
            let _ = reply.send(Ok(()));
            return;
        }
        // Cancel monitor + any pending auto-restart before tearing down the
        // process so a recovery probe doesn't race with the stop.
        if let Some(rs) = self.services.get_mut(name) {
            rs.stop_health_tracking();
            rs.restart_attempts = 0;
        }
        let handle = self
            .services
            .get_mut(name)
            .and_then(|rs| rs.handle.take())
            .ok_or_else(|| CommandError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            });
        let handle = match handle {
            Ok(h) => h,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        let shutdown_config = self
            .services
            .get(name)
            .and_then(|rs| rs.resolved.shutdown.clone());
        // Release attach lock if held — the PTY write in the attach session
        // becomes invalid once the service stops (process gone).
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        self.set_service_state(name, ServiceState::Stopping);
        self.output_manager
            .service_event(name, "stopping... (requested)");
        self.spawn_manual_service_stop_worker(
            name,
            handle,
            shutdown_config,
            false,
            reply,
            ServiceStopAction::None,
        );
    }

    /// Runner-internal handler for [`RunnerCommand::ReadyCheckComplete`].
    ///
    /// Emitted by the async ready-check task inside
    /// [`wire_service_output_and_ready_check`] when there's no `done_tx`
    /// (manual start or rebuild). Updates the runner's own state — the
    /// broadcast follows via `set_service_state`.
    ///
    /// On failure, mirrors `handle_service_done`'s lazy-retry behaviour so
    /// a proxied lazy service resets to `Lazy` instead of getting stuck on
    /// `Failed`.
    fn handle_ready_check_complete(&mut self, name: &str, success: bool) {
        if !self.services.contains_key(name) {
            return;
        }
        if success {
            self.set_service_state(name, ServiceState::Ready);
            self.unblock_dependency_failed_items();
            return;
        }
        let is_lazy = self
            .services
            .get(name)
            .is_some_and(|rs| rs.resolved.lazy && rs.proxy.is_some());
        if is_lazy {
            self.set_service_state(name, ServiceState::Lazy);
            self.unblock_dependency_failed_items();
            if let Some(rs) = self.services.get_mut(name)
                && let Some(ref mut proxy) = rs.proxy
            {
                proxy.rearm_lazy_watchers();
            }
        } else {
            self.set_service_state(name, ServiceState::Failed);
        }
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

    async fn handle_restart_service_cmd(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }
        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => {
                let _ = reply.send(Err(CommandError::UnknownService {
                    name: name.to_string(),
                }));
                return;
            }
        };
        if matches!(
            state,
            ServiceState::Lazy | ServiceState::Stopped | ServiceState::Failed
        ) {
            let _ = reply.send(self.queue_background_service_start(name, ServiceStartMode::Full));
            return;
        }

        if let Some(rs) = self.services.get_mut(name) {
            rs.stop_health_tracking();
            rs.restart_attempts = 0;
        }
        let handle = match self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
            Some(h) => h,
            None => {
                let _ =
                    reply.send(self.queue_background_service_start(name, ServiceStartMode::Full));
                return;
            }
        };
        let shutdown_config = self
            .services
            .get(name)
            .and_then(|rs| rs.resolved.shutdown.clone());
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        self.set_service_state(name, ServiceState::Stopping);
        self.output_manager
            .service_event(name, "stopping... (requested restart)");
        self.spawn_manual_service_stop_worker(
            name,
            handle,
            shutdown_config,
            false,
            reply,
            ServiceStopAction::RestartFull,
        );
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
        let cmd_tx = self.cmd_tx.clone();
        let name_owned = name.to_string();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
            let _ = cmd_tx
                .send(RunnerCommand::AutoRestart {
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
            self.services
                .get(name)
                .is_some_and(|rs| rs.handle.is_some())
        } else {
            self.tasks.get(name).is_some_and(|rt| rt.pgid.is_some())
        };

        if !is_running {
            // Not running — register a waiter. The reply will be sent when
            // the process next spawns.
            self.output_manager
                .service_event(name, "waiting for process to start (attach pending)");
            self.set_attach_waiter(name, AttachWaiter { pid, reply });
            return;
        }

        // Running — attach immediately.
        let result = self.fulfill_attach(name, pid).await;
        let _ = reply.send(result);
    }

    /// Fulfill an attach request for a running process. Assumes the caller
    /// has already validated the name exists and checked the attach lock.
    async fn fulfill_attach(
        &mut self,
        name: &str,
        pid: u32,
    ) -> Result<AttachSession, CommandError> {
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
            .service_event(name, &format!("attached (pid {pid})"));

        Ok(AttachSession {
            pty_write,
            output_rx,
        })
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

        self.output_manager.service_event(name, "detached");
    }

    /// Handle a file-watch-triggered task re-run.
    ///
    /// Respects `auto_run = false` — tasks that have opted out transition to
    /// `PendingRun` instead of spawning. Explicit-run paths (the user
    /// triggering a task via `don run <name>` or `--all-pending`) bypass
    /// this gate by calling [`spawn_task_rerun`] directly.
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

        // If the task has opted out of auto-reruns, mark it pending and don't spawn.
        // The user will need to trigger a manual rerun when they're ready.
        if !task_cfg.auto_run {
            self.set_task_state(name, TaskItemState::PendingRun);
            self.output_manager
                .service_event(name, "files changed (pending — auto_run = false)");
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
            if cur != Some(TaskItemState::Skipped) && cur != Some(TaskItemState::PendingRun) {
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
            self.set_task_state(name, TaskItemState::Completed);
            self.unblock_dependency_failed_items();
            let msg = if timing.is_empty() {
                "complete".to_string()
            } else {
                format!("complete ({timing})")
            };
            self.output_manager.service_event(name, &msg);
        } else {
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

    /// Initiate graceful shutdown of all services.
    async fn initiate_shutdown(&mut self) {
        let _ = self.event_tx.send(RunnerEvent::ShutdownStarted);
        let _ = self.shutdown_flag_tx.send(true);
        self.output_manager
            .lifecycle_event("shutting down gracefully... (Ctrl+C again to force)");

        // Abort the detached batch-build task and await its termination so
        // it can't keep any `LifecycleEmitter`/`SinkHandle` clones alive
        // past shutdown. The `Child` inside has `kill_on_drop(true)`, so
        // dropping the aborted future SIGKILLs the bazel/turbo client;
        // awaiting the JoinHandle guarantees the drop has actually run
        // before we continue. A 5s timeout guards against the pathological
        // case where the inner reader tasks don't drop promptly — we'd
        // rather continue shutdown than wedge on a stuck bazel pipe.
        if let Some(guard) = self.batch_build_handle.take()
            && let Some(handle) = guard.into_inner()
        {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }

        if let Some(guard) = self.rebuild_batch_handle.take()
            && let Some(handle) = guard.into_inner()
        {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }

        if let Some(guard) = self.graph_requery_handle.take()
            && let Some(handle) = guard.into_inner()
        {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }

        let mut service_worker_handles = Vec::new();
        for (name, rs) in &mut self.services {
            if let Some(worker) = rs.start_worker.take() {
                self.output_manager
                    .service_event(name, "start cancelled by shutdown");
                worker.abort();
                service_worker_handles.push(worker);
            }
            if let Some(worker) = rs.rebuild_worker.take() {
                self.output_manager
                    .service_event(name, "rebuild cancelled by shutdown");
                worker.abort();
                service_worker_handles.push(worker);
            }
        }
        for worker in service_worker_handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
        }

        let mut task_worker_handles = Vec::new();
        for (name, rt) in &mut self.tasks {
            if let Some(worker) = rt.run_worker.take() {
                self.output_manager
                    .service_event(name, "run cancelled by shutdown");
                worker.abort();
                task_worker_handles.push(worker);
            }
        }
        for worker in task_worker_handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
        }

        // Same treatment for any in-flight JIT lazy builds. These are
        // spawned when a lazy service's proxy gets its first connection
        // and, until this was tracked, would keep streaming bazel/turbo
        // output long past "shutdown complete".
        let lazy_handles: Vec<tokio::task::JoinHandle<()>> = self
            .lazy_build_handles
            .drain()
            .filter_map(|(_, guard)| guard.into_inner())
            .collect();
        for h in &lazy_handles {
            h.abort();
        }
        for h in lazy_handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), h).await;
        }

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
            let state = self.services.get(name).map(|rs| rs.state());
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
                    .service_event(name, &format!("stopping... ({remaining} remaining)"));
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
                        // Global shutdown — no subsequent restart, so the
                        // pgroup-empty poll adds latency without benefit.
                        let _ = stop_service(handle, shutdown_config.as_ref(), force, false).await;
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
                    let names: Vec<String> = stopping_pgids
                        .iter()
                        .map(|(name, pgid)| {
                            let _ = nix::sys::signal::killpg(
                                nix::unistd::Pid::from_raw(*pgid),
                                nix::sys::signal::Signal::SIGKILL,
                            );
                            name.clone()
                        })
                        .collect();
                    for name in names {
                        self.set_service_state(&name, ServiceState::Stopped);
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
                        self.set_service_state(&name, ServiceState::Stopped);
                        remaining -= 1;
                        self.output_manager
                            .service_event(&name, &format!("stopped ({remaining} remaining)"));
                    }
                    Ok(Some(Err(_))) => {
                        remaining = remaining.saturating_sub(1);
                    }
                    Ok(None) => break,  // All tasks done.
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
                if running_task_pgids.len() == 1 {
                    ""
                } else {
                    "s"
                }
            ));
            for (name, pgid) in &running_task_pgids {
                if let Err(e) = nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(*pgid),
                    nix::sys::signal::Signal::SIGKILL,
                ) {
                    // ESRCH = already dead, which is fine.
                    if e != nix::Error::ESRCH {
                        self.output_manager.service_error_event(
                            name,
                            &format!("failed to kill task pgid {pgid}: {e}"),
                        );
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
            if let Some(worker) = rs.control_worker.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
            }
            if let Some(worker) = rs.start_worker.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
            }
            if let Some(worker) = rs.rebuild_worker.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
            }
            rs.handle = None;
            rs.attach_lock = None;
            rs.attach_waiter = None;
            rs.control_reply = None;
            rs.stop_action = ServiceStopAction::None;
        }
        for (_, rt) in self.tasks.iter_mut() {
            if let Some(worker) = rt.run_worker.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
            }
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
                let cmd = resolved.run_cmd().map(|r| {
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
                    proxy: resolved
                        .proxy
                        .iter()
                        .map(|p| match &p.mode {
                            crate::config::ProxyMode::Env(name) => {
                                format!("{} (env={name})", p.listen)
                            }
                            crate::config::ProxyMode::Listenfd => {
                                format!("{} (listenfd)", p.listen)
                            }
                            crate::config::ProxyMode::Forward(target) => {
                                format!("{} → {target}", p.listen)
                            }
                        })
                        .collect(),
                    bazel_target: resolved.bazel_config().map(|b| b.target.clone()),
                    turbo_task: resolved.turbo_config().map(|t| t.task.clone()),
                    ready,
                    cmd,
                })
            } else {
                None
            };
            statuses.push(ItemStatus::Service {
                name: name.clone(),
                state: rs.state(),
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
                state: rt.state(),
                verbose: verbose_info,
            });
        }
        statuses
    }
}

/// Render an unexpected-exit lifecycle message from the reaped status.
/// Reports the exit code for normal exits, the signal number (and core
/// dump flag) for signal-killed processes, and a plain "no status" line
/// when the wait failed.
fn format_unexpected_exit(status: Option<std::process::ExitStatus>) -> String {
    use std::os::unix::process::ExitStatusExt;
    match status {
        Some(s) => {
            if let Some(code) = s.code() {
                format!("exited unexpectedly with status {code}")
            } else if let Some(sig) = s.signal() {
                let core = if s.core_dumped() {
                    " (core dumped)"
                } else {
                    ""
                };
                format!("exited unexpectedly: killed by signal {sig}{core}")
            } else {
                "exited unexpectedly (no status available)".to_string()
            }
        }
        None => "exited unexpectedly (could not reap exit status)".to_string(),
    }
}

/// Compute the wait before the next auto-restart of an Unhealthy service.
/// Doubles each attempt (1, 2, 4, 8, 16, 32, 60, 60, ...) up to a 60s cap.
/// `attempt` is 1-based — the first restart waits 1s.
fn unhealthy_restart_backoff_secs(attempt: u32) -> u64 {
    let exp = attempt.saturating_sub(1).min(6);
    (1u64 << exp).min(60)
}

/// Long-lived per-service health monitor. Spawned once a service reaches
/// `Ready` when `ready.monitor = true`. Polls `run_one_check` at
/// `monitor_interval` and reports state transitions back to the runner via
/// `RunnerCommand::ServiceHealthChanged`. Exits when the cancel oneshot
/// fires (sent or dropped) — typically on stop/restart/process exit.
async fn run_health_monitor(
    name: String,
    ready: crate::config::ReadyCheck,
    cmd_tx: mpsc::Sender<RunnerCommand>,
    mut cancel: oneshot::Receiver<()>,
) {
    let interval_str = ready.monitor_interval.as_str();
    // Both values were validated at config load; fall back to 1s if a
    // bad value somehow reaches here — panicking in this detached task
    // would silently orphan the monitor.
    let interval = crate::duration::parse_duration(interval_str)
        .unwrap_or_else(|_| std::time::Duration::from_secs(1));
    let unhealthy_after = ready.unhealthy_after.max(1);
    let mut consecutive_failures: u32 = 0;
    let mut currently_unhealthy = false;
    loop {
        tokio::select! {
            _ = &mut cancel => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let probe = service::run_one_check(&ready).await;
        match probe {
            Ok(()) => {
                consecutive_failures = 0;
                if currently_unhealthy {
                    currently_unhealthy = false;
                    let _ = cmd_tx
                        .send(RunnerCommand::ServiceHealthChanged {
                            name: name.clone(),
                            healthy: true,
                        })
                        .await;
                }
            }
            Err(_) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                if !currently_unhealthy && consecutive_failures >= unhealthy_after {
                    currently_unhealthy = true;
                    let _ = cmd_tx
                        .send(RunnerCommand::ServiceHealthChanged {
                            name: name.clone(),
                            healthy: false,
                        })
                        .await;
                }
            }
        }
    }
}

/// Snapshot of a service or task that needs a batch build. Owned — the
/// detached batch-build task runs entirely off this and never touches the
/// live [`Runner`] state.
#[derive(Clone)]
pub(crate) struct BatchBuildItem {
    pub(crate) name: String,
    pub(crate) kind: NodeKind,
    pub(crate) bazel: Option<crate::config::BazelConfig>,
    pub(crate) turbo: Option<crate::config::TurboConfig>,
    /// Absolute directory where the build tool should be invoked.
    pub(crate) working_dir: PathBuf,
    /// Ignore patterns to carry through to the watch manager.
    pub(crate) ignore: Vec<String>,
}

/// Everything the detached batch-build task produces. Applied to runner
/// state in the main loop when [`RunnerCommand::BatchBuildComplete`]
/// arrives — keeps all `&mut self` mutations on the runner task.
pub(crate) struct BatchBuildOutcome {
    /// Per-item resolved watch paths — applied to `resolved_watch_paths` on
    /// the runtime service/task entry.
    pub(crate) resolved_watches: Vec<(String, NodeKind, Vec<String>)>,
    /// Non-fatal warnings (query failures, binary-path cquery failures).
    pub(crate) warnings: Vec<String>,
    /// Names whose batch build succeeded — transition `Building` → `Pending`.
    pub(crate) succeeded: HashSet<String>,
    /// `(name, message)` for items whose batch build failed — transition
    /// `Building` → `Failed` and surface the message as an error event.
    pub(crate) failed: Vec<(String, String)>,
    /// Absolute binary paths from `bazel cquery --output=files`, keyed by
    /// service name. Only populated for bazel services whose build succeeded.
    pub(crate) binary_paths: HashMap<String, String>,
    /// Items whose source and/or build-graph inputs changed while the batch
    /// query/build/cquery pipeline was in flight and need another cycle.
    replay_items: Vec<BatchBuildReplayItem>,
}

/// Run the full startup-phase batch build: watch resolution → batch build
/// → bazel binary-path cquery. Pure off-task function that takes owned
/// inputs and returns an [`BatchBuildOutcome`] the main loop applies.
///
/// Sends [`crate::watch::WatchUpdate`]s directly to the watch manager as
/// they resolve so file watching is live before the builds complete.
async fn run_batch_build_chain(
    items: Vec<BatchBuildItem>,
    base_dir: PathBuf,
    emitter: crate::output::LifecycleEmitter,
    watch_update_tx: Option<mpsc::Sender<crate::watch::WatchUpdate>>,
) -> BatchBuildOutcome {
    let scan_since = SystemTime::now();
    let mut outcome = BatchBuildOutcome {
        resolved_watches: Vec::new(),
        warnings: Vec::new(),
        succeeded: HashSet::new(),
        failed: Vec::new(),
        binary_paths: HashMap::new(),
        replay_items: Vec::new(),
    };
    if items.is_empty() {
        return outcome;
    }

    // Step 1: resolve watch paths with ONE query per tool.
    //
    // Previously we ran N parallel `bazel query` / `turbo run --dry-run`
    // subprocesses, one per item. Bazel's server has a workspace-wide
    // analysis lock, so those parallel queries mostly serialised inside
    // bazel anyway — and each process startup cost stacked. Now we issue
    // a single `deps(T1 + ... + Tn) --output=xml` and DFS-walk per target
    // client-side to keep accurate per-service attribution.
    //
    // Turbo still uses the union-attribution shape — upgrading it to
    // per-filter attribution is a separate effort.

    // Partition items by tool. Each item keeps its own ignore patterns
    // (configured per service/task) but gets the same tier-1/tier-2 globs.
    let bazel_items: Vec<&BatchBuildItem> = items.iter().filter(|i| i.bazel.is_some()).collect();
    let turbo_items: Vec<&BatchBuildItem> = items.iter().filter(|i| i.turbo.is_some()).collect();

    let mut bazel_info_by_target: HashMap<String, crate::build_tool::ResolvedBuildInfo> =
        HashMap::new();
    let mut turbo_info: Option<crate::build_tool::ResolvedBuildInfo> = None;
    let mut resolved_info_by_item: HashMap<String, crate::build_tool::ResolvedBuildInfo> =
        HashMap::new();

    if !bazel_items.is_empty() {
        let targets: Vec<String> = bazel_items
            .iter()
            .filter_map(|i| i.bazel.as_ref().map(|b| b.target.clone()))
            .collect();
        // Unified query runs at the workspace root — all bazel items share a
        // single workspace in practice, so the first item's working_dir is
        // the workspace root.
        let working_dir = bazel_items[0].working_dir.clone();
        let resolver = crate::build_tool::bazel::BazelResolver::new().with_emitter(emitter.clone());
        match resolver.resolve_per_target(&targets, &working_dir).await {
            Ok(info_by_target) => {
                let unique: HashSet<&String> = info_by_target
                    .values()
                    .flat_map(|i| i.watch_paths.iter())
                    .collect();
                emitter.bazel_event(&format!(
                    "resolved {} unique watch path{} across {} target{}",
                    unique.len(),
                    if unique.len() == 1 { "" } else { "s" },
                    targets.len(),
                    if targets.len() == 1 { "" } else { "s" },
                ));
                bazel_info_by_target = info_by_target;
            }
            Err(e) => {
                outcome.warnings.push(format!("bazel query failed: {e}"));
            }
        }
    }

    if !turbo_items.is_empty() {
        let filters: Vec<String> = turbo_items
            .iter()
            .filter_map(|i| i.turbo.as_ref().and_then(|t| t.filter.clone()))
            .collect();
        // Pick the first turbo item's task as the command's task name. All
        // turbo items in a startup batch must share a task (this is what
        // `turbo run <task>` takes) — if configs differ, we'd need per-task
        // grouping. For now, emit a warning on mismatch and use the first.
        let task = turbo_items[0]
            .turbo
            .as_ref()
            .map(|t| t.task.clone())
            .unwrap_or_default();
        for i in &turbo_items[1..] {
            if let Some(t) = &i.turbo
                && t.task != task
            {
                outcome.warnings.push(format!(
                    "{}: turbo.task '{}' differs from batch task '{}' — using batch task",
                    i.name, t.task, task
                ));
            }
        }
        let working_dir = turbo_items[0].working_dir.clone();
        let resolver = crate::build_tool::turbo::TurboResolver::new(&task, None);
        match resolver.resolve_union(&filters, &working_dir).await {
            Ok(info) => {
                emitter.turbo_event(&format!(
                    "resolved {} watch path{} across {} filter{}",
                    info.watch_paths.len(),
                    if info.watch_paths.len() == 1 { "" } else { "s" },
                    filters.len(),
                    if filters.len() == 1 { "" } else { "s" },
                ));
                turbo_info = Some(info);
            }
            Err(e) => {
                outcome.warnings.push(format!("turbo query failed: {e}"));
            }
        }
    }

    // Attribute resolved watch paths to each item. Bazel items get their
    // own per-target result (computed by DFS from the unified XML graph);
    // turbo items still share the union. Each item emits its own
    // `WatchUpdate` so the watch manager keeps a per-service/task
    // `WatchedItem` entry keyed by name. Directories in the watcher dedup
    // via `registered_dirs`, so the actual inotify cost is paid once per
    // directory regardless of how many items claim it.
    for item in &items {
        let info = if let Some(ref bazel) = item.bazel {
            bazel_info_by_target.get(&bazel.target)
        } else if item.turbo.is_some() {
            turbo_info.as_ref()
        } else {
            None
        };
        let Some(info) = info else {
            continue; // query failed for this tool — warning already pushed
        };
        resolved_info_by_item.insert(item.name.clone(), info.clone());

        if let Some(ref tx) = watch_update_tx {
            let watch_kind = match item.kind {
                NodeKind::Service => crate::watch::WatchItemKind::Service,
                NodeKind::Task => crate::watch::WatchItemKind::Task,
            };
            send_watch_update(
                tx,
                item.name.clone(),
                watch_kind,
                info.watch_paths.clone(),
                item.ignore.clone(),
                base_dir.clone(),
            )
            .await;
            if !info.graph_definition_globs.is_empty() {
                send_watch_update(
                    tx,
                    format!("{}__graph", item.name),
                    crate::watch::WatchItemKind::BuildGraph,
                    info.graph_definition_globs.clone(),
                    Vec::new(),
                    base_dir.clone(),
                )
                .await;
            }
        }
        outcome
            .resolved_watches
            .push((item.name.clone(), item.kind, info.watch_paths.clone()));
    }

    // Step 2: batch builds, grouped by tool. Bazel and Turbo run concurrently.
    let mut bazel_items: Vec<(BatchBuildItem, String)> = Vec::new();
    let mut turbo_by_task: HashMap<String, Vec<(BatchBuildItem, String)>> = HashMap::new();

    for item in &items {
        if let Some(ref bazel) = item.bazel {
            bazel_items.push((item.clone(), bazel.target.clone()));
        } else if let Some(ref turbo) = item.turbo {
            let build_task = turbo
                .build_task
                .clone()
                .unwrap_or_else(|| "build".to_string());
            if !build_task.is_empty() {
                if let Some(ref filter) = turbo.filter {
                    turbo_by_task
                        .entry(build_task)
                        .or_default()
                        .push((item.clone(), filter.clone()));
                } else {
                    outcome.warnings.push(format!(
                        "{}: turbo.filter is required for batch builds — skipping batch build",
                        item.name
                    ));
                }
            }
        }
    }

    let mut build_set: JoinSet<crate::build_tool::BatchBuildResult> = JoinSet::new();

    if !bazel_items.is_empty() {
        let targets: Vec<String> = bazel_items.iter().map(|(_, t)| t.clone()).collect();
        let target_to_names: HashMap<String, Vec<String>> = {
            let mut m: HashMap<String, Vec<String>> = HashMap::new();
            for (item, target) in &bazel_items {
                m.entry(target.clone()).or_default().push(item.name.clone());
            }
            m
        };
        let count = targets.len();
        let base = base_dir.clone();
        let em = emitter.clone();
        let em_spawn = emitter.clone();
        emitter.bazel_event(&format!(
            "building {count} target{}...",
            if count == 1 { "" } else { "s" }
        ));
        build_set.spawn(async move {
            let resolver = crate::build_tool::bazel::BazelResolver::new();
            let result = resolver
                .build_targets(
                    &targets,
                    &base,
                    move |line| {
                        em.bazel_event(line);
                    },
                    Some(&em_spawn),
                )
                .await;
            match result {
                Ok(batch) => {
                    let mut succeeded: Vec<String> = Vec::new();
                    for target in &batch.succeeded {
                        if let Some(names) = target_to_names.get(target) {
                            succeeded.extend(names.clone());
                        }
                    }
                    let mut failed: Vec<(String, String)> = Vec::new();
                    for (target, msg) in &batch.failed {
                        if let Some(names) = target_to_names.get(target) {
                            for n in names {
                                failed.push((n.clone(), msg.clone()));
                            }
                        }
                    }
                    crate::build_tool::BatchBuildResult { succeeded, failed }
                }
                // Resolver errored before producing per-target results
                // (e.g. bazel client missing, I/O failure). Mark every item
                // in this batch as failed so the runner doesn't leave them
                // sitting in `Building` forever.
                Err(e) => {
                    let msg = format!("bazel build error: {e}");
                    let failed: Vec<(String, String)> = target_to_names
                        .values()
                        .flatten()
                        .map(|n| (n.clone(), msg.clone()))
                        .collect();
                    crate::build_tool::BatchBuildResult {
                        succeeded: Vec::new(),
                        failed,
                    }
                }
            }
        });
    }

    for (build_task, items_for_task) in turbo_by_task {
        let filters: Vec<String> = items_for_task.iter().map(|(_, f)| f.clone()).collect();
        let filter_to_names: HashMap<String, Vec<String>> = {
            let mut m: HashMap<String, Vec<String>> = HashMap::new();
            for (item, filter) in &items_for_task {
                m.entry(filter.clone()).or_default().push(item.name.clone());
            }
            m
        };
        let count = filters.len();
        let base = base_dir.clone();
        let em = emitter.clone();
        let em_spawn = emitter.clone();
        let bt = build_task.clone();
        emitter.turbo_event(&format!(
            "running '{build_task}' for {count} package{}...",
            if count == 1 { "" } else { "s" }
        ));
        build_set.spawn(async move {
            let resolver = crate::build_tool::turbo::TurboResolver::new(&bt, None);
            let result = resolver
                .build_packages(
                    &bt,
                    &filters,
                    &base,
                    move |line| {
                        em.turbo_event(line);
                    },
                    Some(&em_spawn),
                )
                .await;
            match result {
                Ok(batch) => {
                    let mut succeeded = Vec::new();
                    for filter in &batch.succeeded {
                        if let Some(names) = filter_to_names.get(filter) {
                            succeeded.extend(names.clone());
                        }
                    }
                    let mut failed = Vec::new();
                    for (filter, msg) in &batch.failed {
                        if let Some(names) = filter_to_names.get(filter) {
                            for n in names {
                                failed.push((n.clone(), msg.clone()));
                            }
                        }
                    }
                    crate::build_tool::BatchBuildResult { succeeded, failed }
                }
                // See bazel branch above — convert resolver errors to
                // per-item failures so services don't get stuck in `Building`.
                Err(e) => {
                    let msg = format!("turbo build error: {e}");
                    let failed: Vec<(String, String)> = filter_to_names
                        .values()
                        .flatten()
                        .map(|n| (n.clone(), msg.clone()))
                        .collect();
                    crate::build_tool::BatchBuildResult {
                        succeeded: Vec::new(),
                        failed,
                    }
                }
            }
        });
    }

    while let Some(result) = build_set.join_next().await {
        match result {
            Ok(batch) => {
                for name in batch.succeeded {
                    outcome.succeeded.insert(name);
                }
                for (name, msg) in batch.failed {
                    outcome.failed.push((name, msg));
                }
            }
            Err(e) => outcome
                .warnings
                .push(format!("batch build task panicked: {e}")),
        }
    }

    let built_count = outcome.succeeded.len();
    if built_count > 0 {
        emitter.lifecycle_event(&format!(
            "batch build complete: {built_count} item{} built",
            if built_count == 1 { "" } else { "s" }
        ));
    }

    // Step 3: ONE `bazel cquery` to resolve every succeeded bazel service's
    // built-binary path. Lets the runner spawn the artifact directly instead
    // of via `bazel run`. Tasks and turbo services don't need this.
    let bazel_services_to_resolve: Vec<&BatchBuildItem> = items
        .iter()
        .filter(|i| {
            i.kind == NodeKind::Service && i.bazel.is_some() && outcome.succeeded.contains(&i.name)
        })
        .collect();

    if !bazel_services_to_resolve.is_empty() {
        let targets: Vec<String> = bazel_services_to_resolve
            .iter()
            .filter_map(|i| i.bazel.as_ref().map(|b| b.target.clone()))
            .collect();
        // All bazel items share a workspace in practice — same rationale as
        // the watch-resolution step above.
        let working_dir = bazel_services_to_resolve[0].working_dir.clone();
        let resolver = crate::build_tool::bazel::BazelResolver::new().with_emitter(emitter.clone());
        match resolver.resolve_binary_paths(&targets, &working_dir).await {
            Ok(paths_by_label) => {
                for item in &bazel_services_to_resolve {
                    let Some(ref bazel) = item.bazel else {
                        continue;
                    };
                    match paths_by_label.get(&bazel.target) {
                        Some(rel_path) => {
                            let abs_path = item.working_dir.join(rel_path);
                            let path_str = abs_path.to_string_lossy().to_string();
                            emitter
                                .service_event(&item.name, &format!("resolved binary {rel_path}"));
                            outcome.binary_paths.insert(item.name.clone(), path_str);
                        }
                        None => {
                            outcome.warnings.push(format!(
                                "{}: no binary output for {} — falling back to bazel run",
                                item.name, bazel.target
                            ));
                        }
                    }
                }
            }
            Err(e) => {
                outcome.warnings.push(format!(
                    "bazel cquery for binary paths failed: {e} — falling back to bazel run for {} service{}",
                    bazel_services_to_resolve.len(),
                    if bazel_services_to_resolve.len() == 1 { "" } else { "s" },
                ));
            }
        }
    }

    for item in &items {
        if !outcome.succeeded.contains(&item.name) {
            continue;
        }
        let Some(info) = resolved_info_by_item.get(&item.name) else {
            continue;
        };
        let source_changed =
            any_glob_path_changed_since(&base_dir, &info.watch_paths, &item.ignore, scan_since);
        let graph_changed =
            any_glob_path_changed_since(&base_dir, &info.graph_definition_globs, &[], scan_since);
        if source_changed || graph_changed {
            outcome.replay_items.push(BatchBuildReplayItem {
                name: item.name.clone(),
                kind: item.kind,
                source_changed,
                graph_changed,
            });
        }
    }

    outcome
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

/// Current process-level shutdown signal count.
///
/// `0` means no shutdown signal has been seen, `1` means graceful shutdown
/// has been requested, and `2+` means force-exit escalation has been
/// requested. This is intentionally process-global so outer supervisors can
/// make progress even if the runner task wedges.
pub fn signal_count() -> u8 {
    SIGNAL_COUNT.load(Ordering::SeqCst)
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
            RunnerCommand::ServiceHealthChanged { name, healthy } => {
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
            RunnerCommand::ServiceHealthChanged { name, healthy } => {
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
                auto_run: true,
                download: None,
                bazel: None,
                turbo: None,
                params: Vec::new(),
                hidden: false,
            },
            TaskItemState::Pending,
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
