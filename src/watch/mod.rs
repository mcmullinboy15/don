//! File watching with per-service debounce and change-during-build handling.
//!
//! The [`WatchManager`] sets up `notify` watchers for services and tasks with
//! `watch` patterns, debounces events per-service, and sends [`RunnerCommand::Rebuild`]
//! or [`RunnerCommand::TaskRerun`] to the runner when a rebuild cycle should start.
//!
//! Each watched service has its own state machine:
//!
//! ```text
//! Idle → Debouncing → Rebuilding → Idle
//!                         ↓ (stale)
//!                      Rebuilding (another cycle)
//! ```
//!
//! The watch module subscribes to [`RunnerEvent::RebuildComplete`] to know when
//! a cycle finishes, and checks the `stale` flag to decide whether to immediately
//! start another cycle.

use crate::config::{Config, Platform};
use crate::duration::parse_duration;
use crate::output::LifecycleEmitter;
use crate::runner::{RunnerCommand, RunnerEvent};
use glob::Pattern;
use notify::{EventKind, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::Instant;

/// Default debounce window when none is configured.
const DEFAULT_DEBOUNCE: Duration = Duration::from_millis(200);
/// Synthetic watch item used for workspace-level build graph files.
pub(crate) const WORKSPACE_GRAPH_ITEM_NAME: &str = "__workspace_graph__";

/// Errors from the watch module.
#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error("notify error: {0}")]
    Notify(#[from] notify::Error),
    #[error("invalid debounce duration: {0}")]
    Duration(#[from] crate::duration::DurationError),
    #[error("failed to create watch directory {}: {}", .0.display(), .1)]
    Io(PathBuf, std::io::Error),
}

/// Per-item state machine for file-watch-triggered rebuilds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchState {
    /// No pending changes. Watching for events.
    Idle,
    /// Events received, waiting for debounce window to expire.
    Debouncing,
    /// A rebuild/rerun cycle is in progress.
    Rebuilding,
}

/// What command to send when this item's debounce timer fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatchItemKind {
    /// Send `RunnerCommand::Rebuild { name }`.
    Service,
    /// Send `RunnerCommand::TaskRerun { name }`.
    Task,
    /// Send `RunnerCommand::BuildGraphChanged { name }` — tier-1 watch for
    /// build tool definition files (BUILD, package.json, etc.). No rebuild cycle;
    /// the runner re-queries the build tool and updates tier-2 watch patterns.
    BuildGraph,
}

/// Per-item watch tracking.
struct WatchedItem {
    state: WatchState,
    debounce_duration: Duration,
    /// When in Debouncing state, the deadline at which to fire.
    debounce_deadline: Option<Instant>,
    /// True when events arrived during a rebuild — triggers another cycle on completion.
    stale: bool,
    /// What kind of item this is — determines the command to send.
    kind: WatchItemKind,
    /// Glob patterns for matching file events.
    patterns: Vec<Pattern>,
    /// Glob patterns for ignoring file events (checked before watch patterns).
    ignore_patterns: Vec<Pattern>,
    /// Last diagnostic associated with this item's watch registration or state.
    last_error: Option<String>,
}

/// An update to the watch patterns for a specific item.
///
/// Sent from the runner to the watch manager after a build tool re-query
/// completes, containing the new tier-2 watch patterns.
pub(crate) struct WatchUpdate {
    /// The service or task name (matches the key in `items`).
    pub name: String,
    /// What kind of item this is (Service or Task). Used when creating
    /// a new watch item that didn't exist during initial setup.
    pub kind: WatchItemKind,
    /// New glob patterns to watch (replaces existing patterns).
    pub patterns: Vec<String>,
    /// New ignore patterns (replaces existing ignore patterns).
    pub ignore_patterns: Vec<String>,
    /// Base directory to resolve patterns against.
    pub base_dir: PathBuf,
    /// Optional completion signal sent once the watch manager has applied the
    /// update and registered any needed directories.
    pub applied_tx: Option<oneshot::Sender<()>>,
}

#[derive(Debug, Clone)]
pub(crate) struct WatchItemSnapshot {
    pub kind: &'static str,
    pub state: &'static str,
    pub stale: bool,
    pub debounce_ms: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct WatchSnapshot {
    pub items: HashMap<String, WatchItemSnapshot>,
    pub notify_error_count: u64,
    pub runner_event_lag_count: u64,
    pub last_notify_error: Option<String>,
}

pub(crate) struct WatchQuery {
    pub reply: oneshot::Sender<WatchSnapshot>,
}

/// Manages file watchers for all services and tasks with watch patterns.
///
/// Runs as a background tokio task, communicating with the runner via channels.
pub(crate) struct WatchManager {
    /// The notify watcher handle — kept alive to maintain watches.
    /// Named (not `_watcher`) so we can add new watch directories at runtime.
    watcher: Option<NotifyBackend>,
    /// Sender captured by the notify callback. Kept so deferred watch
    /// registration can allocate the backend only when a real directory exists.
    notify_tx: mpsc::UnboundedSender<notify::Result<notify::Event>>,
    /// Channel receiving raw notify events.
    event_rx: mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
    /// Per-item (service or task) state.
    items: HashMap<String, WatchedItem>,
    /// Sender to the runner's command channel.
    cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
    /// Receiver for runner events (rebuild/rerun completion).
    runner_events: broadcast::Receiver<RunnerEvent>,
    /// Receiver for watch pattern updates from the runner (build tool re-queries).
    update_rx: mpsc::UnboundedReceiver<WatchUpdate>,
    /// Receiver for debug/status queries from the runner.
    query_rx: mpsc::Receiver<WatchQuery>,
    /// Directories already registered with the watcher, keyed by path with
    /// the mode the watch was registered under.
    ///
    /// The mode matters for coverage checks: a NonRecursive watch at
    /// `redo/server` sees direct-child events only; it does NOT cover a
    /// subsequent Recursive request for the same path. Treating it as
    /// coverage causes the Recursive registration to be silently skipped,
    /// and nested files never trigger events.
    registered_dirs: HashMap<PathBuf, RecursiveMode>,
    /// Emitter for `[don]` verbose-mode diagnostics.
    emitter: LifecycleEmitter,
    /// Count of notify backend errors seen since startup.
    notify_error_count: u64,
    /// Count of broadcast lag incidents while consuming runner events.
    runner_event_lag_count: u64,
    /// Most recent notify backend error.
    last_notify_error: Option<String>,
}

enum NotifyBackend {
    Native(RecommendedWatcher),
    Poll(PollWatcher),
}

impl NotifyBackend {
    fn watch(&mut self, path: &Path, mode: RecursiveMode) -> notify::Result<()> {
        match self {
            Self::Native(watcher) => watcher.watch(path, mode),
            Self::Poll(watcher) => watcher.watch(path, mode),
        }
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        match self {
            Self::Native(watcher) => watcher.unwatch(path),
            Self::Poll(watcher) => watcher.unwatch(path),
        }
    }
}

impl WatchManager {
    /// Create a new watch manager from the config.
    ///
    /// Sets up notify watchers for all services and tasks with `watch` patterns.
    /// Creates missing watch directories so we get precise inotify coverage.
    ///
    /// Returns `(Self, warnings)` where warnings are non-fatal issues like
    /// invalid glob patterns (which should have been caught by validation).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: &Config,
        platform: Platform,
        base_dir: &Path,
        cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
        runner_events: broadcast::Receiver<RunnerEvent>,
        update_rx: mpsc::UnboundedReceiver<WatchUpdate>,
        query_rx: mpsc::Receiver<WatchQuery>,
        emitter: LifecycleEmitter,
    ) -> Result<(Self, Vec<String>), WatchError> {
        let mut warnings: Vec<String> = Vec::new();
        let (notify_tx, event_rx) = mpsc::unbounded_channel();

        // `follow_symlinks(false)` is load-bearing: a bazel workspace root
        // has `bazel-*` convenience symlinks into the user-wide bazel cache
        // (millions of generated files, thousands of external repos).
        // Without this, any `RecursiveMode::Recursive` registration at or
        // above the root walks the entire cache and blows through
        // `fs.inotify.max_user_watches` while blocking for minutes.
        let mut watcher = None;

        // Canonicalize base_dir so glob patterns are absolute and match the
        // absolute paths that notify reports in events. Without this, a base_dir
        // of `.` produces patterns like `./definitions/**/*.sql` that don't match
        // the absolute paths notify returns.
        let base_dir = std::fs::canonicalize(base_dir)
            .map_err(|e| WatchError::Io(base_dir.to_path_buf(), e))?;
        let base_dir = base_dir.as_path();
        let global_ignore_patterns: Vec<String> = config
            .watch_ignore
            .iter()
            .map(|pattern| {
                resolve_pattern(base_dir, pattern)
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();

        let mut items: HashMap<String, WatchedItem> = HashMap::new();
        // Track which directories we've already registered, with the mode we
        // registered each under. See `WatchManager::registered_dirs` for why
        // the mode matters.
        let mut registered_dirs: HashMap<PathBuf, RecursiveMode> = HashMap::new();

        // Process services.
        for (name, svc) in &config.services {
            let resolved = svc.resolve(platform);

            // Skip services that handle their own hot-reloading.
            if !resolved.reload {
                continue;
            }

            // Use configured watch patterns, or inject preset defaults.
            let watch_patterns: Vec<String> = if !resolved.watch.is_empty() {
                resolved.watch.clone()
            } else if resolved.rust_config().is_some() {
                vec![
                    "src/**/*.rs".to_string(),
                    "Cargo.toml".to_string(),
                    "Cargo.lock".to_string(),
                ]
            } else if resolved.go_config().is_some() {
                vec![
                    "**/*.go".to_string(),
                    "go.mod".to_string(),
                    "go.sum".to_string(),
                ]
            } else {
                // Docker and custom services require explicit watch config.
                Vec::new()
            };
            if watch_patterns.is_empty() {
                continue;
            }

            // Resolve svc_dir relative to the (canonical) base_dir so patterns
            // are absolute and can match notify's absolute event paths.
            // Canonicalize to eliminate `./` components (e.g. dir = "./app"
            // joined with base_dir would produce `/foo/./app` which won't
            // match notify's canonical event paths).
            let svc_dir = match resolved.dir.as_deref() {
                Some(d) => {
                    let joined = base_dir.join(d);
                    std::fs::canonicalize(&joined).unwrap_or(joined)
                }
                None => base_dir.to_path_buf(),
            };

            let debounce = match &resolved.debounce {
                Some(d) => parse_duration(d)?,
                None => DEFAULT_DEBOUNCE,
            };

            let mut compiled_patterns = Vec::new();
            for pattern_str in &watch_patterns {
                let full_pattern = resolve_pattern(&svc_dir, pattern_str);
                match Pattern::new(&full_pattern.to_string_lossy()) {
                    Ok(pat) => compiled_patterns.push(pat),
                    Err(e) => {
                        warnings.push(format!(
                            "{name}: invalid watch pattern '{pattern_str}': {e}"
                        ));
                        continue;
                    }
                }

                // Determine which directory to watch. Create it if it
                // doesn't exist so we get precise inotify coverage
                // instead of watching a broad ancestor.
                let watch_dir = glob_base_dir(&full_pattern);
                std::fs::create_dir_all(&watch_dir)
                    .map_err(|e| WatchError::Io(watch_dir.clone(), e))?;

                if !is_covered(&watch_dir, RecursiveMode::Recursive, &registered_dirs) {
                    ensure_notify_watcher(&mut watcher, &notify_tx)?
                        .watch(&watch_dir, RecursiveMode::Recursive)?;
                    registered_dirs.insert(watch_dir, RecursiveMode::Recursive);
                }
            }

            let mut compiled_ignore = Vec::new();
            let ignore_patterns: Vec<String> = resolved
                .ignore
                .iter()
                .cloned()
                .chain(global_ignore_patterns.iter().cloned())
                .collect();
            for pattern_str in &ignore_patterns {
                let full_pattern = resolve_pattern(&svc_dir, pattern_str);
                match Pattern::new(&full_pattern.to_string_lossy()) {
                    Ok(pat) => compiled_ignore.push(pat),
                    Err(e) => {
                        warnings.push(format!(
                            "{name}: invalid ignore pattern '{pattern_str}': {e}"
                        ));
                    }
                }
            }

            items.insert(
                name.clone(),
                WatchedItem {
                    state: WatchState::Idle,
                    debounce_duration: debounce,
                    debounce_deadline: None,
                    stale: false,
                    kind: WatchItemKind::Service,
                    patterns: compiled_patterns,
                    ignore_patterns: compiled_ignore,
                    last_error: None,
                },
            );
        }

        // Process tasks.
        for (name, task) in &config.tasks {
            if task.watch.is_empty() {
                continue;
            }

            let task_dir = match task.dir.as_deref() {
                Some(d) => {
                    let joined = base_dir.join(d);
                    std::fs::canonicalize(&joined).unwrap_or(joined)
                }
                None => base_dir.to_path_buf(),
            };

            let mut compiled_patterns = Vec::new();
            for pattern_str in &task.watch {
                let full_pattern = resolve_pattern(&task_dir, pattern_str);
                match Pattern::new(&full_pattern.to_string_lossy()) {
                    Ok(pat) => compiled_patterns.push(pat),
                    Err(e) => {
                        warnings.push(format!(
                            "{name}: invalid watch pattern '{pattern_str}': {e}"
                        ));
                        continue;
                    }
                }

                let watch_dir = glob_base_dir(&full_pattern);
                std::fs::create_dir_all(&watch_dir)
                    .map_err(|e| WatchError::Io(watch_dir.clone(), e))?;

                if !is_covered(&watch_dir, RecursiveMode::Recursive, &registered_dirs) {
                    ensure_notify_watcher(&mut watcher, &notify_tx)?
                        .watch(&watch_dir, RecursiveMode::Recursive)?;
                    registered_dirs.insert(watch_dir, RecursiveMode::Recursive);
                }
            }

            let mut compiled_ignore = Vec::new();
            let ignore_patterns: Vec<String> = task
                .ignore
                .iter()
                .cloned()
                .chain(global_ignore_patterns.iter().cloned())
                .collect();
            for pattern_str in &ignore_patterns {
                let full_pattern = resolve_pattern(&task_dir, pattern_str);
                match Pattern::new(&full_pattern.to_string_lossy()) {
                    Ok(pat) => compiled_ignore.push(pat),
                    Err(e) => {
                        warnings.push(format!(
                            "{name}: invalid ignore pattern '{pattern_str}': {e}"
                        ));
                    }
                }
            }

            items.insert(
                name.clone(),
                WatchedItem {
                    state: WatchState::Idle,
                    debounce_duration: DEFAULT_DEBOUNCE, // Tasks use default debounce.
                    debounce_deadline: None,
                    stale: false,
                    kind: WatchItemKind::Task,
                    patterns: compiled_patterns,
                    ignore_patterns: compiled_ignore,
                    last_error: None,
                },
            );
        }

        // Register tier-1 build graph watches for workspace-level files.
        //
        // Per-package BUILD / package.json watches are NOT seeded here —
        // they're registered lazily via `WatchUpdate { kind: BuildGraph, .. }`
        // once `run_batch_build_chain` resolves the actual package list from
        // `bazel query` / `turbo run --dry-run`. Seeding them from `**/BUILD`
        // would force a recursive `watcher.watch` on the workspace root,
        // which follows `bazel-*` symlinks into the bazel cache and takes
        // minutes on large monorepos (3,000+ external repos under
        // `execroot/_main/external/`).
        //
        // What IS seeded: a single non-recursive watch on the workspace root
        // for workspace-level files (WORKSPACE, MODULE.bazel, turbo.json,
        // pnpm-workspace.yaml). These change rarely but must trigger a full
        // build-graph re-query.
        {
            let has_bazel = config.services.values().any(|s| {
                let resolved = s.resolve(platform);
                resolved
                    .bazel_config()
                    .is_some_and(|bazel| resolved.reload && bazel.watch)
            }) || config
                .tasks
                .values()
                .any(|t| t.bazel.as_ref().is_some_and(|bazel| bazel.watch));
            let has_turbo = config.services.values().any(|s| {
                let resolved = s.resolve(platform);
                resolved
                    .turbo_config()
                    .is_some_and(|turbo| resolved.reload && turbo.watch)
            }) || config
                .tasks
                .values()
                .any(|t| t.turbo.as_ref().is_some_and(|turbo| turbo.watch));

            if has_bazel || has_turbo {
                let mut root_file_names: Vec<&str> = Vec::new();
                if has_bazel {
                    root_file_names.extend(["WORKSPACE", "WORKSPACE.bazel", "MODULE.bazel"]);
                }
                if has_turbo {
                    root_file_names.extend(["turbo.json", "turbo.jsonc", "pnpm-workspace.yaml"]);
                }

                let mut compiled_patterns = Vec::new();
                for file_name in &root_file_names {
                    let full_pattern = resolve_pattern(base_dir, file_name);
                    if let Ok(pat) = Pattern::new(&full_pattern.to_string_lossy()) {
                        compiled_patterns.push(pat);
                    }
                }
                let compiled_ignore: Vec<Pattern> = global_ignore_patterns
                    .iter()
                    .filter_map(|pattern| Pattern::new(pattern).ok())
                    .collect();

                // Non-recursive watch on the workspace root is enough for
                // these specific filenames. No symlink spelunking.
                if !is_covered(base_dir, RecursiveMode::NonRecursive, &registered_dirs) {
                    match ensure_notify_watcher(&mut watcher, &notify_tx).and_then(|watcher| {
                        watcher
                            .watch(base_dir, RecursiveMode::NonRecursive)
                            .map_err(WatchError::from)
                    }) {
                        Ok(()) => {
                            registered_dirs
                                .insert(base_dir.to_path_buf(), RecursiveMode::NonRecursive);
                        }
                        Err(e) => warnings.push(format!(
                            "workspace watch registration failed for {}: {e}",
                            base_dir.display()
                        )),
                    }
                }

                if !compiled_patterns.is_empty() {
                    items.insert(
                        WORKSPACE_GRAPH_ITEM_NAME.to_string(),
                        WatchedItem {
                            state: WatchState::Idle,
                            debounce_duration: DEFAULT_DEBOUNCE,
                            debounce_deadline: None,
                            stale: false,
                            kind: WatchItemKind::BuildGraph,
                            patterns: compiled_patterns,
                            ignore_patterns: compiled_ignore,
                            last_error: None,
                        },
                    );
                }
            }
        }

        // Verbose setup summary: per-item patterns/ignore/debounce, plus the
        // full list of registered directories. This is the first thing a user
        // hitting "nothing reloaded" will want to see.
        let mut names: Vec<&String> = items.keys().collect();
        names.sort();
        for name in names {
            let Some(item) = items.get(name) else {
                continue;
            };
            let pats: Vec<&str> = item.patterns.iter().map(Pattern::as_str).collect();
            let igs: Vec<&str> = item.ignore_patterns.iter().map(Pattern::as_str).collect();
            emitter.service_debug_event(
                name,
                &format!(
                    "watch: registered kind={:?} debounce={:?} patterns={:?} ignore={:?}",
                    item.kind, item.debounce_duration, pats, igs
                ),
            );
        }
        let mut dirs: Vec<(&PathBuf, &RecursiveMode)> = registered_dirs.iter().collect();
        dirs.sort_by(|a, b| a.0.cmp(b.0));
        for (dir, mode) in &dirs {
            emitter.debug_event(&format!("watch: inotify dir {:?} mode={:?}", dir, mode));
        }
        emitter.debug_event(&format!(
            "watch: setup complete — {} items, {} directories registered",
            items.len(),
            registered_dirs.len()
        ));

        Ok((
            Self {
                watcher,
                notify_tx,
                event_rx,
                items,
                cmd_tx,
                runner_events,
                update_rx,
                query_rx,
                registered_dirs,
                emitter,
                notify_error_count: 0,
                runner_event_lag_count: 0,
                last_notify_error: None,
            },
            warnings,
        ))
    }

    /// Returns true if there are any items being watched.
    pub(crate) fn has_watches(&self) -> bool {
        !self.items.is_empty()
    }

    /// Run the watch event loop until the runner shuts down.
    ///
    /// This consumes the manager and runs until channels close.
    pub(crate) async fn run(mut self) {
        loop {
            let next_deadline = self.nearest_debounce_deadline();

            tokio::select! {
                Some(event_result) = self.event_rx.recv() => {
                    match event_result {
                        Ok(event) => self.handle_notify_event(&event).await,
                        Err(err) => self.record_notify_error(&err.to_string()),
                    }
                }
                _ = sleep_until_or_pending(next_deadline) => {
                    self.fire_debounce_timers().await;
                }
                result = self.runner_events.recv() => {
                    match result {
                        Ok(event) => self.handle_runner_event(&event).await,
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            // Missed n events. If one was a RebuildComplete,
                            // the corresponding WatchedItem is stuck in
                            // `Rebuilding` and will swallow future edits until
                            // the item is re-registered. Surface it loudly.
                            self.runner_event_lag_count =
                                self.runner_event_lag_count.saturating_add(n);
                            self.emitter.lifecycle_event(&format!(
                                "watch: broadcast lag — missed {n} runner events; a service may be stuck in Rebuilding"
                            ));
                        }
                    }
                }
                Some(update) = self.update_rx.recv() => {
                    self.apply_watch_update(update);
                }
                Some(query) = self.query_rx.recv() => {
                    let _ = query.reply.send(self.snapshot());
                }
            }
        }
    }

    /// Apply a watch update from the runner (build tool re-query completed).
    ///
    /// Replaces the watch patterns for the named item and registers any
    /// new watch directories with the notify watcher.
    fn apply_watch_update(&mut self, mut update: WatchUpdate) {
        // Tier-1 BuildGraph updates land on specific filename patterns
        // (`<pkg>/BUILD`, `<pkg>/package.json`) — a non-recursive watch on
        // the package directory is exactly right. Tier-2 Service/Task
        // updates are directory-level globs (`<pkg>/**`), which need
        // recursive watching.
        let mode = match update.kind {
            WatchItemKind::BuildGraph => RecursiveMode::NonRecursive,
            WatchItemKind::Service | WatchItemKind::Task => RecursiveMode::Recursive,
        };

        // Canonicalize the base so the compiled globs are absolute and match
        // the cwd-prefixed absolute paths that notify reports in events.
        // Without this, a runner base_dir of `.` produces patterns like
        // `./auth/jwt/**` that will never match `/abs/cwd/./auth/jwt/foo.ts`.
        // The initial-setup path in `WatchManager::new` already canonicalizes;
        // this keeps the build-tool-resolved update path in sync.
        let base_dir =
            std::fs::canonicalize(&update.base_dir).unwrap_or_else(|_| update.base_dir.clone());

        let mut compiled_patterns = Vec::new();
        for pattern_str in &update.patterns {
            let full_pattern = resolve_pattern(&base_dir, pattern_str);
            if let Ok(pat) = Pattern::new(&full_pattern.to_string_lossy()) {
                compiled_patterns.push(pat);

                // Always register the watch directory for build-tool-resolved
                // patterns. A parent recursive watch may not reliably cover
                // all subdirectories (e.g. when bazel symlinks cause inotify
                // to miss directories during the initial recursive walk).
                let watch_dir = glob_base_dir(&full_pattern);
                if watch_dir.exists() && !is_covered(&watch_dir, mode, &self.registered_dirs) {
                    // Upgrade case: an existing NonRecursive watch at this
                    // exact path doesn't cover a Recursive request. Unwatch
                    // the old one first so the new Recursive watch replaces
                    // it cleanly (notify-rs's inotify backend treats the
                    // same path + different mode as a distinct registration,
                    // so leaving the old one leaks a watch descriptor).
                    if mode == RecursiveMode::Recursive
                        && self.registered_dirs.get(&watch_dir)
                            == Some(&RecursiveMode::NonRecursive)
                    {
                        match ensure_notify_watcher(&mut self.watcher, &self.notify_tx) {
                            Ok(watcher) => {
                                if let Err(e) = watcher.unwatch(&watch_dir) {
                                    self.record_item_error(
                                        &update.name,
                                        format!(
                                            "watch: failed to replace non-recursive watch for {}: {e}",
                                            watch_dir.display()
                                        ),
                                    );
                                }
                            }
                            Err(e) => {
                                self.record_item_error(
                                    &update.name,
                                    format!("watch: failed to initialize notify backend: {e}"),
                                );
                                continue;
                            }
                        }
                    }
                    match ensure_notify_watcher(&mut self.watcher, &self.notify_tx).and_then(
                        |watcher| watcher.watch(&watch_dir, mode).map_err(WatchError::from),
                    ) {
                        Ok(()) => {
                            self.registered_dirs.insert(watch_dir, mode);
                        }
                        Err(e) => {
                            self.record_item_error(
                                &update.name,
                                format!(
                                    "watch: failed to register {:?} watch for {}: {e}",
                                    mode,
                                    watch_dir.display()
                                ),
                            );
                        }
                    }
                }
            }
        }

        let mut compiled_ignore = Vec::new();
        for pattern_str in &update.ignore_patterns {
            let full_pattern = resolve_pattern(&base_dir, pattern_str);
            if let Ok(pat) = Pattern::new(&full_pattern.to_string_lossy()) {
                compiled_ignore.push(pat);
            }
        }

        if let Some(item) = self.items.get_mut(&update.name) {
            refresh_item_definition(item, update.kind, compiled_patterns, compiled_ignore);
            let pats: Vec<&str> = item.patterns.iter().map(Pattern::as_str).collect();
            let igs: Vec<&str> = item.ignore_patterns.iter().map(Pattern::as_str).collect();
            self.emitter.service_debug_event(
                &update.name,
                &format!(
                    "watch: patterns updated kind={:?} patterns={:?} ignore={:?}",
                    update.kind, pats, igs
                ),
            );
        } else {
            // Item doesn't exist yet — create it (happens when build tool
            // resolution completes after startup for a service with no
            // explicit watch patterns).
            let pats: Vec<&str> = compiled_patterns.iter().map(Pattern::as_str).collect();
            let igs: Vec<&str> = compiled_ignore.iter().map(Pattern::as_str).collect();
            self.emitter.service_debug_event(
                &update.name,
                &format!(
                    "watch: item created kind={:?} patterns={:?} ignore={:?}",
                    update.kind, pats, igs
                ),
            );
            self.items.insert(
                update.name.clone(),
                WatchedItem {
                    state: WatchState::Idle,
                    debounce_duration: DEFAULT_DEBOUNCE,
                    debounce_deadline: None,
                    stale: false,
                    kind: update.kind,
                    patterns: compiled_patterns,
                    ignore_patterns: compiled_ignore,
                    last_error: None,
                },
            );
        }

        if let Some(applied_tx) = update.applied_tx.take() {
            let _ = applied_tx.send(());
        }
    }

    /// Find the soonest debounce deadline across all items.
    fn nearest_debounce_deadline(&self) -> Option<Instant> {
        self.items
            .values()
            .filter(|item| item.state == WatchState::Debouncing)
            .filter_map(|item| item.debounce_deadline)
            .min()
    }

    /// Route a notify event to the affected items and update their state machines.
    async fn handle_notify_event(&mut self, event: &notify::Event) {
        // Only care about create, modify, and remove events. Renames
        // (vim, sed -i) are reported as Modify(Name(_)) by notify.
        if !matches!(
            event.kind,
            EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
        ) {
            // Don't log these — Access/Other events fire constantly (every
            // open/close/stat) and drown out the signal.
            return;
        }

        self.emitter.debug_event(&format!(
            "watch: event kind={:?} paths={:?}",
            event.kind, event.paths
        ));

        // Find which items are affected by this event's paths.
        // Ignore patterns are checked first — if any ignore pattern matches,
        // the event is skipped for that item.
        let mut affected: Vec<String> = Vec::new();
        for path in &event.paths {
            let path_str = path.to_string_lossy();
            let mut matched_any = false;
            let mut ignored_by: Vec<String> = Vec::new();
            let mut unmatched: Vec<String> = Vec::new();
            for (name, item) in &self.items {
                let state = watch_state_label(item.state);
                if let Some(ig) = item.ignore_patterns.iter().find(|p| p.matches(&path_str)) {
                    self.emitter.service_debug_event(
                        name,
                        &format!(
                            "watch: ignored path={:?} state={} ignore={:?}",
                            path,
                            state,
                            ig.as_str()
                        ),
                    );
                    ignored_by.push(name.clone());
                    continue;
                }
                if let Some(pat) = item.patterns.iter().find(|p| p.matches(&path_str)) {
                    self.emitter.service_debug_event(
                        name,
                        &format!(
                            "watch: matched path={:?} state={} pattern={:?}",
                            path,
                            state,
                            pat.as_str()
                        ),
                    );
                    matched_any = true;
                    if !affected.contains(name) {
                        affected.push(name.clone());
                    }
                } else {
                    unmatched.push(name.clone());
                }
            }
            if !matched_any {
                if ignored_by.is_empty() {
                    self.emitter
                        .debug_event(&format!("watch: no item matched {:?}", path));
                } else {
                    self.emitter.debug_event(&format!(
                        "watch: no rebuild match for {:?} (ignored by {})",
                        path,
                        ignored_by.join(", ")
                    ));
                }
                for name in unmatched {
                    if let Some(item) = self.items.get(&name) {
                        self.emitter.service_debug_event(
                            &name,
                            &format!(
                                "watch: did not match path={:?} state={} reason=no pattern matched",
                                path,
                                watch_state_label(item.state)
                            ),
                        );
                    }
                }
            }
        }

        let now = Instant::now();
        let mut stale_services: Vec<String> = Vec::new();
        for name in affected {
            if let Some(item) = self.items.get_mut(&name) {
                match item.state {
                    // Idle → Debouncing: first change starts the debounce window.
                    WatchState::Idle => {
                        item.state = WatchState::Debouncing;
                        item.debounce_deadline = Some(now + item.debounce_duration);
                        self.emitter.service_debug_event(
                            &name,
                            &format!(
                                "watch: Idle → Debouncing (deadline in {:?})",
                                item.debounce_duration
                            ),
                        );
                    }
                    // Debouncing → Debouncing: sliding window resets the deadline
                    // so rapid consecutive saves coalesce into one rebuild.
                    WatchState::Debouncing => {
                        item.debounce_deadline = Some(now + item.debounce_duration);
                        self.emitter.service_debug_event(
                            &name,
                            &format!(
                                "watch: Debouncing — deadline bumped ({:?})",
                                item.debounce_duration
                            ),
                        );
                    }
                    // Rebuilding: can't start another cycle now. Set stale so we
                    // trigger a new rebuild when the current one completes.
                    WatchState::Rebuilding => {
                        item.stale = true;
                        if item.kind == WatchItemKind::Service && !stale_services.contains(&name) {
                            stale_services.push(name.clone());
                        }
                        self.emitter.service_debug_event(
                            &name,
                            "watch: Rebuilding — marked stale (will re-run after completion)",
                        );
                    }
                }
            }
        }

        for name in stale_services {
            let _ = self.cmd_tx.send(RunnerCommand::RebuildStale { name });
        }
    }

    /// Fire debounce timers that have expired — send rebuild/rerun commands.
    async fn fire_debounce_timers(&mut self) {
        let now = Instant::now();
        let mut to_fire: Vec<(String, WatchItemKind)> = Vec::new();

        for (name, item) in &self.items {
            if item.state == WatchState::Debouncing
                && let Some(deadline) = item.debounce_deadline
                && now >= deadline
            {
                to_fire.push((name.clone(), item.kind));
            }
        }

        for (name, kind) in to_fire {
            if let Some(item) = self.items.get_mut(&name) {
                item.debounce_deadline = None;

                let (cmd, label) = match kind {
                    WatchItemKind::Task => {
                        item.state = WatchState::Rebuilding;
                        (RunnerCommand::TaskRerun { name: name.clone() }, "TaskRerun")
                    }
                    WatchItemKind::Service => {
                        item.state = WatchState::Rebuilding;
                        (RunnerCommand::Rebuild { name: name.clone() }, "Rebuild")
                    }
                    WatchItemKind::BuildGraph => {
                        // Build graph change has no rebuild/complete cycle —
                        // the runner re-queries the build tool asynchronously.
                        // Extract the service/task name by stripping "__graph" suffix.
                        item.state = WatchState::Idle;
                        let item_name = build_graph_command_name(&name);
                        (
                            RunnerCommand::BuildGraphChanged { name: item_name },
                            "BuildGraphChanged",
                        )
                    }
                };
                self.emitter.service_debug_event(
                    &name,
                    &format!(
                        "watch: debounce fired → sending {} (state={:?})",
                        label, item.state
                    ),
                );
                // If the channel is closed, the runner is shutting down.
                if self.cmd_tx.send(cmd).is_err() {
                    self.emitter.service_debug_event(
                        &name,
                        "watch: command channel closed — runner is shutting down",
                    );
                }
            }
        }
    }

    /// Handle a runner event — mainly looking for rebuild/rerun completion.
    async fn handle_runner_event(&mut self, event: &RunnerEvent) {
        match event {
            RunnerEvent::RebuildComplete { name, success } => {
                if let Some(item) = self.items.get_mut(name) {
                    if item.stale {
                        // More changes came in during the rebuild — trigger another cycle.
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        self.emitter.service_debug_event(
                            name,
                            &format!(
                                "watch: RebuildComplete(success={success}) stale=true — re-running"
                            ),
                        );
                        let _ = self
                            .cmd_tx
                            .send(RunnerCommand::Rebuild { name: name.clone() });
                    } else {
                        item.state = WatchState::Idle;
                        self.emitter.service_debug_event(
                            name,
                            &format!("watch: RebuildComplete(success={success}) → Idle"),
                        );
                    }
                } else {
                    self.emitter
                        .debug_event(&format!("watch: RebuildComplete for unknown item {name:?}"));
                }
            }
            RunnerEvent::TaskRerunComplete { name, success } => {
                if let Some(item) = self.items.get_mut(name) {
                    if item.stale {
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        self.emitter.service_debug_event(
                            name,
                            &format!(
                                "watch: TaskRerunComplete(success={success}) stale=true — re-running"
                            ),
                        );
                        let _ = self
                            .cmd_tx
                            .send(RunnerCommand::TaskRerun { name: name.clone() });
                    } else {
                        item.state = WatchState::Idle;
                        self.emitter.service_debug_event(
                            name,
                            &format!("watch: TaskRerunComplete(success={success}) → Idle"),
                        );
                    }
                } else {
                    self.emitter.debug_event(&format!(
                        "watch: TaskRerunComplete for unknown item {name:?}"
                    ));
                }
            }
            RunnerEvent::ShutdownComplete => {
                // Stop watching.
            }
            _ => {}
        }
    }

    fn snapshot(&self) -> WatchSnapshot {
        let items = self
            .items
            .iter()
            .map(|(name, item)| {
                (
                    name.clone(),
                    WatchItemSnapshot {
                        kind: watch_item_kind_label(item.kind),
                        state: watch_state_label(item.state),
                        stale: item.stale,
                        debounce_ms: item.debounce_duration.as_millis() as u64,
                        last_error: item.last_error.clone(),
                    },
                )
            })
            .collect();

        WatchSnapshot {
            items,
            notify_error_count: self.notify_error_count,
            runner_event_lag_count: self.runner_event_lag_count,
            last_notify_error: self.last_notify_error.clone(),
        }
    }

    fn record_notify_error(&mut self, error: &str) {
        self.notify_error_count = self.notify_error_count.saturating_add(1);
        self.last_notify_error = Some(error.to_string());
        self.emitter
            .lifecycle_event(&format!("watch: notify backend error: {error}"));
    }

    fn record_item_error(&mut self, name: &str, error: String) {
        if let Some(item) = self.items.get_mut(name) {
            item.last_error = Some(error.clone());
        }
        self.emitter.service_debug_event(name, &error);
        self.emitter.lifecycle_event(&error);
    }
}

/// Is a watch on `path` with `mode` already covered by something in `existing`?
///
/// A `Recursive` request is covered only by a `Recursive` ancestor (including
/// exact-match). A `NonRecursive` ancestor sees only direct-child events and
/// does NOT cover descendants.
///
/// A `NonRecursive` request is covered by a `Recursive` ancestor (which sees
/// everything under it), or by an exact same-path watch of any mode (which
/// already sees direct-child events at `path`).
fn is_covered(
    path: &Path,
    mode: RecursiveMode,
    existing: &HashMap<PathBuf, RecursiveMode>,
) -> bool {
    if existing
        .iter()
        .any(|(dir, m)| *m == RecursiveMode::Recursive && path.starts_with(dir))
    {
        return true;
    }
    if mode == RecursiveMode::NonRecursive && existing.contains_key(path) {
        return true;
    }
    false
}

fn refresh_item_definition(
    item: &mut WatchedItem,
    kind: WatchItemKind,
    patterns: Vec<Pattern>,
    ignore_patterns: Vec<Pattern>,
) {
    // A build-tool re-query is a full re-registration of this item's watch
    // definition. If we previously missed a RebuildComplete /
    // TaskRerunComplete broadcast, the item may be stuck in `Rebuilding` and
    // would otherwise swallow future edits forever.
    item.state = WatchState::Idle;
    item.debounce_deadline = None;
    item.stale = false;
    item.kind = kind;
    item.patterns = patterns;
    item.ignore_patterns = ignore_patterns;
    item.last_error = None;
}

fn build_graph_command_name(name: &str) -> String {
    if name == WORKSPACE_GRAPH_ITEM_NAME {
        return WORKSPACE_GRAPH_ITEM_NAME.to_string();
    }

    name.strip_suffix("__graph").unwrap_or(name).to_string()
}

fn watch_state_label(state: WatchState) -> &'static str {
    match state {
        WatchState::Idle => "idle",
        WatchState::Debouncing => "debouncing",
        WatchState::Rebuilding => "rebuilding",
    }
}

fn watch_item_kind_label(kind: WatchItemKind) -> &'static str {
    match kind {
        WatchItemKind::Service => "service",
        WatchItemKind::Task => "task",
        WatchItemKind::BuildGraph => "build_graph",
    }
}

/// Extract the base directory from a glob pattern.
///
/// Returns the longest directory prefix before the first glob metacharacter.
/// The result is always a directory path, never a file:
/// - `src/**/*.rs` → `src` (stopped at `**`, so `src` is the directory)
/// - `*.txt` → `.` (first component is a glob, so the directory is `.`)
/// - `a/b/c/*.log` → `a/b/c`
/// - `src/main.rs` → `src` (no glob found, so we take the parent directory)
fn glob_base_dir(pattern: &Path) -> PathBuf {
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
    // If no glob was found, the path is a literal file (e.g. `src/main.rs`).
    // Take its parent directory so we don't create a directory named after the file.
    if !hit_glob {
        base = base.parent().map(Path::to_path_buf).unwrap_or_default();
    }
    // Fall back to current directory if the base is empty (e.g. pattern is `*.txt`
    // or a bare filename like `Makefile`).
    if base.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        base
    }
}

fn resolve_pattern(base_dir: &Path, pattern: &str) -> PathBuf {
    let pattern_path = Path::new(pattern);
    if pattern_path.is_absolute() {
        pattern_path.to_path_buf()
    } else {
        base_dir.join(pattern_path)
    }
}

fn ensure_notify_watcher<'a>(
    watcher: &'a mut Option<NotifyBackend>,
    notify_tx: &mpsc::UnboundedSender<notify::Result<notify::Event>>,
) -> Result<&'a mut NotifyBackend, WatchError> {
    if watcher.is_none() {
        *watcher = Some(if prefer_poll_watcher() {
            create_poll_watcher(notify_tx)?
        } else {
            match create_native_watcher(notify_tx) {
                Ok(watcher) => watcher,
                Err(_) => create_poll_watcher(notify_tx)?,
            }
        });
    }
    watcher
        .as_mut()
        .ok_or_else(|| notify::Error::generic("failed to initialize notify watcher").into())
}

fn prefer_poll_watcher() -> bool {
    cfg!(debug_assertions) && std::env::var_os("DON_NATIVE_WATCH").is_none()
}

fn create_native_watcher(
    notify_tx: &mpsc::UnboundedSender<notify::Result<notify::Event>>,
) -> notify::Result<NotifyBackend> {
    let tx = notify_tx.clone();
    RecommendedWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        notify::Config::default().with_follow_symlinks(false),
    )
    .map(NotifyBackend::Native)
}

fn create_poll_watcher(
    notify_tx: &mpsc::UnboundedSender<notify::Result<notify::Event>>,
) -> notify::Result<NotifyBackend> {
    let tx = notify_tx.clone();
    PollWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        notify::Config::default()
            .with_follow_symlinks(false)
            .with_poll_interval(Duration::from_millis(250))
            .with_compare_contents(true),
    )
    .map(NotifyBackend::Poll)
}

/// Sleep until the given instant, or pend forever if `None`.
async fn sleep_until_or_pending(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_base_dir() {
        struct Case {
            pattern: &'static str,
            expected: &'static str,
        }

        let cases = vec![
            Case {
                pattern: "src/**/*.rs",
                expected: "src",
            },
            Case {
                pattern: "*.txt",
                expected: ".",
            },
            Case {
                pattern: "a/b/c/*.log",
                expected: "a/b/c",
            },
            Case {
                pattern: "a/b/*/d.txt",
                expected: "a/b",
            },
            // No glob: take parent directory, not the file itself.
            Case {
                pattern: "exact/path/file.txt",
                expected: "exact/path",
            },
            Case {
                pattern: "src/[abc]/*.rs",
                expected: "src",
            },
            // Single literal filename: parent is `.`
            Case {
                pattern: "Makefile",
                expected: ".",
            },
        ];

        for case in cases {
            let result = glob_base_dir(Path::new(case.pattern));
            assert_eq!(
                result,
                PathBuf::from(case.expected),
                "glob_base_dir({:?}) = {:?}, expected {:?}",
                case.pattern,
                result,
                case.expected
            );
        }
    }

    #[test]
    fn test_is_covered() {
        struct Case {
            name: &'static str,
            path: &'static str,
            mode: RecursiveMode,
            existing: Vec<(&'static str, RecursiveMode)>,
            expected: bool,
        }

        let cases = vec![
            Case {
                name: "recursive ancestor covers recursive",
                path: "/a/b/c",
                mode: RecursiveMode::Recursive,
                existing: vec![("/a", RecursiveMode::Recursive)],
                expected: true,
            },
            Case {
                name: "recursive ancestor covers non-recursive",
                path: "/a/b/c",
                mode: RecursiveMode::NonRecursive,
                existing: vec![("/a", RecursiveMode::Recursive)],
                expected: true,
            },
            Case {
                // This is the bug we're fixing: a non-recursive ancestor must
                // NOT count as covering a recursive request — descendants
                // would not receive events.
                name: "non-recursive ancestor does NOT cover recursive",
                path: "/a/b",
                mode: RecursiveMode::Recursive,
                existing: vec![("/a", RecursiveMode::NonRecursive)],
                expected: false,
            },
            Case {
                name: "non-recursive ancestor does NOT cover recursive at same path",
                path: "/a",
                mode: RecursiveMode::Recursive,
                existing: vec![("/a", RecursiveMode::NonRecursive)],
                expected: false,
            },
            Case {
                name: "exact same path non-recursive covers non-recursive",
                path: "/a",
                mode: RecursiveMode::NonRecursive,
                existing: vec![("/a", RecursiveMode::NonRecursive)],
                expected: true,
            },
            Case {
                name: "empty existing never covers",
                path: "/a",
                mode: RecursiveMode::Recursive,
                existing: vec![],
                expected: false,
            },
            Case {
                name: "sibling does not cover",
                path: "/a/b",
                mode: RecursiveMode::Recursive,
                existing: vec![("/a/c", RecursiveMode::Recursive)],
                expected: false,
            },
        ];

        for case in cases {
            let map: HashMap<PathBuf, RecursiveMode> = case
                .existing
                .iter()
                .map(|(p, m)| (PathBuf::from(p), *m))
                .collect();
            assert_eq!(
                is_covered(Path::new(case.path), case.mode, &map),
                case.expected,
                "case: {}",
                case.name,
            );
        }
    }

    #[tokio::test]
    async fn test_build_tool_watch_opt_outs_skip_workspace_graph_watch() {
        struct Case {
            name: &'static str,
            toml: &'static str,
            expect_watches: bool,
        }

        let cases = vec![
            Case {
                name: "bazel watch false",
                toml: r#"
[services.api]
bazel.target = "//services/api:api"
bazel.watch = false
"#,
                expect_watches: false,
            },
            Case {
                name: "bazel reload false",
                toml: r#"
[services.api]
bazel.target = "//services/api:api"
reload = false
"#,
                expect_watches: false,
            },
            Case {
                name: "bazel default watches workspace graph",
                toml: r#"
[services.api]
bazel.target = "//services/api:api"
"#,
                expect_watches: true,
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let config: crate::config::Config = case.toml.parse().unwrap();
            let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
            let (_event_tx, event_rx) = broadcast::channel(8);
            let (_update_tx, update_rx) = mpsc::unbounded_channel();
            let (_query_tx, query_rx) = mpsc::channel(8);
            let output = crate::output::OutputManager::new(
                &[("api", &crate::config::LogConfig::Stdout)],
                tokio::io::sink(),
            )
            .await
            .unwrap();

            let (watch_mgr, warnings) = WatchManager::new(
                &config,
                crate::config::Platform::LinuxX86_64,
                temp.path(),
                cmd_tx,
                event_rx,
                update_rx,
                query_rx,
                output.clone_lifecycle_emitter(),
            )
            .unwrap();

            if case.expect_watches {
                // Positive cases need a real notify backend. Some developer
                // machines can exhaust Linux's per-user inotify instance
                // ceiling; that should not hide regressions in the opt-out
                // cases this table primarily covers.
                assert!(
                    warnings.is_empty()
                        || warnings
                            .iter()
                            .all(|warning| warning.contains("workspace watch registration failed")),
                    "case: {} warnings: {:?}",
                    case.name,
                    warnings
                );
            } else {
                assert!(warnings.is_empty(), "case: {}", case.name);
            }
            assert_eq!(
                watch_mgr.has_watches(),
                case.expect_watches,
                "case: {}",
                case.name,
            );
        }
    }

    #[test]
    fn test_glob_pattern_matches_files_in_watched_dirs() {
        struct Case {
            name: &'static str,
            pattern: &'static str,
            path: &'static str,
            expected: bool,
        }

        let cases = vec![
            Case {
                name: "** matches nested file",
                pattern: "/app/src/**/*.rs",
                path: "/app/src/foo/bar.rs",
                expected: true,
            },
            Case {
                name: "** matches deeply nested",
                pattern: "/app/src/**/*.rs",
                path: "/app/src/a/b/c.rs",
                expected: true,
            },
            Case {
                name: "** matches file directly in src",
                pattern: "/app/src/**/*.rs",
                path: "/app/src/main.rs",
                expected: true,
            },
            Case {
                name: "literal file matches",
                pattern: "/app/Cargo.toml",
                path: "/app/Cargo.toml",
                expected: true,
            },
            Case {
                name: "literal file does not match other",
                pattern: "/app/Cargo.toml",
                path: "/app/Cargo.lock",
                expected: false,
            },
            Case {
                name: "does not match outside dir",
                pattern: "/app/src/**/*.rs",
                path: "/other/src/main.rs",
                expected: false,
            },
            // Build tool integration patterns end with /** (no file extension filter)
            Case {
                name: "/** matches nested file",
                pattern: "/app/services/api/**",
                path: "/app/services/api/src/main.py",
                expected: true,
            },
            Case {
                name: "/** matches direct child",
                pattern: "/app/services/api/**",
                path: "/app/services/api/main.py",
                expected: true,
            },
            Case {
                name: "/** does not match sibling dir",
                pattern: "/app/services/api/**",
                path: "/app/services/web/main.py",
                expected: false,
            },
        ];

        for case in cases {
            let pat = Pattern::new(case.pattern).unwrap();
            assert_eq!(
                pat.matches(case.path),
                case.expected,
                "case: {} — pattern {:?} vs path {:?}",
                case.name,
                case.pattern,
                case.path,
            );
        }
    }

    #[tokio::test]
    async fn test_state_machine_debounce_coalesces_events() {
        // Simulate: 10 events arrive in quick succession. Only one rebuild fires.
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        // Create a minimal event to feed the state machine.
        let make_event = || notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };

        // Feed 10 events rapidly (every 10ms).
        let mut mgr_items = items;
        for _ in 0..10 {
            let event = make_event();
            handle_notify_event_standalone(&mut mgr_items, &event, &cmd_tx).await;
            tokio::time::advance(Duration::from_millis(10)).await;
        }

        // All items should be in Debouncing state.
        assert_eq!(mgr_items["api"].state, WatchState::Debouncing);

        // Advance past the debounce window (200ms from the last event).
        tokio::time::advance(Duration::from_millis(200)).await;

        // Fire timers.
        fire_debounce_timers_standalone(&mut mgr_items, &cmd_tx).await;
        assert_eq!(mgr_items["api"].state, WatchState::Rebuilding);

        // Should have received exactly one Rebuild command.
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::Rebuild { ref name } if name == "api"));
        assert!(cmd_rx.try_recv().is_err());

        // Clean up: send rebuild complete event to reset state.
        let event = RunnerEvent::RebuildComplete {
            name: "api".to_string(),
            success: true,
        };
        handle_runner_event_standalone(&mut mgr_items, &event, &cmd_tx).await;
        assert_eq!(mgr_items["api"].state, WatchState::Idle);
    }

    #[tokio::test]
    async fn test_events_after_debounce_trigger_new_cycle() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/lib.rs")],
            attrs: Default::default(),
        };

        // First event: start debouncing.
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        let _ = cmd_rx.try_recv().unwrap(); // consume first Rebuild

        // Simulate rebuild completion.
        let complete = RunnerEvent::RebuildComplete {
            name: "api".to_string(),
            success: true,
        };
        handle_runner_event_standalone(&mut items, &complete, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Idle);

        // Second event: should start a new cycle.
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Debouncing);

        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::Rebuild { ref name } if name == "api"));
    }

    #[tokio::test]
    async fn test_custom_debounce_duration() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(500),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };

        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;

        // At 200ms: should NOT have fired yet (debounce is 500ms).
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Debouncing);
        assert!(cmd_rx.try_recv().is_err());

        // At 500ms: should fire.
        tokio::time::advance(Duration::from_millis(300)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        assert!(cmd_rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_change_during_build_triggers_second_rebuild() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Rebuilding,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        // Event during build — should set stale.
        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        assert!(items["api"].stale);
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::RebuildStale { ref name } if name == "api"));

        // Build completes — should trigger another Rebuild because stale.
        let complete = RunnerEvent::RebuildComplete {
            name: "api".to_string(),
            success: true,
        };
        handle_runner_event_standalone(&mut items, &complete, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        assert!(!items["api"].stale);
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::Rebuild { ref name } if name == "api"));
    }

    #[tokio::test]
    async fn test_multiple_events_during_build_one_followup() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Rebuilding,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };

        // 5 events during build.
        for _ in 0..5 {
            handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        }
        assert!(items["api"].stale);
        let mut stale_count = 0;
        while let Ok(cmd) = cmd_rx.try_recv() {
            if matches!(cmd, RunnerCommand::RebuildStale { ref name } if name == "api") {
                stale_count += 1;
            }
        }
        assert_eq!(stale_count, 5);

        // Build completes — only one follow-up rebuild.
        let complete = RunnerEvent::RebuildComplete {
            name: "api".to_string(),
            success: true,
        };
        handle_runner_event_standalone(&mut items, &complete, &cmd_tx).await;
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::Rebuild { ref name } if name == "api"));
        assert!(cmd_rx.try_recv().is_err()); // No extra commands.
    }

    #[tokio::test]
    async fn test_state_machine_full_cycle() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::Service,
                patterns: vec![Pattern::new("src/**/*.rs").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };

        // Idle -> Debouncing
        assert_eq!(items["api"].state, WatchState::Idle);
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Debouncing);

        // Debouncing -> Rebuilding
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        let _ = cmd_rx.try_recv().unwrap();

        // Events during rebuild set stale.
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        assert!(items["api"].stale);
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::RebuildStale { ref name } if name == "api"));

        // Rebuild completes with stale -> immediately Rebuilding again.
        let complete = RunnerEvent::RebuildComplete {
            name: "api".to_string(),
            success: true,
        };
        handle_runner_event_standalone(&mut items, &complete, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        assert!(!items["api"].stale);
        let _ = cmd_rx.try_recv().unwrap();

        // Second rebuild completes without stale -> Idle.
        handle_runner_event_standalone(&mut items, &complete, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Idle);
        assert!(cmd_rx.try_recv().is_err());
    }

    // --- Test helpers: standalone versions of WatchManager methods ---

    async fn handle_notify_event_standalone(
        items: &mut HashMap<String, WatchedItem>,
        event: &notify::Event,
        cmd_tx: &mpsc::UnboundedSender<RunnerCommand>,
    ) {
        if !matches!(
            event.kind,
            EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
        ) {
            return;
        }

        let mut affected: Vec<String> = Vec::new();
        for path in &event.paths {
            let path_str = path.to_string_lossy();
            for (name, item) in items.iter() {
                if item.ignore_patterns.iter().any(|p| p.matches(&path_str)) {
                    continue;
                }
                if item.patterns.iter().any(|p| p.matches(&path_str)) && !affected.contains(name) {
                    affected.push(name.clone());
                }
            }
        }

        let now = Instant::now();
        let mut stale_services: Vec<String> = Vec::new();
        for name in affected {
            if let Some(item) = items.get_mut(&name) {
                match item.state {
                    WatchState::Idle => {
                        item.state = WatchState::Debouncing;
                        item.debounce_deadline = Some(now + item.debounce_duration);
                    }
                    WatchState::Debouncing => {
                        item.debounce_deadline = Some(now + item.debounce_duration);
                    }
                    WatchState::Rebuilding => {
                        item.stale = true;
                        if item.kind == WatchItemKind::Service && !stale_services.contains(&name) {
                            stale_services.push(name.clone());
                        }
                    }
                }
            }
        }

        for name in stale_services {
            let _ = cmd_tx.send(RunnerCommand::RebuildStale { name });
        }
    }

    async fn fire_debounce_timers_standalone(
        items: &mut HashMap<String, WatchedItem>,
        cmd_tx: &mpsc::UnboundedSender<RunnerCommand>,
    ) {
        let now = Instant::now();
        let mut to_fire: Vec<(String, WatchItemKind)> = Vec::new();

        for (name, item) in items.iter() {
            if item.state == WatchState::Debouncing
                && let Some(deadline) = item.debounce_deadline
                && now >= deadline
            {
                to_fire.push((name.clone(), item.kind));
            }
        }

        for (name, kind) in to_fire {
            if let Some(item) = items.get_mut(&name) {
                item.debounce_deadline = None;
                let cmd = match kind {
                    WatchItemKind::Task => {
                        item.state = WatchState::Rebuilding;
                        RunnerCommand::TaskRerun { name }
                    }
                    WatchItemKind::Service => {
                        item.state = WatchState::Rebuilding;
                        RunnerCommand::Rebuild { name }
                    }
                    WatchItemKind::BuildGraph => {
                        item.state = WatchState::Idle;
                        let item_name = build_graph_command_name(&name);
                        RunnerCommand::BuildGraphChanged { name: item_name }
                    }
                };
                let _ = cmd_tx.send(cmd);
            }
        }
    }

    async fn handle_runner_event_standalone(
        items: &mut HashMap<String, WatchedItem>,
        event: &RunnerEvent,
        cmd_tx: &mpsc::UnboundedSender<RunnerCommand>,
    ) {
        match event {
            RunnerEvent::RebuildComplete { name, .. } => {
                if let Some(item) = items.get_mut(name) {
                    if item.stale {
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        let _ = cmd_tx.send(RunnerCommand::Rebuild { name: name.clone() });
                    } else {
                        item.state = WatchState::Idle;
                    }
                }
            }
            RunnerEvent::TaskRerunComplete { name, .. } => {
                if let Some(item) = items.get_mut(name) {
                    if item.stale {
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        let _ = cmd_tx.send(RunnerCommand::TaskRerun { name: name.clone() });
                    } else {
                        item.state = WatchState::Idle;
                    }
                }
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_build_graph_kind_sends_build_graph_changed() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "api__graph".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::BuildGraph,
                patterns: vec![Pattern::new("**/BUILD.bazel").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("services/api/BUILD.bazel")],
            attrs: Default::default(),
        };

        // Trigger the event.
        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        assert_eq!(items["api__graph"].state, WatchState::Debouncing);

        // Wait for debounce.
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;

        // BuildGraph kind goes straight to Idle (no rebuild cycle).
        assert_eq!(items["api__graph"].state, WatchState::Idle);

        // Should receive BuildGraphChanged with the service name (not the __graph suffix).
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(
            matches!(cmd, RunnerCommand::BuildGraphChanged { ref name } if name == "api"),
            "expected BuildGraphChanged for 'api', got different command"
        );
    }

    #[tokio::test]
    async fn test_build_graph_kind_no_rebuild_cycle() {
        // Build graph changes should NOT enter the Rebuilding state.
        // They go Idle -> Debouncing -> Idle (fire) directly.
        tokio::time::pause();

        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            "web__graph".to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::BuildGraph,
                patterns: vec![Pattern::new("**/package.json").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("apps/web/package.json")],
            attrs: Default::default(),
        };

        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;

        // Should be back to Idle, not Rebuilding.
        assert_eq!(items["web__graph"].state, WatchState::Idle);
        // And stale should still be false.
        assert!(!items["web__graph"].stale);
    }

    #[tokio::test]
    async fn test_workspace_build_graph_kind_preserves_workspace_sentinel() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

        let mut items = HashMap::new();
        items.insert(
            WORKSPACE_GRAPH_ITEM_NAME.to_string(),
            WatchedItem {
                state: WatchState::Idle,
                debounce_duration: Duration::from_millis(200),
                debounce_deadline: None,
                stale: false,
                kind: WatchItemKind::BuildGraph,
                patterns: vec![Pattern::new("**/MODULE.bazel").unwrap()],
                ignore_patterns: vec![],
                last_error: None,
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("MODULE.bazel")],
            attrs: Default::default(),
        };

        handle_notify_event_standalone(&mut items, &event, &cmd_tx).await;
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;

        let cmd = cmd_rx.try_recv().unwrap();
        assert!(
            matches!(cmd, RunnerCommand::BuildGraphChanged { ref name } if name == WORKSPACE_GRAPH_ITEM_NAME),
            "expected BuildGraphChanged for workspace sentinel, got different command"
        );
    }

    #[test]
    fn test_refresh_item_definition_resets_stuck_rebuilding_state() {
        let original_patterns = vec![Pattern::new("src/**/*.rs").unwrap()];
        let replacement_patterns = vec![Pattern::new("pkg/**").unwrap()];
        let replacement_ignore = vec![Pattern::new("pkg/generated/**").unwrap()];

        let mut item = WatchedItem {
            state: WatchState::Rebuilding,
            debounce_duration: Duration::from_millis(200),
            debounce_deadline: Some(Instant::now() + Duration::from_millis(50)),
            stale: true,
            kind: WatchItemKind::Service,
            patterns: original_patterns,
            ignore_patterns: vec![],
            last_error: None,
        };

        refresh_item_definition(
            &mut item,
            WatchItemKind::Task,
            replacement_patterns,
            replacement_ignore,
        );

        assert_eq!(item.state, WatchState::Idle);
        assert_eq!(item.debounce_deadline, None);
        assert!(!item.stale);
        assert_eq!(item.kind, WatchItemKind::Task);
        assert_eq!(item.patterns.len(), 1);
        assert_eq!(item.patterns[0].as_str(), "pkg/**");
        assert_eq!(item.ignore_patterns.len(), 1);
        assert_eq!(item.ignore_patterns[0].as_str(), "pkg/generated/**");
    }
}
