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
use crate::runner::{RunnerCommand, RunnerEvent};
use glob::Pattern;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::time::Instant;

/// Default debounce window when none is configured.
const DEFAULT_DEBOUNCE: Duration = Duration::from_millis(200);

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
    /// Send `RunnerCommand::ConfigReload` — no rebuild/complete cycle.
    Config,
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
}

/// Manages file watchers for all services and tasks with watch patterns.
///
/// Runs as a background tokio task, communicating with the runner via channels.
pub(crate) struct WatchManager {
    /// The notify watcher handle — kept alive to maintain watches.
    /// Named (not `_watcher`) so we can add new watch directories at runtime.
    watcher: RecommendedWatcher,
    /// Channel receiving raw notify events.
    event_rx: mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
    /// Per-item (service or task) state.
    items: HashMap<String, WatchedItem>,
    /// Sender to the runner's command channel.
    cmd_tx: mpsc::Sender<RunnerCommand>,
    /// Receiver for runner events (rebuild/rerun completion).
    runner_events: broadcast::Receiver<RunnerEvent>,
    /// Receiver for watch pattern updates from the runner (build tool re-queries).
    update_rx: mpsc::Receiver<WatchUpdate>,
    /// Directories already registered with the watcher, keyed by path with
    /// the mode the watch was registered under.
    ///
    /// The mode matters for coverage checks: a NonRecursive watch at
    /// `redo/server` sees direct-child events only; it does NOT cover a
    /// subsequent Recursive request for the same path. Treating it as
    /// coverage causes the Recursive registration to be silently skipped,
    /// and nested files never trigger events.
    registered_dirs: HashMap<PathBuf, RecursiveMode>,
}

impl WatchManager {
    /// Create a new watch manager from the config.
    ///
    /// Sets up notify watchers for all services and tasks with `watch` patterns.
    /// Creates missing watch directories so we get precise inotify coverage.
    ///
    /// Returns `(Self, warnings)` where warnings are non-fatal issues like
    /// invalid glob patterns (which should have been caught by validation).
    pub(crate) fn new(
        config: &Config,
        platform: Platform,
        base_dir: &Path,
        config_path: &Path,
        cmd_tx: mpsc::Sender<RunnerCommand>,
        runner_events: broadcast::Receiver<RunnerEvent>,
        update_rx: mpsc::Receiver<WatchUpdate>,
    ) -> Result<(Self, Vec<String>), WatchError> {
        let mut warnings: Vec<String> = Vec::new();
        let (notify_tx, event_rx) = mpsc::unbounded_channel();

        // `follow_symlinks(false)` is load-bearing: a bazel workspace root
        // has `bazel-*` convenience symlinks into the user-wide bazel cache
        // (millions of generated files, thousands of external repos).
        // Without this, any `RecursiveMode::Recursive` registration at or
        // above the root walks the entire cache and blows through
        // `fs.inotify.max_user_watches` while blocking for minutes.
        let mut watcher = RecommendedWatcher::new(
            move |res| {
                let _ = notify_tx.send(res);
            },
            notify::Config::default().with_follow_symlinks(false),
        )?;

        // Canonicalize base_dir so glob patterns are absolute and match the
        // absolute paths that notify reports in events. Without this, a base_dir
        // of `.` produces patterns like `./definitions/**/*.sql` that don't match
        // the absolute paths notify returns.
        let base_dir = std::fs::canonicalize(base_dir)
            .map_err(|e| WatchError::Io(base_dir.to_path_buf(), e))?;
        let base_dir = base_dir.as_path();

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
                    std::fs::canonicalize(&joined)
                        .unwrap_or(joined)
                }
                None => base_dir.to_path_buf(),
            };

            let debounce = match &resolved.debounce {
                Some(d) => parse_duration(d)?,
                None => DEFAULT_DEBOUNCE,
            };

            let mut compiled_patterns = Vec::new();
            for pattern_str in &watch_patterns {
                let full_pattern = svc_dir.join(pattern_str);
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
                std::fs::create_dir_all(&watch_dir).map_err(|e| {
                    WatchError::Io(watch_dir.clone(), e)
                })?;

                if !is_covered(&watch_dir, RecursiveMode::Recursive, &registered_dirs) {
                    watcher.watch(&watch_dir, RecursiveMode::Recursive)?;
                    registered_dirs.insert(watch_dir, RecursiveMode::Recursive);
                }
            }

            let mut compiled_ignore = Vec::new();
            for pattern_str in &resolved.ignore {
                let full_pattern = svc_dir.join(pattern_str);
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
                    std::fs::canonicalize(&joined)
                        .unwrap_or(joined)
                }
                None => base_dir.to_path_buf(),
            };

            let mut compiled_patterns = Vec::new();
            for pattern_str in &task.watch {
                let full_pattern = task_dir.join(pattern_str);
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
                std::fs::create_dir_all(&watch_dir).map_err(|e| {
                    WatchError::Io(watch_dir.clone(), e)
                })?;

                if !is_covered(&watch_dir, RecursiveMode::Recursive, &registered_dirs) {
                    watcher.watch(&watch_dir, RecursiveMode::Recursive)?;
                    registered_dirs.insert(watch_dir, RecursiveMode::Recursive);
                }
            }

            let mut compiled_ignore = Vec::new();
            for pattern_str in &task.ignore {
                let full_pattern = task_dir.join(pattern_str);
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
            let has_bazel = config
                .services
                .values()
                .any(|s| s.resolve(platform).bazel_config().is_some())
                || config.tasks.values().any(|t| t.bazel.is_some());
            let has_turbo = config
                .services
                .values()
                .any(|s| s.resolve(platform).turbo_config().is_some())
                || config.tasks.values().any(|t| t.turbo.is_some());

            if has_bazel || has_turbo {
                let mut root_file_names: Vec<&str> = Vec::new();
                if has_bazel {
                    root_file_names.extend([
                        "WORKSPACE",
                        "WORKSPACE.bazel",
                        "MODULE.bazel",
                    ]);
                }
                if has_turbo {
                    root_file_names.extend([
                        "turbo.json",
                        "turbo.jsonc",
                        "pnpm-workspace.yaml",
                    ]);
                }

                let mut compiled_patterns = Vec::new();
                for file_name in &root_file_names {
                    let full_pattern = base_dir.join(file_name);
                    if let Ok(pat) = Pattern::new(&full_pattern.to_string_lossy()) {
                        compiled_patterns.push(pat);
                    }
                }

                // Non-recursive watch on the workspace root is enough for
                // these specific filenames. No symlink spelunking.
                if !is_covered(base_dir, RecursiveMode::NonRecursive, &registered_dirs) {
                    let _ = watcher.watch(base_dir, RecursiveMode::NonRecursive);
                    registered_dirs.insert(base_dir.to_path_buf(), RecursiveMode::NonRecursive);
                }

                if !compiled_patterns.is_empty() {
                    items.insert(
                        "__workspace_graph__".to_string(),
                        WatchedItem {
                            state: WatchState::Idle,
                            debounce_duration: DEFAULT_DEBOUNCE,
                            debounce_deadline: None,
                            stale: false,
                            kind: WatchItemKind::BuildGraph,
                            patterns: compiled_patterns,
                            ignore_patterns: vec![],
                        },
                    );
                }
            }
        }

        // Watch the config file (don.toml) for auto-reload. We watch the
        // parent directory because editors like vim replace the file via
        // rename, which removes the inode the watcher was tracking.
        if let Ok(canonical_config) = std::fs::canonicalize(config_path)
            && let Some(config_dir) = canonical_config.parent()
            && let Ok(pat) = Pattern::new(&canonical_config.to_string_lossy())
        {
            if !is_covered(config_dir, RecursiveMode::NonRecursive, &registered_dirs) {
                let _ = watcher.watch(config_dir, RecursiveMode::NonRecursive);
                registered_dirs.insert(config_dir.to_path_buf(), RecursiveMode::NonRecursive);
            }
            items.insert(
                "__config__".to_string(),
                WatchedItem {
                    state: WatchState::Idle,
                    debounce_duration: DEFAULT_DEBOUNCE,
                    debounce_deadline: None,
                    stale: false,
                    kind: WatchItemKind::Config,
                    patterns: vec![pat],
                    ignore_patterns: vec![],
                },
            );
        }

        Ok((
            Self {
                watcher,
                event_rx,
                items,
                cmd_tx,
                runner_events,
                update_rx,
                registered_dirs,
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
                    if let Ok(event) = event_result {
                        self.handle_notify_event(&event);
                    }
                }
                _ = sleep_until_or_pending(next_deadline) => {
                    self.fire_debounce_timers().await;
                }
                result = self.runner_events.recv() => {
                    match result {
                        Ok(event) => self.handle_runner_event(&event).await,
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Missed some events — not critical, just continue.
                        }
                    }
                }
                Some(update) = self.update_rx.recv() => {
                    self.apply_watch_update(update);
                }
            }
        }
    }

    /// Apply a watch update from the runner (build tool re-query completed).
    ///
    /// Replaces the watch patterns for the named item and registers any
    /// new watch directories with the notify watcher.
    fn apply_watch_update(&mut self, update: WatchUpdate) {
        // Tier-1 BuildGraph updates land on specific filename patterns
        // (`<pkg>/BUILD`, `<pkg>/package.json`) — a non-recursive watch on
        // the package directory is exactly right. Tier-2 Service/Task
        // updates are directory-level globs (`<pkg>/**`), which need
        // recursive watching.
        let mode = match update.kind {
            WatchItemKind::BuildGraph | WatchItemKind::Config => {
                RecursiveMode::NonRecursive
            }
            WatchItemKind::Service | WatchItemKind::Task => RecursiveMode::Recursive,
        };

        // Canonicalize the base so the compiled globs are absolute and match
        // the cwd-prefixed absolute paths that notify reports in events.
        // Without this, a runner base_dir of `.` produces patterns like
        // `./auth/jwt/**` that will never match `/abs/cwd/./auth/jwt/foo.ts`.
        // The initial-setup path in `WatchManager::new` already canonicalizes;
        // this keeps the build-tool-resolved update path in sync.
        let base_dir = std::fs::canonicalize(&update.base_dir)
            .unwrap_or_else(|_| update.base_dir.clone());

        let mut compiled_patterns = Vec::new();
        for pattern_str in &update.patterns {
            let full_pattern = base_dir.join(pattern_str);
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
                        let _ = self.watcher.unwatch(&watch_dir);
                    }
                    let _ = self.watcher.watch(&watch_dir, mode);
                    self.registered_dirs.insert(watch_dir, mode);
                }
            }
        }

        let mut compiled_ignore = Vec::new();
        for pattern_str in &update.ignore_patterns {
            let full_pattern = base_dir.join(pattern_str);
            if let Ok(pat) = Pattern::new(&full_pattern.to_string_lossy()) {
                compiled_ignore.push(pat);
            }
        }

        if let Some(item) = self.items.get_mut(&update.name) {
            item.patterns = compiled_patterns;
            item.ignore_patterns = compiled_ignore;
        } else {
            // Item doesn't exist yet — create it (happens when build tool
            // resolution completes after startup for a service with no
            // explicit watch patterns).
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
                },
            );
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
    fn handle_notify_event(&mut self, event: &notify::Event) {
        // Only care about create, modify, and remove events. Renames
        // (vim, sed -i) are reported as Modify(Name(_)) by notify.
        if !matches!(
            event.kind,
            EventKind::Create(_)
                | EventKind::Modify(_)
                | EventKind::Remove(_)
        ) {
            return;
        }


        // Find which items are affected by this event's paths.
        // Ignore patterns are checked first — if any ignore pattern matches,
        // the event is skipped for that item.
        let mut affected: Vec<String> = Vec::new();
        for path in &event.paths {
            let path_str = path.to_string_lossy();
            for (name, item) in &self.items {
                if item.ignore_patterns.iter().any(|p| p.matches(&path_str)) {
                    continue;
                }
                if item.patterns.iter().any(|p| p.matches(&path_str))
                    && !affected.contains(name)
                {
                    affected.push(name.clone());
                }
            }
        }

        let now = Instant::now();
        for name in affected {
            if let Some(item) = self.items.get_mut(&name) {
                match item.state {
                    // Idle → Debouncing: first change starts the debounce window.
                    WatchState::Idle => {
                        item.state = WatchState::Debouncing;
                        item.debounce_deadline = Some(now + item.debounce_duration);
                    }
                    // Debouncing → Debouncing: sliding window resets the deadline
                    // so rapid consecutive saves coalesce into one rebuild.
                    WatchState::Debouncing => {
                        item.debounce_deadline = Some(now + item.debounce_duration);
                    }
                    // Rebuilding: can't start another cycle now. Set stale so we
                    // trigger a new rebuild when the current one completes.
                    WatchState::Rebuilding => {
                        item.stale = true;
                    }
                }
            }
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

                let cmd = match kind {
                    WatchItemKind::Task => {
                        item.state = WatchState::Rebuilding;
                        RunnerCommand::TaskRerun { name }
                    }
                    WatchItemKind::Service => {
                        item.state = WatchState::Rebuilding;
                        RunnerCommand::Rebuild { name }
                    }
                    WatchItemKind::Config => {
                        // Config reload has no rebuild/complete cycle —
                        // go straight back to Idle.
                        item.state = WatchState::Idle;
                        RunnerCommand::ConfigReload
                    }
                    WatchItemKind::BuildGraph => {
                        // Build graph change has no rebuild/complete cycle —
                        // the runner re-queries the build tool asynchronously.
                        // Extract the service/task name by stripping "__graph" suffix.
                        item.state = WatchState::Idle;
                        let item_name = name.strip_suffix("__graph")
                            .unwrap_or(&name)
                            .to_string();
                        RunnerCommand::BuildGraphChanged { name: item_name }
                    }
                };
                // If the channel is full/closed, the runner is shutting down.
                let _ = self.cmd_tx.send(cmd).await;
            }
        }
    }

    /// Handle a runner event — mainly looking for rebuild/rerun completion.
    async fn handle_runner_event(&mut self, event: &RunnerEvent) {
        match event {
            RunnerEvent::RebuildComplete { name, .. } => {
                if let Some(item) = self.items.get_mut(name) {
                    if item.stale {
                        // More changes came in during the rebuild — trigger another cycle.
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        let _ = self
                            .cmd_tx
                            .send(RunnerCommand::Rebuild {
                                name: name.clone(),
                            })
                            .await;
                    } else {
                        item.state = WatchState::Idle;
                    }
                }
            }
            RunnerEvent::TaskRerunComplete { name, .. } => {
                if let Some(item) = self.items.get_mut(name) {
                    if item.stale {
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        let _ = self
                            .cmd_tx
                            .send(RunnerCommand::TaskRerun {
                                name: name.clone(),
                            })
                            .await;
                    } else {
                        item.state = WatchState::Idle;
                    }
                }
            }
            RunnerEvent::ShutdownComplete => {
                // Stop watching.
            }
            _ => {}
        }
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

        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);

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
            handle_notify_event_standalone(&mut mgr_items, &event);
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

        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);

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
        handle_notify_event_standalone(&mut items, &event);
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
        handle_notify_event_standalone(&mut items, &event);
        assert_eq!(items["api"].state, WatchState::Debouncing);

        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        let cmd = cmd_rx.try_recv().unwrap();
        assert!(matches!(cmd, RunnerCommand::Rebuild { ref name } if name == "api"));
    }

    #[tokio::test]
    async fn test_custom_debounce_duration() {
        tokio::time::pause();

        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);

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
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("src/main.rs")],
            attrs: Default::default(),
        };

        handle_notify_event_standalone(&mut items, &event);

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

        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);

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
        handle_notify_event_standalone(&mut items, &event);
        assert!(items["api"].stale);
        assert_eq!(items["api"].state, WatchState::Rebuilding);

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

        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);

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
            handle_notify_event_standalone(&mut items, &event);
        }
        assert!(items["api"].stale);

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

        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);

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
        handle_notify_event_standalone(&mut items, &event);
        assert_eq!(items["api"].state, WatchState::Debouncing);

        // Debouncing -> Rebuilding
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;
        assert_eq!(items["api"].state, WatchState::Rebuilding);
        let _ = cmd_rx.try_recv().unwrap();

        // Events during rebuild set stale.
        handle_notify_event_standalone(&mut items, &event);
        assert!(items["api"].stale);
        assert_eq!(items["api"].state, WatchState::Rebuilding);

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

    fn handle_notify_event_standalone(
        items: &mut HashMap<String, WatchedItem>,
        event: &notify::Event,
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
                if item.patterns.iter().any(|p| p.matches(&path_str))
                    && !affected.contains(name)
                {
                    affected.push(name.clone());
                }
            }
        }

        let now = Instant::now();
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
                    }
                }
            }
        }
    }

    async fn fire_debounce_timers_standalone(
        items: &mut HashMap<String, WatchedItem>,
        cmd_tx: &mpsc::Sender<RunnerCommand>,
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
                    WatchItemKind::Config => {
                        item.state = WatchState::Idle;
                        RunnerCommand::ConfigReload
                    }
                    WatchItemKind::BuildGraph => {
                        item.state = WatchState::Idle;
                        let item_name = name.strip_suffix("__graph")
                            .unwrap_or(&name)
                            .to_string();
                        RunnerCommand::BuildGraphChanged { name: item_name }
                    }
                };
                let _ = cmd_tx.send(cmd).await;
            }
        }
    }

    async fn handle_runner_event_standalone(
        items: &mut HashMap<String, WatchedItem>,
        event: &RunnerEvent,
        cmd_tx: &mpsc::Sender<RunnerCommand>,
    ) {
        match event {
            RunnerEvent::RebuildComplete { name, .. } => {
                if let Some(item) = items.get_mut(name) {
                    if item.stale {
                        item.stale = false;
                        item.state = WatchState::Rebuilding;
                        let _ = cmd_tx
                            .send(RunnerCommand::Rebuild {
                                name: name.clone(),
                            })
                            .await;
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
                        let _ = cmd_tx
                            .send(RunnerCommand::TaskRerun {
                                name: name.clone(),
                            })
                            .await;
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

        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);

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
        handle_notify_event_standalone(&mut items, &event);
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

        let (cmd_tx, _cmd_rx) = mpsc::channel(64);

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
            },
        );

        let event = notify::Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            paths: vec![PathBuf::from("apps/web/package.json")],
            attrs: Default::default(),
        };

        handle_notify_event_standalone(&mut items, &event);
        tokio::time::advance(Duration::from_millis(200)).await;
        fire_debounce_timers_standalone(&mut items, &cmd_tx).await;

        // Should be back to Idle, not Rebuilding.
        assert_eq!(items["web__graph"].state, WatchState::Idle);
        // And stale should still be false.
        assert!(!items["web__graph"].stale);
    }
}
