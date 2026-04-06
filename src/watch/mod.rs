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
use std::collections::{HashMap, HashSet};
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
enum WatchItemKind {
    /// Send `RunnerCommand::Rebuild { name }`.
    Service,
    /// Send `RunnerCommand::TaskRerun { name }`.
    Task,
    /// Send `RunnerCommand::ConfigReload` — no rebuild/complete cycle.
    Config,
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

/// Manages file watchers for all services and tasks with watch patterns.
///
/// Runs as a background tokio task, communicating with the runner via channels.
pub(crate) struct WatchManager {
    /// The notify watcher handle — kept alive to maintain watches.
    _watcher: RecommendedWatcher,
    /// Channel receiving raw notify events.
    event_rx: mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
    /// Per-item (service or task) state.
    items: HashMap<String, WatchedItem>,
    /// Sender to the runner's command channel.
    cmd_tx: mpsc::Sender<RunnerCommand>,
    /// Receiver for runner events (rebuild/rerun completion).
    runner_events: broadcast::Receiver<RunnerEvent>,
}

impl WatchManager {
    /// Create a new watch manager from the config.
    ///
    /// Sets up notify watchers for all services and tasks with `watch` patterns.
    /// Creates missing watch directories so we get precise inotify coverage.
    ///
    /// Returns `(Self, warnings)` where warnings are non-fatal issues like
    /// invalid glob patterns (which should have been caught by validation).
    pub(crate) async fn new(
        config: &Config,
        platform: Platform,
        base_dir: &Path,
        config_path: &Path,
        cmd_tx: mpsc::Sender<RunnerCommand>,
        runner_events: broadcast::Receiver<RunnerEvent>,
    ) -> Result<(Self, Vec<String>), WatchError> {
        let mut warnings: Vec<String> = Vec::new();
        let (notify_tx, event_rx) = mpsc::unbounded_channel();

        let mut watcher = RecommendedWatcher::new(
            move |res| {
                let _ = notify_tx.send(res);
            },
            notify::Config::default(),
        )?;

        // Canonicalize base_dir so glob patterns are absolute and match the
        // absolute paths that notify reports in events. Without this, a base_dir
        // of `.` produces patterns like `./definitions/**/*.sql` that don't match
        // the absolute paths notify returns.
        let base_dir = std::fs::canonicalize(base_dir)
            .map_err(|e| WatchError::Io(base_dir.to_path_buf(), e))?;
        let base_dir = base_dir.as_path();

        let mut items: HashMap<String, WatchedItem> = HashMap::new();
        // Track which directories we've already registered to avoid duplicates.
        let mut registered_dirs: HashSet<PathBuf> = HashSet::new();

        // Process services.
        for (name, svc) in &config.services {
            let resolved = svc.resolve(platform);

            // Use configured watch patterns, or inject preset defaults.
            let watch_patterns: Vec<String> = if !resolved.watch.is_empty() {
                resolved.watch.clone()
            } else if resolved.rust.is_some() {
                vec![
                    "src/**/*.rs".to_string(),
                    "Cargo.toml".to_string(),
                    "Cargo.lock".to_string(),
                ]
            } else if resolved.go.is_some() {
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
            let svc_dir = match resolved.dir.as_deref() {
                Some(d) => base_dir.join(d),
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
                tokio::fs::create_dir_all(&watch_dir).await.map_err(|e| {
                    WatchError::Io(watch_dir.clone(), e)
                })?;

                // Skip if an already-registered ancestor covers this path
                // (all watches are recursive, so a parent watch already sees
                // everything under it).
                let already_covered = registered_dirs
                    .iter()
                    .any(|existing| watch_dir.starts_with(existing));
                if !already_covered {
                    watcher.watch(&watch_dir, RecursiveMode::Recursive)?;
                    registered_dirs.insert(watch_dir);
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
                Some(d) => base_dir.join(d),
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
                tokio::fs::create_dir_all(&watch_dir).await.map_err(|e| {
                    WatchError::Io(watch_dir.clone(), e)
                })?;

                // Skip if an already-registered ancestor covers this path
                // (all watches are recursive, so a parent watch already sees
                // everything under it).
                let already_covered = registered_dirs
                    .iter()
                    .any(|existing| watch_dir.starts_with(existing));
                if !already_covered {
                    watcher.watch(&watch_dir, RecursiveMode::Recursive)?;
                    registered_dirs.insert(watch_dir);
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

        // Watch the config file (don.toml) for auto-reload. We watch the
        // parent directory because editors like vim replace the file via
        // rename, which removes the inode the watcher was tracking.
        if let Ok(canonical_config) = std::fs::canonicalize(config_path)
            && let Some(config_dir) = canonical_config.parent()
            && let Ok(pat) = Pattern::new(&canonical_config.to_string_lossy())
        {
            let already_covered = registered_dirs
                .iter()
                .any(|existing| config_dir.starts_with(existing));
            if !already_covered {
                let _ = watcher.watch(config_dir, RecursiveMode::NonRecursive);
                registered_dirs.insert(config_dir.to_path_buf());
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
                _watcher: watcher,
                event_rx,
                items,
                cmd_tx,
                runner_events,
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
            }
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
}
