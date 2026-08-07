//! The global watch report, and the reader that serves it without the runner.
//!
//! `GET /watch` / `don watch` describe everything the file watcher is
//! monitoring right now. None of that is runner state — the watcher owns it —
//! so the server queries the watch manager directly through a
//! [`WatchStatusReader`] rather than round-tripping the runner's command
//! loop. The runner's only involvement is publishing the query sender once
//! the watcher exists (it starts mid-startup, after the API socket is
//! already serving).

use super::{WatchQuery, WatchSnapshot};
use tokio::sync::{mpsc, oneshot, watch};

/// How long a status query waits for the watch manager before giving up —
/// the reader answers "no watches" rather than hanging a status request on
/// a wedged watcher.
const QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

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
    /// Number of errors the notify backend has reported since startup.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub notify_error_count: u64,
    /// Number of runner events the watcher missed due to channel lag.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub runner_event_lag_count: u64,
    /// The most recent notify backend error, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_notify_error: Option<String>,
}

/// One registered watch directory.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WatchDir {
    /// Absolute path of the registered directory.
    pub path: String,
    /// Registration mode (recursive or non-recursive).
    pub mode: String,
}

/// Watch state for one service or task.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WatchReportItem {
    /// Service or task name.
    pub name: String,
    /// Item kind: "service", "task" or "build-graph".
    pub kind: String,
    /// Current watch state (idle, debouncing, rebuilding, …).
    pub state: String,
    /// Whether a change arrived during the current rebuild cycle.
    pub stale: bool,
    /// Effective debounce in milliseconds.
    pub debounce_ms: u64,
    /// Resolved watch patterns.
    pub patterns: Vec<String>,
    /// Resolved ignore patterns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ignore_patterns: Vec<String>,
    /// The most recent per-item watch error, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Reshape a watch snapshot into the sorted, serializable report.
pub(crate) fn build_watch_report(snapshot: &WatchSnapshot) -> WatchReport {
    let mut directories: Vec<WatchDir> = snapshot
        .registered_dirs
        .iter()
        .map(|(path, mode)| WatchDir {
            path: path.to_string_lossy().into_owned(),
            mode: (*mode).to_string(),
        })
        .collect();
    directories.sort_by(|a, b| a.path.cmp(&b.path));

    let mut items: Vec<WatchReportItem> = snapshot
        .items
        .iter()
        .map(|(name, item)| WatchReportItem {
            name: name.clone(),
            kind: item.kind.to_string(),
            state: item.state.to_string(),
            stale: item.stale,
            debounce_ms: item.debounce_ms,
            patterns: item.patterns.clone(),
            ignore_patterns: item.ignore_patterns.clone(),
            last_error: item.last_error.clone(),
        })
        .collect();
    items.sort_by(|a, b| a.name.cmp(&b.name));

    WatchReport {
        directories,
        items,
        global_ignore: snapshot.global_ignore.clone(),
        notify_error_count: snapshot.notify_error_count,
        runner_event_lag_count: snapshot.runner_event_lag_count,
        last_notify_error: snapshot.last_notify_error.clone(),
    }
}

/// A cloneable, read-only handle for the global watch report.
///
/// The channel carries a tri-state because the watcher starts mid-startup:
/// the outer `None` means "watch setup hasn't finished yet" — the reader
/// *waits* through it, preserving the ordering the old command round trip
/// gave for free (a query never answers before setup has decided). The
/// inner `Option` is the decision: `None` = nothing to watch, `Some` = the
/// live query sender.
#[derive(Clone)]
pub struct WatchStatusReader {
    queries: watch::Receiver<Option<Option<mpsc::Sender<WatchQuery>>>>,
}

impl WatchStatusReader {
    /// A reader with no watcher behind it, for tests that need an `ApiState`
    /// without a runner. Every report is `None`.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        let (tx, rx) = watch::channel(Some(None));
        std::mem::forget(tx);
        Self { queries: rx }
    }

    /// Fetch and reshape the current watch snapshot. Waits for watch setup
    /// to finish if it hasn't; answers `None` when there is nothing to
    /// watch, the runner went away, or the watcher does not answer within
    /// [`QUERY_TIMEOUT`].
    pub(crate) async fn report(&self) -> Option<WatchReport> {
        let mut rx = self.queries.clone();
        // wait_for yields once the setup decision is published; it errors if
        // the runner (the sender) is dropped first, which maps to `None`.
        // The guard is cloned out in the same statement so its (non-Send)
        // read lock never overlaps an await.
        let queries = rx
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|decided| decided.clone())
            .flatten()?;
        let (reply, reply_rx) = oneshot::channel();
        queries.send(WatchQuery { reply }).await.ok()?;
        let snapshot = tokio::time::timeout(QUERY_TIMEOUT, reply_rx)
            .await
            .ok()?
            .ok()?;
        Some(build_watch_report(&snapshot))
    }
}

/// Create the publisher/reader pair for the watch query sender. Starts in
/// the undecided state; the runner publishes `Some(None)` (nothing to
/// watch) or `Some(Some(sender))` once watch setup finishes.
pub(crate) fn status_channel() -> (
    watch::Sender<Option<Option<mpsc::Sender<WatchQuery>>>>,
    WatchStatusReader,
) {
    let (tx, rx) = watch::channel(None);
    (tx, WatchStatusReader { queries: rx })
}
