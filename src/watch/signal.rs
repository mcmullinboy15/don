//! How the watcher tells anyone that files changed.
//!
//! Watching files is not a runner concern — it is "these paths changed,
//! under this item's patterns". What that *means* is the receiver's: whether
//! to rebuild, whether a build is already running, whether this task is even
//! allowed to re-run itself. The watcher used to answer several of those
//! questions itself, which is why it needed a completion channel back.
//!
//! It does not any more, so all that is left is one verb. The receiver is a
//! [`WatchDispatch`], which the watcher holds without knowing what is on the
//! other side — a test can drive a `WatchManager` with a recording
//! implementation and no supervisors at all.

use super::WatchItemKind;

/// Something a watcher watches told it that files changed.
///
/// Called from the watcher's own task after the item's debounce window
/// closes, so one save touching several watched files produces one call per
/// item, not one per file. Implementations must not block: the sole
/// implementation puts a message in a supervisor's mailbox.
pub(crate) trait WatchDispatch: Send + Sync {
    /// `name`'s watched files changed. For [`WatchItemKind::BuildGraph`] the
    /// name is the process's, not the synthetic `__graph` item's.
    fn changed(&self, name: &str, kind: WatchItemKind);
}

/// A dispatch that records instead of sending, for tests that want to see
/// what the watcher decided without standing up a supervisor.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct RecordingDispatch {
    pub(crate) calls: std::sync::Mutex<Vec<(String, WatchItemKind)>>,
}

#[cfg(test)]
impl WatchDispatch for RecordingDispatch {
    fn changed(&self, name: &str, kind: WatchItemKind) {
        #[allow(clippy::unwrap_used)]
        self.calls.lock().unwrap().push((name.to_string(), kind));
    }
}
