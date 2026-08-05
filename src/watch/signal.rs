//! The watch module's own vocabulary for talking to whoever owns it.
//!
//! Watching files is not a runner concern — it is "these paths changed, that
//! item should rebuild". Spelling that in [`crate::runner::RunnerCommand`]
//! made `watch` import from `runner` while `runner` constructs and drives
//! `watch`, which is a cycle for no gain: only four of the ~19 runner commands
//! and three of its events were ever involved.
//!
//! So the watcher speaks [`WatchSignal`] out and [`WatchOutcome`] in, and the
//! owner translates. Today that owner is the runner
//! ([`crate::runner::watch_link`]), but nothing here knows that — which is the
//! point. A test can drive a `WatchManager` with two plain channels and no
//! runner at all.

/// Something changed on disk and an item should act on it.
///
/// Emitted after the item's debounce window closes, so one save that touches
/// several watched files produces one signal per item, not one per file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WatchSignal {
    /// A service's watched files changed — rebuild and restart it.
    Rebuild { name: String },
    /// A task's watched files changed — run it again.
    TaskRerun { name: String },
    /// Files changed *while* this item was already rebuilding, so the build
    /// in flight is producing a stale artifact.
    ///
    /// Separate from [`Rebuild`](Self::Rebuild) because the owner cannot act
    /// on it yet: it records the staleness and re-fires once the in-flight
    /// cycle reports back. Sending `Rebuild` here instead would start a second
    /// build against the first one.
    RebuildStale { name: String },
    /// A build-tool definition file changed (`BUILD`, `MODULE.bazel`, …), so
    /// the item's watch paths may no longer be right and need re-querying.
    ///
    /// Unlike the others this has no completion: the owner re-queries
    /// asynchronously and pushes new patterns back through `WatchUpdate`.
    BuildGraphChanged { name: String },
}

/// The result of work this watcher asked for.
///
/// Only rebuilds and re-runs report back, and only because the watcher gates
/// on them: an item stays in [`WatchState::Rebuilding`] — swallowing further
/// edits into a `stale` flag — until its outcome lands. Missing one strands
/// that item, which is why [`Lagged`](Self::Lagged) is explicit rather than
/// silent.
///
/// [`WatchState::Rebuilding`]: super::WatchState::Rebuilding
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WatchOutcome {
    /// A service finished rebuilding, successfully or not.
    ///
    /// `success` is carried for diagnostics only — the watcher clears
    /// `Rebuilding` either way, because a failed build still ends the cycle
    /// and the next edit must be able to start a new one.
    RebuildComplete { name: String, success: bool },
    /// A task finished re-running. Same contract as
    /// [`RebuildComplete`](Self::RebuildComplete).
    TaskRerunComplete { name: String, success: bool },
    /// `n` outcomes were dropped in transit and will never arrive.
    ///
    /// The owner's event stream is lossy under load, and a dropped completion
    /// leaves an item stuck in `Rebuilding` until it is re-registered. The
    /// watcher cannot recover the lost names, so it reports the gap loudly
    /// instead of appearing to work.
    Lagged(u64),
}
