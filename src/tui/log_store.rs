//! Bounded store of formatted log lines for replay on filter change.
//!
//! The TUI uses `Terminal::insert_before` for live log flow, which pushes
//! lines into the terminal's native scrollback. We keep an in-memory copy
//! of recent lines so that when the filter changes — the one case where we
//! clear the screen — we can replay matching lines into the now-empty
//! viewport without losing them.
//!
//! Lines older than the capacity are evicted silently; the user's terminal
//! scrollback is a separate, larger store that we don't manage.

use std::collections::VecDeque;

use crate::output::FormattedLogLine;

/// Default cap. Large enough to cover a few screens of history at typical
/// terminal heights (80–120 rows) while bounding memory growth.
pub(crate) const DEFAULT_CAPACITY: usize = 5_000;

/// A bounded, time-ordered buffer of formatted log lines.
pub(crate) struct LogStore {
    entries: VecDeque<StoredLogLine>,
    capacity: usize,
    next_id: u64,
}

/// One stored log line plus its monotonically increasing sequence id.
pub(crate) struct StoredLogLine {
    pub(crate) id: u64,
    pub(crate) line: FormattedLogLine,
}

impl LogStore {
    /// Create a new store with the given capacity. A capacity of 0 silently
    /// drops every push.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity.min(DEFAULT_CAPACITY)),
            capacity,
            next_id: 0,
        }
    }

    /// Push a line, evicting the oldest if at capacity. Returns the line's
    /// sequence id.
    pub(crate) fn push(&mut self, line: FormattedLogLine) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        if self.capacity == 0 {
            return id;
        }
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(StoredLogLine { id, line });
        id
    }

    /// Iterate oldest-first. Used by filter-change replay to rerender
    /// matching lines into the freshly-cleared terminal.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &StoredLogLine> {
        self.entries.iter()
    }

    /// Id of the most recently stored line, if any.
    pub(crate) fn latest_id(&self) -> Option<u64> {
        self.next_id.checked_sub(1)
    }

    /// Number of lines currently stored.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn line(name: &str, body: &str) -> FormattedLogLine {
        FormattedLogLine {
            name: name.to_string(),
            bytes: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn push_grows_length_up_to_capacity() {
        let mut store = LogStore::with_capacity(10);
        store.push(line("a", "first"));
        store.push(line("b", "second"));
        store.push(line("a", "third"));
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn push_at_capacity_evicts_oldest() {
        let mut store = LogStore::with_capacity(3);
        store.push(line("a", "1"));
        store.push(line("a", "2"));
        store.push(line("a", "3"));
        store.push(line("a", "4"));

        assert_eq!(store.len(), 3);
    }

    #[test]
    fn zero_capacity_drops_every_push() {
        let mut store = LogStore::with_capacity(0);
        store.push(line("a", "1"));
        store.push(line("a", "2"));
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn latest_id_tracks_most_recent_push() {
        let mut store = LogStore::with_capacity(2);
        assert_eq!(store.latest_id(), None);
        assert_eq!(store.push(line("a", "1")), 0);
        assert_eq!(store.latest_id(), Some(0));
        assert_eq!(store.push(line("a", "2")), 1);
        assert_eq!(store.latest_id(), Some(1));
    }
}
