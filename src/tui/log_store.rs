//! The TUI's copy of don's merged log stream.
//!
//! Every line don sends this client lands here, unfiltered and unabridged.
//! What the user sees is a view over it (see [`super::logs`]); the store itself
//! never decides what is worth keeping, because the alternative — filtering at
//! ingest — makes widening a filter reveal nothing, since the lines it would
//! have shown were discarded on the way in.
//!
//! Entries are keyed by don's own [`LogId`], not by a number this store mints.
//! That is what lets two clients refer to the same line, lets a reconnecting
//! client ask for exactly what it missed, and lets a gap be reported as a
//! measured hole rather than a silent thinning.
//!
//! ## What is cached, and why
//!
//! Two things are expensive per frame and cheap per line:
//!
//! - **Parsing.** Upstream hands us pre-rendered ANSI bytes. Turning those into
//!   a styled [`Line`] is done once, at push. A full-screen repaint touches
//!   every visible line every frame, so parsing at render would tie frame cost
//!   to screen height *and* re-do identical work forever.
//! - **Wrapped row counts.** How many rows a line occupies depends on the pane
//!   width, and the pane needs the total before it draws — to place the scroll
//!   anchor and size the scrollbar. Recomputed only when the width changes.

use std::collections::VecDeque;

use ratatui::text::Line;

use crate::output::{FormattedLogLine, LogId};

/// Default cap on retained lines.
///
/// This is the TUI's scrollback now, not a replay cache for the terminal's own
/// history — there is no terminal history to fall back on in the alternate
/// screen. Sized to match don's merged store, so what the user can scroll to is
/// what don still holds.
pub(crate) const DEFAULT_CAPACITY: usize = crate::output::DEFAULT_MERGED_HISTORY_CAPACITY;

/// A bounded, time-ordered buffer of the merged stream.
pub(crate) struct LogStore {
    entries: VecDeque<StoredLogLine>,
    capacity: usize,
    next_id: LogId,
    /// The width `wrapped_rows` was computed against. `None` before the first
    /// reflow.
    wrapped_at: Option<u16>,
}

/// One stored line: what don sent, parsed once, measured once.
pub(crate) struct StoredLogLine {
    pub(crate) id: LogId,
    pub(crate) line: FormattedLogLine,
    /// The styled form, parsed at push. Owned (`'static`) so the store can hand
    /// it out without tying the borrow to the raw bytes.
    pub(crate) parsed: Line<'static>,
    /// Rows this line occupies at the store's current wrap width.
    wrapped_rows: usize,
}

impl StoredLogLine {
    /// Rows this line occupies at the width the store last reflowed to.
    pub(crate) fn wrapped_rows(&self) -> usize {
        self.wrapped_rows
    }
}

impl LogStore {
    /// Create a new store with the given capacity. A capacity of 0 silently
    /// drops every push.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            capacity,
            next_id: LogId::ZERO,
            wrapped_at: None,
        }
    }

    /// Store a line under the id don's merged stream gave it, evicting the
    /// oldest if at capacity.
    pub(crate) fn push(&mut self, id: LogId, line: FormattedLogLine) {
        self.next_id = LogId(id.0.saturating_add(1));
        if self.capacity == 0 {
            return;
        }
        while self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        let parsed = super::parse_ansi_line(&line.bytes);
        let wrapped_rows = match self.wrapped_at {
            Some(width) => super::logs::count_wrapped_rows(&parsed, width),
            None => 1,
        };
        self.entries.push_back(StoredLogLine {
            id,
            line,
            parsed,
            wrapped_rows,
        });
    }

    /// Recompute cached row counts for a new pane width.
    ///
    /// A no-op when the width has not moved, which is every frame but the ones
    /// straddling a resize.
    pub(crate) fn reflow(&mut self, width: u16) {
        if self.wrapped_at == Some(width) {
            return;
        }
        self.wrapped_at = Some(width);
        for entry in &mut self.entries {
            entry.wrapped_rows = super::logs::count_wrapped_rows(&entry.parsed, width);
        }
    }

    /// Iterate oldest-first over everything held.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &StoredLogLine> {
        self.entries.iter()
    }

    /// The oldest id still held, for a reader deciding what it has missed.
    pub(crate) fn oldest_id(&self) -> Option<LogId> {
        self.entries.front().map(|entry| entry.id)
    }

    /// Iterate from the first entry with `id >= from`.
    ///
    /// A binary search rather than a scan: ids ascend, and the callers that
    /// want this — the view index mending itself, the renderer taking the
    /// visible window — would otherwise walk the whole store to reach a
    /// screenful near the end of it.
    pub(crate) fn iter_from(&self, from: LogId) -> impl Iterator<Item = &StoredLogLine> {
        let at = self.entries.partition_point(|entry| entry.id < from);
        self.entries.iter().skip(at)
    }

    /// The line stored under `id`, if it is still held.
    ///
    /// Ids ascend, so this is a binary search. Used by the pane to fetch the
    /// lines the view index selected, rather than re-deciding which lines those
    /// are — see [`super::logs::build_view`].
    pub(crate) fn get(&self, id: LogId) -> Option<&StoredLogLine> {
        let at = self.entries.partition_point(|entry| entry.id < id);
        self.entries.get(at).filter(|entry| entry.id == id)
    }

    /// Id the next stream line is expected to have — what a reconnecting
    /// client asks to resume from.
    pub(crate) fn next_id(&self) -> LogId {
        self.next_id
    }

    /// Id of the most recently stored line, if any.
    #[cfg(test)]
    pub(crate) fn latest_id(&self) -> Option<LogId> {
        self.entries.back().map(|entry| entry.id)
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
            is_lifecycle: false,
            is_verbose: false,
            bytes: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn push_grows_length_up_to_capacity() {
        let mut store = LogStore::with_capacity(10);
        store.push(LogId(0), line("a", "first"));
        store.push(LogId(1), line("b", "second"));
        store.push(LogId(2), line("a", "third"));
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn push_at_capacity_evicts_oldest() {
        let mut store = LogStore::with_capacity(3);
        for id in 0..4 {
            store.push(LogId(id), line("a", "x"));
        }
        assert_eq!(store.len(), 3);
        assert_eq!(
            store.iter().next().map(|entry| entry.id),
            Some(LogId(1)),
            "the oldest goes first"
        );
    }

    #[test]
    fn zero_capacity_drops_every_push() {
        let mut store = LogStore::with_capacity(0);
        store.push(LogId(0), line("a", "1"));
        store.push(LogId(1), line("a", "2"));
        assert_eq!(store.len(), 0);
    }

    /// Ids come from don's merged stream, so the store carries whatever it is
    /// given rather than counting for itself — a client resuming mid-stream
    /// starts at a non-zero id, and one that lost lines skips.
    #[test]
    fn stored_ids_are_the_streams_own() {
        let mut store = LogStore::with_capacity(10);
        assert_eq!(store.latest_id(), None);
        assert_eq!(store.next_id(), LogId::ZERO);

        store.push(LogId(500), line("a", "resumed"));
        assert_eq!(store.latest_id(), Some(LogId(500)));
        assert_eq!(store.next_id(), LogId(501));

        store.push(LogId(900), line("a", "after a drop"));
        assert_eq!(store.latest_id(), Some(LogId(900)));
        assert_eq!(store.iter().count(), 2, "both survive; ids simply skip");
    }

    /// The pane needs a row count before it draws, so the store measures at
    /// push and re-measures only when the width moves.
    #[test]
    fn row_counts_track_the_wrap_width() {
        struct Case {
            name: &'static str,
            width: u16,
            want_rows: usize,
        }

        let cases = vec![
            Case {
                name: "wide enough for one row",
                width: 40,
                want_rows: 1,
            },
            Case {
                name: "half the width doubles it",
                width: 10,
                want_rows: 2,
            },
            Case {
                name: "a quarter quadruples it",
                width: 5,
                want_rows: 4,
            },
        ];

        for case in cases {
            let mut store = LogStore::with_capacity(10);
            store.reflow(case.width);
            store.push(LogId(0), line("a", "12345678901234567890"));
            assert_eq!(
                store.iter().next().unwrap().wrapped_rows(),
                case.want_rows,
                "{}: at push",
                case.name
            );

            // And a line pushed before the width was known catches up on
            // reflow rather than staying wrong.
            let mut later = LogStore::with_capacity(10);
            later.push(LogId(0), line("a", "12345678901234567890"));
            later.reflow(case.width);
            assert_eq!(
                later.iter().next().unwrap().wrapped_rows(),
                case.want_rows,
                "{}: after reflow",
                case.name
            );
        }
    }

    /// Parsing happens once, at push — a repaint touches every visible line
    /// every frame, so doing it at render would tie frame cost to screen height
    /// and redo identical work forever.
    #[test]
    fn ansi_is_parsed_into_styles_at_push() {
        let mut store = LogStore::with_capacity(10);
        store.push(LogId(0), line("a", "\x1b[31mred\x1b[0m plain"));
        let entry = store.iter().next().unwrap();
        assert!(
            entry.parsed.spans.len() >= 2,
            "the escape should have split the line into styled spans"
        );
        assert_eq!(
            entry.parsed.spans[0].style.fg,
            Some(ratatui::style::Color::Red),
            "and the colour should have survived"
        );
    }
}
