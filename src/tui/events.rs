//! Internal event type bridging crossterm input to the main TUI loop.
//!
//! The input task forwards raw key events; the main loop interprets them
//! based on the current [`ViewMode`](super::app::ViewMode). This keeps the
//! input pump oblivious to UI state, which avoids the Arc<Mutex<_>> dance.

use crossterm::event::KeyEvent;

use crate::runner::CompletionError;

/// Event delivered from the input task to the main TUI loop.
#[derive(Debug, Clone)]
pub(crate) enum AppEvent {
    /// A key press (release events are filtered out upstream).
    Key(KeyEvent),
    /// Terminal was resized. Ratatui picks up the new size on the next draw;
    /// this event just triggers an immediate repaint.
    Resize,
    /// Async result of a `RunnerCommand::ResolveCompletions` request.
    /// Delivered back into the main TUI loop from a detached tokio task —
    /// that way a slow completion command doesn't stall rendering or key
    /// handling.
    CompletionsReady {
        /// The param name the result belongs to. The form ignores the event
        /// if the user has since moved past or cancelled this field.
        param: String,
        /// Token identifying the specific request. Lets the form drop stale
        /// replies (e.g. user typed faster than the resolver returned).
        request_id: u64,
        /// Either the list of candidate values, or the resolver's error
        /// (which the form renders inline with a pointer to the log file).
        result: Result<Vec<String>, CompletionError>,
    },
}
