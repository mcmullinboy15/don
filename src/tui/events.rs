//! Internal event type bridging crossterm input to the main TUI loop.
//!
//! The input task forwards raw key events; the main loop interprets them
//! based on the current [`ViewMode`](super::app::ViewMode). This keeps the
//! input pump oblivious to UI state, which avoids the Arc<Mutex<_>> dance.

use crossterm::event::KeyEvent;

/// Event delivered from the input task to the main TUI loop.
#[derive(Debug, Clone)]
pub(crate) enum AppEvent {
    /// A key press (release events are filtered out upstream).
    Key(KeyEvent),
    /// Terminal was resized. Ratatui picks up the new size on the next draw;
    /// this event just triggers an immediate repaint.
    Resize,
}
