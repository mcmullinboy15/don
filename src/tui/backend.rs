//! Thin backend wrapper that dodges ratatui's DSR (cursor position query)
//! on inline-viewport autoresize.
//!
//! ## The problem
//!
//! `CrosstermBackend::get_cursor_position` sends `\x1b[6n` and reads the
//! response from stdin via crossterm's internal event reader. That reader
//! is shared with [`EventStream`], which our input task owns. When
//! `crossterm::event::poll` / `read` is running concurrently — which is
//! **always**, because the input task is continuously awaiting key events —
//! the DSR response races for the queue. `position()` then times out after
//! 2 seconds with "The cursor position could not be read within a normal
//! duration", and since ratatui calls `get_cursor_position` on every
//! `Terminal::draw` that triggers `autoresize` (i.e. every terminal resize),
//! the TUI errors out.
//!
//! ## The fix
//!
//! - **First** call (during initial `Terminal::with_options`): do a real
//!   DSR. It's safe here because the input task hasn't spawned yet, so no
//!   contention on stdin. Using the real cursor anchors the viewport just
//!   below the shell's pre-start output, which keeps scrollback clean.
//! - **Every subsequent** call (autoresize on terminal resize): return
//!   `(0, screen_height - 1)` — the bottom-left cell — so the viewport
//!   pins to the bottom of the screen. The caller must `park_cursor_at_bottom`
//!   before the draw that triggers autoresize, so that `append_lines` runs
//!   at the screen's bottom row (where `\n` actually scrolls).
//!
//! Every other `Backend` method delegates to the inner [`CrosstermBackend`].

use std::io::{self, Write};

use ratatui::backend::{Backend, ClearType, CrosstermBackend, WindowSize};
use ratatui::buffer::Cell;
use ratatui::layout::{Position, Size};

/// Wraps `CrosstermBackend` with safe `get_cursor_position` semantics.
pub(super) struct FixedBottomBackend<W: Write> {
    inner: CrosstermBackend<W>,
    /// `true` after the first `get_cursor_position` call. The first call is
    /// always during `Terminal::with_options` (before the input task spawns)
    /// so it's safe to do a real DSR. Later calls come from autoresize and
    /// have to be faked to avoid the stdin race.
    initial_dsr_done: bool,
    /// One-shot override for the next `get_cursor_position` call. Set by
    /// `force_next_cursor_top` so `clear_and_replay` can re-anchor the
    /// inline viewport at the top of the screen via `Terminal::resize`,
    /// making replayed content flow from top → down instead of pinning
    /// the bar to the bottom.
    next_override: Option<Position>,
}

impl<W: Write> FixedBottomBackend<W> {
    pub(super) fn new(inner: CrosstermBackend<W>) -> Self {
        Self {
            inner,
            initial_dsr_done: false,
            next_override: None,
        }
    }

    /// Tell the wrapper to report `(0, 0)` from the next
    /// `get_cursor_position` call. Consumed on use — subsequent calls go
    /// back to the normal fake-bottom behavior. The caller is responsible
    /// for moving the real cursor to `(0, 0)` first so `append_lines`
    /// inside `compute_inline_size` writes at the matching row.
    pub(super) fn force_next_cursor_top(&mut self) {
        self.next_override = Some(Position { x: 0, y: 0 });
    }
}

impl<W: Write> Backend for FixedBottomBackend<W> {
    type Error = io::Error;

    fn get_cursor_position(&mut self) -> io::Result<Position> {
        // One-shot override takes precedence. Used by `clear_and_replay`
        // to place the viewport at the top before replaying matching
        // log lines.
        if let Some(pos) = self.next_override.take() {
            return Ok(pos);
        }
        if !self.initial_dsr_done {
            // First call — this is the initial Terminal construction, before
            // the input task is spawned, so a real DSR is race-free. Using
            // the shell's actual cursor position anchors the viewport right
            // below existing output (no blank gap in scrollback).
            self.initial_dsr_done = true;
            return self.inner.get_cursor_position();
        }
        // Later calls come from autoresize. Fake "bottom of screen" so the
        // viewport pins to the bottom after a resize. Callers must
        // `park_cursor_at_bottom` before the triggering draw so that
        // `append_lines` writes its newlines at the actual bottom row.
        let size = self.inner.size()?;
        Ok(Position {
            x: 0,
            y: size.height.saturating_sub(1),
        })
    }

    // --- everything below is a straight delegate ---

    fn draw<'a, I>(&mut self, content: I) -> io::Result<()>
    where
        I: Iterator<Item = (u16, u16, &'a Cell)>,
    {
        self.inner.draw(content)
    }

    fn append_lines(&mut self, n: u16) -> io::Result<()> {
        self.inner.append_lines(n)
    }

    fn hide_cursor(&mut self) -> io::Result<()> {
        self.inner.hide_cursor()
    }

    fn show_cursor(&mut self) -> io::Result<()> {
        self.inner.show_cursor()
    }

    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        self.inner.set_cursor_position(position)
    }

    fn clear(&mut self) -> io::Result<()> {
        self.inner.clear()
    }

    fn clear_region(&mut self, clear_type: ClearType) -> io::Result<()> {
        self.inner.clear_region(clear_type)
    }

    fn size(&self) -> io::Result<Size> {
        self.inner.size()
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        self.inner.window_size()
    }

    fn flush(&mut self) -> io::Result<()> {
        // CrosstermBackend also implements `std::io::Write::flush`; disambiguate.
        Backend::flush(&mut self.inner)
    }
}
