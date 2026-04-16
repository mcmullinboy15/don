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
//! We never actually query the terminal for the cursor. Instead,
//! [`FixedBottomBackend::get_cursor_position`] returns `(0, screen_height - 1)`
//! — the bottom-left of the current screen. Combined with moving the real
//! cursor to the bottom row before `Terminal::with_options` and on every
//! resize, `compute_inline_size`'s math produces exactly the viewport we
//! want (pinned to the bottom of the screen).
//!
//! Every other `Backend` method delegates to the inner [`CrosstermBackend`].

use std::io::{self, Write};

use ratatui::backend::{Backend, ClearType, CrosstermBackend, WindowSize};
use ratatui::buffer::Cell;
use ratatui::layout::{Position, Size};

/// Wraps `CrosstermBackend` and returns a deterministic cursor position
/// instead of reading from stdin. See the module doc for why.
pub(super) struct FixedBottomBackend<W: Write> {
    inner: CrosstermBackend<W>,
}

impl<W: Write> FixedBottomBackend<W> {
    pub(super) fn new(inner: CrosstermBackend<W>) -> Self {
        Self { inner }
    }
}

impl<W: Write> Backend for FixedBottomBackend<W> {
    fn get_cursor_position(&mut self) -> io::Result<Position> {
        // The whole point of this wrapper: never do a DSR. Return the
        // bottom-left cell so ratatui's inline viewport math pins the
        // viewport to the bottom of the screen.
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
