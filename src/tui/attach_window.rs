//! The attached process, drawn as a floating window on don's own screen.
//!
//! The old attach handed the terminal over: the TUI left the alternate
//! screen, a raw byte pump owned stdin and stdout, and the dashboard came
//! back when you escaped. That handover is where several sharp edges lived —
//! the cursor-position probe, rebuilding the terminal on return, a full clear
//! every time — and it meant losing sight of everything else while attached.
//!
//! Here the connection is the same, but neither end touches the terminal.
//! Process output feeds a local emulator and is drawn cell by cell into a
//! window; the log keeps flowing behind it.
//!
//! ## Why the keys work the way they do
//!
//! Every keystroke belongs to the process except one. `Ctrl+D` detaches —
//! the same key the TUI already uses to leave a remote session, so it means
//! "back out of what I am in" at both levels.
//!
//! That is the whole vocabulary. There was a `Ctrl+P` prefix here, with chords
//! behind it for detaching and for moving and resizing the window; all of it
//! went, because a window you have to learn a modal grammar to leave is worse
//! than one you cannot move. `Ctrl+D` no longer reaches the process, so a
//! shell attached this way is left by detaching — and if it should actually
//! stop, by stopping it from the services or tasks list.
//!
//! Keys arrive here already parsed, from the same crossterm stream that feeds
//! the rest of the TUI, and [`super::keys::encode`] turns them back into bytes
//! — see that module for why that beats reading stdin raw.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::layout::Rect;

use super::keys::encode;

/// What a key press turned out to mean.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AttachInput {
    /// Forward these bytes to the process.
    Forward(Vec<u8>),
    /// Detach: close the window, leaving the process running.
    Detach,
    /// Nothing a terminal would have sent — a bare modifier press.
    Nothing,
}

/// Detach: close the window, leaving the process running.
const DETACH: char = 'd'; // with Ctrl

/// Smallest window worth drawing: below this a process has nowhere to put a
/// prompt, and the border eats what is left.
const MIN_COLS: u16 = 20;
const MIN_ROWS: u16 = 5;

/// What one key press means to an open window.
pub(crate) fn route(key: KeyEvent) -> AttachInput {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char(DETACH) {
        return AttachInput::Detach;
    }
    match encode(key) {
        Some(bytes) => AttachInput::Forward(bytes),
        None => AttachInput::Nothing,
    }
}

/// Where the window sits and how big it is, in screen cells.
///
/// Kept as a rectangle rather than a docked extent because this one floats: it
/// is placed over the log rather than beside it, so it needs an origin as well
/// as a size, and both have to survive the terminal changing shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WindowRect {
    pub(crate) x: u16,
    pub(crate) y: u16,
    pub(crate) width: u16,
    pub(crate) height: u16,
}

impl WindowRect {
    /// A window centred in `area`, taking most of it.
    ///
    /// Big, because the reason to attach is to use the process and a prompt in
    /// a postage stamp helps nobody — and fixed, because there is nothing to
    /// adjust it with any more.
    pub(crate) fn centred_in(area: Rect) -> Self {
        let width = (area.width.saturating_mul(4) / 5)
            .max(MIN_COLS)
            .min(area.width);
        let height = (area.height.saturating_mul(4) / 5)
            .max(MIN_ROWS)
            .min(area.height);
        Self {
            x: area.x + (area.width.saturating_sub(width)) / 2,
            y: area.y + (area.height.saturating_sub(height)) / 2,
            width,
            height,
        }
    }

    /// Put the window back inside `area` — for when the area moved rather
    /// than the window, which is what a terminal resize is.
    pub(crate) fn fitted(self, area: Rect) -> Self {
        self.clamped_to(area)
    }

    /// Keep the window on screen: shrink to fit, then pull it inside.
    fn clamped_to(mut self, area: Rect) -> Self {
        self.width = self.width.min(area.width).max(1);
        self.height = self.height.min(area.height).max(1);
        self.x = self
            .x
            .min(area.x + area.width.saturating_sub(self.width))
            .max(area.x);
        self.y = self
            .y
            .min(area.y + area.height.saturating_sub(self.height))
            .max(area.y);
        self
    }

    /// Whether a screen cell is the window's.
    pub(crate) fn contains(self, column: u16, row: u16) -> bool {
        column >= self.x
            && column < self.x.saturating_add(self.width)
            && row >= self.y
            && row < self.y.saturating_add(self.height)
    }

    pub(crate) fn to_rect(self) -> Rect {
        Rect::new(self.x, self.y, self.width, self.height)
    }

    /// The grid the process gets: the window minus its border.
    pub(crate) fn grid_size(self) -> (u16, u16) {
        (
            self.width.saturating_sub(2).max(1),
            self.height.saturating_sub(2).max(1),
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn plain(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    /// One key is don's; the rest are the process's, byte for byte.
    #[test]
    fn ctrl_d_detaches_and_everything_else_belongs_to_the_process() {
        struct Case {
            name: &'static str,
            key: KeyEvent,
            want: AttachInput,
        }

        let cases = [
            Case {
                name: "ctrl+d detaches",
                key: ctrl('d'),
                want: AttachInput::Detach,
            },
            Case {
                name: "a letter",
                key: plain(KeyCode::Char('l')),
                want: AttachInput::Forward(b"l".to_vec()),
            },
            Case {
                name: "enter",
                key: plain(KeyCode::Enter),
                want: AttachInput::Forward(b"\r".to_vec()),
            },
            Case {
                // The keys don claims everywhere else in the TUI still belong
                // to the process here — that is what attaching means.
                name: "ctrl+c interrupts the process, it does not stop don",
                key: ctrl('c'),
                want: AttachInput::Forward(vec![0x03]),
            },
            Case {
                name: "escape",
                key: plain(KeyCode::Esc),
                want: AttachInput::Forward(vec![0x1b]),
            },
            Case {
                name: "an arrow",
                key: plain(KeyCode::Up),
                want: AttachInput::Forward(b"\x1b[A".to_vec()),
            },
            Case {
                name: "a bare modifier press is not input",
                key: plain(KeyCode::CapsLock),
                want: AttachInput::Nothing,
            },
        ];

        for case in cases {
            assert_eq!(route(case.key), case.want, "{}", case.name);
        }
    }

    /// However the terminal changes shape, the window stays on screen and
    /// stays big enough for the process to draw in.
    #[test]
    fn the_window_stays_on_screen_and_usable() {
        let area = Rect::new(0, 0, 100, 40);
        let start = WindowRect::centred_in(area);
        assert_eq!((start.width, start.height), (80, 32));
        assert_eq!((start.x, start.y), (10, 4));

        // The terminal shrank under it.
        let smaller = Rect::new(0, 0, 40, 12);
        let fitted = start.fitted(smaller);
        assert!(fitted.width <= smaller.width && fitted.height <= smaller.height);
        assert!(fitted.x + fitted.width <= smaller.width);
        assert!(fitted.y + fitted.height <= smaller.height);

        // And grew again. Refitting never moves a window that already fits.
        assert_eq!(fitted.fitted(area), fitted);
    }

    /// A tiny terminal still produces a window that fits inside it.
    #[test]
    fn a_small_terminal_still_gets_a_window() {
        let area = Rect::new(0, 0, 24, 8);
        let w = WindowRect::centred_in(area);
        assert!(w.width <= area.width && w.height <= area.height);
        let (cols, rows) = w.grid_size();
        assert!(cols >= 1 && rows >= 1, "the process always gets a grid");
    }
}
