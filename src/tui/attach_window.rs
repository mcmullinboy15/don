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
//! Every keystroke belongs to the process, byte for byte, so don cannot claim
//! a chord for its own use — Ctrl+arrow is word-movement in every shell, and
//! stealing it would break the thing you attached to. don already holds
//! `Ctrl+P` as a prefix (that is how Ctrl+P Ctrl+Q detaches), so the window's
//! own commands live behind it, and a `Ctrl+P` followed by anything else
//! releases both bytes to the process untouched.

use ratatui::layout::Rect;

/// What a byte from stdin turned out to mean.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AttachInput {
    /// Forward these bytes to the process.
    Forward(Vec<u8>),
    /// Move the window one step.
    Move(Direction),
    /// Grow or shrink the window one step.
    Resize(Direction),
    /// Close the window, leaving the process running.
    Detach,
    /// The prefix is held; nothing to do until the next byte decides.
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Left,
    Right,
    Up,
    Down,
}

/// The docker-style prefix. A lone one is held, not forwarded, until the next
/// byte says what it meant.
const PREFIX: u8 = 0x10; // Ctrl+P
/// Detach, when it follows the prefix.
const DETACH: u8 = 0x11; // Ctrl+Q

/// Steps a move or resize takes per press.
const MOVE_STEP: u16 = 2;
const RESIZE_STEP: u16 = 2;

/// Smallest window worth drawing: below this a process has nowhere to put a
/// prompt, and the border eats what is left.
const MIN_COLS: u16 = 20;
const MIN_ROWS: u16 = 5;

/// Translates stdin bytes into either process input or window commands.
///
/// Stateful across calls because the prefix spans two bytes, and those two
/// bytes can arrive in different reads.
#[derive(Debug, Default)]
pub(crate) struct KeyRouter {
    holding_prefix: bool,
}

impl KeyRouter {
    /// Route one read of stdin.
    ///
    /// Returns in order: a byte that resolves a held prefix produces its
    /// command, and everything else accumulates into one `Forward`. A read
    /// containing both is possible — pasting, or a fast typist — so this
    /// returns a list rather than a single outcome.
    pub(crate) fn route(&mut self, bytes: &[u8]) -> Vec<AttachInput> {
        let mut out: Vec<AttachInput> = Vec::new();
        let mut forward: Vec<u8> = Vec::new();

        for &byte in bytes {
            if self.holding_prefix {
                self.holding_prefix = false;
                match byte {
                    DETACH => {
                        if !forward.is_empty() {
                            out.push(AttachInput::Forward(std::mem::take(&mut forward)));
                        }
                        out.push(AttachInput::Detach);
                        continue;
                    }
                    b'h' | b'j' | b'k' | b'l' | b'H' | b'J' | b'K' | b'L' => {
                        if !forward.is_empty() {
                            out.push(AttachInput::Forward(std::mem::take(&mut forward)));
                        }
                        let direction = match byte.to_ascii_lowercase() {
                            b'h' => Direction::Left,
                            b'j' => Direction::Down,
                            b'k' => Direction::Up,
                            _ => Direction::Right,
                        };
                        out.push(if byte.is_ascii_uppercase() {
                            AttachInput::Resize(direction)
                        } else {
                            AttachInput::Move(direction)
                        });
                        continue;
                    }
                    // Not a command: the held prefix was the process's after
                    // all, so it goes through ahead of this byte.
                    _ => {
                        forward.push(PREFIX);
                        if byte == PREFIX {
                            self.holding_prefix = true;
                            continue;
                        }
                        forward.push(byte);
                    }
                }
            } else if byte == PREFIX {
                self.holding_prefix = true;
            } else {
                forward.push(byte);
            }
        }

        if !forward.is_empty() {
            out.push(AttachInput::Forward(forward));
        }
        if out.is_empty() {
            out.push(AttachInput::Pending);
        }
        out
    }
}

/// Where the window sits and how big it is, in screen cells.
///
/// Kept as a rectangle rather than a docked extent because this one floats —
/// it is the only region of the TUI the reader positions freely.
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
    /// Big by default: the reason to attach is to use the process, and a
    /// prompt in a postage stamp helps nobody. Moving and resizing are there
    /// for when the log behind matters more.
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

    /// Move one step, staying inside `area`.
    pub(crate) fn moved(self, direction: Direction, area: Rect) -> Self {
        let mut next = self;
        match direction {
            Direction::Left => next.x = next.x.saturating_sub(MOVE_STEP),
            Direction::Right => next.x = next.x.saturating_add(MOVE_STEP),
            Direction::Up => next.y = next.y.saturating_sub(MOVE_STEP),
            Direction::Down => next.y = next.y.saturating_add(MOVE_STEP),
        }
        next.clamped_to(area)
    }

    /// Grow or shrink one step. Right/Down grow, Left/Up shrink — the edge
    /// being dragged is the bottom-right, so the window's own corner follows
    /// the direction pressed.
    pub(crate) fn resized(self, direction: Direction, area: Rect) -> Self {
        let mut next = self;
        match direction {
            Direction::Left => next.width = next.width.saturating_sub(RESIZE_STEP),
            Direction::Right => next.width = next.width.saturating_add(RESIZE_STEP),
            Direction::Up => next.height = next.height.saturating_sub(RESIZE_STEP),
            Direction::Down => next.height = next.height.saturating_add(RESIZE_STEP),
        }
        next.width = next.width.max(MIN_COLS);
        next.height = next.height.max(MIN_ROWS);
        next.clamped_to(area)
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

    /// The prefix belongs to the process unless the next byte claims it, and
    /// a read can carry both process input and a command.
    #[test]
    fn the_prefix_is_held_until_the_next_byte_decides() {
        struct Case {
            name: &'static str,
            reads: &'static [&'static [u8]],
            want: Vec<AttachInput>,
        }

        let cases = vec![
            Case {
                name: "ordinary input goes straight through",
                reads: &[b"ls -la\r"],
                want: vec![AttachInput::Forward(b"ls -la\r".to_vec())],
            },
            Case {
                name: "a lone prefix is held, not forwarded",
                reads: &[&[PREFIX]],
                want: vec![AttachInput::Pending],
            },
            Case {
                name: "prefix then detach",
                reads: &[&[PREFIX, DETACH]],
                want: vec![AttachInput::Detach],
            },
            Case {
                name: "prefix then a non-command releases both bytes",
                reads: &[&[PREFIX, b'x']],
                want: vec![AttachInput::Forward(vec![PREFIX, b'x'])],
            },
            Case {
                name: "the prefix can span two reads",
                reads: &[&[PREFIX], &[DETACH]],
                want: vec![AttachInput::Pending, AttachInput::Detach],
            },
            Case {
                name: "lowercase moves",
                reads: &[&[PREFIX, b'h']],
                want: vec![AttachInput::Move(Direction::Left)],
            },
            Case {
                name: "uppercase resizes",
                reads: &[&[PREFIX, b'L']],
                want: vec![AttachInput::Resize(Direction::Right)],
            },
            Case {
                name: "input before a command is forwarded first",
                reads: &[&[b'a', b'b', PREFIX, DETACH]],
                want: vec![AttachInput::Forward(b"ab".to_vec()), AttachInput::Detach],
            },
            Case {
                name: "a doubled prefix forwards one and holds the other",
                reads: &[&[PREFIX, PREFIX]],
                want: vec![AttachInput::Forward(vec![PREFIX])],
            },
        ];

        for case in cases {
            let mut router = KeyRouter::default();
            let mut got: Vec<AttachInput> = Vec::new();
            for read in case.reads {
                got.extend(router.route(read));
            }
            assert_eq!(got, case.want, "{}", case.name);
        }
    }

    /// The window stays on screen however it is pushed, and never shrinks
    /// below something a process could use.
    #[test]
    fn the_window_stays_on_screen_and_usable() {
        let area = Rect::new(0, 0, 100, 40);
        let start = WindowRect::centred_in(area);
        assert_eq!((start.width, start.height), (80, 32));
        assert_eq!((start.x, start.y), (10, 4));

        // Pushed hard left, it stops at the edge rather than wrapping.
        let mut w = start;
        for _ in 0..50 {
            w = w.moved(Direction::Left, area);
        }
        assert_eq!(w.x, 0, "flush to the left edge");

        // Shrunk hard, it stops at the minimum.
        let mut w = start;
        for _ in 0..100 {
            w = w.resized(Direction::Left, area);
            w = w.resized(Direction::Up, area);
        }
        assert_eq!((w.width, w.height), (MIN_COLS, MIN_ROWS));

        // Grown hard, it stops at the screen.
        let mut w = start;
        for _ in 0..100 {
            w = w.resized(Direction::Right, area);
            w = w.resized(Direction::Down, area);
        }
        assert!(w.width <= area.width && w.height <= area.height);
        assert!(w.x + w.width <= area.width && w.y + w.height <= area.height);
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
