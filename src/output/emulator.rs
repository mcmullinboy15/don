//! Server-side terminal emulation for PTY-backed processes.
//!
//! One screen per PTY-backed process, fed continuously from process start — a
//! correct screen requires having seen the setup sequences, so feeding
//! cannot begin lazily on attach. The PTY byte stream forks: raw chunks go
//! here (the bridge view) and through the existing sanitize pipeline into
//! the ring buffer (the log view). Attaching produces a coherent repaint of
//! the current grid followed by the live stream — never a raw-byte replay.
//!
//! The emulator (ghostty's VT core, via `libghostty-vt`) is not `Send`, so
//! every screen lives on one dedicated OS thread and the rest of don talks
//! to it through [`EmulatorHandle`]'s channels. Feeding is fire-and-forget
//! (an unbounded send per output chunk); repaints are request/reply.
//!
//! The backend is wrapped in the [`Screen`] trait so the emulator stays
//! swappable if the packaging story (Zig at build time) ever changes.

use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

/// What the emulator thread can be asked to do.
pub(crate) enum EmulatorRequest {
    /// (Re)create the screen for a process — a fresh spawn starts blank.
    Register { name: String, cols: u16, rows: u16 },
    /// Feed output bytes into the process's screen.
    Feed { name: String, bytes: Vec<u8> },
    /// Resize the process's grid.
    Resize { name: String, cols: u16, rows: u16 },
    /// Render the current grid as an ANSI repaint frame.
    Repaint {
        name: String,
        reply: oneshot::Sender<Option<RepaintFrame>>,
    },
}

/// A coherent ANSI rendering of a process's current screen: clear, every row
/// with its styles, then cursor position and visibility. Writing this to a
/// blank terminal reproduces the grid.
#[derive(Debug, Clone)]
pub struct RepaintFrame {
    /// The ANSI byte stream.
    pub bytes: Vec<u8>,
}

/// Cloneable handle to the emulator thread.
#[derive(Clone)]
pub struct EmulatorHandle {
    tx: mpsc::UnboundedSender<EmulatorRequest>,
}

impl EmulatorHandle {
    /// (Re)register a process's screen at the given size.
    pub(crate) fn register(&self, name: &str, cols: u16, rows: u16) {
        let _ = self.tx.send(EmulatorRequest::Register {
            name: name.to_string(),
            cols,
            rows,
        });
    }

    /// Resize a process's screen.
    pub(crate) fn resize(&self, name: &str, cols: u16, rows: u16) {
        let _ = self.tx.send(EmulatorRequest::Resize {
            name: name.to_string(),
            cols,
            rows,
        });
    }

    /// Render a process's current screen. `None` when the process has no screen
    /// (never registered, or the emulator backend failed) or the thread is
    /// gone.
    pub(crate) async fn repaint(&self, name: &str) -> Option<RepaintFrame> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(EmulatorRequest::Repaint {
                name: name.to_string(),
                reply: reply_tx,
            })
            .ok()?;
        reply_rx.await.ok().flatten()
    }

    /// The raw feed sender, for wiring into a process's sink list.
    pub(crate) fn feed_sender(&self) -> mpsc::UnboundedSender<EmulatorRequest> {
        self.tx.clone()
    }
}

/// Start the emulator thread. Returns immediately; screens are created on
/// demand via [`EmulatorHandle::register`].
pub(crate) fn spawn_emulator_thread() -> EmulatorHandle {
    let (tx, rx) = mpsc::unbounded_channel();
    // If the thread cannot be spawned the handle's sends fail silently and
    // repaints return `None` — attach degrades to no-repaint rather than
    // taking don down.
    let _ = std::thread::Builder::new()
        .name("term-emulator".to_string())
        .spawn(move || emulator_loop(rx));
    EmulatorHandle { tx }
}

fn emulator_loop(mut rx: mpsc::UnboundedReceiver<EmulatorRequest>) {
    let mut screens: HashMap<String, Box<dyn Screen>> = HashMap::new();
    while let Some(request) = rx.blocking_recv() {
        match request {
            EmulatorRequest::Register { name, cols, rows } => {
                match GhosttyScreen::new(cols, rows) {
                    Ok(screen) => {
                        screens.insert(name, Box::new(screen));
                    }
                    Err(_) => {
                        // No screen: feeds for this name are dropped and
                        // repaints answer None. Attach still works, just
                        // without a repaint.
                        screens.remove(&name);
                    }
                }
            }
            EmulatorRequest::Feed { name, bytes } => {
                if let Some(screen) = screens.get_mut(&name) {
                    screen.feed(&bytes);
                }
            }
            EmulatorRequest::Resize { name, cols, rows } => {
                if let Some(screen) = screens.get_mut(&name) {
                    screen.resize(cols, rows);
                }
            }
            EmulatorRequest::Repaint { name, reply } => {
                let frame = screens.get(&name).and_then(|screen| screen.repaint());
                let _ = reply.send(frame);
            }
        }
    }
}

/// A server-side terminal screen. Object-safe so the backend is swappable.
trait Screen {
    fn feed(&mut self, bytes: &[u8]);
    fn resize(&mut self, cols: u16, rows: u16);
    fn repaint(&self) -> Option<RepaintFrame>;
}

/// The ghostty-backed screen.
struct GhosttyScreen {
    term: libghostty_vt::terminal::Terminal<'static, 'static>,
}

/// Scrollback kept by the bridge view. Deliberately small — the ring buffer
/// remains the deep-history knob; this covers scroll-up during a session.
const BRIDGE_SCROLLBACK_LINES: usize = 2_000;

impl GhosttyScreen {
    fn new(cols: u16, rows: u16) -> Result<Self, libghostty_vt::error::Error> {
        let term = libghostty_vt::terminal::Terminal::new(libghostty_vt::terminal::Options {
            cols: cols.max(1),
            rows: rows.max(1),
            max_scrollback: BRIDGE_SCROLLBACK_LINES,
        })?;
        Ok(Self { term })
    }
}

impl Screen for GhosttyScreen {
    fn feed(&mut self, bytes: &[u8]) {
        self.term.vt_write(bytes);
    }

    fn resize(&mut self, cols: u16, rows: u16) {
        let _ = self.term.resize(cols.max(1), rows.max(1), 0, 0);
    }

    fn repaint(&self) -> Option<RepaintFrame> {
        render_repaint(&self.term).ok()
    }
}

/// Walk the viewport grid and emit an ANSI frame reproducing it.
fn render_repaint(
    term: &libghostty_vt::terminal::Terminal<'_, '_>,
) -> Result<RepaintFrame, libghostty_vt::error::Error> {
    use libghostty_vt::screen::CellWide;
    use libghostty_vt::terminal::{Point, PointCoordinate};

    let cols = term.cols()?;
    let rows = term.rows()?;

    let mut bytes: Vec<u8> = Vec::with_capacity(usize::from(cols) * usize::from(rows) * 2);
    // Clear, home, reset attributes.
    bytes.extend_from_slice(b"\x1b[2J\x1b[H\x1b[0m");

    let mut current_sgr = String::new();
    let mut grapheme_buf = [char::REPLACEMENT_CHARACTER; 16];
    for y in 0..rows {
        if y > 0 {
            bytes.extend_from_slice(b"\r\n");
        }
        for x in 0..cols {
            let grid_ref =
                term.grid_ref(Point::Viewport(PointCoordinate { x, y: u32::from(y) }))?;
            let cell = grid_ref.cell()?;
            if matches!(cell.wide()?, CellWide::SpacerTail | CellWide::SpacerHead) {
                continue;
            }
            let sgr = sgr_for(&grid_ref.style()?);
            if sgr != current_sgr {
                bytes.extend_from_slice(sgr.as_bytes());
                current_sgr = sgr;
            }
            let written = match grid_ref.graphemes(&mut grapheme_buf) {
                Ok(len) => len.min(grapheme_buf.len()),
                Err(_) => 0,
            };
            if written == 0 {
                bytes.push(b' ');
            } else {
                let mut utf8 = [0u8; 4];
                for ch in &grapheme_buf[..written] {
                    bytes.extend_from_slice(ch.encode_utf8(&mut utf8).as_bytes());
                }
            }
        }
    }

    // Reset attributes, place the cursor, restore its visibility.
    bytes.extend_from_slice(b"\x1b[0m");
    let cursor_row = term.cursor_y()?.saturating_add(1);
    let cursor_col = term.cursor_x()?.saturating_add(1);
    bytes.extend_from_slice(format!("\x1b[{cursor_row};{cursor_col}H").as_bytes());
    if term.is_cursor_visible()? {
        bytes.extend_from_slice(b"\x1b[?25h");
    } else {
        bytes.extend_from_slice(b"\x1b[?25l");
    }

    Ok(RepaintFrame { bytes })
}

/// Translate a cell style into one SGR sequence (from a reset baseline).
fn sgr_for(style: &libghostty_vt::style::Style) -> String {
    use libghostty_vt::style::{StyleColor, Underline};

    let mut params: Vec<String> = vec!["0".to_string()];
    if style.bold {
        params.push("1".to_string());
    }
    if style.faint {
        params.push("2".to_string());
    }
    if style.italic {
        params.push("3".to_string());
    }
    match style.underline {
        Underline::Single => params.push("4".to_string()),
        Underline::Double => params.push("21".to_string()),
        Underline::Curly => params.push("4:3".to_string()),
        Underline::Dotted => params.push("4:4".to_string()),
        Underline::Dashed => params.push("4:5".to_string()),
        // Non-exhaustive upstream enum: unknown kinds render un-underlined.
        _ => {}
    }
    if style.blink {
        params.push("5".to_string());
    }
    if style.inverse {
        params.push("7".to_string());
    }
    if style.invisible {
        params.push("8".to_string());
    }
    if style.strikethrough {
        params.push("9".to_string());
    }
    if style.overline {
        params.push("53".to_string());
    }
    match style.fg_color {
        StyleColor::None => {}
        StyleColor::Palette(index) => {
            let n = index.0;
            if n < 8 {
                params.push(format!("{}", 30 + u16::from(n)));
            } else if n < 16 {
                params.push(format!("{}", 90 + u16::from(n) - 8));
            } else {
                params.push(format!("38;5;{n}"));
            }
        }
        StyleColor::Rgb(rgb) => {
            params.push(format!("38;2;{};{};{}", rgb.r, rgb.g, rgb.b));
        }
    }
    match style.bg_color {
        StyleColor::None => {}
        StyleColor::Palette(index) => {
            let n = index.0;
            if n < 8 {
                params.push(format!("{}", 40 + u16::from(n)));
            } else if n < 16 {
                params.push(format!("{}", 100 + u16::from(n) - 8));
            } else {
                params.push(format!("48;5;{n}"));
            }
        }
        StyleColor::Rgb(rgb) => {
            params.push(format!("48;2;{};{};{}", rgb.r, rgb.g, rgb.b));
        }
    }

    format!("\x1b[{}m", params.join(";"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// Drive the whole thread round trip: register, feed, repaint — and
    /// verify the frame reproduces content, styles, and cursor placement.
    #[tokio::test]
    async fn repaint_reproduces_grid_and_styles() {
        let handle = spawn_emulator_thread();
        handle.register("svc", 20, 4);
        let _ = handle.feed_sender().send(EmulatorRequest::Feed {
            name: "svc".to_string(),
            bytes: b"hello \x1b[31mred\x1b[0m\r\nline2".to_vec(),
        });

        let frame = handle.repaint("svc").await.expect("screen registered");
        let text = String::from_utf8_lossy(&frame.bytes).to_string();
        assert!(text.contains("hello "), "plain text present");
        assert!(text.contains("red"), "styled text present");
        assert!(text.contains("[31m") || text.contains(";31m"), "red SGR");
        // Cursor after "line2" on the second row: row 2, col 6.
        assert!(
            text.ends_with("\u{1b}[2;6H\u{1b}[?25h"),
            "cursor restored: {text:?}"
        );
    }

    /// A repaint for an unregistered name answers None instead of hanging.
    #[tokio::test]
    async fn unregistered_repaint_is_none() {
        let handle = spawn_emulator_thread();
        assert!(handle.repaint("ghost").await.is_none());
    }

    /// Re-registering resets the screen — a fresh spawn starts blank.
    #[tokio::test]
    async fn reregister_resets_screen() {
        let handle = spawn_emulator_thread();
        handle.register("svc", 10, 2);
        let _ = handle.feed_sender().send(EmulatorRequest::Feed {
            name: "svc".to_string(),
            bytes: b"old".to_vec(),
        });
        handle.register("svc", 10, 2);
        let frame = handle.repaint("svc").await.expect("screen registered");
        assert!(
            !String::from_utf8_lossy(&frame.bytes).contains("old"),
            "restart must not leak the previous process's screen"
        );
    }
}
