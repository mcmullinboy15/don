//! Turn a parsed key event back into the bytes a terminal would have sent.
//!
//! The attach window needs stdin bytes; don's event loop has `KeyEvent`s. The
//! obvious shortcut is to stop parsing while attached and read stdin raw — that
//! is what this used to do, and it was worse in every way that mattered:
//!
//! - **Two readers, one stdin.** Handing stdin between crossterm's event
//!   stream and a raw reader loses whichever bytes are in flight at the
//!   handover, so the first keystroke after attaching or detaching would
//!   vanish. A blocking `read` also cannot be cancelled by dropping it, so the
//!   old reader stayed parked on stdin and could still steal a byte.
//! - **Mouse reporting doesn't turn off.** don asks the terminal for SGR mouse
//!   events; raw forwarding shoved those escape sequences at a process that
//!   never asked for them, so moving the pointer typed garbage at the prompt.
//! - **Nothing else worked while attached.** No resize, no scrolling the log
//!   behind the window, none of don's own keys, because the only reader was
//!   the one forwarding to the process.
//!
//! Re-encoding costs one table. Everything above stops being a problem,
//! because there is exactly one reader of stdin for the life of the TUI.
//!
//! The encoding is xterm's, which is what crossterm parses and what every
//! terminal program expects: `CSI` sequences for the navigation and function
//! keys, control bytes for `Ctrl`, and an `ESC` prefix for `Alt`.

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

/// The bytes for one key press, or `None` for keys a terminal wouldn't send —
/// releases, and the modifier/media keys that only exist under the kitty
/// protocol.
pub(crate) fn encode(key: KeyEvent) -> Option<Vec<u8>> {
    if key.kind == KeyEventKind::Release {
        return None;
    }

    let m = key.modifiers;
    match key.code {
        KeyCode::Char(c) => Some(char_bytes(c, m)),
        KeyCode::Enter => Some(alt_prefixed(vec![b'\r'], m)),
        KeyCode::Tab => Some(alt_prefixed(vec![b'\t'], m)),
        KeyCode::Backspace => Some(alt_prefixed(vec![0x7f], m)),
        KeyCode::Esc => Some(alt_prefixed(vec![0x1b], m)),
        KeyCode::Null => Some(vec![0]),
        // Shift+Tab has its own sequence rather than a modifier parameter.
        KeyCode::BackTab => Some(b"\x1b[Z".to_vec()),

        KeyCode::Up => Some(csi_letter(b'A', m)),
        KeyCode::Down => Some(csi_letter(b'B', m)),
        KeyCode::Right => Some(csi_letter(b'C', m)),
        KeyCode::Left => Some(csi_letter(b'D', m)),
        KeyCode::End => Some(csi_letter(b'F', m)),
        KeyCode::Home => Some(csi_letter(b'H', m)),

        KeyCode::Insert => Some(csi_tilde(2, m)),
        KeyCode::Delete => Some(csi_tilde(3, m)),
        KeyCode::PageUp => Some(csi_tilde(5, m)),
        KeyCode::PageDown => Some(csi_tilde(6, m)),

        // F1–F4 are SS3 when unmodified and CSI when not, an xterm quirk every
        // terminfo carries. F5 and up are ordinary tilde sequences, with a gap
        // in the numbering that is historical and not worth explaining.
        KeyCode::F(n @ 1..=4) => {
            let final_byte = b'P' + (n - 1);
            if modifier_param(m) == 1 {
                Some(vec![0x1b, b'O', final_byte])
            } else {
                Some(csi_letter(final_byte, m))
            }
        }
        KeyCode::F(n @ 5..=12) => {
            const NUMBERS: [u8; 8] = [15, 17, 18, 19, 20, 21, 23, 24];
            NUMBERS
                .get(usize::from(n - 5))
                .map(|number| csi_tilde(*number, m))
        }

        _ => None,
    }
}

/// A printable key, which `Ctrl` collapses into a control byte and `Alt`
/// prefixes with `ESC`.
fn char_bytes(c: char, m: KeyModifiers) -> Vec<u8> {
    let base = if m.contains(KeyModifiers::CONTROL) {
        match control_byte(c) {
            Some(byte) => vec![byte],
            None => encode_utf8(c),
        }
    } else {
        encode_utf8(c)
    };
    alt_prefixed(base, m)
}

/// The C0 control a `Ctrl` chord produces. Letters are the useful half; the
/// punctuation rows are here because shells and editors bind them (`Ctrl+_` is
/// undo in readline, `Ctrl+\` sends SIGQUIT).
fn control_byte(c: char) -> Option<u8> {
    match c.to_ascii_lowercase() {
        c @ 'a'..='z' => Some(c as u8 - b'a' + 1),
        ' ' | '@' => Some(0),
        '[' => Some(0x1b),
        '\\' => Some(0x1c),
        ']' => Some(0x1d),
        '^' => Some(0x1e),
        '_' => Some(0x1f),
        '?' => Some(0x7f),
        _ => None,
    }
}

fn encode_utf8(c: char) -> Vec<u8> {
    let mut buf = [0u8; 4];
    c.encode_utf8(&mut buf).as_bytes().to_vec()
}

fn alt_prefixed(mut bytes: Vec<u8>, m: KeyModifiers) -> Vec<u8> {
    if m.contains(KeyModifiers::ALT) {
        bytes.insert(0, 0x1b);
    }
    bytes
}

/// xterm's modifier encoding: a bitfield of shift/alt/ctrl, biased by one so
/// that "no modifiers" is 1 rather than 0.
fn modifier_param(m: KeyModifiers) -> u8 {
    let mut bits = 0;
    if m.contains(KeyModifiers::SHIFT) {
        bits |= 1;
    }
    if m.contains(KeyModifiers::ALT) {
        bits |= 2;
    }
    if m.contains(KeyModifiers::CONTROL) {
        bits |= 4;
    }
    bits + 1
}

/// `CSI A` for an unmodified arrow, `CSI 1;5A` for Ctrl+Up, and so on.
fn csi_letter(final_byte: u8, m: KeyModifiers) -> Vec<u8> {
    let param = modifier_param(m);
    if param == 1 {
        vec![0x1b, b'[', final_byte]
    } else {
        let mut bytes = format!("\x1b[1;{param}").into_bytes();
        bytes.push(final_byte);
        bytes
    }
}

/// `CSI 3~` for Delete, `CSI 3;5~` for Ctrl+Delete.
fn csi_tilde(number: u8, m: KeyModifiers) -> Vec<u8> {
    let param = modifier_param(m);
    if param == 1 {
        format!("\x1b[{number}~").into_bytes()
    } else {
        format!("\x1b[{number};{param}~").into_bytes()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crossterm::event::KeyEventState;

    fn key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent {
            code,
            modifiers,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    /// The encoding is the terminal's, not ours, so the cases are written as
    /// the byte sequences a real xterm sends — that is the only thing the
    /// process on the other end will agree with.
    #[test]
    fn keys_encode_the_way_a_terminal_sends_them() {
        const CTRL: KeyModifiers = KeyModifiers::CONTROL;
        const ALT: KeyModifiers = KeyModifiers::ALT;
        const SHIFT: KeyModifiers = KeyModifiers::SHIFT;
        const NONE: KeyModifiers = KeyModifiers::NONE;

        struct Case {
            name: &'static str,
            code: KeyCode,
            modifiers: KeyModifiers,
            want: Option<&'static [u8]>,
        }

        let cases = [
            Case {
                name: "a letter is itself",
                code: KeyCode::Char('a'),
                modifiers: NONE,
                want: Some(b"a"),
            },
            Case {
                name: "shift is already in the char crossterm parsed",
                code: KeyCode::Char('A'),
                modifiers: SHIFT,
                want: Some(b"A"),
            },
            Case {
                name: "non-ascii survives as utf-8",
                code: KeyCode::Char('é'),
                modifiers: NONE,
                want: Some("é".as_bytes()),
            },
            Case {
                name: "ctrl+c is the interrupt byte, which is the point",
                code: KeyCode::Char('c'),
                modifiers: CTRL,
                want: Some(&[0x03]),
            },
            Case {
                name: "ctrl+d ends stdin",
                code: KeyCode::Char('d'),
                modifiers: CTRL,
                want: Some(&[0x04]),
            },
            Case {
                name: "ctrl of an uppercase letter is the same control byte",
                code: KeyCode::Char('C'),
                modifiers: CTRL | SHIFT,
                want: Some(&[0x03]),
            },
            Case {
                name: "ctrl+backslash is SIGQUIT",
                code: KeyCode::Char('\\'),
                modifiers: CTRL,
                want: Some(&[0x1c]),
            },
            Case {
                name: "ctrl+space is NUL",
                code: KeyCode::Char(' '),
                modifiers: CTRL,
                want: Some(&[0x00]),
            },
            Case {
                name: "ctrl of something with no control byte types the char",
                code: KeyCode::Char('1'),
                modifiers: CTRL,
                want: Some(b"1"),
            },
            Case {
                name: "alt is an ESC prefix — this is meta-b, back-a-word",
                code: KeyCode::Char('b'),
                modifiers: ALT,
                want: Some(&[0x1b, b'b']),
            },
            Case {
                name: "alt and ctrl compose",
                code: KeyCode::Char('b'),
                modifiers: ALT | CTRL,
                want: Some(&[0x1b, 0x02]),
            },
            Case {
                name: "enter is CR, not LF — the tty turns it into a newline",
                code: KeyCode::Enter,
                modifiers: NONE,
                want: Some(b"\r"),
            },
            Case {
                name: "backspace is DEL, which is what stty erase expects",
                code: KeyCode::Backspace,
                modifiers: NONE,
                want: Some(&[0x7f]),
            },
            Case {
                name: "tab",
                code: KeyCode::Tab,
                modifiers: NONE,
                want: Some(b"\t"),
            },
            Case {
                name: "shift+tab has its own sequence",
                code: KeyCode::BackTab,
                modifiers: SHIFT,
                want: Some(b"\x1b[Z"),
            },
            Case {
                name: "escape",
                code: KeyCode::Esc,
                modifiers: NONE,
                want: Some(&[0x1b]),
            },
            Case {
                name: "up arrow",
                code: KeyCode::Up,
                modifiers: NONE,
                want: Some(b"\x1b[A"),
            },
            Case {
                name: "ctrl+right is word-forward, and needs the modifier param",
                code: KeyCode::Right,
                modifiers: CTRL,
                want: Some(b"\x1b[1;5C"),
            },
            Case {
                name: "shift+up",
                code: KeyCode::Up,
                modifiers: SHIFT,
                want: Some(b"\x1b[1;2A"),
            },
            Case {
                name: "alt+left",
                code: KeyCode::Left,
                modifiers: ALT,
                want: Some(b"\x1b[1;3D"),
            },
            Case {
                name: "home",
                code: KeyCode::Home,
                modifiers: NONE,
                want: Some(b"\x1b[H"),
            },
            Case {
                name: "end",
                code: KeyCode::End,
                modifiers: NONE,
                want: Some(b"\x1b[F"),
            },
            Case {
                name: "delete",
                code: KeyCode::Delete,
                modifiers: NONE,
                want: Some(b"\x1b[3~"),
            },
            Case {
                name: "ctrl+delete",
                code: KeyCode::Delete,
                modifiers: CTRL,
                want: Some(b"\x1b[3;5~"),
            },
            Case {
                name: "page up",
                code: KeyCode::PageUp,
                modifiers: NONE,
                want: Some(b"\x1b[5~"),
            },
            Case {
                name: "f1 is SS3 when unmodified",
                code: KeyCode::F(1),
                modifiers: NONE,
                want: Some(b"\x1bOP"),
            },
            Case {
                name: "f4",
                code: KeyCode::F(4),
                modifiers: NONE,
                want: Some(b"\x1bOS"),
            },
            Case {
                name: "modified f1 switches to CSI",
                code: KeyCode::F(1),
                modifiers: CTRL,
                want: Some(b"\x1b[1;5P"),
            },
            Case {
                name: "f5 skips 16, as terminfo has it",
                code: KeyCode::F(5),
                modifiers: NONE,
                want: Some(b"\x1b[15~"),
            },
            Case {
                name: "f12",
                code: KeyCode::F(12),
                modifiers: NONE,
                want: Some(b"\x1b[24~"),
            },
            Case {
                name: "an f-key past 12 is not something we can send",
                code: KeyCode::F(13),
                modifiers: NONE,
                want: None,
            },
            Case {
                name: "a bare modifier press is not input",
                code: KeyCode::CapsLock,
                modifiers: NONE,
                want: None,
            },
        ];

        for case in cases {
            let got = encode(key(case.code, case.modifiers));
            assert_eq!(
                got.as_deref(),
                case.want,
                "{}: got {:?}",
                case.name,
                got.as_ref().map(|b| String::from_utf8_lossy(b).to_string())
            );
        }
    }

    /// A release is not a keystroke. Terminals in the kitty protocol report
    /// them, and forwarding one would double every character typed.
    #[test]
    fn releases_send_nothing() {
        let release = KeyEvent {
            code: KeyCode::Char('a'),
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: KeyEventState::NONE,
        };
        assert_eq!(encode(release), None);
    }
}
