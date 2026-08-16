//! Text selection in the log pane, and getting it onto the clipboard.
//!
//! In the alternate screen the terminal's own drag-select is gone: mouse
//! capture takes the events before the emulator sees them. So don owns
//! selection, which turns out to be the better deal — it can copy the *log
//! text* without the `api    | ` prefix don itself added, which native
//! selection never could.
//!
//! ## Coordinates
//!
//! A selection is two screen positions, not two positions in the log. That
//! sounds fragile and is deliberate: what the user dragged across is what is on
//! screen, and rows on screen are what the renderer just laid out. Resolving to
//! text happens against the same rows the renderer produced, in the same frame
//! — so a wrapped line, a filtered-out line and a scrolled view all resolve
//! correctly without selection needing to know about any of them.
//!
//! The consequence is that a selection does not survive scrolling or a resize,
//! which matches what a terminal does with its own selection.
//!
//! ## OSC 52
//!
//! Copying writes `ESC ] 52 ; c ; <base64> BEL` to the terminal, which asks it
//! to set the system clipboard. That works over ssh and inside tmux, where
//! reaching for a local clipboard API would not: the escape travels the same
//! path as the rest of the output. Terminals that have it disabled ignore it,
//! which is why the copy is also reported in the status bar — a silent no-op
//! would be indistinguishable from success.

use std::io::Write;

/// A drag in progress, or a finished selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Selection {
    anchor: Option<(u16, u16)>,
    cursor: Option<(u16, u16)>,
    /// True once the button is released: the selection stands until the next
    /// drag starts or the view moves under it.
    settled: bool,
}

impl Selection {
    /// Start a drag at a screen position.
    pub(crate) fn begin(&mut self, column: u16, row: u16) {
        self.anchor = Some((column, row));
        self.cursor = Some((column, row));
        self.settled = false;
    }

    /// Move the loose end.
    pub(crate) fn extend(&mut self, column: u16, row: u16) {
        if self.anchor.is_some() {
            self.cursor = Some((column, row));
        }
    }

    /// The button came up.
    pub(crate) fn finish(&mut self) {
        if self.anchor.is_some() {
            self.settled = true;
        }
    }

    /// Forget the selection — the view moved and the coordinates no longer
    /// mean what they meant.
    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.span().is_none()
    }

    /// Normalised (start, end) in screen coordinates, reading order, or `None`
    /// for a click that never became a drag.
    pub(crate) fn span(&self) -> Option<((u16, u16), (u16, u16))> {
        let (anchor, cursor) = (self.anchor?, self.cursor?);
        if anchor == cursor {
            return None;
        }
        // Compare by row first: a selection running up-left is still a
        // selection running from the earlier position to the later one.
        let (start, end) = if (anchor.1, anchor.0) <= (cursor.1, cursor.0) {
            (anchor, cursor)
        } else {
            (cursor, anchor)
        };
        Some((start, end))
    }

    /// Whether a given screen cell falls inside the selection.
    pub(crate) fn contains(&self, column: u16, row: u16) -> bool {
        let Some((start, end)) = self.span() else {
            return false;
        };
        if row < start.1 || row > end.1 {
            return false;
        }
        if row == start.1 && column < start.0 {
            return false;
        }
        if row == end.1 && column >= end.0 {
            return false;
        }
        true
    }
}

/// Pull the selected text out of the rows the renderer laid out.
///
/// `rows` are the pane's rows in order, `origin` is the pane's top-left corner,
/// and `prefix_width` is how many columns of each row belong to don's own
/// `name | ` prefix rather than to the process's output. Selections that span
/// more than one row drop the prefix, because what the user wants pasted is the
/// log, not a column of service names. A selection *within* one row is taken
/// verbatim — they pointed at exactly those characters.
pub(crate) fn selected_text(
    selection: &Selection,
    rows: &[String],
    origin: (u16, u16),
    prefix_width: u16,
) -> Option<String> {
    let (start, end) = selection.span()?;
    let single_row = start.1 == end.1;

    let mut out: Vec<String> = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let screen_row = origin
            .1
            .saturating_add(u16::try_from(index).unwrap_or(u16::MAX));
        if screen_row < start.1 || screen_row > end.1 {
            continue;
        }
        let chars: Vec<char> = row.chars().collect();
        let to_col = |screen_col: u16| -> usize {
            usize::from(screen_col.saturating_sub(origin.0)).min(chars.len())
        };
        let mut from = if screen_row == start.1 {
            to_col(start.0)
        } else {
            0
        };
        let until = if screen_row == end.1 {
            to_col(end.0)
        } else {
            chars.len()
        };
        if !single_row {
            from = from.max(usize::from(prefix_width)).min(chars.len());
        }
        if from >= until {
            out.push(String::new());
            continue;
        }
        out.push(chars[from..until].iter().collect::<String>());
    }

    // Trailing blanks are the padding a pane row carries, not content.
    while out.last().is_some_and(|line| line.trim().is_empty()) {
        out.pop();
    }
    if out.is_empty() {
        return None;
    }
    Some(
        out.iter()
            .map(|line| line.trim_end())
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// Ask the terminal to put `text` on the system clipboard, via OSC 52.
///
/// Written straight to stdout rather than through ratatui: it is a request to
/// the terminal, not a cell to paint, and it must not be diffed away.
pub(crate) fn copy_to_clipboard(text: &str) -> std::io::Result<()> {
    let encoded = base64_encode(text.as_bytes());
    let mut stdout = std::io::stdout();
    write!(stdout, "\x1b]52;c;{encoded}\x07")?;
    stdout.flush()
}

/// Base64, standard alphabet with padding.
///
/// Hand-rolled to avoid a dependency for forty lines; OSC 52 is the only thing
/// in don that needs it.
fn base64_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(triple >> 18) as usize & 0x3F] as char);
        out.push(ALPHABET[(triple >> 12) as usize & 0x3F] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(triple >> 6) as usize & 0x3F] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[triple as usize & 0x3F] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_the_standard_alphabet_and_padding() {
        struct Case {
            input: &'static str,
            want: &'static str,
        }

        // The RFC 4648 test vectors: the padding cases are where hand-rolled
        // encoders go wrong, and a mispadded OSC 52 payload is silently ignored
        // by the terminal rather than reported.
        let cases = vec![
            Case {
                input: "",
                want: "",
            },
            Case {
                input: "f",
                want: "Zg==",
            },
            Case {
                input: "fo",
                want: "Zm8=",
            },
            Case {
                input: "foo",
                want: "Zm9v",
            },
            Case {
                input: "foob",
                want: "Zm9vYg==",
            },
            Case {
                input: "fooba",
                want: "Zm9vYmE=",
            },
            Case {
                input: "foobar",
                want: "Zm9vYmFy",
            },
        ];

        for case in cases {
            assert_eq!(
                base64_encode(case.input.as_bytes()),
                case.want,
                "input {:?}",
                case.input
            );
        }
    }

    fn drag(from: (u16, u16), to: (u16, u16)) -> Selection {
        let mut selection = Selection::default();
        selection.begin(from.0, from.1);
        selection.extend(to.0, to.1);
        selection.finish();
        selection
    }

    /// A drag is direction-agnostic and half-open at the far end, so dragging
    /// back over a character deselects it rather than leaving it stuck.
    #[test]
    fn a_selection_reads_the_same_dragged_either_way() {
        struct Case {
            name: &'static str,
            selection: Selection,
            inside: Vec<(u16, u16)>,
            outside: Vec<(u16, u16)>,
        }

        let cases = vec![
            Case {
                name: "left to right on one row",
                selection: drag((2, 0), (5, 0)),
                inside: vec![(2, 0), (4, 0)],
                outside: vec![(1, 0), (5, 0), (2, 1)],
            },
            Case {
                name: "right to left is the same span",
                selection: drag((5, 0), (2, 0)),
                inside: vec![(2, 0), (4, 0)],
                outside: vec![(1, 0), (5, 0)],
            },
            Case {
                name: "across rows takes the whole middle",
                selection: drag((5, 1), (2, 3)),
                inside: vec![(5, 1), (0, 2), (99, 2), (1, 3)],
                outside: vec![(4, 1), (2, 3), (0, 4)],
            },
            Case {
                name: "a click that never moved selects nothing",
                selection: drag((3, 2), (3, 2)),
                inside: vec![],
                outside: vec![(3, 2)],
            },
        ];

        for case in cases {
            for point in case.inside {
                assert!(
                    case.selection.contains(point.0, point.1),
                    "{}: {point:?} should be inside",
                    case.name
                );
            }
            for point in case.outside {
                assert!(
                    !case.selection.contains(point.0, point.1),
                    "{}: {point:?} should be outside",
                    case.name
                );
            }
        }
    }

    /// The prefix is don's, not the process's. Copying several lines should
    /// paste the log; copying part of one line should paste exactly what was
    /// pointed at, prefix included if that is what they dragged over.
    #[test]
    fn copying_across_rows_drops_the_prefix_don_added() {
        let rows = vec![
            "api    | first line".to_string(),
            "api    | second line".to_string(),
            "worker | third line".to_string(),
        ];
        let prefix = 9u16;

        let across = drag((0, 0), (20, 2));
        assert_eq!(
            selected_text(&across, &rows, (0, 0), prefix).unwrap(),
            "first line\nsecond line\nthird line",
            "a multi-row copy is the log, not a column of service names"
        );

        // Columns 0..13, half-open at the far end: thirteen cells.
        let within = drag((0, 1), (13, 1));
        assert_eq!(
            selected_text(&within, &rows, (0, 0), prefix).unwrap(),
            "api    | seco",
            "a single-row copy is exactly the characters dragged over"
        );
    }

    /// A pane is padded with blank rows; those are layout, not content.
    #[test]
    fn trailing_blank_rows_are_not_copied() {
        let rows = vec![
            "api    | only line".to_string(),
            String::new(),
            String::new(),
        ];
        let selection = drag((0, 0), (40, 2));
        assert_eq!(
            selected_text(&selection, &rows, (0, 0), 9).unwrap(),
            "only line"
        );
    }
}
