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
//! Screen rows move, though, and the text under them moves with them: output
//! arrives, the reader scrolls, old lines are evicted. So the rows are carried
//! as signed numbers and shifted by however far the view moved — see
//! [`Selection::shift_rows`] — which keeps the highlight on the text it was
//! dragged across rather than on the coordinates that text happened to occupy.
//! Signed, because a selection scrolled off the top has to keep counting up
//! there to come back to the same place when the view returns.
//!
//! What that cannot survive is a *reflow*: a resize or a filter change moves
//! different rows by different amounts, and there is no single shift to apply.
//! Those clear it.
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
    /// Column and screen row. The row is signed so it can track content that
    /// has scrolled above the pane and come back.
    anchor: Option<(u16, i32)>,
    cursor: Option<(u16, i32)>,
    /// True once the button is released: the selection stands until the next
    /// drag starts or the view moves under it.
    settled: bool,
    /// First selectable screen column: the message column's left edge.
    ///
    /// The process name is don's own furniture, not log text — dragging across
    /// it and pasting `api    | ` in front of every line is never what anyone
    /// wanted. Carried on the selection rather than applied by each reader so
    /// the highlight and the copied text cannot disagree about where the log
    /// starts; they did, and a selection that copies something other than what
    /// it shows is worse than one that includes the name.
    left_edge: u16,
}

impl Selection {
    /// Set the first column a selection may cover — the message column's left
    /// edge, in screen coordinates. Applies to selections started afterwards.
    pub(crate) fn set_left_edge(&mut self, left_edge: u16) {
        self.left_edge = left_edge;
    }

    /// The first column a selection may cover.
    pub(crate) fn left_edge(&self) -> u16 {
        self.left_edge
    }

    /// Start a drag at a screen position.
    pub(crate) fn begin(&mut self, column: u16, row: u16) {
        let column = column.max(self.left_edge);
        self.anchor = Some((column, i32::from(row)));
        self.cursor = Some((column, i32::from(row)));
        self.settled = false;
    }

    /// Move the loose end.
    pub(crate) fn extend(&mut self, column: u16, row: u16) {
        if self.anchor.is_some() {
            self.cursor = Some((column.max(self.left_edge), i32::from(row)));
        }
    }

    /// The view moved by `delta` rows; move with it.
    ///
    /// Positive means the content came down the screen — the view scrolled up.
    /// Applied to a settled selection and to a drag alike: a wheel notch
    /// mid-drag should move the fixed end too, or the selection grows by the
    /// scroll instead of by the pointer.
    pub(crate) fn shift_rows(&mut self, delta: i32) {
        if delta == 0 {
            return;
        }
        for (_, row) in [&mut self.anchor, &mut self.cursor].into_iter().flatten() {
            *row = row.saturating_add(delta);
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
        // The edge belongs to the layout, not to this drag.
        *self = Self {
            left_edge: self.left_edge,
            ..Self::default()
        };
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.span().is_none()
    }

    /// Normalised (start, end) in screen coordinates, reading order, or `None`
    /// for a click that never became a drag.
    pub(crate) fn span(&self) -> Option<((u16, i32), (u16, i32))> {
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
        let row = i32::from(row);
        if row < start.1 || row > end.1 {
            return false;
        }
        if row == start.1 && column < start.0 {
            return false;
        }
        if row == end.1 && column >= end.0 {
            return false;
        }
        // Rows in the middle of a multi-row drag start at column zero, so the
        // edge has to be enforced per cell rather than only at the ends.
        column >= self.left_edge
    }
}

/// The half-open column range of the word under `column`, if there is one.
///
/// Whitespace-delimited, which is what a log wants: paths, ids, durations and
/// bracketed levels all come out whole, and the alternative — a table of
/// "word characters" — argues with itself over `/`, `-`, `:` and `.`, every
/// one of which appears inside something a reader means to grab as a unit.
///
/// `None` when the column is on whitespace or past the end of the row, so a
/// double-click on empty space selects nothing rather than something arbitrary.
pub(crate) fn word_at(row: &str, column: usize) -> Option<(usize, usize)> {
    let chars: Vec<char> = row.chars().collect();
    if column >= chars.len() || chars[column].is_whitespace() {
        return None;
    }
    let start = chars[..column]
        .iter()
        .rposition(|c| c.is_whitespace())
        .map_or(0, |idx| idx + 1);
    let end = chars[column..]
        .iter()
        .position(|c| c.is_whitespace())
        .map_or(chars.len(), |idx| column + idx);
    Some((start, end))
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
    row_ids: &[crate::output::LogId],
    origin: (u16, u16),
) -> Option<String> {
    let (start, end) = selection.span()?;
    let left_edge = selection.left_edge();

    let mut out: Vec<(Option<crate::output::LogId>, String)> = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let screen_row =
            i32::from(origin.1).saturating_add(i32::try_from(index).unwrap_or(i32::MAX));
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
        // Same bound the highlight draws, so the text matches what was shown.
        from = from.max(to_col(left_edge)).min(chars.len());
        let id = row_ids.get(index).copied();
        if from >= until {
            out.push((id, String::new()));
            continue;
        }
        out.push((id, chars[from..until].iter().collect::<String>()));
    }

    // Rows of one message are one line. The wrap is this pane's layout, not
    // something the process wrote, so joining them back gives what it actually
    // emitted — and joins before any trimming, because the character the wrap
    // fell on is often the space between two words.
    let mut lines: Vec<String> = Vec::new();
    let mut previous: Option<crate::output::LogId> = None;
    for (id, text) in out {
        match (previous, id) {
            (Some(before), Some(now)) if before == now => {
                if let Some(line) = lines.last_mut() {
                    line.push_str(&text);
                }
            }
            _ => lines.push(text),
        }
        previous = id;
    }

    // Trailing blanks are the padding a pane row carries, not content.
    while lines.last().is_some_and(|line| line.trim().is_empty()) {
        lines.pop();
    }
    if lines.is_empty() {
        return None;
    }
    Some(
        lines
            .iter()
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

    /// One id per row, so no two rows are treated as one wrapped message.
    fn distinct_ids(rows: &[String]) -> Vec<crate::output::LogId> {
        (0..rows.len() as u64).map(crate::output::LogId).collect()
    }

    fn drag(from: (u16, u16), to: (u16, u16)) -> Selection {
        drag_within(from, to, 0)
    }

    /// A drag in a pane whose message column starts at `left_edge`.
    fn drag_within(from: (u16, u16), to: (u16, u16), left_edge: u16) -> Selection {
        let mut selection = Selection::default();
        selection.set_left_edge(left_edge);
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

    /// A message that wrapped comes back as one line. The break is this pane's
    /// layout, not something the process wrote, and the character the wrap fell
    /// on is often the space between two words — so the rejoin happens before
    /// any trimming, or the words run together.
    #[test]
    fn a_wrapped_message_is_copied_as_one_line() {
        use crate::output::LogId;

        let rows: Vec<String> = vec![
            "api | first message".to_string(),
            "api | a long one that ".to_string(),
            "    | wrapped here".to_string(),
            "api | last message".to_string(),
        ];
        // Rows 1 and 2 are one message; the others stand alone.
        let ids = vec![LogId(1), LogId(2), LogId(2), LogId(3)];
        let edge = 6u16;

        let all = drag_within((0, 0), (40, 3), edge);
        assert_eq!(
            selected_text(&all, &rows, &ids, (0, 0)).unwrap(),
            "first message\na long one that wrapped here\nlast message",
            "the wrap is rejoined; the newlines between messages are kept"
        );
    }

    /// The name column is don's furniture, not log text, and nothing selects
    /// into it — however the drag was made.
    #[test]
    fn selection_never_reaches_into_the_name_column() {
        let rows = vec![
            "api    | first line".to_string(),
            "api    | second line".to_string(),
            "worker | third line".to_string(),
        ];
        let edge = 9u16;

        let across = drag_within((0, 0), (20, 2), edge);
        assert_eq!(
            selected_text(&across, &rows, &distinct_ids(&rows), (0, 0)).unwrap(),
            "first line\nsecond line\nthird line",
            "a multi-row copy is the log, not a column of service names"
        );

        // Started inside the name column and dragged to column 13.
        let within = drag_within((0, 1), (13, 1), edge);
        assert_eq!(
            selected_text(&within, &rows, &distinct_ids(&rows), (0, 0)).unwrap(),
            "seco",
            "a drag that began on the name still copies only the message"
        );

        // And the highlight agrees cell for cell — the text and what was shown
        // are drawn from the same bound.
        for column in 0..edge {
            assert!(
                !within.contains(column, 1),
                "column {column} is inside the name column and must not highlight"
            );
        }
        assert!(within.contains(edge, 1), "the message column highlights");
    }

    /// A pane is padded with blank rows; those are layout, not content.
    #[test]
    fn trailing_blank_rows_are_not_copied() {
        let rows = vec![
            "api    | only line".to_string(),
            String::new(),
            String::new(),
        ];
        let selection = drag_within((0, 0), (40, 2), 9);
        assert_eq!(
            selected_text(&selection, &rows, &distinct_ids(&rows), (0, 0)).unwrap(),
            "only line"
        );
    }

    /// Double-click picks out a word. Whitespace-delimited is what a log wants:
    /// a path, a duration or a bracketed level comes out whole, and clicking
    /// empty space selects nothing rather than something arbitrary.
    #[test]
    fn double_click_selects_the_word_under_the_pointer() {
        struct Case {
            name: &'static str,
            row: &'static str,
            column: usize,
            want: Option<&'static str>,
        }

        let row = "api    | GET /v1/users 200 in 12.4ms";
        let cases = vec![
            Case {
                name: "the process name",
                row,
                column: 1,
                want: Some("api"),
            },
            Case {
                name: "a path, kept whole through its slashes",
                row,
                column: 16,
                want: Some("/v1/users"),
            },
            Case {
                name: "a duration, kept whole through its dot",
                row,
                column: 32,
                want: Some("12.4ms"),
            },
            Case {
                name: "the first character of a word",
                row,
                column: 9,
                want: Some("GET"),
            },
            Case {
                name: "whitespace selects nothing",
                row,
                column: 4,
                want: None,
            },
            Case {
                name: "past the end selects nothing",
                row,
                column: 500,
                want: None,
            },
            Case {
                name: "a blank row selects nothing",
                row: "          ",
                column: 3,
                want: None,
            },
        ];

        for case in cases {
            let got = word_at(case.row, case.column).map(|(start, end)| {
                case.row
                    .chars()
                    .skip(start)
                    .take(end - start)
                    .collect::<String>()
            });
            assert_eq!(got.as_deref(), case.want, "{}", case.name);
        }
    }

    /// A selection is dragged across *text*, and the text moves: output
    /// arrives, the reader scrolls, old lines are evicted. The highlight has
    /// to move with it or it ends up marking whatever happens to be at those
    /// coordinates afterwards.
    #[test]
    fn a_selection_moves_with_the_rows_it_was_dragged_across() {
        let mut selection = Selection::default();
        selection.begin(4, 10);
        selection.extend(9, 10);
        selection.finish();
        assert!(selection.contains(5, 10));

        // Three rows of output arrive while following: the text moves up.
        selection.shift_rows(-3);
        assert!(selection.contains(5, 7), "the highlight followed the text");
        assert!(!selection.contains(5, 10), "and left where it used to be");

        // Scrolled back down, it is where it started.
        selection.shift_rows(3);
        assert!(selection.contains(5, 10));

        // Pushed off the top and brought back. A row count that saturated at
        // zero would lose the position; a signed one does not.
        selection.shift_rows(-40);
        assert!(!selection.contains(5, 0), "well above the pane");
        selection.shift_rows(40);
        assert!(
            selection.contains(5, 10),
            "and comes back to the same characters"
        );
    }

    /// The fixed end moves too. A wheel notch during a drag scrolls the whole
    /// view, so anchoring only the loose end would grow the selection by the
    /// scroll rather than by the pointer.
    #[test]
    fn a_shift_moves_both_ends_of_a_drag() {
        let mut selection = Selection::default();
        selection.begin(2, 5);
        selection.extend(6, 8);
        selection.shift_rows(-2);
        let (start, end) = selection.span().unwrap();
        assert_eq!((start.1, end.1), (3, 6), "the span kept its height");
    }
}
