//! Text selection in the log pane, and getting it onto the clipboard.
//!
//! In the alternate screen the terminal's own drag-select is gone: mouse
//! capture takes the events before the emulator sees them. So don owns
//! selection, which turns out to be the better deal — it can copy the *log
//! text* without the `api    | ` prefix don itself added, which native
//! selection never could.
//!
//! ## What a selection is
//!
//! Two places in the *log*: a line id and a character offset into that line's
//! message. Not two places on the screen.
//!
//! Screen coordinates were the first design and they read as the honest one —
//! what the reader dragged across is what was on screen. But the screen is the
//! one thing here that will not hold still. Output arrives, lines are evicted,
//! the reader scrolls, the terminal is resized, the filter changes: every one
//! of those moves the text under the coordinates, and a selection pinned to
//! them ends up marking whatever landed there instead. The scroll anchor in
//! [`super::logs`] gave up screen rows for a line id for exactly this reason.
//!
//! An id and an offset survive all of it. A line that is evicted takes its end
//! of the selection with it, which is right — that text is genuinely gone.
//!
//! The offset is into the message, so don's own `api    │ ` column simply is
//! not addressable: the highlight and the copied text cannot disagree about
//! where the log starts, because neither of them can point at the prefix.
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

use super::log_store::LogStore;
use super::logs::RowSource;
use super::view_index::ViewIndex;
use crate::output::LogId;

/// One end of a selection: a line, and how far into its message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Point {
    pub(crate) id: LogId,
    pub(crate) offset: usize,
}

impl Point {
    /// The place a screen column on `row` points at.
    pub(crate) fn at(row: RowSource, column: usize) -> Self {
        Self {
            id: row.id,
            offset: row.offset_at(column),
        }
    }
}

/// A drag in progress, or a finished selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Selection {
    anchor: Option<Point>,
    cursor: Option<Point>,
    /// True once the button is released: the selection stands until the next
    /// drag starts, or the reader dismisses it.
    settled: bool,
}

impl Selection {
    /// Start a drag.
    pub(crate) fn begin(&mut self, at: Point) {
        self.anchor = Some(at);
        self.cursor = Some(at);
        self.settled = false;
    }

    /// Move the loose end.
    pub(crate) fn extend(&mut self, to: Point) {
        if self.anchor.is_some() {
            self.cursor = Some(to);
        }
    }

    /// The button came up.
    pub(crate) fn finish(&mut self) {
        if self.anchor.is_some() {
            self.settled = true;
        }
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.span().is_none()
    }

    /// Normalised (start, end) in reading order, or `None` for a click that
    /// never became a drag.
    pub(crate) fn span(&self) -> Option<(Point, Point)> {
        let (anchor, cursor) = (self.anchor?, self.cursor?);
        if anchor == cursor {
            return None;
        }
        // A selection dragged up-left still runs from the earlier place to the
        // later one. Ids ascend with time and offsets ascend within a line, so
        // the derived ordering is reading order.
        Some(if anchor <= cursor {
            (anchor, cursor)
        } else {
            (cursor, anchor)
        })
    }

    /// Whether a character of the log falls inside the selection. Half-open at
    /// the end, like a drag: the cell the pointer stopped on is not included.
    pub(crate) fn contains(&self, at: Point) -> bool {
        let Some((start, end)) = self.span() else {
            return false;
        };
        at >= start && at < end
    }
}

/// The half-open character range of the word under `offset`, if there is one.
///
/// Whitespace-delimited, which is what a log wants: paths, ids, durations and
/// bracketed levels all come out whole, and the alternative — a table of
/// "word characters" — argues with itself over `/`, `-`, `:` and `.`, every
/// one of which appears inside something a reader means to grab as a unit.
///
/// `None` when the offset is on whitespace or past the end, so a double-click
/// on empty space selects nothing rather than something arbitrary.
///
/// Runs over the whole message rather than one rendered row, so a word the
/// pane wrapped in the middle still comes out whole.
pub(crate) fn word_at(message: &str, offset: usize) -> Option<(usize, usize)> {
    let chars: Vec<char> = message.chars().collect();
    if offset >= chars.len() || chars[offset].is_whitespace() {
        return None;
    }
    let start = chars[..offset]
        .iter()
        .rposition(|c| c.is_whitespace())
        .map_or(0, |idx| idx + 1);
    let end = chars[offset..]
        .iter()
        .position(|c| c.is_whitespace())
        .map_or(chars.len(), |idx| offset + idx);
    Some((start, end))
}

/// The selected text, read from the store.
///
/// From the store and not from the rows on screen, because the selection is
/// allowed to outlive the view of it: scroll away from a selection and it is
/// still a selection, and copying it should still give the text. The index
/// supplies which lines are admitted, so a filtered-out line inside the span
/// is skipped exactly as it is on screen.
pub(crate) fn selected_text(
    selection: &Selection,
    index: &ViewIndex,
    store: &LogStore,
) -> Option<String> {
    let (start, end) = selection.span()?;
    let mut lines: Vec<String> = Vec::new();
    for id in index.ids_from(start.id) {
        if id > end.id {
            break;
        }
        let Some(entry) = store.get(id) else {
            continue;
        };
        let message: Vec<char> = entry.message_text().chars().collect();
        let from = if id == start.id { start.offset } else { 0 };
        let until = if id == end.id {
            end.offset.min(message.len())
        } else {
            message.len()
        };
        let from = from.min(until);
        lines.push(
            message
                .get(from..until)
                .unwrap_or_default()
                .iter()
                .collect(),
        );
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

    fn store_of(lines: &[(u64, &str, &str)], width: u16) -> LogStore {
        let mut store = LogStore::with_capacity(100);
        store.reflow(width);
        for (id, name, body) in lines {
            store.push(
                LogId(*id),
                crate::output::FormattedLogLine {
                    name: (*name).to_string(),
                    is_lifecycle: false,
                    is_verbose: false,
                    prefix: format!("{name} | ").into_bytes(),
                    bytes: body.as_bytes().to_vec(),
                },
            );
        }
        store
    }

    fn index_of(store: &LogStore, width: u16, hide: &str) -> ViewIndex {
        let mut index = ViewIndex::default();
        index.sync(
            store,
            super::super::view_index::ViewKey { width, filter: 0 },
            &std::collections::HashMap::new(),
            |entry| entry.line.name != hide,
        );
        index
    }

    fn at(id: u64, offset: usize) -> Point {
        Point {
            id: LogId(id),
            offset,
        }
    }

    fn drag(from: Point, to: Point) -> Selection {
        let mut selection = Selection::default();
        selection.begin(from);
        selection.extend(to);
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
            inside: Vec<Point>,
            outside: Vec<Point>,
        }

        let cases = vec![
            Case {
                name: "within one line",
                selection: drag(at(0, 2), at(0, 5)),
                inside: vec![at(0, 2), at(0, 4)],
                outside: vec![at(0, 1), at(0, 5), at(1, 0)],
            },
            Case {
                name: "dragged back is the same span",
                selection: drag(at(0, 5), at(0, 2)),
                inside: vec![at(0, 2), at(0, 4)],
                outside: vec![at(0, 1), at(0, 5)],
            },
            Case {
                name: "across lines takes everything between",
                selection: drag(at(0, 3), at(2, 1)),
                inside: vec![at(0, 3), at(0, 99), at(1, 0), at(1, 40), at(2, 0)],
                outside: vec![at(0, 2), at(2, 1), at(3, 0)],
            },
            Case {
                name: "a click that never moved is not a selection",
                selection: drag(at(1, 4), at(1, 4)),
                inside: vec![],
                outside: vec![at(1, 4)],
            },
        ];

        for case in cases {
            for point in case.inside {
                assert!(case.selection.contains(point), "{}: {point:?}", case.name);
            }
            for point in case.outside {
                assert!(!case.selection.contains(point), "{}: {point:?}", case.name);
            }
        }
    }

    /// The point of anchoring to the log: everything that moves the text on
    /// screen — scrolling, a resize, a filter change — leaves the selection
    /// covering the same characters.
    #[test]
    fn a_selection_survives_everything_that_moves_the_screen() {
        let store = store_of(
            &[
                (0, "api", "the first line"),
                (1, "web", "a line from the other service"),
                (
                    2,
                    "api",
                    "a much longer line that will wrap differently at each width",
                ),
            ],
            40,
        );
        // "line that will" out of the third message, chosen to straddle a wrap
        // boundary at one width and not at another.
        let selection = drag(at(2, 14), at(2, 28));

        struct Case {
            name: &'static str,
            width: u16,
            hide: &'static str,
        }

        for case in [
            Case {
                name: "as laid out",
                width: 40,
                hide: "",
            },
            Case {
                name: "narrower, so the line wraps more",
                width: 24,
                hide: "",
            },
            Case {
                name: "wide enough not to wrap at all",
                width: 200,
                hide: "",
            },
            Case {
                name: "with the other service filtered out",
                width: 40,
                hide: "web",
            },
        ] {
            let mut store = store_of(
                &[
                    (0, "api", "the first line"),
                    (1, "web", "a line from the other service"),
                    (
                        2,
                        "api",
                        "a much longer line that will wrap differently at each width",
                    ),
                ],
                case.width,
            );
            store.reflow(case.width);
            let index = index_of(&store, case.width, case.hide);
            assert_eq!(
                selected_text(&selection, &index, &store).as_deref(),
                Some("line that will"),
                "{}",
                case.name
            );
        }
        let _ = store;
    }

    /// don's `name | ` column is not part of the message, so no offset can
    /// point at it and the copied text can never contain it.
    #[test]
    fn the_name_column_is_not_selectable() {
        let store = store_of(&[(0, "api", "hello world")], 40);
        let index = index_of(&store, 40, "");
        // A row whose message starts six columns in; columns inside the prefix
        // clamp to the message's first character.
        let row = RowSource {
            id: LogId(0),
            offset: 0,
            indent: 6,
        };
        assert_eq!(Point::at(row, 0).offset, 0, "the far left is the message");
        assert_eq!(
            Point::at(row, 5).offset,
            0,
            "and so is the last prefix cell"
        );
        assert_eq!(Point::at(row, 6).offset, 0);
        assert_eq!(Point::at(row, 9).offset, 3);

        let whole = drag(Point::at(row, 0), Point::at(row, 100));
        assert_eq!(
            selected_text(&whole, &index, &store).as_deref(),
            Some("hello world"),
            "no prefix in the copy"
        );
    }

    /// A message the pane wrapped is one thing to the reader; the wrap is this
    /// pane's layout, not something the process wrote.
    #[test]
    fn a_wrapped_message_is_copied_as_one_line() {
        let store = store_of(&[(0, "api", "one two three four five six seven")], 20);
        let index = index_of(&store, 20, "");
        let all = drag(at(0, 0), at(0, usize::MAX));
        let text = selected_text(&all, &index, &store).unwrap();
        assert_eq!(text, "one two three four five six seven");
        assert!(!text.contains('\n'), "the wrap is not a newline");
    }

    /// Lines the filter excludes are not on screen, so they are not in the
    /// copy either — even when the selection spans across them.
    #[test]
    fn a_filtered_out_line_inside_the_span_is_skipped() {
        let store = store_of(
            &[
                (0, "api", "first"),
                (1, "web", "hidden"),
                (2, "api", "second"),
            ],
            40,
        );
        let index = index_of(&store, 40, "web");
        let across = drag(at(0, 0), at(2, usize::MAX));
        assert_eq!(
            selected_text(&across, &index, &store).as_deref(),
            Some("first\nsecond")
        );
    }

    /// A double-click finds the word in the message, so one the pane wrapped
    /// still comes out whole.
    #[test]
    fn word_at_takes_whole_words_out_of_the_message() {
        struct Case {
            name: &'static str,
            offset: usize,
            want: Option<&'static str>,
        }

        let message = "GET /api/v1/users 200 14ms";
        for case in [
            Case {
                name: "a path is one word",
                offset: 6,
                want: Some("/api/v1/users"),
            },
            Case {
                name: "from the first character of one",
                offset: 0,
                want: Some("GET"),
            },
            Case {
                name: "a duration keeps its unit",
                offset: 23,
                want: Some("14ms"),
            },
            Case {
                name: "whitespace selects nothing",
                offset: 3,
                want: None,
            },
            Case {
                name: "and so does past the end",
                offset: 500,
                want: None,
            },
        ] {
            let got = word_at(message, case.offset).map(|(start, end)| {
                message
                    .chars()
                    .skip(start)
                    .take(end - start)
                    .collect::<String>()
            });
            assert_eq!(got.as_deref(), case.want, "{}", case.name);
        }
    }
}
