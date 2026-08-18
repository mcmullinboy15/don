//! The log pane's view over [`LogStore`].
//!
//! The store holds every line don sent this client, unfiltered. What the user
//! sees is a *view*: the subset the filter admits, wrapped to the pane's width,
//! positioned by a scroll anchor. Nothing is thrown away to render, which is
//! why changing the filter is free — it selects differently over the same
//! store instead of wiping the screen and replaying into it.
//!
//! ## Anchoring
//!
//! Scroll position is a [`LogId`] plus a row offset *within* that logical line,
//! never an absolute row count. Rows are a function of width: the same log line
//! is one row at 200 columns and four at 60. An offset measured in rows would
//! therefore mean something different after every resize, and the view would
//! jump. An id survives resizes, eviction of older lines, and filter changes.
//!
//! Following is the default and is its own state rather than "anchored to the
//! last line" — a distinction that matters when new lines arrive, since the
//! anchor would otherwise need rewriting on every push.

use ratatui::text::Line;

use super::log_store::LogStore;
use crate::output::LogId;

/// Where the log pane is looking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Scroll {
    /// Pinned to the newest line. New output pushes the view along.
    #[default]
    Follow,
    /// Held at a position the user chose. New output does not move it.
    At {
        /// The logical line at the top of the pane.
        id: LogId,
        /// How many wrapped rows of that line are above the top edge. Zero
        /// unless a line taller than the pane is scrolled through.
        row: u16,
    },
}

impl Scroll {
    /// The line the view is held at, if it is held at one.
    ///
    /// The store keeps history from here onward rather than evicting it under
    /// the reader — see [`LogStore::set_pin`].
    pub(crate) fn anchor(self) -> Option<LogId> {
        match self {
            Scroll::Follow => None,
            Scroll::At { id, .. } => Some(id),
        }
    }
}

/// What the renderer needs to paint the pane, and what the input layer needs
/// to know about how far it can move.
pub(crate) struct LogView<'a> {
    /// Wrapped rows, top of pane first, exactly `height` of them or fewer if
    /// the whole log is shorter than the pane.
    pub(crate) rows: Vec<Line<'a>>,
    /// Whether the view is pinned to the newest line.
    pub(crate) following: bool,
    /// Rows of admitted content above the top edge — the scrollbar's position.
    pub(crate) rows_above: usize,
    /// Total rows the admitted content occupies at this width.
    pub(crate) total_rows: usize,
}

/// Where the `name | ` prefix ends, in columns.
///
/// don builds each line as a padded process name, a `| `, then the message, so
/// the first `| ` is the column boundary. A process name cannot contain one, so
/// taking the first is exact rather than a guess — and a message that contains
/// `| ` later is unaffected.
///
/// Zero when there is no prefix at all, which is how a line with no owning
/// process (or a stream-end marker) renders full width.
pub(crate) fn prefix_columns(line: &Line<'_>) -> usize {
    let mut before = 0usize;
    let mut carry = false; // the previous span ended on a '|'
    for span in &line.spans {
        let text: &str = span.content.as_ref();
        if carry && text.starts_with(' ') {
            return before + 1;
        }
        if let Some(at) = text.find("| ") {
            return before + text[..at].chars().count() + 2;
        }
        carry = text.ends_with('|');
        before += text.chars().count();
    }
    0
}

/// The left column on a row that is a continuation of the row above.
///
/// Blank where the name would be, but the separator is carried down, so the
/// boundary between the two columns is a line the eye can follow rather than
/// something that only exists on whichever rows happen to start a message.
fn continuation_indent(prefix_cols: usize) -> String {
    match prefix_cols.checked_sub(2) {
        Some(pad) => format!("{}| ", " ".repeat(pad)),
        None => " ".repeat(prefix_cols),
    }
}

/// Split one already-styled line into rows, holding the prefix in its own
/// column.
///
/// The first row carries the real `name | ` prefix; every row after it is
/// indented to the same width so the message forms a single column down the
/// pane. Wrapping a line back to column zero is what makes a wall of output
/// unreadable — you cannot tell a continuation from a new line, or see which
/// process said what without tracing upwards.
///
/// Wrapping is done here rather than by ratatui's `Wrap` because the pane needs
/// the row *count* before it renders — to place the scroll anchor, to size the
/// scrollbar, and to know whether it is at the bottom. Asking the widget after
/// the fact would be a frame too late.
pub(crate) fn wrap_line<'a>(line: &Line<'a>, prefix_cols: usize, width: u16) -> Vec<Line<'a>> {
    let width = width.max(1) as usize;
    // A pane too narrow to hold the prefix and anything else falls back to
    // plain full-width wrapping; a zero-width message column cannot progress.
    let prefix_cols = if prefix_cols + 1 >= width {
        0
    } else {
        prefix_cols
    };

    let mut rows: Vec<Line<'a>> = Vec::new();
    let mut current: Vec<ratatui::text::Span<'a>> = Vec::new();
    let mut used = 0usize;
    // Columns consumed so far across the whole logical line, so we know when we
    // are still inside the prefix.
    let mut seen = 0usize;

    for span in &line.spans {
        let mut rest: &str = span.content.as_ref();
        while !rest.is_empty() {
            if used >= width {
                rows.push(Line::from(std::mem::take(&mut current)));
                current.push(ratatui::text::Span::raw(continuation_indent(prefix_cols)));
                used = prefix_cols;
            }
            let room = width - used;
            // Never break inside the prefix: it is one cell of the column.
            let room = if seen < prefix_cols {
                room.min(prefix_cols - seen)
            } else {
                room
            };
            let take = rest
                .char_indices()
                .take(room)
                .last()
                .map(|(idx, ch)| idx + ch.len_utf8())
                .unwrap_or(rest.len());
            let (head, tail) = rest.split_at(take);
            let taken = head.chars().count();
            current.push(ratatui::text::Span::styled(head.to_string(), span.style));
            used += taken;
            seen += taken;
            rest = tail;
        }
    }
    if !current.is_empty() || rows.is_empty() {
        rows.push(Line::from(current));
    }
    rows
}

/// How many rows [`wrap_line`] would produce, without producing them.
///
/// The store measures every line it ingests, and only ever paints a screenful
/// — so measuring by wrapping meant allocating the full wrapped form of every
/// line and dropping it, on every push and every reflow.
///
/// Every row reserves `prefix_cols` — the real prefix on the first, an indent on
/// the rest — so the message column is the same width throughout and the count
/// is just the message over that width.
///
/// Kept beside `wrap_line` and pinned against it by a test, because the two
/// disagreeing would put the scroll anchor somewhere the content isn't.
pub(crate) fn count_wrapped_rows(line: &Line<'_>, prefix_cols: usize, width: u16) -> usize {
    let width = width.max(1) as usize;
    let prefix_cols = if prefix_cols + 1 >= width {
        0
    } else {
        prefix_cols
    };
    let chars: usize = line
        .spans
        .iter()
        .map(|span| span.content.chars().count())
        .sum();
    let body = chars.saturating_sub(prefix_cols);
    body.div_ceil(width - prefix_cols).max(1)
}

/// Build the visible rows for a pane of `width` × `height`.
///
/// Which lines are in view is the index's decision, not this function's: it
/// selects the admitted lines and counts their rows, and this walks that
/// selection and paints it. Deciding twice — counting rows from the index and
/// then re-filtering the store while drawing — is how the two drifted apart,
/// which reads as a view that jumps somewhere other than where it was scrolled.
///
/// The filter lives in the index rather than at ingest because the store keeps
/// everything: widening the filter reveals history that a render-time filter
/// would have thrown away, which is why a filter change no longer wipes the
/// screen and replays into it.
///
/// The store must have been reflowed to `width` first; row counts come from its
/// cache, and only the rows that actually land on screen are wrapped.
pub(crate) fn build_view<'a>(
    store: &'a LogStore,
    index: &super::view_index::ViewIndex,
    scroll: Scroll,
    width: u16,
    height: u16,
) -> LogView<'a> {
    let height = height.max(1) as usize;
    let total_rows = index.total_rows();
    let max_above = total_rows.saturating_sub(height);

    let rows_above = match scroll {
        Scroll::Follow => max_above,
        Scroll::At { id, row } => match index.rows_above(id) {
            Some(above) => (above + row as usize).min(max_above),
            // The anchor was evicted or filtered away. The oldest line that
            // survives is where the reader was heading, not the live tail —
            // being yanked to the bottom because history aged out from under
            // you is the "jumpy" a busy log produces once it is at capacity.
            None => 0,
        },
    };

    // Only the visible window is wrapped, and only from the line that owns the
    // top row — no walk of everything above it.
    let mut rows: Vec<Line<'a>> = Vec::with_capacity(height);
    if let Some((first_id, skip_within)) = index.line_at(rows_above) {
        let mut skip = usize::from(skip_within);
        for entry in index.ids_from(first_id).filter_map(|id| store.get(id)) {
            for wrapped in wrap_line(&entry.parsed, entry.prefix_cols(), width)
                .into_iter()
                .skip(skip)
            {
                rows.push(wrapped);
                if rows.len() == height {
                    return LogView {
                        rows,
                        following: matches!(scroll, Scroll::Follow),
                        rows_above,
                        total_rows,
                    };
                }
            }
            skip = 0;
        }
    }

    LogView {
        rows,
        following: matches!(scroll, Scroll::Follow),
        rows_above,
        total_rows,
    }
}

/// The anchor for whichever line currently owns row `rows_above`.
///
/// The lookup [`scrolled`] does, without its "at the bottom means follow"
/// shortcut — because pinning the view *while* it sits at the bottom is exactly
/// what this is for. Selecting text freezes the view, so that the rows under
/// the selection stay the rows the user dragged across; without the freeze, one
/// line of new output invalidates the whole thing.
pub(crate) fn anchor_at(index: &super::view_index::ViewIndex, rows_above: usize) -> Scroll {
    match index.line_at(rows_above) {
        Some((id, row)) => Scroll::At { id, row },
        None => Scroll::Follow,
    }
}

/// Move the anchor by `delta` rows, clamping at both ends.
///
/// Returns the row offset it landed on as well as the anchor. Callers need it:
/// wheel events arrive in bursts, many per frame, and each one has to start
/// from where the previous one left off rather than from what the last frame
/// happened to measure — otherwise a whole burst collapses into one step.
///
/// Scrolling to the bottom re-enters [`Scroll::Follow`] rather than anchoring
/// at the last line — otherwise the view would sit one line behind forever
/// once new output arrived, which reads as a freeze.
pub(crate) fn scrolled(
    index: &super::view_index::ViewIndex,
    rows_above: usize,
    total_rows: usize,
    height: u16,
    delta: isize,
) -> (Scroll, usize) {
    let height = height.max(1) as usize;
    let max_above = total_rows.saturating_sub(height);
    let target = rows_above.saturating_add_signed(delta).min(max_above);
    if target >= max_above {
        return (Scroll::Follow, max_above);
    }
    (anchor_at(index, target), target)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use ratatui::text::Span;

    fn styled(text: &str) -> Line<'static> {
        Line::from(vec![Span::raw(text.to_string())])
    }

    fn row_text(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>()
    }

    /// The rows the pane draws must be the rows the index counted. If the view
    /// renders lines the filter excludes while positioning itself by filtered
    /// row counts, every scroll lands somewhere else than it says.
    #[test]
    fn the_view_draws_only_what_the_filter_admits() {
        let mut store = LogStore::with_capacity(100);
        store.reflow(40);
        for (id, name, body) in [
            (0u64, "api", "api one"),
            (1, "web", "web one"),
            (2, "api", "api two"),
            (3, "web", "web two"),
        ] {
            store.push(
                LogId(id),
                crate::output::FormattedLogLine {
                    name: name.to_string(),
                    is_lifecycle: false,
                    is_verbose: false,
                    bytes: body.as_bytes().to_vec(),
                },
            );
        }
        let mut index = super::super::view_index::ViewIndex::default();
        index.sync(
            &store,
            super::super::view_index::ViewKey {
                width: 40,
                filter: 0,
            },
            |entry| entry.line.name == "api",
        );

        let view = build_view(&store, &index, Scroll::Follow, 40, 10);
        let text: Vec<String> = view.rows.iter().map(row_text).collect();
        assert_eq!(
            text,
            vec!["api one".to_string(), "api two".to_string()],
            "the pane drew lines the filter excludes"
        );
    }

    /// The prefix is a column, not part of the text: what wraps lands under the
    /// message, not back at the left edge.
    #[test]
    fn wrapping_indents_under_the_message_column() {
        struct Case {
            name: &'static str,
            input: &'static str,
            width: u16,
            want: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "a continuation lines up under the message",
                // prefix "api | " is 6 columns, so the message column is 6 wide
                // at width 12.
                input: "api | abcdefghijkl",
                width: 12,
                want: vec!["api | abcdef", "    | ghijkl"],
            },
            Case {
                name: "three rows keep the same indent",
                input: "api | abcdefghijklmnopqr",
                width: 12,
                want: vec!["api | abcdef", "    | ghijkl", "    | mnopqr"],
            },
            Case {
                name: "a short line is one row and is not padded out",
                input: "api | hi",
                width: 12,
                want: vec!["api | hi"],
            },
            Case {
                name: "no prefix means no column, and it wraps full width",
                input: "abcdefghijklmn",
                width: 12,
                want: vec!["abcdefghijkl", "mn"],
            },
            Case {
                name: "a pane too narrow for the column falls back to full width",
                input: "api | abcdef",
                width: 6,
                want: vec!["api | ", "abcdef"],
            },
        ];

        for case in cases {
            let line = styled(case.input);
            let prefix = prefix_columns(&line);
            let rows = wrap_line(&line, prefix, case.width);
            let got: Vec<String> = rows.iter().map(row_text).collect();
            assert_eq!(got, case.want, "{}", case.name);
            assert_eq!(
                count_wrapped_rows(&line, prefix, case.width),
                rows.len(),
                "{}: the count must match what the wrap produced",
                case.name
            );
        }
    }

    /// The boundary is the first `| `, wherever the styling happens to split.
    #[test]
    fn the_prefix_column_ends_at_the_first_separator() {
        struct Case {
            name: &'static str,
            spans: Vec<&'static str>,
            want: usize,
        }

        let cases = vec![
            Case {
                name: "one span",
                spans: vec!["api | hello"],
                want: 6,
            },
            Case {
                name: "coloured name, then the separator",
                spans: vec!["api", " | ", "hello"],
                want: 6,
            },
            Case {
                name: "the separator itself split across spans",
                spans: vec!["api |", " hello"],
                want: 6,
            },
            Case {
                name: "a pipe in the message does not count",
                spans: vec!["api | a | b"],
                want: 6,
            },
            Case {
                name: "no separator at all",
                spans: vec!["just a bare line"],
                want: 0,
            },
            Case {
                name: "padded names give a wider column",
                spans: vec!["nodejs-install  | building"],
                want: 18,
            },
        ];

        for case in cases {
            let line = Line::from(
                case.spans
                    .iter()
                    .map(|text| Span::raw(text.to_string()))
                    .collect::<Vec<_>>(),
            );
            assert_eq!(prefix_columns(&line), case.want, "{}", case.name);
        }
    }

    /// Wrapping owns the row count, so it has to be exact: the pane places its
    /// scroll anchor and sizes its scrollbar from these numbers before anything
    /// is drawn.
    #[test]
    fn wrapping_splits_at_the_pane_width() {
        struct Case {
            name: &'static str,
            input: &'static str,
            width: u16,
            want: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "a line that fits is one row",
                input: "short",
                width: 10,
                want: vec!["short"],
            },
            Case {
                name: "an exact fit does not spill into an empty row",
                input: "exactly10!",
                width: 10,
                want: vec!["exactly10!"],
            },
            Case {
                name: "a long line splits at the width",
                input: "abcdefghij",
                width: 4,
                want: vec!["abcd", "efgh", "ij"],
            },
            Case {
                name: "an empty line still occupies a row",
                input: "",
                width: 10,
                want: vec![""],
            },
            Case {
                name: "width of one degrades rather than looping",
                input: "abc",
                width: 1,
                want: vec!["a", "b", "c"],
            },
        ];

        for case in cases {
            let rows = wrap_line(&styled(case.input), 0, case.width);
            let got: Vec<String> = rows.iter().map(row_text).collect();
            assert_eq!(got, case.want, "{}", case.name);
        }
    }

    /// Multi-byte characters must not be split mid-codepoint, and must count as
    /// the columns they occupy rather than the bytes they take.
    #[test]
    fn wrapping_counts_characters_not_bytes() {
        let rows = wrap_line(&styled("äöüßé"), 0, 2);
        let got: Vec<String> = rows.iter().map(row_text).collect();
        assert_eq!(got, vec!["äö", "üß", "é"]);
    }

    /// Styling survives a split: a wrapped line keeps the colours the upstream
    /// formatter gave it, on both halves.
    #[test]
    fn wrapping_preserves_span_styles() {
        let line = Line::from(vec![
            Span::styled(
                "red".to_string(),
                ratatui::style::Style::default().fg(ratatui::style::Color::Red),
            ),
            Span::styled(
                "blue".to_string(),
                ratatui::style::Style::default().fg(ratatui::style::Color::Blue),
            ),
        ]);
        let rows = wrap_line(&line, 0, 4);
        assert_eq!(rows.len(), 2, "7 columns over a width of 4");
        assert_eq!(
            rows[0].spans[0].style.fg,
            Some(ratatui::style::Color::Red),
            "the first row keeps its colour"
        );
        assert_eq!(
            rows[1].spans.last().unwrap().style.fg,
            Some(ratatui::style::Color::Blue),
            "and so does the second"
        );
    }

    /// The count and the wrap must never disagree: the store places the scroll
    /// anchor from the count and the renderer paints from the wrap, so a
    /// mismatch puts the view somewhere the content is not.
    #[test]
    fn counting_rows_agrees_with_actually_wrapping() {
        struct Case {
            name: &'static str,
            spans: Vec<&'static str>,
            width: u16,
        }

        let cases = vec![
            Case {
                name: "empty",
                spans: vec![],
                width: 20,
            },
            Case {
                name: "one empty span",
                spans: vec![""],
                width: 20,
            },
            Case {
                name: "fits",
                spans: vec!["short"],
                width: 20,
            },
            Case {
                name: "exact fit",
                spans: vec!["exactly10!"],
                width: 10,
            },
            Case {
                name: "one over",
                spans: vec!["exactly10!x"],
                width: 10,
            },
            Case {
                name: "several rows",
                spans: vec!["abcdefghijklmnopqrst"],
                width: 4,
            },
            Case {
                name: "split across spans",
                spans: vec!["abc", "defgh", "ij"],
                width: 4,
            },
            Case {
                name: "multibyte",
                spans: vec!["äöüßé", "àèìòù"],
                width: 3,
            },
            Case {
                name: "width of one",
                spans: vec!["abcde"],
                width: 1,
            },
            Case {
                name: "a span boundary landing exactly on the width",
                spans: vec!["abcd", "efgh"],
                width: 4,
            },
        ];

        for case in cases {
            let line = Line::from(
                case.spans
                    .iter()
                    .map(|text| ratatui::text::Span::raw((*text).to_string()))
                    .collect::<Vec<_>>(),
            );
            // Both with and without a prefix column: the count and the wrap
            // must agree either way, since the anchor is placed from one and
            // the content drawn from the other.
            for prefix in [0, prefix_columns(&line)] {
                assert_eq!(
                    count_wrapped_rows(&line, prefix, case.width),
                    wrap_line(&line, prefix, case.width).len(),
                    "{} (prefix {prefix})",
                    case.name
                );
            }
        }
    }
}
