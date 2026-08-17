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

/// Split one already-styled line into rows no wider than `width`.
///
/// Wrapping is done here rather than by ratatui's `Wrap` because the pane needs
/// the row *count* before it renders — to place the scroll anchor, to size the
/// scrollbar, and to know whether it is at the bottom. Asking the widget after
/// the fact would be a frame too late.
pub(crate) fn wrap_line<'a>(line: &Line<'a>, width: u16) -> Vec<Line<'a>> {
    let width = width.max(1) as usize;
    let mut rows: Vec<Line<'a>> = Vec::new();
    let mut current: Vec<ratatui::text::Span<'a>> = Vec::new();
    let mut used = 0usize;

    for span in &line.spans {
        let mut rest: &str = span.content.as_ref();
        while !rest.is_empty() {
            let room = width.saturating_sub(used);
            if room == 0 {
                rows.push(Line::from(std::mem::take(&mut current)));
                used = 0;
                continue;
            }
            // Split on a character boundary at most `room` columns wide.
            let take = rest
                .char_indices()
                .take(room)
                .last()
                .map(|(idx, ch)| idx + ch.len_utf8())
                .unwrap_or(rest.len());
            let (head, tail) = rest.split_at(take);
            current.push(ratatui::text::Span::styled(head.to_string(), span.style));
            used += head.chars().count();
            rest = tail;
            if used >= width && !rest.is_empty() {
                rows.push(Line::from(std::mem::take(&mut current)));
                used = 0;
            }
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
/// line and dropping it, on every push and every reflow. This is the same
/// arithmetic the wrap performs: it fills to `width` and breaks, so the row
/// count is the character count over the width, and an empty line still takes
/// a row.
///
/// Kept beside `wrap_line` and pinned against it by a test, because the two
/// disagreeing would put the scroll anchor somewhere the content isn't.
pub(crate) fn count_wrapped_rows(line: &Line<'_>, width: u16) -> usize {
    let width = width.max(1) as usize;
    let chars: usize = line
        .spans
        .iter()
        .map(|span| span.content.chars().count())
        .sum();
    chars.div_ceil(width).max(1)
}

/// Build the visible rows for a pane of `width` × `height`.
///
/// `admits` is the filter, applied here rather than at ingest — the store keeps
/// everything, so widening the filter reveals history that a render-time filter
/// would have discarded. That is the whole reason a filter change no longer
/// needs to wipe the screen and replay into it.
///
/// The store must have been reflowed to `width` first; row counts come from its
/// cache, and only the rows that actually land on screen are wrapped.
pub(crate) fn build_view<'a, F>(
    store: &'a LogStore,
    admits: F,
    scroll: Scroll,
    width: u16,
    height: u16,
) -> LogView<'a>
where
    F: Fn(&super::log_store::StoredLogLine) -> bool,
{
    let height = height.max(1) as usize;

    // Two passes over cached counts, then wrapping for the visible window only.
    // Wrapping every retained line every frame would make frame cost a function
    // of scrollback depth rather than of screen height.
    let admitted: Vec<&'a super::log_store::StoredLogLine> =
        store.iter().filter(|entry| admits(entry)).collect();
    let total_rows: usize = admitted
        .iter()
        .map(|entry| entry.wrapped_rows().max(1))
        .sum();
    let max_above = total_rows.saturating_sub(height);

    let rows_above = match scroll {
        Scroll::Follow => max_above,
        Scroll::At { id, row } => {
            let mut above = 0usize;
            let mut found = false;
            for entry in &admitted {
                if entry.id >= id {
                    found = true;
                    break;
                }
                above += entry.wrapped_rows().max(1);
            }
            // The anchor was evicted or filtered away. Falling back to the tail
            // is the honest answer: the position the user chose no longer
            // exists, and pretending otherwise would put them somewhere
            // arbitrary.
            if found {
                (above + row as usize).min(max_above)
            } else {
                max_above
            }
        }
    };

    let mut rows: Vec<Line<'a>> = Vec::with_capacity(height);
    let mut seen = 0usize;
    for entry in admitted {
        let entry_rows = entry.wrapped_rows().max(1);
        // Skip whole lines that finish above the top edge without wrapping them.
        if seen + entry_rows <= rows_above {
            seen += entry_rows;
            continue;
        }
        let skip_within = rows_above.saturating_sub(seen);
        for wrapped in wrap_line(&entry.parsed, width)
            .into_iter()
            .skip(skip_within)
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
        seen += entry_rows;
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
/// The same walk [`scrolled`] does, without its "at the bottom means follow"
/// shortcut — because pinning the view *while* it sits at the bottom is exactly
/// what this is for. Selecting text freezes the view, so that the rows under
/// the selection stay the rows the user dragged across; without the freeze,
/// one line of new output invalidates the whole thing.
pub(crate) fn anchor_at<F>(store: &LogStore, admits: F, rows_above: usize) -> Scroll
where
    F: Fn(&super::log_store::StoredLogLine) -> bool,
{
    let mut seen = 0usize;
    for entry in store.iter().filter(|entry| admits(entry)) {
        let rows = entry.wrapped_rows().max(1);
        if seen + rows > rows_above {
            return Scroll::At {
                id: entry.id,
                row: u16::try_from(rows_above - seen).unwrap_or(0),
            };
        }
        seen += rows;
    }
    Scroll::Follow
}

/// Move the anchor by `delta` rows, clamping at both ends.
///
/// Scrolling to the bottom re-enters [`Scroll::Follow`] rather than anchoring
/// at the last line — otherwise the view would sit one line behind forever
/// once new output arrived, which reads as a freeze.
pub(crate) fn scrolled<F>(
    store: &LogStore,
    admits: F,
    rows_above: usize,
    total_rows: usize,
    height: u16,
    delta: isize,
) -> Scroll
where
    F: Fn(&super::log_store::StoredLogLine) -> bool,
{
    let height = height.max(1) as usize;
    let max_above = total_rows.saturating_sub(height);
    let target = rows_above.saturating_add_signed(delta).min(max_above);
    if target >= max_above {
        return Scroll::Follow;
    }

    // Walk the admitted lines to find which one owns row `target`, using the
    // same cached counts the last frame measured with.
    let mut seen = 0usize;
    for entry in store.iter().filter(|entry| admits(entry)) {
        let rows = entry.wrapped_rows().max(1);
        if seen + rows > target {
            return Scroll::At {
                id: entry.id,
                row: u16::try_from(target - seen).unwrap_or(0),
            };
        }
        seen += rows;
    }
    Scroll::Follow
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
            let rows = wrap_line(&styled(case.input), case.width);
            let got: Vec<String> = rows.iter().map(row_text).collect();
            assert_eq!(got, case.want, "{}", case.name);
        }
    }

    /// Multi-byte characters must not be split mid-codepoint, and must count as
    /// the columns they occupy rather than the bytes they take.
    #[test]
    fn wrapping_counts_characters_not_bytes() {
        let rows = wrap_line(&styled("äöüßé"), 2);
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
        let rows = wrap_line(&line, 4);
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
            assert_eq!(
                count_wrapped_rows(&line, case.width),
                wrap_line(&line, case.width).len(),
                "{}",
                case.name
            );
        }
    }
}
