//! Shared filterable/actionable status table used by service and task views.

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table};

/// Mutable navigation/filter state for a status table modal.
#[derive(Debug, Default, Clone)]
pub(crate) struct StatusTableState {
    pub(crate) highlight: usize,
    pub(crate) query: String,
    pub(crate) filtering: bool,
}

impl StatusTableState {
    pub(crate) fn reset(&mut self) {
        self.highlight = 0;
        self.query.clear();
        self.filtering = false;
    }

    pub(crate) fn selected_index(&self, total: usize) -> Option<usize> {
        if total == 0 {
            None
        } else {
            Some(self.highlight.min(total.saturating_sub(1)))
        }
    }

    pub(crate) fn handle_key(&mut self, key: KeyEvent, total: usize) -> StatusTableKeyOutcome {
        const PAGE: usize = 10;

        if self.filtering {
            match key.code {
                KeyCode::Enter => {
                    self.filtering = false;
                    self.highlight = 0;
                    return StatusTableKeyOutcome::Redraw;
                }
                KeyCode::Esc => {
                    self.filtering = false;
                    self.query.clear();
                    self.highlight = 0;
                    return StatusTableKeyOutcome::Redraw;
                }
                KeyCode::Backspace if self.query.pop().is_some() => {
                    self.highlight = 0;
                    return StatusTableKeyOutcome::Redraw;
                }
                KeyCode::Backspace => return StatusTableKeyOutcome::None,
                KeyCode::Char(c) => {
                    self.query.push(c);
                    self.highlight = 0;
                    return StatusTableKeyOutcome::Redraw;
                }
                _ => return StatusTableKeyOutcome::None,
            }
        }

        let max_idx = total.saturating_sub(1);
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.highlight = self.highlight.saturating_sub(1);
                StatusTableKeyOutcome::Redraw
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.highlight = (self.highlight + 1).min(max_idx);
                StatusTableKeyOutcome::Redraw
            }
            KeyCode::PageUp => {
                self.highlight = self.highlight.saturating_sub(PAGE);
                StatusTableKeyOutcome::Redraw
            }
            KeyCode::PageDown => {
                self.highlight = (self.highlight + PAGE).min(max_idx);
                StatusTableKeyOutcome::Redraw
            }
            KeyCode::Home => {
                self.highlight = 0;
                StatusTableKeyOutcome::Redraw
            }
            KeyCode::End => {
                self.highlight = max_idx;
                StatusTableKeyOutcome::Redraw
            }
            KeyCode::Char('/') => {
                self.filtering = true;
                StatusTableKeyOutcome::Redraw
            }
            KeyCode::Esc if !self.query.is_empty() => {
                self.query.clear();
                self.highlight = 0;
                StatusTableKeyOutcome::Redraw
            }
            KeyCode::Esc => StatusTableKeyOutcome::Close,
            _ => StatusTableKeyOutcome::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatusTableKeyOutcome {
    None,
    Redraw,
    Close,
}

pub(crate) fn retain_fuzzy_matches<T, F>(query: &str, items: &mut Vec<T>, mut name: F)
where
    F: FnMut(&T) -> &str,
{
    if query.is_empty() {
        return;
    }
    let names: Vec<String> = items.iter().map(|item| name(item).to_string()).collect();
    let matched = super::fuzzy::fuzzy_match(query, &names);
    let set: std::collections::HashSet<&str> = matched.iter().map(String::as_str).collect();
    items.retain(|item| set.contains(name(item)));
}

pub(crate) struct StatusTableView<'a> {
    pub(crate) title: String,
    pub(crate) header: Row<'static>,
    pub(crate) rows: Vec<Row<'static>>,
    pub(crate) widths: Vec<Constraint>,
    pub(crate) state: &'a StatusTableState,
    pub(crate) empty_label: &'static str,
    pub(crate) selected_hint: Option<String>,
}

pub(crate) fn draw_status_table(frame: &mut Frame<'_>, area: Rect, view: StatusTableView<'_>) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    let outer = Block::default().borders(Borders::ALL).title(view.title);
    let inner = outer.inner(area);
    frame.render_widget(outer, area);
    if inner.height < 2 {
        return;
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(inner);
    let table_area = layout[0];
    let bar_area = layout[1];

    let total = view.rows.len();
    let body_height = table_area.height.saturating_sub(1) as usize;
    let highlight = view.state.highlight.min(total.saturating_sub(1));
    let scroll = if body_height == 0 {
        0
    } else if highlight >= body_height {
        highlight + 1 - body_height
    } else {
        0
    };
    let max_scroll = total.saturating_sub(body_height);
    let scroll = scroll.min(max_scroll);
    let showing_more_below = scroll + body_height < total;
    let showing_more_above = scroll > 0;
    let highlight_style = Style::default().add_modifier(Modifier::REVERSED);

    let visible: Vec<Row<'static>> = view
        .rows
        .into_iter()
        .enumerate()
        .skip(scroll)
        .take(body_height)
        .map(|(idx, row)| {
            if idx == highlight {
                row.style(highlight_style)
            } else {
                row
            }
        })
        .collect();

    let table = Table::new(visible, view.widths).header(view.header);
    frame.render_widget(table, table_area);

    frame.render_widget(
        Paragraph::new(table_footer(
            view.state,
            total,
            highlight,
            showing_more_above,
            showing_more_below,
            view.empty_label,
            view.selected_hint,
        )),
        bar_area,
    );
}

fn table_footer(
    state: &StatusTableState,
    total: usize,
    highlight: usize,
    showing_more_above: bool,
    showing_more_below: bool,
    empty_label: &str,
    selected_hint: Option<String>,
) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    let has_query = !state.query.is_empty();
    if state.filtering || has_query {
        spans.push(bold_cyan("filter: "));
        spans.push(Span::styled(
            state.query.clone(),
            Style::default().fg(Color::White),
        ));
        if state.filtering {
            spans.push(Span::styled(
                "▌",
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::SLOW_BLINK),
            ));
        }
        spans.push(separator());
    }

    if total == 0 {
        spans.push(dim(if has_query {
            "no matches".to_string()
        } else {
            empty_label.to_string()
        }));
    } else {
        let scroll_hint = if showing_more_above || showing_more_below {
            let up = if showing_more_above { "↑" } else { " " };
            let down = if showing_more_below { "↓" } else { " " };
            format!("{up}{down} {}/{}", (highlight + 1).min(total), total)
        } else {
            format!("{}/{total}", highlight + 1)
        };
        spans.push(dim(scroll_hint));
        if let Some(hint) = selected_hint {
            spans.push(separator());
            spans.push(Span::styled(hint, Style::default().fg(Color::Green)));
        }
    }
    Line::from(spans)
}

fn separator() -> Span<'static> {
    Span::styled("  │  ", Style::default().fg(Color::DarkGray))
}

fn bold_cyan<S: Into<String>>(text: S) -> Span<'static> {
    Span::styled(
        text.into(),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
}

fn dim<S: Into<String>>(text: S) -> Span<'static> {
    Span::styled(text.into(), Style::default().fg(Color::DarkGray))
}
