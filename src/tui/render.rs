//! Rendering primitives for the TUI.
//!
//! Two entry points:
//! - [`draw_bar`] fills the single-row inline viewport with the status bar.
//!   It's called on every state change while the inline terminal is active.
//! - [`draw_modal`] renders full-screen content (filter, palette, status
//!   overlay) into an alt-screen [`Terminal`]. It's called whenever the
//!   modal's app state changes.
//!
//! All UI output is a pure function of the [`App`] state plus the frame size —
//! no cursor math, no incremental writes.
//!
//! Log lines are *not* rendered here; they go into scrollback above the inline
//! viewport via [`Terminal::insert_before`].
//!
//! [`Terminal`]: ratatui::Terminal
//! [`Terminal::insert_before`]: ratatui::Terminal::insert_before

use std::collections::HashMap;

use crossterm::style::Color as CrosstermColor;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, List, ListItem, ListState, Paragraph, Row, Table};

use super::app::{App, OverlayItem, StatusCounts, ViewMode};
use super::filter::{FilterFocus, FilterRow, FilterState};
use super::palette::{Action, ActionPalette};
use crate::runner::{ServiceState, TaskItemState};

/// Total rows the inline viewport reserves: 1 blank buffer row + 3 rows
/// for the bordered status box (top border + content + bottom border).
pub(crate) const BAR_VIEWPORT_HEIGHT: u16 = 4;

/// Spinner frames — the standard "dots" set. Rotate with `app.spinner_frame`.
const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Draw the status bar (blank buffer row + bordered box) into the inline
/// viewport.
pub(crate) fn draw_bar(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.height < BAR_VIEWPORT_HEIGHT || area.width < 2 {
        return;
    }
    // Row 0 (blank) gives breathing room between scrollback logs and the box.
    // Rows 1..=3 render the bordered box.
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(3)])
        .split(area);
    let box_area = layout[1];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(box_area);
    frame.render_widget(block, box_area);

    // Count only services for the filter badge — tasks and synthetic
    // streams (don/bazel/turbo) are filterable too, but the bar should
    // echo what the user thinks of as "my services." Lazy services are
    // excluded so the denominator matches `counts.services_total` (which
    // excludes Lazy for the same "not-yet-started" reason).
    let countable = || {
        app.services_state
            .iter()
            .filter(|(_, s)| !matches!(s, crate::runner::ServiceState::Lazy))
    };
    let visible_services = countable()
        .filter(|(name, _)| app.filter.passes(name))
        .count();
    let total_services = countable().count();
    let bar = if app.shutdown_started {
        Paragraph::new(shutdown_bar_line(&app.counts, app.spinner_frame))
    } else {
        Paragraph::new(normal_bar_line(
            &app.counts,
            &app.filter,
            app.spinner_frame,
            visible_services,
            total_services,
            app.verbose_enabled,
        ))
    };
    frame.render_widget(bar, inner);
}

/// Dispatch to the full-screen render function for the current view mode.
/// Callers should only invoke this when `app.view_mode != Normal`.
pub(crate) fn draw_modal(frame: &mut Frame<'_>, app: &App) {
    match app.view_mode {
        ViewMode::Filter => draw_filter_modal(frame, app),
        ViewMode::Palette => draw_palette_modal(frame, app),
        ViewMode::Overlay => draw_overlay(frame, app),
        ViewMode::Form => draw_form_modal(frame, app),
        ViewMode::Normal => {}
    }
}

fn draw_filter_modal(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.height < 3 || area.width == 0 {
        return;
    }

    // Border + title wraps the whole modal. Inside: list at top, bar at bottom.
    let title = match app.filter.focus() {
        FilterFocus::List => {
            " Filter logs — [j/k ↑↓] move  [space] toggle  [o] only this  [/] search  [enter] done  [esc] revert "
        }
        FilterFocus::Query => {
            " Filter logs — [type] search  [enter] apply/close if single  [tab] back to list  [esc] revert "
        }
    };
    let outer = Block::default().borders(Borders::ALL).title(title);
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    if inner.height < 3 {
        return;
    }
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(inner);
    let name_colors = log_name_colors(app);
    let query = Paragraph::new(filter_query_line(&app.filter));
    frame.render_widget(query, layout[0]);
    draw_filter_list(frame, layout[1], &app.filter, &name_colors);
    let bar = Paragraph::new(filter_bar_line(&app.counts, &app.filter));
    frame.render_widget(bar, layout[2]);
}

fn draw_palette_modal(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.height < 3 || area.width == 0 {
        return;
    }

    let outer = Block::default()
        .borders(Borders::ALL)
        .title(" Tasks — [↑↓] move  [enter] run  [esc] cancel ");
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    if inner.height < 2 {
        return;
    }
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(inner);
    let name_colors = log_name_colors(app);
    draw_palette_list(frame, layout[0], &app.palette, &name_colors);
    let bar = Paragraph::new(palette_bar_line(&app.palette));
    frame.render_widget(bar, layout[1]);
}

/// Render the full-screen status overlay — a table of every known service
/// and task with its current state, sorted errors → running → exited → lazy
/// then alphabetical within each bucket. Arrow keys move a highlight; the
/// render path scrolls so the highlighted row stays visible.
fn draw_overlay(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.height == 0 || area.width == 0 {
        return;
    }

    let outer = Block::default()
        .borders(Borders::ALL)
        .title(
            " don status — [j/k ↑↓] move  [enter] start/stop/retry  [r] restart  [R] hard restart  [/] filter  [esc] clear filter/dismiss ",
        );
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

    let header = Row::new(vec!["KIND", "NAME", "STATE"]).style(
        Style::default()
            .add_modifier(Modifier::BOLD)
            .fg(Color::Cyan),
    );

    let items = app.overlay_items();
    let name_colors = log_name_colors(app);
    let total = items.len();

    // Body height = table area minus header row.
    let body_height = table_area.height.saturating_sub(1) as usize;
    let highlight = app.overlay_highlight.min(total.saturating_sub(1));
    // Scroll so the highlighted row is inside [scroll, scroll + body_height).
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
    let visible: Vec<Row<'static>> = items
        .iter()
        .enumerate()
        .skip(scroll)
        .take(body_height)
        .map(|(i, item)| {
            let (kind, name, state_cell) = match item {
                OverlayItem::Service { name, state } => (
                    "service",
                    name.clone(),
                    Cell::from(service_state_label(*state))
                        .style(Style::default().fg(service_state_color(*state))),
                ),
                OverlayItem::Task { name, state } => (
                    "task",
                    name.clone(),
                    Cell::from(task_state_label(*state))
                        .style(Style::default().fg(task_state_color(*state))),
                ),
            };
            let name_style = name_colors
                .get(&name)
                .copied()
                .map(|color| Style::default().fg(color))
                .unwrap_or_default();
            let row = Row::new(vec![
                Cell::from(kind).style(Style::default().fg(Color::DarkGray)),
                Cell::from(name).style(name_style),
                state_cell,
            ]);
            if i == highlight {
                row.style(highlight_style)
            } else {
                row
            }
        })
        .collect();

    let table = Table::new(
        visible,
        [
            Constraint::Length(9),
            Constraint::Percentage(40),
            Constraint::Percentage(60),
        ],
    )
    .header(header);
    frame.render_widget(table, table_area);

    // Bottom bar: filter input (when active or non-empty) and scroll indicator.
    let mut spans: Vec<Span<'static>> = Vec::new();
    let has_query = !app.overlay_query.is_empty();
    if app.overlay_filtering || has_query {
        spans.push(bold_cyan("filter: "));
        spans.push(Span::styled(
            app.overlay_query.clone(),
            Style::default().fg(Color::White),
        ));
        if app.overlay_filtering {
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
            "(no services or tasks)".to_string()
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
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), bar_area);
}

fn draw_filter_list(
    frame: &mut Frame<'_>,
    area: Rect,
    filter: &FilterState,
    name_colors: &HashMap<String, Color>,
) {
    if area.height == 0 {
        return;
    }
    let rows = filter.rows();
    // Pass every row to ratatui; the list's `ListState` scrolls automatically
    // to keep the selected index in view when rows overflow the area.
    let items: Vec<ListItem<'static>> = rows
        .iter()
        .map(|row| match row {
            FilterRow::All => {
                let selected = filter.all_selected_in_edit();
                let checkbox = if selected { "[x] " } else { "[ ] " };
                let checkbox_style = if selected {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                ListItem::new(Line::from(vec![
                    Span::styled(checkbox, checkbox_style),
                    Span::styled(
                        "all",
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]))
            }
            FilterRow::Name(name) => {
                let selected = filter.is_edit_selected(name);
                let checkbox = if selected { "[x] " } else { "[ ] " };
                let checkbox_style = if selected {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                let name_style = name_colors
                    .get(name)
                    .copied()
                    .map(|color| Style::default().fg(color))
                    .unwrap_or_default();
                ListItem::new(Line::from(vec![
                    Span::styled(checkbox, checkbox_style),
                    Span::styled(name.clone(), name_style),
                ]))
            }
        })
        .collect();

    let list = List::new(items)
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");
    let mut state = ListState::default().with_selected(if rows.is_empty() {
        None
    } else {
        Some(filter.highlight())
    });
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_palette_list(
    frame: &mut Frame<'_>,
    area: Rect,
    palette: &ActionPalette,
    name_colors: &HashMap<String, Color>,
) {
    if area.height == 0 {
        return;
    }
    // Hand every matching action to ratatui — its `ListState` keeps the
    // selected index in view by adjusting the scroll offset.
    let items: Vec<ListItem<'static>> = palette
        .visible()
        .map(|(_, action)| ListItem::new(palette_action_line(action, name_colors)))
        .collect();

    let list = List::new(items)
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");
    let mut state = ListState::default().with_selected(if palette.visible_count() == 0 {
        None
    } else {
        Some(palette.highlight())
    });
    frame.render_stateful_widget(list, area, &mut state);
}

fn palette_action_line(action: &Action, name_colors: &HashMap<String, Color>) -> Line<'static> {
    let Some(name) = action.task_name.as_ref() else {
        return Line::from(Span::styled(
            action.label.clone(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
    };

    let name_style = name_colors
        .get(name)
        .copied()
        .map(|color| Style::default().fg(color))
        .unwrap_or_default();
    let mut spans = vec![
        Span::styled("Run ", Style::default().fg(Color::DarkGray)),
        Span::styled(name.clone(), name_style),
    ];
    if action.needs_run {
        let state_color = action
            .task_state
            .map(task_state_color)
            .unwrap_or(Color::Cyan);
        spans.push(Span::styled(
            " (needs run)",
            Style::default().fg(state_color),
        ));
    }
    Line::from(spans)
}

fn normal_bar_line(
    counts: &StatusCounts,
    filter: &FilterState,
    spinner_frame: usize,
    visible_services: usize,
    total_services: usize,
    verbose_enabled: bool,
) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();

    // Spinner slot is always present so the bar doesn't shift as work
    // starts/stops. When idle, render a space.
    let spinner_glyph = if counts.is_working() {
        SPINNER_FRAMES[spinner_frame % SPINNER_FRAMES.len()]
    } else {
        " "
    };
    spans.push(Span::styled(
        format!(" {spinner_glyph} "),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ));

    spans.extend(base_count_spans(counts));

    if counts.tasks_running > 0 {
        spans.push(separator());
        let label = if counts.tasks_running == 1 {
            "1 task running".to_string()
        } else {
            format!("{} tasks running", counts.tasks_running)
        };
        spans.push(Span::styled(
            label,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
    }

    spans.push(separator());
    spans.push(dim("[l] logs"));
    if filter.is_active() {
        spans.push(dim(format!(" ({visible_services}/{total_services})")));
    }
    spans.push(dim("  [t] tasks  [s] status"));
    if verbose_enabled {
        spans.push(separator());
        spans.push(dim("verbose"));
    }
    Line::from(spans)
}

fn filter_bar_line(counts: &StatusCounts, filter: &FilterState) -> Line<'static> {
    let mut spans = base_count_spans(counts);
    spans.push(separator());
    let hint = match filter.focus() {
        FilterFocus::List => {
            "[j/k] move  [space] toggle  [o] only this  [/] search  [R] defaults".to_string()
        }
        FilterFocus::Query => {
            "[type] search  [enter] apply/close if single  [tab] back to list".to_string()
        }
    };
    spans.push(dim(hint));
    Line::from(spans)
}

fn filter_query_line(filter: &FilterState) -> Line<'static> {
    let mut spans = vec![bold_cyan("search: ")];
    let query_style = match filter.focus() {
        FilterFocus::Query => Style::default().fg(Color::White).bg(Color::DarkGray),
        FilterFocus::List => Style::default().fg(Color::White),
    };
    let query_text = if filter.query().is_empty() && filter.focus() == FilterFocus::List {
        Span::styled(
            "[/] to search".to_string(),
            Style::default().fg(Color::DarkGray),
        )
    } else {
        Span::styled(filter.query().to_string(), query_style)
    };
    spans.push(query_text);
    if filter.focus() == FilterFocus::Query {
        spans.push(Span::styled(
            "▌",
            Style::default()
                .fg(Color::White)
                .bg(Color::DarkGray)
                .add_modifier(Modifier::SLOW_BLINK),
        ));
    }
    Line::from(spans)
}

fn log_name_colors(app: &App) -> HashMap<String, Color> {
    let names: Vec<&str> = app
        .services_state
        .keys()
        .map(String::as_str)
        .chain(app.tasks_state.keys().map(String::as_str))
        .collect();
    crate::output::assign_colors(&names)
        .into_iter()
        .map(|(name, color)| (name, crossterm_color_to_ratatui(color)))
        .collect()
}

fn crossterm_color_to_ratatui(color: CrosstermColor) -> Color {
    match color {
        CrosstermColor::Reset => Color::Reset,
        CrosstermColor::Black => Color::Black,
        CrosstermColor::DarkGrey => Color::DarkGray,
        CrosstermColor::Red => Color::LightRed,
        CrosstermColor::DarkRed => Color::Red,
        CrosstermColor::Green => Color::LightGreen,
        CrosstermColor::DarkGreen => Color::Green,
        CrosstermColor::Yellow => Color::LightYellow,
        CrosstermColor::DarkYellow => Color::Yellow,
        CrosstermColor::Blue => Color::LightBlue,
        CrosstermColor::DarkBlue => Color::Blue,
        CrosstermColor::Magenta => Color::LightMagenta,
        CrosstermColor::DarkMagenta => Color::Magenta,
        CrosstermColor::Cyan => Color::LightCyan,
        CrosstermColor::DarkCyan => Color::Cyan,
        CrosstermColor::White => Color::White,
        CrosstermColor::Grey => Color::Gray,
        CrosstermColor::Rgb { r, g, b } => Color::Rgb(r, g, b),
        CrosstermColor::AnsiValue(value) => Color::Indexed(value),
    }
}

fn shutdown_bar_line(counts: &StatusCounts, spinner_frame: usize) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    let spinner_glyph = if counts.is_working() {
        SPINNER_FRAMES[spinner_frame % SPINNER_FRAMES.len()]
    } else {
        " "
    };
    spans.push(Span::styled(
        format!(" {spinner_glyph} "),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ));
    spans.push(Span::styled(
        "shutting down",
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    ));
    spans.push(separator());
    spans.extend(base_count_spans(counts));
    if counts.tasks_running > 0 {
        spans.push(separator());
        let label = if counts.tasks_running == 1 {
            "1 task running".to_string()
        } else {
            format!("{} tasks running", counts.tasks_running)
        };
        spans.push(Span::styled(
            label,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
    }
    Line::from(spans)
}

fn palette_bar_line(palette: &ActionPalette) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    spans.push(bold_cyan("tasks: "));
    spans.push(Span::styled(
        palette.query().to_string(),
        Style::default().fg(Color::White),
    ));
    spans.push(Span::styled(
        "▌",
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::SLOW_BLINK),
    ));
    if let Some(action) = palette.selected() {
        spans.push(separator());
        spans.push(Span::styled(
            format!("→ {}", action.label),
            Style::default().fg(Color::Green),
        ));
    }
    Line::from(spans)
}

fn base_count_spans(counts: &StatusCounts) -> Vec<Span<'static>> {
    let mut spans = Vec::new();

    let ready_color = if counts.services_failed > 0 {
        Color::Red
    } else if counts.services_unhealthy > 0 {
        Color::LightRed
    } else if counts.services_total > 0 && counts.services_ready == counts.services_total {
        Color::Green
    } else {
        Color::Yellow
    };
    spans.push(Span::styled(
        format!(
            "{}/{} services ready",
            counts.services_ready, counts.services_total
        ),
        Style::default().fg(ready_color),
    ));

    if counts.services_failed > 0 {
        spans.push(separator());
        spans.push(Span::styled(
            format!("{} failed", counts.services_failed),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
    }

    if counts.services_unhealthy > 0 {
        spans.push(separator());
        spans.push(Span::styled(
            format!("{} unhealthy", counts.services_unhealthy),
            Style::default()
                .fg(Color::LightRed)
                .add_modifier(Modifier::BOLD),
        ));
    }

    if counts.tasks_pending_run > 0 {
        spans.push(separator());
        let label = if counts.tasks_pending_run == 1 {
            "1 task pending".to_string()
        } else {
            format!("{} tasks pending", counts.tasks_pending_run)
        };
        spans.push(Span::styled(
            label,
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
    }

    spans
}

fn service_state_label(state: ServiceState) -> &'static str {
    match state {
        ServiceState::Pending => "pending",
        ServiceState::Building => "building",
        ServiceState::Lazy => "lazy",
        ServiceState::Starting => "starting",
        ServiceState::Running => "running",
        ServiceState::Ready => "ready",
        ServiceState::Unhealthy => "unhealthy",
        ServiceState::Stopping => "stopping",
        ServiceState::Stopped => "stopped",
        ServiceState::Failed => "failed",
        ServiceState::DependencyFailed => "dep failed",
    }
}

fn service_state_color(state: ServiceState) -> Color {
    match state {
        ServiceState::Ready | ServiceState::Running => Color::Green,
        ServiceState::Starting
        | ServiceState::Building
        | ServiceState::Pending
        | ServiceState::Stopping => Color::Yellow,
        ServiceState::Lazy => Color::Cyan,
        ServiceState::Stopped => Color::DarkGray,
        ServiceState::Unhealthy => Color::LightRed,
        ServiceState::Failed => Color::Red,
        // Dim red: same family as Failed but visually quieter, reflecting
        // that it's a downstream casualty, not the root cause.
        ServiceState::DependencyFailed => Color::Rgb(150, 60, 60),
    }
}

fn task_state_label(state: TaskItemState) -> &'static str {
    match state {
        TaskItemState::Pending => "pending",
        TaskItemState::Building => "building",
        TaskItemState::Running => "running",
        TaskItemState::Completed => "completed",
        TaskItemState::Skipped => "skipped",
        TaskItemState::Failed => "failed",
        TaskItemState::DependencyFailed => "dep failed",
        TaskItemState::PendingRun => "pending_run",
    }
}

fn task_state_color(state: TaskItemState) -> Color {
    match state {
        TaskItemState::Completed | TaskItemState::Skipped => Color::Green,
        TaskItemState::Running | TaskItemState::Pending | TaskItemState::Building => Color::Yellow,
        TaskItemState::PendingRun => Color::Cyan,
        TaskItemState::Failed => Color::Red,
        TaskItemState::DependencyFailed => Color::Rgb(150, 60, 60),
    }
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

/// Render the param-entry form. Each declared param occupies one row
/// (prompt + input + inline hint); the focused field optionally renders a
/// candidate dropdown beneath itself.
fn draw_form_modal(frame: &mut Frame<'_>, app: &App) {
    use super::form::{CandidateState, FormState};
    use crate::config::ParamKind;

    let area = frame.area();
    if area.height < 3 || area.width == 0 {
        return;
    }
    let Some(form): Option<&FormState> = app.form.as_ref() else {
        return;
    };

    let title = format!(
        " Run {}  — [tab] next/refresh  [↑↓] move  [enter] accept/next/submit  [ctrl-enter] submit  [esc] cancel ",
        form.task
    );
    let outer = Block::default().borders(Borders::ALL).title(title);
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    if inner.height < 2 {
        return;
    }
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(inner);

    // One paragraph with a Line per field, stacked vertically. The field
    // rows render into the same area, so we split by exact row counts.
    let rows = layout[0];
    let mut y = rows.y;
    let available = rows.height as usize;
    let mut used = 0usize;
    for (idx, field) in form.fields.iter().enumerate() {
        let is_focused = idx == form.focus;
        let remaining_fields = form.fields.len().saturating_sub(idx + 1);
        let max_rows_for_field = available.saturating_sub(used + remaining_fields);
        let field_rows = field_render_rows(field, is_focused, max_rows_for_field);
        if used + field_rows.len() > available {
            break;
        }
        for line in field_rows {
            if y >= rows.y + rows.height {
                break;
            }
            let row_area = Rect {
                x: rows.x,
                y,
                width: rows.width,
                height: 1,
            };
            frame.render_widget(Paragraph::new(line), row_area);
            y += 1;
            used += 1;
        }
    }

    // Footer line with submit error (if any) or a contextual hint.
    let footer = match form.submit_error.as_deref() {
        Some(err) => Line::from(vec![Span::styled(
            format!("⚠ {err}"),
            Style::default().fg(Color::Red),
        )]),
        None => {
            let hint = form
                .focused()
                .map(|f| match f.kind {
                    ParamKind::Bool => "space flips the toggle",
                    ParamKind::Int => "↑/↓ steps the value",
                    _ => "↑/↓ selects candidate · enter/→ accepts",
                })
                .unwrap_or("");
            Line::from(vec![dim(hint)])
        }
    };
    frame.render_widget(Paragraph::new(footer), layout[1]);
    // Borrow to satisfy the unused-import lint on variants we don't reach.
    let _ = CandidateState::None;
}

/// Build the lines for one field — at least one row for the input itself,
/// plus optional dropdown rows when the field is focused and has candidates.
fn field_render_rows(
    field: &super::form::Field,
    is_focused: bool,
    max_total_rows: usize,
) -> Vec<Line<'static>> {
    use super::form::CandidateState;
    use crate::config::ParamKind;

    let marker = if is_focused { "▶ " } else { "  " };
    let required_mark = if field.required { "*" } else { "" };
    let prompt = format!("{marker}{}{required_mark}: ", field.prompt);
    let prompt_style = if is_focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White)
    };

    let value_str = match field.kind {
        ParamKind::Bool => {
            if field.value.trim() == "true" {
                "[x] true".to_string()
            } else {
                "[ ] false".to_string()
            }
        }
        _ => field.value.clone(),
    };
    let cursor = if is_focused && !matches!(field.kind, ParamKind::Bool) {
        "▎"
    } else {
        ""
    };

    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::styled(prompt, prompt_style),
        Span::styled(value_str, Style::default().fg(Color::White)),
        Span::styled(cursor, Style::default().fg(Color::DarkGray)),
    ]));
    if lines.len() >= max_total_rows {
        return lines;
    }

    // Error / status banner.
    match &field.candidates {
        CandidateState::Loading if is_focused => {
            lines.push(Line::from(dim("  loading completions…")));
        }
        CandidateState::Failed { message, log_path } if is_focused => {
            let hint = match log_path {
                Some(p) => format!("  ⚠ {message} (log: {})", p.display()),
                None => format!("  ⚠ {message}"),
            };
            lines.push(Line::from(Span::styled(
                hint,
                Style::default().fg(Color::Red),
            )));
        }
        _ => {}
    }
    if lines.len() >= max_total_rows {
        return lines;
    }

    if is_focused {
        let remaining_rows = max_total_rows.saturating_sub(lines.len());
        let candidate_rows = remaining_rows.min(field.visible_candidates().len());
        if candidate_rows == 0 {
            return lines;
        }
        let window = field.visible_candidate_window(candidate_rows);
        let spare_rows = remaining_rows.saturating_sub(window.items.len());
        let show_above = window.hidden_above > 0 && spare_rows >= 2;
        let show_below = window.hidden_below > 0 && spare_rows > usize::from(show_above);

        if show_above {
            lines.push(Line::from(dim(format!(
                "    … {} above",
                window.hidden_above
            ))));
        }
        for (i, cand) in window.items.iter().enumerate() {
            let style = if i == window.highlight {
                Style::default()
                    .bg(Color::DarkGray)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            };
            lines.push(Line::from(Span::styled(format!("    {cand}"), style)));
        }
        if show_below {
            lines.push(Line::from(dim(format!(
                "    … {} more",
                window.hidden_below
            ))));
        }
    }

    lines
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::ParamKind;
    use crate::tui::form::{CandidateState, Field};

    fn line_text(line: Line<'static>) -> String {
        line.spans
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect::<Vec<String>>()
            .join("")
    }

    #[test]
    fn shutdown_bar_hides_interactive_controls() {
        let text = line_text(shutdown_bar_line(&StatusCounts::default(), 0));
        assert!(text.contains("shutting down"));
        assert!(!text.contains("[/] logs"));
        assert!(!text.contains("[t] tasks"));
        assert!(!text.contains("[s] status"));
    }

    #[test]
    fn focused_field_uses_available_space_for_candidates() {
        let field = Field {
            name: "index".into(),
            prompt: "index".into(),
            required: false,
            kind: ParamKind::String,
            value: String::new(),
            static_choices: vec![
                "c0".into(),
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
                "c5".into(),
                "c6".into(),
            ],
            has_dynamic_completions: false,
            candidates: CandidateState::Static(vec![
                "c0".into(),
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
                "c5".into(),
                "c6".into(),
            ]),
            candidate_highlight: 0,
            error: None,
            int_min: None,
            int_max: None,
        };

        let rows = field_render_rows(&field, true, 8);
        let texts: Vec<String> = rows.into_iter().map(line_text).collect();

        assert_eq!(texts.len(), 8);
        assert!(texts.iter().any(|t| t.contains("c0")));
        assert!(texts.iter().any(|t| t.contains("c6")));
    }
}
