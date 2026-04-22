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

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, List, ListItem, ListState, Paragraph, Row, Table};

use super::app::{App, StatusCounts, ViewMode};
use super::filter::{FilterState, MAX_DROPDOWN_ROWS};
use super::palette::{ActionPalette, MAX_PALETTE_ROWS};
use crate::runner::{ServiceState, TaskItemState};

/// Total rows the inline viewport reserves: 1 blank buffer row + 3 rows
/// for the bordered status box (top border + content + bottom border).
pub(crate) const BAR_VIEWPORT_HEIGHT: u16 = 4;

/// Spinner frames — the standard "dots" set. Rotate with `app.spinner_frame`.
const SPINNER_FRAMES: &[&str] = &[
    "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏",
];

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

    let bar = Paragraph::new(normal_bar_line(
        &app.counts,
        &app.filter,
        app.spinner_frame,
    ));
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
    let outer = Block::default()
        .borders(Borders::ALL)
        .title(" Filter logs — [space] toggle  [↑↓] move  [enter] apply  [esc] cancel ");
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    if inner.height < 2 {
        return;
    }
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(inner);
    draw_filter_list(frame, layout[0], &app.filter);
    let bar =
        Paragraph::new(filter_bar_line(&app.counts, &app.filter));
    frame.render_widget(bar, layout[1]);
}

fn draw_palette_modal(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.height < 3 || area.width == 0 {
        return;
    }

    let outer = Block::default()
        .borders(Borders::ALL)
        .title(" Actions — [↑↓] move  [enter] run  [esc] cancel ");
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    if inner.height < 2 {
        return;
    }
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(inner);
    draw_palette_list(frame, layout[0], &app.palette);
    let bar = Paragraph::new(palette_bar_line(&app.palette));
    frame.render_widget(bar, layout[1]);
}

/// Render the full-screen status overlay — a table of every known service
/// and task with its current state. Scrollable: only the window starting
/// at `app.overlay_scroll` is drawn, clamped so we don't leave blank rows
/// below the last entry.
fn draw_overlay(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.height == 0 || area.width == 0 {
        return;
    }

    let header = Row::new(vec!["KIND", "NAME", "STATE"])
        .style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan));

    let mut rows: Vec<Row<'static>> = Vec::new();
    let mut svc_names: Vec<&String> = app.services_state.keys().collect();
    svc_names.sort();
    for name in svc_names {
        if let Some(state) = app.services_state.get(name) {
            rows.push(Row::new(vec![
                Cell::from("service").style(Style::default().fg(Color::DarkGray)),
                Cell::from(name.clone()),
                Cell::from(service_state_label(*state))
                    .style(Style::default().fg(service_state_color(*state))),
            ]));
        }
    }

    let mut task_names: Vec<&String> = app.tasks_state.keys().collect();
    task_names.sort();
    for name in task_names {
        if let Some(state) = app.tasks_state.get(name) {
            rows.push(Row::new(vec![
                Cell::from("task").style(Style::default().fg(Color::DarkGray)),
                Cell::from(name.clone()),
                Cell::from(task_state_label(*state))
                    .style(Style::default().fg(task_state_color(*state))),
            ]));
        }
    }

    // Body height = frame height minus top/bottom borders and the header row.
    let body_height = area.height.saturating_sub(3) as usize;
    let total = rows.len();
    let max_scroll = total.saturating_sub(body_height);
    let scroll = app.overlay_scroll.min(max_scroll);
    let showing_more_below = scroll < max_scroll;
    let showing_more_above = scroll > 0;

    let visible: Vec<Row<'static>> = rows.into_iter().skip(scroll).take(body_height).collect();

    // Hint the user when the table is scrollable. Fits in the title bar so
    // it doesn't steal a row from the body.
    let mut title = String::from(" don status — [↑↓/jk] scroll  [esc] dismiss ");
    if showing_more_above || showing_more_below {
        let up = if showing_more_above { "↑" } else { " " };
        let down = if showing_more_below { "↓" } else { " " };
        title = format!(
            " don status — [↑↓/jk] scroll  [esc] dismiss  {up}{down} ({}/{}) ",
            scroll + visible.len(),
            total,
        );
    }

    let table = Table::new(
        visible,
        [
            Constraint::Length(9),
            Constraint::Percentage(40),
            Constraint::Percentage(60),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title(title));

    frame.render_widget(table, area);
}

fn draw_filter_list(frame: &mut Frame<'_>, area: Rect, filter: &FilterState) {
    if area.height == 0 {
        return;
    }
    let matches = filter.matches();
    let visible = matches
        .len()
        .min(area.height as usize)
        .min(MAX_DROPDOWN_ROWS);
    let items: Vec<ListItem<'static>> = matches
        .iter()
        .take(visible)
        .map(|name| {
            let selected = filter.is_edit_selected(name);
            let checkbox = if selected { "[x] " } else { "[ ] " };
            let checkbox_style = if selected {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            ListItem::new(Line::from(vec![
                Span::styled(checkbox, checkbox_style),
                Span::raw(name.clone()),
            ]))
        })
        .collect();

    let list = List::new(items)
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");
    let mut state = ListState::default().with_selected(if matches.is_empty() {
        None
    } else {
        Some(filter.highlight())
    });
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_palette_list(frame: &mut Frame<'_>, area: Rect, palette: &ActionPalette) {
    if area.height == 0 {
        return;
    }
    let items: Vec<ListItem<'static>> = palette
        .visible()
        .take(area.height as usize)
        .take(MAX_PALETTE_ROWS)
        .map(|(_, action)| ListItem::new(Line::from(Span::raw(action.label.clone()))))
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

fn normal_bar_line(
    counts: &StatusCounts,
    filter: &FilterState,
    spinner_frame: usize,
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
    if filter.is_active() {
        spans.push(bold_cyan("filter: "));
        spans.push(bold_green(filter.active_names().join(", ")));
        spans.push(dim("  [esc] clear"));
    } else {
        spans.push(dim("[l] filter  [a] actions  [s] status  [q] quit"));
    }
    Line::from(spans)
}

fn filter_bar_line(counts: &StatusCounts, filter: &FilterState) -> Line<'static> {
    let mut spans = base_count_spans(counts);
    spans.push(separator());
    spans.push(bold_cyan("filter: "));
    spans.push(Span::styled(
        filter.query().to_string(),
        Style::default().fg(Color::White),
    ));
    spans.push(Span::styled(
        "▌",
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::SLOW_BLINK),
    ));
    Line::from(spans)
}

fn palette_bar_line(palette: &ActionPalette) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    spans.push(bold_cyan("actions: "));
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
            Style::default()
                .fg(Color::Red)
                .add_modifier(Modifier::BOLD),
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
        TaskItemState::PendingRun => "pending_run",
    }
}

fn task_state_color(state: TaskItemState) -> Color {
    match state {
        TaskItemState::Completed | TaskItemState::Skipped => Color::Green,
        TaskItemState::Running | TaskItemState::Pending | TaskItemState::Building => Color::Yellow,
        TaskItemState::PendingRun => Color::Cyan,
        TaskItemState::Failed => Color::Red,
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

fn bold_green<S: Into<String>>(text: S) -> Span<'static> {
    Span::styled(
        text.into(),
        Style::default()
            .fg(Color::Green)
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
        " Run {}  — [tab] next/refresh  [↑↓] move  [enter] next/submit  [ctrl-enter] submit  [esc] cancel ",
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
        let field_rows = field_render_rows(field, is_focused);
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
        Some(err) => {
            Line::from(vec![Span::styled(
                format!("⚠ {err}"),
                Style::default().fg(Color::Red),
            )])
        }
        None => {
            let hint = form
                .focused()
                .map(|f| match f.kind {
                    ParamKind::Bool => "space flips the toggle",
                    ParamKind::Int => "↑/↓ steps the value",
                    _ => "↑/↓ selects candidate · → accepts",
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
fn field_render_rows(field: &super::form::Field, is_focused: bool) -> Vec<Line<'static>> {
    use super::form::CandidateState;
    use crate::config::ParamKind;

    let marker = if is_focused { "▶ " } else { "  " };
    let required_mark = if field.required { "*" } else { "" };
    let prompt = format!("{marker}{}{required_mark}: ", field.prompt);
    let prompt_style = if is_focused {
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
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

    if is_focused {
        let visible = field.visible_candidates();
        let cap = 5.min(visible.len());
        for (i, cand) in visible.iter().take(cap).enumerate() {
            let style = if i == field.candidate_highlight.min(cap.saturating_sub(1)) {
                Style::default()
                    .bg(Color::DarkGray)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            };
            lines.push(Line::from(Span::styled(format!("    {cand}"), style)));
        }
        if visible.len() > cap {
            lines.push(Line::from(dim(format!(
                "    … {} more",
                visible.len() - cap
            ))));
        }
    }

    lines
}
