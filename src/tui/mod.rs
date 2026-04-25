//! Interactive terminal UI for `don start`.
//!
//! The TUI owns stdout when we're attached to a real terminal. Log lines
//! stream into the terminal's native scrollback via [`Terminal::insert_before`],
//! and a persistent inline viewport at the bottom renders the status bar.
//! Pipe mode (non-TTY) bypasses this module entirely — [`OutputManager`]
//! still writes prefixed bytes directly to stdout in that case.
//!
//! Heavy formatting (color prefix, ANSI sanitization, verbose timestamps) is
//! already done upstream; we parse those pre-rendered ANSI bytes back into
//! a styled [`Text`] via `ansi-to-tui` so ratatui can render them into its
//! buffer model.
//!
//! ## Viewport model
//!
//! The inline [`Terminal`] is created once at startup with `Viewport::Inline(1)`
//! and never rebuilt. All non-Normal modes (Filter, Palette, Overlay) render
//! into a separate alt-screen [`Terminal`] ([`Modal`]) that overlays the main
//! screen. Leaving the modal restores the main screen's previous contents;
//! new log lines received during the modal are replayed via
//! [`clear_and_replay`] so the filtered view stays coherent.
//!
//! Avoiding inline rebuilds sidesteps a nasty crossterm race: `get_cursor_position`
//! reads stdin for the DSR response, and racing with the input task's
//! `EventStream` produces either a 2-second block or a misplaced viewport.
//!
//! ## Concurrency
//!
//! One `tokio::select!` loop owns [`App`], the ratatui [`Terminal`], [`LogStore`],
//! and the `Option<Modal>`. Three side channels feed it:
//! - Log lines from the upstream [`OutputManager`].
//! - [`RunnerEvent`]s from the runner broadcast (consumed directly, no side task).
//! - Raw key events from the input task (interpretation is mode-dependent).
//!
//! [`OutputManager`]: crate::output::OutputManager
//! [`Terminal`]: ratatui::Terminal
//! [`Terminal::insert_before`]: ratatui::Terminal::insert_before
//! [`Text`]: ratatui::text::Text

mod app;
mod backend;
mod events;
mod filter;
mod form;
mod fuzzy;
mod input;
mod log_store;
mod palette;
mod render;

use ansi_to_tui::IntoText;
use crossterm::cursor::MoveTo;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Rect;
use ratatui::text::Text;
use ratatui::widgets::{Paragraph, Widget, Wrap};
use ratatui::{TerminalOptions, Viewport};
use tokio::sync::{broadcast, mpsc, oneshot};

use backend::FixedBottomBackend;

use crate::config::ParamKind;
use crate::output::FormattedLogLine;
use crate::runner::{RunnerCommand, RunnerEvent, ServiceState};
use app::{App, OverlayItem, ViewMode};
use events::AppEvent;
use log_store::{DEFAULT_CAPACITY, LogStore};
use palette::ActionKind;

/// Errors that can escape the TUI event loop.
#[derive(Debug, thiserror::Error)]
pub enum TuiError {
    /// Raw mode toggle, cursor operations, ratatui backend IO.
    #[error("terminal io error: {0}")]
    Io(#[from] std::io::Error),
}

/// RAII guard that leaves raw mode on drop.
struct RawModeGuard;

impl RawModeGuard {
    fn enter() -> Result<Self, TuiError> {
        crossterm::terminal::enable_raw_mode()?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

type TuiTerminal = Terminal<FixedBottomBackend<std::io::Stdout>>;

/// Alt-screen full-screen terminal used by Filter, Palette, and Overlay
/// modes. RAII: entering/leaving alt screen is tied to construction/drop,
/// so an error mid-draw still restores the main screen.
///
/// A Fullscreen viewport avoids `compute_inline_size`'s cursor-position
/// probe, which would race with the input task's stdin reader. Because
/// of that, modals don't need the [`FixedBottomBackend`] wrapper the
/// inline terminal uses.
struct Modal {
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
}

impl Modal {
    fn enter() -> Result<Self, TuiError> {
        execute!(std::io::stdout(), EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(std::io::stdout());
        let terminal = Terminal::with_options(
            backend,
            TerminalOptions {
                viewport: Viewport::Fullscreen,
            },
        )?;
        Ok(Self { terminal })
    }

    fn draw(&mut self, app: &App) -> Result<(), TuiError> {
        self.terminal.draw(|f| render::draw_modal(f, app))?;
        Ok(())
    }
}

impl Drop for Modal {
    fn drop(&mut self) {
        let _ = execute!(std::io::stdout(), LeaveAlternateScreen);
    }
}

/// Run the interactive TUI until the runner shuts down or the user quits.
///
/// Ctrl+C raises SIGINT to our own process so the installed signal handler
/// drives graceful shutdown (identical behavior to pipe mode, including the
/// two-Ctrl+C force-kill escalation).
#[allow(clippy::too_many_arguments)]
pub async fn run_tui(
    mut log_rx: mpsc::Receiver<FormattedLogLine>,
    mut runner_events: broadcast::Receiver<RunnerEvent>,
    command_tx: mpsc::Sender<RunnerCommand>,
    service_names: Vec<String>,
    task_names: Vec<String>,
    build_tool_names: Vec<String>,
    task_configs: std::collections::HashMap<String, crate::config::Task>,
    hidden_names: std::collections::HashSet<String>,
) -> Result<(), TuiError> {
    let _raw_guard = RawModeGuard::enter()?;

    let mut app = App::new(
        service_names,
        task_names,
        build_tool_names,
        task_configs,
        hidden_names,
    );
    let mut terminal = build_inline_terminal()?;
    let mut store = LogStore::with_capacity(DEFAULT_CAPACITY);
    let mut modal: Option<Modal> = None;

    let (input_tx, mut input_rx) = mpsc::channel::<AppEvent>(64);
    // Publish the sender so background tasks (completion replies) can
    // inject events back into the loop.
    let _ = INPUT_TX.set(input_tx.clone());
    let input_handle = tokio::spawn(input::run(input_tx));
    // Tracks whether the input task's channel is still open. When the input
    // task exits (crossterm EventStream error), we gate its select arm off
    // so the select loop doesn't busy-spin on a perpetually-ready None.
    let mut input_open = true;

    // Drives the spinner and any other time-based UI. Skip-on-miss so the
    // spinner doesn't catch up in a burst after a slow render.
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Seed the viewport so the terminal reserves the bottom region before
    // the first `insert_before` call. Use the raw `terminal.draw` here
    // (not `draw_inline_bar`) so we don't park the cursor — the wrapper
    // just did a real DSR and placed the viewport at the shell's cursor
    // row; parking would move that cursor away from where ratatui expects
    // subsequent writes to land.
    terminal.draw(|f| render::draw_bar(f, &app))?;

    loop {
        tokio::select! {
            maybe_line = log_rx.recv() => {
                match maybe_line {
                    Some(line) => {
                        // Skip inline writes while a modal owns stdout —
                        // they'd go to the alt screen. `LogStore` still
                        // captures the line, and `clear_and_replay` on
                        // modal exit brings the inline view up to date.
                        if modal.is_none() && app.filter.passes(&line.name) {
                            let width = terminal.size()?.width.max(1);
                            insert_line(&mut terminal, &line, width)?;
                            // `insert_before` resets ratatui's back buffer;
                            // redraw or the bar stays blank until next event.
                            draw_inline_bar(&mut terminal, &app)?;
                        }
                        store.push(line);
                    }
                    None => break, // runner closed the log channel — shut down
                }
            }
            runner_result = runner_events.recv() => {
                match runner_result {
                    Ok(event) => {
                        apply_runner_event(event, &mut app);
                        // Lazy services clutter the filter modal with names
                        // that have never produced a log line. Recompute the
                        // hidden set after each state change so triggered
                        // services (Lazy → Running) reappear automatically.
                        let lazy: std::collections::HashSet<String> = app
                            .services_state
                            .iter()
                            .filter(|(_, s)| matches!(s, crate::runner::ServiceState::Lazy))
                            .map(|(n, _)| n.clone())
                            .collect();
                        app.filter.set_hidden_from_display(lazy);
                        if let Some(m) = modal.as_mut() {
                            m.draw(&app)?;
                        } else {
                            draw_inline_bar(&mut terminal, &app)?;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Missed events — next one resyncs us; nothing to do.
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Runner dropped its broadcast end. The log channel
                        // will close next; the loop exits via that branch.
                    }
                }
            }
            maybe_event = input_rx.recv(), if input_open => {
                match maybe_event {
                    Some(event) => {
                        handle_app_event(
                            event,
                            &mut app,
                            &mut terminal,
                            &mut store,
                            &command_tx,
                            &mut modal,
                        )?;
                    }
                    None => {
                        // Input task exited — keep rendering logs and runner
                        // state, but no more keyboard input. Gate the arm so
                        // we don't spin on an always-ready closed channel.
                        input_open = false;
                    }
                }
            }
            _ = ticker.tick() => {
                app.spinner_frame = app.spinner_frame.wrapping_add(1);
                // Only touch the inline terminal; modals own stdout in alt
                // screen. We redraw unconditionally so the spinner animates —
                // ratatui's diff skips the cost when no cells changed.
                if modal.is_none() {
                    draw_inline_bar(&mut terminal, &app)?;
                }
            }
        }
    }

    input_handle.abort();
    drop(modal);
    terminal.clear()?;
    Ok(())
}

/// Build the persistent inline terminal used for log flow + bar.
///
/// Reserves [`render::BAR_VIEWPORT_HEIGHT`] rows at the bottom of the screen:
/// one blank buffer row, plus a bordered status box (top border + content
/// row + bottom border).
///
/// Note: no cursor parking here. [`FixedBottomBackend`] does a real DSR
/// on its first `get_cursor_position` call (inside `Terminal::with_options`)
/// so the viewport anchors right below the shell's pre-start output. That
/// keeps scrollback gap-free — the trade-off is that the bar starts at the
/// cursor row and drifts to the bottom as the first few log lines flow in.
fn build_inline_terminal() -> Result<TuiTerminal, TuiError> {
    let inner = CrosstermBackend::new(std::io::stdout());
    let backend = FixedBottomBackend::new(inner);
    let term = Terminal::with_options(
        backend,
        TerminalOptions {
            viewport: Viewport::Inline(render::BAR_VIEWPORT_HEIGHT),
        },
    )?;
    Ok(term)
}

/// Move the real cursor to `(0, screen_height - 1)`. Used before any
/// operation that may trigger ratatui's `autoresize` (which calls
/// `compute_inline_size` → `get_cursor_position` → [`FixedBottomBackend`])
/// so the fake "bottom of screen" cursor the wrapper reports actually
/// matches where `\n`s from `append_lines` will land and scroll.
fn park_cursor_at_bottom() -> Result<(), TuiError> {
    let (_cols, rows) = crossterm::terminal::size()?;
    let bottom = rows.saturating_sub(1);
    execute!(std::io::stdout(), MoveTo(0, bottom))?;
    Ok(())
}

/// Draw the inline bar, first parking the real cursor at the screen's
/// bottom row. If `terminal.draw`'s internal `autoresize` fires because
/// the terminal was resized since the last draw, the wrapper's fake
/// cursor (bottom of screen) and the real cursor will be at the same row
/// so `append_lines` scrolls correctly.
fn draw_inline_bar(terminal: &mut TuiTerminal, app: &App) -> Result<(), TuiError> {
    park_cursor_at_bottom()?;
    terminal.draw(|f| render::draw_bar(f, app))?;
    Ok(())
}

/// Apply one [`RunnerEvent`] to the cached state on [`App`].
fn apply_runner_event(event: RunnerEvent, app: &mut App) {
    match event {
        RunnerEvent::ServiceStateChanged { name, state } => {
            app.apply_service_state(name, state);
        }
        RunnerEvent::TaskStateChanged { name, state } => {
            app.apply_task_state(name, state);
        }
        RunnerEvent::RebuildComplete { .. }
        | RunnerEvent::TaskRerunComplete { .. }
        | RunnerEvent::ShutdownComplete => {}
    }
}

/// Wipe the entire visible area and replay every [`LogStore`] entry that
/// passes the current filter. Used after a filter commit/clear and when
/// returning from any modal that may have hidden new log lines.
///
/// Scrollback *buffer* (history above the visible area, managed by the
/// terminal emulator) is preserved — only on-screen pixels get wiped. So
/// the user can still scroll up to see their full log history including
/// pre-filter content.
fn clear_and_replay(
    terminal: &mut TuiTerminal,
    store: &LogStore,
    app: &App,
) -> Result<(), TuiError> {
    // Move the real cursor to (0, 0) and tell the wrapper to report the
    // same from its next `get_cursor_position` call. The subsequent
    // `terminal.resize` anchors the inline viewport at the top of the
    // screen; `insert_before` will then fill rows 0..N with replayed
    // log lines while the bar drifts downward, rather than pinning the
    // bar to the bottom with blank space above the replay content.
    execute!(std::io::stdout(), MoveTo(0, 0))?;
    terminal.backend_mut().force_next_cursor_top();
    // Re-place the viewport using the override. `resize` unconditionally
    // recomputes viewport placement (autoresize would skip when size is
    // unchanged) and its internal `self.clear()` wipes the visible
    // screen and resets ratatui's back buffer.
    let size = terminal.size()?;
    let area = Rect {
        x: 0,
        y: 0,
        width: size.width,
        height: size.height,
    };
    terminal.resize(area)?;
    // `resize` cleared the *visible* area but not the scrollback buffer.
    // Purge it (`\x1b[3J`) so pre-clear content and blank bands from
    // past `insert_before` scroll_ups don't linger when the user scrolls
    // up. Supported by most modern terminals; older ones silently ignore.
    execute!(std::io::stdout(), Clear(ClearType::Purge))?;
    let width = terminal.size()?.width.max(1);
    for entry in store.iter() {
        if app.filter.passes(&entry.name) {
            insert_line(terminal, entry, width)?;
        }
    }
    terminal.draw(|f| render::draw_bar(f, app))?;
    Ok(())
}

/// Dispatch an input or resize event.
fn handle_app_event(
    event: AppEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    command_tx: &mpsc::Sender<RunnerCommand>,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    match event {
        AppEvent::Resize => {
            if let Some(m) = modal.as_mut() {
                m.draw(app)?;
            } else {
                // Full clear + replay so border/bar ghosts from the previous
                // viewport position don't linger on screen. `clear_and_replay`
                // handles cursor-parking and autoresize internally.
                clear_and_replay(terminal, store, app)?;
            }
        }
        AppEvent::Key(key) => handle_key(key, app, terminal, store, command_tx, modal)?,
        AppEvent::CompletionsReady {
            param,
            request_id,
            result,
        } => {
            if let Some(form) = app.form.as_mut() {
                form.apply_completions(&param, request_id, result);
                redraw_modal(modal, app)?;
            }
        }
    }
    Ok(())
}

fn handle_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    command_tx: &mpsc::Sender<RunnerCommand>,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    // Ctrl+C: belt-and-suspenders shutdown. We both send a `Shutdown` command
    // directly down the runner channel AND raise SIGINT. The direct command
    // works even if the signal handler task has died or isn't being polled
    // (e.g., the runner is stuck pre-loop), and SIGINT preserves the
    // two-press force-kill escalation via the runner's signal counter.
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        if matches!(key.code, KeyCode::Char('c')) {
            let tx = command_tx.clone();
            tokio::spawn(async move {
                let _ = tx.send(RunnerCommand::Shutdown).await;
            });
            let _ =
                nix::sys::signal::kill(nix::unistd::Pid::this(), nix::sys::signal::Signal::SIGINT);
        }
        return Ok(());
    }

    match app.view_mode {
        ViewMode::Normal => handle_normal_key(key, app, terminal, store, modal)?,
        ViewMode::Filter => handle_filter_key(key, app, terminal, store, modal)?,
        ViewMode::Palette => handle_palette_key(key, app, terminal, store, command_tx, modal)?,
        ViewMode::Overlay => handle_overlay_key(key, app, terminal, store, command_tx, modal)?,
        ViewMode::Form => handle_form_key(key, app, terminal, store, command_tx, modal)?,
    }
    Ok(())
}

fn handle_normal_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    match key.code {
        KeyCode::Enter => {
            terminal.insert_before(1, |_buf| {})?;
            draw_inline_bar(terminal, app)?;
            store.push(FormattedLogLine {
                name: String::new(),
                bytes: Vec::new(),
            });
        }
        KeyCode::Char('l') => {
            app.filter.enter_edit();
            app.view_mode = ViewMode::Filter;
            let mut m = Modal::enter()?;
            m.draw(app)?;
            *modal = Some(m);
        }
        KeyCode::Char('t') => {
            app.palette.open(&app.tasks_state, &app.task_configs);
            app.view_mode = ViewMode::Palette;
            let mut m = Modal::enter()?;
            m.draw(app)?;
            *modal = Some(m);
        }
        KeyCode::Char('s') => {
            app.overlay_highlight = 0;
            app.overlay_query.clear();
            app.overlay_filtering = false;
            app.view_mode = ViewMode::Overlay;
            let mut m = Modal::enter()?;
            m.draw(app)?;
            *modal = Some(m);
        }
        _ => {}
    }
    Ok(())
}

fn handle_filter_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    match key.code {
        KeyCode::Enter => {
            app.filter.commit();
            app.view_mode = ViewMode::Normal;
            *modal = None; // drops, leaves alt screen
            clear_and_replay(terminal, store, app)?;
        }
        KeyCode::Esc => {
            app.filter.reset_to_defaults();
            app.view_mode = ViewMode::Normal;
            *modal = None;
            clear_and_replay(terminal, store, app)?;
        }
        KeyCode::Char(' ') => {
            app.filter.toggle_highlighted();
            redraw_modal(modal, app)?;
        }
        // `/` is the "filter" key across views — swallow it so pressing it
        // reflexively (from the main bar's [/] convention) doesn't end up
        // as the first character of the query.
        KeyCode::Char('/') => {}
        KeyCode::Char(c) => {
            app.filter.push_query_char(c);
            redraw_modal(modal, app)?;
        }
        KeyCode::Backspace => {
            app.filter.pop_query_char();
            redraw_modal(modal, app)?;
        }
        KeyCode::Up => {
            app.filter.highlight_prev();
            redraw_modal(modal, app)?;
        }
        KeyCode::Down => {
            app.filter.highlight_next();
            redraw_modal(modal, app)?;
        }
        _ => {}
    }
    Ok(())
}

fn handle_palette_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    command_tx: &mpsc::Sender<RunnerCommand>,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    match key.code {
        KeyCode::Enter => {
            let selected_kind = app.palette.selected().map(|a| a.kind.clone());
            app.palette.close();
            match selected_kind {
                Some(ActionKind::RunTaskWithForm(task_name)) => {
                    open_form_for_task(app, &task_name, command_tx)?;
                    // open_form_for_task switched to ViewMode::Form and
                    // rendered the form modal — nothing more to do here.
                    redraw_modal(modal, app)?;
                    return Ok(());
                }
                Some(kind) => {
                    dispatch_action(command_tx, kind);
                }
                None => {}
            }
            app.view_mode = ViewMode::Normal;
            *modal = None;
            clear_and_replay(terminal, store, app)?;
        }
        KeyCode::Esc => {
            app.palette.close();
            app.view_mode = ViewMode::Normal;
            *modal = None;
            clear_and_replay(terminal, store, app)?;
        }
        // `/` is the "filter" key across views — swallow it so pressing it
        // reflexively doesn't end up as the first character of the query.
        KeyCode::Char('/') => {}
        KeyCode::Char(c) => {
            app.palette.push_query_char(c);
            redraw_modal(modal, app)?;
        }
        KeyCode::Backspace => {
            app.palette.pop_query_char();
            redraw_modal(modal, app)?;
        }
        KeyCode::Up => {
            app.palette.highlight_prev();
            redraw_modal(modal, app)?;
        }
        KeyCode::Down => {
            app.palette.highlight_next();
            redraw_modal(modal, app)?;
        }
        _ => {}
    }
    Ok(())
}

fn handle_overlay_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    command_tx: &mpsc::Sender<RunnerCommand>,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    const PAGE: usize = 10;

    // Filter sub-mode: typing narrows, Enter exits keeping the query,
    // Esc clears and exits.
    if app.overlay_filtering {
        match key.code {
            KeyCode::Enter => {
                app.overlay_filtering = false;
                app.overlay_highlight = 0;
                redraw_modal(modal, app)?;
            }
            KeyCode::Esc => {
                app.overlay_filtering = false;
                app.overlay_query.clear();
                app.overlay_highlight = 0;
                redraw_modal(modal, app)?;
            }
            KeyCode::Backspace => {
                if app.overlay_query.pop().is_some() {
                    app.overlay_highlight = 0;
                    redraw_modal(modal, app)?;
                }
            }
            KeyCode::Char(c) => {
                app.overlay_query.push(c);
                app.overlay_highlight = 0;
                redraw_modal(modal, app)?;
            }
            _ => {}
        }
        return Ok(());
    }

    let total = app.overlay_items().len();
    let max_idx = total.saturating_sub(1);
    match key.code {
        KeyCode::Up => {
            app.overlay_highlight = app.overlay_highlight.saturating_sub(1);
            redraw_modal(modal, app)?;
        }
        KeyCode::Down => {
            app.overlay_highlight = (app.overlay_highlight + 1).min(max_idx);
            redraw_modal(modal, app)?;
        }
        KeyCode::PageUp => {
            app.overlay_highlight = app.overlay_highlight.saturating_sub(PAGE);
            redraw_modal(modal, app)?;
        }
        KeyCode::PageDown => {
            app.overlay_highlight = (app.overlay_highlight + PAGE).min(max_idx);
            redraw_modal(modal, app)?;
        }
        KeyCode::Home => {
            app.overlay_highlight = 0;
            redraw_modal(modal, app)?;
        }
        KeyCode::End => {
            app.overlay_highlight = max_idx;
            redraw_modal(modal, app)?;
        }
        KeyCode::Char('/') => {
            app.overlay_filtering = true;
            redraw_modal(modal, app)?;
        }
        KeyCode::Enter => {
            // Start or stop the highlighted service, depending on its state.
            if let Some(cmd) = overlay_toggle_command(app) {
                dispatch_runner_command(command_tx, cmd);
            }
        }
        KeyCode::Char('r') => {
            // Restart the highlighted service, if it's in a state that can
            // be restarted.
            if let Some(name) = highlighted_service_for_restart(app) {
                let (reply, _rx) = oneshot::channel();
                dispatch_runner_command(command_tx, RunnerCommand::Restart { name, reply });
            }
        }
        KeyCode::Char('R') => {
            // Restart every failed and dependency-failed service. The
            // DependencyFailed ones were only stranded because something
            // upstream broke; restarting them alongside the culprit gets
            // the whole cascade moving again in one keystroke.
            let failed: Vec<String> = app
                .services_state
                .iter()
                .filter(|(_, s)| matches!(s, ServiceState::Failed | ServiceState::DependencyFailed))
                .map(|(n, _)| n.clone())
                .collect();
            for name in failed {
                let (reply, _rx) = oneshot::channel();
                dispatch_runner_command(command_tx, RunnerCommand::Restart { name, reply });
            }
        }
        KeyCode::Esc => {
            app.view_mode = ViewMode::Normal;
            app.overlay_query.clear();
            app.overlay_filtering = false;
            *modal = None;
            clear_and_replay(terminal, store, app)?;
        }
        _ => {}
    }
    Ok(())
}

/// Build the Start/Stop command for the highlighted row, if it's an
/// actionable service. Returns `None` for tasks, in-flight services, or
/// when no row is highlighted.
fn overlay_toggle_command(app: &App) -> Option<RunnerCommand> {
    let items = app.overlay_items();
    let idx = app.overlay_highlight.min(items.len().saturating_sub(1));
    let item = items.get(idx)?;
    let OverlayItem::Service { name, state } = item else {
        return None;
    };
    match state {
        ServiceState::Ready | ServiceState::Running | ServiceState::Unhealthy => {
            let (reply, _rx) = oneshot::channel();
            Some(RunnerCommand::Stop {
                name: name.clone(),
                reply,
            })
        }
        ServiceState::Stopped
        | ServiceState::Failed
        | ServiceState::DependencyFailed
        | ServiceState::Lazy => {
            let (reply, _rx) = oneshot::channel();
            Some(RunnerCommand::Start {
                name: name.clone(),
                reply,
            })
        }
        ServiceState::Pending
        | ServiceState::Building
        | ServiceState::Starting
        | ServiceState::Stopping => None,
    }
}

/// Service name for `r` (restart) — only services in a restartable state.
fn highlighted_service_for_restart(app: &App) -> Option<String> {
    let items = app.overlay_items();
    let idx = app.overlay_highlight.min(items.len().saturating_sub(1));
    let item = items.get(idx)?;
    let OverlayItem::Service { name, state } = item else {
        return None;
    };
    match state {
        ServiceState::Ready
        | ServiceState::Running
        | ServiceState::Unhealthy
        | ServiceState::Failed
        | ServiceState::DependencyFailed
        | ServiceState::Stopped => Some(name.clone()),
        _ => None,
    }
}

/// Send a `RunnerCommand` without waiting for the reply. Mirrors the
/// fire-and-forget pattern used by `dispatch_action`.
fn dispatch_runner_command(command_tx: &mpsc::Sender<RunnerCommand>, cmd: RunnerCommand) {
    let command_tx = command_tx.clone();
    tokio::spawn(async move {
        let _ = command_tx.send(cmd).await;
    });
}

fn redraw_modal(modal: &mut Option<Modal>, app: &App) -> Result<(), TuiError> {
    if let Some(m) = modal.as_mut() {
        m.draw(app)?;
    }
    Ok(())
}

/// Fire a [`RunnerCommand`] without waiting for the reply.
///
/// Command replies are intentionally discarded — the user sees the effect
/// reflected in the status bar as soon as the runner emits the state change
/// event. Blocking on the reply would freeze the UI for the duration of a
/// service restart, which can be several seconds.
fn dispatch_action(command_tx: &mpsc::Sender<RunnerCommand>, action: ActionKind) {
    let command_tx = command_tx.clone();
    tokio::spawn(async move {
        let cmd = match action {
            ActionKind::RunPendingTasks => {
                let (reply_tx, _reply_rx) = oneshot::channel();
                RunnerCommand::RunPendingTasks { reply: reply_tx }
            }
            ActionKind::RunTask(name) => {
                let (reply_tx, _reply_rx) = oneshot::channel();
                RunnerCommand::RunTask {
                    name,
                    params: std::collections::HashMap::new(),
                    reply: reply_tx,
                }
            }
            ActionKind::RunTaskWithForm(_) => {
                // Palette's Enter handler intercepts this variant and opens
                // the form modal instead — it never reaches `dispatch_action`.
                // Return early to avoid sending a placeholder command.
                return;
            }
        };
        let _ = command_tx.send(cmd).await;
    });
}

/// Fire `RunnerCommand::RunTask` with the params map the user just submitted
/// via the form modal. Reply is swallowed; state updates come through the
/// event broadcast like any other runner command.
fn dispatch_run_task_with_params(
    command_tx: &mpsc::Sender<RunnerCommand>,
    name: String,
    params: std::collections::HashMap<String, String>,
) {
    let command_tx = command_tx.clone();
    tokio::spawn(async move {
        let (reply_tx, _reply_rx) = oneshot::channel();
        let _ = command_tx
            .send(RunnerCommand::RunTask {
                name,
                params,
                reply: reply_tx,
            })
            .await;
    });
}

/// Build the form state for `task_name`, transition to `ViewMode::Form`,
/// and kick off any completion fetches for fields with dynamic sources.
///
/// `command_tx` is used to send [`RunnerCommand::ResolveCompletions`] and
/// to relay completion replies back into the TUI loop via the shared
/// input channel. We reuse that channel — rather than opening a second
/// async source — so the main `select!` doesn't have to grow another arm.
fn open_form_for_task(
    app: &mut App,
    task_name: &str,
    command_tx: &mpsc::Sender<RunnerCommand>,
) -> Result<(), TuiError> {
    let Some(task) = app.task_configs.get(task_name).cloned() else {
        // Palette built the action from task_configs, so a missing entry
        // here is impossible. Keep the early-return rather than unwrapping.
        return Ok(());
    };
    let Some(form) = form::FormState::new(task_name, &task) else {
        return Ok(());
    };

    let dyn_fields: Vec<String> = form
        .fields
        .iter()
        .filter(|f| f.has_dynamic_completions)
        .map(|f| f.name.clone())
        .collect();

    app.form = Some(form);
    app.view_mode = ViewMode::Form;

    // Kick off an initial fetch for every field that needs it. The replies
    // come back through `input_tx` so they land in the same event queue
    // the main loop already reads.
    for param in dyn_fields {
        request_form_completion(app, task_name, &param, false, command_tx);
    }
    Ok(())
}

/// Spawn the background request/reply wiring for one completion fetch.
/// The reply is converted into `AppEvent::CompletionsReady` and sent to
/// the TUI loop through the global input channel.
fn request_form_completion(
    app: &mut App,
    task: &str,
    param: &str,
    force_refresh: bool,
    command_tx: &mpsc::Sender<RunnerCommand>,
) {
    let Some(form) = app.form.as_mut() else {
        return;
    };
    let partial: std::collections::HashMap<String, String> = form
        .fields
        .iter()
        .filter(|f| f.name != param && !f.value.is_empty())
        .map(|f| (f.name.clone(), f.value.clone()))
        .collect();
    let request_id = form.start_request(param);

    let command_tx = command_tx.clone();
    let Some(input_tx) = app_input_tx().cloned() else {
        return;
    };
    let task = task.to_string();
    let param = param.to_string();
    tokio::spawn(async move {
        let (reply_tx, reply_rx) = oneshot::channel();
        if command_tx
            .send(RunnerCommand::ResolveCompletions {
                task,
                param: param.clone(),
                partial,
                force_refresh,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return;
        }
        match reply_rx.await {
            Ok(result) => {
                let _ = input_tx
                    .send(AppEvent::CompletionsReady {
                        param,
                        request_id,
                        result,
                    })
                    .await;
            }
            Err(_) => {
                // Runner dropped the reply channel (shutting down) — nothing
                // useful to display; the form stays in Loading until the
                // user moves on.
            }
        }
    });
}

/// Shared, lazily-populated handle to the TUI loop's input channel. Set
/// once at the top of `run_tui` and cloned by background tasks that want
/// to inject events (e.g. completion replies). Using an `OnceLock` keeps
/// the API clean without threading the sender through every key handler.
static INPUT_TX: std::sync::OnceLock<mpsc::Sender<AppEvent>> = std::sync::OnceLock::new();

/// Fetch the input-channel handle. Returns `None` before [`run_tui`] runs
/// (e.g. in unit tests that exercise individual key handlers directly);
/// background tasks skip injection in that case rather than panicking.
fn app_input_tx() -> Option<&'static mpsc::Sender<AppEvent>> {
    INPUT_TX.get()
}

/// Handle keys while the form modal is open. Navigation, per-kind input,
/// candidate selection, and submit/cancel live here.
fn handle_form_key(
    key: KeyEvent,
    app: &mut App,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    command_tx: &mpsc::Sender<RunnerCommand>,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    // Grab these up front so later `app.form` borrows don't conflict.
    let task_name = match app.form.as_ref() {
        Some(f) => f.task.clone(),
        None => return Ok(()),
    };

    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Esc => {
            app.form = None;
            app.view_mode = ViewMode::Normal;
            *modal = None;
            clear_and_replay(terminal, store, app)?;
            return Ok(());
        }
        KeyCode::Enter if ctrl => {
            // Submit regardless of focused field.
            try_submit_form(app, command_tx, terminal, store, modal)?;
            return Ok(());
        }
        KeyCode::Enter => {
            // If focused field is on the last row → submit. Otherwise advance.
            if let Some(form) = app.form.as_ref()
                && form.focus + 1 >= form.fields.len()
            {
                try_submit_form(app, command_tx, terminal, store, modal)?;
                return Ok(());
            }
            if let Some(form) = app.form.as_mut() {
                form.focus_next();
            }
        }
        KeyCode::Tab => {
            // Tab on a dynamic field = refresh completions; on others = move focus.
            let refresh = app
                .form
                .as_ref()
                .and_then(|f| f.focused())
                .is_some_and(|f| {
                    matches!(
                        f.candidates,
                        form::CandidateState::Loaded(_)
                            | form::CandidateState::Failed { .. }
                            | form::CandidateState::Loading
                    )
                });
            let focused_param = app
                .form
                .as_ref()
                .and_then(|f| f.focused())
                .map(|f| f.name.clone());
            if refresh && let Some(param) = focused_param {
                request_form_completion(app, &task_name, &param, true, command_tx);
            } else if let Some(form) = app.form.as_mut() {
                form.focus_next();
            }
        }
        KeyCode::BackTab => {
            if let Some(form) = app.form.as_mut() {
                form.focus_prev();
            }
        }
        KeyCode::Up => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                match field.kind {
                    ParamKind::Int => field.step_int(1),
                    _ => {
                        // Move candidate highlight up.
                        if field.candidate_highlight > 0 {
                            field.candidate_highlight -= 1;
                        }
                    }
                }
            }
        }
        KeyCode::Down => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                match field.kind {
                    ParamKind::Int => field.step_int(-1),
                    _ => {
                        let max = field.visible_candidates().len().saturating_sub(1);
                        if field.candidate_highlight < max {
                            field.candidate_highlight += 1;
                        }
                    }
                }
            }
        }
        KeyCode::Char(' ') => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                match field.kind {
                    ParamKind::Bool => field.toggle_bool(),
                    _ => {
                        field.value.push(' ');
                        field.candidate_highlight = 0;
                    }
                }
            }
        }
        KeyCode::Char(c) => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                match field.kind {
                    ParamKind::Bool => {
                        // Letters don't map to a bool value — ignore.
                    }
                    ParamKind::Int => {
                        if c.is_ascii_digit() || (c == '-' && field.value.is_empty()) {
                            field.value.push(c);
                        }
                    }
                    _ => {
                        field.value.push(c);
                        field.candidate_highlight = 0;
                    }
                }
            }
        }
        KeyCode::Backspace => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                field.value.pop();
            }
        }
        KeyCode::Right => {
            // On a field with candidates, Right accepts the highlight.
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
            {
                field.accept_highlighted_candidate();
            }
        }
        _ => {}
    }
    redraw_modal(modal, app)?;
    Ok(())
}

/// Attempt to submit the form. On success: dispatch `RunnerCommand::RunTask`,
/// close the modal, return to Normal. On validation error: record it on the
/// form so the renderer can show it, and stay open.
fn try_submit_form(
    app: &mut App,
    command_tx: &mpsc::Sender<RunnerCommand>,
    terminal: &mut TuiTerminal,
    store: &mut LogStore,
    modal: &mut Option<Modal>,
) -> Result<(), TuiError> {
    let (task_name, params) = {
        let Some(form) = app.form.as_mut() else {
            return Ok(());
        };
        match form.submit() {
            Ok(p) => {
                form.submit_error = None;
                (form.task.clone(), p)
            }
            Err(msg) => {
                form.submit_error = Some(msg);
                redraw_modal(modal, app)?;
                return Ok(());
            }
        }
    };
    dispatch_run_task_with_params(command_tx, task_name, params);
    app.form = None;
    app.view_mode = ViewMode::Normal;
    *modal = None;
    clear_and_replay(terminal, store, app)?;
    Ok(())
}

/// Insert a single formatted log line into the scrollback above the inline
/// viewport. Returns the number of terminal rows actually consumed.
fn insert_line(
    terminal: &mut TuiTerminal,
    line: &FormattedLogLine,
    width: u16,
) -> Result<u16, TuiError> {
    let text = parse_ansi(&line.bytes);
    let height = Paragraph::new(text.clone())
        .wrap(Wrap { trim: false })
        .line_count(width)
        .max(1) as u16;

    terminal.insert_before(height, |buf| {
        let area = Rect {
            x: 0,
            y: 0,
            width: buf.area.width,
            height: buf.area.height,
        };
        Paragraph::new(text)
            .wrap(Wrap { trim: false })
            .render(area, buf);
    })?;

    Ok(height)
}

/// Parse pre-rendered ANSI bytes into a styled ratatui [`Text`]. On parse
/// error fall back to rendering the bytes as lossy UTF-8 so we never drop a
/// log line entirely — a garbled line is better than a silent one.
fn parse_ansi(bytes: &[u8]) -> Text<'static> {
    match bytes.into_text() {
        Ok(text) => text,
        Err(_) => Text::raw(String::from_utf8_lossy(bytes).into_owned()),
    }
}
