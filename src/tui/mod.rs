//! Interactive terminal UI for `don start`.
//!
//! The TUI owns the whole screen: raw mode, the alternate screen, one ratatui
//! [`Terminal`] with a full-screen viewport. Pipe mode (non-TTY) bypasses this
//! module entirely — [`OutputManager`] still writes prefixed bytes directly to
//! stdout in that case.
//!
//! Heavy formatting (color prefix, ANSI sanitization, verbose timestamps) is
//! already done upstream; we parse those pre-rendered ANSI bytes back into
//! styled [`Line`]s via `ansi-to-tui` so ratatui can render them.
//!
//! ## Why one screen
//!
//! This used to be a hybrid: logs went into the terminal's *native* scrollback
//! through `Terminal::insert_before`, a one-row inline viewport held the status
//! bar, and every full-screen view rendered into a **second** alt-screen
//! `Terminal` layered on top. Two screens meant two histories, and the seam
//! between them needed constant repair — replay checkpoints to re-emit lines
//! that arrived while a modal was up, a clear-and-replay pass on every filter
//! change, and a backend wrapper whose entire job was dodging the cursor-position
//! query that an inline viewport forces.
//!
//! None of that exists here. There is one buffer, and [`LogStore`] is the only
//! history. A filter change is a different *view* of the same store rather than
//! a screen wipe and a replay, which is what makes it impossible for the log to
//! drift out of sync with what the user asked to see.
//!
//! ## Concurrency
//!
//! One `tokio::select!` loop owns [`App`], the [`Terminal`] and the
//! [`LogStore`]. Its arms never draw — they mutate state and mark it dirty, and
//! a single rate-capped arm draws the whole screen. So a burst of ten thousand
//! log lines costs one repaint rather than ten thousand writes, and no arm has
//! to know what any other arm would have wanted redrawn.
//!
//! Four side channels feed it:
//! - Merged log events from [`OutputManager`], carrying don's own [`LogId`]s.
//! - [`RunnerEvent`]s from the runner broadcast (consumed directly, no side task).
//! - Input events from the input task (interpretation is mode-dependent).
//! - A timer, for the spinner and relative timestamps.
//!
//! [`OutputManager`]: crate::output::OutputManager
//! [`Terminal`]: ratatui::Terminal
//! [`LogId`]: crate::output::LogId
//! [`Line`]: ratatui::text::Line

mod app;
mod events;
mod failure_summary;
mod filter;
mod form;
mod fuzzy;
mod input;
mod log_store;
mod logs;
mod panes;
mod render;
mod selection;
mod status_table;
mod view_index;

use ansi_to_tui::IntoText;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use crossterm::execute;
use crossterm::style::Print;
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Rect;
use ratatui::text::Text;
use ratatui::{TerminalOptions, Viewport};
use tokio::sync::mpsc;

use crate::client::{Client, EventStreamItem, RunnerEvent, ServiceState, StateSnapshot};
use crate::config::ParamKind;
use crate::output::{FormattedLogLine, LifecycleEmitter, VerbosityControl};
use app::{App, AppInit, ViewMode, line_matches_log_popup};
use events::AppEvent;
use log_store::{DEFAULT_CAPACITY, LogStore};
use status_table::StatusTableKeyOutcome;

/// Errors that can escape the TUI event loop.
#[derive(Debug, thiserror::Error)]
pub enum TuiError {
    /// Raw mode toggle, cursor operations, ratatui backend IO.
    #[error("terminal io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Owns the terminal for as long as the TUI does.
///
/// Raw mode and the alternate screen go up together and come down together in
/// reverse order, from `Drop` — so a `?` anywhere in the loop still gives the
/// user their terminal back, and so does a panic unwinding through it.
struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self, TuiError> {
        crossterm::terminal::enable_raw_mode()?;
        execute!(std::io::stdout(), EnterAlternateScreen, Print(MOUSE_ON))?;
        Ok(Self)
    }

    /// Hand the terminal back for something else to use — `don attach`
    /// bridging the user into a process's PTY.
    ///
    /// Returns a token that must be used to take it again, so the two halves
    /// cannot drift apart: there is no way to release without a plan to
    /// re-acquire, and no way to re-acquire twice.
    fn release(&self) -> Result<ReleasedTerminal, TuiError> {
        execute!(std::io::stdout(), Print(MOUSE_OFF), LeaveAlternateScreen)?;
        crossterm::terminal::disable_raw_mode()?;
        Ok(ReleasedTerminal)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(std::io::stdout(), Print(MOUSE_OFF), LeaveAlternateScreen);
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

/// Proof that the terminal is currently somebody else's.
#[must_use = "the terminal stays released until this is retaken"]
struct ReleasedTerminal;

impl ReleasedTerminal {
    fn retake(self) -> Result<(), TuiError> {
        crossterm::terminal::enable_raw_mode()?;
        execute!(std::io::stdout(), EnterAlternateScreen, Print(MOUSE_ON))?;
        Ok(())
    }
}

/// The mouse reporting modes don actually uses.
///
/// Deliberately *not* crossterm's `EnableMouseCapture`, which also turns on
/// `?1003h` — "report every motion event". don has no hover behaviour, so every
/// one of those reports is parsed and thrown away; what they cost is a flood of
/// input for as long as the pointer merely crosses the window. That is enough
/// to fill the input channel and leave a keystroke queued behind hundreds of
/// events nobody wanted, which reads as the UI ignoring you.
///
/// `?1000h` reports press and release. `?1002h` adds motion *while a button is
/// held*, which is what drag-select and the divider drag need and all they
/// need. `?1006h` asks for SGR coordinates so columns past 223 are reportable —
/// crossterm also sets the older `?1015h` (RXVT) alongside it, which some
/// terminals answer in *both* encodings.
const MOUSE_ON: &str = "\x1b[?1000h\x1b[?1002h\x1b[?1006h";
/// The same modes, reset in reverse.
const MOUSE_OFF: &str = "\x1b[?1006l\x1b[?1002l\x1b[?1000l";

type TuiTerminal = Terminal<CrosstermBackend<std::io::Stdout>>;

/// Shortest gap between full repaints.
///
/// The loop marks state dirty and this decides when that becomes pixels. Under
/// a log flood the cost of the TUI is therefore bounded by the frame rate
/// rather than by the line rate — which is the property the old
/// `insert_before` model could not have, since every line was a write.
const FRAME_INTERVAL: std::time::Duration = std::time::Duration::from_millis(16);

/// Whether this TUI shares a process with the runner or attached over the
/// socket. The two differ in exactly two keys:
///
/// - **Ctrl+C**: in-process raises SIGINT alongside the shutdown request so
///   the runner's two-press force-kill escalation still works; a remote
///   client must not signal itself (there is no runner in this process to
///   catch it — just a TUI to kill mid-raw-mode) and settles for the
///   graceful request.
/// - **Ctrl+D**: remote detaches — exit the TUI, leave the stack running.
///   In-process there is nothing to detach *to* — the runner shares this
///   process — so it is ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuiMode {
    /// The TUI shares a process with the runner (`don start` today).
    InProcess,
    /// The TUI attached to a running project over the socket (`don tui`).
    Remote,
}

#[derive(Clone)]
struct TuiControls {
    verbosity: VerbosityControl,
    lifecycle_emitter: LifecycleEmitter,
    mode: TuiMode,
}

/// Flatten one merged-stream event into the batch the render loop consumes.
///
/// A drop is rendered as a lifecycle line so it lands in the log where the
/// missing lines would have been, rather than in a corner of the status bar.
/// It carries the id the stream resumed at, so its position in the store is
/// the truth about where the hole is.
fn push_merged_event(
    batch: &mut Vec<(crate::output::LogId, FormattedLogLine)>,
    event: crate::output::MergedEvent,
) {
    match event {
        crate::output::MergedEvent::Line(entry) => {
            batch.push((entry.id, (*entry.line).clone()));
        }
        crate::output::MergedEvent::Dropped { count, resumed_at } => batch.push((
            resumed_at,
            FormattedLogLine {
                name: crate::output::LIFECYCLE_EVENT_NAME.to_string(),
                is_lifecycle: true,
                is_verbose: false,
                // The gap notice is the TUI's own, so it has no prefix from
                // the sink to sit under.
                prefix: Vec::new(),
                bytes: format!(
                    "{count} log line(s) dropped — history did not reach back far enough"
                )
                .into_bytes(),
            },
        )),
    }
}

/// Run the interactive TUI until the runner shuts down or the user quits.
///
/// Ctrl+C raises SIGINT to our own process so the installed signal handler
/// drives graceful shutdown (identical behavior to pipe mode, including the
/// two-Ctrl+C force-kill escalation).
#[allow(clippy::too_many_arguments)]
pub async fn run_tui(
    mut log_rx: mpsc::UnboundedReceiver<crate::output::MergedEvent>,
    client: Client,
    mode: TuiMode,
    verbosity: VerbosityControl,
    lifecycle_emitter: LifecycleEmitter,
    service_names: Vec<String>,
    task_names: Vec<String>,
    build_tool_names: Vec<String>,
    task_configs: std::collections::HashMap<String, crate::config::Task>,
    task_last_runs: std::collections::HashMap<String, crate::task_state::TaskRunInfo>,
    hidden_names: std::collections::HashSet<String>,
    auto_filter_on_failure_names: std::collections::HashSet<String>,
    cli_log_filter: Option<std::collections::HashSet<String>>,
) -> Result<(), TuiError> {
    let controls = TuiControls {
        verbosity,
        lifecycle_emitter,
        mode,
    };
    let client = std::sync::Arc::new(client);

    // Follow the runner's event stream as a client. The first record is a
    // state snapshot (see `GET /events`), so the view starts consistent no
    // matter when the connection lands relative to startup. The task ends
    // when the server closes the stream (shutdown) or the TUI drops the
    // receiver; either way the loop's `None` arm takes over.
    let (events_tx, mut events_rx) = mpsc::unbounded_channel::<EventStreamItem>();
    {
        let client = client.clone();
        tokio::spawn(async move {
            let _ = client
                .events_follow_typed(|item| {
                    events_tx
                        .send(item)
                        .map_err(|_| crate::client::ClientError::Invalid("tui closed".into()))
                })
                .await;
        });
    }

    let mut app = App::new(AppInit {
        service_names,
        task_names,
        build_tool_names,
        task_configs,
        task_last_runs,
        hidden_names,
        auto_filter_on_failure_names,
        cli_log_filter,
        verbose_enabled: controls.verbosity.is_enabled(),
    });
    let mut store = LogStore::with_capacity(DEFAULT_CAPACITY);

    let (input_tx, mut input_rx) = mpsc::channel::<AppEvent>(64);
    // Publish the sender so background tasks (completion replies) can
    // inject events back into the loop. Set once for the lifetime of the
    // process — across pause/resume cycles we keep the same sender so
    // pending replies can still land.
    let _ = INPUT_TX.set(input_tx.clone());
    // Tracks whether the input task's channel is still open. When the input
    // task exits (crossterm EventStream error), we gate its select arm off
    // so the select loop doesn't busy-spin on a perpetually-ready None.
    let mut input_open = true;

    // The terminal, owned for as long as the TUI runs. Torn down and rebuilt
    // only around an attach bridge, which needs the tty to itself.
    let _guard = TerminalGuard::enter()?;
    let mut terminal = build_terminal()?;
    let mut input_handle = tokio::spawn(input::run(input_tx.clone()));

    // Drives the spinner and relative timestamps ("5s ago"), which move
    // without any event arriving.
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Nothing below draws. Arms mutate `app` and set this; one arm turns it
    // into pixels, no more often than `FRAME_INTERVAL`. That is what bounds
    // the TUI's cost by frame rate rather than by log rate, and what stops
    // each arm having to know what any other arm would have wanted redrawn.
    let mut dirty = true;
    // One timer, reset after each frame — not a fresh `sleep_until` per loop
    // iteration. `select!` builds every branch's future each time round, so a
    // new sleep meant registering and cancelling a timer entry per iteration,
    // and under a log flood the loop goes round tens of thousands of times a
    // second.
    let frame = tokio::time::sleep_until(tokio::time::Instant::now());
    tokio::pin!(frame);

    // Cap on log events drained per select round. Large bursts still clear in
    // a few rounds, while runner events and input keep getting a turn — so
    // Ctrl+C stays responsive under a flood.
    const LOG_BATCH_LIMIT: usize = 512;

    loop {
        tokio::select! {
            maybe_event = log_rx.recv() => {
                match maybe_event {
                    Some(first) => {
                        let mut batch: Vec<(crate::output::LogId, FormattedLogLine)> =
                            Vec::with_capacity(LOG_BATCH_LIMIT);
                        push_merged_event(&mut batch, first);
                        while batch.len() < LOG_BATCH_LIMIT {
                            match log_rx.try_recv() {
                                Ok(event) => push_merged_event(&mut batch, event),
                                Err(_) => break,
                            }
                        }
                        if !batch.is_empty() && app.log_scroll == logs::Scroll::Follow {
                            // Following means every row shifts up; the
                            // selection's screen coordinates now point at
                            // different text, so it is no longer a selection.
                            app.log_selection.clear();
                        }
                        for (id, line) in batch {
                            if is_shutdown_start_line(&line) && !app.shutdown_started {
                                app.begin_shutdown();
                            }
                            app.append_log_popup_line(&line);
                            store.push(id, line);
                        }
                        dirty = true;
                    }
                    None => break, // runner closed the log channel — shut down
                }
            }
            runner_result = events_rx.recv() => {
                match runner_result {
                    Some(EventStreamItem::Event(RunnerEvent::ShutdownStarted)) => {
                        if !app.shutdown_started {
                            app.begin_shutdown();
                        }
                    }
                    Some(EventStreamItem::Event(event)) => {
                        apply_runner_event(event, &mut app);
                    }
                    Some(EventStreamItem::Snapshot { processes, startup_complete }) => {
                        // The stream's opening record — the authoritative state
                        // at connect time. Later events are newer or equal, so
                        // applying them after this is safe.
                        app.resync_from(&StateSnapshot { processes, startup_complete });
                    }
                    Some(EventStreamItem::Lagged(_)) => {
                        // Transitions were dropped, so the incremental view is
                        // wrong about an unknown set of processes and would stay
                        // wrong. Refetch off-loop and inject the result as an
                        // input event; awaiting here would wedge rendering
                        // behind a slow server.
                        spawn_state_resync(&client);
                    }
                    None => {}
                }
                let lazy: std::collections::HashSet<String> = app
                    .services_state
                    .iter()
                    .filter(|(_, s)| matches!(s, ServiceState::Lazy))
                    .map(|(n, _)| n.clone())
                    .collect();
                app.filter.set_hidden_from_display(lazy);
                dirty = true;
            }
            maybe_event = input_rx.recv(), if input_open => {
                match maybe_event {
                    Some(event) => {
                        handle_app_event(event, &mut app, &mut store, &client, &controls)?;
                        // Input arrives in bursts — a drag reports once per cell
                        // the pointer crosses — and handling one per `select!`
                        // round means a round trip each, so a burst spreads
                        // across frames and the UI lags behind the hand moving
                        // it. Draining what is already queued keeps a burst
                        // inside one frame.
                        loop {
                            if app.exit_requested || app.bridge_request.is_some() {
                                break;
                            }
                            match input_rx.try_recv() {
                                Ok(event) => handle_app_event(
                                    event, &mut app, &mut store, &client, &controls,
                                )?,
                                Err(_) => break,
                            }
                        }
                        dirty = true;
                        if app.exit_requested {
                            break;
                        }
                        if let Some(name) = app.bridge_request.take() {
                            // The bridge needs the tty to itself: stop reading
                            // stdin, give the screen back, run it, then take
                            // both again. Nothing is replayed on return — the
                            // store kept every line, so the next draw simply
                            // paints the current truth.
                            input_handle.abort();
                            let _ = (&mut input_handle).await;
                            let released = _guard.release()?;
                            let end = run_bridge(&client, &name).await;
                            released.retake()?;
                            terminal = build_terminal()?;
                            terminal.clear()?;
                            input_handle = tokio::spawn(input::run(input_tx.clone()));
                            if let Some(message) = end {
                                controls.lifecycle_emitter.lifecycle_event(&message);
                            }
                        }
                    }
                    None => input_open = false,
                }
            }
            _ = ticker.tick() => {
                app.spinner_frame = app.spinner_frame.wrapping_add(1);
                // The copy badge answers something the user just did; once it
                // has been read it is only taking up the slot the update notice
                // wants. OSC 52 never replies, so time is the only thing that
                // can retire it.
                if app
                    .copy_notice
                    .as_ref()
                    .is_some_and(|(_, at)| at.elapsed() > COPY_NOTICE_TTL)
                {
                    app.copy_notice = None;
                }
                dirty = true;
            }
            () = &mut frame, if dirty => {
                draw(&mut terminal, &mut app, &mut store)?;
                dirty = false;
                frame
                    .as_mut()
                    .reset(tokio::time::Instant::now() + FRAME_INTERVAL);
            }
        }
    }

    // One last frame so the final state — "shutdown complete" — is on screen
    // before the guard puts the user's terminal back.
    let _ = draw(&mut terminal, &mut app, &mut store);
    input_handle.abort();
    let _ = input_handle.await;
    Ok(())
}

/// Build the full-screen terminal.
///
/// `Viewport::Fullscreen` never probes the cursor position, so there is no DSR
/// response to race the input task's stdin reader for — the whole reason the
/// old inline viewport needed a backend wrapper around
/// `get_cursor_position`.
fn build_terminal() -> Result<TuiTerminal, TuiError> {
    let backend = CrosstermBackend::new(std::io::stdout());
    let terminal = Terminal::with_options(
        backend,
        TerminalOptions {
            viewport: Viewport::Fullscreen,
        },
    )?;
    Ok(terminal)
}

/// Paint the whole screen from `app` and `store`.
///
/// The store is reflowed to the log pane's width first: row counts have to be
/// current before the view can place its scroll anchor, and a resize is the
/// only time that costs anything.
fn draw(terminal: &mut TuiTerminal, app: &mut App, store: &mut LogStore) -> Result<(), TuiError> {
    let area: Rect = terminal.size()?.into();
    // A layout change moves what every cell means, and a diffing renderer only
    // rewrites the cells it believes changed — so whatever the old layout drew
    // outside the new one's reach stays on screen. Clearing costs a full
    // repaint, which is why it is done on the layout change rather than every
    // frame: opening, moving or resizing a pane is a thing people do
    // occasionally, not sixty times a second.
    if app.painted_layout != Some(app.status_pane) || app.repaint_requested {
        terminal.clear()?;
        app.painted_layout = Some(app.status_pane);
        app.repaint_requested = false;
    }
    store.reflow(render::log_pane_width(area, app.status_pane));
    // Hold history where the reader is looking, so a busy stack cannot evict
    // their place out from under them between frames.
    store.set_pin(app.log_scroll.anchor());
    // Selection may not reach into the name column. Refreshed here because both
    // the column's width and the pane's origin can move under it.
    app.log_selection.set_left_edge(
        render::log_pane_origin(area, app.status_pane)
            .0
            .saturating_add(u16::try_from(store.name_column()).unwrap_or(0)),
    );
    terminal.draw(|frame| render::draw(frame, app, store))?;
    Ok(())
}

fn is_shutdown_start_line(line: &FormattedLogLine) -> bool {
    line.name == crate::output::LIFECYCLE_EVENT_NAME
        && String::from_utf8_lossy(&line.bytes).contains("shutting down gracefully")
}

/// Apply one [`RunnerEvent`] to the cached state on [`App`].
fn apply_runner_event(event: RunnerEvent, app: &mut App) -> bool {
    match event {
        RunnerEvent::ServiceStateChanged {
            name,
            state,
            pid,
            failed_dependencies,
        } => app.apply_service_runtime(name, state, pid, failed_dependencies),
        RunnerEvent::TaskStateChanged {
            name,
            state,
            last_run,
            failed_dependencies,
        } => app.apply_task_state(name, state, last_run, failed_dependencies),
        RunnerEvent::UpdateCheck {
            current_version,
            latest_version,
        } => {
            app.set_update_check(current_version, latest_version);
            false
        }
        // The TUI already shows per-item states, so it learns nothing extra
        // from the sweep finishing — that signal is for API clients deciding
        // whether it's meaningful to ask the runner to run something.
        RunnerEvent::StartupSettled
        | RunnerEvent::ShutdownStarted
        | RunnerEvent::ShutdownComplete => false,
    }
}

/// Dispatch an input or resize event.
fn handle_app_event(
    event: AppEvent,
    app: &mut App,
    store: &mut LogStore,
    client: &std::sync::Arc<Client>,
    controls: &TuiControls,
) -> Result<(), TuiError> {
    match event {
        AppEvent::Resize => {
            // Nothing to do but redraw, which the caller has already asked for
            // by marking the frame dirty. Geometry is read fresh every frame
            // and the store reflows itself when the width moves; the scroll
            // anchor is a line id, so it means the same thing at any size.
        }
        AppEvent::Key(key) => handle_key(key, app, store, client, controls)?,
        AppEvent::Mouse(mouse) => handle_mouse(mouse, app),
        AppEvent::CompletionsReady {
            param,
            request_id,
            result,
        } => {
            if let Some(form) = app.form.as_mut() {
                form.apply_completions(&param, request_id, result);
            }
        }
        AppEvent::StateResync {
            processes,
            startup_complete,
        } => {
            app.resync_from(&StateSnapshot {
                processes,
                startup_complete,
            });
        }
    }
    Ok(())
}

fn handle_key(
    key: KeyEvent,
    app: &mut App,
    store: &mut LogStore,
    client: &std::sync::Arc<Client>,
    controls: &TuiControls,
) -> Result<(), TuiError> {
    // Ctrl+C: belt-and-suspenders shutdown. We both send a `Shutdown` command
    // directly down the runner channel AND raise SIGINT. The direct command
    // works even if the signal handler task has died or isn't being polled
    // (e.g., the runner is stuck pre-loop), and SIGINT preserves the
    // two-press force-kill escalation via the runner's signal counter.
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        match key.code {
            KeyCode::Char('c') => {
                // Second press escalates, matching the runner's own
                // "Ctrl+C again to force" contract — raw mode means the key
                // never becomes a SIGINT, so the escalation goes over the
                // API instead of through the signal counter.
                let force = app.shutdown_started;
                {
                    let client = client.clone();
                    tokio::spawn(async move {
                        let _ = if force {
                            client.shutdown_force().await
                        } else {
                            client.shutdown().await
                        };
                    });
                }
                if controls.mode == TuiMode::InProcess {
                    let _ = nix::sys::signal::kill(
                        nix::unistd::Pid::this(),
                        nix::sys::signal::Signal::SIGINT,
                    );
                }
            }
            // Detach: leave the stack running, exit this client. Only
            // meaningful for a remote TUI — see [`TuiMode`].
            KeyCode::Char('d') if controls.mode == TuiMode::Remote => {
                app.exit_requested = true;
            }
            KeyCode::Char('v') | KeyCode::Char('V') => {
                // Purely local display state: verbose lines are always in
                // the stream, this client just chooses to show them. The
                // toggle no longer touches the process-wide VerbosityControl,
                // so it cannot change what other consumers see or record.
                let enabled = !app.verbose_enabled;
                app.set_verbose_enabled(enabled);
                controls.lifecycle_emitter.lifecycle_event(if enabled {
                    "verbose logging enabled"
                } else {
                    "verbose logging disabled"
                });
            }
            _ => {}
        }
        return Ok(());
    }

    if app.shutdown_started {
        return Ok(());
    }

    match app.view_mode {
        ViewMode::Normal => handle_normal_key(key, app, store)?,
        ViewMode::Filter => handle_filter_key(key, app, store)?,
        ViewMode::Tasks => handle_tasks_key(key, app, store, client)?,
        ViewMode::Services => {
            handle_services_key(key, app, store, client, controls)?;
        }
        ViewMode::Failures => handle_failure_summary_key(key, app, store)?,
        ViewMode::Form => handle_form_key(key, app, store, client)?,
    }
    Ok(())
}

fn handle_normal_key(key: KeyEvent, app: &mut App, store: &mut LogStore) -> Result<(), TuiError> {
    match key.code {
        // Held above the tail, Enter is the way back down — the gesture
        // everyone tries first, and it costs nothing because the separator it
        // would otherwise insert only makes sense at the bottom anyway.
        KeyCode::Enter if app.log_scroll != logs::Scroll::Follow => {
            resume_following(app);
        }
        KeyCode::Enter => {
            // A local artifact, not a stream line: it borrows the id the next
            // real line is expected to have so replay keeps it in place.
            store.push(
                store.next_id(),
                FormattedLogLine {
                    name: String::new(),
                    is_verbose: false,
                    is_lifecycle: false,
                    prefix: Vec::new(),
                    bytes: Vec::new(),
                },
            );
        }
        // The conventional "something has scribbled on my screen" key. An
        // escape hatch, not a fix: anything that leaves the terminal and this
        // renderer disagreeing is a bug, but the user should not have to
        // restart the stack to get a clean screen back.
        KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.repaint_requested = true;
        }
        KeyCode::Char('l') => {
            app.filter.enter_edit();
            app.view_mode = ViewMode::Filter;
        }
        KeyCode::Char('t') => {
            app.tasks_table.reset();
            app.freeze_tasks_order();
            app.view_mode = ViewMode::Tasks;
        }
        KeyCode::Char('s') => {
            app.services_table.reset();
            app.freeze_services_order();
            app.view_mode = ViewMode::Services;
        }
        KeyCode::Char('i') if app.has_failure_summary() => {
            app.open_failure_summary();
        }
        KeyCode::Char('R') => {
            app.filter.reset_to_defaults();
        }
        // Scrolling the log. The pane's own history is the only history now,
        // so these are load-bearing rather than a convenience over the
        // terminal's scrollback.
        KeyCode::Up => scroll_log(app, -1),
        KeyCode::Down => scroll_log(app, 1),
        KeyCode::PageUp => scroll_log(app, -log_page(app)),
        KeyCode::PageDown => scroll_log(app, log_page(app)),
        KeyCode::Home => scroll_log(app, isize::MIN / 2),
        KeyCode::End => {
            app.log_selection.clear();
            app.follow_paused_for_selection = false;
            app.log_scroll = logs::Scroll::Follow;
        }
        // Ctrl+C is shutdown and cannot double as copy, so the keyboard route
        // to the clipboard is `y` — vi's yank, over the current selection.
        KeyCode::Char('y') => copy_selection(app),
        KeyCode::Esc => clear_selection(app),
        // The status pane sits *beside* the log rather than replacing it, so
        // opening it is not a mode change and does not interrupt reading.
        KeyCode::Char('p') => {
            app.status_pane.open = !app.status_pane.open;
            if !app.status_pane.open {
                app.focus = panes::Focus::Logs;
            }
        }
        KeyCode::Char('P') if app.status_pane.open => {
            app.status_pane.side = app.status_pane.side.toggled();
            // Extents mean different things on the two axes; start from the
            // default for the new one rather than carrying a column count over
            // into rows.
            app.status_pane.extent = match app.status_pane.side {
                panes::PaneSide::Right => 42,
                panes::PaneSide::Bottom => 12,
            };
        }
        KeyCode::Tab if app.status_pane.open => {
            app.focus = match app.focus {
                panes::Focus::Logs => panes::Focus::Status,
                panes::Focus::Status => panes::Focus::Logs,
            };
        }
        _ => {}
    }
    Ok(())
}

/// Rows a single wheel tick moves the log.
///
/// Three is the near-universal terminal default; matching it is what makes the
/// pane feel like the scrollback it replaced rather than like a widget.
const WHEEL_ROWS: isize = 3;

/// Apply a mouse event.
///
/// Wheel scrolling works in every mode: a full-screen table on top does not
/// mean the user has stopped caring where the log is, and moving it costs
/// nothing while it is hidden.
fn handle_mouse(mouse: MouseEvent, app: &mut App) {
    match mouse.kind {
        MouseEventKind::ScrollUp => scroll_log(app, -WHEEL_ROWS),
        MouseEventKind::ScrollDown => scroll_log(app, WHEEL_ROWS),
        // A click in the log pane takes focus back from any overlay, which is
        // the gesture people reach for before they remember `esc`.
        MouseEventKind::Down(MouseButton::Left) => {
            // The divider is checked first: it is one cell wide and sits
            // between two panes, so "did they mean to drag it" has to be
            // answered before "which pane did they click".
            if app.panes.on_divider(mouse.column, mouse.row) {
                app.dragging_divider = true;
                return;
            }
            let Some(focus) = app.panes.hit(mouse.column, mouse.row) else {
                return;
            };
            app.focus = focus;
            if focus != panes::Focus::Logs || app.view_mode != ViewMode::Normal {
                return;
            }
            match click_count(app, mouse.column, mouse.row) {
                // A drag is about to start, or a plain click clearing what was
                // selected before.
                1 => {
                    clear_selection(app);
                    app.log_selection.begin(mouse.column, mouse.row);
                }
                2 => select_span(app, mouse.column, mouse.row, selection::word_at),
                // Triple-click means "this log line". It does not need to
                // skip don's own `name | ` chrome itself: the selection clamps
                // to the message column's left edge, so every route into it —
                // drag, double-click, this — starts in the same place.
                _ => select_span(app, mouse.column, mouse.row, |row, _| {
                    selection::line_extent(row)
                }),
            }
        }
        MouseEventKind::Drag(MouseButton::Left) => {
            if app.dragging_divider {
                let area = ratatui::layout::Rect::new(
                    0,
                    0,
                    app.panes.logs.width + app.panes.status.map_or(0, |s| s.width + 1),
                    app.panes.logs.height + app.panes.status.map_or(0, |s| s.height + 1),
                );
                app.status_pane.extent =
                    panes::extent_from_drag(area, app.status_pane.side, mouse.column, mouse.row);
                return;
            }
            // The rows under a selection have to stop moving, or output
            // arriving mid-drag pulls the text out from under the pointer.
            pause_following_for_selection(app);
            app.log_selection.extend(mouse.column, mouse.row);
        }
        MouseEventKind::Up(MouseButton::Left) => {
            app.dragging_divider = false;
            app.log_selection.finish();
        }
        _ => {}
    }
}

/// How long two clicks may be apart and still count as a double click.
///
/// The usual desktop default. Long enough not to demand a fast hand, short
/// enough that two deliberate clicks on the same word are not mistaken for one.
const MULTI_CLICK_WINDOW: std::time::Duration = std::time::Duration::from_millis(400);

/// How long the copy badge stays up. Long enough to read, short enough that it
/// is clearly about the thing you just pressed.
const COPY_NOTICE_TTL: std::time::Duration = std::time::Duration::from_secs(4);

/// How many clicks have now landed in the same place in a row: 1, 2 or 3.
///
/// Terminals report a double click as two ordinary presses; only the gap and
/// the position tell them apart, so the counting has to happen here.
fn click_count(app: &mut App, column: u16, row: u16) -> u8 {
    let count = match app.last_click {
        Some((last_col, last_row, at, count))
            if last_row == row
                && last_col.abs_diff(column) <= 1
                && at.elapsed() <= MULTI_CLICK_WINDOW =>
        {
            // Past a triple, start over rather than inventing a quadruple
            // click nothing has a meaning for.
            if count >= 3 { 1 } else { count + 1 }
        }
        _ => 1,
    };
    app.last_click = Some((column, row, std::time::Instant::now(), count));
    count
}

/// Select whatever `span` picks out of the clicked row.
///
/// Resolved against the rows the last frame drew, so a double click lands on
/// the word actually under the pointer — after wrapping, filtering and scroll,
/// none of which this has to know about.
fn select_span(
    app: &mut App,
    column: u16,
    row: u16,
    span: impl Fn(&str, usize) -> Option<(usize, usize)>,
) {
    let (origin_x, origin_y) = app.log_pane_origin;
    let Some(index) = row.checked_sub(origin_y).map(usize::from) else {
        return;
    };
    let Some(text) = app.log_visible_rows.get(index) else {
        return;
    };
    let within = usize::from(column.saturating_sub(origin_x));
    let Some((start, end)) = span(text, within) else {
        clear_selection(app);
        return;
    };
    // Same reason a drag freezes the view: the selection is screen
    // coordinates, so the rows under it have to stop moving.
    pause_following_for_selection(app);
    let to_screen = |col: usize| origin_x.saturating_add(u16::try_from(col).unwrap_or(u16::MAX));
    app.log_selection.begin(to_screen(start), row);
    app.log_selection.extend(to_screen(end), row);
    app.log_selection.finish();
}

/// Hold the view still while a selection stands.
///
/// A selection is screen coordinates over the rows a frame drew. Following
/// means those rows move, so a selection made while following would be
/// pointing at different text a frame later — which is why copy used to have
/// to happen on mouse-release. Freezing instead is what lets the copy be
/// explicit: the selection stays exactly what the user dragged across until
/// they act on it.
fn pause_following_for_selection(app: &mut App) {
    if app.log_scroll != logs::Scroll::Follow {
        return;
    }
    app.log_scroll = logs::anchor_at(&app.view_index, app.log_rows_above);
    app.follow_paused_for_selection = true;
}

/// Drop the selection, and go back to the live tail if it was what stopped us
/// following. A reader who had scrolled up on purpose stays where they were.
fn clear_selection(app: &mut App) {
    app.log_selection.clear();
    if app.follow_paused_for_selection {
        resume_following(app);
    }
}

fn resume_following(app: &mut App) {
    app.follow_paused_for_selection = false;
    app.log_scroll = logs::Scroll::Follow;
}

/// Put the current selection on the clipboard, and say so.
fn copy_selection(app: &mut App) {
    let Some(text) = selection::selected_text(
        &app.log_selection,
        &app.log_visible_rows,
        app.log_pane_origin,
    ) else {
        return;
    };
    let lines = text.lines().count();
    let now = std::time::Instant::now();
    app.copy_notice = Some(match selection::copy_to_clipboard(&text) {
        // OSC 52 is a request with no reply: a terminal that has it turned off
        // discards it silently. Reporting what was sent is the only honest
        // thing available — "copied" here means "asked the terminal to".
        Ok(()) => (format!("copied {lines} line(s)"), now),
        Err(e) => (format!("copy failed: {e}"), now),
    });
}

/// One screenful, minus a row of overlap so the reader keeps their place.
fn log_page(app: &App) -> isize {
    isize::from(app.log_pane_height.saturating_sub(1).max(1) as i16)
}

/// Move the log view by `delta` rows.
///
/// Geometry comes from what the last frame measured — only the renderer knows
/// how tall the pane came out and how much admitted content there is at this
/// width. A resize between then and now costs one imprecise scroll, which the
/// next frame corrects.
fn scroll_log(app: &mut App, delta: isize) {
    // The selection is in screen coordinates, so it stops meaning anything the
    // moment different content is under those cells — same as a terminal drops
    // its own selection when you scroll. Scrolling is also a deliberate choice
    // of where to be, so it takes ownership of the view from the selection that
    // paused it.
    app.log_selection.clear();
    app.follow_paused_for_selection = false;
    let (scroll, landed_on) = logs::scrolled(
        &app.view_index,
        app.log_rows_above,
        app.log_total_rows,
        app.log_pane_height,
        delta,
    );
    app.log_scroll = scroll;
    // Move the measured offset along with it. Wheel events arrive in bursts —
    // several per frame when someone spins the wheel — and `log_rows_above` is
    // only refreshed when a frame is drawn. Without this every event in a burst
    // starts from the same stale offset and computes the same destination, so
    // ten notches scroll exactly as far as one. The next frame overwrites this
    // with the real measurement.
    app.log_rows_above = landed_on;
}

fn handle_failure_summary_key(
    key: KeyEvent,
    app: &mut App,
    _store: &mut LogStore,
) -> Result<(), TuiError> {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('i') => {
            app.view_mode = ViewMode::Normal;
            app.failure_summary_scroll = 0;
            return Ok(());
        }
        KeyCode::Up | KeyCode::Char('k') => app.scroll_failure_summary_by(-1),
        KeyCode::Down | KeyCode::Char('j') => app.scroll_failure_summary_by(1),
        KeyCode::PageUp => app.scroll_failure_summary_by(-10),
        KeyCode::PageDown => app.scroll_failure_summary_by(10),
        KeyCode::Home | KeyCode::Char('g') => app.scroll_failure_summary_to_top(),
        KeyCode::End | KeyCode::Char('G') => app.scroll_failure_summary_to_bottom(),
        _ => return Ok(()),
    }
    Ok(())
}

fn handle_filter_key(key: KeyEvent, app: &mut App, _store: &mut LogStore) -> Result<(), TuiError> {
    if app.filter.query_editing() {
        match key.code {
            KeyCode::Enter => {
                let close_after_apply = app.filter.query_has_single_match();
                app.filter.apply_query();
                app.filter.end_query_edit();
                if close_after_apply {
                    app.filter.commit();
                    app.view_mode = ViewMode::Normal;
                }
            }
            KeyCode::Tab => {
                app.filter.end_query_edit();
            }
            KeyCode::Backspace => {
                app.filter.pop_query_char();
            }
            KeyCode::Char(c) => {
                app.filter.push_query_char(c);
            }
            KeyCode::Esc => {
                app.filter.cancel_edit();
                app.view_mode = ViewMode::Normal;
            }
            _ => {}
        }
        return Ok(());
    }

    match key.code {
        KeyCode::Enter => {
            app.filter.commit();
            app.view_mode = ViewMode::Normal;
        }
        KeyCode::Esc => {
            app.filter.cancel_edit();
            app.view_mode = ViewMode::Normal;
        }
        KeyCode::Char('R') => {
            app.filter.reset_edit_to_defaults();
        }
        KeyCode::Char(' ') => {
            app.filter.toggle_highlighted();
        }
        KeyCode::Char('o') => {
            app.filter.select_only_highlighted();
        }
        KeyCode::Char('/') => {
            app.filter.begin_query_edit();
        }
        KeyCode::Tab => {
            app.filter.begin_query_edit();
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.filter.highlight_prev();
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.filter.highlight_next();
        }
        _ => {}
    }
    Ok(())
}

fn handle_tasks_key(
    key: KeyEvent,
    app: &mut App,
    store: &mut LogStore,
    client: &std::sync::Arc<Client>,
) -> Result<(), TuiError> {
    let total = app.task_items().len();
    if app.log_popup.is_some() {
        handle_log_popup_key(key, app);
        return Ok(());
    }
    match app.tasks_table.handle_key(key, total) {
        StatusTableKeyOutcome::Redraw => {
            return Ok(());
        }
        StatusTableKeyOutcome::Close => {
            app.view_mode = ViewMode::Normal;
            return Ok(());
        }
        StatusTableKeyOutcome::None => {}
    }

    if key.code == KeyCode::Enter {
        let Some(item) = highlighted_task_item(app) else {
            return Ok(());
        };
        if !item.runnable() {
            return Ok(());
        }
        if item.has_params {
            open_form_for_task(app, &item.name, client)?;
        } else {
            let task_name = item.name;
            dispatch_run_task(client, task_name.clone());
            return_to_logs_after_task_run(&task_name, app, store)?;
        }
    } else if key.code == KeyCode::Char('l') {
        let Some(item) = highlighted_task_item(app) else {
            return Ok(());
        };
        open_log_popup_for_name(app, store, item.name);
    } else if key.code == KeyCode::Char('a') {
        // Bridge into the highlighted task's PTY — the interactive-task flow.
        if let Some(item) = highlighted_task_item(app) {
            app.bridge_request = Some(item.name);
        }
    }
    Ok(())
}

fn handle_services_key(
    key: KeyEvent,
    app: &mut App,
    store: &mut LogStore,
    client: &std::sync::Arc<Client>,
    controls: &TuiControls,
) -> Result<(), TuiError> {
    let total = app.service_items().len();
    if app.log_popup.is_some() {
        handle_log_popup_key(key, app);
        return Ok(());
    }
    match app.services_table.handle_key(key, total) {
        StatusTableKeyOutcome::Redraw => {
            return Ok(());
        }
        StatusTableKeyOutcome::Close => {
            app.view_mode = ViewMode::Normal;
            return Ok(());
        }
        StatusTableKeyOutcome::None => {}
    }

    match key.code {
        KeyCode::Enter => {
            // Start or stop the highlighted service, depending on its state.
            if let Some(cmd) = overlay_toggle_command(app) {
                dispatch_overlay_command(client, &controls.lifecycle_emitter, cmd);
            }
        }
        KeyCode::Char('r') => {
            // Restart the highlighted service, if it's in a state that can
            // be restarted.
            if let Some(cmd) = highlighted_service_restart_command(app) {
                dispatch_overlay_command(client, &controls.lifecycle_emitter, cmd);
            }
        }
        KeyCode::Char('R') => {
            // Hard restart the highlighted service: force a rebuild, then
            // start/restart it on success.
            if let Some(cmd) = highlighted_service_hard_restart_command(app) {
                dispatch_overlay_command(client, &controls.lifecycle_emitter, cmd);
            }
        }
        KeyCode::Char('l') => {
            let Some(item) = highlighted_service_item(app) else {
                return Ok(());
            };
            open_log_popup_for_name(app, store, item.name);
        }
        KeyCode::Char('a') => {
            // Bridge into the highlighted service's PTY.
            if let Some(item) = highlighted_service_item(app) {
                app.bridge_request = Some(item.name);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Run one bridge session against `name`, with a banner on each side.
/// Returns a lifecycle message to emit after the TUI is rebuilt, if the
/// session ended in a way worth narrating.
async fn run_bridge(client: &std::sync::Arc<Client>, name: &str) -> Option<String> {
    {
        use std::io::Write;
        let banner =
            format!("── bridged into '{name}' — Ctrl+P Ctrl+Q returns to the dashboard ──\r\n");
        let mut stdout = std::io::stdout();
        let _ = stdout.write_all(banner.as_bytes());
        let _ = stdout.flush();
    }
    match crate::client::attach::bridge_once(client.socket_path(), name).await {
        crate::client::attach::BridgeEnd::Escape => None,
        crate::client::attach::BridgeEnd::ServerDisconnect => Some(format!(
            "'{name}' bridge ended (process exited or restarted)"
        )),
        crate::client::attach::BridgeEnd::Error(e) => Some(format!("attach '{name}' failed: {e}")),
    }
}

fn handle_log_popup_key(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => app.close_log_popup(),
        KeyCode::Up | KeyCode::Char('k') => app.scroll_log_popup_by(-1),
        KeyCode::Down | KeyCode::Char('j') => app.scroll_log_popup_by(1),
        KeyCode::PageUp => app.scroll_log_popup_by(-10),
        KeyCode::PageDown => app.scroll_log_popup_by(10),
        KeyCode::Home | KeyCode::Char('g') => app.scroll_log_popup_to_top(),
        KeyCode::End | KeyCode::Char('G') => app.scroll_log_popup_to_bottom(),
        _ => {}
    }
}

fn open_log_popup_for_name(app: &mut App, store: &LogStore, name: String) {
    let lines = store
        .iter()
        .filter(|entry| line_matches_log_popup(&name, &entry.line))
        .map(|entry| entry.line.bytes.clone())
        .collect();
    app.open_log_popup(name, lines);
}

fn highlighted_task_item(app: &App) -> Option<app::TaskStatusItem> {
    let items = app.task_items();
    let idx = app.tasks_table.selected_index(items.len())?;
    items.get(idx).cloned()
}

fn highlighted_service_item(app: &App) -> Option<app::OverlayItem> {
    let items = app.service_items();
    let idx = app.services_table.selected_index(items.len())?;
    items.get(idx).cloned()
}

/// Build the Start/Stop command for the highlighted row, if it's an
/// actionable service. Returns `None` for in-flight services or when no row
/// is highlighted.
fn overlay_toggle_command(app: &App) -> Option<OverlayCommand> {
    let items = app.service_items();
    let idx = app.services_table.selected_index(items.len())?;
    let item = items.get(idx)?;
    match item.state {
        ServiceState::Ready | ServiceState::Running | ServiceState::Unhealthy => {
            Some(overlay_stop_command(item.name.clone()))
        }
        ServiceState::Stopped | ServiceState::Lazy => {
            Some(overlay_start_command(item.name.clone()))
        }
        ServiceState::Failed | ServiceState::DependencyFailed => {
            Some(overlay_stop_command(item.name.clone()))
        }
        ServiceState::Pending
        | ServiceState::Building
        | ServiceState::Starting
        | ServiceState::Stopping => None,
    }
}

/// Restart command for `r` — only services in a restartable state.
fn highlighted_service_restart_command(app: &App) -> Option<OverlayCommand> {
    let items = app.service_items();
    let idx = app.services_table.selected_index(items.len())?;
    let item = items.get(idx)?;
    match item.state {
        ServiceState::Ready
        | ServiceState::Running
        | ServiceState::Unhealthy
        | ServiceState::Failed
        | ServiceState::DependencyFailed
        | ServiceState::Stopped => Some(overlay_restart_command(item.name.clone())),
        _ => None,
    }
}

/// Hard restart command for `R` — only services in a restartable state.
fn highlighted_service_hard_restart_command(app: &App) -> Option<OverlayCommand> {
    let items = app.service_items();
    let idx = app.services_table.selected_index(items.len())?;
    let item = items.get(idx)?;
    match item.state {
        ServiceState::Ready
        | ServiceState::Running
        | ServiceState::Unhealthy
        | ServiceState::Failed
        | ServiceState::DependencyFailed
        | ServiceState::Stopped
        | ServiceState::Lazy => Some(overlay_hard_restart_command(item.name.clone())),
        _ => None,
    }
}

/// Which control endpoint an overlay action maps to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlAction {
    Start,
    Stop,
    Restart,
    HardRestart,
}

impl ControlAction {
    fn label(self) -> &'static str {
        match self {
            ControlAction::Start => "start",
            ControlAction::Stop => "stop",
            ControlAction::Restart => "restart",
            ControlAction::HardRestart => "hard restart",
        }
    }
}

struct OverlayCommand {
    name: String,
    action: ControlAction,
}

fn overlay_start_command(name: String) -> OverlayCommand {
    OverlayCommand {
        name,
        action: ControlAction::Start,
    }
}

fn overlay_stop_command(name: String) -> OverlayCommand {
    OverlayCommand {
        name,
        action: ControlAction::Stop,
    }
}

fn overlay_restart_command(name: String) -> OverlayCommand {
    OverlayCommand {
        name,
        action: ControlAction::Restart,
    }
}

fn overlay_hard_restart_command(name: String) -> OverlayCommand {
    OverlayCommand {
        name,
        action: ControlAction::HardRestart,
    }
}

fn dispatch_overlay_command(
    client: &std::sync::Arc<Client>,
    emitter: &LifecycleEmitter,
    pending: OverlayCommand,
) {
    let client = client.clone();
    let emitter = emitter.clone();
    tokio::spawn(async move {
        let label = pending.action.label();
        emitter.service_event(&pending.name, &format!("{label} requested"));
        let result = match pending.action {
            ControlAction::Start => client.start(&pending.name).await,
            ControlAction::Stop => client.stop(&pending.name).await,
            ControlAction::Restart => client.restart(&pending.name).await,
            ControlAction::HardRestart => client.hard_restart(&pending.name).await,
        };
        if let Err(e) = result {
            emitter.service_error_event(&pending.name, &format!("{label} failed: {e}"));
        }
    });
}

/// Refetch the state projection off-loop and inject it as an input event.
///
/// Used when the event stream reports lag: the fetch must not run on the
/// render loop (a slow server would freeze the UI), and the result must
/// come back through the input channel so it is applied in order with
/// whatever the user is doing.
fn spawn_state_resync(client: &std::sync::Arc<Client>) {
    let Some(input_tx) = app_input_tx().cloned() else {
        return;
    };
    let client = client.clone();
    tokio::spawn(async move {
        let Ok(processes) = client.status(false, None).await else {
            return;
        };
        let startup_complete = client.ready().await.unwrap_or(false);
        let _ = input_tx
            .send(AppEvent::StateResync {
                processes,
                startup_complete,
            })
            .await;
    });
}

fn return_to_logs_after_task_run(
    task_name: &str,
    app: &mut App,
    _store: &LogStore,
) -> Result<(), TuiError> {
    let filter_changed = app.filter.select_name(task_name);
    app.view_mode = ViewMode::Normal;
    app.log_popup = None;

    // Nothing to redraw here: the filter change is a different view over the
    // same store, and the loop paints it on the next frame.
    let _ = filter_changed;
    Ok(())
}

/// Fire a param-less task run without waiting for the outcome. State
/// updates come through the event stream like any other transition.
fn dispatch_run_task(client: &std::sync::Arc<Client>, name: String) {
    dispatch_run_task_with_params(client, name, std::collections::HashMap::new());
}

/// Fire a task run with the params map the user just submitted via the
/// form modal. The HTTP result is swallowed on success; failures surface
/// through the event stream (`task_state_changed` → failed).
fn dispatch_run_task_with_params(
    client: &std::sync::Arc<Client>,
    name: String,
    params: std::collections::HashMap<String, String>,
) {
    let client = client.clone();
    tokio::spawn(async move {
        let _ = client.run_task(&name, params).await;
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
    client: &std::sync::Arc<Client>,
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
        request_form_completion(app, task_name, &param, false, client);
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
    client: &std::sync::Arc<Client>,
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

    let client = client.clone();
    let Some(input_tx) = app_input_tx().cloned() else {
        return;
    };
    let task = task.to_string();
    let param = param.to_string();
    tokio::spawn(async move {
        let result = client
            .resolve_completions(&task, &param, partial, force_refresh)
            .await
            .map_err(|e| match e {
                // The server ran the completion command and it failed —
                // structured, with the log path the form renders.
                crate::client::ClientError::Completion(err) => err,
                // Transport-level failure — degrade to a plain message.
                other => crate::client::CompletionError {
                    message: other.to_string(),
                    log_path: None,
                },
            });
        let _ = input_tx
            .send(AppEvent::CompletionsReady {
                param,
                request_id,
                result,
            })
            .await;
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
    store: &mut LogStore,
    client: &std::sync::Arc<Client>,
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
            return Ok(());
        }
        KeyCode::Enter if ctrl => {
            // Submit regardless of focused field.
            try_submit_form(app, client, store)?;
            return Ok(());
        }
        KeyCode::Enter => {
            if let Some(form) = app.form.as_mut()
                && let Some(field) = form.focused_mut()
                && !matches!(field.kind, ParamKind::Bool)
                && !field.visible_candidates().is_empty()
            {
                field.accept_highlighted_candidate();
            }
            // If focused field is on the last row → submit. Otherwise advance.
            if let Some(form) = app.form.as_ref()
                && form.focus + 1 >= form.fields.len()
            {
                try_submit_form(app, client, store)?;
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
                request_form_completion(app, &task_name, &param, true, client);
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
    Ok(())
}

/// Attempt to submit the form. On success: dispatch `RunnerCommand::RunTask`,
/// close the modal, return to Normal. On validation error: record it on the
/// form so the renderer can show it, and stay open.
fn try_submit_form(
    app: &mut App,
    client: &std::sync::Arc<Client>,
    store: &mut LogStore,
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
                return Ok(());
            }
        }
    };
    dispatch_run_task_with_params(client, task_name.clone(), params);
    app.form = None;
    return_to_logs_after_task_run(&task_name, app, store)?;
    Ok(())
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

/// Parse one upstream-formatted line into its styled form.
///
/// Upstream guarantees one line per message, so a multi-line parse result can
/// only come from embedded newlines that sanitization let through; joining them
/// keeps the store's "one entry, one logical line" invariant, which the scroll
/// anchor depends on.
pub(crate) fn parse_ansi_line(bytes: &[u8]) -> ratatui::text::Line<'static> {
    let text = parse_ansi(bytes);
    let mut spans: Vec<ratatui::text::Span<'static>> = Vec::new();
    for line in text.lines {
        spans.extend(line.spans);
    }
    ratatui::text::Line::from(spans)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    fn app_with_service_state(state: ServiceState) -> App {
        let mut app = App::new(AppInit {
            service_names: vec!["api".to_string()],
            task_names: Vec::new(),
            build_tool_names: Vec::new(),
            task_configs: HashMap::new(),
            task_last_runs: HashMap::new(),
            hidden_names: HashSet::new(),
            auto_filter_on_failure_names: HashSet::new(),
            cli_log_filter: None,
            verbose_enabled: false,
        });
        app.apply_service_runtime("api".to_string(), state, None, Vec::new());
        app
    }

    /// A layout change has to force a full repaint, and the user needs a way to
    /// ask for one — the ordering of the `l` arms decides whether Ctrl+L is
    /// reachable at all, and an unguarded arm above it would silently swallow
    /// the chord into the log filter.
    #[test]
    fn a_layout_change_or_ctrl_l_asks_for_a_full_repaint() {
        use crossterm::event::{KeyEvent, KeyModifiers};

        struct Case {
            name: &'static str,
            key: KeyEvent,
            want_repaint: bool,
            want_filter_open: bool,
        }

        let cases = [
            Case {
                name: "ctrl+l asks for a repaint",
                key: KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL),
                want_repaint: true,
                want_filter_open: false,
            },
            Case {
                name: "plain l still opens the log filter",
                key: KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE),
                want_repaint: false,
                want_filter_open: true,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            let mut store = LogStore::with_capacity(10);
            handle_normal_key(case.key, &mut app, &mut store).unwrap();
            assert_eq!(
                app.repaint_requested, case.want_repaint,
                "{}: repaint requested",
                case.name
            );
            assert_eq!(
                app.view_mode == ViewMode::Filter,
                case.want_filter_open,
                "{}: filter opened",
                case.name
            );
        }
    }

    /// Wheel events arrive in bursts, several per frame. Each has to build on
    /// the last rather than on the offset the previous *frame* measured — or a
    /// whole flick of the wheel scrolls exactly as far as one notch, which is
    /// what made scrolling feel like it was ignoring you.
    #[test]
    fn a_burst_of_scrolls_accumulates_between_frames() {
        struct Case {
            name: &'static str,
            /// Deltas delivered without a frame in between.
            burst: &'static [isize],
            start: usize,
            want_rows_above: usize,
        }

        let cases = [
            Case {
                name: "one notch moves one notch",
                burst: &[-3],
                start: 500,
                want_rows_above: 497,
            },
            Case {
                name: "ten notches move ten notches",
                burst: &[-3, -3, -3, -3, -3, -3, -3, -3, -3, -3],
                start: 500,
                want_rows_above: 470,
            },
            Case {
                name: "back and forth nets out",
                burst: &[-3, -3, 3],
                start: 500,
                want_rows_above: 497,
            },
            Case {
                name: "clamped at the top",
                burst: &[-3, -3, -3],
                start: 4,
                want_rows_above: 0,
            },
        ];

        for case in cases {
            let mut app = app_with_service_state(ServiceState::Ready);
            // Stand in for what a frame would have measured.
            app.log_total_rows = 1_000;
            app.log_pane_height = 40;
            app.log_rows_above = case.start;

            for delta in case.burst {
                scroll_log(&mut app, *delta);
            }

            assert_eq!(
                app.log_rows_above, case.want_rows_above,
                "{}: offset after the burst",
                case.name
            );
        }
    }

    #[test]
    fn overlay_enter_stops_failed_service_rows() {
        struct Case {
            name: &'static str,
            state: ServiceState,
        }

        let cases = vec![
            Case {
                name: "failed",
                state: ServiceState::Failed,
            },
            Case {
                name: "dependency failed",
                state: ServiceState::DependencyFailed,
            },
        ];

        for case in cases {
            let app = app_with_service_state(case.state);
            let Some(command) = overlay_toggle_command(&app) else {
                panic!("{}: expected command", case.name);
            };
            assert_eq!(
                command.action,
                ControlAction::Stop,
                "{}: expected stop",
                case.name
            );
            assert_eq!(command.name, "api", "{}: wrong service", case.name);
        }
    }

    #[test]
    fn dependency_failure_events_refresh_tui_detail() {
        struct Case {
            name: &'static str,
            state: ServiceState,
            dependencies: Vec<String>,
            want: Vec<String>,
        }
        let cases = vec![
            Case {
                name: "initial root cause",
                state: ServiceState::DependencyFailed,
                dependencies: vec!["db".to_string()],
                want: vec!["db".to_string()],
            },
            Case {
                name: "changed root cause without state change",
                state: ServiceState::DependencyFailed,
                dependencies: vec!["cache".to_string()],
                want: vec!["cache".to_string()],
            },
            Case {
                name: "recovery clears detail",
                state: ServiceState::Pending,
                dependencies: Vec::new(),
                want: Vec::new(),
            },
        ];

        let mut app = app_with_service_state(ServiceState::Pending);
        for case in cases {
            apply_runner_event(
                RunnerEvent::ServiceStateChanged {
                    name: "api".to_string(),
                    state: case.state,
                    pid: None,
                    failed_dependencies: case.dependencies,
                },
                &mut app,
            );
            let item = app
                .service_items()
                .into_iter()
                .find(|item| item.name() == "api")
                .unwrap();
            assert_eq!(item.failed_dependencies, case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn overlay_uppercase_r_hard_restarts_highlighted_service() {
        let app = app_with_service_state(ServiceState::Ready);
        let Some(command) = highlighted_service_hard_restart_command(&app) else {
            panic!("expected hard restart command");
        };
        assert_eq!(command.action, ControlAction::HardRestart);
        assert_eq!(command.name, "api");
    }
}
