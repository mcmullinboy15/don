//! TUI application state — the single source of truth for what to render.
//!
//! Derived from runner events (status counts, service/task state for the
//! status tables) and from user input (view mode, filter, tables). Kept
//! deliberately small so rendering is a pure function of this struct plus
//! the terminal size.
//!
//! The main TUI loop is the only mutator — there's no shared `Arc<Mutex<_>>`.

use std::collections::{HashMap, HashSet};

use super::failure_summary::{self, FailureSummaryItem};
use super::filter::FilterState;
use super::form::FormState;
use super::status_table::{StatusTableState, retain_fuzzy_matches};
use crate::client::{ServiceState, TaskState};
use crate::config::Task;
use crate::output::{FormattedLogLine, LIFECYCLE_EVENT_NAME};
use crate::task_state::TaskRunInfo;

const LOG_POPUP_MAX_LINES: usize = 500;
const LOG_POPUP_DEFAULT_VISIBLE_LINES: usize = 30;

/// Top-level view mode. Determines how keys are interpreted and how the
/// inline viewport is laid out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ViewMode {
    /// Log flow + status bar. Keys trigger mode changes or scrollback actions.
    #[default]
    Normal,
    /// Log-filter modal. Navigation edits the pending selection; `/` enters
    /// query input, Enter commits, Esc cancels.
    Filter,
    /// Full-screen tasks table. Arrow keys move a highlight; Enter runs the
    /// selected task or opens its param form.
    Tasks,
    /// Full-screen services table (alternate screen). Arrow keys move a
    /// highlight; Enter toggles start/stop on the selected service, `r`
    /// restarts it, `R` hard-restarts it, `Esc` dismisses.
    Services,
    /// Full-screen summary of root failures and dependency-blocked items.
    Failures,
    /// Param-entry form for a task. Opened from the task table when the user
    /// selects a task with declared `params`. Collects values and, on
    /// submit, dispatches `RunnerCommand::RunTask { name, params, reply }`.
    Form,
}

/// A row in the services status table.
/// Exposed so the render path and the key handler agree on which row is
/// highlighted.
#[derive(Debug, Clone)]
pub(crate) struct OverlayItem {
    pub(crate) name: String,
    pub(crate) state: ServiceState,
    pub(crate) pid: Option<i32>,
    pub(crate) failed_dependencies: Vec<String>,
}

/// A row in the tasks status table.
#[derive(Debug, Clone)]
pub(crate) struct TaskStatusItem {
    pub(crate) name: String,
    pub(crate) state: TaskState,
    pub(crate) failed_dependencies: Vec<String>,
    pub(crate) last_run: Option<TaskRunInfo>,
    pub(crate) has_params: bool,
}

/// In-table popup showing recent logs for the highlighted service/task.
#[derive(Debug, Clone)]
pub(crate) struct LogPopup {
    pub(crate) name: String,
    pub(crate) lines: Vec<Vec<u8>>,
    pub(crate) scroll: usize,
    pub(crate) follow_tail: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpdateBadge {
    pub(crate) current_version: String,
    pub(crate) latest_version: String,
}

impl TaskStatusItem {
    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    pub(crate) fn runnable(&self) -> bool {
        !matches!(self.state, TaskState::Running | TaskState::Building)
    }

    fn sort_bucket(&self) -> u8 {
        match self.state {
            TaskState::Failed => 0,
            TaskState::DependencyFailed => 1,
            TaskState::PendingRun => 2,
            TaskState::Running | TaskState::Building => 3,
            TaskState::Pending => 4,
            TaskState::Completed => 5,
            TaskState::Skipped => 6,
        }
    }
}

impl OverlayItem {
    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Sort bucket: actionable rows first, settled rows last. Putting
    /// `DependencyFailed` below `Failed` keeps the actual culprit at the top
    /// so the user sees the thing they need to look at, not the stranded
    /// dependents.
    fn sort_bucket(&self) -> u8 {
        match self.state {
            ServiceState::Failed | ServiceState::Unhealthy => 0,
            ServiceState::DependencyFailed => 1,
            ServiceState::Pending | ServiceState::Building | ServiceState::Starting => 2,
            ServiceState::Running => 3,
            ServiceState::Ready => 4,
            ServiceState::Stopping => 5,
            ServiceState::Stopped => 6,
            ServiceState::Lazy => 7,
        }
    }
}

/// The half of a log pane's state that is swapped out when the other pane is
/// brought to the front.
///
/// The TUI shows one log pane at a time: don's record of the processes, or
/// don's record of itself. Each keeps its own scroll position and its own row
/// index, so switching between them does not move either one or throw away work
/// — the index is the expensive thing, and rebuilding it on every toggle is
/// what made pressing `v` flash.
#[derive(Debug, Default)]
pub(crate) struct StashedView {
    pub(crate) index: super::view_index::ViewIndex,
    pub(crate) scroll: super::logs::Scroll,
    pub(crate) rows_above: usize,
    pub(crate) total_rows: usize,
    pub(crate) blank_after: HashMap<crate::output::LogId, u16>,
}

/// The run of visible rows that belong to the same message as row `index`.
///
/// A message that wrapped is one thing to the reader, so both triple-click and
/// shift-hover work on the run, not the row. One definition, because the two
/// growing their own copies of "which rows are one message" is how a hover
/// would highlight a different extent than a click selects.
///
/// Only what is on screen: a message running off an edge is taken as far as it
/// is visible. Returns `None` when `index` is outside the row list.
pub(crate) fn message_run(ids: &[crate::output::LogId], index: usize) -> Option<(usize, usize)> {
    let id = *ids.get(index)?;
    let first = ids[..index]
        .iter()
        .rposition(|other| *other != id)
        .map_or(0, |before| before + 1);
    let last = ids[index..]
        .iter()
        .position(|other| *other != id)
        .map_or(ids.len() - 1, |after| index + after - 1);
    Some((first, last))
}

/// A scroll the reader asked for, in units that do not need geometry to
/// express: rows, pages, and the two ends.
///
/// Accumulates between frames — a wheel spin is many events per frame — and is
/// resolved once, against the geometry that actually exists when the pane is
/// drawn. Nothing here is clamped: clamping needs to know how much content
/// there is, which is exactly the knowledge this defers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PendingScroll {
    pub(crate) rows: isize,
    pub(crate) pages: isize,
    /// Jump to the oldest line held.
    pub(crate) to_top: bool,
    /// Stop following and hold the current position, without moving it.
    pub(crate) pin: bool,
}

impl PendingScroll {
    pub(crate) fn is_empty(self) -> bool {
        self == Self::default()
    }
}

/// Aggregate counts derived from service/task state, displayed on the bar.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StatusCounts {
    pub(crate) services_total: usize,
    pub(crate) services_ready: usize,
    pub(crate) services_failed: usize,
    /// Services running but with a failing health-check monitor.
    pub(crate) services_unhealthy: usize,
    /// Services actively transitioning (Pending, Starting, Running-not-ready,
    /// Stopping). Used to light up the spinner — `Ready`/`Stopped`/`Failed`/
    /// `Lazy` don't count as "doing work".
    pub(crate) services_active: usize,
    pub(crate) tasks_running: usize,
    pub(crate) tasks_pending_run: usize,
    pub(crate) tasks_failed: usize,
}

impl StatusCounts {
    /// Derive counts from the current service/task state maps.
    ///
    /// Services in [`ServiceState::Lazy`] are excluded from `services_total`:
    /// they haven't been started (and may never be, if no connection arrives),
    /// so counting them makes `N/M services ready` look permanently behind.
    /// Once a lazy service is triggered it leaves the `Lazy` state and
    /// rejoins the count.
    pub(crate) fn from_state(
        services: &HashMap<String, ServiceState>,
        tasks: &HashMap<String, TaskState>,
    ) -> Self {
        let mut counts = Self::default();
        for state in services.values() {
            if matches!(state, ServiceState::Lazy) {
                continue;
            }
            counts.services_total += 1;
            match state {
                ServiceState::Ready => counts.services_ready += 1,
                // DependencyFailed rolls into failed — from the user's
                // perspective it's still "not running because something broke".
                ServiceState::Failed | ServiceState::DependencyFailed => {
                    counts.services_failed += 1
                }
                ServiceState::Unhealthy => counts.services_unhealthy += 1,
                ServiceState::Pending
                | ServiceState::Building
                | ServiceState::Starting
                | ServiceState::Running
                | ServiceState::Stopping => counts.services_active += 1,
                ServiceState::Stopped | ServiceState::Lazy => {}
            }
        }
        for state in tasks.values() {
            match state {
                TaskState::PendingRun => counts.tasks_pending_run += 1,
                TaskState::Running => counts.tasks_running += 1,
                TaskState::Failed | TaskState::DependencyFailed => counts.tasks_failed += 1,
                _ => {}
            }
        }
        counts
    }

    /// True when the runner is actively working on something — drives the
    /// spinner on the status bar.
    pub(crate) fn is_working(&self) -> bool {
        self.services_active > 0 || self.tasks_running > 0
    }
}

/// Top-level TUI app state. Owns everything the renderer reads from.
#[derive(Debug)]
pub(crate) struct App {
    /// Set by key handling (remote-mode Ctrl+D) to ask the main loop to
    /// exit cleanly, restoring the terminal, without a shutdown.
    pub(crate) exit_requested: bool,
    /// Set by the overlay 'a' key: bridge the terminal into this item's
    /// PTY. Consumed by the main loop, which tears the TUI down, runs the
    /// bridge, and rebuilds.
    pub(crate) bridge_request: Option<String>,
    pub(crate) counts: StatusCounts,
    pub(crate) view_mode: ViewMode,
    /// Graceful shutdown is in progress: the inline bar becomes
    /// non-interactive and `[don]`-prefixed lifecycle events bypass the
    /// committed filter (raw service stdout still respects it).
    pub(crate) shutdown_started: bool,
    pub(crate) filter: FilterState,
    /// Monotonically incrementing frame counter, driven by the TUI's timer
    /// tick. The renderer mods into the spinner frame table. Wraps freely.
    pub(crate) spinner_frame: usize,
    /// Current service state — tracked here (not in a side task) because the
    /// status tables read it at render time. Seeded with every service name
    /// in `ServiceState::Pending` so the bar shows `0/N ready` from frame 1.
    pub(crate) services_state: HashMap<String, ServiceState>,
    pub(crate) service_pids: HashMap<String, Option<i32>>,
    pub(crate) tasks_state: HashMap<String, TaskState>,
    failed_dependencies: HashMap<String, Vec<String>>,
    pub(crate) tasks_last_run: HashMap<String, TaskRunInfo>,
    pub(crate) update_badge: Option<UpdateBadge>,
    pub(crate) services_table: StatusTableState,
    pub(crate) tasks_table: StatusTableState,
    /// Vertical scroll offset for the wrapped failure-summary view.
    pub(crate) failure_summary_scroll: usize,
    /// Static task-config snapshot — populated at TUI startup so the
    /// table/form can inspect declared params without reaching back into
    /// the runner. Immutable for the session; the runner re-validates on
    /// submit anyway.
    pub(crate) task_configs: HashMap<String, Task>,
    /// Names that should be inserted into the committed log filter when they
    /// fail. Derived from top-level/service/task config at TUI startup.
    auto_filter_on_failure_names: HashSet<String>,
    /// Active form modal, or `None` when not in [`ViewMode::Form`].
    pub(crate) form: Option<FormState>,
    /// Active service/task log popup shown over the services/tasks table.
    pub(crate) log_popup: Option<LogPopup>,
    /// Where the panes ended up in the last frame. Written by the renderer and
    /// read by mouse handling, so a click resolves against the rectangles that
    /// were actually drawn rather than a second computation of them.
    pub(crate) panes: super::panes::Panes,
    /// The optional status pane beside the log: open, docked, sized.
    pub(crate) status_pane: super::panes::StatusPane,
    /// The layout the screen was last painted with, and whether a full repaint
    /// has been asked for. A pane opening, moving or resizing changes which
    /// cells mean what, and a diffing renderer only rewrites cells it believes
    /// changed — so anything the old layout drew that the new one does not
    /// reach stays on screen. The divider is the visible case: a dashed rule
    /// left behind in the middle of the log.
    pub(crate) painted_layout: Option<super::panes::StatusPane>,
    /// Set by the redraw key, cleared by the next paint.
    pub(crate) repaint_requested: bool,
    /// Which pane takes keys both could claim.
    pub(crate) focus: super::panes::Focus,
    /// Set while the divider is being dragged, so motion resizes instead of
    /// selecting text.
    pub(crate) dragging_divider: bool,
    /// Where the pointer is while shift is held, in screen coordinates.
    ///
    /// The renderer maps it to a message each frame and gives that message a
    /// faint background, so a long wrapped line can be read without losing
    /// one's place. Position rather than a resolved message id: the view moves
    /// under a still pointer, and re-resolving per frame makes the highlight
    /// track what is actually under the cursor instead of chasing a line that
    /// scrolled away.
    pub(crate) hover: Option<(u16, u16)>,
    /// A `g` was pressed and the next key decides: another `g` jumps to the
    /// top, anything else is just itself. Vim's chord, minus the timeout —
    /// a stale half-chord is cleared by whatever key comes next.
    pub(crate) pending_g: bool,
    /// The row order each status table was opened with.
    ///
    /// The tables sort by state — failures first — which is what you want when
    /// the view opens and exactly what you do not want afterwards: starting or
    /// stopping something changes its bucket, so the row moves out from under
    /// the cursor that acted on it. Capturing the order at open keeps the
    /// useful sort and makes the list hold still. Empty means "sort by state",
    /// which is how the first render populates it.
    pub(crate) services_order: Vec<String>,
    pub(crate) tasks_order: Vec<String>,
    /// The admitted-lines index the pane positions itself with. Mended once a
    /// frame; see [`super::view_index`] for why it is not recomputed.
    pub(crate) view_index: super::view_index::ViewIndex,
    /// Where the log pane is looking. `Follow` until the user scrolls away.
    pub(crate) log_scroll: super::logs::Scroll,
    /// The drag in progress, or the last one that settled. Screen coordinates,
    /// so it is discarded whenever the view moves under it.
    pub(crate) log_selection: super::selection::Selection,
    /// What the last copy did, and when it was said. OSC 52 gets no reply, so
    /// this is the only feedback there can be — and it is transient, because a
    /// badge that never leaves stops reading as an answer to what you just did
    /// and starts reading as part of the furniture.
    pub(crate) copy_notice: Option<(String, std::time::Instant)>,
    /// A click's position, time and how many clicks have landed there in a
    /// row. Double- and triple-click are the same button event as a single
    /// one; only the gap between them tells them apart.
    pub(crate) last_click: Option<(u16, u16, std::time::Instant, u8)>,
    /// Set when a selection paused following, so clearing it can resume.
    /// Without this, `esc` after selecting would strand a reader who had
    /// deliberately scrolled up before selecting.
    pub(crate) follow_paused_for_selection: bool,
    /// The plain text of the rows the last frame drew, and where the pane
    /// started. Written by the renderer so a copy resolves against exactly what
    /// was on screen rather than re-deriving wrapping, filtering and scroll.
    pub(crate) log_visible_rows: Vec<String>,
    /// The log line each visible row belongs to, parallel to
    /// `log_visible_rows`. Lets a triple-click take the whole message when it
    /// wrapped across several rows.
    pub(crate) log_visible_ids: Vec<crate::output::LogId>,
    pub(crate) log_pane_origin: (u16, u16),
    /// Geometry the last frame produced, so the input layer can move the
    /// scroll anchor without re-deriving what only the renderer knows: how
    /// tall the pane came out and how much admitted content there is at this
    /// width. Written by the renderer, read by key and mouse handling.
    /// What the reader has asked the view to do, not yet resolved.
    ///
    /// Input records intent; the renderer resolves it. Scroll arithmetic needs
    /// three things — how much admitted content there is, where the view
    /// currently sits in it, and how tall the pane is — and all three are known
    /// only while rendering. Resolving at input time meant using the numbers
    /// the *previous* frame measured, so anything that changed the view between
    /// frames (a verbose toggle rebuilding the index, lines arriving, lines
    /// evicting, a resize) made one keypress land somewhere unrelated.
    pub(crate) pending_scroll: PendingScroll,
    /// Lines the reader asked for a blank row after, by pressing Enter at the
    /// tail — the terminal gesture for "start a fresh patch of screen".
    ///
    /// A mark, not a stored line. A blank pushed into the store had to be given
    /// an id, and the only one available was the id the *next* real line would
    /// arrive with, so the two collided: either the real line replaced the
    /// blank, or both sat under one id and the store's binary searches started
    /// answering with whichever came first.
    ///
    /// Counted, not a set: pressing Enter twice on a quiet stack means two
    /// blank rows, the same as it would in a shell.
    pub(crate) blank_after: HashMap<crate::output::LogId, u16>,
    /// Whether the pane is showing don's diagnostics rather than the processes'
    /// output. Two separate records with separate stores; this says which one
    /// is on screen.
    pub(crate) debug_view: bool,
    /// The other pane's state, waiting its turn. See [`StashedView`].
    pub(crate) stashed_view: StashedView,
    /// Rows above the top edge, as last drawn. For the scrollbar only —    /// Rows above the top edge, as last drawn. For the scrollbar only —
    /// scrolling must not read it, or it is back to deciding from stale
    /// geometry.
    pub(crate) log_rows_above: usize,
    pub(crate) log_total_rows: usize,
    pub(crate) log_pane_height: u16,
}

pub(crate) struct AppInit {
    pub(crate) service_names: Vec<String>,
    pub(crate) task_names: Vec<String>,
    pub(crate) build_tool_names: Vec<String>,
    pub(crate) task_configs: HashMap<String, Task>,
    pub(crate) task_last_runs: HashMap<String, TaskRunInfo>,
    pub(crate) hidden_names: HashSet<String>,
    pub(crate) auto_filter_on_failure_names: HashSet<String>,
    pub(crate) cli_log_filter: Option<HashSet<String>>,
}

impl App {
    pub(crate) fn new(init: AppInit) -> Self {
        let AppInit {
            service_names,
            task_names,
            build_tool_names,
            task_configs,
            task_last_runs,
            hidden_names,
            auto_filter_on_failure_names,
            cli_log_filter,
        } = init;
        let services_state: HashMap<String, ServiceState> = service_names
            .iter()
            .map(|n| (n.clone(), ServiceState::Pending))
            .collect();
        let service_pids: HashMap<String, Option<i32>> =
            service_names.iter().map(|n| (n.clone(), None)).collect();
        let tasks_state: HashMap<String, TaskState> = task_names
            .iter()
            .map(|n| (n.clone(), TaskState::Pending))
            .collect();

        let mut all_filter_names = service_names;
        all_filter_names.extend(task_names);
        // The synthetic build-tool stream ("bazel") emits under its
        // own prefix, not under a service/task name. Without a filter entry
        // they're silently gated out — the user sees nothing while bazel
        // crunches. Add them only when the config actually uses them, so
        // they don't show up as empty rows in unrelated projects.
        all_filter_names.extend(build_tool_names);
        // Expose `[don]` lifecycle events as their own filter entry so the
        // user can opt in/out explicitly, rather than having them always
        // bleed through an active filter.
        all_filter_names.push(LIFECYCLE_EVENT_NAME.to_string());

        let counts = StatusCounts::from_state(&services_state, &tasks_state);

        Self {
            exit_requested: false,
            bridge_request: None,
            counts,
            view_mode: ViewMode::Normal,
            shutdown_started: false,
            filter: FilterState::new(all_filter_names, &hidden_names, cli_log_filter.as_ref()),
            spinner_frame: 0,
            services_state,
            service_pids,
            tasks_state,
            failed_dependencies: HashMap::new(),
            tasks_last_run: task_last_runs,
            update_badge: None,
            services_table: StatusTableState::default(),
            tasks_table: StatusTableState::default(),
            failure_summary_scroll: 0,
            task_configs,
            auto_filter_on_failure_names,
            form: None,
            log_popup: None,
            panes: super::panes::Panes::empty(),
            status_pane: super::panes::StatusPane::default(),
            painted_layout: None,
            repaint_requested: false,
            focus: super::panes::Focus::Logs,
            dragging_divider: false,
            hover: None,
            pending_g: false,
            services_order: Vec::new(),
            tasks_order: Vec::new(),
            view_index: super::view_index::ViewIndex::default(),
            log_scroll: super::logs::Scroll::Follow,
            log_selection: super::selection::Selection::default(),
            copy_notice: None,
            last_click: None,
            follow_paused_for_selection: false,
            log_visible_rows: Vec::new(),
            log_visible_ids: Vec::new(),
            log_pane_origin: (0, 0),
            pending_scroll: PendingScroll::default(),
            blank_after: HashMap::new(),
            debug_view: false,
            stashed_view: StashedView::default(),
            log_rows_above: 0,
            log_total_rows: 0,
            log_pane_height: 0,
        }
    }

    pub(crate) fn begin_shutdown(&mut self) {
        self.shutdown_started = true;
        self.view_mode = ViewMode::Normal;
        self.services_table.reset();
        self.tasks_table.reset();
        self.failure_summary_scroll = 0;
        self.form = None;
        self.log_popup = None;
    }

    pub(crate) fn set_update_check(
        &mut self,
        current_version: String,
        latest_version: Option<String>,
    ) {
        self.update_badge = latest_version.map(|latest_version| UpdateBadge {
            current_version,
            latest_version,
        });
    }

    /// A fingerprint of everything [`Self::should_render_log`] consults.
    ///
    /// Hashed rather than hand-incremented on each mutation: the filter has
    /// more mutators than anyone will remember to keep in step, and a missed
    /// bump would leave the pane indexing against a filter the user has
    /// already changed. Cost is the number of *process names*, not lines.
    /// Bring the other log pane to the front, putting this one away as it is.
    ///
    /// A swap, not a rebuild. Each pane keeps its own index and its own scroll
    /// position, so coming back to one lands where it was left and costs
    /// nothing — the index is the expensive thing, and throwing it away on
    /// every toggle is what made this flash.
    pub(crate) fn swap_log_view(&mut self) {
        std::mem::swap(&mut self.view_index, &mut self.stashed_view.index);
        std::mem::swap(&mut self.log_scroll, &mut self.stashed_view.scroll);
        std::mem::swap(&mut self.log_rows_above, &mut self.stashed_view.rows_above);
        std::mem::swap(&mut self.log_total_rows, &mut self.stashed_view.total_rows);
        std::mem::swap(&mut self.blank_after, &mut self.stashed_view.blank_after);
        // A selection is screen coordinates over content that is about to be
        // entirely different text.
        self.log_selection.clear();
        self.follow_paused_for_selection = false;
        self.pending_scroll = PendingScroll::default();
        self.debug_view = !self.debug_view;
    }

    pub(crate) fn log_filter_fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.shutdown_started.hash(&mut hasher);
        self.filter.fingerprint(&mut hasher);
        // Blank marks change how tall a line is, so they belong to the same key
        // the row index is built against. Order-independent: a set has none.
        let mut marks: u64 = 0;
        for (id, count) in &self.blank_after {
            let mut one = std::collections::hash_map::DefaultHasher::new();
            (id, count).hash(&mut one);
            marks = marks.wrapping_add(Hasher::finish(&one));
        }
        marks.hash(&mut hasher);
        self.blank_after.len().hash(&mut hasher);
        hasher.finish()
    }

    /// Whether the pane shows this line.
    ///
    /// Verbose is not an admission question: it decided which *store* the line
    /// went into, and a store holds only its own kind. What is left is the
    /// name filter, and the shutdown override.
    pub(crate) fn should_render_log(&self, name: &str, _is_lifecycle: bool) -> bool {
        // During shutdown, every line bypasses the filter — the user wants
        // to see what's happening as each service tears down, including
        // service stdout from previously-hidden services (kafka, mongo, …).
        // The TUI render loop batches inserts and amortizes the bar redraw,
        // so a noisy service can't grind shutdown to a halt the way it
        // could before batching landed.
        if self.shutdown_started {
            return true;
        }
        self.filter.passes(name)
    }

    /// Sorted rows for the services table: errors → running → exited →
    /// lazy, alphabetical within a bucket. When its query is non-empty,
    /// rows are narrowed by fuzzy name-match before sorting.
    pub(crate) fn service_items(&self) -> Vec<OverlayItem> {
        let mut items: Vec<OverlayItem> = self
            .services_state
            .iter()
            .map(|(name, state)| OverlayItem {
                name: name.clone(),
                state: *state,
                pid: self.service_pids.get(name).copied().flatten(),
                failed_dependencies: self
                    .failed_dependencies
                    .get(name)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect();
        retain_fuzzy_matches(&self.services_table.query, &mut items, OverlayItem::name);
        let order = &self.services_order;
        items.sort_by(|a, b| {
            match (
                Self::ordered_position(order, a.name()),
                Self::ordered_position(order, b.name()),
            ) {
                (Some(x), Some(y)) => x.cmp(&y),
                // Anything the captured order does not know about goes after
                // everything it does, so a new arrival cannot displace a row.
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a
                    .sort_bucket()
                    .cmp(&b.sort_bucket())
                    .then_with(|| a.name().cmp(b.name())),
            }
        });
        items
    }

    pub(crate) fn task_items(&self) -> Vec<TaskStatusItem> {
        let mut items: Vec<TaskStatusItem> = self
            .tasks_state
            .iter()
            .map(|(name, state)| TaskStatusItem {
                name: name.clone(),
                state: *state,
                failed_dependencies: self
                    .failed_dependencies
                    .get(name)
                    .cloned()
                    .unwrap_or_default(),
                last_run: self.tasks_last_run.get(name).cloned(),
                has_params: self
                    .task_configs
                    .get(name)
                    .is_some_and(|task| !task.params.is_empty()),
            })
            .collect();
        retain_fuzzy_matches(&self.tasks_table.query, &mut items, TaskStatusItem::name);
        let order = &self.tasks_order;
        items.sort_by(|a, b| {
            match (
                Self::ordered_position(order, a.name()),
                Self::ordered_position(order, b.name()),
            ) {
                (Some(x), Some(y)) => x.cmp(&y),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a
                    .sort_bucket()
                    .cmp(&b.sort_bucket())
                    .then_with(|| a.name().cmp(b.name())),
            }
        });
        items
    }

    /// Where `name` sits in a captured order, or `None` if it arrived after
    /// the view opened. Newcomers sort after everything remembered, so a
    /// service that appears mid-session lands at the end instead of shuffling
    /// the rows above it.
    fn ordered_position(order: &[String], name: &str) -> Option<usize> {
        order.iter().position(|held| held == name)
    }

    /// Capture the order the tables should hold, from the state sort. Called
    /// when a table is opened.
    pub(crate) fn freeze_services_order(&mut self) {
        self.services_order.clear();
        self.services_order = self
            .service_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
    }

    pub(crate) fn freeze_tasks_order(&mut self) {
        self.tasks_order.clear();
        self.tasks_order = self
            .task_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
    }

    pub(crate) fn has_failure_summary(&self) -> bool {
        failure_summary::has_failures(&self.services_state, &self.tasks_state)
    }

    pub(crate) fn failure_summary_items(&self) -> Vec<FailureSummaryItem> {
        failure_summary::collect(
            &self.services_state,
            &self.tasks_state,
            &self.failed_dependencies,
        )
    }

    pub(crate) fn open_failure_summary(&mut self) {
        self.failure_summary_scroll = 0;
        self.view_mode = ViewMode::Failures;
    }

    pub(crate) fn scroll_failure_summary_by(&mut self, delta: isize) {
        if delta < 0 {
            self.failure_summary_scroll = self
                .failure_summary_scroll
                .saturating_sub(delta.unsigned_abs());
        } else {
            self.failure_summary_scroll = self
                .failure_summary_scroll
                .saturating_add(delta.unsigned_abs());
        }
    }

    pub(crate) fn scroll_failure_summary_to_top(&mut self) {
        self.failure_summary_scroll = 0;
    }

    pub(crate) fn scroll_failure_summary_to_bottom(&mut self) {
        self.failure_summary_scroll = usize::MAX;
    }

    pub(crate) fn sync_failure_summary_scroll(&mut self, max_scroll: usize) {
        self.failure_summary_scroll = self.failure_summary_scroll.min(max_scroll);
    }

    /// Apply a runner-emitted state change. Returns `true` when counts
    /// changed (so the main loop can limit redraws to interesting events).
    pub(crate) fn apply_service_runtime(
        &mut self,
        name: String,
        state: ServiceState,
        pid: Option<i32>,
        failed_dependencies: Vec<String>,
    ) -> bool {
        let filter_changed = state == ServiceState::Failed
            && self.auto_filter_on_failure_names.contains(&name)
            && self.filter.select_name(&name);
        self.services_state.insert(name.clone(), state);
        self.service_pids.insert(name.clone(), pid);
        self.apply_failed_dependencies(name, failed_dependencies);
        self.counts = StatusCounts::from_state(&self.services_state, &self.tasks_state);
        filter_changed
    }

    pub(crate) fn apply_task_state(
        &mut self,
        name: String,
        state: TaskState,
        last_run: Option<TaskRunInfo>,
        failed_dependencies: Vec<String>,
    ) -> bool {
        let filter_changed = state == TaskState::Failed
            && self.auto_filter_on_failure_names.contains(&name)
            && self.filter.select_name(&name);
        self.tasks_state.insert(name.clone(), state);
        self.apply_failed_dependencies(name.clone(), failed_dependencies);
        if let Some(last_run) = last_run {
            self.tasks_last_run.insert(name, last_run);
        }
        self.counts = StatusCounts::from_state(&self.services_state, &self.tasks_state);
        filter_changed
    }

    /// Reload every item's state from the runner's projection.
    ///
    /// The TUI applies transitions incrementally from the event broadcast,
    /// which is correct right up until that broadcast *lags*. A dropped event
    /// leaves this view silently wrong about the item it described, and it
    /// stays wrong until that item happens to move again — a service can sit
    /// on the status bar as `starting` for the rest of the session. Reloading
    /// from the projection turns that unrecoverable drift into a missed frame.
    ///
    /// Deliberately not a replacement for event handling: the auto-filter on
    /// failure is edge-triggered, and re-firing it here for every service that
    /// is already failed would yank the user's filter out from under them.
    pub(crate) fn resync_from(&mut self, snapshot: &crate::client::StateSnapshot) {
        for status in &snapshot.processes {
            match status {
                crate::client::ProcessStatus::Service {
                    name,
                    state,
                    failed_dependencies,
                    runtime,
                    ..
                } => {
                    // The snapshot is the record for runtime detail, so take
                    // the pid from it rather than keeping whatever we last
                    // saw on an event. This is the *only* way a client that
                    // attached after startup learns a pid at all: state
                    // events fire on transitions, and by the time a `don
                    // start` TUI subscribes the spawns have already happened.
                    self.service_pids
                        .insert(name.clone(), runtime.as_ref().and_then(|rt| rt.pid));
                    self.services_state.insert(name.clone(), *state);
                    self.apply_failed_dependencies(name.clone(), failed_dependencies.clone());
                }
                crate::client::ProcessStatus::Task {
                    name,
                    state,
                    failed_dependencies,
                    last_run,
                    ..
                } => {
                    self.tasks_state.insert(name.clone(), *state);
                    self.apply_failed_dependencies(name.clone(), failed_dependencies.clone());
                    if let Some(last_run) = last_run {
                        self.tasks_last_run.insert(name.clone(), last_run.clone());
                    }
                }
            }
        }
        self.counts = StatusCounts::from_state(&self.services_state, &self.tasks_state);
    }

    fn apply_failed_dependencies(&mut self, name: String, dependencies: Vec<String>) {
        if dependencies.is_empty() {
            self.failed_dependencies.remove(&name);
        } else {
            self.failed_dependencies.insert(name, dependencies);
        }
    }

    pub(crate) fn open_log_popup(&mut self, name: String, mut lines: Vec<Vec<u8>>) {
        if lines.len() > LOG_POPUP_MAX_LINES {
            lines.drain(0..lines.len() - LOG_POPUP_MAX_LINES);
        }
        let scroll = lines.len().saturating_sub(LOG_POPUP_DEFAULT_VISIBLE_LINES);
        self.log_popup = Some(LogPopup {
            name,
            lines,
            scroll,
            follow_tail: true,
        });
    }

    pub(crate) fn close_log_popup(&mut self) {
        self.log_popup = None;
    }

    pub(crate) fn append_log_popup_line(&mut self, line: &FormattedLogLine) -> bool {
        let Some(popup) = self.log_popup.as_mut() else {
            return false;
        };
        if !line_matches_log_popup(&popup.name, line) {
            return false;
        }
        popup.lines.push(line.bytes.clone());
        if popup.lines.len() > LOG_POPUP_MAX_LINES {
            popup.lines.remove(0);
            if !popup.follow_tail {
                popup.scroll = popup.scroll.saturating_sub(1);
            }
        }
        if popup.follow_tail {
            popup.scroll = popup
                .lines
                .len()
                .saturating_sub(LOG_POPUP_DEFAULT_VISIBLE_LINES);
        }
        true
    }

    pub(crate) fn scroll_log_popup_by(&mut self, delta: isize) {
        let Some(popup) = self.log_popup.as_mut() else {
            return;
        };
        popup.follow_tail = false;
        if delta < 0 {
            popup.scroll = popup.scroll.saturating_sub(delta.unsigned_abs());
        } else {
            popup.scroll = popup
                .scroll
                .saturating_add(delta as usize)
                .min(popup.lines.len().saturating_sub(1));
        }
    }

    pub(crate) fn scroll_log_popup_to_top(&mut self) {
        if let Some(popup) = self.log_popup.as_mut() {
            popup.scroll = 0;
            popup.follow_tail = false;
        }
    }

    pub(crate) fn scroll_log_popup_to_bottom(&mut self) {
        if let Some(popup) = self.log_popup.as_mut() {
            popup.scroll = popup
                .lines
                .len()
                .saturating_sub(LOG_POPUP_DEFAULT_VISIBLE_LINES);
            popup.follow_tail = true;
        }
    }

    pub(crate) fn sync_log_popup_scroll(&mut self, visible_rows: usize) {
        let Some(popup) = self.log_popup.as_mut() else {
            return;
        };
        let max_scroll = log_popup_max_scroll(popup.lines.len(), visible_rows);
        if popup.follow_tail {
            popup.scroll = max_scroll;
        } else {
            popup.scroll = popup.scroll.min(max_scroll);
        }
    }
}

fn log_popup_max_scroll(line_count: usize, visible_rows: usize) -> usize {
    if visible_rows == 0 {
        0
    } else {
        line_count.saturating_sub(visible_rows)
    }
}

pub(crate) fn line_matches_log_popup(name: &str, line: &FormattedLogLine) -> bool {
    if line.name == name {
        return true;
    }
    if line.name != LIFECYCLE_EVENT_NAME {
        return false;
    }
    String::from_utf8_lossy(&line.bytes).contains(&format!("{name}:"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn services(entries: &[(&str, ServiceState)]) -> HashMap<String, ServiceState> {
        entries.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    fn tasks(entries: &[(&str, TaskState)]) -> HashMap<String, TaskState> {
        entries.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    fn app_with_names(service_names: Vec<String>, task_names: Vec<String>) -> App {
        app_with_names_and_auto_filter(service_names, task_names, HashSet::new())
    }

    fn app_with_names_and_auto_filter(
        service_names: Vec<String>,
        task_names: Vec<String>,
        auto_filter_on_failure_names: HashSet<String>,
    ) -> App {
        App::new(AppInit {
            service_names,
            task_names,
            build_tool_names: vec![],
            task_configs: HashMap::new(),
            task_last_runs: HashMap::new(),
            hidden_names: HashSet::new(),
            auto_filter_on_failure_names,
            cli_log_filter: None,
        })
    }

    fn apply_service(app: &mut App, name: &str, state: ServiceState, pid: Option<i32>) -> bool {
        app.apply_service_runtime(name.to_string(), state, pid, Vec::new())
    }

    fn apply_task(app: &mut App, name: &str, state: TaskState) -> bool {
        app.apply_task_state(name.to_string(), state, None, Vec::new())
    }

    #[test]
    fn resync_from_replaces_drifted_state() {
        use crate::client::{ProcessStatus, StateSnapshot};

        fn snapshot_service(name: &str, state: ServiceState, pid: Option<i32>) -> ProcessStatus {
            ProcessStatus::Service {
                runtime: pid.map(|pid| crate::client::ServiceRuntime {
                    pid: Some(pid),
                    ..Default::default()
                }),
                name: name.to_string(),
                state,
                failed_dependencies: Vec::new(),
                verbose: None,
            }
        }

        struct Case {
            name: &'static str,
            /// State the app believes, applied from events before the lag.
            before: Vec<(&'static str, ServiceState, Option<i32>)>,
            snapshot: Vec<ProcessStatus>,
            want_states: Vec<(&'static str, ServiceState)>,
            want_pids: Vec<(&'static str, Option<i32>)>,
            want_counts_ready: usize,
        }

        let cases = vec![
            Case {
                name: "a dropped transition is picked up",
                before: vec![("api", ServiceState::Starting, Some(42))],
                snapshot: vec![snapshot_service("api", ServiceState::Ready, Some(42))],
                want_states: vec![("api", ServiceState::Ready)],
                want_pids: vec![("api", Some(42))],
                want_counts_ready: 1,
            },
            Case {
                // The bug this path exists for: a client that subscribes
                // after the spawns have happened sees no state transitions,
                // so the snapshot is the only place a pid can come from.
                name: "a pid we never saw an event for arrives with the snapshot",
                before: vec![("api", ServiceState::Ready, None)],
                snapshot: vec![snapshot_service("api", ServiceState::Ready, Some(42))],
                want_states: vec![("api", ServiceState::Ready)],
                want_pids: vec![("api", Some(42))],
                want_counts_ready: 1,
            },
            Case {
                name: "the snapshot's pid replaces a stale one",
                before: vec![("api", ServiceState::Ready, Some(42))],
                snapshot: vec![snapshot_service("api", ServiceState::Ready, Some(99))],
                want_states: vec![("api", ServiceState::Ready)],
                want_pids: vec![("api", Some(99))],
                want_counts_ready: 1,
            },
            Case {
                // No runtime means no local process — a docker service, or one
                // that has stopped. Either way the pid we hold is a corpse.
                name: "no runtime in the snapshot clears the pid",
                before: vec![("api", ServiceState::Ready, Some(42))],
                snapshot: vec![snapshot_service("api", ServiceState::Ready, None)],
                want_states: vec![("api", ServiceState::Ready)],
                want_pids: vec![("api", None)],
                want_counts_ready: 1,
            },
            Case {
                name: "several items resync independently",
                before: vec![
                    ("api", ServiceState::Ready, Some(1)),
                    ("web", ServiceState::Starting, Some(2)),
                ],
                snapshot: vec![
                    snapshot_service("api", ServiceState::Ready, Some(1)),
                    snapshot_service("web", ServiceState::Failed, None),
                ],
                want_states: vec![("api", ServiceState::Ready), ("web", ServiceState::Failed)],
                want_pids: vec![("api", Some(1)), ("web", None)],
                want_counts_ready: 1,
            },
        ];

        for case in cases {
            let names: Vec<String> = case.before.iter().map(|(n, ..)| n.to_string()).collect();
            let mut app = app_with_names(names, vec![]);
            for (name, state, pid) in &case.before {
                apply_service(&mut app, name, *state, *pid);
            }

            app.resync_from(&StateSnapshot {
                processes: case.snapshot,
                startup_complete: true,
            });

            for (name, want) in case.want_states {
                assert_eq!(
                    app.services_state.get(name),
                    Some(&want),
                    "{}: state of {name}",
                    case.name
                );
            }
            for (name, want) in case.want_pids {
                assert_eq!(
                    app.service_pids.get(name).copied().flatten(),
                    want,
                    "{}: pid of {name}",
                    case.name
                );
            }
            assert_eq!(
                app.counts.services_ready, case.want_counts_ready,
                "{}: counts recomputed",
                case.name
            );
        }
    }

    #[test]
    fn resync_does_not_fire_the_auto_filter_on_already_failed_items() {
        use crate::client::{ProcessStatus, StateSnapshot};

        // Auto-filter-on-failure is edge-triggered. Resyncing is not an edge:
        // re-selecting every already-failed service would yank the user's
        // filter out from under them on every broadcast lag.
        let mut app = app_with_names_and_auto_filter(
            vec!["api".to_string()],
            vec![],
            HashSet::from(["api".to_string()]),
        );
        let before = app.filter.clone();

        app.resync_from(&StateSnapshot {
            processes: vec![ProcessStatus::Service {
                runtime: None,
                name: "api".to_string(),
                state: ServiceState::Failed,
                failed_dependencies: Vec::new(),
                verbose: None,
            }],
            startup_complete: true,
        });

        assert_eq!(app.services_state.get("api"), Some(&ServiceState::Failed));
        assert_eq!(
            format!("{:?}", app.filter),
            format!("{before:?}"),
            "resync must not touch the filter"
        );
    }

    #[test]
    fn from_state_counts_ready_failed_and_pending_run() {
        struct Case {
            name: &'static str,
            services: Vec<(&'static str, ServiceState)>,
            tasks: Vec<(&'static str, TaskState)>,
            want: StatusCounts,
        }

        let cases = vec![
            Case {
                name: "empty",
                services: vec![],
                tasks: vec![],
                want: StatusCounts::default(),
            },
            Case {
                name: "all services ready, no tasks",
                services: vec![
                    ("api", ServiceState::Ready),
                    ("worker", ServiceState::Ready),
                ],
                tasks: vec![],
                want: StatusCounts {
                    services_total: 2,
                    services_ready: 2,
                    services_failed: 0,
                    services_unhealthy: 0,
                    services_active: 0,
                    tasks_running: 0,
                    tasks_pending_run: 0,
                    tasks_failed: 0,
                },
            },
            Case {
                name: "mixed states — lazy excluded from total",
                services: vec![
                    ("api", ServiceState::Ready),
                    ("db", ServiceState::Failed),
                    ("queue", ServiceState::Starting),
                    ("cache", ServiceState::Lazy),
                ],
                tasks: vec![
                    ("migrate", TaskState::PendingRun),
                    ("seed", TaskState::Completed),
                    ("backup", TaskState::PendingRun),
                    ("build", TaskState::Running),
                    ("lint", TaskState::Failed),
                ],
                want: StatusCounts {
                    services_total: 3, // cache (Lazy) doesn't count
                    services_ready: 1,
                    services_failed: 1,
                    services_unhealthy: 0,
                    services_active: 1, // queue (Starting)
                    tasks_running: 1,   // build
                    tasks_pending_run: 2,
                    tasks_failed: 1,
                },
            },
            Case {
                name: "running service counts as active",
                services: vec![("svc", ServiceState::Running)],
                tasks: vec![],
                want: StatusCounts {
                    services_total: 1,
                    services_ready: 0,
                    services_failed: 0,
                    services_unhealthy: 0,
                    services_active: 1,
                    tasks_running: 0,
                    tasks_pending_run: 0,
                    tasks_failed: 0,
                },
            },
        ];

        for case in cases {
            let got = StatusCounts::from_state(&services(&case.services), &tasks(&case.tasks));
            assert_eq!(got, case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn is_working_table() {
        struct Case {
            name: &'static str,
            counts: StatusCounts,
            want: bool,
        }
        let cases = vec![
            Case {
                name: "all idle",
                counts: StatusCounts::default(),
                want: false,
            },
            Case {
                name: "service transitioning",
                counts: StatusCounts {
                    services_active: 1,
                    ..Default::default()
                },
                want: true,
            },
            Case {
                name: "task running",
                counts: StatusCounts {
                    tasks_running: 1,
                    ..Default::default()
                },
                want: true,
            },
            Case {
                name: "pending-run tasks don't count — waiting on user",
                counts: StatusCounts {
                    tasks_pending_run: 3,
                    ..Default::default()
                },
                want: false,
            },
            Case {
                name: "failed service doesn't count",
                counts: StatusCounts {
                    services_total: 1,
                    services_failed: 1,
                    ..Default::default()
                },
                want: false,
            },
        ];
        for case in cases {
            assert_eq!(case.counts.is_working(), case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn apply_state_refreshes_counts() {
        let mut app = app_with_names(vec!["api".into(), "db".into()], vec![]);
        assert_eq!(app.counts.services_ready, 0);
        apply_service(&mut app, "api", ServiceState::Ready, None);
        assert_eq!(app.counts.services_ready, 1);
        apply_service(&mut app, "db", ServiceState::Ready, None);
        assert_eq!(app.counts.services_ready, 2);
        apply_service(&mut app, "db", ServiceState::Stopping, None);
        assert_eq!(app.counts.services_ready, 1);
        assert_eq!(app.counts.services_active, 1);
        apply_service(&mut app, "api", ServiceState::Failed, None);
        assert_eq!(app.counts.services_ready, 0);
        assert_eq!(app.counts.services_failed, 1);
    }

    #[test]
    fn failed_service_is_added_to_log_filter_when_configured() {
        let mut app = app_with_names_and_auto_filter(
            vec!["api".into(), "db".into()],
            vec![],
            HashSet::from(["db".to_string()]),
        );
        app.filter.enter_edit();
        app.filter.select_only_highlighted(); // [all] row keeps everything selected.
        app.filter.toggle_highlighted(); // clear all
        app.filter.commit();

        assert!(!app.should_render_log("db", false));
        let changed = apply_service(&mut app, "db", ServiceState::Failed, None);

        assert!(changed);
        assert!(app.should_render_log("db", false));
        assert!(!app.should_render_log("api", false));
    }

    #[test]
    fn dependency_failed_service_does_not_auto_filter() {
        let mut app = app_with_names_and_auto_filter(
            vec!["api".into()],
            vec![],
            HashSet::from(["api".to_string()]),
        );
        app.filter.enter_edit();
        app.filter.toggle_highlighted(); // clear all
        app.filter.commit();

        let changed = apply_service(&mut app, "api", ServiceState::DependencyFailed, None);

        assert!(!changed);
        assert!(!app.should_render_log("api", false));
    }

    #[test]
    fn failed_task_is_added_to_log_filter_when_configured() {
        let mut app = app_with_names_and_auto_filter(
            vec![],
            vec!["build".into(), "lint".into()],
            HashSet::from(["lint".to_string()]),
        );
        app.filter.enter_edit();
        app.filter.toggle_highlighted(); // clear all
        app.filter.commit();

        let changed = apply_task(&mut app, "lint", TaskState::Failed);

        assert!(changed);
        assert!(app.should_render_log("lint", false));
        assert!(!app.should_render_log("build", false));
    }

    #[test]
    fn log_popup_matches_source_and_named_lifecycle_lines() {
        let direct = FormattedLogLine {
            name: "api".to_string(),
            is_lifecycle: false,
            is_verbose: false,
            prefix: Vec::new(),
            bytes: b"api output".to_vec(),
        };
        let lifecycle = FormattedLogLine {
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
            is_verbose: false,
            prefix: Vec::new(),
            bytes: b"[don] api: started".to_vec(),
        };
        let other = FormattedLogLine {
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
            is_verbose: false,
            prefix: Vec::new(),
            bytes: b"[don] worker: started".to_vec(),
        };

        assert!(line_matches_log_popup("api", &direct));
        assert!(line_matches_log_popup("api", &lifecycle));
        assert!(!line_matches_log_popup("api", &other));
    }

    #[test]
    fn log_popup_sync_clamps_to_actual_visible_rows() {
        struct Case {
            name: &'static str,
            line_count: usize,
            follow_tail: bool,
            initial_scroll: usize,
            visible_rows: usize,
            want_scroll: usize,
        }

        let cases = vec![
            Case {
                name: "tail uses real taller viewport",
                line_count: 100,
                follow_tail: true,
                initial_scroll: 70,
                visible_rows: 40,
                want_scroll: 60,
            },
            Case {
                name: "tail uses real shorter viewport",
                line_count: 100,
                follow_tail: true,
                initial_scroll: 70,
                visible_rows: 10,
                want_scroll: 90,
            },
            Case {
                name: "manual over-scroll clamps to last full page",
                line_count: 100,
                follow_tail: false,
                initial_scroll: 99,
                visible_rows: 40,
                want_scroll: 60,
            },
            Case {
                name: "hidden popup area cannot accumulate scroll debt",
                line_count: 100,
                follow_tail: false,
                initial_scroll: 99,
                visible_rows: 0,
                want_scroll: 0,
            },
            Case {
                name: "viewport larger than content",
                line_count: 5,
                follow_tail: false,
                initial_scroll: 4,
                visible_rows: 40,
                want_scroll: 0,
            },
        ];

        for case in cases {
            let mut app = app_with_names(vec!["api".to_string()], vec![]);
            let lines = (0..case.line_count)
                .map(|i| format!("line {i}").into_bytes())
                .collect();
            app.open_log_popup("api".to_string(), lines);
            let popup = app.log_popup.as_mut().unwrap();
            popup.follow_tail = case.follow_tail;
            popup.scroll = case.initial_scroll;

            app.sync_log_popup_scroll(case.visible_rows);

            assert_eq!(
                app.log_popup.as_ref().unwrap().scroll,
                case.want_scroll,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn service_items_prioritize_service_states_and_exclude_tasks() {
        struct Case {
            name: &'static str,
            services: Vec<(&'static str, ServiceState)>,
            tasks: Vec<(&'static str, TaskState)>,
            want: Vec<&'static str>,
        }

        let cases = vec![Case {
            name: "mixed services and tasks",
            services: vec![
                ("svc-ready", ServiceState::Ready),
                ("svc-building", ServiceState::Building),
                ("svc-running", ServiceState::Running),
                ("svc-stopped", ServiceState::Stopped),
                ("svc-lazy", ServiceState::Lazy),
                ("svc-failed", ServiceState::Failed),
                ("svc-dep", ServiceState::DependencyFailed),
                ("svc-stopping", ServiceState::Stopping),
            ],
            tasks: vec![
                ("task-skipped", TaskState::Skipped),
                ("task-completed", TaskState::Completed),
                ("task-building", TaskState::Building),
                ("task-pending-run", TaskState::PendingRun),
                ("task-failed", TaskState::Failed),
                ("task-dep", TaskState::DependencyFailed),
            ],
            want: vec![
                "svc-failed",
                "svc-dep",
                "svc-building",
                "svc-running",
                "svc-ready",
                "svc-stopping",
                "svc-stopped",
                "svc-lazy",
            ],
        }];

        for case in cases {
            let service_names = case
                .services
                .iter()
                .map(|(name, _)| (*name).to_string())
                .collect();
            let task_names = case
                .tasks
                .iter()
                .map(|(name, _)| (*name).to_string())
                .collect();
            let mut app = app_with_names(service_names, task_names);
            for (name, state) in case.services {
                apply_service(&mut app, name, state, None);
            }
            for (name, state) in case.tasks {
                apply_task(&mut app, name, state);
            }

            let items = app.service_items();
            let got: Vec<&str> = items.iter().map(OverlayItem::name).collect();
            assert_eq!(got, case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn service_items_include_service_pid() {
        let mut app = app_with_names(vec!["api".into()], vec![]);

        apply_service(&mut app, "api", ServiceState::Running, Some(12_345));

        let items = app.service_items();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].name(), "api");
        assert_eq!(items[0].pid, Some(12_345));
    }

    #[test]
    fn shutdown_mode_bypasses_filter_for_all_logs() {
        let mut app = app_with_names(vec!["api".into(), "worker".into()], vec![]);
        app.filter.enter_edit();
        app.filter.push_query_char('a');
        app.filter.select_only_highlighted();
        app.filter.commit();

        // Without shutdown: filter passes "api", rejects "worker" — for both
        // service stdout (is_lifecycle=false) and lifecycle events
        // (is_lifecycle=true).
        assert!(app.should_render_log("api", false));
        assert!(app.should_render_log("api", true));
        assert!(!app.should_render_log("worker", false));
        assert!(!app.should_render_log("worker", true));

        app.begin_shutdown();

        // After shutdown: every line passes regardless of filter — the user
        // wants visibility into everything happening as services tear down.
        assert!(app.should_render_log("api", false));
        assert!(app.should_render_log("api", true));
        assert!(app.should_render_log("worker", false));
        assert!(app.should_render_log("worker", true));
    }

    #[test]
    fn begin_shutdown_returns_to_normal_view() {
        let mut app = app_with_names(vec!["api".into()], vec![]);
        app.view_mode = ViewMode::Services;
        app.services_table.query = "api".into();
        app.services_table.filtering = true;

        app.begin_shutdown();

        assert!(app.shutdown_started);
        assert_eq!(app.view_mode, ViewMode::Normal);
        assert!(app.services_table.query.is_empty());
        assert!(!app.services_table.filtering);
    }

    #[test]
    fn task_items_prioritize_actionable_states_and_include_metadata() {
        let mut app = app_with_names(
            vec![],
            vec![
                "completed".into(),
                "failed".into(),
                "pending-run".into(),
                "running".into(),
            ],
        );
        apply_task(&mut app, "completed", TaskState::Completed);
        apply_task(&mut app, "failed", TaskState::Failed);
        apply_task(&mut app, "pending-run", TaskState::PendingRun);
        apply_task(&mut app, "running", TaskState::Running);
        app.tasks_last_run.insert(
            "completed".into(),
            TaskRunInfo {
                finished_at_unix_secs: 1,
                duration_ms: Some(42),
                success: true,
                exit_code: Some(0),
                message: None,
            },
        );

        let items = app.task_items();
        let got: Vec<&str> = items.iter().map(TaskStatusItem::name).collect();

        assert_eq!(got, vec!["failed", "pending-run", "running", "completed"]);
        assert_eq!(items[3].last_run.as_ref().unwrap().duration_ms, Some(42));
    }

    /// The tables sort by state so failures surface when the view opens — and
    /// then must hold still, because acting on a row changes its state and
    /// would otherwise move it out from under the cursor that acted.
    #[test]
    fn a_table_holds_the_order_it_opened_with() {
        let mut app = App::new(AppInit {
            service_names: vec!["alpha".into(), "beta".into(), "gamma".into()],
            task_names: Vec::new(),
            build_tool_names: Vec::new(),
            task_configs: HashMap::new(),
            task_last_runs: HashMap::new(),
            hidden_names: HashSet::new(),
            auto_filter_on_failure_names: HashSet::new(),
            cli_log_filter: None,
        });
        // beta is broken, so it opens at the top.
        apply_service(&mut app, "alpha", ServiceState::Ready, Some(1));
        apply_service(&mut app, "beta", ServiceState::Failed, None);
        apply_service(&mut app, "gamma", ServiceState::Ready, Some(3));

        let opened: Vec<String> = app
            .service_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
        assert_eq!(opened, vec!["beta", "alpha", "gamma"], "state sort on open");

        app.freeze_services_order();

        // Now act on things: beta recovers, alpha stops. Neither may move.
        apply_service(&mut app, "beta", ServiceState::Ready, Some(2));
        apply_service(&mut app, "alpha", ServiceState::Stopped, None);
        let after: Vec<String> = app
            .service_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
        assert_eq!(after, opened, "rows hold still once the view is open");

        // A service that appears later joins at the end rather than displacing
        // anything above it.
        apply_service(&mut app, "delta", ServiceState::Failed, None);
        let with_newcomer: Vec<String> = app
            .service_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
        assert_eq!(with_newcomer, vec!["beta", "alpha", "gamma", "delta"]);

        // Reopening re-sorts: alpha is stopped, delta failed.
        app.freeze_services_order();
        let reopened: Vec<String> = app
            .service_items()
            .into_iter()
            .map(|item| item.name)
            .collect();
        assert_eq!(reopened[0], "delta", "reopening surfaces the failure again");
    }
}
