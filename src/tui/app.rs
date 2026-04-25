//! TUI application state — the single source of truth for what to render.
//!
//! Derived from runner events (status counts, service/task state for the
//! action palette) and from user input (view mode, filter, palette). Kept
//! deliberately small so rendering is a pure function of this struct plus
//! the terminal size.
//!
//! The main TUI loop is the only mutator — there's no shared `Arc<Mutex<_>>`.

use std::collections::{HashMap, HashSet};

use super::filter::FilterState;
use super::form::FormState;
use super::palette::ActionPalette;
use crate::config::Task;
use crate::output::LIFECYCLE_EVENT_NAME;
use crate::runner::{ServiceState, TaskItemState};

/// Top-level view mode. Determines how keys are interpreted and how the
/// inline viewport is laid out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ViewMode {
    /// Log flow + status bar. Keys trigger mode changes or scrollback actions.
    #[default]
    Normal,
    /// Filter edit mode — typing builds a query, Space toggles selection,
    /// Enter commits.
    Filter,
    /// Tasks palette — typing filters tasks, Enter dispatches.
    Palette,
    /// Full-screen status overlay (alternate screen). Arrow keys move a
    /// highlight; Enter toggles start/stop on the selected service, `r`
    /// restarts it, `R` restarts all failed services, `Esc` dismisses.
    Overlay,
    /// Param-entry form for a task. Opened from the palette when the user
    /// selects a task with declared `params`. Collects values and, on
    /// submit, dispatches `RunnerCommand::RunTask { name, params, reply }`.
    Form,
}

/// A row in the status overlay — a service or task with its current state.
/// Exposed so the render path and the key handler agree on which row is
/// highlighted at `overlay_highlight`.
#[derive(Debug, Clone)]
pub(crate) enum OverlayItem {
    Service { name: String, state: ServiceState },
    Task { name: String, state: TaskItemState },
}

impl OverlayItem {
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Service { name, .. } | Self::Task { name, .. } => name.as_str(),
        }
    }

    /// Sort bucket: genuine failures → dependency-failed cascade → running
    /// → exited → lazy. Putting `DependencyFailed` below `Failed` keeps the
    /// actual culprit at the top so the user sees the thing they need to
    /// look at, not the stranded dependents.
    fn sort_bucket(&self) -> u8 {
        match self {
            Self::Service { state, .. } => match state {
                ServiceState::Failed | ServiceState::Unhealthy => 0,
                ServiceState::DependencyFailed => 1,
                ServiceState::Pending
                | ServiceState::Building
                | ServiceState::Starting
                | ServiceState::Running
                | ServiceState::Ready
                | ServiceState::Stopping => 2,
                ServiceState::Stopped => 3,
                ServiceState::Lazy => 4,
            },
            Self::Task { state, .. } => match state {
                TaskItemState::Failed => 0,
                TaskItemState::DependencyFailed => 1,
                TaskItemState::Pending
                | TaskItemState::Building
                | TaskItemState::Running
                | TaskItemState::Completed
                | TaskItemState::Skipped
                | TaskItemState::PendingRun => 2,
            },
        }
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
        tasks: &HashMap<String, TaskItemState>,
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
                TaskItemState::PendingRun => counts.tasks_pending_run += 1,
                TaskItemState::Running => counts.tasks_running += 1,
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
    pub(crate) counts: StatusCounts,
    pub(crate) view_mode: ViewMode,
    /// Graceful shutdown is in progress: the inline bar becomes
    /// non-interactive and new log lines from every service are shown.
    pub(crate) shutdown_started: bool,
    /// Last stored log id when shutdown started. Older lines still obey the
    /// committed filter; newer ones bypass it.
    pub(crate) shutdown_log_cutoff: Option<u64>,
    pub(crate) filter: FilterState,
    pub(crate) palette: ActionPalette,
    /// Monotonically incrementing frame counter, driven by the TUI's timer
    /// tick. The renderer mods into the spinner frame table. Wraps freely.
    pub(crate) spinner_frame: usize,
    /// Current service state — tracked here (not in a side task) because the
    /// action palette reads it at open time. Seeded with every service name
    /// in `ServiceState::Pending` so the bar shows `0/N ready` from frame 1.
    pub(crate) services_state: HashMap<String, ServiceState>,
    pub(crate) tasks_state: HashMap<String, TaskItemState>,
    /// Highlighted row index in the status overlay. Reset to 0 each time the
    /// overlay opens. The render path scrolls so this row stays visible.
    pub(crate) overlay_highlight: usize,
    /// Fuzzy-filter query applied to the status overlay rows. Empty = show
    /// everything. Activated via `/` inside the overlay; `overlay_filtering`
    /// tracks whether key input is currently editing the query.
    pub(crate) overlay_query: String,
    /// True when `/` has been pressed inside the overlay and subsequent
    /// keystrokes feed the query. Enter commits and exits the sub-mode
    /// (keeping the query active so r/R/Enter operate on the filtered list);
    /// Esc clears the query and exits.
    pub(crate) overlay_filtering: bool,
    /// Static task-config snapshot — populated at TUI startup so the
    /// palette/form can inspect declared params without reaching back into
    /// the runner. Immutable for the session; the runner re-validates on
    /// submit anyway.
    pub(crate) task_configs: HashMap<String, Task>,
    /// Active form modal, or `None` when not in [`ViewMode::Form`].
    pub(crate) form: Option<FormState>,
}

impl App {
    pub(crate) fn new(
        service_names: Vec<String>,
        task_names: Vec<String>,
        build_tool_names: Vec<String>,
        task_configs: HashMap<String, Task>,
        hidden_names: HashSet<String>,
    ) -> Self {
        let services_state: HashMap<String, ServiceState> = service_names
            .iter()
            .map(|n| (n.clone(), ServiceState::Pending))
            .collect();
        let tasks_state: HashMap<String, TaskItemState> = task_names
            .iter()
            .map(|n| (n.clone(), TaskItemState::Pending))
            .collect();

        let mut all_filter_names = service_names;
        all_filter_names.extend(task_names);
        // Synthetic build-tool streams ("bazel", "turbo") emit under their
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
            counts,
            view_mode: ViewMode::Normal,
            shutdown_started: false,
            shutdown_log_cutoff: None,
            filter: FilterState::new(all_filter_names, &hidden_names),
            palette: ActionPalette::default(),
            spinner_frame: 0,
            services_state,
            tasks_state,
            overlay_highlight: 0,
            overlay_query: String::new(),
            overlay_filtering: false,
            task_configs,
            form: None,
        }
    }

    pub(crate) fn begin_shutdown(&mut self, cutoff: Option<u64>) {
        self.shutdown_started = true;
        self.shutdown_log_cutoff = cutoff;
        self.view_mode = ViewMode::Normal;
        self.palette.close();
        self.overlay_query.clear();
        self.overlay_filtering = false;
        self.form = None;
    }

    pub(crate) fn should_render_log(&self, name: &str, id: u64) -> bool {
        if !self.shutdown_started {
            return self.filter.passes(name);
        }
        match self.shutdown_log_cutoff {
            Some(cutoff) => id > cutoff || self.filter.passes(name),
            None => true,
        }
    }

    /// Sorted rows for the status overlay: errors → running → exited → lazy,
    /// alphabetical within a bucket. Services and tasks are interleaved by
    /// the same sort; kind is preserved per row. When `overlay_query` is
    /// non-empty, rows are narrowed by fuzzy name-match before sorting.
    pub(crate) fn overlay_items(&self) -> Vec<OverlayItem> {
        let mut items: Vec<OverlayItem> = self
            .services_state
            .iter()
            .map(|(name, state)| OverlayItem::Service {
                name: name.clone(),
                state: *state,
            })
            .chain(
                self.tasks_state
                    .iter()
                    .map(|(name, state)| OverlayItem::Task {
                        name: name.clone(),
                        state: *state,
                    }),
            )
            .collect();
        if !self.overlay_query.is_empty() {
            let names: Vec<String> = items.iter().map(|i| i.name().to_string()).collect();
            let matched = super::fuzzy::fuzzy_match(&self.overlay_query, &names);
            let set: std::collections::HashSet<&str> = matched.iter().map(String::as_str).collect();
            items.retain(|i| set.contains(i.name()));
        }
        items.sort_by(|a, b| {
            a.sort_bucket()
                .cmp(&b.sort_bucket())
                .then_with(|| a.name().cmp(b.name()))
        });
        items
    }

    /// Apply a runner-emitted state change. Returns `true` when counts
    /// changed (so the main loop can limit redraws to interesting events).
    pub(crate) fn apply_service_state(&mut self, name: String, state: ServiceState) {
        self.services_state.insert(name, state);
        self.counts = StatusCounts::from_state(&self.services_state, &self.tasks_state);
    }

    pub(crate) fn apply_task_state(&mut self, name: String, state: TaskItemState) {
        self.tasks_state.insert(name, state);
        self.counts = StatusCounts::from_state(&self.services_state, &self.tasks_state);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn services(entries: &[(&str, ServiceState)]) -> HashMap<String, ServiceState> {
        entries.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    fn tasks(entries: &[(&str, TaskItemState)]) -> HashMap<String, TaskItemState> {
        entries.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    #[test]
    fn from_state_counts_ready_failed_and_pending_run() {
        struct Case {
            name: &'static str,
            services: Vec<(&'static str, ServiceState)>,
            tasks: Vec<(&'static str, TaskItemState)>,
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
                    ("migrate", TaskItemState::PendingRun),
                    ("seed", TaskItemState::Completed),
                    ("backup", TaskItemState::PendingRun),
                    ("build", TaskItemState::Running),
                ],
                want: StatusCounts {
                    services_total: 3, // cache (Lazy) doesn't count
                    services_ready: 1,
                    services_failed: 1,
                    services_unhealthy: 0,
                    services_active: 1, // queue (Starting)
                    tasks_running: 1,   // build
                    tasks_pending_run: 2,
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
        let mut app = App::new(
            vec!["api".into(), "db".into()],
            vec![],
            vec![],
            HashMap::new(),
            HashSet::new(),
        );
        assert_eq!(app.counts.services_ready, 0);
        app.apply_service_state("api".into(), ServiceState::Ready);
        assert_eq!(app.counts.services_ready, 1);
        app.apply_service_state("db".into(), ServiceState::Ready);
        assert_eq!(app.counts.services_ready, 2);
        app.apply_service_state("api".into(), ServiceState::Failed);
        assert_eq!(app.counts.services_ready, 1);
        assert_eq!(app.counts.services_failed, 1);
    }

    #[test]
    fn shutdown_mode_only_bypasses_filter_for_new_logs() {
        let mut app = App::new(
            vec!["api".into(), "worker".into()],
            vec![],
            vec![],
            HashMap::new(),
            HashSet::new(),
        );
        app.filter.enter_edit();
        app.filter.push_query_char('a');
        app.filter.commit();

        assert!(app.should_render_log("api", 4));
        assert!(!app.should_render_log("worker", 4));

        app.begin_shutdown(Some(4));

        assert!(app.should_render_log("api", 4));
        assert!(!app.should_render_log("worker", 4));
        assert!(app.should_render_log("worker", 5));
    }

    #[test]
    fn begin_shutdown_returns_to_normal_view() {
        let mut app = App::new(
            vec!["api".into()],
            vec![],
            vec![],
            HashMap::new(),
            HashSet::new(),
        );
        app.view_mode = ViewMode::Overlay;
        app.overlay_query = "api".into();
        app.overlay_filtering = true;

        app.begin_shutdown(None);

        assert!(app.shutdown_started);
        assert_eq!(app.view_mode, ViewMode::Normal);
        assert!(app.overlay_query.is_empty());
        assert!(!app.overlay_filtering);
    }
}
