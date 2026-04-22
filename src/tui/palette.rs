//! Command palette — fuzzy-searchable list of context-aware actions.
//!
//! Actions are derived from the runner's current service/task state at open
//! time: restart/stop the running services, start the stopped ones, run all
//! tasks in `PendingRun`. Selecting one dispatches a [`RunnerCommand`].
//!
//! The palette snapshots the action list on open and holds it until close —
//! further state changes don't shuffle the list under the user. Selecting
//! a stale action (e.g. restart a service that was just stopped) still sends
//! the command; the runner will reject it cleanly via its own validation.

use std::collections::HashMap;

use super::fuzzy::fuzzy_match;
use crate::runner::{ServiceState, TaskItemState};

/// Maximum number of match rows to show in the palette dropdown.
pub(crate) const MAX_PALETTE_ROWS: usize = 8;

/// A user-triggerable action. The label is shown in the palette; the kind
/// maps to a [`RunnerCommand`] at dispatch time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Action {
    pub(crate) label: String,
    pub(crate) kind: ActionKind,
}

/// What the action actually does. Kept separate from the display label so
/// we can round-trip via fuzzy search (which sees only the label).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActionKind {
    RunPendingTasks,
    StartService(String),
    StopService(String),
    RestartService(String),
    /// Re-run the build command (if any), then restart. Distinct from
    /// `RestartService`, which skips the build step.
    RebuildService(String),
    RunTask(String),
}

/// Palette state — only meaningful while view_mode == Palette, but lives on
/// [`App`] so a later reopen can preserve the query if we ever want that.
///
/// [`App`]: super::app::App
#[derive(Debug, Default, Clone)]
pub(crate) struct ActionPalette {
    all_actions: Vec<Action>,
    query: String,
    /// Indices into `all_actions`, ordered by fuzzy match rank.
    matches: Vec<usize>,
    highlight: usize,
}

impl ActionPalette {
    /// Rebuild the action list from the current state snapshot and reset
    /// query/highlight. Call on every open — state changes constantly.
    pub(crate) fn open(
        &mut self,
        services: &HashMap<String, ServiceState>,
        tasks: &HashMap<String, TaskItemState>,
    ) {
        self.all_actions = build_actions(services, tasks);
        self.query.clear();
        self.highlight = 0;
        self.matches = (0..self.all_actions.len()).collect();
    }

    /// Clear the palette so `close()` calls can be idempotent.
    pub(crate) fn close(&mut self) {
        self.all_actions.clear();
        self.matches.clear();
        self.query.clear();
        self.highlight = 0;
    }

    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    /// Rows to display, already fuzzy-sorted and capped at [`MAX_PALETTE_ROWS`].
    pub(crate) fn visible(&self) -> impl Iterator<Item = (usize, &Action)> {
        self.matches
            .iter()
            .take(MAX_PALETTE_ROWS)
            .enumerate()
            .filter_map(|(i, idx)| self.all_actions.get(*idx).map(|a| (i, a)))
    }

    pub(crate) fn visible_count(&self) -> usize {
        self.matches.len().min(MAX_PALETTE_ROWS)
    }

    pub(crate) fn highlight(&self) -> usize {
        self.highlight
    }

    pub(crate) fn push_query_char(&mut self, c: char) {
        self.query.push(c);
        self.recompute();
    }

    pub(crate) fn pop_query_char(&mut self) {
        if self.query.pop().is_some() {
            self.recompute();
        }
    }

    pub(crate) fn highlight_prev(&mut self) {
        if self.matches.is_empty() {
            return;
        }
        self.highlight = if self.highlight == 0 {
            self.matches.len() - 1
        } else {
            self.highlight - 1
        };
    }

    pub(crate) fn highlight_next(&mut self) {
        if self.matches.is_empty() {
            return;
        }
        self.highlight = (self.highlight + 1) % self.matches.len();
    }

    /// Currently highlighted action, if any.
    pub(crate) fn selected(&self) -> Option<&Action> {
        self.matches
            .get(self.highlight)
            .and_then(|i| self.all_actions.get(*i))
    }

    fn recompute(&mut self) {
        let labels: Vec<String> = self.all_actions.iter().map(|a| a.label.clone()).collect();
        if self.query.is_empty() {
            self.matches = (0..self.all_actions.len()).collect();
        } else {
            let matched_labels = fuzzy_match(&self.query, &labels);
            // Map labels back to indices. Duplicate labels shouldn't happen —
            // actions embed unique service names — but `position` returning
            // `None` is harmless (we just skip it).
            self.matches = matched_labels
                .into_iter()
                .filter_map(|label| labels.iter().position(|l| l == &label))
                .collect();
        }
        if self.highlight >= self.matches.len() {
            self.highlight = 0;
        }
    }
}

/// Build the available action list from the current state.
///
/// Ordering (top to bottom in the palette):
/// 1. "Run all pending tasks" if any task is in [`PendingRun`](TaskItemState::PendingRun).
/// 2. Per-service actions, sorted alphabetically by service name, with
///    action set varying by state.
/// 3. Per-task run actions, sorted alphabetically. Running tasks are skipped
///    to avoid double-spawning; every other state is runnable, with a
///    `(needs run)` suffix for states that indicate the user should run it
///    (`Pending`, `Failed`, `PendingRun`).
pub(crate) fn build_actions(
    services: &HashMap<String, ServiceState>,
    tasks: &HashMap<String, TaskItemState>,
) -> Vec<Action> {
    let mut actions = Vec::new();

    let pending_count = tasks
        .values()
        .filter(|s| **s == TaskItemState::PendingRun)
        .count();
    if pending_count > 0 {
        actions.push(Action {
            label: format!("Run all pending tasks ({pending_count})"),
            kind: ActionKind::RunPendingTasks,
        });
    }

    let mut names: Vec<&String> = services.keys().collect();
    names.sort();
    for name in names {
        let Some(state) = services.get(name) else {
            continue;
        };
        match state {
            ServiceState::Ready | ServiceState::Running | ServiceState::Unhealthy => {
                actions.push(Action {
                    label: format!("Restart {name}"),
                    kind: ActionKind::RestartService(name.clone()),
                });
                actions.push(Action {
                    label: format!("Rebuild {name}"),
                    kind: ActionKind::RebuildService(name.clone()),
                });
                actions.push(Action {
                    label: format!("Stop {name}"),
                    kind: ActionKind::StopService(name.clone()),
                });
            }
            ServiceState::Stopped | ServiceState::Failed => {
                actions.push(Action {
                    label: format!("Start {name}"),
                    kind: ActionKind::StartService(name.clone()),
                });
                actions.push(Action {
                    label: format!("Rebuild {name}"),
                    kind: ActionKind::RebuildService(name.clone()),
                });
            }
            ServiceState::Pending
            | ServiceState::Building
            | ServiceState::Lazy
            | ServiceState::Starting
            | ServiceState::Stopping => {
                // In-flight states — offering Stop/Start/Restart during these
                // transitions would race with the ongoing lifecycle operation.
            }
        }
    }

    let mut task_names: Vec<&String> = tasks.keys().collect();
    task_names.sort();
    for name in task_names {
        let Some(state) = tasks.get(name) else {
            continue;
        };
        let suffix = match state {
            TaskItemState::Running | TaskItemState::Building => continue,
            TaskItemState::Pending | TaskItemState::Failed | TaskItemState::PendingRun => {
                " (needs run)"
            }
            TaskItemState::Completed | TaskItemState::Skipped => "",
        };
        actions.push(Action {
            label: format!("Run {name}{suffix}"),
            kind: ActionKind::RunTask(name.clone()),
        });
    }

    actions
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
    fn build_actions_table() {
        struct Case {
            name: &'static str,
            services: Vec<(&'static str, ServiceState)>,
            tasks: Vec<(&'static str, TaskItemState)>,
            want_labels: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "empty",
                services: vec![],
                tasks: vec![],
                want_labels: vec![],
            },
            Case {
                name: "one ready service → restart + rebuild + stop",
                services: vec![("api", ServiceState::Ready)],
                tasks: vec![],
                want_labels: vec!["Restart api", "Rebuild api", "Stop api"],
            },
            Case {
                name: "stopped service → start + rebuild",
                services: vec![("worker", ServiceState::Stopped)],
                tasks: vec![],
                want_labels: vec!["Start worker", "Rebuild worker"],
            },
            Case {
                name: "in-flight states offer nothing",
                services: vec![
                    ("a", ServiceState::Pending),
                    ("b", ServiceState::Starting),
                    ("c", ServiceState::Stopping),
                    ("d", ServiceState::Lazy),
                ],
                tasks: vec![],
                want_labels: vec![],
            },
            Case {
                name: "pending tasks surface at top",
                services: vec![("api", ServiceState::Ready)],
                tasks: vec![
                    ("migrate", TaskItemState::PendingRun),
                    ("seed", TaskItemState::PendingRun),
                ],
                want_labels: vec![
                    "Run all pending tasks (2)",
                    "Restart api",
                    "Rebuild api",
                    "Stop api",
                    "Run migrate (needs run)",
                    "Run seed (needs run)",
                ],
            },
            Case {
                name: "every task is runnable with per-state suffix",
                services: vec![],
                tasks: vec![
                    ("a_pending", TaskItemState::Pending),
                    ("b_completed", TaskItemState::Completed),
                    ("c_skipped", TaskItemState::Skipped),
                    ("d_failed", TaskItemState::Failed),
                    ("e_pending_run", TaskItemState::PendingRun),
                ],
                want_labels: vec![
                    "Run all pending tasks (1)",
                    "Run a_pending (needs run)",
                    "Run b_completed",
                    "Run c_skipped",
                    "Run d_failed (needs run)",
                    "Run e_pending_run (needs run)",
                ],
            },
            Case {
                name: "running tasks are skipped to avoid double-spawn",
                services: vec![],
                tasks: vec![
                    ("build", TaskItemState::Running),
                    ("lint", TaskItemState::Completed),
                ],
                want_labels: vec!["Run lint"],
            },
            Case {
                name: "services sorted alphabetically",
                services: vec![
                    ("worker", ServiceState::Ready),
                    ("api", ServiceState::Ready),
                ],
                tasks: vec![],
                want_labels: vec![
                    "Restart api",
                    "Rebuild api",
                    "Stop api",
                    "Restart worker",
                    "Rebuild worker",
                    "Stop worker",
                ],
            },
        ];

        for case in cases {
            let got = build_actions(&services(&case.services), &tasks(&case.tasks));
            let got_labels: Vec<&str> = got.iter().map(|a| a.label.as_str()).collect();
            assert_eq!(got_labels, case.want_labels, "case: {}", case.name);
        }
    }

    #[test]
    fn palette_fuzzy_narrows_visible_matches() {
        let mut p = ActionPalette::default();
        p.open(
            &services(&[
                ("api", ServiceState::Ready),
                ("worker", ServiceState::Ready),
            ]),
            &tasks(&[]),
        );
        // 6 actions per ready service: Restart, Rebuild, Stop ×2 services.
        // The `w` query narrows to the worker triplet plus visible cap.
        assert_eq!(p.visible_count(), 6);
        p.push_query_char('w');
        let labels: Vec<&str> = p.visible().map(|(_, a)| a.label.as_str()).collect();
        // All three worker labels match; ranking order is an implementation
        // detail of the fuzzy matcher — just assert the matched set.
        let mut sorted = labels.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec!["Rebuild worker", "Restart worker", "Stop worker"]
        );
    }

    #[test]
    fn palette_highlight_wraps() {
        let mut p = ActionPalette::default();
        p.open(&services(&[("api", ServiceState::Ready)]), &tasks(&[]));
        // 3 actions: Restart api, Rebuild api, Stop api.
        assert_eq!(p.highlight(), 0);
        p.highlight_prev();
        assert_eq!(p.highlight(), 2);
        p.highlight_next();
        assert_eq!(p.highlight(), 0);
    }

    #[test]
    fn palette_close_empties_state() {
        let mut p = ActionPalette::default();
        p.open(&services(&[("api", ServiceState::Ready)]), &tasks(&[]));
        p.push_query_char('s');
        p.close();
        assert_eq!(p.query(), "");
        assert_eq!(p.visible_count(), 0);
    }

    #[test]
    fn palette_selected_is_none_when_no_matches() {
        let mut p = ActionPalette::default();
        p.open(&services(&[("api", ServiceState::Ready)]), &tasks(&[]));
        p.push_query_char('z'); // matches nothing
        assert_eq!(p.visible_count(), 0);
        assert!(p.selected().is_none());
    }

    #[test]
    fn palette_highlight_clamps_when_matches_shrink() {
        let mut p = ActionPalette::default();
        p.open(
            &services(&[
                ("api", ServiceState::Ready),
                ("worker", ServiceState::Ready),
            ]),
            &tasks(&[]),
        );
        // 6 actions total (Restart, Rebuild, Stop ×2). Highlight the last.
        for _ in 0..5 {
            p.highlight_next();
        }
        assert_eq!(p.highlight(), 5);
        // Typing 'w' narrows to the worker triplet — highlight must clamp to 0.
        p.push_query_char('w');
        assert_eq!(p.visible_count(), 3);
        assert_eq!(p.highlight(), 0);
    }
}
