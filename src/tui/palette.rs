//! Tasks palette — fuzzy-searchable list of runnable tasks.
//!
//! Derived from the runner's current task state at open time: "run all
//! pending tasks" if any are in `PendingRun`, plus a per-task run action
//! for every non-running task. Service lifecycle actions (start/stop/
//! restart) live on the status overlay instead — they're tied to a
//! specific highlighted service and keyed by state.
//!
//! The palette snapshots the list on open and holds it until close — state
//! changes don't shuffle the list under the user. Selecting a stale action
//! (e.g. run a task that just finished) still sends the command; the
//! runner validates on receipt.

use std::collections::HashMap;

use super::fuzzy::fuzzy_match;
use crate::config::Task;
use crate::runner::TaskItemState;

/// A user-triggerable action. The label is shown in the palette; the kind
/// maps to a [`RunnerCommand`] at dispatch time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Action {
    pub(crate) label: String,
    pub(crate) kind: ActionKind,
    pub(crate) task_name: Option<String>,
    pub(crate) task_state: Option<TaskItemState>,
    pub(crate) needs_run: bool,
}

/// What the action actually does. Kept separate from the display label so
/// we can round-trip via fuzzy search (which sees only the label).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum ActionKind {
    RunPendingTasks,
    /// Run a param-less task — dispatches immediately on Enter.
    RunTask(String),
    /// Run a task that has declared `params`. Enter opens a form modal
    /// instead of dispatching; the form collects values and then sends
    /// the final `RunTask` command.
    RunTaskWithForm(String),
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
    ///
    /// `task_configs` is consulted to pick between [`ActionKind::RunTask`]
    /// (no params, dispatch immediately) and
    /// [`ActionKind::RunTaskWithForm`] (declared params, open form first).
    pub(crate) fn open(
        &mut self,
        tasks: &HashMap<String, TaskItemState>,
        task_configs: &HashMap<String, Task>,
    ) {
        self.all_actions = build_actions(tasks, task_configs);
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

    /// Rows to display, already fuzzy-sorted. The caller (ratatui list)
    /// handles vertical scrolling when there are more rows than fit.
    pub(crate) fn visible(&self) -> impl Iterator<Item = (usize, &Action)> {
        self.matches
            .iter()
            .enumerate()
            .filter_map(|(i, idx)| self.all_actions.get(*idx).map(|a| (i, a)))
    }

    pub(crate) fn visible_count(&self) -> usize {
        self.matches.len()
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

/// Build the available action list from the current task state.
///
/// Ordering (top to bottom in the palette):
/// 1. "Run all pending tasks" if any task is in [`PendingRun`](TaskItemState::PendingRun).
/// 2. Per-task run actions that need attention, sorted alphabetically.
/// 3. Other runnable task actions, sorted alphabetically.
///
/// Running/building tasks are skipped to avoid double-spawning.
pub(crate) fn build_actions(
    tasks: &HashMap<String, TaskItemState>,
    task_configs: &HashMap<String, Task>,
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
            task_name: None,
            task_state: None,
            needs_run: true,
        });
    }

    let mut task_entries: Vec<(&String, TaskItemState)> = tasks
        .iter()
        .filter_map(|(name, state)| match state {
            TaskItemState::Running | TaskItemState::Building => None,
            _ => Some((name, *state)),
        })
        .collect();
    task_entries.sort_by(|(a_name, a_state), (b_name, b_state)| {
        task_action_sort_bucket(*a_state)
            .cmp(&task_action_sort_bucket(*b_state))
            .then_with(|| a_name.cmp(b_name))
    });

    for (name, state) in task_entries {
        let needs_run = task_needs_run(state);
        let suffix = if needs_run { " (needs run)" } else { "" };
        let has_params = task_configs.get(name).is_some_and(|t| !t.params.is_empty());
        let kind = if has_params {
            ActionKind::RunTaskWithForm(name.clone())
        } else {
            ActionKind::RunTask(name.clone())
        };
        actions.push(Action {
            label: format!("Run {name}{suffix}"),
            kind,
            task_name: Some(name.clone()),
            task_state: Some(state),
            needs_run,
        });
    }

    actions
}

fn task_action_sort_bucket(state: TaskItemState) -> u8 {
    if task_needs_run(state) { 0 } else { 1 }
}

fn task_needs_run(state: TaskItemState) -> bool {
    matches!(
        state,
        TaskItemState::Pending
            | TaskItemState::Failed
            | TaskItemState::DependencyFailed
            | TaskItemState::PendingRun
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn tasks(entries: &[(&str, TaskItemState)]) -> HashMap<String, TaskItemState> {
        entries.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    fn no_task_configs() -> HashMap<String, Task> {
        HashMap::new()
    }

    fn task_configs_with_params(names: &[(&str, bool)]) -> HashMap<String, Task> {
        use crate::config::ParamKind;
        names
            .iter()
            .map(|(name, has_params)| {
                let params = if *has_params {
                    vec![crate::config::TaskParam {
                        name: "x".into(),
                        prompt: None,
                        required: false,
                        default: None,
                        kind: ParamKind::String,
                        choices: vec![],
                        completions: None,
                        validate: None,
                    }]
                } else {
                    vec![]
                };
                (
                    name.to_string(),
                    Task {
                        cmd: "echo".into(),
                        args: vec![],
                        dir: None,
                        env: HashMap::new(),
                        depends_on: vec![],
                        watch: vec![],
                        ignore: vec![],
                        timeout: None,
                        log: crate::config::LogConfig::Stdout,
                        terminal: crate::config::TaskTerminal::default(),
                        auto_run: crate::config::TaskAutoRun::Always,
                        download: None,
                        bazel: None,
                        turbo: None,
                        params,
                        hidden: false,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn build_actions_table() {
        struct Case {
            name: &'static str,
            tasks: Vec<(&'static str, TaskItemState)>,
            want_labels: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "empty",
                tasks: vec![],
                want_labels: vec![],
            },
            Case {
                name: "every task is runnable with per-state suffix",
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
                    "Run d_failed (needs run)",
                    "Run e_pending_run (needs run)",
                    "Run b_completed",
                    "Run c_skipped",
                ],
            },
            Case {
                name: "running tasks are skipped to avoid double-spawn",
                tasks: vec![
                    ("build", TaskItemState::Running),
                    ("lint", TaskItemState::Completed),
                ],
                want_labels: vec!["Run lint"],
            },
            Case {
                name: "pending-run tasks surface the aggregate action",
                tasks: vec![
                    ("migrate", TaskItemState::PendingRun),
                    ("seed", TaskItemState::PendingRun),
                ],
                want_labels: vec![
                    "Run all pending tasks (2)",
                    "Run migrate (needs run)",
                    "Run seed (needs run)",
                ],
            },
        ];

        for case in cases {
            let got = build_actions(&tasks(&case.tasks), &no_task_configs());
            let got_labels: Vec<&str> = got.iter().map(|a| a.label.as_str()).collect();
            assert_eq!(got_labels, case.want_labels, "case: {}", case.name);
        }
    }

    #[test]
    fn palette_fuzzy_narrows_visible_matches() {
        let mut p = ActionPalette::default();
        p.open(
            &tasks(&[
                ("build", TaskItemState::Completed),
                ("test", TaskItemState::Completed),
                ("lint", TaskItemState::Completed),
            ]),
            &no_task_configs(),
        );
        assert_eq!(p.visible_count(), 3);
        p.push_query_char('t');
        let labels: Vec<&str> = p.visible().map(|(_, a)| a.label.as_str()).collect();
        assert!(labels.iter().all(|l| l.contains('t')));
    }

    #[test]
    fn palette_highlight_wraps() {
        let mut p = ActionPalette::default();
        p.open(
            &tasks(&[
                ("a", TaskItemState::Completed),
                ("b", TaskItemState::Completed),
            ]),
            &no_task_configs(),
        );
        assert_eq!(p.highlight(), 0);
        p.highlight_prev();
        assert_eq!(p.highlight(), 1);
        p.highlight_next();
        assert_eq!(p.highlight(), 0);
    }

    #[test]
    fn palette_close_empties_state() {
        let mut p = ActionPalette::default();
        p.open(
            &tasks(&[("a", TaskItemState::Completed)]),
            &no_task_configs(),
        );
        p.push_query_char('x');
        p.close();
        assert_eq!(p.query(), "");
        assert_eq!(p.visible_count(), 0);
    }

    #[test]
    fn palette_selected_is_none_when_no_matches() {
        let mut p = ActionPalette::default();
        p.open(
            &tasks(&[("a", TaskItemState::Completed)]),
            &no_task_configs(),
        );
        p.push_query_char('z');
        assert_eq!(p.visible_count(), 0);
        assert!(p.selected().is_none());
    }

    #[test]
    fn palette_highlight_clamps_when_matches_shrink() {
        let mut p = ActionPalette::default();
        p.open(
            &tasks(&[
                ("alpha", TaskItemState::Completed),
                ("beta", TaskItemState::Completed),
                ("gamma", TaskItemState::Completed),
            ]),
            &no_task_configs(),
        );
        p.highlight_next();
        p.highlight_next();
        assert_eq!(p.highlight(), 2);
        p.push_query_char('a'); // matches alpha/beta/gamma all
        // Typing can reshuffle; just ensure highlight stays valid.
        assert!(p.highlight() < p.visible_count());
    }

    #[test]
    fn paramd_tasks_use_run_task_with_form() {
        let configs = task_configs_with_params(&[("plain", false), ("interactive", true)]);
        let tasks = tasks(&[
            ("plain", TaskItemState::Completed),
            ("interactive", TaskItemState::Completed),
        ]);
        let got = build_actions(&tasks, &configs);
        let plain = got
            .iter()
            .find(|a| a.label.starts_with("Run plain"))
            .expect("plain missing");
        let interactive = got
            .iter()
            .find(|a| a.label.starts_with("Run interactive"))
            .expect("interactive missing");
        assert!(matches!(plain.kind, ActionKind::RunTask(ref n) if n == "plain"));
        assert!(
            matches!(interactive.kind, ActionKind::RunTaskWithForm(ref n) if n == "interactive"),
            "got {:?}",
            interactive.kind,
        );
        assert_eq!(interactive.label, "Run interactive");
    }

    #[test]
    fn paramd_tasks_keep_needs_run_label() {
        let configs = task_configs_with_params(&[("migrate", true)]);
        let tasks = tasks(&[("migrate", TaskItemState::PendingRun)]);
        let got = build_actions(&tasks, &configs);
        let migrate = got
            .iter()
            .find(|a| a.label.starts_with("Run migrate"))
            .expect("missing migrate");
        assert!(
            matches!(migrate.kind, ActionKind::RunTaskWithForm(_)),
            "got {:?}",
            migrate.kind,
        );
        assert_eq!(migrate.label, "Run migrate (needs run)");
    }
}
