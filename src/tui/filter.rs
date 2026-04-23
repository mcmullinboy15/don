//! Log filter — multi-select set of service/task names.
//!
//! The filter is *always on*. `active_selected` is the set of names whose
//! log lines are rendered. Defaults come from the config: each service or
//! task declares `hidden = true` to start outside the active set. The user
//! can narrow or widen the selection interactively at any time.
//!
//! The filter has two lives:
//! - **Editing**: the user is in filter mode, typing a query and toggling
//!   selections with Space. `editing_selected` holds the work-in-progress set.
//! - **Active**: the filter has been committed with Enter. `active_selected`
//!   is what log lines are checked against.
//!
//! Blank spacer lines (`name == ""`, inserted when the user presses Enter
//! in Normal mode) always pass regardless of the filter — they're UI
//! whitespace, not log content. `[don]` lifecycle events are *not* special:
//! they carry the `"don"` name and are gated like any other entry, and the
//! `hidden` flag can be set per-service/task to hide them by default.
//!
//! `all_names` is the source of truth — the union of service and task names
//! the runner knows about at TUI startup, plus the synthetic `"don"` entry
//! for lifecycle events. It doesn't update on live reload; a config reload
//! that renames services will require Esc'ing the filter.
//!
//! ## The "all" synthetic row
//!
//! The edit view prepends a synthetic `[all]` row when the query is empty.
//! Toggling it with Space flips every name between "all selected" and
//! "none selected". Committing Enter while highlighted on this row selects
//! all names. The row disappears once the user starts typing a query.

use std::collections::HashSet;

use super::fuzzy::fuzzy_match;

/// A row displayed in the filter edit list. `All` is a synthetic convenience
/// row that toggles every name at once; `Name` holds a real service/task
/// name that can be individually toggled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FilterRow {
    /// Synthetic "select/deselect all" row. Only appears when the query is
    /// empty.
    All,
    /// A real service or task name. Membership in the active set controls
    /// whether its log lines render.
    Name(String),
}

/// Filter state, persistent across mode switches.
#[derive(Debug, Clone)]
pub(crate) struct FilterState {
    /// Every filterable name (services + tasks + "don"), sorted and deduped.
    all_names: Vec<String>,
    /// Names the modal list should skip when rendering rows. Used for
    /// services that are currently in [`ServiceState::Lazy`] — they haven't
    /// been started and would otherwise clutter the filter list. Updated
    /// dynamically as services transition in/out of Lazy state.
    ///
    /// Membership in this set does NOT remove a name from `all_names` or
    /// the selection — once the service leaves Lazy, it reappears in the
    /// list and its prior selection state is preserved.
    hidden_from_display: HashSet<String>,
    /// The config-derived default selection: `all_names - hidden`. Used by
    /// [`FilterState::reset_to_defaults`].
    default_selected: HashSet<String>,
    /// Query buffer visible only in filter-edit mode.
    query: String,
    /// Rows for the current query, in display order. Includes a synthetic
    /// [`FilterRow::All`] at position 0 when the query is empty. Names in
    /// `hidden_from_display` are omitted.
    rows: Vec<FilterRow>,
    /// Highlighted index within `rows` — what Up/Down move.
    highlight: usize,
    /// Work-in-progress selection, seeded from `active_selected` on entering
    /// edit mode. Space toggles membership here.
    editing_selected: HashSet<String>,
    /// Whether the user toggled anything (via Space) since entering edit
    /// mode. Used by [`FilterState::commit`] to distinguish the
    /// "type a query, press Enter" narrowing shortcut from curating via
    /// explicit Space toggles.
    edit_touched: bool,
    /// Committed selection. Log lines pass iff their name is in this set.
    active_selected: HashSet<String>,
}

impl FilterState {
    /// Initialize with the full set of filterable names and the set of names
    /// that should start hidden. `active_selected` is seeded to
    /// `all_names - hidden`; that same set is remembered as the reset target.
    pub(crate) fn new(mut all_names: Vec<String>, hidden: &HashSet<String>) -> Self {
        all_names.sort();
        all_names.dedup();
        let default_selected: HashSet<String> = all_names
            .iter()
            .filter(|n| !hidden.contains(n.as_str()))
            .cloned()
            .collect();
        let active_selected = default_selected.clone();
        let hidden_from_display = HashSet::new();
        let rows = build_rows(&all_names, "", &hidden_from_display);
        Self {
            all_names,
            hidden_from_display,
            default_selected,
            query: String::new(),
            rows,
            highlight: 0,
            editing_selected: HashSet::new(),
            edit_touched: false,
            active_selected,
        }
    }

    /// Replace the set of names hidden from the modal list (e.g. Lazy
    /// services) and recompute rows. Highlight is clamped if it fell off
    /// the end. Call this whenever service state changes — it's cheap
    /// (single pass over `all_names`).
    pub(crate) fn set_hidden_from_display(&mut self, hidden: HashSet<String>) {
        if self.hidden_from_display == hidden {
            return;
        }
        self.hidden_from_display = hidden;
        self.recompute_rows();
    }

    /// True when the current active selection hides any names. Used by the
    /// status bar to decide between "filter: api, db [esc] reset" and the
    /// normal idle hint.
    pub(crate) fn is_active(&self) -> bool {
        self.active_selected.len() != self.all_names.len()
    }

    /// True if the given log line name passes the currently-active filter.
    /// Blank spacer lines (empty name) always pass; real names must be in
    /// the active selection.
    pub(crate) fn passes(&self, name: &str) -> bool {
        if name.is_empty() {
            return true;
        }
        self.active_selected.contains(name)
    }

    /// Start editing. Seeds the edit selection from the currently active filter
    /// so users can refine an existing filter rather than rebuild it.
    pub(crate) fn enter_edit(&mut self) {
        self.query.clear();
        self.rows = build_rows(&self.all_names, "", &self.hidden_from_display);
        self.highlight = 0;
        self.editing_selected = self.active_selected.clone();
        self.edit_touched = false;
    }

    /// Commit the current edit selection as the active filter.
    ///
    /// Resolves the user's intent based on what they did:
    /// - Any Space toggles → commit the curated `editing_selected` as-is.
    /// - Otherwise with a non-empty query → narrow to the highlighted row
    ///   (the "type to narrow" shortcut; replaces any prior selection).
    /// - Otherwise → commit `editing_selected` unchanged (likely a no-op).
    pub(crate) fn commit(&mut self) {
        if !self.edit_touched && !self.query.is_empty() {
            self.editing_selected.clear();
            match self.rows.get(self.highlight) {
                Some(FilterRow::Name(name)) => {
                    self.editing_selected.insert(name.clone());
                }
                Some(FilterRow::All) => {
                    // All row only renders with empty query; unreachable
                    // in practice but safe to handle.
                    self.editing_selected.extend(self.all_names.iter().cloned());
                }
                None => {}
            }
        }
        self.active_selected = std::mem::take(&mut self.editing_selected);
        self.edit_touched = false;
        self.query.clear();
        self.highlight = 0;
        self.rows = build_rows(&self.all_names, "", &self.hidden_from_display);
    }

    /// Reset the active selection to the config-derived defaults. Also
    /// clears any in-flight edit state.
    pub(crate) fn reset_to_defaults(&mut self) {
        self.active_selected = self.default_selected.clone();
        self.editing_selected.clear();
        self.edit_touched = false;
        self.query.clear();
        self.highlight = 0;
        self.rows = build_rows(&self.all_names, "", &self.hidden_from_display);
    }

    /// Append a character to the query and recompute rows.
    pub(crate) fn push_query_char(&mut self, c: char) {
        self.query.push(c);
        self.recompute_rows();
    }

    /// Remove the last character of the query (noop if empty).
    pub(crate) fn pop_query_char(&mut self) {
        if self.query.pop().is_some() {
            self.recompute_rows();
        }
    }

    /// Move highlight up with wrap-around.
    pub(crate) fn highlight_prev(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        self.highlight = if self.highlight == 0 {
            self.rows.len() - 1
        } else {
            self.highlight - 1
        };
    }

    /// Move highlight down with wrap-around.
    pub(crate) fn highlight_next(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        self.highlight = (self.highlight + 1) % self.rows.len();
    }

    /// Toggle the highlighted row in the edit selection. For `Name(n)`, flip
    /// membership. For `All`, select every name (if any were missing) or
    /// clear every name (if all were already present).
    pub(crate) fn toggle_highlighted(&mut self) {
        match self.rows.get(self.highlight) {
            Some(FilterRow::Name(name)) => {
                if self.editing_selected.contains(name) {
                    self.editing_selected.remove(name);
                } else {
                    self.editing_selected.insert(name.clone());
                }
                self.edit_touched = true;
            }
            Some(FilterRow::All) => {
                if self.all_selected_in_edit() {
                    self.editing_selected.clear();
                } else {
                    self.editing_selected.extend(self.all_names.iter().cloned());
                }
                self.edit_touched = true;
            }
            None => {}
        }
    }

    /// Current query string (for bar rendering).
    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    /// Currently visible rows (for list rendering).
    pub(crate) fn rows(&self) -> &[FilterRow] {
        &self.rows
    }

    /// Currently highlighted row index.
    pub(crate) fn highlight(&self) -> usize {
        self.highlight
    }

    /// Whether a name is in the edit-mode selection (used to draw the
    /// checkbox for a [`FilterRow::Name`]).
    pub(crate) fn is_edit_selected(&self, name: &str) -> bool {
        self.editing_selected.contains(name)
    }

    /// Whether every name is currently in the edit-mode selection. Used to
    /// draw the checkbox for the synthetic [`FilterRow::All`] row.
    pub(crate) fn all_selected_in_edit(&self) -> bool {
        !self.all_names.is_empty() && self.editing_selected.len() == self.all_names.len()
    }

    #[cfg(test)]
    fn active_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.active_selected.iter().map(String::as_str).collect();
        names.sort();
        names
    }

    fn recompute_rows(&mut self) {
        self.rows = build_rows(&self.all_names, &self.query, &self.hidden_from_display);
        if self.highlight >= self.rows.len() {
            self.highlight = 0;
        }
    }
}

/// Build the row list for the given query. The synthetic [`FilterRow::All`]
/// row only appears when the query is empty — a query implies the user is
/// narrowing to specific names, so the select-all affordance would be noise.
/// Names in `hidden_from_display` are skipped (e.g. Lazy services).
fn build_rows(
    all_names: &[String],
    query: &str,
    hidden_from_display: &HashSet<String>,
) -> Vec<FilterRow> {
    let visible = |name: &String| !hidden_from_display.contains(name);
    if query.is_empty() {
        let mut rows = Vec::with_capacity(all_names.len() + 1);
        rows.push(FilterRow::All);
        rows.extend(
            all_names
                .iter()
                .filter(|n| visible(n))
                .cloned()
                .map(FilterRow::Name),
        );
        rows
    } else {
        // `fuzzy_match` doesn't know about hidden_from_display; filter its
        // output instead of pre-filtering the candidate list to preserve
        // the matcher's scoring view of the full set.
        fuzzy_match(query, all_names)
            .into_iter()
            .filter(|n| !hidden_from_display.contains(n))
            .map(FilterRow::Name)
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn state(names: &[&str]) -> FilterState {
        FilterState::new(
            names.iter().map(|s| s.to_string()).collect(),
            &HashSet::new(),
        )
    }

    fn state_with_hidden(names: &[&str], hidden: &[&str]) -> FilterState {
        let hidden_set: HashSet<String> = hidden.iter().map(|s| s.to_string()).collect();
        FilterState::new(names.iter().map(|s| s.to_string()).collect(), &hidden_set)
    }

    #[test]
    fn default_filter_passes_all_when_nothing_hidden() {
        let s = state(&["api", "worker"]);
        assert!(s.passes("api"));
        assert!(s.passes("worker"));
        assert!(s.passes(""), "blank spacer lines always pass");
        // "filter is active" reports diverges-from-full-set; with no hidden
        // names, every name is selected and the bar treats it as inactive.
        assert!(!s.is_active());
    }

    #[test]
    fn hidden_names_are_gated_by_default() {
        let s = state_with_hidden(&["api", "worker", "db"], &["db"]);
        assert!(s.passes("api"));
        assert!(s.passes("worker"));
        assert!(!s.passes("db"), "db hidden by default");
        assert!(s.is_active(), "filter reports active when any name is hidden");
    }

    #[test]
    fn unknown_names_are_blocked_even_without_config() {
        // With the filter always on, names the runner didn't declare at
        // startup are unrecognized — gated out rather than silently passing.
        let s = state(&["api"]);
        assert!(!s.passes("mystery"));
    }

    #[test]
    fn commit_narrows_to_typed_query() {
        let mut s = state(&["api", "worker", "db"]);
        s.enter_edit();
        s.push_query_char('a');
        s.commit();
        assert!(s.is_active());
        assert!(s.passes("api"));
        assert!(!s.passes("worker"));
        assert!(!s.passes("db"));
        assert!(s.passes(""), "blank spacer lines always pass");
    }

    #[test]
    fn don_lifecycle_gated_when_not_selected() {
        let mut s = state(&["api", "don"]);
        s.enter_edit();
        s.push_query_char('a'); // matches "api" but not "don"
        s.commit();
        assert!(s.passes("api"));
        assert!(!s.passes("don"), "don gated when not in selection");
    }

    #[test]
    fn space_multiselect_then_enter_commits_selection() {
        let mut s = state(&["api", "worker", "db", "cache"]);
        s.enter_edit();
        // Sorted rows: [All, api, cache, db, worker]. Highlight starts at 0 (All).
        // Clear the pre-seeded full selection via the All row, then pick two.
        s.toggle_highlighted(); // All → clear
        s.highlight_next(); // api
        s.highlight_next(); // cache
        s.toggle_highlighted(); // cache
        s.highlight_next(); // db
        s.toggle_highlighted(); // db
        s.commit();
        let mut active: Vec<String> = s.active_names().iter().map(|n| n.to_string()).collect();
        active.sort();
        assert_eq!(active, vec!["cache".to_string(), "db".to_string()]);
        assert!(s.passes("cache"));
        assert!(s.passes("db"));
        assert!(!s.passes("api"));
    }

    #[test]
    fn single_match_plus_enter_auto_selects_highlight() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        s.push_query_char('a'); // fuzzy matches "api"
        s.commit();
        let active: Vec<&str> = s.active_names();
        assert_eq!(active, vec!["api"]);
    }

    #[test]
    fn reset_to_defaults_restores_config_selection() {
        // api hidden by default — after a custom filter, reset should bring
        // back the config defaults (api hidden, worker visible).
        let mut s = state_with_hidden(&["api", "worker"], &["api"]);
        assert!(!s.passes("api"));
        assert!(s.passes("worker"));

        s.enter_edit();
        s.push_query_char('a');
        s.commit();
        assert!(s.passes("api"));
        assert!(!s.passes("worker"));

        s.reset_to_defaults();
        assert!(!s.passes("api"));
        assert!(s.passes("worker"));
    }

    #[test]
    fn highlight_wraps_on_both_ends() {
        let mut s = state(&["a", "b", "c"]);
        s.enter_edit();
        // Rows: [All, a, b, c] — length 4.
        assert_eq!(s.highlight(), 0);
        s.highlight_prev();
        assert_eq!(s.highlight(), 3, "up from top wraps to bottom");
        s.highlight_next();
        assert_eq!(s.highlight(), 0, "down from bottom wraps to top");
    }

    #[test]
    fn highlight_clamps_when_rows_shrink_under_it() {
        let mut s = state(&["api", "db", "worker"]);
        s.enter_edit();
        // Rows: [All, api, db, worker]. Highlight last.
        s.highlight_next();
        s.highlight_next();
        s.highlight_next();
        assert_eq!(s.highlight(), 3);
        // Typing 'a' narrows rows to just "api" (no All row once query is
        // non-empty) — highlight clamps to 0.
        s.push_query_char('a');
        assert_eq!(s.rows().len(), 1);
        assert_eq!(s.highlight(), 0);
    }

    #[test]
    fn all_row_toggle_flips_between_all_and_none() {
        let mut s = state(&["api", "worker", "db"]);
        s.enter_edit();
        // Enter edit seeds editing_selected from active_selected — all names
        // are in by default, so the All row shows checked.
        assert!(s.all_selected_in_edit());
        s.toggle_highlighted(); // highlight is on All row → clear all
        assert!(!s.all_selected_in_edit());
        assert!(!s.is_edit_selected("api"));
        s.toggle_highlighted(); // flip back to all
        assert!(s.all_selected_in_edit());
        assert!(s.is_edit_selected("api"));
    }

    #[test]
    fn all_row_hidden_when_query_is_non_empty() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        assert!(matches!(s.rows().first(), Some(FilterRow::All)));
        s.push_query_char('a');
        // No All row in the filtered results — all entries are names.
        assert!(s.rows().iter().all(|r| matches!(r, FilterRow::Name(_))));
    }

    #[test]
    fn explicit_toggle_commits_even_when_result_is_empty() {
        // Touching the edit state with Space always commits the curated
        // result — including "nothing selected." No auto-select shortcut
        // fires to undo the user's explicit choice.
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        s.toggle_highlighted(); // All → clear everything
        s.commit();
        assert!(s.active_names().is_empty());
        assert!(!s.passes("api"));
        assert!(!s.passes("worker"));
    }

    #[test]
    fn passes_table_for_multi_select_active_filter() {
        struct Case {
            name: &'static str,
            selected: Vec<&'static str>,
            probe: &'static str,
            want: bool,
        }

        let cases = vec![
            Case {
                name: "selected name passes",
                selected: vec!["api"],
                probe: "api",
                want: true,
            },
            Case {
                name: "unselected name gated",
                selected: vec!["api"],
                probe: "worker",
                want: false,
            },
            Case {
                name: "lifecycle (empty name) always passes",
                selected: vec!["api"],
                probe: "",
                want: true,
            },
            Case {
                name: "multi-selected: one of many passes",
                selected: vec!["api", "worker"],
                probe: "worker",
                want: true,
            },
            Case {
                name: "multi-selected: not in set gated",
                selected: vec!["api", "worker"],
                probe: "db",
                want: false,
            },
        ];

        for case in cases {
            let mut s = state(&["api", "worker", "db"]);
            s.enter_edit();
            // Start from a clean edit selection: toggle-all off (highlight
            // is on All row, seeded with everything selected).
            s.toggle_highlighted();
            for selected in &case.selected {
                // Walk rows in order to find the target name. Bounded by
                // rows.len() so a missing name doesn't loop forever.
                for _ in 0..s.rows().len() {
                    match s.rows().get(s.highlight()) {
                        Some(FilterRow::Name(n)) if n == *selected => break,
                        _ => s.highlight_next(),
                    }
                }
                s.toggle_highlighted();
            }
            s.commit();
            assert_eq!(s.passes(case.probe), case.want, "case: {}", case.name);
        }
    }

    #[test]
    fn hidden_from_display_omits_names_from_rows_but_keeps_selection() {
        struct Case {
            name: &'static str,
            hidden: Vec<&'static str>,
            query: &'static str,
            want_rows: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "no hidden names — all rows present",
                hidden: vec![],
                query: "",
                want_rows: vec!["all", "api", "db", "worker"],
            },
            Case {
                name: "hides a single name from the empty-query list",
                hidden: vec!["db"],
                query: "",
                want_rows: vec!["all", "api", "worker"],
            },
            Case {
                name: "fuzzy matches still drop hidden names",
                hidden: vec!["api"],
                query: "a",
                want_rows: vec![],
            },
        ];

        for case in cases {
            let mut s = state(&["api", "worker", "db"]);
            let hidden: HashSet<String> =
                case.hidden.iter().map(|s| s.to_string()).collect();
            s.set_hidden_from_display(hidden);
            for ch in case.query.chars() {
                s.push_query_char(ch);
            }
            let actual: Vec<String> = s
                .rows()
                .iter()
                .map(|r| match r {
                    FilterRow::All => "all".to_string(),
                    FilterRow::Name(n) => n.clone(),
                })
                .collect();
            let want: Vec<String> = case.want_rows.iter().map(|s| s.to_string()).collect();
            assert_eq!(actual, want, "case: {}", case.name);
        }
    }

    #[test]
    fn hidden_from_display_does_not_affect_passes_for_visible_names() {
        let mut s = state(&["api", "worker", "db"]);
        // "db" still passes (it was in the default selection); hidden_from_display
        // only scopes the modal list, not the log-line allowlist. This matters
        // when a Lazy service later transitions and produces output — its
        // selection state must be preserved.
        let hidden: HashSet<String> = ["db"].iter().map(|s| s.to_string()).collect();
        s.set_hidden_from_display(hidden);
        assert!(s.passes("db"));
        assert!(s.passes("api"));
    }
}
