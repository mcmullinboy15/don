//! Log filter — multi-select set of service/task names.
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
//! they carry the `"don"` name and are gated like any other entry, so a
//! narrow filter hides them unless the user explicitly selects `don`.
//!
//! `all_names` is the source of truth — the union of service and task names
//! the runner knows about at TUI startup, plus the synthetic `"don"` entry
//! for lifecycle events. It doesn't update on live reload; a config reload
//! that renames services will require Esc'ing the filter.

use std::collections::HashSet;

use super::fuzzy::fuzzy_match;

/// Maximum number of match rows to show in the dropdown. Cap keeps the
/// viewport reasonably small on machines with many services.
pub(crate) const MAX_DROPDOWN_ROWS: usize = 8;

/// Filter state, persistent across mode switches.
#[derive(Debug, Clone)]
pub(crate) struct FilterState {
    all_names: Vec<String>,
    /// Query buffer visible only in filter-edit mode.
    query: String,
    /// Fuzzy-matched names for the current query, ordered best-first.
    /// In empty-query mode this equals `all_names`.
    matches: Vec<String>,
    /// Highlighted index within `matches` — what Up/Down move.
    highlight: usize,
    /// Work-in-progress selection, seeded from `active_selected` on entering
    /// edit mode. Space toggles membership here.
    editing_selected: HashSet<String>,
    /// Committed selection. If empty, there is no active filter and all
    /// lines pass.
    active_selected: HashSet<String>,
}

impl FilterState {
    /// Initialize with the complete set of filterable names (services + tasks).
    pub(crate) fn new(mut all_names: Vec<String>) -> Self {
        all_names.sort();
        all_names.dedup();
        let matches = all_names.clone();
        Self {
            all_names,
            query: String::new(),
            matches,
            highlight: 0,
            editing_selected: HashSet::new(),
            active_selected: HashSet::new(),
        }
    }

    /// True if a filter is currently applied to incoming log lines.
    pub(crate) fn is_active(&self) -> bool {
        !self.active_selected.is_empty()
    }

    /// True if the given log line name passes the currently-active filter.
    /// Blank spacer lines (empty name) always pass; everything else — including
    /// `[don]` lifecycle events — must be in the active selection.
    pub(crate) fn passes(&self, name: &str) -> bool {
        if !self.is_active() {
            return true;
        }
        if name.is_empty() {
            return true;
        }
        self.active_selected.contains(name)
    }

    /// Start editing. Seeds the edit selection from the currently active filter
    /// so users can refine an existing filter rather than rebuild it.
    pub(crate) fn enter_edit(&mut self) {
        self.query.clear();
        self.matches = self.all_names.clone();
        self.highlight = 0;
        self.editing_selected = self.active_selected.clone();
    }

    /// Discard edits and return to the previously active filter.
    pub(crate) fn cancel_edit(&mut self) {
        self.editing_selected.clear();
        self.query.clear();
        self.highlight = 0;
        self.matches = self.all_names.clone();
    }

    /// Commit the current edit selection as the active filter. If the user
    /// didn't toggle anything with Space, auto-select the highlighted match
    /// so `type → Enter` works as a single-select shortcut.
    pub(crate) fn commit(&mut self) {
        if self.editing_selected.is_empty()
            && let Some(name) = self.matches.get(self.highlight)
        {
            self.editing_selected.insert(name.clone());
        }
        self.active_selected = std::mem::take(&mut self.editing_selected);
        self.query.clear();
        self.highlight = 0;
        self.matches = self.all_names.clone();
    }

    /// Drop the active filter. Also clears any in-flight edit state.
    pub(crate) fn clear_active(&mut self) {
        self.active_selected.clear();
        self.editing_selected.clear();
        self.query.clear();
        self.highlight = 0;
        self.matches = self.all_names.clone();
    }

    /// Append a character to the query and recompute matches.
    pub(crate) fn push_query_char(&mut self, c: char) {
        self.query.push(c);
        self.recompute_matches();
    }

    /// Remove the last character of the query (noop if empty).
    pub(crate) fn pop_query_char(&mut self) {
        if self.query.pop().is_some() {
            self.recompute_matches();
        }
    }

    /// Move highlight up with wrap-around.
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

    /// Move highlight down with wrap-around.
    pub(crate) fn highlight_next(&mut self) {
        if self.matches.is_empty() {
            return;
        }
        self.highlight = (self.highlight + 1) % self.matches.len();
    }

    /// Toggle the highlighted match in the edit selection.
    pub(crate) fn toggle_highlighted(&mut self) {
        if let Some(name) = self.matches.get(self.highlight) {
            if self.editing_selected.contains(name) {
                self.editing_selected.remove(name);
            } else {
                self.editing_selected.insert(name.clone());
            }
        }
    }

    /// Current query string (for bar rendering).
    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    /// Currently visible match rows (for dropdown rendering).
    pub(crate) fn matches(&self) -> &[String] {
        &self.matches
    }

    /// Currently highlighted match index.
    pub(crate) fn highlight(&self) -> usize {
        self.highlight
    }

    /// Whether a given name is in the edit-mode selection.
    pub(crate) fn is_edit_selected(&self, name: &str) -> bool {
        self.editing_selected.contains(name)
    }

    /// Sorted list of names in the active (committed) selection, for bar
    /// rendering in Normal mode.
    pub(crate) fn active_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.active_selected.iter().map(String::as_str).collect();
        names.sort();
        names
    }

    fn recompute_matches(&mut self) {
        self.matches = fuzzy_match(&self.query, &self.all_names);
        if self.highlight >= self.matches.len() {
            self.highlight = 0;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn state(names: &[&str]) -> FilterState {
        FilterState::new(names.iter().map(|s| s.to_string()).collect())
    }

    #[test]
    fn empty_filter_passes_everything() {
        let s = state(&["api", "worker"]);
        assert!(s.passes("api"));
        assert!(s.passes("worker"));
        assert!(s.passes(""));
        assert!(s.passes("unknown"));
        assert!(!s.is_active());
    }

    #[test]
    fn active_filter_gates_non_matching_but_passes_spacers() {
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
    fn active_filter_gates_don_lifecycle_unless_selected() {
        // `"don"` is just another filter entry now — not a special-cased
        // always-pass. Covers the "hide don's logs unless I explicitly
        // select them" behavior.
        let mut s = state(&["api", "don"]);
        s.enter_edit();
        s.push_query_char('a'); // matches "api" but not "don"
        s.commit();
        assert!(s.passes("api"));
        assert!(!s.passes("don"), "don gated when not in selection");

        s.enter_edit();
        // Walk to "don" and toggle it in.
        while s.matches()[s.highlight()].as_str() != "don" {
            s.highlight_next();
        }
        s.toggle_highlighted();
        s.commit();
        assert!(s.passes("don"), "don passes once explicitly selected");
    }

    #[test]
    fn space_multiselect_then_enter_commits_selection() {
        let mut s = state(&["api", "worker", "db", "cache"]);
        s.enter_edit();
        // Highlight moves; space toggles two items into selection.
        s.highlight_next(); // "db" if sorted: cache, api, db, worker — actually sort sorts alpha
        // Sorted: api, cache, db, worker; highlight was 0 → "api"; next → "cache"
        s.toggle_highlighted(); // cache
        s.highlight_next(); // db
        s.toggle_highlighted(); // db
        s.commit();
        let mut active = s.active_names();
        active.sort();
        assert_eq!(active, vec!["cache", "db"]);
        assert!(s.passes("cache"));
        assert!(s.passes("db"));
        assert!(!s.passes("api"));
    }

    #[test]
    fn single_match_plus_enter_auto_selects_highlight() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        s.push_query_char('a'); // fuzzy matches "api" (and "worker" via 'r'? no — "worker" doesn't have 'a')
        s.commit();
        assert_eq!(s.active_names(), vec!["api"]);
    }

    #[test]
    fn cancel_edit_preserves_active_filter() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        s.push_query_char('a');
        s.commit();
        assert!(s.passes("api"));

        s.enter_edit();
        s.push_query_char('w');
        s.cancel_edit();
        // Active filter unchanged — still just "api".
        assert_eq!(s.active_names(), vec!["api"]);
        assert!(s.passes("api"));
        assert!(!s.passes("worker"));
    }

    #[test]
    fn clear_active_restores_unfiltered() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        s.push_query_char('a');
        s.commit();
        s.clear_active();
        assert!(!s.is_active());
        assert!(s.passes("worker"));
    }

    #[test]
    fn highlight_wraps_on_both_ends() {
        let mut s = state(&["a", "b", "c"]);
        s.enter_edit();
        assert_eq!(s.highlight(), 0);
        s.highlight_prev();
        assert_eq!(s.highlight(), 2, "up from top wraps to bottom");
        s.highlight_next();
        assert_eq!(s.highlight(), 0, "down from bottom wraps to top");
    }

    #[test]
    fn toggle_then_untoggle_before_commit_removes_from_selection() {
        let mut s = state(&["api", "worker"]);
        s.enter_edit();
        s.toggle_highlighted(); // api
        s.toggle_highlighted(); // un-toggle api
        s.commit();
        // nothing was selected + nothing highlighted-auto-selected? Actually after
        // the untoggle, editing_selected is empty, so commit auto-selects highlight
        // which is "api". That's expected behavior — single-select shortcut.
        assert_eq!(s.active_names(), vec!["api"]);
    }

    #[test]
    fn pop_query_char_updates_matches() {
        let mut s = state(&["api", "worker", "db"]);
        s.enter_edit();
        s.push_query_char('a');
        assert_eq!(s.matches(), &["api".to_string()]);
        s.pop_query_char();
        // Empty query — all names, sorted.
        assert_eq!(s.matches(), &["api", "db", "worker"]);
    }

    #[test]
    fn highlight_clamps_when_matches_shrink_under_it() {
        let mut s = state(&["api", "db", "worker"]);
        s.enter_edit();
        // Sorted: api, db, worker. Highlight last.
        s.highlight_next();
        s.highlight_next();
        assert_eq!(s.highlight(), 2);
        // Typing 'a' narrows matches to just "api" — highlight must clamp
        // back to 0, otherwise `matches[2]` would be out of bounds for the
        // renderer.
        s.push_query_char('a');
        assert_eq!(s.matches().len(), 1);
        assert_eq!(s.highlight(), 0);
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
            for selected in &case.selected {
                // Walk the matches to find and toggle this name. This mimics
                // user-input flow rather than reaching into private state.
                while s.matches()[s.highlight()].as_str() != *selected {
                    s.highlight_next();
                }
                s.toggle_highlighted();
            }
            s.commit();
            assert_eq!(s.passes(case.probe), case.want, "case: {}", case.name);
        }
    }
}
