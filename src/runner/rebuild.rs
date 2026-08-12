//! Applying build-graph re-queries.
//!
//! The rebuild *cycle* — build, stop, spawn, and the staleness that decides
//! whether the spawn happens — belongs to each service's supervisor, which
//! owns all three steps. What stays here is the cross-process half: a re-query
//! rewrites watch registrations and can fan one BUILD-file change out to
//! several processes, which is a scheduling decision.

use super::build_tools::{GraphRequeryOutcomeItem, GraphRequeryRequestItem, send_watch_update};
use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::{Runner, should_rebuild_after_graph_requery};

impl Runner {
    pub(in crate::runner) async fn handle_graph_requery_complete(
        &mut self,
        outcomes: Vec<GraphRequeryOutcomeItem>,
    ) {
        let watch_update_tx = self.watch.as_ref().map(crate::watch::WatchHandle::updates);
        let mut services_to_rebuild: Vec<String> = Vec::new();
        let mut tasks_to_rerun: Vec<String> = Vec::new();
        let global_watch_ignore = resolve_watch_ignore_patterns(
            &self.base_dir,
            &[],
            &self.base_dir,
            &self.config.watch_ignore,
        );

        for outcome in outcomes {
            match outcome.result {
                Ok(info) => {
                    let count = info.watch_paths.len();
                    self.output_manager.service_event(
                        &outcome.name,
                        &format!(
                            "updated watch paths ({count} path{})",
                            if count == 1 { "" } else { "s" }
                        ),
                    );
                    if outcome.watch_enabled
                        && let Some(ref tx) = watch_update_tx
                    {
                        let kind = if self.services.contains_key(&outcome.name) {
                            crate::watch::WatchItemKind::Service
                        } else {
                            crate::watch::WatchItemKind::Task
                        };
                        send_watch_update(
                            tx,
                            outcome.name.clone(),
                            kind,
                            info.watch_paths.clone(),
                            outcome.ignore_patterns,
                            self.base_dir.clone(),
                        )
                        .await;
                        send_watch_update(
                            tx,
                            format!("{}__graph", outcome.name),
                            crate::watch::WatchItemKind::BuildGraph,
                            info.graph_definition_globs,
                            global_watch_ignore.clone(),
                            self.base_dir.clone(),
                        )
                        .await;
                    }

                    if outcome.watch_enabled
                        && let Some(rs) = self.services.get(&outcome.name)
                    {
                        if should_rebuild_after_graph_requery(rs)
                            && !services_to_rebuild.contains(&outcome.name)
                        {
                            services_to_rebuild.push(outcome.name.clone());
                        }
                    } else if outcome.watch_enabled
                        && self.tasks.contains_key(&outcome.name)
                        && !tasks_to_rerun.contains(&outcome.name)
                    {
                        tasks_to_rerun.push(outcome.name.clone());
                    }
                }
                Err(e) => {
                    self.output_manager.service_error_event(
                        &outcome.name,
                        &format!(
                            "build tool re-query failed: {e} — keeping existing watch patterns"
                        ),
                    );
                }
            }
        }
        for name in services_to_rebuild {
            self.output_manager
                .service_event(&name, "build graph changed — rebuilding");
            self.send_rebuild(&name, false, None);
        }

        for name in tasks_to_rerun {
            self.output_manager
                .service_event(&name, "build graph changed — re-running");
            self.send_task_rerun(&name);
        }
    }

    /// Handle a build graph change event (BUILD files, package.json, etc. changed).
    ///
    /// Queues the item for a batched re-query instead of spawning immediately.
    /// This prevents redundant concurrent queries when a single BUILD file
    /// change affects multiple services.
    /// Per-item build-graph re-query specs, precomputed from resolved
    /// config for `watch_link`'s direct-to-batcher routing. Config is fixed
    /// after construction, so the catalog cannot go stale.
    pub(in crate::runner) fn requery_catalog(
        &self,
    ) -> std::collections::HashMap<String, GraphRequeryRequestItem> {
        let mut catalog = std::collections::HashMap::new();
        for name in self.services.keys().chain(self.tasks.keys()) {
            let (bazel, watch_enabled, item_dir, ignore_patterns) =
                if let Some(rs) = self.services.get(name) {
                    if !rs.resolved.build_tool_watch_enabled() {
                        continue;
                    }
                    (
                        rs.resolved.bazel_config().cloned(),
                        rs.resolved.build_tool_watch_enabled(),
                        rs.resolved.dir.clone(),
                        rs.resolved.ignore.clone(),
                    )
                } else if let Some(rt) = self.tasks.get(name) {
                    if !rt.config.build_tool_watch_enabled() {
                        continue;
                    }
                    (
                        rt.config.bazel.clone(),
                        rt.config.build_tool_watch_enabled(),
                        rt.config.dir.clone(),
                        rt.config.ignore.clone(),
                    )
                } else {
                    continue;
                };
            if bazel.is_none() {
                continue;
            }
            let working_dir = working_dir_for(&self.base_dir, item_dir.as_deref());
            let ignore_patterns = resolve_watch_ignore_patterns(
                &working_dir,
                &ignore_patterns,
                &self.base_dir,
                &self.config.watch_ignore,
            );
            catalog.insert(
                name.clone(),
                GraphRequeryRequestItem {
                    name: name.clone(),
                    bazel,
                    watch_enabled,
                    working_dir,
                    ignore_patterns,
                },
            );
        }
        catalog
    }
}
