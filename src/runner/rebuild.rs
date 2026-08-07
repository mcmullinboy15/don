use super::build_batcher::{BatchRequest, RebuildSpec};
use super::build_tools::{
    BazelRebuildItem, GraphRequeryOutcomeItem, GraphRequeryRequestItem, RebuildBatchOutcome,
    send_watch_update,
};
use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::{Runner, RunnerEvent, ServiceState, should_rebuild_after_graph_requery};

impl Runner {
    /// Capture what the batcher needs to rebuild `name`, from resolved
    /// config. Resolved config is fixed after construction, so a spec built
    /// at queue time equals one built at flush time — which is what frees
    /// the batcher from reading runner state.
    fn rebuild_spec_for(&self, name: &str) -> Option<RebuildSpec> {
        let rs = self.services.get(name)?;
        Some(match &rs.resolved.kind {
            Some(crate::config::ServiceKind::Bazel(bazel)) => {
                RebuildSpec::Bazel(BazelRebuildItem {
                    name: name.to_string(),
                    target: bazel.target.clone(),
                    working_dir: working_dir_for(&self.base_dir, rs.resolved.dir.as_deref()),
                })
            }
            _ => RebuildSpec::Plain {
                name: name.to_string(),
            },
        })
    }

    /// Queue `name` on the batcher's next rebuild batch.
    pub(in crate::runner) fn queue_build_tool_rebuild(&self, name: &str) {
        if let Some(spec) = self.rebuild_spec_for(name) {
            let _ = self.batcher_tx.send(BatchRequest::QueueRebuild { spec });
        }
    }

    /// Re-queue an item whose batch outcome arrived while it is still coming
    /// up. The batcher defers such items at flush time from the state
    /// snapshot, but the snapshot can trail the fold — this is the
    /// application-side safety net. Returns whether the item was re-queued.
    fn requeue_if_coming_up(&mut self, name: &str) -> bool {
        let coming_up = self.services.get(name).is_some_and(|rs| {
            matches!(
                rs.state(),
                ServiceState::Building | ServiceState::Pending | ServiceState::Starting
            )
        });
        if coming_up {
            self.queue_build_tool_rebuild(name);
        }
        coming_up
    }

    pub(in crate::runner) fn fail_rebuild(&self, name: &str, message: &str) {
        self.output_manager.service_error_event(name, message);
        let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
            name: name.to_string(),
            success: false,
        });
    }

    pub(in crate::runner) fn mark_rebuild_stale(&mut self, name: &str) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.rebuild_stale = true;
        }
    }

    pub(in crate::runner) fn clear_rebuild_stale(&mut self, name: &str) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.rebuild_stale = false;
        }
    }

    pub(in crate::runner) fn take_rebuild_stale(&mut self, name: &str) -> bool {
        self.services.get_mut(name).is_some_and(|rs| {
            let stale = rs.rebuild_stale;
            rs.rebuild_stale = false;
            stale
        })
    }

    /// Read and clear the "running process is behind the latest build" flag.
    /// See [`crate::runner::state::RuntimeService::artifact_ahead_of_process`].
    pub(in crate::runner) fn take_artifact_ahead_of_process(&mut self, name: &str) -> bool {
        self.services.get_mut(name).is_some_and(|rs| {
            let ahead = rs.artifact_ahead_of_process;
            rs.artifact_ahead_of_process = false;
            ahead
        })
    }

    pub(in crate::runner) async fn handle_rebuild_batch_complete(
        &mut self,
        outcome: RebuildBatchOutcome,
    ) {
        for (name, message) in &outcome.failed {
            self.fail_rebuild(name, message);
        }
        for name in &outcome.up_to_date {
            if self.requeue_if_coming_up(name) {
                continue;
            }
            // Normally up-to-date means the running process already has the
            // current artifact, so there's nothing to do. But if an earlier
            // stale build deferred this service's restart, the process is still
            // behind the last successful build — restart into it now rather
            // than no-op (up-to-date is measured against the last build, not
            // the running process).
            if self.take_artifact_ahead_of_process(name) {
                self.output_manager.service_debug_event(
                    name,
                    "up to date, but process is behind last build — restarting",
                );
                self.do_rebuild(name).await;
                continue;
            }
            self.output_manager
                .service_debug_event(name, "skipped (no changes)");
            let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                name: name.clone(),
                success: true,
            });
        }
        for name in &outcome.build_succeeded {
            if let Some(rs) = self.services.get_mut(name) {
                rs.batch_built = true;
            }
            if self.requeue_if_coming_up(name) {
                continue;
            }
            if self.take_rebuild_stale(name) {
                // A watched file changed mid-build. Skip restarting into the
                // artifact we just built and let the follow-up cycle pick up
                // the newer change — but record that the running process is now
                // behind a successful build, so the follow-up restarts even if
                // the build tool then reports up-to-date.
                if let Some(rs) = self.services.get_mut(name) {
                    rs.artifact_ahead_of_process = true;
                }
                let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                    name: name.clone(),
                    success: true,
                });
                continue;
            }
            self.do_rebuild(name).await;
        }
        for name in &outcome.plain_rebuilds {
            if self.requeue_if_coming_up(name) {
                continue;
            }
            if self.take_rebuild_stale(name) {
                let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                    name: name.clone(),
                    success: true,
                });
                continue;
            }
            self.do_rebuild(name).await;
        }
    }

    /// Run a forced (skip up-to-date checks) rebuild for one item now. The
    /// batcher answers synchronously whether it accepted — a batch already
    /// in flight is the same "already in progress" error as before.
    pub(in crate::runner) async fn spawn_forced_build_tool_rebuild(
        &mut self,
        name: &str,
    ) -> Result<(), super::CommandError> {
        let spec =
            self.rebuild_spec_for(name)
                .ok_or_else(|| super::CommandError::UnknownService {
                    name: name.to_string(),
                })?;
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let sent = self.batcher_tx.send(BatchRequest::ForceRebuild {
            spec,
            reply: reply_tx,
        });
        if sent.is_err() {
            return Err(super::CommandError::Failed {
                name: name.to_string(),
                message: "build batcher is shutting down".to_string(),
            });
        }
        // Safe to await inline: the batcher never blocks (its outcome
        // channel is unbounded), so the reply is immediate.
        match reply_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(super::CommandError::InvalidState {
                name: name.to_string(),
                message,
            }),
            Err(_) => Err(super::CommandError::Failed {
                name: name.to_string(),
                message: "build batcher is shutting down".to_string(),
            }),
        }
    }

    pub(in crate::runner) async fn handle_graph_requery_complete(
        &mut self,
        outcomes: Vec<GraphRequeryOutcomeItem>,
    ) {
        let watch_update_tx = self
            .watch
            .as_ref()
            .map(super::watch_link::WatchHandle::updates);
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
                    if let Some(rs) = self.services.get_mut(&outcome.name) {
                        rs.resolved_watch_paths = info.watch_paths.clone();
                    } else if let Some(rt) = self.tasks.get_mut(&outcome.name) {
                        rt.resolved_watch_paths = info.watch_paths.clone();
                    }
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
            self.queue_build_tool_rebuild(&name);
        }

        for name in tasks_to_rerun {
            self.output_manager
                .service_event(&name, "build graph changed — re-running");
            self.handle_task_rerun(&name).await;
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

    /// Runs the build (if any), stops the old process, starts a new one.
    /// If the build fails, the old process is kept running.
    /// Broadcasts `RebuildComplete` when done.
    ///
    /// For proxy services: clears the proxy backend (new connections queue),
    /// allocates fresh ephemeral ports, starts the new instance, and sets the
    /// backend once the ready check passes. The proxy never drops — clients
    /// see a brief pause, not a connection refused.
    pub(in crate::runner) async fn handle_rebuild(&mut self, name: &str) {
        self.clear_rebuild_stale(name);
        let rs = match self.services.get(name) {
            Some(rs) => rs,
            None => {
                self.fail_rebuild(name, "rebuild requested for unknown service");
                return;
            }
        };

        // For build-tool-managed services, queue the rebuild into a batch.
        // Multiple services sharing the same source files will be batched into
        // one `bazel build //a //b //c` invocation instead of separate builds.
        //
        // A service that is still mid-build (`Building`, e.g. the initial or
        // first-connection bazel build) is queued too rather than dropped —
        // `flush_pending_rebuilds` holds it until the service has come up, so a
        // file edited during the build still triggers a rebuild instead of
        // being silently lost.
        if rs.resolved.is_build_tool_managed() {
            // Queueing extends the batch window, so several Rebuild commands
            // from the watch module — which fire per-service after their own
            // debounce timers — coalesce into one build.
            self.queue_build_tool_rebuild(name);
            return;
        }

        self.do_rebuild(name).await;
    }

    /// Execute a rebuild for a single service: build, stop old, restart.
    ///
    /// This is the core rebuild logic, called either directly (non-build-tool
    /// services) or after a batch build completes (build-tool services).
    async fn do_rebuild(&mut self, name: &str) {
        // We're committing to a restart, so the process will be brought up to
        // the current artifact — clear the "behind the latest build" flag.
        if let Some(rs) = self.services.get_mut(name) {
            rs.artifact_ahead_of_process = false;
        }
        let resolved = match self.services.get(name) {
            Some(rs) => rs.resolved.clone(),
            None => {
                self.fail_rebuild(name, "rebuild requested for unknown service");
                return;
            }
        };
        // For build-tool-managed services the batch build has already run by
        // the time we reach `do_rebuild`, and the actual restart is surfaced
        // later by `queue_rebuild_service_start`'s "restarting..." event.
        // Emitting another pre-stop "restarting" here just creates log noise.
        //
        // For other kinds, the detached rebuild worker will kick off the
        // build after this lifecycle event, so "rebuilding" still lands
        // before the build output.
        let message = if resolved.is_build_tool_managed() {
            None
        } else {
            Some("rebuilding (file changed)")
        };
        if let Some(message) = message {
            self.output_manager.service_event(name, message);
        }
        if resolved.is_build_tool_managed() {
            self.continue_rebuild_restart(name).await;
            return;
        }
        if let Err(e) = self.spawn_service_rebuild_worker(name, resolved) {
            self.fail_rebuild(name, &e.to_string());
        }
    }
}
