use super::build_tools::{
    BatchBuildItem, BatchBuildOutcome, BatchBuildReplayItem, run_batch_build_chain,
};
use super::graph::{dep_name_map, topological_sort};
use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::{ProcessKind, Runner, RunnerInternalCommand, RuntimeService, ServiceState, TaskState};
use crate::config::Dependency;
use std::collections::HashMap;

fn push_unique_name(names: &mut Vec<String>, name: &str) {
    if !names.iter().any(|existing| existing == name) {
        names.push(name.to_string());
    }
}

fn format_dependency_failure(dependencies: &[String]) -> String {
    match dependencies {
        [dependency] => format!("dependency '{dependency}' failed"),
        dependencies => format!("dependencies '{}' failed", dependencies.join("', '")),
    }
}

fn format_non_blocking_dependencies(dependencies: &[String]) -> String {
    match dependencies {
        [dependency] => format!("dependency '{dependency}'"),
        dependencies => format!("dependencies '{}'", dependencies.join("', '")),
    }
}

impl Runner {
    /// Apply the outcome of the detached batch-build chain: mutate the
    /// runtime state (watch paths, binary paths, `batch_built` flag) and
    /// transition `Building` processes to `Pending` (on success) or `Failed`
    /// (on build failure). The caller is responsible for dropping its
    /// cached batch-build handle. State transitions schedule the normal
    /// pending-process sweep so newly-unblocked processes start.
    pub(in crate::runner) fn apply_batch_build_outcome(&mut self, outcome: BatchBuildOutcome) {
        for warning in &outcome.warnings {
            self.output_manager.error_event(warning);
        }

        for (name, kind, paths) in outcome.resolved_watches {
            match kind {
                ProcessKind::Service => {
                    if let Some(rs) = self.services.get_mut(&name) {
                        rs.resolved_watch_paths = paths;
                    }
                }
                ProcessKind::Task => {
                    if let Some(rt) = self.tasks.get_mut(&name) {
                        rt.resolved_watch_paths = paths;
                    }
                }
            }
        }

        // Binary-path resolution only applies to bazel services — swap in
        // the binary-backed resolved config so subsequent spawns go direct
        // instead of through `bazel run`.
        for (name, path_str) in outcome.binary_paths {
            if let Some(rs) = self.services.get_mut(&name) {
                rs.bazel_binary_path = Some(path_str.clone());
                if let Some(svc) = self.config.services.get(&name) {
                    let mut resolved = svc.resolve_with_bazel_binary(self.platform, &path_str);
                    // Re-expand `depends_on` against the config's service
                    // groups. `resolve_with_bazel_binary` walks back to the
                    // raw user-supplied list (group refs and all) — without
                    // this, a bazel service that lists a group as a dep
                    // ends up with an unexpanded `["mongo-search-deps"]` in
                    // its runtime state, and shutdown's `topological_sort`
                    // bails because the group name isn't a real node.
                    resolved.depends_on = self
                        .config
                        .effective_depends_on(&name, &resolved.depends_on);
                    rs.resolved = resolved.clone();
                    self.configure_supervisor(&name, Some(Box::new(resolved)), None);
                }
            }
        }

        for name in outcome.succeeded {
            let was_building = if let Some(rs) = self.services.get_mut(&name) {
                rs.batch_built = true;
                rs.state() == ServiceState::Building
            } else {
                false
            };
            self.configure_supervisor(&name, None, Some(true));
            if was_building {
                self.set_service_state(&name, ServiceState::Pending);
                continue;
            }
            if self.tasks.contains_key(&name) {
                self.set_task_state(&name, TaskState::Pending);
            }
        }

        for (name, msg) in outcome.failed {
            self.output_manager
                .service_error_event(&name, &format!("batch build failed: {msg}"));
            if self.services.contains_key(&name) {
                self.set_service_state(&name, ServiceState::Failed);
            }
            if self.tasks.contains_key(&name) {
                self.set_task_state(&name, TaskState::Failed);
            }
        }
    }

    fn collect_batch_build_item_by_name(&self, name: &str) -> Option<BatchBuildItem> {
        if let Some(rs) = self.services.get(name) {
            if rs.resolved.is_build_tool_managed() {
                return Some(self.build_batch_item(name, ProcessKind::Service, rs));
            }
            return None;
        }

        let rt = self.tasks.get(name)?;
        rt.config.bazel.as_ref()?;
        let working_dir = working_dir_for(&self.base_dir, rt.config.dir.as_deref());
        let ignore = resolve_watch_ignore_patterns(
            &working_dir,
            &rt.config.ignore,
            &self.base_dir,
            &self.config.watch_ignore,
        );
        Some(BatchBuildItem {
            name: name.to_string(),
            kind: ProcessKind::Task,
            bazel: rt.config.bazel.clone(),
            watch_enabled: rt.config.build_tool_watch_enabled(),
            working_dir,
            ignore,
        })
    }

    pub(in crate::runner) fn spawn_startup_batch_build(&mut self, processes: Vec<BatchBuildItem>) {
        if processes.is_empty() {
            return;
        }

        let cmd_tx = self.internal_tx.clone();
        let base_dir = self.base_dir.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let watch_update_tx = self
            .watch
            .as_ref()
            .map(super::watch_link::WatchHandle::updates);
        let global_watch_ignore = resolve_watch_ignore_patterns(
            &self.base_dir,
            &[],
            &self.base_dir,
            &self.config.watch_ignore,
        );
        let handle = tokio::spawn(async move {
            let outcome = run_batch_build_chain(
                processes,
                base_dir,
                emitter,
                watch_update_tx,
                global_watch_ignore,
            )
            .await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::BatchBuildComplete(outcome))
                .await;
        });
        self.batch_build_handle = Some(crate::build_tool::AbortOnDrop::new(handle));
    }

    pub(in crate::runner) fn spawn_lazy_build(&mut self, name: &str, process: BatchBuildItem) {
        if !self.services.contains_key(name) {
            return;
        }
        // Unique among the *in-flight* builds for this name, which is all the
        // currency check needs: `lazy_build_handles` holds exactly the live
        // one, and each spawn sends exactly one completion.
        let generation = self
            .lazy_build_handles
            .get(name)
            .map_or(0, |(generation, _)| *generation)
            .saturating_add(1);
        let cmd_tx = self.internal_tx.clone();
        let base_dir = self.base_dir.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let watch_update_tx = self
            .watch
            .as_ref()
            .map(super::watch_link::WatchHandle::updates);
        let global_watch_ignore = resolve_watch_ignore_patterns(
            &self.base_dir,
            &[],
            &self.base_dir,
            &self.config.watch_ignore,
        );
        let svc_name = name.to_string();
        let handle = tokio::spawn(async move {
            let outcome = run_batch_build_chain(
                vec![process],
                base_dir,
                emitter,
                watch_update_tx,
                global_watch_ignore,
            )
            .await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::LazyBuildComplete {
                    name: svc_name,
                    generation,
                    outcome,
                })
                .await;
        });
        self.lazy_build_handles.insert(
            name.to_string(),
            (generation, crate::build_tool::AbortOnDrop::new(handle)),
        );
    }

    pub(in crate::runner) fn schedule_startup_batch_replays(
        &mut self,
        replay_items: &[BatchBuildReplayItem],
    ) {
        let mut replay_batch = Vec::new();

        for replay in replay_items {
            let Some(process) = self.collect_batch_build_item_by_name(&replay.name) else {
                continue;
            };
            let message = match (replay.source_changed, replay.graph_changed, replay.kind) {
                (true, true, ProcessKind::Service) => {
                    "files changed during build — rebuilding before start"
                }
                (true, false, ProcessKind::Service) => {
                    "source files changed during build — rebuilding before start"
                }
                (false, true, ProcessKind::Service) => {
                    "build graph changed during build — rebuilding before start"
                }
                (true, true, ProcessKind::Task) => {
                    "files changed during build — re-running build before start"
                }
                (true, false, ProcessKind::Task) => {
                    "source files changed during build — re-running build before start"
                }
                (false, true, ProcessKind::Task) => {
                    "build graph changed during build — re-running build before start"
                }
                (false, false, _) => continue,
            };
            self.output_manager.service_event(&replay.name, message);
            match replay.kind {
                ProcessKind::Service => {
                    self.set_service_state(&replay.name, ServiceState::Building)
                }
                ProcessKind::Task => self.set_task_state(&replay.name, TaskState::Building),
            }
            replay_batch.push(process);
        }

        self.spawn_startup_batch_build(replay_batch);
    }

    pub(in crate::runner) fn schedule_lazy_build_replay(
        &mut self,
        replay: &BatchBuildReplayItem,
    ) -> bool {
        let Some(process) = self.collect_batch_build_item_by_name(&replay.name) else {
            return false;
        };
        let message = match (replay.source_changed, replay.graph_changed) {
            (true, true) => "files changed during build — rebuilding before start",
            (true, false) => "source files changed during build — rebuilding before start",
            (false, true) => "build graph changed during build — rebuilding before start",
            (false, false) => return false,
        };
        self.output_manager.service_event(&replay.name, message);
        self.set_service_state(&replay.name, ServiceState::Building);
        self.spawn_lazy_build(&replay.name, process);
        true
    }

    /// Check if a dependency is satisfied.
    pub(in crate::runner) fn is_dep_satisfied(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return rs.state().is_satisfied();
        }
        if let Some(rt) = self.tasks.get(dep) {
            return rt.dependency_satisfied();
        }
        false
    }

    /// Whether a dependency has stopped making progress: it either failed or
    /// was stopped, and nothing is going to move it to a satisfied state
    /// without another explicit request. Only non-blocking (ordering-only)
    /// edges use this — it is what lets a dependent start anyway.
    pub(in crate::runner) fn is_dep_settled(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return matches!(
                rs.state(),
                ServiceState::Failed | ServiceState::DependencyFailed | ServiceState::Stopped
            );
        }
        if let Some(rt) = self.tasks.get(dep) {
            // `PendingRun`/`Skipped` are settled too: the task is waiting for
            // a manual trigger (or was judged unnecessary) and will not run on
            // its own, so a non-blocking dependent would otherwise wait
            // forever.
            return matches!(
                rt.state(),
                TaskState::Failed
                    | TaskState::DependencyFailed
                    | TaskState::PendingRun
                    | TaskState::Skipped
            );
        }
        false
    }

    /// Whether one `depends_on` edge no longer blocks its dependent.
    ///
    /// A blocking edge opens only when the dependency is satisfied. A
    /// non-blocking edge is ordering-only: it also opens once the dependency
    /// has settled into a failed or stopped state, so the dependent still
    /// starts *after* it, but is not held hostage by it.
    pub(in crate::runner) fn is_dep_gate_open(&self, dep: &Dependency) -> bool {
        if self.is_dep_satisfied(&dep.name) {
            return true;
        }
        !dep.blocking && self.is_dep_settled(&dep.name)
    }

    /// How far this process's dependencies let it go.
    ///
    /// Three-valued because "may I run?" has two different answers depending
    /// on who is asking. The scheduler starts a process only when everything
    /// it needs is actually *up*; a user who names a process explicitly is
    /// willing to proceed past a dependency that has stopped making progress,
    /// because waiting for it would never end.
    ///
    /// Reads only the *dependencies'* states — never this process's own. That
    /// is what keeps the influence graph a DAG (see [`crate::gate`]).
    pub(in crate::runner) fn dep_level(&self, deps: &[Dependency]) -> crate::gate::Gate {
        if deps.iter().all(|dep| self.is_dep_gate_open(dep)) {
            return crate::gate::Gate::Open;
        }
        // Not all satisfied. If every unsatisfied one has *settled*, waiting
        // will not help — an explicit request may still proceed.
        if deps
            .iter()
            .all(|dep| self.is_dep_satisfied(&dep.name) || self.is_dep_settled(&dep.name))
        {
            return crate::gate::Gate::Degraded;
        }
        crate::gate::Gate::Blocked
    }

    /// Announce the non-blocking dependencies this process is not waiting for.
    fn report_skipped_non_blocking_dependencies(&self, name: &str, skipped: &[String]) {
        if skipped.is_empty() {
            return;
        }
        self.output_manager.service_event(
            name,
            &format!(
                "starting without non-blocking {}",
                format_non_blocking_dependencies(skipped)
            ),
        );
    }

    /// Non-blocking dependencies that settled unsuccessfully — reported when
    /// the dependent starts anyway so the log explains why it didn't wait.
    fn skipped_non_blocking_dependencies(&self, dependencies: &[Dependency]) -> Vec<String> {
        dependencies
            .iter()
            .filter(|dep| !dep.blocking && !self.is_dep_satisfied(&dep.name))
            .filter(|dep| self.is_dep_settled(&dep.name))
            .map(|dep| dep.name.clone())
            .collect()
    }

    /// Check if a dependency has failed (including the transitive
    /// `DependencyFailed` cascade — if A fails, B depends on A, C depends
    /// on B, then C also needs to be marked).
    pub(in crate::runner) fn is_dep_failed(&self, dep: &str) -> bool {
        if let Some(rs) = self.services.get(dep) {
            return matches!(
                rs.state(),
                ServiceState::Failed | ServiceState::DependencyFailed
            );
        }
        if let Some(rt) = self.tasks.get(dep) {
            return matches!(rt.state(), TaskState::Failed | TaskState::DependencyFailed);
        }
        false
    }

    /// Mark a start-pending sweep as due. Consumed at the top of the main
    /// loop, so a burst of transitions coalesces into one sweep.
    pub(in crate::runner) fn schedule_gate_recompute(&mut self) {
        self.gate_recompute_scheduled = true;
    }

    /// Resolve failed direct dependencies to their root failures. An
    /// intermediate `DependencyFailed` process contributes the roots it already
    /// recorded, so a chain such as api -> worker -> db reports `db`.
    ///
    /// Non-blocking edges are ignored: their whole point is that a failure on
    /// the other end must not cascade.
    fn failed_dependency_roots(&self, dependencies: &[Dependency]) -> Vec<String> {
        let mut roots = Vec::new();
        for dependency in dependencies.iter().filter(|dep| dep.blocking) {
            let dependency = &dependency.name;
            let inherited = if let Some(rs) = self.services.get(dependency) {
                match rs.state() {
                    ServiceState::Failed => Some(std::slice::from_ref(dependency)),
                    ServiceState::DependencyFailed => Some(rs.failed_dependencies()),
                    _ => None,
                }
            } else if let Some(rt) = self.tasks.get(dependency) {
                match rt.state() {
                    TaskState::Failed => Some(std::slice::from_ref(dependency)),
                    TaskState::DependencyFailed => Some(rt.failed_dependencies()),
                    _ => None,
                }
            } else {
                None
            };

            let Some(inherited) = inherited else {
                continue;
            };
            if inherited.is_empty() {
                push_unique_name(&mut roots, dependency);
                continue;
            }
            for root in inherited {
                push_unique_name(&mut roots, root);
            }
        }
        roots
    }

    /// Refresh dependency-failure causes and return recovered processes to the
    /// pending scheduler. Iterating in topological order lets a root-cause
    /// update flow through every descendant in one sweep.
    fn reconcile_dependency_failures(
        &mut self,
        dep_map: &HashMap<String, Vec<Dependency>>,
        order: &[String],
    ) {
        for name in order {
            let service_state = self.services.get(name).map(RuntimeService::state);
            let task_state = self.tasks.get(name).map(|rt| rt.state());
            let is_pending = service_state == Some(ServiceState::Pending)
                || task_state == Some(TaskState::Pending);
            let is_dependency_failed = service_state == Some(ServiceState::DependencyFailed)
                || task_state == Some(TaskState::DependencyFailed);
            if !is_pending && !is_dependency_failed {
                continue;
            }

            let dependencies = dep_map.get(name).map(Vec::as_slice).unwrap_or_default();
            let failed_dependencies = self.failed_dependency_roots(dependencies);
            if !failed_dependencies.is_empty() {
                let state_changed = if service_state.is_some() {
                    self.mark_service_dependency_failed(name, failed_dependencies.clone())
                } else {
                    self.mark_task_dependency_failed(name, failed_dependencies.clone())
                };
                if state_changed {
                    self.output_manager.service_error_event(
                        name,
                        &format!(
                            "skipped ({})",
                            format_dependency_failure(&failed_dependencies)
                        ),
                    );
                }
                continue;
            }

            if is_dependency_failed {
                if service_state.is_some() {
                    // A lazy service reaches DependencyFailed only after a
                    // connection moved it out of Lazy. Pending preserves that
                    // queued request for the normal scheduler. That scheduler
                    // still waits for every dependency gate to open —
                    // blocking edges on a satisfied dependency, non-blocking
                    // ones on a dependency that has settled either way.
                    self.set_service_state(name, ServiceState::Pending);
                } else {
                    self.set_task_state(name, TaskState::Pending);
                }
                self.output_manager
                    .service_debug_event(name, "dependency recovered; re-queued");
            }
        }
    }

    /// Snapshot of a batch-buildable service or task — everything the
    /// standalone [`run_batch_build_chain`] needs. Taken at startup before
    /// the detached task runs so the task doesn't touch `self`.
    pub(in crate::runner) fn collect_batch_build_items(&self) -> Vec<BatchBuildItem> {
        let mut processes: Vec<BatchBuildItem> = Vec::new();

        for (name, rs) in &self.services {
            if !rs.resolved.is_build_tool_managed() {
                continue;
            }
            // Lazy bazel services defer their query+build+cquery to
            // first connection (JIT in the `lazy_start_rx` handler). Pulling
            // them into the startup batch would query and build services
            // the user may never touch this session.
            if rs.resolved.lazy {
                continue;
            }
            processes.push(self.build_batch_item(name, ProcessKind::Service, rs));
        }
        for (name, rt) in &self.tasks {
            if rt.config.bazel.is_none() {
                continue;
            }
            let working_dir = working_dir_for(&self.base_dir, rt.config.dir.as_deref());
            let ignore = resolve_watch_ignore_patterns(
                &working_dir,
                &rt.config.ignore,
                &self.base_dir,
                &self.config.watch_ignore,
            );
            processes.push(BatchBuildItem {
                name: name.clone(),
                kind: ProcessKind::Task,
                bazel: rt.config.bazel.clone(),
                watch_enabled: rt.config.build_tool_watch_enabled(),
                working_dir,
                ignore,
            });
        }

        processes
    }

    /// Snapshot a single service into a [`BatchBuildItem`] for the JIT
    /// lazy-build path. Shares the field layout with
    /// [`Self::collect_batch_build_items`] so the chain logic doesn't care
    /// whether the build is startup-batched or JIT.
    pub(in crate::runner) fn build_batch_item(
        &self,
        name: &str,
        kind: ProcessKind,
        rs: &RuntimeService,
    ) -> BatchBuildItem {
        let working_dir = working_dir_for(&self.base_dir, rs.resolved.dir.as_deref());
        let ignore = resolve_watch_ignore_patterns(
            &working_dir,
            &rs.resolved.ignore,
            &self.base_dir,
            &self.config.watch_ignore,
        );
        BatchBuildItem {
            name: name.to_string(),
            kind,
            bazel: rs.resolved.bazel_config().cloned(),
            watch_enabled: rs.resolved.build_tool_watch_enabled(),
            working_dir,
            ignore,
        }
    }

    /// Decide which processes may run, and publish that.
    ///
    /// This is the whole of the scheduler: resolve dependency failures to
    /// their roots, work out whose dependencies are satisfied, and set every
    /// gate accordingly. It starts nothing — a supervisor spends its own
    /// permission when it is also idle.
    ///
    /// Initial services begin in `Pending`; lazy services enter `Pending` on
    /// their first proxy connection. Both are permitted by this same pass.
    pub(in crate::runner) async fn publish_start_gates(&mut self) {
        let dep_map = self.build_dep_map();
        let order = match topological_sort(&dep_name_map(&dep_map)) {
            Ok(o) => o,
            Err(_) => return,
        };

        self.reconcile_dependency_failures(&dep_map, &order);

        // One pass, one revision. A supervisor may only act on a level
        // stamped after its demand arose — see [`crate::gate`].
        self.start_gates.begin_pass();

        // Publish a level for *every* process. A gate says only what this
        // process's dependencies allow — never whether anything wants it,
        // which is the supervisor's own business. Keeping this free of the
        // process's own state is what makes the influence graph a DAG.
        for name in &order {
            let deps = dep_map.get(name.as_str()).map(Vec::as_slice).unwrap_or(&[]);
            let mut level = self.dep_level(deps);

            // The build-tool detour: an artifact this process cannot build
            // for itself is as much a precondition as a dependency.
            if level > crate::gate::Gate::Blocked {
                self.start_lazy_build_if_needed(name);
            }
            if !self.artifact_ready(name) {
                level = crate::gate::Gate::Blocked;
            }

            if self.start_gates.set(name, level) && level > crate::gate::Gate::Blocked {
                // Newly permitted: say which non-blocking dependencies we are
                // deliberately not waiting for, so a start that follows a
                // visible failure doesn't look like don ignored the graph.
                let skipped = self.skipped_non_blocking_dependencies(deps);
                self.report_skipped_non_blocking_dependencies(name, &skipped);
            }
        }
    }

    /// Whether this process's build artifact exists.
    ///
    /// Read from build bookkeeping, never from `ServiceState::Building` —
    /// sourcing it from lifecycle state would put `state(X)` back into
    /// `gate(X)` and bring the epoch back with it.
    fn artifact_ready(&self, name: &str) -> bool {
        if self.lazy_build_handles.contains_key(name) {
            return false;
        }
        self.services
            .get(name)
            .is_none_or(|rs| !rs.resolved.is_build_tool_managed() || rs.batch_built)
    }

    /// Whether every process participating in initial startup has settled.
    /// Lazy services are listeners, not startup work, even if a connection
    /// happens to request one while the initial graph is still progressing.
    pub(in crate::runner) fn initial_startup_settled(&self) -> bool {
        let service_work = self.services.values().any(|rs| {
            !rs.resolved.lazy
                && matches!(
                    rs.state(),
                    ServiceState::Pending
                        | ServiceState::Building
                        | ServiceState::Starting
                        | ServiceState::Running
                )
        });
        let task_work = self.tasks.values().any(|rt| {
            matches!(
                rt.state(),
                TaskState::Pending | TaskState::Building | TaskState::Running
            )
        });

        !service_work && !task_work
    }
}
