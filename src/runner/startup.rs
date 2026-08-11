use super::graph::{dep_name_map, topological_sort};
use super::{ProcessKind, Runner, RuntimeService, ServiceState, TaskState};
use crate::config::Dependency;
use crate::process::ArtifactBuildStatus;
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
    /// Fold a supervisor's report about its own artifact build.
    ///
    /// The scheduler neither starts nor waits on the build: it records the
    /// `Building` state so `don status` can show it, so
    /// [`Self::initial_startup_settled`] stays open while one runs, and so a
    /// rebuild requested mid-build is deferred rather than raced.
    pub(in crate::runner) fn handle_artifact_build(
        &mut self,
        name: &str,
        kind: ProcessKind,
        status: ArtifactBuildStatus,
    ) {
        match status {
            ArtifactBuildStatus::Started => match kind {
                ProcessKind::Service => self.set_service_state(name, ServiceState::Building),
                ProcessKind::Task => self.set_task_state(name, TaskState::Building),
            },
            // Only a process still waiting on this build returns to the
            // scheduler. Anything else — stopped, restarted, failed since —
            // has moved on, and the artifact is simply there when it needs it.
            ArtifactBuildStatus::Ready => match kind {
                ProcessKind::Service => {
                    if self.services.get(name).map(RuntimeService::state)
                        == Some(ServiceState::Building)
                    {
                        self.set_service_state(name, ServiceState::Pending);
                    }
                }
                ProcessKind::Task => {
                    if self.tasks.get(name).map(|rt| rt.state()) == Some(TaskState::Building) {
                        self.set_task_state(name, TaskState::Pending);
                    }
                }
            },
            ArtifactBuildStatus::Failed(message) => {
                self.output_manager
                    .service_error_event(name, &format!("build failed: {message}"));
                match kind {
                    ProcessKind::Service => self.set_service_state(name, ServiceState::Failed),
                    ProcessKind::Task => self.set_task_state(name, TaskState::Failed),
                }
            }
        }
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
        // process's dependencies allow — never whether anything wants it, and
        // never whether its artifact exists. Both are the supervisor's own
        // business, and keeping this free of the process's own state is what
        // makes the influence graph a DAG.
        for name in &order {
            let deps = dep_map.get(name.as_str()).map(Vec::as_slice).unwrap_or(&[]);
            let level = self.dep_level(deps);

            if self.start_gates.set(name, level) && level > crate::gate::Gate::Blocked {
                // Newly permitted: say which non-blocking dependencies we are
                // deliberately not waiting for, so a start that follows a
                // visible failure doesn't look like don ignored the graph.
                let skipped = self.skipped_non_blocking_dependencies(deps);
                self.report_skipped_non_blocking_dependencies(name, &skipped);
            }
        }
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
