//! Just-in-time activation helpers for lazy services.
//!
//! `Lazy` means no connection has requested the service yet. The first
//! connection transitions it to `Pending`, after which the normal dependency
//! scheduler owns it just like any other service.

use super::build_tools::BatchBuildOutcome;
use super::{NodeKind, Runner, ServiceState};

impl Runner {
    /// Record the first proxy connection for a lazy service.
    ///
    /// Moving to `Pending` is the request: no parallel name set is needed, and
    /// the normal pending-item scheduler can wait, cascade dependency failure,
    /// and start the service when its dependencies become ready.
    pub(in crate::runner) fn handle_lazy_connection(&mut self, name: &str) {
        let deps = match self.services.get(name) {
            Some(rs) if rs.state() == ServiceState::Lazy => rs.resolved.depends_on.clone(),
            _ => return,
        };

        if let Some(failed) = deps.iter().find(|dep| self.is_dep_failed(dep)) {
            self.output_manager.service_error_event(
                name,
                &format!("first connection — dependency '{failed}' has failed"),
            );
        } else {
            let unsatisfied: Vec<&str> = deps
                .iter()
                .filter(|dep| !self.is_dep_satisfied(dep))
                .map(String::as_str)
                .collect();
            if unsatisfied.is_empty() {
                self.output_manager
                    .service_event(name, "first connection — dependencies satisfied");
            } else {
                self.output_manager.service_event(
                    name,
                    &format!(
                        "waiting for dependencies before start: {}",
                        unsatisfied.join(", ")
                    ),
                );
            }
        }

        self.set_service_state(name, ServiceState::Pending);
    }

    /// Start the detached build-tool chain for a triggered lazy service when
    /// it has not been batch-built yet. Returns whether a build was started.
    pub(in crate::runner) fn start_lazy_build_if_needed(&mut self, name: &str) -> bool {
        let needs_jit = self.services.get(name).is_some_and(|rs| {
            rs.state() == ServiceState::Pending
                && rs.resolved.lazy
                && rs.resolved.is_build_tool_managed()
                && !rs.batch_built
        });
        if !needs_jit {
            return false;
        }

        let item = match self.services.get(name) {
            Some(rs) => self.build_batch_item(name, NodeKind::Service, rs),
            None => return false,
        };
        self.output_manager
            .service_event(name, "dependencies ready — building before start");
        self.set_service_state(name, ServiceState::Building);
        self.spawn_lazy_build(name, item);
        true
    }

    /// Apply a lazy JIT build result and return successful builds to Pending.
    /// The normal scheduler then re-checks every dependency before starting.
    pub(in crate::runner) fn handle_lazy_build_complete(
        &mut self,
        name: &str,
        outcome: BatchBuildOutcome,
    ) {
        self.lazy_build_handles.remove(name);
        let replay_items = outcome.replay_items.clone();
        self.apply_batch_build_outcome(outcome);
        if let Some(item) = replay_items.iter().find(|item| item.name == name) {
            self.schedule_lazy_build_replay(item);
        }
    }
}
