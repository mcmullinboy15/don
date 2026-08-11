//! Just-in-time activation helpers for lazy services.
//!
//! `Lazy` means no connection has requested the service yet. The first
//! connection transitions it to `Pending`, after which the normal dependency
//! scheduler owns it just like any other service.

use super::{Runner, ServiceState};

impl Runner {
    /// Record the first proxy connection for a lazy service.
    ///
    /// Moving to `Pending` is the request: no parallel name set is needed, and
    /// the normal pending-process scheduler can wait, cascade dependency failure,
    /// and start the service when its dependencies become ready.
    /// A supervisor reports that something now wants it running.
    ///
    /// The supervisor owns the demand itself; this only projects it, so
    /// `don status` can say `Pending` and dependents keep waiting. A process
    /// that already holds a live one has an authoritative state already, and
    /// says nothing here.
    pub(in crate::runner) fn handle_demand(&mut self, name: &str, _demand: super::Demand) {
        // Already holding a live process: its state is authoritative.
        if self.service_runtime(name).is_some() {
            return;
        }
        if self.services.contains_key(name) {
            self.narrate_lazy_demand(name);
            self.set_service_state(name, ServiceState::Pending);
            // Whether an artifact has to be built first is the supervisor's
            // business: it asked for the build the moment it saw this same
            // demand, and reports its progress like any other build.
        } else if self.tasks.contains_key(name) {
            self.set_task_state(name, super::TaskState::Pending);
        }
    }

    /// Say why a first connection is or isn't about to start the service.
    fn narrate_lazy_demand(&mut self, name: &str) {
        let deps = match self.services.get(name) {
            Some(rs) if rs.state() == ServiceState::Lazy => rs.resolved.depends_on.clone(),
            _ => return,
        };

        // Only a blocking dependency's failure strands a lazy service — a
        // non-blocking one lets it start anyway.
        if let Some(failed) = deps
            .iter()
            .filter(|dep| dep.blocking)
            .find(|dep| self.is_dep_failed(&dep.name))
        {
            self.output_manager.service_error_event(
                name,
                &format!("first connection — dependency '{}' has failed", failed.name),
            );
        } else {
            let unsatisfied: Vec<&str> = deps
                .iter()
                .filter(|dep| !self.is_dep_gate_open(dep))
                .map(|dep| dep.name.as_str())
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
    }
}
