//! Just-in-time start orchestration for lazy services: a first proxy
//! connection triggers a start, but only once `depends_on` is satisfied.

use super::events::ItemDone;
use super::service_worker::ServiceStartMode;
use super::{NodeKind, Runner, ServiceState};
use std::collections::HashSet;
use tokio::sync::mpsc;

/// Why a lazy service is being started — drives lifecycle event wording.
#[derive(Debug, Clone, Copy)]
pub(in crate::runner) enum LazyStartReason {
    /// A first connection arrived with dependencies already satisfied.
    FirstConnection,
    /// An earlier connection was deferred; dependencies are now satisfied.
    DependenciesReady,
}

impl LazyStartReason {
    fn prefix(self) -> &'static str {
        match self {
            LazyStartReason::FirstConnection => "first connection",
            LazyStartReason::DependenciesReady => "dependencies ready",
        }
    }
}

impl Runner {
    /// React to a lazy service's first proxy connection (caller confirmed
    /// `Lazy`): start now, defer, or surface `DependencyFailed`.
    pub(in crate::runner) fn handle_lazy_connection(
        &mut self,
        name: &str,
        pending: &mut HashSet<String>,
        done_tx: &mpsc::Sender<ItemDone>,
    ) {
        let deps = match self.services.get(name) {
            Some(rs) => rs.resolved.depends_on.clone(),
            None => return,
        };

        // Don't build/start against a broken prerequisite; surface it instead.
        if let Some(failed) = deps.iter().find(|dep| self.is_dep_failed(dep)) {
            let failed = failed.clone();
            self.lazy_start_requested.remove(name);
            pending.remove(name);
            self.set_service_state(name, ServiceState::DependencyFailed);
            self.output_manager.service_error_event(
                name,
                &format!("cannot start — dependency '{failed}' failed"),
            );
            return;
        }

        let unsatisfied: Vec<String> = deps
            .iter()
            .filter(|dep| !self.is_dep_satisfied(dep))
            .cloned()
            .collect();

        if unsatisfied.is_empty() {
            self.lazy_start_requested.remove(name);
            self.start_lazy_service(name, done_tx.clone(), LazyStartReason::FirstConnection);
            return;
        }

        // Defer: stay `Lazy` and keep in `pending` so `start_ready_items`
        // re-fires the recorded request once every dependency is satisfied.
        pending.insert(name.to_string());
        if self.lazy_start_requested.insert(name.to_string()) {
            self.output_manager.service_event(
                name,
                &format!(
                    "waiting for dependencies before start: {}",
                    unsatisfied.join(", ")
                ),
            );
        }
    }

    /// Kick a lazy service's JIT build (build-tool-managed, not yet built) or
    /// its direct start. Caller must have confirmed deps are satisfied.
    pub(in crate::runner) fn start_lazy_service(
        &mut self,
        name: &str,
        done_tx: mpsc::Sender<ItemDone>,
        reason: LazyStartReason,
    ) {
        if !self
            .services
            .get(name)
            .is_some_and(|rs| rs.state() == ServiceState::Lazy)
        {
            return;
        }

        let prefix = reason.prefix();
        let needs_jit = self
            .services
            .get(name)
            .is_some_and(|rs| rs.resolved.is_build_tool_managed() && !rs.batch_built);

        if needs_jit {
            let item = match self.services.get(name) {
                Some(rs) => self.build_batch_item(name, NodeKind::Service, rs),
                None => return,
            };
            self.output_manager
                .service_event(name, &format!("{prefix} — building before start"));
            self.set_service_state(name, ServiceState::Building);
            self.spawn_lazy_build(name, item);
        } else {
            self.output_manager
                .service_event(name, &format!("{prefix} — starting service"));
            if let Err(e) = self.queue_startup_service_start(name, done_tx, ServiceStartMode::Full) {
                self.output_manager
                    .service_error_event(name, &e.to_string());
            }
        }
    }

    /// First failed `depends_on` entry of `name`, if any. Re-checked after a
    /// JIT build since a dependency can fail while the build runs.
    pub(in crate::runner) fn first_failed_dep(&self, name: &str) -> Option<String> {
        self.services
            .get(name)?
            .resolved
            .depends_on
            .iter()
            .find(|dep| self.is_dep_failed(dep))
            .cloned()
    }
}
