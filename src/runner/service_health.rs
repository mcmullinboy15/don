//! Folding the failure reports a supervisor's restart policy produced.
//!
//! The *decisions* — how long to back off, when to give up, whether a lazy
//! service re-arms — belong to the supervisor, which is where every input
//! they read is observed (see [`crate::process::health::RestartPolicy`]).
//! What is left here is scheduling: which state a failure lands the service
//! in, and keeping the projection honest about work still in flight.

use crate::process::health::PolicyOutcome;

use super::{Runner, ServiceState};

impl Runner {
    /// Record what a supervisor's policy decided, so the scheduler's own view
    /// of "is anything still coming up" stays true. A service inside a
    /// backoff has not settled, and `has_running_services` must agree or
    /// teardown would think the stack was already idle.
    fn fold_policy(&mut self, name: &str, policy: &PolicyOutcome) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.restart_pending = policy.restart_pending();
        }
    }

    /// Apply a health-monitor probe transition for a service.
    ///
    /// Only acts when the service is in `Ready` (failure -> `Unhealthy`)
    /// or `Unhealthy` (recovery -> `Ready`). Stale messages from a monitor
    /// task whose service has since stopped/restarted are ignored.
    pub(in crate::runner) async fn handle_service_health_changed(
        &mut self,
        name: &str,
        healthy: bool,
        policy: PolicyOutcome,
    ) {
        let current = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        if healthy {
            if current != ServiceState::Unhealthy {
                return;
            }
            self.fold_policy(name, &policy);
            self.set_service_state(name, ServiceState::Ready);
            self.output_manager
                .service_event(name, "recovered (health check passing)");
        } else {
            if current != ServiceState::Ready {
                return;
            }
            self.fold_policy(name, &policy);
            self.set_service_state(name, ServiceState::Unhealthy);
            if matches!(policy, PolicyOutcome::None) {
                // `notify`: the supervisor scheduled nothing, so the fold is
                // the only thing that will say anything about it.
                self.output_manager
                    .service_error_event(name, "unhealthy (health check failing)");
            }
        }
    }

    /// Handle a process exit reported by its supervisor.
    ///
    /// The supervisor has already reaped, narrated the death, and decided
    /// whether it starts again. This applies the state that follows.
    pub(in crate::runner) async fn handle_service_exited(
        &mut self,
        name: &str,
        pgid: i32,
        status: Option<std::process::ExitStatus>,
        policy: PolicyOutcome,
    ) {
        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        // Custody already matched this exit to the process the supervisor
        // held; this guards the *fold*: by the time the report lands, the
        // runner may have wired a newer process for the service, and this
        // exit is then history, not news.
        if self.services.get(name).and_then(|rs| rs.pgid) != Some(pgid) {
            return;
        }
        // A service can sit in `Failed` with its process still alive: a failed
        // ready check under the default `on_failure = "notify"` reports the
        // failure and leaves the process running. When that process later
        // exits, clear the runtime fields and let the proxy switch to
        // refusing — the failure itself was reported long ago.
        if state == ServiceState::Failed {
            if let Some(rs) = self.services.get_mut(name) {
                rs.pgid = None;
                rs.osc_sink = None;
            }
            self.clear_service_custody(name);
            self.sync_proxy_policy(name);
            if let Some(writer) = self.output_manager.service_writer(name) {
                writer.close_follow_sinks().await;
            }
            return;
        }
        if !matches!(
            state,
            ServiceState::Running | ServiceState::Ready | ServiceState::Unhealthy
        ) {
            return;
        }

        if let Some(rs) = self.services.get_mut(name) {
            rs.pgid = None;
        }
        self.clear_service_custody(name);
        self.fold_policy(name, &policy);

        if status.as_ref().is_some_and(|s| s.success()) {
            self.set_service_state(name, ServiceState::Stopped);
            self.output_manager
                .service_event(name, "exited cleanly (status 0)");
        } else {
            self.apply_failure_state(name, &policy);
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }
    }

    /// Land a failed service in the state its policy implies.
    ///
    /// A lazy service that may still retry returns to `Lazy` so its proxy
    /// re-arms; everything else — including a lazy service past the crash
    /// ceiling — stays `Failed`.
    pub(in crate::runner) fn apply_failure_state(&mut self, name: &str, policy: &PolicyOutcome) {
        match policy {
            PolicyOutcome::LazyRearm { give_up: false, .. } => {
                self.set_service_state(name, ServiceState::Lazy);
                // Re-arm the trigger even when the state was already `Lazy`
                // (`set_service_state` no-ops then and never syncs).
                // `sync_proxy_policy` maps `Lazy` to the trigger policy and is
                // idempotent, so this is safe unconditionally.
                self.sync_proxy_policy(name);
            }
            _ => {
                self.set_service_state(name, ServiceState::Failed);
            }
        }
    }
}
