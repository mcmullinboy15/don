use super::events::ItemDone;
use super::{NodeKind, Runner, RunnerEvent, ServiceState};
use tokio::sync::mpsc;

impl Runner {
    /// Fold a wired start into runner state.
    ///
    /// The supervisor owns the process, the output reader, the ready racer
    /// and the health monitor; this side keeps the shadows attach and
    /// status read, makes the state transition, and remembers where the
    /// ready outcome should answer (`pending_done` for a scheduled start).
    /// The outcome itself arrives later as [`super::ItemReport::ServiceReady`],
    /// on the same channel as the wired report and from the same producer,
    /// so it can never be folded before this bookkeeping runs.
    pub(in crate::runner) async fn handle_service_wired(
        &mut self,
        name: &str,
        wired: super::service_supervisor::ServiceWired,
        done_tx: Option<mpsc::Sender<ItemDone>>,
    ) {
        let super::service_supervisor::ServiceWired {
            identity,
            pgid: spawned_pgid,
            docker_port_bindings,
            osc_sink,
            pty_input,
            proxy_backend_env,
        } = wired;
        for binding in docker_port_bindings
            .iter()
            .filter(|binding| binding.used_fallback())
        {
            if binding.configured_host_port == 0 {
                self.output_manager.service_event(
                    name,
                    &format!(
                        "allocated Docker port {} at {}",
                        binding.configured,
                        binding.connect_addr()
                    ),
                );
            } else {
                self.output_manager.service_event(
                    name,
                    &format!(
                        "Docker port {} is unavailable; using {}",
                        binding.configured,
                        binding.connect_addr()
                    ),
                );
            }
        }

        if let Some(rs) = self.services.get_mut(name) {
            rs.pgid = spawned_pgid;
            rs.docker_port_bindings = docker_port_bindings;
            rs.handle_identity = Some(identity);
            rs.osc_sink = osc_sink;
            rs.pty_input = pty_input;
            rs.pending_done = done_tx;
            // Stamp the spawn time so a fast crash can be distinguished from a
            // failure after the service did real work (see the crash-loop
            // guard in `handle_service_exited`).
            rs.last_start = Some(std::time::Instant::now());
            // Refresh the backend-env shadow: a restart reallocates ephemeral
            // backend ports, and the status path's `${PORT}` display must
            // resolve to the port this spawn was told.
            if let (Some(view), Some(backend_env)) = (rs.proxy_view.as_mut(), proxy_backend_env) {
                view.backend_env = backend_env;
            }
        }
        if let Some(pgid) = spawned_pgid {
            self.output_manager
                .service_debug_event(name, &format!("spawned pid={pgid}"));
        }
        self.set_service_state(name, ServiceState::Running);
        self.refresh_runtime_port_manifest();
    }

    /// Fold a ready outcome reported by the service's supervisor.
    ///
    /// A scheduled start answers the dependency sweep (`pending_done`);
    /// everything else — manual start, rebuild restart — flips state here
    /// and broadcasts `RebuildComplete` so the watch cycle closes.
    pub(in crate::runner) async fn handle_service_ready_report(
        &mut self,
        name: &str,
        op_id: u64,
        success: bool,
        message: Option<String>,
        had_check: bool,
    ) {
        // Interim currency guard until the generations die: identical to
        // the old detached-task guards. With the supervisor forwarding
        // outcomes after its own prepared report on one channel, this can
        // only reject genuinely superseded outcomes.
        let is_current = self
            .services
            .get(name)
            .is_some_and(|rs| rs.start_generation == op_id && rs.state() == ServiceState::Running);
        if !is_current {
            return;
        }
        let pending_done = self
            .services
            .get_mut(name)
            .and_then(|rs| rs.pending_done.take());
        match pending_done {
            Some(done_tx) => {
                // Scheduled start: the dependency sweep folds the outcome
                // via `handle_service_done` (state flip, ready line, lazy
                // failure routing).
                let _ = done_tx
                    .send(ItemDone {
                        name: name.to_string(),
                        kind: NodeKind::Service,
                        success,
                        message,
                        elapsed: None,
                        last_run: None,
                        service_start_generation: Some(op_id),
                        task_run_generation: None,
                    })
                    .await;
            }
            None => {
                // Manual start / rebuild restart. State handling matches the
                // old ReadyCheckComplete handler; a checkless restart also
                // announces itself, as the old immediate-Ready branch did.
                self.handle_ready_check_complete(name, op_id, success, message);
                if success && !had_check {
                    self.output_manager.service_event(name, "restarted");
                }
                let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                    name: name.to_string(),
                    success,
                });
            }
        }
    }
}
