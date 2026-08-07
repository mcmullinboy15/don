use super::events::ItemDone;
use super::health::run_health_monitor;
use super::service;
use super::{NodeKind, Runner, RunnerEvent, RunnerInternalCommand, ServiceState};
use tokio::sync::mpsc;

impl Runner {
    /// Wire up a started service's output and ready check.
    ///
    /// Sets the service to Running, stores the handle, starts output capture,
    /// and spawns the ready check (if configured). On ready check completion:
    /// - If `done_tx` is `Some`, sends `ItemDone` (initial startup path).
    /// - If `done_tx` is `None`, sends `RebuildComplete` (file-watch rebuild path).
    pub(in crate::runner) async fn wire_service_output_and_ready_check(
        &mut self,
        name: &str,
        start_generation: u64,
        wired: super::service_supervisor::ServiceWired,
        resolved: &crate::config::ResolvedService,
        done_tx: Option<mpsc::Sender<ItemDone>>,
    ) {
        let super::service_supervisor::ServiceWired {
            identity,
            pgid: spawned_pgid,
            docker_port_bindings,
            osc_sink,
            ready_exit_rx: exit_rx,
            monitor_cancel_rx,
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
            // Stamp the spawn time so a fast crash can be distinguished from a
            // failure after the service did real work (see the crash-loop
            // guard in `handle_service_exited`).
            rs.last_start = Some(std::time::Instant::now());
        }
        if let Some(pgid) = spawned_pgid {
            self.output_manager
                .service_debug_event(name, &format!("spawned pid={pgid}"));
        }
        self.fulfill_pending_waiter(name).await;
        self.set_service_state(name, ServiceState::Running);
        self.refresh_runtime_port_manifest();

        // Output reader, OSC sink and crash watching all live with the
        // supervisor now — `exit_rx` above is its end-of-stream fan-out for
        // the ready check to race against.

        let name_owned = name.to_string();
        // Resolve ready checks only after the handle is stored: Docker's
        // actual host ports are authoritative only after container start.
        let ready_config = self.effective_ready_check(name, resolved);
        let event_tx = self.event_tx.clone();
        // For proxy services, activate the backend immediately so the proxy
        // can start forwarding. The proxy has connection-level retry with
        // backoff, so it handles the case where the service isn't listening yet.
        if let Some(rs) = self.services.get(name)
            && let Some(ref proxy) = rs.proxy
        {
            proxy.set_backend();
        }

        if let Some(ready) = ready_config {
            // The monitor's cancellation lives with the supervisor — it
            // drops the sender when the process stops or dies, so monitor
            // lifetime is tied to custody. This side only threads the
            // receiver through to the task that starts the monitor.
            let monitor_cancel_rx = ready.monitor.then_some(monitor_cancel_rx);
            let report_tx_for_monitor = self.report_tx.clone();
            let cmd_tx_for_state = self.internal_tx.clone();
            tokio::spawn(async move {
                let ready_result = tokio::select! {
                    result = service::run_ready_check(&ready) => result,
                    _ = exit_rx => {
                        Err(service::ServiceError::ProcessExitedDuringReadyCheck)
                    }
                };

                let success = ready_result.is_ok();

                // State update:
                //   done_tx path -> runner's handle_service_done flips state
                //     via set_service_state (which broadcasts). Don't
                //     duplicate it here.
                //   no-done_tx path (manual start / rebuild) -> no
                //     handle_service_done gets called, so send a command so
                //     the runner can flip state on its own task. Without
                //     this, internal state stays at Running and later
                //     health-monitor probes short-circuit.
                if done_tx.is_none() {
                    let _ = cmd_tx_for_state
                        .send(RunnerInternalCommand::ReadyCheckComplete {
                            name: name_owned.clone(),
                            generation: start_generation,
                            success,
                            message: ready_result.as_ref().err().map(ToString::to_string),
                        })
                        .await;
                }

                // Kick off the long-lived health monitor once Ready, if
                // configured. The cancel rx exists only when ready.monitor
                // was true at wire-up time, so this branch needs no extra check.
                if success && let Some(cancel_rx) = monitor_cancel_rx {
                    let monitor_name = name_owned.clone();
                    tokio::spawn(async move {
                        run_health_monitor(monitor_name, ready, report_tx_for_monitor, cancel_rx)
                            .await;
                    });
                }

                if let Some(done_tx) = done_tx {
                    let _ = done_tx
                        .send(ItemDone {
                            name: name_owned,
                            kind: NodeKind::Service,
                            success,
                            message: ready_result.err().map(|e| e.to_string()),
                            elapsed: None,
                            last_run: None,
                            service_start_generation: Some(start_generation),
                            task_run_generation: None,
                        })
                        .await;
                } else {
                    let _ = event_tx.send(RunnerEvent::RebuildComplete {
                        name: name_owned,
                        success,
                    });
                }
            });
        } else if let Some(done_tx) = done_tx {
            // No ready check, initial startup path — just signal completion.
            // `handle_service_done` flips state to Ready and emits the
            // "{name} started" lifecycle event; doing either here as well
            // would double-log and duplicate the state transition.
            let _ = done_tx
                .send(ItemDone {
                    name: name.to_string(),
                    kind: NodeKind::Service,
                    success: true,
                    message: None,
                    elapsed: None,
                    last_run: None,
                    service_start_generation: Some(start_generation),
                    task_run_generation: None,
                })
                .await;
        } else {
            // No ready check, rebuild path — mark ready immediately.
            // Only the backoff counter resets here: reaching Ready (which is
            // immediate without a ready check) is not proof the service will
            // survive, so the rapid-crash streak is left for the lifetime
            // check in `handle_service_exited` to clear.
            if let Some(rs) = self.services.get_mut(name) {
                if let Some(handle) = rs.pending_restart.take() {
                    handle.abort();
                }
                rs.restart_attempts = 0;
            }
            self.set_service_state(name, ServiceState::Ready);
            self.output_manager.service_event(name, "restarted");
            let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                name: name.to_string(),
                success: true,
            });
        }
    }
}
