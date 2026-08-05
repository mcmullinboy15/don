use super::events::ItemDone;
use super::health::run_health_monitor;
use super::service::{self, ServiceHandle};
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
        start_result: service::StartResult,
        resolved: &crate::config::ResolvedService,
        done_tx: Option<mpsc::Sender<ItemDone>>,
    ) {
        let docker_port_bindings = match &start_result.handle {
            ServiceHandle::Docker(handle) => handle.port_bindings().to_vec(),
            ServiceHandle::Process(_) => Vec::new(),
        };
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

        // One handle for every output operation this spawn needs; see the
        // note in `wire_task_output_and_wait` for why it's taken up front.
        let output = self.output_manager.item_output(name);
        let mut spawned_pgid: Option<i32> = None;
        if let Some(rs) = self.services.get_mut(name) {
            if let ServiceHandle::Process(ref proc) = start_result.handle {
                spawned_pgid = Some(proc.pgid());
            }
            rs.pgid = spawned_pgid;
            rs.docker_port_bindings = docker_port_bindings;
            rs.handle = Some(start_result.handle);
            // Stamp the spawn time so a fast crash can be distinguished from a
            // failure after the service did real work (see the crash-loop
            // guard in `handle_service_exited`).
            rs.last_start = Some(std::time::Instant::now());

            // Add OSC response sink if we have a PTY write handle.
            if let Some(ServiceHandle::Process(process)) = rs.handle.as_mut()
                && let Some(pty) = process.take_pty_write()
                && let Some(output) = output.as_ref()
            {
                rs.osc_sink = Some(output.add_osc_sink(pty).await);
            }
        }
        if let Some(pgid) = spawned_pgid {
            self.output_manager
                .service_debug_event(name, &format!("spawned pid={pgid}"));
        }
        self.fulfill_pending_waiter(name).await;
        self.set_service_state(name, ServiceState::Running);
        self.refresh_runtime_port_manifest();

        // Wire up output processing. We need to fan the EOF (= process died)
        // out to two independent waiters: the ready check (cancels its
        // retry loop), and the crash watcher (reports the exit upstream so
        // the runner can reap the child and transition state).
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        let (crash_exit_tx, crash_exit_rx) = tokio::sync::oneshot::channel();
        let child_output = start_result.child_output;
        let output_worker = output.map(|output| {
            let svc_writer = output.writer();
            tokio::spawn(async move {
                let _ = svc_writer.process_stream(child_output).await;
                let _ = exit_tx.send(());
                let _ = crash_exit_tx.send(());
            })
        });
        if let Some(rs) = self.services.get_mut(name) {
            if let Some(old_worker) = rs.output_worker.take() {
                old_worker.abort();
            }
            rs.output_worker = output_worker;
        }

        // Crash watcher — fires `ServiceExited` to the runner when the
        // child's output stream EOFs. Skipped for Docker because the
        // bollard log stream's EOF semantics aren't yet wired to a status
        // code path. The pgid lets the handler ignore stale events that
        // arrive after the service has already been respawned.
        if let Some(pgid) = spawned_pgid {
            let cmd_tx = self.internal_tx.clone();
            let watch_name = name.to_string();
            tokio::spawn(async move {
                let _ = crash_exit_rx.await;
                let _ = cmd_tx
                    .send(RunnerInternalCommand::ServiceExited {
                        name: watch_name,
                        pgid,
                    })
                    .await;
            });
        }

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
            // If the new instance has `monitor = true`, build the cancel
            // channel up front and stash the sender on the RuntimeService —
            // the spawned task spawns the monitor on Ready and uses the
            // matching receiver. Stop/restart cancels by dropping the sender.
            let monitor_cancel_rx = if ready.monitor {
                if let Some(rs) = self.services.get_mut(name) {
                    rs.stop_health_tracking();
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    rs.monitor_cancel = Some(tx);
                    Some(rx)
                } else {
                    None
                }
            } else {
                None
            };
            let cmd_tx_for_monitor = self.internal_tx.clone();
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
                        run_health_monitor(monitor_name, ready, cmd_tx_for_monitor, cancel_rx)
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
