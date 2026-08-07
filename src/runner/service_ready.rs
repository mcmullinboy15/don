use super::{Runner, RunnerEvent, ServiceState};

impl Runner {
    /// Fold a wired start into runner state.
    ///
    /// The supervisor owns the process, the output reader, the ready racer
    /// and the health monitor; this side keeps the shadows attach and
    /// status read, makes the state transition, and remembers whether the
    /// start answers the dependency sweep. The ready outcome arrives later
    /// as [`super::ProcessReport::ServiceReady`], on the same channel as the
    /// wired report and from the same producer, so it can never be folded
    /// before this bookkeeping runs.
    pub(in crate::runner) async fn handle_service_wired(
        &mut self,
        name: &str,
        wired: super::service_supervisor::ServiceWired,
        scheduled: bool,
    ) {
        let super::service_supervisor::ServiceWired {
            identity,
            pgid: spawned_pgid,
            docker_port_bindings,
            osc_sink,
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
            rs.scheduled_start = scheduled;
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
    /// The one handler for every start flavour — the sweep's scheduled
    /// starts, manual starts, and rebuild restarts — because the outcome
    /// always arrives on the report channel after its own wired report and
    /// before anything the next start produces. The state guard alone
    /// rejects the only stale case left: a supervisor's stop/new-start
    /// cleared the pending outcome, so a stale one is unforwardable, and a
    /// crash's exit report folds first and moves the service out of
    /// `Running`.
    pub(in crate::runner) async fn handle_service_ready_report(
        &mut self,
        name: &str,
        success: bool,
        message: Option<String>,
        had_check: bool,
    ) {
        let Some(rs) = self.services.get(name) else {
            return;
        };
        if rs.state() != ServiceState::Running {
            return;
        }
        let scheduled = rs.scheduled_start;
        if let Some(rs) = self.services.get_mut(name) {
            rs.scheduled_start = false;
        }

        if success {
            // Report the address actually probed, not the configured
            // template — `effective_ready_check` is what the probe ran
            // against (the supervisor resolves through the same function).
            let ready_message = scheduled
                .then(|| {
                    self.services.get(name).map(|rs| {
                        match self.effective_ready_check(name, &rs.resolved) {
                            Some(r) if r.tcp.is_some() => {
                                format!("ready (tcp {})", r.tcp.as_deref().unwrap_or("unknown"))
                            }
                            Some(r) if r.http.is_some() => {
                                format!("ready (http {})", r.http.as_deref().unwrap_or("unknown"))
                            }
                            Some(r) if r.exec.is_some() => "ready (exec)".to_string(),
                            _ => "started".to_string(),
                        }
                    })
                })
                .flatten();
            // Re-activate the proxy backend on ready. The supervisor already
            // activates at wire time; this covers a backend cleared between
            // wire and ready (e.g. a rebuild's ClearBackend landing late).
            if scheduled
                && self
                    .services
                    .get(name)
                    .is_some_and(|rs| rs.proxy_view.is_some())
            {
                self.send_proxy_directive(
                    name,
                    super::service_supervisor::ProxyDirective::SetBackend,
                );
            }
            // Reaching Ready resets the backoff counter, but not the
            // rapid-crash streak — see `handle_service_exited`, which clears
            // that only once the process has survived past the crash window.
            if let Some(rs) = self.services.get_mut(name) {
                if let Some(handle) = rs.pending_restart.take() {
                    handle.abort();
                }
                rs.restart_attempts = 0;
            }
            self.set_service_state(name, ServiceState::Ready);
            if let Some(message) = ready_message {
                self.output_manager.service_event(name, &message);
            } else if !scheduled && !had_check {
                // A checkless restart announces itself; the "restarting..."
                // line already told the user a cycle began.
                self.output_manager.service_event(name, "restarted");
            }
        } else {
            // If a lazy service fails, reset to Lazy so the next connection
            // can re-trigger it instead of leaving it permanently failed.
            let is_lazy = self
                .services
                .get(name)
                .is_some_and(|rs| rs.resolved.lazy && rs.proxy_view.is_some());
            if is_lazy {
                // Route through the crash-loop guard: returns to `Lazy` and
                // re-arms the proxy trigger normally, but gives up (leaving
                // it `Failed`, trigger un-armed) once it has crashed on
                // launch too many times in a row.
                self.handle_lazy_launch_failure(name, message.as_deref());
            } else {
                self.set_service_state(name, ServiceState::Failed);
                if scheduled && let Some(ref msg) = message {
                    self.output_manager.service_error_event(name, msg);
                }
                let policy = self
                    .services
                    .get(name)
                    .map(|rs| rs.resolved.on_failure)
                    .unwrap_or_default();
                if matches!(policy, crate::config::OnFailure::Restart) {
                    self.schedule_auto_restart(
                        name,
                        message
                            .as_deref()
                            .unwrap_or("service failed before becoming ready"),
                        true,
                    );
                }
            }
        }

        if !scheduled {
            // Manual starts and rebuild restarts close the watch cycle.
            let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                name: name.to_string(),
                success,
            });
        }
    }
}
