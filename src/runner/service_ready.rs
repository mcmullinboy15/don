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
            rs.scheduled_start = scheduled;
        }
        self.fold_service_custody(
            name,
            identity,
            spawned_pgid,
            docker_port_bindings,
            proxy_backend_env,
        );
        if let Some(pgid) = spawned_pgid {
            self.output_manager
                .service_debug_event(name, &format!("spawned pid={pgid}"));
        }
        self.set_service_state(name, ServiceState::Running);
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
        policy: crate::process::health::PolicyOutcome,
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
            let ready_message =
                scheduled
                    .then(|| {
                        self.services.get(name).map(|rs| {
                            match crate::endpoints::effective_ready_check(
                                &self.endpoints.snapshot(),
                                name,
                                &rs.resolved,
                            ) {
                                Some(r) if r.tcp.is_some() => {
                                    format!("ready (tcp {})", r.tcp.as_deref().unwrap_or("unknown"))
                                }
                                Some(r) if r.http.is_some() => {
                                    format!(
                                        "ready (http {})",
                                        r.http.as_deref().unwrap_or("unknown")
                                    )
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
            if let Some(rs) = self.services.get_mut(name) {
                rs.restart_pending = false;
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
            // The supervisor already applied its restart policy — including
            // the crash ceiling that stops a lazy service relaunching off a
            // still-queued trigger connection — and narrated the result.
            // Landing it in the right state is what is left.
            if let Some(rs) = self.services.get_mut(name) {
                rs.restart_pending = policy.restart_pending();
            }
            self.apply_failure_state(name, &policy);
            if scheduled
                && matches!(policy, crate::process::health::PolicyOutcome::None)
                && let Some(ref msg) = message
            {
                self.output_manager.service_error_event(name, msg);
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
