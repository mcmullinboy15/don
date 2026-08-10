use super::Demand;
use super::service_worker::ServiceStartMode;
use super::{CommandError, CommandResult, Runner, RunnerEvent, ServiceStartIntent, ServiceState};
use tokio::sync::oneshot;

/// What a restart should do, as the scheduler asks for it.
///
/// The shape varies by caller — a manual restart re-runs the full start, the
/// rebuild cycle re-spawns onto fresh backend ports behind a cleared proxy —
/// so it travels as one value rather than four positional flags.
pub(in crate::runner) struct RestartPlan {
    pub(in crate::runner) wait_full_exit: bool,
    pub(in crate::runner) clear_backend_first: bool,
    pub(in crate::runner) start_mode: ServiceStartMode,
    pub(in crate::runner) fresh_backend_ports: bool,
}

impl RestartPlan {
    /// Stop and run the whole start again: what `don restart` and an
    /// auto-restart both want.
    pub(in crate::runner) fn full() -> Self {
        Self {
            wait_full_exit: false,
            clear_backend_first: false,
            start_mode: ServiceStartMode::Full,
            fresh_backend_ports: false,
        }
    }
}

impl Runner {
    fn lookup_service(&self, name: &str) -> Result<&crate::config::Service, CommandError> {
        if let Some(svc) = self.config.services.get(name) {
            return Ok(svc);
        }
        if self.config.tasks.contains_key(name) {
            return Err(CommandError::NotAService {
                name: name.to_string(),
            });
        }
        Err(CommandError::UnknownService {
            name: name.to_string(),
        })
    }

    /// Queue a start on this service's supervisor.
    ///
    /// Whether a *prepared* start is still current is not a question the
    /// runner asks: a supervisor is the only thing that reports a prepared
    /// start for its service, and only for the start it is committed to.
    fn spawn_service_start_worker(
        &mut self,
        name: &str,
        mode: ServiceStartMode,
        intent: ServiceStartIntent,
        fresh_backend_ports: bool,
    ) -> Result<(), CommandError> {
        let Some(handle) = self.service_starts.registry().get(name).cloned() else {
            return Err(CommandError::UnknownService {
                name: name.to_string(),
            });
        };
        if !handle.request(super::service_supervisor::ServiceCommand::Start(
            super::service_supervisor::StartRequest {
                mode,
                intent,
                fresh_backend_ports,
            },
        )) {
            return Err(CommandError::Failed {
                name: name.to_string(),
                message: "service supervisor is shutting down".to_string(),
            });
        }
        Ok(())
    }

    /// A supervisor spent its start permission, or began the start half of a
    /// restart it owns.
    ///
    /// For a spent permission the transition to `Starting` is what closes the
    /// gate, so this is the runner's half of making a level-triggered grant
    /// single-use. The states accepted are the ones a start can legitimately
    /// begin from: `Pending` for a gate grant, and the settled states a
    /// restart's own stop just produced. Anything else means the service
    /// moved on without this supervisor — a manual start won the race — and
    /// the ack is ignored; the supervisor's own idle-and-empty check is the
    /// other half. Teardown refuses outright: nothing may come up while the
    /// stop order is being walked.
    pub(in crate::runner) fn handle_service_starting(&mut self, name: &str, restarting: bool) {
        let startable = !self.shutting_down
            && self.services.get(name).is_some_and(|rs| {
                matches!(
                    rs.state(),
                    ServiceState::Pending
                        | ServiceState::Stopped
                        | ServiceState::Failed
                        | ServiceState::Unhealthy
                )
            });
        if !startable {
            self.output_manager
                .service_debug_event(name, "start began for a service that could no longer start");
            return;
        }
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager.service_event(
            name,
            if restarting {
                "restarting..."
            } else {
                "starting..."
            },
        );
    }

    pub(in crate::runner) async fn handle_service_start_prepared(
        &mut self,
        name: &str,
        intent: ServiceStartIntent,
        result: Result<Box<super::service_supervisor::ServiceWired>, String>,
        policy: crate::process::health::PolicyOutcome,
    ) {
        if self.shutting_down {
            self.stop_late_service_start(name.to_string(), result).await;
            return;
        }
        match result {
            Ok(wired) => match intent {
                ServiceStartIntent::Scheduled => {
                    self.handle_service_wired(name, *wired, true).await;
                }
                ServiceStartIntent::Reply { reply } => {
                    self.handle_service_wired(name, *wired, false).await;
                    let _ = reply.send(Ok(()));
                }
                ServiceStartIntent::Background => {
                    self.handle_service_wired(name, *wired, false).await;
                }
            },
            Err(message) => {
                self.apply_failure_state(name, &policy);
                self.output_manager.service_error_event(name, &message);
                if let Some(rs) = self.services.get_mut(name) {
                    rs.restart_pending = policy.restart_pending();
                }
                match intent {
                    // The failure handling above (Failed state + optional
                    // auto-restart) is the whole answer for a scheduled
                    // start: the Failed transition re-schedules the sweep.
                    ServiceStartIntent::Scheduled => {}
                    ServiceStartIntent::Reply { reply } => {
                        let _ = reply.send(Err(CommandError::Failed {
                            name: name.to_string(),
                            message,
                        }));
                    }
                    ServiceStartIntent::Background => {}
                }
            }
        }
        self.refresh_runtime_port_manifest();
    }

    /// Ask this service's supervisor to rebuild and restart into the result.
    ///
    /// The whole cycle — build, stop, spawn, and the staleness that decides
    /// whether the spawn happens — is the supervisor's; this only routes the
    /// request and reports an unknown name.
    pub(in crate::runner) fn send_rebuild(
        &mut self,
        name: &str,
        forced: bool,
        reply: Option<oneshot::Sender<CommandResult>>,
    ) {
        if self.shutting_down {
            Self::answer(
                reply,
                Err(CommandError::InvalidState {
                    name: name.to_string(),
                    message: "shutdown in progress".to_string(),
                }),
            );
            return;
        }
        let mut carried = Some(reply);
        let sent = self
            .service_starts
            .registry()
            .get(name)
            .is_some_and(|handle| {
                handle.request(super::service_supervisor::ServiceCommand::Rebuild(
                    super::service_supervisor::RebuildRequest {
                        forced,
                        reply: carried.take().flatten(),
                    },
                ))
            });
        if !sent {
            self.fail_rebuild(name, "rebuild requested for unknown service");
            Self::answer(
                carried.flatten(),
                Err(CommandError::UnknownService {
                    name: name.to_string(),
                }),
            );
        }
    }

    /// A watched file changed while a rebuild cycle was running.
    pub(in crate::runner) fn send_mark_stale(&self, name: &str) {
        if let Some(handle) = self.service_starts.registry().get(name) {
            let _ = handle.request(super::service_supervisor::ServiceCommand::MarkStale);
        }
    }

    /// Close a watch cycle that never reached a supervisor.
    pub(in crate::runner) fn fail_rebuild(&self, name: &str, message: &str) {
        self.output_manager.service_error_event(name, message);
        let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
            name: name.to_string(),
            success: false,
        });
    }

    pub(in crate::runner) fn queue_background_service_start(
        &mut self,
        name: &str,
        mode: ServiceStartMode,
    ) -> Result<(), CommandError> {
        if self.shutting_down {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "shutdown in progress".to_string(),
            });
        }
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager.service_event(name, "starting...");
        self.spawn_service_start_worker(name, mode, ServiceStartIntent::Background, false)
    }

    pub(in crate::runner) async fn handle_start_service_cmd(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }
        let state = self.services.get(name).map(|rs| rs.state());
        // `Pending` is deliberately not here. It does not mean "busy", it
        // means "wanted, waiting for dependencies" — and overriding that wait
        // is exactly what an explicit start is for. It falls through to the
        // dependency rule below.
        let operation_in_progress = self.services.get(name).is_some_and(|rs| {
            self.service_starts.registry().is_busy(name)
                || matches!(
                    rs.state(),
                    ServiceState::Building | ServiceState::Starting | ServiceState::Stopping
                )
        });
        if operation_in_progress {
            let state = state.unwrap_or(ServiceState::Stopped);
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: format!("cannot start while {state:?}"),
            }));
            return;
        }
        if self.service_runtime(name).is_some() {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "already running".to_string(),
            }));
            return;
        }

        // An explicit start honours the dependency graph — this used to be the
        // one start path that didn't, so `don start api` would spawn `api`
        // with `postgres` down.
        //
        // It honours it on the *relaxed* rule: a dependency that is still
        // coming up is worth waiting for, so refuse and say so; one that has
        // settled (failed, stopped, parked awaiting a human) never will be, so
        // proceed — the user asked for this by name. Same predicate the
        // supervisors use, so the two paths cannot drift.
        let deps = self
            .services
            .get(name)
            .map(|rs| rs.resolved.depends_on.clone())
            .unwrap_or_default();
        if !Demand::Requested.permitted_by(self.dep_level(&deps)) {
            let waiting: Vec<&str> = deps
                .iter()
                .filter(|dep| !self.is_dep_gate_open(dep) && !self.is_dep_settled(&dep.name))
                .map(|dep| dep.name.as_str())
                .collect();
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: format!("waiting for dependency '{}'", waiting.join("', '")),
            }));
            return;
        }

        self.set_service_state(name, ServiceState::Starting);
        self.output_manager
            .service_event(name, "starting... (requested)");
        if let Err(e) = self.spawn_service_start_worker(
            name,
            ServiceStartMode::Full,
            ServiceStartIntent::Reply { reply },
            false,
        ) {
            self.output_manager
                .service_error_event(name, &e.to_string());
        }
    }

    pub(in crate::runner) async fn handle_hard_restart_service_cmd(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }

        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => {
                let _ = reply.send(Err(CommandError::UnknownService {
                    name: name.to_string(),
                }));
                return;
            }
        };
        if matches!(
            state,
            ServiceState::Building | ServiceState::Starting | ServiceState::Stopping
        ) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: format!("cannot hard restart while {state:?}"),
            }));
            return;
        }
        self.output_manager
            .service_event(name, "rebuilding (requested hard restart)");
        // The supervisor answers as soon as the build is accepted; a forced
        // rebuild refused because a batch is already in flight is still the
        // synchronous "already in progress" this path has always given.
        self.send_rebuild(name, true, Some(reply));
    }

    /// Ask the service's supervisor — the process's owner — to stop it.
    ///
    /// The reply rides down with the request and comes back on the report
    /// channel. No currency check is needed: a supervisor runs one stop at a
    /// time, so completions arrive in the order the stops were sent.
    pub(in crate::runner) fn send_service_stop(
        &mut self,
        name: &str,
        shutdown_config: crate::config::ShutdownConfig,
        wait_full_exit: bool,
        reply: Option<oneshot::Sender<CommandResult>>,
        reset_policy: bool,
    ) {
        if !self.services.contains_key(name) {
            Self::answer(
                reply,
                Err(CommandError::UnknownService {
                    name: name.to_string(),
                }),
            );
            return;
        }

        let shutdown_rx = self.shutdown_flag_tx.subscribe();
        let mut carried = Some(reply);
        let sent = self
            .service_starts
            .registry()
            .get(name)
            .is_some_and(|handle| {
                handle.request(super::service_supervisor::ServiceCommand::Stop(
                    super::service_supervisor::StopRequest {
                        config: shutdown_config,
                        force: false,
                        wait_full_exit,
                        interrupt: Some(shutdown_rx),
                        notify: super::service_supervisor::StopNotify::Reply(
                            carried.take().flatten(),
                        ),
                        reset_policy,
                    },
                ))
            });
        if !sent {
            Self::answer(
                carried.flatten(),
                Err(CommandError::Failed {
                    name: name.to_string(),
                    message: "service supervisor is shutting down".to_string(),
                }),
            );
        }
    }

    /// Ask the supervisor to stop what it holds and start again — one
    /// operation, because every step of it belongs to the owner.
    pub(in crate::runner) fn send_service_restart(
        &mut self,
        name: &str,
        plan: RestartPlan,
        reply: Option<oneshot::Sender<CommandResult>>,
    ) {
        let shutdown_config = self.effective_shutdown_config(name);
        let shutdown_rx = self.shutdown_flag_tx.subscribe();
        let mut carried = Some(reply);
        let sent = self
            .service_starts
            .registry()
            .get(name)
            .is_some_and(|handle| {
                handle.request(super::service_supervisor::ServiceCommand::Restart(
                    Box::new(super::service_supervisor::RestartRequest {
                        config: shutdown_config,
                        wait_full_exit: plan.wait_full_exit,
                        interrupt: Some(shutdown_rx),
                        clear_backend_first: plan.clear_backend_first,
                        start_mode: plan.start_mode,
                        fresh_backend_ports: plan.fresh_backend_ports,
                        intent: ServiceStartIntent::Background,
                        reply: carried.take().flatten(),
                        announce_restarting: false,
                        // Every caller of this is an explicit request; the
                        // policy's own retry never comes through here.
                        reset_policy: true,
                    }),
                ))
            });
        if !sent {
            Self::answer(
                carried.flatten(),
                Err(CommandError::Failed {
                    name: name.to_string(),
                    message: "service supervisor is shutting down".to_string(),
                }),
            );
        }
    }

    fn answer(reply: Option<oneshot::Sender<CommandResult>>, result: CommandResult) {
        if let Some(reply) = reply {
            let _ = reply.send(result);
        }
    }

    pub(in crate::runner) async fn handle_service_stop_complete(
        &mut self,
        name: &str,
        result: Result<(), String>,
        reply: Option<oneshot::Sender<CommandResult>>,
    ) {
        if !self.services.contains_key(name) {
            return;
        }

        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        match result {
            Ok(()) => {
                self.clear_service_custody(name);
                self.set_service_state(name, ServiceState::Stopped);
                // A restart's start half is the supervisor's; its own
                // `ServiceStarting` report drives the transition.
                Self::answer(reply, Ok(()));
            }
            Err(message) => {
                self.clear_service_custody(name);
                self.set_service_state(name, ServiceState::Failed);
                self.output_manager.service_error_event(name, &message);
                Self::answer(
                    reply,
                    Err(CommandError::Failed {
                        name: name.to_string(),
                        message,
                    }),
                );
            }
        }
        self.refresh_runtime_port_manifest();
    }

    /// Handle an API-initiated Stop command.
    pub(in crate::runner) async fn handle_stop_cmd(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }
        // An untriggered or dependency-blocked lazy service has no process —
        // just mark it Stopped. Pending means its proxy received a connection,
        // but the dependency scheduler has not started it yet.
        if self.services.get(name).is_some_and(|rs| {
            rs.resolved.lazy && matches!(rs.state(), ServiceState::Lazy | ServiceState::Pending)
        }) {
            self.set_service_state(name, ServiceState::Stopped);
            self.output_manager
                .service_event(name, "stopped before lazy start");
            let _ = reply.send(Ok(()));
            return;
        }
        // A failed service has no live process — drop any exited handle and
        // mark it Stopped so the user can clear it without a restart.
        if self.services.get(name).is_some_and(|rs| {
            matches!(
                rs.state(),
                ServiceState::Failed | ServiceState::DependencyFailed
            )
        }) {
            if let Some(rs) = self.services.get_mut(name) {
                rs.restart_pending = false;
            }
            self.clear_service_custody(name);
            self.set_service_state(name, ServiceState::Stopped);
            self.output_manager
                .service_event(name, "stopped (was failed)");
            let _ = reply.send(Ok(()));
            return;
        }
        // Cancel monitor + any pending auto-restart before tearing down the
        // process so a recovery probe doesn't race with the stop.
        if let Some(rs) = self.services.get_mut(name) {
            rs.restart_pending = false;
        }
        if self
            .service_runtime(name)
            .and_then(|runtime| runtime.pid)
            .is_none()
        {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "not running".to_string(),
            }));
            return;
        }
        let shutdown_config = self.effective_shutdown_config(name);
        self.set_service_state(name, ServiceState::Stopping);
        self.output_manager
            .service_event(name, "stopping... (requested)");
        self.send_service_stop(name, shutdown_config, false, Some(reply), true);
    }

    pub(in crate::runner) async fn handle_restart_service_cmd(
        &mut self,
        name: &str,
        reply: oneshot::Sender<CommandResult>,
    ) {
        if let Err(e) = self.lookup_service(name) {
            let _ = reply.send(Err(e));
            return;
        }
        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => {
                let _ = reply.send(Err(CommandError::UnknownService {
                    name: name.to_string(),
                }));
                return;
            }
        };
        if matches!(
            state,
            ServiceState::Pending
                | ServiceState::Building
                | ServiceState::Starting
                | ServiceState::Stopping
        ) || self.services.get(name).is_some_and(|_| false)
        {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: format!("cannot restart while {state:?}"),
            }));
            return;
        }
        if matches!(state, ServiceState::Lazy | ServiceState::Stopped) {
            let _ = reply.send(self.queue_background_service_start(name, ServiceStartMode::Full));
            return;
        }

        if let Some(rs) = self.services.get_mut(name) {
            rs.restart_pending = false;
        }
        if self
            .service_runtime(name)
            .and_then(|runtime| runtime.pid)
            .is_none()
        {
            let _ = reply.send(self.queue_background_service_start(name, ServiceStartMode::Full));
            return;
        }
        self.set_service_state(name, ServiceState::Stopping);
        self.output_manager
            .service_event(name, "stopping... (requested restart)");
        self.send_service_restart(name, RestartPlan::full(), Some(reply));
    }
}
