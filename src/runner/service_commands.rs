use super::Demand;
use super::service_worker::{ServiceStartMode, run_service_build_worker};
use super::{
    CommandError, CommandResult, Runner, RunnerEvent, RunnerInternalCommand, ServiceStartIntent,
    ServiceState, ServiceStopAction,
};
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
    /// Bumps `start_generation` because the ready check and the dependency
    /// sweep still key off it, but deciding whether a *prepared* start is
    /// current is no longer a question the runner asks — the supervisor only
    /// reports the start it is committed to.
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
    pub(in crate::runner) fn handle_service_starting(&mut self, name: &str) {
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
        self.output_manager.service_event(name, "starting...");
    }

    pub(in crate::runner) async fn handle_service_start_prepared(
        &mut self,
        name: &str,
        intent: ServiceStartIntent,
        result: Result<Box<super::service_supervisor::ServiceWired>, String>,
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
                self.set_service_state(name, ServiceState::Failed);
                self.output_manager.service_error_event(name, &message);
                let should_auto_restart = matches!(intent, ServiceStartIntent::Background)
                    && self.services.get(name).is_some_and(|rs| {
                        rs.resolved.on_failure == crate::config::OnFailure::Restart
                    });
                if should_auto_restart {
                    self.schedule_auto_restart(name, &message, true);
                } else if let Some(rs) = self.services.get_mut(name) {
                    rs.reset_restart_tracking();
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

    pub(in crate::runner) fn spawn_service_rebuild_worker(
        &mut self,
        name: &str,
        resolved: crate::config::ResolvedService,
    ) -> Result<(), CommandError> {
        let Some(rs) = self.services.get_mut(name) else {
            return Err(CommandError::UnknownService {
                name: name.to_string(),
            });
        };
        rs.rebuild_generation = rs.rebuild_generation.saturating_add(1);
        let op_id = rs.rebuild_generation;

        let cmd_tx = self.internal_tx.clone();
        let name_owned = name.to_string();
        let base_dir = self.base_dir.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        let docker_client = self.docker_client.clone();
        let service_writer = self.output_manager.service_writer(name);
        let worker = tokio::spawn(async move {
            let result = run_service_build_worker(
                &base_dir,
                docker_client.as_ref(),
                &emitter,
                &name_owned,
                &resolved,
                false,
                service_writer.as_ref(),
            )
            .await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::ServiceRebuildPrepared {
                    name: name_owned,
                    op_id,
                    result,
                })
                .await;
        });
        rs.rebuild_worker = Some(worker);
        Ok(())
    }

    pub(in crate::runner) async fn continue_rebuild_restart(&mut self, name: &str) {
        if self.shutting_down {
            return;
        }
        // Clear the backend before the stop is queued: mailbox FIFO applies
        // it first, so connections arriving during the restart queue instead
        // of racing the dying process.
        if self
            .services
            .get(name)
            .is_some_and(|rs| rs.proxy_view.is_some())
        {
            self.send_proxy_directive(
                name,
                super::service_supervisor::ProxyDirective::ClearBackend,
            );
        }

        if self.services.get(name).is_some_and(|rs| rs.pgid.is_some()) {
            self.set_service_state(name, ServiceState::Stopping);
            let shutdown_config = self.effective_shutdown_config(name);
            let wait_full = self
                .services
                .get(name)
                .and_then(|rs| rs.proxy_view.as_ref())
                .is_some_and(|p| p.requires_full_exit_on_restart());
            self.send_service_stop(
                name,
                shutdown_config,
                wait_full,
                None,
                ServiceStopAction::RestartSpawnOnly,
            );
            return;
        }

        if let Err(e) = self.queue_rebuild_service_start(name).await {
            self.fail_rebuild(name, &e.to_string());
        }
    }

    pub(in crate::runner) async fn handle_service_rebuild_prepared(
        &mut self,
        name: &str,
        op_id: u64,
        result: Result<(), String>,
    ) {
        let is_current = self
            .services
            .get(name)
            .is_some_and(|rs| rs.rebuild_generation == op_id);
        if !is_current {
            return;
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.rebuild_worker = None;
        }

        match result {
            Ok(()) => {
                if self.take_rebuild_stale(name) {
                    let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                        name: name.to_string(),
                        success: true,
                    });
                } else {
                    self.continue_rebuild_restart(name).await;
                }
            }
            Err(message) if message == "shutdown requested" => {
                self.initiate_shutdown().await;
            }
            Err(message) => self.fail_rebuild(name, &message),
        }
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

    pub(in crate::runner) async fn queue_rebuild_service_start(
        &mut self,
        name: &str,
    ) -> Result<(), CommandError> {
        if self.shutting_down {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "shutdown in progress".to_string(),
            });
        }
        // Ephemeral backend ports are reallocated by the supervisor (the
        // proxy's owner) as part of handling this start — a failure there
        // surfaces through the prepared-Err path like any other prepare
        // failure. The runner's backend-env shadow refreshes from the wired
        // message, before ready resolution reads it.
        self.set_service_state(name, ServiceState::Starting);
        self.output_manager.service_event(name, "restarting...");
        self.spawn_service_start_worker(
            name,
            ServiceStartMode::SpawnOnly,
            ServiceStartIntent::Background,
            // Restart: the new process must bind fresh ephemeral backend
            // ports while connections draining to the old ones finish.
            true,
        )
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
        if self
            .services
            .get(name)
            .is_some_and(|rs| rs.handle_identity.is_some())
        {
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
            ServiceState::Pending
                | ServiceState::Building
                | ServiceState::Starting
                | ServiceState::Stopping
        ) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: format!("cannot hard restart while {state:?}"),
            }));
            return;
        }
        if self
            .services
            .get(name)
            .is_some_and(|rs| rs.rebuild_worker.is_some())
        {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "operation already in progress".to_string(),
            }));
            return;
        }

        self.clear_rebuild_stale(name);
        self.output_manager
            .service_event(name, "rebuilding (requested hard restart)");

        let resolved = match self.services.get(name) {
            Some(rs) => rs.resolved.clone(),
            None => {
                let _ = reply.send(Err(CommandError::UnknownService {
                    name: name.to_string(),
                }));
                return;
            }
        };

        let result = if resolved.is_build_tool_managed() {
            self.spawn_forced_build_tool_rebuild(name).await
        } else {
            self.spawn_service_rebuild_worker(name, resolved)
        };

        match result {
            Ok(()) => {
                let _ = reply.send(Ok(()));
            }
            Err(e) => {
                self.output_manager
                    .service_error_event(name, &e.to_string());
                let _ = reply.send(Err(e));
            }
        }
    }

    /// Ask the service's supervisor — the process's owner — to stop it.
    ///
    /// The reply rides down with the request and comes back on the report
    /// channel; only `stop_action` (what the *rebuild* cycle wants next)
    /// stays fold-side, and it needs no currency check: a supervisor runs one
    /// stop at a time, so completions arrive in the order the stops were sent.
    pub(in crate::runner) fn send_service_stop(
        &mut self,
        name: &str,
        shutdown_config: crate::config::ShutdownConfig,
        wait_full_exit: bool,
        reply: Option<oneshot::Sender<CommandResult>>,
        stop_action: ServiceStopAction,
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
        if let Some(rs) = self.services.get_mut(name) {
            rs.stop_action = stop_action;
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
        restarting: bool,
    ) {
        let stop_action = match self.services.get_mut(name) {
            Some(rs) => std::mem::take(&mut rs.stop_action),
            None => return,
        };

        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }

        match result {
            Ok(()) => {
                if let Some(rs) = self.services.get_mut(name) {
                    rs.pgid = None;
                }
                self.clear_service_custody(name);
                self.set_service_state(name, ServiceState::Stopped);
                let next_result = if restarting {
                    // The supervisor has already begun the start half; its
                    // own `ServiceStarting` report drives the transition.
                    Ok(())
                } else {
                    match stop_action {
                        ServiceStopAction::None => Ok(()),
                        ServiceStopAction::RestartSpawnOnly => {
                            if self.take_rebuild_stale(name) {
                                let _ = self.event_tx.send(RunnerEvent::RebuildComplete {
                                    name: name.to_string(),
                                    success: true,
                                });
                                Ok(())
                            } else {
                                self.queue_rebuild_service_start(name).await
                            }
                        }
                    }
                };
                Self::answer(reply, next_result);
            }
            Err(message) => {
                if let Some(rs) = self.services.get_mut(name) {
                    rs.pgid = None;
                }
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
                rs.stop_health_tracking();
                rs.reset_restart_tracking();
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
            rs.stop_health_tracking();
            rs.reset_restart_tracking();
        }
        if self.services.get(name).and_then(|rs| rs.pgid).is_none() {
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
        self.send_service_stop(
            name,
            shutdown_config,
            false,
            Some(reply),
            ServiceStopAction::None,
        );
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
            rs.stop_health_tracking();
            rs.reset_restart_tracking();
        }
        if self.services.get(name).and_then(|rs| rs.pgid).is_none() {
            let _ = reply.send(self.queue_background_service_start(name, ServiceStartMode::Full));
            return;
        }
        self.set_service_state(name, ServiceState::Stopping);
        self.output_manager
            .service_event(name, "stopping... (requested restart)");
        self.send_service_restart(name, RestartPlan::full(), Some(reply));
    }
}
