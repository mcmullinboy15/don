use super::health::{format_unexpected_exit, unhealthy_restart_backoff_secs};
use super::service::ServiceHandle;
use super::service_worker::ServiceStartMode;
use super::{Runner, RunnerInternalCommand, ServiceState};
use tokio::sync::oneshot;

const MAX_STARTUP_FAILURES_BEFORE_GIVE_UP: u32 = 3;

impl Runner {
    /// Apply a health-monitor probe transition for a service.
    ///
    /// Only acts when the service is in `Ready` (failure -> `Unhealthy`)
    /// or `Unhealthy` (recovery -> `Ready`). Stale messages from a monitor
    /// task whose service has since stopped/restarted are ignored.
    pub(in crate::runner) async fn handle_service_health_changed(
        &mut self,
        name: &str,
        healthy: bool,
    ) {
        let current = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        if healthy {
            if current != ServiceState::Unhealthy {
                return;
            }
            self.set_service_state(name, ServiceState::Ready);
            let attempts = self
                .services
                .get(name)
                .map(|rs| rs.restart_attempts)
                .unwrap_or(0);
            if let Some(rs) = self.services.get_mut(name) {
                if let Some(handle) = rs.pending_restart.take() {
                    handle.abort();
                }
                rs.restart_attempts = 0;
            }
            let msg = if attempts > 0 {
                "recovered (cancelled pending restart, attempts reset)"
            } else {
                "recovered (health check passing)"
            };
            self.output_manager.service_event(name, msg);
        } else {
            if current != ServiceState::Ready {
                return;
            }
            self.set_service_state(name, ServiceState::Unhealthy);
            let policy = self
                .services
                .get(name)
                .map(|rs| rs.resolved.on_failure)
                .unwrap_or_default();
            match policy {
                crate::config::OnFailure::Notify => {
                    self.output_manager
                        .service_error_event(name, "unhealthy (health check failing)");
                }
                crate::config::OnFailure::Restart => {
                    self.schedule_auto_restart(name, "unhealthy", false);
                }
            }
        }
    }

    /// Schedule an automatic restart for a failed service. Used for both
    /// `Unhealthy` (monitor-driven) and `Failed` (crash-driven) failures.
    /// Uses exponential backoff (1, 2, 4, 8, 16, 32, 60s) on consecutive
    /// attempts. Replaces any already-scheduled restart for this service.
    /// `reason` is included verbatim in the lifecycle event so a reader
    /// can tell why the restart was scheduled.
    pub(in crate::runner) fn schedule_auto_restart(
        &mut self,
        name: &str,
        reason: &str,
        limit_startup_failures: bool,
    ) {
        let attempt = self
            .services
            .get(name)
            .map(|rs| rs.restart_attempts.saturating_add(1))
            .unwrap_or(1);
        if limit_startup_failures && attempt >= MAX_STARTUP_FAILURES_BEFORE_GIVE_UP {
            if let Some(rs) = self.services.get_mut(name) {
                rs.restart_attempts = attempt;
                if let Some(prev) = rs.pending_restart.take() {
                    prev.abort();
                }
            }
            self.output_manager.service_error_event(
                name,
                &format!(
                    "{reason} — giving up after {attempt} failed starts without becoming ready"
                ),
            );
            return;
        }
        let backoff_secs = unhealthy_restart_backoff_secs(attempt);
        self.output_manager.service_error_event(
            name,
            &format!("{reason} — auto-restart in {backoff_secs}s (attempt {attempt})"),
        );
        let cmd_tx = self.internal_tx.clone();
        let name_owned = name.to_string();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
            let _ = cmd_tx
                .send(RunnerInternalCommand::AutoRestart {
                    name: name_owned,
                    attempt,
                })
                .await;
        });
        if let Some(rs) = self.services.get_mut(name) {
            rs.restart_attempts = attempt;
            if let Some(prev) = rs.pending_restart.replace(handle) {
                prev.abort();
            }
        }
    }

    /// Handle an unexpected exit reported by the per-spawn crash watcher.
    ///
    /// The watcher fires whenever the child's output stream EOFs. That happens
    /// for both crashes and graceful stops, so the handler filters stale/known
    /// stop paths before reaping and applying the on_failure policy.
    pub(in crate::runner) async fn handle_service_exited(&mut self, name: &str, pgid: i32) {
        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        if !matches!(
            state,
            ServiceState::Running | ServiceState::Ready | ServiceState::Unhealthy
        ) {
            return;
        }
        let current_pgid = self.services.get(name).and_then(|rs| match &rs.handle {
            Some(ServiceHandle::Process(p)) => Some(p.pgid()),
            _ => None,
        });
        if current_pgid != Some(pgid) {
            return;
        }
        let handle = match self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
            Some(h) => h,
            None => return,
        };
        let status = if let ServiceHandle::Process(mut proc) = handle {
            proc.wait().await.ok()
        } else {
            None
        };
        if let Some(rs) = self.services.get_mut(name) {
            rs.stop_health_tracking();
        }

        let clean_exit = status.as_ref().is_some_and(|s| s.success());
        if clean_exit {
            if let Some(rs) = self.services.get_mut(name) {
                rs.restart_attempts = 0;
                rs.pgid = None;
            }
            self.set_service_state(name, ServiceState::Stopped);
            self.output_manager
                .service_event(name, "exited cleanly (status 0)");
            if let Some(writer) = self.output_manager.service_writer(name) {
                writer.close_follow_sinks().await;
            }
            return;
        }

        if let Some(rs) = self.services.get_mut(name) {
            rs.pgid = None;
        }
        self.set_service_state(name, ServiceState::Failed);
        let exit_msg = format_unexpected_exit(status);
        self.output_manager.service_error_event(name, &exit_msg);
        let policy = self
            .services
            .get(name)
            .map(|rs| rs.resolved.on_failure)
            .unwrap_or_default();
        if matches!(policy, crate::config::OnFailure::Restart) {
            self.schedule_auto_restart(name, &exit_msg, state == ServiceState::Running);
        } else if let Some(rs) = self.services.get_mut(name) {
            rs.restart_attempts = 0;
        }
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }
    }

    /// Handle a backoff-timer-fired auto-restart.
    pub(in crate::runner) async fn handle_auto_restart(&mut self, name: &str, attempt: u32) {
        let state = match self.services.get(name) {
            Some(rs) => rs.state(),
            None => return,
        };
        if !matches!(state, ServiceState::Unhealthy | ServiceState::Failed) {
            return;
        }
        if let Some(rs) = self.services.get_mut(name) {
            rs.pending_restart = None;
        }
        self.output_manager
            .service_event(name, &format!("auto-restart firing (attempt {attempt})"));
        if self
            .services
            .get(name)
            .is_some_and(|rs| rs.handle.is_some())
        {
            let (reply_tx, _reply_rx) = oneshot::channel();
            self.handle_auto_restart_running_service(name, reply_tx)
                .await;
        } else {
            let _ = self.queue_background_service_start(name, ServiceStartMode::Full);
        }
    }

    async fn handle_auto_restart_running_service(
        &mut self,
        name: &str,
        reply: oneshot::Sender<super::CommandResult>,
    ) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.stop_health_tracking();
        }
        let handle = match self.services.get_mut(name).and_then(|rs| rs.handle.take()) {
            Some(h) => h,
            None => {
                let _ =
                    reply.send(self.queue_background_service_start(name, ServiceStartMode::Full));
                return;
            }
        };
        let shutdown_config = self.effective_shutdown_config(name);
        if self.remove_attach_lock(name) {
            self.output_manager.resume_stdout_sink(name).await;
        }
        self.set_service_state(name, ServiceState::Stopping);
        self.output_manager
            .service_event(name, "stopping... (auto-restart)");
        self.spawn_manual_service_stop_worker(
            name,
            handle,
            shutdown_config,
            false,
            reply,
            super::ServiceStopAction::RestartFull,
        );
    }
}
