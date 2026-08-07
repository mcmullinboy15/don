use super::{AttachSession, AttachWaiter, CommandError, Runner};
use tokio::sync::oneshot;

impl Runner {
    /// Look up the attach lock for a service or task by name.
    pub(in crate::runner) fn get_attach_lock(&self, name: &str) -> Option<u32> {
        self.services
            .get(name)
            .and_then(|rs| rs.attach_lock)
            .or_else(|| self.tasks.get(name).and_then(|rt| rt.attach_lock))
    }

    /// The live spawn's PTY input-gate sender for a service or task.
    fn get_pty_input(
        &self,
        name: &str,
    ) -> Option<tokio::sync::mpsc::Sender<crate::output::PtyInput>> {
        self.services
            .get(name)
            .and_then(|rs| rs.pty_input.clone())
            .or_else(|| self.tasks.get(name).and_then(|rt| rt.pty_input.clone()))
    }

    /// Remove the attach lock for a service or task, returning whether it was set.
    pub(in crate::runner) fn remove_attach_lock(&mut self, name: &str) -> bool {
        if let Some(rs) = self.services.get_mut(name) {
            return rs.attach_lock.take().is_some();
        }
        if let Some(rt) = self.tasks.get_mut(name) {
            return rt.attach_lock.take().is_some();
        }
        false
    }

    /// Set the attach lock for a service or task.
    fn set_attach_lock(&mut self, name: &str, pid: u32) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.attach_lock = Some(pid);
        } else if let Some(rt) = self.tasks.get_mut(name) {
            rt.attach_lock = Some(pid);
        }
    }

    /// Check if there is a pending attach waiter for a service or task.
    fn has_attach_waiter(&self, name: &str) -> bool {
        self.services
            .get(name)
            .is_some_and(|rs| rs.attach_waiter.is_some())
            || self
                .tasks
                .get(name)
                .is_some_and(|rt| rt.attach_waiter.is_some())
    }

    /// Take the pending attach waiter for a service or task.
    fn take_attach_waiter(&mut self, name: &str) -> Option<AttachWaiter> {
        if let Some(rs) = self.services.get_mut(name)
            && rs.attach_waiter.is_some()
        {
            return rs.attach_waiter.take();
        }
        if let Some(rt) = self.tasks.get_mut(name) {
            return rt.attach_waiter.take();
        }
        None
    }

    /// Set a pending attach waiter for a service or task.
    fn set_attach_waiter(&mut self, name: &str, waiter: AttachWaiter) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.attach_waiter = Some(waiter);
        } else if let Some(rt) = self.tasks.get_mut(name) {
            rt.attach_waiter = Some(waiter);
        }
    }

    /// Handle an interactive attach request (services or tasks).
    ///
    /// If the process is running, attaches immediately. If not running
    /// (e.g. task between runs), registers a waiter that will be fulfilled
    /// when the process next spawns.
    pub(in crate::runner) async fn handle_attach_cmd(
        &mut self,
        name: &str,
        pid: u32,
        reply: oneshot::Sender<Result<AttachSession, CommandError>>,
    ) {
        // Must be a known service or task.
        let is_service = self.services.contains_key(name);
        let is_task = self.tasks.contains_key(name);
        if !is_service && !is_task {
            let _ = reply.send(Err(CommandError::UnknownService {
                name: name.to_string(),
            }));
            return;
        }

        // Check attach lock.
        if let Some(existing_pid) = self.get_attach_lock(name) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: format!("process {existing_pid} is currently attached to '{name}'"),
            }));
            return;
        }

        // Check for a pending waiter (another client already waiting).
        if self.has_attach_waiter(name) {
            let _ = reply.send(Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "another client is already waiting to attach".to_string(),
            }));
            return;
        }

        // Check if the process is running.
        let is_running = if is_service {
            self.services
                .get(name)
                .is_some_and(|rs| rs.handle_identity.is_some())
        } else {
            self.tasks.get(name).is_some_and(|rt| rt.pgid.is_some())
        };

        if !is_running {
            // Not running — register a waiter. The reply will be sent when
            // the process next spawns.
            self.output_manager
                .service_event(name, "waiting for process to start (attach pending)");
            self.set_attach_waiter(name, AttachWaiter { pid, reply });
            return;
        }

        // Running — attach immediately.
        let result = self.fulfill_attach(name, pid).await;
        let _ = reply.send(result);
    }

    /// Fulfill an attach request for a running process. Assumes the caller
    /// has already validated the name exists and checked the attach lock.
    async fn fulfill_attach(
        &mut self,
        name: &str,
        pid: u32,
    ) -> Result<AttachSession, CommandError> {
        // The input gate owns the PTY write half for the spawn's lifetime;
        // a bridge just gets a sender into it. No custody changes hands, so
        // nothing needs restoring on detach.
        let pty_input = self
            .get_pty_input(name)
            .ok_or_else(|| CommandError::InvalidState {
                name: name.to_string(),
                message: "no PTY available (spawned in pipe mode)".to_string(),
            })?;

        // Preload the client with a coherent repaint of the item's current
        // screen, then stream live bytes — never a raw-byte replay. Items
        // whose screen never registered (emulator backend unavailable) fall
        // back to the last ring-buffer lines.
        let output_rx = match self.output_manager.emulator_repaint(name).await {
            Some(frame) => self.output_manager.add_attach_sink(name, frame, 256).await,
            None => self.output_manager.add_follow_sink(name, 50, 256).await,
        }
        .ok_or_else(|| CommandError::Failed {
            name: name.to_string(),
            message: "failed to create output sink".to_string(),
        })?;

        // Pause prefixed stdout for this service.
        self.output_manager.pause_stdout_sink(name).await;

        // Acquire the lock.
        self.set_attach_lock(name, pid);

        self.output_manager
            .service_event(name, &format!("attached (pid {pid})"));

        Ok(AttachSession {
            pty_input,
            output_rx,
        })
    }

    /// Check for a pending attach waiter and fulfill it if the process
    /// is now running.
    pub(in crate::runner) async fn fulfill_pending_waiter(&mut self, name: &str) {
        if let Some(waiter) = self.take_attach_waiter(name) {
            // Check the waiter's reply channel is still alive (client may
            // have disconnected while waiting).
            if waiter.reply.is_closed() {
                return;
            }
            let result = self.fulfill_attach(name, waiter.pid).await;
            let _ = waiter.reply.send(result);
        }
    }

    /// Release an attach session. The bridge already dropped its gate
    /// sender; only bookkeeping remains.
    pub(in crate::runner) async fn handle_detach(&mut self, name: &str) {
        // Release lock.
        self.remove_attach_lock(name);

        // Resume prefixed output.
        self.output_manager.resume_stdout_sink(name).await;

        self.output_manager.service_event(name, "detached");
    }
}
