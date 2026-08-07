use super::{AttachSession, CommandError, Runner};
use tokio::sync::oneshot;

impl Runner {
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

    /// The current attach-client count for a service or task.
    fn attach_count(&self, name: &str) -> usize {
        self.services
            .get(name)
            .map(|rs| rs.attach_count)
            .or_else(|| self.tasks.get(name).map(|rt| rt.attach_count))
            .unwrap_or(0)
    }

    fn set_attach_count(&mut self, name: &str, count: usize) {
        if let Some(rs) = self.services.get_mut(name) {
            rs.attach_count = count;
        } else if let Some(rt) = self.tasks.get_mut(name) {
            rt.attach_count = count;
        }
    }

    /// Clear attach bookkeeping when a process goes away. Returns whether
    /// any clients were attached (the caller resumes prefixed output).
    /// The bridges themselves end on their own when the output sinks close;
    /// their late `Detach` notifications no-op against a zero count.
    pub(in crate::runner) fn reset_attach_state(&mut self, name: &str) -> bool {
        let had_clients = self.attach_count(name) > 0;
        self.set_attach_count(name, 0);
        had_clients
    }

    /// Handle an interactive attach request (services or tasks).
    ///
    /// Attach is multi-client: every bridge is read-write and symmetric,
    /// input frames interleave atomically at the PTY gate, and the runner
    /// only counts clients — for the stdout-sink pause and the lifecycle
    /// events. Waiting for a process that isn't running yet is the client's
    /// job (it retries); the runner answers immediately.
    pub(in crate::runner) async fn handle_attach_cmd(
        &mut self,
        name: &str,
        pid: u32,
        reply: oneshot::Sender<Result<AttachSession, CommandError>>,
    ) {
        if !self.services.contains_key(name) && !self.tasks.contains_key(name) {
            let _ = reply.send(Err(CommandError::UnknownService {
                name: name.to_string(),
            }));
            return;
        }
        let result = self.fulfill_attach(name, pid).await;
        let _ = reply.send(result);
    }

    /// Build an attach session for a running process.
    async fn fulfill_attach(
        &mut self,
        name: &str,
        pid: u32,
    ) -> Result<AttachSession, CommandError> {
        // The input gate owns the PTY write half for the spawn's lifetime;
        // a bridge just gets a sender into it. No custody changes hands, so
        // nothing needs restoring on detach. No sender means no live PTY —
        // not running, docker, or pipe mode.
        let pty_input = self
            .get_pty_input(name)
            .ok_or_else(|| CommandError::InvalidState {
                name: name.to_string(),
                message: "no attachable process (not running, or no PTY)".to_string(),
            })?;

        // Preload the client with a coherent repaint of the process's current
        // screen, then stream live bytes — never a raw-byte replay. Processes
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

        let count = self.attach_count(name).saturating_add(1);
        self.set_attach_count(name, count);
        // Pause prefixed stdout while any client is attached.
        if count == 1 {
            self.output_manager.pause_stdout_sink(name).await;
        }

        self.output_manager.service_event(
            name,
            &format!(
                "attached (pid {pid}, {count} client{})",
                if count == 1 { "" } else { "s" }
            ),
        );

        Ok(AttachSession {
            pty_input,
            output_rx,
        })
    }

    /// One bridge ended. The bridge's gate sender is already dropped; only
    /// bookkeeping remains.
    pub(in crate::runner) async fn handle_detach(&mut self, name: &str) {
        let count = self.attach_count(name);
        if count == 0 {
            // A late notification after the process exit already reset the
            // count (and resumed output).
            return;
        }
        let count = count - 1;
        self.set_attach_count(name, count);
        if count == 0 {
            self.output_manager.resume_stdout_sink(name).await;
            self.output_manager.service_event(name, "detached");
        } else {
            self.output_manager.service_event(
                name,
                &format!(
                    "detached ({count} client{} remain)",
                    if count == 1 { "" } else { "s" }
                ),
            );
        }
    }
}
