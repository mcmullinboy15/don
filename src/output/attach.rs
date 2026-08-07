//! Interactive attach, owned by the process's I/O — not the runner.
//!
//! An attach session is two things, both properties of the *live spawn*: a
//! sender into its PTY input gate, and an output sink preloaded with a
//! coherent screen repaint. The supervisor registers the gate sender when it
//! wires a PTY spawn and clears it when it reaps (the same seam where it
//! registers the emulator screen), so "may I attach?" is answered by the
//! per-process output state directly — any client the server trusts can
//! attach, with no runner round trip.
//!
//! There is no detach request. An [`AttachSession`] carries an
//! [`AttachGuard`]; dropping it (the bridge ending, however it ends) *is*
//! the detach. The guard notifies a worker that decrements the client count
//! and resumes prefixed stdout when the last client leaves, so a
//! disconnected client can never leave a process's output muted.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, watch};

use super::{
    LifecycleEmitter, PtyInput, ServiceOutputState, SinkHandle, SinkLine, emulator,
    follow_sink_from,
};
use crate::command::CommandError;

/// An active attach session, returned to the server's upgrade handler.
pub struct AttachSession {
    /// Sender into the spawn's PTY input gate — input frames and resizes
    /// interleave atomically with every other writer.
    pub pty_input: mpsc::Sender<PtyInput>,
    /// Live output receiver (preloaded with a screen repaint, or the last
    /// ring-buffer lines when no emulator screen is registered).
    pub output_rx: mpsc::Receiver<SinkLine>,
    /// Dropping this is the detach. Hold it for the bridge's lifetime.
    pub guard: AttachGuard,
}

/// Detach-on-drop token. The bridge holds it; however the bridge ends —
/// clean close, client vanishing, upgrade failure — the drop notifies the
/// detach worker, which does the bookkeeping.
pub struct AttachGuard {
    name: String,
    detach_tx: mpsc::UnboundedSender<String>,
}

impl Drop for AttachGuard {
    fn drop(&mut self) {
        let _ = self.detach_tx.send(std::mem::take(&mut self.name));
    }
}

/// The sink senders attach needs, held behind a `watch` so they can be
/// *released* at shutdown.
///
/// This is load-bearing, not indirection for its own sake: a `SinkHandle`
/// or `LifecycleEmitter` clone keeps the stdout writer's channel open, and
/// [`OutputManager::shutdown`](super::OutputManager::shutdown) waits (2s,
/// then aborts) for those writers to finish. `AttachControl` lives in the
/// API server's state, which outlives the output flush, so holding the
/// senders directly would put that 2s stall on every shutdown. The manager
/// publishes `None` before flushing, which drops them.
#[derive(Clone)]
pub(super) struct AttachSinks {
    pub(super) stdout_sink: SinkHandle,
    pub(super) emitter: LifecycleEmitter,
}

/// A cloneable handle for opening attach sessions.
///
/// Same family as [`LogReader`](super::LogReader): held by the server,
/// answered off the shared per-process output state, no runner involved.
/// Mint it once ([`super::OutputManager::attach_control`]) — each call
/// spawns its own detach worker.
#[derive(Clone)]
pub struct AttachControl {
    services: watch::Receiver<Arc<HashMap<String, Arc<Mutex<ServiceOutputState>>>>>,
    sinks: watch::Receiver<Option<AttachSinks>>,
    emulator: emulator::EmulatorHandle,
    detach_tx: mpsc::UnboundedSender<String>,
}

impl AttachControl {
    pub(super) fn spawn(
        services: watch::Receiver<Arc<HashMap<String, Arc<Mutex<ServiceOutputState>>>>>,
        sinks: watch::Receiver<Option<AttachSinks>>,
        emulator: emulator::EmulatorHandle,
    ) -> Self {
        let (detach_tx, mut detach_rx) = mpsc::unbounded_channel::<String>();
        let control = Self {
            services,
            sinks,
            emulator,
            detach_tx,
        };
        let worker = control.clone();
        tokio::spawn(async move {
            while let Some(name) = detach_rx.recv().await {
                worker.detach(&name).await;
            }
        });
        control
    }

    fn state_for(&self, name: &str) -> Option<Arc<Mutex<ServiceOutputState>>> {
        self.services.borrow().get(name).cloned()
    }

    /// The sinks, or `None` once output has shut down.
    fn sinks(&self) -> Option<AttachSinks> {
        self.sinks.borrow().clone()
    }

    /// Open an attach session for `name`. `pid` is the attaching client's
    /// pid, used only for the lifecycle message.
    ///
    /// Attach is multi-client: every bridge is read-write and symmetric,
    /// input frames interleave atomically at the PTY gate, and this layer
    /// only counts clients — for the stdout-sink pause and the lifecycle
    /// events. Waiting for a process that isn't running yet is the client's
    /// job (it retries); this answers immediately.
    pub async fn attach(&self, name: &str, pid: u32) -> Result<AttachSession, CommandError> {
        let Some(state_arc) = self.state_for(name) else {
            return Err(CommandError::UnknownService {
                name: name.to_string(),
            });
        };
        // Output has shut down; nothing is attachable any more.
        let Some(sinks) = self.sinks() else {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "no attachable process (not running, or no PTY)".to_string(),
            });
        };

        // Count + pause under one lock, so the "first client mutes stdout"
        // transition can never race a concurrent attach or the reap clear.
        let (pty_input, count) = {
            let mut state = state_arc.lock().await;
            let Some(pty_input) = state.attach_pty.clone() else {
                return Err(CommandError::InvalidState {
                    name: name.to_string(),
                    message: "no attachable process (not running, or no PTY)".to_string(),
                });
            };
            state.attach_clients += 1;
            let count = state.attach_clients;
            if count == 1 {
                let had_stdout = state
                    .sinks
                    .iter()
                    .any(|s| s.same_channel(&sinks.stdout_sink));
                if had_stdout {
                    state.sinks.retain(|s| !s.same_channel(&sinks.stdout_sink));
                    state.stdout_paused = true;
                }
            }
            (pty_input, count)
        };

        // Preload the client with a coherent repaint of the process's current
        // screen, then stream live bytes — never a raw-byte replay. Processes
        // whose screen never registered (emulator backend unavailable) fall
        // back to the last ring-buffer lines.
        let output_rx = match self.emulator.repaint(name).await {
            Some(frame) => {
                // Headroom for live bytes on top of the repaint frame.
                let (tx, rx) = mpsc::channel::<SinkLine>(256);
                let mut state = state_arc.lock().await;
                let sink_line = SinkLine {
                    prefix: Bytes::new(),
                    line: Bytes::from(frame.bytes),
                    name: state.name.clone(),
                    is_lifecycle: false,
                    is_verbose: false,
                };
                // Channel is empty and capacity >= 2, so this cannot fail.
                let _ = tx.try_send(sink_line);
                state.sinks.push(SinkHandle::BoundedDrop(tx));
                rx
            }
            None => follow_sink_from(state_arc.clone(), 50, 256).await,
        };

        sinks.emitter.service_event(
            name,
            &format!(
                "attached (pid {pid}, {count} client{})",
                if count == 1 { "" } else { "s" }
            ),
        );

        Ok(AttachSession {
            pty_input,
            output_rx,
            guard: AttachGuard {
                name: name.to_string(),
                detach_tx: self.detach_tx.clone(),
            },
        })
    }

    /// One bridge ended (its guard dropped). Only bookkeeping remains — the
    /// bridge's gate sender and output sink are already gone.
    async fn detach(&self, name: &str) {
        let Some(state_arc) = self.state_for(name) else {
            return;
        };
        // Output already shut down: the sinks are gone and every
        // ServiceOutputState was cleared, so there is nothing to restore.
        let Some(sinks) = self.sinks() else {
            return;
        };
        let remaining = {
            let mut state = state_arc.lock().await;
            if state.attach_clients == 0 {
                // A late notification after the reap clear already reset the
                // count (and resumed output).
                return;
            }
            state.attach_clients -= 1;
            if state.attach_clients == 0 && state.stdout_paused {
                state.stdout_paused = false;
                let already_present = state
                    .sinks
                    .iter()
                    .any(|s| s.same_channel(&sinks.stdout_sink));
                if !already_present {
                    state.sinks.push(sinks.stdout_sink.clone());
                }
            }
            state.attach_clients
        };
        if remaining == 0 {
            sinks.emitter.service_event(name, "detached");
        } else {
            sinks.emitter.service_event(
                name,
                &format!(
                    "detached ({remaining} client{} remain)",
                    if remaining == 1 { "" } else { "s" }
                ),
            );
        }
    }

    /// An empty control for tests that need an `ApiState` without an
    /// `OutputManager`. Every attach answers `UnknownService`.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        let (tx, rx) = watch::channel(Arc::new(HashMap::new()));
        std::mem::forget(tx);
        let (sink_tx, sink_rx) = watch::channel(None);
        std::mem::forget(sink_tx);
        let (detach_tx, _detach_rx) = mpsc::unbounded_channel();
        Self {
            services: rx,
            sinks: sink_rx,
            emulator: emulator::spawn_emulator_thread(),
            detach_tx,
        }
    }
}
