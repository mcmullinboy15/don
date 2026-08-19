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
use tokio::sync::{mpsc, watch};

use super::actor::OutputHandle;
use super::{LifecycleEmitter, PtyInput, SinkLine, emulator};
use crate::command::CommandError;

/// An active attach session, returned to the server's upgrade handler.
pub struct AttachSession {
    /// Sender into the spawn's PTY input gate — input frames and resizes
    /// interleave atomically with every other writer.
    pub pty_input: mpsc::Sender<PtyInput>,
    /// The process's screen as it stood the instant `output_rx` was cut, to
    /// be written to the client before anything from that receiver.
    ///
    /// Separate from the stream because it has to go first, and the stream
    /// starts taking live bytes the moment it exists. `None` when the process
    /// has no screen to repaint from.
    pub repaint: Option<Bytes>,
    /// Live output receiver, carrying every byte written from the moment the
    /// repaint above was taken.
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

/// The emitter attach narrates through, held behind a `watch` so it can be
/// *released* at shutdown — and doubling as the "is output still up?" signal.
///
/// This is load-bearing, not indirection for its own sake: a
/// `LifecycleEmitter` clone keeps the stdout writer's channel open, and
/// [`OutputManager::shutdown`](super::OutputManager::shutdown) waits (2s,
/// then aborts) for those writers to finish. `AttachControl` lives in the
/// API server's state, which outlives the output flush, so holding the
/// emitter directly would put that 2s stall on every shutdown. The manager
/// publishes `None` before flushing, which drops it.
///
/// The stdout sink used to be here too, for the pause each first client
/// causes. That belongs to the process's own output actor now, which is the
/// only thing that touches its sink list.
#[derive(Clone)]
pub(super) struct AttachSinks {
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
    services: watch::Receiver<Arc<HashMap<String, OutputHandle>>>,
    sinks: watch::Receiver<Option<AttachSinks>>,
    emulator: emulator::EmulatorHandle,
    detach_tx: mpsc::UnboundedSender<String>,
}

impl AttachControl {
    pub(super) fn spawn(
        services: watch::Receiver<Arc<HashMap<String, OutputHandle>>>,
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

    fn output_for(&self, name: &str) -> Option<OutputHandle> {
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
    /// Attach a client, at the grid size the process should now believe it
    /// has.
    ///
    /// `size` is the *effective* size — the smallest attached client wins each
    /// dimension — and it is applied to the screen before the repaint is
    /// rendered off it, which is the whole reason it is a parameter here. The
    /// repaint is a screenful of absolutely-positioned rows padded to the
    /// screen's width; rendered at one width and replayed into a client whose
    /// screen is another, every row wraps, the screen scrolls, and everything
    /// but the last few lines is pushed off the top before the client draws a
    /// single frame. Which looks, from the outside, exactly like attaching
    /// showing you nothing.
    pub async fn attach(
        &self,
        name: &str,
        pid: u32,
        size: (u16, u16),
    ) -> Result<AttachSession, CommandError> {
        let Some(output) = self.output_for(name) else {
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

        // One message, so the "first client mutes stdout" transition cannot
        // race a concurrent attach or the reap clear — the actor applies it
        // as a unit, which is what taking the lock once used to buy.
        let Some((pty_input, count)) = output.attach().await else {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "no attachable process (not running, or no PTY)".to_string(),
            });
        };

        // Preload the client with a coherent repaint of the process's current
        // screen, then stream live bytes — never a raw-byte replay. Processes
        // whose screen never registered (emulator backend unavailable) fall
        // back to the last ring-buffer lines.
        self.emulator.resize(name, size.0, size.1);
        let Some((output_rx, frame_rx)) = output.attach_sink(256).await else {
            return Err(CommandError::InvalidState {
                name: name.to_string(),
                message: "process output is shutting down".to_string(),
            });
        };
        // `None` when the process has no screen — the emulator backend failed,
        // or this is a pipe spawn. The client then simply starts from live
        // bytes, which is what it did before there was a repaint at all.
        let repaint = frame_rx
            .await
            .ok()
            .flatten()
            .map(|frame| Bytes::from(frame.bytes));

        sinks.emitter.service_event(
            name,
            &format!(
                "attached (pid {pid}, {count} client{})",
                if count == 1 { "" } else { "s" }
            ),
        );

        Ok(AttachSession {
            pty_input,
            repaint,
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
        let Some(output) = self.output_for(name) else {
            return;
        };
        // Output already shut down: the sinks are gone and every process's
        // state was cleared, so there is nothing to restore.
        let Some(sinks) = self.sinks() else {
            return;
        };
        // `None` is a late notification after the reap clear already reset
        // the count (and resumed output).
        let Some(remaining) = output.detach().await else {
            return;
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
