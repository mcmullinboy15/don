//! One actor per process, owning that process's output state.
//!
//! Every field here used to sit behind an `Arc<Mutex<_>>` that the writer,
//! the attach sessions, the log reader and the OSC scanner all took in turn.
//! None of them held it across an await, so it was never a deadlock risk —
//! but it was the last shared mutable state in don, and it made two things
//! awkward that are natural for an actor: the "first attaching client mutes
//! stdout" transition needed a comment explaining which mutations had to
//! happen under one lock, and dropping an `OscSinkHandle` had to spawn a
//! task just to take the lock and remove its own sink.
//!
//! The split into two channels is load bearing:
//!
//! - **Output is bounded and strictly ordered.** A child that floods must be
//!   made to wait, which is what the lock did for free; an unbounded channel
//!   would turn a noisy service into unbounded memory growth. Chunks, whole
//!   lines and the end-of-stream flush all ride this one channel, because
//!   they are the same stream and must not overtake each other.
//! - **Everything else is unbounded, and drained first.** An attach, or the
//!   reap that clears one, must not queue behind a burst of output — with the
//!   lock they never did, because they could take it between chunks.
//!
//! The end-of-stream flush is acknowledged, and that is what preserves the
//! guarantee the supervisors depend on: awaiting a process's output reader
//! means its output has been *recorded and fanned out*, not merely handed
//! over. "stopped" still cannot outrun a process's last lines.

use super::ring_buffer::RingBuffer;
use super::{CompiledLogKeepFilter, MAX_FILTER_PENDING, PtyInput, SinkHandle, SinkLine};
use bytes::{Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot};

/// How many chunks may be in flight to an actor before its reader waits.
///
/// Deep enough that a normal burst never blocks the reader, shallow enough
/// that a service spraying output cannot grow the queue without bound.
const CHUNK_QUEUE_DEPTH: usize = 256;

/// A piece of a process's output stream, in order.
enum Output {
    /// Raw bytes from the child.
    Chunk(Bytes),
    /// A whole line, newline included — structured output (docker build
    /// progress) that never was a byte stream.
    Line(Bytes),
    /// End of stream: flush the partial line the filter is holding, then
    /// answer, so the caller knows everything it wrote has landed.
    Flush { ack: oneshot::Sender<()> },
}

/// Per-process output state. Owned outright by [`run`], the actor task for
/// that process — this is why none of it is behind a lock.
struct ServiceOutputState {
    /// Service/task name — stamped onto every emitted `SinkLine` so the TUI
    /// can filter without having to reverse-map the prefix bytes.
    pub(super) name: String,
    prefix: Bytes,
    ring_buffer: RingBuffer,
    log_keep_filter: CompiledLogKeepFilter,
    filter_pending: BytesMut,
    /// Dynamic list of sinks this service writes to.
    sinks: Vec<SinkHandle>,
    /// True while the stdout sink is temporarily removed (during attach).
    /// Used to ensure `resume_stdout_sink` only restores it if it was
    /// actually present before the pause.
    stdout_paused: bool,
    /// The live spawn's PTY input-gate sender, registered by the supervisor
    /// at wire time and cleared at reap. `None` = nothing attachable
    /// (stopped, docker, or pipe mode). See [`attach`].
    attach_pty: Option<mpsc::Sender<PtyInput>>,
    /// Attached clients, counted by [`attach::AttachControl`]; drives the
    /// stdout-sink pause. Reset by the supervisor's reap clear.
    attach_clients: usize,
}

impl ServiceOutputState {
    fn output_chunks(&mut self, chunk: Bytes) -> Vec<Bytes> {
        if self.log_keep_filter.is_empty() {
            self.ring_buffer.push_chunk(chunk.as_ref());
            return vec![chunk];
        }

        self.filter_chunk(chunk.as_ref(), false)
    }

    fn flush_output(&mut self) -> Vec<Bytes> {
        if self.log_keep_filter.is_empty() {
            self.ring_buffer.flush_pending();
            return Vec::new();
        }

        self.filter_chunk(&[], true)
    }

    fn filter_chunk(&mut self, chunk: &[u8], flush: bool) -> Vec<Bytes> {
        self.filter_pending.extend_from_slice(chunk);
        let mut accepted = Vec::new();

        loop {
            let end = if let Some(pos) = self.filter_pending.iter().position(|&b| b == b'\n') {
                pos + 1
            } else if self.filter_pending.len() >= MAX_FILTER_PENDING {
                MAX_FILTER_PENDING
            } else {
                break;
            };

            let line = self.filter_pending.split_to(end).freeze();
            self.accept_line(line, &mut accepted);
        }

        if flush && !self.filter_pending.is_empty() {
            let line = self.filter_pending.split().freeze();
            self.accept_line(line, &mut accepted);
            self.ring_buffer.flush_pending();
        }

        accepted
    }

    fn accept_line(&mut self, line: Bytes, accepted: &mut Vec<Bytes>) {
        if !self.log_keep_filter.keeps(line.as_ref()) {
            return;
        }
        self.ring_buffer.push_chunk(line.as_ref());
        accepted.push(line);
    }
}

/// What a process's output actor can be asked to do.
///
/// Only [`Chunk`](Self::Chunk) and [`Flush`](Self::Flush) ride the bounded
/// channel; the rest are control and never wait behind output.
enum OutputMsg {
    /// Drop transient (follow / attach / OSC) sinks. Persistent ones —
    /// stdout, file — survive for the next spawn.
    CloseFollowSinks,
    AddSink(SinkHandle),
    /// Remove one sink by channel identity. The OSC scanner's handle does
    /// this on drop; it used to have to spawn a task to take the lock.
    RemoveSink(SinkHandle),
    /// Add a sink unless an equal one is already registered.
    AddSinkOnce(SinkHandle),
    PauseStdout,
    ResumeStdout,
    /// Register (or clear) the live spawn's PTY input gate.
    SetAttachPty(Option<mpsc::Sender<PtyInput>>),
    /// The spawn is gone: forget the gate, reset the client count, and undo
    /// any stdout pause those clients caused.
    ClearAttach,
    /// One client is attaching. Answers with the gate and the resulting
    /// client count, having muted stdout if this is the first — one message,
    /// so the transition cannot race a concurrent attach or the reap clear.
    Attach {
        reply: oneshot::Sender<Option<(mpsc::Sender<PtyInput>, usize)>>,
    },
    /// One client detached. Answers with the remaining count; `None` if the
    /// count was already zero (a late notification after the reap clear).
    Detach {
        reply: oneshot::Sender<Option<usize>>,
    },
    /// The last `n` ring-buffer lines, joined, trailing newline stripped.
    ReadLogs {
        n: usize,
        reply: oneshot::Sender<Bytes>,
    },
    /// A fresh follow sink, preloaded with the last `last_n` lines.
    FollowSink {
        last_n: usize,
        live_capacity: usize,
        reply: oneshot::Sender<mpsc::Receiver<SinkLine>>,
    },
    /// A fresh attach sink, preloaded with one frame of repaint bytes.
    RepaintSink {
        frame: Bytes,
        capacity: usize,
        reply: oneshot::Sender<mpsc::Receiver<SinkLine>>,
    },
    /// Drop every sink. Shutdown, so the writer tasks can drain and exit.
    ClearSinks,
    /// How many sinks are registered. Tests only — it is the observable that
    /// pins the OSC scanner's drop behaviour (a leaked sink is a leaked PTY).
    #[cfg(test)]
    SinkCount {
        reply: oneshot::Sender<usize>,
    },
}

/// Handle to one process's output actor. Cloneable; reusable across restarts.
#[derive(Clone)]
pub(super) struct OutputHandle {
    /// Fixed at registration, so a caller that only needs the prefix — the
    /// build-tool column — costs no round trip.
    prefix: Bytes,
    output: mpsc::Sender<Output>,
    control: mpsc::UnboundedSender<OutputMsg>,
}

impl OutputHandle {
    pub(super) fn prefix(&self) -> &Bytes {
        &self.prefix
    }

    /// Hand a chunk of child output to the actor, waiting if it is behind.
    /// Waiting is the point — see the module docs.
    pub(super) async fn chunk(&self, chunk: Bytes) {
        let _ = self.output.send(Output::Chunk(chunk)).await;
    }

    pub(super) async fn line(&self, line: Bytes) {
        let _ = self.output.send(Output::Line(line)).await;
    }

    /// Flush the stream and wait for everything sent before it to be
    /// recorded and fanned out.
    pub(super) async fn flush(&self) {
        let (ack, done) = oneshot::channel();
        if self.output.send(Output::Flush { ack }).await.is_ok() {
            let _ = done.await;
        }
    }

    fn send(&self, msg: OutputMsg) {
        let _ = self.control.send(msg);
    }

    pub(super) fn close_follow_sinks(&self) {
        self.send(OutputMsg::CloseFollowSinks);
    }

    pub(super) fn add_sink(&self, sink: SinkHandle) {
        self.send(OutputMsg::AddSink(sink));
    }

    pub(super) fn add_sink_once(&self, sink: SinkHandle) {
        self.send(OutputMsg::AddSinkOnce(sink));
    }

    pub(super) fn remove_sink(&self, sink: SinkHandle) {
        self.send(OutputMsg::RemoveSink(sink));
    }

    pub(super) fn pause_stdout(&self) {
        self.send(OutputMsg::PauseStdout);
    }

    pub(super) fn resume_stdout(&self) {
        self.send(OutputMsg::ResumeStdout);
    }

    pub(super) fn set_attach_pty(&self, pty: Option<mpsc::Sender<PtyInput>>) {
        self.send(OutputMsg::SetAttachPty(pty));
    }

    pub(super) fn clear_attach(&self) {
        self.send(OutputMsg::ClearAttach);
    }

    pub(super) fn clear_sinks(&self) {
        self.send(OutputMsg::ClearSinks);
    }

    #[cfg(test)]
    pub(super) async fn sink_count(&self) -> usize {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::SinkCount { reply });
        rx.await.unwrap_or_default()
    }

    pub(super) async fn attach(&self) -> Option<(mpsc::Sender<PtyInput>, usize)> {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::Attach { reply });
        rx.await.ok().flatten()
    }

    pub(super) async fn detach(&self) -> Option<usize> {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::Detach { reply });
        rx.await.ok().flatten()
    }

    pub(super) async fn read_logs(&self, n: usize) -> Bytes {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::ReadLogs { n, reply });
        rx.await.unwrap_or_default()
    }

    pub(super) async fn follow_sink(
        &self,
        last_n: usize,
        live_capacity: usize,
    ) -> Option<mpsc::Receiver<SinkLine>> {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::FollowSink {
            last_n,
            live_capacity,
            reply,
        });
        rx.await.ok()
    }

    pub(super) async fn repaint_sink(
        &self,
        frame: Bytes,
        capacity: usize,
    ) -> Option<mpsc::Receiver<SinkLine>> {
        let (reply, rx) = oneshot::channel();
        self.send(OutputMsg::RepaintSink {
            frame,
            capacity,
            reply,
        });
        rx.await.ok()
    }
}

/// Start one process's output actor.
pub(super) fn spawn(
    name: String,
    prefix: Bytes,
    sinks: Vec<SinkHandle>,
    stdout_sink: SinkHandle,
    log_keep_filter: CompiledLogKeepFilter,
) -> OutputHandle {
    let (output_tx, output_rx) = mpsc::channel(CHUNK_QUEUE_DEPTH);
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let state = ServiceOutputState {
        name: name.clone(),
        prefix: prefix.clone(),
        ring_buffer: RingBuffer::new(super::DEFAULT_RING_BUFFER_CAPACITY),
        log_keep_filter,
        filter_pending: BytesMut::new(),
        sinks,
        stdout_paused: false,
        attach_pty: None,
        attach_clients: 0,
    };
    // Detached: the actor ends when its channels close, which happens when
    // the last handle drops — or immediately on `ClearSinks`, which is how
    // shutdown releases the sink senders the writer tasks are waiting on.
    tokio::spawn(run(state, stdout_sink, output_rx, control_rx));
    OutputHandle {
        prefix,
        output: output_tx,
        control: control_tx,
    }
}

/// One process's output loop.
async fn run(
    mut state: ServiceOutputState,
    stdout_sink: SinkHandle,
    mut output: mpsc::Receiver<Output>,
    mut control: mpsc::UnboundedReceiver<OutputMsg>,
) {
    loop {
        tokio::select! {
            // Control first: an attach, or the reap that clears one, must
            // never wait out a backlog of output. See the module docs.
            biased;
            Some(msg) = control.recv() => {
                if !state.apply(msg, &stdout_sink) {
                    return;
                }
            }
            Some(piece) = output.recv() => match piece {
                Output::Chunk(chunk) | Output::Line(chunk) => {
                    let emitted = state.output_chunks(chunk);
                    state.fan_out(emitted);
                }
                Output::Flush { ack } => {
                    let emitted = state.flush_output();
                    state.fan_out(emitted);
                    let _ = ack.send(());
                }
            },
            else => return,
        }
    }
}

impl ServiceOutputState {
    /// Send these chunks to every sink, pruning any that have gone away.
    ///
    /// Pruning is just a retain now. It used to be a second lock acquisition
    /// after the sends, because the sinks had to be cloned out and released
    /// before anything could be written to them.
    fn fan_out(&mut self, chunks: Vec<Bytes>) {
        if chunks.is_empty() {
            return;
        }
        let mut dropped: Vec<SinkHandle> = Vec::new();
        for chunk in chunks {
            for sink in &self.sinks {
                let msg = SinkLine {
                    prefix: self.prefix.clone(),
                    line: chunk.clone(),
                    name: self.name.clone(),
                    is_lifecycle: false,
                    is_verbose: false,
                };
                if sink.send(msg).is_err() {
                    dropped.push(sink.clone());
                }
            }
        }
        if !dropped.is_empty() {
            self.sinks
                .retain(|s| !dropped.iter().any(|d| d.same_channel(s)));
        }
        self.sinks.retain(|s| !s.is_closed());
    }

    /// Restore the prefixed stdout sink if a pause put it away.
    fn resume_stdout(&mut self, stdout_sink: &SinkHandle) {
        if !self.stdout_paused {
            return;
        }
        self.stdout_paused = false;
        if !self.sinks.iter().any(|s| s.same_channel(stdout_sink)) {
            self.sinks.push(stdout_sink.clone());
        }
    }

    /// Take the prefixed stdout sink away, remembering that we did — so a
    /// service with `log = "ignore"` never gains one on resume.
    fn pause_stdout(&mut self, stdout_sink: &SinkHandle) {
        if self.sinks.iter().any(|s| s.same_channel(stdout_sink)) {
            self.sinks.retain(|s| !s.same_channel(stdout_sink));
            self.stdout_paused = true;
        }
    }

    /// Apply one control message. Returns `false` when the actor should end.
    fn apply(&mut self, msg: OutputMsg, stdout_sink: &SinkHandle) -> bool {
        match msg {
            OutputMsg::CloseFollowSinks => self.sinks.retain(|s| !s.is_transient()),
            OutputMsg::AddSink(sink) => self.sinks.push(sink),
            OutputMsg::AddSinkOnce(sink) => {
                if !self.sinks.iter().any(|s| s.same_channel(&sink)) {
                    self.sinks.push(sink);
                }
            }
            OutputMsg::RemoveSink(sink) => self.sinks.retain(|s| !s.same_channel(&sink)),
            OutputMsg::PauseStdout => self.pause_stdout(stdout_sink),
            OutputMsg::ResumeStdout => self.resume_stdout(stdout_sink),
            OutputMsg::SetAttachPty(pty) => self.attach_pty = pty,
            OutputMsg::ClearAttach => {
                self.attach_pty = None;
                self.attach_clients = 0;
                self.resume_stdout(stdout_sink);
            }
            OutputMsg::Attach { reply } => {
                let answer = self.attach_pty.clone().map(|pty| {
                    self.attach_clients += 1;
                    if self.attach_clients == 1 {
                        self.pause_stdout(stdout_sink);
                    }
                    (pty, self.attach_clients)
                });
                let _ = reply.send(answer);
            }
            OutputMsg::Detach { reply } => {
                let answer = if self.attach_clients == 0 {
                    None
                } else {
                    self.attach_clients -= 1;
                    if self.attach_clients == 0 {
                        self.resume_stdout(stdout_sink);
                    }
                    Some(self.attach_clients)
                };
                let _ = reply.send(answer);
            }
            OutputMsg::ReadLogs { n, reply } => {
                let mut result: Vec<u8> = Vec::new();
                for part in self.ring_buffer.last_n(n) {
                    result.extend_from_slice(part);
                }
                // Strip the trailing `\n` for clean output.
                if result.last() == Some(&b'\n') {
                    result.pop();
                }
                let _ = reply.send(Bytes::from(result));
            }
            OutputMsg::FollowSink {
                last_n,
                live_capacity,
                reply,
            } => {
                // Capacity must hold the preloaded snapshot *and* live
                // headroom, or a freshly-connected client is dropped for
                // being slow before it has read anything.
                let capacity = last_n.saturating_add(live_capacity).max(1);
                let (tx, rx) = mpsc::channel::<SinkLine>(capacity);
                for line in self.ring_buffer.last_n(last_n) {
                    // Ring-buffer entries keep their trailing `\n`, but
                    // `SinkLine.line` is contractually newline-free.
                    let line = line.strip_suffix(b"\n").unwrap_or(line);
                    // The channel is empty and has `capacity` slots.
                    let _ = tx.try_send(SinkLine {
                        prefix: self.prefix.clone(),
                        line: Bytes::copy_from_slice(line),
                        name: self.name.clone(),
                        is_lifecycle: false,
                        is_verbose: false,
                    });
                }
                self.sinks.push(SinkHandle::BoundedDrop(tx));
                let _ = reply.send(rx);
            }
            OutputMsg::RepaintSink {
                frame,
                capacity,
                reply,
            } => {
                let (tx, rx) = mpsc::channel::<SinkLine>(capacity);
                // Channel is empty and capacity >= 2, so this cannot fail.
                let _ = tx.try_send(SinkLine {
                    prefix: Bytes::new(),
                    line: frame,
                    name: self.name.clone(),
                    is_lifecycle: false,
                    is_verbose: false,
                });
                self.sinks.push(SinkHandle::BoundedDrop(tx));
                let _ = reply.send(rx);
            }
            OutputMsg::ClearSinks => {
                self.sinks.clear();
                return false;
            }
            #[cfg(test)]
            OutputMsg::SinkCount { reply } => {
                let _ = reply.send(self.sinks.len());
            }
        }
        true
    }
}
