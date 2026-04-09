//! Output handling — line buffering, color-coded prefixing, and lifecycle events.
//!
//! Each output destination (stdout, log files) is a **sink** — an independent
//! writer task with an `mpsc` channel. Services and lifecycle events send lines
//! to sinks via the channel sender. A single stdout sink task ensures no
//! interleaving. Sinks can be added/removed at runtime for `don logs` tailing.
//!
//! Each service has a [`ServiceWriter`] handle that pushes lines to the per-service
//! ring buffer and fans out to the service's current sinks. The ring buffer persists
//! across restarts, and [`ServiceWriter`] is cloneable for reuse.

pub(crate) mod osc;
pub mod ring_buffer;
pub(crate) mod sanitize;

use bytes::Bytes;
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use ring_buffer::RingBuffer;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

/// Default ring buffer capacity per service (lines).
const DEFAULT_RING_BUFFER_CAPACITY: usize = 10_000;

/// ASCII BEL character — emitted on error events for an audible alert.
const BELL: &str = "\x07";

/// Default channel capacity for sinks.
const SINK_CHANNEL_CAPACITY: usize = 1000;

/// Terminal colors for service name prefixes.
const COLORS: &[Color] = &[
    Color::Cyan,
    Color::Yellow,
    Color::Magenta,
    Color::Green,
    Color::Blue,
    Color::Red,
    Color::DarkCyan,
    Color::DarkYellow,
    Color::DarkMagenta,
    Color::DarkGreen,
    Color::DarkBlue,
    Color::DarkRed,
];

/// Handle to an active OSC response sink. Use [`take_pty_write`] to
/// stop the sink and reclaim the PTY handle (e.g., for attach).
pub struct OscSinkHandle {
    /// Our copy of the sender — dropping it + removing the service's
    /// copy from the sinks list closes the channel, stopping the task.
    tx: mpsc::Sender<SinkLine>,
    join: JoinHandle<pty_process::OwnedWritePty>,
    service_state: Arc<Mutex<ServiceOutputState>>,
}

impl OscSinkHandle {
    /// Stop the OSC sink and reclaim the PTY write handle.
    /// Removes the sink from the service's sinks list, closes the channel,
    /// and waits for the task to return the handle.
    pub async fn take_pty_write(self) -> Option<pty_process::OwnedWritePty> {
        // Remove our sender from the service's sinks list.
        {
            let mut state = self.service_state.lock().await;
            state
                .sinks
                .retain(|s| !s.tx.same_channel(&self.tx));
        }
        // Drop our sender to close the channel.
        drop(self.tx);
        self.join.await.ok()
    }
}

/// A message to a sink. Sinks receive these and write them to their destination.
pub struct SinkLine {
    /// Formatted prefix (color-coded service name, or bold [don] for lifecycle).
    /// Empty for file sinks (raw output).
    pub prefix: Bytes,
    /// The raw line content (no newline).
    pub line: Bytes,
}

/// A handle to a sink. Clone the sender to subscribe a service to it.
#[derive(Clone)]
pub(crate) struct SinkHandle {
    pub tx: mpsc::Sender<SinkLine>,
    /// When true, use `try_send` and drop the sink if the channel is full.
    /// Used for follow sinks so that a slow HTTP client can't block the
    /// service's output pipeline.
    pub drop_on_full: bool,
}

/// Per-service output state. Owned by OutputManager, never removed.
struct ServiceOutputState {
    prefix: Bytes,
    ring_buffer: RingBuffer,
    /// Dynamic list of sinks this service writes to.
    sinks: Vec<SinkHandle>,
    /// True while the stdout sink is temporarily removed (during attach).
    /// Used to ensure `resume_stdout_sink` only restores it if it was
    /// actually present before the pause.
    stdout_paused: bool,
}

/// Per-service handle for writing output. Cloneable, reusable across restarts.
///
/// Holds an `Arc` to the service's state in `OutputManager`. Multiple
/// writers can be created for the same service (e.g. on restart), all
/// sharing the same ring buffer.
#[derive(Clone)]
pub struct ServiceWriter {
    state: Arc<Mutex<ServiceOutputState>>,
}

impl ServiceWriter {
    /// Process an async readable stream (from a child process) as raw chunks.
    ///
    /// Reads raw byte chunks and broadcasts them to all sinks. Each sink
    /// decides its own buffering strategy (the stdout sink accumulates
    /// per-service until `\n`, the ring buffer splits on `\n`, attach/follow
    /// sinks forward immediately, the OSC sink detects terminal queries and
    /// writes responses). No UTF-8 assumption — binary output is handled
    /// correctly. Runs until EOF (the child closes its output).
    pub async fn process_stream<R: AsyncRead + Unpin>(
        &self,
        mut reader: R,
    ) -> Result<(), OutputError> {
        let mut buf = [0u8; 8192];

        loop {
            match read_chunk(&mut reader, &mut buf).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    let chunk = Bytes::copy_from_slice(&buf[..n]);

                    // Lock: push to ring buffer + snapshot sinks. Released before sends.
                    // Prune closed sinks (e.g. disconnected follow clients) inline.
                    let (prefix, sinks) = {
                        let mut state = self.state.lock().await;
                        state.sinks.retain(|s| !s.tx.is_closed());
                        state.ring_buffer.push_chunk(&chunk);
                        (state.prefix.clone(), state.sinks.clone())
                    };

                    let mut dropped: Vec<mpsc::Sender<SinkLine>> = Vec::new();
                    for sink in &sinks {
                        let msg = SinkLine {
                            prefix: prefix.clone(),
                            line: chunk.clone(),
                        };
                        if sink.drop_on_full {
                            // Non-blocking: if the client can't keep up, drop
                            // the sink so the service's output isn't stalled.
                            if sink.tx.try_send(msg).is_err() {
                                dropped.push(sink.tx.clone());
                            }
                        } else {
                            let _ = sink.tx.send(msg).await;
                        }
                    }
                    if !dropped.is_empty() {
                        let mut state = self.state.lock().await;
                        state
                            .sinks
                            .retain(|s| !dropped.iter().any(|d| d.same_channel(&s.tx)));
                    }
                }
                Err(e) => return Err(e),
            }
        }

        // Flush any partial line remaining in the ring buffer.
        {
            let mut state = self.state.lock().await;
            state.ring_buffer.flush_pending();
        }

        Ok(())
    }

    /// Close all transient (follow/attach) sinks. Called when the process
    /// stream ends (process exited) so that attach sessions and log followers
    /// detect the closure and exit instead of blocking forever.
    ///
    /// Only removes sinks with `drop_on_full = true` (follow/attach sinks).
    /// Persistent sinks (stdout, file) are kept for the next process lifecycle.
    pub async fn close_follow_sinks(&self) {
        let mut state = self.state.lock().await;
        state.sinks.retain(|s| !s.drop_on_full);
    }

    /// Write a single line to the ring buffer and sinks.
    ///
    /// Used for structured output like Docker build progress that arrives
    /// as individual text lines rather than a byte stream. Appends `\n` to
    /// the data so sinks can flush immediately.
    pub async fn write_line(&self, line: &str) {
        let data = Bytes::from(format!("{line}\n"));
        let (prefix, sinks) = {
            let mut state = self.state.lock().await;
            state.sinks.retain(|s| !s.tx.is_closed());
            state.ring_buffer.push_chunk(data.as_ref());
            (state.prefix.clone(), state.sinks.clone())
        };
        let mut dropped: Vec<mpsc::Sender<SinkLine>> = Vec::new();
        for sink in &sinks {
            let msg = SinkLine {
                prefix: prefix.clone(),
                line: data.clone(),
            };
            if sink.drop_on_full {
                if sink.tx.try_send(msg).is_err() {
                    dropped.push(sink.tx.clone());
                }
            } else {
                let _ = sink.tx.send(msg).await;
            }
        }
        if !dropped.is_empty() {
            let mut state = self.state.lock().await;
            state
                .sinks
                .retain(|s| !dropped.iter().any(|d| d.same_channel(&s.tx)));
        }
    }
}

/// Assigns a deterministic color index to a service name.
fn assign_colors(names: &[&str]) -> HashMap<String, usize> {
    let mut sorted: Vec<&str> = names.to_vec();
    sorted.sort();
    sorted
        .into_iter()
        .enumerate()
        .map(|(i, name)| (name.to_string(), i % COLORS.len()))
        .collect()
}

/// Build a formatted prefix as bytes for a service name.
fn format_prefix(name: &str, color_index: usize, max_name_len: usize) -> Bytes {
    let color = COLORS[color_index % COLORS.len()];
    Bytes::from(format!(
        "{}{:width$}{} | ",
        SetForegroundColor(color),
        name,
        ResetColor,
        width = max_name_len,
    ))
}

/// Manages output for all services — creates sinks, spawns writer tasks,
/// and provides lifecycle event formatting.
pub struct OutputManager {
    /// Per-service output state, retained for the lifetime of the program.
    services: HashMap<String, Arc<Mutex<ServiceOutputState>>>,
    /// The formatted `[don]` prefix, padded to align with service prefixes.
    don_prefix: String,
    /// Stdout sink sender — used for lifecycle events and service output.
    stdout_sink: SinkHandle,
    /// Writer task JoinHandles for clean shutdown.
    writer_handles: Vec<JoinHandle<()>>,
    /// Verbose mode — enables extra diagnostic lifecycle events.
    verbose: bool,
}

/// Errors from output handling.
#[derive(Debug, thiserror::Error)]
pub enum OutputError {
    /// Failed to open a log file.
    #[error("failed to open log file '{path}': {source}")]
    FileOpen {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// I/O error reading from child output.
    #[error("error reading service output: {0}")]
    Read(#[from] std::io::Error),
}

impl OutputManager {
    /// Create a new output manager for the given services.
    ///
    /// `writer` is the stdout destination — `std::io::stdout()` in production,
    /// a test buffer in tests. It is consumed by a spawned writer task.
    /// Colors are assigned deterministically based on sorted service names.
    /// Prefixes are padded to the longest name for column alignment.
    pub async fn new<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        writer: W,
    ) -> Result<Self, OutputError> {
        Self::new_verbose(services, writer, false).await
    }

    /// Create a new output manager. When `verbose` is true, every output
    /// line is prefixed with an elapsed timestamp.
    pub async fn new_verbose<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        writer: W,
        verbose: bool,
    ) -> Result<Self, OutputError> {
        let names: Vec<&str> = services.iter().map(|(n, _)| *n).collect();
        let color_map = assign_colors(&names);
        let max_name_len = names.iter().map(|n| n.len()).max().unwrap_or(0).max(5);

        // Spawn stdout sink task.
        let (stdout_tx, stdout_rx) = mpsc::channel(SINK_CHANNEL_CAPACITY);
        let stdout_handle = tokio::spawn(stdout_sink_task(stdout_rx, writer, verbose));
        let stdout_sink = SinkHandle { tx: stdout_tx, drop_on_full: false };

        // Spawn file sink tasks (deduplicated by path).
        let mut file_sinks: HashMap<PathBuf, SinkHandle> = HashMap::new();
        let mut writer_handles = vec![stdout_handle];

        for (_, config) in services {
            if let crate::config::LogConfig::File(path) = config
                && !file_sinks.contains_key(path)
            {
                let file = open_log_file(path).await?;
                let (tx, rx) = mpsc::channel(SINK_CHANNEL_CAPACITY);
                writer_handles.push(tokio::spawn(file_sink_task(rx, file)));
                file_sinks.insert(path.clone(), SinkHandle { tx, drop_on_full: false });
            }
        }

        // Build per-service state.
        let mut service_map = HashMap::new();
        for (name, config) in services {
            let sinks = match config {
                crate::config::LogConfig::Stdout => vec![stdout_sink.clone()],
                crate::config::LogConfig::File(path) => {
                    // File sink always gets the raw line. The stdout sink is not
                    // added for file-mode services (output goes to file only).
                    match file_sinks.get(path).cloned() {
                        Some(sink) => vec![sink],
                        None => vec![], // Shouldn't happen — file sinks are created from the same config.
                    }
                }
                crate::config::LogConfig::Ignore => vec![],
            };

            let color_idx = color_map.get(*name).copied().unwrap_or(0);
            let prefix = format_prefix(name, color_idx, max_name_len);

            service_map.insert(
                name.to_string(),
                Arc::new(Mutex::new(ServiceOutputState {
                    prefix,
                    ring_buffer: RingBuffer::new(DEFAULT_RING_BUFFER_CAPACITY),
                    sinks,
                    stdout_paused: false,
                })),
            );
        }

        let don_prefix = format!(
            "{}{:width$}{} | ",
            SetAttribute(Attribute::Bold),
            "[don]",
            SetAttribute(Attribute::Reset),
            width = max_name_len,
        );

        Ok(Self {
            services: service_map,
            don_prefix,
            stdout_sink,
            writer_handles,
            verbose,
        })
    }

    /// Get a writer handle for a service. Cloneable, reusable across restarts.
    ///
    /// Returns `None` if the service name is not registered.
    /// Can be called multiple times — each call returns a new handle
    /// pointing to the same underlying ring buffer and sinks.
    pub fn service_writer(&self, name: &str) -> Option<ServiceWriter> {
        self.services
            .get(name)
            .cloned()
            .map(|state| ServiceWriter { state })
    }

    /// Attach a follow sink: a freshly-created mpsc channel preloaded with
    /// the last N buffered lines, then registered as a sink for this service.
    /// New lines are delivered until the receiver is dropped (or the client
    /// is too slow and the sink's buffer fills — it then gets disconnected).
    ///
    /// `live_capacity` is the headroom for live lines *on top of* the preloaded
    /// snapshot, so slow readers don't block immediately after connection.
    ///
    /// Returns `None` if the service name is unknown.
    pub async fn add_follow_sink(
        &self,
        name: &str,
        last_n: usize,
        live_capacity: usize,
    ) -> Option<mpsc::Receiver<SinkLine>> {
        let state_arc = self.services.get(name)?.clone();
        // Channel must hold the preloaded snapshot AND live headroom without
        // blocking (or dropping the freshly-connected client immediately).
        let capacity = last_n.saturating_add(live_capacity).max(1);
        let (tx, rx) = mpsc::channel::<SinkLine>(capacity);
        let mut state = state_arc.lock().await;
        let prefix = state.prefix.clone();
        // Preload last N ring buffer lines. Channel has `capacity` slots and
        // is empty, so try_send is safe here.
        for line in state.ring_buffer.last_n(last_n) {
            let sink_line = SinkLine {
                prefix: prefix.clone(),
                line: Bytes::copy_from_slice(line),
            };
            if tx.try_send(sink_line).is_err() {
                break;
            }
        }
        state.sinks.push(SinkHandle { tx, drop_on_full: true });
        Some(rx)
    }

    /// Add an OSC response sink to a service. The sink scans each chunk for
    /// terminal queries (OSC 10/11, cursor position) and writes responses
    /// directly to the PTY write handle.
    ///
    /// The sink uses `drop_on_full = true` so it never blocks the output
    /// pipeline. Returns a [`OscSinkHandle`] that can be used to reclaim
    /// the PTY write handle (e.g., for attach).
    pub async fn add_osc_sink(
        &self,
        name: &str,
        pty_write: pty_process::OwnedWritePty,
    ) -> Option<OscSinkHandle> {
        let state_arc = self.services.get(name)?.clone();
        let (tx, rx) = mpsc::channel::<SinkLine>(16);
        {
            let mut state = state_arc.lock().await;
            state.sinks.push(SinkHandle {
                tx: tx.clone(),
                drop_on_full: true,
            });
        }
        let join = tokio::spawn(osc_sink_task(rx, pty_write));
        Some(OscSinkHandle {
            tx,
            join,
            service_state: state_arc,
        })
    }

    /// Read the last N lines from a service's ring buffer, joined by newlines.
    ///
    /// Returns `None` if the service is not registered.
    pub async fn read_logs(&self, name: &str, n: usize) -> Option<Bytes> {
        let state_arc = self.services.get(name)?;
        let state = state_arc.lock().await;
        let parts: Vec<&[u8]> = state.ring_buffer.last_n(n).collect();
        // Entries include `\n` delimiters — concatenate directly.
        let mut result: Vec<u8> = Vec::new();
        for part in &parts {
            result.extend_from_slice(part);
        }
        // Strip trailing `\n` for clean output.
        if result.last() == Some(&b'\n') {
            result.pop();
        }
        Some(Bytes::from(result))
    }

    /// Register a new service that wasn't in the original config (added via
    /// live config reload). Creates a ring buffer, assigns a color, and wires
    /// up sinks based on the log config. Existing services are left unchanged.
    pub async fn register_service(&mut self, name: &str, log_config: &crate::config::LogConfig) {
        if self.services.contains_key(name) {
            return;
        }
        // Determine max_name_len from existing prefix width. New services use
        // the wider of the current alignment and their own name length.
        let current_max = self
            .services
            .keys()
            .map(|n| n.len())
            .max()
            .unwrap_or(0)
            .max(5);
        let max_name_len = current_max.max(name.len());
        let color_idx = self.services.len() % COLORS.len();
        let prefix = format_prefix(name, color_idx, max_name_len);

        let sinks = match log_config {
            crate::config::LogConfig::Stdout => vec![self.stdout_sink.clone()],
            crate::config::LogConfig::File(_) => {
                // For simplicity, new file-mode services log to stdout.
                // Full file-sink creation would require opening the file and
                // spawning a task, which can be added later if needed.
                vec![self.stdout_sink.clone()]
            }
            crate::config::LogConfig::Ignore => vec![],
        };

        self.services.insert(
            name.to_string(),
            Arc::new(Mutex::new(ServiceOutputState {
                prefix,
                ring_buffer: RingBuffer::new(DEFAULT_RING_BUFFER_CAPACITY),
                sinks,
                stdout_paused: false,
            })),
        );
    }

    /// Get a lightweight, cloneable handle for emitting `[don]` lifecycle
    /// events from spawned tasks (e.g. build output).
    pub fn clone_lifecycle_emitter(&self) -> LifecycleEmitter {
        LifecycleEmitter {
            don_prefix: self.don_prefix.clone(),
            stdout_sink: self.stdout_sink.clone(),
        }
    }

    /// Emit a `[don]` lifecycle event.
    pub fn lifecycle_event(&self, message: &str) {
        let _ = self.stdout_sink.tx.try_send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
        });
    }

    /// Emit a `[don]` lifecycle event only when verbose mode is enabled.
    pub fn debug_event(&self, message: &str) {
        if self.verbose {
            self.lifecycle_event(message);
        }
    }

    /// Emit a `[don]` lifecycle event for a specific service.
    pub fn service_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.tx.try_send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
        });
    }

    /// Emit a `[don]` error event with a terminal bell.
    pub fn error_event(&self, message: &str) {
        let _ = self.stdout_sink.tx.try_send(SinkLine {
            prefix: Bytes::from(format!("{BELL}{}", self.don_prefix)),
            line: Bytes::from(format!("{message}\n")),
        });
    }

    /// Emit a `[don]` error event for a specific service with a terminal bell.
    pub fn service_error_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.tx.try_send(SinkLine {
            prefix: Bytes::from(format!("{BELL}{}", self.don_prefix)),
            line: Bytes::from(format!("{service}: {message}\n")),
        });
    }

    /// Temporarily remove the stdout sink from a service so its output
    /// doesn't appear in the don terminal (e.g. during interactive attach).
    /// The ring buffer continues to be fed. No-op if the service is unknown
    /// or the stdout sink is not present.
    pub async fn pause_stdout_sink(&self, name: &str) {
        if let Some(state_arc) = self.services.get(name) {
            let mut state = state_arc.lock().await;
            let had_stdout = state
                .sinks
                .iter()
                .any(|s| s.tx.same_channel(&self.stdout_sink.tx));
            if had_stdout {
                state
                    .sinks
                    .retain(|s| !s.tx.same_channel(&self.stdout_sink.tx));
                state.stdout_paused = true;
            }
        }
    }

    /// Re-add the stdout sink to a service after an attach session ends.
    /// Only restores the sink if it was previously paused via
    /// `pause_stdout_sink` — services with `log = "ignore"` won't
    /// accidentally start writing to stdout.
    pub async fn resume_stdout_sink(&self, name: &str) {
        if let Some(state_arc) = self.services.get(name) {
            let mut state = state_arc.lock().await;
            if state.stdout_paused {
                state.stdout_paused = false;
                let already_present = state
                    .sinks
                    .iter()
                    .any(|s| s.tx.same_channel(&self.stdout_sink.tx));
                if !already_present {
                    state.sinks.push(self.stdout_sink.clone());
                }
            }
        }
    }

    /// Shut down the output system. Clears all sink lists, drops senders,
    /// and waits for writer tasks to drain remaining messages.
    pub async fn shutdown(self) {
        // Clear each service's sink list so outstanding ServiceWriter handles
        // don't keep channel senders alive.
        for state_arc in self.services.values() {
            let mut state = state_arc.lock().await;
            state.sinks.clear();
        }
        // Drop all senders (stdout_sink + services map) to close channels.
        drop(self.stdout_sink);
        drop(self.services);
        // Wait for writer tasks to finish draining.
        for handle in self.writer_handles {
            let _ = handle.await;
        }
    }
}

/// A lightweight, cloneable handle for emitting `[don]` lifecycle events
/// from spawned tasks. Does not carry the full `OutputManager` state.
#[derive(Clone)]
pub struct LifecycleEmitter {
    don_prefix: String,
    stdout_sink: SinkHandle,
}

impl LifecycleEmitter {
    /// Emit a `[don]` lifecycle event.
    pub fn lifecycle_event(&self, message: &str) {
        let _ = self.stdout_sink.tx.try_send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
        });
    }
}

/// Stdout sink writer task. Receives raw byte chunks and accumulates
/// per-service until `\n` or overflow, then writes the formatted line.
///
/// Each service's partial output is buffered independently so that
/// interleaved chunks from different services don't produce garbled output.
/// Runs until all senders are dropped.
async fn stdout_sink_task<W: tokio::io::AsyncWrite + Unpin + Send>(
    mut rx: mpsc::Receiver<SinkLine>,
    mut writer: W,
    verbose: bool,
) {
    use bytes::BytesMut;

    let start = std::time::Instant::now();
    /// Maximum bytes to accumulate per-service before forcing a flush.
    const MAX_LINE: usize = 16 * 1024;

    // Per-service line accumulator, keyed by prefix bytes.
    let mut accumulators: HashMap<Bytes, BytesMut> = HashMap::new();
    // Track which accumulators just flushed via \r. When a \n immediately
    // follows a \r, the resulting empty line is suppressed — the \r already
    // flushed the content.
    let mut cr_flushed: HashSet<Bytes> = HashSet::new();

    while let Some(msg) = rx.recv().await {
        let acc = accumulators.entry(msg.prefix.clone()).or_default();

        for &byte in msg.line.iter() {
            acc.extend_from_slice(&[byte]);
            if byte == b'\n' {
                // Complete line — strip \r\n, sanitize, write prefixed output.
                acc.truncate(acc.len() - 1); // remove \n
                if acc.last() == Some(&b'\r') {
                    acc.truncate(acc.len() - 1); // remove \r
                }
                // Suppress empty lines that follow a \r flush — the content
                // was already written when \r was processed.
                let is_empty_after_cr = acc.is_empty() && cr_flushed.remove(&msg.prefix);
                if !is_empty_after_cr {
                    cr_flushed.remove(&msg.prefix);
                    let sanitized = if msg.prefix.is_empty() {
                        acc.to_vec()
                    } else {
                        sanitize::sanitize_terminal_output(acc)
                    };
                    write_prefixed_line(&mut writer, &msg.prefix, &sanitized, verbose, start).await;
                }
                acc.clear();
            } else if byte == b'\r' {
                // Bare carriage return (no \n) — programs like Bazel use
                // \r to overwrite progress lines in-place. Treat as a line
                // boundary so each progress update gets prefixed correctly.
                acc.truncate(acc.len() - 1); // remove \r
                if !acc.is_empty() {
                    let sanitized = if msg.prefix.is_empty() {
                        acc.to_vec()
                    } else {
                        sanitize::sanitize_terminal_output(acc)
                    };
                    write_prefixed_line(
                        &mut writer, &msg.prefix, &sanitized, verbose, start,
                    )
                    .await;
                }
                acc.clear();
                cr_flushed.insert(msg.prefix.clone());
            } else {
                // Non-control byte — any pending \r suppression is stale.
                cr_flushed.remove(&msg.prefix);
                if acc.len() >= MAX_LINE {
                    // Overflow — flush without stripping.
                    let sanitized = if msg.prefix.is_empty() {
                        acc.to_vec()
                    } else {
                        sanitize::sanitize_terminal_output(acc)
                    };
                    write_prefixed_line(&mut writer, &msg.prefix, &sanitized, verbose, start).await;
                    acc.clear();
                }
            }
        }
    }

    // Flush remaining accumulators on shutdown.
    for (prefix, acc) in &accumulators {
        if !acc.is_empty() {
            let sanitized = if prefix.is_empty() {
                acc.to_vec()
            } else {
                sanitize::sanitize_terminal_output(acc)
            };
            write_prefixed_line(&mut writer, prefix, &sanitized, verbose, start).await;
        }
    }
}

/// Write a single prefixed, sanitized line to the writer.
async fn write_prefixed_line<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    prefix: &[u8],
    line: &[u8],
    verbose: bool,
    start: std::time::Instant,
) {
    use tokio::io::AsyncWriteExt;
    if verbose {
        let elapsed = start.elapsed();
        let ts = format!("{:.3}s ", elapsed.as_secs_f64());
        let _ = writer.write_all(ts.as_bytes()).await;
    }
    let _ = writer.write_all(prefix).await;
    let _ = writer.write_all(line).await;
    let _ = writer.write_all(b"\n").await;
}

/// File sink writer task. Receives raw byte chunks and writes them directly.
/// Runs until all senders are dropped.
async fn file_sink_task(mut rx: mpsc::Receiver<SinkLine>, mut file: tokio::fs::File) {
    use tokio::io::AsyncWriteExt;
    while let Some(msg) = rx.recv().await {
        let _ = file.write_all(&msg.line).await;
    }
}

/// OSC response sink task. Scans each chunk for terminal queries and
/// writes responses directly to the PTY write handle. Returns the PTY
/// write handle when the channel closes (process exit or sink removal)
/// so it can be reclaimed by the caller.
async fn osc_sink_task(
    mut rx: mpsc::Receiver<SinkLine>,
    mut pty_write: pty_process::OwnedWritePty,
) -> pty_process::OwnedWritePty {
    use tokio::io::AsyncWriteExt;
    while let Some(msg) = rx.recv().await {
        for response in osc::find_responses(&msg.line) {
            let _ = pty_write.write_all(response).await;
        }
    }
    pty_write
}

/// Open a log file for appending, creating parent directories as needed.
async fn open_log_file(path: &std::path::Path) -> Result<tokio::fs::File, OutputError> {
    if let Some(parent) = path.parent() {
        let os_str = parent.as_os_str();
        if !os_str.is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| OutputError::FileOpen {
                    path: path.to_path_buf(),
                    source,
                })?;
        }
    }
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|source| OutputError::FileOpen {
            path: path.to_path_buf(),
            source,
        })
}

/// Read a chunk of bytes from the reader.
///
/// Returns the number of bytes read (0 = EOF).
/// For PTY reads, an `EIO` error signals the child exited — treated as EOF.
async fn read_chunk<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> Result<usize, OutputError> {
    match reader.read(buf).await {
        Ok(n) => Ok(n),
        Err(e) if e.raw_os_error() == Some(libc::EIO) => Ok(0),
        Err(e) => Err(OutputError::Read(e)),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A test buffer that implements AsyncWrite and allows reading back contents.
    #[derive(Clone)]
    struct TestBuffer(Arc<Mutex<Vec<u8>>>);

    impl TestBuffer {
        fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
            let buf = Arc::new(Mutex::new(Vec::new()));
            (TestBuffer(buf.clone()), buf)
        }
    }

    impl tokio::io::AsyncWrite for TestBuffer {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            data: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.0.lock().unwrap().extend_from_slice(data);
            std::task::Poll::Ready(Ok(data.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// Read the test buffer as a string.
    fn read_buf(buf: &Arc<Mutex<Vec<u8>>>) -> String {
        String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
    }

    /// Strip ANSI escape sequences from bytes.
    fn strip_ansi(s: &[u8]) -> String {
        let mut result = Vec::with_capacity(s.len());
        let mut i = 0;
        while i < s.len() {
            if s[i] == b'\x1b' {
                i += 1;
                while i < s.len() && s[i] != b'm' {
                    i += 1;
                }
                i += 1;
            } else {
                result.push(s[i]);
                i += 1;
            }
        }
        String::from_utf8_lossy(&result).into_owned()
    }

    #[test]
    fn test_color_assignment_deterministic() {
        struct Case {
            name: &'static str,
            names: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "same names, same order",
                names: vec!["api", "worker", "postgres"],
            },
            Case {
                name: "same names, different order",
                names: vec!["worker", "postgres", "api"],
            },
        ];

        let mut prev_result: Option<HashMap<String, usize>> = None;
        for case in &cases {
            let result = assign_colors(&case.names);
            if let Some(ref prev) = prev_result {
                assert_eq!(
                    &result, prev,
                    "case: {} — color assignment should be deterministic",
                    case.name
                );
            }
            prev_result = Some(result);
        }
    }

    #[test]
    fn test_color_assignment_distinct() {
        let names = vec!["a", "b", "c", "d"];
        let result = assign_colors(&names);
        let indices: Vec<usize> = {
            let mut sorted_names: Vec<&str> = names.clone();
            sorted_names.sort();
            sorted_names.iter().map(|n| result[*n]).collect()
        };
        let unique: std::collections::HashSet<usize> = indices.iter().copied().collect();
        assert_eq!(unique.len(), indices.len());
    }

    #[test]
    fn test_color_assignment_wraps() {
        let names: Vec<String> = (0..COLORS.len() + 3).map(|i| format!("svc{i}")).collect();
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let result = assign_colors(&name_refs);
        assert_eq!(result.len(), names.len());
    }

    #[test]
    fn test_prefix_alignment() {
        struct Case {
            name: &'static str,
            service_name: &'static str,
            max_len: usize,
        }

        let cases = vec![
            Case {
                name: "short padded",
                service_name: "api",
                max_len: 8,
            },
            Case {
                name: "exact",
                service_name: "postgres",
                max_len: 8,
            },
            Case {
                name: "single char",
                service_name: "a",
                max_len: 8,
            },
        ];

        for case in cases {
            let prefix = format_prefix(case.service_name, 0, case.max_len);
            let stripped = strip_ansi(&prefix);
            let expected = format!("{:width$} | ", case.service_name, width = case.max_len);
            assert_eq!(stripped, expected, "case: {}", case.name);
        }
    }

    #[tokio::test]
    async fn test_line_buffering_complete_lines() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("api").unwrap();

        let data = b"hello world\nsecond line\n";
        let cursor = std::io::Cursor::new(data.to_vec());
        svc.process_stream(cursor).await.unwrap();

        let logs = mgr.read_logs("api", 10).await.unwrap();
        assert_eq!(logs.as_ref(), b"hello world\nsecond line");

        mgr.shutdown().await;
        let output = read_buf(&buf);
        assert!(output.contains("hello world"), "should contain first line");
        assert!(output.contains("second line"), "should contain second line");
    }

    #[tokio::test]
    async fn test_line_buffering_no_trailing_newline() {
        let (writer, _buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("svc", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("svc").unwrap();

        let data = b"line one\npartial";
        let cursor = std::io::Cursor::new(data.to_vec());
        svc.process_stream(cursor).await.unwrap();

        let logs = mgr.read_logs("svc", 10).await.unwrap();
        assert_eq!(logs.as_ref(), b"line one\npartial");

        mgr.shutdown().await;
    }

    #[tokio::test]
    async fn test_non_utf8_output() {
        let (writer, _buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("bin", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("bin").unwrap();

        let data: Vec<u8> = vec![0xff, 0xfe, b'h', b'i', b'\n', 0x80, 0x81, b'\n'];
        let cursor = std::io::Cursor::new(data);
        svc.process_stream(cursor).await.unwrap();

        let logs = mgr.read_logs("bin", 10).await.unwrap();
        let expected: Vec<u8> = vec![0xff, 0xfe, b'h', b'i', b'\n', 0x80, 0x81];
        assert_eq!(logs.as_ref(), expected.as_slice());

        mgr.shutdown().await;
    }

    #[tokio::test]
    async fn test_service_writer_reusable() {
        let (writer, _buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();

        // Can get multiple writers for the same service.
        let w1 = mgr.service_writer("api");
        let w2 = mgr.service_writer("api");
        assert!(w1.is_some());
        assert!(w2.is_some());

        // Both share the same ring buffer.
        let w1 = w1.unwrap();
        let data = std::io::Cursor::new(b"from w1\n".to_vec());
        w1.process_stream(data).await.unwrap();

        let logs = mgr.read_logs("api", 10).await.unwrap();
        assert_eq!(logs.as_ref(), b"from w1");

        mgr.shutdown().await;
    }

    #[tokio::test]
    async fn test_unknown_service_returns_none() {
        let (writer, _buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();
        assert!(mgr.service_writer("nonexistent").is_none());
        mgr.shutdown().await;
    }

    #[tokio::test]
    async fn test_ignore_mode_no_stdout() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Ignore;
        let mgr = OutputManager::new(&[("quiet", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("quiet").unwrap();

        let data = b"secret\n";
        let cursor = std::io::Cursor::new(data.to_vec());
        svc.process_stream(cursor).await.unwrap();

        // Ring buffer should have the line.
        let logs = mgr.read_logs("quiet", 10).await.unwrap();
        assert_eq!(logs.as_ref(), b"secret");

        mgr.shutdown().await;

        // Stdout should be empty.
        let output = read_buf(&buf);
        assert!(output.is_empty(), "ignore mode should not write to stdout");
    }

    #[tokio::test]
    async fn test_lifecycle_event_format() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("postgres", &config)], writer)
            .await
            .unwrap();

        mgr.lifecycle_event("loading don.toml");
        mgr.shutdown().await;

        let output = read_buf(&buf);
        let stripped = strip_ansi(output.as_bytes());
        assert!(stripped.contains("[don]") && stripped.contains("loading don.toml"));
    }

    #[tokio::test]
    async fn test_error_event_includes_bell() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();

        mgr.error_event("build failed");
        mgr.shutdown().await;

        let output = read_buf(&buf);
        assert!(output.contains(BELL), "error events should include bell");
    }

    #[tokio::test]
    async fn test_empty_service_list() {
        let (writer, buf) = TestBuffer::new();
        let mgr = OutputManager::new(&[], writer).await.unwrap();
        mgr.lifecycle_event("hello");
        mgr.shutdown().await;

        let output = read_buf(&buf);
        let stripped = strip_ansi(output.as_bytes());
        assert!(stripped.contains("[don]") && stripped.contains("hello"));
    }

    #[tokio::test]
    async fn test_concurrent_services_both_write() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("alpha", &config), ("beta", &config)], writer)
            .await
            .unwrap();

        let alpha = mgr.service_writer("alpha").unwrap();
        let beta = mgr.service_writer("beta").unwrap();

        let (r_a, r_b) = tokio::join!(
            alpha.process_stream(std::io::Cursor::new(b"alpha line\n".to_vec())),
            beta.process_stream(std::io::Cursor::new(b"beta line\n".to_vec())),
        );
        r_a.unwrap();
        r_b.unwrap();

        mgr.shutdown().await;

        let output = read_buf(&buf);
        assert!(output.contains("alpha"), "should have alpha output");
        assert!(output.contains("beta"), "should have beta output");
    }
}
