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

pub(crate) mod emulator;
pub(crate) mod osc;
pub(crate) mod ring_buffer;
pub(crate) mod sanitize;

use bytes::{Bytes, BytesMut};
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use ring_buffer::RingBuffer;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{Mutex, broadcast, mpsc, watch};
use tokio::task::JoinHandle;

/// Default ring buffer capacity per service (lines).
const DEFAULT_RING_BUFFER_CAPACITY: usize = 10_000;
const MAX_FILTER_PENDING: usize = 16 * 1024;

/// Name stamped on `[don]` lifecycle events so the TUI filter can treat them
/// like any other service/task: gated when a filter is active, selectable
/// from the filter dropdown. Must not collide with a user service/task name.
pub const LIFECYCLE_EVENT_NAME: &str = "don";

/// Terminal colors for normal service/task name prefixes.
const SERVICE_COLORS: &[Color] = &[
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
    Color::AnsiValue(208),
    Color::AnsiValue(45),
    Color::AnsiValue(141),
    Color::AnsiValue(118),
    Color::AnsiValue(81),
    Color::AnsiValue(203),
    Color::AnsiValue(220),
    Color::AnsiValue(39),
];

/// Dedicated build-tool colors so these synthetic streams stay neutral and
/// never collide with the rotating service palette.
const BAZEL_COLOR: Color = Color::Grey;

/// One unit of input to a PTY-backed process, funneled through its gate.
#[derive(Debug)]
pub enum PtyInput {
    /// Bytes to write. Frame-atomic: the gate performs one write per frame,
    /// so interleaved writers (several attached clients, the OSC scanner)
    /// cannot shear a paste mid-blob.
    Frame(Vec<u8>),
    /// Resize the PTY to `(cols, rows)`.
    Resize(u16, u16),
}

/// Spawn the input gate for one PTY-backed spawn. The gate owns the write
/// half for the process's whole lifetime; everything that wants to write —
/// attach bridges, the OSC scanner, resize — holds a sender. The gate (and
/// with it the PTY master's write side) ends when the last sender drops:
/// the process's stored sender on exit, the scanner with its sink, and each
/// bridge on disconnect.
pub(crate) fn spawn_pty_gate(mut pty_write: pty_process::OwnedWritePty) -> mpsc::Sender<PtyInput> {
    let (tx, mut rx) = mpsc::channel::<PtyInput>(64);
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        while let Some(input) = rx.recv().await {
            match input {
                PtyInput::Frame(bytes) => {
                    if pty_write.write_all(&bytes).await.is_err() {
                        return;
                    }
                }
                PtyInput::Resize(cols, rows) => {
                    let _ = pty_write.resize(pty_process::Size::new(rows, cols));
                }
            }
        }
    });
    tx
}

/// Handle to an active OSC response scanner. Exists for its [`Drop`]: a
/// restart replaces the process's handle, and dropping the old one removes its
/// sink and releases its gate sender.
pub struct OscSinkHandle {
    /// Our copy of the sender. Dropping it *and* removing the service's copy
    /// from the sinks list closes the channel, stopping the task.
    handle: Option<SinkHandle>,
    /// The scanner task, which holds a gate sender until its channel closes.
    join: Option<JoinHandle<()>>,
    service_state: Arc<Mutex<ServiceOutputState>>,
}

impl Drop for OscSinkHandle {
    /// Stop the scanner when the handle is dropped — e.g. when a restart
    /// replaces `osc_sink` or a service stops. Without this the task lives
    /// on (the service's copy of the channel sender stays in the sinks
    /// list, so the channel never closes), keeping a gate sender alive and
    /// with it one PTY master per restart.
    fn drop(&mut self) {
        // Aborting the task drops its gate sender.
        if let Some(join) = self.join.take() {
            join.abort();
        }
        // Remove the service's lingering copy of our sender so the dead sink
        // stops receiving lines. The sinks list is behind an async lock, so
        // hand the removal to a task — there is always a runtime on the runner
        // loop, where these handles are dropped.
        if let Some(handle) = self.handle.take() {
            let service_state = Arc::clone(&self.service_state);
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                rt.spawn(async move {
                    let mut state = service_state.lock().await;
                    state.sinks.retain(|s| !s.same_channel(&handle));
                });
            }
        }
    }
}

/// A message to a sink. Sinks receive these and write them to their destination.
pub struct SinkLine {
    /// Formatted prefix (color-coded service name, or bold [don] for lifecycle).
    /// Empty for file sinks (raw output).
    pub prefix: Bytes,
    /// The raw line content (no newline).
    pub line: Bytes,
    /// The service/task name this line belongs to. `[don]` lifecycle events
    /// use [`LIFECYCLE_EVENT_NAME`] so they participate in the TUI filter
    /// like any other entry — selectable, but hidden when the filter is
    /// active and doesn't include them.
    pub name: String,
    /// True for `[don]`-prefixed lifecycle events ("api: stopping...",
    /// "shutting down gracefully", build progress). False for raw service
    /// stdout/stderr. The TUI uses this to keep lifecycle events visible
    /// during shutdown even for services the user has filtered out — a
    /// hidden kafka should still surface "send SIGTERM" without flooding
    /// the screen with kafka's own (filtered) shutdown chatter.
    pub is_lifecycle: bool,
}

/// A handle to a sink. Clone the sender to subscribe a service to it.
///
/// Two flavors:
///
/// - **Unbounded**: don's own internal pipeline (stdout/TUI sink, file sinks,
///   lifecycle events). Send always succeeds unless the receiver has been
///   dropped. We don't impose backpressure here — a noisy service is the
///   user's problem to filter, not ours to throttle. Better to let memory
///   grow briefly than silently drop control-plane events ("send SIGTERM…",
///   "stopping…") because some service is spamming.
/// - **BoundedDrop**: slow external consumers (`don logs --follow` HTTP
///   clients, OSC response sinks). When their channel fills, drop the sink
///   entirely so a stuck client can't stall don's output.
#[derive(Clone)]
pub(crate) enum SinkHandle {
    Unbounded(mpsc::UnboundedSender<SinkLine>),
    BoundedDrop(mpsc::Sender<SinkLine>),
    /// Feeds the server-side terminal emulator (see [`emulator`]). Carries
    /// only the raw bytes; the emulator thread keys screens by the line's
    /// process name.
    Emulator(mpsc::UnboundedSender<emulator::EmulatorRequest>),
}

impl SinkHandle {
    /// Send a line. Returns `Err(())` if the sink should be pruned —
    /// receiver dropped, or (for `BoundedDrop`) the consumer is too slow.
    pub fn send(&self, msg: SinkLine) -> Result<(), ()> {
        match self {
            Self::Unbounded(tx) => tx.send(msg).map_err(|_| ()),
            Self::BoundedDrop(tx) => tx.try_send(msg).map_err(|_| ()),
            Self::Emulator(tx) => tx
                .send(emulator::EmulatorRequest::Feed {
                    name: msg.name,
                    bytes: msg.line.to_vec(),
                })
                .map_err(|_| ()),
        }
    }

    pub fn is_closed(&self) -> bool {
        match self {
            Self::Unbounded(tx) => tx.is_closed(),
            Self::BoundedDrop(tx) => tx.is_closed(),
            Self::Emulator(tx) => tx.is_closed(),
        }
    }

    pub fn same_channel(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Unbounded(a), Self::Unbounded(b)) => a.same_channel(b),
            (Self::BoundedDrop(a), Self::BoundedDrop(b)) => a.same_channel(b),
            (Self::Emulator(a), Self::Emulator(b)) => a.same_channel(b),
            _ => false,
        }
    }

    /// True when this sink should be dropped on `close_follow_sinks` —
    /// follow/OSC sinks for transient external clients, not stdout/file.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::BoundedDrop(_))
    }
}

/// A fully formatted, sanitized log line emitted by the stdout pipeline.
///
/// Emitted on the merged log-stream tap ([`OutputManager::log_stream_sender`])
/// for every follower — the TUI feeds each line into `terminal.insert_before`
/// (preserving native scrollback) and stamps the `name` for filter matching.
/// The bytes already include any verbose-mode timestamp and the color-coded
/// service prefix; the consumer just renders them as-is.
#[derive(Debug, Clone)]
pub struct FormattedLogLine {
    /// Owning service/task name. `[don]` lifecycle events carry
    /// [`LIFECYCLE_EVENT_NAME`] so the filter treats them as a selectable
    /// entry rather than always passing them.
    pub name: String,
    /// True for `[don]`-prefixed lifecycle events; false for raw service
    /// stdout/stderr. Lets the TUI keep lifecycle events visible even when
    /// the source service is filtered out (esp. during shutdown).
    pub is_lifecycle: bool,
    /// Fully formatted line bytes. Does NOT include a trailing newline —
    /// the renderer appends one (or, for ratatui, treats it as one row).
    pub bytes: Vec<u8>,
}

/// Shared runtime control for verbose lifecycle and watch logging.
///
/// Clones point at the same atomic flag, so the TUI can toggle verbosity
/// live while the stdout sink and background emitters observe the change
/// immediately.
#[derive(Clone, Debug)]
pub struct VerbosityControl {
    enabled: Arc<AtomicBool>,
}

impl VerbosityControl {
    fn new(enabled: bool) -> Self {
        Self {
            enabled: Arc::new(AtomicBool::new(enabled)),
        }
    }

    /// Return whether verbose logging is currently enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Set verbose logging on or off for all current emitters and sinks.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Flip verbose logging and return the new state.
    pub fn toggle(&self) -> bool {
        let new_value = !self.is_enabled();
        self.set_enabled(new_value);
        new_value
    }
}

/// Allowlist applied at the stdout sink so pipe-mode runs can scope output
/// to a subset of services without a TUI filter modal. When the inner
/// `OnceLock` is unset, no filter is active and everything passes.
///
/// When set, a line passes when any of:
/// - its `name` is in the allowlist (the user's chosen services), or
/// - its `name` is [`LIFECYCLE_EVENT_NAME`] (top-level `[don]` events —
///   shutdown progress, errors, restart info — are control plane and stay
///   unconditionally visible so the user can still tell what the runner
///   is doing), or
/// - its `name` is empty (only the end-of-stream accumulator flush emits
///   nameless lines; partial lines at shutdown should always surface).
///
/// Per-service lifecycle events (`[don] api: restarted`) carry the service
/// name, so they're allowlisted with the service — matching the TUI filter.
///
/// Set once at startup via [`OutputManager::set_log_filter`] before the
/// runner spawns any service. Subsequent set attempts are silently ignored.
#[derive(Clone, Debug, Default)]
struct LogFilterControl {
    allowlist: Arc<std::sync::OnceLock<HashSet<String>>>,
}

impl LogFilterControl {
    fn passes(&self, name: &str) -> bool {
        match self.allowlist.get() {
            None => true,
            Some(set) => name.is_empty() || name == LIFECYCLE_EVENT_NAME || set.contains(name),
        }
    }

    fn set(&self, names: HashSet<String>) {
        let _ = self.allowlist.set(names);
    }
}

#[derive(Clone, Debug, Default)]
struct CompiledLogKeepFilter {
    patterns: Vec<regex::bytes::Regex>,
}

impl CompiledLogKeepFilter {
    fn from_config(
        name: &str,
        config: Option<&crate::config::LogFilterConfig>,
    ) -> Result<Self, OutputError> {
        let Some(config) = config else {
            return Ok(Self::default());
        };
        let mut patterns = Vec::with_capacity(config.patterns.len());
        for pattern in &config.patterns {
            let compiled = regex::bytes::Regex::new(pattern).map_err(|source| {
                OutputError::InvalidLogFilter {
                    name: name.to_string(),
                    pattern: pattern.clone(),
                    source,
                }
            })?;
            patterns.push(compiled);
        }
        Ok(Self { patterns })
    }

    fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    fn keeps(&self, line: &[u8]) -> bool {
        self.patterns.iter().any(|pattern| pattern.is_match(line))
    }
}

/// Where the stdout sink task sends its formatted lines.
///
/// Pipe-mode output writes bytes directly to an `AsyncWrite` (today's stdout).
/// The TUI is not a target: it follows [`OutputManager::log_stream_sender`]
/// and TUI-mode callers pass a null writer.
enum StdoutTarget<W: tokio::io::AsyncWrite + Unpin + Send> {
    Writer(W),
}

/// Per-service output state. Owned by OutputManager, never removed.
struct ServiceOutputState {
    /// Service/task name — stamped onto every emitted `SinkLine` so the TUI
    /// can filter without having to reverse-map the prefix bytes.
    name: String,
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
                    let (name, prefix, sinks, chunks) = {
                        let mut state = self.state.lock().await;
                        state.sinks.retain(|s| !s.is_closed());
                        let chunks = state.output_chunks(chunk);
                        (
                            state.name.clone(),
                            state.prefix.clone(),
                            state.sinks.clone(),
                            chunks,
                        )
                    };

                    self.send_chunks(name, prefix, sinks, chunks).await;
                }
                Err(e) => return Err(e),
            }
        }

        // Flush any partial line remaining in the ring buffer.
        let (name, prefix, sinks, chunks) = {
            let mut state = self.state.lock().await;
            let chunks = state.flush_output();
            (
                state.name.clone(),
                state.prefix.clone(),
                state.sinks.clone(),
                chunks,
            )
        };
        self.send_chunks(name, prefix, sinks, chunks).await;

        Ok(())
    }

    /// Close all transient (follow/attach) sinks. Called when the process
    /// stream ends (process exited) so that attach sessions and log followers
    /// detect the closure and exit instead of blocking forever.
    ///
    /// Only removes transient sinks (follow/OSC). Persistent sinks (stdout,
    /// file) are kept for the next process lifecycle.
    pub async fn close_follow_sinks(&self) {
        let mut state = self.state.lock().await;
        state.sinks.retain(|s| !s.is_transient());
    }

    /// Write a single line to the ring buffer and sinks.
    ///
    /// Used for structured output like Docker build progress that arrives
    /// as individual text lines rather than a byte stream. Appends `\n` to
    /// the data so sinks can flush immediately.
    pub async fn write_line(&self, line: &str) {
        let data = Bytes::from(format!("{line}\n"));
        let (name, prefix, sinks, chunks) = {
            let mut state = self.state.lock().await;
            state.sinks.retain(|s| !s.is_closed());
            let chunks = state.output_chunks(data);
            (
                state.name.clone(),
                state.prefix.clone(),
                state.sinks.clone(),
                chunks,
            )
        };
        self.send_chunks(name, prefix, sinks, chunks).await;
    }

    async fn send_chunks(
        &self,
        name: String,
        prefix: Bytes,
        sinks: Vec<SinkHandle>,
        chunks: Vec<Bytes>,
    ) {
        if chunks.is_empty() {
            return;
        }
        let mut dropped: Vec<SinkHandle> = Vec::new();
        for chunk in chunks {
            for sink in &sinks {
                let msg = SinkLine {
                    prefix: prefix.clone(),
                    line: chunk.clone(),
                    name: name.clone(),
                    is_lifecycle: false,
                };
                if sink.send(msg).is_err() {
                    dropped.push(sink.clone());
                }
            }
        }
        if !dropped.is_empty() {
            let mut state = self.state.lock().await;
            state
                .sinks
                .retain(|s| !dropped.iter().any(|d| d.same_channel(s)));
        }
    }
}

fn reserved_color(name: &str) -> Option<Color> {
    match name {
        "bazel" => Some(BAZEL_COLOR),
        _ => None,
    }
}

/// Assigns a deterministic color to each service/task/build-tool name.
pub(crate) fn assign_colors(names: &[&str]) -> HashMap<String, Color> {
    let mut sorted: Vec<&str> = names.to_vec();
    sorted.sort_unstable();

    let mut next_service_color = 0usize;
    let mut assigned = HashMap::with_capacity(sorted.len());
    for name in sorted {
        let color = reserved_color(name).unwrap_or_else(|| {
            let color = SERVICE_COLORS[next_service_color % SERVICE_COLORS.len()];
            next_service_color += 1;
            color
        });
        assigned.insert(name.to_string(), color);
    }
    assigned
}

/// Format a command + args pair into a shell-ish one-liner for debug logs.
///
/// Any argument containing whitespace, quotes, or shell metacharacters gets
/// single-quoted with embedded `'` escaped as `'\''`. Safe to copy-paste
/// into a shell.
pub(crate) fn format_cmdline<S: AsRef<str>>(cmd: &str, args: &[S]) -> String {
    let mut out = shell_quote(cmd);
    for arg in args {
        out.push(' ');
        out.push_str(&shell_quote(arg.as_ref()));
    }
    out
}

fn shell_quote(s: &str) -> String {
    let needs_quoting = s.is_empty()
        || s.bytes().any(|b| {
            !(b.is_ascii_alphanumeric()
                || b == b'_'
                || b == b'-'
                || b == b'.'
                || b == b'/'
                || b == b':'
                || b == b'='
                || b == b',')
        });
    if !needs_quoting {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

/// Build a formatted prefix as bytes for a service name.
fn format_prefix(name: &str, color: Color, max_name_len: usize) -> Bytes {
    Bytes::from(format!(
        "{}{:width$}{} | ",
        SetForegroundColor(color),
        name,
        ResetColor,
        width = max_name_len,
    ))
}

/// Read the last N ring-buffer lines from one service's output state,
/// joined by newlines (trailing newline stripped).
async fn read_logs_from(state_arc: &Arc<Mutex<ServiceOutputState>>, n: usize) -> Bytes {
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
    Bytes::from(result)
}

/// Attach a follow sink to one service's output state: a freshly-created mpsc
/// channel preloaded with the last N buffered lines, then registered as a
/// sink. New lines are delivered until the receiver is dropped (or the client
/// is too slow and the sink's buffer fills — it then gets disconnected).
async fn follow_sink_from(
    state_arc: Arc<Mutex<ServiceOutputState>>,
    last_n: usize,
    live_capacity: usize,
) -> mpsc::Receiver<SinkLine> {
    // Channel must hold the preloaded snapshot AND live headroom without
    // blocking (or dropping the freshly-connected client immediately).
    let capacity = last_n.saturating_add(live_capacity).max(1);
    let (tx, rx) = mpsc::channel::<SinkLine>(capacity);
    let mut state = state_arc.lock().await;
    let svc_name = state.name.clone();
    let prefix = state.prefix.clone();
    // Preload last N ring buffer lines. Channel has `capacity` slots and
    // is empty, so try_send is safe here.
    for line in state.ring_buffer.last_n(last_n) {
        // Ring-buffer entries keep their trailing `\n`, but `SinkLine.line`
        // is contractually newline-free (the follow route embeds it in a
        // JSON "line" value and the client adds its own newline). Strip it
        // so the replayed snapshot doesn't render a blank line per entry.
        let line = line.strip_suffix(b"\n").unwrap_or(line);
        let sink_line = SinkLine {
            prefix: prefix.clone(),
            line: Bytes::copy_from_slice(line),
            name: svc_name.clone(),
            // Ring-buffer replays mostly carry raw service stdout. Even
            // if a few lifecycle events are mixed in, marking them all
            // as non-lifecycle is correct for follow consumers, which
            // don't have a TUI filter to short-circuit anyway.
            is_lifecycle: false,
        };
        if tx.try_send(sink_line).is_err() {
            break;
        }
    }
    state.sinks.push(SinkHandle::BoundedDrop(tx));
    rx
}

/// A cloneable, read-only handle to every process's buffered output.
///
/// This is to logs what [`StateReader`](crate::state_store::StateReader) is
/// to state: the server holds one and answers `GET /logs` (and the follow
/// variant) without a runner round trip, so reading logs never queues behind
/// whatever the runner is currently doing. The registry is republished
/// through a `watch`, so a name registered late (the build-tool prefix) is
/// visible to handles minted before it existed.
#[derive(Clone)]
pub struct LogReader {
    services: watch::Receiver<Arc<HashMap<String, Arc<Mutex<ServiceOutputState>>>>>,
}

impl LogReader {
    /// An empty reader for tests that need an `ApiState` without an
    /// `OutputManager`. Every lookup answers `None`.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        let (tx, rx) = watch::channel(Arc::new(HashMap::new()));
        std::mem::forget(tx);
        Self { services: rx }
    }

    /// Read the last N lines from a process's ring buffer, joined by
    /// newlines. `None` if the name is not registered.
    pub async fn read_logs(&self, name: &str, n: usize) -> Option<Bytes> {
        let state_arc = self.services.borrow().get(name)?.clone();
        Some(read_logs_from(&state_arc, n).await)
    }

    /// Attach a follow sink (see [`OutputManager::add_follow_sink`]).
    /// `None` if the name is not registered.
    pub async fn add_follow_sink(
        &self,
        name: &str,
        last_n: usize,
        live_capacity: usize,
    ) -> Option<mpsc::Receiver<SinkLine>> {
        let state_arc = self.services.borrow().get(name)?.clone();
        Some(follow_sink_from(state_arc, last_n, live_capacity).await)
    }
}

/// Manages output for all services — creates sinks, spawns writer tasks,
/// and provides lifecycle event formatting.
pub struct OutputManager {
    /// Per-service output state, retained for the lifetime of the program.
    services: HashMap<String, Arc<Mutex<ServiceOutputState>>>,
    /// The same registry, republished for [`LogReader`] handles. Almost
    /// always set once at construction; `register_service` republishes for
    /// the rare late registration (the build-tool prefix).
    services_watch: watch::Sender<Arc<HashMap<String, Arc<Mutex<ServiceOutputState>>>>>,
    /// The formatted `[don]` prefix, padded to align with service prefixes.
    don_prefix: String,
    /// Stdout sink sender — used for lifecycle events and service output.
    stdout_sink: SinkHandle,
    /// Writer task JoinHandles for clean shutdown.
    writer_handles: Vec<JoinHandle<()>>,
    /// Shared runtime verbose mode — enables extra diagnostic lifecycle events
    /// and timestamps on stdout/TUI log lines.
    verbosity: VerbosityControl,
    /// Handle to the server-side terminal-emulator thread. Screens register
    /// per PTY-backed spawn; see [`emulator`].
    emulator: emulator::EmulatorHandle,
    /// Allowlist applied to stdout-bound lines. None until
    /// [`Self::set_log_filter`] is called; once set, only lines whose name
    /// is in the allowlist (or empty, for `[don]` lifecycle events) reach
    /// the writer. Ring buffers and file sinks are unaffected.
    log_filter: LogFilterControl,
    /// Cached formatted prefix for the synthetic "bazel" stream. Populated
    /// by [`Self::register_build_tool`]; `None` means build output falls
    /// back to a `[don]`-prefixed lifecycle event with a `bazel:` text
    /// prefix.
    bazel_prefix: Option<Bytes>,
    /// Broadcast tap + bounded history of the merged, formatted log stream.
    /// See [`MergedLogTap`].
    log_tap: MergedLogTap,
}

/// The merged log stream's fan-out point: a broadcast of every line
/// [`stdout_sink_task`] emits (post filter, post sanitize), plus a bounded
/// history of the same lines so a late-connecting follower can preload.
///
/// History and broadcast carry the *same* `Arc`s — one allocation per line —
/// which is also what makes preload/live dedupe exact: a follower that
/// subscribes first and snapshots second can drop live lines already seen in
/// the snapshot by pointer identity, no sequence numbers needed.
#[derive(Clone)]
pub struct MergedLogTap {
    tx: broadcast::Sender<Arc<FormattedLogLine>>,
    history: Arc<Mutex<std::collections::VecDeque<Arc<FormattedLogLine>>>>,
}

/// How many merged lines a late joiner can preload. Smaller than the
/// per-service rings (which serve `don logs <name>`): this covers "what was
/// happening just before I attached", not deep history.
const MERGED_HISTORY_CAPACITY: usize = 2_000;

impl MergedLogTap {
    fn new() -> Self {
        // Broadcast capacity trades memory for how far a slow follower may
        // fall behind before it must resync; entries are pointers.
        let (tx, _) = broadcast::channel(4096);
        Self {
            tx,
            history: Arc::new(Mutex::new(std::collections::VecDeque::with_capacity(
                MERGED_HISTORY_CAPACITY,
            ))),
        }
    }

    /// Record and fan out one line.
    async fn publish(&self, line: Arc<FormattedLogLine>) {
        {
            let mut history = self.history.lock().await;
            if history.len() == MERGED_HISTORY_CAPACITY {
                history.pop_front();
            }
            history.push_back(line.clone());
        }
        // Send on a receiver-less broadcast is a cheap no-op.
        let _ = self.tx.send(line);
    }

    /// Subscribe to lines from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<FormattedLogLine>> {
        self.tx.subscribe()
    }

    /// An empty tap for tests that need an `ApiState` without an
    /// `OutputManager`.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        Self::new()
    }

    /// The last `n` lines, oldest first.
    pub async fn history(&self, n: usize) -> Vec<Arc<FormattedLogLine>> {
        let history = self.history.lock().await;
        history
            .iter()
            .skip(history.len().saturating_sub(n))
            .cloned()
            .collect()
    }
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
    /// Invalid regex in a log keep filter.
    #[error("service '{name}': invalid log_filter regex '{pattern}': {source}")]
    InvalidLogFilter {
        name: String,
        pattern: String,
        #[source]
        source: regex::Error,
    },
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

    /// Create a new output manager with per-service regex keep filters.
    pub async fn new_with_log_filters<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        log_filters: &HashMap<String, crate::config::LogFilterConfig>,
        writer: W,
    ) -> Result<Self, OutputError> {
        Self::new_verbose_with_log_filters(services, log_filters, writer, false).await
    }

    /// Create a new output manager. When `verbose` is true, every output
    /// line is prefixed with an elapsed timestamp.
    pub async fn new_verbose<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        writer: W,
        verbose: bool,
    ) -> Result<Self, OutputError> {
        Self::new_inner(
            services,
            &HashMap::new(),
            verbose,
            StdoutTarget::Writer(writer),
        )
        .await
    }

    /// Create a new verbose output manager with per-service regex keep filters.
    pub async fn new_verbose_with_log_filters<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        log_filters: &HashMap<String, crate::config::LogFilterConfig>,
        writer: W,
        verbose: bool,
    ) -> Result<Self, OutputError> {
        Self::new_inner(services, log_filters, verbose, StdoutTarget::Writer(writer)).await
    }

    // The TUI has no dedicated constructor: it subscribes to
    // [`Self::log_stream_sender`] like any other follower, and TUI-mode
    // callers pass `tokio::io::sink()` as the writer so nothing touches the
    // real stdout while the TUI owns the screen.

    async fn new_inner<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
        services: &[(&str, &crate::config::LogConfig)],
        log_filters: &HashMap<String, crate::config::LogFilterConfig>,
        verbose: bool,
        target: StdoutTarget<W>,
    ) -> Result<Self, OutputError> {
        let names: Vec<&str> = services.iter().map(|(n, _)| *n).collect();
        let color_map = assign_colors(&names);
        let max_name_len = names.iter().map(|n| n.len()).max().unwrap_or(0).max(5);
        let verbosity = VerbosityControl::new(verbose);
        let log_filter = LogFilterControl::default();
        let emulator = emulator::spawn_emulator_thread();

        // Spawn stdout sink task.
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel();
        let log_tap = MergedLogTap::new();
        let stdout_handle = tokio::spawn(stdout_sink_task(
            stdout_rx,
            target,
            verbosity.clone(),
            log_filter.clone(),
            log_tap.clone(),
        ));
        let stdout_sink = SinkHandle::Unbounded(stdout_tx);

        // Spawn file sink tasks (deduplicated by path).
        let mut file_sinks: HashMap<PathBuf, SinkHandle> = HashMap::new();
        let mut writer_handles = vec![stdout_handle];

        for (_, config) in services {
            if let crate::config::LogConfig::File(path) = config
                && !file_sinks.contains_key(path)
            {
                let file = open_log_file(path).await?;
                let (tx, rx) = mpsc::unbounded_channel();
                writer_handles.push(tokio::spawn(file_sink_task(rx, file)));
                file_sinks.insert(path.clone(), SinkHandle::Unbounded(tx));
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

            let color = color_map.get(*name).copied().unwrap_or(Color::White);
            let prefix = format_prefix(name, color, max_name_len);
            let log_keep_filter = CompiledLogKeepFilter::from_config(
                name,
                log_filters.get(*name).filter(|filter| !filter.is_empty()),
            )?;

            service_map.insert(
                name.to_string(),
                Arc::new(Mutex::new(ServiceOutputState {
                    name: name.to_string(),
                    prefix,
                    ring_buffer: RingBuffer::new(DEFAULT_RING_BUFFER_CAPACITY),
                    log_keep_filter,
                    filter_pending: BytesMut::new(),
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

        let (services_watch, _) = watch::channel(Arc::new(service_map.clone()));
        Ok(Self {
            services: service_map,
            services_watch,
            don_prefix,
            stdout_sink,
            writer_handles,
            verbosity,
            emulator,
            log_filter,
            bazel_prefix: None,
            log_tap,
        })
    }

    /// A handle on the merged log stream — subscriptions plus bounded
    /// history — for components that hand out follows (the API server, the
    /// in-process TUI forwarder). Holding the handle rather than a receiver
    /// means idle handles cost nothing and never lag.
    pub fn log_stream_sender(&self) -> MergedLogTap {
        self.log_tap.clone()
    }

    /// Register a synthetic "tool" service (`bazel`) so build
    /// output gets its own color-coded prefix column instead of an inline
    /// `bazel: …` text prefix on a `[don]` lifecycle event.
    ///
    /// Idempotent: if already registered, just returns — the cached prefix
    /// stays intact. Name must be `"bazel"` (panics
    /// otherwise in debug builds; silently drops the prefix cache in
    /// release).
    /// Restrict stdout-bound output to lines whose source name is in
    /// `allowlist`. Top-level `[don]` lifecycle events (whose `name` is
    /// empty) always pass; per-service lifecycle events (`[don] api: ...`)
    /// pass only when `api` is in the allowlist, mirroring the TUI filter.
    ///
    /// File sinks and ring buffers are unaffected — `don logs <name>`
    /// still returns the full output, and `log = { file = ... }` continues
    /// to receive every line.
    ///
    /// Idempotent guard: only the first call takes effect. Call this once
    /// at startup, before the runner spawns any service. Pass an empty set
    /// to silence every per-service line while keeping `[don]` events
    /// visible.
    pub fn set_log_filter(&self, allowlist: HashSet<String>) {
        self.log_filter.set(allowlist);
    }

    pub async fn register_build_tool(&mut self, name: &str) {
        self.register_service(name, &crate::config::LogConfig::Stdout)
            .await;
        let prefix = if let Some(state_arc) = self.services.get(name) {
            let state = state_arc.lock().await;
            Some(state.prefix.clone())
        } else {
            None
        };
        match name {
            "bazel" => self.bazel_prefix = prefix,
            _ => debug_assert!(false, "register_build_tool: unknown tool '{name}'"),
        }
    }

    /// Emit a line prefixed as bazel tool output. Falls back to a
    /// `[don]`-prefixed `bazel: {message}` lifecycle event if
    /// [`Self::register_build_tool`] wasn't called.
    pub fn bazel_event(&self, message: &str) {
        match self.bazel_prefix.as_ref() {
            Some(prefix) => {
                let _ = self.stdout_sink.send(SinkLine {
                    prefix: prefix.clone(),
                    line: Bytes::from(format!("{message}\n")),
                    name: "bazel".to_string(),
                    is_lifecycle: true,
                });
            }
            None => self.lifecycle_event(&format!("bazel: {message}")),
        }
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
        Some(follow_sink_from(state_arc, last_n, live_capacity).await)
    }

    /// Add an attach sink preloaded with a screen repaint frame instead of a
    /// ring-buffer snapshot. The bridge writes the frame verbatim, so the
    /// client's terminal shows the process's current grid before live bytes
    /// resume — no raw-byte replay.
    pub(crate) async fn add_attach_sink(
        &self,
        name: &str,
        repaint: emulator::RepaintFrame,
        live_capacity: usize,
    ) -> Option<mpsc::Receiver<SinkLine>> {
        let state_arc = self.services.get(name)?.clone();
        let capacity = live_capacity.max(2);
        let (tx, rx) = mpsc::channel::<SinkLine>(capacity);
        let mut state = state_arc.lock().await;
        let sink_line = SinkLine {
            prefix: Bytes::new(),
            line: Bytes::from(repaint.bytes),
            name: state.name.clone(),
            is_lifecycle: false,
        };
        // Channel is empty and capacity >= 2, so this cannot fail.
        let _ = tx.try_send(sink_line);
        state.sinks.push(SinkHandle::BoundedDrop(tx));
        Some(rx)
    }

    /// Render an process's current screen; `None` when it has no screen.
    pub(crate) async fn emulator_repaint(&self, name: &str) -> Option<emulator::RepaintFrame> {
        self.emulator.repaint(name).await
    }

    /// A handle to the emulator thread, for the server's resize path.
    pub(crate) fn emulator_handle(&self) -> emulator::EmulatorHandle {
        self.emulator.clone()
    }

    /// Add an OSC response sink to a service. The sink scans each chunk for
    /// terminal queries (OSC 10/11, cursor position) and writes responses
    /// directly to the PTY write handle.
    ///
    /// The sink uses bounded-drop semantics so it never blocks the output
    /// pipeline. Returns a [`OscSinkHandle`] that can be used to reclaim
    /// the PTY write handle (e.g., for attach).
    pub async fn add_osc_sink(
        &self,
        name: &str,
        pty_input: mpsc::Sender<PtyInput>,
    ) -> Option<OscSinkHandle> {
        let state_arc = self.services.get(name)?.clone();
        let (tx, rx) = mpsc::channel::<SinkLine>(16);
        let handle = SinkHandle::BoundedDrop(tx);
        {
            let mut state = state_arc.lock().await;
            state.sinks.push(handle.clone());
        }
        let join = tokio::spawn(osc_sink_task(rx, pty_input));
        Some(OscSinkHandle {
            handle: Some(handle),
            join: Some(join),
            service_state: state_arc,
        })
    }

    /// Read the last N lines from a service's ring buffer, joined by newlines.
    ///
    /// Returns `None` if the service is not registered.
    pub async fn read_logs(&self, name: &str, n: usize) -> Option<Bytes> {
        let state_arc = self.services.get(name)?;
        Some(read_logs_from(state_arc, n).await)
    }

    /// Register a service that wasn't known at OutputManager construction
    /// (currently used by `register_build_tool` to give `bazel`
    /// their own prefix column). Creates a ring buffer, assigns a color,
    /// and wires up sinks based on the log config. Existing services are
    /// left unchanged.
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
        let mut all_names: Vec<&str> = self.services.keys().map(String::as_str).collect();
        all_names.push(name);
        let color = assign_colors(&all_names)
            .get(name)
            .copied()
            .unwrap_or(Color::White);
        let prefix = format_prefix(name, color, max_name_len);

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
                name: name.to_string(),
                prefix,
                ring_buffer: RingBuffer::new(DEFAULT_RING_BUFFER_CAPACITY),
                log_keep_filter: CompiledLogKeepFilter::default(),
                filter_pending: BytesMut::new(),
                sinks,
                stdout_paused: false,
            })),
        );
        let _ = self.services_watch.send(Arc::new(self.services.clone()));
    }

    /// Get a cloneable read-only handle to every process's buffered output.
    pub fn log_reader(&self) -> LogReader {
        LogReader {
            services: self.services_watch.subscribe(),
        }
    }

    /// Get a shared runtime controller for verbose logging.
    pub fn verbosity_control(&self) -> VerbosityControl {
        self.verbosity.clone()
    }

    /// Get a cloneable output handle scoped to one registered process.
    ///
    /// Returns `None` for an unregistered name — every service and task is
    /// registered at construction, so `None` means a genuine name mismatch,
    /// not a timing window.
    pub fn process_output(&self, name: &str) -> Option<ProcessOutput> {
        Some(ProcessOutput {
            name: name.to_string(),
            state: Arc::clone(self.services.get(name)?),
            events: self.clone_lifecycle_emitter(),
            emulator: self.emulator.clone(),
        })
    }

    /// Get a lightweight, cloneable handle for emitting `[don]` lifecycle
    /// events from spawned tasks (e.g. build output).
    pub fn clone_lifecycle_emitter(&self) -> LifecycleEmitter {
        LifecycleEmitter {
            don_prefix: self.don_prefix.clone(),
            stdout_sink: self.stdout_sink.clone(),
            bazel_prefix: self.bazel_prefix.clone(),
            verbosity: self.verbosity.clone(),
        }
    }

    /// Emit a `[don]` lifecycle event.
    pub fn lifecycle_event(&self, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
        });
    }

    /// Emit a `[don]` lifecycle event only when verbose mode is enabled.
    pub fn debug_event(&self, message: &str) {
        if self.verbosity.is_enabled() {
            self.lifecycle_event(message);
        }
    }

    /// Log (in verbose mode only) the exact executable and arguments about
    /// to be passed to `execve`. See [`LifecycleEmitter::debug_spawn`].
    pub fn debug_spawn<S: AsRef<str>>(&self, label: &str, cmd: &str, args: &[S]) {
        if !self.verbosity.is_enabled() {
            return;
        }
        self.service_event(label, &format!("spawn {}", format_cmdline(cmd, args)));
    }

    /// Emit a `[don]` lifecycle event scoped to a service/task. The message
    /// is prefixed with `{service}: ` and the line is tagged with `service`
    /// so it passes the TUI filter when the service is selected — `[don] api:
    /// restarted` shows up under the "api" filter, not under "don".
    pub fn service_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
            name: service.to_string(),
            is_lifecycle: true,
        });
    }

    /// Emit a `[don]` lifecycle event scoped to a service/task, only when
    /// verbose mode is enabled. See [`Self::service_event`].
    pub fn service_debug_event(&self, service: &str, message: &str) {
        if self.verbosity.is_enabled() {
            self.service_event(service, message);
        }
    }

    /// Emit a `[don]` error event.
    pub fn error_event(&self, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
        });
    }

    /// Emit a `[don]` error event for a specific service. Tagged with the
    /// service name so the filter surfaces it alongside that service's own
    /// output. See [`Self::service_event`] for rationale.
    pub fn service_error_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
            name: service.to_string(),
            is_lifecycle: true,
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
                .any(|s| s.same_channel(&self.stdout_sink));
            if had_stdout {
                state.sinks.retain(|s| !s.same_channel(&self.stdout_sink));
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
                    .any(|s| s.same_channel(&self.stdout_sink));
                if !already_present {
                    state.sinks.push(self.stdout_sink.clone());
                }
            }
        }
    }

    /// Shut down the output system. Clears all sink lists, drops senders,
    /// and waits for writer tasks to drain remaining messages.
    ///
    /// A writer task only exits once every clone of its sink sender has
    /// dropped. If some detached task in the process is still holding a
    /// [`LifecycleEmitter`] (and thus a sender clone) — the API server,
    /// a slow-to-abort proxy accept loop, a lingering background build —
    /// `handle.await` blocks forever. That would hang the daemon long
    /// past the `shutdown complete` lifecycle event, preventing `don`
    /// from returning to the shell.
    ///
    /// To make that failure mode self-correcting, each writer gets a
    /// bounded drain window: we abort stragglers so shutdown completes
    /// within a predictable time. The downside is occasional truncation
    /// of the very last log line under load, which is preferable to the
    /// daemon silently refusing to exit.
    pub async fn shutdown(self) {
        for state_arc in self.services.values() {
            let mut state = state_arc.lock().await;
            state.sinks.clear();
        }
        drop(self.stdout_sink);
        drop(self.services);
        for mut handle in self.writer_handles {
            let wait = std::time::Duration::from_secs(2);
            match tokio::time::timeout(wait, &mut handle).await {
                Ok(_) => {}
                Err(_) => {
                    // A straggler task is still holding a sender clone
                    // (the API server task, a slow-to-abort proxy accept
                    // loop, a lingering build). `abort()` cancels the
                    // writer task so its `log_tx` inside the TUI plumbing
                    // actually drops — without that, the TUI's
                    // `log_rx.recv()` blocks forever and `don` never
                    // returns to the shell after "shutdown complete".
                    handle.abort();
                    let _ = handle.await;
                }
            }
        }
    }
}

/// Everything one process needs to do its own output, in a cloneable handle.
///
/// [`OutputManager`] is deliberately not `Clone` — it owns the writer task
/// handles that `shutdown` must join, so exactly one thing may own it. But a
/// per-process supervisor still has to write its child's output, attach an OSC
/// sink, mute the terminal for a foreground run, and emit its own lifecycle
/// events. This hands out that slice, scoped to a single name.
///
/// "Process" rather than "service" because tasks use the same machinery — and so
/// does the synthetic `bazel` stream. The name is whatever the
/// stream was registered under.
#[derive(Clone)]
pub struct ProcessOutput {
    name: String,
    state: Arc<Mutex<ServiceOutputState>>,
    events: LifecycleEmitter,
    emulator: emulator::EmulatorHandle,
}

impl ProcessOutput {
    /// The name this handle is scoped to.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// A writer for piping this process's child process output.
    ///
    /// Several may exist at once (a restart creates a new one before the old
    /// reader has finished draining); they share one ring buffer.
    pub fn writer(&self) -> ServiceWriter {
        ServiceWriter {
            state: Arc::clone(&self.state),
        }
    }

    /// Attach an OSC response sink so terminal queries from the child reach
    /// its PTY. See [`OutputManager::add_osc_sink`].
    pub async fn add_osc_sink(&self, pty_input: mpsc::Sender<PtyInput>) -> OscSinkHandle {
        let (tx, rx) = mpsc::channel::<SinkLine>(16);
        let handle = SinkHandle::BoundedDrop(tx);
        {
            let mut state = self.state.lock().await;
            state.sinks.push(handle.clone());
        }
        let join = tokio::spawn(osc_sink_task(rx, pty_input));
        OscSinkHandle {
            handle: Some(handle),
            join: Some(join),
            service_state: Arc::clone(&self.state),
        }
    }

    /// (Re)register this process's server-side screen and route its output
    /// bytes into the emulator. See [`OutputManager::register_emulator`].
    pub async fn register_emulator(&self, cols: u16, rows: u16) {
        self.emulator.register(&self.name, cols, rows);
        let feed = SinkHandle::Emulator(self.emulator.feed_sender());
        let mut state = self.state.lock().await;
        if !state.sinks.iter().any(|sink| sink.same_channel(&feed)) {
            state.sinks.push(feed);
        }
    }

    /// Emit a `[don]` event tagged with this process's name.
    pub fn event(&self, message: &str) {
        self.events.service_event(&self.name, message);
    }

    /// Emit a `[don]` error event tagged with this process's name.
    pub fn error_event(&self, message: &str) {
        self.events.service_error_event(&self.name, message);
    }

    /// Emit a `[don]` event tagged with this process's name, verbose mode only.
    pub fn debug_event(&self, message: &str) {
        self.events.service_debug_event(&self.name, message);
    }

    /// The untagged lifecycle emitter, for handing to spawned helpers.
    pub fn emitter(&self) -> &LifecycleEmitter {
        &self.events
    }
}

/// A lightweight, cloneable handle for emitting `[don]` lifecycle events
/// from spawned tasks. Does not carry the full `OutputManager` state.
#[derive(Clone)]
pub struct LifecycleEmitter {
    don_prefix: String,
    stdout_sink: SinkHandle,
    bazel_prefix: Option<Bytes>,
    verbosity: VerbosityControl,
}

impl LifecycleEmitter {
    /// Emit a `[don]` lifecycle event.
    pub fn lifecycle_event(&self, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{message}\n")),
            name: LIFECYCLE_EVENT_NAME.to_string(),
            is_lifecycle: true,
        });
    }

    /// Emit a `[don]` lifecycle event scoped to a service/task. Tagged with
    /// the service name so the TUI filter shows it when the service is
    /// selected. See [`OutputManager::service_event`] for rationale.
    pub fn service_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
            name: service.to_string(),
            is_lifecycle: true,
        });
    }

    /// Emit a `[don]` error event scoped to a service. Tagged with the
    /// service name.
    pub fn service_error_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}\n")),
            name: service.to_string(),
            is_lifecycle: true,
        });
    }

    /// Emit a `[don]` lifecycle event only when verbose mode is enabled.
    pub fn debug_event(&self, message: &str) {
        if self.verbosity.is_enabled() {
            self.lifecycle_event(message);
        }
    }

    /// Emit a service-scoped `[don]` event only when verbose mode is enabled.
    pub fn service_debug_event(&self, service: &str, message: &str) {
        if self.verbosity.is_enabled() {
            self.service_event(service, message);
        }
    }

    /// Log (in verbose mode only) the exact executable and arguments about
    /// to be passed to `execve`. Use before every `Command::spawn()` so
    /// `don -v` shows what don is actually asking the kernel to run.
    ///
    /// `label` is a short tag (e.g. service name, "bazel") to
    /// help the user identify the source of the spawn.
    pub fn debug_spawn<S: AsRef<str>>(&self, label: &str, cmd: &str, args: &[S]) {
        if !self.verbosity.is_enabled() {
            return;
        }
        self.service_event(label, &format!("spawn {}", format_cmdline(cmd, args)));
    }

    /// Emit a line prefixed as bazel tool output. Falls back to a
    /// `[don]`-prefixed `bazel: {message}` event if the synthetic tool
    /// service wasn't registered.
    pub fn bazel_event(&self, message: &str) {
        match self.bazel_prefix.as_ref() {
            Some(prefix) => {
                let _ = self.stdout_sink.send(SinkLine {
                    prefix: prefix.clone(),
                    line: Bytes::from(format!("{message}\n")),
                    name: "bazel".to_string(),
                    is_lifecycle: true,
                });
            }
            None => self.lifecycle_event(&format!("bazel: {message}")),
        }
    }
}

/// Stdout sink writer task. Receives raw byte chunks and accumulates
/// per-service until `\n` or overflow, then emits the formatted line to
/// the configured target (pipe writer or TUI channel).
///
/// Each service's partial output is buffered independently so that
/// interleaved chunks from different services don't produce garbled output.
/// Runs until all senders are dropped.
async fn stdout_sink_task<W: tokio::io::AsyncWrite + Unpin + Send>(
    mut rx: mpsc::UnboundedReceiver<SinkLine>,
    mut target: StdoutTarget<W>,
    verbosity: VerbosityControl,
    filter: LogFilterControl,
    tap: MergedLogTap,
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
        // Drop messages whose source service isn't in the active allowlist.
        // Wipe any partial accumulator for that prefix too — carrying half a
        // line across a filter change would replay it the next time the
        // service became visible. Empty `name` (top-level lifecycle events)
        // always passes; see `LogFilterControl::passes`.
        if !filter.passes(&msg.name) {
            accumulators.remove(&msg.prefix);
            cr_flushed.remove(&msg.prefix);
            continue;
        }
        let acc = accumulators.entry(msg.prefix.clone()).or_default();

        for &byte in msg.line.iter() {
            acc.extend_from_slice(&[byte]);
            if byte == b'\n' {
                // Complete line — strip \r\n, sanitize, emit prefixed output.
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
                    emit_line(
                        &mut target,
                        &tap,
                        &msg.name,
                        &msg.prefix,
                        &sanitized,
                        msg.is_lifecycle,
                        &verbosity,
                        start,
                    )
                    .await;
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
                    emit_line(
                        &mut target,
                        &tap,
                        &msg.name,
                        &msg.prefix,
                        &sanitized,
                        msg.is_lifecycle,
                        &verbosity,
                        start,
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
                    emit_line(
                        &mut target,
                        &tap,
                        &msg.name,
                        &msg.prefix,
                        &sanitized,
                        msg.is_lifecycle,
                        &verbosity,
                        start,
                    )
                    .await;
                    acc.clear();
                }
            }
        }
    }

    // Flush remaining accumulators on shutdown. The name is lost here — the
    // accumulator map is keyed on prefix bytes — so end-of-stream partial
    // lines arrive at the TUI with an empty name (unfilterable). Partial
    // lines at shutdown are rare and the user probably wants to see them.
    for (prefix, acc) in &accumulators {
        if !acc.is_empty() {
            let sanitized = if prefix.is_empty() {
                acc.to_vec()
            } else {
                sanitize::sanitize_terminal_output(acc)
            };
            // End-of-stream partial line: lifecycle vs not is unknowable at
            // this point (the accumulator key is the prefix, not the source
            // flag). Defaulting to false is correct — these are usually
            // service-stdout fragments that didn't end in `\n` before the
            // child closed its pipe.
            emit_line(
                &mut target,
                &tap,
                "",
                prefix,
                &sanitized,
                false,
                &verbosity,
                start,
            )
            .await;
        }
    }
}

/// Build the formatted line bytes (optional verbose timestamp + prefix + content).
/// No trailing newline — the consumer decides how to terminate the line.
fn build_formatted_bytes(
    prefix: &[u8],
    line: &[u8],
    verbosity: &VerbosityControl,
    start: std::time::Instant,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + line.len() + 16);
    if verbosity.is_enabled() {
        let elapsed = start.elapsed();
        let ts = format!("{:.3}s ", elapsed.as_secs_f64());
        out.extend_from_slice(ts.as_bytes());
    }
    out.extend_from_slice(prefix);
    out.extend_from_slice(line);
    out
}

/// Emit a complete formatted line to the target — either write to the pipe
/// writer with a trailing `\n`, or ship it to the TUI as a [`FormattedLogLine`].
#[allow(clippy::too_many_arguments)]
async fn emit_line<W: tokio::io::AsyncWrite + Unpin + Send>(
    target: &mut StdoutTarget<W>,
    tap: &MergedLogTap,
    name: &str,
    prefix: &[u8],
    line: &[u8],
    is_lifecycle: bool,
    verbosity: &VerbosityControl,
    start: std::time::Instant,
) {
    let bytes = build_formatted_bytes(prefix, line, verbosity, start);
    // Feed the tap first, so history and followers see the line even if
    // the writer below blocks.
    tap.publish(Arc::new(FormattedLogLine {
        name: name.to_string(),
        is_lifecycle,
        bytes: bytes.clone(),
    }))
    .await;
    match target {
        StdoutTarget::Writer(writer) => {
            use tokio::io::AsyncWriteExt;
            let _ = writer.write_all(&bytes).await;
            let _ = writer.write_all(b"\n").await;
        }
    }
}

/// File sink writer task. Receives raw byte chunks and writes them directly.
/// Runs until all senders are dropped.
async fn file_sink_task(mut rx: mpsc::UnboundedReceiver<SinkLine>, mut file: tokio::fs::File) {
    use tokio::io::AsyncWriteExt;
    while let Some(msg) = rx.recv().await {
        let _ = file.write_all(&msg.line).await;
    }
    let _ = file.flush().await;
}

/// OSC response sink task. Scans each chunk for terminal queries and
/// writes responses directly to the PTY write handle. Returns the PTY
/// write handle when the channel closes (process exit or sink removal)
/// so it can be reclaimed by the caller.
async fn osc_sink_task(mut rx: mpsc::Receiver<SinkLine>, pty_input: mpsc::Sender<PtyInput>) {
    while let Some(msg) = rx.recv().await {
        for response in osc::find_responses(&msg.line) {
            if pty_input
                .send(PtyInput::Frame(response.to_vec()))
                .await
                .is_err()
            {
                return;
            }
        }
    }
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

        let mut prev_result: Option<HashMap<String, Color>> = None;
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
        let colors: Vec<Color> = {
            let mut sorted_names: Vec<&str> = names.clone();
            sorted_names.sort_unstable();
            sorted_names.iter().map(|n| result[*n]).collect()
        };
        let unique: std::collections::HashSet<Color> = colors.iter().copied().collect();
        assert_eq!(unique.len(), colors.len());
    }

    #[test]
    fn test_color_assignment_wraps() {
        let names: Vec<String> = (0..SERVICE_COLORS.len() + 3)
            .map(|i| format!("svc{i}"))
            .collect();
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let result = assign_colors(&name_refs);
        assert_eq!(result.len(), names.len());
    }

    #[test]
    fn test_build_tool_colors_are_reserved() {
        let result = assign_colors(&["api", "bazel", "worker"]);
        assert_eq!(result["bazel"], BAZEL_COLOR);
        assert_eq!(result["bazel"], Color::Grey);
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
            let prefix = format_prefix(case.service_name, Color::Cyan, case.max_len);
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
    async fn follow_preload_lines_have_no_trailing_newline() {
        // Regression: `don logs -f` replays the last N ring-buffer lines as the
        // initial snapshot. Ring-buffer entries carry a trailing `\n`, but
        // `SinkLine.line` must be newline-free — the follow route embeds it in a
        // JSON "line" value and the client prints it with its own newline. If
        // the `\n` leaks through, every replayed line renders a blank line after
        // it.
        let (writer, _buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();
        let svc = mgr.service_writer("api").unwrap();

        svc.process_stream(std::io::Cursor::new(b"alpha\nbeta\ngamma\n".to_vec()))
            .await
            .unwrap();

        let mut rx = mgr.add_follow_sink("api", 10, 8).await.unwrap();
        let mut lines: Vec<String> = Vec::new();
        while let Ok(sink_line) = rx.try_recv() {
            let text = String::from_utf8_lossy(&sink_line.line).into_owned();
            assert!(
                !text.ends_with('\n'),
                "preloaded follow line must not carry a trailing newline: {text:?}",
            );
            lines.push(text);
        }
        assert_eq!(lines, vec!["alpha", "beta", "gamma"]);

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
    async fn log_tap_mirrors_the_emitted_stream() {
        struct Case {
            name: &'static str,
            emit: fn(&OutputManager),
            want_name: &'static str,
            want_contains: &'static str,
            want_lifecycle: bool,
        }

        let cases = vec![
            Case {
                name: "top-level lifecycle event",
                emit: |mgr| mgr.lifecycle_event("loading don.toml"),
                want_name: LIFECYCLE_EVENT_NAME,
                want_contains: "loading don.toml",
                want_lifecycle: true,
            },
            Case {
                // Tagged with the service so followers can filter it the way
                // the TUI does.
                name: "service-scoped lifecycle event",
                emit: |mgr| mgr.service_event("postgres", "started"),
                want_name: "postgres",
                want_contains: "postgres: started",
                want_lifecycle: true,
            },
        ];

        for case in cases {
            let (writer, _buf) = TestBuffer::new();
            let config = crate::config::LogConfig::Stdout;
            let mgr = OutputManager::new(&[("postgres", &config)], writer)
                .await
                .unwrap();
            // Subscribe before emitting — the tap is a broadcast, not a ring.
            let mut tap = mgr.log_stream_sender().subscribe();
            (case.emit)(&mgr);
            mgr.shutdown().await;

            let line = tap.recv().await.unwrap();
            assert_eq!(line.name, case.want_name, "{}: name", case.name);
            assert_eq!(
                line.is_lifecycle, case.want_lifecycle,
                "{}: lifecycle flag",
                case.name
            );
            let text = strip_ansi(&line.bytes);
            assert!(
                text.contains(case.want_contains),
                "{}: expected {:?} in {:?}",
                case.name,
                case.want_contains,
                text
            );
        }
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
    async fn item_output_is_a_view_of_the_manager_not_a_copy() {
        struct Case {
            name: &'static str,
            want: &'static str,
            registered: bool,
        }

        let cases = vec![
            Case {
                name: "alpha",
                want: "alpha",
                registered: true,
            },
            Case {
                name: "beta",
                want: "beta",
                registered: true,
            },
            Case {
                name: "never-registered",
                want: "",
                registered: false,
            },
        ];

        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("alpha", &config), ("beta", &config)], writer)
            .await
            .unwrap();

        for case in cases {
            let output = mgr.process_output(case.name);
            assert_eq!(
                output.is_some(),
                case.registered,
                "{}: registered processes get a handle and nothing else does",
                case.name
            );
            if let Some(output) = output {
                assert_eq!(output.name(), case.want, "{}: scoped name", case.name);
                output
                    .writer()
                    .process_stream(std::io::Cursor::new(
                        format!(
                            "{} via handle
",
                            case.name
                        )
                        .into_bytes(),
                    ))
                    .await
                    .unwrap();
            }
        }

        mgr.shutdown().await;

        let out = read_buf(&buf);
        assert!(
            out.contains("alpha via handle"),
            "handle writer reached stdout: {out}"
        );
        assert!(
            out.contains("beta via handle"),
            "handle writer reached stdout: {out}"
        );
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

    #[tokio::test]
    async fn test_runtime_verbose_toggle_updates_emitters_and_stdout_formatting() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new_verbose(&[("svc", &config)], writer, false)
            .await
            .unwrap();
        let verbosity = mgr.verbosity_control();
        let svc = mgr.service_writer("svc").unwrap();

        mgr.debug_event("hidden while verbose is off");
        svc.write_line("before").await;
        tokio::task::yield_now().await;

        verbosity.set_enabled(true);
        mgr.debug_event("visible after toggle");
        svc.write_line("after").await;
        tokio::task::yield_now().await;

        mgr.shutdown().await;

        let output = strip_ansi(read_buf(&buf).as_bytes());
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines.len(),
            3,
            "expected one normal line, one lifecycle line, one verbose line"
        );
        assert!(
            !lines[0]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_digit()),
            "first line should not have a verbose timestamp: {:?}",
            lines[0]
        );
        assert!(
            lines[1].contains("visible after toggle"),
            "lifecycle debug event should become visible after toggle"
        );
        assert!(
            lines[2]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_digit()),
            "third line should gain a verbose timestamp after toggle: {:?}",
            lines[2]
        );
    }

    #[test]
    fn log_filter_passes_table() {
        struct Case {
            name: &'static str,
            allowlist: Option<&'static [&'static str]>,
            line_name: &'static str,
            want: bool,
        }

        let cases = vec![
            Case {
                name: "no filter set passes everything",
                allowlist: None,
                line_name: "api",
                want: true,
            },
            Case {
                name: "no filter set passes don",
                allowlist: None,
                line_name: LIFECYCLE_EVENT_NAME,
                want: true,
            },
            Case {
                name: "filter passes allowlisted name",
                allowlist: Some(&["api"]),
                line_name: "api",
                want: true,
            },
            Case {
                name: "filter rejects non-allowlisted name",
                allowlist: Some(&["api"]),
                line_name: "worker",
                want: false,
            },
            Case {
                name: "filter passes lifecycle even when not in allowlist",
                allowlist: Some(&["api"]),
                line_name: LIFECYCLE_EVENT_NAME,
                want: true,
            },
            Case {
                name: "filter passes empty-name shutdown flush",
                allowlist: Some(&["api"]),
                line_name: "",
                want: true,
            },
            Case {
                name: "empty allowlist still passes lifecycle",
                allowlist: Some(&[]),
                line_name: LIFECYCLE_EVENT_NAME,
                want: true,
            },
            Case {
                name: "empty allowlist drops services",
                allowlist: Some(&[]),
                line_name: "api",
                want: false,
            },
            Case {
                name: "build-tool name is filterable like services",
                allowlist: Some(&["bazel"]),
                line_name: "bazel",
                want: true,
            },
        ];

        for case in cases {
            let filter = LogFilterControl::default();
            if let Some(names) = case.allowlist {
                filter.set(names.iter().map(|s| (*s).to_string()).collect());
            }
            assert_eq!(
                filter.passes(case.line_name),
                case.want,
                "case: {}",
                case.name
            );
        }
    }

    #[tokio::test]
    async fn set_log_filter_drops_non_allowlisted_service_output() {
        let (writer, buf) = TestBuffer::new();
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config), ("worker", &config)], writer)
            .await
            .unwrap();
        mgr.set_log_filter(["api".to_string()].into_iter().collect());

        let api = mgr.service_writer("api").unwrap();
        let worker = mgr.service_writer("worker").unwrap();
        api.process_stream(std::io::Cursor::new(b"api line\n".to_vec()))
            .await
            .unwrap();
        worker
            .process_stream(std::io::Cursor::new(b"worker line\n".to_vec()))
            .await
            .unwrap();
        // Lifecycle events stay visible regardless of the allowlist.
        mgr.lifecycle_event("shutdown complete");
        // Per-service lifecycle events are tagged with the service name, so
        // they follow the allowlist alongside that service's own output.
        mgr.service_event("worker", "send SIGTERM");
        mgr.service_event("api", "ready");
        mgr.shutdown().await;

        let output = strip_ansi(read_buf(&buf).as_bytes());
        assert!(output.contains("api line"), "api stdout should pass");
        assert!(
            !output.contains("worker line"),
            "worker stdout should be filtered out: {output:?}"
        );
        assert!(
            output.contains("shutdown complete"),
            "top-level lifecycle should pass: {output:?}"
        );
        assert!(
            !output.contains("worker: send SIGTERM"),
            "per-service lifecycle for filtered name should drop: {output:?}"
        );
        assert!(
            output.contains("api: ready"),
            "per-service lifecycle for allowlisted name should pass: {output:?}"
        );
    }

    /// Dropping an `OscSinkHandle` (as a restart/stop does when it replaces or
    /// clears `osc_sink`) must remove the sink and stop its task — otherwise
    /// the scanner keeps a gate sender alive, keeping the PTY master's write
    /// half open and leaking one PTY per drop on any exit path that skips
    /// `close_follow_sinks` (e.g. lazy ready-check failures).
    #[tokio::test]
    async fn dropping_osc_sink_handle_removes_sink() {
        let config = crate::config::LogConfig::Stdout;
        let (writer, _buf) = TestBuffer::new();
        let mgr = OutputManager::new(&[("api", &config)], writer)
            .await
            .unwrap();

        let state = mgr.services.get("api").unwrap().clone();
        let before = state.lock().await.sinks.len();

        // Hand a real PTY's write half to a gate, and its sender to the
        // scanner — the shape every PTY-backed wire produces.
        let (pty, _pts) = pty_process::open().unwrap();
        let (_read, write) = pty.into_split();
        let pty_input = spawn_pty_gate(write);
        let handle = mgr.add_osc_sink("api", pty_input).await.unwrap();
        assert_eq!(
            state.lock().await.sinks.len(),
            before + 1,
            "osc sink should be registered"
        );

        drop(handle);

        // Drop aborts the task and spawns the sink removal; let it run.
        let mut removed = false;
        for _ in 0..50 {
            tokio::task::yield_now().await;
            if state.lock().await.sinks.len() == before {
                removed = true;
                break;
            }
        }
        assert!(
            removed,
            "osc sink must be removed when its handle is dropped (PTY leak guard)"
        );
    }
}
