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

pub mod ring_buffer;

use bytes::Bytes;
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use ring_buffer::RingBuffer;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
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
    /// Process an async readable stream (from a child process) line by line.
    ///
    /// Reads raw bytes, splits on `\n`, pushes each line to the ring buffer,
    /// and fans out to all current sinks. No UTF-8 assumption — binary output
    /// is handled correctly. Runs until EOF (the child closes its output).
    pub async fn process_stream<R: AsyncRead + Unpin>(&self, reader: R) -> Result<(), OutputError> {
        let mut buf_reader = BufReader::new(reader);
        let mut line_buf = Vec::new();

        loop {
            line_buf.clear();
            match read_until_newline(&mut buf_reader, &mut line_buf).await {
                Ok(0) => break, // EOF
                Ok(_) => {
                    // Strip trailing \n and \r.
                    if line_buf.last() == Some(&b'\n') {
                        line_buf.pop();
                    }
                    if line_buf.last() == Some(&b'\r') {
                        line_buf.pop();
                    }
                    let line = Bytes::copy_from_slice(&line_buf);

                    // Lock: push to ring buffer + snapshot sinks. Released before sends.
                    // Prune closed sinks (e.g. disconnected follow clients) inline.
                    let (prefix, sinks) = {
                        let mut state = self.state.lock().await;
                        state.sinks.retain(|s| !s.tx.is_closed());
                        state.ring_buffer.push(line.clone());
                        (state.prefix.clone(), state.sinks.clone())
                    };

                    let mut dropped: Vec<mpsc::Sender<SinkLine>> = Vec::new();
                    for sink in &sinks {
                        let msg = SinkLine {
                            prefix: prefix.clone(),
                            line: line.clone(),
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

        Ok(())
    }

    /// Write a single line to the ring buffer and sinks.
    ///
    /// Used for structured output like Docker build progress that arrives
    /// as individual text lines rather than a byte stream.
    pub async fn write_line(&self, line: &str) {
        let line = Bytes::from(line.to_string());
        let (prefix, sinks) = {
            let mut state = self.state.lock().await;
            state.sinks.retain(|s| !s.tx.is_closed());
            state.ring_buffer.push(line.clone());
            (state.prefix.clone(), state.sinks.clone())
        };
        let mut dropped: Vec<mpsc::Sender<SinkLine>> = Vec::new();
        for sink in &sinks {
            let msg = SinkLine {
                prefix: prefix.clone(),
                line: line.clone(),
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
        let names: Vec<&str> = services.iter().map(|(n, _)| *n).collect();
        let color_map = assign_colors(&names);
        let max_name_len = names.iter().map(|n| n.len()).max().unwrap_or(0).max(5);

        // Spawn stdout sink task.
        let (stdout_tx, stdout_rx) = mpsc::channel(SINK_CHANNEL_CAPACITY);
        let stdout_handle = tokio::spawn(stdout_sink_task(stdout_rx, writer));
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

    /// Read the last N lines from a service's ring buffer, joined by newlines.
    ///
    /// Returns `None` if the service is not registered.
    pub async fn read_logs(&self, name: &str, n: usize) -> Option<Bytes> {
        let state_arc = self.services.get(name)?;
        let state = state_arc.lock().await;
        let parts: Vec<&[u8]> = state.ring_buffer.last_n(n).collect();
        Some(Bytes::from(parts.join(b"\n" as &[u8])))
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
            })),
        );
    }

    /// Emit a `[don]` lifecycle event.
    pub fn lifecycle_event(&self, message: &str) {
        let _ = self.stdout_sink.tx.try_send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(message.to_string()),
        });
    }

    /// Emit a `[don]` lifecycle event for a specific service.
    pub fn service_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.tx.try_send(SinkLine {
            prefix: Bytes::from(self.don_prefix.clone()),
            line: Bytes::from(format!("{service}: {message}")),
        });
    }

    /// Emit a `[don]` error event with a terminal bell.
    pub fn error_event(&self, message: &str) {
        let _ = self.stdout_sink.tx.try_send(SinkLine {
            prefix: Bytes::from(format!("{BELL}{}", self.don_prefix)),
            line: Bytes::from(message.to_string()),
        });
    }

    /// Emit a `[don]` error event for a specific service with a terminal bell.
    pub fn service_error_event(&self, service: &str, message: &str) {
        let _ = self.stdout_sink.tx.try_send(SinkLine {
            prefix: Bytes::from(format!("{BELL}{}", self.don_prefix)),
            line: Bytes::from(format!("{service}: {message}")),
        });
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

/// Stdout sink writer task. Receives lines and writes them to the provided writer.
/// Runs until all senders are dropped.
async fn stdout_sink_task<W: tokio::io::AsyncWrite + Unpin + Send>(
    mut rx: mpsc::Receiver<SinkLine>,
    mut writer: W,
) {
    use tokio::io::AsyncWriteExt;
    while let Some(msg) = rx.recv().await {
        let _ = writer.write_all(&msg.prefix).await;
        let _ = writer.write_all(&msg.line).await;
        let _ = writer.write_all(b"\n").await;
    }
}

/// File sink writer task. Receives lines and writes raw output (no prefix) to a file.
/// Runs until all senders are dropped.
async fn file_sink_task(mut rx: mpsc::Receiver<SinkLine>, mut file: tokio::fs::File) {
    use tokio::io::AsyncWriteExt;
    while let Some(msg) = rx.recv().await {
        let _ = file.write_all(&msg.line).await;
        let _ = file.write_all(b"\n").await;
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

/// Read bytes until `\n` or EOF, appending to `buf`.
///
/// Returns the number of bytes read (0 = EOF).
/// For PTY reads, an `EIO` error signals the child exited — treated as EOF.
async fn read_until_newline<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    buf: &mut Vec<u8>,
) -> Result<usize, OutputError> {
    match reader.read_until(b'\n', buf).await {
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
