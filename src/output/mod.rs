//! Output handling — line buffering, color-coded prefixing, and lifecycle events.
//!
//! Reads from [`ChildOutput`](crate::process::ChildOutput) streams, buffers per-line,
//! applies service name prefixes with deterministic color assignment, and routes
//! output through [`LogRouter`] to the appropriate destination.
//!
//! Output lines are treated as raw bytes — no UTF-8 assumption. Child processes
//! may emit binary data, non-UTF-8 locales, or raw escape sequences.
//!
//! Each service gets its own [`ServiceOutput`] that can be moved into an independent
//! tokio task, enabling concurrent stream processing without shared mutable state.

pub(crate) mod log_router;
pub mod ring_buffer;

use bytes::Bytes;
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use log_router::{LogRouter, LogRouterError};
use std::collections::HashMap;
use std::io::Write;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

/// Default ring buffer capacity per service (lines).
const DEFAULT_RING_BUFFER_CAPACITY: usize = 10_000;

/// ASCII BEL character — emitted on error events for an audible alert.
const BELL: &str = "\x07";

/// Terminal colors for service name prefixes.
/// Chosen to be distinct and readable on both light and dark terminals.
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

/// Assigns a deterministic color index to a service name.
///
/// Names are sorted first, then assigned colors in order (cycling through
/// the palette). The same set of names always gets the same color assignments
/// regardless of input order.
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
///
/// The prefix is color-coded and padded to `max_name_len` so columns align:
/// `"api      | "` (with ANSI color codes around the name).
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

/// An independent output processor for a single service.
///
/// Owns its [`LogRouter`] (and thus its [`RingBuffer`](ring_buffer::RingBuffer)),
/// prefix bytes, and everything needed to process a stream. Can be moved into
/// its own tokio task for concurrent processing without shared mutable state.
#[derive(Debug)]
pub struct ServiceOutput {
    router: LogRouter,
    prefix: Bytes,
}

impl ServiceOutput {
    /// Process an async readable stream (from a child process) line by line.
    ///
    /// Reads raw bytes, splits on `\n`, and routes each line through the
    /// service's [`LogRouter`]. No UTF-8 assumption — binary output is
    /// handled correctly. Partial lines are buffered until a newline arrives.
    /// This method runs until EOF (the child closes its output).
    pub async fn process_stream<R: AsyncRead + Unpin, W: Write + ?Sized>(
        &mut self,
        reader: R,
        stdout: &mut W,
    ) -> Result<(), OutputError> {
        let mut buf_reader = BufReader::new(reader);
        let mut line_buf = Vec::new();

        loop {
            line_buf.clear();
            match read_until_newline(&mut buf_reader, &mut line_buf).await {
                Ok(0) => break, // EOF
                Ok(_) => {
                    // Strip trailing \n if present (don't store it in the ring buffer).
                    if line_buf.last() == Some(&b'\n') {
                        line_buf.pop();
                    }
                    // Also strip \r for \r\n line endings (PTY on some systems).
                    if line_buf.last() == Some(&b'\r') {
                        line_buf.pop();
                    }
                    self.router
                        .route_line(&line_buf, &self.prefix, stdout)
                        .await?;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    /// Access this service's ring buffer for reading.
    pub fn ring_buffer(&self) -> &ring_buffer::RingBuffer {
        self.router.ring_buffer()
    }
}

/// Manages output configuration for all services — creates [`ServiceOutput`]
/// instances and provides lifecycle event formatting.
///
/// The typical usage is:
/// 1. Create an `OutputManager` with all service names and log configs.
/// 2. Call [`take_service_output`](OutputManager::take_service_output) for each
///    service to get an independent [`ServiceOutput`] that can be moved into
///    its own tokio task.
/// 3. Use the `OutputManager` for lifecycle events (`[don]` messages).
#[derive(Debug)]
pub struct OutputManager {
    /// Per-service log routers (removed as services are taken).
    routers: HashMap<String, LogRouter>,
    /// Per-service formatted prefix bytes (removed as services are taken).
    prefixes: HashMap<String, Bytes>,
    /// The formatted `[don]` prefix, padded to align with service prefixes.
    don_prefix: String,
}

/// Errors from output handling.
#[derive(Debug, thiserror::Error)]
pub enum OutputError {
    /// Log router error (file I/O).
    #[error(transparent)]
    Router(#[from] LogRouterError),
    /// I/O error reading from child output.
    #[error("error reading service output: {0}")]
    Read(#[from] std::io::Error),
}

impl OutputManager {
    /// Create a new output manager for the given services.
    ///
    /// `services` is a list of `(name, log_config)` pairs.
    /// Colors are assigned deterministically based on sorted service names.
    /// Prefixes are padded to the longest name for column alignment.
    pub async fn new(
        services: &[(&str, &crate::config::LogConfig)],
    ) -> Result<Self, OutputError> {
        let names: Vec<&str> = services.iter().map(|(n, _)| *n).collect();
        let color_map = assign_colors(&names);
        // [don] is 5 chars — ensure prefix is at least that wide.
        let max_name_len = names.iter().map(|n| n.len()).max().unwrap_or(0).max(5);

        let mut routers = HashMap::new();
        let mut prefixes = HashMap::new();

        for (name, config) in services {
            let router = LogRouter::new(config, DEFAULT_RING_BUFFER_CAPACITY).await?;
            routers.insert(name.to_string(), router);

            let color_idx = color_map.get(*name).copied().unwrap_or(0);
            prefixes.insert(name.to_string(), format_prefix(name, color_idx, max_name_len));
        }

        let don_prefix = format!(
            "{}{:width$}{} | ",
            SetAttribute(Attribute::Bold),
            "[don]",
            SetAttribute(Attribute::Reset),
            width = max_name_len,
        );

        Ok(Self {
            routers,
            prefixes,
            don_prefix,
        })
    }

    /// Take a service's output processor for independent concurrent use.
    ///
    /// Removes the service's router and prefix from this manager and returns
    /// an owned [`ServiceOutput`] that can be moved into its own tokio task.
    /// Returns `None` if the service was already taken or was never registered.
    pub fn take_service_output(&mut self, name: &str) -> Option<ServiceOutput> {
        let router = self.routers.remove(name)?;
        let prefix = self.prefixes.remove(name)?;
        Some(ServiceOutput { router, prefix })
    }

    /// Emit a `[don]` lifecycle event to stdout.
    pub fn lifecycle_event<W: Write>(&self, message: &str, stdout: &mut W) {
        let _ = writeln!(stdout, "{}{message}", self.don_prefix);
    }

    /// Emit a `[don]` lifecycle event for a specific service to stdout.
    ///
    /// Format: `[don] <service>: <message>`
    pub fn service_event<W: Write>(&self, service: &str, message: &str, stdout: &mut W) {
        let _ = writeln!(stdout, "{}{service}: {message}", self.don_prefix);
    }

    /// Emit a `[don]` error event to stdout with a terminal bell.
    ///
    /// Used for build failures, service crashes, task failures, etc.
    /// The bell character (`\x07`) is emitted so the user gets an audible
    /// alert even if they are in another window.
    pub fn error_event<W: Write>(&self, message: &str, stdout: &mut W) {
        let _ = writeln!(stdout, "{BELL}{}{message}", self.don_prefix);
    }

    /// Emit a `[don]` error event for a specific service with a terminal bell.
    pub fn service_error_event<W: Write>(
        &self,
        service: &str,
        message: &str,
        stdout: &mut W,
    ) {
        let _ = writeln!(stdout, "{BELL}{}{service}: {message}", self.don_prefix);
    }
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
mod tests {
    use super::*;

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
                    "case: {} — color assignment should be deterministic regardless of input order",
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
        assert_eq!(
            unique.len(),
            indices.len(),
            "all services should get distinct colors when count <= palette size"
        );
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
                name: "short name padded",
                service_name: "api",
                max_len: 8,
            },
            Case {
                name: "name equals max len",
                service_name: "postgres",
                max_len: 8,
            },
            Case {
                name: "single char name",
                service_name: "a",
                max_len: 8,
            },
        ];

        for case in cases {
            let prefix = format_prefix(case.service_name, 0, case.max_len);
            let stripped = strip_ansi(&prefix);
            let expected = format!("{:width$} | ", case.service_name, width = case.max_len);
            assert_eq!(
                stripped, expected,
                "case: {} — prefix alignment wrong",
                case.name
            );
        }
    }

    #[test]
    fn test_prefix_columns_align_across_services() {
        let names = ["api", "worker", "postgres"];
        let max_len = names.iter().map(|n| n.len()).max().unwrap();
        let color_map = assign_colors(&names);

        let prefixes: Vec<Bytes> = names
            .iter()
            .map(|n| format_prefix(n, color_map[*n], max_len))
            .collect();

        let stripped: Vec<String> = prefixes.iter().map(|p| strip_ansi(p)).collect();
        let lengths: Vec<usize> = stripped.iter().map(|s| s.len()).collect();
        assert!(
            lengths.windows(2).all(|w| w[0] == w[1]),
            "all prefixes should have equal visible width: {stripped:?}"
        );
    }

    #[tokio::test]
    async fn test_line_buffering_complete_lines() {
        let config = crate::config::LogConfig::Stdout;
        let mut mgr = OutputManager::new(&[("api", &config)]).await.unwrap();
        let mut svc = mgr.take_service_output("api").unwrap();

        let data = b"hello world\nsecond line\n";
        let cursor = std::io::Cursor::new(data.to_vec());
        let mut stdout = Vec::new();

        svc.process_stream(cursor, &mut stdout).await.unwrap();

        let output = String::from_utf8_lossy(&stdout);
        let stripped = strip_ansi(output.as_bytes());
        assert!(stripped.contains("hello world\n"), "should contain first line");
        assert!(stripped.contains("second line\n"), "should contain second line");

        // Ring buffer should have both lines (as bytes).
        assert_eq!(
            svc.ring_buffer().last_n(10),
            vec![b"hello world" as &[u8], b"second line"]
        );
    }

    #[tokio::test]
    async fn test_line_buffering_partial_delivery() {
        let config = crate::config::LogConfig::Stdout;
        let mut mgr = OutputManager::new(&[("svc", &config)]).await.unwrap();
        let mut svc = mgr.take_service_output("svc").unwrap();

        // SlowReader delivers one byte at a time.
        let data = b"hello world\nsecond line\n".to_vec();
        let reader = SlowReader::new(data, 1);
        let mut stdout = Vec::new();

        svc.process_stream(reader, &mut stdout).await.unwrap();

        assert_eq!(
            svc.ring_buffer().last_n(10),
            vec![b"hello world" as &[u8], b"second line"]
        );

        // Verify each output line has the prefix (no partial lines leaked).
        let output = String::from_utf8_lossy(&stdout);
        for line in output.lines() {
            let stripped = strip_ansi(line.as_bytes());
            assert!(
                stripped.contains(" | "),
                "every line must have the prefix separator, got: {stripped:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_line_buffering_no_trailing_newline() {
        let config = crate::config::LogConfig::Stdout;
        let mut mgr = OutputManager::new(&[("svc", &config)]).await.unwrap();
        let mut svc = mgr.take_service_output("svc").unwrap();

        let data = b"line one\npartial";
        let cursor = std::io::Cursor::new(data.to_vec());
        let mut stdout = Vec::new();

        svc.process_stream(cursor, &mut stdout).await.unwrap();

        assert_eq!(
            svc.ring_buffer().last_n(10),
            vec![b"line one" as &[u8], b"partial"]
        );
    }

    #[tokio::test]
    async fn test_non_utf8_output_handled() {
        let config = crate::config::LogConfig::Stdout;
        let mut mgr = OutputManager::new(&[("bin", &config)]).await.unwrap();
        let mut svc = mgr.take_service_output("bin").unwrap();

        // Invalid UTF-8 bytes.
        let data: Vec<u8> = vec![0xff, 0xfe, b'h', b'i', b'\n', 0x80, 0x81, b'\n'];
        let cursor = std::io::Cursor::new(data);
        let mut stdout = Vec::new();

        svc.process_stream(cursor, &mut stdout).await.unwrap();

        let lines = svc.ring_buffer().last_n(10);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], &[0xff, 0xfe, b'h', b'i']);
        assert_eq!(lines[1], &[0x80, 0x81]);
    }

    #[tokio::test]
    async fn test_take_service_output_enables_independent_processing() {
        let config_a = crate::config::LogConfig::Stdout;
        let config_b = crate::config::LogConfig::Stdout;
        let mut mgr =
            OutputManager::new(&[("alpha", &config_a), ("beta", &config_b)]).await.unwrap();

        let mut alpha = mgr.take_service_output("alpha").unwrap();
        let mut beta = mgr.take_service_output("beta").unwrap();

        assert!(mgr.take_service_output("alpha").is_none());
        assert!(mgr.take_service_output("beta").is_none());

        let data_a = std::io::Cursor::new(b"alpha line 1\nalpha line 2\n".to_vec());
        let data_b = std::io::Cursor::new(b"beta line 1\nbeta line 2\n".to_vec());

        let mut stdout_a = Vec::new();
        let mut stdout_b = Vec::new();

        let (result_a, result_b) = tokio::join!(
            alpha.process_stream(data_a, &mut stdout_a),
            beta.process_stream(data_b, &mut stdout_b),
        );
        result_a.unwrap();
        result_b.unwrap();

        assert_eq!(
            alpha.ring_buffer().last_n(10),
            vec![b"alpha line 1" as &[u8], b"alpha line 2"]
        );
        assert_eq!(
            beta.ring_buffer().last_n(10),
            vec![b"beta line 1" as &[u8], b"beta line 2"]
        );

        // Each stdout has only its own prefixed output.
        assert!(stdout_a.windows(5).any(|w| w == b"alpha") && !stdout_a.windows(4).any(|w| w == b"beta"));
        assert!(stdout_b.windows(4).any(|w| w == b"beta") && !stdout_b.windows(5).any(|w| w == b"alpha"));
    }

    #[tokio::test]
    async fn test_concurrent_service_outputs_no_interleave() {
        let config = crate::config::LogConfig::Stdout;
        let mut mgr =
            OutputManager::new(&[("alpha", &config), ("beta", &config)]).await.unwrap();

        let mut alpha = mgr.take_service_output("alpha").unwrap();
        let mut beta = mgr.take_service_output("beta").unwrap();

        let make_data = |prefix: &str, count: usize| -> Vec<u8> {
            let mut data = String::new();
            for i in 0..count {
                data.push_str(&format!("{prefix} output line {i}\n"));
            }
            data.into_bytes()
        };

        let data_a = std::io::Cursor::new(make_data("alpha", 50));
        let data_b = std::io::Cursor::new(make_data("beta", 50));

        let mut stdout_a = Vec::new();
        let mut stdout_b = Vec::new();

        let (r_a, r_b) = tokio::join!(
            alpha.process_stream(data_a, &mut stdout_a),
            beta.process_stream(data_b, &mut stdout_b),
        );
        r_a.unwrap();
        r_b.unwrap();

        for (label, stdout) in [("alpha", &stdout_a), ("beta", &stdout_b)] {
            let output = String::from_utf8_lossy(stdout);
            for line in output.lines() {
                assert!(
                    line.contains(" | "),
                    "{label}: every line should have prefix separator, got: {line:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_take_returns_none_for_unknown() {
        let config = crate::config::LogConfig::Stdout;
        let mut mgr = OutputManager::new(&[("api", &config)]).await.unwrap();
        assert!(mgr.take_service_output("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_take_returns_none_on_second_call() {
        let config = crate::config::LogConfig::Stdout;
        let mut mgr = OutputManager::new(&[("api", &config)]).await.unwrap();
        assert!(mgr.take_service_output("api").is_some());
        assert!(mgr.take_service_output("api").is_none());
    }

    #[tokio::test]
    async fn test_empty_service_list() {
        let mgr = OutputManager::new(&[]).await.unwrap();
        let mut stdout = Vec::new();
        mgr.lifecycle_event("hello", &mut stdout);
        let output = strip_ansi(&stdout);
        assert!(output.contains("[don]"), "should still have [don] prefix: {output:?}");
        assert!(output.contains("hello"), "should contain message: {output:?}");
    }

    #[tokio::test]
    async fn test_lifecycle_event_format() {
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("postgres", &config)]).await.unwrap();

        let mut stdout = Vec::new();
        mgr.lifecycle_event("loading don.toml", &mut stdout);
        let output = strip_ansi(&stdout);
        assert!(
            output.contains("[don]") && output.contains("loading don.toml"),
            "lifecycle event should have [don] prefix: {output:?}"
        );
    }

    #[tokio::test]
    async fn test_service_event_format() {
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)]).await.unwrap();

        let mut stdout = Vec::new();
        mgr.service_event("api", "file change detected (src/main.rs)", &mut stdout);
        let output = strip_ansi(&stdout);
        assert!(
            output.contains("[don]") && output.contains("api: file change detected"),
            "service event should have [don] prefix and service name: {output:?}"
        );
    }

    #[tokio::test]
    async fn test_error_event_includes_bell() {
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("api", &config)]).await.unwrap();

        let mut stdout = Vec::new();
        mgr.error_event("build failed (exit code 1)", &mut stdout);
        let output = String::from_utf8(stdout).unwrap();
        assert!(output.contains(BELL), "error events should include terminal bell");
        let bell_pos = output.find(BELL).unwrap();
        let msg_pos = output.find("build failed").unwrap();
        assert!(
            bell_pos < msg_pos,
            "bell should precede the message: bell at {bell_pos}, msg at {msg_pos}"
        );
    }

    #[tokio::test]
    async fn test_service_error_event_includes_bell_and_name() {
        let config = crate::config::LogConfig::Stdout;
        let mgr = OutputManager::new(&[("worker", &config)]).await.unwrap();

        let mut stdout = Vec::new();
        mgr.service_error_event("worker", "exited with code 1", &mut stdout);
        let output = String::from_utf8(stdout).unwrap();
        assert!(output.contains(BELL), "service error should include terminal bell");
        let stripped = strip_ansi(output.as_bytes());
        assert!(
            stripped.contains("worker: exited with code 1"),
            "should contain service name and message: {stripped:?}"
        );
    }

    /// Strip ANSI escape sequences from bytes, returning a String for easy assertion.
    fn strip_ansi(s: &[u8]) -> String {
        let mut result = Vec::with_capacity(s.len());
        let mut i = 0;
        while i < s.len() {
            if s[i] == b'\x1b' {
                // Skip until 'm' (end of ANSI sequence).
                i += 1;
                while i < s.len() && s[i] != b'm' {
                    i += 1;
                }
                i += 1; // skip the 'm'
            } else {
                result.push(s[i]);
                i += 1;
            }
        }
        String::from_utf8_lossy(&result).into_owned()
    }

    /// An AsyncRead adapter that delivers data in small chunks to simulate
    /// fragmented/partial delivery from a real network or PTY.
    struct SlowReader {
        data: Vec<u8>,
        pos: usize,
        chunk_size: usize,
    }

    impl SlowReader {
        fn new(data: Vec<u8>, chunk_size: usize) -> Self {
            Self {
                data,
                pos: 0,
                chunk_size,
            }
        }
    }

    impl AsyncRead for SlowReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if self.pos >= self.data.len() {
                return std::task::Poll::Ready(Ok(()));
            }
            let remaining = &self.data[self.pos..];
            let to_read = remaining.len().min(self.chunk_size).min(buf.remaining());
            buf.put_slice(&remaining[..to_read]);
            self.pos += to_read;
            std::task::Poll::Ready(Ok(()))
        }
    }
}
