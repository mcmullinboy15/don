//! Log routing — directs service output to the appropriate destination
//! based on [`LogConfig`]: stdout (with prefix), file (raw), or ignore (discard).
//!
//! All modes feed the per-service ring buffer so `don logs` always works.
//! File writes use async I/O via `tokio::fs::File` to avoid blocking the runtime.

use bytes::Bytes;
use crate::config::LogConfig;
use crate::output::ring_buffer::RingBuffer;
use std::io::Write;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

/// Routes output lines for a single service based on its [`LogConfig`].
///
/// Every line pushed is stored in the ring buffer regardless of routing mode.
/// The routing mode only controls where the line is _also_ sent:
/// - `Stdout`: written to the provided writer with the service prefix (sync)
/// - `File`: written to a file without prefix via async I/O (raw)
/// - `Ignore`: discarded (ring buffer still fed)
#[derive(Debug)]
pub(crate) struct LogRouter {
    ring_buffer: RingBuffer,
    destination: Destination,
}

#[derive(Debug)]
enum Destination {
    Stdout,
    File { file: tokio::fs::File, path: PathBuf },
    Ignore,
}

/// Errors from log routing operations.
#[derive(Debug, thiserror::Error)]
pub enum LogRouterError {
    /// Failed to open or write to the log file.
    #[error("log file error for '{path}': {source}")]
    FileError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl LogRouter {
    /// Create a new log router for the given config.
    ///
    /// For `LogConfig::File`, the file is opened (created/appended) immediately
    /// using async I/O. The ring buffer is created with the given capacity.
    pub(crate) async fn new(
        config: &LogConfig,
        ring_buffer_capacity: usize,
    ) -> Result<Self, LogRouterError> {
        let destination = match config {
            LogConfig::Stdout => Destination::Stdout,
            LogConfig::Ignore => Destination::Ignore,
            LogConfig::File(path) => {
                // Ensure parent directory exists.
                if let Some(parent) = path.parent() {
                    let os_str = parent.as_os_str();
                    if !os_str.is_empty() {
                        tokio::fs::create_dir_all(parent).await.map_err(|source| {
                            LogRouterError::FileError {
                                path: path.clone(),
                                source,
                            }
                        })?;
                    }
                }
                let file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .await
                    .map_err(|source| LogRouterError::FileError {
                        path: path.clone(),
                        source,
                    })?;
                Destination::File {
                    file,
                    path: path.clone(),
                }
            }
        };

        Ok(Self {
            ring_buffer: RingBuffer::new(ring_buffer_capacity),
            destination,
        })
    }

    /// Route a line of output. The line should NOT include a trailing newline.
    ///
    /// Lines are raw bytes — no UTF-8 assumption. The ring buffer stores them as-is.
    ///
    /// - Always pushes to the ring buffer (raw, no prefix).
    /// - For stdout mode: writes `prefix + line + \n` to `stdout_writer` (sync).
    /// - For file mode: writes `line + \n` to the log file (async).
    /// - For ignore mode: does nothing beyond the ring buffer push.
    ///
    /// `prefix` is the formatted, color-coded service name prefix bytes.
    pub(crate) async fn route_line<W: Write + ?Sized>(
        &mut self,
        line: &[u8],
        prefix: &[u8],
        stdout_writer: &mut W,
    ) -> Result<(), LogRouterError> {
        // Always feed the ring buffer with raw output.
        self.ring_buffer.push(Bytes::copy_from_slice(line));

        match &mut self.destination {
            Destination::Stdout => {
                // Prefix + line + newline to stdout.
                // Write errors to stdout are not fatal — the terminal might be gone.
                let _ = stdout_writer.write_all(prefix);
                let _ = stdout_writer.write_all(line);
                let _ = stdout_writer.write_all(b"\n");
            }
            Destination::File { file, path } => {
                // Raw line + newline to file (async).
                file.write_all(line).await.map_err(|source| LogRouterError::FileError {
                    path: path.clone(),
                    source,
                })?;
                file.write_all(b"\n").await.map_err(|source| LogRouterError::FileError {
                    path: path.clone(),
                    source,
                })?;
            }
            Destination::Ignore => {
                // Discard — ring buffer already fed above.
            }
        }

        Ok(())
    }

    /// Access the ring buffer for reading.
    pub(crate) fn ring_buffer(&self) -> &RingBuffer {
        &self.ring_buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_log_routing() {
        struct Case {
            name: &'static str,
            config: LogConfig,
            lines: Vec<&'static [u8]>,
            prefix: &'static [u8],
            expect_stdout_contains: Vec<&'static [u8]>,
            expect_stdout_not_contains: Vec<&'static [u8]>,
            expect_ring_buffer_lines: Vec<&'static [u8]>,
        }

        let cases = vec![
            Case {
                name: "stdout mode — output appears with prefix",
                config: LogConfig::Stdout,
                lines: vec![b"hello world", b"second line"],
                prefix: b"api      | ",
                expect_stdout_contains: vec![b"api      | hello world\n", b"api      | second line\n"],
                expect_stdout_not_contains: vec![],
                expect_ring_buffer_lines: vec![b"hello world", b"second line"],
            },
            Case {
                name: "ignore mode — no stdout, ring buffer still fed",
                config: LogConfig::Ignore,
                lines: vec![b"hello world", b"secret line"],
                prefix: b"api      | ",
                expect_stdout_contains: vec![],
                expect_stdout_not_contains: vec![b"hello world", b"secret line"],
                expect_ring_buffer_lines: vec![b"hello world", b"secret line"],
            },
        ];

        for case in cases {
            let mut router = LogRouter::new(&case.config, 1000).await.unwrap();
            let mut stdout = Vec::new();

            for line in &case.lines {
                router.route_line(line, case.prefix, &mut stdout).await.unwrap();
            }

            for expected in &case.expect_stdout_contains {
                assert!(
                    contains_bytes(&stdout, expected),
                    "case: {} — stdout should contain {:?}",
                    case.name,
                    String::from_utf8_lossy(expected),
                );
            }

            for not_expected in &case.expect_stdout_not_contains {
                assert!(
                    !contains_bytes(&stdout, not_expected),
                    "case: {} — stdout should NOT contain {:?}",
                    case.name,
                    String::from_utf8_lossy(not_expected),
                );
            }

            let ring_lines = router.ring_buffer().last_n(100);
            let expected_refs: Vec<&[u8]> = case.expect_ring_buffer_lines.to_vec();
            assert_eq!(
                ring_lines, expected_refs,
                "case: {} — ring buffer mismatch",
                case.name
            );
        }
    }

    #[tokio::test]
    async fn test_file_mode_writes_raw_and_feeds_ring_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let config = LogConfig::File(log_path.clone());

        let mut router = LogRouter::new(&config, 1000).await.unwrap();
        let mut stdout = Vec::new();

        router.route_line(b"raw line 1", b"svc | ", &mut stdout).await.unwrap();
        router.route_line(b"raw line 2", b"svc | ", &mut stdout).await.unwrap();

        // Stdout should be empty (file mode doesn't write to stdout).
        assert!(stdout.is_empty(), "file mode should not write to stdout");

        // File should contain raw lines without prefix.
        let file_content = tokio::fs::read_to_string(&log_path).await.unwrap();
        assert_eq!(file_content, "raw line 1\nraw line 2\n");

        // Ring buffer should still have the lines.
        let ring_lines = router.ring_buffer().last_n(10);
        assert_eq!(ring_lines, vec![b"raw line 1" as &[u8], b"raw line 2"]);
    }

    #[tokio::test]
    async fn test_file_mode_creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("nested").join("dir").join("test.log");
        let config = LogConfig::File(log_path.clone());

        let router = LogRouter::new(&config, 10).await;
        assert!(router.is_ok(), "should create parent directories");
        assert!(log_path.exists(), "log file should exist");
    }

    /// Check if `haystack` contains the byte subsequence `needle`.
    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }
}
