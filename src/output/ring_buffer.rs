//! Bounded per-service ring buffer for recent output.
//!
//! Stores output entries as [`Bytes`] in a bounded buffer. Each entry is
//! typically a complete line (including the `\n` delimiter). Raw byte chunks
//! are ingested via [`push_chunk`], which accumulates bytes and splits on
//! newlines. All output feeds the ring buffer regardless of log routing, so
//! `don logs <name> --last N` always works even for `log = "ignore"` services.

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;

/// Maximum bytes to accumulate before forcing a flush (even without a `\n`).
/// Prevents unbounded memory growth from binary output or programs that
/// never emit newlines.
const MAX_PENDING: usize = 16 * 1024;

/// A bounded buffer that stores the most recent N entries of output.
///
/// Entries are stored as raw [`Bytes`] — no UTF-8 assumption. When the buffer
/// is full, the oldest entry is evicted. The total number of entries ever
/// written is tracked separately from the buffer contents.
#[derive(Debug)]
pub struct RingBuffer {
    entries: VecDeque<Bytes>,
    /// Accumulator for the current incomplete line.
    pending: BytesMut,
    /// Total number of entries ever pushed (including evicted ones).
    total_written: usize,
    /// Maximum number of entries to store.
    capacity: usize,
}

impl RingBuffer {
    /// Create a new ring buffer with the given entry capacity.
    ///
    /// A capacity of 0 is handled gracefully — entries are counted but never stored.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            pending: BytesMut::new(),
            total_written: 0,
            capacity,
        }
    }

    /// Ingest a raw byte chunk. Bytes are accumulated in an internal buffer
    /// and split into entries on each `\n`. Entries include the `\n` delimiter.
    /// If the accumulator exceeds [`MAX_PENDING`] without a `\n`, it is
    /// flushed as an overflow entry.
    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) {
        for &byte in chunk {
            self.pending.extend_from_slice(&[byte]);
            if byte == b'\n' || self.pending.len() >= MAX_PENDING {
                let entry = self.pending.split().freeze();
                self.push_entry(entry);
            }
        }
    }

    /// Flush any remaining bytes in the accumulator as a final entry.
    /// Called at EOF to ensure crash output without a trailing `\n` is preserved.
    pub(crate) fn flush_pending(&mut self) {
        if !self.pending.is_empty() {
            let entry = self.pending.split().freeze();
            self.push_entry(entry);
        }
    }

    /// Push a complete line (without `\n`) into the buffer, appending `\n`.
    /// Used for structured output (Docker build, downloads) and tests.
    #[cfg(test)]
    pub(crate) fn push_line(&mut self, line: Bytes) {
        // Flush any pending bytes first so ordering is preserved.
        self.flush_pending();
        let mut entry = BytesMut::with_capacity(line.len() + 1);
        entry.extend_from_slice(&line);
        entry.extend_from_slice(b"\n");
        self.push_entry(entry.freeze());
    }

    /// Push an entry into the buffer, evicting the oldest if full.
    fn push_entry(&mut self, entry: Bytes) {
        self.total_written += 1;
        if self.capacity == 0 {
            return;
        }
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    /// Iterate over the last `n` entries in chronological order.
    ///
    /// If `n` exceeds the number of stored entries, all stored entries are yielded.
    pub fn last_n(&self, n: usize) -> impl Iterator<Item = &[u8]> {
        let stored = self.entries.len();
        let count = n.min(stored);
        self.entries.range(stored - count..).map(|b| b.as_ref())
    }

    /// Total number of entries ever pushed (including evicted ones).
    pub fn total_written(&self) -> usize {
        self.total_written
    }

    /// Number of entries currently stored in the buffer.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_push_chunk_basic() {
        struct Case {
            name: &'static str,
            capacity: usize,
            chunks: Vec<&'static [u8]>,
            flush: bool,
            last_n: usize,
            expected: Vec<&'static [u8]>,
            expected_total: usize,
        }

        let cases = vec![
            Case {
                name: "single complete line",
                capacity: 10,
                chunks: vec![b"hello\n"],
                flush: false,
                last_n: 10,
                expected: vec![b"hello\n" as &[u8]],
                expected_total: 1,
            },
            Case {
                name: "two lines in one chunk",
                capacity: 10,
                chunks: vec![b"line1\nline2\n"],
                flush: false,
                last_n: 10,
                expected: vec![b"line1\n" as &[u8], b"line2\n"],
                expected_total: 2,
            },
            Case {
                name: "partial line across chunks",
                capacity: 10,
                chunks: vec![b"hel", b"lo\n"],
                flush: false,
                last_n: 10,
                expected: vec![b"hello\n" as &[u8]],
                expected_total: 1,
            },
            Case {
                name: "partial line flushed at EOF",
                capacity: 10,
                chunks: vec![b"no newline"],
                flush: true,
                last_n: 10,
                expected: vec![b"no newline" as &[u8]],
                expected_total: 1,
            },
            Case {
                name: "partial line not visible without flush",
                capacity: 10,
                chunks: vec![b"pending"],
                flush: false,
                last_n: 10,
                expected: vec![],
                expected_total: 0,
            },
            Case {
                name: "CRLF preserved in entry",
                capacity: 10,
                chunks: vec![b"hello\r\n"],
                flush: false,
                last_n: 10,
                expected: vec![b"hello\r\n" as &[u8]],
                expected_total: 1,
            },
            Case {
                name: "eviction at capacity",
                capacity: 2,
                chunks: vec![b"a\nb\nc\n"],
                flush: false,
                last_n: 10,
                expected: vec![b"b\n" as &[u8], b"c\n"],
                expected_total: 3,
            },
            Case {
                name: "zero capacity counts but doesn't store",
                capacity: 0,
                chunks: vec![b"hello\n"],
                flush: false,
                last_n: 10,
                expected: vec![],
                expected_total: 1,
            },
            Case {
                name: "empty chunk is no-op",
                capacity: 10,
                chunks: vec![b"" as &[u8]],
                flush: false,
                last_n: 10,
                expected: vec![],
                expected_total: 0,
            },
        ];

        for case in cases {
            let mut buf = RingBuffer::new(case.capacity);
            for chunk in case.chunks {
                buf.push_chunk(chunk);
            }
            if case.flush {
                buf.flush_pending();
            }
            assert_eq!(
                buf.last_n(case.last_n).collect::<Vec<_>>(),
                case.expected,
                "case: {}",
                case.name
            );
            assert_eq!(
                buf.total_written(),
                case.expected_total,
                "case: {} (total_written)",
                case.name
            );
        }
    }

    #[test]
    fn test_push_chunk_overflow() {
        let mut buf = RingBuffer::new(100);
        // Write more than MAX_PENDING bytes without a newline.
        let big_chunk = vec![b'x'; MAX_PENDING + 100];
        buf.push_chunk(&big_chunk);

        // Should have flushed at MAX_PENDING, leaving 100 bytes pending.
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.last_n(1).next().unwrap().len(), MAX_PENDING);

        // Flush the remaining.
        buf.flush_pending();
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.last_n(1).next().unwrap().len(), 100);
    }

    #[test]
    fn test_push_line() {
        let mut buf = RingBuffer::new(10);
        buf.push_line(Bytes::from_static(b"hello"));

        let entries: Vec<_> = buf.last_n(10).collect();
        assert_eq!(entries, vec![b"hello\n" as &[u8]]);
        assert_eq!(buf.total_written(), 1);
    }

    #[test]
    fn test_push_line_flushes_pending() {
        let mut buf = RingBuffer::new(10);
        buf.push_chunk(b"partial");
        buf.push_line(Bytes::from_static(b"complete"));

        let entries: Vec<_> = buf.last_n(10).collect();
        assert_eq!(entries, vec![b"partial" as &[u8], b"complete\n"]);
        assert_eq!(buf.total_written(), 2);
    }

    #[test]
    fn test_flush_pending_idempotent() {
        let mut buf = RingBuffer::new(10);
        buf.flush_pending(); // no-op on empty
        assert_eq!(buf.len(), 0);

        buf.push_chunk(b"data");
        buf.flush_pending();
        buf.flush_pending(); // second flush is no-op
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn test_is_empty() {
        let mut buf = RingBuffer::new(5);
        assert!(buf.is_empty());
        buf.push_chunk(b"x\n");
        assert!(!buf.is_empty());
    }
}
