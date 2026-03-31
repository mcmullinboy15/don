//! Bounded per-service ring buffer for recent output lines.
//!
//! Stores raw output lines (without prefix) as [`Bytes`] in a bounded buffer.
//! All output feeds the ring buffer regardless of log routing, so
//! `don logs <name> --last N` always works even for `log = "ignore"` services.

use bytes::Bytes;
use std::collections::VecDeque;

/// A bounded buffer that stores the most recent N lines of output.
///
/// Lines are stored as raw [`Bytes`] — no UTF-8 assumption. When the buffer
/// is full, the oldest line is evicted. The total number of lines ever written
/// is tracked separately from the buffer contents.
#[derive(Debug)]
pub struct RingBuffer {
    lines: VecDeque<Bytes>,
    /// Total number of lines ever pushed (including evicted ones).
    total_written: usize,
    /// Maximum number of lines to store.
    capacity: usize,
}

impl RingBuffer {
    /// Create a new ring buffer with the given line capacity.
    ///
    /// A capacity of 0 is handled gracefully — lines are counted but never stored.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            total_written: 0,
            capacity,
        }
    }

    /// Push a line into the buffer. If full, the oldest line is evicted.
    pub(crate) fn push(&mut self, line: Bytes) {
        self.total_written += 1;
        if self.capacity == 0 {
            return;
        }
        if self.lines.len() == self.capacity {
            self.lines.pop_front();
        }
        self.lines.push_back(line);
    }

    /// Read the last `n` lines in chronological order.
    ///
    /// If `n` exceeds the number of stored lines, returns all stored lines.
    pub fn last_n(&self, n: usize) -> Vec<&[u8]> {
        let stored = self.lines.len();
        let count = n.min(stored);
        self.lines
            .range(stored - count..)
            .map(|b| b.as_ref())
            .collect()
    }

    /// Total number of lines ever pushed (including evicted ones).
    pub fn total_written(&self) -> usize {
        self.total_written
    }

    /// Number of lines currently stored in the buffer.
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_behavior() {
        struct Case {
            name: &'static str,
            capacity: usize,
            lines: Vec<&'static [u8]>,
            last_n: usize,
            expected: Vec<&'static [u8]>,
            expected_total: usize,
            expected_len: usize,
        }

        let cases = vec![
            Case {
                name: "empty buffer returns empty",
                capacity: 10,
                lines: vec![],
                last_n: 5,
                expected: vec![],
                expected_total: 0,
                expected_len: 0,
            },
            Case {
                name: "single line works",
                capacity: 10,
                lines: vec![b"hello"],
                last_n: 5,
                expected: vec![b"hello" as &[u8]],
                expected_total: 1,
                expected_len: 1,
            },
            Case {
                name: "last_n=0 returns empty",
                capacity: 10,
                lines: vec![b"hello"],
                last_n: 0,
                expected: vec![],
                expected_total: 1,
                expected_len: 1,
            },
            Case {
                name: "under capacity — all lines stored",
                capacity: 5,
                lines: vec![b"a", b"b", b"c"],
                last_n: 10,
                expected: vec![b"a" as &[u8], b"b", b"c"],
                expected_total: 3,
                expected_len: 3,
            },
            Case {
                name: "at capacity — all lines stored",
                capacity: 3,
                lines: vec![b"a", b"b", b"c"],
                last_n: 3,
                expected: vec![b"a" as &[u8], b"b", b"c"],
                expected_total: 3,
                expected_len: 3,
            },
            Case {
                name: "over capacity — oldest evicted, order preserved",
                capacity: 3,
                lines: vec![b"a", b"b", b"c", b"d", b"e"],
                last_n: 3,
                expected: vec![b"c" as &[u8], b"d", b"e"],
                expected_total: 5,
                expected_len: 3,
            },
            Case {
                name: "over capacity — request fewer than stored",
                capacity: 3,
                lines: vec![b"a", b"b", b"c", b"d", b"e"],
                last_n: 2,
                expected: vec![b"d" as &[u8], b"e"],
                expected_total: 5,
                expected_len: 3,
            },
            Case {
                name: "way over capacity — wraps multiple times",
                capacity: 2,
                lines: vec![b"a", b"b", b"c", b"d", b"e", b"f", b"g"],
                last_n: 2,
                expected: vec![b"f" as &[u8], b"g"],
                expected_total: 7,
                expected_len: 2,
            },
            Case {
                name: "zero capacity — nothing stored",
                capacity: 0,
                lines: vec![b"a", b"b"],
                last_n: 5,
                expected: vec![],
                expected_total: 2,
                expected_len: 0,
            },
            Case {
                name: "non-utf8 bytes stored correctly",
                capacity: 5,
                lines: vec![b"\xff\xfe\x00\x01"],
                last_n: 5,
                expected: vec![b"\xff\xfe\x00\x01" as &[u8]],
                expected_total: 1,
                expected_len: 1,
            },
        ];

        for case in cases {
            let mut buf = RingBuffer::new(case.capacity);
            for line in case.lines {
                buf.push(Bytes::from_static(line));
            }
            assert_eq!(
                buf.last_n(case.last_n),
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
            assert_eq!(buf.len(), case.expected_len, "case: {} (len)", case.name);
        }
    }

    #[test]
    fn test_is_empty() {
        let mut buf = RingBuffer::new(5);
        assert!(buf.is_empty());
        buf.push(Bytes::from_static(b"x"));
        assert!(!buf.is_empty());
    }
}
