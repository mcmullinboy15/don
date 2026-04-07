//! Detect and respond to terminal query escape sequences.
//!
//! Some programs (e.g. charmbracelet/termenv) send OSC queries to detect
//! terminal capabilities like background color. Since don owns the PTY
//! master, it must respond to these queries or the program blocks until
//! its timeout (typically 5 seconds).
//!
//! This module provides a simple substring scanner that detects known
//! query patterns in raw byte chunks and returns the appropriate responses.

/// A terminal query pattern and its response.
struct Query {
    pattern: &'static [u8],
    response: &'static [u8],
}

/// Known terminal queries we respond to.
///
/// The responses assume a dark terminal with white foreground text.
/// The cursor position response places the cursor at row 1, col 1.
const QUERIES: &[Query] = &[
    // OSC 10 — foreground color query (ST-terminated)
    Query {
        pattern: b"\x1b]10;?\x1b\\",
        response: b"\x1b]10;rgb:ffff/ffff/ffff\x1b\\",
    },
    // OSC 10 — foreground color query (BEL-terminated)
    Query {
        pattern: b"\x1b]10;?\x07",
        response: b"\x1b]10;rgb:ffff/ffff/ffff\x1b\\",
    },
    // OSC 11 — background color query (ST-terminated)
    Query {
        pattern: b"\x1b]11;?\x1b\\",
        response: b"\x1b]11;rgb:0000/0000/0000\x1b\\",
    },
    // OSC 11 — background color query (BEL-terminated)
    Query {
        pattern: b"\x1b]11;?\x07",
        response: b"\x1b]11;rgb:0000/0000/0000\x1b\\",
    },
    // DSR — Device Status Report / Cursor Position Report
    Query {
        pattern: b"\x1b[6n",
        response: b"\x1b[1;1R",
    },
];

/// Scan a chunk for known terminal query patterns and return responses.
///
/// Returns a list of response byte slices to write back to the PTY.
/// Fast path: returns empty if no ESC byte is found in the chunk.
///
/// Queries that span chunk boundaries are missed — the program will
/// fall back to its timeout. This is acceptable since chunks are
/// typically 4-8KB and queries are only a few bytes.
pub(crate) fn find_responses(chunk: &[u8]) -> Vec<&'static [u8]> {
    // Fast path: no ESC in chunk, nothing to scan.
    if !chunk.contains(&0x1b) {
        return Vec::new();
    }

    let mut responses = Vec::new();
    for query in QUERIES {
        if chunk
            .windows(query.pattern.len())
            .any(|w| w == query.pattern)
        {
            responses.push(query.response);
        }
    }
    responses
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_find_responses() {
        struct Case {
            name: &'static str,
            chunk: &'static [u8],
            expected_count: usize,
        }

        let cases = vec![
            Case {
                name: "no ESC byte — fast path",
                chunk: b"hello world\n",
                expected_count: 0,
            },
            Case {
                name: "OSC 11 background query (ST)",
                chunk: b"\x1b]11;?\x1b\\",
                expected_count: 1,
            },
            Case {
                name: "OSC 11 background query (BEL)",
                chunk: b"\x1b]11;?\x07",
                expected_count: 1,
            },
            Case {
                name: "OSC 10 foreground query (ST)",
                chunk: b"\x1b]10;?\x1b\\",
                expected_count: 1,
            },
            Case {
                name: "DSR cursor position query",
                chunk: b"\x1b[6n",
                expected_count: 1,
            },
            Case {
                name: "OSC 11 + DSR in same chunk",
                chunk: b"\x1b]11;?\x1b\\\x1b[6n",
                expected_count: 2,
            },
            Case {
                name: "query embedded in normal output",
                chunk: b"some text\x1b]11;?\x1b\\more text\n",
                expected_count: 1,
            },
            Case {
                name: "ESC but not a known query",
                chunk: b"\x1b[31mred text\x1b[0m\n",
                expected_count: 0,
            },
            Case {
                name: "empty chunk",
                chunk: b"",
                expected_count: 0,
            },
        ];

        for case in cases {
            let responses = find_responses(case.chunk);
            assert_eq!(
                responses.len(),
                case.expected_count,
                "case: {}",
                case.name
            );
        }
    }

    #[test]
    fn test_osc11_response_content() {
        let responses = find_responses(b"\x1b]11;?\x1b\\");
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], b"\x1b]11;rgb:0000/0000/0000\x1b\\");
    }

    #[test]
    fn test_osc10_response_content() {
        let responses = find_responses(b"\x1b]10;?\x1b\\");
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], b"\x1b]10;rgb:ffff/ffff/ffff\x1b\\");
    }

    #[test]
    fn test_dsr_response_content() {
        let responses = find_responses(b"\x1b[6n");
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], b"\x1b[1;1R");
    }
}
