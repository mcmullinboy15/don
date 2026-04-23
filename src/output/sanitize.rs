//! Terminal output sanitization.
//!
//! Strips ANSI escape sequences that could disrupt don's shared terminal
//! (cursor movement, screen clearing, alternate screen, etc.) while
//! preserving SGR sequences (colors, bold, underline) which are safe.
//!
//! Applied in the stdout sink before writing — the ring buffer retains
//! raw output so `don logs` and `don attach` see everything.

const ESC: u8 = 0x1B;
const BEL: u8 = 0x07;

/// Strip dangerous ANSI escape sequences from a line of output, keeping
/// only SGR (Select Graphic Rendition) sequences — colors and text styles.
/// Standalone BEL (`\x07`) bytes are stripped too; some tools (npm, tsc,
/// readline) ring the bell on warnings, and with many services multiplexed
/// the noise is useless — you can't tell who beeped. The raw pre-sanitize
/// bytes still live in the ring buffer, so `don logs` and `don attach`
/// preserve BEL.
///
/// Returns the sanitized bytes. If nothing was stripped, returns the input
/// unchanged (no allocation).
pub(crate) fn sanitize_terminal_output(input: &[u8]) -> Vec<u8> {
    // Fast path: no ESC and no BEL → nothing to strip.
    if !input.contains(&ESC) && !input.contains(&BEL) {
        return input.to_vec();
    }

    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;

    while i < input.len() {
        if input[i] == BEL {
            // Standalone BEL — drop it. (BEL inside OSC is consumed by
            // skip_osc as the terminator.)
            i += 1;
            continue;
        }
        if input[i] != ESC {
            out.push(input[i]);
            i += 1;
            continue;
        }

        // ESC found. Look at the next byte to determine the sequence type.
        if i + 1 >= input.len() {
            // Lone ESC at end of input — strip it.
            break;
        }

        match input[i + 1] {
            // CSI: ESC [ <params> <intermediates> <final>
            b'[' => {
                i = handle_csi(input, i, &mut out);
            }
            // OSC: ESC ] ... ST (or BEL)
            b']' => {
                i = skip_osc(input, i);
            }
            // DCS: ESC P ... ST
            b'P' => {
                i = skip_until_st(input, i);
            }
            // APC: ESC _ ... ST
            b'_' => {
                i = skip_until_st(input, i);
            }
            // PM: ESC ^ ... ST
            b'^' => {
                i = skip_until_st(input, i);
            }
            // Single-character ESC sequences (ESC + one byte):
            // ESC c (reset), ESC 7/8 (save/restore cursor), ESC D/E/M, etc.
            // Covers Fp (0x30-0x3F), Fe (0x40-0x5F), and Fs (0x60-0x7E).
            // All stripped — none are safe for shared output.
            0x30..=0x7E => {
                i += 2;
            }
            // ESC followed by space + one byte (e.g., ESC SP F/G) — 7/8-bit control
            b' ' => {
                i += if i + 2 < input.len() { 3 } else { 2 };
            }
            // Anything else after ESC — strip the ESC, keep the next byte
            // (probably a malformed sequence).
            _ => {
                i += 1; // skip ESC only
            }
        }
    }

    out
}

/// Handle a CSI sequence starting at `input[start]` (which is ESC).
/// If the final byte is `m` (SGR), copies the entire sequence to `out`.
/// Otherwise, strips the sequence. Returns the index after the sequence.
fn handle_csi(input: &[u8], start: usize, out: &mut Vec<u8>) -> usize {
    // start points at ESC, start+1 is '['.
    let mut i = start + 2;

    // Parameter bytes: 0x30-0x3F
    while i < input.len() && (0x30..=0x3F).contains(&input[i]) {
        i += 1;
    }
    // Intermediate bytes: 0x20-0x2F
    while i < input.len() && (0x20..=0x2F).contains(&input[i]) {
        i += 1;
    }
    // Final byte: 0x40-0x7E
    if i < input.len() && (0x40..=0x7E).contains(&input[i]) {
        let final_byte = input[i];
        i += 1; // consume final byte

        if final_byte == b'm' {
            // SGR — safe, keep it.
            out.extend_from_slice(&input[start..i]);
        }
        // else: stripped (cursor movement, screen clear, etc.)
    }
    // If we ran out of input without a final byte, the sequence is
    // malformed/incomplete — just skip what we consumed.

    i
}

/// Skip an OSC sequence: ESC ] ... (terminated by BEL or ST).
/// ST is ESC \.
fn skip_osc(input: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    while i < input.len() {
        if input[i] == 0x07 {
            // BEL terminates OSC.
            return i + 1;
        }
        if input[i] == ESC && i + 1 < input.len() && input[i + 1] == b'\\' {
            // ST (ESC \) terminates OSC.
            return i + 2;
        }
        i += 1;
    }
    i
}

/// Skip a sequence terminated by ST (ESC \): DCS, APC, PM.
fn skip_until_st(input: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    while i < input.len() {
        if input[i] == ESC && i + 1 < input.len() && input[i + 1] == b'\\' {
            return i + 2;
        }
        i += 1;
    }
    i
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    struct Case {
        name: &'static str,
        input: &'static [u8],
        expected: &'static [u8],
    }

    #[test]
    fn sanitize_table() {
        let cases = vec![
            Case {
                name: "plain text unchanged",
                input: b"hello world",
                expected: b"hello world",
            },
            Case {
                name: "SGR color preserved",
                input: b"\x1b[31mred text\x1b[0m",
                expected: b"\x1b[31mred text\x1b[0m",
            },
            Case {
                name: "SGR bold preserved",
                input: b"\x1b[1mbold\x1b[22m",
                expected: b"\x1b[1mbold\x1b[22m",
            },
            Case {
                name: "256-color SGR preserved",
                input: b"\x1b[38;5;196mred\x1b[0m",
                expected: b"\x1b[38;5;196mred\x1b[0m",
            },
            Case {
                name: "cursor up stripped",
                input: b"before\x1b[Aafter",
                expected: b"beforeafter",
            },
            Case {
                name: "cursor position stripped",
                input: b"before\x1b[10;20Hafter",
                expected: b"beforeafter",
            },
            Case {
                name: "clear screen stripped",
                input: b"\x1b[2Jhello",
                expected: b"hello",
            },
            Case {
                name: "alternate screen enter stripped",
                input: b"\x1b[?1049hhello",
                expected: b"hello",
            },
            Case {
                name: "alternate screen leave stripped",
                input: b"\x1b[?1049lhello",
                expected: b"hello",
            },
            Case {
                name: "cursor hide stripped",
                input: b"\x1b[?25lhello",
                expected: b"hello",
            },
            Case {
                name: "scroll up stripped",
                input: b"\x1b[3Shello",
                expected: b"hello",
            },
            Case {
                name: "erase in line stripped",
                input: b"partial\x1b[Krest",
                expected: b"partialrest",
            },
            Case {
                name: "OSC title set stripped",
                input: b"\x1b]0;my title\x07hello",
                expected: b"hello",
            },
            Case {
                name: "OSC with ST stripped",
                input: b"\x1b]0;my title\x1b\\hello",
                expected: b"hello",
            },
            Case {
                name: "ESC c (reset) stripped",
                input: b"\x1bcafter reset",
                expected: b"after reset",
            },
            Case {
                name: "ESC 7 (save cursor) stripped",
                input: b"\x1b7text\x1b8",
                expected: b"text",
            },
            Case {
                name: "mixed SGR and cursor — SGR kept, cursor stripped",
                input: b"\x1b[1m\x1b[Hbold at home\x1b[0m",
                expected: b"\x1b[1mbold at home\x1b[0m",
            },
            Case {
                name: "DCS sequence stripped",
                input: b"\x1bPdata\x1b\\after",
                expected: b"after",
            },
            Case {
                name: "empty input",
                input: b"",
                expected: b"",
            },
            Case {
                name: "lone ESC at end",
                input: b"text\x1b",
                expected: b"text",
            },
            Case {
                name: "multiple SGR in sequence",
                input: b"\x1b[1;31;42mcolorful\x1b[0m",
                expected: b"\x1b[1;31;42mcolorful\x1b[0m",
            },
            Case {
                name: "standalone BEL stripped",
                input: b"warning\x07!",
                expected: b"warning!",
            },
            Case {
                name: "multiple standalone BELs stripped",
                input: b"\x07\x07a\x07b\x07",
                expected: b"ab",
            },
            Case {
                name: "BEL-only input becomes empty",
                input: b"\x07",
                expected: b"",
            },
            Case {
                name: "BEL inside OSC still terminates correctly",
                input: b"\x1b]0;title\x07after",
                expected: b"after",
            },
        ];

        for case in cases {
            let result = sanitize_terminal_output(case.input);
            assert_eq!(
                result, case.expected,
                "case '{}': input={:?}, expected={:?}, got={:?}",
                case.name,
                String::from_utf8_lossy(case.input),
                String::from_utf8_lossy(case.expected),
                String::from_utf8_lossy(&result),
            );
        }
    }
}
