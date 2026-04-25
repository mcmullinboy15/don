//! Human-readable duration string parsing.
//!
//! Supports formats like `"200ms"`, `"1s"`, `"1.5s"`, `"5m"`, `"1h"`.
//! Used by config validation for ready check intervals, shutdown timeouts,
//! debounce windows, and task timeouts.

use std::time::Duration;

/// Parse a human-readable duration string into a [`std::time::Duration`].
///
/// # Supported formats
///
/// - `"200ms"` — milliseconds
/// - `"1s"` or `"1.5s"` — seconds
/// - `"5m"` — minutes
/// - `"1h"` — hours
///
/// Whitespace is trimmed from both ends.
///
/// # Errors
///
/// Returns [`DurationError`] for empty strings, negative values,
/// missing units, or unrecognized formats.
pub fn parse_duration(s: &str) -> Result<Duration, DurationError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(DurationError::Empty);
    }

    // Find where the numeric part ends and the unit suffix begins
    let unit_start = s
        .find(|c: char| c.is_ascii_alphabetic())
        .ok_or_else(|| DurationError::MissingUnit(s.to_string()))?;

    let (num_str, unit) = s.split_at(unit_start);
    // Empty num_str will itself produce a ParseFloatError — no special case needed.
    let value: f64 = num_str.parse().map_err(|e| DurationError::InvalidNumber {
        input: s.to_string(),
        source: e,
    })?;

    if value < 0.0 {
        return Err(DurationError::Negative(s.to_string()));
    }

    let millis = match unit {
        "ms" => value,
        "s" => value * 1_000.0,
        "m" => value * 60_000.0,
        "h" => value * 3_600_000.0,
        _ => {
            return Err(DurationError::UnknownUnit {
                input: s.to_string(),
                unit: unit.to_string(),
            });
        }
    };

    Ok(Duration::from_micros((millis * 1_000.0) as u64))
}

/// Errors from parsing a duration string.
#[derive(Debug, thiserror::Error)]
pub enum DurationError {
    /// The input string was empty or whitespace-only.
    #[error("empty duration string")]
    Empty,
    /// No unit suffix was found (e.g. `"5"` instead of `"5s"`).
    #[error("invalid duration '{0}': missing unit (expected ms, s, m, or h)")]
    MissingUnit(String),
    /// The unit suffix was not recognized.
    #[error("invalid duration '{input}': unrecognized unit '{unit}'")]
    UnknownUnit { input: String, unit: String },
    /// The numeric part could not be parsed.
    #[error("invalid duration '{input}': {source}")]
    InvalidNumber {
        input: String,
        source: std::num::ParseFloatError,
    },
    /// The value was negative.
    #[error("invalid duration '{0}': value must be non-negative")]
    Negative(String),
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        struct Case {
            input: &'static str,
            expect_ok: bool,
            expect_duration: Option<Duration>,
        }

        let cases = vec![
            Case {
                input: "200ms",
                expect_ok: true,
                expect_duration: Some(Duration::from_millis(200)),
            },
            Case {
                input: "1s",
                expect_ok: true,
                expect_duration: Some(Duration::from_secs(1)),
            },
            Case {
                input: "1.5s",
                expect_ok: true,
                expect_duration: Some(Duration::from_millis(1500)),
            },
            Case {
                input: "5m",
                expect_ok: true,
                expect_duration: Some(Duration::from_secs(300)),
            },
            Case {
                input: "1h",
                expect_ok: true,
                expect_duration: Some(Duration::from_secs(3600)),
            },
            Case {
                input: "0s",
                expect_ok: true,
                expect_duration: Some(Duration::ZERO),
            },
            Case {
                input: "0ms",
                expect_ok: true,
                expect_duration: Some(Duration::ZERO),
            },
            Case {
                input: "  200ms  ",
                expect_ok: true,
                expect_duration: Some(Duration::from_millis(200)),
            },
            Case {
                input: "500ms",
                expect_ok: true,
                expect_duration: Some(Duration::from_millis(500)),
            },
            Case {
                input: "0.5s",
                expect_ok: true,
                expect_duration: Some(Duration::from_millis(500)),
            },
            // Error cases
            Case {
                input: "",
                expect_ok: false,
                expect_duration: None,
            },
            Case {
                input: "   ",
                expect_ok: false,
                expect_duration: None,
            },
            Case {
                input: "banana",
                expect_ok: false,
                expect_duration: None,
            },
            Case {
                input: "5",
                expect_ok: false,
                expect_duration: None,
            },
            Case {
                input: "-1s",
                expect_ok: false,
                expect_duration: None,
            },
            Case {
                input: "5x",
                expect_ok: false,
                expect_duration: None,
            },
            Case {
                input: "5sec",
                expect_ok: false,
                expect_duration: None,
            },
            Case {
                input: "ms",
                expect_ok: false,
                expect_duration: None,
            },
        ];

        for case in &cases {
            let result = parse_duration(case.input);
            if case.expect_ok {
                let duration = result
                    .unwrap_or_else(|e| panic!("'{}': expected Ok, got Err({e})", case.input));
                assert_eq!(
                    duration,
                    case.expect_duration.unwrap(),
                    "'{}': duration mismatch",
                    case.input
                );
            } else {
                assert!(
                    result.is_err(),
                    "'{}': expected Err, got Ok({:?})",
                    case.input,
                    result.unwrap()
                );
            }
        }
    }
}
