//! Runtime service-port reference expansion for inline environment values.
//!
//! `$(service.key)` references are resolved immediately before an item starts,
//! after its dependencies have published their effective runtime ports.
//! `$$(...)` escapes the syntax and produces a literal `$(...)`.
//!
//! Rendering is deliberately lenient: a `$(...)` token is only treated as a
//! Don runtime reference when the name before the first `.` is a known service.
//! Anything else — shell-style command substitution like `$(git rev-parse
//! HEAD)`, an unterminated `$(`, or an empty `$()` — is passed through
//! untouched so existing configs keep working. A token that *does* name a
//! known service but fails to resolve is a real misconfiguration and errors.

use std::collections::{HashMap, HashSet};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(in crate::runner) enum EnvRefError {
    #[error(
        "unknown runtime port reference '$({reference})' — ensure the service is in depends_on and exposes a proxy or Docker port"
    )]
    Unknown { reference: String },
}

pub(in crate::runner) fn render(
    value: &str,
    refs: &HashMap<String, String>,
    known_services: &HashSet<String>,
) -> Result<String, EnvRefError> {
    let mut out = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;

    while index < bytes.len() {
        if bytes.get(index) == Some(&b'$')
            && bytes.get(index + 1) == Some(&b'$')
            && bytes.get(index + 2) == Some(&b'(')
        {
            out.push_str("$(");
            index += 3;
            continue;
        }

        if bytes.get(index) == Some(&b'$') && bytes.get(index + 1) == Some(&b'(') {
            if let Some((reference, next)) = parse_reference(value, index) {
                if let Some(resolved) = refs.get(reference) {
                    out.push_str(resolved);
                    index = next;
                    continue;
                }
                // Only a token that names a known service is a real reference
                // that must resolve; treat everything else as literal text.
                let service = reference.split('.').next().unwrap_or("");
                if !service.is_empty() && known_services.contains(service) {
                    return Err(EnvRefError::Unknown {
                        reference: reference.to_string(),
                    });
                }
            }
            // Not a Don reference — emit the `$` literally and re-scan the
            // rest as ordinary text (the `(` carries no special meaning
            // without a preceding `$`).
            out.push('$');
            index += 1;
            continue;
        }

        let Some(ch) = value.get(index..).and_then(|rest| rest.chars().next()) else {
            break;
        };
        out.push(ch);
        index += ch.len_utf8();
    }

    Ok(out)
}

/// Parse a `$(...)` token beginning at `start`. Returns the trimmed inner
/// reference and the byte index just past the closing `)`, or `None` when the
/// token is unterminated (no closing `)`), in which case the caller treats it
/// as literal text.
fn parse_reference(value: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = value.as_bytes();
    let name_start = start + 2;
    let mut end = name_start;
    while end < bytes.len() && bytes[end] != b')' {
        end += 1;
    }
    if end >= bytes.len() {
        return None;
    }
    let reference = value.get(name_start..end)?.trim();
    Some((reference, end + 1))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn render_runtime_references_table() {
        struct Case {
            name: &'static str,
            input: &'static str,
            expected: Result<&'static str, EnvRefError>,
        }

        let refs = HashMap::from([
            ("db.PORT".to_string(), "54321".to_string()),
            ("db.addr".to_string(), "127.0.0.1:54321".to_string()),
        ]);
        // `db` is a known service that exposes ports; `plain` is a known
        // service with no ports; everything else is unknown.
        let known = HashSet::from(["db".to_string(), "plain".to_string()]);
        let cases = vec![
            Case {
                name: "port in URL",
                input: "postgres://localhost:$(db.PORT)/app",
                expected: Ok("postgres://localhost:54321/app"),
            },
            Case {
                name: "address",
                input: "$(db.addr)",
                expected: Ok("127.0.0.1:54321"),
            },
            Case {
                name: "escaped",
                input: "$$(db.PORT)",
                expected: Ok("$(db.PORT)"),
            },
            Case {
                name: "unicode",
                input: "pré-$(db.PORT)-post",
                expected: Ok("pré-54321-post"),
            },
            Case {
                name: "known service unresolved key errors",
                input: "$(db.MISSING)",
                expected: Err(EnvRefError::Unknown {
                    reference: "db.MISSING".to_string(),
                }),
            },
            Case {
                name: "known service without ports errors",
                input: "$(plain.port)",
                expected: Err(EnvRefError::Unknown {
                    reference: "plain.port".to_string(),
                }),
            },
            Case {
                name: "unknown service passes through literally",
                input: "$(missing.PORT)",
                expected: Ok("$(missing.PORT)"),
            },
            Case {
                name: "shell command substitution is literal",
                input: "rev is $(git rev-parse HEAD)",
                expected: Ok("rev is $(git rev-parse HEAD)"),
            },
            Case {
                name: "unterminated is literal",
                input: "x $(db.PORT",
                expected: Ok("x $(db.PORT"),
            },
            Case {
                name: "empty is literal",
                input: "$()",
                expected: Ok("$()"),
            },
        ];

        for case in cases {
            let actual = render(case.input, &refs, &known);
            match case.expected {
                Ok(expected) => assert_eq!(actual.unwrap(), expected, "{}", case.name),
                Err(expected) => assert_eq!(actual.unwrap_err(), expected, "{}", case.name),
            }
        }
    }
}
