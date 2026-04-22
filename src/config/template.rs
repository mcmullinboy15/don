//! `{{name}}` placeholder substitution for task command strings.
//!
//! Used by the task runner to substitute user-supplied param values into a
//! task's `cmd`, `args`, `env`, and `dir` before spawning. Intentionally
//! minimal — no expressions, conditionals, or loops; just named substitution.
//!
//! Syntax:
//! - `{{name}}` — replaced by `values[name]`. Unknown names error out.
//! - `\{{` — escape producing a literal `{{` in the output. A bare `\` is
//!   otherwise preserved.

use std::collections::HashMap;

/// Errors from [`render`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TemplateError {
    /// Placeholder references a name not in the supplied value map.
    #[error("unknown placeholder '{name}'")]
    UnknownName { name: String },
    /// `{{` opened but never closed before end-of-string.
    #[error("unterminated placeholder starting at byte {start}")]
    Unterminated { start: usize },
    /// An empty `{{}}` placeholder, which is always a typo.
    #[error("empty placeholder at byte {start}")]
    Empty { start: usize },
}

/// Substitute `{{name}}` placeholders in `s` with values from `values`.
///
/// Returns the rendered string, or a [`TemplateError`] if a placeholder
/// references an unknown name or is malformed.
pub fn render(s: &str, values: &HashMap<String, String>) -> Result<String, TemplateError> {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        // Escape `\{{` → literal `{{`. Any other `\` is passed through.
        if bytes[i] == b'\\' && i + 2 < bytes.len() && bytes[i + 1] == b'{' && bytes[i + 2] == b'{'
        {
            out.push_str("{{");
            i += 3;
            continue;
        }

        if bytes[i] == b'{' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            let start = i;
            let name_start = i + 2;
            // Scan for the closing `}}`.
            let mut j = name_start;
            let mut found_close = false;
            while j + 1 < bytes.len() {
                if bytes[j] == b'}' && bytes[j + 1] == b'}' {
                    found_close = true;
                    break;
                }
                j += 1;
            }
            if !found_close {
                return Err(TemplateError::Unterminated { start });
            }
            // Placeholder runs `name_start..j`, closing braces at `j..j+2`.
            let raw_name = &s[name_start..j];
            let name = raw_name.trim();
            if name.is_empty() {
                return Err(TemplateError::Empty { start });
            }
            let value = values
                .get(name)
                .ok_or_else(|| TemplateError::UnknownName {
                    name: name.to_string(),
                })?;
            out.push_str(value);
            i = j + 2;
            continue;
        }

        // Regular byte. Safe to push because we're walking the same UTF-8
        // string — continuation bytes are never confused with `{` or `\`.
        let ch_start = i;
        let mut ch_end = ch_start + 1;
        while ch_end < bytes.len() && (bytes[ch_end] & 0b1100_0000) == 0b1000_0000 {
            ch_end += 1;
        }
        out.push_str(&s[ch_start..ch_end]);
        i = ch_end;
    }

    Ok(out)
}

/// Collect every `{{name}}` reference found in `s`, with whitespace trimmed.
///
/// Used by config validation to confirm that placeholders in a task's
/// command only reference declared params. Skips escaped `\{{` and ignores
/// unterminated or empty placeholders (those are caught by [`render`] at
/// runtime, which is a better place to error since validation runs over
/// every string the task might render).
pub(crate) fn collect_references(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 2 < bytes.len() && bytes[i + 1] == b'{' && bytes[i + 2] == b'{'
        {
            i += 3;
            continue;
        }
        if bytes[i] == b'{' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            let name_start = i + 2;
            let mut j = name_start;
            while j + 1 < bytes.len() {
                if bytes[j] == b'}' && bytes[j + 1] == b'}' {
                    break;
                }
                j += 1;
            }
            // Malformed placeholders get caught by render() — skip here.
            if j + 1 < bytes.len() {
                let name = s[name_start..j].trim();
                if !name.is_empty() {
                    out.push(name.to_string());
                }
                i = j + 2;
                continue;
            }
        }
        i += 1;
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn values(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn collect_references_table() {
        struct Case {
            name: &'static str,
            input: &'static str,
            want: Vec<&'static str>,
        }
        let cases = [
            Case { name: "none", input: "plain", want: vec![] },
            Case { name: "single", input: "a {{x}} b", want: vec!["x"] },
            Case {
                name: "multiple distinct",
                input: "{{a}} and {{b}}",
                want: vec!["a", "b"],
            },
            Case {
                name: "duplicates preserved",
                input: "{{x}} {{x}}",
                want: vec!["x", "x"],
            },
            Case {
                name: "trims whitespace",
                input: "{{  padded }}",
                want: vec!["padded"],
            },
            Case {
                name: "escape ignored",
                input: r"\{{ literal {{real}}",
                want: vec!["real"],
            },
            Case {
                name: "unterminated ignored",
                input: "foo {{x",
                want: vec![],
            },
            Case {
                name: "empty placeholder ignored",
                input: "{{}}",
                want: vec![],
            },
        ];
        for case in cases {
            let got = collect_references(case.input);
            let want: Vec<String> = case.want.iter().map(|s| s.to_string()).collect();
            assert_eq!(got, want, "{}", case.name);
        }
    }

    #[test]
    fn render_table() {
        struct Case {
            name: &'static str,
            input: &'static str,
            values: &'static [(&'static str, &'static str)],
            want: Result<&'static str, TemplateError>,
        }

        let cases = [
            Case {
                name: "plain text passes through",
                input: "hello world",
                values: &[],
                want: Ok("hello world"),
            },
            Case {
                name: "single substitution",
                input: "hello {{name}}",
                values: &[("name", "world")],
                want: Ok("hello world"),
            },
            Case {
                name: "multiple substitutions",
                input: "{{a}} and {{b}}",
                values: &[("a", "x"), ("b", "y")],
                want: Ok("x and y"),
            },
            Case {
                name: "adjacent placeholders",
                input: "{{a}}{{b}}",
                values: &[("a", "x"), ("b", "y")],
                want: Ok("xy"),
            },
            Case {
                name: "whitespace inside braces is trimmed",
                input: "{{ name }}",
                values: &[("name", "world")],
                want: Ok("world"),
            },
            Case {
                name: "empty value substitutes empty string",
                input: "[{{x}}]",
                values: &[("x", "")],
                want: Ok("[]"),
            },
            Case {
                name: "escape produces literal double-brace",
                input: r"\{{ not a placeholder",
                values: &[],
                want: Ok("{{ not a placeholder"),
            },
            Case {
                name: "escape before a real placeholder",
                input: r"\{{ {{name}}",
                values: &[("name", "x")],
                want: Ok("{{ x"),
            },
            Case {
                name: "lone backslash passes through",
                input: r"a\b",
                values: &[],
                want: Ok(r"a\b"),
            },
            Case {
                name: "unknown name errors",
                input: "{{missing}}",
                values: &[],
                want: Err(TemplateError::UnknownName {
                    name: "missing".into(),
                }),
            },
            Case {
                name: "empty placeholder errors",
                input: "{{}}",
                values: &[],
                want: Err(TemplateError::Empty { start: 0 }),
            },
            Case {
                name: "whitespace-only placeholder errors",
                input: "{{   }}",
                values: &[],
                want: Err(TemplateError::Empty { start: 0 }),
            },
            Case {
                name: "unterminated placeholder errors",
                input: "foo {{name",
                values: &[],
                want: Err(TemplateError::Unterminated { start: 4 }),
            },
            Case {
                name: "single brace is literal",
                input: "a { b } c",
                values: &[],
                want: Ok("a { b } c"),
            },
            Case {
                name: "utf8 surrounding placeholder",
                input: "héllo {{name}} wörld",
                values: &[("name", "✨")],
                want: Ok("héllo ✨ wörld"),
            },
        ];

        for case in cases {
            let got = render(case.input, &values(case.values));
            match (&got, &case.want) {
                (Ok(g), Ok(w)) => assert_eq!(g, w, "{}", case.name),
                (Err(g), Err(w)) => assert_eq!(g, w, "{}", case.name),
                other => panic!("{}: got {:?}, want {:?}", case.name, other.0, other.1),
            }
        }
    }
}
