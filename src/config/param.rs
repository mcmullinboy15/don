//! Per-task parameter definitions.
//!
//! Tasks can declare `params` in `don.toml`. When the task runs, the user
//! supplies each param's value via `don run <task> --<name>=<val>` or the
//! interactive TUI form; values are then substituted into the task's
//! `cmd`/`args`/`env`/`dir` via `{{name}}` placeholders (see
//! [`super::template`]).
//!
//! Param values come from one of three places:
//! - Static [`choices`](TaskParam::choices) declared in config.
//! - A [`completions`](TaskParam::completions) command that shells out to
//!   produce candidates at form-open time.
//! - Free text (for `string`/`int`/`bool` without any candidate source).

use serde::Deserialize;

/// A single parameter declaration on a task.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TaskParam {
    /// Parameter identifier — referenced as `{{name}}` in the task command
    /// and passed on the CLI as `--<name>=<value>`.
    pub name: String,
    /// Optional human-readable prompt shown in the TUI form. Falls back to
    /// `name` when absent.
    pub prompt: Option<String>,
    /// When `true`, the task refuses to run unless the user supplies a
    /// value (explicitly or via [`default`](TaskParam::default)).
    #[serde(default)]
    pub required: bool,
    /// Optional default value used when the user doesn't supply one.
    /// For `kind = "bool"` this should be `"true"` or `"false"`.
    pub default: Option<String>,
    /// Value kind. Drives validation and the widget shown in the TUI form.
    #[serde(default)]
    pub kind: ParamKind,
    /// Fixed list of candidate values. Mutually exclusive with
    /// [`completions`](TaskParam::completions). Non-empty `choices` constrain
    /// the value set on submit.
    #[serde(default)]
    pub choices: Vec<String>,
    /// Shell-out completion command. Produces candidate values at form-open
    /// time (and on explicit refresh).
    pub completions: Option<Completions>,
    /// Optional numeric bounds for `kind = "int"`.
    pub validate: Option<ParamValidate>,
}

/// Value kind. Determines the TUI widget and built-in validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParamKind {
    /// Free-text string. Default.
    #[default]
    String,
    /// Integer value. TUI form renders with ↑/↓ stepper; value is validated
    /// against `validate.min`/`validate.max` when present.
    Int,
    /// Boolean toggle. Accepts `"true"`/`"false"` (case-insensitive) or
    /// bare `--flag` on the CLI (treated as `"true"`).
    Bool,
    /// Explicit choice kind. Set automatically when `choices` or
    /// `completions` is present; may also be set by the user for emphasis.
    Choice,
}

/// Shell-out completion command. Produces a list of candidate values for a
/// single param.
///
/// The command inherits the task's `env` plus `DON_PARAM_<NAME>=<value>`
/// for every already-entered param in the current form, which lets one
/// param's completions depend on earlier param values (e.g., pick a
/// database, then list its tables).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Completions {
    /// Program to invoke.
    pub cmd: String,
    /// Arguments to pass.
    #[serde(default)]
    pub args: Vec<String>,
    /// How to parse the command's stdout into candidate values.
    #[serde(default)]
    pub parse: CompletionParse,
    /// Optional cache TTL (e.g., `"5m"`, `"30s"`). When set, cached results
    /// are reused until the TTL expires. When absent, the command runs on
    /// every resolve (subject to dedupe of simultaneous requests).
    pub cache: Option<String>,
    /// Maximum time the command is allowed to run. Defaults to `"10s"`.
    pub timeout: Option<String>,
}

/// Strategy for parsing a completion command's stdout into candidate values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionParse {
    /// One value per line. Blank lines skipped. Default.
    #[default]
    Lines,
    /// NUL-separated values (e.g., `find -print0`). Use when values may
    /// contain newlines.
    NullSeparated,
    /// Top-level JSON array of strings, e.g. `["users", "orders"]`.
    Json,
}

/// Numeric bounds for `kind = "int"` params.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct ParamValidate {
    pub min: Option<i64>,
    pub max: Option<i64>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn minimal_param_deserializes() {
        let toml = r#"name = "index""#;
        let got: TaskParam = toml::from_str(toml).unwrap();
        assert_eq!(got.name, "index");
        assert!(!got.required);
        assert_eq!(got.kind, ParamKind::String);
        assert!(got.choices.is_empty());
        assert!(got.completions.is_none());
        assert!(got.default.is_none());
    }

    #[test]
    fn full_param_deserializes() {
        let toml = r#"
            name = "index"
            prompt = "Which index?"
            required = true
            default = "users"
            kind = "choice"
            choices = ["users", "orders"]
        "#;
        let got: TaskParam = toml::from_str(toml).unwrap();
        assert_eq!(got.name, "index");
        assert_eq!(got.prompt.as_deref(), Some("Which index?"));
        assert!(got.required);
        assert_eq!(got.default.as_deref(), Some("users"));
        assert_eq!(got.kind, ParamKind::Choice);
        assert_eq!(got.choices, vec!["users", "orders"]);
    }

    #[test]
    fn int_param_with_validate() {
        let toml = r#"
            name = "batch_size"
            kind = "int"
            default = "500"
            validate = { min = 1, max = 10000 }
        "#;
        let got: TaskParam = toml::from_str(toml).unwrap();
        assert_eq!(got.kind, ParamKind::Int);
        let v = got.validate.unwrap();
        assert_eq!(v.min, Some(1));
        assert_eq!(v.max, Some(10000));
    }

    #[test]
    fn completions_deserialize() {
        let toml = r#"
            name = "index"
            [completions]
            cmd = "curl"
            args = ["-s", "http://example"]
            parse = "lines"
            cache = "5m"
        "#;
        let got: TaskParam = toml::from_str(toml).unwrap();
        let comp = got.completions.unwrap();
        assert_eq!(comp.cmd, "curl");
        assert_eq!(comp.args, vec!["-s", "http://example"]);
        assert_eq!(comp.parse, CompletionParse::Lines);
        assert_eq!(comp.cache.as_deref(), Some("5m"));
        assert!(comp.timeout.is_none());
    }

    #[test]
    fn completions_default_parse_is_lines() {
        let toml = r#"
            name = "x"
            [completions]
            cmd = "ls"
        "#;
        let got: TaskParam = toml::from_str(toml).unwrap();
        let comp = got.completions.unwrap();
        assert_eq!(comp.parse, CompletionParse::Lines);
        assert!(comp.args.is_empty());
    }

    #[test]
    fn param_kind_aliases() {
        struct Case {
            kind_str: &'static str,
            want: ParamKind,
        }
        let cases = [
            Case {
                kind_str: "string",
                want: ParamKind::String,
            },
            Case {
                kind_str: "int",
                want: ParamKind::Int,
            },
            Case {
                kind_str: "bool",
                want: ParamKind::Bool,
            },
            Case {
                kind_str: "choice",
                want: ParamKind::Choice,
            },
        ];
        for c in cases {
            let toml = format!(r#"name = "x"{}kind = "{}""#, "\n", c.kind_str);
            let got: TaskParam = toml::from_str(&toml).unwrap();
            assert_eq!(got.kind, c.want, "{}", c.kind_str);
        }
    }
}
