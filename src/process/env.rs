//! Environment variable loading and merging for services.
//!
//! Supports `.env` file parsing with `KEY=VALUE` format, comments, blank lines,
//! quoted values, and `export` prefix. Merge order (later wins):
//! 1. `.env.<service-name>` (auto-loaded if exists)
//! 2. `env_file` entries in declaration order
//! 3. `env` from config (inline)
//! 4. Don-injected variables (LISTEN_FDS, etc.)

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A non-fatal warning from env file parsing (e.g. malformed line).
#[derive(Debug, Clone)]
pub struct EnvWarning {
    pub path: PathBuf,
    pub line_number: usize,
    pub line: String,
}

impl std::fmt::Display for EnvWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}: malformed line (expected KEY=VALUE): {}",
            self.path.display(),
            self.line_number,
            self.line
        )
    }
}

/// Errors from environment variable loading.
#[derive(Debug, thiserror::Error)]
pub enum EnvError {
    /// An explicitly declared env_file could not be read.
    #[error("failed to read env file '{}': {source}", path.display())]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Parse a `.env` file into key-value pairs.
///
/// Supports:
/// - `KEY=VALUE` (basic)
/// - `KEY="VALUE"` and `KEY='VALUE'` (quoted — outer quotes stripped)
/// - `# comments` and blank lines (skipped)
/// - `export KEY=VALUE` (leading `export ` stripped)
/// - `KEY=` (empty value)
/// - `KEY=val=ue` (value containing `=`)
///
/// Malformed lines (no `=` sign) are collected as warnings but do not
/// cause a hard failure.
pub fn parse_env_file(path: &Path) -> Result<(HashMap<String, String>, Vec<EnvWarning>), EnvError> {
    let content = std::fs::read_to_string(path).map_err(|source| EnvError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;

    Ok(parse_env_content(&content, path))
}

/// Parse env file content (split out for testing without filesystem).
fn parse_env_content(content: &str, path: &Path) -> (HashMap<String, String>, Vec<EnvWarning>) {
    let mut vars = HashMap::new();
    let mut warnings = Vec::new();

    for (i, line) in content.lines().enumerate() {
        let line = line.trim();

        // Skip blank lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Strip optional `export ` prefix
        let line = line.strip_prefix("export ").unwrap_or(line);

        // Split on first `=`
        let Some((key, value)) = line.split_once('=') else {
            warnings.push(EnvWarning {
                path: path.to_path_buf(),
                line_number: i + 1,
                line: line.to_string(),
            });
            continue;
        };

        let key = key.trim().to_string();
        let value = strip_quotes(value.trim());

        vars.insert(key, value);
    }

    (vars, warnings)
}

/// Strip matching outer quotes (single or double) from a value.
fn strip_quotes(s: &str) -> String {
    for quote in ['"', '\''] {
        if let Some(inner) = s.strip_prefix(quote).and_then(|s| s.strip_suffix(quote)) {
            return inner.to_string();
        }
    }
    s.to_string()
}

/// Merge environment variables for a service in the correct precedence order.
///
/// Starts with the current process's environment as a base (so PATH, HOME,
/// TERM, etc. are inherited), then layers on service-specific overrides.
///
/// Merge order (later wins):
/// 0. Inherited environment from the current process (base)
/// 1. `.env.<service_name>` in `service_dir` (auto-loaded, skipped if missing)
/// 2. `env_file_paths` entries in declaration order (error if explicit env file is missing)
/// 3. `config_env` (inline env from config)
/// 4. `injected` (don-provided variables like LISTEN_FDS)
///
/// If `service_dir` is `None`, it defaults to the current directory for
/// the auto `.env.<name>` lookup.
///
/// Returns the merged map and any warnings from env file parsing.
pub fn merge_env(
    service_name: &str,
    service_dir: Option<&Path>,
    env_file_paths: &[PathBuf],
    config_env: &HashMap<String, String>,
    injected: &HashMap<String, String>,
) -> Result<(HashMap<String, String>, Vec<EnvWarning>), EnvError> {
    // 0. Start with inherited environment
    let mut merged: HashMap<String, String> = std::env::vars().collect();
    let mut all_warnings = Vec::new();

    // 1. Auto-load .env.<service_name> if it exists
    let dir = service_dir.unwrap_or_else(|| Path::new("."));
    let auto_path = dir.join(format!(".env.{service_name}"));
    if auto_path.is_file() {
        let (vars, warnings) = parse_env_file(&auto_path)?;
        merged.extend(vars);
        all_warnings.extend(warnings);
    }

    // 2. Explicit env_file entries (error if missing)
    for path in env_file_paths {
        let (vars, warnings) = parse_env_file(path)?;
        merged.extend(vars);
        all_warnings.extend(warnings);
    }

    // 3. Inline env from config
    merged.extend(config_env.clone());

    // 4. Don-injected variables
    merged.extend(injected.clone());

    Ok((merged, all_warnings))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::test_util::TempDir;
    use std::fs;

    #[test]
    fn test_parse_env_content() {
        struct Case {
            name: &'static str,
            content: &'static str,
            expect_vars: Vec<(&'static str, &'static str)>,
            expect_warnings: usize,
        }

        let cases = vec![
            Case {
                name: "basic key=value",
                content: "FOO=bar",
                expect_vars: vec![("FOO", "bar")],
                expect_warnings: 0,
            },
            Case {
                name: "double quoted value",
                content: "FOO=\"bar baz\"",
                expect_vars: vec![("FOO", "bar baz")],
                expect_warnings: 0,
            },
            Case {
                name: "single quoted value",
                content: "FOO='bar baz'",
                expect_vars: vec![("FOO", "bar baz")],
                expect_warnings: 0,
            },
            Case {
                name: "comment lines",
                content: "# this is a comment\nFOO=bar\n  # indented comment",
                expect_vars: vec![("FOO", "bar")],
                expect_warnings: 0,
            },
            Case {
                name: "blank lines",
                content: "\n\nFOO=bar\n\n",
                expect_vars: vec![("FOO", "bar")],
                expect_warnings: 0,
            },
            Case {
                name: "export prefix",
                content: "export FOO=bar",
                expect_vars: vec![("FOO", "bar")],
                expect_warnings: 0,
            },
            Case {
                name: "value containing equals",
                content: "FOO=bar=baz",
                expect_vars: vec![("FOO", "bar=baz")],
                expect_warnings: 0,
            },
            Case {
                name: "empty value",
                content: "FOO=",
                expect_vars: vec![("FOO", "")],
                expect_warnings: 0,
            },
            Case {
                name: "malformed line no equals",
                content: "GARBAGE",
                expect_vars: vec![],
                expect_warnings: 1,
            },
            Case {
                name: "mixed valid and malformed",
                content: "FOO=bar\nGARBAGE\nBAZ=qux",
                expect_vars: vec![("FOO", "bar"), ("BAZ", "qux")],
                expect_warnings: 1,
            },
            Case {
                name: "whitespace around key and value",
                content: "  FOO  =  bar  ",
                expect_vars: vec![("FOO", "bar")],
                expect_warnings: 0,
            },
            Case {
                name: "multiple vars",
                content: "A=1\nB=2\nC=3",
                expect_vars: vec![("A", "1"), ("B", "2"), ("C", "3")],
                expect_warnings: 0,
            },
            Case {
                name: "later value overwrites earlier",
                content: "FOO=first\nFOO=second",
                expect_vars: vec![("FOO", "second")],
                expect_warnings: 0,
            },
            Case {
                name: "export with quotes",
                content: "export FOO=\"hello world\"",
                expect_vars: vec![("FOO", "hello world")],
                expect_warnings: 0,
            },
            Case {
                name: "empty file",
                content: "",
                expect_vars: vec![],
                expect_warnings: 0,
            },
        ];

        let dummy_path = Path::new("test.env");
        for case in &cases {
            let (vars, warnings) = parse_env_content(case.content, dummy_path);
            for (key, expected_value) in &case.expect_vars {
                assert_eq!(
                    vars.get(*key).map(|s| s.as_str()),
                    Some(*expected_value),
                    "case '{}': key '{key}' mismatch",
                    case.name
                );
            }
            assert_eq!(
                vars.len(),
                case.expect_vars.len(),
                "case '{}': var count mismatch",
                case.name
            );
            assert_eq!(
                warnings.len(),
                case.expect_warnings,
                "case '{}': warning count mismatch",
                case.name
            );
        }
    }

    #[test]
    fn test_parse_env_file_missing() {
        let result = parse_env_file(Path::new("/tmp/don-test-nonexistent-env-file"));
        assert!(matches!(result, Err(EnvError::ReadFile { .. })));
    }

    #[test]
    fn test_merge_env_order() {
        let dir = TempDir::new("merge-order");

        // Auto .env.myservice — lowest priority
        fs::write(
            dir.path().join(".env.myservice"),
            "A=from-auto\nB=from-auto\nC=from-auto",
        )
        .unwrap();

        // Explicit env file — medium priority
        let env_file = dir.path().join("custom.env");
        fs::write(&env_file, "B=from-envfile\nC=from-envfile").unwrap();

        // Config env — high priority
        let mut config_env = HashMap::new();
        config_env.insert("C".to_string(), "from-config".to_string());

        // Injected — highest priority
        let mut injected = HashMap::new();
        injected.insert("D".to_string(), "from-injected".to_string());

        let (merged, warnings) = merge_env(
            "myservice",
            Some(dir.path()),
            &[env_file],
            &config_env,
            &injected,
        )
        .unwrap();

        assert!(warnings.is_empty());
        assert_eq!(merged["A"], "from-auto");
        assert_eq!(merged["B"], "from-envfile");
        assert_eq!(merged["C"], "from-config");
        assert_eq!(merged["D"], "from-injected");
    }

    #[test]
    fn test_merge_env_auto_file_missing_is_ok() {
        let dir = TempDir::new("merge-no-auto");
        // No .env.myservice file — should not error

        let config_env = HashMap::from([("FOO".to_string(), "bar".to_string())]);
        let (merged, _) = merge_env(
            "myservice",
            Some(dir.path()),
            &[],
            &config_env,
            &HashMap::new(),
        )
        .unwrap();

        assert_eq!(merged["FOO"], "bar");
    }

    #[test]
    fn test_merge_env_explicit_file_missing_is_error() {
        let dir = TempDir::new("merge-missing-explicit");
        let missing = dir.path().join("does-not-exist.env");

        let result = merge_env(
            "myservice",
            Some(dir.path()),
            &[missing],
            &HashMap::new(),
            &HashMap::new(),
        );

        assert!(matches!(result, Err(EnvError::ReadFile { .. })));
    }
}
