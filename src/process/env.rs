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
/// Prepend a directory to the PATH entry in an env map. Creates PATH if it
/// doesn't exist. Does nothing if the directory is already the first entry.
pub fn prepend_to_path(env: &mut HashMap<String, String>, bin_dir: &Path) {
    let bin_str = bin_dir.to_string_lossy();
    let current = env.get("PATH").cloned().unwrap_or_default();
    // Skip prepend if already at the front.
    if current
        .split(':')
        .next()
        .is_some_and(|first| first == bin_str)
    {
        return;
    }
    let new_path = if current.is_empty() {
        bin_str.into_owned()
    } else {
        format!("{bin_str}:{current}")
    };
    env.insert("PATH".to_string(), new_path);
}

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

    // If don itself was launched via `bazel run` (e.g. a
    // `//tools/service-manager:local` target that wraps don), bazel
    // populated the env with RUNFILES_DIR / RUNFILES_MANIFEST_FILE /
    // JAVA_RUNFILES / exported `BASH_FUNC_runfiles_*` helpers describing
    // DON'S runfiles. Passing those through to a bazel-built service's
    // launcher is disastrous: the service's `runfiles.bash` init sees
    // RUNFILES_MANIFEST_FILE already set and uses don's manifest, so
    // `rlocation` returns empty strings for the service's files and the
    // launcher fails with `. "": No such file or directory`.
    //
    // Strip anything bazel-runfiles-related so the spawned service's own
    // launcher falls through to `$0.runfiles/` discovery correctly.
    //
    // The catches here:
    // - `runfiles` substring: `RUNFILES_DIR`, `RUNFILES_MANIFEST_FILE`,
    //   `JAVA_RUNFILES`, `BASH_FUNC_runfiles_*` (exported bash helpers).
    // - `rlocation` substring: `BASH_FUNC_rlocation%%` (exported function),
    //   `_RLOCATION_ISABS_PATTERN`, `_RLOCATION_GREP_CASE_INSENSITIVE_ARGS`
    //   (internals used by runfiles.bash).
    // - per-invocation bazel state: `BUILD_ID`, `BUILD_RANDOM`,
    //   `BUILD_EXECROOT`, `BUILD_WORKING_DIRECTORY`. These describe don's
    //   own bazel invocation and would mislead any child that consults
    //   them. `BUILD_WORKSPACE_DIRECTORY` is intentionally kept — it's
    //   the shared source-tree root and remains correct for children.
    // - `TEST_SRCDIR`: bazel's runfiles pointer in test contexts.
    merged.retain(|k, _| {
        let lk = k.to_ascii_lowercase();
        let bazel_state = matches!(
            lk.as_str(),
            "build_id"
                | "build_random"
                | "build_execroot"
                | "build_working_directory"
                | "test_srcdir"
        );
        !(lk.contains("runfiles") || lk.contains("rlocation") || bazel_state)
    });

    // Point PWD at the directory the child will actually run in.
    //
    // Don sets the child's cwd but, without this, leaves it inheriting Don's
    // own PWD — so anything reading `$PWD` (shell scripts especially) sees
    // the directory Don was launched from rather than its own. Shells
    // recompute PWD when it disagrees with `getcwd()`, which hides the bug;
    // a Python or Node child reading the variable directly does not.
    //
    // It also makes `${PWD}` usable in config, which is the only way to
    // write an absolute path relative to the project without hard-coding
    // one machine's layout.
    if let Some(dir) = service_dir {
        let absolute = if dir.is_absolute() {
            Some(dir.to_path_buf())
        } else {
            std::fs::canonicalize(dir).ok()
        };
        if let Some(absolute) = absolute {
            merged.insert("PWD".to_string(), absolute.to_string_lossy().into_owned());
        }
    }

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

    // 3. Inline env from config, with `${VAR}` expanded.
    //
    //    The base for expansion is everything *except* the config block
    //    itself — the inherited environment, any env files, and Don's own
    //    injected variables. Config values deliberately can't see each
    //    other: `HashMap` iteration order is arbitrary, so `A = "${B}"`
    //    alongside `B = "x"` would resolve differently run to run, and a
    //    config that works by luck is worse than one that doesn't work.
    let mut expansion_base = merged.clone();
    expansion_base.extend(injected.clone());
    for (key, value) in config_env {
        merged.insert(key.clone(), expand_env_vars(value, &expansion_base));
    }

    // 4. Don-injected variables
    merged.extend(injected.clone());

    Ok((merged, all_warnings))
}

/// Expand `${VAR}` references in a string using values from the env map.
/// Unknown variables are left as-is, so a value that merely *looks* like a
/// reference (a shell snippet, a template for some downstream tool) survives
/// untouched rather than being silently emptied.
pub fn expand_env_vars(input: &str, env: &HashMap<String, String>) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            let mut found_close = false;
            for c in chars.by_ref() {
                if c == '}' {
                    found_close = true;
                    break;
                }
                var_name.push(c);
            }
            if found_close {
                if let Some(val) = env.get(&var_name) {
                    result.push_str(val);
                } else {
                    // Leave unresolved vars as-is.
                    result.push_str("${");
                    result.push_str(&var_name);
                    result.push('}');
                }
            } else {
                // Unclosed ${, emit literally.
                result.push_str("${");
                result.push_str(&var_name);
            }
        } else {
            result.push(ch);
        }
    }
    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::process::test_util::TempDir;
    use std::fs;

    #[test]
    fn test_expand_env_vars() {
        struct Case {
            name: &'static str,
            input: &'static str,
            env: Vec<(&'static str, &'static str)>,
            expected: &'static str,
        }

        let cases = vec![
            Case {
                name: "no vars",
                input: "hello world",
                env: vec![],
                expected: "hello world",
            },
            Case {
                name: "single var",
                input: "--port ${PORT}",
                env: vec![("PORT", "8080")],
                expected: "--port 8080",
            },
            Case {
                name: "multiple vars",
                input: "${HOST}:${PORT}",
                env: vec![("HOST", "localhost"), ("PORT", "3000")],
                expected: "localhost:3000",
            },
            Case {
                name: "unknown var left as-is",
                input: "--port ${UNKNOWN}",
                env: vec![],
                expected: "--port ${UNKNOWN}",
            },
            Case {
                name: "var at start",
                input: "${PORT}",
                env: vec![("PORT", "9090")],
                expected: "9090",
            },
            Case {
                name: "bare dollar sign",
                input: "cost is $5",
                env: vec![],
                expected: "cost is $5",
            },
            Case {
                name: "unclosed brace",
                input: "broken ${VAR",
                env: vec![("VAR", "val")],
                expected: "broken ${VAR",
            },
        ];

        for case in cases {
            let env: HashMap<String, String> = case
                .env
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let result = expand_env_vars(case.input, &env);
            assert_eq!(result, case.expected, "{}", case.name);
        }
    }

    #[test]
    fn merge_env_expands_config_values() {
        let dir = TempDir::new("env-expand");
        let injected: HashMap<String, String> =
            [("PORT".to_string(), "45678".to_string())].into();

        struct Case {
            name: &'static str,
            value: &'static str,
            expect: &'static str,
        }

        // `HOME` stands in for "something from the inherited environment";
        // it's set in every environment don runs in.
        let home = std::env::var("HOME").unwrap();

        let cases = vec![
            Case {
                name: "an injected var, which is the point of the feature",
                value: "postgres://localhost:${PORT}/app",
                expect: "postgres://localhost:45678/app",
            },
            Case {
                name: "PWD resolves to the child's own directory",
                value: "${PWD}/.don/state",
                expect: "DIR/.don/state",
            },
            Case {
                name: "an unknown var is left alone rather than emptied",
                value: "${NOT_SET_ANYWHERE}/x",
                expect: "${NOT_SET_ANYWHERE}/x",
            },
            Case {
                name: "a plain value is untouched",
                value: "just-a-value",
                expect: "just-a-value",
            },
        ];

        for case in cases {
            let config: HashMap<String, String> =
                [("TARGET".to_string(), case.value.to_string())].into();
            let (merged, _) =
                merge_env("svc", Some(dir.path()), &[], &config, &injected).unwrap();
            let expect = case
                .expect
                .replace("DIR", &dir.path().to_string_lossy())
                .replace("HOME_UNUSED", &home);
            assert_eq!(merged.get("TARGET"), Some(&expect), "case: {}", case.name);
        }
    }

    #[test]
    fn merge_env_config_values_cannot_see_each_other() {
        // Deliberate: HashMap order is arbitrary, so resolving config
        // against config would give different answers on different runs.
        // Leaving the reference intact is the honest, reproducible outcome.
        let dir = TempDir::new("env-expand-order");
        let config: HashMap<String, String> = [
            ("BASE".to_string(), "/srv".to_string()),
            ("FULL".to_string(), "${BASE}/app".to_string()),
        ]
        .into();

        let (merged, _) =
            merge_env("svc", Some(dir.path()), &[], &config, &HashMap::new()).unwrap();
        assert_eq!(merged.get("BASE"), Some(&"/srv".to_string()));
        assert_eq!(merged.get("FULL"), Some(&"${BASE}/app".to_string()));
    }

    #[test]
    fn merge_env_points_pwd_at_the_child_directory() {
        let dir = TempDir::new("env-pwd");
        let (merged, _) =
            merge_env("svc", Some(dir.path()), &[], &HashMap::new(), &HashMap::new()).unwrap();

        assert_eq!(
            merged.get("PWD").map(String::as_str),
            Some(dir.path().to_string_lossy().as_ref()),
            "PWD should name the directory the child will run in, not don's"
        );
    }

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
    fn test_prepend_to_path() {
        struct Case {
            name: &'static str,
            initial: Option<&'static str>,
            bin_dir: &'static str,
            expected: &'static str,
        }
        let cases = vec![
            Case {
                name: "prepend to existing PATH",
                initial: Some("/usr/bin:/bin"),
                bin_dir: "/don/bin",
                expected: "/don/bin:/usr/bin:/bin",
            },
            Case {
                name: "create PATH when missing",
                initial: None,
                bin_dir: "/don/bin",
                expected: "/don/bin",
            },
            Case {
                name: "idempotent when already at front",
                initial: Some("/don/bin:/usr/bin"),
                bin_dir: "/don/bin",
                expected: "/don/bin:/usr/bin",
            },
            Case {
                name: "prepend even if present later",
                initial: Some("/usr/bin:/don/bin"),
                bin_dir: "/don/bin",
                expected: "/don/bin:/usr/bin:/don/bin",
            },
        ];
        for case in cases {
            let mut env: HashMap<String, String> = HashMap::new();
            if let Some(v) = case.initial {
                env.insert("PATH".to_string(), v.to_string());
            }
            prepend_to_path(&mut env, Path::new(case.bin_dir));
            assert_eq!(
                env.get("PATH").map(String::as_str),
                Some(case.expected),
                "case: {}",
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
