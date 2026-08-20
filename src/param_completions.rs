//! Shell-out completion resolution for task params.
//!
//! A task's param can declare a [`Completions`](crate::config::Completions)
//! block — a command that produces candidate values on stdout. The runner
//! invokes it on behalf of the TUI form (and the CLI tab completer) via
//! `RunnerCommand::ResolveCompletions`; the engine itself — cache, shell-out,
//! parse, failure log — has no runner dependency and lives here.
//!
//! Design:
//!
//! - The command inherits the task's `env` + don's own env + `.don/bin`
//!   prepended to `PATH`, plus `DON_PARAM_<NAME>=<value>` for every
//!   param the user has already entered in the current form. This lets
//!   one param's candidates depend on another (pick a DB, then list its
//!   tables).
//! - Results are cached by `(task, param, partial-hash)` with an optional
//!   TTL from config.
//! - Failures are written to `.don/logs/completions/<task>-<param>-<ts>.log`
//!   with the full invocation + stdout + stderr + exit code, so the user
//!   can diagnose without losing context.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::RwLock;

use crate::config::{CompletionParse, Completions};
use crate::duration::parse_duration;

/// Error returned from [`RunnerCommand::ResolveCompletions`].
///
/// The TUI displays `message` inline and, when `log_path` is set, offers
/// the user a way to pull up the full command invocation + stdout/stderr
/// that was saved at that path.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompletionError {
    /// Human-readable summary suitable for a status bar / inline banner.
    pub message: String,
    /// Filesystem path to the saved log file, when one was written.
    /// Absent when the failure happened before the command was invoked
    /// (e.g., unknown task or param).
    pub log_path: Option<std::path::PathBuf>,
}

impl std::fmt::Display for CompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.log_path {
            Some(p) => write!(f, "{} (see {})", self.message, p.display()),
            None => write!(f, "{}", self.message),
        }
    }
}

/// Cache of completion results keyed by `(task, param, partial-hash)`.
///
/// The partial-hash folds in already-entered param values so that a
/// completion command that depends on `DON_PARAM_DB` doesn't return stale
/// results when the user changes the DB selection.
///
/// Shared via `Arc<RwLock<...>>` so independent background tasks can hit
/// the cache without contending on the runner's main command loop.
#[derive(Debug, Default)]
pub(crate) struct CompletionCache {
    entries: HashMap<CacheKey, CacheEntry>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct CacheKey {
    task: String,
    param: String,
    partial_hash: u64,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    values: Vec<String>,
    inserted: Instant,
    /// None = forever (within process lifetime). Some(d) = expires after
    /// `inserted + d`.
    ttl: Option<Duration>,
}

impl CompletionCache {
    pub(crate) fn get(&self, key: &CacheKey) -> Option<Vec<String>> {
        let entry = self.entries.get(key)?;
        if let Some(ttl) = entry.ttl
            && entry.inserted.elapsed() > ttl
        {
            return None;
        }
        Some(entry.values.clone())
    }

    pub(crate) fn put(&mut self, key: CacheKey, values: Vec<String>, ttl: Option<Duration>) {
        self.entries.insert(
            key,
            CacheEntry {
                values,
                inserted: Instant::now(),
                ttl,
            },
        );
    }
}

/// Hash the `partial` map into a stable u64 for cache keying.
///
/// Sorts keys first so `{a: 1, b: 2}` and `{b: 2, a: 1}` produce the same
/// hash — `HashMap` iteration order is unspecified.
fn hash_partial(partial: &HashMap<String, String>) -> u64 {
    let mut pairs: Vec<(&str, &str)> = partial
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    pairs.sort_unstable();
    let mut hasher = DefaultHasher::new();
    pairs.hash(&mut hasher);
    hasher.finish()
}

/// All inputs needed to resolve completions for a single param. Bundled
/// into one struct so the public API stays under clippy's argument-count
/// threshold; also keeps call sites readable.
pub(crate) struct ResolveRequest<'a> {
    pub cache: &'a Arc<RwLock<CompletionCache>>,
    pub task: &'a str,
    pub param: &'a str,
    pub completions: &'a Completions,
    /// Project root — completion command's working directory and the base
    /// for the failure-log path.
    pub base_dir: &'a Path,
    /// Task's configured env, merged into don's env before spawn.
    pub task_env: &'a HashMap<String, String>,
    /// Already-entered values for *other* params in the form, exposed as
    /// `DON_PARAM_<NAME>` env vars to the child.
    pub partial: &'a HashMap<String, String>,
    /// When true, skip the cache but still populate it on success.
    pub force_refresh: bool,
}

/// A cloneable handle for resolving task-param completions without the
/// runner: task configs are fixed at construction, the cache is shared, and
/// the shell-out runs on the caller's task. This is to completions what
/// [`LogReader`](crate::output::LogReader) is to logs — the server holds one
/// and answers `POST /completions` with no runner round trip.
#[derive(Clone)]
pub struct CompletionResolver {
    tasks: Arc<std::collections::HashMap<String, crate::config::Task>>,
    cache: Arc<RwLock<CompletionCache>>,
    base_dir: std::path::PathBuf,
}

impl CompletionResolver {
    /// Build a resolver over a fixed task-config set.
    pub fn new(
        tasks: std::collections::HashMap<String, crate::config::Task>,
        base_dir: std::path::PathBuf,
    ) -> Self {
        Self {
            tasks: Arc::new(tasks),
            cache: Arc::new(RwLock::new(CompletionCache::default())),
            base_dir,
        }
    }

    /// An empty resolver for tests that need an `ApiState` without a runner.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        Self::new(std::collections::HashMap::new(), std::path::PathBuf::new())
    }

    /// Resolve candidate values for `param` on `task`: static `choices`
    /// answer directly; a `completions` block shells out through
    /// [`resolve`], with caching and failure logs.
    pub async fn resolve_param(
        &self,
        task: &str,
        param: &str,
        partial: &HashMap<String, String>,
        force_refresh: bool,
    ) -> Result<Vec<String>, CompletionError> {
        let Some(cfg) = self.tasks.get(task) else {
            return Err(CompletionError {
                message: format!("unknown task '{task}'"),
                log_path: None,
            });
        };
        let Some(p) = cfg.params.iter().find(|p| p.name == param) else {
            return Err(CompletionError {
                message: format!("task '{task}' has no param '{param}'"),
                log_path: None,
            });
        };
        let Some(completion_cfg) = p.completions.clone() else {
            // Static choices fast-path: return them directly without any
            // shell-out. The TUI form can still fuzzy-filter locally.
            return Ok(p.choices.clone());
        };
        resolve(ResolveRequest {
            cache: &self.cache,
            task,
            param,
            completions: &completion_cfg,
            base_dir: &self.base_dir,
            task_env: &cfg.env,
            partial,
            force_refresh,
        })
        .await
    }
}

/// Resolve candidate values for one param by running its `completions`
/// command. See [`ResolveRequest`] for the field meanings.
pub(crate) async fn resolve(req: ResolveRequest<'_>) -> Result<Vec<String>, CompletionError> {
    let ResolveRequest {
        cache,
        task,
        param,
        completions,
        base_dir,
        task_env,
        partial,
        force_refresh,
    } = req;
    let key = CacheKey {
        task: task.to_string(),
        param: param.to_string(),
        partial_hash: hash_partial(partial),
    };

    if !force_refresh && let Some(cached) = cache.read().await.get(&key) {
        return Ok(cached);
    }

    let ttl = match completions.cache.as_deref() {
        Some(s) => Some(parse_duration(s).map_err(|e| CompletionError {
            message: format!("invalid cache duration: {e}"),
            log_path: None,
        })?),
        None => None,
    };
    let timeout = match completions.timeout.as_deref() {
        Some(s) => parse_duration(s).map_err(|e| CompletionError {
            message: format!("invalid timeout: {e}"),
            log_path: None,
        })?,
        None => Duration::from_secs(10),
    };

    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.extend(task_env.clone());
    crate::sys::env::prepend_to_path(&mut env, &base_dir.join(".don").join("bin"));
    for (k, v) in partial {
        env.insert(format!("DON_PARAM_{}", k.to_ascii_uppercase()), v.clone());
    }

    let output = match run_command(completions, base_dir, &env, timeout).await {
        Ok(o) => o,
        Err(e) => {
            let log_path =
                write_failure_log(base_dir, task, param, completions, None, &e.to_string()).await;
            return Err(CompletionError {
                message: format!("completion command failed: {e}"),
                log_path,
            });
        }
    };

    if !output.status.success() {
        let log_path = write_failure_log(
            base_dir,
            task,
            param,
            completions,
            Some(&output),
            &format!("exit status {}", output.status),
        )
        .await;
        return Err(CompletionError {
            message: format!(
                "completion command exited with {}",
                output
                    .status
                    .code()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "signal".into())
            ),
            log_path,
        });
    }

    let parsed = match parse_output(&output.stdout, completions.parse) {
        Ok(v) => v,
        Err(msg) => {
            let log_path =
                write_failure_log(base_dir, task, param, completions, Some(&output), &msg).await;
            return Err(CompletionError {
                message: format!("completion command output could not be parsed: {msg}"),
                log_path,
            });
        }
    };

    cache.write().await.put(key, parsed.clone(), ttl);
    Ok(parsed)
}

/// Spawn the completion command and wait for it (with timeout). Captures
/// stdout + stderr for parsing / failure logging.
async fn run_command(
    completions: &Completions,
    base_dir: &Path,
    env: &HashMap<String, String>,
    timeout: Duration,
) -> std::io::Result<std::process::Output> {
    let mut cmd = Command::new(&completions.cmd);
    cmd.args(&completions.args)
        .current_dir(base_dir)
        .env_clear()
        .envs(env)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    let child = cmd.spawn()?;
    let fut = child.wait_with_output();
    match tokio::time::timeout(timeout, fut).await {
        Ok(res) => res,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("timed out after {timeout:?}"),
        )),
    }
}

/// Parse stdout bytes into candidate values per the configured strategy.
pub(crate) fn parse_output(bytes: &[u8], parse: CompletionParse) -> Result<Vec<String>, String> {
    match parse {
        CompletionParse::Lines => {
            let text = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
            Ok(text
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(|l| l.to_string())
                .collect())
        }
        CompletionParse::NullSeparated => Ok(bytes
            .split(|&b| b == 0)
            .filter(|s| !s.is_empty())
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect()),
        CompletionParse::Json => {
            let text = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
            let v: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
            let arr = v
                .as_array()
                .ok_or_else(|| "expected top-level JSON array".to_string())?;
            arr.iter()
                .map(|process| {
                    process
                        .as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| "JSON array must contain strings".to_string())
                })
                .collect()
        }
    }
}

/// Write a failure-diagnostic log file under `.don/logs/completions/` and
/// return its path (or `None` if writing itself failed — we swallow those
/// errors since the TUI has already lost this round of completion).
async fn write_failure_log(
    base_dir: &Path,
    task: &str,
    param: &str,
    completions: &Completions,
    output: Option<&std::process::Output>,
    summary: &str,
) -> Option<PathBuf> {
    let log_dir = base_dir.join(".don").join("logs").join("completions");
    if let Err(e) = tokio::fs::create_dir_all(&log_dir).await {
        eprintln!("don: failed to create completions log dir {log_dir:?}: {e}");
        return None;
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let safe_task = sanitize(task);
    let safe_param = sanitize(param);
    let path = log_dir.join(format!("{safe_task}-{safe_param}-{ts}.log"));

    let mut body = String::new();
    body.push_str(&format!("task: {task}\n"));
    body.push_str(&format!("param: {param}\n"));
    body.push_str(&format!("cmd: {}\n", completions.cmd));
    body.push_str(&format!("args: {:?}\n", completions.args));
    body.push_str(&format!("summary: {summary}\n"));
    if let Some(o) = output {
        body.push_str(&format!("status: {}\n", o.status));
        body.push_str("--- stdout ---\n");
        body.push_str(&String::from_utf8_lossy(&o.stdout));
        body.push_str("\n--- stderr ---\n");
        body.push_str(&String::from_utf8_lossy(&o.stderr));
    }

    match tokio::fs::File::create(&path).await {
        Ok(mut f) => {
            if let Err(e) = f.write_all(body.as_bytes()).await {
                eprintln!("don: failed to write completions log {path:?}: {e}");
                return None;
            }
            if let Err(e) = f.sync_all().await {
                eprintln!("don: failed to sync completions log {path:?}: {e}");
                return None;
            }
            Some(path)
        }
        Err(e) => {
            eprintln!("don: failed to create completions log {path:?}: {e}");
            None
        }
    }
}

/// Replace anything that isn't `[A-Za-z0-9_-]` with `_`. Keeps the log
/// filename safe across filesystems without pulling in a dep.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn parse_lines() {
        let bytes = b"users\norders\n\n  products  \n";
        let got = parse_output(bytes, CompletionParse::Lines).unwrap();
        assert_eq!(got, vec!["users", "orders", "products"]);
    }

    #[test]
    fn parse_null_separated() {
        let bytes = b"a\0b\0\0c\0";
        let got = parse_output(bytes, CompletionParse::NullSeparated).unwrap();
        assert_eq!(got, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_json_array() {
        let bytes = br#"["users", "orders"]"#;
        let got = parse_output(bytes, CompletionParse::Json).unwrap();
        assert_eq!(got, vec!["users", "orders"]);
    }

    #[test]
    fn parse_json_non_array_errors() {
        let bytes = br#"{"not": "array"}"#;
        let err = parse_output(bytes, CompletionParse::Json).unwrap_err();
        assert!(err.contains("array"), "got {err}");
    }

    #[test]
    fn parse_json_non_strings_errors() {
        let bytes = b"[1, 2]";
        let err = parse_output(bytes, CompletionParse::Json).unwrap_err();
        assert!(err.contains("strings"), "got {err}");
    }

    #[test]
    fn cache_hit_and_expiry() {
        let mut cache = CompletionCache::default();
        let key = CacheKey {
            task: "t".into(),
            param: "p".into(),
            partial_hash: 0,
        };
        cache.put(key.clone(), vec!["a".into()], None);
        assert_eq!(cache.get(&key), Some(vec!["a".into()]));

        let key2 = CacheKey {
            task: "t".into(),
            param: "p".into(),
            partial_hash: 1,
        };
        cache.put(
            key2.clone(),
            vec!["b".into()],
            Some(Duration::from_millis(1)),
        );
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(cache.get(&key2), None, "expired entry should be missed");
    }

    #[test]
    fn hash_partial_is_order_independent() {
        let a: HashMap<String, String> = [
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
        ]
        .into_iter()
        .collect();
        let b: HashMap<String, String> = [
            ("k2".to_string(), "v2".to_string()),
            ("k1".to_string(), "v1".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(hash_partial(&a), hash_partial(&b));
    }

    #[test]
    fn sanitize_keeps_safe_chars() {
        assert_eq!(sanitize("abc-123_DEF"), "abc-123_DEF");
        assert_eq!(sanitize("foo/bar baz"), "foo_bar_baz");
        assert_eq!(sanitize("../danger"), "___danger");
    }

    #[tokio::test]
    async fn resolve_success_writes_cache() {
        // Use `printf` as a reliable way to emit newline-separated output.
        let cache = Arc::new(RwLock::new(CompletionCache::default()));
        let base_dir = tempfile::tempdir().unwrap();
        let completions = Completions {
            cmd: "printf".into(),
            args: vec![r"alpha\nbeta\n".into()],
            parse: CompletionParse::Lines,
            cache: Some("1h".into()),
            timeout: None,
        };
        let env = HashMap::new();
        let partial = HashMap::new();
        let got = resolve(ResolveRequest {
            cache: &cache,
            task: "task",
            param: "param",
            completions: &completions,
            base_dir: base_dir.path(),
            task_env: &env,
            partial: &partial,
            force_refresh: false,
        })
        .await
        .unwrap();
        assert_eq!(got, vec!["alpha", "beta"]);

        // Second call hits the cache — proved by swapping the command to
        // one that would otherwise fail, but with the original key.
        let broken = Completions {
            cmd: "definitely-not-a-real-binary-xyz".into(),
            ..completions.clone()
        };
        let again = resolve(ResolveRequest {
            cache: &cache,
            task: "task",
            param: "param",
            completions: &broken,
            base_dir: base_dir.path(),
            task_env: &env,
            partial: &partial,
            force_refresh: false,
        })
        .await
        .unwrap();
        assert_eq!(again, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn resolve_failure_writes_log() {
        let cache = Arc::new(RwLock::new(CompletionCache::default()));
        let base_dir = tempfile::tempdir().unwrap();
        let completions = Completions {
            cmd: "false".into(),
            args: vec![],
            parse: CompletionParse::Lines,
            cache: None,
            timeout: None,
        };
        let err = resolve(ResolveRequest {
            cache: &cache,
            task: "sync",
            param: "index",
            completions: &completions,
            base_dir: base_dir.path(),
            task_env: &HashMap::new(),
            partial: &HashMap::new(),
            force_refresh: false,
        })
        .await
        .unwrap_err();
        let log_path = err.log_path.expect("log path should be set on failure");
        assert!(log_path.exists(), "log file should have been written");
        let body = std::fs::read_to_string(&log_path).unwrap();
        assert!(body.contains("task: sync"));
        assert!(body.contains("param: index"));
    }

    #[tokio::test]
    async fn resolve_timeout_writes_log() {
        let cache = Arc::new(RwLock::new(CompletionCache::default()));
        let base_dir = tempfile::tempdir().unwrap();
        let completions = Completions {
            cmd: "sleep".into(),
            args: vec!["5".into()],
            parse: CompletionParse::Lines,
            cache: None,
            timeout: Some("100ms".into()),
        };
        let err = resolve(ResolveRequest {
            cache: &cache,
            task: "t",
            param: "p",
            completions: &completions,
            base_dir: base_dir.path(),
            task_env: &HashMap::new(),
            partial: &HashMap::new(),
            force_refresh: false,
        })
        .await
        .unwrap_err();
        assert!(err.message.contains("failed") || err.message.contains("timed"));
    }
}
