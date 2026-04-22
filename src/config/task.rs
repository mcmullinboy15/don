use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

use super::download::DownloadConfig;
use super::param::TaskParam;
use super::platform::Platform;
use super::types::{BazelConfig, LogConfig, TurboConfig};

/// A one-shot task that runs to completion.
///
/// Tasks can depend on services (waits for ready) and other tasks.
/// File watching determines whether the task needs to re-run.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Task {
    /// The command to execute.
    pub cmd: String,
    /// Arguments to pass to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// Working directory.
    pub dir: Option<PathBuf>,
    /// Environment variables.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Services or tasks that must be ready/complete before this task runs.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// File glob patterns — task only re-runs if these files changed since last success.
    /// If empty, the task always runs.
    #[serde(default)]
    pub watch: Vec<String>,
    /// File glob patterns to ignore when watching (e.g. "**/*.log", "target/**").
    #[serde(default)]
    pub ignore: Vec<String>,
    /// Maximum time the task is allowed to run (e.g. "5m", "30s"). No timeout by default.
    pub timeout: Option<String>,
    /// Where to send stdout/stderr. Defaults to stdout.
    #[serde(default)]
    pub log: LogConfig,
    /// Whether the task runs automatically.
    /// When false, the task starts in a pending state (both at startup and on
    /// file-watch changes) and must be triggered manually via `don run --all-pending`.
    /// Defaults to true.
    #[serde(default = "default_auto_run")]
    pub auto_run: bool,
    /// Optional download configuration — artifacts to fetch before running.
    /// When a download exists for the current platform, its binary path
    /// replaces `cmd`. Without a matching platform entry, `cmd` is looked up on PATH.
    pub download: Option<DownloadConfig>,
    /// Bazel build tool integration — auto-resolve watch patterns from the build graph.
    /// Mutually exclusive with `turbo`.
    pub bazel: Option<BazelConfig>,
    /// Turborepo build tool integration — auto-resolve watch patterns from the task graph.
    /// Mutually exclusive with `bazel`.
    pub turbo: Option<TurboConfig>,
    /// Optional parameter declarations. When non-empty, the task is
    /// considered "interactive" — file-watch changes park it in
    /// `PendingRun` instead of auto-running, and the user supplies values
    /// via `don run <task> --<name>=<value>` or the TUI form.
    /// Values substitute into `cmd`/`args`/`env`/`dir` via `{{name}}`
    /// placeholders.
    #[serde(default)]
    pub params: Vec<TaskParam>,
}

impl Task {
    /// Resolve the task's command path, using the cached download binary
    /// if one is configured for this platform.
    pub fn resolved_cmd(
        &self,
        platform: Platform,
        task_name: &str,
        cache_base: Option<&std::path::Path>,
    ) -> Result<PathBuf, String> {
        let cache_base = cache_base
            .map(PathBuf::from)
            .unwrap_or_else(super::download::default_cache_base);

        let executable = match &self.download {
            Some(dl) => match dl.for_platform(platform) {
                Some(artifact) => artifact
                    .binary_path(&cache_base, task_name)
                    .ok_or_else(|| format!("download url has no filename: {}", artifact.url))?,
                None => PathBuf::from(&self.cmd),
            },
            None => PathBuf::from(&self.cmd),
        };
        Ok(executable)
    }
}

fn default_auto_run() -> bool {
    true
}
