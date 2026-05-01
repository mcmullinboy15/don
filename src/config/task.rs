use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

use super::download::DownloadConfig;
use super::param::TaskParam;
use super::platform::Platform;
use super::types::{BazelConfig, LogConfig, TurboConfig};

/// Automatic run policy for a task.
///
/// This controls whether don may start the task without an explicit manual
/// trigger when the task is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskAutoRun {
    /// Run automatically whenever the runner decides the task is needed.
    #[default]
    Always,
    /// Never run automatically; move to `PendingRun` instead.
    Never,
    /// Run automatically only on startup, and only until the task has one
    /// successful run recorded. After that, the task becomes manual forever
    /// unless the user explicitly triggers it.
    Once,
}

impl TaskAutoRun {
    pub(crate) fn runs_automatically_on_startup(self, has_success: bool) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::Once => !has_success,
        }
    }

    pub(crate) fn runs_automatically_on_watch(self) -> bool {
        matches!(self, Self::Always)
    }
}

impl<'de> Deserialize<'de> for TaskAutoRun {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawTaskAutoRun {
            Bool(bool),
            String(String),
        }

        match RawTaskAutoRun::deserialize(deserializer)? {
            RawTaskAutoRun::Bool(true) => Ok(TaskAutoRun::Always),
            RawTaskAutoRun::Bool(false) => Ok(TaskAutoRun::Never),
            RawTaskAutoRun::String(value) => match value.as_str() {
                "always" => Ok(TaskAutoRun::Always),
                "never" => Ok(TaskAutoRun::Never),
                "once" => Ok(TaskAutoRun::Once),
                _ => Err(serde::de::Error::custom(format!(
                    "unknown auto_run value '{value}', expected true, false, \"always\", \"never\", or \"once\""
                ))),
            },
        }
    }
}

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
    ///
    /// Supported values:
    /// - `true` / `"always"`: run automatically whenever needed
    /// - `false` / `"never"`: never auto-run; enter `PendingRun` when needed
    /// - `"once"`: auto-run on startup until the first successful run, then
    ///   become manual forever unless explicitly triggered
    ///
    /// “Needed” means a dependent is waiting on the task, or watched inputs
    /// have changed. Defaults to `true`.
    #[serde(default)]
    pub auto_run: TaskAutoRun,
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
    /// considered "interactive" — when the task is needed, file-watch
    /// changes or dependent startup will park it in `PendingRun` instead
    /// of auto-running, and the user supplies values via `don run <task>
    /// --<name>=<value>` or the TUI form.
    /// Values substitute into `cmd`/`args`/`env`/`dir` via `{{name}}`
    /// placeholders.
    #[serde(default)]
    pub params: Vec<TaskParam>,
    /// Whether this task's log output is hidden by default in the TUI
    /// filter. Users can still unhide it interactively from the filter view.
    /// Defaults to `false` (visible).
    #[serde(default)]
    pub hidden: bool,
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
