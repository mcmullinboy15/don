//! Turborepo build tool integration.
//!
//! Queries Turborepo for the task graph and source inputs using
//! `turbo run <task> --dry-run=json`. The JSON output contains the full
//! resolved task graph including commands, directories, dependencies,
//! and input file mappings.

use super::{BuildGraphResolver, BuildToolError, ResolvedBuildInfo};
use std::path::Path;
use std::time::Duration;

/// Default query timeout in seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Turborepo build graph resolver.
///
/// Shells out to `turbo run --dry-run=json` to get the resolved task graph.
/// Extracts watch paths from the `inputs` and `directory` fields.
/// Falls back to `npx turbo` if `turbo` is not directly on PATH.
pub(crate) struct TurboResolver {
    /// The turbo task name to query (e.g. "dev", "build").
    task: String,
    /// Optional package filter (e.g. "@myorg/api").
    filter: Option<String>,
    /// Query timeout duration.
    timeout: Duration,
}

impl TurboResolver {
    /// Create a new resolver for the given turbo task.
    pub(crate) fn new(task: &str, filter: Option<&str>, timeout_secs: Option<u64>) -> Self {
        Self {
            task: task.to_string(),
            filter: filter.map(String::from),
            timeout: Duration::from_secs(timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS)),
        }
    }

    /// Find the turbo command — either `turbo` on PATH or `npx turbo` as fallback.
    /// Returns the (program, prefix_args) to use for running turbo.
    async fn find_turbo_cmd(&self) -> Result<(String, Vec<String>), BuildToolError> {
        // Try `turbo` directly first.
        let direct = tokio::process::Command::new("turbo")
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;

        if let Ok(status) = direct
            && status.success()
        {
            return Ok(("turbo".to_string(), Vec::new()));
        }

        // Fall back to `npx turbo`.
        let npx = tokio::process::Command::new("npx")
            .args(["turbo", "--version"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;

        match npx {
            Ok(status) if status.success() => {
                Ok(("npx".to_string(), vec!["turbo".to_string()]))
            }
            _ => Err(BuildToolError::NotInstalled {
                tool: "turbo".to_string(),
            }),
        }
    }

    /// Run `turbo run <build_task>` for the given package filters.
    ///
    /// Turbo handles parallelism internally across workspace packages.
    /// Build output is streamed line-by-line through the provided callback.
    pub(crate) async fn build_packages<F>(
        &self,
        build_task: &str,
        filters: &[String],
        working_dir: &Path,
        mut on_line: F,
    ) -> Result<super::BatchBuildResult, BuildToolError>
    where
        F: FnMut(&str) + Send + 'static,
    {
        if filters.is_empty() {
            return Ok(super::BatchBuildResult {
                succeeded: Vec::new(),
                failed: Vec::new(),
            });
        }

        let (program, prefix_args) = self.find_turbo_cmd().await?;

        let mut cmd = tokio::process::Command::new(&program);
        cmd.args(&prefix_args);
        cmd.args(["run", build_task]);

        for filter in filters {
            cmd.args(["--filter", filter]);
        }

        cmd.current_dir(working_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| BuildToolError::Io {
            tool: "turbo".to_string(),
            source: e,
        })?;

        // Stream stdout (turbo writes build output to stdout).
        let stdout = child.stdout.take();
        let stream_handle = tokio::spawn(async move {
            if let Some(stdout) = stdout {
                let mut reader = tokio::io::BufReader::new(stdout);
                let mut line_buf = Vec::new();
                loop {
                    line_buf.clear();
                    match tokio::io::AsyncBufReadExt::read_until(
                        &mut reader, b'\n', &mut line_buf,
                    ).await {
                        Ok(0) => break,
                        Ok(_) => {
                            if line_buf.last() == Some(&b'\n') { line_buf.pop(); }
                            if line_buf.last() == Some(&b'\r') { line_buf.pop(); }
                            let text = String::from_utf8_lossy(&line_buf);
                            on_line(&text);
                        }
                        Err(_) => break,
                    }
                }
            }
        });

        // Drain stderr.
        let stderr = child.stderr.take();
        let stderr_handle = tokio::spawn(async move {
            let mut collected = String::new();
            if let Some(stderr) = stderr {
                let mut reader = tokio::io::BufReader::new(stderr);
                let _ = tokio::io::AsyncReadExt::read_to_string(
                    &mut reader, &mut collected,
                ).await;
            }
            collected
        });

        let timeout_secs = self.timeout.as_secs();
        let status = tokio::time::timeout(self.timeout, child.wait())
            .await
            .map_err(|_| BuildToolError::QueryTimeout {
                tool: "turbo".to_string(),
                timeout_secs,
            })?
            .map_err(|e| BuildToolError::Io {
                tool: "turbo".to_string(),
                source: e,
            })?;

        let _ = stream_handle.await;
        let stderr_output = stderr_handle.await.unwrap_or_default();

        if status.success() {
            Ok(super::BatchBuildResult {
                succeeded: filters.to_vec(),
                failed: Vec::new(),
            })
        } else {
            let code = status.code().unwrap_or(-1);
            let error_msg = if stderr_output.trim().is_empty() {
                format!("turbo run {build_task} failed (exit code {code})")
            } else {
                let truncated = if stderr_output.len() > 300 {
                    format!("{}...", &stderr_output[..300])
                } else {
                    stderr_output.trim().to_string()
                };
                format!("turbo run {build_task} failed: {truncated}")
            };
            // Turbo doesn't easily report per-package failures in a parseable way,
            // so conservatively mark all filtered packages as failed.
            Ok(super::BatchBuildResult {
                succeeded: Vec::new(),
                failed: filters
                    .iter()
                    .map(|f| (f.clone(), error_msg.clone()))
                    .collect(),
            })
        }
    }
}

/// A task from Turborepo's `--dry-run=json` output.
#[derive(serde::Deserialize)]
struct TurboTask {
    /// Fully qualified task ID (e.g. "@myorg/api#dev").
    #[serde(rename = "taskId")]
    task_id: String,
    /// Workspace-relative directory of the package.
    directory: String,
    /// Task IDs this task depends on.
    #[serde(default)]
    dependencies: Vec<String>,
    /// Input files mapped to their hashes. Keys are relative file paths.
    #[serde(default)]
    inputs: std::collections::HashMap<String, String>,
    /// The resolved task definition.
    #[serde(default, rename = "resolvedTaskDefinition")]
    resolved_task_definition: Option<TurboTaskDefinition>,
}

/// Resolved task definition from Turborepo.
#[derive(serde::Deserialize, Default)]
struct TurboTaskDefinition {
    /// Whether this is a long-running task (like a dev server).
    #[serde(default)]
    persistent: bool,
}

/// Top-level structure of `turbo run --dry-run=json` output.
#[derive(serde::Deserialize)]
struct TurboDryRun {
    tasks: Vec<TurboTask>,
}

/// Parse the dry-run JSON and extract watch paths and dependencies.
///
/// For each task in the output, the `directory` field is the workspace package
/// root and `inputs` contains the individual files. We use directory-level
/// patterns for watch efficiency.
pub(crate) fn parse_dry_run(json: &str) -> Result<Vec<ParsedTurboTask>, BuildToolError> {
    let dry_run: TurboDryRun =
        serde_json::from_str(json).map_err(|e| BuildToolError::ParseError {
            tool: "turbo".to_string(),
            message: format!("failed to parse dry-run JSON: {e}"),
        })?;

    Ok(dry_run.tasks.into_iter().map(|task| {
        let persistent = task
            .resolved_task_definition
            .as_ref()
            .is_some_and(|d| d.persistent);

        // Collect unique subdirectories from input file paths.
        // This gives us directory-level watch patterns rather than
        // watching every individual file.
        let mut input_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();
        for file_path in task.inputs.keys() {
            // Get the parent directory of each input file
            if let Some(parent) = std::path::Path::new(file_path).parent() {
                let dir_str = parent.to_string_lossy();
                if !dir_str.is_empty() && dir_str != "." {
                    input_dirs.insert(format!("{dir_str}/**"));
                }
            }
        }

        // If no specific input dirs were found, watch the whole package directory
        let watch_paths = if input_dirs.is_empty() {
            vec![format!("{}/**", task.directory)]
        } else {
            // Prefix input dirs with the task directory since inputs are
            // relative to the workspace root, not the package
            let mut paths: Vec<String> = input_dirs.into_iter().collect();
            paths.sort();
            paths
        };

        ParsedTurboTask {
            task_id: task.task_id,
            directory: task.directory,
            dependencies: task.dependencies,
            watch_paths,
            persistent,
        }
    }).collect())
}

/// A parsed turbo task with resolved watch information.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields are part of the public parse result; used by consumers.
pub(crate) struct ParsedTurboTask {
    /// Fully qualified task ID (e.g. "@myorg/api#dev").
    pub task_id: String,
    /// Workspace-relative directory.
    pub directory: String,
    /// Task IDs this task depends on.
    pub dependencies: Vec<String>,
    /// Glob patterns to watch for source changes.
    pub watch_paths: Vec<String>,
    /// Whether this is a long-running (persistent) task.
    pub persistent: bool,
}

impl BuildGraphResolver for TurboResolver {
    async fn resolve(
        &self,
        _target: &str,
        working_dir: &Path,
    ) -> Result<ResolvedBuildInfo, BuildToolError> {
        let (program, prefix_args) = self.find_turbo_cmd().await?;

        let mut cmd = tokio::process::Command::new(&program);
        cmd.args(&prefix_args);
        cmd.args(["run", &self.task, "--dry-run=json"]);

        if let Some(ref filter) = self.filter {
            cmd.args(["--filter", filter]);
        }

        cmd.current_dir(working_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let child = cmd.spawn().map_err(|e| BuildToolError::Io {
            tool: "turbo".to_string(),
            source: e,
        })?;

        let timeout_secs = self.timeout.as_secs();
        let output = tokio::time::timeout(self.timeout, child.wait_with_output())
            .await
            .map_err(|_| BuildToolError::QueryTimeout {
                tool: "turbo".to_string(),
                timeout_secs,
            })?
            .map_err(|e| BuildToolError::Io {
                tool: "turbo".to_string(),
                source: e,
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let truncated = if stderr.len() > 500 {
                format!("{}...", &stderr[..500])
            } else {
                stderr.to_string()
            };
            return Err(BuildToolError::QueryFailed {
                tool: "turbo".to_string(),
                message: truncated.trim().to_string(),
            });
        }

        let stdout = String::from_utf8(output.stdout).map_err(|e| BuildToolError::ParseError {
            tool: "turbo".to_string(),
            message: format!("non-UTF-8 output: {e}"),
        })?;

        let tasks = parse_dry_run(&stdout)?;

        // Aggregate watch paths and dependencies from all tasks in the graph.
        let mut all_watch_paths = Vec::new();
        let mut all_dependencies = Vec::new();
        let mut seen_paths: std::collections::HashSet<String> = std::collections::HashSet::new();

        for task in &tasks {
            for path in &task.watch_paths {
                if seen_paths.insert(path.clone()) {
                    all_watch_paths.push(path.clone());
                }
            }
            for dep in &task.dependencies {
                if !all_dependencies.contains(dep) {
                    all_dependencies.push(dep.clone());
                }
            }
        }

        Ok(ResolvedBuildInfo {
            watch_paths: all_watch_paths,
            dependencies: all_dependencies,
            graph_definition_globs: vec![
                "**/package.json".to_string(),
                "turbo.json".to_string(),
                "turbo.jsonc".to_string(),
                "pnpm-workspace.yaml".to_string(),
            ],
        })
    }

    fn tool_name(&self) -> &'static str {
        "turbo"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    const SAMPLE_DRY_RUN: &str = r#"{
        "id": "abc123",
        "version": "1",
        "turboVersion": "2.0.0",
        "monorepo": true,
        "tasks": [
            {
                "taskId": "@myorg/utils#build",
                "task": "build",
                "package": "@myorg/utils",
                "directory": "packages/utils",
                "dependencies": [],
                "inputs": {
                    "packages/utils/src/index.ts": "abc123",
                    "packages/utils/src/helpers.ts": "def456",
                    "packages/utils/package.json": "ghi789"
                },
                "resolvedTaskDefinition": {
                    "persistent": false
                }
            },
            {
                "taskId": "@myorg/web#dev",
                "task": "dev",
                "package": "@myorg/web",
                "directory": "apps/web",
                "dependencies": ["@myorg/utils#build"],
                "inputs": {
                    "apps/web/src/App.tsx": "aaa111",
                    "apps/web/src/index.tsx": "bbb222",
                    "apps/web/pages/index.tsx": "ccc333",
                    "apps/web/package.json": "ddd444"
                },
                "resolvedTaskDefinition": {
                    "persistent": true
                }
            }
        ]
    }"#;

    #[test]
    fn test_parse_dry_run_basic() {
        let tasks = parse_dry_run(SAMPLE_DRY_RUN).unwrap();
        assert_eq!(tasks.len(), 2);

        let utils = &tasks[0];
        assert_eq!(utils.task_id, "@myorg/utils#build");
        assert_eq!(utils.directory, "packages/utils");
        assert!(utils.dependencies.is_empty());
        assert!(!utils.persistent);
        // Should have directory-level patterns from input files
        assert!(utils.watch_paths.iter().any(|p| p.contains("packages/utils/src")));

        let web = &tasks[1];
        assert_eq!(web.task_id, "@myorg/web#dev");
        assert_eq!(web.directory, "apps/web");
        assert_eq!(web.dependencies, vec!["@myorg/utils#build"]);
        assert!(web.persistent);
        assert!(web.watch_paths.iter().any(|p| p.contains("apps/web/src")));
    }

    #[test]
    fn test_parse_dry_run_persistent_flag() {
        struct Case {
            name: &'static str,
            json: &'static str,
            expected_persistent: bool,
        }

        let cases = vec![
            Case {
                name: "persistent true",
                json: r#"{"tasks": [{"taskId": "a#dev", "directory": "a", "dependencies": [], "inputs": {}, "resolvedTaskDefinition": {"persistent": true}}]}"#,
                expected_persistent: true,
            },
            Case {
                name: "persistent false",
                json: r#"{"tasks": [{"taskId": "a#build", "directory": "a", "dependencies": [], "inputs": {}, "resolvedTaskDefinition": {"persistent": false}}]}"#,
                expected_persistent: false,
            },
            Case {
                name: "no resolved definition",
                json: r#"{"tasks": [{"taskId": "a#test", "directory": "a", "dependencies": [], "inputs": {}}]}"#,
                expected_persistent: false,
            },
        ];

        for case in cases {
            let tasks = parse_dry_run(case.json).unwrap();
            assert_eq!(tasks.len(), 1, "case: {}", case.name);
            assert_eq!(
                tasks[0].persistent, case.expected_persistent,
                "case: {}",
                case.name
            );
        }
    }

    #[test]
    fn test_parse_dry_run_empty_inputs_fallback() {
        let json = r#"{"tasks": [{"taskId": "a#dev", "directory": "apps/web", "dependencies": [], "inputs": {}}]}"#;
        let tasks = parse_dry_run(json).unwrap();
        assert_eq!(tasks[0].watch_paths, vec!["apps/web/**"]);
    }

    #[test]
    fn test_parse_dry_run_invalid_json() {
        let result = parse_dry_run("not json");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("turbo"));
    }

    #[test]
    fn test_parse_dry_run_empty_tasks() {
        let json = r#"{"tasks": []}"#;
        let tasks = parse_dry_run(json).unwrap();
        assert!(tasks.is_empty());
    }
}
