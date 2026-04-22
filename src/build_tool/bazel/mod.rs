//! Bazel build tool integration.
//!
//! Queries Bazel for the source packages that feed into a given target,
//! using `bazel query` with `--output=package` for directory-level granularity.
//! External dependencies and generated files are filtered out.

mod graph;

use super::{AbortOnDrop, BuildGraphResolver, BuildToolError, ResolvedBuildInfo};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

/// Default query timeout in seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Bazel build graph resolver.
///
/// Shells out to `bazel query` to determine which first-party source packages
/// contribute to a given target. The resolved packages become watch directories.
pub(crate) struct BazelResolver {
    /// Query timeout duration.
    timeout: Duration,
}

impl BazelResolver {
    /// Create a new resolver with the given timeout in seconds.
    pub(crate) fn new(timeout_secs: Option<u64>) -> Self {
        Self {
            timeout: Duration::from_secs(timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS)),
        }
    }

    /// Check that the `bazel` binary is available on PATH.
    async fn check_installed(&self) -> Result<(), BuildToolError> {
        let result = tokio::process::Command::new("bazel")
            .arg("version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;

        match result {
            Ok(status) if status.success() => Ok(()),
            Ok(_) => Ok(()), // bazel version may return non-zero in some setups, but binary exists
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(BuildToolError::NotInstalled {
                    tool: "bazel".to_string(),
                })
            }
            Err(e) => Err(BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            }),
        }
    }

    /// Run a bazel query and return stdout as a string.
    async fn run_query(
        &self,
        query: &str,
        output_format: &str,
        working_dir: &Path,
    ) -> Result<String, BuildToolError> {
        let child = tokio::process::Command::new("bazel")
            .args(["query", query, &format!("--output={output_format}")])
            .current_dir(working_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            })?;

        let timeout_secs = self.timeout.as_secs();
        let output = tokio::time::timeout(self.timeout, child.wait_with_output())
            .await
            .map_err(|_| BuildToolError::QueryTimeout {
                tool: "bazel".to_string(),
                timeout_secs,
            })?
            .map_err(|e| BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Truncate long error messages
            let truncated = if stderr.len() > 500 {
                format!("{}...", &stderr[..500])
            } else {
                stderr.to_string()
            };
            return Err(BuildToolError::QueryFailed {
                tool: "bazel".to_string(),
                message: truncated.trim().to_string(),
            });
        }

        String::from_utf8(output.stdout).map_err(|e| BuildToolError::ParseError {
            tool: "bazel".to_string(),
            message: format!("non-UTF-8 output: {e}"),
        })
    }

    /// Run `bazel build` for multiple targets in a single invocation.
    ///
    /// Bazel parallelizes the build internally, so this is more efficient than
    /// running separate builds per target. Returns which targets succeeded/failed.
    ///
    /// Build output is streamed line-by-line through the provided callback.
    pub(crate) async fn build_targets<F>(
        &self,
        targets: &[String],
        working_dir: &Path,
        mut on_line: F,
        emitter: Option<&crate::output::LifecycleEmitter>,
    ) -> Result<super::BatchBuildResult, BuildToolError>
    where
        F: FnMut(&str) + Send + 'static,
    {
        if targets.is_empty() {
            return Ok(super::BatchBuildResult {
                succeeded: Vec::new(),
                failed: Vec::new(),
            });
        }

        self.check_installed().await?;

        let mut cmd = tokio::process::Command::new("bazel");
        cmd.arg("build");
        // `--curses=no` forces line-buffered progress output. Without it,
        // bazel detects the piped stderr and *may* still emit progress with
        // \r-only updates (or buffer for seconds), so our line-reader sees
        // nothing for long stretches of analysis/loading. With curses off,
        // each progress tick is a separate \n-terminated line we can stream.
        cmd.arg("--curses=no");
        // Bazel's `--color=auto` suppresses ANSI when stderr isn't a TTY
        // (our case — we pipe it). Force colors on so INFO/WARN/ERROR are
        // visually distinct in the bazel-prefixed stream. Our sanitize pass
        // keeps SGR sequences, strips only cursor/screen codes.
        cmd.arg("--color=yes");
        cmd.args(targets);
        cmd.current_dir(working_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // If the spawning future is dropped (e.g. shutdown mid-startup),
            // SIGKILL the bazel client. The bazel server detects the client
            // disconnect and cancels the in-flight build.
            .kill_on_drop(true);

        if let Some(em) = emitter {
            let mut args: Vec<String> = vec![
                "build".into(),
                "--curses=no".into(),
                "--color=yes".into(),
            ];
            args.extend(targets.iter().cloned());
            em.debug_spawn("bazel", "bazel", &args);
        }

        let mut child = cmd.spawn().map_err(|e| BuildToolError::Io {
            tool: "bazel".to_string(),
            source: e,
        })?;

        // Stream stderr (Bazel writes build progress to stderr).
        // Wrap the spawn handle in an AbortOnDrop guard so cancellation of
        // `build_targets` (e.g. shutdown mid-build) tears the reader down
        // immediately. Without this, the reader stays alive holding a clone
        // of the `on_line` callback's senders (often a `LifecycleEmitter`
        // bound to the OutputManager). Bazel's child *build action*
        // processes inherit fds 1/2, so the pipe doesn't close just because
        // the bazel client gets SIGKILL'd — the action processes can keep
        // the writer end open for minutes. That kept stdout_sink_task's
        // channel from closing and made `OutputManager::shutdown` hang.
        let stderr = child.stderr.take();
        let targets_for_parse = targets.to_vec();
        let stream_handle = AbortOnDrop::new(tokio::spawn(async move {
            let mut failed_targets: Vec<String> = Vec::new();
            if let Some(stderr) = stderr {
                let mut reader = tokio::io::BufReader::new(stderr);
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
                            // Parse ERROR lines to identify failed targets.
                            if text.contains("ERROR:") {
                                for target in &targets_for_parse {
                                    if text.contains(target.as_str()) {
                                        failed_targets.push(target.clone());
                                    }
                                }
                            }
                            on_line(&text);
                        }
                        Err(_) => break,
                    }
                }
            }
            failed_targets
        }));

        // Also drain stdout. Same drop-on-cancel rationale as stderr above.
        let stdout = child.stdout.take();
        let stdout_handle = AbortOnDrop::new(tokio::spawn(async move {
            if let Some(mut stdout) = stdout {
                let mut buf = vec![0u8; 4096];
                loop {
                    match tokio::io::AsyncReadExt::read(&mut stdout, &mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            }
        }));

        let timeout_secs = self.timeout.as_secs();
        let status = tokio::time::timeout(self.timeout, child.wait())
            .await
            .map_err(|_| BuildToolError::QueryTimeout {
                tool: "bazel".to_string(),
                timeout_secs,
            })?
            .map_err(|e| BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            })?;

        if let Some(h) = stdout_handle.into_inner() {
            let _ = h.await;
        }
        let failed_from_output = match stream_handle.into_inner() {
            Some(h) => h.await.unwrap_or_default(),
            None => Vec::new(),
        };

        if status.success() {
            Ok(super::BatchBuildResult {
                succeeded: targets.to_vec(),
                failed: Vec::new(),
            })
        } else {
            // Determine which targets failed. If we identified specific targets
            // from ERROR lines, mark those as failed and the rest as succeeded.
            // Otherwise, conservatively mark all as failed.
            if failed_from_output.is_empty() {
                let code = status.code().unwrap_or(-1);
                Ok(super::BatchBuildResult {
                    succeeded: Vec::new(),
                    failed: targets
                        .iter()
                        .map(|t| (t.clone(), format!("bazel build failed (exit code {code})")))
                        .collect(),
                })
            } else {
                let failed_set: std::collections::HashSet<&str> =
                    failed_from_output.iter().map(|s| s.as_str()).collect();
                let succeeded: Vec<String> = targets
                    .iter()
                    .filter(|t| !failed_set.contains(t.as_str()))
                    .cloned()
                    .collect();
                let failed: Vec<(String, String)> = failed_from_output
                    .iter()
                    .map(|t| (t.clone(), "bazel build failed".to_string()))
                    .collect();
                Ok(super::BatchBuildResult { succeeded, failed })
            }
        }
    }

    /// Check if targets are already up to date without building.
    ///
    /// Uses `bazel build --check_up_to_date` which exits 0 if all targets
    /// are up to date and non-zero if any need rebuilding. This avoids
    /// unnecessary service restarts when a watched file changed but the
    /// build output would be identical.
    pub(crate) async fn check_up_to_date(
        &self,
        targets: &[String],
        working_dir: &Path,
    ) -> Result<bool, BuildToolError> {
        if targets.is_empty() {
            return Ok(true);
        }

        let mut cmd = tokio::process::Command::new("bazel");
        cmd.arg("build");
        cmd.arg("--check_up_to_date");
        cmd.args(targets);
        cmd.current_dir(working_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);

        let child = cmd.spawn().map_err(|e| BuildToolError::Io {
            tool: "bazel".to_string(),
            source: e,
        })?;

        let timeout_secs = self.timeout.as_secs();
        let status = tokio::time::timeout(self.timeout, child.wait_with_output())
            .await
            .map_err(|_| BuildToolError::QueryTimeout {
                tool: "bazel".to_string(),
                timeout_secs,
            })?
            .map_err(|e| BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            })?;

        Ok(status.status.success())
    }

    /// Resolve per-target source packages in ONE `bazel query` call.
    ///
    /// Runs `deps(T1 + T2 + ... + Tn) --output=xml`, stream-parses the
    /// resulting graph, and DFS-walks from each input target to attribute
    /// its own set of first-party source packages. Returns a map keyed by
    /// input target label; every input appears in the map, even if its
    /// package set is empty.
    ///
    /// This is strictly better than running N separate queries: Bazel's
    /// analysis phase loads the workspace graph once, the client starts up
    /// once, and we get accurate per-target attribution rather than a union.
    pub(crate) async fn resolve_per_target(
        &self,
        targets: &[String],
        working_dir: &Path,
    ) -> Result<HashMap<String, ResolvedBuildInfo>, BuildToolError> {
        let mut out: HashMap<String, ResolvedBuildInfo> = HashMap::new();
        if targets.is_empty() {
            return Ok(out);
        }

        self.check_installed().await?;

        // `+` is bazel's set-union operator. Targets are parenthesised so
        // operator precedence of `//` / `:` / flags can't bite us.
        let union_expr: String = targets
            .iter()
            .map(|t| format!("({t})"))
            .collect::<Vec<_>>()
            .join(" + ");
        let query = format!("deps({union_expr})");
        let xml = self.run_query(&query, "xml", working_dir).await?;

        let graph = graph::BazelDepGraph::parse_xml(xml.as_bytes())?;

        for target in targets {
            let packages = graph.packages_for(target);
            let watch_paths: Vec<String> =
                packages.iter().map(|p| format!("{p}/**")).collect();
            let graph_definition_globs: Vec<String> = packages
                .iter()
                .flat_map(|p| [format!("{p}/BUILD"), format!("{p}/BUILD.bazel")])
                .collect();
            out.insert(
                target.clone(),
                ResolvedBuildInfo {
                    watch_paths,
                    dependencies: Vec::new(),
                    graph_definition_globs,
                },
            );
        }
        Ok(out)
    }

    /// Resolve the output binary path for a Bazel target.
    ///
    /// Uses `bazel cquery --output=files` to find the built artifact path.
    /// Returns the first executable output file (typically the binary in bazel-bin).
    pub(crate) async fn resolve_binary_path(
        &self,
        target: &str,
        working_dir: &Path,
    ) -> Result<String, BuildToolError> {
        let child = tokio::process::Command::new("bazel")
            .args(["cquery", "--output=files", target])
            .current_dir(working_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            })?;

        let timeout_secs = self.timeout.as_secs();
        let output = tokio::time::timeout(self.timeout, child.wait_with_output())
            .await
            .map_err(|_| BuildToolError::QueryTimeout {
                tool: "bazel".to_string(),
                timeout_secs,
            })?
            .map_err(|e| BuildToolError::Io {
                tool: "bazel".to_string(),
                source: e,
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(BuildToolError::QueryFailed {
                tool: "bazel".to_string(),
                message: format!("cquery failed for {target}: {}", stderr.trim()),
            });
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        // cquery --output=files returns one file per line. Pick the first
        // output in bazel-out (the built artifact), not source files.
        stdout
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty() && line.starts_with("bazel-out/"))
            .map(String::from)
            .ok_or_else(|| BuildToolError::ParseError {
                tool: "bazel".to_string(),
                message: format!(
                    "no output binary found for {target} (cquery returned: {})",
                    stdout.trim()
                ),
            })
    }

    /// Parse the package output from `bazel query --output=package`.
    ///
    /// Each line is a package path like `services/api/src`. External packages
    /// (prefixed with `@`) and empty lines are filtered out. Returns raw
    /// package paths — callers append `/**` for tier-2 source globs and
    /// `/BUILD` / `/BUILD.bazel` for tier-1 build-graph globs.
    fn parse_packages(output: &str) -> Vec<String> {
        output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            // Filter out external dependencies (e.g. @rules_go//...)
            .filter(|line| !line.starts_with('@'))
            // Filter out bazel-out generated files
            .filter(|line| !line.starts_with("bazel-out"))
            .map(String::from)
            .collect()
    }
}

impl BuildGraphResolver for BazelResolver {
    async fn resolve(
        &self,
        target: &str,
        working_dir: &Path,
    ) -> Result<ResolvedBuildInfo, BuildToolError> {
        self.check_installed().await?;

        // Query for source packages that contribute to the target.
        // `kind("source file", deps(...))` gives us all source files in the
        // transitive closure. External packages (prefixed with `@`) are filtered
        // out during parsing rather than using `intersect //...` which can be
        // too aggressive with some Bazel versions.
        let query = format!(
            "kind(\"source file\", deps({target}))"
        );
        let output = self.run_query(&query, "package", working_dir).await?;
        let packages = Self::parse_packages(&output);
        let watch_paths: Vec<String> = packages.iter().map(|p| format!("{p}/**")).collect();
        // Tier-1 build-graph globs are per-package so the watcher can register
        // a non-recursive watch on each package dir (matching just the
        // `BUILD` / `BUILD.bazel` filename). Workspace-level files
        // (WORKSPACE, MODULE.bazel) are handled separately by the watch
        // manager with a single non-recursive watch on the repo root — no
        // point broadcasting them here.
        let graph_definition_globs: Vec<String> = packages
            .iter()
            .flat_map(|p| {
                [format!("{p}/BUILD"), format!("{p}/BUILD.bazel")]
            })
            .collect();

        // Query for direct first-party dependencies (for informational purposes).
        // External deps (prefixed with `@`) are filtered during parsing.
        let deps_query = format!("deps({target}, 1)");
        let deps_output = self.run_query(&deps_query, "label", working_dir).await;
        let dependencies = match deps_output {
            Ok(out) => out
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with('@') && *l != target)
                .map(String::from)
                .collect(),
            Err(_) => Vec::new(), // Non-fatal: deps query failure doesn't block watching
        };

        Ok(ResolvedBuildInfo {
            watch_paths,
            dependencies,
            graph_definition_globs,
        })
    }

    fn tool_name(&self) -> &'static str {
        "bazel"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_packages() {
        struct Case {
            name: &'static str,
            input: &'static str,
            expected: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "basic packages",
                input: "services/api/src\nlibs/auth\nlibs/common\n",
                expected: vec!["services/api/src", "libs/auth", "libs/common"],
            },
            Case {
                name: "filters external deps",
                input: "@rules_go//go/tools\nservices/api\n@com_google_protobuf//:protobuf\nlibs/db\n",
                expected: vec!["services/api", "libs/db"],
            },
            Case {
                name: "filters bazel-out",
                input: "services/api\nbazel-out/k8-fastbuild/genfiles/proto\nlibs/common\n",
                expected: vec!["services/api", "libs/common"],
            },
            Case {
                name: "handles empty lines",
                input: "\nservices/api\n\n\nlibs/db\n\n",
                expected: vec!["services/api", "libs/db"],
            },
            Case {
                name: "handles whitespace",
                input: "  services/api  \n  libs/db  \n",
                expected: vec!["services/api", "libs/db"],
            },
            Case {
                name: "empty input",
                input: "",
                expected: vec![],
            },
            Case {
                name: "only external deps",
                input: "@rules_go//go\n@io_bazel_rules_docker//docker\n",
                expected: vec![],
            },
        ];

        for case in cases {
            let result = BazelResolver::parse_packages(case.input);
            let expected: Vec<String> = case.expected.iter().map(|s| s.to_string()).collect();
            assert_eq!(result, expected, "case: {}", case.name);
        }
    }
}
