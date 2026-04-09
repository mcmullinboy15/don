//! Build tool integration for resolving watch paths and dependencies.
//!
//! Don can query external build tools (Bazel, Turborepo) to automatically
//! determine which source directories to watch for a given build target.
//! This eliminates the need to manually maintain `watch` patterns as the
//! build graph evolves.
//!
//! The integration uses a two-tier watch strategy:
//! - **Tier 1**: Watch build graph definition files (BUILD, package.json, etc.).
//!   Changes to these files trigger a re-query of the build tool.
//! - **Tier 2**: Watch resolved source directories. These are the directories
//!   the build tool reports as inputs for a given target.

pub(crate) mod bazel;
pub(crate) mod turbo;

use std::path::Path;

/// Errors from build tool integration.
#[derive(Debug, thiserror::Error)]
pub enum BuildToolError {
    /// The build tool binary is not installed or not on PATH.
    #[error("{tool} not found on PATH — install it or remove the build tool config from don.toml")]
    NotInstalled { tool: String },
    /// The build tool query command exited with a non-zero status.
    #[error("{tool} query failed: {message}")]
    QueryFailed { tool: String, message: String },
    /// The build tool query took too long.
    #[error("{tool} query timed out after {timeout_secs}s")]
    QueryTimeout { tool: String, timeout_secs: u64 },
    /// The build tool returned output that could not be parsed.
    #[error("{tool} returned unparseable output: {message}")]
    ParseError { tool: String, message: String },
    /// An I/O error occurred while running the build tool.
    #[error("io error running {tool}: {source}")]
    Io {
        tool: String,
        #[source]
        source: std::io::Error,
    },
}

/// Resolved information from a build tool query.
///
/// Contains the watch paths (tier 2) and graph definition file patterns (tier 1)
/// that the watch module uses to set up file watching.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields are part of the resolver contract; some are used by future consumers.
pub(crate) struct ResolvedBuildInfo {
    /// Directories/glob patterns to watch for source changes (tier 2).
    /// These are relative to the service/task's working directory.
    pub watch_paths: Vec<String>,
    /// Names of other targets this target depends on (informational).
    pub dependencies: Vec<String>,
    /// Glob patterns for build graph definition files (tier 1).
    /// Changes to these files should trigger a re-query of the build tool.
    pub graph_definition_globs: Vec<String>,
}

/// Trait for querying build tools to resolve watch paths and dependencies.
///
/// Implementations shell out to the build tool CLI and parse the structured
/// output to determine which source directories feed into a given target.
pub(crate) trait BuildGraphResolver: Send + Sync {
    /// Resolve watch paths and dependencies for a build target.
    ///
    /// `target` is the build-tool-specific target string (e.g. `"//services/api:api"`
    /// for Bazel or `"dev"` for Turborepo).
    ///
    /// `working_dir` is the absolute path to the directory where the build tool
    /// should be invoked (typically the service's `dir` or the project root).
    fn resolve(
        &self,
        target: &str,
        working_dir: &Path,
    ) -> impl std::future::Future<Output = Result<ResolvedBuildInfo, BuildToolError>> + Send;

    /// Name of the build tool (for error messages and logging).
    #[allow(dead_code)]
    fn tool_name(&self) -> &'static str;
}

/// Result of a batch build operation.
///
/// Reports which targets succeeded and which failed, allowing the runner
/// to start services whose builds succeeded while marking failures.
pub(crate) struct BatchBuildResult {
    /// Targets that built successfully.
    pub succeeded: Vec<String>,
    /// Targets that failed, with error messages.
    pub failed: Vec<(String, String)>,
}
