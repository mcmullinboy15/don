//! Build tool integration for resolving watch paths and dependencies.
//!
//! Don can query Bazel to automatically determine which source directories
//! to watch for a given build target.
//! This eliminates the need to manually maintain `watch` patterns as the
//! build graph evolves.
//!
//! The integration uses a two-tier watch strategy:
//! - **Tier 1**: Watch build graph definition files (BUILD, package.json, etc.).
//!   Changes to these files trigger a re-query of the build tool.
//! - **Tier 2**: Watch resolved source directories. These are the directories
//!   the build tool reports as inputs for a given target.

pub(crate) mod bazel;
pub(crate) mod manager;

/// Errors from build tool integration.
#[derive(Debug, thiserror::Error)]
pub enum BuildToolError {
    /// The build tool binary is not installed or not on PATH.
    #[error("{tool} not found on PATH — install it or remove the build tool config from don.toml")]
    NotInstalled { tool: String },
    /// The build tool query command exited with a non-zero status.
    #[error("{tool} query failed: {message}")]
    QueryFailed { tool: String, message: String },
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
pub(crate) struct ResolvedBuildInfo {
    /// Directories/glob patterns to watch for source changes (tier 2).
    /// These are relative to the service/task's working directory.
    pub watch_paths: Vec<String>,
    /// Glob patterns for build graph definition files (tier 1).
    /// Changes to these files should trigger a re-query of the build tool.
    pub graph_definition_globs: Vec<String>,
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

/// Owns a [`tokio::task::JoinHandle`] and aborts it when dropped, unless
/// the handle was explicitly extracted via [`Self::into_inner`].
///
/// We use this to make the stderr/stdout streaming tasks spawned inside
/// `build_targets` cancellable: when the parent future
/// is dropped (e.g. shutdown mid-build), the streaming tasks must stop
/// reading and release any senders they hold (typically a
/// [`crate::output::LifecycleEmitter`] cloned for the on-line callback).
/// Without that, the stdout sink channel never closes, and
/// `OutputManager::shutdown` blocks forever waiting on its writer task.
///
/// The orphaned bazel build *action* processes inherit the child's
/// stdout/stderr fds and can hold them open long after we SIGKILL the
/// bazel client, so "EOF on the pipe" is not a reliable wakeup.
pub(crate) struct AbortOnDrop<T> {
    handle: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortOnDrop<T> {
    pub(crate) fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// Take the inner handle out, suppressing the abort-on-drop. Use when
    /// the streaming task is expected to finish naturally (the success
    /// path, after `child.wait()` returns and the pipes close on their own).
    ///
    /// Returns `None` if the handle has already been taken — this never
    /// happens in practice because the type is constructed with `Some` and
    /// `take` is only called by this method, which consumes `self`.
    pub(crate) fn into_inner(mut self) -> Option<tokio::task::JoinHandle<T>> {
        self.handle.take()
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
