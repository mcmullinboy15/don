//! Per-service and per-task config holders.
//!
//! Neither keeps a phase. A process's phase, its custody, whether it has a
//! retry armed, and whether it satisfies its dependents all belong to its
//! supervisor, which publishes them (see [`crate::facts`]) — so there is no
//! shadow of any of them to drift.
//!
//! What is left is the config the root resolved once at construction, and the
//! two values it read out of `.don/task-state` to seed a task's supervisor.

/// All per-service runtime state, consolidated into a single struct.
///
/// Each running service gets one `RuntimeService` in `Runner::services`.
pub(crate) struct RuntimeService {
    /// The fully resolved service config (platform overrides applied once).
    pub resolved: crate::config::service::ResolvedService,
}

impl RuntimeService {
    pub(crate) fn new(resolved: crate::config::service::ResolvedService) -> Self {
        Self { resolved }
    }
}

/// All per-task runtime state, consolidated into a single struct.
///
/// Each task gets one `RuntimeTask` in `Runner::tasks`.
pub(crate) struct RuntimeTask {
    /// The task config (stored once, no repeated lookups).
    pub config: crate::config::task::Task,
    /// Whether the task has ever completed successfully, read from
    /// `.don/task-state` at construction and handed to its supervisor, which
    /// owns it from then on.
    pub has_success: bool,
    /// Metadata for the most recent run, likewise read once and handed on.
    pub last_run: Option<crate::task_state::TaskRunInfo>,
}

impl RuntimeTask {
    pub(crate) fn new(
        config: crate::config::task::Task,
        has_success: bool,
        last_run: Option<crate::task_state::TaskRunInfo>,
    ) -> Self {
        Self {
            config,
            has_success,
            last_run,
        }
    }
}
