//! Per-service and per-task runtime state holders.
//!
//! The `state` fields are deliberately **private to this submodule** so the
//! rest of the runner cannot bypass [`RuntimeService::set_state`] /
//! [`RuntimeTask::set_state`]. Those setters return `Option<…State>` marked
//! `#[must_use]`: forgetting to broadcast the resulting event is a clippy
//! error rather than a silent bug.
//!
//! Use the [`Runner::set_service_state`] and [`Runner::set_task_state`]
//! helpers in the parent module for the common case — they look up the
//! entry, call `set_state`, and broadcast the [`RunnerEvent`] in one step.
//!
//! [`Runner::set_service_state`]: super::Runner::set_service_state
//! [`Runner::set_task_state`]: super::Runner::set_task_state
//! [`RunnerEvent`]: super::RunnerEvent

use super::{AttachWaiter, ServiceHandle, ServiceState, TaskItemState};
use tokio::sync::oneshot;

/// All per-service runtime state, consolidated into a single struct.
///
/// Each running service gets one `RuntimeService` in `Runner::services`.
pub(crate) struct RuntimeService {
    /// Lifecycle state. Private so every mutation routes through
    /// [`set_state`](Self::set_state) and gets broadcast.
    state: ServiceState,
    /// The fully resolved service config (platform overrides applied once).
    pub resolved: crate::config::service::ResolvedService,
    /// Handle to the running process (if spawned).
    pub handle: Option<ServiceHandle>,
    /// OSC query sink for reclaiming PTY write on attach.
    pub osc_sink: Option<crate::output::OscSinkHandle>,
    /// PID of the client holding the interactive attach lock.
    pub attach_lock: Option<u32>,
    /// Pending attach waiter (client waiting for process to start).
    pub attach_waiter: Option<AttachWaiter>,
    /// TCP proxy listener — outlives restarts. Owns the bound public
    /// listeners for both env and listenfd mode entries.
    pub proxy: Option<crate::proxy::ServiceProxy>,
    /// Watch paths resolved from build tool queries (bazel/turbo).
    pub resolved_watch_paths: Vec<String>,
    /// Bazel binary path resolved via `bazel cquery --output=files`.
    pub bazel_binary_path: Option<String>,
    /// Whether this service was built during the batch build phase.
    pub batch_built: bool,
    /// Cancel channel for the per-service health monitor task. `Some` when
    /// the monitor is running; dropping it (or sending) signals the loop
    /// to exit. Cleared on stop, restart, or process exit.
    pub monitor_cancel: Option<oneshot::Sender<()>>,
    /// Number of consecutive `on_failure = "restart"` cycles we've
    /// triggered without the service recovering to Ready. Drives backoff
    /// for the next scheduled restart. Reset to 0 on Ready.
    pub restart_attempts: u32,
    /// Handle to a scheduled `RestartUnhealthy` command. Aborted on stop,
    /// recovery, or manual restart so we don't fire a stale auto-restart.
    pub pending_restart: Option<tokio::task::JoinHandle<()>>,
}

impl RuntimeService {
    pub(crate) fn new(
        resolved: crate::config::service::ResolvedService,
        initial_state: ServiceState,
    ) -> Self {
        Self {
            state: initial_state,
            resolved,
            handle: None,
            osc_sink: None,
            attach_lock: None,
            attach_waiter: None,
            proxy: None,
            resolved_watch_paths: Vec::new(),
            bazel_binary_path: None,
            batch_built: false,
            monitor_cancel: None,
            restart_attempts: 0,
            pending_restart: None,
        }
    }

    /// Stop any running health monitor and abort any pending auto-restart.
    /// Safe to call when neither is set. Used on stop/restart/process exit
    /// to make sure stale monitor traffic and stale auto-restart timers
    /// can't fire after the service is no longer in Ready/Unhealthy.
    pub(crate) fn stop_health_tracking(&mut self) {
        if let Some(tx) = self.monitor_cancel.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.pending_restart.take() {
            handle.abort();
        }
    }

    pub(crate) fn state(&self) -> ServiceState {
        self.state
    }

    /// Transition to `new_state`. Returns `Some(new_state)` when the state
    /// actually changed — the caller **must** broadcast a
    /// `RunnerEvent::ServiceStateChanged` for that value. Returns `None`
    /// when already at `new_state`.
    #[must_use = "state changes must be broadcast via RunnerEvent::ServiceStateChanged — use Runner::set_service_state or forward the returned state to event_tx.send"]
    pub(crate) fn set_state(&mut self, new_state: ServiceState) -> Option<ServiceState> {
        if self.state == new_state {
            return None;
        }
        self.state = new_state;
        Some(new_state)
    }
}

/// All per-task runtime state, consolidated into a single struct.
///
/// Each task gets one `RuntimeTask` in `Runner::tasks`.
pub(crate) struct RuntimeTask {
    /// Lifecycle state. Private so every mutation routes through
    /// [`set_state`](Self::set_state) and gets broadcast.
    state: TaskItemState,
    /// The task config (stored once, no repeated lookups).
    pub config: crate::config::task::Task,
    /// Process group ID of the running task (for shutdown kills).
    pub pgid: Option<i32>,
    /// OSC query sink for reclaiming PTY write on attach.
    pub osc_sink: Option<crate::output::OscSinkHandle>,
    /// PID of the client holding the interactive attach lock.
    pub attach_lock: Option<u32>,
    /// Pending attach waiter (client waiting for process to start).
    pub attach_waiter: Option<AttachWaiter>,
    /// Watch paths resolved from build tool queries (bazel/turbo).
    pub resolved_watch_paths: Vec<String>,
}

impl RuntimeTask {
    pub(crate) fn new(
        config: crate::config::task::Task,
        initial_state: TaskItemState,
    ) -> Self {
        Self {
            state: initial_state,
            config,
            pgid: None,
            osc_sink: None,
            attach_lock: None,
            attach_waiter: None,
            resolved_watch_paths: Vec::new(),
        }
    }

    pub(crate) fn state(&self) -> TaskItemState {
        self.state
    }

    /// Transition to `new_state`. Returns `Some(new_state)` when the state
    /// actually changed — the caller **must** broadcast a
    /// `RunnerEvent::TaskStateChanged` for that value.
    #[must_use = "state changes must be broadcast via RunnerEvent::TaskStateChanged — use Runner::set_task_state or forward the returned state to event_tx.send"]
    pub(crate) fn set_state(&mut self, new_state: TaskItemState) -> Option<TaskItemState> {
        if self.state == new_state {
            return None;
        }
        self.state = new_state;
        Some(new_state)
    }
}
