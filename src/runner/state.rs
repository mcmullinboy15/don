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

use super::{
    CommandResult, ServiceHandleIdentity, ServiceState, ServiceStopAction, TaskRunWaiter, TaskState,
};
use crate::config::TaskAutoRun;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::oneshot;

/// All per-service runtime state, consolidated into a single struct.
///
/// Each running service gets one `RuntimeService` in `Runner::services`.
pub(crate) struct RuntimeService {
    /// Lifecycle state. Private so every mutation routes through
    /// [`set_state`](Self::set_state) and gets broadcast.
    state: ServiceState,
    /// Direct dependencies that caused the current `DependencyFailed` state.
    /// Cleared on every transition out of that state.
    failed_dependencies: Vec<String>,
    /// The fully resolved service config (platform overrides applied once).
    pub resolved: crate::config::service::ResolvedService,
    /// Last authoritative Docker host-port bindings. Retained while the
    /// container is stopped so a restart can request the same fallback ports.
    pub docker_port_bindings: Vec<crate::docker::DockerPortBinding>,
    /// What kind of process the service's supervisor currently holds, if
    /// any — the runner-side shadow of custody, for liveness gates and
    /// docker-ness checks now that the handle itself lives with the
    /// supervisor. Set at wire, cleared wherever the handle used to be.
    pub handle_identity: Option<ServiceHandleIdentity>,
    /// Process group ID for the current local process. This is equal to the
    /// child PID for services spawned by don. Docker services do not expose a
    /// local PID here.
    pub pgid: Option<i32>,
    /// Output reader task for the current process. It must drain before
    /// output sinks are torn down or final shutdown logs can be lost.
    pub output_worker: Option<tokio::task::JoinHandle<()>>,
    /// OSC query sink for reclaiming PTY write on attach.
    pub osc_sink: Option<crate::output::OscSinkHandle>,
    /// Whether the current start was asked for by the dependency sweep —
    /// its ready outcome then drives the sweep-visible transition and the
    /// "ready" lifecycle line; manual/rebuild starts instead close the
    /// watch cycle with `RebuildComplete`.
    pub(in crate::runner) scheduled_start: bool,
    /// The runner's read-only view of the supervisor-owned proxy: binding
    /// metadata for status, port references and the ports manifest, plus the
    /// policy and backend-env shadows the runner refreshes itself.
    pub proxy_view: Option<crate::proxy::ProxyView>,
    /// Watch paths resolved from build tool queries (bazel).
    pub resolved_watch_paths: Vec<String>,
    /// Bazel binary path resolved via `bazel cquery --output=files`.
    pub bazel_binary_path: Option<String>,
    /// Whether this service was built during the batch build phase.
    pub batch_built: bool,
    /// Cancel channel for the per-service health monitor task. `Some` when
    /// the monitor is running; dropping it (or sending) signals the loop
    /// to exit. Cleared on stop, restart, or process exit.
    /// Number of consecutive `on_failure = "restart"` cycles we've
    /// triggered without the service recovering to Ready. Drives backoff
    /// for the next scheduled restart. Reset to 0 on Ready.
    pub restart_attempts: u32,
    /// When the current process was last spawned. Used to detect a crash
    /// loop: a process that exits within a few seconds of starting is
    /// likely failing on launch rather than after doing useful work.
    pub last_start: Option<Instant>,
    /// Number of consecutive crashes where the process died within the
    /// rapid-crash window of being started. A hard cap on this count makes
    /// don give up auto-restarting regardless of `on_failure`. Reset
    /// whenever the service recovers, is stopped, or runs long enough to
    /// clear the streak.
    pub rapid_crashes: u32,
    /// Handle to a scheduled `RestartUnhealthy` command. Aborted on stop,
    /// recovery, or manual restart so we don't fire a stale auto-restart.
    pub pending_restart: Option<tokio::task::JoinHandle<()>>,
    /// In-flight manual stop/restart worker, if any.
    pub control_worker: Option<tokio::task::JoinHandle<()>>,
    /// Monotonic generation for manual control workers so stale completions
    /// can be ignored.
    pub control_generation: u64,
    /// Pending reply for a manual stop/restart request.
    pub control_reply: Option<oneshot::Sender<CommandResult>>,
    /// Follow-up action to run after the current stop completes.
    pub stop_action: ServiceStopAction,
    /// Token for the in-flight lazy JIT build, so a stale build completion
    /// (superseded by a newer connection-triggered build) is ignored. Only
    /// the lazy build path uses this — every other completion is ordered by
    /// construction on the report channel.
    pub lazy_build_token: u64,
    /// In-flight rebuild-preparation worker (build only) for this service.
    pub rebuild_worker: Option<tokio::task::JoinHandle<()>>,
    /// Monotonic generation for rebuild workers so stale completions can
    /// be ignored.
    pub rebuild_generation: u64,
    /// A watched file changed during the current rebuild cycle, so any
    /// pending restart for that cycle should be skipped.
    pub rebuild_stale: bool,
    /// A build completed successfully but its restart was skipped because the
    /// process went stale (a watched file changed mid-build), so the running
    /// process is now *behind* the latest built artifact. The follow-up
    /// rebuild cycle must restart even if the build tool reports "up to date" —
    /// up-to-date is measured against the last *build*, not against the
    /// *running process*. Cleared once the process is (re)started.
    pub artifact_ahead_of_process: bool,
}

impl RuntimeService {
    pub(crate) fn new(
        resolved: crate::config::service::ResolvedService,
        initial_state: ServiceState,
    ) -> Self {
        Self {
            state: initial_state,
            failed_dependencies: Vec::new(),
            resolved,

            docker_port_bindings: Vec::new(),
            handle_identity: None,
            pgid: None,
            output_worker: None,
            osc_sink: None,
            scheduled_start: false,
            proxy_view: None,
            resolved_watch_paths: Vec::new(),
            bazel_binary_path: None,
            batch_built: false,
            restart_attempts: 0,
            last_start: None,
            rapid_crashes: 0,
            pending_restart: None,
            control_worker: None,
            control_generation: 0,
            control_reply: None,
            stop_action: ServiceStopAction::None,
            lazy_build_token: 0,
            rebuild_worker: None,
            rebuild_generation: 0,
            rebuild_stale: false,
            artifact_ahead_of_process: false,
        }
    }

    /// Stop any running health monitor and abort any pending auto-restart.
    /// Safe to call when neither is set. Used on stop/restart/process exit
    /// to make sure stale monitor traffic and stale auto-restart timers
    /// can't fire after the service is no longer in Ready/Unhealthy.
    pub(crate) fn stop_health_tracking(&mut self) {
        // Monitor cancellation moved to the supervisor (dropped on Stop or
        // process death); this now only clears the runner-side restart
        // timer.
        if let Some(handle) = self.pending_restart.take() {
            handle.abort();
        }
    }

    /// Reset the auto-restart failure-streak counters. Called when the
    /// service reaches a healthy state, exits cleanly, or is stopped /
    /// restarted by the user — any of which means the prior run of failures
    /// no longer counts toward the give-up thresholds.
    pub(crate) fn reset_restart_tracking(&mut self) {
        self.restart_attempts = 0;
        self.rapid_crashes = 0;
    }

    pub(crate) fn state(&self) -> ServiceState {
        self.state
    }

    pub(crate) fn failed_dependencies(&self) -> &[String] {
        &self.failed_dependencies
    }

    /// Atomically enter `DependencyFailed` with the supplied root causes.
    pub(crate) fn mark_dependency_failed(&mut self, dependencies: Vec<String>) -> bool {
        if self.state == ServiceState::DependencyFailed && self.failed_dependencies == dependencies
        {
            return false;
        }
        self.state = ServiceState::DependencyFailed;
        self.failed_dependencies = dependencies;
        true
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
        if new_state != ServiceState::DependencyFailed {
            self.failed_dependencies.clear();
        }
        Some(new_state)
    }
}

/// All per-task runtime state, consolidated into a single struct.
///
/// Each task gets one `RuntimeTask` in `Runner::tasks`.
pub(crate) struct RuntimeTask {
    /// Lifecycle state. Private so every mutation routes through
    /// [`set_state`](Self::set_state) and gets broadcast.
    state: TaskState,
    /// Direct dependencies that caused the current `DependencyFailed` state.
    /// Cleared on every transition out of that state.
    failed_dependencies: Vec<String>,
    /// The task config (stored once, no repeated lookups).
    pub config: crate::config::task::Task,
    /// Params used for the most recent spawned run. Empty for param-less
    /// tasks and startup/watch-triggered runs.
    pub last_params: HashMap<String, String>,
    /// Process group ID of the running task (for shutdown kills).
    pub pgid: Option<i32>,
    /// A run request is with the supervisor and its prepared result has not
    /// come back yet. Covers the window the supervisor's `is_busy` cannot:
    /// after it emits `TaskRunPrepared` (busy drops) but before the runner
    /// wires the run (state becomes `Running`). Without this, a scheduler
    /// sweep landing in that window re-requests the task and double-spawns.
    pub run_requested: bool,
    /// Watch paths resolved from build tool queries (bazel).
    pub resolved_watch_paths: Vec<String>,
    /// Monotonic token for `don run --wait` waiters, bumped per waiter
    /// registration so a superseded waiter's timeout cannot answer its
    /// replacement. Exit completions need no token: a present waiter always
    /// belongs to the in-flight run (concurrent runs are rejected, and
    /// supersession replaces the waiter explicitly).
    pub waiter_token: u64,
    /// Pending reply for a manually-triggered `don run --wait` request.
    pub run_waiter: Option<TaskRunWaiter>,
    /// Whether the task has ever completed successfully.
    pub has_success: bool,
    /// Metadata for the most recent process run, including failures.
    pub last_run: Option<crate::task_state::TaskRunInfo>,
    /// Whether the runner has finished the initial startup dependency-gate
    /// evaluation for this task.
    pub dependency_evaluated: bool,
    /// Whether the task currently needs another run to bring its watched
    /// inputs up to date.
    pub needs_run_now: bool,
}

impl RuntimeTask {
    pub(crate) fn new(
        config: crate::config::task::Task,
        initial_state: TaskState,
        has_success: bool,
        last_run: Option<crate::task_state::TaskRunInfo>,
    ) -> Self {
        Self {
            state: initial_state,
            failed_dependencies: Vec::new(),
            config,
            last_params: HashMap::new(),
            pgid: None,
            resolved_watch_paths: Vec::new(),
            waiter_token: 0,
            run_requested: false,
            run_waiter: None,
            has_success,
            last_run,
            dependency_evaluated: false,
            needs_run_now: false,
        }
    }

    pub(crate) fn state(&self) -> TaskState {
        self.state
    }

    pub(crate) fn failed_dependencies(&self) -> &[String] {
        &self.failed_dependencies
    }

    /// Atomically enter `DependencyFailed` with the supplied root causes.
    pub(crate) fn mark_dependency_failed(&mut self, dependencies: Vec<String>) -> bool {
        if self.state == TaskState::DependencyFailed && self.failed_dependencies == dependencies {
            return false;
        }
        self.state = TaskState::DependencyFailed;
        self.failed_dependencies = dependencies;
        true
    }

    /// Transition to `new_state`. Returns `Some(new_state)` when the state
    /// actually changed — the caller **must** broadcast a
    /// `RunnerEvent::TaskStateChanged` for that value.
    #[must_use = "state changes must be broadcast via RunnerEvent::TaskStateChanged — use Runner::set_task_state or forward the returned state to event_tx.send"]
    pub(crate) fn set_state(&mut self, new_state: TaskState) -> Option<TaskState> {
        if self.state == new_state {
            return None;
        }
        self.state = new_state;
        if new_state != TaskState::DependencyFailed {
            self.failed_dependencies.clear();
        }
        Some(new_state)
    }

    pub(crate) fn set_needs_run_now(&mut self, needs_run_now: bool) {
        self.dependency_evaluated = true;
        self.needs_run_now = needs_run_now;
    }

    pub(crate) fn mark_success(&mut self) {
        self.has_success = true;
        self.dependency_evaluated = true;
        self.needs_run_now = false;
    }

    pub(crate) fn dependency_satisfied(&self) -> bool {
        if matches!(self.state, TaskState::DependencyFailed) {
            return false;
        }
        if !self.dependency_evaluated && self.state == TaskState::Pending {
            return false;
        }
        if !self.has_success {
            return false;
        }
        if self.needs_run_now
            && self.config.params.is_empty()
            && matches!(self.config.auto_run, TaskAutoRun::Always)
        {
            return false;
        }
        true
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{RuntimeTask, TaskState};

    /// The dependency gate reads `needs_run_now`: a task with a successful
    /// history but an outstanding run is *not* satisfied, so dependents wait
    /// rather than starting against stale output. This is the runner half of
    /// the background-failure bug the task supervisor's `NoSpawnOutcome`
    /// classifier tests pin from the process side.
    #[test]
    fn an_outstanding_run_blocks_dependents_despite_a_successful_history() {
        let config: crate::config::Config = "[tasks.build]\ncmd = \"true\"\n".parse().unwrap();
        let task = config.tasks.get("build").unwrap().clone();
        let mut rt = RuntimeTask::new(task, TaskState::Completed, true, None);
        assert!(rt.dependency_satisfied(), "a completed task satisfies deps");
        rt.set_needs_run_now(true);
        assert!(
            !rt.dependency_satisfied(),
            "an outstanding run must block dependents"
        );
    }
}
