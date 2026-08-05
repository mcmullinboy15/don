//! The parts of a task run that belong to the task, not to the runner.
//!
//! A task run is a pipeline: prepare (resolve params, hash inputs, decide
//! whether to run at all) → spawn → wire output → wait for exit → record the
//! outcome. Today the runner drives that pipeline, which is why it needs
//! `run_generation`: each stage hands off through a detached task, completions
//! from every task land on one shared channel, and the runner cannot otherwise
//! tell a current completion from a superseded one.
//!
//! This module is where that pipeline moves. It starts with the exit half —
//! the stage that has no runner state to touch at all — as free functions
//! taking owned inputs, so they can run anywhere. What is left in
//! `task_commands` after each piece moves is the part only the runner may do:
//! transition item state, which drives the cross-item dependency scheduler.

use super::paths::{resolve_watch_ignore_patterns, working_dir_for};
use super::{ItemDone, NodeKind, RunnerInternalCommand, TaskExit};
use crate::task_state::{TaskRunInfo, TaskState};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;

/// Everything the exit half of a task run needs, owned outright.
///
/// Owned rather than borrowed because this outlives the runner's command loop
/// — the exit wait is a detached task, and holding a reference into runner
/// state across it is what the whole decomposition is trying to stop.
pub(in crate::runner) struct TaskRunOutcome {
    pub(in crate::runner) name: String,
    pub(in crate::runner) task_cfg: crate::config::Task,
    pub(in crate::runner) base_dir: PathBuf,
    pub(in crate::runner) global_watch_ignore: Vec<String>,
    /// Process group of the run that just ended.
    pub(in crate::runner) pgid: i32,
    /// `Some` for a startup-scheduled run — the dependency scheduler is
    /// waiting on it. `None` for a watch rerun or a background `don run`,
    /// which report through the runner's internal channel instead.
    pub(in crate::runner) done_tx: Option<mpsc::Sender<ItemDone>>,
    pub(in crate::runner) internal_tx: mpsc::Sender<RunnerInternalCommand>,
    /// Whether this run was triggered by a file watch, which decides if a
    /// `TaskRerunComplete` event is broadcast when it lands.
    pub(in crate::runner) rerun: bool,
}

impl TaskRunOutcome {
    /// Record a finished run and send exactly one completion message.
    ///
    /// Both the background and foreground wait paths end here, so the rules
    /// for what counts as success, what gets persisted, and who is told stay
    /// in one place — they were duplicated verbatim before, which is how they
    /// drift.
    ///
    /// A successful run records its watched inputs alongside the run info, so
    /// the next startup can skip it when nothing changed; a failed one records
    /// only the run info, leaving the previous input hashes stale on purpose
    /// so the task is not skipped next time.
    pub(in crate::runner) async fn finish(
        self,
        result: Result<std::process::ExitStatus, super::task::TaskError>,
        elapsed: Duration,
    ) {
        let (success, exit_code, message) = match result {
            Ok(status) if status.success() => (true, status.code(), None),
            Ok(status) => {
                let code = status.code().unwrap_or(-1);
                (false, status.code(), Some(format!("exit code {code}")))
            }
            Err(e) => (false, None, Some(e.to_string())),
        };
        let last_run =
            TaskRunInfo::finished_now(success, Some(elapsed), exit_code, message.clone());

        let task_state = TaskState::new(self.base_dir.join(".don").join("task-state"));
        if success {
            let task_dir = working_dir_for(&self.base_dir, self.task_cfg.dir.as_deref());
            let ignore_patterns = resolve_watch_ignore_patterns(
                &task_dir,
                &self.task_cfg.ignore,
                &self.base_dir,
                &self.global_watch_ignore,
            );
            let _ = task_state
                .record_success_with_info(
                    &self.name,
                    &self.task_cfg.watch,
                    &ignore_patterns,
                    Some(&task_dir),
                    &last_run,
                )
                .await;
        } else {
            let _ = task_state.record_run(&self.name, &last_run).await;
        }

        match self.done_tx {
            Some(done_tx) => {
                let _ = done_tx
                    .send(ItemDone {
                        name: self.name,
                        kind: NodeKind::Task,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        last_run: Some(last_run),
                        service_start_generation: None,
                        task_run_generation: None,
                    })
                    .await;
            }
            None => {
                let _ = self
                    .internal_tx
                    .send(RunnerInternalCommand::TaskExited(TaskExit {
                        name: self.name,
                        pgid: self.pgid,
                        success,
                        message,
                        elapsed: Some(elapsed),
                        last_run: Some(last_run),
                        rerun: self.rerun,
                    }))
                    .await;
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    /// A real parsed task, so the defaults here are the product's defaults.
    fn test_task() -> crate::config::Task {
        let config: crate::config::Config = "[tasks.build]\ncmd = \"true\"\n".parse().unwrap();
        config.tasks.get("build").unwrap().clone()
    }

    fn outcome(
        name: &str,
        base_dir: &std::path::Path,
        done_tx: Option<mpsc::Sender<ItemDone>>,
        internal_tx: mpsc::Sender<RunnerInternalCommand>,
        rerun: bool,
    ) -> TaskRunOutcome {
        TaskRunOutcome {
            name: name.to_string(),
            task_cfg: test_task(),
            base_dir: base_dir.to_path_buf(),
            global_watch_ignore: Vec::new(),
            pgid: 4242,
            done_tx,
            internal_tx,
            rerun,
        }
    }

    /// A scheduled run answers the dependency scheduler; anything else
    /// reports through the runner's internal channel. Exactly one of the two,
    /// never both — a startup task that also emitted `TaskExited` would be
    /// applied twice.
    #[tokio::test]
    async fn a_finished_run_reports_to_exactly_one_place() {
        struct Case {
            name: &'static str,
            scheduled: bool,
            status: std::process::ExitStatus,
            want_success: bool,
            want_message: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "scheduled success",
                scheduled: true,
                status: ExitStatusExt::from_raw(0),
                want_success: true,
                want_message: None,
            },
            Case {
                name: "scheduled failure carries the exit code",
                scheduled: true,
                status: ExitStatusExt::from_raw(3 << 8),
                want_success: false,
                want_message: Some("exit code 3"),
            },
            Case {
                name: "rerun success",
                scheduled: false,
                status: ExitStatusExt::from_raw(0),
                want_success: true,
                want_message: None,
            },
            Case {
                name: "rerun failure",
                scheduled: false,
                status: ExitStatusExt::from_raw(1 << 8),
                want_success: false,
                want_message: Some("exit code 1"),
            },
        ];

        for case in cases {
            let temp = tempfile::tempdir().unwrap();
            let (done_tx, mut done_rx) = mpsc::channel(4);
            let (internal_tx, mut internal_rx) = mpsc::channel(4);
            let scheduled = case.scheduled.then_some(done_tx);

            outcome(
                "build",
                temp.path(),
                scheduled,
                internal_tx,
                !case.scheduled,
            )
            .finish(Ok(case.status), Duration::from_millis(5))
            .await;

            if case.scheduled {
                let done = done_rx.try_recv().expect("scheduled run answers done_tx");
                assert_eq!(done.name, "build", "{}", case.name);
                assert_eq!(done.success, case.want_success, "{}", case.name);
                assert_eq!(done.message.as_deref(), case.want_message, "{}", case.name);
                assert!(
                    internal_rx.try_recv().is_err(),
                    "{}: must not also emit TaskExited",
                    case.name
                );
            } else {
                let Ok(RunnerInternalCommand::TaskExited(exit)) = internal_rx.try_recv() else {
                    panic!("{}: expected a TaskExited", case.name);
                };
                assert_eq!(exit.name, "build", "{}", case.name);
                assert_eq!(exit.pgid, 4242, "{}", case.name);
                assert_eq!(exit.success, case.want_success, "{}", case.name);
                assert_eq!(exit.message.as_deref(), case.want_message, "{}", case.name);
                assert!(exit.rerun, "{}", case.name);
                assert!(
                    done_rx.try_recv().is_err(),
                    "{}: must not also answer done_tx",
                    case.name
                );
            }
        }
    }

    /// Only a successful run records its input hashes. Recording them on
    /// failure would let the next startup skip a task that never worked.
    #[tokio::test]
    async fn only_success_records_watched_inputs() {
        for (label, status, want_success) in [
            ("success", ExitStatusExt::from_raw(0), true),
            ("failure", ExitStatusExt::from_raw(1 << 8), false),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let (internal_tx, _internal_rx) = mpsc::channel(4);
            outcome("build", temp.path(), None, internal_tx, false)
                .finish(Ok(status), Duration::from_millis(1))
                .await;

            let state = TaskState::new(temp.path().join(".don").join("task-state"));
            assert_eq!(
                state.has_success("build").await.unwrap(),
                want_success,
                "{label}: has_success"
            );
            assert!(
                state.last_run("build").await.unwrap().is_some(),
                "{label}: every run records its outcome"
            );
        }
    }
}
