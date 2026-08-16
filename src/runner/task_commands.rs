use super::task_supervisor;
use super::{Runner, TaskRunIntent};
use tokio::sync::oneshot;

impl Runner {
    pub(in crate::runner) async fn handle_task_run_prepared(
        &mut self,
        name: &str,
        task_cfg: &crate::config::Task,
        intent: TaskRunIntent,
        result: Result<task_supervisor::TaskRunReport, String>,
    ) {
        if self.shutting_down {
            self.stop_late_task_start(name.to_string(), result).await;
            return;
        }
        match result {
            // A run that never spawned is narrated by the supervisor that
            // decided it, before it publishes the phase — otherwise the line
            // lands behind the dependent that the phase just unblocked.
            Ok(task_supervisor::TaskRunReport::PendingRun { .. })
            | Ok(task_supervisor::TaskRunReport::Skipped { .. })
            | Err(_) => {}
            Ok(task_supervisor::TaskRunReport::Running(wired)) => {
                let emitter = self.output_manager.clone_lifecycle_emitter();
                emitter.service_debug_event(name, &format!("process spawned (pid {})", wired.pgid));
                emitter.service_event(name, &format!("spawn {}", wired.rendered_cmdline));
                // An interactive task waits for a user on its PTY; say how
                // to reach it, loudly enough to act on.
                if task_cfg.interactive {
                    emitter.service_event(
                        name,
                        &format!("waiting for input — run 'don attach {name}'"),
                    );
                }
                // The supervisor holds the process; for runtime detail the
                // snapshot is the record, not a copy of one. It published the
                // pid alongside its phase before sending this report — this is
                // the read-side projection catching up.
                self.state.set_task_pid(name, Some(wired.pgid));
                self.begin_task_run(name, intent, Some("running..."));
            }
        }
    }

    /// Say that a scheduled run is under way.
    ///
    /// Only a *scheduled* run is announced: a background `don run` is not
    /// something the dependency sweep is waiting on. The phase itself is the
    /// supervisor's and was published before this report was sent.
    fn begin_task_run(&mut self, name: &str, intent: TaskRunIntent, running_message: Option<&str>) {
        if let (TaskRunIntent::Scheduled, Some(message)) = (intent, running_message) {
            self.output_manager.service_event(name, message);
        }
    }

    /// Ask a task's supervisor to end the run it is holding, if any.
    ///
    /// Returns the done-signal to join on. Teardown must: the supervisors are
    /// aborted right after, and aborting one that has not read this yet would
    /// leave its child unreaped.
    pub(in crate::runner) fn send_task_kill(&self, name: &str) -> Option<oneshot::Receiver<()>> {
        let handle = self.task_supervisors.registry().get(name)?;
        let (done_tx, done_rx) = oneshot::channel();
        handle
            .request(task_supervisor::TaskCommand::Kill {
                done: Some(done_tx),
            })
            .then_some(done_rx)
    }
}
