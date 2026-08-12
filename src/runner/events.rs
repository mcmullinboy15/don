use super::support::format_duration;
use super::{CommandError, CommandResult, Runner, TaskExit, TaskState};

impl Runner {
    /// A task's supervisor picked up a triggered run.
    ///
    /// The task-side twin of `handle_service_starting`: the supervisor says
    /// when, and what to call it; the state transition is the scheduler's
    /// because it wakes the cross-process dependency sweep.
    pub(in crate::runner) fn handle_task_starting(&mut self, name: &str, message: &str) {
        if self.shutting_down || !self.tasks.contains_key(name) {
            return;
        }
        if let Some(rt) = self.tasks.get_mut(name) {
            rt.set_needs_run_now(true);
        }
        self.output_manager.service_event(name, message);
        self.set_task_state(name, TaskState::Running);
        self.output_manager
            .service_debug_event(name, "spawning process...");
    }

    pub(in crate::runner) fn handle_task_exit(&mut self, exit: TaskExit) {
        let TaskExit {
            name,
            success,
            message,
            elapsed,
            last_run,
            reply,
        } = exit;
        let name = name.as_str();
        if !self.tasks.contains_key(name) {
            return;
        }
        // No currency check: the supervisor is the single producer of this
        // task's messages and holds each run to its exit, so the Nth exit is
        // the Nth run's. A run it killed on purpose reports nothing at all.
        self.state.set_task_pid(name, None);

        let timing = elapsed.map(format_duration).unwrap_or_default();
        let wait_result: CommandResult = if success {
            Ok(())
        } else {
            Err(CommandError::Failed {
                name: name.to_string(),
                message: message.clone().unwrap_or_else(|| "task failed".to_string()),
            })
        };
        if success {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.mark_success();
                rt.last_run = last_run;
            }
            self.set_task_state(name, TaskState::Completed);
            let msg = if timing.is_empty() {
                "complete".to_string()
            } else {
                format!("complete ({timing})")
            };
            self.output_manager.service_event(name, &msg);
        } else {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.set_needs_run_now(true);
                rt.last_run = last_run;
            }
            self.set_task_state(name, TaskState::Failed);
            if let Some(ref err_msg) = message {
                let msg = if timing.is_empty() {
                    format!("failed ({err_msg})")
                } else {
                    format!("failed ({err_msg}, {timing})")
                };
                self.output_manager.service_error_event(name, &msg);
            }
        }

        // The reply rode down with the run and back up on its exit report, so
        // answering it here means what every other command reply means: the
        // scheduler has applied this.
        if let Some(reply) = reply {
            let _ = reply.send(wait_result);
        }
    }
}
