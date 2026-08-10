use super::support::format_duration;
use super::{CommandError, CommandResult, Runner, RunnerEvent, TaskExit, TaskState};

impl Runner {
    pub(in crate::runner) fn handle_task_exit(&mut self, exit: TaskExit) {
        let TaskExit {
            name,
            pgid,
            success,
            message,
            elapsed,
            last_run,
            rerun,
            reply,
        } = exit;
        let name = name.as_str();
        if self.tasks.get(name).is_none_or(|rt| rt.pgid != Some(pgid)) {
            return;
        }

        if let Some(rt) = self.tasks.get_mut(name) {
            rt.pgid = None;
        }
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

        if rerun {
            let _ = self.event_tx.send(RunnerEvent::TaskRerunComplete {
                name: name.to_string(),
                success,
            });
        }
        // The reply rode down with the run and back up on its exit report, so
        // answering it here means what every other command reply means: the
        // scheduler has applied this.
        if let Some(reply) = reply {
            let _ = reply.send(wait_result);
        }
    }
}
