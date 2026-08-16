//! Narration for the two moments a task run is worth a line.
//!
//! The phases these accompany belong to the task's supervisor, which publishes
//! them before sending the report this reacts to. Nothing here decides
//! anything; what is left is the words, and answering a `don run --wait`.

use super::support::format_duration;
use super::{CommandError, Runner, TaskExit};

impl Runner {
    /// A task's supervisor picked up a triggered run and said what to call it.
    pub(in crate::runner) fn handle_task_starting(&mut self, name: &str, message: &str) {
        if self.shutting_down || !self.tasks.contains_key(name) {
            return;
        }
        self.output_manager.service_event(name, message);
        self.output_manager
            .service_debug_event(name, "spawning process...");
    }

    /// A task run ended.
    ///
    /// No currency check: the supervisor is the single producer of this task's
    /// messages and holds each run to its exit, so the Nth exit is the Nth
    /// run's. A run it killed on purpose reports nothing at all.
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
        // `last_run` rides along for the supervisor's own publication; the
        // projection reads it from there.
        drop(last_run);

        let timing = elapsed.map(format_duration).unwrap_or_default();
        if success {
            let msg = if timing.is_empty() {
                "complete".to_string()
            } else {
                format!("complete ({timing})")
            };
            self.output_manager.service_event(name, &msg);
        } else if let Some(ref err_msg) = message {
            let msg = if timing.is_empty() {
                format!("failed ({err_msg})")
            } else {
                format!("failed ({err_msg}, {timing})")
            };
            self.output_manager.service_error_event(name, &msg);
        }

        // The reply rode down with the run and back up on its exit report, so
        // answering it here means what every other command reply means: the
        // scheduler has applied this.
        if let Some(reply) = reply {
            let _ = reply.send(if success {
                Ok(())
            } else {
                Err(CommandError::Failed {
                    name: name.to_string(),
                    message: message.unwrap_or_else(|| "task failed".to_string()),
                })
            });
        }
    }
}
