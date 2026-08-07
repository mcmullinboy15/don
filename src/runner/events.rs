use super::support::format_duration;
use super::{CommandError, CommandResult, NodeKind, Runner, RunnerEvent, TaskItemState};

/// Completion notification from a spawned item.
pub(in crate::runner) struct ItemDone {
    pub(in crate::runner) name: String,
    pub(in crate::runner) kind: NodeKind,
    pub(in crate::runner) success: bool,
    pub(in crate::runner) message: Option<String>,
    /// How long the item took (for tasks).
    pub(in crate::runner) elapsed: Option<std::time::Duration>,
    /// Metadata for a task process that actually ran.
    pub(in crate::runner) last_run: Option<crate::task_state::TaskRunInfo>,
    /// Run generation for manually-triggered task completions that need to
    /// re-notify startup dependency resolution. `None` for normal startup
    /// item completions.
    pub(in crate::runner) task_run_generation: Option<u64>,
}

#[derive(Debug)]
pub(in crate::runner) struct TaskExit {
    pub(in crate::runner) name: String,
    pub(in crate::runner) pgid: i32,
    pub(in crate::runner) success: bool,
    pub(in crate::runner) message: Option<String>,
    pub(in crate::runner) elapsed: Option<std::time::Duration>,
    pub(in crate::runner) last_run: Option<crate::task_state::TaskRunInfo>,
    pub(in crate::runner) rerun: bool,
}

impl Runner {
    /// Handle an item completion notification. Services no longer produce
    /// these — their ready outcomes fold directly from the report channel.
    pub(in crate::runner) fn handle_item_done(&mut self, item: &ItemDone) {
        match item.kind {
            NodeKind::Service => {}
            NodeKind::Task => self.handle_task_done(item),
        }
    }

    fn handle_task_done(&mut self, item: &ItemDone) {
        if let Some(task_generation) = item.task_run_generation
            && self
                .tasks
                .get(&item.name)
                .is_some_and(|rt| rt.run_generation != task_generation)
        {
            return;
        }
        if let Some(rt) = self.tasks.get_mut(&item.name)
            && rt.pgid.take().is_some()
        {
            // Reset attach bookkeeping.
            rt.attach_count = 0;
            rt.pty_input = None;
            // Can't await here (sync fn), but the stdout sink resume
            // will happen naturally when the follow sink closes.
        }
        let timing = item.elapsed.map(format_duration).unwrap_or_default();

        if item.success {
            let cur = self.tasks.get(&item.name).map(|rt| rt.state());
            if cur != Some(TaskItemState::Skipped)
                && cur != Some(TaskItemState::PendingRun)
                && let Some(rt) = self.tasks.get_mut(&item.name)
            {
                rt.mark_success();
                rt.last_run = item.last_run.clone();
            }
            if cur != Some(TaskItemState::Skipped)
                && cur != Some(TaskItemState::PendingRun)
                && cur != Some(TaskItemState::Completed)
            {
                self.set_task_state(&item.name, TaskItemState::Completed);
                let msg = if timing.is_empty() {
                    "complete".to_string()
                } else {
                    format!("complete ({timing})")
                };
                self.output_manager.service_event(&item.name, &msg);
            }
        } else {
            if let Some(rt) = self.tasks.get_mut(&item.name) {
                rt.set_needs_run_now(true);
                rt.last_run = item.last_run.clone();
            }
            self.set_task_state(&item.name, TaskItemState::Failed);
            if let Some(ref err_msg) = item.message {
                let msg = if timing.is_empty() {
                    format!("failed ({err_msg})")
                } else {
                    format!("failed ({err_msg}, {timing})")
                };
                self.output_manager.service_error_event(&item.name, &msg);
            }
        }
    }

    pub(in crate::runner) fn handle_task_exit(&mut self, exit: TaskExit) {
        let TaskExit {
            name,
            pgid,
            success,
            message,
            elapsed,
            last_run,
            rerun,
        } = exit;
        let name = name.as_str();
        if self.tasks.get(name).is_none_or(|rt| rt.pgid != Some(pgid)) {
            return;
        }

        if let Some(rt) = self.tasks.get_mut(name) {
            rt.pgid = None;
            rt.attach_count = 0;
            rt.pty_input = None;
        }

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
            self.set_task_state(name, TaskItemState::Completed);
            let msg = if timing.is_empty() {
                "complete".to_string()
            } else {
                format!("complete ({timing})")
            };
            self.output_manager.service_event(name, &msg);
            let run_generation = self.tasks.get(name).map(|rt| rt.run_generation);
            if let Some(done_tx) = self.done_tx.clone() {
                let name = name.to_string();
                tokio::spawn(async move {
                    let _ = done_tx
                        .send(ItemDone {
                            name,
                            kind: NodeKind::Task,
                            success: true,
                            message: None,
                            elapsed: None,
                            last_run: None,
                            task_run_generation: run_generation,
                        })
                        .await;
                });
            }
        } else {
            if let Some(rt) = self.tasks.get_mut(name) {
                rt.set_needs_run_now(true);
                rt.last_run = last_run;
            }
            self.set_task_state(name, TaskItemState::Failed);
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
        if let Some(rt) = self.tasks.get_mut(name)
            && rt
                .run_waiter
                .as_ref()
                .is_some_and(|waiter| waiter.generation() == rt.run_generation)
            && let Some(waiter) = rt.run_waiter.take()
        {
            waiter.complete(wait_result);
        }
    }
}
