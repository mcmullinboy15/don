//! The runner's half of param completion: look up the task and param,
//! then hand off to [`crate::param_completions`] on a detached task.

use std::collections::HashMap;

use tokio::sync::oneshot;

use crate::param_completions::{CompletionError, ResolveRequest, resolve};

use super::Runner;

impl Runner {
    /// Resolve candidate values for `param` on `task` by shelling out to its
    /// `completions` command. Does not block the runner's main loop — the
    /// actual command invocation is spawned as a detached tokio task so
    /// slow completions can't freeze status queries or lifecycle events.
    pub(in crate::runner) async fn handle_resolve_completions(
        &mut self,
        task: &str,
        param: &str,
        partial: HashMap<String, String>,
        force_refresh: bool,
        reply: oneshot::Sender<Result<Vec<String>, CompletionError>>,
    ) {
        let Some(rt) = self.tasks.get(task) else {
            let _ = reply.send(Err(CompletionError {
                message: format!("unknown task '{task}'"),
                log_path: None,
            }));
            return;
        };
        let param_cfg = rt.config.params.iter().find(|p| p.name == param).cloned();
        let task_env = rt.config.env.clone();
        let Some(p) = param_cfg else {
            let _ = reply.send(Err(CompletionError {
                message: format!("task '{task}' has no param '{param}'"),
                log_path: None,
            }));
            return;
        };
        let Some(completion_cfg) = p.completions.clone() else {
            // Static choices fast-path: return them directly without any
            // shell-out. The TUI form can still fuzzy-filter locally.
            let _ = reply.send(Ok(p.choices.clone()));
            return;
        };

        let cache = self.completion_cache.clone();
        let base_dir = self.base_dir.clone();
        let task_name = task.to_string();
        let param_name = param.to_string();
        tokio::spawn(async move {
            let result = resolve(ResolveRequest {
                cache: &cache,
                task: &task_name,
                param: &param_name,
                completions: &completion_cfg,
                base_dir: &base_dir,
                task_env: &task_env,
                partial: &partial,
                force_refresh,
            })
            .await;
            let _ = reply.send(result);
        });
    }
}
