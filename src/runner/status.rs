use super::{ItemStatus, Runner, VerboseInfo};
use tokio::sync::oneshot;

impl Runner {
    pub(in crate::runner) async fn fetch_watch_snapshot(
        &self,
    ) -> Option<crate::watch::WatchSnapshot> {
        let tx = self.watch_query_tx.as_ref()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        if tx
            .send(crate::watch::WatchQuery { reply: reply_tx })
            .await
            .is_err()
        {
            return None;
        }
        tokio::time::timeout(std::time::Duration::from_millis(250), reply_rx)
            .await
            .ok()
            .and_then(Result::ok)
    }

    /// Collect status of all items.
    pub(in crate::runner) async fn collect_status(&self, verbose: bool) -> Vec<ItemStatus> {
        let mut statuses = Vec::new();
        let watch_snapshot = if verbose {
            self.fetch_watch_snapshot().await
        } else {
            None
        };
        let watch_snapshot_available = watch_snapshot.is_some();
        for (name, rs) in &self.services {
            let verbose_info = if verbose {
                let resolved = &rs.resolved;
                let ready = resolved.ready.as_ref().map(|r| {
                    if let Some(ref tcp) = r.tcp {
                        format!("tcp {tcp}")
                    } else if let Some(ref http) = r.http {
                        format!("http {http}")
                    } else if let Some(ref exec) = r.exec {
                        format!("{} {}", exec.cmd, exec.args.join(" "))
                    } else {
                        "none".to_string()
                    }
                });
                let cmd = resolved.run_cmd().map(|r| {
                    if r.args.is_empty() {
                        r.cmd.clone()
                    } else {
                        format!("{} {}", r.cmd, r.args.join(" "))
                    }
                });
                // Use resolved build tool watch paths if explicit ones are empty.
                let watch = if resolved.watch.is_empty() {
                    rs.resolved_watch_paths.clone()
                } else {
                    resolved.watch.clone()
                };
                let watch_item = watch_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.items.get(name));
                let mut watch_notes = Vec::new();
                if let Some(snapshot) = &watch_snapshot {
                    if snapshot.notify_error_count > 0
                        && let Some(ref last) = snapshot.last_notify_error
                    {
                        watch_notes.push(format!(
                            "notify errors={} last={last}",
                            snapshot.notify_error_count
                        ));
                    }
                    if snapshot.runner_event_lag_count > 0 {
                        watch_notes.push(format!(
                            "runner-event lag count={}",
                            snapshot.runner_event_lag_count
                        ));
                    }
                }
                if let Some(item) = watch_item {
                    if let Some(ref last_error) = item.last_error {
                        watch_notes.push(last_error.clone());
                    }
                } else if !watch.is_empty() && watch_snapshot_available {
                    watch_notes.push("watch item missing from watch manager".to_string());
                } else if !watch.is_empty() {
                    watch_notes.push("watch manager unavailable".to_string());
                }
                Some(VerboseInfo {
                    depends_on: resolved.depends_on.clone(),
                    watch,
                    proxy: resolved
                        .proxy
                        .iter()
                        .map(|p| match &p.mode {
                            crate::config::ProxyMode::Env(name) => {
                                format!("{} (env={name})", p.listen)
                            }
                            crate::config::ProxyMode::Listenfd => {
                                format!("{} (listenfd)", p.listen)
                            }
                            crate::config::ProxyMode::Forward(target) => {
                                format!("{} → {target}", p.listen)
                            }
                        })
                        .collect(),
                    proxy_active_connections: rs
                        .proxy
                        .as_ref()
                        .and_then(|proxy| proxy.active_forward_connections()),
                    bazel_target: resolved.bazel_config().map(|b| b.target.clone()),
                    turbo_task: resolved.turbo_config().map(|t| t.task.clone()),
                    ready,
                    cmd,
                    watch_state: watch_item.map(|item| {
                        format!(
                            "{} state={} stale={} debounce={}ms",
                            item.kind, item.state, item.stale, item.debounce_ms
                        )
                    }),
                    watch_notes,
                })
            } else {
                None
            };
            statuses.push(ItemStatus::Service {
                name: name.clone(),
                state: rs.state(),
                verbose: verbose_info,
            });
        }
        for (name, rt) in &self.tasks {
            let verbose_info = if verbose {
                let task = &rt.config;
                let cmd_str = if task.args.is_empty() {
                    task.cmd.clone()
                } else {
                    format!("{} {}", task.cmd, task.args.join(" "))
                };
                let watch = if task.watch.is_empty() {
                    rt.resolved_watch_paths.clone()
                } else {
                    task.watch.clone()
                };
                let watch_item = watch_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.items.get(name));
                let mut watch_notes = Vec::new();
                if let Some(snapshot) = &watch_snapshot {
                    if snapshot.notify_error_count > 0
                        && let Some(ref last) = snapshot.last_notify_error
                    {
                        watch_notes.push(format!(
                            "notify errors={} last={last}",
                            snapshot.notify_error_count
                        ));
                    }
                    if snapshot.runner_event_lag_count > 0 {
                        watch_notes.push(format!(
                            "runner-event lag count={}",
                            snapshot.runner_event_lag_count
                        ));
                    }
                }
                if let Some(item) = watch_item {
                    if let Some(ref last_error) = item.last_error {
                        watch_notes.push(last_error.clone());
                    }
                } else if !watch.is_empty() && watch_snapshot_available {
                    watch_notes.push("watch item missing from watch manager".to_string());
                } else if !watch.is_empty() {
                    watch_notes.push("watch manager unavailable".to_string());
                }
                Some(VerboseInfo {
                    depends_on: task.depends_on.clone(),
                    watch,
                    proxy: Vec::new(),
                    proxy_active_connections: None,
                    bazel_target: task.bazel.as_ref().map(|b| b.target.clone()),
                    turbo_task: task.turbo.as_ref().map(|t| t.task.clone()),
                    ready: None,
                    cmd: Some(cmd_str),
                    watch_state: watch_item.map(|item| {
                        format!(
                            "{} state={} stale={} debounce={}ms",
                            item.kind, item.state, item.stale, item.debounce_ms
                        )
                    }),
                    watch_notes,
                })
            } else {
                None
            };
            statuses.push(ItemStatus::Task {
                name: name.clone(),
                state: rt.state(),
                last_run: rt.last_run.clone(),
                verbose: verbose_info,
            });
        }
        statuses
    }
}
