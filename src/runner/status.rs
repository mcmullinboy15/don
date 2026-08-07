use super::{ProcessStatus, Runner, VerboseInfo};

impl Runner {
    pub(in crate::runner) async fn fetch_watch_snapshot(
        &self,
    ) -> Option<crate::watch::WatchSnapshot> {
        self.watch.as_ref()?.snapshot().await
    }

    /// The cheap, allocation-only projection of every process's state.
    ///
    /// Synchronous by construction: it touches nothing but the runner's own
    /// maps. That is what lets it be republished on every state transition and
    /// read straight out of [`StateReader`] without a command round trip.
    ///
    /// [`StateReader`]: super::StateReader
    pub(in crate::runner) fn status_projection(
        &self,
        detail_name: Option<&str>,
    ) -> Vec<ProcessStatus> {
        let mut statuses = Vec::new();
        for (name, rs) in &self.services {
            if detail_name.is_some_and(|want| want != name) {
                continue;
            }
            statuses.push(ProcessStatus::Service {
                name: name.clone(),
                state: rs.state(),
                failed_dependencies: rs.failed_dependencies().to_vec(),
                verbose: None,
            });
        }
        for (name, rt) in &self.tasks {
            if detail_name.is_some_and(|want| want != name) {
                continue;
            }
            statuses.push(ProcessStatus::Task {
                name: name.clone(),
                state: rt.state(),
                failed_dependencies: rt.failed_dependencies().to_vec(),
                last_run: rt.last_run.clone(),
                verbose: None,
            });
        }
        statuses
    }

    /// Collect status of all processes.
    ///
    /// When `detail_name` is `Some`, only that single service/task is returned
    /// and its fully-resolved `watch` path list is included. The all-processes view
    /// (`detail_name == None`) deliberately omits the path list — for a
    /// build-tool stack it can be hundreds of resolved paths per service, so the
    /// default verbose view reports only the count and callers drill in by name.
    ///
    /// The non-verbose answer is [`status_projection`](Self::status_projection),
    /// so what a client reads from the state store and what it reads from this
    /// command cannot drift.
    pub(in crate::runner) async fn collect_status(
        &self,
        verbose: bool,
        detail_name: Option<&str>,
    ) -> Vec<ProcessStatus> {
        if !verbose {
            return self.status_projection(detail_name);
        }
        let mut statuses = Vec::new();
        let watch_snapshot = self.fetch_watch_snapshot().await;
        let watch_snapshot_available = watch_snapshot.is_some();
        for (name, rs) in &self.services {
            if detail_name.is_some_and(|want| want != name) {
                continue;
            }
            let verbose_info = {
                let resolved = &rs.resolved;
                let ready = self.endpoint_ready_check(name, resolved).as_ref().map(|r| {
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
                if let Some(process) = watch_item {
                    if let Some(ref last_error) = process.last_error {
                        watch_notes.push(last_error.clone());
                    }
                } else if !watch.is_empty() && watch_snapshot_available {
                    watch_notes.push("watch process missing from watch manager".to_string());
                } else if !watch.is_empty() {
                    watch_notes.push("watch manager unavailable".to_string());
                }
                Some(VerboseInfo {
                    depends_on: resolved.depends_on.clone(),
                    // Services have no params; only tasks are run with values.
                    params: Vec::new(),
                    watch_count: watch.len(),
                    // Full path list only on a single-process drill-in; see
                    // `collect_status` doc for why the all-processes view omits it.
                    watch: if detail_name.is_some() {
                        watch
                    } else {
                        Vec::new()
                    },
                    proxy: rs.proxy_view.as_ref().map_or_else(
                        || {
                            resolved
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
                                .collect()
                        },
                        |proxy| {
                            let mut entries = proxy.descriptions();
                            // A failed service's listeners still exist, so
                            // say why they are closing connections instead of
                            // leaving the address looking healthy.
                            if proxy.is_refusing() {
                                for entry in &mut entries {
                                    entry.push_str(" — refusing (service failed)");
                                }
                            }
                            entries
                        },
                    ),
                    docker_ports: match rs.handle_identity {
                        Some(super::ServiceHandleIdentity::Docker) => rs
                            .docker_port_bindings
                            .iter()
                            .map(|binding| {
                                format!(
                                    "{} → {} ({}/{})",
                                    binding.configured,
                                    binding.connect_addr(),
                                    binding.container_port,
                                    binding.protocol
                                )
                            })
                            .collect(),
                        _ => Vec::new(),
                    },
                    proxy_active_connections: rs
                        .proxy_view
                        .as_ref()
                        .and_then(|proxy| proxy.active_forward_connections()),
                    bazel_target: resolved.bazel_config().map(|b| b.target.clone()),
                    ready,
                    cmd,
                    watch_state: watch_item.map(|process| {
                        format!(
                            "{} state={} stale={} debounce={}ms",
                            process.kind, process.state, process.stale, process.debounce_ms
                        )
                    }),
                    watch_notes,
                })
            };
            statuses.push(ProcessStatus::Service {
                name: name.clone(),
                state: rs.state(),
                failed_dependencies: rs.failed_dependencies().to_vec(),
                verbose: verbose_info,
            });
        }
        for (name, rt) in &self.tasks {
            if detail_name.is_some_and(|want| want != name) {
                continue;
            }
            let verbose_info = {
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
                if let Some(process) = watch_item {
                    if let Some(ref last_error) = process.last_error {
                        watch_notes.push(last_error.clone());
                    }
                } else if !watch.is_empty() && watch_snapshot_available {
                    watch_notes.push("watch process missing from watch manager".to_string());
                } else if !watch.is_empty() {
                    watch_notes.push("watch manager unavailable".to_string());
                }
                Some(VerboseInfo {
                    depends_on: task.depends_on.clone(),
                    watch_count: watch.len(),
                    // Full path list only on a single-process drill-in; see
                    // `collect_status` doc for why the all-processes view omits it.
                    watch: if detail_name.is_some() {
                        watch
                    } else {
                        Vec::new()
                    },
                    proxy: Vec::new(),
                    docker_ports: Vec::new(),
                    proxy_active_connections: None,
                    bazel_target: task.bazel.as_ref().map(|b| b.target.clone()),
                    params: task
                        .params
                        .iter()
                        .map(super::ParamInfo::from_config)
                        .collect(),
                    ready: None,
                    cmd: Some(cmd_str),
                    watch_state: watch_item.map(|process| {
                        format!(
                            "{} state={} stale={} debounce={}ms",
                            process.kind, process.state, process.stale, process.debounce_ms
                        )
                    }),
                    watch_notes,
                })
            };
            statuses.push(ProcessStatus::Task {
                name: name.clone(),
                state: rt.state(),
                failed_dependencies: rt.failed_dependencies().to_vec(),
                last_run: rt.last_run.clone(),
                verbose: verbose_info,
            });
        }
        statuses
    }
}
