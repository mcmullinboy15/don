use super::graph::topological_sort;

use super::{Runner, ServiceState, ServiceStopAction};
use crate::signals::force_shutdown_requested;
use std::collections::{BTreeMap, HashMap};
use tokio::task::JoinSet;

impl Runner {
    /// Initiate graceful shutdown of all services.
    pub(in crate::runner) async fn initiate_shutdown(&mut self) {
        // No gate revocation is needed: every supervisor watches the same
        // shutdown flag this sets, and refuses to self-start once it is set.

        if self.shutting_down {
            return;
        }
        self.shutting_down = true;
        let _ = self.event_tx.send(super::RunnerEvent::ShutdownStarted);
        let _ = self.shutdown_flag_tx.send(true);
        self.output_manager
            .lifecycle_event("shutting down gracefully... (Ctrl+C again to force)");

        // `ShutdownStarted` above is what withdraws this project from the
        // daemon — the binary subscribes and does it, detached. Deliberately
        // broadcast before any of the slow teardown below: the user is
        // waiting on Ctrl+C, and stopping services takes long enough that the
        // withdrawal normally lands well before we exit.

        // Abort the detached batch-build task and await its termination so
        // it can't keep any `LifecycleEmitter`/`SinkHandle` clones alive
        // past shutdown. The `Child` inside has `kill_on_drop(true)`, so
        // dropping the aborted future SIGKILLs the bazel client;
        // awaiting the JoinHandle guarantees the drop has actually run
        // before we continue. A 5s timeout guards against the pathological
        // case where the inner reader tasks don't drop promptly — we'd
        // rather continue shutdown than wedge on a stuck bazel pipe.
        if let Some(guard) = self.batch_build_handle.take()
            && let Some(handle) = guard.into_inner()
        {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }
        // Same treatment for the batched rebuild / graph re-query workers:
        // the batcher actor aborts its in-flight batches (bounded joins
        // inside) and exits, so no new batch can spawn mid-teardown.
        let (batcher_done_tx, batcher_done_rx) = tokio::sync::oneshot::channel();
        if self
            .batcher_tx
            .send(super::build_batcher::BatchRequest::Shutdown {
                done: batcher_done_tx,
            })
            .is_ok()
        {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(11), batcher_done_rx).await;
        }
        if let Some(handle) = self.batcher_handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }
        if let Some(handle) = self.update_check_handle.take() {
            handle.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }

        let mut service_worker_handles = Vec::new();
        for (name, rs) in &mut self.services {
            if let Some(worker) = rs.rebuild_worker.take() {
                self.output_manager
                    .service_event(name, "rebuild cancelled by shutdown");
                worker.abort();
                service_worker_handles.push(worker);
            }
        }
        for worker in service_worker_handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
        }

        for (name, rt) in &mut self.tasks {
            if let Some(waiter) = rt.run_waiter.take() {
                waiter.complete(Err(super::CommandError::Failed {
                    name: name.clone(),
                    message: "run cancelled by shutdown".to_string(),
                }));
            }
        }

        // Stop every task's run supervisor, cancelling anything it is
        // preparing. Draining the map drops the senders too, so nothing can
        // queue a run after this point. Abort them all before awaiting any,
        // or the 1s bound is paid once per task rather than once.
        let starting: Vec<String> = self
            .service_starts
            .registry()
            .busy_names()
            .map(str::to_string)
            .collect();
        for name in starting {
            self.output_manager
                .service_event(&name, "start cancelled by shutdown");
        }
        let busy: Vec<String> = self
            .task_supervisors
            .registry()
            .busy_names()
            .map(str::to_string)
            .collect();
        for name in busy {
            self.output_manager
                .service_event(&name, "run cancelled by shutdown");
        }
        // Neither supervisor set may be ended here: both own processes now
        // (task supervisors hold their runs to exit; service supervisors
        // execute the reverse-dependency stops below). Both are ended at the
        // teardown tail, once the stops have joined and the task pgid sweep
        // has run — aborting earlier would drop held handles (kill_on_drop)
        // before the graceful path decides anything.

        // Same treatment for any in-flight JIT lazy builds. These are
        // spawned when a lazy service's proxy gets its first connection
        // and, until this was tracked, would keep streaming bazel
        // output long past "shutdown complete".
        let lazy_handles: Vec<tokio::task::JoinHandle<()>> = self
            .lazy_build_handles
            .drain()
            .filter_map(|(_, (_, guard))| guard.into_inner())
            .collect();
        for h in &lazy_handles {
            h.abort();
        }
        for h in lazy_handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), h).await;
        }

        self.drain_late_worker_results().await;

        // Shut down all proxy listeners first (stop accepting new
        // connections). The supervisors own them; the directive is applied
        // even mid-prepare, and lands before any queued Stop per mailbox
        // FIFO — so no listener outlives the teardown it narrates.
        let proxy_names: Vec<String> = self
            .services
            .iter()
            .filter(|(_, rs)| !rs.resolved.proxy.is_empty())
            .map(|(name, _)| name.clone())
            .collect();
        for name in proxy_names {
            self.send_proxy_directive(&name, super::service_supervisor::ProxyDirective::Shutdown);
        }

        // The API server is NOT told to stop here, deliberately. Streaming
        // responses (log/event followers) end on that signal, and firing it
        // before the stop loop would cut every attached client off from the
        // entire teardown narration — the stopping/stopped lines they are
        // watching for. The flip happens at the end of teardown, after the
        // output flush, in `run`'s tail; serving reads during teardown is
        // harmless (commands land in a queue nobody reads and their reply
        // channels drop, which clients already handle).

        // Build reverse dependency order for shutdown.
        // Services at the same depth (no dependency relationship) stop concurrently.
        let dep_map = self.build_dep_name_map();
        let order = match topological_sort(&dep_map) {
            Ok(o) => o,
            Err(cycle) => {
                self.output_manager.error_event(&format!(
                    "shutdown: dependency graph has a cycle ({cycle:?}) — \
                     stopping live services in arbitrary order"
                ));
                self.services.keys().cloned().collect()
            }
        };

        // Compute depth of each service node for grouping.
        let mut depths: HashMap<String, usize> = HashMap::new();
        for name in &order {
            let node_deps = dep_map.get(name).cloned().unwrap_or_default();
            let max_dep_depth = node_deps
                .iter()
                .filter_map(|d| depths.get(d))
                .max()
                .copied()
                .unwrap_or(0);
            let depth = if node_deps.is_empty() {
                0
            } else {
                max_dep_depth + 1
            };
            depths.insert(name.clone(), depth);
        }

        // Group live services by depth, then iterate from highest depth
        // (most dependent) to lowest (least dependent). A service handle is
        // the source of truth here: states like Unhealthy still have a live
        // process and must be signalled during shutdown.
        //
        // Services the shadow says are *not* live are collected separately
        // and sent an unjoined Stop at the same depth. A supervisor spends
        // its own start permission now, so the shadow can trail reality by a
        // channel hop at exactly the moment teardown decides who to stop —
        // and a supervisor holding nothing answers immediately, so telling
        // everyone costs nothing. They stay out of the narration and the
        // countdown: a service with no live process was never "stopping".
        // Custody, read once from the projection: the fold is not running
        // during teardown, so this is exactly as fresh as the old shadow was.
        let live: HashMap<String, Option<i32>> = self
            .state
            .current()
            .processes
            .iter()
            .filter_map(|status| match status {
                crate::state_store::ProcessStatus::Service { name, runtime, .. } => {
                    runtime.as_ref().map(|runtime| (name.clone(), runtime.pid))
                }
                _ => None,
            })
            .collect();
        let mut by_depth: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        let mut quiet_by_depth: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        for name in &order {
            if !self.services.contains_key(name) {
                continue;
            }
            let depth = depths.get(name).copied().unwrap_or(0);
            if !live.contains_key(name) {
                quiet_by_depth.entry(depth).or_default().push(name.clone());
                continue;
            }
            by_depth.entry(depth).or_default().push(name.clone());
        }

        let mut remaining: usize = by_depth.values().map(|v| v.len()).sum();

        // Stop from highest depth to lowest (dependents first).
        for (depth, names) in by_depth.into_iter().rev() {
            for name in quiet_by_depth.remove(&depth).unwrap_or_default() {
                self.send_unjoined_stop(&name);
            }
            for name in &names {
                self.set_service_state(name, ServiceState::Stopping);
                self.output_manager
                    .service_event(name, &format!("stopping... ({remaining} remaining)"));
            }

            // Track PGIDs of services being stopped so we can SIGKILL
            // them if a second Ctrl+C arrives during graceful shutdown.
            let mut stopping_pgids: HashMap<String, i32> = HashMap::new();
            let mut join_set: JoinSet<String> = JoinSet::new();
            for name in &names {
                if let Some(pgid) = live.get(name).copied().flatten() {
                    stopping_pgids.insert(name.clone(), pgid);
                }
                // The supervisor owns the process; ask it to stop and join
                // on the done-signal. A supervisor holding nothing answers
                // immediately, so sending unconditionally is safe — and it
                // is what keeps docker services (no pgid) covered.
                let shutdown_config = self.effective_shutdown_config(name);
                let force = force_shutdown_requested();
                let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                let sent = self.service_starts.registry().get(name).is_some_and(|h| {
                    h.request(super::service_supervisor::ServiceCommand::Stop(
                        super::service_supervisor::StopRequest {
                            config: shutdown_config,
                            force,
                            wait_full_exit: true,
                            interrupt: None,
                            notify: super::service_supervisor::StopNotify::Done(done_tx),
                            // Teardown: the failure history dies with the runner.
                            reset_policy: false,
                        },
                    ))
                });
                if sent {
                    let name_owned = name.clone();
                    join_set.spawn(async move {
                        let _ = done_rx.await;
                        name_owned
                    });
                }
            }

            // Wait for graceful stops, but if a second Ctrl+C arrives,
            // SIGKILL all processes being stopped and abort the futures.
            loop {
                if force_shutdown_requested() && !join_set.is_empty() {
                    self.output_manager
                        .lifecycle_event("forcing immediate shutdown");
                    // SIGKILL all processes that are still being stopped.
                    let names: Vec<String> = stopping_pgids
                        .iter()
                        .map(|(name, pgid)| {
                            self.output_manager.service_event(
                                name,
                                &format!("send SIGKILL to pgid {pgid} (force shutdown)"),
                            );
                            let _ = nix::sys::signal::killpg(
                                nix::unistd::Pid::from_raw(*pgid),
                                nix::sys::signal::Signal::SIGKILL,
                            );
                            name.clone()
                        })
                        .collect();
                    for name in names {
                        self.clear_service_custody(&name);
                        self.set_service_state(&name, ServiceState::Stopped);
                    }
                    join_set.abort_all();
                    while join_set.join_next().await.is_some() {}
                    remaining = 0;
                    break;
                }

                // Poll for the next completed stop, with a short sleep so
                // we can re-check the force flag promptly.
                match tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    join_set.join_next(),
                )
                .await
                {
                    Ok(Some(Ok(name))) => {
                        stopping_pgids.remove(&name);
                        self.clear_service_custody(&name);
                        self.set_service_state(&name, ServiceState::Stopped);
                        remaining -= 1;
                        self.output_manager
                            .service_event(&name, &format!("stopped ({remaining} remaining)"));
                    }
                    Ok(Some(Err(_))) => {
                        remaining = remaining.saturating_sub(1);
                    }
                    Ok(None) => break,  // All tasks done.
                    Err(_) => continue, // Timeout — re-check force flag.
                }
            }

            if remaining == 0 {
                break;
            }
        }

        // Depths with no live service never ran the loop above; catch their
        // supervisors here so nothing is left unasked.
        let leftover: Vec<String> = quiet_by_depth.into_values().flatten().collect();
        for name in leftover {
            self.send_unjoined_stop(&name);
        }

        // Kill any still-running task process groups.
        let running_task_pgids: Vec<(String, i32)> = self
            .tasks
            .iter()
            .filter_map(|(name, rt)| rt.pgid.map(|pgid| (name.clone(), pgid)))
            .collect();
        if !running_task_pgids.is_empty() {
            self.output_manager.lifecycle_event(&format!(
                "killing {} running task{}",
                running_task_pgids.len(),
                if running_task_pgids.len() == 1 {
                    ""
                } else {
                    "s"
                }
            ));
            for (name, pgid) in &running_task_pgids {
                self.output_manager
                    .service_event(name, &format!("send SIGKILL to task pgid {pgid}"));
                if let Err(e) = nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(*pgid),
                    nix::sys::signal::Signal::SIGKILL,
                ) {
                    // ESRCH = already dead, which is fine.
                    if e != nix::Error::ESRCH {
                        self.output_manager.service_error_event(
                            name,
                            &format!("failed to kill task pgid {pgid}: {e}"),
                        );
                    }
                }
                if let Some(rt) = self.tasks.get_mut(name) {
                    rt.pgid = None;
                }
                self.state.set_task_pid(name, None);
            }
        }

        // Every stop has been executed and joined, and the task pgid sweep
        // has run; both supervisor sets are idle (or their waits have
        // completed against killed processes) and can end now.
        // Abort-all-then-await keeps the 1s bound paid once, not once per
        // process.
        let supervisors = self.service_starts.abort_all();
        for (_, handle) in supervisors {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }
        let supervisors = self.task_supervisors.abort_all();
        for (_, handle) in supervisors {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        }
    }

    /// Wait for remaining async tasks to finish after shutdown.
    pub(in crate::runner) async fn wait_for_shutdown(&mut self) {
        // All handles should already be stopped by initiate_shutdown.
        // Drop remaining handles, release sockets, clear attach state.
        for rs in self.services.values_mut() {
            if let Some(worker) = rs.rebuild_worker.take() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await;
            }
            rs.stop_action = ServiceStopAction::None;
        }
        // Custody goes through the funnel even here, so the projection and
        // the shadow end teardown agreeing.
        let names: Vec<String> = self.services.keys().cloned().collect();
        for name in names {
            self.clear_service_custody(&name);
        }
    }

    async fn drain_late_worker_results(&mut self) {
        // Late prepared reports carry spawned processes' identities; kill
        // those. Everything else is bookkeeping nobody needs mid-teardown.
        while let Ok(report) = self.report_rx.try_recv() {
            match report {
                super::ProcessReport::ServiceStartPrepared { name, result, .. } => {
                    self.stop_late_service_start(name, result).await;
                }
                super::ProcessReport::TaskRunPrepared { name, result, .. } => {
                    self.stop_late_task_start(name, result).await;
                }
                _ => {}
            }
        }
        while self.internal_rx.try_recv().is_ok() {}
    }

    /// Ask a supervisor to stop whatever it holds, without waiting.
    ///
    /// For services the runner's shadow says are not running: it may have
    /// spent a start permission the runner has not folded yet. Nothing is
    /// joined and nothing is narrated — this is a safety net, not a step.
    fn send_unjoined_stop(&self, name: &str) {
        let shutdown_config = self.effective_shutdown_config(name);
        let (done_tx, _done_rx) = tokio::sync::oneshot::channel();
        if let Some(handle) = self.service_starts.registry().get(name) {
            let _ = handle.request(super::service_supervisor::ServiceCommand::Stop(
                super::service_supervisor::StopRequest {
                    config: shutdown_config,
                    force: force_shutdown_requested(),
                    wait_full_exit: false,
                    interrupt: None,
                    notify: super::service_supervisor::StopNotify::Done(done_tx),
                    // Teardown: the failure history dies with the runner.
                    reset_policy: false,
                },
            ));
        }
    }

    pub(in crate::runner) async fn stop_late_service_start(
        &mut self,
        name: String,
        result: Result<Box<super::service_supervisor::ServiceWired>, String>,
    ) {
        let Ok(_wired) = result else {
            return;
        };
        self.output_manager
            .service_event(&name, "start cancelled by shutdown");
        // The supervisor wired this spawn and holds the process — ask it to
        // stop. Its reader drains before the done-signal, so joining this
        // covers the output too (what the old inline reader+stop did).
        let shutdown_config = self.effective_shutdown_config(&name);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let sent = self.service_starts.registry().get(&name).is_some_and(|h| {
            h.request(super::service_supervisor::ServiceCommand::Stop(
                super::service_supervisor::StopRequest {
                    config: shutdown_config,
                    force: force_shutdown_requested(),
                    wait_full_exit: false,
                    interrupt: None,
                    notify: super::service_supervisor::StopNotify::Done(done_tx),
                    // Teardown: the failure history dies with the runner.
                    reset_policy: false,
                },
            ))
        });
        if sent {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(15), done_rx).await;
        }
    }

    pub(in crate::runner) async fn stop_late_task_start(
        &mut self,
        name: String,
        result: Result<super::task_supervisor::TaskRunReport, String>,
    ) {
        let Ok(super::task_supervisor::TaskRunReport::Running(wired)) = result else {
            return;
        };
        // The supervisor holds the process and is waiting on its exit; kill
        // the group and let that wait complete. The reader drains inside
        // the supervisor before it reports.
        self.output_manager
            .service_event(&name, "run cancelled by shutdown");
        self.output_manager
            .service_event(&name, &format!("send SIGKILL to task pgid {}", wired.pgid));
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(wired.pgid),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}
