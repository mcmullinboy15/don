use super::graph::topological_sort;

use super::{Runner, ServiceState};
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

        // End the build manager. It aborts every in-flight batch — the first
        // build as much as a rebuild or a re-query — and awaits them, so no
        // `LifecycleEmitter`/`SinkHandle` clone outlives shutdown and no new
        // batch can spawn mid-teardown. The `Child` inside has
        // `kill_on_drop(true)`, so dropping the aborted future SIGKILLs the
        // bazel client; the bounded joins inside guarantee the drop has run
        // before this returns.
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
        // Names only — what to do with the process is the supervisor's, which
        // is why the pgids this used to carry are no longer read.
        let live: std::collections::HashSet<String> = self
            .state
            .current()
            .processes
            .iter()
            .filter_map(|status| match status {
                crate::state_store::ProcessStatus::Service { name, runtime, .. } => {
                    runtime.as_ref().map(|_| name.clone())
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
            if !live.contains(name) {
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

            let mut join_set: JoinSet<String> = JoinSet::new();
            for name in &names {
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
                            // A second Ctrl+C arriving mid-stop cuts the
                            // grace period short. The supervisor holds the
                            // process, so it is the one that escalates; this
                            // used to be the scheduler polling a flag and
                            // signalling process groups it read out of the
                            // snapshot.
                            interrupt: Some(crate::signals::force_watch()),
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

            let mut force_rx = crate::signals::force_watch();
            let mut announced_force = false;
            loop {
                tokio::select! {
                    joined = join_set.join_next() => match joined {
                        Some(Ok(name)) => {
                            self.clear_service_custody(&name);
                            self.set_service_state(&name, ServiceState::Stopped);
                            remaining -= 1;
                            self.output_manager
                                .service_event(&name, &format!("stopped ({remaining} remaining)"));
                        }
                        Some(Err(_)) => {
                            remaining = remaining.saturating_sub(1);
                        }
                        // All stops joined.
                        None => break,
                    },
                    // Narration only: the escalation reached the supervisors
                    // directly, and their stops are already collapsing.
                    _ = force_rx.changed(), if !announced_force => {
                        if *force_rx.borrow() {
                            announced_force = true;
                            self.output_manager
                                .lifecycle_event("forcing immediate shutdown");
                        }
                    }
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

        // End any still-running task run. The supervisor holds the process and
        // is parked on its exit, so it is the one that signals — this says
        // which tasks, and waits for the kills to land before the supervisors
        // are ended below.
        let running_tasks: Vec<String> = self
            .state
            .current()
            .processes
            .iter()
            .filter_map(|status| match status {
                crate::state_store::ProcessStatus::Task { name, pid, .. } => {
                    pid.map(|_| name.clone())
                }
                _ => None,
            })
            .collect();
        if !running_tasks.is_empty() {
            self.output_manager.lifecycle_event(&format!(
                "killing {} running task{}",
                running_tasks.len(),
                if running_tasks.len() == 1 { "" } else { "s" }
            ));
            let mut done: Vec<tokio::sync::oneshot::Receiver<()>> = Vec::new();
            for name in &running_tasks {
                done.extend(self.send_task_kill(name));
                self.state.set_task_pid(name, None);
            }
            // Bounded once for the whole set, not once per task.
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                futures_util::future::join_all(done),
            )
            .await;
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
        // Custody goes through the funnel even here, so the projection ends
        // teardown agreeing with reality.
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
        while self.update_rx.try_recv().is_ok() {}
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
        // The supervisor holds the process and is parked on its exit; ask it
        // to end the run. It signals the group and drains the reader before
        // answering, which is what this waits for.
        let _ = wired;
        self.output_manager
            .service_event(&name, "run cancelled by shutdown");
        if let Some(done) = self.send_task_kill(&name) {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), done).await;
        }
    }
}
