//! Translation between the file watcher's vocabulary and the runner's.
//!
//! [`crate::watch`] speaks [`WatchSignal`] and [`WatchOutcome`]; the runner
//! speaks [`RunnerCommand`] and [`RunnerEvent`]. Keeping the two apart is what
//! stops `watch` importing from `runner` while `runner` constructs and drives
//! `watch` — so the adapter lives on this side, where the dependency already
//! points.
//!
//! It is a task rather than an arm of the runner's `select!` because the
//! inbound direction is a `broadcast` subscription: draining it promptly is
//! what keeps the watcher off the lag path, and the runner's loop can be busy
//! for as long as a service takes to stop.

use super::{RunnerCommand, RunnerEvent};
use crate::watch::{WatchOutcome, WatchQuery, WatchSignal, WatchSnapshot, WatchUpdate};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

/// How long to wait for a watcher to answer a status query.
///
/// Verbose status is interactive, so a wedged watcher must degrade to "no
/// watch info" rather than hang the caller.
const QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

/// The runner's half of the link to a *running* watcher.
///
/// Held as an `Option` on the runner, and `Some` only once the watcher is
/// actually running — which is the point. These two senders used to be
/// separate `Option` fields set together before `WatchManager` was even
/// constructed, so `is_none()` meant "startup hasn't got there yet", never
/// "there is no watcher". A config with nothing to watch left both `Some`,
/// pointing at receivers that had already been dropped.
pub(in crate::runner) struct WatchHandle {
    /// Revised watch patterns, pushed after a build-tool re-query.
    updates: mpsc::UnboundedSender<WatchUpdate>,
    /// Status queries for verbose output.
    queries: mpsc::Sender<WatchQuery>,
}

impl WatchHandle {
    pub(in crate::runner) fn new(
        updates: mpsc::UnboundedSender<WatchUpdate>,
        queries: mpsc::Sender<WatchQuery>,
    ) -> Self {
        Self { updates, queries }
    }

    /// A sender for pushing revised watch patterns.
    pub(in crate::runner) fn updates(&self) -> mpsc::UnboundedSender<WatchUpdate> {
        self.updates.clone()
    }

    /// Ask the watcher what it is currently watching.
    ///
    /// `None` if it has gone away or does not answer within
    /// [`QUERY_TIMEOUT`] — verbose status drops the watch section rather than
    /// blocking on it.
    pub(in crate::runner) async fn snapshot(&self) -> Option<WatchSnapshot> {
        let (reply, reply_rx) = oneshot::channel();
        self.queries.send(WatchQuery { reply }).await.ok()?;
        tokio::time::timeout(QUERY_TIMEOUT, reply_rx)
            .await
            .ok()
            .and_then(Result::ok)
    }
}

/// Wire a watcher's two channels to the runner's, and run until either side
/// goes away.
///
/// Shutdown propagates by channel closure in both directions: when the runner
/// drops `event_tx` this task returns, dropping the outcome sender, which is
/// how [`crate::watch::WatchManager::run`] learns to stop. When the watcher
/// stops first, its signal sender drops and this task returns.
pub(in crate::runner) fn spawn(
    mut signal_rx: mpsc::UnboundedReceiver<WatchSignal>,
    cmd_tx: mpsc::UnboundedSender<RunnerCommand>,
    batcher_tx: mpsc::UnboundedSender<super::build_batcher::BatchRequest>,
    requery_catalog: std::collections::HashMap<String, super::build_tools::GraphRequeryRequestItem>,
    mut events: broadcast::Receiver<RunnerEvent>,
    outcome_tx: mpsc::UnboundedSender<WatchOutcome>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                signal = signal_rx.recv() => {
                    // `None` means the watcher is gone; there is nothing left
                    // to translate in either direction.
                    let Some(signal) = signal else { return };
                    // Graph re-queries go straight to the batcher actor —
                    // deciding what a BUILD-file change means is a per-item
                    // config fact, precomputed into the catalog. Everything
                    // else lands on runner state and goes through the fold.
                    if let WatchSignal::BuildGraphChanged { name } = signal {
                        let items: Vec<_> =
                            if name == crate::watch::WORKSPACE_GRAPH_ITEM_NAME {
                                requery_catalog.values().cloned().collect()
                            } else {
                                requery_catalog.get(&name).cloned().into_iter().collect()
                            };
                        for item in items {
                            if batcher_tx
                                .send(super::build_batcher::BatchRequest::QueueRequery { item })
                                .is_err()
                            {
                                return;
                            }
                        }
                        continue;
                    }
                    if let Some(command) = command_for(signal)
                        && cmd_tx.send(command).is_err()
                    {
                        return;
                    }
                }
                event = events.recv() => {
                    match event {
                        Ok(event) => {
                            // Most runner events mean nothing to a watcher.
                            let Some(outcome) = outcome_for(event) else { continue };
                            if outcome_tx.send(outcome).is_err() {
                                return;
                            }
                        }
                        // Dropped events may include a completion the watcher
                        // is blocked on, and the names are unrecoverable — so
                        // pass the gap along rather than swallowing it.
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            if outcome_tx.send(WatchOutcome::Lagged(n)).is_err() {
                                return;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        }
    })
}

/// The runner command a watch signal asks for. `None` for the one signal
/// with a non-runner recipient: graph changes go to the batcher actor.
fn command_for(signal: WatchSignal) -> Option<RunnerCommand> {
    match signal {
        WatchSignal::Rebuild { name } => Some(RunnerCommand::Rebuild { name }),
        WatchSignal::TaskRerun { name } => Some(RunnerCommand::TaskRerun { name }),
        WatchSignal::RebuildStale { name } => Some(RunnerCommand::RebuildStale { name }),
        WatchSignal::BuildGraphChanged { .. } => None,
    }
}

/// The watch outcome a runner event carries, if any.
///
/// Only the two completions the watcher gates on cross over. Everything else —
/// state changes, log events, shutdown — is none of its business, and
/// forwarding it would just be lag it has to absorb.
fn outcome_for(event: RunnerEvent) -> Option<WatchOutcome> {
    match event {
        RunnerEvent::RebuildComplete { name, success } => {
            Some(WatchOutcome::RebuildComplete { name, success })
        }
        RunnerEvent::TaskRerunComplete { name, success } => {
            Some(WatchOutcome::TaskRerunComplete { name, success })
        }
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// `RunnerCommand` carries reply channels and so cannot derive `Debug`;
    /// reduce the four this module emits to a comparable shape.
    fn describe(cmd: &RunnerCommand) -> (&'static str, &str) {
        match cmd {
            RunnerCommand::Rebuild { name } => ("Rebuild", name),
            RunnerCommand::TaskRerun { name } => ("TaskRerun", name),
            RunnerCommand::RebuildStale { name } => ("RebuildStale", name),
            _ => ("other", ""),
        }
    }

    #[tokio::test]
    async fn every_signal_translates_to_its_command() {
        struct Case {
            name: &'static str,
            signal: WatchSignal,
            want: (&'static str, &'static str),
        }

        let cases = vec![
            Case {
                name: "rebuild",
                signal: WatchSignal::Rebuild {
                    name: "api".to_string(),
                },
                want: ("Rebuild", "api"),
            },
            Case {
                name: "task rerun",
                signal: WatchSignal::TaskRerun {
                    name: "migrate".to_string(),
                },
                want: ("TaskRerun", "migrate"),
            },
            Case {
                // Must not become `Rebuild`: the runner has to record the
                // staleness and re-fire, not start a second build.
                name: "rebuild stale",
                signal: WatchSignal::RebuildStale {
                    name: "api".to_string(),
                },
                want: ("RebuildStale", "api"),
            },
        ];

        for case in cases {
            let (signal_tx, signal_rx) = mpsc::unbounded_channel();
            let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
            let (batcher_tx, _batcher_rx) = mpsc::unbounded_channel();
            let (_event_tx, events) = broadcast::channel(16);
            let (outcome_tx, _outcome_rx) = mpsc::unbounded_channel();
            let _handle = spawn(
                signal_rx,
                cmd_tx,
                batcher_tx,
                std::collections::HashMap::new(),
                events,
                outcome_tx,
            );

            signal_tx.send(case.signal).unwrap();
            let got = cmd_rx.recv().await.unwrap();
            assert_eq!(describe(&got), case.want, "{}: wrong command", case.name);
        }
    }

    /// A graph change routes to the batcher actor, not the runner: the
    /// catalog answers what to re-query, and the workspace marker fans out
    /// to every catalogued item.
    #[tokio::test]
    async fn graph_changes_route_to_the_batcher() {
        let item = |name: &str| super::super::build_tools::GraphRequeryRequestItem {
            name: name.to_string(),
            bazel: None,
            watch_enabled: true,
            working_dir: std::path::PathBuf::from("/tmp"),
            ignore_patterns: Vec::new(),
        };
        let catalog: std::collections::HashMap<_, _> = [
            ("api".to_string(), item("api")),
            ("worker".to_string(), item("worker")),
        ]
        .into_iter()
        .collect();

        let (signal_tx, signal_rx) = mpsc::unbounded_channel();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let (batcher_tx, mut batcher_rx) = mpsc::unbounded_channel();
        let (_event_tx, events) = broadcast::channel(16);
        let (outcome_tx, _outcome_rx) = mpsc::unbounded_channel();
        let _handle = spawn(signal_rx, cmd_tx, batcher_tx, catalog, events, outcome_tx);

        // A named item queues exactly its own re-query.
        signal_tx
            .send(WatchSignal::BuildGraphChanged {
                name: "api".to_string(),
            })
            .unwrap();
        let super::super::build_batcher::BatchRequest::QueueRequery { item } =
            batcher_rx.recv().await.unwrap()
        else {
            panic!("expected a requery");
        };
        assert_eq!(item.name, "api");

        // The workspace marker fans out to every catalogued item.
        signal_tx
            .send(WatchSignal::BuildGraphChanged {
                name: crate::watch::WORKSPACE_GRAPH_ITEM_NAME.to_string(),
            })
            .unwrap();
        let mut names = vec![];
        for _ in 0..2 {
            let super::super::build_batcher::BatchRequest::QueueRequery { item } =
                batcher_rx.recv().await.unwrap()
            else {
                panic!("expected a requery");
            };
            names.push(item.name);
        }
        names.sort();
        assert_eq!(names, vec!["api".to_string(), "worker".to_string()]);
        assert!(
            cmd_rx.try_recv().is_err(),
            "graph changes must not reach the runner"
        );
    }

    #[tokio::test]
    async fn only_the_two_completions_reach_the_watcher() {
        let (_signal_tx, signal_rx) = mpsc::unbounded_channel();
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, events) = broadcast::channel(16);
        let (outcome_tx, mut outcome_rx) = mpsc::unbounded_channel();
        let _handle = {
            let (batcher_tx, _batcher_rx) = mpsc::unbounded_channel();
            spawn(
                signal_rx,
                cmd_tx,
                batcher_tx,
                std::collections::HashMap::new(),
                events,
                outcome_tx,
            )
        };

        // An event the watcher does not care about must not be forwarded —
        // otherwise every state change becomes lag it has to absorb.
        event_tx.send(RunnerEvent::ShutdownStarted).unwrap();
        event_tx
            .send(RunnerEvent::RebuildComplete {
                name: "api".to_string(),
                success: true,
            })
            .unwrap();

        assert_eq!(
            outcome_rx.recv().await.unwrap(),
            WatchOutcome::RebuildComplete {
                name: "api".to_string(),
                success: true,
            },
            "ShutdownStarted should have been dropped, not queued ahead"
        );
    }

    #[tokio::test]
    async fn a_dropped_runner_stops_the_watcher() {
        let (_signal_tx, signal_rx) = mpsc::unbounded_channel();
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, events) = broadcast::channel(16);
        let (outcome_tx, mut outcome_rx) = mpsc::unbounded_channel();
        let handle = {
            let (batcher_tx, _batcher_rx) = mpsc::unbounded_channel();
            spawn(
                signal_rx,
                cmd_tx,
                batcher_tx,
                std::collections::HashMap::new(),
                events,
                outcome_tx,
            )
        };

        drop(event_tx);

        // The watcher learns to stop by its outcome channel closing, so this
        // task must not hold the sender open after the runner is gone.
        assert!(outcome_rx.recv().await.is_none());
        assert!(handle.await.is_ok());
    }

    #[tokio::test]
    async fn lag_is_reported_rather_than_swallowed() {
        let (_signal_tx, signal_rx) = mpsc::unbounded_channel();
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, events) = broadcast::channel(2);
        let (outcome_tx, mut outcome_rx) = mpsc::unbounded_channel();

        // Overflow the channel before the task can drain it, so `recv`
        // reports `Lagged`.
        for i in 0..8 {
            event_tx
                .send(RunnerEvent::RebuildComplete {
                    name: format!("svc-{i}"),
                    success: true,
                })
                .unwrap();
        }
        let _handle = {
            let (batcher_tx, _batcher_rx) = mpsc::unbounded_channel();
            spawn(
                signal_rx,
                cmd_tx,
                batcher_tx,
                std::collections::HashMap::new(),
                events,
                outcome_tx,
            )
        };

        assert!(
            matches!(outcome_rx.recv().await, Some(WatchOutcome::Lagged(n)) if n > 0),
            "a lagged broadcast must surface as WatchOutcome::Lagged"
        );
    }
}
