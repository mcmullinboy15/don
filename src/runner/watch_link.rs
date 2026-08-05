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
use crate::watch::{WatchOutcome, WatchSignal};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

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
                    if cmd_tx.send(command_for(signal)).is_err() {
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

/// The runner command a watch signal asks for.
fn command_for(signal: WatchSignal) -> RunnerCommand {
    match signal {
        WatchSignal::Rebuild { name } => RunnerCommand::Rebuild { name },
        WatchSignal::TaskRerun { name } => RunnerCommand::TaskRerun { name },
        WatchSignal::RebuildStale { name } => RunnerCommand::RebuildStale { name },
        WatchSignal::BuildGraphChanged { name } => RunnerCommand::BuildGraphChanged { name },
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
            RunnerCommand::BuildGraphChanged { name } => ("BuildGraphChanged", name),
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
            Case {
                name: "build graph changed",
                signal: WatchSignal::BuildGraphChanged {
                    name: "api".to_string(),
                },
                want: ("BuildGraphChanged", "api"),
            },
        ];

        for case in cases {
            let (signal_tx, signal_rx) = mpsc::unbounded_channel();
            let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
            let (_event_tx, events) = broadcast::channel(16);
            let (outcome_tx, _outcome_rx) = mpsc::unbounded_channel();
            let _handle = spawn(signal_rx, cmd_tx, events, outcome_tx);

            signal_tx.send(case.signal).unwrap();
            let got = cmd_rx.recv().await.unwrap();
            assert_eq!(describe(&got), case.want, "{}: wrong command", case.name);
        }
    }

    #[tokio::test]
    async fn only_the_two_completions_reach_the_watcher() {
        let (_signal_tx, signal_rx) = mpsc::unbounded_channel();
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, events) = broadcast::channel(16);
        let (outcome_tx, mut outcome_rx) = mpsc::unbounded_channel();
        let _handle = spawn(signal_rx, cmd_tx, events, outcome_tx);

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
        let handle = spawn(signal_rx, cmd_tx, events, outcome_tx);

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
        let _handle = spawn(signal_rx, cmd_tx, events, outcome_tx);

        assert!(
            matches!(outcome_rx.recv().await, Some(WatchOutcome::Lagged(n)) if n > 0),
            "a lagged broadcast must surface as WatchOutcome::Lagged"
        );
    }
}
