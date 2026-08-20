use super::service;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Maximum consecutive starts that never reached `Ready` before don stops
/// retrying, regardless of `on_failure`.
const MAX_STARTUP_FAILURES_BEFORE_GIVE_UP: u32 = 3;

/// A process that exits within this window of being started is treated as a
/// crash on launch (a likely crash loop) rather than a normal failure.
pub(crate) const RAPID_CRASH_WINDOW: Duration = Duration::from_secs(5);

/// Maximum number of back-to-back rapid crashes before don gives up
/// auto-restarting a service, regardless of `on_failure`. Two strikes: the
/// initial start plus one retry that also dies inside [`RAPID_CRASH_WINDOW`].
const MAX_RAPID_CRASHES: u32 = 2;

/// Update the rapid-crash streak after a non-clean process exit.
///
/// `lived` is how long the process ran since its last start (`None` when that
/// is unknown, treated as an immediate crash). Returns the new streak count
/// and whether don should give up instead of scheduling another auto-restart.
/// A process that ran at least [`RAPID_CRASH_WINDOW`] clears the streak — it
/// wasn't stuck in a tight crash loop.
fn rapid_crash_outcome(lived: Option<Duration>, prior: u32) -> (u32, bool) {
    let rapid = lived.map(|d| d < RAPID_CRASH_WINDOW).unwrap_or(true);
    if !rapid {
        return (0, false);
    }
    let count = prior.saturating_add(1);
    (count, count >= MAX_RAPID_CRASHES)
}

/// Why a service's supervisor is consulting the restart policy.
pub(crate) enum FailureKind {
    /// The start could not be prepared at all (build, download, ports).
    Prepare,
    /// The ready check failed, or the service failed before becoming ready.
    Ready,
    /// The health monitor saw a `Ready` service go unhealthy.
    Unhealthy,
    /// The process exited non-zero. `lived` is how long this spawn ran.
    Crash {
        lived: Option<Duration>,
        reached_ready: bool,
    },
}

/// What a failure means for whether the service starts again.
///
/// Produced by [`RestartPolicy::decide`] inside the supervisor — the place
/// every failure signal originates — and carried up to the scheduler, which
/// needs only enough of it to keep its own view honest.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PolicyOutcome {
    /// Nothing is scheduled: either the policy is `notify`, or this exit was
    /// expected.
    None,
    /// A restart is armed to fire after `backoff_secs`.
    RestartScheduled { attempt: u32, backoff_secs: u64 },
    /// Too many consecutive starts that never became ready.
    GaveUpStarting { attempts: u32 },
    /// Too many back-to-back crashes inside [`RAPID_CRASH_WINDOW`].
    GaveUpCrashing { rapid_crashes: u32 },
    /// A lazy service's launch failed. It returns to `Lazy` and its trigger
    /// re-arms, unless the crash ceiling tripped — then it stays `Failed`
    /// with the trigger un-armed, so a queued connection stops relaunching it.
    LazyRearm { give_up: bool, rapid_crashes: u32 },
}

impl PolicyOutcome {
    /// Whether the service is expected to come back up on its own. The
    /// scheduler folds this so a stack sitting in a backoff still counts as
    /// having work in flight.
    pub(crate) fn restart_pending(&self) -> bool {
        matches!(self, Self::RestartScheduled { .. })
    }
}

/// The restart counters a supervisor carries across its spawns, and the rules
/// that read them.
///
/// This is the whole of don's restart policy. It lives beside the process it
/// governs because every input it needs — did the prepare fail, did the ready
/// check pass, how long did this spawn live — is something the supervisor
/// observed itself.
pub(crate) struct RestartPolicy {
    on_failure: crate::config::OnFailure,
    /// Lazy services restart on a proxy connection rather than a timer, so
    /// their failures take the re-arm path instead of the backoff path.
    lazy_with_proxy: bool,
    /// Consecutive auto-restarts without the service recovering. Drives the
    /// backoff and the never-became-ready ceiling.
    attempts: u32,
    /// Consecutive crashes inside [`RAPID_CRASH_WINDOW`].
    rapid_crashes: u32,
}

impl RestartPolicy {
    pub(crate) fn new(on_failure: crate::config::OnFailure, lazy_with_proxy: bool) -> Self {
        Self {
            on_failure,
            lazy_with_proxy,
            attempts: 0,
            rapid_crashes: 0,
        }
    }

    /// The service reached `Ready`. Clears the backoff counter but *not* the
    /// rapid-crash streak: that is cleared only by a spawn that survives past
    /// the crash window, so a service that flaps Ready-then-dead still trips
    /// the ceiling.
    pub(crate) fn on_ready(&mut self) {
        self.attempts = 0;
    }

    /// Forget everything — the service was stopped, restarted by request, or
    /// exited cleanly, so the prior run of failures no longer counts.
    pub(crate) fn reset(&mut self) {
        self.attempts = 0;
        self.rapid_crashes = 0;
    }

    pub(crate) fn decide(&mut self, kind: FailureKind) -> PolicyOutcome {
        // Lazy launches are bounded by the crash ceiling rather than a
        // backoff: their retry is a connection, and a connection the dying
        // service never accepts stays queued and re-fires immediately.
        if self.lazy_with_proxy
            && let FailureKind::Ready | FailureKind::Crash { .. } = kind
        {
            let lived = match kind {
                FailureKind::Crash { lived, .. } => lived,
                _ => None,
            };
            let (rapid_crashes, give_up) = rapid_crash_outcome(lived, self.rapid_crashes);
            self.rapid_crashes = rapid_crashes;
            return PolicyOutcome::LazyRearm {
                give_up,
                rapid_crashes,
            };
        }

        if self.on_failure != crate::config::OnFailure::Restart {
            self.reset();
            return PolicyOutcome::None;
        }

        // A crash first faces the hard ceiling that no `on_failure` overrides.
        let limit_startup_failures = match kind {
            FailureKind::Crash {
                lived,
                reached_ready,
            } => {
                let (rapid_crashes, give_up) = rapid_crash_outcome(lived, self.rapid_crashes);
                self.rapid_crashes = rapid_crashes;
                if give_up {
                    return PolicyOutcome::GaveUpCrashing { rapid_crashes };
                }
                !reached_ready
            }
            // A start that never became ready is bounded; an already-healthy
            // service going unhealthy is not.
            FailureKind::Prepare | FailureKind::Ready => true,
            FailureKind::Unhealthy => false,
        };

        let attempt = self.attempts.saturating_add(1);
        self.attempts = attempt;
        if limit_startup_failures && attempt >= MAX_STARTUP_FAILURES_BEFORE_GIVE_UP {
            return PolicyOutcome::GaveUpStarting { attempts: attempt };
        }
        PolicyOutcome::RestartScheduled {
            attempt,
            backoff_secs: unhealthy_restart_backoff_secs(attempt),
        }
    }
}

/// Render an unexpected-exit lifecycle message from the reaped status.
/// Reports the exit code for normal exits, the signal number (and core
/// dump flag) for signal-killed processes, and a plain "no status" line
/// when the wait failed.
pub(crate) fn format_unexpected_exit(status: Option<std::process::ExitStatus>) -> String {
    use std::os::unix::process::ExitStatusExt;
    match status {
        Some(s) => {
            if let Some(code) = s.code() {
                format!("exited unexpectedly with status {code}")
            } else if let Some(sig) = s.signal() {
                let core = if s.core_dumped() {
                    " (core dumped)"
                } else {
                    ""
                };
                format!("exited unexpectedly: killed by signal {sig}{core}")
            } else {
                "exited unexpectedly (no status available)".to_string()
            }
        }
        None => "exited unexpectedly (could not reap exit status)".to_string(),
    }
}

/// Compute the wait before the next auto-restart of an Unhealthy service.
/// Doubles each attempt (1, 2, 4, 8, 16, 32, 60, 60, ...) up to a 60s cap.
/// `attempt` is 1-based — the first restart waits 1s.
pub(crate) fn unhealthy_restart_backoff_secs(attempt: u32) -> u64 {
    let exp = attempt.saturating_sub(1).min(6);
    (1u64 << exp).min(60)
}

/// Long-lived per-service health monitor. Spawned once a service reaches
/// `Ready` when `ready.monitor = true`. Polls `run_one_check` at
/// `monitor_interval` and reports each healthy/unhealthy transition to its
/// *supervisor*, which applies the restart policy before anything reaches the
/// scheduler. Exits when the cancel oneshot fires (sent or dropped) —
/// typically on stop/restart/process exit.
pub(crate) async fn run_health_monitor(
    ready: crate::config::ReadyCheck,
    health_tx: mpsc::UnboundedSender<bool>,
    mut cancel: oneshot::Receiver<()>,
) {
    let interval_str = ready.monitor_interval.as_str();
    // Both values were validated at config load; fall back to 1s if a
    // bad value somehow reaches here — panicking in this detached task
    // would silently orphan the monitor.
    let interval = crate::duration::parse_duration(interval_str)
        .unwrap_or_else(|_| std::time::Duration::from_secs(1));
    let unhealthy_after = ready.unhealthy_after.max(1);
    let mut consecutive_failures: u32 = 0;
    let mut currently_unhealthy = false;
    loop {
        tokio::select! {
            _ = &mut cancel => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let probe = service::run_one_check_with_config_timeout(&ready).await;
        match probe {
            Ok(()) => {
                consecutive_failures = 0;
                if currently_unhealthy {
                    currently_unhealthy = false;
                    let _ = health_tx.send(true);
                }
            }
            Err(_) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                if !currently_unhealthy && consecutive_failures >= unhealthy_after {
                    currently_unhealthy = true;
                    let _ = health_tx.send(false);
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::OnFailure;
    use tokio::sync::oneshot;

    #[test]
    fn rapid_crash_outcome_streak_and_give_up() {
        struct Case {
            name: &'static str,
            lived: Option<Duration>,
            prior: u32,
            expect_count: u32,
            expect_give_up: bool,
        }

        let just_under = RAPID_CRASH_WINDOW - Duration::from_millis(1);
        let cases = vec![
            Case {
                name: "first fast crash, unknown lifetime",
                lived: None,
                prior: 0,
                expect_count: 1,
                expect_give_up: false,
            },
            Case {
                name: "first fast crash",
                lived: Some(Duration::from_millis(200)),
                prior: 0,
                expect_count: 1,
                expect_give_up: false,
            },
            Case {
                name: "second fast crash hits the cap",
                lived: Some(Duration::from_millis(200)),
                prior: 1,
                expect_count: MAX_RAPID_CRASHES,
                expect_give_up: true,
            },
            Case {
                name: "just inside the window still counts",
                lived: Some(just_under),
                prior: 1,
                expect_count: 2,
                expect_give_up: true,
            },
            Case {
                name: "exactly at the window clears the streak",
                lived: Some(RAPID_CRASH_WINDOW),
                prior: 1,
                expect_count: 0,
                expect_give_up: false,
            },
            Case {
                name: "long-lived crash clears a large streak",
                lived: Some(Duration::from_secs(60)),
                prior: 5,
                expect_count: 0,
                expect_give_up: false,
            },
            Case {
                name: "unknown lifetime past the cap gives up",
                lived: None,
                prior: MAX_RAPID_CRASHES,
                expect_count: MAX_RAPID_CRASHES + 1,
                expect_give_up: true,
            },
        ];

        for case in cases {
            let (count, give_up) = rapid_crash_outcome(case.lived, case.prior);
            assert_eq!(count, case.expect_count, "{}: count", case.name);
            assert_eq!(give_up, case.expect_give_up, "{}: give_up", case.name);
        }
    }

    /// One failure against a fresh policy, per kind and configuration.
    #[test]
    fn first_failure_by_kind_and_policy() {
        struct Case {
            name: &'static str,
            on_failure: OnFailure,
            lazy: bool,
            kind: FailureKind,
            expect: PolicyOutcome,
        }

        let cases = vec![
            Case {
                name: "notify never schedules",
                on_failure: OnFailure::Notify,
                lazy: false,
                kind: FailureKind::Crash {
                    lived: Some(Duration::from_secs(60)),
                    reached_ready: true,
                },
                expect: PolicyOutcome::None,
            },
            Case {
                name: "a long-lived crash restarts after 1s",
                on_failure: OnFailure::Restart,
                lazy: false,
                kind: FailureKind::Crash {
                    lived: Some(Duration::from_secs(60)),
                    reached_ready: true,
                },
                expect: PolicyOutcome::RestartScheduled {
                    attempt: 1,
                    backoff_secs: 1,
                },
            },
            Case {
                name: "unhealthy restarts after 1s",
                on_failure: OnFailure::Restart,
                lazy: false,
                kind: FailureKind::Unhealthy,
                expect: PolicyOutcome::RestartScheduled {
                    attempt: 1,
                    backoff_secs: 1,
                },
            },
            Case {
                name: "a failed prepare restarts, under the startup ceiling",
                on_failure: OnFailure::Restart,
                lazy: false,
                kind: FailureKind::Prepare,
                expect: PolicyOutcome::RestartScheduled {
                    attempt: 1,
                    backoff_secs: 1,
                },
            },
            Case {
                name: "a lazy launch failure re-arms its trigger",
                on_failure: OnFailure::Notify,
                lazy: true,
                kind: FailureKind::Ready,
                expect: PolicyOutcome::LazyRearm {
                    give_up: false,
                    rapid_crashes: 1,
                },
            },
            Case {
                name: "a lazy service's unhealthy takes the ordinary path",
                on_failure: OnFailure::Restart,
                lazy: true,
                kind: FailureKind::Unhealthy,
                expect: PolicyOutcome::RestartScheduled {
                    attempt: 1,
                    backoff_secs: 1,
                },
            },
        ];

        for case in cases {
            let mut policy = RestartPolicy::new(case.on_failure, case.lazy);
            assert_eq!(policy.decide(case.kind), case.expect, "{}", case.name);
        }
    }

    /// The ceilings, and what clears the counters they read. Each case walks a
    /// sequence and asserts the *last* outcome.
    #[test]
    fn ceilings_and_what_resets_them() {
        /// Steps a supervisor can take between failures.
        enum Step {
            Fail(FailureKind),
            Ready,
            Reset,
        }
        struct Case {
            name: &'static str,
            on_failure: OnFailure,
            lazy: bool,
            steps: Vec<Step>,
            expect: PolicyOutcome,
        }

        let fast_crash = || {
            Step::Fail(FailureKind::Crash {
                lived: Some(Duration::from_millis(10)),
                reached_ready: false,
            })
        };
        let slow_crash = || {
            Step::Fail(FailureKind::Crash {
                lived: Some(Duration::from_secs(60)),
                reached_ready: true,
            })
        };
        // A service that came up, then died inside the crash window. Counts
        // toward the crash streak but not the never-became-ready ceiling.
        let fast_crash_after_ready = || {
            Step::Fail(FailureKind::Crash {
                lived: Some(Duration::from_millis(10)),
                reached_ready: true,
            })
        };

        let cases = vec![
            Case {
                name: "two fast crashes trip the crash ceiling",
                on_failure: OnFailure::Restart,
                lazy: false,
                steps: vec![fast_crash(), fast_crash()],
                expect: PolicyOutcome::GaveUpCrashing { rapid_crashes: 2 },
            },
            Case {
                // Without the long-lived crash in the middle, the second fast
                // one would be `GaveUpCrashing` — surviving the window is what
                // buys another chance.
                name: "a long-lived crash between them clears the crash streak",
                on_failure: OnFailure::Restart,
                lazy: false,
                steps: vec![
                    fast_crash_after_ready(),
                    slow_crash(),
                    fast_crash_after_ready(),
                ],
                expect: PolicyOutcome::RestartScheduled {
                    attempt: 3,
                    backoff_secs: 4,
                },
            },
            Case {
                name: "three starts that never became ready give up",
                on_failure: OnFailure::Restart,
                lazy: false,
                steps: vec![
                    Step::Fail(FailureKind::Ready),
                    Step::Fail(FailureKind::Ready),
                    Step::Fail(FailureKind::Ready),
                ],
                expect: PolicyOutcome::GaveUpStarting { attempts: 3 },
            },
            Case {
                name: "reaching ready clears the attempt count, so the ceiling recedes",
                on_failure: OnFailure::Restart,
                lazy: false,
                steps: vec![
                    Step::Fail(FailureKind::Ready),
                    Step::Fail(FailureKind::Ready),
                    Step::Ready,
                    Step::Fail(FailureKind::Ready),
                ],
                expect: PolicyOutcome::RestartScheduled {
                    attempt: 1,
                    backoff_secs: 1,
                },
            },
            Case {
                name: "reaching ready does NOT clear the crash streak",
                on_failure: OnFailure::Restart,
                lazy: false,
                steps: vec![fast_crash(), Step::Ready, fast_crash()],
                expect: PolicyOutcome::GaveUpCrashing { rapid_crashes: 2 },
            },
            Case {
                name: "an explicit reset clears the crash streak",
                on_failure: OnFailure::Restart,
                lazy: false,
                steps: vec![fast_crash(), Step::Reset, fast_crash()],
                expect: PolicyOutcome::RestartScheduled {
                    attempt: 1,
                    backoff_secs: 1,
                },
            },
            Case {
                name: "unhealthy alone never trips the startup ceiling",
                on_failure: OnFailure::Restart,
                lazy: false,
                steps: vec![
                    Step::Fail(FailureKind::Unhealthy),
                    Step::Fail(FailureKind::Unhealthy),
                    Step::Fail(FailureKind::Unhealthy),
                    Step::Fail(FailureKind::Unhealthy),
                ],
                expect: PolicyOutcome::RestartScheduled {
                    attempt: 4,
                    backoff_secs: 8,
                },
            },
            Case {
                name: "a lazy service stops re-arming after two fast launches",
                on_failure: OnFailure::Notify,
                lazy: true,
                steps: vec![fast_crash(), fast_crash()],
                expect: PolicyOutcome::LazyRearm {
                    give_up: true,
                    rapid_crashes: 2,
                },
            },
        ];

        for case in cases {
            let mut policy = RestartPolicy::new(case.on_failure, case.lazy);
            let mut last = PolicyOutcome::None;
            for step in case.steps {
                match step {
                    Step::Fail(kind) => last = policy.decide(kind),
                    Step::Ready => policy.on_ready(),
                    Step::Reset => policy.reset(),
                }
            }
            assert_eq!(last, case.expect, "{}", case.name);
        }
    }

    #[test]
    fn unhealthy_restart_backoff_table() {
        struct Case {
            attempt: u32,
            want_secs: u64,
        }
        let cases = [
            Case {
                attempt: 1,
                want_secs: 1,
            },
            Case {
                attempt: 2,
                want_secs: 2,
            },
            Case {
                attempt: 3,
                want_secs: 4,
            },
            Case {
                attempt: 4,
                want_secs: 8,
            },
            Case {
                attempt: 5,
                want_secs: 16,
            },
            Case {
                attempt: 6,
                want_secs: 32,
            },
            // Cap kicks in at attempt 7 (1<<6 = 64 → clamped to 60).
            Case {
                attempt: 7,
                want_secs: 60,
            },
            Case {
                attempt: 12,
                want_secs: 60,
            },
            Case {
                attempt: u32::MAX,
                want_secs: 60,
            },
            // Defensive: a 0 attempt shouldn't blow up — saturating_sub keeps
            // exp at 0 and the wait at 1s.
            Case {
                attempt: 0,
                want_secs: 1,
            },
        ];
        for c in cases {
            assert_eq!(
                unhealthy_restart_backoff_secs(c.attempt),
                c.want_secs,
                "attempt {}",
                c.attempt
            );
        }
    }

    /// Drive `run_health_monitor` against a controllable TCP target and
    /// verify it reports the right healthy/unhealthy sequence.
    ///
    /// Strategy: bind a real `TcpListener`, point the monitor at its port
    /// with a tiny interval, then close/rebind to flip health. We assert
    /// only the sequence of `healthy` flags, not their timing — the loop
    /// is naturally jittery and exact timings would make the test flaky.
    #[tokio::test(flavor = "current_thread", start_paused = false)]
    async fn run_health_monitor_emits_unhealthy_then_recovers() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let ready = crate::config::ReadyCheck {
            exec: None,
            tcp: Some(format!("127.0.0.1:{port}")),
            http: None,
            interval: "1s".to_string(),
            retries: 1,
            timeout: "100ms".to_string(),
            monitor: true,
            monitor_interval: "20ms".to_string(),
            unhealthy_after: 2,
        };

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let monitor = tokio::spawn(run_health_monitor(ready, cmd_tx, cancel_rx));

        // Listener is up — the monitor sees only successes and reports nothing.
        // Drain for ~120ms to confirm silence on the happy path.
        let no_msg =
            tokio::time::timeout(std::time::Duration::from_millis(120), cmd_rx.recv()).await;
        assert!(
            no_msg.is_err(),
            "monitor should not emit while target is healthy"
        );

        // Drop the listener so connect() starts failing. After
        // unhealthy_after=2 consecutive failures, expect healthy=false.
        drop(listener);
        let msg = tokio::time::timeout(std::time::Duration::from_millis(500), cmd_rx.recv())
            .await
            .expect("timeout waiting for unhealthy event")
            .expect("monitor channel closed unexpectedly");
        assert!(!msg, "expected unhealthy event first");

        // Rebind so probes pass again — expect a recovery event.
        //
        // The port had to be genuinely released to make the monitor fail, and
        // the kernel can hand it to any other process in that window, so this
        // bind can lose a race the test can't prevent. Retry briefly: a real
        // regression keeps the port free and binds on the first attempt, while
        // a thief that never leaves fails the test rather than hiding.
        let restored = {
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(300);
            loop {
                match TcpListener::bind(format!("127.0.0.1:{port}")).await {
                    Ok(listener) => break Ok(listener),
                    Err(e) if std::time::Instant::now() < deadline => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        let _ = e;
                    }
                    Err(e) => break Err(e),
                }
            }
        };
        let _restored = restored.expect("another process took the monitored port mid-test");
        let msg = tokio::time::timeout(std::time::Duration::from_millis(500), cmd_rx.recv())
            .await
            .expect("timeout waiting for recovery event")
            .expect("monitor channel closed unexpectedly");
        assert!(msg, "expected recovery event after rebind");

        // Tear the monitor down cleanly so the test exits.
        let _ = cancel_tx.send(());
        let _ = tokio::time::timeout(std::time::Duration::from_millis(200), monitor).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_health_monitor_exits_on_cancel() {
        let ready = crate::config::ReadyCheck {
            exec: None,
            tcp: Some("127.0.0.1:1".to_string()),
            http: None,
            interval: "10s".to_string(),
            retries: 1,
            timeout: "100ms".to_string(),
            monitor: true,
            monitor_interval: "10s".to_string(),
            unhealthy_after: 5,
        };
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let monitor = tokio::spawn(run_health_monitor(ready, cmd_tx, cancel_rx));
        // Long monitor_interval — without cancel, the join would hang.
        // Cancel and confirm the task returns within a short window.
        let _ = cancel_tx.send(());
        let result = tokio::time::timeout(std::time::Duration::from_millis(200), monitor).await;
        assert!(result.is_ok(), "monitor should exit promptly after cancel");
    }
}
