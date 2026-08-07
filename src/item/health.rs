use super::service_process as service;
use tokio::sync::{mpsc, oneshot};

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
/// `monitor_interval` and reports state transitions back to the runner via
/// `RunnerInternalCommand::ServiceHealthChanged`. Exits when the cancel oneshot
/// fires (sent or dropped) — typically on stop/restart/process exit.
pub(crate) async fn run_health_monitor(
    name: String,
    ready: crate::config::ReadyCheck,
    report_tx: mpsc::UnboundedSender<super::ItemReport>,
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
                    let _ = report_tx.send(super::ItemReport::HealthChanged {
                        name: name.clone(),
                        healthy: true,
                    });
                }
            }
            Err(_) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                if !currently_unhealthy && consecutive_failures >= unhealthy_after {
                    currently_unhealthy = true;
                    let _ = report_tx.send(super::ItemReport::HealthChanged {
                        name: name.clone(),
                        healthy: false,
                    });
                }
            }
        }
    }
}
