//! Process-level shutdown signal handling.
//!
//! Lives at the crate root rather than under `runner` because it is not
//! orchestration: the daemon needs it too, and having it inside `runner` was
//! the sole reason `daemon` depended on `runner` at all.

use std::sync::atomic::{AtomicU8, Ordering};
use tokio::sync::{mpsc, watch};

/// Signal counter: 0 = running, 1 = graceful shutdown, 2 = force shutdown.
static SIGNAL_COUNT: AtomicU8 = AtomicU8::new(0);

/// Force-shutdown as something you can *wait* on, not only poll.
///
/// A second Ctrl+C has to reach whoever is holding a process, mid-graceful-
/// stop, so they can stop waiting out the grace period and kill it. The
/// scheduler used to do that on their behalf, polling this counter every
/// 100ms and signalling process groups it read out of the snapshot — which
/// is the one place it still touched a pid.
///
/// Lazily created rather than handed out at startup: the daemon, the tests
/// and `don` itself all reach for it, and only some of them install signal
/// handlers.
static FORCE: std::sync::LazyLock<(watch::Sender<bool>, watch::Receiver<bool>)> =
    std::sync::LazyLock::new(|| watch::channel(false));

/// A receiver that flips to `true` when force-shutdown is requested — by a
/// second signal or by `POST /shutdown?force=true`.
pub(crate) fn force_watch() -> watch::Receiver<bool> {
    FORCE.1.clone()
}

/// Publish the current escalation to anyone waiting on [`force_watch`].
fn publish_force() {
    if force_shutdown_requested() {
        let _ = FORCE.0.send(true);
    }
}

/// Install signal handlers for SIGINT and SIGTERM.
///
/// Returns a receiver that gets a message on each signal. Pass this to `Runner::new()`.
/// First signal triggers graceful shutdown. Second signal sets the force-shutdown flag
/// checked by shutdown helpers in this module.
pub async fn install_signal_handlers() -> Result<mpsc::Receiver<()>, std::io::Error> {
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let (tx, rx) = mpsc::channel(2);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = sigint.recv() => {},
                _ = sigterm.recv() => {},
            }

            let prev = SIGNAL_COUNT.fetch_add(1, Ordering::SeqCst);
            // Wake anyone parked on the escalation before the runner is even
            // told: a supervisor waiting out a grace period is exactly who
            // this is for.
            publish_force();
            // Notify the runner. If the channel is full or closed, that's fine.
            let _ = tx.try_send(());

            if prev >= 1 {
                // Second signal — force flag is set via SIGNAL_COUNT.
                break;
            }
        }
    });

    Ok(rx)
}

/// Current process-level shutdown signal count.
///
/// `0` means no shutdown signal has been seen, `1` means graceful shutdown
/// has been requested, and `2+` means force-exit escalation has been
/// requested. This is intentionally process-global so outer supervisors can
/// make progress even if the runner task wedges.
pub fn signal_count() -> u8 {
    SIGNAL_COUNT.load(Ordering::SeqCst)
}

/// Escalate to force-shutdown as if a second signal had arrived.
///
/// The API's `POST /shutdown?force=true` lands here: attached clients run
/// in raw mode, so their Ctrl+C arrives as a key event and goes over the
/// socket — this gives that path the same escalation a second SIGINT gets.
/// `fetch_max` so it never *un*-escalates, and works whether or not a
/// graceful shutdown is already in flight.
pub(crate) fn request_force_shutdown() {
    SIGNAL_COUNT.fetch_max(2, Ordering::SeqCst);
    publish_force();
}

pub(crate) fn shutdown_requested() -> bool {
    signal_count() >= 1
}

pub(crate) fn force_shutdown_requested() -> bool {
    signal_count() >= 2
}
