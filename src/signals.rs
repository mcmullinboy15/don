//! Process-level shutdown signal handling.
//!
//! Lives at the crate root rather than under `runner` because it is not
//! orchestration: the daemon needs it too, and having it inside `runner` was
//! the sole reason `daemon` depended on `runner` at all.

use std::sync::atomic::{AtomicU8, Ordering};
use tokio::sync::mpsc;

/// Signal counter: 0 = running, 1 = graceful shutdown, 2 = force shutdown.
static SIGNAL_COUNT: AtomicU8 = AtomicU8::new(0);

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
}

pub(crate) fn shutdown_requested() -> bool {
    signal_count() >= 1
}

pub(crate) fn force_shutdown_requested() -> bool {
    signal_count() >= 2
}
