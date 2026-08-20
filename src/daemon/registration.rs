//! Announcing a running project to the system-wide daemon.
//!
//! This is the daemon's side of the relationship, and it is deliberately the
//! *only* side. Whether a project should appear in some daemon's project list
//! is a deployment policy — `don start --no-daemon` turns it off, most users
//! never install a daemon at all — and the runner has no business knowing a
//! daemon exists. So the runner broadcasts what happened and this watches for
//! the two moments that matter:
//!
//! Registration happens when the caller says so — it binds the API socket, so
//! it is the thing that knows when there is something worth announcing.
//! Withdrawal watches for [`RunnerEvent::ShutdownStarted`].
//!
//! Everything here is best-effort and nothing is ever awaited by the runner.
//! A daemon that is absent, slow, or wedged must not cost a project a single
//! millisecond of startup or of Ctrl+C. When a withdrawal doesn't land, the
//! daemon prunes unreachable projects on its next read, so the worst case is
//! a stale row for a few seconds.

use super::registry::{ProjectEntry, project_id};
use crate::output::LifecycleEmitter;
use crate::runner::RunnerEvent;
use std::path::PathBuf;
use tokio::sync::broadcast;

/// Announce this project to the daemon at `socket`, and withdraw on shutdown.
///
/// Call once the API socket is bound and serving.
///
/// `base_dir` must be the runner's *canonical* root ([`crate::Runner::base_dir`]) —
/// the daemon keys projects by a hash of it, so registering under the
/// as-passed path and deregistering under the canonical one (or vice versa)
/// leaves a row behind forever.
///
/// Returns immediately; the watcher runs detached until shutdown or until the
/// runner drops its event sender.
pub fn spawn(
    mut events: broadcast::Receiver<RunnerEvent>,
    socket: PathBuf,
    base_dir: PathBuf,
    profile: Option<String>,
    emitter: LifecycleEmitter,
) {
    let entry = ProjectEntry::new(base_dir.clone(), std::process::id(), profile);
    tokio::spawn(async move {
        if let Some(message) = super::client::register_best_effort(socket.clone(), entry).await {
            emitter.debug_event(&message);
        }
        loop {
            let event = match events.recv().await {
                Ok(event) => event,
                // Both events we care about fire exactly once and far apart,
                // so lagging behind a burst of state changes never loses one
                // permanently — keep reading.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            };
            if matches!(event, RunnerEvent::ShutdownStarted) {
                super::client::deregister_best_effort(socket.clone(), project_id(&base_dir)).await;
                return;
            }
        }
    });
}
