//! Announcing a running project to the system-wide daemon.
//!
//! This is the daemon's side of the relationship, and it is deliberately the
//! *only* side. Whether a project should appear in some daemon's project list
//! is a deployment policy — `don start --no-daemon` turns it off, most users
//! never install a daemon at all — and the runner has no business knowing a
//! daemon exists. So the runner broadcasts what happened and this watches for
//! the two moments that matter:
//!
//! - [`RunnerEvent::ApiListening`] — the socket a daemon would proxy to now
//!   exists, so there is something worth announcing.
//! - [`RunnerEvent::ShutdownStarted`] — withdraw, as early as possible.
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

/// Watch `events` and keep the daemon at `socket` informed about this project.
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
    tokio::spawn(async move {
        loop {
            let event = match events.recv().await {
                Ok(event) => event,
                // Both events we care about fire exactly once and far apart,
                // so lagging behind a burst of state changes never loses one
                // permanently — keep reading.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            };
            match event {
                RunnerEvent::ApiListening { .. } => {
                    let entry =
                        ProjectEntry::new(base_dir.clone(), std::process::id(), profile.clone());
                    if let Some(message) =
                        super::client::register_best_effort(socket.clone(), entry).await
                    {
                        emitter.debug_event(&message);
                    }
                }
                RunnerEvent::ShutdownStarted => {
                    super::client::deregister_best_effort(socket.clone(), project_id(&base_dir))
                        .await;
                    return;
                }
                _ => {}
            }
        }
    });
}
