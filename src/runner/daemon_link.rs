//! The runner's side of talking to the system-wide daemon.
//!
//! Both calls here are fire-and-forget by construction. The daemon is an
//! optional convenience — most users will never install one — so a project
//! must start and stop exactly as fast and exactly as reliably whether or
//! not anything is listening. Nothing in this file is awaited on the runner
//! task, which is also what keeps Ctrl+C responsive (see the "Shutdown
//! Responsiveness" rules in CLAUDE.md).

use super::{DaemonRegistration, Runner};
use crate::daemon::registry::ProjectEntry;
use std::path::PathBuf;

impl Runner {
    /// Announce this project to the daemon at `socket` once the API is up.
    ///
    /// Call before [`Runner::run`]. Without it the runner never contacts a
    /// daemon, which is the default and the behaviour of `--no-daemon`.
    pub fn enable_daemon_registration(&mut self, socket: PathBuf, profile: Option<String>) {
        self.daemon_registration = Some(DaemonRegistration { socket, profile });
    }

    /// Spawn a detached best-effort registration. No-op when registration
    /// wasn't enabled.
    pub(in crate::runner) fn register_with_daemon(&self) {
        let Some(registration) = &self.daemon_registration else {
            return;
        };
        let entry = ProjectEntry::new(
            self.base_dir.clone(),
            std::process::id(),
            registration.profile.clone(),
        );
        let socket = registration.socket.clone();
        let emitter = self.output_manager.clone_lifecycle_emitter();
        tokio::spawn(async move {
            if let Some(message) = crate::daemon::client::register_best_effort(socket, entry).await
            {
                emitter.debug_event(&message);
            }
        });
    }

    /// Spawn a detached best-effort deregistration. No-op when registration
    /// wasn't enabled.
    pub(in crate::runner) fn deregister_from_daemon(&self) {
        let Some(registration) = &self.daemon_registration else {
            return;
        };
        let socket = registration.socket.clone();
        let id = crate::daemon::registry::project_id(&self.base_dir);
        tokio::spawn(crate::daemon::client::deregister_best_effort(socket, id));
    }
}
