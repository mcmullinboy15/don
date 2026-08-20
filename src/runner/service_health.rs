//! What is left of the scheduler's side of a service failing.
//!
//! The *decisions* — how long to back off, when to give up, whether a lazy
//! service re-arms — belong to the supervisor, which is where every input they
//! read is observed (see [`crate::process::health::RestartPolicy`]). The
//! *phase* they land in belongs there too now: a supervisor publishes
//! `Failed`, `Lazy` or `Stopped` itself, beside the reap that decided it.
//!
//! What remains here is the projections a scheduler owns and a supervisor does
//! not: where a service can be reached, the runtime ports manifest, and the
//! follow sinks a client is streaming from.

use super::Runner;

impl Runner {
    /// A service's process is gone.
    ///
    /// The supervisor has already reaped, narrated the death, decided whether
    /// it starts again, and published the phase that followed. Ending the
    /// projections that outlived the process is the scheduler's part: they
    /// span process generations, so nothing per-spawn can own them.
    pub(in crate::runner) async fn handle_service_exited(&mut self, name: &str) {
        if !self.services.contains_key(name) {
            return;
        }
        self.clear_service_custody(name);
        if let Some(writer) = self.output_manager.service_writer(name) {
            writer.close_follow_sinks().await;
        }
    }
}
