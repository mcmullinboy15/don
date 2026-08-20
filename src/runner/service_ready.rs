//! Folding a wired start into the projections that span process generations.
//!
//! The phase, the ready check and everything that follows from either belong
//! to the supervisor — it resolved the probe against its own proxy and docker
//! state, so it is the only thing that can say what was actually probed.
//!
//! What lands here is the part no single spawn can own: where a service can be
//! reached (`endpoints`) and the runtime ports manifest. Both outlive
//! individual processes, so they belong to the thing that outlives them.

use super::Runner;

impl Runner {
    /// Fold a wired start into the cross-process projections.
    ///
    /// Deliberately says nothing about phase. The supervisor published
    /// `Running` and its pid together before this report was sent, and the two
    /// travel on different channels — so anything here that read a phase would
    /// be reading one that may not have arrived yet. That race is why the
    /// ready narration moved.
    pub(in crate::runner) async fn handle_service_wired(
        &mut self,
        name: &str,
        wired: super::service_supervisor::ServiceWired,
    ) {
        let super::service_supervisor::ServiceWired {
            identity,
            pgid: spawned_pgid,
            docker_port_bindings,
            proxy_backend_env,
        } = wired;
        for binding in docker_port_bindings
            .iter()
            .filter(|binding| binding.used_fallback())
        {
            if binding.configured_host_port == 0 {
                self.output_manager.service_event(
                    name,
                    &format!(
                        "allocated Docker port {} at {}",
                        binding.configured,
                        binding.connect_addr()
                    ),
                );
            } else {
                self.output_manager.service_event(
                    name,
                    &format!(
                        "Docker port {} is unavailable; using {}",
                        binding.configured,
                        binding.connect_addr()
                    ),
                );
            }
        }

        self.fold_service_custody(
            name,
            identity,
            spawned_pgid,
            docker_port_bindings,
            proxy_backend_env,
        );
        if let Some(pgid) = spawned_pgid {
            self.output_manager
                .service_debug_event(name, &format!("spawned pid={pgid}"));
        }
    }
}
