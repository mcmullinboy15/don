//! The `.don/ports.json` manifest writer, and the two custody funnels that
//! keep the runner's shadow, the endpoint projection and the manifest in step.

use super::Runner;
use crate::output::LifecycleEmitter;
use crate::ports::PortManifest;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A unit of work for the serialized runtime-port-manifest writer task.
pub(in crate::runner) enum ManifestWrite {
    /// Persist the given manifest snapshot.
    Update(Box<PortManifest>),
    /// Remove the manifest file.
    Remove,
}

/// Spawn the background task that owns all `.don/ports.json` filesystem I/O.
///
/// The runner builds manifest snapshots on its command loop (cheap, in-memory)
/// and hands them off over this channel, so the blocking serialize + rename
/// never runs on the runner task. Messages are processed in FIFO order, so the
/// last one sent wins — including the final [`ManifestWrite::Remove`] emitted
/// at shutdown.
pub(in crate::runner) fn spawn_manifest_writer(
    base_dir: PathBuf,
    emitter: LifecycleEmitter,
) -> (mpsc::UnboundedSender<ManifestWrite>, JoinHandle<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel::<ManifestWrite>();
    let handle = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let base_dir = base_dir.clone();
            let result = tokio::task::spawn_blocking(move || match message {
                ManifestWrite::Update(manifest) => {
                    crate::ports::write_manifest(&base_dir, *manifest)
                }
                ManifestWrite::Remove => crate::ports::remove_manifest(&base_dir),
            })
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    emitter.lifecycle_event(&format!("failed to update runtime ports: {error}"));
                }
                Err(join_error) => {
                    emitter.lifecycle_event(&format!("runtime ports writer failed: {join_error}"));
                }
            }
        }
    });
    (tx, handle)
}

impl Runner {
    /// This service's ready check, resolved against published endpoints —
    /// for `don status -v` and the ready lifecycle line.
    ///
    /// The authoritative resolution for the check that actually *runs* happens
    /// in the service supervisor over its own live proxy and docker state.
    /// Both go through [`crate::process::ready::resolve_ready_check`], so they
    /// cannot drift.
    pub(in crate::runner) fn endpoint_ready_check(
        &self,
        name: &str,
        resolved: &crate::config::ResolvedService,
    ) -> Option<crate::config::ReadyCheck> {
        crate::endpoints::effective_ready_check(&self.endpoints.snapshot(), name, resolved)
    }

    /// Fold a wired spawn's custody: the shadow, the endpoint projection and
    /// the port manifest, in one place so the three cannot drift.
    ///
    /// There is exactly one other custody funnel,
    /// [`clear_service_custody`](Self::clear_service_custody). Setting
    /// Recording custody anywhere else means the endpoint projection and the
    /// state projection disagree about whether a container is live — which
    /// shows up as a peer's `$(db.PORT)` resolving when it shouldn't, or not
    /// resolving when it should.
    pub(in crate::runner) fn fold_service_custody(
        &mut self,
        name: &str,
        identity: super::ServiceHandleIdentity,
        pgid: Option<i32>,
        docker_port_bindings: Vec<crate::docker::DockerPortBinding>,
        proxy_backend_env: Option<std::collections::HashMap<String, String>>,
    ) {
        let docker_live = identity == super::ServiceHandleIdentity::Docker;
        self.endpoints.publish_wired(
            name,
            docker_port_bindings.clone(),
            proxy_backend_env.clone(),
            docker_live,
        );
        // The projection is where custody is recorded, not a copy of where it
        // is recorded. Published here rather than on the state transition,
        // because a wire can land while the service is already `Running` and
        // `set_service_state` would no-op.
        self.state.set_service_runtime(
            name,
            Some(crate::state_store::ServiceRuntime {
                pid: pgid,
                docker: docker_live,
                docker_ports: crate::docker::describe_port_bindings(&docker_port_bindings),
            }),
        );
        if let Some(rs) = self.services.get_mut(name) {
            // Refresh the backend-env shadow: a restart reallocates ephemeral
            // backend ports, and the status path's `${PORT}` display must
            // resolve to the port this spawn was told.
            if let (Some(view), Some(backend_env)) = (rs.proxy_view.as_mut(), proxy_backend_env) {
                view.backend_env = backend_env;
            }
        }
        self.refresh_runtime_port_manifest();
    }

    /// Custody ended. Docker *bindings* are retained so a restart can request
    /// the same host ports, but the service stops being reachable — so its
    /// references stop resolving and it leaves the port manifest.
    pub(in crate::runner) fn clear_service_custody(&mut self, name: &str) {
        self.endpoints.clear_custody(name);
        self.state.set_service_runtime(name, None);
        self.refresh_runtime_port_manifest();
    }

    /// What this service's supervisor currently holds, read from the
    /// projection the fold publishes. The scheduler keeps no second copy —
    /// this *is* the record.
    pub(in crate::runner) fn service_runtime(
        &self,
        name: &str,
    ) -> Option<crate::state_store::ServiceRuntime> {
        self.state
            .current()
            .processes
            .iter()
            .find_map(|status| match status {
                crate::state_store::ProcessStatus::Service {
                    name: process_name,
                    runtime,
                    ..
                } if process_name == name => runtime.clone(),
                _ => None,
            })
    }

    /// The running task's process group id, if it has one.
    pub(in crate::runner) fn task_pid(&self, name: &str) -> Option<i32> {
        self.state
            .current()
            .processes
            .iter()
            .find_map(|status| match status {
                crate::state_store::ProcessStatus::Task {
                    name: process_name,
                    pid,
                    ..
                } if process_name == name => *pid,
                _ => None,
            })
    }

    /// Queue a rewrite of `.don/ports.json` from the runner's current live
    /// bindings. The snapshot is built here (cheap) and the blocking write is
    /// performed by the manifest-writer task.
    pub(in crate::runner) fn refresh_runtime_port_manifest(&self) {
        if let Some(tx) = &self.manifest_writer_tx {
            let manifest = crate::endpoints::port_manifest(&self.endpoints.snapshot());
            let _ = tx.send(ManifestWrite::Update(Box::new(manifest)));
        }
    }

    /// Queue removal of `.don/ports.json`, then wait for the manifest-writer
    /// task to drain every queued write and the remove before returning. Called
    /// on the shutdown paths so callers can rely on the file being gone once
    /// the runner has stopped.
    pub(in crate::runner) async fn finish_runtime_port_manifest(&mut self) {
        if let Some(tx) = self.manifest_writer_tx.take() {
            let _ = tx.send(ManifestWrite::Remove);
            // Dropping the sender lets the writer's recv loop terminate once
            // the queued messages (including the Remove) are processed.
            drop(tx);
        }
        if let Some(handle) = self.manifest_writer_handle.take() {
            let _ = handle.await;
        }
    }
}
