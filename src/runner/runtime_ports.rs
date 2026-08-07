//! Runtime port publication, references, and ready-check resolution.

use super::{CommandError, Runner};
use crate::config::ReadyCheck;
use crate::output::LifecycleEmitter;
use crate::ports::{DockerPort, PortManifest, ProxyPort, ServicePorts};
use crate::process::ready::{port_replacements_for, resolve_ready_check};
use crate::proxy::ProxyBindingMode;
use std::collections::{BTreeMap, HashMap, HashSet};
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
    /// Render `$(service.key)` references in inline environment values.
    pub(in crate::runner) fn render_runtime_env(
        &self,
        process_name: &str,
        env: &mut HashMap<String, String>,
    ) -> Result<(), CommandError> {
        let references = self.runtime_port_references();
        // Only tokens naming a known service are treated as runtime references;
        // this keeps shell-style `$(...)` values in inline env untouched.
        let known_services: HashSet<String> = self.services.keys().cloned().collect();
        for (key, value) in env.iter_mut() {
            *value =
                super::env_refs::render(value, &references, &known_services).map_err(|error| {
                    CommandError::Failed {
                        name: process_name.to_string(),
                        message: format!("invalid runtime port reference in env.{key}: {error}"),
                    }
                })?;
        }
        Ok(())
    }

    /// Resolve a service ready check against its actual public runtime
    /// ports, from the runner's shadows — for `don status -v` and the ready
    /// lifecycle line. The authoritative resolution for the check that
    /// actually runs happens in the service supervisor, over its live proxy
    /// and docker state, via [`resolve_ready_check`]; both call the same
    /// function, so they cannot drift.
    pub(in crate::runner) fn effective_ready_check(
        &self,
        name: &str,
        resolved: &crate::config::ResolvedService,
    ) -> Option<ReadyCheck> {
        resolve_ready_check(
            resolved.ready.as_ref(),
            &resolved.env,
            &self.runtime_backend_env(name),
            &self.runtime_public_env(name),
            &self.ready_port_replacements(name),
        )
    }

    /// Queue a rewrite of `.don/ports.json` from the runner's current live
    /// bindings. The snapshot is built here (cheap) and the blocking write is
    /// performed by the manifest-writer task.
    pub(in crate::runner) fn refresh_runtime_port_manifest(&self) {
        if let Some(tx) = &self.manifest_writer_tx {
            let _ = tx.send(ManifestWrite::Update(Box::new(self.port_manifest())));
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

    fn runtime_port_references(&self) -> HashMap<String, String> {
        let mut references = HashMap::new();
        for (service_name, runtime) in &self.services {
            // A proxy is the outer public endpoint when a service declares
            // both proxy and Docker mappings, so insert it last.
            if runtime.handle_identity == Some(super::ServiceHandleIdentity::Docker) {
                extend_service_references(
                    &mut references,
                    service_name,
                    crate::docker::env_reference_values(&runtime.docker_port_bindings),
                );
            }
            if let Some(proxy) = runtime.proxy_view.as_ref() {
                extend_service_references(
                    &mut references,
                    service_name,
                    proxy.env_reference_values(),
                );
            }
        }
        references
    }

    /// Env-mode proxy backend vars, e.g. `{"PORT": "49152"}` — the ephemeral
    /// port the service itself was told to bind, as distinct from Don's public
    /// listener.
    fn runtime_backend_env(&self, name: &str) -> HashMap<String, String> {
        self.services
            .get(name)
            .and_then(|runtime| runtime.proxy_view.as_ref())
            .map(|proxy| proxy.backend_env.clone())
            .unwrap_or_default()
    }

    fn runtime_public_env(&self, name: &str) -> HashMap<String, String> {
        let mut env = HashMap::new();
        let Some(runtime) = self.services.get(name) else {
            return env;
        };
        if runtime.handle_identity == Some(super::ServiceHandleIdentity::Docker) {
            env.extend(crate::docker::public_env_vars(
                &runtime.docker_port_bindings,
            ));
        }
        if let Some(proxy) = runtime.proxy_view.as_ref() {
            env.extend(proxy.public_env_vars());
        }
        env
    }

    fn ready_port_replacements(&self, name: &str) -> HashMap<u16, u16> {
        let Some(runtime) = self.services.get(name) else {
            return HashMap::new();
        };
        let proxy_bindings = runtime
            .proxy_view
            .as_ref()
            .map(|view| view.bindings.as_slice())
            .unwrap_or(&[]);
        let docker_bindings =
            if runtime.handle_identity == Some(super::ServiceHandleIdentity::Docker) {
                runtime.docker_port_bindings.as_slice()
            } else {
                &[]
            };
        port_replacements_for(proxy_bindings, docker_bindings)
    }

    fn port_manifest(&self) -> PortManifest {
        let mut services = BTreeMap::new();
        for (name, runtime) in &self.services {
            let proxy: Vec<ProxyPort> = runtime
                .proxy_view
                .as_ref()
                .map(|proxy| {
                    proxy
                        .bindings
                        .iter()
                        .map(|binding| {
                            let (mode, env, target) = match &binding.mode {
                                ProxyBindingMode::Env { env_name } => {
                                    ("env".to_string(), Some(env_name.clone()), None)
                                }
                                ProxyBindingMode::Forward { target } => {
                                    ("forward".to_string(), None, Some(target.to_string()))
                                }
                                ProxyBindingMode::Listenfd => ("listenfd".to_string(), None, None),
                            };
                            ProxyPort {
                                configured_addr: binding.configured_addr.clone(),
                                bound_addr: binding.bound_addr.to_string(),
                                mode,
                                env,
                                target,
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            let docker = match runtime.handle_identity {
                Some(super::ServiceHandleIdentity::Docker) => runtime
                    .docker_port_bindings
                    .iter()
                    .map(|binding| DockerPort {
                        configured: binding.configured.clone(),
                        host_addr: binding.host_addr().to_string(),
                        container_port: binding.container_port.to_string(),
                        protocol: binding.protocol.clone(),
                    })
                    .collect(),
                _ => Vec::new(),
            };

            if !proxy.is_empty() || !docker.is_empty() {
                services.insert(name.clone(), ServicePorts { proxy, docker });
            }
        }
        PortManifest {
            version: 1,
            generated_at_unix_secs: 0,
            services,
        }
    }
}

fn extend_service_references(
    references: &mut HashMap<String, String>,
    service_name: &str,
    values: HashMap<String, String>,
) {
    for (key, value) in values {
        references.insert(format!("{service_name}.{key}"), value);
    }
}
