//! Where every service can be reached, published by the runner and read by
//! whatever is about to start something.
//!
//! `$(other_service.PORT)` in an inline env value is a question about a peer's
//! *public endpoint*. The runner used to answer it because it shadowed every
//! service's proxy bindings and docker port mappings — but supervisors produce
//! both, and the answer is a pure function of them. So the runner's copy was
//! never the source of truth, only the place it happened to be reachable from.
//!
//! This module is that answer, projected, in the same shape as
//! [`crate::state_store`]: an [`EndpointWriter`] the runner owns (not `Clone`)
//! and an [`EndpointReader`] anyone can hold (`Clone`, reads only). A
//! supervisor about to start its process renders its own env from a snapshot,
//! with no round trip and no dependency on `crate::runner`.
//!
//! # What a snapshot carries
//!
//! Per service: its proxy bindings, its current spawn's env-mode backend vars,
//! its last known docker port bindings, and whether it currently holds a
//! docker handle. Plus two precomputed views, because every reader wants them
//! and the rules for building them must live in exactly one place:
//!
//! - `references` — the flat `"service.KEY" -> value` map that
//!   [`crate::process::env_refs::render`] takes.
//! - `known_services` — the set of names that count as runtime references at
//!   all, so a shell-style `$(git rev-parse HEAD)` is left alone.
//!
//! # Two rules that are load-bearing
//!
//! **Proxy wins.** Docker values are laid down first and proxy values second,
//! so a service declaring both resolves to its proxy — the outer public
//! endpoint a caller should be talking to.
//!
//! **Docker references require a live handle.** A stopped docker service's
//! `$(db.PORT)` deliberately does not resolve; the dependent's start fails
//! rather than silently connecting to a port nothing is listening on. Proxy
//! bindings are the opposite: they exist from `Runner::new`, before any
//! supervisor, and outlive every process generation — which is what lets a
//! service resolve `$(daemon.addr)` for a dependency that has not started yet.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::watch;

use crate::command::CommandError;
use crate::config::{ReadyCheck, ResolvedService};
use crate::docker::DockerPortBinding;
use crate::proxy::ProxyBinding;

/// Where one service can be reached.
#[derive(Debug, Clone, Default)]
pub(crate) struct ServiceEndpoints {
    /// Proxy binding metadata in declaration order. Bound in `Runner::new`
    /// before any supervisor exists, and never rewritten — the listeners span
    /// process generations.
    pub(crate) proxy: Vec<ProxyBinding>,
    /// The env-mode backend vars this service's current spawn was launched
    /// with, e.g. `{"PORT": "49152"}`. Reallocated on every restart, and never
    /// published as a reference — it is the private port behind the proxy.
    pub(crate) backend_env: HashMap<String, String>,
    /// Docker host-port mappings from the most recent wire. Retained while the
    /// container is down so a restart can request the same fallback ports.
    pub(crate) docker: Vec<DockerPortBinding>,
    /// Whether the supervisor currently holds a docker handle. Gates docker
    /// references, the port manifest, and ready-check port rewriting.
    pub(crate) docker_live: bool,
}

/// An immutable point-in-time view of every active service's endpoints.
#[derive(Debug, Clone, Default)]
pub(crate) struct EndpointSnapshot {
    services: HashMap<String, ServiceEndpoints>,
    references: HashMap<String, String>,
    known_services: HashSet<String>,
}

impl EndpointSnapshot {
    /// One service's endpoints, or `None` if the name isn't an active service.
    pub(crate) fn get(&self, name: &str) -> Option<&ServiceEndpoints> {
        self.services.get(name)
    }

    /// The flat `"service.KEY" -> value` reference map.
    pub(crate) fn references(&self) -> &HashMap<String, String> {
        &self.references
    }

    /// The names that count as runtime references — the active service set.
    pub(crate) fn known_services(&self) -> &HashSet<String> {
        &self.known_services
    }

    /// Env-mode backend vars for one service (empty when it has no proxy).
    pub(crate) fn backend_env(&self, name: &str) -> HashMap<String, String> {
        self.get(name)
            .map(|e| e.backend_env.clone())
            .unwrap_or_default()
    }

    /// Public `DON_PUBLIC_*` vars for one service, docker then proxy so the
    /// proxy wins — same layering as [`Self::references`].
    pub(crate) fn public_env(&self, name: &str) -> HashMap<String, String> {
        let mut env = HashMap::new();
        let Some(endpoints) = self.get(name) else {
            return env;
        };
        if endpoints.docker_live {
            env.extend(crate::docker::public_env_vars(&endpoints.docker));
        }
        env.extend(crate::proxy::public_env_vars_for(&endpoints.proxy));
        env
    }

    /// Configured-port -> actual-port rewrites for one service's ready check.
    pub(crate) fn port_replacements(&self, name: &str) -> HashMap<u16, u16> {
        let Some(endpoints) = self.get(name) else {
            return HashMap::new();
        };
        let docker: &[DockerPortBinding] = if endpoints.docker_live {
            &endpoints.docker
        } else {
            &[]
        };
        crate::process::ready::port_replacements_for(&endpoints.proxy, docker)
    }

    /// Rebuild the two precomputed views from the per-service records.
    fn reindex(&mut self) {
        self.known_services = self.services.keys().cloned().collect();
        let mut references = HashMap::new();
        for (name, endpoints) in &self.services {
            // Docker first, proxy second: a service declaring both is reached
            // through its proxy, so the proxy's values must win.
            if endpoints.docker_live {
                extend_references(
                    &mut references,
                    name,
                    crate::docker::env_reference_values(&endpoints.docker),
                );
            }
            extend_references(
                &mut references,
                name,
                crate::proxy::env_reference_values_for(&endpoints.proxy),
            );
        }
        self.references = references;
    }
}

fn extend_references(
    references: &mut HashMap<String, String>,
    service_name: &str,
    values: HashMap<String, String>,
) {
    for (key, value) in values {
        references.insert(format!("{service_name}.{key}"), value);
    }
}

/// The write half. Owned by the runner and deliberately **not** `Clone`, so
/// single-writer is enforced by ownership — see [`crate::state_store`].
pub(crate) struct EndpointWriter {
    tx: watch::Sender<Arc<EndpointSnapshot>>,
}

impl EndpointWriter {
    /// Seed one empty record per active service.
    ///
    /// The key set is fixed from here on, which is what makes
    /// `$(unknown.thing)` stay literal instead of becoming an error: only
    /// names seeded here are treated as runtime references at all.
    pub(crate) fn seed(&self, names: impl Iterator<Item = String>) {
        let mut snapshot = EndpointSnapshot::default();
        for name in names {
            snapshot.services.insert(name, ServiceEndpoints::default());
        }
        snapshot.reindex();
        self.tx.send_replace(Arc::new(snapshot));
    }

    /// Publish a service's proxy bindings. Called once, at bind time.
    pub(crate) fn publish_proxy(&self, name: &str, bindings: Vec<ProxyBinding>) {
        self.update(name, |endpoints| endpoints.proxy = bindings);
    }

    /// Fold a wired spawn: its docker bindings, its proxy backend vars, and
    /// whether it is docker-backed.
    pub(crate) fn publish_wired(
        &self,
        name: &str,
        docker: Vec<DockerPortBinding>,
        backend_env: Option<HashMap<String, String>>,
        docker_live: bool,
    ) {
        self.update(name, |endpoints| {
            endpoints.docker = docker;
            endpoints.docker_live = docker_live;
            if let Some(backend_env) = backend_env {
                endpoints.backend_env = backend_env;
            }
        });
    }

    /// Custody ended. Docker *bindings* are retained so a restart can request
    /// the same ports, but `docker_live` clears, so references stop resolving.
    pub(crate) fn clear_custody(&self, name: &str) {
        self.update(name, |endpoints| endpoints.docker_live = false);
    }

    /// The runner's own read, so it needn't hold a second handle.
    pub(crate) fn snapshot(&self) -> Arc<EndpointSnapshot> {
        self.tx.borrow().clone()
    }

    /// Apply an edit to one service's record and republish.
    ///
    /// Unknown names are ignored: the key set is decided by [`Self::seed`],
    /// and a name outside it is not a service this runner manages.
    fn update(&self, name: &str, edit: impl FnOnce(&mut ServiceEndpoints)) {
        // Clone out and drop the borrow before sending — holding a `Ref`
        // across `send_replace` deadlocks against its own write lock.
        let mut snapshot = (**self.tx.borrow()).clone();
        let Some(endpoints) = snapshot.services.get_mut(name) else {
            return;
        };
        edit(endpoints);
        snapshot.reindex();
        self.tx.send_replace(Arc::new(snapshot));
    }
}

/// The read half of the endpoint projection. Clone one per consumer.
///
/// Reads never block on the runner's command loop, which is what lets a
/// supervisor render its own `$(peer.KEY)` references at the moment it
/// starts, with no round trip and no dependency on `crate::runner`.
#[derive(Clone, Debug)]
pub(crate) struct EndpointReader {
    rx: watch::Receiver<Arc<EndpointSnapshot>>,
}

impl EndpointReader {
    /// The latest published snapshot.
    pub(crate) fn snapshot(&self) -> Arc<EndpointSnapshot> {
        self.rx.borrow().clone()
    }
}

/// Create a linked writer/reader pair over an empty snapshot.
pub(crate) fn channel() -> (EndpointWriter, EndpointReader) {
    let (tx, rx) = watch::channel(Arc::new(EndpointSnapshot::default()));
    (EndpointWriter { tx }, EndpointReader { rx })
}

/// Render `$(service.key)` references in inline environment values.
///
/// A free function over a snapshot rather than a method on the runner: the
/// caller is whoever is about to start something, which is a supervisor.
pub(crate) fn render_env(
    snapshot: &EndpointSnapshot,
    process_name: &str,
    env: &mut HashMap<String, String>,
) -> Result<(), CommandError> {
    for (key, value) in env.iter_mut() {
        *value = crate::process::env_refs::render(
            value,
            snapshot.references(),
            snapshot.known_services(),
        )
        .map_err(|error| CommandError::Failed {
            name: process_name.to_string(),
            message: format!("invalid runtime port reference in env.{key}: {error}"),
        })?;
    }
    Ok(())
}

/// Resolve a service's ready check against published endpoints — for
/// `don status -v` and the ready lifecycle line.
///
/// The authoritative resolution for the check that actually runs happens in
/// the service supervisor over its own live proxy and docker state; both go
/// through [`crate::process::ready::resolve_ready_check`], so they cannot
/// drift.
pub(crate) fn effective_ready_check(
    snapshot: &EndpointSnapshot,
    name: &str,
    resolved: &ResolvedService,
) -> Option<ReadyCheck> {
    crate::process::ready::resolve_ready_check(
        resolved.ready.as_ref(),
        &resolved.env,
        &snapshot.backend_env(name),
        &snapshot.public_env(name),
        &snapshot.port_replacements(name),
    )
}

/// Build a `.don/ports.json` snapshot from published endpoints.
pub(crate) fn port_manifest(snapshot: &EndpointSnapshot) -> crate::ports::PortManifest {
    use crate::ports::{DockerPort, PortManifest, ProxyPort, ServicePorts};
    use crate::proxy::ProxyBindingMode;

    let mut services = std::collections::BTreeMap::new();
    for (name, endpoints) in &snapshot.services {
        let proxy: Vec<ProxyPort> = endpoints
            .proxy
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
            .collect();

        let docker: Vec<DockerPort> = if endpoints.docker_live {
            endpoints
                .docker
                .iter()
                .map(|binding| DockerPort {
                    configured: binding.configured.clone(),
                    host_addr: binding.host_addr().to_string(),
                    container_port: binding.container_port.to_string(),
                    protocol: binding.protocol.clone(),
                })
                .collect()
        } else {
            Vec::new()
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::proxy::{ProxyBinding, ProxyBindingMode};
    use std::net::SocketAddr;

    fn proxy_binding(configured: &str, bound: &str, mode: ProxyBindingMode) -> ProxyBinding {
        ProxyBinding {
            configured_addr: configured.to_string(),
            bound_addr: bound.parse::<SocketAddr>().unwrap(),
            mode,
            used_fallback: false,
        }
    }

    fn docker_binding(configured: &str, host_port: u16, container_port: u16) -> DockerPortBinding {
        DockerPortBinding {
            configured: configured.to_string(),
            configured_host_port: host_port,
            host_ip: "127.0.0.1".parse().unwrap(),
            host_port,
            container_port,
            protocol: "tcp".to_string(),
        }
    }

    fn seeded(names: &[&str]) -> (EndpointWriter, EndpointReader) {
        let (writer, reader) = channel();
        writer.seed(names.iter().map(|n| n.to_string()));
        (writer, reader)
    }

    #[test]
    fn seeding_fixes_the_known_service_set() {
        let (_writer, reader) = seeded(&["api", "db"]);
        let snapshot = reader.snapshot();
        assert_eq!(snapshot.known_services().len(), 2);
        assert!(snapshot.known_services().contains("api"));
        // A name that was never seeded stays unknown, which is what keeps
        // shell-style `$(...)` values from being treated as references.
        assert!(!snapshot.known_services().contains("ghost"));
        assert!(snapshot.get("ghost").is_none());
    }

    #[test]
    fn proxy_references_resolve_before_the_service_ever_starts() {
        // The `$(daemon.addr)` case: bindings are published at bind time, so a
        // dependent can render its env before the dependency has spawned.
        let (writer, reader) = seeded(&["daemon"]);
        writer.publish_proxy(
            "daemon",
            vec![proxy_binding(
                "127.0.0.1:3777",
                "127.0.0.1:3777",
                ProxyBindingMode::Listenfd,
            )],
        );

        let snapshot = reader.snapshot();
        let mut env = HashMap::from([(
            "DON_UI_TARGET".to_string(),
            "http://$(daemon.addr)".to_string(),
        )]);
        render_env(&snapshot, "web", &mut env).unwrap();
        assert_eq!(env["DON_UI_TARGET"], "http://127.0.0.1:3777");
    }

    /// The docker liveness rule, stated as the behaviour a user sees.
    #[test]
    fn docker_reference_resolution_tracks_custody() {
        struct Case {
            name: &'static str,
            wire: bool,
            clear: bool,
            want_resolved: bool,
        }

        let cases = vec![
            Case {
                name: "never wired — nothing to resolve to",
                wire: false,
                clear: false,
                want_resolved: false,
            },
            Case {
                name: "wired and live",
                wire: true,
                clear: false,
                want_resolved: true,
            },
            Case {
                name: "wired then stopped — deliberately stops resolving",
                wire: true,
                clear: true,
                want_resolved: false,
            },
        ];

        for case in cases {
            let (writer, reader) = seeded(&["db"]);
            if case.wire {
                writer.publish_wired("db", vec![docker_binding("5432", 55432, 5432)], None, true);
            }
            if case.clear {
                writer.clear_custody("db");
            }

            let snapshot = reader.snapshot();
            let mut env = HashMap::from([("URL".to_string(), "$(db.PORT)".to_string())]);
            let result = render_env(&snapshot, "api", &mut env);
            assert_eq!(
                result.is_ok(),
                case.want_resolved,
                "{}: render outcome",
                case.name
            );
            if case.want_resolved {
                assert_eq!(env["URL"], "55432", "{}", case.name);
            }
        }
    }

    #[test]
    fn stopping_a_docker_service_keeps_its_bindings_for_the_next_start() {
        let (writer, reader) = seeded(&["db"]);
        writer.publish_wired("db", vec![docker_binding("5432", 55432, 5432)], None, true);
        writer.clear_custody("db");

        // The mapping survives so a restart can request the same host port,
        // even though references and the manifest hide it while it is down.
        let snapshot = reader.snapshot();
        assert_eq!(snapshot.get("db").unwrap().docker.len(), 1);
        assert!(!snapshot.get("db").unwrap().docker_live);
        assert!(port_manifest(&snapshot).services.is_empty());
    }

    #[test]
    fn a_service_with_both_resolves_to_its_proxy() {
        let (writer, reader) = seeded(&["api"]);
        writer.publish_wired("api", vec![docker_binding("8080", 18080, 8080)], None, true);
        writer.publish_proxy(
            "api",
            vec![proxy_binding(
                "127.0.0.1:9000",
                "127.0.0.1:9000",
                ProxyBindingMode::Listenfd,
            )],
        );

        let snapshot = reader.snapshot();
        // Proxy is the outer public endpoint, so it wins the shared key.
        assert_eq!(snapshot.references()["api.PORT"], "9000");
    }

    #[test]
    fn backend_env_is_never_published_as_a_reference() {
        let (writer, reader) = seeded(&["api"]);
        writer.publish_proxy(
            "api",
            vec![proxy_binding(
                "127.0.0.1:9000",
                "127.0.0.1:9000",
                ProxyBindingMode::Env {
                    env_name: "PORT".to_string(),
                },
            )],
        );
        writer.publish_wired(
            "api",
            Vec::new(),
            Some(HashMap::from([("PORT".to_string(), "49152".to_string())])),
            false,
        );

        let snapshot = reader.snapshot();
        // The reference is the public listener, not the ephemeral backend port
        // the process was told to bind.
        assert_eq!(snapshot.references()["api.PORT"], "9000");
        assert_eq!(snapshot.backend_env("api")["PORT"], "49152");
    }

    #[test]
    fn an_unknown_service_reference_is_an_error_but_a_shell_call_is_not() {
        let (_writer, reader) = seeded(&["api"]);
        let snapshot = reader.snapshot();

        let mut shell = HashMap::from([("REV".to_string(), "$(git rev-parse HEAD)".to_string())]);
        render_env(&snapshot, "web", &mut shell).unwrap();
        assert_eq!(shell["REV"], "$(git rev-parse HEAD)");

        let mut missing = HashMap::from([("URL".to_string(), "$(api.PORT)".to_string())]);
        let err = render_env(&snapshot, "web", &mut missing).unwrap_err();
        assert!(
            err.to_string().contains("env.URL"),
            "error should name the offending key: {err}"
        );
    }

    #[test]
    fn writes_land_with_no_readers_alive() {
        // `send_replace`, not `send` — the same bug `state_store` pins.
        let (writer, reader) = seeded(&["api"]);
        drop(reader);
        writer.publish_proxy(
            "api",
            vec![proxy_binding(
                "127.0.0.1:9000",
                "127.0.0.1:9000",
                ProxyBindingMode::Listenfd,
            )],
        );
        assert_eq!(writer.snapshot().references()["api.PORT"], "9000");
    }
}
