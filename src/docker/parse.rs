//! Parsing Docker configuration values into bollard API types.
//!
//! Handles port mappings ("8080:80/tcp"), environment variable merging,
//! and volume mount pass-through.

use bollard::models::PortBinding;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, UdpSocket};
use std::path::PathBuf;

use super::{DockerError, DockerPortBinding};

/// Bollard port binding map: container port -> host bindings.
pub(crate) type PortMap = HashMap<String, Option<Vec<PortBinding>>>;
/// Bollard exposed ports list.
pub(crate) type ExposedPorts = Vec<String>;

/// Transport protocol for a Docker port mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PortProtocol {
    Tcp,
    Udp,
}

impl PortProtocol {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

/// A validated Docker port mapping from configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DockerPortSpec {
    pub(crate) configured: String,
    pub(crate) host_ip: Option<IpAddr>,
    pub(crate) host_port: u16,
    pub(crate) container_port: u16,
    pub(crate) protocol: PortProtocol,
}

impl DockerPortSpec {
    pub(crate) fn container_key(&self) -> String {
        format!("{}/{}", self.container_port, self.protocol.as_str())
    }

    fn bind_ip(&self) -> IpAddr {
        self.host_ip.unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
    }
}

/// Validated mappings prepared for Docker container creation.
#[derive(Debug, Clone)]
pub(crate) struct PreparedPortMappings {
    specs: Vec<DockerPortSpec>,
    bindings: PortMap,
    exposed: ExposedPorts,
}

impl PreparedPortMappings {
    pub(crate) fn specs(&self) -> &[DockerPortSpec] {
        &self.specs
    }

    pub(crate) fn bindings(&self) -> &PortMap {
        &self.bindings
    }

    pub(crate) fn exposed(&self) -> &ExposedPorts {
        &self.exposed
    }

    /// Ask Docker to allocate every host port. Used for the one-shot retry
    /// when the preferred bindings race with another process after probing.
    pub(crate) fn force_dynamic(&mut self) {
        for bindings in self.bindings.values_mut().flatten() {
            for binding in bindings {
                binding.host_port = Some(String::new());
            }
        }
    }

    /// Ask Docker to allocate host ports only for bindings whose currently
    /// requested host port is in `conflict_ports`, leaving unrelated mappings
    /// on their preferred ports. Returns whether any binding changed; a `false`
    /// result means the caller should fall back to [`Self::force_dynamic`].
    pub(crate) fn force_dynamic_conflicts(&mut self, conflict_ports: &HashSet<u16>) -> bool {
        let mut changed = false;
        for bindings in self.bindings.values_mut().flatten() {
            for binding in bindings {
                let current = binding
                    .host_port
                    .as_deref()
                    .and_then(|port| port.parse::<u16>().ok());
                if let Some(port) = current
                    && conflict_ports.contains(&port)
                {
                    binding.host_port = Some(String::new());
                    changed = true;
                }
            }
        }
        changed
    }
}

/// Extract host ports referenced in a Docker port-allocation error message,
/// e.g. `Bind for 0.0.0.0:5432 failed: port is already allocated` → `{5432}`.
/// Only digit runs immediately following a `:` are considered, so IP octets
/// and unrelated numbers are ignored.
pub(crate) fn conflict_ports_in_message(message: &str) -> HashSet<u16> {
    let bytes = message.as_bytes();
    let mut ports = HashSet::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b':' {
            index += 1;
            continue;
        }
        let start = index + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end > start
            && let Some(port) = message
                .get(start..end)
                .and_then(|raw| raw.parse::<u16>().ok())
            && port != 0
        {
            ports.insert(port);
        }
        index = end.max(index + 1);
    }
    ports
}

/// Parse port mapping strings into bollard's PortBinding format.
///
/// Accepts formats:
/// - `"8080:80"` — host 8080 -> container 80/tcp
/// - `"8080:80/tcp"` — explicit protocol
/// - `"8080:80/udp"` — UDP
/// - `"127.0.0.1:8080:80"` — bind to specific host IP
///
/// Returns `(port_bindings, exposed_ports)` for bollard's HostConfig and ContainerConfig.
pub(crate) fn parse_port_mappings(
    ports: &[String],
) -> Result<(PortMap, ExposedPorts), DockerError> {
    let prepared = prepare_port_mappings(ports, false, &[])?;
    Ok((prepared.bindings, prepared.exposed))
}

/// Parse and validate Docker port mappings without probing the host.
///
/// Both port numbers must be numeric and fit in `u16`; container port zero is
/// rejected. Host IPs, when present, must be valid IP literals.
pub(crate) fn parse_port_specs(ports: &[String]) -> Result<Vec<DockerPortSpec>, DockerError> {
    ports
        .iter()
        .map(|mapping| parse_one_port(mapping))
        .collect()
}

/// Prepare validated mappings for Docker.
///
/// With fallback disabled, configured non-zero ports are preserved and Docker
/// reports any conflict. With fallback enabled, a previous runtime binding is
/// tried first (for restart stability), then the configured port. If neither
/// can be bound, or the configured port is zero, an empty `HostPort` asks the
/// Docker daemon to atomically choose an ephemeral port.
pub(crate) fn prepare_port_mappings(
    ports: &[String],
    fallback_ports: bool,
    prior_bindings: &[DockerPortBinding],
) -> Result<PreparedPortMappings, DockerError> {
    let specs = parse_port_specs(ports)?;
    let mut bindings: PortMap = HashMap::new();
    let mut exposed: ExposedPorts = Vec::new();
    let mut claimed_prior = HashSet::new();

    for spec in &specs {
        let container_key = spec.container_key();
        if !exposed.contains(&container_key) {
            exposed.push(container_key.clone());
        }
        let host_port = choose_host_port(spec, fallback_ports, prior_bindings, &mut claimed_prior)?;
        if let Some(v) = bindings
            .entry(container_key)
            .or_insert_with(|| Some(Vec::new()))
            .as_mut()
        {
            v.push(PortBinding {
                host_ip: spec.host_ip.map(|ip| ip.to_string()),
                host_port: Some(host_port),
            });
        }
    }

    Ok(PreparedPortMappings {
        specs,
        bindings,
        exposed,
    })
}

/// Parse a single port mapping string.
fn parse_one_port(mapping: &str) -> Result<DockerPortSpec, DockerError> {
    // Split off protocol suffix if present.
    let (addr_part, protocol) = if let Some(idx) = mapping.rfind('/') {
        let proto = &mapping[idx + 1..];
        let protocol = match proto {
            "tcp" => PortProtocol::Tcp,
            "udp" => PortProtocol::Udp,
            _ => {
                return Err(DockerError::InvalidPort(
                    mapping.to_string(),
                    format!("unknown protocol '{proto}', expected 'tcp' or 'udp'"),
                ));
            }
        };
        (&mapping[..idx], protocol)
    } else {
        (mapping, PortProtocol::Tcp)
    };

    // Split from the right so bracketed or unbracketed IPv6 host literals can
    // contain colons. The last two fields are always host and container port.
    let mut parts = addr_part.rsplitn(3, ':');
    let container_port = parts.next().ok_or_else(|| {
        DockerError::InvalidPort(
            mapping.to_string(),
            "expected format 'host_port:container_port' or 'host_ip:host_port:container_port'"
                .to_string(),
        )
    })?;
    let host_port = parts.next().ok_or_else(|| {
        DockerError::InvalidPort(
            mapping.to_string(),
            "expected format 'host_port:container_port' or 'host_ip:host_port:container_port'"
                .to_string(),
        )
    })?;
    let host_ip = parts
        .next()
        .map(|raw| {
            raw.strip_prefix('[')
                .and_then(|s| s.strip_suffix(']'))
                .unwrap_or(raw)
        })
        .map(|raw| {
            raw.parse::<IpAddr>().map_err(|e| {
                DockerError::InvalidPort(
                    mapping.to_string(),
                    format!("invalid host IP '{raw}': {e}"),
                )
            })
        })
        .transpose()?;

    if host_port.is_empty() || container_port.is_empty() {
        return Err(DockerError::InvalidPort(
            mapping.to_string(),
            "host and container ports must not be empty".to_string(),
        ));
    }

    let host_port = host_port.parse::<u16>().map_err(|e| {
        DockerError::InvalidPort(
            mapping.to_string(),
            format!("invalid host port '{host_port}': {e}"),
        )
    })?;
    let container_port = container_port.parse::<u16>().map_err(|e| {
        DockerError::InvalidPort(
            mapping.to_string(),
            format!("invalid container port '{container_port}': {e}"),
        )
    })?;
    if container_port == 0 {
        return Err(DockerError::InvalidPort(
            mapping.to_string(),
            "container port must be between 1 and 65535".to_string(),
        ));
    }

    Ok(DockerPortSpec {
        configured: mapping.to_string(),
        host_ip,
        host_port,
        container_port,
        protocol,
    })
}

fn choose_host_port(
    spec: &DockerPortSpec,
    fallback_ports: bool,
    prior_bindings: &[DockerPortBinding],
    claimed_prior: &mut HashSet<usize>,
) -> Result<String, DockerError> {
    if !fallback_ports {
        return Ok(if spec.host_port == 0 {
            String::new()
        } else {
            spec.host_port.to_string()
        });
    }

    if let Some((index, prior)) = prior_bindings.iter().enumerate().find(|(index, binding)| {
        !claimed_prior.contains(index)
            && binding.configured == spec.configured
            && binding.container_port == spec.container_port
            && binding.protocol == spec.protocol.as_str()
    }) {
        claimed_prior.insert(index);
        if prior.host_port != 0
            && is_port_available(
                spec.bind_ip(),
                prior.host_port,
                spec.protocol,
                &spec.configured,
            )?
        {
            return Ok(prior.host_port.to_string());
        }
    }

    if spec.host_port != 0
        && is_port_available(
            spec.bind_ip(),
            spec.host_port,
            spec.protocol,
            &spec.configured,
        )?
    {
        return Ok(spec.host_port.to_string());
    }

    Ok(String::new())
}

fn is_port_available(
    ip: IpAddr,
    port: u16,
    protocol: PortProtocol,
    mapping: &str,
) -> Result<bool, DockerError> {
    let addr = SocketAddr::new(ip, port);
    let result = match protocol {
        PortProtocol::Tcp => TcpListener::bind(addr).map(drop),
        PortProtocol::Udp => UdpSocket::bind(addr).map(drop),
    };
    match result {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => Ok(false),
        Err(source) => Err(DockerError::PortProbe {
            mapping: mapping.to_string(),
            source,
        }),
    }
}

/// Convert Docker's inspected `NetworkSettings.Ports` into stable runtime
/// metadata in configuration order.
pub(crate) fn resolve_actual_port_bindings(
    specs: &[DockerPortSpec],
    actual: &PortMap,
) -> Result<Vec<DockerPortBinding>, DockerError> {
    let mut remaining: HashMap<String, VecDeque<PortBinding>> = actual
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                value.clone().unwrap_or_default().into_iter().collect(),
            )
        })
        .collect();
    let mut resolved = Vec::with_capacity(specs.len());

    for spec in specs {
        let key = spec.container_key();
        let candidates =
            remaining
                .get_mut(&key)
                .ok_or_else(|| DockerError::MissingPortBinding {
                    mapping: spec.configured.clone(),
                })?;
        let index = select_actual_binding(candidates, spec).ok_or_else(|| {
            DockerError::MissingPortBinding {
                mapping: spec.configured.clone(),
            }
        })?;
        let binding = candidates
            .remove(index)
            .ok_or_else(|| DockerError::MissingPortBinding {
                mapping: spec.configured.clone(),
            })?;
        let host_port_raw =
            binding
                .host_port
                .as_deref()
                .ok_or_else(|| DockerError::InvalidRuntimePort {
                    mapping: spec.configured.clone(),
                    value: "<missing>".to_string(),
                })?;
        let host_port =
            host_port_raw
                .parse::<u16>()
                .map_err(|_| DockerError::InvalidRuntimePort {
                    mapping: spec.configured.clone(),
                    value: host_port_raw.to_string(),
                })?;
        if host_port == 0 {
            return Err(DockerError::InvalidRuntimePort {
                mapping: spec.configured.clone(),
                value: host_port_raw.to_string(),
            });
        }
        let host_ip = parse_runtime_host_ip(binding.host_ip.as_deref(), spec)?;
        resolved.push(DockerPortBinding {
            configured: spec.configured.clone(),
            configured_host_port: spec.host_port,
            host_ip,
            host_port,
            container_port: spec.container_port,
            protocol: spec.protocol.as_str().to_string(),
        });
    }

    Ok(resolved)
}

fn select_actual_binding(
    candidates: &VecDeque<PortBinding>,
    spec: &DockerPortSpec,
) -> Option<usize> {
    // When several host ports share one container port, the configured host
    // port is the only thing that disambiguates them. Match it exactly first
    // (this only fires when the port was not reassigned via fallback), so the
    // configured→actual labels stay correct instead of collapsing to index 0.
    if spec.host_port != 0
        && let Some(index) = candidates.iter().position(|binding| {
            binding
                .host_port
                .as_deref()
                .and_then(|raw| raw.parse::<u16>().ok())
                == Some(spec.host_port)
                && host_ip_matches(binding, spec)
        })
    {
        return Some(index);
    }

    if let Some(expected) = spec.host_ip
        && let Some(index) = candidates.iter().position(|binding| {
            binding
                .host_ip
                .as_deref()
                .and_then(|raw| raw.parse::<IpAddr>().ok())
                == Some(expected)
        })
    {
        return Some(index);
    }

    candidates
        .iter()
        .position(|binding| {
            binding
                .host_ip
                .as_deref()
                .is_some_and(|raw| raw == "0.0.0.0")
        })
        .or_else(|| (!candidates.is_empty()).then_some(0))
}

/// True when the candidate's host IP is compatible with the spec's: either the
/// spec left the host IP unspecified, or it matches the candidate exactly.
fn host_ip_matches(binding: &PortBinding, spec: &DockerPortSpec) -> bool {
    let Some(expected) = spec.host_ip else {
        return true;
    };
    binding
        .host_ip
        .as_deref()
        .and_then(|raw| raw.parse::<IpAddr>().ok())
        == Some(expected)
}

fn parse_runtime_host_ip(raw: Option<&str>, spec: &DockerPortSpec) -> Result<IpAddr, DockerError> {
    match raw {
        Some("") | None => Ok(spec.bind_ip()),
        Some(value) => value
            .parse::<IpAddr>()
            .map_err(|_| DockerError::InvalidRuntimeHostIp {
                mapping: spec.configured.clone(),
                value: value.to_string(),
            }),
    }
}

/// Build a Docker env var list from inline env + env files.
///
/// Returns `Vec<String>` of `"KEY=VALUE"` entries. Env files are loaded first
/// (lower priority), then inline env vars are appended (higher priority, overwrites).
pub(crate) fn build_env_vars(
    env: &HashMap<String, String>,
    env_files: &[PathBuf],
) -> Result<Vec<String>, DockerError> {
    let mut vars: HashMap<String, String> = HashMap::new();

    // Load env files first (lower priority).
    for path in env_files {
        let contents = std::fs::read_to_string(path).map_err(DockerError::EnvFile)?;
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = line.split_once('=') {
                vars.insert(key.trim().to_string(), value.trim().to_string());
            }
        }
    }

    // Overlay inline env vars (higher priority).
    for (k, v) in env {
        vars.insert(k.clone(), v.clone());
    }

    Ok(vars.into_iter().map(|(k, v)| format!("{k}={v}")).collect())
}

/// Build the full container environment from every source.
///
/// A docker service draws env from two distinct config fields — the service's
/// own `env_file` and the docker-scoped `docker.env_file` — plus inline `env`.
/// Precedence, lowest to highest: service env files, then docker env files
/// (more specific, so they override), then inline env vars.
///
/// This exists because `docker.env_file` must actually reach the container:
/// previously only the service-level env/env_file was forwarded and
/// `docker.env_file` was silently dropped.
pub(crate) fn build_container_env(
    env: &HashMap<String, String>,
    service_env_files: &[PathBuf],
    docker_env_files: &[PathBuf],
) -> Result<Vec<String>, DockerError> {
    let mut env_files = Vec::with_capacity(service_env_files.len() + docker_env_files.len());
    env_files.extend_from_slice(service_env_files);
    env_files.extend_from_slice(docker_env_files);
    build_env_vars(env, &env_files)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_port_mappings() {
        struct Case {
            name: &'static str,
            input: Vec<&'static str>,
            expect_ok: bool,
            expected_bindings: Vec<(&'static str, &'static str, Option<&'static str>)>, // (container_key, host_port, host_ip)
        }

        let cases = vec![
            Case {
                name: "simple host:container",
                input: vec!["8080:80"],
                expect_ok: true,
                expected_bindings: vec![("80/tcp", "8080", None)],
            },
            Case {
                name: "explicit tcp",
                input: vec!["8080:80/tcp"],
                expect_ok: true,
                expected_bindings: vec![("80/tcp", "8080", None)],
            },
            Case {
                name: "udp",
                input: vec!["5353:53/udp"],
                expect_ok: true,
                expected_bindings: vec![("53/udp", "5353", None)],
            },
            Case {
                name: "with host ip",
                input: vec!["127.0.0.1:8080:80"],
                expect_ok: true,
                expected_bindings: vec![("80/tcp", "8080", Some("127.0.0.1"))],
            },
            Case {
                name: "with ipv6 host ip",
                input: vec!["[::1]:8080:80"],
                expect_ok: true,
                expected_bindings: vec![("80/tcp", "8080", Some("::1"))],
            },
            Case {
                name: "multiple ports",
                input: vec!["8080:80", "8443:443"],
                expect_ok: true,
                expected_bindings: vec![("80/tcp", "8080", None), ("443/tcp", "8443", None)],
            },
            Case {
                name: "invalid format",
                input: vec!["just-a-port"],
                expect_ok: false,
                expected_bindings: vec![],
            },
            Case {
                name: "non-numeric host port",
                input: vec!["http:80"],
                expect_ok: false,
                expected_bindings: vec![],
            },
            Case {
                name: "out of range container port",
                input: vec!["8080:65536"],
                expect_ok: false,
                expected_bindings: vec![],
            },
            Case {
                name: "zero container port",
                input: vec!["8080:0"],
                expect_ok: false,
                expected_bindings: vec![],
            },
            Case {
                name: "invalid host ip",
                input: vec!["not-an-ip:8080:80"],
                expect_ok: false,
                expected_bindings: vec![],
            },
            Case {
                name: "unknown protocol",
                input: vec!["8080:80/sctp"],
                expect_ok: false,
                expected_bindings: vec![],
            },
        ];

        for case in cases {
            let input: Vec<String> = case.input.iter().map(|s| s.to_string()).collect();
            let result = parse_port_mappings(&input);
            if case.expect_ok {
                let (bindings, exposed) = result.unwrap_or_else(|e| panic!("{}: {e}", case.name));

                for (container_key, host_port, host_ip) in &case.expected_bindings {
                    assert!(
                        exposed.contains(&container_key.to_string()),
                        "{}: missing exposed port {container_key}",
                        case.name
                    );
                    let binding_list = bindings
                        .get(*container_key)
                        .and_then(|v| v.as_ref())
                        .unwrap_or_else(|| {
                            panic!("{}: missing binding for {container_key}", case.name)
                        });
                    assert!(
                        binding_list.iter().any(|b| {
                            b.host_port.as_deref() == Some(*host_port)
                                && b.host_ip.as_deref() == *host_ip
                        }),
                        "{}: binding mismatch for {container_key}",
                        case.name
                    );
                }
            } else {
                assert!(result.is_err(), "{}: expected error", case.name);
            }
        }
    }

    #[test]
    fn test_prepare_port_mappings_fallback() {
        #[derive(Clone, Copy)]
        enum Expected {
            Configured,
            Dynamic,
            Prior,
        }

        struct Case {
            name: &'static str,
            fallback_ports: bool,
            occupy_configured: bool,
            reuse_prior: bool,
            expected: Expected,
        }

        let cases = vec![
            Case {
                name: "fallback disabled preserves occupied configured port",
                fallback_ports: false,
                occupy_configured: true,
                reuse_prior: false,
                expected: Expected::Configured,
            },
            Case {
                name: "fallback keeps available configured port",
                fallback_ports: true,
                occupy_configured: false,
                reuse_prior: false,
                expected: Expected::Configured,
            },
            Case {
                name: "fallback requests docker port when configured is occupied",
                fallback_ports: true,
                occupy_configured: true,
                reuse_prior: false,
                expected: Expected::Dynamic,
            },
            Case {
                name: "fallback reuses available prior port",
                fallback_ports: true,
                occupy_configured: true,
                reuse_prior: true,
                expected: Expected::Prior,
            },
        ];

        // Cases that need a port to be *available* have to release it first,
        // and the kernel can hand it to an unrelated process in that window.
        // The steal then presents as the wrong `Expected` variant, which is
        // indistinguishable from a real regression at the assert — so detect
        // it explicitly and retry the case with fresh ports. A genuine
        // regression reproduces on every attempt and still fails.
        const ATTEMPTS: usize = 10;

        for case in cases {
            for attempt in 1..=ATTEMPTS {
                let configured_listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let configured_port = configured_listener.local_addr().unwrap().port();
                let configured_listener = if case.occupy_configured {
                    Some(configured_listener)
                } else {
                    drop(configured_listener);
                    None
                };

                let prior_listener = case.reuse_prior.then(|| {
                    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                    let port = listener.local_addr().unwrap().port();
                    (listener, port)
                });
                let prior_port = prior_listener.as_ref().map(|(_, port)| *port).unwrap_or(0);
                if let Some((listener, _)) = prior_listener {
                    drop(listener);
                }

                let mapping = format!("127.0.0.1:{configured_port}:80");
                let prior = if case.reuse_prior {
                    vec![DockerPortBinding {
                        configured: mapping.clone(),
                        configured_host_port: configured_port,
                        host_ip: "127.0.0.1".parse().unwrap(),
                        host_port: prior_port,
                        container_port: 80,
                        protocol: "tcp".to_string(),
                    }]
                } else {
                    Vec::new()
                };
                let prepared = prepare_port_mappings(
                    std::slice::from_ref(&mapping),
                    case.fallback_ports,
                    &prior,
                )
                .unwrap_or_else(|e| panic!("{}: {e}", case.name));
                let host_port = prepared
                    .bindings()
                    .get("80/tcp")
                    .and_then(Option::as_ref)
                    .and_then(|bindings| bindings.first())
                    .and_then(|binding| binding.host_port.as_deref())
                    .unwrap();

                let expected_host_port = match case.expected {
                    Expected::Configured => configured_port.to_string(),
                    Expected::Dynamic => String::new(),
                    Expected::Prior => prior_port.to_string(),
                };
                let matched = host_port == expected_host_port;

                // Probe only the ports this case *relied* on being free — an
                // occupied configured port is held by us, not a thief.
                let relied_on_port_taken = (!case.occupy_configured
                    && TcpListener::bind(("127.0.0.1", configured_port)).is_err())
                    || (case.reuse_prior && TcpListener::bind(("127.0.0.1", prior_port)).is_err());

                let retry = !matched && attempt < ATTEMPTS && relied_on_port_taken;
                drop(configured_listener);
                if retry {
                    continue;
                }

                assert_eq!(host_port, expected_host_port, "{}", case.name);
                break;
            }
        }
    }

    #[test]
    fn test_configured_zero_requests_docker_assigned_port() {
        let mapping = "127.0.0.1:0:80".to_string();
        let prepared = prepare_port_mappings(std::slice::from_ref(&mapping), false, &[]).unwrap();
        let host_port = prepared
            .bindings()
            .get("80/tcp")
            .and_then(Option::as_ref)
            .and_then(|bindings| bindings.first())
            .and_then(|binding| binding.host_port.as_deref());

        assert_eq!(host_port, Some(""));
    }

    #[test]
    fn test_resolve_actual_port_bindings_table() {
        struct Case {
            name: &'static str,
            configured: &'static str,
            actual_ip: &'static str,
            actual_port: &'static str,
            expect_ok: bool,
        }

        let cases = vec![
            Case {
                name: "ipv4 wildcard",
                configured: "0:80",
                actual_ip: "0.0.0.0",
                actual_port: "49152",
                expect_ok: true,
            },
            Case {
                name: "explicit ipv4",
                configured: "127.0.0.1:0:80",
                actual_ip: "127.0.0.1",
                actual_port: "49153",
                expect_ok: true,
            },
            Case {
                name: "invalid runtime port",
                configured: "0:80",
                actual_ip: "0.0.0.0",
                actual_port: "not-a-port",
                expect_ok: false,
            },
        ];

        for case in cases {
            let specs = parse_port_specs(&[case.configured.to_string()]).unwrap();
            let mut actual = PortMap::new();
            actual.insert(
                "80/tcp".to_string(),
                Some(vec![PortBinding {
                    host_ip: Some(case.actual_ip.to_string()),
                    host_port: Some(case.actual_port.to_string()),
                }]),
            );

            let result = resolve_actual_port_bindings(&specs, &actual);
            if case.expect_ok {
                let binding = result
                    .unwrap_or_else(|e| panic!("{}: {e}", case.name))
                    .remove(0);
                assert_eq!(binding.configured, case.configured, "{}", case.name);
                assert_eq!(
                    binding.host_port.to_string(),
                    case.actual_port,
                    "{}",
                    case.name
                );
            } else {
                assert!(result.is_err(), "{}: expected error", case.name);
            }
        }
    }

    #[test]
    fn test_conflict_ports_in_message() {
        struct Case {
            name: &'static str,
            message: &'static str,
            expected: &'static [u16],
        }

        let cases = vec![
            Case {
                name: "linux bind failure",
                message: "Bind for 0.0.0.0:5432 failed: port is already allocated",
                expected: &[5432],
            },
            Case {
                name: "ipv6 host",
                message: "Bind for [::]:8080 failed: port is already allocated",
                expected: &[8080],
            },
            Case {
                name: "no ports",
                message: "invalid mount config",
                expected: &[],
            },
        ];

        for case in cases {
            let actual = conflict_ports_in_message(case.message);
            let expected: HashSet<u16> = case.expected.iter().copied().collect();
            assert_eq!(actual, expected, "{}", case.name);
        }
    }

    #[test]
    fn test_force_dynamic_conflicts_is_targeted() {
        let mut prepared = prepare_port_mappings(
            &[
                "127.0.0.1:5432:5432".to_string(),
                "127.0.0.1:6060:80".to_string(),
            ],
            false,
            &[],
        )
        .unwrap();

        let changed = prepared.force_dynamic_conflicts(&HashSet::from([5432]));
        assert!(changed);

        let conflicting = prepared
            .bindings()
            .get("5432/tcp")
            .and_then(Option::as_ref)
            .and_then(|b| b.first())
            .and_then(|b| b.host_port.as_deref());
        assert_eq!(conflicting, Some(""), "conflicting port becomes dynamic");

        let untouched = prepared
            .bindings()
            .get("80/tcp")
            .and_then(Option::as_ref)
            .and_then(|b| b.first())
            .and_then(|b| b.host_port.as_deref());
        assert_eq!(untouched, Some("6060"), "unrelated port keeps preference");

        assert!(
            !prepared.force_dynamic_conflicts(&HashSet::from([49999])),
            "a non-matching port set changes nothing"
        );
    }

    #[test]
    fn test_resolve_multiple_host_ports_same_container_port() {
        // Two host ports map to container port 80; each must resolve to its own
        // actual binding rather than collapsing onto the first candidate.
        let specs = parse_port_specs(&["8080:80".to_string(), "8081:80".to_string()]).unwrap();
        let mut actual = PortMap::new();
        actual.insert(
            "80/tcp".to_string(),
            Some(vec![
                PortBinding {
                    host_ip: Some("0.0.0.0".to_string()),
                    host_port: Some("8081".to_string()),
                },
                PortBinding {
                    host_ip: Some("0.0.0.0".to_string()),
                    host_port: Some("8080".to_string()),
                },
            ]),
        );

        let resolved = resolve_actual_port_bindings(&specs, &actual).unwrap();
        assert_eq!(resolved[0].configured, "8080:80");
        assert_eq!(resolved[0].host_port, 8080);
        assert_eq!(resolved[1].configured, "8081:80");
        assert_eq!(resolved[1].host_port, 8081);
    }

    #[test]
    fn test_build_env_vars() {
        struct Case {
            name: &'static str,
            env: Vec<(&'static str, &'static str)>,
            expected_contains: Vec<&'static str>,
        }

        let cases = vec![
            Case {
                name: "simple vars",
                env: vec![("FOO", "bar"), ("BAZ", "qux")],
                expected_contains: vec!["FOO=bar", "BAZ=qux"],
            },
            Case {
                name: "empty",
                env: vec![],
                expected_contains: vec![],
            },
        ];

        for case in cases {
            let env: HashMap<String, String> = case
                .env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let result = build_env_vars(&env, &[]).unwrap();
            for expected in &case.expected_contains {
                assert!(
                    result.iter().any(|v| v == expected),
                    "{}: expected '{}' in {:?}",
                    case.name,
                    expected,
                    result
                );
            }
            assert_eq!(result.len(), case.expected_contains.len(), "{}", case.name);
        }
    }

    #[test]
    fn test_build_env_vars_with_file() {
        let dir = tempfile::tempdir().unwrap();
        let env_file = dir.path().join(".env");
        std::fs::write(
            &env_file,
            "FILE_VAR=from_file\n# comment\nOVERRIDE=file_val\n",
        )
        .unwrap();

        let mut env = HashMap::new();
        env.insert("OVERRIDE".to_string(), "inline_val".to_string());
        env.insert("INLINE".to_string(), "only".to_string());

        let result = build_env_vars(&env, &[env_file]).unwrap();
        // Inline takes precedence over file.
        assert!(result.contains(&"OVERRIDE=inline_val".to_string()));
        assert!(result.contains(&"FILE_VAR=from_file".to_string()));
        assert!(result.contains(&"INLINE=only".to_string()));
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_build_container_env_merges_docker_env_file() {
        let dir = tempfile::tempdir().unwrap();

        let service_file = dir.path().join("service.env");
        std::fs::write(&service_file, "SERVICE_VAR=svc\nOVERRIDE=from_service\n").unwrap();
        let docker_file = dir.path().join("docker.env");
        std::fs::write(&docker_file, "DOCKER_VAR=dock\nOVERRIDE=from_docker\n").unwrap();

        let mut env = HashMap::new();
        env.insert("INLINE_VAR".to_string(), "inline".to_string());

        let result = build_container_env(
            &env,
            std::slice::from_ref(&service_file),
            std::slice::from_ref(&docker_file),
        )
        .unwrap();

        // Regression: the docker-scoped env_file used to be dropped entirely.
        // All three sources must reach the container.
        assert!(result.contains(&"SERVICE_VAR=svc".to_string()));
        assert!(result.contains(&"DOCKER_VAR=dock".to_string()));
        assert!(result.contains(&"INLINE_VAR=inline".to_string()));
        // Docker env_file overrides the service env_file on conflict.
        assert!(result.contains(&"OVERRIDE=from_docker".to_string()));
    }
}
