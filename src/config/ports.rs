//! Docker port-mapping syntax.
//!
//! This is a *config grammar*, not a Docker mechanism: `don validate` has to
//! reject `"8080:notaport"` without Docker being involved at all. Keeping it
//! here is what lets `config` validate its own syntax instead of reaching into
//! `docker` for it — which was the only thing making those two modules
//! mutually dependent.
//!
//! Nothing here touches bollard. `docker::parse` builds the bollard types on
//! top of these specs and maps [`PortSpecError`] into its own error.

use std::net::{IpAddr, Ipv4Addr};

/// A port mapping that failed to parse.
///
/// Deliberately not a `DockerError` — the whole point is that this layer
/// doesn't know Docker exists. `docker::parse` converts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PortSpecError {
    /// The mapping string as written in `don.toml`.
    pub(crate) mapping: String,
    /// Why it was rejected, phrased for a config error message.
    pub(crate) reason: String,
}

/// Parse and validate Docker port mappings without probing the host.
///
/// Both port numbers must be numeric and fit in `u16`; container port zero is
/// rejected. Host IPs, when present, must be valid IP literals.
pub(crate) fn parse_port_specs(ports: &[String]) -> Result<Vec<DockerPortSpec>, PortSpecError> {
    ports
        .iter()
        .map(|mapping| parse_one_port(mapping))
        .collect()
}

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

    /// Address Docker should bind on, defaulting to all interfaces when the
    /// mapping didn't name one.
    pub(crate) fn bind_ip(&self) -> IpAddr {
        self.host_ip.unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
    }
}

/// Parse a single port mapping string.
fn parse_one_port(mapping: &str) -> Result<DockerPortSpec, PortSpecError> {
    // Split off protocol suffix if present.
    let (addr_part, protocol) = if let Some(idx) = mapping.rfind('/') {
        let proto = &mapping[idx + 1..];
        let protocol = match proto {
            "tcp" => PortProtocol::Tcp,
            "udp" => PortProtocol::Udp,
            _ => {
                return Err(PortSpecError::new(
                    mapping,
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
        PortSpecError::new(
            mapping,
            "expected format 'host_port:container_port' or 'host_ip:host_port:container_port'"
                .to_string(),
        )
    })?;
    let host_port = parts.next().ok_or_else(|| {
        PortSpecError::new(
            mapping,
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
            raw.parse::<IpAddr>()
                .map_err(|e| PortSpecError::new(mapping, format!("invalid host IP '{raw}': {e}")))
        })
        .transpose()?;

    if host_port.is_empty() || container_port.is_empty() {
        return Err(PortSpecError::new(
            mapping,
            "host and container ports must not be empty".to_string(),
        ));
    }

    let host_port = host_port.parse::<u16>().map_err(|e| {
        PortSpecError::new(mapping, format!("invalid host port '{host_port}': {e}"))
    })?;
    let container_port = container_port.parse::<u16>().map_err(|e| {
        PortSpecError::new(
            mapping,
            format!("invalid container port '{container_port}': {e}"),
        )
    })?;
    if container_port == 0 {
        return Err(PortSpecError::new(
            mapping,
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

impl std::fmt::Display for PortSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid port mapping '{}': {}",
            self.mapping, self.reason
        )
    }
}

impl PortSpecError {
    fn new(mapping: &str, reason: impl Into<String>) -> Self {
        Self {
            mapping: mapping.to_string(),
            reason: reason.into(),
        }
    }
}
