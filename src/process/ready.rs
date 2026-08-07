//! Ready-check resolution against live runtime ports.
//!
//! The supervisor resolves the check it actually runs from its own proxy
//! and docker state; the runner's status display resolves the same way
//! from its shadows. One algorithm, two input sources — they cannot drift.

use crate::config::ReadyCheck;
use std::collections::HashMap;
use std::net::SocketAddr;

/// Resolve a ready check's `${VAR}` references and configured-to-actual
/// port rewrites.
///
/// Backend vars layer first, then public ones. A `proxy = { env = "PORT" }`
/// service is told its ephemeral backend port through `PORT`, and a ready
/// check pointed at `${PORT}` means "is the service itself up?" — checking
/// the public listener instead would pass the moment Don bound the proxy,
/// before the service had started at all.
pub(crate) fn resolve_ready_check(
    ready: Option<&ReadyCheck>,
    base_env: &HashMap<String, String>,
    backend_env: &HashMap<String, String>,
    public_env: &HashMap<String, String>,
    replacements: &HashMap<u16, u16>,
) -> Option<ReadyCheck> {
    let mut ready = ready?.clone();
    let mut env = base_env.clone();
    env.extend(backend_env.iter().map(|(k, v)| (k.clone(), v.clone())));
    env.extend(public_env.iter().map(|(k, v)| (k.clone(), v.clone())));

    if let Some(tcp) = ready.tcp.take() {
        let expanded = crate::sys::env::expand_env_vars(&tcp, &env);
        ready.tcp = Some(rewrite_tcp_port(&expanded, replacements));
    }
    if let Some(http) = ready.http.take() {
        let expanded = crate::sys::env::expand_env_vars(&http, &env);
        ready.http = Some(rewrite_http_port(&expanded, replacements));
    }
    Some(ready)
}

/// Configured-to-actual port replacements for a spawn, from its live proxy
/// bindings and docker port bindings — the supervisor-side twin of
/// [`Runner::ready_port_replacements`].
pub(crate) fn port_replacements_for(
    proxy_bindings: &[crate::proxy::ProxyBinding],
    docker_bindings: &[crate::docker::DockerPortBinding],
) -> HashMap<u16, u16> {
    let mut candidates: HashMap<u16, Option<u16>> = HashMap::new();
    for binding in proxy_bindings {
        let Ok(configured) = binding.configured_addr.parse::<SocketAddr>() else {
            continue;
        };
        record_port_replacement(
            &mut candidates,
            configured.port(),
            binding.bound_addr.port(),
        );
    }
    for binding in docker_bindings.iter().filter(|b| b.protocol == "tcp") {
        record_port_replacement(
            &mut candidates,
            binding.configured_host_port,
            binding.host_port,
        );
    }
    candidates
        .into_iter()
        .filter_map(|(configured, actual)| {
            actual
                .filter(|actual| configured != 0 && configured != *actual)
                .map(|actual| (configured, actual))
        })
        .collect()
}

fn record_port_replacement(
    candidates: &mut HashMap<u16, Option<u16>>,
    configured: u16,
    actual: u16,
) {
    match candidates.entry(configured) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(Some(actual));
        }
        std::collections::hash_map::Entry::Occupied(mut entry)
            if entry.get().is_some_and(|existing| existing != actual) =>
        {
            entry.insert(None);
        }
        std::collections::hash_map::Entry::Occupied(_) => {}
    }
}

fn rewrite_tcp_port(value: &str, replacements: &HashMap<u16, u16>) -> String {
    if let Ok(mut address) = value.parse::<SocketAddr>()
        && let Some(actual) = replacements.get(&address.port())
    {
        address.set_port(*actual);
        return address.to_string();
    }

    let Some((prefix, port)) = value.rsplit_once(':') else {
        return value.to_string();
    };
    let Ok(configured) = port.parse::<u16>() else {
        return value.to_string();
    };
    match replacements.get(&configured) {
        Some(actual) => format!("{prefix}:{actual}"),
        None => value.to_string(),
    }
}

fn rewrite_http_port(value: &str, replacements: &HashMap<u16, u16>) -> String {
    let Ok(mut url) = reqwest::Url::parse(value) else {
        return value.to_string();
    };
    let Some(configured) = url.port_or_known_default() else {
        return value.to_string();
    };
    let Some(actual) = replacements.get(&configured) else {
        return value.to_string();
    };
    if url.set_port(Some(*actual)).is_err() {
        return value.to_string();
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_port_rewrite_table() {
        struct Case {
            name: &'static str,
            input: &'static str,
            http: bool,
            expected: &'static str,
        }

        let replacements = HashMap::from([(3000, 49152), (80, 49153)]);
        let cases = vec![
            Case {
                name: "socket address",
                input: "127.0.0.1:3000",
                http: false,
                expected: "127.0.0.1:49152",
            },
            Case {
                name: "hostname address",
                input: "localhost:3000",
                http: false,
                expected: "localhost:49152",
            },
            Case {
                name: "explicit HTTP port",
                input: "http://localhost:3000/health",
                http: true,
                expected: "http://localhost:49152/health",
            },
            Case {
                name: "implicit HTTP port",
                input: "http://localhost/health",
                http: true,
                expected: "http://localhost:49153/health",
            },
            Case {
                name: "unmapped",
                input: "127.0.0.1:9000",
                http: false,
                expected: "127.0.0.1:9000",
            },
        ];

        for case in cases {
            let actual = if case.http {
                rewrite_http_port(case.input, &replacements)
            } else {
                rewrite_tcp_port(case.input, &replacements)
            };
            assert_eq!(actual, case.expected, "{}", case.name);
        }
    }

    #[test]
    fn ambiguous_replacement_is_suppressed() {
        let mut candidates = HashMap::new();
        record_port_replacement(&mut candidates, 3000, 49152);
        record_port_replacement(&mut candidates, 3000, 49153);
        assert_eq!(candidates.get(&3000), Some(&None));
    }
}
