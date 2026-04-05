//! Parsing Docker configuration values into bollard API types.
//!
//! Handles port mappings ("8080:80/tcp"), environment variable merging,
//! and volume mount pass-through.

use bollard::models::PortBinding;
use std::collections::HashMap;
use std::path::PathBuf;

use super::DockerError;

/// Bollard port binding map: container port -> host bindings.
pub(crate) type PortMap = HashMap<String, Option<Vec<PortBinding>>>;
/// Bollard exposed ports list.
pub(crate) type ExposedPorts = Vec<String>;

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
    let mut bindings: PortMap = HashMap::new();
    let mut exposed: ExposedPorts = Vec::new();

    for mapping in ports {
        let (host_ip, host_port, container_port, protocol) = parse_one_port(mapping)?;

        let container_key = format!("{container_port}/{protocol}");
        if !exposed.contains(&container_key) {
            exposed.push(container_key.clone());
        }
        if let Some(v) = bindings
            .entry(container_key)
            .or_insert_with(|| Some(Vec::new()))
            .as_mut()
        {
            v.push(PortBinding {
                host_ip: host_ip.map(|s| s.to_string()),
                host_port: Some(host_port.to_string()),
            });
        }
    }

    Ok((bindings, exposed))
}

/// Parse a single port mapping string.
/// Returns (host_ip, host_port, container_port, protocol).
fn parse_one_port(mapping: &str) -> Result<(Option<&str>, &str, &str, &str), DockerError> {
    // Split off protocol suffix if present.
    let (addr_part, protocol) = if let Some(idx) = mapping.rfind('/') {
        let proto = &mapping[idx + 1..];
        if proto != "tcp" && proto != "udp" {
            return Err(DockerError::InvalidPort(
                mapping.to_string(),
                format!("unknown protocol '{proto}', expected 'tcp' or 'udp'"),
            ));
        }
        (&mapping[..idx], proto)
    } else {
        (mapping, "tcp")
    };

    let parts: Vec<&str> = addr_part.split(':').collect();
    match parts.len() {
        // "host_port:container_port"
        2 => Ok((None, parts[0], parts[1], protocol)),
        // "host_ip:host_port:container_port"
        3 => Ok((Some(parts[0]), parts[1], parts[2], protocol)),
        _ => Err(DockerError::InvalidPort(
            mapping.to_string(),
            "expected format 'host_port:container_port' or 'host_ip:host_port:container_port'"
                .to_string(),
        )),
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
                let (bindings, exposed) =
                    result.unwrap_or_else(|e| panic!("{}: {e}", case.name));

                for (container_key, host_port, host_ip) in &case.expected_bindings {
                    assert!(
                        exposed.contains(&container_key.to_string()),
                        "{}: missing exposed port {container_key}",
                        case.name
                    );
                    let binding_list = bindings
                        .get(*container_key)
                        .and_then(|v| v.as_ref())
                        .unwrap_or_else(|| panic!("{}: missing binding for {container_key}", case.name));
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
        std::fs::write(&env_file, "FILE_VAR=from_file\n# comment\nOVERRIDE=file_val\n").unwrap();

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
}
