//! Who-gets-what and the pointer to Key. Mapping lives in key.toml, not here.

use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Top-level `[secrets]` table: how to invoke Key. Mapping is in `key.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SecretsConfig {
    /// Binary on PATH. Defaults to `key`.
    #[serde(default = "default_command")]
    pub command: String,
    /// Mapping file, relative to the project root. Defaults to `key.toml`.
    #[serde(default = "default_config")]
    pub config: PathBuf,
}

fn default_command() -> String {
    "key".to_string()
}

fn default_config() -> PathBuf {
    PathBuf::from("key.toml")
}

impl Default for SecretsConfig {
    fn default() -> Self {
        Self {
            command: default_command(),
            config: default_config(),
        }
    }
}

/// A named bundle of secret keys, referenced from `secrets = ["group"]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "RawSecretGroup")]
pub struct SecretGroup {
    pub keys: Vec<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawSecretGroup {
    Simple(Vec<String>),
    Detailed {
        #[serde(default)]
        keys: Vec<String>,
    },
}

impl From<RawSecretGroup> for SecretGroup {
    fn from(raw: RawSecretGroup) -> Self {
        match raw {
            RawSecretGroup::Simple(keys) => SecretGroup { keys },
            RawSecretGroup::Detailed { keys } => SecretGroup { keys },
        }
    }
}

/// Names and paths from `key.toml`. Values are never stored here.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyCatalog {
    pub vars: HashMap<String, String>,
    pub groups: HashMap<String, SecretGroup>,
}

#[derive(Deserialize)]
struct KeyToml {
    #[serde(default)]
    vars: HashMap<String, String>,
    #[serde(default)]
    groups: HashMap<String, SecretGroup>,
}

impl KeyCatalog {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        Self::parse(&text, path)
    }

    pub fn parse(text: &str, path: &Path) -> Result<Self, String> {
        let raw: KeyToml = toml::from_str(text)
            .map_err(|error| format!("failed to parse {}: {error}", path.display()))?;
        Ok(Self {
            vars: raw.vars,
            groups: raw.groups,
        })
    }
}

pub(crate) fn is_valid_secret_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    }
}

pub(crate) fn expand_secret_refs<'a>(
    refs: &'a [String],
    vars: &HashSet<&str>,
    groups: &'a HashMap<String, SecretGroup>,
) -> Result<Vec<String>, Vec<ExpandSecretError<'a>>> {
    let mut errors = Vec::new();
    let mut seen = HashSet::new();
    let mut expanded = Vec::new();
    for name in refs {
        if vars.contains(name.as_str()) {
            if seen.insert(name.as_str()) {
                expanded.push(name.clone());
            }
            continue;
        }
        if let Some(group) = groups.get(name) {
            for key in &group.keys {
                if seen.insert(key.as_str()) {
                    expanded.push(key.clone());
                }
            }
            continue;
        }
        errors.push(ExpandSecretError { name });
    }
    if errors.is_empty() {
        Ok(expanded)
    } else {
        Err(errors)
    }
}

#[derive(Debug)]
pub(crate) struct ExpandSecretError<'a> {
    pub name: &'a str,
}

pub(crate) fn validate_secrets(
    secrets: Option<&SecretsConfig>,
    catalog: Option<&KeyCatalog>,
    services: &HashMap<String, super::Service>,
    tasks: &HashMap<String, super::Task>,
    suggest_typo: impl Fn(&str, &HashSet<&str>) -> String,
    errors: &mut Vec<String>,
) {
    let configured = secrets.is_some();
    if !configured {
        for (name, svc) in services {
            if !svc.secrets.is_empty() {
                errors.push(format!(
                    "service '{name}': secrets = [...] requires a [secrets] table"
                ));
            }
        }
        for (name, task) in tasks {
            if !task.secrets.is_empty() {
                errors.push(format!(
                    "task '{name}': secrets = [...] requires a [secrets] table"
                ));
            }
        }
        return;
    }

    let Some(catalog) = catalog else {
        errors.push(
            "[secrets]: key.toml was not loaded — it must sit next to don.toml \
             (or set [secrets] config = \"...\")"
                .to_string(),
        );
        return;
    };

    if catalog.vars.is_empty() {
        errors.push("[secrets]: key.toml vars is empty — nothing to fetch".to_string());
    }

    let mut var_names: HashSet<&str> = HashSet::new();
    for (name, path) in &catalog.vars {
        if !is_valid_secret_name(name) {
            errors.push(format!(
                "key.toml vars.{name}: invalid name — must be an env-var identifier"
            ));
        }
        if path.is_empty() {
            errors.push(format!("key.toml vars.{name}: path is empty"));
        } else if !path.starts_with('/') {
            errors.push(format!(
                "key.toml vars.{name}: SSM parameter path '{path}' must start with '/'"
            ));
        }
        var_names.insert(name.as_str());
    }

    let mut group_names: HashSet<&str> = HashSet::new();
    for (name, group) in &catalog.groups {
        if var_names.contains(name.as_str()) {
            errors.push(format!(
                "secret group '{name}' has the same name as a secret var '{name}' — \
                 rename one so `secrets = [\"{name}\"]` is unambiguous"
            ));
        }
        if group.keys.is_empty() {
            errors.push(format!("secret group '{name}': keys is empty"));
        }
        let mut seen = HashSet::new();
        for key in &group.keys {
            if !var_names.contains(key.as_str()) {
                let suggestion = suggest_typo(key, &var_names);
                errors.push(format!(
                    "secret group '{name}': unknown secret '{key}'{suggestion}"
                ));
            }
            if !seen.insert(key.as_str()) {
                errors.push(format!(
                    "secret group '{name}': key '{key}' is listed more than once"
                ));
            }
        }
        group_names.insert(name.as_str());
    }

    let mut candidates: HashSet<&str> = var_names.iter().copied().collect();
    candidates.extend(group_names.iter().copied());

    for (name, svc) in services {
        push_ref_errors(
            "service",
            name,
            &svc.secrets,
            &var_names,
            &catalog.groups,
            &candidates,
            &suggest_typo,
            errors,
        );
        for (platform, ov) in &svc.platform {
            if let Some(refs) = &ov.secrets {
                push_ref_errors(
                    "service",
                    &format!("{name}.platform.{platform}"),
                    refs,
                    &var_names,
                    &catalog.groups,
                    &candidates,
                    &suggest_typo,
                    errors,
                );
            }
        }
    }
    for (name, task) in tasks {
        push_ref_errors(
            "task",
            name,
            &task.secrets,
            &var_names,
            &catalog.groups,
            &candidates,
            &suggest_typo,
            errors,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn push_ref_errors(
    kind: &str,
    name: &str,
    refs: &[String],
    vars: &HashSet<&str>,
    groups: &HashMap<String, SecretGroup>,
    candidates: &HashSet<&str>,
    suggest_typo: impl Fn(&str, &HashSet<&str>) -> String,
    errors: &mut Vec<String>,
) {
    if let Err(unknown) = expand_secret_refs(refs, vars, groups) {
        for err in unknown {
            let suggestion = suggest_typo(err.name, candidates);
            errors.push(format!(
                "{kind} '{name}': unknown secret or secret group '{}' {suggestion}",
                err.name
            ));
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::{Config, Platform};

    fn catalog_from(toml: &str) -> KeyCatalog {
        KeyCatalog::parse(toml, Path::new("key.toml")).unwrap()
    }

    fn validate_pair(don: &str, key: &str) -> Result<Vec<String>, crate::config::ConfigError> {
        let mut config: Config = don.parse().unwrap();
        config.key_catalog = Some(catalog_from(key));
        config.validate(Platform::LinuxX86_64)
    }

    #[test]
    fn secret_name_table() {
        assert!(is_valid_secret_name("STRIPE_SECRET_KEY"));
        assert!(!is_valid_secret_name("1KEY"));
        assert!(!is_valid_secret_name("DD-API-KEY"));
    }

    #[test]
    fn config_secrets_validation_table() {
        struct Case {
            name: &'static str,
            don: &'static str,
            key: Option<&'static str>,
            want_err: Option<&'static str>,
        }
        let cases = [
            Case {
                name: "valid mapping and per-service list",
                don: r#"
                    [secrets]
                    [services.api]
                    run.cmd = "true"
                    secrets = ["app"]
                    [services.web]
                    run.cmd = "true"
                "#,
                key: Some(
                    r#"
                    provider = "aws-ssm"
                    [vars]
                    STRIPE_SECRET_KEY = "/app/StripeSecretKey"
                    DD_API_KEY = "/app/Datadog/ApiKey"
                    [groups]
                    app = ["STRIPE_SECRET_KEY"]
                    "#,
                ),
                want_err: None,
            },
            Case {
                name: "unknown secret gets a suggestion",
                don: r#"
                    [secrets]
                    [services.api]
                    run.cmd = "true"
                    secrets = ["STRIPE_SECRET_KE"]
                "#,
                key: Some(
                    r#"
                    provider = "aws-ssm"
                    [vars]
                    STRIPE_SECRET_KEY = "/app/StripeSecretKey"
                    "#,
                ),
                want_err: Some("did you mean 'STRIPE_SECRET_KEY'"),
            },
            Case {
                name: "secrets list without [secrets] table",
                don: r#"
                    [services.api]
                    run.cmd = "true"
                    secrets = ["STRIPE_SECRET_KEY"]
                "#,
                key: None,
                want_err: Some("requires a [secrets] table"),
            },
            Case {
                name: "ssm path must start with slash",
                don: r#"
                    [secrets]
                    [services.api]
                    run.cmd = "true"
                "#,
                key: Some(
                    r#"
                    provider = "aws-ssm"
                    [vars]
                    STRIPE_SECRET_KEY = "app/StripeSecretKey"
                    "#,
                ),
                want_err: Some("must start with '/'"),
            },
        ];
        for case in cases {
            let result = match case.key {
                Some(key) => validate_pair(case.don, key),
                None => {
                    let config: Config = case.don.parse().unwrap();
                    config.validate(Platform::LinuxX86_64)
                }
            };
            match (case.want_err, result) {
                (None, Ok(_)) => {}
                (None, Err(e)) => panic!("{}: unexpected error {e}", case.name),
                (Some(needle), Err(e)) => {
                    let text = e.to_string();
                    assert!(
                        text.contains(needle),
                        "{}: expected {needle:?} in {text}",
                        case.name
                    );
                }
                (Some(needle), Ok(_)) => {
                    panic!("{}: expected error containing {needle:?}", case.name)
                }
            }
        }
    }
}
