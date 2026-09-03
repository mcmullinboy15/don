//! Mapping: names and SSM paths only. Same table as key.toml, nested under [secrets].

use super::error::SecretError;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

/// How values are fetched. This crate currently implements AWS SSM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Provider {
    /// AWS Systems Manager Parameter Store (`aws ssm get-parameters`).
    AwsSsm,
}

/// A named bundle of secret keys, referenced from `secrets = ["group"]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "RawGroup")]
pub struct Group {
    pub keys: Vec<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawGroup {
    Simple(Vec<String>),
    Detailed {
        #[serde(default)]
        keys: Vec<String>,
    },
}

impl From<RawGroup> for Group {
    fn from(raw: RawGroup) -> Self {
        match raw {
            RawGroup::Simple(keys) => Group { keys },
            RawGroup::Detailed { keys } => Group { keys },
        }
    }
}

/// Body of `[secrets]` in don.toml, and of a standalone key.toml.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SecretsConfig {
    pub provider: Provider,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub vars: HashMap<String, String>,
    #[serde(default)]
    pub groups: HashMap<String, Group>,
}

impl SecretsConfig {
    pub fn validate(&self) -> Result<(), SecretError> {
        let errors = self.mapping_errors();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(SecretError::msg(errors.join("\n")))
        }
    }

    pub fn mapping_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.vars.is_empty() {
            errors.push("vars is empty — nothing to fetch".to_string());
        }
        let mut var_names: HashSet<&str> = HashSet::new();
        for (name, path) in &self.vars {
            if !is_valid_name(name) {
                errors.push(format!(
                    "vars.{name}: invalid name — must be an env-var identifier"
                ));
            }
            if path.is_empty() {
                errors.push(format!("vars.{name}: path is empty"));
            } else if self.provider == Provider::AwsSsm && !path.starts_with('/') {
                errors.push(format!(
                    "vars.{name}: SSM parameter path '{path}' must start with '/'"
                ));
            }
            var_names.insert(name.as_str());
        }
        for (name, group) in &self.groups {
            if var_names.contains(name.as_str()) {
                errors.push(format!(
                    "group '{name}' has the same name as a var '{name}' — rename one"
                ));
            }
            if group.keys.is_empty() {
                errors.push(format!("group '{name}': keys is empty"));
            }
            let mut seen = HashSet::new();
            for key in &group.keys {
                if !var_names.contains(key.as_str()) {
                    errors.push(format!("group '{name}': unknown secret '{key}'"));
                }
                if !seen.insert(key.as_str()) {
                    errors.push(format!(
                        "group '{name}': key '{key}' is listed more than once"
                    ));
                }
            }
        }
        errors
    }

    /// Expand refs into var names. Empty `refs` means every var (a full fetch).
    pub fn expand(&self, refs: &[String]) -> Result<Vec<String>, SecretError> {
        if refs.is_empty() {
            let mut names: Vec<String> = self.vars.keys().cloned().collect();
            names.sort();
            return Ok(names);
        }
        expand_secret_refs(
            refs,
            &self.vars.keys().map(String::as_str).collect(),
            &self.groups,
        )
        .map_err(|unknown| {
            SecretError::msg(format!(
                "unknown secret or group: {}",
                unknown
                    .iter()
                    .map(|e| e.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })
    }
}

pub(crate) fn is_valid_name(name: &str) -> bool {
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
    groups: &'a HashMap<String, Group>,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn name_table() {
        assert!(is_valid_name("STRIPE_SECRET_KEY"));
        assert!(is_valid_name("_PRIVATE"));
        assert!(!is_valid_name(""));
        assert!(!is_valid_name("1KEY"));
        assert!(!is_valid_name("DD-API-KEY"));
    }

    #[test]
    fn expand_groups_and_dedupes() {
        let file: SecretsConfig = toml::from_str(
            r#"
            provider = "aws-ssm"
            [vars]
            A = "/app/A"
            B = "/app/B"
            C = "/app/C"
            [groups]
            app = ["A", "B"]
            "#,
        )
        .unwrap();
        file.validate().unwrap();
        let expanded = file
            .expand(&["app".into(), "C".into(), "A".into()])
            .unwrap();
        assert_eq!(expanded, vec!["A", "B", "C"]);
    }

    #[test]
    fn ssm_path_must_start_with_slash() {
        let file: SecretsConfig = toml::from_str(
            r#"
            provider = "aws-ssm"
            [vars]
            A = "app/A"
            "#,
        )
        .unwrap();
        let err = file.validate().unwrap_err().to_string();
        assert!(err.contains("must start with '/'"), "{err}");
    }
}
