//! In-memory secret values and AWS SSM fetch. Extract this directory as `key`.

mod aws;
mod config;
mod error;

pub use aws::AwsSsm;
pub use config::{Group, Provider, SecretsConfig};
pub use error::SecretError;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) use config::expand_secret_refs;

/// Pulled secret values plus the mapping needed to apply them to a process env.
#[derive(Clone, Default)]
pub struct SecretStore {
    inner: Arc<SecretStoreInner>,
}

impl std::fmt::Debug for SecretStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretStore")
            .field("len", &self.inner.values.len())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct SecretStoreInner {
    values: HashMap<String, String>,
    managed: HashSet<String>,
    groups: HashMap<String, Vec<String>>,
}

impl SecretStore {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_parts(
        values: HashMap<String, String>,
        groups: HashMap<String, Group>,
        managed: HashSet<String>,
    ) -> Self {
        let groups = groups
            .into_iter()
            .map(|(name, group)| (name, group.keys))
            .collect();
        Self {
            inner: Arc::new(SecretStoreInner {
                values,
                managed,
                groups,
            }),
        }
    }

    pub fn from_config(config: &SecretsConfig, values: HashMap<String, String>) -> Self {
        Self::from_parts(
            values,
            config.groups.clone(),
            config.vars.keys().cloned().collect(),
        )
    }

    pub fn len(&self) -> usize {
        self.inner.values.len()
    }

    /// Values long enough to redact from logs.
    pub fn redactable_values(&self) -> Vec<String> {
        self.inner
            .values
            .values()
            .filter(|v| v.len() >= 8)
            .cloned()
            .collect()
    }

    pub fn expand(&self, refs: &[String]) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for name in refs {
            if self.inner.managed.contains(name) {
                if seen.insert(name.as_str()) {
                    out.push(name.clone());
                }
                continue;
            }
            if let Some(keys) = self.inner.groups.get(name) {
                for key in keys {
                    if seen.insert(key.as_str()) {
                        out.push(key.clone());
                    }
                }
            }
        }
        out
    }

    pub fn strip_undeclared(&self, env: &mut HashMap<String, String>, refs: &[String]) {
        if self.inner.managed.is_empty() {
            return;
        }
        let declared = self.expand(refs);
        let declared: HashSet<&str> = declared.iter().map(String::as_str).collect();
        env.retain(|key, _| !self.inner.managed.contains(key) || declared.contains(key.as_str()));
    }

    pub fn inject(&self, env: &mut HashMap<String, String>, refs: &[String]) {
        for key in self.expand(refs) {
            if let Some(value) = self.inner.values.get(&key) {
                env.insert(key, value.clone());
            }
        }
    }

    pub fn apply(&self, env: &mut HashMap<String, String>, refs: &[String]) {
        self.strip_undeclared(env, refs);
        self.inject(env, refs);
    }
}

async fn fetch_values(config: &SecretsConfig) -> Result<HashMap<String, String>, SecretError> {
    config.validate()?;
    let names = config.expand(&[])?;
    match config.provider {
        Provider::AwsSsm => {
            let client = AwsSsm::new(config.region.clone(), config.profile.clone());
            client.fetch(&config.vars, &names).await
        }
    }
}

/// Fetch every mapped secret. Raced against shutdown so Ctrl+C is not stuck on AWS.
pub async fn resolve(
    config: &SecretsConfig,
    shutdown: &mut mpsc::Receiver<()>,
) -> Result<SecretStore, SecretError> {
    let pull = fetch_values(config);
    tokio::pin!(pull);
    let values = tokio::select! {
        result = &mut pull => result?,
        _ = shutdown.recv() => {
            return Err(SecretError::msg("interrupted while fetching secrets"));
        }
    };
    Ok(SecretStore::from_config(config, values))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn store(pairs: &[(&str, &str)], groups: &[(&str, &[&str])]) -> SecretStore {
        let values: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let managed: HashSet<String> = values.keys().cloned().collect();
        let groups = groups
            .iter()
            .map(|(name, keys)| {
                (
                    (*name).to_string(),
                    Group {
                        keys: keys.iter().map(|k| (*k).to_string()).collect(),
                    },
                )
            })
            .collect();
        SecretStore::from_parts(values, groups, managed)
    }

    #[test]
    fn apply_injects_only_declared_and_strips_the_rest() {
        let secrets = store(
            &[
                ("STRIPE_SECRET_KEY", "sk_live_value"),
                ("DD_API_KEY", "dd-key-xx"),
            ],
            &[("app", &["STRIPE_SECRET_KEY"])],
        );
        let mut env: HashMap<String, String> = [
            ("PATH", "/bin"),
            ("DD_API_KEY", "from-shell"),
            ("STRIPE_SECRET_KEY", "from-shell"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        secrets.apply(&mut env, &["STRIPE_SECRET_KEY".into()]);
        assert_eq!(
            env.get("STRIPE_SECRET_KEY").map(String::as_str),
            Some("sk_live_value")
        );
        assert!(!env.contains_key("DD_API_KEY"));
        assert_eq!(env.get("PATH").map(String::as_str), Some("/bin"));
    }

    #[test]
    fn debug_omits_values() {
        let secrets = store(&[("STRIPE_SECRET_KEY", "sk_live_secret")], &[]);
        let text = format!("{secrets:?}");
        assert!(!text.contains("sk_live_secret"), "{text}");
    }
}
