use serde::Deserialize;

/// A named subset of services and tasks to run.
///
/// Transitive dependencies are automatically included at runtime.
#[derive(Debug, Clone, Deserialize)]
pub struct Profile {
    /// Services to include in this profile.
    #[serde(default)]
    pub services: Vec<String>,
    /// Tasks to include in this profile.
    #[serde(default)]
    pub tasks: Vec<String>,
}
