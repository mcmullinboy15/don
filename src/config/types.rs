use serde::Deserialize;
use std::path::PathBuf;

/// A command to execute: a binary/program name plus arguments.
#[derive(Debug, Clone, Deserialize)]
pub struct Command {
    /// The binary or program to execute.
    pub cmd: String,
    /// Arguments to pass to the binary.
    #[serde(default)]
    pub args: Vec<String>,
}

/// Ready check configuration. Exactly one of `exec`, `tcp`, or `http` must be set.
///
/// Used to gate dependent services — they won't start until this check passes.
#[derive(Debug, Clone, Deserialize)]
pub struct ReadyCheck {
    /// Run a command — exit code 0 means ready.
    pub exec: Option<Command>,
    /// Connect to a TCP address — successful connection means ready.
    pub tcp: Option<String>,
    /// Hit an HTTP endpoint — 2xx response means ready.
    pub http: Option<String>,
    /// How often to check (e.g. "1s", "500ms"). Defaults to "1s".
    #[serde(default = "ReadyCheck::default_interval")]
    pub interval: String,
    /// How many times to retry before giving up. Defaults to 30.
    #[serde(default = "ReadyCheck::default_retries")]
    pub retries: u32,
}

impl ReadyCheck {
    fn default_interval() -> String {
        "1s".to_string()
    }

    fn default_retries() -> u32 {
        30
    }
}

/// Shutdown behavior for a service.
#[derive(Debug, Clone, Deserialize)]
pub struct ShutdownConfig {
    /// Signal to send for graceful shutdown (e.g. "SIGTERM", "SIGINT"). Defaults to "SIGTERM".
    #[serde(default = "ShutdownConfig::default_signal")]
    pub signal: String,
    /// Time to wait for graceful shutdown before sending SIGKILL (e.g. "10s"). Defaults to "10s".
    #[serde(default = "ShutdownConfig::default_timeout")]
    pub timeout: String,
}

impl ShutdownConfig {
    fn default_signal() -> String {
        "SIGTERM".to_string()
    }

    fn default_timeout() -> String {
        "10s".to_string()
    }
}

/// Where to send a service's or task's stdout/stderr.
///
/// In TOML, this is either a string shorthand or a table:
/// - `log = "stdout"` (default)
/// - `log = "ignore"`
/// - `log = "path/to/file.log"`
/// - `log = { file = "path/to/file.log" }` (equivalent to the string form)
#[derive(Debug, Clone, Default)]
pub enum LogConfig {
    /// Print to don's stdout, prefixed with the service name.
    #[default]
    Stdout,
    /// Discard all output.
    Ignore,
    /// Write to a file.
    File(PathBuf),
}

impl<'de> Deserialize<'de> for LogConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            String(String),
            Table { file: PathBuf },
        }

        match Raw::deserialize(deserializer)? {
            Raw::String(s) => match s.as_str() {
                "stdout" => Ok(Self::Stdout),
                "ignore" => Ok(Self::Ignore),
                path => Ok(Self::File(PathBuf::from(path))),
            },
            Raw::Table { file } => Ok(Self::File(file)),
        }
    }
}
