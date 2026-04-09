use serde::Deserialize;
use std::path::PathBuf;

/// Bazel build tool integration for a service or task.
///
/// When configured, Don queries Bazel at startup to determine which source
/// packages contribute to the target, and watches those directories for changes.
/// This replaces the need for manually maintaining `watch` patterns.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BazelConfig {
    /// Bazel target label (e.g. `"//services/api:api"`).
    pub target: String,
    /// Query timeout in seconds. Defaults to 30.
    pub query_timeout: Option<u64>,
}

/// Turborepo build tool integration for a service or task.
///
/// When configured, Don queries Turborepo at startup to determine the task
/// graph and source inputs, and watches the relevant package directories.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TurboConfig {
    /// Turbo task name to query for watch resolution (e.g. `"dev"`, `"build"`).
    pub task: String,
    /// Turbo task to run during the batch build phase (e.g. `"build"`).
    /// Defaults to `"build"`. Set to `""` to skip the batch build for this item.
    pub build_task: Option<String>,
    /// Filter to a specific package (e.g. `"@myorg/api"`).
    pub filter: Option<String>,
    /// Query timeout in seconds. Defaults to 30.
    pub query_timeout: Option<u64>,
}

/// A proxy entry: Don listens on `listen` and forwards TCP connections to the
/// service on a random ephemeral port.
///
/// If `env` is set, Don injects the ephemeral port as that environment variable
/// (and supports `${VAR}` substitution in `run.args`). If `env` is `None`, Don
/// passes the ephemeral socket via LISTEN_FDS (systemd socket activation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyEntry {
    /// Address Don binds and accepts connections on (e.g. "127.0.0.1:3000").
    pub listen: String,
    /// If set, the env var name Don sets to the ephemeral port number.
    /// If `None`, Don passes the socket via LISTEN_FDS instead.
    pub env: Option<String>,
}

impl<'de> Deserialize<'de> for ProxyEntry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            String(String),
            Table {
                listen: String,
                env: Option<String>,
            },
        }

        match Raw::deserialize(deserializer)? {
            Raw::String(s) => Ok(ProxyEntry {
                listen: s,
                env: None,
            }),
            Raw::Table { listen, env } => Ok(ProxyEntry { listen, env }),
        }
    }
}

/// Deserialize the `proxy` field which accepts:
/// - A single string: `proxy = "127.0.0.1:3000"`
/// - A single table: `proxy = { listen = "127.0.0.1:3000", env = "PORT" }`
/// - An array of strings/tables: `proxy = ["127.0.0.1:3000", { listen = "...", env = "PORT" }]`
pub(crate) fn deserialize_proxy<'de, D>(deserializer: D) -> Result<Vec<ProxyEntry>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RawProxy {
        Single(ProxyEntry),
        List(Vec<ProxyEntry>),
    }

    match RawProxy::deserialize(deserializer)? {
        RawProxy::Single(entry) => Ok(vec![entry]),
        RawProxy::List(entries) => Ok(entries),
    }
}

/// Deserialize an optional `proxy` field (for `ServiceOverride`).
/// Returns `None` when the field is absent, `Some(vec)` when present.
pub(crate) fn deserialize_proxy_option<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<ProxyEntry>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_proxy(deserializer).map(Some)
}

/// A command to execute: a binary/program name plus arguments.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
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
