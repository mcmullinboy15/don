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
}

/// A single proxy entry. Don binds `listen` once at startup and holds the
/// port across service restarts.
///
/// Exactly one of two modes must be chosen:
///
/// - **Forwarding with env injection.** Don accepts on `listen`, allocates an
///   ephemeral port for the backend, sets `env` to the ephemeral port number,
///   and forwards bytes between client and backend. The service binds the
///   ephemeral port using the value of the env var.
/// - **Listenfd handoff.** Don binds `listen` and passes the bound listener
///   to the service as `fd 3` via the systemd `LISTEN_FDS` protocol. The
///   service accepts on the fd directly — no proxy involved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyEntry {
    /// Address Don binds on (e.g. "127.0.0.1:3000").
    pub listen: String,
    /// Mode selector. Exactly one variant applies per entry.
    pub mode: ProxyMode,
}

/// How a given [`ProxyEntry`] exposes the public listener to the service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyMode {
    /// Accept on the public address, forward to an ephemeral backend port.
    /// The ephemeral port number is injected into the service's environment
    /// under the contained variable name.
    Env(String),
    /// Hand the bound listener to the service via `LISTEN_FDS` / `LISTEN_FDNAMES`
    /// / `LISTEN_PID`. No forwarding — the service accepts directly.
    Listenfd,
    /// Accept on the public address, forward to the fixed backend address
    /// `addr`. The service is expected to bind `addr` on its own — don
    /// does NOT inject an env var, and does NOT allocate a port. Use this
    /// when the service has a compile-time or config-baked backend port
    /// that can't come from the environment.
    ///
    /// Restart semantics differ from env/listenfd: don waits for the old
    /// instance's process group to fully exit before starting the new one,
    /// because two processes trying to bind the same fixed port would race.
    Forward(String),
}

impl<'de> Deserialize<'de> for ProxyEntry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum ForwardRaw {
            Addr(String),
            Port(u16),
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            String(String),
            Table {
                listen: String,
                env: Option<String>,
                #[serde(default)]
                listenfd: bool,
                forward: Option<ForwardRaw>,
            },
        }

        match Raw::deserialize(deserializer)? {
            // Shorthand `proxy = "127.0.0.1:3000"` means listenfd mode — the
            // simpler of the two modes and the common case for services that
            // speak the systemd socket-activation protocol.
            Raw::String(s) => Ok(ProxyEntry {
                listen: s,
                mode: ProxyMode::Listenfd,
            }),
            Raw::Table {
                listen,
                env,
                listenfd,
                forward,
            } => {
                let forward_addr = forward.map(|f| match f {
                    ForwardRaw::Addr(a) => a,
                    ForwardRaw::Port(p) => format!("127.0.0.1:{p}"),
                });
                let mode = match (env, listenfd, forward_addr) {
                    (Some(e), false, None) => ProxyMode::Env(e),
                    (None, true, None) => ProxyMode::Listenfd,
                    (None, false, Some(a)) => ProxyMode::Forward(a),
                    (None, false, None) => {
                        return Err(serde::de::Error::custom(
                            "proxy entry: one of 'env = \"VAR\"', 'listenfd = true', or 'forward = \"addr\"' must be set",
                        ));
                    }
                    _ => {
                        return Err(serde::de::Error::custom(
                            "proxy entry: 'env', 'listenfd', and 'forward' are mutually exclusive",
                        ));
                    }
                };
                Ok(ProxyEntry { listen, mode })
            }
        }
    }
}

/// Deserialize the `proxy` field which accepts:
/// - A single string: `proxy = "127.0.0.1:3000"` (listenfd mode)
/// - A single table: `proxy = { listen = "127.0.0.1:3000", env = "PORT" }`
///   or `{ listen = "127.0.0.1:3000", listenfd = true }`
/// - An array of strings/tables.
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
/// When `monitor = true`, the same check keeps running after the service is
/// ready and can mark it `Unhealthy` if it starts failing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReadyCheck {
    /// Run a command — exit code 0 means ready.
    pub exec: Option<Command>,
    /// Connect to a TCP address — successful connection means ready.
    pub tcp: Option<String>,
    /// Hit an HTTP endpoint — 2xx response means ready.
    pub http: Option<String>,
    /// How often to check during startup (e.g. "1s", "500ms"). Defaults to "1s".
    #[serde(default = "ReadyCheck::default_interval")]
    pub interval: String,
    /// How many times to retry before giving up. Defaults to 30.
    #[serde(default = "ReadyCheck::default_retries")]
    pub retries: u32,
    /// If true, keep running the same check after the service reaches Ready.
    /// Consecutive failures will mark the service Unhealthy. The service-level
    /// `on_failure` field controls what happens on that transition.
    #[serde(default)]
    pub monitor: bool,
    /// Interval between checks while monitoring. Defaults to "10s".
    #[serde(default = "ReadyCheck::default_monitor_interval")]
    pub monitor_interval: String,
    /// Consecutive monitor failures required to transition Ready → Unhealthy.
    /// Defaults to 3.
    #[serde(default = "ReadyCheck::default_unhealthy_after")]
    pub unhealthy_after: u32,
}

impl ReadyCheck {
    fn default_interval() -> String {
        "1s".to_string()
    }

    fn default_monitor_interval() -> String {
        "10s".to_string()
    }

    fn default_retries() -> u32 {
        30
    }

    fn default_unhealthy_after() -> u32 {
        3
    }
}

/// Action to take when a service fails. A "failure" is either:
///   - the health monitor marking the service `Unhealthy`, or
///   - the process exiting with a non-zero status (or terminating signal).
///
/// Clean exits (status 0) are treated as intentional shutdowns and never
/// trigger this policy — the service transitions to `Stopped`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    /// Mark the service `Unhealthy` / `Failed` and emit a lifecycle event.
    /// No restart.
    #[default]
    Notify,
    /// Restart the service with escalating backoff (1, 2, 4, 8, 16, 32, 60s
    /// capped at 60s). Backoff attempts reset when the service recovers
    /// to `Ready` or a restart succeeds.
    Restart,
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
